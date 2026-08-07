# Saga 数据库迁移

本目录中的 SQL 是 `nasaga-mysql` 生产结构合同。`MySqlSagaStore::ensure_schema` 只用于受控自举，
生产数据库必须由部署系统按固定顺序执行迁移并保存执行事实。

## 执行顺序

1. `saga_timer_control_generation.up.sql`
2. `saga_start_digest_control_audit.up.sql`
3. Orchestrator 数据库执行 `saga_orchestrator_binary_collation.up.sql`
4. 参与方数据库执行 `saga_participant_binary_collation.up.sql`
5. Orchestrator 数据库执行 `saga_management_actor_audit.up.sql`
6. Orchestrator 数据库执行 `saga_conflict_fact_audit.up.sql`
7. Orchestrator 数据库执行 `saga_observability_indexes.up.sql`
8. 参与方数据库执行 `saga_participant_definition_digest.up.sql`
9. 按受信定义发布记录回填全部历史 participant gate，并逐组核对影响行数
10. 确认摘要没有 `NULL`、空串、长度错误或非法字符后执行
    `saga_participant_definition_digest_required.up.sql`

普通迁移期间先阻止旧 binary 写 Saga 表，再执行 DDL，最后滚动启动新 binary。上线前需要在与目标
数据量相当的副本上评估 `EXPLAIN ALTER`、元数据锁等待、复制延迟和完成时间，并由部署负责人批准
维护窗或在线 DDL 预算。

`start_request_digest` 对历史行保留 `NULL`，因为旧请求正文无法从 Orchestrator 数据库可靠重建。
此类实例再次命中 `business_key` 时运行时会 fail-closed，操作员必须核对事实，不能伪造摘要回填。

## 参与方摘要封口

摘要扩展与摘要约束脚本构成强制的两阶段变更。旧进程停写后，部署系统从受信、不可变的定义记录生成
`(workflow_name, definition_version, definition_digest)` 清单；每组先锁定并核对 gate 数量，再在单一
事务中只更新 `definition_digest IS NULL` 的行：

```sql
START TRANSACTION;
SELECT COUNT(*) FROM saga_participant_step
 WHERE workflow_name = 'checkout' AND definition_version = 7
   AND definition_digest IS NULL FOR UPDATE;
UPDATE saga_participant_step
   SET definition_digest = '<approved-64-lowercase-hex-digest>'
 WHERE workflow_name = 'checkout' AND definition_version = 7
   AND definition_digest IS NULL;
COMMIT;
SELECT COUNT(*) FROM saga_participant_step
 WHERE definition_digest IS NULL
    OR CHAR_LENGTH(definition_digest) <> 64
    OR BINARY definition_digest <> BINARY LOWER(definition_digest)
    OR definition_digest REGEXP '[^0-9a-f]';
```

最后一个查询必须为 0，才允许执行摘要约束脚本。该脚本先添加格式 `CHECK`，再将列改为 `NOT NULL`，避免
非严格 `sql_mode` 把残留 `NULL` 静默转换为空串。找不到唯一受信记录、同一 workflow 和定义版本
出现多个摘要或影响行数不符时必须停止发布并人工核验；不得使用 command 自报摘要回填。

## 排序规则转换

Orchestrator 与参与方的 raw binary collation SQL 只适用于停写维护窗。大表在线变更应由
组织批准的在线 DDL 执行器完成相同语义，并满足以下条件：

- shadow copy 期间业务写持续成功，复制延迟不越过批准阈值；
- metadata lock 等待、总耗时和磁盘余量位于批准预算内；
- 切换前后行数、关键索引、唯一约束和排序规则一致；
- 异常时先停止切换并保留原表，不允许在证据不足时强制推进。

排序规则转换没有通用 `.down.sql`。执行前记录
`information_schema.TABLES.TABLE_COLLATION`；确需回退时，依据该记录生成当前部署专用 SQL，不能
猜测目标排序规则。

## 回退边界

`.down.sql` 只描述 binary 回退后的结构恢复步骤，不是日常自动回滚。删除
`saga_control_transition`、`saga_management_audit` 或 `saga_conflict_fact` 会丢失控制操作幂等、主体
归因或人工介入证据；执行前必须导出相关事实，并确认 replay horizon 内不会再接收对应 operation。

任何迁移动作都不得输出连接凭据、业务 payload 或完整业务键。部署日志只记录迁移文件、目标逻辑库、
批准单号、起止时间、影响行数和脱敏错误分类。
