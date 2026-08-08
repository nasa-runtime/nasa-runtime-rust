# Outbox 数据库迁移

生产数据库不能用运行期 `ensure_schema` 替代结构治理：

1. 新部署执行 `outbox_schema.up.sql`，直接建立当前完整结构；
2. 已有 `(dispatched, id)` 旧索引的部署执行 `outbox_dispatch_indexes.up.sql`，在线替换索引；
3. 部署系统保存执行事实，禁止对已经具备当前索引的新表重复执行历史索引迁移。

当前结构优化三条固定查询路径：

- `(dispatched, dead, id)` 为待投递计数提供覆盖读取，并为按 `id` 有序批量领取定位真实候选；领取
  payload 等事件列时仍按主键回表；
- `(dead, id)` 服务死信计数；
- 查询前缀已由新索引包含的 `(dispatched, id)` 在新索引建立后删除，避免重复写放大。

DDL 前先在目标规模副本上核对执行计划、metadata lock、复制延迟、索引空间和完成时间。生产表持续写入时，
必须使用组织批准的在线 DDL 执行器保持等价索引语义。部署记录应保存目标逻辑库、起止时间、影响行数、
索引定义和脱敏错误，不得输出 payload、凭据或完整业务身份。

`outbox_dispatch_indexes.down.sql` 只用于 binary 回退已经明确要求旧索引形态的场景。回退会重新引入
死信计数全表扫描和死信热区过滤成本，执行前必须停止指标抓取与 dispatcher，并取得数据库负责人批准。
