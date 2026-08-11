# nasaga-mysql

`nasaga-mysql` 为 Saga 提供 MySQL 持久化，包括实例、步骤、attempt、迁移日志、durable timer、冻结
补偿计划、管理审计、冲突事实、运行指标和参与方 gate。它由 `nasaga-runtime` 调用；只有自定义
运行时或受控结构自举需要直接依赖本 crate。

本 crate 的核心价值是把 Saga 裁决变成**可恢复的数据库事实**：状态 CAS、触发去重、timer fencing、
审计、租户配额和下一条 Outbox 消息在各自规定的事务边界内提交；进程崩溃后不依赖内存队列恢复，
多个副本也不能凭本地状态越过已经失去的租约或实例版本。

```toml
[dependencies]
nasaga-mysql = "1"
natx = "1"
```

## 初始化

本 crate 复用 `natx` 的 ambient MySQL 事务。Orchestrator 的 Saga 表、Inbox 与 command Outbox 必须
位于同一数据库；参与方的 gate、业务表、Inbox 与 result Outbox 必须位于该参与方自己的同一数据库。

受控自举环境可在连接池安装后创建表结构：

```rust
natx::init(pool);
nasaga_mysql::MySqlSagaStore::ensure_schema().await?;
nasaga_mysql::MySqlSagaStore::ensure_participant_schema().await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

生产环境使用 [迁移说明](migrations/README.md) 中的 SQL 顺序，不调用 `ensure_schema` 代替部署系统。

## 数据模型与事务域

| 数据 | 权威作用 | 事务要求 |
| --- | --- | --- |
| instance / step / attempt / transition | 当前投影、完整尝试历史与版本 CAS | 与 Orchestrator Inbox、下一 command Outbox 同事务 |
| participant gate | 阶段准入、定义摘要与最终结果投影 | 与参与方 Inbox、业务事实、result Outbox 同事务 |
| durable timer | deadline、取消、裁决与补偿恢复 | 领取独立提交；执行前复验租约、generation、版本与 token |
| control / management / conflict audit | 可归因管理动作、幂等 operation 与互斥事实 | 与对应状态变化同事务 |
| tenant quota / action rate | 在飞实例与变更类管理动作预算 | 预留和释放与业务动作同事务 |

创建幂等由 `(tenant_id, workflow_name, business_key)` 唯一事实承担；同 key 的 canonical 请求摘要不一致
时拒绝把新请求伪装成重复。attempt 身份、trigger 身份和 transition 序号各有唯一权威，不能由调用方
另建第二套序列。

## YML 配置

本 crate 不新增配置根，复用 `database:` 或 `datasources:`。Saga、Inbox 和 Outbox 必须解析到同一
ambient datasource。

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 32
  acquire_timeout_ms: 3000
  migrations:
    mode: validate
```

连接池容量需要同时覆盖业务请求、消息消费、Outbox 投递和 timer 领取，不能按单一路径估算。

## 启动校验与滚动升级

`Orchestrator::verify_startup` 会读取非终态实例，确认其 workflow、定义版本和摘要仍能由当前注册表解释；
参与方 descriptor 与 gate 摘要也必须在 Ready 前一致。扫描受 `startup_scan_limit` 约束，超出上限不是
“其余实例默认合法”，而是容量或发布阻断。

生产结构按 [迁移说明](migrations/README.md) expand-first。trace、租户配额和管理动作速率对应的表或列
必须先于启用能力的 binary/配置上线。`MANUALLY_CLOSED` 使用“先升级全部读者、再打开
`enable_manual_close`”的门禁；旧读者尚存时不得产生新终态。

存量租户启用实例配额前，必须先让全部创建/终态写路径运行记账版本，再在事务内调用
`reconcile_tenant_quota` 把非终态事实写入账本并置初始化标记，最后才下发上限。未初始化账本必须拒绝
新建和 Ready，不能用零计数冒充真实存量。

## 并发与租约

- 所有推进以受影响行数和实例版本 CAS 为权威，冲突事务不得发布新 Outbox。
- timer worker 使用 `TimerFencingTokenIssuer` 发行不可复制的 capability；`claim_due_timers` 消费该
  capability，并将所有权交给 `TimerClaimBatch`。
- 租约过期、token 丢失或实例版本不匹配时，worker 必须立即停止当前动作。
- 参与方在任何业务写之前锁定 gate，并让 gate、业务事实、Inbox 和结果 Outbox 共享一次提交。

## 主要边界

- 本 crate 不提供跨数据库事务，也不会把两个连接池包装成一个原子边界。
- `claim_due_timers` 独立提交租约；其余写路径要求 ambient 事务。
- token 不接受裸字符串构造，不能跨领取批次复用，也不应进入日志。
- replay horizon 到期前不得删除 Inbox、attempt journal、participant gate 或控制审计事实。
- 排序规则转换可能重建大表，执行方式和资源预算必须由部署负责人批准。

完整运行边界见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。
