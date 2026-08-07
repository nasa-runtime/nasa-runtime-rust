# nasaga-mysql

`nasaga-mysql` 为 Saga 提供 MySQL 持久化，包括实例、步骤、attempt、迁移日志、durable timer、冻结
补偿计划、管理审计、冲突事实、运行指标和参与方 gate。它由 `nasaga-runtime` 调用；只有自定义
运行时或受控结构自举需要直接依赖本 crate。

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

完整运行边界见 [Saga 生产运行指南](../docs/saga-production.md)。
