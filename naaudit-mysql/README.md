# naaudit-mysql

`naaudit-mysql` 是 `TransactionalAuditSink` 的 MySQL Outbox adapter。它本身无连接池和全局状态，
每次写入都从 `natx` ambient 事务取得同一条连接。

业务通常不直接依赖本 crate，而是通过门面：

```toml
[dependencies]
nasa = { version = "1", features = ["audit"] }
```

```rust
use nasa::audit::{
    AuditEvent, AuditOutcome, MySqlOutboxAuditSink, TransactionalAuditSink,
};
use nasa::tx::transactional;

#[transactional(datasource = "orders")]
async fn record_cancel(occurred_at_millis: u64) -> anyhow::Result<()> {
    let sink = MySqlOutboxAuditSink::with_datasource("orders")?;
    sink.record_transactional(AuditEvent::new(
        "subject:7",
        "order.cancel",
        "order:1001",
        AuditOutcome::Success,
        occurred_at_millis,
    ))
    .await?;
    Ok(())
}
```

调用点必须位于同一 datasource 的 `#[transactional]` 或 `nasa::tx::run_for` 内；否则返回脱敏的
`AuditWriteError`。`new()` 保留默认库语义，`datasource_ref()` 可供启动计划复验绑定。

## YML 配置

本 adapter 不新增配置根，复用 `database:` / `datasources:`；审计事件与该句柄绑定库中的业务写、
Outbox 行共享同一个本地事务。

```yaml
datasources:
  orders:
    url: ${APP_ORDERS_MYSQL_URL}
    migrations:
      mode: validate
```

## 主要边界

- adapter 只负责追加审计 Outbox 行，不启动 dispatcher。
- 生产 schema 必须由 migration 拥有。
- 底层 SQL、连接信息和事件 payload 不会进入公开错误。
- 若业务写和审计写使用不同 datasource，就不再具备同事务原子性。
