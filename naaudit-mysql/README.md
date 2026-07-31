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

let sink = MySqlOutboxAuditSink::new();
sink.record_transactional(AuditEvent::new(
    "subject:7",
    "order.cancel",
    "order:1001",
    AuditOutcome::Success,
    occurred_at_millis,
))
.await?;
```

调用点必须位于 `#[transactional]` 或 `nasa::tx::run` 内；否则返回脱敏的 `AuditWriteError`。

## YML 配置

本 adapter 不新增配置根，复用 `database:` / `datasources:` 和 `natx` 默认 datasource。

```yaml
database:
  url: ${APP_MYSQL_URL}
  migrations:
    mode: validate
```

## 主要边界

- adapter 只负责追加审计 Outbox 行，不启动 dispatcher。
- 生产 schema 必须由 migration 拥有。
- 底层 SQL、连接信息和事件 payload 不会进入公开错误。
- 若业务写和审计写使用不同 datasource，就不再具备同事务原子性。
