# naaudit-pgsql

`naaudit-pgsql` 是 `TransactionalAuditSink` 的 PostgreSQL Outbox adapter。它不创建连接池，也不在
同步接口内阻塞 runtime；审计事件经公共 `AuditEvent::into_outbox_event` 映射后，加入句柄绑定
datasource 的 `natx-pgsql` ambient transaction。

## 初始化与使用

```toml
[dependencies]
naaudit = "1"
naaudit-pgsql = "1"
natx-pgsql = "1"
```

业务经门面使用时开启 `audit-pgsql`，从 `nasa::audit::pgsql` 导入 sink，并从 `nasa::audit` 使用公共事件
合同；与 `application` 组合时复用受管 PostgreSQL datasource 和 Outbox 生命周期。

```rust
use naaudit::{AuditEvent, AuditOutcome, TransactionalAuditSink};
use naaudit_pgsql::PgOutboxAuditSink;

#[natx_pgsql::transactional(datasource = "orders")]
async fn record_cancel(occurred_at_millis: u64) -> anyhow::Result<()> {
    PgOutboxAuditSink::with_datasource("orders")?
        .record_transactional(AuditEvent::new(
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

## 配置与观测

本 crate 不读取独立 YAML，也不启动 dispatcher。Application 连接沿用 `database` / `datasources`，
审计投递健康、积压与死信由同源 Outbox 观测；actor、resource 和 context 属于高基数业务字段，不进入
基础设施指标 label。

## 原子性与边界

- 业务事实、审计事件和 Outbox 行必须使用同一个 datasource 和同一个 ambient transaction。
- 缺少事务或 datasource 不一致时，底层 `PgOutbox::append_transactional` 在 SQL 前拒绝。
- adapter 只追加审计 Outbox 行，不启动 dispatcher，也不创建第二份 pool 或 registry。
- actor、action、resource、outcome、发生时刻与脱敏 context 使用 `naaudit` 的唯一映射来源；
  PostgreSQL adapter 不重新解释归因字段。
- SQL、连接信息与审计 payload 不进入公开错误文本。
