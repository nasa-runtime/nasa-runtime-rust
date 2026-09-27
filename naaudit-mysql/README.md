# naaudit-mysql

`naaudit-mysql` 是 `TransactionalAuditSink` 的 MySQL Outbox adapter。它本身无连接池和全局状态，
每次写入都从 `natx` ambient 事务取得同一条连接。
门面同时开启 `application,audit` 时可声明命名 `audit_sinks`，由 Application 绑定数据源、验证 schema
并管理句柄关闭，业务仍显式决定审计事件的记录时机。

## 写入架构

```text
业务处理器 ──→ 同 datasource ambient transaction ──→ 业务表
                                      └──────────────→ 审计 Outbox 行
COMMIT 明确成功 ──→ Outbox dispatcher ──→ 审计消费方
```

adapter 只在当前事务中追加审计事件，不自行提交或发布。事务回滚时业务写与审计行一并消失；提交结果
不确定时调用方不得把事件当作已经可靠投递，应按业务事务的未知结果流程收敛。

业务通常不直接依赖本 crate，而是通过门面：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["audit"] }
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

独立 adapter 显式绑定已注册的数据源。Application 声明 `"db"` 后按 `audit_sinks` 装配命名 sink；
审计事件与该句柄绑定库中的业务写、Outbox 行共享同一个本地事务。

```yaml
datasources:
  orders:
    driver: mysql
    url: ${APP_ORDERS_MYSQL_URL}
    migrations:
      mode: validate
audit_sinks:
  changes:
    enabled: true
    driver: mysql
    source: orders
```

## 主要边界

- adapter 只负责追加审计 Outbox 行，不启动 dispatcher。
- 生产 schema 必须由 migration 拥有。
- 底层 SQL、连接信息和事件 payload 不会进入公开错误。
- 若业务写和审计写使用不同 datasource，就不再具备同事务原子性。

## Application 接入

门面 feature 为 `application,audit`；直接依赖 `napp` 时对应 `audit-mysql`。迁移门禁之后只读验证
Outbox schema，再通过 `app.audit_sink("changes").await` 提供句柄。Service 在 initializer 或 Ready
后取得 sink，Batch 在工作负载前完成装配；关闭后旧句柄拒绝新记录。

sink 只追加同来源 ambient 事务，缺事务或错源会拒绝，不自行提交或启动 dispatcher。
需要投递时显式配置同来源 Outbox 生命周期；构造 sink 不代表审计事件已经送达。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
