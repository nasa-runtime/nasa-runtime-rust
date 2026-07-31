# naaudit

`naaudit` 定义业务审计事件和可靠写入合同。它记录谁在何时对哪个资源执行了什么动作以及结果，
并把事件映射为 Outbox 事件；它不是普通运行日志，也不负责数据库连接或 dispatcher 生命周期。

业务项目通过门面开启 `audit`：

```toml
[dependencies]
nasa = { version = "1", features = ["audit"] }
```

## 初始化与使用

`MySqlOutboxAuditSink` 必须在 `#[transactional]` / `nasa::tx::run` 的 ambient MySQL 事务内调用。

```rust
use nasa::audit::{
    AuditEvent, AuditOutcome, MySqlOutboxAuditSink, TransactionalAuditSink,
};
use nasa::tx::transactional;

#[transactional]
async fn grant_role(user_id: i64, now_millis: u64) -> anyhow::Result<()> {
    update_role(user_id).await?;

    MySqlOutboxAuditSink::new()
        .record_transactional(
            AuditEvent::new(
                "operator:42",
                "user.role.grant",
                format!("user:{user_id}"),
                AuditOutcome::Success,
                now_millis,
            )
            .with_context("tenant", "tenant-a"),
        )
        .await?;
    Ok(())
}
```

事务提交时业务写与审计 outbox 行一起可见；回滚时两者一起消失。

## YML 配置

本 crate 不读取独立 yml。MySQL 连接、迁移和事务配置归 `database:` / `datasources:`；审计事件字段
由业务代码提供。

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 16
```

生产环境应由 migration 创建 `outbox_event` 表，不能把运行期自举方法当作 schema 管理。

## 主要边界

- `actor`、`action`、`resource` 和 `context` 只能放稳定、已脱敏信息，不能放 token、secret 或请求正文。
- `record_transactional` 在事务外明确失败，不会退化为 autocommit。
- Outbox 只保证可靠投递路径；下游仍要按 `event_id` 幂等去重。
- `AuditSink` 的同步接口适合已有同步 Outbox writer；关键持久审计优先使用 `TransactionalAuditSink`。
