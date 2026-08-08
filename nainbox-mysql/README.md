# nainbox-mysql

`nainbox-mysql` 让消息唯一标记与业务 MySQL 副作用共享同一个 `natx` ambient 事务。复合主键
`(consumer_name, message_id)` 串行化并发重投，只允许一个成功事务执行业务。

```toml
[dependencies]
nasa = { version = "1", features = ["inbox"] }
```

```rust
use nasa::inbox::MySqlInbox;
async fn consume(message_id: &str) -> anyhow::Result<()> {
    match MySqlInbox::new()
        .process("order-projection", message_id, || async {
            update_projection().await
        })
        .await?
    {
        nasa::inbox::InboxProcess::Applied(()) => {}
        nasa::inbox::InboxProcess::Duplicate => {}
    }
    Ok(())
}
```

## YML 配置

本 crate 不新增配置根，复用 `database:` / `datasources:` 和当前 ambient datasource。

```yaml
database:
  url: ${APP_MYSQL_URL}
  migrations:
    mode: validate
```

生产环境由 migration 创建 `inbox_message` 表；`ensure_schema` 只用于本地自举。

## 主要边界

- `claim` 在事务外明确失败，不会 autocommit。
- 返回 `Claimed` 后必须在同一事务调用栈内完成业务 SQL。
- `process` 统一执行 claim、业务闭包和提交，业务项目无需重复编写事务外壳；返回
  `CommitUncertain` 或 `RollbackFailed` 时必须保留原消息继续收敛。
- 回滚会同时撤销唯一标记和业务写，因此重投仍能继续。
- 该合同不覆盖外部服务调用和消息再发布；这些副作用需要 Outbox 或目标系统幂等键。
