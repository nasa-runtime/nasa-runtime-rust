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
    match MySqlInbox::with_datasource("orders")?
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

本 crate 不新增配置根，复用 `database:` / `datasources:`。`new()` 固定绑定 `default`；
`with_datasource(name)` 固定绑定命名库，且必须与调用栈中的 ambient datasource 一致。

```yaml
datasources:
  orders:
    url: ${APP_ORDERS_MYSQL_URL}
    migrations:
      mode: validate
```

生产环境由 migration 创建 `inbox_message` 表；启用保留任务前，既有表必须应用语义迁移
`migrations/add_inbox_retention_index.sql`，建立 `(consumer_name, processed_at)` 范围索引。
`ensure_schema` 只用于本地自举，会为新表和既有本地表确认该索引；并发自举采用另一副本已完成的
同形索引。既有大表补建索引的耗时与表规模相关，生产环境应在批准的 migration 窗口执行，不把该
DDL 放进应用启动时限。

启用 `application,inbox` 时，可把 `InboxRetentionPlan` 交给 Application 运行串行 fixed-delay
清理；计划绑定的 datasource 与 `MySqlInbox::with_datasource` 使用同一命名 registry，不会回退默认
库。重投视界、指标与停机边界见 `nainbox-core` 和 `napp` README。

## 主要边界

- `claim` 在事务外明确失败，不会 autocommit。
- `process`、`claim` 与 schema 自举都使用句柄绑定的 datasource；名称不一致时不会查询默认库。
- `MySqlInbox` 同时实现后端中立 `nainbox_core::InboxStore`，既有 inherent methods 与类型路径保持不变。
- 返回 `Claimed` 后必须在同一事务调用栈内完成业务 SQL。
- `process` 统一执行 claim、业务闭包和提交，业务项目无需重复编写事务外壳；返回
  `CommitUncertain` 或 `RollbackFailed` 时必须保留原消息继续收敛。
- 回滚会同时撤销唯一标记和业务写，因此重投仍能继续。
- 该合同不覆盖外部服务调用和消息再发布；这些副作用需要 Outbox 或目标系统幂等键。
- `MySqlInbox` 同时实现 `DurableInboxRetention`：按 `nainbox-core` 保留清理合同做 owner 互斥
  (`GET_LOCK`，锁与删除同连接)的分批过期清理；锁身份是完整 consumer namespace 的定长摘要，
  不受 MySQL 64 字节锁名上限影响。cutoff 轮初冻结为字面时间参数，删除沿保留索引从最老标记开始；
  完整墙上时间预算覆盖取连接、取锁、删除、统计和解锁；轮次拒绝任一数据库 driver 的环境事务；
  取消、超时或解除锁未获确认时关闭物理连接，不把可能持锁的 session 放回池。策略视界约束与
  轮次报告见 `nainbox-core` README。
