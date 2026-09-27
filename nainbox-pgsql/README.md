# nainbox-pgsql

`nainbox-pgsql` 让消息唯一标记与业务 PostgreSQL 副作用共享同一个 `natx-pgsql` ambient
transaction。复合主键 `(consumer_name, message_id)` 串行化并发重投，只允许一个成功事务执行业务；
其它事务把已提交主键裁决为 `Duplicate`，不会再次调用业务闭包。

## 初始化与使用

```toml
[dependencies]
nainbox-pgsql = "2.0.0"
```

业务经门面使用时开启 `inbox-pgsql`，并从 `nasa::inbox::pgsql` 导入相同合同；与 `application` 组合时，
命名 PgPool 由 `"db"` 组件从 YAML 创建和关闭。

```rust
use nainbox_pgsql::{InboxProcess, PgInbox};

async fn consume(message_id: &str) -> anyhow::Result<()> {
    match PgInbox::with_datasource("orders")?
        .process("order-projection", message_id, || async {
            update_projection().await
        })
        .await?
    {
        InboxProcess::Applied(()) => {}
        InboxProcess::Duplicate => {}
    }
    Ok(())
}
```

## Datasource 与配置

本 crate 不创建独立 registry。`new()` 固定绑定 `default`；`with_datasource(name)` 固定绑定
`natx-pgsql` 已登记的命名 datasource。claim 与业务 SQL 的 datasource 不一致时直接失败，不回退默认库。

生产结构由 migration 创建；启用保留任务前，既有表必须应用语义迁移
`migrations/add_inbox_retention_index.sql`，建立 `(consumer_name, processed_at_ms)` 范围索引。
`ensure_schema` 只用于显式自举，并以幂等 DDL 确认该索引。

本 crate 不读取独立 YAML。Application 配置沿用 `database` / `datasources`；启用
`application,inbox-pgsql` 后可显式提交 `InboxRetentionPlan`，由 Application 在 Ready 后运行串行
fixed-delay 清理。计划与 `PgInbox::with_datasource` 绑定同一命名 registry，不会回退默认库。Inbox
结果由消费端按 `Applied`、`Duplicate` 和事务失败分类记录，message id 与 consumer name 不直接作为
无限指标 label；受管保留指标为无标签进程聚合。

## 事务与失败边界

- `claim` 在事务外明确失败，不会 autocommit。
- 只有 `inbox_message_pkey` 冲突可以成为 `Duplicate`；其它唯一约束、检查约束和数据库错误保持失败。
- `process` 统一执行 claim、业务闭包与提交；只有 `Applied` 或 `Duplicate` 才能据此确认原消息。
- `CommitUncertain`、`CommitRejected` 或 `RollbackFailed` 时必须保留原消息持续重投，不能消耗普通有限重试预算。
- 回滚同时撤销唯一标记与业务写，后续投递仍可取得 claim。
- 外部 HTTP、Kafka 或其它数据库副作用不在本地事务合同内，应使用同源 Outbox 或目标系统幂等键收敛。
- `PgInbox` 同时实现 `DurableInboxRetention`：按 `nainbox-core` 保留清理合同做 owner 互斥
  (会话级 advisory lock，锁与删除同连接)的分批过期清理，cutoff 轮初以数据库时钟冻结、经 ctid
  子查询沿 `(consumer_name, processed_at_ms)` 索引从最老标记开始有界批删除；策略视界约束与
  轮次报告见 `nainbox-core` README。完整墙上时间预算覆盖取连接、取锁、删除、统计和解锁；轮次拒绝
  任一数据库 driver 的环境事务；取消、超时或解除锁未获确认时关闭物理连接，不把可能持锁的
  session 放回池。
