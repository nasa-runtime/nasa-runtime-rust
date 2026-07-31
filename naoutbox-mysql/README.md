# naoutbox-mysql

`naoutbox-mysql` 提供同事务 Outbox 写入和单 owner 轮询 dispatcher。写侧通过 `natx` 感知 ambient
事务；投递侧使用绑定 MySQL session 的 advisory claim，保证同一数据库同一时刻只有一个 dispatcher。

本 adapter 当前没有独立门面 feature；基础设施集成层直接依赖 adapter，业务事务仍从门面进入：

```toml
[dependencies]
nasa = { version = "1", features = ["tx"] }
naoutbox-core = { version = "1" }
naoutbox-mysql = { version = "1" }
```

```rust
use naoutbox_core::OutboxEvent;
use naoutbox_mysql::MySqlOutbox;
use nasa::tx::transactional;

#[transactional]
async fn create_order(order_id: i64) -> anyhow::Result<()> {
    insert_order(order_id).await?;
    MySqlOutbox::new()
        .append_transactional(&OutboxEvent::new(
            "Order",
            order_id.to_string(),
            "OrderCreated",
            serde_json::to_vec(&order_id)?,
        ))
        .await?;
    Ok(())
}
```

dispatcher 通过 `dispatch_batch(publisher, limit)` 保序发布。选择
`dispatch_batch_with_dlt(publisher, limit, max_attempts)` 时，毒丸达到阈值后移入死信，后续事件
可以继续；这会明确放弃毒丸位置上的严格局部顺序。

## YML 配置

本 crate 不新增配置根，复用 `database:` / `datasources:`。轮询间隔、批量上限和死信阈值由
受监督业务任务配置。

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 16
```

生产环境由 migration 创建和升级 `outbox_event`；`ensure_schema` 只用于本地自举。

## 主要边界

- 关键双写使用 `append_transactional`；普通 `append` 在事务外会走独立提交。
- dispatcher 不能在业务 ambient 事务内运行。
- 正常完成显式释放 advisory claim；错误、panic 或取消通过关闭 session 兜底释放。
- 成功只按本轮精确 ID 标记，不能用范围更新跨越并发或死信空洞。
- 下游仍必须按 `event_id` 去重。
