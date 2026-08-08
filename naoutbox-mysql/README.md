# naoutbox-mysql

`naoutbox-mysql` 提供同事务 Outbox 写入和单 owner 轮询 dispatcher。写侧通过 `natx` 感知 ambient
事务；投递侧使用绑定 MySQL session 的 advisory claim，保证同一数据库同一时刻只有一个 dispatcher。
事务明确提交后会发送进程内代际通知，受管 dispatcher 立即尝试投递；配置的轮询周期只负责跨进程写入、
进程重启和通知合并后的持久化兜底。

业务通过 `nasa` 门面开启写侧与受管 dispatcher：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "outbox"] }
```

```rust
use nasa::outbox::{MySqlOutbox, OutboxEvent};
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

不使用 Application 时，dispatcher 可通过 `dispatch_batch(publisher, limit)` 保序发布。选择
`dispatch_batch_with_dlt(publisher, limit, max_attempts)` 时，毒丸达到阈值后移入死信，后续事件
可以继续；这会明确放弃毒丸位置上的严格局部顺序。

Application 服务声明 `#[nasa::application("outbox")]`，并在启动钩子提交
`OutboxApplicationPlan::new(publisher)` 后，由组件持续投递、退避、发布 readiness 和反向停机。Saga
声明会隐式加入同一组件，不需要再写 `"outbox"`。

## YML 配置

持久化复用 `database:` / `datasources:`；Application dispatcher 使用独立 `outbox:` 预算段。

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 16

outbox:
  poll_interval_ms: 500
  error_backoff_ms: 1000
  operation_timeout_ms: 5000
  batch_size: 100
  failure_threshold: 3
```

生产环境按 [Outbox 数据库迁移](migrations/README.md) 创建和升级 `outbox_event`；`ensure_schema` 只用于
受控自举。当前投递索引为 `(dispatched, dead, id)`，死信计数索引为 `(dead, id)`；两者必须在开放指标
抓取和 dispatcher 前完成。

## 主要边界

- 关键双写使用 `append_transactional`；普通 `append` 在事务外会走独立提交。
- 提交通知只在最外层事务确认成功后产生；回滚、rollback-only 和提交失败不会唤醒投递。
- 进程内通知不替代数据库事实，dispatcher 即使漏通知也会在下一轮重新读取待投递行。
- dispatcher 不能在业务 ambient 事务内运行。
- 正常完成显式释放 advisory claim；错误、panic 或取消通过关闭 session 兜底释放。
- 成功只按本轮精确 ID 标记，不能用范围更新跨越并发或死信空洞。
- 下游仍必须按 `event_id` 去重。
- 待投递与死信计数必须命中覆盖索引；投递批次必须通过 `idx_dispatchable` 只定位真实候选，再按主键
  回表读取 payload 等事件列，不能让 Prometheus 抓取或批次领取退化为全表扫描。
- `dispatch_batch` 按整张表的 `id` 串行前移；首个永久失败事件会阻塞其后的所有事件类型。
- 同表复用时唯一 publisher 必须覆盖全部事件类型；不能共享发布合同的领域应使用独立事务数据库和独立
  Outbox，避免审计或辅助事件扩大 Saga command/result 的停摆半径。
