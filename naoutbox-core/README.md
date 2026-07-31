# naoutbox-core

`naoutbox-core` 定义 Outbox 事件、写入端、下游发布端和保序至少一次投递算法。事件字段包含
全局 `event_id`、聚合类型/ID、事件类型、payload 和可选 W3C `traceparent`。

本 crate 是 adapter 集成合同，当前没有独立门面 feature；实现 dispatcher 时直接依赖：

```toml
[dependencies]
naoutbox-core = { version = "1" }
```

```rust
use naoutbox_core::OutboxEvent;

let event = OutboxEvent::new(
    "Order",
    order_id.to_string(),
    "OrderCreated",
    serde_json::to_vec(&payload)?,
)
.with_traceparent(traceparent);
```

持久业务代码通常使用 `naoutbox-mysql`；`InMemoryOutbox` 只适合允许进程退出后丢失待投事件的场景。

## 发布合同

实现 `OutboxPublisher` 后，`dispatch_in_order` 按输入顺序逐条发布，遇首个失败立即停止。成功前缀
由调用方标记或移除，失败项和后缀留到下一轮，因此消费者必须按 `event_id` 幂等去重。

## YML 配置

本 crate 不读取 yml。topic 路由、批量上限、轮询间隔和死信策略归具体 dispatcher；事件 schema
归业务合同。

## 主要边界

- 至少一次投递允许重复，不允许静默丢失。
- `aggregate_id` 常用于分区 key，但不能代替唯一 `event_id`。
- payload 应有业务大小上限；敏感正文不能进入错误和日志。
- 取消正在进行的内存投递时，未确认批次会恢复到队首。
