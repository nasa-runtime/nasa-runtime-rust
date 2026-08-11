# naoutbox-core

`naoutbox-core` 定义 Outbox 事件、写入端、下游发布端和保序至少一次投递算法。事件字段包含
全局 `event_id`、聚合类型/ID、事件类型、payload 和可选 W3C `traceparent`。

业务应用通常通过门面的 `outbox` feature 使用；实现独立 adapter 时才直接依赖本 crate：

```toml
[dependencies]
nasa = { version = "1", features = ["outbox"] }
```

```rust
use nasa::outbox::OutboxEvent;

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

`OutboxPublishError` 的类别属于发布端合同：`Terminal` 只用于身份、路由或协议等确定性拒绝，允许由
已经批准的死信预算裁决；`Transient` 覆盖网络失败、deadline、断连、回包丢失和远端结果不确定，必须
保留重投且不消耗死信预算。dispatcher 不解析错误文本猜测类别。Saga command/result 默认使用
`Block`，首个未确认事件停止同一通道后续投递，避免把瞬态失败当成可越过的毒丸。

## YML 配置

本 crate 不读取 yml。topic 路由、批量上限、轮询间隔和死信策略归具体 dispatcher；事件 schema
归业务合同。

## 主要边界

- 至少一次投递允许重复，不允许静默丢失。
- `aggregate_id` 常用于分区 key，但不能代替唯一 `event_id`。
- payload 应有业务大小上限；敏感正文不能进入错误和日志。
- 取消正在进行的内存投递时，未确认批次会恢复到队首。
- gRPC/HTTP 等 request/response 发布端只有收到明确 `Committed`/`Duplicate` 收据才能返回成功；
  response 丢失属于结果不确定。
