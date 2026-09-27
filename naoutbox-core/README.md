# naoutbox-core

`naoutbox-core` 定义 Outbox 事件、同步进程内 writer、下游发布端、保序至少一次算法，以及
MySQL/PostgreSQL 持久 adapter 共用的异步角色合同。事件字段包含全局 `event_id`、聚合类型/ID、
事件类型、payload、受信租户归因和可选 W3C `traceparent`。

业务应用通常通过门面的 `outbox` feature 使用；实现独立 adapter 时才直接依赖本 crate：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["outbox"] }
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

持久业务代码按 datasource driver 选择 `naoutbox-mysql` 或 `naoutbox-pgsql`；`InMemoryOutbox` 只适合
允许进程退出后丢失待投事件的场景。

## 持久 adapter 合同

既有 `OutboxWriter` 是无错误的同步进程内接口，签名保持不变。数据库 adapter 通过加法角色 trait 暴露：

- `DurableOutboxAppend`：普通 append、强制事务 append 和受信租户配额 append；
- `DurableOutboxDispatch`：owner claim、成功前缀、死信和 lane 投递；
- `DurableOutboxQuota`：租户在飞账本读取与事务内对账；
- `DurableOutboxRetention`：收据门禁的有界归档与清理；
- `DurableOutboxWakeup`：只在明确提交后按 driver、datasource_ref 与 lane 推进的进程内唤醒代际。

`DurableOutbox` 是上述能力的组合合同。具体 adapter 继续保留同名 inherent methods，业务可以渐进迁移，
受管运行时则无需按数据库类型匹配方法名。

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
