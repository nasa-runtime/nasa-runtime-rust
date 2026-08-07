# nafka

`nafka` 是 NASA Rust 运行时的 Kafka 实现组件，提供类型化发布与消费、属性宏消费者收集、
Auto/Manual Ack、消费组控制、DLT、独立 producer lane，以及面向 `naws` 的借用式少拷贝
passthrough。

业务项目统一通过 `nasa::kafka` 使用，不直接依赖实现 crate：

```toml
[dependencies]
nasa = { version = "1", features = ["kafka"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

相关文档：

- [naws Kafka 集成](../naws/README.md)：WebSocket、socket.io 与 Kafka passthrough。
- [napp 受管生命周期](../napp/README.md#kafka-受管模式)：组件配置、Ready、健康和两段停机。

## 能力概览

- JSON 与 ProtocolBytes 类型化发布/消费；二进制支持 `JSON_BYTES`、`VARINT_TLV`、
  `BITPACK_TLV`，`FAST_FIXED` 暂不开放。
- `#[kafka_consumer]` 收集无状态 async free function；trait 注册有状态消费者。
- route 级 `AckMode::Auto` / `AckMode::Manual`，统一由 owner 推进连续安全提交水位。
- publish、raw publish、tombstone、批发布、key、partition、timestamp、有序重复 header。
- 默认 lane 与命名 lane 物理隔离；每个 lane 独立 producer、queue、admission 和 delivery observer。
- 默认 group、命名 group、进程实例广播 group和固定分区 assign。
- 有界重试、DLT、控制命令、ready 门禁、健康状态和可注入指标出口。
- `PassthroughConsumer` 同步借用 Kafka payload/header，支持 Kafka 到本地 socket outbox 的少拷贝路径。
- 公共 API 不暴露 `rdkafka` 类型。

## Feature

| `nasa` feature | 用途 |
|---|---|
| `kafka` | 基础 Kafka 能力与 `#[kafka_consumer]` |
| `kafka-tls` | TLS、SASL_SSL、SCRAM；静态内嵌 OpenSSL |
| `kafka-gssapi` | Kerberos/GSSAPI，同时包含 TLS 支持 |
| `kafka-zstd` | Zstandard 压缩 |
| `ws-kafka` | `nasa::ws` 与 Kafka control/data plane 集成 |

## 快速开始

下面示例使用属性宏收集消费者。`client` 必须与 `KafkaConfig.client_name` 一致；
`start()` 只表示本地 owner 已构造，接流量前再用 `await_group_ready()` 等待真实 broker assignment。

```rust
use std::time::{Duration, Instant};

use nasa::kafka::{
    kafka_consumer, AdminConfig, BehaviorConfig, ConsumerConfig, KafkaConfig, KafkaProxy,
    ProducerConfig, ReadyRequirement, Result, SecurityConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct OrderCreated {
    id: String,
}

#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-worker",
    client = "order-service"
)]
async fn consume_order(message: OrderCreated) -> Result<()> {
    println!("order={}", message.id);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let kafka = KafkaProxy::connect(KafkaConfig {
        client_name: "order-service".into(),
        bootstrap_servers: "127.0.0.1:9092".into(),
        group_id: Some("order-service".into()),
        client_id_prefix: Some("nasa".into()),
        producer: ProducerConfig::default(),
        consumer: ConsumerConfig::default(),
        admin: AdminConfig::default(),
        behavior: BehaviorConfig::default(),
        security: SecurityConfig::default(),
        properties: Default::default(),
    })?;

    let _consumers = kafka
        .consumers()
        .with_collected()?
        .start()
        .await?;

    kafka
        .await_group_ready(
            "order-worker",
            ReadyRequirement::AssignedTopics(vec!["orders".into()]),
            Instant::now() + Duration::from_secs(30),
        )
        .await?;

    let order = OrderCreated { id: "O-1001".into() };
    kafka
        .publish("orders", &order)
        .event("created")
        .key(&order.id)
        .send()
        .await?;

    kafka.shutdown().await
}
```

配置字段使用 `snake_case`，可以直接嵌入应用自己的配置结构：

```yaml
kafka:
  client_name: order-service
  bootstrap_servers: 127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096
  group_id: order-service
  client_id_prefix: nasa
  producer:
    acks: all
    retries: 3
    enable_idempotence: true
    max_in_flight_requests_per_connection: 1
  behavior:
    max_consume_attempts: 3
    dead_letter_topic_suffix: .DLT
    dead_letter_required: true
```

框架固定关闭 `enable.auto.commit` 和 `enable.auto.offset.store`。这两个值属于正确性不变量，
不能通过 raw properties 覆盖。

## Application 受管模式

Service 同时开启 `application` 与 `kafka` 后，可以把 Kafka 声明成正式容器组件。此时业务不再手写
`connect`、registry start、broker Ready 或 shutdown：

```toml
nasa = { version = "1", features = ["application", "kafka", "web"] }
```

```rust
#[nasa::application("kafka", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_kafka("order-service", |registry| {
        registry.register(OrderConsumer::new())
    })?;
    Ok(())
}
```

受管配置在数据面字段之外增加严格 `container` 段；单 client 使用 `kafka`，多 client 使用互斥的
`kafkas.<name>`。完整字段、ReadyRule 和顺序见 [napp README](../napp/README.md)。属性 consumer 的
`client` 必须命中一个 `consumers: collected` client；容器在 UserHook 后一次性冻结 registry，并在对外
Ready 前等待真实 join/assignment 或 producer metadata。

业务通过 `app.kafka(name)` 取得 `KafkaHandle`。它可返回 `ProducerLane`、健康/assignment/position，
并开放 pause/resume、seek、subscribe/unsubscribe、restart 和只读 metadata，但不会暴露原始
`KafkaProxy`、`consumers()`、admin 写操作或关闭权。独立批处理与运维工具继续使用本 README 其他章节的
显式 `KafkaProxy` 模式，不声明 Application 的 `"kafka"` 组件。

### 两段停机 API

容器使用接收绝对 deadline 的入口，保证所有 action 共享同一预算：

```rust
proxy.stop_consumers_until(deadline).await?; // 停 owner，producer 仍可用于退出收尾
proxy.shutdown_until(deadline).await?;       // 关闭准入、flush lane、关闭 admin
```

`stop_consumers_until` 与 consumer start、全量 shutdown 共用生命周期门禁；只有全部 owner 确认退出后才清
group 表、指标和静态收集租约。超时会保留这些所有权，后续 `shutdown_until` 可以继续收尾。成功 drain 后
再执行 final shutdown 会把空 group 表视为正常路径，只处理 producer/admin；两者及其并发重复调用均幂等。
独立模式的 `shutdown()` 保持兼容，它用 `behavior.shutdown_timeout_ms` 计算一次绝对 deadline 后委托
`shutdown_until`。

## 消费者使用场景

消费者按 `(group, topic, event)` 路由。producer 未显式设置 `event` 时使用 `DEFAULT`；consumer 的
`event()` 或宏中的 `event` 必须与消息 header 一致。同一 group 内同一个 `(topic, event)` 只能注册一个
handler，注册表在 `start()` 前完成冲突检查并冻结。

| 场景 | 推荐入口 | 消息所有权 | 确认方式 |
|---|---|---|---|
| 无状态单条业务消费 | `#[kafka_consumer] async fn(T)` | 拥有型 payload | Auto |
| 需要 key/header/offset | `#[kafka_consumer] async fn(KafkaRecord<T>)` | 拥有型 record | Auto/Manual |
| 无状态批消费 | `async fn(Vec<T>)` / `Vec<KafkaRecord<T>>` | 拥有型连续 run | Auto/Manual |
| 有依赖注入 | `SingleConsumer` / `BatchConsumer` | 拥有型 record | Auto/Manual |
| 每个实例都要收到 | `broadcast` / `GroupSpec::Broadcast` | 拥有型 record | Auto/Manual |
| 固定分区回放或运维任务 | `KafkaProxy::assign` | 拥有型单条 record | Auto/Manual |
| Kafka → socket 热路径 | `PassthroughConsumer` | 同步借用 record | disposition/Manual |

### 1. 属性宏：单条 payload

最简场景只关心解码后的业务对象。默认使用 `AckMode::Auto`：函数只有返回 `Ok(())` 才确认。

```rust
use nasa::kafka::prelude::*;

#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-worker",
    client = "order-service"
)]
async fn consume_order(message: OrderCreated) -> Result<()> {
    save_order(&message).await?;
    Ok(())
}
```

`save_order()` 返回错误时直接 `?` 即可；handler 的 `Err` 不会产生 ack，也不会推进 offset。
一个 handler 可以声明多个 topic，例如 `topics = ["orders", "orders-priority"]`；两个 topic 使用相同
message codec、event 和处理函数，实际来源通过 `KafkaRecord.ctx.topic` 区分。

### 2. 属性宏：单条完整 record

需要 topic、partition、offset、timestamp、key、重复 header 或 passthrough 上下文时使用
`KafkaRecord<T>`：

```rust
#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "audit",
    client = "order-service"
)]
async fn audit_order(record: KafkaRecord<OrderCreated>) -> Result<()> {
    let traceparent = record
        .ctx
        .headers
        .last("traceparent")
        .and_then(|header| header.value.as_deref());

    audit(
        &record.value,
        &record.ctx.topic,
        record.ctx.partition,
        record.ctx.offset,
        record.ctx.key.as_deref(),
        traceparent,
    )
    .await?;
    Ok(())
}
```

`KafkaHeaders` 不是 Map：`iter()` 保留原始顺序，`last(name)` 取最后一个同名值，`all(name)` 遍历
全部重复项；header value 的 `None`、`Some(&[])` 和非空字节是三种不同事实。
`record.topic_partition()` 返回自有 `Tp`；只想拆出 payload 和上下文、不保留确认能力时使用
`let (value, ctx) = record.into_parts()`。

### 3. 属性宏：批消费

批参数必须直接写成下面两种规范形态：

```rust
#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-index",
    client = "order-service"
)]
async fn payload_batch(messages: Vec<OrderCreated>) -> Result<()> {
    write_index_batch(&messages).await?;
    Ok(())
}

#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-audit",
    client = "order-service"
)]
async fn record_batch(records: Vec<KafkaRecord<OrderCreated>>) -> Result<()> {
    for record in &records {
        println!("partition={} offset={}", record.ctx.partition, record.ctx.offset);
    }
    write_audit_batch(&records).await?;
    Ok(())
}
```

一次回调拿到的是同一 route、同一 partition 上按 offset 排列的连续 run，大小受
`max_batch_records` 和 `max_batch_bytes` 限制。v1 不跨 poll 等待凑满 N/T；当前 poll 有多少安全连续记录
就派发多少。Auto 批回调返回 `Err`、panic 或 timeout 时，整次 run 都不确认。

### 4. 属性宏约束与显式激活

宏支持的四种签名为 `T`、`KafkaRecord<T>`、`Vec<T>` 和 `Vec<KafkaRecord<T>>`：

```rust
use nasa::kafka::prelude::*;

// 假设 OrderCreated、PaymentCreated 已实现 serde 解码，persist 是业务持久化函数。

#[kafka_consumer(topics = ["orders"], event = "created")]
async fn payload_single(message: OrderCreated) -> Result<()> {
    Ok(())
}

#[kafka_consumer(topics = ["orders"], event = "created")]
async fn record_single(record: KafkaRecord<OrderCreated>) -> Result<()> {
    Ok(())
}

#[kafka_consumer(topics = ["orders"], event = "created", group = "indexer")]
async fn payload_batch(messages: Vec<OrderCreated>) -> Result<()> {
    Ok(())
}

#[kafka_consumer(topics = ["payments"], event = "created", ack = "manual")]
async fn record_batch(records: Vec<KafkaRecord<PaymentCreated>>) -> Result<()> {
    for record in &records {
        persist(&record.value).await?;
        record.ack()?;
    }
    Ok(())
}
```

约束如下：

- 只能标注 async free function，不接受 receiver 或泛型函数。
- `topics` 至少一个；`topics`、`event`、`group`、`client` 不接受首尾空白。
- `group` 与 `broadcast` 互斥。
- `ack` 只允许 `auto` 或 `manual`；Manual 必须使用 `KafkaRecord<T>` 形态。
- 批量参数直接写 `Vec<T>` 或 `Vec<KafkaRecord<T>>`，不要用类型别名表达批形态。
- 宏只负责静态发现；必须显式调用 `with_collected()` 或 `with_collected_for()` 才会激活。

`client` 用于在同一进程存在多个 `KafkaProxy` 时选择静态消费者集合：

```rust
let primary_runtime = primary
    .consumers()
    .with_collected_for("order-service")?
    .start()
    .await?;

let audit_runtime = audit_cluster
    .consumers()
    .with_collected_for("audit-service")?
    .start()
    .await?;
```

`with_collected()` 等价于使用当前 `KafkaConfig.client_name`。宏发现不会隐式连接 broker、启动线程或
开始消费；真正启动点始终是 `start().await`。同一个 collected client 在同一进程内只能被一个活动 runtime
持有；重复激活会 fail-fast，前一个 runtime 完成 shutdown 并释放 activation lease 后才能再次启动。

### 5. group：默认、命名与广播

三种 group 语义：

```rust
// 未写 group/broadcast：使用 KafkaConfig.group_id。
#[kafka_consumer(topics = ["orders"], event = "created")]
async fn default_group(message: OrderCreated) -> Result<()> {
    Ok(())
}

// 所有实例竞争同一个稳定 group，消息由 Kafka 分摊。
#[kafka_consumer(topics = ["orders"], event = "created", group = "order-worker")]
async fn named_group(message: OrderCreated) -> Result<()> {
    Ok(())
}

// 每个进程实例派生独占 group，因此每个实例都会收到完整消息流。
#[kafka_consumer(topics = ["sys-config"], event = "reload", broadcast = "config-cache")]
async fn broadcast_group(message: ConfigChanged) -> Result<()> {
    reload_config(message).await?;
    Ok(())
}
```

裸 `broadcast` 会以 module path + function name 作为 scope；多个 handler 需要共享稳定广播语义时使用
`broadcast = "scope"`。`group` 与 `broadcast` 不能同时出现。
trait 消费者的等价写法是 `fn group(&self) -> GroupSpec { GroupSpec::Broadcast("scope".into()) }`。
使用 `GroupSpec::Default` 或宏中省略 `group` 时，`KafkaConfig.group_id` 必须存在；命名 group 与广播 group
不依赖这个默认值。

### 6. 有状态单条消费者

需要依赖注入时，实现 `SingleConsumer` 或 `BatchConsumer`，再通过 `register()` / `register_batch()`
注册实例：

```rust
use std::sync::Arc;
use nasa::kafka::{GroupSpec, KafkaRecord, Result, SingleConsumer};

// 假设 Db 是业务数据库句柄，OrderCreated 已实现 serde 解码。
struct OrderConsumer {
    db: Arc<Db>,
}

impl SingleConsumer for OrderConsumer {
    type Message = OrderCreated;

    fn topics(&self) -> Vec<String> { vec!["orders".into()] }
    fn event(&self) -> String { "created".into() }
    fn group(&self) -> GroupSpec { GroupSpec::Named("order-worker".into()) }

    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()> {
        self.db.save(&record.value).await
    }
}
```

```rust
let runtime = kafka
    .consumers()
    .with_collected()? // 可以和宏消费者同时注册
    .register(OrderConsumer { db: db.clone() })?
    .start()
    .await?;

println!("resolved groups: {:?}", runtime.groups());
```

有状态消费者需要 Manual Ack 时覆盖 `ack_mode()`；确认规则与宏消费者完全相同：

```rust
use nasa::kafka::{AckMode, GroupSpec, KafkaRecord, Result, SingleConsumer};

struct PaymentConsumer {
    db: Arc<Db>,
}

impl SingleConsumer for PaymentConsumer {
    type Message = PaymentCreated;

    fn topics(&self) -> Vec<String> { vec!["payments".into()] }
    fn event(&self) -> String { "created".into() }
    fn group(&self) -> GroupSpec { GroupSpec::Named("settlement".into()) }
    fn ack_mode(&self) -> AckMode { AckMode::Manual }

    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()> {
        self.db.save(&record.value).await?;
        record.ack()?;
        Ok(())
    }
}
```

### 7. 有状态批消费者

```rust
use nasa::kafka::{BatchConsumer, GroupSpec, KafkaRecord, Result};

struct RebuildConsumer {
    index: Arc<SearchIndex>,
}

impl BatchConsumer for RebuildConsumer {
    type Message = OrderCreated;

    fn topics(&self) -> Vec<String> { vec!["orders".into()] }
    fn event(&self) -> String { "created".into() }
    fn group(&self) -> GroupSpec { GroupSpec::Named("search-index".into()) }

    async fn consume_batch(
        &self,
        records: Vec<KafkaRecord<Self::Message>>,
    ) -> Result<()> {
        self.index.replace_batch(records.iter().map(|record| &record.value)).await?;
        Ok(())
    }
}

let runtime = kafka
    .consumers()
    .register_batch(RebuildConsumer { index })?
    .start()
    .await?;
```

### 8. Auto/Manual Ack

`AckMode` 是 route 级业务确认策略，不是 Kafka 客户端的 auto commit 开关。两种模式最后都进入
同一个 partition 连续安全水位，由 owner 批量提交 broker offset。

| 模式 | handler 结果 | 本次记录是否安全确认 |
|---|---|---|
| Auto | `Ok(())` | 是 |
| Auto | `Err`、panic、timeout/cancel | 否 |
| Manual | 已 `ack()` 且最终 `Ok(())` | 是，仅计算连续已确认前缀 |
| Manual | 未 `ack()` 但返回 `Ok(())` | 否 |
| Manual | 已 `ack()` 后返回 `Err`、panic、timeout/cancel | 否，临时确认作废 |

Manual Ack 推荐写法：先完成具有业务意义的持久化，再同步调用 `ack()`，最后返回 `Ok(())`。
`ack()` 只记录本次回调内的临时确认，不访问 broker，也不能确认任意 topic/partition/offset。
不要把 `AckToken` 移交给后台任务后让 handler 提前返回。

```rust
#[kafka_consumer(
    topics = ["payments"],
    event = "created",
    group = "settlement",
    ack = "manual"
)]
async fn settle(record: KafkaRecord<PaymentCreated>) -> Result<()> {
    persist(&record.value).await?;
    record.ack()?;
    Ok(())
}
```

需要取得 payload、上下文和 token 的所有权时使用 `into_manual_parts()`，但 token 仍只在当前 handler
invocation 内有效：

```rust
async fn settle_owned(record: KafkaRecord<PaymentCreated>) -> Result<()> {
    let (payment, ctx, ack) = record.into_manual_parts()?;
    persist_with_source(&payment, &ctx).await?;
    ack.ack()?;
    Ok(())
}
```

Auto route 调用 `record.ack()` 或 `into_manual_parts()` 会返回 `NafkaError::AckNotManual`，不会改变 offset。

批消费允许非连续 ack，但只有从本批第一条开始的连续前缀能够推进提交水位；空洞之后的 ack
只作为审计信息保留，不能越过未确认记录。

Manual 批消费全部成功时可以使用 `AckBatchExt::ack_all()`：

```rust
use nasa::kafka::{AckBatchExt, AckMode, BatchConsumer, KafkaRecord, Result};

impl BatchConsumer for SettlementBatchConsumer {
    type Message = PaymentCreated;

    fn topics(&self) -> Vec<String> { vec!["payments".into()] }
    fn event(&self) -> String { "created".into() }
    fn ack_mode(&self) -> AckMode { AckMode::Manual }

    async fn consume_batch(
        &self,
        records: Vec<KafkaRecord<Self::Message>>,
    ) -> Result<()> {
        persist_all(&records).await?;
        records.ack_all()?;
        Ok(())
    }
}
```

如果只确认下标 `0、1、3`，安全前缀只到 `1`；下标 `2` 是空洞，`3` 不能被提交越过。即使已调用
`ack()`/`ack_all()`，后续返回 `Err`、panic 或 timeout 时，本次所有临时确认仍全部作废。

### 9. ProtocolBytes 消费者

业务类型通过 `Proto<T>` 选择二进制 codec，并用 `ProtoMode` 把类型固定到一种 wire mode。下面使用
`ws-kafka` feature 中重导出的 `Mode`/`WireCodec`；纯 JSON 消费者不需要这些类型。

```rust
use nasa::kafka::{kafka_consumer, KafkaRecord, Proto, ProtoMode, Result};
use nasa::ws::{Mode, WireCodec};

struct BitpackOrder(OrderWire);

impl WireCodec for BitpackOrder {
    fn encode(&self, mode: Mode) -> nasa::ws::proto::Result<Vec<u8>> {
        self.0.encode(mode)
    }

    fn decode(mode: Mode, bytes: &[u8]) -> nasa::ws::proto::Result<Self> {
        OrderWire::decode(mode, bytes).map(Self)
    }
}

impl ProtoMode for BitpackOrder {
    const MODE: Mode = Mode::BitpackTlv;
}

#[kafka_consumer(
    topics = ["orders-binary"],
    event = "created",
    group = "binary-worker"
)]
async fn consume_binary(record: KafkaRecord<Proto<BitpackOrder>>) -> Result<()> {
    handle_wire_order(&record.value.0.0).await?;
    Ok(())
}
```

producer 和 consumer 必须对同一业务类型使用相同 mode；nafka 会通过 codec/mode header 校验，未知或不匹配
的 mode 不会被猜测解码。可用 mode 为 `JsonBytes`、`VarintTlv`、`BitpackTlv`。

### 10. 同步借用式 passthrough

`PassthroughConsumer` 用于 payload 已经是最终业务字节、不希望先反序列化成拥有型对象的路径：

```rust
use nasa::kafka::{
    BorrowedKafkaRecord, GroupSpec, InvalidRecordReason, PassthroughConsumer,
    PassthroughDisposition, PassthroughFailure, PassthroughRetryReason,
};

struct RawOutboxAdapter {
    outbox: LocalOutbox,
}

impl PassthroughConsumer for RawOutboxAdapter {
    fn topics(&self) -> Vec<String> { vec!["socket-data".into()] }
    fn event(&self) -> String { "NAWS_PUSH".into() }
    fn group(&self) -> GroupSpec { GroupSpec::Named("socket-node-a.data".into()) }

    fn consume_borrowed(
        &self,
        record: BorrowedKafkaRecord<'_>,
    ) -> std::result::Result<PassthroughDisposition, PassthroughFailure> {
        let payload = record.payload.ok_or_else(|| {
            PassthroughFailure::invalid(
                InvalidRecordReason::MissingPayload,
                "data topic 不允许 tombstone",
            )
        })?;

        let queued = self.outbox.enqueue_borrowed(payload).map_err(|_| {
            PassthroughFailure::retryable(
                PassthroughRetryReason::SocketEnqueue,
                "本地 outbox 暂时不可入队",
            )
        })?;
        Ok(PassthroughDisposition::Handled { queued })
    }
}

let runtime = kafka
    .consumers()
    .register_passthrough(RawOutboxAdapter { outbox })?
    .start()
    .await?;
```

`LocalOutbox` 是示例中的业务同步适配层；生产 WebSocket 集群应直接使用 `nasa::ws::kafka` 提供的
`HeaderPassthroughAdapter` 和 `WsKafkaRuntime`。借用 record 为 `!Send + !Sync`，不能放入 async task、线程或
channel。回调必须同步返回强类型结果：

- `Handled`：全部目标已进入本地有界 outbox。
- `Skipped`：无本地目标、节点不匹配、回环或旧来源等安全跳过。
- `BestEffortDropped`：显式 best-effort 下允许部分丢弃。
- `PassthroughFailure::retryable/invalid/dead_letter/halt/fatal`：分别进入唯一对应的框架状态机。

普通 passthrough 默认 Auto，成功 disposition 就产生安全结果。自定义 adapter 也可以返回
`AckMode::Manual` 并在同步回调内调用 `record.ack()`；Kafka→naws 的标准 adapter 使用 disposition 表达本地
outbox 决策，不需要业务 Manual token。这里的确认边界是“本地 outbox 已完成入队决策”，不是客户端已经收到。

### 11. 无匹配 route、坏消息与 tombstone

这三类记录不会按普通业务失败重试：

- `(topic, event)` 没有注册 route：按 `unmatched_policy` 执行 `skip`、`dead_letter` 或 `halt`。
- event 非 UTF-8、未知 codec、mode 不匹配或解码失败：按 `invalid_record_policy` 执行
  `dead_letter` 或 `halt`，不调用业务 handler，也不消耗 `max_consume_attempts`。
- typed route 收到 tombstone：默认 `SafeSkip`，不调用反序列化和业务 handler，但仍受 partition 连续水位约束；
  passthrough route 会收到 `payload: None`，由 adapter 明确决定。socket data adapter 将其分类为
  `MissingPayload`。

需要消费 compacted topic 当前状态时，普通 value 继续使用 typed consumer；如果业务确实需要观察删除事件，
应让 producer 另发有 payload 的业务删除事件，或实现明确处理 `payload: None` 的 passthrough adapter。不要把
typed tombstone 当成 `T` 的某个默认值。

### 12. 固定分区 assign

回放、迁移和运维任务可以绕过 group subscription，固定消费指定分区：

```rust
use nasa::kafka::{StartOffset, Tp};

let assignment = kafka
    .assign(
        "order-replay",
        [Tp::new("orders", 0), Tp::new("orders", 1)],
        StartOffset::Timestamp(replay_from_epoch_ms),
        OrderConsumer { db: db.clone() },
    )
    .await?;

println!("partitions={:?}", assignment.partitions().await?);
println!("health={:?}", assignment.health().await?);

assignment.stop().await?;
```

起点支持：

- `Earliest`：当前 retention 范围内最早位置。
- `Latest`：启动时末尾，只消费之后的新消息。
- `Timestamp(epoch_ms)`：第一条 timestamp 不小于目标时间的记录，查不到时到末尾。
- `Offset(offset)`：显式 offset。
- `Committed`：沿用 group 已提交位置；无提交时回退 `auto_offset_reset`。

assign group 与 subscribe group 共用同一命名空间，不能重名；`AssignmentHandle::stop()` 幂等，Drop 不会
隐式停止任务。

## 生产者使用场景

生产者 API 分为类型化、ProtocolBytes、raw、tombstone 和批量五类；`KafkaProxy` 便捷方法固定使用
`default` lane，`ProducerLane` 提供相同的单条发布入口。

| 场景 | 入口 | 等待 broker 结果 | payload 处理 |
|---|---|---|---|
| JSON 业务对象 | `publish(topic, &value)` | `send()` 是；`fire()` 否 | serde JSON |
| ProtocolBytes | `publish(topic, &Proto(value))` | 可选 | 固定二进制 mode |
| 已编码原始字节 | `publish_raw(topic, bytes)` | 可选 | 不经过 serde |
| compacted topic 删除标记 | `tombstone(topic, key)` | 可选 | Kafka null value |
| 有序批量业务对象 | `publish_batch(topic, items)` | 是，逐条 | 保留成功前缀 |
| control/data 隔离 | `producer_lane(name)` | 可选 | 独立物理 producer |

### 1. 类型化 JSON：发送并等待 delivery

实现 `serde::Serialize` 的类型自动走 JSON codec：

```rust
let delivery = kafka
    .publish("orders", &order)
    .event("created")
    .key(&order.id)
    .timestamp(epoch_ms)
    .header("traceparent", Some(traceparent.as_bytes()))
    .send()
    .await?;

println!(
    "broker partition={} offset={}",
    delivery.partition,
    delivery.offset,
);
```

builder 字段语义：

- `event(name)`：写入 `X-Nasa-Event`；不设置时使用 `DEFAULT`。
- `key(string)`：参与 Kafka 分区；默认 `murmur2_random` 与常见跨语言 producer 对字符串 key 的结果一致。
- `partition(n)`：显式指定非负分区，设置后不再由 key 选择。
- `timestamp(epoch_ms)`：设置 Kafka record timestamp。
- `header(name, value)`：追加 header；允许同名重复，`None` 表示 Kafka null header value。
- `ctx(&record.ctx)`：复制消费上下文中的 passthrough Map，适合消费后继续发布。
- `passthrough(key, value)?`：追加一个可 JSON 序列化的跨服务上下文字段。

业务 header 保留顺序、重复名和 null value。`X-Nasa-*` 框架 header 由类型化 builder 生成，业务不能
通过通用 `header()` 覆盖。

### 2. `send()` 与 `fire()`

`send()` 编码、入队并等待 delivery report，返回实际 partition/offset。调用方需要确认 broker 结果时使用：

```rust
let delivery = kafka
    .publish("orders", &order)
    .event("created")
    .key(&order.id)
    .send()
    .await?;
```

`fire()` 完成有界 observer 准入和同步入队后立即返回，后台仍会统计最终 delivery；适合 presence、通知等
不阻塞当前调用的场景：

```rust
kafka
    .publish("presence", &presence)
    .event("online")
    .key(&presence.user_id)
    .fire()?;
```

`fire() == Ok(())` 只表示消息已进入受观察的本地投递流程，不表示 broker 已确认，更不表示消费者已处理。
用 `kafka.producer_lane("default")?.stats()` 观察累计 delivery、queue 和 generation 状态；停机时
`kafka.shutdown().await` 会排空所有 lane。

### 3. 消费后继续发布并透传上下文

有状态消费者可以持有 `KafkaProxy`，把 trace 等 passthrough 上下文传给下游消息：

```rust
struct OrderPipeline {
    kafka: KafkaProxy,
}

impl SingleConsumer for OrderPipeline {
    type Message = OrderCreated;

    fn topics(&self) -> Vec<String> { vec!["orders".into()] }
    fn event(&self) -> String { "created".into() }
    fn group(&self) -> GroupSpec { GroupSpec::Named("order-pipeline".into()) }

    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()> {
        let indexed = build_index_event(&record.value)?;
        self.kafka
            .publish("order-index", &indexed)
            .event("upsert")
            .key(&record.value.id)
            .ctx(&record.ctx)
            .passthrough("source_offset", record.ctx.offset)?
            .send()
            .await?;
        Ok(())
    }
}
```

该例中下游 publish 失败会让当前 handler 返回 `Err`，因此来源记录不 ack，重投时下游可能重复收到；下游应使用
业务 key/幂等键去重。

### 4. 原始字节 `publish_raw`

payload 已经编码完成时使用 raw builder。它不会添加 codec/mode header，也不会先经过 serde 或构造中间
`Vec<u8>`：

```rust
let delivery = kafka
    .publish_raw("socket-data", payload)
    .event("cluster-data")
    .key(source_node)
    .timestamp(epoch_ms)
    .header("content-type", Some(b"application/octet-stream"))
    .send()
    .await?;
```

也可以调用 `.fire()?` 使用异步观察投递。空 slice 是合法的 `Some(&[])`，与 tombstone 的 null value 不同。
raw builder 只提供 `event/key/partition/timestamp/header`；它不提供 typed builder 的 `ctx()` 与
`passthrough()` JSON 上下文。已有编码 bytes 直接使用 raw；只有业务对象才使用 typed publish，避免把
`Vec<u8>` 误序列化成 JSON 数字数组。

Kafka→WebSocket 数据面不要手写 `X-Nasa-WS-*` header；使用 `nasa::ws::kafka::WsKafkaPublisher` 或
`KafkaClusterDataPublisher`，由安全 builder 生成路由、来源、目标节点、sink 和 message mode。

### 5. ProtocolBytes 发布

ProtocolBytes 类型需要实现 `WireCodec + ProtoMode`，发布时用透明包装 `Proto<T>`：

```rust
let wire = Proto(BitpackOrder(OrderWire::from(&order)));
let delivery = kafka
    .publish("orders-binary", &wire)
    .event("created")
    .key(&order.id)
    .send()
    .await?;
```

nafka 会自动写 `X-Nasa-Payload-Codec=protocol-bytes` 和匹配的 `X-Nasa-Payload-Mode`。同一业务类型应固定
一种 mode，consumer 使用相同的 `Proto<T>` 类型；不要在运行时根据消息内容猜 codec。

### 6. tombstone

compacted topic 需要删除某个 key 时显式发布 tombstone：

```rust
let delivery = kafka
    .tombstone("order-state", "order/O-1001")
    .event("deleted")
    .header("reason", Some(b"expired"))
    .send()
    .await?;
```

tombstone 必须有非空 key，value 固定为 Kafka null。普通 typed consumer 不会把 tombstone 交给业务反序列化；
匹配到 typed route 时默认安全跳过。tombstone builder 只提供 `event/header/send/fire`，不接受 payload；同样
支持 `.fire()?`。需要观察墓碑的专用同步路径应使用明确处理 `payload: None` 的 passthrough adapter，或改用
携带业务删除对象的普通事件。

### 7. 顺序批发布

`publish_batch()` 按输入顺序逐条等待 delivery，在首个失败或结果未知处停止：

```rust
use nasa::kafka::PublishItem;

let items = orders.into_iter().map(|order| {
    let key = order.id.clone();
    PublishItem::new(order)
        .key(key)
        .event("created")
});

match kafka.publish_batch("orders", items).await {
    Ok(deliveries) => {
        println!("全部发布成功 count={}", deliveries.len());
    }
    Err(error) => {
        eprintln!(
            "成功前缀={} 首个失败下标={} 原因={}",
            error.delivered.len(),
            error.failed_index,
            error.source,
        );
    }
}
```

这是确定的顺序便利 API，不是 Kafka transaction：已确认成功的前缀不会回滚。批量项支持 payload、key、event；
需要 partition/timestamp/custom header 时逐条使用 builder。

### 8. 命名 producer lane

命名 lane 用于 control/data、实时/批处理或不同可靠性等级的物理隔离：

```rust
let control = kafka.producer_lane("control")?;
let delivery = control
    .publish_raw("socket-control", bytes)
    .key(node_id)
    .send()
    .await?;

let stats = control.stats();
control.flush().await?;
```

每个 lane 必须在 `KafkaConfig.producer.lanes` 中预先声明，运行中不能动态创建。每个 lane 都有独立 native
producer、queue、admission、observer 和 client.id；`KafkaProxy` 上的 `publish/publish_raw/tombstone/
publish_batch` 始终使用 `default` lane。

lane 还可以分别覆盖 `compression`、linger、batch size、超时、重试和 stalled-generation 重建策略。
使用 `compression: zstd` 时编译开启 `kafka-zstd`；control lane 通常保持低 linger，data lane 再按吞吐目标调优。

示例配置：

```yaml
kafka:
  producer:
    acks: all
    retries: 3
    enable_idempotence: true
    max_in_flight_requests_per_connection: 1
    lanes:
      control:
        acks: all
        enable_idempotence: true
        max_in_flight_requests_per_connection: 1
        queue_buffering_max_messages: 10000
        queue_buffering_max_kbytes: 65536
      data:
        acks: all
        enable_idempotence: true
        max_in_flight_requests_per_connection: 5
        queue_buffering_max_messages: 100000
        queue_buffering_max_kbytes: 262144
```

### 9. 发布错误分类

不要按错误字符串判断是否重试，使用 `NafkaError` variant：

```rust
use nasa::kafka::NafkaError;

match kafka.publish("orders", &order).event("created").send().await {
    Ok(delivery) => mark_sent(delivery),
    Err(NafkaError::ProducerQueueFull { queue, .. }) => {
        // 可以确认尚未入队，按容量退避后重试。
        backoff_for_queue(queue).await;
    }
    Err(NafkaError::Publish { .. }) => {
        // 确定失败；是否重试由业务幂等策略决定。
        schedule_retry(&order).await?;
    }
    Err(NafkaError::OutcomeUnknown { .. }) => {
        // 消息可能已经成功，禁止盲目重发；先按业务幂等键核对或进入人工/对账流程。
        mark_reconciliation_required(&order.id).await?;
    }
    Err(NafkaError::Lifecycle(_)) => {
        // runtime 已关闭或正在关闭，不应继续发布。
    }
    Err(error) => return Err(error),
}
```

### 10. flush 与停机

- `ProducerLane::flush().await`：只排空指定 lane。
- `KafkaProxy::flush().await`：排空当前 runtime 的全部 lane，但不停止消费者。
- `KafkaProxy::shutdown().await`：关闭外部发布准入、停止 consumer、完成 DLT 收尾、排空并关闭全部 lane。

业务正常停机使用 `shutdown()`；不要只 Drop `KafkaProxy`，也不要在 Tokio runtime 已停止后再尝试异步排空。

## 生命周期与 ready

推荐顺序：

1. `KafkaProxy::connect(config)`：本地校验配置并建立 producer/runtime 句柄。
2. `kafka.consumers()`：注册静态、trait 或 passthrough 消费者。
3. `start().await`：冻结 registry，启动每个 group 的 owner。
4. `await_group_ready()` / `await_groups_ready()`：等待真实 join 和 assignment。
5. 对外开放 HTTP/WS ingress。
6. 停机时调用 `kafka.shutdown().await`，停止 owner 并排空全部 producer lane。

可选 ready 条件：

- `ReadyRequirement::Joined`：已完成一次 join，允许竞争组处于空 assignment standby。
- `ReadyRequirement::Assigned { min_partitions }`：至少持有指定数量的分区。
- `ReadyRequirement::AssignedTopics(topics)`：每个要求的 topic 至少分配一个分区。

ready 返回的是带 assignment epoch 的时点快照；rebalance 后应以最新健康状态为准。

### 同时等待多个消费组

socket control/data 或一个服务内存在多个业务 group 时，共享同一个绝对 deadline：

```rust
let deadline = Instant::now() + Duration::from_secs(30);
let snapshots = kafka
    .await_groups_ready(
        vec![
            (
                "socket-node-a.control".into(),
                ReadyRequirement::AssignedTopics(vec!["socket-control".into()]),
            ),
            (
                "socket-node-a.data".into(),
                ReadyRequirement::AssignedTopics(vec!["socket-data".into()]),
            ),
        ],
        deadline,
    )
    .await?;
```

任一 group fatal 会立即返回；deadline 到期时错误携带所有尚未就绪 group 的最后健康快照。

### 查询 assignment、position 和健康状态

```rust
use nasa::kafka::Tp;

let group = "order-worker";
let assignment = kafka.assignment(group).await?;
let positions = kafka.position(group, assignment.clone()).await?;
let health = kafka.group_health(group).await?;

for tp in assignment {
    println!(
        "{}:{} position={:?}",
        tp.topic,
        tp.partition,
        positions.get(&tp).copied().flatten(),
    );
}
println!("state={:?} paused={:?}", health.state, health.paused);
```

`position` 是当前 owner 的消费位置；broker 已提交 offset 和 lag 使用 admin 查询：

```rust
let tps = vec![Tp::new("orders", 0), Tp::new("orders", 1)];
let committed = kafka.admin().committed_offsets(group, tps.clone()).await?;
let lag = kafka.admin().committed_lag(group, tps).await?;
```

### pause、resume 与 seek

```rust
use std::collections::BTreeMap;
use nasa::kafka::Tp;

let tp0 = Tp::new("orders", 0);

kafka.pause("order-worker", vec![tp0.clone()]).await?;
kafka.resume("order-worker", vec![tp0.clone()]).await?;

kafka.seek("order-worker", tp0.clone(), 1200).await?;
kafka.seek_to_beginning("order-worker", vec![tp0.clone()]).await?;
kafka.seek_to_end("order-worker", vec![tp0.clone()]).await?;
kafka.seek_to_committed("order-worker", vec![tp0.clone()]).await?;
kafka
    .seek_to_timestamp(
        "order-worker",
        BTreeMap::from([(tp0, replay_from_epoch_ms)]),
    )
    .await?;
```

对整个 group 暂停/恢复使用 `pause_all(group)` / `resume_all(group)`。seek 会清理目标分区的本地
progress/retry 状态；存在未完成 DLT 等冲突时会被拒绝，不能用 seek 越过未安全处理的记录。

### 动态订阅与显式恢复

```rust
kafka
    .subscribe_group(
        "order-worker",
        vec!["orders".into(), "orders-priority".into()],
    )
    .await?;

kafka.unsubscribe_group("order-worker").await?;

// 仅用于允许恢复的 Degraded/Crashed 状态，不是正常流量下的重启按钮。
kafka.restart_group("order-worker").await?;
```

动态 subscribe 只改变 topic 集合，route 表仍是在 `start()` 时冻结的；新 topic 上没有匹配 event 的记录按
`unmatched_policy` 处理。固定 assign owner 不支持 subscribe/unsubscribe。

控制命令错误需要按语义处理：

- `CommandQueueFull`：owner 正忙，稍后退避重试。
- `ControlNotApplied`：超时且已确认未执行，可以重试。
- `ControlOutcomeUnknown`：owner 已开始执行，先查询 `position()` / `assignment()` 再决定，不能盲目重发命令。
- `NoSuchGroup`：group 未启动或已经停止。

### 消费失败策略配置

```yaml
kafka:
  consumer:
    auto_offset_reset: latest
    session_timeout_ms: 30000
    heartbeat_interval_ms: 10000
    max_poll_interval_ms: 300000
  behavior:
    handler_timeout_ms: 30000
    max_consume_attempts: 3       # 0 表示无限业务重试
    consume_retry_backoff_ms: 500
    invalid_record_policy: dead_letter
    unmatched_policy: skip
    dead_letter_topic_suffix: .DLT
    dead_letter_required: true
    dlt_queue_capacity: 1024
    dlt_queue_max_bytes: 268435456
    dlt_max_in_flight: 16
    commit_interval_ms: 1000
    commit_batch_records: 1000
```

- 业务 handler `Err`/panic/timeout：按 `max_consume_attempts` 和退避策略重试，耗尽后进入 DLT/Halt。
- 受审计控制态暂不允许处理时可返回 `NafkaError::HandlerDeferred`：仍 pause/seek/退避，但不消耗
  `max_consume_attempts`；只有确有外部恢复动作的门禁可使用，普通故障不得借此无限重投。
- 确定坏格式、缺 payload、codec 不匹配：不消耗业务重试次数，直接应用 `invalid_record_policy`。
- topic/event 没有 route：应用 `unmatched_policy`。
- DLT 成功完成后来源记录才获得安全结果；DLT required 场景不会因 DLT 暂时不可用而静默跳过。

## 投递与失败模型

- 消费语义为 at-least-once，不宣称 exactly-once。
- 同一 partition 串行处理；失败 partition 退避时，其它健康 partition 可以继续推进。
- Auto 模式只在 handler `Ok(())` 后产生安全确认。
- Manual 模式还要求业务显式 ack；最终提交仍不能越过未确认空洞。
- 业务 `Err`、panic 和 timeout 进入相同的失败状态机，按配置重试或进入 DLT。
- `HandlerDeferred` 保留来源 offset 并退避，但与普通毒消息预算隔离；恢复后同一消息重新进入 handler。
- `ConsumeCtx.delivery_attempt` 统计当前会话内的全部真实 handler 投递，包含 `HandlerDeferred`，并进入
  DLT 证据；`ConsumeCtx.retry_attempt` 只统计普通失败预算，暂停期间保持不变。业务重试策略必须使用
  后者，不能因暂停次数追溯性地耗尽毒消息预算。
- 格式确定无效的记录按 `InvalidRecordPolicy` 进入 DLT 或 Halt，不消耗业务重试次数。
- DLT 使用 job 数和 retained bytes 双重有界队列，DLT 完成后才能推进来源记录水位。
- revoke、shutdown 和控制命令共享明确 deadline；结果未知不会伪装成未执行。

## 少拷贝 passthrough

`PassthroughConsumer` 面向 Kafka payload 已经是最终 socket 业务字节的场景。回调接收
`BorrowedKafkaRecord<'_>`，其 topic、key、headers 和 payload 都借用当前 Kafka 消息：

- `BorrowedKafkaRecord` 明确为 `!Send + !Sync`，不能逃逸到异步任务、线程或 channel。
- 回调必须同步完成路由解析和本地 outbox 入队决策。
- NASA 二进制路径不构造拥有型 Kafka record，也不先复制 payload 到中间 `Vec`。
- payload 与信封一次写入最终 `Bytes`；fan-out 时仅 `Bytes::clone()`，多个 outbox 共享同一底层分配。
- 无本地目标时不构造最终 frame。
- librdkafka producer 入队仍会执行自己的 `RD_KAFKA_MSG_F_COPY`，因此 broker 发布侧定义为
  “框架零中间复制”，不是端到端绝对零复制。
- socket.io 必须执行 JSON/base64 文本转换，单独计入 one-copy/转码路径，不属于 NASA binary 少拷贝口径。

Kafka header 只表达逻辑路由和消息元数据，不能指定任意 IP、host 或端口。最终出站只能选择预注册的
本地 `Sender`、session 和白名单 sink。完整的 control/data plane、header 契约和 frame 流程见
[naws README](../naws/README.md#websocket--kafka-集群推送与少拷贝)。

## 安全配置

TLS/SCRAM 使用 `nasa` 的 `kafka-tls` feature：

```toml
nasa = { version = "1", features = ["kafka-tls"] }
```

```yaml
kafka:
  bootstrap_servers: kafka-1:9192,kafka-2:9194,kafka-3:9196
  security:
    protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: ${KAFKA_USERNAME}
    sasl_password: ${KAFKA_PASSWORD}
    ssl_ca_location: /etc/credentials/kafka/ca.crt
```

密码、token、JAAS 和 key material 在 `Debug` 与配置错误中必须保持脱敏。生产 topic、DLT、consumer group
和 DescribeConfigs 权限应按 principal 分离；不要在配置文件中写固定明文口令。

## 健康、指标与管理面

- `KafkaProxy::group_health(group).await`：group 状态、assignment、pause 原因、commit 与重启累计值。
- `KafkaProxy::producer_lane(name)?.stats()`：物理 lane 的 in-flight、queue、delivery 与 generation 状态。
- `connect_with_metrics()`：注入非阻塞 `MetricsSink`，对接应用自己的 Prometheus/监控出口。
- `admin()`：topic 创建、删除、描述、配置与 metadata 操作；业务 API 仍不暴露 rdkafka 类型。
- 控制面支持 seek、beginning/end/committed/timestamp、pause/resume、subscribe/unsubscribe 和显式 restart。

开发或预发环境创建 topic：

```rust
use nasa::kafka::TopicSpec;

let admin = kafka.admin();
admin
    .create_if_absent(
        TopicSpec::new("orders", 12, 3)
            .config("min.insync.replicas", "2")
            .config("cleanup.policy", "delete")
            .config("unclean.leader.election.enable", "false"),
    )
    .await?;

let description = admin.describe_topic("orders", true).await?;
let exists = admin.topic_exists("orders").await?;
```

生产部署通常预建 topic，并在启动阶段调用 `describe_topic()` 校验分区数、RF 和关键配置。在线增加分区会改变
key→partition 映射，只能在业务明确接受顺序边界变化时调用 `increase_partitions()`；删除 topic 和修改配置也应由
受控运维流程执行。
