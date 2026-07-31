# naws

`naws` 是通用长连接框架，覆盖 TCP/WebSocket/socket.io 兼容层、NASA wire codec、endpoint/handler 抽象、鉴权、背压、心跳、优雅关闭，以及可选的 Redis Stream 或 Kafka 集群 transport。它不包含业务逻辑，业务通过 endpoint 注册事件处理。

业务项目通过门面开启 `ws`：

```toml
[dependencies]
nasa = { version = "1", features = ["ws"] }
```

## 服务端

```rust
use nasa::ws::{AuthResult, Endpoint, Server};

async fn start() -> anyhow::Result<()> {
    let endpoint = Endpoint::builder("/ws")
        .on_event_async("ping", |_session, _payload| async move {
            // 处理客户端事件。
        })
        .build();

    let server = Server::builder()
        .addr("0.0.0.0:19091")
        .ws_addr("0.0.0.0:19092")
        .authorize(|_req, _ctx| async move {
            AuthResult::ok("uid-1001")
        })
        .endpoint(endpoint)
        .max_connections(50_000)
        .max_unauthenticated(1_000)
        .build()?;

    let running = server.bind().await?;
    tracing::info!(tcp = %running.local_addr, ws = ?running.ws_local_addr);
    Ok(())
}
```

## 发送消息

`RunningServer::sender()` 可向会话、用户、群组或 endpoint 发送消息。

```rust
let sender = running.sender().clone();
let msg = nasa::ws::Message {
    events: Some(vec![Some("notice".to_string())]),
    uids: Some(vec![Some("uid-1001".to_string())]),
    payload: Some(b"hello".to_vec()),
    ..Default::default()
};
let report = sender.send(&msg, nasa::ws::Mode::BitpackTlv);
tracing::info!(queued = report.queued, dropped = report.dropped, closed = report.closed);
```

## Endpoint 生命周期

```rust
let endpoint = nasa::ws::Endpoint::builder("/trade")
    .on_connect(|session| {
        tracing::info!(sid = %session.id(), "connected");
    })
    .on_disconnect(|session| {
        tracing::info!(sid = %session.id(), "disconnected");
    })
    .on_event_inline("subscribe", |session, msg| {
        let _ = (session, msg);
    })
    .build();
```

## 客户端

```rust
let client = nasa::ws::Client::builder("127.0.0.1:19091")
    .endpoint("/ws")
    .token("demo-token")
    .auto_heartbeat(true)
    .build();

let auth = client.connect().await?;
client.send("ping", b"hello");
client.close_and_wait().await;
```

客户端的连接超时、重连退避以及服务端下发的心跳周期统一收敛到 1ms–365 天；零退避或极端
`Duration` 不会进入 Tokio timer。`NodeRegistry` 的 TTL 最长 365 天，墙钟回拨和极端观测时间戳
使用饱和差值判断，不触发整数溢出。

## 集群广播

开启 `redis` feature 后，可用 `RedisNotifier` 做跨节点 fan-out。生产集群必须使用稳定 `node_id` 和外部分配的 `Incarnation` fencing token，避免重启旧实例的迟到消息污染新实例。

装配入口在 Server builder 上：

```rust
let server = nasa::ws::Server::builder()
    .addr(cfg.tcp_addr)
    .cluster(cfg.node_id.clone(), std::sync::Arc::new(redis_notifier)) // 节点 ID + 广播 transport
    .cluster_incarnation(incarnation)                                   // 本次进程 boot id(fencing)
    .build()?;
```

`build()` 会校验:配了 `cluster` 必须同时给 `cluster_incarnation`,`node_id` 不能为空。

路由与 fencing 的两条硬语义:

- **空目标列表 = 不发给任何节点,不是广播。** `publish_to(&[])`(以及元素全为空串的列表)在 wire 上
  编成"存在但为空"的 target 列表,接收端一律不投递。只有 `target_nodes` **缺省**才表示广播。
  否则"发给零个节点"会退化成"发给所有节点",路由原语失败开放。
- **incarnation 接收侧限长 20 位十进制。** `Incarnation::from_epoch` 的 `{epoch:020}` 契约此前只在
  构造侧成立,接收侧不校验。一条携带 39 位巨值 incarnation 的事件会把该 node id 的围栏顶到天花板,
  此后真实节点的全部事件都因 incarnation 更小被静默拒绝,而 tombstone 按设计永不过期——
  恢复要重启**所有对端**进程。现在超长值在解析期即判非法。

## 背压与安全

- `max_connections` 限制活跃连接总数。
- `max_unauthenticated` 限制慢握手/慢鉴权连接。
- `max_inflight_handlers` 和 `max_inflight_handlers_per_conn` 防止事件 handler 过载。
- 出站队列按条数和字节双限，队列满时按 `BackpressurePolicy` 丢弃业务帧，控制帧优先。

## YML 配置与使用

`nasa::ws::ServerConfig` 不是 `Deserialize` 目标，推荐应用定义自己的 `ws:` 段，再通过 builder
映射。这样可以把 `Duration`、背压枚举、鉴权回调和 endpoint 注册留在业务启动代码中。

完整示例：

```yaml
ws:
  addr: 0.0.0.0:19091
  ws_addr: 0.0.0.0:19092
  auth_timeout_ms: 5000
  heartbeat_timeout_ms: 60000
  max_frame_bytes: 16777216
  max_ws_message_bytes: 67108880
  outbox_cap: 256
  outbox_max_bytes: 67108864
  backpressure: drop_new
  max_inflight_handlers: 4096
  max_inflight_handlers_per_conn: 256
  max_connections: 100000
  max_unauthenticated: 4096
  cluster_degraded: false
  redis_notifier:
    uri: ${APP_WS_REDIS_URL}
    stream_key: myapp:ws:cluster
    read_block_ms: 5000
    read_count: 256
    maxlen: 10000
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `addr` | `127.0.0.1:9090` | TCP 监听地址。 |
| `ws_addr` | `null` | WebSocket 监听地址；为空不启用 WS 端口。 |
| `auth_timeout_ms` | `5000` | 连接建立后等待认证的最长时间；必须在 1ms–365 天。 |
| `heartbeat_timeout_ms` | `60000` | 认证后允许多久未收到 PING；必须在 1ms–365 天。 |
| `max_frame_bytes` | `16777216` | 单条内部 frame 的长度上限。 |
| `max_ws_message_bytes` | 自动 | WS 外层聚合消息上限；必须大于等于 frame + 4。 |
| `outbox_cap` | `256` | 单连接出站队列条数上限。 |
| `outbox_max_bytes` | `67108864` | 单连接出站队列字节上限。 |
| `backpressure` | `drop_new` | 队列满时业务帧处理策略；控制帧仍优先。 |
| `max_inflight_handlers` | `4096` | 全局 async handler 并发上限。 |
| `max_inflight_handlers_per_conn` | `256` | 单连接 async handler 并发上限；0 表示不单独限制。 |
| `max_connections` | `100000` | 活跃连接总数上限；0 表示不限制。 |
| `max_unauthenticated` | `4096` | 未认证连接上限；0 表示不限制。 |
| `cluster_degraded` | `false` | 集群 transport 未就绪时是否允许降级启动。 |
| `redis_notifier.*` | 见示例 | 开启 `redis` feature 后的跨节点广播配置。 |

启动映射示例：

```rust
let mut builder = nasa::ws::Server::builder()
    .addr(&cfg.ws.addr)
    .auth_timeout(std::time::Duration::from_millis(cfg.ws.auth_timeout_ms))
    .heartbeat_timeout(std::time::Duration::from_millis(cfg.ws.heartbeat_timeout_ms))
    .max_frame_bytes(cfg.ws.max_frame_bytes)
    .outbox_capacity(cfg.ws.outbox_cap)
    .outbox_max_bytes(cfg.ws.outbox_max_bytes)
    .max_connections(cfg.ws.max_connections)
    .max_unauthenticated(cfg.ws.max_unauthenticated)
    .cluster_degraded(cfg.ws.cluster_degraded)
    .authorize(authorize)
    .endpoint(endpoint);

if let Some(addr) = &cfg.ws.ws_addr {
    builder = builder.ws_addr(addr);
}

let server = builder.build()?;
```

Redis 集群广播是 at-most-once 语义，适合 presence 对账、订阅状态同步和最新值覆盖；需要强可靠投递的业务不要直接把它当任务队列。

## WebSocket + Kafka 集群推送与少拷贝

### Feature 与组件职责

业务经统一门面接入时应显式启用组合 feature：

```toml
[dependencies]
nasa = { version = "1.0.0", features = ["ws-kafka"] }
# 生产使用 SASL_SSL 时再加 "kafka-tls"；使用 Zstd 时再加 "kafka-zstd"。
```

直接依赖实现 crate 时使用 `naws = { features = ["kafka"] }` 并同时依赖 `nafka`。单独的 `ws`、`kafka`
以及当前门面的 `full` 都不表示已经启用 `ws + kafka` 组合，业务必须明确选择 `ws-kafka`。

| 组件 | 平面 | 职责 |
|---|---|---|
| `WsKafkaRuntime` | 统一生命周期 | 冻结 topic、group、producer lane、内存预算，启动 consumer，等待精确 assignment，监控 topic 漂移并统一关闭。 |
| `KafkaNotifier` | control | 承载 presence/join/leave 等 `ClusterEvent`；固定使用 control topic、group 和物理 producer lane。 |
| `KafkaClusterDataPublisher` | data producer | 从 `Message` 借用路由和 payload；路由写 header，payload 用 `publish_raw` 写 Kafka value，不构造 `ClusterEvent`。 |
| `HeaderPassthroughAdapter` | data consumer | 在 Kafka owner 回调内借用解析 header、过滤节点/代际、解析本地目标，并把 payload 一次写入最终 NASA frame。 |
| `WsKafkaPublisher` | 显式业务发布 | 提供受约束的 raw builder；固定 data lane，但调用方仍须传入已配置的 data topic。 |

control 与 data 必须使用不同 topic、consumer group 和物理 producer lane；DLT lane 可以与 data 共用，但不能
与 control 共用。Redis 和 Kafka 也不能同时作为 active 集群 transport，否则同一业务消息会被发布两次。

### 数据面协议与执行路径

数据面不把业务 payload 包进 JSON、`Message` 或 `ClusterEvent` 信封：

```text
Sender::send / relay
  ├─ 本节点 Sender fan-out
  └─ KafkaClusterDataPublisher
       ├─ Message 路由字段 → X-Nasa-WS-* headers
       └─ Message.payload  → Kafka raw value
                                │
                             broker
                                │
                     BorrowedKafkaRecord
                       ├─ 借用解析 headers
                       ├─ loopback / stale / target-node / local-target 过滤
                       └─ MessageRef → 最终 NASA Bytes → 有界 outbox × N
```

`X-Nasa-Event=NAWS_PUSH` 负责把记录路由到 data adapter。`X-Nasa-WS-*` header 负责 socket 路由：

| Header | 基数 | 说明 |
|---|---:|---|
| `X-Nasa-WS-Event` | 1..N | 客户端事件；至少一个。 |
| `X-Nasa-WS-Router` | 0..N | endpoint/router；与 uid/group 至少有一类目标。 |
| `X-Nasa-WS-Uid` | 0..N | 目标用户；typed builder 禁止与 group 同时设置。 |
| `X-Nasa-WS-Group` | 0..1 | 目标群组。 |
| `X-Nasa-WS-Exclude-Uid` | 0..N | 排除用户。 |
| `X-Nasa-WS-From-Uid` / `X-Nasa-WS-From-Client` | 各 0..1 | 来源与回送排除信息。 |
| `X-Nasa-WS-Source-Node` / `X-Nasa-WS-Source-Incarnation` | 成对出现 | cluster relay 必填，用于 loopback 与旧实例 fencing。 |
| `X-Nasa-WS-Target-Node` | 0..N | 可选逻辑节点过滤；不能提供任意 IP、host 或端口。 |
| `X-Nasa-WS-Sink-Id` | 0..1 | 只能选择启动期 `SenderSlot` 白名单中的本地 Sender。 |
| `X-Nasa-WS-Message-Mode` | 0..1 | 缺省使用配置默认值；必须属于 allowlist。 |
| `X-Nasa-WS-Payload-Layout` | 0..1 | v1 只允许 `raw`；安全 publisher 总会显式写入。 |

所有 `X-Nasa-WS-*` 值都必须是非空、无控制字符的 UTF-8。未知 `X-Nasa-WS-*` header、singleton 重复、来源身份不成对、
超出 header 数量/字节预算或空目标都会被确定拒绝。Kafka key 不参与 socket 路由，只用于分区与顺序；cluster
publisher 默认以 source node 为 key，因此同一来源在冻结分区数下保持顺序。

### 完整装配顺序

下面省略业务配置反序列化，但保留不能调换的生命周期顺序。通过 `nasa` 门面使用时，长连接类型从
`nasa::ws::*` 引入，`WsKafka*`/`SenderSlot` 从 `nasa::ws::kafka::*` 引入，底层 `KafkaConfig` 等 nafka
类型从 `nasa::kafka::*` 引入。

```rust
use std::sync::Arc;
use std::time::Duration;

use nasa::ws::kafka::{
    SenderSlot, WsKafkaRuntime, WsKafkaRuntimeConfig, WsKafkaTopicContract,
};
use nasa::ws::{AuthResult, Incarnation, Notifier, Server};

async fn start_cluster(
    kafka_config: nasa::kafka::KafkaConfig,
    contract: WsKafkaTopicContract,
    incarnation_epoch: i64, // 必须来自 Redis INCR 等外部持久单调源
) -> anyhow::Result<(nasa::ws::RunningServer, WsKafkaRuntime)> {
    let node_id = "socket-node-a";
    let mut runtime_config = WsKafkaRuntimeConfig::new(node_id);
    runtime_config.control_topic = contract.control.name.clone();
    runtime_config.data_topic = contract.data.name.clone();
    runtime_config.control_lane = "control".into();
    runtime_config.data_lane = "data".into();
    runtime_config.dlt_lane = "data".into();

    let ready_timeout = runtime_config
        .ready_timeout_ms
        .checked_add(5_000)
        .ok_or_else(|| anyhow::anyhow!("Kafka ready timeout 余量溢出"))?;
    let runtime = WsKafkaRuntime::connect(kafka_config, runtime_config, contract)?;
    let notifier: Arc<dyn Notifier> = Arc::new(runtime.notifier());
    let incarnation = Incarnation::from_epoch(incarnation_epoch)?;

    let server = Server::builder()
        .addr("0.0.0.0:19091")
        .ws_addr("0.0.0.0:19092")
        .authorize(|_req, _ctx| async move { AuthResult::ok("uid-1001") })
        .cluster(node_id, notifier)
        .cluster_data_publisher(runtime.cluster_data_publisher())
        .cluster_ready_timeout(Duration::from_millis(ready_timeout))
        .cluster_incarnation(incarnation)
        .build()?;

    // build 后才能取得最终 Sender/NodeRegistry，bind 前必须回绑；否则 listener 不得 accept。
    runtime.bind_data_plane(
        SenderSlot::new(server.sender().clone()),
        server.kafka_source_fence(),
    )?;

    // bind 内部调用 KafkaNotifier::start；TopicContract 和 control/data assignment 全部 ready
    // 后才返回。生产不要打开 cluster_degraded。
    let running = match server.bind().await {
        Ok(running) => running,
        Err(error) => {
            let _ = runtime.shutdown().await;
            return Err(error.into());
        }
    };
    Ok((running, runtime))
}
```

正常停机必须检查两个阶段的结果：先调用 `running.shutdown_graceful(deadline).await`，它会先关闭 cluster ingress，
再停止 accept/reader 并排空连接任务；随后调用 `runtime.shutdown().await` 幂等收尾 consumer 和 producer lane。
不能直接 drop runtime，也不能在 Tokio runtime 已停止后再等待 Kafka shutdown。启动失败分支同样必须显式
`runtime.shutdown().await`，如上例所示。`shutdown_graceful` 的非 fallible `Duration` 参数最长按
365 天执行，极端值不会使绝对 deadline 加法溢出。

`Sender::send/relay` 的返回值只统计本节点 outbox；Kafka data 发布使用有界 `fire`，远端 broker delivery 通过
`WsKafkaRuntime::plane_stats()` 观察，不能从本地 `SendReport` 推断远端已收到。需要等待 broker delivery 的显式
业务发布可在 runtime ready 后使用 `runtime.publisher()?.publish(data_topic, payload)...send().await`，但它不会
替调用方补做本地 fan-out，也不能绕过 source identity、route、mode 和 header 容量校验。

### 启动与配置硬约束

- 生产 `TopicManagement` 必须是 `ValidateOnly`，`strict_describe_configs=true`；control/data/DLT topic 预建且
  精确匹配分区数、`cleanup.policy=delete`、retention、`max.message.bytes`，RF 至少 3、min ISR 至少 2。
- producer 必须存在独立 `control`、`data` lane；生产 lane 要求 `acks=all`、idempotence，并显式限制 native
  queue 条数、字节和 fire observer。所有物理 lane queue 加 DLT backlog 不得超过
  `kafka_memory_budget_bytes`。
- consumer 必须使用 `auto_offset_reset=latest`；`max_partition_fetch_bytes`、nafka `max_record_bytes`、NASA
  `max_frame_bytes`、topic `max.message.bytes` 和 route header 预算必须按同一最大消息口径配置。
  启动期会交叉核对 `behavior.max_record_bytes + 帧信封开销` 与全部目标 Sender 的最小 `max_frame`，
  装不下时打 **warn 而非拒绝启动**：nafka 的 `max_record_bytes` 与 naws 的 `max_frame` 默认都是 16MiB，
  topic 契约又要求 `max.message.bytes >= max_record_bytes + header 预算`，任何硬拒绝形式的判据都会让
  **全默认配置无法启动**（合法区间为空）。默认值不变，要消除风险二选一：调小 `behavior.max_record_bytes`，
  或调大 `max_frame_bytes`。这条取舍留给业务侧，组件只负责给出确定信号。
  已知局限：该告警取的是各 `Sender` 的编码器上限，而实际投递还会按每个 session 的 `max_frame` 再查一次；
  绕过 `Server` 直接用 `Sender::new()` 装配时前者恒为 16MiB 默认值，此时告警可能与真实投递上限不一致。
- `EphemeralNode` 让每个进程实例收到完整 data 流，适合广播与滚动；`Durable` group 必须按逻辑节点唯一，不能
  让不同节点共享同一个 group，否则 Kafka 会分摊分区并造成节点漏收。
- `local_node` 必须稳定且唯一；`Incarnation` 必须来自外部持久单调源。首次跨节点发布时会比对
  egress 的 `Source-Node` 与 ingress 回环判定所用的 `local_node`，不一致即 `error` 告警一次
  ——两者不同会让本节点的消息被自己重复投递。注意该自检只在经 `WsKafkaRuntime::cluster_data_publisher()`
  装配时生效；直接用 `KafkaClusterDataPublisher::new()` 自行装配的路径没有期望值可比，检查静默关闭。cluster relay 固定使用
  `SourceIdentityPolicy::Required`，旧 incarnation、loopback 和未命中 target node 都在 frame 物化前安全跳过。
- `cluster_ready_timeout` 应覆盖 `ready_timeout_ms` 并留出外层余量。生产保持 `cluster_degraded=false`，topic
  契约或 assignment 未就绪时不得开始接收连接。运行期发现确定 topic 漂移会关闭 Kafka ingress。
- readiness 通过后仍要监控 `health()`、`control_group_health()`、`data_group_health()` 与 `plane_stats()`；至少对
  lifecycle 非 Running、group paused/degraded/crashed、topic contract error、native/observer queue full 和
  delivery failed 告警。control 与 data 指标必须分开，不能用总量掩盖 presence 被 data 面挤压。

### ACK、重试与投递边界

data adapter 在同步 Kafka owner 回调内完成“路由→最终 frame→本地有界 outbox 入队决策”，随后由 nafka 的连续
安全水位和批量 commit 推进 offset。这里的“成功”只表示本地 outbox 已接受，不表示客户端已经收到或处理：

| 情况 | 默认结果 |
|---|---|
| 无本地目标、target node 不匹配、loopback、旧 incarnation | `Skipped`，安全前移；不物化最终 frame。 |
| 全部目标成功入队 | `Handled`，进入可提交安全水位。 |
| 部分 outbox dropped/closed + `RequireAllEnqueued` | retry；已经入队的目标可能因 Kafka 重放再次收到，业务须接受 at-least-once。 |
| 部分 outbox dropped/closed + `BestEffort` | 记录丢弃后安全前移；这是显式弱化可靠性的选择。 |
| fan-out 超限 | 默认 `Halt`；也可配置为先写 DLT 再前移。 |
| 非法 header、非法 mode、未知 sink、tombstone | `InvalidRecord`，按 nafka 策略 Halt 或 DLT；不能静默 ACK。 |

passthrough 热路径不使用 `#[kafka_consumer]` 的业务手动 ACK token；它用类型化 disposition 表达 ACK 决策。
socket ACK 的边界是“完成本地 outbox 入队决策”，不是客户端回执。若业务要求客户端端到端确认，需要在应用协议
中另设 ACK/去重 ID，不能把 Kafka offset commit 当客户端确认。

nafka 的统一不变量仍然成立：Auto 只在 handler 成功返回后产生确认，`Err`、panic 或 timeout 绝不 ACK；Manual
handler 即使已经调用 `ack()`，之后返回 `Err`/panic/timeout 时，本次 provisional ACK 也全部作废。passthrough 是
同步借用回调，没有 async timeout，但 `Err`/panic 同样不能推进 offset。

### “零拷贝”口径与复制账本

必须区分三件事：Kafka broker 的 `sendfile` zero-copy 是 broker 侧优化；librdkafka 的 borrowed payload/header
只在 poll 回调内有效；应用无法无复制地把这段借用内存交给异步 socket outbox，`detach()` 反而会复制整条记录。

本项目承诺的 **“one-copy payload + zero-copy fan-out”** 仅指接收端应用层的
`BorrowedKafkaRecord → 最终 NASA frame → N 个原生会话 outbox`，不是 producer→broker→socket 端到端一次复制：

```text
生产端 Message.payload
  → publish_raw(&[u8])：不构造 serde/Vec 中间信封
  → librdkafka producer queue：RD_KAFKA_MSG_F_COPY 取得所有权                 [一次 producer 侧 copy]
  → broker / 网络 / consumer 内核缓冲                                      [系统与协议层 copy]
  → librdkafka fetch buffer：payload/header 以 BorrowedKafkaRecord 借用       [应用 0 copy]
  → encode_event_frame_ref：路由字段 + payload 写入最终 BytesMut              [一次 consumer 应用层 payload copy]
  → Bytes::clone 到 N 个 NASA TCP/WS outbox                                 [0 payload copy]
  → writer 写 socket                                                         [内核/依赖可能继续缓冲或复制]
```

为什么保留 consumer 侧这一次物化：借用不能逃逸 poll 回调，而 frame 必须进入有界异步 outbox；同步直写 socket
会让一个慢客户端阻塞 Kafka poll/heartbeat，破坏背压、帧序和 consumer group 稳定性。最终 `Bytes` 对 NASA TCP
和原生 NASA WebSocket 会话共享同一 backing allocation；WS writer 使用 binary message 接管该 `Bytes`。这不等于
内核 `MSG_ZEROCOPY`，也不承诺第三方 WebSocket/TLS 实现内部永不缓冲。

收益和边界如下：

- payload 不做 decode→对象→re-encode；headers 只做借用 UTF-8/契约校验。
- 有 NASA 目标时只构造一个最终 frame；N 个 outbox 只做 `Bytes::clone`，双 session 的 `as_ptr()` 相同。
- `NoLocalTarget`、loopback、旧 incarnation 和 target mismatch 都是 **0 次最终 frame 物化、0 次 payload copy**；
  路由索引查询自身仍可能创建临时集合，不能宣称整条路径“0 分配”。
- 混合协议 fan-out 时，NASA 会话共享上述 frame；Socket.IO 会话另走 JSON/base64 文本转换，不属于 one-copy SLO。
- `BITPACK_TLV`、`VARINT_TLV` 属于少拷贝路径；`JSON_BYTES` 只有显式加入 allowlist 才可用，并会构造拥有型
  JSON/临时缓冲；`FAST_FIXED` v1 拒绝。
- tombstone 不是空 payload：空 Kafka value 是合法 `Some(&[])`，null value 是非法 data record，必须进入
  Invalid/DLT/Halt 流程。只有失败耗尽需要 DLT 时才复制 borrowed record。

实现落点：`sender.rs` 负责一次 frame 物化和共享 outbox backing allocation；`wire.rs` 负责
borrowed/owned 编码一致性；`nafka/src/consumer/passthrough.rs` 的公开生命周期合同限制 borrowed record
不能逃逸 poll 回调。真实 broker 的 raw/empty/tombstone、commit、assignment 和故障场景由仓库外验证
工作区覆盖，不进入产品发布包。
