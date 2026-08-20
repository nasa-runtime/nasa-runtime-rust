# nadis

`nadis` 是 Redis 基础层，覆盖连接、常用命令、pipeline、nonce 幂等计数、分布式锁、leader、Pub/Sub、Stream、分区消费和可选 RedisJob、RediSearch/RedisJSON。它把旧系统兼容协议和纯 Rust 增强协议用 `CompatibilityProfile` 明确区分，避免业务无意混用持久化边界。

它解决两类容易被业务代码错误拼装的原子边界：计数写与 nonce 凭证在同一 Lua 内结算，分布式任务的调度、租约、fencing、Fanout 与完成状态也只通过封闭脚本迁移。业务只调用类型化 API，不接触账本命令或裸 Lua 返回值。

Handler 通过 `JobContext::parameter::<T>()` 获取 JSON 参数，`T` 可以是包含集合、映射或嵌套结构的任意 `serde::DeserializeOwned` 类型；框架会先执行重复键、深度和节点预算校验。非 JSON 参数通过 `payload()` 读取原始字节并由业务按声明的 codec 解码，框架不会猜测 Protobuf 类型。

业务项目通过门面开启 `redis`：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["redis"] }
```

## 连接 Redis

```rust
use nasa::redis::{CompatibilityProfile, RedisClient, RedisConfig};

async fn connect() -> nasa::redis::Result<std::sync::Arc<RedisClient>> {
    let cfg = RedisConfig::new(
        "redis://127.0.0.1:6379",
        "order-service",
        CompatibilityProfile::RustV2,
    );
    RedisClient::connect(cfg).await
}
```

`profile` 必须显式选择：

- `LegacyV1`: 与旧版/历史 Redis 协议互通。
- `RustV2`: 纯 Rust 集群增强协议，包含 Redis TIME、fencing 等新语义。

不同 profile 不要共享同一持久化 namespace。

## 常用命令

```rust
let client = connect().await?;

client.set("user:{1001}:name", "Alice").await?;
let name: Option<String> = client.get("user:{1001}:name").await?;

client.h_set("user:{1001}", "level", 3).await?;
let level: Option<i64> = client.h_get("user:{1001}", "level").await?;

client.expire("user:{1001}", std::time::Duration::from_secs(60)).await?;
```

Cluster 多 key 命令会做同 slot 守卫；需要跨 key 时请使用 `{tag}` 约束 slot。分区运行时以
“分区组”为同槽单位：同组所有 stream、锁、marker 和控制 key 固定到一个 slot。`partition.count`
增加的是组内并发，不会把单组分散到多个 master；需要跨 master 扩吞吐时，用 `partition.groups`
拆成多个隔离组。

Redis 相对 TTL 最终由 signed 64-bit 毫秒或秒参数表达；超过服务端范围的 `Duration` 会在本地
返回配置错误，不会截断、回绕或发送负 TTL。

## Pipeline

适合批量读写并减少 RTT。命令入队后通过 `execute().await` 统一发送，再从 `Ticket` 取结果。

```rust
let mut pipe = client.pipeline();
let name = pipe.get::<String>("user:{1001}:name")?;
let age = pipe.h_get::<i64>("user:{1001}", "age")?;

pipe.execute().await?;

let name = name.await_result()?;
let age = age.await_result()?;
```

需要防重放的计数必须显式使用 nonce API。直发与批量入口共用同一账本；每条批量命令拥有自己的 nonce 和完整原子 Lua，不存在“先查 nonce、再入队原生命令”的并发窗口：

```rust
let settled = client.incr_by_idempotent("balance:{42}", 5, "credit:2026-08-16:1").await?;

let mut batch = client.idempotent_pipeline().await?;
let replay = batch.decr_by_idempotent("balance:{42}", 100, "credit:2026-08-16:1")?;
let next = batch.h_incr_by_idempotent("quota:{42}", "daily", 2, "quota:2026-08-16:1")?;
batch.execute().await?;

assert_eq!(replay.await_result()?, settled);
let quota = next.await_result()?;
```

String、Hash 与 ZSet 共提供九个 `*_idempotent` helper。nonce 不能为空；同一目标、结构成员与 nonce 在窗口内只结算一次，即使重放时改变方向或 delta 也返回首次结果。专用会话只在 `execute()` 提交；达到普通 Pipeline 的条数或字节上限会在入队前拒绝，尚未提交的整会话确定未发送。普通 `pipeline()` 不推断 nonce，也不会改变原命令语义。首次调用会确认账本布局；受管 Redis 仅预留本地指标，未显式配置且未调用时不会探测额外能力。

`HASH_FIELD` 需要全部 master 支持 `HPEXPIRE`；`HASH_BUCKET` 使用时间桶；`AUTO` 只在全量响应明确确认命令不存在时选择时间桶。探测超时、ACL 拒绝、拓扑不稳或节点结果缺失会拒绝本次准备且不建立 marker，后续调用可重新探测。与 Java `LegacyV1` 共享账本还要求 key、Hash field 和 ZSet member 的序列化字节一致。

## RedisJob

直接依赖 `nadis` 时开启 `job` feature；业务经 `nasa` 门面时开启 `redis-job`。`nadis::job` 提供无全局 leader 的分布式任务运行时。多节点可以同时扫描，到期触发、领取、续租、完成、恢复和 Fanout assignment 都由 Redis 时间、CAS、owner、attempt token 与 fencing 收敛。普通任务可以共享 Worker Stream 与独立容量池；Fanout 目标必须声明为 `FANOUT_ONLY`，避免普通 Dispatcher 与定向 shard 竞争同一 Handler。

### 运行架构

RedisJob 将一次任务执行拆成四个持久阶段：定义与能力登记、调度与投递、执行权租约、终态对账。

```text
业务定义/Handler
      │ Prepare：source、namespace、schema、codec、能力快照冻结
      ▼
Redis 调度账本 ── Dispatch Stream/定向 inbox ──> Executor
      │                 │                         │
      │                 └─ receipt/ready          └─ start：owner + attempt token + lease
      ▼
扫描器：receipt、ready、lease、root 分桶对账 ──> CAS/Lua 终态提交与 Fanout 聚合
```

Prepare 阶段只发布已通过配置、定义、codec、容量和 source 门禁的计划；Dispatcher 取得执行权后才调用 Handler。Handler 外部写入必须携带 `attempt_token` 并在副作用前调用 `checkpoint()`，租约失效、取消或本地保守截止到达后立即停止。扫描器不是单一 leader，重复扫描依靠 Redis 权威时间、CAS、assignment epoch 和 fencing 收敛。

普通 Run 通过 Worker Stream 分发；Fanout 先冻结兼容执行器快照，再为每个快照成员建立一个 shard。目标先写入持久 receipt，再申请本地 Handler 槽位；容量不足进入独立容量窗口。容量超窗优先切换兼容目标，无候选时重开窗口并保留 assignment，不把健康满载节点改判为失联。

### 参数与编码

JSON Handler 使用 `JobContext::parameter::<T>()` 反序列化参数，支持 `Vec<T>`、`HashMap<K, V>`、嵌套结构和其它 `serde::DeserializeOwned` 类型。框架在业务类型反序列化前拒绝重复键、危险类型元数据、过深结构和节点数超限；反序列化失败返回结构化 `InvalidPayload`。Protobuf 或 RAW 参数不做类型猜测，业务通过 `payload()` 按定义的 codec 解码。

`schema_id` 与 `wire_codec` 是持久合同，不能由 Handler 根据 payload 内容动态选择；同一 Worker 的定义、Fanout 快照和执行器能力必须使用兼容的 schema 与 codec。

Fanout 目标在持久确认接收后若本地执行槽暂满，会在 `fanout_capacity_wait_ms` 窗口内保留当前 assignment、延后 ready 并清除普通唤醒计数；短时容量背压不会被当作节点失联，也不会产生 `SKIPPED` 或消耗节点切换配额。默认窗口为 `10_000` 毫秒，是防止单个满载目标无限占用分片的有界策略。连续等待超窗后，系统优先切换到兼容目标；没有可切换目标时重开容量等待窗口，继续保留当前 assignment，不把健康满载目标判为失联。只有稳定目标节点真的改变才增加 `assignmentCount`，同一节点的新启动或心跳证据只推进 assignment epoch。

容量路由使用独立的 `capacityRouteCount` 预算，不消耗故障重分配的 `assignmentCount`；每次容量迁移还会在 shard HASH 中累加 `capacityRouteTotal`。`capacityRouteCount` 用于当前调度裁决，成功取得执行权后清除；`capacityRouteTotal` 保留到 shard 清理前，供运维审计迁移次数。运行时指标反映进程级即时计数，不能替代该持久字段。

```rust
let definition = nasa::redis::job::JobDefinition::builder("wallet-sweep")
    .qualifier("primary")
    .fixed_rate_ms(30_000)
    .build()?;

let plan = nasa::redis::job::RedisJobPlan::new()
    .register(definition, |context| async move {
        context.checkpoint()?;
        Ok::<_, nasa::redis::NasaRedisError>(())
    })?;
```

每个定义的 `qualifier` 是强路由字段。`prepare_sources` 接受冻结的 `RedisJobSources`，每个 source 独立拥有客户端、namespace、键布局、连接角色、监督代次、健康、指标和停机边界；未知、禁用或 client 身份不一致的 source 在写定义前拒绝。独立运行时可用 `shutdown_source(qualifier, budget)` 只排空一个 source，其它 source 继续领取和续租；全量停机使用 `shutdown(budget)`。`JobControl` 提供 trigger、单任务 pause/resume、当前 source 的 `pause_namespace`/`resume_namespace`、cancel、delete 与 conflict resolve；命名空间暂停阻止新的普通 Run 触发、尚未领取的普通 Run 和可见性消息重建，不撤销已有 attempt，也不影响其它 source。普通定义不宣告 Fanout 能力，删除 `FANOUT_ONLY` 定义会在 tombstone 提交后同步撤销执行器能力，撤销失败可用相同定义修订号幂等补偿，避免新 Fanout 快照继续选择已失去 Handler 的节点。`JobQuery` 提供 definition、run、Fanout、健康和无 Redis I/O 的指标快照。

命名空间暂停或恢复按调度分片逐一发布，不是跨分片原子事务。控制调用返回错误时，部分分片可能已经进入目标状态，调用方不得据此判定整个 source 已暂停或已恢复，必须使用同一 actor 幂等重试直至成功；当前不提供聚合的“部分生效”指标，返回错误本身就是未收敛信号。该门禁只阻止普通 Run 取得新执行权：已经取得的 attempt 可以继续续期和完成，已经建立的 Fanout 批次也会继续投递和收敛。

JSON Handler 在业务反序列化前递归拒绝重复键、`@class`/`@type`、过深或节点过多的结构。运行时只支持已经声明的协议、codec 与 Cron 语义，不承诺无限 Run 历史、永久 nonce、永久 Fanout 去重，也不会把普通本地定时任务自动升级为 RedisJob。

## 分布式锁

```rust
use nasa::redis::DistributedLock;

let lock = DistributedLock::new(client.clone());
let guard = lock.lock("lock:{order:1}", Some(std::time::Duration::from_secs(3))).await?;

// 临界区:同一把锁由 watchdog 续租。

guard.unlock().await?;
```

也可以用 `with_lock` 包裹业务 future：

```rust
lock.with_lock("lock:{order:1}", Some(std::time::Duration::from_secs(3)), || async {
    // 临界区。
}).await?;
```

## Leader

适合单 leader 后台任务或和 `nasched` 的 `NadisLeaderGate` 配合。

```rust
let lock = std::sync::Arc::new(nasa::redis::DistributedLock::new(client.clone()));
let leader = nasa::redis::Leader::elect(
    lock,
    "leader:{scheduler}",
    std::time::Duration::from_secs(1),
);

leader.run_if_leader(|| async {
    // 只有当前 leader 执行。
}).await;

// 正常停机显式退位并等待锁释放。
leader.shutdown().await;
```

最后一个 `Leader` 句柄被直接丢弃时也会停止竞选并尝试释放锁，但 Drop 只能作为异常路径兜底；
服务停机仍应调用 `shutdown().await`。

## Pub/Sub

```rust
let mut sub = client.sub(&["events"]).await?;
client.r#pub("events", "hello").await?;

if let Some(msg) = sub.next_message().await {
    let text: String = msg.parse()?;
}
```

## 分布式雪花 workerId

`nabase` 的本地雪花器需要业务保证 workerId 唯一;跨节点自动分配用本 crate 的 Redis 版:

```rust
use nasa::redis::{Snowflake, SnowflakeConfig};

let cfg = SnowflakeConfig::default(); // key/bits 可调
let (sf, lease) = cfg.build_with_redis(client.clone()).await?; // ZSET 租约分配 workerId
let id = sf.generate();
// 优雅停机时释放租约,workerId 可被其它节点复用:
lease.release().await?;
```

租约未释放时靠 TTL 到期回收;`lease.worker_id()` 可用于日志与监控。

## Stream 消费

发布用 `client.publish/publish_default/publish_many`;订阅是 builder 链:

```rust
let sub = client
    .subscribe("order-events")            // stream key
    .group("settle", "node-1")            // 消费组 + 消费者名
    .on("order.created", |ev| async move { // 按事件名注册处理器
        let _ = ev;
        Ok(())
    })
    .start()
    .await?;
// 停机:
sub.shutdown().await;
```

单点和 Redis Cluster 都支持该 builder。Cluster 下每个订阅只读取一个 stream key，专用
`ClusterConnection` 按该 key 的 slot 路由并跟随 MOVED/ASK；API 不提供跨 slot 的多 stream
阻塞读。阻塞连接与普通命令连接隔离，订阅停机会取消阻塞读、idle、错误退避、重连和在途
handler；若取消与 XACK 同时发生，确认结果按不确定态处理，不会伪报未执行。

组内竞争消费、ack 策略、重投上限(`max_redeliver`)、毒消息策略(`poison_policy`)按下方 `stream.*` 配置;分区消费(同 key 串行、跨节点再均衡)入口为 `PreparedPartition`/`RunningPartition`,配置见 `partition.*`。

## PROXY 共享消费组

`PreparedProxy` 适合一个共享 stream 上的多 consumer 并行消费；它不提供分区顺序或 owner fencing，
handler 必须幂等。默认 handler 超时为 10 秒，reclaim idle 为 30 秒，stream 写入使用
`MAXLEN ~ 1000000` 控制历史长度。

```rust
use nasa::redis::{PreparedProxy, ProxyCfg};

let mut prepared = PreparedProxy::prepare(
    client.clone(),
    "jobs:{billing}",
    "billing-workers",
    ProxyCfg::default(),
).await?;
prepared.register::<serde_json::Value, _, _>("billing", "settle", |items| async move {
    let _ = items;
    Ok(())
});

let proxy = prepared.start().await?;
proxy.publish("billing", "settle", &serde_json::json!({"order_id": 42})).await?;
proxy.shutdown().await;
```

`consumers` 最大 256，`batch_size` 最大 10000；`reclaim_min_idle_ms`、`handler_timeout_ms` 和
`drain_deadline_ms` 必须大于 0，所有 timer 时长不超过 365 天。直接丢弃 `RunningProxy` 会立即
停止 consumer/reclaim，但不会冒险删除可能含 PEL 的 consumer；正常停机应显式调用
`shutdown().await`。

## RediSearch 文档

开启 `derive` feature 后可派生 `RedisDocument`。

```rust
#[derive(nasa::redis::RedisDocument)]
#[rs(index = "idx:user", prefix = "user:")]
struct UserDoc {
    #[rs(id)]
    id: i64,
    #[rs(tag)]
    tenant: String,
    #[rs(text)]
    name: String,
}
```

## 生产注意

- `RedisConfig` 的 `url` 在 Debug 输出中会脱敏，但不要把明文连接串写进仓库。
- `RustV2` 和 `LegacyV1` 的分区/锁/stream 协议不要混同一 namespace。
- 非幂等命令遇到 Redis IO 错误时，框架不会透明重试；调用方要按业务处理“执行状态未知”。
- ACK 后异步 XDEL 只回收 stream 空间，不改变确认语义；删除暂时失败会在后续周期重试，
  停机做末次 flush。末次删除仍失败时 entry 会保留在 stream；仅在 autoTrim 同时启用时，
  才会由其保留窗继续回收。持续删除失败会先积累有界待删 ID，再通过队列反压暂停消费；
  这是为了避免 ACK 已成功后静默遗失待删 ID。应把 `RunningPartition::async_delete_pending()`
  接入运行时 gauge，并对持续非零告警。
- Partition 正常停机应显式调用 `RunningPartition::shutdown().await`。直接 Drop 会关闭准入并启动
  best-effort drain；并发 shutdown 会串行复用同一个收口，不会提前释放后台资源。

## YML 配置与使用

推荐把 Redis 配置放在 `redis:` 根节点，然后直接反序列化为
`nasa::redis::RedisConfig`。`url`、`namespace`、`profile` 必填；其它字段都有默认值。

完整示例：

```yaml
redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2
  command:
    timeout_ms: 0
    response_timeout_ms: 30000
  pipeline:
    session_max_commands: 1000
    session_max_bytes: 4194304
    dedicated_conn: true
  idempotent_counter:
    ttl_mode: AUTO
    nonce_ttl_ms: 604800000
    bucket_span_ms: 86400000
    ledger_shards: 256
    ledger_key_prefix: rpidem
    layout_marker_id: primary
  lock:
    prefix: "DISTRIBUTED-LOCK:"
    lease_ms: 30000
  stream:
    poll_timeout_ms: 500
    batch_size: 100
    data_expire_ms: 3600000
    auto_trim_rate_ms: 60000
    async_del_record_period_ms: 5000
    inflight_max: 1000
  partition:
    enabled: false
    default_group: SINGLE-CONSUME
    count: 64
    rebalance_ms: 10000
    min_idle_ms: 30000
    holds_check_interval_ms: 5000
    drain_timeout_ms: 35000
    max_redeliver: 5
    handler_timeout_ms: 30000
    poison_policy: Park
    groups:
      hot:
        topics: [trade, quote]
        count: 128
        batch_size: 500
        poll_timeout_ms: 100
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `url` | 必填 | Redis URI；单点和 cluster 都用 redis URI 传入，密码写在 URI 中。 |
| `namespace` | 必填 | 协议命名空间；不同系统、不同 profile 不要复用。 |
| `profile` | 必填 | `RustV2` 或 `LegacyV1`，决定 key 布局、锁、stream 和分区协议。 |
| `command.timeout_ms` | `0` | 单命令业务超时；0 表示不限，非零最大 365 天。 |
| `command.response_timeout_ms` | `30000` | 连接级响应超时；0 表示不限，非零最大 365 天。 |
| `pipeline.session_max_commands` | `1000` | 单个 PipelineSession 自动滚动 flush 的命令数。 |
| `pipeline.session_max_bytes` | `4194304` | 单批参数字节上限。 |
| `pipeline.dedicated_conn` | `true` | pipeline 是否使用独立连接 lane。 |
| `idempotent_counter` | 未显式配置 | nonce 账本布局；缺省时首次调用使用保守默认值，未调用不触发能力探测。 |
| `lock.prefix` | `DISTRIBUTED-LOCK:` | 分布式锁 key 前缀。 |
| `lock.lease_ms` | `30000` | 锁租约；有效范围 3s 到 300s。 |
| `stream.poll_timeout_ms` | `500` | 冷流轮询间隔，必须大于 0 且不超过 365 天。 |
| `stream.batch_size` | `100` | XREADGROUP 单批数量，范围 1–10000。 |
| `stream.data_expire_ms` | `3600000` | stream 数据保留窗口，最大 365 天。 |
| `stream.auto_trim_rate_ms` | `60000` | 自动 XTRIM 周期；0 表示禁用，非零最大 365 天。 |
| `stream.async_del_record_period_ms` | `5000` | ACK 后批量 XDEL 周期；0 表示禁用，entry 留在 stream；若启用 autoTrim，则按其保留窗回收。 |
| `stream.inflight_max` | `1000` | 分区消费全局在飞批次预算。 |
| `partition.enabled` | `false` | 是否启用分区消费。 |
| `partition.default_group` | `SINGLE-CONSUME` | 默认消费组。 |
| `partition.count` | `64` | 默认分区数，必须大于 0。 |
| `partition.rebalance_ms` | `10000` | 重平衡周期。 |
| `partition.min_idle_ms` | `30000` | PEL 消息可接管的最小 idle，最大 365 天。 |
| `partition.holds_check_interval_ms` | `5000` | owner 持锁状态的复核周期，最大 365 天。 |
| `partition.drain_timeout_ms` | `35000` | 停机等待 handler drain 的窗口；默认大于 30s handler timeout，最大 365 天。 |
| `partition.max_redeliver` | `5` | 连续重投上限。 |
| `partition.handler_timeout_ms` | `30000` | 单个 handler 桶执行超时，最大 365 天。 |
| `partition.poison_policy` | `Park` | `Drop`、`Park` 或 `Dlq`。 |
| `partition.groups` | `{}` | topic 隔离组，可覆盖分区数、batch、poll、超时和毒消息策略；最多 256 组。 |

启动代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    redis: nasa::redis::RedisConfig,
}

let client = nasa::redis::RedisClient::connect(cfg.redis).await?;
```

Cluster 使用注意：多 key 命令必须同 slot；普通业务 key 可使用 `{tenant}` 这类 hash tag。
分区运行时会自动给每个分区组生成组级 hash tag，并在启动时校验全部协议 key 同槽。
默认组与隔离组的 resolved 分区总数最多 65536，配置 topic 总数最多 4096；这些边界限制单实例
启动时的 task、channel 和扫描状态规模。隔离组覆盖的 poll、idle、持锁复核、handler 与 drain
时长也使用相同上限，不能绕过全局配置保护。
