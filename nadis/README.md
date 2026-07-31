# nadis

`nadis` 是 Redis 基础层，覆盖连接、常用命令、pipeline、分布式锁、leader、Pub/Sub、Stream、分区消费和可选 RediSearch/RedisJSON。它把旧系统兼容协议和纯 Rust 增强协议用 `CompatibilityProfile` 明确区分，避免业务无意混用持久化边界。

业务项目通过门面开启 `redis`：

```toml
[dependencies]
nasa = { version = "1", features = ["redis"] }
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
