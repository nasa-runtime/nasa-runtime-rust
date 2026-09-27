# nadis

`nadis` 是 Redis 基础层，覆盖连接、常用命令、pipeline、nonce 幂等计数、分布式锁、leader、Pub/Sub、Stream、分区消费和可选 RedisJob、RediSearch/RedisJSON。它把旧系统兼容协议和纯 Rust 增强协议用 `CompatibilityProfile` 明确区分，避免业务无意混用持久化边界。

它解决两类容易被业务代码错误拼装的原子边界：计数写与 nonce 凭证在同一 Lua 内结算，分布式任务的调度、租约、fencing、Fanout 与完成状态也只通过封闭脚本迁移。业务只调用类型化 API，不接触账本命令或裸 Lua 返回值。

分区消费由 Redis 负责持久接管，每个 `RunningPartition` 独占 napart Runner 集合；默认按源共享，
可配置 `group` 或 `stream` 隔离慢组或慢物理 Stream 的调度与容量。不同 Redis 源始终独立。业务键
顺序覆盖 handler、ACK 和精确重试。ACK 结果不确定时保留提交责任，不重跑成功 handler。全部消费组
受源级有界记录与正文总预算约束，隔离模式将预算分为固定域份额。正常停机超时可以继续等待同一个排干操作。

普通 Stream、共享消费组 Proxy 与 AutoPipeline 可由 Application 按命名计划管理：统一放行消费和
调用、限制微批参数字节、等待真实任务退出，并在缺少 PEL 证据时保留 consumer。业务无需另建消费
或关闭 owner；具体配置与边界见[受管入口](#普通-streamproxy-与微批的受管入口)。

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

`nasa::redis::Json<T>` 使用标准 serde_json 编码。需要处理序列化失败时，先调用
`Json(&value).to_bytes()?`，再把字节交给 Redis 命令；直接作为命令参数时，序列化失败会 panic。
浮点 NaN 和无穷会编码为 JSON null，要求有限数值的业务必须先校验。整数 map key 可以编码为
字符串键，数组等复合 key 会被拒绝；解码不会根据类名或类型元数据自动选择业务类型。

Cluster 多 key 命令会做同 slot 守卫；需要跨 key 时请使用 `{tag}` 约束 slot。分区运行时以
“分区组”为同槽单位：同组所有 stream、锁、marker 和控制 key 固定到一个 slot。`partition.count`
增加的是组内并发，不会把单组分散到多个 master。`partition.groups` 可拆出不同 hash tag，
但不同组仍可能落在同一 master；需要跨 master 扩吞吐时须同时核对实际 slot 分布。

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

每个定义的 `qualifier` 是强路由字段。`prepare_sources` 接受冻结的 `RedisJobSources`，每个 source 独立拥有客户端、namespace、键布局、连接角色、监督代次、健康、指标和停机边界；未知、禁用或 client 身份不一致的 source 在写定义前拒绝。独立运行时可用 `shutdown_source(qualifier, budget)` 只排空一个 source，其它 source 继续领取和续租；全量停机使用 `shutdown(budget)`。启动先建立 Fanout 订阅再逐项登记能力；任一登记失败都会关闭本地准入，并按全部已尝试 Worker 坐标尝试全量注销，包含“脚本已提交但回包丢失”的能力。若 Redis 持续不可达而无法取得注销确认，本地不再接纳或处理工作，服务端成员与能力记录只由 TTL 和 Registry GC 收敛。`JobControl` 提供 trigger、单任务 pause/resume、当前 source 的 `pause_namespace`/`resume_namespace`、cancel、delete 与 conflict resolve；命名空间暂停阻止新的普通 Run 触发、尚未领取的普通 Run 和可见性消息重建，不撤销已有 attempt，也不影响其它 source。普通定义不宣告 Fanout 能力，删除 `FANOUT_ONLY` 定义会在 tombstone 提交后同步撤销执行器能力，撤销失败可用相同定义修订号幂等补偿，避免新 Fanout 快照继续选择已失去 Handler 的节点。`JobQuery` 提供 definition、run、Fanout、健康和无 Redis I/O 的指标快照。

停机先发布 `Draining` 并关闭新准入，再等待监督循环与已接纳 Handler。绝对期限到达时会对剩余 Tokio task 提交 `abort`，但注销 Registry 前仍会逐个 await `JoinHandle`，直到 future 实际结束、权威 guard 析构且容量许可释放；取消请求本身不作为所有权已终止的证据。

执行器心跳以最近一次 Redis 成功回包中的 `redisNow/expireAt` 折算本地保守权威截止，并使用不与
其它状态脚本共享断链结局的独立连接 lane。单机装载、Cluster 切换或槽迁移产生的
`LOADING`、`TRYAGAIN`、`CLUSTERDOWN`、`MASTERDOWN`、`MOVED`、`ASK`、`READONLY`
以及 Redis 的 `ERR max number of clients reached` 容量响应与传输失败一样，只能在该截止前有上限退避；
认证、ACL、本地客户端配置、RESP 解析、返回类型、其它 `ERR` 文案、跨 slot 或脚本合同错误立即拒绝；
兼容实现不得依赖未声明的模糊文案匹配，也不得把确定性的凭据或协议错误延迟到租约截止。
受监督运行时为每个逻辑心跳携带稳定请求 ID 和最后确认的 `heartbeatRevision`，因此传输重发只读取
第一次执行结果，不会重复延长服务端存活期；脚本也拒绝续期已经到期但尚未 GC 的记录。第一次续租若
已在 Redis 执行但其结果始终无法在旧截止前取回，本地仍会立即关闭准入，服务端成员可能保留到该次
续租的 TTL 到期；Fanout 投递必须依赖持久 receipt 和失联重分配收敛，不能把成员记录等同于进程确认。
后续成功会把 Degraded 恢复为 Up；`NotFound`、权威 revision 不一致、协议响应非法或已确认截止耗尽
会让 source 进入 NotReady。监督器只记录 source、循环与封闭原因，不记录 endpoint、payload 或凭据。

命名空间暂停或恢复按调度分片逐一发布，不是跨分片原子事务。控制调用返回错误时，部分分片可能已经进入目标状态，调用方不得据此判定整个 source 已暂停或已恢复，必须使用同一 actor 幂等重试直至成功。`JobControl::namespace_governance` 聚合 `enabled`、`paused`、`untouched` 与 `unknown` 分片数、最近权威时刻和 actor；混合状态或未知持久值都会把 `divergent` 置真，供治理面告警与续作，但不会把逐分片操作包装为原子成功。该门禁只阻止普通 Run 取得新执行权：已经取得的 attempt 可以继续续期和完成，已经建立的 Fanout 批次也会继续投递和收敛。

手工 trigger 在存在环境 trace 时只记录 `trace_id` 到派生 `run_id` 的映射；业务 `request_id` 原文
不进入日志。Run hash 与共享 Lua 的跨语言线协议在所有实现共同扩展前保持不变，因此 worker 侧
暂不从 RedisJob 持久状态恢复该 trace。

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

Redis 分配使用显式初始化、编号永久不复用的账本。管理方须从账本外证明首次初始化权威，
并保证已确认领取记录不会丢失或回退；不存在的 key 不能证明这是首次部署。

```rust
use nasa::redis::{SnowflakeConfig, WorkerIdNamespace};

let cfg = SnowflakeConfig::default();
let namespace = WorkerIdNamespace {
    incarnation: "orders-worker-space".into(),
    first_worker_id: 0,
    last_worker_id: 63,
};
// 管理方确认整个 ID 空间未使用后，独立执行一次 namespace.initialize(&client, &cfg)。
let (sf, lease) = namespace.allocate(&client, &cfg).await?;
let id = sf.generate();
lease.release().await?;
```

缺失账本、身份/位布局不符或容量耗尽均拒绝分配；`release` 不归还编号，没有 TTL 自动回收。
`alloc_worker_id/build_with_redis` 始终返回错误。incarnation 不进入 ID 位编码，改名不保证新旧 ID 空间隔离。
`build_local` 固定 workerId=1，同一 ID 空间只允许一个共享生成器；重启须越过上一实例的逻辑时间上界。
Application 使用 `redis.snowflake.<name>` 与 `app.snowflake(name).await`，由框架领取和关闭生成器。

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

`PreparedPartition::try_register_legacy` 在注册点拒绝重复 `(topic,event)`，并保留原 handler；
兼容链式 `register` 保存首个注册错误，随后由 `start` 返回。topic/event 必须非空、无首尾空白、
无控制字符且不超过 256 字节。没有任何 handler 的消费运行时不能启动。

`prepare` 只准备 Stream/group；`start` 复验全部路由和远端合同。全部组的监督任务就绪前不抢锁、
不读取、不调用 handler，启动事务提交后才一次性开放消费。远端 group 在 prepare 后被删除时
会幂等补齐；Stream 类型不符等错误会使启动失败，并关闭尚未激活的任务。

批量 handler 仍按 `(topic,event)` 接收一次成功解码子集的 `Vec<T>`，单条类型解码失败独立留在
PEL。一次批量调用返回失败、超时或 panic 时，该次传入的成功解码子集整体进入重投。

## 业务键有序分区消费

`register_partitioned` 将多个 topic 绑定为一个不可变消费计划，逐条 handler 接收
`PartitionRecord<T>`，包含已解码数据、完整 `(stream,id)` 和显式 passthrough。key 提取一次后，
同一摘要同时用于本地 gate 与 napart strict 路由；没有 key 的记录使用 relaxed 类型。兼容批量
入口仍按 Vec 调用和重投。

同一实例、同一计划的业务键顺序覆盖全部物理组。跨进程串行要求生产者把相同业务键路由到相同
物理 Stream；系统仍为 at-least-once，业务必须幂等。`RustV2` 使用原子 fencing，`LegacyV1` 的
holder 复验不能提供相同强度的原子拒绝。

注册示例、容量配置、恢复屏障、发布未知结果、停机报告及能力边界见
[Redis 分区消费](docs/partition.md)。Redis 消费器自己拥有执行器生命周期，不使用 Application
配置的命名 Runner。使用 Application 时通过 `configure_redis_partition` 登记计划，
由 redis 子能力统一管理 Ready、逐源健康和聚合停机。

### 执行域与消费架构

每个 `RunningPartition` 都拥有私有 Runner 注册表。不同 Redis 源互不共享本地任务、容量或停机
控制；同源内通过 `partition.executor.scope` 决定隔离粒度：

| 模式 | Runner 数量 | 容量使用 |
| --- | --- | --- |
| `source`（默认） | 1 | 全部组使用同一份源级预算 |
| `group` | 默认组与隔离组的总数 | 每组获得固定份额 |
| `stream` | 全部组的物理分区数之和 | 每个物理 Stream 获得固定份额 |

```text
来源持锁与 PEL 恢复
        ↓
读取前预留 record / payload / batch / 后继责任
        ↓
共享记录账本 + 同计划同业务键顺序门禁
        ↓
来源所属 Runner → 受监督业务任务 → 稳定执行结果
                                    ↙        ↘
                            独立提交 ACK    原坐标精确重试
                                  ↓
                            有界异步 XDEL
```

每域有独立的提交和重试监督，网络等待不持有共享账本锁。同计划同业务键的后继仍需等待原记录
确认或处置完成。ACK 已确认而删除队列满时，Commit 保留删除交接责任，业务顺序门禁可以继续推进。
域表和份额在启动前固定，再平衡或重获租约不重新分配；所有配置的 Stream 都占份额，包括当前未
持锁的 Stream。空闲域不借出容量，源级总量也不会随 Runner 数量扩大。

以下配置提供两个组、每组两个物理 Stream；`stream` 模式共有四个 Runner，每个 Runner 两个本地槽。

```yaml
redis:
  stream:
    batch_size: 16
  partition:
    enabled: true
    count: 2
    groups:
      notifications:
        count: 2
        topics: [notifications]
    executor:
      scope: stream
      partitions: 2
      max_runners: 4
      max_total_partitions: 8
    limits:
      max_inflight_records: 256
      max_record_bytes: 65536
      max_inflight_payload_bytes: 16777216
      max_active_batches: 16
      max_async_delete_records: 256
```

此片段合并进带 `url`、`namespace` 和 `profile` 的 Redis 配置。其它记录后继上限采用默认值时，
每域可保留 64 条记录、4 MiB 估算正文、4 个读取批次和 64 个待删 ID。正文同时覆盖原始 Envelope
与解码对象，单条线格式上限不等于对象存活内存上限。最低批次预留不够时 `start` 拒绝启动。

执行隔离不提供独立线程、Redis 连接或外部服务；共享连接阻塞、同步阻塞线程仍可影响多个域。
`snapshot()` 同时报告聚合责任与 `execution_domains`，可区分域满载、来源 Park、提交未知和 Runner
降级。正常停机覆盖全部域，任一任务、I/O 或锁未取得退出证明时 `converged` 保持 false。

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

Application 受管 Redis 客户端由 `"redis"` 组件关闭，业务一次性收尾可以在其关闭前使用
`app.register_graceful_shutdown`，不应重复关闭同一个受管客户端。业务自己启动的 Stream、Partition 或
其它长期消费循环，应先由对应 owner 停止准入并确认退出；一次性 future 不能替代运行期间的任务监督。

- `RedisConfig` 的 `url` 在 Debug 输出中会脱敏，但不要把明文连接串写进仓库。
- `RustV2` 和 `LegacyV1` 的分区/锁/stream 协议不要混同一 namespace。
- 非幂等命令遇到 Redis IO 错误时，框架不会透明重试；调用方要按业务处理“执行状态未知”。
- ACK 后异步 XDEL 只回收 stream 空间，不改变确认语义；删除暂时失败会在后续周期重试，
  停机做末次 flush。删除 owner 退出时，剩余待删份额转为 `delete_retained_records` 并归还容量，
  不撤销 ACK，也不重跑 handler。末次删除仍失败时 entry 会保留在 stream；仅在 autoTrim 同时启用时，
  才会由其保留窗继续回收。持续删除失败会先积累有界待删 ID，再通过队列反压暂停消费；
  这是为了避免 ACK 已成功后静默遗失待删 ID。应把 `RunningPartition::async_delete_pending()`
  接入运行时 gauge，并对持续非零告警。
- Partition 正常停机应显式调用 `RunningPartition::shutdown().await`。直接 Drop 会关闭准入并启动
  best-effort drain。必须检查返回报告的 `converged`；等待超时后可再次等待同一个操作，只有显式
  `force_shutdown_until` 才请求有损中止，不能把取消请求当作 handler、I/O 或锁已退出的证明。

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
| `stream.inflight_max` | `1000` | 每物理组同时读取批次数上限，还受 `partition.limits` 共享预算约束。 |
| `partition.enabled` | `false` | 是否启用分区消费。 |
| `partition.default_group` | `SINGLE-CONSUME` | 默认消费组。 |
| `partition.count` | `64` | 默认分区数，必须大于 0。 |
| `partition.rebalance_ms` | `10000` | 重平衡周期。 |
| `partition.min_idle_ms` | `30000` | PEL 消息可接管的最小 idle，最大 365 天。 |
| `partition.holds_check_interval_ms` | `5000` | owner 持锁状态的复核周期，最大 365 天。 |
| `partition.drain_timeout_ms` | `35000` | 普通停机本次等待窗口，超时返回未收敛报告并继续排干；最大 365 天。 |
| `partition.max_redeliver` | `5` | 连续重投上限。 |
| `partition.handler_timeout_ms` | `30000` | 单个 handler 桶执行超时，最大 365 天。 |
| `partition.poison_policy` | `Park` | `Drop`、`Park` 或 `Dlq`。 |
| `partition.groups` | `{}` | topic 隔离组，可覆盖分区数、batch、poll、超时和毒消息策略；最多 256 组。 |
| `partition.executor` | `scope: source`，每 Runner 8 槽、4096 在途 | 支持 `source`、`group`、`stream` 隔离；域数、总槽数和固定容量份额在启动前校验，详见分区消费说明。 |
| `partition.limits` | 4096 条，256 MiB 正文 | 默认组与隔离组共享的发布、读取、任务、后继、提交、重投、gate 和删除预算。 |

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

## 普通 Stream、Proxy 与微批的受管入口

组合 `application,redis` 并声明 `"redis"` 后，`redis_streams`、`redis_proxies`、`redis_pipelines`
提供命名计划。前两项在 Service UserHook 只登记 handler，Prepare 建立 owner，统一 Ready 后消费；
AutoPipeline 也支持 Batch。具体 YAML 和 getter 见
[napp](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#redis-streamproxyautopipeline)。

### 运行架构与责任边界

```text
命名配置 + handler 计划 → 受管 Redis 来源 → Prepare 建立领域 owner
        → Ready 与共享启动许可 → 消费 / Proxy 发布 / 微批入队
        → 关闭新工作 → 同一截止点排干与清理 → 归还 Redis 来源
```

普通 Stream 按显式读取模式处理记录；组模式的 `on_success` 在 handler 失败时保留 PEL，不自动重投。
Proxy 以共享组竞争、回收和毒消息策略处理自己的信封格式；两者不能共用同源同 stream/group。
AutoPipeline 合并命令传输，既不提供 Redis 事务，也不提供业务键有序消费。需要分区执行与业务键
顺序时使用 `PreparedPartition` / `RunningPartition`。Service 的命名发送入口在放行前明确拒绝，
Batch 仅装配 AutoPipeline，不创建长期 Stream 或 Proxy 消费计划。

独立 `start()` 保持立即激活；需要准备屏障时使用 `start_suspended()` 后显式 `activate()`。
多个入口共同开放时使用 `start_suspended_with_activation(token)`，仅由宿主取消共享 token 发布许可，
不逐项调用 `activate()`。受管入口与宿主终端共用许可，公开 Ready 后首次调用无需等待监控激活。
Stream、Proxy、AutoPipeline 的 `with_running` 在本地任务责任完整的保护内执行短小同步裁决，
闭包不能阻塞或重入领域句柄；已退出的关键 owner 阻止 Ready 发布，不把本地存活等同远端可用。
Stream 的 `begin_shutdown()` / `wait_closed()`、Proxy 的 `begin_shutdown_until()` / `wait_closed()`
和 AutoPipeline 的 `shutdown_result()` 都保留真实退出证据，可在前一次等待被取消后再次等待。
Proxy 强制关闭、查询失败或摘要不完整时保留 consumer/PEL；不把未知 pending 当作零。
`ProxyStopReport.cleanup` 区分完整清理、合法 pending、证据不可用和预算耗尽，后两类在宿主中
进入次要停机失败。删除回包超时保留结果未知。收尾 owner 被执行器销毁时，旧入口关闭，状态读取
报告失败；Proxy 未取得全部 join 证据时保持 `terminated: false`，不会把任务取消冒充优雅退出。

受管微批要求有限单条参数字节 M 和单批软上限 B，正常合批与排干均不超过 B＋M 的保守参数字节边界，
每批同时受条数限制；独立 `MicroBatchCfg` 的零字节限制仍表示不限。队列上界只计算已接纳命令参数，
不涵盖编码、响应、Cmd 预留容量及等待中的调用者。关闭拒绝新的入队，已接纳命令逐项返回执行结果或
`ExecutionUnknown`，不会自动重放写入。`submit` 没有逐条回执需求时才适用。

三个领域句柄的 `observation()` 返回本地任务数、处理进展与未完成责任；微批同时提供排队参数字节
和传输失败批次数。`completed` 表示本地处理结束，包含 handler 失败及结果未知，不是业务成功计数。
同一消息重投可形成多次处理，未完成次数不等于 PEL 大小。最近进展为 None 时尚无可验证的运行进展；
有效空轮询不会被误判为消费失败。`barrier()` 只建立已入队工作的处理顺序，不汇总此前写入的错误。
