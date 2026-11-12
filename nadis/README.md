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

JSON 业务类型解码失败只保留错误类别和行列位置，不传播输入值或自定义 `Deserialize` 的错误正文。
结构校验失败保留固定原因；业务自行实现的 `JobParameter::decode` 仍须保证其错误摘要不含敏感载荷。

JSON target-type decoding reports only the error category and line/column location, excluding input
values and custom `Deserialize` error text. Structural validation retains fixed rejection reasons.
Business implementations of `JobParameter::decode` must also keep sensitive payloads out of errors.

业务项目通过门面开启 `redis`：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["redis"] }
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

连接时必须取得明确的 `INFO cluster` 响应：唯一的 `cluster_enabled:0` 选择单节点传输，
唯一的 `cluster_enabled:1` 选择集群传输。字段缺失、重复或值非法时返回 `topology` 阶段错误，
不会写入协议标记；不能因为拓扑证据缺失而退回单节点模式。部署所用账号须允许该探测命令。

Connection setup requires an unambiguous `INFO cluster` response. A single `cluster_enabled:0`
selects standalone transport; a single `cluster_enabled:1` selects cluster transport. Missing,
duplicate, or invalid values fail during `topology` before any protocol marker is written.
Missing topology evidence never selects standalone transport. The deployment account must allow
this probe command.

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

`h_decr_by` 在发送前拒绝无法取负的 `i64::MIN` 步长；`set_range` 在发送前拒绝超出
`isize` 非负范围的字节偏移。服务端仍独立检查字段计数和字符串长度限制。
`x_group_create_if_missing` 仅将精确的 `BUSYGROUP` 错误码视为组已存在；其它错误会原样返回，
不会因为描述文本出现同名词而被当作成功。

`h_decr_by` rejects an `i64::MIN` step before sending because its negation cannot be represented.
`set_range` rejects byte offsets outside the nonnegative `isize` range before sending. Redis still
enforces counter and string size limits. `x_group_create_if_missing` treats only the exact
`BUSYGROUP` error code as an existing group; all other errors propagate, regardless of their text.

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

单节点和集群的能力探测都校验 `COMMAND INFO HPEXPIRE` 的唯一响应及命令定义结构；仅唯一的 nil 项表示
命令缺失。畸形定义、额外结果或错误响应表示能力未知，不能据此建立任何账本布局标记。

Both standalone and cluster capability probes validate the single `COMMAND INFO HPEXPIRE` result and
its command definition. Only a single nil entry confirms an absent command. Malformed definitions,
extra results, and error responses leave capability unknown and cannot establish a layout marker.

## RedisJob

直接依赖 `nadis` 时开启 `job` feature；业务经 `nasa` 门面时开启 `redis-job`。`nadis::job` 提供无全局 leader 的分布式任务运行时。多节点可以同时扫描，到期触发、领取、续租、完成、恢复和 Fanout assignment 都由 Redis 时间、CAS、owner、attempt token 与 fencing 收敛。普通任务可以共享 Worker Stream 与独立容量池；Fanout 目标必须声明为 `FANOUT_ONLY`，避免普通 Dispatcher 与定向 shard 竞争同一 Handler。

逐数据源配置可以覆盖任务命名空间和容量，但不能覆盖 `application_name` 或 `instance_identity`。
`JobConfig::resolve_source` 先检查根配置及原始覆盖，再校验合并结果，使同一进程的执行器身份保持一致。

Per-source settings may override the task namespace and capacities, but cannot override
`application_name` or `instance_identity`. `JobConfig::resolve_source` validates the root and
original overrides before validating the merged result, preserving one executor identity per process.

`FanoutCoordinator` 接受的根尝试次数为 `1..=i32::MAX`，与 Fanout 标识的整数编码一致。
超出范围会在建立持久 intent 前返回协议错误，避免整数截断使不同尝试共享标识。

`FanoutCoordinator` accepts root attempt numbers in `1..=i32::MAX`, matching the integer encoding
used by Fanout identifiers. Out-of-range values return a protocol error before creating a durable
intent, preventing integer truncation from assigning the same identifier to different attempts.

Fanout 清理请求携带构造分片键时的观察游标。并发请求已经推进游标时，旧请求返回零删除和当前游标，
调用方重新读取持久根后再继续，避免跳过分片。清理前检查游标范围、批量及键对数量；未对账或未到保留期
的根不会被删除。到期扫描严格检查四组索引的字段完整性和每组批量上限，在分配成员缓冲前校验声明数量
不超过实际响应能容纳的成员数；畸形响应返回协议错误。

Fanout cleanup carries the cursor used to construct its shard keys. If another caller has advanced
the cursor, the stale request returns zero deletions and the current cursor; the caller must read
the durable root again before continuing. Cursor ranges, batch sizes and key pairs are checked
before deletion. Unreconciled roots and roots still within retention are preserved. Due-index scans
validate the exact four-group layout and per-group limits, and bound member allocation by the actual
response length. Malformed responses return protocol errors.

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

Prepare 阶段逐 source 确认布局和能力后登记定义，全部准备成功才返回可启动计划；后续 source 失败不会撤销先前已提交的布局和定义。初次触发使用经过分量及毫秒范围校验的 Redis TIME，非法时间在登记定义前拒绝。Dispatcher 取得执行权后才调用 Handler。Handler 外部写入必须携带 `attempt_token` 并在副作用前调用 `checkpoint()`，租约失效、取消或本地保守截止到达后立即停止。扫描器不是单一 leader，重复扫描依靠 Redis 权威时间、CAS、assignment epoch 和 fencing 收敛。

Preparation validates each source and registers its definitions before returning a startable plan.
Failure on a later source does not roll back earlier layout markers or definitions. Initial trigger
time uses Redis TIME only after validating its components and millisecond range; invalid time is
rejected before definition registration.

Fanout 后台任务异常退出也属于执行权威失败：运行时必须取得已结束任务的退出结果并关闭 source 准入，
不会丢弃任务句柄后继续接收新工作。Handler 正常结束或被超时停止后，先关闭所有上下文副本的本地权威，
再释放业务 Future 并提交持久结果；完成请求的网络等待不能延长业务写入权限。

Abnormal Fanout task exits are authority failures: the runtime observes completed task results and
closes source admission instead of discarding failed task handles. Once a Handler completes or times
out, local authority is closed for every context clone before dropping its future and committing the
durable result. Waiting for that commit does not extend the Handler's permission to perform effects.

普通 Run 和 Fanout shard 的本地租约以请求发出前的单调时刻为基准，扣除安全余量，
响应等待与载荷读取时间不会额外延长持权窗口。首次领取的证据到达时已经过期，则不调用 Handler。
旧窗口已经到期后，即使迟到的续期响应表示服务端续租成功，也永久关闭该上下文并交给权威监督收敛；
租约仍有效时的正常续期可以推进截止点。外部存储仍须使用 attempt token 实施 fencing。

Ordinary runs and Fanout shards measure local lease windows from the monotonic instant before
sending each request, less the safety allowance. Response waits and payload reads do not extend
authority. An expired initial grant cannot invoke the Handler. Once the previous local window
expires, even a delayed successful renewal cannot reopen that context; authority supervision
handles the failure. Timely renewals can advance the deadline. External stores must still fence
writes using the attempt token.

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
旧截止前被采用的成功回包会把 Degraded 恢复为 Up；原窗口过期后，迟到结果不能推进本地截止或修订号。
即使超时 Future 同时拿到成功结果，也先在状态锁内复验旧截止，再采用新证据。
`NotFound`、权威 revision 不一致、协议响应非法或已确认截止耗尽
会让 source 进入 NotReady。监督器只记录 source、循环与封闭原因，不记录 endpoint、payload 或凭据。

Only a heartbeat accepted before the previously confirmed deadline can recover a degraded source.
A late response cannot advance its local deadline or revision after that window expires, even if
the timeout Future also observes a ready success. The old deadline is checked under the state lock
before the response is adopted. Shutdown still reports expired executor authority as an error.

命名空间暂停或恢复按调度分片逐一发布，不是跨分片原子事务。控制调用返回错误时，部分分片可能已经进入目标状态，调用方不得据此判定整个 source 已暂停或已恢复，必须使用同一 actor 幂等重试直至成功。`JobControl::namespace_governance` 聚合 `enabled`、`paused`、`untouched` 与 `unknown` 分片数、最近权威时刻和 actor；混合状态或未知持久值都会把 `divergent` 置真，供治理面告警与续作，但不会把逐分片操作包装为原子成功。该门禁只阻止普通 Run 取得新执行权：已经取得的 attempt 可以继续续期和完成，已经建立的 Fanout 批次也会继续投递和收敛。

Fanout 取消先校验根记录、身份、游标、批量和分片键对，再发布 `CANCELLING` 并逐批处理。
记录缺失或参数非法时拒绝，不新建残缺根；陈旧游标返回当前进度，由调用方重读后继续。
运行中分片只登记协作取消请求，保留执行器、令牌和租约，直到持有者结束或租约恢复流程接管。

Fanout cancellation validates the root, identity, cursors, batch bounds and shard key pairs before
publishing `CANCELLING`. Missing roots or invalid arguments are rejected without creating partial
records. A stale cursor returns current progress for a fresh read. Running shards retain their
owner, token and lease while cooperative cancellation waits for completion or lease recovery.

完成事件按近似长度上界和最长保留时间定期裁剪；长度限制可能先删除仍在时间窗口内的事件。
普通调度分片和 Fanout 桶都在任何裁剪前校验 Redis TIME 的分量及毫秒范围，非法时间直接拒绝。
裁剪由多个命令完成，后续失败不会回滚此前删除，最旧事件 ID 也只是读取时的观测值。

Completion streams are trimmed by approximate maximum length and maximum retention time; the
length bound may remove events still within the time window. Both ordinary shards and Fanout
buckets validate Redis TIME before any trim. The operations are not one transaction: later errors
do not undo earlier deletions, and the oldest event ID is only a point-in-time observation.

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

最后一份守卫开始解锁后，即使等待解锁的 Future 被取消，也会请求停止看门狗。取消不证明
远端已释放；已提交的续租仍可能完成，残留锁依赖最后一次服务端续租的租期回收。
业务副作用仍需复验持有状态，并在外部写入端实施 fencing。

Once the final guard starts unlocking, cancelling the unlock Future also requests watchdog
shutdown. Cancellation does not confirm remote release: a renewal already submitted may still
complete, so a remaining lock expires according to its last server-side renewal. Business side
effects still require ownership checks and fencing at the external write boundary.

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

Pub/Sub 是瞬时广播，重连只恢复订阅，不重放断线期间的消息。`next_message` 保留空正文和二进制正文，
只接收当前已确认频道或模式的消息；无法确认字段、连接结束或订阅状态未知时返回 `None`。
成功退订后，该来源在本地排队的消息也不再交付。

追加或取消订阅时，一旦开始等待结果便暂时停止信任当前连接。等待被取消或返回错误后，
最后确认的本地订阅列表保留，但后续拉取返回 `None`，后续订阅变更被拒绝；即使服务端明确拒绝也要求重连。
调用 `reconnect` 或 `reconnect_timeout` 会在独立连接上重放最后确认的全部频道与模式，
全部成功才替换旧连接并恢复使用；失败、超时或取消不会把半配置连接交给业务，也不会解除已有门禁。
初始 `sub/psubscribe` 不设置整体准备期限，调用方可以为等待设置预算；取消等待不能撤回已发送的命令。

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
本地时间容量也必须可编码；已超出时间位上限的配置在访问账本前拒绝，领取后再构造失败也不归还编号。
`ManagedSnowflake::generate` 在关闭、状态锁中毒或时间容量耗尽时返回错误，不交付 ID。
直接持有 `Snowflake` 时可用 `try_generate/try_generate_n` 处理同类错误；原有 `generate/generate_n`
在失败时 panic。时间耗尽不通过复用 workerId、重置账本或回绕时间位恢复。

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

分区管理命令先保存可查询结果，再投递操作 ID。重试必须保持动作与接纳窗口一致，即使短摘要
发生碰撞也不能复用不同请求；既有终态不再重复投递。TIME 分量和毫秒范围在写入前校验，动作
执行前重新比对持久动作。接纳超时只限制尚未开始的请求，不能据此中止已经开始的副作用。
网络错误不撤销先前写入，结果确认与后续消息清理也不构成一个跨命令事务。

Partition administration persists a queryable result before publishing its operation ID. Retries
must retain the same action and admission window even when short digests collide. Terminal results
do not produce another command. Invalid TIME components or millisecond overflow are rejected before
writing, and execution checks the persisted action before polling the action Future. Admission
timeouts do not cancel side effects already in progress; network errors do not undo earlier writes.

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
`drain_deadline_ms` 必须大于 0，所有 timer 时长不超过 365 天。关闭信号停止后续领取，已经开始的
读取、业务处理及确认由独立 owner 等待；空闲和退避等待可立即结束。预算耗尽后请求强停，
并在取得任务退出证据后返回，不能强制中断不让出执行权的同步处理器。
直接丢弃 `RunningProxy` 也会发起关闭，但不等待结果；需要确认退出时调用
`begin_shutdown()` / `wait_closed()`，或使用等待关闭的 `shutdown().await`。

正常停机在同一 Redis 脚本内复验每个消费者没有 pending，再删除消费者元数据，避免旧摘要与
删除之间的重新分配丢失 PEL。清理需要 `EVALSHA`、`SCRIPT LOAD` 及脚本内 `XPENDING`、
`XGROUP DELCONSUMER` 的权限。权限不足、脚本或回复异常返回 `ProxyCleanup::Unavailable`；
本地任务已经退出不等于清理已完成，网络错误也不能证明已发出的删除没有执行。

## RediSearch 文档

开启 `derive` feature 后可派生 `RedisDocument`。

JSON 文档共用 `to_json_omit_null` 编码：递归省略对象中的 null 成员，保留数组中的 null 位置。
编码失败返回只含类别与行列的 `Codec` 错误；捕获自定义 `Serialize` 展开时返回固定错误。
编码只调用一次 `Serialize`，不会重试或撤销其自身副作用，也不改变宿主 panic hook。
该边界不隔离进程终止或栈耗尽；ARRAY 编码失败发生在追加、覆盖或批量写命令发送之前。

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

`Query::new()` 与 `Query::default()` 都使用偏移 0、上限 10，且尚未标记显式分页。
调用 `limit(offset, count)` 才设置显式窗口，`count=0` 原样保留；ARRAY 查询拒绝显式分页和排序，
不会忽略这些选项后返回全部子文档。

`tag_in` 的空集合表示匹配空集：FT 渲染返回空串哨兵，ARRAY 渲染返回零宽切片 `$[0:0]`。
直接使用渲染器的调用方应遵守哨兵语义；空集合不能退化为全量查询或删除。
TEXT 输入把 ASCII 查询结构字符变为空格，保留非 ASCII 原文，不做 Unicode 归一化；
TAG 使用自己的字面量转义规则，非 ASCII 原文也保持不变。后缀和包含条件要求字段声明
`WITHSUFFIXTRIE`，并分别使用 TEXT 或 TAG 查询语法；TAG 输入中的标点和空格仍属于值本身。

ARRAY 查询中的字段名是 `DocMeta` 的查询别名；找到定义后，按实际存储的 `name` 或显式
`json_path` 读取子文档。未指定路径时，`name` 整体作为属性名，名称中的点号不会变成层级。
显式路径以 `$` 表示当前子文档，支持点号属性、引号括号属性和有符号 64 位数组下标；
例如 `$.details.state`、`$["details.state"]`、`$.items[-1].state`。
属性名中的引号和反斜杠可在括号路径中转义。通配、递归、联合、切片、过滤表达式、
控制字符和越界下标会在本地拒绝，避免将单值字段条件扩展为多对象选择。
这些规则同样约束数组的查询、计数和条件删除；FT 索引模式的路径能力独立于此限制。
按 ID 操作同样将 `id_name` 整体视为实际属性名；点号、空格和引号属于名称本身，
不会作为嵌套路径或过滤表达式执行。

ARRAY 的字符串比较值和子文档 ID 保留普通反斜杠；同时含两类引号、反斜杠紧接引号，
或含 U+0020 以下控制字符的值会在生成条件时返回配置错误。该限制用于避免查询、覆盖或
删除时改变原始身份，区别于属性名的转义能力。字符串 ID 的直接追加不会构造 ID 条件；需要随后按 ID
查询或覆盖的数据应符合上述限制。

数值 ID 必须能解析为有限数值。单条追加、批量追加和覆盖都会在发送写入前检查；
NaN、正负无穷不能作为数组文档身份，因为 JSON 编码会将它们转换为 null 并省略该字段。
批量追加会先准备全部文档，任一 ID 不符合约束时整批不发送。

ARRAY 追加和覆盖只接受与请求数量一致的 RESP 整数确认，批量追加按每个键的分组数量核对。
删除与分桶计数只接受非负 RESP 整数；负数、字符串数字和其它回复形态会返回错误。
这些检查约束完成确认，不能撤销已经执行的写入或删除；异常回复不提供安全重试依据。

ARRAY 读取的 JSON 正文必须是数组，缺失键和合法空数组表示没有匹配；空字符串、null 或对象
不会被当成空结果。存在性判断和普通数组计数只检查匹配列表，不构造业务实体。多桶读取的
每个片段必须是 UTF-8 BulkString，片段数不得超过请求桶数；任一片段解码失败则整个读取返回错误。
JSON 解码错误只包含类别和位置，不带正文或自定义错误消息；自定义 Deserialize 的展开仍向调用方传播。

## 生产注意

数组的数值 ID 按文本选择比较形式：可解析为 `i64` 的整数文本保留完整有符号 64 位精度；
其它形式只接受绝对值小于 `2^53` 的有限浮点值。超出 `i64` 的整数、边界外的小数或指数形式
在追加、批量追加、覆盖、按 ID 查询和删除前返回配置错误，避免以浮点转换选择相邻整数。
非零系数在浮点解析后下溢为零也会被拒绝，避免选择零身份；真正的正负零及带指数的零可以使用。
需要更大整数身份时使用字符串字段，并按业务迁移原数据；改变字段类型不会自动搬移数组或桶。
追加、批量追加和覆盖还会检查编码后的根对象：`id_name` 对应的属性必须存在，且类型、值与本次
`id()` 观察一致。字符串按原文比较；整型双方保持 `i64` 精度，涉及浮点的比较只允许绝对值小于
`2^53`。不一致返回配置错误，覆盖不会先删除旧记录，混合批次也不会先写入前面的有效文档。
该检查复用同一次 `Serialize` 的结果，不改变 ID 文本、字段编码或桶布局。例如 `f32` 的 `0.1`
经现有 JSON 值编码为 `0.10000000149011612`，与 ID 文本不同，因此拒绝；`0.5` 可精确表示，仍可使用。
需要十进制身份时使用字符串字段，并显式迁移已有数据；本地检查不会清理已有的不一致或混合数值记录。
数值等价的 ID 文本仍可能散列到不同桶，查询和删除应使用写入时相同的 ID 文本；不会自动跨桶查找别名。

ARRAY 追加、同键批量追加和覆盖在脚本内先通过 `JSON.TYPE` 检查已有根值。缺失键可以初始化，
已有 JSON 对象、null、字符串及其它标量会在写入或删除旧成员前拒绝；普通 Redis 非 JSON 键也返回错误。
该检查不会把错误布局自动改成数组。脚本执行隔离不提供失败回滚：通过根类型检查后，后续命令的
权限或执行错误仍可能保留前序副作用；覆盖报错时旧记录可能已删除。跨键批次也不回滚已经完成的键，
调用方必须核对持久状态，不能把错误当成安全重试依据。

按 ID 查找、判断存在、覆盖或删除完整 i64 整数时，绝对值达到 2^53 的条件若命中已有浮点 ID，整个操作会拒绝。
类型复验与读取或删除在同一个 Lua 中完成；只含整数的匹配保留完整 i64 范围，无命中时覆盖仍可追加、删除返回零。
该门禁不清理或改写历史混合数据。通用 `Query` 的数值参数为 `f64`，大整数可能在传入前就已舍入；
例如 `9007199254740993_i64 as f64` 已变成 `9007199254740992`。ARRAY 路径沿用 RedisJSON
的数值比较，混合整数与浮点记录也可能同时匹配相邻业务值；`remove_in_array` 会删除这些匹配。
需要精确整数身份定位时，使用上述按 ID 入口传入原始整数文本，或使用字符串字段；
数值范围查询保持浮点语义，不会自动把数字改成字符串，也不会自动改写已有桶的身份文本。
直接调用 `REPLACE_OR_APPEND` 脚本时还必须提供第三个参数：需要保护时传命中项的 ID 属性路径，
其它身份传空字符串；省略第三个参数会在任何写入前拒绝。

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
关闭会中断读取、回调及确认等待，已发送的命令仍可能在远端生效；订阅不删除消费者或自动回收 PEL。
`observation().unfinished` 只统计已收到并登记的本地责任，不能从零值推断远端没有待确认消息；
尚未取得读取回复时的领取结果须由外部恢复流程核对。

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
Proxy 强制关闭时跳过消费者清理；正常关闭只在原子查询确认该消费者没有 pending 时删除。
查询或脚本失败不把未知 pending 当作零；已发出的脚本结果不明时，空消费者可能已经被删除。
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
