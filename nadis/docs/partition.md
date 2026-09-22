# Redis 分区消费

`PreparedPartition` / `RunningPartition` 将持久消息接管与本地业务执行分开：Redis Stream、PEL、租约和
fencing 负责跨进程所有权，`RunningPartition` 独占的 napart Runner 集合负责本地执行。
业务成功后由独立提交责任完成 ACK；ACK 响应不确定时只查询 PEL 或重试 ACK，不重新调用成功 handler。

不同 Redis 源的 `RunningPartition` 各自拥有独立的 napart Runner、执行槽、任务队列、容量预算与
停机控制。默认 `source` 模式下，本源全部组和物理分区共享一个 Runner；`group` 模式按逻辑组隔离，
`stream` 模式按物理 Stream 隔离。即使各实例使用相同 Runner 名称，也只在各自独占的注册表内
解析，不会跨源复用执行域。

## 执行模式与拓扑

`partition.count` 表示默认组的 Redis 物理 Stream 数，隔离组可以覆盖自己的 `count`。
`partition.executor.partitions` 则是每个 napart Runner 的本地槽数，不改变 Redis 路由和 key 布局。

| 模式 | 冻结执行域 | 慢 handler 的主要影响范围 |
| --- | --- | --- |
| `source` | 整个 RunningPartition 一个域 | 共享 Runner 的本地队列与源级容量 |
| `group` | 每个默认组或隔离组一个域 | 所属组的 Runner 与固定份额 |
| `stream` | 每个 `(逻辑组, 物理分区编号)` 一个域 | 所属物理 Stream 的 Runner 与固定份额 |

域表按全部已配置来源建立，包括尚未持锁的 Stream；不按 topic、业务键、消费计划或当前 claim
数量动态生成 Runner。默认组的逻辑 ID 为 `""`。域索引按逻辑组排序、物理编号递增确定，同一
实例再平衡或租约重获不改变索引；拓扑或模式变更需重建运行时。

选择模式时还要保留跨域业务顺序与外部依赖边界：同计划同 key 后继会等待原 gate，所有域仍使用
同一个 Redis 客户端和 Tokio runtime。增加 Runner 不会使同 key 并行，也不增加源级总预算。

## 顺序与责任

```text
Redis 分区锁 → 来源恢复 → 公平读取预算 → 整批解析与冻结 ConsumerPlan
                                         ↓
                           精确坐标账本 + 业务键 gate
                                         ↓
                         TaskTicket → 来源所属 napart Runner
                                         ↓
                       业务结果 cell + 稳定 Submission 终态
                              ↙                     ↘
                         Commit                    Exact Retry
                      PEL 对账 / ACK              原坐标重新读取
                           ↓                           ↓
                     有界异步 XDEL                 同一 dispatcher
```

本地顺序域是 `(RunningPartition, ConsumerPlan, RouteHash)`。同一个计划可以覆盖多个 topic 和物理组；
相同业务键在这些来源间串行，包括属于不同 Runner 的来源。不同计划互不共享 gate。不同键可以
并行，但落到同一 Runner 的相同 napart home 与 strict task type 时保守串行；摘要碰撞也保守合并。
`key_fn` 返回 `None` 的逐条消息使用独立 relaxed
类型，一条消息对应一个 Task，不承诺业务顺序。

这不是跨进程业务键锁。需要集群内同 key 串行时，生产者必须用同一规范化业务键路由到相同的物理
Stream。隔离组使用不同 Stream，即使发布相同 key，也不能据此承诺跨进程串行。交付语义仍为
at-least-once：进程在产生业务副作用后、ACK 前退出，新 owner 可能再次执行业务。

gate 的业务状态与确认状态分别推进。一个 Task 可以产生成功前缀、失败头和未调用的后缀；成功前缀
确认之前不重投失败头，失败头解决之前后缀不能越过。ACK 已确认而删除队列满时，Commit 继续持有
删除交接容量，但该记录不再阻塞业务顺序，也不会变回 ACK UNKNOWN。

账本身份是完整 `(group, stream, id)`，与当前 consumer、claim epoch 分离。重获同一分区不会覆盖旧
任务的责任。一次持锁期的 epoch 不随批次递增；batch sequence、gate token 和 task identity 各自独立。
payload 只由准备区、正在执行的 Task 或短期精确读取持有，重试和提交账本只保留坐标及元数据。

再平衡按存活节点计算目标份额，由 Claim coordinator 根据当前 Active 来源决定额外让出数量。
Quiescing 来源继续持锁并完成已有责任，但不再计入应保留的 Active 份额；重复通知不会继续关闭
已达到目标份额的来源。Park 仍占持有份额，不自动让出。真实排干和 unlock 后才发布组 wake。

## 注册与使用

```rust
use nasa::redis::{DistributedLock, PartitionRecord, PreparedPartition};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct OrderEvent {
    event_id: String,
    order_id: String,
}

let lock = Arc::new(DistributedLock::new(client.clone()));
let mut prepared = PreparedPartition::prepare(client.clone(), lock).await?;
prepared.register_partitioned(
    ["orders", "order-updates"],
    "changed",
    |data: &OrderEvent| Some(data.order_id.clone()),
    |record: PartitionRecord<OrderEvent>| async move {
        let _business_id = &record.data.event_id;
        let _redis_coordinate = &record.identity;
        let _context = &record.passthrough;
        Ok(())
    },
)?;
let running = prepared.start().await?;
```

`PartitionRecord<T>` 暴露 `identity.stream`、`identity.id`、`topic`、`event`、`data` 与可选
`passthrough`。Redis 坐标只能定位一条物理记录；重复 XADD 会产生不同 ID，业务幂等应使用生产者携带
的稳定事件 ID。框架不自动从 passthrough 恢复环境上下文。

一次准备中 `T` 只反序列化一次，key 提取与哈希只执行一次；已经冻结的 `RouteHash` 同时用于 gate 与
napart。精确重投会重新读取并准备记录，key 回调必须稳定、无副作用。需要准确估算集合、字符串等
额外堆内存时使用 `register_partitioned_with_weight`；回调返回 T 的额外堆分配字节估算，框架另计
原始 Envelope 长度与 `size_of::<T>()`。

`try_register_legacy::<T, _, _>(topic, event, handler)` 保留批量 `Vec<T>` 入口：同一桶的成功解码
子集一次调用，单条类型解码失败独立进入 poison；Vec 失败、超时或 panic 后，仍按该失败向量重建
批量调用。兼容 `register` 保存首次注册错误，由 `start` 返回。重复路由不会替换原 handler；多 topic
注册先校验全集再发布。空计划、空名称、首尾空白、控制字符和超过 256 字节的名称均拒绝。

handler 必须协作让出，不能派生脱离监督的副作用任务。异步 timeout 或 cancel 不能终止不让出的
同步代码，也不能撤销已提交的外部业务写入。外部写入需要业务幂等或自己的 fencing 协议。

## 容量与配置

`partition.limits` 属于一个 `RunningPartition`：全局字段给出源级总额，`*_per_source` 与
`*_per_key` 分别约束单个来源和业务键。读取前同时预留
batch、record、估算 payload bytes 和每条记录可能产生的最坏后继容量；handler 成功后不再申请可能
失败的 commit/retry 许可。此策略偏保守：有效记录上限取 record、task、continuation、commit、retry、
gate、deferred（包括每 key）和 unroutable 各上限的最小值。

下方 `redis.partition` 适用于单源。Application 多源使用
`redis.properties.<qualifier>.partition`；同一源的全部域使用相同 executor 参数，组级覆盖只决定
物理布局、读取与消费策略，不覆盖 executor 或源级 limits。Executor 设置不进入远端协议 marker，
改变本地执行模式不会修改现有 Stream、consumer group 或跨语言线格式。

```yaml
redis:
  partition:
    executor:
      scope: source
      max_runners: 256
      max_total_partitions: 4096
      partitions: 8
      queue_capacity_per_type: 256
      global_inflight: 4096
      max_type_states: 4096
    limits:
      max_inflight_publishes: 128
      max_inflight_publish_bytes: 67108864
      max_inflight_records: 4096
      max_inflight_payload_bytes: 268435456
      max_active_batches: 256
      max_read_waiters: 4096
      max_record_bytes: 1048576
      max_inflight_tasks: 4096
      max_continuations: 4096
      max_pending_commits: 4096
      max_commit_records: 4096
      max_async_delete_records: 4096
      max_retry_tickets: 4096
      max_ordered_keys: 4096
      max_blocked_keys_per_source: 4096
      max_deferred_records_per_key: 4096
      max_deferred_records: 4096
      max_unroutable_records: 4096
```

| 执行配置 | 含义 |
| --- | --- |
| `executor.scope` | `source`（默认）共用一个 Runner；`group` 每个默认组或隔离组一个 Runner；`stream` 每个 `(逻辑组, 物理分区编号)` 一个 Runner |
| `executor.max_runners` | 完整配置拓扑的 Runner 数上限，默认 256，允许 1..=4096；不按当前持锁分区数缩减 |
| `executor.max_total_partitions` | 全部 Runner 的规范化本地槽数之和上限，默认 4096，必须大于零 |
| `executor.partitions` | 每个 Runner 的本地执行槽数，默认 8；输入允许 1..=65536，向上取整为 2 的幂后参与总槽数与类型容量校验，与 Redis 物理分区数不同 |
| `executor.queue_capacity_per_type` | 每个 Runner 中各 `(home, task type)` 的等待许可上限，默认 256 |
| `executor.global_inflight` | 每个 Runner 的任务总许可上限，默认 4096；仍受源级记录预算与域份额约束 |
| `executor.max_type_states` | 每个 Runner 的类型状态上限，默认 4096 |

`group` 和 `stream` 在启动前按完整拓扑划分固定份额：每域先保留一批记录、
`batch_size × max_record_bytes` 正文、一个读取批次和一个异步删除位置；源级余量按域均分，
余数按逻辑组排序、物理编号递增的顺序分配。记录总额采用上述有效记录上限。
任一资源不足以覆盖全部域最低需求时，启动返回配置错误。默认 `source` 取得全部预算。
例如两个域的 batch 均为 1、有效记录总额为 8，则每域有 4 条记录额度。

设域数为 `D`、各域最低需求为 `m[i]`、源级总额为 `T`：先要求 `T >= sum(m)`，再给每域
`m[i] + floor((T - sum(m)) / D)`；前 `(T - sum(m)) % D` 个域再获得一个单位。
`source` 的 batch 下限取各组 resolved batch 的最大值，`group` 和 `stream` 使用所属组的 batch。
读取批次和删除位置的最低需求均为 1，删除未启用也保留配置容量校验。四类份额之和分别等于
对应源级上限；Task、Commit、Retry 等后继仍受有效记录上限约束。

仅把默认配置改为 `scope: stream` 会因预算不足而拒绝启动：默认 64 个物理分区、每批 100 条、
每条线格式上限 1 MiB，合计最低需要 6400 条记录额度和 6.25 GiB 正文额度，超过默认的 4096 条
与 256 MiB。应按业务报文大小和读取批量调整完整配置，不能只增加 `max_runners`。

域份额不借出：一个域的慢 handler、提交积压、精确重试或删除积压不会用完其它域的保证份额。
空闲域的剩余额度也不会自动转给繁忙域。份额在再平衡、释放和重获租约时保持不变，需要调整时
重建运行时。正文额度包含线格式与解码对象，最低读取份额不保证任意解码对象都能执行；应按实际
对象权重增加 `max_inflight_payload_bytes`。每域还有独立的提交与精确重试监督循环，Redis await
不占共享账本锁。

这些模式隔离本地调度和容量，不提供进程、线程或外部服务隔离。各域仍共享 Tokio runtime、
Redis 后端及客户端，组内异步 XDEL 仍由一个组级 owner 推进，Publisher 使用源级预算。
同步阻塞线程、Redis 服务整体变慢或共享连接阻塞仍可能影响多个域。跨域同计划同业务键仍受全源
ordered gate 约束，不能用执行隔离绕过 ACK、重试或 Park 的顺序屏障。

| 配置 | 容量含义 |
| --- | --- |
| `max_inflight_publishes` / `max_inflight_publish_bytes` | 发布序列化及发送票据数与正文预算，登记和预留先于序列化 |
| `max_inflight_records` / `max_inflight_payload_bytes` | 全部读取预留与非终态记录、估算存活正文 |
| `max_active_batches` / `max_read_waiters` | 正在读取或准备的批次、有效来源的 FIFO 等待项 |
| `max_record_bytes` | 单条原始 Envelope 的线格式上限，发布和读取使用同一合同 |
| `max_inflight_tasks` / `max_continuations` | 业务任务及每条记录完成后的责任交接预留 |
| `max_pending_commits` / `max_commit_records` | 提交票据与提交记录预留，逐 ID 对账 |
| `max_async_delete_records` | 已进入异步 XDEL owner 的总 ID 数，等待交接的 ID 仍占 Commit |
| `max_retry_tickets` / `max_unroutable_records` | 精确重试与无法确定路由的记录责任 |
| `max_ordered_keys` / `max_blocked_keys_per_source` | 全局 gate 上限与每来源最坏阻断键预留 |
| `max_deferred_records_per_key` / `max_deferred_records` | 单 key 与全局后缀预留 |

各数量上限必须非零。有效记录上限和 `max_blocked_keys_per_source` 至少容纳一批记录；
`max_active_batches` 与 `max_async_delete_records` 至少覆盖执行域数，分别以批次和 ID 计数，
不要求每域删除额度容纳整批。`batch_size × max_record_bytes` 必须可计算并满足各域正文最低需求；
发布 byte 预算须容纳一条最大正文，发布票据数不要求达到消费 batch 大小。
`max_read_waiters` 至少覆盖全部物理来源数。`stream.inflight_max` 是组内同时读取批次数上限，
还受共享预算和域份额约束。

napart 的规范化本地槽位数乘以计划数再乘以两个 task type，不能超过 `executor.max_type_states`。
例如四个 Runner 各配置 `partitions: 3`，每个实际使用四槽，总槽数按 16 校验。
本地 Runner 不复用 Application 的 `partition.runners`；这些设置不改变 Redis 的 wire 或物理 key
布局，也不修改生产者的既有路由算法。

读取容量采用跨组 FIFO，每个来源最多一个有效等待项。组、来源或执行域份额已满时先撤销其等待项，避免
占住全局队首；失权来源同样退出队列。少于 COUNT 的响应归还余量；超过 COUNT 的响应登记全部
坐标和 record debt，永久关闭本实例的新读取，需停止并重建运行时。

原始 Envelope 与解码对象估算之和计入共享 `max_inflight_payload_bytes`；解码权重不改变单条
线格式是否超限的判断。派发和精确重投均在同一账本临界区尝试扩大预留，成功后才发布 Task；
预算不足时释放解码对象，保留坐标与 ordered gate，退避后精确重读，不增加业务失败次数。
一个对象的估算或一次兼容批量调用所需总权重若始终超过所属域的正文份额，来源保持有界等待，需要调整
预算或数据后重建运行时；这类容量等待不计为 oversized，也不会自动触发 poison Drop。

超大正文不进入 handler，该来源保持阻断；临时正文释放后其它来源仍可使用 byte 预算。任意业务
`DeserializeOwned` 都可能分配额外内存，因此 payload bytes 是背压权重，不是 allocator 或 RSS
硬上限。Redis 客户端接收完整响应的瞬时占用也需单独观测。

## 恢复与异常

新持锁期先完成 XAUTOCLAIM 接管，再无 minIdle 过滤扫描当前 consumer PEL。读取或接管回包丢失时，
先排干旧本地责任，再以当前 consumer 的明确 XPENDING 空页及 holder 复验解除屏障；空响应、正文
缺失和协议解析失败不会被混为一谈。已有记录由 exact `(stream,id)` 重试，不通过普通 poll 间接找回。
retry-op 的同轮网络重放保持 operation identity，本地票据在 CAS 前冻结目标 delivery count。
响应丢失或远端 marker 到期都不会重新递增，也不会将同一次重投误判为超限失败。

真实 tombstone 只处理 PEL 清理；存在 entry 但缺少 `data` 或正文为空属于确定性解码失败。未知路由、
key/weight 回调 panic 与内部合同异常冻结来源，不自动 ACK。确定性数据解码失败按 `max_redeliver`
与 `Drop`、`Park`、`Dlq` 处理；整批路由无法证明时保留整批坐标，完成异常记录处置并排干旧后缀后才
开放该来源新读取。未知路由需要部署正确计划并重建运行时；现有 Park 管理 API 不提供任意未知 ID
的裸 ACK 通道。

Park 保留原记录及其 ordered gate，不计为已确认；其它物理来源中同计划、同 key 的后继继续等待，
不同 key 可继续处理。管理处置完成后按精确 PEL 状态收口原记录，或重新读取正文。
`parked_sources`、`parked_records` 分别给出受影响来源与本地保留记录数；存在 Park 来源时 readiness 为 false。

接管与 `parked_in` 使用同槽 Lua 原子读取 disposition marker 及 parked index。并发管理转换时，
查询可观察到转换前或转换后的完整状态；非终态 marker 始终冻结来源，缺失或陈旧的 index 不覆盖
marker 权威。只有 index、非终态缺少 park_id、未知状态或读取错误时拒绝接管，查询如实返回错误。

`RustV2` 的提交使用 holder 与 fencing Lua 原子校验；`LegacyV1` 在业务开始和 ACK 前复验 holder，
不能消除最后一次校验与 XACK 之间的竞争窗口。两种 profile 不能共享 namespace。

异步 XDEL 不改变 ACK 事实。删除 owner 退出时关闭交接并归还组和共享待删容量，未证明删除的数量
累加到 `delete_retained_records`，正文可能仍留在 Stream。停机期间末次删除失败也采用这一终态，使
等待删除交接的 Commit 能收口；普通运行期间的短暂失败仍按周期重试，不重新调用 handler。

发布在有界序列化前登记票据；调用方取消等待后，监督任务仍持有 XADD。发送后断线返回
`PublishOutcomeUnknown`，不会自动重发，也不能解释为消息未发布。需要可重放发布时，应另行使用
业务事件 ID 与持久去重协议。

## 启停与观测

`prepare` 可幂等创建远端 Stream/group。`start` 冻结全部计划、拓扑和域份额，登记清理责任后启动
全部专属 Runner，并登记等待激活的组监督者；复验全部远端合同后才一次性开放消费。启动失败
按同一资源集合回滚；未取得退出证明时返回 `StartRollbackNotConverged`，包含原始原因和剩余责任，
清理操作继续持有资源。

```rust
let report = running.shutdown_until(
    std::time::Instant::now() + std::time::Duration::from_secs(10),
).await;
if !report.converged {
    let report = running.shutdown_until(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    ).await;
    let _remaining = report.remaining;
}
```

`shutdown()` 使用 `partition.drain_timeout_ms` 作为本次等待期限。所有正常停机调用复用一个持续
操作：关闭发布和读取根准入，取消尚未开始的任务，允许当前业务结束并交接成功前缀，交回未执行
责任，等待来源 I/O 和任务真正退出，再释放锁并停止全部 Runner。任一域未退出都不能报告收敛。
超时只返回 `converged=false`，不会自动强停或提前释放仍有责任的锁。

`force_shutdown_until` 是显式有损升级：撤销来源权限，中止受监督 Future 并等待 join；未取得证明
时停止续租，依靠租约/fencing 接管，报告仍未收敛。发送中的发布可能结果未知。直接 Drop 只能关闭
准入并触发尽力清理，不能代替显式停机报告。

`snapshot()` 给出 readiness、Runner 降级、record/payload/batch/waiter/task、commit UNKNOWN、retry、
gate、deferred、unroutable、来源阻断、超大正文、协议异常、债务与锁及读取 join 状态。
`executor_scope` 和 `execution_domains` 给出冻结域索引、逻辑组/物理编号、记录/正文/批次/删除份额、
当前占用、任务数以及 Runner 降级和退出状态。Runner 降级关闭本域新业务并使整体 readiness 失败，
健康域仍可继续消费；共享账本监督异常则关闭全源准入。聚合责任必须与全部域共同收口。
`publisher_snapshot()` 给出发布容量、成功、发送前失败、结果未知和未 join 数；
`async_delete_pending()` 给出全部组待删 ID 数。停机报告同时包含这些未完成责任。指标不携带
业务 key、entry id 或租约 token。持续 UNKNOWN、来源阻断、超大消息、债务、Runner 降级和待删积压
应接入告警；只有容量下降本身可以自动解除普通背压。

快照中的 `group` 在 source 模式为 `None`，其它模式使用逻辑组 ID，默认组为 `Some("")`；
`partition` 只在 stream 模式给出物理编号。record、payload、batch、task 投影在账本锁内读取；
异步删除和 Runner 状态来自独立原子状态，不能把不同调用时刻的 gauge 拼成事务证明。
`snapshot().ready` 是消费器的本地状态，不会自动成为 Application 的 readiness 门禁，业务需显式
接入健康策略。`PreparedPartition::start` 也不等待 Application Ready；需要统一接流时由业务协调
启动时点，并在共享 Redis 客户端关闭前完成消费器停机。
