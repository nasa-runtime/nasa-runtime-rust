# napart

`napart` 是面向异步服务、提供严格 FIFO 保序任务窃取的命名分区执行器。每个 `PartitionRunner` 独立拥有 generation、
分区 slot、主 MPSC 队列、类型路由、盗洞、延迟索引、容量、指标、健康与停机权威；一个
Runner 的满载、迁移、失败、停止或重启不会改变其它 Runner 的状态。

它同时提供三项核心能力：

- 稳定 key 分区与 `TaskType` 严格 FIFO；严格顺序边界是
  `(Runner, 原始分区, TaskType)`。
- 热点任务通过盗洞搬到空闲 slot 消费；严格任务跨 stock、incremental、staging 与源主队列
  仍保持同一受理序号，不会因迁移或归还倒挂。
- 每类型排队容量、Runner 全局在飞预算、类型基数、入站盗洞与失败证据均有界；取消、拒绝、
  失败和停机损耗都有稳定终态。

Runner 共享调用方提供的 Tokio runtime，因此隔离的是调度状态、容量和生命周期，不是 CPU 核、
进程内存或 runtime 调度器。需要硬隔离时应使用独立进程。

## 接入方式

- **直接动态模式**：业务持有一个进程级 `PartitionRunnerRegistry`，可以在 Tokio runtime 运行期间按
  稳定业务域调用 `get_or_create`，再显式 `start`。同一注册表负责保持名称身份，调用方负责健康策略以及
  `stop`、`force_stop` 或 `stop_all`；不能为每次请求新建注册表，否则相同名称会成为彼此无关的执行域。
- **Application 受管模式**：启用 `napp` 或 `nasa` 的 `partition` 组件，由 YAML 或 Service UserHook
  提交启动计划。全部名称在 UserHook 结束时冻结，Prepare 统一启动、发布 readiness 并接管停机；业务只
  取得不含启停权的提交句柄。该模式不在 Running 阶段追加 Runner，运行期才确定成员关系的业务应使用
  直接动态模式并显式管理注册表。

两种方式使用同一 napart 内核，但生命周期所有者不同。一个业务执行域只能由其中一方持有，不能同时
创建直接 Runner 和同名受管 Runner 后假定二者共享队列、容量或顺序状态。

## 快速开始

```toml
[dependencies]
napart = "1.1.2"
```

```rust
use std::time::{Duration, Instant};

use napart::{
    PartitionRunnerRegistry, RunnerConfig, TaskSpec, TaskStatus, TaskType,
};

const SETTLEMENT: TaskType = TaskType(1);
const NOTIFICATION: TaskType = TaskType(2);

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let registry = PartitionRunnerRegistry::builder()
        .max_runners(8)
        .build()?;

    let settlement = registry.get_or_create(
        "settlement",
        RunnerConfig {
            partitions: 16,
            queue_capacity_per_type: 1_024,
            global_inflight: 16_384,
            max_type_states: 4_096,
            ..RunnerConfig::default()
        },
    )?;
    settlement.start().await?;

    let submission = settlement.submit_typed(
        "order:1001",
        TaskSpec::strict(SETTLEMENT),
        || async {
            // 同 Runner、原始分区和 TaskType 按受理顺序执行。
        },
    )?;

    settlement.exec_typed(
        "order:1001",
        TaskSpec::relaxed(NOTIFICATION),
        || async {
            // 非严格类型可以通过多个租约盗洞并发推进。
        },
    )?;

    assert_eq!(submission.await_outcome().await, TaskStatus::Completed);
    settlement
        .stop(Instant::now() + Duration::from_secs(5))
        .await?;
    Ok(())
}
```

同一注册表内，同名、同配置查询返回同一控制对象；同名不同配置返回 `ConfigConflict`。名称必须是
有界常量或启动配置，不能使用租户号、订单号、请求 ID 等无界业务值。注册表不自动驱逐名称，完整
停止后的同一对象可以启动全新 generation。注册表不是库级全局单例；需要随处取得同一 Runner 的业务
应共享同一个注册表，而不是在调用点重复构造注册表。

## 运行架构

```text
PartitionRunnerRegistry
  ├─ settlement ─> PartitionRunner
  │                  ├─ generation + supervisor
  │                  ├─ global/type admission budgets
  │                  ├─ timer + delayed index
  │                  ├─ observer + moving audit
  │                  └─ PartitionSlot[0..N)
  │                       ├─ one worker
  │                       ├─ main SequencedMpsc<TaskEntry>
  │                       ├─ control SequencedMpsc<StealRequest>
  │                       ├─ TypeState map
  │                       └─ inbound tunnels
  └─ notification ─> 独立 PartitionRunner
```

每个 slot 只有一个主队列 consumer worker。`TaskType` 对应稳定 `TypeState`，不再拥有独立物理主
队列。集中 observer 只选择空闲目标并向繁忙源提交 `StealRequest`；只有源 worker 能选择候选类型、
安装盗洞和移动主队列存量，目标 worker 不扫描或改写其它 slot 的类型表。

任务的状态、逻辑 owner、物理 retention、移动描述符、容量许可和业务 Future 集中在内部
`TaskEntry`。跨容器移动先登记接收方逻辑计数，再切换 owner 并发布目标容器，最后结算来源计数。
任何无法证明唯一 owner、唯一物理位置或当前 generation 的状态都会关闭最小故障域并留下有界证据。

## 严格与非严格盗洞

非严格类型可以同时安装多个目标盗洞。新 producer 在活动快照上轮转直投，源 worker 也会把主队列
存量搬入盗洞，同时保留本地执行份额。每条盗洞有独立 FIFO、producer 边界与租约；空闲过期后先关
producer，再排空合法迟到前缀，最后撤销两侧引用。单条盗洞失效不会冻结同类型其它方向。

严格类型最多有一个盗洞，路由按以下状态推进：

```text
Local -> Migrating -> Stolen
                       |
                       v
               ReturnPrepare
                 |           |
                 v           v
               StolenCatchup   Returning -> LocalCatchup -> Local
```

严格迁移的调度单元是单个 `(Runner, 原始分区, TaskType)`。只有一个严格 `TaskType`
的单键热点也能在相邻业务任务的边界迁移，不要为了获得盗洞而把同一顺序域拆成多个
`TaskType`。严格任务正在执行时，源 worker 保留一笔已去重的窃取请求；任务释放执行门禁后，
该请求先于下一笔本地任务恢复，因而可在逻辑积压仍超过阈值时安装迁移声明。

- `Migrating` 关闭旧本地执行位置。迁移前可能进入源主队列的任务按队列边界进入 stock；看到新路由的
  producer 进入 incremental。目标完整跨过 stock 前缀后才执行 incremental。
- `ReturnPrepare` 把新 producer 改投无 consumer 的 staging，并等待旧 direct producer 离场。准备条件
  消失且尚未回写时可经 `StolenCatchup` 恢复目标执行。
- `Returning` 开始后不能回退。incremental 回写源主队列，随后 `LocalCatchup` 收口 staging 与源主队列
  后继。每笔严格任务保留首次受理序号，类型级连续前缀和独占执行门禁共同保证物理队列切换不改变
  FIFO。

迁移、归还和业务执行只在严格任务边界切换位置。超时、owner 冲突、队列失权或代次不一致会冻结该
严格类型；其它类型、slot 和 Runner 在自身顺序证明完整时继续服务。

## 提交、背压与取消

`RunnerConfig` 的主要资源边界如下：

| 字段 | 含义 |
| --- | --- |
| `partitions` | 规范化为 2 的幂的 slot/worker 数 |
| `queue_capacity_per_type` | 每个 `(home, TaskType)` 尚未运行任务上限 |
| `global_inflight` | 延迟、排队、移动和执行中任务总上限 |
| `max_type_states` | 当前 Runner 全部类型状态和指标基数上限 |
| `frozen_evidence_capacity` | 最近失败证据环容量 |
| `max_inbound_tunnels` | 单个目标 slot 的入站盗洞总上限 |
| `idle_task_threshold` | 热点迁移和空闲判断阈值 |
| `strict_opportunity_attempts` | 每轮保留给严格候选的独立机会数 |
| `return_observations` | 发起严格归还所需连续观察数 |
| `tunnel_lease` | 非严格盗洞没有真实进展时的租约上界 |
| `load_observer_interval` | 全 slot 负载观察间隔 |
| `control_tick` | 活动迁移、归还和审计推进间隔 |
| `transition_timeout` | 单次物理移动允许保持 Moving 的上界 |
| `shutdown_timeout` | standalone 兼容入口的默认收口预算 |
| `drain_batch` | worker 单轮处理各物理方向的批量上限 |

非阻塞类型化提交先取得类型排队许可，再取得 Runner 全局许可，分别以 `QueueFull` 和 `Overloaded`
拒绝。`submit_async` 使用相同固定顺序等待；等待 Future 被取消时 RAII 会归还已经取得的许可，不留下
空条目或保留槽。

同一 Runner 的业务任务内不得 await 任何可能等待全局预算的提交，不论 key、任务类型或原始分区；
外层任务已经占用全局许可时，跨分区再入同样可能形成容量环。此类流程应使用非阻塞提交并处理
`QueueFull` / `Overloaded`，或把再入工作交给不占当前 Runner 许可的受监督调度边界。

`SubmitRejection` 有七类拒绝：`ShuttingDown`、`QueueFull`、`Overloaded`、`OrderingConflict`、
`LaneFailed`、`LaneLimitExceeded` 和 `ReservedTaskType`。低基数指标逐类累计，其中保留类型入口使用
`rejected_reserved_type`；拒绝原因不从字符串日志反推。

`Submission::cancel()` 只在业务执行开始前成功。任务位于 Moving 时只登记协作取消，由唯一移动发布者
或目标 consumer 收口；尚未到期且没有物理 consumer 的任务由受监督阻塞清理任务释放载荷，公开调用线程
不运行用户 Future 的析构逻辑。返回 true 表示取消意图已被权威接纳，不代表稳定终态已经发布；需要完成
证明时使用 `await_outcome`。Running 后普通取消不改变任务状态。所有稳定终态都在业务载荷、逻辑计数、
类型许可和全局许可恰好结算一次后发布。

## 延迟任务

`submit_after` 登记时只占 Runner 全局许可，不创建 `TypeState`，也不占类型排队容量。每个 Runner 只有
一个受监督 timer 驱动和可索引延迟表；到期时再按当前路由非阻塞尝试类型许可。类型满载、Runner 已
停机、类型失败或 generation 不一致都会发布明确拒绝终态。由于 `submit_after` 已经完成登记，这类结果
通过 `Submission::await_outcome()` 返回 `TaskStatus::Rejected`，稳定 reason 可用于区分到期容量、发布或
路由门禁；调用方不能把登记成功等同于未来一定执行。

取消会立即从逻辑索引幂等摘除；定时堆中的失效槽按几何阈值压缩，最后一项离场时清空，因此突发取消
保持摊销线性且物理存量有界。超长 delay 从同步登记时刻按绝对边界分段等待，runtime 时钟跨过多段会
一次消费全部已流逝段，不会重新起算或提前到期；`Duration::MAX` 可用于“只等待取消或停机”的任务。
取消与到期重新准入竞争时只允许一个稳定终态；只有实际取得 `Rejected` 权威的路径才增加拒绝分类，
取消获胜不会同时被记为拒绝。无论哪一方获胜，全局许可、可选类型许可和排队投影都只结算一次。
指标同时报告逻辑 delayed 数、物理 timer 槽位、取消移除和堆压缩次数。

## 生命周期与停机

`start`、`stop`、`force_stop` 和注册表 `stop_all` 的状态迁移由 generation 唯一 supervisor 推进。公开
Future 被 `select!`、timeout 或 abort 取消只会停止本次等待，已经发布的 operation 会继续运行；后续调用
复用同一动作和句柄注册表。

- `stop(deadline)` 关闭入口并无损排空。没有在期限内取得 producer、worker、业务任务、载荷清理任务、
  observer、timer 和 supervisor 的真实 join 证明时返回 `NotConverged`，Runner 保持 Stopping。
- `force_stop(deadline)` 是显式有损入口，中止协作让出的业务 Future 并冻结未执行任务。deadline 不能
  替代退出证明；尚有未 join 任务时仍返回 `NotConverged`。
- 完整停止后再次 `start` 创建全新 generation、slot、队列、盗洞和 timer。旧 `Submission` 仍可读取旧
  终态，但旧代回调不能进入新代。

直接 `Drop` 最后一个 Runner 控制句柄或 standalone 包装时只发布无等待的有损兜底；对业务 Future 不让出
执行权的非协作任务不承诺自行收口。该路径不等待 join，也不提供退出证明；runtime 已停止时同样不能
替代显式停止。需要确定结果的调用方必须调用 `stop`、`force_stop`、`stop_all` 或兼容包装的 shutdown 方法。

## 健康与观测

`health()` 区分 `Healthy`、`Degraded`、`Failed`、`Starting`、`Stopping` 和 `Stopped`。单个类型或
slot 被隔离且仍有可接纳 slot 时为 `Degraded`；控制面失权、timer/observer 退出或全部 slot 失败会先
关闭入口再报告 `Failed`。同一 generation 不原地恢复失败类型或 slot，恢复必须完整停止并启动新代。

`metrics_snapshot()` 提供 phase、health、epoch、接纳与失败 slot、提交终态与拒绝分类、全局许可、
已完整受理的逻辑排队量、主/控制队列深度、reserved head、producer inflight、timer、moving、各类盗洞
深度、严格路由状态、受监督任务和停止损耗；仍在等待另一层容量的调用不计入逻辑排队量。
`rejected_*` 同时覆盖公开提交入口的立即拒绝，以及已经登记的 delayed 任务到期后实际形成的拒绝终态；
`submitted` 只统计已经受理的任务，因此不能把全部拒绝累计量直接从 `submitted` 中相减。需要逐任务证明时
应读取 `Submission` 的稳定状态与 reason，取消竞争不会重复增加拒绝累计量。
`frozen_evidence()` 只返回不持有业务载荷的有界诊断快照；逐任务权威结果始终由 `Submission` 保存。

## Application 受管模式

启用 `napp` 或 `nasa` 的 `partition` feature 后，Application 可以在 YAML 的
`partition.runners.<name>` 下声明多个执行域。Prepare 批量启动并原子发布，`app.partition()` 返回配置的
default Runner，`app.partition_runner(name)` 返回命名业务投影；投影不含启停权。每个 Runner 有独立
readiness，关键性和无损超时后是否允许有损收口由各自计划明确配置。Service UserHook 也可以用
`configure_partition` 与 `configure_partition_runner` 提交 YAML 未占用名称的计划，但这些调用只登记
参数；Runner 在 UserHook 返回后的 Prepare 才启动，因此同一 Hook 不能立即取得业务句柄。UserHook
结束后计划入口关闭，Running 阶段不能再向受管集合追加名称。

Application 的一次性业务停机任务不持有受管 Runner 的启停权。`register_graceful_shutdown` 只安排
业务收尾，不能再次调用受管 Runner 的 stop 或 force_stop，也不能把 future 已释放当作该 Runner
全部任务已 join 的证明；退出证明仍由 partition 组件依据上述状态机取得。

## standalone 兼容入口

`PartitionExecutor::new`、`with_partitions`、`with_limits` 和既有 `submit*` 入口仍可用。包装内部创建一个
不进入共享注册表、构造时立即启动的 standalone Runner；它不提供按名称幂等取得能力。需要多个隔离
执行域的新代码应直接使用 `PartitionRunnerRegistry`。

## 能力边界

`napart` 不提供跨进程任务接管、持久化队列、至少一次投递或进程崩溃恢复；key 到分区的哈希也不是跨
版本持久化分片协议。单条严格类型不会因盗洞而并行执行，长任务仍会阻塞该严格顺序方向。业务 Future
的 `Drop` 必须不 panic；框架能隔离业务 Future 的 poll panic 和单次析构展开，但不承诺屏蔽展开期间
再次 panic 导致的进程终止。
