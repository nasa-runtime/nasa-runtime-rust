# napart

`napart` 是分 lane 的串行/并行执行器。顺序边界是「原始分区 + 任务类型」：同一 key
的严格类型任务按提交顺序串行落地；不同 key 会散列到原始分区，落入同一分区且类型相同的
严格任务也会串行。不同类型不共享严格顺序门禁，非严格 lane 允许多个空闲 worker 并发消费。
空闲分区还可通过一次执行权转移接管其它分区的严格 lane，不移动队列数据，FIFO 由 lane
队列保证。该模型适合订单、账户、用户维度的异步事件处理。

直接依赖：

```toml
[dependencies]
napart = "1.1"
```

必须在 Tokio 运行时内构造（内部启动常驻 worker）；`shutdown` 为 async。

## 基本使用（同 key 串行）

```rust
use napart::{PartitionExecutor, SubmitError};

async fn run() -> Result<(), SubmitError> {
    let executor = PartitionExecutor::with_partitions(16);

    executor.submit("user:1001", || async {
        // 同一个 user key 下的任务会串行。
    })?;

    executor.submit("user:1002", || async {
        // 不同 key 可能并发。
    })?;

    executor.shutdown().await;
    Ok(())
}
```

`submit` / `submit_sync` / `submit_async` 保持同 key 严格 FIFO、有界背压与被拒可见。
不同 key 只有在落入不同分区且有可用 worker 时才会并发；哈希碰撞只会降低并发度，不会削弱
同 key 顺序。

## 类型化提交（跨类型并行 + 任务句柄）

同一 key 的不同任务类型不共享严格顺序门禁；严格类型内部仍按提交顺序串行。实际并发度仍受
worker 数和全局预算约束。句柄支持状态查询、取消与等待终态。

```rust
use napart::{PartitionExecutor, TaskSpec, TaskStatus, TaskType};

const SETTLE: TaskType = TaskType(1);   // 结算:必须按序
const NOTIFY: TaskType = TaskType(2);   // 通知:允许并发

async fn run(executor: &PartitionExecutor) {
    let handle = executor
        .submit_typed("order:1", TaskSpec::strict(SETTLE), || async {
            // 同 key + 同类型 严格按提交顺序执行。
        })
        .expect("受理失败原因见 SubmitRejection");

    executor
        .exec_typed("order:1", TaskSpec::relaxed(NOTIFY), || async {
            // 非严格类型:任意空闲分区并发执行,每个任务仍至多执行一次。
        })
        .ok();

    match handle.await_outcome().await {
        TaskStatus::Completed => {}
        other => eprintln!("任务终态: {other:?}, 原因: {:?}", handle.reason()),
    }
}
```

- 同一「分区 + 类型」的顺序要求（strict/relaxed）不得混用，冲突提交返回 `OrderingConflict`。
- `TaskType(u32::MAX)` 是未分型兼容入口的保留值：类型化入口对它 fail-fast 返回
  `ReservedTaskType`，公开输入不能改写兼容保留 lane 的顺序要求或借用其上限豁免。
- `cancel()` 只在任务开始执行前生效；执行权与取消权竞争同一原子位置，至多一方成功，不存在"取消成功但仍被执行"。
- 终态与原因码同一原子字发布：观察到 `Rejected`/`Failed`/`Cancelled` 时 `reason()` 必然可读，原因码取自封闭集合。
- 取消延迟任务会立即摘除其登记；`delayed_pending` 计量在长驻进程中应随负载波动而非单调增长。

## 延迟提交

到期前只占一个全局名额、不占任何 lane 深度；到期时按当时路由入队。

```rust
let handle = executor.submit_after(
    "order:1",
    std::time::Duration::from_secs(30),
    napart::TaskSpec::strict(SETTLE),
    || async { /* 到期执行 */ },
)?;
// 到期前可 cancel();停机会把未到期任务置为 Rejected(shutdown_before_expiry)。
// 到期时 lane 满载则置为 Rejected(queue_full_at_expiry),不等待、不静默消失。
```

零延迟不走定时器，与 `submit_typed` 完全等价：同一容量状态下受理/拒绝结论一致，满载同步返回
`QueueFull`，不会先受理再异步 `Rejected`。

`delay` 无上限：超出底层定时器可表示范围的时长按有界分段消耗，任务绝不早于
`登记时刻 + delay` 到期。`Duration::MAX` 语义上即"永不到期"——仍可取消，停机时照常置为
`Rejected(shutdown_before_expiry)`，不会被静默截短为更早的时刻。

## 双层准入与背压

- **每 lane 深度上限**约束排队量：worker 取走任务即腾出；满时 `submit` 返回 `QueueFull`。
- **全局在飞预算**约束「排队 + 执行中」总量：任务到达终态即归还（含取消——被取消任务即使
  载荷仍滞留队列，名额也立即腾出）；耗尽时类型化入口返回 `Overloaded`（兼容入口并入
  `QueueFull`）。
- **类型化 lane 总数上限**（`with_max_lanes`，默认 `max(4096, partitions)`）约束类型基数：`TaskType` 是开放的
  u32，注册表与按类型导出的指标序列只增不减，超限提交返回 `LaneLimitExceeded`，不静默扩张
  内存。兼容入口的保留 lane（每分区至多一条）豁免本上限——typed 类型先到先得占满额度不会让
  兼容 `submit` 失去容量，进程内 lane 总数上界为 `上限 + 分区数`。

```rust
// 分区数、每 lane 深度、全局在飞预算全显式:
let executor = napart::PartitionExecutor::with_limits(32, 1024, 65_536);

match executor.submit("order:1", || async {}) {
    Ok(()) => {}
    Err(napart::SubmitError::QueueFull) => { /* 重试、降级或返回 429 */ }
    Err(e) => return Err(e),
}
```

`submit_async` 会等待容量（真背压），**永不**返回 `QueueFull`：

```rust
executor.submit_async("order:1", || async {}).await?;
```

取消不立即腾出 lane 深度——载荷要等消费者取到后物理丢弃才归还，因此"取消一批后立即重提"仍可能
`QueueFull`；但取消腾出的全局名额立即可用于其它 lane。

## panic 隔离、冻结与停机

- 单任务 panic 只终结该任务（终态 `Failed`，原因 `task_panicked`），不杀 worker、不冻结 lane、不影响后续任务。
- 结构性异常（worker 异常死亡等）会冻结相关 lane：后续提交明确拒绝（`PartitionDead` / `LaneFailed`），队列中未执行任务转为带原因的失败证据，可经 `frozen_evidence()` 审计，不静默丢弃。
- `shutdown().await`：停止接收新任务并排空已受理任务；`stop_timeout` 超时后强制中止在途任务
  （终态 `Failed(aborted_during_shutdown)`）并冻结未排空任务。返回前会等到 worker、被中止的
  业务任务与全部延迟定时器（含到期前已取消的）按 join 结果确认退出——返回即证明不再有本
  执行器任务在后台运行，而不是仅发出了取消请求。正在提交临界区内的 producer 也会被等到离场
  （无超时、周期性告警）：在 producer 可能完成入队时发布停止，会让终局报告被迟到提交改写。
  停机由独立受监督任务驱动，调用方 future 被 timeout/select 取消不会把生命周期卡在停机中，
  后续调用取回同一份终局报告。
- 被中止的任务在其下一个让出点结束；从不让出的业务 future 无法被任何方式取消，会推迟停机完成
  （协作式调度的固有边界）。
- 需要审计停机损耗时使用 `shutdown_with_report().await`，完全优雅的判定是
  `frozen == 0 && aborted == 0`：

```rust
let report = executor.shutdown_with_report().await;
if report.frozen > 0 || report.aborted > 0 {
    // 有损停机:frozen 个任务未执行即冻结(原因 shutdown_frozen),
    // aborted 个任务执行中被强制中止(原因 aborted_during_shutdown,可能已有部分业务效果)。
    for e in executor.frozen_evidence() {
        tracing::error!("停机损耗: type={} partition={} reason={:?}", e.task_type, e.partition, e.reason);
    }
}
```

## 观测

`is_healthy()`（运行中且无 worker 死亡、无 lane 冻结）、`dead_partitions()`、`failed_lanes()`、`lanes()`、`metrics_snapshot()`（受理/完成/取消/panic/停机中止、七类拒绝、窃取与归还、冻结、排队深度、预算余量、延迟登记量等低基数计数）。计数口径：`submitted` 按受理时点计（延迟提交在登记返回句柄时即计入）；`cancelled` 计取消成功的真实次数（取消发生即计数，含到期前取消的延迟任务），不是载荷被物理丢弃的时点；七类拒绝计数为 `rejected_shutting_down` / `rejected_queue_full` / `rejected_overloaded` / `rejected_ordering_conflict` / `rejected_lane_failed` / `rejected_lane_limit` / `rejected_reserved_type`，其中前六类含延迟到期变体（如 `queue_full_at_expiry` 计入 `rejected_queue_full`）。排空后公开累计量守恒：`submitted == completed + cancelled + task_panics + 已受理延迟任务的到期或停机拒绝 + frozen + aborted`；同步拒绝（含 `rejected_reserved_type`）发生在受理之前，不计入 `submitted`，也不属于该守恒式的分解项。`shutdown` 之后 `is_healthy()` 恒为 false（设计如此，监控口径注意）。

## 选型建议

- 需要同 key 顺序一致时，用 `napart`；同 key 下还有可并行的旁路工作时，用类型化提交拆 lane。
- 只需要普通并发、不关心顺序时，直接用 `tokio::spawn` 或普通队列。
- 提交入口要处理 `SubmitError` / `SubmitRejection`，不要把满队列当成成功。

## 行为边界

- 严格 lane 顺序保证：pop 与执行整体互斥，执行权转移不搬数据，因此转移前后顺序不变；一个正在执行的长任务会推迟该 lane 被接管的时点。
- `submit`/`submit_sync` 非阻塞，队满返回 `QueueFull`；`submit_async` 等待容量，**永不** `QueueFull`。
- `submit_sync` 的闭包直接占用异步 worker，必须保持短小且不得执行阻塞 I/O；长耗时或阻塞工作
  应先移出执行器，或在业务侧使用受控的 blocking 线程池并等待其结果。
- **再入死锁警告**：业务任务内不得 `await` 任何可能等待全局预算的**同执行器**提交（即任务内不要调用 `submit_async(...).await`），**不论 key、任务类型或原始分区**——运行中的任务在终态前始终持有一个全局许可，其内部等待型提交又要再取一份许可，预算贴满时即使目标是完全不同分区的空闲 lane 也会互等（全局许可不分 lane，"换个 key"消除不了互等条件）。任务内派生工作只可使用非阻塞 `submit` / `submit_typed`（满载得到明确错误，自行退避或降级），或提交到**独立执行器**。在任务内 `await` 自己刚提交到同一严格 lane 的任务同样会死锁。
- 每个已受理任务恰好四种走向之一：执行一次（`Completed`/`Failed(task_panicked)`）、零次取消（`Cancelled`）、零次冻结留证（`Failed` 带原因）、执行中被停机强制中止（`Failed(aborted_during_shutdown)`，计入报告与证据）。不存在双执行与静默消失。
- **业务 future 析构边界**：未执行任务（取消/冻结/到期拒绝）的业务 future 由框架在隔离边界内析构，单次析构 panic 被捕获记录，不影响 worker、停机驱动与损耗归因；析构清理期再次 panic（同一 future 内多个字段的析构接连展开）会被 Rust 运行时以进程中止处理，任何库都无法在进程内屏蔽——**业务 future 的析构不得 panic** 是公开前置条件。
- 直接 `Drop` `PartitionExecutor` 是非阻塞尽力收口：拒收新任务、把全部排队任务终态化为
  `Failed(shutdown_frozen)`、拒绝延迟登记，并向常驻 worker 与定时任务发出中止与唤醒请求。
  协作式任务在下一个让出点退出；**非协作**（poll 不让出）的业务 future 无法被抢占，会继续
  存活到它下一个让出点，Drop 也不提供 join 退出证明与损耗报告。需要终局报告与退出证明必须
  显式 `shutdown().await`。

## 应用配置映射

`napart` 不读取 YML，也不固定配置根节点。下面的 `partition_executor` 只是应用自有配置示例；
应用应完成解析与校验，再在 Tokio 运行时内构造 `PartitionExecutor`。

```yaml
partition_executor:
  partitions: 64
  queue_capacity: 1024
  global_inflight: 131072
  max_lanes: 4096
  shutdown_timeout_ms: 5000
  full_policy: reject
```

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `partitions` | `2 × CPU` 后向上取 2 的幂 | 显式值也会至少取 1 并向上取 2 的幂。 |
| `queue_capacity` | `65536` | 每 lane 深度上限；最小按 1、最大按 `u32::MAX` 处理，满时非阻塞提交返回错误。 |
| `global_inflight` | `partitions × (queue_capacity + 1)` | `with_partitions*` 构造器采用的排队 + 执行中总量上限；`with_limits` 由应用显式提供。 |
| `max_lanes` | `max(4096, partitions)` | 类型化 lane 总数上限；兼容入口保留 lane 豁免不计入，显式值低于分区数时按分区数生效。 |
| `shutdown_timeout_ms` | `2000` | 排空阶段等待上限；最大按 365 天处理，超时后中止在途任务、冻结未排空任务并等待退出证明。 |
| `full_policy` | `reject` | 应用侧策略示例；组件只返回明确错误，不读取此键，重试或降级由业务决定。 |

启动代码：

```rust
use std::time::Duration;

let executor = napart::PartitionExecutor::with_limits(
    cfg.partition_executor.partitions,
    cfg.partition_executor.queue_capacity,
    cfg.partition_executor.global_inflight,
)
.with_max_lanes(cfg.partition_executor.max_lanes)
.with_stop_timeout(Duration::from_millis(
    cfg.partition_executor.shutdown_timeout_ms,
));
```

同 key 的任务必须使用稳定 key，例如 `order:{id}`、`account:{id}`。不要把随机值、时间戳或请求 ID 放进 key，否则会破坏串行语义。任务类型同理：`TaskType` 必须是编译期稳定的业务常量，不要动态生成。
