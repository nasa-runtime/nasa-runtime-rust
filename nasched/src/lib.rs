//! 异步任务和定时任务运行时。
//!
//! 提供 linkme 收集的定时任务启动、错过触发策略、固定频率/固定延迟任务、
//! 分布式 FireLog 去重和主节点门禁。
// ============================================================================
// scheduling 运行端 —— #[scheduled] 定时任务的【收集 + 启动 + 停机】
//
// 设计取舍(给后续维护者,免翻外部源码即可理解):
//   1) 任务收集靠 linkme 的 distributed_slice(链接器在编译期把各处 #[scheduled] 生成的注册项汇总进
//      SCHEDULED_TASKS),**不做运行期反射/扫描**——Rust 没有容器,也不需要。
//   2) cron 任务分两路：misfire=Skip 走 tokio-cron-scheduler；misfire=FireOnce/ClaimOnly 走
//      【自管 CronPlan driver】(croner 算名义触发时刻),因为前者回调拿不到"本拍名义 scheduled_at",而 FireLog
//      claim 去重要跨节点一致的 scheduled_at(FireOnce 额外挂 misfire 巡检补漏,ClaimOnly 只每拍 claim)。非 cron(固定频率/固定延迟/一次性)**自管 tokio::time**
//      (该库对 non-cron 的 Duration 会按秒截断 + 固定 500ms tick,毫秒级会失真)。
//   3) #[Async] 是【编译期改写"调用即 spawn"】,不经过这里;本文件只管 #[scheduled]。
//
// 刻意不做的能力(它们在原框架里靠 DI/容器/AOP 实现,Rust 宏 + 静态收集模型无法也不应硬凑):
//   配置占位符解析(${...})、按名字选调度器/执行器、注解代理(proxy/mode)、容器式任务清单注册表。
//   需要这些时由业务在调用处显式处理,而非框架隐式注入。
// ============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use std::sync::Arc;

use linkme::distributed_slice;
use tokio::task::JoinHandle;
use tokio_cron_scheduler::{Job, JobScheduler};

// ============================================================================
// 集群门控(cluster gate)—— 让 #[scheduled(cluster="leader")] 只由当前 leader 触发。
// core 不依赖 Redis；连接 nadis::Leader 的 adapter 位于 `cluster` feature 下。
// ============================================================================

/// 任务的集群执行模式(由 `#[scheduled(cluster=...)]` 决定,默认 `Local`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    /// 每个节点各跑一份(默认,行为同无集群)。
    Local,
    /// 仅当前 leader 节点触发(其余节点本拍跳过)。
    ///
    /// ⚠️ **leader-only,不是 exactly-once**:`is_leader()` 是触发瞬间的时间点判定。lease 交接 / 脑裂窗口里
    /// 两个节点可能同时自认 leader,**同一拍可能被双触发**。要「同一 `scheduled_at` 跨节点去重」请用
    /// `misfire="fire_once"`/`"claim_only"`(经 [`FireLog`] 原子 claim 去重),或在业务侧做幂等 / done-marker。
    /// claim 只表示“某个节点负责触发了这一拍”，不等于业务执行成功。
    Leader,
}

/// leader 判定的注入点:调度层只依赖它读 `is_leader()`,不直接持有 `nadis::Leader`——
/// 该边界允许接入不同选主实现，调度层不依赖具体租约客户端。
pub trait LeaderGate: Send + Sync + 'static {
    /// 返回当前节点此刻是否拥有任务触发权。
    fn is_leader(&self) -> bool;
}

/// 集群启动选项:gate + 用于启动指纹的稳定 id。
pub struct ClusterOptions {
    /// 当前进程的 leader 判定实现,由调度器在触发点读取。
    pub gate: Arc<dyn LeaderGate>,
    /// 仅用于同进程启动配置指纹(不参与选主),建议填 leader lock key。
    pub gate_id: String,
}

/// 调度器启动选项。`#[non_exhaustive]` 要求调用方使用构造器与 `with_*` builder，新增字段不会破坏调用方。
#[non_exhaustive]
pub struct SchedulerOptions {
    /// 默认集群执行配置;未设置时所有 `cluster="leader"` 任务都会跳过。
    pub cluster: Option<ClusterOptions>,
    /// 运行记录器；默认 `NoopRecorder`（不记录）。用 `with_recorder` 设置。
    pub recorder: Arc<dyn ExecutionRecorder>,
    /// 记录用的节点标识(写进 `RunEvent.node`);默认空串。用 `with_node_id` 设。
    pub node_id: String,
    /// misfire claim 存储；`misfire=FireOnce`/`ClaimOnly` 任务必需，否则可为 `None`。用 `with_fire_log` 设置。
    pub fire_log: Option<Arc<dyn FireLog>>,
    /// misfire 巡检周期（默认 30s）。仅 `misfire=FireOnce` 任务使用，应按部署环境的时钟偏差调整。
    pub misfire_sweep_interval: Duration,
    /// misfire claim 容差 ms(默认 5000):应覆盖最大节点间时钟偏差 + 一个 sweep 周期。用 `with_misfire_tolerance_ms` 设。
    pub misfire_tolerance_ms: i64,
    /// 分组 leader gate：`#[scheduled(cluster="leader", group="g")]` 任务使用 `group_gates["g"]`，
    /// 未给 group 或该 group 未注册时用 `cluster` 默认 gate。让不同任务族由不同 leader 节点承载(负载分散)。
    /// 用 `with_group_gate` / `with_group_gate_id` 设;空 = 全部用默认 gate(= 现状,向后兼容)。
    pub group_gates: std::collections::HashMap<String, Arc<dyn LeaderGate>>,
    /// 各 group gate 的稳定标识(如该 group 的 leader lock key);仅 `with_group_gate_id` 注册时有。
    /// 纳入启动指纹，使同进程二次启动或同 group 更换底层 gate key 时能够 fail-fast。
    /// 热路径仍只调用 `LeaderGate::is_leader()`;这里的 id 只用于启动期配置漂移检测,不泄露 gate 内部实现。
    pub group_gate_ids: std::collections::HashMap<String, String>,
}

impl Default for SchedulerOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            cluster: None,
            recorder: Arc::new(NoopRecorder),
            node_id: String::new(),
            fire_log: None,
            misfire_sweep_interval: MISFIRE_SWEEP_INTERVAL,
            misfire_tolerance_ms: MISFIRE_TOLERANCE_MS,
            group_gates: std::collections::HashMap::new(),
            group_gate_ids: std::collections::HashMap::new(),
        }
    }
}

impl SchedulerOptions {
    /// 本地模式(= `Default`)。
    pub fn local() -> Self {
        Self::default()
    }

    /// 集群模式,`gate_id` 默认 `"custom"`(简单场景)。
    ///
    /// # 参数
    /// - `gate`: leader 判断实现,调度器只在它返回 leader 时触发 leader 任务。
    pub fn clustered(gate: Arc<dyn LeaderGate>) -> Self {
        Self::clustered_with_id("custom", gate)
    }

    /// 集群模式 + 稳定 `gate_id`(生产建议;同进程误换 gate 会被启动指纹发现)。
    ///
    /// # 参数
    /// - `gate_id`: leader gate 的稳定业务标识,会进入启动指纹。
    /// - `gate`: leader 判断实现,调度器只在它返回 leader 时触发 leader 任务。
    pub fn clustered_with_id(gate_id: impl Into<String>, gate: Arc<dyn LeaderGate>) -> Self {
        Self {
            cluster: Some(ClusterOptions {
                gate,
                gate_id: gate_id.into(),
            }),
            ..Self::default()
        }
    }

    /// 设运行记录器(builder;不调用就是 `NoopRecorder`)。
    ///
    /// # 参数
    /// - `recorder`: 任务 started/finished/skipped 事件的记录器实现。
    pub fn with_recorder(mut self, recorder: Arc<dyn ExecutionRecorder>) -> Self {
        self.recorder = recorder;
        self
    }

    /// 设节点标识(builder;写进 `RunEvent.node`)。
    ///
    /// # 参数
    /// - `node_id`: 当前进程或实例的节点标识,用于运行记录和排查。
    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = node_id.into();
        self
    }

    /// 设 misfire claim 存储(builder;`misfire=FireOnce`/`ClaimOnly` 任务必需)。
    ///
    /// # 参数
    /// - `fire_log`: misfire claim 存储实现,用于跨节点去重和补漏。
    pub fn with_fire_log(mut self, fire_log: Arc<dyn FireLog>) -> Self {
        self.fire_log = Some(fire_log);
        self
    }

    /// 设 misfire 巡检周期(builder;默认 30s)。
    ///
    /// # 参数
    /// - `interval`: 后台 misfire sweep 的执行间隔。
    pub fn with_misfire_sweep_interval(mut self, interval: Duration) -> Self {
        self.misfire_sweep_interval = interval;
        self
    }

    /// 设 misfire claim 容差 ms(builder;默认 5000)。
    ///
    /// # 参数
    /// - `tolerance_ms`: 判定某一拍是否可被补偿 claim 的容差毫秒数。
    pub fn with_misfire_tolerance_ms(mut self, tolerance_ms: i64) -> Self {
        self.misfire_tolerance_ms = tolerance_ms;
        self
    }

    /// 注册一个分组 leader gate(builder;`group="g"` 的 leader 任务用它,负载分散到不同节点)。
    ///
    /// # 参数
    /// - `group`: 任务分组名,匹配 `ScheduledTask::group`。
    /// - `gate`: 该分组使用的 leader 判断实现。
    pub fn with_group_gate(mut self, group: impl Into<String>, gate: Arc<dyn LeaderGate>) -> Self {
        self.group_gates.insert(group.into(), gate);
        self
    }

    /// 同 [`with_group_gate`](Self::with_group_gate),但额外带该 group gate 的**稳定标识** `gate_id`
    /// (建议填该 group 的 leader lock key)→ 进启动指纹,使"同进程二次启动、同 group 名换了底层 gate"被
    /// 通过启动指纹 fail-fast 捕获。不暴露 `nadis::Leader` 内部字段，由业务显式传入 id。
    /// 未调用本方法而只用 `with_group_gate` 时,指纹只能确认 group 集合是否变化,不能确认底层 gate 是否替换。
    ///
    /// # 参数
    /// - `group`: 任务分组名,匹配 `ScheduledTask::group`。
    /// - `gate_id`: 该分组 leader gate 的稳定业务标识,会进入启动指纹。
    /// - `gate`: 该分组使用的 leader 判断实现。
    pub fn with_group_gate_id(
        mut self,
        group: impl Into<String>,
        gate_id: impl Into<String>,
        gate: Arc<dyn LeaderGate>,
    ) -> Self {
        let g = group.into();
        self.group_gate_ids.insert(g.clone(), gate_id.into());
        self.group_gates.insert(g, gate);
        self
    }
}

/// 全局启动配置指纹，用于防止 local/clustered 静默混用以及同一 gate 下 group/FireLog 配置漂移。
/// `recorder` 不进入指纹：`Arc<dyn ExecutionRecorder>` 没有可稳定比较的标识。
/// group gate 按 (group 名, 可选 gate_id) 比对:`with_group_gate_id` 给了 id 则连底层 gate key 一起比;
/// 仅 `with_group_gate`(无 id)则只比 group 名。FireLog 底层 key 经 `FireLog::fingerprint()` 比对。
#[derive(Debug, Clone, PartialEq, Eq)]
enum StartFingerprint {
    Local,
    Clustered {
        gate_id: String,
        /// 已注册的 (group 名, 可选 gate_id),按名排序;group 集合 / 某 group 的 gate_id 变 → 指纹变,二次启动 fail-fast。
        groups: Vec<(String, Option<String>)>,
        /// 是否装了 FireLog(fire_once/claim_only 需要);装/不装变 → 指纹变。
        has_fire_log: bool,
        /// FireLog 底层标识(如 `NadisFireLog` 的 Redis key,见 `FireLog::fingerprint()`);同进程二次启动换 key → 指纹变。
        fire_log_key: Option<String>,
    },
}

impl SchedulerOptions {
    /// 生成启动参数指纹；用于避免重复启动同一个调度任务。
    fn fingerprint(&self) -> StartFingerprint {
        match &self.cluster {
            None => StartFingerprint::Local,
            Some(c) => {
                // 指纹只采集可稳定比较的配置值。trait object 本身不可比较,因此 group gate 通过业务传入的
                // gate_id 参与比对;没给 gate_id 时只比较 group 名集合,保持向后兼容。
                let mut groups: Vec<(String, Option<String>)> = self
                    .group_gates
                    .keys()
                    .map(|g| (g.clone(), self.group_gate_ids.get(g).cloned()))
                    .collect();
                groups.sort();
                StartFingerprint::Clustered {
                    gate_id: c.gate_id.clone(),
                    groups,
                    has_fire_log: self.fire_log.is_some(),
                    fire_log_key: self.fire_log.as_ref().and_then(|f| f.fingerprint()),
                }
            }
        }
    }

    /// 解析某任务应使用的 leader gate:`group=Some(g)` 且已注册 → 用 `group_gates[g]`;否则用 `cluster` 默认 gate。
    /// 启动期为每个 `cluster=leader` 任务调用一次,把结果随任务一起带到 driver(不在触发热路径反复解析)。
    ///
    /// # 参数
    /// - `group`: 任务声明的分组名；`None` 表示未分组,直接回落到默认集群 gate。
    fn gate_for(&self, group: Option<&str>) -> Option<Arc<dyn LeaderGate>> {
        group
            .and_then(|g| self.group_gates.get(g))
            .or(self.cluster.as_ref().map(|c| &c.gate))
            .cloned()
    }
}

/// 触发时刻判定本节点是否应执行该任务。
/// `Local` 恒 true;`Leader` 读 gate;`Leader` 但无 gate = 不应发生(启动期已 fail-fast),保守 false。
///
/// # 参数
/// - `cluster`: 当前任务的集群执行模式。
/// - `gate`: 启动期为该任务解析出的 leader gate；本地任务可为 `None`。
fn should_run(cluster: ClusterMode, gate: &Option<Arc<dyn LeaderGate>>) -> bool {
    match cluster {
        ClusterMode::Local => true,
        ClusterMode::Leader => gate.as_ref().map(|g| g.is_leader()).unwrap_or(false),
    }
}

// ============================================================================
// 运行记录通过 trait 注入，默认 no-op，core 不依赖 Redis。
// 观测面 = fire-and-forget:绝不阻塞 tick、绝不让记录失败/ panic 影响任务(safe_record 隔离 panic)。
// ============================================================================

/// 一次运行的结局状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    /// 任务 future 正常返回成功结果。
    Success,
    /// 任务 future 正常完成但返回失败结果。
    Failed,
    /// 任务级 timeout(`#[scheduled(timeout_ms=...)]`)超时:`tokio::time::timeout` 到点 → 协作式 abort run、记本状态。
    /// **仅在 await 边界生效**：无法终止阻塞 I/O、CPU 死循环或已 detach 的子任务。
    TimedOut,
    /// 任务 future panic,已被调度运行时隔离。
    Panicked,
}

/// 一次运行生命周期阶段。
pub enum RunPhase {
    /// 任务开始执行。
    Started,
    /// 任务已结束。
    Finished {
        /// 本次任务执行的最终状态。
        status: RunStatus,
        /// 从 Started 到 Finished 的耗时毫秒数。
        elapsed_ms: u64,
    },
    /// 本拍**未执行**(无 Started/Finished):非 leader / 并发上限 / claim 失败等。供 metrics/审计回答"为什么没跑"。
    /// ⚠ 高频任务在 follower 上每拍都会产生 `Skipped { NotLeader }`,recorder 自行决定计数/采样/忽略(必须非阻塞)。
    Skipped {
        /// 本拍跳过执行的原因。
        reason: SkipReason,
    },
}

/// [`RunPhase::Skipped`] 的原因(metrics-as-recorder 从同一事件流分类计数)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// 本节点非 leader(`cluster="leader"` 任务):本拍不在本节点触发。
    NotLeader,
    /// 并发已达 `max_in_flight` 上限(`fixed_rate` 过载保护):本拍跳过。
    InflightLimit,
    /// FireOnce/ClaimOnly:本拍已被其它节点/路径认领(claim 返回 false)。
    ClaimLost,
    /// FireOnce/ClaimOnly:claim 调用出错(fail-closed 跳过本拍,等后续正常触发/巡检补)。
    ClaimError,
}

/// 一次运行的生命周期事件。`fire_at_ms` 在同一次运行的 Started/Finished 间一致,作关联键。
pub struct RunEvent<'a> {
    /// 任务展示名(= `#[scheduled]` 的 fn 名或显式 `name`)。
    pub task: &'a str,
    /// 稳定 task id(= [`ScheduledTask::id`];cron 含 `|cron|expr|zone` 签名)。审计/admin 用它关联同一任务的
    /// claim 与运行记录、并区分 repeatable(同名 + 不同 cron)。非 cron 任务 id == name。
    pub id: &'a str,
    /// 本任务的集群模式,用于 recorder 侧区分本地任务与 leader-only 任务。
    pub cluster: ClusterMode,
    /// 名义触发时间(epoch ms)。Started/Finished 使用同一值,便于与 FireLog claim 对账。
    pub fire_at_ms: i64,
    /// 当前执行节点或进程实例标识。
    pub node: &'a str,
    /// 本事件所处的运行阶段。
    pub phase: RunPhase,
}

/// 业务实现它把运行记录落 Redis/DB。**同步、非阻塞**:实现内部应缓冲/批量/spawn 落库,
/// 不在本方法里做阻塞 I/O;记录失败只能内部吞掉或自记日志,不得上抛影响任务。
pub trait ExecutionRecorder: Send + Sync + 'static {
    /// 记录 record 事件；用于保存运行过程中的观测数据。
    ///
    /// # 参数
    /// - `event`: 单次任务运行或跳过事件,包含任务名、稳定 id、节点、名义触发时间和阶段。
    fn record(&self, event: &RunEvent<'_>);
}

/// 默认实现:什么都不记。runtime 恒持有一个 recorder(默认它),避免 Option 分支。
pub struct NoopRecorder;
impl ExecutionRecorder for NoopRecorder {
    /// 记录 record 事件；用于保存运行过程中的观测数据。
    ///
    /// # 参数
    /// - `_`: 被忽略的运行事件；默认记录器不做任何持久化。
    fn record(&self, _: &RunEvent<'_>) {}
}

/// 把每个 [`RunEvent`] 扇出给多个 [`ExecutionRecorder`](让「业务运行记录 + metrics 记录」等并存)。
/// 每个子 recorder 经内部安全记录入口**独立隔离 panic**——一个子 recorder 出错/ panic 不影响其它子 recorder,
/// 也不影响任务本身；该扇出机制允许业务记录器与 metrics 记录器并存。
pub struct CompositeRecorder {
    recorders: Vec<Arc<dyn ExecutionRecorder>>,
}

impl CompositeRecorder {
    /// 用一组子 recorder 构造;空列表 = 等价 [`NoopRecorder`]。
    ///
    /// # 参数
    /// - `recorders`: 需要并行接收运行事件的子记录器列表。
    pub fn new(recorders: Vec<Arc<dyn ExecutionRecorder>>) -> Self {
        Self { recorders }
    }
}

impl ExecutionRecorder for CompositeRecorder {
    /// 记录 record 事件；用于保存运行过程中的观测数据。
    ///
    /// # 参数
    /// - `event`: 需要扇出给所有子 recorder 的运行事件。
    fn record(&self, event: &RunEvent<'_>) {
        for r in &self.recorders {
            safe_record(r.as_ref(), event); // 逐个隔离:一个子 recorder panic 不吞掉其它子 recorder
        }
    }
}

/// 读取当前毫秒时间戳；用于调度器计算下一次触发时间。
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// best-effort 调用 recorder:`catch_unwind` 隔离 panic,记录失败/ panic 不影响任务。
///
/// # 参数
/// - `recorder`: 业务注入的记录器实现。
/// - `event`: 需要记录的运行事件。
fn safe_record(recorder: &dyn ExecutionRecorder, event: &RunEvent<'_>) {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| recorder.record(event)));
    if r.is_err() {
        tracing::error!(
            "[scheduled] ExecutionRecorder::record panic(任务 {}),已隔离",
            event.task
        );
    }
}

/// 记一次"本拍未执行"(`Skipped`)事件;非阻塞、panic 隔离(同 [`safe_record`])。
///
/// # 参数
/// - `recorder`: 记录跳过事件的 recorder。
/// - `name`: 任务展示名。
/// - `id`: 稳定任务 id,用于关联 claim 和 repeatable 任务。
/// - `cluster`: 当前任务的集群模式。
/// - `node_id`: 当前执行节点标识。
/// - `fire_at_ms`: 本拍名义触发时间。
/// - `reason`: 本拍被跳过的业务原因。
#[allow(clippy::too_many_arguments)]
fn record_skip(
    recorder: &dyn ExecutionRecorder,
    name: &'static str,
    id: &'static str,
    cluster: ClusterMode,
    node_id: &str,
    fire_at_ms: i64,
    reason: SkipReason,
) {
    safe_record(
        recorder,
        &RunEvent {
            task: name,
            id,
            cluster,
            fire_at_ms,
            node: node_id,
            phase: RunPhase::Skipped { reason },
        },
    );
}

/// 把一次 run 包成"记录 Started → 跑 → 记录 Finished"的 future(产出 `()`,可直接进 JoinSet)。
/// run 在**内层 JoinSet** 里跑:panic 被收成 JoinError → 记 `Panicked`(不打穿外层);外层 future 被
/// drop(shutdown)时,内层 JoinSet 随之 drop → run 任务一并 abort。not-leader skip 不走本函数(由 record_skip 记 `Skipped` 事件)。
///
/// # 参数
/// - `run`: 宏生成的任务函数指针,调用后返回业务 future。
/// - `recorder`: 运行事件记录器。
/// - `node_id`: 当前节点标识,写入 Started/Finished 事件。
/// - `name`: 任务展示名。
/// - `id`: 稳定任务 id。
/// - `cluster`: 当前任务的集群模式。
/// - `fire_at_ms`: 本次运行的名义触发时间。
/// - `timeout_ms`: 可选任务级超时毫秒数；`None` 表示不做超时控制。
#[allow(clippy::too_many_arguments)]
fn recorded_run(
    run: RunFn,
    recorder: Arc<dyn ExecutionRecorder>,
    node_id: Arc<str>,
    name: &'static str,
    id: &'static str,
    cluster: ClusterMode,
    fire_at_ms: i64,
    timeout_ms: Option<u64>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        safe_record(
            &*recorder,
            &RunEvent {
                task: name,
                id,
                cluster,
                fire_at_ms,
                node: &node_id,
                phase: RunPhase::Started,
            },
        );
        let started = Instant::now();
        let mut js = tokio::task::JoinSet::new();
        js.spawn(run());
        // timeout_ms=Some:tokio::time::timeout 协作式超时——仅在 await 边界生效;阻塞 I/O / CPU 死循环 / 已 detach 的子任务杀不掉。
        let joined = match timeout_ms {
            Some(ms) => tokio::time::timeout(Duration::from_millis(ms), js.join_next()).await,
            None => Ok(js.join_next().await),
        };
        let status = match joined {
            Ok(Some(Ok(RunOutcome::Success))) => RunStatus::Success,
            Ok(Some(Ok(RunOutcome::Failed))) => RunStatus::Failed,
            Ok(Some(Err(je))) if je.is_panic() => RunStatus::Panicked,
            // 内层被 cancel(shutdown abort 链)或异常:运行已被打断,不补 Finished。
            Ok(_) => return,
            // 超时:abort 仍在跑的任务(协作式),记 TimedOut。
            Err(_elapsed) => {
                js.abort_all();
                RunStatus::TimedOut
            }
        };
        let elapsed_ms = started.elapsed().as_millis() as u64;
        safe_record(
            &*recorder,
            &RunEvent {
                task: name,
                id,
                cluster,
                fire_at_ms,
                node: &node_id,
                phase: RunPhase::Finished { status, elapsed_ms },
            },
        );
    })
}

/// 调度方式(由 #[scheduled] 的参数决定):cron / fixedRate / fixedDelay / 仅 initialDelay 的一次性任务。
/// `initial_delay_ms=None` 表示未配置首跑延迟。
#[derive(Debug, Clone, Copy)]
pub enum Schedule {
    /// cron 表达式(6 字段含秒:秒 分 时 日 月 周);`expr = "-"` = 禁用。
    /// `zone`:cron 触发时区。`None`/`"UTC"` = UTC;IANA 名(如 `Asia/Shanghai`,含夏令时)经 chrono-tz 解析;
    /// 固定 offset(`"+08:00"`/`"-05:30"`)经 chrono 解析。非法 zone 在启动时返 `Err`(放运行时而非编译期,便于给清晰文案)。
    Cron {
        /// cron 表达式文本,6 字段含秒。
        expr: &'static str,
        /// cron 触发时区;`None` 表示 UTC。
        zone: Option<&'static str>,
    },
    /// 固定频率(`fixedRate` 语义):两次**开始**间隔 `period_ms`,**按开始时间触发、不论任务耗时**。
    /// `initial_delay_ms`:`Some(d)` → 首跑在 d 后(`Some(0)` = 立即);`None` → 一个 period 后首跑。
    /// (注:宏对主名 `fixed_rate(_ms/_string)` 不配首延迟时默认生成 `Some(0)` 立即首跑;`None` 仅旧别名 `every_ms` 走到。)
    ///
    /// **重叠**:周期 < 任务耗时时,多次执行会并发重叠(每拍独立 spawn,稳态并发数 ≈ 任务耗时 / 周期)。
    /// `max_in_flight`:并发上限(过载保护)。`None` = 不限(允许重叠);`Some(n)` = 在跑数已达 n 则**跳过本拍**
    /// (`Some(1)` 即"上次没跑完就跳过",对应宏参数 `skip_if_running`)。不限时若任务长期卡住,在飞数会累积。
    FixedRate {
        /// 两次任务开始之间的间隔毫秒数。
        period_ms: u64,
        /// 首次执行前的延迟毫秒数;`None` 表示一个周期后首跑。
        initial_delay_ms: Option<u64>,
        /// 同一任务允许并发在飞的上限;达到上限时跳过本拍。
        max_in_flight: Option<u64>,
    },
    /// 固定延迟(对照 `fixedDelay`):上次**完成**后再等 `delay_ms` 跑下次。
    /// `initial_delay_ms=None` → 立即首跑;`Some(d)` → 首跑在 d 后。
    FixedDelay {
        /// 上次完成到下次开始之间的延迟毫秒数。
        delay_ms: u64,
        /// 首次执行前的延迟毫秒数;`None` 表示立即首跑。
        initial_delay_ms: Option<u64>,
    },
    /// 一次性(对照仅 `initialDelay`):启动后 `delay_ms` 跑【一次】(= 旧 `delay_ms` 单独)。
    OneShot {
        /// 启动后等待多久触发唯一一次执行。
        delay_ms: u64,
    },
}

/// 任务一次执行的结果(供 [`ExecutionRecorder`] 区分 Success/Failed)。
/// 不携带错误值，因此不会对任务的 `E` 强加 `Debug`/`Display` 约束；在不增加该约束时无法提取错误文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// 任务业务逻辑成功完成。
    Success,
    /// 任务业务逻辑返回失败。
    Failed,
}

/// 把被注解 fn 包成"产出 [`RunOutcome`] 的 future"的函数指针类型(宏生成的非捕获闭包 coerce 而来)。
/// `+ Send` 供 tokio-cron-scheduler/多线程运行时跨线程执行。
pub type RunFn = fn() -> Pin<Box<dyn Future<Output = RunOutcome> + Send>>;

/// 一个被 #[scheduled] 标记的定时任务项(字段全 'static/函数指针,可放进 static 被 linkme 收集)。
pub struct ScheduledTask {
    /// 展示用任务名,用于日志、指标和 recorder 事件。
    pub name: &'static str,
    /// 稳定唯一 task id(宏生成;`FireOnce`/`ClaimOnly` 的 FireLog claim key)。cron=`name|cron|expr|zone`,非 cron=`name`。
    /// 与 `name`（展示名）分离：repeatable 同名但 cron 不同时，fire_once claim 不会互相冲突。
    pub id: &'static str,
    /// 任务触发方式和时间参数。
    pub schedule: Schedule,
    /// 见 [`RunFn`]。
    pub run: RunFn,
    /// 集群执行模式(由 `#[scheduled(cluster=...)]` 决定,默认 `Local`)。
    pub cluster: ClusterMode,
    /// 漏触发补偿策略(由 `#[scheduled(misfire=...)]` 决定,默认 `Skip`)。
    pub misfire: MisfirePolicy,
    /// 任务级超时 ms(由 `#[scheduled(timeout_ms=...)]` 决定;`None`=不超时)。超时记 `RunStatus::TimedOut`(协作式取消,不强杀)。
    pub timeout_ms: Option<u64>,
    /// 分组(由 `#[scheduled(group=...)]` 决定;仅 `cluster=leader` 有意义)。决定用哪个 `SchedulerOptions::group_gates` gate。
    pub group: Option<&'static str>,
}

/// 全局分布式数组:所有 #[scheduled] 任务项汇集于此(链接器编译期收集,零运行时扫描)。
#[distributed_slice]
pub static SCHEDULED_TASKS: [ScheduledTask];

// ============================================================================
// misfire 补偿仅用于 cron + cluster=leader + FireOnce 任务。
// 无主窗口漏掉的那拍,由独立低频巡检任务在 leader 上补一次;靠 FireLog 的原子 claim 做
// 集群级"至多一次"去重(正常触发与补偿、双主下的多 leader 都经同一 claim)。
// ============================================================================

/// 漏触发补偿策略(由 `#[scheduled(misfire=...)]` 决定,默认 `Skip`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MisfirePolicy {
    /// 默认:漏了就漏了,等下一次正常触发(靠业务幂等/扫描自愈)。**leader-only**:仅 `gate.is_leader()` 放行,**无跨节点去重**。
    Skip,
    /// 每拍经 FireLog claim 去重(同一 `scheduled_at` 跨节点只触发一次)**+ misfire 巡检补漏**无主窗口漏掉的一拍。
    FireOnce,
    /// 每拍经 FireLog claim 去重(同一 `scheduled_at` 跨节点只触发一次),但**不做** misfire 补漏。
    /// 适合需要同拍去重但不需要补漏拍的任务，将“同拍去重”与“misfire 补偿”解耦。
    ClaimOnly,
}

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 每个任务“上次已认领触发时刻”的集群共享存储。`misfire=FireOnce`/`ClaimOnly` 任务需要，业务可用 Redis/DB 实现。
/// 注意:记录的是 claim(谁负责触发了这一拍),**不是业务执行成功**;执行结果由 [`ExecutionRecorder`] 记。
pub trait FireLog: Send + Sync + 'static {
    /// 读取最近一次 fire；用于续跑和幂等判断。
    ///
    /// # 参数
    /// - `task`: 稳定任务 id,不是展示名；Redis/DB 实现通常把它作为 field 或唯一键。
    fn last_fire<'a>(&'a self, task: &'a str) -> BoxFut<'a, anyhow::Result<Option<i64>>>;

    /// **原子 claim**:仅当现存值为空或 `< stale_before_ms` 时写入 `scheduled_at_ms` 并返回 true;否则 false。
    /// 正常触发:`stale_before = scheduled_at`(去掉双主窗口同一拍重复);补偿:`stale_before = scheduled_at - tol`。
    /// Redis 实现 = HASH 字段上一段 Lua(GET→比较→HSET,原子)。
    ///
    /// # 参数
    /// - `task`: 稳定任务 id,作为 claim 存储键。
    /// - `scheduled_at_ms`: 本拍名义触发时间(epoch ms),claim 成功后写入存储。
    /// - `stale_before_ms`: 旧值小于该时间才允许覆盖,用于正常去重和 misfire 补偿窗口。
    fn try_claim_fire<'a>(
        &'a self,
        task: &'a str,
        scheduled_at_ms: i64,
        stale_before_ms: i64,
    ) -> BoxFut<'a, anyhow::Result<bool>>;

    /// 本 FireLog 的稳定启动指纹（例如 Redis key），供 `StartFingerprint` 检测同进程
    /// 二次启动时 FireLog 底层 key 漂移。默认 `None`(不可标识的实现 → 仅按"是否装了 FireLog"判定)。
    fn fingerprint(&self) -> Option<String> {
        None
    }
}

/// cron 时区(启动期解析一次):UTC / IANA / 固定 offset。**FireOnce/ClaimOnly 自管 driver 与(仅 FireOnce 的)misfire 巡检共用此解析**
/// (它俩参与 claim 去重,须对同一拍算出同一 `scheduled_at`);**Skip cron 由 tokio-cron-scheduler 自身解析**
/// 不参与 claim 或名义时刻对账，只要求各实现保持相同语义。
enum CronZone {
    Utc,
    Iana(chrono_tz::Tz),
    Offset(chrono::FixedOffset),
}

/// 把 zone 字符串解析成 [`CronZone`](复用 [`parse_fixed_offset`];与 [`build_cron_job`] 的 zone 语义一致)。
///
/// # 参数
/// - `zone`: 注解里的 cron 时区；空/`UTC`/`GMT`/`Z` 走 UTC,带 `/` 走 IANA,其余按固定 offset 解析。
fn resolve_cron_zone(zone: Option<&str>) -> anyhow::Result<CronZone> {
    let z = zone.map(|s| s.trim()).unwrap_or("");
    if z.is_empty() || z.eq_ignore_ascii_case("UTC") || z.eq_ignore_ascii_case("GMT") || z == "Z" {
        Ok(CronZone::Utc)
    } else if z.contains('/') {
        let tz: chrono_tz::Tz = z
            .parse()
            .map_err(|_| anyhow::anyhow!("未知 cron zone(IANA 名)\"{z}\""))?;
        Ok(CronZone::Iana(tz))
    } else {
        let off = parse_fixed_offset(z).ok_or_else(|| {
            anyhow::anyhow!("非法 cron zone \"{z}\"(用 UTC / IANA 名如 \"Asia/Shanghai\" / 固定 offset 如 \"+08:00\")")
        })?;
        Ok(CronZone::Offset(off))
    }
}

/// 自管 cron 计划：启动期解析表达式和 zone（失败即启动失败），并给出名义触发时刻（epoch ms，UTC）。
/// FireOnce/ClaimOnly 自管 driver 与(仅 FireOnce 的)misfire 巡检**共用同一 plan**,保证同一拍跨节点/跨路径算出完全一致的 `scheduled_at`。
pub struct CronPlan {
    cron: croner::Cron,
    zone: CronZone,
}

impl CronPlan {
    /// 启动期解析。croner `with_seconds_required`:**强制 6 字段含秒**(秒 分 时 日 月 周),与
    /// `Schedule::Cron` 文档约定一致;5 字段 fail-fast(避免缺秒被静默误解)。
    /// 失败 → Err(由调用方转启动失败)。
    ///
    /// # 参数
    /// - `expr`: 6 字段 cron 表达式。
    /// - `zone`: 可选 cron 时区,语义同 [`resolve_cron_zone`]。
    fn parse(expr: &str, zone: Option<&str>) -> anyhow::Result<Self> {
        let cron = croner::Cron::new(expr)
            .with_seconds_required()
            .parse()
            .map_err(|e| anyhow::anyhow!("非法 cron 表达式 \"{expr}\"(需 6 字段含秒): {e}"))?;
        let zone = resolve_cron_zone(zone)?;
        Ok(Self { cron, zone })
    }

    /// 严格晚于 `after_ms` 的下一个名义触发时刻(epoch ms)。
    ///
    /// # 参数
    /// - `after_ms`: 基准时间(epoch ms),返回值必须严格晚于它。
    fn next_after(&self, after_ms: i64) -> anyhow::Result<i64> {
        let after = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(after_ms)
            .ok_or_else(|| anyhow::anyhow!("非法时间戳 {after_ms}"))?;
        let r = match &self.zone {
            CronZone::Utc => self
                .cron
                .find_next_occurrence(&after, false)
                .map(|d| d.timestamp_millis()),
            CronZone::Iana(tz) => self
                .cron
                .find_next_occurrence(&after.with_timezone(tz), false)
                .map(|d| d.timestamp_millis()),
            CronZone::Offset(off) => self
                .cron
                .find_next_occurrence(&after.with_timezone(off), false)
                .map(|d| d.timestamp_millis()),
        };
        r.map_err(|e| anyhow::anyhow!("cron next_after 计算失败: {e}"))
    }

    /// `now_ms` 之前(含)最近的名义触发时刻;无则 `None`。croner 无 prev API,用 doubling 反向窗口。
    ///
    /// # 参数
    /// - `now_ms`: 当前或指定墙钟时间(epoch ms),用于查找最近一拍。
    fn prev_before_or_equal(&self, now_ms: i64) -> Option<i64> {
        let now = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now_ms)?;
        match &self.zone {
            CronZone::Utc => latest_before(&self.cron, now).map(|d| d.timestamp_millis()),
            CronZone::Iana(tz) => {
                latest_before(&self.cron, now.with_timezone(tz)).map(|d| d.timestamp_millis())
            }
            CronZone::Offset(off) => {
                latest_before(&self.cron, now.with_timezone(off)).map(|d| d.timestamp_millis())
            }
        }
    }
}

/// doubling 反向窗口:从 `now - window` 向前迭代,取 ≤ now 的最后一个;找不到则翻倍窗口,上限 ~5 年(1830 天)。
///
/// # 参数
/// - `cron`: 已解析的 croner 表达式。
/// - `now`: 带时区的基准时间,返回值不会晚于它。
fn latest_before<Tz: chrono::TimeZone>(
    cron: &croner::Cron,
    now: chrono::DateTime<Tz>,
) -> Option<chrono::DateTime<Tz>> {
    // 上限 ~5 年:覆盖最低频的标准 cron(如闰日 `0 0 0 29 2 *`,间隔可达 ~4 年)。doubling 对常见 cron
    // 仍在头几轮命中,极低频才会走到大窗口且每窗口产出极少,代价可忽略。
    let max = chrono::Duration::days(1830);
    let mut window = chrono::Duration::seconds(120);
    loop {
        let start = now.clone() - window;
        let mut last: Option<chrono::DateTime<Tz>> = None;
        for (i, occ) in cron.iter_from(start).enumerate() {
            if i > 100_000 || occ > now {
                break;
            }
            last = Some(occ);
        }
        if last.is_some() {
            return last;
        }
        if window >= max {
            return None;
        }
        window = std::cmp::min(window * 2, max);
    }
}

/// FireOnce/ClaimOnly 任务的单次触发:先 claim(原子去重),拿到才跑(经 recorded_run 记录);claim 失败 fail-closed 跳过。
///
/// # 参数
/// - `run`: 宏生成的任务函数指针。
/// - `recorder`: 运行事件记录器。
/// - `node_id`: 当前节点标识。
/// - `name`: 任务展示名。
/// - `id`: 稳定任务 id,作为 FireLog claim key。
/// - `cluster`: 当前任务的集群模式。
/// - `fire_log`: 跨节点共享的 claim 存储。
/// - `scheduled_at_ms`: 本拍名义触发时间(epoch ms)。
/// - `stale_before_ms`: 允许覆盖旧 claim 的阈值(epoch ms)。
/// - `timeout_ms`: 可选任务级超时毫秒数。
#[allow(clippy::too_many_arguments)]
fn fire_once_guarded(
    run: RunFn,
    recorder: Arc<dyn ExecutionRecorder>,
    node_id: Arc<str>,
    name: &'static str,
    id: &'static str,
    cluster: ClusterMode,
    fire_log: Arc<dyn FireLog>,
    scheduled_at_ms: i64,
    stale_before_ms: i64,
    timeout_ms: Option<u64>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        // claim 用稳定 id(非展示 name);RunEvent/record 仍用 name。
        match fire_log
            .try_claim_fire(id, scheduled_at_ms, stale_before_ms)
            .await
        {
            // fire_at_ms 使用 nominal scheduled_at 而非 now_ms，使运行记录能与 FireLog claim 对账。
            Ok(true) => {
                recorded_run(
                    run,
                    recorder,
                    node_id,
                    name,
                    id,
                    cluster,
                    scheduled_at_ms,
                    timeout_ms,
                )
                .await
            }
            Ok(false) => {
                tracing::trace!(
                    "[scheduled] {} scheduled_at={} 已被认领,跳过",
                    name,
                    scheduled_at_ms
                );
                record_skip(
                    recorder.as_ref(),
                    name,
                    id,
                    cluster,
                    node_id.as_ref(),
                    scheduled_at_ms,
                    SkipReason::ClaimLost,
                );
            }
            // claim 不可用时 fail-closed，避免绕过去重门禁；等待正常触发或巡检补偿。
            Err(e) => {
                tracing::error!(
                    "[scheduled] {} try_claim_fire 失败,fail-closed 跳过:{}",
                    name,
                    e
                );
                record_skip(
                    recorder.as_ref(),
                    name,
                    id,
                    cluster,
                    node_id.as_ref(),
                    scheduled_at_ms,
                    SkipReason::ClaimError,
                );
            }
        }
    })
}

/// FireOnce/ClaimOnly cron 任务的运行期数据(driver +(仅 FireOnce 的)巡检共用同一 [`CronPlan`])。
struct FireOnceTask {
    name: &'static str,
    /// 稳定 claim id(见 [`ScheduledTask::id`]);claim/last_fire 用它,`name` 仅作展示。
    id: &'static str,
    run: RunFn,
    cluster: ClusterMode,
    plan: Arc<CronPlan>,
    /// 是否参与 misfire 巡检补漏:`FireOnce`=true;`ClaimOnly`=false(只每拍 claim 去重,不补漏)。
    sweep: bool,
    /// 任务级超时 ms(`None`=不超时)。
    timeout_ms: Option<u64>,
    /// 本任务(按 group)解析好的 leader gate(driver + sweep 都用它做 should_run)。
    gate: Option<Arc<dyn LeaderGate>>,
}

/// FireOnce/ClaimOnly cron 的自管 driver：不使用 tokio-cron-scheduler，因为其回调拿不到名义触发时刻。
/// driver 自己用 `CronPlan::next_after` 算出本拍的【精确名义 `scheduled_at`】,睡到点后用它做 claim + recorder,
/// 跨节点一致 → claim 去重成立。run 用内层 JoinSet spawn(长任务不阻塞下一拍的计算);shutdown 时随 JoinSet abort。
///
/// # 参数
/// - `plan`: 已解析 cron 计划,负责计算下一拍名义触发时间。
/// - `run`: 宏生成的任务函数指针。
/// - `recorder`: 运行事件记录器。
/// - `node_id`: 当前节点标识。
/// - `name`: 任务展示名。
/// - `id`: 稳定任务 id,作为 FireLog claim key。
/// - `cluster`: 当前任务的集群模式。
/// - `gate`: 启动期解析好的 leader gate。
/// - `fire_log`: 跨节点共享的 claim 存储。
/// - `timeout_ms`: 可选任务级超时毫秒数。
#[allow(clippy::too_many_arguments)]
async fn fire_once_cron_driver(
    plan: Arc<CronPlan>,
    run: RunFn,
    recorder: Arc<dyn ExecutionRecorder>,
    node_id: Arc<str>,
    name: &'static str,
    id: &'static str,
    cluster: ClusterMode,
    gate: Option<Arc<dyn LeaderGate>>,
    fire_log: Arc<dyn FireLog>,
    timeout_ms: Option<u64>,
) {
    let mut inflight = tokio::task::JoinSet::new();
    loop {
        let now = now_ms();
        // 每轮都按当前墙钟重新求下一拍,不把进程暂停期间错过的旧拍排队补跑。
        // FireOnce 的漏拍由 sweep 低频补偿;ClaimOnly 明确只做同拍去重,不做补漏。
        let next = match plan.next_after(now) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(
                    "[scheduled] {} cron next_after 失败,driver 退出:{}",
                    name,
                    e
                );
                return;
            }
        };
        tokio::time::sleep(Duration::from_millis((next - now).max(0) as u64)).await;
        while let Some(res) = inflight.try_join_next() {
            log_join(res, name);
        }
        // sleep 期间 leader 可能已经换届;到点后再读一次 gate,非 leader 只记跳过,不抢 claim。
        if should_run(cluster, &gate) {
            // 用【精确名义时刻 next】做 claim(stale_before=next:同一拍只一次)+ recorder,跨节点一致。
            // claim 失败由 fire_once_guarded 统一 fail-closed:不执行业务,等待后续正常触发或 sweep 再尝试。
            inflight.spawn(fire_once_guarded(
                run,
                recorder.clone(),
                node_id.clone(),
                name,
                id,
                cluster,
                fire_log.clone(),
                next,
                next,
                timeout_ms,
            ));
        } else {
            record_skip(
                recorder.as_ref(),
                name,
                id,
                cluster,
                node_id.as_ref(),
                next,
                SkipReason::NotLeader,
            );
        }
    }
}

/// misfire 巡检后台循环：仅在 leader 上低频地为每个 FireOnce cron 任务补漏一拍。
/// 不挂在 cron 回调上(cron 两次触发间无 tick)。tolerance 吸收节点间时钟偏差 + 一个 sweep 周期。
/// 补偿用 spawn(不 inline await):单个长任务不拖垮整轮巡检。
///
/// # 参数
/// - `tasks`: 需要补漏的 FireOnce cron 任务列表。
/// - `fire_log`: 跨节点共享的 claim 存储。
/// - `recorder`: 运行事件记录器。
/// - `node_id`: 当前节点标识。
/// - `sweep_interval`: 巡检周期。
/// - `tolerance_ms`: 认定旧 claim 可被补偿覆盖的容差毫秒数。
async fn misfire_sweep_loop(
    tasks: Vec<FireOnceTask>,
    fire_log: Arc<dyn FireLog>,
    recorder: Arc<dyn ExecutionRecorder>,
    node_id: Arc<str>,
    sweep_interval: Duration,
    tolerance_ms: i64,
) {
    let mut ticker = tokio::time::interval(sweep_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut inflight = tokio::task::JoinSet::new();
    loop {
        ticker.tick().await;
        while let Some(res) = inflight.try_join_next() {
            log_join(res, "misfire");
        }
        for t in &tasks {
            // 每个任务独立判定 group leader 与最近名义时刻;单个任务异常不能拖垮整轮巡检。
            // 仅本任务 group 的 leader 巡检(读本地原子,不碰 Redis);per-group leader 下逐任务判定。
            if !should_run(ClusterMode::Leader, &t.gate) {
                continue;
            }
            let now = now_ms();
            let Some(prev) = t.plan.prev_before_or_equal(now) else {
                continue;
            };
            let last = match fire_log.last_fire(t.id).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("[scheduled] {} misfire 巡检 last_fire 失败:{}", t.name, e);
                    continue;
                }
            };
            // 快查:看似漏了(从无记录,或上次认领早于 prev-容差)才 spawn 原子 claim 补(不阻塞巡检)。
            // 这里先用 last_fire 降低无效 claim 压力;最终是否补跑仍由 try_claim_fire 原子判定,避免多节点并发补同一拍。
            if last.is_none_or(|l| l < prev - tolerance_ms) {
                inflight.spawn(fire_once_guarded(
                    t.run,
                    recorder.clone(),
                    node_id.clone(),
                    t.name,
                    t.id,
                    t.cluster,
                    fire_log.clone(),
                    prev,
                    prev - tolerance_ms,
                    t.timeout_ms,
                ));
            }
        }
    }
}

/// misfire 巡检周期与 claim 容差的默认值。
const MISFIRE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// 避免极端配置在 Tokio interval deadline 中溢出。
const MAX_MISFIRE_SWEEP_INTERVAL: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const MISFIRE_TOLERANCE_MS: i64 = 5_000;

/// FireOnce/ClaimOnly 的 claim 以稳定 **task id** 作 key(见 [`ScheduledTask::id`];`NadisFireLog` 用它当 Redis HASH field)。
/// 两个任务若 id 相同(= 同 `name` + 同 `cron`+`zone`)会共享同一 claim field → `scheduled_at` 序列互相覆盖/阻塞,
/// 因此必须在启动期 fail-fast。id 已含 cron 签名，repeatable 的“同名 + 不同 cron”天然不冲突；
/// 只有真正等价的两个任务才会触发本错误。
///
/// # 参数
/// - `ids`: 本次启动收集到的 FireOnce/ClaimOnly 稳定任务 id 集合。
fn ensure_unique_fireonce_ids<'a>(ids: impl IntoIterator<Item = &'a str>) -> anyhow::Result<()> {
    let mut seen = std::collections::HashSet::new();
    for id in ids {
        anyhow::ensure!(
            seen.insert(id),
            "[scheduled] fire_once/claim_only 任务 id 重复:{id:?}(同 name + 同 cron+zone)。claim 必须唯一;\
             请给其中一个不同的 #[scheduled(name = \"...\")] 或不同 cron"
        );
    }
    Ok(())
}

// cron 调度器要【活到进程结束】(drop 会停掉后台 tick),存进全局 static 保命;`shutdown_scheduled` 取出并 shutdown。
static SCHED: Mutex<Option<JobScheduler>> = Mutex::new(None);
// non-cron 后台 loop 的句柄(供 shutdown abort)。
static NON_CRON_HANDLES: Mutex<Vec<JoinHandle<()>>> = Mutex::new(Vec::new());
// 幂等且并发安全的启动状态。保存启动配置指纹而不是裸 bool：
//   None=未启动;Some(fp)=已用 fp 启动。重复启动只允许同指纹(幂等 Ok),不同指纹报错——
//   防"先 local 再 clustered"等静默混用。async Mutex 把 start/shutdown 串行化,消除并发假成功。
static START_STATE: tokio::sync::Mutex<Option<StartFingerprint>> =
    tokio::sync::Mutex::const_new(None);

/// 调度库对宿主容器开放的只读运行时句柄。
///
/// 句柄不包含关闭或重启入口，因此持有者可以观测底层调度库，却不能绕过宿主的逆序清理协议。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SchedulerHandle;

impl SchedulerHandle {
    /// 查询调度库是否已经提交启动指纹。
    ///
    /// # 参数
    ///
    /// 本方法无参数；读取与启动、关闭共用同一把异步锁，结果不会看到提交到一半的状态。
    pub async fn is_running(&self) -> bool {
        START_STATE.lock().await.is_some()
    }

    /// 返回当前二进制在编译期收集到的调度任务数量。
    ///
    /// # 参数
    ///
    /// 本方法无参数；数量包含配置为禁用的任务，是否实际触发仍由任务声明决定。
    pub fn declared_task_count(&self) -> usize {
        SCHEDULED_TASKS.len()
    }
}

/// 创建不带生命周期所有权的只读调度运行时句柄。
///
/// # 参数
///
/// 本函数无参数；宿主应在自己的调度组件已经启动后再把句柄交给业务。
pub const fn scheduler_handle() -> SchedulerHandle {
    SchedulerHandle
}

// #[scheduled] 任务返回值处理:由宏按返回类型生成包装(对照原"返回值被调度器忽略"语义):
//   - **带显式路径的标准 Result**(仅 std::result::Result / core::result::Result / anyhow::Result)→ `Err` 记 error 日志
//     (带任务名,**不格式化错误值**故不要求 `E: Debug`),`Ok(_)` 忽略;
//   - 其它任意返回类型 + 裸名 `Result` + 类型别名 + 自定义 `xxx::Result` → 直接忽略(裸名无法解析来源,保守忽略防误判)。
// 故运行端不再用 trait 兜(blanket `impl<T>` 与 `impl Result` 在稳定 Rust 会冲突),改在宏展开处按真 Result 路径分派。

/// 记录一次执行的 JoinHandle/JoinSet 退出状态(带任务名上下文)。
///
/// 任务体里的 `Result::Err` 已由宏生成的 run 包装在内部打了日志;这里兜的是**另一类失败**:任务体 **panic**
/// 或被 **cancel**(shutdown)。每次执行都用 `tokio::spawn` 跑、再看其退出码——这样一次 panic 只损失这一次执行,
/// 不会打掉外层调度 driver(否则该任务后续周期会永久停摆),且生产里能按任务名聚合 panic 而非只靠 stderr 的 panic hook。
///
/// # 参数
/// - `res`: JoinSet/JoinHandle 返回的执行结果。
/// - `name`: 任务展示名,用于日志定位。
fn log_join(res: Result<(), tokio::task::JoinError>, name: &str) {
    match res {
        Ok(()) => {}
        Err(e) if e.is_cancelled() => {
            tracing::debug!("[scheduled] 任务 {} 已取消(shutdown)", name)
        }
        Err(e) => tracing::error!("[scheduled] 任务 {} 执行 panic:{}", name, e),
    }
}

/// 启动所有 #[scheduled] 任务(由 #[EnableScheduling](及兼容别名 #[EnableAsync])在 main 体首部 .await 调用,此时已在 tokio runtime 内)。
///
/// **幂等**:重复调用直接 `Ok(())`,不重复注册/重复后台 loop。
/// **非 cron 任务用 `tokio::time` 真毫秒精度**:`tokio-cron-scheduler` 的 one-shot/repeated 内部按
/// `Duration::as_secs()` 截断到秒 + 固定 500ms tick,`_ms` 名不副实;故 delay/every/delay+every 改为自管 tokio 任务。
/// cron:misfire=Skip 走 `tokio-cron-scheduler`;misfire=FireOnce/ClaimOnly 走自管 `CronPlan` driver。
///
/// **两阶段 + 失败不污染启动状态**:先做全部可失败步骤(校验周期、建 cron job、起 cron 调度器),
/// **全部成功后**才 spawn non-cron 后台 loop——避免"cron 失败但 non-cron 已在跑"的半启动;失败则保持"未启动",
/// 使重试不被误判为"已启动"。`cron = "-"` 跳过注册。
/// **并发安全**:持启动锁串行化,后到的并发调用要么看到已启动直接返回、要么等首启结束后真正启动,不会拿到假成功。
///
pub async fn start_scheduled() -> anyhow::Result<()> {
    start_scheduled_with(SchedulerOptions::local()).await
}

/// 用显式 [`SchedulerOptions`] 启动(本地或集群)。`start_scheduled()` = `start_scheduled_with(local())`。
///
/// # 参数
/// - `opts`: 调度器启动选项,包含本地/集群模式、记录器、misfire 和分组 gate 配置。
///
/// 启动指纹保证幂等：同配置重复调用返回 Ok；不同配置（如先 local 后 clustered，或更换
/// gate_id)返回 Err,提示先 `shutdown_scheduled()`。校验在 spawn 任何 loop 之前完成,失败不留半启动。
pub async fn start_scheduled_with(opts: SchedulerOptions) -> anyhow::Result<()> {
    let fp = opts.fingerprint();
    let mut state = START_STATE.lock().await; // 全程持锁:start 与并发 start/shutdown 严格串行
    match &*state {
        Some(existing) if *existing == fp => {
            tracing::debug!("[scheduled] 已以相同配置启动,跳过重复 start(幂等)");
            return Ok(());
        }
        Some(existing) => {
            anyhow::bail!(
                "[scheduled] 已以 {existing:?} 启动,不能再以 {fp:?} 启动;请先 shutdown_scheduled()"
            );
        }
        None => {}
    }
    start_scheduled_inner(&opts).await?; // 失败:state 仍 None,允许重试;`?` 把错误传给本次调用
    *state = Some(fp);
    Ok(())
}

/// 把固定 offset 字符串解析为 `FixedOffset`。容忍多种写法:`+08:00` / `+0800` / `+8` / `-05:30`,
/// 以及带 `GMT`/`UTC` 前缀的 `GMT+08:00` / `UTC+8`(剥前缀后同上)。空(仅 `GMT`/`UTC`)= 0 偏移。
///
/// # 参数
/// - `s`: 用户配置的固定时区 offset 字符串。
fn parse_fixed_offset(s: &str) -> Option<chrono::FixedOffset> {
    // 剥掉可选的 GMT/UTC 前缀。
    let body = s
        .strip_prefix("GMT")
        .or_else(|| s.strip_prefix("gmt"))
        .or_else(|| s.strip_prefix("UTC"))
        .or_else(|| s.strip_prefix("utc"))
        .unwrap_or(s)
        .trim();
    if body.is_empty() {
        return chrono::FixedOffset::east_opt(0); // 裸 GMT/UTC
    }
    let (sign, rest) = match body.as_bytes()[0] {
        b'+' => (1, &body[1..]),
        b'-' => (-1, &body[1..]),
        _ => return None,
    };
    // 分出时/分:支持 "HH:MM" / "HHMM" / "H" / "HH"。
    let (h, m) = if let Some((h, m)) = rest.split_once(':') {
        (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?)
    } else if rest.len() > 2 {
        let (h, m) = rest.split_at(rest.len() - 2);
        (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?)
    } else {
        (rest.parse::<i32>().ok()?, 0)
    };
    if !(0..=23).contains(&h) || !(0..=59).contains(&m) {
        return None;
    }
    chrono::FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

/// Skip cron 一次到点要执行的 future:非本节点(Leader 且非 leader)→ 空跳过;否则 recorded_run。
/// **仅给 misfire=Skip 的 cron 用**(经 tokio-cron-scheduler 回调);FireOnce/ClaimOnly cron 走自管 [`fire_once_cron_driver`]。
/// 注:Skip cron 经回调触发拿不到名义触发时刻,故 `RunEvent.fire_at_ms` 用 `now_ms()`(它不参与 claim 去重)。
///
/// # 参数
/// - `should`: 当前触发点是否允许本节点执行。
/// - `run`: 宏生成的任务函数指针。
/// - `recorder`: 运行事件记录器。
/// - `node_id`: 当前节点标识。
/// - `name`: 任务展示名。
/// - `id`: 稳定任务 id。
/// - `cluster`: 当前任务的集群模式。
/// - `timeout_ms`: 可选任务级超时毫秒数。
#[allow(clippy::too_many_arguments)]
fn cron_fire(
    should: bool,
    run: RunFn,
    recorder: &Arc<dyn ExecutionRecorder>,
    node_id: &Arc<str>,
    name: &'static str,
    id: &'static str,
    cluster: ClusterMode,
    timeout_ms: Option<u64>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    if !should {
        record_skip(
            recorder.as_ref(),
            name,
            id,
            cluster,
            node_id.as_ref(),
            now_ms(),
            SkipReason::NotLeader,
        );
        return Box::pin(async {});
    }
    recorded_run(
        run,
        recorder.clone(),
        node_id.clone(),
        name,
        id,
        cluster,
        now_ms(),
        timeout_ms,
    )
}

/// 按 zone 建 cron Job(**仅 misfire=Skip cron**)。zone 解析:`UTC`/`GMT`/`Z`/空 → UTC;含 `/` → IANA;
/// 其余 → 固定 offset。无法解析 → `Err`。FireOnce/ClaimOnly cron 不走这里(见 [`fire_once_cron_driver`])。
///
/// # 参数
/// - `expr`: 6 字段 cron 表达式。
/// - `zone`: 可选 cron 时区。
/// - `run`: 宏生成的任务函数指针。
/// - `cluster`: 当前任务的集群模式。
/// - `gate`: 启动期解析好的 leader gate。
/// - `name`: 任务展示名。
/// - `id`: 稳定任务 id。
/// - `recorder`: 运行事件记录器。
/// - `node_id`: 当前节点标识。
/// - `timeout_ms`: 可选任务级超时毫秒数。
#[allow(clippy::too_many_arguments)]
fn build_cron_job(
    expr: &'static str,
    zone: Option<&'static str>,
    run: RunFn,
    cluster: ClusterMode,
    gate: Option<Arc<dyn LeaderGate>>,
    name: &'static str,
    id: &'static str,
    recorder: Arc<dyn ExecutionRecorder>,
    node_id: Arc<str>,
    timeout_ms: Option<u64>,
) -> anyhow::Result<Job> {
    let z = match zone {
        None => "",
        Some(s) => s.trim(),
    };
    // 闭包是 Fn(每次到点调一次)→ cron_fire 决定跑/跳。捕获项在被命中分支里 move 进闭包。
    let job = if z.is_empty()
        || z.eq_ignore_ascii_case("UTC")
        || z.eq_ignore_ascii_case("GMT")
        || z == "Z"
    {
        Job::new_async(expr, move |_uuid, _l| {
            cron_fire(
                should_run(cluster, &gate),
                run,
                &recorder,
                &node_id,
                name,
                id,
                cluster,
                timeout_ms,
            )
        })?
    } else if z.contains('/') {
        let tz: chrono_tz::Tz = z
            .parse()
            .map_err(|_| anyhow::anyhow!("未知 cron zone(IANA 名)\"{z}\""))?;
        Job::new_async_tz(expr, tz, move |_uuid, _l| {
            cron_fire(
                should_run(cluster, &gate),
                run,
                &recorder,
                &node_id,
                name,
                id,
                cluster,
                timeout_ms,
            )
        })?
    } else {
        let offset = parse_fixed_offset(z).ok_or_else(|| {
            anyhow::anyhow!("非法 cron zone \"{z}\"(用 UTC / IANA 名如 \"Asia/Shanghai\" / 固定 offset 如 \"+08:00\"、\"GMT+08:00\")")
        })?;
        Job::new_async_tz(expr, offset, move |_uuid, _l| {
            cron_fire(
                should_run(cluster, &gate),
                run,
                &recorder,
                &node_id,
                name,
                id,
                cluster,
                timeout_ms,
            )
        })?
    };
    Ok(job)
}

/// 已校验、待 spawn 的 non-cron 任务(period 已确保 > 0)。
struct NonCron {
    name: &'static str,
    id: &'static str,
    run: RunFn,
    schedule: Schedule, // 仅 FixedRate / FixedDelay / OneShot
    cluster: ClusterMode,
    timeout_ms: Option<u64>,
    /// 本任务(按 group)解析好的 leader gate;Local 为 None。
    gate: Option<Arc<dyn LeaderGate>>,
}

/// 启动 start scheduled inner 流程；用于初始化后台任务或运行时。
///
/// # 参数
/// - `opts`: 已通过启动指纹检查的调度器配置。
async fn start_scheduled_inner(opts: &SchedulerOptions) -> anyhow::Result<()> {
    let recorder = opts.recorder.clone();
    let node_id: Arc<str> = Arc::from(opts.node_id.as_str());
    let fire_log = opts.fire_log.clone();
    let misfire_sweep_interval = opts.misfire_sweep_interval;
    let misfire_tolerance_ms = opts.misfire_tolerance_ms;
    // sweep_interval=0 会让 tokio::time::interval panic，且 tolerance 不能为负，因此启动前统一校验。
    anyhow::ensure!(
        !misfire_sweep_interval.is_zero(),
        "[scheduled] misfire_sweep_interval 不能为 0(tokio interval 会 panic)"
    );
    anyhow::ensure!(
        misfire_sweep_interval <= MAX_MISFIRE_SWEEP_INTERVAL,
        "[scheduled] misfire_sweep_interval 不能超过 365 天"
    );
    anyhow::ensure!(
        misfire_tolerance_ms >= 0,
        "[scheduled] misfire_tolerance_ms 不能为负(当前 {misfire_tolerance_ms})"
    );
    // 按任务 group 解析 gate：每个 cluster=leader 任务在启动校验阶段解析自己的 gate，
    // (`group_gates[group]` 或默认 gate),随任务带到 driver;无对应 gate 即 fail-fast(替代原全局"无 LeaderGate"检查)。
    let mut cron_sched: Option<JobScheduler> = None;
    // 收集 non-cron,**全部 fallible 步骤成功后**再 spawn(防半启动)。
    let mut non_cron: Vec<NonCron> = Vec::new();
    let mut fireonce_tasks: Vec<FireOnceTask> = Vec::new(); // FireOnce/ClaimOnly 自管 cron 任务;仅 FireOnce 参与 sweep 补漏
    let mut disabled = 0usize; // cron="-" 禁用计数,用于最终日志区分发现数与启用数。

    // ── 阶段 1:校验 + 建 cron job(可失败,失败时尚未 spawn 任何 non-cron)──
    for task in SCHEDULED_TASKS.iter() {
        let run = task.run; // 函数指针(Copy)
        let name = task.name;
        let cluster = task.cluster;
        let misfire = task.misfire;
        let timeout_ms = task.timeout_ms;
        let id = task.id;
        let group = task.group;
        // 按 group 解析本任务的 leader gate(Local 不门控 → None);leader 任务无对应 gate → fail-fast。
        let gate = match cluster {
            ClusterMode::Local => None,
            ClusterMode::Leader => match opts.gate_for(group) {
                Some(g) => Some(g),
                None => anyhow::bail!(
                    "[scheduled] cluster=leader 任务 {name}{} 无对应 LeaderGate;\
                     请装默认 gate(start_scheduled_clustered*/clustered*)或 with_group_gate({:?}, ...)",
                    group.map(|g| format!("(group={g})")).unwrap_or_default(),
                    group.unwrap_or("")
                ),
            },
        };
        match task.schedule {
            Schedule::FixedRate { period_ms: 0, .. } => {
                anyhow::bail!("#[scheduled] 任务 {name} 的 fixed_rate_ms/every_ms 必须 > 0");
            }
            Schedule::FixedDelay { delay_ms: 0, .. } => {
                anyhow::bail!("#[scheduled] 任务 {name} 的 fixed_delay_ms 必须 > 0");
            }
            Schedule::Cron { expr, zone } => {
                // cron = "-" 表示显式禁用,跳过注册且不创建调度器。
                if expr.trim() == "-" {
                    tracing::info!("[scheduled] cron 任务 {} 已禁用(cron=\"-\")", name);
                    disabled += 1;
                    continue;
                }
                tracing::info!(
                    "[scheduled] 注册 cron 任务 {} ({}{}, misfire={:?})",
                    name,
                    expr,
                    zone.map(|z| format!(", zone={z}")).unwrap_or_default(),
                    misfire
                );
                match misfire {
                    // FireOnce 与 ClaimOnly 都走自管 CronPlan driver(每拍 claim 去重);区别仅在是否 misfire 补漏(sweep)。
                    MisfirePolicy::FireOnce | MisfirePolicy::ClaimOnly => {
                        // 回调拿不到名义触发时刻，因此不进入 tokio-cron-scheduler，改走自管 driver。
                        // 启动期解析 CronPlan + zone(失败即启动失败,杜绝运行期 unwrap_or(now))。
                        if cluster != ClusterMode::Leader {
                            anyhow::bail!("[scheduled] 任务 {name}:misfire={misfire:?} 需配合 cluster=leader(claim 去重/补漏针对 leader 触发;Local 任务每节点各跑、无此问题)");
                        }
                        if fire_log.is_none() {
                            anyhow::bail!("[scheduled] 存在 misfire=fire_once/claim_only 任务,但未安装 FireLog;请用 SchedulerOptions::with_fire_log(...)");
                        }
                        let plan = Arc::new(CronPlan::parse(expr, zone).map_err(|e| {
                            anyhow::anyhow!("[scheduled] 任务 {name} cron 解析失败:{e}")
                        })?);
                        fireonce_tasks.push(FireOnceTask {
                            name,
                            id,
                            run,
                            cluster,
                            plan,
                            sweep: misfire == MisfirePolicy::FireOnce, // ClaimOnly 不补漏
                            timeout_ms,
                            gate,
                        });
                    }
                    MisfirePolicy::Skip => {
                        // 普通 cron:仍用 tokio-cron-scheduler(zone 门控 + 运行记录;fire_at_ms 用 now_ms)。
                        if cron_sched.is_none() {
                            cron_sched = Some(JobScheduler::new().await?);
                        }
                        let job = build_cron_job(
                            expr,
                            zone,
                            run,
                            cluster,
                            gate.clone(),
                            name,
                            id,
                            recorder.clone(),
                            node_id.clone(),
                            timeout_ms,
                        )?;
                        cron_sched.as_ref().unwrap().add(job).await?;
                    }
                }
            }
            other => {
                // cluster=leader 不支持 one-shot：它只检查一次，若当时不是 leader 将永久漏执行，因此启动期拒绝。
                if cluster == ClusterMode::Leader && matches!(other, Schedule::OneShot { .. }) {
                    anyhow::bail!(
                        "[scheduled] 任务 {name}:cluster=leader 不支持 one-shot;请改用 fixed_delay/cron,或用业务 done-marker"
                    );
                }
                // misfire 仅 cron 有意义(fixed_rate/fixed_delay/one-shot 无"应触发时刻";宏也会编译期拦)。
                if matches!(misfire, MisfirePolicy::FireOnce | MisfirePolicy::ClaimOnly) {
                    anyhow::bail!(
                        "[scheduled] 任务 {name}:misfire={misfire:?} 仅用于 cron(fixed_rate/fixed_delay/one-shot 无独立 misfire 语义)"
                    );
                }
                non_cron.push(NonCron {
                    name,
                    id,
                    run,
                    schedule: other,
                    cluster,
                    timeout_ms,
                    gate,
                });
            }
        }
    }

    // FireOnce/ClaimOnly claim 以稳定 task id 作 key；相同 id 会互相干扰，因此启动期 fail-fast。
    ensure_unique_fireonce_ids(fireonce_tasks.iter().map(|t| t.id))?;

    // ── 阶段 2:起 cron 调度器(可失败)──
    if let Some(sched) = cron_sched {
        sched.start().await?; // 启动 cron 调度器后台 tick
        *SCHED.lock().unwrap() = Some(sched); // 保命 + 供 shutdown 取出
    }

    // ── 阶段 3:全部成功后才 spawn non-cron 后台 loop(不可失败,故无半启动);句柄存起来供 shutdown ──
    let mut handles = NON_CRON_HANDLES.lock().unwrap();
    for nc in non_cron {
        let NonCron {
            name,
            id,
            run,
            schedule,
            cluster,
            timeout_ms,
            gate,
        } = nc;
        let handle = match schedule {
            Schedule::OneShot { delay_ms } => {
                // one-shot 仅 Local(Leader+one-shot 已在阶段 1 fail-fast),无需门控。
                tracing::info!(
                    "[scheduled] 注册一次性任务 {} (启动后 {}ms 跑一次)",
                    name,
                    delay_ms
                );
                let recorder = recorder.clone();
                let node_id = node_id.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    // 用 JoinSet 跑这一次(recorded_run 内部记 Started/Finished):panic 收成 JoinError 记录、不打到 driver;
                    // shutdown abort driver → JoinSet drop → 这次执行也被 abort。
                    let mut once = tokio::task::JoinSet::new();
                    once.spawn(recorded_run(
                        run,
                        recorder.clone(),
                        node_id.clone(),
                        name,
                        id,
                        cluster,
                        now_ms(),
                        timeout_ms,
                    ));
                    if let Some(res) = once.join_next().await {
                        log_join(res, name);
                    }
                })
            }
            Schedule::FixedRate {
                period_ms,
                initial_delay_ms,
                max_in_flight,
            } => {
                tracing::info!(
                    "[scheduled] 注册固定频率任务 {} (每 {}ms,首跑 {}{})",
                    name,
                    period_ms,
                    initial_delay_ms
                        .map(|d| format!("{d}ms 后"))
                        .unwrap_or_else(|| "一个周期后".into()),
                    max_in_flight
                        .map(|m| format!(",最多 {m} 并发"))
                        .unwrap_or_default()
                );
                let gate = gate.clone();
                let recorder = recorder.clone();
                let node_id = node_id.clone();
                tokio::spawn(async move {
                    let period = Duration::from_millis(period_ms);
                    // 首跑:initial 给了在 initial 后,否则在一个 period 后(= 旧 every_ms 语义)。
                    let first = Duration::from_millis(initial_delay_ms.unwrap_or(period_ms));
                    let mut ticker =
                        tokio::time::interval_at(tokio::time::Instant::now() + first, period);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    // fixedRate 须按开始时间触发:每拍把 run 独立 spawn,**不被上一次 run().await 阻塞**(慢任务不拖慢周期)。
                    // in-flight 子任务进 JoinSet:① driver 被 abort(shutdown)时 JoinSet 随 future drop → 子任务一并 abort;
                    // ② select 的 join_next 分支让子任务一完成即回收,防 JoinSet 只增不减。
                    // 过载保护:max_in_flight=Some(n) 时,本拍若在跑数已达 n 就**跳过**(skip_if_running=Some(1));None=不限(允许重叠)。
                    // ticker 使用 MissedTickBehavior::Skip:运行时短暂卡顿后只恢复后续节拍,不把积压 tick 连续打爆。
                    let mut inflight = tokio::task::JoinSet::new();
                    loop {
                        tokio::select! {
                            _ = ticker.tick() => {
                                // 先非阻塞回收已完成的(并记录 panic),拿到准确在飞计数(JoinSet::len 含未回收的已完成项)。
                                while let Some(res) = inflight.try_join_next() { log_join(res, name); }
                                // leader gate 必须先于限流判断，非 leader 跳过本拍时不占 in-flight，也不污染限流指标。
                                if !should_run(cluster, &gate) {
                                    record_skip(recorder.as_ref(), name, id, cluster, node_id.as_ref(), now_ms(), SkipReason::NotLeader);
                                    continue;
                                }
                                match max_in_flight {
                                    Some(max) if inflight.len() as u64 >= max => {
                                        tracing::warn!(
                                            "[scheduled] {} 在跑 {} 个已达上限 {},跳过本拍",
                                            name, inflight.len(), max
                                        );
                                        record_skip(recorder.as_ref(), name, id, cluster, node_id.as_ref(), now_ms(), SkipReason::InflightLimit);
                                    }
                                    _ => { inflight.spawn(recorded_run(run, recorder.clone(), node_id.clone(), name, id, cluster, now_ms(), timeout_ms)); }
                                }
                            }
                            // 任一子任务完成即回收 + 记录 panic(子任务 panic 不影响 driver 继续调度)。
                            Some(res) = inflight.join_next() => { log_join(res, name); }
                        }
                    }
                })
            }
            Schedule::FixedDelay {
                delay_ms,
                initial_delay_ms,
            } => {
                tracing::info!(
                    "[scheduled] 注册固定延迟任务 {} (完成后 {}ms 再跑,首跑 {})",
                    name,
                    delay_ms,
                    initial_delay_ms
                        .map(|d| format!("{d}ms 后"))
                        .unwrap_or_else(|| "立即".into())
                );
                let gate = gate.clone();
                let recorder = recorder.clone();
                let node_id = node_id.clone();
                tokio::spawn(async move {
                    if let Some(d) = initial_delay_ms {
                        tokio::time::sleep(Duration::from_millis(d)).await;
                    }
                    let delay = Duration::from_millis(delay_ms);
                    // 每轮把本次执行 spawn 进 JoinSet 再 await 它:既保持"跑完再 delay、不重叠"的 fixedDelay 语义,
                    // 又把 panic 收成 JoinError 记录(带任务名)而**不打掉 driver**(否则一次 panic 后该任务永久停摆——
                    // 直接 run().await 会让本 driver task 随之 panic);shutdown abort driver → JoinSet drop → 在跑的一次也被 abort。
                    let mut set = tokio::task::JoinSet::new();
                    loop {
                        // 非 leader:不执行,仅按 delay 周期重检(晋升 leader 后最长一个 delay 内开跑)。
                        if should_run(cluster, &gate) {
                            set.spawn(recorded_run(
                                run,
                                recorder.clone(),
                                node_id.clone(),
                                name,
                                id,
                                cluster,
                                now_ms(),
                                timeout_ms,
                            ));
                            if let Some(res) = set.join_next().await {
                                log_join(res, name);
                            }
                        } else {
                            record_skip(
                                recorder.as_ref(),
                                name,
                                id,
                                cluster,
                                node_id.as_ref(),
                                now_ms(),
                                SkipReason::NotLeader,
                            );
                        }
                        tokio::time::sleep(delay).await;
                    }
                })
            }
            Schedule::Cron { .. } => unreachable!("cron 已在阶段 1 处理"),
        };
        handles.push(handle);
    }

    // FireOnce/ClaimOnly cron 为每个任务启动一个自管 driver（正常触发并给出精确名义 scheduled_at），仅 FireOnce 再挂一个共享
    // 【misfire 巡检】(补无主窗口漏触发)。两者共用同一 CronPlan。fire_log/gate 已在阶段 1 校验存在;
    // 全部纳入 NON_CRON_HANDLES → shutdown 一并 abort。
    if !fireonce_tasks.is_empty() {
        let fire_log = fire_log
            .clone()
            .expect("FireOnce/ClaimOnly 任务已在阶段 1 校验 fire_log 存在");
        tracing::info!(
            "[scheduled] 启动 {} 个 FireOnce/ClaimOnly cron driver(+ 仅 FireOnce 的 misfire 巡检)",
            fireonce_tasks.len()
        );
        // 每个 FireOnce/ClaimOnly 任务一个自管 driver(正常触发)。
        for t in &fireonce_tasks {
            let h = tokio::spawn(fire_once_cron_driver(
                t.plan.clone(),
                t.run,
                recorder.clone(),
                node_id.clone(),
                t.name,
                t.id,
                t.cluster,
                t.gate.clone(),
                fire_log.clone(),
                t.timeout_ms,
            ));
            handles.push(h);
        }
        // 共享巡检 loop(补漏)—— 仅 FireOnce(sweep=true);ClaimOnly 只每拍去重、不补漏。
        let sweep_tasks: Vec<FireOnceTask> =
            fireonce_tasks.into_iter().filter(|t| t.sweep).collect();
        if !sweep_tasks.is_empty() {
            let h = tokio::spawn(misfire_sweep_loop(
                sweep_tasks,
                fire_log,
                recorder.clone(),
                node_id.clone(),
                misfire_sweep_interval,
                misfire_tolerance_ms,
            ));
            handles.push(h);
        }
    }
    drop(handles);

    let total = SCHEDULED_TASKS.len();
    tracing::info!(
        "[scheduled] 共发现 {} 个 #[scheduled],启用 {},禁用 {}(cron=\"-\")",
        total,
        total - disabled,
        disabled
    );
    Ok(())
}

/// 【优雅停机】停掉所有 #[scheduled] 任务:abort non-cron 后台 loop + shutdown cron 调度器,并复位启动状态
/// (之后可再 `start_scheduled` 重启)。**纯 Rust(无 DI)**:句柄/调度器存在进程 static,直接取出 abort/shutdown。
///
pub async fn shutdown_scheduled() -> anyhow::Result<()> {
    // 持启动锁:与 start_scheduled 串行,停机期间不会有并发启动穿插。
    let mut state = START_STATE.lock().await;
    // 1. abort 所有 non-cron 后台 loop
    let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *NON_CRON_HANDLES.lock().unwrap());
    for h in handles {
        h.abort();
    }
    // 2. shutdown cron 调度器(取出 owned 再 await,不跨标准锁 await)
    let sched = SCHED.lock().unwrap().take();
    if let Some(mut s) = sched {
        let _ = s.shutdown().await;
    }
    // 3. 复位指纹:允许后续用另一套配置重新启动
    *state = None;
    tracing::info!("[scheduled] 已停机(non-cron loop 已 abort,cron 调度器已 shutdown)");
    Ok(())
}

// ── re-export 过程宏:业务项目 `use scheduling::{Async, scheduled, EnableScheduling};` ──
// `EnableScheduling` 是清晰命名(启动 #[scheduled] 调度);`EnableAsync` 保留为兼容别名。
pub use async_macro::{scheduled, Async, EnableAsync, EnableScheduling};

/// 宏展开专用的第三方依赖桥:`#[Async]/#[scheduled]/#[EnableAsync]`
/// 生成代码经此引用 tokio/linkme/tracing。**不属于稳定业务 API**。
#[doc(hidden)]
pub mod __private {
    pub use linkme;
    pub use tokio;
    pub use tracing;
}

// ============================================================================
// cluster feature 提供 nadis::Leader adapter 与便捷启动函数。
// 关 feature 时整段不编译,scheduling core 不依赖 nadis / Redis。
// ============================================================================
#[cfg(feature = "cluster")]
mod cluster_adapter {
    use super::{
        start_scheduled_with, ExecutionRecorder, FireLog, LeaderGate, RunEvent, RunPhase,
        SchedulerOptions,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    /// 把 `nadis::Leader` 适配成 `LeaderGate`(只读 `is_leader()`,零 Redis 往返)。
    pub struct NadisLeaderGate {
        leader: Arc<nadis::Leader>,
    }

    impl NadisLeaderGate {
        /// 业务要"nadis gate + 自定义 options" 组合时:
        /// `SchedulerOptions::clustered_with_id(id, Arc::new(NadisLeaderGate::new(leader)))`。
        ///
        /// # 参数
        /// - `leader`: 已初始化的 nadis leader 对象,本适配器只读取它的本地 leader 状态。
        pub fn new(leader: Arc<nadis::Leader>) -> Self {
            Self { leader }
        }
    }

    impl LeaderGate for NadisLeaderGate {
        /// 返回绑定的 Redis leader 当前是否仍由本进程持有。
        fn is_leader(&self) -> bool {
            self.leader.is_leader()
        }
    }

    /// 把 misfire claim 存入 Redis 的 [`FireLog`] 实现，供 `misfire="fire_once"`/`"claim_only"` 使用。
    ///
    /// 存储 = **单个 HASH**:`field = 稳定 task id`(= [`crate::ScheduledTask::id`],非展示 name),`value = 已认领的名义 scheduled_at`(epoch ms)。
    /// [`try_claim_fire`](FireLog::try_claim_fire) 用一段 Lua **原子**完成「HGET→比较→HSET」:仅当字段为空
    /// 或 `current < stale_before` 时写入 `scheduled_at` 并返回 `true`(与 trait 契约/内存版 `FakeFireLog` 同义)。
    /// 正常触发(`stale = scheduled_at`)去重同一拍双主;misfire 巡检(`stale = scheduled_at - tol`)补漏一拍。
    ///
    /// **不会无界增长**:HASH 字段数 = `fire_once`/`claim_only` 任务数(linkme 静态、有界),值原地覆盖,故**无需 TTL/清理**。
    /// `key` 由业务给(**建议含 app/namespace**,避免多服务任务名碰撞);跨进程共用同一 `key` 才能共享 claim。
    ///
    /// ```ignore
    /// let fire_log = Arc::new(NadisFireLog::new(client.clone(), "myapp:scheduling:firelog"));
    /// let opts = SchedulerOptions::clustered_with_id("nadis", gate).with_fire_log(fire_log);
    /// start_scheduled_with(opts).await?;
    /// ```
    pub struct NadisFireLog {
        client: Arc<nadis::RedisClient>,
        key: String,
    }

    // 原子 claim:KEYS[1]=HASH key;ARGV[1]=task id(field;调用方传稳定 id)、ARGV[2]=scheduled_at_ms、ARGV[3]=stale_before_ms。
    // 语义对齐 trait 契约 + 内存版 FakeFireLog:空 或 current<stale_before → 写 scheduled_at、返 1;否则返 0。
    const CLAIM_LUA: &str = r"
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if (not cur) or (tonumber(cur) < tonumber(ARGV[3])) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
";

    impl NadisFireLog {
        /// `key` = 存 claim 的 HASH key(建议含 app/namespace,如 `"myapp:scheduling:firelog"`)。
        ///
        /// # 参数
        /// - `client`: 已初始化的 Redis 客户端,用于执行 Lua claim 和读取 HASH。
        /// - `key`: 存放 misfire claim 的 Redis HASH key。
        pub fn new(client: Arc<nadis::RedisClient>, key: impl Into<String>) -> Self {
            Self {
                client,
                key: key.into(),
            }
        }
    }

    impl FireLog for NadisFireLog {
        /// 返回启动指纹。
        ///
        /// 指纹使用 Redis HASH key；同进程二次启动如果换了 key,调度器能通过 `StartFingerprint` 识别状态漂移。
        fn fingerprint(&self) -> Option<String> {
            Some(self.key.clone())
        }

        /// 读取最近一次 fire；用于续跑和幂等判断。
        ///
        /// # 参数
        /// - `task`: 调度任务或业务任务标识。
        fn last_fire<'a>(
            &'a self,
            task: &'a str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<i64>>> + Send + 'a>> {
            Box::pin(async move {
                self.client
                    .h_get::<i64>(&self.key, task)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "nadis FireLog last_fire 失败(key={}, task={task}): {e}",
                            self.key
                        )
                    })
            })
        }

        /// 尝试执行 claim fire；用于在失败时返回可处理错误。
        ///
        /// # 参数
        /// - `task`: 调度任务或业务任务标识。
        /// - `scheduled_at_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
        /// - `stale_before_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
        fn try_claim_fire<'a>(
            &'a self,
            task: &'a str,
            scheduled_at_ms: i64,
            stale_before_ms: i64,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + 'a>> {
            Box::pin(async move {
                let sched = scheduled_at_ms.to_string();
                let stale = stale_before_ms.to_string();
                let claimed: i64 = self
                    .client
                    .eval(
                        CLAIM_LUA,
                        &[self.key.as_str()],
                        &[task, sched.as_str(), stale.as_str()],
                    )
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "nadis FireLog try_claim_fire 失败(key={}, task={task}): {e}",
                            self.key
                        )
                    })?;
                Ok(claimed != 0)
            })
        }
    }

    /// 把运行记录（`RunEvent`）写入 Redis Stream 的 [`ExecutionRecorder`] 实现。
    ///
    /// **非阻塞**:`record()` 只把事件字段 `try_send` 进**有界** channel(同步、不打 Redis);后台 task 异步 `XADD`
    /// 到 stream(`id="*"`),每 `trim_every` 条做一次 `XTRIM MAXLEN ~ maxlen` 控制保留量。channel 接收端随
    /// recorder(tx)drop 关闭 → 后台 task 自然退出(无需显式 abort)。
    ///
    /// **背压**：channel 满（Redis 跟不上或 skipped 过密）时，`try_send` 丢弃本条运行记录以保持内存有界，
    /// 绝不阻塞任务),累计丢弃数由 [`dropped()`](Self::dropped) 暴露 + 限频 `warn`;容量经 [`with_capacity`](Self::with_capacity) 可配。
    ///
    /// 字段:`task`(展示名)、`id`(稳定 task id)、`node`、`fire_at`(名义 scheduled_at,关联键)、`phase`(started/finished/skipped),
    /// 以及 finished 的 `status`/`elapsed_ms`、skipped 的 `reason`。**必须在 tokio runtime 内构造**(要 spawn 后台 task)。
    ///
    /// ```ignore
    /// let rec = Arc::new(NadisExecutionRecorder::new(client.clone(), "myapp:scheduling:runlog"));
    /// let opts = SchedulerOptions::clustered_with_id("nadis", gate).with_recorder(rec);
    /// ```
    pub struct NadisExecutionRecorder {
        tx: tokio::sync::mpsc::Sender<Vec<(&'static str, String)>>,
        /// channel 满时累计丢弃的运行记录数(可观测)。
        dropped: std::sync::atomic::AtomicU64,
        /// 优雅停机信号:通知后台 drain 把已缓冲的写完再退(见 [`shutdown`](Self::shutdown))。
        shutdown: std::sync::Arc<tokio::sync::Notify>,
        /// 后台 drain task 句柄(供 `shutdown` await);take 后 = 已停机。
        drain: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    }

    /// 默认 stream 保留条数(`XTRIM MAXLEN ~`)。
    const DEFAULT_RUNLOG_MAXLEN: u64 = 10_000;
    /// 默认有界 channel 容量(可经 [`NadisExecutionRecorder::with_capacity`] 覆盖)。
    const DEFAULT_RECORDER_CHANNEL_CAP: usize = 8192;

    impl NadisExecutionRecorder {
        /// `key` = 运行记录 Stream 的 key(建议含 app/namespace)。默认保留 ~1 万条、channel 容量 8192。
        ///
        /// # 参数
        /// - `client`: 已初始化的 Redis 客户端,后台任务通过它写入 Stream。
        /// - `key`: 运行记录 Stream key。
        pub fn new(client: Arc<nadis::RedisClient>, key: impl Into<String>) -> Self {
            Self::with_maxlen(client, key, DEFAULT_RUNLOG_MAXLEN)
        }

        /// 同 [`new`](Self::new),但指定 stream 保留上限(`XTRIM MAXLEN ~ maxlen`)。
        ///
        /// # 参数
        /// - `client`: 已初始化的 Redis 客户端,后台任务通过它写入 Stream。
        /// - `key`: 运行记录 Stream key。
        /// - `maxlen`: Stream 近似保留条数上限,最小会兜底为 1。
        pub fn with_maxlen(
            client: Arc<nadis::RedisClient>,
            key: impl Into<String>,
            maxlen: u64,
        ) -> Self {
            Self::with_capacity(client, key, maxlen, DEFAULT_RECORDER_CHANNEL_CAP)
        }

        /// 同 [`with_maxlen`](Self::with_maxlen)，并指定有界 channel 容量 `capacity`；满时丢弃以限制背压。
        ///
        /// # 参数
        /// - `client`: 已初始化的 Redis 客户端,后台任务通过它写入 Stream。
        /// - `key`: 运行记录 Stream key。
        /// - `maxlen`: Stream 近似保留条数上限,最小会兜底为 1。
        /// - `capacity`: 本地有界 channel 容量,满时 `record` 丢弃新运行记录。
        pub fn with_capacity(
            client: Arc<nadis::RedisClient>,
            key: impl Into<String>,
            maxlen: u64,
            capacity: usize,
        ) -> Self {
            let maxlen = maxlen.max(1); // maxlen=0 会让 XTRIM 删空，因此下限固定为 1。
            let capacity = capacity.max(1); // 容量 ≥1。
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<(&'static str, String)>>(capacity);
            let key = key.into();
            let trim_every = (maxlen / 10).max(1);
            // 后台落库 task:① rx 随 recorder(tx)drop 关闭 → recv 返回 None → 退出(自清理);
            // ② shutdown 信号 → 把已缓冲的 try_recv 干完再退，保证优雅停机不丢运行记录。
            let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
            let shutdown_drain = shutdown.clone();
            let handle = tokio::spawn(async move {
                let mut since_trim = 0u64;
                let mut shutting = false;
                loop {
                    let fields = if shutting {
                        // shutdown 后只排空已经进入 channel 的事件,不再阻塞等待新事件。
                        match rx.try_recv() {
                            Ok(f) => f,
                            Err(_) => break, // 缓冲已清空 → 退出
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = shutdown_drain.notified() => {
                                shutting = true;
                                continue; // 转入收尾 drain(把已缓冲写完)
                            }
                            ev = rx.recv() => match ev {
                                Some(f) => f,
                                None => break, // 所有 tx drop(recorder 已 drop)
                            },
                        }
                    };
                    let f: Vec<(&str, &str)> =
                        fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
                    match client.x_add(&key, "*", &f).await {
                        Ok(_) => {
                            since_trim += 1;
                            if since_trim >= trim_every {
                                since_trim = 0;
                                if let Err(e) = client.x_trim_maxlen_approx(&key, maxlen).await {
                                    tracing::warn!(
                                        "[scheduled] NadisExecutionRecorder XTRIM {key} 失败:{e}"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("[scheduled] NadisExecutionRecorder XADD {key} 失败:{e}")
                        }
                    }
                }
            });
            Self {
                tx,
                dropped: std::sync::atomic::AtomicU64::new(0),
                shutdown,
                drain: std::sync::Mutex::new(Some(handle)),
            }
        }

        /// channel 满时累计丢弃的运行记录数(可观测;0 = 没丢过)。
        pub fn dropped(&self) -> u64 {
            self.dropped.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// 优雅停机：通知后台 drain 把**已缓冲**的运行记录写完，再 await 它退出。
        /// 用于优雅停机前确保尾部记录落库(默认 drop 路径是 best-effort,进程立即退可能丢尾部)。
        /// 幂等:重复调用 / drop 后调用安全(handle 已 take 则直接返回);停机后再 `record` 的事件会被丢弃。
        pub async fn shutdown(&self) {
            // notify 只负责把后台 task 从 recv/select 唤醒;drain handle 用 Mutex<Option<_>> 保证只有首个调用者等待真实退出。
            self.shutdown.notify_one();
            let handle = self.drain.lock().expect("recorder drain lock").take();
            if let Some(h) = handle {
                let _ = h.await;
            }
        }
    }

    impl ExecutionRecorder for NadisExecutionRecorder {
        /// 记录 record 事件；用于保存运行过程中的观测数据。
        ///
        /// # 参数
        /// - `e`: 错误对象或外部错误值。
        fn record(&self, e: &RunEvent<'_>) {
            let mut fields: Vec<(&'static str, String)> = vec![
                ("task", e.task.to_owned()), // 展示名
                ("id", e.id.to_owned()),     // 稳定 task id(关联 claim / 区分 repeatable)
                ("node", e.node.to_owned()),
                ("fire_at", e.fire_at_ms.to_string()),
            ];
            match &e.phase {
                RunPhase::Started => fields.push(("phase", "started".to_owned())),
                RunPhase::Finished { status, elapsed_ms } => {
                    fields.push(("phase", "finished".to_owned()));
                    fields.push(("status", format!("{status:?}")));
                    fields.push(("elapsed_ms", elapsed_ms.to_string()));
                }
                RunPhase::Skipped { reason } => {
                    fields.push(("phase", "skipped".to_owned()));
                    fields.push(("reason", format!("{reason:?}")));
                }
            }
            // 非阻塞:try_send —— channel 满(Redis 跟不上 / 高频 skipped)或接收端没了 → 丢弃本条运行记录,
            // 绝不阻塞 / panic 任务(运行记录是观测面、可丢;内存有界优先)。丢弃计数 + 限频 warn(首条 / 每翻倍)暴露。
            // shutdown/drain 完成后接收端已退出,这里也会走丢弃计数,保持停机后的 record 行为可观测。
            if self.tx.try_send(fields).is_err() {
                let n = self
                    .dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                if n == 1 || n.is_power_of_two() {
                    tracing::warn!(
                        "[scheduled] NadisExecutionRecorder 运行记录已丢弃 {n} 条(channel 满:Redis 跟不上 / 高频 skipped)"
                    );
                }
            }
        }
    }

    /// 集群模式启动:默认 `gate_id = "nadis"`。仅"一个进程只有一套 leader gate"的简单场景用;
    /// 多 gate / 想要可辨识的启动指纹时用 [`start_scheduled_clustered_with_id`]。
    ///
    /// # 参数
    /// - `leader`: 已初始化的 nadis leader 对象,用于构造默认集群 leader gate。
    pub async fn start_scheduled_clustered(leader: Arc<nadis::Leader>) -> anyhow::Result<()> {
        start_scheduled_clustered_with_id("nadis", leader).await
    }

    /// 集群模式启动并设置稳定 `gate_id`，建议使用 leader lock key 以参与启动指纹校验。
    ///
    /// # 参数
    /// - `gate_id`: nadis leader gate 的稳定业务标识,会进入启动指纹。
    /// - `leader`: 已初始化的 nadis leader 对象,用于构造集群 leader gate。
    pub async fn start_scheduled_clustered_with_id(
        gate_id: impl Into<String>,
        leader: Arc<nadis::Leader>,
    ) -> anyhow::Result<()> {
        let gate: Arc<dyn LeaderGate> = Arc::new(NadisLeaderGate::new(leader));
        start_scheduled_with(SchedulerOptions::clustered_with_id(gate_id, gate)).await
    }
}

#[cfg(feature = "cluster")]
pub use cluster_adapter::{
    start_scheduled_clustered, start_scheduled_clustered_with_id, NadisExecutionRecorder,
    NadisFireLog, NadisLeaderGate,
};
