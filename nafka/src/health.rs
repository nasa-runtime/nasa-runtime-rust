//! 健康与就绪模型:group 状态、分区暂停原因、broker ready 契约。

use std::time::SystemTime;

use crate::consumer::InvalidRecordReason;
use crate::types::Tp;

/// group 生命周期/健康状态;Running 只表示 poll 循环正常,不等价于 ReadyRequirement 已满足。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupState {
    /// owner 线程已建、consumer 本地构造完成,尚未完成首次 join。
    Starting,
    /// 正在 join/rebalance,尚无有效 assignment。
    Joining,
    /// 正常拉取与派发。
    Running,
    /// commit 失败挂起:暂停派发、保留 snapshot、按退避重试提交。
    CommitBlocked,
    /// 有失败分区处于回退重投窗口。
    Retrying,
    /// DLT permit 不足,group 级派发门禁。
    DltBackpressure,
    /// progress outcome permit 不足,暂停派发等待 commit 裁剪释放。
    ProgressBackpressure,
    /// 有分区被 Halt/DLT 长期失败等需人工介入的降级状态,其余分区可能仍在工作。
    Degraded,
    /// 收到停止指令,正在收尾。
    Stopping,
    /// 已停止(显式 stop/shutdown)。
    Stopped,
    /// 不可恢复错误(fatal/预算耗尽/框架不变量破坏);仅显式 restart_group 或应用重启可离开。
    Crashed,
}

impl GroupState {
    /// 业务作用：判断当前 group 是否允许 operator 显式重建 consumer 会话。
    ///
    /// 允许集合 = 「需要人工介入且没有自动出路」的全部状态：
    /// `Crashed` 由 owner 外层恢复邮箱处理；`Degraded` 由运行中的 owner 在批次边界处理；
    /// `CommitBlocked` / `DltBackpressure` / `ProgressBackpressure` 是外部条件不恢复就永远停在
    /// 那里的门禁态，且此时 seek 会被 in-flight DLT 规则拒绝、resume 会被框架暂停原因过滤，
    /// 若再拒绝 restart 就等于「没有任何控制 API 能救回这个 group」，只剩重启进程一条路。
    ///
    /// 仍然拒绝的是 `Starting`/`Joining`/`Running`/`Retrying`（正常推进中，restart 只会
    /// 丢失进度）与 `Stopping`/`Stopped`（已在停机流程里，恢复必须创建新运行时）。
    pub(crate) fn permits_operator_restart(self) -> bool {
        matches!(
            self,
            Self::Degraded
                | Self::Crashed
                | Self::CommitBlocked
                | Self::DltBackpressure
                | Self::ProgressBackpressure
        )
    }

    /// 业务作用：返回允许 restart 的状态名列表，用于生成可定位的拒绝错误文本。
    pub(crate) fn operator_restart_states() -> &'static str {
        "Degraded/Crashed/CommitBlocked/DltBackpressure/ProgressBackpressure"
    }
}

/// 分区暂停原因;resume 只能移除 User,框架安全暂停与 Halt 不受 resume 影响。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PauseReason {
    /// 业务显式 pause。
    User,
    /// 失败重投退避窗口。
    RetryBackoff,
    /// 本分区有 DLT job 在途,收尾前不再派发。
    DltPending,
    /// group 级 DLT 反压(permit 不足)。
    DltBackpressure,
    /// group 级进度 outcome permit 不足;只能由安全水位裁剪自动释放。
    ProgressBackpressure,
    /// commit 失败挂起。
    CommitBlocked,
    /// 需人工介入的硬暂停;仅 operator seek/restart(清理 attempt + 审计)可恢复。
    Halt(HaltReason),
}

/// Halt 的具体原因:区分它们是为了运维一眼定位"该修什么"。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HaltReason {
    /// 单条记录超过 max_record_bytes,拒绝复制进内存。
    OversizeRecord,
    /// unmatched_policy=Halt 命中无主消息。
    UnmatchedRoute,
    /// invalid_record_policy=Halt 命中确定无效记录，并保留强类型根因。
    InvalidRecord(InvalidRecordReason),
    /// fan-out 展开超上限且策略为 Halt。
    RouteExpansionTooLarge,
    /// 编码后帧超过目标上限(passthrough 数据面)。
    FrameTooLarge,
    /// progress 状态机走入"满而无法推进"的框架不变量破坏。
    ProgressInvariant,
    /// 已带 DLT 来源 header 的记录再次要求 DLT:禁止递归 DLT 链。
    RecursiveDlt,
    /// 消费重试耗尽但配置显式关闭了 DLT，必须人工决定如何处置原记录。
    DltDisabled,
    /// DLT dispatcher、准入或 worker 基础设施异常，无法安全处置失败单元。
    DltInfrastructure,
    /// topic 契约漂移(分区数变化等)。
    TopicContract,
}

/// group 健康快照,经 `KafkaProxy::group_health` 查询;字段都是观测值,不构成控制面。
#[derive(Clone, Debug)]
pub struct GroupHealth {
    /// group.id。
    pub group: String,
    /// 当前状态。
    pub state: GroupState,
    /// 当前 assignment 分区集合。
    pub assignment: Vec<Tp>,
    /// 与 [`GroupReadySnapshot::assignment_epoch`] 同源的内部 epoch;None = 尚未完成首次 Assign。
    pub ready_assignment_epoch: Option<u64>,
    /// 最近一次 assignment 变更时间。
    pub last_assignment_at: Option<SystemTime>,
    /// 处于暂停的分区及原因。
    pub paused: Vec<(Tp, PauseReason)>,
    /// 最近一次 poll 返回时间(心跳观测)。
    pub last_poll_at: Option<SystemTime>,
    /// 最近一次业务处理成功时间。
    pub last_success_at: Option<SystemTime>,
    /// owner 创建底层 consumer 会话的累计次数，包含首次会话与失败后重建尝试。
    pub owner_session_started: u64,
    /// 首次会话之后的自动或显式重建尝试次数。
    pub owner_session_restarted: u64,
    /// owner 实际发起的同步 commit 次数，包含失败重试与停机收尾。
    pub commit_attempted: u64,
    /// broker 已确认成功的 commit 次数。
    pub commit_succeeded: u64,
    /// broker 拒绝、超时或传输失败的 commit 次数。
    pub commit_failed: u64,
    /// 最近一次错误的分类文本(已脱敏)。
    pub last_error: Option<String>,
}

/// broker 就绪要求:start() 只保证本地构造,需要"确认 Kafka 可用再开流量"的
/// 上层用它显式等待,避免把 metadata 请求成功冒充就绪。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadyRequirement {
    /// 已观察到一次成功 join/post-rebalance;允许空 assignment(竞争组 standby 合法形态)。
    Joined,
    /// 已 join 且 assignment 至少包含 min_partitions 个分区。
    Assigned {
        /// 最少分区数,>0。
        min_partitions: usize,
    },
    /// assignment 必须覆盖列出的每个 topic 至少一个分区;socket 数据/控制面用它做启动门禁。
    AssignedTopics(Vec<String>),
}

/// 就绪快照:仅表示"该时刻已满足要求",rebalance 会使其降级,不是永久租约。
#[derive(Clone, Debug)]
pub struct GroupReadySnapshot {
    /// group.id。
    pub group: String,
    /// nafka 内部 assignment epoch(owner 每次 Assign/Revoke 递增),不是 broker 的 group
    /// generation id——那是 i32 且随协议演进变化,公共 API 不暴露底层 generation。
    pub assignment_epoch: u64,
    /// 满足要求时的 assignment 快照。
    pub assignment: Vec<Tp>,
    /// 快照产生时间。
    pub observed_at: SystemTime,
}
