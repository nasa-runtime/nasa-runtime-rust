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
    /// 旧会话失权，正在等待实际调用退出或自动重建退避。
    Recovering,
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
    /// 参数说明：无。
    /// 返回：保护与自动重建等待允许申请恢复，正常推进及停机状态拒绝；实际重建仍须等待旧责任退出。
    pub(crate) fn permits_operator_restart(self) -> bool {
        matches!(
            self,
            Self::Degraded
                | Self::Recovering
                | Self::Crashed
                | Self::CommitBlocked
                | Self::DltBackpressure
                | Self::ProgressBackpressure
        )
    }

    /// 业务作用：返回允许 restart 的状态名列表，用于生成可定位的拒绝错误文本。
    pub(crate) fn operator_restart_states() -> &'static str {
        "Degraded/Recovering/Crashed/CommitBlocked/DltBackpressure/ProgressBackpressure"
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
    /// 当前分区 DLT 已达到持续失败阈值；保留交付保护并继续退避，其它分区成功不能清除。
    DltRetryDegraded,
    /// group 级 DLT 反压(permit 不足)。
    DltBackpressure,
    /// group 级进度 outcome permit 不足;只能由安全水位裁剪自动释放。
    ProgressBackpressure,
    /// 组共享输入额度不足；仅暂停触发分区，已登记的重试可以复用原额度。
    InputBackpressure,
    /// 当前有界业务调用窗口尚未退出；保持 poll 协调但不继续取入新记录。
    HandlerInFlight,
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

/// 单槽或全组健康快照；group_health 汇总本实例全部槽，group_slots_health 返回各槽。
/// 字段都是观测值，不构成控制租约。
#[derive(Clone, Debug)]
pub struct GroupHealth {
    /// group.id。
    pub group: String,
    /// 当前状态。
    pub state: GroupState,
    /// 当前 assignment 分区集合。
    pub assignment: Vec<Tp>,
    /// 单槽 assignment epoch 或全组槽向量的观测编号；None 表示尚未就绪或已失权，两种编号不可互比。
    pub ready_assignment_epoch: Option<u64>,
    /// 最近一次 assignment 变更时间。
    pub last_assignment_at: Option<SystemTime>,
    /// 处于暂停的分区及原因。
    pub paused: Vec<(Tp, PauseReason)>,
    /// 最近一次 poll 返回时间；全组取最慢槽，任一槽尚未 poll 则为 None。
    pub last_poll_at: Option<SystemTime>,
    /// 最近一次业务处理成功时间。
    pub last_success_at: Option<SystemTime>,
    /// 单槽或全组创建底层 consumer 会话的累计次数，包含各槽首次与失败后重建尝试。
    pub owner_session_started: u64,
    /// 首次会话之后的自动或显式重建尝试次数。
    pub owner_session_restarted: u64,
    /// owner 实际发起的同步 commit 次数，包含失败重试与停机收尾。
    pub commit_attempted: u64,
    /// broker 已确认成功的 commit 次数。
    pub commit_succeeded: u64,
    /// broker 拒绝、超时或传输失败的 commit 次数。
    pub commit_failed: u64,
    /// typed 模式中尚未实际退出的解码或 handler 调用数；批调用计为一个，逻辑超时不提前归零。
    pub active_handlers: usize,
    /// 仍被输入批、实际执行、重试或待确认提交持有的原始记录数，同一责任只计一次。
    pub retained_records: usize,
    /// 上述记录的原文及框架副本预留字节，按原始 key、payload 和有序头字节和的八倍计费；不含 native 预取和解码对象堆内存。
    pub retained_bytes: usize,
    /// 按键模式中尚未实际退出的 handler 数；逻辑超时不提前归零。
    pub key_active_handlers: usize,
    /// 已完成但尚未进入连续安全前缀的按键结果数，不包含正在执行的调用。
    pub key_pending_records: usize,
    /// 按键完成证据按原消息和诊断余量保守计费的字节数，不含 native 缓冲。
    pub key_pending_bytes: usize,
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
    /// 当前 assignment 必须覆盖每个明确的 topic/partition 坐标。
    AssignedPartitions(Vec<Tp>),
}

impl ReadyRequirement {
    /// 业务作用：统一启动门禁与受管监控对当前消费权威和分配条件的判断。
    /// 参数说明：health 为单槽或聚合健康快照，不是跨线程持有的永久租约。
    /// 返回：仅 Running、有效分配代次且满足所选范围时为 true；重试、保护、失权及停机均拒绝开放。
    pub fn satisfied_by(&self, health: &GroupHealth) -> bool {
        self.reason(health) == ReadyReason::Ready
    }

    /// 业务作用：返回接流量条件未满足的首要稳定原因，不以错误文本承担业务判断。
    /// 参数说明：health 为当前单槽或聚合快照。
    /// 返回：非法要求优先拒绝；状态与失权优先于分配缺口，用户暂停不撤销分配就绪。
    pub fn reason(&self, health: &GroupHealth) -> ReadyReason {
        if self.validate().is_err() {
            return ReadyReason::InvalidRequirement;
        }
        // 保留 assignment 只是诊断证据；保护或重试期间不能据此重新开放流量。
        let reason = match health.state {
            GroupState::Starting => ReadyReason::Starting,
            GroupState::Joining => ReadyReason::Joining,
            GroupState::Running => ReadyReason::Ready,
            GroupState::Recovering => ReadyReason::Recovering,
            GroupState::Retrying => ReadyReason::Retrying,
            GroupState::CommitBlocked => ReadyReason::CommitBlocked,
            GroupState::DltBackpressure => ReadyReason::DltBackpressure,
            GroupState::ProgressBackpressure => ReadyReason::ProgressBackpressure,
            GroupState::Degraded => ReadyReason::Degraded,
            GroupState::Crashed => ReadyReason::Crashed,
            GroupState::Stopping => ReadyReason::Stopping,
            GroupState::Stopped => ReadyReason::Stopped,
        };
        if reason != ReadyReason::Ready {
            return reason;
        }
        if health.ready_assignment_epoch.is_none() {
            return ReadyReason::NotJoined;
        }
        match self {
            Self::Assigned { min_partitions } if health.assignment.len() < *min_partitions => {
                ReadyReason::InsufficientPartitions
            }
            Self::AssignedTopics(topics)
                if topics
                    .iter()
                    .any(|topic| !health.assignment.iter().any(|tp| &tp.topic == topic)) =>
            {
                ReadyReason::MissingTopics
            }
            Self::AssignedPartitions(tps)
                if tps.iter().any(|tp| !health.assignment.contains(tp)) =>
            {
                ReadyReason::MissingPartitions
            }
            _ => ReadyReason::Ready,
        }
    }

    /// 业务作用：在查询或等待开始前排除永远不能满足的要求。
    /// 参数说明：无。
    /// 返回：合法要求返回成功，零分区、空白、重复或负坐标返回 Config。
    pub fn validate(&self) -> crate::Result<()> {
        let valid = match self {
            Self::Joined => true,
            Self::Assigned { min_partitions } => *min_partitions > 0,
            Self::AssignedTopics(topics) => {
                !topics.is_empty()
                    && topics.iter().all(|topic| !topic.trim().is_empty())
                    && topics
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == topics.len()
            }
            Self::AssignedPartitions(tps) => {
                !tps.is_empty()
                    && tps
                        .iter()
                        .all(|tp| !tp.topic.trim().is_empty() && tp.partition >= 0)
                    && tps.iter().collect::<std::collections::BTreeSet<_>>().len() == tps.len()
            }
        };
        if valid {
            Ok(())
        } else {
            Err(crate::NafkaError::Config(
                "Ready 要求须非空、无重复且坐标有效".into(),
            ))
        }
    }
}

/// 接流量判定的稳定原因，Rust 的非法枚举值另行返回 InvalidRequirement。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyReason {
    /// 当次观测满足要求。
    Ready,
    /// 要求的范围本身非法。
    InvalidRequirement,
    /// 尚未完成本地启动。
    Starting,
    /// 正在等待 join 或 assignment。
    Joining,
    /// 当前没有有效分配代次。
    NotJoined,
    /// 旧会话退出或自动重建等待中。
    Recovering,
    /// 失败责任仍在退避。
    Retrying,
    /// 安全水位尚未得到 broker 提交确认。
    CommitBlocked,
    /// 死信容量阻止继续派发。
    DltBackpressure,
    /// 未提交进度占满容量。
    ProgressBackpressure,
    /// 需要人工处置的局部保护。
    Degraded,
    /// owner 永久错误或重建预算耗尽。
    Crashed,
    /// 已关闭新增消费准入。
    Stopping,
    /// owner 已经退出。
    Stopped,
    /// 已分配数量低于要求。
    InsufficientPartitions,
    /// 未覆盖指定 topic。
    MissingTopics,
    /// 未覆盖指定分区。
    MissingPartitions,
}

/// 就绪诊断保存要求与同次健康观测，不是持久租约。
#[derive(Clone, Debug)]
pub struct ReadyStatus {
    /// 逻辑源身份。
    pub source_id: String,
    /// 调用方的冻结要求。
    pub requirement: ReadyRequirement,
    /// 当前健康证据，包括 group、assignment 和代次。
    pub health: GroupHealth,
    /// 首要稳定原因。
    pub reason: ReadyReason,
    /// 此诊断生成时间。
    pub observed_at: SystemTime,
}

impl ReadyStatus {
    /// 业务作用：从同一份健康观测构造原因，避免分配与状态跨次混合。
    /// 参数说明：source_id 为源身份，requirement 为要求，health 为当前快照。
    /// 返回：拥有独立证据的诊断，无 broker 副作用。
    pub(crate) fn evaluate(
        source_id: &str,
        requirement: ReadyRequirement,
        health: GroupHealth,
    ) -> Self {
        Self {
            source_id: source_id.to_owned(),
            reason: requirement.reason(&health),
            requirement,
            health,
            observed_at: SystemTime::now(),
        }
    }
    /// 业务作用：供接流量门禁读取本次诊断。
    /// 参数说明：无。
    /// 返回：仅当前要求已满足时为真。
    pub fn ready(&self) -> bool {
        self.reason == ReadyReason::Ready
    }
}

/// 就绪快照:仅表示"该时刻已满足要求",rebalance 会使其降级,不是永久租约。
#[derive(Clone, Debug)]
pub struct GroupReadySnapshot {
    /// group.id。
    pub group: String,
    /// nafka 内部 assignment epoch，多槽时为完整槽向量的观测编号，不是 broker 的 group
    /// generation id——那是 i32 且随协议演进变化,公共 API 不暴露底层 generation。
    pub assignment_epoch: u64,
    /// 满足要求时的 assignment 快照。
    pub assignment: Vec<Tp>,
    /// 快照产生时间。
    pub observed_at: SystemTime,
}
