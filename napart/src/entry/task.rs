//! 新调度内核的任务级状态与所有权权威。
//!
//! 本模块把任务状态、逻辑 owner、物理持有位置和移动描述符集中在同一个对象中，为
//! `PartitionSlot` 主队列与盗洞迁移提供唯一裁决点。

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use tokio::sync::{Notify, OwnedSemaphorePermit};

const OWNER_RETURN_STAGING: i32 = -1;
const OWNER_CANCELLED: i32 = -2;
const OWNER_COMPLETED: i32 = -3;
const OWNER_REJECTED: i32 = -4;
const OWNER_FAILED: i32 = -5;

/// 任务在新调度内核中的完整生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum EntryState {
    /// producer 正在准备逻辑计数、owner 与物理发布。
    Enqueueing = 0,
    /// 已登记到当前 Runner generation 的延迟索引。
    Delayed = 1,
    /// 已承诺给唯一物理容器，等待对应 consumer。
    Queued = 2,
    /// 当前唯一 consumer 正在把任务迁移到另一物理容器。
    Moving = 3,
    /// 当前 owner slot 已取得业务任务执行权。
    Running = 4,
    /// 唯一结算者正在归还计数、许可、载荷和物理持有权。
    Terminating = 5,
    /// 业务任务正常完成。
    Completed = 6,
    /// 任务在取得执行权前被取消。
    Cancelled = 7,
    /// 任务未完成准入或物理发布。
    Rejected = 8,
    /// 已受理任务失去安全推进条件。
    Failed = 9,
}

impl EntryState {
    /// 业务作用：把原子状态值解码为封闭枚举，避免未知状态被继续当作可执行任务。
    ///
    /// 参数说明：
    /// - `raw`: 原子字段中的状态值。
    ///
    /// 返回：已知状态返回对应枚举；未知值返回 None，调用方应关闭最小故障域。
    fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(Self::Enqueueing),
            1 => Some(Self::Delayed),
            2 => Some(Self::Queued),
            3 => Some(Self::Moving),
            4 => Some(Self::Running),
            5 => Some(Self::Terminating),
            6 => Some(Self::Completed),
            7 => Some(Self::Cancelled),
            8 => Some(Self::Rejected),
            9 => Some(Self::Failed),
            _ => None,
        }
    }

    /// 业务作用：判断状态是否已经成为不可逆的稳定终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Completed、Cancelled、Rejected 或 Failed 返回 true。
    pub(crate) fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::Rejected | Self::Failed
        )
    }
}

/// 框架内部 owner；业务 Future 不读取也不修改该权威。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner {
    /// 指定分区 slot 拥有执行或迁移责任。
    Slot(u32),
    /// 严格归还期间由目标 slot 的 staging 独占持有。
    ReturnStaging,
    /// 取消终态哨兵。
    Cancelled,
    /// 完成终态哨兵。
    Completed,
    /// 拒绝终态哨兵。
    Rejected,
    /// 失败终态哨兵。
    Failed,
}

impl Owner {
    /// 业务作用：把结构化 owner 编码为单个原子值，保留负数作为框架哨兵。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可编码时返回原子值；slot 超出正数范围时返回 InvalidOwner。
    fn encode(self) -> Result<i32, TaskAuthorityError> {
        match self {
            Self::Slot(slot) => i32::try_from(slot).map_err(|_| TaskAuthorityError::InvalidOwner),
            Self::ReturnStaging => Ok(OWNER_RETURN_STAGING),
            Self::Cancelled => Ok(OWNER_CANCELLED),
            Self::Completed => Ok(OWNER_COMPLETED),
            Self::Rejected => Ok(OWNER_REJECTED),
            Self::Failed => Ok(OWNER_FAILED),
        }
    }

    /// 业务作用：解码任务 owner 原子值，拒绝未定义的负数哨兵。
    ///
    /// 参数说明：
    /// - `raw`: owner 原子值。
    ///
    /// 返回：合法值返回结构化 owner；未知哨兵返回 None。
    fn decode(raw: i32) -> Option<Self> {
        match raw {
            OWNER_RETURN_STAGING => Some(Self::ReturnStaging),
            OWNER_CANCELLED => Some(Self::Cancelled),
            OWNER_COMPLETED => Some(Self::Completed),
            OWNER_REJECTED => Some(Self::Rejected),
            OWNER_FAILED => Some(Self::Failed),
            value if value >= 0 => Some(Self::Slot(value as u32)),
            _ => None,
        }
    }

    /// 业务作用：把稳定任务终态映射为对应 owner 哨兵，使 owner 先于状态关闭执行权。
    ///
    /// 参数说明：
    /// - `terminal`: 即将发布的稳定终态。
    ///
    /// 返回：合法终态返回对应哨兵；活动状态返回 InvalidTerminal。
    fn for_terminal(terminal: EntryState) -> Result<Self, TaskAuthorityError> {
        match terminal {
            EntryState::Completed => Ok(Self::Completed),
            EntryState::Cancelled => Ok(Self::Cancelled),
            EntryState::Rejected => Ok(Self::Rejected),
            EntryState::Failed => Ok(Self::Failed),
            _ => Err(TaskAuthorityError::InvalidTerminal),
        }
    }

    /// 业务作用：判断 owner 是否已经关闭全部执行与迁移权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定终态哨兵返回 true；slot 与 ReturnStaging 返回 false。
    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Cancelled | Self::Completed | Self::Rejected | Self::Failed
        )
    }
}

/// 任务 Arc 当前承诺给哪个物理持有位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PhysicalRetention {
    /// 尚未进入队列，或已经完整脱离。
    Detached = 0,
    /// 唯一业务队列拥有物理持有责任。
    Container = 1,
    /// 唯一 consumer 的处理栈拥有物理持有责任。
    Handling = 2,
    /// 失败收口队列拥有物理持有责任。
    FailureQueued = 3,
    /// 失败收口 consumer 的处理栈拥有物理持有责任。
    FailureHandling = 4,
}

impl PhysicalRetention {
    /// 业务作用：把原子物理持有值解码为封闭枚举，未知值不得继续参与容器转移。
    ///
    /// 参数说明：
    /// - `raw`: 原子字段中的持有值。
    ///
    /// 返回：已知值返回对应枚举；未知值返回 None。
    fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(Self::Detached),
            1 => Some(Self::Container),
            2 => Some(Self::Handling),
            3 => Some(Self::FailureQueued),
            4 => Some(Self::FailureHandling),
            _ => None,
        }
    }
}

/// 逻辑计数所属容器类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogicalContainer {
    /// 分区主队列或当前 worker。
    Main,
    /// 非严格任务盗洞。
    NonStrictTunnel,
    /// 严格任务存量 FIFO。
    StrictStock,
    /// 严格任务增量 FIFO。
    StrictIncremental,
    /// 严格归还暂存 FIFO。
    ReturnStaging,
    /// 无法安全继续推进时的失败保留队列。
    FailureRetention,
}

/// 一笔任务当前计入的逻辑位置；迁移结算用它保证先增目标再减来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogicalOwner {
    /// 对该逻辑计数负责的 slot。
    pub(crate) slot: u32,
    /// 不可变路由快照 epoch。
    pub(crate) route_epoch: u64,
    /// 具体逻辑容器类别。
    pub(crate) container: LogicalContainer,
}

/// 物理移动阶段，用于审计者按协议判断合法帮助范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MoveStage {
    /// 非严格任务从主队列迁入目标盗洞。
    NonStrictOutbound,
    /// 严格存量从源主队列迁入 stock FIFO。
    StrictStock,
    /// 严格增量进入 incremental FIFO。
    StrictIncremental,
    /// 严格归还期间进入 staging FIFO。
    ReturnStaging,
    /// staging 或目标 FIFO 回写源主队列。
    ReturnToSource,
    /// 任务转入失败保留队列。
    FailureRetention,
}

/// 每次物理移动前完整安装的不可变描述符。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MoveDescriptor {
    /// 当前移动协议阶段。
    pub(crate) stage: MoveStage,
    /// 交出任务的 slot。
    pub(crate) source_slot: u32,
    /// 移动开始时实际持有物理责任的 owner；归还 staging 不伪装成目标 slot。
    source_owner: Owner,
    /// 接收任务的 slot。
    pub(crate) target_slot: u32,
    /// 来源队列中的全局序号。
    pub(crate) source_sequence: u64,
    /// 移动开始时刻，供 transition timeout 审计。
    pub(crate) started_at: Instant,
    /// 描述符所属 Runner generation。
    pub(crate) runner_generation: u64,
    /// 单任务内不可复用的移动 epoch。
    pub(crate) move_epoch: u64,
}

/// 已安装移动描述符的能力票据；所有推进动作都必须复验同一 epoch。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MoveTicket {
    epoch: u64,
}

impl MoveTicket {
    /// 业务作用：读取移动 epoch，供 moving 表和活动审计集合关联同一笔迁移。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：单任务内不可复用的移动 epoch。
    pub(crate) fn epoch(self) -> u64 {
        self.epoch
    }
}

/// 取消请求的线性化结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelDecision {
    /// 已取得结算权，物理 owner 或延迟索引责任方必须继续完成结算。
    Accepted,
    /// 任务正在移动，只登记协作意图，由移动发布者在稳定落点后转入结算。
    DeferredMoving,
    /// 同一移动期间已经登记过取消意图。
    AlreadyDeferred,
    /// 任务已经运行、正在其它结算或已进入稳定终态。
    TooLate,
}

/// 移动发布后的状态；取消协作可能让目标 consumer 只执行物理清理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MoveCompletion {
    /// 任务在目标容器保持 Queued，可以正常竞争执行权。
    Queued,
    /// 任务已经转入 Terminating，目标 consumer 必须摘除后发布取消终态。
    CancellationPending,
}

/// 任务权威操作失败；调用方必须停止当前动作并按所属故障域处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskAuthorityError {
    /// 任务状态不允许当前迁移。
    InvalidState,
    /// owner 与执行当前动作的 slot 或协议哨兵不一致。
    OwnerMismatch,
    /// slot 无法编码进保留负数哨兵的原子字段。
    InvalidOwner,
    /// 物理持有位置与调用方声称的容器责任不一致。
    RetentionMismatch,
    /// 已存在另一笔活动移动描述符。
    MoveAlreadyPrepared,
    /// 移动票据与当前描述符或 generation 不一致。
    MoveTicketMismatch,
    /// 单任务移动 epoch 已耗尽。
    MoveEpochExhausted,
    /// 终态类型、原因或 owner 哨兵不合法。
    InvalidTerminal,
    /// 结算意图已经由另一责任方安装。
    SettlementConflict,
    /// 同一终态已经由另一责任方取得 owner 哨兵或完成发布。
    SettlementOwned,
    /// 取得运行权后没有找到唯一业务载荷。
    TaskMissing,
    /// 原子字段含有未定义值，不能继续证明唯一性。
    AuthorityCorrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// 业务作用：把任务终态与稳定原因绑定为一次不可分割的结算记录。
struct TerminalRecord {
    state: EntryState,
    reason: Option<&'static str>,
}

/// 新调度内核中的任务条目；`Submission` 最终只保留本对象的稳定 Arc 投影。
pub(crate) struct TaskEntry<T> {
    id: u64,
    runner_id: u64,
    runner_generation: u64,
    home: u32,
    task_type: u64,
    state: AtomicU8,
    owner: AtomicI32,
    cancel_requested: AtomicBool,
    terminal: OnceLock<TerminalRecord>,
    pending_terminal: Mutex<Option<TerminalRecord>>,
    task: Mutex<Option<T>>,
    logical_owner: Mutex<Option<LogicalOwner>>,
    physical_retention: AtomicU8,
    move_descriptor: Mutex<Option<MoveDescriptor>>,
    next_move_epoch: AtomicU64,
    global_permit: Mutex<Option<OwnedSemaphorePermit>>,
    queued_permit: Mutex<Option<OwnedSemaphorePermit>>,
    completed: Notify,
    /// 提交时刻的环境 trace 上下文；执行期恢复为业务 Future 的环境作用域。
    /// 26 字节 Copy 值，迁移、归还与失败收口均不读写该字段。
    trace: Option<natelemetry::TraceContext>,
}

impl<T> TaskEntry<T> {
    /// 业务作用：创建处于 Enqueueing 的任务权威，冻结 Runner、generation、home、类型和
    /// 唯一业务载荷；初始 owner 为 home slot，尚未承诺给任何物理容器。
    ///
    /// 参数说明：
    /// - `id`: 当前 generation 内不可复用的提交标识。
    /// - `runner_id`: 稳定 Runner 标识。
    /// - `runner_generation`: 本任务所属 Runner 代次。
    /// - `home`: 按 key hash 得出的原始 slot。
    /// - `task_type`: 当前 Runner 内的任务类型标识。
    /// - `task`: 唯一业务载荷。
    /// - `global_permit`: Runner 全局在飞许可。
    /// - `queued_permit`: 类型排队许可；延迟任务可在到期准入后再安装。
    ///
    /// 返回：字段合法时返回新的 Arc；home 无法编码时返回 InvalidOwner，载荷与许可归还调用方。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: u64,
        runner_id: u64,
        runner_generation: u64,
        home: u32,
        task_type: u64,
        task: T,
        global_permit: Option<OwnedSemaphorePermit>,
        queued_permit: Option<OwnedSemaphorePermit>,
    ) -> Result<Arc<Self>, TaskAuthorityError> {
        let owner = Owner::Slot(home).encode()?;
        Ok(Arc::new(Self {
            id,
            runner_id,
            runner_generation,
            home,
            task_type,
            state: AtomicU8::new(EntryState::Enqueueing as u8),
            owner: AtomicI32::new(owner),
            cancel_requested: AtomicBool::new(false),
            terminal: OnceLock::new(),
            pending_terminal: Mutex::new(None),
            task: Mutex::new(Some(task)),
            logical_owner: Mutex::new(None),
            physical_retention: AtomicU8::new(PhysicalRetention::Detached as u8),
            move_descriptor: Mutex::new(None),
            next_move_epoch: AtomicU64::new(0),
            global_permit: Mutex::new(global_permit),
            queued_permit: Mutex::new(queued_permit),
            completed: Notify::new(),
            // 构造点即提交点(立即与延迟两条入口都经此),在此捕获提交方的环境链路;
            // 延迟任务因此关联到提交时刻而不是到期 tick,与"谁发起谁的链路"一致。
            trace: natelemetry::ambient(),
        }))
    }

    /// 业务作用：读取提交时捕获的环境 trace 上下文，供 worker 在执行业务 Future 前恢复同一链路。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：提交方处于链路作用域内时返回其上下文，否则返回 None。
    pub(crate) fn trace(&self) -> Option<natelemetry::TraceContext> {
        self.trace
    }

    /// 业务作用：读取不可复用提交标识，供延迟索引、moving 表和诊断记录关联同一任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的提交标识。
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// 业务作用：读取稳定 Runner 标识，阻止跨 Runner 容器接纳本任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的 Runner 标识。
    pub(crate) fn runner_id(&self) -> u64 {
        self.runner_id
    }

    /// 业务作用：读取任务代次，供 producer、timer、worker 和审计者拒绝迟到旧代动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的 Runner generation。
    pub(crate) fn runner_generation(&self) -> u64 {
        self.runner_generation
    }

    /// 业务作用：读取原始路由 slot，严格任务归还时据此恢复本地执行位置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 key hash 冻结的 home slot。
    pub(crate) fn home(&self) -> u32 {
        self.home
    }

    /// 业务作用：读取冻结的任务类型标识，供目标容器复验 TypeState 一致性。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 Runner 内的任务类型标识。
    pub(crate) fn task_type(&self) -> u64 {
        self.task_type
    }

    /// 业务作用：读取当前任务状态；未知原子值立即返回 AuthorityCorrupted。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Acquire 快照或权威损坏错误。
    pub(crate) fn state(&self) -> Result<EntryState, TaskAuthorityError> {
        EntryState::from_raw(self.state.load(Ordering::Acquire))
            .ok_or(TaskAuthorityError::AuthorityCorrupted)
    }

    /// 业务作用：读取当前 owner，使 worker 在执行和迁移副作用前复验自身权威。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Acquire 快照；未知哨兵返回 AuthorityCorrupted。
    pub(crate) fn owner(&self) -> Result<Owner, TaskAuthorityError> {
        Owner::decode(self.owner.load(Ordering::Acquire))
            .ok_or(TaskAuthorityError::AuthorityCorrupted)
    }

    /// 业务作用：读取当前物理持有位置，供容器发布、consumer 摘取和失败收口交接责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Acquire 快照；未知值返回 AuthorityCorrupted。
    pub(crate) fn physical_retention(&self) -> Result<PhysicalRetention, TaskAuthorityError> {
        PhysicalRetention::from_raw(self.physical_retention.load(Ordering::Acquire))
            .ok_or(TaskAuthorityError::AuthorityCorrupted)
    }

    /// 业务作用：把 Enqueueing 任务发布为 Delayed；调用方必须已经登记延迟索引和 timer 槽位。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回 Ok；状态已被取消或其它责任方推进时返回 InvalidState。
    pub(crate) fn publish_delayed(&self) -> Result<(), TaskAuthorityError> {
        self.transition(EntryState::Enqueueing, EntryState::Delayed)
    }

    /// 业务作用：延迟到期责任方把唯一索引项重新取得为 Enqueueing，之后才能申请类型许可和路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回 Ok；timer 代次迟到或取消已胜出时返回 InvalidState。
    pub(crate) fn claim_expired(&self) -> Result<(), TaskAuthorityError> {
        self.transition(EntryState::Delayed, EntryState::Enqueueing)
    }

    /// 业务作用：延迟到期准入成功后安装类型排队许可；重复安装表示两条到期路径同时取得权威。
    ///
    /// 参数说明：
    /// - `permit`: 当前 TypeState 的排队许可。
    ///
    /// 返回：首次安装返回 Ok；已有许可返回 SettlementConflict，并把传入许可立即归还。
    pub(crate) fn install_queued_permit(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), TaskAuthorityError> {
        if self.state()? != EntryState::Enqueueing {
            drop(permit);
            return Err(TaskAuthorityError::InvalidState);
        }
        let mut queued = self.queued_permit.lock().unwrap_or_else(|e| e.into_inner());
        if queued.is_some() {
            drop(queued);
            drop(permit);
            return Err(TaskAuthorityError::SettlementConflict);
        }
        *queued = Some(permit);
        Ok(())
    }

    /// 业务作用：在队列 reservation 已取得后准备首次物理发布，按“逻辑位置、owner、物理
    /// retention、Queued 状态”的顺序建立可消费承诺；真正发布 reservation 必须在本调用后完成。
    ///
    /// 参数说明：
    /// - `expected_owner`: producer 当前持有的 owner。
    /// - `target_owner`: 物理落点对应的新 owner。
    /// - `logical_owner`: 已经增加逻辑计数的位置。
    ///
    /// 返回：全部权威一次建立返回 Ok；任一步竞争失败会回滚本调用已经写入的字段。
    pub(crate) fn prepare_initial_queue(
        &self,
        expected_owner: Owner,
        target_owner: Owner,
        logical_owner: LogicalOwner,
    ) -> Result<(), TaskAuthorityError> {
        if self.state()? != EntryState::Enqueueing {
            return Err(TaskAuthorityError::InvalidState);
        }
        let expected = expected_owner.encode()?;
        let target = target_owner.encode()?;
        if target_owner.is_terminal() {
            return Err(TaskAuthorityError::InvalidOwner);
        }
        let mut logical = self.logical_owner.lock().unwrap_or_else(|e| e.into_inner());
        if logical.is_some() {
            return Err(TaskAuthorityError::SettlementConflict);
        }
        *logical = Some(logical_owner);
        if self
            .owner
            .compare_exchange(expected, target, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            *logical = None;
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        if self
            .physical_retention
            .compare_exchange(
                PhysicalRetention::Detached as u8,
                PhysicalRetention::Container as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.owner.store(expected, Ordering::Release);
            *logical = None;
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        if self
            .state
            .compare_exchange(
                EntryState::Enqueueing as u8,
                EntryState::Queued as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.physical_retention
                .store(PhysicalRetention::Detached as u8, Ordering::Release);
            self.owner.store(expected, Ordering::Release);
            *logical = None;
            return Err(TaskAuthorityError::InvalidState);
        }
        Ok(())
    }

    /// 业务作用：物理队列发布明确失败且 reservation 从未携带任务时，撤销 Container 承诺，
    /// 让 producer 可以从 Queued 进入拒绝或失败结算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Container 确认回到 Detached 返回 Ok；位置不一致返回 RetentionMismatch。
    pub(crate) fn detach_unpublished_container(&self) -> Result<(), TaskAuthorityError> {
        self.change_retention(PhysicalRetention::Container, PhysicalRetention::Detached)
    }

    /// 业务作用：唯一 consumer 摘取队列任务时把物理责任从 Container 转到处理栈，并复验
    /// owner；该步骤失败意味着重复消费或跨容器引用。
    ///
    /// 参数说明：
    /// - `expected_owner`: 当前容器声明的 owner。
    ///
    /// 返回：owner 与 retention 同时一致返回 Ok；否则返回具体权威错误。
    pub(crate) fn claim_container(&self, expected_owner: Owner) -> Result<(), TaskAuthorityError> {
        self.clear_published_move_at_container(expected_owner)?;
        if self.owner()? != expected_owner {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        self.change_retention(PhysicalRetention::Container, PhysicalRetention::Handling)
    }

    /// 业务作用：目标 worker 在撤销严格归还时接管 staging 条目，先把专属 owner 转回目标
    /// slot，再取得唯一处理栈责任。
    ///
    /// 参数说明：
    /// - `target_slot`: 当前严格盗洞唯一目标 slot。
    ///
    /// 返回：ReturnStaging owner 与 Container retention 同时一致时返回 Ok；迟到或重复取得返回错误。
    pub(crate) fn claim_return_staging(&self, target_slot: u32) -> Result<(), TaskAuthorityError> {
        self.clear_published_move_at_container(Owner::ReturnStaging)?;
        self.owner
            .compare_exchange(
                Owner::ReturnStaging.encode()?,
                Owner::Slot(target_slot).encode()?,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| TaskAuthorityError::OwnerMismatch)?;
        if let Err(error) =
            self.change_retention(PhysicalRetention::Container, PhysicalRetention::Handling)
        {
            self.owner
                .compare_exchange(
                    Owner::Slot(target_slot).encode()?,
                    Owner::ReturnStaging.encode()?,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .map_err(|_| TaskAuthorityError::AuthorityCorrupted)?;
            return Err(error);
        }
        Ok(())
    }

    /// 业务作用：真实物理 consumer 取得移动发布的任务后，帮助清理上一跳已完成描述符，
    /// 避免发布者与高速 consumer 交错时把稳定 Queued 任务误留在 Moving 审计域。
    ///
    /// 参数说明：
    /// - `expected_owner`: 当前物理容器按移动阶段应持有的 owner。
    ///
    /// 返回：没有描述符或描述符已经完整发布到当前容器时返回 Ok；状态、owner、retention
    /// 或目标不一致时拒绝取得任务。
    fn clear_published_move_at_container(
        &self,
        expected_owner: Owner,
    ) -> Result<(), TaskAuthorityError> {
        let mut current = self
            .move_descriptor
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(descriptor) = current.as_ref() else {
            return Ok(());
        };
        if !matches!(self.state()?, EntryState::Queued | EntryState::Terminating) {
            return Err(TaskAuthorityError::InvalidState);
        }
        if self.physical_retention()? != PhysicalRetention::Container {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        if self.owner()? != expected_owner {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        let published_owner = match descriptor.stage {
            MoveStage::ReturnStaging => Owner::ReturnStaging,
            _ => Owner::Slot(descriptor.target_slot),
        };
        if published_owner != expected_owner {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        // consumer 已持有真实队列载荷，物理发布必然先于本步骤；此时清理描述符不会
        // 把尚未发布的 reservation 伪装成完成，也允许迟到发布者幂等结束 moving 登记。
        *current = None;
        Ok(())
    }

    /// 业务作用：当前 consumer 为物理移动安装完整描述符；调用方随后必须先登记 moving 表与
    /// 活动审计集合，再调用 `activate_move` 发布 Moving。
    ///
    /// 参数说明：
    /// - `stage`: 当前移动协议阶段。
    /// - `source_slot`: 交出任务的 slot。
    /// - `target_slot`: 接收任务的 slot。
    /// - `source_sequence`: 来源队列中的全局序号。
    /// - `started_at`: 移动开始时刻。
    ///
    /// 返回：成功返回不可复用票据；状态、owner、generation 或 retention 不一致时拒绝。
    pub(crate) fn prepare_move(
        &self,
        stage: MoveStage,
        source_slot: u32,
        target_slot: u32,
        source_sequence: u64,
        started_at: Instant,
    ) -> Result<MoveTicket, TaskAuthorityError> {
        if self.state()? != EntryState::Queued {
            return Err(TaskAuthorityError::InvalidState);
        }
        let source_owner = self.owner()?;
        if source_owner != Owner::Slot(source_slot)
            && !(stage == MoveStage::ReturnToSource && source_owner == Owner::ReturnStaging)
        {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        if self.physical_retention()? != PhysicalRetention::Handling {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        let epoch = self
            .next_move_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| TaskAuthorityError::MoveEpochExhausted)?
            + 1;
        let descriptor = MoveDescriptor {
            stage,
            source_slot,
            source_owner,
            target_slot,
            source_sequence,
            started_at,
            runner_generation: self.runner_generation,
            move_epoch: epoch,
        };
        let mut current = self
            .move_descriptor
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if current.is_some() {
            return Err(TaskAuthorityError::MoveAlreadyPrepared);
        }
        *current = Some(descriptor);
        Ok(MoveTicket { epoch })
    }

    /// 业务作用：在 moving 表和活动审计集合均已登记后发布 Queued 到 Moving；竞争失败时
    /// 先由调用方撤销外部登记，再清理描述符。
    ///
    /// 参数说明：
    /// - `ticket`: `prepare_move` 返回的当前移动票据。
    ///
    /// 返回：描述符与状态一致时返回 Ok；迟到或取消已取得结算权时返回错误。
    pub(crate) fn activate_move(&self, ticket: MoveTicket) -> Result<(), TaskAuthorityError> {
        self.ensure_move(ticket)?;
        self.transition(EntryState::Queued, EntryState::Moving)
    }

    /// 业务作用：移动尚未发布 Moving 时，在外部 moving 登记已经撤销后清理描述符。
    ///
    /// 参数说明：
    /// - `ticket`: 当前准备阶段的移动票据。
    ///
    /// 返回：任务尚未发布 Moving 且票据一致时清理成功；否则拒绝迟到清理。
    pub(crate) fn abandon_prepared_move(
        &self,
        ticket: MoveTicket,
    ) -> Result<(), TaskAuthorityError> {
        if self.state()? == EntryState::Moving {
            return Err(TaskAuthorityError::InvalidState);
        }
        self.clear_move(ticket)
    }

    /// 业务作用：在增加目标逻辑计数后把执行 owner 从来源 CAS 到目标；业务队列发布前必须
    /// 完成该步骤，失败时不能继续移动。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    ///
    /// 返回：owner 唯一转移成功返回 Ok；描述符迟到或 owner 不一致返回错误。
    pub(crate) fn transfer_move_owner(&self, ticket: MoveTicket) -> Result<(), TaskAuthorityError> {
        let descriptor = self.active_move(ticket)?;
        let source = descriptor.source_owner.encode()?;
        let target = match descriptor.stage {
            MoveStage::ReturnStaging => Owner::ReturnStaging.encode()?,
            _ => Owner::Slot(descriptor.target_slot).encode()?,
        };
        self.owner
            .compare_exchange(source, target, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| TaskAuthorityError::OwnerMismatch)
    }

    /// 业务作用：目标 queue reservation 已取得后，把处理栈的物理责任转成 Container 承诺；
    /// 随后的稳定状态必须先于 reservation 携带任务发布。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    ///
    /// 返回：票据与 Handling retention 一致返回 Ok；否则拒绝发布。
    pub(crate) fn prepare_move_container(
        &self,
        ticket: MoveTicket,
    ) -> Result<(), TaskAuthorityError> {
        self.active_move(ticket)?;
        self.change_retention(PhysicalRetention::Handling, PhysicalRetention::Container)
    }

    /// 业务作用：在目标 owner 和物理 Container 均已建立后切换逻辑 owner；调用方应先增加
    /// 新位置计数，并用返回的旧位置完成来源减计。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    /// - `target`: 已经增加计数的目标逻辑位置。
    ///
    /// 返回：成功返回旧逻辑位置；缺失或票据迟到返回权威错误。
    pub(crate) fn replace_logical_owner(
        &self,
        ticket: MoveTicket,
        target: LogicalOwner,
    ) -> Result<LogicalOwner, TaskAuthorityError> {
        self.active_move(ticket)?;
        if self.physical_retention()? != PhysicalRetention::Container {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        let mut logical = self.logical_owner.lock().unwrap_or_else(|e| e.into_inner());
        logical
            .replace(target)
            .ok_or(TaskAuthorityError::AuthorityCorrupted)
    }

    /// 业务作用：目标逻辑计数、owner、Container 承诺和来源减计全部完成后发布稳定 Queued；
    /// 若移动期间收到取消意图，则立即转入 Terminating，禁止目标 consumer 执行业务载荷。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    ///
    /// 返回：正常排队或待取消清理状态；任何权威不一致返回错误并保持 Moving。
    pub(crate) fn publish_move(
        &self,
        ticket: MoveTicket,
    ) -> Result<MoveCompletion, TaskAuthorityError> {
        let descriptor = self.active_move(ticket)?;
        if self.physical_retention()? != PhysicalRetention::Container {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        let expected_owner = match descriptor.stage {
            MoveStage::ReturnStaging => Owner::ReturnStaging,
            _ => Owner::Slot(descriptor.target_slot),
        };
        if self.owner()? != expected_owner {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        self.transition(EntryState::Moving, EntryState::Queued)?;
        if self.cancel_requested.swap(false, Ordering::AcqRel) {
            if let Err(error) = self.begin_termination(
                EntryState::Queued,
                EntryState::Cancelled,
                Some("cancelled_by_caller"),
            ) {
                self.cancel_requested.store(true, Ordering::Release);
                if self.state()? != EntryState::Terminating {
                    return Err(error);
                }
            }
        }
        self.move_completion()
    }

    /// 业务作用：成功移动或回滚已经发布稳定状态且 moving 表已摘除后清理描述符；旧审计者
    /// 仍持有 Arc 时也不能用旧票据推进下一笔移动。
    ///
    /// 参数说明：
    /// - `ticket`: 已结束移动的票据。
    ///
    /// 返回：当前不再处于 Moving 且票据一致时清理成功；否则拒绝。
    pub(crate) fn clear_completed_move(
        &self,
        ticket: MoveTicket,
    ) -> Result<(), TaskAuthorityError> {
        if self.state()? == EntryState::Moving {
            return Err(TaskAuthorityError::InvalidState);
        }
        self.clear_move(ticket)
    }

    /// 业务作用：目标 reservation 明确未发布时撤销 Container 承诺，使移动发布者能够恢复
    /// owner、目标逻辑计数和来源执行位置。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    ///
    /// 返回：Container 回到 Handling 返回 Ok；票据或 retention 不一致返回错误。
    pub(crate) fn rollback_move_container(
        &self,
        ticket: MoveTicket,
    ) -> Result<(), TaskAuthorityError> {
        self.active_move(ticket)?;
        self.change_retention(PhysicalRetention::Container, PhysicalRetention::Handling)
    }

    /// 业务作用：目标物理发布从未发生且目标逻辑计数已撤销后，把 owner 恢复到来源 slot。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    ///
    /// 返回：目标 owner 唯一恢复成功返回 Ok；票据或 owner 不一致返回错误。
    pub(crate) fn restore_move_owner(&self, ticket: MoveTicket) -> Result<(), TaskAuthorityError> {
        let descriptor = self.active_move(ticket)?;
        let target = match descriptor.stage {
            MoveStage::ReturnStaging => Owner::ReturnStaging.encode()?,
            _ => Owner::Slot(descriptor.target_slot).encode()?,
        };
        let source = descriptor.source_owner.encode()?;
        self.owner
            .compare_exchange(target, source, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| TaskAuthorityError::OwnerMismatch)
    }

    /// 业务作用：目标物理发布失败且来源逻辑计数已经恢复后，把逻辑 owner 从目标位置退回
    /// 来源；返回值供调用方恰好减少一次目标计数。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    /// - `expected_target`: 此前由 `replace_logical_owner` 安装的目标位置。
    /// - `source`: 已经重新增加计数的来源位置。
    ///
    /// 返回：当前逻辑 owner 与目标完全一致时返回被撤销的目标位置；否则不改写并返回错误。
    pub(crate) fn restore_logical_owner(
        &self,
        ticket: MoveTicket,
        expected_target: LogicalOwner,
        source: LogicalOwner,
    ) -> Result<LogicalOwner, TaskAuthorityError> {
        self.active_move(ticket)?;
        let mut logical = self.logical_owner.lock().unwrap_or_else(|e| e.into_inner());
        if logical.as_ref() != Some(&expected_target) {
            return Err(TaskAuthorityError::AuthorityCorrupted);
        }
        Ok(logical.replace(source).expect("logical owner was checked"))
    }

    /// 业务作用：移动的所有目标副作用已经逆序撤销后，把状态恢复为来源 Queued；来源 worker
    /// 仍独占 Handling，随后可以本地执行或重新发布到来源容器。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    ///
    /// 返回：来源 owner 与 Handling retention 一致时返回稳定排队或待取消状态；否则保持
    /// Moving 并返回错误。
    pub(crate) fn rollback_move(
        &self,
        ticket: MoveTicket,
    ) -> Result<MoveCompletion, TaskAuthorityError> {
        let descriptor = self.active_move(ticket)?;
        if self.owner()? != descriptor.source_owner {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        if self.physical_retention()? != PhysicalRetention::Handling {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        self.transition(EntryState::Moving, EntryState::Queued)?;
        if self.cancel_requested.swap(false, Ordering::AcqRel) {
            if let Err(error) = self.begin_termination(
                EntryState::Queued,
                EntryState::Cancelled,
                Some("cancelled_by_caller"),
            ) {
                self.cancel_requested.store(true, Ordering::Release);
                if self.state()? != EntryState::Terminating {
                    return Err(error);
                }
            }
        }
        self.move_completion()
    }

    /// 业务作用：调用方在执行前请求取消；Queued、Delayed 与 Enqueueing 直接竞争结算权，
    /// Moving 只登记协作意图，Running 和稳定终态不再接受普通取消。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：明确说明本次请求取得结算权、延期协作、重复登记或已经过晚。
    pub(crate) fn request_cancel(&self) -> CancelDecision {
        loop {
            let Ok(state) = self.state() else {
                return CancelDecision::TooLate;
            };
            match state {
                EntryState::Enqueueing | EntryState::Delayed | EntryState::Queued => {
                    match self.begin_termination(
                        state,
                        EntryState::Cancelled,
                        Some("cancelled_by_caller"),
                    ) {
                        Ok(()) => return CancelDecision::Accepted,
                        Err(TaskAuthorityError::InvalidState) => continue,
                        Err(_) => return CancelDecision::TooLate,
                    }
                }
                EntryState::Moving => {
                    return if self.cancel_requested.swap(true, Ordering::AcqRel) {
                        CancelDecision::AlreadyDeferred
                    } else {
                        CancelDecision::DeferredMoving
                    };
                }
                EntryState::Running
                | EntryState::Terminating
                | EntryState::Completed
                | EntryState::Cancelled
                | EntryState::Rejected
                | EntryState::Failed => return CancelDecision::TooLate,
            }
        }
    }

    /// 业务作用：读取当前移动描述符的审计能力，使 supervisor 能在 worker 异常离场后收口
    /// 已登记但未摘除的 moving 项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在现役描述符时返回对应票据与开始时刻；没有移动责任时返回 None。
    pub(crate) fn move_audit_snapshot(&self) -> Option<(MoveTicket, Instant)> {
        self.move_descriptor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|descriptor| {
                (
                    MoveTicket {
                        epoch: descriptor.move_epoch,
                    },
                    descriptor.started_at,
                )
            })
    }

    /// 业务作用：当前 owner slot 在取得 Handling 后竞争运行权并取出唯一业务载荷，在开放
    /// 类型容量前先提交排队减计；Queued 与取消结算竞争同一个状态 CAS，至多一方成功。
    ///
    /// 参数说明：
    /// - `slot`: 当前 worker 所属 slot。
    /// - `commit_queued`: 只执行内部原子减计且不得展开的账本动作。
    ///
    /// 返回：成功返回唯一业务载荷；失权、状态变化或载荷缺失返回明确错误。
    pub(crate) fn take_for_running(
        &self,
        slot: u32,
        commit_queued: impl FnOnce(),
    ) -> Result<T, TaskAuthorityError> {
        if self.owner()? != Owner::Slot(slot) {
            return Err(TaskAuthorityError::OwnerMismatch);
        }
        if self.physical_retention()? != PhysicalRetention::Handling {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        self.transition(EntryState::Queued, EntryState::Running)?;
        let task = self
            .task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or(TaskAuthorityError::TaskMissing)?;
        let queued_permit = self
            .queued_permit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        // 排队投影必须先于容量许可撤销；否则被唤醒的新提交可在旧投影尚未减少时
        // 建立新 TaskEnvelope，使瞬时 queued_depth 越过冻结容量。
        commit_queued();
        crate::run_isolated("任务取得运行权后的类型许可归还", || {
            drop(queued_permit)
        });
        Ok(task)
    }

    /// 业务作用：执行权持有者记录正常完成、任务异常或有损停止结果，先取得唯一 Terminating
    /// 权威；调用方随后必须结算逻辑计数与许可。
    ///
    /// 参数说明：
    /// - `terminal`: Completed 或 Failed。
    /// - `reason`: Failed 的稳定原因；Completed 必须为 None。
    ///
    /// 返回：Running 到 Terminating 的唯一迁移成功返回 Ok；否则返回权威错误。
    pub(crate) fn finish_running(
        &self,
        terminal: EntryState,
        reason: Option<&'static str>,
    ) -> Result<(), TaskAuthorityError> {
        if !matches!(terminal, EntryState::Completed | EntryState::Failed) {
            return Err(TaskAuthorityError::InvalidTerminal);
        }
        self.begin_termination(EntryState::Running, terminal, reason)
    }

    /// 业务作用：提交或 timer 责任方从指定活动状态取得拒绝/失败结算权；活动移动必须携带
    /// move ticket 走 `fail_move`，避免仅凭 Arc 越过移动 epoch。
    ///
    /// 参数说明：
    /// - `expected`: 调用方独占处理的活动状态。
    /// - `terminal`: Rejected 或 Failed。
    /// - `reason`: 稳定失败原因。
    ///
    /// 返回：唯一取得 Terminating 权威返回 Ok；状态或终态参数不合法时返回错误。
    pub(crate) fn fail_from(
        &self,
        expected: EntryState,
        terminal: EntryState,
        reason: &'static str,
    ) -> Result<(), TaskAuthorityError> {
        if !matches!(terminal, EntryState::Rejected | EntryState::Failed) {
            return Err(TaskAuthorityError::InvalidTerminal);
        }
        if expected == EntryState::Moving {
            return Err(TaskAuthorityError::MoveTicketMismatch);
        }
        self.begin_termination(expected, terminal, Some(reason))
    }

    /// 业务作用：活动移动的发布者或合法审计者在证明任务已脱离全部可执行容器后，以当前
    /// move epoch 取得失败结算权，迟到旧审计者不能影响下一笔移动。
    ///
    /// 参数说明：
    /// - `ticket`: 当前活动移动票据。
    /// - `reason`: 稳定失败原因。
    ///
    /// 返回：票据与 Moving 状态一致时进入 Terminating；否则返回权威错误。
    pub(crate) fn fail_move(
        &self,
        ticket: MoveTicket,
        reason: &'static str,
    ) -> Result<(), TaskAuthorityError> {
        self.active_move(ticket)?;
        self.begin_termination(EntryState::Moving, EntryState::Failed, Some(reason))
    }

    /// 业务作用：唯一结算者在任务已脱离队列或由当前 Handling 栈独占时完成资源与逻辑计数
    /// 收口，先发布终态 owner 哨兵，最后以 Release 语义发布稳定终态并通知等待者。
    ///
    /// 参数说明：
    /// - `commit_logical`: 若任务已有逻辑 owner，则在终态可见前恰好调用一次以减少对应计数。
    ///
    /// 返回：全部权威复验和结算完成返回 Ok；物理位置、owner 或终态意图不一致时保持
    /// Terminating 并返回错误。
    pub(crate) fn finalize_termination(
        &self,
        commit_logical: impl FnOnce(LogicalOwner),
    ) -> Result<(), TaskAuthorityError> {
        self.finalize_termination_with_accounting(commit_logical, |_| {})
    }

    /// 业务作用：在稳定终态发布前一并提交 Runner 终态累计量，使停机基线与任务终态共享
    /// 明确线性化顺序；业务载荷和容量许可仍在累计量提交后才释放。
    ///
    /// 参数说明：
    /// - `commit_logical`: 若任务已有逻辑 owner，则在终态可见前恰好调用一次以减少对应计数。
    /// - `commit_accounting`: 接收即将发布的稳定终态，在任何可能唤醒容量等待者的资源释放前
    ///   提交不产生异常的原子累计量。
    ///
    /// 返回：全部权威复验、累计量和资源结算完成返回 Ok；物理位置、owner 或终态意图
    /// 不一致时保持 Terminating 并返回错误。
    pub(crate) fn finalize_termination_with_accounting(
        &self,
        commit_logical: impl FnOnce(LogicalOwner),
        commit_accounting: impl FnOnce(EntryState),
    ) -> Result<(), TaskAuthorityError> {
        let state = self.state()?;
        if state.is_terminal() {
            return Err(TaskAuthorityError::SettlementOwned);
        }
        if state != EntryState::Terminating {
            return Err(TaskAuthorityError::InvalidState);
        }
        let record = self
            .pending_terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .copied();
        let Some(record) = record else {
            // 现役结算者会在稳定终态发布前取走 pending 记录；迟到帮助者若同时观察到终态
            // owner、终态记录或稳定状态，说明结算权已有唯一持有者。
            if self.state().is_ok_and(EntryState::is_terminal)
                || self.owner().is_ok_and(Owner::is_terminal)
                || self.terminal.get().is_some()
            {
                return Err(TaskAuthorityError::SettlementOwned);
            }
            return Err(TaskAuthorityError::SettlementConflict);
        };
        if !record.state.is_terminal() {
            return Err(TaskAuthorityError::InvalidTerminal);
        }
        let current_retention = self.physical_retention()?;
        if !matches!(
            current_retention,
            PhysicalRetention::Detached
                | PhysicalRetention::Handling
                | PhysicalRetention::FailureHandling
        ) {
            return Err(TaskAuthorityError::RetentionMismatch);
        }
        let current_owner = self.owner()?;
        if current_owner.is_terminal() {
            return Err(TaskAuthorityError::SettlementOwned);
        }
        if self.terminal.get().is_some() {
            return Err(TaskAuthorityError::SettlementOwned);
        }
        if self
            .move_descriptor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            return Err(TaskAuthorityError::SettlementConflict);
        }
        let terminal_owner = Owner::for_terminal(record.state)?.encode()?;
        if let Err(observed) = self.owner.compare_exchange(
            current_owner.encode()?,
            terminal_owner,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // 多个合法帮助者可能同时读取到 Terminating 与原 owner；终态哨兵表示其中一个
            // 已取得唯一结算权，迟到帮助者只能退出，不能把正常竞争升级为类型失权。
            return if Owner::decode(observed).is_some_and(Owner::is_terminal) {
                Err(TaskAuthorityError::SettlementOwned)
            } else {
                Err(TaskAuthorityError::OwnerMismatch)
            };
        }
        // owner 哨兵先关闭执行与迁移权，随后才能解除物理持有责任并归还逻辑计数。
        self.physical_retention
            .store(PhysicalRetention::Detached as u8, Ordering::Release);
        let logical_owner = self
            .logical_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(logical_owner) = logical_owner {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                commit_logical(logical_owner)
            })) {
                // 逻辑减计没有取得成功证明时恢复 owner、retention 与逻辑位置，继续保持
                // Terminating；后续监督者可以携带新的内部计数动作重试，不能发布虚假终态。
                let _ = self.owner.compare_exchange(
                    terminal_owner,
                    current_owner.encode()?,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.physical_retention
                    .store(current_retention as u8, Ordering::Release);
                *self.logical_owner.lock().unwrap_or_else(|e| e.into_inner()) = Some(logical_owner);
                crate::run_isolated("任务终态逻辑计数归还", || {
                    std::panic::resume_unwind(payload)
                });
                return Err(TaskAuthorityError::AuthorityCorrupted);
            }
        }
        // 停机基线读取同一组原子累计量；先提交累计量再释放许可，容量等待者即使同步
        // 唤醒外部 Waker，也不能把已经线性化的终态跨到停机基线之后。
        commit_accounting(record.state);
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        let queued_permit = self
            .queued_permit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let global_permit = self
            .global_permit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        crate::run_isolated("任务终态载荷与许可归还", || {
            drop(task);
            drop(queued_permit);
            drop(global_permit);
        });
        self.terminal
            .set(record)
            .map_err(|_| TaskAuthorityError::SettlementConflict)?;
        self.pending_terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        // 所有计数、载荷、许可与物理引用均已收口后才允许观察到稳定终态。
        self.state.store(record.state as u8, Ordering::Release);
        crate::run_isolated("任务终态通知", || self.completed.notify_waiters());
        Ok(())
    }

    /// 业务作用：读取稳定终态原因；活动状态和正常完成没有原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：终态已经发布时返回其稳定原因，否则返回 None。
    pub(crate) fn reason(&self) -> Option<&'static str> {
        self.terminal.get().and_then(|record| record.reason)
    }

    /// 业务作用：等待稳定终态发布，供 `Submission::await_outcome` 避免轮询并抵抗通知窗口竞争。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Completed、Cancelled、Rejected 或 Failed；内部权威损坏返回 AuthorityCorrupted。
    pub(crate) async fn await_terminal(&self) -> Result<EntryState, TaskAuthorityError> {
        loop {
            let notified = self.completed.notified();
            let state = self.state()?;
            if state.is_terminal() {
                return Ok(state);
            }
            crate::shield_future(notified).await;
        }
    }

    /// 业务作用：执行封闭的活动状态迁移，所有竞争路径共享同一 CAS 线性化点。
    ///
    /// 参数说明：
    /// - `from`: 期望活动状态。
    /// - `to`: 目标活动状态。
    ///
    /// 返回：CAS 成功返回 Ok；当前状态已经变化返回 InvalidState。
    fn transition(&self, from: EntryState, to: EntryState) -> Result<(), TaskAuthorityError> {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| TaskAuthorityError::InvalidState)
    }

    /// 业务作用：安装唯一终态意图后竞争 Terminating，使终态类型与原因在结算窗口内不可被
    /// 其它路径替换。
    ///
    /// 参数说明：
    /// - `expected`: 期望活动状态。
    /// - `terminal`: 最终稳定终态。
    /// - `reason`: 失败、拒绝或取消的稳定原因；正常完成为 None。
    ///
    /// 返回：意图和 Terminating 均建立返回 Ok；竞争失败清除本次意图并返回错误。
    fn begin_termination(
        &self,
        expected: EntryState,
        terminal: EntryState,
        reason: Option<&'static str>,
    ) -> Result<(), TaskAuthorityError> {
        if !terminal.is_terminal()
            || (terminal == EntryState::Completed && reason.is_some())
            || (terminal != EntryState::Completed && reason.is_none())
        {
            return Err(TaskAuthorityError::InvalidTerminal);
        }
        let record = TerminalRecord {
            state: terminal,
            reason,
        };
        let mut pending = self
            .pending_terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if pending.is_some() {
            return Err(TaskAuthorityError::SettlementConflict);
        }
        *pending = Some(record);
        if self
            .state
            .compare_exchange(
                expected as u8,
                EntryState::Terminating as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            *pending = None;
            return Err(TaskAuthorityError::InvalidState);
        }
        Ok(())
    }

    /// 业务作用：复验移动票据属于当前描述符，不读取或接受历史 epoch。
    ///
    /// 参数说明：
    /// - `ticket`: 调用方持有的移动能力票据。
    ///
    /// 返回：票据一致时返回描述符副本；否则返回 MoveTicketMismatch。
    fn ensure_move(&self, ticket: MoveTicket) -> Result<MoveDescriptor, TaskAuthorityError> {
        let descriptor = self
            .move_descriptor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .copied()
            .ok_or(TaskAuthorityError::MoveTicketMismatch)?;
        if descriptor.move_epoch != ticket.epoch
            || descriptor.runner_generation != self.runner_generation
        {
            return Err(TaskAuthorityError::MoveTicketMismatch);
        }
        Ok(descriptor)
    }

    /// 业务作用：同时复验任务仍为 Moving 与票据 epoch，防止迟到帮助者写入下一笔移动。
    ///
    /// 参数说明：
    /// - `ticket`: 调用方持有的移动能力票据。
    ///
    /// 返回：当前活动描述符副本；状态稳定或票据迟到返回错误。
    fn active_move(&self, ticket: MoveTicket) -> Result<MoveDescriptor, TaskAuthorityError> {
        if self.state()? != EntryState::Moving {
            return Err(TaskAuthorityError::InvalidState);
        }
        self.ensure_move(ticket)
    }

    /// 业务作用：清除与票据完全匹配的移动描述符，使下一移动取得新 epoch。
    ///
    /// 参数说明：
    /// - `ticket`: 已撤销或已完成移动的票据。
    ///
    /// 返回：唯一匹配描述符被清除返回 Ok；迟到票据返回 MoveTicketMismatch。
    fn clear_move(&self, ticket: MoveTicket) -> Result<(), TaskAuthorityError> {
        let mut descriptor = self
            .move_descriptor
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if descriptor
            .as_ref()
            .is_some_and(|current| current.move_epoch == ticket.epoch)
        {
            *descriptor = None;
            Ok(())
        } else {
            Err(TaskAuthorityError::MoveTicketMismatch)
        }
    }

    /// 业务作用：移动结束后读取 Queued 或取消结算结果，避免取消请求与稳定状态发布交错时
    /// 把 Terminating 误报为可执行任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Queued 返回正常排队；取消结算返回 CancellationPending；其它状态返回权威错误。
    fn move_completion(&self) -> Result<MoveCompletion, TaskAuthorityError> {
        match self.state()? {
            EntryState::Queued => Ok(MoveCompletion::Queued),
            EntryState::Terminating => {
                let pending = self
                    .pending_terminal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if pending.as_ref().is_some_and(|record| {
                    record.state == EntryState::Cancelled
                        && record.reason == Some("cancelled_by_caller")
                }) {
                    Ok(MoveCompletion::CancellationPending)
                } else {
                    Err(TaskAuthorityError::SettlementConflict)
                }
            }
            _ => Err(TaskAuthorityError::InvalidState),
        }
    }

    /// 业务作用：以 CAS 转移物理持有责任，保证同一时刻只有一个容器或处理栈被记为权威。
    ///
    /// 参数说明：
    /// - `from`: 调用方必须当前独占的位置。
    /// - `to`: 完成本步骤后承担物理责任的位置。
    ///
    /// 返回：唯一转移成功返回 Ok；当前位置不一致返回 RetentionMismatch。
    fn change_retention(
        &self,
        from: PhysicalRetention,
        to: PhysicalRetention,
    ) -> Result<(), TaskAuthorityError> {
        self.physical_retention
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| TaskAuthorityError::RetentionMismatch)
    }
}

impl<T> Drop for TaskEntry<T> {
    /// 业务作用：最后一个 Arc 离场时隔离析构尚未交给执行任务的业务载荷与许可；正常终态已
    /// 清空这些字段，本路径仅承担启动回滚或权威失败后的最终资源兜底。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；尚未交付的业务载荷、类型许可和全局许可在隔离边界内释放。
    fn drop(&mut self) {
        let task = self
            .task
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let queued_permit = self
            .queued_permit
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let global_permit = self
            .global_permit
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        crate::run_isolated("任务条目最终资源归还", || {
            drop(task);
            drop(queued_permit);
            drop(global_permit);
        });
    }
}
