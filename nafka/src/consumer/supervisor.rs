//! group owner 句柄、控制命令 fencing 与健康快照共享域。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, Notify};

use crate::consumer::erased::ErasedConsumer;
use crate::consumer::registry::RegisteredPassthrough;
use crate::error::{NafkaError, Result};
use crate::health::{GroupHealth, GroupState};
use crate::types::Tp;
use crate::KafkaProxyInner;

/// 命令仍在队列中，允许调用端超时取消。
const COMMAND_QUEUED: u8 = 0;
/// owner 已开始执行，超时后结果未知。
const COMMAND_EXECUTING: u8 = 1;
/// 调用端在执行前成功取消。
const COMMAND_CANCELLED: u8 = 2;
/// owner 已完成执行。
const COMMAND_COMPLETED: u8 = 3;

/// owner 已完成本地构造，等待 registry/assign 统一放行。
const STARTUP_PENDING: u8 = 0;
/// registry 已原子登记全部 owner，可以开始消费。
const STARTUP_OPEN: u8 = 1;
/// 启动批次失败或生命周期已关闭，owner 必须退出。
const STARTUP_ABORTED: u8 = 2;

/// 同一轮 owner 启动共享的放行门。
///
/// 每个线程先完成 `BaseConsumer` 本地构造并回报结果；只有调用方把全部句柄原子登记到
/// 运行时后才打开门。任一构造失败时统一 abort，避免部分 group 提前进入 poll。
pub(crate) struct StartupGate {
    /// pending/open/aborted 状态。
    state: AtomicU8,
}

impl StartupGate {
    /// 创建尚未放行的启动门。
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(STARTUP_PENDING),
        }
    }

    /// 全部 owner 已登记后统一放行。
    pub(crate) fn open(&self) {
        let _ = self.state.compare_exchange(
            STARTUP_PENDING,
            STARTUP_OPEN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// 启动批次失败时阻止所有尚未运行的 owner 继续。
    pub(crate) fn abort(&self) {
        let _ = self.state.compare_exchange(
            STARTUP_PENDING,
            STARTUP_ABORTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// 启动门是否已经放行。
    pub(crate) fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) == STARTUP_OPEN
    }

    /// 启动门是否已经取消。
    pub(crate) fn is_aborted(&self) -> bool {
        self.state.load(Ordering::Acquire) == STARTUP_ABORTED
    }
}

/// owner 的订阅方式。
#[derive(Clone, Debug)]
pub(crate) enum OwnerMode {
    /// 通过 group coordinator 订阅 topic。
    Subscribe {
        /// 排序去重后的 topic 列表。
        topics: Vec<String>,
    },
    /// 独占固定分区，不参与 subscribe rebalance。
    Assign {
        /// 固定分区列表。
        partitions: Vec<Tp>,
        /// 每个分区的底层起始 offset 表达。
        starts: BTreeMap<Tp, crate::types::StartOffset>,
    },
}

/// 一个 resolved group 的冻结启动计划。
pub(crate) struct GroupPlan {
    /// 最终 group.id。
    pub(crate) group: String,
    /// owner 的订阅方式。
    pub(crate) mode: OwnerMode,
    /// 类型化 routes。
    pub(crate) typed: Vec<Arc<dyn ErasedConsumer>>,
    /// 同步借用 routes。
    pub(crate) passthrough: Vec<RegisteredPassthrough>,
}

/// owner 命令执行结果。
pub(crate) enum CommandReply {
    /// 无返回值的控制操作成功。
    Unit,
    /// position 查询结果。
    Positions(BTreeMap<Tp, Option<i64>>),
    /// assignment 查询结果。
    Assignment(Vec<Tp>),
}

/// owner 支持的有界控制命令。
pub(crate) enum OwnerCommandKind {
    /// 停止 owner。
    Stop,
    /// seek 到显式 offset。
    Seek {
        /// 目标分区与 offset。
        offsets: BTreeMap<Tp, i64>,
    },
    /// seek 到最早位置。
    SeekBeginning {
        /// 目标分区。
        tps: Vec<Tp>,
    },
    /// seek 到末尾位置。
    SeekEnd {
        /// 目标分区。
        tps: Vec<Tp>,
    },
    /// seek 到 broker 已提交位置。
    SeekCommitted {
        /// 目标分区。
        tps: Vec<Tp>,
    },
    /// seek 到按时间戳解析的位置。
    SeekTimestamp {
        /// 每分区目标毫秒时间戳。
        times: BTreeMap<Tp, i64>,
    },
    /// 增加业务暂停原因。
    Pause {
        /// 目标分区；空列表表示当前全部 assignment。
        tps: Vec<Tp>,
    },
    /// 移除业务暂停原因。
    Resume {
        /// 目标分区；空列表表示当前全部 assignment。
        tps: Vec<Tp>,
    },
    /// 查询 position。
    Position {
        /// 目标分区。
        tps: Vec<Tp>,
    },
    /// 查询 assignment。
    Assignment,
    /// 动态替换 subscribe topic 集合。
    Subscribe {
        /// 新 topic 列表。
        topics: Vec<String>,
    },
    /// 取消当前订阅并保留 owner。
    Unsubscribe,
    /// 清理本地失败状态并重新发起订阅或 assign。
    Restart,
}

/// 带执行状态和异步响应的 owner 命令信封。
pub(crate) struct OwnerCommand {
    /// 稳定命令 id。
    pub(crate) id: u64,
    /// queued/executing/cancelled/completed 状态。
    pub(crate) state: Arc<AtomicU8>,
    /// 具体命令。
    pub(crate) kind: OwnerCommandKind,
    /// 单次结果通道。
    pub(crate) reply: oneshot::Sender<Result<CommandReply>>,
}

/// 已启动 group 的共享句柄。
pub(crate) struct GroupHandle {
    /// 最终 group.id。
    group: Arc<str>,
    /// 有界 owner 命令发送端。
    commands: std::sync::mpsc::SyncSender<OwnerCommand>,
    /// 控制调用等待时长。
    control_timeout: Duration,
    /// 命令 id 生成器。
    next_command: AtomicU64,
    /// 当前健康快照。
    health: Arc<RwLock<GroupHealth>>,
    /// owner 是否已经完全退出。
    stopped: AtomicBool,
    /// owner 退出通知。
    stopped_notify: Notify,
    /// 只允许取走一次的线程 join handle。
    join: Mutex<Option<JoinHandle<()>>>,
    /// librdkafka 后台线程 broker 级错误单槽:consumer context 覆盖写,`health()` 读侧合并。
    global_error: Arc<Mutex<Option<String>>>,
}

impl GroupHandle {
    /// 启动一个 group owner 线程。
    ///
    /// # 参数
    ///
    /// - `runtime`: 所属 Kafka 运行时。
    /// - `plan`: 冻结 group 计划。
    ///
    /// # 错误
    ///
    /// 命令队列或线程构造失败时返回错误。
    pub(crate) fn start(
        runtime: Arc<KafkaProxyInner>,
        plan: GroupPlan,
        startup_gate: Arc<StartupGate>,
    ) -> Result<(Arc<Self>, oneshot::Receiver<Result<()>>)> {
        let capacity = runtime.config.behavior.command_queue_capacity;
        let control_timeout = Duration::from_millis(runtime.config.behavior.control_timeout_ms);
        let (commands, receiver) = std::sync::mpsc::sync_channel(capacity);
        let (startup_reply, startup_result) = oneshot::channel();
        let group: Arc<str> = Arc::from(plan.group.as_str());
        let health = Arc::new(RwLock::new(GroupHealth {
            group: plan.group.clone(),
            state: GroupState::Starting,
            assignment: Vec::new(),
            ready_assignment_epoch: None,
            last_assignment_at: None,
            paused: Vec::new(),
            last_poll_at: None,
            last_success_at: None,
            owner_session_started: 0,
            owner_session_restarted: 0,
            commit_attempted: 0,
            commit_succeeded: 0,
            commit_failed: 0,
            last_error: None,
        }));
        let global_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let handle = Arc::new(Self {
            group,
            commands,
            control_timeout,
            next_command: AtomicU64::new(1),
            health: Arc::clone(&health),
            stopped: AtomicBool::new(false),
            stopped_notify: Notify::new(),
            join: Mutex::new(None),
            global_error: Arc::clone(&global_error),
        });
        let weak_handle = Arc::downgrade(&handle);
        let health_for_thread = Arc::clone(&health);
        let group_for_name = handle.group.to_string();
        let join = std::thread::Builder::new()
            .name(format!("nafka-owner-{group_for_name}"))
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::consumer::poll_task::run_owner(
                        runtime,
                        plan,
                        receiver,
                        Arc::clone(&health_for_thread),
                        startup_gate,
                        startup_reply,
                        global_error,
                    )
                }));
                if let Err(payload) = outcome {
                    let detail = if let Some(value) = payload.downcast_ref::<&str>() {
                        (*value).to_owned()
                    } else if let Some(value) = payload.downcast_ref::<String>() {
                        value.clone()
                    } else {
                        "非字符串 panic payload".into()
                    };
                    let mut snapshot = health_for_thread
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    snapshot.state = GroupState::Crashed;
                    snapshot.last_error = Some(format!("owner panic: {detail}"));
                }
                if let Some(handle) = weak_handle.upgrade() {
                    handle.stopped.store(true, Ordering::Release);
                    handle.stopped_notify.notify_waiters();
                }
            })
            .map_err(|error| {
                NafkaError::Lifecycle(format!(
                    "group `{}` owner 线程创建失败: {error}",
                    handle.group
                ))
            })?;
        *handle
            .join
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(join);
        Ok((handle, startup_result))
    }

    /// 返回当前健康快照。
    ///
    /// 读侧合并:owner 状态机没写过 `last_error` 时,用 librdkafka 后台 callback 捕获的
    /// broker 级错误(ALL_BROKERS_DOWN/认证/SSL)兜底填充——join 迟迟不成时快照能给出真实根因,
    /// 且绝不覆盖状态机已写入的更具体主因。
    pub(crate) fn health(&self) -> GroupHealth {
        let mut snapshot = self
            .health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if snapshot.last_error.is_none() {
            snapshot.last_error = self
                .global_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
        }
        snapshot
    }

    /// 提交一条有界命令并按 fencing 状态等待结果。
    ///
    /// # 参数
    ///
    /// - `kind`: 具体 owner 命令。
    ///
    /// # 错误
    ///
    /// 队列满、owner 退出、确定未执行、结果未知或命令本身失败时返回错误。
    pub(crate) async fn command(&self, kind: OwnerCommandKind) -> Result<CommandReply> {
        self.command_for(kind, self.control_timeout).await
    }

    /// Drop 路径使用的非阻塞停止通知。
    ///
    /// 本方法只尝试把 fenced Stop 放入有界邮箱，不等待执行结果；队列已满时返回 false，
    /// 由调用方记录告警。显式关闭仍必须走 [`Self::stop_until`]。
    pub(crate) fn request_stop_best_effort(&self) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return true;
        }
        let id = self.next_command.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(AtomicU8::new(COMMAND_QUEUED));
        let (reply, _receiver) = oneshot::channel();
        self.commands
            .try_send(OwnerCommand {
                id,
                state,
                kind: OwnerCommandKind::Stop,
                reply,
            })
            .is_ok()
    }

    /// 在调用方给定的等待预算内提交一条 fenced 命令。
    ///
    /// # 参数
    ///
    /// - `kind`: 具体 owner 命令。
    /// - `timeout`: 本次等待预算。
    ///
    /// # 错误
    ///
    /// 队列满、owner 退出、确定未执行、结果未知或命令本身失败时返回错误。
    async fn command_for(&self, kind: OwnerCommandKind, timeout: Duration) -> Result<CommandReply> {
        let id = self.next_command.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(AtomicU8::new(COMMAND_QUEUED));
        let (reply, mut receiver) = oneshot::channel();
        let command = OwnerCommand {
            id,
            state: Arc::clone(&state),
            kind,
            reply,
        };
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                std::sync::mpsc::TrySendError::Full(_) => {
                    NafkaError::CommandQueueFull(self.group.to_string())
                }
                std::sync::mpsc::TrySendError::Disconnected(_) => {
                    NafkaError::NoSuchGroup(self.group.to_string())
                }
            })?;
        match tokio::time::timeout(timeout, &mut receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(classify_closed_command(id, &state)),
            Err(_) => {
                if state
                    .compare_exchange(
                        COMMAND_QUEUED,
                        COMMAND_CANCELLED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    Err(NafkaError::ControlNotApplied(id))
                } else {
                    match state.load(Ordering::Acquire) {
                        COMMAND_COMPLETED => match receiver.try_recv() {
                            Ok(result) => result,
                            Err(_) => Err(NafkaError::ControlOutcomeUnknown(id)),
                        },
                        COMMAND_CANCELLED => Err(NafkaError::ControlNotApplied(id)),
                        COMMAND_EXECUTING | COMMAND_QUEUED => {
                            Err(NafkaError::ControlOutcomeUnknown(id))
                        }
                        _ => Err(NafkaError::ControlOutcomeUnknown(id)),
                    }
                }
            }
        }
    }

    /// 在全局 deadline 前停止并 join owner 线程。
    ///
    /// # 参数
    ///
    /// - `deadline`: shutdown 共享的绝对截止时刻。
    ///
    /// # 错误
    ///
    /// 命令或线程退出超过截止时刻时返回关闭超时。
    pub(crate) async fn stop_until(&self, deadline: Instant) -> Result<()> {
        if !self.stopped.load(Ordering::Acquire) {
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match self
                    .command_for(OwnerCommandKind::Stop, remaining.min(self.control_timeout))
                    .await
                {
                    Ok(_) => break,
                    // ControlOutcomeUnknown 的语义恰恰是"可能没执行"。把它当成"已停止"
                    // 就会在 Stop 真的丢失时不再重发：owner 按 Transient 自动重建，
                    // group 在 ShuttingDown 之后继续消费，而 shutdown 只能超时。
                    // 这里退避后继续重发，直到 stopped 或 deadline 到期。
                    Err(NafkaError::ControlOutcomeUnknown(_))
                    | Err(NafkaError::ControlNotApplied(_))
                    | Err(NafkaError::CommandQueueFull(_)) => {
                        if self.stopped.load(Ordering::Acquire) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(NafkaError::NoSuchGroup(_)) if self.stopped.load(Ordering::Acquire) => {
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
        while !self.stopped.load(Ordering::Acquire) {
            // notified() 必须先于 double-check 创建：它在**构造那一刻**就快照了 notify_waiters
            // 调用计数，首次 poll 时比对计数即可发现"通知发生在构造之后"。因此 owner 线程恰好在
            // double-check 与 .await 之间退出并 notify_waiters 时通知不会丢失，已退出的 owner
            // 不会被误报成 ShutdownTimeout。调整这两行的顺序会引入真实竞态。
            let notified = self.stopped_notify.notified();
            if self.stopped.load(Ordering::Acquire) {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return Err(NafkaError::ShutdownTimeout {
                    groups: vec![self.group.to_string()],
                    producer_lanes: Vec::new(),
                });
            }
        }
        let join = self
            .join
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(join) = join {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, tokio::task::spawn_blocking(move || join.join()))
                    .await
                    .is_err()
            {
                return Err(NafkaError::ShutdownTimeout {
                    groups: vec![self.group.to_string()],
                    producer_lanes: Vec::new(),
                });
            }
        }
        Ok(())
    }

    /// owner 取命令时执行 queued→executing fencing。
    ///
    /// # 参数
    ///
    /// - `command`: 待执行命令。
    ///
    /// # 返回
    ///
    /// 调用端尚未取消时返回 true。
    pub(crate) fn begin_command(command: &OwnerCommand) -> bool {
        command
            .state
            .compare_exchange(
                COMMAND_QUEUED,
                COMMAND_EXECUTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// owner 完成命令并发送结果。
    ///
    /// # 参数
    ///
    /// - `command`: 已进入 executing 的命令。
    /// - `result`: 执行结果。
    pub(crate) fn finish_command(command: OwnerCommand, result: Result<CommandReply>) {
        let state = Arc::clone(&command.state);
        let _ = command.reply.send(result);
        // 结果必须先进入 oneshot，再以 Release 发布 Completed；超时调用方 Acquire
        // 观察到 Completed 后即可用 try_recv 读取确定结果。
        state.store(COMMAND_COMPLETED, Ordering::Release);
    }
}

/// 更新健康快照中的状态和可选错误。
///
/// # 参数
///
/// - `health`: owner 共享健康状态。
/// - `state`: 新 group 状态。
/// - `error`: 已脱敏的可选错误文本。
pub(crate) fn update_state(
    health: &Arc<RwLock<GroupHealth>>,
    state: GroupState,
    error: Option<String>,
) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.state = state;
    snapshot.last_error = error;
}

/// oneshot 关闭时按 fenced command 状态区分确定未执行与结果未知。
///
/// - `id`: 稳定命令编号。
/// - `state`: 与 owner 共享的单状态 CAS。
fn classify_closed_command(id: u64, state: &AtomicU8) -> NafkaError {
    if state
        .compare_exchange(
            COMMAND_QUEUED,
            COMMAND_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
    {
        return NafkaError::ControlNotApplied(id);
    }
    match state.load(Ordering::Acquire) {
        COMMAND_CANCELLED => NafkaError::ControlNotApplied(id),
        COMMAND_EXECUTING | COMMAND_COMPLETED | COMMAND_QUEUED => {
            NafkaError::ControlOutcomeUnknown(id)
        }
        _ => NafkaError::ControlOutcomeUnknown(id),
    }
}
