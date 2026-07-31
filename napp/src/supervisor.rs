use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::{Id as TokioTaskId, JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 受管任务注册通道容量。
///
/// 固定上限为突发注册提供缓冲，同时让发送端在监督器处理不过来时形成背压，避免无界增长。
pub const SUPERVISOR_QUEUE_CAPACITY: usize = 64;

/// 统一交给监督器托管的异步任务。
pub(crate) type ManagedTaskFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'static>>;

/// Runner 为受管任务分配的进程内稳定标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    /// 返回任务标识的数值形式，供日志和诊断信息建立关联。
    ///
    /// # 参数
    ///
    /// - `self`：需要读取的任务标识。
    pub fn get(self) -> u64 {
        self.0
    }
}

/// 受管任务的业务角色，决定退出事件应如何影响应用生命周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskKind {
    UserHook,
    Critical,
    Background,
}

/// 用户任务组的生命周期状态；Runner 单线程写入并据此分类任务退出。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskGroupState {
    Running,
    Stopping,
}

/// 一次待确认的任务注册请求。
///
/// 请求只有被 Runner 接收并加入任务集合后才会确认，调用方因此不会把“进入通道”误认为“已经受管”。
pub(crate) struct TaskRegistration {
    pub(crate) name: Arc<str>,
    pub(crate) kind: TaskKind,
    pub(crate) future: ManagedTaskFuture,
    pub(crate) acknowledged: oneshot::Sender<ApplicationResult<TaskId>>,
}

/// 业务侧提交受管任务的轻量句柄。
///
/// 该句柄不持有任务集合，只能通过有界通道请求 Runner 完成注册。
#[derive(Clone)]
pub(crate) struct SupervisorClient {
    sender: mpsc::Sender<TaskRegistration>,
    registration_open: Arc<AtomicBool>,
    task_group_token: CancellationToken,
}

impl SupervisorClient {
    /// 注册一个受管任务，并等待 Runner 返回稳定任务标识。
    ///
    /// # 参数
    ///
    /// - `self`：连接当前应用任务监督器的客户端。
    /// - `name`：应用内唯一的任务名称，用于去重和故障定位。
    /// - `kind`：任务的业务角色，用于决定异常退出策略。
    /// - `future`：由监督器启动、收割和取消的任务主体。
    pub(crate) async fn register(
        &self,
        name: Arc<str>,
        kind: TaskKind,
        future: ManagedTaskFuture,
    ) -> ApplicationResult<TaskId> {
        if !self.registration_open.load(Ordering::Acquire) {
            return Err(supervisor_error("managed task registration is closed"));
        }

        let (acknowledged, accepted) = oneshot::channel();
        self.sender
            .send(TaskRegistration {
                name,
                kind,
                future,
                acknowledged,
            })
            .await
            .map_err(|_| supervisor_error("managed task supervisor is unavailable"))?;
        accepted
            .await
            .map_err(|_| supervisor_error("managed task registration was not acknowledged"))?
    }

    /// 为一个新受管任务创建组级取消令牌。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回的子令牌只接收 Runner 的组级停机广播，任务自行取消不会反向取消整组。
    pub(crate) fn task_token(&self) -> CancellationToken {
        self.task_group_token.child_token()
    }
}

/// 任务主体结束后写回 `JoinSet` 的原始结果。
struct TaskExit {
    id: TaskId,
    result: anyhow::Result<()>,
}

/// 与运行时任务标识关联的业务元数据。
///
/// 元数据与执行结果分离保存，使 panic 或取消时仍能恢复业务名称和任务角色。
#[derive(Clone)]
struct TaskMeta {
    id: TaskId,
    name: Arc<str>,
    kind: TaskKind,
    abort_handle: tokio::task::AbortHandle,
}

/// 监督器归一化后的任务退出结果。
pub(crate) enum TaskOutcome {
    Completed,
    Failed(anyhow::Error),
    Panicked,
    Cancelled,
}

/// 一次已完成任务及其业务上下文。
pub(crate) struct TaskCompletion {
    pub(crate) id: TaskId,
    pub(crate) name: Arc<str>,
    pub(crate) kind: TaskKind,
    pub(crate) outcome: TaskOutcome,
}

/// Runner 等待监督器时可能收到的事件。
pub(crate) enum SupervisorEvent {
    RegistrationAccepted,
    RegistrationChannelClosed,
    TaskCompleted(TaskCompletion),
}

/// `JoinSet` 只有 Runner 持有；业务侧只持有 bounded sender，并等待接收确认。
pub(crate) struct TaskSupervisor {
    receiver: mpsc::Receiver<TaskRegistration>,
    registration_open: Arc<AtomicBool>,
    tasks: JoinSet<TaskExit>,
    task_meta: HashMap<TokioTaskId, TaskMeta>,
    task_names: HashSet<Arc<str>>,
    next_id: u64,
    task_group_state: TaskGroupState,
    task_group_token: CancellationToken,
}

impl TaskSupervisor {
    /// 创建业务侧注册句柄与 Runner 独占的监督器。
    ///
    /// # 参数
    ///
    /// 本函数无参数；返回值的两端共享注册开关和任务组取消源。
    pub(crate) fn channel() -> (SupervisorClient, Self) {
        let (sender, receiver) = mpsc::channel(SUPERVISOR_QUEUE_CAPACITY);
        let registration_open = Arc::new(AtomicBool::new(true));
        let task_group_token = CancellationToken::new();
        (
            SupervisorClient {
                sender,
                registration_open: registration_open.clone(),
                task_group_token: task_group_token.clone(),
            },
            Self {
                receiver,
                registration_open,
                tasks: JoinSet::new(),
                task_meta: HashMap::new(),
                task_names: HashSet::new(),
                next_id: 1,
                task_group_state: TaskGroupState::Running,
                task_group_token,
            },
        )
    }

    /// 等待下一次注册或任务退出事件。
    ///
    /// # 参数
    ///
    /// - `self`：由 Runner 独占推进的任务监督器。
    pub(crate) async fn next_event(&mut self) -> SupervisorEvent {
        if self.tasks.is_empty() {
            return match self.receiver.recv().await {
                Some(registration) => {
                    self.accept(registration);
                    SupervisorEvent::RegistrationAccepted
                }
                None => SupervisorEvent::RegistrationChannelClosed,
            };
        }

        /// 同一次公平等待中先到达的内部事件。
        enum Next {
            Registration(Option<TaskRegistration>),
            Joined(Option<Result<(TokioTaskId, TaskExit), JoinError>>),
        }
        let next = {
            let receiver = &mut self.receiver;
            let tasks = &mut self.tasks;
            tokio::select! {
                registration = receiver.recv() => Next::Registration(registration),
                joined = tasks.join_next_with_id() => Next::Joined(joined),
            }
        };
        match next {
            Next::Registration(Some(registration)) => {
                self.accept(registration);
                SupervisorEvent::RegistrationAccepted
            }
            Next::Registration(None) => SupervisorEvent::RegistrationChannelClosed,
            Next::Joined(Some(joined)) => match self.complete_joined(joined) {
                Some(completion) => SupervisorEvent::TaskCompleted(completion),
                None => SupervisorEvent::RegistrationChannelClosed,
            },
            Next::Joined(None) => SupervisorEvent::RegistrationChannelClosed,
        }
    }

    /// 把用户启动钩子直接纳入监督器，并返回其稳定标识。
    ///
    /// 启动钩子使用保留名称且不走注册通道，保证它在业务注册开放前就已受管。
    ///
    /// # 参数
    ///
    /// - `self`：由 Runner 独占推进的任务监督器。
    /// - `future`：用户启动钩子的异步主体。
    pub(crate) fn spawn_user_hook(&mut self, future: ManagedTaskFuture) -> TaskId {
        let name: Arc<str> = Arc::from("user-hook");
        let inserted = self.task_names.insert(name.clone());
        debug_assert!(inserted, "the reserved user-hook task must be unique");
        self.spawn_task(name, TaskKind::UserHook, future)
    }

    /// 在组件 Ready action 已压栈后直接登记一个框架关键任务。
    ///
    /// 该入口由 Runner 单线程调用，不经过已经关闭的业务注册通道；任务一旦加入就会参与同一退出分类和强制收割。
    ///
    /// # 参数
    ///
    /// - `self`：由 Runner 独占推进的任务监督器。
    /// - `name`：组件关键任务在应用内唯一的稳定名称。
    /// - `future`：拥有终端运行资源并在停止通知后结束的任务主体。
    pub(crate) fn spawn_component_critical(
        &mut self,
        name: &'static str,
        future: ManagedTaskFuture,
    ) -> ApplicationResult<TaskId> {
        let name: Arc<str> = Arc::from(name);
        if name.trim().is_empty() {
            return Err(supervisor_error("component task name cannot be empty"));
        }
        if !self.task_names.insert(name.clone()) {
            return Err(supervisor_error(format!(
                "managed task `{name}` is already registered"
            )));
        }
        Ok(self.spawn_task(name, TaskKind::Critical, future))
    }

    /// 接受并确认一个注册请求，或把拒绝原因回传给调用方。
    ///
    /// # 参数
    ///
    /// - `self`：由 Runner 独占推进的任务监督器。
    /// - `registration`：包含名称、角色、任务主体和确认通道的注册请求。
    fn accept(&mut self, registration: TaskRegistration) {
        if !self.registration_open.load(Ordering::Acquire) {
            let _ = registration
                .acknowledged
                .send(Err(supervisor_error("managed task registration is closed")));
            return;
        }
        if registration.acknowledged.is_closed() {
            return;
        }
        if !self.task_names.insert(registration.name.clone()) {
            let _ = registration.acknowledged.send(Err(supervisor_error(format!(
                "managed task `{}` is already registered",
                registration.name
            ))));
            return;
        }

        // 先加入任务集合再确认，这是“注册成功”的线性化点；确认返回后任务必然可被清理路径收割。
        let id = self.spawn_task(registration.name, registration.kind, registration.future);
        let _ = registration.acknowledged.send(Ok(id));
    }

    /// 把任务加入运行时集合，并建立运行时标识到业务元数据的映射。
    ///
    /// # 参数
    ///
    /// - `self`：由 Runner 独占推进的任务监督器。
    /// - `name`：应用内唯一的任务名称。
    /// - `kind`：任务的业务角色。
    /// - `future`：需要被启动和收割的任务主体。
    fn spawn_task(&mut self, name: Arc<str>, kind: TaskKind, future: ManagedTaskFuture) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        let abort_handle = self.tasks.spawn(async move {
            TaskExit {
                id,
                result: future.await,
            }
        });
        let meta = TaskMeta {
            id,
            name,
            kind,
            abort_handle: abort_handle.clone(),
        };
        self.task_meta.insert(abort_handle.id(), meta);
        id
    }

    /// 等待并归一化下一项任务完成结果。
    ///
    /// # 参数
    ///
    /// - `self`：由 Runner 独占推进的任务监督器。
    pub(crate) async fn join_next(&mut self) -> Option<TaskCompletion> {
        let joined = self.tasks.join_next_with_id().await?;
        self.complete_joined(joined)
    }

    /// 将运行时完成结果还原为带业务上下文的任务完成事件。
    ///
    /// # 参数
    ///
    /// - `self`：持有运行时标识与业务元数据映射的监督器。
    /// - `joined`：`JoinSet` 返回的成功、失败、取消或 panic 结果。
    fn complete_joined(
        &mut self,
        joined: Result<(TokioTaskId, TaskExit), JoinError>,
    ) -> Option<TaskCompletion> {
        match joined {
            Ok((tokio_id, exit)) => {
                let meta = self.task_meta.remove(&tokio_id)?;
                debug_assert_eq!(meta.id, exit.id);
                self.task_names.remove(&meta.name);
                Some(TaskCompletion {
                    id: exit.id,
                    name: meta.name,
                    kind: meta.kind,
                    outcome: match exit.result {
                        Ok(()) => TaskOutcome::Completed,
                        Err(error) => TaskOutcome::Failed(error),
                    },
                })
            }
            Err(error) => {
                let meta = self.task_meta.remove(&error.id())?;
                self.task_names.remove(&meta.name);
                Some(TaskCompletion {
                    id: meta.id,
                    name: meta.name,
                    kind: meta.kind,
                    outcome: if error.is_cancelled() {
                        TaskOutcome::Cancelled
                    } else {
                        TaskOutcome::Panicked
                    },
                })
            }
        }
    }

    /// 判断监督器中是否仍有尚未收割的任务。
    ///
    /// # 参数
    ///
    /// - `self`：需要检查的任务监督器。
    pub(crate) fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// 将用户任务组切换为停机态并广播取消。
    ///
    /// 状态写入先于令牌取消，这是任务退出分类的线性化点：Runner 在该点之后收割到的退出均属于预期停机。
    ///
    /// # 参数
    ///
    /// - `self`：需要切换到停机态的任务监督器。
    pub(crate) fn begin_stopping(&mut self) {
        if self.task_group_state == TaskGroupState::Running {
            self.task_group_state = TaskGroupState::Stopping;
            self.task_group_token.cancel();
            // UserHook 没有业务任务令牌；启动中断时必须显式 abort，避免它占满整个清理预算。
            for task in self.task_meta.values() {
                if task.kind == TaskKind::UserHook {
                    task.abort_handle.abort();
                }
            }
        }
    }

    /// 返回用户任务组当前的退出分类状态。
    ///
    /// # 参数
    ///
    /// - `self`：需要读取状态的任务监督器。
    pub(crate) fn task_group_state(&self) -> TaskGroupState {
        self.task_group_state
    }

    /// 永久关闭注册入口，并拒绝通道中尚未处理的请求。
    ///
    /// 先发布关闭状态再关闭接收端，使并发发送方可以在入队前快速失败；随后逐项确认拒绝，避免调用方悬挂。
    ///
    /// # 参数
    ///
    /// - `self`：需要关闭注册入口的任务监督器。
    pub(crate) async fn close_registration(&mut self) {
        self.registration_open.store(false, Ordering::Release);
        self.receiver.close();
        while let Some(registration) = self.receiver.recv().await {
            let _ = registration
                .acknowledged
                .send(Err(supervisor_error("managed task registration is closed")));
        }
    }

    /// 强制终止剩余任务，并在给定剩余预算内收割运行时结果。
    ///
    /// 返回 `true` 表示 `JoinSet` 已清空；返回 `false` 表示仍有不合作任务，调用方应记录未优雅
    /// 收割并继续退场。不能在全局 deadline 之后无界等待，因为任务取消只能在 future 再次让出时生效。
    ///
    /// # 参数
    ///
    /// - `self`：需要终止剩余任务的监督器。
    /// - `budget`：当前全局停机截止时间尚余的时长。
    pub(crate) async fn abort_and_drain(&mut self, budget: Duration) -> bool {
        self.tasks.abort_all();
        if self.tasks.is_empty() {
            return true;
        }
        let drained =
            tokio::time::timeout(budget, async { while self.join_next().await.is_some() {} })
                .await
                .is_ok();
        drained && self.tasks.is_empty()
    }
}

/// 构造任务监督阶段的统一应用错误。
///
/// # 参数
///
/// - `message`：不含敏感数据的稳定诊断信息。
fn supervisor_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Supervisor, ApplicationPhase::UserHook, message)
}
