//! 可撤销的命令目录、配置归属与集中周期观测。

use crate::{Command, IsolationCfg, IsolationRule, IsolationTable};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, OnceLock, Weak,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

static OWNER: Mutex<Option<Arc<ManagedState>>> = Mutex::new(None);
static BUILD_GATE: Mutex<()> = Mutex::new(());
static GENERATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct ManagedState {
    pub(crate) generation: u64,
    pub(crate) limit: usize,
    gate: Mutex<bool>,
    active: AtomicUsize,
    idle: tokio::sync::Notify,
    cancel: CancellationToken,
    completion: Mutex<Completion>,
    done: tokio::sync::watch::Sender<Option<bool>>,
    task_done: tokio::sync::watch::Receiver<Option<bool>>,
}
struct Completion {
    tasks: usize,
    failed: bool,
    published: bool,
}
struct TaskGuard {
    inner: Arc<ManagedState>,
    succeeded: bool,
}
/// 持有当前应用命令面资源；独立模式不需要创建此 owner。
pub struct ManagedRuntime {
    inner: Arc<ManagedState>,
}

/// 受管目录安装或关闭的确定性失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedError {
    /// 已有受管 owner 或独立运行资源占用进程级命令入口。
    Conflict,
    /// 规则、目录容量或运行环境不满足受管启动条件。
    InvalidConfiguration,
    /// 命令身份重复、容量不足或命令不属于当前 owner，不能加入目录。
    DirectoryRejected,
    /// 所属 owner 已关闭，新调用不能借用其它实例的运行资源。
    Closed,
    /// 观测或收尾任务异常结束，未获得正常退出结论。
    TaskFailed,
}
impl std::fmt::Display for ManagedError {
    /// 业务作用：输出固定失败类别，不包含路由或业务材料。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：稳定错误文本。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Conflict => "hystrix owner conflict",
            Self::InvalidConfiguration => "invalid managed hystrix configuration",
            Self::DirectoryRejected => "hystrix command directory rejected",
            Self::Closed => "hystrix runtime closed",
            Self::TaskFailed => "hystrix observer task failed",
        })
    }
}
impl std::error::Error for ManagedError {}

/// 业务作用：串行裁决独立构造、受管安装与撤销，避免命令误归下一代。
/// 参数说明：无。
/// 返回：短期全局所有权锁，持有期间不得 await。
pub(crate) fn lock_owner() -> MutexGuard<'static, Option<Arc<ManagedState>>> {
    OWNER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ManagedRuntime {
    /// 业务作用：校验隔离规则并独占本代目录，建立可等待的集中周期观测。
    /// 参数说明：`rules` 为固定隔离规则；`context_path` 为前缀；`max_commands` 为本代命令数量上限。
    /// 返回：可撤销 owner；独立状态冲突、非法规则或静态命令目录冲突时失败。
    pub async fn start(
        rules: &HashMap<String, IsolationRule>,
        context_path: &str,
        max_commands: usize,
    ) -> Result<Self, ManagedError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ManagedError::InvalidConfiguration);
        }
        if !(1..=4096).contains(&max_commands)
            || rules.len() > max_commands
            || context_path.len() > 512
            || !context_path.is_empty() && !context_path.starts_with('/')
        {
            return Err(ManagedError::InvalidConfiguration);
        }
        let mut trie = matchit::Router::new();
        for (pattern, rule) in rules {
            if pattern.len() > 512 || rule.max_concurrent > 65536 || rule.timeout_ms > 3600000 {
                return Err(ManagedError::InvalidConfiguration);
            }
            trie.insert(
                crate::normalize_pattern(pattern),
                IsolationCfg {
                    pattern: pattern.clone(),
                    max_concurrent: rule.max_concurrent,
                    timeout: Duration::from_millis(rule.timeout_ms),
                    tps_weight: rule.tps_weight,
                },
            )
            .map_err(|_| ManagedError::InvalidConfiguration)?;
        }
        let inner = {
            let _construction = BUILD_GATE.lock().unwrap();
            let mut owner = lock_owner();
            if owner.is_some()
                || !crate::lock_registry().is_empty()
                || crate::isolation().lock().unwrap().is_some()
                || crate::fallback::has_manual_fallback()
            {
                return Err(ManagedError::Conflict);
            }
            crate::fallback::initialize_global_fallback().map_err(|_| ManagedError::Conflict)?;
            let (done, task_done) = tokio::sync::watch::channel(None);
            let inner = Arc::new(ManagedState {
                generation: GENERATION.fetch_add(1, Ordering::AcqRel),
                limit: max_commands,
                gate: Mutex::new(true),
                active: AtomicUsize::new(0),
                idle: tokio::sync::Notify::new(),
                cancel: CancellationToken::new(),
                completion: Mutex::new(Completion {
                    tasks: 2,
                    failed: false,
                    published: false,
                }),
                done,
                task_done,
            });
            *crate::isolation().lock().unwrap() = Some(Arc::new(IsolationTable {
                owner: Some(Arc::downgrade(&inner)),
                trie,
                ctx_prefix: context_path.trim_end_matches('/').into(),
                commands: dashmap::DashMap::new(),
            }));
            *owner = Some(inner.clone());
            inner
        };
        let runtime = Self {
            inner: inner.clone(),
        };
        let cancel = inner.cancel.clone();
        let generation = inner.generation;
        // 守卫在 spawn 前建立；执行器销毁未轮询的任务时也必须归还责任并关闭旧代准入。
        let mut observer_guard = TaskGuard {
            inner: inner.clone(),
            succeeded: false,
        };
        let mut owner_guard = TaskGuard {
            inner: inner.clone(),
            succeeded: false,
        };
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(crate::WINDOW_SECS));
            timer.tick().await;
            loop {
                tokio::select! {biased;_=cancel.cancelled()=>break,_=timer.tick()=>{}}
                let commands = crate::lock_registry().clone();
                for command in commands {
                    if command
                        .owner
                        .as_ref()
                        .and_then(Weak::upgrade)
                        .is_some_and(|owner| owner.generation == generation)
                    {
                        command.log_cost();
                    }
                }
            }
            observer_guard.succeeded = true;
            drop(observer_guard);
        });
        // 收尾 owner 独立于调用者，观察任务与在途调用结束前不允许下一代接管全局状态。
        tokio::spawn(async move {
            let success = task.await.is_ok();
            *inner.gate.lock().unwrap() = false;
            inner.cancel.cancel();
            loop {
                let notified = inner.idle.notified();
                if inner.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
            owner_guard.succeeded = success;
            drop(owner_guard);
        });
        // 宏描述只持有代码身份，运行实例按本代构造并在 Ready 前验证目录冲突。
        for descriptor in COLLECTED_COMMANDS {
            if let Err(error) = descriptor.slot.get(descriptor.factory) {
                let _ = runtime.shutdown().await;
                return Err(error);
            }
        }
        Ok(runtime)
    }
    /// 业务作用：在本代目录中创建显式命令，并将重复或超量变成确定性错误。
    /// 参数说明：`name`、`group` 为固定身份；`rule` 为并发、超时和 TPS 权重。
    /// 返回：登记成功的命令；关闭、身份重复或容量耗尽时拒绝。
    pub fn command(
        &self,
        name: &str,
        group: &str,
        rule: &IsolationRule,
    ) -> Result<Arc<Command>, ManagedError> {
        if rule.max_concurrent > 65536 || rule.timeout_ms > 3600000 {
            return Err(ManagedError::InvalidConfiguration);
        }
        let _guard = self.inner.enter()?;
        let command = Command::build(
            name,
            group,
            Some(rule.max_concurrent),
            Some(Duration::from_millis(rule.timeout_ms)),
            rule.tps_weight,
        );
        if !command.admitted.load(Ordering::Acquire) {
            return Err(ManagedError::DirectoryRejected);
        }
        Ok(command)
    }
    /// 业务作用：识别周期观测异常或关闭后的准入状态。
    /// 参数说明：无。
    /// 返回：当前代次仍接受调用时为 true。
    pub fn is_running(&self) -> bool {
        self.inner.is_open()
    }

    /// 业务作用：持有命令 owner 的准入保护完成宿主最终接流裁决。
    /// 参数说明：`publish` 必须短小同步、不阻塞、不重入当前 owner 或命令。
    /// 返回：准入仍开放时执行并返回 Some，否则不执行；关闭与周期任务退出使用同一保护。
    pub fn with_running<T>(&self, publish: impl FnOnce() -> T) -> Option<T> {
        let gate = self.inner.gate.lock().unwrap();
        // 关闭后的命令 owner 不能成为宿主接流依据，且不能借最终复验重新开放准入。
        if !*gate {
            return None;
        }
        Some(publish())
    }
    /// 业务作用：关闭本代新命令与调用准入，保留已接纳业务的执行责任。
    /// 参数说明：无。
    /// 返回：准入永久关闭，周期观测收到取消信号。
    pub fn begin_shutdown(&self) {
        *self.inner.gate.lock().unwrap() = false;
        self.inner.cancel.cancel();
    }
    /// 业务作用：等待周期任务与已接纳业务完成后撤销本代目录和隔离表。
    /// 参数说明：无。
    /// 返回：真实退出并撤销成功；观察任务异常时明确失败。
    pub async fn shutdown(&self) -> Result<(), ManagedError> {
        self.begin_shutdown();
        let mut done = self.inner.task_done.clone();
        loop {
            let outcome = *done.borrow_and_update();
            if let Some(success) = outcome {
                return if success {
                    Ok(())
                } else {
                    Err(ManagedError::TaskFailed)
                };
            }
            done.changed().await.map_err(|_| ManagedError::TaskFailed)?;
        }
    }
    /// 业务作用：观察本代目录和运行责任，不将周期任务创建视为退出证明。
    /// 参数说明：无。
    /// 返回：命令数量、在途调用数与周期观测是否结束。
    pub fn snapshot(&self) -> (usize, usize, bool) {
        (
            crate::lock_registry()
                .iter()
                .filter(|command| {
                    command
                        .owner
                        .as_ref()
                        .and_then(Weak::upgrade)
                        .is_some_and(|state| state.generation == self.inner.generation)
                })
                .count(),
            self.inner.active.load(Ordering::Acquire),
            self.inner.task_done.borrow().is_some(),
        )
    }
}
impl Drop for ManagedRuntime {
    /// 业务作用：启动回滚或 owner 释放时拒绝旧代调用，通知独立收尾任务。
    /// 参数说明：无。
    /// 返回：关闭准入；全局引用由独立收尾任务在真实退出后撤销。
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// 业务作用：实际收口后只撤销仍归本代的全局状态，旧 owner 不影响后来安装者。
/// 参数说明：`inner` 为已结束观察与业务责任的当前代次。
/// 返回：本代目录强引用和隔离状态被移除。
fn revoke(inner: &Arc<ManagedState>) {
    let mut owner = lock_owner();
    if owner
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, inner))
    {
        crate::lock_registry().retain(|command| {
            command
                .owner
                .as_ref()
                .and_then(Weak::upgrade)
                .is_none_or(|state| state.generation != inner.generation)
        });
        *crate::isolation().lock().unwrap() = None;
        *owner = None;
    }
}

pub(crate) struct CallGuard(Arc<ManagedState>);
impl ManagedState {
    /// 业务作用：与关闭同步裁决一次命令执行，并记录在途责任。
    /// 参数说明：无。
    /// 返回：开放时提供归还守卫，关闭后拒绝执行业务。
    pub(crate) fn enter(self: &Arc<Self>) -> Result<CallGuard, ManagedError> {
        let open = self.gate.lock().unwrap();
        if !*open {
            return Err(ManagedError::Closed);
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        Ok(CallGuard(self.clone()))
    }
    /// 业务作用：检查本代是否仍允许登记新的运行命令。
    /// 参数说明：无。
    /// 返回：关闭后永久为 false。
    pub(crate) fn is_open(&self) -> bool {
        *self.gate.lock().unwrap()
    }
    /// 业务作用：周期任务、收尾任务和业务责任都归还后撤销本代，覆盖执行器强制销毁。
    /// 参数说明：无。
    /// 返回：仅一个调用者发布留存终态；任务取消或 panic 保留失败结果。
    fn finish_if_idle(self: &Arc<Self>) {
        let success = {
            let mut completion = self.completion.lock().unwrap();
            if completion.tasks != 0
                || self.active.load(Ordering::Acquire) != 0
                || completion.published
            {
                return;
            }
            completion.published = true;
            !completion.failed
        };
        // 两个任务均已归还责任且准入关闭，旧业务不能再执行，也不能跨代撤销新 owner。
        revoke(self);
        self.done.send_replace(Some(success));
    }
}
impl Drop for TaskGuard {
    /// 业务作用：任务完成或 future 被销毁时归还本代责任，不依赖执行器继续调度收尾任务。
    /// 参数说明：无。
    /// 返回：先关闭新调用，再记录任务结局；已有业务仍持有 owner，直到最后一个调用归还。
    fn drop(&mut self) {
        *self.inner.gate.lock().unwrap() = false;
        self.inner.cancel.cancel();
        {
            let mut completion = self.inner.completion.lock().unwrap();
            completion.failed |= !self.succeeded;
            completion.tasks -= 1;
        }
        self.inner.finish_if_idle();
    }
}
impl Drop for CallGuard {
    /// 业务作用：业务完成、取消或 panic 后归还本代在途责任。
    /// 参数说明：无。
    /// 返回：计数归零时唤醒关闭等待者。
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
            self.0.finish_if_idle();
        }
    }
}

/// 宏的进程级代码槽；受管模式只缓存弱引用和代次，避免旧实例被静态 Arc 永久保留。
pub struct CommandSlot {
    managed: Mutex<Option<(u64, Weak<Command>)>>,
    standalone: OnceLock<Arc<Command>>,
}
impl CommandSlot {
    /// 业务作用：创建不持有任何运行资源的静态宏槽。
    /// 参数说明：无。
    /// 返回：空代码槽，不创建任务。
    pub const fn new() -> Self {
        Self {
            managed: Mutex::new(None),
            standalone: OnceLock::new(),
        }
    }
    /// 业务作用：按当前 owner 代次取得命令，独立模式保持进程级缓存合同。
    /// 参数说明：`factory` 为宏生成的固定命令构造函数。
    /// 返回：本代命令；无 owner 的旧受管槽或无效目录拒绝调用。
    pub fn get(&self, factory: fn() -> Arc<Command>) -> Result<Arc<Command>, ManagedError> {
        let _construction = BUILD_GATE.lock().unwrap();
        let current = lock_owner().clone();
        // 构造也属于本代责任；关闭不能在工厂完成前撤销 owner 并把命令登记到独立目录。
        let _reservation = current.as_ref().map(|owner| owner.enter()).transpose()?;
        let mut slot = self.managed.lock().unwrap();
        match current {
            Some(owner) => {
                if !owner.is_open() {
                    return Err(ManagedError::Closed);
                }
                if let Some((generation, command)) = &*slot {
                    if *generation == owner.generation {
                        if let Some(command) = command.upgrade() {
                            return Ok(command);
                        }
                    }
                }
                let command = factory();
                if !command.admitted.load(Ordering::Acquire)
                    || command
                        .owner
                        .as_ref()
                        .and_then(Weak::upgrade)
                        .is_none_or(|state| state.generation != owner.generation)
                {
                    return Err(ManagedError::DirectoryRejected);
                }
                *slot = Some((owner.generation, Arc::downgrade(&command)));
                Ok(command)
            }
            None if slot.is_some() => Err(ManagedError::Closed),
            None => Ok(self.standalone.get_or_init(factory).clone()),
        }
    }
}
impl Default for CommandSlot {
    /// 业务作用：提供空宏槽的标准构造。
    /// 参数说明：无。
    /// 返回：尚未绑定任何运行实例的槽。
    fn default() -> Self {
        Self::new()
    }
}
/// 宏静态描述只保存构造代码，不保存跨 Application 的运行实例。
pub struct CollectedCommand {
    /// 属性函数对应的静态缓存槽，仅以弱引用保存受管命令。
    pub slot: &'static CommandSlot,
    /// 按属性策略构造命令的同步工厂，由当前 owner 接管生成实例。
    pub factory: fn() -> Arc<Command>,
}
/// 属性宏汇总的命令描述；受管启动时统一装配并校验目录身份与容量。
#[linkme::distributed_slice]
pub static COLLECTED_COMMANDS: [CollectedCommand];
