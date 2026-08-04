//! 进程级终态降级扩展点。
//!
//! 端点没有声明局部降级时，并发拒绝与执行超时进入这里。处理器只负责同步生成最终响应，
//! 不再叠加并发、超时或第二次业务降级，避免形成“降级后再降级”的递归保护链。

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::response::Response;

/// 触发降级的运行时原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FallbackCause {
    /// 端点并发许可已经用尽，请求未进入业务执行体。
    BulkheadRejected {
        /// 当前命令允许的最大业务并发数。
        max_concurrent: usize,
        /// 拒绝发生时正在执行的业务请求数。
        current_inflight: usize,
    },
    /// 端点业务执行超过声明时限，原执行 future 已被取消。
    ExecutionTimeout {
        /// 端点声明的业务执行时限。
        timeout: Duration,
        /// 从业务执行开始到超时判定的实际耗时。
        elapsed: Duration,
    },
}

/// 交给业务全局降级处理器的稳定请求视图。
#[derive(Debug, Clone)]
pub struct FallbackContext {
    command: String,
    group: String,
    path: String,
    transaction_weight: Option<u64>,
    cause: FallbackCause,
}

impl FallbackContext {
    /// 业务作用：冻结一次降级事件的命令元数据，使终态处理不再借用命令对象。
    ///
    /// # 参数说明
    ///
    /// - `command`: 面板和指标使用的命令名。
    /// - `group`: 命令分组。
    /// - `path`: 真实 REST 路由；无法取得时由调用方回退到命令名。
    /// - `transaction_weight`: REST 事务权重；`None` 表示不计入 TPS。
    /// - `cause`: 本次触发降级的运行时原因。
    ///
    /// # 返回
    ///
    /// 返回拥有全部字符串的只读上下文。
    pub(crate) fn new(
        command: &str,
        group: &str,
        path: &str,
        transaction_weight: Option<u64>,
        cause: FallbackCause,
    ) -> Self {
        Self {
            command: command.to_owned(),
            group: group.to_owned(),
            path: path.to_owned(),
            transaction_weight,
            cause,
        }
    }

    /// 业务作用：返回面板与指标使用的稳定命令名。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回本次降级事件所属的命令名。
    pub fn command(&self) -> &str {
        &self.command
    }

    /// 业务作用：返回命令分组，供业务按领域选择终态响应。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回本次降级事件所属的命令分组。
    pub fn group(&self) -> &str {
        &self.group
    }

    /// 业务作用：返回真实 REST 路由，供统一响应和低基数观测使用。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回真实路由；运行时无法取得路由时返回命令名。
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 业务作用：返回 REST 事务权重，不改变本次请求已经完成的 TPS 计数。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// `Some(weight)` 表示该端点每次请求贡献的事务权重，`None` 表示非事务端点。
    pub fn transaction_weight(&self) -> Option<u64> {
        self.transaction_weight
    }

    /// 业务作用：返回并发拒绝或执行超时原因，供业务选择终态响应。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回本次降级事件的不可变原因快照。
    pub fn cause(&self) -> FallbackCause {
        self.cause
    }
}

/// 业务全局降级处理器对当前事件的处理决定。
pub enum FallbackDecision {
    /// 已生成完整 HTTP 响应，运行时直接返回给调用方。
    Respond(Response),
    /// 当前处理器不接管该事件，运行时使用内置 429 或 504 响应。
    UseBuiltin,
}

/// 业务手动实现的进程级终态降级扩展点。
pub trait GlobalFallbackHandler: Send + Sync + 'static {
    /// 业务作用：为没有局部降级配置的并发拒绝或执行超时同步生成最终响应。
    ///
    /// # 参数说明
    ///
    /// - `context`: 已冻结的命令、路由、事务权重和触发原因。
    ///
    /// # 返回
    ///
    /// 返回完整响应或明确退回组件内置响应；实现不得发起阻塞或异步外部调用。
    fn handle(&self, context: FallbackContext) -> FallbackDecision;
}

/// 属性宏写入组件级链接切片的终态处理描述。
#[doc(hidden)]
pub struct CollectedGlobalFallback {
    /// 声明位置，用于确定性冲突诊断。
    pub source: &'static str,
    /// 已擦除具体响应类型的同步终态处理入口。
    pub handle: fn(FallbackContext) -> FallbackDecision,
}

/// 当前组件链接进最终程序的全部全局终态处理声明。
#[linkme::distributed_slice]
#[doc(hidden)]
pub static HYSTRIX_COLLECTED_GLOBAL_FALLBACKS: [CollectedGlobalFallback];

/// 安装全局降级处理器时可能出现的唯一性错误。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum GlobalFallbackInstallError {
    /// 当前进程已经手动安装过处理器，不允许静默替换。
    AlreadyInstalled,
    /// 已存在属性宏收集项，不能再手动安装另一个实现。
    CollectedHandlerPresent {
        /// 已收集实现的静态声明位置。
        handler: &'static str,
    },
    /// 同一组件收集到多个实现，无法隐式选择其中一个。
    MultipleCollectedHandlers {
        /// 按声明位置排序后的冲突实现。
        handlers: Vec<&'static str>,
    },
    /// 手动实现已经安装，无法再启用属性宏收集项。
    ManualHandlerPresent,
    /// 链接切片的声明位置与描述项不一致，拒绝执行不完整入口。
    CollectedHandlerUnavailable {
        /// 无法解析到描述项的静态声明位置。
        handler: &'static str,
    },
}

impl std::fmt::Display for GlobalFallbackInstallError {
    /// 业务作用：输出不包含请求数据的稳定安装冲突说明。
    ///
    /// # 参数说明
    ///
    /// - `formatter`: 标准格式化输出目标。
    ///
    /// # 返回
    ///
    /// 返回格式化结果，不泄露处理器内部状态。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInstalled => formatter.write_str("全局降级处理器已经手动安装"),
            Self::CollectedHandlerPresent { handler } => {
                write!(
                    formatter,
                    "已收集全局降级处理器 {handler}，不能重复手动安装"
                )
            }
            Self::MultipleCollectedHandlers { handlers } => write!(
                formatter,
                "同一组件只能声明一个 #[global_fallback]，当前冲突项: {}",
                handlers.join(", ")
            ),
            Self::ManualHandlerPresent => {
                formatter.write_str("已手动安装全局降级处理器，不能再启用自动收集项")
            }
            Self::CollectedHandlerUnavailable { handler } => {
                write!(formatter, "全局降级处理器 {handler} 的链接描述不可用")
            }
        }
    }
}

impl std::error::Error for GlobalFallbackInstallError {}

enum GlobalFallbackHandlerKind {
    Manual(Arc<dyn GlobalFallbackHandler>),
    Collected(fn(FallbackContext) -> FallbackDecision),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobalFallbackOrigin {
    Manual,
    Collected(&'static str),
}

struct GlobalFallbackRuntime {
    handler: GlobalFallbackHandlerKind,
    origin: GlobalFallbackOrigin,
}

static GLOBAL_FALLBACK: OnceLock<GlobalFallbackRuntime> = OnceLock::new();

thread_local! {
    static GLOBAL_FALLBACK_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

struct GlobalFallbackGuard;

impl Drop for GlobalFallbackGuard {
    /// 业务作用：无论处理器正常返回还是崩溃，都解除当前线程的递归门禁。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 无返回值；只恢复线程局部执行标记。
    fn drop(&mut self) {
        GLOBAL_FALLBACK_ACTIVE.with(|active| active.set(false));
    }
}

/// 业务作用：按声明位置生成稳定的自动收集冲突列表。
///
/// # 参数说明
///
/// 参数说明: 无。
///
/// # 返回
///
/// 返回已排序的静态声明位置；不会修改链接切片。
fn collected_sources() -> Vec<&'static str> {
    let mut sources = HYSTRIX_COLLECTED_GLOBAL_FALLBACKS
        .iter()
        .map(|entry| entry.source)
        .collect::<Vec<_>>();
    sources.sort_unstable();
    sources
}

/// 安装进程级全局降级处理器。
///
/// 业务作用：无属性宏声明时冻结一个手动 trait 实现，作为当前组件唯一的终态处理器。
///
/// # 参数说明
///
/// - `handler`: 业务实现的共享终态处理器。
///
/// # 返回
///
/// 首次且不存在自动收集项时返回成功；重复安装或与属性宏声明冲突时返回明确错误。
pub fn install_global_fallback(
    handler: Arc<dyn GlobalFallbackHandler>,
) -> Result<(), GlobalFallbackInstallError> {
    let sources = collected_sources();
    match sources.as_slice() {
        [] => {}
        [source] => {
            return Err(GlobalFallbackInstallError::CollectedHandlerPresent { handler: source })
        }
        _ => {
            return Err(GlobalFallbackInstallError::MultipleCollectedHandlers { handlers: sources })
        }
    }
    GLOBAL_FALLBACK
        .set(GlobalFallbackRuntime {
            handler: GlobalFallbackHandlerKind::Manual(handler),
            origin: GlobalFallbackOrigin::Manual,
        })
        .map_err(|_| GlobalFallbackInstallError::AlreadyInstalled)
}

/// 初始化属性宏自动收集的全局降级处理器。
///
/// 业务作用：确定性验证当前组件只有一个 `#[global_fallback]`，并把它冻结为进程级终态入口。
///
/// # 参数说明
///
/// 参数说明: 无。
///
/// # 返回
///
/// 没有声明或唯一声明已就绪时返回成功；多个声明或与手动实现冲突时返回明确错误。
pub fn initialize_global_fallback() -> Result<(), GlobalFallbackInstallError> {
    let sources = collected_sources();
    let source = match sources.as_slice() {
        [] => return Ok(()),
        [source] => *source,
        _ => {
            return Err(GlobalFallbackInstallError::MultipleCollectedHandlers { handlers: sources })
        }
    };
    if let Some(runtime) = GLOBAL_FALLBACK.get() {
        return match runtime.origin {
            GlobalFallbackOrigin::Collected(existing) if existing == source => Ok(()),
            GlobalFallbackOrigin::Collected(_) => {
                Err(GlobalFallbackInstallError::MultipleCollectedHandlers {
                    handlers: collected_sources(),
                })
            }
            GlobalFallbackOrigin::Manual => Err(GlobalFallbackInstallError::ManualHandlerPresent),
        };
    }
    let Some(entry) = HYSTRIX_COLLECTED_GLOBAL_FALLBACKS
        .iter()
        .find(|entry| entry.source == source)
    else {
        return Err(GlobalFallbackInstallError::CollectedHandlerUnavailable { handler: source });
    };
    if GLOBAL_FALLBACK
        .set(GlobalFallbackRuntime {
            handler: GlobalFallbackHandlerKind::Collected(entry.handle),
            origin: GlobalFallbackOrigin::Collected(source),
        })
        .is_ok()
    {
        return Ok(());
    }
    match GLOBAL_FALLBACK.get().map(|runtime| runtime.origin) {
        Some(GlobalFallbackOrigin::Collected(existing)) if existing == source => Ok(()),
        Some(GlobalFallbackOrigin::Collected(_)) => {
            Err(GlobalFallbackInstallError::MultipleCollectedHandlers {
                handlers: collected_sources(),
            })
        }
        Some(GlobalFallbackOrigin::Manual) | None => {
            Err(GlobalFallbackInstallError::ManualHandlerPresent)
        }
    }
}

/// 查询当前进程是否已经安装全局降级处理器。
///
/// 业务作用：触发自动收集初始化，并供启动审计确认终态降级能力是否就绪。
///
/// # 参数说明
///
/// 参数说明: 无。
///
/// # 返回
///
/// 唯一实现已成功安装返回 `true`；没有实现或存在配置冲突返回 `false`。
pub fn global_fallback_installed() -> bool {
    initialize_global_fallback().is_ok() && GLOBAL_FALLBACK.get().is_some()
}

/// 全局降级执行结果，供命令运行时选择业务响应、内置响应并记录原因。
pub(crate) enum GlobalFallbackExecution {
    Handled(Response),
    UseBuiltin,
    NotInstalled,
    InvalidConfiguration,
    Panicked,
    Recursive,
}

/// 业务作用：获取当前线程的终态处理执行权，防止处理器间接递归进入自身。
///
/// # 参数说明
///
/// 参数说明: 无。
///
/// # 返回
///
/// 首次进入返回守卫；已经处于处理器内部时返回 `None`，且不改变现有门禁。
fn try_enter_global_fallback() -> Option<GlobalFallbackGuard> {
    GLOBAL_FALLBACK_ACTIVE.with(|active| {
        if active.replace(true) {
            None
        } else {
            Some(GlobalFallbackGuard)
        }
    })
}

/// 业务作用：调用当前组件的唯一全局终态处理器，并把配置冲突、崩溃或递归收敛为内置响应信号。
///
/// # 参数说明
///
/// - `context`: 已冻结的降级事件上下文。
///
/// # 返回
///
/// 返回细分执行结果；处理器只运行一次，失败后不再进入任何业务降级链。
pub(crate) fn execute_global_fallback(context: FallbackContext) -> GlobalFallbackExecution {
    if initialize_global_fallback().is_err() {
        return GlobalFallbackExecution::InvalidConfiguration;
    }
    let Some(runtime) = GLOBAL_FALLBACK.get() else {
        return GlobalFallbackExecution::NotInstalled;
    };
    let Some(_guard) = try_enter_global_fallback() else {
        return GlobalFallbackExecution::Recursive;
    };
    let decision = catch_unwind(AssertUnwindSafe(|| match &runtime.handler {
        GlobalFallbackHandlerKind::Manual(handler) => handler.handle(context),
        GlobalFallbackHandlerKind::Collected(handler) => handler(context),
    }));
    match decision {
        Ok(FallbackDecision::Respond(response)) => GlobalFallbackExecution::Handled(response),
        Ok(FallbackDecision::UseBuiltin) => GlobalFallbackExecution::UseBuiltin,
        Err(_) => GlobalFallbackExecution::Panicked,
    }
}
