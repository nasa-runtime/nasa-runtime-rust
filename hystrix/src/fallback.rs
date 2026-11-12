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
    /// 本次降级所属的稳定命令身份，用于响应选择和观测归属。
    command: String,
    /// 命令声明的分组，使全局处理器可按领域区分策略。
    group: String,
    /// 构造时固定的路由标识；无法取得路由时由调用方使用命令名。
    path: String,
    /// 本次请求的事务统计权重，None 表示不计入事务端点统计。
    transaction_weight: Option<u64>,
    /// 并发拒绝或超时发生时的原因快照，处理器无需重复判断运行状态。
    cause: FallbackCause,
}

impl FallbackContext {
    /// 业务作用：冻结一次降级事件的命令元数据，使终态处理不再借用命令对象。
    ///
    /// 参数说明：
    /// - `command`: 面板和指标使用的命令名。
    /// - `group`: 命令分组。
    /// - `path`: 真实 REST 路由；无法取得时由调用方回退到命令名。
    /// - `transaction_weight`: REST 事务权重；`None` 表示不计入 TPS。
    /// - `cause`: 本次触发降级的运行时原因。
    ///
    /// 返回：拥有命令、分组和路由字符串的只读上下文，保留本次权重及原因快照，不执行降级处理。
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
    /// 参数说明：无。
    ///
    /// 返回：借用本次事件冻结的命令名，不受后续命令目录变化影响。
    pub fn command(&self) -> &str {
        &self.command
    }

    /// 业务作用：返回命令分组，供业务按领域选择终态响应。
    ///
    /// 参数说明：无。
    ///
    /// 返回：借用本次事件冻结的命令分组，不读取请求内容或改变处理策略。
    pub fn group(&self) -> &str {
        &self.group
    }

    /// 业务作用：返回真实 REST 路由，供统一响应和低基数观测使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：借用构造时记录的路由标识；无法取得真实路由时由调用方传入命令名，本方法不重新解析请求。
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 业务作用：返回 REST 事务权重，不改变本次请求已经完成的 TPS 计数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：Some 表示本次请求使用的事务权重，None 表示非事务端点；只返回快照，不追加统计。
    pub fn transaction_weight(&self) -> Option<u64> {
        self.transaction_weight
    }

    /// 业务作用：返回并发拒绝或执行超时原因，供业务选择终态响应。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本次降级的原因及对应并发量或时间快照，不重复执行准入或超时判断。
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
    /// 参数说明：
    /// - `context`: 已冻结的命令、路由、事务权重和触发原因。
    ///
    /// 返回：完整响应或明确退回组件内置响应；实现不得发起阻塞或异步外部调用，也不得再次进入业务降级链。
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
    /// 受管运行已冻结全局降级归属，不接纳不可撤销的手动运行实例。
    ManagedRuntimeActive,
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
    /// 参数说明：
    /// - `formatter`: 标准格式化输出目标。
    ///
    /// 返回：安装错误说明的写入结果；只包含固定原因和声明位置，不读取处理器内部状态。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ManagedRuntimeActive => {
                formatter.write_str("受管运行不允许安装进程级手动降级实例")
            }
            Self::AlreadyInstalled => formatter.write_str("全局降级处理器已经手动安装"),
            Self::CollectedHandlerPresent {
                handler,
            } => {
                write!(
                    formatter,
                    "已收集全局降级处理器 {handler}，不能重复手动安装"
                )
            }
            Self::MultipleCollectedHandlers {
                handlers,
            } => write!(
                formatter,
                "同一组件只能声明一个 #[global_fallback]，当前冲突项: {}",
                handlers.join(", ")
            ),
            Self::ManualHandlerPresent => {
                formatter.write_str("已手动安装全局降级处理器，不能再启用自动收集项")
            }
            Self::CollectedHandlerUnavailable {
                handler,
            } => {
                write!(formatter, "全局降级处理器 {handler} 的链接描述不可用")
            }
        }
    }
}

impl std::error::Error for GlobalFallbackInstallError {}

enum GlobalFallbackHandlerKind {
    /// 进程级手动实现，持有业务对象，不能由受管 owner 撤销。
    Manual(Arc<dyn GlobalFallbackHandler>),
    /// 链接时收集的静态同步函数，不持有应用运行实例。
    Collected(fn(FallbackContext) -> FallbackDecision),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobalFallbackOrigin {
    /// 手动安装来源，拒绝与自动声明或受管运行资源混用。
    Manual,
    /// 自动声明的静态位置，用于幂等初始化与冲突判断。
    Collected(&'static str),
}

/// 业务作用：把全局降级处理器与其安装来源绑定，保证进程内只发布一份可追溯运行时。
struct GlobalFallbackRuntime {
    /// 唯一终态处理入口，只同步调用一次，不叠加第二条降级链。
    handler: GlobalFallbackHandlerKind,
    /// 安装来源身份，用于判定重复初始化是否仍指向同一声明。
    origin: GlobalFallbackOrigin,
}

static GLOBAL_FALLBACK: OnceLock<GlobalFallbackRuntime> = OnceLock::new();

thread_local! {
    static GLOBAL_FALLBACK_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// 业务作用：在一次全局降级调用期间持有线程局部递归门禁，并在离开作用域时解除门禁。
struct GlobalFallbackGuard;

impl Drop for GlobalFallbackGuard {
    /// 业务作用：在处理器正常返回或 panic 展开后解除线程局部递归门禁，允许后续独立事件进入。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前线程恢复为未执行全局降级的状态，不改变处理器注册或重新执行业务。
    fn drop(&mut self) {
        GLOBAL_FALLBACK_ACTIVE.with(|active| active.set(false));
    }
}

/// 业务作用：按声明位置生成稳定的自动收集冲突列表。
///
/// 参数说明：无。
///
/// 返回：按位置排序的静态声明列表，保留重复项以暴露安装冲突，不修改链接切片。
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
/// 参数说明：
/// - `handler`: 业务实现的共享终态处理器。
///
/// 返回：没有受管 owner、自动收集项或既有手动安装时成功；否则返回明确冲突错误且不替换处理器。
/// 手动处理器由进程级槽永久持有，不具备受管运行时的撤销语义。
pub fn install_global_fallback(
    handler: Arc<dyn GlobalFallbackHandler>,
) -> Result<(), GlobalFallbackInstallError> {
    let owner = crate::managed::lock_owner();
    // 手动实现不可撤销，受管 owner 存在时不能把应用资源挂入永久进程槽。
    if owner.is_some() {
        return Err(GlobalFallbackInstallError::ManagedRuntimeActive);
    }
    let sources = collected_sources();
    // 自动声明与手动实例必须唯一，不能靠启动先后顺序隐式挑选终态策略。
    match sources.as_slice() {
        [] => {}
        [source] => {
            return Err(GlobalFallbackInstallError::CollectedHandlerPresent {
                handler: source,
            })
        }
        _ => {
            return Err(GlobalFallbackInstallError::MultipleCollectedHandlers {
                handlers: sources,
            })
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
/// 参数说明：无。
///
/// 返回：无自动声明、唯一声明首次安装或已安装同一声明时成功；多声明、手动实现冲突或缺少链接描述时失败。
/// 无自动声明时不改变已有手动实现；并发安装完成后再次核对归属，不静默替换处理器。
pub fn initialize_global_fallback() -> Result<(), GlobalFallbackInstallError> {
    let sources = collected_sources();
    let source = match sources.as_slice() {
        [] => return Ok(()),
        [source] => *source,
        _ => {
            return Err(GlobalFallbackInstallError::MultipleCollectedHandlers {
                handlers: sources,
            })
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
        return Err(GlobalFallbackInstallError::CollectedHandlerUnavailable {
            handler: source,
        });
    };
    // 初始化可能与其他线程竞争；发布失败后复验实际来源，只有同一声明才视为幂等成功。
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
/// 参数说明：无。
///
/// 返回：自动声明检查成功且存在已安装处理器时为 true；未安装或声明冲突时为 false，不调用处理器。
pub fn global_fallback_installed() -> bool {
    initialize_global_fallback().is_ok() && GLOBAL_FALLBACK.get().is_some()
}

/// 业务作用：识别持有运行资源的手动进程级降级实现，避免受管 owner 误认其可撤销。
///
/// 参数说明：无。
///
/// 返回：已有手动实现时为 true，静态收集函数不计入。
pub(crate) fn has_manual_fallback() -> bool {
    GLOBAL_FALLBACK
        .get()
        .is_some_and(|runtime| runtime.origin == GlobalFallbackOrigin::Manual)
}

/// 全局降级执行结果，供命令运行时选择业务响应、内置响应并记录原因。
pub(crate) enum GlobalFallbackExecution {
    /// 处理器已提供完整响应，命令运行时直接使用。
    Handled(Response),
    /// 处理器明确让运行时采用内置响应。
    UseBuiltin,
    /// 没有安装处理器，运行时选择内置响应。
    NotInstalled,
    /// 声明或安装归属冲突，拒绝任意选择业务处理器。
    InvalidConfiguration,
    /// 同步处理器发生可捕获的 panic，运行时不再次降级。
    Panicked,
    /// 当前线程已在全局处理器内，阻止递归调用。
    Recursive,
}

/// 业务作用：获取当前线程的终态处理执行权，防止处理器间接递归进入自身。
///
/// 参数说明：无。
///
/// 返回：首次进入时置位并返回解除门禁的守卫；递归调用返回 None，保持外层执行标记。
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
/// 参数说明：
/// - `context`: 已冻结的降级事件上下文。
///
/// 返回：业务响应、内置响应选择或未安装、配置冲突、递归、panic 的分类结果；处理器至多调用一次。
/// panic 只在展开模式下被捕获，不重试处理器，也不继续业务降级链。
pub(crate) fn execute_global_fallback(context: FallbackContext) -> GlobalFallbackExecution {
    if initialize_global_fallback().is_err() {
        return GlobalFallbackExecution::InvalidConfiguration;
    }
    let Some(runtime) = GLOBAL_FALLBACK.get() else {
        return GlobalFallbackExecution::NotInstalled;
    };
    // 终态处理不得递归进入自身；拒绝递归时保持外层守卫，避免提前解除保护。
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
