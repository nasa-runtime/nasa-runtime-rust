use crate::{Notification, Notify, NotifyError, NotifyReceipt};
use std::fmt;
use std::sync::{Arc, OnceLock};

/// 未显式指定命名渠道时使用的进程通知路由，不要求 YAML 声明 provider。
pub const DEFAULT_PROVIDER_ID: &str = "default";

static NOTIFIER: OnceLock<Arc<dyn Notify>> = OnceLock::new();

/// 进程通知实现已经由业务初始化，不允许覆盖正在使用的实现。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyInitialized;

impl fmt::Display for AlreadyInitialized {
    /// 业务作用：报告重复初始化而不暴露业务实现或渠道配置。
    /// 参数说明：`formatter` 为错误输出目标。
    /// 返回：写入稳定错误说明，不包含通知内容。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("notification implementation is already initialized")
    }
}

impl std::error::Error for AlreadyInitialized {}

/// 业务作用：由业务主动安装进程唯一通知实现，框架不创建客户端或解析通信协议。
/// 参数说明：`provider` 是业务实现的共享 Notify，可在应用启动前或业务初始化阶段设置。
/// 返回：首次设置成功；重复设置返回 AlreadyInitialized，保留已发布实现且不发送消息。
pub fn init(provider: Arc<dyn Notify>) -> Result<(), AlreadyInitialized> {
    // 一次发布后保持稳定，避免在途通知跨实现切换、凭据被替换或重试改投另一服务。
    NOTIFIER.set(provider).map_err(|_| AlreadyInitialized)
}

/// 业务作用：供调用方判断业务是否已经提供通知能力，不触发惰性初始化或外部 I/O。
/// 参数说明：无。
/// 返回：已初始化实现的共享引用；未设置时返回 None，不等待后续初始化。
pub fn get() -> Option<&'static Arc<dyn Notify>> {
    NOTIFIER.get()
}

/// 业务作用：有业务实现时直接异步调用，没有实现时忽略通知。
/// 参数说明：`notification` 是调用方已经裁决和限制内容的消息。
/// 返回：未初始化为 Ok(None)，已初始化为回执或业务实现返回的稳定错误；不自行排队、重试或启动任务。
/// 直接调用者负责超时与异常边界；SQL 告警由宿主 worker 调用并施加隔离，不能在 SQL 完成路径等待本函数。
pub async fn notify(notification: &Notification) -> Result<Option<NotifyReceipt>, NotifyError> {
    let Some(provider) = get() else {
        return Ok(None);
    };
    provider.notify(notification).await.map(Some)
}
