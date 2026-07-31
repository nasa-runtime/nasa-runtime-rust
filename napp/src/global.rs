use std::sync::{Arc, OnceLock, RwLock, Weak};

use crate::application::ApplicationInner;
use crate::{Application, ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

static GLOBAL_APPLICATION: OnceLock<RwLock<Weak<ApplicationInner>>> = OnceLock::new();

/// 发布当前 Service 的迁移期全局 Weak 句柄。
///
/// # 参数
///
/// - `application`：已经封存资源并即将进入 Ready 的 Service Application。
pub(crate) fn install(application: &Application) -> ApplicationResult<()> {
    let slot = GLOBAL_APPLICATION.get_or_init(|| RwLock::new(Weak::new()));
    let mut current = slot
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if current.upgrade().is_some() {
        return Err(ApplicationError::new(
            ComponentId::Application,
            ApplicationPhase::Ready,
            "another live application is already published for global access",
        ));
    }
    // 在同一写锁临界区完成存活检查与替换，两个并发 Runner 不可能都观察到空槽后同时发布。
    *current = Arc::downgrade(&application.inner);
    Ok(())
}

/// 尝试升级当前全局 Weak 句柄。
///
/// # 参数
///
/// 本函数无参数；槽为空或 Application 已释放时返回 `None`。
pub(crate) fn get() -> Option<Application> {
    let slot = GLOBAL_APPLICATION.get()?;
    let inner = slot
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .upgrade()?;
    Some(Application { inner })
}

/// 仅在全局槽仍指向目标 Application 时清除它。
///
/// # 参数
///
/// - `application`：即将进入业务资源 Closing 的当前实例。
pub(crate) fn clear(application: &Application) {
    let Some(slot) = GLOBAL_APPLICATION.get() else {
        return;
    };
    let mut current = slot
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if current
        .upgrade()
        .is_some_and(|inner| Arc::ptr_eq(&inner, &application.inner))
    {
        *current = Weak::new();
    }
}
