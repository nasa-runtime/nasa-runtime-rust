use std::sync::{Arc, OnceLock, RwLock, Weak};

use crate::application::ApplicationInner;
use crate::{Application, ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 全局兼容入口只保留弱引用，不延长应用生命周期；锁串行化实例发布与按身份清除。
static GLOBAL_APPLICATION: OnceLock<RwLock<Weak<ApplicationInner>>> = OnceLock::new();

/// 业务作用：发布当前 Service 的迁移期全局 Weak 句柄。
///
/// 参数说明：
///
/// - `application`：已经封存资源并即将进入 Ready 的 Service Application。
///
/// 返回：槽为空或旧弱引用无法升级时安装当前弱引用；仍指向任一存活应用时返回 Ready 错误，包括重复安装同一实例。
/// 不持有应用强引用，不自行检查模式或就绪状态，发布时机由 Runner 保证。
pub(crate) fn install(application: &Application) -> ApplicationResult<()> {
    let slot = GLOBAL_APPLICATION.get_or_init(|| RwLock::new(Weak::new()));
    let mut current = slot
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // 存活实例仍拥有全局入口，拒绝覆盖以免不同应用的能力在同一进程内混用。
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

/// 业务作用：尝试升级当前全局 Weak 句柄。
///
/// 参数说明：无。
///
/// 返回：槽已初始化且目标仍存活时返回延长其存活期的 Application 强句柄，否则为 None；不复验业务资源是否仍开放。
/// 取得句柄后仍可能进入停机，各业务能力继续执行自身生命周期门禁。
pub(crate) fn get() -> Option<Application> {
    let slot = GLOBAL_APPLICATION.get()?;
    let inner = slot
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .upgrade()?;
    Some(Application {
        inner,
    })
}

/// 业务作用：仅在全局槽仍指向目标 Application 时清除它。
///
/// 参数说明：
///
/// - `application`：即将进入业务资源 Closing 的当前实例。
///
/// 返回：仅当槽内仍可升级且与目标为同一实例时清空弱引用；未初始化、已失效或已指向其它实例时保持原状。
/// 不停止目标应用、不释放外部已取得的强句柄，不能以清槽代替资源关闭。
pub(crate) fn clear(application: &Application) {
    let Some(slot) = GLOBAL_APPLICATION.get() else {
        return;
    };
    let mut current = slot
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // 旧应用的延迟停机不能清除后来发布的实例，必须在持锁期间核对实例身份。
    if current
        .upgrade()
        .is_some_and(|inner| Arc::ptr_eq(&inner, &application.inner))
    {
        *current = Weak::new();
    }
}
