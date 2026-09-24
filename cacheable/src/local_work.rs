//! 本地刷新和异步失效共用的有界任务 owner。

use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::Semaphore;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

static CURRENT: RwLock<Option<Arc<LocalWork>>> = RwLock::new(None);

/// 纯 L1 缓存的刷新任务 owner；与两级 CacheRuntimeGuard 互斥安装。
pub struct LocalCacheRuntimeGuard(Arc<LocalWork>);

impl LocalCacheRuntimeGuard {
    /// 业务作用：为独立 L1 宏调用提供有界异步刷新责任，不建立 Redis 连接。
    /// 参数说明：无。
    /// 返回：唯一 owner；已有本地或两级缓存 owner 时拒绝。
    pub fn install() -> anyhow::Result<Self> {
        Ok(Self(LocalWork::install()?))
    }

    /// 业务作用：关闭刷新准入并等待全部在途 future 释放。
    /// 参数说明：无。
    /// 返回：实际任务数归零后完成；取消等待不会重开准入。
    pub async fn shutdown(&self) {
        self.0.close();
        self.0.wait().await;
    }
}

impl Drop for LocalCacheRuntimeGuard {
    /// 业务作用：收回本代刷新入口，避免旧 owner 影响后续应用。
    /// 参数说明：无。
    /// 返回：同步取消任务，不声称任务已经退出。
    fn drop(&mut self) {
        self.0.close();
    }
}

pub(crate) struct LocalWork {
    closed: Mutex<bool>,
    capacity: Arc<Semaphore>,
    cancel: CancellationToken,
    tasks: TaskTracker,
}

impl LocalWork {
    /// 业务作用：为当前缓存运行态安装唯一有界刷新 owner。
    /// 参数说明：无。
    /// 返回：最多 128 个在途任务的 owner；已有未关闭 owner 时拒绝重复装配。
    pub(crate) fn install() -> anyhow::Result<Arc<Self>> {
        let mut current = CURRENT
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        anyhow::ensure!(
            current.is_none(),
            "cache background owner is already installed"
        );
        let owner = Arc::new(Self {
            closed: Mutex::new(false),
            capacity: Arc::new(Semaphore::new(128)),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        });
        *current = Some(owner.clone());
        Ok(owner)
    }

    /// 业务作用：关闭新刷新准入并通知在途读取结束，只撤下仍属于自己的全局入口。
    /// 参数说明：无。
    /// 返回：不会再创建该代次任务；实际退出由 wait 证明。
    pub(crate) fn close(self: &Arc<Self>) {
        *self.closed.lock().expect("cache task admission") = true;
        self.tasks.close();
        self.cancel.cancel();
        let mut current = CURRENT
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current
            .as_ref()
            .is_some_and(|value| Arc::ptr_eq(value, self))
        {
            // 移除本代场景与开放下一 owner 使用同一把锁，旧 loader 只会写已撤下的场景。
            crate::local_cache::clear_all();
            current.take();
        }
    }

    /// 业务作用：等待全部已登记 future 实际析构，不把取消通知当作收口证明。
    /// 参数说明：无。
    /// 返回：任务计数清零后完成；宿主负责施加停机期限。
    pub(crate) async fn wait(&self) {
        self.tasks.wait().await;
    }
}

/// 业务作用：非阻塞登记当前运行态的一项读取或本地失效任务。
/// 参数说明：`future` 为只读回源或进程内缓存操作。
/// 返回：有开放 owner 和剩余容量时接纳；否则释放 future，由调用方沿同步读取或整场景失效降级。
pub(crate) fn try_spawn(future: impl std::future::Future<Output = ()> + Send + 'static) -> bool {
    let owner = CURRENT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let Some(owner) = owner else {
        return false;
    };
    let closed = owner.closed.lock().expect("cache task admission");
    if *closed {
        return false;
    }
    let Ok(permit) = owner.capacity.clone().try_acquire_owned() else {
        return false;
    };
    let cancel = owner.cancel.clone();
    owner.tasks.spawn(async move {
        let _permit = permit;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {},
            _ = future => {},
        }
    });
    true
}

/// 业务作用：确认纯 L1 或两级缓存仍有开放的本地工作 owner。
/// 参数说明：无。
/// 返回：当前 owner 尚未关闭时为 true，不创建任务或修改缓存。
pub(crate) fn is_open() -> bool {
    CURRENT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .is_some_and(|owner| !*owner.closed.lock().expect("cache task admission"))
}
