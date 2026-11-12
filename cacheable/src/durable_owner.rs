//! 持久失效意图策略的可撤销所有权；记录与执行分离。

use crate::{CacheRuntime, DurableInvalidationSink};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

struct ManagedSink {
    /// 永久关闭本 owner 的新记录准入；已进入底层策略的调用继续持有读锁。
    closed: AtomicBool,
    /// 读锁覆盖记录调用的完整等待过程，关闭时用写锁等待在途调用释放策略。
    value: tokio::sync::RwLock<Option<Arc<dyn DurableInvalidationSink>>>,
}

#[async_trait::async_trait]
impl DurableInvalidationSink for ManagedSink {
    /// 业务作用：在原业务事务上下文内记录意图，并把完整 await 纳入 owner 的在途责任。
    ///
    /// 参数说明：
    /// - `scene`：待失效缓存所属的场景。
    /// - `key`：场景内待失效的完整缓存键。
    ///
    /// 返回：底层持久记录结果；关闭后拒绝新调用，不重试不确定写入。
    async fn record(&self, scene: &str, key: &str) -> anyhow::Result<()> {
        let value = self.value.read().await;
        // 关闭后保留的旧句柄也必须拒绝新记录，不能仅依赖全局入口已被撤下。
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "durable invalidation is closed"
        );
        value
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("durable invalidation is closed"))?
            .record(scene, key)
            .await
    }
}

/// 一个进程内唯一的持久意图 owner；不会创建 dispatcher 或更改调用方事务。
pub struct DurableInvalidationOwner {
    /// 本 owner 唯一可撤销的策略入口；比较入口身份后才移除全局注册。
    sink: Arc<ManagedSink>,
}

impl DurableInvalidationOwner {
    /// 业务作用：建立独占持久意图入口，拒绝与标准或独立安装竞争。
    ///
    /// 参数说明：
    /// - `sink`：参与业务事务的持久记录策略；其依赖资源仍由宿主组件关闭。
    ///
    /// 返回：安装成功返回可关闭 owner；已有策略时不改变槽。
    pub fn install(sink: Arc<dyn DurableInvalidationSink>) -> anyhow::Result<Self> {
        let runtime = CacheRuntime::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut slot = runtime
            .durable_sink
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 空槽可能仍有正在关闭的 owner；其责任释放前不接纳另一持久记录策略。
        anyhow::ensure!(
            slot.is_none() && !runtime.durable_owned.load(Ordering::Acquire),
            "durable invalidation strategy is already installed"
        );
        let sink = Arc::new(ManagedSink {
            closed: AtomicBool::new(false),
            value: tokio::sync::RwLock::new(Some(sink)),
        });
        *slot = Some(sink.clone());
        runtime.durable_owned.store(true, Ordering::Release);
        runtime.generation.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            sink,
        })
    }

    /// 业务作用：收回本 owner 的全局入口，避免旧调用重新开始持久副作用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：准入永久关闭；已进入的记录继续由 shutdown 等待。
    pub fn close(&self) {
        // 先关闭旧句柄准入再撤下注册；已通过准入的事务记录仍由 shutdown 等待。
        self.sink.closed.store(true, Ordering::Release);
        let runtime = CacheRuntime::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut slot = runtime
            .durable_sink
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expected: Arc<dyn DurableInvalidationSink> = self.sink.clone();
        // 全局入口可能已被撤销或替换，只允许收回自己的策略。
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &expected))
        {
            slot.take();
            runtime.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// 业务作用：等待记录 future 释放其事务依赖后移除本代策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：全部已进入调用离开后完成；宿主可施加截止点，取消等待不会重新开放。
    pub async fn shutdown(&self) {
        self.close();
        self.sink.value.write().await.take();
    }
}

impl Drop for DurableInvalidationOwner {
    /// 业务作用：在启动失败或关闭超时后保证本代入口不可再用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：关闭新准入；在途调用持有原策略直到自身 future 释放。
    fn drop(&mut self) {
        self.close();
        let runtime = CacheRuntime::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.durable_owned.store(false, Ordering::Release);
    }
}
