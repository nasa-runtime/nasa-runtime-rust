//! 持久失效意图策略的可撤销所有权；记录与执行分离。

use crate::{CacheRuntime, DurableInvalidationSink};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

struct ManagedSink {
    closed: AtomicBool,
    value: tokio::sync::RwLock<Option<Arc<dyn DurableInvalidationSink>>>,
}

#[async_trait::async_trait]
impl DurableInvalidationSink for ManagedSink {
    /// 业务作用：在原业务事务上下文内记录意图，并把完整 await 纳入 owner 的在途责任。
    /// 参数说明：`scene` 为缓存场景，`key` 为完整缓存键。
    /// 返回：底层持久记录结果；关闭后拒绝新调用，不重试不确定写入。
    async fn record(&self, scene: &str, key: &str) -> anyhow::Result<()> {
        let value = self.value.read().await;
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
    sink: Arc<ManagedSink>,
}

impl DurableInvalidationOwner {
    /// 业务作用：建立独占持久意图入口，拒绝与标准或独立安装竞争。
    /// 参数说明：`sink` 为业务事务策略，依赖资源的关闭仍由对应宿主组件承担。
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
        Ok(Self { sink })
    }

    /// 业务作用：收回本 owner 的全局入口，避免旧调用重新开始持久副作用。
    /// 参数说明：无。
    /// 返回：准入永久关闭；已进入的记录继续由 shutdown 等待。
    pub fn close(&self) {
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
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &expected))
        {
            slot.take();
            runtime.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// 业务作用：等待记录 future 释放其事务依赖后移除本代策略。
    /// 参数说明：无。
    /// 返回：全部已进入调用离开后完成；宿主可施加截止点，取消等待不会重新开放。
    pub async fn shutdown(&self) {
        self.close();
        self.sink.value.write().await.take();
    }
}

impl Drop for DurableInvalidationOwner {
    /// 业务作用：在启动失败或关闭超时后保证本代入口不可再用。
    /// 参数说明：无。
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
