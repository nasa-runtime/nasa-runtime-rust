//! 默认 L2 的可撤销 owner；旧句柄停机后不能开始新的回填。

use crate::{MapperCacheLoad, MapperCacheLoader, MapperL2Cache, DEFAULT_L2_CACHE};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{RwLock, RwLockReadGuard};

struct ManagedCache {
    closed: AtomicBool,
    inner: RwLock<Option<Arc<dyn MapperL2Cache>>>,
}

impl ManagedCache {
    /// 业务作用：在新调用进入前复验当前代次仍接受缓存工作。
    /// 参数说明：无。
    /// 返回：持有至整个缓存或 loader 调用结束的读守卫；关闭后拒绝。
    async fn enter(&self) -> anyhow::Result<RwLockReadGuard<'_, Option<Arc<dyn MapperL2Cache>>>> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "mapper cache owner is closed"
        );
        let guard = self.inner.read().await;
        // 获取资源之后再次复验，避免等待期间关闭的句柄进入下一次调用。
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire) && guard.is_some(),
            "mapper cache owner is closed"
        );
        Ok(guard)
    }
}

/// 当前默认 L2 的安装凭证；撤销只影响自己安装的实例。
pub struct MapperCacheOwner {
    cache: Arc<ManagedCache>,
}

impl MapperCacheOwner {
    /// 业务作用：安装受 owner 门禁保护的默认缓存，拒绝与其它标准或自定义安装冲突。
    /// 参数说明：`cache` 为已完成后端校验的缓存。
    /// 返回：安装成功的 owner；重复安装返回错误，不覆盖当前实例。
    pub fn install(cache: Arc<dyn MapperL2Cache>) -> anyhow::Result<Self> {
        let managed = Arc::new(ManagedCache {
            closed: AtomicBool::new(false),
            inner: RwLock::new(Some(cache)),
        });
        crate::set_default_l2_cache(managed.clone())?;
        Ok(Self { cache: managed })
    }

    /// 业务作用：撤下当前代次的默认入口并关闭所有旧句柄的新调用准入。
    /// 参数说明：无。
    /// 返回：同步生效，不等待已经接纳的缓存调用或数据库 loader。
    pub fn close(&self) {
        let installed: Arc<dyn MapperL2Cache> = self.cache.clone();
        let mut slot = DEFAULT_L2_CACHE
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.cache.closed.store(true, Ordering::Release);
        // 旧 owner 的延迟清理不能取下后续 Application 已安装的同名资源。
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &installed))
        {
            *slot = None;
        }
    }

    /// 业务作用：关闭准入后等待已接纳的 loader 和缓存调用归还资源。
    /// 参数说明：`deadline` 为宿主统一停机截止点。
    /// 返回：全部调用退出后释放后端；超时保留资源及关闭态，可再次等待。
    pub async fn shutdown(&self, deadline: tokio::time::Instant) -> anyhow::Result<()> {
        self.close();
        let mut resource = tokio::time::timeout_at(deadline, self.cache.inner.write())
            .await
            .map_err(|_| {
                anyhow::anyhow!("mapper cache calls did not drain before shutdown deadline")
            })?;
        resource.take();
        Ok(())
    }
}

impl Drop for MapperCacheOwner {
    /// 业务作用：启动失败或 owner 被释放时撤销入口，不遗留不可撤销全局引用。
    /// 参数说明：无。
    /// 返回：新调用立即拒绝；已接纳调用按其原有生命周期结束。
    fn drop(&mut self) {
        self.close();
    }
}

#[crate::async_trait]
impl MapperL2Cache for ManagedCache {
    /// 业务作用：在当前 owner 准入下读取字段。
    /// 参数说明：`key` 为命名空间；`hash_key` 为查询字段。
    /// 返回：命中值、未命中或关闭/后端错误。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let guard = self.enter().await?;
        guard
            .as_ref()
            .expect("admitted cache")
            .get(key, hash_key)
            .await
    }

    /// 业务作用：在当前 owner 准入下写入查询结果。
    /// 参数说明：`key` 为命名空间；`hash_key` 为字段；`value` 为编码值；`ttl_ms` 为可选过期时间。
    /// 返回：写入确认或关闭/后端错误。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        let guard = self.enter().await?;
        guard
            .as_ref()
            .expect("admitted cache")
            .put(key, hash_key, value, ttl_ms)
            .await
    }

    /// 业务作用：在当前 owner 准入下失效单个字段。
    /// 参数说明：`key` 为命名空间；`hash_key` 为字段。
    /// 返回：删除确认或关闭/后端错误。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        let guard = self.enter().await?;
        guard
            .as_ref()
            .expect("admitted cache")
            .evict(key, hash_key)
            .await
    }

    /// 业务作用：在当前 owner 准入下失效整个命名空间。
    /// 参数说明：`key` 为命名空间。
    /// 返回：清理确认或关闭/后端错误。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        let guard = self.enter().await?;
        guard.as_ref().expect("admitted cache").clear_key(key).await
    }

    /// 业务作用：在一次完整准入中执行缓存查询、数据库回源和回填。
    /// 参数说明：`key` 为命名空间；`hash_key` 为字段；`ttl_ms` 为回填过期时间；`loader` 为业务回源方法。
    /// 返回：查询结果或错误；owner 排干包含回源与回填，不只等待首次读取。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        let guard = self.enter().await?;
        guard
            .as_ref()
            .expect("admitted cache")
            .get_or_load(key, hash_key, ttl_ms, loader)
            .await
    }
}
/// 默认 codec 与指标槽的可撤销所有权；释放旧 owner 不会清除后续安装。
pub struct MapperDefaultsOwner {
    codec: Option<std::sync::Arc<dyn crate::MapperCacheCodec>>,
    metrics: Option<std::sync::Arc<dyn crate::MapperMetrics>>,
    closed: AtomicBool,
}

impl MapperDefaultsOwner {
    /// 业务作用：原子占用所需的默认算法和观测槽，拒绝与已有安装混用。
    /// 参数说明：`codec` 为可选编解码策略；`metrics` 为可选观测出口。
    /// 返回：全部所选槽空闲时安装成功；冲突不改变任何槽。
    pub fn install(
        codec: Option<std::sync::Arc<dyn crate::MapperCacheCodec>>,
        metrics: Option<std::sync::Arc<dyn crate::MapperMetrics>>,
    ) -> anyhow::Result<Self> {
        let mut codec_slot = crate::DEFAULT_MAPPER_CACHE_CODEC
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut metrics_slot = crate::DEFAULT_MAPPER_METRICS
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        anyhow::ensure!(
            codec.is_none() || codec_slot.is_none(),
            "mapper codec is already installed"
        );
        anyhow::ensure!(
            metrics.is_none() || metrics_slot.is_none(),
            "mapper metrics is already installed"
        );
        if let Some(codec) = &codec {
            *codec_slot = Some(codec.clone());
        }
        if let Some(metrics) = &metrics {
            *metrics_slot = Some(metrics.clone());
        }
        Ok(Self {
            codec,
            metrics,
            closed: AtomicBool::new(false),
        })
    }

    /// 业务作用：只撤销当前 owner 仍占有的槽，允许同进程再次启动。
    /// 参数说明：无。
    /// 返回：首次关闭撤下仍属于本次安装的入口；重复关闭不影响后续安装，已借出的算法引用可完成调用。
    pub fn close(&self) {
        // 同一个策略 Arc 可以在后续应用再次安装；旧凭证只能撤销一次，不能把复用指针当作自己的新占用。
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(codec) = &self.codec {
            let mut slot = crate::DEFAULT_MAPPER_CACHE_CODEC
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot
                .as_ref()
                .is_some_and(|current| std::sync::Arc::ptr_eq(current, codec))
            {
                slot.take();
            }
        }
        if let Some(metrics) = &self.metrics {
            let mut slot = crate::DEFAULT_MAPPER_METRICS
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot
                .as_ref()
                .is_some_and(|current| std::sync::Arc::ptr_eq(current, metrics))
            {
                slot.take();
            }
        }
    }
}

impl Drop for MapperDefaultsOwner {
    /// 业务作用：安装失败回滚和宿主释放时同步撤下仍属于自己的默认槽。
    /// 参数说明：无。
    /// 返回：不创建后台任务，也不撤销后续 owner。
    fn drop(&mut self) {
        self.close();
    }
}
