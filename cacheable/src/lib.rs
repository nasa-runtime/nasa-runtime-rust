//! 两级缓存运行时支持。
//!
//! 提供本地 L1、Redis L2、single-flight、失效广播和宏展开所需的稳定入口。
/// Redis L2、分组缓存、序列化和分布式防击穿缓存层。
pub mod cache;
/// 进程内 L1 缓存和刷新/过期策略。
pub mod local_cache;
mod local_work;
pub use local_work::LocalCacheRuntimeGuard;
mod durable_owner;
pub use durable_owner::DurableInvalidationOwner;

// nacache-macro 的缓存宏构造场景键并调用本模块入口；宿主负责装配后端及关闭运行时。
// 读取依次使用 L1、L2 和业务回源，写后失效以尽力广播通知其他节点，不提供强一致读取。

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::cache::CacheLayer;

// ── 进程级缓存运行时──
// `CacheRuntime` 的可撤销槽保存当前后端与发布器，宏入口读取当前代次。
// napp CacheComponent 持有安装与撤销责任，并管理 readiness 和停机。

/// 进程级两级缓存运行时:统一持有 L2 后端、失效广播发布器与可选 durable 失效 sink。
///
/// 后端和发布器按 owner 撤销，generation 记录变化；长借 Arc 保留旧资源直至释放。
/// 分别读取多个槽不构成跨组件原子快照。持久意图策略具有独立关闭门禁，后续应用可重新装配。
pub struct CacheRuntime {
    /// L2(Redis 三防)后端句柄;`init`(首装)/`install_generation`(换代)注入,`revoke_runtime` 清空。
    backend: RwLock<Option<Arc<CacheLayer>>>,
    /// 由拥有式 guard 安装的失效广播发布器；独立创建广播句柄不会自动写入此槽。
    publisher: RwLock<Option<Arc<BoundedInvalidatePublisher>>>,
    /// 显式 record_invalidation 使用的持久意图入口；执行失效不会调用它。
    durable_sink: RwLock<Option<Arc<dyn DurableInvalidationSink>>>,
    /// 持久策略 owner 尚未释放的独占声明；即使全局槽已清空，也拒绝其他策略争用。
    durable_owned: std::sync::atomic::AtomicBool,
    /// 槽代次:任何 install/revoke 单调 +1,供诊断观察换代。
    generation: AtomicU64,
    /// 当前拥有 backend/publisher 槽的 guard owner；旧 guard 只能停止自己的任务，不能撤销新 owner。
    owner: AtomicU64,
    /// owner id 分配器。
    next_owner: AtomicU64,
    /// 串行化跨多个槽的换代/撤销，避免旧 guard 在新一代写到一半时清空新 backend。
    transition: Mutex<()>,
}

impl CacheRuntime {
    /// 业务作用：返回进程级唯一运行时(懒初始化;槽内容由 CacheComponent/guard 拥有生命周期)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享运行时槽；首次调用仅初始化空槽和控制状态，不连接 Redis 或装配缓存后端。
    fn global() -> &'static CacheRuntime {
        static RUNTIME: OnceLock<CacheRuntime> = OnceLock::new();
        RUNTIME.get_or_init(|| CacheRuntime {
            backend: RwLock::new(None),
            publisher: RwLock::new(None),
            durable_sink: RwLock::new(None),
            durable_owned: std::sync::atomic::AtomicBool::new(false),
            generation: AtomicU64::new(0),
            owner: AtomicU64::new(0),
            next_owner: AtomicU64::new(0),
            transition: Mutex::new(()),
        })
    }

    /// 业务作用：首装 L2 后端:槽空时写入并换代;已有则忽略(保持 `init` 的既有“重复注入被忽略”契约)。
    ///
    /// 参数说明：
    /// - `layer`：已构建的 L2 后端；只有全局后端槽为空时才接管。
    ///
    /// 返回：空槽获得后端并递增配置代次；已有后端时不修改任何槽，不分配新的 owner。
    fn set_backend(layer: Arc<CacheLayer>) {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut slot = runtime
            .backend
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(layer);
            runtime.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// 业务作用：重新装配 L2 后端并分配新 owner，防止旧 guard 关闭时撤销替换后的后端。
    ///
    /// 参数说明：
    /// - `layer`：替换全局后端槽的新缓存层。
    ///
    /// 返回：后端和 owner 已更新、配置代次递增；保留发布器与持久策略槽，已借出的旧后端继续有效。
    fn replace_backend(layer: Arc<CacheLayer>) {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner = runtime.next_owner.fetch_add(1, Ordering::AcqRel) + 1;
        *runtime
            .backend
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(layer);
        runtime.owner.store(owner, Ordering::Release);
        runtime.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// 业务作用：取 L2 后端句柄(clone 当前代的 Arc)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已安装后端的共享句柄，未装配或已撤销时返回 None；此后换代不改变已经借出的句柄。
    fn backend() -> Option<Arc<CacheLayer>> {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime
            .backend
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：由拥有式 guard 安装一代 backend + 可选 publisher，返回唯一 owner id。
    ///
    /// 参数说明：
    /// - `layer`：安装到当前运行时的 L2 后端。
    /// - `publisher`：已准备好的失效广播发布器；None 会清除先前的发布器槽。
    ///
    /// 返回：控制后端与发布器撤销权限的新 owner 标识；两槽在同一转换锁内更新后递增配置代次。
    fn install_owned(layer: Arc<CacheLayer>, publisher: Option<BoundedInvalidatePublisher>) -> u64 {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner = runtime.next_owner.fetch_add(1, Ordering::AcqRel) + 1;
        *runtime
            .backend
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(layer);
        *runtime
            .publisher
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = publisher.map(Arc::new);
        runtime.owner.store(owner, Ordering::Release);
        runtime.generation.fetch_add(1, Ordering::AcqRel);
        owner
    }

    /// 业务作用：凭 owner 标识收回全局缓存入口，阻止旧 guard 关闭时清空后续 owner 的运行时。
    ///
    /// 参数说明：
    /// - `owner`：执行撤销的 guard 在安装时取得的 owner 标识。
    ///
    /// 返回：匹配时清空后端、发布器和持久策略槽并递增配置代次，返回 true；不匹配时保持现状并返回 false。
    fn revoke_if_owner(owner: u64) -> bool {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 比较控制身份与清空槽受同一转换锁保护，旧 guard 不得撤销后续安装的资源。
        if runtime
            .owner
            .compare_exchange(owner, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        *runtime
            .backend
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *runtime
            .publisher
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *runtime
            .durable_sink
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        runtime.generation.fetch_add(1, Ordering::AcqRel);
        true
    }

    /// 业务作用：取失效广播发布器(clone 当前代的 Arc)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前发布器的共享句柄；未安装时返回 None，取得句柄不代表广播已被远端接收。
    fn publisher() -> Option<Arc<BoundedInvalidatePublisher>> {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime
            .publisher
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：取得当前持久失效记录策略，供业务事务显式记录失效意图。
    ///
    /// 参数说明：无。
    ///
    /// 返回：策略共享句柄或 None；是否仍接纳记录由策略自身的关闭门禁决定，此处不执行持久写入。
    fn durable_sink() -> Option<Arc<dyn DurableInvalidationSink>> {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime
            .durable_sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// 将失效意图记录到可重放通道的事务适配器。
///
/// `record_invalidation` 只记录意图，不删除缓存或发布 Pub/Sub。事务适配器必须参与调用方的
/// 同源业务事务，记录失败应使业务事务失败。提交后由 dispatcher 调用 `apply_invalidation`。
/// Pub/Sub 是尽力通知，离线节点仍可能漏收；该接口不提供持久广播或 cache-aside 强一致保证。
#[async_trait::async_trait]
pub trait DurableInvalidationSink: Send + Sync {
    /// 业务作用：在调用方的同源业务事务中持久记录失效意图，供提交后的执行器应用。
    ///
    /// 参数说明：
    /// - `scene`：需要失效的缓存场景名。
    /// - `key`：该场景内需要失效的完整缓存键。
    ///
    /// 返回：意图成功记录或持久化错误；调用方必须向业务事务传播失败，成功也不代表缓存已经失效。
    async fn record(&self, scene: &str, key: &str) -> anyhow::Result<()>;
}

/// 业务作用：为独立使用方安装持久意图策略，避免覆盖受管 owner。
///
/// 参数说明：
/// - `sink`：参与同源事务的意图记录器；允许替换既有的非受管策略。
///
/// 返回：非受管槽安装成功；受管 owner 存在时拒绝且保留现有策略。
pub fn set_durable_invalidation_sink(sink: Arc<dyn DurableInvalidationSink>) -> anyhow::Result<()> {
    let runtime = CacheRuntime::global();
    let _transition = runtime
        .transition
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // 受管 owner 可能正在排空事务记录，即使槽已撤下也不能抢先替换策略。
    anyhow::ensure!(
        !runtime.durable_owned.load(Ordering::Acquire),
        "durable invalidation has a managed owner"
    );
    *runtime
        .durable_sink
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sink);
    runtime.generation.fetch_add(1, Ordering::AcqRel);
    Ok(())
}

/// 业务作用：替换全局 L2 后端，供调用方重新装配缓存资源后切换后续读取。
///
/// 参数说明：
/// - `layer`：已准备好的新 L2 缓存层。
///
/// 返回：更新后端、owner 和配置代次；保留发布器及持久策略，旧请求继续持有原 Arc，调用方仍负责关闭旧资源。
pub fn install_generation(layer: Arc<CacheLayer>) {
    CacheRuntime::replace_backend(layer);
}

/// 业务作用：无条件收回全局缓存后端、广播发布器与持久意图策略入口，供宿主主动撤销运行时。
///
/// 参数说明：无。
///
/// 返回：清空三个槽与 owner 并递增配置代次；依赖后端的新调用返回未装配错误，可重新安装。
/// 本操作不等待在途请求、不终止已借出的资源，也不替代各生命周期句柄的 shutdown。
pub fn revoke_runtime() {
    let runtime = CacheRuntime::global();
    let _transition = runtime
        .transition
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    runtime.owner.store(0, Ordering::Release);
    *runtime
        .backend
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    *runtime
        .publisher
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    *runtime
        .durable_sink
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    runtime.generation.fetch_add(1, Ordering::AcqRel);
}

/// 缓存运行时的只读诊断快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheRuntimeSnapshot {
    /// 当前运行时代次；任何 install/revoke 都会单调递增。
    pub generation: u64,
    /// 当前是否安装了 L2 后端。
    pub backend_installed: bool,
    /// 当前是否安装了跨节点失效广播发布器。
    pub invalidation_broadcast_installed: bool,
    /// 当前是否安装了 durable 失效 sink。
    pub durable_invalidation_installed: bool,
}

/// 只读缓存能力句柄。
///
/// 句柄不拥有后端、广播任务或停机权限；每次读取都固定当前 generation 的 `Arc`，因此一次探针
/// 不会在运行中混用两代后端。
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheHandle;

impl CacheHandle {
    /// 业务作用：返回当前代的配置/装配摘要，不暴露连接串或缓存 key。
    ///
    /// 参数说明：无。
    ///
    /// 返回：在同一转换锁内读取的代次与能力安装状态；不触发网络探测。
    pub fn snapshot(self) -> CacheRuntimeSnapshot {
        runtime_snapshot()
    }

    /// 业务作用：对当前代 L2 后端执行只读健康探针。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定本次后端句柄后等待探针结果；后端未安装或探针失败时返回错误，不读写业务缓存键。
    pub async fn health_check(self) -> anyhow::Result<()> {
        let backend = CacheRuntime::backend()
            .ok_or_else(|| anyhow::anyhow!("cache runtime has no installed backend"))?;
        backend.health_check().await
    }
}

/// 业务作用：返回进程级缓存运行时的只读句柄。
///
/// 参数说明：无。
///
/// 返回：不持有资源所有权的能力句柄；创建句柄不要求后端已安装，也不具备停机权限。
pub const fn cache_handle() -> CacheHandle {
    CacheHandle
}

/// 业务作用：读取缓存运行时的配置变化计数，供宿主观察安装、替换与撤销。
///
/// 参数说明：无。
///
/// 返回：当前代次；零表示尚无槽变更，单独读取该计数不构成其他槽的原子快照或健康证明。
pub fn runtime_generation() -> u64 {
    CacheRuntime::global().generation.load(Ordering::Acquire)
}

/// 业务作用：返回不含连接信息、业务 key 或 secret 的运行时摘要。
///
/// 参数说明：无。
///
/// 返回：同一转换锁保护下读取的代次及三个槽的安装状态；只反映装配情况，不证明依赖服务可用。
pub fn runtime_snapshot() -> CacheRuntimeSnapshot {
    let runtime = CacheRuntime::global();
    let _transition = runtime
        .transition
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    CacheRuntimeSnapshot {
        generation: runtime.generation.load(Ordering::Acquire),
        backend_installed: runtime
            .backend
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some(),
        invalidation_broadcast_installed: runtime
            .publisher
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some(),
        durable_invalidation_installed: runtime
            .durable_sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some(),
    }
}

/// 业务作用：为独立调用方首次安装 L2 缓存层，使两级缓存读取具备后端入口。
///
/// 参数说明：
/// - `layer`：已经构建的 L2 缓存层，由调用方准备连接与过期配置。
///
/// 返回：空槽写入并递增配置代次；已有后端时忽略本次安装，不自动启动本地后台任务或失效广播。
pub fn init(layer: Arc<CacheLayer>) {
    // 只注入一次:重复注入被忽略(保持既有语义)。
    CacheRuntime::set_backend(layer);
}

/// 业务作用：取当前代 L2 句柄(clone 一份 Arc)。未装配/已撤销返回明确错误(撤销后宏入口不 panic)。
///
/// 参数说明：无。
///
/// 返回：本次操作使用的共享 L2 后端；全局槽为空时返回未装配错误，不尝试隐式建连。
fn try_l2() -> anyhow::Result<Arc<CacheLayer>> {
    CacheRuntime::backend().ok_or_else(|| {
        anyhow::anyhow!(
            "cacheable 未初始化或已撤销:声明 `cache` 组件(或调用 init/install_generation)后再使用缓存宏"
        )
    })
}

/// 业务作用：为缓存宏提供 L1、L2 与业务回源的读取链路，在软刷新窗口内允许返回旧值。
///
/// 参数说明：
/// - `scene`：本地场景名；读写失效必须一致，不得与会缓存空哨兵的直接调用共用相同条目。
/// - `key`：宏已构造的完整缓存键，同时用于 L1 和 L2。
/// - `refresh_ms`：首次建 L1 场景时绑定的软刷新阈值，单位毫秒。
/// - `expire_ms`：首次建 L1 场景时绑定的硬过期时长，单位毫秒；已有场景保持原配置。
/// - `loader`：L2 也未命中时执行的业务回源闭包，可能由受管后台刷新任务持有。
///
/// 返回：从共享 L1 值克隆的业务结果；未安装 L2 或未命中后的加载失败返回错误，软刷新失败仍保留旧值。
/// 同名场景类型冲突，或其他入口在相同条目写入空哨兵时 panic；该入口不提供强一致读取。
pub async fn get_or_load_2level<T, F, Fut>(
    scene: &'static str,
    key: String,
    refresh_ms: u64,
    expire_ms: u64,
    loader: F,
) -> anyhow::Result<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
{
    let l2 = try_l2()?; // 取当前代 L2 句柄(Arc,move 进下面的 L1 loader;旧请求持旧代直至释放)
    let l2_key = key.clone(); // L1/L2 用同一个 key;L1 会消费 key,故给 L2 留一份

    // L1:getRefresh —— 命中且新鲜直接返;到刷新窗口返旧值+后台刷;硬过期/缺失则跑下面的闭包(走 L2)。
    //   sync=false 只让软刷新走后台；缓存缺失时请求仍等待加载，闭包须能被后台任务持有。
    let arc: Option<Arc<T>> = local_cache::getRefresh::<String, T, _, _>(
        scene,
        key,
        false, // 异步刷新
        refresh_ms,
        expire_ms,
        move || async move {
            // L1 未命中/刷新 → 进 L2(CacheLayer.get_or_load 自带穿透/雪崩/击穿三防)→ 仍未命中跑 loader(DB)
            let v = l2.get_or_load(l2_key, loader).await?;
            // 包成 Some:本场景永远有值(loader 成功就返回 T),不用 L1 的空哨兵(None)
            Ok::<Option<T>, anyhow::Error>(Some(v))
        },
    )
    .await?;

    // L1 存的是 Arc<T>。上面 loader 恒返回 Some,故这里必有值;clone 出自有 T 返回给调用方。
    Ok((*arc.expect("2-level loader 恒返回 Some,理论上必有值")).clone())
}

/// 业务作用：为失效宏执行即时缓存撤销，复用提交后失效入口但不自动记录持久意图。
///
/// 参数说明：
/// - `scene`：与读取入口一致的本地缓存场景名。
/// - `key`：宏构造的完整缓存键；泛型 V 保留宏签名，不参与运行时场景匹配。
///
/// 返回：共享缓存删除成功后发起本地失效与尽力广播；未装配或 L2 删除失败返回错误。
/// 成功不证明外层业务事务已提交，也不证明所有远端节点已失效；事务意图须显式调用 record_invalidation。
pub async fn invalidate<V>(scene: &'static str, key: String) -> anyhow::Result<()>
where
    V: Send + Sync + 'static,
{
    apply_invalidation(scene, &key).await
}

/// 业务作用：仅持久记录失效意图，使调用方可在业务事务中原子提交意图与数据。
///
/// 参数说明：
/// - `scene`：失效意图对应的缓存场景名。
/// - `key`：该场景内需要在事务提交后失效的完整缓存键。
///
/// 返回：sink 成功记录时成功；未安装或记录失败直接返回错误，不访问 L1、L2 或广播队列。
pub async fn record_invalidation(scene: &str, key: &str) -> anyhow::Result<()> {
    let sink = CacheRuntime::durable_sink()
        .ok_or_else(|| anyhow::anyhow!("durable invalidation sink is not installed"))?;
    sink.record(scene, key).await
}

/// 业务作用：执行已提交的失效意图，供 dispatcher 和显式即时失效共用。
///
/// 参数说明：
/// - `scene`：与缓存读取一致的本地场景名。
/// - `key`：待失效的完整缓存键，同时用于 L2 删除与 L1 失效。
///
/// 返回：L2 删除成功后发起本节点 L1 失效并尝试广播；纯 L1 只需开放的本地任务 owner。
/// 未安装运行态或 L2 失败返回错误以供重试，不再记录意图。
/// 并发旧 loader 可能在删除后回填，Pub/Sub 也可能丢失；该入口不提供强一致读取。
pub async fn apply_invalidation(scene: &str, key: &str) -> anyhow::Result<()> {
    // 先删除共享缓存，再清本节点，缩小清理过程中从共享旧值重新回填的窗口。
    if let Some(layer) = CacheRuntime::backend() {
        layer.delete(key).await?;
    } else {
        anyhow::ensure!(local_work::is_open(), "cache runtime is not installed");
    }
    local_cache::remove_any(scene, key);
    publish_invalidate(scene, key);
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// 跨节点 L1 失效广播（Redis Pub/Sub）
// ════════════════════════════════════════════════════════════════════════════
// 本地缓存失效广播：
//   写节点先删 L2，再提交本地 L1 失效和包含 scene/key 的 JSON 广播。
//   订阅节点收到广播后提交本地失效；离线、队列满或发布失败均可能使远端保留旧值。
//
// 注:用一条【专用的普通 redis 连接】做 pub/sub(SUBSCRIBE 会独占连接,不能复用 CacheLayer 的命令连接;
//   且 pub/sub 用单节点连接即可——Redis Cluster 会把普通 PUBLISH 跨节点传播给所有订阅者)。

use futures_util::StreamExt;
use redis::AsyncCommands; // 提供 conn.publish(...) // 提供 pubsub 消息流的 .next().await

// 失效广播频道名(所有节点订阅同一个)
const INVALIDATE_CHANNEL: &str = "cacheable:invalidate";

/// 失效广播有界队列的默认容量；突发失效在此范围内缓冲，超出时丢弃并告警。
const INVALIDATE_QUEUE_CAPACITY: usize = 1024;

/// 一条待广播的失效消息(scene + key)。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvalidateMessage {
    /// 缓存事件所属业务场景名。
    pub scene: String,
    /// 需从各节点 L1 删除的业务缓存 key。
    pub key: String,
}

/// 一次入队尝试的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// 已入有界队列,drainer 会异步 PUBLISH。
    Enqueued,
    /// 队列已满,本条被丢弃(远端广播未保证;本地 L1 仍已失效)。
    DroppedQueueFull,
    /// drainer 已停(停机中/未启动),本条被丢弃。
    DroppedClosed,
}

/// 有界失效广播发布器:业务侧 `try_publish` 只做非阻塞入队,单个 drainer 任务负责真正 PUBLISH。
///
/// 满队列时丢弃并告警；发布任务的取消与等待由持有 [`InvalidateBroadcast`] 的一方负责。
#[derive(Clone)]
pub struct BoundedInvalidatePublisher {
    /// 有界发送入口；克隆发布器只共享队列，队列接纳不代表 Redis 已完成发布。
    sender: tokio::sync::mpsc::Sender<InvalidateMessage>,
}

impl BoundedInvalidatePublisher {
    /// 业务作用：建立失效广播的有界队列，将业务入队与异步 Redis 发布分离。
    ///
    /// 参数说明：
    /// - `capacity`：期望队列容量；自动限制在 1 到 Tokio Semaphore::MAX_PERMITS 之间。
    ///
    /// 返回：共享发布器与唯一接收端；本操作不连接 Redis，也不启动消费任务。
    pub fn channel(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<InvalidateMessage>) {
        let capacity = capacity.clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (
            Self {
                sender,
            },
            receiver,
        )
    }

    /// 业务作用：非阻塞提交失效广播，限制发布压力并向调用方区分队列接纳与丢弃。
    ///
    /// 参数说明：
    /// - `scene`：失效事件所属的缓存场景名。
    /// - `key`：需要从各节点 L1 撤销的完整缓存键。
    ///
    /// 返回：成功入队、队列满丢弃或接收端关闭丢弃；入队不代表 Redis 发布成功，也不修改本地缓存。
    pub fn try_publish(&self, scene: &str, key: &str) -> PublishOutcome {
        let message = InvalidateMessage {
            scene: scene.to_owned(),
            key: key.to_owned(),
        };
        match self.sender.try_send(message) {
            Ok(()) => PublishOutcome::Enqueued,
            Err(tokio::sync::mpsc::error::TrySendError::Full(dropped)) => {
                tracing::warn!(
                    scene = %dropped.scene,
                    "cacheable 失效广播队列已满,本条远端广播未保证"
                );
                PublishOutcome::DroppedQueueFull
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => PublishOutcome::DroppedClosed,
        }
    }
}

/// 业务作用：建立两级缓存 L2 使用的 Redis Cluster 连接。
///
/// 参数说明：
/// - `url`：逗号分隔的 Redis Cluster 种子节点连接串；各项去除两侧空白并忽略空项。
///
/// 返回：与当前缓存组件 Redis 依赖类型一致的异步集群连接；节点列表为空、地址无效或建连失败返回错误。
pub async fn connect_cluster(url: &str) -> anyhow::Result<redis::cluster_async::ClusterConnection> {
    let nodes: Vec<String> = url
        .split(',')
        .map(str::trim)
        .filter(|node| !node.is_empty())
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(!nodes.is_empty(), "cache: Redis Cluster 连接串为空");
    let client = redis::cluster::ClusterClient::new(nodes)?;
    Ok(client.get_async_connection().await?)
}

/// 失效广播订阅任务的可取消、可 join 句柄。
///
/// 订阅循环是常驻后台任务:没有句柄的话,宿主(应用容器)既不能取消它,也无法确认它已经退出,
/// 只能等进程结束回收。持有本句柄的一方负责在停机时显式 `shutdown().await`。
pub struct InvalidateBroadcast {
    /// 订阅监督循环的停止通知；重连退避也监听此信号。
    stop: std::sync::Arc<tokio::sync::Notify>,
    /// 订阅监督任务句柄，用于等待实际退出或请求中止。
    handle: tokio::task::JoinHandle<()>,
    /// 发布任务的停止信号，通知其尽力排空已经接纳的队列。
    publisher_stop: std::sync::Arc<tokio::sync::Notify>,
    /// 发布任务句柄，先于订阅任务等待退出。
    publisher_handle: tokio::task::JoinHandle<()>,
    /// 尚未发布到全局 runtime 的有界发布器；由 owner 安装阶段统一发布。
    publisher: BoundedInvalidatePublisher,
}

impl InvalidateBroadcast {
    /// 业务作用：通知发布 drainer 与订阅循环退出并等待二者真正结束。
    ///
    /// 参数说明：无。
    ///
    /// 返回：先等待发布任务尽力排空队列并退出，再等待订阅任务退出；不保证消息被远端收到。
    /// 消费句柄且不设置内置等待期限，任务异常也视为已退出；宿主取消等待时由 Drop 请求中止任务。
    pub async fn shutdown(mut self) {
        // notify_one 在任务尚未首次 poll 时会保存一个 permit；notify_waiters 不保存，极早停机可能
        // 丢失通知并让 join 永久等待。每个单消费者任务各用一个独立 Notify、各一个 permit。
        self.publisher_stop.notify_one();
        let _ = (&mut self.publisher_handle).await;
        self.stop.notify_one();
        let _ = (&mut self.handle).await;
        tracing::info!("cacheable 失效广播已停止(频道 {})", INVALIDATE_CHANNEL);
    }
}

impl Drop for InvalidateBroadcast {
    /// 业务作用：在显式关闭未完成时向两个广播任务发出取消和中止请求，防止仅丢弃句柄后继续常驻。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已通知并请求 abort；不等待任务析构，不保证队列排空，确认实际退出须使用 shutdown。
    fn drop(&mut self) {
        // shutdown future 被外层 deadline 取消时，JoinHandle 的普通 Drop 只会 detach。兜底必须同时
        // 发取消信号并请求 abort；本析构不能等待实际退出，宿主需要确认退出时应使用 shutdown。
        self.publisher_stop.notify_one();
        self.stop.notify_one();
        self.publisher_handle.abort();
        self.handle.abort();
    }
}

/// 缓存运行时的**拥有式生命周期句柄**:一处装配 L2 后端 + 失效广播,统一停机。
///
/// 取代业务分散调 `init` + `start_invalidate_broadcast` + 自建 shutdown 资源;把"装配 + 拥有 + 停机"
/// 收进单一对象，便于由 napp `CacheComponent` 统一构造并持有该 guard。
pub struct CacheRuntimeGuard {
    /// 本运行时负责关闭并等待的有界本地刷新与失效任务。
    local_work: Arc<local_work::LocalWork>,
    /// 可选的跨节点广播任务；宿主停机时按发布、订阅顺序关闭。
    broadcast: Option<InvalidateBroadcast>,
    /// 撤销全局运行时的控制身份，防止旧 guard 清空新 owner 的槽。
    owner: u64,
}

impl CacheRuntimeGuard {
    /// 业务作用：统一装配 L2、本地后台任务 owner 和可选失效广播，并持有其关闭责任。
    ///
    /// 参数说明：
    /// - `layer`：已准备好的 L2 缓存层，此入口不重新探测该后端。
    /// - `broadcast_url`：可选的普通 Redis 广播地址；None 表示不启动跨节点失效广播。
    ///
    /// 返回：全局运行时安装后返回拥有式句柄；发布建连失败或已有本地任务 owner 时返回错误并保留原运行时。
    /// 普通广播订阅在后台首次建连及重试，成功返回不代表已经订阅；需要首次订阅门禁时使用受管 Redis 入口。
    pub async fn start(
        layer: Arc<CacheLayer>,
        broadcast_url: Option<&str>,
    ) -> anyhow::Result<Self> {
        // 先把广播资源完整建好；失败时不触碰当前 last-good runtime。
        let broadcast = match broadcast_url {
            Some(url) => Some(start_invalidate_broadcast(url).await?),
            None => None,
        };
        let publisher = broadcast.as_ref().map(|value| value.publisher.clone());
        let local_work = local_work::LocalWork::install()?;
        let owner = CacheRuntime::install_owned(layer, publisher);
        Ok(Self {
            broadcast,
            owner,
            local_work,
        })
    }

    /// 业务作用：以宿主受管的 `RedisClient` 装配 L2 后端和跨实例失效广播。
    ///
    /// 发布命令复用受管客户端；订阅仍由该客户端派生协议要求的专用连接，并由返回的 guard 负责停止。
    ///
    /// 参数说明：
    /// - `layer`: 已建好的 L2 缓存层。
    /// - `client`: 已由宿主完成配置、探测与生命周期托管的 Redis 客户端。
    ///
    /// 返回：发布和订阅路径均成功建立后返回拥有式句柄；订阅建立失败时不发布新一代缓存运行时。
    #[cfg(feature = "managed-redis")]
    pub async fn start_with_managed_redis(
        layer: Arc<CacheLayer>,
        client: Arc<nadis::RedisClient>,
    ) -> anyhow::Result<Self> {
        // 广播资源完整就绪后再替换运行时，避免订阅建立失败时覆盖仍可工作的上一代。
        let broadcast = start_invalidate_broadcast_with_managed_redis(client).await?;
        let publisher = Some(broadcast.publisher.clone());
        let local_work = local_work::LocalWork::install()?;
        let owner = CacheRuntime::install_owned(layer, publisher);
        Ok(Self {
            broadcast: Some(broadcast),
            owner,
            local_work,
        })
    }

    /// 业务作用：按依赖顺序关闭本地后台工作与广播，最后凭 owner 身份收回全局缓存入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地任务退出、广播关闭且所属运行时已撤销；旧 owner 不撤销新运行时。
    /// 消费句柄，等待期限由宿主设置；中途取消会触发 Drop 收回入口并请求中止广播，不证明所有任务已退出。
    pub async fn shutdown(mut self) {
        // 先阻止并等待本地后台工作，避免刷新继续使用已进入关闭流程的广播与后端。
        self.local_work.close();
        self.local_work.wait().await;
        if let Some(broadcast) = self.broadcast.take() {
            broadcast.shutdown().await;
        }
        // 旧 guard 与新 guard 可能短暂重叠；只允许当前 owner 撤销全局槽。
        self.local_work.close();
        CacheRuntime::revoke_if_owner(self.owner);
    }
}

impl Drop for CacheRuntimeGuard {
    /// 业务作用：关闭本 guard 的本地任务准入并撤销仍属于自己的运行时，由广播句柄析构请求中止任务。
    ///
    /// 参数说明：无。
    ///
    /// 返回：取消本地工作并按 owner 匹配结果撤销槽；不会等待任务实际退出，也不会撤销后续 owner 的运行时。
    fn drop(&mut self) {
        // 正常 shutdown 和被取消/直接 drop 共用同一 owner fencing；重复撤销只会返回 false。
        // broadcast 的 Drop 会停止并 abort 尚未 join 的后台任务。
        self.local_work.close();
        CacheRuntime::revoke_if_owner(self.owner);
    }
}

/// 业务作用：建立普通 Redis 发布连接并启动有界发布任务与可重连订阅任务，交由调用方统一关闭。
///
/// 参数说明：
/// - `redis_url`：用于发布和独立订阅连接的普通 Redis 地址。
///
/// 返回：发布建连成功后返回广播句柄；地址无效或发布建连失败返回错误，订阅首次建连和失败重试发生在后台。
/// 调用方须持有句柄并显式关闭；此入口不发布全局运行时槽，队列接纳与订阅存活也不保证远端收到消息。
pub async fn start_invalidate_broadcast(redis_url: &str) -> anyhow::Result<InvalidateBroadcast> {
    // 发布端:一条多路复用连接 + 有界队列 + 单 drainer 任务。业务侧只做非阻塞入队,
    // drainer 负责真正 PUBLISH;满队列丢弃并记日志,不再每次调用 detached spawn。
    let client = redis::Client::open(redis_url)?;
    let mut publisher_conn = client.get_multiplexed_async_connection().await?;
    let (publisher, mut receiver) = BoundedInvalidatePublisher::channel(INVALIDATE_QUEUE_CAPACITY);

    let publisher_stop = std::sync::Arc::new(tokio::sync::Notify::new());
    let drainer_stop = publisher_stop.clone();
    let publisher_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = drainer_stop.notified() => {
                    // 停机:best-effort 排空队列里剩余的失效再退出。
                    while let Ok(message) = receiver.try_recv() {
                        let payload = serde_json::to_string(&message)
                            .unwrap_or_else(|_| "{\"scene\":\"\",\"key\":\"\"}".to_owned());
                        let _: Result<usize, _> =
                            publisher_conn.publish(INVALIDATE_CHANNEL, payload).await;
                    }
                    return;
                }
                maybe = receiver.recv() => match maybe {
                    Some(message) => {
                        let payload = serde_json::to_string(&message)
                            .unwrap_or_else(|_| "{\"scene\":\"\",\"key\":\"\"}".to_owned());
                        // publish 返回订阅者数;失败只忽略(远端广播 best-effort,本地 L1 已失效)。
                        let _: Result<usize, _> =
                            publisher_conn.publish(INVALIDATE_CHANNEL, payload).await;
                    }
                    None => return, // 全部 sender 释放
                },
            }
        }
    });

    // 订阅端:独立 client(SUBSCRIBE 独占连接);重连等待也参与取消,停机时不必干等满一个退避周期。
    let sub_url = redis_url.to_string();
    // 单消费者各保存一个停机 permit，使任务尚未首次运行时收到的关闭通知仍然有效。
    let stop = std::sync::Arc::new(tokio::sync::Notify::new());
    let task_stop = stop.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = task_stop.notified() => return,
                result = run_subscriber(&sub_url) => {
                    if let Err(e) = result {
                        tracing::warn!("cacheable 失效订阅中断,3s 后重连: {}", e);
                    }
                }
            }
            // 退避等待同样可取消:停机不必干等满一个重连周期。
            tokio::select! {
                _ = task_stop.notified() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {}
            }
        }
    });
    tracing::info!("cacheable 失效广播已启动(频道 {})", INVALIDATE_CHANNEL);
    Ok(InvalidateBroadcast {
        stop,
        handle,
        publisher_stop,
        publisher_handle,
        publisher,
    })
}

/// 业务作用：使用宿主受管 Redis 客户端启动失效广播，并把派生订阅连接纳入可停止句柄。
///
/// 参数说明：
/// - `client`: 已由宿主完成启动探测并负责最终释放的共享客户端。
///
/// 返回：首次订阅成功后返回广播生命周期句柄；首次订阅失败时不启动后台任务。
#[cfg(feature = "managed-redis")]
pub async fn start_invalidate_broadcast_with_managed_redis(
    client: Arc<nadis::RedisClient>,
) -> anyhow::Result<InvalidateBroadcast> {
    // 首次订阅必须在返回前成功，保证组件 Ready 不会掩盖错误的实例引用或不可用的 Pub/Sub。
    let initial_subscription = client.sub(&[INVALIDATE_CHANNEL]).await?;
    let (publisher, mut receiver) = BoundedInvalidatePublisher::channel(INVALIDATE_QUEUE_CAPACITY);

    let publisher_stop = Arc::new(tokio::sync::Notify::new());
    let drainer_stop = publisher_stop.clone();
    let publisher_client = client.clone();
    let publisher_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = drainer_stop.notified() => {
                    // 停机时尽力排空已接纳消息；外层预算耗尽会丢弃 guard 并中止本任务。
                    while let Ok(message) = receiver.try_recv() {
                        let payload = serde_json::to_string(&message)
                            .unwrap_or_else(|_| "{\"scene\":\"\",\"key\":\"\"}".to_owned());
                        let _ = publisher_client.r#pub(INVALIDATE_CHANNEL, payload).await;
                    }
                    return;
                }
                maybe = receiver.recv() => match maybe {
                    Some(message) => {
                        let payload = serde_json::to_string(&message)
                            .unwrap_or_else(|_| "{\"scene\":\"\",\"key\":\"\"}".to_owned());
                        // Pub/Sub 是尽力通道，发布失败不反向改变已经完成的本地失效。
                        let _ = publisher_client.r#pub(INVALIDATE_CHANNEL, payload).await;
                    }
                    None => return,
                },
            }
        }
    });

    let stop = Arc::new(tokio::sync::Notify::new());
    let task_stop = stop.clone();
    let handle = tokio::spawn(run_managed_subscriber(
        client,
        initial_subscription,
        task_stop,
    ));
    tracing::info!(
        "cacheable 失效广播已通过受管 Redis 启动(频道 {})",
        INVALIDATE_CHANNEL
    );
    Ok(InvalidateBroadcast {
        stop,
        handle,
        publisher_stop,
        publisher_handle,
        publisher,
    })
}

/// 业务作用：持续消费受管 Redis 订阅并在断线后重建专用连接，直至收到停机信号。
///
/// 参数说明：
/// - `client`: 用于重新派生专用订阅连接的受管客户端。
/// - `subscription`: 启动门禁阶段已经成功建立的首次订阅。
/// - `stop`: 广播拥有者发出的停机信号。
///
/// 返回：收到停机信号后正常结束；运行期连接失败会记录并退避，不终止监督任务。
#[cfg(feature = "managed-redis")]
async fn run_managed_subscriber(
    client: Arc<nadis::RedisClient>,
    mut subscription: nadis::Subscription,
    stop: Arc<tokio::sync::Notify>,
) {
    loop {
        loop {
            tokio::select! {
                biased;
                _ = stop.notified() => return,
                message = subscription.next_message() => match message {
                    Some(message) => apply_invalidate_payload(message.as_str().as_ref()),
                    None => break,
                },
            }
        }
        tracing::warn!("cacheable 受管 Redis 失效订阅中断,3s 后重连");
        tokio::select! {
            _ = stop.notified() => return,
            _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {}
        }
        // 每次从受管客户端重新派生专用连接，避免断开的订阅对象残留旧连接状态。
        loop {
            tokio::select! {
                biased;
                _ = stop.notified() => return,
                result = client.sub(&[INVALIDATE_CHANNEL]) => match result {
                    Ok(next) => {
                        subscription = next;
                        break;
                    }
                    Err(_) => {
                        // 连接错误可能携带 endpoint 或认证上下文；运行期日志只公开稳定状态，
                        // 具体资源身份由 Application readiness 的有界 qualifier 负责归因。
                        tracing::warn!("cacheable 受管 Redis 失效订阅重连失败");
                    }
                },
            }
            tokio::select! {
                _ = stop.notified() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {}
            }
        }
    }
}

/// 业务作用：解析一条失效广播载荷并删除本节点对应的 L1 条目。
///
/// 参数说明：
/// - `payload`: Redis Pub/Sub 收到的 UTF-8 JSON 载荷。
///
/// 返回：无返回值；无法解析的非协议载荷被忽略，不影响后续订阅消息。
fn apply_invalidate_payload(payload: &str) {
    if let Ok(message) = serde_json::from_str::<InvalidateMessage>(payload) {
        local_cache::remove_any(&message.scene, &message.key);
        tracing::debug!(
            scene = %message.scene,
            "cacheable 收到失效广播并已删本地 L1"
        );
    }
}

/// 业务作用：在专用 Redis 订阅连接上接收失效 JSON，并向本节点 L1 提交对应失效操作。
///
/// 参数说明：
/// - `redis_url`：用于建立普通 Redis 专用订阅连接的地址。
///
/// 返回：持续消费直到建连、订阅失败或消息流结束，返回错误交给外层重连；非字符串或无效 JSON 载荷跳过。
async fn run_subscriber(redis_url: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?; // 异步 pub/sub 连接
    pubsub.subscribe(INVALIDATE_CHANNEL).await?;
    let mut stream = pubsub.on_message(); // 消息流
    while let Some(msg) = stream.next().await {
        // JSON 编码避免 scene/key 自身含分隔符时误删另一个 key。
        let payload: String = match msg.get_payload() {
            Ok(p) => p,
            Err(_) => continue, // 非字符串载荷,跳过
        };
        apply_invalidate_payload(&payload);
    }
    // 流结束 = 连接断,返回 Err 触发上面的重连
    anyhow::bail!("pubsub 流结束(连接断开)")
}

/// 业务作用：向当前运行时的有界发布器尽力提交失效通知，使远端节点有机会撤销相同缓存键。
///
/// 参数说明：
/// - `scene`：失效事件对应的缓存场景名。
/// - `key`：各节点应失效的完整缓存键。
///
/// 返回：不等待网络发布；未安装发布器时跳过，队列满或接收端关闭时丢弃，不改变已执行的缓存操作结果。
fn publish_invalidate(scene: &str, key: &str) {
    // 非阻塞入有界队列;未启动广播(无 PUBLISHER)则跳过 → 退化为仅本地失效。
    // 满队列/drainer 已停时 try_publish 内部记日志并丢弃,调用方不阻塞、不 panic。
    if let Some(publisher) = CacheRuntime::publisher() {
        let _ = publisher.try_publish(scene, key);
    }
}

// ── re-export 过程宏 ──
pub use nacache_macro::{cache_invalidate, cached};

// ── 缓存场景使用 descriptor 的编译期收集──
// `#[cached]` 除生成读路径 wrapper 外,还额外注册一条**静态** `CacheSceneUsage`(不改写业务函数);
// 运行时/组件可遍历 [`scene_usages`] 做启动断言，例如拒绝同一 scene 声明不同 value 类型。

/// 一条 `#[cached]` 声明的缓存场景使用元信息(字段全 `'static`,可放进 static 被 linkme 收集)。
#[derive(Clone, Copy)]
pub struct CacheSceneUsage {
    /// L1 本地缓存池名 / 缓存场景名。
    pub scene: &'static str,
    /// 缓存值类型名(源码文本形式,便于诊断)。
    pub value_type_name: &'static str,
    /// 缓存值类型的运行时 `TypeId`,用于检测同一 scene 被赋予不同值类型。
    pub value_type_id: fn() -> core::any::TypeId,
    /// L1 软刷新阈值(毫秒)。
    pub refresh_ms: u64,
    /// L1 硬过期(毫秒)。
    pub expire_ms: u64,
    /// 被注解的 handler 函数名。
    pub handler: &'static str,
}

/// `#[cached]` 生成的 [`CacheSceneUsage`] 的编译期收集数组(跨 crate,由 linkme 汇聚)。
#[linkme::distributed_slice]
pub static CACHE_SCENE_USAGES: [CacheSceneUsage] = [..];

/// 业务作用：返回本进程编译期收集到的全部缓存场景使用 descriptor。
///
/// 参数说明：无。
///
/// 返回：链接时汇集的静态声明切片；顺序由链接器决定，可包含同一场景的多处声明，调用方按需排序与核对。
pub fn scene_usages() -> &'static [CacheSceneUsage] {
    &CACHE_SCENE_USAGES
}

/// scene 一致性审计失败详情:同名 scene 被多处 `#[cached]` 赋予不一致的值类型或 TTL 合同。
#[derive(Debug, Clone)]
pub struct SceneAuditError {
    /// 发生冲突的 scene 名。
    pub scene: &'static str,
    /// 不一致维度与两侧 handler 的可读描述(不含业务数据)。
    pub detail: String,
}

impl std::fmt::Display for SceneAuditError {
    /// 业务作用：输出稳定、无业务数据的冲突摘要。
    ///
    /// 参数说明：
    /// - `formatter`：错误展示的格式化目标，用于写入场景名和声明冲突详情。
    ///
    /// 返回：摘要写入成功或格式化目标的写入错误，不读取缓存内容或连接配置。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cache scene `{}` has inconsistent #[cached] declarations: {}",
            self.scene, self.detail
        )
    }
}

impl std::error::Error for SceneAuditError {}

/// 业务作用：在启动阶段核对同名缓存场景的值类型及过期配置，提前拒绝类型冲突和配置歧义。
///
/// 参数说明：无。
///
/// 返回：全部编译期声明一致时成功；按场景及 handler 排序后返回首个值类型或刷新、过期时长冲突。
/// 只核对声明间一致性，不验证时间范围、不建立缓存，也不覆盖绕过宏直接调用本地缓存的场景。
pub fn audit_scenes() -> Result<(), SceneAuditError> {
    use std::collections::BTreeMap;
    let mut by_scene: BTreeMap<&'static str, Vec<&'static CacheSceneUsage>> = BTreeMap::new();
    for usage in scene_usages() {
        by_scene.entry(usage.scene).or_default().push(usage);
    }
    for (_scene, mut usages) in by_scene {
        usages.sort_by_key(|usage| usage.handler);
        let head = usages[0];
        for usage in &usages[1..] {
            // 同名场景共享类型擦除状态；类型或过期配置分歧必须在开放业务前拒绝。
            if (head.value_type_id)() != (usage.value_type_id)() {
                return Err(SceneAuditError {
                    scene: usage.scene,
                    detail: format!(
                        "value type mismatch: handler `{}` uses `{}` but handler `{}` uses `{}`",
                        head.handler, head.value_type_name, usage.handler, usage.value_type_name
                    ),
                });
            }
            if head.refresh_ms != usage.refresh_ms || head.expire_ms != usage.expire_ms {
                return Err(SceneAuditError {
                    scene: usage.scene,
                    detail: format!(
                        "TTL contract mismatch: handler `{}` declares refresh_ms={}/expire_ms={} but handler `{}` declares refresh_ms={}/expire_ms={}",
                        head.handler, head.refresh_ms, head.expire_ms,
                        usage.handler, usage.refresh_ms, usage.expire_ms
                    ),
                });
            }
        }
    }
    Ok(())
}

/// 宏展开专用的第三方依赖桥。**不属于稳定业务 API**。
#[doc(hidden)]
pub mod __private {
    pub use linkme;
    pub use tracing;
}
