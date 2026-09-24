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

// ============================================================================
// rust-cache/cacheable.rs —— Cacheable-lite 的【运行期支持】
//
// #[cached] / #[cache_invalidate] 这两个宏(在 cache-macro crate)只负责"拼 key + 调用",
// 真正的两级缓存逻辑在这里。三件事:
//   ① init(layer)            —— main 启动时把 L2(CacheLayer)注入静态变量
//   ② get_or_load_2level(..) —— 读:L1(local_cache moka)→ L2(CacheLayer Redis 三防)→ loader(DB)
//   ③ invalidate(..)         —— 写后删除 L1/L2，并通过 Redis Pub/Sub 广播跨节点失效。
// ============================================================================

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use serde::de::DeserializeOwned;
use serde::Serialize;

// 同目录的兄弟模块(都是 rcache 的子模块),用 super:: 引用
use crate::cache::CacheLayer; // L2:Redis 三防 cache-aside

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
    /// 失效广播有界发布器;`start_invalidate_broadcast` 注入(换代覆盖——二次启动广播不再被旧值挡住)。
    publisher: RwLock<Option<Arc<BoundedInvalidatePublisher>>>,
    /// 显式 record_invalidation 使用的持久意图入口；执行失效不会调用它。
    durable_sink: RwLock<Option<Arc<dyn DurableInvalidationSink>>>,
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

    /// 业务作用：换代覆盖 L2 后端(无条件写入 + generation+1)。
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

    /// 业务作用：仅当前 owner 匹配时撤销；返回是否确实撤销。
    fn revoke_if_owner(owner: u64) -> bool {
        let runtime = Self::global();
        let _transition = runtime
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    /// 业务作用：取 durable 失效 sink(clone 当前代的 Arc)。
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
    /// 业务作用：持久记录一次失效。
    ///
    /// # 参数
    /// - `scene`: 缓存场景名。
    /// - `key`: 完整缓存 key。
    ///
    /// # 错误
    ///
    /// 持久化失败时返回错误；调用方必须传播到业务事务，不得视为已经记录。
    async fn record(&self, scene: &str, key: &str) -> anyhow::Result<()>;
}

/// 业务作用：为独立使用方安装持久意图策略，避免覆盖受管 owner。
/// 参数说明：`sink` 为参与同源事务的意图记录器。
/// 返回：非受管槽安装成功；受管 owner 存在时拒绝且保留现有策略。
pub fn set_durable_invalidation_sink(sink: Arc<dyn DurableInvalidationSink>) -> anyhow::Result<()> {
    let runtime = CacheRuntime::global();
    let _transition = runtime
        .transition
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// 业务作用：换代安装 L2 后端:无条件覆盖当前槽并 generation+1。
///
/// 与首装且重复调用会被忽略的 [`init`] 不同,本入口用于**重新装配**(组件重启/重新装配):
/// 新请求立刻使用新后端,
/// 已借出的旧 `Arc` 持旧代直至释放。
///
/// # 参数
/// - `layer`: 新一代 L2 缓存层。
pub fn install_generation(layer: Arc<CacheLayer>) {
    CacheRuntime::replace_backend(layer);
}

/// 业务作用：撤销缓存运行时:清空 L2 后端/广播发布器/durable sink 三槽并 generation+1。
///
/// 撤销后宏入口([`get_or_load_2level`]/[`invalidate`])返回明确错误(不 panic);同进程随后可经
/// [`init`]/[`install_generation`] 重新装配。`CacheRuntimeGuard::shutdown` 停机时自动调用。
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
    pub fn snapshot(self) -> CacheRuntimeSnapshot {
        runtime_snapshot()
    }

    /// 业务作用：对当前代 L2 后端执行只读健康探针。
    pub async fn health_check(self) -> anyhow::Result<()> {
        let backend = CacheRuntime::backend()
            .ok_or_else(|| anyhow::anyhow!("cache runtime has no installed backend"))?;
        backend.health_check().await
    }
}

/// 业务作用：返回进程级缓存运行时的只读句柄。
pub const fn cache_handle() -> CacheHandle {
    CacheHandle
}

/// 业务作用：当前运行时代次(任何 install/revoke 单调 +1;0 = 从未装配)。诊断用。
pub fn runtime_generation() -> u64 {
    CacheRuntime::global().generation.load(Ordering::Acquire)
}

/// 业务作用：返回不含连接信息、业务 key 或 secret 的运行时摘要。
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

/// 业务作用：注入 L2 缓存层。★ main 构造好 CacheLayer 后调用一次(见 main.rs)。
///
/// # 参数
/// - `layer`: 已建好的 `Arc<CacheLayer>`,内部持有 Redis 集群连接和 TTL 配置。
pub fn init(layer: Arc<CacheLayer>) {
    // 只注入一次:重复注入被忽略(保持既有语义)。
    CacheRuntime::set_backend(layer);
}

/// 业务作用：取当前代 L2 句柄(clone 一份 Arc)。未装配/已撤销返回明确错误(撤销后宏入口不 panic)。
fn try_l2() -> anyhow::Result<Arc<CacheLayer>> {
    CacheRuntime::backend().ok_or_else(|| {
        anyhow::anyhow!(
            "cacheable 未初始化或已撤销:声明 `cache` 组件(或调用 init/install_generation)后再使用缓存宏"
        )
    })
}

/// 业务作用：两级缓存读取(#[cached] 生成的代码调它)。
///
/// 数据流:**L1(moka,µs 级,返旧值+后台刷)→ miss/刷新走 L2(Redis 三防)→ 再 miss 跑 loader(查 DB)+ 双回填**。
///
/// 参数:
///   scene      L1 池名(local_cache 按 scene 分池)
///   key        完整缓存 key(宏已用模板拼好,如 "kline:BTCUSDC:1")
///   refresh_ms L1 软刷新阈值(到点先返旧值、后台异步重载)
///   expire_ms  L1 硬过期(到点 moka 淘汰,下次必重载)
///   loader     回源闭包 = 被注解函数的原体(查 DB),返回 `anyhow::Result<T>`
///
/// 泛型约束:
///   T: Serialize + DeserializeOwned —— 要存进/读出 L2(Redis 里是 JSON 文本)
///      + Clone                      —— L1 存 `Arc<T>`,返回时 clone 出 T
///      + Send + Sync + 'static      —— 要放进 moka(并发缓存)、能跨 await/线程
///   F/Fut: + Send + 'static         —— loader 可能被 L1 的【后台刷新任务】(tokio::spawn)持有,故必须 'static+Send
///
/// # 参数
/// - `scene`: L1 本地缓存池名,同一业务缓存读写失效必须使用同一个 scene。
/// - `key`: 完整缓存 key,宏已按模板拼好并同时用于 L1 和 L2。
/// - `refresh_ms`: L1 软刷新阈值毫秒数,到点后可返回旧值并触发刷新。
/// - `expire_ms`: L1 硬过期毫秒数,到点后本地条目被淘汰并阻塞重载。
/// - `loader`: L1/L2 都未命中时执行的真实回源闭包。
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
    //   sync=false:刷新走后台(请求侧永不阻塞),所以闭包要 Send+'static(上面已约束)。
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

/// 业务作用：失效 key 对应的 L1 + L2(#[cache_invalidate] 生成的代码调它)。
///
/// 参数:
///   scene  L1 池名(要和对应 #[cached] 一致)
///   key    完整缓存 key(宏已拼好)
/// 泛型 `V` 保留宏调用签名；实际按 scene/key 清理。即时失效不代表外层事务已提交，
/// 也不自动记录持久意图。需要事务语义时显式调用 `record_invalidation` 并传播错误。
///
/// # 参数
/// - `scene`: L1 本地缓存池名,必须与对应读取入口使用的 scene 一致。
/// - `key`: 完整缓存 key,宏已按模板拼好。
pub async fn invalidate<V>(scene: &'static str, key: String) -> anyhow::Result<()>
where
    V: Send + Sync + 'static,
{
    apply_invalidation(scene, &key).await
}

/// 业务作用：仅持久记录失效意图，使调用方可在业务事务中原子提交意图与数据。
/// 参数说明：`scene` 为缓存场景；`key` 为完整缓存身份。
/// 返回：sink 成功记录时成功；未安装或记录失败直接返回错误，不访问 L1、L2 或广播队列。
pub async fn record_invalidation(scene: &str, key: &str) -> anyhow::Result<()> {
    let sink = CacheRuntime::durable_sink()
        .ok_or_else(|| anyhow::anyhow!("durable invalidation sink is not installed"))?;
    sink.record(scene, key).await
}

/// 业务作用：执行已提交的失效意图，供 dispatcher 和显式即时失效共用。
/// 参数说明：`scene` 为缓存场景；`key` 为完整缓存身份。
/// 返回：L2 删除成功后清理本节点 L1 并尝试广播；纯 L1 直接清理本节点。
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
//   写节点 invalidate 时,除了删本地 L1 + 删 L2,还 PUBLISH 一条 {scene|key} 到固定频道;
//   每个节点启动时都 spawn 一个订阅任务,收到广播就 remove_any(删本节点 L1)。
// 这样某节点改了数据,所有节点的 L1 都会被清掉,而不是只清写节点自己的。
//
// 注:用一条【专用的普通 redis 连接】做 pub/sub(SUBSCRIBE 会独占连接,不能复用 CacheLayer 的命令连接;
//   且 pub/sub 用单节点连接即可——Redis Cluster 会把普通 PUBLISH 跨节点传播给所有订阅者)。

use futures_util::StreamExt;
use redis::AsyncCommands; // 提供 conn.publish(...) // 提供 pubsub 消息流的 .next().await

// 失效广播频道名(所有节点订阅同一个)
const INVALIDATE_CHANNEL: &str = "cacheable:invalidate";

/// 失效广播有界队列的默认容量。突发失效在此上限内缓冲;超过按策略丢弃并记日志,
/// 不再像旧实现每次调用 `tokio::spawn` 制造无限 detached 任务。
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
/// 取代旧的"每次调用 `tokio::spawn` 且丢句柄":满队列按策略丢弃并记"远端广播未保证",
/// 不再制造无限 detached 任务;drainer 的取消/join 由持有 [`InvalidateBroadcast`] 的一方负责。
#[derive(Clone)]
pub struct BoundedInvalidatePublisher {
    sender: tokio::sync::mpsc::Sender<InvalidateMessage>,
}

impl BoundedInvalidatePublisher {
    /// 业务作用：创建发布器与接收端；非 fallible 入口把容量收敛到 Tokio 有界队列可表达范围。
    ///
    /// # 参数
    ///
    /// - `capacity`:有界队列容量,必须大于 0。
    pub fn channel(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<InvalidateMessage>) {
        let capacity = capacity.clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (Self { sender }, receiver)
    }

    /// 业务作用：非阻塞入队一条失效消息;队列满或 drainer 已停时丢弃并返回对应结果(绝不阻塞、不 panic)。
    ///
    /// # 参数
    ///
    /// - `scene`:缓存事件所属业务场景名。
    /// - `key`:需从各节点 L1 删除的业务缓存 key。
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
/// 由本 crate 提供而不是让调用方自己拼:`CacheLayer` 与 mapper L2 吃的都是
/// `redis::cluster_async::ClusterConnection`,这是个具体的第三方类型——调用方自建就必须直接依赖
/// 同一个 redis crate 版本,版本一错类型就不同一。这里集中建连,宿主只需要拿到不透明的连接值。
///
/// # 参数
/// - `url`: Redis Cluster 连接串;逗号分隔可给多个种子节点。
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
    stop: std::sync::Arc<tokio::sync::Notify>,
    handle: tokio::task::JoinHandle<()>,
    /// 发布端 drainer 的停止信号与句柄;停机时先让它排空有界队列再退出。
    publisher_stop: std::sync::Arc<tokio::sync::Notify>,
    publisher_handle: tokio::task::JoinHandle<()>,
    /// 尚未发布到全局 runtime 的有界发布器；由 owner 安装阶段统一发布。
    publisher: BoundedInvalidatePublisher,
}

impl InvalidateBroadcast {
    /// 业务作用：通知发布 drainer 与订阅循环退出并等待二者真正结束。
    ///
    /// 消费 self:句柄只能用一次,避免"关了又关"或关完还以为任务在跑。先停发布 drainer(它会排空
    /// 有界队列里剩余的失效再退出,best-effort flush),再停订阅循环。
    ///
    /// # 参数
    ///
    /// 本方法无参数;等待上限由调用方在外层用 timeout 施加。
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
    /// 业务作用：在显式 shutdown 未完成时取消并终止两个后台任务，防止失效广播越过拥有者生命周期。
    fn drop(&mut self) {
        // shutdown future 被外层 deadline 取消时，JoinHandle 的普通 Drop 只会 detach。兜底必须同时
        // 发取消信号并 abort 两个任务，保证 listener/Redis I/O 不越过拥有者生命周期。
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
    local_work: Arc<local_work::LocalWork>,
    broadcast: Option<InvalidateBroadcast>,
    owner: u64,
}

impl CacheRuntimeGuard {
    /// 业务作用：装配 L2 后端并(可选)启动失效广播,返回统一生命周期句柄。
    ///
    /// # 参数
    ///
    /// - `layer`: 已建好的 L2 缓存层(Redis 三防)。
    /// - `broadcast_url`: `Some(url)` 时启动跨实例 L1 失效广播;`None` 则仅本地失效。
    ///
    /// # 错误
    ///
    /// 广播发布/订阅连接建立失败时返回错误。
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

    /// 业务作用：停机:排空并停止失效广播(发布 drainer + 订阅循环)。消费 self,只能停一次。
    ///
    /// # 参数
    ///
    /// 本方法无参数;等待上限由调用方在外层用 timeout 施加。
    pub async fn shutdown(mut self) {
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
    /// 业务作用：撤销仍由本 guard 持有的全局 runtime，并由广播句柄兜底终止后台任务。
    fn drop(&mut self) {
        // 正常 shutdown 和被取消/直接 drop 共用同一 owner fencing；重复撤销只会返回 false。
        // broadcast 的 Drop 会停止并 abort 尚未 join 的后台任务。
        self.local_work.close();
        CacheRuntime::revoke_if_owner(self.owner);
    }
}

/// 业务作用：【B 步】启动失效广播:建发布连接 + 启动可取消的订阅任务,返回其生命周期句柄。
///
/// 调用方必须持有返回句柄并在停机时 shutdown；发布端句柄由拥有式 runtime guard 一起发布。
///
/// # 参数
/// - `redis_url`: 普通 Redis 连接串,用于发布和订阅 L1 失效广播。
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
    // 用 Notify 而不是 watch：每个单消费者只需一个可保存的停机 permit；拥有式句柄 Drop 会
    // 通知并 abort，legacy 兼容入口则显式 forget 整个句柄以维持旧的常驻语义。
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

/// 业务作用：订阅循环:SUBSCRIBE 频道 → 每收到一条 {scene|key} → 删本节点 L1。正常不返回(除非连接断)。
///
/// # 参数
/// - `redis_url`: 用于建立失效订阅连接的 Redis URL。
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

/// 业务作用：发布一条失效广播(fire-and-forget:发布失败不影响写操作,仅本地已失效)。
/// 未由拥有式 runtime guard 注入发布连接时跳过——退化为仅本地失效。
///
/// # 参数
/// - `scene`: 缓存事件所属的业务场景名称。
/// - `key`: 需要从本节点 L1 中删除的业务缓存 key。
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
/// # 返回
///
/// 静态切片;顺序由链接器决定,消费者需自行按 scene 排序/去重。
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
    /// # 参数
    /// - `formatter`: 目标格式化缓冲。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cache scene `{}` has inconsistent #[cached] declarations: {}",
            self.scene, self.detail
        )
    }
}

impl std::error::Error for SceneAuditError {}

/// 业务作用：审计编译期收集的全部 `#[cached]` scene descriptor:同一 scene 名下所有声明必须有**一致的
/// 值类型(`TypeId`)与 TTL 合同(`refresh_ms`/`expire_ms`)**。
///
/// 否则运行期 L1(按 scene 分池、类型擦除存 `Arc<dyn Any>`)会因值类型不一致 downcast panic,或因 TTL
/// 分歧产生难查的软刷新/过期行为。本审计把这类声明冲突**前移到启动期**一次性拒绝(结构化运行时错误仍是最后
/// 防线)。按 scene 名与 handler 名稳定排序后比对,冲突可复现。
///
/// # 返回
///
/// 全部一致返回 `Ok(())`;首个冲突返回 `Err(SceneAuditError)`。
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
