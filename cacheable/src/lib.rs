//! 两级缓存运行时支持。
//!
//! 提供本地 L1、Redis L2、single-flight、失效广播和宏展开所需的稳定入口。
/// Redis L2、分组缓存、序列化和分布式防击穿缓存层。
pub mod cache;
/// 进程内 L1 缓存和刷新/过期策略。
pub mod local_cache;

// ============================================================================
// rust-cache/cacheable.rs —— Cacheable-lite 的【运行期支持】
//
// #[cached] / #[cache_invalidate] 这两个宏(在 cache-macro crate)只负责"拼 key + 调用",
// 真正的两级缓存逻辑在这里。三件事:
//   ① init(layer)            —— main 启动时把 L2(CacheLayer)注入静态变量
//   ② get_or_load_2level(..) —— 读:L1(local_cache moka)→ L2(CacheLayer Redis 三防)→ loader(DB)
//   ③ invalidate(..)         —— 写后失效:删 L1 + 删 L2(B 步会在这再加 redis pub/sub 跨节点广播)
//
// 设计:rust-cache/00-Cacheable-lite设计.md
// ============================================================================

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use serde::de::DeserializeOwned;
use serde::Serialize;

// 同目录的兄弟模块(都是 rcache 的子模块),用 super:: 引用
use crate::cache::CacheLayer; // L2:Redis 三防 cache-aside

// ── 进程级缓存运行时──
// 把原先两个独立全局(L2 后端句柄 + 失效广播发布器)收拢进**单一** `CacheRuntime` 对象,
// 由一个全局 `OnceLock` 持有。`#[cached]`/`#[cache_invalidate]` 展开仍调 `get_or_load_2level`/
// `invalidate` 两个自由入口(宏契约不变),它们改从 runtime 读后端与发布器。
// 后端与发布器仍各用内部 `OnceLock` 两段设置,保持"init 后端 / 稍后起广播"的既有时序独立性;
// 后续第③步由 napp `CacheComponent` 构造并**拥有**该 runtime(不再是进程全局),接 readiness 与停机。

/// 进程级两级缓存运行时:统一持有 L2 后端、失效广播发布器与可选 durable 失效 sink。
///
/// **带 generation 的可撤销槽**:三个槽都可换代(install 覆盖)与撤销(revoke 清空),每次变更
/// 令 `generation` 单调 +1。宏入口每次调用读**当前代**;长借的 `Arc` 持旧代直至释放——旧请求继续用旧
/// 后端,新请求立刻用新后端,无一致性窗口。`CacheRuntimeGuard::shutdown` 撤销全部槽,同进程随后可再
/// 装配(重装/组件重启),不再受一次性 `OnceLock` 限制。
pub struct CacheRuntime {
    /// L2(Redis 三防)后端句柄;`init`(首装)/`install_generation`(换代)注入,`revoke_runtime` 清空。
    backend: RwLock<Option<Arc<CacheLayer>>>,
    /// 失效广播有界发布器;`start_invalidate_broadcast` 注入(换代覆盖——二次启动广播不再被旧值挡住)。
    publisher: RwLock<Option<Arc<BoundedInvalidatePublisher>>>,
    /// 可选 durable 失效 sink:失效动作先经它持久记录,再走尽力 pub/sub。
    durable_sink: RwLock<Option<Arc<dyn DurableInvalidationSink>>>,
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
    /// 返回进程级唯一运行时(懒初始化;槽内容由 CacheComponent/guard 拥有生命周期)。
    fn global() -> &'static CacheRuntime {
        static RUNTIME: OnceLock<CacheRuntime> = OnceLock::new();
        RUNTIME.get_or_init(|| CacheRuntime {
            backend: RwLock::new(None),
            publisher: RwLock::new(None),
            durable_sink: RwLock::new(None),
            generation: AtomicU64::new(0),
            owner: AtomicU64::new(0),
            next_owner: AtomicU64::new(0),
            transition: Mutex::new(()),
        })
    }

    /// 首装 L2 后端:槽空时写入并换代;已有则忽略(保持 `init` 的既有“重复注入被忽略”契约)。
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

    /// 换代覆盖 L2 后端(无条件写入 + generation+1)。
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

    /// 取 L2 后端句柄(clone 当前代的 Arc)。
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

    /// 由拥有式 guard 安装一代 backend + 可选 publisher，返回唯一 owner id。
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

    /// 仅当前 owner 匹配时撤销；返回是否确实撤销。
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

    /// 取失效广播发布器(clone 当前代的 Arc)。
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

    /// 取 durable 失效 sink(clone 当前代的 Arc)。
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

/// durable 失效 sink:把每次缓存失效**持久记录**到可重放通道。
///
/// pub/sub 广播是尽力而为(at-most-once):节点掉线/队列满时远端 L1 可能漏失效,只能靠 TTL 兜底。
/// 注册本 sink 后,[`invalidate`] 在广播**之前**先 `record`——典型实现把 `(scene, key)` 写进事务型 outbox
/// (业务事务内则与业务写同提交),由带重试/重放的 dispatcher 兜底投递失效,弥补广播丢失。
///
/// 本 trait 刻意窄(不依赖任何 outbox 类型):适配层把 `OutboxWriter`/`MySqlOutbox` 包成本 trait 即可。
/// `record` 返回错误时 [`invalidate`] 整体返错且**不发广播**(L1/L2 已删,重试 `invalidate` 幂等)。
#[async_trait::async_trait]
pub trait DurableInvalidationSink: Send + Sync {
    /// 持久记录一次失效。
    ///
    /// # 参数
    /// - `scene`: 缓存场景名。
    /// - `key`: 完整缓存 key。
    ///
    /// # 错误
    ///
    /// 持久化失败时返回错误;调用方(`invalidate`)会把错误上抛且不发广播。
    async fn record(&self, scene: &str, key: &str) -> anyhow::Result<()>;
}

/// 注册 durable 失效 sink(换代覆盖;`revoke_runtime` 一并清空)。
///
/// # 参数
/// - `sink`: 持久记录失效的 sink 实现(典型为 outbox 适配层)。
pub fn set_durable_invalidation_sink(sink: Arc<dyn DurableInvalidationSink>) {
    let runtime = CacheRuntime::global();
    let _transition = runtime
        .transition
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *runtime
        .durable_sink
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sink);
    runtime.generation.fetch_add(1, Ordering::AcqRel);
}

/// 换代安装 L2 后端:无条件覆盖当前槽并 generation+1。
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

/// 撤销缓存运行时:清空 L2 后端/广播发布器/durable sink 三槽并 generation+1。
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
    /// 返回当前代的配置/装配摘要，不暴露连接串或缓存 key。
    pub fn snapshot(self) -> CacheRuntimeSnapshot {
        runtime_snapshot()
    }

    /// 对当前代 L2 后端执行只读健康探针。
    pub async fn health_check(self) -> anyhow::Result<()> {
        let backend = CacheRuntime::backend()
            .ok_or_else(|| anyhow::anyhow!("cache runtime has no installed backend"))?;
        backend.health_check().await
    }
}

/// 返回进程级缓存运行时的只读句柄。
pub const fn cache_handle() -> CacheHandle {
    CacheHandle
}

/// 当前运行时代次(任何 install/revoke 单调 +1;0 = 从未装配)。诊断用。
pub fn runtime_generation() -> u64 {
    CacheRuntime::global().generation.load(Ordering::Acquire)
}

/// 返回不含连接信息、业务 key 或 secret 的运行时摘要。
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

/// 注入 L2 缓存层。★ main 构造好 CacheLayer 后调用一次(见 main.rs)。
///
/// # 参数
/// - `layer`: 已建好的 `Arc<CacheLayer>`,内部持有 Redis 集群连接和 TTL 配置。
pub fn init(layer: Arc<CacheLayer>) {
    // 只注入一次:重复注入被忽略(保持既有语义)。
    CacheRuntime::set_backend(layer);
}

/// 取当前代 L2 句柄(clone 一份 Arc)。未装配/已撤销返回明确错误(撤销后宏入口不 panic)。
fn try_l2() -> anyhow::Result<Arc<CacheLayer>> {
    CacheRuntime::backend().ok_or_else(|| {
        anyhow::anyhow!(
            "cacheable 未初始化或已撤销:声明 `cache` 组件(或调用 init/install_generation)后再使用缓存宏"
        )
    })
}

/// 两级缓存读取(#[cached] 生成的代码调它)。
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

/// 失效 key 对应的 L1 + L2(#[cache_invalidate] 生成的代码调它)。
///
/// 参数:
///   scene  L1 池名(要和对应 #[cached] 一致)
///   key    完整缓存 key(宏已拼好)
/// 泛型 V:该 scene 缓存的值类型——L1 是按 <K,V> 强类型分池的,删除要用相同 V 才能 downcast 到那个池
///        (所以 #[cache_invalidate] 必须传 value=)。B 步会用 pub/sub + 非泛型失效绕开它。
///
/// # 参数
/// - `scene`: L1 本地缓存池名,必须与对应读取入口使用的 scene 一致。
/// - `key`: 完整缓存 key,宏已按模板拼好。
pub async fn invalidate<V>(scene: &'static str, key: String) -> anyhow::Result<()>
where
    V: Send + Sync + 'static,
{
    // 失效顺序【先 L2 后 L1】:先删共享 L2(下游真源),再删本节点 L1。
    // 若反过来(先 L1 后 L2),两步之间本节点的并发读会 L1 miss → 从【尚未删除的】L2 载入旧值 →
    // 把旧值重新灌回 L1,失效完成后 L1 反而残留脏值。先删 L2 则此刻 L1 仍持旧值(读命中旧值、不回源),
    // 待 ② 删除收尾;且 L2 删除失败时直接返错、L1 不动,两级保持一致的旧值可重试(不会 L1 空/L2 脏)。
    // ① 失效 L2:redis DEL 这个 key(共享 Redis,所有节点的 L2 都没了)。失败即返错,不再动 L1。
    try_l2()?.delete(&key).await?;
    // ② 失效 L1:从该 scene 的 moka 池里 invalidate 这个 key(本节点)。
    local_cache::remove::<String, V>(scene, key.clone()).await;
    // ③【durable,可选】持久记录失效:先可靠层后尽力层——sink 失败即返错且不发广播
    //    (L1/L2 已删,重试本函数幂等),由 outbox 等带重放的通道兜底广播丢失。
    if let Some(sink) = CacheRuntime::durable_sink() {
        sink.record(scene, &key).await?;
    }
    // ④【B 步】广播失效:redis PUBLISH,让【其它节点】清理它们各自的 L1(本节点 L1 已在 ② 清理)。
    //    解决多节点 L1 不一致:不再只靠短 TTL 兜底。订阅端见 spawn_invalidate_subscriber。
    publish_invalidate(scene, &key);
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// B 步:跨节点 L1 失效广播(redis pub/sub)
// ════════════════════════════════════════════════════════════════════════════
// 思路(对标 Cacheable cacheType=BOTH 的本地缓存失效广播):
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
    /// 创建发布器与接收端；非 fallible 入口把容量收敛到 Tokio 有界队列可表达范围。
    ///
    /// # 参数
    ///
    /// - `capacity`:有界队列容量,必须大于 0。
    pub fn channel(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<InvalidateMessage>) {
        let capacity = capacity.clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (Self { sender }, receiver)
    }

    /// 非阻塞入队一条失效消息;队列满或 drainer 已停时丢弃并返回对应结果(绝不阻塞、不 panic)。
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

/// 建立两级缓存 L2 使用的 Redis Cluster 连接。
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
    /// 通知发布 drainer 与订阅循环退出并等待二者真正结束。
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
    /// 在显式 shutdown 未完成时取消并终止两个后台任务，防止失效广播越过拥有者生命周期。
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
/// 收进单一对象,是迈向 napp `CacheComponent` 托管的中间形态(组件届时构造并持有本 guard)。
pub struct CacheRuntimeGuard {
    broadcast: Option<InvalidateBroadcast>,
    owner: u64,
}

impl CacheRuntimeGuard {
    /// 装配 L2 后端并(可选)启动失效广播,返回统一生命周期句柄。
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
        let owner = CacheRuntime::install_owned(layer, publisher);
        Ok(Self { broadcast, owner })
    }

    /// 停机:排空并停止失效广播(发布 drainer + 订阅循环)。消费 self,只能停一次。
    ///
    /// # 参数
    ///
    /// 本方法无参数;等待上限由调用方在外层用 timeout 施加。
    pub async fn shutdown(mut self) {
        if let Some(broadcast) = self.broadcast.take() {
            broadcast.shutdown().await;
        }
        // 旧 guard 与新 guard 可能短暂重叠；只允许当前 owner 撤销全局槽。
        let _ = CacheRuntime::revoke_if_owner(self.owner);
    }
}

impl Drop for CacheRuntimeGuard {
    /// 撤销仍由本 guard 持有的全局 runtime，并由广播句柄兜底终止后台任务。
    fn drop(&mut self) {
        // 正常 shutdown 和被取消/直接 drop 共用同一 owner fencing；重复撤销只会返回 false。
        // broadcast 的 Drop 会停止并 abort 尚未 join 的后台任务。
        let _ = CacheRuntime::revoke_if_owner(self.owner);
    }
}

/// 【B 步】启动失效广播:建发布连接 + 启动可取消的订阅任务,返回其生命周期句柄。
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

/// 订阅循环:SUBSCRIBE 频道 → 每收到一条 {scene|key} → 删本节点 L1。正常不返回(除非连接断)。
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
        if let Ok(message) = serde_json::from_str::<InvalidateMessage>(&payload) {
            // 删【本节点】L1(非泛型,按 scene+key;见 local_cache::remove_any)
            local_cache::remove_any(&message.scene, &message.key);
            tracing::debug!(
                scene = %message.scene,
                "cacheable 收到失效广播并已删本地 L1"
            );
        }
    }
    // 流结束 = 连接断,返回 Err 触发上面的重连
    anyhow::bail!("pubsub 流结束(连接断开)")
}

/// 发布一条失效广播(fire-and-forget:发布失败不影响写操作,仅本地已失效)。
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
// 运行时/组件可遍历 [`scene_usages`] 做启动断言(如同一 scene 被声明为不同 value 类型即为 bug)。

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

/// 返回本进程编译期收集到的全部缓存场景使用 descriptor。
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
    /// 输出稳定、无业务数据的冲突摘要。
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

/// 审计编译期收集的全部 `#[cached]` scene descriptor:同一 scene 名下所有声明必须有**一致的
/// 值类型(`TypeId`)与 TTL 合同(`refresh_ms`/`expire_ms`)**。
///
/// 否则运行期 L1(按 scene 分池、类型擦除存 `Arc<dyn Any>`)会因值类型不一致 downcast panic,或因 TTL
/// 分歧产生难查的软刷新/过期行为。本审计把该 bug **前移到启动期**一次性拒绝(结构化运行时错误仍是最后
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
