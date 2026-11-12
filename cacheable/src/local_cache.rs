//! 按 scene 隔离的本地缓存，提供过期加载、同步刷新和有界后台刷新。
//! 同一 scene 必须使用一致的 K/V 类型；复合键使用 BTreeMap，缺失值以 Option 哨兵缓存。
//! 超过刷新阈值后可返回旧值并排队刷新，超过硬过期时间则等待回源；这些策略不保证数据库强一致。
#![allow(non_snake_case)]

use dashmap::DashMap;
use moka::future::Cache;
use std::any::Any;
use std::collections::BTreeMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// 可嵌入基础设施组件的有界 L1 缓存。
///
/// 与下面面向注解宏的全局 scene registry 不同，本类型由调用方显式持有，容量和生命周期
/// 随调用方快照走；适合网关 route cache 这类热更后应整体退场的状态。值以 `Arc` 保存，
/// 命中只增加引用计数，不复制响应 payload。
pub struct BoundedLocalCache<K, V> {
    /// 按最大条目容量执行准入与淘汰的 moka 缓存；条目数观测可能滞后于内部维护。
    cache: Cache<K, Arc<V>>,
}

impl<K, V> BoundedLocalCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 业务作用：建立一个由调用方拥有的有界缓存。
    ///
    /// 参数说明：
    /// - `max_entries`：最大条目容量；必须大于零，由 moka 执行准入和淘汰策略。
    ///
    /// 返回：空缓存句柄；容量为零时 panic，不建立缓存。
    pub fn new(max_entries: u64) -> Self {
        assert!(
            max_entries > 0,
            "bounded local cache capacity must be positive"
        );
        Self {
            cache: Cache::builder().max_capacity(max_entries).build(),
        }
    }

    /// 业务作用：读取一个值；命中返回共享 `Arc`，不深复制 value。
    ///
    /// 参数说明：
    /// - `key`：调用方构造的缓存键，用于定位同一份共享值。
    ///
    /// 返回：命中时返回共享值；键不存在或已失效时返回 None，不执行回源。
    pub async fn get(&self, key: &K) -> Option<Arc<V>> {
        self.cache.get(key).await
    }

    /// 业务作用：覆盖写入一个值并返回同一份共享 `Arc`。
    ///
    /// 参数说明：
    /// - `key`：调用方构造的稳定缓存键；已有同键值时覆盖。
    /// - `value`：移入缓存的值，无需先复制其内容。
    ///
    /// 返回：与缓存写入使用同一份分配的 Arc；后续淘汰不影响已返回的共享值。
    pub async fn insert(&self, key: K, value: V) -> Arc<V> {
        let value = Arc::new(value);
        self.cache.insert(key, value.clone()).await;
        value
    }

    /// 业务作用：撤销指定键的缓存命中，供调用方在业务数据变更后主动失效。
    ///
    /// 参数说明：
    /// - `key`：需要撤销的精确缓存键。
    ///
    /// 返回：该键当前条目失效；键不存在时无操作，已经借出的 Arc 仍然可用。
    pub async fn invalidate(&self, key: &K) {
        self.cache.invalidate(key).await;
    }

    /// 业务作用：标记全部 entry 失效；用于 route 快照或管理面整组 PURGE。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前缓存条目全部失效；不撤销已借出的 Arc，也不阻止后续写入。
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// 业务作用：返回近似 entry 数，只用于低频观测，不作为并发正确性判断。
    ///
    /// 参数说明：无。
    ///
    /// 返回：moka 当前维护的条目数估计；维护任务尚未同步时可能滞后于写入或淘汰。
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

// 默认最大容量限制长期驻留；到达上限后由 TinyLFU 淘汰。
const DEFAULT_CAPACITY: u64 = 100_000;
/// Map 场景的复合缓存键类型,用于把一组有序字段作为本地缓存 key。
///
/// 键和值都用字符串,并通过 `BTreeMap` 的稳定排序保证同一组字段构造出的 key 可比较、可哈希。
pub type MapKey = BTreeMap<String, String>;

/// 本地缓存槽,保存业务值或空哨兵以及写入时间。
///
/// `None` 表示 loader 返回空值后的短缓存哨兵,用于抗穿透；`loaded_at` 用于判断 refresh/expire 窗口。
/// 泛型 `V` 是被缓存的业务值类型,例如用户对象、配置 DTO 或聚合结果。
struct Slot<V> {
    // 缓存的值。Some(Arc<V>)=真实值;None="空哨兵"(loader 返回 None 时缓存,防穿透,get 也返回 None)。
    // 命中返回共享值，避免复制大对象。
    value: Option<Arc<V>>,
    // 这条数据被写入缓存的时刻;后面用 loaded_at.elapsed() 判断是否到了 refresh / expire 窗口。
    loaded_at: Instant,
}
// 手动实现 Clone:只 clone Arc 指针 + copy Instant,不要求 V: Clone
// (若用 #[derive(Clone)],编译器会要求 V: Clone;手写则只需 Arc 可 clone,V 本身不必 clone)
impl<V> Clone for Slot<V> {
    /// 业务作用：为缓存读取复制槽位快照，共享业务值并保留原始刷新计时起点。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享原值和写入时刻的新槽位；不要求业务值实现 Clone，也不延长刷新窗口。
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            loaded_at: self.loaded_at,
        }
    }
}

/// 一个缓存 scene 的运行态。
///
/// 每个 scene 对应一组业务缓存命名空间,内部包含 moka 缓存、同 key 刷新单飞标记和刷新阈值。
/// 泛型参数 `K` 是缓存键类型,`V` 是业务值类型；两者必须能跨 tokio 多线程任务安全共享。
struct SceneState<K, V>
where
    // Hash + Eq:作为哈希表(moka/DashMap)的 key 必须能算哈希、能判等。
    // Clone:很多地方要复制 key(如插入 refreshing 表、spawn 进后台任务),故需可克隆。
    // Send + Sync:要在多线程/异步任务间传递与共享(tokio 多线程运行时),故需线程安全。
    // 'static:不能借用栈上短命引用,需能在缓存中长期存活(整个进程生命周期)。
    K: Hash + Eq + Clone + Send + Sync + 'static,
    // V 同理需 Send + Sync + 'static,以便跨线程共享、长期驻留缓存。
    V: Send + Sync + 'static,
{
    // 本 scene 的真正存储:moka 缓存,键 K、值 Slot<V>(带写入时刻)。
    cache: Cache<K, Slot<V>>,
    // 刷新单飞:refresh 窗口内同 key 只让 1 个线程真正重载,其余返回旧值。
    // 用 DashMap<K, ()> 当"集合用"(value 是空元组 ()),insert 的返回值用来判断是不是第一个抢到的人。
    refreshing: DashMap<K, ()>,
    // 刷新阈值(毫秒):写入后超过这个时长就进入"刷新窗口"。建池时绑定。
    refresh_ms: u64,
}

/// 刷新权 RAII 守卫:插入 `refreshing` 抢到刷新权即持有,Drop 时移除对应 key。
/// 关键:保证 `loader` **panic** 时也能释放刷新权——否则该 key 的刷新单飞标记永久泄漏,
/// 之后永不再进入刷新窗口(一直返回旧值,直到 expire 硬重载)。
/// 同步分支作用域结束即释放;异步分支把守卫 move 进后台任务,任务完成或 panic 都释放。
struct RefreshPermit<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 取得刷新权时所属的场景，关闭后仍只访问这一份旧状态。
    st: Arc<SceneState<K, V>>,
    /// 本守卫持有刷新权的键，析构时释放对应占用标记。
    key: K,
}

impl<K, V> Drop for RefreshPermit<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 业务作用：释放当前 key 的刷新权。
    ///
    /// 参数说明：无。
    ///
    /// 返回：移除刷新占用标记；加载结束、取消或 panic 展开后允许后续调用重新争取刷新权。
    fn drop(&mut self) {
        self.st.refreshing.remove(&self.key);
    }
}

// 全局 scene 注册表由运行时 owner 撤销。
// 值类型擦除成 Arc<dyn Any>,按 scene 取出后 downcast 回 Arc<SceneState<K,V>>。
// 约定:同一个 scene 必须用一致的 K/V,否则 downcast 失败会 panic。
// OnceLock:全局静态变量,首次访问时才初始化一次(线程安全),之后复用;键=scene 名,值=该 scene 的状态(已类型擦除)。
// dyn Any + Send + Sync:把不同 K/V 的 SceneState<K,V> 抹平成同一种"任意类型"存进同一张表,且保证可跨线程共享。
static REGISTRY: OnceLock<DashMap<String, Arc<dyn Any + Send + Sync>>> = OnceLock::new();
/// 业务作用：提供按场景名共享缓存状态的进程级注册表，统一各泛型入口的场景归属。
///
/// 参数说明：无。
///
/// 返回：首次调用时创建、后续复用的注册表；取得表本身不创建缓存场景。
fn registry() -> &'static DashMap<String, Arc<dyn Any + Send + Sync>> {
    REGISTRY.get_or_init(DashMap::new)
}

/// 跨节点失效用的类型擦除失效器。
///
/// Redis pub/sub 广播端只拿到 `(scene, key)` 两个字符串,没有泛型 `K/V`,不能直接 downcast 到
/// `SceneState<K,V>` 调 invalidate。建池时为每个 scene 登记一个非泛型失效器,由它捕获具体 moka
/// 缓存并向外暴露字符串 key 失效入口。
trait SceneOps: Send + Sync {
    /// 业务作用：把跨节点广播中的字符串键交给场景失效器，撤销对应的本地命中。
    ///
    /// 参数说明：
    /// - `key`：广播携带的完整缓存键；只适用于 String 键场景。
    ///
    /// 返回：发起本地失效；非字符串键场景不处理，不提供异步逐键失效的完成确认。
    fn invalidate_key(&self, key: &str);
    /// 业务作用：在运行态关闭或逐键失效容量不足时清除整个场景，避免遗留已知旧值。
    ///
    /// 参数说明：无。
    ///
    /// 返回：同步推进场景失效边界。
    fn invalidate_all(&self);
}

impl<K, V> SceneOps for SceneState<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 业务作用：撤销整个场景中的缓存值，供运行态关闭和失效排队失败使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：所有当前条目标记失效，已经借出的值不受影响。
    fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// 业务作用：接收广播并撤销匹配的本地缓存键。
    ///
    /// 参数说明：
    /// - `key`：广播中的完整缓存键，用于匹配当前场景的 String 键条目。
    ///
    /// 返回：有本地任务容量时排队失效；容量不足时清空本场景，非字符串键不处理。
    fn invalidate_key(&self, key: &str) {
        // 订阅端给的是字符串 key。把 &str → String → 装箱成 dyn Any → 尝试 downcast 成 K:
        //   K==String 时成功(拿到 owned K),异步失效该 key;K 非 String(如 BTreeMap)→ downcast 失败 → no-op。
        let boxed: Box<dyn Any> = Box::new(key.to_string());
        if let Ok(k) = boxed.downcast::<K>() {
            let cache = self.cache.clone(); // moka Cache 可廉价 clone(内部就是 Arc)
            let k = *k;
            // 逐键失效交给受 owner 管理的任务；无准入时必须撤销更大范围，避免保留已知旧值。
            let fallback = cache.clone();
            if !crate::local_work::try_spawn(async move {
                cache.invalidate(&k).await;
            }) {
                // 拒绝排队时扩大本地失效范围，不能静默保留已经知道应失效的值。
                fallback.invalidate_all();
            }
        }
    }
}

// scene 名 → 类型擦除失效器 的全局表(和 REGISTRY 平行)。
// Arc<dyn SceneOps>:SceneOps 有 Send+Sync 超 trait,故该 Arc 自动 Send+Sync,可跨线程存取。
static SCENE_OPS: OnceLock<DashMap<String, Arc<dyn SceneOps>>> = OnceLock::new();
/// 业务作用：返回缓存场景操作表；用于按场景批量清理本地缓存。
///
/// 参数说明：无。
///
/// 返回：首次访问时创建的进程级失效器注册表；可按场景名调用失效操作而无需恢复键值类型。
fn scene_ops() -> &'static DashMap<String, Arc<dyn SceneOps>> {
    SCENE_OPS.get_or_init(DashMap::new)
}

/// 业务作用：通过类型擦除入口处理跨节点广播，发起指定场景的本地逐键失效。
///
/// 参数说明：
/// - `scene_name`：已注册的本地缓存场景名。
/// - `key`：场景内需要失效的完整字符串键。
///
/// 返回：向场景失效器提交操作；场景不存在或键类型不是 String 时无操作，容量不足时失效整个场景。
pub fn remove_any(scene_name: &str, key: &str) {
    if let Some(ops) = scene_ops().get(scene_name) {
        ops.invalidate_key(key);
    }
}

/// 业务作用：取得或首次建立场景缓存，并同时登记跨节点广播使用的本地失效入口。
///
/// 参数说明：
/// - `name`：进程内唯一场景名；同名场景必须始终使用相同的键和值类型。
/// - `refresh_ms`：首次建场景时绑定的软刷新阈值，单位毫秒；已有场景保留原值。
/// - `expire_ms`：首次建场景时绑定的硬过期时长，单位毫秒；已有场景不会重新配置。
///
/// 返回：共享的类型化场景状态；同名场景使用不同键值类型时 panic，禁止把不同数据模型混入同一缓存。
fn scene<K, V>(name: &str, refresh_ms: u64, expire_ms: u64) -> Arc<SceneState<K, V>>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let reg = registry();
    // 取出擦除后的 Arc<dyn Any>:分两条路,避免"每次访问都写锁"。
    // ★ DashMap 的 entry() 永远拿【写锁】(它可能 insert),所以若每次都用 entry,即便 scene 早已存在
    //   的读多热路径也会把同分片访问串行化。这里改成「读锁快路径 + miss 才升级写锁」:
    let raw: Arc<dyn Any + Send + Sync> = match reg.get(name) {
        // 命中(warmup 后的常态):get() 只拿【读锁】,并发读互不阻塞,clone 出 Arc 即放锁
        Some(e) => e.value().clone(),
        // 缺失(罕见,仅首次):entry().or_insert_with 升级【写锁】建池。
        //   它是锁内原子 check-and-insert——即便此刻多个线程同时 miss,也只有一个真正建,其余复用。
        //   (get() 放锁到 entry() 拿锁之间若被别人抢先建好,or_insert_with 会直接返回已有的、不重复建。)
        None => reg
            .entry(name.to_string())
            .or_insert_with(|| {
                // Cache::builder():moka 的建造者模式,逐步配置后 build() 生成缓存实例。
                let cache = Cache::builder()
                    // 最大容量上限:到顶后按 TinyLFU(命中率)淘汰冷数据。
                    .max_capacity(DEFAULT_CAPACITY)
                    // 硬过期:expire_ms 到点 moka 自动淘汰
                    .time_to_live(Duration::from_millis(expire_ms))
                    // 完成配置,生成真正的 Cache 实例。
                    .build();
                // 先建成【强类型】Arc<SceneState<K,V>>,再分出两个视图(同一份状态,各自 +1 引用计数):
                let st: Arc<SceneState<K, V>> = Arc::new(SceneState {
                    cache,
                    refreshing: DashMap::new(),
                    refresh_ms,
                });
                // 在场景发布前先登记失效入口，使已发布的场景可被跨节点广播找到。
                scene_ops().insert(name.to_string(), st.clone() as Arc<dyn SceneOps>);
                // ② 把强类型状态【类型擦除】成 Arc<dyn Any> 存进主 REGISTRY:供 getRefresh/getExpire 的强类型路径用
                st as Arc<dyn Any + Send + Sync>
            })
            .value()
            .clone(), // clone 一份 Arc(引用计数 +1),随后 RefMut 在本分支结束时 drop、写锁释放
    };
    // 锁已释放(raw 是自有 Arc),在锁外把擦除后的 Any 还原回具体的 Arc<SceneState<K,V>>。
    // downcast 失败 = 同名 scene 被用了不同 K/V → panic(开发期约束,等价契约违反)。
    raw.downcast::<SceneState<K, V>>()
        .unwrap_or_else(|_| panic!("LocalCache: scene '{name}' 用了不一致的 K/V 类型"))
}

/// 业务作用：根据刷新阈值推导默认硬过期时间。
///
/// 参数说明：
/// - `refresh_ms`：软刷新阈值，单位毫秒；必须能在 u64 范围内加上 1000。
///
/// 返回：阈值不足 1000 毫秒时返回其两倍，否则增加 1000 毫秒，为刷新保留旧值可用窗口。
pub fn defaultExpireMs(refresh_ms: u64) -> u64 {
    if refresh_ms < 1000 {
        refresh_ms * 2
    } else {
        refresh_ms + 1000
    }
}

// ════════════════════════════════════════════════════════════════════════════
// expire 模式:过期后阻塞加载(同 key 单飞)
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：读取过期式缓存，在未命中时合并同键并发加载，避免同时回源造成压力。
///
/// 参数说明：
/// - `scene_name`：缓存场景名；同名场景必须始终使用相同的键和值类型。
/// - `key`：需要读取或加载的缓存键。
/// - `expireMs`：首次创建场景时绑定的过期时长，单位毫秒；同时作为刷新阈值，已有场景不变更。
/// - `loader`：未命中时执行的异步回源闭包；成功的 None 也会作为空哨兵缓存。
///
/// 返回：命中或加载成功时返回共享值或 None；加载失败返回错误且不缓存，同名场景类型冲突时 panic。
pub async fn getExpire<K, V, F, Fut>(
    scene_name: &str,
    key: K,
    expireMs: u64,
    loader: F,
) -> anyhow::Result<Option<Arc<V>>>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<Option<V>>> + Send + 'static,
{
    // 取/建该 scene;纯过期模式,refresh 与 expire 都传 expireMs。
    let st = scene::<K, V>(scene_name, expireMs, expireMs);
    // try_get_with:单飞加载;init 返回 Err 时【不缓存】并把错误传给所有等待者
    // (即同 key 并发只跑一次 loader,其余 await 同一结果——这正是"防击穿")。
    let slot = st
        .cache
        .try_get_with(key, async move {
            let v = loader().await?; // 执行 loader;若 Err 用 ? 提前返回,故失败的值不会被写入缓存
                                     // 成功:把 Option<V> 用 v.map(Arc::new) 包成 Option<Arc<V>>(Some 才 new Arc;None 保持 None=空哨兵),
                                     // 连同当前时刻一起封装成 Slot 写入缓存。Ok::<Slot<V>, _> 显式标注成功类型帮助类型推导。
            Ok::<Slot<V>, anyhow::Error>(Slot {
                value: v.map(Arc::new),
                loaded_at: Instant::now(),
            })
        })
        .await
        // try_get_with 失败返回的是 Arc<Error>,这里转成 anyhow 错误并附上下文,? 上抛。
        .map_err(|e| anyhow::anyhow!("local-cache load failed: {e}"))?;
    // 只把槽里的值(Option<Arc<V>>)返给调用方,内部的 loaded_at 不外露。
    Ok(slot.value)
}

// ════════════════════════════════════════════════════════════════════════════
// refresh 模式:不存在阻塞加载;refresh 窗口内 sync 同步重载/async 后台重载,返旧值
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：在硬过期前允许使用旧值并尝试刷新，未命中时合并同键回源请求。
///
/// 参数说明：
/// - `scene_name`：缓存场景名；同名场景必须始终使用相同的键和值类型。
/// - `key`：需要读取或刷新的缓存键。
/// - `sync`：为 true 时，取得刷新权的调用等待加载；为 false 时尝试后台刷新并返回旧值。
/// - `refreshMs`：首次建场景时绑定的软刷新阈值，单位毫秒；小于硬过期时长才存在刷新窗口。
/// - `expireMs`：首次建场景时绑定的硬过期时长，单位毫秒；已有场景保持原配置。
/// - `loader`：未命中或获得刷新权时使用的回源闭包，成功的 None 会缓存为空哨兵。
///
/// 返回：共享值或 None；刷新失败、未获刷新权或后台容量不足时保留旧值，未命中且加载失败时返回错误。
/// 同名场景类型冲突时 panic；后台刷新需要开放的本地任务 owner，不保证旧值与业务存储强一致。
pub async fn getRefresh<K, V, F, Fut>(
    scene_name: &str,
    key: K,
    sync: bool,
    refreshMs: u64,
    expireMs: u64,
    loader: F,
) -> anyhow::Result<Option<Arc<V>>>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<Option<V>>> + Send + 'static,
{
    // 取/建该 scene;刷新模式 refresh/expire 分别用 refreshMs/expireMs。
    let st = scene::<K, V>(scene_name, refreshMs, expireMs);

    // 先纯查一次(不触发加载):命中走 Some 分支,未命中/已被 TTL 淘汰走 None 分支。
    match st.cache.get(&key).await {
        // 命中(且未过 expire,否则 moka 已淘汰)
        Some(slot) => {
            // 还没到刷新时间 → 直接返回(新鲜)
            // elapsed() = 距 loaded_at 已过去多久;小于 refresh_ms 说明仍在"新鲜期"。
            if slot.loaded_at.elapsed() < Duration::from_millis(st.refresh_ms) {
                return Ok(slot.value);
            }
            // 进入刷新窗口:抢"刷新权"(同 key 只一个能抢到)
            // DashMap::insert 返回旧值:Some(())=之前已有人插过(别人在刷)→ is_some() 为真;None=我是第一个。
            if st.refreshing.insert(key.clone(), ()).is_some() {
                // 别人正在刷 → 返回旧值
                return Ok(slot.value);
            }
            // 刷新权 RAII:抢到后即持有,Drop 保证 loader **panic** 时也释放(否则该 key
            // 刷新单飞标记永久泄漏,之后再不进入刷新)。同步分支作用域结束释放,异步分支 move 进后台任务。
            let permit = RefreshPermit {
                st: st.clone(),
                key: key.clone(),
            };
            if sync {
                // 同步重载:本线程加载;成功写回返新值,失败保留旧值
                let result = match loader().await {
                    Ok(v) => {
                        // 组装新槽(刷新写入时刻),并先 clone 出要返回的值。
                        let new = Slot {
                            value: v.map(Arc::new),
                            loaded_at: Instant::now(),
                        };
                        let ret = new.value.clone();
                        st.cache.insert(key.clone(), new).await; // 写回缓存(覆盖旧值,重置 refresh/expire 计时)
                        Ok(ret) // 返回刚加载的新值
                    }
                    Err(e) => {
                        // 刷新失败:打 warn,不动缓存(保留旧值),返回旧值。
                        tracing::warn!("local-cache sync refresh failed, keep stale: {e}");
                        Ok(slot.value)
                    }
                };
                drop(permit); // 释放刷新权(panic 路径由 Drop 兜底)
                result
            } else {
                // 异步重载:立刻返回旧值,后台任务去加载并写回(失败保留旧值)
                let stale = slot.value.clone();
                let st2 = st.clone();
                let key2 = key.clone();
                // owner 关闭或容量不足时释放任务及刷新权，旧值仍受硬过期限制，后续调用可再次尝试。
                crate::local_work::try_spawn(async move {
                    // permit move 进任务:任务正常结束或 panic 都释放刷新权(Drop 兜底)。
                    let _permit = permit;
                    match loader().await {
                        Ok(v) => {
                            // 加载成功 → 写回缓存(新值 + 新写入时刻)。
                            st2.cache
                                .insert(
                                    key2.clone(),
                                    Slot {
                                        value: v.map(Arc::new),
                                        loaded_at: Instant::now(),
                                    },
                                )
                                .await;
                        }
                        // 失败 → 仅告警,保留旧值。
                        Err(e) => {
                            tracing::warn!("local-cache async refresh failed, keep stale: {e}")
                        }
                    }
                });
                Ok(stale) // 主调用立刻返回旧值
            }
        }
        // 缺失或已过 expire → 阻塞加载(单飞;失败不缓存、向上抛)
        // (与 getExpire 的 try_get_with 同款:同 key 并发只加载一次,其余等同一结果。)
        None => {
            let slot = st
                .cache
                .try_get_with(key, async move {
                    let v = loader().await?; // 失败用 ? 上抛,不缓存
                    Ok::<Slot<V>, anyhow::Error>(Slot {
                        value: v.map(Arc::new),
                        loaded_at: Instant::now(),
                    })
                })
                .await
                .map_err(|e| anyhow::anyhow!("local-cache load failed: {e}"))?;
            Ok(slot.value)
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 手动操作:put / refresh / remove / removeAll
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：按场景和过期时间主动写入缓存，返回共享的 `Arc<V>`。
///
/// 参数说明：
/// - `scene_name`：缓存场景名；不存在时创建，同名场景必须保持键值类型一致。
/// - `key`：待写入的缓存键，已有同键条目时覆盖。
/// - `value`：移入缓存的真实业务值，不写入空哨兵。
/// - `expireMs`：首次建场景时绑定的刷新和过期时长，单位毫秒；已有场景保留原配置。
///
/// 返回：与缓存共享同一份业务值的 Arc；写入重置该条目计时，同名场景类型冲突时 panic。
pub async fn expire<K, V>(scene_name: &str, key: K, value: V, expireMs: u64) -> Arc<V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let st = scene::<K, V>(scene_name, expireMs, expireMs);
    let arc = Arc::new(value); // 把值放进 Arc,便于"返回一份 + 缓存一份"共享同一内存
    st.cache
        // Some(arc.clone()):clone 只是指针 +1;手动 put 一定是真实值,故用 Some 而非空哨兵。
        .insert(
            key,
            Slot {
                value: Some(arc.clone()),
                loaded_at: Instant::now(),
            },
        )
        .await;
    arc // 返回给调用方
}

/// 业务作用：使用本次调用提供的回源闭包，手动刷新已有场景中的指定键。
///
/// 参数说明：
/// - `scene_name`：已经存在的缓存场景名；此入口不创建场景。
/// - `key`：加载成功后覆盖写入的缓存键。
/// - `loader`：在当前任务中等待的回源闭包，不沿用其他调用的上下文，也不参与刷新单飞。
///
/// 返回：加载成功后写入业务值或空哨兵并重置计时；场景缺失或类型不符时无操作，加载失败只告警并保留旧值。
pub async fn refresh<K, V, F, Fut>(scene_name: &str, key: K, loader: F)
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<V>>>,
{
    // 仅当 scene 已存在时才操作(get 返回 Some);不存在直接略过。
    if let Some(e) = registry().get(scene_name) {
        // 把擦除的 Any 还原回具体 SceneState<K,V>;类型不符(Err)则静默跳过(不 panic)。
        if let Ok(st) = e.value().clone().downcast::<SceneState<K, V>>() {
            match loader().await {
                Ok(v) => {
                    // 加载成功 → 覆盖写入(重置计时)。
                    st.cache
                        .insert(
                            key,
                            Slot {
                                value: v.map(Arc::new),
                                loaded_at: Instant::now(),
                            },
                        )
                        .await;
                }
                // 失败 → 仅告警,保留原有缓存(不改动)。
                Err(e) => tracing::warn!("local-cache manual refresh failed: {e}"),
            }
        }
    }
}

/// 业务作用：在已有的类型化场景中撤销单个缓存键，供业务数据变更后主动失效。
///
/// 参数说明：
/// - `scene_name`：待操作的本地缓存场景名，不存在时不创建。
/// - `key`：需要失效的精确缓存键。
///
/// 返回：场景存在且键值类型匹配时完成该键失效；场景缺失或类型不符时无操作，已借出的值不受影响。
pub async fn remove<K, V>(scene_name: &str, key: K)
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    if let Some(e) = registry().get(scene_name) {
        if let Ok(st) = e.value().clone().downcast::<SceneState<K, V>>() {
            st.cache.invalidate(&key).await; // moka 失效单个 key(立即移除)
        }
    }
}

/// 业务作用：撤销已有场景的全部缓存条目，保留场景注册和原有过期配置。
///
/// 参数说明：
/// - `scene_name`：需要整体失效的本地缓存场景名。
///
/// 返回：场景存在且键值类型匹配时同步标记当前条目失效；否则无操作，不撤销已借出的值或阻止后续写入。
pub fn removeAll<K, V>(scene_name: &str)
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    if let Some(e) = registry().get(scene_name) {
        if let Ok(st) = e.value().clone().downcast::<SceneState<K, V>>() {
            st.cache.invalidate_all(); // moka 失效该 scene 内全部条目(清空本池)
        }
    }
}

// 复合键使用 BTreeMap<String, String>，相同键值集合共享场景条目。
// 作用:把 K 固定为 MapKey(BTreeMap<String,String>),调用方无需再写泛型 K,直接传一个 Map 当复合键。
// 参数同 getExpire,只是 key 的类型从泛型 K 收窄成 MapKey;泛型只剩 V/F/Fut。
/// 业务作用：MapKey 版本的过期式读取;用于复合键场景,其余行为与 [`getExpire`] 一致。
///
/// 参数说明：
/// - `scene_name`：缓存场景名，同名场景必须保持 MapKey 键和业务值类型一致。
/// - `key`：由 BTreeMap 排序的复合键，相同字段集合定位同一条目。
/// - `expireMs`：首次建场景时绑定的过期时长，单位毫秒；已有场景不变更。
/// - `loader`：未命中时执行的回源闭包，成功的 None 也会被缓存。
///
/// 返回：共享业务值或 None；回源失败不缓存并返回错误，同名场景类型冲突时 panic。
pub async fn getExpireMap<V, F, Fut>(
    scene_name: &str,
    key: MapKey,
    expireMs: u64,
    loader: F,
) -> anyhow::Result<Option<Arc<V>>>
where
    V: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<Option<V>>> + Send + 'static,
{
    // 直接转调泛型版,把 K 显式定为 MapKey。
    getExpire::<MapKey, V, F, Fut>(scene_name, key, expireMs, loader).await
}

/// 业务作用：MapKey 版本的刷新式读取;用于复合键场景,其余行为与 [`getRefresh`] 一致。
///
/// 参数说明：
/// - `scene_name`：缓存场景名，同名场景必须保持 MapKey 键和业务值类型一致。
/// - `key`：由 BTreeMap 排序的复合键，相同字段集合定位同一条目。
/// - `sync`：获得刷新权后是否在当前调用内等待加载；为 false 时尝试有界后台刷新。
/// - `refreshMs`：首次建场景时绑定的软刷新阈值，单位毫秒。
/// - `expireMs`：首次建场景时绑定的硬过期时长，单位毫秒；已有场景保持原配置。
/// - `loader`：未命中或取得刷新权时执行的回源闭包，成功的 None 会缓存为空哨兵。
///
/// 返回：共享值或 None；刷新受阻或失败时仍可返回旧值，未命中且加载失败时返回错误，场景类型冲突时 panic。
pub async fn getRefreshMap<V, F, Fut>(
    scene_name: &str,
    key: MapKey,
    sync: bool,
    refreshMs: u64,
    expireMs: u64,
    loader: F,
) -> anyhow::Result<Option<Arc<V>>>
where
    V: Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<Option<V>>> + Send + 'static,
{
    // 转调泛型版,K=MapKey。
    getRefresh::<MapKey, V, F, Fut>(scene_name, key, sync, refreshMs, expireMs, loader).await
}

/// 业务作用：撤销当前运行态所有本地场景，后续代次不会复用旧 loader 所持场景。
///
/// 参数说明：无。
///
/// 返回：当前缓存全部失效并移除注册；旧引用只能访问被撤下的场景。
pub(crate) fn clear_all() {
    for scene in scene_ops().iter() {
        scene.value().invalidate_all();
    }
    scene_ops().clear();
    registry().clear();
}
