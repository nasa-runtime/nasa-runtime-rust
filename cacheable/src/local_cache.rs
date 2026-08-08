// ============================================================================
// src/local_cache.rs —— 本地缓存工具(对照 原实现 com.nasa.common.cache.LocalCache)
//
// 底层用 moka(Rust 版 Caffeine):TinyLFU 淘汰 + TTL + 单飞加载。
// API 名与语义对齐 原实现 版(故方法名用 camelCase,加 #![allow(non_snake_case)]):
//   · getExpire  —— 过期后【阻塞】加载;同 key 并发只有 1 个去加载,其余等(防击穿)
//   · getRefresh —— 不存在时阻塞加载;到 refresh 时间后:
//                     sync=true  触发线程同步重载、其余返回旧值
//                     sync=false 所有线程返回旧值、后台线程异步重载
//                   超过 expire 时间则全部阻塞重载
//   · expire     —— 手动放入(put)
//   · refresh    —— 手动重载(原实现 用建池时的 loader;Rust 无法存异步闭包,故这里要传 loader)
//   · remove / removeAll —— 失效
// 按 scene(场景名)分池,与 原实现 一致。null 用哨兵缓存(防穿透)——这里用 Option<Arc<V>> 表达,
//   loader 返回 None 即"空值",会被缓存(短期内不再回源),get 返回 None。
//
// ⚠️ 与 原实现 的差异(Rust 限制所致,已尽量贴近):
//   1. Rust 无方法重载 → 原实现 的多个重载在此【合并成最全签名】(getExpire/getRefresh 各一个)。
//   2. 原实现 的 key 分 String 池 / Map 池;Rust 用泛型 K 统一,Map 场景传 BTreeMap<String,String>，
//      并提供 *Map 同名薄封装。
//   3. 返回 Arc<V>(共享引用,零拷贝,等价 原实现 返回对象引用),null/缺失为 None。
//   4. loader 是 async 闭包(配 tokio);refresh 需显式传 loader(原实现 用建池时绑定的)。
// ============================================================================
#![allow(non_snake_case)]
// 作为可复用工具提供,尚未接入业务,先整体 allow dead_code;真正用上后可移除本行。
#![allow(dead_code)]

// ── 依赖导入(对照 原实现 的 import)──
use std::any::Any; // 类型擦除/还原:Any 让我们能把不同 K/V 的 SceneState 统一塞进一张表,取出时再 downcast 还原
use std::collections::BTreeMap; // 有序 Map,用作 Map 池的复合键(对照 原实现 的 Map<String,Object> key);BTreeMap 实现了 Hash/Eq/Ord,可当 key
use std::future::Future; // loader 通过 Future 表达可等待的异步返回值。
use std::hash::Hash; // key 需要可哈希(放进 moka/DashMap 的哈希表里),约束 K: Hash
use std::sync::{Arc, OnceLock}; // Arc=原子引用计数共享指针(等价 原实现 对象引用,可跨线程共享);OnceLock=懒初始化一次的全局单例容器
use std::time::{Duration, Instant}; // Duration=时间长度(如 50ms);Instant=单调时钟时间点,用来记录"写入时刻"并算已过去多久

use dashmap::DashMap; // 分段锁保护的并发 HashMap，供多任务共享 scene 注册表。
use moka::future::Cache; // moka 异步缓存(Rust 版 Caffeine):提供 TinyLFU 淘汰 + TTL + 单飞加载

/// 可嵌入基础设施组件的有界 L1 缓存。
///
/// 与下面面向注解宏的全局 scene registry 不同，本类型由调用方显式持有，容量和生命周期
/// 随调用方快照走；适合网关 route cache 这类热更后应整体退场的状态。值以 `Arc` 保存，
/// 命中只增加引用计数，不复制响应 payload。
pub struct BoundedLocalCache<K, V> {
    /// moka TinyLFU 缓存；capacity 是硬的 entry 数上限。
    cache: Cache<K, Arc<V>>,
}

impl<K, V> BoundedLocalCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 业务作用：建立一个由调用方拥有的有界缓存。
    ///
    /// # 参数
    /// - `max_entries`: 最大 entry 数；必须大于 0，达到后由 moka TinyLFU 淘汰。
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
    /// # 参数
    /// - `key`: 待查键的借用。
    pub async fn get(&self, key: &K) -> Option<Arc<V>> {
        self.cache.get(key).await
    }

    /// 业务作用：覆盖写入一个值并返回同一份共享 `Arc`。
    ///
    /// # 参数
    /// - `key`: 调用方构造的稳定缓存键。
    /// - `value`: 待缓存值；函数接管所有权，不先 clone payload。
    pub async fn insert(&self, key: K, value: V) -> Arc<V> {
        let value = Arc::new(value);
        self.cache.insert(key, value.clone()).await;
        value
    }

    /// 业务作用：删除一个精确 key。
    ///
    /// # 参数
    /// - `key`: 待失效键的借用。
    pub async fn invalidate(&self, key: &K) {
        self.cache.invalidate(key).await;
    }

    /// 业务作用：标记全部 entry 失效；用于 route 快照或管理面整组 PURGE。
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// 业务作用：返回近似 entry 数，只用于低频观测，不作为并发正确性判断。
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

// 默认最大容量(TinyLFU 到顶后按命中率淘汰)。原实现 Caffeine 默认不限,这里给个安全上限。
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
    // 用 Arc 包裹:取值时只 clone 指针(零拷贝),等价 原实现 返回对象引用。
    value: Option<Arc<V>>,
    // 这条数据被写入缓存的时刻;后面用 loaded_at.elapsed() 判断是否到了 refresh / expire 窗口。
    loaded_at: Instant,
}
// 手动实现 Clone:只 clone Arc 指针 + copy Instant,不要求 V: Clone
// (若用 #[derive(Clone)],编译器会要求 V: Clone;手写则只需 Arc 可 clone,V 本身不必 clone)
impl<V> Clone for Slot<V> {
    /// 业务作用：复制当前句柄；用于共享同一底层资源或配置快照。
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
    // 刷新阈值(毫秒):写入后超过这个时长就进入"刷新窗口"。建池时绑定(对照 原实现 建池参数)。
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
    st: Arc<SceneState<K, V>>,
    key: K,
}

impl<K, V> Drop for RefreshPermit<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 业务作用：释放当前 key 的刷新权。
    ///
    /// loader 正常完成、返回错误或 panic 展开时都会执行，避免 `refreshing` 表里永久残留该 key。
    fn drop(&mut self) {
        self.st.refreshing.remove(&self.key);
    }
}

// ── 全局 scene 注册表(对应 原实现 的 STRING_CACHE/MAP_CACHE)──
// 值类型擦除成 Arc<dyn Any>,按 scene 取出后 downcast 回 Arc<SceneState<K,V>>。
// 约定:同一个 scene 必须用一致的 K/V(原实现 也是如此),否则 downcast 失败会 panic。
// OnceLock:全局静态变量,首次访问时才初始化一次(线程安全),之后复用;键=scene 名,值=该 scene 的状态(已类型擦除)。
// dyn Any + Send + Sync:把不同 K/V 的 SceneState<K,V> 抹平成同一种"任意类型"存进同一张表,且保证可跨线程共享。
static REGISTRY: OnceLock<DashMap<String, Arc<dyn Any + Send + Sync>>> = OnceLock::new();
/// 业务作用：取得全局 scene 注册表。
///
/// `get_or_init` 保证全局只创建一次 DashMap(懒加载单例),返回 `'static` 引用供所有缓存池复用。
fn registry() -> &'static DashMap<String, Arc<dyn Any + Send + Sync>> {
    REGISTRY.get_or_init(DashMap::new)
}

/// 跨节点失效用的类型擦除失效器。
///
/// Redis pub/sub 广播端只拿到 `(scene, key)` 两个字符串,没有泛型 `K/V`,不能直接 downcast 到
/// `SceneState<K,V>` 调 invalidate。建池时为每个 scene 登记一个非泛型失效器,由它捕获具体 moka
/// 缓存并向外暴露字符串 key 失效入口。
trait SceneOps: Send + Sync {
    /// 业务作用：失效本 scene 里该 key。仅对 String-key 的 scene 生效(cacheable 都用 String key);
    /// 非 String 的 scene(如 Map 池)做 no-op(下面用 Any downcast 判断 K 是不是 String)。
    ///
    /// # 参数
    /// - `key`: 广播消息里携带的字符串缓存 key。
    fn invalidate_key(&self, key: &str);
}

impl<K, V> SceneOps for SceneState<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// 业务作用：使指定本地缓存键失效；用于数据变更后清除旧值。
    ///
    /// # 参数
    /// - `key`: 要从当前 scene 中删除的字符串缓存 key。
    fn invalidate_key(&self, key: &str) {
        // 订阅端给的是字符串 key。把 &str → String → 装箱成 dyn Any → 尝试 downcast 成 K:
        //   K==String 时成功(拿到 owned K),异步失效该 key;K 非 String(如 BTreeMap)→ downcast 失败 → no-op。
        let boxed: Box<dyn Any> = Box::new(key.to_string());
        if let Ok(k) = boxed.downcast::<K>() {
            let cache = self.cache.clone(); // moka Cache 可廉价 clone(内部就是 Arc)
            let k = *k;
            // moka invalidate 是 async;这里 fire-and-forget spawn 去删(最终一致即可,无需等)
            tokio::spawn(async move {
                cache.invalidate(&k).await;
            });
        }
    }
}

// scene 名 → 类型擦除失效器 的全局表(和 REGISTRY 平行)。
// Arc<dyn SceneOps>:SceneOps 有 Send+Sync 超 trait,故该 Arc 自动 Send+Sync,可跨线程存取。
static SCENE_OPS: OnceLock<DashMap<String, Arc<dyn SceneOps>>> = OnceLock::new();
/// 业务作用：返回缓存场景操作表；用于按场景批量清理本地缓存。
fn scene_ops() -> &'static DashMap<String, Arc<dyn SceneOps>> {
    SCENE_OPS.get_or_init(DashMap::new)
}

/// 业务作用：【非泛型】按 scene+key 失效本节点 L1。供跨节点广播订阅端调用(它只有字符串,没有泛型类型)。
/// 内部走该 scene 登记的 SceneOps;仅 String-key 的 scene 真正生效,其余 no-op。找不到 scene 也安全跳过。
///
/// # 参数
/// - `scene_name`: L1 本地缓存池名。
/// - `key`: 要失效的字符串缓存 key。
pub fn remove_any(scene_name: &str, key: &str) {
    if let Some(ops) = scene_ops().get(scene_name) {
        ops.invalidate_key(key);
    }
}

// 取/建某 scene 的状态(computeIfAbsent 语义;建池时绑定 expire/refresh 阈值)
// 参数:
//   name       —— 场景名(池名),用于在注册表里定位/创建该 scene。
//   refresh_ms —— 刷新阈值(毫秒),存进 SceneState.refresh_ms,getRefresh 用它判断刷新窗口。
//   expire_ms  —— 硬过期时长(毫秒),用作 moka 的 time_to_live(TTL),到点自动淘汰。
// 泛型 K/V:见 SceneState 的约束说明,需在编译期固定该 scene 的键/值类型。
///
/// # 参数
/// 业务作用：- `name`: 业务名称、字段名或配置名,用于定位目标对象。
/// - `refresh_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
/// - `expire_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
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
                    // 硬过期:expire_ms 到点 moka 自动淘汰(对应 原实现 expireAfterWrite)
                    .time_to_live(Duration::from_millis(expire_ms))
                    // 完成配置,生成真正的 Cache 实例。
                    .build();
                // 先建成【强类型】Arc<SceneState<K,V>>,再分出两个视图(同一份状态,各自 +1 引用计数):
                let st: Arc<SceneState<K, V>> = Arc::new(SceneState {
                    cache,
                    refreshing: DashMap::new(),
                    refresh_ms,
                });
                // ① 登记【类型擦除失效器】到 SCENE_OPS:供 remove_any / 跨节点广播按 scene+key 失效(见 SceneOps)
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
/// 规则:刷新很快(<1s)时 expire 取 2 倍给后台刷新留缓冲;否则只比刷新多 1s。
///
/// # 参数
/// - `refresh_ms`: 刷新阈值毫秒数。
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
/// 业务作用：缓存不存在或已过期时,同 key 只有 1 个任务去 loader 加载,其余 await 同一结果(防击穿)。
/// loader 返回:
///   Ok(Some(v)) → 缓存值;Ok(None) → 缓存空哨兵(防穿透),get 返回 None;
///   Err(e)      → 【不缓存】,错误向上抛(对照 Caffeine:loader 抛异常不缓存)。
///
/// 参数:
///   scene_name —— 场景名(池名);同名 scene 必须始终用一致的 K/V。
///   key        —— 要查询的缓存键(按值传入,函数内会移交给 moka)。
///   expireMs   —— 过期时长(毫秒);本模式 refresh 与 expire 取同值(纯过期式,无刷新窗口)。
///   loader     —— 回源加载闭包:缓存未命中时被调用,返回一个异步 Future。
/// 泛型:
///   K/V —— 键/值类型(约束含义同前)。
///   F   —— loader 闭包类型;FnOnce(只调一次即可)+ Send(可送进 moka 的加载任务/跨线程)+ 'static(不借用短命栈数据)。
///   Fut —— loader 返回的 Future 类型;Output 为 `anyhow::Result<Option<V>>`:
///           Ok(Some(v))=拿到真实值;Ok(None)=确认无此数据(缓存空哨兵);Err=加载失败(不缓存、上抛)。
///           Send + 'static:该 Future 要在 moka 内部跨线程驱动执行。
///
/// # 参数
/// - `scene_name`: 本地缓存池名;同名 scene 必须始终使用一致的键和值类型。
/// - `key`: 要查询或加载的缓存键。
/// - `expireMs`: 硬过期毫秒数,本模式下也作为 scene 的刷新阈值。
/// - `loader`: 缓存未命中或过期时执行的回源闭包。
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
/// 业务作用：refresh 模式取值。
/// 参数:
///   scene_name —— 场景名(池名)。
///   key        —— 缓存键。
///   sync       —— 进入刷新窗口后的重载策略:
///                  true  = 同步重载:抢到刷新权的【本调用】当场 await loader,成功则返回新值;其余并发调用返回旧值。
///                  false = 异步重载:抢到刷新权的调用也【立刻返回旧值】,真正的重载丢到后台 tokio 任务里跑。
///                  (无论 true/false,没抢到刷新权的并发调用都返回旧值,保证不阻塞。)
///   refreshMs  —— 刷新阈值(毫秒):写入后超过它即进入"刷新窗口",触发重载逻辑,但旧值仍可用。
///   expireMs   —— 硬过期(毫秒):写入后超过它,moka 直接淘汰该条,下次取走 None 分支阻塞重载。
///                  关系:refreshMs < expireMs;[0,refreshMs) 用旧值不刷,[refreshMs,expireMs) 旧值+触发刷新,>=expireMs 已淘汰。
///   loader     —— 回源加载闭包(语义同 getExpire)。
/// 泛型 K/V/F/Fut 约束含义同 getExpire。
///
/// # 参数
/// - `scene_name`: 本地缓存池名;同名 scene 必须始终使用一致的键和值类型。
/// - `key`: 要查询或刷新的缓存键。
/// - `sync`: 刷新窗口内是否由当前调用同步等待新值。
/// - `refreshMs`: 软刷新阈值毫秒数,超过后触发刷新但旧值仍可返回。
/// - `expireMs`: 硬过期毫秒数,超过后必须阻塞重载。
/// - `loader`: 缓存不存在、过期或刷新时执行的回源闭包。
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
                // 同步重载:本线程加载;成功写回返新值,失败保留旧值(对照 Caffeine 刷新失败保旧值)
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
                let stale = slot.value.clone(); // 先备份旧值(待会儿立即返回它)
                let st2 = st.clone(); // clone SceneState 的 Arc(指针,引用计数+1)给后台任务用
                let key2 = key.clone(); // 同样把 key 复制一份移进后台任务
                                        // tokio::spawn:把重载丢到后台异步任务,主调用不等它,直接返回旧值(不阻塞调用方)。
                tokio::spawn(async move {
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
/// 业务作用：手动放入(对照 原实现 expire(scene,key,value,expireMs))。返回放入的 `Arc<V>`。
/// 参数:
///   scene_name —— 场景名(池名);若该 scene 还不存在会顺带建池。
///   key        —— 要写入的键。
///   value      —— 要写入的值(按值传入,函数内 Arc::new 接管所有权)。
///   expireMs   —— 该写入的过期时长(毫秒);此处 refresh/expire 同值。
/// 泛型 K/V 约束同前。返回:包好的 `Arc<V>`(供调用方继续使用同一份共享值)。
///
/// # 参数
/// - `scene_name`: 本地缓存池名;不存在时会创建。
/// - `key`: 要写入的缓存键。
/// - `value`: 要放入缓存的真实值。
/// - `expireMs`: 该条缓存的硬过期毫秒数。
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

/// 业务作用：手动重载某 key(用传入的 loader 重新加载并写回)。
/// 注:原实现 版 refresh(scene,key) 用建池时绑定的 loader;Rust 无法存异步闭包,故需显式传 loader。
/// 参数:
///   scene_name —— 场景名;注意:与 get* 不同,这里【不建池】——scene 不存在就直接跳过(什么也不做)。
///   key        —— 要重载的键。
///   loader     —— 重新加载用的闭包,语义同 get*(Ok(Some)/Ok(None)/Err)。
/// 泛型:F/Fut 此处无 Send + 'static 约束(就在当前任务内 await,不跨线程 spawn,故无需)。
///
/// # 参数
/// - `scene_name`: 本地缓存池名;不存在时直接跳过。
/// - `key`: 要重新加载并覆盖写入的缓存键。
/// - `loader`: 手动重载时执行的回源闭包。
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

/// 业务作用：失效单个 key
///
/// # 参数
/// - `scene_name`: 本地缓存池名。
/// - `key`: 要失效删除的缓存键。
///
/// 行为:scene 存在且类型匹配才删;否则什么也不做。
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

/// 业务作用：失效整个 scene
///
/// # 参数
/// - `scene_name`: 要整体清空的本地缓存池名。
///
/// 注意:本函数非 async——invalidate_all 只是打个失效标记,不需 await。
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

// ── Map 池薄封装(对照 原实现 的 *Map 系列;key = BTreeMap<String,String>)──
// 作用:把 K 固定为 MapKey(BTreeMap<String,String>),调用方无需再写泛型 K,直接传一个 Map 当复合键。
// 参数同 getExpire,只是 key 的类型从泛型 K 收窄成 MapKey;泛型只剩 V/F/Fut。
/// 业务作用：MapKey 版本的过期式读取;用于复合键场景,其余行为与 [`getExpire`] 一致。
///
/// # 参数
/// - `scene_name`: 本地缓存池名。
/// - `key`: 复合缓存键,字段顺序由 `BTreeMap` 保证稳定。
/// - `expireMs`: 硬过期毫秒数。
/// - `loader`: 缓存未命中或过期时执行的回源闭包。
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
/// # 参数
/// - `scene_name`: 本地缓存池名。
/// - `key`: 复合缓存键,字段顺序由 `BTreeMap` 保证稳定。
/// - `sync`: 刷新窗口内是否由当前调用同步等待新值。
/// - `refreshMs`: 软刷新阈值毫秒数。
/// - `expireMs`: 硬过期毫秒数。
/// - `loader`: 缓存不存在、过期或刷新时执行的回源闭包。
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
