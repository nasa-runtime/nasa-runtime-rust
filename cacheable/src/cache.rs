// ============================================================================
// src/cache.rs —— 缓存层（两种互补的缓存实现，合于一文件）
//
// 本文件含两个可复用缓存组件，按场景二选一（或并用）：
//
//   ┌─ CacheLayer ──── 通用 cache-aside + 缓存三防（扁平 key + TTL 为主）
//   │   "查缓存 → 未命中 → single-flight → 回源 → 回填"模板。
//   │   三防：穿透(空哨兵短 TTL) / 击穿(进程内 key 级锁) / 雪崩(TTL 抖动)。
//   │   适合：以 TTL 自动过期保新鲜、不需要写后主动失效的只读热点查询。
//   │
//   └─ GroupedCache ── 分组缓存（对标 原实现 MybatisCache）+ 跨节点 single-flight
//       按【组】组织(一个 group = 一个 Redis Hash)，以【显式失效】为主、长兜底 TTL 兜底。
//       支持 invalidate_field/invalidate_group + 关联失效(@CacheClearRef)；single-flight 两级
//       去重(进程内 tokio::Mutex + Redis SET NX 分布式锁)。
//       适合：写库后要让缓存立刻失效、需要强一致 / 关联失效的场景（撮合/账务）。
//
// ⚠️ 与 原实现 端 MyBatis 二级缓存（MybatisCache）的差异：
//   - MyBatis 缓存【框架透明】：CachingExecutor 在执行 SQL 前后自动查/写/清缓存，DAO 无感。
//   - 本文件两者都是【显式调用】：service 主动 get_or_load / invalidate（sqlx 不是 ORM、无执行器钩子）。
//
// ⚠️ 必须在 Tokio 运行时内使用。
// ⚠️ GroupedCache 的 per-field 兜底 TTL 用 HPEXPIRE，需 Redis 7.4+。
// ============================================================================

// 作为可复用组件提供，尚未全部接入路由，故整体 allow dead_code。真正接入后可移除本行。
#![allow(dead_code)]

// ── import 逐行说明（Rust 的 use ≈ 原实现 的 import，:: ≈ .，但有两个关键差异见 ★）──
use std::collections::{HashMap, HashSet}; // GroupedCache 关联失效表用：HashMap<组, 关联组集合>；HashSet 去重
use std::future::Future; // Future trait：异步计算的"凭证/占位符"，约束 loader 返回值；.await 才真正驱动它跑
use std::sync::Arc; // 原子引用计数（Atomically Ref-Counted）：多请求/多线程共享同一份所有权。
                    //   clone 只让计数 +1、不深拷贝；计数归零才真正析构。这里用它共享同一把锁 / clear_map。
use std::sync::RwLock; // 读写锁：GroupedCache.clear_map 注册罕见、读多（每次失效都读），std RwLock 足够；
                       //   ★ 它是【同步】锁，绝不能跨 .await 持有 —— 本文件只在同步代码段内 read()/write()
use std::time::{Duration, Instant}; // Duration=时长（给 sleep/锁 TTL）；Instant=单调时钟（算等待截止点）

use dashmap::DashMap; // 并发安全的 HashMap（内部分段加锁）：存 single-flight 的 key->锁，≈ ConcurrentHashMap
use rand::Rng; // ★ trait：gen_range 等方法"挂"在这个 trait 上。Rust 里方法解析靠"trait 必须在作用域内"，
               //   不 use 进来，哪怕类型对，conn.gen_range(..) 也编译不过（原实现 没有这个概念）
use redis::aio::ConnectionManager; // 异步 Redis【单机】连接管理器：句柄可 clone、内部自带断线重连（GroupedCache 仍用它作单机演示）
use redis::cluster_async::ClusterConnection; // 异步 Redis 集群连接（redis 1.2.2,cluster_async 已优化）
use redis::AsyncCommands; // ★ 同理：.get()/.pset_ex()/.hget()/.hset()/.del()/.pexpire() 等命令方法由这个 trait 提供，
                          //   必须 use 进来才能在 conn 上点出来
use serde::de::DeserializeOwned; // 约束：能从 JSON 反序列化出【完全自有】的值（读缓存时用，详见 where 注释）
use serde::Serialize; // 约束：能序列化成 JSON（写缓存时用 serde_json::to_string）
use tokio::sync::Mutex; // 异步互斥锁：锁可以【跨 .await 持有】。注意不是 std::sync::Mutex——后者跨 await 持有会出问题
use tracing::{debug, warn}; // 结构化日志宏（类似 原实现 slf4j 的 log.debug / log.warn）

/// 释放分布式锁的 compare-and-del 脚本（Lua，在 Redis 服务端原子执行）。GroupedCache 用。
/// 只有锁值仍等于自己写入的 token 才删，否则不动。
///
/// 为什么不能直接 DEL：若「本节点回源很慢、超过了锁的 PX TTL」→ 锁已自动过期 → 别的节点
/// 重新抢到并写了【它的】token；此时本节点若无脑 DEL，会误删【别人正持有】的锁 → 击穿保护失效。
/// 用 GET 比对 + DEL 两步，且必须【原子】完成（中间不能被打断），所以塞进一条 Lua 脚本里。
///   KEYS[1] = 锁 key；ARGV[1] = 自己的 token
const UNLOCK_LUA: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

// ── single-flight 锁项的 RAII 守卫（CacheLayer 与 GroupedCache 共用，防错误路径泄漏锁表）──
// 旧实现只在 get_or_load 成功末尾手动 remove_if 回收锁项；loader 报错 / 序列化失败的 `?` 早退路径会绕过它，
// 导致该 key 的 Arc<Mutex> 永久滞留在 locks 表 —— 一批失败 key 累积下来 locks 无界增长
// （内存隐患，且可被构造大量失败 key 打爆）。改成 RAII：守卫在【任何】出口（正常返回 / ? 早退 /
// panic 展开）的 Drop 里都回收锁项。
//
// ── 关于 <'b>：它是【生命周期参数(lifetime)】，不是类型、不是值，而是给"借用能活多久"起的一个名字 ──
//   ※ 这里特意用 'b（不是惯例的 'a）来说明：名字是【任意】的，叫 'a / 'b / 'x 都行，编译完全等价。
//     生命周期名只是编译期的一个标签，不产生任何运行时开销，编译后即被擦除。
//   · 为什么需要它：本结构体存的是【借用】(带 & 的字段)，不是自有数据 —— `locks: &DashMap`、
//     `key: &str` 都只是"指向别人的指针"，没拿所有权。Rust 没有 GC，编译器必须在【编译期】证明：
//     "这些指针指向的东西，在 FlightGuard 活着的全程都还没被释放"，否则就是悬垂指针(dangling)。
//     `'b` 就是用来做这个证明的标签。任何【含借用字段】的 struct，都必须给借用标上生命周期。
//   · 怎么读：`<'b>` = 给这个 struct 引入一个名叫 'b 的生命周期；字段里的 `&'b DashMap`、`&'b str`
//     表示"这两个借用都至少要活到 'b 这么久"。编译器据此得出约束：
//     【FlightGuard 实例的存活范围 ⊆ 'b】，即守卫绝不能比它借用的 locks / key 活得更久。
//     在 get_or_load 里它借的是函数局部的 self.locks 和 key —— 函数返回时守卫先 Drop、之后局部变量
//     才离开作用域 → 守卫总是"先死"，约束成立，编译通过。
//   · 特例 'static：表示"整个程序期间都有效"，是唯一有固定含义的生命周期名（其余都可随意命名）。
//   · impl 上的 <'_> 是"匿名生命周期"：有但不必起名，让编译器推断（等价 impl<'b>...FlightGuard<'b>）。
/// key 级 single-flight 锁表的 RAII 清理守卫。
///
/// 业务读取同一个缓存 key 时只允许一个 loader 进入后端加载；这个守卫确保 loader 正常返回、提前返回或
/// panic 展开时都能把对应锁项从 `locks` 表中移除，避免失败 key 持续累积造成内存增长。
struct FlightGuard<'b> {
    // &'b：借用 locks 表（不夺所有权）；'b 标明这个借用的有效期，守卫不能比它活得久
    locks: &'b DashMap<String, Arc<Mutex<()>>>,
    // &'b str：借用本次的 single-flight key（借用不拷贝字符串）；同一个 'b → 和上面同期有效
    key: &'b str,
    // 这个字段是【自有数据】（Arc 持有所有权，不是借用），所以它不带 'b、不参与生命周期约束
    lock: Arc<Mutex<()>>,
}
impl Drop for FlightGuard<'_> {
    /// 释放 key 级 single-flight 锁表条目。
    ///
    /// Drop 是 Rust 的析构钩子（≈ 没有的 原实现 finalize，但确定性触发）：离开作用域立刻跑。
    fn drop(&mut self) {
        // remove_if + Arc::ptr_eq：只在表里的 Arc 仍是"我们这一把"（同一块内存）时才删，
        // 不误删后来者新建的同 key 锁（地址不同 → ptr_eq 为 false → 不删）
        self.locks
            .remove_if(self.key, |_, v| Arc::ptr_eq(v, &self.lock));
    }
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ CacheBackend —— L2 命令窄接口:隔离具体 Redis 连接类型                ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// [`CacheLayer`] 依赖的 L2 后端命令面。
///
/// 只暴露 cache-aside 真正需要的原子操作和只读健康探针(值以 JSON 字符串承载,TTL 走毫秒),把 `redis::ClusterConnection`
/// /`ConnectionManager` 这类**第三方具体连接类型**挡在 `CacheLayer` 之外——从而同一个 `CacheLayer` 既能跑在
/// 自建集群连接上([`ClusterConnectionBackend`]),也能由上层用**受管 Redis** 的 adapter 实现来复用连接
/// (`redis_ref: default`,adapter 在编排层实现)。single-flight、TTL 抖动、空值哨兵、serde 仍全在 `CacheLayer`,
/// 本 trait 只做无状态转发,不含任何缓存策略。
///
/// 对象安全(`Arc<dyn CacheBackend>`)靠 `async-trait`;方法只读写单 key,不感知 scene/group。
#[async_trait::async_trait]
pub trait CacheBackend: Send + Sync {
    /// 执行不修改业务数据的后端健康探针。
    ///
    /// 编排层的 readiness monitor 调用本方法确认当前代后端仍可服务。实现不得把成功构造客户端
    /// 当成健康，也不得写入探针 key；Redis 实现使用 `PING`。
    async fn health_check(&self) -> anyhow::Result<()>;

    /// 读一个 key;不存在返回 `Ok(None)`。底层报错原样上抛,由 `CacheLayer` 决定降级回源。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>>;

    /// 以毫秒 TTL 写入一个 key(PSETEX 语义);TTL 抖动/空值短 TTL 由 `CacheLayer` 决定后传入。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    /// - `value`: 已序列化的 JSON 载荷。
    /// - `ttl_ms`: 过期毫秒数(> 0)。
    async fn set(&self, key: &str, value: &str, ttl_ms: u64) -> anyhow::Result<()>;

    /// 删除一个 key(失效用)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn delete(&self, key: &str) -> anyhow::Result<()>;
}

/// 直接建在 `redis::cluster_async::ClusterConnection` 上的 [`CacheBackend`] 实现(自建集群连接路径)。
///
/// 保留 `cluster_async` 的多路复用语义：句柄 clone 廉价，每次操作 clone 一份可变句柄。
pub struct ClusterConnectionBackend {
    /// Redis 集群连接(cluster_async);clone 廉价,内部多路复用同一条连接。
    redis: ClusterConnection,
}

impl ClusterConnectionBackend {
    /// 用一个已建立的集群连接构造后端。
    ///
    /// # 参数
    /// - `redis`: 已连接的 `ClusterConnection`(通常来自 [`crate::connect_cluster`])。
    pub fn new(redis: ClusterConnection) -> Self {
        Self { redis }
    }
}

#[async_trait::async_trait]
impl CacheBackend for ClusterConnectionBackend {
    /// 使用 Redis `PING` 验证当前集群连接仍可完成一次协议往返。
    async fn health_check(&self) -> anyhow::Result<()> {
        let mut conn = self.redis.clone();
        let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
        anyhow::ensure!(
            pong.eq_ignore_ascii_case("PONG"),
            "cache backend ping rejected"
        );
        Ok(())
    }

    /// 读一个 key(GET,返回 `Option<String>`)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self.redis.clone();
        Ok(conn.get::<_, Option<String>>(key).await?)
    }

    /// 以毫秒 TTL 写入一个 key(PSETEX)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    /// - `value`: 已序列化的 JSON 载荷。
    /// - `ttl_ms`: 过期毫秒数。
    async fn set(&self, key: &str, value: &str, ttl_ms: u64) -> anyhow::Result<()> {
        let mut conn = self.redis.clone();
        conn.pset_ex::<_, _, ()>(key, value, ttl_ms).await?;
        Ok(())
    }

    /// 删除一个 key(DEL)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let mut conn = self.redis.clone();
        conn.del::<_, ()>(key).await?;
        Ok(())
    }
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ CacheLayer —— 通用 cache-aside + 三防（扁平 key + TTL 为主）              ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// Redis 二级缓存层,提供通用 cache-aside 读取、空值短缓存、key 级 single-flight 和 TTL 抖动。
///
/// 被多个 service 共享,外层通常包成 `Arc<CacheLayer>` 注入到宏展开代码或业务服务中。
pub struct CacheLayer {
    // L2 后端命令面:经 [`CacheBackend`] 转发,不再直接持有 `ClusterConnection`,从而具体连接类型
    // 可换成受管 Redis 的 adapter。single-flight/TTL/serde 仍在本层。
    backend: Arc<dyn CacheBackend>,
    // 缓存基础 TTL（秒）
    cache_ttl_secs: u64,
    // 空结果 TTL（秒），用于防穿透
    null_ttl_secs: u64,
    // single-flight 用：同一个 cache key 仅允许一个请求穿透到回源
    // key: 缓存键; value: 一把异步互斥锁（包成 Arc 让多请求共享同一把锁）
    //
    // ── 为什么需要这张「key->锁」表（防击穿 / cache stampede）──
    //   场景：某个热点 key 在 Redis 里【刚好过期】的瞬间，成百上千并发请求同时未命中，
    //         于是【全部一起冲到 DB 回源】——这就是缓存击穿，可能直接打垮数据库。
    //   解法：未命中后取出【该 key 专属】的锁并 lock().await，同 key 的并发请求在此串行排队，
    //         只有第一个进临界区回源 + 回填；其余堵在锁上，待其放锁后 double-check 命中缓存、不再回源。
    //         => 热点 key 失效瞬间，只有一个请求真正打到 DB。
    //
    // ── 类型逐层拆解（从里往外读）──
    //   Mutex<()>            一把锁，锁的是【空元组 ()】——只要互斥这个动作本身，不保护任何数据
    //   Arc<Mutex<()>>       把锁包成共享所有权，让【同一个 key】的多个并发请求拿到【同一把锁】
    //   DashMap<String, _>   key=缓存键, value=该 key 专属锁；分段加锁 => 锁粒度是「每个 key 一把」，
    //                        不同 key 互不阻塞，只串行化「同一个 key」的回源，并发度最大化
    //   最外层 Arc<...>      整张锁表被多个请求/任务共享（CacheLayer 自身常被包成 Arc 注入各 service）
    //
    // ── 几个关键约束（具体见 get_or_load 内注释）──
    //   · 取锁后立刻 .value().clone()：尽快释放 DashMap 的分段写锁，否则攥着它去 .await 会死锁
    //   · 用 tokio::sync::Mutex 而非 std::sync::Mutex：这把锁要【跨 .await 持有】（回源是异步的）
    //   · 用完靠 FlightGuard 删「自己这一把」：避免锁表无限膨胀，又不误删后来者新建的锁
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl CacheLayer {
    /// 构造 Redis 二级缓存层；由启动代码传入 Redis 集群连接与缓存 TTL 配置。
    ///
    /// # 参数
    /// - `redis`: Redis 集群连接句柄,用于读取、写入和删除 L2 缓存。
    /// - `cache_ttl_secs`: 正常缓存值的基础 TTL 秒数,写入时会叠加少量抖动防雪崩。
    /// - `null_ttl_secs`: 空结果哨兵的 TTL 秒数,用于防缓存穿透。
    pub fn new(redis: ClusterConnection, cache_ttl_secs: u64, null_ttl_secs: u64) -> Self {
        // 向后兼容入口:把自建集群连接包成默认 backend 再走 [`Self::with_backend`],既有调用方零改动。
        Self::with_backend(
            Arc::new(ClusterConnectionBackend::new(redis)),
            cache_ttl_secs,
            null_ttl_secs,
        )
    }

    /// 用任意 [`CacheBackend`] 构造二级缓存层。
    ///
    /// 让 `CacheLayer` 与具体 Redis 连接类型解耦:编排层可传入**复用受管 Redis** 的 adapter,而不必新开一条
    /// 集群连接。single-flight/TTL 抖动/空值哨兵/serde 全在本层,与 backend 具体实现无关。
    ///
    /// # 参数
    /// - `backend`: L2 命令后端(自建集群连接或受管 Redis adapter)。
    /// - `cache_ttl_secs`: 正常缓存值的基础 TTL 秒数,写入时叠加少量抖动防雪崩。
    /// - `null_ttl_secs`: 空结果哨兵的 TTL 秒数,用于防缓存穿透。
    pub fn with_backend(
        backend: Arc<dyn CacheBackend>,
        cache_ttl_secs: u64,
        null_ttl_secs: u64,
    ) -> Self {
        Self {
            backend,
            cache_ttl_secs,
            null_ttl_secs,
            locks: Arc::new(DashMap::new()),
        }
    }

    /// 对当前 L2 后端执行一次只读健康探针。
    ///
    /// 该入口只供拥有运行时生命周期的组件 monitor 使用；请求热路径不会同步探测远端。
    pub async fn health_check(&self) -> anyhow::Result<()> {
        self.backend.health_check().await
    }

    /// 删除一个 key（给缓存失效用，对标 Cacheable @CacheInvalidate 的远程删除）。
    /// 由 cacheable::invalidate 调用。删 L2(Redis)这一份;失败上抛由调用方决定怎么处理。
    ///
    /// # 参数
    /// - `key`: 完整 Redis 缓存 key,通常由宏按业务模板拼好。
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        // 经 backend 转发 DEL:失败原样上抛由调用方决定处理(具体连接类型不在本层)。
        self.backend.delete(key).await
    }

    /// 通用 cache-aside 读取:先查 Redis,未命中时用 single-flight 串行化回源并回填。
    ///
    /// 三防能力:空结果短缓存防穿透、key 级互斥防击穿、非空 TTL 抖动防雪崩。调用方只需要提供完整缓存 key 和回源闭包。
    ///
    /// # 参数
    /// - `key`: 完整缓存键,调用方需要带上命名空间或业务前缀避免跨业务碰撞。
    /// - `loader`: 缓存未命中时执行的回源闭包,通常是数据库或远程服务查询。
    pub async fn get_or_load<T, F, Fut>(&self, key: String, loader: F) -> anyhow::Result<T>
    where
        // ① T：缓存的值类型。
        //    Serialize       —— 能序列化成 JSON（写缓存时用 serde_json::to_string）
        //    DeserializeOwned —— 能从 JSON 反序列化出【完全自有】的值（读缓存时用）
        //                        （区别于 Deserialize<'de>：后者可能借用输入串；这里要
        //                         返回出去的自有值，故用 Owned 版）
        //    `+` = 同时满足这两个 trait。缓存里存的是 JSON 字符串，存取都需要。
        T: Serialize + DeserializeOwned,
        // ② F：回源闭包的类型。
        //    FnOnce()  —— "不接参数、至少能调用一次"（最宽松的闭包约束，允许闭包把
        //                 捕获的值 move 出去；这里只在未命中时调一次，正好够用）
        //    -> Fut    —— 调用后返回一个 Future（异步回源，见 ③）
        F: FnOnce() -> Fut,
        // ③ Fut：F 调用后返回的那个 Future。
        //    Future<Output = anyhow::Result<T>> —— 它 .await 完成后产出的值必须是
        //    anyhow::Result<T>：要么给出 T，要么给出 anyhow 错误。
        //    这就是为什么调用方闭包里要 .map_err(..) 把 sqlx::Error 转成 anyhow::Error。
        Fut: Future<Output = anyhow::Result<T>>,
    {
        // ---- 1. 先查缓存 ----
        // 经 backend 转发 GET:backend 返回 `Option<String>`——key 不存在时 `Ok(None)`,存在则 `Ok(Some(值))`。
        // 【插桩】计 redis GET 这一跳的耗时
        let t_get = Instant::now();
        let cached: Option<String> = match self.backend.get(&key).await {
            Ok(v) => v,
            // Redis 不可用时降级回源，只记日志，不让整个请求挂掉
            Err(e) => {
                warn!("redis get failed, fallback to source: {}", e);
                None
            }
        };
        let get_ms = t_get.elapsed().as_micros() as f64 / 1000.0;
        if let Some(s) = cached {
            // 命中：反序列化成功直接返回（命中空哨兵 "[]"/"null" 也会还原为空值 → 防穿透）
            // 反序列化失败视为脏缓存，降级回源
            match serde_json::from_str::<T>(&s) {
                Ok(v) => {
                    tracing::debug!("⏱ cache HIT  key={} redis_get={:.2}ms", key, get_ms);
                    return Ok(v);
                }
                Err(e) => warn!("cache value broken, drop and refetch: {}", e),
            }
        }
        tracing::debug!(
            "⏱ cache MISS key={} redis_get={:.2}ms (→ single-flight + db)",
            key,
            get_ms
        );

        // ---- 2. 未命中：进入 single-flight 临界区（防击穿）----
        // entry(key).or_insert_with(...)：取本 key 对应的锁，没有就新建一把空 Mutex 并塞进表。
        //   Mutex::new(()) 锁的是"空元组 ()"——我们要的只是互斥这个动作本身，不需要锁保护任何数据。
        // ★ 关键陷阱：entry() 返回的引用内部攥着 DashMap 的【分段写锁】。所以这里立刻
        //   .value().clone() 把里面的 Arc 拷一份出来，让整条语句结束时那个分段写锁守卫即刻释放；
        //   否则若攥着它去执行下面的 lock().await，期间别的线程对同段做 entry() 就会互相死锁。
        let lock = self
            .locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();
        // 拿锁；其他并发同 key 请求在此 .await 处串行排队等待。
        // _guard 是 RAII 守卫：它活着 = 锁被持有；它离开作用域（函数 return / 走出当前块）= 自动解锁，
        //   无需手动 unlock（对比 原实现 的 synchronized 块或 try/finally + lock.unlock()）。
        //   变量名带下划线前缀，表示"只为副作用而持有、不会去读它的值"，否则编译器会警告未使用变量。
        let _guard = lock.lock().await;
        // 锁项回收交给 RAII 守卫：任何出口（含下面 ? 早退）都会在 Drop 里删表项，杜绝泄漏。
        // 声明在 _guard 之后 → Drop 先于 _guard 执行（先删表项再解锁），二者用的是不同的锁，无死锁。
        let _flight = FlightGuard {
            locks: &self.locks,
            key: &key,
            lock: lock.clone(),
        };

        // ---- 3. double-check：拿到锁后再查一次，避免每个等待者都各自回源 ----
        // 经典的双重检查锁（DCL）：第一个请求在临界区内回源 + 写回缓存期间，后到的同 key 请求都堵在
        //   上面的 lock().await。等第一个释放锁，它们依次拿到锁后这一查就能命中缓存，于是直接返回、不再回源。
        // 这里用 `if let Ok(Some(s))`：只在"Redis 没报错(Ok) 且 key 存在(Some)"时才尝试用缓存；
        //   其余情况（Redis 报错 / key 仍不存在 / 反序列化失败）一律落空，继续往下走真正的回源。
        if let Ok(Some(s)) = self.backend.get(&key).await {
            if let Ok(v) = serde_json::from_str::<T>(&s) {
                debug!("cache hit after lock: key={}", key);
                return Ok(v);
            }
        }

        // ---- 4. 回源（调用方提供的 loader，比如真正查 DB）----
        // loader() 调一次拿到 Future，.await 驱动它跑完，结尾的 ? 是【错误传播运算符】：
        //   若结果是 Err(e)，立即让 get_or_load 整个函数 return Err(e)（连带前面已写好的清理也不再执行）；
        //   若是 Ok(v)，则解包出里面的值赋给 data。≈ 原实现 的"往上抛异常"，但完全显式、写在表达式末尾。
        // 【插桩】计回源(loader,通常是查 DB)这一段的耗时
        let t_db = Instant::now();
        let data = loader().await?;
        tracing::debug!(
            "⏱ loader(db) key={} db={:.2}ms",
            key,
            t_db.elapsed().as_micros() as f64 / 1000.0
        );

        // ---- 5. 回填（带随机抖动 TTL，防雪崩；空结果短 TTL，防穿透）----
        // to_string 可能失败（如类型含非法 JSON 值），同样用 ? 传播错误：序列化不出来就别往下写缓存了
        let payload = serde_json::to_string(&data)?;
        // 通用"空"判断（详见底部 is_empty_payload）：空数组/null/空对象/空串都算空结果。
        let is_empty = is_empty_payload(&payload);
        // 用毫秒级 TTL（PSETEX）：秒级 TTL 配毫秒抖动会被整数截断，抖动等于失效
        let ttl_ms = if is_empty {
            self.null_ttl_secs.saturating_mul(1000)
        } else {
            let jitter_ms: u64 = rand::thread_rng().gen_range(0..=1000);
            self.cache_ttl_secs
                .saturating_mul(1000)
                .saturating_add(jitter_ms)
        };
        // 写缓存失败不影响业务返回；只记日志（数据已从 loader 拿到，缓存只是加速副本）。经 backend 转发 PSETEX。
        if let Err(e) = self.backend.set(&key, &payload, ttl_ms).await {
            warn!("redis cache set failed: {}", e);
        }

        // ---- 6. 回收 single-flight 表项 ----
        // 已交给上面的 FlightGuard（Drop 时执行）：函数任何出口都会回收，无需手动 remove。
        // （仍持锁的等待者照常完成；新进入的请求会建新锁，但缓存已写回，double-check 命中即返回。）
        Ok(data)
    }
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ GroupedCache —— 分组缓存（对标 MybatisCache）+ 跨节点 single-flight       ║
// ║                                                                            ║
// ║ B. 写失效 / 关联失效（MybatisCache 的核心）                                ║
// ║   · 按【组】组织：一个 group = 一个 Redis Hash，entry 是 hash 的 field      ║
// ║   · invalidate_field(group, field) = HDEL            （≈ removeObject）     ║
// ║   · invalidate_group(group)        = DEL 整个 Hash    （≈ clear()）         ║
// ║   · 关联失效 register_clear_ref(source, also_clear)  （≈ @CacheClearRef）   ║
// ║                                                                            ║
// ║ C. 跨节点 single-flight（防分布式击穿）：两级去重                          ║
// ║   ① 进程内 tokio::Mutex（本节点并发压成 1）                                ║
// ║   ② Redis SET NX 分布式锁（跨节点压成 1，只有抢到锁的节点回源）            ║
// ║   没抢到的节点轮询等缓存出现；超时降级自行回源（防持锁节点崩溃饿死）。      ║
// ║                                                                            ║
// ║ 取舍：hash + 显式失效为主 + 长兜底 TTL                                         ║
// ║   显式 invalidate 保新鲜（写库后必须调）；另设【长兜底 TTL（小时/天级）】作 ║
// ║   安全网，防两类灾难：① 漏调/写-删竞态/崩在 del 前 → 否则永久脏；          ║
// ║   ② 无 TTL 的 hash 永不淘汰 → field（尤其负缓存）堆积 OOM。               ║
// ║   兜底 TTL 从 group【首次创建】起算、不被后续写续命（set-once），故热点组也 ║
// ║   会周期性整组过期重建。                                                    ║
// ║                                                                            ║
// ║ 已知取舍（诚实标注）：                                                      ║
// ║   · 非框架透明：需 service 主动 get_or_load + 写后主动 invalidate。        ║
// ║   · per-field 兜底 TTL：用 Redis 7.4+ 的 HPEXPIRE 给单个 field 设过期      ║
// ║     （HPEXPIRE ... NX，set-once）。空哨兵/正常值各自 backstop_ttl 后过期，  ║
// ║     穿透产生的 field 也按 field 各自自动淘汰（不必整组一起过期）。          ║
// ║   · 大 key / Cluster：一个 group = 一个 key = 单 slot/单节点，务必把       ║
// ║     namespace 切细、控制单组 field 基数，避免热点大 hash 集中单节点。       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

/// 分组缓存层。被多个 service 共享，外层通常包 `Arc<GroupedCache>` 注入。
pub struct GroupedCache {
    // Redis 连接管理器（内部是可 clone 的句柄，自带断线重连）
    redis: ConnectionManager,
    // 【兜底 TTL】（毫秒，0 = 不设）。注意：它【不是】用来保证新鲜度的（新鲜度由显式 invalidate 负责），
    // 而是纯【安全网】——只在"漏调 invalidate / 写-删竞态 / 进程崩在 del 前"等失效路径出问题时救命：
    //   · 防永久脏读：再脏也最多脏到这个 TTL 到点（建议设【小时/天级】，正常永远等不到它过期）
    //   · 防无界膨胀/OOM：无 TTL 的 hash 永不淘汰，堆积的 field（尤其负缓存）会撑爆内存
    // 关键：本 TTL 是【per-field】的（HPEXPIRE），从【该 field 首次创建】起算、不被后续写续命
    //   （HPEXPIRE ... NX：仅当该 field 尚无 TTL 时才设）。故每个 field 各自到点过期重建，
    //   漏清的脏 field 也能被兜底清掉，且互不影响（不像 per-group 那样整组一起过期）。
    backstop_ttl_ms: u64,
    // 分布式锁持有 TTL（毫秒）：抢到锁的节点最多持有这么久；即便它崩了，锁也会到点自动释放，不会永久卡住别人
    lock_ttl_ms: u64,
    // 没抢到分布式锁时，轮询等待"缓存被别的节点填好"的最长时间（毫秒）
    wait_ms: u64,
    // 轮询间隔（毫秒）：每隔这么久去看一眼缓存有没有出现
    poll_ms: u64,
    // 进程内 single-flight：缓存键 -> 一把异步锁（包 Arc 让同 key 并发请求共享同一把）
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    // 关联失效表：source_group -> {失效 source 时连带失效的组}（对应 MybatisCache.clearMap）
    // 外层 Arc 让多处共享，内层 RwLock 保证并发读写安全
    clear_map: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl GroupedCache {
    /// 构造。`backstop_ttl_secs` = 兜底 TTL（秒）。
    /// 以显式 invalidate 为主、长兜底 TTL 防漏清/防 OOM。
    ///   · 建议设【小时/天级】（如 86400 = 1 天）：正常靠失效保新鲜，TTL 永远等不到，只在漏网时救命。
    ///   · 设 0 = 关闭兜底（退化成纯 MybatisCache 无 TTL 模型）——除非你能从框架层面保证"写库必清缓存"，否则不推荐。
    /// 其余参数（锁 TTL / 等待 / 轮询）给了一组保守默认值，需要可后续暴露 setter。
    ///
    /// # 参数
    /// - `redis`: Redis 单机连接管理器,用于 Hash 读写、分布式锁和组失效。
    /// - `backstop_ttl_secs`: 每个 Hash field 的兜底 TTL 秒数;`0` 表示不设置兜底过期。
    pub fn new(redis: ConnectionManager, backstop_ttl_secs: u64) -> Self {
        Self::try_new(redis, backstop_ttl_secs)
            .expect("grouped cache backstop TTL must fit Redis milliseconds")
    }

    /// 校验 Redis 毫秒 TTL 后构造；外部配置应优先使用本入口，把单位转换错误留在启动阶段。
    pub fn try_new(redis: ConnectionManager, backstop_ttl_secs: u64) -> anyhow::Result<Self> {
        let backstop_ttl_ms = backstop_ttl_secs
            .checked_mul(1000)
            .filter(|millis| *millis <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("backstop_ttl_secs exceeds Redis TTL range"))?;
        Ok(Self {
            redis,
            // 秒转毫秒：内部统一用毫秒（PEXPIRE / PX 都是毫秒精度）
            backstop_ttl_ms,
            lock_ttl_ms: 3000, // 锁默认最多持 3s：够一次正常回源，又不至于崩溃后长时间卡别人
            wait_ms: 1000,     // 最多等 1s 别人把缓存填好
            poll_ms: 20,       // 每 20ms 探一次缓存
            locks: Arc::new(DashMap::new()),
            clear_map: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// 注册关联失效（对应 `@CacheClearRef`）：失效 `source_group` 时，连带失效 `also_clear` 里的组。
    ///
    /// 例：订单写表会让"用户持仓"缓存失效 → `register_clear_ref("order", &["position"])`，
    /// 之后 `invalidate_group("order")` 会同时 DEL `order` 和 `position` 两个 Hash。
    ///
    /// 对应 MybatisCache 构造里 `clearMap.computeIfAbsent(C, ..).add(this.id)` 那段建表逻辑。
    ///
    /// # 参数
    /// - `source_group`: 触发失效的源缓存组名。
    /// - `also_clear`: 源组失效时需要一并清理的其它缓存组列表。
    pub fn register_clear_ref(&self, source_group: &str, also_clear: &[&str]) {
        // 拿写锁：注册是写操作。write() 返回 RAII 守卫，本语句块结束即释放（不跨 .await，安全）
        let mut map = self.clear_map.write().unwrap();
        // entry(k).or_default()：取 source_group 对应的集合，没有就插一个空 HashSet 再返回可变引用
        //   （≈ 原实现 的 computeIfAbsent(k, _ -> new HashSet<>())）
        let set = map.entry(source_group.to_string()).or_default();
        // 把每个关联组登记进去。*g 是 &&str → &str 的解引用；to_string 转成自有 String 存表
        for g in also_clear {
            set.insert((*g).to_string());
        }
    }

    /// cache-aside 取值：先查 Hash，未命中则【两级 single-flight】回源并回填。
    ///
    /// ── 键约定（务必遵守，详见文件底部 SEP / field()）──
    ///   · `group` = Redis key   = 【业务隔离串】，整组失效的单位。例：`"spot:kline"` / `"perp:order"`
    ///   · `field` = Hash field  = 【子业务隔离串】，组内一条缓存。例：`"BTCUSDT:1m"` / `"{userId}:{symbol}"`
    ///   建议 field 用 `cache::field(&[...])` 拼，统一分隔符、避免手拼碰撞；并控制单组 field 基数（防大 key）。
    ///
    /// ── 函数签名拆解 ──
    ///   get_or_load<T, F, Fut>(&self, group, field, loader) -> `anyhow::Result<T>`
    ///   ├─ <T, F, Fut>          三个泛型，调用处自动推断
    ///   ├─ &self                只读借用（不改 GroupedCache，故可多请求并发调）
    ///   ├─ group / field        业务隔离 key / 子业务隔离 field（见上"键约定"）
    ///   ├─ loader: F            回源闭包（未命中时调用，相当于真正查 DB）
    ///   └─ -> `anyhow::Result<T>` 成功返回 T；失败返回 anyhow 错误
    ///
    /// # 参数
    /// - `group`: Redis Hash key,表示整组缓存和整组失效的业务边界。
    /// - `field`: Redis Hash field,表示组内单条缓存的子业务键。
    /// - `loader`: 组内 field 未命中时执行的回源闭包。
    pub async fn get_or_load<T, F, Fut>(
        &self,
        group: &str,
        field: &str,
        loader: F,
    ) -> anyhow::Result<T>
    where
        // 三个约束含义同 CacheLayer.get_or_load（详见那边的 ①②③ 注释）
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        // clone 连接句柄（内部复用底层连接，ConnectionManager 设计上就廉价 clone）。
        // 标 mut 是因为下面命令会改连接内部状态（pipeline 缓冲等），需可变借用
        let mut conn = self.redis.clone();

        // ---- 1. 先查缓存（HGET group field）----
        // try_read 命中（含空哨兵 → 防穿透）就直接返回，根本不用进 single-flight
        if let Some(v) = self.try_read::<T>(&mut conn, group, field).await {
            return Ok(v);
        }

        // ---- 2. 进程内 single-flight（先把本节点并发压成 1）+ RAII 防泄漏 ----
        // 单飞 key：group 与 field 拼一起，避免不同 (group,field) 撞同一把锁。format! ≈ String.format
        let sf_key = format!("{group}::{field}");
        // 取本 key 的锁，没有就新建一把空 Mutex(()) 塞进表。
        // ★ 关键陷阱：entry() 返回的引用内部攥着 DashMap 的分段写锁，所以立刻 .value().clone() 把里面
        //   的 Arc 拷一份出来，让整条语句结束时分段写锁即释放；否则攥着它去 lock().await 会和别的线程死锁。
        let lock = self
            .locks
            .entry(sf_key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();
        // 拿锁：同节点其他同 key 请求在此 .await 处串行排队等待。
        // _guard 是 RAII 守卫：活着=持锁，离开作用域=自动解锁；下划线前缀表示"只为副作用持有、不读它"
        let _guard = lock.lock().await;
        // 单飞锁项的回收交给 FlightGuard：本函数任何出口（含下面 ? 早退）都会在 Drop 里删表项，杜绝泄漏。
        // 声明在 _guard 之后 → Drop 先于 _guard（先删表项再解锁）；两者是不同的锁，无死锁。
        let _flight = FlightGuard {
            locks: &self.locks,
            key: &sf_key,
            lock: lock.clone(),
        };

        // ---- 3. double-check：拿到锁后再查一次 ----
        // 同节点的等待者在第一个请求回源+回填期间都堵在上面 lock().await；待其放锁，它们依次拿锁后
        // 这一查就命中，直接返回、不再回源（经典双重检查锁 DCL）。
        if let Some(v) = self.try_read::<T>(&mut conn, group, field).await {
            return Ok(v);
        }

        // ---- 4. 跨节点 single-flight：抢 Redis 分布式锁（SET key token NX PX ttl）----
        // 锁 key 与单飞 key 区分前缀，避免和业务缓存键混淆
        let lock_key = format!("sf:lock:{group}::{field}");
        // token：本次持锁的唯一标识，用于释放时 compare-and-del（防误删他人锁）。
        // rand::random::<u64>() 取一个随机 u64；{:016x} 格式化成 16 位十六进制字符串
        let token = format!("{:016x}", rand::random::<u64>());
        // 手动拼 SET 命令（redis 高层 API 不直接暴露 NX+PX 组合，用 cmd 最直观）：
        //   SET lock_key token NX PX lock_ttl_ms
        //   NX = 仅当 key 不存在才设置（抢锁语义）；PX = 毫秒级过期（到点自动释放，崩溃也不卡死）
        // 返回：抢到 → Some("OK")；没抢到（key 已存在）→ None（Redis 返回 nil）
        let acquired: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(self.lock_ttl_ms)
            .query_async(&mut conn)
            .await
            // Redis 出错时不让缓存层挂掉：当作"没抢到"，走下面的等待/降级
            .unwrap_or(None);

        if acquired.is_some() {
            // 抢到了：本节点（且此刻全局唯一）负责回源 + 回填
            let result = self
                .load_and_fill::<T, F, Fut>(&mut conn, group, field, loader)
                .await;
            // 释放锁（compare-and-del Lua）：只有锁值还是自己的 token 才删，防超时后误删别人锁。
            // KEYS[1]=lock_key（.key 传入），ARGV[1]=token（.arg 传入）；返回值用不上，忽略错误
            let _ = redis::Script::new(UNLOCK_LUA)
                .key(&lock_key)
                .arg(&token)
                .invoke_async::<i64>(&mut conn)
                .await;
            // 不管回源成功失败，结果原样返回（失败时 result 是 Err，调用方感知）
            return result;
        }

        // ---- 5. 没抢到锁：别的节点正在回源，轮询等缓存出现（bounded，防无限等）----
        // 截止时刻 = 现在 + wait_ms（Instant 单调时钟，不受墙钟回拨影响）
        let deadline = Instant::now() + Duration::from_millis(self.wait_ms);
        while Instant::now() < deadline {
            // 退避一会儿再看，别空转烧 CPU
            tokio::time::sleep(Duration::from_millis(self.poll_ms)).await;
            // 缓存被别的节点填好了 → 命中返回（这就是跨节点去重的收益：本请求没打 DB）
            if let Some(v) = self.try_read::<T>(&mut conn, group, field).await {
                return Ok(v);
            }
        }

        // ---- 6. 等待超时降级：自行回源 ----
        // 走到这里说明等了 wait_ms 缓存还没出现：很可能持锁节点崩了 / 回源特别慢。
        // 为不让本请求饿死，降级为自己回源一次（代价：极端情况下可能有 >1 个节点回源，但不会卡死）。
        warn!(
            "grouped cache distributed single-flight wait timeout, degrade to local load: {}::{}",
            group, field
        );
        self.load_and_fill::<T, F, Fut>(&mut conn, group, field, loader)
            .await
    }

    /// 失效单个条目（≈ MybatisCache.removeObject）：HDEL group field。
    ///
    /// # 参数
    /// - `group`: Redis Hash key,表示要操作的缓存组。
    /// - `field`: Redis Hash field,表示要删除的组内条目。
    pub async fn invalidate_field(&self, group: &str, field: &str) -> anyhow::Result<()> {
        let mut conn = self.redis.clone();
        // turbofish ::<_, _, ()>：前两个 _ 让编译器推断 key/field 类型；()=不关心 HDEL 的返回值（删了几个）
        conn.hdel::<_, _, ()>(group, field).await?;
        Ok(())
    }

    /// 失效整个组 + 关联组（≈ MybatisCache.clear() + @CacheClearRef 级联）。
    ///
    /// # 参数
    /// - `group`: 要清理的源缓存组名;函数会同时清理已登记的关联组。
    pub async fn invalidate_group(&self, group: &str) -> anyhow::Result<()> {
        // 先算出要删除的目标集合：自身 + clear_map 里登记的关联组。
        // read() 拿读锁，在这一行同步算完即释放（不跨 .await），再去做异步 DEL
        let targets = cascade_targets(&self.clear_map.read().unwrap(), group);
        let mut conn = self.redis.clone();
        // DEL 接受多个 key：一次清理自身 + 关联组（Vec<String> 会展开成多个 key 参数）。
        conn.del::<_, ()>(targets).await?;
        Ok(())
    }

    // ── 内部辅助 ──────────────────────────────────────────────────────────

    /// 读 Hash field 并反序列化；脏值 / Redis 报错都降级为「未命中」（返回 None 继续回源）。
    ///
    /// # 参数
    /// - `conn`: 执行 HGET 的 Redis connection manager。
    /// - `group`: 场景对应的 Redis Hash key。
    /// - `field`: Hash field,通常由业务缓存 key 归一化得到。
    async fn try_read<T: DeserializeOwned>(
        &self,
        conn: &mut ConnectionManager,
        group: &str,
        field: &str,
    ) -> Option<T> {
        // HGET group field：用 Option<String> 接 —— field 不存在返回 None（而非报错），存在返回 Some
        match conn.hget::<_, _, Option<String>>(group, field).await {
            // 命中且能反序列化 → 返回（空哨兵 "[]"/"null" 也会还原为空值，达成防穿透）
            Ok(Some(s)) => match serde_json::from_str::<T>(&s) {
                Ok(v) => {
                    debug!("grouped cache hit: {}::{}", group, field);
                    Some(v)
                }
                // 反序列化失败=脏缓存：当作未命中，落空去回源（之后回填会覆盖脏值）
                Err(e) => {
                    warn!(
                        "grouped cache broken value {}::{}, refetch: {}",
                        group, field, e
                    );
                    None
                }
            },
            // field 不存在 → 未命中
            Ok(None) => None,
            // Redis 报错 → 降级回源，只记日志，不让请求挂掉
            Err(e) => {
                warn!("grouped cache hget failed, fallback to source: {}", e);
                None
            }
        }
    }

    /// 回源 + 回填（HSET），并按需给【该 field】设 per-field 兜底 TTL（HPEXPIRE，带抖动防雪崩）。
    ///
    /// # 参数
    /// - `conn`: 执行 HSET/HPEXPIRE 的 Redis connection manager。
    /// - `group`: 场景对应的 Redis Hash key。
    /// - `field`: Hash field,通常由业务缓存 key 归一化得到。
    /// - `loader`: 缓存未命中时执行的回源加载闭包。
    async fn load_and_fill<T, F, Fut>(
        &self,
        conn: &mut ConnectionManager,
        group: &str,
        field: &str,
        loader: F,
    ) -> anyhow::Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        // 调一次 loader 拿 Future 并 .await 驱动它跑完；? 传播错误（失败就别往下写缓存）
        let data = loader().await?;
        // 序列化成 JSON 字符串准备写缓存；序列化失败同样用 ? 传播
        let payload = serde_json::to_string(&data)?;
        // 空结果也写入（这是【防穿透】：下次 HGET 命中空哨兵直接还原空值，不再打 DB）。
        // 这里判一下只为打日志/可观测；写入动作两种情况都做
        if is_empty_payload(&payload) {
            debug!(
                "grouped cache negative-cache (penetration guard): {}::{}",
                group, field
            );
        }
        // HSET group field payload：写回缓存。turbofish ::<_,_,_,()> 末位 () = 不关心返回值
        if let Err(e) = conn.hset::<_, _, _, ()>(group, field, &payload).await {
            // 写缓存失败不影响业务返回，只记日志（数据已经从 loader 拿到了）
            warn!("grouped cache hset failed: {}", e);
        } else if self.backstop_ttl_ms > 0 {
            // per-field 兜底 TTL（Redis 7.4+ 的 HPEXPIRE）：给【单个 field】设毫秒级过期。
            //   HPEXPIRE key ms NX FIELDS 1 field
            //   · NX = 仅当该 field 当前【没有】过期时间时才设 → "set-once"：从该 field 首次创建起算，
            //          后续写入（HSET 不会清已有 field 的 TTL）不续命，故漏清的脏 field 也会到点被清。
            //   · 相比旧的 per-group PEXPIRE：每个 field 各自过期、互不影响，连穿透产生的负缓存 field
            //          也能按 field 单独自动淘汰（不必整组一起过期）。
            //   · FIELDS 1 field = 只给这 1 个 field 设；HPEXPIRE 返回每 field 一个状态码（数组），这里用不上。
            // 0~1000ms 抖动防雪崩（大量 field 不在同一刻集体过期）；as i64 因毫秒数用 i64
            let jitter: u64 = rand::thread_rng().gen_range(0..=1000);
            let ms = (self.backstop_ttl_ms + jitter) as i64;
            // redis-rs 0.25 未必有高层 hpexpire 方法，用 cmd 手拼最稳；&mut *conn 把 &mut 重借一份传进去
            let _: redis::RedisResult<Vec<i64>> = redis::cmd("HPEXPIRE")
                .arg(group)
                .arg(ms)
                .arg("NX")
                .arg("FIELDS")
                .arg(1)
                .arg(field)
                .query_async(&mut *conn)
                .await;
        }
        // 返回从 loader 拿到的数据（即使上面写缓存失败，业务仍拿到正确结果）
        Ok(data)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 纯函数（不依赖 self / Redis，保持键规则与数据面解耦）
// ════════════════════════════════════════════════════════════════════════════

/// 键约定的统一分隔符。业务串 / 子业务串内【禁止】出现它，否则拼接后可能与别的键碰撞。
pub const SEP: &str = ":";

/// 拼 hash field（子业务隔离串）：把多段用 [`SEP`] 连接。
///
/// 用它而非各 service 手拼字符串：① 分隔符集中一处、改一次全改；② 杜绝"这里用 `:`、那里用 `_`"的不一致。
/// 例：`field(&["BTCUSDT", "1m"])` → `"BTCUSDT:1m"`；`field(&[&uid.to_string(), symbol])` → `"123:BTCUSDT"`。
///
/// # 参数
/// - `parts`: 组成 Hash field 的业务片段,每个片段内部不应包含 [`SEP`]。
pub fn field(parts: &[&str]) -> String {
    parts.join(SEP)
}

/// 通用"空"判断：空数组 / null / 空对象 / 空串都算空（CacheLayer 与 GroupedCache 共用）。
/// matches! 宏：判断 s 是否匹配右边任一字面量，等价一串 ||，但更紧凑可读。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn is_empty_payload(s: &str) -> bool {
    matches!(s, "[]" | "null" | "{}" | "")
}

/// 计算失效 `group` 时要 DEL 的目标集合：自身 + clear_map 里登记的关联组。
/// 对应 MybatisCache.clear() 里 `clearMap.get(this.id)`（其集合天然含 id 自身）的逻辑。
///
/// # 参数
/// - `map`: 当前函数读取或更新的键值映射。
/// - `group`: 消费组、服务分组或任务分组名称。
fn cascade_targets(map: &HashMap<String, HashSet<String>>, group: &str) -> Vec<String> {
    // 用 HashSet 收集去重：自身和关联组可能重叠，避免 DEL 重复 key
    let mut set: HashSet<String> = HashSet::new();
    // 自身一定在内（同 MybatisCache：clearMap[id] 含 id）
    set.insert(group.to_string());
    // 查 clear_map：登记过关联组就并进来；没登记则只剩自身
    if let Some(deps) = map.get(group) {
        for d in deps {
            set.insert(d.clone());
        }
    }
    // HashSet → Vec：DEL 命令需要一串 key
    set.into_iter().collect()
}
