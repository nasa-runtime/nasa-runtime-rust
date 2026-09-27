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
//   └─ GroupedCache ── 分组缓存 + 跨节点 single-flight
//       按【组】组织(一个 group = 一个 Redis Hash)，以【显式失效】为主、长兜底 TTL 兜底。
//       支持 invalidate_field/invalidate_group 与关联失效；single-flight 两级
//       去重(进程内 tokio::Mutex + Redis SET NX 分布式锁)。
//       适合：按业务组显式失效的 cache-aside 场景；并发中的旧 loader 仍可能回填旧值。
//
// 本文件提供显式缓存调用：service 主动执行 get_or_load / invalidate，不依赖数据访问层隐式拦截。
//
// ⚠️ 必须在 Tokio 运行时内使用。
// ⚠️ GroupedCache 的 per-field 兜底 TTL 用 HPEXPIRE，需 Redis 7.4+。
// ============================================================================

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use rand::Rng;
use redis::aio::ConnectionManager;
use redis::cluster_async::ClusterConnection;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// 释放分布式锁的 compare-and-del 脚本（Lua，在 Redis 服务端原子执行）。GroupedCache 用。
/// 只有锁值仍等于自己写入的 token 才删，否则不动。
///
/// 为什么不能直接 DEL：若「本节点回源很慢、超过了锁的 PX TTL」→ 锁已自动过期 → 别的节点
/// 重新抢到并写了【它的】token；此时本节点若无脑 DEL，会误删【别人正持有】的锁 → 击穿保护失效。
/// 用 GET 比对 + DEL 两步，且必须【原子】完成（中间不能被打断），所以塞进一条 Lua 脚本里。
///   KEYS[1] = 锁 key；ARGV[1] = 自己的 token
const UNLOCK_LUA: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

/// 写入前确认补偿删除权限；Redis 脚本不回滚，因此过期失败必须在原子执行段中撤下新值。
const WRITE_FIELD_LUA: &str = r#"
if not redis.acl_check_cmd('HSET', KEYS[1], ARGV[1], ARGV[2])
    or not redis.acl_check_cmd('HDEL', KEYS[1], ARGV[1]) then
    return redis.error_reply('grouped cache requires HSET and HDEL permission')
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local expiry = redis.pcall('HPEXPIRE', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
if expiry.err or expiry[1] ~= 1 then
    redis.call('HDEL', KEYS[1], ARGV[1])
    return redis.error_reply('grouped cache field expiry unavailable')
end
return 1
"#;

/// 同 key 的持有者和等待者共同保有锁项；最后一个请求退出时回收。
struct FlightGuard<'b> {
    locks: &'b DashMap<String, Arc<Mutex<()>>>,
    key: &'b str,
    lock: Option<Arc<Mutex<()>>>,
}

impl<'b> FlightGuard<'b> {
    /// 业务作用：在等待互斥锁之前登记请求，取消等待也能归还锁项引用。
    /// 参数说明：`locks` 为进程内锁表；`key` 为完整缓存身份。
    /// 返回：与同 key 所有在途请求共享锁的守卫，不跨异步等待持有锁表分段锁。
    fn enter(locks: &'b DashMap<String, Arc<Mutex<()>>>, key: &'b str) -> Self {
        let lock = locks
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();
        Self {
            locks,
            key,
            lock: Some(lock),
        }
    }

    /// 业务作用：取得本请求登记的互斥锁，锁守卫必须先于请求登记释放。
    /// 参数说明：无。
    /// 返回：当前 flight 的锁；引用受登记守卫生命周期约束。
    fn mutex(&self) -> &Mutex<()> {
        self.lock.as_deref().expect("flight remains registered")
    }
}

impl Drop for FlightGuard<'_> {
    /// 业务作用：归还当前请求的登记，只回收没有持有者和等待者的锁项。
    /// 参数说明：无。
    /// 返回：最后一个请求退出后移除对应项；其它请求继续使用同一把锁。
    fn drop(&mut self) {
        let Some(lock) = self.lock.take() else {
            return;
        };
        // 引用归还和最后持有者判定必须在同一分段锁内完成，避免并发退出遗留空锁项。
        self.locks.remove_if(self.key, move |_, current| {
            let same = Arc::ptr_eq(current, &lock);
            drop(lock);
            same && Arc::strong_count(current) == 1
        });
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
    /// 业务作用：执行不修改业务数据的后端健康探针。
    ///
    /// 编排层的 readiness monitor 调用本方法确认当前代后端仍可服务。实现不得把成功构造客户端
    /// 当成健康，也不得写入探针 key；Redis 实现使用 `PING`。
    async fn health_check(&self) -> anyhow::Result<()>;

    /// 业务作用：读一个 key;不存在返回 `Ok(None)`。底层报错原样上抛,由 `CacheLayer` 决定降级回源。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>>;

    /// 业务作用：以毫秒 TTL 写入一个 key(PSETEX 语义);TTL 抖动/空值短 TTL 由 `CacheLayer` 决定后传入。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    /// - `value`: 已序列化的 JSON 载荷。
    /// - `ttl_ms`: 过期毫秒数(> 0)。
    async fn set(&self, key: &str, value: &str, ttl_ms: u64) -> anyhow::Result<()>;

    /// 业务作用：删除一个 key(失效用)。
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
    /// 业务作用：用一个已建立的集群连接构造后端。
    ///
    /// # 参数
    /// - `redis`: 已连接的 `ClusterConnection`(通常来自 [`crate::connect_cluster`])。
    pub fn new(redis: ClusterConnection) -> Self {
        Self { redis }
    }
}

#[async_trait::async_trait]
impl CacheBackend for ClusterConnectionBackend {
    /// 业务作用：使用 Redis `PING` 验证当前集群连接仍可完成一次协议往返。
    async fn health_check(&self) -> anyhow::Result<()> {
        let mut conn = self.redis.clone();
        let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
        anyhow::ensure!(
            pong.eq_ignore_ascii_case("PONG"),
            "cache backend ping rejected"
        );
        Ok(())
    }

    /// 业务作用：读一个 key(GET,返回 `Option<String>`)。
    ///
    /// # 参数
    /// - `key`: 完整缓存 key。
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self.redis.clone();
        Ok(conn.get::<_, Option<String>>(key).await?)
    }

    /// 业务作用：以毫秒 TTL 写入一个 key(PSETEX)。
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

    /// 业务作用：删除一个 key(DEL)。
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
    /// 业务作用：构造 Redis 二级缓存层；由启动代码传入 Redis 集群连接与缓存 TTL 配置。
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

    /// 业务作用：用任意 [`CacheBackend`] 构造二级缓存层。
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

    /// 业务作用：对当前 L2 后端执行一次只读健康探针。
    ///
    /// 该入口只供拥有运行时生命周期的组件 monitor 使用；请求热路径不会同步探测远端。
    pub async fn health_check(&self) -> anyhow::Result<()> {
        self.backend.health_check().await
    }

    /// 业务作用：删除一个 key（给缓存失效用，对标 Cacheable @CacheInvalidate 的远程删除）。
    /// 由 cacheable::invalidate 调用。删 L2(Redis)这一份;失败上抛由调用方决定怎么处理。
    ///
    /// # 参数
    /// - `key`: 完整 Redis 缓存 key,通常由宏按业务模板拼好。
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        // 经 backend 转发 DEL:失败原样上抛由调用方决定处理(具体连接类型不在本层)。
        self.backend.delete(key).await
    }

    /// 业务作用：通用 cache-aside 读取:先查 Redis,未命中时用 single-flight 串行化回源并回填。
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

        // 先登记再等待，避免持有者退出时把等待者仍在使用的锁移除。
        let flight = FlightGuard::enter(&self.locks, &key);
        let _guard = flight.mutex().lock().await;

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
        // 回源失败直接交还业务错误；守卫负责归还单飞责任，不将失败写入缓存。
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

// GroupedCache 按 Redis Hash 组织缓存，支持单字段、整组及关联组失效。
// 进程内 single-flight 覆盖同 key 的全部持有者与等待者；跨节点锁仅减少重复回源，
// 等待超时或租期短于 loader 时仍可能并发回源，不构成跨节点严格互斥或强一致协议。
// 正 TTL 在每次字段写入时重新计时，空哨兵和正常值分别过期，不设置整组固定到期点。
// 旧 loader 可在失效完成后重新回填并开始新的 TTL，因此提交后的陈旧窗口不只取决于 TTL。
// 每个 group 位于同一个 Redis slot，业务须限制单组字段数量与写入速率。

/// 分组缓存层。被多个 service 共享，外层通常包 `Arc<GroupedCache>` 注入。
pub struct GroupedCache<C = ConnectionManager> {
    // Redis 连接管理器（内部是可 clone 的句柄，自带断线重连）
    redis: C,
    // 每次成功写入后独立计算字段保留时间（毫秒，0 = 不设）；不限制 loader 延迟或瞬时字段基数。
    // 写值与设置过期同脚本完成，过期失败撤下本次字段；其它 field 的过期时间不受影响。
    backstop_ttl_ms: u64,
    // 分布式锁持有 TTL（毫秒）：抢到锁的节点最多持有这么久；即便它崩了，锁也会到点自动释放，不会永久卡住别人
    lock_ttl_ms: u64,
    // 没抢到分布式锁时，轮询等待"缓存被别的节点填好"的最长时间（毫秒）
    wait_ms: u64,
    // 轮询间隔（毫秒）：每隔这么久去看一眼缓存有没有出现
    poll_ms: u64,
    // 进程内 single-flight：缓存键 -> 一把异步锁（包 Arc 让同 key 并发请求共享同一把）
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    // 关联失效表：source_group -> {失效 source 时连带失效的组}。
    // 外层 Arc 让多处共享，内层 RwLock 保证并发读写安全
    clear_map: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl<C> GroupedCache<C>
where
    C: redis::aio::ConnectionLike + Clone + Send + Sync + 'static,
{
    /// 业务作用：在接纳分组缓存请求前证明 field TTL 可用，探针与清理在同一脚本完成。
    /// 参数说明：无。
    /// 返回：过期语义确认时成功；权限、版本或返回异常时拒绝，未启用 TTL 时不创建探针。
    pub async fn verify_field_ttl(&self) -> anyhow::Result<()> {
        if self.backstop_ttl_ms == 0 {
            return Ok(());
        }
        let key = format!(
            "cache:field-probe:{}:{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let ttl: i64 = redis::Script::new(r#"
if not redis.acl_check_cmd('DEL', KEYS[1]) or not redis.acl_check_cmd('HSET', KEYS[1], 'probe', '1') then
    return redis.error_reply('cache probe permissions are insufficient')
end
if not redis.acl_check_cmd('HPEXPIRE', KEYS[1], 5000, 'FIELDS', 1, 'probe') then
    return redis.error_reply('cache field expiry permission is insufficient')
end
redis.call('HSET', KEYS[1], 'probe', '1')
local expiry = redis.pcall('HPEXPIRE', KEYS[1], 5000, 'FIELDS', 1, 'probe')
if expiry.err then
    redis.call('DEL', KEYS[1])
    return redis.error_reply('cache field expiry is unavailable')
end
local ttl = redis.pcall('HPTTL', KEYS[1], 'FIELDS', 1, 'probe')
redis.call('DEL', KEYS[1])
if ttl.err then return redis.error_reply('cache field TTL cannot be observed') end
return ttl[1]
"#).key(&key).invoke_async(&mut self.redis.clone()).await?;
        anyhow::ensure!(
            ttl > 0 && ttl <= 5000,
            "cache field expiry is not effective"
        );
        Ok(())
    }

    /// 业务作用：创建按组显式失效、每次字段写入独立过期的缓存。
    /// 参数说明：`redis` 为 Redis 连接；`backstop_ttl_secs` 为字段保留秒数，最大一年，零表示不设置过期。
    /// 返回：配置合法时返回缓存；越界时 panic，需要可恢复错误的调用方使用 try_new。
    /// 禁用过期时业务自行限制字段保留与数量；正 TTL 也不能消除旧 loader 晚到回填的窗口。
    pub fn new(redis: C, backstop_ttl_secs: u64) -> Self {
        Self::try_new(redis, backstop_ttl_secs)
            .expect("grouped cache backstop TTL must fit Redis milliseconds")
    }

    /// 业务作用：在启动阶段校验 field 过期配置，避免写入后才发现过期范围无效。
    /// 参数说明：`redis` 为单机连接；`backstop_ttl_secs` 为每次写入后的过期秒数，最大一年，零表示不设置过期。
    /// 返回：配置合法时返回分组缓存；越界时返回错误，不写入 Redis。
    pub fn try_new(redis: C, backstop_ttl_secs: u64) -> anyhow::Result<Self> {
        let backstop_ttl_ms = backstop_ttl_secs
            .checked_mul(1000)
            .filter(|millis| *millis <= 365 * 24 * 60 * 60 * 1000)
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

    /// 业务作用：注册关联失效：失效 `source_group` 时，连带失效 `also_clear` 里的组。
    ///
    /// 例：订单写表会让"用户持仓"缓存失效 → `register_clear_ref("order", &["position"])`，
    /// 之后 `invalidate_group("order")` 会同时 DEL `order` 和 `position` 两个 Hash。
    ///
    /// 注册关系在写入时去重，读取路径只需展开已经确认的目标集合。
    ///
    /// # 参数
    /// - `source_group`: 触发失效的源缓存组名。
    /// - `also_clear`: 源组失效时需要一并清理的其它缓存组列表。
    pub fn register_clear_ref(&self, source_group: &str, also_clear: &[&str]) {
        // 拿写锁：注册是写操作。write() 返回 RAII 守卫，本语句块结束即释放（不跨 .await，安全）
        let mut map = self.clear_map.write().unwrap();
        let set = map.entry(source_group.to_string()).or_default();
        // 把每个关联组登记进去。*g 是 &&str → &str 的解引用；to_string 转成自有 String 存表
        for g in also_clear {
            set.insert((*g).to_string());
        }
    }

    /// 业务作用：cache-aside 取值：先查 Hash，未命中则【两级 single-flight】回源并回填。
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

        // 编码长度区分组名和 field 边界，业务名称中的分隔符不会串用锁项。
        let sf_key = format!("{}:{group}{field}", group.len());
        let flight = FlightGuard::enter(&self.locks, &sf_key);
        let _guard = flight.mutex().lock().await;

        // ---- 3. double-check：拿到锁后再查一次 ----
        // 同节点的等待者在第一个请求回源+回填期间都堵在上面 lock().await；待其放锁，它们依次拿锁后
        // 这一查就命中，直接返回、不再回源（经典双重检查锁 DCL）。
        if let Some(v) = self.try_read::<T>(&mut conn, group, field).await {
            return Ok(v);
        }

        // ---- 4. 跨节点 single-flight：抢 Redis 分布式锁（SET key token NX PX ttl）----
        // 锁 key 与单飞 key 区分前缀，避免和业务缓存键混淆
        let lock_key = format!("sf:lock:{}:{group}{field}", group.len());
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

    /// 业务作用：通过 HDEL 失效指定 group 的单个 field。
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

    /// 业务作用：失效整个组及其登记的关联组。
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

    /// 业务作用：读 Hash field 并反序列化；脏值 / Redis 报错都降级为「未命中」（返回 None 继续回源）。
    ///
    /// # 参数
    /// - `conn`: 执行 HGET 的 Redis connection manager。
    /// - `group`: 场景对应的 Redis Hash key。
    /// - `field`: Hash field,通常由业务缓存 key 归一化得到。
    async fn try_read<T: DeserializeOwned>(
        &self,
        conn: &mut C,
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

    /// 业务作用：回源 + 回填（HSET），并按需给【该 field】设 per-field 兜底 TTL（HPEXPIRE，带抖动防雪崩）。
    ///
    /// 返回：回源成功时返回业务值；缓存写入或过期失败会记录降级，回源失败直接返回错误。
    ///
    /// 参数说明：
    /// - `conn`: 执行 HSET/HPEXPIRE 的 Redis connection manager。
    /// - `group`: 场景对应的 Redis Hash key。
    /// - `field`: Hash field,通常由业务缓存 key 归一化得到。
    /// - `loader`: 缓存未命中时执行的回源加载闭包。
    async fn load_and_fill<T, F, Fut>(
        &self,
        conn: &mut C,
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
        let result = if self.backstop_ttl_ms == 0 {
            conn.hset::<_, _, _, ()>(group, field, &payload).await
        } else {
            let jitter: u64 = rand::thread_rng().gen_range(0..=1000);
            // 脚本内写入和过期不可被其它写入穿插；过期失败时删除本次字段，避免留下永久缓存。
            redis::Script::new(WRITE_FIELD_LUA)
                .key(group)
                .arg(field)
                .arg(&payload)
                .arg(self.backstop_ttl_ms + jitter)
                .invoke_async::<()>(&mut *conn)
                .await
        };
        if let Err(error) = result {
            warn!("grouped cache write with expiry failed: {}", error);
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

/// 业务作用：拼 hash field（子业务隔离串）：把多段用 [`SEP`] 连接。
///
/// 用它而非各 service 手拼字符串：① 分隔符集中一处、改一次全改；② 杜绝"这里用 `:`、那里用 `_`"的不一致。
/// 例：`field(&["BTCUSDT", "1m"])` → `"BTCUSDT:1m"`；`field(&[&uid.to_string(), symbol])` → `"123:BTCUSDT"`。
///
/// # 参数
/// - `parts`: 组成 Hash field 的业务片段,每个片段内部不应包含 [`SEP`]。
pub fn field(parts: &[&str]) -> String {
    parts.join(SEP)
}

/// 业务作用：通用"空"判断：空数组 / null / 空对象 / 空串都算空（CacheLayer 与 GroupedCache 共用）。
/// matches! 宏：判断 s 是否匹配右边任一字面量，等价一串 ||，但更紧凑可读。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn is_empty_payload(s: &str) -> bool {
    matches!(s, "[]" | "null" | "{}" | "")
}

/// 业务作用：计算失效 `group` 时要 DEL 的目标集合：自身 + clear_map 里登记的关联组。
/// 返回集合始终包含源组自身，保证没有关联项时仍能完成本组清理。
///
/// # 参数
/// - `map`: 当前函数读取或更新的键值映射。
/// - `group`: 消费组、服务分组或任务分组名称。
fn cascade_targets(map: &HashMap<String, HashSet<String>>, group: &str) -> Vec<String> {
    // 用 HashSet 收集去重：自身和关联组可能重叠，避免 DEL 重复 key
    let mut set: HashSet<String> = HashSet::new();
    // 源组必须进入集合，否则只有关联组会被清理而源组继续暴露旧值。
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
