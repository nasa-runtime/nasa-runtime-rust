use crate::{
    JsonMapperCacheCodec, MapperCacheCodec, MapperCacheLoad, MapperCacheLoader, MapperL2Cache,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

#[cfg(feature = "redis-cache")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "redis-cache")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAPPER_CODEC_MAGIC: &[u8] = b"namapper:codec:";
const MAPPER_CODEC_V1_PREFIX: &[u8] = b"namapper:codec:v1:";

/// 给任意二级缓存增加进程内 single-flight 防击穿能力。
///
/// 相同进程内，同一 `(key, hash_key)` 的并发 miss 只允许一个调用执行 loader；跨进程互斥由
/// `RedisDistributedSingleFlightMapperL2Cache` 或业务缓存网关提供。
pub struct SingleFlightMapperL2Cache {
    inner: Arc<dyn MapperL2Cache>,
    locks: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// 业务作用：在 single-flight 调用离场时回收没有等待者的 key 锁，限制锁表随业务 key 增长。
struct SingleFlightLockCleanup<'a> {
    locks: &'a StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    lock_key: String,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl Drop for SingleFlightLockCleanup<'_> {
    /// 业务作用: 在最后一个等待者离开后回收 single-flight 锁表项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 无；锁表中仍有等待者时保留条目，最后一个引用离开时删除条目。
    fn drop(&mut self) {
        let Ok(mut locks) = self.locks.lock() else {
            tracing::error!(
                component = "mapper",
                event = "single_flight_cleanup_error",
                "mapper single-flight lock table poisoned during cleanup"
            );
            return;
        };
        if let Some(lock) = self.lock.take() {
            let same = locks
                .get(&self.lock_key)
                .is_some_and(|current| Arc::ptr_eq(current, &lock));
            drop(lock);
            if same
                && locks
                    .get(&self.lock_key)
                    .is_some_and(|current| Arc::strong_count(current) == 1)
            {
                locks.remove(&self.lock_key);
            }
        }
    }
}

impl SingleFlightMapperL2Cache {
    /// 业务作用: 包装已有二级缓存，在数据库加载路径增加进程内同 key 合并。
    ///
    /// # 参数
    /// - `inner`: 真实执行缓存读写的 L2 cache。
    ///
    /// 返回: 共享底层缓存的新 single-flight 包装器。
    pub fn new(inner: Arc<dyn MapperL2Cache>) -> Self {
        Self {
            inner,
            locks: StdMutex::new(HashMap::new()),
        }
    }

    /// 业务作用: 返回被包装的缓存，供启动诊断或继续组合缓存层。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 底层缓存的共享引用。
    pub fn inner(&self) -> Arc<dyn MapperL2Cache> {
        self.inner.clone()
    }
}

#[crate::async_trait]
impl MapperL2Cache for SingleFlightMapperL2Cache {
    /// 业务作用: 透传单条缓存读取。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    ///
    /// 返回: 底层缓存的命中值、未命中状态或读取错误。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key, hash_key).await
    }

    /// 业务作用: 透传单条缓存写入。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `value`: 已编码的查询结果。
    /// - `ttl_ms`: 可选毫秒级 TTL。
    ///
    /// 返回: 底层缓存确认写入时成功，否则返回实现错误。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        self.inner.put(key, hash_key, value, ttl_ms).await
    }

    /// 业务作用: 透传单条缓存删除。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 需要删除的查询字段。
    ///
    /// 返回: 底层缓存确认删除时成功。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        self.inner.evict(key, hash_key).await
    }

    /// 业务作用: 透传整个 Mapper namespace 清理。
    ///
    /// # 参数
    /// - `key`: 需要整体失效的 Mapper cache namespace。
    ///
    /// 返回: 底层缓存确认清理时成功。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        self.inner.clear_key(key).await
    }

    /// 业务作用: 合并同进程内相同 key 的并发 cache miss，避免重复查询数据库。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `ttl_ms`: loader 结果写回时采用的 TTL。
    /// - `loader`: 只有当前进程首个 miss 调用会执行的数据库加载回调。
    ///
    /// 返回: 命中或加载结果及来源；缓存和 loader 错误原样返回。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        // 热 key 命中时不进入锁表，避免给常规读取增加互斥开销。
        if let Some(bytes) = self.inner.get(key, hash_key).await? {
            return Ok(MapperCacheLoad::hit(bytes));
        }

        let lock_key = format!("{key}\0{hash_key}");
        let lock = {
            let mut locks = self
                .locks
                .lock()
                .map_err(|_| anyhow::anyhow!("mapper single-flight lock table poisoned"))?;
            locks
                .entry(lock_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let cleanup = SingleFlightLockCleanup {
            locks: &self.locks,
            lock_key,
            lock: Some(lock),
        };
        let guard = cleanup
            .lock
            .as_ref()
            .expect("registered flight")
            .lock()
            .await;
        // 等待期间可能已有先到请求完成写入，取得控制权后必须再次读取。
        let result = match self.inner.get(key, hash_key).await {
            Ok(Some(bytes)) => Ok(MapperCacheLoad::hit_after_wait(bytes)),
            Ok(None) => {
                let bytes = loader().await?;
                self.inner.put(key, hash_key, &bytes, ttl_ms).await?;
                Ok(MapperCacheLoad::loaded(bytes))
            }
            Err(error) => Err(error),
        };
        drop(guard);
        drop(cleanup);
        result
    }
}

/// 带稳定版本头的 Mapper cache value codec。
///
/// 写入格式为 `namapper:codec:v1:<codec_name>:<payload>`；读取时按名称选择当前或历史 codec，
/// 无版本头的数据可按历史 JSON 格式读取。
pub struct VersionedMapperCacheCodec {
    codec_name: String,
    codec: Arc<dyn MapperCacheCodec>,
    fallbacks: Vec<(String, Arc<dyn MapperCacheCodec>)>,
    read_legacy_json: bool,
}

impl VersionedMapperCacheCodec {
    /// 业务作用: 创建写入稳定版本头的 cache value codec。
    ///
    /// # 参数
    /// - `codec_name`: 当前 codec 的稳定名称。
    /// - `codec`: 负责编解码 payload 的主 codec。
    ///
    /// 返回: 名称合法时返回 codec；非法名称返回错误。
    pub fn new(
        codec_name: impl Into<String>,
        codec: Arc<dyn MapperCacheCodec>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            codec_name: validate_mapper_codec_name(codec_name.into())?,
            codec,
            fallbacks: Vec::new(),
            read_legacy_json: true,
        })
    }

    /// 业务作用: 注册一个具名历史 codec，用于平滑读取旧缓存。
    ///
    /// # 参数
    /// - `codec_name`: 历史缓存头中的 codec 名称。
    /// - `codec`: 能够解码该历史 payload 的 codec。
    ///
    /// 返回: 名称合法时返回更新后的组合；非法名称返回错误。
    pub fn with_named_fallback(
        mut self,
        codec_name: impl Into<String>,
        codec: Arc<dyn MapperCacheCodec>,
    ) -> anyhow::Result<Self> {
        self.fallbacks
            .push((validate_mapper_codec_name(codec_name.into())?, codec));
        Ok(self)
    }

    /// 业务作用: 关闭无版本头 JSON value 的兼容读取。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 仅接受已知版本头的 codec 组合。
    pub fn without_legacy_json_fallback(mut self) -> Self {
        self.read_legacy_json = false;
        self
    }
}

impl MapperCacheCodec for VersionedMapperCacheCodec {
    /// 业务作用: 给主 codec payload 写入稳定版本头。
    ///
    /// # 参数
    /// - `value`: 待编码的查询结果 JSON value。
    ///
    /// 返回: 带版本头的缓存 bytes 或主 codec 错误。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        let payload = self.codec.encode_value(value)?;
        let mut bytes = Vec::with_capacity(
            MAPPER_CODEC_V1_PREFIX.len() + self.codec_name.len() + 1 + payload.len(),
        );
        bytes.extend_from_slice(MAPPER_CODEC_V1_PREFIX);
        bytes.extend_from_slice(self.codec_name.as_bytes());
        bytes.push(b':');
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    /// 业务作用: 按缓存版本头选择当前 codec、历史 codec 或无版本头 JSON 路径。
    ///
    /// # 参数
    /// - `bytes`: L2 cache 返回的原始 value。
    ///
    /// 返回: 解码后的 JSON value；未知版本、未知 codec 或 payload 非法时返回错误。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        if bytes.starts_with(MAPPER_CODEC_V1_PREFIX) {
            let rest = &bytes[MAPPER_CODEC_V1_PREFIX.len()..];
            let Some(name_end) = rest.iter().position(|byte| *byte == b':') else {
                anyhow::bail!("mapper cache codec v1 payload missing codec name separator");
            };
            let codec_name = std::str::from_utf8(&rest[..name_end])
                .map_err(|error| anyhow::anyhow!("mapper cache codec name is not utf8: {error}"))?;
            let payload = &rest[name_end + 1..];
            if codec_name == self.codec_name {
                return self.codec.decode_value(payload);
            }
            if let Some((_, codec)) = self.fallbacks.iter().find(|(name, _)| name == codec_name) {
                return codec.decode_value(payload);
            }
            anyhow::bail!("unknown mapper cache codec `{codec_name}`");
        }
        // 已带协议 magic 的未知格式不能退回 JSON，否则会把新协议误判为业务数据。
        if bytes.starts_with(MAPPER_CODEC_MAGIC) {
            anyhow::bail!("unknown mapper cache codec version");
        }
        if self.read_legacy_json {
            JsonMapperCacheCodec.decode_value(bytes)
        } else {
            self.codec.decode_value(bytes)
        }
    }

    /// 业务作用: 判断旧格式 value 是否需要按当前 codec 回写。
    ///
    /// # 参数
    /// - `bytes`: 已成功解码的缓存 value。
    ///
    /// 返回: 命中历史格式且当前 codec 可生成新格式时为 `true`。
    fn should_rewrite_value(&self, bytes: &[u8]) -> bool {
        if bytes.starts_with(MAPPER_CODEC_V1_PREFIX) {
            let rest = &bytes[MAPPER_CODEC_V1_PREFIX.len()..];
            let Some(name_end) = rest.iter().position(|byte| *byte == b':') else {
                return false;
            };
            return std::str::from_utf8(&rest[..name_end])
                .map(|codec_name| codec_name != self.codec_name)
                .unwrap_or(false);
        }
        self.read_legacy_json && !bytes.starts_with(MAPPER_CODEC_MAGIC)
    }
}

/// Mapper cache value 迁移用 fallback codec。
///
/// 写入始终使用 primary，读取失败时按声明顺序尝试历史 codec。
pub struct FallbackMapperCacheCodec {
    primary: Arc<dyn MapperCacheCodec>,
    fallbacks: Vec<Arc<dyn MapperCacheCodec>>,
}

impl FallbackMapperCacheCodec {
    /// 业务作用: 创建以指定 codec 为当前写入格式的 fallback 组合。
    ///
    /// # 参数
    /// - `primary`: 当前写入和首选读取使用的 codec。
    ///
    /// 返回: 尚未注册历史格式的 codec 组合。
    pub fn new(primary: Arc<dyn MapperCacheCodec>) -> Self {
        Self {
            primary,
            fallbacks: Vec::new(),
        }
    }

    /// 业务作用: 追加一个历史读取 codec。
    ///
    /// # 参数
    /// - `fallback`: 主 codec 读取失败后尝试的 codec。
    ///
    /// 返回: 更新后的 codec 组合。
    pub fn with_fallback(mut self, fallback: Arc<dyn MapperCacheCodec>) -> Self {
        self.fallbacks.push(fallback);
        self
    }

    /// 业务作用: 按尝试顺序批量追加历史读取 codec。
    ///
    /// # 参数
    /// - `fallbacks`: 历史 codec 集合。
    ///
    /// 返回: 更新后的 codec 组合。
    pub fn with_fallbacks<I>(mut self, fallbacks: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn MapperCacheCodec>>,
    {
        self.fallbacks.extend(fallbacks);
        self
    }
}

impl MapperCacheCodec for FallbackMapperCacheCodec {
    /// 业务作用: 始终使用 primary codec 编码新缓存值。
    ///
    /// # 参数
    /// - `value`: 待编码的查询结果 JSON value。
    ///
    /// 返回: primary codec 产生的 bytes 或编码错误。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        self.primary.encode_value(value)
    }

    /// 业务作用: 先用 primary 解码，失败后按顺序尝试历史 codec。
    ///
    /// # 参数
    /// - `bytes`: L2 cache 返回的原始 value。
    ///
    /// 返回: 首个成功解码的 JSON value；全部失败时返回聚合错误。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        let mut errors = Vec::new();
        match self.primary.decode_value(bytes) {
            Ok(value) => return Ok(value),
            Err(error) => errors.push(format!("primary={error}")),
        }
        for (index, fallback) in self.fallbacks.iter().enumerate() {
            match fallback.decode_value(bytes) {
                Ok(value) => return Ok(value),
                Err(error) => errors.push(format!("fallback[{index}]={error}")),
            }
        }
        anyhow::bail!("mapper cache codec decode failed: {}", errors.join("; "))
    }

    /// 业务作用: 判断当前 value 是否只能由历史 codec 读取并需要回写。
    ///
    /// # 参数
    /// - `bytes`: 已成功解码的缓存 value。
    ///
    /// 返回: primary 失败且任一 fallback 成功时为 `true`。
    fn should_rewrite_value(&self, bytes: &[u8]) -> bool {
        self.primary.decode_value(bytes).is_err()
            && self
                .fallbacks
                .iter()
                .any(|fallback| fallback.decode_value(bytes).is_ok())
    }
}

/// 业务作用: 校验 codec 名称能安全写入版本头。
///
/// # 参数
/// - `codec_name`: 业务传入的稳定名称。
///
/// 返回: 合法名称原样返回；空值或包含分隔符的名称返回错误。
fn validate_mapper_codec_name(codec_name: String) -> anyhow::Result<String> {
    if codec_name.is_empty() {
        anyhow::bail!("mapper cache codec name must not be empty");
    }
    if codec_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(codec_name)
    } else {
        anyhow::bail!(
            "mapper cache codec name `{codec_name}` may only contain ASCII letters, digits, '-', '_' or '.'"
        )
    }
}

#[cfg(feature = "redis-cache")]
const MAX_MAPPER_RUNTIME_MILLIS: u64 = 365 * 24 * 60 * 60 * 1_000;

/// 业务作用: 把非 fallible builder 输入收敛到 Tokio 与 Redis 接受的毫秒范围。
///
/// # 参数
/// - `value`: 业务传入的毫秒值。
///
/// 返回: 位于 1 毫秒至 365 天范围内的值。
#[cfg(feature = "redis-cache")]
fn bounded_mapper_millis(value: u64) -> u64 {
    value.clamp(1, MAX_MAPPER_RUNTIME_MILLIS)
}

/// 业务作用: 在写入 Redis 前校验 TTL，避免非法过期参数留下永久缓存字段。
///
/// # 参数
/// - `ttl_ms`: 待应用的毫秒级 TTL。
///
/// 返回: 有效范围内成功，否则返回参数错误。
#[cfg(feature = "redis-cache")]
fn validate_mapper_ttl(ttl_ms: u64) -> anyhow::Result<()> {
    if !(1..=MAX_MAPPER_RUNTIME_MILLIS).contains(&ttl_ms) {
        anyhow::bail!("mapper cache TTL must be within 1ms..=365 days");
    }
    Ok(())
}

/// Redis Hash 版 Mapper 二级缓存适配器。
///
/// Redis key 对应 Mapper namespace，Hash field 对应 SQL 与 bind 派生的查询字段。TTL 优先使用
/// Hash field 级过期；服务端不支持 `HPEXPIRE` 时退到整个 Hash key 的 `PEXPIRE`。
#[cfg(feature = "redis-cache")]
pub struct RedisMapperL2Cache<C = redis::cluster_async::ClusterConnection> {
    redis: C,
}

#[cfg(feature = "redis-cache")]
impl<C> RedisMapperL2Cache<C>
where
    C: redis::aio::ConnectionLike + Clone + Send + Sync + 'static,
{
    /// 业务作用: 创建 Redis Hash 版 Mapper 二级缓存。
    ///
    /// # 参数
    /// - `redis`: 已由独立调用方或宿主管理的 Redis 异步连接，不创建第二套连接池。
    ///
    /// 返回: 可共享使用的 Redis L2 cache。
    pub fn new(redis: C) -> Self {
        Self { redis }
    }

    /// 业务作用: 启动期确认 Redis 支持 Hash field 级 TTL。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: `HPEXPIRE` 与 `HPTTL` 语义完整时成功，否则返回能力错误。
    pub async fn assert_hash_field_ttl_supported(&self) -> anyhow::Result<()> {
        assert_redis_hash_field_ttl_supported(self.redis.clone()).await
    }
}

#[cfg(feature = "redis-cache")]
#[crate::async_trait]
impl<C> MapperL2Cache for RedisMapperL2Cache<C>
where
    C: redis::aio::ConnectionLike + Clone + Send + Sync + 'static,
{
    /// 业务作用: 从 Redis Hash 读取单条 Mapper 查询缓存。
    ///
    /// # 参数
    /// - `key`: Redis Hash key。
    /// - `hash_key`: Redis Hash field。
    ///
    /// 返回: 命中 bytes、未命中 `None` 或 Redis 错误。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        use redis::AsyncCommands;
        let mut conn = self.redis.clone();
        Ok(conn.hget(key, hash_key).await?)
    }

    /// 业务作用: 写入 Redis Hash field 并应用可选 TTL。
    ///
    /// # 参数
    /// - `key`: Redis Hash key。
    /// - `hash_key`: Redis Hash field。
    /// - `value`: 已编码的查询结果。
    /// - `ttl_ms`: 可选毫秒级 TTL。
    ///
    /// 返回: value 与过期策略均确认后成功；参数或 Redis 操作失败时返回错误。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        use redis::AsyncCommands;
        if let Some(ttl_ms) = ttl_ms {
            validate_mapper_ttl(ttl_ms)?;
        }
        let mut conn = self.redis.clone();
        if let Some(ttl_ms) = ttl_ms {
            // 值与过期在一个脚本内完成；TTL 失败时删除字段，避免取消或拒绝留下永久缓存。
            redis::Script::new(WRITE_WITH_TTL)
                .key(key)
                .arg(hash_key)
                .arg(value)
                .arg(ttl_ms)
                .invoke_async::<i64>(&mut conn)
                .await?;
        } else {
            conn.hset::<_, _, _, ()>(key, hash_key, value).await?;
        }
        Ok(())
    }

    /// 业务作用: 删除 Redis Hash 中的单条查询缓存字段。
    ///
    /// # 参数
    /// - `key`: Redis Hash key。
    /// - `hash_key`: 需要删除的 Hash field。
    ///
    /// 返回: Redis 确认执行删除时成功。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        use redis::AsyncCommands;
        let mut conn = self.redis.clone();
        conn.hdel::<_, _, ()>(key, hash_key).await?;
        Ok(())
    }

    /// 业务作用: 删除整个 Redis Hash，使 Mapper namespace 全量失效。
    ///
    /// # 参数
    /// - `key`: 需要删除的 Redis Hash key。
    ///
    /// 返回: Redis 确认执行删除时成功。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        use redis::AsyncCommands;
        let mut conn = self.redis.clone();
        conn.del::<_, ()>(key).await?;
        Ok(())
    }
}

/// 业务作用: 启动期探测 Redis Cluster 是否支持 Hash field 级 TTL。
///
/// # 参数
/// - `redis`: 用于执行隔离探测的 Redis Cluster 异步连接。
///
/// 返回: field TTL 生效且没有设置 key TTL 时成功；能力不完整或清理失败时返回错误。
#[cfg(feature = "redis-cache")]
pub async fn assert_redis_hash_field_ttl_supported(
    redis: impl redis::aio::ConnectionLike + Send,
) -> anyhow::Result<()> {
    let mut conn = redis;
    let probe_key = redis_hash_field_ttl_probe_key();
    // 探测和清理同属一个执行单元，调用方取消等待不影响服务端删除探测 key。
    redis::Script::new(FIELD_TTL_PROBE)
        .key(probe_key)
        .invoke_async::<i64>(&mut conn)
        .await?;
    Ok(())
}

#[cfg(feature = "redis-cache")]
const WRITE_WITH_TTL: &str = r#"
if not redis.acl_check_cmd('HSET', KEYS[1], ARGV[1], ARGV[2]) or
   not redis.acl_check_cmd('HDEL', KEYS[1], ARGV[1]) then
    return redis.error_reply('cache write and cleanup permissions required')
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local result = redis.pcall('HPEXPIRE', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
if result.err then
    local message = string.lower(result.err)
    if string.find(message, 'unknown command') or string.find(message, 'unsupported command') then
        local fallback = redis.pcall('PEXPIRE', KEYS[1], ARGV[3])
        if type(fallback) == 'number' and fallback == 1 then return 1 end
    end
    redis.call('HDEL', KEYS[1], ARGV[1])
    return redis.error_reply('cache expiration unavailable')
end
if result[1] ~= 1 then
    redis.call('HDEL', KEYS[1], ARGV[1])
    return redis.error_reply('cache expiration rejected')
end
return 1
"#;

#[cfg(feature = "redis-cache")]
const FIELD_TTL_PROBE: &str = r#"
if not redis.acl_check_cmd('HSET', KEYS[1], 'probe', '1') or not redis.acl_check_cmd('DEL', KEYS[1]) then
    return redis.error_reply('cache probe and cleanup permissions required')
end
redis.call('HSET', KEYS[1], 'probe', '1')
local result = redis.pcall('HPEXPIRE', KEYS[1], 5000, 'FIELDS', 1, 'probe')
local ttl = redis.pcall('HPTTL', KEYS[1], 'FIELDS', 1, 'probe')
redis.call('DEL', KEYS[1])
if result.err or ttl.err or result[1] ~= 1 or ttl[1] <= 0 or ttl[1] > 5000 then
    return redis.error_reply('hash field TTL required')
end
return 1
"#;

#[cfg(feature = "redis-cache")]
static REDIS_FIELD_TTL_PROBE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// 业务作用: 生成不会与并发进程冲突的 Redis field TTL 探测 key。
///
/// 参数说明: 无。
///
/// 返回: 含进程号、时刻与进程内序号的隔离 key。
#[cfg(feature = "redis-cache")]
fn redis_hash_field_ttl_probe_key() -> String {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = REDIS_FIELD_TTL_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "namapper:hash-field-ttl-probe:{}:{now_nanos}:{counter}",
        std::process::id()
    )
}

/// Redis 分布式 single-flight 包装器。
///
/// 同一 `(key, hash_key)` 的并发 miss 由 Redis 短锁选择一个 loader，其它进程只轮询底层 cache。
#[cfg(feature = "redis-cache")]
pub struct RedisDistributedSingleFlightMapperL2Cache {
    inner: Arc<dyn MapperL2Cache>,
    redis: redis::cluster_async::ClusterConnection,
    lock_ttl_ms: u64,
    wait_timeout_ms: u64,
    poll_interval_ms: u64,
}

#[cfg(feature = "redis-cache")]
impl RedisDistributedSingleFlightMapperL2Cache {
    /// 业务作用: 用 Redis 短锁给已有 L2 cache 增加跨进程 miss 合并。
    ///
    /// # 参数
    /// - `inner`: 真实执行缓存读写的 L2 cache。
    /// - `redis`: 用于锁操作的 Redis Cluster 连接。
    ///
    /// 返回: 采用保守默认超时的分布式 single-flight 包装器。
    pub fn new(
        inner: Arc<dyn MapperL2Cache>,
        redis: redis::cluster_async::ClusterConnection,
    ) -> Self {
        Self {
            inner,
            redis,
            lock_ttl_ms: 5_000,
            wait_timeout_ms: 5_000,
            poll_interval_ms: 25,
        }
    }

    /// 业务作用: 设置持锁进程失联后的自动释放时间。
    ///
    /// # 参数
    /// - `lock_ttl_ms`: Redis 锁的毫秒级 TTL。
    ///
    /// 返回: 将数值收敛到有效范围后的包装器。
    pub fn with_lock_ttl_ms(mut self, lock_ttl_ms: u64) -> Self {
        self.lock_ttl_ms = bounded_mapper_millis(lock_ttl_ms);
        self
    }

    /// 业务作用: 设置等待者单轮轮询的最长时间，超时后重新竞争锁。
    ///
    /// # 参数
    /// - `wait_timeout_ms`: 单轮等待的毫秒上限。
    ///
    /// 返回: 将数值收敛到有效范围后的包装器。
    pub fn with_wait_timeout_ms(mut self, wait_timeout_ms: u64) -> Self {
        self.wait_timeout_ms = bounded_mapper_millis(wait_timeout_ms);
        self
    }

    /// 业务作用: 设置等待者读取底层 cache 的轮询间隔。
    ///
    /// # 参数
    /// - `poll_interval_ms`: 轮询间隔毫秒值。
    ///
    /// 返回: 将数值收敛到有效范围后的包装器。
    pub fn with_poll_interval_ms(mut self, poll_interval_ms: u64) -> Self {
        self.poll_interval_ms = bounded_mapper_millis(poll_interval_ms);
        self
    }

    /// 业务作用: 返回被包装的缓存，供启动诊断或继续组合缓存层。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 底层缓存的共享引用。
    pub fn inner(&self) -> Arc<dyn MapperL2Cache> {
        self.inner.clone()
    }
}

#[cfg(feature = "redis-cache")]
#[crate::async_trait]
impl MapperL2Cache for RedisDistributedSingleFlightMapperL2Cache {
    /// 业务作用: 透传单条缓存读取。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    ///
    /// 返回: 底层缓存的命中值、未命中状态或读取错误。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key, hash_key).await
    }

    /// 业务作用: 透传单条缓存写入。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `value`: 已编码的查询结果。
    /// - `ttl_ms`: 可选毫秒级 TTL。
    ///
    /// 返回: 底层缓存确认写入时成功。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        self.inner.put(key, hash_key, value, ttl_ms).await
    }

    /// 业务作用: 透传单条缓存删除。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 需要删除的查询字段。
    ///
    /// 返回: 底层缓存确认删除时成功。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        self.inner.evict(key, hash_key).await
    }

    /// 业务作用: 透传整个 Mapper namespace 清理。
    ///
    /// # 参数
    /// - `key`: 需要整体失效的 Mapper cache namespace。
    ///
    /// 返回: 底层缓存确认清理时成功。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        self.inner.clear_key(key).await
    }

    /// 业务作用: 用 Redis 短锁合并跨进程相同 key 的并发 cache miss。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `ttl_ms`: loader 结果写回时采用的 TTL。
    /// - `loader`: 只有获得 Redis 锁的调用会执行的数据库加载回调。
    ///
    /// 返回: 命中或加载结果及来源；缓存、锁和 loader 错误原样返回。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        if let Some(bytes) = self.inner.get(key, hash_key).await? {
            return Ok(MapperCacheLoad::hit(bytes));
        }

        let mut loader = Some(loader);
        let lock_key = distributed_single_flight_lock_key(key, hash_key);
        let poll_interval = Duration::from_millis(self.poll_interval_ms);
        let wait_timeout = Duration::from_millis(self.wait_timeout_ms);
        loop {
            let token = distributed_single_flight_token();
            let mut conn = self.redis.clone();
            if redis_try_acquire_single_flight(&mut conn, &lock_key, &token, self.lock_ttl_ms)
                .await?
            {
                let result = async {
                    // 获得锁后复验缓存，避免刚完成写入的其它进程触发重复数据库查询。
                    if let Some(bytes) = self.inner.get(key, hash_key).await? {
                        return Ok(MapperCacheLoad::hit_after_wait(bytes));
                    }
                    let loader = loader.take().ok_or_else(|| {
                        anyhow::anyhow!(
                            "mapper distributed single-flight loader was already consumed"
                        )
                    })?;
                    let bytes = loader().await?;
                    self.inner.put(key, hash_key, &bytes, ttl_ms).await?;
                    Ok(MapperCacheLoad::loaded(bytes))
                }
                .await;
                // token 复验避免误删已由其它请求重新获得的锁。
                if let Err(error) = redis_release_single_flight(&mut conn, &lock_key, &token).await
                {
                    tracing::warn!(
                        component = "mapper",
                        event = "distributed_single_flight_release_error",
                        lock_key = %lock_key,
                        error = %error,
                        "mapper distributed single-flight release failed"
                    );
                }
                return result;
            }

            let wait_started = tokio::time::Instant::now();
            // 未持有锁的进程不访问数据库，只等待持锁方把结果写入共享缓存。
            while wait_started.elapsed() < wait_timeout {
                tokio::time::sleep(poll_interval).await;
                if let Some(bytes) = self.inner.get(key, hash_key).await? {
                    return Ok(MapperCacheLoad::hit_after_wait(bytes));
                }
            }
        }
    }
}

/// 业务作用: 从 Mapper namespace 与查询字段派生分布式 single-flight 锁 key。
///
/// # 参数
/// - `key`: Mapper cache namespace。
/// - `hash_key`: 单条查询缓存字段。
///
/// 返回: 不暴露原始参数的稳定摘要 key。
#[cfg(feature = "redis-cache")]
fn distributed_single_flight_lock_key(key: &str, hash_key: &str) -> String {
    let mut raw = String::with_capacity(key.len() + hash_key.len() + 1);
    raw.push_str(key);
    raw.push('\0');
    raw.push_str(hash_key);
    format!("namapper:singleflight:{}", sha256_hex(raw.as_bytes()))
}

#[cfg(feature = "redis-cache")]
static REDIS_SINGLE_FLIGHT_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

/// 业务作用: 生成用于安全释放 Redis 短锁的唯一持有者 token。
///
/// 参数说明: 无。
///
/// 返回: 含进程号、时刻与进程内序号的 token。
#[cfg(feature = "redis-cache")]
fn distributed_single_flight_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = REDIS_SINGLE_FLIGHT_TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}:{nanos}:{counter}", std::process::id())
}

/// 业务作用: 原子尝试获取带自动过期的 Redis single-flight 锁。
///
/// # 参数
/// - `conn`: 当前操作使用的 Redis Cluster 连接。
/// - `lock_key`: 摘要后的锁 key。
/// - `token`: 当前调用的持有者 token。
/// - `lock_ttl_ms`: 锁的自动过期毫秒值。
///
/// 返回: 成功获得锁时为 `true`，锁已存在时为 `false`，命令失败时返回 Redis 错误。
#[cfg(feature = "redis-cache")]
async fn redis_try_acquire_single_flight(
    conn: &mut redis::cluster_async::ClusterConnection,
    lock_key: &str,
    token: &str,
    lock_ttl_ms: u64,
) -> redis::RedisResult<bool> {
    let response = redis::cmd("SET")
        .arg(lock_key)
        .arg(token)
        .arg("NX")
        .arg("PX")
        .arg(lock_ttl_ms)
        .query_async::<Option<String>>(conn)
        .await?;
    Ok(response.is_some())
}

/// 业务作用: 仅在 token 仍属于当前调用时原子释放 Redis single-flight 锁。
///
/// # 参数
/// - `conn`: 当前操作使用的 Redis Cluster 连接。
/// - `lock_key`: 当前调用竞争的锁 key。
/// - `token`: 当前调用写入的持有者 token。
///
/// 返回: Lua 脚本执行成功时为 `Ok`；锁已过期或已换主不会删除新持有者的锁。
#[cfg(feature = "redis-cache")]
async fn redis_release_single_flight(
    conn: &mut redis::cluster_async::ClusterConnection,
    lock_key: &str,
    token: &str,
) -> redis::RedisResult<()> {
    let script = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"#;
    redis::cmd("EVAL")
        .arg(script)
        .arg(1)
        .arg(lock_key)
        .arg(token)
        .query_async::<i64>(conn)
        .await?;
    Ok(())
}

/// 业务作用: 计算稳定 SHA-256 十六进制摘要，用于避免锁 key 泄露完整 SQL 参数。
///
/// # 参数
/// - `bytes`: namespace 与查询字段组合后的原始 bytes。
///
/// 返回: 小写十六进制 SHA-256 摘要。
#[cfg(feature = "redis-cache")]
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}
