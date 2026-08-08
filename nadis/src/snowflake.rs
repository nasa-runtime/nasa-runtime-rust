//! 雪花 ID 的 Redis workerId 分配适配。
//!
//! 纯本地算法在 `nabase::id` 中,本模块只保留基于 Redis ZSET 的 workerId 领取和归还。
//!
//! ## ID 位布局(从高到低,对照 原实现)
//! ```text
//! [ timestamp(64-shift) ][ workerId(worker_id_bits) ][ seq(seq_bits) ]   shift = worker_id_bits + seq_bits
//! id = (last_ts << shift) | (worker_id << seq_bits) | seq
//! ```
//! 默认 `worker_id_bits=6, seq_bits=6, base_time=1704038400000`(2024-01-01 UTC)→ 64 实例 / 64 ID 每毫秒。
//!
//! ## 与 原实现 的有意差异
//! - workerId 分配:原实现 用 pipeline `ZRANGE 0,0` + `ZREMRANGEBYRANK 0,0`(**非原子**,双节点可能取同号);
//!   本实现用 **Lua 原子脚本**(单次执行,杜绝竞态;nadis 无 by-rank ZSET 命令,正好走 `eval`)。
//! - 停机归还:原实现 `Graceful.registry` ambient 钩子 → Rust **显式 `lease.release().await`**(`Drop` 不能 async)。
//! - `@EnableSnowflake`/`@Bean`/`@ConfigurationProperties` → 显式 builder + [`SnowflakeConfig`]。
//!
//! ## ⚠ 池耗尽语义(同 原实现)
//! ZSET 池为空时(**首启 OR 所有 workerId 都被占用**)脚本会**重新初始化 `[0, 2^bits-1]`**——无法区分这两种情况。
//! 故若同时运行的实例数 **超过 `2^worker_id_bits`**,会重新发出 `0` → **workerId 撞号**。这是该方案固有上限
//! (只有 `2^bits` 个不同 worker),原实现 原样如此,保真照搬。

use crate::RedisClient;
pub use nabase::id::{Snowflake, SnowflakeError};
use serde::Deserialize;
use std::sync::Arc;

// ==================== Redis ZSET 分配 workerId(Lua 原子)====================

/// 原子分配 + 首启初始化的 Lua 脚本:取最小 workerId、(池空则初始化 `[0, maxWorkers-1]`)、摘除、返回。
const ALLOC_SCRIPT: &str = r#"
local v = redis.call('ZRANGE', KEYS[1], 0, 0)
if #v == 0 then
  for i = 0, tonumber(ARGV[1]) - 1 do redis.call('ZADD', KEYS[1], i, i) end
  v = redis.call('ZRANGE', KEYS[1], 0, 0)
end
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, 0)
return tonumber(v[1])
"#;

/// 业务作用：从 Redis ZSET 池**原子**分配一个 workerId(对照 原实现 `worker()`,池空自动初始化 `[0, 2^bits-1]`)。
///
/// 比 原实现 的 pipeline 更稳:Lua 单次执行,杜绝双节点取同号。仍 1 RTT。
///
/// # 参数
/// - `client`: 执行 workerId 池 Lua 的 Redis 客户端。
/// - `key`: workerId 池 ZSET key。
/// - `worker_id_bits`: workerId 位数,决定池容量 `2^bits`。
pub async fn alloc_worker_id(
    client: &RedisClient,
    key: &str,
    worker_id_bits: u32,
) -> Result<i64, SnowflakeError> {
    if !(1..=15).contains(&worker_id_bits) {
        return Err(SnowflakeError::BitRange(format!(
            "worker_id_bits must be [1, 15], got {worker_id_bits}"
        )));
    }
    let max_workers = 1i64 << worker_id_bits;
    let arg = max_workers.to_string();
    client
        .eval::<i64>(ALLOC_SCRIPT, &[key], &[arg.as_str()])
        .await
        .map_err(|e| SnowflakeError::External(e.to_string()))
}

/// workerId 租约:持有已分配的 workerId,停机时显式归还到池。
pub struct WorkerIdLease {
    client: Arc<RedisClient>,
    key: String,
    worker_id: i64,
}

impl WorkerIdLease {
    /// 业务作用：当前持有的 workerId。
    pub fn worker_id(&self) -> i64 {
        self.worker_id
    }

    /// 业务作用：归还 workerId 到池(对照 原实现 `Graceful` 的 `zAdd(key, worker, worker)`)。
    /// **调用方在优雅停机序列里 `.await`**(`Drop` 不能 async,不在 Drop 自动归还)。
    pub async fn release(&self) -> Result<(), SnowflakeError> {
        self.client
            .z_add(&self.key, self.worker_id as f64, self.worker_id)
            .await
            .map(|_| ())
            .map_err(|e| SnowflakeError::External(e.to_string()))
    }
}

// ==================== 配置 + builder(替 原框架 自动配置)====================

/// 雪花配置(对照 原实现 `nasa.snowflake`;serde 反序列化,不进容器)。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SnowflakeConfig {
    /// Redis 源 qualifier(选 client 用;**由调用方据此挑 `Arc<RedisClient>`** 传入 builder,本结构仅存值)。
    pub qualifier: String,
    /// workerId 池的 ZSET key。
    pub key: String,
    /// 基础时间戳(epoch ms)。
    pub base_time: i64,
    /// 机器码位长。
    pub worker_id_bits: u32,
    /// 序列号位长。
    pub seq_bits: u32,
}

impl Default for SnowflakeConfig {
    /// 业务作用：返回 Redis workerId 分配的默认配置。
    fn default() -> Self {
        Self {
            qualifier: String::new(),
            key: "SNOWFLAKE-WORKERS".to_string(),
            base_time: 1_704_038_400_000,
            worker_id_bits: 6,
            seq_bits: 6,
        }
    }
}

impl SnowflakeConfig {
    /// 业务作用：单节点 / 无 redis:workerId = 1(对照 原实现 `Objects.isNull(redisProxy) ? 1 : worker(...)`)。
    pub fn build_local(&self) -> Result<Snowflake, SnowflakeError> {
        Snowflake::new(1, self.base_time, self.worker_id_bits, self.seq_bits)
    }

    /// 业务作用：分布式:从 redis 池原子分配 workerId,返回(生成器, 归还租约)。
    /// 调用方:启动时拿 `(sf, lease)`,停机时 `lease.release().await`。
    ///
    /// # 参数
    /// - `client`: 用于分配和归还 workerId 的 Redis 客户端。
    pub async fn build_with_redis(
        &self,
        client: Arc<RedisClient>,
    ) -> Result<(Snowflake, WorkerIdLease), SnowflakeError> {
        // 先校验位长(避免给 redis 初始化坏池),再分配。
        nabase::id::validate_bits(self.worker_id_bits, self.seq_bits)?;
        let worker_id = alloc_worker_id(&client, &self.key, self.worker_id_bits).await?;
        let sf = Snowflake::new(
            worker_id,
            self.base_time,
            self.worker_id_bits,
            self.seq_bits,
        )?;
        let lease = WorkerIdLease {
            client,
            key: self.key.clone(),
            worker_id,
        };
        Ok((sf, lease))
    }
}
