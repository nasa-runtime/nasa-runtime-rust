//! Snowflake workerId 的显式分配边界。
//!
//! 自动初始化和回收的池入口不再分配编号。受管或独立调用应使用显式初始化的非复用账本；
//! 业务领取不会创建缺失账本。账本丢失、回滚或回收编号都可能破坏唯一性，部署必须保证
//! 分配记录不会回退，并由管理方证明首次初始化覆盖的 ID 空间未被使用。
//! namespace incarnation 不进入 ID 位布局，不能用更换名称证明新的 ID 空间互不重叠。

use crate::RedisClient;
pub use nabase::id::{Snowflake, SnowflakeError};
use serde::Deserialize;
use std::sync::Arc;

/// 业务作用：拒绝没有初始化权威和非复用账本的自动 workerId 领取。
/// 参数说明：`client` 为 Redis 客户端；`key` 为池名；`worker_id_bits` 为旧入口的位宽。
/// 返回：始终返回明确错误，不读写旧池；调用方须迁移到显式 namespace 入口。
pub async fn alloc_worker_id(
    _client: &RedisClient,
    _key: &str,
    _worker_id_bits: u32,
) -> Result<i64, SnowflakeError> {
    Err(SnowflakeError::External("automatic worker allocation is disabled; an explicitly initialized non-reusing namespace is required".into()))
}

/// 已消耗的 workerId 领取凭证；编号永久保留，释放不会使它再次可分配。
pub struct WorkerIdLease {
    worker_id: i64,
}

impl WorkerIdLease {
    /// 业务作用：读取本次领取的 workerId。
    /// 参数说明：无。
    /// 返回：只属于本次领取的编号。
    pub fn worker_id(&self) -> i64 {
        self.worker_id
    }

    /// 业务作用：结束调用方对领取凭证的使用，不回收已消耗的编号。
    /// 参数说明：无。
    /// 返回：幂等成功；不会写 Redis 或允许其它生成器复用 workerId。
    pub async fn release(&self) -> Result<(), SnowflakeError> {
        Ok(())
    }
}

// ==================== 显式配置与 builder ====================

/// Snowflake 位布局和 Redis 源配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SnowflakeConfig {
    /// Redis 源 qualifier(选 client 用;**由调用方据此挑 `Arc<RedisClient>`** 传入 builder,本结构仅存值)。
    pub qualifier: String,
    /// 分配命名空间前缀；非复用账本使用独立的派生 key。
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
    /// 业务作用：为已由调用方独占 ID 空间的进程创建固定 workerId 为 1 的生成器。
    /// 参数说明：无。
    /// 返回：位布局合法时返回生成器；同一 ID 空间只能保有一个共享实例。
    /// 重启仍须证明超过上一实例的逻辑时间上界；仅等待经验间隔不保证安全。
    pub fn build_local(&self) -> Result<Snowflake, SnowflakeError> {
        Snowflake::new(1, self.base_time, self.worker_id_bits, self.seq_bits)
    }

    /// 业务作用：拒绝无显式初始化权威的 Redis 自动分配。
    /// 参数说明：`client` 为 Redis 客户端。
    /// 返回：始终返回错误；不创建池、不分配或回收编号。
    pub async fn build_with_redis(
        &self,
        client: Arc<RedisClient>,
    ) -> Result<(Snowflake, WorkerIdLease), SnowflakeError> {
        alloc_worker_id(&client, &self.key, self.worker_id_bits).await?;
        unreachable!("automatic allocation rejects all requests")
    }
}

/// 明确绑定位布局和 workerId 范围的非复用 namespace。
/// 初始化只能来自独立管理动作，业务配置不得触发自动初始化。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdNamespace {
    /// 管理方为已确认的新账本指定的不可复用身份。
    pub incarnation: String,
    /// 已确认不与其它分配范围重叠的首个 workerId。
    pub first_worker_id: i64,
    /// 已确认不与其它分配范围重叠的最后一个 workerId，包含此值。
    pub last_worker_id: i64,
}

const INITIALIZE_NAMESPACE: &str = r#"
if redis.call('EXISTS', KEYS[1]) ~= 0 then
    return redis.error_reply('worker namespace already exists')
end
redis.call('HSET', KEYS[1], 'incarnation', ARGV[1], 'config', ARGV[2], 'next', ARGV[3], 'last', ARGV[4])
return 1
"#;

const ALLOCATE_NAMESPACE: &str = r#"
local meta = redis.call('HMGET', KEYS[1], 'incarnation', 'config', 'next', 'last')
if meta[1] ~= ARGV[1] or meta[2] ~= ARGV[2] or not meta[3] or not meta[4] then
    return redis.error_reply('worker namespace missing or configuration mismatch')
end
local next = tonumber(meta[3])
local last = tonumber(meta[4])
if not next or not last or next ~= math.floor(next) or last ~= math.floor(last)
    or next < tonumber(ARGV[3]) or last ~= tonumber(ARGV[4]) or next > last then
    return redis.error_reply('worker namespace exhausted or invalid')
end
redis.call('HSET', KEYS[1], 'next', next + 1)
return next
"#;

impl WorkerIdNamespace {
    /// 业务作用：校验位布局及分配范围，形成与账本逐字比较的配置身份。
    /// 参数说明：`config` 为生成器配置。
    /// 返回：校验通过的规范配置串；非法范围或身份返回错误。
    fn identity(&self, config: &SnowflakeConfig) -> Result<String, SnowflakeError> {
        nabase::id::validate_bits(config.worker_id_bits, config.seq_bits)?;
        if self.incarnation.is_empty()
            || self.incarnation.len() > 128
            || config.key.is_empty()
            || self.first_worker_id < 0
            || self.last_worker_id < self.first_worker_id
            || self.last_worker_id >= (1i64 << config.worker_id_bits)
        {
            return Err(SnowflakeError::External(
                "invalid worker namespace identity or range".into(),
            ));
        }
        // 创建前校验生成器，避免无效位布局或 epoch 消耗编号。
        Snowflake::new(
            self.first_worker_id,
            config.base_time,
            config.worker_id_bits,
            config.seq_bits,
        )?;
        Ok(format!(
            "snowflake:{}:{}:{}:{}:{}",
            config.base_time,
            config.worker_id_bits,
            config.seq_bits,
            self.first_worker_id,
            self.last_worker_id
        ))
    }

    /// 业务作用：由管理方显式创建此前未使用的非复用编号账本。
    /// 参数说明：`client` 为具备管理权限的 Redis 客户端；`config` 为已批准的 ID 位布局和源。
    /// 返回：账本不存在且参数有效时创建；已存在时拒绝，不支持覆盖或重置。
    /// 调用前管理方必须通过账本之外的历史权威证明该 ID 空间未使用，禁止把 Redis key 缺失当作证明。
    /// Redis 持久化和故障转移必须保证领取记录不回退；本接口不能检测整个 Redis 历史被替换。
    pub async fn initialize(
        &self,
        client: &RedisClient,
        config: &SnowflakeConfig,
    ) -> Result<(), SnowflakeError> {
        let identity = self.identity(config)?;
        let key = format!("{}:non-reusing-workers", config.key);
        let first = self.first_worker_id.to_string();
        let last = self.last_worker_id.to_string();
        client
            .eval::<i64>(
                INITIALIZE_NAMESPACE,
                &[&key],
                &[&self.incarnation, &identity, &first, &last],
            )
            .await
            .map_err(|error| SnowflakeError::External(error.to_string()))?;
        Ok(())
    }

    /// 业务作用：从匹配的现有账本永久消耗一个编号，建立本次进程生命周期的生成器。
    /// 参数说明：`client` 为受管或独立 Redis 客户端；`config` 为预期配置。
    /// 返回：成功时返回生成器和不回收凭证；账本缺失、配置不符或耗尽时拒绝。
    /// 领取结果不确定时允许损失编号，不能根据超时自动归还；任何后续重试都消耗新的编号。
    pub async fn allocate(
        &self,
        client: &RedisClient,
        config: &SnowflakeConfig,
    ) -> Result<(Snowflake, WorkerIdLease), SnowflakeError> {
        let identity = self.identity(config)?;
        let key = format!("{}:non-reusing-workers", config.key);
        let first = self.first_worker_id.to_string();
        let last = self.last_worker_id.to_string();
        let worker_id = client
            .eval::<i64>(
                ALLOCATE_NAMESPACE,
                &[&key],
                &[&self.incarnation, &identity, &first, &last],
            )
            .await
            .map_err(|error| SnowflakeError::External(error.to_string()))?;
        Ok((
            Snowflake::new(
                worker_id,
                config.base_time,
                config.worker_id_bits,
                config.seq_bits,
            )?,
            WorkerIdLease { worker_id },
        ))
    }
}

/// 可撤销的生成器句柄；关闭与发号在同一互斥区裁决，旧句柄不能继续发号。
pub struct ManagedSnowflake {
    generator: std::sync::Mutex<Option<Snowflake>>,
    worker_id: i64,
}

impl ManagedSnowflake {
    /// 业务作用：从现有非复用账本创建具有唯一 owner 的生成器。
    /// 参数说明：`client` 为 Redis 源；`config` 为生成器配置；`namespace` 为预期管理身份。
    /// 返回：领取成功后返回受管生成器；失败不开放发号。
    pub async fn allocate(
        client: &RedisClient,
        config: &SnowflakeConfig,
        namespace: &WorkerIdNamespace,
    ) -> Result<Self, SnowflakeError> {
        let (generator, lease) = namespace.allocate(client, config).await?;
        Ok(Self {
            generator: std::sync::Mutex::new(Some(generator)),
            worker_id: lease.worker_id(),
        })
    }

    /// 业务作用：在当前 owner 尚开放时生成一个 ID。
    /// 参数说明：无。
    /// 返回：成功返回 ID；已经关闭或同步状态损坏时拒绝。
    pub fn generate(&self) -> Result<i64, SnowflakeError> {
        let guard = self
            .generator
            .lock()
            .map_err(|_| SnowflakeError::External("generator state unavailable".into()))?;
        let generator = guard
            .as_ref()
            .ok_or_else(|| SnowflakeError::External("generator is closed".into()))?;
        Ok(generator.generate())
    }

    /// 业务作用：永久关闭当前 owner 的发号入口。
    /// 参数说明：无。
    /// 返回：已进入生成临界区的调用完成后关闭；编号永久保留，不归还账本。
    pub fn close(&self) {
        self.generator
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    /// 业务作用：读取该 owner 已消耗的 workerId，用于有界运行诊断。
    /// 参数说明：无。
    /// 返回：不随关闭改变的编号。
    pub fn worker_id(&self) -> i64 {
        self.worker_id
    }
}
