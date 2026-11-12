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

/// 业务作用：拒绝缺少显式初始化权威和非复用账本的旧自动领取入口。
///
/// 参数说明：
/// - `_client`：旧入口传入的 Redis 客户端，此入口不使用或访问它。
/// - `_key`：旧池名，保留调用签名但不读取、创建或变更该池。
/// - `_worker_id_bits`：旧入口位宽，不据此分配编号。
///
/// 返回：始终返回固定外部错误，要求调用方使用显式 namespace；不产生 Redis 副作用。
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
    /// 业务作用：读取本次永久消耗的 workerId，供构造生成器或记录领取身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回领取凭证保存的编号，不查询 Redis，也不表示编号可以回收。
    pub fn worker_id(&self) -> i64 {
        self.worker_id
    }

    /// 业务作用：结束调用方对领取凭证的使用约定，同时保留编号永久不可复用的约束。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：幂等成功且不写 Redis；不撤销已创建生成器的发号能力，不允许其它领取重新使用该编号。
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
    /// 业务作用：提供独立 Snowflake 配置的默认 Redis 源选择和 ID 位布局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回空 qualifier、SNOWFLAKE-WORKERS 前缀、固定基础时间戳及各六位的 worker/序列布局；不创建账本或生成器。
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
    /// 业务作用：为调用方已独占的 ID 空间创建 workerId 固定为 1 的本地生成器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：位布局与相对时间容量合法时返回独立生成器，否则返回错误；同一 ID 空间必须只使用一个共享实例，本方法不验证外部独占权或持久化时间上界。
    /// 重启前须证明已超过上一实例逻辑时间，不能仅用经验等待间隔代替该证据。
    pub fn build_local(&self) -> Result<Snowflake, SnowflakeError> {
        Snowflake::new(1, self.base_time, self.worker_id_bits, self.seq_bits)
    }

    /// 业务作用：拒绝不携带显式 namespace 的 Redis 自动生成器构造。
    ///
    /// 参数说明：
    /// - `client`：旧构造入口传入的 Redis 源，只传给拒绝分配的旧入口，不发送命令。
    ///
    /// 返回：始终传播自动领取被禁用的错误，不建立生成器，也不创建或回收编号。
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
    /// 业务作用：核对位布局、账本身份与允许范围，形成供 Lua 与现有账本比较的配置串。
    ///
    /// 参数说明：
    /// - `config`：期望使用的 key 前缀、基础时间戳和 worker/序列位宽。
    ///
    /// 返回：位布局、incarnation 长度、编号范围及当前相对时间容量有效时返回配置串；非法参数返回错误，不访问 Redis。
    /// 不证明 ID 空间从未使用，不验证 Redis 持久性或重启前的逻辑时间上界。
    fn identity(&self, config: &SnowflakeConfig) -> Result<String, SnowflakeError> {
        nabase::id::validate_bits(config.worker_id_bits, config.seq_bits)?;
        // 身份或范围不满足本地约束时必须在 Redis 副作用前拒绝，不能靠分配脚本纠正调用配置。
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
        // 先确认位布局内可构造该编号的生成器，避免已知的本地参数错误消耗账本编号。
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

    /// 业务作用：执行管理方已授权的首次账本创建，禁止覆盖现有编号历史。
    ///
    /// 参数说明：
    /// - `client`：具备账本管理权限的 Redis 客户端。
    /// - `config`：管理方批准的 key 前缀、纪元和位布局。
    ///
    /// 返回：本地校验后调用原子 Lua，账本存在则由脚本拒绝，缺失时写入身份、范围及下一个编号；仅脚本约定的 Integer 1 表示成功。
    /// 命令错误向上传播，回复不符返回固定错误；回复异常也不能证明远端写入未发生。
    /// 调用方须在账本之外证明 ID 空间未使用；超时或取消不能证明账本未创建，不能直接重置重试。
    /// Redis 持久化和故障转移必须保证记录不回退；本接口不能识别整个 Redis 历史被替换。
    pub async fn initialize(
        &self,
        client: &RedisClient,
        config: &SnowflakeConfig,
    ) -> Result<(), SnowflakeError> {
        let identity = self.identity(config)?;
        let key = format!("{}:non-reusing-workers", config.key);
        let first = self.first_worker_id.to_string();
        let last = self.last_worker_id.to_string();
        // 只有显式管理动作到达此处；脚本原子地拒绝现存账本，禁止覆盖已消耗的编号历史。
        let reply = client
            .eval::<redis::Value>(
                INITIALIZE_NAMESPACE,
                &[&key],
                &[&self.incarnation, &identity, &first, &last],
            )
            .await
            .map_err(|error| SnowflakeError::External(error.to_string()))?;
        // 只有约定的成功标记才能确认首次创建，通用整数转换不能代替脚本完成证据。
        if !matches!(reply, redis::Value::Int(1)) {
            return Err(SnowflakeError::External(
                "worker namespace initialization reply is invalid".into(),
            ));
        }
        Ok(())
    }

    /// 业务作用：从预期的已初始化账本永久消耗一个编号，并据此建立本地生成器。
    ///
    /// 参数说明：
    /// - `client`：执行原子分配脚本的 Redis 客户端。
    /// - `config`：需要与账本一致的 key 前缀、纪元和位布局。
    ///
    /// 返回：脚本校验身份、配置和范围后推进 next，仅收到位于当前 namespace 范围内的 Integer 编号时构造生成器及不回收凭证。
    /// 本地校验或脚本错误向上传播，回复类型或编号范围不符返回固定错误，不开放发号入口。
    /// 结果未知时允许损失编号，不能根据超时或取消归还；后续重试可能再次消耗新编号，不能推断前次未执行。
    pub async fn allocate(
        &self,
        client: &RedisClient,
        config: &SnowflakeConfig,
    ) -> Result<(Snowflake, WorkerIdLease), SnowflakeError> {
        let identity = self.identity(config)?;
        let key = format!("{}:non-reusing-workers", config.key);
        let first = self.first_worker_id.to_string();
        let last = self.last_worker_id.to_string();
        // 脚本先核对账本身份再永久推进 next；结果未知也不补偿归还，以免并发领取复用编号。
        let reply = client
            .eval::<redis::Value>(
                ALLOCATE_NAMESPACE,
                &[&key],
                &[&self.incarnation, &identity, &first, &last],
            )
            .await
            .map_err(|error| SnowflakeError::External(error.to_string()))?;
        // 位宽合法不代表属于本次分配范围；在发布生成器前复验来源账本允许使用的编号。
        let worker_id = match reply {
            redis::Value::Int(worker_id)
                if (self.first_worker_id..=self.last_worker_id).contains(&worker_id) =>
            {
                worker_id
            }
            _ => {
                return Err(SnowflakeError::External(
                    "worker namespace allocation reply is invalid".into(),
                ));
            }
        };
        Ok((
            Snowflake::new(
                worker_id,
                config.base_time,
                config.worker_id_bits,
                config.seq_bits,
            )?,
            WorkerIdLease {
                worker_id,
            },
        ))
    }
}

/// 可撤销的生成器句柄；关闭与发号在同一互斥区裁决，旧句柄不能继续发号。
pub struct ManagedSnowflake {
    generator: std::sync::Mutex<Option<Snowflake>>,
    worker_id: i64,
}

impl ManagedSnowflake {
    /// 业务作用：将一次非复用编号领取与可关闭的发号入口绑定为受管生成器。
    ///
    /// 参数说明：
    /// - `client`：执行编号领取的 Redis 源。
    /// - `config`：生成器的 key 前缀、纪元和位布局。
    /// - `namespace`：预期已由管理方初始化且不会回退的账本身份与编号范围。
    ///
    /// 返回：领取及构造成功后返回受管句柄；失败不开放发号，已消耗或结果未知的编号不自动补偿或回收。
    pub async fn allocate(
        client: &RedisClient,
        config: &SnowflakeConfig,
        namespace: &WorkerIdNamespace,
    ) -> Result<Self, SnowflakeError> {
        let (generator, lease) = namespace.allocate(client, config).await?;
        // 领取与本地构造均已完成，才开放可关闭的发号句柄；不会发布半完成实例。
        Ok(Self {
            generator: std::sync::Mutex::new(Some(generator)),
            worker_id: lease.worker_id(),
        })
    }

    /// 业务作用：在关闭门禁允许时生成一个 ID，并与 close 在同一临界区裁决。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未关闭、状态锁完整且时间容量足够时返回非负 ID；已关闭、锁中毒或时间耗尽返回错误，不交付编号。
    pub fn generate(&self) -> Result<i64, SnowflakeError> {
        // 关闭与发号使用同一把锁，取得许可后的发号必须在释放临界区之前完成。
        let guard = self
            .generator
            .lock()
            .map_err(|_| SnowflakeError::External("generator state unavailable".into()))?;
        let generator = guard
            .as_ref()
            .ok_or_else(|| SnowflakeError::External("generator is closed".into()))?;
        generator.try_generate()
    }

    /// 业务作用：永久关闭当前受管实例的发号入口，使仍持有该句柄的调用方不能继续生成。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：等待已进入发号临界区的调用结束后移除生成器，重复关闭无额外效果；即使外层锁中毒也撤销入口，不归还编号。
    pub fn close(&self) {
        // 即使已有调用导致锁中毒，也必须取走生成器；清除入口不能清除永久消耗的编号记录。
        self.generator
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    /// 业务作用：提供受管实例已永久消耗的 workerId，供关联该实例的运行状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回创建时保存的编号，关闭后保持不变；不证明当前仍可发号，也不使编号可重新领取。
    pub fn worker_id(&self) -> i64 {
        self.worker_id
    }
}
