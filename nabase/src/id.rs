//! 本地 ID 生成工具。

use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// 雪花 ID 默认基础时间戳。
pub const DEFAULT_BASE_TIME: i64 = 1_704_038_400_000;
/// 雪花 ID 默认机器码位长。
pub const DEFAULT_WORKER_ID_BITS: u32 = 6;
/// 雪花 ID 默认序列号位长。
pub const DEFAULT_SEQ_BITS: u32 = 6;
/// 单节点默认 worker id。
pub const DEFAULT_WORKER_ID: i64 = 1;

/// ID 生成器抽象。
pub trait IdGenerate {
    /// 业务作用：生成下一个 ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前生成器分配的下一个业务 ID。
    fn next_id(&self) -> i64;
}

/// 雪花生成器错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnowflakeError {
    /// 位长越界。
    BitRange(String),
    /// worker id 越界。
    WorkerIdRange(String),
    /// 外部 worker id 分配或归还失败;纯本地生成器不会返回此项。
    External(String),
}

impl core::fmt::Display for SnowflakeError {
    /// 业务作用：格式化错误信息。
    ///
    /// 参数说明:
    /// - `f`: 标准格式化器，用于写入可读的错误文本。
    ///
    /// 返回: 错误文本成功写入时返回成功，否则透传格式化错误。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SnowflakeError::BitRange(s) => write!(f, "snowflake bit range: {s}"),
            SnowflakeError::WorkerIdRange(s) => write!(f, "snowflake worker id range: {s}"),
            SnowflakeError::External(s) => write!(f, "snowflake external worker id error: {s}"),
        }
    }
}

impl std::error::Error for SnowflakeError {}

/// 业务作用：校验雪花 ID 位长组合。
///
/// 参数说明:
/// - `worker_id_bits`: worker id 占用的位数，决定单个业务集群最多可容纳的节点数。
/// - `seq_bits`: 同一毫秒内递增序列占用的位数，决定单节点瞬时发号容量。
///
/// 返回: 位长分别落在合法范围且总和不超过 22 时返回成功，否则返回
/// [`SnowflakeError::BitRange`]。
pub fn validate_bits(worker_id_bits: u32, seq_bits: u32) -> Result<(), SnowflakeError> {
    if !(1..=15).contains(&worker_id_bits) {
        return Err(SnowflakeError::BitRange(format!(
            "worker_id_bits must be [1, 15], got {worker_id_bits}"
        )));
    }
    if !(3..=21).contains(&seq_bits) {
        return Err(SnowflakeError::BitRange(format!(
            "seq_bits must be [3, 21], got {seq_bits}"
        )));
    }
    if worker_id_bits + seq_bits > 22 {
        return Err(SnowflakeError::BitRange(format!(
            "worker_id_bits + seq_bits must <= 22, got {}",
            worker_id_bits + seq_bits
        )));
    }
    Ok(())
}

/// 业务作用：当前 epoch 毫秒。
///
/// 参数说明: 无。
///
/// 返回: 系统时间相对 Unix epoch 的毫秒数；系统时间早于 epoch 时返回零。
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 保存雪花生成器的递增状态。
struct State {
    last_ts: i64,
    seq: i64,
}

/// 纯本地雪花 ID 生成器。
pub struct Snowflake {
    total_shift: u32,
    max_seq: i64,
    base_time: i64,
    shifted_worker_id: i64,
    state: Mutex<State>,
}

impl Snowflake {
    /// 业务作用：按完整参数构造雪花 ID 生成器。
    ///
    /// 参数说明:
    /// - `worker_id`: 当前进程使用的 worker 编号，必须落在 `worker_id_bits` 可表达的范围内。
    /// - `base_time`: 业务纪元毫秒时间戳，生成结果会存储相对该时间的毫秒差。
    /// - `worker_id_bits`: worker 编号占用位数，影响可分配节点数量。
    /// - `seq_bits`: 毫秒内序列号占用位数，影响单节点同毫秒内可生成的 ID 数。
    ///
    /// 返回: 参数合法时返回独立的本地生成器；位长或 worker id 越界时返回相应
    /// [`SnowflakeError`]。
    pub fn new(
        worker_id: i64,
        base_time: i64,
        worker_id_bits: u32,
        seq_bits: u32,
    ) -> Result<Self, SnowflakeError> {
        validate_bits(worker_id_bits, seq_bits)?;
        let max_worker_id = (1i64 << worker_id_bits) - 1;
        if worker_id < 0 || worker_id > max_worker_id {
            return Err(SnowflakeError::WorkerIdRange(format!(
                "worker_id must be [0, {max_worker_id}], got {worker_id}"
            )));
        }
        Ok(Self {
            total_shift: worker_id_bits + seq_bits,
            max_seq: (1i64 << seq_bits) - 1,
            base_time,
            shifted_worker_id: worker_id << seq_bits,
            state: Mutex::new(State { last_ts: 0, seq: 0 }),
        })
    }

    /// 业务作用：按默认位长和基础时间戳构造雪花 ID 生成器。
    ///
    /// 参数说明:
    /// - `worker_id`: 当前进程使用的 worker 编号，按默认 worker 位长校验范围。
    ///
    /// 返回: worker id 合法时返回使用默认纪元和位长的生成器，否则返回范围错误。
    pub fn with_default_bits(worker_id: i64) -> Result<Self, SnowflakeError> {
        Self::new(
            worker_id,
            DEFAULT_BASE_TIME,
            DEFAULT_WORKER_ID_BITS,
            DEFAULT_SEQ_BITS,
        )
    }

    /// 业务作用：生成一个 ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前生成器的下一个本地 ID；内部互斥锁失效时终止当前调用。
    pub fn generate(&self) -> i64 {
        let mut st = self.state.lock().expect("snowflake mutex poisoned");
        self.next_id_locked(&mut st)
    }

    /// 业务作用：批量生成 `count` 个 ID,只加一次锁。
    ///
    /// 参数说明:
    /// - `count`: 本次批量申请的 ID 数量；为 `0` 时返回空列表。
    ///
    /// 返回: 在一次持锁区间内按生成顺序分配的 `count` 个 ID。
    pub fn generate_n(&self, count: usize) -> Vec<i64> {
        let mut st = self.state.lock().expect("snowflake mutex poisoned");
        (0..count).map(|_| self.next_id_locked(&mut st)).collect()
    }

    /// 业务作用：在已持锁状态下生成下一个 ID。
    ///
    /// 参数说明:
    /// - `st`: 当前生成器的时间戳和序列状态；调用方必须已持有对应互斥锁。
    ///
    /// 返回: 更新状态后生成的下一个 ID；墙钟未前进时通过序列或逻辑毫秒保持递增。
    #[inline]
    fn next_id_locked(&self, st: &mut State) -> i64 {
        let now = now_ms() - self.base_time;
        if now > st.last_ts {
            st.last_ts = now;
            st.seq = 0;
        } else if st.seq < self.max_seq {
            st.seq += 1;
        } else {
            st.last_ts += 1;
            st.seq = 0;
        }
        (st.last_ts << self.total_shift) | self.shifted_worker_id | st.seq
    }
}

impl IdGenerate for Snowflake {
    /// 业务作用：生成下一个 ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前生成器分配的下一个本地 ID。
    fn next_id(&self) -> i64 {
        self.generate()
    }
}

/// 本地雪花 ID 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SnowflakeConfig {
    /// worker id。
    pub worker_id: i64,
    /// 基础时间戳。
    pub base_time: i64,
    /// 机器码位长。
    pub worker_id_bits: u32,
    /// 序列号位长。
    pub seq_bits: u32,
}

impl Default for SnowflakeConfig {
    /// 业务作用：返回单节点默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 使用内置纪元、worker id 和位长的配置。
    fn default() -> Self {
        Self {
            worker_id: DEFAULT_WORKER_ID,
            base_time: DEFAULT_BASE_TIME,
            worker_id_bits: DEFAULT_WORKER_ID_BITS,
            seq_bits: DEFAULT_SEQ_BITS,
        }
    }
}

impl SnowflakeConfig {
    /// 业务作用：按当前配置构造本地雪花 ID 生成器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 配置合法时返回本地生成器，否则返回位长或 worker id 范围错误。
    pub fn build(&self) -> Result<Snowflake, SnowflakeError> {
        Snowflake::new(
            self.worker_id,
            self.base_time,
            self.worker_id_bits,
            self.seq_bits,
        )
    }

    /// 业务作用：[`build`](Self::build) 的别名；“local”强调只做本地构造，
    /// (位长校验 + 纯算法,无副作用),不含 Redis workerId 分配;分布式分配走
    /// `nadis::SnowflakeConfig::build_with_redis`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 配置合法时返回本地生成器，否则返回与 [`build`](Self::build) 相同的配置错误。
    pub fn build_local(&self) -> Result<Snowflake, SnowflakeError> {
        self.build()
    }
}
