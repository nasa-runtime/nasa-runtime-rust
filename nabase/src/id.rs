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
    /// 时间差或逻辑时间已超出非负 ID 的时间位容量。
    TimeRange,
    /// 发号状态锁中毒，无法再确认状态完整。
    StateUnavailable,
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
            SnowflakeError::TimeRange => f.write_str("snowflake timestamp capacity exhausted"),
            SnowflakeError::StateUnavailable => f.write_str("snowflake state unavailable"),
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

/// 业务作用：取得本地发号使用的 Unix 毫秒时间，并避免窄化转换回绕。
///
/// 参数说明: 无。
///
/// 返回：早于 Unix epoch 时返回零；超出 i64 毫秒范围时返回 i64::MAX，由生成器的时间容量门禁裁决。
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        // 超大时间不能截断成过去的时间；饱和值继续接受相对时间容量核对。
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
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
    /// 业务作用：为调用方独占的 worker 编号建立具有有限时间容量的本地生成器。
    ///
    /// 参数说明:
    /// - `worker_id`: 当前实例独占的 worker 编号，必须落在 worker 位宽允许范围内。
    /// - `base_time`: ID 空间统一的 Unix 毫秒纪元；未来纪元从逻辑毫秒零开始递增。
    /// - `worker_id_bits`: worker 编号位数，合法范围为 1 到 15。
    /// - `seq_bits`: 序列位数，合法范围为 3 到 21，与 worker 位数之和不超过 22。
    ///
    /// 返回：参数合法且当前相对时间尚可编码时返回生成器；位宽、worker 或时间容量不合法时返回相应错误。
    /// 不协调其它实例或保存重启时间上界；同一 worker 的新实例必须由调用方证明不会与历史 ID 重叠。
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
        let total_shift = worker_id_bits + seq_bits;
        let elapsed = i128::from(now_ms()) - i128::from(base_time);
        // 保留符号位为零；构造时已耗尽的时间域不能交付可发号实例。
        if elapsed > i128::from(i64::MAX >> total_shift) {
            return Err(SnowflakeError::TimeRange);
        }
        Ok(Self {
            total_shift,
            max_seq: (1i64 << seq_bits) - 1,
            base_time,
            shifted_worker_id: worker_id << seq_bits,
            state: Mutex::new(State {
                last_ts: 0,
                seq: 0,
            }),
        })
    }

    /// 业务作用：按默认位长和基础时间戳构造雪花 ID 生成器。
    ///
    /// 参数说明:
    /// - `worker_id`: 当前进程使用的 worker 编号，按默认 worker 位长校验范围。
    ///
    /// 返回: worker id 合法且默认纪元的相对时间可编码时返回生成器，否则返回 worker 或时间容量错误。
    pub fn with_default_bits(worker_id: i64) -> Result<Self, SnowflakeError> {
        Self::new(
            worker_id,
            DEFAULT_BASE_TIME,
            DEFAULT_WORKER_ID_BITS,
            DEFAULT_SEQ_BITS,
        )
    }

    /// 业务作用：为需要处理时间耗尽或状态失效的调用方申请一个本地 ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时提交一次发号并返回非负 ID；时间容量耗尽或状态锁中毒时返回错误，不交付 ID。
    pub fn try_generate(&self) -> Result<i64, SnowflakeError> {
        // 锁中毒时不能证明先前状态完整，必须停止发号而不是恢复使用可能重叠的序列。
        let mut state = self
            .state
            .lock()
            .map_err(|_| SnowflakeError::StateUnavailable)?;
        self.next_id_locked(&mut state)
    }

    /// 业务作用：提供遇到发号错误时终止调用的单次 ID 接口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时返回非负且严格递增的本地 ID；时间容量耗尽或状态锁中毒时 panic。
    /// 需要显式处理失败的调用方使用 try_generate；此入口不会在时间位耗尽后回绕。
    pub fn generate(&self) -> i64 {
        self.try_generate()
            .expect("snowflake generation unavailable")
    }

    /// 业务作用：在一次持锁期间准备整批 ID，只在全部可编码时提交生成器状态。
    ///
    /// 参数说明:
    /// - `count`: 本次需要的连续发号次数；零表示不申请 ID。
    ///
    /// 返回：成功时返回按生成顺序排列的整批 ID；时间容量耗尽或状态锁中毒时返回错误，原发号状态不变。
    pub fn try_generate_n(&self, count: usize) -> Result<Vec<i64>, SnowflakeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SnowflakeError::StateUnavailable)?;
        let mut candidate = State {
            last_ts: state.last_ts,
            seq: state.seq,
        };
        // 不把未能完整交付的批次部分写入状态，失败后仍保留调用前的时间和序列。
        let ids = (0..count)
            .map(|_| self.next_id_locked(&mut candidate))
            .collect::<Result<Vec<_>, _>>()?;
        *state = candidate;
        Ok(ids)
    }

    /// 业务作用：提供遇到发号错误时终止调用的整批 ID 接口。
    ///
    /// 参数说明:
    /// - `count`: 本次申请的 ID 数量；零时返回空列表。
    ///
    /// 返回：成功时提交整批状态并返回按生成顺序排列的 ID；时间容量耗尽或状态锁中毒时 panic，原发号状态不变。
    /// 需要显式处理失败的调用方使用 try_generate_n。
    pub fn generate_n(&self, count: usize) -> Vec<i64> {
        self.try_generate_n(count)
            .expect("snowflake generation unavailable")
    }

    /// 业务作用：在持锁状态中预先核对时间容量，再提交一次不会回绕的发号状态。
    ///
    /// 参数说明:
    /// - `st`: 由本生成器建立的时间和序列状态，调用方必须已持有对应互斥锁。
    ///
    /// 返回：成功时更新状态并返回非负且严格递增的 ID；墙钟或逻辑毫秒超出容量时返回 TimeRange，保持输入状态不变。
    /// 墙钟回退或纪元尚未到达时继续使用逻辑时间，序列耗尽后推进一毫秒。
    #[inline]
    fn next_id_locked(&self, st: &mut State) -> Result<i64, SnowflakeError> {
        let now = i128::from(now_ms()) - i128::from(self.base_time);
        let max_timestamp = i64::MAX >> self.total_shift;
        // 先拒绝无法编码的墙钟，避免移位丢弃高位后重新发出历史编号。
        if now > i128::from(max_timestamp) {
            return Err(SnowflakeError::TimeRange);
        }
        let (timestamp, sequence) = if now > i128::from(st.last_ts) {
            (now as i64, 0)
        } else if st.seq < self.max_seq {
            (st.last_ts, st.seq + 1)
        } else {
            // 序列耗尽只能推进仍可编码的逻辑毫秒，不能绕过时间位上界。
            if st.last_ts >= max_timestamp {
                return Err(SnowflakeError::TimeRange);
            }
            (st.last_ts + 1, 0)
        };
        let id = (timestamp << self.total_shift) | self.shifted_worker_id | sequence;
        st.last_ts = timestamp;
        st.seq = sequence;
        Ok(id)
    }
}

impl IdGenerate for Snowflake {
    /// 业务作用：生成下一个 ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前生成器分配的下一个非负本地 ID；时间容量耗尽或状态锁中毒时 panic。
    fn next_id(&self) -> i64 {
        self.generate()
    }
}

/// 本地雪花 ID 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SnowflakeConfig {
    /// 节点机器码；同一 ID 空间内各生成节点必须使用不同值以避免重复。
    pub worker_id: i64,
    /// ID 空间统一的 Unix 毫秒纪元；未来纪元使用从零开始的逻辑时间，不能据此假定 ID 是实际创建时间。
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
    /// 返回: 位宽、worker id 和相对时间容量合法时返回本地生成器，否则返回相应配置或时间错误。
    pub fn build(&self) -> Result<Snowflake, SnowflakeError> {
        Snowflake::new(
            self.worker_id,
            self.base_time,
            self.worker_id_bits,
            self.seq_bits,
        )
    }

    /// 业务作用：作为 [`build`](Self::build) 的本地构造别名，校验位宽、worker id 和时间容量；不领取分布式编号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 配置合法时返回本地生成器，否则返回与 [`build`](Self::build) 相同的配置错误。
    pub fn build_local(&self) -> Result<Snowflake, SnowflakeError> {
        self.build()
    }
}
