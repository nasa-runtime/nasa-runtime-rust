//! Durable timer 的后端中立模型与不可复制 fencing capability。

use std::sync::atomic::{AtomicU64, Ordering};

use nasaga_core::{AttemptNo, SagaId, StepName};
use uuid::Uuid;

const INSTANCE_SCOPE_KEY: &str = "instance";
const FENCING_TOKEN_NAMESPACE: Uuid = Uuid::from_bytes(*b"nasasaga-v1-fcns");

/// 业务作用：以长度前缀编码 fencing 派生字段，消除可变长字段拼接歧义。
///
/// 参数说明：
/// - `fields`：按稳定顺序给出的 runtime nonce、owner、时钟和领取序号。
///
/// 返回：字段边界唯一、可直接送入 UUIDv5 的规范字节串。
fn canonical_fencing_bytes(fields: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for field in fields {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    encoded
}

/// 业务作用：表示只能由安全发行器产生的 timer 租约 capability。
///
/// 类型不实现 `Clone`，领取后所有权进入 [`TimerClaimBatch`]，不能跨轮复制复用。
#[derive(PartialEq, Eq)]
pub struct TimerFencingToken(String);

impl TimerFencingToken {
    /// 业务作用：向具体数据库 adapter 提供 opaque token 的持久化表示。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定为小写 UUID 文本的只读切片；调用方不得记录或对外传播。
    #[doc(hidden)]
    pub fn persistence_value(&self) -> &str {
        &self.0
    }
}

/// 业务作用：为单个 timer worker runtime 发行跨实例不碰撞的 fencing capability。
pub struct TimerFencingTokenIssuer {
    runtime_nonce: Uuid,
    claim_seq: AtomicU64,
}

impl TimerFencingTokenIssuer {
    /// 业务作用：建立独立的 fencing token 发行域，隔离副本误配和进程重启。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：持有不可注入随机 nonce、领取序号从零开始的新发行器。
    pub fn new() -> Self {
        // nonce 必须来自随机源，禁止从 owner 或配置派生；同名副本不能共享 fencing 权威。
        Self {
            runtime_nonce: Uuid::new_v4(),
            claim_seq: AtomicU64::new(0),
        }
    }

    /// 业务作用：为一次 timer 批量领取生成当前 runtime 与批次唯一的 capability。
    ///
    /// 参数说明：
    /// - `owner`：本副本稳定租约身份，只参与域分离。
    /// - `now_ms`：本轮领取时注入的 epoch 毫秒。
    ///
    /// 返回：包含 runtime 随机熵与进程内单调序号且不可复制的 token。
    pub fn issue(&self, owner: &str, now_ms: i64) -> TimerFencingToken {
        let seq = self.claim_seq.fetch_add(1, Ordering::Relaxed);
        TimerFencingToken(
            Uuid::new_v5(
                &FENCING_TOKEN_NAMESPACE,
                &canonical_fencing_bytes(&[
                    self.runtime_nonce.as_bytes(),
                    owner.as_bytes(),
                    &now_ms.to_be_bytes(),
                    &seq.to_be_bytes(),
                ]),
            )
            .to_string(),
        )
    }
}

impl Default for TimerFencingTokenIssuer {
    /// 业务作用：按安全默认值创建独立的 worker fencing token 发行域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`TimerFencingTokenIssuer::new`] 相同的新发行器。
    fn default() -> Self {
        Self::new()
    }
}

/// 业务作用：区分 timer 的实例级与步骤级作用域。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerScope<'a> {
    /// 实例级期限或预算。
    Instance,
    /// 步骤级超时，绑定真实 step name。
    Step(&'a StepName),
}

impl TimerScope<'_> {
    /// 业务作用：返回作用域类别的稳定文本名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`INSTANCE` 或 `STEP`。
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::Instance => "INSTANCE",
            Self::Step(_) => "STEP",
        }
    }

    /// 业务作用：返回作用域唯一键。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：实例级固定键或真实 step name。
    pub fn key_str(&self) -> &str {
        match self {
            Self::Instance => INSTANCE_SCOPE_KEY,
            Self::Step(step) => step.as_str(),
        }
    }
}

/// 业务作用：表示 timer 行的持久生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerState {
    /// 等待到期。
    Pending,
    /// 已被副本领取，租约生效中。
    Claimed,
    /// 到期触发的状态迁移已经提交。
    Fired,
    /// 所属步骤或实例已迁移离开，永久不再触发。
    Cancelled,
}

impl TimerState {
    /// 业务作用：返回 timer 状态的稳定持久化名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：封闭状态对应的大写文本。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Claimed => "CLAIMED",
            Self::Fired => "FIRED",
            Self::Cancelled => "CANCELLED",
        }
    }

    /// 业务作用：把持久化文本严格解析回 timer 状态。
    ///
    /// 参数说明：
    /// - `raw`：数据库读出的候选状态文本。
    ///
    /// 返回：命中封闭词汇表时返回状态，否则返回 `None`。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "PENDING" => Some(Self::Pending),
            "CLAIMED" => Some(Self::Claimed),
            "FIRED" => Some(Self::Fired),
            "CANCELLED" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// 业务作用：描述一次 durable timer 调度的全部持久化输入。
#[derive(Debug, Clone)]
pub struct TimerSpec<'a> {
    /// timer 的稳定身份。
    pub timer_id: &'a str,
    /// 所属实例。
    pub saga_id: &'a SagaId,
    /// timer 作用域。
    pub scope: TimerScope<'a>,
    /// timer 稳定种类。
    pub kind: &'a str,
    /// 到期时刻。
    pub due_at_ms: i64,
    /// 关联尝试序号。
    pub attempt: AttemptNo,
    /// 调度时刻的实例版本，消费前必须复验。
    pub expected_saga_version: u64,
}

/// 业务作用：区分 timer 首次调度与完全一致的幂等重放。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerSchedule {
    /// 本事务真实创建了 timer。
    Scheduled,
    /// 同一作用域、种类和 attempt 已存在同 id timer。
    AlreadyScheduled,
}

/// 业务作用：区分 timer 重排成功与不可复活的终态或缺失行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerReschedule {
    /// 到期时刻已更新、generation 已递增并回到待领取状态。
    Rescheduled,
    /// timer 不存在或已进入不可复活终态。
    NotFound,
}

/// 业务作用：区分 timer fencing 操作生效与权威丢失。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerFencing {
    /// 操作生效，fencing 校验通过。
    Applied,
    /// 租约、状态或 token 已不属于当前 owner。
    Lost,
}

/// 业务作用：保存 claim 批次中除 capability 外的全部 timer 复查依据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaTimerRow {
    /// timer 稳定身份。
    pub timer_id: String,
    /// 所属实例。
    pub saga_id: SagaId,
    /// 作用域类别。
    pub scope_kind: String,
    /// 作用域键。
    pub scope_key: String,
    /// timer 种类。
    pub kind: String,
    /// 业务到期时刻。
    pub due_at_ms: i64,
    /// 下次允许 worker 领取的时刻。
    pub available_at_ms: i64,
    /// 当前状态。
    pub state: TimerState,
    /// 关联尝试序号。
    pub attempt: AttemptNo,
    /// 调度时刻的实例版本。
    pub expected_saga_version: u64,
    /// 重排代数。
    pub generation: u32,
    /// 当前租约持有者。
    pub owner: Option<String>,
    /// 租约到期时刻。
    pub claimed_until_ms: Option<i64>,
}

/// 业务作用：绑定一次领取返回的 timer 集合与不可复制 fencing capability。
pub struct TimerClaimBatch {
    token: TimerFencingToken,
    timers: Vec<SagaTimerRow>,
}

impl TimerClaimBatch {
    /// 业务作用：由 adapter 在数据库领取明确提交后组合 token 与精确 timer 集合。
    ///
    /// 参数说明：
    /// - `token`：已被本轮 claim 消费的不可复制 capability。
    /// - `timers`：数据库确认属于该 token 的已领取行。
    ///
    /// 返回：只能借用 token 完成或交还其中行的 claim 批次。
    #[doc(hidden)]
    pub fn from_committed_claim(token: TimerFencingToken, timers: Vec<SagaTimerRow>) -> Self {
        Self { token, timers }
    }

    /// 业务作用：借用本批 fencing capability。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与本批数据库领取绑定的只读 token。
    pub fn token(&self) -> &TimerFencingToken {
        &self.token
    }

    /// 业务作用：读取本轮成功领取的 timer 快照集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：每项只能凭本批 token 完成或交还的只读切片。
    pub fn timers(&self) -> &[SagaTimerRow] {
        &self.timers
    }

    /// 业务作用：返回本轮成功领取的 timer 数量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本批 timer 数。
    pub fn len(&self) -> usize {
        self.timers.len()
    }

    /// 业务作用：判断本轮是否未领取任何 timer。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：集合为空时返回 `true`。
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }
}
