//! Saga 实例创建、检索和 CAS 推进的后端中立输入输出。

use nasaga_core::{
    ControlState, DefinitionVersion, Direction, SagaId, SagaStatus, StepName, TriggerKind,
};

use crate::SagaInstanceRow;

/// 创建时间检索可移植到全部受支持数据库的最小 epoch 毫秒。
pub const SAGA_INSTANCE_TIME_MIN_MS: i64 = 0;

/// 创建时间检索可移植到全部受支持数据库的最大 epoch 毫秒。
pub const SAGA_INSTANCE_TIME_MAX_MS: i64 = 32_536_771_199_999;

const SAGA_INSTANCE_TIME_MAX_US: i64 = SAGA_INSTANCE_TIME_MAX_MS * 1_000 + 999;

/// 业务作用：描述一次实例创建请求的全部持久化输入。
#[derive(Debug, Clone)]
pub struct NewSagaInstance<'a> {
    /// 实例身份，由调用方生成并全程稳定。
    pub saga_id: &'a SagaId,
    /// 租户身份；无租户部署使用固定 system tenant。
    pub tenant: &'a nasaga_core::TenantId,
    /// workflow 名称。
    pub workflow: &'a nasaga_core::WorkflowName,
    /// 业务幂等键，同一业务意图只允许一个实例。
    pub business_key: &'a nasaga_core::BusinessKey,
    /// 固定到实例的 definition 版本。
    pub definition_version: DefinitionVersion,
    /// definition 的 canonical 内容摘要。
    pub definition_digest: &'a str,
    /// 启动请求的 canonical 摘要。
    pub start_request_digest: &'a str,
    /// 仅匹配既有无 schema JSON 实例的兼容摘要；新实例仍写入原始字节摘要。
    pub legacy_start_request_digest: Option<&'a str>,
    /// 实例级业务 deadline；无全局期限时为空。
    pub deadline_at_ms: Option<i64>,
    /// 创建时定位的首个步骤。
    pub current_step: Option<&'a StepName>,
    /// 初始 transition 的触发来源类别。
    pub trigger_kind: TriggerKind,
    /// 初始 transition 的稳定触发身份。
    pub trigger_id: &'a str,
    /// 创建入口显式传入的 canonical W3C traceparent。
    pub traceparent: Option<&'a str>,
}

/// 业务作用：描述一次租户受限实例检索的全部过滤条件。
#[derive(Debug, Clone, Copy)]
pub struct SagaInstanceQuery<'a> {
    /// 租户身份，强制过滤条件。
    pub tenant: &'a nasaga_core::TenantId,
    /// workflow 名称；为空不过滤。
    pub workflow: Option<&'a nasaga_core::WorkflowName>,
    /// 业务状态集合；为空不过滤。
    pub statuses: Option<&'a [SagaStatus]>,
    /// 创建时刻下界，含该时刻。
    pub created_from_ms: Option<i64>,
    /// 创建时刻上界，不含该时刻。
    pub created_to_ms: Option<i64>,
    /// 时间有序查询中上一页最后一行的精确创建时刻；单位为微秒，必须与 `after` 同时出现。
    pub after_created_at_us: Option<i64>,
    /// 上一页最后一个 saga_id；时间有序查询中与创建时刻共同组成游标。
    pub after: Option<&'a SagaId>,
    /// 单次存储读取上限；协议层为判定下一页可请求公开页大小加一，最大为 1001。
    pub limit: u32,
}

/// 业务作用：区分实例检索参数违反的公共协议约束，供 HTTP 与数据库适配器执行同一裁决。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaInstanceQueryParameterError {
    /// 存储读取上限不在公共边界内。
    PageSizeOutOfRange,
    /// 时间过滤或时间游标超出数据库公共可表示范围。
    TimeOutOfRange,
    /// 创建时间下界晚于上界。
    TimeWindowInverted,
    /// 时间有序分页只提供了创建时刻或 saga_id 中的一部分。
    CursorIncomplete,
}

impl std::fmt::Display for SagaInstanceQueryParameterError {
    /// 业务作用：把类型化参数错误转换为不含业务数据的稳定诊断文本。
    ///
    /// 参数说明：`formatter` 接收标准格式化输出目标。
    ///
    /// 返回：写入对应公共约束的稳定文本；格式化目标失败时返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::PageSizeOutOfRange => "instance query fetch limit must be within 1..=1001",
            Self::TimeOutOfRange => "instance query time is outside the portable range",
            Self::TimeWindowInverted => "instance query time window is inverted",
            Self::CursorIncomplete => "instance query time cursor is incomplete",
        })
    }
}

impl std::error::Error for SagaInstanceQueryParameterError {}

/// 业务作用：在进入任一数据库前验证实例检索的公共页大小、时间范围与复合游标合同。
///
/// 参数说明：`query` 是协议层或内部调用方组装的完整检索条件。
///
/// 返回：参数在全部受支持数据库上语义一致时成功；否则返回可映射为客户端错误的类型化原因。
pub fn validate_saga_instance_query(
    query: &SagaInstanceQuery<'_>,
) -> Result<(), SagaInstanceQueryParameterError> {
    if query.limit == 0 || query.limit > 1_001 {
        return Err(SagaInstanceQueryParameterError::PageSizeOutOfRange);
    }
    for timestamp in [query.created_from_ms, query.created_to_ms]
        .into_iter()
        .flatten()
    {
        if !(SAGA_INSTANCE_TIME_MIN_MS..=SAGA_INSTANCE_TIME_MAX_MS).contains(&timestamp) {
            return Err(SagaInstanceQueryParameterError::TimeOutOfRange);
        }
    }
    if query.after_created_at_us.is_some_and(|timestamp| {
        !(SAGA_INSTANCE_TIME_MIN_MS..=SAGA_INSTANCE_TIME_MAX_US).contains(&timestamp)
    }) {
        return Err(SagaInstanceQueryParameterError::TimeOutOfRange);
    }
    if query
        .created_from_ms
        .zip(query.created_to_ms)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(SagaInstanceQueryParameterError::TimeWindowInverted);
    }
    let time_ordered = query.created_from_ms.is_some() || query.created_to_ms.is_some();
    let cursor_complete = match (
        time_ordered,
        query.after_created_at_us.is_some(),
        query.after.is_some(),
    ) {
        (true, has_time, has_id) => has_time == has_id,
        (false, false, _) => true,
        (false, true, _) => false,
    };
    if !cursor_complete {
        return Err(SagaInstanceQueryParameterError::CursorIncomplete);
    }
    Ok(())
}

/// 业务作用：区分实例真实创建和业务幂等命中，决定是否允许发布首步命令。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaCreation {
    /// 本事务真实创建了实例；调用方须在同一事务内补齐首步命令、timer 与 Audit。
    Created(SagaInstanceRow),
    /// 业务幂等键命中已有实例；不产生任何新副作用。
    Existing(SagaInstanceRow),
}

/// 业务作用：描述一次 CAS 状态推进要写入的业务状态、触发证据和冻结计划。
#[derive(Debug, Clone)]
pub struct TransitionSpec<'a> {
    /// 目标业务状态。
    pub to_status: SagaStatus,
    /// 推进后的方向。
    pub direction: Direction,
    /// 推进后的当前步骤；无明确定位时为空。
    pub current_step: Option<&'a StepName>,
    /// 触发来源类别。
    pub trigger_kind: TriggerKind,
    /// 触发身份。
    pub trigger_id: &'a str,
    /// 实例固定的 definition 版本。
    pub definition_version: DefinitionVersion,
    /// 本次迁移产生的稳定失败原因码；为空时保留既有值。
    pub failure_code: Option<&'a str>,
    /// 进入补偿时冻结的计划摘要；为空时保留既有值。
    pub compensation_plan_version: Option<&'a str>,
}

/// 业务作用：聚合暂停或恢复 CAS 的实例权威与同事务审计事实。
#[derive(Debug, Clone, Copy)]
pub struct ControlTransitionSpec<'a> {
    /// 实例身份。
    pub saga_id: &'a SagaId,
    /// 调用方持有的实例版本。
    pub expected_version: u64,
    /// 调用方持有的控制态 generation。
    pub expected_control_version: u64,
    /// 预期当前控制状态。
    pub from: ControlState,
    /// 目标控制状态。
    pub to: ControlState,
    /// 管理请求稳定幂等身份。
    pub operation_id: &'a str,
    /// 认证层提供的稳定主体 id。
    pub actor: &'a str,
    /// 本次控制动作的业务原因。
    pub reason: &'a str,
}

/// 业务作用：区分 CAS 推进成功、快照冲突和重复触发。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome {
    /// 推进成功；新版本同时是新的 transition sequence。
    Applied {
        /// CAS 之后的实例版本。
        new_version: u64,
    },
    /// 预期版本或状态未命中，调用方必须重新读取。
    Conflict,
    /// 同一触发已经推进过，本事务必须回滚后按已生效裁决。
    DuplicateTrigger,
}

/// 业务作用：区分控制状态 CAS 的应用、幂等重放和快照冲突。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlCasOutcome {
    /// 控制状态已切换。
    Applied,
    /// 同一 operation 已提交过，不再次改变控制态。
    AlreadyApplied,
    /// 实例版本、控制 generation 或状态与预期不符。
    Conflict,
}

/// 业务作用：区分人工业务动作审计的首次写入和完全一致幂等重放。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagementAuditOutcome {
    /// 本事务首次登记该 operation。
    Recorded,
    /// 同一 operation 与审计字段已提交，不再产生业务副作用。
    AlreadyRecorded,
}
