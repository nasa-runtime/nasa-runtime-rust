//! Orchestrator durable timer 的种类词汇与稳定身份派生。
//!
//! timer 种类是有界词汇（进入 `saga_timer.kind` 唯一键与运维查询维度）；timer 身份由
//! `(saga, scope, kind, attempt)` 确定性派生，调度事务的崩溃重试在任何进程都会得到
//! 同一 `timer_id`，从而命中 store 的幂等吸收而不是产生第二个计时器。

use nasaga_core::{AttemptNo, Direction, SagaId, StepName, StepPhase};
use nasaga_mysql::TimerScope;
use uuid::Uuid;

use crate::envelope::canonical_bytes;

/// 步骤 execute 超时 timer：到期进入取消屏障，绝不直接补偿。
pub const KIND_STEP_TIMEOUT: &str = "step-timeout";

/// 实例级业务 deadline timer：到期按实例所处阶段执行固定裁决。
pub const KIND_INSTANCE_DEADLINE: &str = "instance-deadline";

/// 旧版未区分方向的 `Unknown` 解决预算 timer；仅用于滚动升级期间消费既有行。
///
/// 新调度必须使用 [`KIND_FORWARD_RESOLUTION_BUDGET`] 或
/// [`KIND_COMPENSATION_RESOLUTION_BUDGET`]，避免同一步骤两次解决阶段复用自然键。
pub const KIND_RESOLUTION_BUDGET: &str = "resolution-budget";

/// 正向 `Unknown` 的解决预算 timer：超期升级人工介入，绝不自动降级为 `Rejected`。
pub const KIND_FORWARD_RESOLUTION_BUDGET: &str = "forward-resolution-budget";

/// 补偿 `Unknown` 的解决预算 timer：与正向预算隔离自然键，避免已取消行阻断后续补偿查询。
pub const KIND_COMPENSATION_RESOLUTION_BUDGET: &str = "compensation-resolution-budget";

/// 取消命令有界重发 timer：预算耗尽升级人工介入，自动状态不允许无限滞留。
pub const KIND_CANCEL_TIMEOUT: &str = "cancel-timeout";

/// 补偿命令有界重发 timer：预算耗尽升级人工介入，不伪造 `COMPENSATED`。
pub const KIND_COMPENSATE_TIMEOUT: &str = "compensate-timeout";

/// 解决查询有界重发 timer：在解决预算内按 attempt 重发 resolve 命令。
pub const KIND_RESOLVE_TIMEOUT: &str = "resolve-timeout";

/// 派生 timer 身份的固定命名空间（ASCII `nasasaga-v1-tmid`）。
///
/// 发布后**不得修改**：改动会让恢复进程为同一逻辑 timer 派生出不同身份，
/// 使调度幂等失效并产生重复计时器。
const TIMER_ID_NAMESPACE: Uuid = Uuid::from_bytes(*b"nasasaga-v1-tmid");

/// 业务作用：由作用域与种类确定性派生 timer 身份，使调度重试跨进程幂等。
///
/// 参数说明：
/// - `saga_id`: 实例身份。
/// - `scope`: timer 作用域。
/// - `kind`: timer 种类。
/// - `attempt`: 关联尝试序号。
///
/// 返回：跨进程一致的 timer id（UUID 文本）。
pub fn derive_timer_id(
    saga_id: &SagaId,
    scope: &TimerScope<'_>,
    kind: &str,
    attempt: AttemptNo,
) -> String {
    Uuid::new_v5(
        &TIMER_ID_NAMESPACE,
        &canonical_bytes(&[
            saga_id.as_str().as_bytes(),
            scope.kind_str().as_bytes(),
            scope.key_str().as_bytes(),
            kind.as_bytes(),
            &attempt.get().to_be_bytes(),
        ]),
    )
    .to_string()
}

/// 业务作用：返回某阶段命令的超时 timer 种类，把"命令在途"与"预算重发"统一到词汇表。
///
/// 参数说明：
/// - `phase`: 命令所处阶段。
///
/// 返回：该阶段命令对应的 timer 种类稳定名。
pub fn phase_timeout_kind(phase: StepPhase) -> &'static str {
    match phase {
        StepPhase::Execute => KIND_STEP_TIMEOUT,
        StepPhase::Cancel => KIND_CANCEL_TIMEOUT,
        StepPhase::Compensate => KIND_COMPENSATE_TIMEOUT,
        StepPhase::Resolve => KIND_RESOLVE_TIMEOUT,
    }
}

/// 业务作用：按当前推进方向选择解决预算 timer 种类，隔离同一步骤的正向与补偿解决阶段。
///
/// 参数说明：
/// - `direction`: 当前未知效果所属的推进方向。
///
/// 返回：正向或补偿方向各自稳定的 timer kind。
pub(crate) fn resolution_budget_kind(direction: Direction) -> &'static str {
    match direction {
        Direction::Forward => KIND_FORWARD_RESOLUTION_BUDGET,
        Direction::Compensating => KIND_COMPENSATION_RESOLUTION_BUDGET,
    }
}

/// 业务作用：把 timer 行的作用域列还原为步骤作用域，供裁决时定位步骤。
///
/// 参数说明：
/// - `scope_kind`: 持久化的作用域类别文本。
/// - `scope_key`: 持久化的作用域键。
///
/// 返回：步骤级返回步骤名；实例级返回 `None`；类别文本非法返回错误。
pub fn parse_step_scope(scope_kind: &str, scope_key: &str) -> anyhow::Result<Option<StepName>> {
    match scope_kind {
        "INSTANCE" => Ok(None),
        "STEP" => {
            Ok(Some(StepName::new(scope_key).map_err(|v| {
                anyhow::anyhow!("bad step scope: {}", v.code())
            })?))
        }
        _ => anyhow::bail!("unknown timer scope kind"),
    }
}
