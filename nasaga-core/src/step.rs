//! 参与方步骤合同。
//!
//! 这些 trait 只描述**业务语义**：本地事务、Inbox claim、step gate、结果事件与 Audit
//! 由宏生成的 adapter 统一包裹，业务实现不得自行 publish 事件或开启第二套事务。
//! 正向执行与补偿在同一类型上绑定，使“有补偿”成为编译期可检查的事实，
//! 而不是运行期才发现缺失。

use crate::context::SagaContext;
use crate::outcome::{CancelResult, CompensationResult, SagaResult};

/// 业务作用：定义一个参与方步骤的正向执行与显式补偿，二者在类型层面成对出现。
///
/// 关联类型说明：`Command` 是正向输入；`Success` 是可持久化的成功产物；
/// `Compensation` 是补偿输入，其内容只能来自持久化事实（步骤身份或受限 artifact），
/// 不得依赖 Orchestrator 内存或正向命令的原始 payload 重放。
pub trait SagaStep: Send + Sync {
    /// 正向步骤的业务命令类型。
    type Command: Send;
    /// 正向步骤成功后的业务产物类型。
    type Success: Send;
    /// 补偿步骤的业务命令类型。
    type Compensation: Send;

    /// 业务作用：执行正向业务并返回可由 Saga Runtime 持久化的确定性处理结论。
    ///
    /// 实现必须把全部业务写留在框架提供的本地事务内；确定性拒绝返回 `Rejected` 而不是
    /// `Err`，因为拒绝事实需要与 Inbox、结果事件一起提交；只有本次尝试没有得到任何
    /// 可提交结论时才返回 `Retryable` 使事务回滚。
    ///
    /// 参数说明：
    /// - `context`: 携带稳定 `saga_id`、跨 attempt 的 `effect_id`、本 attempt 的 `command_id`
    ///   与 definition 版本绑定；业务去重必须使用 `effect_id`。
    /// - `command`: 本步骤的业务输入。
    ///
    /// 返回：成功返回 `Succeeded` 及产物；确定性业务拒绝返回 `Rejected`；外部效果未定返回
    /// `Unknown`（仅当 definition 允许）；不变量破坏返回 `Halted`；临时故障返回 `Retryable`
    /// 并使本地事务回滚且不 ACK 输入消息。
    fn execute(
        &self,
        context: &SagaContext,
        command: Self::Command,
    ) -> impl std::future::Future<Output = SagaResult<Self::Success>> + Send;

    /// 业务作用：幂等撤销本步骤实际产生的正向效果，作为显式业务补偿。
    ///
    /// 补偿必须按 `effect_id` 幂等：同一补偿可能被重复投递，重复执行不得造成二次退款、
    /// 释放非本 Saga 的资源或重复取消。补偿没有“拒绝”语义——无法完成时必须返回 `Halted`
    /// 冻结并转人工，不能通过失败去触发新一轮上游补偿。
    ///
    /// 参数说明：
    /// - `context`: 携带原 Saga 身份、跨 attempt 稳定的补偿 `effect_id` 与本 attempt 的
    ///   `command_id`；业务去重使用前者，投递审计使用后者。
    /// - `command`: 只包含本步骤曾成功产生且允许撤销的凭据。
    ///
    /// 返回：撤销完成或此前已撤销返回 `Succeeded`；外部撤销结果未定返回 `Unknown`；
    /// 不变量破坏返回 `Halted`；临时故障返回 `Retryable` 并使本地事务回滚。
    fn compensate(
        &self,
        context: &SagaContext,
        command: Self::Compensation,
    ) -> impl std::future::Future<Output = CompensationResult> + Send;
}

/// 业务作用：为 `externally_cancellable` 步骤提供取消能力，使超时取消屏障能得到真实裁决。
///
/// 只有 definition 声明为外部可取消的步骤才需要实现本 trait；框架**绝不允许**对没有
/// 取消实现的外部步骤伪造 `CancelConfirmed`，缺少实现的 definition 在启动预检失败。
pub trait SagaCancelStep: Send + Sync {
    /// 取消步骤的业务命令类型；v1 hosted adapter 传 JSON null，通常使用 `()`，并按
    /// `SagaContext::target_effect_id()` 从参与方本地事实恢复外部凭据。
    type Command: Send;

    /// 业务作用：尝试阻止本步骤的正向效果发生，并如实回报真实裁决。
    ///
    /// 实现必须在与正向执行相同的串行化点上裁决，使结果恰好收敛为三者之一；调用远端
    /// cancel API 时必须使用 `context.effect_id()` 作为取消操作的稳定幂等键，并用
    /// `context.target_effect_id()` 定位原 execute 请求；远端已生效但本地事务 COMMIT 失败会重投
    /// 同一取消，不能因此产生第二份外部副作用。
    /// 已提交外部 intent 或调用在途时必须返回 `ResolutionPending`，不得谎报取消成功——
    /// 谎报会让 Orchestrator 提前进入补偿，而远端效果随后仍可能生效。
    ///
    /// 参数说明：
    /// - `context`: 携带取消操作的稳定 `effect_id` 和原 execute 的 `target_effect_id`。
    /// - `command`: 取消所需的步骤身份信息。
    ///
    /// 返回：成功建立准入屏障返回 `CancelConfirmed`；正向已有确定终态返回 `AlreadyTerminal`；
    /// 效果未知返回 `ResolutionPending`；临时故障返回 `Retryable` 并使本地事务回滚。
    fn cancel(
        &self,
        context: &SagaContext,
        command: Self::Command,
    ) -> impl std::future::Future<Output = CancelResult> + Send;
}

/// 业务作用：为 `poll` 解决模式的步骤提供状态查询能力，把 `Unknown` 收敛为唯一裁决。
///
/// 查询必须排除“迟到生效”：只读查询返回“无此记录”不等于该请求永远不会生效，
/// 因此实现只有在供应商请求有效期结束、或使用了裁决型接口（以同一幂等键 void/cancel-inquiry
/// 保证“取消或已生效”二择一）之后，才可以给出 `Rejected` 结论。
pub trait SagaResolveStep: Send + Sync {
    /// 查询步骤的业务命令类型；v1 hosted adapter 传 JSON null，通常使用 `()` 或能接收 null
    /// 的类型，并按 `SagaContext::target_effect_id()` 从参与方本地事实恢复查询凭据。
    type Command: Send;
    /// 查询确认成功时返回的业务产物类型。
    type Success: Send;

    /// 业务作用：向外部系统确认此前未知结果的真实终态。
    ///
    /// 参数说明：
    /// - `context`: 携带查询操作的稳定 `effect_id`，以及 gate 裁决方向对应的
    ///   `target_phase/target_effect_id`；查询必须使用后者定位原始请求。
    /// - `command`: 查询所需的步骤身份与外部凭据引用。
    ///
    /// 返回：确认成功返回 `Succeeded`；在排除迟到生效后确认未发生返回 `Rejected`；
    /// 仍无法裁决返回 `Unknown` 继续消耗解决预算；不变量破坏返回 `Halted`；
    /// 临时故障返回 `Retryable`。
    fn resolve(
        &self,
        context: &SagaContext,
        command: Self::Command,
    ) -> impl std::future::Future<Output = SagaResult<Self::Success>> + Send;
}
