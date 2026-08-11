//! 调度驱动的 Saga 发起：把周期任务（对账、清算、批量处置）安全接到幂等创建。
//!
//! 分工边界：集群调度器（leader gate + FireLog claim）只解决"谁来触发本名义时刻"，
//! **不是 exactly-once**；真正的去重由三件事共同承担——稳定的名义调度时刻、由
//! `任务名 + 名义调度时刻 + 对象稳定身份` 派生的业务幂等键，以及创建请求 canonical
//! 摘要的不变性（同 key 摘要漂移 fail-closed，绝不把另一批输入伪装成重复）。
//!
//! 崩溃恢复模型：claim 成功但进程随后崩溃时，**由下一次触发重扫已提交业务事实补漏**；
//! 本模块不在进程内保留任何跨周期状态——已创建的实例被业务幂等键吸收，漏掉的对象在
//! 重扫的条目列表里自然重新出现。

use nasaga_core::{BusinessKey, SagaId, TenantId, TriggerKind};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::orchestrator::{Orchestrator, StartOutcome, StartSagaRequest};

/// 业务作用：从"任务名 + 名义调度时刻 + 对象稳定身份"确定性派生批次内业务幂等键。
///
/// 对象身份参与 SHA-256 后取前 32 位十六进制：无论业务对象 id 多长、含何种字符，
/// 派生键都有界且落在 `BusinessKey` 封闭字符集内；同一批次重复触发得到完全相同的键，
/// 由创建入口的业务唯一键把重复吸收成同一实例。
///
/// 参数说明：
/// - `task_name`: 调度任务稳定名称。
/// - `scheduled_at_ms`: 名义调度时刻（epoch 毫秒）——用调度表的名义时刻，不用实际
///   触发时刻；否则重试触发会派生出第二个批次身份。
/// - `object_id`: 批次内业务对象的稳定身份。
///
/// 返回：确定性业务幂等键；任务名越界导致整体键非法时返回错误文本。
pub fn derive_scheduled_business_key(
    task_name: &str,
    scheduled_at_ms: i64,
    object_id: &str,
) -> Result<BusinessKey, String> {
    let digest = Sha256::digest(object_id.as_bytes());
    let key = format!(
        "sched:{task_name}@{scheduled_at_ms}:{}",
        hex::encode(&digest[..16])
    );
    BusinessKey::new(&key).map_err(|violation| violation.code().to_string())
}

/// 业务作用：描述一次名义调度时刻批次的固定输入与有界预算。
#[derive(Debug, Clone)]
pub struct ScheduledBatchSpec<'a> {
    /// 调度任务稳定名称，参与业务幂等键派生。
    pub task_name: &'a str,
    /// 名义调度时刻（epoch 毫秒），批次身份的一部分。
    pub scheduled_at_ms: i64,
    /// 目标 workflow 名称。
    pub workflow: &'a nasaga_core::WorkflowName,
    /// 目标 definition 版本。
    pub version: nasaga_core::DefinitionVersion,
    /// 租户身份。
    pub tenant: &'a TenantId,
    /// 单批次发起数量上限；未处理对象由下一次触发的重扫继续。
    pub max_items: u32,
    /// 单批次时间预算（毫秒）；超出即停止，不在进程内滞留。
    pub time_budget_ms: i64,
    /// 当前时刻（epoch 毫秒），由调用方注入统一时钟。
    pub now_ms: i64,
}

/// 业务作用：批次内单个业务对象的发起输入。
#[derive(Debug, Clone)]
pub struct ScheduledItem {
    /// 对象稳定身份，参与业务幂等键派生。
    pub object_id: String,
    /// 首步 execute 命令的业务输入；同一对象在同一批次内重试必须给出相同 payload，
    /// 创建摘要漂移会 fail-closed。
    pub first_command_payload: Option<serde_json::Value>,
    /// 实例级业务 deadline（epoch 毫秒）；为空不设全局期限。
    pub deadline_at_ms: Option<i64>,
}

/// 业务作用：一次批次发起的低基数结果，供任务层指标与告警。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScheduledBatchReport {
    /// 本批真实创建的实例数。
    pub started: u32,
    /// 命中业务幂等键、被吸收为既有实例的重复数。
    pub duplicates: u32,
    /// 因数量上限、时间预算或失去领导权而未处理的对象数；由下一次触发重扫继续。
    pub unprocessed: u32,
    /// 键派生失败等可继续观察的首个稳定原因；无失败为空。创建路径的失败以带稳定
    /// 原因码的错误返回（本报告不随 `Err` 返回）。
    pub first_error: Option<&'static str>,
}

impl Orchestrator {
    /// 业务作用：在一次已取得触发权的名义调度时刻内，按预算幂等发起一批 Saga。
    ///
    /// 触发权合同：调用方必须已持有领导权与该名义时刻的 FireLog claim；`still_leader`
    /// 在**每个对象之间**复验——失去领导权立即停止领取新项，已提交创建的实例由 Saga
    /// 自身推进，不随领导权变化回滚。重复触发（重试、换届重扫、崩溃后补漏）由派生
    /// 业务幂等键吸收；同 key 的创建摘要漂移由创建入口 fail-closed，以带稳定原因码的
    /// 错误停止本批（输入口径漂移是配置事故，不是可跳过的单行失败）。
    ///
    /// 参数说明：
    /// - `spec`: 批次身份、目标 workflow 与有界预算。
    /// - `items`: 本名义时刻扫描出的业务对象列表（来自已提交业务事实）。
    /// - `still_leader`: 领导权探针；返回假即停止领取新项。
    ///
    /// 返回：批次结果；创建路径的基础设施失败返回错误（已创建实例保持已提交）。
    pub async fn start_scheduled_batch(
        &self,
        spec: &ScheduledBatchSpec<'_>,
        items: &[ScheduledItem],
        mut still_leader: impl FnMut() -> bool,
    ) -> anyhow::Result<ScheduledBatchReport> {
        if spec.max_items == 0 || spec.time_budget_ms <= 0 {
            anyhow::bail!("scheduled batch requires positive item and time budgets");
        }
        if spec.task_name.is_empty()
            || spec.task_name.len() > 64
            || !spec
                .task_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            anyhow::bail!("scheduled task name must be a bounded canonical identifier");
        }
        let mut report = ScheduledBatchReport::default();
        let started_at = std::time::Instant::now();
        let budget = std::time::Duration::from_millis(spec.time_budget_ms as u64);
        for (index, item) in items.iter().enumerate() {
            // 失去领导权立即停止领取新项:触发权威已经易主,继续发起会与新 leader 的
            // 同批次重扫并发;已创建实例交由业务幂等键与状态机保证安全。
            if !still_leader() {
                report.unprocessed = (items.len() - index) as u32;
                return Ok(report);
            }
            if report.started + report.duplicates >= spec.max_items
                || started_at.elapsed() >= budget
            {
                // 预算耗尽:未处理对象不在进程内滞留,由下一次触发的业务事实重扫继续。
                report.unprocessed = (items.len() - index) as u32;
                return Ok(report);
            }
            let business_key = match derive_scheduled_business_key(
                spec.task_name,
                spec.scheduled_at_ms,
                &item.object_id,
            ) {
                Ok(key) => key,
                Err(_) => {
                    report.first_error = Some("scheduled_key_derivation_failed");
                    report.unprocessed = (items.len() - index) as u32;
                    return Ok(report);
                }
            };
            // saga_id 每次随机:幂等完全由业务键承担,重复触发命中唯一键即返回既有实例,
            // 不产生第二次首步命令。触发身份复用派生键文本,重放的初始 transition 触发
            // 也保持稳定。
            let saga_id = SagaId::new(Uuid::new_v4().to_string())
                .map_err(|violation| anyhow::anyhow!("saga id violation: {}", violation.code()))?;
            let outcome = self
                .start_saga(&StartSagaRequest {
                    saga_id: &saga_id,
                    tenant: spec.tenant,
                    workflow: spec.workflow,
                    version: spec.version,
                    business_key: &business_key,
                    deadline_at_ms: item.deadline_at_ms,
                    trigger_kind: TriggerKind::Timer,
                    trigger_id: business_key.as_str(),
                    first_command_payload: item.first_command_payload.clone(),
                    now_ms: spec.now_ms,
                })
                .await;
            match outcome {
                Ok(StartOutcome::Started(_)) => report.started += 1,
                Ok(StartOutcome::AlreadyExists(_)) => report.duplicates += 1,
                Err(error) => {
                    // 同 key 摘要漂移是输入口径事故:继续逐对象重试只会重复失败,
                    // 停止本批;稳定原因并入错误链交给任务层告警(Err 路径不返回报告)。
                    let code = if error.to_string().contains("different start request") {
                        "scheduled_start_digest_drift"
                    } else {
                        "scheduled_start_failed"
                    };
                    return Err(error.context(format!(
                        "scheduled batch stopped at object index {index}: {code}"
                    )));
                }
            }
        }
        Ok(report)
    }
}
