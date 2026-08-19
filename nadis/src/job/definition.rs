//! Job 定义模型：业务声明的调度、并发、重试与 Fanout 合同，以及用于同修订冲突检测的规范摘要。
//!
//! `definition_digest` 覆盖全部影响执行语义的字段，是相同 `definition_revision` 下的冲突判据：两节点若
//! 用同一修订号提交不同语义的定义，摘要不同即被拒绝，避免同一任务在集群中出现两套行为。

use crate::error::{NasaRedisError, Result};
use crate::idempotent::wire::framed_digest;
use crate::job::identifiers::worker_key;
use crate::job::model::{
    JobConcurrency, JobFanoutFailurePolicy, JobMisfire, JobScheduleType, JobTrigger, JobWireCodec,
};
use crate::job::names::require_name;
use crate::job::trigger::validate_cron_compatibility;

/// 已冻结并校验的任务定义；字段私有，只能经 builder 构造以保证不变量与摘要一致。
#[derive(Debug, Clone)]
pub struct JobDefinition {
    qualifier: String,
    name: String,
    worker_name: String,
    trigger: JobTrigger,
    schedule_type: JobScheduleType,
    cron: String,
    zone: String,
    interval_ms: u64,
    concurrency: JobConcurrency,
    misfire: JobMisfire,
    timeout_ms: u64,
    max_attempts: u32,
    retry_delay_ms: u64,
    contract_revision: i64,
    schema_id: String,
    codecs: Vec<JobWireCodec>,
    fanout_receipt_timeout_ms: u64,
    fanout_receipt_max_retries: u32,
    fanout_failure_policy: JobFanoutFailurePolicy,
    definition_revision: i64,
    worker_key: String,
    wire_codecs: String,
    definition_digest: String,
}

impl JobDefinition {
    /// 业务作用：创建以稳定默认值预填的定义 builder。
    ///
    /// 参数说明：
    /// - `name`: 任务名，构造时按名称合同校验。
    ///
    /// 返回：可链式配置调度与合同的 builder。
    pub fn builder(name: impl Into<String>) -> JobDefinitionBuilder {
        JobDefinitionBuilder::new(name.into())
    }

    /// 业务作用：返回定义强绑定的语言无关 source id。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：canonical qualifier，未显式配置时为 `primary`。
    pub fn qualifier(&self) -> &str {
        &self.qualifier
    }

    /// 业务作用：返回任务名。参数说明: 无。返回：已校验任务名。
    pub fn name(&self) -> &str {
        &self.name
    }
    /// 业务作用：返回 Worker 能力名。参数说明: 无。返回：已校验能力名。
    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }
    /// 业务作用：返回触发类型。参数说明: 无。返回：触发类型。
    pub fn trigger(&self) -> JobTrigger {
        self.trigger
    }
    /// 业务作用：返回调度类型。参数说明: 无。返回：解析后的调度类型。
    pub fn schedule_type(&self) -> JobScheduleType {
        self.schedule_type
    }
    /// 业务作用：返回 cron 表达式文本。参数说明: 无。返回：非 CRON 时为空串。
    pub fn cron(&self) -> &str {
        &self.cron
    }
    /// 业务作用：返回时区 ID。参数说明: 无。返回：时区 ID 文本（默认 UTC）。
    pub fn zone(&self) -> &str {
        &self.zone
    }
    /// 业务作用：返回固定周期毫秒。参数说明: 无。返回：非固定周期调度时为 0。
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }
    /// 业务作用：返回并发策略。参数说明: 无。返回：并发策略。
    pub fn concurrency(&self) -> JobConcurrency {
        self.concurrency
    }
    /// 业务作用：返回误触发策略。参数说明: 无。返回：误触发策略。
    pub fn misfire(&self) -> JobMisfire {
        self.misfire
    }
    /// 业务作用：返回单次执行超时毫秒。参数说明: 无。返回：正数超时。
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }
    /// 业务作用：返回最大 attempt 数。参数说明: 无。返回：正数 attempt 上限。
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
    /// 业务作用：返回重试基础延迟毫秒。参数说明: 无。返回：正数延迟。
    pub fn retry_delay_ms(&self) -> u64 {
        self.retry_delay_ms
    }
    /// 业务作用：返回契约修订号。参数说明: 无。返回：正数修订号。
    pub fn contract_revision(&self) -> i64 {
        self.contract_revision
    }
    /// 业务作用：返回 schema 标识。参数说明: 无。返回：未显式指定时回退为能力名。
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }
    /// 业务作用：返回排序去重后的线编码集合。参数说明: 无。返回：非空线编码列表。
    pub fn codecs(&self) -> &[JobWireCodec] {
        &self.codecs
    }
    /// 业务作用：返回 Fanout 接收回执超时毫秒。参数说明: 无。返回：正数超时。
    pub fn fanout_receipt_timeout_ms(&self) -> u64 {
        self.fanout_receipt_timeout_ms
    }
    /// 业务作用：返回 Fanout 接收回执最大重发数。参数说明: 无。返回：非负重发上限。
    pub fn fanout_receipt_max_retries(&self) -> u32 {
        self.fanout_receipt_max_retries
    }
    /// 业务作用：返回 Fanout 失败策略。参数说明: 无。返回：失败策略。
    pub fn fanout_failure_policy(&self) -> JobFanoutFailurePolicy {
        self.fanout_failure_policy
    }
    /// 业务作用：返回定义修订号。参数说明: 无。返回：正数修订号。
    pub fn definition_revision(&self) -> i64 {
        self.definition_revision
    }
    /// 业务作用：返回完整 Worker 摘要键。参数说明: 无。返回：64 位 hex worker key。
    pub fn worker_key(&self) -> &str {
        &self.worker_key
    }
    /// 业务作用：返回排序后的线编码文本。参数说明: 无。返回：如 `JSON,PROTOBUF`。
    pub fn wire_codecs(&self) -> &str {
        &self.wire_codecs
    }
    /// 业务作用：返回规范定义摘要，用于同修订号冲突检测。参数说明: 无。返回：64 位 hex 摘要。
    pub fn definition_digest(&self) -> &str {
        &self.definition_digest
    }
}

/// 任务定义 builder；默认值与稳定合同一致，只在 `build` 时统一解析与校验。
#[derive(Debug, Clone)]
pub struct JobDefinitionBuilder {
    qualifier: String,
    name: String,
    worker_name: Option<String>,
    trigger: JobTrigger,
    cron: String,
    zone: String,
    fixed_rate_ms: u64,
    fixed_delay_ms: u64,
    concurrency: JobConcurrency,
    misfire: JobMisfire,
    timeout_ms: u64,
    max_attempts: u32,
    retry_delay_ms: u64,
    contract_revision: i64,
    schema_id: String,
    codecs: Vec<JobWireCodec>,
    fanout_receipt_timeout_ms: u64,
    fanout_receipt_max_retries: u32,
    fanout_failure_policy: JobFanoutFailurePolicy,
    definition_revision: i64,
}

impl JobDefinitionBuilder {
    /// 业务作用：以稳定默认值创建 builder。参数说明：`name` 任务名。返回：预填默认的 builder。
    fn new(name: String) -> Self {
        Self {
            qualifier: "primary".to_owned(),
            name,
            worker_name: None,
            trigger: JobTrigger::Scheduled,
            cron: String::new(),
            zone: "UTC".to_owned(),
            fixed_rate_ms: 0,
            fixed_delay_ms: 0,
            concurrency: JobConcurrency::SerialQueue,
            misfire: JobMisfire::FireOnceNow,
            timeout_ms: 120_000,
            max_attempts: 3,
            retry_delay_ms: 10_000,
            contract_revision: 1,
            schema_id: String::new(),
            codecs: vec![JobWireCodec::Json],
            fanout_receipt_timeout_ms: 2_000,
            fanout_receipt_max_retries: 3,
            fanout_failure_policy: JobFanoutFailurePolicy::ReassignOnFailure,
            definition_revision: 1,
        }
    }

    /// 业务作用：把定义强路由到指定 Redis source；运行期不会按 Handler 结果改选数据源。
    ///
    /// 参数说明：
    /// - `qualifier`: 语言无关 source id；空值规范化为 `primary`。
    ///
    /// 返回：更新后的 builder。
    pub fn qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = qualifier.into();
        self
    }

    /// 业务作用：设置 Worker 能力名。参数说明：`worker_name` 能力名。返回：更新后的 builder。
    pub fn worker_name(mut self, worker_name: impl Into<String>) -> Self {
        self.worker_name = Some(worker_name.into());
        self
    }
    /// 业务作用：声明 CRON 调度并在 builder 边界拒绝本地无法解析的表达式或命名时区。
    ///
    /// 参数说明：`cron` 为六段表达式，`zone` 为 IANA 时区 ID。
    ///
    /// 返回：语法与时区可解析时返回更新后的 builder；否则返回配置错误。
    pub fn cron(mut self, cron: impl Into<String>, zone: impl Into<String>) -> Result<Self> {
        let cron = cron.into();
        let zone = zone.into();
        validate_cron_compatibility(cron.trim(), zone.trim())?;
        self.cron = cron.trim().to_owned();
        self.zone = zone.trim().to_owned();
        Ok(self)
    }

    /// 业务作用：显式声明只允许手工触发，并清除其它自动调度字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：更新后的 builder。
    pub fn manual(mut self) -> Self {
        self.trigger = JobTrigger::Scheduled;
        self.cron.clear();
        self.fixed_rate_ms = 0;
        self.fixed_delay_ms = 0;
        self
    }

    /// 业务作用：声明只作为 Fanout Worker 的定义，不进入普通调度或手工触发。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：更新后的 builder。
    pub fn fanout_only(mut self) -> Self {
        self.trigger = JobTrigger::FanoutOnly;
        self.cron.clear();
        self.fixed_rate_ms = 0;
        self.fixed_delay_ms = 0;
        self
    }
    /// 业务作用：声明固定速率调度。参数说明：`interval_ms` 周期毫秒。返回：更新后的 builder。
    pub fn fixed_rate_ms(mut self, interval_ms: u64) -> Self {
        self.fixed_rate_ms = interval_ms;
        self
    }
    /// 业务作用：声明固定延迟调度。参数说明：`interval_ms` 周期毫秒。返回：更新后的 builder。
    pub fn fixed_delay_ms(mut self, interval_ms: u64) -> Self {
        self.fixed_delay_ms = interval_ms;
        self
    }
    /// 业务作用：设置触发类型。参数说明：`trigger` 触发类型。返回：更新后的 builder。
    pub fn trigger(mut self, trigger: JobTrigger) -> Self {
        self.trigger = trigger;
        self
    }
    /// 业务作用：设置并发策略。参数说明：`concurrency` 并发策略。返回：更新后的 builder。
    pub fn concurrency(mut self, concurrency: JobConcurrency) -> Self {
        self.concurrency = concurrency;
        self
    }
    /// 业务作用：设置误触发策略。参数说明：`misfire` 策略。返回：更新后的 builder。
    pub fn misfire(mut self, misfire: JobMisfire) -> Self {
        self.misfire = misfire;
        self
    }
    /// 业务作用：设置单次执行超时。参数说明：`timeout_ms` 超时毫秒。返回：更新后的 builder。
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    /// 业务作用：设置最大 attempt。参数说明：`max_attempts` 上限。返回：更新后的 builder。
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
    /// 业务作用：设置重试基础延迟。参数说明：`retry_delay_ms` 延迟毫秒。返回：更新后的 builder。
    pub fn retry_delay_ms(mut self, retry_delay_ms: u64) -> Self {
        self.retry_delay_ms = retry_delay_ms;
        self
    }
    /// 业务作用：设置契约修订号。参数说明：`contract_revision` 修订号。返回：更新后的 builder。
    pub fn contract_revision(mut self, contract_revision: i64) -> Self {
        self.contract_revision = contract_revision;
        self
    }
    /// 业务作用：设置 schema 标识。参数说明：`schema_id` 标识。返回：更新后的 builder。
    pub fn schema_id(mut self, schema_id: impl Into<String>) -> Self {
        self.schema_id = schema_id.into();
        self
    }
    /// 业务作用：设置线编码集合。参数说明：`codecs` 线编码列表。返回：更新后的 builder。
    pub fn codecs(mut self, codecs: impl IntoIterator<Item = JobWireCodec>) -> Self {
        self.codecs = codecs.into_iter().collect();
        self
    }
    /// 业务作用：设置 Fanout 接收回执参数。参数说明：`timeout_ms` 超时、`max_retries` 重发上限。返回：更新后的 builder。
    pub fn fanout_receipt(mut self, timeout_ms: u64, max_retries: u32) -> Self {
        self.fanout_receipt_timeout_ms = timeout_ms;
        self.fanout_receipt_max_retries = max_retries;
        self
    }
    /// 业务作用：设置 Fanout 失败策略。参数说明：`policy` 失败策略。返回：更新后的 builder。
    pub fn fanout_failure_policy(mut self, policy: JobFanoutFailurePolicy) -> Self {
        self.fanout_failure_policy = policy;
        self
    }
    /// 业务作用：设置定义修订号。参数说明：`revision` 修订号。返回：更新后的 builder。
    pub fn definition_revision(mut self, revision: i64) -> Self {
        self.definition_revision = revision;
        self
    }

    /// 业务作用：解析并校验全部字段，冻结定义并计算规范摘要；任一不变量不成立即拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：定义合法时返回冻结定义；名称非法、调度声明冲突、正数字段非正或线编码为空时返回配置错误。
    pub fn build(self) -> Result<JobDefinition> {
        let qualifier_source = if self.qualifier.trim().is_empty() {
            "primary"
        } else {
            self.qualifier.trim()
        };
        let qualifier = require_name(qualifier_source, "qualifier")?;
        if qualifier.contains(':') {
            return Err(cfg("qualifier 不得包含 ':'"));
        }
        let name = require_name(&self.name, "name")?;
        // FANOUT_ONLY 或未显式给出 Worker 名时回退为任务名，避免空能力名破坏 dispatch 路由键。
        let worker_source = if self.trigger == JobTrigger::FanoutOnly
            || self
                .worker_name
                .as_deref()
                .is_none_or(|w| w.trim().is_empty())
        {
            name.clone()
        } else {
            self.worker_name.clone().unwrap()
        };
        let worker_name = require_name(&worker_source, "worker_name")?;
        let schedule_type = resolve_schedule_type(&self)?;
        let cron = self.cron.trim().to_owned();
        let interval_ms = match schedule_type {
            JobScheduleType::FixedRate => self.fixed_rate_ms,
            JobScheduleType::FixedDelay => self.fixed_delay_ms,
            _ => 0,
        };
        if interval_ms > i64::MAX as u64 {
            return Err(cfg("固定调度间隔超出 Redis 毫秒时间范围"));
        }
        let schema_id = if self.schema_id.trim().is_empty() {
            worker_name.clone()
        } else {
            self.schema_id.trim().to_owned()
        };
        if self.codecs.is_empty() {
            return Err(cfg("codecs 不能为空"));
        }
        // 线编码去重并按名称排序：wireCodecs 与摘要 codec 段都要求确定顺序，否则同集合不同书写顺序会
        // 产生不同定义摘要，误报冲突。
        let mut codec_names: Vec<&'static str> =
            self.codecs.iter().map(|c| c.wire_name()).collect();
        codec_names.sort_unstable();
        codec_names.dedup();
        let mut codecs: Vec<JobWireCodec> = codec_names
            .iter()
            .map(|n| JobWireCodec::parse(n).expect("wire_name 恒可解析"))
            .collect();
        codecs.dedup();

        require_positive_u64(self.timeout_ms, "timeout_ms")?;
        require_positive_u32(self.max_attempts, "max_attempts")?;
        require_positive_u64(self.retry_delay_ms, "retry_delay_ms")?;
        require_positive_i64(self.contract_revision, "contract_revision")?;
        require_positive_u64(self.fanout_receipt_timeout_ms, "fanout_receipt_timeout_ms")?;
        require_positive_i64(self.definition_revision, "definition_revision")?;

        let wire_codecs = codec_names.join(",");
        let worker_key = worker_key(&worker_name);
        let definition_digest = compute_digest(
            &name,
            &worker_name,
            self.trigger,
            schedule_type,
            &cron,
            &self.zone,
            interval_ms,
            self.concurrency,
            self.misfire,
            self.timeout_ms,
            self.max_attempts,
            self.retry_delay_ms,
            self.contract_revision,
            &schema_id,
            &codec_names,
            self.fanout_receipt_timeout_ms,
            self.fanout_receipt_max_retries,
            self.fanout_failure_policy,
        );

        Ok(JobDefinition {
            qualifier,
            name,
            worker_name,
            trigger: self.trigger,
            schedule_type,
            cron,
            zone: self.zone,
            interval_ms,
            concurrency: self.concurrency,
            misfire: self.misfire,
            timeout_ms: self.timeout_ms,
            max_attempts: self.max_attempts,
            retry_delay_ms: self.retry_delay_ms,
            contract_revision: self.contract_revision,
            schema_id,
            codecs,
            fanout_receipt_timeout_ms: self.fanout_receipt_timeout_ms,
            fanout_receipt_max_retries: self.fanout_receipt_max_retries,
            fanout_failure_policy: self.fanout_failure_policy,
            definition_revision: self.definition_revision,
            worker_key,
            wire_codecs,
            definition_digest,
        })
    }
}

/// 业务作用：按触发类型与声明的调度参数解析调度类型；互斥或非法组合拒绝。
///
/// 参数说明：
/// - `builder`: 待解析的定义 builder。
///
/// 返回：合法时返回调度类型；FANOUT_ONLY 声明调度、多种调度并存时返回配置错误。cron 引擎等价性由调度
/// 阶段的登记门禁按黄金向量校验，本处只按是否非空判定 CRON 类型。
fn resolve_schedule_type(builder: &JobDefinitionBuilder) -> Result<JobScheduleType> {
    let has_cron = !builder.cron.trim().is_empty();
    if builder.trigger == JobTrigger::FanoutOnly {
        if has_cron || builder.fixed_rate_ms > 0 || builder.fixed_delay_ms > 0 {
            return Err(cfg("FANOUT_ONLY 任务不能声明调度"));
        }
        return Ok(JobScheduleType::FanoutOnly);
    }
    let configured = u32::from(has_cron)
        + u32::from(builder.fixed_rate_ms > 0)
        + u32::from(builder.fixed_delay_ms > 0);
    if configured > 1 {
        return Err(cfg("cron、fixed_rate 与 fixed_delay 互斥"));
    }
    if configured == 0 {
        return Ok(JobScheduleType::Manual);
    }
    if has_cron {
        return Ok(JobScheduleType::Cron);
    }
    Ok(if builder.fixed_rate_ms > 0 {
        JobScheduleType::FixedRate
    } else {
        JobScheduleType::FixedDelay
    })
}

/// 业务作用：按固定字段顺序计算定义摘要；顺序与编码是持久协议，不可调整。
#[allow(clippy::too_many_arguments)]
fn compute_digest(
    name: &str,
    worker_name: &str,
    trigger: JobTrigger,
    schedule_type: JobScheduleType,
    cron: &str,
    zone: &str,
    interval_ms: u64,
    concurrency: JobConcurrency,
    misfire: JobMisfire,
    timeout_ms: u64,
    max_attempts: u32,
    retry_delay_ms: u64,
    contract_revision: i64,
    schema_id: &str,
    codec_names: &[&str],
    fanout_receipt_timeout_ms: u64,
    fanout_receipt_max_retries: u32,
    fanout_failure_policy: JobFanoutFailurePolicy,
) -> String {
    // codec 段用前导逗号累加拼接（"",",JSON",...），与协议一致；与 wireCodecs 的普通逗号连接不同。
    let mut codec_field = String::new();
    for codec in codec_names {
        codec_field.push(',');
        codec_field.push_str(codec);
    }
    let interval = interval_ms.to_string();
    let timeout = timeout_ms.to_string();
    let attempts = max_attempts.to_string();
    let retry = retry_delay_ms.to_string();
    let contract = contract_revision.to_string();
    let receipt_timeout = fanout_receipt_timeout_ms.to_string();
    let receipt_retries = fanout_receipt_max_retries.to_string();
    let fields: [&str; 18] = [
        name,
        worker_name,
        trigger.wire_name(),
        schedule_type.wire_name(),
        cron,
        zone,
        &interval,
        concurrency.wire_name(),
        misfire.wire_name(),
        &timeout,
        &attempts,
        &retry,
        &contract,
        schema_id,
        &codec_field,
        &receipt_timeout,
        &receipt_retries,
        fanout_failure_policy.wire_name(),
    ];
    hex::encode(framed_digest(
        &fields.iter().map(|f| f.as_bytes()).collect::<Vec<_>>(),
    ))
}

/// 业务作用：校验 u64 字段为正。参数说明：`value` 值、`name` 字段名。返回：非正时返回配置错误。
fn require_positive_u64(value: u64, name: &str) -> Result<()> {
    if value == 0 {
        return Err(cfg(&format!("{name} 必须大于 0")));
    }
    Ok(())
}

/// 业务作用：校验 u32 字段为正。参数说明：`value` 值、`name` 字段名。返回：非正时返回配置错误。
fn require_positive_u32(value: u32, name: &str) -> Result<()> {
    if value == 0 {
        return Err(cfg(&format!("{name} 必须大于 0")));
    }
    Ok(())
}

/// 业务作用：校验 i64 字段为正。参数说明：`value` 值、`name` 字段名。返回：非正时返回配置错误。
fn require_positive_i64(value: i64, name: &str) -> Result<()> {
    if value <= 0 {
        return Err(cfg(&format!("{name} 必须大于 0")));
    }
    Ok(())
}

/// 业务作用：构造带 `job definition` 前缀的配置错误。参数说明：`message` 摘要。返回：配置错误。
fn cfg(message: &str) -> NasaRedisError {
    crate::job::JobError::Config(format!("definition {message}")).into()
}
