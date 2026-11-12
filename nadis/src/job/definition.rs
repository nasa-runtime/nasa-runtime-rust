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
    /// 已规范化并冻结的 Redis 数据源身份。
    qualifier: String,
    /// 任务的稳定名称，参与调度分片和定义冲突检测。
    name: String,
    /// 承接任务执行的能力名称。
    worker_name: String,
    /// 普通调度任务或仅接收定向分片的 Worker 类型。
    trigger: JobTrigger,
    /// 从互斥调度声明解析出的唯一调度方式。
    schedule_type: JobScheduleType,
    /// 已验证的 CRON 表达式，其它调度方式为空。
    cron: String,
    /// CRON 使用的 IANA 时区，缺省为 UTC。
    zone: String,
    /// 固定速率或固定延迟的周期毫秒数，其它方式为零。
    interval_ms: u64,
    /// 相同任务多个逻辑执行之间的并发策略。
    concurrency: JobConcurrency,
    /// 错过计划时刻时采用的补偿策略。
    misfire: JobMisfire,
    /// 单次执行的正数超时上限，单位毫秒。
    timeout_ms: u64,
    /// 一次逻辑执行允许的最大 attempt 数，包含首次执行。
    max_attempts: u32,
    /// 重试可见时间的基础延迟，单位毫秒。
    retry_delay_ms: u64,
    /// Worker 合同的正数修订号，用于能力兼容检查。
    contract_revision: i64,
    /// 载荷 Schema 标识，未显式声明时使用能力名。
    schema_id: String,
    /// 按线名称排序并去重的非空编码集合。
    codecs: Vec<JobWireCodec>,
    /// Fanout 接收回执的等待时限，单位毫秒。
    fanout_receipt_timeout_ms: u64,
    /// 未收到接收回执时允许的重发次数。
    fanout_receipt_max_retries: u32,
    /// 定向分片目标失联或执行失败时的策略。
    fanout_failure_policy: JobFanoutFailurePolicy,
    /// 定义本身的正数修订号，用于控制更新和删除。
    definition_revision: i64,
    /// 完整 Worker 名称摘要，用作能力 Stream 路由身份。
    worker_key: String,
    /// 排序后的编码名称文本，用于持久合同比较。
    wire_codecs: String,
    /// 固定字段顺序派生的定义摘要，用于同修订号冲突判定。
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

    /// 业务作用：提供持久定义定位和调度分片使用的任务名。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已经规范化并冻结的稳定任务名。
    pub fn name(&self) -> &str {
        &self.name
    }
    /// 业务作用：提供能力登记和执行器匹配使用的 Worker 名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已校验的能力名；仅 Fanout 的定义使用任务名作为能力名。
    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }
    /// 业务作用：区分普通任务与仅接收 Fanout 定向执行的 Worker。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造时冻结的触发类型，运行中不自动切换。
    pub fn trigger(&self) -> JobTrigger {
        self.trigger
    }
    /// 业务作用：提供调度器选择下一逻辑时刻算法所需的唯一调度类型。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已验证互斥声明后确定的调度方式。
    pub fn schedule_type(&self) -> JobScheduleType {
        self.schedule_type
    }
    /// 业务作用：提供 CRON 调度使用的已验证表达式。
    ///
    /// 参数说明：无。
    ///
    /// 返回：去除首尾空白的表达式；其它调度方式返回空串。
    pub fn cron(&self) -> &str {
        &self.cron
    }
    /// 业务作用：提供 CRON 逻辑时刻计算使用的时区标识。
    ///
    /// 参数说明：无。
    ///
    /// 返回：冻结的时区文本，默认 UTC；不读取进程所在机器的本地时区。
    pub fn zone(&self) -> &str {
        &self.zone
    }
    /// 业务作用：提供固定速率或固定延迟调度所需的周期长度。
    ///
    /// 参数说明：无。
    ///
    /// 返回：冻结的周期毫秒数；其它调度类型返回零。
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }
    /// 业务作用：提供同一任务多个逻辑执行之间的准入策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已冻结的并发方式，由调度和完成路径共同执行。
    pub fn concurrency(&self) -> JobConcurrency {
        self.concurrency
    }
    /// 业务作用：提供错过计划触发时刻时的补偿策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：冻结的补偿方式，实际补偿仍受运行配置中的数量和时间窗口限制。
    pub fn misfire(&self) -> JobMisfire {
        self.misfire
    }
    /// 业务作用：提供执行器计算单次尝试截止时间所需的超时上限。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已校验为正数的超时毫秒数，不等同于取消已经发生的外部副作用。
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }
    /// 业务作用：提供重试裁决使用的尝试次数上限。
    ///
    /// 参数说明：无。
    ///
    /// 返回：包含首次执行的正数 attempt 上限。
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
    /// 业务作用：提供失败后计算下一可见时刻的基础等待时间。
    ///
    /// 参数说明：无。
    ///
    /// 返回：正数延迟毫秒数；实际重试时刻还受退避、抖动和运行上限约束。
    pub fn retry_delay_ms(&self) -> u64 {
        self.retry_delay_ms
    }
    /// 业务作用：提供 Worker 能力登记和 Fanout 快照使用的合同修订号。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已冻结的正数合同修订号，与定义本身的修订号独立。
    pub fn contract_revision(&self) -> i64 {
        self.contract_revision
    }
    /// 业务作用：提供载荷解释和执行器兼容检查使用的 Schema 身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：冻结的 Schema 标识，未声明时使用能力名。
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }
    /// 业务作用：提供当前 Worker 接受的线编码集合，用于构造和验证单批载荷合同。
    ///
    /// 参数说明：无。
    ///
    /// 返回：按线名称排序并去重的非空集合；单个 Fanout 批次仍只选择一种编码。
    pub fn codecs(&self) -> &[JobWireCodec] {
        &self.codecs
    }
    /// 业务作用：提供定向投递等待接收回执的时限。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已校验为正数的等待毫秒数，不代表分片执行完成时限。
    pub fn fanout_receipt_timeout_ms(&self) -> u64 {
        self.fanout_receipt_timeout_ms
    }
    /// 业务作用：提供未确认接收时允许重复投递的次数上限。
    ///
    /// 参数说明：无。
    ///
    /// 返回：非负重发上限，零表示不允许额外重发。
    pub fn fanout_receipt_max_retries(&self) -> u32 {
        self.fanout_receipt_max_retries
    }
    /// 业务作用：提供定向目标失联或分片执行失败后的收敛策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前定义中冻结的 Fanout 失败策略。
    pub fn fanout_failure_policy(&self) -> JobFanoutFailurePolicy {
        self.fanout_failure_policy
    }
    /// 业务作用：提供定义登记、更新和删除使用的修订号。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已冻结的正数修订号；相同修订号的内容仍须满足摘要一致性。
    pub fn definition_revision(&self) -> i64 {
        self.definition_revision
    }
    /// 业务作用：提供能力 Stream 路由使用的完整 Worker 名称摘要。
    ///
    /// 参数说明：无。
    ///
    /// 返回：64 个小写十六进制字符，不以截断摘要合并不同能力。
    pub fn worker_key(&self) -> &str {
        &self.worker_key
    }
    /// 业务作用：提供能力登记与持久合同比较使用的确定性编码文本。
    ///
    /// 参数说明：无。
    ///
    /// 返回：按线名称排序去重后以逗号连接的非空文本，例如 `JSON,PROTOBUF`。
    pub fn wire_codecs(&self) -> &str {
        &self.wire_codecs
    }
    /// 业务作用：提供相同定义修订号下检测语义冲突的规范摘要。
    ///
    /// 参数说明：无。
    ///
    /// 返回：由冻结业务字段派生的 64 个小写十六进制字符，字段顺序由持久协议固定。
    pub fn definition_digest(&self) -> &str {
        &self.definition_digest
    }
}

/// 任务定义 builder；默认值与稳定合同一致，只在 `build` 时统一解析与校验。
#[derive(Debug, Clone)]
pub struct JobDefinitionBuilder {
    /// 待规范化的数据源身份，空白值在构造时使用 primary。
    qualifier: String,
    /// 待校验的稳定任务名。
    name: String,
    /// 显式指定的普通任务能力名，未设置时使用任务名。
    worker_name: Option<String>,
    /// 普通任务或仅接收定向分片的触发类别。
    trigger: JobTrigger,
    /// 已通过解析检查但仍需参与互斥判断的 CRON 声明。
    cron: String,
    /// CRON 使用的时区声明，缺省为 UTC。
    zone: String,
    /// 固定速率的周期声明，零表示未启用。
    fixed_rate_ms: u64,
    /// 固定延迟的周期声明，零表示未启用。
    fixed_delay_ms: u64,
    /// 待冻结的任务并发方式。
    concurrency: JobConcurrency,
    /// 待冻结的错过触发时刻补偿方式。
    misfire: JobMisfire,
    /// 待校验为正数的单次尝试超时毫秒数。
    timeout_ms: u64,
    /// 包含首次执行的最大尝试次数，构造时拒绝零。
    max_attempts: u32,
    /// 待校验为正数的基础重试延迟毫秒数。
    retry_delay_ms: u64,
    /// 待校验的 Worker 合同修订号。
    contract_revision: i64,
    /// 显式 Schema 标识，空白时使用最终能力名。
    schema_id: String,
    /// 待排序去重的编码声明，构造时拒绝空集合。
    codecs: Vec<JobWireCodec>,
    /// 待校验为正数的定向接收回执等待毫秒数。
    fanout_receipt_timeout_ms: u64,
    /// 未确认接收时的重发次数上限，允许零。
    fanout_receipt_max_retries: u32,
    /// 目标失联或分片失败后的持久策略声明。
    fanout_failure_policy: JobFanoutFailurePolicy,
    /// 待校验的定义修订号，须为正数。
    definition_revision: i64,
}

impl JobDefinitionBuilder {
    /// 业务作用：为任务声明预填稳定默认值，供后续统一构造和摘要计算。
    ///
    /// 参数说明：
    /// - `name`：调用方提供的原始任务名，在最终构造时规范化和校验。
    ///
    /// 返回：默认手工调度、串行排队、JSON 编码及有界执行参数的构建器，尚未冻结定义。
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

    /// 业务作用：声明普通任务需要匹配的 Worker 能力身份。
    ///
    /// 参数说明：
    /// - `worker_name`：能力名，空白时回退为任务名；仅 Fanout 的定义始终使用任务名。
    ///
    /// 返回：保存声明的构建器，最终名称在 `build` 中规范化。
    pub fn worker_name(mut self, worker_name: impl Into<String>) -> Self {
        self.worker_name = Some(worker_name.into());
        self
    }
    /// 业务作用：声明 CRON 调度并在 builder 边界拒绝本地无法解析的表达式或命名时区。
    ///
    /// 参数说明：
    /// - `cron`：六段 CRON 表达式，在此处验证本地语法和支持的跨实现语义范围。
    /// - `zone`：可解析的 IANA 时区标识，不依赖机器默认时区。
    ///
    /// 返回：语法与时区合法时保存去除首尾空白的声明，否则返回错误；与其它调度声明的冲突在构造时拒绝。
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
    /// 业务作用：声明按固定逻辑周期推进的调度方式。
    ///
    /// 参数说明：
    /// - `interval_ms`：固定速率周期，单位毫秒；零表示不启用此项声明。
    ///
    /// 返回：保存周期的构建器，不清除其它调度字段；互斥冲突及数值范围由 `build` 校验。
    pub fn fixed_rate_ms(mut self, interval_ms: u64) -> Self {
        self.fixed_rate_ms = interval_ms;
        self
    }
    /// 业务作用：声明在前次执行完成后按固定延迟继续调度。
    ///
    /// 参数说明：
    /// - `interval_ms`：完成后的等待毫秒数；零表示不启用此项声明。
    ///
    /// 返回：保存延迟的构建器，不清除其它调度字段；互斥冲突及数值范围由 `build` 校验。
    pub fn fixed_delay_ms(mut self, interval_ms: u64) -> Self {
        self.fixed_delay_ms = interval_ms;
        self
    }
    /// 业务作用：选择普通任务或仅接收定向分片的定义类别。
    ///
    /// 参数说明：
    /// - `trigger`：目标触发类别，不在此方法内清除已有调度声明。
    ///
    /// 返回：更新后的构建器；仅 Fanout 类型与自动调度并存时，最终构造拒绝。
    pub fn trigger(mut self, trigger: JobTrigger) -> Self {
        self.trigger = trigger;
        self
    }
    /// 业务作用：声明同一任务多个逻辑执行之间的并发准入规则。
    ///
    /// 参数说明：
    /// - `concurrency`：由持久调度及完成路径共同执行的并发策略。
    ///
    /// 返回：保存策略的构建器，最终策略进入定义摘要。
    pub fn concurrency(mut self, concurrency: JobConcurrency) -> Self {
        self.concurrency = concurrency;
        self
    }
    /// 业务作用：声明错过计划触发时刻时如何补偿。
    ///
    /// 参数说明：
    /// - `misfire`：错过时刻的处理方式，实际补偿仍受运行配置上限约束。
    ///
    /// 返回：保存补偿策略的构建器，最终策略进入定义摘要。
    pub fn misfire(mut self, misfire: JobMisfire) -> Self {
        self.misfire = misfire;
        self
    }
    /// 业务作用：声明单次尝试允许执行的时间上限。
    ///
    /// 参数说明：
    /// - `timeout_ms`：超时毫秒数，最终构造时必须为正。
    ///
    /// 返回：保存超时声明的构建器，不在此处启动计时或终止任何执行。
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    /// 业务作用：声明一次逻辑执行允许尝试的总次数。
    ///
    /// 参数说明：
    /// - `max_attempts`：包含首次执行的次数上限，最终构造时拒绝零。
    ///
    /// 返回：保存尝试上限的构建器；是否继续重试由运行时结合当前 attempt 裁决。
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
    /// 业务作用：声明失败后计算重试可见时间的基础等待长度。
    ///
    /// 参数说明：
    /// - `retry_delay_ms`：基础延迟毫秒数，最终构造时必须为正。
    ///
    /// 返回：保存延迟的构建器，实际退避与抖动由运行时计算。
    pub fn retry_delay_ms(mut self, retry_delay_ms: u64) -> Self {
        self.retry_delay_ms = retry_delay_ms;
        self
    }
    /// 业务作用：声明 Worker 能力匹配使用的合同修订号。
    ///
    /// 参数说明：
    /// - `contract_revision`：最终构造时须为正的合同修订号，与定义修订号分别管理。
    ///
    /// 返回：保存修订号的构建器，最终值参与能力兼容及定义摘要。
    pub fn contract_revision(mut self, contract_revision: i64) -> Self {
        self.contract_revision = contract_revision;
        self
    }
    /// 业务作用：声明载荷解释和能力匹配使用的 Schema 身份。
    ///
    /// 参数说明：
    /// - `schema_id`：Schema 标识，构造时去除首尾空白；空白声明回退为最终能力名。
    ///
    /// 返回：保存声明的构建器，不依据载荷内容自动推断 Schema。
    pub fn schema_id(mut self, schema_id: impl Into<String>) -> Self {
        self.schema_id = schema_id.into();
        self
    }
    /// 业务作用：声明 Worker 能够接受的线编码集合。
    ///
    /// 参数说明：
    /// - `codecs`：替换原声明的编码集合，构造时排序去重并拒绝空集合。
    ///
    /// 返回：保存声明的构建器，集合顺序不会改变最终规范摘要。
    pub fn codecs(mut self, codecs: impl IntoIterator<Item = JobWireCodec>) -> Self {
        self.codecs = codecs.into_iter().collect();
        self
    }
    /// 业务作用：声明定向分片接收回执的等待和重发边界。
    ///
    /// 参数说明：
    /// - `timeout_ms`：等待接收回执的毫秒数，构造时须为正。
    /// - `max_retries`：未确认接收时允许的额外重发次数，允许零。
    ///
    /// 返回：保存回执参数的构建器，参数不等同于业务执行完成时限。
    pub fn fanout_receipt(mut self, timeout_ms: u64, max_retries: u32) -> Self {
        self.fanout_receipt_timeout_ms = timeout_ms;
        self.fanout_receipt_max_retries = max_retries;
        self
    }
    /// 业务作用：声明定向目标失联或分片执行失败后的处理方式。
    ///
    /// 参数说明：
    /// - `policy`：进入持久定义和摘要的封闭失败策略。
    ///
    /// 返回：保存策略的构建器，尚不创建或迁移任何分片。
    pub fn fanout_failure_policy(mut self, policy: JobFanoutFailurePolicy) -> Self {
        self.fanout_failure_policy = policy;
        self
    }
    /// 业务作用：声明定义登记、更新和删除时使用的修订号。
    ///
    /// 参数说明：
    /// - `revision`：最终构造时须为正的定义修订号，同号定义还必须满足语义摘要一致性。
    ///
    /// 返回：保存修订号的构建器，不在此处发布持久定义。
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
/// 返回：合法时返回唯一调度类型；定向 Worker 声明调度或多种调度并存时返回配置错误。
/// 此处按非空表达式确定 CRON 类型，跨实现调度语义还须通过计划登记门禁。
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
///
/// 参数说明：
/// - `name`：已规范化的任务名。
/// - `worker_name`：最终采用的 Worker 能力名。
/// - `trigger`：普通任务或定向 Worker 的触发类别。
/// - `schedule_type`：解析互斥声明后确定的调度方式。
/// - `cron`：去除首尾空白的 CRON 表达式，其它方式为空。
/// - `zone`：调度使用的时区标识。
/// - `interval_ms`：固定周期毫秒数，其它方式为零。
/// - `concurrency`：任务并发准入策略。
/// - `misfire`：错过触发时刻的补偿策略。
/// - `timeout_ms`：单次尝试的超时毫秒数。
/// - `max_attempts`：包含首次执行的尝试次数上限。
/// - `retry_delay_ms`：重试基础延迟毫秒数。
/// - `contract_revision`：Worker 合同修订号。
/// - `schema_id`：已冻结的载荷 Schema 身份。
/// - `codec_names`：已按线名称排序去重的编码列表。
/// - `fanout_receipt_timeout_ms`：定向接收回执等待毫秒数。
/// - `fanout_receipt_max_retries`：未确认接收时的重发次数上限。
/// - `fanout_failure_policy`：目标失联或分片失败后的策略。
///
/// 返回：18 个协议字段按长度前缀摘要后形成的 64 个小写十六进制字符，不包含定义修订号。
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

/// 业务作用：拒绝定义中本应启用的超时或延迟被配置为零。
///
/// 参数说明：
/// - `value`：待校验的无符号时间数值。
/// - `name`：错误摘要中使用的固定字段名。
///
/// 返回：大于零时通过，否则返回配置错误；此检查不提供其它时间上界保证。
fn require_positive_u64(value: u64, name: &str) -> Result<()> {
    if value == 0 {
        return Err(cfg(&format!("{name} 必须大于 0")));
    }
    Ok(())
}

/// 业务作用：确保任务至少允许一次执行尝试。
///
/// 参数说明：
/// - `value`：待校验的无符号次数上限。
/// - `name`：错误摘要中使用的固定字段名。
///
/// 返回：正数通过，零返回配置错误，不修改调用方数值。
fn require_positive_u32(value: u32, name: &str) -> Result<()> {
    if value == 0 {
        return Err(cfg(&format!("{name} 必须大于 0")));
    }
    Ok(())
}

/// 业务作用：确保定义和 Worker 合同的修订号具有有效的正数身份。
///
/// 参数说明：
/// - `value`：待校验的有符号修订号。
/// - `name`：错误摘要中使用的固定字段名。
///
/// 返回：正数通过，零或负数返回配置错误，不推断其与远端修订号的先后关系。
fn require_positive_i64(value: i64, name: &str) -> Result<()> {
    if value <= 0 {
        return Err(cfg(&format!("{name} 必须大于 0")));
    }
    Ok(())
}

/// 业务作用：为任务定义校验失败添加固定来源提示，并归为任务配置错误。
///
/// 参数说明：
/// - `message`：名称、调度声明或数值约束不满足的原因摘要。
///
/// 返回：添加 `definition` 前缀的任务配置错误，不发布未通过校验的定义。
fn cfg(message: &str) -> NasaRedisError {
    crate::job::JobError::Config(format!("definition {message}")).into()
}
