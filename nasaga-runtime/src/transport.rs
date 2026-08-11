//! Saga 与 `nafka` 的生产 transport 适配。
//!
//! 本模块只组装已经冻结的安全语义：`nafka` owner 负责分区 pause/seek、退避、连续安全
//! 水位和 durability-first DLT；本适配器负责 producer owner 绑定、Orchestrator 本地事务，
//! 并且只在事务提交后调用手动 ACK。两层之间不再依赖错误字符串决定 offset 是否前移。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nafka::{AckMode, GroupSpec, KafkaRecord, NafkaError, SingleConsumer};
use nasaga_core::{DefinitionVersion, ServiceIdentity, StepName, WorkflowName};

use crate::observability::{
    record_kafka_ack, record_kafka_command_ack, record_kafka_command_dlt_requested,
    record_kafka_command_processing, record_kafka_command_retry, record_kafka_dlt_requested,
    record_kafka_processing, record_kafka_retry,
};
use crate::transport_shared::{SagaCommandHandler, SagaResultHandler};
use crate::{
    CommandDeliveryDisposition, CommandDeliveryPolicy, HandleOutcome, Orchestrator,
    ParticipantHandled, ResultDeliveryDisposition, ResultDeliveryPolicy, SagaCommandEnvelope,
    SagaCommandProcessingError, SagaResultEnvelope, COMMAND_EVENT_TYPE, RESULT_EVENT_TYPE,
};

/// 业务作用：冻结 command topic 的可信 Orchestrator owner 与唯一 definition/步骤合同。
///
/// Kafka ACL/SASL principal 必须保证该 topic 只有 `producer` 对应部署能写；本值再把 topic
/// 精确收窄到 workflow/version/digest/step，避免可信 producer 的合同漂移或错路由执行业务 handler。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaCommandRoute {
    /// topic 唯一可信的 Orchestrator 逻辑身份。
    producer: ServiceIdentity,
    /// 本 topic 承载的 workflow。
    workflow: WorkflowName,
    /// 本 topic 承载的 definition 版本。
    definition_version: DefinitionVersion,
    /// 本 topic 承载的 canonical definition SHA-256 摘要。
    definition_digest: String,
    /// 本 topic 唯一目标步骤。
    step: StepName,
}

impl SagaCommandRoute {
    /// 业务作用：构造已由封闭身份类型和 canonical digest 校验的 command topic route。
    ///
    /// 参数说明：
    /// - `producer`: topic 唯一可信 Orchestrator 身份。
    /// - `workflow`: 允许投递的 workflow。
    /// - `definition_version`: 允许投递的 definition 版本。
    /// - `definition_digest`: 允许投递的该版本 canonical SHA-256 小写十六进制摘要。
    /// - `step`: 允许投递的目标步骤。
    ///
    /// 返回：摘要满足 64 字节小写十六进制合同则返回不可变 route；否则启动前失败。
    pub fn new(
        producer: ServiceIdentity,
        workflow: WorkflowName,
        definition_version: DefinitionVersion,
        definition_digest: impl Into<String>,
        step: StepName,
    ) -> anyhow::Result<Self> {
        let definition_digest = definition_digest.into();
        if definition_digest.len() != 64
            || !definition_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            anyhow::bail!(
                "saga command route definition digest must be 64 lowercase hex characters"
            );
        }
        Ok(Self {
            producer,
            workflow,
            definition_version,
            definition_digest,
            step,
        })
    }

    /// 业务作用：读取 topic 认证出的 Orchestrator 逻辑身份，供审计和自定义 handler 使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：冻结 producer 身份引用。
    pub fn producer(&self) -> &ServiceIdentity {
        &self.producer
    }

    /// 业务作用：复验 envelope 已证身份是否精确落在本 topic 的唯一 definition 与目标步骤。
    ///
    /// 参数说明：
    /// - `identity`: command envelope 经确定性派生复验后的身份。
    ///
    /// 返回：workflow、version、digest 与 step 全部一致时返回真。
    fn matches(&self, identity: &crate::VerifiedIdentity) -> bool {
        self.workflow == identity.workflow
            && self.definition_version == identity.definition_version
            && self.definition_digest == identity.definition_digest
            && self.step == identity.step
    }
}

/// 业务作用：冻结 Participant command topics、受信 route、稳定 group 与有界重试预算。
#[derive(Debug, Clone)]
pub struct SagaKafkaCommandConsumerConfig {
    /// command topic 到可信 Orchestrator 及唯一目标步骤的映射。
    pub topic_routes: BTreeMap<String, SagaCommandRoute>,
    /// Participant consumer group；必须稳定，部署重启不得随机变化。
    pub group: String,
    /// 瞬态业务/数据库错误的重试预算。
    pub delivery_policy: CommandDeliveryPolicy,
}

impl SagaKafkaCommandConsumerConfig {
    /// 业务作用：校验 command topic route、稳定 group 与正重试预算，启动前拒绝不安全配置。
    ///
    /// 参数说明：
    /// - `topic_routes`: 非空的 command topic 信任与目标绑定表。
    /// - `group`: 稳定 Kafka consumer group。
    /// - `delivery_policy`: 有界普通失败预算。
    ///
    /// 返回：名称与预算合同合法时返回配置；否则在 consumer 注册前失败。
    pub fn new(
        topic_routes: BTreeMap<String, SagaCommandRoute>,
        group: impl Into<String>,
        delivery_policy: CommandDeliveryPolicy,
    ) -> anyhow::Result<Self> {
        let group = group.into();
        if topic_routes.is_empty()
            || topic_routes
                .keys()
                .any(|topic| !valid_kafka_name(topic, 249))
            || !valid_kafka_name(&group, 255)
            || delivery_policy.max_retries == 0
        {
            anyhow::bail!(
                "saga Kafka command consumer requires non-empty topic routes, a stable group and a positive retry budget"
            );
        }
        Ok(Self {
            topic_routes,
            group,
            delivery_policy,
        })
    }
}

/// 业务作用：把 Participant command handler 注册成 `nafka` 手动确认 consumer。
///
/// 处理顺序固定为 topic owner/target route → envelope 派生复验 → Participant 本地事务 →
/// COMMIT → ACK。任一步失败都没有 ACK 权限；不可恢复输入由 `nafka` 先可靠写 DLT。
pub struct SagaKafkaCommandConsumer<H> {
    /// 精确步骤分发与本地事务处理实现。
    handler: Arc<H>,
    /// 启动阶段冻结的 transport 配置。
    config: SagaKafkaCommandConsumerConfig,
}

impl<H> SagaKafkaCommandConsumer<H> {
    /// 业务作用：构造可注册到 `nafka::ConsumerRegistryBuilder` 的 Saga command consumer。
    ///
    /// 参数说明：
    /// - `handler`: 单步骤适配器或多步骤显式路由 handler。
    /// - `config`: 已校验的 topic route 与重试配置。
    ///
    /// 返回：不启动后台任务的轻量 consumer；生命周期由 `nafka` 统一托管。
    pub fn new(handler: Arc<H>, config: SagaKafkaCommandConsumerConfig) -> Self {
        Self { handler, config }
    }
}

impl<H: SagaCommandHandler> SagaKafkaCommandConsumer<H> {
    /// 业务作用：执行 ACK 前的来源认证、目标绑定、Participant 提交和错误分类。
    ///
    /// 参数说明：
    /// - `envelope`: 当前 Kafka command。
    /// - `context`: 携带可信 topic/offset 与投递预算序号的消费上下文。
    ///
    /// 返回：事务已提交返回可 ACK 结论；瞬态错误保留 offset；身份、合同或预算耗尽
    /// 返回 `HandlerDeadLetter`，由 nafka 保证 DLT durable 后再提交原 offset。
    pub async fn handle_delivery(
        &self,
        envelope: &SagaCommandEnvelope,
        context: &nafka::ConsumeCtx,
    ) -> nafka::Result<ParticipantHandled> {
        let started_at = Instant::now();
        let Some(route) = self.config.topic_routes.get(&context.topic) else {
            // 未绑定 topic 说明运行注册表与冻结信任表漂移；越权消息不能进入 Inbox，
            // 也不能无限重试占住同分区后续合法命令。
            record_kafka_command_processing(started_at.elapsed());
            record_kafka_command_dlt_requested();
            return Err(NafkaError::HandlerDeadLetter(
                "saga_command_topic_route_unbound".into(),
            ));
        };
        let identity = match envelope.verified_for_delivery() {
            Ok(identity) => identity,
            Err(error) => {
                record_kafka_command_processing(started_at.elapsed());
                record_kafka_command_dlt_requested();
                return Err(NafkaError::HandlerDeadLetter(
                    command_dead_letter_reason(&error).into(),
                ));
            }
        };
        if !route.matches(&identity) {
            // topic owner 可信不代表同一版本合同未漂移，也不代表它可以把任意步骤塞给当前
            // Service；digest/目标错配必须在 Inbox 前隔离，防止错误合同产生不可补偿副作用。
            record_kafka_command_processing(started_at.elapsed());
            record_kafka_command_dlt_requested();
            return Err(NafkaError::HandlerDeadLetter(
                SagaCommandProcessingError::RouteUnauthorized
                    .dead_letter_reason()
                    .into(),
            ));
        }

        // 收据 trace 只在此处显式解析并传入:参与方写 result Outbox 时据此派生子上下文,
        // 框架不读取任何 ambient 状态。
        let receipt_trace = context.trace_context();
        let result = self
            .handler
            .handle_authenticated_command_traced(envelope, route.producer(), receipt_trace.as_ref())
            .await;
        record_kafka_command_processing(started_at.elapsed());
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => match self
                .config
                .delivery_policy
                .decide(&error, context.retry_attempt)
            {
                CommandDeliveryDisposition::Retry => {
                    record_kafka_command_retry();
                    tracing::warn!(
                        kafka_topic = %context.topic,
                        kafka_partition = context.partition,
                        kafka_offset = context.offset,
                        delivery_attempt = context.delivery_attempt,
                        retry_attempt = context.retry_attempt,
                        "Saga command 本地事务未提交，保留 offset 退避重投"
                    );
                    Err(NafkaError::Broker(
                        "saga command transaction not committed".into(),
                    ))
                }
                CommandDeliveryDisposition::Defer => {
                    record_kafka_command_retry();
                    tracing::error!(
                        kafka_topic = %context.topic,
                        kafka_partition = context.partition,
                        kafka_offset = context.offset,
                        delivery_attempt = context.delivery_attempt,
                        "Saga command COMMIT/回滚阶段不可确认，保留 offset 且不消耗 DLT 预算"
                    );
                    Err(NafkaError::HandlerDeferred(
                        "saga_command_transaction_outcome_unresolved".into(),
                    ))
                }
                CommandDeliveryDisposition::DeadLetter => {
                    record_kafka_command_dlt_requested();
                    let reason = command_dead_letter_reason(&error);
                    tracing::error!(
                        kafka_topic = %context.topic,
                        kafka_partition = context.partition,
                        kafka_offset = context.offset,
                        delivery_attempt = context.delivery_attempt,
                        retry_attempt = context.retry_attempt,
                        reason,
                        "Saga command 进入 durability-first DLT"
                    );
                    Err(NafkaError::HandlerDeadLetter(reason.into()))
                }
            },
        }
    }
}

impl<H: SagaCommandHandler> SingleConsumer for SagaKafkaCommandConsumer<H> {
    type Message = SagaCommandEnvelope;

    /// 业务作用：返回启动时冻结的 owner 独占 command topics。
    fn topics(&self) -> Vec<String> {
        self.config.topic_routes.keys().cloned().collect()
    }

    /// 业务作用：只接收 Saga command 事件，避免 result 或普通业务事件进入 Participant。
    fn event(&self) -> String {
        COMMAND_EVENT_TYPE.into()
    }

    /// 业务作用：使用稳定 group 恢复已提交 offset，部署重启后继续未完成 command。
    fn group(&self) -> GroupSpec {
        GroupSpec::Named(self.config.group.clone())
    }

    /// 业务作用：提供在协议演进期间稳定的 handler id，单个进程用一个 consumer 显式路由全部 Saga steps。
    fn id(&self) -> &'static str {
        "nasaga-runtime::saga-command-consumer"
    }

    /// 业务作用：强制手动 ACK，使 ACK 严格晚于 Participant 本地事务 COMMIT。
    fn ack_mode(&self) -> AckMode {
        AckMode::Manual
    }

    /// 业务作用：消费一条 command；只在本地提交或 Inbox 幂等吸收后手动确认。
    ///
    /// 参数说明：
    /// - `record`: `nafka` 解码后的 command、可信投递证据与回调期 ACK 能力。
    ///
    /// 返回：提交后 ACK 成功返回 `Ok`；瞬态失败不 ACK；不可恢复输入先进入 DLT。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> nafka::Result<()> {
        let outcome = self.handle_delivery(&record.value, &record.ctx).await?;
        // COMMIT 与 ACK 间崩溃会重放同一 command；Participant Inbox/gate 只能返回重复或
        // 既有事实，绝不能再次执行外部业务效果。
        record.ack()?;
        record_kafka_command_ack(matches!(outcome, ParticipantHandled::Duplicate));
        tracing::info!(
            saga_id = %record.value.saga_id,
            workflow = %record.value.workflow,
            step = %record.value.step,
            phase = %record.value.phase,
            kafka_topic = %record.ctx.topic,
            kafka_partition = record.ctx.partition,
            kafka_offset = record.ctx.offset,
            delivery_attempt = record.ctx.delivery_attempt,
            duplicate = matches!(outcome, ParticipantHandled::Duplicate),
            "Saga command 本地事务已提交并完成手动 ACK"
        );
        Ok(())
    }
}

/// 业务作用：冻结 Saga result topic 到 producer 逻辑身份的信任映射与消费重试预算。
///
/// topic 必须按 owner 独占，例如 `saga.result.inventory`；共享 topic 无法仅靠 Kafka record
/// 证明 producer 身份，除非宿主在进入本适配器前已有等价的 mTLS/SASL principal 绑定。
#[derive(Debug, Clone)]
pub struct SagaKafkaResultConsumerConfig {
    /// result topic 到该 topic 唯一可信 producer 逻辑身份的映射。
    pub topic_owners: BTreeMap<String, ServiceIdentity>,
    /// Orchestrator consumer group；必须稳定，部署重启不得随机变化。
    pub group: String,
    /// 瞬态错误的业务重试预算；超过预算后进入 DLT。
    pub delivery_policy: ResultDeliveryPolicy,
}

impl SagaKafkaResultConsumerConfig {
    /// 业务作用：校验 topic-owner 信任表和稳定 group，拒绝无法安全认证或恢复 offset 的配置。
    ///
    /// 参数说明：
    /// - `topic_owners`: 非空的独占 result topic 信任表。
    /// - `group`: 稳定 Kafka consumer group。
    /// - `delivery_policy`: 有界业务重试预算。
    ///
    /// 返回：配置满足 Kafka 名称和有界预算合同则返回实例；否则启动失败。
    pub fn new(
        topic_owners: BTreeMap<String, ServiceIdentity>,
        group: impl Into<String>,
        delivery_policy: ResultDeliveryPolicy,
    ) -> anyhow::Result<Self> {
        let group = group.into();
        if topic_owners.is_empty()
            || topic_owners
                .keys()
                .any(|topic| !valid_kafka_name(topic, 249))
            || !valid_kafka_name(&group, 255)
            || delivery_policy.max_retries == 0
        {
            anyhow::bail!(
                "saga Kafka result consumer requires non-empty topic owners, a stable group and a positive retry budget"
            );
        }
        Ok(Self {
            topic_owners,
            group,
            delivery_policy,
        })
    }
}

/// 业务作用：把 Saga Orchestrator 注册成 `nafka` 手动确认 consumer。
///
/// 关键不变量：`handle_authenticated_result` 返回成功意味着 MySQL COMMIT 已确认；只有此后
/// 才调用 `record.ack()`。返回瞬态错误时不 ACK，`nafka` 保留 offset 并分区退避；协议或
/// 认证错误返回 [`NafkaError::HandlerDeadLetter`]，DLT broker delivery 成功后才提交原 offset。
pub struct SagaKafkaResultConsumer<H = Orchestrator> {
    /// 认证与事务处理实现。
    handler: Arc<H>,
    /// 启动阶段冻结的 transport 配置。
    config: SagaKafkaResultConsumerConfig,
}

impl<H> SagaKafkaResultConsumer<H> {
    /// 业务作用：构造可注册到 `nafka::ConsumerRegistryBuilder` 的 Saga result consumer。
    ///
    /// 参数说明：
    /// - `handler`: Orchestrator 或保持同一提交语义的自定义实现。
    /// - `config`: 已校验的 topic-owner 与重试配置。
    ///
    /// 返回：不启动后台任务的轻量 consumer；生命周期仍由 `nafka` 统一托管。
    pub fn new(handler: Arc<H>, config: SagaKafkaResultConsumerConfig) -> Self {
        Self { handler, config }
    }
}

impl<H: SagaResultHandler> SagaKafkaResultConsumer<H> {
    /// 业务作用：执行 ACK 之前的认证、事务提交和错误分类，供 hosted consumer 与自定义宿主共用。
    ///
    /// 参数说明：
    /// - `envelope`: 当前 Kafka 结果消息。
    /// - `context`: 携带可信 topic、offset、总投递序号与普通失败预算序号的消费上下文。
    ///
    /// 返回：事务已提交时返回可 ACK 结论；瞬态错误返回普通 handler 错误以保留 offset；
    /// 协议、认证或预算耗尽返回 `HandlerDeadLetter`。
    pub async fn handle_delivery(
        &self,
        envelope: &SagaResultEnvelope,
        context: &nafka::ConsumeCtx,
    ) -> nafka::Result<HandleOutcome> {
        let started_at = Instant::now();
        let Some(producer) = self.config.topic_owners.get(&context.topic) else {
            // topic 未绑定说明运行时注册表与冻结信任表漂移；绝不能把它当普通重试，
            // 否则越权消息会无限占住分区。
            record_kafka_processing(started_at.elapsed());
            record_kafka_dlt_requested();
            return Err(NafkaError::HandlerDeadLetter(
                "saga_result_topic_owner_unbound".into(),
            ));
        };
        let now_ms = match current_epoch_ms() {
            Ok(now_ms) => now_ms,
            Err(reason) => {
                record_kafka_processing(started_at.elapsed());
                record_kafka_retry();
                return Err(NafkaError::Broker(reason.into()));
            }
        };
        // 收据 trace 只在此处显式解析并传入:结果推进事务据此更新实例因果上下文,
        // 后续命令与 timer 恢复由同一 trace 派生 child。
        let receipt_trace = context.trace_context();
        let result = self
            .handler
            .handle_authenticated_result_traced(envelope, producer, receipt_trace.as_ref(), now_ms)
            .await;
        record_kafka_processing(started_at.elapsed());
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let disposition = self
                    .config
                    .delivery_policy
                    .decide(&error, context.retry_attempt);
                match disposition {
                    ResultDeliveryDisposition::Retry => {
                        record_kafka_retry();
                        tracing::warn!(
                            kafka_topic = %context.topic,
                            kafka_partition = context.partition,
                            kafka_offset = context.offset,
                            delivery_attempt = context.delivery_attempt,
                            retry_attempt = context.retry_attempt,
                            "Saga 结果事务未提交，保留 offset 退避重投"
                        );
                        Err(NafkaError::Broker(
                            "saga result transaction not committed".into(),
                        ))
                    }
                    ResultDeliveryDisposition::Defer => {
                        record_kafka_retry();
                        tracing::info!(
                            kafka_topic = %context.topic,
                            kafka_partition = context.partition,
                            kafka_offset = context.offset,
                            "Saga 结果受暂停或事务阶段门禁延后，保留 offset 且不消耗 DLT 预算"
                        );
                        Err(NafkaError::HandlerDeferred("saga_result_deferred".into()))
                    }
                    ResultDeliveryDisposition::DeadLetter => {
                        record_kafka_dlt_requested();
                        let reason = dead_letter_reason(&error);
                        tracing::error!(
                            kafka_topic = %context.topic,
                            kafka_partition = context.partition,
                            kafka_offset = context.offset,
                            delivery_attempt = context.delivery_attempt,
                            retry_attempt = context.retry_attempt,
                            reason,
                            "Saga 结果进入 durability-first DLT"
                        );
                        Err(NafkaError::HandlerDeadLetter(reason.into()))
                    }
                }
            }
        }
    }
}

impl<H: SagaResultHandler> SingleConsumer for SagaKafkaResultConsumer<H> {
    type Message = SagaResultEnvelope;

    /// 业务作用：返回启动时冻结的 owner 独占 result topics。
    fn topics(&self) -> Vec<String> {
        self.config.topic_owners.keys().cloned().collect()
    }

    /// 业务作用：只接收 Saga result 事件，避免 command 或普通业务事件进入 Orchestrator。
    fn event(&self) -> String {
        RESULT_EVENT_TYPE.into()
    }

    /// 业务作用：使用稳定 group 恢复已提交 offset，部署重启后继续未完成结果。
    fn group(&self) -> GroupSpec {
        GroupSpec::Named(self.config.group.clone())
    }

    /// 业务作用：提供在协议演进期间稳定的 handler id，供注册冲突检查和低基数指标使用。
    fn id(&self) -> &'static str {
        "nasaga-runtime::saga-result-consumer"
    }

    /// 业务作用：强制手动 ACK，使 ACK 时点严格晚于 Orchestrator 本地事务提交。
    fn ack_mode(&self) -> AckMode {
        AckMode::Manual
    }

    /// 业务作用：消费并认证一条结果；只在 MySQL 已提交或 Inbox 已吸收重复后确认。
    ///
    /// 参数说明：
    /// - `record`: `nafka` 解码后的 envelope、可信 delivery attempt 与回调期 ACK 能力。
    ///
    /// 返回：提交后 ACK 成功返回 `Ok`；瞬态失败不 ACK 并退避；不可恢复错误进入 DLT。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> nafka::Result<()> {
        let outcome = self.handle_delivery(&record.value, &record.ctx).await?;
        // ACK 是本地事务成功后的最后一步；若进程在 COMMIT 与 ACK 之间崩溃，Kafka 会重放，
        // Orchestrator Inbox/attempt journal 幂等吸收，绝不重复推进或丢失结果。
        record.ack()?;
        record_kafka_ack(matches!(outcome, HandleOutcome::Duplicate));
        match outcome {
            HandleOutcome::Applied { status } => tracing::info!(
                saga_id = %record.value.saga_id,
                workflow = %record.value.workflow,
                step = %record.value.step,
                phase = %record.value.phase,
                saga_status = status.as_str(),
                kafka_topic = %record.ctx.topic,
                kafka_partition = record.ctx.partition,
                kafka_offset = record.ctx.offset,
                delivery_attempt = record.ctx.delivery_attempt,
                "Saga 结果事务已提交并完成手动 ACK"
            ),
            HandleOutcome::Duplicate => tracing::info!(
                saga_id = %record.value.saga_id,
                kafka_topic = %record.ctx.topic,
                kafka_partition = record.ctx.partition,
                kafka_offset = record.ctx.offset,
                delivery_attempt = record.ctx.delivery_attempt,
                "Saga 重复结果已由 Inbox 吸收并完成手动 ACK"
            ),
        }
        Ok(())
    }
}

/// 业务作用：校验 Kafka topic/group 使用的稳定名称，避免空白、控制字符和重启漂移。
///
/// 参数说明：
/// - `value`: 待校验名称。
/// - `max_len`: Kafka 对应字段的长度上限。
///
/// 返回：非空、无首尾空白、长度有界且只含 Kafka 可移植字符时返回真。
fn valid_kafka_name(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value.trim() == value
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// 业务作用：读取当前 epoch 毫秒，为结果推进写入统一业务时间。
///
/// 参数说明: 无。
///
/// 返回：系统时间合法且未溢出 i64 时返回毫秒；时钟早于 epoch 或溢出时返回稳定错误。
fn current_epoch_ms() -> Result<i64, &'static str> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before Unix epoch")?;
    i64::try_from(elapsed.as_millis()).map_err(|_| "system clock exceeds supported epoch range")
}

/// 业务作用：把不可恢复错误收敛为不含 payload、凭据或数据库细节的稳定 DLT 原因码。
///
/// 参数说明：
/// - `error`: Orchestrator 返回的脱敏错误链。
///
/// 返回：可安全写入 DLT reason header 的低基数代码。
fn dead_letter_reason(error: &anyhow::Error) -> &'static str {
    error
        .chain()
        .find_map(|cause| {
            cause
                .downcast_ref::<crate::SagaResultProcessingError>()
                .and_then(|classified| classified.dead_letter_reason())
        })
        .unwrap_or("saga_result_retry_exhausted")
}

/// 业务作用：把 command 不可恢复错误收敛为不含 payload、身份值或数据库细节的 DLT 原因码。
///
/// 参数说明：
/// - `error`: transport 或 Participant 返回的脱敏错误链。
///
/// 返回：类型化来源/合同原因，或普通失败预算耗尽的稳定代码。
fn command_dead_letter_reason(error: &anyhow::Error) -> &'static str {
    error
        .chain()
        .find_map(|cause| {
            cause
                .downcast_ref::<SagaCommandProcessingError>()
                .map(|classified| classified.dead_letter_reason())
        })
        .unwrap_or("saga_command_retry_exhausted")
}
