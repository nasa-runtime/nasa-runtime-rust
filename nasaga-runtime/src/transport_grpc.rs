//! Saga 与 gRPC(request/response)transport 的合同适配。
//!
//! gRPC 没有 broker 的 offset/PEL/ACK 模型,也没有 broker DLT:服务端对每次投递只返回
//! **封闭收据**——已提交、重复、确定性拒绝或可重试;发布端 Outbox 根据自身 `Block`/DLT
//! 策略裁决行的去留。deadline 超时、连接中断或回包丢失都按**结果不确定**处理:行保留在
//! Outbox,以同 `event_id` 重投,由参与方 Inbox 幂等吸收。
//!
//! 本模块提供框架自带的 generated command/result service 与底层裁决器。常规 Application 宿主只
//! 提交 `#[saga]` Service 和不可推断的 mTLS leaf 授权映射；`napp` 从既有 Saga 计划生成 handler，
//! 协议、身份解析与 service 登记由框架完成，transport 上限与 drain 统一由 `nagrpc` listener 承担。
//! 多参与方高级路由和独立宿主仍可直接复用裁决器或 generated client/server。

use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nasaga_core::ServiceIdentity;
use natelemetry::TraceContext;

use crate::transport_shared::{SagaCommandHandler, SagaResultHandler};
use crate::{HandleOutcome, ParticipantHandled, SagaCommandEnvelope, SagaResultEnvelope};

/// 框架统一生成的 Saga command/result gRPC 协议、client、server 与 descriptor。
pub mod proto {
    nagrpc::include_proto!("nasa.saga.transport.v1");
}

/// mTLS leaf 指纹到 Saga 逻辑 producer 的绑定配置错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaGrpcBindingError {
    /// principal 不是 nagrpc 发布的 `sha256:<64 lowercase hex>` 身份。
    InvalidPeerPrincipal,
}

impl fmt::Display for SagaGrpcBindingError {
    /// 业务作用：返回不包含证书、principal 或业务身份内容的稳定配置错误。
    ///
    /// 参数说明：
    /// - `formatter`: 接收稳定错误文本的格式化目标。
    ///
    /// 返回：文本写入成功时完成，否则透传格式化失败。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Saga gRPC peer principal is invalid")
    }
}

impl std::error::Error for SagaGrpcBindingError {}

/// 已验证 mTLS leaf 指纹与 Saga 逻辑 producer 的启动期冻结绑定。
#[derive(Clone)]
pub struct SagaGrpcPeerBinding {
    peer_principal: Arc<str>,
    producer: ServiceIdentity,
}

impl SagaGrpcPeerBinding {
    /// 业务作用：建立不可由 metadata 自报绕过的 mTLS principal 到 Saga producer 映射。
    ///
    /// 参数说明：
    /// - `peer_principal`: nagrpc 从已验证 client leaf certificate 派生的 SHA-256 指纹。
    /// - `producer`: 该证书唯一获准代表的 Saga 逻辑服务身份。
    ///
    /// 返回：principal 形态合法时返回冻结绑定；其它输入返回脱敏配置错误。
    pub fn new(
        peer_principal: impl Into<Arc<str>>,
        producer: ServiceIdentity,
    ) -> Result<Self, SagaGrpcBindingError> {
        let peer_principal = peer_principal.into();
        let digest = peer_principal.strip_prefix("sha256:").unwrap_or_default();
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SagaGrpcBindingError::InvalidPeerPrincipal);
        }
        Ok(Self {
            peer_principal,
            producer,
        })
    }

    /// 业务作用：只接受 listener TLS driver 写入且与冻结指纹相同的 PeerIdentity。
    ///
    /// 参数说明：
    /// - `request`: 当前 generated gRPC 请求，extensions 由 nagrpc 在进入 handler 前建立。
    ///
    /// 返回：指纹匹配时返回可交给 Saga 裁决器的可信身份；缺失或不匹配返回 `Unauthenticated`。
    fn authenticate<T>(
        &self,
        request: &nagrpc::Request<T>,
    ) -> Result<SagaGrpcPeerIdentity, nagrpc::Status> {
        let authenticated = request
            .extensions()
            .get::<nagrpc::PeerIdentity>()
            .is_some_and(|peer| peer.principal() == self.peer_principal.as_ref());
        if !authenticated {
            return Err(nagrpc::Status::unauthenticated(
                "verified Saga gRPC peer identity is required",
            ));
        }
        Ok(SagaGrpcPeerIdentity::MtlsPrincipal(self.producer.clone()))
    }
}

/// 业务作用：从唯一合法的 gRPC metadata `traceparent` 解析显式收据因果上下文。
///
/// 参数说明：
/// - `request`: 当前 generated gRPC 请求。
///
/// 返回：恰有一个且格式合法时返回上下文；缺失、重复或非法输入不进入业务因果链。
fn receipt_trace<T>(request: &nagrpc::Request<T>) -> Option<TraceContext> {
    let mut values = request.metadata().get_all("traceparent").iter();
    match (values.next(), values.next()) {
        (Some(value), None) => value
            .to_str()
            .ok()
            .and_then(TraceContext::parse_traceparent),
        _ => None,
    }
}

/// 业务作用：把内部封闭收据稳定映射为 protobuf enum 与可选确定性原因。
///
/// 参数说明：
/// - `receipt`: command 或 result 裁决器形成的最终收据。
///
/// 返回：客户端无需解析 status 文本即可裁决 Outbox 行的协议响应。
fn delivery_receipt(receipt: SagaGrpcReceipt) -> proto::SagaDeliveryReceipt {
    let (kind, reason) = match receipt {
        SagaGrpcReceipt::Committed => (proto::SagaReceiptKind::Committed, String::new()),
        SagaGrpcReceipt::Duplicate => (proto::SagaReceiptKind::Duplicate, String::new()),
        SagaGrpcReceipt::DeterministicReject { reason } => (
            proto::SagaReceiptKind::DeterministicReject,
            reason.to_owned(),
        ),
        SagaGrpcReceipt::Retryable => (proto::SagaReceiptKind::Retryable, String::new()),
    };
    proto::SagaDeliveryReceipt {
        kind: kind as i32,
        reason,
    }
}

/// 业务作用：gRPC 投递的封闭收据——发布端与服务端共同的全部合法结论。
///
/// `Retryable` 不携带任何"越过"语义:发布端收到它(或根本收不到回包)都只能保留
/// Outbox 行重投;只有 `DeterministicReject` 允许发布端按已批准的 DLT 策略把行移入
/// 死信集合。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaGrpcReceipt {
    /// 本地事务已 COMMIT;发布端可标记该行已投递。
    Committed,
    /// Inbox 命中重复;无新副作用,发布端同样可标记已投递。
    Duplicate,
    /// 确定性协议拒绝(身份伪造、越权、合同漂移、不可解析);携带稳定原因码,
    /// 发布端按自身 Block/DLT 策略裁决。
    DeterministicReject {
        /// 稳定、低基数拒绝原因码。
        reason: &'static str,
    },
    /// 瞬态失败(数据库不可达、事务回滚、暂停等):行保留,同 `event_id` 重投。
    Retryable,
}

/// 业务作用：gRPC 服务端的 producer 身份来源——mTLS principal 或等价的端到端签名结论。
///
/// **绝不信任 metadata 中自报的服务名**:`MtlsPrincipal` 必须来自 TLS 层验证过的证书
/// 身份;`VerifiedSignature` 必须来自已完成签名校验(如
/// [`SagaHttpMessageAuthenticator`](crate::SagaHttpMessageAuthenticator) 同款 canonical
/// HMAC + 重放守卫)的结论。两者都由宿主在调用裁决器之前完成。
#[derive(Debug, Clone)]
pub enum SagaGrpcPeerIdentity {
    /// TLS 层验证 leaf 指纹后，经启动期冻结绑定映射出的逻辑服务身份。
    MtlsPrincipal(ServiceIdentity),
    /// 端到端签名校验通过后映射出的逻辑服务身份。
    VerifiedSignature(ServiceIdentity),
}

impl SagaGrpcPeerIdentity {
    /// 业务作用：读取已验证的逻辑服务身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：producer 逻辑身份引用。
    pub fn producer(&self) -> &ServiceIdentity {
        match self {
            Self::MtlsPrincipal(identity) | Self::VerifiedSignature(identity) => identity,
        }
    }
}

/// 业务作用：参与方侧的 gRPC command 裁决器——把已鉴权请求字节收敛为封闭收据。
///
/// 处理顺序固定:对端身份必须等于冻结的可信 Orchestrator → payload 解码 → envelope
/// 身份复验(由参与方运行时内部完成)→ 本地事务 → 收据。任何一步的确定性失败都变成
/// `DeterministicReject`,瞬态失败变成 `Retryable`,绝不把错误文本交给调用方解析。
pub struct SagaGrpcCommandServer<H> {
    handler: Arc<H>,
    trusted_producer: ServiceIdentity,
}

/// generated `SagaCommandTransport` 的框架实现；业务只提供本地 handler 与启动期 mTLS 绑定。
pub struct SagaGrpcCommandTransportService<H> {
    adjudicator: SagaGrpcCommandServer<H>,
    peer: SagaGrpcPeerBinding,
}

impl<H: SagaCommandHandler> SagaGrpcCommandTransportService<H> {
    /// 业务作用：把 command 裁决器与唯一可信 client certificate 绑定为可登记的 generated service。
    ///
    /// 参数说明：
    /// - `handler`: 提交本地参与方事务的 command handler。
    /// - `peer`: mTLS leaf 指纹与可信 Orchestrator 逻辑身份的冻结绑定。
    ///
    /// 返回：尚未加入 listener、可由 `SagaApplicationPlan` 自动登记的 service 实现。
    pub fn new(handler: Arc<H>, peer: SagaGrpcPeerBinding) -> Self {
        let adjudicator = SagaGrpcCommandServer::new(handler, peer.producer.clone());
        Self { adjudicator, peer }
    }
}

#[nagrpc::async_trait]
impl<H: SagaCommandHandler> proto::saga_command_transport_server::SagaCommandTransport
    for SagaGrpcCommandTransportService<H>
{
    /// 业务作用：验证 mTLS 身份并把 command envelope 交给本地事务裁决器，最终只返回封闭收据。
    ///
    /// 参数说明：
    /// - `request`: protobuf envelope bytes 与受管连接 extensions/metadata。
    ///
    /// 返回：身份通过时始终返回四值收据；PeerIdentity 缺失或不匹配时返回 `Unauthenticated`。
    async fn deliver(
        &self,
        request: nagrpc::Request<proto::SagaDeliveryRequest>,
    ) -> Result<nagrpc::Response<proto::SagaDeliveryReceipt>, nagrpc::Status> {
        let peer = self.peer.authenticate(&request)?;
        let trace = receipt_trace(&request);
        let receipt = self
            .adjudicator
            .adjudicate(&peer, &request.get_ref().envelope_json, trace.as_ref())
            .await;
        Ok(nagrpc::Response::new(delivery_receipt(receipt)))
    }
}

impl<H: SagaCommandHandler> SagaGrpcCommandServer<H> {
    /// 业务作用：构造绑定唯一可信 Orchestrator 的 command 裁决器。
    ///
    /// 参数说明：
    /// - `handler`: 命令处理实现（宏生成 Service 的
    ///   [`ParticipantCommandHandler`](crate::ParticipantCommandHandler)）。
    /// - `trusted_producer`: 本入口唯一可信的 Orchestrator 逻辑身份。
    ///
    /// 返回：裁决器。
    pub fn new(handler: Arc<H>, trusted_producer: ServiceIdentity) -> Self {
        Self {
            handler,
            trusted_producer,
        }
    }

    /// 业务作用：裁决一次 command 投递并返回封闭收据。
    ///
    /// 参数说明：
    /// - `peer`: TLS/签名层已验证的对端身份。
    /// - `payload`: command envelope JSON 字节。
    /// - `receipt_trace`: gRPC metadata 中显式解析出的 `traceparent`。
    ///
    /// 返回：封闭收据;调用方(generated service)据此构造响应,不解析错误文本。
    pub async fn adjudicate(
        &self,
        peer: &SagaGrpcPeerIdentity,
        payload: &[u8],
        receipt_trace: Option<&TraceContext>,
    ) -> SagaGrpcReceipt {
        // 对端身份必须精确等于冻结的可信 Orchestrator:可信 TLS 网内的其它服务
        // 同样无权向本参与方发命令。
        if peer.producer() != &self.trusted_producer {
            return SagaGrpcReceipt::DeterministicReject {
                reason: "saga_command_producer_unauthorized",
            };
        }
        let Ok(envelope) = serde_json::from_slice::<SagaCommandEnvelope>(payload) else {
            return SagaGrpcReceipt::DeterministicReject {
                reason: "saga_command_payload_undecodable",
            };
        };
        let outcome = self
            .handler
            .handle_authenticated_command_traced(&envelope, peer.producer(), receipt_trace)
            .await;
        match outcome {
            Ok(
                ParticipantHandled::Executed(_)
                | ParticipantHandled::Suppressed
                | ParticipantHandled::Replayed(_)
                | ParticipantHandled::ContractViolation,
            ) => SagaGrpcReceipt::Committed,
            Ok(ParticipantHandled::Duplicate) => SagaGrpcReceipt::Duplicate,
            Err(error) => match crate::command_dead_letter_reason(&error) {
                Some(reason) => SagaGrpcReceipt::DeterministicReject { reason },
                // 事务未提交/结果不确定/数据库不可达:一律可重试,行留在发布端 Outbox。
                None => SagaGrpcReceipt::Retryable,
            },
        }
    }
}

/// 业务作用：Orchestrator 侧的 gRPC result 裁决器——与 command 侧同一收据词汇。
pub struct SagaGrpcResultServer<H> {
    handler: Arc<H>,
    trusted_producer: ServiceIdentity,
}

/// generated `SagaResultTransport` 的框架实现；Orchestrator 与参与方共用同一身份和收据合同。
pub struct SagaGrpcResultTransportService<H> {
    adjudicator: SagaGrpcResultServer<H>,
    peer: SagaGrpcPeerBinding,
}

impl<H: SagaResultHandler> SagaGrpcResultTransportService<H> {
    /// 业务作用：把 result 裁决器与唯一可信参与方 client certificate 绑定为 generated service。
    ///
    /// 参数说明：
    /// - `handler`: 提交 Orchestrator 结果推进事务的 handler。
    /// - `peer`: mTLS leaf 指纹与可信参与方逻辑身份的冻结绑定。
    ///
    /// 返回：尚未加入 listener、可由 `SagaApplicationPlan` 自动登记的 service 实现。
    pub fn new(handler: Arc<H>, peer: SagaGrpcPeerBinding) -> Self {
        let adjudicator = SagaGrpcResultServer::new(handler, peer.producer.clone());
        Self { adjudicator, peer }
    }
}

#[nagrpc::async_trait]
impl<H: SagaResultHandler> proto::saga_result_transport_server::SagaResultTransport
    for SagaGrpcResultTransportService<H>
{
    /// 业务作用：验证 mTLS 身份并把 result envelope 交给 Orchestrator 事务裁决器。
    ///
    /// 参数说明：
    /// - `request`: protobuf envelope bytes 与受管连接 extensions/metadata。
    ///
    /// 返回：身份与时钟可用时返回四值收据；身份失败返回 `Unauthenticated`，系统时钟不可用返回
    /// `Internal` 且不调用结果 handler。
    async fn deliver(
        &self,
        request: nagrpc::Request<proto::SagaDeliveryRequest>,
    ) -> Result<nagrpc::Response<proto::SagaDeliveryReceipt>, nagrpc::Status> {
        let peer = self.peer.authenticate(&request)?;
        let trace = receipt_trace(&request);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_millis()).ok())
            .ok_or_else(|| nagrpc::Status::internal("Saga gRPC clock is unavailable"))?;
        let receipt = self
            .adjudicator
            .adjudicate(
                &peer,
                &request.get_ref().envelope_json,
                trace.as_ref(),
                now_ms,
            )
            .await;
        Ok(nagrpc::Response::new(delivery_receipt(receipt)))
    }
}

impl<H: SagaResultHandler> SagaGrpcResultServer<H> {
    /// 业务作用：构造绑定唯一可信参与方身份的 result 裁决器。
    ///
    /// 参数说明：
    /// - `handler`: 结果处理实现（通常为 [`crate::Orchestrator`]）。
    /// - `trusted_producer`: 本入口唯一可信的参与方逻辑身份。
    ///
    /// 返回：裁决器。
    pub fn new(handler: Arc<H>, trusted_producer: ServiceIdentity) -> Self {
        Self {
            handler,
            trusted_producer,
        }
    }

    /// 业务作用：裁决一次 result 投递并返回封闭收据。
    ///
    /// 参数说明：
    /// - `peer`: TLS/签名层已验证的对端身份。
    /// - `payload`: result envelope JSON 字节。
    /// - `receipt_trace`: gRPC metadata 中显式解析出的 `traceparent`。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：封闭收据。
    pub async fn adjudicate(
        &self,
        peer: &SagaGrpcPeerIdentity,
        payload: &[u8],
        receipt_trace: Option<&TraceContext>,
        now_ms: i64,
    ) -> SagaGrpcReceipt {
        if peer.producer() != &self.trusted_producer {
            return SagaGrpcReceipt::DeterministicReject {
                reason: "saga_result_producer_unauthorized",
            };
        }
        let Ok(envelope) = serde_json::from_slice::<SagaResultEnvelope>(payload) else {
            return SagaGrpcReceipt::DeterministicReject {
                reason: "saga_result_payload_undecodable",
            };
        };
        let outcome = self
            .handler
            .handle_authenticated_result_traced(&envelope, peer.producer(), receipt_trace, now_ms)
            .await;
        match outcome {
            Ok(HandleOutcome::Applied { .. }) => SagaGrpcReceipt::Committed,
            Ok(HandleOutcome::Duplicate) => SagaGrpcReceipt::Duplicate,
            Err(error) => {
                if matches!(
                    crate::classify_result_delivery_error(&error),
                    crate::ResultDeliveryDisposition::DeadLetter
                ) {
                    let reason = error
                        .chain()
                        .find_map(|cause| {
                            cause
                                .downcast_ref::<crate::SagaResultProcessingError>()
                                .copied()
                        })
                        .and_then(crate::SagaResultProcessingError::dead_letter_reason)
                        .unwrap_or("saga_result_contract_invalid");
                    SagaGrpcReceipt::DeterministicReject { reason }
                } else {
                    // PAUSED 与全部瞬态:可重试;gRPC 无 PEL,重投责任在发布端 Outbox。
                    SagaGrpcReceipt::Retryable
                }
            }
        }
    }
}

/// 业务作用：把 gRPC 收据映射回发布端 Outbox 的投递结论。
///
/// 参数说明：
/// - `receipt`: 服务端返回的封闭收据；回包丢失/deadline 由调用方直接按
///   `None` 传入。
///
/// 返回：`Committed`/`Duplicate` 返回 `Ok`(行可标记已投递);`DeterministicReject`
/// 返回携带稳定原因的错误(由 Outbox Block/DLT 策略裁决);`Retryable` 与回包缺失
/// 返回瞬态错误(行保留,同 `event_id` 重投)。
pub fn outbox_disposition_of(
    receipt: Option<&SagaGrpcReceipt>,
) -> Result<(), naoutbox_core::OutboxPublishError> {
    match receipt {
        Some(SagaGrpcReceipt::Committed) | Some(SagaGrpcReceipt::Duplicate) => Ok(()),
        Some(SagaGrpcReceipt::DeterministicReject { reason }) => {
            Err(naoutbox_core::OutboxPublishError::new(*reason))
        }
        // 结果不确定(超时/断连/回包丢失)与显式 Retryable 同类:保留重投。
        Some(SagaGrpcReceipt::Retryable) | None => Err(
            naoutbox_core::OutboxPublishError::transient("saga_grpc_delivery_unresolved"),
        ),
    }
}
