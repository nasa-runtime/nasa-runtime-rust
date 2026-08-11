//! Saga 与 gRPC(request/response)transport 的合同适配。
//!
//! gRPC 没有 broker 的 offset/PEL/ACK 模型,也没有 broker DLT:服务端对每次投递只返回
//! **封闭收据**——已提交、重复、确定性拒绝或可重试;发布端 Outbox 根据自身 `Block`/DLT
//! 策略裁决行的去留。deadline 超时、连接中断或回包丢失都按**结果不确定**处理:行保留在
//! Outbox,以同 `event_id` 重投,由参与方 Inbox 幂等吸收。
//!
//! 本模块不绑定具体 tonic generated service:业务的 generated service 把已鉴权的请求
//! 字节与显式 metadata(principal/trace)交给这里的裁决器,并把封闭收据映射回自己的
//! 响应类型;transport 上限与 drain 由 `nagrpc` 的 listener 配置承担。

use std::sync::Arc;

use nasaga_core::ServiceIdentity;
use natelemetry::TraceContext;

use crate::transport_shared::{SagaCommandHandler, SagaResultHandler};
use crate::{HandleOutcome, ParticipantHandled, SagaCommandEnvelope, SagaResultEnvelope};

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
    /// TLS 层验证过的对端 principal(证书 CN/SAN 映射出的逻辑服务身份)。
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
    /// - `receipt_trace`: gRPC metadata 中显式解析出的 `traceparent`（与 16.8 同边界）。
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
