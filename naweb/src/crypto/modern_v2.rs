//! modern-v2 严格信封、规范化 AAD、时间窗与重放处理。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use super::{
    enforce_encrypted_limit, CryptoDirection, CryptoError, CryptoErrorKind, CryptoExecutionContext,
    CryptoFuture, CryptoProtocol, DecryptInput, EncryptInput, ProtocolDecryptInput,
    ProtocolDecryptOutput, ProtocolEncryptInput, ProtocolEncryptOutput, ReplayDecision, ReplayKey,
    RequestCryptoContext,
};

/// modern-v2 的精确 HTTP 媒体类型。
pub const MODERN_V2_MEDIA_TYPE: &str = "application/vnd.nasa.crypto+json;v=2";

/// modern-v2 外层信封；deny_unknown_fields 与必填字段共同保证恰好七个字段。
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ModernEnvelope {
    /// 固定整数协议版本，只允许 2。
    v: u8,
    /// 固定算法文本，只允许大小写精确的 A256GCM。
    alg: String,
    /// 当前 route key ring 中的公开 key 定位符。
    kid: String,
    /// 16 字节随机请求标识的无填充 Base64URL。
    rid: String,
    /// 12 字节随机 nonce 的无填充 Base64URL。
    nonce: String,
    /// Unix epoch 毫秒整数。
    ts: i64,
    /// `ciphertext || 16-byte tag` 的无填充 Base64URL。
    data: String,
}

/// modern-v2 无状态协议实现。
#[derive(Debug, Default)]
pub struct ModernV2Protocol;

impl ModernV2Protocol {
    /// 建立无可变状态的 modern-v2 协议对象。
    ///
    /// # 返回
    ///
    /// 返回可共享注册到 [`super::CryptoRuntime`] 的协议实现。
    pub const fn new() -> Self {
        Self
    }
}

impl CryptoProtocol for ModernV2Protocol {
    /// 返回静态协议 ID。
    ///
    /// # 返回
    ///
    /// 返回 route policy 使用的 `modern-v2`。
    fn id(&self) -> &'static str {
        "modern-v2"
    }

    /// 返回请求和响应共同使用的 vendor JSON 媒体类型。
    ///
    /// # 返回
    ///
    /// 返回 [`MODERN_V2_MEDIA_TYPE`]。
    fn request_content_type(&self) -> &'static str {
        MODERN_V2_MEDIA_TYPE
    }

    /// 严格解析、认证解密、校验时间并最后写入重放存储。
    ///
    /// # 参数
    ///
    /// - `input`: 请求固定快照提供的完整有界输入。
    ///
    /// # 返回
    ///
    /// 成功返回业务 JSON 与响应上下文；任何失败都不尝试其它协议或明文。
    fn decrypt<'a>(
        &'a self,
        input: ProtocolDecryptInput,
    ) -> CryptoFuture<'a, Result<ProtocolDecryptOutput, CryptoError>> {
        Box::pin(async move {
            if input.envelope.len() > input.limits.max_request_envelope_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "modern-request-envelope-too-large",
                ));
            }
            let envelope: ModernEnvelope = serde_json::from_slice(&input.envelope)
                .map_err(|_| CryptoError::new(CryptoErrorKind::Envelope, "modern-envelope-json"))?;
            validate_envelope_strings(&envelope, input.limits.max_string_bytes)?;
            if envelope.v != 2 || envelope.alg != "A256GCM" {
                return Err(CryptoError::new(
                    CryptoErrorKind::Envelope,
                    "modern-version-or-algorithm",
                ));
            }
            let rid = decode_exact::<16>(&envelope.rid, "modern-rid")?;
            let nonce = decode_exact::<12>(&envelope.nonce, "modern-nonce")?;
            let ciphertext = decode_unpadded(&envelope.data, input.limits.max_ciphertext_bytes)?;
            if ciphertext.len() < 16 {
                return Err(CryptoError::new(
                    CryptoErrorKind::Envelope,
                    "modern-ciphertext-without-tag",
                ));
            }
            let key = input
                .key_ring
                .resolve_for_decrypt(&envelope.kid, input.now)?;
            if key.algorithm != super::KeyAlgorithm::A256Gcm {
                return Err(CryptoError::new(
                    CryptoErrorKind::Key,
                    "modern-key-algorithm",
                ));
            }
            let key_scope =
                Arc::<str>::from(input.route.crypto_key_scope.ok_or_else(|| {
                    CryptoError::new(CryptoErrorKind::Policy, "key-scope-missing")
                })?);
            let aad = build_modern_aad(ModernAadInput {
                direction: CryptoDirection::Request,
                kid: &key.kid,
                tenant_scope: &input.tenant_scope,
                key_scope: &key_scope,
                audience: input.route.audience,
                method: input.route.method,
                target: &input.target,
                rid: &rid,
                nonce: &nonce,
                timestamp_ms: envelope.ts,
                status: None,
            })?;
            let context = CryptoExecutionContext {
                direction: CryptoDirection::Request,
                tenant_scope: input.tenant_scope.clone(),
                key_scope: key_scope.clone(),
                key: key.clone(),
            };
            let plaintext = input
                .provider
                .decrypt(
                    DecryptInput {
                        ciphertext,
                        auxiliary: None,
                        nonce: Some(nonce),
                        aad,
                    },
                    context,
                )
                .await?;
            let plaintext_bytes = plaintext.into_vec();
            if plaintext_bytes.len() > input.limits.max_plaintext_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "modern-plaintext-too-large",
                ));
            }

            // 时间窗只能在 AEAD 认证后判断，避免不同未认证输入形成额外探测面。
            validate_timestamp(
                envelope.ts,
                input.now,
                input.limits.clock_past_window,
                input.limits.clock_future_skew,
            )?;
            if input.route.replay == crate::ReplayRequirement::Required {
                let guard = input.replay_guard.as_ref().ok_or_else(|| {
                    CryptoError::new(CryptoErrorKind::Policy, "replay-guard-missing")
                })?;
                let replay_key = ReplayKey {
                    tenant_scope: input.tenant_scope.clone(),
                    key_scope: key_scope.clone(),
                    kid: key.kid.clone(),
                    direction: "request",
                    rid,
                };
                // replay 写入必须晚于 tag 认证；否则伪造请求可用任意 rid 污染共享存储。
                let decision = timeout(
                    input.replay_timeout,
                    guard.reserve(replay_key, input.replay_ttl),
                )
                .await
                .map_err(|_| {
                    CryptoError::new(CryptoErrorKind::ReplayBackend, "replay-store-timeout")
                })?
                .map_err(|_| {
                    CryptoError::new(CryptoErrorKind::ReplayBackend, "replay-store-error")
                })?;
                if decision == ReplayDecision::Duplicate {
                    return Err(CryptoError::new(
                        CryptoErrorKind::Replayed,
                        "modern-request-replayed",
                    ));
                }
            }
            validate_json(&plaintext_bytes, input.limits.max_json_depth)?;

            Ok(ProtocolDecryptOutput {
                plaintext: super::SecretBytes::new(plaintext_bytes),
                context: RequestCryptoContext {
                    protocol_id: self.id(),
                    rid: Some(rid),
                    key,
                    tenant_scope: input.tenant_scope,
                    key_scope,
                    audience: Arc::from(input.route.audience),
                    method: input.route.method,
                    target: input.target,
                    provider: input.provider,
                },
            })
        })
    }

    /// 使用请求固定的 rid、key、target 和 audience 加密完整响应。
    ///
    /// # 参数
    ///
    /// - `input`: handler 完整响应、状态和请求固定上下文。
    ///
    /// # 返回
    ///
    /// 返回七字段 modern-v2 信封；随机源、上限或响应 JSON 错误均禁止透传明文。
    fn encrypt<'a>(
        &'a self,
        input: ProtocolEncryptInput,
    ) -> CryptoFuture<'a, Result<ProtocolEncryptOutput, CryptoError>> {
        Box::pin(async move {
            if input.plaintext.len() > input.limits.max_response_plaintext_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "modern-response-plaintext-too-large",
                ));
            }
            validate_json(&input.plaintext, input.limits.max_json_depth)?;
            let rid = input.context.rid.ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::Policy, "modern-response-rid-missing")
            })?;
            let mut nonce = [0_u8; 12];
            OsRng
                .try_fill_bytes(&mut nonce)
                .map_err(|_| CryptoError::new(CryptoErrorKind::Internal, "response-nonce-rng"))?;
            let timestamp_ms = system_time_millis(input.now)?;
            // 旧 key 请求已经进入处理时，响应必须继续使用同一快照和 kid；这里按存量使用消费配额，
            // 不重新选择新 active，避免轮换发生在请求中途造成客户端无法关联响应。
            input.context.key.begin_operation(input.now, false)?;
            let aad = build_modern_aad(ModernAadInput {
                direction: CryptoDirection::Response,
                kid: &input.context.key.kid,
                tenant_scope: &input.context.tenant_scope,
                key_scope: &input.context.key_scope,
                audience: &input.context.audience,
                method: input.context.method,
                target: &input.context.target,
                rid: &rid,
                nonce: &nonce,
                timestamp_ms,
                status: Some(input.status),
            })?;
            let encrypted = input
                .context
                .provider
                .encrypt(
                    EncryptInput {
                        plaintext: input.plaintext,
                        nonce: Some(nonce),
                        aad,
                    },
                    CryptoExecutionContext {
                        direction: CryptoDirection::Response,
                        tenant_scope: input.context.tenant_scope.clone(),
                        key_scope: input.context.key_scope.clone(),
                        key: input.context.key.clone(),
                    },
                )
                .await?;
            let encrypted =
                enforce_encrypted_limit(encrypted, input.limits.max_response_ciphertext_bytes)?;
            let envelope = ModernEnvelope {
                v: 2,
                alg: "A256GCM".to_string(),
                kid: input.context.key.kid.to_string(),
                rid: URL_SAFE_NO_PAD.encode(rid),
                nonce: URL_SAFE_NO_PAD.encode(nonce),
                ts: timestamp_ms,
                data: URL_SAFE_NO_PAD.encode(encrypted.ciphertext),
            };
            let encoded = serde_json::to_vec(&envelope).map_err(|_| {
                CryptoError::new(CryptoErrorKind::Internal, "modern-response-envelope-json")
            })?;
            if encoded.len() > input.limits.max_response_envelope_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "modern-response-envelope-too-large",
                ));
            }
            Ok(ProtocolEncryptOutput {
                envelope: encoded,
                content_type: MODERN_V2_MEDIA_TYPE,
            })
        })
    }
}

/// 规范化 AAD 构建所需的可信字段集合。
#[derive(Debug, Clone, Copy)]
pub struct ModernAadInput<'a> {
    /// 请求或响应方向，编码为固定单字节。
    pub direction: CryptoDirection,
    /// 当前有限 key ring 已确认的公开 kid。
    pub kid: &'a str,
    /// 已认证租户域或固定服务域。
    pub tenant_scope: &'a str,
    /// route policy 声明的密钥域。
    pub key_scope: &'a str,
    /// 跨语言稳定的 route audience。
    pub audience: &'a str,
    /// 大写 HTTP 方法。
    pub method: &'a str,
    /// 可信入口确定的 path 与可选 query。
    pub target: &'a str,
    /// 发起方生成的固定 16 字节请求标识。
    pub rid: &'a [u8; 16],
    /// 本次 AEAD 调用独有的 12 字节 nonce。
    pub nonce: &'a [u8; 12],
    /// Unix epoch 毫秒整数。
    pub timestamp_ms: i64,
    /// 响应方向绑定的 HTTP 状态；请求方向必须为 `None`。
    pub status: Option<u16>,
}

/// 按跨语言二进制合同构造 modern-v2 AAD。
///
/// # 参数
///
/// - `input`: 已由路由、认证、key ring 和本地 HTTP 栈确认的固定字段。
///
/// # 返回
///
/// 返回无 JSON 歧义的二进制 AAD；字段过长或方向/status 组合错误会关闭请求。
pub fn build_modern_aad(input: ModernAadInput<'_>) -> Result<Vec<u8>, CryptoError> {
    if (input.direction == CryptoDirection::Request && input.status.is_some())
        || (input.direction == CryptoDirection::Response && input.status.is_none())
    {
        return Err(CryptoError::new(
            CryptoErrorKind::Policy,
            "modern-aad-status-direction",
        ));
    }
    let mut output = Vec::with_capacity(256 + input.target.len());
    output.extend_from_slice(b"nasa-web-crypto");
    output.push(2);
    output.push(match input.direction {
        CryptoDirection::Request => 1,
        CryptoDirection::Response => 2,
    });
    push_lp(&mut output, "A256GCM")?;
    push_lp(&mut output, input.kid)?;
    push_lp(&mut output, input.tenant_scope)?;
    push_lp(&mut output, input.key_scope)?;
    push_lp(&mut output, input.audience)?;
    push_lp(&mut output, input.method)?;
    push_lp(&mut output, input.target)?;
    output.extend_from_slice(input.rid);
    output.extend_from_slice(input.nonce);
    output.extend_from_slice(&input.timestamp_ms.to_be_bytes());
    if let Some(status) = input.status {
        output.extend_from_slice(&status.to_be_bytes());
    }
    Ok(output)
}

/// 追加 `u32 big-endian length + UTF-8 bytes` 字段。
///
/// # 参数
///
/// - `output`: 当前 AAD 拥有缓冲区。
/// - `value`: 经过上游语义验证的文本字段。
///
/// # 返回
///
/// 长度可表示时返回空值，否则返回策略错误。
fn push_lp(output: &mut Vec<u8>, value: &str) -> Result<(), CryptoError> {
    let length = u32::try_from(value.len())
        .map_err(|_| CryptoError::new(CryptoErrorKind::Policy, "modern-aad-field-too-large"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

/// 校验信封字符串字段在 Base64 解码前已经受限。
///
/// # 参数
///
/// - `envelope`: 已通过字段数量和类型检查的信封。
/// - `max_string_bytes`: 任一字符串允许的最大 UTF-8 字节数。
///
/// # 返回
///
/// 全部字段有界时返回空值，否则返回公开大小错误。
fn validate_envelope_strings(
    envelope: &ModernEnvelope,
    max_string_bytes: usize,
) -> Result<(), CryptoError> {
    let bounded = [
        envelope.alg.as_str(),
        envelope.kid.as_str(),
        envelope.rid.as_str(),
        envelope.nonce.as_str(),
        envelope.data.as_str(),
    ]
    .into_iter()
    .all(|value| !value.is_empty() && value.len() <= max_string_bytes);
    if bounded {
        Ok(())
    } else {
        Err(CryptoError::new(
            CryptoErrorKind::TooLarge,
            "modern-envelope-string-limit",
        ))
    }
}

/// 解码无 padding Base64URL 并限制解码后长度。
///
/// # 参数
///
/// - `value`: 信封中的受限编码文本。
/// - `max_decoded`: 解码后二进制上限。
///
/// # 返回
///
/// 合法时返回二进制；padding、字符集或长度错误返回统一信封错误。
fn decode_unpadded(value: &str, max_decoded: usize) -> Result<Vec<u8>, CryptoError> {
    if value.contains('=') || value.len() > max_decoded.saturating_mul(4).saturating_add(3) / 3 {
        return Err(CryptoError::new(
            CryptoErrorKind::Envelope,
            "modern-base64-precheck",
        ));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| CryptoError::new(CryptoErrorKind::Envelope, "modern-base64-decode"))?;
    if decoded.len() > max_decoded {
        Err(CryptoError::new(
            CryptoErrorKind::TooLarge,
            "modern-decoded-too-large",
        ))
    } else {
        Ok(decoded)
    }
}

/// 解码固定长度的无 padding Base64URL 字段。
///
/// # 类型参数
///
/// - `N`: 协议规定的固定二进制字节数。
///
/// # 参数
///
/// - `value`: 信封中的编码文本。
/// - `reason`: 由调用方写死的低基数内部原因。
///
/// # 返回
///
/// 长度与编码严格匹配时返回数组，否则返回信封错误。
fn decode_exact<const N: usize>(value: &str, reason: &'static str) -> Result<[u8; N], CryptoError> {
    let decoded = decode_unpadded(value, N)?;
    decoded
        .try_into()
        .map_err(|_| CryptoError::new(CryptoErrorKind::Envelope, reason))
}

/// 把系统时间转换成协议使用的 Unix epoch 毫秒。
///
/// # 参数
///
/// - `time`: 当前请求固定或响应读取的系统时间。
///
/// # 返回
///
/// 在 epoch 之后且不超过 i64 时返回毫秒，否则返回内部时间错误。
fn system_time_millis(time: SystemTime) -> Result<i64, CryptoError> {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CryptoError::new(CryptoErrorKind::Internal, "clock-before-epoch"))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| CryptoError::new(CryptoErrorKind::Internal, "clock-out-of-range"))
}

/// 在 AEAD 认证成功后检查请求时间窗。
///
/// # 参数
///
/// - `timestamp_ms`: 信封中已认证的 Unix epoch 毫秒。
/// - `now`: 本次请求固定系统时间。
/// - `past_window`: 允许落后服务端的最大时长。
/// - `future_skew`: 允许领先服务端的最大时长。
///
/// # 返回
///
/// 时间在闭区间内返回空值，否则返回过期错误。
fn validate_timestamp(
    timestamp_ms: i64,
    now: SystemTime,
    past_window: std::time::Duration,
    future_skew: std::time::Duration,
) -> Result<(), CryptoError> {
    let now_ms = system_time_millis(now)?;
    let past = i64::try_from(past_window.as_millis()).unwrap_or(i64::MAX);
    let future = i64::try_from(future_skew.as_millis()).unwrap_or(i64::MAX);
    if timestamp_ms < now_ms.saturating_sub(past) || timestamp_ms > now_ms.saturating_add(future) {
        Err(CryptoError::new(
            CryptoErrorKind::Expired,
            "modern-request-clock-window",
        ))
    } else {
        Ok(())
    }
}

/// 解析业务 JSON 并检查实际嵌套深度。
///
/// # 参数
///
/// - `bytes`: 已认证解密或准备加密的完整 JSON 字节。
/// - `max_depth`: 根节点计为一的最大嵌套深度。
///
/// # 返回
///
/// 单个完整 JSON value 且深度有界时返回空值；尾随字节、语法或深度错误均拒绝。
pub fn validate_json(bytes: &[u8], max_depth: usize) -> Result<(), CryptoError> {
    if !json_depth_within_limit(bytes, max_depth) {
        return Err(CryptoError::new(
            CryptoErrorKind::TooLarge,
            "plaintext-json-depth",
        ));
    }
    // `IgnoredAny` 完整消费一个 JSON value 但不构造 `serde_json::Value` 树，避免攻击者用大量短
    // 数组元素把小明文放大成远超内存预算的临时对象。`end` 继续拒绝尾随第二个 value。
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    IgnoredAny::deserialize(&mut deserializer)
        .and_then(|_| deserializer.end())
        .map_err(|_| CryptoError::new(CryptoErrorKind::Envelope, "plaintext-json-invalid"))
}

/// 在不分配 JSON 对象树的前提下预检容器嵌套深度。
///
/// # 参数
///
/// - `bytes`: 尚待严格语法解析的 JSON 字节。
/// - `max_depth`: 根容器或根标量都按深度一计算的上限。
///
/// # 返回
///
/// 扫描过程中未超过上限返回 `true`。字符串内括号和转义不会改变深度；括号配对与其它 JSON
/// 语法仍交给后续 serde 解析器统一判断。
fn json_depth_within_limit(bytes: &[u8], max_depth: usize) -> bool {
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                if depth > max_depth {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}
