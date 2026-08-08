//! legacy-v1 严格信封和 `BaseResponse.data` 兼容适配。

use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};
use zeroize::Zeroizing;

use super::{
    CryptoDirection, CryptoError, CryptoErrorKind, CryptoExecutionContext, CryptoFuture,
    CryptoProtocol, DecryptInput, EncryptInput, KeyAlgorithm, ProtocolDecryptInput,
    ProtocolDecryptOutput, ProtocolEncryptInput, ProtocolEncryptOutput, RequestCryptoContext,
};

/// legacy-v1 请求和响应使用的 JSON 媒体类型。
pub const LEGACY_V1_MEDIA_TYPE: &str = "application/json";

/// legacy-v1 严格请求信封。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyEnvelope {
    /// 仅 RSA_AES 请求允许携带的标准 Base64 会话 key 包装文本；显式 null 不等同于字段缺失。
    #[serde(default, deserialize_with = "deserialize_legacy_aes_field")]
    aes: LegacyAesField,
    /// 必填且非空的标准 Base64 密文文本。
    data: String,
}

/// legacy 请求中 `aes` 字段的三态，避免 Serde `Option` 把显式 null 与缺失字段合并。
#[derive(Default)]
enum LegacyAesField {
    /// 非 RSA_AES 请求要求字段完全缺失。
    #[default]
    Missing,
    /// 字段存在但值为 null；严格信封拒绝这种歧义表达。
    Null,
    /// RSA_AES 请求携带的非空包装文本。
    Value(String),
}

/// 业务作用：只在字段实际出现时区分 JSON null 与字符串；字段缺失由 `Default` 产生。
fn deserialize_legacy_aes_field<'de, D>(deserializer: D) -> Result<LegacyAesField, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|value| match value {
        Some(value) => LegacyAesField::Value(value),
        None => LegacyAesField::Null,
    })
}

/// legacy-v1 响应序列化所需的受控字段集合。
#[derive(Serialize)]
struct LegacyResponseEnvelope {
    /// 原业务响应码，保持明文且不提供协议级完整性。
    code: serde_json::Value,
    /// 原业务提示字段；缺失时继续缺失。
    #[serde(skip_serializing_if = "Option::is_none")]
    msg: Option<serde_json::Value>,
    /// 仅 RSA_AES 响应生成的会话 key 包装文本。
    #[serde(skip_serializing_if = "Option::is_none")]
    aes: Option<String>,
    /// 被改写为标准 Base64 字符串的 data；缺失或 null 时保持原状态。
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

/// legacy-v1 无状态协议实现。
#[derive(Debug, Default)]
pub struct LegacyV1Protocol;

impl LegacyV1Protocol {
    /// 业务作用：建立无可变状态的 legacy-v1 协议对象。
    ///
    /// # 返回
    ///
    /// 返回只应在受控兼容路由注册的协议实现。
    pub const fn new() -> Self {
        Self
    }
}

impl CryptoProtocol for LegacyV1Protocol {
    /// 业务作用：返回静态兼容协议 ID。
    ///
    /// # 返回
    ///
    /// 返回 route policy 使用的 `legacy-v1`。
    fn id(&self) -> &'static str {
        "legacy-v1"
    }

    /// 业务作用：返回 legacy 信封要求的 JSON 媒体类型。
    ///
    /// # 返回
    ///
    /// 返回 `application/json`。
    fn request_content_type(&self) -> &'static str {
        LEGACY_V1_MEDIA_TYPE
    }

    /// 业务作用：严格解析旧信封并按 active 加至多一个 previous key 解密。
    ///
    /// # 参数
    ///
    /// - `input`: 请求固定快照提供的有界信封、ring 与 provider。
    ///
    /// # 返回
    ///
    /// 成功返回单个完整业务 JSON；失败不回退明文，也不遍历无界历史 key。
    fn decrypt<'a>(
        &'a self,
        input: ProtocolDecryptInput,
    ) -> CryptoFuture<'a, Result<ProtocolDecryptOutput, CryptoError>> {
        Box::pin(async move {
            if input.envelope.len() > input.limits.max_request_envelope_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "legacy-request-envelope-too-large",
                ));
            }
            let envelope: LegacyEnvelope = serde_json::from_slice(&input.envelope)
                .map_err(|_| CryptoError::new(CryptoErrorKind::Envelope, "legacy-envelope-json"))?;
            if matches!(envelope.aes, LegacyAesField::Null) {
                return Err(CryptoError::new(
                    CryptoErrorKind::Envelope,
                    "legacy-aes-null",
                ));
            }
            let wrapped_aes = match &envelope.aes {
                LegacyAesField::Value(value) => Some(value),
                LegacyAesField::Missing | LegacyAesField::Null => None,
            };
            if envelope.data.is_empty()
                || envelope.data.len() > input.limits.max_string_bytes
                || wrapped_aes.is_some_and(|value| {
                    value.is_empty() || value.len() > input.limits.max_string_bytes
                })
            {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "legacy-envelope-string-limit",
                ));
            }
            if decoded_base64_upper_bound(envelope.data.len()) > input.limits.max_ciphertext_bytes
                || wrapped_aes.is_some_and(|value| {
                    decoded_base64_upper_bound(value.len()) > input.limits.max_ciphertext_bytes
                })
            {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "legacy-decoded-ciphertext-limit",
                ));
            }
            let candidates = input.key_ring.legacy_decrypt_candidates(input.now)?;
            let key_scope =
                Arc::<str>::from(input.route.crypto_key_scope.ok_or_else(|| {
                    CryptoError::new(CryptoErrorKind::Policy, "key-scope-missing")
                })?);
            let mut selected = None;
            let mut decrypted = None;
            for key in candidates {
                let aes_rule_valid = match key.algorithm {
                    KeyAlgorithm::LegacyRsaAes => wrapped_aes.is_some(),
                    KeyAlgorithm::LegacyAes | KeyAlgorithm::LegacyRsa => wrapped_aes.is_none(),
                    KeyAlgorithm::A256Gcm => false,
                };
                if !aes_rule_valid {
                    return Err(CryptoError::new(
                        CryptoErrorKind::Envelope,
                        "legacy-aes-field-rule",
                    ));
                }
                // 旧协议没有 kid，只能按固定候选顺序尝试。额度必须紧邻真正的 provider 调用消费；
                // 若在构造候选集时提前扣除，active 成功也会无故耗尽 previous key 的迁移窗口。
                key.begin_operation(input.now, false)?;
                let result = input
                    .provider
                    .decrypt(
                        DecryptInput {
                            ciphertext: envelope.data.as_bytes().to_vec(),
                            auxiliary: wrapped_aes.map(|value| value.as_bytes().to_vec()),
                            nonce: None,
                            aad: Vec::new(),
                        },
                        CryptoExecutionContext {
                            direction: CryptoDirection::Request,
                            tenant_scope: input.tenant_scope.clone(),
                            key_scope: key_scope.clone(),
                            key: key.clone(),
                        },
                    )
                    .await;
                match result {
                    Ok(plaintext) => {
                        selected = Some(key);
                        decrypted = Some(plaintext);
                        break;
                    }
                    Err(error) if error.kind == CryptoErrorKind::Authentication => continue,
                    Err(error) => return Err(error),
                }
            }
            let key = selected.ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::Authentication, "legacy-all-keys-failed")
            })?;
            let plaintext = decrypted.ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::Internal, "legacy-plaintext-missing")
            })?;
            let plaintext_bytes = plaintext.into_vec();
            if plaintext_bytes.len() > input.limits.max_plaintext_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "legacy-plaintext-too-large",
                ));
            }
            super::validate_json(&plaintext_bytes, input.limits.max_json_depth)?;
            Ok(ProtocolDecryptOutput {
                plaintext: super::SecretBytes::new(plaintext_bytes),
                context: RequestCryptoContext {
                    protocol_id: self.id(),
                    rid: None,
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

    /// 业务作用：严格验证 `BaseResponse` 并只改写非空 data 字段。
    ///
    /// # 参数
    ///
    /// - `input`: handler 响应和请求阶段固定的 legacy key/provider 上下文。
    ///
    /// # 返回
    ///
    /// 合同正确时返回兼容 JSON；未知结构或加密失败不透传原明文。
    fn encrypt<'a>(
        &'a self,
        input: ProtocolEncryptInput,
    ) -> CryptoFuture<'a, Result<ProtocolEncryptOutput, CryptoError>> {
        Box::pin(async move {
            if input.plaintext.len() > input.limits.max_response_plaintext_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "legacy-response-plaintext-too-large",
                ));
            }
            let mut object = match serde_json::from_slice::<serde_json::Value>(&input.plaintext)
                .map_err(|_| {
                    CryptoError::new(CryptoErrorKind::ResponseContract, "legacy-response-json")
                })? {
                serde_json::Value::Object(object) => object,
                _ => {
                    return Err(CryptoError::new(
                        CryptoErrorKind::ResponseContract,
                        "legacy-response-not-object",
                    ))
                }
            };
            if !object.contains_key("code")
                || object
                    .keys()
                    .any(|key| !matches!(key.as_str(), "code" | "msg" | "aes" | "data"))
            {
                return Err(CryptoError::new(
                    CryptoErrorKind::ResponseContract,
                    "legacy-response-fields",
                ));
            }
            let code = object.remove("code").ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::ResponseContract, "legacy-response-code")
            })?;
            if code
                .as_i64()
                .is_none_or(|value| i32::try_from(value).is_err())
            {
                return Err(CryptoError::new(
                    CryptoErrorKind::ResponseContract,
                    "legacy-response-code-type",
                ));
            }
            let msg = object.remove("msg");
            if msg
                .as_ref()
                .is_some_and(|value| !value.is_null() && !value.is_string())
            {
                return Err(CryptoError::new(
                    CryptoErrorKind::ResponseContract,
                    "legacy-response-msg-type",
                ));
            }
            let existing_aes = object.remove("aes");
            if existing_aes.as_ref().is_some_and(|value| !value.is_null()) {
                return Err(CryptoError::new(
                    CryptoErrorKind::ResponseContract,
                    "legacy-response-aes-prepopulated",
                ));
            }
            let data = object.remove("data");
            if data.as_ref().is_none_or(serde_json::Value::is_null) {
                let envelope = LegacyResponseEnvelope {
                    code,
                    msg,
                    aes: None,
                    data,
                };
                return encode_response(envelope, &input.limits);
            }
            let data_value = data.as_ref().ok_or_else(|| {
                CryptoError::new(CryptoErrorKind::ResponseContract, "legacy-response-data")
            })?;
            let data_json = serde_json::to_vec(data_value).map_err(|_| {
                CryptoError::new(CryptoErrorKind::ResponseContract, "legacy-data-json")
            })?;
            input.context.key.begin_operation(input.now, false)?;
            let encrypted = input
                .context
                .provider
                .encrypt(
                    EncryptInput {
                        plaintext: Zeroizing::new(data_json),
                        nonce: None,
                        aad: Vec::new(),
                    },
                    CryptoExecutionContext {
                        direction: CryptoDirection::Response,
                        tenant_scope: input.context.tenant_scope.clone(),
                        key_scope: input.context.key_scope.clone(),
                        key: input.context.key.clone(),
                    },
                )
                .await?;
            if encrypted.ciphertext.len() > input.limits.max_response_ciphertext_bytes {
                return Err(CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "legacy-response-ciphertext-too-large",
                ));
            }
            let ciphertext = String::from_utf8(encrypted.ciphertext).map_err(|_| {
                CryptoError::new(CryptoErrorKind::Internal, "legacy-response-ciphertext-text")
            })?;
            let aes = encrypted
                .auxiliary
                .map(String::from_utf8)
                .transpose()
                .map_err(|_| {
                    CryptoError::new(CryptoErrorKind::Internal, "legacy-response-aes-text")
                })?;
            let envelope = LegacyResponseEnvelope {
                code,
                msg,
                aes,
                data: Some(serde_json::Value::String(ciphertext)),
            };
            encode_response(envelope, &input.limits)
        })
    }
}

/// 业务作用：计算标准 Base64 文本解码后的保守字节上界，不解析或分配输出缓冲区。
///
/// # 参数
///
/// - `encoded_len`: 外层严格 JSON 字符串的 UTF-8 字节数；标准 Base64 合法输入全部是 ASCII。
///
/// # 返回
///
/// 返回按每四字符至多三个字节计算并额外覆盖非完整末组的饱和值。该上界只用于在 provider 前
/// 执行资源门禁，实际编码合法性仍由底层统一认证错误收敛。
fn decoded_base64_upper_bound(encoded_len: usize) -> usize {
    encoded_len
        .saturating_add(3)
        .saturating_div(4)
        .saturating_mul(3)
}

/// 业务作用：序列化 legacy 响应并应用最终信封上限。
///
/// # 参数
///
/// - `envelope`: 已完成 `BaseResponse` 字段检查与 data 改写的结构。
/// - `limits`: 当前请求固定快照的响应大小限制。
///
/// # 返回
///
/// 在上限内返回 JSON 响应，否则返回大小或内部序列化错误。
fn encode_response(
    envelope: LegacyResponseEnvelope,
    limits: &super::CryptoLimits,
) -> Result<ProtocolEncryptOutput, CryptoError> {
    let encoded = serde_json::to_vec(&envelope).map_err(|_| {
        CryptoError::new(CryptoErrorKind::Internal, "legacy-response-envelope-json")
    })?;
    if encoded.len() > limits.max_response_envelope_bytes {
        return Err(CryptoError::new(
            CryptoErrorKind::TooLarge,
            "legacy-response-envelope-too-large",
        ));
    }
    Ok(ProtocolEncryptOutput {
        envelope: encoded,
        content_type: LEGACY_V1_MEDIA_TYPE,
    })
}
