//! 线协议信封与密码 provider 之间的对象安全合同。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Bytes;
use zeroize::Zeroizing;

use crate::RoutePolicy;

use super::{CryptoError, EncryptedBytes};
use super::{
    CryptoFuture, CryptoLimits, KeyEntry, KeyRing, ReplayGuard, SecretBytes, WebCryptoProvider,
};

/// 协议处理请求所需的完整固定输入。
pub struct ProtocolDecryptInput {
    /// 已在 HTTP 层按上限读取的外层信封字节。
    pub envelope: Bytes,
    /// 已匹配路由的静态安全合同。
    pub route: RoutePolicy,
    /// 应用实际接收的原始 path 与可选 query，不读取客户端可伪造转发头。
    pub target: Arc<str>,
    /// auth 提供的已认证租户域或单租户固定域。
    pub tenant_scope: Arc<str>,
    /// 当前不可变快照中的有限 key ring。
    pub key_ring: Arc<KeyRing>,
    /// 启动审计已确认能力匹配的 provider。
    pub provider: Arc<dyn WebCryptoProvider>,
    /// required route 使用的共享重放存储；legacy 或关闭重放时为 `None`。
    pub replay_guard: Option<Arc<dyn ReplayGuard>>,
    /// rid 原子占位的保留时间。
    pub replay_ttl: Duration,
    /// 单次重放存储调用允许占用的最长时间。
    pub replay_timeout: Duration,
    /// 本次请求固定的系统时间，时间窗和 key 有效期都使用它。
    pub now: SystemTime,
    /// 当前快照的单请求资源限制。
    pub limits: Arc<CryptoLimits>,
}

/// 协议解密成功后固定到本请求响应阶段的上下文。
#[derive(Clone)]
pub struct RequestCryptoContext {
    /// 静态协议 ID，响应阶段禁止重新协商。
    pub protocol_id: &'static str,
    /// 请求 rid；modern-v2 响应必须沿用，legacy 为 `None`。
    pub rid: Option<[u8; 16]>,
    /// 请求开始时解析并消费配额的 key；响应不跨快照重新选择。
    pub key: Arc<KeyEntry>,
    /// 已认证租户域或固定服务域。
    pub tenant_scope: Arc<str>,
    /// route policy 声明的密钥域。
    pub key_scope: Arc<str>,
    /// 公开且跨语言稳定的 AAD 受众。
    pub audience: Arc<str>,
    /// 大写 HTTP 方法。
    pub method: &'static str,
    /// 原始 path 与可选 query 的可信表示。
    pub target: Arc<str>,
    /// 完成本次请求和响应密码操作的同一 provider。
    pub provider: Arc<dyn WebCryptoProvider>,
}

impl std::fmt::Debug for RequestCryptoContext {
    /// 业务作用：输出不含 key、rid 与租户原值的协议摘要。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回脱敏格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestCryptoContext")
            .field("protocol_id", &self.protocol_id)
            .field("method", &self.method)
            .field("key_generation", &self.key.generation)
            .finish_non_exhaustive()
    }
}

/// 协议请求解密的规范化输出。
pub struct ProtocolDecryptOutput {
    /// 即将写回 HTTP request body 的敏感业务 JSON。
    pub plaintext: SecretBytes,
    /// handler 完成后响应加密必须继续持有的固定上下文。
    pub context: RequestCryptoContext,
}

/// 协议处理响应所需的固定输入。
pub struct ProtocolEncryptInput {
    /// handler 返回且已通过上限检查的完整响应字节。
    pub plaintext: Zeroizing<Vec<u8>>,
    /// handler 返回的 HTTP 状态，modern-v2 会绑定进响应 AAD。
    pub status: u16,
    /// 请求解密阶段建立的固定上下文。
    pub context: RequestCryptoContext,
    /// 响应阶段读取的系统时间，只用于时间戳和 key 有效期检查。
    pub now: SystemTime,
    /// 当前请求固定快照中的资源限制。
    pub limits: Arc<CryptoLimits>,
}

/// 协议响应加密后的 HTTP 实体。
pub struct ProtocolEncryptOutput {
    /// 需要写回 response body 的完整外层信封字节。
    pub envelope: Vec<u8>,
    /// route layer 必须设置的精确响应 Content-Type。
    pub content_type: &'static str,
}

/// 严格线协议扩展点，负责信封、AAD、时间窗和重放顺序。
pub trait CryptoProtocol: Send + Sync + 'static {
    /// 业务作用：返回 route policy 使用的稳定协议 ID。
    ///
    /// # 返回
    ///
    /// 返回非空、有限长度的 ASCII 标识。
    fn id(&self) -> &'static str;

    /// 业务作用：返回请求外层信封要求的精确 Content-Type。
    ///
    /// # 返回
    ///
    /// 返回静态媒体类型；调用方只允许协议明确声明的参数。
    fn request_content_type(&self) -> &'static str;

    /// 业务作用：严格解析、认证并解密请求信封。
    ///
    /// # 参数
    ///
    /// - `input`: 拥有信封并固定 route、target、tenant、ring、provider 与重放存储的完整输入。
    ///
    /// # 返回
    ///
    /// 成功返回业务 JSON 和响应上下文；失败不允许按明文或另一协议重试。
    fn decrypt<'a>(
        &'a self,
        input: ProtocolDecryptInput,
    ) -> CryptoFuture<'a, Result<ProtocolDecryptOutput, CryptoError>>;

    /// 业务作用：按请求固定上下文加密 handler 响应。
    ///
    /// # 参数
    ///
    /// - `input`: 拥有完整响应明文、状态与请求固定上下文的有界输入。
    ///
    /// # 返回
    ///
    /// 成功返回完整外层信封与媒体类型；合同不匹配时禁止透传半处理响应。
    fn encrypt<'a>(
        &'a self,
        input: ProtocolEncryptInput,
    ) -> CryptoFuture<'a, Result<ProtocolEncryptOutput, CryptoError>>;
}

/// 业务作用：确认 provider 返回值在协议编码前没有异常膨胀。
///
/// # 参数
///
/// - `encrypted`: provider 返回的密文与可选辅助字段。
/// - `max_ciphertext_bytes`: 当前方向允许的二进制密文上限。
///
/// # 返回
///
/// 在上限内原样返回；超限返回公开大小错误。
pub fn enforce_encrypted_limit(
    encrypted: EncryptedBytes,
    max_ciphertext_bytes: usize,
) -> Result<EncryptedBytes, CryptoError> {
    if encrypted.ciphertext.len() > max_ciphertext_bytes {
        Err(CryptoError::new(
            super::CryptoErrorKind::TooLarge,
            "provider-ciphertext-too-large",
        ))
    } else {
        Ok(encrypted)
    }
}
