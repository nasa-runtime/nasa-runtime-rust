//! 密码算法 provider 合同与本地受限实现。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use zeroize::Zeroizing;

use super::{CryptoCpuExecutor, CryptoError, CryptoErrorKind, KeyAlgorithm, KeyEntry};

/// provider 对外声明的有限算法能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CryptoCapabilities {
    /// 是否支持 modern-v2 的 A256GCM 与 HKDF 方向派生。
    pub modern_aead: bool,
    /// 是否支持 legacy-v1 AES 兼容路径。
    pub legacy_aes: bool,
    /// 是否支持存在已知侧信道风险的 legacy 私钥路径。
    pub legacy_private_operations: bool,
}

/// provider 的密码操作方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoDirection {
    /// 客户端发送、服务端解密的请求方向。
    Request,
    /// 服务端生成、客户端解密的响应方向。
    Response,
}

impl CryptoDirection {
    /// 返回 HKDF 合同使用的固定小写方向文本。
    ///
    /// # 返回
    ///
    /// 请求返回 `request`，响应返回 `response`。
    pub const fn as_key_label(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

/// 单次 provider 调用固定持有的可信执行上下文。
#[derive(Clone)]
pub struct CryptoExecutionContext {
    /// 请求或响应方向，用于 HKDF 域隔离。
    pub direction: CryptoDirection,
    /// 已认证租户域或固定服务域，不得来自未认证 body。
    pub tenant_scope: Arc<str>,
    /// route policy 声明的密钥域。
    pub key_scope: Arc<str>,
    /// 已在有限 key ring 中解析并消费调用配额的记录。
    pub key: Arc<KeyEntry>,
}

impl std::fmt::Debug for CryptoExecutionContext {
    /// 输出不含密钥材料的操作摘要。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CryptoExecutionContext")
            .field("direction", &self.direction)
            .field("key_generation", &self.key.generation)
            .finish_non_exhaustive()
    }
}

/// provider 解密调用拥有的全部输入。
pub struct DecryptInput {
    /// 协议层已经严格解码的二进制密文；legacy 路径保存受限 Base64 文本字节。
    pub ciphertext: Vec<u8>,
    /// RSA_AES 请求携带的受限会话 key 包装文本，其它算法必须为 `None`。
    pub auxiliary: Option<Vec<u8>>,
    /// modern-v2 的 12 字节 nonce；legacy 算法必须为 `None`。
    pub nonce: Option<[u8; 12]>,
    /// 协议层规范化生成的 AAD；legacy 算法使用空值。
    pub aad: Vec<u8>,
}

/// provider 加密调用拥有的全部输入。
pub struct EncryptInput {
    /// 已通过协议明文上限检查的敏感响应字节。
    pub plaintext: Zeroizing<Vec<u8>>,
    /// modern-v2 响应新生成的 12 字节 nonce；legacy 算法必须为 `None`。
    pub nonce: Option<[u8; 12]>,
    /// 协议层规范化生成的 AAD；legacy 算法使用空值。
    pub aad: Vec<u8>,
}

/// provider 返回的敏感明文字节容器。
pub struct SecretBytes {
    bytes: Zeroizing<Vec<u8>>,
}

impl SecretBytes {
    /// 接管 provider 已解密的拥有字节。
    ///
    /// # 参数
    ///
    /// - `bytes`: 尚未交给 JSON extractor 的敏感明文。
    ///
    /// # 返回
    ///
    /// 返回 best-effort 清零容器。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// 把明文移动给 request body 重写阶段。
    ///
    /// # 返回
    ///
    /// 返回拥有字节；移动后容器不再保留副本。
    pub fn into_vec(self) -> Vec<u8> {
        let mut bytes = self.bytes;
        // 所有权直接转交给下一阶段，避免 `to_vec` 在大请求上额外制造一份同时驻留的敏感明文。
        // wrapper 随后只清零已经被取空的容器；成功路径中的新所有者仍受 endpoint 生命周期和
        // 全局内存 permit 约束。
        std::mem::take(&mut *bytes)
    }
}

impl std::fmt::Debug for SecretBytes {
    /// 只输出明文长度，避免调试链泄露内容。
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
            .debug_struct("SecretBytes")
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// provider 返回的协议层待编码密文与可选辅助字段。
pub struct EncryptedBytes {
    /// modern-v2 为原始 `ciphertext || tag`，legacy-v1 为标准 Base64 文本字节。
    pub ciphertext: Vec<u8>,
    /// RSA_AES 响应的会话 key 包装文本，其它算法为 `None`。
    pub auxiliary: Option<Vec<u8>>,
}

/// provider 对象安全异步返回值。
pub type CryptoFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 与线协议解析、HTTP 和 key 选择解耦的密码执行扩展点。
pub trait WebCryptoProvider: Send + Sync + 'static {
    /// 返回注册表使用的稳定 provider ID。
    ///
    /// # 返回
    ///
    /// 返回非空、有限长度的 ASCII 标识，不得包含地址和 secret。
    fn id(&self) -> &'static str;

    /// 返回启动审计使用的静态能力集合。
    ///
    /// # 返回
    ///
    /// 返回 provider 能安全执行的算法边界。
    fn capabilities(&self) -> CryptoCapabilities;

    /// 解密协议层已经严格验证的输入。
    ///
    /// # 参数
    ///
    /// - `input`: 拥有全部密文、辅助字段、nonce 与 AAD，大小已由协议层限制。
    /// - `context`: 固定请求快照解析的方向、租户、密钥域和 key handle。
    ///
    /// # 返回
    ///
    /// 成功返回敏感明文；算法、tag 和底层错误必须收敛，禁止 plaintext fallback。
    fn decrypt<'a>(
        &'a self,
        input: DecryptInput,
        context: CryptoExecutionContext,
    ) -> CryptoFuture<'a, Result<SecretBytes, CryptoError>>;

    /// 加密通过响应合同检查的明文。
    ///
    /// # 参数
    ///
    /// - `input`: 拥有敏感明文、响应 nonce 与规范化 AAD 的有界输入。
    /// - `context`: 固定请求快照解析的响应方向与同一 key handle。
    ///
    /// # 返回
    ///
    /// 成功返回协议层待编码密文；资源或算法故障一律 fail closed。
    fn encrypt<'a>(
        &'a self,
        input: EncryptInput,
        context: CryptoExecutionContext,
    ) -> CryptoFuture<'a, Result<EncryptedBytes, CryptoError>>;
}

/// 使用 `ncrypto` 纯算法并通过有界执行器隔离同步工作的本地 provider。
pub struct LocalCryptoProvider {
    id: &'static str,
    executor: CryptoCpuExecutor,
    allow_insecure_legacy_private_operations: bool,
}

impl LocalCryptoProvider {
    /// 建立本地纯算法 provider。
    ///
    /// # 参数
    ///
    /// - `id`: 注册表稳定 ID，建议按部署能力区分。
    /// - `executor`: 所有同步算法共享的有界阻塞执行器。
    /// - `allow_insecure_legacy_private_operations`: 仅受控迁移环境可显式开启的 RSA 私钥风险门。
    ///
    /// # 返回
    ///
    /// 返回不可变 provider；风险门关闭时 legacy RSA/RSA_AES 审计和执行都应被拒绝。
    pub fn new(
        id: &'static str,
        executor: CryptoCpuExecutor,
        allow_insecure_legacy_private_operations: bool,
    ) -> Self {
        Self {
            id,
            executor,
            allow_insecure_legacy_private_operations,
        }
    }
}

impl WebCryptoProvider for LocalCryptoProvider {
    /// 返回构造时声明的稳定 ID。
    ///
    /// # 返回
    ///
    /// 返回注册表索引文本。
    fn id(&self) -> &'static str {
        self.id
    }

    /// 声明本地 provider 的静态算法能力。
    ///
    /// # 返回
    ///
    /// modern 与 legacy AES 始终可用；legacy 私钥能力只有编译期 feature 与运行时风险门
    /// 同时开启时可见。
    fn capabilities(&self) -> CryptoCapabilities {
        CryptoCapabilities {
            modern_aead: true,
            legacy_aes: true,
            legacy_private_operations: cfg!(feature = "legacy-rsa-private")
                && self.allow_insecure_legacy_private_operations,
        }
    }

    /// 在有界阻塞执行器中执行本地解密。
    ///
    /// # 参数
    ///
    /// - `input`: 协议层验证后的拥有输入，闭包结束时统一释放。
    /// - `context`: 固定 key 与方向元数据，不跨快照重新解析。
    ///
    /// # 返回
    ///
    /// 返回敏感明文；所有底层错误统一为认证失败，避免形成算法 oracle。
    fn decrypt<'a>(
        &'a self,
        input: DecryptInput,
        context: CryptoExecutionContext,
    ) -> CryptoFuture<'a, Result<SecretBytes, CryptoError>> {
        let executor = self.executor.clone();
        let allow_private = self.allow_insecure_legacy_private_operations;
        Box::pin(async move {
            executor
                .execute(move || decrypt_local(input, context, allow_private))
                .await
        })
    }

    /// 在有界阻塞执行器中执行本地加密。
    ///
    /// # 参数
    ///
    /// - `input`: 已限制大小的拥有明文和协议元数据。
    /// - `context`: 固定 key 与方向元数据，不跨快照重新选择 active key。
    ///
    /// # 返回
    ///
    /// 返回待协议编码密文；随机源或算法故障一律关闭响应。
    fn encrypt<'a>(
        &'a self,
        input: EncryptInput,
        context: CryptoExecutionContext,
    ) -> CryptoFuture<'a, Result<EncryptedBytes, CryptoError>> {
        let executor = self.executor.clone();
        let allow_private = self.allow_insecure_legacy_private_operations;
        Box::pin(async move {
            executor
                .execute(move || encrypt_local(input, context, allow_private))
                .await
        })
    }
}

/// 执行按 key 算法分派的同步解密。
///
/// # 参数
///
/// - `input`: 协议层有界输入。
/// - `context`: 固定 key 和派生域。
/// - `allow_private`: legacy 私钥风险门，只能来自启动配置。
///
/// # 返回
///
/// 成功返回明文，任何底层差异统一映射为认证失败。
fn decrypt_local(
    input: DecryptInput,
    context: CryptoExecutionContext,
    allow_private: bool,
) -> Result<SecretBytes, CryptoError> {
    let failed = || CryptoError::new(CryptoErrorKind::Authentication, "provider-decrypt-failed");
    match context.key.algorithm {
        KeyAlgorithm::A256Gcm => {
            let nonce = input.nonce.ok_or_else(failed)?;
            let derived = Zeroizing::new(
                ncrypto::derive_web_aead_key(
                    context.key.material(),
                    context.direction.as_key_label(),
                    &context.tenant_scope,
                    &context.key_scope,
                    &context.key.kid,
                )
                .map_err(|_| failed())?,
            );
            let plaintext =
                ncrypto::decrypt_web_aead(derived.as_ref(), &nonce, &input.ciphertext, &input.aad)
                    .map_err(|_| failed())?;
            Ok(SecretBytes::new(plaintext))
        }
        KeyAlgorithm::LegacyAes => {
            let cipher = std::str::from_utf8(&input.ciphertext).map_err(|_| failed())?;
            let key = std::str::from_utf8(context.key.material()).map_err(|_| failed())?;
            let plaintext = ncrypto::decrypt_aes(cipher, key).map_err(|_| failed())?;
            Ok(SecretBytes::new(plaintext.into_bytes()))
        }
        KeyAlgorithm::LegacyRsa if allow_private => {
            let cipher = std::str::from_utf8(&input.ciphertext).map_err(|_| failed())?;
            let key = std::str::from_utf8(context.key.material()).map_err(|_| failed())?;
            let plaintext = ncrypto::decrypt_rsa_private(cipher, key).map_err(|_| failed())?;
            Ok(SecretBytes::new(plaintext.into_bytes()))
        }
        KeyAlgorithm::LegacyRsaAes if allow_private => {
            let wrapped = input.auxiliary.ok_or_else(failed)?;
            let wrapped = std::str::from_utf8(&wrapped).map_err(|_| failed())?;
            let cipher = std::str::from_utf8(&input.ciphertext).map_err(|_| failed())?;
            let private_key = std::str::from_utf8(context.key.material()).map_err(|_| failed())?;
            let session_key = Zeroizing::new(
                ncrypto::decrypt_rsa_private(wrapped, private_key).map_err(|_| failed())?,
            );
            let plaintext =
                ncrypto::decrypt_aes(cipher, session_key.as_str()).map_err(|_| failed())?;
            Ok(SecretBytes::new(plaintext.into_bytes()))
        }
        KeyAlgorithm::LegacyRsa | KeyAlgorithm::LegacyRsaAes => Err(CryptoError::new(
            CryptoErrorKind::Policy,
            "legacy-private-operation-disabled",
        )),
    }
}

/// 执行按 key 算法分派的同步加密。
///
/// # 参数
///
/// - `input`: 协议层有界明文、nonce 与 AAD。
/// - `context`: 固定 key 和派生域。
/// - `allow_private`: legacy 私钥风险门，只能来自启动配置。
///
/// # 返回
///
/// 成功返回密文与辅助字段，所有底层错误统一收敛。
fn encrypt_local(
    input: EncryptInput,
    context: CryptoExecutionContext,
    allow_private: bool,
) -> Result<EncryptedBytes, CryptoError> {
    let failed = || CryptoError::new(CryptoErrorKind::Internal, "provider-encrypt-failed");
    match context.key.algorithm {
        KeyAlgorithm::A256Gcm => {
            let nonce = input.nonce.ok_or_else(failed)?;
            let derived = Zeroizing::new(
                ncrypto::derive_web_aead_key(
                    context.key.material(),
                    context.direction.as_key_label(),
                    &context.tenant_scope,
                    &context.key_scope,
                    &context.key.kid,
                )
                .map_err(|_| failed())?,
            );
            let ciphertext =
                ncrypto::encrypt_web_aead(derived.as_ref(), &nonce, &input.plaintext, &input.aad)
                    .map_err(|_| failed())?;
            Ok(EncryptedBytes {
                ciphertext,
                auxiliary: None,
            })
        }
        KeyAlgorithm::LegacyAes => {
            let plaintext = std::str::from_utf8(&input.plaintext).map_err(|_| failed())?;
            let key = std::str::from_utf8(context.key.material()).map_err(|_| failed())?;
            let ciphertext = ncrypto::encrypt_aes(plaintext, key).map_err(|_| failed())?;
            Ok(EncryptedBytes {
                ciphertext: ciphertext.into_bytes(),
                auxiliary: None,
            })
        }
        KeyAlgorithm::LegacyRsa if allow_private => {
            let plaintext = std::str::from_utf8(&input.plaintext).map_err(|_| failed())?;
            let key = std::str::from_utf8(context.key.material()).map_err(|_| failed())?;
            let ciphertext = ncrypto::encrypt_rsa_private(plaintext, key).map_err(|_| failed())?;
            Ok(EncryptedBytes {
                ciphertext: ciphertext.into_bytes(),
                auxiliary: None,
            })
        }
        KeyAlgorithm::LegacyRsaAes if allow_private => {
            let plaintext = std::str::from_utf8(&input.plaintext).map_err(|_| failed())?;
            let key = std::str::from_utf8(context.key.material()).map_err(|_| failed())?;
            let (wrapped, ciphertext) =
                ncrypto::rsa_aes_seal(plaintext, key, None).map_err(|_| failed())?;
            Ok(EncryptedBytes {
                ciphertext: ciphertext.into_bytes(),
                auxiliary: Some(wrapped.into_bytes()),
            })
        }
        KeyAlgorithm::LegacyRsa | KeyAlgorithm::LegacyRsaAes => Err(CryptoError::new(
            CryptoErrorKind::Policy,
            "legacy-private-operation-disabled",
        )),
    }
}
