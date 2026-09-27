//! # ncrypto —— 密码学工具
//!
//! 本 crate 提供无状态 hash、HMAC、KDF、AES、RSA、Ed25519 与 Base64 能力。业务可直接使用
//! `ncrypto::*`，也可经 `nasa::crypto::*` 门面接入；本 crate 不拥有 Web 路由或运行期密钥状态。
//!
//! ## 字节合同
//! - `sha256` 与 AES-HEX 密文使用大写 hex；`md5`、`sha1`、`sha384`、`sha512`、`hmac*`、`pbkdf2`、
//!   `generate_salt` 和 `generate_aes_key_hex` 使用小写 hex。
//! - AES key 直接取 key 字符串的 UTF-8 字节，不执行 hex 或 Base64 解码，因此长度必须是 16、24 或 32。
//! - AES-CBC 兼容变体使用 key 字节作为 IV；AES-GCM token 布局为 `Base64(IV[12] ‖ ct ‖ tag[16])`。
//! - RSA PKCS1 v1.5 按 key 长度自动分段；公钥使用 X509 SPKI，私钥使用 PKCS8，二者均以 Base64 表示。
//!
//! ## 现代加密入口
//! 所有 salt、AES key、GCM IV 和临时 key 都使用 OS CSPRNG。新写入默认使用 [`encrypt_modern`]：NC2
//! token 以 Argon2id 派生 AES-256-GCM key 并执行认证加密。NC1 PBKDF2-HMAC-SHA256 token 仅保留读取
//! 能力；需要绑定租户、记录或协议上下文时使用 [`encrypt_modern_with_aad`] 和
//! [`decrypt_modern_with_aad`]。
//!
//! ## 安全边界
//! - AES-ECB、固定 IV 的 AES-CBC、RSA 私钥 type-1 运算、MD5 与 SHA-1 仅用于受控互通；新协议应使用
//!   AES-GCM、Ed25519、SHA-256、Argon2id 或 BCrypt 等满足实际威胁模型的能力。
//! - PKCS1 v1.5 私钥解密存在 RUSTSEC-2023-0071 所述时序侧信道风险。默认构建不执行该路径；
//!   `decrypt_rsa_private` 与私钥 type-1 运算必须显式启用 `legacy-rsa-private`，Web 层还需通过风险门禁。

mod encryptor;

pub use encryptor::*;

/// 加密、解密和配置校验使用的统一错误类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CryptoError {
    /// 加密 / 编码失败。
    Encrypt(String),
    /// 解密 / 解码 / 验签失败。
    Decrypt(String),
    /// 配置 / 参数非法(如 RSA keysize < 2048、AES key 长度错)。
    Config(String),
}

impl CryptoError {
    /// 业务作用: 加密 encrypt 数据；用于生成受保护的输出。
    pub(crate) fn encrypt(m: impl Into<String>) -> Self {
        CryptoError::Encrypt(m.into())
    }

    /// 业务作用: 解密 decrypt 数据；用于还原受保护的输入。
    pub(crate) fn decrypt(m: impl Into<String>) -> Self {
        CryptoError::Decrypt(m.into())
    }

    /// 业务作用: 创建加密配置构造器；用于选择算法模式并生成运行参数。
    pub(crate) fn config(m: impl Into<String>) -> Self {
        CryptoError::Config(m.into())
    }
}

impl std::fmt::Display for CryptoError {
    /// 业务作用: 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::Encrypt(m) => write!(f, "crypto encrypt error: {m}"),
            CryptoError::Decrypt(m) => write!(f, "crypto decrypt error: {m}"),
            CryptoError::Config(m) => write!(f, "crypto config error: {m}"),
        }
    }
}

impl std::error::Error for CryptoError {}

/// 本 crate 统一 `Result`；所有可失败 API 返回该类型而不触发 panic。
pub type Result<T> = std::result::Result<T, CryptoError>;

/// AES 加密模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AesMode {
    /// `AES/ECB/PKCS5Padding`(默认;相同明文→相同密文,弱)。
    EcbPkcs5,
    /// `AES/CBC/PKCS5Padding`,IV = key 字节(默认变体;IV 不应等于 key,弱)。
    CbcPkcs5,
}

/// AES 输出与输入编码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncOutput {
    /// 标准 Base64(带填充)。
    Base64,
    /// 大写 hex。
    Hex,
}
