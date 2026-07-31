//! # ncrypto —— 密码学工具(L1)
//!
//! 对照 原实现 `原工具包` 的 `Encryptor` 静态工具类移植。
//! 本 crate 是 **L1 纯函数工具**:hash / hmac / pbkdf2 / aes / rsa / ed25519 / base64 —— 无 self、无注解、
//! 无 web 依赖,直接 `use ncrypto::*` 调用(经 `nasa` 门面接入后为 `use nasa::crypto::*`,门面模块名仍 `crypto`)。
//! crate 命名 `ncrypto`(非裸 `crypto`)以避开 RustCrypto 生态 `crypto` 门面 crate 的同包名碰撞。
//!
//! ## 字节保真(架构文档,跨语言互通硬约束)
//! - hex 大小写按函数区分:`sha256` 与 **AES-HEX 密文**(`encrypt_aes_hex`/`EncOutput::Hex`)**大写**
//!   (对照 原实现 `StringUtils.toHex`);`md5`/`sha1`/`sha384`/`sha512`/`hmac*`/`pbkdf2`/`generate_salt`/
//!   `generate_aes_key_hex` 均**小写**(`toHex` 后再 `toLowerCase`)。
//!   ⚠ 安全说明 已纠正:此前本行误把"AES-HEX 密文"归入小写——实现一直是大写(`hex_upper`,有 golden 佐证)。
//! - **AES key = key 字符串的 UTF-8 字节(不解码)**,故 key 串长度必须 16/24/32。
//! - AES-CBC 默认变体 IV = key 字节;AES-GCM 布局 = `Base64(IV[12] ‖ ct ‖ tag[16])`。
//! - RSA = PKCS1 v1.5;自动分段(encrypt 块=keyLen-11、decrypt 块=keyLen);私钥加密 = PKCS1 **type-1**。
//! - RSA 公钥 = Base64(X509 SPKI)、私钥 = Base64(PKCS8);Ed25519 同。
//!
//! ## 安全改良
//! 所有随机值(salt / AES key / GCM IV / RSA_AES 临时 key)一律用 **OS 安全 RNG**(`OsRng`),**不复刻** 原实现
//! `StringUtils.random` 的 `Math.random()`(非密码学 RNG)——这些值每次随机、永不上线固定,不需要与 原实现 逐字节一致。
//! 新业务默认加密请使用 [`encrypt_modern`] / [`decrypt_modern`]:该 token 用随机 salt + PBKDF2-HMAC-SHA256
//! 派生 AES-256-GCM key,再做认证加密,不要求调用方自行准备 16/24/32 字节 AES 原始 key。
//!
//! ## ⚠ 安全诚实标注
//! - **AES-ECB**(Web 默认策略用)、**AES-CBC 用 key 当 IV**、**RSA 私钥加密当机密性**(实为签名语义,公钥人人可解)、
//!   **MD5/SHA-1/HMAC-MD5**——均为遗留/弱实践,**新(非互通)用途请改用** GCM/OAEP/Ed25519/SHA-256/PBKDF2/BCrypt。
//! - `rsa` crate 的 PKCS1 v1.5 **私钥解密**背负 RUSTSEC-2023-0071(Marvin Attack,时序
//!   侧信道),至今无修复版本。默认构建只保留 RS256 公钥验签等不执行该私钥解密的能力；
//!   `decrypt_rsa_private` 与历史私钥 type-1 运算必须显式启用 `legacy-rsa-private`，Web 层还需
//!   同时通过运行时风险门。该 feature 只用于有截止日期的互通迁移，不应作为新协议安全边界。

mod encryptor;

pub use encryptor::*;

/// 统一错误类型(对照 原实现 `EncryptException`/`DecryptException` + 配置校验)。
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
    /// 加密 encrypt 数据；用于生成受保护的输出。
    pub(crate) fn encrypt(m: impl Into<String>) -> Self {
        CryptoError::Encrypt(m.into())
    }

    /// 解密 decrypt 数据；用于还原受保护的输入。
    pub(crate) fn decrypt(m: impl Into<String>) -> Self {
        CryptoError::Decrypt(m.into())
    }

    /// 创建加密配置构造器；用于选择算法模式并生成运行参数。
    pub(crate) fn config(m: impl Into<String>) -> Self {
        CryptoError::Config(m.into())
    }
}

impl std::fmt::Display for CryptoError {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
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

/// 本 crate 统一 `Result`。所有可失败 API 返此,**不 panic**(对齐 Rust 纪律)。
pub type Result<T> = std::result::Result<T, CryptoError>;

/// AES 加密模式(对照 原实现 transformation 的 ECB/CBC 选择)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AesMode {
    /// `AES/ECB/PKCS5Padding`(默认;相同明文→相同密文,弱)。
    EcbPkcs5,
    /// `AES/CBC/PKCS5Padding`,IV = key 字节(默认变体;IV 不应等于 key,弱)。
    CbcPkcs5,
}

/// AES 输出 / 输入编码(对照 原实现 `EncryptMode`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncOutput {
    /// 标准 Base64(带填充)。
    Base64,
    /// 小写? **否——对照 原实现 `toHex` 为大写 hex**。
    Hex,
}
