//! NASA secret 容器与解析。
//!
//! 覆盖"本地 `application.yml` 存 pre、Nacos 存 suffix,启动后合成完整 RSA 私钥 / AES key"的用法:
//! 一个 [`SecretSpec`] 声明**有序分片**([`SecretFragmentRef`])+ 拼接后**一次性**解码方式
//! ([`SecretEncoding`])+ 长度上限。解析顺序固定:
//!
//! ```text
//! 按声明顺序取原始 fragment(不 trim、不补换行)-> 先拼接 -> 再一次 base64/hex decode
//!   -> 校验长度 -> 构造 Zeroizing 容器(SecretBytes)
//! ```
//!
//! `Base64AfterConcat` 让 RSA DER / AES key 可先整体 Base64、再在任意位置切成 pre/suffix:启动时
//! 拼接 Base64 文本后**只 decode 一次**,避免 PEM 换行和 YAML chomping 改变字节。
//!
//! 脱敏纪律:[`SecretBytes`]/[`SecretSnapshot`]/[`SecretError`] 的 `Debug` 永远只输出长度、ID、
//! generation 与稳定 reason,**绝不**输出 secret 值。中间拼接缓冲与 decode 缓冲都用 zeroize 容器。
//!
//! 本 crate **不依赖 `napp`**:`ConfigPath` fragment 由调用方以闭包提供标量查询,避免反向依赖。

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

/// 发布给普通 `Application::config()` 的树里,secret fragment 值被替换成的固定占位符。
pub const REDACTED: &str = "<redacted>";
/// 单个 secret 允许的最大分片数；防止异常配置用大量微小 fragment 放大解析成本。
pub const MAX_SECRET_FRAGMENTS: usize = 32;
/// 单个 secret 最终 material 的框架硬上限；业务 `max_bytes` 只能在该范围内进一步收紧。
pub const MAX_SECRET_BYTES: usize = 16 * 1024 * 1024;

/// 敏感字节容器:Drop 时 best-effort 清零,`Debug` 只输出长度。
pub struct SecretBytes {
    bytes: Zeroizing<Vec<u8>>,
}

impl SecretBytes {
    /// 接管已解析的拥有字节。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// 从已 zeroize 的缓冲直接构造,避免多一份同时驻留的敏感明文。
    pub fn from_zeroizing(bytes: Zeroizing<Vec<u8>>) -> Self {
        Self { bytes }
    }

    /// 在一次受控调用期间借用字节;生命周期不超过容器。
    pub fn expose(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// 字节长度(非敏感)。
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// 把字节移动给下一阶段;移动后容器不再保留副本。
    pub fn into_vec(self) -> Vec<u8> {
        let mut bytes = self.bytes;
        std::mem::take(&mut *bytes)
    }
}

impl fmt::Debug for SecretBytes {
    /// 只输出长度,防止调试链/panic 泄露内容。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretBytes")
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// secret 的一个有序分片来源。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretFragmentRef {
    /// 引用最终候选配置树中的一个标量(禁止递归 SecretSpec / 循环引用)。
    ConfigPath(Arc<str>),
    /// 环境变量名。
    Env(Arc<str>),
    /// 文件路径。
    File(PathBuf),
    /// 由已注册 provider 按稳定 key 读取；provider 的 bootstrap credential 不得反向引用自身。
    Provider {
        /// provider 逻辑名。
        provider: Arc<str>,
        /// provider 内部的稳定 key；不得包含 secret 值。
        key: Arc<str>,
    },
}

/// 拼接后一次性解码方式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretEncoding {
    /// 拼接结果即最终字节。
    Raw,
    /// 拼接后整体做一次 base64 解码(RSA DER / AES key 的推荐方式)。
    Base64AfterConcat,
    /// 拼接后整体做一次 hex 解码。
    HexAfterConcat,
}

/// 一个 secret 的解析规格。
#[derive(Clone, Debug)]
pub struct SecretSpec {
    /// 稳定 secret ID:用于脱敏报告、变更检测与错误归因(非敏感)。
    pub id: Arc<str>,
    /// 按声明顺序合并的分片。
    pub fragments: Vec<SecretFragmentRef>,
    /// 拼接后的一次性解码方式。
    pub encoding: SecretEncoding,
    /// 解码后允许的最大字节数；必须在 `1..=MAX_SECRET_BYTES`，超出即拒绝。
    pub max_bytes: usize,
}

/// 解析失败的稳定原因(**绝不含 secret 值**;仅含 ID、定位符与长度)。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretErrorReason {
    /// secret ID 超长、为空或含日志不安全字符。
    InvalidId,
    /// fragment 的配置路径、环境变量名、provider ID/key 不满足有界元数据约束。
    InvalidReference,
    /// 未声明任何分片。
    NoFragments,
    /// 候选配置树中缺少该标量路径(路径是配置键名,非 secret 值)。
    MissingConfigPath(Arc<str>),
    /// 缺少环境变量(变量名非 secret 值)。
    MissingEnv(Arc<str>),
    /// 文件读取失败(路径是声明位置,非 secret 值)。
    FileReadFailed,
    /// `max_bytes` 为零或超过框架硬上限，无法形成有界 secret。
    InvalidLimit,
    /// 拼接/编码输入已超过由 `max_bytes` 推导出的安全上限，在复制或解码前拒绝。
    InputTooLarge {
        /// 已观察到的最小输入字节数。
        actual: usize,
        /// 允许的拼接输入上限。
        max: usize,
    },
    /// 同一快照声明了重复 secret ID。
    DuplicateId,
    /// 单个 secret 声明的 fragment 数超过硬上限。
    TooManyFragments {
        /// 实际 fragment 数。
        actual: usize,
        /// 允许的最大 fragment 数。
        max: usize,
    },
    /// provider 未注册。
    MissingProvider(Arc<str>),
    /// provider 调用失败；不转发可能含响应正文的底层错误。
    ProviderFailed {
        /// provider 逻辑名。
        provider: Arc<str>,
        /// provider key。
        key: Arc<str>,
    },
    /// 拼接后一次性解码失败(不携带任何输入或输出字节)。
    DecodeFailed,
    /// 解码结果为空。
    Empty,
    /// 解码结果超过 `max_bytes`(仅长度)。
    TooLarge {
        /// 实际解码字节数。
        actual: usize,
        /// 允许上限。
        max: usize,
    },
}

/// secret 解析错误:只含稳定标识与原因,可安全进日志与 `Debug`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretError {
    /// 出错 secret 的稳定 ID。
    pub id: Arc<str>,
    /// 稳定失败原因。
    pub reason: SecretErrorReason,
}

impl fmt::Display for SecretError {
    /// 输出稳定 ID 与分类元数据，绝不包含任何 fragment 或解析后 material。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.reason == SecretErrorReason::InvalidId {
            return write!(formatter, "secret has an invalid id");
        }
        write!(formatter, "secret `{}`: ", self.id)?;
        match &self.reason {
            SecretErrorReason::InvalidId => unreachable!("handled before rendering the secret id"),
            SecretErrorReason::InvalidReference => {
                write!(formatter, "fragment reference metadata is invalid")
            }
            SecretErrorReason::NoFragments => write!(formatter, "no fragments declared"),
            SecretErrorReason::MissingConfigPath(path) => {
                write!(formatter, "config path `{path}` is missing or not a scalar")
            }
            SecretErrorReason::MissingEnv(name) => {
                write!(formatter, "environment variable `{name}` is not set")
            }
            SecretErrorReason::FileReadFailed => write!(formatter, "secret file could not be read"),
            SecretErrorReason::InvalidLimit => {
                write!(formatter, "max_bytes must be within 1..={MAX_SECRET_BYTES}")
            }
            SecretErrorReason::InputTooLarge { actual, max } => {
                write!(
                    formatter,
                    "concatenated input is at least {actual} bytes, exceeds max {max}"
                )
            }
            SecretErrorReason::DuplicateId => {
                write!(formatter, "secret id is declared more than once")
            }
            SecretErrorReason::TooManyFragments { actual, max } => {
                write!(
                    formatter,
                    "secret declares {actual} fragments, exceeding limit {max}"
                )
            }
            SecretErrorReason::MissingProvider(provider) => {
                write!(formatter, "secret provider `{provider}` is not registered")
            }
            SecretErrorReason::ProviderFailed { provider, key } => {
                write!(
                    formatter,
                    "secret provider `{provider}` failed for key `{key}`"
                )
            }
            SecretErrorReason::DecodeFailed => {
                write!(formatter, "concatenated fragments failed to decode")
            }
            SecretErrorReason::Empty => write!(formatter, "resolved material is empty"),
            SecretErrorReason::TooLarge { actual, max } => {
                write!(
                    formatter,
                    "resolved material is {actual} bytes, exceeds max {max}"
                )
            }
        }
    }
}

impl std::error::Error for SecretError {}

impl SecretSpec {
    /// 以当前 spec 的安全 ID 构造统一解析错误。
    fn error(&self, reason: SecretErrorReason) -> SecretError {
        secret_error(&self.id, reason)
    }

    /// 从最终 material 上限反推可接受的拼接输入上限，避免先聚合/解码任意大数据再做结果校验。
    fn input_limit(&self) -> Result<usize, SecretError> {
        if !valid_id(&self.id) {
            return Err(self.error(SecretErrorReason::InvalidId));
        }
        if self.fragments.len() > MAX_SECRET_FRAGMENTS {
            return Err(self.error(SecretErrorReason::TooManyFragments {
                actual: self.fragments.len(),
                max: MAX_SECRET_FRAGMENTS,
            }));
        }
        let valid_reference = self.fragments.iter().all(|fragment| match fragment {
            SecretFragmentRef::ConfigPath(path) => {
                valid_metadata(path, 512) && path.split('.').next() != Some("secrets")
            }
            SecretFragmentRef::Env(name) => {
                valid_metadata(name, 255)
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            }
            SecretFragmentRef::File(_) => true,
            SecretFragmentRef::Provider { provider, key } => {
                valid_id(provider) && valid_metadata(key, 512)
            }
        });
        if !valid_reference {
            return Err(self.error(SecretErrorReason::InvalidReference));
        }
        if self.max_bytes == 0 || self.max_bytes > MAX_SECRET_BYTES {
            return Err(self.error(SecretErrorReason::InvalidLimit));
        }
        Ok(match self.encoding {
            SecretEncoding::Raw => self.max_bytes,
            SecretEncoding::Base64AfterConcat => self
                .max_bytes
                .checked_add(2)
                .and_then(|value| value.checked_div(3))
                .and_then(|value| value.checked_mul(4))
                .unwrap_or(usize::MAX),
            SecretEncoding::HexAfterConcat => self.max_bytes.saturating_mul(2),
        })
    }

    /// 在复制前检查剩余输入预算，再把 fragment 追加到可清零拼接缓冲。
    fn append(
        &self,
        concat: &mut Zeroizing<Vec<u8>>,
        fragment: &[u8],
        input_limit: usize,
    ) -> Result<(), SecretError> {
        let remaining = input_limit.saturating_sub(concat.len());
        if fragment.len() > remaining {
            return Err(self.error(SecretErrorReason::InputTooLarge {
                actual: concat.len().saturating_add(fragment.len()),
                max: input_limit,
            }));
        }
        concat.extend_from_slice(fragment);
        Ok(())
    }

    /// 最多读取剩余预算加一个探测字节，避免先把无界文件载入内存再报超限。
    fn read_file(
        &self,
        path: &std::path::Path,
        remaining: usize,
        input_limit: usize,
    ) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        let file =
            std::fs::File::open(path).map_err(|_| self.error(SecretErrorReason::FileReadFailed))?;
        let read_limit = u64::try_from(remaining)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut value = Zeroizing::new(Vec::new());
        file.take(read_limit)
            .read_to_end(&mut value)
            .map_err(|_| self.error(SecretErrorReason::FileReadFailed))?;
        if value.len() > remaining {
            return Err(self.error(SecretErrorReason::InputTooLarge {
                actual: input_limit.saturating_add(1),
                max: input_limit,
            }));
        }
        Ok(value)
    }

    /// 按声明顺序合并分片、一次性解码、校验长度,返回 zeroize 容器。
    ///
    /// # 参数
    ///
    /// - `config_scalar`:把 `ConfigPath` 分片映射到最终候选树标量的查询闭包;找不到或非标量返回
    ///   `None`。`Env`/`File` 分片由本函数直接读取。
    ///
    /// # 错误
    ///
    /// 任一分片缺失、解码失败、结果为空或超过 `max_bytes` 时返回不含值的 [`SecretError`]。
    pub fn resolve<F>(&self, config_scalar: F) -> Result<SecretBytes, SecretError>
    where
        F: Fn(&str) -> Option<Arc<str>>,
    {
        let input_limit = self.input_limit()?;
        if self.fragments.is_empty() {
            return Err(self.error(SecretErrorReason::NoFragments));
        }

        // 1. 按声明顺序取原始 fragment(不 trim、不补换行),拼进 zeroize 缓冲。
        let mut concat: Zeroizing<Vec<u8>> = Zeroizing::new(Vec::new());
        for fragment in &self.fragments {
            match fragment {
                SecretFragmentRef::ConfigPath(path) => {
                    let value = config_scalar(path).ok_or_else(|| {
                        self.error(SecretErrorReason::MissingConfigPath(Arc::clone(path)))
                    })?;
                    self.append(&mut concat, value.as_bytes(), input_limit)?;
                }
                SecretFragmentRef::Env(name) => {
                    let value = Zeroizing::new(std::env::var(name.as_ref()).map_err(|_| {
                        self.error(SecretErrorReason::MissingEnv(Arc::clone(name)))
                    })?);
                    self.append(&mut concat, value.as_bytes(), input_limit)?;
                }
                SecretFragmentRef::File(path) => {
                    let value = self.read_file(
                        path,
                        input_limit.saturating_sub(concat.len()),
                        input_limit,
                    )?;
                    self.append(&mut concat, &value, input_limit)?;
                }
                SecretFragmentRef::Provider { provider, .. } => {
                    return Err(
                        self.error(SecretErrorReason::MissingProvider(Arc::clone(provider)))
                    );
                }
            }
        }

        self.finish(concat)
    }

    /// 同 [`Self::resolve`]，并允许通过注册表异步读取外部 provider 分片。
    pub async fn resolve_async<F>(
        &self,
        providers: &SecretProviderRegistry,
        config_scalar: F,
    ) -> Result<SecretBytes, SecretError>
    where
        F: Fn(&str) -> Option<Arc<str>>,
    {
        let input_limit = self.input_limit()?;
        if self.fragments.is_empty() {
            return Err(self.error(SecretErrorReason::NoFragments));
        }
        let mut concat: Zeroizing<Vec<u8>> = Zeroizing::new(Vec::new());
        for fragment in &self.fragments {
            match fragment {
                SecretFragmentRef::ConfigPath(path) => {
                    let value = config_scalar(path).ok_or_else(|| {
                        self.error(SecretErrorReason::MissingConfigPath(Arc::clone(path)))
                    })?;
                    self.append(&mut concat, value.as_bytes(), input_limit)?;
                }
                SecretFragmentRef::Env(name) => {
                    let value = Zeroizing::new(std::env::var(name.as_ref()).map_err(|_| {
                        self.error(SecretErrorReason::MissingEnv(Arc::clone(name)))
                    })?);
                    self.append(&mut concat, value.as_bytes(), input_limit)?;
                }
                SecretFragmentRef::File(path) => {
                    let path = path.clone();
                    let spec = self.clone();
                    let remaining = input_limit.saturating_sub(concat.len());
                    let value = tokio::task::spawn_blocking(move || {
                        spec.read_file(&path, remaining, input_limit)
                    })
                    .await
                    .map_err(|_| self.error(SecretErrorReason::FileReadFailed))??;
                    self.append(&mut concat, &value, input_limit)?;
                }
                SecretFragmentRef::Provider { provider, key } => {
                    let adapter = providers.get(provider).ok_or_else(|| {
                        self.error(SecretErrorReason::MissingProvider(Arc::clone(provider)))
                    })?;
                    let value = adapter.read(key).await.map_err(|_| {
                        self.error(SecretErrorReason::ProviderFailed {
                            provider: Arc::clone(provider),
                            key: Arc::clone(key),
                        })
                    })?;
                    self.append(&mut concat, value.expose(), input_limit)?;
                }
            }
        }
        self.finish(concat)
    }

    /// 对完整拼接输入执行一次编码解码，并校验最终 material 的非空与大小边界。
    fn finish(&self, concat: Zeroizing<Vec<u8>>) -> Result<SecretBytes, SecretError> {
        // 拼接后一次性解码,decode 缓冲同样 zeroize。
        let decoded: Zeroizing<Vec<u8>> = match self.encoding {
            SecretEncoding::Raw => concat,
            SecretEncoding::Base64AfterConcat => {
                let mut buffer = Zeroizing::new(Vec::new());
                base64::engine::general_purpose::STANDARD
                    .decode_vec(concat.as_slice(), &mut buffer)
                    .map_err(|_| self.error(SecretErrorReason::DecodeFailed))?;
                buffer
            }
            SecretEncoding::HexAfterConcat => Zeroizing::new(
                hex::decode(concat.as_slice())
                    .map_err(|_| self.error(SecretErrorReason::DecodeFailed))?,
            ),
        };

        // 3. 校验长度。
        if decoded.is_empty() {
            return Err(self.error(SecretErrorReason::Empty));
        }
        if decoded.len() > self.max_bytes {
            return Err(self.error(SecretErrorReason::TooLarge {
                actual: decoded.len(),
                max: self.max_bytes,
            }));
        }

        Ok(SecretBytes::from_zeroizing(decoded))
    }
}

/// 外部 secret provider 的脱敏失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretProviderError;

/// Vault/OpenBao/KMS 等外部来源的最小公共合同。
#[async_trait::async_trait]
pub trait SecretProvider: Send + Sync {
    /// 按稳定 key 读取 material；错误正文不得包含响应 body、token 或 material。
    async fn read(&self, key: &str) -> Result<SecretBytes, SecretProviderError>;
}

/// 启动期构造、随后只读的 provider 注册表。
#[derive(Default)]
pub struct SecretProviderRegistry {
    providers: BTreeMap<Arc<str>, Arc<dyn SecretProvider>>,
}

impl SecretProviderRegistry {
    /// 创建空注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册唯一 provider ID。
    pub fn register(
        &mut self,
        id: impl Into<Arc<str>>,
        provider: Arc<dyn SecretProvider>,
    ) -> Result<(), SecretProviderRegistrationError> {
        let id = id.into();
        if !valid_id(&id) {
            return Err(SecretProviderRegistrationError::InvalidId);
        }
        if self.providers.contains_key(&id) {
            return Err(SecretProviderRegistrationError::Duplicate(id));
        }
        self.providers.insert(id, provider);
        Ok(())
    }

    /// 按稳定 provider ID 借用已冻结 adapter。
    fn get(&self, id: &str) -> Option<&Arc<dyn SecretProvider>> {
        self.providers.get(id)
    }
}

/// 将 provider、secret 与 participant ID 限制为短 ASCII 标识。
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 校验引用元数据的非空、长度与控制字符边界。
fn valid_metadata(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

impl fmt::Debug for SecretProviderRegistry {
    /// 只输出已注册 provider ID，不展示 adapter 内部状态或凭据。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretProviderRegistry")
            .field("ids", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// provider 注册错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretProviderRegistrationError {
    /// ID 为空。
    InvalidId,
    /// ID 重复。
    Duplicate(Arc<str>),
}

/// 对 material 求私有 fingerprint,仅供**同代变更检测**(不对外暴露)。
fn fingerprint(material: &SecretBytes) -> [u8; 32] {
    Sha256::digest(material.expose()).into()
}

/// 快照中的 material 与私有变更指纹；两者始终同代发布。
struct SecretEntry {
    material: SecretBytes,
    fingerprint: [u8; 32],
}

/// 一份同 generation 的已解析 secret 集合;真实值只存在于此,不进普通 ConfigView。
pub struct SecretSnapshot {
    generation: u64,
    entries: BTreeMap<Arc<str>, SecretEntry>,
}

impl SecretSnapshot {
    /// 开始构造 `generation` 代快照。
    pub fn builder(generation: u64) -> SecretSnapshotBuilder {
        SecretSnapshotBuilder {
            generation,
            entries: BTreeMap::new(),
        }
    }

    /// 逐个解析 `specs` 构造快照;任一失败返回不含值的错误并整体放弃(保留 last-good 由调用方负责)。
    ///
    /// # 参数
    ///
    /// - `generation`:本次发布的代号。
    /// - `specs`:待解析的 secret 规格集合。
    /// - `config_scalar`:`ConfigPath` 分片的标量查询闭包。
    pub fn resolve<'a, I, F>(
        generation: u64,
        specs: I,
        config_scalar: F,
    ) -> Result<SecretSnapshot, SecretError>
    where
        I: IntoIterator<Item = &'a SecretSpec>,
        F: Fn(&str) -> Option<Arc<str>>,
    {
        let mut builder = SecretSnapshot::builder(generation);
        let mut ids = BTreeSet::new();
        for spec in specs {
            if !valid_id(&spec.id) {
                return Err(spec.error(SecretErrorReason::InvalidId));
            }
            if !ids.insert(Arc::clone(&spec.id)) {
                return Err(spec.error(SecretErrorReason::DuplicateId));
            }
            let material = spec.resolve(&config_scalar)?;
            builder.insert(Arc::clone(&spec.id), material)?;
        }
        Ok(builder.build())
    }

    /// 异步解析含 provider fragment 的同代快照；任一失败时不返回部分结果。
    pub async fn resolve_async<'a, I, F>(
        generation: u64,
        specs: I,
        providers: &SecretProviderRegistry,
        config_scalar: F,
    ) -> Result<SecretSnapshot, SecretError>
    where
        I: IntoIterator<Item = &'a SecretSpec>,
        F: Fn(&str) -> Option<Arc<str>>,
    {
        let mut builder = SecretSnapshot::builder(generation);
        let mut ids = BTreeSet::new();
        for spec in specs {
            if !valid_id(&spec.id) {
                return Err(spec.error(SecretErrorReason::InvalidId));
            }
            if !ids.insert(Arc::clone(&spec.id)) {
                return Err(spec.error(SecretErrorReason::DuplicateId));
            }
            let material = spec.resolve_async(providers, &config_scalar).await?;
            builder.insert(Arc::clone(&spec.id), material)?;
        }
        Ok(builder.build())
    }

    /// 本快照代号。
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 借用某 secret 的字节;未知 ID 返回 `None`。
    pub fn get(&self, id: &str) -> Option<&SecretBytes> {
        self.entries.get(id).map(|entry| &entry.material)
    }

    /// 是否含某 secret ID。
    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    /// 按有序遍历全部 secret ID(非敏感)。
    pub fn ids(&self) -> impl Iterator<Item = &Arc<str>> {
        self.entries.keys()
    }

    /// 相对 `previous`,material 发生变化(新增、被移除或 fingerprint 不同)的 secret ID 集合。
    ///
    /// consumer 据此判断自己的 material 是否需要重取,而**不必**也**不能**解析对外的 `<redacted>` 树。
    pub fn changed_ids(&self, previous: &SecretSnapshot) -> BTreeSet<Arc<str>> {
        let mut changed = BTreeSet::new();
        for (id, entry) in &self.entries {
            match previous.entries.get(id) {
                Some(prev) if prev.fingerprint == entry.fingerprint => {}
                _ => {
                    changed.insert(Arc::clone(id));
                }
            }
        }
        for id in previous.entries.keys() {
            if !self.entries.contains_key(id) {
                changed.insert(Arc::clone(id));
            }
        }
        changed
    }
}

/// 一个已经完成全部可失败准备工作的 secret consumer 变更。
///
/// `commit` 必须只做内存中的无失败指针发布，不能执行解析、网络或磁盘 I/O；`abort` 释放尚未发布的
/// 候选资源。协调器只有在所有 participant 都 prepare 成功后才调用 commit。
pub trait PreparedSecretRotation: Send {
    /// 无失败发布已经准备好的 consumer 状态。
    fn commit(self: Box<Self>);
    /// 放弃尚未发布的 consumer 状态。
    fn abort(self: Box<Self>);
}

/// 已成功 prepare、但尚未统一 commit 的 participant 变更集合。
struct PreparedRotationBatch {
    changes: Vec<Box<dyn PreparedSecretRotation>>,
}

impl PreparedRotationBatch {
    /// 创建空准备批次。
    fn new() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    /// 按 participant 顺序登记一项已完成的候选变更。
    fn push(&mut self, change: Box<dyn PreparedSecretRotation>) {
        self.changes.push(change);
    }

    /// 取走全部候选并执行无失败内存发布；Drop 不再 abort 已提交项。
    fn commit(mut self) {
        let changes = std::mem::take(&mut self.changes);
        for change in changes {
            change.commit();
        }
    }
}

impl Drop for PreparedRotationBatch {
    /// 任一 prepare 失败或协调 future 被取消时，逆序放弃所有尚未提交的候选资源。
    fn drop(&mut self) {
        while let Some(change) = self.changes.pop() {
            change.abort();
        }
    }
}

/// participant prepare 的脱敏错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretPrepareError {
    code: &'static str,
}

impl SecretPrepareError {
    /// 用不含 secret、URL 或响应正文的静态错误码构造。
    pub const fn new(code: &'static str) -> Self {
        Self { code }
    }

    /// 返回可进入日志和指标的静态错误码。
    pub const fn code(self) -> &'static str {
        self.code
    }
}

/// 需要随 secret generation 一起轮换的 consumer。
#[async_trait::async_trait]
pub trait SecretRotationParticipant: Send + Sync {
    /// 稳定 participant ID，只能用于日志和错误归因。
    fn id(&self) -> &str;

    /// 当前 material 变更是否影响本 participant。
    fn affected(&self, changed_ids: &BTreeSet<Arc<str>>) -> bool;

    /// 对完整候选快照完成全部可失败准备工作，但不得发布给新请求。
    async fn prepare(
        &self,
        candidate: Arc<SecretSnapshot>,
        changed_ids: &BTreeSet<Arc<str>>,
    ) -> Result<Box<dyn PreparedSecretRotation>, SecretPrepareError>;
}

/// 用候选 secret 快照完整构造一代强类型 consumer 资源。
///
/// DB pool、Redis client、Kafka producer 或 OTLP client 的 adapter 实现本 trait；所有解析、建连和
/// TLS 校验都必须在 `build` 内完成，返回成功后 commit 只剩内存指针发布。
#[async_trait::async_trait]
pub trait SecretResourceFactory<R>: Send + Sync
where
    R: Send + Sync + 'static,
{
    /// 构造完整候选资源；错误只能返回不含凭据的静态码。
    async fn build(&self, candidate: Arc<SecretSnapshot>) -> Result<R, SecretPrepareError>;
}

/// 一代强类型 consumer 资源快照。
pub struct SecretResourceSnapshot<R> {
    generation: u64,
    resource: Arc<R>,
}

impl<R> SecretResourceSnapshot<R> {
    /// 该资源由哪一代 secret 构造。
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// 借用本代资源；一次业务操作应固定同一个 snapshot。
    pub fn resource(&self) -> &Arc<R> {
        &self.resource
    }
}

/// provider-neutral 的两阶段强类型 secret consumer。
///
/// 它可承载 DB/Redis/Kafka/OTLP 等不同客户端而不让 `nasecret` 反向依赖这些实现 crate。调用方通过
/// [`SecretResourceFactory`] 注入候选资源构造逻辑；当前代使用 ArcSwap 发布，请求固定
/// [`SecretResourceSnapshot`] 后不会在一次操作中混用两代连接。
pub struct RotatingSecretResource<R>
where
    R: Send + Sync + 'static,
{
    participant_id: Arc<str>,
    watched_ids: Arc<BTreeSet<Arc<str>>>,
    factory: Arc<dyn SecretResourceFactory<R>>,
    current: Arc<arc_swap::ArcSwap<SecretResourceSnapshot<R>>>,
}

impl<R> Clone for RotatingSecretResource<R>
where
    R: Send + Sync + 'static,
{
    /// 克隆同一个 participant 的共享发布槽；不会复制资源或形成第二个轮换 owner。
    fn clone(&self) -> Self {
        Self {
            participant_id: Arc::clone(&self.participant_id),
            watched_ids: Arc::clone(&self.watched_ids),
            factory: Arc::clone(&self.factory),
            current: Arc::clone(&self.current),
        }
    }
}

impl<R> RotatingSecretResource<R>
where
    R: Send + Sync + 'static,
{
    /// 从已验证初始快照构造第一代资源。
    pub async fn new(
        participant_id: impl Into<Arc<str>>,
        watched_ids: BTreeSet<Arc<str>>,
        initial: Arc<SecretSnapshot>,
        factory: Arc<dyn SecretResourceFactory<R>>,
    ) -> Result<Self, SecretResourceError> {
        let participant_id = participant_id.into();
        if !valid_id(&participant_id) {
            return Err(SecretResourceError::InvalidParticipantId);
        }
        if watched_ids.is_empty()
            || watched_ids
                .iter()
                .any(|id| !valid_id(id) || !initial.contains(id))
        {
            return Err(SecretResourceError::InvalidSecretReference);
        }
        let generation = initial.generation();
        let resource = factory
            .build(initial)
            .await
            .map_err(SecretResourceError::InitialPrepare)?;
        Ok(Self {
            participant_id,
            watched_ids: Arc::new(watched_ids),
            factory,
            current: Arc::new(arc_swap::ArcSwap::from_pointee(SecretResourceSnapshot {
                generation,
                resource: Arc::new(resource),
            })),
        })
    }

    /// 固定当前 generation/resource 快照。
    pub fn current(&self) -> Arc<SecretResourceSnapshot<R>> {
        self.current.load_full()
    }

    /// 返回本 participant 观察的 secret ID。
    pub fn watched_ids(&self) -> &BTreeSet<Arc<str>> {
        &self.watched_ids
    }
}

#[async_trait::async_trait]
impl<R> SecretRotationParticipant for RotatingSecretResource<R>
where
    R: Send + Sync + 'static,
{
    /// 返回只用于协调器去重和错误归因的脱敏标识。
    fn id(&self) -> &str {
        &self.participant_id
    }

    /// 仅当候选变化与声明依赖相交时参与本轮 prepare，避免无关资源重连。
    fn affected(&self, changed_ids: &BTreeSet<Arc<str>>) -> bool {
        changed_ids.iter().any(|id| self.watched_ids.contains(id))
    }

    /// 完整构造候选资源但暂不发布；watched secret 被删除时在调用 adapter 前拒绝。
    async fn prepare(
        &self,
        candidate: Arc<SecretSnapshot>,
        _changed_ids: &BTreeSet<Arc<str>>,
    ) -> Result<Box<dyn PreparedSecretRotation>, SecretPrepareError> {
        // 被删除的 secret 同样属于一次变更。即使自定义 factory 没有主动读取该 ID，也不能让它
        // 构造出一代名义上成功、实际已失去声明依赖的资源。
        if self.watched_ids.iter().any(|id| !candidate.contains(id)) {
            return Err(SecretPrepareError::new("missing_watched_secret"));
        }
        let generation = candidate.generation();
        let resource = self.factory.build(candidate).await?;
        Ok(Box::new(PreparedSecretResource {
            current: Arc::clone(&self.current),
            next: Arc::new(SecretResourceSnapshot {
                generation,
                resource: Arc::new(resource),
            }),
        }))
    }
}

/// 已构造但尚未发布的一代资源；Drop/abort 只释放候选，commit 才交换共享指针。
struct PreparedSecretResource<R>
where
    R: Send + Sync + 'static,
{
    current: Arc<arc_swap::ArcSwap<SecretResourceSnapshot<R>>>,
    next: Arc<SecretResourceSnapshot<R>>,
}

impl<R> PreparedSecretRotation for PreparedSecretResource<R>
where
    R: Send + Sync + 'static,
{
    /// 执行无 I/O、无失败的 ArcSwap 发布，满足协调器 commit 阶段约束。
    fn commit(self: Box<Self>) {
        self.current.store(self.next);
    }

    /// 放弃候选时依靠所有权析构资源，不触碰当前 last-good 指针。
    fn abort(self: Box<Self>) {}
}

/// 强类型 secret consumer 初始装配错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretResourceError {
    /// participant ID 非法。
    InvalidParticipantId,
    /// watched ID 为空、非法或不在初始快照内。
    InvalidSecretReference,
    /// 第一代资源构造失败。
    InitialPrepare(SecretPrepareError),
}

impl fmt::Display for SecretResourceError {
    /// 输出稳定错误分类，不展示 participant 内部资源或 secret material。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "secret resource error: {self:?}")
    }
}

impl std::error::Error for SecretResourceError {}

/// 原子 last-good secret 快照；只接受严格递增 generation 的完整候选。
///
/// 带 participant 的轮换采用 prepare/commit/abort：全部 prepare 成功前不会发布任何 consumer；失败时
/// 已准备项按反序 abort。commit 被限定为无失败内存发布，因此不会留下“部分 prepare 成功”的状态。
/// 多个独立 consumer 的指针无法在一个 CPU 原子操作里同时切换；请求必须从各 consumer 自己的
/// generation 快照固定客户端，不能在一次请求中反复读取可变全局。
pub struct RotatingSecretStore {
    current: arc_swap::ArcSwap<SecretSnapshot>,
    rotation_gate: tokio::sync::Mutex<()>,
}

impl RotatingSecretStore {
    /// 用已验证初始快照创建。
    pub fn new(initial: SecretSnapshot) -> Self {
        Self {
            current: arc_swap::ArcSwap::from_pointee(initial),
            rotation_gate: tokio::sync::Mutex::new(()),
        }
    }

    /// 返回当前 last-good。
    pub fn current(&self) -> Arc<SecretSnapshot> {
        self.current.load_full()
    }

    /// 原子发布完整候选，并返回发生 material 变化的 ID。
    pub fn rotate(
        &self,
        candidate: SecretSnapshot,
    ) -> Result<BTreeSet<Arc<str>>, SecretRotationError> {
        let _gate = self
            .rotation_gate
            .try_lock()
            .map_err(|_| SecretRotationError::ConcurrentRotation)?;
        let previous = self.current.load_full();
        validate_generation(&previous, &candidate)?;
        let changed = candidate.changed_ids(&previous);
        self.current.store(Arc::new(candidate));
        Ok(changed)
    }

    /// 两阶段轮换完整候选及其 consumer。
    ///
    /// participant 按调用方登记顺序 prepare/commit；prepare 失败时，已经准备的项按相反顺序 abort。
    /// participant ID 必须非空且唯一。未受 `changed_ids` 影响的 participant 不会被调用。
    pub async fn rotate_prepared(
        &self,
        candidate: SecretSnapshot,
        participants: &[Arc<dyn SecretRotationParticipant>],
    ) -> Result<BTreeSet<Arc<str>>, SecretRotationError> {
        let _gate = self.rotation_gate.lock().await;
        let previous = self.current.load_full();
        validate_generation(&previous, &candidate)?;

        let mut participant_ids = BTreeSet::new();
        for participant in participants {
            let id = participant.id();
            if !valid_id(id) {
                return Err(SecretRotationError::InvalidParticipantId);
            }
            if !participant_ids.insert(id) {
                return Err(SecretRotationError::DuplicateParticipant(Arc::from(id)));
            }
        }

        let changed = candidate.changed_ids(&previous);
        let candidate = Arc::new(candidate);
        // RAII batch 同时覆盖显式 prepare 失败和 future 在 await 期间被取消：尚未 commit 的项都会
        // 反向 abort，不能只依赖 participant 自己的普通 Drop。
        let mut prepared = PreparedRotationBatch::new();
        for participant in participants {
            if !participant.affected(&changed) {
                continue;
            }
            match participant.prepare(Arc::clone(&candidate), &changed).await {
                Ok(change) => prepared.push(change),
                Err(error) => {
                    return Err(SecretRotationError::PrepareFailed {
                        participant: Arc::from(participant.id()),
                        error,
                    });
                }
            }
        }

        prepared.commit();
        self.current.store(candidate);
        Ok(changed)
    }
}

/// 强制候选 generation 严格递增，阻止旧快照或同代快照覆盖当前值。
fn validate_generation(
    previous: &SecretSnapshot,
    candidate: &SecretSnapshot,
) -> Result<(), SecretRotationError> {
    if candidate.generation() <= previous.generation() {
        return Err(SecretRotationError::NonIncreasingGeneration {
            current: previous.generation(),
            candidate: candidate.generation(),
        });
    }
    Ok(())
}

/// secret 快照发布错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretRotationError {
    /// 候选 generation 必须严格递增。
    NonIncreasingGeneration {
        /// 当前代。
        current: u64,
        /// 候选代。
        candidate: u64,
    },
    /// 同步兼容入口遇到另一个正在进行的轮换。
    ConcurrentRotation,
    /// participant ID 为空。
    InvalidParticipantId,
    /// participant ID 重复。
    DuplicateParticipant(Arc<str>),
    /// 某 participant 拒绝候选；错误码不含 material。
    PrepareFailed {
        /// 稳定 participant ID。
        participant: Arc<str>,
        /// 脱敏静态错误。
        error: SecretPrepareError,
    },
}

impl fmt::Display for SecretRotationError {
    /// 输出 generation、participant ID 与静态错误码，不包含 secret material。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "secret rotation error: {self:?}")
    }
}

impl std::error::Error for SecretRotationError {}

/// PEM mTLS 身份引用；material 始终留在 secret 快照内。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsIdentityRef {
    /// PEM certificate chain secret ID。
    pub certificate_chain: Arc<str>,
    /// PEM private key secret ID。
    pub private_key: Arc<str>,
}

/// PEM trust bundle 引用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustBundleRef {
    /// PEM CA bundle secret ID。
    pub certificates: Arc<str>,
}

/// 一次受控借用的 mTLS material。
pub struct TlsIdentityMaterial<'a> {
    /// PEM certificate chain。
    pub certificate_chain: &'a SecretBytes,
    /// PEM private key。
    pub private_key: &'a SecretBytes,
}

/// 一次受控借用的 trust bundle。
pub struct TrustBundleMaterial<'a> {
    /// PEM CA certificates。
    pub certificates: &'a SecretBytes,
}

/// TLS 引用解析错误；只报告 secret ID/结构类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsMaterialError {
    /// 快照缺少 secret ID。
    Missing(Arc<str>),
    /// material 不是所需 PEM 类型。
    InvalidPem(Arc<str>),
}

impl TlsIdentityRef {
    /// 从同一 snapshot 解析证书链与私钥，拒绝缺失或类型错误。
    pub fn resolve<'a>(
        &self,
        snapshot: &'a SecretSnapshot,
    ) -> Result<TlsIdentityMaterial<'a>, TlsMaterialError> {
        let certificate_chain = snapshot
            .get(&self.certificate_chain)
            .ok_or_else(|| TlsMaterialError::Missing(Arc::clone(&self.certificate_chain)))?;
        let private_key = snapshot
            .get(&self.private_key)
            .ok_or_else(|| TlsMaterialError::Missing(Arc::clone(&self.private_key)))?;
        validate_pem(
            certificate_chain,
            b"-----BEGIN CERTIFICATE-----",
            &self.certificate_chain,
        )?;
        if !private_key
            .expose()
            .windows(b"-----BEGIN PRIVATE KEY-----".len())
            .any(|window| window == b"-----BEGIN PRIVATE KEY-----")
            && !private_key
                .expose()
                .windows(b"-----BEGIN RSA PRIVATE KEY-----".len())
                .any(|window| window == b"-----BEGIN RSA PRIVATE KEY-----")
        {
            return Err(TlsMaterialError::InvalidPem(Arc::clone(&self.private_key)));
        }
        Ok(TlsIdentityMaterial {
            certificate_chain,
            private_key,
        })
    }
}

impl TrustBundleRef {
    /// 从同一 snapshot 解析 CA bundle。
    pub fn resolve<'a>(
        &self,
        snapshot: &'a SecretSnapshot,
    ) -> Result<TrustBundleMaterial<'a>, TlsMaterialError> {
        let certificates = snapshot
            .get(&self.certificates)
            .ok_or_else(|| TlsMaterialError::Missing(Arc::clone(&self.certificates)))?;
        validate_pem(
            certificates,
            b"-----BEGIN CERTIFICATE-----",
            &self.certificates,
        )?;
        Ok(TrustBundleMaterial { certificates })
    }
}

/// 通过预期 PEM block marker 验证引用类型，错误只携带安全 secret ID。
fn validate_pem(
    material: &SecretBytes,
    marker: &[u8],
    id: &Arc<str>,
) -> Result<(), TlsMaterialError> {
    if material
        .expose()
        .windows(marker.len())
        .any(|window| window == marker)
    {
        Ok(())
    } else {
        Err(TlsMaterialError::InvalidPem(Arc::clone(id)))
    }
}

impl fmt::Debug for SecretSnapshot {
    /// 只输出 generation 与 ID 列表,绝不输出 material。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretSnapshot")
            .field("generation", &self.generation)
            .field("ids", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// [`SecretSnapshot`] 的增量构造器。
pub struct SecretSnapshotBuilder {
    generation: u64,
    entries: BTreeMap<Arc<str>, SecretEntry>,
}

impl SecretSnapshotBuilder {
    /// 放入一个已解析的 secret；非法 ID 或同代重复 ID 均拒绝，不能绕过 [`SecretSnapshot::resolve`]
    /// 的快照不变量。
    ///
    /// # 错误
    ///
    /// ID 不安全或本代快照已含同名 ID 时返回脱敏的 [`SecretError`]。
    pub fn insert(
        &mut self,
        id: Arc<str>,
        material: SecretBytes,
    ) -> Result<&mut Self, SecretError> {
        if !valid_id(&id) {
            return Err(secret_error(&id, SecretErrorReason::InvalidId));
        }
        if self.entries.contains_key(&id) {
            return Err(secret_error(&id, SecretErrorReason::DuplicateId));
        }
        if material.len() > MAX_SECRET_BYTES {
            return Err(secret_error(
                &id,
                SecretErrorReason::TooLarge {
                    actual: material.len(),
                    max: MAX_SECRET_BYTES,
                },
            ));
        }
        let fingerprint = fingerprint(&material);
        self.entries.insert(
            id,
            SecretEntry {
                material,
                fingerprint,
            },
        );
        Ok(self)
    }

    /// 完成构造。
    pub fn build(self) -> SecretSnapshot {
        SecretSnapshot {
            generation: self.generation,
            entries: self.entries,
        }
    }
}

/// 构造只含安全 ID 的错误；非法原值统一替换，避免 builder 等低层入口把控制字符带进日志。
fn secret_error(id: &Arc<str>, reason: SecretErrorReason) -> SecretError {
    let id = if reason == SecretErrorReason::InvalidId {
        Arc::from("<invalid>")
    } else {
        Arc::clone(id)
    };
    SecretError { id, reason }
}
