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
//! [`SecretBytes`]/[`SecretSnapshot`] 的 `Debug` 只输出长度、ID 和 generation，不展开材料。
//! [`SecretError`] 可能展示引用元数据，公开构造时仍须保证这些字段不含敏感值。拼接与解码缓冲使用清零容器。
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
    /// 业务作用：接管已解析的拥有字节。
    ///
    /// 参数说明：
    /// - `bytes`：将由容器持有并在释放时尽力清零的敏感字节。
    ///
    /// 返回：返回接管该 Vec 的清零容器；不校验非空或长度，不复制字节。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// 业务作用：直接接管具备清零语义的缓冲，避免再复制一份敏感明文。
    ///
    /// 参数说明：
    /// - `bytes`：仍含敏感字节、已具备释放清零语义的缓冲。
    ///
    /// 返回：返回直接接管已有清零缓冲的容器；不复制、不校验内容。
    pub fn from_zeroizing(bytes: Zeroizing<Vec<u8>>) -> Self {
        Self {
            bytes,
        }
    }

    /// 业务作用：在一次受控调用期间借用字节;生命周期不超过容器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回生命周期受容器约束的敏感字节借用；调用方仍须防止复制或输出泄露。
    pub fn expose(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// 业务作用：字节长度(非敏感)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回当前字节数，不暴露内容。
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// 业务作用：是否为空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：长度为零时返回 true，否则返回 false。
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// 业务作用：把字节移动给下一阶段;移动后容器不再保留副本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回原字节 Vec 而不复制；返回值脱离 Zeroizing 包装，后续清零责任交给调用方。
    pub fn into_vec(self) -> Vec<u8> {
        let mut bytes = self.bytes;
        std::mem::take(&mut *bytes)
    }
}

impl fmt::Debug for SecretBytes {
    /// 业务作用：只格式化敏感容器的长度，避免默认展示字节内容。
    ///
    /// 参数说明：
    /// - `formatter`：接收展示内容的标准格式化器。
    ///
    /// 返回：仅写出类型与长度。 写出失败时传播格式化错误；调用方构造的公开元数据仍须满足非敏感约束。
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
        /// 提供方内部用于查询材料的稳定标识。
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
    /// 业务作用：格式化 secret 错误的标识、分类和定位元数据。
    ///
    /// 参数说明：
    /// - `formatter`：接收展示内容的标准格式化器。
    ///
    /// 返回：InvalidId 不输出原 ID；其它原因会输出其中的 ID 或引用元数据。 写出失败时传播格式化错误；调用方构造的公开元数据仍须满足非敏感约束。
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
            SecretErrorReason::InputTooLarge {
                actual,
                max,
            } => {
                write!(
                    formatter,
                    "concatenated input is at least {actual} bytes, exceeds max {max}"
                )
            }
            SecretErrorReason::DuplicateId => {
                write!(formatter, "secret id is declared more than once")
            }
            SecretErrorReason::TooManyFragments {
                actual,
                max,
            } => {
                write!(
                    formatter,
                    "secret declares {actual} fragments, exceeding limit {max}"
                )
            }
            SecretErrorReason::MissingProvider(provider) => {
                write!(formatter, "secret provider `{provider}` is not registered")
            }
            SecretErrorReason::ProviderFailed {
                provider,
                key,
            } => {
                write!(
                    formatter,
                    "secret provider `{provider}` failed for key `{key}`"
                )
            }
            SecretErrorReason::DecodeFailed => {
                write!(formatter, "concatenated fragments failed to decode")
            }
            SecretErrorReason::Empty => write!(formatter, "resolved material is empty"),
            SecretErrorReason::TooLarge {
                actual,
                max,
            } => {
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
    /// 业务作用：以当前 spec 的安全 ID 构造统一解析错误。
    ///
    /// 参数说明：
    /// - `reason`：不含材料值的失败分类及有界元数据。
    ///
    /// 返回：返回带本规格 ID 的分类错误；InvalidId 的原值替换为固定占位符。
    fn error(&self, reason: SecretErrorReason) -> SecretError {
        secret_error(&self.id, reason)
    }

    /// 业务作用：从最终 material 上限反推可接受的拼接输入上限，避免先聚合/解码任意大数据再做结果校验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：规格 ID、分片数、引用元数据和最终长度上限有效时，返回按 Raw、Base64 或 Hex 推导的编码输入预算；非法时返回分类错误。此处不拒绝零分片，由解析入口检查。
    fn input_limit(&self) -> Result<usize, SecretError> {
        if !valid_secret_id(&self.id) {
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
            SecretFragmentRef::Provider {
                provider,
                key,
            } => valid_id(provider) && valid_metadata(key, 512),
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

    /// 业务作用：在复制前检查剩余输入预算，再把 fragment 追加到可清零拼接缓冲。
    ///
    /// 参数说明：
    /// - `concat`：已聚合的敏感输入清零缓冲，应未超过预算。
    /// - `fragment`：本次追加的原始分片字节。
    /// - `input_limit`：整个拼接输入允许的最大字节数。
    ///
    /// 返回：剩余预算足够时原样追加并成功；超限时返回 InputTooLarge 且不追加，不去空白或补换行。
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

    /// 业务作用：最多读取剩余预算加一个探测字节，避免先把无界文件载入内存再报超限。
    ///
    /// 参数说明：
    /// - `path`：待读取的分片文件路径。
    /// - `remaining`：拼接输入尚可接纳的字节数。
    /// - `input_limit`：用于超限错误的整份编码输入预算。
    ///
    /// 返回：成功返回有界清零缓冲；打开或读取失败返回 FileReadFailed，探测到超额字节返回 InputTooLarge，不在错误中带文件正文。
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

    /// 业务作用：按声明顺序合并分片、一次性解码、校验长度,返回 zeroize 容器。
    ///
    /// 参数说明：
    /// - `config_scalar`：查询最终候选树标量的闭包；缺失或非标量返回 None，返回的 Arc 内容由调用方管理。
    ///
    /// 返回：成功时返回拼接后一次解码、非空且不超最终上限的清零容器；规格、分片读取或解码失败返回分类错误，不返回部分材料。 同步入口遇到 Provider 引用返回 MissingProvider。
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
                SecretFragmentRef::Provider {
                    provider,
                    ..
                } => {
                    return Err(
                        self.error(SecretErrorReason::MissingProvider(Arc::clone(provider)))
                    );
                }
            }
        }

        self.finish(concat)
    }

    /// 业务作用：同 [`Self::resolve`]，并允许通过注册表异步读取外部 provider 分片。
    ///
    /// 参数说明：
    /// - `providers`：供 Provider 分片查询的只读提供方注册表。
    /// - `config_scalar`：查询最终候选树标量的闭包；缺失或非标量返回 None，返回的 Arc 内容由调用方管理。
    ///
    /// 返回：成功时返回拼接后一次解码、非空且不超最终上限的清零容器；规格、分片读取或解码失败返回分类错误，不返回部分材料。 文件读取交给阻塞任务，取消等待不保证已启动的文件读取立即停止；提供方错误正文被丢弃。
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
                SecretFragmentRef::Provider {
                    provider,
                    key,
                } => {
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

    /// 业务作用：对完整拼接输入执行一次编码解码，并校验最终 material 的非空与大小边界。
    ///
    /// 参数说明：
    /// - `concat`：已按分片顺序聚合且应受输入预算约束的清零缓冲。
    ///
    /// 返回：成功时返回非空且不超过 max_bytes 的材料；编码非法、空结果或超限时返回分类错误。Raw 直接接管输入，其余编码解码一次。
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
    /// 业务作用：按稳定 key 读取 material；错误正文不得包含响应 body、token 或 material。
    ///
    /// 参数说明：
    /// - `key`：提供方内部用于定位材料的稳定非敏感标识。
    ///
    /// 返回：成功返回敏感字节容器，失败返回无正文的 SecretProviderError；实现不得把材料、凭据或响应正文放入错误或日志。
    async fn read(&self, key: &str) -> Result<SecretBytes, SecretProviderError>;
}

/// 启动期构造、随后只读的 provider 注册表。
#[derive(Default)]
pub struct SecretProviderRegistry {
    providers: BTreeMap<Arc<str>, Arc<dyn SecretProvider>>,
}

impl SecretProviderRegistry {
    /// 业务作用：创建空注册表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回无提供方的空注册表，不读取外部材料。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：注册唯一 provider ID。
    ///
    /// 参数说明：
    /// - `id`：最长 64 字节的非空 ASCII 提供方标识。
    /// - `provider`：按 key 异步读取材料的共享实现。
    ///
    /// 返回：ID 合法且未登记时插入提供方；非法或重复返回错误并保留已有注册，不调用提供方。
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

    /// 业务作用：按稳定 provider ID 借用已冻结 adapter。
    ///
    /// 参数说明：
    /// - `id`：提供方的稳定注册标识。
    ///
    /// 返回：已登记时返回提供方 Arc 的借用，否则返回 None；不发起读取。
    fn get(&self, id: &str) -> Option<&Arc<dyn SecretProvider>> {
        self.providers.get(id)
    }
}

/// 业务作用: 将 provider 与 participant ID 限制为短 ASCII 标识。
///
/// 参数说明:
/// - `value`: 需要校验的 provider 或 participant ID。
///
/// 返回: ID 非空、长度不超过 64 字节且仅含封闭 ASCII 字符集时返回 `true`。
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 业务作用: 将 secret ID 限制为有界 ASCII 层级标识，避免空分段和路径穿越。
///
/// 参数说明:
/// - `value`: 需要校验的层级 secret ID。
///
/// 返回: ID 非空、长度不超过 128 字节，且每个分段均非空、不是路径特殊段并仅含封闭 ASCII 字符集时返回 `true`。
fn valid_secret_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

/// 业务作用：校验引用元数据的非空、长度与控制字符边界。
///
/// 参数说明：
/// - `value`：引用路径、变量名或提供方 key 等元数据。
/// - `maximum`：允许的 UTF-8 字节数上限。
///
/// 返回：值非空、不超 maximum 字节且不含控制字符时返回 true；不排除空格、路径分隔符或其它可打印字符。
fn valid_metadata(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

impl fmt::Debug for SecretProviderRegistry {
    /// 业务作用：展示提供方注册表的 ID，不展开适配器和凭据。
    ///
    /// 参数说明：
    /// - `formatter`：接收展示内容的标准格式化器。
    ///
    /// 返回：仅写出注册 ID 列表。 写出失败时传播格式化错误；调用方构造的公开元数据仍须满足非敏感约束。
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

/// 业务作用：对 material 求私有 fingerprint,仅供**同代变更检测**(不对外暴露)。
///
/// 参数说明：
/// - `material`：需要计算变更摘要的敏感容器。
///
/// 返回：返回材料的 SHA-256 摘要；用于相同 ID 间的内容变更检测，不对外输出，不验证材料用途。
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
    /// 业务作用：开始构造 `generation` 代快照。
    ///
    /// 参数说明：
    /// - `generation`：由调用方分配的候选快照代次。
    ///
    /// 返回：返回指定代次的空构造器；不检查代次递增，该检查由轮换入口负责。
    pub fn builder(generation: u64) -> SecretSnapshotBuilder {
        SecretSnapshotBuilder {
            generation,
            entries: BTreeMap::new(),
        }
    }

    /// 业务作用：逐个解析 `specs` 构造快照;任一失败返回不含值的错误并整体放弃(保留 last-good 由调用方负责)。
    ///
    /// 参数说明：
    /// - `generation`：本次快照使用的代次。
    /// - `specs`：待按顺序解析的规格集合。
    /// - `config_scalar`：查询最终候选树标量的闭包；缺失或非标量返回 None，返回的 Arc 内容由调用方管理。
    ///
    /// 返回：全部规格解析成功后返回完整快照；非法或重复 ID、分片解析失败时整体返回错误，已构造材料随局部构造器释放，不发布新状态。允许空规格集合，不检查代次是否递增。
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
            if !valid_secret_id(&spec.id) {
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

    /// 业务作用：异步解析含 provider fragment 的同代快照；任一失败时不返回部分结果。
    ///
    /// 参数说明：
    /// - `generation`：本次快照使用的代次。
    /// - `specs`：待按顺序解析的规格集合。
    /// - `providers`：解析外部材料引用的提供方注册表。
    /// - `config_scalar`：查询最终候选树标量的闭包；缺失或非标量返回 None，返回的 Arc 内容由调用方管理。
    ///
    /// 返回：全部规格解析成功后返回完整快照；非法或重复 ID、分片解析失败时整体返回错误，已构造材料随局部构造器释放，不发布新状态。允许空规格集合，不检查代次是否递增。
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
            if !valid_secret_id(&spec.id) {
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

    /// 业务作用：本快照代号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回构造时指定的快照代次，不据此证明与其它快照存在先后关系。
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：借用某 secret 的字节;未知 ID 返回 `None`。
    ///
    /// 参数说明：
    /// - `id`：快照内的 secret 标识。
    ///
    /// 返回：存在时借用材料容器，未知 ID 返回 None；不复制材料。
    pub fn get(&self, id: &str) -> Option<&SecretBytes> {
        self.entries.get(id).map(|entry| &entry.material)
    }

    /// 业务作用：是否含某 secret ID。
    ///
    /// 参数说明：
    /// - `id`：需要判断是否存在的 secret 标识。
    ///
    /// 返回：快照含该 ID 时返回 true，否则返回 false；不读取材料内容。
    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    /// 业务作用：按有序遍历全部 secret ID(非敏感)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回按 BTreeMap 键序遍历的 ID 借用迭代器，不包含材料值。
    pub fn ids(&self) -> impl Iterator<Item = &Arc<str>> {
        self.entries.keys()
    }

    /// 业务作用：相对 `previous`,material 发生变化(新增、被移除或 fingerprint 不同)的 secret ID 集合。 consumer 据此判断自己的 material 是否需要重取,而**不必**也**不能**解析对外的 `<redacted>` 树。
    ///
    /// 参数说明：
    /// - `previous`：作为材料变更比较基线的旧快照。
    ///
    /// 返回：返回新增、删除或 SHA-256 摘要不同的 ID 有序集合；不比较代次，也不把摘要相等作为无碰撞的数学证明。
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
    /// 业务作用：无失败发布已经准备好的 consumer 状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费准备结果并发布新状态；实现必须只做无失败的内存发布，不执行解析、网络或磁盘 I/O，不返回可恢复错误。
    fn commit(self: Box<Self>);
    /// 业务作用：放弃尚未发布的 consumer 状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费并释放未发布候选；不得改变当前生效资源，不返回结果，协调器在准备失败或取消时调用。
    fn abort(self: Box<Self>);
}

/// 已成功 prepare、但尚未统一 commit 的 participant 变更集合。
struct PreparedRotationBatch {
    changes: Vec<Box<dyn PreparedSecretRotation>>,
}

impl PreparedRotationBatch {
    /// 业务作用：创建空准备批次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回尚无候选变更的批次，不发布状态。
    fn new() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    /// 业务作用：按 participant 顺序登记一项已完成的候选变更。
    ///
    /// 参数说明：
    /// - `change`：已完成可失败准备但尚未发布的参与方变更。
    ///
    /// 返回：将候选追加到准备序列；尚未提交时释放批次会逆序调用 abort。
    fn push(&mut self, change: Box<dyn PreparedSecretRotation>) {
        self.changes.push(change);
    }

    /// 业务作用：取走全部候选并执行无失败内存发布；Drop 不再 abort 已提交项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按登记顺序消费并提交全部候选；先取走列表使批次 Drop 不再 abort。依赖各实现遵守无失败约定，不提供多指针同时切换或展开后的回滚保证。
    fn commit(mut self) {
        let changes = std::mem::take(&mut self.changes);
        for change in changes {
            change.commit();
        }
    }
}

impl Drop for PreparedRotationBatch {
    /// 业务作用：任一 prepare 失败或协调 future 被取消时，逆序放弃所有尚未提交的候选资源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：逆序消费剩余候选并调用 abort；未进入 commit 的准备失败和取消路径均覆盖。依赖 abort 不展开。
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
    /// 业务作用：用不含 secret、URL 或响应正文的静态错误码构造。
    ///
    /// 参数说明：
    /// - `code`：可公开记录且不含材料、URL 或响应正文的静态分类码。
    ///
    /// 返回：返回携带原静态码的错误值；不检查码的内容，调用方必须使用不含敏感值的稳定分类。
    pub const fn new(code: &'static str) -> Self {
        Self {
            code,
        }
    }

    /// 业务作用：返回可进入日志和指标的静态错误码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按值返回原静态分类码，不附加底层错误正文。
    pub const fn code(self) -> &'static str {
        self.code
    }
}

/// 需要随 secret generation 一起轮换的 consumer。
#[async_trait::async_trait]
pub trait SecretRotationParticipant: Send + Sync {
    /// 业务作用：稳定 participant ID，只能用于日志和错误归因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用稳定非敏感参与方 ID；轮换协调器要求短 ASCII 标识且在本批唯一。
    fn id(&self) -> &str;

    /// 业务作用：当前 material 变更是否影响本 participant。
    ///
    /// 参数说明：
    /// - `changed_ids`：材料新增、删除或摘要变化的 ID 集合。
    ///
    /// 返回：受本次变更影响时返回 true，协调器才调用 prepare；false 会跳过该参与方。
    fn affected(&self, changed_ids: &BTreeSet<Arc<str>>) -> bool;

    /// 业务作用：对完整候选快照完成全部可失败准备工作，但不得发布给新请求。
    ///
    /// 参数说明：
    /// - `candidate`：完整候选 secret 快照，供解析和构造资源。
    /// - `changed_ids`：本次材料发生变化的 ID 集合。
    ///
    /// 返回：成功返回尚未发布的候选变更；失败返回静态分类错误。实现负责取消时释放未完成资源，只有协调器统一 commit 才可对新请求生效。
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
    /// 业务作用：构造完整候选资源；错误只能返回不含凭据的静态码。
    ///
    /// 参数说明：
    /// - `candidate`：构造资源必须固定使用的完整候选 secret 快照。
    ///
    /// 返回：成功返回已完成解析、建连和 TLS 检查的候选资源；失败返回不含凭据的静态错误码，不向业务发布候选。
    async fn build(&self, candidate: Arc<SecretSnapshot>) -> Result<R, SecretPrepareError>;
}

/// 一代强类型 consumer 资源快照。
pub struct SecretResourceSnapshot<R> {
    generation: u64,
    resource: Arc<R>,
}

impl<R> SecretResourceSnapshot<R> {
    /// 业务作用：该资源由哪一代 secret 构造。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回构造本资源时固定的 secret 代次。
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：借用本代资源；一次业务操作应固定同一个 snapshot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用本代资源 Arc；保持此快照即可继续使用该代，轮换不会修改该对象。
    pub fn resource(&self) -> &Arc<R> {
        &self.resource
    }
}

/// 面向不同提供方的两阶段强类型 secret consumer。
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
    /// 业务作用：克隆同一参与方的共享资源句柄，保持后续轮换读取同一发布槽。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回共享工厂、观察 ID 和同一发布槽的新句柄；不构造新资源。
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
    /// 业务作用：从已验证初始快照构造第一代资源。
    ///
    /// 参数说明：
    /// - `participant_id`：用于轮换登记去重的短 ASCII 标识。
    /// - `watched_ids`：非空的依赖 ID 集合，必须全部存在于初始快照。
    /// - `initial`：已解析的完整初始快照。
    /// - `factory`：完成全部可失败资源准备的共享工厂。
    ///
    /// 返回：标识和依赖有效且工厂构造成功时返回第一代资源；非法参与方、空依赖、非法或缺失 secret、工厂失败时返回错误，不发布部分资源。
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
                .any(|id| !valid_secret_id(id) || !initial.contains(id))
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

    /// 业务作用：固定当前 generation/resource 快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回固定代次与资源的 Arc 快照；后续全局切换不改变此快照。
    pub fn current(&self) -> Arc<SecretResourceSnapshot<R>> {
        self.current.load_full()
    }

    /// 业务作用：返回本 participant 观察的 secret ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用构造时登记的依赖集合，不重新计算工厂实际使用的材料。
    pub fn watched_ids(&self) -> &BTreeSet<Arc<str>> {
        &self.watched_ids
    }
}

#[async_trait::async_trait]
impl<R> SecretRotationParticipant for RotatingSecretResource<R>
where
    R: Send + Sync + 'static,
{
    /// 业务作用：返回只用于协调器去重和错误归因的脱敏标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用构造时验证的参与方 ID。
    fn id(&self) -> &str {
        &self.participant_id
    }

    /// 业务作用：仅当候选变化与声明依赖相交时参与本轮 prepare，避免无关资源重连。
    ///
    /// 参数说明：
    /// - `changed_ids`：本次新增、删除或内容摘要变化的 ID 集合。
    ///
    /// 返回：变更集合与登记依赖有交集时返回 true，否则跳过资源重建。
    fn affected(&self, changed_ids: &BTreeSet<Arc<str>>) -> bool {
        changed_ids.iter().any(|id| self.watched_ids.contains(id))
    }

    /// 业务作用：完整构造候选资源但暂不发布；watched secret 被删除时在调用 adapter 前拒绝。
    ///
    /// 参数说明：
    /// - `candidate`：用于新资源构造的完整候选快照。
    /// - `_changed_ids`：协调器已筛选的变更集合，本实现仅用完整依赖检查决定可构造性。
    ///
    /// 返回：依赖全部存在且工厂构造成功时返回未发布的新资源快照；缺少依赖或工厂失败时返回静态错误，当前发布槽不变。
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
    /// 业务作用：通过 ArcSwap 发布已准备资源，完成本参与方的内存状态切换。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：将候选 Arc 写入共享发布槽并消费准备结果，不执行显式取数或解析；旧快照仍可被在途请求持有。
    fn commit(self: Box<Self>) {
        self.current.store(self.next);
    }

    /// 业务作用：放弃候选时依靠所有权析构资源，不触碰当前 last-good 指针。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅消费并析构候选及其资源，不写入当前发布槽。
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
    /// 业务作用：展示强类型资源装配错误的枚举分类。
    ///
    /// 参数说明：
    /// - `formatter`：接收展示内容的标准格式化器。
    ///
    /// 返回：写出分类及可能的静态准备错误码。 写出失败时传播格式化错误；调用方构造的公开元数据仍须满足非敏感约束。
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
    /// 业务作用：用已验证初始快照创建。
    ///
    /// 参数说明：
    /// - `initial`：调用方已经构造的完整初始快照。
    ///
    /// 返回：返回接管初始快照的存储和空闲轮换门禁，不重新校验或解析初始材料。
    pub fn new(initial: SecretSnapshot) -> Self {
        Self {
            current: arc_swap::ArcSwap::from_pointee(initial),
            rotation_gate: tokio::sync::Mutex::new(()),
        }
    }

    /// 业务作用：返回当前 last-good。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回当前已发布快照的 Arc；调用方固定该快照可避免一次操作混用多代材料。
    pub fn current(&self) -> Arc<SecretSnapshot> {
        self.current.load_full()
    }

    /// 业务作用：原子发布完整候选，并返回发生 material 变化的 ID。
    ///
    /// 参数说明：
    /// - `candidate`：已完整构造的候选 secret 快照。
    ///
    /// 返回：门禁空闲且代次严格递增时发布候选并返回变化 ID；存在并发轮换或代次未递增时拒绝。本入口不准备或切换任何 consumer。
    pub fn rotate(
        &self,
        candidate: SecretSnapshot,
    ) -> Result<BTreeSet<Arc<str>>, SecretRotationError> {
        // 同步入口不等待正在进行的轮换，避免绕过异步准备阶段覆盖候选。
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

    /// 业务作用：两阶段轮换完整候选及其 consumer。 participant 按调用方登记顺序 prepare/commit；prepare 失败时，已经准备的项按相反顺序 abort。 participant ID 必须非空且唯一。未受 `changed_ids` 影响的 participant 不会被调用。
    ///
    /// 参数说明：
    /// - `candidate`：本次待发布的完整候选快照，代次必须递增。
    /// - `participants`：按准备与提交顺序登记的参与方，ID 必须合法且唯一。
    ///
    /// 返回：串行等待轮换门禁后校验代次与参与方，依次准备受影响资源；准备失败或等待取消时逆序 abort 已登记候选。全部准备成功后依次提交 consumer，再发布 secret 快照并返回变化 ID；各指针非同时原子切换，依赖参与方 commit 不展开。
    pub async fn rotate_prepared(
        &self,
        candidate: SecretSnapshot,
        participants: &[Arc<dyn SecretRotationParticipant>],
    ) -> Result<BTreeSet<Arc<str>>, SecretRotationError> {
        // 整轮持有门禁，使代次校验、准备和发布基于同一个已生效前代。
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
            // 未观察到依赖变化时不重建资源，仍依赖参与方准确声明影响集合。
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

        // 全部可失败准备完成后才依次发布 consumer，随后发布 secret 快照；各槽并非同时原子切换。
        prepared.commit();
        self.current.store(candidate);
        Ok(changed)
    }
}

/// 业务作用：强制候选 generation 严格递增，阻止旧快照或同代快照覆盖当前值。
///
/// 参数说明：
/// - `previous`：当前生效快照。
/// - `candidate`：拟发布的候选快照。
///
/// 返回：候选代次严格大于当前值时成功，否则返回包含两个代次的拒绝错误，不发布状态。
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
    /// 业务作用：展示轮换失败的代次、参与方标识与错误分类。
    ///
    /// 参数说明：
    /// - `formatter`：接收展示内容的标准格式化器。
    ///
    /// 返回：写出错误枚举及其中的静态准备错误码。 写出失败时传播格式化错误；调用方构造的公开元数据仍须满足非敏感约束。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "secret rotation error: {self:?}")
    }
}

impl std::error::Error for SecretRotationError {}

/// PEM mTLS 身份引用；material 始终留在 secret 快照内。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsIdentityRef {
    /// PEM 证书链对应的 secret ID。
    pub certificate_chain: Arc<str>,
    /// PEM 私钥对应的 secret ID。
    pub private_key: Arc<str>,
}

/// PEM trust bundle 引用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustBundleRef {
    /// PEM CA 集合对应的 secret ID。
    pub certificates: Arc<str>,
}

/// 一次受控借用的 mTLS material。
pub struct TlsIdentityMaterial<'a> {
    /// PEM 编码的证书链材料。
    pub certificate_chain: &'a SecretBytes,
    /// PEM 编码的私钥材料。
    pub private_key: &'a SecretBytes,
}

/// 一次受控借用的 trust bundle。
pub struct TrustBundleMaterial<'a> {
    /// PEM 编码的 CA 证书材料。
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
    /// 业务作用：从同一快照借用 mTLS 材料，并用起始标记筛除明显不符合预期的内容。
    ///
    /// 参数说明：
    /// - `snapshot`：借用证书链与私钥的同一材料快照。
    ///
    /// 返回：两个 ID 存在且材料包含预期证书和私钥起始标记时返回同快照借用；缺失或未发现标记返回错误。不解析 PEM，不验证证书链、密钥匹配或有效期。
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
    /// 业务作用：从快照借用信任材料，并检查证书起始标记。
    ///
    /// 参数说明：
    /// - `snapshot`：借用 CA 材料的固定快照。
    ///
    /// 返回：ID 存在且材料包含证书起始标记时返回借用；缺失或标记不存在时返回错误，不验证 CA 权限、链关系或 PEM 编码完整性。
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
        Ok(TrustBundleMaterial {
            certificates,
        })
    }
}

/// 业务作用：以预期起始标记识别材料类型，避免把明显错误的引用交给后续 TLS 解析。
///
/// 参数说明：
/// - `material`：需要检查起始标记的材料。
/// - `marker`：调用方提供的非空预期标记字节。
/// - `id`：用于分类错误定位的安全 secret ID。
///
/// 返回：材料包含 marker 子串时成功，否则返回仅含传入 ID 的 InvalidPem。marker 必须非空，空值会使 windows 展开；本函数不执行 PEM 解码。
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
    /// 业务作用：展示 secret 快照的代次与有序 ID 列表。
    ///
    /// 参数说明：
    /// - `formatter`：接收展示内容的标准格式化器。
    ///
    /// 返回：仅写出代次与标识，不写出材料及其摘要。 写出失败时传播格式化错误；调用方构造的公开元数据仍须满足非敏感约束。
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
    /// 业务作用：向候选快照登记材料，保护 ID 唯一性与框架长度上限。
    ///
    /// 参数说明：
    /// - `id`：本快照内唯一的层级 secret ID。
    /// - `material`：由调用方准备的敏感字节容器。
    ///
    /// 返回：ID 合法、未重复且材料不超过框架硬上限时，计算摘要后插入并返回构造器借用；否则返回分类错误且不插入。允许空材料，不复验来源规格的解码及 max_bytes 约束。
    pub fn insert(
        &mut self,
        id: Arc<str>,
        material: SecretBytes,
    ) -> Result<&mut Self, SecretError> {
        if !valid_secret_id(&id) {
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

    /// 业务作用：完成构造。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回接管全部条目的快照，不发布全局状态，也不复验代次、非空或材料来源。
    pub fn build(self) -> SecretSnapshot {
        SecretSnapshot {
            generation: self.generation,
            entries: self.entries,
        }
    }
}

/// 业务作用：构造只含安全 ID 的错误；非法原值统一替换，避免 builder 等低层入口把控制字符带进日志。
///
/// 参数说明：
/// - `id`：用于定位错误的 secret ID。
/// - `reason`：已分类的失败原因，不应携带材料。
///
/// 返回：返回错误；仅当原因是 InvalidId 时替换原 ID，其它 ID 及原因元数据原样保留，调用方负责先验证其安全性。
fn secret_error(id: &Arc<str>, reason: SecretErrorReason) -> SecretError {
    let id = if reason == SecretErrorReason::InvalidId {
        Arc::from("<invalid>")
    } else {
        Arc::clone(id)
    };
    SecretError {
        id,
        reason,
    }
}
