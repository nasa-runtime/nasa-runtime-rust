//! 由 `nasecret` 两阶段轮换驱动的 reqwest TLS/mTLS 客户端。
//!
//! 每个请求先取得一个 [`TlsHttpClientSnapshot`]，再在整个请求期间固定该 `Arc`；轮换只替换下一批
//! 请求看到的客户端。证书、私钥、CA 的解析和 reqwest client 构造全部发生在 prepare 阶段。

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use nasecret::{
    PreparedSecretRotation, SecretPrepareError, SecretRotationParticipant, SecretSnapshot,
    TlsIdentityRef, TlsMaterialError, TrustBundleRef,
};
use zeroize::Zeroizing;

/// 单次 TLS HTTP 请求允许的最大总时长。
pub const MAX_TLS_HTTP_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 一个 TLS HTTP participant 的不可变配置。
#[derive(Debug, Clone)]
pub struct TlsHttpClientConfig {
    /// 两阶段协调器里的稳定 participant ID。
    pub participant_id: Arc<str>,
    /// 可选 mTLS client certificate + private key 引用。
    pub identity: Option<TlsIdentityRef>,
    /// 可选服务端 CA bundle 引用。
    pub trust: Option<TrustBundleRef>,
    /// 整个 HTTP 请求的超时。
    pub request_timeout: Duration,
}

impl TlsHttpClientConfig {
    /// 业务作用：创建使用默认 10 秒请求超时的配置。
    ///
    /// 参数说明：
    /// - `participant_id`：后续参与轮换去重与错误归因的逻辑标识。
    ///
    /// 返回：返回保存原参与方 ID、无 mTLS 身份和显式信任包、十秒请求超时的配置；不验证 ID 或读取 secret。
    pub fn new(participant_id: impl Into<Arc<str>>) -> Self {
        Self {
            participant_id: participant_id.into(),
            identity: None,
            trust: None,
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// HTTP/TLS client 候选构造错误；不包含 PEM、URL 或底层错误正文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsHttpClientError {
    /// participant ID 为空。
    InvalidParticipantId,
    /// 请求超时必须大于零。
    InvalidRequestTimeout,
    /// TLS identity/trust 引用不是稳定安全 ID。
    InvalidSecretReference,
    /// secret 快照缺失或 PEM marker 不符合引用类型。
    InvalidMaterial(TlsMaterialError),
    /// certificate chain/private key 无法组成 rustls client identity。
    InvalidIdentity,
    /// CA bundle 无法解析，或 bundle 中没有证书。
    InvalidTrustBundle,
    /// reqwest client builder 拒绝配置。
    ClientBuildFailed,
}

impl fmt::Display for TlsHttpClientError {
    /// 业务作用：输出稳定配置错误分类，不包含 PEM、URL 或底层 TLS 错误正文。
    ///
    /// 参数说明：
    /// - `formatter`：接收配置错误摘要的格式化器。
    ///
    /// 返回：写出错误枚举分类及可能的材料引用 ID，失败时返回格式化错误，不主动附加 PEM 内容或底层网络正文。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "TLS HTTP client configuration error: {self:?}")
    }
}

impl std::error::Error for TlsHttpClientError {}

/// 一次请求固定的 HTTP client 与 secret generation。
#[derive(Clone)]
pub struct TlsHttpClientSnapshot {
    generation: u64,
    client: reqwest::Client,
}

impl TlsHttpClientSnapshot {
    /// 业务作用：构造该客户端所用的 secret generation。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回构造本客户端时固定的 secret 快照代次，不自动跟随存储当前代次。
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：借用已经完成 TLS 配置的 reqwest client。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用本快照内的 reqwest 客户端；在一次业务操作中固定该快照可避免切换材料来源，本方法不发送请求。
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

impl fmt::Debug for TlsHttpClientSnapshot {
    /// 业务作用：只展示 secret generation，隐藏 reqwest client 内部 TLS 状态。
    ///
    /// 参数说明：
    /// - `formatter`：接收快照摘要的格式化器。
    ///
    /// 返回：仅写出客户端快照的代次，写出失败返回错误，不展开客户端或 TLS 材料。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsHttpClientSnapshot")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// 所有 clone 共享的原子 last-good TLS client 槽。
struct Inner {
    current: arc_swap::ArcSwap<TlsHttpClientSnapshot>,
}

/// 可由 [`nasecret::RotatingSecretStore`] 两阶段轮换的 TLS/mTLS HTTP client。
#[derive(Clone)]
pub struct RotatingTlsHttpClient {
    config: Arc<TlsHttpClientConfig>,
    watched_ids: Arc<BTreeSet<Arc<str>>>,
    inner: Arc<Inner>,
}

impl RotatingTlsHttpClient {
    /// 业务作用：从已验证初始 secret 快照构造 last-good client。
    ///
    /// 参数说明：
    /// - `initial`：固定第一代客户端材料的完整 secret 快照。
    /// - `config`：参与方身份、可选证书与信任包引用和总请求超时。
    ///
    /// 返回：参与方、引用 ID 与超时合法且本地 TLS 材料解析和客户端构建成功时返回第一代轮换入口；否则返回分类错误。不发起连接或握手，不能据此证明远端可用。引用使用本 adapter 的 64 字节平面 ASCII 标识约束。
    pub fn new(
        initial: &SecretSnapshot,
        config: TlsHttpClientConfig,
    ) -> Result<Self, TlsHttpClientError> {
        if !valid_id(&config.participant_id) {
            return Err(TlsHttpClientError::InvalidParticipantId);
        }
        if config.request_timeout.is_zero() || config.request_timeout > MAX_TLS_HTTP_TIMEOUT {
            return Err(TlsHttpClientError::InvalidRequestTimeout);
        }
        if config.identity.as_ref().is_some_and(|identity| {
            !valid_id(&identity.certificate_chain) || !valid_id(&identity.private_key)
        }) || config
            .trust
            .as_ref()
            .is_some_and(|trust| !valid_id(&trust.certificates))
        {
            return Err(TlsHttpClientError::InvalidSecretReference);
        }
        let mut watched_ids = BTreeSet::new();
        if let Some(identity) = &config.identity {
            watched_ids.insert(Arc::clone(&identity.certificate_chain));
            watched_ids.insert(Arc::clone(&identity.private_key));
        }
        if let Some(trust) = &config.trust {
            watched_ids.insert(Arc::clone(&trust.certificates));
        }
        let initial_client = build_client(initial, &config)?;
        Ok(Self {
            config: Arc::new(config),
            watched_ids: Arc::new(watched_ids),
            inner: Arc::new(Inner {
                current: arc_swap::ArcSwap::from_pointee(initial_client),
            }),
        })
    }

    /// 业务作用：固定当前 generation/client；调用方应在一个请求内复用返回的同一 `Arc`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回当前已发布客户端的 Arc 快照，后续轮换不修改该对象；空依赖集合时不会因无关 secret 变化自动更新。
    pub fn current(&self) -> Arc<TlsHttpClientSnapshot> {
        self.inner.current.load_full()
    }

    /// 业务作用：返回本 client 观察的 secret ID 集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用配置声明的证书链、私钥与信任包 ID 去重集合，允许为空。
    pub fn watched_ids(&self) -> &BTreeSet<Arc<str>> {
        &self.watched_ids
    }
}

#[async_trait::async_trait]
impl SecretRotationParticipant for RotatingTlsHttpClient {
    /// 业务作用：返回两阶段协调器使用的稳定 participant ID。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用构造时已经验证的参与方标识，不包含材料。
    fn id(&self) -> &str {
        self.config.participant_id.as_ref()
    }

    /// 业务作用：判断本 client 引用的 identity 或 trust secret 是否发生变化。
    ///
    /// 参数说明：
    /// - `changed_ids`：协调器计算的新增、删除或内容变化的 secret ID。
    ///
    /// 返回：变更 ID 与声明材料引用有交集时返回 true，否则 false；不读取候选内容。
    fn affected(&self, changed_ids: &BTreeSet<Arc<str>>) -> bool {
        changed_ids.iter().any(|id| self.watched_ids.contains(id))
    }

    /// 业务作用：完整构造候选 reqwest client；任何 PEM/TLS 错误都发生在统一 commit 之前。
    ///
    /// 参数说明：
    /// - `candidate`：本轮客户端构造固定使用的完整候选快照。
    /// - `_changed_ids`：由协调器用于影响筛选的集合，本实现直接按配置重建完整客户端。
    ///
    /// 返回：本地材料解析和客户端构建成功后返回未发布的候选；缺少或非法材料、构建失败时返回静态准备错误。当前客户端不变，不验证候选代次递增，也不主动握手。
    async fn prepare(
        &self,
        candidate: Arc<SecretSnapshot>,
        _changed_ids: &BTreeSet<Arc<str>>,
    ) -> Result<Box<dyn PreparedSecretRotation>, SecretPrepareError> {
        let next = build_client(&candidate, &self.config).map_err(prepare_error)?;
        Ok(Box::new(PreparedHttpClient {
            inner: Arc::clone(&self.inner),
            next: Arc::new(next),
        }))
    }
}

/// 已完成 TLS 构造、等待协调器原子发布的候选 client。
struct PreparedHttpClient {
    inner: Arc<Inner>,
    next: Arc<TlsHttpClientSnapshot>,
}

impl PreparedSecretRotation for PreparedHttpClient {
    /// 业务作用：将候选 client 原子替换为后续请求使用的 last-good。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：把已构造候选 Arc 交换到共享当前槽；旧请求仍可持有旧快照，不主动发起网络操作或等待旧请求退出。
    fn commit(self: Box<Self>) {
        self.inner.current.store(self.next);
    }

    /// 业务作用：候选未发布时直接释放；其中不持有外部副作用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅消费并释放候选与其客户端，不改动当前已发布快照。
    fn abort(self: Box<Self>) {}
}

/// 业务作用：从同代 secret 快照解析 identity/trust，并构造禁重定向、仅 HTTPS 的 reqwest client。
///
/// 参数说明：
/// - `snapshot`：本次借用所有 TLS 材料的同一快照。
/// - `config`：调用方已检查身份、引用和超时的客户端配置。
///
/// 返回：成功返回与 snapshot 同代的 HTTPS-only、禁止重定向客户端；配置 identity 时解析拼接的证书链与私钥，配置 trust 时禁用系统根并要求非空证书集合。材料解析或构建失败返回分类错误，不发起请求或证明证书与远端握手成功。
fn build_client(
    snapshot: &SecretSnapshot,
    config: &TlsHttpClientConfig,
) -> Result<TlsHttpClientSnapshot, TlsHttpClientError> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .https_only(true)
        .timeout(config.request_timeout);

    if let Some(reference) = &config.identity {
        let material = reference
            .resolve(snapshot)
            .map_err(TlsHttpClientError::InvalidMaterial)?;
        let mut pem = Zeroizing::new(Vec::with_capacity(
            material.certificate_chain.len() + material.private_key.len() + 1,
        ));
        pem.extend_from_slice(material.certificate_chain.expose());
        pem.push(b'\n');
        pem.extend_from_slice(material.private_key.expose());
        let identity =
            reqwest::Identity::from_pem(&pem).map_err(|_| TlsHttpClientError::InvalidIdentity)?;
        builder = builder.identity(identity);
    }

    if let Some(reference) = &config.trust {
        // 显式 trust bundle 表示该 client 的完整信任边界；不能在背后继续信任系统 Web PKI。
        builder = builder.tls_built_in_root_certs(false);
        let material = reference
            .resolve(snapshot)
            .map_err(TlsHttpClientError::InvalidMaterial)?;
        let certificates = reqwest::Certificate::from_pem_bundle(material.certificates.expose())
            .map_err(|_| TlsHttpClientError::InvalidTrustBundle)?;
        if certificates.is_empty() {
            return Err(TlsHttpClientError::InvalidTrustBundle);
        }
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }

    let client = builder
        .build()
        .map_err(|_| TlsHttpClientError::ClientBuildFailed)?;
    Ok(TlsHttpClientSnapshot {
        generation: snapshot.generation(),
        client,
    })
}

/// 业务作用：校验 participant 与 secret 引用使用的短 ASCII ID。
///
/// 参数说明：
/// - `value`：参与方或本 adapter 引用的平面标识。
///
/// 返回：非空、最多 64 字节且只含 ASCII 字母数字、连字符、下划线或点时返回 true；不接受 slash 层级，也不检查引用实际存在。
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 业务作用：将 TLS 细节错误压缩为协调器可安全记录的静态错误码。
///
/// 参数说明：
/// - `error`：本地 TLS 客户端准备阶段的分类错误。
///
/// 返回：按错误类别返回稳定 SecretPrepareError 静态码，丢弃材料引用及下层详情，不改变当前客户端。
fn prepare_error(error: TlsHttpClientError) -> SecretPrepareError {
    let code = match error {
        TlsHttpClientError::InvalidParticipantId => "invalid-participant-id",
        TlsHttpClientError::InvalidRequestTimeout => "invalid-request-timeout",
        TlsHttpClientError::InvalidSecretReference => "invalid-secret-reference",
        TlsHttpClientError::InvalidMaterial(_) => "tls-material-invalid",
        TlsHttpClientError::InvalidIdentity => "tls-identity-invalid",
        TlsHttpClientError::InvalidTrustBundle => "tls-trust-invalid",
        TlsHttpClientError::ClientBuildFailed => "http-client-build-failed",
    };
    SecretPrepareError::new(code)
}
