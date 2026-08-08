//! OAuth Resource Server / JWKS 认证组件。
//!
//! `AuthComponent` 把 JWT access token 校验所需的 **JWKS 注册表 + token 策略** 做成配置驱动、由容器
//! 托管生命周期的能力:Start 只解析并校验配置(不做任何网络 I/O),Ready 阶段(必须早于 Web Ready)
//! warmup 出不可变 [`JwksRegistry`],构造 [`Authenticator`](crate::authn::Authenticator) 并发布到
//! Application 的认证槽位,供 Web 装配 authentication 中间件消费。
//!
//! 组件顺序固定为 `nacos-config/secrets -> auth -> web`:auth 声明序在 web 前,故 Ready 先发布
//! Authenticator,web Ready 随后读取。auth **必须与 web 同时声明**——没有 Web 消费者的独立
//! AuthComponent 在当前 runtime 中没有意义,也因此自然进入 Service 模式。
//!
//! JWKS 有两种取法,互斥:**静态内联**(`auth.jwks`)——纯内存 warmup,无后台任务;或**远程**
//! (`auth.jwks_uri`)——Ready 首拉(失败 fail-closed 拒启),随后由受管刷新任务周期 re-fetch 并经
//! [`JwksRegistry::rotate`] 原子发布(候选先校验、失败保留 last-good);拉取受 HTTPS(loopback 允 http)、
//! host allowlist、超时与大小上限约束。远程模式注册 readiness contributor:每次成功刷新 observe Ready,
//! 拉取/候选失败 observe Degraded(last-good 仍验签、`/readyz` 保持 200),超 `jwks_stale_secs` 未成功则
//! 因 `affects_ready` 升级 NotReady(503)。刷新任务是受 Runner 监督的关键任务,进入停机态即优雅退出。

use std::sync::Arc;
use std::time::{Duration, Instant};

use nauth_oauth::{JwkSet, JwksRegistry, MetadataClient, MetadataOptions, TokenPolicy};
use serde::Deserialize;

use crate::authn::Authenticator;
use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ReadyContext, StartContext,
};

/// JWKS 定期刷新缺省间隔秒数。
const DEFAULT_JWKS_REFRESH_SECS: u64 = 300;
/// 单次 JWKS 拉取缺省超时毫秒数。
const DEFAULT_JWKS_TIMEOUT_MS: u64 = 3000;
/// JWKS 响应体缺省大小上限(字节),防超大响应打爆内存。
const DEFAULT_JWKS_MAX_BYTES: usize = 1_048_576;
/// JWKS key 数缺省上限。
const DEFAULT_JWKS_MAX_KEYS: usize = nauth_oauth::jwks::DEFAULT_MAX_KEYS;
/// last-good 缺省 stale 上限秒数;超此仍未成功刷新则 `/readyz` 转 503。
const DEFAULT_JWKS_STALE_SECS: u64 = 3600;
/// 配置可接受的最大 token 时钟偏移，防止把过期检查实质关闭。
const MAX_LEEWAY_SECS: u64 = 3600;
/// 单次 JWKS 响应硬上限；配置只能在此范围内收紧。
const MAX_JWKS_BYTES: usize = 16 * 1024 * 1024;
/// 单个 JWKS key 数硬上限。Registry 当前固定使用同一上限，配置只允许进一步收紧。
const MAX_JWKS_KEYS: usize = nauth_oauth::jwks::DEFAULT_MAX_KEYS;

/// 认证组件负责读取的顶层配置根投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct AuthConfigRoot {
    auth: Option<AuthConfig>,
}

/// `auth` 配置段:JWT access token 校验策略 + 静态内联 JWKS。
///
/// `deny_unknown_fields`:拼写错误(如 `audiance`)在建立任何副作用前即被拒,而不是静默按缺省处理。
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthConfig {
    /// 期望 issuer(`iss`);必须与 token 精确相等。
    issuer: String,
    /// 期望 audience(`aud`)。
    audience: String,
    /// 算法白名单;缺省 `["RS256"]`。不得包含 `none`。
    #[serde(default = "default_algorithms")]
    allowed_algorithms: Vec<String>,
    /// 时钟偏移容忍秒数;缺省 60。
    #[serde(default = "default_leeway_secs")]
    leeway_secs: u64,
    /// 静态内联 JWKS(标准 `{ "keys": [...] }` 结构);与 `jwks_uri` **互斥**,必给其一。
    #[serde(default)]
    jwks: Option<JwkSet>,
    /// 远程 JWKS 端点 URL;与 `jwks` **互斥**。scheme 必须 https(loopback 主机允 http)。
    #[serde(default)]
    jwks_uri: Option<String>,
    /// RFC 8414 Authorization Server Metadata URL；由其安全解析 `jwks_uri`，与另外两种来源互斥。
    #[serde(default)]
    metadata_uri: Option<String>,
    /// 远程 JWKS 定期刷新间隔秒数;缺省 300。仅 `jwks_uri` 生效。
    #[serde(default = "default_jwks_refresh_secs")]
    jwks_refresh_secs: u64,
    /// 单次 JWKS 拉取超时毫秒数;缺省 3000。
    #[serde(default = "default_jwks_timeout_ms")]
    jwks_timeout_ms: u64,
    /// JWKS 响应体大小上限(字节);缺省 1 MiB。
    #[serde(default = "default_jwks_max_bytes")]
    jwks_max_bytes: usize,
    /// JWKS key 数上限；与字节上限共同抵御大量小 key。
    #[serde(default = "default_jwks_max_keys")]
    jwks_max_keys: usize,
    /// `jwks_uri` 主机白名单;非空时 URL 主机必须命中。空=只按 URL 主机自身(仍受 https/loopback 约束)。
    #[serde(default)]
    jwks_allowed_hosts: Vec<String>,
    /// last-good stale 上限秒数;超此仍未成功刷新则 `/readyz` 转 503。缺省 3600。
    #[serde(default = "default_jwks_stale_secs")]
    jwks_stale_secs: u64,
}

/// 业务作用：token 算法白名单缺省值:仅 RS256。
fn default_algorithms() -> Vec<String> {
    vec!["RS256".to_owned()]
}

/// 业务作用：时钟偏移容忍缺省值:60 秒。
fn default_leeway_secs() -> u64 {
    60
}

/// 业务作用：JWKS 刷新间隔缺省值。
fn default_jwks_refresh_secs() -> u64 {
    DEFAULT_JWKS_REFRESH_SECS
}

/// 业务作用：JWKS 拉取超时缺省值。
fn default_jwks_timeout_ms() -> u64 {
    DEFAULT_JWKS_TIMEOUT_MS
}

/// 业务作用：JWKS 响应大小上限缺省值。
fn default_jwks_max_bytes() -> usize {
    DEFAULT_JWKS_MAX_BYTES
}

/// 业务作用：JWKS 单次快照最大 key 数缺省值。
fn default_jwks_max_keys() -> usize {
    DEFAULT_JWKS_MAX_KEYS
}

/// 业务作用：JWKS last-good stale 上限缺省值。
fn default_jwks_stale_secs() -> u64 {
    DEFAULT_JWKS_STALE_SECS
}

impl AuthConfig {
    /// 业务作用：无副作用校验:issuer/audience 非空、算法白名单非空且不含 `none`、JWKS 结构合法。
    ///
    /// # 参数
    ///
    /// - `phase`:本次校验所属生命周期阶段,用于错误归因。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.issuer.trim().is_empty() {
            return Err(auth_error(phase, "auth.issuer cannot be empty"));
        }
        if self.audience.trim().is_empty() {
            return Err(auth_error(phase, "auth.audience cannot be empty"));
        }
        if self.allowed_algorithms.as_slice() != ["RS256"] {
            return Err(auth_error(
                phase,
                "auth.allowed_algorithms currently supports exactly [`RS256`]",
            ));
        }
        if self.leeway_secs > MAX_LEEWAY_SECS {
            return Err(auth_error(
                phase,
                format!("auth.leeway_secs must not exceed {MAX_LEEWAY_SECS}"),
            ));
        }
        // key 来源三选一:静态、直接 JWKS URL 或 RFC 8414 metadata。
        let has_static = self.jwks.is_some();
        let has_jwks_uri = self
            .jwks_uri
            .as_deref()
            .is_some_and(|uri| !uri.trim().is_empty());
        let has_metadata = self
            .metadata_uri
            .as_deref()
            .is_some_and(|uri| !uri.trim().is_empty());
        if usize::from(has_static) + usize::from(has_jwks_uri) + usize::from(has_metadata) != 1 {
            return Err(auth_error(
                phase,
                "auth requires exactly one of `jwks` (static inline) or `jwks_uri` (remote), or `metadata_uri` (RFC 8414)",
            ));
        }
        if let Some(jwks) = &self.jwks {
            // 静态 JWKS 结构校验(非空、无重复 kid、每个 key 有非空 kty)。
            jwks.validate_with_max_keys(self.jwks_max_keys)
                .map_err(|error| auth_error_src(phase, "invalid auth.jwks", error))?;
        }
        if let Some(uri) = self
            .jwks_uri
            .as_deref()
            .filter(|uri| !uri.trim().is_empty())
        {
            self.validate_jwks_uri(uri, phase)?;
        }
        if let Some(uri) = self
            .metadata_uri
            .as_deref()
            .filter(|uri| !uri.trim().is_empty())
        {
            MetadataClient::new(
                self.issuer.clone(),
                uri,
                MetadataOptions {
                    timeout: Duration::from_millis(self.jwks_timeout_ms),
                    max_response_bytes: self.jwks_max_bytes,
                    allowed_hosts: self
                        .jwks_allowed_hosts
                        .iter()
                        .map(|host| host.to_ascii_lowercase())
                        .collect(),
                },
            )
            .map_err(|error| {
                auth_error_src(phase, "auth.metadata_uri is not a safe metadata URL", error)
            })?;
        }
        if has_jwks_uri || has_metadata {
            if self.jwks_refresh_secs == 0
                || self.jwks_timeout_ms == 0
                || self.jwks_max_bytes == 0
                || self.jwks_max_keys == 0
            {
                return Err(auth_error(
                    phase,
                    "auth.jwks_refresh_secs / jwks_timeout_ms / jwks_max_bytes / jwks_max_keys must be greater than zero",
                ));
            }
            if self.jwks_stale_secs <= self.jwks_refresh_secs {
                return Err(auth_error(
                    phase,
                    "auth.jwks_stale_secs must be greater than auth.jwks_refresh_secs",
                ));
            }
            if self.jwks_timeout_ms > 60_000
                || self.jwks_refresh_secs > crate::runner::MAX_LIFECYCLE_TIMEOUT.as_secs()
                || self.jwks_stale_secs > crate::runner::MAX_LIFECYCLE_TIMEOUT.as_secs()
                || self.jwks_max_bytes > MAX_JWKS_BYTES
                || self.jwks_max_keys > MAX_JWKS_KEYS
            {
                return Err(auth_error(
                    phase,
                    "auth JWKS timeout/refresh/stale/response/key limits exceed framework hard limits",
                ));
            }
        }
        Ok(())
    }

    /// 业务作用：校验 `jwks_uri` 的 scheme 与主机:必须 https(loopback 主机允 http),命中主机白名单。
    ///
    /// # 参数
    ///
    /// - `uri`:配置的远程 JWKS URL。
    /// - `phase`:错误归因所属生命周期阶段。
    fn validate_jwks_uri(&self, uri: &str, phase: ApplicationPhase) -> ApplicationResult<()> {
        let parsed = reqwest::Url::parse(uri)
            .map_err(|error| auth_error_src(phase, "auth.jwks_uri is not a valid URL", error))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| auth_error(phase, "auth.jwks_uri must include a host"))?
            .to_ascii_lowercase();
        let is_loopback = matches!(host.as_str(), "127.0.0.1" | "::1" | "localhost");
        if parsed.scheme() != "https" && !(parsed.scheme() == "http" && is_loopback) {
            return Err(auth_error(
                phase,
                "auth.jwks_uri must use https (http is allowed only for loopback hosts)",
            ));
        }
        if !self.jwks_allowed_hosts.is_empty()
            && !self
                .jwks_allowed_hosts
                .iter()
                .any(|allowed| allowed.to_ascii_lowercase() == host)
        {
            return Err(auth_error(
                phase,
                format!("auth.jwks_uri host `{host}` is not in auth.jwks_allowed_hosts"),
            ));
        }
        Ok(())
    }

    /// 业务作用：由配置构造 token 校验策略。
    fn token_policy(&self) -> TokenPolicy {
        TokenPolicy {
            expected_issuer: self.issuer.clone(),
            expected_audience: self.audience.clone(),
            allowed_algorithms: self.allowed_algorithms.clone(),
            leeway_secs: self.leeway_secs,
        }
    }
}

/// 认证组件:Start 解析并冻结配置,Ready warmup JWKS 并发布 Authenticator。
pub(crate) struct AuthComponent {
    config: Option<AuthConfig>,
    /// 远程 `jwks_uri` 模式下 Start 登记(封口前)、Ready observe、并交给刷新任务的 JWKS 就绪贡献句柄。
    jwks_contributor: Option<ReadinessContributor>,
    /// 远程 `jwks_uri` 模式下 Ready 创建、交由 Runner 监督的 JWKS 刷新任务;静态模式为 None。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl AuthComponent {
    /// 业务作用：创建尚未读取配置的认证组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数;JWKS 注册表与 Authenticator 在 Ready 阶段才构造。
    pub(crate) fn new() -> Self {
        Self {
            config: None,
            jwks_contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for AuthComponent {
    /// 业务作用：返回认证组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数;Runner 用它归类认证相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::Auth
    }

    /// 业务作用：从最终配置读取并冻结认证设置;不做任何网络 I/O。
    ///
    /// # 参数
    ///
    /// - `context`:提供最终配置树的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_auth_config(context.application())?;
            config.validate(ApplicationPhase::Start)?;
            // 远程模式:readiness contributor 必须在 UserHook 封口前(Start)注册,运行期由刷新任务翻转;
            // 首拉在 Ready(warmup 前)完成,故此处只登记 slot,初值 Unknown(启动态本就 503)。
            if config
                .jwks_uri
                .as_deref()
                .is_some_and(|uri| !uri.trim().is_empty())
                || config
                    .metadata_uri
                    .as_deref()
                    .is_some_and(|uri| !uri.trim().is_empty())
            {
                let contributor = context.application().register_readiness(
                    ComponentId::Auth,
                    Arc::<str>::from("auth:jwks"),
                    ReadinessPolicy {
                        affects_ready: true,
                        failure_threshold: 1,
                        recovery_threshold: 1,
                        stale_after: Some(Duration::from_secs(config.jwks_stale_secs)),
                    },
                )?;
                self.jwks_contributor = Some(contributor);
            }
            self.config = Some(config);
            Ok(())
        })
    }

    /// 业务作用：warmup JWKS(静态内存或远程首拉)、构造并发布 Authenticator(必须早于 Web Ready);远程模式再
    /// 注册 readiness contributor 并创建刷新任务。
    ///
    /// # 参数
    ///
    /// - `context`:提供 Application(据此发布 Authenticator、注册 readiness)的 Ready 上下文。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = self.config.clone().ok_or_else(|| {
                auth_error(
                    ApplicationPhase::Ready,
                    "auth configuration was not prepared during start",
                )
            })?;
            let application = context.application().clone();
            let policy = config.token_policy();

            if config.jwks_uri.is_some() || config.metadata_uri.is_some() {
                let (uri, source) = if let Some(uri) = config
                    .jwks_uri
                    .as_deref()
                    .filter(|uri| !uri.trim().is_empty())
                {
                    (uri.to_owned(), "jwks_uri")
                } else {
                    let metadata_uri = config
                        .metadata_uri
                        .as_deref()
                        .filter(|uri| !uri.trim().is_empty())
                        .expect("validated remote source");
                    let metadata = MetadataClient::new(
                        config.issuer.clone(),
                        metadata_uri,
                        MetadataOptions {
                            timeout: Duration::from_millis(config.jwks_timeout_ms),
                            max_response_bytes: config.jwks_max_bytes,
                            allowed_hosts: config
                                .jwks_allowed_hosts
                                .iter()
                                .map(|host| host.to_ascii_lowercase())
                                .collect(),
                        },
                    )
                    .map_err(|error| {
                        auth_error_src(
                            ApplicationPhase::Ready,
                            "auth metadata client construction failed",
                            error,
                        )
                    })?
                    .fetch()
                    .await
                    .map_err(|error| {
                        auth_error_src(
                            ApplicationPhase::Ready,
                            "auth initial Authorization Server Metadata fetch failed",
                            error,
                        )
                    })?;
                    (metadata.jwks_uri, "metadata_uri")
                };
                // 远程首拉:失败即 fail closed 拒启(首次无 key fail closed),不发布半初始化的认证器。
                let jwks = fetch_jwks(
                    &uri,
                    config.jwks_timeout_ms,
                    config.jwks_max_bytes,
                    config.jwks_max_keys,
                )
                .await
                .map_err(|error| {
                    auth_error_src(
                        ApplicationPhase::Ready,
                        "auth initial JWKS fetch from jwks_uri failed",
                        error,
                    )
                })?;
                let key_count = jwks.keys.len();
                let registry = Arc::new(JwksRegistry::warmup(jwks).map_err(|error| {
                    auth_error_src(ApplicationPhase::Ready, "auth JWKS warmup failed", error)
                })?);
                let authenticator: crate::authn::SharedAuthenticator =
                    Arc::new(Authenticator::new(Arc::clone(&registry), policy));
                application.publish_authenticator_from_component(authenticator)?;

                // Start 已登记的 contributor:首拉成功即 observe Ready;运行期由刷新任务翻转。affects_ready
                // + stale_after → 长时间刷新失败(last-good 过期)升级为 NotReady(503)。
                let contributor = self.jwks_contributor.take().ok_or_else(|| {
                    auth_error(
                        ApplicationPhase::Ready,
                        "auth jwks readiness contributor was not registered during start",
                    )
                })?;
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());

                self.critical_task = Some(Box::pin(run_jwks_refresh(JwksRefreshContext {
                    application,
                    registry,
                    contributor,
                    uri,
                    interval: Duration::from_secs(config.jwks_refresh_secs),
                    timeout_ms: config.jwks_timeout_ms,
                    max_bytes: config.jwks_max_bytes,
                    max_keys: config.jwks_max_keys,
                })));
                tracing::info!(
                    "auth component published JWT authenticator from {source} ({key_count} JWK(s), refresh every {}s)",
                    config.jwks_refresh_secs,
                );
            } else {
                // 静态 key set:warmup 是纯内存操作,失败即 fail closed。validate 已保证此时 jwks 存在。
                let jwks = config
                    .jwks
                    .clone()
                    .expect("validate guarantees static jwks is present when jwks_uri is absent");
                let key_count = jwks.keys.len();
                let registry = JwksRegistry::warmup(jwks).map_err(|error| {
                    auth_error_src(ApplicationPhase::Ready, "auth JWKS warmup failed", error)
                })?;
                let authenticator: crate::authn::SharedAuthenticator =
                    Arc::new(Authenticator::new(Arc::new(registry), policy));
                // 发布到认证槽位;若业务已在 UserHook 用 set_authenticator 注入,则冲突并明确报错。
                application.publish_authenticator_from_component(authenticator)?;
                tracing::info!(
                    "auth component published JWT authenticator (static JWKS, {key_count} JWK(s))"
                );
            }
            Ok(())
        })
    }

    /// 业务作用：取出远程 JWKS 刷新任务,交由 Runner 按关键任务监督。
    ///
    /// # 返回
    ///
    /// 远程 `jwks_uri` 模式下首次调用返回刷新任务;静态模式或重复调用返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("auth-jwks-refresh", task))
    }
}

/// 远程 JWKS 定期刷新任务:周期 re-fetch → 校验候选 → 原子 rotate 发布;失败保留 last-good。
///
/// 拉取或候选校验失败都**不**返回错误(那会被 Runner 判为关键任务崩溃):last-good key set 仍能验签,只把
/// contributor 置 Degraded(`/readyz` 保持 200);连续失败超 `stale_after` 才由 registry 升级 NotReady。任务
/// 只在进入停机态时返回 `Ok(())` 优雅退出(单任务天然单飞,无并发拉取)。
///
struct JwksRefreshContext {
    application: Application,
    registry: Arc<JwksRegistry>,
    contributor: ReadinessContributor,
    uri: String,
    interval: Duration,
    timeout_ms: u64,
    max_bytes: usize,
    max_keys: usize,
}

/// # 参数
///
/// 业务作用：- `context`:刷新任务独占的生命周期句柄、last-good registry、readiness contributor 和远程拉取边界。
async fn run_jwks_refresh(context: JwksRefreshContext) -> ApplicationResult<()> {
    loop {
        // 先睡:Ready 已完成首拉,下一次是间隔后的刷新。
        tokio::time::sleep(context.interval).await;
        match context.application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                return Ok(());
            }
            ApplicationState::Starting => continue,
            ApplicationState::Ready => {}
        }
        let now = Instant::now();
        match fetch_jwks(
            &context.uri,
            context.timeout_ms,
            context.max_bytes,
            context.max_keys,
        )
        .await
        {
            Ok(jwks) => match context.registry.rotate(jwks) {
                Ok(generation) => {
                    context
                        .contributor
                        .observe(DependencyState::Ready, reason::HEALTHY, now);
                    tracing::debug!("auth JWKS refreshed from jwks_uri (generation={generation})");
                }
                Err(error) => {
                    // 候选非法:保留 last-good,不发布坏 key set。
                    context.contributor.observe_without_refreshing_freshness(
                        DependencyState::Degraded,
                        reason::DEGRADED,
                        now,
                    );
                    tracing::warn!(
                        "auth JWKS refresh candidate rejected, keeping last-good: {error}"
                    );
                }
            },
            Err(error) => {
                // 拉取失败:last-good 仍验签;stale_after 到点后 registry 自动升级 NotReady。
                context.contributor.observe_without_refreshing_freshness(
                    DependencyState::Degraded,
                    reason::DEGRADED,
                    now,
                );
                tracing::warn!("auth JWKS refresh fetch failed, keeping last-good: {error}");
            }
        }
    }
}

/// 业务作用：从远程 `jwks_uri` 拉取并解析 JWKS:超时、大小上限、UTF-8、结构校验。
///
/// 只做一次 GET,不重试(重试由刷新任务的下一周期承担)。content-length 预检 + 读后复检双重限大小;正文
/// 非 200、超限、非 UTF-8、解析失败、结构非法都返回错误,由调用方决定 fail-closed 或保留 last-good。
///
/// # 参数
///
/// - `uri`:远程 JWKS URL(已在 validate 期校验 scheme/host)。
/// - `timeout_ms`:单次请求超时毫秒。
/// - `max_bytes`:响应体字节上限。
async fn fetch_jwks(
    uri: &str,
    timeout_ms: u64,
    max_bytes: usize,
    max_keys: usize,
) -> anyhow::Result<JwkSet> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        // 重定向后的目标没有经过 scheme/host allowlist 校验；拒绝跟随，封住 SSRF 绕过。
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut response = client.get(uri).send().await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("JWKS endpoint returned HTTP {}", status.as_u16());
    }
    if let Some(length) = response.content_length() {
        anyhow::ensure!(
            length <= max_bytes as u64,
            "JWKS response content-length {length} exceeds {max_bytes} byte limit"
        );
    }
    let mut bytes =
        Vec::with_capacity(response.content_length().unwrap_or(0).min(max_bytes as u64) as usize);
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            chunk.len() <= max_bytes.saturating_sub(bytes.len()),
            "JWKS response exceeds {max_bytes} byte limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| anyhow::anyhow!("JWKS response body is not valid UTF-8"))?;
    let jwks = JwkSet::parse(text)?;
    jwks.validate_with_max_keys(max_keys)?;
    Ok(jwks)
}

/// 业务作用：从最终配置读取 `auth` 段;缺失该段却声明了组件时报明确错误。
///
/// # 参数
///
/// - `application`:提供当前不可变配置快照的共享上下文。
fn read_auth_config(application: &Application) -> ApplicationResult<AuthConfig> {
    let snapshot = application.config();
    let root: AuthConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            auth_error_src(
                ApplicationPhase::Start,
                "invalid `auth` configuration section",
                error,
            )
        })?;
    root.auth.ok_or_else(|| {
        auth_error(
            ApplicationPhase::Start,
            "component `auth` is declared but the `auth` configuration section is missing",
        )
    })
}

/// 业务作用：在不构造任何注册表的前提下校验候选配置树中的 `auth` 段。
///
/// # 参数
///
/// - `tree`:合并、插值完成但尚未发布的候选配置树。
/// - `phase`:本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_auth_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("auth") else {
        return Ok(());
    };
    let config: AuthConfig = serde_json::from_value(section.clone())
        .map_err(|error| auth_error_src(phase, "invalid `auth` configuration section", error))?;
    config.validate(phase)
}

/// 业务作用：创建认证组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含 token、签名、claims 或 JWKS 正文的稳定摘要。
fn auth_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Auth, phase, message)
}

/// 业务作用：创建带底层错误链的认证组件错误(输出前统一脱敏)。
///
/// # 参数
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含敏感内容的稳定摘要。
/// - `source`:只供诊断的底层错误。
fn auth_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Auth, phase, message, source)
}
