//! OAuth Resource Server / JWKS 认证组件。
//!
//! `AuthComponent` 把 JWT access token 校验所需的 **JWKS 注册表 + token 策略** 做成配置驱动、由容器
//! 托管生命周期的能力:Start 只解析并校验配置(不做任何网络 I/O),Ready 阶段(必须早于 Web Ready)
//! 以已校验快照初始化 [`JwksRegistry`]，构造 [`Authenticator`](crate::authn::Authenticator) 并发布到
//! Application 的认证槽位,供 Web 装配 authentication 中间件消费。
//!
//! 组件顺序固定为 `nacos-config/secrets -> auth -> web`:auth 声明序在 web 前,故 Ready 先发布
//! Authenticator,web Ready 随后读取。auth **必须与 web 同时声明**——没有 Web 消费者的独立
//! AuthComponent 在当前 runtime 中没有意义,也因此自然进入 Service 模式。
//!
//! 密钥来源三选一：静态 `auth.jwks`、直接远程 `auth.jwks_uri` 或 RFC 8414 `auth.metadata_uri`。
//! 静态来源只在内存预热；metadata 在 Ready 解析一次 JWKS 地址，随后与直接远程来源共用定期刷新。
//! 初次拉取或校验失败拒绝启动；后续候选通过校验才经 [`JwksRegistry::rotate`] 发布，失败保留上一份可用快照。
//! 远程拉取受 HTTPS、本机 HTTP 例外、主机白名单、超时与响应大小约束，不跟随重定向。
//! 刷新失败降低就绪贡献状态但不刷新成功新鲜度，超过 `jwks_stale_secs` 会使该依赖不再满足就绪要求；
//! 保留的密钥仍可用于验签。刷新由 Runner 监督，任务在轮次之间观察停机状态。

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
    /// 可选认证段；明确声明组件却缺少此段时由组件启动拒绝。
    auth: Option<AuthConfig>,
}

/// `auth` 配置段：JWT access token 校验策略与唯一密钥来源。
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
    /// 静态内联 JWKS（标准 `{ "keys": [...] }` 结构）；三种密钥来源必须恰好配置一种。
    #[serde(default)]
    jwks: Option<JwkSet>,
    /// 远程 JWKS 端点 URL；非空白时与其它来源互斥，HTTPS 或本机 HTTP 才允许。
    #[serde(default)]
    jwks_uri: Option<String>,
    /// RFC 8414 Authorization Server Metadata URL；非空白时作为唯一来源，校验后从响应解析 JWKS 地址。
    #[serde(default)]
    metadata_uri: Option<String>,
    /// 直接远程或 metadata 解析出的 JWKS 定期刷新等待秒数，缺省 300。
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
///
/// 参数说明：无。
///
/// 返回：只允许 RS256 的独立列表，供未配置算法字段时使用，不根据 token 内容自动扩展算法。
fn default_algorithms() -> Vec<String> {
    vec!["RS256".to_owned()]
}

/// 业务作用：时钟偏移容忍缺省值:60 秒。
///
/// 参数说明：无。
///
/// 返回：60 秒的 token 时间校验容差，不改变节点时钟，也不关闭过期检查。
fn default_leeway_secs() -> u64 {
    60
}

/// 业务作用：JWKS 刷新间隔缺省值。
///
/// 参数说明：无。
///
/// 返回：远程密钥刷新每轮开始前的缺省等待秒数；不包含上一轮拉取耗时，不创建刷新任务。
fn default_jwks_refresh_secs() -> u64 {
    DEFAULT_JWKS_REFRESH_SECS
}

/// 业务作用：JWKS 拉取超时缺省值。
///
/// 参数说明：无。
///
/// 返回：缺省单次远程请求超时毫秒数，用于限制首拉和刷新等待，不提供整个启动阶段的总预算。
fn default_jwks_timeout_ms() -> u64 {
    DEFAULT_JWKS_TIMEOUT_MS
}

/// 业务作用：JWKS 响应大小上限缺省值。
///
/// 参数说明：无。
///
/// 返回：缺省远程响应正文的最大字节数，用于 Content-Length 预检和分块累积检查，不代表进程总内存预算。
fn default_jwks_max_bytes() -> usize {
    DEFAULT_JWKS_MAX_BYTES
}

/// 业务作用：JWKS 单次快照最大 key 数缺省值。
///
/// 参数说明：无。
///
/// 返回：与 nauth_oauth 注册表一致的缺省 key 数限制；配置可以进一步收紧，不按响应实际数量自动放宽。
fn default_jwks_max_keys() -> usize {
    DEFAULT_JWKS_MAX_KEYS
}

/// 业务作用：JWKS last-good stale 上限缺省值。
///
/// 参数说明：无。
///
/// 返回：远程密钥成功观测事实的缺省有效秒数；超时影响就绪状态，不由该数值直接删除现有密钥。
fn default_jwks_stale_secs() -> u64 {
    DEFAULT_JWKS_STALE_SECS
}

impl AuthConfig {
    /// 业务作用：在认证发布前校验身份策略、唯一密钥来源与远程资源上限，拒绝不受支持的认证配置。
    ///
    /// 参数说明：
    ///
    /// - `phase`:本次校验所属生命周期阶段,用于错误归因。
    ///
    /// 返回：issuer/audience 非空、算法恰为 RS256、来源三选一且适用预算合法时成功；首次违反约束返回认证组件错误，不拉取远程资源。
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
        // 密钥来源必须唯一，避免静态与远程证据并存时无法确定实际验签依据。
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
            // 静态候选同样验证 key 数、标识唯一性与 RSA/RS256 能力，不能发布不可用的验签材料。
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
    /// 参数说明：
    ///
    /// - `uri`:配置的远程 JWKS URL。
    /// - `phase`:错误归因所属生命周期阶段。
    ///
    /// 返回：URL 可解析、scheme 满足 HTTPS 或列明的本机 HTTP 条件、配置的主机白名单命中时成功；否则返回认证配置错误，不解析 DNS 或访问网络。
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：复制 issuer、audience、算法及时间容差的 TokenPolicy，不重复校验配置或读取 token，调用前须完成 validate。
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
    /// Start 阶段验证并冻结的认证策略，Ready 不重新读取配置。
    config: Option<AuthConfig>,
    /// 远程来源在 Start 封口前登记、Ready 首次观测并交给刷新任务的就绪贡献句柄。
    jwks_contributor: Option<ReadinessContributor>,
    /// 远程来源在 Ready 创建并交由 Runner 监督的刷新任务；静态来源为 None。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl AuthComponent {
    /// 业务作用：创建尚未读取配置的认证组件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无配置、无 readiness 句柄和刷新任务的组件；JWKS 注册表与认证器在后续 Ready 阶段创建。
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
    /// 参数说明：无。
    ///
    /// 返回：固定的 ComponentId::Auth，供 Runner 排序、错误归类与生命周期记录使用，不读取配置。
    fn id(&self) -> ComponentId {
        ComponentId::Auth
    }

    /// 业务作用：从最终配置读取并冻结认证设置;不做任何网络 I/O。
    ///
    /// 参数说明：
    ///
    /// - `context`:提供最终配置树的 Start 上下文。
    ///
    /// 返回：配置有效时冻结认证设置，并为远程来源登记初始未就绪的贡献项；配置或登记失败返回错误，不发布认证器。
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

    /// 业务作用：加载并校验初始 JWKS，在 Web Ready 前发布认证器，远程模式同时准备持续刷新任务。
    ///
    /// 参数说明：
    ///
    /// - `context`:提供认证器发布槽位与应用生命周期的 Ready 上下文。
    ///
    /// 返回：密钥加载、注册表预热与唯一认证器发布成功时为 Ok；配置未准备、拉取、校验或发布冲突时返回错误。
    /// 远程模式使用 Start 已登记的 contributor，首次成功后标记 Ready 并保存待监督任务，不在此 spawn。
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

            // 来源选择与配置校验、Start 登记保持一致；空白可选地址不能覆盖已通过校验的静态密钥来源。
            if config
                .jwks_uri
                .as_deref()
                .is_some_and(|uri| !uri.trim().is_empty())
                || config
                    .metadata_uri
                    .as_deref()
                    .is_some_and(|uri| !uri.trim().is_empty())
            {
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
    /// 参数说明：无。
    ///
    /// 返回：远程来源 Ready 已完成时首次移交带固定名称的刷新任务；静态模式、尚未准备或已移交时为 None，不在此执行任务。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("auth-jwks-refresh", task))
    }
}

/// 远程 JWKS 定期刷新任务:周期 re-fetch → 校验候选 → 原子 rotate 发布;失败保留 last-good。
///
/// 拉取或候选失败不结束任务，上一份有效密钥仍可验签；失败只降低贡献状态，不延长其成功观测的有效期。
/// 超过 stale_after 后就绪聚合拒绝该依赖；刷新串行执行，轮次之间观察应用生命周期。
///
struct JwksRefreshContext {
    /// 用于判断是否进入停机或失败状态的应用句柄。
    application: Application,
    /// 先验证候选再发布新密钥快照的共享注册表。
    registry: Arc<JwksRegistry>,
    /// 记录成功新鲜度与刷新失败状态的就绪贡献句柄。
    contributor: ReadinessContributor,
    /// 首次解析后固定的 JWKS 地址，不在刷新期间重复读取 metadata。
    uri: String,
    /// 每次拉取前等待的间隔，不包含上次请求耗时。
    interval: Duration,
    /// 单次 GET 与响应读取的超时毫秒数。
    timeout_ms: u64,
    /// 累积远程正文时允许的最大字节数。
    max_bytes: usize,
    /// 候选快照允许的最大 key 数。
    max_keys: usize,
}

/// 业务作用：周期加载远程 JWKS，候选通过校验后轮换注册表，失败时保留上一份可用密钥并降低就绪状态。
///
/// 参数说明：
/// - `context`：本任务独占的应用状态、共享密钥注册表、readiness 句柄与固定远程拉取预算。
///
/// 返回：每轮先等待间隔，观察到停机或失败状态后正常结束；Starting 时跳过拉取，Ready 时单次拉取并更新观测。
/// 拉取或候选失败不会结束任务或刷新成功新鲜度；睡眠与请求期间不会主动响应状态变化，终止还受外层监督器控制。
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
/// 每次只发一个 GET，不重试且不跟随重定向；响应长度先预检，再在逐块累积前复验，避免超限正文继续分配。
///
/// 参数说明：
///
/// - `uri`:远程 JWKS URL(已在 validate 期校验 scheme/host)。
/// - `timeout_ms`:单次请求超时毫秒。
/// - `max_bytes`:响应体字节上限。
/// - `max_keys`：解析后允许的最大 key 数，每个 key 还须通过 RSA/RS256 能力与标识校验。
///
/// 返回：HTTP 成功状态、正文及密钥结构均合法时返回 JwkSet；网络、超时、非成功状态、超限、UTF-8 或结构错误均传播给调用方。
/// 不在此复验 URL 白名单或更新注册表，首拉失败拒启与刷新失败保留旧值由调用方决定。
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
/// 参数说明：
///
/// - `application`:提供当前不可变配置快照的共享上下文。
///
/// 返回：从同一配置快照反序列化的 AuthConfig；字段类型、未知 auth 属性或缺少 auth 段时返回 Start 阶段错误，不执行完整策略校验。
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
/// 参数说明：
///
/// - `tree`:合并、插值完成但尚未发布的候选配置树。
/// - `phase`:本次无副作用校验所属的生命周期阶段。
///
/// 返回：auth 段不存在时直接成功，存在时须能反序列化且通过完整配置校验；失败带入指定阶段，不发布配置或认证器。
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
/// 参数说明：
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含 token、签名、claims 或 JWKS 正文的稳定摘要。
///
/// 返回：归属于 Auth 与指定阶段、没有底层 source 的 ApplicationError；只保存摘要，不记录日志或主动脱敏输入。
fn auth_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Auth, phase, message)
}

/// 业务作用：保留认证失败的稳定摘要与底层原因，供生命周期统一处理。
///
/// 参数说明：
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含敏感内容的稳定摘要。
/// - `source`:只供诊断的底层错误。
///
/// 返回：带 Auth 身份、指定阶段与 source 的 ApplicationError；本函数不脱敏或输出，错误链的对外呈现必须遵守统一脱敏规则。
fn auth_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Auth, phase, message, source)
}
