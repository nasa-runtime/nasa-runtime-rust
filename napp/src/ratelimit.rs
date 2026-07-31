//! 跨副本分布式业务配额:`RateLimitProvider` 抽象 + nadis Redis 固定窗口后端。
//!
//! 与单实例每客户端令牌桶(`governance` 的 `RateLimit`)是**两层、正确性合同不同**:后者只护本
//! 进程(每副本各自一套桶);本层用**共享 Redis 原子计数**把「租户 / 主体 / API-key 总配额」在所有副本间
//! **合并计量**——N 个副本共用同一 key 即受同一上限,不会因扩容而放大总量。两者可叠加:先本进程护栏,
//! 再跨副本总配额。
//!
//! 后端故障策略:限流后端(Redis)不可达/超时时 [`RedisRateLimitProvider`] 选 **fail-open**(可用性优先,
//! 治理层自身基础设施抖动不该把业务流量打死),并 `warn`;需要 fail-closed 的高保障场景由调用方另择实现。

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nadis::RedisClient;
use sha2::{Digest as _, Sha256};

/// 一次配额判定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitOutcome {
    /// 是否放行。
    pub allowed: bool,
    /// 建议重试等待(仅拒绝时有意义;放行为 `None`)。
    pub retry_after: Option<Duration>,
}

impl RateLimitOutcome {
    /// 放行结果(无重试建议)。
    pub fn allow() -> Self {
        Self {
            allowed: true,
            retry_after: None,
        }
    }

    /// 拒绝结果 + 建议重试等待(补足窗口所需时长)。
    ///
    /// # 参数
    ///
    /// - `retry_after`:建议客户端等待多久再试。
    pub fn deny(retry_after: Duration) -> Self {
        Self {
            allowed: false,
            retry_after: Some(retry_after),
        }
    }
}

/// 跨副本业务配额提供方:对 `key` 记一次命中并判定它是否在 `window` 内超过 `limit`。
///
/// `key` 的粒度由调用方决定(租户 id / subject / API-key ……),本抽象只认字符串主体。实现**必须分布式
/// 原子**:多副本并发对同一 key 的计数不丢不重(典型后端 = 共享存储的原子计数)。基础设施故障时的放行/
/// 拒绝策略由实现决定并应在其文档中写明。
#[async_trait]
pub trait RateLimitProvider: Send + Sync + 'static {
    /// 记一次命中并判定 `key` 是否在 `window` 内超过 `limit`。
    ///
    /// # 参数
    ///
    /// - `key`:配额主体标识(调用方决定粒度)。
    /// - `limit`:窗口内允许的最大命中数(应 > 0)。
    /// - `window`:配额窗口时长。
    ///
    /// # 返回
    ///
    /// 放行 / 拒绝(拒绝含建议重试等待)。
    async fn check(&self, key: &str, limit: u32, window: Duration) -> RateLimitOutcome;
}

/// 业务注入用的共享配额提供方句柄。
pub type SharedRateLimitProvider = Arc<dyn RateLimitProvider>;

/// 固定窗口计数脚本:`INCR` 计数,首次命中(计数=1)时 `PEXPIRE` 开窗;返回 `{当前计数, 剩余 TTL(ms)}`。
///
/// 原子性由单条 Lua 在 Redis 服务端串行化保证:并发副本对同一 key 的自增不丢不重。窗口自首次命中起算、
/// 到期自动清零重开(rolling fixed window),无需时钟或窗口序号 key。
const FIXED_WINDOW_SCRIPT: &str = "local current = redis.call('INCR', KEYS[1])\n\
     if current == 1 then\n\
       redis.call('PEXPIRE', KEYS[1], ARGV[1])\n\
     end\n\
     local ttl = redis.call('PTTL', KEYS[1])\n\
     return {current, ttl}";

/// Redis 的毫秒 TTL 使用有符号 64 位整数。超出该边界的 `Duration` 不能通过强转截断后发给后端。
const MAX_REDIS_WINDOW_MILLIS: u128 = i64::MAX as u128;
/// 中间件冻结配置的运维硬上限；避免 provider-neutral 配置把超长窗口带入计时器或重试建议。
pub const MAX_DISTRIBUTED_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// 普通 token 继续沿用历史 `{namespace}:{key}`，避免升级时无故重置现有窗口；复杂/超长输入改用定长摘要键。
const MAX_LEGACY_NAMESPACE_BYTES: usize = 128;
const MAX_LEGACY_SUBJECT_BYTES: usize = 512;

/// 将窗口转换为 Redis 正 i64 毫秒范围，拒绝零值和截断。
fn redis_window_millis(window: Duration) -> Option<u64> {
    let millis = window.as_millis();
    if millis == 0 || millis > MAX_REDIS_WINDOW_MILLIS {
        None
    } else {
        u64::try_from(millis).ok()
    }
}

/// 判断输入是否可安全沿用不含分隔符的历史明文 key 片段。
fn legacy_key_segment(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.contains(':')
}

/// 历史简单 key 保持兼容；含分隔符或超长主体使用长度前缀摘要，既消除跨 namespace 拼接碰撞，也给
/// Redis key 长度设置常数上界。摘要不用于认证，只用于无损域分隔。
fn redis_rate_limit_key(namespace: &str, subject: &str) -> String {
    if legacy_key_segment(namespace, MAX_LEGACY_NAMESPACE_BYTES)
        && legacy_key_segment(subject, MAX_LEGACY_SUBJECT_BYTES)
    {
        return format!("{namespace}:{subject}");
    }

    let mut hasher = Sha256::new();
    hasher.update((namespace.len() as u128).to_be_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update((subject.len() as u128).to_be_bytes());
    hasher.update(subject.as_bytes());
    let digest = hasher.finalize();
    let mut key = String::with_capacity("ratelimit:v2:".len() + digest.len() * 2);
    key.push_str("ratelimit:v2:");
    for byte in digest {
        write!(&mut key, "{byte:02x}").expect("writing to String cannot fail");
    }
    key
}

/// Redis 固定窗口计数的分布式配额后端(nadis)。
///
/// 简单 key 沿用 `{namespace}:{key}`；包含分隔符或超长的输入使用域分隔摘要。计数与开窗由
/// `FIXED_WINDOW_SCRIPT` 原子完成,故所有共用同一 Redis 的副本对同一主体受同一上限。
/// **fail-open**:Redis 报错(不可达/超时/脚本错)→ 放行 + `warn`,不背压业务。
pub struct RedisRateLimitProvider {
    /// 受管 Redis 客户端(与业务其余 Redis 用途共用同一连接池)。
    client: Arc<RedisClient>,
    /// key 前缀命名空间(隔离配额 key 与其它业务 key)。
    namespace: String,
}

impl RedisRateLimitProvider {
    /// 用受管 Redis 客户端与命名空间构造分布式配额后端。
    ///
    /// # 参数
    ///
    /// - `client`:[`crate::Application::redis`] 取得的受管客户端。
    /// - `namespace`:配额 key 前缀(如 `"ratelimit"`)。
    pub fn new(client: Arc<RedisClient>, namespace: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl RateLimitProvider for RedisRateLimitProvider {
    /// 对 `{namespace}:{key}` 原子自增计数并按 TTL 判定,Redis 故障时 fail-open。
    ///
    /// # 参数
    ///
    /// - `key`:配额主体标识。
    /// - `limit`:窗口内允许的最大命中数。
    /// - `window`:配额窗口时长。
    async fn check(&self, key: &str, limit: u32, window: Duration) -> RateLimitOutcome {
        let Some(window_ms) = redis_window_millis(window) else {
            tracing::warn!("distributed rate limit received an invalid window, failing open");
            return RateLimitOutcome::allow();
        };
        if limit == 0 {
            tracing::warn!("distributed rate limit received a zero limit, failing open");
            return RateLimitOutcome::allow();
        }
        let full_key = redis_rate_limit_key(&self.namespace, key);
        let window_ms = window_ms.to_string();
        match self
            .client
            .eval::<Vec<i64>>(FIXED_WINDOW_SCRIPT, &[&full_key], &[&window_ms])
            .await
        {
            Ok(values) => {
                let current = values.first().copied().unwrap_or(1);
                let ttl_ms = values.get(1).copied().unwrap_or(-1);
                if current <= i64::from(limit) {
                    RateLimitOutcome::allow()
                } else {
                    // TTL 缺失(-1/-2)时退回整窗时长,避免建议 0 秒。
                    let retry_ms = if ttl_ms > 0 {
                        ttl_ms as u64
                    } else {
                        redis_window_millis(window)
                            .expect("window was validated before the Redis request")
                    };
                    RateLimitOutcome::deny(Duration::from_millis(retry_ms.max(1)))
                }
            }
            Err(error) => {
                tracing::warn!(
                    "distributed rate limit backend error for a subject, failing open: {error}"
                );
                RateLimitOutcome::allow()
            }
        }
    }
}

/// 分布式限流中间件的启动配置错误。
#[cfg(feature = "web")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DistributedRateLimitConfigError {
    /// 窗口上限必须为正。
    #[error("distributed rate limit must be greater than zero")]
    ZeroLimit,
    /// 窗口时长必须为正。
    #[error("distributed rate limit window must be greater than zero")]
    ZeroWindow,
    /// 窗口时长超过框架硬上限。
    #[error("distributed rate limit window must not exceed 365 days")]
    WindowTooLarge,
}

/// 分布式限流中间件的启动期冻结配置:提供方 + 每主体上限 + 窗口。
#[cfg(feature = "web")]
pub struct DistributedRateLimit {
    /// 跨副本配额提供方(如 [`RedisRateLimitProvider`])。
    provider: SharedRateLimitProvider,
    /// 每主体窗口内上限(> 0)。
    limit: u32,
    /// 配额窗口时长。
    window: Duration,
}

#[cfg(feature = "web")]
impl DistributedRateLimit {
    /// 绑定提供方与配额参数,供 [`distributed_rate_limit`] 中间件按 `State` 注入。
    ///
    /// # 参数
    ///
    /// - `provider`:跨副本配额提供方。
    /// - `limit`:每主体窗口内上限(> 0)。
    /// - `window`:配额窗口时长。
    pub fn new(provider: SharedRateLimitProvider, limit: u32, window: Duration) -> Self {
        Self::try_new(provider, limit, window)
            .expect("distributed rate limit configuration must be valid")
    }

    /// 校验并绑定提供方与配额参数；适合把外部配置错误转换为启动失败，而不是等第一条请求才暴露。
    pub fn try_new(
        provider: SharedRateLimitProvider,
        limit: u32,
        window: Duration,
    ) -> Result<Self, DistributedRateLimitConfigError> {
        if limit == 0 {
            return Err(DistributedRateLimitConfigError::ZeroLimit);
        }
        if window.is_zero() {
            return Err(DistributedRateLimitConfigError::ZeroWindow);
        }
        if window > MAX_DISTRIBUTED_RATE_LIMIT_WINDOW {
            return Err(DistributedRateLimitConfigError::WindowTooLarge);
        }
        Ok(Self {
            provider,
            limit,
            window,
        })
    }
}

/// 跨副本分布式限流中间件:按真实客户端 IP 作配额主体,经共享后端在**所有副本间合并计量**,
/// 超额即 429 + `Retry-After`。
///
/// 与单实例每客户端 `governance::rate_limit` 分工:那层护本进程、各副本独立;本层把同一 IP 的总量
/// 在所有副本间合并到一个上限(扩容不放大总配额)。装配位置同 `rate_limit`——在 `resolve_client_ip` 之内
/// (依赖其写入的 [`crate::ClientIp`]);无 `ClientIp` 时保守放行(不误杀)。业务若要按**租户/subject** 而非
/// IP 计量,直接用 [`RateLimitProvider::check`] 自定 key,不经本 IP 中间件。
///
/// # 参数
///
/// - `config`:启动期冻结的提供方 + 配额参数。
/// - `request`:入站请求(从扩展读已解析客户端 IP)。
/// - `next`:下游放行句柄。
#[cfg(feature = "web")]
pub async fn distributed_rate_limit(
    axum::extract::State(config): axum::extract::State<Arc<DistributedRateLimit>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(client) = request
        .extensions()
        .get::<crate::ClientIp>()
        .map(crate::ClientIp::ip)
    else {
        return next.run(request).await;
    };
    let outcome = config
        .provider
        .check(&client.to_string(), config.limit, config.window)
        .await;
    if outcome.allowed {
        next.run(request).await
    } else {
        // Retry-After 以秒计,向上取整到整秒且至少 1(不建议 0 秒)。
        let retry_after = outcome
            .retry_after
            .map(|wait| {
                wait.as_secs()
                    .saturating_add(u64::from(wait.subsec_nanos() > 0))
            })
            .unwrap_or(1)
            .max(1);
        (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::RETRY_AFTER, retry_after.to_string())],
            "rate limit exceeded",
        )
            .into_response()
    }
}
