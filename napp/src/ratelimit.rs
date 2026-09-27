//! 跨副本分布式业务配额:`RateLimitProvider` 抽象 + nadis Redis 固定窗口后端。
//!
//! 与单实例每客户端令牌桶(`governance` 的 `RateLimit`)是**两层、正确性合同不同**:后者只护本
//! 进程(每副本各自一套桶);本层用**共享 Redis 原子计数**把「租户 / 主体 / API-key 总配额」在所有副本间
//! **合并计量**——N 个副本共用同一 key 即受同一上限,不会因扩容而放大总量。两者可叠加:先本进程护栏,
//! 再跨副本总配额。
//!
//! 后端故障策略:可配置 [`RateLimitFailurePolicy`]。缺省 **fail-open**(可用性优先,治理层自身基础设施
//! 抖动不该把业务流量打死),并 `warn`;高保障路径可在构造时选 **fail-closed**——后端不可达、窗口非法或
//! 零上限一律拒绝,把"配额层失效"当作放行禁令而不是放行理由。两种策略的命中都进入进程级计数,
//! 供部署核对真实生效的策略与后端健康。
//!
//! Redis 实例选择:构造方必须经 [`crate::Application::redis`] 按 qualifier 显式取得客户端,本层不提供
//! 默认实例装配,也不猜测唯一实例——配额是跨副本共享状态,连错实例等于各副本各算各的。

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
    /// 业务作用：放行结果(无重试建议)。
    pub fn allow() -> Self {
        Self {
            allowed: true,
            retry_after: None,
        }
    }

    /// 业务作用：拒绝结果 + 建议重试等待(补足窗口所需时长)。
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
    /// 业务作用：记一次命中并判定 `key` 是否在 `window` 内超过 `limit`。
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

/// 业务作用：将窗口转换为 Redis 正 i64 毫秒范围，拒绝零值和截断。
fn redis_window_millis(window: Duration) -> Option<u64> {
    let millis = window.as_millis();
    if millis == 0 || millis > MAX_REDIS_WINDOW_MILLIS {
        None
    } else {
        u64::try_from(millis).ok()
    }
}

/// 业务作用：判断输入是否可安全沿用不含分隔符的历史明文 key 片段。
fn legacy_key_segment(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.contains(':')
}

/// 业务作用：历史简单 key 保持兼容；含分隔符或超长主体使用长度前缀摘要，既消除跨 namespace 拼接碰撞，也给
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

/// 配额后端失效(不可达/超时/非法参数)时的裁决策略。
///
/// 缺省 `Open` 保持兼容放行语义；`Closed` 供高保障路径把"无法计量"视同"超额"——配额层的目的若是
/// 防资损或防滥用,后端失效期照常放行等于该保护在最需要时消失。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RateLimitFailurePolicy {
    /// 后端失效即放行并告警(可用性优先,兼容缺省)。
    #[default]
    Open,
    /// 后端失效即拒绝(保护优先);拒绝携带保守的整窗 Retry-After。
    Closed,
}

impl RateLimitFailurePolicy {
    /// 业务作用：把配置文本解析为失败策略，供 YAML 与代码共用同一稳定词表。
    ///
    /// # 参数
    ///
    /// - `value`: 小写策略文本，允许 `open`、`closed`。
    ///
    /// # 返回
    ///
    /// 合法文本返回对应策略；未知文本返回 `None`，由调用方给出定位明确的配置错误。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }

    /// 业务作用：返回策略的稳定小写文本，用于日志与低基数标签。
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// `open` / `closed`。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }
}

/// 进程级配额观测计数：部署据此核对策略真实生效面与后端健康。
static RATE_LIMIT_ALLOWED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RATE_LIMIT_DENIED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RATE_LIMIT_BACKEND_ERRORS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RATE_LIMIT_INVALID_CONFIG: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RATE_LIMIT_MISSING_SUBJECT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// 业务作用：读取分布式配额五类累计计数，顺序为 allowed、denied、backend_error、invalid_config、
/// missing_subject；供运行核对与指标投影。
///
/// 参数说明: 无。
///
/// # 返回
///
/// 进程启动以来的累计五元组。
pub fn rate_limit_counters() -> (u64, u64, u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (
        RATE_LIMIT_ALLOWED.load(Relaxed),
        RATE_LIMIT_DENIED.load(Relaxed),
        RATE_LIMIT_BACKEND_ERRORS.load(Relaxed),
        RATE_LIMIT_INVALID_CONFIG.load(Relaxed),
        RATE_LIMIT_MISSING_SUBJECT.load(Relaxed),
    )
}

static RATE_LIMIT_EVENTS_DESCRIPTOR: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_rate_limit_events_total",
        help: "跨副本分布式配额事件累计数;event 为封闭枚举 allowed/denied/backend_error/invalid_config/missing_subject。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &["event"],
        histogram_bounds: &[],
    };
static RATE_LIMIT_METRIC_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 1] =
    [&RATE_LIMIT_EVENTS_DESCRIPTOR];
/// 配额事件的封闭 label 词表;顺序与 [`rate_limit_counters`] 五元组一致。
const RATE_LIMIT_EVENT_LABELS: [&str; 5] = [
    "allowed",
    "denied",
    "backend_error",
    "invalid_config",
    "missing_subject",
];
/// 配额观测的最坏序列数:单 family、五个封闭 event 值,启动期一次性预留。
pub(crate) const RATE_LIMIT_METRIC_SERIES: usize = RATE_LIMIT_EVENT_LABELS.len();

/// 分布式配额观测源:把五类进程级事件计数投影为单 family 封闭 label 序列。
///
/// 值与 [`rate_limit_counters`] 同源;event 词表编译期冻结,业务无法注入新 label 值,
/// 序列数恒为五条,零流量时保持全零可见(部署据此确认策略与主体来源已生效)。
pub(crate) struct RateLimitMetricsSource;

impl nametrics_core::LegacyMetricsSource for RateLimitMetricsSource {
    /// 业务作用：返回分布式配额观测的固定 family 目录，供启动期冲突与容量审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅含 event 单 label 的一个 counter family。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &RATE_LIMIT_METRIC_DESCRIPTORS
    }

    /// 业务作用：把五类配额事件计数投影为恒定五条样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：event 词表全量样本；未发生事件的项保持零。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let (allowed, denied, backend_error, invalid_config, missing_subject) =
            rate_limit_counters();
        let values = [
            allowed,
            denied,
            backend_error,
            invalid_config,
            missing_subject,
        ];
        Some(
            RATE_LIMIT_EVENT_LABELS
                .iter()
                .zip(values)
                .map(|(event, value)| nametrics_core::MetricSample {
                    name: RATE_LIMIT_EVENTS_DESCRIPTOR.name,
                    labels: vec![("event", (*event).to_owned())],
                    value: nametrics_core::MetricValue::Counter(value),
                })
                .collect(),
        )
    }

    /// 业务作用：结构化快照已覆盖全部序列，文本渲染统一由 hub 完成，本源不自渲染。
    ///
    /// 参数说明：
    /// - `_output`: 未使用的文本缓冲区。
    ///
    /// 返回：无。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// Redis 固定窗口计数的分布式配额后端(nadis)。
///
/// 简单 key 沿用 `{namespace}:{key}`；包含分隔符或超长的输入使用域分隔摘要。计数与开窗由
/// `FIXED_WINDOW_SCRIPT` 原子完成,故所有共用同一 Redis 的副本对同一主体受同一上限。
/// 失效裁决按构造时冻结的 [`RateLimitFailurePolicy`] 执行,缺省 fail-open。
pub struct RedisRateLimitProvider {
    /// 受管 Redis 客户端(与业务其余 Redis 用途共用同一连接池)。
    client: Arc<RedisClient>,
    /// key 前缀命名空间(隔离配额 key 与其它业务 key)。
    namespace: String,
    /// 后端失效时的裁决策略,构造期冻结。
    failure_policy: RateLimitFailurePolicy,
}

impl RedisRateLimitProvider {
    /// 业务作用：用受管 Redis 客户端与命名空间构造分布式配额后端。
    ///
    /// # 参数
    ///
    /// - `client`:[`crate::Application::redis`] 按 qualifier 显式取得的受管客户端。
    /// - `namespace`:配额 key 前缀(如 `"ratelimit"`)。
    ///
    /// 返回：冻结命名空间且后端失效缺省为 Open 的分布式配额实现。
    pub fn new(client: Arc<RedisClient>, namespace: impl Into<String>) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            failure_policy: RateLimitFailurePolicy::Open,
        }
    }

    /// 业务作用：冻结后端失效裁决策略；高保障路径以 `Closed` 把配额层失效转为拒绝而不是放行。
    ///
    /// # 参数
    ///
    /// - `policy`: 失效裁决策略。
    ///
    /// # 返回
    ///
    /// 更新后的后端。
    pub fn with_failure_policy(mut self, policy: RateLimitFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// 业务作用：把一次失效按冻结策略折算为放行或保守拒绝，并归入对应计数。
    ///
    /// # 参数
    ///
    /// - `retry_hint`: `Closed` 拒绝时建议的重试等待；无可信窗口时给最小正值。
    ///
    /// # 返回
    ///
    /// `Open` 返回放行；`Closed` 返回带 Retry-After 的拒绝。
    fn failure_outcome(&self, retry_hint: Duration) -> RateLimitOutcome {
        match self.failure_policy {
            RateLimitFailurePolicy::Open => RateLimitOutcome::allow(),
            RateLimitFailurePolicy::Closed => {
                RateLimitOutcome::deny(retry_hint.max(Duration::from_millis(1)))
            }
        }
    }
}

#[async_trait]
impl RateLimitProvider for RedisRateLimitProvider {
    /// 业务作用：对 `{namespace}:{key}` 原子自增计数并按 TTL 判定,失效按冻结策略裁决。
    ///
    /// # 参数
    ///
    /// - `key`:配额主体标识。
    /// - `limit`:窗口内允许的最大命中数。
    /// - `window`:配额窗口时长。
    ///
    /// 返回：计数未超额时放行，超额时返回窗口剩余等待；后端或参数异常按冻结失效策略裁决。
    async fn check(&self, key: &str, limit: u32, window: Duration) -> RateLimitOutcome {
        use std::sync::atomic::Ordering::Relaxed;
        let policy = self.failure_policy.as_str();
        // 非法参数属部署错误而非流量事实:计入 invalid_config 并按失效策略裁决,
        // fail-closed 下部署错误立即以拒绝显形,而不是静默放行到事后审计才发现。
        let Some(window_ms) = redis_window_millis(window) else {
            RATE_LIMIT_INVALID_CONFIG.fetch_add(1, Relaxed);
            tracing::warn!(
                failure_policy = policy,
                "distributed rate limit received an invalid window"
            );
            return self.failure_outcome(Duration::from_secs(1));
        };
        if limit == 0 {
            RATE_LIMIT_INVALID_CONFIG.fetch_add(1, Relaxed);
            tracing::warn!(
                failure_policy = policy,
                "distributed rate limit received a zero limit"
            );
            return self.failure_outcome(window);
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
                RATE_LIMIT_BACKEND_ERRORS.fetch_add(1, Relaxed);
                tracing::warn!(
                    failure_policy = policy,
                    "distributed rate limit backend error for a subject: {error}"
                );
                // Closed 用整窗作保守 Retry-After:后端恢复前重试大概率仍失败,提示过短只会放大压力。
                self.failure_outcome(window)
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

/// 中间件的配额主体来源(类型化枚举,禁止任意运行期函数以保证可审计)。
///
/// 计量口径:IP 与已认证主体标识按原文计量(非秘密,保留 Redis key 的可读性,超长或含分隔符时
/// 由后端 key 派生统一摘要);header 值属凭证形态(API key),在离开中间件前即做域分隔摘要,
/// 原文既不进 provider 也不落 Redis key 或日志。
#[cfg(feature = "web")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QuotaSubject {
    /// 真实客户端 IP(依赖 `resolve_client_ip` 写入的 [`crate::ClientIp`],其可信代理链有独立启动审计)。
    ClientIp,
    /// 已认证主体身份(认证中间件写入的 `Principal`);匿名请求视为主体缺失。
    Principal,
    /// 已验证身份中的租户标识；没有 tenant claim 的请求视为主体缺失。
    Tenant,
    /// 指定请求 header 的唯一值(如 API key),以域分隔摘要计量；缺失、非法或重复 header
    /// 视为主体缺失，避免配额层与后续业务对多值 header 选择不同身份。
    Header(&'static str),
}

/// 主体缺失(拿不到 IP/身份/header)时的裁决。
///
/// 缺省放行保持兼容语义；`Deny` 供"配额必须可归因"的路径把不可归因请求直接拒绝——
/// 主体缺失往往意味着代理链或认证装配错误,放行等于对这类流量完全不设限。
#[cfg(feature = "web")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissingSubjectPolicy {
    /// 主体缺失即放行并计数(兼容缺省)。
    #[default]
    Allow,
    /// 主体缺失即 403 拒绝并计数。
    Deny,
}

/// 分布式限流中间件的启动期冻结配置:提供方 + 每主体上限 + 窗口 + 主体来源与缺失裁决。
#[cfg(feature = "web")]
pub struct DistributedRateLimit {
    /// 跨副本配额提供方(如 [`RedisRateLimitProvider`])。
    provider: SharedRateLimitProvider,
    /// 每主体窗口内上限(> 0)。
    limit: u32,
    /// 配额窗口时长。
    window: Duration,
    /// 配额主体来源,构造期冻结。
    subject: QuotaSubject,
    /// 主体缺失时的裁决,构造期冻结。
    missing_subject: MissingSubjectPolicy,
}

#[cfg(feature = "web")]
impl DistributedRateLimit {
    /// 业务作用：绑定提供方与配额参数,供 [`distributed_rate_limit`] 中间件按 `State` 注入。
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

    /// 业务作用：校验并绑定提供方与配额参数；适合把外部配置错误转换为启动失败，而不是等第一条请求才暴露。
    ///
    /// 参数说明：
    /// - `provider`：跨副本共享的配额裁决提供方。
    /// - `limit`：单主体在一个窗口内的正整数上限。
    /// - `window`：非零且不超过硬上限的计量窗口。
    ///
    /// 返回：参数合法时返回冻结配置；零上限、零窗口或超大窗口返回启动配置错误。
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
            subject: QuotaSubject::ClientIp,
            missing_subject: MissingSubjectPolicy::Allow,
        })
    }

    /// 业务作用：冻结配额主体来源；按租户/subject/API key 计量时替换缺省的客户端 IP。
    ///
    /// # 参数
    ///
    /// - `subject`: 类型化主体来源。
    ///
    /// # 返回
    ///
    /// 更新后的配置。
    pub fn with_subject(mut self, subject: QuotaSubject) -> Self {
        self.subject = subject;
        self
    }

    /// 业务作用：冻结主体缺失裁决；要求配额必须可归因的路径以 `Deny` 拒绝不可归因流量。
    ///
    /// # 参数
    ///
    /// - `policy`: 主体缺失时的裁决。
    ///
    /// # 返回
    ///
    /// 更新后的配置。
    pub fn with_missing_subject_policy(mut self, policy: MissingSubjectPolicy) -> Self {
        self.missing_subject = policy;
        self
    }

    /// 业务作用：按冻结的主体来源从请求解析配额主体。
    ///
    /// # 参数
    ///
    /// - `request`: 入站请求(读扩展与 header,不读 body)。
    ///
    /// # 返回
    ///
    /// 解析成功返回主体文本(header 来源为域分隔摘要)；来源缺失(无 ClientIp/匿名/缺头/
    /// 非 UTF-8 头)返回 `None`。
    fn resolve_subject(&self, request: &axum::extract::Request) -> Option<String> {
        match self.subject {
            QuotaSubject::ClientIp => request
                .extensions()
                .get::<crate::ClientIp>()
                .map(|client| client.ip().to_string()),
            QuotaSubject::Principal => request
                .extensions()
                .get::<naauthz::Principal>()
                .and_then(|principal| principal.authenticated_identity().map(str::to_owned)),
            QuotaSubject::Tenant => request
                .extensions()
                .get::<naauthz::Principal>()
                .and_then(|principal| principal.tenant.as_deref())
                .filter(|tenant| !tenant.trim().is_empty())
                .map(str::to_owned),
            // header 值属凭证形态:只接受唯一值并立即摘要。多值输入若在此取首值、业务层取末值，
            // 会让被计量主体与实际主体分裂；因此重复值必须进入主体缺失裁决。header 名先经
            // HeaderName 规范化，大小写不同的等价配置在滚动实例间仍共享同一配额域。
            QuotaSubject::Header(name) => {
                let header_name = axum::http::HeaderName::from_bytes(name.as_bytes()).ok()?;
                let mut values = request.headers().get_all(&header_name).iter();
                match (values.next(), values.next()) {
                    (Some(value), None) => value
                        .to_str()
                        .ok()
                        .filter(|value| !value.is_empty())
                        .map(|value| digest_header_subject(header_name.as_str(), value)),
                    _ => None,
                }
            }
        }
    }
}

/// 业务作用：把 header 来源的配额主体折算为域分隔摘要，使凭证原文不离开中间件。
///
/// # 参数
///
/// - `name`: header 名，参与域分隔——同一值出现在不同 header 下不共享配额。
/// - `value`: header 原文值。
///
/// # 返回
///
/// 64 个十六进制字符的摘要文本；确定性映射保证同一凭证在所有副本共享同一配额 key。
#[cfg(feature = "web")]
fn digest_header_subject(name: &str, value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"napp:quota:header");
    hasher.update((name.len() as u128).to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update((value.len() as u128).to_be_bytes());
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    let mut subject = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut subject, "{byte:02x}").expect("writing to String cannot fail");
    }
    subject
}

/// 业务作用：跨副本分布式限流中间件:按冻结的 IP、租户、认证主体或 API key 来源解析配额主体，
/// 经共享后端在**所有副本间合并计量**，超额即 429 + `Retry-After`。
///
/// 与单实例每客户端 `governance::rate_limit` 分工:那层护本进程、各副本独立;本层把同一 IP 的总量
/// 在所有副本间合并到一个上限(扩容不放大总配额)。IP 来源依赖外层 `resolve_client_ip`；租户与
/// 认证主体来源依赖 authentication 写入的 `Principal`；header 来源在进入 provider 前完成摘要。
/// 任一来源缺失时按冻结的 [`MissingSubjectPolicy`] 裁决，不会回退到另一种主体。
///
/// # 参数
///
/// - `config`:启动期冻结的提供方 + 配额参数。
/// - `request`:入站请求(只读取 header 与已验证扩展，不读取 body)。
/// - `next`:下游放行句柄。
///
/// 返回：主体缺失或超额时按冻结策略返回 403/429；允许时返回下游响应。
#[cfg(feature = "web")]
pub async fn distributed_rate_limit(
    axum::extract::State(config): axum::extract::State<Arc<DistributedRateLimit>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(subject) = config.resolve_subject(&request) else {
        // 主体缺失多为装配错误(代理链未解析 IP/认证层未写主体/客户端缺头):计数暴露真实规模,
        // 裁决按冻结策略——Deny 把"不可归因即不设限"的旁路直接封死。
        RATE_LIMIT_MISSING_SUBJECT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return match config.missing_subject {
            MissingSubjectPolicy::Allow => {
                // missing_subject 是原因计数，allowed 是本次请求的最终配额结局；两者同时记录，
                // 才能让五类事件在主体缺失分支仍保持统一口径。
                RATE_LIMIT_ALLOWED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                next.run(request).await
            }
            MissingSubjectPolicy::Deny => {
                RATE_LIMIT_DENIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (
                    axum::http::StatusCode::FORBIDDEN,
                    "quota subject unavailable for this request",
                )
                    .into_response()
            }
        };
    };
    let outcome = config
        .provider
        .check(&subject, config.limit, config.window)
        .await;
    if outcome.allowed {
        // 中间件统一记录最终 allow/deny，故所有 provider 经本入口都有相同的请求结局口径；
        // backend_error/invalid_config 只由内置 Redis provider 记录其可判定的原因。
        RATE_LIMIT_ALLOWED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        next.run(request).await
    } else {
        RATE_LIMIT_DENIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
