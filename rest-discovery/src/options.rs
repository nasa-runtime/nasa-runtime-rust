//! RestDiscovery 运行选项。
//!
//! 时长字段用 [`std::time::Duration`],与最近邻 `nadisc::WatchOptions` / `nacos::SubscribeOptions` 一致
//! (它们都用 Duration,不用 u64 ms)。所有 struct `#[non_exhaustive]` + `Default` + builder,字段增减不破坏下游。

use std::time::Duration;

use natelemetry::SpanRecorder;

use crate::error::{RestDiscoveryError, Result};

/// 所有运行时 timeout、轮询和缓存窗口的统一硬上限。
///
/// 这些值会进入 `tokio::time` 或与 `Instant` 相加；允许接近 `Duration::MAX` 的配置既没有运维意义，
/// 也会在不同平台上变成 panic。365 天保留长周期服务的余量，同时给所有 deadline 运算确定边界。
const MAX_RUNTIME_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 服务名匹配大小写策略(仅档3启发式索引用;第三阶段才生效)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServiceMatchMode {
    /// 忽略大小写(按 ASCII lowercase 归一化)。
    #[default]
    CaseInsensitive,
    /// 大小写敏感。
    CaseSensitive,
}

/// 裸 http(s) 启发式识别开关。第一阶段默认 `Disabled`:裸 http(s) 全部当外部直连。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HeuristicHttpMode {
    /// 不维护服务名索引;裸 http(s) 全部普通 HTTP。
    #[default]
    Disabled,
    /// 维护服务名索引;裸 http(s) host 命中索引才走 LB(第三阶段实现)。
    Enabled,
}

/// 裸 http(s) URL 入口的 scheme 策略(`service_request`/`lb://` 用 `default_instance_scheme`,不看本项)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemePolicy {
    /// 原 URL 是 http 就请求实例 http,https 就 https。
    #[default]
    Preserve,
    /// 无论原始 URL 是否为 https,都用 http 访问目标实例。
    ForceHttp,
    /// 无论原始 URL 是否为 http,都用 https 访问目标实例。
    ForceHttps,
}

/// `service_request`/`lb://` 没有原始 scheme 时,连接实例用的默认协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InstanceScheme {
    /// 默认用 http 访问服务实例。
    #[default]
    Http,
    /// 默认用 https 访问服务实例。
    Https,
}

impl InstanceScheme {
    /// URL scheme 字面量。
    pub fn as_str(self) -> &'static str {
        match self {
            InstanceScheme::Http => "http",
            InstanceScheme::Https => "https",
        }
    }
}

/// 裸 http(s) host 不在服务名索引时的策略(第三阶段才生效)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnknownHostPolicy {
    /// 直接走普通 HTTP 外部调用。
    #[default]
    ExternalHttp,
    /// 返回 `UnknownServiceHost` 错误。
    Error,
}

/// 负载均衡策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LbStrategy {
    /// round-robin(默认;每服务计数器随机起种)。
    #[default]
    RoundRobin,
    /// 平滑加权 round-robin(按 `Instance.weight` 比例分流)。
    Weighted,
}

/// host 已确认是内部服务但无可用实例时的策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NoInstancePolicy {
    /// 直接返回 `NoAvailableInstance`(默认;权威空列表不 fallback)。
    #[default]
    Error,
    /// 发现调用出错时允许短暂使用 stale 实例(仅出错,不含成功空列表)。
    StaleIfDiscoveryError,
}

/// 启动期是否要求初始 list_services 成功(仅启发式裸 http(s) 模式相关)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartupPolicy {
    /// 仅在 `heuristic_http=Enabled` 时要求初始 list_services 成功。
    #[default]
    RequireInitialServiceListWhenHeuristicEnabled,
    /// 允许空服务名索引启动,后续由周期刷新补齐索引。
    AllowEmptyServiceIndex,
}

/// watch / 实例缓存调优项。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RestWatchOptions {
    /// 传给后端 watch 的轮询兜底间隔(调小换「删到空」的新鲜度)。
    pub poll_interval: Duration,
    /// watch 失败降级时,discover 结果的 TTL。
    pub ttl_fallback: Duration,
    /// discover **出错**时允许沿用旧实例的窗口(不用于成功空列表)。
    pub stale_if_error: Duration,
    /// watch pump 退出后重建的最小退避。
    pub restart_backoff_min: Duration,
    /// watch pump 退出后重建的最大退避。
    pub restart_backoff_max: Duration,
}

impl Default for RestWatchOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            ttl_fallback: Duration::from_secs(3),
            stale_if_error: Duration::from_secs(10),
            restart_backoff_min: Duration::from_secs(1),
            restart_backoff_max: Duration::from_secs(30),
        }
    }
}

impl RestWatchOptions {
    /// 用默认 watch 参数创建 builder 起点。
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置后端 watch 的轮询兜底间隔。
    ///
    /// # 参数
    ///
    /// - `d`: watch 后端用于校正实例快照的轮询兜底间隔。
    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// 设置 watch 降级时 discover 快照的 TTL。
    ///
    /// # 参数
    ///
    /// - `d`: watch 不可用时主动 discover 结果可复用的时长。
    pub fn with_ttl_fallback(mut self, d: Duration) -> Self {
        self.ttl_fallback = d;
        self
    }

    /// 设置 discover 出错时允许沿用旧实例的窗口。
    ///
    /// # 参数
    ///
    /// - `d`: discovery 调用失败时允许继续使用旧实例快照的时间窗口。
    pub fn with_stale_if_error(mut self, d: Duration) -> Self {
        self.stale_if_error = d;
        self
    }

    /// 设置 watch pump 重建时的退避范围。
    ///
    /// # 参数
    ///
    /// - `min`: watch pump 首次重建或失败后重试的最小退避。
    /// - `max`: watch pump 连续失败后允许增长到的最大退避。
    pub fn with_restart_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.restart_backoff_min = min;
        self.restart_backoff_max = max;
        self
    }
}

/// 裸 http(s) 启发式索引(`ServiceNameIndex`)调优项。仅 `heuristic_http=Enabled` 时生效。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RestHeuristicOptions {
    /// 周期刷新服务名列表的间隔。
    pub refresh_interval: Duration,
    /// 服务从列表消失后,索引保留的 grace(超过才移除;容忍 list_services 瞬时不一致)。
    pub removed_service_grace: Duration,
}

impl Default for RestHeuristicOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
            removed_service_grace: Duration::from_secs(60),
        }
    }
}

impl RestHeuristicOptions {
    /// 用默认启发式索引参数创建 builder 起点。
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置服务名列表周期刷新间隔。
    ///
    /// # 参数
    ///
    /// - `d`: 裸 http(s) 启发式服务名索引的周期刷新间隔。
    pub fn with_refresh_interval(mut self, d: Duration) -> Self {
        self.refresh_interval = d;
        self
    }

    /// 设置服务名消失后的保留窗口。
    ///
    /// # 参数
    ///
    /// - `d`: 服务名从列表消失后继续保留在索引中的宽限时间。
    pub fn with_removed_service_grace(mut self, d: Duration) -> Self {
        self.removed_service_grace = d;
        self
    }
}

/// 底层 reqwest HTTP client 超时。远程调用默认必须有超时兜底,避免请求长期挂起。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RestHttpOptions {
    /// 单请求总超时(连接 + 发送 + 接收完整响应)。
    pub timeout: Duration,
    /// 连接建立超时。
    pub connect_timeout: Duration,
}

impl Default for RestHttpOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
        }
    }
}

impl RestHttpOptions {
    /// 用默认 HTTP 超时参数创建 builder 起点。
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置单请求总超时。
    ///
    /// # 参数
    ///
    /// - `d`: HTTP 请求从连接、发送到完整接收响应的总超时。
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// 设置连接建立超时。
    ///
    /// # 参数
    ///
    /// - `d`: 建立 TCP/TLS 连接允许消耗的最长时间。
    pub fn with_connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = d;
        self
    }
}

/// 跨实例重试策略(第一版默认全关;第四阶段再扩)。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RetryOptions {
    /// GET/HEAD 在传输错误上重试下一个实例。
    pub retry_get_head_on_transport_error: bool,
    /// GET/HEAD 在 429/502/503/504 响应上按 `Retry-After` 重试下一个实例。
    pub retry_get_head_on_retryable_status: bool,
    /// 最大尝试次数(含首次)。
    pub max_attempts: usize,
    /// 每服务 retry token bucket 容量；每个额外 attempt 消耗 1。
    pub budget_capacity: u32,
    /// 每服务每秒恢复的 retry token 数。
    pub budget_refill_per_second: f64,
}

impl Default for RetryOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            retry_get_head_on_transport_error: false,
            retry_get_head_on_retryable_status: false,
            max_attempts: 1,
            budget_capacity: 20,
            budget_refill_per_second: 10.0,
        }
    }
}

impl RetryOptions {
    /// 默认重试配置:不跨实例重试,只尝试一次。
    pub fn new() -> Self {
        Self::default()
    }

    /// 开启 GET/HEAD 传输错误重试,并设置最大尝试次数(含首次;`>=2` 才真正重试)。
    ///
    /// # 参数
    ///
    /// - `max_attempts`: 最大尝试次数，包含首次请求。
    pub fn get_head_on_transport_error(max_attempts: usize) -> Self {
        Self {
            retry_get_head_on_transport_error: true,
            retry_get_head_on_retryable_status: false,
            max_attempts: max_attempts.max(1),
            ..Self::default()
        }
    }

    /// 开启 GET/HEAD 的传输错误与瞬态 HTTP 状态重试，并配置有界 retry token bucket。
    pub fn get_head_on_transient_failure(
        max_attempts: usize,
        budget_capacity: u32,
        budget_refill_per_second: f64,
    ) -> Self {
        Self {
            retry_get_head_on_transport_error: true,
            retry_get_head_on_retryable_status: true,
            max_attempts: max_attempts.max(1),
            budget_capacity,
            budget_refill_per_second,
        }
    }
}

/// 每服务/实例的运行期隔离与故障抑制策略。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RestResilienceOptions {
    /// 每服务同时执行的逻辑请求上限；超额立即拒绝，不在进程内再堆一条无界等待队列。
    pub max_concurrent_per_service: usize,
    /// 连续多少个逻辑请求失败后打开服务熔断器。
    pub circuit_failure_threshold: u32,
    /// 熔断打开后等待多久再允许一个 half-open 探针。
    pub circuit_open_duration: Duration,
    /// 单实例连续失败多少次后临时从负载均衡候选中摘除。
    pub outlier_failure_threshold: u32,
    /// 单实例异常摘除时长。
    pub outlier_ejection_duration: Duration,
}

impl Default for RestResilienceOptions {
    /// 提供有界并发和保守故障窗口；所有值仍会在 client 构造时统一校验。
    fn default() -> Self {
        Self {
            max_concurrent_per_service: 256,
            circuit_failure_threshold: 5,
            circuit_open_duration: Duration::from_secs(30),
            outlier_failure_threshold: 3,
            outlier_ejection_duration: Duration::from_secs(30),
        }
    }
}

impl RestResilienceOptions {
    /// 设置每服务 bulkhead 并发上限。
    pub fn with_max_concurrent_per_service(mut self, limit: usize) -> Self {
        self.max_concurrent_per_service = limit;
        self
    }

    /// 设置服务级熔断阈值与打开窗口。
    pub fn with_circuit(mut self, failure_threshold: u32, open_duration: Duration) -> Self {
        self.circuit_failure_threshold = failure_threshold;
        self.circuit_open_duration = open_duration;
        self
    }

    /// 设置实例级异常摘除阈值与窗口。
    pub fn with_outlier_ejection(mut self, failure_threshold: u32, duration: Duration) -> Self {
        self.outlier_failure_threshold = failure_threshold;
        self.outlier_ejection_duration = duration;
        self
    }
}

/// RestDiscoveryClient 的全部运行选项。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RestDiscoveryOptions {
    /// 服务名索引匹配时的大小写策略。
    pub service_match: ServiceMatchMode,
    /// 裸 http(s) host 是否尝试命中服务名索引。
    pub heuristic_http: HeuristicHttpMode,
    /// 裸 http(s) 命中内部服务后如何处理原始 scheme。
    pub scheme_policy: SchemePolicy,
    /// `service_request` / `lb://` 这类无原始 scheme 的内部调用默认协议。
    pub default_instance_scheme: InstanceScheme,
    /// 裸 http(s) host 未命中服务名索引时的处理策略。
    pub unknown_host: UnknownHostPolicy,
    /// 已确认内部服务但无可用实例时的处理策略。
    pub no_instance: NoInstancePolicy,
    /// 内置负载均衡策略;注入自定义 LB 时此字段会被忽略。
    pub lb_strategy: LbStrategy,
    /// 启发式索引模式下启动期是否要求初始服务名列表成功。
    pub startup: StartupPolicy,
    /// watch、TTL、降级窗口与重建退避参数。
    pub watch: RestWatchOptions,
    /// 裸 http(s) 启发式索引调优(仅 `heuristic_http=Enabled` 生效)。
    pub heuristic: RestHeuristicOptions,
    /// GET/HEAD 等跨实例重试策略。
    pub retry: RetryOptions,
    /// 每服务 bulkhead/熔断与实例异常摘除策略。
    pub resilience: RestResilienceOptions,
    /// 可选的 Application-owned span 只写入口。配置后每个带 trace context 的请求自动产 client span。
    pub span_recorder: Option<SpanRecorder>,
    /// 底层 reqwest client 超时(默认 timeout 10s / connect 2s)。
    pub http: RestHttpOptions,
    /// 默认 false:让 reqwest 自动生成 `Host: ip:port`;开启才保留原始 host(只允许 RestDiscovery 内部设置)。
    pub preserve_original_host_header: bool,
}

impl Default for RestDiscoveryOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            service_match: ServiceMatchMode::CaseInsensitive,
            heuristic_http: HeuristicHttpMode::Disabled,
            scheme_policy: SchemePolicy::Preserve,
            default_instance_scheme: InstanceScheme::Http,
            unknown_host: UnknownHostPolicy::ExternalHttp,
            no_instance: NoInstancePolicy::Error,
            lb_strategy: LbStrategy::RoundRobin,
            startup: StartupPolicy::RequireInitialServiceListWhenHeuristicEnabled,
            watch: RestWatchOptions::default(),
            heuristic: RestHeuristicOptions::default(),
            retry: RetryOptions::default(),
            resilience: RestResilienceOptions::default(),
            span_recorder: None,
            http: RestHttpOptions::default(),
            preserve_original_host_header: false,
        }
    }
}

impl RestDiscoveryOptions {
    /// 用默认运行选项创建 builder 起点。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注入只写遥测记录器；不会把 exporter 生命周期或 flush 权限交给 REST client。
    pub fn with_span_recorder(mut self, recorder: SpanRecorder) -> Self {
        self.span_recorder = Some(recorder);
        self
    }

    /// 连接实例的默认协议(`service_request`/`lb://` 用)。
    ///
    /// # 参数
    ///
    /// - `scheme`: 显式内部调用没有原始 scheme 时使用的实例访问协议。
    pub fn with_default_instance_scheme(mut self, scheme: InstanceScheme) -> Self {
        self.default_instance_scheme = scheme;
        self
    }

    /// 开/关裸 http(s) 启发式识别。开启后由 `RestDiscovery::init_with_discovery` / `RestDiscoveryClient::connect`
    /// 同步首拉一次 `list_services` 并周期刷新 `ServiceNameIndex`。
    ///
    /// # 参数
    ///
    /// - `mode`: 是否启用裸 http(s) host 命中内部服务名索引的识别逻辑。
    pub fn with_heuristic_http(mut self, mode: HeuristicHttpMode) -> Self {
        self.heuristic_http = mode;
        self
    }

    /// 裸 http(s) 启发式索引调优(刷新间隔 / 移除 grace)。
    ///
    /// # 参数
    ///
    /// - `heuristic`: 服务名索引刷新和移除宽限配置。
    pub fn with_heuristic_options(mut self, heuristic: RestHeuristicOptions) -> Self {
        self.heuristic = heuristic;
        self
    }

    /// 服务名匹配大小写策略。
    ///
    /// # 参数
    ///
    /// - `mode`: 裸 http(s) host 与服务名索引比对时使用的大小写策略。
    pub fn with_service_match(mut self, mode: ServiceMatchMode) -> Self {
        self.service_match = mode;
        self
    }

    /// 裸 http(s) URL 入口的 scheme 策略。
    ///
    /// # 参数
    ///
    /// - `policy`: 裸 http(s) 命中内部服务后保留或强制改写 scheme 的策略。
    pub fn with_scheme_policy(mut self, policy: SchemePolicy) -> Self {
        self.scheme_policy = policy;
        self
    }

    /// 裸 http(s) host 未命中索引时的策略(外部直连 / 报错)。
    ///
    /// # 参数
    ///
    /// - `policy`: 裸 http(s) host 未确认是内部服务时的处理策略。
    pub fn with_unknown_host(mut self, policy: UnknownHostPolicy) -> Self {
        self.unknown_host = policy;
        self
    }

    /// host 已确认是内部服务但无可用实例 / discover 出错时的策略。
    ///
    /// # 参数
    ///
    /// - `policy`: 内部服务无可用实例或发现出错时是否允许 stale 兜底的策略。
    pub fn with_no_instance(mut self, policy: NoInstancePolicy) -> Self {
        self.no_instance = policy;
        self
    }

    /// 启动期是否要求初始 list_services 成功(仅启发式相关)。
    ///
    /// # 参数
    ///
    /// - `policy`: 启发式索引启动时首拉服务名失败的处理策略。
    pub fn with_startup(mut self, policy: StartupPolicy) -> Self {
        self.startup = policy;
        self
    }

    /// 负载均衡策略(round-robin / 加权)。
    ///
    /// # 参数
    ///
    /// - `strategy`: 内置负载均衡算法；自定义算法注入时会被忽略。
    pub fn with_lb_strategy(mut self, strategy: LbStrategy) -> Self {
        self.lb_strategy = strategy;
        self
    }

    /// 跨实例重试策略(GET/HEAD 传输错误重试)。
    ///
    /// # 参数
    ///
    /// - `retry`: 跨实例重试开关和最大尝试次数。
    pub fn with_retry(mut self, retry: RetryOptions) -> Self {
        self.retry = retry;
        self
    }

    /// 设置每服务 bulkhead、熔断和实例异常摘除策略。
    pub fn with_resilience(mut self, resilience: RestResilienceOptions) -> Self {
        self.resilience = resilience;
        self
    }

    /// 底层 reqwest client 超时(单请求总超时 / 连接超时)。
    ///
    /// # 参数
    ///
    /// - `http`: 底层 HTTP client 的请求总超时和连接超时配置。
    pub fn with_http(mut self, http: RestHttpOptions) -> Self {
        self.http = http;
        self
    }

    /// watch / 实例缓存调优。
    ///
    /// # 参数
    ///
    /// - `watch`: 实例 watch、TTL fallback、stale 窗口和重建退避配置。
    pub fn with_watch(mut self, watch: RestWatchOptions) -> Self {
        self.watch = watch;
        self
    }

    /// 是否保留原始 Host header。
    ///
    /// # 参数
    ///
    /// - `yes`: `true` 表示请求实例时保留原始 host header，默认 `false` 让底层 client 使用实例地址。
    pub fn with_preserve_original_host_header(mut self, yes: bool) -> Self {
        self.preserve_original_host_header = yes;
        self
    }

    // ── direct API 分场景 fail-fast 校验 ──
    // 与 rest-discovery-nacos 的 yml 映射 fail-fast 同等严格,只是分构造器各校验其真正会用到的字段:
    // pump/index 循环里的 clamp 仍保留作内部不变量兜底,但这里先以 typed error 告诉调用方配置错了。

    /// `connect()` 入口校验:http + watch;`heuristic_http=Enabled` 时再校验 heuristic
    /// (connect 会同步首拉 + 启动索引刷新循环,refresh_interval=0 会让 `interval()` 兜底前本应 fail-fast)。
    pub fn validate_for_connect(&self) -> Result<()> {
        validate_http(&self.http)?;
        validate_retry(&self.retry)?;
        validate_resilience(&self.resilience)?;
        validate_watch(&self.watch)?;
        if self.heuristic_http == HeuristicHttpMode::Enabled {
            validate_heuristic(&self.heuristic)?;
        }
        Ok(())
    }

    /// `new()` 入口校验:http + watch。`new()` **不**启动启发式索引刷新,故不因 heuristic.refresh_interval=0 报错。
    pub fn validate_for_new(&self) -> Result<()> {
        validate_http(&self.http)?;
        validate_retry(&self.retry)?;
        validate_resilience(&self.resilience)?;
        validate_watch(&self.watch)?;
        Ok(())
    }

    /// `external_only` 入口校验:只用底层 http client,故只校验 http(不连 provider、不 watch、不建索引)。
    pub fn validate_for_external_only(&self) -> Result<()> {
        validate_http(&self.http)?;
        validate_retry(&self.retry)?;
        validate_resilience(&self.resilience)
    }
}

/// 构造配置校验错误；用于把非法选项统一转换为启动失败。
///
/// # 参数
/// - `reason`: 具体不满足约束的配置原因,会写入 `RestDiscoveryError::InvalidOptions`。
fn invalid_options(reason: impl Into<String>) -> RestDiscoveryError {
    RestDiscoveryError::InvalidOptions {
        reason: reason.into(),
    }
}

/// 校验 HTTP client 超时约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `http`: REST 底层 HTTP client 的总超时和连接超时配置。
fn validate_http(http: &RestHttpOptions) -> Result<()> {
    if http.timeout.is_zero() {
        return Err(invalid_options("http.timeout 必须 > 0(0 会立即超时)"));
    }
    if http.connect_timeout.is_zero() {
        return Err(invalid_options("http.connect_timeout 必须 > 0"));
    }
    validate_duration_bound("http.timeout", http.timeout)?;
    validate_duration_bound("http.connect_timeout", http.connect_timeout)?;
    Ok(())
}

/// 校验 retry 流量预算，避免配置为无界/NaN 后在故障时放大流量。
fn validate_retry(retry: &RetryOptions) -> Result<()> {
    if retry.max_attempts == 0 {
        return Err(invalid_options("retry.max_attempts 必须 >= 1"));
    }
    let enabled =
        retry.retry_get_head_on_transport_error || retry.retry_get_head_on_retryable_status;
    if enabled && retry.max_attempts > 1 {
        if retry.budget_capacity == 0 {
            return Err(invalid_options(
                "retry.budget_capacity 必须 > 0（启用重试时）",
            ));
        }
        if !retry.budget_refill_per_second.is_finite() || retry.budget_refill_per_second <= 0.0 {
            return Err(invalid_options(
                "retry.budget_refill_per_second 必须是有限正数",
            ));
        }
    }
    Ok(())
}

/// 校验隔离/熔断参数，防止零值关闭保护或异常大 semaphore 分配。
fn validate_resilience(resilience: &RestResilienceOptions) -> Result<()> {
    if resilience.max_concurrent_per_service == 0
        || resilience.max_concurrent_per_service > 1_000_000
    {
        return Err(invalid_options(
            "resilience.max_concurrent_per_service 必须在 1..=1000000",
        ));
    }
    if resilience.circuit_failure_threshold == 0 {
        return Err(invalid_options(
            "resilience.circuit_failure_threshold 必须 > 0",
        ));
    }
    if resilience.outlier_failure_threshold == 0 {
        return Err(invalid_options(
            "resilience.outlier_failure_threshold 必须 > 0",
        ));
    }
    if resilience.circuit_open_duration.is_zero() || resilience.outlier_ejection_duration.is_zero()
    {
        return Err(invalid_options(
            "resilience circuit/outlier duration 必须 > 0",
        ));
    }
    validate_duration_bound(
        "resilience.circuit_open_duration",
        resilience.circuit_open_duration,
    )?;
    validate_duration_bound(
        "resilience.outlier_ejection_duration",
        resilience.outlier_ejection_duration,
    )
}

/// 校验服务实例 watch 约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `watch`: 实例轮询、过期兜底和 watch 重建退避配置。
fn validate_watch(watch: &RestWatchOptions) -> Result<()> {
    if watch.poll_interval.is_zero() {
        return Err(invalid_options(
            "watch.poll_interval 必须 > 0(0 会让轮询 panic)",
        ));
    }
    // restart_backoff=0:watch 失败后 sleep(0) + backoff*2 恒为 0 → 高频空转打爆 provider;min>max 语义反直觉。
    if watch.restart_backoff_min.is_zero() {
        return Err(invalid_options(
            "watch.restart_backoff_min 必须 > 0(0 会让 watch 失败后高频空转)",
        ));
    }
    if watch.restart_backoff_max.is_zero() {
        return Err(invalid_options("watch.restart_backoff_max 必须 > 0"));
    }
    if watch.restart_backoff_min > watch.restart_backoff_max {
        return Err(invalid_options(format!(
            "watch.restart_backoff_min({:?})不能大于 restart_backoff_max({:?})",
            watch.restart_backoff_min, watch.restart_backoff_max
        )));
    }
    for (field, duration) in [
        ("watch.poll_interval", watch.poll_interval),
        ("watch.ttl_fallback", watch.ttl_fallback),
        ("watch.stale_if_error", watch.stale_if_error),
        ("watch.restart_backoff_min", watch.restart_backoff_min),
        ("watch.restart_backoff_max", watch.restart_backoff_max),
    ] {
        validate_duration_bound(field, duration)?;
    }
    Ok(())
}

/// 校验裸 http(s) 启发式服务索引约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `heuristic`: 启发式索引刷新间隔和服务移除宽限配置。
fn validate_heuristic(heuristic: &RestHeuristicOptions) -> Result<()> {
    if heuristic.refresh_interval.is_zero() {
        return Err(invalid_options("heuristic.refresh_interval 必须 > 0"));
    }
    if heuristic.removed_service_grace.is_zero() {
        return Err(invalid_options("heuristic.removed_service_grace 必须 > 0"));
    }
    validate_duration_bound("heuristic.refresh_interval", heuristic.refresh_interval)?;
    validate_duration_bound(
        "heuristic.removed_service_grace",
        heuristic.removed_service_grace,
    )?;
    Ok(())
}

/// 拒绝会让计时器或 `Instant + Duration` 跨平台溢出的超长运行参数。
fn validate_duration_bound(field: &str, duration: Duration) -> Result<()> {
    if duration > MAX_RUNTIME_DURATION {
        return Err(invalid_options(format!(
            "{field} 不能超过 {MAX_RUNTIME_DURATION:?}"
        )));
    }
    Ok(())
}
