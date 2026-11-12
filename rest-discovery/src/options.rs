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

/// 服务名匹配大小写策略，仅对 HTTP(S) 启发式服务名索引生效。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServiceMatchMode {
    /// 忽略大小写(按 ASCII lowercase 归一化)。
    #[default]
    CaseInsensitive,
    /// 大小写敏感。
    CaseSensitive,
}

/// 通用 URL 入口的 HTTP(S) 启发式识别开关，默认 `Disabled`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HeuristicHttpMode {
    /// 不维护服务名索引;裸 http(s) 全部普通 HTTP。
    #[default]
    Disabled,
    /// 维护服务名索引；通用 URL 入口的 HTTP(S) host 命中索引时走 LB，不影响显式外部调用。
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
    /// 业务作用：提供实例连接协议的 URL scheme 字面值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：HTTP 返回 http，HTTPS 返回 https，不推断目标端口或 TLS 配置。
    pub fn as_str(self) -> &'static str {
        match self {
            InstanceScheme::Http => "http",
            InstanceScheme::Https => "https",
        }
    }
}

/// 通用 URL 入口启用启发式时，HTTP(S) host 未命中服务名索引的处理策略。
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
    /// 当前运行时允许保留的服务订阅身份上限，达到后拒绝新服务。
    pub max_services: usize,
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
    /// 业务作用：构造服务观察的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回最多 1024 个服务、两秒轮询、三秒发现缓存、十秒错误宽限和一至三十秒重建退避的选项。
    fn default() -> Self {
        Self {
            max_services: 1024,
            poll_interval: Duration::from_secs(2),
            ttl_fallback: Duration::from_secs(3),
            stale_if_error: Duration::from_secs(10),
            restart_backoff_min: Duration::from_secs(1),
            restart_backoff_max: Duration::from_secs(30),
        }
    }
}

impl RestWatchOptions {
    /// 业务作用：取得可继续设置的默认服务观察选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与对应 Default 相同的配置值，不启动任务、网络或资源 owner。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：设置服务实例观察的轮询兜底间隔。
    ///
    /// 参数说明：
    /// - `d`：要保存的服务实例观察的轮询兜底间隔。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// 业务作用：设置watch 不可用时发现快照的缓存时长。
    ///
    /// 参数说明：
    /// - `d`：要保存的watch 不可用时发现快照的缓存时长。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_ttl_fallback(mut self, d: Duration) -> Self {
        self.ttl_fallback = d;
        self
    }

    /// 业务作用：设置发现失败后允许使用旧快照的宽限窗口。
    ///
    /// 参数说明：
    /// - `d`：要保存的发现失败后允许使用旧快照的宽限窗口。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_stale_if_error(mut self, d: Duration) -> Self {
        self.stale_if_error = d;
        self
    }

    /// 业务作用：设置实例观察任务重建时的退避区间。
    ///
    /// 参数说明：
    /// - `min`：失败重试的最小退避。
    /// - `max`：连续失败时允许增长到的最大退避。
    ///
    /// 返回：返回更新后的选项；不检查非零或大小顺序，客户端构造时统一校验。
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
    /// 业务作用：构造启发式服务名索引的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回三十秒刷新和六十秒移除宽限的选项，不启动服务枚举。
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
            removed_service_grace: Duration::from_secs(60),
        }
    }
}

impl RestHeuristicOptions {
    /// 业务作用：取得可继续设置的默认启发式服务索引选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与对应 Default 相同的配置值，不启动任务、网络或资源 owner。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：设置启发式服务名索引的刷新间隔。
    ///
    /// 参数说明：
    /// - `d`：要保存的启发式服务名索引的刷新间隔。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_refresh_interval(mut self, d: Duration) -> Self {
        self.refresh_interval = d;
        self
    }

    /// 业务作用：设置服务名消失后在索引中的保留窗口。
    ///
    /// 参数说明：
    /// - `d`：要保存的服务名消失后在索引中的保留窗口。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
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
    /// 业务作用：构造HTTP 请求预算的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回十秒请求总超时及两秒建连超时，不建立连接池。
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
        }
    }
}

impl RestHttpOptions {
    /// 业务作用：取得可继续设置的默认HTTP 请求预算选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与对应 Default 相同的配置值，不启动任务、网络或资源 owner。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：设置HTTP 请求的总超时。
    ///
    /// 参数说明：
    /// - `d`：要保存的HTTP 请求的总超时。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// 业务作用：设置HTTP 建连超时。
    ///
    /// 参数说明：
    /// - `d`：要保存的HTTP 建连超时。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = d;
        self
    }
}

/// 跨实例重试策略；默认全部关闭，调用方必须显式开启所需重试条件。
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
    /// 业务作用：构造跨实例重试的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回禁用传输及状态重试、一次尝试、令牌容量二十和每秒补充十个令牌的选项。
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
    /// 业务作用：取得可继续设置的默认跨实例重试选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与对应 Default 相同的配置值，不启动任务、网络或资源 owner。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：构造只允许 GET/HEAD 传输错误重试的选项。
    ///
    /// 参数说明：
    /// - `max_attempts`：含首次请求的总尝试上限，最小规范为一。
    ///
    /// 返回：返回开启传输重试、关闭状态重试的选项并沿用默认令牌预算；总尝试上限为一时不发生额外尝试。
    pub fn get_head_on_transport_error(max_attempts: usize) -> Self {
        Self {
            retry_get_head_on_transport_error: true,
            retry_get_head_on_retryable_status: false,
            max_attempts: max_attempts.max(1),
            ..Self::default()
        }
    }

    /// 业务作用：构造允许 GET/HEAD 传输及可重试状态失败的受限重试策略。
    ///
    /// 参数说明：
    /// - `max_attempts`：含首次的总尝试次数，至少保存为一。
    /// - `budget_capacity`：额外尝试令牌桶容量。
    /// - `budget_refill_per_second`：每秒令牌补充速率。
    ///
    /// 返回：返回两个重试条件均开启的配置；容量和补充速率在此原样保存，客户端构造时统一校验。
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
    /// 业务作用：构造服务隔离及故障保护的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回每服务并发 256、五次失败熔断、三次实例失败摘除及各三十秒窗口的选项；构造客户端时再统一校验。
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
    /// 业务作用：设置每服务隔离并发上限。
    ///
    /// 参数说明：
    /// - `limit`：要保存的每服务隔离并发上限。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_max_concurrent_per_service(mut self, limit: usize) -> Self {
        self.max_concurrent_per_service = limit;
        self
    }

    /// 业务作用：设置服务级连续失败熔断阈值及开放窗口。
    ///
    /// 参数说明：
    /// - `failure_threshold`：触发熔断的失败阈值。
    /// - `open_duration`：熔断后的开放持续时间。
    ///
    /// 返回：返回更新后的选项，不改变运行中服务状态；数值合法性由客户端构造检查。
    pub fn with_circuit(mut self, failure_threshold: u32, open_duration: Duration) -> Self {
        self.circuit_failure_threshold = failure_threshold;
        self.circuit_open_duration = open_duration;
        self
    }

    /// 业务作用：设置实例级失败摘除阈值及窗口。
    ///
    /// 参数说明：
    /// - `failure_threshold`：触发实例摘除的失败阈值。
    /// - `duration`：单次摘除持续时间。
    ///
    /// 返回：返回更新后的选项；不立即摘除实例，阈值及期限由客户端构造检查。
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
    /// 业务作用：构造REST 发现运行的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回大小写不敏感匹配、关闭启发式、保留 URL scheme、默认 HTTP、未知 host 外部直连、无实例报错、轮询负载均衡和默认子策略；不保留原 Host，也未设置 span 记录器。
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
    /// 业务作用：取得可继续设置的默认REST 发现运行选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与对应 Default 相同的配置值，不启动任务、网络或资源 owner。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：设置只写 span 记录入口，不转移 exporter 关闭或刷新权。
    ///
    /// 参数说明：
    /// - `recorder`：要保存的只写 span 记录入口，不转移 exporter 关闭或刷新权。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_span_recorder(mut self, recorder: SpanRecorder) -> Self {
        self.span_recorder = Some(recorder);
        self
    }

    /// 业务作用：设置无原始 URL scheme 时访问实例的默认协议。
    ///
    /// 参数说明：
    /// - `scheme`：要保存的无原始 URL scheme 时访问实例的默认协议。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_default_instance_scheme(mut self, scheme: InstanceScheme) -> Self {
        self.default_instance_scheme = scheme;
        self
    }

    /// 业务作用：设置是否启用裸 HTTP host 的服务名索引识别。
    ///
    /// 参数说明：
    /// - `mode`：要保存的是否启用裸 HTTP host 的服务名索引识别。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_heuristic_http(mut self, mode: HeuristicHttpMode) -> Self {
        self.heuristic_http = mode;
        self
    }

    /// 业务作用：设置服务名索引刷新与移除宽限配置。
    ///
    /// 参数说明：
    /// - `heuristic`：要保存的服务名索引刷新与移除宽限配置。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_heuristic_options(mut self, heuristic: RestHeuristicOptions) -> Self {
        self.heuristic = heuristic;
        self
    }

    /// 业务作用：设置HTTP host 与服务名比较的大小写策略。
    ///
    /// 参数说明：
    /// - `mode`：要保存的HTTP host 与服务名比较的大小写策略。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_service_match(mut self, mode: ServiceMatchMode) -> Self {
        self.service_match = mode;
        self
    }

    /// 业务作用：设置识别为内部服务后保留或改写 URL scheme 的策略。
    ///
    /// 参数说明：
    /// - `policy`：要保存的识别为内部服务后保留或改写 URL scheme 的策略。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_scheme_policy(mut self, policy: SchemePolicy) -> Self {
        self.scheme_policy = policy;
        self
    }

    /// 业务作用：设置HTTP host 未命中内部服务索引时的处理策略。
    ///
    /// 参数说明：
    /// - `policy`：要保存的HTTP host 未命中内部服务索引时的处理策略。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_unknown_host(mut self, policy: UnknownHostPolicy) -> Self {
        self.unknown_host = policy;
        self
    }

    /// 业务作用：设置内部服务无可用实例时的处理策略。
    ///
    /// 参数说明：
    /// - `policy`：要保存的内部服务无可用实例时的处理策略。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_no_instance(mut self, policy: NoInstancePolicy) -> Self {
        self.no_instance = policy;
        self
    }

    /// 业务作用：设置启发式索引首次服务枚举失败的启动策略。
    ///
    /// 参数说明：
    /// - `policy`：要保存的启发式索引首次服务枚举失败的启动策略。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_startup(mut self, policy: StartupPolicy) -> Self {
        self.startup = policy;
        self
    }

    /// 业务作用：设置内置负载均衡选择策略。
    ///
    /// 参数说明：
    /// - `strategy`：要保存的内置负载均衡选择策略。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_lb_strategy(mut self, strategy: LbStrategy) -> Self {
        self.lb_strategy = strategy;
        self
    }

    /// 业务作用：设置跨实例重试条件、次数和令牌预算配置。
    ///
    /// 参数说明：
    /// - `retry`：要保存的跨实例重试条件、次数和令牌预算配置。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_retry(mut self, retry: RetryOptions) -> Self {
        self.retry = retry;
        self
    }

    /// 业务作用：设置服务隔离、熔断和实例摘除配置。
    ///
    /// 参数说明：
    /// - `resilience`：要保存的服务隔离、熔断和实例摘除配置。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_resilience(mut self, resilience: RestResilienceOptions) -> Self {
        self.resilience = resilience;
        self
    }

    /// 业务作用：设置底层 HTTP 请求和建连预算配置。
    ///
    /// 参数说明：
    /// - `http`：要保存的底层 HTTP 请求和建连预算配置。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_http(mut self, http: RestHttpOptions) -> Self {
        self.http = http;
        self
    }

    /// 业务作用：设置实例观察、缓存和重建配置。
    ///
    /// 参数说明：
    /// - `watch`：要保存的实例观察、缓存和重建配置。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_watch(mut self, watch: RestWatchOptions) -> Self {
        self.watch = watch;
        self
    }

    /// 业务作用：设置是否向目标实例转发原始 Host 头。
    ///
    /// 参数说明：
    /// - `yes`：要保存的是否向目标实例转发原始 Host 头。
    ///
    /// 返回：返回更新后的配置；本方法仅保存选项，不校验组合有效性、不发起网络或修改已有客户端。
    pub fn with_preserve_original_host_header(mut self, yes: bool) -> Self {
        self.preserve_original_host_header = yes;
        self
    }

    // ── direct API 分场景 fail-fast 校验 ──
    // 与 rest-discovery-nacos 的 yml 映射 fail-fast 同等严格,只是分构造器各校验其真正会用到的字段:
    // pump/index 循环里的 clamp 仍保留作内部不变量兜底,但这里先以 typed error 告诉调用方配置错了。

    /// 业务作用：在连接发现后端前校验所有会启用的 REST 运行策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：HTTP、重试、保护和 watch 策略合法时成功；启用启发式时还校验索引策略，任何非法配置返回 InvalidOptions，不执行连接。
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

    /// 业务作用：校验不负责启动启发式索引的客户端构造选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：HTTP、重试、保护及 watch 策略合法时成功，非法时返回 InvalidOptions；不检查未启用的启发式刷新策略。
    pub fn validate_for_new(&self) -> Result<()> {
        validate_http(&self.http)?;
        validate_retry(&self.retry)?;
        validate_resilience(&self.resilience)?;
        validate_watch(&self.watch)?;
        Ok(())
    }

    /// 业务作用：校验外部直连客户端使用的请求、重试及保护配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：HTTP、重试和故障保护合法时成功，否则返回 InvalidOptions；不检查发现 watch 或服务名索引。
    pub fn validate_for_external_only(&self) -> Result<()> {
        validate_http(&self.http)?;
        validate_retry(&self.retry)?;
        validate_resilience(&self.resilience)
    }
}

/// 业务作用：将具体选项约束失败封装成统一构造错误。
///
/// 参数说明：
/// - `reason`：将原样保留的约束失败原因。
///
/// 返回：返回携带该原因的 InvalidOptions，不自动脱敏或启动恢复。
fn invalid_options(reason: impl Into<String>) -> RestDiscoveryError {
    RestDiscoveryError::InvalidOptions {
        reason: reason.into(),
    }
}

/// 业务作用：在建立 HTTP 客户端前检查请求和建连期限。
///
/// 参数说明：
/// - `http`：提供总超时和连接超时的配置。
///
/// 返回：两项均非零且不超过一年时成功，否则返回 InvalidOptions；不要求连接超时小于总超时。
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

/// 业务作用：检查最大尝试次数和实际开启额外尝试时的令牌预算。
///
/// 参数说明：
/// - `retry`：重试条件、次数及共享预算配置。
///
/// 返回：次数至少一时继续；仅重试已开启且次数大于一时要求正容量和有限正补充速率，非法返回 InvalidOptions。
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

/// 业务作用：检查服务并发、熔断和实例摘除保护参数。
///
/// 参数说明：
/// - `resilience`：隔离容量、失败阈值及保护期限配置。
///
/// 返回：并发为一至一百万、两个失败阈值非零且保护期限非零并不超过一年时成功，否则返回 InvalidOptions。
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

/// 业务作用：检查观察目录容量、轮询和任务恢复期限。
///
/// 参数说明：
/// - `watch`：服务容量、轮询、缓存、错误宽限及重建退避。
///
/// 返回：容量为一至 65536、轮询及退避非零、退避有序且全部期限不超过一年时成功，否则返回 InvalidOptions；缓存与错误宽限允许为零。
fn validate_watch(watch: &RestWatchOptions) -> Result<()> {
    if watch.max_services == 0 || watch.max_services > 65536 {
        return Err(invalid_options(
            "watch.max_services must be between 1 and 65536",
        ));
    }
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

/// 业务作用：检查启发式服务名索引刷新和移除宽限。
///
/// 参数说明：
/// - `heuristic`：索引刷新周期及名称移除保留期。
///
/// 返回：两项均非零且不超过一年时成功，否则返回 InvalidOptions。
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

/// 业务作用：限制运行期限以保持计时器与截止点算术在约定范围内。
///
/// 参数说明：
/// - `field`：用于定位错误的配置字段名。
/// - `duration`：不能超过一年上限的期限。
///
/// 返回：不超过上限时成功，超限返回携带字段名的 InvalidOptions；本函数允许零，非零约束由各配置检查器负责。
fn validate_duration_bound(field: &str, duration: Duration) -> Result<()> {
    if duration > MAX_RUNTIME_DURATION {
        return Err(invalid_options(format!(
            "{field} 不能超过 {MAX_RUNTIME_DURATION:?}"
        )));
    }
    Ok(())
}
