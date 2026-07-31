//! 端点安全流水线的低基数进程内指标与 Prometheus 文本渲染。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::http::StatusCode;

use crate::{AuthRequirement, CryptoDirections, ReplayRequirement, RoutePolicy};

/// 延迟直方图的有限上界，单位纳秒。
const DURATION_BUCKETS_NANOS: [u64; 13] = [
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
];

/// 与直方图上界逐项对应的 Prometheus `le` 标签。
const DURATION_BUCKET_LABELS: [&str; 13] = [
    "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1",
    "2.5", "5",
];

/// 一次安全阶段对外可观察的有限结果类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityMetricOutcome {
    /// 阶段完成并满足静态合同。
    Success,
    /// 客户端输入、身份或重放冲突被安全边界拒绝。
    Rejected,
    /// 后端、策略、资源或内部故障导致服务端关闭请求。
    Failure,
    /// 单一总 deadline 或密码操作预算耗尽。
    Timeout,
}

impl SecurityMetricOutcome {
    /// 根据最终 HTTP 状态收敛一次端点请求的结果。
    ///
    /// # 参数
    ///
    /// - `status`: 安全 composer 产生的最终公开状态，不包含响应正文。
    ///
    /// # 返回
    ///
    /// 2xx/3xx 为成功、4xx 为拒绝、5xx 为服务故障；显式 deadline 应由调用方记录为
    /// [`Self::Timeout`]，避免把同为 503 的超时与后端故障混为一类。
    pub fn from_status(status: StatusCode) -> Self {
        if status.is_success() || status.is_redirection() {
            Self::Success
        } else if status.is_client_error() {
            Self::Rejected
        } else {
            Self::Failure
        }
    }

    /// 返回原子数组使用的固定槽位。
    ///
    /// # 返回
    ///
    /// 返回 `0..4` 的稳定索引，不接受外部字符串。
    const fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Rejected => 1,
            Self::Failure => 2,
            Self::Timeout => 3,
        }
    }

    /// 返回 Prometheus 使用的固定结果标签。
    ///
    /// # 返回
    ///
    /// 返回低基数小写文本。
    const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Failure => "failure",
            Self::Timeout => "timeout",
        }
    }
}

/// 身份阶段不会携带 subject 或 token 的有限结果类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMetricOutcome {
    /// provider 返回经过验证的身份上下文。
    Authenticated,
    /// optional 路由没有凭证并以匿名身份继续。
    Anonymous,
    /// provider 或 adapter 返回受控业务短路响应。
    ShortCircuit,
    /// 身份缺失、无效、禁用、后端或策略错误。
    Rejected,
}

impl AuthMetricOutcome {
    /// 返回原子数组使用的固定槽位。
    ///
    /// # 返回
    ///
    /// 返回 `0..4` 的稳定索引。
    const fn index(self) -> usize {
        match self {
            Self::Authenticated => 0,
            Self::Anonymous => 1,
            Self::ShortCircuit => 2,
            Self::Rejected => 3,
        }
    }

    /// 返回 Prometheus 使用的固定身份结果标签。
    ///
    /// # 返回
    ///
    /// 返回不含身份值的低基数文本。
    const fn label(self) -> &'static str {
        match self {
            Self::Authenticated => "authenticated",
            Self::Anonymous => "anonymous",
            Self::ShortCircuit => "short_circuit",
            Self::Rejected => "rejected",
        }
    }
}

/// 密码阶段的固定操作名称。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityMetricOperation {
    /// header 身份认证与 condition 计算。
    Auth,
    /// 只读可信元数据的密码 condition 计算。
    Condition,
    /// 请求信封校验、解密与 required replay 占位。
    Decrypt,
    /// extractor 与业务 handler 执行。
    Handler,
    /// 完整响应序列化、加密与实体头重建。
    Encrypt,
    /// 从进入 route composer 到最终响应的总耗时。
    Total,
}

impl SecurityMetricOperation {
    /// 返回直方图数组使用的固定槽位。
    ///
    /// # 返回
    ///
    /// 返回 `0..6` 的稳定索引。
    const fn index(self) -> usize {
        match self {
            Self::Auth => 0,
            Self::Condition => 1,
            Self::Decrypt => 2,
            Self::Handler => 3,
            Self::Encrypt => 4,
            Self::Total => 5,
        }
    }

    /// 返回 Prometheus 使用的固定操作标签。
    ///
    /// # 返回
    ///
    /// 返回低基数小写文本。
    const fn label(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Condition => "condition",
            Self::Decrypt => "decrypt",
            Self::Handler => "handler",
            Self::Encrypt => "encrypt",
            Self::Total => "total",
        }
    }
}

/// required replay 占位的有限结果类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayMetricOutcome {
    /// rid 首次成功占位。
    Fresh,
    /// rid 已存在，业务 handler 未执行。
    Duplicate,
    /// 共享存储失败或超时，required 路由关闭请求。
    BackendFailure,
}

impl ReplayMetricOutcome {
    /// 返回原子数组使用的固定槽位。
    ///
    /// # 返回
    ///
    /// 返回 `0..3` 的稳定索引。
    const fn index(self) -> usize {
        match self {
            Self::Fresh => 0,
            Self::Duplicate => 1,
            Self::BackendFailure => 2,
        }
    }

    /// 返回 Prometheus 使用的固定重放结果标签。
    ///
    /// # 返回
    ///
    /// 返回不含 rid 的低基数文本。
    const fn label(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Duplicate => "duplicate",
            Self::BackendFailure => "backend_failure",
        }
    }
}

/// 单个固定操作的单调累计延迟直方图。
struct DurationHistogram {
    /// 已按 Prometheus 累计语义更新的有限 bucket。
    buckets: [AtomicU64; DURATION_BUCKETS_NANOS.len()],
    /// 所有观测次数，对应 `+Inf` bucket。
    count: AtomicU64,
    /// 所有观测纳秒和，使用饱和转换避免平台时长溢出。
    sum_nanos: AtomicU64,
}

impl DurationHistogram {
    /// 建立所有槽位为零的直方图。
    ///
    /// # 返回
    ///
    /// 返回可被多线程无锁写入的固定 bucket 集合。
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }

    /// 记录一次有界阶段耗时。
    ///
    /// # 参数
    ///
    /// - `duration`: 调用方用单调时钟测得的阶段时长。
    fn observe(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        for (index, bound) in DURATION_BUCKETS_NANOS.iter().enumerate() {
            if nanos <= *bound {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    /// 读取一次一致性要求较弱但单调的抓取视图。
    ///
    /// # 返回
    ///
    /// 返回各 bucket、总数和总纳秒；并发写入期间允许相邻字段有极短时间差，符合 Prometheus
    /// 累计指标抓取语义。
    fn export(&self) -> HistogramExport {
        HistogramExport {
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            count: self.count.load(Ordering::Relaxed),
            sum_nanos: self.sum_nanos.load(Ordering::Relaxed),
        }
    }
}

/// 抓取时复制的直方图只读视图。
struct HistogramExport {
    /// 有限累计 bucket。
    buckets: [u64; DURATION_BUCKETS_NANOS.len()],
    /// 总观测次数。
    count: u64,
    /// 总观测纳秒。
    sum_nanos: u64,
}

/// 一条编译期路由的全部安全指标槽位。
pub struct RouteSecurityMetrics {
    /// 审计后的稳定 route ID，不包含运行时 path 参数。
    route_id: &'static str,
    /// 静态协议 ID；无密码路由使用 `none`。
    protocol: &'static str,
    /// 静态身份要求文本。
    auth_requirement: &'static str,
    /// 静态 condition ID；无 condition 时为 `None`。
    condition: Option<&'static str>,
    /// route 声明的密码方向，用于避免输出无意义时间序列。
    crypto_directions: CryptoDirections,
    /// route 是否要求共享重放占位。
    replay_required: bool,
    /// 最终端点结果计数。
    requests: [AtomicU64; 4],
    /// 身份阶段结果计数。
    auth: [AtomicU64; 4],
    /// 请求解密方向结果计数。
    crypto_request: [AtomicU64; 4],
    /// 响应加密方向结果计数。
    crypto_response: [AtomicU64; 4],
    /// required replay 占位结果计数。
    replay: [AtomicU64; 3],
    /// condition 实际关闭全部声明方向的次数。
    bypass: AtomicU64,
    /// 固定安全阶段延迟直方图。
    durations: [DurationHistogram; 6],
}

impl RouteSecurityMetrics {
    /// 从已审计静态路由建立有限指标槽位。
    ///
    /// # 参数
    ///
    /// - `policy`: 过程宏生成且不包含请求输入的路由合同。
    ///
    /// # 返回
    ///
    /// 返回只使用静态 label 的原子指标集合。
    fn new(policy: RoutePolicy) -> Self {
        Self {
            route_id: policy.route_id,
            protocol: policy.crypto_protocol.unwrap_or("none"),
            auth_requirement: auth_requirement_label(policy.auth),
            condition: policy.crypto_condition,
            crypto_directions: CryptoDirections::declared(&policy),
            replay_required: policy.replay == ReplayRequirement::Required,
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            auth: std::array::from_fn(|_| AtomicU64::new(0)),
            crypto_request: std::array::from_fn(|_| AtomicU64::new(0)),
            crypto_response: std::array::from_fn(|_| AtomicU64::new(0)),
            replay: std::array::from_fn(|_| AtomicU64::new(0)),
            bypass: AtomicU64::new(0),
            durations: std::array::from_fn(|_| DurationHistogram::new()),
        }
    }

    /// 记录一次最终端点结果和总耗时。
    ///
    /// # 参数
    ///
    /// - `outcome`: 已由 composer 收敛的有限结果类别。
    /// - `duration`: 从进入 route layer 到最终响应的单调时长。
    pub fn record_request(&self, outcome: SecurityMetricOutcome, duration: Duration) {
        self.requests[outcome.index()].fetch_add(1, Ordering::Relaxed);
        self.observe_duration(SecurityMetricOperation::Total, duration);
    }

    /// 记录一次身份阶段结果和耗时。
    ///
    /// # 参数
    ///
    /// - `outcome`: 不包含 subject、token 或后端详情的身份结果。
    /// - `duration`: provider 与 condition 的单调总耗时。
    pub fn record_auth(&self, outcome: AuthMetricOutcome, duration: Duration) {
        self.auth[outcome.index()].fetch_add(1, Ordering::Relaxed);
        self.observe_duration(SecurityMetricOperation::Auth, duration);
    }

    /// 记录一次请求解密或响应加密结果。
    ///
    /// # 参数
    ///
    /// - `request_direction`: `true` 表示请求解密，`false` 表示响应加密。
    /// - `outcome`: 有限成功、拒绝、故障或超时类别。
    /// - `duration`: 本方向协议与 provider 执行的单调耗时。
    pub fn record_crypto(
        &self,
        request_direction: bool,
        outcome: SecurityMetricOutcome,
        duration: Duration,
    ) {
        let counters = if request_direction {
            &self.crypto_request
        } else {
            &self.crypto_response
        };
        counters[outcome.index()].fetch_add(1, Ordering::Relaxed);
        self.observe_duration(
            if request_direction {
                SecurityMetricOperation::Decrypt
            } else {
                SecurityMetricOperation::Encrypt
            },
            duration,
        );
    }

    /// 记录 required replay 的有限占位结果。
    ///
    /// # 参数
    ///
    /// - `outcome`: 首次占位、重复或共享后端故障，不包含 rid。
    pub fn record_replay(&self, outcome: ReplayMetricOutcome) {
        self.replay[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 condition 实际关闭 route 声明方向的次数。
    ///
    /// 调用方只有在 effective directions 严格小于 declared directions 时调用，避免把普通请求误记为
    /// 旁路。指标 label 只使用 route 静态 condition ID。
    pub fn record_bypass(&self) {
        self.bypass.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一个固定操作的单调耗时。
    ///
    /// # 参数
    ///
    /// - `operation`: 编译期有限操作类别。
    /// - `duration`: 调用方用 `Instant` 测得的时长。
    pub fn observe_duration(&self, operation: SecurityMetricOperation, duration: Duration) {
        self.durations[operation.index()].observe(duration);
    }
}

/// 跨热更新持续存在的安全指标注册表。
pub struct SecurityMetrics {
    /// 按静态 route ID 排序的有限路由指标集合。
    routes: RwLock<BTreeMap<&'static str, Arc<RouteSecurityMetrics>>>,
    /// 成功发布与失败保留 last-good 的热更新计数。
    reloads: [AtomicU64; 2],
    /// 当前已发布安全快照代次。
    generation: AtomicU64,
}

impl SecurityMetrics {
    /// 建立空注册表并设置初始快照代次。
    ///
    /// # 参数
    ///
    /// - `generation`: 应用启动期已经完整构建的非敏感配置代次。
    ///
    /// # 返回
    ///
    /// 返回跨后续 ArcSwap 热更新持续复用的指标对象。
    pub fn new(generation: u64) -> Self {
        Self {
            routes: RwLock::new(BTreeMap::new()),
            reloads: std::array::from_fn(|_| AtomicU64::new(0)),
            generation: AtomicU64::new(generation),
        }
    }

    /// 返回或注册一条静态路由的指标句柄。
    ///
    /// # 参数
    ///
    /// - `policy`: 已由宏生成并将在启动期审计的静态路由合同。
    ///
    /// # 返回
    ///
    /// 同一 route ID 始终返回同一原子集合；注册数量受二进制静态路由数限制，不读取 raw path。
    pub fn route(&self, policy: RoutePolicy) -> Arc<RouteSecurityMetrics> {
        if let Some(existing) = read_routes(&self.routes).get(policy.route_id).cloned() {
            return existing;
        }
        let mut routes = write_routes(&self.routes);
        routes
            .entry(policy.route_id)
            .or_insert_with(|| Arc::new(RouteSecurityMetrics::new(policy)))
            .clone()
    }

    /// 记录一次安全快照热更新结果。
    ///
    /// # 参数
    ///
    /// - `success`: `true` 表示更高代次已发布，`false` 表示保留 last-good。
    pub fn record_reload(&self, success: bool) {
        self.reloads[usize::from(!success)].fetch_add(1, Ordering::Relaxed);
    }

    /// 更新当前快照代次 gauge。
    ///
    /// # 参数
    ///
    /// - `generation`: 已完成全量构建和原子发布的更高代次。
    pub fn set_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::Release);
    }

    /// 渲染不含密钥、凭证、请求数据和动态 path 的 Prometheus 文本。
    ///
    /// # 返回
    ///
    /// 返回稳定排序的 text exposition 片段，可与应用其它指标直接拼接。
    pub fn render_prometheus(&self) -> String {
        let routes = read_routes(&self.routes)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut output = String::with_capacity(4096 + routes.len() * 8192);
        render_request_counters(&mut output, &routes);
        render_auth_counters(&mut output, &routes);
        render_crypto_counters(&mut output, &routes);
        render_replay_counters(&mut output, &routes);
        render_bypass_counters(&mut output, &routes);
        render_duration_histograms(&mut output, &routes);
        output.push_str("# HELP mapping_crypto_key_reload_total 安全快照热更新结果计数。\n");
        output.push_str("# TYPE mapping_crypto_key_reload_total counter\n");
        output.push_str(&format!(
            "mapping_crypto_key_reload_total{{outcome=\"success\"}} {}\n",
            self.reloads[0].load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "mapping_crypto_key_reload_total{{outcome=\"failure\"}} {}\n",
            self.reloads[1].load(Ordering::Relaxed)
        ));
        output.push_str("# HELP mapping_crypto_snapshot_generation 当前安全快照代次。\n");
        output.push_str("# TYPE mapping_crypto_snapshot_generation gauge\n");
        output.push_str(&format!(
            "mapping_crypto_snapshot_generation {}\n",
            self.generation.load(Ordering::Acquire)
        ));
        output
    }
}

impl Default for SecurityMetrics {
    /// 建立 generation 为零的空安全指标注册表。
    ///
    /// # 返回
    ///
    /// 返回适合无安全路由兼容运行时的指标对象。
    fn default() -> Self {
        Self::new(0)
    }
}

/// 从读锁取得路由注册表，并在锁中毒时保留已有数据继续服务。
///
/// # 参数
///
/// - `lock`: 安全指标私有路由注册表锁。
///
/// # 返回
///
/// 返回读 guard；恢复中毒只影响观测，不允许指标故障打断业务请求。
fn read_routes<'a>(
    lock: &'a RwLock<BTreeMap<&'static str, Arc<RouteSecurityMetrics>>>,
) -> std::sync::RwLockReadGuard<'a, BTreeMap<&'static str, Arc<RouteSecurityMetrics>>> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 从写锁取得路由注册表，并在锁中毒时保留已有数据继续注册。
///
/// # 参数
///
/// - `lock`: 安全指标私有路由注册表锁。
///
/// # 返回
///
/// 返回写 guard；调用方只插入编译期静态 route ID。
fn write_routes<'a>(
    lock: &'a RwLock<BTreeMap<&'static str, Arc<RouteSecurityMetrics>>>,
) -> std::sync::RwLockWriteGuard<'a, BTreeMap<&'static str, Arc<RouteSecurityMetrics>>> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 返回身份要求的固定 Prometheus 标签。
///
/// # 参数
///
/// - `requirement`: 路由编译期身份合同。
///
/// # 返回
///
/// 返回四种有限小写文本之一。
fn auth_requirement_label(requirement: AuthRequirement) -> &'static str {
    match requirement {
        AuthRequirement::Unspecified => "unspecified",
        AuthRequirement::Required => "required",
        AuthRequirement::Optional => "optional",
        AuthRequirement::Public => "public",
    }
}

/// 转义 Prometheus label 中的反斜线、引号和换行。
///
/// # 参数
///
/// - `value`: 只来自编译期 route policy 的静态文本。
///
/// # 返回
///
/// 返回符合 text exposition 的安全标签值。
fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// 渲染端点最终结果计数。
///
/// # 参数
///
/// - `output`: 调用方拥有的 Prometheus 文本缓冲区。
/// - `routes`: 已按 route ID 排序复制出的有限指标句柄。
fn render_request_counters(output: &mut String, routes: &[Arc<RouteSecurityMetrics>]) {
    output.push_str("# HELP mapping_security_requests_total 安全端点最终请求结果计数。\n");
    output.push_str("# TYPE mapping_security_requests_total counter\n");
    for route in routes {
        let route_id = escape_label(route.route_id);
        for outcome in metric_outcomes() {
            output.push_str(&format!(
                "mapping_security_requests_total{{route_id=\"{route_id}\",outcome=\"{}\"}} {}\n",
                outcome.label(),
                route.requests[outcome.index()].load(Ordering::Relaxed)
            ));
        }
    }
}

/// 渲染身份阶段结果计数。
///
/// # 参数
///
/// - `output`: 调用方拥有的 Prometheus 文本缓冲区。
/// - `routes`: 已按 route ID 排序复制出的有限指标句柄。
fn render_auth_counters(output: &mut String, routes: &[Arc<RouteSecurityMetrics>]) {
    output.push_str("# HELP mapping_auth_requests_total 身份阶段结果计数，不包含身份值。\n");
    output.push_str("# TYPE mapping_auth_requests_total counter\n");
    for route in routes {
        if route.auth_requirement == "public" || route.auth_requirement == "unspecified" {
            continue;
        }
        let route_id = escape_label(route.route_id);
        for outcome in auth_outcomes() {
            output.push_str(&format!(
                "mapping_auth_requests_total{{route_id=\"{route_id}\",requirement=\"{}\",outcome=\"{}\"}} {}\n",
                route.auth_requirement,
                outcome.label(),
                route.auth[outcome.index()].load(Ordering::Relaxed)
            ));
        }
    }
}

/// 渲染请求解密与响应加密结果计数。
///
/// # 参数
///
/// - `output`: 调用方拥有的 Prometheus 文本缓冲区。
/// - `routes`: 已按 route ID 排序复制出的有限指标句柄。
fn render_crypto_counters(output: &mut String, routes: &[Arc<RouteSecurityMetrics>]) {
    output.push_str("# HELP mapping_crypto_requests_total 密码方向执行结果计数。\n");
    output.push_str("# TYPE mapping_crypto_requests_total counter\n");
    for route in routes {
        let route_id = escape_label(route.route_id);
        for (enabled, direction, counters) in [
            (
                route.crypto_directions.request,
                "request",
                &route.crypto_request,
            ),
            (
                route.crypto_directions.response,
                "response",
                &route.crypto_response,
            ),
        ] {
            if !enabled {
                continue;
            }
            for outcome in metric_outcomes() {
                output.push_str(&format!(
                    "mapping_crypto_requests_total{{route_id=\"{route_id}\",protocol=\"{}\",direction=\"{direction}\",outcome=\"{}\"}} {}\n",
                    escape_label(route.protocol),
                    outcome.label(),
                    counters[outcome.index()].load(Ordering::Relaxed)
                ));
            }
        }
    }
}

/// 渲染 required replay 原子占位结果。
///
/// # 参数
///
/// - `output`: 调用方拥有的 Prometheus 文本缓冲区。
/// - `routes`: 已按 route ID 排序复制出的有限指标句柄。
fn render_replay_counters(output: &mut String, routes: &[Arc<RouteSecurityMetrics>]) {
    output.push_str("# HELP mapping_crypto_replay_total required replay 占位结果计数。\n");
    output.push_str("# TYPE mapping_crypto_replay_total counter\n");
    for route in routes {
        if !route.replay_required {
            continue;
        }
        let route_id = escape_label(route.route_id);
        for outcome in replay_outcomes() {
            output.push_str(&format!(
                "mapping_crypto_replay_total{{route_id=\"{route_id}\",outcome=\"{}\"}} {}\n",
                outcome.label(),
                route.replay[outcome.index()].load(Ordering::Relaxed)
            ));
        }
    }
}

/// 渲染受控 condition 实际旁路计数。
///
/// # 参数
///
/// - `output`: 调用方拥有的 Prometheus 文本缓冲区。
/// - `routes`: 已按 route ID 排序复制出的有限指标句柄。
fn render_bypass_counters(output: &mut String, routes: &[Arc<RouteSecurityMetrics>]) {
    output.push_str("# HELP mapping_crypto_bypass_total 静态 condition 实际关闭密码方向的次数。\n");
    output.push_str("# TYPE mapping_crypto_bypass_total counter\n");
    for route in routes {
        let Some(condition) = route.condition else {
            continue;
        };
        output.push_str(&format!(
            "mapping_crypto_bypass_total{{route_id=\"{}\",condition=\"{}\"}} {}\n",
            escape_label(route.route_id),
            escape_label(condition),
            route.bypass.load(Ordering::Relaxed)
        ));
    }
}

/// 渲染全部已观测安全阶段的累计延迟直方图。
///
/// # 参数
///
/// - `output`: 调用方拥有的 Prometheus 文本缓冲区。
/// - `routes`: 已按 route ID 排序复制出的有限指标句柄。
fn render_duration_histograms(output: &mut String, routes: &[Arc<RouteSecurityMetrics>]) {
    output.push_str("# HELP mapping_crypto_duration_seconds 安全流水线固定阶段延迟秒数。\n");
    output.push_str("# TYPE mapping_crypto_duration_seconds histogram\n");
    for route in routes {
        let route_id = escape_label(route.route_id);
        let protocol = escape_label(route.protocol);
        for operation in metric_operations() {
            let export = route.durations[operation.index()].export();
            if export.count == 0 {
                continue;
            }
            for (label, value) in DURATION_BUCKET_LABELS.iter().zip(export.buckets) {
                output.push_str(&format!(
                    "mapping_crypto_duration_seconds_bucket{{route_id=\"{route_id}\",protocol=\"{protocol}\",operation=\"{}\",le=\"{label}\"}} {value}\n",
                    operation.label()
                ));
            }
            output.push_str(&format!(
                "mapping_crypto_duration_seconds_bucket{{route_id=\"{route_id}\",protocol=\"{protocol}\",operation=\"{}\",le=\"+Inf\"}} {}\n",
                operation.label(),
                export.count
            ));
            output.push_str(&format!(
                "mapping_crypto_duration_seconds_sum{{route_id=\"{route_id}\",protocol=\"{protocol}\",operation=\"{}\"}} {:.9}\n",
                operation.label(),
                export.sum_nanos as f64 / 1_000_000_000_f64
            ));
            output.push_str(&format!(
                "mapping_crypto_duration_seconds_count{{route_id=\"{route_id}\",protocol=\"{protocol}\",operation=\"{}\"}} {}\n",
                operation.label(),
                export.count
            ));
        }
    }
}

/// 返回全部端点结果类别的固定顺序。
///
/// # 返回
///
/// 返回用于稳定渲染的四项数组。
const fn metric_outcomes() -> [SecurityMetricOutcome; 4] {
    [
        SecurityMetricOutcome::Success,
        SecurityMetricOutcome::Rejected,
        SecurityMetricOutcome::Failure,
        SecurityMetricOutcome::Timeout,
    ]
}

/// 返回全部身份结果类别的固定顺序。
///
/// # 返回
///
/// 返回用于稳定渲染的四项数组。
const fn auth_outcomes() -> [AuthMetricOutcome; 4] {
    [
        AuthMetricOutcome::Authenticated,
        AuthMetricOutcome::Anonymous,
        AuthMetricOutcome::ShortCircuit,
        AuthMetricOutcome::Rejected,
    ]
}

/// 返回全部重放结果类别的固定顺序。
///
/// # 返回
///
/// 返回用于稳定渲染的三项数组。
const fn replay_outcomes() -> [ReplayMetricOutcome; 3] {
    [
        ReplayMetricOutcome::Fresh,
        ReplayMetricOutcome::Duplicate,
        ReplayMetricOutcome::BackendFailure,
    ]
}

/// 返回全部安全阶段的固定顺序。
///
/// # 返回
///
/// 返回用于稳定渲染的六项数组。
const fn metric_operations() -> [SecurityMetricOperation; 6] {
    [
        SecurityMetricOperation::Auth,
        SecurityMetricOperation::Condition,
        SecurityMetricOperation::Decrypt,
        SecurityMetricOperation::Handler,
        SecurityMetricOperation::Encrypt,
        SecurityMetricOperation::Total,
    ]
}
