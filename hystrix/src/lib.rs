//! 路由级 bulkhead 隔离、超时保护和指标流。
//!
//! 本 crate 提供可显式调用的 `Command` 运行时与 axum 中间件辅助，适合保护慢下游、
//! 热点接口和需要被 Dashboard 观测的业务入口。
//!
//! # 与受管治理面的分工
//!
//! 本 crate 支持独立 `Command`，也支持由 `ManagedRuntime` 持有目录、规则与集中周期观测。
//! Application 可通过显式配置接管该 owner；业务仍负责选择需要隔离的执行体：
//!
//! - Web 限流（单进程令牌桶 + 分布式配额）管**入站配额**；
//! - REST 发现客户端内建的 bulkhead/circuit 只对**传输级失败**（建连、超时、可分类 5xx）熔断；
//! - hystrix `Command` 包裹**业务判定的失败**(语义错误、慢成功)与非 REST 出站依赖
//!   (慢查询隔离、第三方 SDK)。
//!
//! # 受管命令的运行架构
//!
//! Prepare 将固定隔离规则、显式命令与静态属性描述安装到唯一 `ManagedRuntime`，统一周期观测。
//! 属性宏只缓存带代次的弱引用；命令可用于后续初始化、业务调用与收尾。关闭先拒绝新调用，
//! 等待在途业务、降级与观察任务退出，再撤销目录和全局引用。旧 Command 永久返回 503，下一实例
//! 使用自己的 owner；取消关闭等待不转移退出责任。命名计划、规则与目录容量冻结到启动，变化要求重启。
//! owner 存活不表示下游健康；本能力不提供错误率 Open/HalfOpen/Closed 状态机。
//!
//! # 反模式
//!
//! - **REST 调用外层再包 hystrix 时，`Command` timeout 必须大于该客户端的重试总预算**：否则内层
//!   还在跨实例重试、外层已判超时并计入结局，被放弃的重试还会继续占用连接。
//! - **包执行体而不是包提交**：`Command` 不感知任务队列语义，包裹 napart/`#[Async]` 的提交动作
//!   只测到入队耗时，超时与隔离对真实执行不生效。
//! - **命令名必须是代码常量级低基数**：拼接租户、订单等业务值会让指标序列失控。
#![recursion_limit = "512"] // hystrix snapshot_json 的 json!{} 字段多,提高宏递归上限
                            // ============================================================================

mod fallback;

pub use fallback::{
    global_fallback_installed, initialize_global_fallback, install_global_fallback, FallbackCause,
    FallbackContext, FallbackDecision, GlobalFallbackHandler, GlobalFallbackInstallError,
};
// 路由命令由信号量限制并发、由 deadline 限制执行时间，并向 Dashboard 输出滚动结局与延迟。
// 本组件不维护错误率触发的 Closed/Open/HalfOpen 状态机；下游持续失败时只由并发和超时边界
// 限制当前进程的资源消耗，`isCircuitBreakerOpen` 恒为 false，短路计数恒为零。
//
// 显式 `Command` 与配置驱动的路由匹配共享同一执行合同：并发满载立即拒绝，不在组件内排队；
// 每个命令独立维护滚动窗口和当前并发；受管模式集中周期观测，独立模式按命令启动观测。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use fallback::{execute_global_fallback, GlobalFallbackExecution};

mod managed;
pub use managed::{ManagedError, ManagedRuntime};

/// 公开构造器保持非 fallible，因此把无实际意义的极端时长收敛到安全 deadline 上限。
const MAX_COMMAND_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

// 配置驱动隔离的规则结构(原在业务 config.rs;门面 crate 自带它,业务 config 直接 use hystrix::IsolationRule)。
/// 一条隔离模式的参数(并发上限/超时/是否计 TPS)。`#[derive(Deserialize)]` 让业务 yml 能直接反序列化进来。
#[derive(Debug, Clone, serde::Deserialize)]
// 未知字段直接反序列化失败:`timeoutMs`/`maxConcurrent` 这类拼写错误若被静默忽略,
// 表现是"保护静默关闭"(0 = 不限并发/不超时),比启动失败危险得多。
#[serde(deny_unknown_fields)]
pub struct IsolationRule {
    /// 当前接口允许同时执行的最大请求数;0 表示不启用并发隔离。
    pub max_concurrent: usize,
    /// 当前接口的执行超时毫秒数;0 表示不启用超时保护。
    pub timeout_ms: u64,
    #[serde(default)]
    /// 是否计入全局 TPS,以及计入时的权重。
    pub tps_weight: Option<u64>,
}

// 滚动窗口长度：10 秒（对应 Hystrix metricsRollingStatisticalWindowInMilliseconds=10000）
const WINDOW_SECS: u64 = 10;

// ── 全局命令注册表：所有被监控的路由都注册在这，SSE 端点遍历它逐个上报 ──
static REGISTRY: OnceLock<Mutex<Vec<Arc<Command>>>> = OnceLock::new();
/// 业务作用：返回全局熔断命令表；用于集中登记和查询命令配置。
fn registry() -> &'static Mutex<Vec<Arc<Command>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// 业务作用：取全局命令表的锁;一次持锁 panic 不应让后续所有请求和 Dashboard 上报都跟着 panic。
///
/// # 参数
///
/// 本函数没有参数,返回可继续使用的命令表守卫(中毒时取回内部数据)。
fn lock_registry() -> std::sync::MutexGuard<'static, Vec<Arc<Command>>> {
    registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// 全局 TPS 从计入吞吐的各命令滚动窗口派生：
// TPS = Σ(requestCount × weight) / WINDOW_SECS。
// 与 Dashboard QPS 共用采样窗口，避免独立时钟造成统计口径不一致。

/// 业务作用：读取当前 TPS（每秒事务数）= 所有计 TPS 的命令的 requestCount(×weight) 求和 / 窗口秒数。
/// 先 clone 出命令列表立刻释放 REGISTRY 锁，再逐个 snapshot（避免攥着 registry 锁去锁 stats）。
fn tps_rate() -> f64 {
    let commands: Vec<Arc<Command>> = lock_registry().clone();
    let total: u64 = commands
        .iter()
        // 只统计标了 @TPS(tps_weight=Some) 的命令；map 把 weight 拿出来
        .filter_map(|c| {
            c.tps_weight.map(|w| {
                // requestCount = 该命令窗口内 成功+失败+超时+被拒+被取消（= 圈上那个数）
                c.stats().request_count() * w
            })
        })
        .sum();
    total as f64 / WINDOW_SECS as f64
}

// ── 一次执行的结局 ──
// 一次 run() 走完的四种归宿，决定计入哪个滚动计数（对应 Hystrix 的 rollingCountXxx）。
#[derive(Clone, Copy)]
enum Outcome {
    Success,  // 成功（下游正常返回，且非 5xx）→ rollingCountSuccess
    Failure,  // 失败（下游返回 5xx）→ rollingCountFailure
    Timeout,  // 超时（tokio timeout 触发）→ rollingCountTimeout
    Rejected, // 信号量满，bulkhead 拒绝（对应 rollingCountSemaphoreRejected）
    Canceled, // 执行 future 在产生正常结局前被丢弃/unwind（对应 rollingCountCanceled）
}

/// 降级执行面的最终结局，与主业务执行结局分轨统计。
#[derive(Clone, Copy)]
enum FallbackOutcome {
    Success,
    Failure,
}

/// 滚动窗口中的一个 1 秒统计桶。
///
/// 把一秒内的成功、失败、超时、拒绝和降级次数聚在同一桶里；窗口内最多保留 `WINDOW_SECS` 个桶。
#[derive(Default, Clone, Copy)]
struct Bucket {
    sec: u64,      // 这个桶代表的“第几秒”（now_sec 的值，作 key）
    success: u64,  // 这一秒内的成功次数
    failure: u64,  // 这一秒内的失败次数(5xx)
    timeout: u64,  // 这一秒内的超时次数
    rejected: u64, // 这一秒内被 bulkhead 拒绝的次数
    canceled: u64, // 这一秒内执行 future 被丢弃/unwind 的次数(客户端断连、外层超时、panic)
    fallback_success: u64,
    fallback_failure: u64,
}

/// Hystrix 命令的滚动统计窗口。
///
/// 保存最近 `WINDOW_SECS` 秒的计数桶和延迟样本,为熔断判断与 dashboard 快照提供基础数据。
struct Rolling {
    start: Instant,            // 窗口起点（单调时钟）
    buckets: VecDeque<Bucket>, // 按秒滑动的计数桶，队首最旧、队尾最新
    // (秒, 延迟ms)：用于算百分位；只保留窗口内、且总量封顶防膨胀
    latencies: VecDeque<(u64, u64)>,
}

impl Rolling {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    fn new() -> Self {
        Self {
            start: Instant::now(),
            buckets: VecDeque::new(),
            latencies: VecDeque::new(),
        }
    }

    /// 业务作用：返回当前时间相对窗口起点的第几秒。
    ///
    /// 该值用作桶 key,保证桶切换不依赖墙钟回拨。
    fn now_sec(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    // 丢弃滑出窗口的旧桶/旧样本
    // 参数：now = 当前秒。floor = 窗口内允许保留的最早一秒。
    ///
    /// # 参数
    /// 业务作用：- `now`: 当前时间戳,用于窗口统计和状态过期判断。
    fn evict(&mut self, now: u64) {
        let floor = now.saturating_sub(WINDOW_SECS - 1);
        // 弹出过期的计数桶
        while self.buckets.front().map(|b| b.sec < floor).unwrap_or(false) {
            self.buckets.pop_front();
        }
        // 弹出过期的延迟样本
        while self
            .latencies
            .front()
            .map(|&(s, _)| s < floor)
            .unwrap_or(false)
        {
            self.latencies.pop_front();
        }
    }

    /// 业务作用：记录一次执行结果。
    /// 参数：outcome = 本次结局（成功/失败/超时/被拒）；latency_ms = 本次耗时(毫秒)。
    ///
    /// # 参数
    /// - `outcome`: 命令、发送或任务执行结果。
    /// - `latency_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
    fn record(&mut self, outcome: Outcome, latency_ms: u64) {
        let now = self.now_sec();
        self.evict(now);
        // 取/建当前秒的桶（队尾不是当前秒就新建一个空桶）
        if self.buckets.back().map(|b| b.sec) != Some(now) {
            self.buckets.push_back(Bucket {
                sec: now,
                ..Default::default()
            });
        }
        let b = self.buckets.back_mut().unwrap();
        // 按结局把对应计数器 +1
        match outcome {
            Outcome::Success => b.success += 1,
            Outcome::Failure => b.failure += 1,
            Outcome::Timeout => b.timeout += 1,
            Outcome::Rejected => b.rejected += 1,
            Outcome::Canceled => b.canceled += 1,
        }
        // 只有跑完整段执行的才有延迟意义:被拒没进执行区,被取消没有完整耗时
        if !matches!(outcome, Outcome::Rejected | Outcome::Canceled) {
            self.latencies.push_back((now, latency_ms));
            // 封顶：窗口内样本最多 5000 条，防突发流量把内存撑大
            while self.latencies.len() > 5000 {
                self.latencies.pop_front();
            }
        }
    }

    /// 业务作用：记录一次降级执行结局，与主请求的拒绝或超时结局保持独立口径。
    ///
    /// # 参数说明
    ///
    /// - `outcome`: 局部或全局降级最终成功、失败或被自身资源边界拒绝的结局。
    ///
    /// # 返回
    ///
    /// 无返回值；当前秒滚动桶对应计数增加一次。
    fn record_fallback(&mut self, outcome: FallbackOutcome) {
        let now = self.now_sec();
        self.evict(now);
        if self.buckets.back().map(|b| b.sec) != Some(now) {
            self.buckets.push_back(Bucket {
                sec: now,
                ..Default::default()
            });
        }
        let bucket = self.buckets.back_mut().unwrap();
        match outcome {
            FallbackOutcome::Success => bucket.fallback_success += 1,
            FallbackOutcome::Failure => bucket.fallback_failure += 1,
        }
    }

    /// 业务作用：汇总当前滚动窗口的主请求与降级结局，作为 Dashboard 单一事实源。
    ///
    /// 会先驱逐过期桶和样本,再把窗口内计数相加并计算延迟百分位,供熔断判断和 `/hystrix.stream` 输出。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回当前有效窗口内的计数和延迟分位快照。
    fn snapshot(&mut self) -> WindowSum {
        let now = self.now_sec();
        self.evict(now);
        let mut sum = WindowSum::default();
        // 累加窗口内所有桶的各类计数
        for b in &self.buckets {
            sum.success += b.success;
            sum.failure += b.failure;
            sum.timeout += b.timeout;
            sum.rejected += b.rejected;
            sum.canceled += b.canceled;
            sum.fallback_success += b.fallback_success;
            sum.fallback_failure += b.fallback_failure;
        }
        // 延迟百分位：取出窗口内全部延迟样本，排序后算分位
        let mut lat: Vec<u64> = self.latencies.iter().map(|&(_, ms)| ms).collect();
        lat.sort_unstable();
        sum.latency = Percentiles::from_sorted(&lat);
        sum
    }

    /// 业务作用：只累加窗口内的请求总数,不收集也不排序延迟样本。
    ///
    /// TPS 只需要"这个圈窗口内跑了多少次",走 `snapshot()` 会白白复制并排序最多 5000 个延迟样本;
    /// 命令多时 `tps_rate()` 每次调用的开销会随命令数线性放大。
    ///
    /// # 参数
    ///
    /// 本函数没有参数,返回当前滚动窗口内的请求总数。
    fn request_count(&mut self) -> u64 {
        let now = self.now_sec();
        self.evict(now);
        self.buckets
            .iter()
            .map(|b| b.success + b.failure + b.timeout + b.rejected + b.canceled)
            .sum()
    }
}

/// 一次滚动窗口汇总的结果。
///
/// 这是 `snapshot()` 的产物,随后被转换为 Hystrix Dashboard 兼容的 JSON 指标。
#[derive(Default)]
struct WindowSum {
    success: u64,          // 窗口内成功总数
    failure: u64,          // 窗口内失败(5xx)总数
    timeout: u64,          // 窗口内超时总数
    rejected: u64,         // 窗口内被 bulkhead 拒绝总数
    canceled: u64,         // 窗口内执行被取消/中止总数
    fallback_success: u64, // 窗口内产出降级响应总数(FALLBACK_SUCCESS)
    fallback_failure: u64, // 窗口内全局降级配置冲突、panic 或递归总数
    latency: Percentiles,  // 窗口内延迟百分位
}

/// 延迟百分位集合。
///
/// Hystrix Dashboard 需要 0/25/50/75/90/95/99/99.5/100 分位和平均值,这里集中保存一次窗口快照的结果。
#[derive(Default)]
struct Percentiles {
    mean: u64, // 平均延迟（ms）
    p0: u64,   // 最小值（0 分位）
    p25: u64,  // 25 分位
    p50: u64,  // 中位数
    p75: u64,  // 75 分位
    p90: u64,  // 90 分位
    p95: u64,  // 95 分位
    p99: u64,  // 99 分位
    p995: u64, // 99.5 分位
    p100: u64, // 最大值（100 分位）
}

impl Percentiles {
    /// 业务作用：从【已升序排序】的延迟样本算出各分位。
    /// 参数：sorted = 升序排好的延迟数组(ms)。空数组直接返回全 0（default）。
    ///
    /// # 参数
    /// - `sorted`: 已按时间或优先级排序的统计样本。
    fn from_sorted(sorted: &[u64]) -> Self {
        if sorted.is_empty() {
            return Self::default();
        }
        // 闭包 pick：给一个百分比 p，用“最近秩法”取对应分位值
        let pick = |p: f64| -> u64 {
            // 最近秩法取分位：idx = round(p% * (n-1))，再夹到合法下标范围
            let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
            sorted[idx.min(sorted.len() - 1)]
        };
        // 平均值 = 总和 / 样本数
        let mean = (sorted.iter().sum::<u64>()) / (sorted.len() as u64);
        Self {
            mean,
            p0: sorted[0],
            p25: pick(25.0),
            p50: pick(50.0),
            p75: pick(75.0),
            p90: pick(90.0),
            p95: pick(95.0),
            p99: pick(99.0),
            p995: pick(99.5),
            p100: sorted[sorted.len() - 1],
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Command —— 一条被隔离 + 监控的路由
// ════════════════════════════════════════════════════════════════════════════
/// 一条被 bulkhead、超时和滚动指标保护的命令。
///
/// `Command` 通常对应一个接口路由或一个配置匹配模式。它会自动注册到全局表,供 `/hystrix.stream`
/// 输出 Dashboard 指标,同时也负责 CostTime 风格的周期延迟日志。
pub struct Command {
    owner: Option<std::sync::Weak<managed::ManagedState>>,
    admitted: std::sync::atomic::AtomicBool,
    name: String,  // Dashboard 里那个圈的标题（HystrixCommand name）
    group: String, // 归类名（command group / threadPool 名，圈会按 group 分组）
    // 并发上限。None = 不限并发（不建信号量、永不 429），用于"只看监控"。Some(n) = bulkhead 容量 n。
    max_concurrent: Option<usize>,
    // 单请求超时。None = 不超时（跳过 timeout 包裹，永不 504）。Some(d) = 超时时长。
    timeout: Option<Duration>,
    // bulkhead 本体。None = 不限并发时不建信号量；Some(sem) 许可数 = max_concurrent。
    sem: Option<tokio::sync::Semaphore>,
    concurrent: AtomicI64, // 当前并发(gauge)：进入 +1、离开 -1
    // 进程生命周期内的并发峰值(fetch_max 只增不减,无窗口衰减)。喂 rollingMaxConcurrentExecutionCount
    // ——注意与真实 Hystrix 语义偏离:原版是滚动窗口内峰值(随窗口回落),这里一次尖峰会永久显示;
    // 当前窗口按总样本聚合，不维护 per-bucket max；调用方不能据此推断单桶峰值。
    rolling_max_concurrent: AtomicI64,
    stats: Mutex<Rolling>, // 滚动窗口统计（成功/失败/超时/拒绝 + 延迟），加锁访问
    // None 不计入全局 TPS；Some(weight) 按权重计入同一窗口的请求吞吐。
    tps_weight: Option<u64>,
    // 延迟日志的可选附加字段生成器，接收统计周期毫秒数。
    // OnceLock 保证构造后只安装一次，Send + Sync 允许周期任务跨线程调用。
    extra: OnceLock<Box<dyn Fn(u64) -> String + Send + Sync>>,
    // 日志使用路由模板，Dashboard 使用命令名；两者的展示含义不同。
    // 路径只允许设置一次，未设置时日志回退到命令名。
    path: OnceLock<String>,
    // 【自定义限流返回】#[hystrix(reject_response = "{...}")] 设的 JSON body：bulkhead 满时返回它(HTTP 200)。
    //   None(未设) → 回退默认 429 壳 rejected_response。OnceLock：构造后由 set_reject_response_str 补设。
    reject_body: OnceLock<Value>,
    // 【自定义超时返回】#[hystrix(timeout_response = "{...}")] 设的 JSON body：超时时返回它(HTTP 200)。
    //   None(未设) → 回退默认 504 壳 timeout_response。
    timeout_body: OnceLock<Value>,
    // 【自定义限流降级 fn】#[hystrix(reject_fn = path)] 设:bulkhead 满时调它产出 Response。
    //   优先级高于 reject_body;None → 回退 reject_body → 默认 429 壳。由 set_reject_fn 补设。
    reject_fb: OnceLock<FallbackFn>,
    // 【自定义超时降级 fn】#[hystrix(timeout_fn = path)] 设:超时时调它产出 Response。
    //   优先级高于 timeout_body;None → 回退 timeout_body → 默认 504 壳。由 set_timeout_fn 补设。
    timeout_fb: OnceLock<FallbackFn>,
}

/// 降级 fn 槽类型:无捕获、可多次调用(每个被拒/超时请求各产一个 future),故 Fn + Send + Sync。
/// 产出 Response 的 future。由宏生成的 set_reject_fn/set_timeout_fn 注入(reject_fn/timeout_fn 路径)。
type FallbackFn = Box<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> + Send + Sync,
>;

/// 并发 gauge 的 RAII 守卫:构造 +1 并抬峰值,Drop 时 -1。
/// 用 RAII 而非手动 fetch_sub——请求被取消时(客户端断连,run_fn future 被 drop)Drop 仍会归还,
/// 不会像旧实现那样让 `currentConcurrentExecutionCount` 单调泄漏虚高(信号量 permit 本就 RAII,不受影响)。
struct ConcurrentGauge<'a> {
    command: &'a Command,
    completed: bool,
}

impl<'a> ConcurrentGauge<'a> {
    /// 业务作用：进入命令执行区并递增当前并发数。
    ///
    /// # 参数
    /// - `command`: 本次执行所属命令(并发 gauge、峰值和滚动窗口都挂在它上面)。
    fn enter(command: &'a Command) -> Self {
        let cur = command.concurrent.fetch_add(1, Ordering::Relaxed) + 1;
        command
            .rolling_max_concurrent
            .fetch_max(cur, Ordering::Relaxed);
        Self {
            command,
            completed: false,
        }
    }

    /// 业务作用：标记本次执行已产生 success/failure/timeout 结局,Drop 时不再补记 canceled。
    ///
    /// # 参数
    ///
    /// 本函数没有参数,只翻转守卫内部的完成标记。
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for ConcurrentGauge<'_> {
    /// 业务作用：离开命令执行区时归还当前并发计数;未产生结局的(客户端断连、外层超时丢弃、panic
    /// unwind)补记一次 canceled,避免这些请求在 QPS 与错误率里凭空消失。
    fn drop(&mut self) {
        self.command.concurrent.fetch_sub(1, Ordering::Relaxed);
        if !self.completed {
            self.command.stats().record(Outcome::Canceled, 0);
        }
    }
}

impl Command {
    /// 业务作用：取本命令滚动窗口的锁;一次持锁 panic 不应让该端点后续所有请求都跟着 panic。
    ///
    /// # 参数
    ///
    /// 本函数没有参数,返回可继续使用的滚动窗口守卫(中毒时取回内部数据)。
    fn stats(&self) -> std::sync::MutexGuard<'_, Rolling> {
        self.stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 业务作用：统一构造隔离命令，将运行实例登记到当前受管代次或独立进程目录。
    /// 参数说明：`name`、`group` 为固定身份；`max_concurrent`、`timeout` 为执行边界；`tps_weight` 为统计权重。
    /// 返回：独立模式按原有方式归一化参数；受管模式身份、容量或边界不合法时返回只拒绝执行的命令。
    fn build(
        name: &str,
        group: &str,
        max_concurrent: Option<usize>, // None = 不限并发
        timeout: Option<Duration>,     // None = 不超时
        tps_weight: Option<u64>,
    ) -> Arc<Self> {
        // ── 0 值归一化(所有构造入口的唯一收口:new / with_tps / monitor / 配置驱动 dispatch)──
        // yml 的 IsolationRule 与注解都把 0 记作"不启用",但 Some(0) 会建出 0 许可信号量
        // (每个请求都 429)和 0 时长超时(任何带 await 的 handler 立刻 504),与文档相反。
        // 统一在这里折成 None,让"不启用"就是真的不启用。
        let max_concurrent = max_concurrent
            .filter(|limit| *limit > 0)
            .map(|limit| limit.min(tokio::sync::Semaphore::MAX_PERMITS));
        let timeout = timeout
            .filter(|duration| !duration.is_zero())
            .map(|duration| duration.min(MAX_COMMAND_TIMEOUT));
        // 构造与 owner 安装/撤销串行，避免运行命令被登记到错误代次。
        let owner = managed::lock_owner();
        let cmd = Arc::new(Self {
            owner: owner.as_ref().map(Arc::downgrade),
            admitted: std::sync::atomic::AtomicBool::new(true),
            name: name.to_string(),
            group: group.to_string(),
            max_concurrent,
            timeout,
            // 有上限才建信号量(许可数=上限)；不限并发(None)则不建，run_fn 里直接放行
            sem: max_concurrent.map(tokio::sync::Semaphore::new),
            concurrent: AtomicI64::new(0),
            rolling_max_concurrent: AtomicI64::new(0),
            stats: Mutex::new(Rolling::new()),
            tps_weight,
            extra: OnceLock::new(), // extra 钩子默认空，需要时由 set_extra 补设
            path: OnceLock::new(),  // 路由默认空，需要时由 set_path 补设；未设则日志回退用 name
            reject_body: OnceLock::new(), // 自定义限流返回默认空，需要时由 set_reject_response_str 补设
            timeout_body: OnceLock::new(), // 自定义超时返回默认空，需要时由 set_timeout_response_str 补设
            reject_fb: OnceLock::new(),    // 限流降级 fn 默认空，需要时由 set_reject_fn 补设
            timeout_fb: OnceLock::new(),   // 超时降级 fn 默认空，需要时由 set_timeout_fn 补设
        });
        {
            let mut table = lock_registry();
            if let Some(owner) = owner.as_ref() {
                // 受管目录有界且同名唯一，旧代构造不能绕过关闭门禁创建新周期工作。
                if !owner.is_open()
                    || name.is_empty()
                    || name.trim() != name
                    || name.len() > 128
                    || group.is_empty()
                    || group.trim() != group
                    || group.len() > 128
                    || max_concurrent.is_some_and(|limit| limit > 65536)
                    || timeout.is_some_and(|duration| duration > Duration::from_secs(3600))
                    || table.len() >= owner.limit
                    || table
                        .iter()
                        .any(|command| command.name == name && command.group == group)
                {
                    cmd.admitted.store(false, Ordering::Release);
                    return cmd;
                }
            } else if table
                .iter()
                .any(|command| command.name == name && command.group == group)
            {
                tracing::warn!("duplicate standalone hystrix command identity");
            }
            table.push(cmd.clone());
        }
        let standalone = owner.is_none();
        drop(owner);

        // 每个命令的日志周期锚定创建时刻，避免全部路由同时集中输出。
        // 任务只持有 Weak，命令释放后自行退出；没有 Tokio runtime 时不启动任务。
        if standalone && tokio::runtime::Handle::try_current().is_ok() {
            let weak = Arc::downgrade(&cmd);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
                ticker.tick().await; // 吃掉立即触发的首拍（interval 首拍在 t0）→ 首打在 t0+10s（对齐 TimingWheel 初始 delay=PERIOD）
                loop {
                    ticker.tick().await; // t0+10s、t0+20s …（相位锚定本命令的创建时刻）
                    match weak.upgrade() {
                        Some(c) => c.log_cost(), // 命令还在 → 打一行（窗口内无样本则 log_cost 内部跳过）
                        None => break,           // 命令已被回收 → 任务退出
                    }
                }
            });
        }
        cmd
    }

    /// 业务作用：创建命令并【自动注册】到全局表（SSE 端点据此上报）。
    /// name 会成为 Dashboard 里那个圈的标题；group 用于归类。
    /// 本入口创建的命令不计入全局 TPS。
    ///
    /// # 参数
    /// - `name`: Dashboard 圈标题和默认日志标签。
    /// - `group`: Dashboard 分组名。
    /// - `max_concurrent`: 并发上限,会作为信号量许可数。
    /// - `timeout`: 单请求执行超时时长。
    pub fn new(name: &str, group: &str, max_concurrent: usize, timeout: Duration) -> Arc<Self> {
        // 这两个构造器收【具体值】，包成 Some 传给 build —— 行为和重构前完全一致（硬编码/配置驱动路由用这俩）。
        Self::build(name, group, Some(max_concurrent), Some(timeout), None)
    }

    /// 业务作用：【最通用构造器】并发上限/超时都可为 None（None = 不限并发 / 不超时），tps_weight 也可 None。
    /// 给 #[hystrix] 属性宏用：注解里不写 max_concurrent → None(不限)，不写 timeout_ms → None(不超时)，
    ///   `#[hystrix()]` 空参 → 全 None = 只采集监控指标、不做任何拦截/限时。
    ///
    /// # 参数
    /// - `name`: Dashboard 圈标题和默认日志标签。
    /// - `group`: Dashboard 分组名。
    /// - `max_concurrent`: 可选并发上限;`None` 表示不做 bulkhead 限流。
    /// - `timeout`: 可选单请求超时;`None` 表示不做超时包裹。
    /// - `tps_weight`: 可选 TPS 权重;`None` 表示不计入全局 TPS。
    pub fn monitor(
        name: &str,
        group: &str,
        max_concurrent: Option<usize>,
        timeout: Option<Duration>,
        tps_weight: Option<u64>,
    ) -> Arc<Self> {
        Self::build(name, group, max_concurrent, timeout, tps_weight)
    }

    /// 业务作用：创建计入全局 TPS 的命令，按 weight 加权统计每个请求。
    /// 只有用本构造器创建的命令，其请求才会累加到顶栏 TPS；其余命令对 TPS 贡献为 0。
    ///
    /// # 参数
    /// - `name`: Dashboard 圈标题和默认日志标签。
    /// - `group`: Dashboard 分组名。
    /// - `max_concurrent`: 并发上限,会作为信号量许可数。
    /// - `timeout`: 单请求执行超时时长。
    /// - `weight`: 每个请求给全局 TPS 累加的权重。
    pub fn with_tps(
        name: &str,
        group: &str,
        max_concurrent: usize,
        timeout: Duration,
        weight: u64,
    ) -> Arc<Self> {
        Self::build(
            name,
            group,
            Some(max_concurrent),
            Some(timeout),
            Some(weight),
        )
    }

    /// 业务作用：【融合自 CostTime.extra】给本命令挂一个"附加信息生成器"，每 10s 打延迟日志时拼到行尾。
    /// 参数：
    ///   f —— 闭包 `Fn(period_ms: u64) -> String`：入参是统计周期毫秒数（= WINDOW_SECS*1000），
    ///        返回一段要追加到日志行尾的文本（如自定义吞吐/计数指标）。Send+Sync+'static 因为
    ///        它会被后台定时任务在别的线程里调用、且要活到进程结束。
    /// 只能设一次（OnceLock）；重复设无效（忽略）。在 main.rs 构造完命令后调用即可，例如：
    ///   `kline_cmd.set_extra(|_p| format!("tps={:.0}/s", hystrix::current_tps()));`
    ///
    /// # 参数
    /// - `f`: 周期日志的附加信息生成闭包,入参为统计周期毫秒数。
    pub fn set_extra<F>(&self, f: F)
    where
        F: Fn(u64) -> String + Send + Sync + 'static,
    {
        // set 返回 Result：已设过会 Err，这里忽略（保持"只设一次"语义）
        let _ = self.extra.set(Box::new(f));
    }

    /// 业务作用：设置本命令的真实接口路由（如 "/spot/kline"），只影响 CostTime 定时日志的显示。
    /// 不设则 log_cost 回退用 name（Dashboard 圈标题）。Dashboard 显示不受影响（它读 name）。
    ///
    /// # 参数
    /// - `path`: 真实接口路由字符串,用于周期延迟日志展示。
    pub fn set_path(&self, path: &str) {
        let _ = self.path.set(path.to_string());
    }

    /// 业务作用：【自定义限流返回】设 bulkhead 满时返回的 JSON body(随后以 HTTP 200 返回)。
    /// 由 `#[hystrix(reject_response = "...")]` 在构造时调用(宏已在编译期校验过 JSON);传入【合法 JSON 字符串】。
    /// **非法 JSON 会被静默忽略**(回退默认 429 壳)——直接调用方若要感知失败,请用 [`try_set_reject_response_str`]。只设一次。
    ///
    /// [`try_set_reject_response_str`]: Self::try_set_reject_response_str
    ///
    /// # 参数
    /// - `json`: bulkhead 拒绝时返回的 JSON body 字符串。
    pub fn set_reject_response_str(&self, json: &str) {
        let _ = self.try_set_reject_response_str(json);
    }

    /// 业务作用：同 [`set_reject_response_str`],但**返回结果供直接调用方感知**:JSON 非法 → `Err`;合法且本次设入 → `Ok(true)`;
    /// 合法但**之前已设过**(OnceLock 只设一次,本次未覆盖)→ `Ok(false)`。
    ///
    /// [`set_reject_response_str`]: Self::set_reject_response_str
    ///
    /// # 参数
    /// - `json`: bulkhead 拒绝时返回的 JSON body 字符串。
    pub fn try_set_reject_response_str(&self, json: &str) -> Result<bool, serde_json::Error> {
        let v = serde_json::from_str::<Value>(json)?;
        Ok(self.reject_body.set(v).is_ok()) // set: Ok(())=本次设入 / Err(v)=已设过
    }

    /// 业务作用：【自定义超时返回】设超时时返回的 JSON body(随后以 HTTP 200 返回)。用法同 [`set_reject_response_str`]。
    /// 非法 JSON 静默忽略;要感知失败用 [`try_set_timeout_response_str`]。
    ///
    /// [`set_reject_response_str`]: Self::set_reject_response_str
    /// [`try_set_timeout_response_str`]: Self::try_set_timeout_response_str
    ///
    /// # 参数
    /// - `json`: 请求超时时返回的 JSON body 字符串。
    pub fn set_timeout_response_str(&self, json: &str) {
        let _ = self.try_set_timeout_response_str(json);
    }

    /// 业务作用：同 [`set_timeout_response_str`],但**返回结果**:JSON 非法 → `Err`;本次设入 → `Ok(true)`;已设过 → `Ok(false)`。
    ///
    /// [`set_timeout_response_str`]: Self::set_timeout_response_str
    ///
    /// # 参数
    /// - `json`: 请求超时时返回的 JSON body 字符串。
    pub fn try_set_timeout_response_str(&self, json: &str) -> Result<bool, serde_json::Error> {
        let v = serde_json::from_str::<Value>(json)?;
        Ok(self.timeout_body.set(v).is_ok())
    }

    /// 业务作用：【自定义限流降级 fn】设 bulkhead 满时调用的降级闭包(产出 Response)。
    /// 由 `#[hystrix(reject_fn = path)]` 在构造时调用;优先级高于 reject_response;只设一次(OnceLock)。
    ///
    /// # 参数
    /// - `fb`: bulkhead 拒绝时执行的异步降级响应闭包。
    pub fn set_reject_fn(&self, fb: FallbackFn) {
        let _ = self.reject_fb.set(fb);
    }

    /// 业务作用：【自定义超时降级 fn】设超时时调用的降级闭包(产出 Response)。用法同 set_reject_fn。
    ///
    /// # 参数
    /// - `fb`: 请求超时时执行的异步降级响应闭包。
    pub fn set_timeout_fn(&self, fb: FallbackFn) {
        let _ = self.timeout_fb.set(fb);
    }

    /// 业务作用：按“局部函数、局部静态响应、全局处理器、内置响应”的固定顺序解析一次降级。
    ///
    /// # 参数说明
    ///
    /// - `cause`: 已完成主结局记账的并发拒绝或执行超时原因。
    ///
    /// # 返回
    ///
    /// 返回最终 HTTP 响应；全局处理器配置冲突、崩溃或递归时安全回退到内置响应。
    async fn resolve_fallback(&self, cause: FallbackCause) -> Response {
        match cause {
            FallbackCause::BulkheadRejected { .. } => {
                if let Some(fallback) = self.reject_fb.get() {
                    let response = fallback().await;
                    self.stats().record_fallback(FallbackOutcome::Success);
                    return response;
                }
                if let Some(body) = self.reject_body.get() {
                    self.stats().record_fallback(FallbackOutcome::Success);
                    return custom_response(body);
                }
            }
            FallbackCause::ExecutionTimeout { .. } => {
                if let Some(fallback) = self.timeout_fb.get() {
                    let response = fallback().await;
                    self.stats().record_fallback(FallbackOutcome::Success);
                    return response;
                }
                if let Some(body) = self.timeout_body.get() {
                    self.stats().record_fallback(FallbackOutcome::Success);
                    return custom_response(body);
                }
            }
        }

        let path = self.path.get().map(String::as_str).unwrap_or(&self.name);
        let context = FallbackContext::new(&self.name, &self.group, path, self.tps_weight, cause);
        match execute_global_fallback(context) {
            GlobalFallbackExecution::Handled(response) => {
                self.stats().record_fallback(FallbackOutcome::Success);
                return response;
            }
            GlobalFallbackExecution::UseBuiltin | GlobalFallbackExecution::NotInstalled => {
                self.stats().record_fallback(FallbackOutcome::Success);
            }
            GlobalFallbackExecution::InvalidConfiguration => {
                self.stats().record_fallback(FallbackOutcome::Failure);
                tracing::error!(
                    command = %self.name,
                    ?cause,
                    "全局降级配置冲突，使用内置终态响应"
                );
            }
            GlobalFallbackExecution::Panicked => {
                self.stats().record_fallback(FallbackOutcome::Failure);
                tracing::error!(
                command = %self.name,
                ?cause,
                    "全局降级处理器发生 panic，使用内置终态响应"
                );
            }
            GlobalFallbackExecution::Recursive => {
                self.stats().record_fallback(FallbackOutcome::Failure);
                tracing::warn!(
                    command = %self.name,
                    ?cause,
                    "全局降级发生递归调用，使用内置终态响应"
                );
            }
        }

        match cause {
            FallbackCause::BulkheadRejected { max_concurrent, .. } => {
                rejected_response(&self.name, max_concurrent)
            }
            FallbackCause::ExecutionTimeout { timeout, .. } => {
                timeout_response(&self.name, timeout)
            }
        }
    }

    /// 业务作用：按命令周期输出最近窗口的延迟聚合日志。
    /// 参数说明：无。
    /// 返回：有样本时输出统计；不清空滚动窗口，以免破坏共用窗口的 SSE 观测。
    fn log_cost(&self) {
        // 取最近窗口汇总（含各结局计数 + 延迟百分位）
        let w = self.stats().snapshot();
        // count = 真正执行过的样本数（成功+失败+超时；被拒的没执行、无延迟，不计）——对齐 min/avg/max 的样本集
        let count = w.success + w.failure + w.timeout;
        // 窗口内没有样本时不输出，避免把空窗口当作新的延迟证据。
        if count == 0 {
            return;
        }
        let lat = &w.latency;
        // extra 钩子：有就调它生成附加文本（入参=周期毫秒），没有就空串
        let extra = self
            .extra
            .get()
            .map(|f| f(WINDOW_SECS * 1000))
            .unwrap_or_default();
        // 被拒数 >0 时附带显示（bulkhead 在限流的信号），=0 则不打扰
        let rejected = if w.rejected > 0 {
            format!(" rejected {}", w.rejected)
        } else {
            String::new()
        };
        // 被取消数 >0 时附带显示(客户端断连/外层超时/panic 的信号),=0 则不打扰
        let canceled = if w.canceled > 0 {
            format!(" canceled {}", w.canceled)
        } else {
            String::new()
        };
        // 标识符：优先用真实路由(set_path 设的，如 "/spot/kline")，没设则回退 name(Dashboard 标题)
        let label = self.path.get().map(|s| s.as_str()).unwrap_or(&self.name);
        // 延迟日志格式：`<url> <count>次/<10>s min <min> avg <avg> max <max> (ms) <extra>`
        //   min = p0（窗口最小延迟）、avg = mean（均值）、max = p100（窗口最大延迟）
        tracing::info!(
            "{} {}次/{}s min {} avg {} max {} (ms){}{}{}",
            label,
            count,
            WINDOW_SECS,
            lat.p0,
            lat.mean,
            lat.p100,
            rejected,
            canceled,
            if extra.is_empty() {
                String::new()
            } else {
                format!(" {extra}")
            },
        );
    }

    /// 业务作用：作为中间件包裹一次请求：bulkhead + 超时 + 指标记录。
    /// 用法见 main.rs：`axum::middleware::from_fn(move |req, next| cmd.clone().run(req, next))`
    /// 参数：
    ///   `self: Arc<Self>` —— 用 Arc 接收者(而非 &self)，因为同一个 Command 会被多个并发请求/
    ///                       SSE 端点共享；Arc 让闭包能 move 一份并跨 .await 持有所有权。
    ///   req  —— axum 的 Request（本次进来的 HTTP 请求）。
    ///   next —— axum 的 Next 放行句柄；next.run(req) 把请求交给下游 handler/中间件链。
    /// 【中间件形态】用本命令保护下游 (req, next)。
    /// 现在它只是 run_fn 的【薄封装】：把 `next.run(req)` 这个"下游 future"包成闭包交给 run_fn。
    /// /heavy/slow 的 `.layer(... cmd.run)` 和配置驱动 dispatch 都走这里，行为与重构前完全一致。
    ///
    ///
    /// # 参数
    /// - `req`: HTTP 请求对象。
    /// - `next`: 下一个中间件或后续处理器。
    pub async fn run(self: Arc<Self>, req: Request, next: Next) -> Response {
        // move 把 next、req 一起搬进闭包；闭包只有在 run_fn 里【抢到许可后】才被调用 → 才真正执行下游。
        self.run_fn(move || next.run(req)).await
    }

    /// 业务作用：【通用执行入口】用本命令的 bulkhead + 超时保护【任意 async 执行体】，并把
    /// 成功/失败/超时/拒绝记入滚动窗口（→ /hystrix.stream → Dashboard）。
    ///
    /// 为什么需要它：`run` 是中间件签名 `(req, next)`，只适合 `.layer(...)`。而 `#[hystrix]` 属性宏
    ///   贴的是【普通 handler】（没有 Next），原来只能在宏里自带一套 Semaphore+timeout，结果【不上 Dashboard】。
    ///   抽出 run_fn 后，宏把"原函数体"包成闭包传进来 → 注解版也走真正的 Command → 指标自然流进 Dashboard。
    ///
    /// 参数 f：`FnOnce() -> Future<Output = Response>`，即"要被保护的那段执行体"。
    ///   只有 try_acquire 抢到许可后才会调用 f()（拿不到许可直接 429，f 根本不执行）。
    ///
    /// # 参数说明
    ///
    /// - `f`: 被当前命令保护的异步执行体,只有通过 bulkhead 后才会被调用。
    ///
    /// # 返回
    ///
    /// 返回业务响应、局部降级响应、全局降级响应或组件内置拒绝/超时响应。
    pub async fn run_fn<F, Fut>(self: Arc<Self>, f: F) -> Response
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Response>,
    {
        // 关闭后的旧命令不能因下一代全局安装而复活；守卫覆盖业务和降级的完整执行。
        if !self.admitted.load(Ordering::Acquire) {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        let _owner_call = match &self.owner {
            Some(owner) => match owner.upgrade().and_then(|owner| owner.enter().ok()) {
                Some(guard) => Some(guard),
                None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            },
            None => None,
        };
        // 注：不再在此 tps_hit。TPS 改为从下面记的 requestCount 派生（见 tps_rate），
        //   既和圈/QPS 同源、对得上，又免去热路径上抢全局 TPS 锁。tps_weight 仅作"是否计入 TPS"的标记。

        // ── 1. bulkhead（仅当有并发上限时）：try_acquire 拿不到许可立刻拒绝（不排队，钉死队列长度=0）──
        // self.sem 为 None（不限并发，如 #[hystrix()] 只看监控）→ 不抢许可、直接放行（permit=None）。
        // Some(sem) → try_acquire 非阻塞：有空许可就拿到 permit，满了立即 Err（不像 acquire 会排队等）。
        let permit = match &self.sem {
            None => None, // 不限并发：不做 bulkhead
            Some(sem) => match sem.try_acquire() {
                Ok(p) => Some(p),
                Err(_) => {
                    // 主请求只记录 Rejected；降级执行结局在局部/全局策略真正完成后单独记账，避免失败误报成功。
                    self.stats().record(Outcome::Rejected, 0);
                    let max = self.max_concurrent.unwrap_or(0);
                    tracing::warn!(
                        command = %self.name,
                        max = max,
                        "bulkhead full, rejecting request (429)"
                    );
                    let inflight = self.concurrent.load(Ordering::Relaxed).max(0) as usize;
                    return self
                        .resolve_fallback(FallbackCause::BulkheadRejected {
                            max_concurrent: max,
                            current_inflight: inflight,
                        })
                        .await;
                }
            },
        };

        // ── 2. 并发 gauge +1(RAII:进入 +1、离开/取消 -1),并刷新峰值 ──
        // gauge 在本方法作用域结束或【future 被取消 drop】时都会 fetch_sub,不会泄漏虚高计数;
        // 未产生结局就被丢弃时,它还会补记一次 canceled(见 ConcurrentGauge::drop)。
        let mut gauge = ConcurrentGauge::enter(&self);

        // ── 3. 执行下游（handler），可选限时 ──
        let start = Instant::now();
        // self.timeout 为 Some(d) → tokio::time::timeout 包住 f()：超过 d 还没完成就【丢弃】该 Future 并返回 Err(Elapsed)。
        // self.timeout 为 None（不超时，如 #[hystrix()] 只看监控）→ 直接 await，包成 Ok 以统一下面的 match 类型。
        let result = match self.timeout {
            Some(d) => tokio::time::timeout(d, f()).await,
            None => Ok(f().await),
        };
        let elapsed = start.elapsed();
        let latency_ms = elapsed.as_millis() as u64;

        // ── 4. 释放许可与并发计数（降级不占 bulkhead 容量，也不算业务执行并发）──
        // drop(permit) 显式归还信号量许可（RAII：即便不写也会在作用域结束自动还，这里提前释放）
        drop(permit);
        // 本次已经跑出结局(下面立刻记 success/failure/timeout),标记完成后 Drop 不再补 canceled。
        gauge.complete();
        drop(gauge);

        // ── 5. 记录结局 ──
        match result {
            Ok(resp) => {
                // 5xx 视为失败，其余视为成功（含 4xx 业务错误，按 Hystrix 习惯不计入熔断失败）
                let outcome = if resp.status().is_server_error() {
                    Outcome::Failure
                } else {
                    Outcome::Success
                };
                self.stats().record(outcome, latency_ms);
                tracing::debug!("⏱ [{}] latency={}ms", self.name, latency_ms);
                resp
            }
            // Err(Elapsed) = 超时分支：下游未在 timeout 内完成，记 Timeout 并返回 504。
            // 只有 self.timeout=Some(d) 时才可能走到这里（None 永远返回 Ok），故 unwrap_or 只是防御。
            Err(_elapsed) => {
                let dur = self.timeout.unwrap_or_default();
                // 主请求只记录 Timeout；降级执行结局在局部/全局策略真正完成后单独记账。
                self.stats().record(Outcome::Timeout, latency_ms);
                tracing::warn!(
                    command = %self.name,
                    timeout_ms = dur.as_millis() as u64,
                    "command timed out (504)"
                );
                self.resolve_fallback(FallbackCause::ExecutionTimeout {
                    timeout: dur,
                    elapsed,
                })
                .await
            }
        }
    }

    /// 业务作用：生成包含主请求与全局降级真实结局的 Hystrix Dashboard 命令快照。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回 Dashboard 可消费的 `HystrixCommand` JSON，不改变运行时状态。
    fn snapshot_json(&self) -> Value {
        // 取窗口汇总：成功/失败/超时/拒绝计数 + 延迟百分位
        let w = self.stats().snapshot();
        // requestCount = 窗口内全部请求 = 成功 + 失败 + 超时 + 被拒 + 被取消
        let request_count = w.success + w.failure + w.timeout + w.rejected + w.canceled;
        // errorCount = 非成功的总数 = 失败 + 超时 + 被拒 + 被取消
        let error_count = w.failure + w.timeout + w.rejected + w.canceled;
        // errorPercentage = error_count / request_count * 100（无请求时为 0，防除零）
        // checked_div:无请求时为 0,防除零(clippy manual_checked_div)。
        let error_pct = (error_count * 100).checked_div(request_count).unwrap_or(0) as i64;
        // currentTime：当前 Unix 毫秒时间戳（Dashboard 用来判断数据新鲜度）
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let lat = &w.latency;
        // 延迟分位 map：key 是百分位字符串，value 是对应延迟(ms)。execute/total 共用这一份
        let lat_json = json!({
            "0": lat.p0, "25": lat.p25, "50": lat.p50, "75": lat.p75,
            "90": lat.p90, "95": lat.p95, "99": lat.p99, "99.5": lat.p995, "100": lat.p100
        });

        json!({
            "type": "HystrixCommand",       // 数据类型，Dashboard 据此识别这是一条命令快照
            "name": self.name,              // 圈标题
            "group": self.group,            // 归类（Dashboard 按它分组）
            "currentTime": now_ms,          // 当前时间戳(ms)
            "isCircuitBreakerOpen": false,  // 熔断器是否打开（本实现不做熔断，恒 false）

            // 滚动计数（窗口内）
            "errorPercentage": error_pct,                    // 错误率(%)：error_count/request_count*100
            "errorCount": error_count,                       // 错误总数 = 失败+超时+被拒
            "requestCount": request_count,                   // 请求总数 = 成功+失败+超时+被拒+被取消
            "rollingCountSuccess": w.success,                // 成功数
            "rollingCountFailure": w.failure,                // 失败数(5xx)
            "rollingCountTimeout": w.timeout,                // 超时数
            "rollingCountSemaphoreRejected": w.rejected,     // 信号量(bulkhead)拒绝数
            // 执行被取消/中止数(客户端断连、外层超时丢弃、panic unwind)。官方 Dashboard 不认识这个
            // 字段会直接忽略它;它已计入 requestCount 与 errorPercentage,不会在圈上凭空消失。
            "rollingCountCanceled": w.canceled,
            "rollingCountShortCircuited": 0,                 // 被熔断短路数（无熔断，恒 0）
            "rollingCountThreadPoolRejected": 0,             // 线程池拒绝数（信号量模式无，恒 0）
            "rollingCountBadRequests": 0,                    // 错误请求数（未用，恒 0）
            "rollingCountExceptionsThrown": 0,               // 抛异常数（未用，恒 0）
            "rollingCountFallbackFailure": w.fallback_failure, // fallback 配置冲突、panic 或递归数
            "rollingCountFallbackRejection": 0, // 终态 fallback 不设置独立并发门禁，因此恒为 0
            "rollingCountFallbackSuccess": w.fallback_success, // fallback 成功数 = 拒绝/超时产出降级响应的次数
            "rollingCountResponsesFromCache": 0,             // 命中请求缓存数（无缓存，恒 0）
            "rollingCountCollapsedRequests": 0,              // 请求合并数（未用，恒 0）
            "rollingCountEmit": 0,                           // emit 事件数（流式场景，恒 0）
            "rollingCountFallbackEmit": 0,                   // fallback emit 数（恒 0）

            // 延迟（execute=执行耗时，total=含排队总耗时；本实现无排队，二者相同）
            "latencyExecute_mean": lat.mean, // 执行延迟均值(ms)
            "latencyExecute": lat_json,      // 执行延迟分位 map
            "latencyTotal_mean": lat.mean,   // 总延迟均值(ms)
            "latencyTotal": lat_json,        // 总延迟分位 map

            // 并发 gauge
            "currentConcurrentExecutionCount": self.concurrent.load(Ordering::Relaxed),            // 当前并发数（实时读原子计数）
            "rollingMaxConcurrentExecutionCount": self.rolling_max_concurrent.load(Ordering::Relaxed), // 进程生命周期峰值(非滚动窗口,见字段注释)

            // 静态属性（Dashboard 圈下方显示 + 决定渲染）
            "propertyValue_executionIsolationStrategy": "SEMAPHORE",                                    // 隔离策略：信号量
            // 并发上限：None(不限) → 显示 0（Dashboard 上 0 表示"无 bulkhead，只看监控"）；Some(n) → n
            "propertyValue_executionIsolationSemaphoreMaxConcurrentRequests": self.max_concurrent.unwrap_or(0),
            // 超时(ms)：None(不超时) → 显示 0；Some(d) → 毫秒数
            "propertyValue_executionTimeoutInMilliseconds": self.timeout.map_or(0, |d| d.as_millis() as u64),
            // ↓ 这 3 个是 dashboard 的 validateData() 强制要求的字段，缺了会抛异常导致整页 "Loading..."。
            //   语义上对应"线程隔离"的属性，信号量模式下用不到，但必须存在：给等价值即可。
            "propertyValue_executionIsolationThreadTimeoutInMilliseconds": self.timeout.map_or(0, |d| d.as_millis() as u64), // 线程隔离超时(必填占位)
            "propertyValue_executionIsolationThreadInterruptOnTimeout": true,                          // 超时是否中断线程(必填占位)
            "propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests": 0, // 终态 fallback 不叠加第二层并发保护
            "propertyValue_metricsRollingStatisticalWindowInMilliseconds": WINDOW_SECS * 1000,          // 滚动统计窗口(ms)=10000
            "propertyValue_circuitBreakerEnabled": false,                  // 熔断器开关（关）
            "propertyValue_circuitBreakerForceOpen": false,                // 强制打开熔断（否）
            "propertyValue_circuitBreakerForceClosed": false,              // 强制关闭熔断（否）
            "propertyValue_circuitBreakerErrorThresholdPercentage": 50,    // 熔断错误率阈值(%)（仅展示）
            "propertyValue_circuitBreakerRequestVolumeThreshold": 20,      // 熔断最小请求量阈值（仅展示）
            "propertyValue_circuitBreakerSleepWindowInMilliseconds": 5000, // 熔断半开等待窗口(ms)（仅展示）
            "propertyValue_requestCacheEnabled": false,                    // 请求缓存（关）
            "propertyValue_requestLogEnabled": false,                      // 请求日志（关）

            "threadPool": self.group,  // 线程池名（这里复用 group）
            "reportingHosts": 1,        // 上报主机数（单机，恒 1）

            // 全局 TPS（每条 command JSON 都带同一个值）——dashboard 顶栏 `TPS: X/s` 读取
            "TPS": tps_rate()
        })
    }
}

// ── 拒绝/超时统一返回 JSON 壳 ──
// 注:门面 crate 不依赖业务的 BaseResponse,这里用 serde_json::json! 直接拼同样形状 {code, message, data}。
/// 业务作用：bulkhead 满时的 429 响应。
///
/// # 参数
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
/// - `max`: 允许的最大值或区间上界。
fn rejected_response(name: &str, max: usize) -> Response {
    let body = Json(json!({
        "code": 429,
        "message": format!("[{name}] bulkhead full (max_concurrent={max}), rejected, try again later"),
        "data": null,
    }));
    (StatusCode::TOO_MANY_REQUESTS, body).into_response()
}

/// 业务作用：超时时的 504 响应。
///
/// # 参数
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
/// - `dur`: 持续时间,用于超时、滑窗或指标统计。
fn timeout_response(name: &str, dur: Duration) -> Response {
    let body = Json(json!({
        "code": 504,
        "message": format!("[{name}] execution timed out after {}ms", dur.as_millis()),
        "data": null,
    }));
    (StatusCode::GATEWAY_TIMEOUT, body).into_response()
}

/// 业务作用：自定义限流/超时返回:**HTTP 200 + 用户给的 JSON body**(对齐统一响应风格,业务码放在 body 里,
/// 前端 axios 不会因 4xx/5xx 抛错)。body 由 #[hystrix(reject_response/timeout_response)] 在构造时解析存好。
///
/// # 参数
/// - `body`: 请求体、响应体或待处理原始内容。
fn custom_response(body: &Value) -> Response {
    (StatusCode::OK, Json(body.clone())).into_response()
}

// ════════════════════════════════════════════════════════════════════════════
// GET /hystrix.stream —— SSE 指标流，喂给 Hystrix Dashboard
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：输出 Hystrix Dashboard 可消费的 SSE 指标流。
///
/// Dashboard 填入这个 URL 即可监控。它消费的是 text/event-stream,每条 `data: {json}\n\n`
/// 是一个命令的快照;本端点每秒推一轮,每个命令一条。
pub async fn hystrix_stream() -> impl IntoResponse {
    // async_stream::stream! 宏：把一段含 yield 的异步代码块变成一个 Stream（异步迭代器），
    // 每个 yield 出去的元素就是一条 SSE Event。
    let stream = async_stream::stream! {
        // interval：每 1 秒触发一次的定时器（推送节奏）
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            // tick().await：挂起等到下一个 1 秒节拍（不忙等，让出执行权）
            ticker.tick().await;
            // 拷一份 Arc 列表，尽快释放锁（不要攥着锁去 await/序列化）
            // clone 只复制 Arc 指针(引用计数+1)很廉价；clone 完这行结束锁就释放了
            let commands: Vec<Arc<Command>> = lock_registry().clone();
            for cmd in commands {
                // 把每个命令的快照序列化成 JSON 字符串
                let data = cmd.snapshot_json().to_string();
                // yield 一条 SSE 事件出去（data: {json}）；Infallible = 永不出错的错误类型占位
                yield Ok::<Event, std::convert::Infallible>(Event::default().data(data));
            }
        }
    };

    // keep_alive：定期发 SSE 注释行做心跳，防中间代理掐断空闲连接（Dashboard 忽略注释行）
    // Sse::new(stream) 把上面的 Stream 包成 text/event-stream 响应；每 5 秒发一次心跳
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(5)))
}

// 命令的周期日志复用 Rolling 的 count、最小值、均值和最大值。
// 每个 Command 独立持有周期任务，无需业务额外启动；日志读取不会清空 SSE 共用的窗口。

/// 业务作用：读取当前全局 TPS（每秒事务数）的公开入口。
/// 给 set_extra 的闭包用——例如让某条命令的 CostTime 日志行尾带上实时吞吐：
///   `cmd.set_extra(|_period_ms| format!("tps={:.0}/s", hystrix::current_tps()));`
/// （tps_rate 本身是模块私有，这里开一个只读窗口暴露出去。）
pub fn current_tps() -> f64 {
    tps_rate()
}

// 路由隔离从 YAML 建立 matchit 索引，并按匹配到的路由模板复用并发信号量。
// 异步请求通过许可与期限控制并发，不创建额外线程池；阻塞业务须由调用方隔离。

/// 运行期的一条隔离规则（由 zconf::IsolationRule 转来，多带一个 pattern 串做 key/名字）。
struct IsolationCfg {
    pattern: String, // 原始模式（如 "/download/*"），做 Command 的 key/name/path（日志/Dashboard 显示它）
    max_concurrent: usize, // 并发上限 = 信号量许可数
    timeout: Duration, // 单请求超时
    tps_weight: Option<u64>, // None 不计 TPS / Some(w) 计
}

/// 全局隔离表：Trie + context-path 前缀 + 「模式 → 已建 Command」缓存。
struct IsolationTable {
    owner: Option<std::sync::Weak<managed::ManagedState>>,
    // 前缀树：path → 隔离规则。matchit 是 axum 内部用的那个基数树匹配器
    trie: matchit::Router<IsolationCfg>,
    // context-path（如 "/rust-simple-mvc"），dispatch 匹配前从 path 剥掉，让 yml 模式写相对路径
    ctx_prefix: String,
    // 模式 → 懒加载的 Command
    commands: dashmap::DashMap<String, Arc<Command>>,
}

// 全局隔离表，启动时 init 一次；没配 hystrix.isolation 则保持空（dispatch 全放行）
static ISOLATION: OnceLock<Mutex<Option<Arc<IsolationTable>>>> = OnceLock::new();

/// 业务作用：提供可按 owner 撤销的隔离表槽，独立安装仍保持一次性合同。
/// 参数说明：无。
/// 返回：进程级槽，读取方取得 Arc 后立即释放锁。
fn isolation() -> &'static Mutex<Option<Arc<IsolationTable>>> {
    ISOLATION.get_or_init(|| Mutex::new(None))
}

/// 业务作用：独立模式安装进程隔离规则，已有受管 owner 时保留其配置权。
/// 参数说明：`rules` 为路由规则；`context_path` 为匹配时剥离的上下文前缀。
/// 返回：首次合法配置安装生效；重复安装、受管冲突或非法模式保留原有警告行为。
pub fn init_isolation(
    rules: &std::collections::HashMap<String, IsolationRule>,
    context_path: &str,
) {
    let owner = managed::lock_owner();
    if owner.is_some() {
        tracing::warn!("managed hystrix owner rejects standalone isolation installation");
        return;
    }
    if rules.is_empty() {
        return;
    }
    let mut trie = matchit::Router::new();
    for (pattern, rule) in rules {
        // 把 配置中的 "/download/*" 归一成 matchit 0.8 的命名 catch-all "/download/{*rest}"
        let route = normalize_pattern(pattern);
        let cfg = IsolationCfg {
            pattern: pattern.clone(),
            max_concurrent: rule.max_concurrent,
            timeout: Duration::from_millis(rule.timeout_ms),
            tps_weight: rule.tps_weight,
        };
        // insert 失败（模式语法非法）只告警跳过，不让整个启动崩
        if let Err(e) = trie.insert(route.clone(), cfg) {
            tracing::warn!(
                "hystrix isolation pattern '{}' (route '{}') invalid, skipped: {}",
                pattern,
                route,
                e
            );
        }
    }
    // 去掉 context-path 末尾斜杠，得到 "/rust-simple-mvc"（context_path 为空则 ctx_prefix=""）
    let ctx = context_path.trim().trim_end_matches('/').to_string();
    let mut slot = isolation().lock().unwrap();
    if slot.is_some() {
        tracing::warn!("hystrix isolation already installed");
        return;
    }
    *slot = Some(Arc::new(IsolationTable {
        owner: None,
        trie,
        ctx_prefix: ctx,
        commands: dashmap::DashMap::new(),
    }));
    tracing::info!(
        "hystrix zconf-driven isolation initialized ({} patterns)",
        rules.len()
    );
}

/// 业务作用：把 配置中的通配 "/download/*" 转成 matchit 0.8 的命名 catch-all "/download/{*rest}"。
/// matchit 要求 catch-all 必须带名字且在末尾；非 "/*" 结尾的（已是具体路由）原样返回。
/// 注：matchit 0.8 起 catch-all 用花括号 {*rest}（0.7 是 *rest），与 axum 0.8 路由占位一致。
///
/// # 参数
/// - `pattern`: 匹配模式,用于扫描、订阅或路径匹配。
fn normalize_pattern(pattern: &str) -> String {
    if let Some(prefix) = pattern.strip_suffix("/*") {
        format!("{prefix}/{{*rest}}") // "/download/*" → "/download/{*rest}"
    } else {
        pattern.to_string()
    }
}

/// 业务作用：按固定路由模板匹配隔离规则并复用命令，受管关闭期间拒绝新执行。
/// 参数说明：`req` 为业务 HTTP 请求；`next` 为取得许可后执行的业务链路。
/// 返回：未匹配时原样放行；命中时返回业务、降级或隔离响应，目录关闭／超限返回 503。
pub async fn dispatch(req: Request, next: Next) -> Response {
    // 没配 hystrix.isolation → ISOLATION 未初始化 → 全部放行
    let table = isolation().lock().unwrap().clone();
    let Some(table) = table else {
        return next.run(req).await;
    };
    // 路由表借用归入旧代责任，关闭后不得在下一代目录创建命令。
    let _guard = match &table.owner {
        Some(owner) => match owner.upgrade().and_then(|owner| owner.enter().ok()) {
            Some(guard) => Some(guard),
            None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
        None => None,
    };
    // 剥掉 context-path 前缀，得相对路径再匹配（"/rust-simple-mvc/download/x" → "/download/x"）。
    // strip_prefix 失败（path 本就不带前缀，如 axum nest 已剥过）则原样用——两种情况都正确。
    let full = req.uri().path();
    let rel = full.strip_prefix(&table.ctx_prefix).unwrap_or(full);
    let rel = if rel.is_empty() {
        "/"
    } else {
        rel
    };
    match table.trie.at(rel) {
        Ok(m) => {
            // 通配匹配到的只是【一份共享参数】(并发/超时/tps)，不是桶本身。
            let cfg = m.value;
            // 每个路由模板使用独立的 Command 和信号量；匹配配置只提供参数。
            // 以 MatchedPath 作为桶身份，避免路径变量为每个业务对象生成新桶；缺失时回退配置模式。
            let route: String = match req.extensions().get::<axum::extract::MatchedPath>() {
                Some(mp) => {
                    let t = mp.as_str();
                    // 与 rel 一致地剥掉 context-path 前缀，让圈名是相对路由
                    t.strip_prefix(&table.ctx_prefix).unwrap_or(t).to_string()
                }
                None => cfg.pattern.clone(),
            };
            // 按【真实路由】取/建独立 Command（computeIfAbsent）。clone 出 Arc 后立刻结束分段锁再 .await。
            let cmd = match table.commands.entry(route.clone()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let c = match cfg.tps_weight {
                        Some(w) => Command::with_tps(
                            &route,
                            "isolation",
                            cfg.max_concurrent,
                            cfg.timeout,
                            w,
                        ),
                        None => Command::new(&route, "isolation", cfg.max_concurrent, cfg.timeout),
                    };
                    // 拒绝的候选不能存入路由缓存，否则失败名称仍会无界累积。
                    if !c.admitted.load(Ordering::Acquire) {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    if cfg.tps_weight.is_some() {
                        c.set_extra(|_period_ms| format!("tps={:.0}/s", current_tps()));
                    }
                    c.set_path(&route);
                    entry.insert(c.clone());
                    c
                }
            };
            // 命中 → 套 bulkhead + 超时 + 指标执行（复用 ① 的 Command::run）
            cmd.run(req, next).await
        }
        // Trie 没匹配到 → 不隔离，放行（硬编码路由 /spot/kline 等走这条，不被双重包裹）
        Err(_) => next.run(req).await,
    }
}

// ── re-export 过程宏 ──
pub use hystrix_macro::{global_fallback, hystrix};

/// 宏展开专用的第三方依赖桥:`#[hystrix]` 生成代码经
/// `<运行时根>::__private::axum` 引用 axum——业务只依赖 `nasa` 时无需再直接声明 axum。
/// **不属于稳定业务 API**,随时可能变化。
#[doc(hidden)]
pub mod __private {
    pub use crate::fallback::{CollectedGlobalFallback, HYSTRIX_COLLECTED_GLOBAL_FALLBACKS};
    pub use crate::managed::{CollectedCommand, CommandSlot, COLLECTED_COMMANDS};
    pub use axum;
    pub use linkme;
}
