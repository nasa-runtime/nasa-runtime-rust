//! 路由级 bulkhead 隔离、超时保护和指标流。
//!
//! 本 crate 提供可显式调用的 `Command` 运行时与 axum 中间件辅助，适合保护慢下游、
//! 热点接口和需要被 Dashboard 观测的业务入口。
#![recursion_limit = "512"] // hystrix snapshot_json 的 json!{} 字段多,提高宏递归上限
                            // ============================================================================
                            // 路由级 bulkhead 隔离 + 超时 + Dashboard 指标流
                            //
                            // ★ 能力范围(明确,避免误读):本模块只做【信号量隔离(bulkhead)+ 超时 + 滚动窗口指标】,
                            //   **不提供错误率触发的短路熔断**(无 Closed/Open/HalfOpen 状态机、无 error-threshold 短路;
                            //   Dashboard 的 isCircuitBreakerOpen 恒 false、rollingCountShortCircuited 恒 0)。
                            //   下游持续失败时靠并发上限 + 超时保护自身,不会自动短路。完整熔断器可作为独立状态机扩展。
                            //
                            // 对照参考实现的隔离 filter：
                            //   - 它按 URL 给每类接口套【线程池/信号量隔离 + 超时 + 队列拒绝】并上报 /hystrix.stream
                            //   - 本模块在 async Rust 里用【per-route 信号量(bulkhead)+ 超时 + 滚动窗口指标】等价实现
                            //     （async 不需要线程池隔离：慢调用 .await 挂起不占 worker 线程，详见文档）
                            //
                            // 四部分：
                            //   ① Command —— 一个被隔离+监控的“命令”(= 一条路由)：
                            //        · tokio::Semaphore 限并发(bulkhead)，try_acquire 满了立刻拒(429)，不排队
                            //        · tokio::time::timeout 限时(对应 executionTimeoutInMilliseconds)
                            //        · 滚动窗口(10s)统计 success/failure/timeout/rejected + 延迟百分位
                            //        · 当前并发数 gauge(currentConcurrentExecutionCount)
                            //      用法：作为 axum middleware 包在某条路由上(见 main.rs)。
                            //
                            //   ② hystrix_stream —— GET /hystrix.stream 的 SSE 端点：
                            //        每秒把所有已注册 Command 的快照序列化成 Hystrix Dashboard 认得的 JSON 推出去。
                            //        字段名严格对齐 SerialHystrixDashboardData(type=HystrixCommand / rollingCountXxx / latencyExecute...)。
                            //
                            //   ③ CostTime 风格的定时延迟日志(融合自 原工具包 CostTime)：
                            //        【每个 Command 在 build() 里各自 spawn 一个独立的 10s 周期任务】(相位锚定创建时刻、
                            //        互相错开，对齐 原实现「每 url 各起一个 TimingWheel 任务」)，每拍打一行
                            //        `path N次/10s min/avg/max (ms)`，复用 ① 的 Rolling 滚动窗口、不另存计数器。
                            //        是"每请求延迟日志"的低开销聚合替代。可选 extra 钩子(set_extra)对齐 CostTime 的 Function<Long,String>。
                            //
                            //   ④ 配置驱动隔离(init_isolation + dispatch) —— 对标 原实现 HystrixDashboardFilter：
                            //        yml 配 hystrix.isolation 的路由前缀模式(/download/*) → 建 matchit Trie；
                            //        一个全局中间件 dispatch 每请求拿 path 匹配 Trie，命中就按模式懒加载 Command 套上 ①。
                            //        与"硬编码 per-route Command"(main.rs 的 SpotKline/HeavySlow)并存，对照两种范式。
                            // ============================================================================

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
/// 返回全局熔断命令表；用于集中登记和查询命令配置。
fn registry() -> &'static Mutex<Vec<Arc<Command>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// 取全局命令表的锁;一次持锁 panic 不应让后续所有请求和 Dashboard 上报都跟着 panic。
///
/// # 参数
///
/// 本函数没有参数,返回可继续使用的命令表守卫(中毒时取回内部数据)。
fn lock_registry() -> std::sync::MutexGuard<'static, Vec<Arc<Command>>> {
    registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ── 全局 TPS（吞吐：每秒事务数）──
// 对照 原实现 com.nasa.common.interceptor.TPSInterceptor extends OPS：dashboard 顶栏 `TPS: X/s` 显示它
//   （SerialHystrixDashboardData 把同一个 TPS 值写进每条 command JSON 的 "TPS" 字段，
//    JS hystrixCommand.js 里 `$('#TPS').html(data.TPS + "/s")` 取用）。
//
// ★ 这里【不再自立一套独立窗口】（早先用过一个全局 TpsWindow，结果它和圈/QPS 用的是两套
//   独立时钟的滑动窗口，采样相位错开 → TPS 和「圈数字/10」对不上）。现在 TPS 直接从
//   【各命令 Rolling 的 requestCount】派生：
//       TPS = Σ(所有 tps_weight=Some 的命令的 requestCount × weight) / WINDOW_SECS
//   而 requestCount 正是 Dashboard 画圈/算 QPS 用的同一份数据、同一套窗口 → TPS 必然 =
//   「这些圈的 QPS 之和」，只差取整级别，不会再各自漂。
//   附带好处：run() 热路径上不再有「每请求抢一个全局互斥锁」的 tps_hit。

/// 读取当前 TPS（每秒事务数）= 所有计 TPS 的命令的 requestCount(×weight) 求和 / 窗口秒数。
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
    // 这一秒内产出降级响应的次数(对照 FALLBACK_SUCCESS):拒绝/超时分支返回 fn / 静态串 / 默认壳都算一次。
    // 当前 FallbackFn 产出 Response 无错误模型,故只统计 success;失败/拒绝细分需先扩展返回类型。
    fallback_success: u64,
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
    /// 构造新实例；用于集中初始化内部字段和默认状态。
    fn new() -> Self {
        Self {
            start: Instant::now(),
            buckets: VecDeque::new(),
            latencies: VecDeque::new(),
        }
    }

    /// 返回当前时间相对窗口起点的第几秒。
    ///
    /// 该值用作桶 key,保证桶切换不依赖墙钟回拨。
    fn now_sec(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    // 丢弃滑出窗口的旧桶/旧样本
    // 参数：now = 当前秒。floor = 窗口内允许保留的最早一秒。
    ///
    /// # 参数
    /// - `now`: 当前时间戳,用于窗口统计和状态过期判断。
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

    /// 记录一次执行结果。
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

    /// 记一次"产出了降级响应"(fallback_success)。在拒绝/超时分支记完 primary outcome 后调用,
    /// 与 record 共用当前秒的桶(同一把锁内连续调用,见 run_fn)。
    fn record_fallback_success(&mut self) {
        let now = self.now_sec();
        self.evict(now);
        if self.buckets.back().map(|b| b.sec) != Some(now) {
            self.buckets.push_back(Bucket {
                sec: now,
                ..Default::default()
            });
        }
        self.buckets.back_mut().unwrap().fallback_success += 1;
    }

    /// 汇总当前滚动窗口。
    ///
    /// 会先驱逐过期桶和样本,再把窗口内计数相加并计算延迟百分位,供熔断判断和 `/hystrix.stream` 输出。
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
        }
        // 延迟百分位：取出窗口内全部延迟样本，排序后算分位
        let mut lat: Vec<u64> = self.latencies.iter().map(|&(_, ms)| ms).collect();
        lat.sort_unstable();
        sum.latency = Percentiles::from_sorted(&lat);
        sum
    }

    /// 只累加窗口内的请求总数,不收集也不排序延迟样本。
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
    /// 从【已升序排序】的延迟样本算出各分位。
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
    // 是否计入全局 TPS + 计数权重。对标 原实现 的 @TPS 注解：
    //   None       = 没标 @TPS → 不计入 TPS（默认）
    //   Some(w)    = @TPS(value=w) → 每个请求给全局 TPS +w
    tps_weight: Option<u64>,
    // 【融合自 原工具包 CostTime 的 extra 钩子】可选的附加信息生成器。
    // 对标 原实现 `CostTime.extra(url, Function<Long,String>)`：每 10s 打那行延迟日志时，
    //   把这个闭包的输出拼到行尾（入参是统计周期毫秒数 = WINDOW_SECS*1000，对应 原实现 的 PERIOD）。
    //   典型用途：拼一段自定义指标（如吞吐、外部计数）到日志里。
    // 用 OnceLock 包：Command 一出生就在 Arc 里、之后不可变，OnceLock 提供"只设一次"的内部可变性，
    //   让构造后还能用 set_extra(&self, ...) 补设；Box<dyn Fn..+Send+Sync> 保证可跨线程被定时任务调用。
    extra: OnceLock<Box<dyn Fn(u64) -> String + Send + Sync>>,
    // 【CostTime 日志用】真实接口路由(如 "/spot/kline")。对标 原实现 CostTime 打的是 url。
    //   name 是 Dashboard 圈标题(如 "SpotKline")，path 是日志里更直观的路由；二者用途不同。
    //   OnceLock：构造后由 set_path 补设；未设则 log_cost 回退用 name。
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
    /// 进入命令执行区并递增当前并发数。
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

    /// 标记本次执行已产生 success/failure/timeout 结局,Drop 时不再补记 canceled。
    ///
    /// # 参数
    ///
    /// 本函数没有参数,只翻转守卫内部的完成标记。
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for ConcurrentGauge<'_> {
    /// 离开命令执行区时归还当前并发计数;未产生结局的(客户端断连、外层超时丢弃、panic
    /// unwind)补记一次 canceled,避免这些请求在 QPS 与错误率里凭空消失。
    fn drop(&mut self) {
        self.command.concurrent.fetch_sub(1, Ordering::Relaxed);
        if !self.completed {
            self.command.stats().record(Outcome::Canceled, 0);
        }
    }
}

impl Command {
    /// 取本命令滚动窗口的锁;一次持锁 panic 不应让该端点后续所有请求都跟着 panic。
    ///
    /// # 参数
    ///
    /// 本函数没有参数,返回可继续使用的滚动窗口守卫(中毒时取回内部数据)。
    fn stats(&self) -> std::sync::MutexGuard<'_, Rolling> {
        self.stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // 内部构造：是否计 TPS 由 tps_weight 决定
    // 参数：
    //   name           = Dashboard 圈标题
    //   group          = 归类名
    //   max_concurrent = 并发上限（信号量许可数）
    //   timeout        = 单请求超时
    //   tps_weight     = None 不计 TPS / Some(w) 每请求给全局 TPS +w
    ///
    /// # 参数
    /// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
    /// - `group`: 消费组、服务分组或任务分组名称。
    /// - `max_concurrent`: 熔断隔离允许的最大并发数。
    /// - `timeout`: 等待或执行超时时间,用于控制阻塞边界。
    /// - `tps_weight`: 吞吐权重,用于按规则计算限流阈值。
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
        let cmd = Arc::new(Self {
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
        // 自动注册到全局表：clone 出一份 Arc 放进 REGISTRY，SSE 端点据此遍历上报
        {
            let mut table = lock_registry();
            // 同 (group, name) 已存在:Dashboard 会画出两个同名圈、CostTime 也会有两行同名日志。
            // 保持"各自独立统计"的既有行为(不合并),但必须让重名可见,否则排查时无从下手。
            if table
                .iter()
                .any(|c| c.name == cmd.name && c.group == cmd.group)
            {
                tracing::warn!(
                    group = %cmd.group,
                    command = %cmd.name,
                    "duplicate hystrix command name; dashboard will show separate circles with the same title"
                );
            }
            table.push(cmd.clone());
        }

        // 【CostTime 风格定时日志】给【本命令】单独起一个 10s 周期任务（对齐 原实现 CostTime：
        //   每个 url 在自己构造时各 TimingWheel.exec 一个独立周期任务）。
        //   相位锚定【创建时刻】→ 不同命令的日志在时间轴上【自然错开】，不会像「一个全局 ticker
        //   遍历所有命令」那样 N 个命令同一瞬间齐刷刷喷 N 行（100 个路由就是一秒 100 行）。
        //   用 Weak 持有：命令若被回收，任务自行退出（当前命令常驻 REGISTRY，主要图稳妥）。
        //   仅在 tokio runtime 内才起（build 总在 runtime 内被调用；try_current 仅作防御，避免无运行时时 panic）。
        if tokio::runtime::Handle::try_current().is_ok() {
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

    /// 创建命令并【自动注册】到全局表（SSE 端点据此上报）。
    /// name 会成为 Dashboard 里那个圈的标题；group 用于归类。
    /// 【不计入全局 TPS】——等价于 原实现 端【没标 @TPS】的接口。
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

    /// 【最通用构造器】并发上限/超时都可为 None（None = 不限并发 / 不超时），tps_weight 也可 None。
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

    /// 同 new，但【计入全局 TPS】，weight 对应 原实现 `@TPS(value=weight)`（默认 1）。
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

    /// 【融合自 CostTime.extra】给本命令挂一个"附加信息生成器"，每 10s 打延迟日志时拼到行尾。
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

    /// 设置本命令的真实接口路由（如 "/spot/kline"），只影响 CostTime 定时日志的显示。
    /// 不设则 log_cost 回退用 name（Dashboard 圈标题）。Dashboard 显示不受影响（它读 name）。
    ///
    /// # 参数
    /// - `path`: 真实接口路由字符串,用于周期延迟日志展示。
    pub fn set_path(&self, path: &str) {
        let _ = self.path.set(path.to_string());
    }

    /// 【自定义限流返回】设 bulkhead 满时返回的 JSON body(随后以 HTTP 200 返回)。
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

    /// 同 [`set_reject_response_str`],但**返回结果供直接调用方感知**:JSON 非法 → `Err`;合法且本次设入 → `Ok(true)`;
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

    /// 【自定义超时返回】设超时时返回的 JSON body(随后以 HTTP 200 返回)。用法同 [`set_reject_response_str`]。
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

    /// 同 [`set_timeout_response_str`],但**返回结果**:JSON 非法 → `Err`;本次设入 → `Ok(true)`;已设过 → `Ok(false)`。
    ///
    /// [`set_timeout_response_str`]: Self::set_timeout_response_str
    ///
    /// # 参数
    /// - `json`: 请求超时时返回的 JSON body 字符串。
    pub fn try_set_timeout_response_str(&self, json: &str) -> Result<bool, serde_json::Error> {
        let v = serde_json::from_str::<Value>(json)?;
        Ok(self.timeout_body.set(v).is_ok())
    }

    /// 【自定义限流降级 fn】设 bulkhead 满时调用的降级闭包(产出 Response)。
    /// 由 `#[hystrix(reject_fn = path)]` 在构造时调用;优先级高于 reject_response;只设一次(OnceLock)。
    ///
    /// # 参数
    /// - `fb`: bulkhead 拒绝时执行的异步降级响应闭包。
    pub fn set_reject_fn(&self, fb: FallbackFn) {
        let _ = self.reject_fb.set(fb);
    }

    /// 【自定义超时降级 fn】设超时时调用的降级闭包(产出 Response)。用法同 set_reject_fn。
    ///
    /// # 参数
    /// - `fb`: 请求超时时执行的异步降级响应闭包。
    pub fn set_timeout_fn(&self, fb: FallbackFn) {
        let _ = self.timeout_fb.set(fb);
    }

    /// 【融合自 CostTime】打印本命令最近一个窗口的延迟聚合日志，由 build() 里给本命令起的那个独立 10s 周期任务每拍调一次。
    /// 复用现成的滚动窗口 snapshot（不像 原实现 CostTime 那样 restore 清零——因为这份 Rolling
    /// 同时喂着 SSE 流，清零会破坏 SSE；滑动窗口每 10s 读一次即"最近 10s"，语义等价）。
    fn log_cost(&self) {
        // 取最近窗口汇总（含各结局计数 + 延迟百分位）
        let w = self.stats().snapshot();
        // count = 真正执行过的样本数（成功+失败+超时；被拒的没执行、无延迟，不计）——对齐 min/avg/max 的样本集
        let count = w.success + w.failure + w.timeout;
        // 对齐 原实现 CostTime：窗口内没有样本就不打（避免刷空日志）
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
        // 日志格式对齐 原实现 CostTime.cost：`<url> <count>次/<10>s min <min> avg <avg> max <max> (ms) <extra>`
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

    /// 作为中间件包裹一次请求：bulkhead + 超时 + 指标记录。
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

    /// 【通用执行入口】用本命令的 bulkhead + 超时保护【任意 async 执行体】，并把
    /// 成功/失败/超时/拒绝记入滚动窗口（→ /hystrix.stream → Dashboard）。
    ///
    /// 为什么需要它：`run` 是中间件签名 `(req, next)`，只适合 `.layer(...)`。而 `#[hystrix]` 属性宏
    ///   贴的是【普通 handler】（没有 Next），原来只能在宏里自带一套 Semaphore+timeout，结果【不上 Dashboard】。
    ///   抽出 run_fn 后，宏把"原函数体"包成闭包传进来 → 注解版也走真正的 Command → 指标自然流进 Dashboard。
    ///
    /// 参数 f：`FnOnce() -> Future<Output = Response>`，即"要被保护的那段执行体"。
    ///   只有 try_acquire 抢到许可后才会调用 f()（拿不到许可直接 429，f 根本不执行）。
    ///
    /// # 参数
    /// - `f`: 被当前命令保护的异步执行体,只有通过 bulkhead 后才会被调用。
    pub async fn run_fn<F, Fut>(self: Arc<Self>, f: F) -> Response
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Response>,
    {
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
                    // 信号量满 → 记一次 Rejected（延迟无意义传 0）+ 一次 fallback_success（下面必产出降级响应），打告警
                    {
                        let mut st = self.stats();
                        st.record(Outcome::Rejected, 0);
                        st.record_fallback_success();
                    }
                    let max = self.max_concurrent.unwrap_or(0);
                    tracing::warn!(
                        command = %self.name,
                        max = max,
                        "bulkhead full, rejecting request (429)"
                    );
                    // 降级优先级:reject_fn(闭包)→ reject_response(静态 JSON,HTTP 200)→ 默认 429 壳。
                    if let Some(fb) = self.reject_fb.get() {
                        return fb().await;
                    }
                    return match self.reject_body.get() {
                        Some(v) => custom_response(v),
                        None => rejected_response(&self.name, max),
                    };
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
        let latency_ms = start.elapsed().as_millis() as u64;

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
                // 记一次 Timeout + 一次 fallback_success（下面必产出降级响应）
                {
                    let mut st = self.stats();
                    st.record(Outcome::Timeout, latency_ms);
                    st.record_fallback_success();
                }
                tracing::warn!(
                    command = %self.name,
                    timeout_ms = dur.as_millis() as u64,
                    "command timed out (504)"
                );
                // 降级优先级:timeout_fn(闭包)→ timeout_response(静态 JSON)→ 默认 504 壳。
                if let Some(fb) = self.timeout_fb.get() {
                    return fb().await;
                }
                match self.timeout_body.get() {
                    Some(v) => custom_response(v),
                    None => timeout_response(&self.name, dur),
                }
            }
        }
    }

    /// 生成 Hystrix Dashboard 认得的一条 HystrixCommand JSON。
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
            "rollingCountFallbackFailure": 0,                // fallback 失败数（当前 fallback 产出 Response 无错误模型，恒 0）
            "rollingCountFallbackRejection": 0,              // fallback 被拒数（无 fallback 限流，恒 0）
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
            "propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests": 0,                         // fallback 并发上限:本实现无 fallback 信号量(不限),按本模块约定用 0 表示不限(诚实展示,不虚标 10)
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
/// bulkhead 满时的 429 响应。
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

/// 超时时的 504 响应。
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

/// 自定义限流/超时返回:**HTTP 200 + 用户给的 JSON body**(对齐统一响应风格,业务码放在 body 里,
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
/// 输出 Hystrix Dashboard 可消费的 SSE 指标流。
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

// ════════════════════════════════════════════════════════════════════════════
// CostTime 风格的定时延迟日志（融合自 原工具包 com.nasa.common.interceptor.CostTime）
// ════════════════════════════════════════════════════════════════════════════
// 原实现 那边 CostTime 是【每 url 一个聚合器 + 每个 url 在自己构造时各起一个 TimingWheel 10s 周期任务】。
// 本项目把它的「料」直接复用到既有结构上，不另起文件、不另存计数器：
//   · 「每 url 一个聚合器」      → 已有的【每路由一个 Command + 它的 Rolling 滚动窗口】
//   · 「counter/min/max/total」 → Rolling 已经在算 count / p0(min) / mean(avg) / p100(max)
//   · 「PERIOD = 10000ms」       → 已有常量 WINDOW_SECS = 10
//   · 「每 url 各自一个 TimingWheel 周期任务」→ 【每个 Command 在 build() 里各 spawn 一个独立的
//        10s 周期任务】，相位锚定创建时刻 → 不同命令日志在时间轴上【自然错开】，
//        不像「一个全局 ticker 遍历所有命令」那样 N 个命令同一瞬间齐刷刷打 N 行。
//   · 「extra: Function<Long,String>」→ Command.extra 钩子 + set_extra（见上）
// 与 原实现 的唯一行为差异：不做 restore() 清零（Rolling 是滑动窗口、且同时喂着 SSE，清零会坏 SSE），
//   每 10s 读一次滑动 snapshot 即「最近 10s」，语义等价。
//
// 实际 spawn 逻辑在 Command::build —— 命令一创建就自带它的定时日志任务，无需 main 额外调用任何启动函数。
// log_cost() 是「每请求 `⏱ latency` debug 日志」的生产级替代：1 行/10s/路由、相位错开、开销可忽略，
//   可放心在 info 级别常开，不像每请求日志那样压垮吞吐。

/// 读取当前全局 TPS（每秒事务数）的公开入口。
/// 给 set_extra 的闭包用——例如让某条命令的 CostTime 日志行尾带上实时吞吐：
///   `cmd.set_extra(|_period_ms| format!("tps={:.0}/s", hystrix::current_tps()));`
/// （tps_rate 本身是模块私有，这里开一个只读窗口暴露出去。）
pub fn current_tps() -> f64 {
    tps_rate()
}

// ════════════════════════════════════════════════════════════════════════════
// ④ 配置驱动的路由级 bulkhead 隔离（对标 原实现 HystrixDashboardFilter 的 Trie 匹配）
// ════════════════════════════════════════════════════════════════════════════
// 与硬编码 demo（main.rs 给 /spot/kline、/heavy/slow 各挂一个 Command）并存、对照：
//   · 硬编码：     编译期把 Command 钉在某条 .route() 上，加/调接口要改代码重编译。
//   · 配置驱动：   一个全局中间件 dispatch，请求时拿 path 在 Trie 上匹配出隔离参数，
//                  按"匹配到的模式"懒加载并复用 Command。加/调隔离只改 yml。
// 数据流：yml(hystrix.isolation) → init_isolation 建 matchit Trie → dispatch 每请求匹配。
// async 下无需线程池隔离（慢调用不占线程），所以只实现 原实现 filter 的信号量快路径，
//   它的 doFilterWithCommand（线程池+fallback）整段不需要。

/// 运行期的一条隔离规则（由 zconf::IsolationRule 转来，多带一个 pattern 串做 key/名字）。
struct IsolationCfg {
    pattern: String, // 原始模式（如 "/download/*"），做 Command 的 key/name/path（日志/Dashboard 显示它）
    max_concurrent: usize, // 并发上限 = 信号量许可数
    timeout: Duration, // 单请求超时
    tps_weight: Option<u64>, // None 不计 TPS / Some(w) 计
}

/// 全局隔离表：Trie + context-path 前缀 + 「模式 → 已建 Command」缓存。
struct IsolationTable {
    // 前缀树：path → 隔离规则。matchit 是 axum 内部用的那个基数树匹配器
    trie: matchit::Router<IsolationCfg>,
    // context-path（如 "/rust-simple-mvc"），dispatch 匹配前从 path 剥掉，让 yml 模式写相对路径
    ctx_prefix: String,
    // 模式 → 懒加载的 Command（对标 原实现 metricsCache.computeIfAbsent；同模式所有请求共用一个桶）
    commands: dashmap::DashMap<String, Arc<Command>>,
}

// 全局隔离表，启动时 init 一次；没配 hystrix.isolation 则保持空（dispatch 全放行）
static ISOLATION: OnceLock<IsolationTable> = OnceLock::new();

/// 启动时调用一次：把 yml 的 hystrix.isolation 建成 Trie。
/// 规则为空则不初始化 → dispatch 全部放行（= 配置驱动隔离未启用）。
///
/// # 参数
/// - `rules`: 「模式 → 规则」表,通常来自配置里的 hystrix.isolation。
/// - `context_path`: 服务 context-path;匹配前会从请求 path 剥掉。
pub fn init_isolation(
    rules: &std::collections::HashMap<String, IsolationRule>,
    context_path: &str,
) {
    if rules.is_empty() {
        return;
    }
    let mut trie = matchit::Router::new();
    for (pattern, rule) in rules {
        // 把 原实现 风格 "/download/*" 归一成 matchit 0.8 的命名 catch-all "/download/{*rest}"
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
    let _ = ISOLATION.set(IsolationTable {
        trie,
        ctx_prefix: ctx,
        commands: dashmap::DashMap::new(),
    });
    tracing::info!(
        "hystrix zconf-driven isolation initialized ({} patterns)",
        rules.len()
    );
}

/// 把 原实现 风格通配 "/download/*" 转成 matchit 0.8 的命名 catch-all "/download/{*rest}"。
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

/// 全局中间件（对标 HystrixDashboardFilter.doFilter）：拿请求 path 在 Trie 上匹配。
///   命中 → 按"匹配到的模式"懒加载/复用 Command，套 bulkhead+超时+指标后执行；
///   未命中 / 未初始化 → 直接放行（不影响硬编码路由和无需隔离的路由）。
/// 用法见 main.rs：整个 app 套一层 from_fn(dispatch)。
///
/// # 参数
/// - `req`: 本次进入全局中间件的 HTTP 请求。
/// - `next`: 未命中隔离规则或执行通过时的 axum 放行句柄。
pub async fn dispatch(req: Request, next: Next) -> Response {
    // 没配 hystrix.isolation → ISOLATION 未初始化 → 全部放行
    let Some(table) = ISOLATION.get() else {
        return next.run(req).await;
    };
    // 剥掉 context-path 前缀，得相对路径再匹配（"/rust-simple-mvc/download/x" → "/download/x"）。
    // strip_prefix 失败（path 本就不带前缀，如 axum nest 已剥过）则原样用——两种情况都正确。
    let full = req.uri().path();
    let rel = full.strip_prefix(&table.ctx_prefix).unwrap_or(full);
    let rel = if rel.is_empty() { "/" } else { rel };
    match table.trie.at(rel) {
        Ok(m) => {
            // 通配匹配到的只是【一份共享参数】(并发/超时/tps)，不是桶本身。
            let cfg = m.value;
            // 对标 原实现 HystrixDashboardFilter：每个【接口(路由)】有自己独立的 Command/圈/信号量；
            //   Trie(matchingGet) 只负责把参数查出来，同模式下各接口【共用同一份参数值】。
            // 桶身份 = 本次命中的【真实路由模板】(对标 原实现 的 collector.getUrl())：
            //   用 axum 的 MatchedPath（如 "/download/*rest"）——它按 handler【有界】，
            //   不会像“原始具体路径”那样每个文件名炸出一个新桶（路径变量值不应拆桶）。
            //   理论兜底：拿不到 MatchedPath 时退回配置模式 cfg.pattern（仍有界）。
            let route: String = match req.extensions().get::<axum::extract::MatchedPath>() {
                Some(mp) => {
                    let t = mp.as_str();
                    // 与 rel 一致地剥掉 context-path 前缀，让圈名是相对路由
                    t.strip_prefix(&table.ctx_prefix).unwrap_or(t).to_string()
                }
                None => cfg.pattern.clone(),
            };
            // 按【真实路由】取/建独立 Command（computeIfAbsent）。clone 出 Arc 后立刻结束分段锁再 .await。
            let cmd = table
                .commands
                .entry(route.clone())
                .or_insert_with(|| {
                    // 首次见到该路由才建 Command；建时自动注册进 REGISTRY → Dashboard/CostTime 看得到。
                    // 参数(并发/超时/tps)来自通配匹配 cfg —— 同模式下各接口共用同一份参数值。
                    let c = match cfg.tps_weight {
                        Some(w) => {
                            let c = Command::with_tps(
                                &route,
                                "isolation",
                                cfg.max_concurrent,
                                cfg.timeout,
                                w,
                            );
                            // 计 TPS 的命令，CostTime 日志尾巴带实时 tps
                            c.set_extra(|_period_ms| format!("tps={:.0}/s", current_tps()));
                            c
                        }
                        None => Command::new(&route, "isolation", cfg.max_concurrent, cfg.timeout),
                    };
                    c.set_path(&route); // 显示真实路由（如 "/spot/kline"、"/download/{*rest}"）
                    c
                })
                .clone();
            // 命中 → 套 bulkhead + 超时 + 指标执行（复用 ① 的 Command::run）
            cmd.run(req, next).await
        }
        // Trie 没匹配到 → 不隔离，放行（硬编码路由 /spot/kline 等走这条，不被双重包裹）
        Err(_) => next.run(req).await,
    }
}

// ── re-export 过程宏 ──
pub use hystrix_macro::hystrix;

/// 宏展开专用的第三方依赖桥:`#[hystrix]` 生成代码经
/// `<运行时根>::__private::axum` 引用 axum——业务只依赖 `nasa` 时无需再直接声明 axum。
/// **不属于稳定业务 API**,随时可能变化。
#[doc(hidden)]
pub mod __private {
    pub use axum;
}
