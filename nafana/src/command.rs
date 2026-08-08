//! Command 运行时:bulkhead + 超时 + 降级 + 双轨指标记录。
//!
//! 执行语义:try_acquire 满即拒不排队、tokio timeout 限时、降级三级回退
//! (fn → 静态 JSON → 默认壳)、10s 请求汇总日志。每次结局同时写「单调 counter/直方图」
//! (Prometheus 轨)与「10s 滚动窗口」(实时快照轨)。
//!
//! 0 值规范(合同):所有入口统一在 [`Command::monitor`] 归一化——`max_concurrent=0`
//! 与 `timeout_ms=0` 都视为"不限制/不超时"，避免 0 许可信号量和 0ms timeout；
//! `tps` 显式给 0 = 标记参与 TPS 但权重 0。

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::counters::{CommandCounters, LatencyHistogram};
use crate::fallback::{
    execute_global_fallback, FallbackCause, FallbackContext, GlobalFallbackExecution,
};
use crate::registry::{self, CommandKey, CommandParams, MonitorConflict};
use crate::rolling::{Outcome, RollingWindow, WINDOW_SECS};

/// 非 fallible 构造器可接受的最长命令超时；防止极端 `Duration` 进入 Tokio deadline 运算。
const MAX_COMMAND_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 降级 fn 槽类型:无捕获、可多次调用(每个被拒/超时请求各产一个 future),故 Fn + Send + Sync。
/// 由宏生成的 set_reject_fn/set_timeout_fn 注入。
pub type FallbackFn = Box<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> + Send + Sync,
>;

/// 一条被 bulkhead、超时和双轨指标保护的命令(= 一个接口端点)。
///
/// 通常经三种途径创建:`#[grafana]` 注解、显式构造器、配置驱动 [`crate::dispatch`]。
/// 全部经全局注册表按 (group, command) 去重,`/metrics` 据此遍历输出。
pub struct Command {
    /// 命令名(Grafana 面板 tile 标题;`command` 标签)。
    command: String,
    /// 分组名(`group` 标签)。
    group: String,
    /// 并发上限;None = 不限并发(不建信号量,永不拒)。
    max_concurrent: Option<usize>,
    /// 单请求超时;None = 不超时。
    timeout: Option<Duration>,
    /// TPS 权重;None = 不计 TPS,Some(w) = 每请求(含被拒、被取消)给 `nafana_tps_total` +w。
    tps_weight: Option<u64>,
    /// bulkhead 本体;None = 不限并发时不建。
    sem: Option<tokio::sync::Semaphore>,
    /// 当前并发 gauge:进入 +1、离开(含请求取消)-1,RAII 保证配对。
    inflight: AtomicI64,
    /// 进程终身并发峰值(只增不减;滚动口径在 rolling 桶里另记)。
    inflight_lifetime_max: AtomicI64,
    /// 10s 滚动窗口(实时快照轨),加锁访问。
    rolling: Mutex<RollingWindow>,
    /// 单调计数(Prometheus 轨),无锁。
    counters: CommandCounters,
    /// 延迟直方图(Prometheus 轨),无锁。
    histogram: LatencyHistogram,
    /// 自定义限流返回 JSON(HTTP 200);未设回退默认 429 壳。
    reject_body: OnceLock<Value>,
    /// 自定义超时返回 JSON(HTTP 200);未设回退默认 504 壳。
    timeout_body: OnceLock<Value>,
    /// 自定义限流降级 fn;优先级高于 reject_body。
    reject_fb: OnceLock<FallbackFn>,
    /// 自定义超时降级 fn;优先级高于 timeout_body。
    timeout_fb: OnceLock<FallbackFn>,
    /// 真实接口路由(周期请求日志与 `nafana_command_info{path}` 显示);未设回退 command 名。
    path: OnceLock<String>,
    /// 周期请求日志行尾附加信息生成器。
    extra: OnceLock<Box<dyn Fn(u64) -> String + Send + Sync>>,
}

/// 执行区 RAII 守卫：构造时增加并发；Drop 时归还，并为未完成的 future 记录 canceled。
struct InflightGuard<'a> {
    command: &'a Command,
    completed: bool,
}

impl<'a> InflightGuard<'a> {
    /// 业务作用：进入执行区:当前并发 +1、抬终身峰值,返回 (守卫, 进入后的并发数)。
    ///
    /// # 参数
    /// - `command`: 当前执行所属命令。
    fn enter(command: &'a Command) -> (Self, u64) {
        let cur = command.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        command
            .inflight_lifetime_max
            .fetch_max(cur, Ordering::Relaxed);
        (
            Self {
                command,
                completed: false,
            },
            cur.max(0) as u64,
        )
    }

    /// 业务作用：标记执行已经产生 success/failure/timeout 结局，Drop 时不再补 canceled。
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for InflightGuard<'_> {
    /// 业务作用：离开执行区(正常返回/超时/取消)时归还并发计数。
    fn drop(&mut self) {
        self.command.inflight.fetch_sub(1, Ordering::Relaxed);
        if !self.completed {
            self.command
                .counters
                .canceled
                .fetch_add(1, Ordering::Relaxed);
            self.command.add_tps();
            self.command.rolling_state().record(Outcome::Canceled);
        }
    }
}

/// 业务作用：并发上限归一化:0 视为"不限"，超大值收敛到 Tokio semaphore 可表达的上限。
fn normalize_max_concurrent(v: Option<usize>) -> Option<usize> {
    v.filter(|n| *n > 0)
        .map(|limit| limit.min(tokio::sync::Semaphore::MAX_PERMITS))
}

/// 业务作用：超时归一化:0 时长视为"不超时"，超长值收敛到运行时安全上限。
fn normalize_timeout(v: Option<Duration>) -> Option<Duration> {
    v.filter(|d| !d.is_zero())
        .map(|duration| duration.min(MAX_COMMAND_TIMEOUT))
}

impl Command {
    /// 业务作用：取得滚动窗口锁；即使此前持锁线程 panic，也继续使用其中可恢复的数据。
    fn rolling_state(&self) -> std::sync::MutexGuard<'_, RollingWindow> {
        self.rolling
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 业务作用：内部构造:只建实例与周期汇总任务,不进注册表(注册/去重在 monitor 系入口)。
    ///
    /// # 参数
    /// - `command`: 命令名。
    /// - `group`: 分组名。
    /// - `max_concurrent`: 已归一化的并发上限。
    /// - `timeout`: 已归一化的单请求超时。
    /// - `tps_weight`: TPS 权重标记。
    fn build(
        command: &str,
        group: &str,
        max_concurrent: Option<usize>,
        timeout: Option<Duration>,
        tps_weight: Option<u64>,
    ) -> Arc<Self> {
        let cmd = Arc::new(Self {
            command: command.to_string(),
            group: group.to_string(),
            max_concurrent,
            timeout,
            tps_weight,
            sem: max_concurrent.map(tokio::sync::Semaphore::new),
            inflight: AtomicI64::new(0),
            inflight_lifetime_max: AtomicI64::new(0),
            rolling: Mutex::new(RollingWindow::new()),
            counters: CommandCounters::default(),
            histogram: LatencyHistogram::new(),
            reject_body: OnceLock::new(),
            timeout_body: OnceLock::new(),
            reject_fb: OnceLock::new(),
            timeout_fb: OnceLock::new(),
            path: OnceLock::new(),
            extra: OnceLock::new(),
        });
        // 每命令一个独立 10s 请求汇总任务,相位锚定创建时刻自然错开
        // Weak 持有:命令若被回收任务自行退出;仅在 tokio runtime 内才启动。
        if tokio::runtime::Handle::try_current().is_ok() {
            let weak = Arc::downgrade(&cmd);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
                // 吃掉立即触发的首拍,首打落在 t0+10s。
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    match weak.upgrade() {
                        Some(c) => c.log_cost(),
                        None => break,
                    }
                }
            });
        }
        cmd
    }

    /// 业务作用：【最通用构造器】创建或复用命令(带 0 值归一化 + 注册表去重)。
    ///
    /// 同 key(group+command)重复调用:参数一致返回既有实例;参数不同 `warn` 后
    /// **复用既有实例**(合同)。要感知冲突用 [`Command::try_monitor`]。
    ///
    /// # 参数
    /// - `command`: 命令名(面板 tile 标题)。
    /// - `group`: 分组名。
    /// - `max_concurrent`: 可选并发上限;None 或 Some(0) = 不限。
    /// - `timeout`: 可选单请求超时;None 或 0 时长 = 不超时。
    /// - `tps_weight`: 可选 TPS 权重;None = 不计,Some(0) = 标记但权重 0。
    pub fn monitor(
        command: &str,
        group: &str,
        max_concurrent: Option<usize>,
        timeout: Option<Duration>,
        tps_weight: Option<u64>,
    ) -> Arc<Self> {
        match Self::try_monitor(command, group, max_concurrent, timeout, tps_weight) {
            Ok(cmd) => cmd,
            Err((conflict, existing)) => {
                if group == "macro" {
                    tracing::warn!(
                        "{conflict}; duplicate grafana command name, later parameters, path and fallback settings are ignored"
                    );
                } else {
                    tracing::warn!("{conflict},本次参数被忽略");
                }
                existing
            }
        }
    }

    /// 业务作用：同 [`Command::monitor`],但同 key 异参时返回 `Err((冲突, 既有实例))` 供显式调用方感知。
    ///
    /// # 参数
    /// - `command`: 命令名。
    /// - `group`: 分组名。
    /// - `max_concurrent`: 可选并发上限。
    /// - `timeout`: 可选单请求超时。
    /// - `tps_weight`: 可选 TPS 权重。
    #[allow(clippy::type_complexity)]
    pub fn try_monitor(
        command: &str,
        group: &str,
        max_concurrent: Option<usize>,
        timeout: Option<Duration>,
        tps_weight: Option<u64>,
    ) -> Result<Arc<Self>, (MonitorConflict, Arc<Self>)> {
        let max_concurrent = normalize_max_concurrent(max_concurrent);
        let timeout = normalize_timeout(timeout);
        let key = CommandKey {
            group: group.to_string(),
            command: command.to_string(),
        };
        let params = CommandParams {
            max_concurrent,
            timeout,
            tps_weight,
        };
        registry::get_or_register(key, params, || {
            Self::build(command, group, max_concurrent, timeout, tps_weight)
        })
    }

    /// 业务作用：创建命令，不计入 TPS。
    /// `max_concurrent = 0` / 零时长 `timeout` 分别表示不限并发/不超时(合同)。
    ///
    /// # 参数
    /// - `command`: 命令名。
    /// - `group`: 分组名。
    /// - `max_concurrent`: 并发上限(0 = 不限)。
    /// - `timeout`: 单请求超时(零时长 = 不超时)。
    pub fn new(command: &str, group: &str, max_concurrent: usize, timeout: Duration) -> Arc<Self> {
        Self::monitor(command, group, Some(max_concurrent), Some(timeout), None)
    }

    /// 业务作用：同 [`Command::new`]，但计入 TPS。
    ///
    /// # 参数
    /// - `command`: 命令名。
    /// - `group`: 分组名。
    /// - `max_concurrent`: 并发上限(0 = 不限)。
    /// - `timeout`: 单请求超时(零时长 = 不超时)。
    /// - `weight`: 每个请求给 TPS 累加的权重。
    pub fn with_tps(
        command: &str,
        group: &str,
        max_concurrent: usize,
        timeout: Duration,
        weight: u64,
    ) -> Arc<Self> {
        Self::monitor(
            command,
            group,
            Some(max_concurrent),
            Some(timeout),
            Some(weight),
        )
    }

    /// 业务作用：给本命令挂周期请求日志行尾的附加信息生成器(只能设一次,重复设忽略)。
    ///
    /// # 参数
    /// - `f`: 闭包 `Fn(period_ms) -> String`,入参为统计周期毫秒数。
    pub fn set_extra<F>(&self, f: F)
    where
        F: Fn(u64) -> String + Send + Sync + 'static,
    {
        let _ = self.extra.set(Box::new(f));
    }

    /// 业务作用：设置真实接口路由,影响周期请求日志与 `nafana_command_info{path}` 显示;只能设一次。
    ///
    /// # 参数
    /// - `path`: 真实接口路由字符串。
    pub fn set_path(&self, path: &str) {
        let _ = self.path.set(path.to_string());
    }

    /// 业务作用：【自定义限流返回】设 bulkhead 满时返回的 JSON body(以 HTTP 200 返回)。
    /// 非法 JSON 静默忽略(回退默认 429 壳);要感知失败用 [`Command::try_set_reject_response_str`]。
    ///
    /// # 参数
    /// - `json`: bulkhead 拒绝时返回的 JSON body 字符串。
    pub fn set_reject_response_str(&self, json: &str) {
        let _ = self.try_set_reject_response_str(json);
    }

    /// 业务作用：同 [`Command::set_reject_response_str`],但返回结果:非法 JSON → `Err`;
    /// 本次设入 → `Ok(true)`;之前已设过 → `Ok(false)`。
    ///
    /// # 参数
    /// - `json`: bulkhead 拒绝时返回的 JSON body 字符串。
    pub fn try_set_reject_response_str(&self, json: &str) -> Result<bool, serde_json::Error> {
        let v = serde_json::from_str::<Value>(json)?;
        Ok(self.reject_body.set(v).is_ok())
    }

    /// 业务作用：【自定义超时返回】设超时时返回的 JSON body(以 HTTP 200 返回);用法同 reject 侧。
    ///
    /// # 参数
    /// - `json`: 超时时返回的 JSON body 字符串。
    pub fn set_timeout_response_str(&self, json: &str) {
        let _ = self.try_set_timeout_response_str(json);
    }

    /// 业务作用：同 [`Command::set_timeout_response_str`],但返回结果供调用方感知。
    ///
    /// # 参数
    /// - `json`: 超时时返回的 JSON body 字符串。
    pub fn try_set_timeout_response_str(&self, json: &str) -> Result<bool, serde_json::Error> {
        let v = serde_json::from_str::<Value>(json)?;
        Ok(self.timeout_body.set(v).is_ok())
    }

    /// 业务作用：【自定义限流降级 fn】bulkhead 满时调用,优先级高于 reject_response;只能设一次。
    ///
    /// # 参数
    /// - `fb`: 产出 Response 的异步降级闭包。
    pub fn set_reject_fn(&self, fb: FallbackFn) {
        let _ = self.reject_fb.set(fb);
    }

    /// 业务作用：【自定义超时降级 fn】超时时调用,优先级高于 timeout_response;只能设一次。
    ///
    /// # 参数
    /// - `fb`: 产出 Response 的异步降级闭包。
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
                    return fallback().await;
                }
                if let Some(body) = self.reject_body.get() {
                    return custom_response(body);
                }
            }
            FallbackCause::ExecutionTimeout { .. } => {
                if let Some(fallback) = self.timeout_fb.get() {
                    return fallback().await;
                }
                if let Some(body) = self.timeout_body.get() {
                    return custom_response(body);
                }
            }
        }

        let path = self.path.get().map(String::as_str).unwrap_or(&self.command);
        let context =
            FallbackContext::new(&self.command, &self.group, path, self.tps_weight, cause);
        match execute_global_fallback(context) {
            GlobalFallbackExecution::Handled(response) => {
                self.counters
                    .global_fallback_handled
                    .fetch_add(1, Ordering::Relaxed);
                return response;
            }
            GlobalFallbackExecution::UseBuiltin => {
                self.counters
                    .global_fallback_builtin
                    .fetch_add(1, Ordering::Relaxed);
            }
            GlobalFallbackExecution::NotInstalled => {}
            GlobalFallbackExecution::InvalidConfiguration => {
                self.counters
                    .global_fallback_failed
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    command = %self.command,
                    ?cause,
                    "全局降级配置冲突，使用内置终态响应"
                );
            }
            GlobalFallbackExecution::Panicked => {
                self.counters
                    .global_fallback_failed
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                command = %self.command,
                ?cause,
                    "全局降级处理器发生 panic，使用内置终态响应"
                );
            }
            GlobalFallbackExecution::Recursive => {
                self.counters
                    .global_fallback_failed
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    command = %self.command,
                    ?cause,
                    "全局降级发生递归调用，使用内置终态响应"
                );
            }
        }

        match cause {
            FallbackCause::BulkheadRejected { max_concurrent, .. } => {
                rejected_response(&self.command, max_concurrent)
            }
            FallbackCause::ExecutionTimeout { timeout, .. } => {
                timeout_response(&self.command, timeout)
            }
        }
    }

    /// 业务作用：【中间件形态】用本命令保护下游,`run_fn` 的薄封装。
    ///
    /// # 参数
    /// - `req`: HTTP 请求对象。
    /// - `next`: axum 放行句柄。
    pub async fn run(self: Arc<Self>, req: Request, next: Next) -> Response {
        self.run_fn(move || next.run(req)).await
    }

    /// 业务作用：【通用执行入口】用 bulkhead 与超时保护任意异步执行体，并把主结局和降级结局写入双轨指标。
    ///
    /// 执行顺序(合同):try_acquire(满 → 拒) → 并发 gauge/峰值采样 → (可选)超时
    /// 包裹执行 → 结局归类(5xx=failure,429/504 由本组件自己产出不参与归类) → 双轨记录。
    /// 调用方丢弃执行 future 时由 RAII 守卫补记 canceled，并归还 inflight。
    ///
    /// # 参数说明
    ///
    /// - `f`: 被保护的异步执行体,只有通过 bulkhead 后才会被调用。
    ///
    /// # 返回
    ///
    /// 返回业务响应、局部降级响应、全局降级响应或组件内置拒绝/超时响应。
    pub async fn run_fn<F, Fut>(self: Arc<Self>, f: F) -> Response
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Response>,
    {
        // ── 1. bulkhead:try_acquire 拿不到许可立刻拒绝(不排队) ──
        let permit = match &self.sem {
            None => None,
            Some(sem) => match sem.try_acquire() {
                Ok(p) => Some(p),
                Err(_) => {
                    // 被拒也是一次请求:单调 counter + 滚动桶 + fallback + TPS 全记
                    // (合同);不进 inflight/延迟统计。
                    self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                    self.counters.fallback.fetch_add(1, Ordering::Relaxed);
                    self.add_tps();
                    {
                        let mut st = self.rolling_state();
                        st.record(Outcome::Rejected);
                        st.record_fallback();
                    }
                    let max = self.max_concurrent.unwrap_or(0);
                    tracing::warn!(
                        command = %self.command,
                        max = max,
                        "bulkhead full, rejecting request (429)"
                    );
                    let inflight = self.inflight.load(Ordering::Relaxed).max(0) as usize;
                    return self
                        .resolve_fallback(FallbackCause::BulkheadRejected {
                            max_concurrent: max,
                            current_inflight: inflight,
                        })
                        .await;
                }
            },
        };

        // ── 2. 并发 gauge +1(RAII,取消安全)+ 终身峰值 + 滚动峰值采样 ──
        let (mut guard, current) = InflightGuard::enter(&self);
        self.rolling_state().observe_inflight(current);

        // ── 3. 执行下游,可选限时 ──
        let start = Instant::now();
        let result = match self.timeout {
            Some(d) => tokio::time::timeout(d, f()).await,
            None => Ok(f().await),
        };
        let elapsed = start.elapsed();
        // 纳秒样本避免快速端点在整数毫秒转换时全部变成 0；导出阶段再统一换算成毫秒。
        let latency_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let latency_ms = latency_ns as f64 / 1_000_000.0;

        // ── 4. 离开受保护执行区。fallback 不占 bulkhead 许可,也不计入业务执行并发。 ──
        drop(permit);
        guard.complete();
        drop(guard);

        // ── 5. 结局归类 + 双轨记录 ──
        self.add_tps();
        self.histogram.observe(elapsed);
        match result {
            Ok(resp) => {
                let outcome = if resp.status().is_server_error() {
                    self.counters.failure.fetch_add(1, Ordering::Relaxed);
                    Outcome::Failure
                } else {
                    self.counters.success.fetch_add(1, Ordering::Relaxed);
                    Outcome::Success
                };
                self.rolling_state().record(outcome);
                tracing::debug!("⏱ [{}] latency={:.3}ms", self.command, latency_ms);
                resp
            }
            Err(_elapsed) => {
                let dur = self.timeout.unwrap_or_default();
                self.counters.timeout.fetch_add(1, Ordering::Relaxed);
                self.counters.fallback.fetch_add(1, Ordering::Relaxed);
                {
                    let mut st = self.rolling_state();
                    st.record(Outcome::Timeout);
                    st.record_fallback();
                }
                tracing::warn!(
                    command = %self.command,
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

    /// 业务作用：若本命令标了 TPS,给单调 TPS 计数按权重累加(Some(0) 权重为 0,自然无增量)。
    fn add_tps(&self) {
        if let Some(w) = self.tps_weight {
            if w > 0 {
                self.counters.tps.fetch_add(w, Ordering::Relaxed);
            }
        }
    }

    /// 业务作用：每 10s 打一行最近窗口的请求结局汇总。
    /// 窗口内没有执行样本就不打,避免刷空日志。
    fn log_cost(&self) {
        let w = self.rolling_state().snapshot();
        let count = w.success + w.failure + w.timeout;
        if count == 0 {
            return;
        }
        let extra = self
            .extra
            .get()
            .map(|f| f(WINDOW_SECS * 1000))
            .unwrap_or_default();
        let rejected = if w.rejected > 0 {
            format!(" rejected {}", w.rejected)
        } else {
            String::new()
        };
        let canceled = if w.canceled > 0 {
            format!(" canceled {}", w.canceled)
        } else {
            String::new()
        };
        let label = self.path.get().map(|s| s.as_str()).unwrap_or(&self.command);
        tracing::info!(
            "{} {}次/{}s{}{}{}",
            label,
            count,
            WINDOW_SECS,
            rejected,
            canceled,
            if extra.is_empty() {
                String::new()
            } else {
                format!(" {extra}")
            },
        );
    }

    /// 业务作用：导出 `/metrics` 所需的主请求、全局降级、TPS、并发与延迟一致性快照。
    ///
    /// # 参数说明
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回当前命令的只读渲染视图；仅在取滚动窗口快照时短暂持锁。
    pub(crate) fn export(&self) -> CommandExport {
        let window = self.rolling_state().snapshot();
        let inflight = self.inflight.load(Ordering::Relaxed).max(0) as u64;
        CommandExport {
            command: self.command.clone(),
            group: self.group.clone(),
            path: self
                .path
                .get()
                .cloned()
                .unwrap_or_else(|| self.command.clone()),
            max_concurrent: self.max_concurrent.unwrap_or(0) as u64,
            timeout_ms: self.timeout.map_or(0, |d| d.as_millis() as u64),
            tps_weight: self.tps_weight.unwrap_or(0),
            success: self.counters.success.load(Ordering::Relaxed),
            failure: self.counters.failure.load(Ordering::Relaxed),
            timeout: self.counters.timeout.load(Ordering::Relaxed),
            rejected: self.counters.rejected.load(Ordering::Relaxed),
            canceled: self.counters.canceled.load(Ordering::Relaxed),
            fallback: self.counters.fallback.load(Ordering::Relaxed),
            global_fallback_handled: self
                .counters
                .global_fallback_handled
                .load(Ordering::Relaxed),
            global_fallback_builtin: self
                .counters
                .global_fallback_builtin
                .load(Ordering::Relaxed),
            global_fallback_failed: self.counters.global_fallback_failed.load(Ordering::Relaxed),
            tps: self.counters.tps.load(Ordering::Relaxed),
            inflight,
            rolling_max_inflight: window.rolling_max_inflight.max(inflight),
            lifetime_max_inflight: self.inflight_lifetime_max.load(Ordering::Relaxed).max(0) as u64,
            histogram: self.histogram.export(),
        }
    }

    /// 业务作用：本命令窗口内的请求总数 × TPS 权重(current_tps 的被加项);未标 TPS 返回 None。
    fn tps_window_contribution(&self) -> Option<u64> {
        self.tps_weight.map(|w| {
            let request_count = self.rolling_state().request_count();
            request_count * w
        })
    }
}

/// 业务作用：读取当前全局 TPS(每秒事务数):所有标了 TPS 的命令窗口请求数(×权重)之和 / 窗口秒数。
/// 给周期请求日志行尾使用的当前 TPS；Prometheus 侧请用
/// `sum(rate(nafana_tps_total[10s]))`，两者只差取整级别。
pub fn current_tps() -> f64 {
    let total: u64 = registry::all()
        .iter()
        .filter_map(|c| c.tps_window_contribution())
        .sum();
    total as f64 / WINDOW_SECS as f64
}

/// `/metrics` 渲染视图:一个命令的全量导出数据。
pub(crate) struct CommandExport {
    /// 命令名(`command` 标签)。
    pub(crate) command: String,
    /// 分组名(`group` 标签)。
    pub(crate) group: String,
    /// 真实路由(`nafana_command_info{path}`)。
    pub(crate) path: String,
    /// 并发上限(0 = 不限)。
    pub(crate) max_concurrent: u64,
    /// 超时毫秒(0 = 不超时)。
    pub(crate) timeout_ms: u64,
    /// TPS 权重(0 = 未标或权重 0)。
    pub(crate) tps_weight: u64,
    /// 成功累计。
    pub(crate) success: u64,
    /// 失败累计。
    pub(crate) failure: u64,
    /// 超时累计。
    pub(crate) timeout: u64,
    /// 拒绝累计。
    pub(crate) rejected: u64,
    /// 执行被取消或中止累计。
    pub(crate) canceled: u64,
    /// 降级累计。
    pub(crate) fallback: u64,
    /// 全局降级成功响应累计。
    pub(crate) global_fallback_handled: u64,
    /// 全局处理器主动使用内置响应累计。
    pub(crate) global_fallback_builtin: u64,
    /// 全局降级执行失败累计。
    pub(crate) global_fallback_failed: u64,
    /// TPS 累计。
    pub(crate) tps: u64,
    /// 当前并发。
    pub(crate) inflight: u64,
    /// 10s 滚动并发峰值。
    pub(crate) rolling_max_inflight: u64,
    /// 进程终身并发峰值。
    pub(crate) lifetime_max_inflight: u64,
    /// 直方图渲染视图。
    pub(crate) histogram: crate::counters::HistogramExport,
}

/// 业务作用：bulkhead 满时的默认 429 响应壳。
///
/// # 参数
/// - `name`: 命令名,拼进提示信息。
/// - `max`: 并发上限,拼进提示信息。
fn rejected_response(name: &str, max: usize) -> Response {
    let body = Json(json!({
        "code": 429,
        "message": format!("[{name}] bulkhead full (max_concurrent={max}), rejected, try again later"),
        "data": null,
    }));
    (StatusCode::TOO_MANY_REQUESTS, body).into_response()
}

/// 业务作用：超时时的默认 504 响应壳。
///
/// # 参数
/// - `name`: 命令名,拼进提示信息。
/// - `dur`: 超时时长,拼进提示信息。
fn timeout_response(name: &str, dur: Duration) -> Response {
    let body = Json(json!({
        "code": 504,
        "message": format!("[{name}] execution timed out after {}ms", dur.as_millis()),
        "data": null,
    }));
    (StatusCode::GATEWAY_TIMEOUT, body).into_response()
}

/// 业务作用：自定义限流/超时返回:HTTP 200 + 用户给的 JSON body(业务码放 body 里,
/// 前端 axios 不因 4xx/5xx 抛错。
///
/// # 参数
/// - `body`: 拒绝/超时时返回的 JSON body。
fn custom_response(body: &Value) -> Response {
    (StatusCode::OK, Json(body.clone())).into_response()
}
