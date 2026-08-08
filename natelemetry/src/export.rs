//! 有界 span 导出队列与停机 flush。
//!
//! 请求路径只做**非阻塞入队**;满则丢弃并计数(不无限缓冲、不背压拖垮业务)。停机时停止接收新
//! span,再在统一 deadline 内 flush 已缓冲的;超时把未导出的计为丢弃后继续退出。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;

use crate::{random_span_id, TraceContext};

/// 停机导出 deadline 的硬上限；更大的值没有运维意义且可能使 `Instant` 加法溢出。
pub const MAX_TELEMETRY_FLUSH_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// span 种类(OTLP `SpanKind` 子集,治理层只区分入站/内部/出站三类)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    /// 入站服务端 span(Web 请求入口)。
    Server,
    /// 进程内业务子操作(显式插桩的 db/redis/kafka 调用等)。
    Internal,
    /// 出站客户端 span(调下游)。
    Client,
    /// 出站消息发布 span。
    Producer,
    /// 入站消息消费/handler span。
    Consumer,
}

impl SpanKind {
    /// 业务作用：OTLP proto `SpanKind` 枚举值(INTERNAL=1,SERVER=2,CLIENT=3;两种 wire 编码共用)。
    pub fn otlp_value(self) -> u8 {
        match self {
            SpanKind::Internal => 1,
            SpanKind::Server => 2,
            SpanKind::Client => 3,
            SpanKind::Producer => 4,
            SpanKind::Consumer => 5,
        }
    }
}

/// 一条待导出的 span 记录(最小治理层字段;OTLP 具体字段由后续 exporter 增量映射)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanRecord {
    /// span 名(低基数操作名)。
    pub name: String,
    /// 所属 trace-id 十六进制。
    pub trace_id_hex: String,
    /// 本 span-id 十六进制。
    pub span_id_hex: String,
    /// 上游 parent span id；根 span 为 `None`。
    pub parent_span_id_hex: Option<String>,
    /// span 种类(入站/内部/出站),决定 OTLP `kind` 字段——业务子 span 不得冒充服务端 span。
    pub kind: SpanKind,
    /// 真实开始时间(epoch 纳秒)。
    pub start_unix_nano: u64,
    /// 真实结束时间(epoch 纳秒，必须不早于开始)。
    pub end_unix_nano: u64,
    /// 可选 HTTP 状态码；server span 用于映射 OTLP status。
    pub http_status_code: Option<u16>,
}

/// 一次 span 入队结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportOutcome {
    /// 已入有界队列。
    Enqueued,
    /// 队列已满,本条丢弃(已计入 `dropped`)。
    DroppedQueueFull,
    /// 导出侧已停(停机),本条丢弃。
    DroppedClosed,
    /// 父链路未采样；只传播上下文，不计入 exporter 丢弃量。
    DroppedUnsampled,
}

/// 根链路采样率配置非法。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSampleRatio;

impl std::fmt::Display for InvalidSampleRatio {
    /// 业务作用：返回不包含配置原值的稳定校验说明。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("root sample ratio must be finite and between 0.0 and 1.0")
    }
}

impl std::error::Error for InvalidSampleRatio {}

/// 停机 flush 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushOutcome {
    /// 在 deadline 内成功导出的 span 数。
    pub exported: u64,
    /// deadline 到时仍未导出、被丢弃的 span 数。
    pub dropped: u64,
}

/// 业务/管理端可读取的有界 exporter 摘要。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExporterSnapshot {
    /// 已入队但尚未由 sink 成功确认或记为丢弃的 span 数。
    pub pending: u64,
    /// 因队列满、关闭、sink 失败或停机超时累计丢弃的 span 数。
    pub dropped: u64,
}

/// 有界 span 导出器:业务侧 `export` 只做非阻塞入队;单独的 flush 侧负责真正导出。
pub struct BoundedSpanExporter {
    sender: tokio::sync::mpsc::Sender<SpanRecord>,
    dropped: Arc<AtomicU64>,
    pending: Arc<AtomicU64>,
    /// 新根链路采样阈值；0=全关，u64::MAX=全开，其余与均匀随机数比较。
    root_sample_threshold: u64,
}

/// 可克隆的非阻塞 span 记录入口。
///
/// 领域 crate 只持有本类型，不取得 exporter 接收端、flush 或关闭权限。开始 span 时立即派生唯一
/// 子上下文；[`SpanGuard`] 正常完成或被取消 Drop 时都只做一次 `try_send`。
#[derive(Clone)]
pub struct SpanRecorder {
    exporter: Arc<BoundedSpanExporter>,
}

impl SpanRecorder {
    /// 业务作用：从 Application 拥有的 exporter 创建只写记录器。
    pub fn new(exporter: Arc<BoundedSpanExporter>) -> Self {
        Self { exporter }
    }

    /// 业务作用：开始一个以 `parent` 为父的 span，并返回用于传播的子上下文与完成 guard。
    pub fn start(
        &self,
        name: impl Into<String>,
        parent: &TraceContext,
        kind: SpanKind,
    ) -> SpanGuard {
        SpanGuard {
            recorder: self.clone(),
            name: Some(name.into()),
            parent: *parent,
            context: parent.child(random_span_id()),
            kind,
            start_unix_nano: unix_nanos_now(),
        }
    }
}

impl std::fmt::Debug for SpanRecorder {
    /// 业务作用：仅输出 exporter 健康摘要，不展示 span 名、属性或下游 endpoint。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpanRecorder")
            .field("snapshot", &self.exporter.snapshot())
            .finish()
    }
}

/// 一个进行中的 span。Drop 会自动以无状态码完成，因此超时、取消和提前返回不会泄漏 span。
pub struct SpanGuard {
    recorder: SpanRecorder,
    name: Option<String>,
    parent: TraceContext,
    context: TraceContext,
    kind: SpanKind,
    start_unix_nano: u64,
}

impl SpanGuard {
    /// 业务作用：返回应向下游传播的子上下文。
    pub fn context(&self) -> TraceContext {
        self.context
    }

    /// 业务作用：完成 span；HTTP client/server 可附加稳定状态码，其他协议传 `None`。
    pub fn finish(mut self, http_status_code: Option<u16>) -> ExportOutcome {
        self.export(http_status_code)
    }

    /// 业务作用：取走 span 名并只提交一次完整记录；重复完成按 closed 丢弃处理。
    fn export(&mut self, http_status_code: Option<u16>) -> ExportOutcome {
        let Some(name) = self.name.take() else {
            return ExportOutcome::DroppedClosed;
        };
        if !self.context.is_sampled() {
            return ExportOutcome::DroppedUnsampled;
        }
        self.recorder.exporter.export(SpanRecord {
            name,
            trace_id_hex: self.context.trace_id_hex(),
            span_id_hex: self.context.parent_id_hex(),
            parent_span_id_hex: Some(self.parent.parent_id_hex()),
            kind: self.kind,
            start_unix_nano: self.start_unix_nano,
            end_unix_nano: unix_nanos_now().max(self.start_unix_nano),
            http_status_code,
        })
    }
}

impl Drop for SpanGuard {
    /// 业务作用：调用方未显式 finish 时仍提交无 HTTP 状态的 span，覆盖提前返回与取消路径。
    fn drop(&mut self) {
        let _ = self.export(None);
    }
}

/// 业务作用：读取当前 UNIX 纳秒并饱和到 u64，系统时间早于 epoch 时回退为零。
fn unix_nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

impl BoundedSpanExporter {
    /// 业务作用：创建导出器与接收端；非 fallible 入口把容量收敛到 Tokio 有界队列可表达范围。
    pub fn channel(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<SpanRecord>) {
        let capacity = capacity.clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (
            Self {
                sender,
                dropped: Arc::new(AtomicU64::new(0)),
                pending: Arc::new(AtomicU64::new(0)),
                root_sample_threshold: u64::MAX,
            },
            receiver,
        )
    }

    /// 业务作用：在 exporter 发布给生产者之前冻结新根链路采样率。
    ///
    /// `0.0` 表示只传播不记录新根，`1.0` 表示全部记录；上游已有 traceparent 时始终沿用其 sampled 位。
    pub fn set_root_sample_ratio(&mut self, ratio: f64) -> Result<(), InvalidSampleRatio> {
        if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
            return Err(InvalidSampleRatio);
        }
        self.root_sample_threshold = if ratio == 0.0 {
            0
        } else if ratio == 1.0 {
            u64::MAX
        } else {
            (ratio * u64::MAX as f64) as u64
        };
        Ok(())
    }

    /// 业务作用：按冻结的采样率裁决一条没有上游上下文的新根链路。
    pub fn should_sample_root(&self) -> bool {
        match self.root_sample_threshold {
            0 => false,
            u64::MAX => true,
            threshold => rand::thread_rng().gen::<u64>() <= threshold,
        }
    }

    /// 业务作用：非阻塞入队一条 span;满或已停即丢弃并计数(绝不阻塞、不 panic)。
    pub fn export(&self, span: SpanRecord) -> ExportOutcome {
        // 必须先计 pending 再发送；否则 receiver 可能在 send 成功与计数之间完成导出，留下幽灵 pending。
        self.pending.fetch_add(1, Ordering::Relaxed);
        match self.sender.try_send(span) {
            Ok(()) => ExportOutcome::Enqueued,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                subtract_saturating(&self.pending, 1);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                ExportOutcome::DroppedQueueFull
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                subtract_saturating(&self.pending, 1);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                ExportOutcome::DroppedClosed
            }
        }
    }

    /// 业务作用：累计丢弃的 span 数(停机诊断用:"超时记录丢弃数后继续退出")。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 业务作用：当前已经入队、但尚未由 sink 成功确认或记为丢弃的 span 数。
    pub fn pending(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// 业务作用：sink 成功导出一批 span。
    pub fn record_exported(&self, count: u64) {
        subtract_saturating(&self.pending, count);
    }

    /// 业务作用：sink/flush 在入队之后丢弃 span 时同时减少 pending 并累计 dropped。
    pub fn record_dropped(&self, count: u64) {
        subtract_saturating(&self.pending, count);
        self.dropped.fetch_add(count, Ordering::Relaxed);
    }

    /// 业务作用：停机预算耗尽并终止 drainer 后，把全部未决 span 原子记为丢弃。
    pub fn drop_all_pending(&self) -> u64 {
        let pending = self.pending.swap(0, Ordering::AcqRel);
        self.dropped.fetch_add(pending, Ordering::Relaxed);
        pending
    }

    /// 业务作用：返回不包含 endpoint、span 名或业务属性的只读健康摘要。
    pub fn snapshot(&self) -> ExporterSnapshot {
        ExporterSnapshot {
            pending: self.pending(),
            dropped: self.dropped(),
        }
    }
}

/// 业务作用：对原子 pending 计数做饱和扣减，容忍并发 drain/drop 的交错。
fn subtract_saturating(counter: &AtomicU64, count: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(count))
    });
}

/// 业务作用：停机 flush:在 `deadline` 内排空**已缓冲**的 span,对每条 `await export_one`(真实用途里是发往
/// OTLP 的 I/O);每条导出前检查 deadline,超时即停,把仍缓冲的计为 dropped 返回。
///
/// 调用前应先释放所有 [`BoundedSpanExporter`](停止接收新 span)。只 flush 停机时刻已缓冲的,不等待
/// 新 span 到达。
///
/// # 参数
///
/// - `receiver`:span 接收端(由 flush 消费)。
/// - `deadline`:flush 总预算。
/// - `export_one`:对每条 span 的异步导出动作(验证时可用 sleep 模拟 I/O 耗时)。
pub async fn flush_within<F, Fut>(
    mut receiver: tokio::sync::mpsc::Receiver<SpanRecord>,
    deadline: Duration,
    mut export_one: F,
) -> FlushOutcome
where
    F: FnMut(SpanRecord) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let deadline_at = tokio::time::Instant::now() + deadline.min(MAX_TELEMETRY_FLUSH_DURATION);
    let mut exported = 0u64;
    loop {
        if tokio::time::Instant::now() >= deadline_at {
            break; // 到达 deadline
        }
        match receiver.try_recv() {
            Ok(span) => {
                match tokio::time::timeout_at(deadline_at, export_one(span)).await {
                    Ok(()) => exported += 1,
                    Err(_) => {
                        // 当前正在导出的 span 也已因 deadline 放弃。
                        let mut dropped = 1u64;
                        while receiver.try_recv().is_ok() {
                            dropped += 1;
                        }
                        return FlushOutcome { exported, dropped };
                    }
                }
            }
            // 队列已排空(senders 已释放或暂无缓冲):停机 flush 不等待新 span。
            Err(_) => break,
        }
    }
    let mut dropped = 0u64;
    while receiver.try_recv().is_ok() {
        dropped += 1;
    }
    FlushOutcome { exported, dropped }
}
