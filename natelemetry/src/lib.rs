//! NASA 遥测核心：W3C Trace Context 传播、根采样裁决、有界 span 导出队列与停机 flush。
//!
//! # 传播与采样边界
//!
//! 合法上游上下文严格继承 sampled 位；没有合法上游时，只有 [`SpanRecorder`] 绑定的 exporter
//! sampler 可以裁决新根。仅提供传播能力的入口必须创建未采样根，避免替下游声明已采样。
//! 已采样记录通过非阻塞有界队列导出，容量耗尽或 flush 超时增加 dropped，不反向阻塞业务。
//!
//! 本 crate 保持 provider-neutral，不建立全局 tracer provider，也不依赖 `napp`。OTLP 编码、HTTP
//! sink、指标出口和组件生命周期由上层装配；span 名与属性必须维持低基数、有界且不含敏感内容。

#![forbid(unsafe_code)]

mod context;
mod export;
mod trace;

pub use context::{ambient, with_ambient, with_ambient_sync};
pub use export::{
    flush_within, BoundedSpanExporter, ExportOutcome, ExporterSnapshot, FlushOutcome,
    InvalidSampleRatio, SpanAttribute, SpanGuard, SpanKind, SpanRecord, SpanRecorder,
};
pub use trace::{random_span_id, TraceContext};
