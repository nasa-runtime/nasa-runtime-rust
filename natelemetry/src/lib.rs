//! NASA 遥测核心:W3C trace context 传播、有界 span 导出队列与停机 flush。
//!
//! 本 crate 只拥有**provider-neutral 的传播与导出机制**,不绑定具体 OTLP/OpenTelemetry SDK,也
//! **不依赖 `napp`**(否则接入时形成反向依赖)。OTLP/exporter 后端由后续增量在此之上装配。

#![forbid(unsafe_code)]

mod export;
mod trace;

pub use export::{
    flush_within, BoundedSpanExporter, ExportOutcome, ExporterSnapshot, FlushOutcome,
    InvalidSampleRatio, SpanGuard, SpanKind, SpanRecord, SpanRecorder,
};
pub use trace::{random_span_id, TraceContext};
