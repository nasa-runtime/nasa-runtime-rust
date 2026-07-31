//! W3C Trace Context 传播中间件:把 [`natelemetry`] 的链路上下文接到 Web 入口。
//!
//! 服务入口的职责(对齐 W3C Trace Context 对服务端的要求):解析入站 `traceparent`——有效则**沿用同一 trace-id**
//! 派生一个新的服务端 span([`TraceContext::child`]);缺失或非法则开启新链路([`TraceContext::new_root`])。
//! 当前上下文写入请求扩展 [`TraceContext`],供 handler 在调用下游时以 `to_traceparent()` 继续透传;
//! 同时把 trace-id 回写响应头 `trace-id` 便于日志/客户端关联(非 W3C 标准,仅关联用途)。
//!
//! 装在治理链靠前位置(request-id 附近),使后续所有观测都落在同一 span 下。

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use natelemetry::{random_span_id, TraceContext};
#[cfg(feature = "telemetry")]
use std::time::{SystemTime, UNIX_EPOCH};

const TRACEPARENT_HEADER: &str = "traceparent";
const TRACE_ID_HEADER: &str = "trace-id";

/// 只接受唯一且语法有效的 `traceparent`，重复 header 按不可信输入处理。
fn inbound_trace_context(request: &Request) -> Option<TraceContext> {
    let mut values = request.headers().get_all(TRACEPARENT_HEADER).iter();
    match (values.next(), values.next()) {
        (Some(value), None) => value
            .to_str()
            .ok()
            .and_then(TraceContext::parse_traceparent),
        _ => None,
    }
}

#[cfg(feature = "telemetry")]
/// 返回当前 UNIX 纳秒并饱和到 u64，供服务端 span 记录开始/结束时刻。
fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Trace 传播中间件。入站有效 `traceparent` → 派生子 span;否则开启新链路。当前上下文入请求扩展。
pub async fn trace_context(mut request: Request, next: Next) -> Response {
    let inbound = inbound_trace_context(&request);
    let current = match inbound {
        Some(parent) => parent.child(random_span_id()),
        None => TraceContext::new_root(true),
    };
    request.extensions_mut().insert(current);

    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&current.trace_id_hex()) {
        response.headers_mut().insert(TRACE_ID_HEADER, value);
    }
    response
}

/// Trace 传播中间件(遥测激活变体):在 [`trace_context`] 的传播之外,对每个请求向遥测组件的有界导出器
/// 产一个**服务端 span**。
///
/// 只在声明并激活 `telemetry` 组件时由 Web 装配启用(exporter 由 `State` 注入)。span 名取
/// `方法 + 路由模板`([`axum::extract::MatchedPath`],低基数;拿不到模板时退回 `方法 + ?`,绝不用原始
/// 路径以免高基数)。入队非阻塞,满即丢弃并计数,绝不阻塞或拖慢业务。
///
/// # 参数
///
/// - `exporter`:遥测组件发布的有界 span 导出器。
/// - `request`:入站请求。
/// - `next`:下游放行句柄。
#[cfg(feature = "telemetry")]
pub async fn trace_context_export(
    axum::extract::State(exporter): axum::extract::State<
        std::sync::Arc<natelemetry::BoundedSpanExporter>,
    >,
    mut request: Request,
    next: Next,
) -> Response {
    let inbound = inbound_trace_context(&request);
    let parent_span_id_hex = inbound.map(|parent| parent.parent_id_hex());
    let current = match inbound {
        Some(parent) => parent.child(random_span_id()),
        None => TraceContext::new_root(exporter.should_sample_root()),
    };
    request.extensions_mut().insert(current);

    // span 名:方法 + 路由模板(低基数);MatchedPath 拿不到时退回方法 + "?"。
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|matched| matched.as_str().to_owned())
        .unwrap_or_else(|| "?".to_owned());
    let started = unix_nanos();
    let mut response = next.run(request).await;
    // 入站未采样时只传播，不违反上游采样决定。
    if current.is_sampled() {
        let _ = exporter.export(natelemetry::SpanRecord {
            name: format!("{method} {route}"),
            trace_id_hex: current.trace_id_hex(),
            span_id_hex: current.parent_id_hex(),
            parent_span_id_hex,
            kind: natelemetry::SpanKind::Server,
            start_unix_nano: started,
            end_unix_nano: unix_nanos().max(started),
            http_status_code: Some(response.status().as_u16()),
        });
    }
    if let Ok(value) = HeaderValue::from_str(&current.trace_id_hex()) {
        response.headers_mut().insert(TRACE_ID_HEADER, value);
    }
    response
}
