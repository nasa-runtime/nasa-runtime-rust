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

/// 业务作用：只接受唯一且语法有效的 `traceparent`，重复 header 按不可信输入处理。
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
/// 业务作用：返回当前 UNIX 纳秒并饱和到 u64，供服务端 span 记录开始/结束时刻。
fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// 业务作用：Trace 传播中间件。入站有效 `traceparent` 时继承采样位并派生子上下文；否则建立
/// 未采样新根。当前上下文同时进入请求扩展和环境作用域，供出站调用延续链路。
///
/// 参数说明：
/// - `request`：可能携带唯一 W3C `traceparent` 的入站请求。
/// - `next`：在当前 trace 作用域内执行的下游服务。
///
/// 返回：下游响应，并附加仅用于关联的 `trace-id` 响应头；本入口没有 sampler 权威，不导出 span。
pub async fn trace_context(mut request: Request, next: Next) -> Response {
    let inbound = inbound_trace_context(&request);
    let current = match inbound {
        Some(parent) => parent.child(random_span_id()),
        // 纯传播入口没有 exporter/sampler 权威，不能替下游擅自声明 sampled；只建立未采样根并继续
        // 传播。安装 exporter 的变体由其冻结 sampler 决定新根采样位。
        None => TraceContext::new_root(false),
    };
    request.extensions_mut().insert(current);

    // 请求扩展供显式穿线；环境作用域让未显式绑定的出站客户端(REST/Kafka)也能延续同一链路,
    // 显式绑定始终优先于环境值。
    let mut response = natelemetry::with_ambient(Some(current), next.run(request)).await;
    if let Ok(value) = HeaderValue::from_str(&current.trace_id_hex()) {
        response.headers_mut().insert(TRACE_ID_HEADER, value);
    }
    response
}

/// 业务作用：Trace 传播中间件(遥测激活变体):在 [`trace_context`] 的传播之外,对每个请求向遥测组件的有界导出器
/// 产一个**服务端 span**。
///
/// 只在声明并激活 `telemetry` 组件时由 Web 装配启用(exporter 由 `State` 注入)。span 名取
/// `方法 + 路由模板`([`axum::extract::MatchedPath`],低基数;拿不到模板时退回 `方法 + ?`,绝不用原始
/// 路径以免高基数)。入队非阻塞,满即丢弃并计数,绝不阻塞或拖慢业务。
///
/// 参数说明：
///
/// - `exporter`:遥测组件发布的有界 span 导出器。
/// - `request`:入站请求。
/// - `next`:下游放行句柄。
///
/// 返回：下游响应及关联 trace-id；合法上游采样位原样继承，新根由 exporter 冻结 sampler 裁决。
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
    // 与纯传播变体同语义:handler 全程处于环境作用域,出站客户端可回退取当前上下文。
    let mut response = natelemetry::with_ambient(Some(current), next.run(request)).await;
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
            attributes: Vec::new(),
        });
    }
    if let Ok(value) = HeaderValue::from_str(&current.trace_id_hex()) {
        response.headers_mut().insert(TRACE_ID_HEADER, value);
    }
    response
}
