//! Kafka 链路输入验证及有界批链接；观测身份不参与业务路由或确认裁决。

use crate::{ConsumeCtx, KafkaHeaders};
use natelemetry::TraceContext;

/// 有限非法原因，不携带原始头值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceInvalid {
    /// 同一精确线路字段出现多次。
    Duplicate,
    /// 空值、非UTF-8、非规范身份或不支持的traceparent版本。
    Malformed,
}
impl TraceInvalid {
    /// 业务作用：提供固定低基数原因标签。参数说明：无。返回：不包含用户输入的稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::Malformed => "malformed",
        }
    }
}
/// 业务作用：读取唯一规范的version 00上下文，拒绝歧义观测而不拒绝业务消息。
/// 参数说明：headers 为有序Kafka头，名称精确匹配traceparent。
/// 返回：缺失为Ok(None)，有效为上下文，重复或非法值为有限错误。
pub fn extract_trace_context(
    headers: &KafkaHeaders,
) -> std::result::Result<Option<TraceContext>, TraceInvalid> {
    extract_fields(
        headers
            .iter()
            .map(|h| (h.name.as_str(), h.value.as_deref())),
    )
}
/// 业务作用：共用拥有型和借用型头的trace校验，避免透传路线为了观测复制全部原文。
/// 参数说明：headers 为原始名称与可空字节的借用序列。返回：缺失、有效上下文或有限非法原因。
pub(crate) fn extract_fields<'a>(
    headers: impl Iterator<Item = (&'a str, Option<&'a [u8]>)>,
) -> std::result::Result<Option<TraceContext>, TraceInvalid> {
    let mut matches = headers.filter(|(name, _)| *name == crate::wire::HEADER_TRACEPARENT);
    let Some((_, value)) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(TraceInvalid::Duplicate);
    }
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(TraceContext::parse_traceparent)
        .map(Some)
        .ok_or(TraceInvalid::Malformed)
}

/// 业务作用：为实际消费建立子span或真实根，避免把未导出的随机身份作为伪父节点。
/// 参数说明：recorder 为可选应用出口，parent 为有效单条入站父上下文；批调用传None并单独暴露links。
/// 返回：可选有界导出凭证及调用域内传播上下文；未配置出口时仍支持不采样的链路传播。
pub(crate) fn consume_span(
    recorder: Option<&natelemetry::SpanRecorder>,
    parent: Option<TraceContext>,
) -> (Option<natelemetry::SpanGuard>, TraceContext) {
    let guard = recorder.map(|recorder| match parent {
        Some(parent) => recorder.start("Kafka consume", &parent, natelemetry::SpanKind::Consumer),
        None => recorder.start_root("Kafka consume", natelemetry::SpanKind::Consumer, []),
    });
    let context = guard
        .as_ref()
        .map(natelemetry::SpanGuard::context)
        .unwrap_or_else(|| {
            parent
                .map(|p| p.child(natelemetry::random_span_id()))
                .unwrap_or_else(|| TraceContext::new_root(false))
        });
    (guard, context)
}

/// 一次批调用的独立父链接；超过128个不同保留项的后续输入只累计遗漏数。
#[derive(Clone, Default)]
pub struct KafkaTraceLinks {
    /// 最多128个不同有效父上下文，保持输入顺序。
    pub contexts: Vec<TraceContext>,
    /// 因链接容量不足而未保留的有效输入数。
    pub omitted: usize,
}
tokio::task_local! {static CURRENT_LINKS:KafkaTraceLinks;}
impl KafkaTraceLinks {
    /// 业务作用：从明确的批输入建立有限链接，不拼接不同trace为伪造父节点。
    /// 参数说明：contexts 为原始记录上下文序列。
    /// 返回：保序去重的前128个有效链接及遗漏数，非法观测不进入链接。
    pub fn from_contexts<'a>(contexts: impl IntoIterator<Item = &'a ConsumeCtx>) -> Self {
        let mut links = Self::default();
        for context in contexts {
            if let Some(trace) = context.trace_context() {
                if !links.contexts.contains(&trace) {
                    if links.contexts.len() < 128 {
                        links.contexts.push(trace);
                    } else {
                        links.omitted = links.omitted.saturating_add(1);
                    }
                }
            }
        }
        links
    }
    /// 业务作用：在当前异步业务调用中读取批链接，供应用遥测适配器建立span links。
    /// 参数说明：无。返回：有限自有快照；调用域外为空，跨任务需显式传递。
    pub fn current() -> Self {
        CURRENT_LINKS.try_with(Clone::clone).unwrap_or_default()
    }
    /// 业务作用：将批链接限定在一次真实业务future的生命周期内，结束或取消后自动恢复外层作用域。
    /// 参数说明：future 为已取得执行权的业务future。返回：原future结局，不修改确认权威。
    pub(crate) async fn scope<F: std::future::Future>(self, future: F) -> F::Output {
        CURRENT_LINKS.scope(self, future).await
    }
}
