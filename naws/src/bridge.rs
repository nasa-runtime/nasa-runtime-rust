// ============================================================================
// src/bridge.rs —— payload 桥接(NASA opaque bytes ↔ socket.io JSON arg)。
// 决策:JSON 透传 + 可插拔。
// 接口用 JSON【文本片段】(String)而非 serde Value,故 core 无需 serde 依赖。
// ============================================================================

use std::sync::Arc;

/// payload 桥接:socket.io 目标出站、socket.io 客户端入站时翻译 payload。
pub trait PayloadBridge: Send + Sync {
    /// 出站:NASA payload(opaque bytes)→ socket.io EVENT 的 arg(一段 JSON 文本)。
    ///
    /// # 参数
    /// - `event`: 业务事件名,自定义 bridge 可按事件选择编码策略。
    /// - `payload`: 框架内部消息 payload 字节。
    fn outbound(&self, event: &str, payload: &[u8]) -> String;

    /// 入站:socket.io EVENT 的 arg(JSON 文本)→ NASA payload bytes。
    ///
    /// # 参数
    /// - `event`: 业务事件名,自定义 bridge 可按事件选择解码策略。
    /// - `arg_json`: socket.io EVENT 的单个 JSON 参数文本片段。
    fn inbound(&self, event: &str, arg_json: &str) -> Vec<u8>;
}

/// 默认:payload 当 UTF-8 JSON 文本透传。
/// - 出站:payload 是合法 UTF-8 → 原样当 arg(假定业务 payload 本就是 JSON 文本);空/非 UTF-8 → `null`。
/// - 入站:arg JSON 文本 → UTF-8 字节。
///
/// 走二进制 payload(BITPACK/protobuf)的系统应注入自定义 bridge(如 `Base64Bridge`)。
pub struct JsonPassthrough;

impl PayloadBridge for JsonPassthrough {
    /// 把内部负载转换为外发文本；用于适配不同客户端协议。
    ///
    /// # 参数
    /// - `event`: socket.io 事件名,用于让 bridge 选择事件级编码策略。
    /// - `payload`: 内部 NASA 消息中的业务负载字节。
    fn outbound(&self, _event: &str, payload: &[u8]) -> String {
        match std::str::from_utf8(payload) {
            Ok(s) if !s.is_empty() => s.to_string(),
            _ => "null".to_string(),
        }
    }

    /// 把入站文本转换为内部字节负载；用于统一事件处理入口。
    ///
    /// # 参数
    /// - `event`: socket.io 事件名,用于让 bridge 选择事件级解码策略。
    /// - `arg_json`: 网关转发给目标服务的业务参数 JSON 字符串。
    fn inbound(&self, _event: &str, arg_json: &str) -> Vec<u8> {
        arg_json.as_bytes().to_vec()
    }
}

/// 返回默认负载桥接器；用于未指定协议适配器时直接透传 JSON。
pub fn default_bridge() -> Arc<dyn PayloadBridge> {
    Arc::new(JsonPassthrough)
}

/// 二进制 payload 桥接示例:payload ↔ socket.io 的 base64 **JSON 字符串** arg。
/// 适合 payload 是二进制(BITPACK/protobuf/原始字节)的系统:
/// - 出站:payload 字节 → base64 → 作为 JSON 字符串 `"<b64>"`(base64 字母表无需转义);
/// - 入站:JSON 字符串 → 取出 base64 文本 → 解码回字节。
#[cfg(feature = "socketio")]
pub struct Base64Bridge;

#[cfg(feature = "socketio")]
impl PayloadBridge for Base64Bridge {
    /// 把内部负载转换为外发文本；用于适配不同客户端协议。
    ///
    /// # 参数
    /// - `event`: socket.io 事件名,用于让 bridge 选择事件级编码策略。
    /// - `payload`: 内部 NASA 消息中的二进制业务负载。
    fn outbound(&self, _event: &str, payload: &[u8]) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(payload);
        format!("\"{b64}\"")
    }

    /// 把入站文本转换为内部字节负载；用于统一事件处理入口。
    ///
    /// # 参数
    /// - `event`: socket.io 事件名,用于让 bridge 选择事件级解码策略。
    /// - `arg_json`: 网关转发给目标服务的业务参数 JSON 字符串。
    fn inbound(&self, _event: &str, arg_json: &str) -> Vec<u8> {
        use base64::Engine;
        // arg 是 JSON 字符串字面量 → 取出内容再 base64 解码;非法输入回退空。
        let s: String = serde_json::from_str(arg_json).unwrap_or_default();
        base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .unwrap_or_default()
    }
}

/// 把一个事件名转成 JSON 字符串字面量(最小转义),用于拼 socket.io EVENT 数组。
///
/// # 参数
/// - `s`: 待写入 socket.io EVENT 数组的事件名。
pub fn json_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
