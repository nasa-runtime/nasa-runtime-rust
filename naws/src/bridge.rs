// ============================================================================
// src/bridge.rs —— payload 桥接(NASA opaque bytes ↔ socket.io JSON arg)。
// 决策:JSON 透传 + 可插拔。
// 接口用 JSON【文本片段】(String)而非 serde Value,故 core 无需 serde 依赖。
// ============================================================================

use std::sync::Arc;

/// payload 桥接:socket.io 目标出站、socket.io 客户端入站时翻译 payload。
pub trait PayloadBridge: Send + Sync {
    /// 业务作用：出站:NASA payload(opaque bytes)→ socket.io EVENT 的 arg(一段 JSON 文本)。
    ///
    /// 参数说明：
    /// - `event`: 业务事件名,自定义 bridge 可按事件选择编码策略。
    /// - `payload`: 框架内部消息 payload 字节。
    ///
    /// 返回：可直接拼入 Socket.IO 事件数组的完整 JSON 值文本；实现方须保证语法有效。
    /// 此接口没有错误返回通道，转换失败时的替代值由实现约定。
    fn outbound(&self, event: &str, payload: &[u8]) -> String;

    /// 业务作用：入站:socket.io EVENT 的 arg(JSON 文本)→ NASA payload bytes。
    ///
    /// 参数说明：
    /// - `event`: 业务事件名,自定义 bridge 可按事件选择解码策略。
    /// - `arg_json`: socket.io EVENT 的单个 JSON 参数文本片段。
    ///
    /// 返回：交给业务处理器的负载字节；不负责身份校验、订阅权限或目标选择。
    /// 此接口没有错误返回通道，空负载是否表示转换失败由具体实现约定。
    fn inbound(&self, event: &str, arg_json: &str) -> Vec<u8>;
}

/// 默认:payload 当 UTF-8 JSON 文本透传。
/// - 出站:payload 是合法 UTF-8 → 原样当 arg(假定业务 payload 本就是 JSON 文本);空/非 UTF-8 → `null`。
/// - 入站:arg JSON 文本 → UTF-8 字节。
///
/// 走二进制 payload(BITPACK/protobuf)的系统应注入自定义 bridge(如 `Base64Bridge`)。
pub struct JsonPassthrough;

impl PayloadBridge for JsonPassthrough {
    /// 业务作用：将非空 UTF-8 负载原样作为 Socket.IO 事件参数，避免额外解释业务数据。
    ///
    /// 参数说明：
    /// - `_event`: 当前事件名称；此实现对所有事件使用相同透传规则。
    /// - `payload`: 内部 NASA 消息中的业务负载字节。
    ///
    /// 返回：非空 UTF-8 文本的副本；空负载或非法 UTF-8 返回 JSON 文本 `null`。
    /// 不校验 JSON 语法，调用方须保证非空文本本身是完整有效的 JSON 值。
    fn outbound(&self, _event: &str, payload: &[u8]) -> String {
        match std::str::from_utf8(payload) {
            Ok(s) if !s.is_empty() => s.to_string(),
            _ => "null".to_string(),
        }
    }

    /// 业务作用：保留 Socket.IO 参数的 JSON 文本表示，交给业务处理器解释负载。
    ///
    /// 参数说明：
    /// - `_event`: 当前事件名称；此实现不据此选择转换规则。
    /// - `arg_json`: 协议层传入的单个事件参数 JSON 文本。
    ///
    /// 返回：文本的 UTF-8 字节副本，保留引号和转义；不解析 JSON，也不把 `null` 转为空负载。
    fn inbound(&self, _event: &str, arg_json: &str) -> Vec<u8> {
        arg_json.as_bytes().to_vec()
    }
}

/// 业务作用：返回默认负载桥接器；用于未指定协议适配器时直接透传 JSON。
///
/// 参数说明: 无。
///
/// 返回：使用 `JsonPassthrough` 的共享接口实例；没有事件级配置，也不提供 JSON 语法校验。
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
    /// 业务作用：把任意二进制负载编码为 Socket.IO 可携带的 Base64 JSON 字符串。
    ///
    /// 参数说明：
    /// - `_event`: 当前事件名称；此实现统一使用标准 Base64 编码。
    /// - `payload`: 内部 NASA 消息中的二进制业务负载。
    ///
    /// 返回：包含双引号的完整 JSON 字符串字面量，正文采用带填充的标准 Base64；空字节编码为空字符串。
    fn outbound(&self, _event: &str, payload: &[u8]) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(payload);
        format!("\"{b64}\"")
    }

    /// 业务作用：从 Socket.IO 的 JSON 字符串参数恢复 Base64 编码的二进制业务负载。
    ///
    /// 参数说明：
    /// - `_event`: 当前事件名称；此实现统一使用标准 Base64 解码。
    /// - `arg_json`: 包含 Base64 正文的完整 JSON 字符串字面量。
    ///
    /// 返回：JSON 字符串和 Base64 均合法时返回解码字节；非字符串 JSON、语法或编码错误返回空字节。
    /// 合法空负载也返回空字节，业务层不能仅据返回长度区分空内容与转换失败。
    fn inbound(&self, _event: &str, arg_json: &str) -> Vec<u8> {
        use base64::Engine;
        // arg 是 JSON 字符串字面量 → 取出内容再 base64 解码;非法输入回退空。
        let s: String = serde_json::from_str(arg_json).unwrap_or_default();
        base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .unwrap_or_default()
    }
}

/// 业务作用：把一个事件名转成 JSON 字符串字面量(最小转义),用于拼 socket.io EVENT 数组。
///
/// 参数说明：
/// - `s`: 待写入 socket.io EVENT 数组的事件名。
///
/// 返回：带双引号的完整 JSON 字符串字面量，转义引号、反斜线及控制字符并保留其它 Unicode 字符。
/// 不校验事件名称是否被允许，也不构造完整事件数组。
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
