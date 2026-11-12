//! W3C Trace Context 传播(`traceparent`),对齐 <https://www.w3.org/TR/trace-context/>。
//!
//! `traceparent = version "-" trace-id "-" parent-id "-" trace-flags`,version=00:
//! `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`。

use std::fmt;

use rand::RngCore as _;

/// trace-flags 的 sampled 位。
const SAMPLED_FLAG: u8 = 0x01;

/// 业务作用：为新 span 生成非零随机身份。
///
/// 参数说明: 无。
///
/// 返回：返回由线程随机源填充的八字节非零数组；遇全零重新生成，不保证跨调用绝对唯一。
pub fn random_span_id() -> [u8; 8] {
    let mut id = [0u8; 8];
    loop {
        rand::thread_rng().fill_bytes(&mut id);
        if id != [0u8; 8] {
            return id;
        }
    }
}

/// 业务作用：生成一个非零随机 trace-id(16B)。
///
/// 参数说明: 无。
///
/// 返回：返回十六字节非零随机数组；遇全零重新生成，不保证绝对唯一。
fn random_trace_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    loop {
        rand::thread_rng().fill_bytes(&mut id);
        if id != [0u8; 16] {
            return id;
        }
    }
}

/// W3C 追踪上下文，包含 16 字节 trace-id、8 字节 parent span-id 与 1 字节 trace-flags。
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: [u8; 16],
    parent_id: [u8; 8],
    flags: u8,
}

impl TraceContext {
    /// 业务作用：用原始字节构造；全零 trace-id/parent-id 返回 `None`。
    ///
    /// 参数说明：
    /// - `trace_id`：所属追踪链的十六字节身份。
    /// - `parent_id`：传播中当前父跨度的八字节身份。
    /// - `flags`：保留原值的追踪标志字节，最低位表示采样。
    ///
    /// 返回：trace_id 或 parent_id 全零时返回 None，否则保存所有字节及完整 flags；不验证身份来源或其它标志位。
    pub fn new(trace_id: [u8; 16], parent_id: [u8; 8], flags: u8) -> Option<Self> {
        if trace_id == [0u8; 16] || parent_id == [0u8; 8] {
            return None;
        }
        Some(Self {
            trace_id,
            parent_id,
            flags,
        })
    }

    /// 业务作用：解析本层支持的 W3C traceparent 版本布局并拒绝无效传播身份。
    ///
    /// 参数说明：
    /// - `header`：入站 traceparent 原始头部文本。
    ///
    /// 返回：仅精确四段且版本为 00、小写十六进制长度分别为 32、16、2 且两种 ID 非零时返回上下文，其它返回 None；不去空白，不接纳未来版本布局。
    pub fn parse_traceparent(header: &str) -> Option<Self> {
        let mut parts = header.split('-');
        let version = parts.next()?;
        let trace_id_hex = parts.next()?;
        let parent_id_hex = parts.next()?;
        let flags_hex = parts.next()?;
        if parts.next().is_some() {
            return None; // 必须恰好 4 段(保守:只认 version-00 布局)
        }
        // 本实现只支持 version 00；未来版本不能按 v00 固定布局误解析。
        if version != "00" {
            return None;
        }
        let lower_hex = |value: &str| {
            value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        if trace_id_hex.len() != 32 || parent_id_hex.len() != 16 || flags_hex.len() != 2 {
            return None;
        }
        if !lower_hex(trace_id_hex) || !lower_hex(parent_id_hex) || !lower_hex(flags_hex) {
            return None;
        }

        let mut trace_id = [0u8; 16];
        hex::decode_to_slice(trace_id_hex, &mut trace_id).ok()?;
        let mut parent_id = [0u8; 8];
        hex::decode_to_slice(parent_id_hex, &mut parent_id).ok()?;
        let mut flags = [0u8; 1];
        hex::decode_to_slice(flags_hex, &mut flags).ok()?;

        // 全零 trace-id/span-id 非法。
        if trace_id == [0u8; 16] || parent_id == [0u8; 8] {
            return None;
        }
        Some(Self {
            trace_id,
            parent_id,
            flags: flags[0],
        })
    }

    /// 业务作用：生成出站 `traceparent`(version-00)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回版本 00、固定宽度小写十六进制的四段传播文本；不重新验证通过 child 写入的身份。
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            hex::encode(self.trace_id),
            hex::encode(self.parent_id),
            self.flags
        )
    }

    /// 业务作用：是否被采样(sampled 位)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：flags 的最低位为 1 时返回 true，不改变其它标志。
    pub fn is_sampled(&self) -> bool {
        self.flags & SAMPLED_FLAG != 0
    }

    /// 业务作用：trace-id 十六进制。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回当前十六字节追踪 ID 的三十二字符小写十六进制副本。
    pub fn trace_id_hex(&self) -> String {
        hex::encode(self.trace_id)
    }

    /// 业务作用：parent span-id 十六进制。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回当前八字节传播跨度 ID 的十六字符小写十六进制副本。
    pub fn parent_id_hex(&self) -> String {
        hex::encode(self.parent_id)
    }

    /// 业务作用：trace-flags 原值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原样返回追踪标志字节，不只返回 sampled 位。
    pub fn flags(&self) -> u8 {
        self.flags
    }

    /// 业务作用：生成全新根上下文:随机非零 trace-id + parent span-id;`sampled` 决定采样位。 入站无 `traceparent`(或非法)时用它开启一条新链路。
    ///
    /// 参数说明：
    /// - `sampled`：新根链路是否携带采样标志。
    ///
    /// 返回：生成非零随机追踪与跨度 ID，并按 sampled 设置最低位，其它标志置零；不建立或导出 span 记录。
    pub fn new_root(sampled: bool) -> Self {
        Self {
            trace_id: random_trace_id(),
            parent_id: random_span_id(),
            flags: if sampled {
                SAMPLED_FLAG
            } else {
                0
            },
        }
    }

    /// 业务作用：派生子上下文:**同一 trace-id**,parent-id 换成新 span-id,flags 沿用(采样决策向下传播)。
    ///
    /// 参数说明：
    /// - `new_span_id`：子 span 使用的八字节身份，应由非零 ID 生成器提供。
    ///
    /// 返回：返回保留 trace_id 与全部 flags、用新 ID 替换传播跨度的上下文；不检查 new_span_id 非零，调用方应提供有效身份。
    pub fn child(&self, new_span_id: [u8; 8]) -> Self {
        Self {
            trace_id: self.trace_id,
            parent_id: new_span_id,
            flags: self.flags,
        }
    }
}

impl fmt::Debug for TraceContext {
    /// 业务作用：输出可关联的十六进制 ID 与采样位，不包含 baggage 或业务属性。
    ///
    /// 参数说明：
    /// - `formatter`：接收传播上下文摘要的格式化器。
    ///
    /// 返回：输出两个 ID 的十六进制与采样状态；格式化写出失败返回错误，不展示其它标志位。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceContext")
            .field("trace_id", &self.trace_id_hex())
            .field("parent_id", &self.parent_id_hex())
            .field("sampled", &self.is_sampled())
            .finish()
    }
}
