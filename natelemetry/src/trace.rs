//! W3C Trace Context 传播(`traceparent`),对齐 <https://www.w3.org/TR/trace-context/>。
//!
//! `traceparent = version "-" trace-id "-" parent-id "-" trace-flags`,version=00:
//! `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`。

use std::fmt;

use rand::RngCore as _;

/// trace-flags 的 sampled 位。
const SAMPLED_FLAG: u8 = 0x01;

/// 业务作用：生成一个非零随机 span-id(8B);W3C 要求 span-id 随机且非全零。
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
fn random_trace_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    loop {
        rand::thread_rng().fill_bytes(&mut id);
        if id != [0u8; 16] {
            return id;
        }
    }
}

/// W3C trace context:trace-id(16B)+ parent span-id(8B)+ trace-flags(1B)。
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: [u8; 16],
    parent_id: [u8; 8],
    flags: u8,
}

impl TraceContext {
    /// 业务作用：用原始字节构造；全零 trace-id/parent-id 返回 `None`。
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

    /// 业务作用：解析入站 `traceparent`(version-00);段数/长度错、非 hex、trace-id/span-id 全零或 version=ff 返回 `None`。
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
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            hex::encode(self.trace_id),
            hex::encode(self.parent_id),
            self.flags
        )
    }

    /// 业务作用：是否被采样(sampled 位)。
    pub fn is_sampled(&self) -> bool {
        self.flags & SAMPLED_FLAG != 0
    }

    /// 业务作用：trace-id 十六进制。
    pub fn trace_id_hex(&self) -> String {
        hex::encode(self.trace_id)
    }

    /// 业务作用：parent span-id 十六进制。
    pub fn parent_id_hex(&self) -> String {
        hex::encode(self.parent_id)
    }

    /// 业务作用：trace-flags 原值。
    pub fn flags(&self) -> u8 {
        self.flags
    }

    /// 业务作用：生成全新根上下文:随机非零 trace-id + parent span-id;`sampled` 决定采样位。
    ///
    /// 入站无 `traceparent`(或非法)时用它开启一条新链路。
    pub fn new_root(sampled: bool) -> Self {
        Self {
            trace_id: random_trace_id(),
            parent_id: random_span_id(),
            flags: if sampled { SAMPLED_FLAG } else { 0 },
        }
    }

    /// 业务作用：派生子上下文:**同一 trace-id**,parent-id 换成新 span-id,flags 沿用(采样决策向下传播)。
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceContext")
            .field("trace_id", &self.trace_id_hex())
            .field("parent_id", &self.parent_id_hex())
            .field("sampled", &self.is_sampled())
            .finish()
    }
}
