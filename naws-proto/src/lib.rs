//! 长连接消息中心的协议模型与 wire codec。
//!
//! 定义业务消息、鉴权帧、关闭原因、集群事件和多种二进制编码模式。
// ============================================================================
// proto —— ProtocolBytes 的 Rust 移植(逐字节兼容 原实现 原工具包)。
//
// 手写 VARINT_TLV + BITPACK_TLV codec(由 proto-derive 派生)+ serde 实现的 JSON_BYTES,
// 覆盖 Message / AuthRequest / AuthResponse / CloseReason / ClusterEvent,
// FAST_FIXED 按路线图后置(架构说明)。wire 规范见 架构说明 附录 A。
// ============================================================================

mod io;
mod json;
mod message_ref;
mod schema;

pub use io::{CodecError, Result};
pub use message_ref::MessageRef;
pub use schema::{AuthRequest, AuthResponse, CloseReason, ClusterEvent, Message};

/// 派生宏 `#[derive(ProtocolBytes)]` 生成代码用的运行期支持(内部,doc-hidden)。
#[doc(hidden)]
pub mod __rt {
    pub use crate::io::{check_wt, Reader, Writer, WT_LEN, WT_VARINT};
    pub use crate::json::{from_slice as json_from_slice, to_vec as json_to_vec};
}

/// 序列化模式。**ordinal 必须与 原实现 `ProtocolBytes.Mode` 一致**:
/// 它既是 Frame 头部的 mode byte,也是 ClusterEvent.messageMode 的取值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mode {
    /// JSON_BYTES 模式,字段按 JSON 名称编码,主要用于调试和兼容文本通道。
    JsonBytes = 0,
    /// VARINT_TLV 模式,字段按 tag-key + wiretype + value 顺序编码。
    VarintTlv = 1,
    /// BITPACK_TLV 模式,先写字段存在性位图,再按 schema 顺序写值。
    BitpackTlv = 2,
    /// 固定布局模式的预留枚举值;当前运行时会返回 Unsupported。
    FastFixed = 3,
}

impl Mode {
    /// 从 ordinal 构造序列化模式。
    ///
    /// # 参数
    /// - `v`: Frame 头或 ClusterEvent 中携带的模式序号。
    pub fn from_ordinal(v: u8) -> Option<Mode> {
        match v {
            0 => Some(Mode::JsonBytes),
            1 => Some(Mode::VarintTlv),
            2 => Some(Mode::BitpackTlv),
            3 => Some(Mode::FastFixed),
            _ => None,
        }
    }

    /// 返回协议类型序号；用于在线路格式中写入枚举标识。
    pub fn ordinal(self) -> u8 {
        self as u8
    }
}

/// 统一 codec 入口。每个 schema 类型实现它,按 mode 分派到对应编解码。
pub trait WireCodec: Sized {
    /// 按指定模式编码当前 schema 对象。
    ///
    /// # 参数
    /// - `mode`: 要使用的线协议模式,决定走 JSON_BYTES、VARINT_TLV、BITPACK_TLV 或 FAST_FIXED。
    fn encode(&self, mode: Mode) -> Result<Vec<u8>>;

    /// 按指定模式把字节还原为 schema 对象。
    ///
    /// # 参数
    /// - `mode`: 输入字节所使用的线协议模式。
    /// - `data`: 待解码的完整 schema payload 字节。
    fn decode(mode: Mode, data: &[u8]) -> Result<Self>;
}
