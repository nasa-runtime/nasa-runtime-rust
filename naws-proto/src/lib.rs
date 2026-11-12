//! 长连接消息中心的协议模型与 wire codec。
//!
//! 定义业务消息、鉴权帧、关闭原因、集群事件和多种有界编码模式。VARINT_TLV 与 BITPACK_TLV
//! 显式控制字段 tag、长度和顺序，JSON_BYTES 通过 serde 属性控制字段名称和缺省值。
//! FAST_FIXED 按 tag 顺序编码小端定宽基础类型，不接受可空或变长字段。
//! `dynamic::SchemaPlan` 提供带容量约束的动态字段、集合和嵌套对象编码，以及同一 schema 的 JSON 导入导出。
//! 它不加载应用代码，也不参与路由授权或集群控制。

/// 受限动态 schema 编译与四种模式编码，不加载业务代码。
pub mod dynamic;
mod io;
mod json;
mod message_ref;
mod schema;
/// Socket.IO 无状态包头模型，供服务端与客户端共同使用。
pub mod socketio;

pub use io::{CodecError, Result};
pub use message_ref::MessageRef;
pub use naws_proto_derive::ProtocolBytes;
pub use schema::{AuthRequest, AuthResponse, CloseReason, ClusterEvent, Message};

/// 派生宏 `#[derive(ProtocolBytes)]` 生成代码用的运行期支持(内部,doc-hidden)。
#[doc(hidden)]
pub mod __rt {
    pub use crate::io::{check_wt, Reader, Writer, WT_LEN, WT_VARINT};
    pub use crate::json::{from_slice as json_from_slice, to_vec as json_to_vec};
}

/// 序列化模式；ordinal 是稳定线协议字段，同时用于 Frame mode byte 与 ClusterEvent.messageMode。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mode {
    /// JSON_BYTES 模式,字段按 JSON 名称编码,主要用于调试和兼容文本通道。
    JsonBytes = 0,
    /// VARINT_TLV 模式,字段按 tag-key + wiretype + value 顺序编码。
    VarintTlv = 1,
    /// BITPACK_TLV 模式,先写字段存在性位图,再按 schema 顺序写值。
    BitpackTlv = 2,
    /// 固定布局模式，只接受不可空的定宽基础类型 schema。
    FastFixed = 3,
}

impl Mode {
    /// 业务作用：从 ordinal 构造序列化模式。
    ///
    /// 参数说明：
    /// - `v`: Frame 头或 ClusterEvent 中携带的模式序号。
    ///
    /// 返回：0 至 3 对应固定协议模式；其它值返回 None，不猜测默认编码。
    pub fn from_ordinal(v: u8) -> Option<Mode> {
        match v {
            0 => Some(Mode::JsonBytes),
            1 => Some(Mode::VarintTlv),
            2 => Some(Mode::BitpackTlv),
            3 => Some(Mode::FastFixed),
            _ => None,
        }
    }

    /// 业务作用：返回协议类型序号；用于在线路格式中写入枚举标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：该模式稳定的线协议编号，不随声明顺序以外的运行状态变化。
    pub fn ordinal(self) -> u8 {
        self as u8
    }
}

/// 统一 codec 入口。每个 schema 类型实现它,按 mode 分派到对应编解码。
pub trait WireCodec: Sized {
    /// 业务作用：按指定模式编码当前 schema 对象。
    ///
    /// 参数说明：
    /// - `mode`: 要使用的线协议模式,决定走 JSON_BYTES、VARINT_TLV、BITPACK_TLV 或 FAST_FIXED。
    ///
    /// 返回：目标模式的完整载荷；schema 不支持该模式或字段无法编码时返回错误。
    fn encode(&self, mode: Mode) -> Result<Vec<u8>>;

    /// 业务作用：按指定模式把字节还原为 schema 对象。
    ///
    /// 参数说明：
    /// - `mode`: 输入字节所使用的线协议模式。
    /// - `data`: 待解码的完整 schema payload 字节。
    ///
    /// 返回：完整校验后的 schema 实例；不支持的模式或畸形输入返回错误。
    fn decode(mode: Mode, data: &[u8]) -> Result<Self>;
}
