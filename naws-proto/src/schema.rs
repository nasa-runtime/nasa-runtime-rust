// ============================================================================
// proto/src/schema.rs —— 信封 / 控制帧 / 集群事件的结构。
// codec 由 #[derive(ProtocolBytes)] 生成(从手写 codec 提炼,golden 对拍验证逐字节一致)。
//
// 字段 tag / wiretype / mode 见 架构说明 附录 A.7;ClusterEvent 新 schema。
// 建模要点:引用字段(String/数组/byte[])用 Option(nullable);
//   primitive(ack_id/timestamp/message_mode/code/ok/heartbeat)恒编码(BITPACK 位恒 1)。
// ============================================================================

use naws_proto_derive::ProtocolBytes;
use serde::{Deserialize, Serialize};

// 各字段的 serde 属性见 json.rs:
//   Option<String>          → #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
//   Option<Vec<Opt<String>>> → ... opt_strvec_empty
//   Option<Vec<u8>>(byte[]) → ... opt_bytes_empty + 有符号字节 serialize_with/deserialize_with
//   primitive               → #[serde(default)]

// ════════════════════════════════════════════════════════════════════════════
// Message —— 消息信封。默认 BITPACK_TLV;也支持 VARINT_TLV。
// ════════════════════════════════════════════════════════════════════════════
/// 业务消息信封,承载事件名、路由目标、发送方信息、payload 和 ack id。
#[derive(Debug, Clone, Default, PartialEq, Eq, ProtocolBytes, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    #[proto(tag = 10)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 本消息触发的业务事件名列表;服务端会按 endpoint 事件表分派。
    pub events: Option<Vec<Option<String>>>,
    #[proto(tag = 20)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 发送方用户 ID,通常由服务端认证后补写,用于排除自己或审计。
    pub from_uid: Option<String>,
    #[proto(tag = 30)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 发送方 session/client id,用于从 fan-out 目标中排除当前连接。
    pub from_client: Option<String>,
    #[proto(tag = 40)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 目标 endpoint/router 列表,用于端点级广播。
    pub routers: Option<Vec<Option<String>>>,
    #[proto(tag = 50)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 目标群组名;存在时优先按群组成员投递。
    pub group: Option<String>,
    #[proto(tag = 60)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 目标用户 ID 列表,用于用户级单播或多播。
    pub uids: Option<Vec<Option<String>>>,
    #[proto(tag = 70)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 需要排除的用户 ID 列表,用于广播时跳过指定用户的全部连接。
    pub excludes: Option<Vec<Option<String>>>,
    #[proto(tag = 80)]
    #[serde(
        default,
        skip_serializing_if = "crate::json::opt_bytes_empty",
        serialize_with = "crate::json::ser_opt_bytes_signed",
        deserialize_with = "crate::json::de_opt_bytes_signed"
    )]
    /// 业务 payload 原始字节,由上层约定 JSON、二进制或其它业务格式。
    pub payload: Option<Vec<u8>>,
    #[proto(tag = 90)]
    #[serde(default)]
    /// ACK 关联 ID;0 表示不要求 ACK。
    pub ack_id: i64, // primitive,恒编码
}

// ════════════════════════════════════════════════════════════════════════════
// AuthRequest —— 鉴权请求(控制帧,服务端按 VARINT_TLV 收发)。
// ════════════════════════════════════════════════════════════════════════════
// 注:`Debug` 手写脱敏(见下方 impl),不进 derive——`token` 是客户端鉴权凭据,derive(Debug) 会让
// ws server 在错误/调试日志里 `{:?}` 出明文 token(公共库默认防御)。其余 derive(含 wire 编解码
// ProtocolBytes、Serialize)不变,token 仍正常参与序列化/上线。
/// 客户端鉴权请求,承载 token、endpoint、设备版本以及握手 query/header 参数。
#[derive(Clone, Default, PartialEq, Eq, ProtocolBytes, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthRequest {
    #[proto(tag = 10)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 客户端鉴权 token;Debug 输出会脱敏。
    pub token: Option<String>,
    #[proto(tag = 20)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 客户端请求绑定的 endpoint 路径。
    pub endpoint: Option<String>,
    #[proto(tag = 30)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 业务设备 ID,用于同一用户多端识别。
    pub device_id: Option<String>,
    #[proto(tag = 40)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 客户端版本号,用于兼容性判断和灰度策略。
    pub version: Option<String>,
    #[proto(tag = 50)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 握手 query 参数名列表,与 `query_values` 按下标对应。
    pub query_keys: Option<Vec<Option<String>>>,
    #[proto(tag = 60)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 握手 query 参数值列表,与 `query_keys` 按下标对应。
    pub query_values: Option<Vec<Option<String>>>,
    #[proto(tag = 70)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 握手 header 名列表,与 `header_values` 按下标对应。
    pub header_keys: Option<Vec<Option<String>>>,
    #[proto(tag = 80)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 握手 header 值列表,与 `header_keys` 按下标对应。
    pub header_values: Option<Vec<Option<String>>>,
}

impl std::fmt::Debug for AuthRequest {
    /// 格式化鉴权请求;`token` 脱敏(只标记是否存在),其余字段正常。
    ///
    /// # 参数
    /// - `f`: 标准格式化器,由日志或调试输出提供。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthRequest")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("endpoint", &self.endpoint)
            .field("device_id", &self.device_id)
            .field("version", &self.version)
            .field("query_keys", &self.query_keys)
            .field("query_values", &self.query_values)
            .field("header_keys", &self.header_keys)
            .field("header_values", &self.header_values)
            .finish()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// AuthResponse —— 鉴权响应。ok/heartbeat 恒编码。
// ════════════════════════════════════════════════════════════════════════════
/// 服务端鉴权响应,告知是否通过、session id、错误原因和心跳超时。
#[derive(Debug, Clone, Default, PartialEq, Eq, ProtocolBytes, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthResponse {
    #[proto(tag = 10)]
    #[serde(default)]
    /// 鉴权是否通过。
    pub ok: bool, // 裸 varint(不 zigzag),恒编码
    #[proto(tag = 20)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 服务端分配的 session id;仅鉴权成功时有值。
    pub session_id: Option<String>,
    #[proto(tag = 30)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 鉴权失败或协议拒绝时的可读原因。
    pub err_msg: Option<String>,
    #[proto(tag = 40)]
    #[serde(default)]
    /// 服务端心跳超时毫秒数,客户端可据此安排 PING。
    pub heartbeat_timeout_ms: i64, // 恒编码
}

// ════════════════════════════════════════════════════════════════════════════
// CloseReason —— 关闭原因。code 恒编码(int,zigzag)。
// ════════════════════════════════════════════════════════════════════════════
/// 连接关闭原因,用稳定 code 和可读 reason 表示协议、鉴权、踢下线或服务端停机原因。
#[derive(Debug, Clone, Default, PartialEq, Eq, ProtocolBytes, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseReason {
    #[proto(tag = 10)]
    #[serde(default)]
    /// 稳定关闭 code,用于客户端区分正常关闭、超时、鉴权失败等场景。
    pub code: i32, // 恒编码
    #[proto(tag = 20)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 面向日志和排障的关闭原因文本。
    pub reason: Option<String>,
}

impl CloseReason {
    /// 正常关闭。
    pub const CODE_NORMAL: i32 = 1000;
    /// 心跳超时或握手超时。
    pub const CODE_TIMEOUT: i32 = 1001;
    /// 鉴权失败。
    pub const CODE_AUTH_FAILED: i32 = 1002;
    /// endpoint 被替换或注销。
    pub const CODE_ENDPOINT_REMOVED: i32 = 1003;
    /// 被业务或管理端踢下线。
    pub const CODE_KICKED: i32 = 1004;
    /// 服务端主动停机。
    pub const CODE_SERVER_SHUTDOWN: i32 = 1005;
    /// 协议帧非法或状态机不允许。
    pub const CODE_PROTOCOL_ERROR: i32 = 1006;
}

// ════════════════════════════════════════════════════════════════════════════
// ClusterEvent —— 集群事件(BITPACK_TLV)。修复后 schema。
// timestamp / message_mode 恒编码。
// ════════════════════════════════════════════════════════════════════════════
/// 集群跨节点事件,封装来源节点、fencing token、目标节点和被转发的业务消息字节。
#[derive(Debug, Clone, Default, PartialEq, Eq, ProtocolBytes, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterEvent {
    #[proto(tag = 10)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 发布事件的节点 ID,接收端用它防回环和维护 presence。
    pub source_node: Option<String>,
    #[proto(tag = 20)]
    #[serde(default)]
    /// 事件创建时间戳毫秒,用于日志、排障和迟到消息观测。
    pub timestamp: i64, // 恒编码
    #[proto(tag = 30)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    /// 链路追踪 ID,由业务或网关透传。
    pub trace_id: Option<String>,
    #[proto(tag = 40)]
    #[serde(default)]
    /// `message_bytes` 内部业务消息采用的 [`crate::Mode`] ordinal。
    pub message_mode: i8, // byte,zigzag,恒编码 = Mode.ordinal()
    #[proto(tag = 50)]
    #[serde(
        default,
        skip_serializing_if = "crate::json::opt_bytes_empty",
        serialize_with = "crate::json::ser_opt_bytes_signed",
        deserialize_with = "crate::json::de_opt_bytes_signed"
    )]
    /// 已编码的 [`Message`] 字节,按 `message_mode` 解码后本地投递。
    pub message_bytes: Option<Vec<u8>>,
    #[proto(tag = 60)]
    #[serde(default, skip_serializing_if = "crate::json::opt_strvec_empty")]
    /// 指定目标节点列表;为空表示由接收端按本地路由或 presence 决定。
    pub target_nodes: Option<Vec<Option<String>>>,
    /// 发送方 boot id(incarnation fencing token)。presence **与** data 事件都带,接收端统一围栏:
    /// 旧实例(更小 incarnation)的迟到消息一律拒绝,不续命、不投递。
    /// 专用字段,不再复用 trace_id(留给真正的链路追踪)。
    #[proto(tag = 70)]
    #[serde(default, skip_serializing_if = "crate::json::opt_str_empty")]
    pub source_incarnation: Option<String>,
}
