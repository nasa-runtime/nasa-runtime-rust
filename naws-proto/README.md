# naws-proto

`naws-proto` 是 NASA 长连接协议层，提供 wire schema、`WireCodec` 编解码 trait，以及 VARINT_TLV、BITPACK_TLV、JSON_BYTES 等模式的逐字节兼容实现。`naws` 在网络层收发 frame，协议细节由本 crate 处理。

业务类型优先从 `nasa::ws` 门面取得；自定义协议结构体还需要派生宏包：

```toml
[dependencies]
nasa = { version = "1", features = ["ws"] }
naws-proto-derive = { version = "1" }
```

## 派生消息体

```rust
use nasa::ws::{Mode, WireCodec};
use naws_proto_derive::ProtocolBytes;

#[derive(ProtocolBytes)]
struct ChatMessage {
    room_id: i64,
    content: String,
}

fn encode(msg: &ChatMessage) -> anyhow::Result<Vec<u8>> {
    Ok(msg.encode(Mode::BitpackTlv)?)
}
```

## 手动解码

```rust
use nasa::ws::{Mode, WireCodec};

fn decode(bytes: &[u8]) -> anyhow::Result<ChatMessage> {
    Ok(ChatMessage::decode(Mode::BitpackTlv, bytes)?)
}
```

## JSON_BYTES 场景

JSON_BYTES 适合需要与 JSON 网关、socket.io 兼容层或调试工具交互的消息。结构体仍通过 serde 控制字段名和默认值。

```rust
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonPayload {
    user_id: i64,
    display_name: String,
}
```

## 边界

- 本 crate 只处理字节协议，不启动 TCP/WebSocket 服务。
- 修改 schema 前需要做 golden 对拍，避免破坏旧客户端兼容。
- 业务项目通常通过 `naws::proto` 或 `nasa::ws::proto` 使用。

## 行为边界(实测)

- JSON_BYTES / VARINT_TLV / BITPACK_TLV 三模式 round-trip 稳定(含 CJK、负数、bytes);FAST_FIXED 未实现,编码解码均返回 `Unsupported`。
- **`Some("")` 与 `None` 归一**:字符串数组元素的空串与 null 在全部模式下同编码,解码一律还原为 `None`;业务不要依赖 `Some("")` round-trip。
- 防御性解码:varint ≤10 字节、声明长度/count 不超剩余字节、单字段 ≤16MiB、单数组 ≤65536 元素、尾随字节报错;逐字节截断全扫描与随机噪声输入均不 panic(实测)。
- TLV 无总长度字段:截断恰落在字段边界时前缀仍可解码(后续字段按缺省),字段中间截断必报错——依赖外层帧长度保证完整性。

## YML 配置与使用

`naws-proto` 不读取 yml。协议模式和消息 schema 应由代码显式选择；运行期网络配置放在 `naws` 的 `ws:` 段。

推荐把默认协议模式写入应用配置，启动时映射为 `Mode`：

```yaml
ws:
  protocol:
    default_mode: BitpackTlv
    max_frame_bytes: 16777216
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `default_mode` | 编码消息时默认使用的 wire 模式，可映射为 `Mode::BitpackTlv`、`Mode::VarintTlv` 或 `Mode::JsonBytes`。 |
| `max_frame_bytes` | 网络层帧大小上限；由 `naws` 使用，本 crate 只处理 payload 编解码。 |

使用代码：

```rust
let mode = match cfg.ws.protocol.default_mode.as_str() {
    "BitpackTlv" => nasa::ws::Mode::BitpackTlv,
    "VarintTlv" => nasa::ws::Mode::VarintTlv,
    "JsonBytes" => nasa::ws::Mode::JsonBytes,
    other => anyhow::bail!("unsupported protocol mode: {other}"),
};

let bytes = message.encode(mode)?;
```

协议字段类型、字段顺序和是否可空不应交给 yml 动态控制，必须保持在结构体定义中。
