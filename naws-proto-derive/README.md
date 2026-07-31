# naws-proto-derive

`naws-proto-derive` 提供 `#[derive(ProtocolBytes)]`，为 WebSocket/TCP wire schema 生成逐字节兼容的 NASA 协议编解码代码。业务通常通过 `naws-proto` 使用，不直接依赖本 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["ws"] }
naws-proto-derive = { version = "1" }
```

## 基本使用

```rust
use naws_proto_derive::ProtocolBytes;

#[derive(ProtocolBytes)]
struct LoginReq {
    uid: i64,
    token: String,
}
```

派生宏会根据 `naws-proto` 的字段规则生成 `WireCodec` 编码和解码实现，供 `naws` 服务端、客户端或跨语言对拍使用。

## 适用场景

- 新增长连接消息体时，避免手写 VARINT_TLV / BITPACK_TLV 字段编解码。
- 做协议 golden 对拍时，保持结构体定义和 wire 编码绑定。
- 迁移旧协议时，把旧版字段顺序和类型明确写在结构体上。

## 边界

- 宏只负责 schema 到 codec 的静态展开，不负责网络收发。
- wire 兼容要求字段顺序和类型稳定，修改字段前要补 golden 对拍。

## YML 配置与使用

`naws-proto-derive` 没有运行期 yml。它只读取结构体上的派生宏和字段属性，生成 `naws-proto::WireCodec` 实现。

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| 消息字段、字段顺序、字段类型 | Rust 结构体 |
| 字段重命名、协议模式约束 | Rust 属性 |
| TCP/WS 监听、帧大小、默认模式 | `naws` 应用 yml |

使用建议：

```rust
#[derive(ProtocolBytes)]
struct QuoteEvent {
    symbol: String,
    price: i64,
}
```

字段演进建议：

| 变更 | 处理方式 |
| --- | --- |
| 新增可选字段 | 放在末尾,给默认值或 `Option<T>`。 |
| 修改字段类型 | 视为协议不兼容,新增结构体或版本字段。 |
| 删除字段 | 先保持解码兼容,确认所有调用方升级后再清理。 |
| 切换编码模式 | 先补 golden 对拍,确认 BITPACK、VARINT、JSON 三种入口行为符合预期。 |

不要用 yml 控制 schema；schema 一旦变化就属于协议升级，应通过代码评审和兼容性验证处理。
