# naws-proto-derive

`naws-proto-derive` 提供 `#[derive(ProtocolBytes)]`，为 NASA WebSocket/TCP wire schema 生成
VARINT_TLV、BITPACK_TLV 和 JSON_BYTES 分派代码。它是 `naws-proto` 的编译期支撑 crate，
生成代码固定引用使用方 crate 根下的 `WireCodec`、`Mode`、`CodecError` 和 `__rt`，不是独立的通用序列化框架。

```toml
[dependencies]
naws-proto-derive = { version = "1" }
```

业务项目通常只依赖 `naws-proto`；只有维护协议 schema 的 crate 才直接依赖本包。

## Schema 端使用

```rust
use naws_proto_derive::ProtocolBytes;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, ProtocolBytes)]
struct LoginReq {
    #[proto(tag = 1)]
    uid: i64,
    #[proto(tag = 2)]
    token: Option<String>,
}
```

上例只适用于已经提供内部 codec runtime 的协议 crate；普通业务 crate 直接照搬会因缺少 `crate::__rt`
等内部合同而编译失败。对外消息类型应由 `naws-proto` 发布和复用。

## 适用场景

- 维护 `naws-proto` 内的长连接 schema，避免手写 VARINT_TLV / BITPACK_TLV 字段编解码。
- 保持结构体定义、字段 tag 和 wire 编码绑定，便于跨语言 golden 对拍。
- 迁移旧协议时，把旧版字段 tag 和类型明确写在结构体上。

## 边界

- 仅支持具名 struct，最多 64 个字段。
- `tag` 必须唯一、非零且不大于 `2^61-1`；wire 兼容依赖 tag 与类型稳定，而不是 Rust 字段排列位置。
- 支持 `Option<String>`、`Option<Vec<u8>>`、`Option<Vec<Option<String>>>`、`i64`、`i32`、`i8` 和 `bool`。
- 结构体必须满足生成代码需要的 `Default` 与 serde 合同。
- 宏只负责 schema 到 codec 的静态展开，不负责网络收发；FAST_FIXED 当前明确返回 unsupported。

## YML 配置与使用

`naws-proto-derive` 没有运行期 yml。它只读取结构体上的派生宏和字段属性，生成所在协议 crate 的 `WireCodec` 实现。

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| 消息字段、字段顺序、字段类型 | Rust 结构体 |
| 字段 tag、协议模式约束 | Rust 属性与协议 crate |
| TCP/WS 监听、帧大小、默认模式 | `naws` 应用 yml |

使用建议：

```rust
#[derive(Default, serde::Serialize, serde::Deserialize, ProtocolBytes)]
struct QuoteEvent {
    #[proto(tag = 1)]
    symbol: Option<String>,
    #[proto(tag = 2)]
    price: i64,
}
```

字段演进建议：

| 变更 | 处理方式 |
| --- | --- |
| 新增可选字段 | 使用新且唯一的 tag，并保持旧 tag 语义不变。 |
| 修改字段类型 | 视为协议不兼容，分配新 tag 或新增版本化结构体。 |
| 删除字段 | 永久保留其 tag，不得复用给其它语义。 |
| 切换编码模式 | 先补 golden 对拍,确认 BITPACK、VARINT、JSON 三种入口行为符合预期。 |

不要用 yml 控制 schema；schema 一旦变化就属于协议升级，应通过代码评审和兼容性验证处理。
