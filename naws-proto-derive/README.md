# naws-proto-derive

`naws-proto-derive` 提供 `#[derive(ProtocolBytes)]`，按字段 tag 和类型生成 JSON_BYTES、VARINT_TLV、BITPACK_TLV、FAST_FIXED 编解码。它依赖 `naws-proto` 的协议原语，不建立连接、不读取配置，也不绑定具体服务的路由或业务模型。

## 独立应用使用

通常从 `naws_proto::ProtocolBytes` 导入重导出的宏。也可以直接依赖本宏包，并同时依赖 `naws-proto`、`naws` 或启用 `ws` 的 `nasa`；生成代码解析实际依赖名，支持依赖别名和公开门面，不要求消费方定义 `crate::__rt`。

```rust
use naws_proto::{ProtocolBytes, WireCodec, Mode};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize, ProtocolBytes)]
struct Reading {
    #[proto(tag = 10)]
    source: i64,
    #[proto(tag = 20)]
    value: f64,
}

let bytes = Reading { source: 3, value: 1.5 }.encode(Mode::FastFixed)?;
```

消费方通过其它公开库重导出协议模块时，可在业务结构体上使用 `#[proto(crate_path = "my_facade::protocol")]` 指定完整协议入口。该路径需要提供 `naws-proto` 的公开协议原语；依赖别名可直接写入路径。未指定时仍按既有直接依赖和门面规则查找，重复路径或非法容器属性在编译时拒绝。

## Schema 约束

- 仅支持具名、无泛型参数的结构体，最多 64 个字段。
- 每个字段需要唯一 `#[proto(tag = N)]`，tag 范围是 `1..=2^61-1`；重复或未知属性拒绝展开。
- 基础类型支持 `i8`、`i16`、`i32`、`i64`、`u16`、`bool`、`f32`、`f64`。
- 引用类型支持 `Option<String>`、`Option<Vec<u8>>`、`Option<Vec<Option<String>>>`。
- FAST_FIXED 只接受基础类型，按 tag 升序写小端固定字节；引用字段返回 `CodecError::Unsupported`，不隐式更换模式。
- JSON 的字段名称、缺省、字节及字符映射由 serde 属性决定；二进制布局相同不代表任意自定义 JSON 映射与 Java 相同。

完整 wire 合同与各类型边界见 [naws-proto](../naws-proto/README.md)。已有 tag 不复用于不同语义；VARINT_TLV 的未知字段跳过能力不适用于固定 schema 的 BITPACK_TLV 和 FAST_FIXED。
