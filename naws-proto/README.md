# naws-proto

`naws-proto` 提供 ProtocolBytes 的 typed codec、已有长连接 schema 和有界字节读取。JSON_BYTES、VARINT_TLV、BITPACK_TLV 与 FAST_FIXED 的格式由调用方显式选择；本组件不决定路由权限、业务时间、投递期限或消息目标。

## 编解码与网络边界

```text
schema + 显式 Mode → WireCodec → payload 字节
payload + 外层 frame → naws → TCP / WebSocket
```

本组件不读取 YAML、不建立连接，也不拥有认证或重试。外层帧负责完整 payload 的边界，应用负责业务字段校验。字符串和字节字段的读取上限为 16 MiB，字符串数组最多 65536 个元素；非法长度、截断、位图和 wire type 返回 `CodecError`。

## 派生自定义 schema

直接依赖 `naws-proto` 时，可从同一 crate 导入 `ProtocolBytes` 宏和 `WireCodec`。生成代码也支持 Cargo 依赖别名；经 `naws::proto` 或 `nasa::ws::proto` 使用时，运行时路径解析到对应门面。

```rust
use naws_proto::{Mode, ProtocolBytes, WireCodec};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, ProtocolBytes)]
struct Sample {
    #[proto(tag = 10)]
    sequence: i64,
    #[proto(tag = 20)]
    reading: f64,
}

let sample = Sample { sequence: 7, reading: 1.5 };
let bytes = sample.encode(Mode::FastFixed)?;
let decoded = Sample::decode(Mode::FastFixed, &bytes)?;
```

字段必须显式标注唯一的非零 tag，具名结构体最多 64 个字段，需要 `Default`、`Serialize` 和 `Deserialize`。当前 derive 支持 `i8`、`i16`、`i32`、`i64`、`u16`、`bool`、`f32`、`f64`、`Option<String>`、`Option<Vec<u8>>`、`Option<Vec<Option<String>>>`。其它集合、嵌套、枚举、可空数值和泛型结构体不在当前 derive 类型集合中。

## 各模式的合同

| 模式 | 布局与限制 |
| --- | --- |
| JSON_BYTES | 使用 serde 字段名称和属性，UTF-8 JSON；自定义 schema 的 JSON 映射由 serde 定义 |
| VARINT_TLV | 按 tag 排序写 key 与 value；可空字段缺失时不写，未知字段按 wire type 跳过 |
| BITPACK_TLV | 按 tag 排序确定存在位，位图采用小端；禁止 schema 外的位和尾随字节 |
| FAST_FIXED | 按 tag 排序连续写小端基础类型，解码必须恰好等于 schema 总长度；任何可空或变长字段均返回 `Unsupported` |

FAST_FIXED 中 bool/i8 为 1 字节，i16/u16 为 2 字节，i32/f32 为 4 字节，i64/f64 为 8 字节。空 schema 合法，编码为空字节。包含字符串的已有 `Message`、`AuthRequest` 等 schema 不满足固定布局，调用其 FAST_FIXED 入口仍会拒绝。

二进制模式的有符号整数使用 ZigZag（FAST_FIXED 使用原始补码），u16 使用无符号值，对应 Java 的 char 字段二进制布局；JSON 默认是数值，并不自动转换为 Java 字符串。TLV 浮点使用规范 NaN 位模式，FAST_FIXED 保留原始浮点位；JSON 非有限浮点的行为由 serde 决定，不能依赖其无损往返。窄整数超出 schema 位宽返回 `VarintOverflow`，不会截断。

VARINT_TLV 在字段边界结束时，缺失字段保留 `Default`；不能据此检测整条消息被截去若干完整字段，完整性由外层 framing 保证。BITPACK_TLV 与 FAST_FIXED 依赖双方 schema 一致，修改字段集合、类型或排序时必须协调协议合同。

## 已有 schema 与跨语言映射

已有 `Message`、`AuthRequest`、`AuthResponse`、`CloseReason`、`ClusterEvent` 保留既有 tag 和 JSON 属性。内置 byte[] 的 JSON 映射使用有符号字节；自定义 `Vec<u8>` 的 serde 默认映射不会自动继承这一规则。字符串数组元素的空串和 null 在现有内置 schema 中按约定归一，业务不应依赖空串元素与 null 的区别。

派生宏只生成字节转换，不校验业务身份、时间戳、授权版本和领域状态。应用应在自己的模型与准入边界处理这些不变量。

## 动态 schema 与 JSON 边界

`dynamic::SchemaPlan` 将封闭的 `SchemaDescriptor` 目录编译为可共享的不可变计划。它支持
基础数值、可空字段、字符串、字节、枚举、单层集合和嵌套对象，拒绝循环引用、任意代码与网络类型加载。
每份计划固定一个 Mode，嵌套对象使用同一模式。FAST_FIXED 仍只接受不可空的定宽基础字段。

```rust
use naws_proto::{Mode, dynamic::{Limits, SchemaDescriptor, SchemaPlan}};
use std::collections::BTreeMap;

let schema: SchemaDescriptor = serde_json::from_str(r#"{
  "schemaId":"sample",
  "fields":[{"tag":1,"name":"number","type":{"kind":"i64"}}]
}"#)?;
let catalog = BTreeMap::from([(schema.schema_id.clone(), schema)]);
let plan = SchemaPlan::compile("sample", &catalog, Mode::BitpackTlv, Limits::default())?;
let object = plan.import_json(br#"{"number":9223372036854775807}"#)?;
let bytes = plan.encode(&object)?;
let decoded = plan.decode(bytes)?;
let json = plan.export_json(&decoded)?;
```

默认预算为消息 1 MiB、引用深度 8、展开字段 4096 和动态值 65536；每个字段还可限制字节数与集合元素数。
JSON 导入拒绝重复键、超深结构和整数窄化溢出；i64 保留整数词法精度。二进制字节字段可共享输入 Bytes，
对象、集合和字符串仍有分配，JSON 转换也会构造中间值，不承诺整个过程零拷贝。

动态 JSON 遵循 ProtocolBytes 字段映射：直接 byte[] 是有符号字节数组，集合里的 byte[] 是 Base64 字符串；
char 是单个 UTF-16 单元的字符串，孤立 surrogate 拒绝。空引用按约定省略，NaN 与 Infinity 使用明确字符串。
Java JSON 解码可能将负零归一为正零，JSON 转换不保证保留浮点原始位；需要保留 NaN 原始位时使用合法
FAST_FIXED schema。VARINT/BITPACK 的零长度集合元素归一为 null，不能区分空字节、空字符串和 null。
`import_json` 与 `export_json` 不改变计划模式，只在 HTTP 或配置边界转换同一 schema 的表示。
