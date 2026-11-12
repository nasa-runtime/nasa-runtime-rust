# nadis-derive

`nadis-derive` 提供 `#[derive(RedisDocument)]`，用于为 RediSearch/RedisJSON 文档结构生成 `nadis::RedisDocument` 元数据和字段转换代码。宏只生成对 `nadis` 公共 API 的调用，手写 impl 与派生 impl 语义一致。

业务通常通过 `nadis` 的 `derive` feature 或 `nasa::redis` 使用。

```toml
[dependencies]
nasa = { version = "2.0.6", features = ["redis-derive"] }
```

## 编译与运行边界

```text
结构体属性 ──→ nadis-derive 编译期校验 ──→ RedisDocument 元数据与转换实现
                                                    │
                                                    └─→ nadis / SearchActuator
```

未知或重复属性、冲突的索引类型、缺少唯一 ID 或非法占位符会直接导致编译失败。宏不连接 Redis，不创建索引，也不拥有后台
任务或指标；运行期 schema 校验、索引创建、错误与观测由 `nadis` 和调用方负责。

## 基本文档

```rust
use nasa::redis::RedisDocument;

#[derive(RedisDocument)]
#[rs(index = "idx:user", prefix = "user:", data_type = "hash")]
struct UserDoc {
    #[rs(id)]
    id: i64,
    #[rs(tag, sortable)]
    tenant: String,
    #[rs(text, weight = 2.0)]
    name: String,
    #[rs(numeric, sortable)]
    age: i64,
}
```

生成内容包括：

- 文档元数据：index、prefix、bucket_count、字段类型。
- `id()`：生成 Redis document id。
- `to_fields()`：把结构体序列化为 Redis field/value。
- `from_fields()`：从 Redis field/value 恢复结构体。
- placeholder 和 array key 元数据。

## 字段类型场景

Tag 字段适合枚举、租户、状态：

```rust
#[rs(tag, alias = "state")]
status: String,
```

Text 字段适合全文检索：

```rust
#[rs(text, weight = 1.5)]
title: String,
```

Numeric 字段适合范围查询和排序：

```rust
#[rs(numeric, sortable)]
created_at: i64,
```

JSON path 适合 RedisJSON 文档中的嵌套字段：

```rust
#[rs(tag, json_path = "$.profile.country")]
country: String,
```

## 约束

- 必须且只能有一个 `#[rs(id)]` 字段。
- 一个字段只能声明 tag、text、numeric、geo 中的一种索引类型；同一结构体或字段的 rs 属性不能重复，分散在多个属性块中也一样。
- 仅支持没有泛型、生命周期或 const 泛型参数的具名字段结构体。
- ID 的数值类型按 Rust 原始整数和浮点类型名称识别，支持透明括号和宏类型分组；不解析类型别名。
- 字段按 Rust 名称生成 HASH 映射，不解释 serde 重命名。JSON 模式下须使序列化名称与元数据一致。
- `r#type` 等 raw identifier 的存储名为 `type`，键模板也写 `{type}`；Rust 代码访问仍保留关键字转义。
- 全部字段参与 HASH 编解码，未建立索引的字段也会保存；缺失字段使用 Default，存在但解析失败时返回错误。
- HASH 解析错误只包含静态字段名，不格式化原始值或自定义错误，也不要求 FromStr 的错误类型实现 Debug。自定义解析、Default 和析构中的展开仍由调用方承担。
- `bucket_count` 仅适用于 `json_array_bucket`，并且必须大于 1；它属于持久化布局，不能直接更改已有数据的桶数。
- `order` 必须和 `array_key` 一起使用。索引选项必须适用于字段类型，不会被静默忽略。
- `prefix` 中的占位符会被解析为文档 key 片段；占位符写错会编译时报错。
- 运行时路径会自动识别直接依赖 `nadis` 或经 `nasa::redis` 门面使用。

| 字段选项 | 可用索引类型 |
| --- | --- |
| sortable | tag、text、numeric |
| separator、casesensitive | tag |
| weight、nostem、phonetic | text |
| noindex | text、numeric |
| suffix | tag、text |
| alias、json_path | tag、text、numeric、geo |

## YML 配置与使用

`nadis-derive` 没有运行期 yml。索引名、key 前缀、字段类型、sortable、alias、JSON path 都写在 Rust 属性上；Redis 连接和 RediSearch 是否启用由 `nadis` 配置负责。

推荐应用配置：

```yaml
redis:
  url: ${APP_REDIS_URL}
  namespace: search-service
  profile: RustV2

search:
  create_index_on_start: true
  validate_schema_on_start: true
```

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| index、prefix、id 字段 | `#[rs(...)]` 属性 |
| tag/text/numeric/geo 字段类型 | 字段属性 |
| Redis URL、namespace、profile | `nasa::redis::RedisConfig` yml |
| 启动时是否建索引、校验 schema | 应用 yml + `SearchActuator` 调用 |

示例：

```rust
#[derive(nasa::redis::RedisDocument)]
#[rs(index = "idx:user", prefix = "user:{tenant}:")]
struct UserDoc {
    #[rs(id)]
    id: i64,
    tenant: String,
    #[rs(text, sortable)]
    name: String,
}
```
