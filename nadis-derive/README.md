# nadis-derive

`nadis-derive` 提供 `#[derive(RedisDocument)]`，用于为 RediSearch/RedisJSON 文档结构生成 `nadis::RedisDocument` 元数据和字段转换代码。宏只生成对 `nadis` 公共 API 的调用，手写 impl 与派生 impl 语义一致。

业务通常通过 `nadis` 的 `derive` feature 或 `nasa::redis` 使用。

```toml
[dependencies]
nasa = { version = "1", features = ["redis-derive"] }
```

## 基本文档

```rust
use nasa::redis::RedisDocument;

#[derive(RedisDocument)]
#[rs(index = "idx:user", prefix = "user:", bucket_count = 128)]
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
- 数值字段会按 Rust 类型自动识别。
- `prefix` 中的占位符会被解析为文档 key 片段；占位符写错会编译时报错。
- 运行时路径会自动识别直接依赖 `nadis` 或经 `nasa::redis` 门面使用。

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
| tag/text/numeric/vector 字段类型 | 字段属性 |
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
