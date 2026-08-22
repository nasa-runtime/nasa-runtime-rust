# namapper-macro

`namapper-macro` 是 MySQL 与 PostgreSQL Mapper 共用的过程宏实现，提供 Mapper 派生、SQL 方法注解和
后端专用代码生成。业务应优先使用 `namapper`、`namapper-pgsql` 或 `nasa` 门面，通常不直接依赖本 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["mapper"] }
```

## Mapper trait

```rust
use nasa::mapper::{Mapper, Query};

#[Mapper]
trait UserMapper {
    #[Query("select id, name from user where id = #{id}")]
    async fn find_by_id(&self, id: i64) -> anyhow::Result<Option<User>>;
}
```

宏会为 trait 生成可执行实现，把 SQL、参数绑定、动态 SQL、返回类型解析、缓存策略等编译为运行时代码。

## 写操作

```rust
use nasa::mapper::{Delete, Insert, Mapper, Update};

#[Mapper]
trait UserWriteMapper {
    #[Insert("insert into user(id, name) values(#{id}, #{name})")]
    async fn insert_user(&self, id: i64, name: String) -> anyhow::Result<u64>;

    #[Update("update user set name = #{name} where id = #{id}")]
    async fn rename_user(&self, id: i64, name: String) -> anyhow::Result<u64>;

    #[Delete("delete from user where id = #{id}")]
    async fn delete_user(&self, id: i64) -> anyhow::Result<u64>;
}
```

## 事务内缓存声明

`cache = true` 表示方法允许使用 L2 缓存；事务内是否读写共享 L2 由业务用 `cache_in_tx = true` 显式声明。

```rust
#[Mapper]
trait UserReadMapper {
    #[Query(
        "select id, name from user where id = #{id}",
        cache = true,
        cache_in_tx = true
    )]
    async fn find_cached_in_tx(&self, id: i64) -> anyhow::Result<Option<User>>;
}
```

## 边界

- 本 crate 只在编译期运行；MySQL 运行时位于 `namapper`，PostgreSQL 运行时位于 `namapper-pgsql`。
- 既有 `Mapper` 入口继续生成 MySQL `?` bind；PostgreSQL 专用入口生成 `$n` bind，不扫描替换 SQL 文本
  中的 `?`。
- 宏展开路径会识别直接运行时依赖或对应的 `nasa::mapper` 后端模块。
- 新增参数语义时要同步更新 `namapper/README.md` 和运行时行为说明。

## YML 配置与使用

`namapper-macro` 没有运行期 yml 配置。它只读取 Rust 属性宏参数，并在编译期生成代码。Application
按 datasource driver 受管 MySQL/PostgreSQL pool；standalone PostgreSQL Mapper 由业务显式注册
`natx-pgsql` pool。

属性和 yml 的分工：

| 配置项 | 位置 | 示例 |
| --- | --- | --- |
| Mapper key | Rust 属性 | `#[Mapper(key = "user")]` |
| 查询 SQL | Rust 属性 | `#[Query("select ...")]` |
| 是否读写 L2 | Rust 属性 | `cache = true` / `cache = false` |
| 事务内是否允许 L2 | Rust 属性 | `cache_in_tx = true` |
| datasource 名称 | Rust 属性 | `datasource = "reporting"` |
| MySQL URL | 应用 yml | `datasources.orders.url` |
| PostgreSQL URL | 应用 yml | `datasources.reporting.url` |
| Redis L2 地址 | 应用 yml | `redis.url` |

应用侧 yml 示例见 `namapper` README。不要为本 crate 单独新增配置根节点；它没有运行时可初始化对象。
