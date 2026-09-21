# namapper-macro

`namapper-macro` 是 MySQL 与 PostgreSQL Mapper 共用的过程宏实现，提供 Mapper 派生、SQL 方法注解和
后端专用代码生成。业务应优先使用 `namapper`、`namapper-pgsql` 或 `nasa` 门面，通常不直接依赖本 crate。
每个方法同时生成静态观测身份和生命周期守卫，逻辑调用与实际 SQLx 调用独立计量，结果流按 poll 与
终态计量。开发参数显示不增加原参数类型的必需 trait bound；策略由运行时 YAML 冻结。
合同见 [namapper-core](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/namapper-core/README.md#sql-观测与配置)。

```toml
[dependencies]
nasa = { version = "1", features = ["mapper"] }
```

## 静态目录与运行架构

编译期生成 SQL 结构、bind 顺序和 `module_path::Trait::method` 方法身份；应用启动时按该目录
冻结全局、数据源和方法策略。运行时守卫分别记录逻辑调用、SQLx 执行和 Stream 终态，缓存命中
不产生数据库调用。基础指标不需要业务 Hook，也不要求参数实现额外的 `Debug` 或 `Serialize`。

慢 SQL 阈值使用原始耗时的大于等于比较。日志、通知和导出配置由运行时解释，宏不创建 worker、
不连接通知渠道，也不把参数或 SQL 放进指标标签。业务初始化 `Notify` 后，由受管队列投递；
逐条通知还需启用告警并将通知冷却设为 0。

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
- 方法注解与返回类型的运行合同由所选 MySQL 或 PostgreSQL adapter 提供。

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
