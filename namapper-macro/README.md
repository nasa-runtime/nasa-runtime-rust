# namapper-macro

`namapper-macro` 是 MySQL、PostgreSQL 与 SQLite Mapper 共用的过程宏实现，提供 Mapper 派生、SQL 方法注解和
后端专用代码生成。业务应优先使用 `namapper`、`namapper-pgsql`、`namapper-sqlite` 或 `nasa` 门面，通常不直接依赖本 crate。
每个方法同时生成静态观测身份和生命周期守卫，逻辑调用与实际 SQLx 调用独立计量，结果流按 poll 与
终态计量。开发参数显示不增加原参数类型的必需 trait bound；策略由运行时 YAML 冻结。
合同见 [namapper-core](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/namapper-core/README.md#sql-观测与配置)。

```toml
[dependencies]
nasa = { version = "2.0.6", features = ["mapper"] }
```

## 静态目录与运行架构

编译期生成 SQL 结构、bind 顺序和 `module_path::Trait::method` 方法身份；应用启动时按该目录
冻结全局、数据源和方法策略。运行时守卫分别记录逻辑调用、SQLx 执行和 Stream 终态，缓存命中
不执行结果加载 SQL；SQLite 仍短暂借用连接进行只读性和来源验证。基础指标不需要业务 Hook，也不要求参数实现额外的 `Debug` 或 `Serialize`。

慢 SQL 阈值使用原始耗时的大于等于比较。日志、通知和导出配置由运行时解释，宏不创建 worker、
不连接通知渠道，也不把参数或 SQL 放进指标标签。业务初始化 `Notify` 后，由受管队列投递；
逐条通知还需启用告警并将通知冷却设为 0。

## Mapper trait

```rust
use nasa::mapper::{Mapper, Query};

#[Mapper]
trait UserMapper {
    #[Query("select id, name from user where id = #{id}")]
    /// 业务作用：读取指定用户的可选记录。
    /// 参数说明：
    /// - `id`：用户主键。
    /// 返回：存在时返回用户，不存在时为 None，数据库失败时返回错误。
    async fn find_by_id(&self, id: i64) -> anyhow::Result<Option<User>>;
}
```

宏会为 trait 生成可执行实现，把 SQL、参数绑定、动态 SQL、返回类型解析、缓存策略等编译为运行时代码。

方法注解同时接受 `#[Query("SELECT ...", cache = false)]` 与 `#[Query(sql = "SELECT ...", cache = false)]`；SQL 只能声明一次。

SQLite 导入 `nasa::mapper::sqlite::Mapper` 或 `namapper_sqlite::Mapper`，仍使用相同方法注解。匿名绑定通过 SQLite 词法解析，字节集合按使用位置区分 BLOB 与显式 IN/foreach；禁止裸占位符与行注释，保留字面量和块注释。静态 `checked = true` 使用对应 SQLite schema 或 offline 元数据，运行时仍通过受控 executor。完整边界见 [namapper-sqlite](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/namapper-sqlite/README.md)。

## 三种后端的选择与执行边界

| 业务导入路径 | driver 配置 | 生成代码的运行时 |
| --- | --- | --- |
| `nasa::mapper::Mapper` | `mysql` | `namapper` |
| `nasa::mapper::pgsql::Mapper` | `postgresql` | `namapper-pgsql` |
| `nasa::mapper::sqlite::Mapper` | `sqlite` | `namapper-sqlite` |

配置可让不同 datasource 使用不同 driver，宏仍固定绑定类型与 executor。SQLite client 固定文件 owner，执行前校验单条 SQL、权限和事务身份；SQL 方言和 Rust 行类型不会随 YAML 自动转换。napp 管理资源生命周期，宏不会另外创建数据库连接。

## 写操作

```rust
use nasa::mapper::{Delete, Insert, Mapper, Update};

#[Mapper]
trait UserWriteMapper {
    #[Insert("insert into user(id, name) values(#{id}, #{name})")]
    /// 业务作用：创建具有稳定主键的用户记录。
    /// 参数说明：
    /// - `id`：用户主键。
    /// - `name`：用户名称。
    /// 返回：成功影响行数；约束或数据库失败时返回错误。
    async fn insert_user(&self, id: i64, name: String) -> anyhow::Result<u64>;

    #[Update("update user set name = #{name} where id = #{id}")]
    /// 业务作用：更新指定用户的名称。
    /// 参数说明：
    /// - `id`：用户主键。
    /// - `name`：新的名称。
    /// 返回：成功影响行数；数据库失败时返回错误。
    async fn rename_user(&self, id: i64, name: String) -> anyhow::Result<u64>;

    #[Delete("delete from user where id = #{id}")]
    /// 业务作用：删除指定用户的记录。
    /// 参数说明：
    /// - `id`：用户主键。
    /// 返回：成功影响行数；数据库失败时返回错误。
    async fn delete_user(&self, id: i64) -> anyhow::Result<u64>;
}
```

## 事务内缓存声明

`cache = true` 表示方法允许使用 L2 缓存；事务内是否读写共享 L2 由业务用 `cache_in_tx = true` 显式声明。

SQLite 的显式事务缓存仅允许弱一致读取已有值，未命中时读取当前事务且不把结果写入共享 L2，不共享事务内加载任务。

```rust
#[Mapper]
trait UserReadMapper {
    #[Query(
        "select id, name from user where id = #{id}",
        cache = true,
        cache_in_tx = true
    )]
    /// 业务作用：按显式事务缓存策略读取用户记录。
    /// 参数说明：
    /// - `id`：用户主键。
    /// 返回：命中或查询得到的可选用户；来源或读取失败时返回错误。
    async fn find_cached_in_tx(&self, id: i64) -> anyhow::Result<Option<User>>;
}
```

## 边界

- 本 crate 只在编译期运行；MySQL、PostgreSQL、SQLite 运行时分别位于 `namapper`、`namapper-pgsql`、`namapper-sqlite`。
- 既有 `Mapper` 入口继续生成 MySQL `?` bind；PostgreSQL 专用入口生成 `$n` bind，不扫描替换 SQL 文本
  中的 `?`。
- 宏展开路径会识别直接运行时依赖或对应的 `nasa::mapper` 后端模块。
- 方法注解与返回类型的运行合同由所选 adapter 提供，YAML 不改变已生成代码的数据库类型。

## YML 配置与使用

`namapper-macro` 没有运行期 yml 配置。它只读取 Rust 属性宏参数，并在编译期生成代码。Application
按 datasource driver 受管 MySQL/PostgreSQL pool 与 SQLite 数据源；standalone PostgreSQL Mapper 由业务显式注册
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
| SQLite 文件 | 应用 yml | `datasources.local.sqlite.path`，同时声明 `driver: sqlite` |
| Redis L2 地址 | 应用 yml | `redis.url` |

应用侧 yml 示例见 `namapper` README。不要为本 crate 单独新增配置根节点；它没有运行时可初始化对象。
