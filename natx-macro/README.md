# natx-macro

`natx-macro` 提供 MySQL、PostgreSQL 与 SQLite 编译期固定后端的事务属性宏。业务通常从
`nasa::tx::transactional`、`natx::transactional`、`nasa::tx::pgsql::transactional` 或
`natx_pgsql::transactional`、`nasa::tx::sqlite::transactional` 或 `natx_sqlite::transactional` 使用，不直接依赖本宏 crate。

```toml
[dependencies]
nasa = { version = "2.0.6", features = ["tx"] }
```

PostgreSQL 入口使用独立 feature 和模块：

```toml
[dependencies]
nasa = { version = "2.0.6", default-features = false, features = ["tx-pgsql"] }
```

```rust
use nasa::tx::pgsql::transactional;

#[transactional(datasource = "reporting")]
/// 业务作用：在 reporting 的 PostgreSQL 事务中执行报表业务。
/// 参数说明：无。
/// 返回：事务提交确认后成功；业务或数据库失败时返回错误。
async fn create_report() -> anyhow::Result<()> {
    Ok(())
}
```

```rust
use nasa::tx::transactional;

#[transactional]
/// 业务作用：在默认 MySQL 来源中执行同一事务域的业务。
/// 参数说明：无。
/// 返回：事务提交确认后成功；业务或数据库失败时返回错误。
async fn service_method() -> anyhow::Result<()> {
    Ok(())
}

#[transactional(datasource = "reporting")]
/// 业务作用：在指定 MySQL 来源中执行报表业务。
/// 参数说明：无。
/// 返回：事务提交确认后成功；业务或数据库失败时返回错误。
async fn report_method() -> anyhow::Result<()> {
    Ok(())
}
```

## 编译期选择与事务架构

宏保留原函数签名和返回类型，把函数体交给所选后端的 `run` 或 `run_for`；SQLite 显式模式由 `run_for_mode` 处理。

```text
宏导入路径 + datasource/mode 属性 → 编译期固定 typed runtime
  → 根事务取得同源 lease 并 BEGIN，或验证并加入环境事务
  → 业务 future → COMMIT / ROLLBACK → 确认提交后执行回调
```

每个 datasource 的 `driver` 可以不同，但 YAML 不会改变已生成函数的后端类型。事务运行时校验来源、owner 和期限，宏不创建连接池或持有文件关闭权。

PostgreSQL 宏固定展开到 `nasa::tx::pgsql::run[_for]` 或直接运行时 `natx_pgsql::run[_for]`，不会根据
URL 在运行期猜测数据库后端。

约束：

- 只能用于 `async fn`。
- 函数返回值必须与 `anyhow::Result<T>` 兼容。
- 来源支持空参数、字符串短写或 `datasource = "..."`；`never` 禁止环境事务。
- SQLite 还支持 `mode = "deferred"` / `"immediate"`；只有最外层选择物理模式，内层省略时继承，显式冲突时拒绝。`never` 与 `mode` 不能同时使用，MySQL/PostgreSQL 不接受 SQLite 的模式参数。

SQLite 应用开启 `application,tx-sqlite`，通过 `#[nasa::application("db")]` 与 `datasources.<name>.driver: sqlite` 托管文件来源。

```rust
use nasa::tx::sqlite::transactional;

#[transactional(datasource = "local", mode = "immediate")]
/// 业务作用：在 local 文件来源中取得 Immediate 事务并更新本地状态。
/// 参数说明：无。
/// 返回：提交确认后成功；锁竞争或数据库失败时返回错误。
async fn update_local_state() -> anyhow::Result<()> {
    Ok(())
}
```

SQLite 的执行、取消、排空和文件所有权边界见 [natx-sqlite](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/natx-sqlite/README.md)。事务体不自动重试；提交未确认必须核对持久业务事实。

## YML 配置与使用

`natx-macro` 没有运行期 yml。事务 datasource 名称写在属性上；连接 URL、pool 参数与注册由对应 typed
runtime 处理。Application 组合通过 `napp` 受管 MySQL/PostgreSQL pool 与 SQLite 文件数据源；standalone 使用时由业务显式
建立并注册对应 pool；SQLite 调用 `open_owned` 保留 owner，并通过 `try_init_datasource` 注册使用句柄。

推荐配置：

```yaml
datasources:
  default:
    url: ${APP_MYSQL_URL}
    max_connections: 16
  reporting:
    driver: postgresql
    url: ${APP_REPORTING_POSTGRES_URL}
    max_connections: 8
```

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| 是否开启事务 | `#[transactional]` 属性 |
| datasource 名称 | `#[transactional(datasource = "...")]` |
| MySQL/PostgreSQL URL、连接池参数 | 应用 yml |
| pool 注册 | `natx::try_init` / `natx::try_init_datasource` |
| PostgreSQL standalone pool 注册 | `natx_pgsql::try_init` / `natx_pgsql::try_init_datasource` |
| SQLite 文件与独立 owner | `sqlite.path`；`natx_sqlite::open_owned` / `try_init_datasource` |

示例：

```rust
#[transactional(datasource = "reporting")]
/// 业务作用：将报表重建工作放入已选择后端的 reporting 事务。
/// 参数说明：无。
/// 返回：提交确认后成功；业务或数据库失败时返回错误。
async fn rebuild_report() -> anyhow::Result<()> {
    Ok(())
}
```

常见边界：

| 现象 | 处理方式 |
| --- | --- |
| 非 async 函数使用宏 | 改成 `async fn`,事务上下文需要随 future 传播。 |
| datasource 找不到 | 先在启动阶段注册同名 pool,再调用带 datasource 的事务方法。 |
| 事务内要做提交后动作 | 使用所选 driver 的 `after_commit` API,不要在事务体提前执行外部副作用。 |
