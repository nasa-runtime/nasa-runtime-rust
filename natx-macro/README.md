# natx-macro

`natx-macro` 提供 MySQL 与 PostgreSQL 两条编译期固定后端的事务属性宏。业务通常从
`nasa::tx::transactional`、`natx::transactional`、`nasa::tx::pgsql::transactional` 或
`natx_pgsql::transactional` 使用，不直接依赖本宏 crate。

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["tx"] }
```

PostgreSQL 入口使用独立 feature 和模块：

```toml
[dependencies]
nasa = { version = "1.0.3", default-features = false, features = ["tx-pgsql"] }
```

```rust
use nasa::tx::pgsql::transactional;

#[transactional(datasource = "reporting")]
async fn create_report() -> anyhow::Result<()> {
    Ok(())
}
```

```rust
use nasa::tx::transactional;

#[transactional]
async fn service_method() -> anyhow::Result<()> {
    Ok(())
}

#[transactional(datasource = "reporting")]
async fn report_method() -> anyhow::Result<()> {
    Ok(())
}
```

宏展开会保留原函数签名，把函数体包进：

```rust
nasa::tx::run(async move { ... }).await
```

或命名 datasource：

```rust
nasa::tx::run_for("reporting", async move { ... }).await
```

PostgreSQL 宏固定展开到 `nasa::tx::pgsql::run[_for]` 或直接运行时 `natx_pgsql::run[_for]`，不会根据
URL 在运行期猜测数据库后端。

约束：

- 只能用于 `async fn`。
- 函数返回值必须与 `anyhow::Result<T>` 兼容。
- 参数只支持空参数、字符串短写或 `datasource = "..."`。

## YML 配置与使用

`natx-macro` 没有运行期 yml。事务 datasource 名称写在属性上；连接 URL、pool 参数与注册由对应 typed
runtime 处理。Application 组合通过 `napp` 受管 MySQL/PostgreSQL pool；standalone 使用时由业务显式
建立并注册对应 pool。

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

示例：

```rust
#[transactional(datasource = "reporting")]
async fn rebuild_report() -> anyhow::Result<()> {
    Ok(())
}
```

常见边界：

| 现象 | 处理方式 |
| --- | --- |
| 非 async 函数使用宏 | 改成 `async fn`,事务上下文需要随 future 传播。 |
| datasource 找不到 | 先在启动阶段注册同名 pool,再调用带 datasource 的事务方法。 |
| 事务内要做提交后动作 | 使用 `natx` 的 after-commit API,不要在事务体提前执行外部副作用。 |
