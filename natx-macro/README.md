# natx-macro

`natx-macro` 提供 `#[transactional]` 属性宏。业务通常从 `nasa::tx::transactional` 或 `natx::transactional` 使用，不直接依赖本宏 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["tx"] }
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

约束：

- 只能用于 `async fn`。
- 函数返回值必须与 `anyhow::Result<T>` 兼容。
- 参数只支持空参数、字符串短写或 `datasource = "..."`。

## YML 配置与使用

`natx-macro` 没有运行期 yml。事务 datasource 名称写在属性上，MySQL URL 和连接池大小写在应用 yml，并由 `natx` 启动时注册。

推荐配置：

```yaml
datasources:
  default:
    url: ${APP_MYSQL_URL}
    max_connections: 16
  reporting:
    url: ${APP_REPORTING_MYSQL_URL}
    max_connections: 8
```

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| 是否开启事务 | `#[transactional]` 属性 |
| datasource 名称 | `#[transactional(datasource = "...")]` |
| MySQL URL、连接池参数 | 应用 yml |
| pool 注册 | `natx::try_init` / `natx::try_init_datasource` |

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
