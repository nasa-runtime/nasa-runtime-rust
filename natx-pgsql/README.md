# natx-pgsql

`natx-pgsql` 提供 PostgreSQL 的命名连接池、ambient transaction、强制事务连接与提交结果分类。
它与 `natx` 共享 `natx-core` 的 datasource catalog，为同 owner 的 MySQL/PostgreSQL 受管编排提供
typed registry 基础；本 crate 自身不接入 Application。单个本地事务不能跨 driver 或跨 datasource。

本 crate 只提供事务与连接基础能力，不包含 Application YAML 受管接线、migration、Mapper、Inbox、
Outbox 或 Saga。依赖本 crate 不会自动启用这些能力。

## 使用入口

直接依赖运行时：

```toml
[dependencies]
natx-pgsql = "1"
```

也可以通过门面只启用 PostgreSQL 事务能力：

```toml
[dependencies]
nasa = { version = "1", default-features = false, features = ["tx-pgsql"] }
```

```rust
use nasa::tx::pgsql::{self, transactional};

#[transactional(datasource = "reporting")]
async fn write_report() -> anyhow::Result<()> {
    let mut connection = pgsql::mandatory_conn_for("reporting").await?;
    sqlx::query("INSERT INTO reports(id) VALUES ($1)")
        .bind(1_i64)
        .execute(connection.as_mut())
        .await?;
    Ok(())
}
```

## 数据源配置与注册

`datasource::DataSourceConfig` 接受 `postgres://` 与 `postgresql://`，并在网络 I/O 前校验 pool 容量、
超时和 URL scheme。兼容入口 `probe` / `build_pool` 不改写会话参数，保留 PostgreSQL 服务端默认
`search_path`；需要显式业务 schema 时使用 `probe_in_schema` / `build_pool_in_schema`，它们校验普通
PostgreSQL identifier，并在探测及每条新连接上确认 `current_schema()`。目标 schema 必须由数据库初始化
流程预先创建。`Debug`、探测错误与建池错误不会输出用户名、口令、schema 名称、查询参数或完整连接串。

```rust
use natx_pgsql::datasource::{build_pool, probe, DataSourceConfig};

let config: DataSourceConfig = serde_json::from_value(raw)?;
config.validate()?;
probe(&config).await?;
let pool = build_pool(&config)?;
natx_pgsql::try_init(pool)?;
```

默认池使用 `try_init`，命名池使用 `try_init_datasource`。standalone PostgreSQL 与 standalone MySQL
共享进程模式门禁，不能同时成为全局权威；双后端并存只允许由同一 owner、同一 catalog 的受管编排入口
完成。

## 事务与失败语义

- `run` / `run_for` 保持 `anyhow::Result` 便利语义；`run_decided_for_checked` 分离结构化 datasource
  lookup 与事务执行错误。
- 嵌套调用只复用同 driver、同 datasource 的外层事务。跨 driver 和跨 datasource 在业务闭包及 SQL
  执行前拒绝。
- `mandatory_conn` 不会在事务上下文缺失时降级为 autocommit；同一事务内不要同时持有两个 `PgConn`。
- 内层回滚会把最外层事务置为 rollback-only。after-commit hook 只在数据库明确确认最外层提交后执行。
- 数据库明确拒绝 COMMIT 与结果未知分开返回；结果未知不会自动重放业务闭包，也不会运行 hook。
- `#[transactional_pgsql(never)]`(运行时入口 `run_never`)拒绝任一数据库 driver 的 ambient 事务且不开启事务——供
  "绝不能被外层事务包住"的路径把违规调用变成进入前的显式错误；`never` 与 `datasource` 互斥。
- 提交/回滚前对事务连接槽做 fail-fast 独占：业务体已返回却仍被持有的连接句柄(未消费完的事务内
  `MapperStream`、被移出事务体的 `PgConn`)会让事务以 "transaction connection is still held at
  commit" 显式失败，而不是在槽锁上永久卡死。
- `classify_sqlstate` 提供 `23505`、`40001`、`40P01`、`55P03` 和 SQLSTATE class `08` 的稳定分类，
  不解析数据库错误正文。
