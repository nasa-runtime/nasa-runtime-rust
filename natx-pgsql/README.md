# natx-pgsql

`natx-pgsql` 提供 PostgreSQL 的命名连接池、ambient transaction、强制事务连接、提交结果分类，
以及分离的连接池获取、事务连接槽等待和执行前拒绝观测。
它与 `natx` 共享 `natx-core` 的 datasource catalog，为同 owner 的 MySQL/PostgreSQL 受管编排提供
typed registry 基础。通过门面同时开启 `application,tx-pgsql` 后，由 `napp` 的 `"db"` 组件纳管
连接、迁移门禁与关闭；本 crate 自身不依赖 Application。单个本地事务不能跨 driver 或跨 datasource。

本 crate 只提供事务与连接基础能力，不包含 Application YAML 受管接线、migration、Mapper、Inbox、
Outbox 或 Saga。依赖本 crate 不会自动启用这些能力。

## 使用入口

直接依赖运行时：

```toml
[dependencies]
natx-pgsql = "2.0.0"
```

也可以通过门面只启用 PostgreSQL 事务能力：

```toml
[dependencies]
nasa = { version = "2.0.0", default-features = false, features = ["tx-pgsql"] }
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

Application 的业务停机任务在受管数据库关闭之前执行，可通过 PostgreSQL datasource 入口发起有界
事务；任务返回前应归还连接和资源借用。pool 的最终关闭属于 Application，不再另登记重复关闭 future。
若全局停机期限内未取得 COMMIT 的明确结果，不能把任务结束视为事务已提交，也不能盲目重放业务闭包。

## 连接观测与语句日志

`conn_*` 的连接用途固定为 `direct`，`mapper_conn_for`、`mapper_mandatory_conn_for` 与
`mapper_never_conn_for` 固定为 `mapper`。`conn_for_with_purpose` 允许框架明确声明迁移或探测用途。
Pool acquire 与 ambient transaction 连接槽等待分别记录次数、固定桶延迟与在途数量；取消和正常
完成只统计一次。事务门禁、跨库、跨后端和未知 datasource 被归类为执行前拒绝。

`observability::ConnectionMetricsSource` 与 `pool_metrics_source` 在抓取时提供结构化快照，
Prometheus 与 OTLP 使用同一份指标合同。Pool 使用数是 total 与 idle 的饱和差值，属于近似瞬时状态。
直接取得原始 Pool 后调用 `acquire()` 不在连接观测覆盖范围内。

等待日志默认关闭；超时通知只提交给启动期冻结的有界队列，不执行业务回调或网络请求。
通知拥塞、停机和 provider 失败不会改变数据库连接结果或事务裁决。等待策略与通知路由变更要求重启。
Pool acquire、事务槽等待与 Mapper 慢 SQL 使用独立阈值，不能用等待时间触发慢 SQL 通知。
`alerts.acquire_timeout` 默认只选 `mapper` 用途；其它用途需显式声明。业务安装 `Notify` 后由 worker
调用适配器，未安装则忽略。完整配置见
[SQL 与连接观测](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/namapper-core/README.md#sql-观测与配置)。

受管 `sql.observability.console` 通过 `build_pool_with_logging`、`probe_with_logging` 以及相应的
`*_in_schema_with_logging` 入口设置 SQLx ConnectOptions。关闭时不产生逐条语句日志；开启时关闭
SQLx 独立慢语句升级，保留 Mapper 的业务慢阈值。SQLx statement 日志本身不打印 bind 参数。
原有不带 logging 的入口保持 SQLx 默认行为，schema 门禁与服务端默认 `search_path` 语义不变。

## 事务裁决与失败语义

- `run` / `run_for` 保持 `anyhow::Result` 便利语义；`run_decided_for_checked` 分离结构化 datasource
  lookup 与事务执行错误。
- 嵌套调用只复用同 driver、同 datasource 的外层事务。跨 driver 和跨 datasource 在业务闭包及 SQL
  执行前拒绝。
- `mandatory_conn` 不会在事务上下文缺失时降级为 autocommit；同一事务内已有 `PgConn` 尚未释放时，
  再次取连接立即返回 `tx_connection_busy`，不会等待当前作用域释放自身持有的连接。先结束已有
  `PgConn` 的作用域再取连接，后续操作仍复用同一物理事务；该拒绝不计入 Pool acquire 或数据库执行。
- 内层回滚会把最外层事务置为 rollback-only。after-commit hook 只在数据库明确确认最外层提交后执行。
- 数据库明确拒绝 COMMIT 与结果未知分开返回；结果未知不会自动重放业务闭包，也不会运行 hook。
- `#[transactional_pgsql(never)]`(运行时入口 `run_never`)拒绝任一数据库 driver 的 ambient 事务且不开启事务——供
  "绝不能被外层事务包住"的路径把违规调用变成进入前的显式错误；`never` 与 `datasource` 互斥。
- 提交/回滚前对事务连接槽做 fail-fast 独占：业务体已返回却仍被持有的连接句柄(未消费完的事务内
  `MapperStream`、被移出事务体的 `PgConn`)会让事务以 "transaction connection is still held at
  commit" 显式失败，而不是在槽锁上永久卡死。
- `classify_sqlstate` 提供 `23505`、`40001`、`40P01`、`55P03` 和 SQLSTATE class `08` 的稳定分类，
  不解析数据库错误正文。

## Application 接入

`"db"` 组件从 `datasources.<name>` 的 `driver: postgresql` 与连接配置建立来源；业务通过
`app.pg_datasource(name).await` 取得池，通过同名 ambient 事务复用连接。Application 的 Batch 使用
`MIGRATION_PLANS` 静态工厂在工作负载前执行迁移，持久 adapter 随后校验 schema。

`conn_for_budget` 对 pool acquire 应用剩余绝对预算，`read_with_budget` 仅供调用方已确认无副作用
的读取使用；不按 SQL 前缀猜测只读，不自动包装事务写、COMMIT 或重放未知结果。
`after_commit` 只在确认提交后执行进程内尽力回调，不提供持久补偿。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
