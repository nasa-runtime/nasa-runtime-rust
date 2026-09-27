# natx

`natx` 提供基于 `tokio::task_local!` 的 ambient MySQL 事务，并将连接池获取、事务连接槽取得和
执行前拒绝分别计量，帮助区分池饱和与事务内连接竞争。业务一般通过门面使用：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["tx"] }
```

```rust
use nasa::tx::{self, transactional};

#[transactional]
async fn create_order() -> anyhow::Result<()> {
    let mut conn = tx::mandatory_conn().await?;
    sqlx::query("INSERT INTO orders(id) VALUES (?)")
        .bind(1_i64)
        .execute(conn.as_mut())
        .await?;
    Ok(())
}
```

## 受管命名数据源架构

启动时必须先注入连接池。`#[application]` 运行时下声明 `db` 组件即可跳过手工注入：组件按 `database` /
`datasources.<name>` 配置先校验全部名称、逐项探测并建池，最后一次性发布由 Application 拥有的冻结
`DataSourceRegistry`。`Application::datasource(name)`、事务、Mapper 和持久 adapter 都解析这张表，不再
由 natx 的 standalone 静态表持有第二份受管 pool 所有权。

手工建池推荐用 `nasa::tx::datasource` 模块（db 组件走的同一实现）：

```rust
use nasa::tx::datasource::{build_pool, probe, DataSourceConfig};

let cfg: DataSourceConfig = serde_json::from_value(raw_database_section)?;
cfg.validate()?;
probe(&cfg).await?;            // 单连接探测:拿到 Access denied / Unknown database 等真实根因
let pool = build_pool(&cfg)?;  // 惰性池;`nasa::tx::MySqlPool` 已重导出供签名引用
nasa::tx::try_init(pool)?;
```

也可以直接用 sqlx 建池后注入：

```rust
let database_url = std::env::var("APP_MYSQL_URL")?;
let pool = sqlx::mysql::MySqlPoolOptions::new()
    .connect(&database_url)
    .await?;
nasa::tx::try_init(pool)?;
```

独立使用时，命名 datasource 注册入口是 `try_init_datasource(impl Into<String>, pool)`，
`pool_for_datasource(&str)` 按同名选择。standalone 注册与 Application 受管 registry 严格互斥；同一进程
不能同时维护两张 datasource 表。Saga 的 `database_bootstrap=user_hook` 是唯一接管边界：它把 standalone
默认池从静态槽移入 Application 拥有的单源 registry，停机封口后不保留可被后续实例读取的强引用。
如果 UserHook 在 Prepare 接管前失败，Application 的预压栈停机动作会先从 standalone 表撤销本轮注入，
再显式关闭这些池。
`DataSourceConfig` 的诊断格式会对连接串脱敏，可安全写入日志。

Application 的一次性业务停机任务先于数据库组件最终关闭，可以在任务执行时按 datasource 名称取得
连接并完成有界事务。任务必须在返回前归还连接与资源借用；不能把数据库 pool 的 close 再登记为业务
停机任务。共享停机期限不是提交确认，COMMIT 应答不确定仍按事务结果分类处理，不自动重放业务写。

关键规则：

- 想加入 `#[transactional]` 的访问必须用 `nasa::tx::conn()` 或
  `nasa::tx::mandatory_conn()` 取连接。
- 使用 `&self.pool` 会绕过 ambient 事务，写入不会随事务 rollback。
- 嵌套事务只支持同 datasource 复用外层事务，不支持 savepoint、独立子事务和跨 datasource 事务。
- 与 `natx-pgsql` 同时编入时，driver task-local 会在业务闭包及 SQL 执行前拒绝跨后端嵌套。
- 具名 store 或 Mapper 与 ambient datasource 不一致时会在 SQL 前失败，不会回落到 `default`。
- `after_commit` 只在最外层事务 commit 成功后执行，适合缓存失效和提交后通知。
- `run_decided_for_checked` 以 `TxEntryError` 分离结构化 datasource lookup 与原有 `TxRunError`；既有
  `run_decided_for` 签名和可穷举错误保持不变。
- `DatasourceRef::new` 继续返回 `anyhow::Result`；需要结构化名称错误的新代码使用
  `DatasourceRef::try_new`。

## 连接观测与语句日志

`conn_*` 保持原有事务选择与返回语义，连接用途记为 `direct`；Mapper 使用专用的
`mapper_conn_for`、`mapper_mandatory_conn_for`、`mapper_never_conn_for`，用途记为 `mapper`。
显式迁移、探测调用可使用 `conn_for_with_purpose` 声明用途。原始 Pool 的 `acquire()` 不经过这些
入口，因此不会自动产生连接等待指标。

Pool acquire 与 ambient transaction 连接槽取得分别记录 counter、固定桶耗时与 in-flight。
取消会收口在途计数且只记录一次；缺少强制事务、禁止事务、跨库、跨后端和未知 datasource 都作为
执行前拒绝，不计入数据库操作失败。同一事务已有 `Conn` 时再次取连接会立即返回错误，并记录
`tx_connection_busy`，不会等待当前任务释放自己持有的守卫。`PoolTimedOut` 只表示连接获取预算耗尽，
不能据此推断究竟是池排队还是反复握手失败。未知 datasource 共用固定回退身份，不按业务输入创建标签。

`observability::ConnectionMetricsSource` 与 `pool_metrics_source` 提供 MetricHub 结构化快照。
Pool total、idle、in_use 为近似瞬时状态，使用数以饱和减法计算；source 不创建周期采样线程。
等待日志默认关闭，开启后只在成功等待达到阈值时输出稳定字段；连接超时可向具体有界通知队列投递，
队列满、停机和渠道失败不会改写 SQL 返回或事务裁决。观测策略在启动期冻结，变更需要重启。
`acquire_wait.threshold_ms` 与 `transaction_slot_wait.threshold_ms` 控制各自的等待日志，
不使用 Mapper 的 `slow_sql.threshold_ms`。连接超时通知通过 `alerts.acquire_timeout` 启用，默认
只包含 `mapper` 用途；`migration/probe/direct` 需显式加入 `purposes`。业务须先安装 `Notify`，
没有实现时忽略。完整配置见
[SQL 与连接观测](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/namapper-core/README.md#sql-观测与配置)。

受管应用通过 `sql.observability.console.enabled` 控制逐条 SQL 日志；底层建池和探测适配使用
`datasource::build_pool_with_logging` 与 `probe_with_logging`。关闭时 SQLx 不产生语句事件；开启时
可选择语句日志级别，独立慢语句升级被关闭，慢操作统一由 Mapper 阈值裁决。SQLx statement 日志
本身不打印 bind 参数。原有 `build_pool`、`probe` 保留独立使用时的 SQLx 默认行为。

## 事务裁决

- 嵌套 `run` 复用外层事务、一起提交;内层 body 返回 Err 会标记 **rollback-only**——即便外层吞掉错误返回 Ok,最外层提交前整体回滚并返回 `RollbackOnly` 错误(`err.downcast_ref::<nasa::tx::RollbackOnly>()` 可识别)。
- `after_commit` 仅事务内可注册(事务外返回 Err);提交成功后执行一次,回滚 / rollback-only 时全部丢弃。
- `mandatory_conn` 无事务直接 Err,不回退池连接。
- 事务内任一 SQL 执行错误使整个事务回滚;未注册 datasource 的 `run_for` / `pool_for_datasource` 返回 Err。
- `tokio::spawn` 出的任务不继承 ambient 事务。同一作用域不要同时持有两个 `Conn`：已有 `Conn` 尚未
  离开作用域时，内层再次取连接会立即返回 `tx_connection_busy`；先用代码块结束已有 `Conn` 的作用域，
  内层调用即可继续复用同一个物理事务。
- `#[transactional(never)]`(运行时入口 `run_never`):拒绝任一数据库 driver 的 ambient 事务且不开启事务——供"绝不能
  被外层事务包住"的路径(自治审计写、对外即时可见的副作用)把违规调用变成进入前的显式错误,
  而不是静默入伙随外层回滚;`never` 与 `datasource` 互斥。
- 提交/回滚前对事务连接槽做 fail-fast 独占:业务体已返回却仍被持有的连接句柄(未消费完的事务内
  `MapperStream`、被移出事务体的 `Conn`)会让事务以 "transaction connection is still held at
  commit" 显式失败,而不是在槽锁上永久卡死。

持有连接调用内层事务方法会立即返回错误：

```rust,ignore
#[transactional]
async fn outer() -> anyhow::Result<()> {
    let mut conn = natx::conn().await?;
    sqlx::query("UPDATE account SET status = ? WHERE id = ?")
        .bind("active")
        .bind(7_i64)
        .execute(conn.as_mut())
        .await?;

    inner().await?; // inner 再取连接时返回连接占用错误
    Ok(())
}
```

先结束连接作用域，内层方法会继续复用同一个物理事务：

```rust,ignore
#[transactional]
async fn outer() -> anyhow::Result<()> {
    {
        let mut conn = natx::conn().await?;
        sqlx::query("UPDATE account SET status = ? WHERE id = ?")
            .bind("active")
            .bind(7_i64)
            .execute(conn.as_mut())
            .await?;
    }

    inner().await?;
    Ok(())
}
```

## YML 配置与使用

受管 `db` 组件读取 `database:` 或 `datasources:`，两者互斥。`database` 固定映射为 `default`；
`datasources` 可以不含默认库，但此时无参事务、无参 store 与 `default_datasource()` 都会明确失败。
名称只能包含 ASCII 字母、数字、`.`、`_`、`-`，单进程最多受管 64 个实例。

单数据源示例：

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 16
  min_connections: 1
  acquire_timeout_ms: 3000
  connect_timeout_ms: 5000
  probe_on_start: true
```

多数据源示例：

```yaml
datasources:
  default:
    url: ${APP_MYSQL_URL}
    max_connections: 16
  report:
    url: ${APP_REPORT_MYSQL_URL}
    max_connections: 8
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `url` | MySQL 连接串。 |
| `max_connections` | pool 最大连接数。 |
| `min_connections` | pool 最小连接数。 |
| `acquire_timeout_ms` | 获取连接超时。 |
| `connect_timeout_ms` | 建立单条连接及启动探测的超时。 |
| `probe_on_start` | 是否在建池前探测地址、鉴权和目标数据库。 |

启动代码：

```rust
let pool = sqlx::mysql::MySqlPoolOptions::new()
    .max_connections(cfg.mysql.max_connections)
    .min_connections(cfg.mysql.min_connections)
    .acquire_timeout(std::time::Duration::from_millis(cfg.mysql.acquire_timeout_ms))
    .connect(&cfg.mysql.url)
    .await?;

nasa::tx::try_init(pool)?;
```

不使用 Application 的 standalone 模式需要按名称注册，Mapper 和业务事务方法再用相同 datasource 名称：

```rust
nasa::tx::try_init(default_pool)?; // 默认数据源
nasa::tx::try_init_datasource("report", report_pool)?;
```

Application 模式不调用这些注册函数；业务通过 `app.default_datasource().await?` 或
`app.datasource("report").await?` 取得已经发布的 pool。运行期增删、改名或更换连接配置需要重启；
本 crate 只保证单 datasource 的本地事务，不提供跨库原子提交。

约束：所有需要参加事务的 DB 访问必须通过 `nasa::tx::conn()` / `conn_for()`、
`nasa::tx::mandatory_conn()` / `mandatory_conn_for()` 或 Mapper 生成代码获取连接。

## Application 接入

conn_for_budget 对 pool acquire 应用剩余绝对预算，read_with_budget 只供调用方已确认无副作用的读取使用；不按 SQL 前缀猜测只读，不自动包装事务写、COMMIT 或重放未知结果。Application 的 Batch 通过 MIGRATION_PLANS 静态工厂在工作负载前执行迁移，Service 也可在 UserHook 登记；持久 adapter 在迁移与 schema 校验之后才能使用。after_commit 只在确认提交后执行进程内尽力回调，不提供持久补偿。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
