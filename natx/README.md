# natx

`natx` 提供基于 `tokio::task_local!` 的 ambient MySQL 事务。业务一般通过门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["tx"] }
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

关键规则：

- 想加入 `#[transactional]` 的访问必须用 `nasa::tx::conn()` 或
  `nasa::tx::mandatory_conn()` 取连接。
- 使用 `&self.pool` 会绕过 ambient 事务，写入不会随事务 rollback。
- 嵌套事务只支持同 datasource 复用外层事务，不支持 savepoint、独立子事务和跨 datasource 事务。
- 具名 store 或 Mapper 与 ambient datasource 不一致时会在 SQL 前失败，不会回落到 `default`。
- `after_commit` 只在最外层事务 commit 成功后执行，适合缓存失效和提交后通知。

## 事务语义

- 嵌套 `run` 复用外层事务、一起提交;内层 body 返回 Err 会标记 **rollback-only**——即便外层吞掉错误返回 Ok,最外层提交前整体回滚并返回 `RollbackOnly` 错误(`err.downcast_ref::<nasa::tx::RollbackOnly>()` 可识别)。
- `after_commit` 仅事务内可注册(事务外返回 Err);提交成功后执行一次,回滚 / rollback-only 时全部丢弃。
- `mandatory_conn` 无事务直接 Err,不回退池连接。
- 事务内任一 SQL 执行错误使整个事务回滚;未注册 datasource 的 `run_for` / `pool_for_datasource` 返回 Err。
- `tokio::spawn` 出的任务不继承 ambient 事务;同一作用域不要同时持有两个 `Conn`(会互等池连接)。

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
