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

启动时必须先注入连接池。`#[application]` 运行时下声明 `db` 组件即可跳过手工注入：组件按 `database` / `datasources.<name>` 配置先用单连接探测真实连通性、再建池，并同时写入本运行时与应用资源容器。

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

命名 datasource 注册签名已放开为 `try_init_datasource(impl Into<String>, pool)`：运行期拼出的名称也能注册，原 `&'static str` 调用不受影响；`pool_for_datasource(&str)` 同步放开。`DataSourceConfig` 的 `Debug` 输出对连接串脱敏，可安全进日志。

关键规则：

- 想加入 `#[transactional]` 的访问必须用 `nasa::tx::conn()` 或
  `nasa::tx::mandatory_conn()` 取连接。
- 使用 `&self.pool` 会绕过 ambient 事务，写入不会随事务 rollback。
- 嵌套事务只支持同 datasource 复用外层事务，不支持 savepoint、独立子事务和跨 datasource 事务。
- `after_commit` 只在最外层事务 commit 成功后执行，适合缓存失效和提交后通知。

## 事务语义(实测)

- 嵌套 `run` 复用外层事务、一起提交;内层 body 返回 Err 会标记 **rollback-only**——即便外层吞掉错误返回 Ok,最外层提交前整体回滚并返回 `RollbackOnly` 错误(`err.downcast_ref::<nasa::tx::RollbackOnly>()` 可识别)。
- `after_commit` 仅事务内可注册(事务外返回 Err);提交成功后执行一次,回滚 / rollback-only 时全部丢弃。
- `mandatory_conn` 无事务直接 Err,不回退池连接。
- 事务内任一 SQL 执行错误使整个事务回滚;未注册 datasource 的 `run_for` / `pool_for_datasource` 返回 Err。
- `tokio::spawn` 出的任务不继承 ambient 事务;同一作用域不要同时持有两个 `Conn`(会互等池连接)。

## YML 配置与使用

受管 `db` 组件读取 `database:` 或 `datasources:`，两者互斥。手工装配时也建议复用相同投影，再构造
`sqlx::MySqlPool` 注入事务运行时。

单数据源示例：

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 16
  min_connections: 1
  acquire_timeout_ms: 3000
  idle_timeout_ms: 600000
  max_lifetime_ms: 1800000
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
| `idle_timeout_ms` | 空闲连接回收时间。 |
| `max_lifetime_ms` | 单连接最大生命周期。 |

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

多数据源需要按名称注册，mapper 和业务事务方法再用相同 datasource 名称：

```rust
nasa::tx::try_init(default_pool)?; // 默认数据源
nasa::tx::try_init_datasource("report", report_pool)?;
```

约束：所有需要参加事务的 DB 访问必须通过 `nasa::tx::conn()`、
`nasa::tx::mandatory_conn()` 或 mapper 生成代码获取连接。
