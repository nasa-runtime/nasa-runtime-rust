# SQLite 接入与运行边界

业务通过 `nasa::mapper::sqlite` 使用 `#[Mapper]`、`#[Query]`、`#[Insert]` 等 SQL 宏；同一个 napp `"db"` 组件管理文件 owner、连接、迁移、事务观测和停机。每个 datasource 独立声明 driver，可以与 MySQL、PostgreSQL 混配，业务不需要另建 SQLx pool 或关闭任务。

## 后端选择与运行架构

| 数据源 | Cargo feature | Mapper 导入 | 资源配置 | Application getter |
| --- | --- | --- | --- | --- |
| MySQL | `application,mapper` | `nasa::mapper::Mapper` | `driver: mysql`、`url` | `datasource(name)` |
| PostgreSQL | `application,mapper-pgsql` | `nasa::mapper::pgsql::Mapper` | `driver: postgresql`、`url` | `pg_datasource(name)` |
| SQLite | `application,mapper-sqlite` | `nasa::mapper::sqlite::Mapper` | `driver: sqlite`、`sqlite.path` | `sqlite_datasource(name)` |

Mapper feature 已包含对应 transaction feature。只用低层事务时可改选 `tx`、`tx-pgsql`、`tx-sqlite`。配置决定资源后端，宏导入路径决定编译类型，二者必须匹配。SQLite-only 组合不引入 MySQL/PostgreSQL runtime。

```text
最终 YAML → 完整 driver、能力与引用校验 → 每源 owner 和统一 catalog
  → Prepare 执行显式迁移计划 → 初始化与业务开放
  → Mapper client 固定来源 → 绑定与权限检查 → 同源原生执行
  → 关闭新根准入 → 限时排空 → 真实原生关闭 → 释放文件权威
```

`database` 是单源形式，发布为 `default`；与 `datasources` 互斥。每个名称只能绑定一种 driver。未知名称、错 driver 和未编入能力会被拒绝，默认 getter 不会猜选唯一来源。SQLite 要求显式 driver，不从 URI 或文件后缀推断。

## 可运行的 Batch 应用

下面使用私有工作目录中的文件数据库。应用启动时先应用显式迁移，再通过 Mapper 完成同源事务；重复运行会保留已有数据。

`Cargo.toml`：

```toml
[package]
name = "sqlite-records"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1"
linkme = "0.3"
tokio = { version = "1", features = ["full"] }
nasa = { version = "2.0.6", default-features = false, features = ["application", "mapper-sqlite"] }
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "sqlite-bundled", "migrate"] }
```

SQLx 用于构造嵌入迁移计划；业务 SQL 仍由 Mapper 宏执行。`migrations/create_records.sql`：

```sql
CREATE TABLE records (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
```

`zcf/application.yml`：

```yaml
application:
  name: sqlite-records
  mode: batch
  startup_timeout_ms: 10000
  shutdown_timeout_ms: 5000
datasources:
  local:
    driver: sqlite
    sqlite:
      path: ./data/local.db
      create_if_missing: true
      create_parent_directories: true
      journal_mode: wal
      synchronous: full
    max_connections: 4
    acquire_timeout_ms: 3000
    operation_timeout_ms: 30000
    transaction_timeout_ms: 30000
    migrations:
      mode: apply
      lock_timeout_ms: 5000
      execution_timeout_ms: 30000
```

`src/main.rs`：

```rust
use nasa::mapper::sqlite::Mapper;
use nasa::tx::sqlite::transactional;
use sqlx::migrate::{Migration, MigrationType, Migrator};
use sqlx::SqlSafeStr;

/// 业务作用：为 local 来源声明完整嵌入计划，确保工作负载执行前已具备业务表。
/// 参数说明：无。
/// 返回：不执行 I/O 的迁移计划，交由 Application Prepare 应用。
fn migration_plans() -> Vec<(String, Migrator)> {
    let migration = Migration::new(
        1,
        "create records".into(),
        MigrationType::Simple,
        sqlx::AssertSqlSafe(include_str!("../migrations/create_records.sql")).into_sql_str(),
        false,
    );
    vec![("local".into(), Migrator::with_migrations(vec![migration]))]
}

#[linkme::distributed_slice(nasa::application::MIGRATION_PLANS)]
static MIGRATIONS: nasa::application::MigrationPlanFactory = migration_plans;

#[Mapper(datasource = "local", cache = false)]
trait Records {
    /// 业务作用：追加一条记录，由调用方选择独立提交或加入同源事务。
    /// 参数说明：
    /// - `name`：记录名称，作为参数绑定。
    /// 返回：成功影响行数；约束或数据库错误时失败。
    #[Insert("INSERT INTO records(name) VALUES(#{name})")]
    async fn insert(&self, name: &str) -> anyhow::Result<u64>;

    /// 业务作用：读取当前可见的记录总数。
    /// 参数说明：无。
    /// 返回：同源查询的记录数；来源或查询失败时返回错误。
    #[Query("SELECT count(*) FROM records", fetch = "scalar")]
    async fn count(&self) -> anyhow::Result<i64>;
}

/// 业务作用：在同一 Immediate 事务内追加记录并读取更新后的总数。
/// 参数说明：
/// - `records`：固定 local 来源的 Mapper client。
/// 返回：提交确认后的记录总数；任何数据库失败都会阻止成功返回。
#[transactional(datasource = "local", mode = "immediate")]
async fn append_record(records: &RecordsClient) -> anyhow::Result<i64> {
    records.insert("first record").await?;
    records.count().await
}

/// 业务作用：在受管迁移完成后执行业务，退出时由 Application 关闭数据库。
/// 参数说明：
/// - `app`：已完成 Batch 准备的应用句柄。
/// 返回：业务提交并输出总数后成功；初始化或业务失败时非正常退出。
#[nasa::application("db")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let records = RecordsClient::new().with_datasource(app.sqlite_datasource("local").await?)?;
    let total = append_record(&records).await?;
    println!("records={total}");
    Ok(())
}
```

从应用根目录执行 `cargo run`。首次输出 `records=1`，再次执行输出 `records=2`。`data` 目录可由配置创建为私有目录；已有目录不得允许其它账号写入，主文件、锁文件与 sidecar 必须属于运行账号且无 group/other 权限。

## Service 与迁移门禁

Service 可以使用相同静态计划，也可在 UserHook 调用 `app.configure_migrations("local", migrator)`；不要同时为同一来源登记两份计划。Service 的 UserHook 早于 Prepare，业务 SQL 应放入后续 initializer 或通过 `serve_when_ready` 开放的工作中。Batch 的工作负载晚于 Prepare，必须使用静态计划，不能在 main 中补登记迁移。

YAML 仅定义策略，不自动发现 SQL。显式配置非 disabled 的 SQLite migrations 必须有完整计划，无迁移时登记显式空计划。`validate` 在只读快照中核对历史；`apply` 将全部待应用脚本与历史写入放在同一个 `BEGIN IMMEDIATE` 事务；`disabled` 跳过历史门禁但仍验证计划本身。脚本使用语义文件名，持久 ID 通过 `Migration::new` 声明。SQL/checksum 漂移、未知历史或缺洞均拒绝，不能通过修改旧脚本覆盖历史。

## 事务、预算与观测

本地事务只绑定一种 driver、一个 datasource 和实际 owner。task-local 不跨 `spawn` 传播，必须加入事务的 Mapper 写方法可声明 `tx = "mandatory"`。嵌套取消、失败或 panic 不赋予外层继续提交的权利。`after_commit` 只在确认提交并释放事务借用后运行，最多 256 项共享 5 秒期限，不能代替持久消息投递。

普通独立借用的期限由 `operation_timeout_ms` 固定；`conn_for_budget` 在接纳前取配置与请求绝对期限的较短者，选池等待、首次 SQL 前的持有和后续执行都消耗同一期限。环境事务保留 `transaction_timeout_ms` 总期限，局部句柄预算不改写它。`read_with_budget` 约束完整读取取消；取消单次读取不意味着未到期租约必须失效。

流受到总行数、总字节数、总期限与消费空闲期限限制。业务必须在事务体退出前释放连接和流。预算或停机超时不保证阻塞系统调用立即退出；真实原生关闭前仍占容量并保活文件 owner。提交未确认与明确回滚不同，不能自动重放写入。

Application 自动安装 `sql.observability` 的方法、执行、流和连接观测。SQLite 的错误、维护、备份、隔离、WAL 与空间压力通过固定指标和 readiness 表达；参数默认不输出。持续隔离不会因一次探针成功解除，需停止并重新创建来源。数据源配置变化要求重启，旧 client 不能自动切到同名新 owner。

## 文件部署与恢复

仅支持 Linux/macOS 私有本地文件系统。默认 WAL/FULL；DELETE 默认 EXTRA，已有文件不能在普通建连时隐式改变日志模式。挂载整个私有数据库目录，禁止多实例共写、网络共享文件、硬链接别名或绕过 owner 协议的工具写入。只读配置仍可能需要 owner 锁或 SQLite 协调文件。

`source.maintenance().inspect(&budget)` 在结构和外键检查通过后返回持久 `user_version`，可用于已有数据库的启动门禁；它不开放任意 PRAGMA。

同时管理外部对象目录的宿主可通过 `source.database_path()` 返回的固定绝对路径核对元数据文件位置；该路径已解析父目录，不随相对配置或后续工作目录变化重新解释。用 `source.retain_file_guard(Arc<T>)` 可将独占守卫绑定到实际文件 owner。每个 owner 只接纳一次，守卫不能引用该 datasource；只有原生连接全部退出且文件权威释放后才解除保活，异步取消不能提前交接对象目录。

通过 `source.maintenance()` 做有界 checkpoint 和原生一致备份。备份输出经过校验的独立 DELETE 文件；不能通过只拷贝活动主文件替代一致备份，也不能删除活动 WAL。`restore_to_new` 只写不存在的新目标，重新启动时验证迁移和业务状态。发布同步失败时可能已有目标，必须先检查事实，不能覆盖重试。

SQLite 不提供 Saga、Inbox、Outbox、事务审计或幂等持久 adapter，不提供跨库原子提交、SQL 方言转换、网络文件库或设备断电保证。它不会备份外部对象文件和密钥。完整配置与约束见 [数据源与文件权威](../natx-sqlite/README.md)、[Mapper](../namapper-sqlite/README.md)、[迁移](../namigrate-sqlite/README.md) 和 [Application](../napp/README.md#sqlite-与逐数据源-driver)。
