# namigrate-pgsql

`namigrate-pgsql` 为 PostgreSQL 提供 `disabled`、`validate` 和 `apply` migration 门禁。锁、catalog
查询、执行与显式 unlock 始终位于同一物理 session；unlock 结果无法确认时，连接不会回到业务池。

## 初始化与使用

```toml
[dependencies]
namigrate-pgsql = "1.0.0"
sqlx = { version = "0.9", default-features = false, features = ["macros", "migrate", "postgres"] }
```

```rust
use namigrate_pgsql::{run_gate, MigrationMode, MigrationSettings, Migrator};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

let report = run_gate(
    &pool,
    "public",
    &MIGRATOR,
    &MigrationSettings {
        mode: MigrationMode::Validate,
        ..Default::default()
    },
)
.await?;
# let _ = report;
```

## 连接合同

### 连接观测与锁预算

池入口以 `purpose=migration` 记录真实 Pool acquire，取到连接后即结束计时；schema 身份查询、
advisory lock 竞争和 migration 执行不计入连接等待，也不作为 Mapper 方法计量。
`run_gate_with_evidence_for` 与 `verify_target_identity_for` 接受命名数据源身份；不带 `_for` 的
池入口使用 `default`。直接交入专有连接不会伪造一次 Pool acquire。

受管等待日志与通知由 `sql.observability` 配置。连接超时通知默认只选 `mapper`，需要迁移通知时
显式将 `migration` 加入 `alerts.acquire_timeout.purposes` 并安装业务 `Notify`。通知失败不改变
门禁结果，也不延长从获取连接开始计算的原始锁预算。

### Session 与失败后果

`run_gate` 只适用于 PostgreSQL 直连或保证 session affinity 的会话级代理。事务级池化代理可能让相邻
操作落到不同 backend，不能承载 session advisory lock。此时应通过直连或会话级 migration endpoint
建立专有 `PgConnection`，并调用消费该连接的 `run_gate_on_connection`。

lock key 由当前 database 与受管 schema 使用稳定算法生成。schema 仅接受 PostgreSQL 普通未引号
identifier 形状，并且必须在门禁执行前存在；缺失 schema 使用独立错误分类，不与 migration endpoint
身份不一致混淆。`lock_timeout_ms` 从获取连接开始约束锁取得阶段，不截断已经开始的 catalog 查询或
migration 执行；`0` 表示锁取得不设置外层截止时间。

普通 migration 保持事务性。标记为非事务的 migration（例如 `CREATE INDEX CONCURRENTLY`）必须通过
`NonTransactionalEvidence` 提供独立超时和完成探针；执行中断后只有探针确认目标状态完整，框架才补记
checksum，避免盲目重复外部副作用。独立超时发生时不再向同一 session 排队发送 unlock，而是关闭
物理连接释放会话锁；下次启动重新读取完成证据，不把未知状态连接放回业务池。

只支持 SQLx 默认的 `_sqlx_migrations` catalog 名称。业务 pool 与 migration endpoint 分离时，应先用
`verify_target_identity` 复验 database 与 schema，再把专有连接交给 `run_gate_on_connection`。

公开错误不包含 SQL 正文、schema 名称、endpoint 或凭据。

## Application 受管入口

通过 `nasa` 使用时开启 `application,tx-pgsql`，在 Service UserHook 调用
`app.configure_migrations(datasource, sqlx::migrate!("./migrations"))`。YAML 的
`migrations.mode` 与锁参数只定义策略，不会自动发现 SQL 文件；Application 在 initializer 与入站监听
之前执行门禁。同一数据源只能登记一次。Batch 模式的 DB Prepare 已先于业务 Hook 完成，应显式调用
`nasa::migration::pgsql::run_gate`。

PostgreSQL `connection_topology` 可选 `direct`、`session_pool` 或 `transaction_pool`。门禁模式不是
`disabled` 时，事务级业务代理必须另配保证 session affinity 的 `migrations.session_url`；Application
会在 advisory lock 前确认它与业务 pool 的 database/schema 身份一致。
