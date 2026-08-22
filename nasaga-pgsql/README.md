# nasaga-pgsql

`nasaga-pgsql` 是 NASA Saga 的 PostgreSQL 持久后端，实现 `nasaga-backend`
分组能力，与 `nainbox-pgsql`、`naoutbox-pgsql` 和 `natx-pgsql` 共享同一
datasource 与 ambient transaction。

它保存 Saga 实例、attempt、transition、参与方 gate、durable timer、租户治理和
审计事实。状态裁决不在本 crate 复制，由 `nasaga-runtime-core` 统一执行。

## 事务与并发边界

- 除 timer 领取与交还外，所有写入要求已存在同 datasource PostgreSQL 事务。
- timer 租约使用 owner、过期时刻与不可复制 fencing token；失权 worker 的条件写会被拒绝。
- 并发 timer 领取使用 `FOR UPDATE SKIP LOCKED`，不依赖 session advisory lock。
- SQLSTATE `23505` 只在已声明的业务唯一约束处转换为幂等或冲突；其它约束失败保留为基础设施错误。
- 本后端不提供跨 datasource、跨 driver 或 XA 事务。

## 初始化与配置

```toml
[dependencies]
nasaga-pgsql = "1"
natx-pgsql = "1"
```

```rust
let store = nasaga_pgsql::PgSagaStore::with_datasource("orders")?;
```

schema 应在 Application Prepare 阶段通过 migration 管理：Orchestrator 执行
[`create_saga.sql`](migrations/create_saga.sql)，参与方执行
[`create_saga_participant.sql`](migrations/create_saga_participant.sql)。
`PgSagaStore::ensure_schema` 与 `PgSagaStore::ensure_participant_schema` 读取相同文件，
仅用于 standalone 引导，不会创建连接池或改变 datasource 所有权。

本 crate 不读取 YAML。业务经门面使用时开启 `saga-runtime-pgsql`，从 `nasa::saga::pgsql` 使用运行时
包装；Application 依据 Saga 计划中的 `datasource_ref` 复验 driver，并拥有 pool、timer 与停机顺序。

## 观测

`PgSagaStore::load_operational_metrics(now_ms)` 从已提交表聚合实例终态、Unknown、人工介入、重试、冲突、
当前运行状态、到期 timer 与生命周期耗时。结果只含低基数总量，不使用 saga id、tenant、step 或
datasource endpoint 作为 label；数据库查询失败保持失败，不能用进程内计数伪装持久事实。
