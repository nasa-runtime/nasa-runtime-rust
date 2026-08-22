# nasaga-runtime-pgsql

`nasaga-runtime-pgsql` 将 PostgreSQL Saga store、Inbox、Outbox 与 ambient transaction
绑定到同一命名 datasource，并把它们交给 `nasaga-runtime-core` 中唯一的 Orchestrator、Participant、
timer、补偿、恢复和 fencing 状态机。

## 核心价值

分布式步骤在进程崩溃、至少一次重投和多副本竞争下，从 PostgreSQL 已提交事实继续推进；无法确定的
外部效果进入类型化裁决或人工介入，不猜测成功或失败。该运行时提供最终一致性，不提供跨服务 ACID、
物理 exactly-once、跨 datasource 原子提交或并发 Saga 隔离。

## 初始化

直接依赖包装层：

```toml
[dependencies]
nasaga-runtime-pgsql = "1"
```

同一后端中的 Saga store、Inbox 和 Outbox 必须使用同一个 PostgreSQL datasource。宿主应在开放业务
路由前完成 schema、definition descriptor 与非终态实例摘要复验。

```rust
use nasaga_runtime_pgsql::{DefinitionRegistry, OrchestratorConfig, PgOrchestrator};

let registry = DefinitionRegistry::new();
let orchestrator = PgOrchestrator::with_datasource(
    registry,
    OrchestratorConfig::default(),
    "workflow",
)?;
orchestrator.verify_startup().await?;
# Ok::<(), anyhow::Error>(())
```

通过 `nasa` 使用时开启 `saga-runtime-pgsql`，业务入口仍声明同一个 `"saga"` 组件，并用
`SagaApplicationPlan::pgsql_orchestrator` 或 `SagaApplicationPlan::pgsql_participant` 提交角色。组件根据
计划的 datasource 在统一 catalog 中复验 PostgreSQL driver，Ready 前校验 schema、definition 与历史
非终态实例，随后监督 durable timer；停机先关闭角色能力，再排空 transport 与 Outbox，最后由 DB 组件
关闭 pool。

Kafka、Redis Streams 与 gRPC 分别由 `kafka`、`redis-stream`、`grpc-transport` feature 转发到共享核心。
门面对应 `saga-kafka-pgsql`、`saga-redis-stream-pgsql` 与 `saga-grpc-pgsql`。混合 Application 同时启用
MySQL 与 PostgreSQL transport 时仍只有一套 envelope、ACK/DLT 裁决和进程指标，不按数据库复制协议。

一个事务只能使用一种 driver 和一个 datasource。跨 driver 的耐久写入必须通过源库 Outbox 与目标库
Inbox 收敛，不能把 after-commit hook 当成可靠双写。

## 观测

运行核心提供唯一的 transport、治理与处理计数；`PgSagaStore::load_operational_metrics` 提供数据库已提交
的实例、attempt、timer、冲突和配额聚合。Application 会把两类来源组合进统一指标目录，混合 MySQL 与
PostgreSQL 时不会按 driver 重复登记相同进程指标。高基数业务身份、消息正文与 datasource endpoint
不进入指标 label。
