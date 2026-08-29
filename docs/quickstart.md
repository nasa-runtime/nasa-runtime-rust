# 快速开始

业务应用只依赖 `nasa` 门面，并按实际运行能力启用 feature。服务型项目使用
`#[nasa::application]` 统一拥有配置、组件生命周期、信号和停机顺序。

## 开始前的边界

服务型项目优先使用 `nasa` 门面和 Application，让最终 YAML、资源探测、Ready 与反向停机形成一个
生命周期。只需要算法、协议 codec 或显式连接管理的库型项目可以直接装配对应组件，但必须自行负责
初始化失败、观测和资源释放。Application 管理多种 datasource，不提供跨 driver 原子提交；跨库流程
应组合源库 Outbox、目标库 Inbox 和稳定事件标识。

## Cargo 依赖

以下组合提供配置装载、日志、MySQL 事务、Mapper、Redis、两级缓存和 Web：

```toml
[dependencies]
nasa = { version = "1.0.3", features = [
    "application",
    "config-boot",
    "log",
    "tx",
    "mapper",
    "mapper-redis-cache",
    "redis",
    "cache",
    "web",
] }
```

PostgreSQL 服务把 `tx`、`mapper` 换成 `tx-pgsql`、`mapper-pgsql`；需要同时访问两种数据库时可同时
开启两组 feature，Application 会按每个 datasource 的 `driver` 建立 typed pool，但不提供跨 driver
原子事务：

```toml
[dependencies]
nasa = { version = "1.0.3", features = [
    "application", "config-boot", "log", "tx-pgsql", "mapper-pgsql", "web",
] }
```

使用仓库坐标时只替换依赖来源，feature 保持一致：

```toml
[dependencies]
nasa = { git = "https://github.com/nasa-runtime/nasa-runtime-rust.git", features = [
    "application", "config-boot", "log", "tx", "mapper",
    "mapper-redis-cache", "redis", "cache", "web",
] }
```

## 应用入口

```rust
mod controller;

#[nasa::application("log", "db", "redis", "cache", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register(OrderService::new(app.datasource("default").await?))?;
    Ok(())
}
```

组件字符串可按任意顺序书写，宏会按规范顺序启动并严格反向停机。需要 Saga 时只声明
`#[nasa::application("saga", "web")]`；Saga 会隐式加入 DB 与 Outbox，transport 仍由业务显式选择。

## Saga 最小接线

Orchestrator 服务启用 Application，并按 Saga 所在 datasource 的 driver 选择 MySQL
`saga-runtime` 或 PostgreSQL `saga-runtime-pgsql`。Kafka、Redis Streams、HTTP 或 gRPC 按真实链路
另选，不能只配置地址就假定已经具备消费、确认和 DLT 闭环。以下是 MySQL 接线：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "saga-runtime", "web"] }
```

```rust
use std::sync::Arc;
use nasa::application::SagaApplicationPlan;
use nasa::saga::{DefinitionRegistry, Orchestrator, OrchestratorConfig};

#[nasa::application("saga", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let mut definitions = DefinitionRegistry::new();
    definitions.register(checkout_definition()?)?;

    let orchestrator = Arc::new(Orchestrator::new(
        definitions,
        OrchestratorConfig::default(),
    )?);
    let publisher = Arc::new(build_event_publisher(&app).await?);

    app.configure_saga(
        SagaApplicationPlan::orchestrator(orchestrator, "checkout-orchestrator-a")?
            .with_event_publisher(publisher)?,
    )?;
    Ok(())
}
```

PostgreSQL 使用相同状态机合同，只替换后端入口和受管计划构造：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "saga-runtime-pgsql", "web"] }
```

```rust
use std::sync::Arc;
use nasa::application::SagaApplicationPlan;
use nasa::saga::pgsql::{DefinitionRegistry, OrchestratorConfig, PgOrchestrator};

#[nasa::application("saga", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let mut definitions = DefinitionRegistry::new();
    definitions.register(checkout_definition()?)?;

    let orchestrator = Arc::new(PgOrchestrator::new(
        definitions,
        OrchestratorConfig::default(),
    )?);
    let publisher = Arc::new(build_event_publisher(&app).await?);

    app.configure_saga(
        SagaApplicationPlan::pgsql_orchestrator(orchestrator, "checkout-orchestrator-a")?
            .with_event_publisher(publisher)?,
    )?;
    Ok(())
}
```

`timer_owner` 必须逐副本唯一且重启稳定。纯参与方使用
`SagaApplicationPlan::participant(name, runtime)`；同一进程同时承载 Orchestrator 与参与方时用
`with_participant` 追加。发布端必须实现 `OutboxPublisher` 并且只在下游已经明确确认后返回成功。

启动前必须按选定后端准备每个本地事务域：MySQL 执行
[Saga 迁移顺序](../nasaga-mysql/migrations/README.md) 与
[Outbox 迁移顺序](../naoutbox-mysql/migrations/README.md)；PostgreSQL 的 Orchestrator 执行
[Saga 结构](../nasaga-pgsql/migrations/create_saga.sql)，参与方执行
[Saga gate 结构](../nasaga-pgsql/migrations/create_saga_participant.sql)，两侧按需执行
[Outbox 结构](../naoutbox-pgsql/migrations/create_outbox.sql)。Application 会在 Ready 前校验定义、
descriptor、历史非终态实例、数据库结构、发布端和参与方信任；任何一项不完整都拒绝开放监听或消费。

| transport | 需要的门面 feature | Application 声明 | 额外责任 |
| --- | --- | --- | --- |
| Kafka | MySQL `saga-kafka`；PostgreSQL `saga-kafka-pgsql` | 增加 `"kafka"` | topic owner、consumer group、ACL、DLT 与 broker 容量 |
| Redis Streams | MySQL `saga-redis-stream`；PostgreSQL `saga-redis-stream-pgsql` | 增加 `"redis"` | group、consumer 身份、HMAC/独占写 ACL、PEL 与同槽 DLT key |
| HTTP | MySQL `saga-runtime`；PostgreSQL `saga-runtime-pgsql` | 按宿主 listener | mTLS/HMAC、共享 nonce claim、路由、重试与 durable DLT |
| gRPC | MySQL `saga-grpc`；PostgreSQL `saga-grpc-pgsql` | 入站增加 Application `"grpc"`；纯出站 client 不声明组件 | 框架 generated service/client、mTLS principal、deadline、资源上限、封闭收据与 drain |

gRPC 入站不手工创建 tonic Router、generated server 或 `Arc` handler。单参与方在计划上调用
`with_grpc_command_service(service, peer_principal)`，Application 从 Participant runtime 的冻结信任投影
取得 producer；Orchestrator 调用 `with_grpc_result_service(producer, peer_principal)`。框架 service 自动
进入唯一受管 listener。纯出站发布端使用 `nasa::saga::grpc_proto` 的 generated client，并自行拥有
channel、deadline、重试和 Outbox 收据裁决。

## 配置

在业务进程工作目录提供 `zcf/application.yml`：

```yaml
application:
  name: order-service
  mode: service
  shutdown_timeout_ms: 15000

log:
  level: info
  path: logs/order-service

database:
  url: ${APP_MYSQL_URL}
  max_connections: 16

redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2

cache:
  mode: two_level
  redis_ref: default
  cache_ttl_secs: 300
  null_ttl_secs: 30

server:
  host: 0.0.0.0
  port: 8080
  context_path: /orders
  http2:
    enabled: false
```

PostgreSQL 或混合数据源改用带 driver 的命名配置；业务分别通过 `app.datasource("orders")` 与
`app.pg_datasource("reporting")` 取得 typed pool：

```yaml
datasources:
  orders:
    driver: mysql
    url: ${APP_MYSQL_URL}
  reporting:
    driver: postgresql
    url: ${APP_POSTGRES_URL}
    schema: reporting
    migrations:
      mode: validate
```

显式配置 `schema` 时，它同时约束业务连接的 `search_path` 与受管 migration，且需要在部署数据库时
预先创建；框架会在连接握手后复验 `current_schema()`。省略 `schema` 时，业务连接保留 PostgreSQL
服务端默认 `search_path`（通常为 `"$user", public`），受管 migration 默认使用 `public`。需要业务 SQL
与 migration 始终落在同一非默认 schema 时应显式配置。

YAML 只定义门禁策略，业务 migration 仍需在 Service 启动 Hook 中登记；业务 manifest 同时直依赖启用
PostgreSQL 与 `migrate` 的 `sqlx`：

```toml
[dependencies]
sqlx = { version = "0.9", default-features = false, features = ["macros", "migrate", "postgres"] }
```

```rust
#[nasa::application("db", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_migrations("reporting", sqlx::migrate!("./migrations"))?;
    Ok(())
}
```

门禁会在 initializer 与 listener 之前执行；同一数据源只能登记一次。门禁模式不是 `disabled` 时，事务级代理应声明
`connection_topology: transaction_pool`，并在 `migrations.session_url` 提供指向同一 database/schema
的直连或会话级 endpoint，Application 会在取 advisory lock 前复验目标身份。Batch 模式应在 Hook 内
显式运行 `nasa::migration::pgsql::run_gate`，不使用 `configure_migrations`。

配置默认拒绝未知字段。数据库、Redis 和其它外部凭据通过环境变量或部署平台 secret 注入，不写入仓库。
本示例已经启用 `application,web` 并声明 `"web"` 组件，因此受管 Web listener 默认只接受 HTTP/1；
业务需要 h2c prior knowledge 时只需把 `server.http2.enabled` 改为 `true`，高级 transport 字段可以
省略。该开关不提供 TLS 终止或 `Upgrade: h2c`。

## Mapper 与事务

以下是 MySQL 根入口：

```rust
use nasa::mapper::{Mapper, Query};
use nasa::tx::transactional;

#[derive(sqlx::FromRow)]
pub struct OrderRow {
    pub id: i64,
    pub state: String,
}

#[Mapper]
pub trait OrderMapper {
    #[Query("select id, state from orders where id = #{id}")]
    async fn find_by_id(&self, id: i64) -> sqlx::Result<Option<OrderRow>>;
}

#[transactional]
async fn create_order() -> anyhow::Result<()> {
    insert_order().await?;
    Ok(())
}
```

参与事务的写入必须使用当前 ambient datasource。需要可靠发布外部事件时使用 Outbox；消费消息并与
本地业务写共同提交时使用 Inbox。

PostgreSQL 使用 `nasa::mapper::pgsql::{Mapper, Query}` 与
`nasa::tx::pgsql::transactional`。Mapper 生成 `$1..$n` bind；两种 ambient transaction 不能嵌套，
同一业务原子链中的 Inbox、业务事实和 Outbox 必须使用同一 driver 与 datasource。

## 路由

```rust
use nasa::web::get_mapping;

#[get_mapping("/health")]
async fn health() -> &'static str {
    "ok"
}
```

声明 `web` 后运行时提供统一监听、readiness、排空和停机。业务路由、数据源和后台任务应从
`nasa::Application` 取得受管能力，不自行复制生命周期。

## 后续阅读

| 需求 | 文档 |
| --- | --- |
| 全组件索引和 feature 列表 | [根 README](../README.md) |
| Application 组件字符串与扩展点 | [napp README](../napp/README.md) |
| Saga 生产接线 | [Saga 生产运行指南](saga-production.md) |
| 部署与停机 | [应用部署指南](deployment.md) |
| 运行期观测与故障处理 | [应用运维指南](operations.md) |
