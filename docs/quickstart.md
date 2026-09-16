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

零装配路径可选择 managed HTTP、gRPC、Kafka 或 Redis Streams。MySQL 开启 saga-runtime，PostgreSQL
开启 saga-runtime-pgsql；
Application 依据角色作用域的 datasource_ref 选择 driver、创建角色所需结构并构造运行时。

~~~toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "saga-runtime", "web"] }
~~~

纯 Orchestrator 的业务入口可以为空：

~~~rust
#[nasa::application("saga", "web")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
~~~

~~~yaml
datasources:
  saga-control:
    driver: mysql
    url: ${SAGA_DATABASE_URL}

outbox:
  datasource_ref: saga-control

saga:
  role: orchestrator
  plan_mode: managed
  service_identity: checkout-orchestrator
  replica_identity: ${SAGA_REPLICA_ID}
  orchestrator:
    datasource_ref: saga-control
  definition_catalog:
    mode: dynamic
    datasource_ref: saga-control
    activation_policy: validated
    watch_interval_ms: 500
    capability_registry_ref: saga-participant-capabilities
    publisher_authorization_policy_ref: saga-definition-publishers
  http:
    base_path: /_nasa/saga
  api:
    page_token_key_ref: saga-api-page-token
    http:
      enabled: true
      authorization_policy_ref: checkout-saga-http-rbac
      callers:
        order-api:
          credential_ref: saga-order-client
          tenants: [system]
          permissions: [start, read]
  transport:
    address_policies:
      saga-participant-routing:
        http_schemes: [https]
        http_hosts: [inventory.internal, payment.internal]
        http_ports: [443]
    command_result:
      kind: http
      http:
        shared_replay_claim: saga-http-replay
        command_credential_ref: saga-command
        result_credential_ref: saga-result
        routing:
          mode: capability-registry
          address_policy_ref: saga-participant-routing
~~~

participant 使用相同 Application 声明，但配置 saga.role: participant，并明确 service_identity、
consumer_identity、orchestrator_identity 与 datasource_ref。业务步骤使用 managed=true 的 #[saga]；
框架负责 command 路由、HMAC/replay、Inbox/gate、本地事务、result Outbox 和 capability 续租，业务
只实现 execute、compensate 以及需要的 resolve/cancel。workflow owner 使用 #[nasa::saga_workflow]
返回完整有序 definition；main 不调用 register、publish 或 configure_saga。

client 角色不构造 Orchestrator。direct 模式无需本地数据库，调用 SagaRemoteClient::start/get；
reliable_start 模式必须声明 client.datasource_ref，并在当前同源业务事务内调用 enqueue_start，使业务
事实和 start-intent Outbox 原子提交。远端暂不可用时事件保持待派发，恢复后由周期扫描或提交唤醒送达。
dispatcher 同样固定使用 `saga.client.datasource_ref`；显式 `outbox.datasource_ref` 若与它冲突，应用
会在 Ready 前拒绝启动。仅设置 Outbox 轮询预算不改变扫描数据源。
`enqueue_start` 返回 `event_id` 后还需等待外层业务事务明确提交，才能返回本地已受理；远端是否创建
或完成须查收据及实例状态，不能由 Ready 或已受理响应推断。

PostgreSQL 只需把 datasource driver 改为 postgresql 并开启 saga-runtime-pgsql；角色、Definition
Catalog、收据和恢复语义相同。HTTP、gRPC、Kafka 与 Redis Streams 均可作为受管 command/result
数据面；多 datasource participant 通过每个 descriptor 的显式 binding 精确选库。控制面可独立选择
HTTP 或 gRPC，`saga_orchestrator.proto` 随 runtime core 发布，供非 Rust 客户端从同一协议源生成代码。
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

## 一次性业务收尾

业务自有客户端需要 close、flush、归还或注销时，在 UserHook 把收尾 future 交给 Application。
下面的 `client` 表示由业务创建且尚未交给其它关闭 owner 的客户端：

```rust
let shutdown_client = client.clone();
app.register_graceful_shutdown(100, "orders-client", async move {
    shutdown_client.flush().await
})?;
```

任务返回 `()` 或 `Result<(), E>`，其中 `E: Into<anyhow::Error>`。数值越小越早执行；同优先级保持登记
顺序。任务名是固定业务名称，不能拼接租户、请求或对象身份，单个 Application 最多 256 项。
Service 与 Batch 都可登记，但只能在 UserHook 内完成；登记失败会释放传入的 future。

两种模式都先收口受监督任务，随后执行业务停机任务，再释放 UserHook 业务资源。
Service 的 initializer 在业务任务之前清理；Batch 的静态 initializer 在业务资源之后清理。
任务可以执行时再调用 `Application::resource` 查找尚未撤销的资源，借用应在任务返回前释放。同一对象只保留一个最终关闭
owner：已经使用 `register_managed` 的对象，或 Application 拥有的数据库、Redis、Kafka、listener 等
组件，不再另登记重复关闭任务。

所有清理共享 `application.shutdown_timeout_ms`，单项不能阻塞或无限循环。失败、超时和可隔离展开不
覆盖首次终止原因；期限耗尽时未开始项记为 `Abandoned`。直接取消 Runner 会撤销本实例的全局入口和
新资源借用，将 Starting/Ready 置为 Stopping，已有 Stopped/Failed 不变；不保证执行异步收尾。
剩余 action、停机任务与所属资源按实际激活栈逆序同步释放，保持 Service/Batch 各自的 initializer 位置。
任务门上尚未析构的受监督 future 会保留后续清理所有权，最后一个 future 释放后才归还依赖；
全局入口与新借用立即撤销，取消调用方不等待这个过程，任务不让出执行权时资源会继续保留。
已借出的资源随借用归还释放，此前复制的外部客户端句柄不在撤销范围内。
需要完整优雅停机时应使用 Service 的 `app.shutdown()` 或进程 SIGTERM。完整限制见
[业务优雅停机任务](../napp/README.md#业务优雅停机任务)。

## 后续阅读

| 需求 | 文档 |
| --- | --- |
| 全组件索引和 feature 列表 | [根 README](../README.md) |
| Application 组件字符串与扩展点 | [napp README](../napp/README.md) |
| Saga 生产接线 | [Saga 生产运行指南](saga-production.md) |
| 部署与停机 | [应用部署指南](deployment.md) |
| 运行期观测与故障处理 | [应用运维指南](operations.md) |
