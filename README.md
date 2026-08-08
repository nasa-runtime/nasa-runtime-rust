# nasa-runtime-rust

NASA Rust 共享库是一组按特性组合的基础设施包。
**业务唯一入口是门面包 `nasa`**：业务项目只依赖 `nasa`，再按需开启映射、事务、缓存、Redis、WebSocket、配置、服务发现等特性。
其余成员用于实现和宏展开,默认不建议业务项目直接依赖。

> 名称声明：本项目是独立开源项目，与美国国家航空航天局不存在隶属、赞助、认可或官方项目关系，
> 也不使用其徽章、标识、印章或其它官方视觉标识。完整声明见 [NOTICE](NOTICE)。

## 使用

### 推荐入口

业务项目只声明 `nasa`，根据实际场景开启特性：

```toml
[dependencies]
nasa = { git = "https://github.com/nasa-runtime/nasa-runtime-rust.git", features = [
    "web",
    "mapper",
    "mapper-redis-cache",
    "tx",
    "redis",
    "cache",
    "log",
    "yml",
] }
```

```rust
use nasa::mapper::{Mapper, Query};
use nasa::tx::transactional;

#[Mapper]
pub trait UserMapper {
    #[Query("select id, name from users where id = #{id}")]
    async fn find_by_id(&self, id: i64) -> sqlx::Result<Option<User>>;
}

#[transactional]
pub async fn save_user() -> anyhow::Result<()> {
    Ok(())
}
```

### 应用入口：`#[nasa::application]`

服务型项目推荐用声明式入口替代手写 main 装配：配置装载、组件启动/停机顺序、信号处理、优雅停机、
任务监督与配置热刷新全部由应用运行时接管，业务只声明组件与启动钩子：

```toml
[dependencies]
nasa = { git = "https://github.com/nasa-runtime/nasa-runtime-rust.git", features = [
    "application", "log", "config-boot", "tx", "mapper", "mapper-redis-cache",
    "redis", "cache", "web", "scheduling",
] }
```

```rust
mod controller; // #[get_mapping] 等端点

#[nasa::application("log", "nacos-config", "db", "redis", "web", "scheduling")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    // 启动钩子：注册业务资源、登记受监督任务、注入路由或长连接定制。
    app.register(MyService::new(app.datasource("default").await?))?;
    Ok(())
}
```

可声明组件：`log`、`nacos-config`、`telemetry`、`db`、`redis`、`cache`、`saga`、`kafka`、
`outbox`、`auth`、`web`、`ws`、`nacos-discovery`、`scheduling`。宏接受任意书写顺序，并按规范顺序
启动、严格反序停机；声明了但特性未编入时会在编译期能力探测处失败。`saga` 会隐式加入 `db` 与
`outbox`，业务入口无需重复声明这两个组件；为兼容显式依赖，三者同时写出也合法且语义相同。
`hystrix`、`grafana`、`mapper` 是门面 feature 或函数级能力，**不是**可声明组件。
详见 [napp](napp/README.md)。

### Mapping interceptor：手动与自动装配

`#[interceptor]` 的 `global` 默认为 `false`，因此声明宏本身不会让拦截器自动执行。现有业务可以继续
在路由属性 `interceptors(...)`，或在 `app.configure_mapping(...)` 内使用 `plan.global(...)`、
`plan.scope(...)` 精确控制
覆盖范围。只有显式写出 `global = true`，napp 才会在 Web Ready 阶段自动把它装配到当前 crate 中
符合该 interceptor 阶段合同的全部 `*_mapping` 端点：

```rust
use nasa::web::interceptor;
use nasa::web::{Next, Request, Response};

// 手动例子：默认 global=false。
#[interceptor(id = "manual-edge", kind = "edge", order = 10)]
async fn manual_edge(request: Request, next: Next) -> Response {
    next.run(request).await
}

// 自动例子：main 不需要也不允许重复登记同一 binding。
#[interceptor(id = "automatic-edge", kind = "edge", order = 20, global = true)]
async fn automatic_edge(request: Request, next: Next) -> Response {
    next.run(request).await
}

#[nasa::application("web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_mapping(|plan| Ok(plan.global(manual_edge::binding())))?;
    Ok(())
}
```

自动 global 只覆盖 `*_mapping` 自动端点，不覆盖 `configure_router` 添加的 Axum 路由，也不覆盖
`/healthz`、`/readyz`。自动函数只能无 State，或使用与 Router 根 State 相同的 `State<T>`；窄 State、
路径 scope、`when_route` 和动态开关仍应手动装配。自动和手动对同一 ID 发生重叠时，监听前审计会明确
报错，不会静默去重或执行两次。全部属性、阶段顺序和 binding 规则见
[naweb README](naweb/README.md#通用-interceptor) 与
[naweb-macro README](naweb-macro/README.md#interceptor)。

### 按场景选择特性

只开启业务需要的特性。宏包和底层实现包通常由 `nasa` 自动带出，应用无需直接依赖。

| 业务场景 | 建议特性 | 常用入口 |
| --- | --- | --- |
| 应用入口与生命周期 | `application` | `#[nasa::application(...)]`、`nasa::Application` |
| HTTP 路由 | `web` | `nasa::web::{get_mapping, mvc_router}` |
| 路由身份认证 | `web-auth` | `nasa::web::auth::{AuthProvider, AuthContext}` |
| 路由双协议加密 | `web-crypto` | `nasa::web::crypto::{CryptoRuntime, KeyRing}` |
| 历史 RSA 私钥协议迁移 | `web-crypto-legacy-rsa` | 编译期开关；仍需 provider 运行时显式允许 |
| 完整端点安全流水线 | `web-security` | `try_register_all`、`MappingRuntime`、route policy |
| SQL Mapper | `mapper` | `nasa::mapper::{Mapper, Query, Insert, Update, Delete}` |
| Mapper L2 缓存 | `mapper-redis-cache` | `#[Query(..., cache = true)]` |
| MySQL 事务 | `tx` | `nasa::tx::{transactional, run}` |
| Saga 纯合同 | `saga` | `nasa::saga::{WorkflowDefinition, SagaOutcome}` |
| Saga MySQL Runtime | `saga-runtime` | `nasa::saga::{Orchestrator, ParticipantRuntime, saga}`、`nasa::application::SagaApplicationPlan` |
| Saga Kafka command/result 托管 | `saga-kafka` | `nasa::saga::{SagaKafkaCommandConsumer, SagaKafkaResultConsumer}` |
| 消费去重 Inbox | `inbox` | `nasa::inbox::MySqlInbox` |
| 受管事务 Outbox | `outbox` | `nasa::application::{OutboxApplicationPlan, OutboxHandle}` |
| 事务型业务审计 | `audit` | `nasa::audit::{MySqlOutboxAuditSink, TransactionalAuditSink}` |
| OpenAPI 3.1 | `openapi`（配合 `application` + `web`） | `Application::openapi_document`、`ApiSchema`、mapping 的 `request_schema` / `response_schema` |
| Secret/TLS 引用与两阶段轮换 | `secret` / `secret-http` / `secret-vault` | `RotatingSecretStore`、`RotatingTlsHttpClient`、`VaultKvV2Provider` |
| OAuth/JWKS/Metadata | `oauth` | `nasa::oauth::{MetadataClient, JwksRegistry}` |
| Schema Registry（实验） | `kafka-schema-registry` | `nasa::kafka::{ConfluentSchemaRegistry, ConfluentEnvelope}` |
| 对象存储（实验） | `object-store-experimental` | `nasa::object::{ObjectStore, S3ObjectStore}` |
| gRPC listener（实验） | `grpc-experimental` | `nasa::grpc::{GrpcServerConfig, GrpcServerHandle}` |
| Redis 命令、Stream、锁 | `redis` | `nasa::redis::RedisClient` |
| 方法级 L1/L2 缓存 | `cache` | `nasa::cache::{cached, cache_invalidate}` |
| 接口保护与 Prometheus/Grafana 面板 | `grafana` | `nasa::grafana::{grafana, Command, metrics}` |
| 日志 | `log` | `nasa::log::LogManager` |
| yml 配置加载 | `yml` / `config-boot` | `nasa::yml`、`nasa::config_boot` |
| 注册中心 | `nacos` / `nacos-sdk` | `nasa::nacos` |
| 静态/DNS 服务发现 | `discovery` | `nasa::discovery::{StaticDiscovery, DnsDiscovery}` |
| REST 负载均衡 | `rest-discovery` / `rest-discovery-nacos` | `nasa::discovery::rest` |
| 长连接 | `ws`、`ws-redis`、`ws-socketio` | `nasa::ws::Server` |
| 加密、金额、日期、图片 | `crypto`、`numeric`、`date`、`image` | `nasa::crypto` 等模块 |
| 定时和异步任务 | `scheduling`、`scheduling-cluster` | `nasa::scheduling::{Async, scheduled}` |

标为“实验”的能力不会被 `full` 隐式启用。它们已经具备有界 I/O、脱敏错误和独立生命周期，
但在两个真实业务项目形成共同合同之前不承诺稳定 API。

### GitHub 仓库依赖

```toml
# 业务项目 Cargo.toml——只依赖门面 nasa，按需开启特性
[dependencies]
nasa = { git = "https://github.com/nasa-runtime/nasa-runtime-rust.git", features = ["hystrix", "cache", "ws-redis", "rest-client"] }
```

```rust
use nasa::hystrix::hystrix;          // 熔断/隔离/超时/指标
use nasa::ws::Server;                // WebSocket 服务端
#[nasa::web::get_mapping("/x")]   // Axum MVC 风格路由
```

同仓内部依赖在源码 manifest 中使用 `path + version`：工作区构建解析本地路径，Cargo 生成公开归档时
会移除 `path` 并保留 registry 版本约束。下游 crate 只有在该版本已能从 registry 解析后才能发布，
不得用 `[patch]` 掩盖缺失的前置发布。根级质量工程不进入产品归档。

### crates.io 依赖

发布到 crates.io 后，业务项目仍只依赖门面 `nasa`：

```toml
[dependencies]
nasa = { version = "1", features = ["hystrix", "cache", "ws-redis", "rest-client"] }
```

内部实现包按工作区 `Cargo.toml` 中的 package name 发布，例如 `nabase`、`nadate`、`naimg`、`naws`。
包名可用性是发布时事实，正式发布前仍须按 [发布指南](docs/publishing.md) 实时复查 crates.io，不能依赖
历史占用结论。Cargo 包坐标不改变业务接口：门面模块仍是 `nasa::base`、`nasa::date`、
`nasa::image` 等。

## YML 配置总览

根 README 只给组合方式；每个组件的完整字段、默认值、初始化代码和使用场景在各自 README 中维护。
下面是 `#[nasa::application]` 各组件读取的配置根（`zcf/application.yml`，必须存在、内容可为 `{}`）：

```yaml
application:            # 仅启动期读取，远端不可改写
  name: app
  mode: auto            # 可填 auto、service、batch；无 Web 的常驻服务应显式填写 service

log:
  level: info,app=debug
  path: logs/app

nacos:                  # 配置中心（nacos-config 组件），与服务发现独立
  enabled: false
  server_addr: 127.0.0.1:8848
  group: DEFAULT_GROUP

database:               # db 组件，多库使用 datasources.<name>
  url: ${APP_MYSQL_URL}
  max_connections: 16

saga:                   # saga 组件；发布端由 SagaApplicationPlan 注入
  database_bootstrap: application
  timer_poll_interval_ms: 500
  timer_error_backoff_ms: 1000
  timer_operation_timeout_ms: 5000
  timer_failure_threshold: 3

outbox:                 # outbox 组件，也由 saga 隐式纳入
  poll_interval_ms: 500
  error_backoff_ms: 1000
  operation_timeout_ms: 5000
  batch_size: 100
  failure_threshold: 3

redis:                  # redis 组件
  url: ${APP_REDIS_URL}
  namespace: app
  profile: RustV2

cache:                  # cache 组件，通过 redis_ref 显式复用受管 Redis
  mode: two_level
  redis_ref: default
  cache_ttl_secs: 300
  null_ttl_secs: 30
  invalidation:
    enabled: false
    redis_url: ""

server:                 # web 组件
  host: 0.0.0.0
  port: 8080
  context_path: /app

ws:                     # ws 组件，独立于 server
  addr: 0.0.0.0:9000
  ws_addr: 0.0.0.0:9001

rest_discovery:         # nacos-discovery 组件
  enabled: false
  provider: nacos

scheduling:             # scheduling 组件
  cluster: local        # 可填 local、leader；leader 模式需要声明 redis 组件
```

手工装配（不使用应用运行时）的项目可自定根节点，启动顺序建议：

1. `naml` 加载本地 yml、profile、环境变量并解析 import 描述；`config-boot` 负责拉取远端配置并组装 overlay。
2. `nalog` 初始化日志。
3. `nadis`、`natx` 初始化 Redis 和 MySQL pool。
4. `namapper`、`cacheable` 注入缓存和数据源。
5. `nanacos`、`rest-discovery` 初始化注册发现和 REST 负载均衡。
6. `napp` Web 组件装配 naweb 路由并启动 HTTP 服务,`naws` 启动长连接服务,`nasched` 启动调度器。

`#[nasa::application]` 按规范组件顺序自动完成上述全部步骤，并补齐手工装配普遍缺失的部分：
信号处理、启动失败反向回滚、统一停机预算与配置热刷新。

## 组件 README 索引

| 组件 | 门面模块或特性 | 主要场景 | 配置入口 |
| --- | --- | --- | --- |
| [nasa](nasa/README.md) | 门面包 | 统一导出所有业务能力 | 不直接读取 yml，按组件配置 |
| [napp](napp/README.md) | `application` | `#[nasa::application]` 应用生命周期运行时:组件编排、信号、优雅停机、配置热刷新 | `application.*` 及各组件配置根 |
| [napp-macro](napp-macro/README.md) | `application` | `#[nasa::application(...)]` 属性宏与编译期校验 | 由 `napp` 运行时读取 |
| [naml](naml/README.md) | `yml` | 分层 yml、profile、环境变量覆盖、解析本地/远端 import 描述 | `yml.*`、业务自定义根节点 |
| [config-boot](config-boot/README.md) | `config-boot` | 启动期读取本地和远端配置 | `nacos.*`、`nacos.imports` |
| [nanacos](nanacos/README.md) | `nacos` / `nacos-sdk` | Nacos 配置、注册、发现、监听 | `nacos.*` |
| [nadisc](nadisc/README.md) | `discovery` | 服务发现抽象、实例过滤、watch 契约 | `discovery.*` |
| [rest-discovery](rest-discovery/README.md) | `rest-discovery` | `lb://service` REST 负载均衡调用 | `rest.*` |
| [rest-discovery-nacos](rest-discovery-nacos/README.md) | `rest-discovery-nacos` | Nacos 注册发现和 REST 负载均衡组合 | `rest_discovery.*`、`nacos.*` |
| [rest-client-macro](rest-client-macro/README.md) | `rest-client` | 声明式 REST 客户端宏 | `rest_clients.*` |
| [naweb](naweb/README.md) | `web` / `web-security` | Axum 路由、interceptor 与端点安全运行时 | `server.*` 由 napp 读取 |
| [naweb-macro](naweb-macro/README.md) | `web` | MVC 风格路由注解和路由收集 | 编译期属性，无运行期配置 |
| [namapper](namapper/README.md) | `mapper` / `mapper-redis-cache` | 声明式 SQL Mapper、动态 SQL、二级缓存 | `mysql.*`、`datasources.*`、`mapper.*`、`redis.*` |
| [namapper-macro](namapper-macro/README.md) | `mapper` | Mapper 派生和 SQL 注解宏 | 由 `namapper` 运行时读取 |
| [natx](natx/README.md) | `tx` | ambient MySQL 事务、after-commit 回调、多数据源 | `mysql.*`、`datasources.*` |
| [natx-macro](natx-macro/README.md) | `tx` | `#[transactional]` 事务宏 | 由 `natx` 运行时读取 |
| [nasaga-core](nasaga-core/README.md) | `saga` | Saga 身份、definition、状态机、结果与补偿计划合同 | 无 I/O；definition 由业务注册 |
| [nasaga-mysql](nasaga-mysql/README.md) | runtime 内部 store | Saga journal、CAS、timer fencing、参与方 gate 与 migration | 复用 `natx` MySQL pool |
| [nasaga-runtime](nasaga-runtime/README.md) | `saga-runtime` / `saga-kafka` | Orchestrator、参与方事务 adapter、管理审计、指标与 Kafka result consumer | 由业务注入 definition、topic owner 与投递策略 |
| [nasaga-macro](nasaga-macro/README.md) | `saga-runtime` | `#[saga]` descriptor 和类型化参与方 adapter | 编译期属性，无运行期配置 |
| [nadis](nadis/README.md) | `redis` | Redis 单点或集群、流水线、数据流、锁 | `redis.*` |
| [nadis-derive](nadis-derive/README.md) | `redis-derive` | Redis Search 文档派生 | `redis.search.*` 由业务映射 |
| [cacheable](cacheable/README.md) | `cache` | L1/L2 缓存、刷新保护、失效广播 | `cache.*`、`redis.*` |
| [nacache-macro](nacache-macro/README.md) | `cache` | `#[cached]`、`#[cache_invalidate]` | 由 `cacheable` 运行时读取 |
| [naidempotency](naidempotency/README.md) | `nasa::idempotency` | 幂等状态机、首次执行、重放与冲突裁决 | 无固定 yml；由业务注入 store |
| [naidempotency-mysql](naidempotency-mysql/README.md) | `idempotency-mysql` | 与业务事务共享记录或提供持久响应重放 | 复用 `database.*` |
| [naidempotency-redis](naidempotency-redis/README.md) | `idempotency-redis` | 有 TTL 的跨副本响应重放 | 复用 `redis.*` |
| [naoutbox-core](naoutbox-core/README.md) | `nasa::outbox` | Outbox 事件、发布端和保序投递 | 无运行期 yml |
| [naoutbox-mysql](naoutbox-mysql/README.md) | `outbox` | 同事务写事件、单 owner dispatcher、可选死信 | 复用 `database.*` |
| [nainbox-core](nainbox-core/README.md) | `inbox` 内部合同 | 消费去重裁决 | 无运行期 yml |
| [nainbox-mysql](nainbox-mysql/README.md) | `inbox` | Inbox 标记与业务副作用同事务 | 复用 `database.*` |
| [naaudit](naaudit/README.md) | `audit` | 脱敏业务审计事件与事务型 sink 合同 | 无独立配置根 |
| [naaudit-mysql](naaudit-mysql/README.md) | `audit` | 审计事件写入同事务 MySQL Outbox | 复用 `database.*` |
| [hystrix](hystrix/README.md) | `hystrix` | 熔断、隔离、超时、指标流 | `hystrix.*` |
| [hystrix-macro](hystrix-macro/README.md) | `hystrix` | `#[hystrix]` 宏 | 由 `hystrix` 运行时读取 |
| [nafana](nafana/README.md) | `grafana` | 接口隔离、Prometheus 指标、Grafana 原生自适应接口墙 | `grafana.*`、`/metrics` |
| [nafana-macro](nafana-macro/README.md) | `grafana` | `#[grafana]` 编译期参数校验与包装 | 无运行期 yml |
| [nametrics-core](nametrics-core/README.md) | `application` 内部合同 | 指标目录、冲突审计、结构化与 Prometheus 导出 | 无运行期 yml |
| [natelemetry](natelemetry/README.md) | `telemetry` 内部运行时 | Trace Context、有界 span 队列与停机 flush | `telemetry.*` 由 `napp` 读取 |
| [nasched](nasched/README.md) | `scheduling` / `scheduling-cluster` | 异步任务、定时任务、Redis 集群去重 | `scheduling.*` |
| [async-macro](async-macro/README.md) | `scheduling` | `#[Async]`、`#[scheduled]` 宏 | 由 `nasched` 运行时读取 |
| [napart](napart/README.md) | `partition` | 同 key 串行、有界队列、停机取消 | `partition_executor.*` |
| [naws](naws/README.md) | `ws` / `ws-redis` / `ws-socketio` | TCP/WebSocket 长连接、鉴权、广播、背压 | `ws.*` |
| [naws-proto](naws-proto/README.md) | `ws` | 长连接协议帧和编码模式 | `ws.protocol.*` |
| [naws-proto-derive](naws-proto-derive/README.md) | `ws` | 协议结构体派生 | 网络配置由 `naws` 读取 |
| [nafka](nafka/README.md) | `kafka` | 发布、消费、路由、确认、健康与安全配置 | `kafka.*` / `kafkas.*` |
| [nafka-macro](nafka-macro/README.md) | `kafka` | `#[kafka_consumer]` 静态收集 | 由 Kafka 运行时读取 |
| [ncrypto](ncrypto/README.md) | `crypto` | 现代令牌加密和历史兼容加解密 | `crypto.*`、环境变量承载密钥 |
| [nanum](nanum/README.md) | `numeric` | 定点金额、价格、最小变动单位对齐、舍入 | `numeric.*` |
| [nadate](nadate/README.md) | `date` | 日期解析、格式化、窗口计算 | `date.*` |
| [naimg](naimg/README.md) | `image` | 图片压缩、尺寸裁剪、格式转换 | `image.*` |
| [nalog](nalog/README.md) | `log` | 控制台和文件日志、级别热切换 | `log.*` |
| [nabase](nabase/README.md) | `base` | BaseResponse、ByteSize、Snowflake | `base.*` |
| [nabudget](nabudget/README.md) | REST/Web 内部合同 | 绝对 deadline 与取消树 | 无运行期 yml |
| [namigrate](namigrate/README.md) | `application` + `tx` | MySQL migration validate/apply 门禁 | `database.migrations` |
| [naopenapi](naopenapi/README.md) | `openapi` | 从已审计路由事实生成确定性 OpenAPI 3.1 | `application.*` 文档信息 |
| [naauthz](naauthz/README.md) | `application` + `web` 内部合同 | 路由 scope 与对象级授权 | 策略由代码或外部 provider 注入 |
| [nauth-oauth](nauth-oauth/README.md) | `oauth` | JWT、JWKS 与授权服务器 metadata | `auth.*` 由 `napp` 读取 |
| [nasecret](nasecret/README.md) | `secret` | 分片解析、脱敏快照与两阶段轮换 | `secrets.*` |
| [nasecret-http](nasecret-http/README.md) | `secret-http` | 随 secret 代际轮换的 TLS/mTLS HTTP client | 引用 `secrets.*` ID |
| [nasecret-vault](nasecret-vault/README.md) | `secret-vault` | 有界 KV v2 secret provider | provider 配置由业务投影 |
| [naobject](naobject/README.md) | `object-store-experimental` | 有界对象合同与 S3-compatible adapter | 无受管组件配置；业务显式构造 |
| [nagrpc](nagrpc/README.md) | `grpc-experimental` | 独立 HTTP/2 listener、健康、反射与排空 | 无受管组件配置；业务显式构造 |
| [macro-support](macro-support/README.md) | 宏内部依赖 | 过程宏路径解析 | 无运行时 yml |

## 安全说明(务必阅读)

- **`ncrypto` 的弱加密是【刻意兼容历史实现】,不是缺陷、也不适合作新系统的机密性边界。**
  为逐字节对齐既有服务,ncrypto 保留了 AES-ECB、CBC(IV=Key)、RSA PKCS#1 v1.5、
  以及"用 RSA 私钥做保密"等**已知弱**的构造。**只用于与既有系统互操作**;新系统请用
  `nasa::crypto::encrypt_modern` / `decrypt_modern` 这类现代入口,不要复用这些兼容函数。现代入口默认使用
  随机盐 + Argon2id + AES-256-GCM，返回自描述 `NC2.*` 令牌；业务可用 AAD 绑定租户或记录上下文。
  既有 PBKDF2-HMAC-SHA256 的 `NC1.*` 仅保持兼容读取，错误口令、AAD 错配或密文篡改都会失败。

- **`rsa` 0.9 计时侧信道（RUSTSEC-2023-0071，Marvin 攻击）当前没有上游修复。**
  由 ncrypto 引入。默认构建只保留 RS256 公钥验签等不执行易受攻击私钥解密的能力；历史
  PKCS#1 v1.5 私钥解密与私钥 type-1 运算受专用编译 feature 和 Web 运行时开关双重隔离，
  且不进入 `full`。`deny.toml` 仍按包级 advisory 显式登记，待上游发布修复即移除。

- **拒绝服务攻击防护与资源上限**（默认已提供保守兜底，可按部署调整）：
  - `ws`:`ServerConfig.max_connections`(连接总数,accept 处背压)、`max_unauthenticated`
    (未认证连接数,防慢握手/慢鉴权占满连接池)、`max_inflight_handlers`(全局)
    与 `max_inflight_handlers_per_conn`(单连接配额,防单连接抢占全局池)。
  - `partition`:有界队列(`with_partitions_and_capacity`);`submit` 返回 `Result<(), SubmitError>`,
    满/停机/分区死对调用方**可见**(不静默丢),`submit_async` 提供等容量的真背压。
  - `image`:输出像素上限(`MAX_OUTPUT_PIXELS`)防解压炸弹式放大。

## 开发

```bash
cargo fmt --all
cargo clippy --workspace --all-features -- -D warnings
cargo deny check          # 需要 cargo-deny，用于供应链检查
cargo publish --dry-run -p nabase
```

持续集成（`.github/workflows/ci.yml`）只依赖产品源码入口执行格式、静态检查、构建和依赖审计。
本地质量工程由不参与产品发布的根级入口统一管理，组件 crate 与 `.crate` 归档只能携带产品
源码、公开文档和再分发所需文件。
真实后端连接信息只能由本地环境注入，不要把内网地址、账号或密码写进说明文档或持续集成配置。

## 开源文档

| 文档 | 用途 |
| --- | --- |
| [架构说明]() | 门面分层、应用生命周期、可靠消息与生产边界。 |
| [快速开始](docs/quickstart.md) | 业务应用如何依赖 `nasa`、选择特性、配置 yml 和编写最小示例。 |
| [部署指南](docs/deployment.md) | 应用模式构建、配置注入、容器信号、健康端点和接流条件。 |
| [运维指南](docs/operations.md) | 运行状态、退出码、停机顺序、配置刷新和故障排查。 |
| [交付就绪清单](docs/release-checklist.md) | 产品归档、组件边界和生产环境批准条件。 |
| [公开归档说明](docs/publishing.md) | 多包依赖拓扑、归档内容和许可说明。 |
| [贡献指南](CONTRIBUTING.md) | 贡献规则、文档、注释和代码维护约束。 |
| [安全说明](SECURITY.md) | 安全报告方式、敏感面和默认安全策略。 |
| [当前变更说明](CHANGELOG.md) | 当前工作区的业务能力变化。 |

## 许可证

采用双许可证：[MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE)，二选一。
