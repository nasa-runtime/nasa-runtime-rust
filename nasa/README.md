# nasa

`nasa` 是 `nasa-runtime-rust` 的唯一业务门面。应用只依赖本 crate，通过 feature 选择能力，再从
`nasa::<module>` 使用稳定入口；实现 crate 和宏 crate 由门面按需引入。

本 crate 属于独立开源项目，与美国国家航空航天局不存在隶属、赞助、认可或官方项目关系；完整
声明随包交付于 `NOTICE`。

## 核心价值与门面架构

业务 manifest 只选择 `nasa` feature，业务代码只使用 `nasa::<module>`；门面负责把实现 crate、过程宏、
codegen ABI、受管多源 registry 和可选 Application 组件接成一张一致的依赖图。这样业务不会直接锁定
内部 crate 名称，也不会因为 tonic/prost、事务上下文或生命周期实现出现两份 package 身份。

```text
业务 feature 与 #[nasa::application(...)]
                 │
                 v
        nasa 稳定模块与类型门面
                 │
                 v
实现 crate / 宏 crate / Application 唯一组件 owner
```

门面只组织公开类型和 feature，不持有额外运行状态，也不自动启动未在属性中声明的 listener 或消费端。
业务仍负责协议与领域语义、实际容量、路由、凭据来源和外部系统治理；Application 负责已声明组件的
Ready 门禁、监督和反向停机。

其中的 Saga 能力用本地事务、Outbox/Inbox、持久化状态机、稳定效果身份和显式补偿组成最终一致性
闭环；进程崩溃、重复投递、Unknown 结果和 timer 多副本竞争均从已提交事实恢复。它不把远端调用
伪装成跨服务 ACID，也不承诺物理 exactly-once 或并发隔离。业务通常启用 `application` +
`saga-runtime`，再显式选择 Kafka、Redis Streams、HTTP 或 gRPC transport；完整合同见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。

门面还提供三项稳定基础设施合同：有界 Schema Registry client、有完整性门禁的对象存储 adapter，
以及可独立运行或交给 Application 托管的 gRPC listener。三者保持独立 feature 以控制依赖面，同时
纳入 `full`；所有权、运行边界和非目标在下文单独说明。

```toml
[dependencies]
nasa = { version = "2", features = [
    "application",
    "tx",
    "mapper",
    "redis",
    "cache",
    "web",
] }
```

```rust
use nasa::mapper::{Mapper, Query};
use nasa::tx::transactional;

#[Mapper]
trait OrderMapper {
    #[Query("select id from orders where id = #{id}")]
    async fn find_id(&self, id: i64) -> sqlx::Result<Option<i64>>;
}

#[transactional]
async fn save_order() -> anyhow::Result<()> {
    Ok(())
}
```

## 受管单源与多源

启用 `application` 后，MySQL、Redis 与 Kafka 都由最终 YAML 创建，业务不在 `main` 中自行建池或维护
第二张连接表。单源与多源配置根、默认身份及选择入口如下：

| 资源 | 单源 | 多源 | 选择入口 |
| --- | --- | --- | --- |
| MySQL | `database` | `datasources.<name>` | `app.default_datasource().await` / `app.datasource(name).await` |
| Redis | 扁平 `redis` | `redis.properties.<qualifier>` | `app.default_redis().await` / `app.redis(name).await` |
| Kafka | `kafka` | `kafkas.<client>` | `app.default_kafka()` / `app.kafka(name)` |

Application 先校验完整命名表，再按稳定名称逐源探测，全部成功后一次性发布。Outbox 与 Saga 通过
`datasource_ref` 选择 MySQL；Cache、缓存失效广播与 Scheduling 通过 `redis_ref` 选择 Redis；Kafka
consumer/producer 使用 client name。引用不存在时会在 Ready 前失败，不会回退默认源。

MySQL 持久适配器同时提供显式绑定入口：`MySqlInbox::with_datasource`、
`MySqlOutbox::with_datasource`、`MySqlIdempotencyStore::with_datasource`、
`MySqlOutboxAuditSink::with_datasource`、`MySqlSagaStore::with_datasource`，以及
`Orchestrator::with_datasource` 和 `ParticipantRuntime::with_datasource`。同一原子链必须使用相同
qualifier；不同 datasource 之间不构成一个事务。source 集合、endpoint、凭据和身份字段在运行期
保持冻结，变化后必须重启。完整 YAML 与生命周期合同见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#yaml-创建单源与多源)。

## 应用入口

`application` feature 提供声明式入口。组件字符串可以任意书写；宏会拒绝未知项与重复项，再按唯一规范顺序
`log -> nacos-config -> telemetry -> db -> redis -> cache -> partition -> saga -> kafka -> outbox -> redis-job -> grpc -> auth -> web -> ws ->
nacos-discovery -> scheduling` 启动，并严格反序停机。

```rust
#[nasa::application("web", "cache", "redis", "log")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register(MyService::new())?;
    Ok(())
}
```

声明组件时必须启用对应 feature。`auth` 必须与 `web` 同时声明；`hystrix`、`grafana`、`mapper`、
`openapi` 等是函数级或门面能力，不是组件字符串。完整生命周期合同见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md)。

`#[nasa::application("saga")]` 隐式纳入 DB 与 Outbox，业务无需再声明 `"db"` 或 `"outbox"`；
`#[nasa::application("outbox")]` 可脱离 Saga 独立运行，并隐式纳入 DB。Inbox 是事务内原语，没有独立
组件字符串。Kafka、Redis Streams 或 HTTP 等 transport 不由 Saga 猜测，业务必须按发布端和消费端的
真实实现显式选择。显式同时写出 `"saga"`、`"db"` 与 `"outbox"` 也合法，并与只声明 `"saga"` 等价。

## RedisJob 门面

启用 `redis-job` 后，业务从 `nasa::redis::job` 使用定义、上下文、结果、控制和查询类型，并用
`#[nasa::redis_job]` 静态登记 Handler。`#[nasa::application("redis-job")]` 会隐式纳入 Redis transport；
宏 descriptor 与 UserHook 中通过 `app.configure_redis_jobs(plan)` 提交的动态定义进入同一冻结计划，
无需业务拼接 Lua、管理扫描器、续租、Fanout 订阅或停机任务。

Fanout 遇到本地槽位不足时会在有界容量窗口内等待，超窗优先切换兼容执行器，无候选时继续保留当前 assignment；容量迁移次数可通过 shard 的 `capacityRouteTotal` 审计。该字段是持久状态，进程指标不跨重启累计。

Handler 可使用 `JobContext::parameter::<T>()` 读取复杂 JSON 参数，`T` 支持集合、映射和嵌套结构；框架会先执行重复键、深度和节点数门禁。Protobuf/RAW 参数通过 `payload()` 按定义 codec 解码，不根据内容猜测类型。

```rust
use nasa::redis::job::{JobContext, JobResult};

#[nasa::redis_job(name = "ledger-close", qualifier = "primary", fixed_rate_ms = 60_000)]
async fn ledger_close(ctx: JobContext) -> anyhow::Result<JobResult> {
    ctx.checkpoint()?;
    Ok(JobResult::success())
}

#[nasa::application("redis-job")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

Application Ready 后，业务控制面通过 `app.redis_job_control(qualifier)` 和
`app.redis_job_query(qualifier)` 显式选 source；未知或已停止准入的 source 返回结构化错误，不会回退到
`primary`。取得的门面不拥有 shutdown 权限，停机仍由 Application 唯一编排。多 source 配置、独立
`RedisJobPlan` 与完整运行边界见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#redisjob-受管模式) 和
[nadis README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadis/README.md#redisjob-分布式任务运行时)。

## 业务初始化屏障

initializer 用于在 migration 与出站依赖已经可用、入站监听和消费循环尚未开放的窗口，装配动态
路由与注册表、恢复业务状态、回填或预热依赖。`#[nasa::initializer(...)]` 在业务二进制中静态收集
initializer；Service 的启动 Hook 也可通过
`app.register_initializer(spec, instance)` 动态登记。两种入口使用同一份依赖图，在 migration 和
出站依赖就绪后严格执行全部 `before -> initialize -> after` 三轮屏障，完成前不开放入站监听。
属性入口省略 `name` 时从实现类型名派生 canonical kebab-case，省略 `order` 时使用 `100000`；运行时
入口仍需在 `InitializerSpec::new(name)` 中显式提供稳定名称，并使用相同的默认顺序。派生名称也是依赖、
日志和指标 label 使用的稳定身份；需要让这些身份跨实现类型重命名保持不变时应显式填写 `name`。
依赖边始终优先于 `order`，同一可执行集合中数值越小越先执行。失败、panic、启动超时或取消都会阻止
Ready，并严格逆序停止已取得所有权的任务、撤销 action 并关闭资源；已经提交到 DB、Redis 或 Kafka
的事实不会被本地清理伪装成已撤销，因此实现必须使用事务或稳定幂等键保证可安全重跑。initializer
没有独立超时
配置，执行与条件工厂共同消费 `application.startup_timeout_ms` 的全局绝对预算。

完整元数据、`one-shot`/`hosted` 任务激活、指标与幂等边界见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#业务-initializer)。

## 稳定基础设施合同

这三项能力共享“资源有硬上限、错误不泄露敏感上下文、所有权必须唯一、观测事实可并入统一指标目录”
的边界，但运行架构不同：

| 能力 | feature 与入口 | 生命周期 owner | 核心安全合同 |
| --- | --- | --- | --- |
| Schema Registry | `kafka-schema-registry` → `nasa::kafka` | 业务持有 client；无组件字符串 | schema ID 白名单、有界正负缓存、默认禁止注册、凭据由 `SecretBytes` 承载 |
| 对象存储 | `object-store` → `nasa::object` | 业务持有 adapter；无组件字符串 | 有界单对象、`CreateOnly` 条件写、默认 SHA-256 metadata 复核、SigV4 credential 脱敏 |
| gRPC listener | `grpc` → `nasa::grpc` | 独立 `GrpcServerHandle` 或 Application `"grpc"` 二选一 | 统一 codegen/service registry、permit 先于 accept、TLS/mTLS、固定方法指标与有预算排空 |

Schema Registry 与对象存储在 UserHook 从最终配置和 secret 快照构造，不会因为启用 `application`
自动获得生命周期组件；需要统一 Prometheus/OTLP 出口时显式调用各自的 `metrics_source`，同一 family
只能登记一个 owner，多实例使用 `metrics_source_many`。gRPC 只有在同时启用 `application` 并声明
`"grpc"` 时才由容器托管，独立模式仍由业务显式 shutdown。

完整合同见 [nafka Schema Registry](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md#schema-registry)、
[naobject](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naobject/README.md) 和
[nagrpc](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nagrpc/README.md)。

### gRPC 完整接入

`#[nasa::application("grpc")]` 的目标是让业务只保留不可推断的协议与业务语义。自持 proto 的项目只需：

```toml
[dependencies]
nasa = { version = "2", features = ["application", "grpc"] }

[build-dependencies]
nagrpc-build = "2"
```

1. 在 `build.rs` 调用 `nagrpc_build::compile("proto/order.proto")`；
2. 用 `nasa::grpc::include_proto!("order.v1")` 包含生成类型并实现 generated trait；
3. 在 `main.rs` 的 UserHook 调用
   `app.register_grpc_service(OrderServiceServer::new(OrderService))`。

业务不直接依赖 tonic/prost，不构造 Router、health reporter 或 reflection builder，不逐 service 应用
消息上限，也不持有受管 listener 的 shutdown handle。`nagrpc-build` 统一 HOST protoc、codec 身份、
descriptor/摘要和 generated adapter；`nagrpc` 统一 service registry、Router、HTTP/2/TLS、容量、deadline、
方法策略与独立 `ServerPlan`；`napp` 在 Prepare 封口 registry，等 initializer 全部成功后才绑定并负责
readiness、发现 metadata、指标和反向停机；`nasa::grpc` 是业务运行时唯一门面。

`include_proto!` 不等价于简单转调 `tonic::include_proto!`：它还包含同一次构建的 descriptor 与摘要，
并连接 codegen ABI 和 `ManagedGrpcService` 适配，使运行时能在 bind 前验证 service/method 目录、冲突和
方法策略。协议已由独立 contract crate 发布时，应用不再需要自己的 `build.rs`，只依赖 `nasa` 与该
contract crate。完整配置、安全、发现、指标、兼容门禁和独立模式见
[nagrpc README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nagrpc/README.md)。

## Feature 总表

默认 feature 为空。只开启业务实际使用的能力：

| feature | 业务入口 | 说明 |
| --- | --- | --- |
| `application` | `nasa::application`、`nasa::Application` | 生命周期、配置快照、资源和受管任务 |
| `tx` | `nasa::tx` | ambient MySQL 事务和 `#[transactional]` |
| `mapper` | `nasa::mapper` | 声明式 SQL Mapper，蕴含 `tx` |
| `mapper-redis-cache` | `nasa::mapper` | Mapper Redis Hash L2 |
| `mapper-cache-grouped` | `nasa::mapper` | Mapper 接 `GroupedCache` |
| `inbox` | `nasa::inbox` | 与业务 MySQL 副作用同事务的消费去重 |
| `outbox` | `nasa::outbox` | 与业务写同事务的事件落库和顺序投递 |
| `idempotency` | `nasa::idempotency` | Provider-neutral 幂等状态机和进程内 store |
| `idempotency-mysql` / `idempotency-redis` | `nasa::idempotency` | MySQL 强一致或 Redis response-cache 后端 |
| `audit` | `nasa::audit` | 与业务写同事务的 Outbox 审计 |
| `openapi` | `nasa::openapi` | 确定性 OpenAPI 3.1 合同 |
| `redis` | `nasa::redis` | Redis 命令、pipeline、stream、lock |
| `redis-job` | `nasa::redis::job`、`nasa::redis_job` | 多 source RedisJob 状态机、`#[redis_job]` 与受管生命周期；蕴含 `application` 和 `redis` |
| `redis-search` / `redis-derive` | `nasa::redis` | 搜索封装和文档派生 |
| `cache` | `nasa::cache` | 两级缓存、失效广播和缓存宏 |
| `kafka` | `nasa::kafka` | 发布、消费、路由、确认和健康 |
| `kafka-tls` / `kafka-gssapi` / `kafka-zstd` | `nasa::kafka` | Kafka 传输安全与压缩子能力 |
| `kafka-schema-registry` | `nasa::kafka`、`nasa::secret` | 有界 schema adapter；蕴含 `kafka` 与 `secret`，进入 `full` |
| `saga` | `nasa::saga` | 无 I/O 的 definition、身份和补偿合同 |
| `saga-runtime` | `nasa::saga`、`nasa::application` | Orchestrator、参与方 adapter 与 Application Saga 组件 |
| `saga-kafka` | `nasa::saga` | 受管 command/result Kafka transport |
| `saga-redis-stream` | `nasa::saga`、`nasa::application` | 受管 Redis Streams 发布、消费、重领、原子 DLT 与积压观测 |
| `saga-grpc` | `nasa::saga`、`nasa::grpc`、`nasa::application` | 已包含 `grpc` 类型门面；generated command/result service、mTLS principal 绑定与封闭收据，入站计划复用 `"grpc"` listener，纯出站不启动 listener |
| `hystrix` | `nasa::hystrix` | 并发隔离、超时和 Dashboard 流 |
| `grafana` | `nasa::grafana` | 接口隔离、Prometheus 指标和面板 |
| `telemetry` | `nasa::application` | 受管 span 队列、OTLP/HTTP 导出和停机 flush |
| `web` | `nasa::web` | 路由宏、interceptor 和 HTTP 运行时 |
| `web-auth` | `nasa::web::auth` | 路由身份合同 |
| `web-crypto` | `nasa::web::crypto` | 双协议密码处理和重放保护 |
| `web-crypto-legacy-rsa` | `nasa::web::crypto` | 受控迁移的 legacy RSA 私钥路径，不进入 `full` |
| `web-security` | `nasa::web` | 身份、解密、重放、handler、加密固定流水线 |
| `oauth` | `nasa::oauth` | JWT、JWKS 与授权服务器 metadata |
| `secret` | `nasa::secret` | secret 分片、快照和两阶段轮换 |
| `secret-http` / `secret-vault` | `nasa::secret` | TLS client 和 KV v2 provider |
| `object-store` | `nasa::object`、`nasa::secret` | 有界对象存储合同；蕴含 `secret`，进入 `full` |
| `grpc` | `nasa::grpc`、`nasa::application` | 统一 codegen、独立或 `"grpc"` Application 受管 listener、TLS/mTLS、方法策略与观测，进入 `full` |
| `scheduling` | `nasa::scheduling` | 异步与定时任务 |
| `scheduling-cluster` | `nasa::scheduling` | Redis leader gate 和集群调度 |
| `partition` | `nasa::partition`；与 `application` 组合时含 `PartitionApplicationPlan`、`app.partition()` | 保序任务窃取、同 key 严格 FIFO、有界背压，以及不向业务开放收口权的 Application 受管健康与停机 |
| `ws` | `nasa::ws` | TCP/WebSocket 长连接 |
| `ws-redis` / `ws-socketio` / `ws-kafka` | `nasa::ws` | 长连接集群与协议子能力 |
| `log` | `nasa::log` | tracing、滚动文件和级别热切 |
| `yml` | `nasa::yml` | 分层 YAML、overlay、环境变量和占位符 |
| `yml-watch` | `nasa::yml::watch` | 精确文件变化观察；同时启用 `yml` |
| `config-boot` / `nacos-config` | `nasa::yml::nacos` | 配置中心引导与应用组件桥 |
| `discovery` | `nasa::discovery` | 静态/DNS/provider-neutral 发现合同 |
| `nacos` / `nacos-sdk` | `nasa::config::nacos`、`nasa::discovery::nacos` | API 层与真实传输层 |
| `rest-discovery` | `nasa::discovery::rest` | 服务发现 REST 负载均衡 |
| `rest-discovery-nacos` / `nacos-discovery` | `nasa::discovery` | 注册发现装配与应用组件桥 |
| `rest-client` / `rest-client-nacos` | `nasa::discovery::rest` | 声明式 REST client |
| `base` / `crypto` / `numeric` / `date` / `image` | 对应同名模块 | 基础类型和纯工具 |
| `crypto-legacy-rsa` | `nasa::crypto` | 受控迁移的 RSA 私钥兼容入口，不进入 `full` |
| `full` | 上述稳定能力的组合 | 非默认；包含 Schema Registry、对象存储、Saga gRPC 和 gRPC listener |

`kafka-gssapi` 使用目标系统的 Cyrus SASL。macOS 无需额外安装；Linux 构建环境需提供
`libsasl2-dev` 或 `cyrus-sasl-devel`，具体包名由发行版决定。

`nacos` 和 `rest-discovery-nacos` 只保证 API 可编译；真正连接后端必须同时启用 `nacos-sdk`。
`full` 选择 Kafka 和 gRPC 作为纳入的 Saga transport，同时纳入 gRPC listener；Redis Streams 替代
通道仍需显式开启。生产服务仍建议只选择实际使用的 feature，避免无意扩大依赖面与运行责任。

## YML 配置与使用

门面本身不读取 yml。声明式应用要求 `zcf/application.yml` 存在，内容可为 `{}`；具体根节点由对应
组件读取。

```yaml
application:
  name: order-service
  mode: service
  shutdown_timeout_ms: 20000

database:
  url: ${APP_MYSQL_URL}
  max_connections: 16

saga:
  database_bootstrap: application
  timer_poll_interval_ms: 500
  timer_error_backoff_ms: 1000
  timer_operation_timeout_ms: 5000
  timer_failure_threshold: 3

outbox:
  poll_interval_ms: 500
  error_backoff_ms: 1000
  operation_timeout_ms: 5000
  batch_size: 100
  failure_threshold: 3

redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2

cache:
  mode: two_level
  redis_ref: default

server:
  host: 0.0.0.0
  port: 8080
```

| 根节点 | 负责组件 |
| --- | --- |
| `application` | `napp` |
| `log` | `nalog` |
| `nacos` | `config-boot` / `nanacos` |
| `telemetry` | `napp` telemetry 组件，含受管 OTLP trace 与可选 cumulative metrics 出口 |
| `database` / `datasources` | `natx` / `namigrate` / `namapper` |
| `redis` | `nadis` |
| `cache` | `cacheable` 受管组件 |
| `saga` | Application Saga 组件 |
| `outbox` | Application Outbox 组件；也由 Saga 隐式纳入 |
| `kafka` / `kafkas` | `nafka` 受管组件 |
| `grpc` | Application gRPC listener、TLS、方法策略与协议能力；独立模式不读取此根 |
| `auth` | OAuth/JWKS 认证组件 |
| `server` | Web 组件 |
| `ws` | `naws` |
| `rest_discovery` | 注册发现组件 |
| `scheduling` | `nasched` |

Schema Registry 和对象存储没有固定配置根；README 中的 yml 仅是业务投影示例，门面不会隐式读取。

## 主要边界

- `nasa` 只做模块组织与重导出，不承载业务状态，也不提供全量 prelude。
- feature 是编译期能力；组件字符串是运行期生命周期 owner，两者不能混用。
- `full` 不应设为默认；生产服务应选择实际使用的能力，避免扩大编译与安全边界。
- 宏会识别门面被 Cargo 重命名的情况；业务无需直接依赖宏实现 crate。
- 具体失败语义、配置默认值和资源上限以各组件 README 为准。
