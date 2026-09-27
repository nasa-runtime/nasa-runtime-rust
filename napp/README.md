# napp

`napp` 是 `#[nasa::application]` 属性入口背后的应用生命周期运行时：统一配置装载、Ready 前业务初始化、
按优先级执行业务停机任务、组件启停、任务监督、信号处理与退出码。声明 `"web"` 时还独占具备确定
HTTP/1/h2c 选择、容量门禁和有预算排空的 listener。业务项目经 `nasa` 门面开启 `application` feature
使用，不需要为异步业务收尾另建信号监听或 callback 集合。
直接取消 Runner 时，仍存活的受监督任务保留其依赖资源，直至任务 future 析构；受管可靠 Saga client
则把业务事务、发起意图和 dispatcher 绑定同一数据源，在开放流量前拒绝显式配置冲突。
受管 Mapper 自动登记 SQL、连接与 Pool 指标，并按 YAML 装配开发 SQL/参数输出、离散通知与指标
出口。所有可调参数有 YAML 入口，非必要项递归补齐默认值，观测故障与 SQL/事务结果隔离。
慢 SQL 达到配置阈值后可经业务主动安装的 `Notify` 发送；关闭通知冷却即可逐条提交，框架不选择
通知微服务协议或持有机器人连接。

Application 标准纳管 Redis Stream／Proxy／AutoPipeline、出站 TCP 帧客户端、hystrix 命令目录、Mapper 缓存、命名幂等与审计、REST、对象存储、Schema Registry、
secret/TLS 与本地文件监听，把连接来源、启动屏障、配置应用状态和退出责任绑定到同一资源 owner。
Redis 消费、微批调用和出站 Client 与宿主终端共用启动许可；关键资源失效会阻止 Ready，
停机必须等待实际任务与在途调用退出。隔离命令随 Application 实例重建，旧句柄永久失去准入。
文件监听与密钥解析使用同一活跃消费者集合：仅由禁用计划引用的密钥文件及其无消费者 provider
引导文件不建立观察；共享 ID 仍有活跃引用时继续解析和监听。候选新增的文件依赖必须先可观察，
再发布配置；失败时保留旧视图及旧监听。
业务提交配置与 handler，框架负责启动门禁、健康和停机；各能力的 feature、入口、配置和失败边界见
[受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。

## 命名资源运行架构

```text
Service：配置与 feature 校验 → 来源启动 → UserHook 登记计划
         → Prepare 迁移与命名资源装配 → initializer 取得句柄
         → 关键资源、健康证据和启动期限复验 → Ready 与统一启动许可
Batch：配置与静态计划校验 → 来源、迁移、资源与初始化完成 → 工作负载
关闭：撤销准入 → 等待已接纳工作退出 → 释放 owner 与依赖
```

Service 的 UserHook 不能提前取得尚未装配的标准 adapter；Batch 的工作负载无需 Web 即可使用
幂等、审计和出站客户端。`source`/`redis_ref` 固定资源来源，错误引用不会猜测默认库。
数据库持久 adapter 在迁移门禁之后只读校验 schema，不在业务请求中隐式建表。
取得句柄与准许调用是两个阶段：Service 的 initializer 可以保存命名 Pipeline、Proxy 和 Client
句柄，但它们在统一放行前拒绝业务发送；消费 handler 和 Client 业务回调同样等待启动许可。
Batch 的 Pipeline 与发送 Client 在工作负载前开放，不等待 Service Ready，也不接受长期消费或回调计划。
hystrix 在 Prepare 安装本代目录，可用于后续初始化与业务收尾；它的准入由命令 owner 管理。

配置更新先准备材料与安全资源，再发布同代视图和真实应用状态。`Applied` 表示该目标已经采用
对应配置；`RestartRequired` 和 `ApplyFailed` 保留最后成功版本，不因无关更新而消失。
配置读取应固定一次 `ConfigView`，不能把新 YAML 的可见性当成所有运行资源已切换的证明。
每项资源只保留一个关闭 owner；关闭通知后仍须等待任务和在途调用的退出证明。

## 命名对象存储与 Schema Registry

这两类客户端在 Prepare 装配，无需新增组件字符串，也无需启动 Web 或 Kafka 消费组件：

| 能力 | nasa 门面 feature | 直接 napp feature | 命名配置与获取入口 |
| --- | --- | --- | --- |
| 对象存储 | `application,object-store` | `object-store` | `object_stores.<name>` → `app.object_store(name).await` |
| Schema Registry | `application,kafka-schema-registry` | `kafka-schema-registry` | `schema_registries.<name>` → `app.schema_registry(name).await` |

命名计划须显式设置 `enabled: true`；未声明或禁用时不构造客户端、不读取其独占凭据。Prepare 使用
同代配置和 `secret://` 材料绑定资源，并自动登记聚合指标。Service 在后续 initializer 或 Ready 后任务中
使用句柄，Batch 在工作负载开始前完成装配。停机关闭新调用、等待在途调用，旧句柄永久返回 `Closed`；
参数或凭据变化报告 `RestartRequired`。对象存储按显式健康策略监督远端状态；Registry 构造不探测远端、
不注册 schema，其指标仅反映实际调用。

独立 `S3ObjectStore` 和 `ConfluentSchemaRegistry` 仍可由业务显式构造并自行持有；与 Application
组合时，独立指标源可在 UserHook 登记。标准受管计划已有指标 owner，不应重复手工登记。
完整配置见 [对象存储](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naobject/README.md#配置投影)
与 [Schema Registry](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md#配置与使用)。

## SQL 观测与通知

门面同时启用 `application` 与 `mapper`/`mapper-pgsql` 后，在 DB 建连前冻结方法目录与观测策略，
预留所有低基数指标系列；UserHook 已能查询本地指标和有效配置。无需添加新的组件字符串。
直接依赖本 crate 时使用 `mapper-observability`，统一出口使用 `observability`。关闭通知不创建队列；
渠道客户端由业务提供，框架不包含具体消息渠道实现。关闭外送不停止基础计量。

`sql.observability` 统一控制 console、行数、慢 SQL、执行失败、Pool acquire 与事务槽等待、告警和
dispatcher。覆盖按 `method > datasource > global` 逐叶继承，空对象不会重置上层值。
逐条慢 SQL 通知需同时满足业务已安装 `Notify`、`alerts.slow_sql.enabled=true` 和
`alerts.slow_sql.cooldown_ms=0`；默认冷却为 60000 ms。判定使用原始耗时 `>= slow_sql.threshold_ms`，
不包含连接等待与消费者处理时间，也不依赖 `slow_sql.log_enabled`。有界队列不承诺故障下绝对送达。
策略在启动时冻结；日志级别可热刷新，但不能借此改变 SQL 连接选项。完整默认 YAML、范围、生产
数据边界及指标口径见 [SQL 观测合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/namapper-core/README.md#sql-观测与配置)。

业务实现 `nanotify_core::Notify` 并调用 `nanotify_core::init(Arc<dyn Notify>)` 安装进程实现。
告警省略 `provider_ref` 即使用默认实现，无需声明 `notifications.providers`；未初始化时忽略通知，
不阻断启动、不入队也不记为失败。Service UserHook 期间已初始化的通知只允许候选入队，全部
Ready 门禁通过后 worker 才投递。通知通过有界并发、总超时和有限安全重试隔离第三方行为，通知失败、
队列满或停机丢弃不能改变 SQL 返回、事务裁决或数据库 readiness。它不是持久事件投递系统。
停机先摘流，再关闭新通知、按预算排空，最后释放 Pool。
Batch 在工作负载前激活指标出口和默认通知 worker，不执行业务组件 Ready；业务可在工作负载内
调用 `init`，之后触发的通知正常投递。进程实现只允许初始化一次，多个 Application 共用，不会自动重置。
需要多路由时仍可声明 `kind: custom` 并在 Service UserHook 调用
`app.register_notify_provider(provider_ref, provider).await`；命名目录在 Prepare 冻结，缺失实现忽略投递。
Batch 不支持活跃命名路由；显式命名引用不存在或禁用仍拒绝启动，`default` 保留给进程实现。

`app.sql_observability_effective(datasource, method).await` 返回冻结的有效配置，不包含 SQL、URL 或
凭据。外部注入 Pool 的已有 SQLx 日志选项不由 Application 追溯修改，调用方须在建池时设置。
通知 provider 配置与通信边界见 [nanotify-core](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/nanotify-core/README.md)，
独立或 Web 指标出口、remote write 与 controller 边界见
[nafana](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/nafana/README.md)。

## 核心价值与生命周期架构

业务入口只声明组件并在 UserHook 提交不可推断的业务计划；`napp` 负责把配置、migration、出站资源、
initializer、listener、消费循环、readiness、关键任务和停机编排成一个唯一所有者。业务不需要为每个
组件另写启动顺序、信号处理、后台任务脱离检测或 shutdown glue。

```text
Service 配置装载 → Start 出站资源 → UserHook 提交计划 → Prepare / initializer 屏障
         → Ready 装配与静态登记 → initializer 任务工厂构造 → 静态检查与关键本地权威复验
         → 同次提交 Ready 与启动许可 → Running 监督关键任务与领域 owner
         → NotReady / 摘流 → 受监督任务与 initializer 收口
         → 业务停机任务 → 业务资源与更早启动的组件 → Stopped
```

Batch 先完成 Prepare，再执行静态 initializer，最后运行作为工作负载的 UserHook；Hook 中同样可以登记
业务停机任务，工作结束或失败后依次收口受监督任务、业务停机任务、业务资源、静态 initializer 与
更早启动的组件。`priority` 只排序业务任务，不改变组件 active stack。

组件 Ready 按声明顺序装配并登记可逆 action，交出的关键任务先暂存、未经 poll。
所有参与 Ready 的组件完成、initializer 暂存任务工厂全部构造成功后，统一执行 `validate_ready()`，
其中 remote write 核对最终静态指标容量。任务所有权可以先登记进 Supervisor，但组件与 initializer
主体共用一个关闭的执行屏障；全部检查通过并发布 Application Ready 后才统一放行。
最终发布还在关键领域的本地状态保护内核对任务责任、Client 认证、健康新鲜度与启动期限，
保护持续到共享许可发布；Redis 派生入口与 Client 因此不需要等待监控任务另行激活。
检查失败、工厂异常、超时或中断时屏障保持关闭，主体未经 poll，由统一清理释放任务与反向关闭 action。
同步复验必须短时、只读、非阻塞，不添加登记或外部副作用；panic 收敛到启动失败，每次返回后及最终
放行前都复验共享 deadline。同步代码不能被异步 timeout 抢占，超时后返回也不会再发布 Ready。
Batch 只让观测组件参与该屏障，在工作负载前放行，不发布 Service Ready，也不开放业务 listener。
统一许可覆盖组件终端、initializer 暂存任务及上述受管领域入口，不会延迟 UserHook 中普通
`spawn_background` / `spawn_critical`，也不能拦截业务自行派生的任务或 I/O。initializer 任务工厂
只负责构造 future，不能在工厂执行期间开放入口；自管 listener 使用 `serve_when_ready`。

initializer 是 Ready 前的初始化屏障：migration 与出站依赖完成后统一执行静态宏和运行时登记项的
`before -> initialize -> after` 三轮。任何阶段失败都不会开放监听、消费或服务发现，已经启动的资源按
active stack 反向释放。Application 拥有业务声明的组件与 feature 对应的自动观测节点，不猜测外部 transport，也不替业务决定
事务边界、路由、鉴权主体、容量值或重试/DLT 策略。

gRPC 入站同样遵守这个模型：业务登记 generated service 或提交 Saga gRPC handler，Application 在
同一个 sealed registry 中完成 Router、TLS、health、reflection、容量、指标、listener 和 drain，不会
产生第二个 tonic Router 或第二套生命周期。

Web 请求安全和 trace 也服从同一快照边界：认证完成后冻结 Principal、route 策略、未命中缺省与
generation，显式策略不会被公开路由豁免绕过；合法上游 trace flags 原样继承，无上游时只有
`"telemetry"` exporter 的 sampler 可以决定新根 sampled 位。没有声明 `"telemetry"` 的 Web 仍传播
上下文，但不会替下游宣布已采样。调度执行 span 只在 scheduling 取得 leader/claim 权威后建立。

## 最小入口

```toml
nasa = { version = "2.0.0", features = [
    "application", "log", "nacos-config", "telemetry", "tx", "redis", "cache",
    "kafka", "oauth", "web",
    "nacos-discovery", "scheduling",
] }
```

```rust
mod controller;

#[nasa::application(
    "log", "nacos-config", "telemetry", "db", "redis", "cache",
    "partition", "kafka", "auth", "web", "nacos-discovery", "scheduling"
)]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    // 业务启动 Hook：注册资源、登记受监督任务、注入路由/长连接定制和运行时 initializer。
    app.configure_router(|router| router)?;
    Ok(())
}
```

## `application` 支持的组件字符串

属性当前只接受下面 17 个小写字符串，名称区分大小写，不支持别名。业务可按任意顺序书写；宏会拒绝
未知名称和重复名称，再按唯一规范顺序生成组件列表。字符串对应的门面 feature 没有启用时会在编译期
拒绝，不会静默跳过。

| 属性中填写的精确字符串 | 需要启用的 `nasa` feature | 配置根 | 组件作用 | 容器能力入口 |
| --- | --- | --- | --- | --- |
| `"log"` | `log` | `log` | 两阶段日志：早期控制台 → 最终文件日志；运行期 `log` 段可热重应用 | `app.log()` |
| `"nacos-config"` | `nacos-config`；真实远端连接再加 `nacos-sdk` | `nacos` | 远端 overlay 首拉与 watch 热刷新；`enabled=false` 走纯本地 | `app.nacos_config()` |
| `"telemetry"` | `telemetry` | `telemetry` | 有界 span 管道、可选 OTLP trace/metrics、受管停机 flush | `app.telemetry_snapshot()` / `app.otlp_metrics_snapshot()` |
| `"db"` | MySQL 用 `tx`，PostgreSQL 用 `tx-pgsql`，混配同时开启 | `database` 或 `datasources.<name>` | 跨 driver 全表校验、逐源探测、冻结 catalog/registry 与显式停机 | MySQL 用 `app.datasource(name).await`；PostgreSQL 用 `app.pg_datasource(name).await` |
| `"redis"` | `redis` | `redis` 或 `redis.properties.<qualifier>` | 多实例统一建连、逐源健康与显式停机 | `app.redis(name).await`、`app.default_redis().await` |
| `"cache"` | `cache`；使用 `redis_ref` 时还需 `redis` | `cache` | scene 审计、L2 安装、失效广播与代际 owner | 宏经进程级 cache runtime 使用 |
| `"partition"` | `partition` | `partition`，或 UserHook 提交 `PartitionApplicationPlan` | Prepare 启动命名 Runner、逐域健康与反序有界停机 | `app.partition()`、`app.partition_runner(name)` |
| `"saga"` | MySQL 用 `saga-runtime`，PostgreSQL 用 `saga-runtime-pgsql` | `saga` | 按 datasource driver 校验步骤合同与历史实例，发布运行角色并监督 durable timer | `app.saga()` |
| `"kafka"` | `kafka` | `kafka` 或 `kafkas.<client>` | 受管 producer/consumer、broker Ready、动态健康与两段停机 | `app.kafka(name)`、`app.default_kafka()` |
| `"outbox"` | MySQL 用 `outbox`，PostgreSQL 用 `outbox-pgsql` | `outbox` | 按 datasource driver 持续投递已提交事件、退避、readiness 与反向停机；可脱离 Saga 使用 | `app.outbox()` |
| `"redis-job"` | `redis-job`；隐式纳入 `redis` | `redis.job` 与 `redis.properties.<source>` | 冻结多 source 计划、布局与能力门禁，Ready 后启动扫描、租约、Fanout、逐源监督和有界停机 | `app.redis_job()`、`app.redis_job_control(source)`、`app.redis_job_query(source)` |
| `"grpc"` | `grpc` | `grpc` | initializer 后自动装配 registered service、health、可选 reflection、TLS、listener、方法指标与有界排空 | `app.grpc()` |
| `"auth"` | `web`，并同时声明 `"web"`；直接使用 OAuth 类型再开 `oauth` | `auth` | 静态/远程 JWKS 首拉、刷新、认证器发布和 readiness | Web 安全流水线消费 |
| `"web"` | `web`；需要端点安全时使用 `web-security` | `server` | 自动收集端点、HTTP/1/h2c 监听、探针与排空；定制经 `configure_router` | `app.web()` |
| `"ws"` | `ws` | `ws` | TCP/WebSocket 长连接监听与排空；鉴权和 endpoint 经 `configure_ws` 注入 | `app.ws()` |
| `"nacos-discovery"` | `nacos-discovery`；真实 provider 再加 `nacos-sdk` | `rest_discovery` | Start 装出站客户端、Ready 冻结注册计划、统一放行后注册、停机先摘流后关客户端 | `app.nacos_discovery()` |
| `"scheduling"` | `scheduling`；选主模式使用 `scheduling-cluster` | `scheduling` | Ready 末尾启动已收集任务；选主模式复用已声明的 Redis 客户端 | `app.scheduling()` |

只用部分能力时只填写需要的字符串。例如纯 Web 应用写 `#[nasa::application("web")]`；本地配置的
数据库 Web 应用可写 `#[nasa::application("db", "web")]`。独立批处理仍可只开启 `kafka` feature 并显式
管理 `KafkaProxy`；Service 一旦把 `"kafka"` 写入属性，连接、消费、Ready、监控和停机就全部归容器所有。
`hystrix`、`grafana`、`mapper` 不是属性组件字符串，仍通过各自 feature 使用。
MySQL 或 PostgreSQL Mapper 缓存不新增组件字符串。配置 `mapper_cache.enabled: true` 与 `redis_ref` 后，
框架复用受管 Redis 安装 L2 并验证字段过期能力；Service 在 Ready 前、Batch 在工作负载前完成门禁。
通过 `nasa` 门面时，MySQL 使用 `mapper-redis-cache`，PostgreSQL 使用 `mapper-redis-cache-pgsql`。

直接依赖 `napp` 时，`mapper-cache` 是 MySQL Mapper 的 Ready 门禁，并同步启用
`namapper/redis-cache`；`mapper-cache-pgsql` 是 PostgreSQL Mapper 的 Ready 门禁，只依赖
`namapper-core`，不会引入 MySQL runtime。标准配置同时需要 `redis` feature 和组件；自定义实现仍可
在启动 Hook 安装，不能与标准计划重复占用默认槽。codec/metrics 使用 `configure_mapper_defaults`，
与缓存 owner 一起受启动回滚和停机管理。

不要填写 `"nacos"`、`"discovery"`、`"database"`、`"websocket"` 或 `"schedule"`；对应的合法
字符串分别是 `"nacos-config"`、`"nacos-discovery"`、`"db"`、`"ws"` 和 `"scheduling"`。

规范顺序固定为 `log -> nacos-config -> telemetry -> db -> redis -> cache -> partition -> saga -> kafka -> outbox -> redis-job ->
grpc -> auth -> web -> ws -> nacos-discovery -> scheduling`。业务书写顺序不改变启动顺序；停机严格反向执行。
`auth` 缺少 `web` 会被拒绝，`cache.redis_ref` 指向受管 Redis 时还必须声明 `"redis"`。

## YAML 创建单源与多源

MySQL、PostgreSQL、Redis 与 Kafka 的 endpoint、凭据、池和客户端参数都属于 YAML。Application 在启动期读取最终
配置并创建全部实例；业务 `main` 只提交 publisher、consumer、handler、Saga 定义等业务计划，再通过
`app.datasource(...)`、`app.redis(...)` 或 `app.kafka(...)` 取得已经受管的句柄，不自行建池、建连或
维护第二张 source 表。所有 source 先完成全表校验，再按名称稳定排序创建；任一项失败都会阻止 Ready。

| 资源 | 单源 YAML | 多源 YAML | 默认身份 | 被其它组件引用的字段 |
| --- | --- | --- | --- | --- |
| MySQL / PostgreSQL | `database` | `datasources.<name>` | `default` | `outbox.datasource_ref`、`saga.orchestrator/participant/client.datasource_ref` |
| Redis | 扁平 `redis` | `redis.properties.<qualifier>` | 持久身份 `primary`，查询兼容名 `default` | `cache.redis_ref`、`cache.invalidation.redis_ref`、`scheduling.redis_ref`、`redis.job.sources.<qualifier>` |
| Kafka | `kafka` | `kafkas.<client>` | `client_name: default` | `configure_kafka(client, ...)`、`app.kafka(client)` 与 consumer 的 `client` |

Saga 协调角色使用 `saga.orchestrator.datasource_ref`，参与方使用
`saga.participant.datasource_ref` 或逐 binding 引用，可靠 client 使用 `saga.client.datasource_ref`；这些
角色字段不会回退不存在的顶层 `saga.datasource_ref`。

同一种资源的单源根和多源根互斥，不能在同一份配置中混用。显式 getter 只选择启动期已发布的句柄，
不会在调用时建连；默认 getter 只查找上述默认身份，缺失时不会猜测唯一实例或第一个实例。运行期改变
source 集合、endpoint、凭据、database、namespace、profile、group 或身份都会要求重启。

### MySQL 单源

单源使用 `database`，Application 将它发布为 `default`：

```yaml
database:
  url: ${APP_DB_URL}
  max_connections: 20
  min_connections: 2
  acquire_timeout_ms: 2000
  connect_timeout_ms: 5000
  probe_on_start: true
  migrations:
    mode: validate
    lock_timeout_ms: 30000
    allow_dirty: false

# 独立 Outbox 未绑定发布计划时默认选择 default；可靠 Saga client 使用自己的角色数据源。
outbox:
  datasource_ref: default
```

业务使用 `app.default_datasource().await` 或 `app.datasource("default").await`。`database` 只表示一个
source，不能在它下面再嵌套自定义名称。

### MySQL 多源

多源使用 `datasources` map；map key 是权威 qualifier，每个值都拥有完整且独立的连接池与 migration
设置：

```yaml
datasources:
  default:
    url: ${APP_PRIMARY_DB_URL}
    max_connections: 20
    min_connections: 2
    migrations:
      mode: validate
  reporting:
    url: ${APP_REPORTING_DB_URL}
    max_connections: 8
    min_connections: 1
    migrations:
      mode: validate

outbox:
  datasource_ref: reporting
```

业务分别调用 `app.default_datasource().await` 与 `app.datasource("reporting").await`。如果多源 map 没有
`default`，命名 getter 仍可用，但默认 getter 会明确失败。Outbox 与 Saga 的引用必须命中同一份
`datasources`，两者共同形成原子事件链时必须使用相同的 `datasource_ref`；不存在的引用会在首次数据库
握手前被拒绝。`database_bootstrap: user_hook` 只允许单个 `default`，命名库必须使用
`database_bootstrap: application` 让容器从 YAML 创建。

### PostgreSQL 单源

PostgreSQL 必须显式声明 `driver: postgresql`；URL 接受 `postgres://` 与 `postgresql://`。业务通过
`app.default_pg_datasource().await` 或 `app.pg_datasource("default").await` 取得 typed pool：

```yaml
database:
  driver: postgresql
  url: ${APP_POSTGRES_URL}
  max_connections: 20
  min_connections: 2
  acquire_timeout_ms: 2000
  connect_timeout_ms: 5000
  probe_on_start: true
  schema: public
  connection_topology: direct
  migrations:
    mode: validate
    lock_timeout_ms: 30000

outbox:
  datasource_ref: default
```

`connection_topology` 只接受 `direct`、`session_pool` 或 `transaction_pool`。直连与会话级代理都能让
advisory lock、catalog 查询、执行和 unlock 留在同一物理 session；事务级代理不能保持这一不变量，
因此门禁模式不是 `disabled` 时，`transaction_pool` 还必须提供保证 session affinity 的
`migrations.session_url`，普通业务连接仍可
继续使用事务池。Application 会在取锁前复验 migration endpoint 与业务池的 database/schema 身份，
不一致时拒绝 Ready。`schema` 与这两个字段只属于 PostgreSQL，
MySQL 配置出现时会在建连前拒绝。显式配置 `schema` 时，该值同时决定每条业务连接的 `search_path` 和
受管 migration 的对象作用域；目标 schema 必须由数据库初始化流程预先创建，框架不会隐式创建。省略
`schema` 时，业务连接保留 PostgreSQL 服务端的默认 `search_path`（通常为 `"$user", public`），受管
migration 仍以 `public` 为默认作用域；需要两者严格一致时必须显式配置。启用 `probe_on_start` 时，显式
schema 不存在或不能成为 `current_schema()` 会在 Application 开放业务路由前拒绝。

### MySQL 与 PostgreSQL 混配

同一个 `datasources` map 可以同时受管两种 driver，名称在统一 catalog 中不可重复：

```yaml
datasources:
  orders:
    driver: mysql
    url: ${APP_MYSQL_URL}
    max_connections: 16
  analytics:
    driver: postgresql
    url: ${APP_POSTGRES_URL}
    max_connections: 8
    schema: reporting
    migrations:
      mode: validate
```

MySQL 未写 `driver` 时仍可由 `mysql://` URL 归一化；PostgreSQL 不从 URL 隐式猜测，必须显式声明。
`app.datasource("orders")` 与 `app.pg_datasource("analytics")` 分别返回对应 typed pool，反向查询返回
driver mismatch，不会回退到同名另一后端。`analytics` 池中的不限定 SQL 与 migration 都以
`reporting` 为首选 schema，不需要在 URL 中重复编码 `options=-csearch_path=...`。单个 ambient transaction 只能绑定一个 driver 和一个
datasource；需要耐久跨库收敛时使用源库 Outbox 与目标库 Inbox，不把 after-commit 当作可靠双写。

### 业务 migration 登记

YAML 的 `migrations.mode`、锁等待和 topology 字段只定义执行策略，不包含业务 SQL。Service 必须在
UserHook 为每个需要门禁的数据源登记一份业务嵌入的 `Migrator`；业务因此需要直接依赖启用相应 driver
与 `migrate` 能力的 `sqlx`，供 `sqlx::migrate!` 在构建期读取语义化 migration 文件：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["application", "tx-pgsql"] }
sqlx = { version = "0.9", default-features = false, features = ["macros", "migrate", "postgres"] }
```

```rust
#[nasa::application("db")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_migrations("default", sqlx::migrate!("./migrations"))?;
    Ok(())
}
```

数据源必须已经出现在 `database` 或 `datasources`，同一名称只能登记一次。Application 在 Prepare 阶段、
initializer 和入站监听之前依该数据源的 driver 执行门禁；`disabled` 跳过、`validate` 只接受完全一致的
已应用集合、`apply` 才应用未执行项。Service Hook 返回后登记入口封口。Batch 的 DB Prepare 早于业务
Hook，必须在 Hook 内显式取得 pool；MySQL 调用 `nasa::application::run_gate`，PostgreSQL 调用
`nasa::migration::pgsql::run_gate`，不能使用 `configure_migrations`。

### Redis 单源

单源使用扁平 `redis`。其持久 qualifier 固定为 `primary`，Application 同时提供 `default` 查询别名：

```yaml
redis:
  url: ${APP_REDIS_URL}
  namespace: orders
  profile: RustV2

cache:
  mode: two_level
  redis_ref: primary
scheduling:
  cluster: leader
  leader_key: scheduled:leader
  redis_ref: primary
```

`app.default_redis().await`、`app.redis("default").await` 与 `app.redis("primary").await` 返回同一个
`Arc<RedisClient>`；协议 marker、key 空间和指标仍使用 `primary`，不会写入本地兼容别名。

### Redis 分区消费生命周期

`"redis"` 组件管理客户端及通过 `configure_redis_partition(source, critical, configure)` 登记的消费计划。
业务在 UserHook 提供 handler，框架 Prepare 冻结计划，统一 Ready 后开放消费；通过
`redis_partition(source)` 取得发布与查询句柄。每个来源拥有独立 napart Runner 集合，无需声明 `"partition"`。

每个 Redis 源的 `partition.executor.scope` 可选 `source`（默认）、`group`、`stream`，分别按
源实例、逻辑组、物理 Stream 划分执行与固定容量份额。各源注册表与预算独立；跨域同计划同 key
仍受消费器的 ACK、重试和 Park 顺序门禁约束。

框架聚合 owner 先关闭所有来源准入，再并发使用共享截止点排干，提供逐来源健康和未完成报告。
未取得退出证明时保留依赖责任，不能把取消通知视为 I/O 与租约已经退出。
独立组件使用方仍可持有 `RunningPartition` 自行管理生命周期。
完整合同见
[Redis 分区消费](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/nadis/docs/partition.md)。

### Redis 多源

多源把每个完整客户端配置放在 `redis.properties` 下；`primary` 是推荐的默认键，也可以只写
`default` 作为配置边界同义名，但二者不能同时出现：

```yaml
redis:
  properties:
    primary:
      url: ${APP_PRIMARY_REDIS_URL}
      namespace: orders
      profile: RustV2
    sessions:
      url: ${APP_SESSION_REDIS_URL}
      namespace: order-sessions
      profile: RustV2

cache:
  mode: two_level
  redis_ref: sessions
  invalidation:
    enabled: true
    redis_ref: sessions

scheduling:
  cluster: leader
  leader_key: scheduled:leader
  redis_ref: primary
```

`app.redis("sessions").await` 只返回 `sessions` 客户端。缓存失效广播的 `redis_ref` 复用所选受管客户端的
命令连接，并从它派生专用订阅连接；兼容的 `redis_url` 独立路径仍可单独使用，但两个字段互斥。集群
调度和 RedisJob 同样只使用显式 qualifier，不按唯一实例推断其它 source。

### Kafka 单 client 与多 client

单 client 使用 `kafka`；省略 `client_name` 时默认发布为 `default`，可通过
`app.default_kafka()` 或 `app.kafka("default")` 获取：

```yaml
kafka:
  bootstrap_servers: ${APP_KAFKA_BOOTSTRAP_SERVERS}
  group_id: order-worker
  producer:
    acks: all
  container:
    consumers: collected
```

多 client 使用 `kafkas` map，map key 是权威 client name：

```yaml
kafkas:
  default:
    bootstrap_servers: ${APP_PRIMARY_KAFKA_BOOTSTRAP_SERVERS}
    group_id: order-worker
  audit:
    bootstrap_servers: ${APP_AUDIT_KAFKA_BOOTSTRAP_SERVERS}
    group_id: audit-worker
```

业务用 `configure_kafka("default", ...)`、`configure_kafka("audit", ...)` 登记各 client 的消费行为，
并用对应名称取得 producer/admin/readiness 能力；这些入口不创建 Kafka source。单 client 也可以显式设置
其它 `client_name`，此时必须使用命名入口，`default_kafka()` 不会猜测它。

上方组件表列出的每个配置根都由 napp 直接解析和校验。Saga、Outbox、RedisJob 与 Kafka 的 UserHook
入口只接收不可静态配置的业务定义或处理逻辑；连接地址、客户端参数和通用运行参数仍全部来自 YAML。

## RedisJob 受管模式

`"redis-job"` 隐式纳入 `"redis"`，但仍是独立的长生命周期组件。业务只声明定义与 Handler；Application
在 Prepare 阶段按 `qualifier` 精确绑定受管 Redis source，完成布局、ACL、脚本、定义和指标容量门禁，
全部 initializer 成功后才启动领取。能力登记只要有一项失败，就会关闭该 source 准入，并按全部已尝试
Worker 坐标执行补偿注销；Redis 持续不可达时，本地不会接纳工作，未确认的远端 `ACTIVE` 成员依赖
TTL 与 Registry GC 收敛。停机时先关闭所有 source 的新控制动作和领取，再按稳定逆序排空 Handler；
超时 task 的取消必须等待实际退出后，才注销执行器并关闭专用连接。一个 source 的健康、监督代次和
停机结果不会被另一个 source 覆盖。

Ready 后的运行路径由 `nadis::job` 执行：计划按 source 冻结，执行器发布能力快照，Dispatcher 投递并由目标持久确认，Handler 取得带 lease 和 fencing 的 attempt 后执行，receipt、ready、lease 和 root 扫描器分别完成恢复与聚合。Fanout 容量背压使用独立窗口和路由预算；`capacityRouteTotal` 保留历史容量迁移次数，便于 source 级运维审计。

心跳传输失败不会在第一次错误时直接宣告失权：运行时只在最近一次 Redis 已确认的执行器
`expireAt` 之前退避重试并发布 Degraded，成功后恢复；`NotFound`、协议失败或硬截止耗尽才关闭
当前 source 准入。终态运行错误包含 `JobSourceHealthReason` 的封闭名称，且状态迁移会写入不含
endpoint、任务参数或 payload 的监督日志。

静态任务使用 `#[nasa::redis_job]`，无需在 `main` 中手工建立 plan：

```rust
use nasa::redis::job::{JobContext, JobResult};

#[nasa::redis_job(
    name = "wallet-sweep",
    qualifier = "match",
    fixed_rate_ms = 30_000,
    timeout_ms = 20_000
)]
async fn wallet_sweep(ctx: JobContext) -> anyhow::Result<JobResult> {
    // 外部副作用前复验当前 attempt 仍持有执行权；下游写入还应携带 attempt token 做 fencing。
    ctx.checkpoint()?;
    Ok(JobResult::success())
}

#[nasa::application("redis-job")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

动态定义通过 `app.configure_redis_jobs(plan)` 在 UserHook 一次性移交，并与静态 descriptor 合并后共同
冻结。该入口不接触 Redis；重复提交、静态/动态重名或 Prepare 后提交都会被拒绝。业务控制器只能在
Application 已 Ready 时调用 `app.redis_job_control("match")`、`app.redis_job_query("match")` 或
`app.redis_job()`；所有入口都要求显式 source，不按唯一成员、任务名或 `run_id` 猜测数据源，也不开放
运行时 shutdown 权限。控制 API 只负责 Redis 状态权限，HTTP/RPC 调用者身份和 requestId 授权仍由业务
入口校验。

```yaml
redis:
  properties:
    primary:
      url: ${APP_PRIMARY_REDIS_URL}
      namespace: order-primary
      profile: LegacyV1
    match:
      url: ${APP_MATCH_REDIS_URL}
      namespace: order-match
      profile: LegacyV1
  job:
    instance_identity: order-service-0
    sources:
      match:
        namespace: order-match-jobs
```

`redis.job` 根字段是所有被引用 source 的默认值，`sources.<qualifier>` 只是稀疏覆盖；没有覆盖块的已托管
source 仍可被任务使用。定义引用未知 source 或 `enabled: false` 的覆盖时启动失败，不会回退到
`primary`。完整协议、独立 `RedisJobPlan`、Fanout、Cron 和观测合同见
[nadis README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/nadis/README.md#redisjob)。

## 跨副本分布式业务配额

启用门面 `rate-limit` 后会同时启用 `application` 与 `redis`，`nasa::application` 提供后端中立的 `RateLimitProvider`、Redis 固定窗口实现
`RedisRateLimitProvider`，以及与 `web` 组合时可用的 IP 中间件 `distributed_rate_limit`。业务从
`app.redis(qualifier).await` 取得受管 Redis 客户端并显式构造 provider；本能力没有组件字符串或独立
YAML 根，也不会仅因声明 `"redis"` 或 `"web"` 自动装到路由。

共用 Redis 与 namespace 的所有副本对同一主体合并计数。简单主体使用
`{namespace}:{subject}`，包含分隔符或过长的输入使用域分隔摘要；服务端 Lua 原子完成 `INCR` 与首次
`PEXPIRE`，窗口从第一次命中开始。

后端失效(Redis 不可达、脚本失败、零上限、非法窗口)按构造期冻结的
`RateLimitFailurePolicy` 裁决：缺省 `open` 放行并写 `warn`(可用性优先)；高保障路径用
`with_failure_policy(Closed)` 把"无法计量"视同"超额"——失效期一律拒绝并携带保守的整窗
`Retry-After`。

`DistributedRateLimit::try_new` 在启动期拒绝零上限、零窗口和超过 365 天的窗口。配额主体来源经
`with_subject` 类型化选择：`QuotaSubject::ClientIp`(缺省，依赖外层 `resolve_client_ip` 写入的
`ClientIp`)、`QuotaSubject::Tenant`(已验证 Principal 的 tenant)、`QuotaSubject::Principal`
(优先 subject、否则 client_id)、`QuotaSubject::Header`(API key 形态——header 值在离开中间件前
即做域分隔 SHA-256 摘要，凭据原文不进 provider、Redis key 或日志)。选择的来源缺失时不会回退
到其它身份，统一按 `with_missing_subject_policy` 裁决：缺省 `Allow` 计数放行，`Deny` 直接
`403` 封死"不可归因即不设限"的旁路。超额返回 `429` 与向上取整且至少一秒的 `Retry-After`。

五类事件(allowed/denied/backend_error/invalid_config/missing_subject)进入统一指标目录的
`napp_rate_limit_events_total{event=...}`(封闭词表、恒五条序列，Web Ready 注册)，也可经
`rate_limit_counters()` 直接读取累计值。`allowed`/`denied` 与 `missing_subject` 统计 Web
中间件请求；`backend_error`/`invalid_config` 是内置 Redis provider 的原因计数。业务直接调用
`RateLimitProvider::check` 不经过中间件，因此不会增加最终请求结局计数。该跨副本总配额可与 Web
自带的单实例令牌桶叠加，两者不共享计数语义。

## Partition 受管模式

`"partition"` 让 Application 显式拥有一个 `PartitionRunnerRegistry`。每个稳定名称对应独立的
generation、slot 主队列、类型路由、盗洞、延迟索引、容量、指标和停机权威。Prepare 按名称启动全部
Runner，全部成功后才发布业务句柄；任一启动失败都会反序收口已启动项。句柄只开放提交和只读观测，
不开放 `start`、`stop` 或 `force_stop`。

热点类型会通过盗洞交给空闲 slot 推进；严格类型跨迁移与归还保持同一受理序号，非严格类型可以使用
多个租约盗洞并发消费。`submit_after` 登记成功只表示 Runner 已持有该延迟任务，到期时仍需竞争类型容量
和当前路由；若门禁拒绝，`await_outcome` 返回稳定 `Rejected`。取消与到期竞争只形成一个终态，许可和
拒绝指标不会重复结算。

```yaml
partition:
  default_runner: default
  runners:
    default: {}
    settlement:
      partitions: 8
      queue_capacity_per_type: 2048
      global_inflight: 8192
      critical: false
      force_on_timeout: false
```

每个 Runner 的字段都可以省略，省略项逐字段使用 `RunnerConfig::default()`；`max_type_states` 还会
自动覆盖规范化后的分区数，因此 `default: {}` 就是完整有效计划。配置项只覆盖显式给出的字段。
单 Runner 也可使用扁平 `partition` 段，`queue_capacity` 与 `max_lanes` 分别作为
`queue_capacity_per_type` 与 `max_type_states` 的兼容键，同一对键不能同时出现。运行期更改计划需要
重启 Application。

完整 YAML 属性如下。容量和时长必须为正数；所有 `*_ms` 都以毫秒为单位，最大值为 365 天。

| 属性 | 默认值 | 作用与约束 |
| --- | --- | --- |
| `partition` | 无 | Partition 受管计划根；声明组件后必须提供该段或在 UserHook 提交至少一个计划 |
| `partition.default_runner` | `default` | `app.partition()` 指向的稳定名称；必须存在于 `runners` |
| `partition.runners` | 无 | 稳定名称到 Runner 计划的映射，命名形态至少包含一项，最多 4096 项 |
| `partition.runners.<name>` | `{}` | 单个隔离执行域；名称非空、无首尾空格和控制字符，UTF-8 最多 128 字节 |
| `partitions` | 可用并行度的两倍，再向上取 2 的幂 | slot 与 worker 数；输入范围 1..=65536，最终规范化为不小于输入的 2 的幂 |
| `queue_capacity_per_type` | 65536 | 每个 `(home, TaskType)` 尚未进入 Running 的许可上限，覆盖入队、移动和 worker 暂存；最大为 Tokio `Semaphore::MAX_PERMITS` |
| `queue_capacity` | 同上 | `queue_capacity_per_type` 的兼容键；两者同时出现会拒绝配置 |
| `global_inflight` | 默认分区数 × 65537，并钳制到 Tokio semaphore 上限 | 当前 Runner 延迟、排队、移动和执行中任务总上限；显式覆盖 `partitions` 不会联动重算本字段 |
| `max_type_states` | `max(RunnerConfig 默认值, 规范化分区数)` | 当前 Runner 全部 `(home, TaskType)` 状态及指标基数上限；`RunnerConfig` 默认值为 `max(4096, 默认分区数)`，该字段不得小于当前规范化分区数，不得超过 1048576 |
| `max_lanes` | 同上 | `max_type_states` 的兼容键；两者同时出现会拒绝配置 |
| `frozen_evidence_capacity` | 1024 | 最近失败证据环容量；覆盖旧样本时继续累计总数与覆盖数，不持有业务任务 |
| `max_inbound_tunnels` | 64 | 单个目标 slot 同时登记的严格与非严格入站盗洞总上限 |
| `idle_task_threshold` | 8 | 热点源选择、普通空闲目标和严格归还观察使用的逻辑任务阈值；已有同向借入可为后来出现的严格热点复用目标 |
| `strict_opportunity_attempts` | 2 | 每轮观察独立保留给严格候选的安装机会数，非严格流量不能占用 |
| `return_observations` | 3 | 严格盗洞发起归还前必须连续满足任务边界与低负载条件的观察次数 |
| `tunnel_lease_ms` | 2000 | 非严格盗洞没有真实发布或消费进展时允许保持开放的最长时间 |
| `load_observer_interval_ms` | 1000 | 集中 observer 扫描全部 slot 负载的间隔，不按类型数量全表扫描 |
| `control_tick_ms` | 1 | 活动严格迁移、归还、Moving 审计和停止推进的快速控制间隔；完全空闲 slot 不按该间隔轮询 |
| `transition_timeout_ms` | 5000 | 单笔物理移动允许保持 Moving 的最长时间；超时关闭最小故障域 |
| `shutdown_timeout_ms` | 2000 | Application 为本 Runner 申请的总收口预算上限，仍受应用剩余绝对期限约束；前半尝试无损排空，允许升级时后半用于有损收口 |
| `drain_batch` | 64 | worker 单轮从主队列、控制队列和各盗洞方向处理的最大批量，限制单方向垄断 |
| `critical` | `true` | true 时该 Runner 的 `Degraded` 或 `Failed` 触发 Application 统一停机；false 时只影响自身 readiness |
| `force_on_timeout` | `true` | true 时无损预算耗尽后显式升级 `force_stop`；false 时保留未收敛错误且不隐式中止任务 |

扁平单 Runner 形态把上述 Runner 字段直接放在 `partition` 下，并固定名称为 `default`；它不能再包含
`default_runner` 或 `runners`。未知属性、零容量、非法时长、名称不合法、default 缺失或兼容键冲突
都会在创建 worker 之前拒绝候选配置。每个 Runner 的默认值彼此独立，覆盖一个名称不会改变其它名称。

需要由代码计算容量时可在 UserHook 按名称提交计划；同一个名称只能由 YAML 或 UserHook 一方提供：

```rust
use std::time::Duration;
use nasa::application::PartitionApplicationPlan;

#[nasa::application("partition", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let default = PartitionApplicationPlan::new(16, 1_024, 16_384)?
        .with_max_lanes(4_096)?
        .with_shutdown_timeout(Duration::from_secs(5))?;
    app.configure_partition(default)?;

    let settlement = PartitionApplicationPlan::new(8, 2_048, 8_192)?
        .with_critical(false)
        .with_force_on_timeout(false);
    app.configure_partition_runner("settlement", settlement)?;
    Ok(())
}
```

`configure_partition*` 只登记纯参数。UserHook 返回后，Prepare 才创建、启动并发布 Runner，因此不能在
同一 Hook 中立即调用 `app.partition()` 或 `app.partition_runner(name)`。Initializer 位于 Prepare 之后，
可以通过 `InitializationContext::resource` 或 `named_resource` 取得已发布句柄；Ready 后的 handler、
受监督任务和其它业务代码也可以查询：

```rust
async fn submit_settlement(order_id: u64) -> anyhow::Result<()> {
    let app = nasa::Application::try_global()?;
    let settlement = app.partition_runner("settlement")?;
    settlement.submit(order_id, || async move {
        // 该任务进入 settlement Runner 的独立容量与顺序域。
    })?;
    Ok(())
}
```

受管模式要求全部 Runner 名称在 Service UserHook 结束前确定。若名称或容量只能在 Application Running
期间根据租户注册、业务事件或请求参数决定，应直接使用 `napart::PartitionRunnerRegistry` 动态创建，
并由业务保持单一进程级注册表、限制名称总数以及显式执行 `stop`、`force_stop` 或 `stop_all`。直接模式
不会自动加入 Application 的 readiness 和停机链。

每个 Runner 有独立 readiness。关键 Runner 进入 `Degraded` 或 `Failed` 会触发统一停机；非关键 Runner
只把自己的 readiness 置为 NotReady，其它执行域继续服务。停机先同时关闭全部入口，再按启动反序共享
Application 的绝对期限：计划允许时，无损预算耗尽后才显式升级有损收口。`frozen` 或 `aborted` 非零
会进入停机失败并保留有界证据。业务句柄的 Drop 不代表退出证明；确定停机只属于 Application 的
生命周期 action。

## OTLP trace 与指标

`"telemetry"` 组件默认只把 span 写入结构化日志，OTLP 指标默认关闭。显式配置两个
endpoint 后，trace 和 metrics 使用相同的 `service.name` / `service.instance.id` resource、
JSON 或 protobuf 编码和 Application 停机剩余预算：

```yaml
telemetry:
  enabled: true
  service_name: order-service
  service_instance_id: order-service-az1-01
  queue_capacity: 2048
  otlp_endpoint: http://127.0.0.1:4318/v1/traces
  otlp_metrics_endpoint: http://127.0.0.1:4318/v1/metrics
  otlp_encoding: protobuf
  metrics_interval_ms: 10000
  root_sample_ratio: 1.0
```

`service_instance_id` 留空时会从进程 ID 与 Application 启动时刻生成本进程稳定值。
`metrics_interval_ms` 在启用指标出口时必须为 `1000..=300000`。Counter 使用 cumulative
temporality，Gauge 发当前快照，Histogram 直接复用 Prometheus descriptor 的边界。

Web 对合法上游 `traceparent` 严格继承 sampled 位；没有上游时由 `root_sample_ratio` 唯一裁决。
只声明 `"web"`、没有声明 `"telemetry"` 时，入口仍传播 trace 上下文，但新根固定为未采样，
不会替下游擅自开启记录。Application 同时声明 `"telemetry"` 与 `"scheduling"` 时，取得实际
执行权的调度运行会导出新根 span，并携带稳定任务名和名义触发时刻；leader/claim 拒绝不产生该 span。

Prometheus `/metrics` 与 OTLP 从同一 `MetricHub` 结构化快照读取，包括原生指标、
naweb、nafana、Outbox 和 Saga Streams；naweb 在 Ready 期按全部静态 route 形状预留最坏序列，
每个 histogram 完整计入有限 bucket、正无穷 bucket、sum 与 count，容量不足时源与 descriptor
都不发布。两个出口并存且都不清零计数。单次失败只将
`telemetry:metrics-exporter` 降级，不重试当前快照、不背压业务；停机时在全局剩余预算内
尝试最后一次快照。`app.otlp_metrics_snapshot()` 只暴露批次、已确认样本和失败数，
不暴露 endpoint 或 label 值。

结构化兼容源当前无样本时仍保持同一快照路径，不会改读自身文本入口。单个 label 值最多 4096 个
UTF-8 字节；超过资源上限或与 descriptor 形状不一致的样本会从 Prometheus 与 OTLP 一致拒绝，并以
`nametrics_samples_rejected_total{source,reason}` 累计，诊断 label 不包含业务输入。

Outbox 的 `pending`、`dead` 与逐 lane `pending` 来自数据库已提交事实，每次完整刷新需要执行
`2 + lane 数` 条计数查询。Prometheus 与 OTLP 共享同一串行缓存，最多每 30 秒刷新一次，因而
`metrics_interval_ms` 只控制 OTLP 导出频率，不会同比放大数据库查询频率；这些 gauge 正常情况下
最多滞后 30 秒。刷新失败会保留上一份完整快照并每秒重试，通过
`napp_outbox_metrics_refresh_failed`、`napp_outbox_metrics_refresh_failures_total` 和
`napp_outbox_metrics_snapshot_age_seconds` 明确其可信边界；Prometheus 继续返回其它独立指标族，
OTLP 继续发送完整缓存并把 exporter 标为 Degraded。停机在数据库释放前强制刷新一次，最终 OTLP
flush 只读取缓存。

## Inbox 去重标记保留

Inbox claim 是事务内原语，没有独立组件字符串；MySQL 使用 `inbox` feature，PostgreSQL 使用
`inbox-pgsql`。长期运行的服务可在 UserHook 调用 `configure_inbox_retention`，把 datasource、消费
命名空间、最大重投视界、最小保留年龄、单轮预算与 fixed-delay 间隔冻结到 Application：

```rust
use std::time::Duration;
use nasa::application::InboxRetentionPlan;
use nasa::inbox::InboxRetentionPolicy;

#[nasa::application("db")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let policy = InboxRetentionPolicy {
        redelivery_horizon_ms: 86_400_000,
        processed_min_age_ms: 86_400_000,
        batch_limit: 500,
        round_time_budget_ms: 2_000,
    };
    app.configure_inbox_retention(InboxRetentionPlan::new(
        "default",
        "order-projection",
        policy,
        Duration::from_secs(30),
    )?).await?;
    Ok(())
}
```

Application 在 Ready 后立即执行首轮，随后从上一轮完整结束时计算下一次延迟，因此慢轮次不会重叠或
积累补跑债务。同一 datasource 与 consumer 组合只能登记一次；不同 datasource 上的同名 consumer
属于独立去重集合。协作停机不再开启新轮次并等待当前轮次，若全局停机截止时间先到，adapter 会关闭
锁状态未知的物理连接，不把该 session 放回池。

`napp_inbox_retention_rounds_total`、`deleted_total`、`claim_contended_total`、
`budget_exhausted_total`、`failed_rounds_total` 和 `oldest_candidate_age_ms` 均为无标签聚合指标，避免
datasource、consumer 或 message id 扩张序列数。`inbox_retention_snapshot()` 提供相同进程账目。
Application 不推断消息源的重投视界，也不自动创建或在线变更生产索引；表结构和
`(consumer_name, processed_at)` 保留索引必须由对应 datasource migration 管理。

## Saga 受管模式

声明 saga 会隐式加入 DB 与 Outbox 生命周期。saga.role 是部署取得权限的唯一开关，必须显式选择
orchestrator、participant、client 或 combined；managed 是默认计划模式，业务调用 configure_saga
只允许用于明确的 custom 模式。角色不是根据链接到二进制的 descriptor、Web 组件或端口形态推断的。

| 角色 | 本地持久资源 | 对外能力 | 不会取得的权限 |
| --- | --- | --- | --- |
| orchestrator | instance、journal、result Inbox、command Outbox、timer、Catalog、审计与配额 | start/query/audit/管理、result、Definition Registry、metrics | participant gate 与业务 handler |
| participant | command Inbox、gate、业务事实与 result Outbox | command 入口、capability 自动续租 | 全局实例、timer 与协调管理面 |
| client direct | 无 Saga 表 | 受管远程 start/query client | Orchestrator、participant 与本地 Outbox |
| client reliable | 本地 start-intent Outbox | 事务内 enqueue_start 与远程 query | Orchestrator、participant |
| combined | 两个数据角色的并集 | 两侧入口 | 未经 allow_combined_role 批准时不能启动 |

### 零装配入口

HTTP Orchestrator 的业务入口可以为空；Application 从配置、链接期 workflow artifact 与共享 Catalog
构造运行时，业务 main 不创建 DefinitionRegistry、Orchestrator、SagaApplicationPlan、Router、签名器
或 dispatcher。

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
    signing_keys:
      checkout-owner-key: checkout-owner-public-key
  http:
    base_path: /_nasa/saga
  api:
    page_token_key_ref: saga-api-page-token
    http:
      enabled: true
      expose_admin: true
      expose_definition_registry: true
      authorization_policy_ref: checkout-saga-http-rbac
      callers:
        order-api:
          credential_ref: saga-order-client
          tenants: [system]
          permissions: [start, read]
        checkout-workflow-owner:
          credential_ref: saga-definition-publisher
          tenants: [system]
          workflows: [order_checkout]
          permissions: [registry]
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

动态 Catalog 允许空定义启动；未知或未激活的 workflow 仍确定性拒绝。流程所有者通过
`#[nasa::saga_workflow]` 提供完整 definition；`napp` 使用 `registry_client` 指定的 Ed25519 私钥对
canonical artifact 签名并自动发布。Orchestrator 从 `signing_keys` 引用的独立公钥验签，不信任请求
自带密钥。参与方通过 `managed=true` 的 `#[saga]` descriptor 自动登记并续租 capability。相同
definition key 只有在 owner、canonical artifact、digest 与 seal 全部相同时幂等，任一事实不同都冲突；
已创建实例始终冻结自己的 definition version 与 digest。HTTP/gRPC capability 登记在组件已启动且未停止时
开放，参与方租约自然到期或编排端重启后仍可续租；该入口始终保留身份、租户、workflow、地址政策和
协议认证中的 replay 检查。缺少 route 的 command 保留在 Outbox，Catalog 监督循环在全部 route 恢复前
保持业务摘流；成功登记本身不会开放新 start/claim。
definition deprecated 只关闭新实例准入；已提交 Start 的同摘要重放仍返回 Duplicate，不同摘要返回
Conflict，运行中与终态实例均适用。重复请求继续接受认证、租户权限和事务执行资格检查，可靠 client
可在首次回执丢失后凭 Duplicate 结清原 start-intent，不产生重复首步命令。
HTTP 参与方的显式 `advertised_endpoint` 在构造时规范化；未指定时从已绑定 Web listener 推导，
发布前统一验证完整 origin 和 Saga 路径合同。Catalog 在取得目录与能力行锁之后使用数据库时钟建立租约。
HTTP/gRPC 发布端同时检查收据期限和本地请求预算，整批能力以最早到期时间约束 Ready；续租阻塞或失败
不会延长已有就绪证据，全部能力重新取得有效收据后才恢复。
协调副本在取得 generation 行和 replica 行控制权之后建立确认租约；本地以单调期限约束
readiness 与业务入口。数据库调用阻塞不会延长资格，到期拒绝新业务，完整快照重新确认后才恢复。
依赖 Catalog 资格的 HTTP 业务入口在异步认证之后复验资格。HTTP/gRPC start 在使用 definition 快照前冻结本次操作的期限和
撤销标识，每次创建事务恢复执行及交给事务层提交前都重新检查；失权时通过事务层回滚已暂写的实例、
配额和 Outbox。续租不会延长旧操作期限，新确认也不会恢复已撤销的操作。COMMIT 发出后的迟到结果
仍按数据库提交收据或提交结果不明处理，调用方通过同一幂等请求核对，不能将其报告为确定回滚。
capability 的 route generation 由 Catalog 数据库在主键行锁内分配并随收据回传；publisher 吸收该值后
再复验 descriptor 摘要，因此主机时钟回拨和提交结果不确定都不会把稳定副本永久留在 NotReady。
具有 `registry` permission 的 caller 还必须用非空 `workflows` 声明精确 workflow 或 `*`；tenant 与
workflow 授权在读取任何 definition 或 capability 持久事实前共同复验。

Registry 的确定性参数、权限、存在性、前置条件和冲突分别映射为 HTTP 4xx 与对应 gRPC code；数据库
断连、超时或事务结果不明统一映射为 HTTP 503 / gRPC `UNAVAILABLE`，自动发布和 capability 续租可保留
相同请求重试。MySQL/PostgreSQL 编排角色在 schema 串行权威内迁移已识别的直接前代 Catalog，再逐项
复验 CHECK 表达式、时间默认行为、identity/collation 和索引合同；未知结构或租户归属歧义阻止 Ready。

workflow owner 或 participant/client 链接了本地 workflow 时，应配置独立控制面。私钥 secret 只供
发布端使用，HTTP HMAC 或 gRPC mTLS 凭据只证明在线主体，两者不能互相替代：

~~~yaml
saga:
  definition_catalog:
    mode: dynamic
    activation_policy: validated
    publish_tenants: [system]
    registry_client:
      protocol: http
      discovery_ref: http://127.0.0.1:38080/app/_nasa/saga
      credential_ref: saga-definition-publisher
      definition_signing_key_id: checkout-owner-key
      definition_signing_key_ref: checkout-owner-private-key
~~~

### HTTP 路径与安全边界

saga.http.base_path 是 Application context 内唯一 Saga 子路径，默认 /_nasa/saga。实际线上基础路径
严格等于 server.context_path 与 base_path 各拼接一次。base_path 不能是根、不能带尾斜杠、重复斜杠、
点段、percent encoding、query、fragment、模板或通配符。参与方 capability 发布逐实例 origin、
effective_saga_base_path 与 route generation，发送端不从 service name 猜路径，也不跟随重定向。

标准 Saga 路由由 Web 组件在保留前缀分支中装配，不进入业务 Router 的 middleware、fallback 或 nest。
业务尝试注册保留前缀会在封口时拒绝。启用 Saga HTTP 入站时，原始 `configure_router` 变换因作用域
不可验证而拒绝 Ready；手写端点使用 `configure_router_scoped("/ops", transform)` 等非根静态业务前缀。
transform 内写相对作用域的路径，例如 `/status` 对外成为 `/ops/status`；同前缀变换按顺序组合，
其 layer/fallback 只影响该前缀。所有内部路径都挂在作用域之下，作用域与 Saga 前缀相交或覆盖其父路径时拒绝 Ready。
启用 `server.health` 时，Saga 前缀不得遮蔽 `/healthz`、`/readyz`。统一观测选择 Web 抓取时，配置的
指标路径独立保留，不受 health 开关控制；未编入统一观测能力时，health 才联动保留 `/metrics`。
冲突在绑定 Web listener 前拒绝，未挂载的框架入口不占用路径。
自动 mapping 端点同时校验静态、参数与通配路径的交集。HTTP 请求使用实际规范路径和原始 body 做 HMAC，认证 producer、
tenant 权限、nonce claim、body/并发上限和收据裁决均由 Saga 分支负责。Committed 与 Duplicate 才能
推进 Outbox；连接失败、超时、限流、服务端故障或响应不明都保留原 event_id 重投。

definition activate/deprecate 的 HTTP body 必须包含 `expected_sha256`、`operation_id`、`reason`；管理写
body 包含 `operation_id`、`reason`，并可包含 `expected_state_version`、`expected_control_version`。
这些前置版本在持锁事务中复验，冲突请求不改变实例、审计或 Outbox；HTTP 与 gRPC 使用相同裁决。
初次读取时控制态就不允许动作属于前置条件失败；初次快照允许动作而提交 CAS 失去竞争属于可重试并发，
gRPC 分别返回 `FAILED_PRECONDITION` 与 `ABORTED`，客户端遇到后者应重新加载快照再裁决。
同一 `operation_id` 和相同动作参数的已提交请求会在 expected version 复验前命中持久幂等事实，因此
响应丢失后的原请求可直接重放；该分支也早于租户动作额度预留，不改变速率账本。新 operation 才预留
额度，并仍须命中事务内版本才能产生状态或 Outbox 副作用；任一步失败时预算与动作共同回滚。

HTTP audit 的签名 body 可携带 `page_size` 与上一页 `page_token`；gRPC `GetAuditTrail` 使用对应字段。
两者都返回按数据库全局 `audit_seq` 递增的统一 records 和可选 `next_page_token`，不按类别或内存数组
offset 定位。任何非空页都会返回末项 checkpoint；attempt 开始与终态变化分别形成不可变事件，运行中
追加的任意类别事实都位于既有游标之后。数据库按 Saga 隔离的提交 guard 约束并发事务序号可见顺序，
所有记录携带数据库生成的发生时间，历史超过 1000 条仍能继续读取。

### client 发起等级

direct client 不需要 database、datasources 或 outbox 配置。调用方生成稳定 saga_id、trigger_id、
definition_version 与 business_key，使用 SagaRemoteClient::start 和 get；同步结果不明时保持同一请求
重试。reliable_start 需要 client.datasource_ref，并在当前同源业务事务内调用 enqueue_start：业务事实
与 SagaStartIntent 同时提交，after-commit 只唤醒 dispatcher，不在业务请求线程执行远端网络调用。
可靠 client 的 dispatcher 固定绑定 `saga.client.datasource_ref`，不继承全局 Outbox 默认数据源。
若显式配置 `outbox.datasource_ref`，它必须与 client 数据源一致，否则启动配置校验失败，应用不会进入
Ready；省略该字段或只设置 Outbox 轮询预算不改变此绑定。MySQL 与 PostgreSQL 遵循相同约束。

| Outbox 配置 | 可靠 client 的扫描数据源 | 启动结果 |
| --- | --- | --- |
| 未设置 Outbox 段 | `saga.client.datasource_ref` | 继续执行其它 Ready 门禁 |
| 仅设置轮询、批量或超时预算 | `saga.client.datasource_ref` | 继续执行其它 Ready 门禁 |
| 显式数据源与 client 相同 | `saga.client.datasource_ref` | 继续执行其它 Ready 门禁 |
| 显式数据源与 client 不同 | 不发布 dispatcher | 配置校验失败，不开放业务流量 |

`enqueue_start` 返回稳定 `event_id` 表示已向当前事务追加意图，不表示外层事务已提交。调用方必须等
外层事务明确成功后才能返回本地已受理；业务事实和意图任一失败都应回滚整个事务。远端调用不在该
事务内执行，只有 `Committed` 或 `Duplicate` 收据允许 dispatcher 标记完成；超时、连接失败和收据
丢失都保留原事件重投。Ready 不是远端完成证明，应同时观测 `napp_outbox_pending`、
`napp_outbox_published_total` 和远端实例状态；确定性拒绝还需关注 `napp_outbox_dead`。

~~~yaml
saga:
  role: client
  plan_mode: managed
  client:
    service_identity: order-api
    orchestrator_identity: checkout-orchestrator
    orchestrator_discovery_ref: http://127.0.0.1:38080/app/_nasa/saga
    credential_ref: saga-order-client
    reliable_start: true
    datasource_ref: orders
~~~

### 共享事务域的业务事件发布

受管 Saga 和业务事件可以共用同一数据库 Outbox。业务在 UserHook 调用
`Application::register_outbox_publisher(datasource, aggregate_type, event_type, publisher)`
登记精确目标；Ready 将这些目标组合进该数据源已有计划，仅启动该计划的 dispatcher。
订单与 Outbox 事件仍须在同一数据源事务中提交。未匹配事件交由原计划处理；
以 `Saga` 开头的 aggregate type 和以 `saga.` 开头的 event type 保留给协议发布端。
重复目标、非法名称或没有对应计划的数据源会拒绝启动。

业务发布端继承该计划的顺序和毒丸策略，收到下游确认后才返回成功。
网络超时等未确认结果应返回 `OutboxPublishError::transient`，保留事件重试；
永久拒绝使用终态错误并由计划处理。额外业务目标不会创建第二个 dispatcher，
也不改变 claim、至少一次投递和保留策略的边界。

### 提交后即时投递

数据库明确提交后，Outbox 只发布按 driver、datasource_ref 与 lane 限定的有界可合并唤醒。对应
dispatcher 立即领取数据库中最早的有序前缀；唤醒不携带可替代数据库的事件事实，也不直接调用 HTTP、
Kafka、Redis Streams 或 gRPC。信号丢失、跨进程追加和进程恢复由 poll_interval_ms 周期扫描兜底；
瞬态失败继续遵守 error_backoff_ms，后续提交不能形成绕过退避的重试风暴。

### 受管协议边界

HTTP、gRPC、Kafka 与 Redis Streams command/result 数据面均由 `napp` 按角色构造。HTTP/gRPC 控制面
负责 start/query/audit/admin、definition 发布和 capability 租约；控制面与数据面可独立选择。gRPC
自动登记公开 Orchestrator、Definition Registry、result 或 command service，创建 generated client，
并以标准 health Check 作为 Ready 与动态 route 切换门禁。Kafka 以 owner topic 和 broker ACK 为前移
收据，`result_dlt_topic` 必须写成 `<result_topic>.{owner}<Kafka client DLT suffix>`；例如 suffix 为
`.DLT` 时使用 `saga.results.{owner}.DLT`。Redis Streams 使用同槽 stream、consumer group、
XAUTOCLAIM、HMAC keyring、XADD 收据和原子 DLT+XACK。

动态 gRPC command 路由保留同一步骤的多个合法 replica endpoint；每个目标都独立验证地址政策、
mTLS 和 deadline。health 逐成员并发检查，共享一轮绝对期限；只有明确 Serving 的成员参与轮询
分流，业务直接复用该成员完成探测的 channel。单个 NotServing、连接失败或超时不否定其它成员的健康证据，全部不服务时关闭新投递并拒绝
发布该步骤。后续 Catalog 轮次重新复验成员集合；恢复成员通过检查后才能重新参与投递。
逐请求凭据/发现刷新在首个 Serving 响应后，将其余探测的收集窗口限制为当时剩余预算的一半；
未完成成员留待后续复验，已确认成员保留实际投递及收据读取预算。
共享 channel 的重叠请求可复用等锁期间完成的同代健康结果，每个请求仍重新核对发现与凭据。
探测结束后再次核对成员和凭据，变化时重新探测；后续独立请求不沿用上轮健康结论。
刷新发布串行执行，等锁超时只结束该请求；刷新请求取消会释放探测，等待者可在自己的原期限内接续。
所有受管 gRPC client 的单次期限覆盖 channel 就绪、连接、响应头、流式正文与最终 trailers，
正文到达不会重置预算。调用者更短的 `grpc-timeout` 继续生效，派发前向远端传播剩余预算；到期返回
DeadlineExceeded，不把迟到或不完整的收据当作成功确认。每次连接建立也独立受配置上限约束。
投递结果不明时仍保留相同 Outbox event，参与方副本必须共享其角色要求的持久幂等与业务事实。
面向 Orchestrator 的 client、result 和 Registry `discovery_ref` 支持固定地址或 `saga.discovery` 中的
受信 Nacos 服务引用，每次调用绑定同一实例的身份、origin、有效路径和代次。没有合法目标时保留原事件重试。
HTTP HMAC 和 gRPC mTLS 随配置视图一次发布，`saga.credential_overlap_ms` 指定旧材料接受窗口；入站、
出站、capability 续租和 Catalog 执行资格共同读取当前安全合同，非法候选保留上一份有效配置。动态 principal
使用 `secret://certificate_ref`，既有连接在每个 RPC 上也要复验；静态 `sha256:...` 保持固定身份。
完整发现配置、证书轮换和多副本收敛边界见
[Saga 生产指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/docs/saga-production.md)。

业务通过 `SagaHandle::orchestrator` 或 `pgsql_orchestrator` 取得 `SagaOrchestratorHandle`，可发起、查询、
暂停实例和读取运行指标；每次业务调用复验组件资格。句柄不暴露底层 Arc、registry 替换、timer 领取或
fencing 控制。独立宿主仍可显式构造 core Orchestrator，并自行承担其控制面权限。

HTTP start 的 `payload` 与 gRPC `SagaPayload` 都保留 `content_type`、`schema_id` 和精确 `body` 字节。
首步 definition、capability 和 handler 声明相同解释合同，schema 不匹配在业务执行前拒绝。摘要覆盖原始
字节与合同，HTTP/gRPC 重投共用相同幂等事实。旧 HTTP `input` JSON 入口保持可用，与 `payload` 互斥。

HTTP/gRPC 的权限、分页、start、管理动作和审计均由唯一内部 API 执行，支持创建时间过滤与跨协议 page token。
Registry 提供发布、读取、激活、废弃和退休；退休在同一事务检查全部保留实例、审计、Outbox 与有效租约，
不自动删除历史事实。`nasaga_stage_duration_seconds` 提供 handler、事务、transition 和 timer lateness 的
固定标签直方图，`napp_outbox_oldest_pending_age_seconds` 提供数据库时钟下的最老待投递 Saga 事件年龄。

Redis credential secret 是封闭 JSON。`signing_keys` 按逻辑服务身份提供当前发送 key，
`verification_keys` 按 key id 绑定认证身份；发布端不能只持有验签表，消费端也不能接受未知 key id：

~~~json
{
  "signing_keys": {
    "checkout-orchestrator": {"key_id": "orchestrator-current", "key_hex": "<至少三十二字节的十六进制密钥>"},
    "inventory-service": {"key_id": "inventory-current", "key_hex": "<至少三十二字节的十六进制密钥>"}
  },
  "verification_keys": {
    "orchestrator-current": {"service_identity": "checkout-orchestrator", "key_hex": "<对应十六进制密钥>"},
    "inventory-current": {"service_identity": "inventory-service", "key_hex": "<对应十六进制密钥>"}
  }
}
~~~

多 datasource participant 为每个 `participant.bindings.<name>.datasource_ref` 建立独立 runtime 和 Outbox
dispatcher；每个 `#[saga(binding = "...")]` descriptor 必须且只能命中一个 binding。任一协议、路由、
凭据、DLT、group、owner 或 datasource 不完整都会在 Ready 前拒绝，不会改走默认库或手工路径。

Application 的停止顺序先关闭 Saga 新调用与管理写入口，再停止 timer、Catalog/capability 循环和
dispatcher，最后由 Web、transport 与数据库组件反向释放资源。Saga 提供本地 ACID、至少一次投递、
Inbox 幂等、持久状态机和显式补偿，不提供跨服务 ACID、物理 exactly-once 或多个并发 Saga 的业务隔离。

### Outbox 租户配额

`OutboxApplicationPlan::with_tenant_quotas` 提交每租户在飞事件上限(进程级冻结,Ready 时安装,
冲突拒绝 Ready)。列出的租户在受信 append(`append_transactional_with_context`)事务内原子预留,
投递标记/死信裁决同语句(或同事务)释放;超限以稳定原因码 `outbox_tenant_quota_exceeded` 拒绝
且事件行从未写入。未列出的租户不记账不设限。**把某租户纳入配额前,该租户全部写入必须已改走
受信上下文入口**,否则释放路径造成账本漂移(由 `reconcile_outbox_tenant_quota` 在事务内有界对账收敛)。
拒绝计数经 `napp_outbox_tenant_quota_rejections_total` 导出,不带租户标签。

## Kafka 受管模式

单 client 使用 `kafka`，多 client 使用 `kafkas`；两根互斥，解析后立即归一成按 client name 排序的
同一张内部表。受管投影严格拒绝未知 client/container 字段，多 client 的 map key 是权威 client name：

```yaml
kafka:
  client_name: order-service
  bootstrap_servers: 127.0.0.1:9092
  group_id: order-worker
  producer:
    acks: all
  container:
    consumers: collected       # collected | disabled
    monitor_interval_ms: 500   # 100..=10000
    readiness:
      default:
        kind: joined            # joined | assigned | assigned_topics
      groups:
        order-worker:
          kind: assigned_topics
          topics: [orders]
```

```yaml
kafkas:
  orders:
    bootstrap_servers: kafka-a:9092
    group_id: order-worker
  audit:
    bootstrap_servers: kafka-b:9092
    container:
      consumers: disabled
      readiness:
        producer_probe_topic: audit-events
```

`collected` 会装入所有 `client` 与配置名一致的 `#[kafka_consumer]`，再按 Hook 登记顺序应用
`configure_kafka` 闭包；任何收集项指向未知或 disabled client 都会拒绝 Ready。默认 `joined` 允许竞争组
中已经 join 但暂时没有分区的 standby；必须拿到最少分区时用 `assigned + min_partitions`，必须覆盖指定
topic 时用 `assigned_topics + topics`。`disabled` 不启动 consumer，以集群 metadata 或显式
`producer_probe_topic` 作为 Ready 条件；进入运行期后仍按 `monitor_interval_ms` 低频复查，失败只让动态
readiness 变为 false，broker 恢复后自动恢复，不因一次瞬时探测直接终止进程。

```rust
#[nasa::application("log", "kafka", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_kafka("order-service", |registry| {
        registry.register(OrderConsumer::new())
    })?;

    let kafka = app.kafka("order-service")?;
    let lane = kafka.producer_lane("default")?;
    // Hook 中只做装配；正常业务发布应在整个 Application Ready 后由请求或受监督任务触发。
    app.register_named("order-producer", lane)?;
    Ok(())
}
```

`KafkaHandle` 提供 client lifecycle/readiness、`ProducerLane`、group health/assignment/position、
pause/resume、seek、动态 subscribe/unsubscribe、人工 restart，以及受管 admin 的 topic metadata、
`create_if_absent` 和显式删除能力；`KafkaAdmin::client_name()` 可复验管理操作的 client 归属。它不会
返回原始 `KafkaProxy`、consumer registry、`connect` 或
`shutdown`。即使业务仍持有 handle，容器停机仍先在 Ready 层停止 consumer，再让 UserTask/业务资源
利用仍开放的 producer/admin 做退出收尾，最后在 Start 层关闭发布准入、flush lane 和 admin；全部步骤
共用一个绝对停机 deadline。

Kafka 配置可以由 `nacos-config` 的初次 overlay 提供；运行期候选帧仍先做完整无副作用校验，非法帧保留
旧快照，合法但发生变化统一标记 `RestartRequired`，本版本不热切 broker、凭据、group 或 route。

### 业务项目复制后必须修改的 Kafka 项

复制配置或示例代码时至少核对下面六处；名称错位会在配置、收集或 Ready 阶段直接失败，不会静默漏消费：

1. `bootstrap_servers`：本地文件只放无敏感信息的默认值，部署环境用
   `APP__KAFKA__BOOTSTRAP_SERVERS` 覆盖真实集群地址。
2. `client_name`：必须与 `#[kafka_consumer(client = "...")]` 和 `app.kafka("...")` 使用同一个稳定名称；
   多 client 模式下还必须与 `kafkas.<name>` 的 map key 相同。
3. `group_id`：按业务消费语义设置。竞争消费实例使用同一 group；需要每实例都收到时使用 nafka 的
   broadcast group，不要靠随机修改普通 group 模拟广播。
4. `topics` 与 `event`：producer 的 topic/事件 header 必须和 consumer 路由完全一致；producer 不设置 event
   时只会命中 `DEFAULT` 路由。
5. `container.consumers`：存在属性或动态 consumer 时使用 `collected`；纯 producer 服务必须显式写
   `disabled`，并按需设置 `producer_probe_topic`。
6. Cargo feature 与组件顺序：启用 `application + kafka`，并把 `"kafka"` 放在 db/redis 之后、
   web/ws/nacos-discovery/scheduling 之前。

安全协议、用户名、密码、证书路径和原生 properties 仍使用 `KafkaConfig` 对应字段；这些值不要写进示例、
日志或管理端点。运行期配置变更只报告 `RestartRequired`，必须通过应用重启生效。

## Web HTTP listener 受管模式

同时启用 `application,web` feature 并声明 `#[nasa::application("web")]` 后，`"web"` 组件独占明文
TCP listener、协议判定、连接容量、路由服务图与停机排空。在这些前提成立时，业务只需设置
`server.http2.enabled=true` 即可接受 h2c，其余 HTTP/2 transport 字段均可省略。HTTP/2 不依赖
`grpc` 或其它 feature 的传递依赖：`enabled=false` 时只接受 HTTP/1.0/1.1；显式开启后，同一端口按
HTTP/2 固定前言接受 h2c prior knowledge，并继续兼容 HTTP/1。该 listener 不实现 `Upgrade: h2c`，
也不终止 TLS；需要 h2 over TLS 时应由具有明确证书和 ALPN 合同的上游代理终止 TLS，再按已配置协议
连接本 listener。

```yaml
server:
  host: 0.0.0.0
  port: 8080
  graceful_shutdown_timeout_ms: 10000
  max_inflight_requests: 1024
  http2:
    enabled: true
```

业务通常只配置 `enabled`。以下高级参数全部可省略，框架会采用已经过边界校验的固定默认值；只有连接
容量、报文规模或网络时延的实测结果要求不同边界时才覆盖：

| 可选字段 | 默认值 | 作用 |
| --- | ---: | --- |
| `max_connections` | `256` | 共享 listener 的 HTTP/1 与 h2c 连接总上限 |
| `connection_handshake_timeout_ms` | `10000` | 等待 h2c 固定前言的最长时间 |
| `max_connection_age_ms` | 未启用 | 主动连接轮转时长 |
| `max_concurrent_streams` | `128` | 单条 HTTP/2 连接的并发 stream 上限 |
| `initial_stream_window_size` | `65536` | 单 stream 初始流控窗口 |
| `initial_connection_window_size` | `1048576` | 单连接初始流控窗口 |
| `max_frame_size` | `16384` | HTTP/2 frame 最大字节数 |
| `max_header_list_size` | `16384` | 单次请求头列表最大字节数 |
| `header_table_size` | `4096` | HPACK 动态表最大字节数 |
| `max_send_buffer_size` | `65536` | 单 stream 发送缓冲上限 |
| `max_pending_accept_reset_streams` | `20` | 对端尚未确认的 reset stream 上限 |
| `max_local_error_reset_streams` | `20` | 本端因协议错误产生的 reset stream 上限 |
| `keep_alive_interval_ms` | `30000` | HTTP/2 PING 周期 |
| `keep_alive_timeout_ms` | `10000` | 等待 PING ACK 的时限 |

`max_connections` 在 HTTP/2 关闭时仍约束 HTTP/1 连接；容量耗尽的新连接在读取请求前关闭。HTTP/2 开启
后，stream、流控、frame/header/HPACK、发送缓冲、reset 和 PING 边界全部显式交给 hyper driver，
不采用随依赖组合变化的隐式默认值。`max_inflight_requests` 继续作为跨连接、跨协议的请求级总门禁，
两者不能互相替代。

启动校验采用与受管 gRPC listener 对齐的边界：连接数为 `1..=4096`，协议前言等待为
`1..=60000` 毫秒，并发 stream 为 `1..=65535`，stream 窗口为 `1..=1 MiB`，连接窗口为
`1..=16 MiB`，header list 为 `1..=64 KiB`，HPACK 表为 `1..=16 KiB`，发送缓冲为
`1..=1 MiB`，两类 reset 上限均为 `1..=1024`；`max_frame_size` 必须在 `16384..=65535`。
连接年龄若配置，必须在 1 分钟到 24 小时之间；两个 PING 时长必须大于零且不超过 365 天。协议和容量
设置在 Start 阶段冻结，运行期配置变化需要重启才能作用于新 listener。开启 HTTP/2 后，前言超时或
对端在判定完成前关闭会终止该连接，并累计连接级错误。

停机先关闭 accept，再通知存量 HTTP/1 连接停止 keep-alive、HTTP/2 连接发送 GOAWAY；两者共同消费
`graceful_shutdown_timeout_ms` 子预算，未配置时由 Application 全局停机预算兜底。子预算耗尽会终止
剩余连接任务，尚未完成的请求不再等待。

下列 transport 指标进入统一 MetricHub。编入 `observability` 时，通过 `grafana.observability`
显式配置独立/Web 抓取或 remote write；Web 抓取与 `server.health` 独立，默认关闭外部出口。
未编入该能力时，`server.health=true` 才提供兼容的 `<context_path>/metrics`。标签取值固定，
不包含地址、路由或客户端输入：

| 指标 | 语义 |
| --- | --- |
| `napp_web_requests_by_protocol_total{protocol="http1"}` / `napp_web_requests_by_protocol_total{protocol="http2"}` | 按实际请求版本累计进入路由的请求 |
| `napp_web_http2_requests_in_flight` | 当前仍在路由或响应 future 中的 HTTP/2 请求 |
| `napp_web_connections_accepted_total` | TCP accept 成功次数，包含随后因容量被拒的连接 |
| `napp_web_connections_rejected_total{reason="capacity"}` | 在协议判定前因连接容量耗尽而拒绝的连接 |
| `napp_web_connections_active` | 当前仍由 listener 管理的连接 |
| `napp_web_accept_errors_total` | 可恢复的 TCP accept 错误 |
| `napp_web_connection_errors_total` | 协议判定或 HTTP driver 的连接级错误 |

Web 请求体只处理有界缓冲 body 与显式声明的 streaming 响应：不支持 `multipart/form-data`
(文件上传走对象存储直传，Web 层只收上传凭证与元数据)，也不提供 SSE(`text/event-stream`)
的事件帧、心跳与 `Last-Event-ID` 续传语义(服务端推送使用受管 WebSocket)。

## route 授权与未命中缺省

业务在 UserHook 经 `set_authz_registry` 注入路由策略注册表、经 `set_object_authorizer` 注入对象
授权 provider 后，Web 装配统一的授权边界(认证之后、幂等与 handler 之前)。对象授权恒
fail-closed：provider 错误与超时都不降级为放行。

route 未命中任何策略时按三态缺省裁决，词表 `permit`/`observe`/`deny`：

- `permit`(兼容缺省)：放行；
- `observe`：放行，但累计计数并对每个 route 首次命中输出警示日志，供翻转前清点漏配面；
- `deny`：fail-closed 直接 `403`，与对象授权失败语义对齐。

配置入口二选一：UserHook `set_authz_unmatched_policy(...)` 或 YAML
`server.authz_unmatched_route`；两处同时出现且取值不一致时 Ready 期拒绝装配，不做静默优先级。

豁免与启动门禁：

- 声明公开(`auth_required=false`)的路由、已启用的框架探针与实际配置的指标路径不受
  `observe`/`deny` 收紧；该豁免随覆盖合同进入 registry 与单请求安全快照，Web、registry 便捷入口和
  handler 复用 `RequestSecurityContext` 时得到同一结果；未命中真实路由的请求交 router 兜底 `404`；
- 指标路径不借用业务 JWT；统一抓取配置的 bearer 鉴权仍独立执行，不能把策略豁免解释为无鉴权；
- 悬空策略(route_id 不指向任何有效路由)阻断 Ready；
- `deny` 下仍存在未命中策略的鉴权 route 时阻断 Ready——漏配在部署期显形，而不是上线后全量
  `403`；
- `observe`/`deny` 要求已注入策略注册表，否则 Ready 期拒绝装配。

`configure_router` 的动态路由默认游离在对账之外；确有对外合同的动态路由经
`register_route_contract` 显式提交后，与静态路由同等进入 OpenAPI、覆盖对账与豁免口径。
生成 OpenAPI 时静态和动态 path 都会带上实际 `context_path`，Axum catch-all 语法只在文档路径中
转换为 OpenAPI 模板。

Ready 安装的路由覆盖合同会继续约束运行期策略更新：`PolicyRegistry::reload` 保留当前三态缺省并
复验完整覆盖关系；需要同时改变策略与缺省时使用 `reload_with_unmatched`。任一候选含悬空策略，
或 deny 候选留下未覆盖鉴权 route，策略、缺省和 generation 都保持 last-good。

观测：覆盖账目与未命中计数进入统一指标目录(`napp_authz_unmatched_observed_total`、
`napp_authz_unmatched_denied_total` 两 counter 与 `napp_authz_routes_covered`、
`napp_authz_routes_uncovered` 两 gauge，仅在装配授权层时注册)，也可经
`unmatched_observed_total()` / `unmatched_denied_total()` 直接读取。

## gRPC listener 受管模式

`grpc` 把 generated service registry、单个 HTTP/2 listener、TLS、健康、反射、固定方法指标、服务
发现 metadata 与反向停机纳入 Application 的唯一所有权。业务保留 proto 与 handler 语义，在
UserHook 只登记统一 codegen 生成的 server；组件随后按固定顺序取得运行资源：

```text
UserHook 登记 generated service
  -> Prepare 永久封口 registry
  -> 全部 initializer 成功
  -> Ready 校验 descriptor 和方法策略
  -> 自动装配 Router、health、可选 reflection 与 TLS
  -> 预绑定 listener 并发布 Bound observer，准备发现 metadata，不 accept
  -> 全部任务工厂和最终检查成功，Application Ready 后统一放行
  -> gRPC 发布 Running / health Serving，随后注册发现 endpoint
  -> 停机先注销发现实例，再停止准入并在全局剩余预算内排空
```

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["application", "grpc"] }

[build-dependencies]
nagrpc-build = "2.0.0"
```

```rust
#[nasa::application("grpc")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register_grpc_service(
        proto::order_service_server::OrderServiceServer::new(OrderApi),
    )?;
    Ok(())
}
```

受管模式读取固定 `grpc` 根；字段、默认值和硬上限见
[nagrpc README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/nagrpc/README.md#application-受管模式)。
缺少业务 service、重复或晚到登记、ABI/descriptor 冲突、未知方法策略、非法 TLS/容量配置或端口绑定
失败都会阻止 Ready，且不会留下监听任务。显式 `grpc.health_only: true` 可启动只有标准 health 的
基础设施 listener。`app.grpc()` 只返回 `GrpcServerObserver`，业务不能越过组件直接 shutdown。

观察句柄存在不等于已接流：`Bound` 仅表示 socket 所有权已取得；此时 TCP backlog 可能完成连接，
但没有 accept、TLS 握手或 RPC 响应。`grpc:listener` 在激活确认前保持 NotReady。
服务发现也等待统一放行以及 gRPC Running 后才登记，注册失败或超过原启动 deadline 会触发关键任务
失败停机；确认之前 `app.is_ready()` 与 `/readyz` 不可用，即使生命周期状态已为 Ready。

listener 成功发布后，Application 指标目录提供 `napp_grpc_serving`、连接/TLS 事实，以及由 sealed
descriptor 固定 label 的 active、started、rejected、completed RPC 指标。`rejected` 以封闭 `reason`
区分连接并发、进程并发、方法并发、方法速率和身份门禁；stream idle、总时长与双方向累计上限也使用
各自完成结局，不与 unary timeout 或通用 `ResourceExhausted` 混合。持续 accept 失败会先把
`grpc:listener` 摘为 NotReady，serve task 仍持有 socket、有界退避并做本机恢复探测；成功重新接流后
恢复 Ready。serve 所有权丢失或进入 `Failed` 会作为关键任务失败触发统一停机，不能仅靠业务 health
service 的状态替代 listener readiness。

reflection 默认关闭；显式开启时由 sealed generated service registry 自动形成 full-name allowlist，
业务不配置 reflection builder 或重复登记 descriptor。`ListServices`、symbol 查询和实际 Router 使用同一
service 集合；`health_only + reflection` 在绑定端口前拒绝。停机连接使用整个 gRPC drain 预算，首请求、
空闲与连接年龄驱逐才使用较短的逐连接 grace。

Prepare 会按实际业务方法、自动 health/reflection 方法、五种拒绝原因和 27 种完成结局计算最坏公开
序列数。gRPC 子预算为 20,000，并通过 `nametrics-core` 与全进程 100,000 序列预算原子提交；任一预算
不足时在 bind 前失败，descriptor、指标源和预留计数都不产生半份状态。

非 loopback 明文默认拒绝；TLS/mTLS 只从 `secret://` locator 读取同代材料，证书与密钥在 bind 前
复验。`methods.<完整RPC路径>` 可要求验证过的 `PeerIdentity`，并设置方法并发与 token bucket；策略
只能收紧全局上限，未知路径直接阻止启动。该组件只托管一个 listener；Saga 结合 `nacos-config`
启用完整安全快照时，新握手使用当前证书，旧 client CA 仅在有界重叠窗口内受信。通用业务 client
仍由调用方拥有；Saga 的受管连接池与发现由 Saga 组件装配。需要独立 listener 所有权时使用
`nasa::grpc::ServerPlan`，不要同时声明 `"grpc"` 组件。

## 业务 initializer

initializer 用于在 migration 和出站依赖已就绪、入站监听和消费循环尚未启动时，完成动态路由、
注册表、恢复、回填和预热。静态属性入口与 Service UserHook 中的运行时入口会合并为同一个冻结计划：

```rust
use nasa::application::{
    ApplicationFuture, Initialization, InitializationContext, InitializerSpec,
};

#[derive(Default)]
struct RoutesInitialization;

#[nasa::initializer(
    name = "routes",
    order = 100,
    requires = ["schema"],
    kind = "one-shot",
)]
impl Initialization for RoutesInitialization {
    fn initialize<'a>(
        &'a mut self,
        context: &'a mut InitializationContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let routes = build_routes(context.config())?;
            context.register_resource(None, routes)?;
            Ok(())
        })
    }
}

// Service UserHook 内也可以登记已构造的实例。
app.register_initializer(
    InitializerSpec::new("tenant-cache")
        .order(200)
        .requires(["routes"]),
    TenantCacheInitialization::new(),
)?;
```

`name` 和 `requires` 使用 canonical 名称；依赖边始终优先于 `order`，无依赖冲突时按
`(order, name)` 稳定裁决。条件工厂返回 `None` 表示本项未启用；其它项仍依赖它时启动失败，
不会静默删边。实际启用项严格串行执行三轮全局屏障：全部 `before`，再全部 `initialize`，
最后全部 `after`。

属性入口可省略 `name` 和 `order`。宏挂在完整 trait impl 上，因此默认 `name` 从实现类型名转换为
canonical kebab-case，例如 `RoutesInitialization` 得到 `routes-initialization`；类型无法稳定转换时必须
显式声明。派生名称同样是依赖、日志和指标 label 使用的稳定业务身份；重命名实现类型会改变该身份，
需要跨发布保持依赖引用和观测连续性时应显式声明 `name`。默认 `order` 为 `100000`。运行时入口没有可供推导的实现项，仍通过
`InitializerSpec::new(name)` 显式给出名称，其默认 `order` 同样为 `100000`。

`one-shot` 只做有界初始化。`hosted` 只允许 Service，可通过 `register_readiness`、
`stage_background` 和 `stage_critical` 暂存长期能力；任务在所有 initializer、Seal 和组件 Ready action
成功后构造并交给 Supervisor，全部工厂和最终检查通过后，主体与组件任务在 Application Ready 后统一放行。
业务自管 listener 应使用
`app.serve_when_ready(...)`，这会保证启动失败或停机时根本不调用 listener 工厂。

initializer 失败、panic、超时或取消都会停止后续阶段，不发布 Ready，并严格逆序停止任务、
撤销 action 和关闭受管资源。已提交的 DB/Redis/Kafka 外部事实无法由本地回滚，因此实现必须可安全重跑：
单库多步写使用事务，跨资源事实使用稳定幂等键或与 Outbox 同事务提交。

## 业务优雅停机任务

业务需要在受监督任务收口后执行一次性的异步 close、flush、归还或注销时，可以在 Service 或 Batch 的
UserHook 中调用 `Application::register_graceful_shutdown`。登记成功后，future 的所有权交给当前
`Application`，业务不再监听进程信号或保存停机 callback 集合：

```rust
let consumer = consumer.clone();
app.register_graceful_shutdown(100, "orders-consumer", async move {
    consumer.stop_and_drain().await
})?;

let client = client.clone();
app.register_graceful_shutdown(200, "orders-client", async move {
    client.close().await
})?;
```

`priority` 只决定业务任务集合内部的顺序，数值越小越早执行；同一优先级按登记顺序执行。任务名在
当前 Application 内唯一，首尾空白会去除，名称必须非空、不含控制字符、不可见格式控制、URL、地址、凭据语义 token 或明显动态身份，且不超过
128 个 UTF-8 字节；单个 Application 最多登记 256 项。任务可以返回 `()` 或 `Result<(), E>`，其中
`E: Into<anyhow::Error>`。
凭据按 camelCase、acronym、常见分隔符和明确凭据后缀识别，不受业务前缀影响；普通复合词和操作词不依赖白名单。
全角及其它可兼容分解为 ASCII 字母数字的字形使用同一比较规则；名称拒绝 Unicode Format 类、组合字形连接符和
变体选择符，包括零宽及双向格式控制；Unicode 行、段分隔符也不能进入单行诊断身份。
正常可见的多语言业务名称和普通组合音标仍可使用。
门禁拒绝点分四段 IPv4 形态（每段十进制值不超过 255，允许前导零）及完整可解析的 IPv6 候选，
不从普通单词中截取十六进制子串，因此 `cache::flush` 等 namespace 名称可用。带值的独立
`id`、`uid`、`uuid` 字段同样拒绝；词边界处大写 `ID`、`UID`、`UUID` 直接拼接小写 ASCII 字母、数字
或非 ASCII 字母数字值采用保守拒绝策略（如 `requestIDabc`、`requestIDαβ`）。`Id`、`Uid`、`Uuid`
直接拼接数字或非 ASCII 字母数字值也会拒绝，但不从 `Identity` 等普通 ASCII 单词中猜测身份字段。
缩写组合与该保守规则重合时应使用分隔符表达稳定名称，例如 `client-ui-driver`，而不是二义性的
`clientUIDriver`；运行时无法区分后者的 `UI + Driver` 与 `UID + river`。连续四位及以上 Unicode 十进制数字（含混合书写系统）、UUID、ULID、
连续 16 位十六进制串，以及长度至少 16 且含至少四个数字的字母数字混合段同样拒绝。短固定编号可以使用。
这些形态检查不能证明名称的业务基数；调用方仍必须使用固定业务名称，不得拼接租户、请求、对象身份或秘密，
包括形态上无法与普通单词区分的纯字母标识。
UserHook 结束、Seal、Ready 或停机开始后登记都会失败。

Service 停机时，运行时先让入口变为 NotReady、关闭 Ready action、收口受监督任务并撤销 initializer action，
再按优先级执行业务停机任务，之后才释放 UserHook 登记的业务资源和更早启动的组件资源。
Batch 的静态初始化早于工作负载：收口受监督任务后，先执行业务停机任务、释放业务资源，再撤销静态
initializer action 与其资源，最后清理更早启动的组件；不会套用 Service 的 initializer 清理位置。
两种模式都只允许查找尚未撤销的资源，不得假设 Service 的 initializer 资源仍可用于业务收尾。所有任务共享
`application.shutdown_timeout_ms` 的绝对期限；任务组会为后续资源清理预留尾部，组内按剩余时间和未执行项数
分配公平份额。单项失败、超时或 panic 不截断后续任务，也不覆盖更早的 primary error；期限耗尽时未开始的
任务标为 `Abandoned` 并释放其 future。任务必须是可让出执行权的异步操作，不能包含阻塞 I/O、无界 CPU
循环或阻塞式析构。取消和放弃时的单次析构 panic 也会被隔离；已尝试项计入 `Panicked`，未开始项仍计入
`Abandoned` 并附加析构错误。panic payload 正文不会被读取，首次异常对象在独立的展开边界内尝试析构；
只有该析构再次 panic 时才保留第二个异常对象及其持有的资源，避免无限展开。
从登记接管开始，future 在注册表、待执行列表和当前执行项中始终保留一次性析构隔离。
Runner 被释放时先关闭公共登记门，再随业务停机步骤于锁外清理注册表；后续释放受已接管任务 future
的存活约束，不取决于 Application 副本数量。析构中的重入登记会被拒绝。
直接取消并释放 Runner 不等同于请求优雅停机，不保证执行异步任务主体或产生退出报告；
执行器会同步关闭登记，将尚处于 Starting/Ready 的实例置为 Stopping，再沿实际激活栈逆序释放剩余
action、停机任务与各自所属资源。任务门已出栈但仍在等待时也保留约束：若受监督 future 尚未析构，
执行器请求 abort，并把剩余栈和组件所有权交给任务存活守卫；最后一个 future 析构后才同步释放尾部。
此时新资源借用和全局入口立即撤销，不等待任务退出，也不创建额外异步清理任务；尾部析构不能再发起
新资源借用。若没有存活任务，则在逆序释放后关闭资源表并撤销全局入口。Service 的 initializer
先于业务停机任务释放，Batch 的静态 initializer 晚于业务资源释放；两种模式均保持停机任务先于业务资源。
已有 Stopped/Failed 保持不变；Stopping 表示异步收尾结果未获确认。
保留的 Application 副本不能发起新的资源借用，也不会继续占用全局槽；已借出的资源随借用归还释放，
此前复制的外部客户端句柄不在同步撤销范围内。
不让出执行权的任务会延迟 abort 及依赖释放；执行器不会为了提前归还资源而破坏仍存活任务的依赖顺序。
因所有权释放而发生的任务析构异常只同步输出固定告警，不阻止其它已接管任务释放，不伪造或追改摘要。
同步阻塞、`panic=abort` 以及同一次展开中的再次 panic 不在可隔离范围内。
返回错误对象的 `Display`、`source` 和 `Drop` 也在独立展开边界内调用。错误链重复节点或超过 32 层时停止展开，
累计保留的正文最多 16 KiB；格式化超出剩余接收预算或未完整返回时，正文被丢弃并替换为稳定分类。
同步错误报告先脱敏，再将正文的控制字符、Unicode 行/段分隔符及不可见格式字符编码为可见转义；
反斜杠也转义，普通多语言可见文本和错误链分隔信息保留。框架只追加一个末尾 LF，单条报告连同固定前缀
最多 2 KiB，截断不拆开 UTF-8 字符或转义。采集端应按物理行的固定前缀识别报告，不把正文内的同名文字当成摘要。
Authorization 的 Basic/Bearer 引号凭据和 Digest 参数列表整体隐藏，内部引号与参数逗号不会提前终止脱敏。
边界为外层 JSON 字符串或当前 header 行；Digest 的扩展参数与同一行尾部字段无法可靠区分时一并隐藏，
需要保留的公开诊断应放在独立字段或下一行。
独立 PEM 私钥块也会整体隐藏，不要求外层字段前缀；明确私钥缺少匹配结束标记时保守隐藏剩余正文，
普通 PEM 证书与公钥不因块标记被隐藏。
错误对象在所有者释放时逐项隔离析构，晚于 shutdown summary 的异常输出 `application error release warning`，
不追改任务结果计数、首次原因或退出码。错误回调必须在有限时间内返回；同步死循环、阻塞或忽略格式化写入错误的
无限输出无法由异步 deadline 抢占，仍需进程级强制终止兜底。

长期运行的循环继续使用 `spawn_critical`、`spawn_background` 或 `serve_when_ready`；需要运行期借用和
明确资源所有权的对象继续使用 `register_managed`。同一个对象只能有一个最终关闭 owner，业务停机任务也
不能关闭由数据库、Redis、Kafka、Web、gRPC 或日志组件拥有的底层对象。

## 生命周期要点

- `zcf/application.yml` 必须存在（内容可为 `{}`）；整个 `application.*`（name/mode/worker_threads/超时）是 bootstrap-only，远端首拉改写拒绝启动，运行期改写只记 `RestartRequired`。
- `mode: auto | service | batch`：auto 在声明 saga/kafka/outbox/web/ws/nacos-discovery/scheduling 任一长生命周期组件，或收集到静态 `hosted` initializer 时解析为 Service，否则 Batch；显式 Batch 不允许这些长生命周期能力。
- Service 启动顺序为 `Bootstrap -> Start -> UserHook -> InitializerFreeze -> Prepare -> Initialization -> Seal -> Ready`；全部阶段共用 `application.startup_timeout_ms` 形成的一个绝对 deadline。
- 信号：broker ready 先于任何异步组件；Service 首次 Ctrl-C/SIGTERM 优雅停机退 0，Batch 未完成被取消退 128+signo；Stopping 中再次收到信号立即强退。
- 停机按 active stack 严格反序，所有清理共享 `application.shutdown_timeout_ms` 一个绝对预算；Runner 会在每个 `ShutdownAction` 外层派生提前截止预算，防止单个扩展耗尽后续清理时间。action 内部需要为自身报告或补偿继续细分预算时使用公开的 `ShutdownContext::child_budget`，不能直接把全部 `remaining()` 交给可能用满预算的子操作。启动失败沿同一条回滚链，primary 错误不被回滚错误覆盖。每个已尝试步骤产生带递增序号、固定类型、稳定归属、耗时和失败增量的 `debug` 事件；清理结束后由不依赖日志组件的同步诊断通道输出一次有界摘要，包含各类步骤计数、任务 abort、deadline、放弃步骤与总耗时。
- 组件身份 `id` 和静态 `dependencies` 在任何组件阶段之前统一受保护地读取一次；排序、配置投影和各阶段上下文只使用冻结值。元数据方法必须无副作用且有限时间返回。读取展开成为 Bootstrap 错误，不启动任何组件阶段；阶段执行后不再读取动态元数据，已登记补偿仍由统一异步回滚负责。
- 组件 Bootstrap、Start、Prepare、Ready 的方法调用和 future 轮询发生单次展开式 panic 时，按组件与当前阶段报告固定错误，并沿 active stack 执行异步回滚。组件与 initializer 的启动 future、initializer 实例、暂存终端任务和任务工厂均在接管时建立释放保护；完成、超时、取消、信号或部分移交失败时，析构展开不越过回滚边界。实例从运行时登记或静态工厂产出起保持保护，拓扑排序只借用元数据；三轮结束及失败出口逐项释放实例后才清理其依赖。已有失败或停止原因保持不变，次要释放异常单独报告；成功结果后的释放异常阻止启动。异常 payload 的析构在独立边界处理，不输出正文。同步阻塞、`panic=abort` 与同一次展开中的再次 panic 无法安全抢占。
- `ApplicationComponent` 对象从登记起受析构隔离保护，在清理栈和资源收口后逐项释放；尚未退出的受管任务保留组件所有权直到实际退出。批任务完成时的组件释放异常返回稳定的 Stopping 错误；已有主失败、信号或主动停止原因保持不变，释放异常进入次要清理报告。直接取消及延迟释放路径仅同步告警，不追改已交付结果。

- 配置热刷新：整帧校验失败保留旧快照；可热刷组件（当前 log）成功记 `Applied`、失败保留 last-known-good 记 `ApplyFailed`；其余组件的段变化或依赖材料变化如实记 `RestartRequired`，保留最后实际生效版本。材料依赖识别完整合法的 `secret://id` 与既有裸 ID 引用，file/env/provider 内容改变即使配置树不变也参与判断；材料恢复到实际使用值后可重新记 `Applied`。命名 HTTP 客户端材料随视图发布，初始及后续成功版本记在 `Managed("http_clients")`；准备失败保留旧视图，名称集合变化拒绝整帧。`app.config_view()` 保证快照与状态表同版本。
- 错误报告有界展开错误链并统一脱敏（URI userinfo、常见敏感键）；敏感键比较忽略不可见格式控制并识别 ASCII 字母数字兼容字形，替换坐标保持原文。兼容标点不产生新的值结束边界；这不是任意视觉混淆字符检测。进程级 panic hook 只写受控 location marker，不读 payload。

## 业务扩展点

| 入口 | 开放窗口 | 用途 |
| --- | --- | --- |
| `app.register / register_named / register_managed` | 启动 Hook | 把业务资源所有权交给容器；运行期只读借用 `app.resource::<T>()` |
| `app.register_graceful_shutdown` | 启动 Hook | 登记一次性业务异步收尾；在受监督任务之后、业务资源之前按 priority 执行 |
| `app.spawn_critical / spawn_background` | 启动 Hook | 登记后即可受监督执行，不隐式等待 Ready；critical 提前退出触发失败停机 |
| `app.serve_when_ready` | Service 启动 Hook | 登记只在 Application Ready 后才构造与 poll 的自管 listener |
| `app.register_initializer` | Service 启动 Hook | 登记运行时 initializer，与 `#[nasa::initializer]` 静态项合并冻结 |
| `app.configure_router(...)` | 启动 Hook | 手写路由和全局业务中间件；启用 Saga HTTP 入站时拒绝未声明作用域的变换 |
| `app.configure_router_scoped(prefix, ...)` | 启动 Hook | 非根静态业务前缀内的手写路由和中间件；内部路径相对声明前缀，所有子路由均限定在该前缀之下 |
| `app.configure_mapping(...)` | 启动 Hook | 手动 global/scope/selector、窄 State 与安全运行时计划；`global = true` 的 interceptor 无需在此重复登记 |
| `app.configure_ws(...)` | 启动 Hook | 长连接逃生舱：`authorize`、endpoint 事件表、集群 notifier（声明 `ws` 组件时必须至少提供 `authorize`） |
| `app.configure_saga(plan)` | 启动 Hook | 仅在 `saga.plan_mode=custom` 提交高级计划；managed 模式调用即拒绝 |
| `app.configure_outbox(plan)` | 启动 Hook | 为脱离 Saga 的事件流提交唯一受管发布计划 |
| `app.register_outbox_publisher(...)` | 启动 Hook | 为已有事务域计划登记精确业务事件发布目标，共享同一 dispatcher |
| `app.configure_kafka(name, ...)` | 启动 Hook | 在自动收集项之后追加有状态 consumer；Ready 取走后入口永久封口 |
| `app.configure_kafka_metrics(name, sink)` | 启动 Hook | 为指定 client 安装一次无阻塞指标出口；未设置时为 Noop |
| `app.configure_redis_jobs(plan)` | 启动 Hook | 把唯一拥有式 RedisJob plan 交给独立组件；Prepare 后入口永久封口 |
| `app.datasource(name) / redis(name)` | Start 完成后 | 直接取得共享语义明确、且能被容器显式关闭的数据源池与缓存客户端 |
| `app.redis_job() / redis_job_control(source) / redis_job_query(source)` | RedisJob Ready 后至清理 | 取得无停机权的业务门面，或显式选择 source 的结构化控制、查询、健康与指标；未知 source 不回退 |
| `app.kafka(name)` | Start 完成后至清理 | 受控发布、只读 metadata、健康快照和 consumer 控制命令；不暴露 connect/registry/shutdown |
| `app.saga()` | Saga Ready 完成后至清理 | 取得已校验 Orchestrator 或命名参与方；停机保护态拒绝新工作 |
| `app.outbox()` | Outbox Ready 完成后至清理 | 读取持久化积压、死信累计与低基数投递快照 |
| `app.log() / nacos_config()` | 组件声明后至终态 | 日志初始化状态；配置中心只读拉取能力，不开放重配置、监听注册和关闭权 |
| `app.web() / ws()` | 组件声明后至终态 | Web 只读状态与指标；长连接真实地址及底层广播发送器 |
| `app.mapping()` | Web Ready 后至清理 | mapping generation、配置年龄、最近失败与统一生命周期的只读句柄 |
| `app.nacos_discovery()` | Start 完成后至清理 | 底层负载均衡 HTTP 客户端以及本实例注册状态；摘流和 provider 关闭仍由容器负责 |
| `app.scheduling()` | Ready 完成后至清理 | 底层调度库只读句柄、任务数量和可选选主状态，不开放停止或重启 |
| `app.shutdown()` | Service 运行期 | 幂等主动停机；Batch 返回阶段错误 |
| `Application::try_global()` | Service sealed 后 | 迁移期全局逃生舱，返回 `Result`，Batch 不发布 |

`Application` 是自动 Web Router 的唯一根 State。业务 interceptor 需要容器时直接声明
`State<Application>`；高频路径可在 UserHook 从容器一次性构造窄 State，再通过
`binding_with::<Application>(state)` 装配。`InterceptorContext` 只保存路由与执行计划元数据，故意不把
Application 做成运行期服务定位器。

```rust
use nasa::web::interceptor;
use nasa::web::{Next, Request, Response};

// 例 1：缺省 global=false，只声明；main 必须手动登记后才执行。
#[interceptor(id = "manual-edge", kind = "edge")]
async fn manual_edge(request: Request, next: Next) -> Response {
    next.run(request).await
}

// 例 2：显式 global=true，由 napp Web Ready 自动装配。
#[interceptor(id = "automatic-edge", kind = "edge", global = true)]
async fn automatic_edge(request: Request, next: Next) -> Response {
    next.run(request).await
}

#[nasa::application("web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    // manual_edge 仍保留完整的手动装配流程。
    app.configure_mapping(|plan| Ok(plan.global(manual_edge::binding())))?;

    // automatic_edge 不得在这里再次登记，否则重叠端点会因重复 ID 拒绝启动。
    Ok(())
}
```

需要路径层级、窄 State 或动态条件时，保持 `global = false`，在同一个
`configure_mapping` 闭包中使用 `plan.scope(...)`、`binding_with::<Application>(...)` 或 selector。

Web Ready 固定执行 `手动 mapping plan 封口 -> global=true 自动 binding 合并 -> 自动端点注册/审计
-> configure_router -> 框架探针 -> with_state(Application)`。自动 global 只覆盖 `*_mapping` 端点；
它和手动 binding 使用相同 ID 时会在监听前报重复错误。`configure_router` 仍适合普通 Tower Layer，
但不能用来冒充参与 auth-before-decrypt 排序的安全 interceptor；Ready 后再次调用两个 configure
入口都会明确失败。

全部能力入口先检查组件声明，再区分“底层对象尚未装配就绪”和“已经清理”。共享对象只开放底层类型本身
具有稳定共享和显式关闭语义的部分；具有装配权或关闭权的对象只给受控句柄。`WebHandle` 不持有路由
服务图、监听器、服务任务或业务资源；路由修改仍只允许在启动 Hook 调用 `configure_router`。
`routes()` 只包含自动收集端点和运行时探针，不伪造无法从不透明定制闭包枚举的手写端点。

### 在启动 Hook 装配未组件化依赖

业务可以在同一份 yml 增加自定义顶层段，并在启动 Hook 通过 `config_section` 反序列化。该时点已经完成
本地配置、profile、环境变量和配置中心首拉合并，同时尚未进入 Ready，适合构造外部客户端并注册资源：

```rust
/// 外部客户端需要的最终配置投影。
#[derive(serde::Deserialize)]
struct VendorConfig {
    /// 服务访问地址。
    endpoint: String,
    /// 单次调用超时毫秒数。
    timeout_ms: u64,
}

/// 业务作用：从最终配置装配业务依赖，交由应用资源容器持有。
///
/// # 参数
///
/// - `app`：已经完成初始配置合并、但尚未对外就绪的应用容器。
#[nasa::application("log", "nacos-config", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let config: VendorConfig = app.config_section("vendor")?;
    let client = build_vendor_client(config).await?;
    app.register(client)?;
    Ok(())
}
```

需要显式异步关闭的对象实现 `ManagedResource` 后使用 `register_managed`；常驻 future 使用
`spawn_critical` 或 `spawn_background`。需要热更新时，通过 `subscribe_config()` 在受监督任务中解析新快照、
校验成功后替换依赖内部状态；运行时不会猜测外部库的重应用协议。若某依赖必须早于内置组件 Start，
则应实现正式 `ApplicationComponent` 并进入声明顺序，而不是放在启动 Hook 中抢时序。

业务、组件和 initializer 拥有的受管资源使用相同的逆登记清理与异常隔离：`shutdown` 同步创建 future、
future 的 poll、future 释放以及资源值的析构分别置于独立展开边界。单项返回错误、超时或展开式 panic
记录为次要失败，在剩余预算内继续后续资源及 active stack，不覆盖首次停机原因或退出码。
清理 future 先释放对资源的借用，再释放资源值；已有 `ResourceRef` 必须先归还，超时仍被借用的资源不会被强行关闭。
外层取消或最后一个借用延迟归还时的析构异常单独同步告警，不追改已经发布的摘要。关闭容器先撤销新登记和借用，
再在注册表锁外释放所有权。同步回调与析构必须有限时间返回；阻塞、不让出执行权、`panic=abort` 和同一次展开
中的再次 panic 无法由异步期限或展开隔离强制终止。

initializer 或组件的局部资源清理只撤销该所有者的 key，不提前关闭其它资源的查找。业务停机任务仍可通过
`Application::resource` 借用尚未清理的业务和组件资源；任务结束并进入业务资源步骤后，注册表才进入全局
`Closing`，拒绝全部新借用。异步取锁完成后会复验 key 与阶段，已经撤销的条目不会因排队查找重新发布。

组件与 initializer 的 `ShutdownAction` 共用异常隔离执行器，分别保护 `label`、future 创建、poll、future
释放及 action 本体释放。`label` 展开时不再调用该 action 的 `shutdown`；其余异常保留为次要失败，在预算
仍可用时继续下一步骤。未执行的 action 在预算耗尽后只释放所有权，不计作已尝试；取消时先释放 future，
再释放 action，无法纳入报告的析构异常单独同步告警。所有同步回调和析构必须有限时间返回，阻塞、不协作
future、`panic=abort` 与同一次展开中的再次 panic 不在可抢占范围内。

## 发布边界

产品 crate 只包含运行时代码、业务使用说明与许可文件。MySQL、PostgreSQL、Redis、Nacos、Kafka、OTLP 等连接信息
只从部署环境注入，不写入源码、示例或发布归档。

## Redis Stream、Proxy、AutoPipeline

门面启用 `application,redis` 并声明 `"redis"`，直接 napp 使用 `redis` feature。
三类命名计划合计最多 64 个；`redis_ref` 缺省为 `default`，显式错误名称不会回退。
Stream 与 Proxy 仅支持 Service，handler 在 UserHook 登记，Prepare 后创建的任务等待统一 Ready 才读取。
每项 handler 目录为 1～256 个。配置的消费模式最终生效，handler builder 中的模式设置不会覆盖 YAML。

Service 的派生 Redis 入口、出站 Client 和受管终端共用一次启动许可。公开状态成为 Ready 时，
存活入口已经获得许可，首次业务调用无需等待健康监控再次激活。发布前在关键领域的本地状态保护内
复验任务责任、认证连接、健康证据新鲜度与启动期限；已观察到的失效按启动失败清理，不放行业务。
保护持续到许可发布完成，不覆盖尚未被本地观察到的远端故障，也不保证后续调用一定成功。

```yaml
redis_streams:
  notices:
    enabled: true
    redis_ref: default
    stream: notices
    critical: false
    config:
      mode: { group: { group: billing, consumer: worker-a, start: history } }
      batch_size: 100
      block_ms: 500
      idle_sleep_ms: 200
      handler_timeout_ms: 30000
      ack_policy: on_success
redis_proxies:
  orders:
    enabled: true
    redis_ref: default
    stream: orders
    group: processors
    start_offset: new
    config:
      consumers: 1
      handler_timeout_ms: 10000
      reclaim_min_idle_ms: 30000
      requeue_unregistered: true
      drain_deadline_ms: 10000
redis_pipelines:
  writes:
    enabled: true
    redis_ref: default
    window_ms: 1
    max_batch: 1000
    queue_capacity: 4096
    max_command_bytes: 16384
    max_batch_bytes: 1048576
```

Service 在 UserHook 登记消费 handler：

```rust,ignore
app.configure_redis_stream("notices", |subscriber| {
    subscriber.on_typed::<Notice, _, _>("created", |notice| async move {
        handle_notice(notice).await
    })
})?;
app.configure_redis_proxy("orders", |proxy| {
    proxy.register::<Order, _, _>("orders", "created", |order| async move {
        handle_order(order).await
    });
})?;
```

initializer 可以取得并保存发送句柄；以下调用放在 Service 获准运行的业务任务或 Batch 工作负载中，
放行前调用会被拒绝：

```rust,ignore
let writes = app.redis_pipeline("writes").await?;
let reply: String = writes.execute(nasa::redis::RedisCommand::new("PING")).await?;
```

Service 的 `app.redis_proxy("orders").await` 返回同样受准入保护的发布句柄，
`publish(topic, event, &data).await` 返回 XADD entry ID，不表示 consumer 已经处理该消息。

普通 Stream 的 `on_success` 在 handler 失败时保留 PEL，但没有自动重投；Proxy 保留原有组竞争、
回收和毒消息合同，业务 handler 必须幂等。两种格式不能共用同源同 stream/group；同一普通组消费者
身份不能重复登记。关闭先停止全部计划，再按同一截止点并发等待；PEL 查询失败、格式不完整或任务
被强制终止时保留 consumer，不以“查不到”推断无 pending。Stream、Proxy 和 AutoPipeline 的独立
关闭 owner 持续等待真实退出，取消等待不会转移任务责任。
`proxy_stop` 保留 Proxy 的任务终态与清理分类。合法 PEL 中仍有 pending 时保留 consumer，
正常退出可报告 `Pending`；查询或删除失败、摘要不完整为 `Unavailable`，共用预算耗尽为 `Deadline`，
后二者进入宿主次要停机失败，不能由任务归零推断清理成功。已发出删除后超时不证明 Redis 未执行。

受管 AutoPipeline 必须给出非零 `max_command_bytes` 与 `max_batch_bytes`，上限分别为 16 MiB、
64 MiB；队列与单批条数均为 1～65536，窗口不超过 60 秒。所有队列参数字节预算合计不超过 256 MiB。
单批 B 是软上限，单命令 M 有限时正常合批和关闭排干均受 B＋M 保守参数字节上界约束；Cmd 容量、
编码、当前批次、响应以及等待生产者的内存另计。关闭与队列预留共同裁决准入，旧句柄永久拒绝新调用。
已接纳写入仍可能结果未知，框架不自动重放。`submit` 仅确认入队，具体命令需要回执时使用 `execute`。

`redis_derived_observations()` 返回冻结名称、类别、任务运行／关闭状态及微批队列占用。
其中 `activity` 分别报告 consumer、reclaim、flusher 存活数、排队参数字节、未完成工作次数、
已结束工作次数、微批传输失败批次数与最近进展间隔。未发生有效读取或处理时进展为 None，空闲轮询
可以形成读取证据；这些本地次数不等于远端 PEL 数、唯一消息数或业务成功数，强停后未完成责任仍保留。
Service 健康默认不影响 Ready；`critical: true` 时影响 Ready 且后台任务意外退出触发停机。
失败／恢复阈值均为 1，观测超过 15 秒过期，来源连接健康仍由 Redis 组件负责。

## 纯出站 TCP 帧客户端

门面启用 `application,ws-client`，直接 napp 开启 `ws-client`；无需声明 `"ws"` 或 `"web"`。
本入口管理 naws 现有 TCP wire 协议，不接受 ws/wss URL，不提供 TLS、业务消息重发或远端送达保证。

```yaml
ws_clients:
  upstream:
    enabled: true
    address: 127.0.0.1:19091
    endpoint: /ws
    token: secret://upstream_token
    device_id: worker
    version: "1.0"
    connect_timeout_ms: 5000
    auto_reconnect: true
    reconnect_min_ms: 200
    reconnect_max_ms: 5000
    queue_capacity: 256
    max_frame_bytes: 65536
    critical: false
```

`token` 可省略；提供时必须使用 `secret://`，材料来自同一启动快照。仅被 disabled 计划引用的材料
不解析。首连包含 TCP 建连与认证，共用一份超时；所有启用计划首连失败均拒绝启动。
运行期重连由 `auto_reconnect` 控制，使用冻结的材料，每次重连都先等待旧 writer 与 heartbeat 退出。
配置与材料变化报告 `RestartRequired`。

Service UserHook 使用 `configure_ws_client_event(name, event, callback)` 登记同步非阻塞回调，
每客户端最多 256 个事件。Ready 前控制帧照常处理，业务帧最多缓冲 64 条且正文合计不超过单帧配置；
超限断开，不能以无界缓冲等待启动。回调 panic 被隔离并计数；`observation()` 分别给出连接状态、最近认证／PONG 间隔及固定失败类别。Batch 可发送消息，但不接受长期回调计划。
客户端最多 64 个，队列 1～4096 条，单帧 body（含 type/mode）1～16 MiB，配置队列 body 预算合计
不超过 256 MiB；帧头、当前写入和入站缓冲另计。连接超时 1～300000 ms，重连间隔 1～60000 ms。

`app.ws_client(name).await` 只提供发送与观测权。`send`/`send_message` 返回 true 仅证明本地入队，
断线或关闭可以丢弃尚未确认的数据；业务应使用明确确认与幂等协议。`active_tasks()` 与
`ws_client_observations()` 提供 supervisor、writer、heartbeat 的实际任务数量。
Service 每秒根据认证连接更新健康，失败／恢复阈值为 1，5 秒没有更新则证据过期。
`critical` 缺省 false：断连或重连期间为 Degraded，仍可接流；设为 true 时为 NotReady，停止接流，
重新认证成功后恢复 Ready。关键连接 owner 意外结束触发停机；准备后、Ready 发布前已失效则拒绝启动。
健康反映本地已观察到的连接事实，存在协议检测与采样间隔。关闭与健康发布串行，晚到认证不能重新
开放已关闭的句柄或恢复 Ready，旧句柄不能重连或重新发送。

## hystrix 命令 owner

门面组合 `application,hystrix`，直接 napp 使用 `hystrix` feature；没有 `"hystrix"` 组件字符串。
仅 `hystrix.enabled: true` 安装规则和集中周期观测，缺省保留独立 API 合同。

```yaml
hystrix:
  enabled: true
  max_commands: 256
  context_path: ""
  isolation:
    /orders/*: { max_concurrent: 16, timeout_ms: 800 }
  commands:
    settlement: { group: billing, max_concurrent: 8, timeout_ms: 500 }
```

Service 和 Batch 都在 Prepare 装配；`app.hystrix_command("settlement").await` 取得预装配显式命令。
受管 Web 自动将 `hystrix::dispatch` 装在业务路由上，框架探针不经过规则；无需业务再次添加该层。
静态 `#[hystrix]` 描述在 Ready 前建立本代命令；不持有旧代静态 Arc，下一应用实例按新 owner 重建。
显式旧 Command 永久拒绝执行，返回 503；动态命令重名或超限也拒绝执行，不新增目录或周期任务。

目录缺省 256，允许 1～4096；名称和 group 非空且不超过 128 字节，并发上限不超过 65536，超时不超过
3600000 ms，0 保持“关闭该项保护”的原语义。规则格式错误、重复 owner、独立目录／隔离表已安装，
或存在手工全局 fallback 时拒绝受管启动。静态收集的全局 fallback 函数可复用，运行资源不能通过
手工进程 fallback 移交给此 owner。

规则、目录计划和容量冻结到启动，变化报告 `RestartRequired`。Service 业务 graceful shutdown
仍可调用命令，随后才关闭新调用、等待在途执行与集中周期任务退出，再撤销本代全局引用。
等待方取消不解除该责任；上一代真实退出前再次安装会得到 owner 冲突。观察任务意外结束影响 Ready
并触发停机，健康阈值为 1，5 秒过期。该能力不提供错误率熔断或自适应限流。
执行器销毁会关闭旧代准入；只有周期任务、收尾任务和全部在途业务 future 都已释放，才撤销全局引用。
这种退出保留失败结果；外部仍持有业务 future 时，下一代继续得到 owner 冲突。
