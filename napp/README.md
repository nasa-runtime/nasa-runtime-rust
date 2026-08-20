# napp

`napp` 是 `#[nasa::application]` 属性入口背后的应用生命周期运行时：统一配置装载、组件启动/停机编排、任务监督、信号处理与退出码。业务项目**不要直接依赖本 crate**，经 `nasa` 门面开启 `application` feature 使用；使用入口与生命周期约束见仓库的快速开始和运维指南。

## 核心价值与生命周期架构

业务入口只声明组件并在 UserHook 提交不可推断的业务计划；`napp` 负责把配置、migration、出站资源、
initializer、listener、消费循环、readiness、关键任务和停机编排成一个唯一所有者。业务不需要为每个
组件另写启动顺序、信号处理、后台任务脱离检测或 shutdown glue。

```text
配置装载 → Start 出站资源 → UserHook 提交计划 → Prepare / initializer 屏障
         → Ready 绑定入站与发布发现 → Running 监督关键任务
         → NotReady / 摘流 → 反向有界排空 → Stopped
```

initializer 是 Ready 前的初始化屏障：migration 与出站依赖完成后统一执行静态宏和运行时登记项的
`before -> initialize -> after` 三轮。任何阶段失败都不会开放监听、消费或服务发现，已经启动的资源按
active stack 反向释放。Application 只拥有业务显式声明的组件，不猜测外部 transport，也不替业务决定
事务边界、路由、鉴权主体、容量值或重试/DLT 策略。

gRPC 入站同样遵守这个模型：业务登记 generated service 或提交 Saga gRPC handler，Application 在
同一个 sealed registry 中完成 Router、TLS、health、reflection、容量、指标、listener 和 drain，不会
产生第二个 tonic Router 或第二套生命周期。

## 最小入口

```toml
nasa = { version = "2", features = [
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
| `"db"` | `tx` | `database` 或 `datasources.<name>` | 全表校验、逐源探测、冻结 registry 与显式停机 | `app.datasource(name).await`、`app.default_datasource().await` |
| `"redis"` | `redis` | `redis` 或 `redis.properties.<qualifier>` | 多实例统一建连、逐源健康与显式停机 | `app.redis(name).await`、`app.default_redis().await` |
| `"cache"` | `cache`；使用 `redis_ref` 时还需 `redis` | `cache` | scene 审计、L2 安装、失效广播与代际 owner | 宏经进程级 cache runtime 使用 |
| `"partition"` | `partition` | `partition`，或 UserHook 提交 `PartitionApplicationPlan` | Prepare 创建保序分 lane 执行器、动态健康与有界停机 | `app.partition()` |
| `"saga"` | `saga-runtime` | `saga` | Ready 前校验步骤合同与历史实例，发布运行角色并监督 durable timer | `app.saga()` |
| `"kafka"` | `kafka` | `kafka` 或 `kafkas.<client>` | 受管 producer/consumer、broker Ready、动态健康与两段停机 | `app.kafka(name)`、`app.default_kafka()` |
| `"outbox"` | `outbox` | `outbox` | 持续投递已提交事件、退避、readiness 与反向停机；可脱离 Saga 使用 | `app.outbox()` |
| `"redis-job"` | `redis-job`；隐式纳入 `redis` | `redis.job` 与 `redis.properties.<source>` | 冻结多 source 计划、布局与能力门禁，Ready 后启动扫描、租约、Fanout、逐源监督和有界停机 | `app.redis_job()`、`app.redis_job_control(source)`、`app.redis_job_query(source)` |
| `"grpc"` | `grpc` | `grpc` | initializer 后自动装配 registered service、health、可选 reflection、TLS、listener、方法指标与有界排空 | `app.grpc()` |
| `"auth"` | `web`，并同时声明 `"web"`；直接使用 OAuth 类型再开 `oauth` | `auth` | 静态/远程 JWKS 首拉、刷新、认证器发布和 readiness | Web 安全流水线消费 |
| `"web"` | `web`；需要端点安全时使用 `web-security` | `server` | 自动收集端点、探针、监听与排空；定制经 `configure_router` | `app.web()` |
| `"ws"` | `ws` | `ws` | TCP/WebSocket 长连接监听与排空；鉴权和 endpoint 经 `configure_ws` 注入 | `app.ws()` |
| `"nacos-discovery"` | `nacos-discovery`；真实 provider 再加 `nacos-sdk` | `rest_discovery` | Start 装出站客户端、Ready 注册、停机先摘流后关客户端 | `app.nacos_discovery()` |
| `"scheduling"` | `scheduling`；选主模式使用 `scheduling-cluster` | `scheduling` | Ready 末尾启动已收集任务；选主模式复用已声明的 Redis 客户端 | `app.scheduling()` |

只用部分能力时只填写需要的字符串。例如纯 Web 应用写 `#[nasa::application("web")]`；本地配置的
数据库 Web 应用可写 `#[nasa::application("db", "web")]`。独立批处理仍可只开启 `kafka` feature 并显式
管理 `KafkaProxy`；Service 一旦把 `"kafka"` 写入属性，连接、消费、Ready、监控和停机就全部归容器所有。
`hystrix`、`grafana`、`mapper` 不是属性组件字符串，仍通过各自 feature 使用。

不要填写 `"nacos"`、`"discovery"`、`"database"`、`"websocket"` 或 `"schedule"`；对应的合法
字符串分别是 `"nacos-config"`、`"nacos-discovery"`、`"db"`、`"ws"` 和 `"scheduling"`。

规范顺序固定为 `log -> nacos-config -> telemetry -> db -> redis -> cache -> partition -> saga -> kafka -> outbox -> redis-job ->
grpc -> auth -> web -> ws -> nacos-discovery -> scheduling`。业务书写顺序不改变启动顺序；停机严格反向执行。
`auth` 缺少 `web` 会被拒绝，`cache.redis_ref` 指向受管 Redis 时还必须声明 `"redis"`。

## YAML 创建单源与多源

MySQL、Redis 与 Kafka 的 endpoint、凭据、池和客户端参数都属于 YAML。Application 在启动期读取最终
配置并创建全部实例；业务 `main` 只提交 publisher、consumer、handler、Saga 定义等业务计划，再通过
`app.datasource(...)`、`app.redis(...)` 或 `app.kafka(...)` 取得已经受管的句柄，不自行建池、建连或
维护第二张 source 表。所有 source 先完成全表校验，再按名称稳定排序创建；任一项失败都会阻止 Ready。

| 资源 | 单源 YAML | 多源 YAML | 默认身份 | 被其它组件引用的字段 |
| --- | --- | --- | --- | --- |
| MySQL | `database` | `datasources.<name>` | `default` | `outbox.datasource_ref`、`saga.datasource_ref` |
| Redis | 扁平 `redis` | `redis.properties.<qualifier>` | 持久身份 `primary`，查询兼容名 `default` | `cache.redis_ref`、`cache.invalidation.redis_ref`、`scheduling.redis_ref`、`redis.job.sources.<qualifier>` |
| Kafka | `kafka` | `kafkas.<client>` | `client_name: default` | `configure_kafka(client, ...)`、`app.kafka(client)` 与 consumer 的 `client` |

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

# 只有声明了对应组件时才需要下面的段；省略 datasource_ref 也默认选择 default。
outbox:
  datasource_ref: default
saga:
  database_bootstrap: application
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
saga:
  database_bootstrap: application
  datasource_ref: reporting
```

业务分别调用 `app.default_datasource().await` 与 `app.datasource("reporting").await`。如果多源 map 没有
`default`，命名 getter 仍可用，但默认 getter 会明确失败。Outbox 与 Saga 的引用必须命中同一份
`datasources`，两者共同形成原子事件链时必须使用相同的 `datasource_ref`；不存在的引用会在首次数据库
握手前被拒绝。`database_bootstrap: user_hook` 只允许单个 `default`，命名库必须使用
`database_bootstrap: application` 让容器从 YAML 创建。

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
全部 initializer 成功后才启动领取。停机时先关闭所有 source 的新控制动作和领取，再按稳定逆序排空
Handler、注销执行器并关闭专用连接。一个 source 的健康、监督代次和停机结果不会被另一个 source 覆盖。

Ready 后的运行路径由 `nadis::job` 执行：计划按 source 冻结，执行器发布能力快照，Dispatcher 投递并由目标持久确认，Handler 取得带 lease 和 fencing 的 attempt 后执行，receipt、ready、lease 和 root 扫描器分别完成恢复与聚合。Fanout 容量背压使用独立窗口和路由预算；`capacityRouteTotal` 保留历史容量迁移次数，便于 source 级运维审计。

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
[nadis README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadis/README.md#redisjob-分布式任务运行时)。

## Partition 受管模式

`"partition"` 把 `napart::PartitionExecutor` 纳入 Application 的唯一所有权。容量和停机预算推荐直接
写在 YAML；组件在 UserHook 成功结束后的 Prepare 阶段创建执行器，登记到统一资源视图并发布
`app.partition()`。因此静态工厂与 initializer 三阶段可以使用同一句柄，但初始化后续失败仍会沿
active stack 停止 worker，不留下脱离容器的执行器。该业务句柄只开放提交与只读观测，不开放
`shutdown*`；完整执行器的收口权只属于 Application 生命周期 action。

```yaml
partition:
  partitions: 16
  queue_capacity: 1024
  global_inflight: 16384
  max_lanes: 4096
  shutdown_timeout_ms: 5000
```

`partitions`、`queue_capacity` 和 `global_inflight` 必填；`max_lanes` 省略时取 4096 与规范化分区数的
较大值，`shutdown_timeout_ms` 默认 2000。运行期改变这些字段会报告 `RestartRequired`。

需要由代码计算容量时仍可在 UserHook 提交同一份纯参数计划，但不能与 YAML `partition` 同时使用：

```rust
use std::time::Duration;
use nasa::application::PartitionApplicationPlan;

#[nasa::application("partition", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let plan = PartitionApplicationPlan::new(16, 1_024, 16_384)?
        .with_max_lanes(4_096)?
        .with_shutdown_timeout(Duration::from_secs(5))?;
    app.configure_partition(plan)?;
    Ok(())
}
```

`partitions`、每 lane 深度、全局在飞量、lane 总数和停机排空预算都必须有界；YAML 与 UserHook
计划冲突、计划非法或声明组件后没有任何计划会在监听前拒绝启动。worker 异常死亡或 lane 冻结会使
`partition:executor` readiness 变为
NotReady，并通过关键 monitor 触发应用统一停机。反向停机先拒绝新任务，再在计划预算与 Application
全局剩余 deadline 派生的子预算内排空，并为损耗报告与后续逆序清理保留尾部预算；`frozen` 或
`aborted` 非零会进入停机失败，同时证据仍可由业务句柄读取。`app.partition()` 返回的受管业务句柄
支持任务提交和只读观测，但不开放 `shutdown*`；执行器收口权只属于 Application 生命周期。
直接依赖 `napart` 的调用方仍可自管执行器；只有声明 `"partition"` 才形成上述容器合同。

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

Prometheus `/metrics` 与 OTLP 从同一 `MetricHub` 结构化快照读取，包括原生指标、
naweb、nafana、Outbox 和 Saga Streams；两个出口并存且都不清零计数。单次失败只将
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

## Saga 受管模式

`"saga"` 是组合组件：宏会隐式加入 DB 与 Outbox，业务不再重复写 `"db"`、`"outbox"` 或手工
dispatcher。Inbox 没有后台生命周期，它由 Orchestrator 和参与方在本地事务中直接调用，因此不存在
单独的 `"inbox"` 组件字符串。Kafka、Redis Streams、HTTP 等 transport 不由 Saga 猜测：业务明确
选择 Kafka 托管消费时声明 `"kafka"` 并启用 `saga-kafka`；选择 Redis Streams 托管消费时声明
`"redis"` 并启用 `saga-redis-stream`,经 `SagaApplicationPlan::with_redis_stream_transport`
提交已构造的 result/command 消费者(`(stream, group, consumer)` 身份须唯一)。Redis 组件在
Saga 之前建立、在其之后释放;Ready 前用真实客户端统一探测 PING/配置合同/group 幂等创建
(兼 ACL 探测),失败拒绝 Ready;消费循环由 Runner 监督,停机先关领取、排空在途轮次、未确认
消息留 PEL 交重启后重领。按冻结 (stream, group) 导出 `napp_saga_stream_*` 低基数指标
(`Application::saga_stream_metrics_prometheus`),其中 `deleted_pending_total` 非零必须告警。

Saga gRPC command/result transport 通过门面 `saga-grpc` 启用；该 feature 已包含 gRPC 类型门面，但
不会因只使用出站 client 而隐式声明 listener。入站宿主显式声明 `"grpc"`，再由
`SagaApplicationPlan::with_grpc_command_service` 提交 `#[saga]` Service 与 mTLS leaf 指纹；Application
从单参与方计划冻结的信任投影生成 handler。Orchestrator 使用 `with_grpc_result_service`，只声明无法从
多参与方定义推断的 producer/principal 绑定。Application 自动把框架 generated service 登记到唯一
gRPC registry。业务不创建 `Arc` handler、generated server、第二个 Router、listener 或身份解析器。
回包缺失和 `Retryable` 都让发布端保留 Outbox 行重投，不能按确定失败消耗死信预算。

为兼容显式依赖声明，`#[nasa::application("saga", "db")]` 和
`#[nasa::application("saga", "db", "outbox")]` 都合法，并与只声明 `"saga"` 生成相同组件图；只有属性中
把同一个字符串写两次才按重复声明拒绝。

业务在 UserHook 内装配 definition、运行角色和唯一发布端；Application 在 Ready 前完成流程合同、
历史非终态实例、数据库与 Outbox 门禁，随后启动 timer 和 dispatcher：

对应的门面依赖至少启用 `application` 与 `saga-runtime`；选用受管 Kafka transport 时再启用
`saga-kafka`，选用受管 Redis Streams transport 时启用 `saga-redis-stream` 并声明 `"redis"`。

```rust
use std::sync::Arc;
use nasa::application::SagaApplicationPlan;
use nasa::saga::{DefinitionRegistry, Orchestrator, OrchestratorConfig};

#[nasa::application("saga")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let mut definitions = DefinitionRegistry::new();
    definitions.register(checkout_definition()?)?;
    let orchestrator = Arc::new(Orchestrator::new(
        definitions,
        OrchestratorConfig::default(),
    )?);
    let publisher = Arc::new(build_command_publisher()?);
    app.configure_saga(
        SagaApplicationPlan::orchestrator(orchestrator, "checkout-orchestrator-a")?
            .with_event_publisher(publisher)?,
    )?;
    Ok(())
}
```

纯参与方使用 `SagaApplicationPlan::participant(name, runtime)`；同一进程承载多个参与方时使用
`with_participant` 逐项追加。`app.saga()` 只在 Ready 门禁通过后返回能力，停机保护态拒绝新工作。
`with_event_publisher` 接收 provider-neutral 的 `OutboxPublisher`。发布确认可以来自 Kafka、Redis Streams
或 HTTP；未绑定发布端时组合组件拒绝 Ready，避免 Saga 已提交 command/result 却没有持续投递者。
规范顺序为 `db -> saga -> kafka -> outbox -> web/ws`，反向停机时先关闭入口、停止 dispatcher 与消息
消费，再关闭 Saga 能力和数据库。

Saga 隐式 Outbox 默认采用 `Block`：首个未确认事件会阻塞同一 `outbox_event` 表的全部后续事件，避免
把瞬态网络失败按固定次数误判为可以越过的 command/result。审计或其它事件若写入同一张表，绑定的唯一
publisher 必须覆盖所有事件类型；否则应使用独立事务数据库和独立 Outbox 生命周期。`app.outbox()` 的
`render_prometheus()` 输出无业务标签的积压、死信、发布量与失败轮次，必须接入值班告警。

`saga` 配置段只控制宿主轮询预算：

```yaml
saga:
  database_bootstrap: application
  datasource_ref: workflow
  timer_poll_interval_ms: 500
  timer_error_backoff_ms: 1000
  timer_operation_timeout_ms: 5000
  timer_failure_threshold: 3
```

`database_bootstrap` 默认为 `application`，DB 组件在 Start 阶段按 `database` 或 `datasources` 建池。
`datasource_ref` 默认为 `default`，并必须与 UserHook 提交的全部 Orchestrator/Participant 以及
`outbox.datasource_ref` 一致；Saga 的 Inbox、状态、timer、审计和 Outbox 只承诺该库内的本地事务。
确实需要先创建隔离库的进程可设为 `user_hook`，启动钩子注入默认事务池后，DB 组件会在 Prepare 接管并
完成连接探针与 migration 门禁；独立入口的 pool 所有权会原子转交给单源受管 registry，关闭所有权在
Start 阶段预占，确保受监督任务退出后先撤销事务解析权威、再释放连接。Ready 后
`app.datasource("default")` 返回同一受管池，停机态拒绝新的借用。该模式不读取
`database`/`datasources` 的连接设置并会记录提示，不能用来绕过数据库门禁。

timer owner 不从共享配置推断，必须随计划提供逐副本唯一且重启稳定的 canonical 身份。

`OrchestratorConfig` 也是 UserHook 前构造、提交后冻结的业务合同：`tenant_quotas` 限制每租户在飞实例，
`tenant_action_rates` 限制 pause/resume/retry/manual-close 等变更动作，`enable_manual_close` 默认关闭并
要求全部副本先升级为可解析 `MANUALLY_CLOSED` 的读者。它们不是 `saga:` YAML 热配置；精确用量只能
通过有权限的管理查询读取，Prometheus 只导出无租户标签的拒绝总数。

完整事务、transport、迁移、恢复和生产批准边界见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。

独立 Outbox 场景可显式声明 `#[nasa::application("outbox")]`，并在 UserHook 调用
`app.configure_outbox(OutboxApplicationPlan::new(publisher))`。该声明会隐式加入 DB，但不会加入 Saga 或
Inbox，适合领域事件、审计和缓存失效通知。事件所在事务确认提交后会立即唤醒本进程 dispatcher；
`outbox.poll_interval_ms` 是跨进程写入、进程重启和漏通知恢复的兜底上限，不会固定消耗每条 Saga
步骤的执行预算。下游失败时提交通知不能绕过 `error_backoff_ms`。

独立 Outbox 的 `outbox.datasource_ref` 同样默认为 `default`；配置命名库后，其启动探针、投递、保留
清理、租户配额与指标查询不会回落到默认库。

已投递行与死信的保留清理是显式子计划：`plan.with_retention(policy, interval_ms, archive)`。
执行器绝不从"开启了 Outbox"推断保留期——未提交策略就没有任何删除；策略值不自洽、要求收据却
缺归档端或间隔越界都在 UserHook 拒绝启动。行分类合同固定：待投递行（`dispatched=0 AND dead=0`）
永不删除；已投递行达到最小保留期才可归档/删除；死信默认保留，只有独立批准标识、最小年龄与
归档收据齐备才逐批清理。清理使用与 dispatcher 分离的 session-bound retention claim（同库同刻
仅一个清理 owner，竞争即让路），按主键集合逐批独立提交删除，受批大小与单轮时间预算约束；
若启用归档，先按 `event_id` 幂等写入并取得可重新验证的收据才删源行，回包不确定用收据重查恢复。
清理停摆只体现在 `napp_outbox_retention_*` 指标与"最后成功时刻"上（严格治理 degraded 信号），
绝不反向停止 dispatcher 投递。删除 `COMMIT` 的应答不确定会单独增加
`napp_outbox_retention_commit_uncertain_total`，不虚增已确认删除数，也不刷新最近成功时刻；下一轮
按数据库中的持久候选事实继续收敛。

多通道分片是显式 opt-in：`plan.with_channel_lanes(routes, lanes)` 提交 aggregate_type → lane 的
冻结路由与本进程 lane 集合（必须含默认 `global` lane）。写侧按路由稳定派生 lane——只依赖聚合类型，
同一聚合根二元组自始至终同 lane；路由进程级冻结，运行期变更被拒绝。每个 lane 拥有独立
session-bound claim、退避与指标，毒丸只停摆自己的 lane（`Block` 语义与"成功前缀才标记"不变，
改变的只是停摆半径）；整体 readiness 在全部 lane 健康时 Ready，`napp_outbox_lane_*{channel=...}`
区分单领域停摆与整体退出。启用分片后同库禁止再运行未分片 dispatcher（两种 claim 锁名不同，
并行会双重发布）；上线顺序见 naoutbox-mysql 迁移说明（先加列回填、行为不变，再切按 lane 所有权）。

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
  -> 预绑定 listener，发布 observer、readiness 和发现 metadata
  -> 停机先注销发现实例，再停止准入并在全局剩余预算内排空
```

```toml
[dependencies]
nasa = { version = "2", features = ["application", "grpc"] }

[build-dependencies]
nagrpc-build = "2"
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
[nagrpc README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nagrpc/README.md#application-受管模式)。
缺少业务 service、重复或晚到登记、ABI/descriptor 冲突、未知方法策略、非法 TLS/容量配置或端口绑定
失败都会阻止 Ready，且不会留下监听任务。显式 `grpc.health_only: true` 可启动只有标准 health 的
基础设施 listener。`app.grpc()` 只返回 `GrpcServerObserver`，业务不能越过组件直接 shutdown。

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
只能收紧全局上限，未知路径直接阻止启动。该组件只托管一个 listener，不提供证书进程内热切、客户端
连接池或负载均衡；需要独立所有权时使用 `nasa::grpc::ServerPlan`，不要同时声明 `"grpc"` 组件。

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
成功后才交给 Supervisor，任务主体等到 Application 发布 Ready 才开始。业务自管 listener 应使用
`app.serve_when_ready(...)`，这会保证启动失败或停机时根本不调用 listener 工厂。

initializer 失败、panic、超时或取消都会停止后续阶段，不发布 Ready，并严格逆序停止任务、
撤销 action 和关闭受管资源。已提交的 DB/Redis/Kafka 外部事实无法由本地回滚，因此实现必须可安全重跑：
单库多步写使用事务，跨资源事实使用稳定幂等键或与 Outbox 同事务提交。

## 生命周期要点

- `zcf/application.yml` 必须存在（内容可为 `{}`）；整个 `application.*`（name/mode/worker_threads/超时）是 bootstrap-only，远端首拉改写拒绝启动，运行期改写只记 `RestartRequired`。
- `mode: auto | service | batch`：auto 在声明 saga/kafka/outbox/web/ws/nacos-discovery/scheduling 任一长生命周期组件，或收集到静态 `hosted` initializer 时解析为 Service，否则 Batch；显式 Batch 不允许这些长生命周期能力。
- Service 启动顺序为 `Bootstrap -> Start -> UserHook -> InitializerFreeze -> Prepare -> Initialization -> Seal -> Ready`；全部阶段共用 `application.startup_timeout_ms` 形成的一个绝对 deadline。
- 信号：broker ready 先于任何异步组件；Service 首次 Ctrl-C/SIGTERM 优雅停机退 0，Batch 未完成被取消退 128+signo；Stopping 中再次收到信号立即强退。
- 停机按 active stack 严格反序，所有清理共享 `application.shutdown_timeout_ms` 一个绝对预算；Runner 会在每个 `ShutdownAction` 外层派生提前截止预算，防止单个扩展耗尽后续清理时间。action 内部需要为自身报告或补偿继续细分预算时使用公开的 `ShutdownContext::child_budget`，不能直接把全部 `remaining()` 交给可能用满预算的子操作。启动失败沿同一条回滚链，primary 错误不被回滚错误覆盖。每个已尝试步骤产生带递增序号、固定类型、稳定归属、耗时和失败增量的 `debug` 事件；清理结束后由不依赖日志组件的同步诊断通道输出一次有界摘要，包含各类步骤计数、任务 abort、deadline、放弃步骤与总耗时。
- 配置热刷新：整帧校验失败保留旧快照；可热刷组件（当前 log）成功记 `Applied`、失败保留 last-known-good 记 `ApplyFailed`；其余组件的段变化如实记 `RestartRequired`。`app.config_view()` 保证快照与状态表同版本。
- 错误报告统一脱敏（URI userinfo、常见敏感键）后输出完整错误链；进程级 panic hook 只写受控 location marker，不读 payload。

## 业务扩展点

| 入口 | 开放窗口 | 用途 |
| --- | --- | --- |
| `app.register / register_named / register_managed` | 启动 Hook | 把业务资源所有权交给容器；运行期只读借用 `app.resource::<T>()` |
| `app.spawn_critical / spawn_background` | 启动 Hook | 受监督任务；critical 提前退出触发失败停机 |
| `app.serve_when_ready` | Service 启动 Hook | 登记只在 Application Ready 后才构造与 poll 的自管 listener |
| `app.register_initializer` | Service 启动 Hook | 登记运行时 initializer，与 `#[nasa::initializer]` 静态项合并冻结 |
| `app.configure_router(...)` | 启动 Hook | Web 逃生舱：手写路由、全局中间件、`/hystrix.stream` 等 |
| `app.configure_mapping(...)` | 启动 Hook | 手动 global/scope/selector、窄 State 与安全运行时计划；`global = true` 的 interceptor 无需在此重复登记 |
| `app.configure_ws(...)` | 启动 Hook | 长连接逃生舱：`authorize`、endpoint 事件表、集群 notifier（声明 `ws` 组件时必须至少提供 `authorize`） |
| `app.configure_saga(plan)` | 启动 Hook | 提交唯一 Orchestrator/参与方计划；Ready 取走后入口永久封口 |
| `app.configure_outbox(plan)` | 启动 Hook | 为脱离 Saga 的事件流提交唯一受管发布计划 |
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

全部能力入口先检查组件声明，再区分“底层对象尚未发布”和“已经清理”。共享对象只开放底层类型本身
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

/// 从最终配置装配尚未组件化的业务依赖。
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

## 发布边界

产品 crate 只包含运行时代码、业务使用说明与许可文件。MySQL、Redis、Nacos、Kafka、OTLP 等连接信息
只从部署环境注入，不写入源码、示例或发布归档。
