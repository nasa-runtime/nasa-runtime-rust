# napp

`napp` 是 `#[nasa::application]` 属性入口背后的应用生命周期运行时：统一配置装载、组件启动/停机编排、任务监督、信号处理与退出码。业务项目**不要直接依赖本 crate**，经 `nasa` 门面开启 `application` feature 使用；使用入口与生命周期约束见仓库的快速开始和运维指南。

其中的业务 initializer 是 Ready 前的初始化屏障：migration 与出站依赖完成后统一执行静态宏和运行时
登记项的 `before -> initialize -> after` 三轮，全部成功前不开放监听、消费或服务发现。

```toml
nasa = { version = "1", features = [
    "application", "log", "nacos-config", "telemetry", "tx", "redis", "cache",
    "kafka", "oauth", "web",
    "nacos-discovery", "scheduling",
] }
```

```rust
mod controller;

#[nasa::application(
    "log", "nacos-config", "telemetry", "db", "redis", "cache",
    "kafka", "auth", "web", "nacos-discovery", "scheduling"
)]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    // 业务启动 Hook：注册资源、登记受监督任务、注入路由/长连接定制和运行时 initializer。
    app.configure_router(|router| router)?;
    Ok(())
}
```

## `application` 支持的组件字符串

属性当前只接受下面 14 个小写字符串，名称区分大小写，不支持别名。业务可按任意顺序书写；宏会拒绝
未知名称和重复名称，再按唯一规范顺序生成组件列表。字符串对应的门面 feature 没有启用时会在编译期
拒绝，不会静默跳过。

| 属性中填写的精确字符串 | 需要启用的 `nasa` feature | 配置根 | 组件作用 | 容器能力入口 |
| --- | --- | --- | --- | --- |
| `"log"` | `log` | `log` | 两阶段日志：早期控制台 → 最终文件日志；运行期 `log` 段可热重应用 | `app.log()` |
| `"nacos-config"` | `nacos-config`；真实远端连接再加 `nacos-sdk` | `nacos` | 远端 overlay 首拉与 watch 热刷新；`enabled=false` 走纯本地 | `app.nacos_config()` |
| `"telemetry"` | `telemetry` | `telemetry` | 有界 span 管道、日志或 OTLP/HTTP sink、受管停机 flush | `app.telemetry_snapshot()` |
| `"db"` | `tx` | `database` 或 `datasources.<name>` | 数据源探测、建池、资源注册和事务运行时注入 | `app.datasource(name).await` |
| `"redis"` | `redis` | `redis` | 统一客户端建连与显式停机 | `app.redis(name).await` |
| `"cache"` | `cache`；使用 `redis_ref` 时还需 `redis` | `cache` | scene 审计、L2 安装、失效广播与代际 owner | 宏经进程级 cache runtime 使用 |
| `"saga"` | `saga-runtime` | `saga` | Ready 前校验步骤合同与历史实例，发布运行角色并监督 durable timer | `app.saga()` |
| `"kafka"` | `kafka` | `kafka` 或 `kafkas.<client>` | 受管 producer/consumer、broker Ready、动态健康与两段停机 | `app.kafka(name)` |
| `"outbox"` | `outbox` | `outbox` | 持续投递已提交事件、退避、readiness 与反向停机；可脱离 Saga 使用 | `app.outbox()` |
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

规范顺序固定为 `log -> nacos-config -> telemetry -> db -> redis -> cache -> saga -> kafka -> outbox ->
auth -> web -> ws -> nacos-discovery -> scheduling`。业务书写顺序不改变启动顺序；停机严格反向执行。
`auth` 缺少 `web` 会被拒绝，`cache.redis_ref` 指向受管 Redis 时还必须声明 `"redis"`。

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

gRPC request/response connector 通过门面 `saga-grpc-experimental` 启用，只提供封闭收据裁决，不是
Application 组件字符串，也不拥有 listener。业务必须另行启用并托管 `grpc-experimental` listener，
完成 mTLS/签名身份映射、deadline、资源上限与 drain；回包缺失和 `Retryable` 都让发布端保留 Outbox
行重投，不能按确定失败消耗死信预算。

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
  timer_poll_interval_ms: 500
  timer_error_backoff_ms: 1000
  timer_operation_timeout_ms: 5000
  timer_failure_threshold: 3
```

`database_bootstrap` 默认为 `application`，DB 组件在 Start 阶段按 `database` 或 `datasources` 建池。
确实需要先创建隔离库的进程可设为 `user_hook`，启动钩子注入默认事务池后，DB 组件会在 Prepare 接管并
完成连接探针与 migration 门禁；关闭所有权在 Start 阶段预占，确保受监督任务退出后才释放连接。Ready 后
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

已投递行与死信的保留清理是显式子计划：`plan.with_retention(policy, interval_ms, archive)`。
执行器绝不从"开启了 Outbox"推断保留期——未提交策略就没有任何删除；策略值不自洽、要求收据却
缺归档端或间隔越界都在 UserHook 拒绝启动。行分类合同固定：待投递行（`dispatched=0 AND dead=0`）
永不删除；已投递行达到最小保留期才可归档/删除；死信默认保留，只有独立批准标识、最小年龄与
归档收据齐备才逐批清理。清理使用与 dispatcher 分离的 session-bound retention claim（同库同刻
仅一个清理 owner，竞争即让路），按主键集合逐批独立提交删除，受批大小与单轮时间预算约束；
若启用归档，先按 `event_id` 幂等写入并取得可复验收据才删源行，回包不确定用收据重查恢复。
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
pause/resume、seek、动态 subscribe/unsubscribe、人工 restart 和只读 topic metadata。它不会返回原始
`KafkaProxy`，也没有 `connect`、`consumers`、admin 写操作或 `shutdown`。即使业务仍持有 handle，容器
停机仍先在 Ready 层停止 consumer，再让 UserTask/业务资源利用仍开放的 producer 做退出收尾，最后在
Start 层关闭发布准入、flush lane 和 admin；全部步骤共用一个绝对停机 deadline。

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
- 停机按 active stack 严格反序，所有清理共享 `application.shutdown_timeout_ms` 一个绝对预算；启动失败沿同一条回滚链，primary 错误不被回滚错误覆盖。每个已尝试步骤产生带递增序号、固定类型、稳定归属、耗时和失败增量的 `debug` 事件；清理结束后由不依赖日志组件的同步诊断通道输出一次有界摘要，包含各类步骤计数、任务 abort、deadline、放弃步骤与总耗时。
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
| `app.datasource(name) / redis(name)` | Start 完成后 | 直接取得共享语义明确、且能被容器显式关闭的数据源池与缓存客户端 |
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
