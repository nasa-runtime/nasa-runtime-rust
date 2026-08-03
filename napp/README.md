# napp

`napp` 是 `#[nasa::application]` 属性入口背后的应用生命周期运行时：统一配置装载、组件启动/停机编排、任务监督、信号处理与退出码。业务项目**不要直接依赖本 crate**，经 `nasa` 门面开启 `application` feature 使用；使用入口与生命周期约束见仓库的快速开始和运维指南。

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
    // 业务启动 Hook：注册资源、登记受监督任务、注入路由/长连接定制;成功返回后资源封存。
    app.configure_router(|router| router)?;
    Ok(())
}
```

## `application` 支持的组件字符串

属性当前只接受下面 12 个小写字符串，名称区分大小写，不支持别名。业务可按任意顺序书写；宏会拒绝
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
| `"kafka"` | `kafka` | `kafka` 或 `kafkas.<client>` | 受管 producer/consumer、broker Ready、动态健康与两段停机 | `app.kafka(name)` |
| `"auth"` | `web`，并同时声明 `"web"`；直接使用 OAuth 类型再开 `oauth` | `auth` | 静态/远程 JWKS 首拉、刷新、认证器发布和 readiness | Web 安全流水线消费 |
| `"web"` | `web`；需要端点安全时使用 `web-security` | `server` | 自动收集端点、探针、监听与排空；定制经 `configure_router` | `app.web()` |
| `"ws"` | `ws` | `ws` | TCP/WebSocket 长连接监听与排空；鉴权和 endpoint 经 `configure_ws` 注入 | `app.ws()` |
| `"nacos-discovery"` | `nacos-discovery`；真实 provider 再加 `nacos-sdk` | `rest_discovery` | Start 装出站客户端、Ready 注册、停机先摘流后关客户端 | `app.nacos_discovery()` |
| `"scheduling"` | `scheduling`；选主模式使用 `scheduling-cluster` | `scheduling` | Ready 末尾启动已收集任务；选主模式复用已声明的 Redis 客户端 | `app.scheduling()` |

完整声明可以直接写成：

```rust
#[nasa::application(
    "log",
    "nacos-config",
    "telemetry",
    "db",
    "redis",
    "cache",
    "kafka",
    "auth",
    "web",
    "ws",
    "nacos-discovery",
    "scheduling"
)]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_router(|router| router)?;
    app.configure_ws(configure_socket_service)?;
    Ok(())
}
```

其中 `configure_socket_service` 是业务提供的 `fn(ServerBuilder) -> ServerBuilder`，至少需要注入鉴权回调；
声明了 `"ws"` 却没有完成该定制时，长连接组件会在 Ready 阶段明确拒绝启动。

对应的门面依赖至少为：

```toml
nasa = { version = "1", features = [
    "application", "log", "nacos-config", "telemetry", "tx", "redis", "cache",
    "kafka", "oauth", "web",
    "ws", "nacos-discovery", "scheduling",
] }
```

只用部分能力时只填写需要的字符串。例如纯 Web 应用写 `#[nasa::application("web")]`；本地配置的
数据库 Web 应用可写 `#[nasa::application("db", "web")]`。独立批处理仍可只开启 `kafka` feature 并显式
管理 `KafkaProxy`；Service 一旦把 `"kafka"` 写入属性，连接、消费、Ready、监控和停机就全部归容器所有。
`hystrix`、`grafana`、`mapper` 不是属性组件字符串，仍通过各自 feature 使用。

不要填写 `"nacos"`、`"discovery"`、`"database"`、`"websocket"` 或 `"schedule"`；对应的合法
字符串分别是 `"nacos-config"`、`"nacos-discovery"`、`"db"`、`"ws"` 和 `"scheduling"`。

规范顺序固定为 `log -> nacos-config -> telemetry -> db -> redis -> cache -> kafka -> auth ->
web -> ws -> nacos-discovery -> scheduling`。业务书写顺序不改变启动顺序；停机严格反向执行。
`auth` 缺少 `web` 会被拒绝，`cache.redis_ref` 指向受管 Redis 时还必须声明 `"redis"`。

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

### 可运行的 REST Producer → Consumer 验证

同级 `application-demo` 项目是完整样板，而不是只做连接探测：

- `POST /application-demo/ops/kafka/producer` 从请求 JSON 构造消息，通过受管 `ProducerLane` 等待 broker
  delivery；`stateful=false` 发送到属性宏 consumer，`stateful=true` 发送到 UserHook 注册的有状态 consumer。
- 两个 consumer 收到消息后都会打印包含事件名和业务序号的日志；日志不打印消息正文、broker 或凭据。
- `GET /application-demo/ops/kafka` 返回 lifecycle、动态 Ready、group assignment/commit、producer delivery
  统计，以及两类 consumer 的消费计数和最近业务序号。
- `/readyz` 只有在真实 group join 满足 `ReadyRule` 后才返回 200；进入停机后立即失去 Ready，再按容器反序
  停流量、排 consumer、停业务资源并最终 flush producer。

启动与人工验证：

```bash
cd ../application-demo
APP__KAFKA__BOOTSTRAP_SERVERS=127.0.0.1:9092 cargo run --offline

curl -X POST http://127.0.0.1:38080/application-demo/ops/kafka/producer \
  -H 'content-type: application/json' \
  -d '{"sequence":1,"message":"from REST","stateful":false}'

curl -X POST http://127.0.0.1:38080/application-demo/ops/kafka/producer \
  -H 'content-type: application/json' \
  -d '{"sequence":2,"message":"stateful from REST","stateful":true}'

curl http://127.0.0.1:38080/application-demo/ops/kafka
```

该示例的 topic 是 `application-demo-events`，默认 group 是 `application-demo-group`。复制到业务服务时必须按
上一节同步修改配置、宏、producer 和部署环境，不能只改 broker 地址。

## 生命周期要点

- `zcf/application.yml` 必须存在（内容可为 `{}`）；整个 `application.*`（name/mode/worker_threads/超时）是 bootstrap-only，远端首拉改写拒绝启动，运行期改写只记 `RestartRequired`。
- `mode: auto | service | batch`：auto 在声明 kafka/web/ws/nacos-discovery/scheduling 任一长生命周期组件时解析为 Service，否则 Batch；显式 Batch 不允许声明这些组件。
- 信号：broker ready 先于任何异步组件；Service 首次 Ctrl-C/SIGTERM 优雅停机退 0，Batch 未完成被取消退 128+signo；Stopping 中再次收到信号立即强退。
- 停机按 active stack 严格反序，所有清理共享 `application.shutdown_timeout_ms` 一个绝对预算；启动失败沿同一条回滚链，primary 错误不被回滚错误覆盖。
- 配置热刷新：整帧校验失败保留旧快照；可热刷组件（当前 log）成功记 `Applied`、失败保留 last-known-good 记 `ApplyFailed`；其余组件的段变化如实记 `RestartRequired`。`app.config_view()` 保证快照与状态表同版本。
- 错误报告统一脱敏（URI userinfo、常见敏感键）后输出完整错误链；进程级 panic hook 只写受控 location marker，不读 payload。

## 业务扩展点

| 入口 | 开放窗口 | 用途 |
| --- | --- | --- |
| `app.register / register_named / register_managed` | 启动 Hook | 把业务资源所有权交给容器；运行期只读借用 `app.resource::<T>()` |
| `app.spawn_critical / spawn_background` | 启动 Hook | 受监督任务；critical 提前退出触发失败停机 |
| `app.configure_router(...)` | 启动 Hook | Web 逃生舱：手写路由、全局中间件、`/hystrix.stream` 等 |
| `app.configure_mapping(...)` | 启动 Hook | 手动 global/scope/selector、窄 State 与安全运行时计划；`global = true` 的 interceptor 无需在此重复登记 |
| `app.configure_ws(...)` | 启动 Hook | 长连接逃生舱：`authorize`、endpoint 事件表、集群 notifier（声明 `ws` 组件时必须至少提供 `authorize`） |
| `app.configure_kafka(name, ...)` | 启动 Hook | 在自动收集项之后追加有状态 consumer；Ready 取走后入口永久封口 |
| `app.configure_kafka_metrics(name, sink)` | 启动 Hook | 为指定 client 安装一次无阻塞指标出口；未设置时为 Noop |
| `app.datasource(name) / redis(name)` | Start 完成后 | 直接取得共享语义明确、且能被容器显式关闭的数据源池与缓存客户端 |
| `app.kafka(name)` | Start 完成后至清理 | 受控发布、只读 metadata、健康快照和 consumer 控制命令；不暴露 connect/registry/shutdown |
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
