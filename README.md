# nasa-runtime-rust

NASA Rust 共享库是一组按特性组合的基础设施包。
**业务唯一入口是门面包 `nasa`**：业务项目只依赖 `nasa`，再按需开启 Saga、映射、事务、缓存、Redis、RedisJob、跨副本业务配额、WebSocket、配置、服务发现等特性。
其余成员用于实现和宏展开,默认不建议业务项目直接依赖。

> 名称声明：本项目是独立开源项目，与美国国家航空航天局不存在隶属、赞助、认可或官方项目关系，
> 也不使用其徽章、标识、印章或其它官方视觉标识。完整声明见 [NOTICE](NOTICE)。

## 受管多源运行时

Application 可从最终 YAML 同时创建多组 MySQL、PostgreSQL、Redis 与 Kafka 连接，并把它们冻结为当前进程唯一的
命名资源表。业务通过 qualifier 取得句柄；Inbox、Outbox、幂等、审计与 Saga 等持久适配器也把
datasource 身份固化在自身句柄中，不需要各自维护连接池或依赖隐式默认库。

```text
最终 YAML → 全表校验与逐源探测 → 冻结命名 registry → Ready
                                      │
                                      ├─ Application getter
                                      └─ 事务 / Mapper / 持久适配器
停机信号  ←  撤销 registry 发布  ←  停止新事务并按生命周期反向排空
```

```yaml
datasources:
  default:
    driver: mysql
    url: ${APP_PRIMARY_DB_URL}
  workflow:
    driver: postgresql
    url: ${APP_WORKFLOW_DB_URL}

outbox:
  datasource_ref: workflow
saga:
  database_bootstrap: application
  datasource_ref: workflow

redis:
  properties:
    primary:
      url: ${APP_PRIMARY_REDIS_URL}
      namespace: orders
      profile: RustV2
    sessions:
      url: ${APP_SESSION_REDIS_URL}
      namespace: sessions
      profile: RustV2
```

任一 source 无效都会在 Ready 前拒绝整张表；显式引用未知名称时不会猜测唯一实例，也不会回退到
`default`。同一原子链中的业务写、Inbox、Outbox、审计与 Saga 必须绑定同一 datasource。本能力不提供
跨数据库原子事务，也不热切换 endpoint、凭据或 source 集合；这些身份变化需要重启并重新完成启动
门禁。完整配置、查询入口、失败语义和停机边界见
[napp 的单源与多源章节](napp/README.md#yaml-创建单源与多源)。

## 跨副本业务配额

`rate-limit` feature 提供 `RateLimitProvider` 和共享 Redis 固定窗口实现，让所有副本对同一 tenant、
subject、API key 或客户端 IP 合并计数，扩容不会成倍放大业务总配额。业务从 Application 取得受管
Redis 客户端后显式构造 provider，并选择直接判定业务主体或安装 Web IP 中间件；它没有组件字符串和
独立 YAML 根，也不会自动改变路由。

Redis Lua 在服务端原子完成计数与首次过期设置。默认 Redis provider 在后端不可达、窗口非法或脚本
失败时 fail-open 并记录告警，优先保持业务可用；要求配额基础设施失效时拒绝请求的场景必须提供自己的
provider。该能力与 Web 的单实例令牌桶分层：前者约束跨副本总量，后者只保护当前进程。

## PostgreSQL 受管与独立持久能力

开启 `application` 与 PostgreSQL 对应 feature 后，`#[nasa::application("db")]` 会从 `database` 或
`datasources` 建立 PostgreSQL pool；同一 `datasources` 表可以同时声明 MySQL 与 PostgreSQL。跨 driver
名称空间由一个 catalog 冻结，业务分别使用 `app.datasource(name)` 与 `app.pg_datasource(name)` 取得
typed pool。任一 datasource 建池、迁移或引用校验失败都会阻止整张表进入 Ready。

不使用 Application 生命周期时，PostgreSQL 应用也可以直接组合命名事务、migration、Mapper 与
持久 adapter。`natx-pgsql` 提供 default/命名 datasource registry 和 ambient transaction；
`namigrate-pgsql`、`namapper-pgsql` 分别承载 schema 门禁与 SQL 映射；`naidempotency-pgsql`、
`nainbox-pgsql`、`naoutbox-pgsql` 与 `naaudit-pgsql` 复用同一 datasource，不各自创建连接池。

```text
PgPool registry ── natx-pgsql ambient transaction
       ├────────── migration / Mapper
       └────────── idempotency / Inbox / Outbox / audit
```

事务内跨 datasource 调用会在 SQL 前拒绝。跨数据库副作用不属于本地原子事务：需要源库 Outbox、目标库
Inbox 和稳定 `event_id` 收敛至少一次投递。PostgreSQL Outbox 以数据库 owner 租约和 fencing token
限制投递权威，通过 `FOR UPDATE SKIP LOCKED` 领取稳定 `id` 前缀；只有明确成功前缀可以标记完成。

受管 Outbox 与 Saga 根据 `datasource_ref` 的 driver 选择对应持久后端；二者形成同一原子链时必须指向
同一个 datasource。`full-pgsql` 提供 PostgreSQL-only 的完整门面组合，不拉入 MySQL SQLx runtime；
同时开启 `full` 与 `full-pgsql` 则允许两种 driver 在同一 Application 中共存。

YAML 中的 `migrations` 只定义门禁策略；Service 仍需在 UserHook 用
`app.configure_migrations(datasource, sqlx::migrate!("./migrations"))` 登记构建期嵌入的业务 SQL。
门禁模式不是 `disabled` 时，PostgreSQL 事务级代理还需提供与业务池指向同一 database/schema 的
`migrations.session_url`；Application
会在 advisory lock 前复验目标身份。完整入口与 Batch 边界见
[napp 的业务 migration 登记章节](napp/README.md#业务-migration-登记)。

## 持久化 Saga 编排

本仓库提供面向生产故障语义的 Saga：用**本地 ACID + Outbox 至少一次 + Inbox 幂等 + 持久化状态机 +
显式补偿**协调跨服务业务步骤。核心价值不是把远端调用包装成“分布式事务”，而是让进程崩溃、消息重投、
结果未知和多副本竞争都收敛到可恢复、可审计的数据库事实。

```text
Orchestrator DB                    Participant DB
Inbox + CAS/journal + timer        Inbox + gate + business fact
          │                                      │
          └─ command Outbox → transport → result Outbox ─┘
```

流程定义摘要、稳定 `effect_id`、取消/裁决屏障、冻结补偿计划和 timer fencing 共同阻止定义漂移、重复
副作用、未知结果误补偿与失权副本继续写入。Kafka 和 Redis Streams 提供受管消费接入；HTTP 可复用认证
构件；gRPC 提供框架 generated command/result service、mTLS 身份绑定与封闭收据。Saga 不提供跨服务
ACID、物理 exactly-once 或并发隔离，
业务资源竞争仍需唯一键、条件更新或语义锁。接入入口见 [Saga 快速开始](docs/quickstart.md#saga-最小接线)，
完整架构、迁移、运维与恢复边界见 [Saga 生产运行指南](docs/saga-production.md)。

## 稳定基础设施运行合同

Kafka Schema Registry、对象存储、gRPC listener 和受管 Web listener 已形成稳定公共合同，并继续用
独立 feature 控制依赖面；`full` 会显式纳入这些能力。它们都定义容量与超时上限、唯一所有权、
敏感信息脱敏、失败语义和可观测事实。

| 能力 | 解决的问题 | 运行架构 | 明确不负责 |
| --- | --- | --- | --- |
| [Schema Registry](nafka/README.md#schema-registry) | Confluent envelope、批准 ID、schema 拉取/兼容/注册与缓存 | Kafka codec 子能力；业务持有 client，无 Application 组件 | codec 生成、schema 治理、subject ACL、灾备复制 |
| [对象存储](naobject/README.md) | 有界单对象读写、条件创建、SigV4 与内容完整性 | provider-neutral trait + path-style S3 adapter；业务持有实例 | multipart、流式/range/list、STS 刷新、对象版本治理 |
| [gRPC listener](nagrpc/README.md) | 统一 codegen、generated service registry、HTTP/2/TLS/方法门禁、观测与有预算排空 | 独立 handle，或在 Ready 阶段交给 Application `"grpc"` 组件托管 | proto 业务语义、service mesh、客户端负载均衡 |
| [Web listener](napp/README.md#web-http-listener-受管模式) | 确定的 HTTP/1/h2c 选择、连接与 stream 上界、协议观测和有预算排空 | Application `"web"` 组件独占明文 TCP listener 与路由服务图 | TLS 终止、h2c Upgrade 协商、服务间协议选择 |

Schema Registry 和对象存储没有固定配置根，也不会因启用 Application 自动启动；业务从最终配置与
secret 快照构造，并可把低基数累计事实登记到统一 Prometheus/OTLP 目录。gRPC 受管模式则由容器独占
shutdown，在 initializer 全部完成前不会绑定端口。Web listener 只有在同时启用 `application,web`
feature 并声明 `#[nasa::application("web")]` 时才由容器创建；满足这些前提后，最终 YAML 的
`server.http2.enabled` 决定是否接受 h2c。各组件 README 是默认值、观测判读和成熟度边界的完整合同。

## 使用

### RedisJob Fanout 背压与观测

启用 `redis-job` 后，运行时按“计划冻结 → 能力快照 → 调度投递 → receipt/租约 → Handler 执行 → CAS 对账”的路径工作。Fanout 在目标节点持久确认接收后再申请本地执行槽。槽位不足时使用有界容量窗口和独立容量路由预算：超窗优先切换兼容执行器，无候选时继续保留当前 assignment，不把健康满载节点判为失联。shard 的 `capacityRouteTotal` 持久记录历史容量迁移次数，`capacityRouteCount` 只表示当前路由预算；实时指标用于告警，不跨进程重启累计。`JobContext::parameter::<T>()` 支持集合、映射和嵌套 JSON 参数，并执行结构安全门禁；非 JSON 参数由业务按声明 codec 从 `payload()` 解码。完整配置、边界和查询方式见 [nadis README](nadis/README.md#redisjob)。

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

`application` 同时提供业务初始化屏障：静态 `#[nasa::initializer]` 与启动 Hook 动态登记的
initializer 先合并冻结为一份依赖计划；migration 和出站依赖准备完成后再严格执行全部
`before -> initialize -> after` 三轮。全部成功前不开放入站监听、消费循环或服务发现。

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

可声明组件：`log`、`nacos-config`、`telemetry`、`db`、`redis`、`cache`、`partition`、`saga`、`kafka`、
`outbox`、`redis-job`、`grpc`、`auth`、`web`、`ws`、`nacos-discovery`、`scheduling`。宏接受任意书写顺序，并按规范顺序
启动、严格反序停机；声明了但特性未编入时会在编译期能力探测处失败。`saga` 会隐式加入 `db` 与
`outbox`，业务入口无需重复声明这两个组件；为兼容显式依赖，三者同时写出也合法且语义相同。
`hystrix`、`grafana`、`mapper` 是门面 feature 或函数级能力，**不是**可声明组件。
详见 [napp](napp/README.md)。

### Partition YAML 属性

`partition` 支持命名 Runner 映射，也兼容把单个 default Runner 的字段直接写在根下。Runner 字段全部
可省略：省略项逐字段采用有界默认值，显式配置只覆盖对应字段；不同 Runner 的默认值和覆盖值互不影响。

```yaml
partition:
  default_runner: default
  runners:
    default: {}
    settlement:
      partitions: 8
      global_inflight: 8192
      critical: false
```

容量、次数和时长必须为正数；`*_ms` 均以毫秒为单位，合法范围为 1..=31536000000（365 天）。
每个 Runner 先独立取得一份 `RunnerConfig::default()`，再用 YAML 中显式出现的属性逐项覆盖，因此业务侧
可以只写需要调整的键。以下表格覆盖解析器接受的全部属性：

| YAML 属性 | 类型 | 默认值 | 作用与约束 |
| --- | --- | --- | --- |
| `partition` | mapping | 无 | Partition 受管计划根；声明组件后必须提供该段，或在 UserHook 至少提交一个命名计划。`partition: {}` 是合法的扁平 default Runner 计划 |
| `partition.default_runner` | string | `default` | `app.partition()` 选择的稳定名称；只能用于命名形态，且必须精确匹配 `partition.runners` 中的一项 |
| `partition.runners` | mapping | 无 | 稳定名称到隔离执行域的映射；命名形态必须有 1..=4096 项，不能与任何扁平 Runner 属性混用 |
| `partition.runners.<name>` | mapping | `{}` | 单个 Runner 的稀疏覆盖；名称不能为空、不能有首尾空格或控制字符，UTF-8 编码最多 128 字节 |
| `partition.runners.<name>.partitions` | integer | 可用并行度的两倍，再向上取 2 的幂并封顶 65536 | slot 和唯一 worker 数；输入范围 1..=65536，非 2 的幂会向上规范化，因此其它下限校验使用规范化后的值 |
| `partition.runners.<name>.queue_capacity_per_type` | integer | 65536 | 每个 `(home, TaskType)` 尚未进入 Running 的许可上限，覆盖入队、移动和 worker 暂存；范围为 1..=Tokio `Semaphore::MAX_PERMITS` |
| `partition.runners.<name>.queue_capacity` | integer | 同上 | `queue_capacity_per_type` 的兼容键，语义和边界完全相同；同一 Runner 同时声明两个键会拒绝整份候选配置 |
| `partition.runners.<name>.global_inflight` | integer | `min(默认分区数 × 65537, Semaphore::MAX_PERMITS)` | 当前 Runner 的延迟、排队、移动和执行中任务共享总上限；范围为 1..=Tokio `Semaphore::MAX_PERMITS`，不会被 `partitions` 的显式覆盖联动重算 |
| `partition.runners.<name>.max_type_states` | integer | `max(4096, 默认分区数, 规范化后的 partitions)` | 当前 generation 全部 `(home, TaskType)` 状态与指标基数上限；必须不少于规范化分区数，且不超过 1048576 |
| `partition.runners.<name>.max_lanes` | integer | 同上 | `max_type_states` 的兼容键，语义和边界完全相同；同一 Runner 同时声明两个键会拒绝整份候选配置 |
| `partition.runners.<name>.frozen_evidence_capacity` | integer | 1024 | 最近失败证据环保留条数；必须大于零，满后覆盖最旧样本但继续累计总数与覆盖数，样本不持有业务任务 |
| `partition.runners.<name>.max_inbound_tunnels` | integer | 64 | 单个目标 slot 同时登记的严格与非严格入站盗洞共享上限；必须大于零，登记名额通过原子门禁裁决 |
| `partition.runners.<name>.idle_task_threshold` | integer | 8 | 选择热点源、普通空闲目标和观察严格归还时使用的逻辑任务阈值；必须大于零，已有同向借入可为后来出现的严格热点复用目标 |
| `partition.runners.<name>.strict_opportunity_attempts` | integer | 2 | 每个已投递窃取请求独立保留给严格候选的最大安装次数；必须大于零，非严格候选不消耗这部分机会 |
| `partition.runners.<name>.return_observations` | integer | 3 | 严格盗洞发起归还前，任务边界和低负载条件必须连续成立的观察次数；必须大于零，任一轮不满足会重新累计 |
| `partition.runners.<name>.tunnel_lease_ms` | integer | 2000 | 非严格盗洞没有真实发布或消费进展时允许保持开放的最长时间；到期先停止新发布，再排空既有任务 |
| `partition.runners.<name>.load_observer_interval_ms` | integer | 1000 | `Accepting` 期间集中 observer 扫描全部 slot 负载的间隔；每轮按 slot 数量扫描，不按类型数量扫描 |
| `partition.runners.<name>.control_tick_ms` | integer | 1 | 活动严格迁移、归还、Moving 审计和停止推进的兜底间隔；完全空闲且没有控制责任时只等待事件，不按该值轮询 |
| `partition.runners.<name>.transition_timeout_ms` | integer | 5000 | 单笔物理移动允许保持 Moving 的最长时间；超时后停止猜测位置并关闭能够证明的最小故障域 |
| `partition.runners.<name>.shutdown_timeout_ms` | integer | 2000 | Application 为该 Runner 分配的总收口上限，仍受应用剩余绝对期限约束；受管停机先使用其中一半尝试无损排空，允许升级时剩余部分用于显式有损收口 |
| `partition.runners.<name>.drain_batch` | integer | 64 | worker 每轮从主队列、控制队列和各盗洞方向处理的最大批量；必须大于零，用于限制单一方向长期占用 worker |
| `partition.runners.<name>.critical` | boolean | `true` | `true` 时该 Runner 的 `Degraded` 或 `Failed` 会使整体 readiness 失败并触发统一停机；显式设为 `false` 时只更新自身 readiness，不影响其它 Runner |
| `partition.runners.<name>.force_on_timeout` | boolean | `true` | `true` 时无损阶段未收敛会由 Application 显式调用 `force_stop`；显式设为 `false` 时保留未收敛错误，不隐式中止运行中任务 |

扁平单 Runner 形态把表中 `partition.runners.<name>.` 后的字段直接放到 `partition.` 下，名称固定为
`default`。未知字段、零值、非法时长、缺失 default、非法名称或兼容键冲突会在创建 worker 前拒绝。
运行架构、UserHook 命名计划和停机语义见 [napp Partition 受管模式](napp/README.md#partition-受管模式)。
`submit_after` 登记成功不保证未来一定执行：到期时仍会按当前类型容量和路由重新准入，稳定拒绝通过
`Submission::await_outcome()` 与 reason 返回。取消和到期重新准入竞争时只形成一个稳定终态，许可与
拒绝指标不会重复结算。

Partition 有两种生命周期所有者。直接使用 `napart::PartitionRunnerRegistry` 时，业务可在 Tokio runtime
运行期间按稳定名称动态 `get_or_create` 并 `start`，但必须共享同一个进程级注册表、限制名称总数并显式
停机；不同注册表中的同名 Runner 不共享顺序或容量。声明 Application 的 `"partition"` 组件时，YAML
和 Service UserHook 只负责提交启动期计划，全部名称在 Hook 结束时冻结，Prepare 统一启动并接管
readiness 与停机，Running 阶段不再追加受管 Runner。

### 业务初始化屏障

initializer 适合装配动态路由与注册表、恢复业务状态、回填或预热依赖。它不是新的组件字符串，而是
`application` 生命周期中位于 `Prepare` 与 `Seal` 之间的固定阶段：

```text
UserHook 登记 -> InitializerFreeze -> Prepare（migration / 出站门禁）
              -> before 全局轮 -> initialize 全局轮 -> after 全局轮
              -> Seal -> Ready（监听、消费与服务发现）
```

属性入口可省略 `name` 和 `order`：默认名称从实现类型派生 canonical kebab-case，默认顺序为
`100000`。依赖边始终优先于 `order`；同一可执行集合中 `order` 越小越先执行，再按名称稳定裁决。
Service 启动 Hook 也可用 `Application::register_initializer` 登记已经构造的实例，两种入口共享同一份
名称唯一性、依赖环和顺序校验。

失败、panic、启动超时或取消都会阻止 Ready，并沿 active stack 严格逆序停止受管任务、撤销
initializer action 并关闭资源。框架不能撤销已经提交到 DB、Redis 或 Kafka 的外部事实，因此
initializer 必须可安全重跑：单库多步写使用事务，跨资源事实使用稳定幂等键或与 Outbox 同事务提交。
阶段耗时和失败按稳定
initializer 名称输出低基数指标；停机逐步骤事件与最终有界摘要可用于核对真实清理顺序。

完整宏属性、`one-shot`/`hosted` 边界、条件工厂和运行时示例见
[napp 的业务 initializer 章节](napp/README.md#业务-initializer)。

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
| 业务初始化屏障 | `application` | `#[nasa::initializer(...)]`、`Application::register_initializer` |
| HTTP 路由 | `web` | `nasa::web::{get_mapping, mvc_router}` |
| 路由身份认证 | `web-auth` | `nasa::web::auth::{AuthProvider, AuthContext}` |
| 路由双协议加密 | `web-crypto` | `nasa::web::crypto::{CryptoRuntime, KeyRing}` |
| 历史 RSA 私钥协议迁移 | `web-crypto-legacy-rsa` | 编译期开关；仍需 provider 运行时显式允许 |
| 完整端点安全流水线 | `web-security` | `try_register_all`、`MappingRuntime`、route policy |
| SQL Mapper | `mapper` | `nasa::mapper::{Mapper, Query, Insert, Update, Delete}` |
| PostgreSQL SQL Mapper | `mapper-pgsql` | `nasa::mapper::pgsql::{Mapper, Query, Insert, Update, Delete}`；与 `application` 组合时复用受管 PgPool |
| Mapper L2 缓存 | `mapper-redis-cache` | `#[Query(..., cache = true)]` |
| PostgreSQL Mapper L2 缓存 | `mapper-redis-cache-pgsql` | `nasa::mapper::pgsql::{RedisMapperL2Cache, MapperL2Cache}`；与 MySQL 共享合同和进程默认注册槽 |
| MySQL 事务 | `tx` | `nasa::tx::{transactional, run}` |
| PostgreSQL 事务 | `tx-pgsql` | `nasa::tx::pgsql::{transactional, run}`；与 `application` 组合时启用 PostgreSQL 数据源生命周期 |
| Saga 纯合同 | `saga` | `nasa::saga::{WorkflowDefinition, SagaOutcome}` |
| Saga MySQL Runtime | `saga-runtime` | `nasa::saga::{Orchestrator, ParticipantRuntime, saga}`、`nasa::application::SagaApplicationPlan` |
| Saga PostgreSQL Runtime | `saga-runtime-pgsql` | `nasa::saga::pgsql::{PgOrchestrator, PgParticipantRuntime, saga}`、同一 `SagaApplicationPlan` 生命周期 |
| Saga Kafka command/result 托管 | `saga-kafka` | `nasa::saga::{SagaKafkaCommandConsumer, SagaKafkaResultConsumer}` |
| Saga PostgreSQL Kafka 托管 | `saga-kafka-pgsql` | `nasa::saga::pgsql::{SagaKafkaCommandConsumer, SagaKafkaResultConsumer}` |
| Saga Redis Streams command/result 托管 | `saga-redis-stream` | `SagaRedisStreamPublisher`、`SagaRedisStreamCommandConsumer`、`SagaRedisStreamResultConsumer` |
| Saga PostgreSQL Redis Streams 托管 | `saga-redis-stream-pgsql` | `nasa::saga::pgsql` 下相同 transport 类型 |
| Saga gRPC command/result transport | `saga-grpc` | `SagaApplicationPlan::with_grpc_command_service` / `with_grpc_result_service`、generated client、mTLS 身份绑定与封闭收据 |
| Saga PostgreSQL gRPC transport | `saga-grpc-pgsql` | `with_pgsql_grpc_command_service` / `with_pgsql_grpc_result_service` |
| 消费去重 Inbox | `inbox` | `nasa::inbox::MySqlInbox` |
| PostgreSQL 消费去重 Inbox | `inbox-pgsql` | `nasa::inbox::pgsql::PgInbox` |
| 受管事务 Outbox | `outbox` | `nasa::application::{OutboxApplicationPlan, OutboxHandle}` |
| PostgreSQL 受管事务 Outbox | `outbox-pgsql` | 同一 `OutboxApplicationPlan` 与按 driver 选择的持久后端 |
| 事务型业务审计 | `audit` | `nasa::audit::{MySqlOutboxAuditSink, TransactionalAuditSink}` |
| OpenAPI 3.1 | `openapi`（配合 `application` + `web`） | `Application::openapi_document`、`ApiSchema`、mapping 的 `request_schema` / `response_schema` |
| Secret/TLS 引用与两阶段轮换 | `secret` / `secret-http` / `secret-vault` | `RotatingSecretStore`、`RotatingTlsHttpClient`、`VaultKvV2Provider` |
| OAuth/JWKS/Metadata | `oauth` | `nasa::oauth::{MetadataClient, JwksRegistry}` |
| Schema Registry | `kafka-schema-registry`（蕴含 `kafka`、`secret`） | `nasa::kafka::{ConfluentSchemaRegistry, ConfluentEnvelope}` |
| 对象存储 | `object-store`（蕴含 `secret`） | `nasa::object::{ObjectStore, S3ObjectStore}` |
| gRPC listener | `grpc` | `nasa::grpc::{ServerPlan, GrpcServerConfig, GrpcServerHandle}`、`Application::register_grpc_service`、`#[application("grpc")]` |
| Redis 命令、Stream、锁 | `redis` | `nasa::redis::RedisClient` |
| 方法级 L1/L2 缓存 | `cache` | `nasa::cache::{cached, cache_invalidate}` |
| 接口保护与 Prometheus/Grafana 面板 | `grafana` | `nasa::grafana::{grafana, Command, metrics}` |
| 日志 | `log` | `nasa::log::LogManager` |
| yml 配置加载与文件观察 | `yml` / `yml-watch` / `config-boot` | `nasa::yml`、`nasa::yml::watch`、`nasa::yml::nacos` |
| 注册中心 | `nacos` / `nacos-sdk` | `nasa::nacos` |
| 静态/DNS 服务发现 | `discovery` | `nasa::discovery::{StaticDiscovery, DnsDiscovery}` |
| REST 负载均衡 | `rest-discovery` / `rest-discovery-nacos` | `nasa::discovery::rest` |
| 长连接 | `ws`、`ws-redis`、`ws-socketio` | `nasa::ws::Server` |
| 基础工具、加密、金额、图片 | `base`、`crypto`、`numeric`、`image` | 日期能力位于 `nasa::base::date`，其余使用对应同名模块 |
| 定时和异步任务 | `scheduling`、`scheduling-cluster` | `nasa::scheduling::{Async, scheduled}` |

`full` 用于现有完整能力构建，`full-pgsql` 用于不带 MySQL runtime 的 PostgreSQL 完整持久能力；两者都
不是生产服务的默认选择。生产项目仍应只启用实际使用的 feature，以免扩大依赖、安全和运行责任。

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

同仓实现 crate 之间只使用 registry 版本约束，工作区构建与公开归档保持相同依赖来源。下游 crate
只有在前置版本已经能从 registry 解析后才能发布，不得用 `[patch]` 掩盖缺失的前置发布。

### crates.io 依赖

发布到 crates.io 后，业务项目仍只依赖门面 `nasa`：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["hystrix", "cache", "ws-redis", "rest-client"] }
```

内部实现包按工作区 `Cargo.toml` 中的 package name 发布，例如 `nabase`、`naimg`、`naws`。
包名可用性是发布时事实，正式发布前仍须按 [发布指南](docs/publishing.md) 实时复查 crates.io，不能依赖
本地缓存结论。Cargo 包坐标不改变业务接口：门面模块仍是 `nasa::base`、`nasa::date`、
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
  datasource_ref: default
  timer_poll_interval_ms: 500
  timer_error_backoff_ms: 1000
  timer_operation_timeout_ms: 5000
  timer_failure_threshold: 3

outbox:                 # outbox 组件，也由 saga 隐式纳入
  datasource_ref: default
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

scheduling:             # scheduling 组件
  cluster: leader       # 可填 local、leader；leader 模式需要声明 redis 组件
  leader_key: scheduled:leader
  redis_ref: default

server:                 # web 组件
  host: 0.0.0.0
  port: 8080
  context_path: /app
  http2:
    enabled: false      # 显式开启后，同一明文端口接受 HTTP/1 与 h2c prior knowledge

ws:                     # ws 组件，独立于 server
  addr: 0.0.0.0:9000
  ws_addr: 0.0.0.0:9001

rest_discovery:         # nacos-discovery 组件
  enabled: false
  provider: nacos
```

手工装配（不使用应用运行时）的项目可自定根节点，启动顺序建议：

1. `naml` 加载本地 yml、profile、环境变量并解析 import 描述；`config-boot` 负责拉取远端配置并组装 overlay。
2. `nalog` 初始化日志。
3. `nadis` 初始化 Redis；`natx` / `natx-pgsql` 按实际 driver 初始化 MySQL/PostgreSQL pool。
4. `namapper`、`cacheable` 注入缓存和数据源。
5. `nanacos`、`rest-discovery` 初始化注册发现和 REST 负载均衡。
6. `naweb` 装配路由，业务自行拥有 HTTP listener、协议选择与排空；`naws` 启动长连接服务，`nasched`
   启动调度器。

`#[nasa::application]` 按规范组件顺序自动完成上述全部步骤，并补齐手工装配普遍缺失的部分：
信号处理、启动失败反向回滚、统一停机预算与配置热刷新；声明 `"web"` 时由 `napp` Web 组件启动受管
HTTP/1/h2c listener。

## 组件 README 索引

| 组件 | 门面模块或特性 | 主要场景 | 配置入口 |
| --- | --- | --- | --- |
| [nasa](nasa/README.md) | 门面包 | 统一导出所有业务能力 | 不直接读取 yml，按组件配置 |
| [napp](napp/README.md) | `application` / `rate-limit` | `#[nasa::application]` 生命周期编排，以及显式装配的跨副本 Redis 业务配额 | `application.*` 及各组件配置根；配额参数由业务构造 |
| [napp-macro](napp-macro/README.md) | `application` | `#[nasa::application(...)]` 属性宏与编译期校验 | 由 `napp` 运行时读取 |
| [naml](naml/README.md) | `yml` / `yml-watch` | 分层配置、来源追踪、精确文件观察与本地/Nacos import 中性描述 | `yml.*`、业务自定义根节点 |
| [config-boot](config-boot/README.md) | `config-boot` | 启动期读取本地和远端配置 | `nacos.*`、`nacos.imports` |
| [nanacos](nanacos/README.md) | `nacos` / `nacos-sdk` | Nacos 配置、注册、发现、监听 | `nacos.*` |
| [nadisc](nadisc/README.md) | `discovery` | 服务发现抽象、实例过滤、watch 契约 | `discovery.*` |
| [rest-discovery](rest-discovery/README.md) | `rest-discovery` | `lb://service` REST 负载均衡调用 | `rest.*` |
| [rest-discovery-nacos](rest-discovery-nacos/README.md) | `rest-discovery-nacos` | Nacos 注册发现和 REST 负载均衡组合 | `rest_discovery.*`、`nacos.*` |
| [rest-client-macro](rest-client-macro/README.md) | `rest-client` | 声明式 REST 客户端宏 | `rest_clients.*` |
| [naweb](naweb/README.md) | `web` / `web-security` | Axum 路由、interceptor 与端点安全运行时 | `server.*` 由 napp 读取 |
| [naweb-macro](naweb-macro/README.md) | `web` | MVC 风格路由注解和路由收集 | 编译期属性，无运行期配置 |
| [namapper](namapper/README.md) | `mapper` / `mapper-redis-cache` | MySQL 声明式 SQL Mapper、动态 SQL、二级缓存 | `mysql.*`、`datasources.*`、`mapper.*`、`redis.*` |
| [namapper-macro](namapper-macro/README.md) | `mapper` | Mapper 派生和 SQL 注解宏 | 由 `namapper` 运行时读取 |
| [namapper-core](namapper-core/README.md) | runtime 内部合同 | 后端中立分页、排序、缓存合同、共享缓存运行时与结构化 SQL 节点 | 不读取业务配置 |
| [namapper-pgsql](namapper-pgsql/README.md) | `mapper-pgsql` / `mapper-redis-cache-pgsql` | PostgreSQL Mapper、`$n` bind、动态 SQL、流式结果与 Redis L2 | standalone 显式注册 pool；Application 模式复用受管 datasource |
| [natx](natx/README.md) | `tx` | ambient MySQL 事务、after-commit 回调、多数据源 | `mysql.*`、`datasources.*` |
| [natx-core](natx-core/README.md) | runtime 内部合同 | datasource/driver catalog、owner、事务结果分类 | 不读取业务配置 |
| [natx-pgsql](natx-pgsql/README.md) | `tx-pgsql` | PostgreSQL ambient 事务、命名 pool、SQLSTATE 分类 | 可显式建池，也可由 `napp` 从 YAML 受管 |
| [natx-macro](natx-macro/README.md) | `tx` | `#[transactional]` 事务宏 | 由 `natx` 运行时读取 |
| [nasaga-core](nasaga-core/README.md) | `saga` | Saga 身份、definition、状态机、结果与补偿计划合同 | 无 I/O；definition 由业务注册 |
| [nasaga-backend](nasaga-backend/README.md) | runtime adapter 内部合同 | 后端中立行模型、分组 store 能力、封闭错误与 Saga/Inbox/Outbox/事务组合身份 | 无 SQLx driver，不执行 schema 自举 |
| [nasaga-mysql](nasaga-mysql/README.md) | runtime 内部 store | Saga journal、CAS、timer fencing、参与方 gate 与 migration | 复用 `natx` MySQL pool |
| [nasaga-pgsql](nasaga-pgsql/README.md) | runtime 内部 store | PostgreSQL Saga journal、CAS、timer fencing、参与方 gate、治理与迁移资源 | 复用 `natx-pgsql` pool；原子写要求同源 ambient transaction |
| [nasaga-runtime-core](nasaga-runtime-core/README.md) | backend wrapper 共享核心 | 唯一 Orchestrator/Participant 状态机、timer、恢复、transport 与治理计数 | 数据库事实由具体 store 提供 |
| [nasaga-runtime](nasaga-runtime/README.md) | `saga-runtime` / `saga-kafka` / `saga-redis-stream` / `saga-grpc` | Orchestrator、参与方事务 adapter、恢复管理、指标与受管 transport | 由业务注入 definition、受信 producer、路由与投递策略 |
| [nasaga-runtime-pgsql](nasaga-runtime-pgsql/README.md) | `saga-runtime-pgsql` 及 PostgreSQL transport feature | 组合 PostgreSQL Store/Inbox/Outbox/事务并复用唯一 Saga 状态机 | 可独立运行，也可交给 Application 托管 |
| [nasaga-macro](nasaga-macro/README.md) | `saga-runtime` | `#[saga]` descriptor 和类型化参与方 adapter | 编译期属性，无运行期配置 |
| [nadis](nadis/README.md) | `redis` / `redis-job` | Redis 单点或集群、nonce 幂等计数、流水线、数据流、锁与分布式任务 | `redis.*`、`redis.job.*` |
| [nadis-derive](nadis-derive/README.md) | `redis-derive` | Redis Search 文档派生 | `redis.search.*` 由业务映射 |
| [cacheable](cacheable/README.md) | `cache` | L1/L2 缓存、刷新保护、失效广播 | `cache.*`、`redis.*` |
| [nacache-macro](nacache-macro/README.md) | `cache` | `#[cached]`、`#[cache_invalidate]` | 由 `cacheable` 运行时读取 |
| [naidempotency](naidempotency/README.md) | `nasa::idempotency` | 幂等状态机、首次执行、重放与冲突裁决 | 无固定 yml；由业务注入 store |
| [naidempotency-mysql](naidempotency-mysql/README.md) | `idempotency-mysql` | 与业务事务共享记录或提供持久响应重放 | 复用 `database.*` |
| [naidempotency-pgsql](naidempotency-pgsql/README.md) | `idempotency-pgsql` | PostgreSQL 租约 fencing、持久重放与命名 datasource | 复用显式或受管 `natx-pgsql` pool |
| [naidempotency-redis](naidempotency-redis/README.md) | `idempotency-redis` | 有 TTL 的跨副本响应重放 | 复用 `redis.*` |
| [naoutbox-core](naoutbox-core/README.md) | `nasa::outbox` | Outbox 事件、发布端、保序投递与持久 adapter 角色合同 | 无运行期 yml |
| [naoutbox-mysql](naoutbox-mysql/README.md) | `outbox` | 同事务写事件、单 owner dispatcher、可选死信 | 复用 `database.*` |
| [naoutbox-pgsql](naoutbox-pgsql/README.md) | `outbox-pgsql` | PostgreSQL 同事务追加、fenced dispatcher、lane、配额与保留 | 可独立使用；Application 负责受管 dispatcher 生命周期 |
| [nainbox-core](nainbox-core/README.md) | `inbox` 内部合同 | 后端中立消费去重与事务结果裁决 | 无运行期 yml |
| [nainbox-mysql](nainbox-mysql/README.md) | `inbox` | Inbox 标记与业务副作用同事务 | 复用 `database.*` |
| [nainbox-pgsql](nainbox-pgsql/README.md) | `inbox-pgsql` | PostgreSQL Inbox 标记与业务副作用同事务 | 复用显式或受管 `natx-pgsql` pool |
| [naaudit](naaudit/README.md) | `audit` | 脱敏业务审计事件与事务型 sink 合同 | 无独立配置根 |
| [naaudit-mysql](naaudit-mysql/README.md) | `audit` | 审计事件写入同事务 MySQL Outbox | 复用 `database.*` |
| [naaudit-pgsql](naaudit-pgsql/README.md) | `audit-pgsql` | 审计事件写入同事务 PostgreSQL Outbox | 复用显式或受管 `natx-pgsql` pool |
| [hystrix](hystrix/README.md) | `hystrix` | 熔断、隔离、超时、指标流 | `hystrix.*` |
| [hystrix-macro](hystrix-macro/README.md) | `hystrix` | `#[hystrix]` 宏 | 由 `hystrix` 运行时读取 |
| [nafana](nafana/README.md) | `grafana` | 接口隔离、Prometheus 指标、Grafana 原生自适应接口墙 | `grafana.*`、`/metrics` |
| [nafana-macro](nafana-macro/README.md) | `grafana` | `#[grafana]` 编译期参数校验与包装 | 无运行期 yml |
| [nametrics-core](nametrics-core/README.md) | `application` 内部合同 | 单一指标目录、冲突审计、同源 Prometheus/OTLP 快照与样本拒绝诊断 | 无运行期 yml |
| [natelemetry](natelemetry/README.md) | `telemetry` 内部运行时 | Trace Context、有界 span 队列，并由 `napp` 统一管理 OTLP trace/metrics 停机 flush | `telemetry.*` 由 `napp` 读取 |
| [nasched](nasched/README.md) | `scheduling` / `scheduling-cluster` | 异步任务、定时任务、Redis 集群去重 | `scheduling.*` |
| [async-macro](async-macro/README.md) | `scheduling` | `#[Async]`、`#[scheduled]` 宏 | 由 `nasched` 运行时读取 |
| [napart](napart/README.md) | `partition` | 命名 Runner 隔离、严格 FIFO 的保序任务窃取、有界背压、延迟稳定终态与可证明停机 | 直接模式运行期动态创建并显式停机；Application 模式由 YAML/UserHook 提交启动期计划 |
| [naws](naws/README.md) | `ws` / `ws-redis` / `ws-socketio` | TCP/WebSocket 长连接、鉴权、广播、背压 | `ws.*` |
| [naws-proto](naws-proto/README.md) | `ws` | 长连接协议帧和编码模式 | `ws.protocol.*` |
| [naws-proto-derive](naws-proto-derive/README.md) | `ws` | 协议结构体派生 | 网络配置由 `naws` 读取 |
| [nafka](nafka/README.md) | `kafka` / `kafka-schema-registry` | 发布、消费、路由、确认，以及可选 Confluent envelope 与有界 schema client | Kafka 用 `kafka.*` / `kafkas.*`；Registry 由业务配置投影 |
| [nafka-macro](nafka-macro/README.md) | `kafka` | `#[kafka_consumer]` 静态收集 | 由 Kafka 运行时读取 |
| [ncrypto](ncrypto/README.md) | `crypto` | 现代令牌加密和历史兼容加解密 | `crypto.*`、环境变量承载密钥 |
| [nanum](nanum/README.md) | `numeric` | 定点金额、价格、最小变动单位对齐、舍入 | `numeric.*` |
| [naimg](naimg/README.md) | `image` | 图片压缩、尺寸裁剪、格式转换 | `image.*` |
| [nalog](nalog/README.md) | `log` | 控制台和文件日志、级别热切换 | `log.*` |
| [nabase](nabase/README.md) | `base` | BaseResponse、日期时间、ByteSize、Snowflake、字符串、环境变量和翻译抽象 | `base.*`；日期配置由业务投影 |
| [nabudget](nabudget/README.md) | REST/Web 内部合同 | 绝对 deadline 与取消树 | 无运行期 yml |
| [namigrate](namigrate/README.md) | `application` + `tx` | MySQL migration validate/apply 门禁 | `database.migrations` |
| [namigrate-core](namigrate-core/README.md) | runtime 内部合同 | 后端中立 migration 状态比较与封闭失败分类 | 不读取业务配置 |
| [namigrate-pgsql](namigrate-pgsql/README.md) | `tx-pgsql` 内部迁移门禁 | PostgreSQL migration validate/apply、session advisory lock 与非事务完成证据 | standalone 显式调用；Application 在 Prepare 执行 |
| [naopenapi](naopenapi/README.md) | `openapi` | 从已审计路由事实生成确定性 OpenAPI 3.1 | `application.*` 文档信息 |
| [naauthz](naauthz/README.md) | `application` + `web` 内部合同 | 路由 scope 与对象级授权 | 策略由代码或外部 provider 注入 |
| [nauth-oauth](nauth-oauth/README.md) | `oauth` | JWT、JWKS 与授权服务器 metadata | `auth.*` 由 `napp` 读取 |
| [nasecret](nasecret/README.md) | `secret` | 分片解析、脱敏快照与两阶段轮换 | `secrets.*` |
| [nasecret-http](nasecret-http/README.md) | `secret-http` | 随 secret 代际轮换的 TLS/mTLS HTTP client | 引用 `secrets.*` ID |
| [nasecret-vault](nasecret-vault/README.md) | `secret-vault` | 有界 KV v2 secret provider | provider 配置由业务投影 |
| [naobject](naobject/README.md) | `object-store` | 有界对象合同与 S3-compatible adapter | 无受管组件配置；业务显式构造 |
| [nagrpc](nagrpc/README.md) | `grpc` | 统一 codegen/service registry、HTTP/2/TLS、健康、反射、方法策略、观测与排空 | `grpc.*`；可独立构造或交给 Application 托管 |
| [nagrpc-build](nagrpc-build/README.md) | `grpc` 的 build dependency | vendored protoc、descriptor/摘要、受管 server adapter 与兼容门禁 | 只写 Cargo `OUT_DIR` |
| [macro-support](macro-support/README.md) | 宏内部依赖 | 过程宏路径解析 | 无运行时 yml |

## 安全说明(务必阅读)

- **`ncrypto` 的弱加密是【刻意兼容历史实现】,不是缺陷、也不适合作新系统的机密性边界。**
  为逐字节对齐既有服务,ncrypto 保留了 AES-ECB、CBC(IV=Key)、RSA PKCS#1 v1.5、
  以及"用 RSA 私钥做保密"等**已知弱**的构造。**只用于与既有系统互操作**;新系统请用
  `nasa::crypto::encrypt_modern` / `decrypt_modern` 这类现代入口,不要复用这些兼容函数。现代入口默认使用
  随机盐 + Argon2id + AES-256-GCM，返回自描述 `NC2.*` 令牌；业务可用 AAD 绑定租户或记录上下文。
  既有 PBKDF2-HMAC-SHA256 的 `NC1.*` 仅保持兼容读取，错误口令、AAD 错配或密文篡改都会失败。

- **`rsa` 0.9 计时侧信道（RUSTSEC-2023-0071，Marvin 攻击）当前没有上游安全更新。**
  由 ncrypto 引入。默认构建只保留 RS256 公钥验签等不执行易受攻击私钥解密的能力；历史
  PKCS#1 v1.5 私钥解密与私钥 type-1 运算受专用编译 feature 和 Web 运行时开关双重隔离，
  且不进入 `full`。`deny.toml` 仍按包级 advisory 显式登记，待上游提供安全更新后移除。

- **拒绝服务攻击防护与资源上限**（默认已提供保守兜底，可按部署调整）：
  - `ws`:`ServerConfig.max_connections`(连接总数,accept 处背压)、`max_unauthenticated`
    (未认证连接数,防慢握手/慢鉴权占满连接池)、`max_inflight_handlers`(全局)
    与 `max_inflight_handlers_per_conn`(单连接配额,防单连接抢占全局池)。
  - `partition`:每个命名 Runner 独立限制类型排队、全局在飞、类型基数和入站盗洞；`submit` 的
    满载、停机和隔离失败对调用方**可见**，`submit_async` 提供等待容量的真背压；已登记 delayed
    任务到期拒绝同样形成可等待的稳定终态。
  - `image`:输出像素上限(`MAX_OUTPUT_PIXELS`)防解压炸弹式放大。

## 归档边界

组件 crate 与 `.crate` 归档只携带产品源码、公开文档和再分发所需文件。真实后端连接信息只能由部署
环境注入，不得把内网地址、账号或密码写进说明文档或配置样例。

## 开源文档

| 文档 | 用途 |
| --- | --- |
| [快速开始](docs/quickstart.md) | 业务应用如何依赖 `nasa`、选择特性、配置 yml 和编写最小示例。 |
| [部署指南](docs/deployment.md) | 应用模式构建、配置注入、容器信号、健康端点和接流条件。 |
| [运维指南](docs/operations.md) | 运行状态、退出码、停机顺序、配置刷新和故障排查。 |
| [交付就绪清单](docs/release-checklist.md) | 产品归档、组件边界和生产环境批准条件。 |
| [公开归档说明](docs/publishing.md) | 多包依赖拓扑、归档内容和许可说明。 |
| [贡献指南](CONTRIBUTING.md) | 贡献规则、文档、注释和代码维护约束。 |
| [安全说明](SECURITY.md) | 安全报告方式、敏感面和默认安全策略。 |
| [当前变更说明](CHANGELOG.md) | 当前公开业务能力概览。 |

## 许可证

采用双许可证：[MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE)，二选一。
