# nasa

[中文](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.md) | [English](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.en.md)

`nasa` 是面向 Rust 服务端应用的受管生命周期与可靠业务执行门面，也是 `nasa-runtime-rust` 的唯一业务入口。
应用只依赖本 crate，通过 feature 选择能力，再从
`nasa::<module>` 使用稳定入口；实现 crate 和宏 crate 由门面按需引入。
从 [最小服务](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/quickstart.md#最小可运行服务)
开始，或阅读 [架构说明](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/architecture.md)
与 [接入与升级](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.md)。
启用 `application` 后，业务初始化与优雅停机收尾进入同一生命周期：Ready 前执行初始化屏障，
受监督任务结束后按优先级执行一次性收尾，最后释放业务资源。
`redis` 提供业务键有序分区消费：不同 Redis 源独立执行，同源支持 `source`、`group`、`stream`
隔离；ACK 不确定保留提交责任，跨域同业务键仍等待前序确认或重试收口。
可靠 Saga client 把业务事实、start-intent 与 dispatcher 固定到同一事务域，配置冲突在接流前失败；
直接取消 Runner 也不会先释放仍存活任务所依赖的资源。
受管 Orchestrator 将已提交 result 的恢复资格与 command 路由分开，允许保护态参与方重投原事件，
同时持续关闭新 Start、timer claim 和 Ready；在途事务始终受原安全发布代际与期限约束。
与 `mapper`/`mapper-pgsql` 组合时，Application 自动装配 SQL 原子指标、受控日志、有界通知和指标
出口，业务通过同一份 YAML 配置；参数默认不输出，通知故障不会改变 SQL 与事务结果。
业务通过 `nasa::application::notifications::init(Arc<dyn Notify>)` 安装进程通知实现；没有实现就忽略，
默认不要求 provider 配置。通知微服务的协议与客户端由业务实现，框架不连接具体消息渠道。

本 crate 属于独立开源项目，与美国国家航空航天局不存在隶属、赞助、认可或官方项目关系；完整
声明随包交付于 `NOTICE`。

Application 标准纳管 Redis Stream／Proxy／AutoPipeline、出站 TCP 帧客户端、hystrix 命令目录、Mapper 缓存、命名幂等与审计、REST、对象存储、Schema Registry、
secret/TLS 与本地文件监听。业务提供配置与 handler，框架负责接流前装配、健康监督、配置应用状态
和停机，无需另建资源关闭流程；各能力的 feature、入口、配置和失败边界见
[受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
Redis 消费、微批调用和出站 Client 与宿主终端共用启动许可，关键资源失效会阻止 Ready；
hystrix 命令目录随 Application 实例重建，业务收尾后排干，旧命令永久拒绝执行。

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

门面只组织公开类型和 feature，不持有额外运行状态。业务 listener 与消费端由属性声明；编入观测
能力后，其 source 自动登记，通知与指标 listener 仍必须由 YAML 显式启用。
业务仍负责领域定义、业务副作用、实际容量、凭据来源和外部系统治理；Application 负责已声明组件的
配置门禁、受管协议路由、Ready 监督和反向停机。

其中的 Saga 能力用本地事务、Outbox/Inbox、持久化状态机、稳定效果身份和显式补偿组成最终一致性
闭环。声明 #[nasa::application("saga", "web")] 后，配置中的 saga.role 明确选择 orchestrator、
participant 或 client；managed 模式支持 HTTP、gRPC、Kafka 与 Redis Streams command/result 数据面，
并由 Application 构造标准 API、Catalog、签名 definition 发布、capability 续租、timer 与 dispatcher，
普通业务不提交运行计划。Saga 不把远端调用伪装成跨服务 ACID，也不承诺物理 exactly-once 或并发隔离。
完整合同见
[napp Saga 受管模式](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#saga-受管模式)。

Saga 支持 MySQL/PostgreSQL、HTTP/gRPC 共用的管理语义、原始字节与 schema 合同，以及 Nacos
协调侧服务发现。HTTP HMAC 与 gRPC mTLS 可通过完整配置快照热轮换；definition 退休保留实例、
消息与审计引用门禁。部署、轮换与观测配置见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。

门面还提供四项稳定基础设施合同：有界 Schema Registry client、有完整性门禁的对象存储 adapter、
可独立运行或交给 Application 托管的 gRPC listener，以及由 Application 独占的受管 Web listener。
同时启用 `application,web` 并声明 `#[nasa::application("web")]` 后，Web 默认只接受 HTTP/1；在最终
YAML 中设置 `server.http2.enabled=true`，同一明文端口即接受 h2c prior knowledge 并继续兼容
HTTP/1，其它 transport 参数均可省略并采用受校验默认值。四项能力保持独立 feature 以控制依赖面，
同时纳入 `full`；所有权、运行边界和非目标在下文单独说明。

## SQL 阈值通知与观测架构

同时启用 `application` 与 `mapper`/`mapper-pgsql`，Application 会自动登记方法、数据库执行、连接
等待、Pool 与通知指标。完成路径只更新原子事实并尝试入队；受管 worker 调用业务实现，指标出口
读取统一快照。无需业务编写 Mapper Hook 或周期采集任务。

业务实现 `nasa::application::notifications::Notify`，通过同模块的 `init(Arc<dyn Notify>)` 安装一次。
没有实现时忽略通知，稍后初始化不会补发；重复安装返回错误。适配器可以调用独立 `telegram-bot`
微服务，HTTP REST/gRPC、鉴权和最终发送由业务负责，框架不连接机器人。

```yaml
sql:
  observability:
    slow_sql:
      threshold_ms: 1000
    alerts:
      slow_sql:
        enabled: true
        cooldown_ms: 0
```

原始 SQL 耗时达到或超过阈值时命中；默认 60000 ms 冷却不适用于逐条通知。日志开关与通知开关
独立，Stream 使用底层活跃取行耗时而非消费者等待。队列满、下游失败和进程退出可能丢失通知，
不会改变 SQL 返回或事务裁决；直接调用 `notify` 不自动获得受管队列、超时和异常隔离。

`grafana.observability` 显式启用独立/Web 抓取或 remote write，关闭出口不停止内部采集。
平台资源由独立 controller 管理，remote write 失联只引用平台提供的期望实例指标。
完整配置见 [SQL 观测](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/namapper-core/README.md#sql-观测与配置)、
[通知接口](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nanotify-core/README.md) 和
[观测出口](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafana/OBSERVABILITY.md)。

## 请求安全与链路传播

同时启用 `application,web` 后，通过 `nasa::application` 使用 `PolicyRegistry`、
`PolicyDecisionSnapshot` 与 `RequestSecurityContext`。策略集合、未命中三态缺省和 generation 同代冻结，
Web 边界、registry 入口与 handler 请求上下文不会各自读取不同代配置；显式策略不会被公开路由豁免
绕过。对象授权沿用同一请求快照，provider 缺失、拒绝、错误或超时都拒绝访问。身份验签仍由
`nasa::oauth` 或业务认证层完成，授权入口只消费已经验证的 `Principal`。

```text
OAuth/JWKS 或业务认证 → Principal → route 完整快照 → handler 对象授权
合法 traceparent ───────────────────→ REST / Kafka 继续传播
无上游上下文 ── exporter sampler ───→ 新根；无 exporter 时保持未采样
```

链路传播严格继承合法上游的 sampled 位，只有受管 exporter 能按 `root_sample_ratio` 裁决
无上游的新根。`application,web` 组合通过 `nasa::application::TraceContext` 暴露链路上下文；
再启用 `telemetry` 并声明同名组件时，由 Application 配置并持有 exporter。
`nasa::scheduling` 在 leader 与 claim 权威均取得后才创建调度执行 span；拒绝拍次只
记录 Skipped。该组合提供传播与低基数观测，不替业务建立身份信任、对象归属、跨服务采样协调或
exactly-once 调度。

```toml
[dependencies]
nasa = { version = "2.0.1", features = [
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

启用 `application` 后，MySQL、PostgreSQL、Redis 与 Kafka 都由最终 YAML 创建，业务不在 `main` 中自行建池或维护
第二张连接表。单源与多源配置根、默认身份及选择入口如下：

| 资源 | 单源 | 多源 | 选择入口 |
| --- | --- | --- | --- |
| MySQL | `database` | `datasources.<name>` | `app.default_datasource().await` / `app.datasource(name).await` |
| PostgreSQL | `database` | `datasources.<name>` | `app.default_pg_datasource().await` / `app.pg_datasource(name).await` |
| Redis | 扁平 `redis` | `redis.properties.<qualifier>` | `app.default_redis().await` / `app.redis(name).await` |
| Kafka | `kafka` | `kafkas.<client>` | `app.default_kafka()` / `app.kafka(name)` |

Application 先校验完整命名表，再按稳定名称逐源探测，全部成功后一次性发布。同一 `datasources` 表可
同时声明 MySQL 与 PostgreSQL，Outbox 与 Saga 通过 `datasource_ref` 的 driver 选择持久后端；Cache、缓存失效广播与 Scheduling 通过 `redis_ref` 选择 Redis；Kafka
consumer/producer 使用 client name。引用不存在时会在 Ready 前失败，不会回退默认源。

两种数据库持久适配器都提供显式绑定入口。MySQL 使用 `MySqlInbox::with_datasource`、
`MySqlOutbox::with_datasource`、`MySqlIdempotencyStore::with_datasource`、
`MySqlOutboxAuditSink::with_datasource`、`MySqlSagaStore::with_datasource`，以及
`Orchestrator::with_datasource` 和 `ParticipantRuntime::with_datasource`；PostgreSQL 对应入口位于
`nasa::inbox::pgsql`、`nasa::outbox::pgsql`、`nasa::idempotency::pgsql`、`nasa::audit::pgsql` 和
`nasa::saga::pgsql`。同一原子链必须使用相同
qualifier；不同 datasource 之间不构成一个事务。source 集合、endpoint、凭据和身份字段在运行期
保持冻结，变化后必须重启。完整 YAML 与生命周期合同见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#yaml-创建单源与多源)。

### Inbox 保留治理

`inbox` 与 `inbox-pgsql` 除了导出事务内 claim adapter，也向 Application 提供显式
`InboxRetentionPlan`。业务必须按消息源事实声明最大重投视界和不小于该视界的标记年龄；Application
不从 Kafka topic、消费组或数据库配置猜测窗口。计划在 Ready 后由单一串行 fixed-delay 循环执行，
停机先停止新轮次，adapter 在取消或预算耗尽时不会把锁状态未知的 session 放回池。

`Application::inbox_retention_snapshot()` 与统一指标端点公开轮次、删除、owner 争用、预算耗尽、失败
轮次和最老候选年龄的无标签聚合账目。该能力只清理已经提交且超过安全窗口的去重标记，不创建生产
索引，不延长消息系统实际可重投的期限，也不改变 Inbox 仅覆盖同一数据库事务内副作用的边界。完整
配置入口和指标名见 [napp Inbox 章节](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#inbox-去重标记保留)。

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
组件字符串。Saga 角色、角色数据源、API 暴露、command/result transport、安全引用与运行预算必须在
受信配置中显式给出；缺失时不会按链接内容或唯一数据源猜测。direct client 是唯一不创建本地 DB/Outbox
资源的 managed 角色。
可靠 client 的业务事务、start-intent 和 dispatcher 必须同源，发布计划固定绑定
`saga.client.datasource_ref`。显式 `outbox.datasource_ref` 与之冲突时配置校验失败，应用不会进入
Ready；未设置该字段时不影响 client 的数据源绑定。

### 可靠 Saga client

`app.saga()?.remote_client()?.enqueue_start(...)` 必须在 client 指定数据源的业务事务内调用。
返回 `event_id` 只确认事务内追加，外层事务明确提交后才可对外返回本地已受理；此时远端仍可能尚未
创建 Saga。dispatcher 使用同一数据库持续投递，HTTP/gRPC 收据丢失时保持原事件身份重投，只有
`Committed` 或 `Duplicate` 才标记投递完成。省略 Outbox 段或只配置轮询预算不会改变扫描数据源。

该能力不建立跨库事务，不把 Ready 当作流程完成证明；运行中应同时关注 `napp_outbox_pending`、
`napp_outbox_published_total`、`napp_outbox_dead` 和远端实例查询。完整配置见
[client 发起等级](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#client-发起等级)。

### 已提交 Saga result

结果恢复只接受原参与方本地事务已经提交的事件，不能代替参与方证明业务事实。受管入口要求共享
Catalog 覆盖在途实例的冻结 definition，并继续核对 producer 信任、事件身份、Inbox 和状态机合同。
command route 暂缺时可以保持原结果收敛，但不会开放新业务入口。请求冻结截止时刻、撤销身份、安全
发布 generation 与合同摘要；HTTP、gRPC、Kafka 和 Redis Streams 在实例锁等待及事务交还前持续复验。
失权完整回滚且保留原事件重投，安全材料经历 A→B→A 也不会恢复旧资格。

## Redis 分区消费门面

启用 `redis` 后，通过 `nasa::redis::{PreparedPartition, PartitionRecord, RunningPartition}` 登记
不可变消费计划并显式启动。Redis 租约与 PEL 承担持久接管，消费器私有的 napart Runner 集合承担
本地执行；消费器不复用 `nasa::partition` 中 Application 的命名 Runner，也不要求业务额外声明
Application 的 `"partition"` 组件。

`redis.partition.executor.scope` 默认 `source`，同源全部组共享一个 Runner；`group` 按默认组和
隔离组拆分，`stream` 按 `(逻辑组, 物理分区编号)` 拆分。多源配置把对应字段放在
`redis.properties.<qualifier>.partition.executor` 下，不同源始终拥有各自的注册表和容量。
`group`、`stream` 在源级总预算内切出固定份额，未满足每域整批读取下限时拒绝启动。

同一实例、同一计划、同一业务键的顺序覆盖 handler、ACK、精确重试和 Park，即使它们跨越不同
Runner。进程崩溃后的交付仍为至少一次，需要业务幂等；同步阻塞和共享 Redis 后端故障不在本地
执行隔离的保证内。观测使用 `snapshot()`、`publisher_snapshot()` 与 `async_delete_pending()`。

Application 通过 `configure_redis_partition(source, critical, configure)` 接收 handler，负责 Prepare、
Ready、逐来源健康与聚合停机；单独声明 `"redis"` 不会自动注册 handler。所有来源先关闭准入，再
共用截止点并发排干，未完成时保留依赖责任。独立使用方持有 `RunningPartition` 并自行等待
`shutdown_until(deadline)`；只有显式 `force_shutdown_until` 才请求有损中止。
完整配置、逐条注册和容量算法见
[nadis 分区消费](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadis/README.md#业务键有序分区消费)。

## RedisJob 门面

启用 `redis-job` 后，业务从 `nasa::redis::job` 使用定义、上下文、结果、控制和查询类型，并用
`#[nasa::redis_job]` 静态登记 Handler。`#[nasa::application("redis-job")]` 会隐式纳入 Redis transport；
宏 descriptor 与 UserHook 中通过 `app.configure_redis_jobs(plan)` 提交的动态定义进入同一冻结计划，
无需业务拼接 Lua、管理扫描器、续租、Fanout 订阅或停机任务。启动登记部分成功时运行时会先关闭本地准入，
再按全部已尝试能力坐标执行补偿注销；Redis 持续不可达时，未确认的远端记录依赖 TTL 与 Registry GC
收敛。停机对超时 task 发出取消后仍等待其实际退出，Registry 注销不会越过仍存活的执行权 guard。

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
[nadis README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadis/README.md#redisjob)。

## 跨副本业务配额门面

`rate-limit` 蕴含 `application` 与 `redis`，从 `nasa::application` 暴露后端中立
`RateLimitProvider`、共享 Redis 固定窗口实现和可选 Web 配额中间件。业务仍需在应用入口声明
`"redis"` 组件，再从受管 source 构造 provider；feature 本身不自动启动资源或修改路由。

```rust
use std::time::Duration;
use nasa::application::{RateLimitProvider, RedisRateLimitProvider};

#[nasa::application("redis")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let provider = RedisRateLimitProvider::new(app.default_redis().await?, "checkout");
    let outcome = provider
        .check("tenant-a", 100, Duration::from_secs(60))
        .await;
    if !outcome.allowed {
        // 业务在协议边界映射拒绝响应；主体身份与配额语义不由框架猜测。
        return Err(anyhow::anyhow!("business quota exceeded"));
    }
    Ok(())
}
```

所有共用 Redis 与 namespace 的副本合并同一主体计数。默认实现后端失败时 fail-open，也可在构造期
选择 fail-closed。与 `web` 同时启用后，`distributed_rate_limit` 可按已解析客户端 IP、已验证
Principal 的 tenant 或 subject/client_id，以及摘要后的 API key 计量；来源缺失时按冻结策略放行或
拒绝，超额返回 `429` / `Retry-After`。完整装配顺序、窗口上限和键空间合同见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#跨副本分布式业务配额)。

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

## 业务优雅停机任务

通过 `application` feature 使用 `nasa::Application` 时，业务可在 UserHook 登记只执行一次的异步收尾：

```rust
let stop = client.clone();
app.register_graceful_shutdown(100, "orders-client", async move {
    stop.close().await
})?;
```

任务按 `priority` 升序执行，同一优先级按登记顺序执行。名称在当前 Application 内唯一，必须是非空、无控制
字符、URL、地址、凭据语义 token 或明显动态身份且不超过 128 个 UTF-8 字节。形态门禁不能证明实际业务基数，
也不按普通身份词前缀猜测未知字母串；调用方必须使用固定业务名称，不得拼接租户、请求或对象身份。
单个 Application 最多 256 项。Service 和 Batch 都可登记，但 UserHook
关闭后、Seal 后或进入运行期再登记会返回错误。两种模式都先收口受监督任务，再执行业务停机任务，随后
释放 UserHook 登记的业务资源。Service 在业务任务前完成 NotReady、入口摘流和 initializer action
清理；Batch 的静态 initializer 则在业务任务和业务资源之后清理，遵守各自启动栈的反序。所有任务共享
`application.shutdown_timeout_ms`，单项失败、超时或 panic 只增加停机报告并继续后续项；未取得执行机会的
任务标为 `Abandoned`。任务不能承担长期循环、阻塞 I/O 或内置组件底层对象的关闭责任。

任务可以在执行时通过 Application 查找尚未清理的业务资源；initializer/component 的局部清理不提前
关闭其它 owner 的查找。取得资源后应在任务返回前归还借用，避免后续关闭等待自身持有的资源。
任务从登记到执行始终具有一次性析构隔离。直接取消 Runner 会同步关闭登记，将 Starting/Ready 置为
Stopping，沿实际激活栈逆序释放剩余 action、停机任务和所属资源。经过任务门时，若受监督 future
尚未析构，则立即撤销新借用和全局入口，并将剩余清理所有权保留到最后一个 future 析构；
不等待任务退出，也不创建额外异步清理任务。没有存活任务时在栈清理后撤销入口。
Service 的 initializer 先于业务停机任务释放，Batch 的静态 initializer 晚于业务资源释放；
两种模式均保持停机任务先于业务资源。已有 Stopped/Failed 保持不变。
保留 Application 不再允许新资源借用，也不继续占用全局槽；已借出的资源随借用归还释放，
此前复制的外部客户端句柄不在同步撤销范围内。Stopping 表示异步收尾结果未获确认，直接取消不保证
执行异步收尾或产生退出报告；此时的析构异常同步告警，不追改摘要。
延迟释放期间的析构不能发起新资源借用；任务不让出执行权时，abort 与依赖释放都会延后。
`panic=abort`、同步阻塞和析构自身展开期间的未隔离再次 panic 不属于可隔离范围。

完整 API 签名、生命周期位置、名称边界和资源所有权约束见
[napp 的业务优雅停机任务章节](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#业务优雅停机任务)。

## 稳定基础设施合同

这四项能力共享“资源有硬上限、错误不泄露敏感上下文、所有权必须唯一、观测事实可并入统一指标目录”
的边界，但运行架构不同：

| 能力 | feature 与入口 | 生命周期 owner | 核心安全合同 |
| --- | --- | --- | --- |
| Schema Registry | `kafka-schema-registry` → `nasa::kafka`；配合 `application` 用 `app.schema_registry(name).await` | 独立 client 由业务持有；`schema_registries.<name>` 由 Application 托管，无需 Kafka 消费组件 | schema ID 白名单、有界正负缓存、默认禁止注册、凭据由 `SecretBytes` 承载 |
| 对象存储 | `object-store` → `nasa::object`；配合 `application` 用 `app.object_store(name).await` | 独立 adapter 由业务持有；`object_stores.<name>` 由 Application 托管，无需新增组件字符串 | 有界单对象、`CreateOnly` 条件写、默认 SHA-256 metadata 复核、SigV4 credential 脱敏 |
| gRPC listener | `grpc` → `nasa::grpc` | 独立 `GrpcServerHandle` 或 Application `"grpc"` 二选一 | 统一 codegen/service registry、permit 先于 accept、TLS/mTLS、固定方法指标与有预算排空 |
| Web listener | `application,web` → `#[nasa::application("web")]` | Application 独占明文 listener；不提供脱离 Application 的 listener 模式 | HTTP/1/h2c 确定选择、连接与 stream 上界、固定协议指标与有预算排空；不终止 TLS |

独立模式显式构造 `ConfluentSchemaRegistry` 或 `S3ObjectStore`，由业务管理实例。Application 模式
在上述命名计划中显式设置 `enabled: true`，Prepare 按同代配置和 `secret://` 材料建立客户端、聚合指标
及关闭门禁。Service 在后续 initializer 或 Ready 后任务中取得资源，Batch 在工作负载前完成装配；
未声明或禁用的计划不建立客户端、不读取其独占凭据。对象存储按显式策略监督健康；Registry 构造不访问
远端、不注册 schema，也不证明远端 readiness。参数或凭据变化报告 `RestartRequired`，停机关闭新调用、
等待在途调用，旧句柄返回 `Closed`。

独立实例接入 Application 的 Prometheus/OTLP 出口时，可以在 UserHook 登记各自的 `metrics_source`；
同一 family 只能登记一个 owner，多实例使用 `metrics_source_many`。标准受管计划已自动登记聚合源，
不要重复手工登记。gRPC 只有在同时启用 `application` 并声明
`"grpc"` 时才由容器托管，独立模式仍由业务显式 shutdown。Web 只有在同时启用 `application,web`
并声明 `"web"` 时才读取 `server` 配置和创建 listener；单独启用 `web` 只提供路由与安全门面。
受管 Web/gRPC 可以在 Ready 装配阶段预绑定，但必须等全部任务工厂、最终检查和启动预算通过，
并发布 Application Ready 后统一接流。gRPC 的 `Bound` 观察状态不是 health Serving；发现注册确认
之前动态 readiness 仍不可用。独立 gRPC 可用 `ServerPlan::bind` 与一次性激活权接入自己的启动屏障。

完整合同见 [nafka Schema Registry](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md#schema-registry)、
[naobject](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naobject/README.md) 和
[nagrpc](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nagrpc/README.md)，以及
[napp Web listener](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#web-http-listener-受管模式)。

### gRPC 完整接入

`#[nasa::application("grpc")]` 的目标是让业务只保留不可推断的协议与业务语义。自持 proto 的项目只需：

```toml
[dependencies]
nasa = { version = "2.0.1", features = ["application", "grpc"] }

[build-dependencies]
nagrpc-build = "2.0.0"
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

## 受管配置与资源取得

Service 的 UserHook 登记计划，Prepare 装配标准命名资源，initializer 与 Ready 后业务通过
`app.idempotency_store_named(name)`、`app.audit_sink(name)`、`app.rest_client(name)`、
`app.object_store(name)`、`app.schema_registry(name)`、`app.http_client(name)` 取得句柄。
Batch 在工作负载前完成装配，不要求启动 Web。feature 名称以本 README 的总表为准；例如 MySQL
审计使用 `audit`，普通 REST 使用 `rest-discovery`，不使用底层 `napp` 的 feature 名称。

命名计划显式启用，来源必须精确匹配；仅由禁用计划引用的凭据不解析、不建立文件观察。
本地文件与 Nacos 共用配置候选流程，材料准备失败保留旧视图。TLS HTTP 和受管日志可应用已支持
参数；对象存储、Schema Registry 与连接来源变化保留现有资源并报告 `RestartRequired`。
配置可见与资源实际生效分别记录，单次业务操作应固定一次 `app.config_view()`。

停机由所属 owner 撤销准入并等待在途工作，旧受管句柄不能重新开放。诊断快照不发起后端探测；
构造成功也不等于远端健康。各能力的健康策略、容量、事务与取消边界仍按组件合同执行。

## Feature 总表

默认 feature 为空。只开启业务实际使用的能力：

| feature | 业务入口 | 说明 |
| --- | --- | --- |
| `application` | `nasa::application`、`nasa::Application` | 生命周期、配置快照、资源和受管任务 |
| `tx` | `nasa::tx` | ambient MySQL 事务和 `#[transactional]` |
| `tx-pgsql` | `nasa::tx::pgsql` | PostgreSQL ambient 事务、`PgConn` 和专用 `#[transactional]`；与 `application` 组合时启用受管 PgPool |
| `mapper` | `nasa::mapper` | 声明式 SQL Mapper，蕴含 `tx` |
| `mapper-pgsql` | `nasa::mapper::pgsql` | PostgreSQL Mapper，蕴含 `tx-pgsql`；可复用受管或 standalone PgPool |
| `mapper-redis-cache` | `nasa::mapper` | Mapper Redis Hash L2 |
| `mapper-cache-grouped` | `nasa::mapper` | Mapper 接 `GroupedCache` |
| `mapper-redis-cache-pgsql` | `nasa::mapper::pgsql` | PostgreSQL Mapper Redis Hash L2；不引入 MySQL runtime |
| `mapper-cache-grouped-pgsql` | `nasa::mapper::pgsql` | PostgreSQL Mapper 接 `GroupedCache` |
| `inbox` | `nasa::inbox` | 与业务 MySQL 副作用同事务的消费去重 |
| `inbox-pgsql` | `nasa::inbox::pgsql` | 与业务 PostgreSQL 副作用同事务的消费去重 |
| `outbox` | `nasa::outbox` | 与业务写同事务的事件落库和顺序投递 |
| `outbox-pgsql` | `nasa::outbox::pgsql`、`nasa::application` | PostgreSQL 同事务事件落库与受管 dispatcher |
| `idempotency` | `nasa::idempotency` | Provider-neutral 幂等状态机和进程内 store |
| `idempotency-mysql` / `idempotency-redis` | `nasa::idempotency` | MySQL 强一致或 Redis response-cache 后端 |
| `idempotency-pgsql` | `nasa::idempotency::pgsql` | PostgreSQL 租约 fencing 与持久响应重放 |
| `audit` | `nasa::audit` | 与业务写同事务的 Outbox 审计 |
| `audit-pgsql` | `nasa::audit::pgsql` | 与 PostgreSQL 业务写同事务的 Outbox 审计 |
| `openapi` | `nasa::openapi` | 静态 mapping 与显式动态路由的确定性 OpenAPI 3.1 合同；path 包含 Application `context_path` |
| `redis` | `nasa::redis` | Redis 命令、pipeline、stream、lock；分区消费支持 source/group/stream 执行隔离，跨域业务键顺序覆盖 handler、ACK 和精确重试，停机返回可继续等待的排干报告 |
| `redis-job` | `nasa::redis::job`、`nasa::redis_job` | 多 source RedisJob 状态机、`#[redis_job]` 与受管生命周期；蕴含 `application` 和 `redis` |
| `rate-limit` | `nasa::application` | 基于共享 Redis 原子计数的跨副本业务配额；蕴含 `application` 与 `redis`，无组件字符串，后端故障默认 fail-open，进入 `full` |
| `redis-search` / `redis-derive` | `nasa::redis` | 搜索封装和文档派生 |
| `cache` | `nasa::cache` | 两级缓存、失效广播和缓存宏 |
| `kafka` | `nasa::kafka` | 发布、消费、路由、确认和健康 |
| `kafka-tls` / `kafka-gssapi` / `kafka-zstd` | `nasa::kafka` | Kafka 传输安全与压缩子能力 |
| `kafka-schema-registry` | `nasa::kafka`、`nasa::secret` | 有界 schema adapter；蕴含 `kafka` 与 `secret`，配合 `application` 纳管 `schema_registries`，进入 `full` |
| `saga` | `nasa::saga` | 无 I/O 的 definition、身份和补偿合同 |
| `saga-runtime` | `nasa::saga`、`nasa::application` | Orchestrator、参与方 adapter 与 Application Saga 组件 |
| `saga-runtime-pgsql` | `nasa::saga::pgsql`、`nasa::application` | PostgreSQL Store/Inbox/Outbox 组合与共享 Saga 状态机 |
| `saga-kafka` | `nasa::saga` | 受管 command/result Kafka transport |
| `saga-kafka-pgsql` | `nasa::saga::pgsql` | PostgreSQL Saga command/result Kafka transport |
| `saga-redis-stream` | `nasa::saga`、`nasa::application` | 受管 Redis Streams 发布、消费、重领、原子 DLT 与积压观测 |
| `saga-redis-stream-pgsql` | `nasa::saga::pgsql`、`nasa::application` | PostgreSQL Saga 的同一 Redis Streams 传输合同 |
| `saga-grpc` | `nasa::saga`、`nasa::grpc`、`nasa::application` | 已包含 `grpc` 类型门面；generated command/result service、mTLS principal 绑定与封闭收据，入站计划复用 `"grpc"` listener，纯出站不启动 listener |
| `saga-grpc-pgsql` | `nasa::saga::pgsql`、`nasa::grpc`、`nasa::application` | PostgreSQL Saga generated service 与同一受管 gRPC listener |
| `hystrix` | `nasa::hystrix` | 并发隔离、超时和 Dashboard 流；与 `application` 组合可纳管固定规则、命名目录与属性命令 |
| `grafana` | `nasa::grafana` | 接口隔离、Prometheus 指标和面板 |
| `telemetry` | `nasa::application` | 受管 span 队列、OTLP/HTTP 导出和停机 flush |
| `web` | `nasa::web` | 路由宏与 interceptor；和 `application` 组合并声明 `"web"` 时提供受管 HTTP/1/h2c listener |
| `web-auth` | `nasa::web::auth` | 路由身份合同 |
| `web-crypto` | `nasa::web::crypto` | 双协议密码处理和重放保护 |
| `web-crypto-legacy-rsa` | `nasa::web::crypto` | 受控迁移的 legacy RSA 私钥路径，不进入 `full` |
| `web-security` | `nasa::web` | 身份、解密、重放、handler、加密固定流水线 |
| `oauth` | `nasa::oauth` | JWT、JWKS 与授权服务器 metadata |
| `secret` | `nasa::secret` | secret 分片、快照和两阶段轮换 |
| `secret-http` / `secret-vault` | `nasa::secret` | TLS client 和 KV v2 provider |
| `object-store` | `nasa::object`、`nasa::secret` | 有界对象存储合同；蕴含 `secret`，配合 `application` 纳管 `object_stores`，进入 `full` |
| `grpc` | `nasa::grpc`、`nasa::application` | 统一 codegen、独立或 `"grpc"` Application 受管 listener、TLS/mTLS、方法策略与观测，进入 `full` |
| `scheduling` | `nasa::scheduling` | 异步与定时任务 |
| `scheduling-cluster` | `nasa::scheduling` | Redis leader gate 和集群调度 |
| `partition` | `nasa::partition`；与 `application` 组合时含 `PartitionApplicationPlan`、`app.partition()`、`app.partition_runner(name)`；与 `scheduling` 组合时连带开启 `#[Async(runner = .., spec = ..)]` 分区执行域形态 | 直接 Registry 支持运行期动态 Runner 并由业务显式停机；Application 模式冻结启动期计划，提供命名隔离、严格 FIFO 保序任务窃取、逐域健康与统一停机 |
| `ws` | `nasa::ws` | TCP/WebSocket 长连接 |
| `ws-client` | `nasa::ws`；与 `application` 组合时含命名发送句柄 | 原生 TCP 帧 Client，按 `ws_clients` 管理首连、启动许可、重连健康和退出；不启动入站 listener |
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
| `base` | `nasa::base`、`nasa::date` | `nabase` 的完整能力：响应、日期时间、容量、ID、字符串、环境变量和翻译抽象 |
| `crypto` / `numeric` / `image` | 对应同名模块 | 密码、精确数值和图片工具 |
| `crypto-legacy-rsa` | `nasa::crypto` | 受控迁移的 RSA 私钥兼容入口，不进入 `full` |
| `full` | 上述稳定能力的组合 | 非默认；包含 Schema Registry、对象存储、Saga gRPC、gRPC listener 和受管 Web listener |
| `full-pgsql` | PostgreSQL 完整持久能力组合 | 非默认；包含 PostgreSQL Mapper Redis L2，且不引入 MySQL runtime |

`kafka-gssapi` 使用目标系统的 Cyrus SASL。macOS 无需额外安装；Linux 构建环境需提供
`libsasl2-dev` 或 `cyrus-sasl-devel`，具体包名由发行版决定。

`nacos` 和 `rest-discovery-nacos` 只保证 API 可编译；真正连接后端必须同时启用 `nacos-sdk`。
`full` 选择 Kafka 和 gRPC 作为纳入的 Saga transport，同时纳入 gRPC listener；Redis Streams 替代
通道仍需显式开启。生产服务仍建议只选择实际使用的 feature，避免无意扩大依赖面与运行责任。

## PostgreSQL 受管与 standalone 入口

`tx-pgsql` 与 `mapper-pgsql` 不改变现有 MySQL `tx` / `mapper` 行为。不启用 `application` 时，业务先显式
注册默认或命名 `PgPool`，再从后端模块声明 Mapper：

```rust
use nasa::mapper::pgsql::{Mapper, Query};

#[derive(sqlx::FromRow)]
struct AccountRow {
    id: i64,
}

#[Mapper(datasource = "reporting", cache = false)]
trait AccountMapper {
    #[Query("SELECT id FROM account WHERE id = #{id}", tx = "mandatory")]
    async fn find(&self, id: i64) -> anyhow::Result<Option<AccountRow>>;
}

nasa::tx::pgsql::try_init_datasource("reporting", pool)?;
let mapper = AccountMapperClient::new();
let row = nasa::tx::pgsql::run_for("reporting", async { mapper.find(7).await }).await?;
# let _ = row;
```

PostgreSQL Mapper 生成 `$n` bind，并保持 SQL 文本中的 `?` 原样；不会翻译 MySQL 专有 SQL。与
`application` 组合时，`tx-pgsql` 会向下启用 `napp/db-pgsql`，同一个 `"db"` 组件从 YAML 创建、探测、
发布并关闭 PgPool：

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

业务用 `app.datasource("orders")` 与 `app.pg_datasource("reporting")` 取得各自 typed pool。反向查询会返回
driver mismatch；显式配置 PostgreSQL `schema` 时，它同时设置业务连接的 `search_path` 与 migration
对象作用域，且目标 schema 必须预先存在。省略时业务连接保留服务端默认 `search_path`，受管 migration
默认使用 `public`；需要两者严格一致时应显式配置。两种事务不能嵌套，也不提供跨库原子提交。`full-pgsql` 是 PostgreSQL-only 的稳定能力
集合，包含受管 Application、事务、Mapper、Inbox、Outbox、幂等、审计、Saga 及四种 Saga transport；
其依赖图不包含 MySQL runtime。需要混配时同时开启实际使用的 MySQL 与 PostgreSQL feature。

`migrations` 配置只决定 `disabled`、`validate`、`apply`、锁等待和 PostgreSQL session topology，不会
从目录自动发现业务 SQL。Service 可以在 UserHook 中使用
`app.configure_migrations("reporting", sqlx::migrate!("./migrations"))?` 登记构建期嵌入的 migration；
同一数据源只能登记一次，门禁在 initializer 与入站监听之前执行。业务需直接依赖启用对应 driver 和
`migrate` 的 `sqlx`，例如 PostgreSQL 使用
`sqlx = { version = "0.9", default-features = false, features = ["macros", "migrate", "postgres"] }`。门禁模式不是
`disabled` 时，PostgreSQL 事务级代理还必须提供指向同一 database/schema 的
`migrations.session_url`，Application 会在 advisory lock 前复验目标身份。Service 与 Batch 都可通过
`nasa::application::MIGRATION_PLANS` 静态工厂登记迁移；Batch 在 initializer 与工作负载之前执行，
不接受工作负载 Hook 的动态登记。静态工厂的登记方式见
[业务 migration 登记](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#业务-migration-登记)。

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
  role: orchestrator
  plan_mode: managed
  database_bootstrap: application
  service_identity: checkout-orchestrator
  replica_identity: checkout-orchestrator-1
  orchestrator:
    datasource_ref: default
    timer_poll_interval_ms: 500
    timer_error_backoff_ms: 1000
    timer_operation_timeout_ms: 5000
    timer_failure_threshold: 3
  definition_catalog:
    mode: dynamic
    datasource_ref: default
    activation_policy: validated
    watch_interval_ms: 500
    capability_registry_ref: saga-participant-capabilities
    publisher_authorization_policy_ref: saga-definition-publishers
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
  http2:
    enabled: false
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
| `schema_registries.<name>` | Application Prepare 装配的命名 Registry client；需 `application,kafka-schema-registry`，无需 Kafka 消费组件 |
| `object_stores.<name>` | Application Prepare 装配的命名对象存储；需 `application,object-store`，无需新增组件字符串 |
| `grpc` | Application gRPC listener、TLS、方法策略与协议能力；独立模式不读取此根 |
| `auth` | OAuth/JWKS 认证组件 |
| `server` | Web 组件 |
| `ws` | `naws` |
| `rest_discovery` | 注册发现组件 |
| `scheduling` | `nasched` |

`schema_registries` 与 `object_stores` 是 Application 受管配置根，每个命名计划以 `enabled: true`
启用；仅编入 feature 不会创建客户端。独立 `ConfluentRegistryOptions` 与 `S3Options` 仍由调用方显式
传入，不读取 Application 配置。两种入口的完整配置和边界见
[受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。

## 主要边界

- `nasa` 只做模块组织与重导出，不承载业务状态，也不提供全量 prelude。
- feature 是编译期能力；组件字符串是运行期生命周期 owner，两者不能混用。
- `full` 不应设为默认；生产服务应选择实际使用的能力，避免扩大编译与安全边界。
- 宏会识别门面被 Cargo 重命名的情况；业务无需直接依赖宏实现 crate。
- 具体失败语义、配置默认值和资源上限以各组件 README 为准。

## Stream、出站帧客户端与隔离命令

| 使用范围 | 门面 feature | Application 声明与配置 |
| --- | --- | --- |
| 普通 Stream / Proxy / AutoPipeline | `application,redis` | 声明 `"redis"`；`redis_streams`、`redis_proxies`、`redis_pipelines` |
| 纯出站原生 TCP 帧 Client | `application,ws-client` | 无需组件字符串；`ws_clients` |
| 隔离规则、显式命令与属性命令目录 | `application,hystrix` | 无需组件字符串；`hystrix.enabled: true` |

Stream 与 Proxy 在 Service UserHook 登记 handler；它们的消费、AutoPipeline 与纯出站 Client
的准入与宿主终端共用一次启动许可。Ready 前在本地状态保护内复验关键 owner、认证连接、健康
证据和启动期限，保护持续到许可发布完成。公开 Ready 表示入口已获许可，仍可能遇到远端故障。
AutoPipeline 与纯出站 Client
也支持 Batch。`ws-client` 不启动 WebSocket listener，不支持 ws/wss URL。hystrix 保留独立使用方式，
显式受管时只安装一个全局 owner，命令缓存随应用实例切换，业务收尾结束后才撤销。
详细字段、容量和失败语义见 [napp 配置](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#redis-streamproxyautopipeline)。
