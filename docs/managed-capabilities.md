# Application 受管能力与资源边界

业务通过 `nasa` 门面选择 feature，通过组件声明、命名配置和启动期计划把资源交给 `napp`。
框架负责依赖绑定、启动门禁、资源登记、健康与退出等待；业务提供 handler、领域策略和显式配置。
默认 feature 为空，启用 feature 不等于建立连接。以下命名配置只有 `enabled: true` 才读取所需凭据、
装配资源；显式选择却缺 feature、未知 provider 或错误来源会拒绝启动。

## 装配与取得入口

表中的 feature 均指 `nasa` feature，并须同时开启 `application`。

| 能力 | feature 与组件 | 配置或计划 | 业务取得入口 |
| --- | --- | --- | --- |
| RedisPartition | `redis`；`"redis"` | `redis.partition` 或每源对应段，`configure_redis_partition` 登记 handler | `redis_partition(source)` |
| 独立 Redis 竞选 | `redis`；`"redis"` | `redis_leaders.<name>` | `redis_leader(name)` |
| Redis Pub/Sub | `redis`；`"redis"` | `redis_subscriptions.<name>`，`configure_redis_subscription` | 启动期登记 handler |
| 普通 Stream / Proxy / AutoPipeline | `redis`；`"redis"` | `redis_streams` / `redis_proxies` / `redis_pipelines` | `configure_redis_stream` / `configure_redis_proxy` 登记；`redis_proxy(name)` / `redis_pipeline(name)` 取得发送句柄 |
| 原生 TCP 帧出站 | `ws-client`；无需入站组件 | `ws_clients.<name>` | `configure_ws_client_event` / `ws_client(name)` |
| 隔离命令目录 | `hystrix`；无需组件字符串 | `hystrix.enabled` / `isolation` / `commands` | 属性命令、受管 Web dispatch、`hystrix_command(name)` |
| Snowflake | `redis`；`"redis"` | `redis.snowflake.<name>` | `snowflake(name)` |
| Mapper L2 | `mapper-redis-cache` 或 `mapper-redis-cache-pgsql`；`"redis"` | `mapper_cache.enabled`、`redis_ref` | Mapper 查询复用默认 L2 |
| Mapper codec/metrics | `mapper` 或 `mapper-pgsql` | `configure_mapper_defaults` | Mapper 调用路径 |
| 分组缓存 | `cache,redis`；`"redis"` | `grouped_caches.<name>` | `grouped_cache(name)` |
| L1/两级缓存 | `cache`；`"cache"` | `cache.mode` 与后端配置 | `nasa::cache` |
| 持久失效意图策略 | `cache`；`"cache"` | `configure_cache_invalidation_sink` | `record_invalidation` |
| 幂等 store | `idempotency-mysql`、`idempotency-pgsql` 或 `idempotency-redis`；对应 `"db"`/`"redis"` | `idempotency_stores.<name>` | `idempotency_store_named(name)` |
| 事务审计 | `audit` 或 `audit-pgsql`；`"db"` | `audit_sinks.<name>` | `audit_sink(name)` |
| 普通 REST 出站 | `rest-discovery`；无需发现组件 | `rest_clients.<name>` | `rest_client(name)` |
| Nacos REST 发现 | `nacos-discovery`；`"nacos-discovery"` | `rest_discovery` | `nacos_discovery()` 的 client |
| 对象存储 | `object-store`；无需额外组件 | `object_stores.<name>` | `object_store(name)` |
| Schema Registry | `kafka-schema-registry`；无需声明 Kafka 消费组件 | `schema_registries.<name>` | `schema_registry(name)` |
| 外部 secret | `secret-vault` | `secret_providers` 与 provider fragment | 同代 `ConfigView::secrets()` |
| TLS HTTP | `secret-http` | `http_clients.<name>` | `http_client(name)` |
| 本地 watch | `yml-watch` | `config_watch.enabled: true` | 复用配置视图订阅与 reload 状态 |
| WS Redis/Kafka 集群 | `ws-redis` / `ws-kafka`；来源组件与 `"ws"` | `configure_ws_redis` / `configure_ws_kafka` | `ws()` |
| Web 认证/加密 | `web-auth` / `web-crypto`；`"web"` | 既有路由安全计划 | 受管 Web 路由 |
| 集群调度 | `scheduling-cluster`；`"redis","scheduling"` | 既有调度计划 | `scheduling()` |

命名集合通常最多 64 个；secret provider 最多 16 个，secret 最多 256 个。名称在启动时冻结，
不支持热应用的资源参数或凭据变化会保留实际资源并报告 `RestartRequired`。
无 Web 的 Service 和 Batch 可以使用持久 store、审计、REST、对象和 TLS HTTP client。
Service 的 UserHook 只登记计划；标准资源在 Prepare 装配，供 initializer、Ready 后业务取得。
Batch 先装配所选资源，再进入工作负载。
普通 Stream、Proxy、RedisPartition、独立 Leader/Subscription、WS 入站消费要求 Service；Batch 在工作负载前拒绝这些计划。

## 配置准备与原子发布

本地文件 watch 与 Nacos 使用同一个候选发布流程：合并和结构校验 → 解析全部材料 → 准备 Saga/TLS
安全资源与日志资源 → 进入既有 `publication_gate` 复验版本 → 同步安装日志 → 按实际结果构造状态表
并发布 `ConfigView`。锁内不执行外部 I/O、日志线程 join、目录清理或可失败的材料准备。
日志旧资源由有界回收器在锁外处理；目录创建失败、secret 无法解析或证书无效不会提前改变运行日志。

同步安装前取消会丢弃候选。一旦开始安装，必须完成同次视图与状态发布，中途不响应取消。
组件状态记录期望配置和最后成功版本；`RestartRequired`、`ApplyFailed` 不会被无关更新清除。
恢复到实际生效配置可以解除重启要求。相同 fingerprint 且材料未变的候选不会隐含触发重试。
业务操作应固定一次 `app.config_view()`，从中同时读取配置和凭据，避免两次独立加载跨代。

Saga 结果事务把安全快照 generation 与合同摘要一起冻结。成功发布任何新安全快照都会使旧请求资格
失效，即使材料随后恢复为相同字节；watcher 只能基于新代重新确认后续请求。失败或无变化候选不发布，
不会单独撤销当前资格。该保守边界可能让与 Saga 无关的成功配置发布短暂关闭结果准入，但不会放宽
新 Start、timer、管理操作或 Ready。

```yaml
config_watch:
  enabled: true
secret_providers:
  vault:
    enabled: true
    kind: vault_kv2
    endpoint: https://vault.example
    mount: services
    token: { env: VAULT_BOOTSTRAP_TOKEN }
secrets:
  api_key:
    encoding: raw
    max_bytes: 4096
    fragments:
      - provider: { provider: vault, key: payments/client#key }
```

provider 还支持 `openbao_kv2`。bootstrap token 只能来自独立 env/file，不从该 provider 自身获取。
远端材料在首个消费者之前异步准备，总预算 15 秒；单 provider 默认 3 秒，可配置至 10 秒，响应上限最大 1 MiB。
本地监听有 owner，最多监听 128 个配置/secret/引导材料路径，采用合并事件与周期补读。
候选监听集准备成功后才切换当前配置，切换完成后撤销旧监听集。Batch 拒绝启用持续文件监听。
材料解析与文件观察使用同一活跃消费者集合：禁用计划的独占密钥及无消费者 provider 的引导文件
不读取、不监听；共享 ID 仍有活跃引用时继续保留。观察范围不能因无效候选而替换成不可用的新集合。

`http_clients` 只接受 HTTPS，配置 `base_url`、可选 `certificate/private_key/trust` secret 引用、
`request_timeout_ms` 和 `max_body_bytes`。证书与私钥必须成对；显式 trust bundle 替代内置信任根。
请求返回完整有界正文及实际使用的 `ConfigView`，不允许跨 origin 跳转。名称集合不能热改变，
已有名称的 TLS 材料与参数随配置同点发布，初始装配及后续发布均在
`ReloadTarget::Managed("http_clients")` 记录 `Applied` 和实际视图版本。材料准备失败或名称集合变化
拒绝整个候选，旧资源与最后成功版本保持不变。调用取消只表示本地停止等待。

## Redis 来源、派生任务与停机

```rust
#[nasa::application("redis")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_redis_partition("primary", true, |prepared| {
        prepared.try_register_legacy::<String, _, _>("orders", "created", |events| async move {
            consume(events).await
        })?;
        Ok(())
    })?;
    Ok(())
}
```

每个来源有独立 napart 执行域；`source/group/stream` 只决定同源域划分。框架 Prepare 冻结路由，
统一 Ready 后才放行消费。一个聚合 owner 先关闭全部来源准入，再并发使用同一截止点排干。
`redis_partition_observations()` 与 `redis_partition_stop_results()` 提供逐来源事实；超时保持未收口责任，
在退出证明到达前保留依赖，不自动转为有损停止。通用 `"partition"` Runner 不参与此消费链。

独立 Leader 计划指定 `redis_ref`、`key`、可选 `period_ms`；`is_leader/run` 受 Ready 和关闭门禁约束。
租约仍由 Redis 裁决，任务必须响应取消并自行实现外部 fencing。订阅计划指定 `redis_ref`、`channels`、
`max_payload_bytes` 与 `critical`，逐条 handler 在 Ready 后执行，错误、超大消息或 panic 反映为任务失败。
重连不补回 Pub/Sub 缺口。调度、RedisJob 和 partition 已拥有的竞选/订阅复用原 owner。

Snowflake 标准计划只领取已显式初始化的非复用 namespace。`config` 使用 `SnowflakeConfig`，
`namespace` 指定 `incarnation`、`first_worker_id`、`last_worker_id`，`redis_ref` 选择来源。
历史缺失、布局不符或耗尽均拒绝；归还不回收编号，停机后的旧生成器拒绝发号。
管理方必须通过 Redis 账本之外的记录证明首次初始化权威，并保证已确认领取记录不会回退。
incarnation 不进入 ID 编码，更换名称不能证明新旧 ID 空间隔离。
`alloc_worker_id/build_with_redis` 始终拒绝自动初始化。`build_local` 固定 workerId=1，要求同一 ID 空间
只有一个生成器状态 owner；重启须越过已确认的逻辑时间上界。

## 缓存与事务

```yaml
mapper_cache:
  enabled: true
  redis_ref: primary
grouped_caches:
  prices:
    enabled: true
    redis_ref: primary
    backstop_ttl_secs: 300
```

Mapper L2 复用受管 Redis，在放行查询前验证字段过期能力；默认槽具有可撤销 owner，旧句柄关闭后
不能继续查询或回填。Service 与 Batch 均执行安装门禁。自定义默认 codec/metrics 通过
`configure_mapper_defaults` 纳入启动回滚和停机，不能与另一 owner 重复安装。
L1 刷新由有界任务 owner 持有，最多 128 个后台工作；停机撤下本代场景并等待真实退出。

`GroupedCache` 字段过期依赖 Redis 7.4+。正 TTL 的写值和设置字段过期在同一 Lua 中执行，过期失败
会删除本次写入并记录写入失败，读取仍返回回源结果；TTL=0 明确表示不设置过期。业务 TTL 最大 365 天，抖动另有固定上界。
single-flight 只减少重复回源；失败、取消与等待者都共享同一锁项，不能仅因持有者退出就创建第二把锁。

即时 `invalidate/apply_invalidation` 只删除 L2/L1 并发出尽力广播；`record_invalidation` 只追加持久意图，
错误必须传回业务事务。事务策略在 `configure_cache_invalidation_sink` 登记，dispatcher 提交后执行
`apply_invalidation`，不会重复追加同一意图。Redis 不可用不会阻止纯意图入口写入事务 Outbox。

`#[transactional]` 与 `#[cache_invalidate]` 的属性顺序决定包装层级；有外层事务时，内层函数返回仍不表示
外层已提交。`cache_invalidate` 在业务返回 Err 时也会尝试即时失效，失效错误不改变原业务结果，
因此不能承担事务持久意图的强制成功条件。要求可靠失效的业务显式追加意图并传播错误。
`after_commit` 是进程内尽力回调：仅确认提交后执行，回调失败不改写已提交业务结果，也不提供崩溃补偿。

cache-aside 不保证强一致；失效后，已经读取旧值的 loader 仍可能晚到回填，且重新起算 TTL。
Pub/Sub 离线节点可能漏收。框架不把这些边界描述为一个 TTL 内必定新鲜。

## 持久适配器与迁移

```yaml
idempotency_stores:
  orders:
    enabled: true
    driver: mysql
    source: primary
    web_default: true
audit_sinks:
  changes:
    enabled: true
    driver: mysql
    source: primary
```

store 的 driver 可选 `mysql/pgsql/redis`；datasource 自身的 PostgreSQL driver 仍是 `postgresql`。
store 和审计 sink 固定绑定来源，装配前只读验证必要 schema，不在请求或 Ready 中隐式建表。
Service 可以在 UserHook 登记迁移；Batch 必须用 `MIGRATION_PLANS` 静态工厂，在工作负载之前执行。
工厂仅返回来源与嵌入的 `Migrator`，不执行 I/O；重复来源或超过 128 项的计划在执行迁移前拒绝。
来源必须精确匹配已配置的数据源；遇到未知来源会阻止启动，此前其它来源已经完成的迁移不会自动撤销。

`web_default` 只在声明 Web 时可用，与手工 `set_idempotency_store` 冲突时拒绝重复安装。
HTTP 取消、不可缓存状态或响应传输失败不证明业务已回滚，不自动 `abort` 执行占位。
Redis store 仍是带 TTL 的响应缓存，TTL 与恢复策略不能代替业务最终幂等事实。
审计只在同源 ambient 事务中追加 Outbox，缺事务或错源会拒绝；业务决定记录时机，不另起提交或 dispatcher。

## 出站客户端、健康与诊断

`rest_clients` 支持 `external/static/dns/custom`。external 仅接受显式 URL，不连接注册中心；static 配置
`services.<service>` 的 host/port 列表，dns 每服务一个域名/端口，custom 借用启动期已登记的
`Arc<dyn DiscoveryClient>`。借用 provider 的独立资源仍须由其资源 owner 负责，REST 只管理自己的任务。
每个 client 默认最多 1024 个服务 watch，上限 65536；关闭准入后即使旧调用返回也不能登记新 watch。
全局默认仅可有一个 owner，关闭等待实际任务退出，旧句柄永不重新开放。原始 response 的正文由调用方消费；
`send_text/send_bytes/send_json` 保持完整正文的预算边界。

`object_stores` 当前 provider 为 `s3`，必填 endpoint、bucket、region 与 access_key/secret_key 的 secret
引用，可选 session_token。标准入口最大正文 256 MiB，请求超时最大 300 秒，不自动重放写操作。
`health_probe: head_bucket` 在启动时探测，随后由宿主监督器每 15 秒并发发起只读 HEAD，单次最多 5 秒，
60 秒没有新证据时过期；HEAD 不能证明所有对象操作权限。传输、远端拒绝和完整性失败使 `critical: true`
资源 `NotReady`，非关键资源 `Degraded`；成功或确定性对象裁决可恢复健康。非关键 `on_request` 只记录
最近实际调用，不创建周期探测、不因空闲过期，也不承诺持续可达。仅完成构造不等于远端健康。
停机取消并等待探测退出，关闭后旧句柄返回 `Closed`，在途业务调用归还后才释放 owner。

`schema_registries` 提供 endpoint、Bearer 或 Basic secret 引用、有界缓存/正文及显式 `auto_register`。
构造不探测 Kafka、不注册 schema；调用与指标由命名 owner 持有，关闭后禁止新调用。
Registry 指标只反映实际调用结果，不把构造成功当成远端 readiness。

WS 集群计划绑定受管 Redis/Kafka 来源和调用方证明持久单调的 `Incarnation`。
Kafka 来源必须关闭 collected consumers，不能让 WS 与普通消费者争用同一个 consumer owner。
监听可预绑定但 Ready 前不处理连接；关闭先收回准入，等待 listener 与集群任务退出后才释放来源。

`diagnostic_snapshot(limit)` 只组合已有 readiness、配置应用状态、telemetry、RedisPartition 与 SQL 策略
快照，不执行后端探测；limit 范围 1..=256，各部分包含采样边界与截断事实。
它不是跨组件原子快照，也不默认暴露 HTTP 管理端点；公开内容不包含 URL 凭据、secret 明文或业务载荷。

## 调用链预算

Web 的 `server.request_deadline_ms` 覆盖 handler 与完整响应体，结束、错误和丢弃都会通知取消。
响应头前到期返回 504；响应头后只能中止正文，不能改写已经发送的状态码，数据帧和 trailers 均保留。
DB `conn_for_budget/read_with_budget`、Redis 明确只读 helper、REST 与 TLS HTTP 接收同一个绝对预算。
DB 只读入口要求调用方确认无业务副作用，不按 SQL 前缀推断；事务写、COMMIT、XADD 不自动包装为超时失败。

gRPC `Deadline::request_budget` 保留绝对截止点；出站使用 `propagate_request_budget` 写 `grpc-timeout`，
再用 `call_with_budget` 响应本地取消。入站 Deadline 不含客户端断开令牌，drop 当前调用 future 不证明
服务端未执行，业务自行 spawn 的任务也不会自动加入跨协议取消树。

## 消费、出站与命令运行架构

```text
最终配置 + 启动期计划 → feature / 来源 / 容量校验 → Prepare 建立唯一 owner
Service：initializer 取得句柄 → 关键本地权威与健康复验 → Ready 与共享启动许可
Batch：完成装配与初始化 → 开放 Pipeline、Client 发送 → 工作负载
关闭：撤销新调用 → 等待任务与在途责任 → 释放来源及本代全局引用
```

hystrix 的目录在 Prepare 装配后即可供初始化使用；Service 的业务收尾仍可调用命令，之后才撤销
命令准入。Redis 派生发送与 Client 句柄在 Service 的 Ready 前拒绝业务调用，取得句柄本身不开放入口。

普通 Stream、Proxy 和原生 TCP Client 的业务回调等待统一 Ready；AutoPipeline 与纯出站 Client
支持 Batch。未激活、已关闭或超出容量的调用明确拒绝，已入队不等于远端执行成功。
Service 的领域准入与受管终端共用一次启动许可，公开 Ready 时首次合法调用不再等待健康监控激活。
关键任务责任、Client 认证连接和健康新鲜度在发布前复验，本地状态保护延续到发布完成；已观察到的
失效走启动失败清理。未被观察到的远端故障不在该本地保护内，发布后的故障按运行期策略处理。
Client 断连或重连期间，关键计划贡献 NotReady，可选计划贡献 Degraded，重新认证后恢复 Ready；
关键 owner 意外结束仍触发停机。每秒采样与 5 秒证据过期不构成远端故障的即时通知。
领域 owner 持有实际 join 责任，重复关闭和取消等待不会使后台任务脱离所有权；旧受管句柄不能复活。
Proxy 只在完整 PEL 证据下清理空 consumer，整个清理过程与任务排干共用截止时间。
Proxy 清理证据不可用或预算耗尽进入次要停机失败；本地只读快照保留具体清理分类。
执行器销毁后不能继续沿用 Running 快照；缺少退出证据时明确失败，不推断远端写入或删除未执行。
AutoPipeline 的单批字节限制是参数字节软边界 B＋M，不能解释为进程内存上限。

hystrix 保持独立 API；显式受管时按应用代次重建属性命令缓存，业务收尾完成后才撤销配置与目录。
这些命名计划、隔离规则和出站认证材料均冻结到启动，变化报告 `RestartRequired`。
完整字段见 [napp](../napp/README.md#redis-streamproxyautopipeline)。

| 观测入口 | 可用于判断 | 不能据此推断 |
| --- | --- | --- |
| `redis_derived_observations()` | 本地 consumer/reclaim/flusher 责任、进展、队列参数字节与 Proxy 清理分类 | 远端 PEL 大小、唯一消息数或业务成功数 |
| `ws_client_observations()` | 当前连接事实、最近认证/PONG、固定失败类别与子任务数量 | 已入队消息被远端收到或执行 |
| hystrix 命令指标与 owner 健康 | 并发拒绝、超时、调用结果与周期观测职责 | 被保护的依赖始终可达、错误率熔断状态 |

Redis 派生计划的健康阈值为 1，证据 15 秒过期；Client 每秒采样，阈值为 1，5 秒过期。
两者 `critical` 缺省 false，关键任务意外退出触发宿主停机。hystrix 观察任务的健康阈值为 1，
5 秒过期，意外退出触发停机。健康时效包含协议检测与本地采样延迟，不能作为实时远端存活证明。
