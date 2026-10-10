# nadisc-runtime

任意 Rust 微服务都可使用的多注册中心生命周期组件。一个 owner 管理多个具名连接、显式注册目标、共享发现证据和有界停机；业务只持有查询与调用句柄，无需依赖 napp、Saga 或数据库。

默认包含 Static/DNS 装配；可选 `nacos`、`consul`、`kubernetes` feature 分别启用对应 adapter。也可实现 `nadisc::managed::ManagedProvider` 接入其他后端。

## 架构与安全边界

`OwnerBuilder → prepare → freeze_registrations → publish → begin_quiesce → begin_close` 是同一个资源生命周期。`DiscoveryOwner` 不可 Clone；`DiscoveryHandle`、`Observation` 和 `RuntimeObserver` 不获得关闭权。

- 每个连接有独立 provider、凭据、命名空间、服务绑定、观察与失效状态。明确选择的连接失败后，不会借另一环境的同名服务继续调用。
- 同一连接、同一服务共享观察；一个等待者超时不取消其他等待者的需求。完整空列表立即撤销旧候选。
- `DiscoveryHandle::observe(service, deadline)` 的总期限覆盖缓存命中、锁等待和初次查询。已经过期的调用返回 `DeadlineExceeded`，不刷新观察访问时间、不新增观察槽或派生查询；等待后成功返回前也会复验期限。
- 普通调用可配置异常宽限，轮询和 watch 的网络失败、不完整同步、响应容量拒绝都把期限收紧到最后一次完整成功时刻加上 `min(max_age_ms, stale_if_error_ms)`，重复失败不续期。严格调用立即拒绝失败后的旧证据。
- 身份撤销、权限裁剪、明确空集、已确认非法候选（`InvalidSnapshot`）和关闭没有宽限；完整新集合已撤下旧候选时，即使新代因本地容量限制未能发布，也不能继续发送被撤下的候选。单纯分页、解码或传输不完整使用 `IncompleteSnapshot`，不冒充明确撤权。
- 连接认证或授权撤销同时取消该连接全部注册的健康证据并通知宿主，其他连接保持独立。启动确认、`registrations()` 与 `RuntimeObserver::snapshot()` 都按当前身份代次裁决就绪；撤权前发起的迟到健康回查不能延长证据期限，恢复须取得原身份撤权后的新有效回查。
- `snapshot()` 是诊断与选择输入。真正发送须使用 `submit_if_current` 或 `submit_if_current_strict`，在短临界区内立即提交；闭包不得等待或重入 owner。授权不等同于网络 fencing，已提交请求仍可能到达远端。
- 注册前冻结全部计划、检查能力和重复后端键。注册成功、健康确认、本地资格分别记录。超时写可能仍在远端执行；结果不明后不盲目重建，注销 ACK 也不能证明先前写入以后不会生效。
- 已确认注册的续约响应丢失时，只有声明 `supports_idempotent_renewal` 的 provider 才按有限指数退避重复相同身份、相同健康 payload；每次提交仍复验本地资格。新的续约成功并通过健康回查后恢复 Active。先前请求可能迟到的事实仍保留，因此后续注销即使收到 ACK 也报告 Residual，不能据此重新注册；普通回查不解除此限制。未声明该能力的 provider 和首次注册结果不明继续保守阻断。
- `begin_quiesce` 同步撤销新的注册和健康发布，保留出站发现供业务排空；`begin_close` 关闭出站并回收任务。两者共享首次停机期限，丢弃等待凭据不取消清理。

## 独立宿主

```rust,no_run
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use nadisc::Instance;
use nadisc_runtime::{ConnectionSpec, OwnerBuilder, config::ConnectionPolicy, providers::LocalProvider};
use tokio::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = LocalProvider::static_services(BTreeMap::from([
        ("orders".into(), vec![Instance::new("127.0.0.1", 8080)]),
    ]))?;
    let owner = OwnerBuilder::default()
        .connection(ConnectionSpec {
            name: "business".into(),
            provider: Arc::new(provider),
            policy: ConnectionPolicy::default(),
        })?
        .build()?;
    owner.prepare(Instant::now() + Duration::from_secs(5)).await?;
    owner.freeze_registrations(Vec::new())?;
    owner.publish(Instant::now() + Duration::from_secs(5)).await?;
    let handle = owner.client(Some("business"))?;
    let observation = handle.observe("orders", Instant::now() + Duration::from_secs(2)).await?;
    let snapshot = observation.snapshot(true)?;
    assert_eq!(snapshot.instances().len(), 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    owner.begin_quiesce(deadline).wait().await?;
    let report = owner.begin_close(deadline).wait().await?;
    assert!(!report.deadline_elapsed);
    Ok(())
}
```

独立宿主可直接构造 `RegistrationSpec`，也可将完成占位符解析的配置反序列化为 `assembly::DiscoveryConfig`，调用 `build()` 和 `resolve(listeners, eligibility)`。组件不展开环境变量；宿主须在创建网络资源前完成配置解析。直接传入已构造的 JSON map 时，原文重复键检查由宿主负责。

本地资格推荐 `Eligibility::channel()`：保留唯一 controller，周期性 `update(true, duration)`；有效期为 1 毫秒至 30 秒，controller 丢弃或到期立即失效。`Eligibility::from_check` 适合能同步读取生命周期与期限的宿主，闭包不得网络等待、重入 owner，且必须自行包含停机判断。资格不可反读包含自身注册结果的整体 readyz，否则会形成循环等待。

## napp 自动接管与多 Nacos

启用 NASA 的 `application`、`web`、`discovery-nacos`，使用 `#[nasa::application("web", "discovery")]`。需要 REST 时增加 `rest-discovery`，需要动态 gRPC 调用时增加 `grpc-discovery`。宏在监听器完成绑定后解析注册计划，等待启动必需目标确认后才完成启动。

```yaml
application:
  name: order-api
server:
  host: 0.0.0.0
  port: 8080
  health: true
discovery:
  enabled: true
  eligibility_probe: true
  connections:
    primary:
      provider: nacos
      nacos:
        server_addr: http://nacos-business:8848/nacos/
        namespace: public
        group: BUSINESS
        username: ${BUSINESS_NACOS_USERNAME}
        password: ${BUSINESS_NACOS_PASSWORD}
        services:
          self: {name: order-api}
          orders: {name: order-worker}
      rest:
        request_timeout_ms: 5000
        connect_timeout_ms: 1000
        lb_strategy: weighted
    infrastructure:
      provider: nacos
      nacos:
        server_addr: http://nacos-infra:8848/nacos/
        namespace: public
        group: INFRASTRUCTURE
        username: ${INFRA_NACOS_USERNAME}
        password: ${INFRA_NACOS_PASSWORD}
        services:
          coordinator: {name: saga-server}
  registrations:
    business-http:
      qualifier: primary
      service_name: self
      listener_ref: web
      eligibility_ref: web
      ip: ${BUSINESS_REGISTER_IP}
      port: 18080
      startup_required: true
      readiness_critical: true
```

`qualifier` 统一表示连接名称，独立于 provider 类型和服务别名。`app.discovery()?.client(Some("infrastructure"))` 选择基础设施环境；`app.discovery()?.rest(Some("primary"))` 选择业务 HTTP 调用器；`rest(None)` 和默认 `RestDiscovery` 门面采用 `primary`。声明式 REST 使用 `#[GetMapping(path = "/orders", qualifier = "primary")]` 等属性，并以 `Client::with_clients(app.discovery()?.rest_clients()?)` 注入具名集合；方法值覆盖 `#[rest_client(qualifier = "...")]` 的默认值。未标注的方法使用 `primary`，显式未知名称直接失败，内部服务请求不因实例为空回退到其他连接或普通 DNS。`Client::new(selected_client)` 保留单客户端注入，仅用于未显式选择 qualifier 的方法。

默认连接采用固定 qualifier `primary`（区分大小写），无需额外默认配置字段。一个或多个连接都不按数量、排序推断默认项；没有 `primary` 的目录仍可显式选择，省略选择则失败。注册目标与 Saga 目标也使用 `qualifier`；注册目标省略时为 `primary`，Saga 省略时使用 owner 的默认选择。旧 `connection_ref` 作为兼容别名读取，同时声明 `qualifier` 与该别名会拒绝。Rust 既有公开结构字段保留原名称，序列化使用 `qualifier`；旧 `default_connection` 配置和 builder 方法继续兼容，其显式引用优先于 `primary`，但不会改变显式 qualifier 的匹配。

`service_name` 是连接内服务别名，不是自动展开的任意远端服务名。增加连接不会自动注册当前应用。每个网络的注册地址和映射端口分别配置；框架不能推断容器宿主机地址。禁止把 `0.0.0.0`、`::` 注册为可达地址。新模式不会隐式读取 `LOCAL_NETWORK_IP`，需要它时显式写占位符。

`listener_ref` 选择 `web`、`grpc` 或 `Application::configure_discovery_endpoint` 登记的端点；只有一个 listener 时可省略。`eligibility_ref` 默认使用对应 listener 的本地资格。独立资格探针 `/discovery/eligible/{registration}` 返回真实 200/503，与业务 JSON 状态包装分离；只在 `eligibility_probe: true` 时开放，须限定网络访问。

只有明确选择 `grpc` 或唯一 listener 为 `grpc` 的注册计划才自动生成 `nasa.grpc.*` 协议元数据。自定义端点保留自身协议边界，不继承同进程另一 listener 的 TLS 声明；使用受管 gRPC 客户端访问无协议元数据的自定义端点时，显式设置 `DiscoveryProtocol`。

旧 `rest_discovery` 与 `nacos-discovery` 宏入口仍保留原合同。新旧根节点同时出现即拒绝，即使其中一个 disabled；迁移时删除旧根。新根 disabled 时不能同时携带连接、注册、默认引用或资格探针配置。零连接只允许外部 URL，内部查询立即拒绝。

## 声明式 HTTP 与外部 API

启用门面 `rest-client`，以 `Client::with_clients(app.discovery()?.rest_clients()?)` 注入共享集合。
`service + path` 根据 qualifier 使用对应发现证据；完整 `url` 使用默认 HTTP 客户端，
不查询注册中心，也不接受或继承 qualifier。零连接目录提供 external-only 客户端；
存在具名连接但缺少默认项时，未标注调用仍然失败。

公网请求不依赖 Nacos 实例列表，但应用启动仍受必需连接的准备结果约束。宏不展开 YML 中
的地址占位符；动态 URL 使用请求构建器。见[内部服务和 Google API 示例](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md)。

## 配置、容量和时效

全部连接、注册、默认目标、REST 策略和凭据来源在 owner 周期内冻结，变更要求重启。napp 热更新遇到 `discovery` 变化时拒绝整份候选配置，保留原配置视图与原连接。后端自身 token 续期、既定 ServiceAccount token 文件轮换不属于更换连接配置。重复键、未知字段、未知引用、错误 provider 块与非法预算拒绝装配；引用或 provider 错误携带具体配置路径，例如 `discovery.connections.business.provider`。

| 配置 | 默认值 | 约束或含义 |
| --- | --- | --- |
| `enabled` / `eligibility_probe` | false / false | 显式启用发现与独立探针 |
| 默认连接 | `primary` | 固定连接名；缺失时只允许显式选择 |
| `registrations.*.qualifier` | `primary` | 精确选择发布目标的具名连接，不因后端同类而合并 |
| `connections.*.startup_required` | true | 准备失败阻止启动；必需注册提升其所属连接要求 |
| `request_timeout_ms` / `poll_interval_ms` / `idle_timeout_ms` | 3000 / 5000 / 30000 | 单次操作含排队；周期复验；无人持有观察后的空闲回收 |
| `snapshot.max_age_ms` / `stale_if_error_ms` | 15000 / 0 | 旧证据最大年龄 / 查询异常宽限；宽限不大于最大年龄 |
| `retry.initial_backoff_ms` / `max_backoff_ms` | 1000 / 30000 | 指数退避，使用当前上限一半到上限之间的抖动 |
| `registrations.*.startup_required` / `readiness_critical` | true / false | 首次确认 / 运行期是否影响整体就绪，两个维度独立 |
| `monitor_interval_ms` / `observation_grace_ms` / `deregister_timeout_ms` | 5000 / 15000 / 3000 | 回查周期 / 有效回查证据期限 / 单目标注销上限 |
| `limits.max_connections` / `max_registrations` | 16 / 64 | 硬上限 256 / 65536 |
| `max_service_watches_per_connection` / `max_parallel_operations` | 1024 / 4 | 服务观察槽 / 一次性控制操作并发 |
| `max_watch_streams` | 64 | 长 watch 全局并发，最大 4096；Kubernetes 同时观察 Service 与 EndpointSlice，占用两份；排队期间继续周期查询 |
| `max_instances_per_service` / `max_total_instances` | 4096 / 65536 | 总量包括仍被消费者持有的旧快照 |
| `max_snapshot_bytes` / `max_transport_tasks` | 67108864 / 4096 | 已接纳快照字节 / owner 托管的连接、预约与正文任务 |

配置时要求 `poll_interval_ms + request_timeout_ms < max_age_ms`，`monitor_interval_ms + request_timeout_ms < observation_grace_ms`。时长最多一天。连接上限与每连接观察上限的乘积不能超过 65536。队列竞争会消耗期限；容量上限不承诺所有服务均能在默认年龄内复验，超载会明确不可用。快照字节额度包含已接纳实例数组、metadata 和后端身份，不等同于进程总内存；provider 响应字节限制及控制请求/watch 并发共同约束接收和解码中的候选，不把容量截断当成完整快照。

注册回查的暂时失败保留最后一次 `Healthy` 的 `observation_grace_ms` 截止点，失败不能延长该期限；明确禁用、缺失、不健康、身份撤销或非法证据立即取消就绪资格。`Observation::changed(deadline)` 只在收到变化通知或真实证据到期时成功；只有调用方等待期限到达时返回 `DeadlineExceeded`，静态证据不会因等待超时而生成到期事件。

每连接 `rest` 默认为总预算 10000 毫秒、连接 2000 毫秒、`lb_strategy: round_robin`、`instance_scheme: http`、`max_attempts: 1`。可选 `weighted`、`https`，以及 `retry_get_head_on_transport_error`、`retry_get_head_on_retryable_status`（均默认 false）；尝试次数 1–16，共享原预算。REST 不叠加另一层发现缓存宽限。需要更多调用选项的独立宿主可直接给 `RestDiscoveryClient` 传入选项。

## 后端与协议边界

| 后端 | 注册 | 发现证据 | 配置入口 |
| --- | --- | --- | --- |
| Static | 无 | 显式静态事实 | `static.services` 的完整实例列表 |
| DNS | 无 | A/AAAA 地址，不证明业务健康 | `dns.services.<alias>: {host, port}`；无 SRV 或 DNS TTL 承诺 |
| Nacos | 临时或持久实例 | 实例 enabled、healthy、weight | [nanacos](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nanacos/README.md) |
| Consul | 固定 agent、HTTP 或 TTL 检查 | 节点与服务检查均 passing | [nadisc-consul](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-consul/README.md) |
| Kubernetes | 无 | Service 与归属正确的 EndpointSlice 联合事实 | [nadisc-kubernetes](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-kubernetes/README.md) |

Static/DNS 不是探活器；快照标记 `EvidenceKind`，调用方不能把地址存在解释为数据库、业务或租约健康。`Snapshot::identities()` 保留 provider 实例身份，不能只按 IP/端口判断重建。

动态 gRPC 使用 `nagrpc::discovery::DiscoveryGrpcService`，可以注入生成客户端。对没有协议 metadata 的 DNS/Kubernetes，调用方使用完整 `DiscoveryProtocol` 指定 authority 和 TLS 模式，端口来自当前实例；两种协议来源互斥。旧 `grpc_endpoint()` 和自行构造的 Channel 是一次性选址，不获得动态撤权能力。

## Saga 的独立使用场景

Saga 目标可配置 `qualifier`，并继续要求 service identity、incarnation、generation、地址白名单与同代协议投影。多环境注册复用进程 incarnation；不同网络或恢复目标采用分开的服务别名/查询集合，不能将同一 incarnation 的不同同代地址混入同一查询集合。

同一后端查询集合的不同同代协议投影在注册前拒绝。HTTP 发起、Registry 和结果投递的受管响应正文上限为 4 MiB，具体调用协议可设置更小上限。

普通新业务注册绑定 `eligibility_ref: web` 或 `grpc`。需要 capability 续租和已提交结果恢复时，另设注册目标与服务别名，绑定 `eligibility_ref: saga-recovery`；该资格只表示 Saga 运行中且应用未停机，不授予普通业务的目录执行权。请求仍执行完整认证、结果信任期限与数据库事务门禁。固定受信 URL 也是可选恢复入口，不能用整体 `/readyz` 将其一并摘除。

Saga 使用具名连接执行 gRPC 调用时，每次最多并发检查 16 个候选的 health；探测最多占用当前剩余请求预算的 75%，首个明确 Serving 的成员用于本次业务提交，其余探测随即取消。下一次请求轮换候选起点；全部失败时本次请求拒绝。提交前再次核对当前凭据和注册事实，不自动重发已经提交的业务 RPC。

## 运维与停机结果

`owner.observer().snapshot()` 提供阶段、准备状态、查询计数与耗时、证据年龄、快照容量、传输任务数和注册状态。napp 的 Prometheus 输出使用 `napp_discovery_*`，只以固定配置连接名、注册名和封闭状态标注，不放密码、地址、任意服务名或 metadata。`prepared` 仅表示初始化通过，不表示所有业务实例健康。

监控 `RegistrationState::OutcomeUnknown` / `Residual`、查询错误、证据年龄、非 Ready 注册和关闭预算耗尽。永久实例、进程崩溃、不可达注销及同地址进程交叠可能留下远端记录；这些情况不能靠 Drop 保证删除。显式检查 `CloseReport`，且保持 Tokio 存活直到清理结束。REST 裸 `send()` 的响应由调用方释放；使用 `send_managed()` 并关闭 REST 客户端，napp 会统一回收它们。

## License

MIT OR Apache-2.0。
