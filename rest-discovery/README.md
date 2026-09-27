# rest-discovery

`rest-discovery` 是带服务发现和客户端负载均衡的 HTTP 客户端。它只依赖 provider-neutral 的 `nadisc::DiscoveryClient` 抽象，不依赖 Nacos；Nacos 装配在 `rest-discovery-nacos`。
与 `application` 组合时，`rest_clients` 可装配 external、static、dns 或 custom 来源，统一拥有
服务 watch、请求准入与退出等待；普通出站调用无需启动注册中心或入站 Web。

业务项目通过门面开启 `rest-discovery`：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["rest-discovery"] }
```

## 全局初始化

```rust ignore
use std::sync::Arc;
use nasa::discovery::DiscoveryClient;
use nasa::discovery::RestDiscovery;
use nasa::discovery::rest::RestDiscoveryOptions;

async fn init(discovery: Arc<dyn DiscoveryClient>) -> anyhow::Result<()> {
    let opts = RestDiscoveryOptions::new();
    RestDiscovery::init_with_discovery(discovery, opts).await?;
    Ok(())
}
```

外部-only 模式适合本地开发或只调用公网 HTTP：

```rust
nasa::discovery::RestDiscovery::init_external_only(
    nasa::discovery::rest::RestDiscoveryOptions::new(),
)
.await?;
```

进程级 runtime 可显式关闭——全局槽自身持有强引用，只 drop 手上的 `Arc` 不会停掉索引刷新与 watch 后台任务：

```rust
// 幂等:取下当前 runtime 并主动终止其后台任务;槽为空时返回 false。
nasa::discovery::RestDiscovery::shutdown();

// 只在槽仍指向自己安装的实例时关闭(Arc 指针相等判定),避免误关后来安装的 runtime:
// nasa::discovery::RestDiscovery::shutdown_if_current(&runtime);
```

`#[application]` 的 `nacos-discovery` 组件停机时走 `shutdown_if_current`，顺序固定为"先摘流 → drain → 最后关 runtime"。

## 内部服务调用

显式 service 调用：

```rust
use nasa::discovery::RestDiscovery;
use nasa::discovery::rest::Method;

let rest = RestDiscovery::get();
let dto: OrderDto = rest
    .service_request("order-service", Method::GET, "/api/orders/1001")
    .send_json()
    .await?;
```

`lb://` URL 调用：

```rust
let text = RestDiscovery::get()
    .get("lb://order-service/api/health")
    .send_text()
    .await?;
```

普通外部 HTTP：

```rust
let bytes = RestDiscovery::get()
    .get("https://example.com/file.bin")
    .send_bytes()
    .await?;
```

## 请求构建器

```rust
let resp: UserDto = RestDiscovery::get()
    .post("lb://user-service/api/users")
    .header("X-Trace-Id", trace_id)
    .query_pair("verbose", true)
    .json(&create_user)
    .timeout(std::time::Duration::from_secs(3))
    .send_json()
    .await?;
```

解包统一响应字段：

```rust
let data: UserDto = RestDiscovery::get()
    .get("lb://user-service/api/users/1001")
    .send_json_unwrap("data")
    .await?;
```

## 选项

```rust
use nasa::discovery::rest::{
    HeuristicHttpMode, InstanceScheme, LbStrategy, RestDiscoveryOptions, RetryOptions,
    RestResilienceOptions, SchemePolicy,
};

let opts = RestDiscoveryOptions::new()
    .with_default_instance_scheme(InstanceScheme::Http)
    .with_scheme_policy(SchemePolicy::Preserve)
    .with_heuristic_http(HeuristicHttpMode::Disabled)
    .with_lb_strategy(LbStrategy::RoundRobin)
    .with_retry(RetryOptions::get_head_on_transport_error(2))
    .with_resilience(
        RestResilienceOptions::default()
            .with_max_concurrent_per_service(256)
            .with_circuit(5, std::time::Duration::from_secs(30))
            .with_outlier_ejection(3, std::time::Duration::from_secs(30)),
    );
```

## 识别规则

- `service_request(service, method, path)`: 显式内部服务调用。
- `lb://service/path`: 显式内部服务调用。
- `http(s)://host/path`: 默认普通外部 HTTP；启用 heuristic 后，host 命中服务索引才走内部 LB。

## 声明式客户端

配合 `rest-client-macro` 可从 trait 生成客户端：

```rust ignore
#[rest_client_macro::rest_client(service = "order-service")]
trait OrderApi {
    #[rest_client_macro::GetMapping("/api/orders/{id}")]
    async fn get(&self, #[PathVariable("id")] id: i64) -> anyhow::Result<OrderDto>;
}
```

- 返回类型接受 `anyhow::Result<T>`(错误经 anyhow 收敛)或
  `Result<T, E>`(要求 `E: From<RestDiscoveryError>`)——全部失败点都是已脱敏的
  `RestDiscoveryError`，业务错误转换不存在回显远端敏感正文的通道。
- `#[QueryMap]` 支持 `Option<T>` 的稳定合同：`None` 不追加任何 query 片段，`Some` 按字段声明
  顺序展开内层结构体；`Option` 本身从不进序列化器。
- 未显式绑定 trace 上下文的调用在环境(task-local)链路作用域内发起时，自动延续同一 trace
  并派生 client 子 span；显式绑定始终优先。

## YML 配置与使用

`rest-discovery` 本体是 provider-neutral 运行时，不直接读取 yml。若需要从 yml 一键装配 Nacos 注册发现，请使用 `rest-discovery-nacos` 的 `rest_discovery:` 配置。只用本 crate 时，推荐应用定义 `rest:` 段再映射到 `RestDiscoveryOptions`。

以下是业务自行定义并映射的独立配置示例，不是 Application 的 `rest_clients` 配置结构：

```yaml
rest:
  heuristic_http: false
  service_match: case_insensitive
  scheme_policy: preserve
  default_instance_scheme: http
  lb_strategy: round_robin
  startup: require_initial_service_list_when_heuristic_enabled
  unknown_host: external_http
  no_instance: error
  preserve_original_host_header: false
  watch:
    poll_interval_ms: 2000
    ttl_fallback_ms: 3000
    stale_if_error_ms: 10000
    restart_backoff_min_ms: 1000
    restart_backoff_max_ms: 30000
  heuristic:
    refresh_interval_ms: 30000
    removed_service_grace_ms: 60000
  retry:
    get_head_on_transport_error: false
    max_attempts: 1
  http:
    timeout_ms: 10000
    connect_timeout_ms: 2000
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `heuristic_http` | `false` | 裸 `http(s)://host` 是否尝试把 host 当服务名。 |
| `service_match` | `case_insensitive` | 服务名索引匹配大小写策略。 |
| `scheme_policy` | `preserve` | 裸 URL 命中内部服务时是否保留原始 scheme。 |
| `default_instance_scheme` | `http` | `lb://` 和 `service_request` 默认访问实例协议。 |
| `lb_strategy` | `round_robin` | 内置负载均衡策略。 |
| `startup` | `require_initial_service_list_when_heuristic_enabled` | 启发式模式启动期是否要求服务列表可用。 |
| `unknown_host` | `external_http` | 裸 URL host 未命中服务索引时的处理策略。 |
| `no_instance` | `error` | 内部服务无实例时的处理策略。 |
| `preserve_original_host_header` | `false` | 是否保留原始 Host header。 |
| `watch.*` | 见示例 | 实例 watch、TTL、降级窗口和重建退避。 |
| `heuristic.*` | 见示例 | 服务名列表索引刷新参数。 |
| `retry.*` | 见示例 | GET/HEAD 传输错误跨实例重试。 |
| `resilience.*` | 见代码配置 | 每服务 bulkhead/熔断与单实例异常摘除。 |
| `http.*` | 见示例 | 底层 HTTP client 超时。 |

初始化代码通常是：

```rust
let opts = nasa::discovery::rest::RestDiscoveryOptions::new()
    .with_retry(nasa::discovery::rest::RetryOptions::get_head_on_transport_error(2));

nasa::discovery::RestDiscovery::init_with_discovery(discovery, opts).await?;
```

没有服务发现后端时，用 `init_external_only`；此时普通外部 URL 可用，`lb://` 会返回明确错误。

## 主要边界

- `lb://` 和 `service_request` 明确表示内部服务；裸 HTTP 启发式默认关闭。
- 只有 GET/HEAD 或调用方显式声明幂等的请求才允许跨实例重试。
- discovery、attempt、退避和 `Retry-After` 必须消耗同一个 `RequestBudget`。
- bulkhead 无等待队列，满载立即拒绝；熔断窗口到期只放一个 half-open 探针。
- 实例异常摘除只影响当前进程的候选快照，窗口到期自动恢复；服务状态表和实例状态表均有硬上限。
- 进程级 runtime 需要显式 `shutdown` / `shutdown_if_current`，只丢弃局部 `Arc` 不会停止后台 watch。
- 外部 URL 与内部服务名的识别失败不能静默改写请求目标。

## Application 接入

门面开启 `application,rest-discovery`，直接依赖 `napp` 时对应 `rest` feature。以下命名计划由
Application 标准装配，无需新增组件字符串：

```yaml
rest_clients:
  inventory:
    enabled: true
    provider: static
    timeout_ms: 3000
    connect_timeout_ms: 1000
    max_services: 1024
    services:
      inventory:
        - host: inventory.internal
          port: 8080
```

Service 在 initializer 或 Ready 后通过 `app.rest_client("inventory").await` 取得实例，Batch 在
工作负载前完成装配。`external` 只调用显式 URL；`dns` 每服务配置一个域名与端口；`custom` 使用
`provider_ref` 借用已登记的发现 provider，其独立资源仍由原 owner 管理。

```text
命名来源 → 有界服务 watch 与候选快照 → 实例选择 → 同一预算内调用及重试
停机     → 关闭请求与任务登记准入 → 等待退出 → 释放 runtime
```

每个实例默认最多 1024 个服务 watch，上限 65536。关闭准入与任务登记使用同一门禁，晚到 watch
不能重建已关闭任务；shutdown 等待任务与登记调用真实退出，旧句柄永久返回 `RuntimeClosed`。
带预算的 `send_text/send_bytes/send_json` 覆盖完整正文；直接 `send` 返回的原始 response 仍由
调用方负责正文期限。参数变更需要重启，不因读取新配置就替换运行实例。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
