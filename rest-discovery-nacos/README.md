# rest-discovery-nacos

`rest-discovery-nacos` 是 `rest-discovery` 与 `nanacos` 的装配层。它把强类型 `DiscoveryConfig` 映射成 Nacos 连接、可选实例注册、`RestDiscoveryOptions`，并安装全局 `RestDiscovery`。

`rest-discovery` 本体保持 provider-neutral，不直接依赖 Nacos。

普通业务只依赖门面；需要直接控制 `DiscoverySession` 的框架集成层再显式增加 adapter：

```toml
[dependencies]
nasa = { version = "1", features = ["nacos-sdk", "rest-discovery-nacos"] }
rest-discovery-nacos = { version = "1", features = ["nacos-sdk"] }
```

## 配置形状

```yaml
rest_discovery:
  enabled: true
  provider: nacos
  nacos:
    server_addr: 127.0.0.1:8848
    namespace: public
    group: DEFAULT_GROUP
    app_name: order-service
  registration:
    enabled: true
    service_name: order-service
    ip: 10.0.0.10
    port: 8080
  rest:
    heuristic_http: false
    lb_strategy: round_robin
```

## 初始化并注册

```rust
use nasa::discovery::{init_from_config, AppRegistrationInfo, DiscoveryConfig, DiscoveryHandle};

async fn init(cfg: &DiscoveryConfig) -> anyhow::Result<DiscoveryHandle> {
    let app = AppRegistrationInfo::new("order-service", "10.0.0.10", 8080);
    init_from_config(cfg, app).await
}
```

优雅停机时先下线注册实例，再 drain HTTP server：

```rust
let mut handle = init(&cfg).await?;
handle.deregister().await?;
```

## 分段生命周期

`init_from_config` 把"连 provider + 装出站 runtime + 注册本实例"捆成一步。需要三段各自独立时（出站 `lb://` 先于注册可用、注册使用真实监听端口、停机先摘流再关客户端后台任务）用 `DiscoverySession`：

```rust
use rest_discovery_nacos::{prepare_from_config, AppRegistrationInfo};

// 只连接 provider 并安装出站 runtime,不注册本实例;此后 lb:// 即可用。
let mut session = prepare_from_config(&cfg).await?;

// 监听端口就绪后再注册(重复调用 no-op):
session.register(AppRegistrationInfo::new("order-service", "10.0.0.10", real_port)).await?;

// 停机:先摘流(幂等),等在途请求 drain 完成后最后关 runtime(clear-if-current,不误关后来者)。
session.deregister().await?;
session.shutdown_runtime().await?;
```

`#[application]` 的 `nacos-discovery` 组件即按这三段编排（Start 装 runtime / Ready 注册 / 停机反序）；`init_from_config` 保留给不使用应用运行时的项目。

## 自定义负载均衡

```rust
use std::sync::Arc;
use nasa::discovery::rest::{LoadBalancer, RoundRobinLoadBalancer};

let lb: Arc<dyn LoadBalancer> = Arc::new(RoundRobinLoadBalancer::new());
let handle =
    nasa::discovery::init_from_config_with_load_balancer(&cfg, app, lb).await?;
```

## IP 解析优先级

注册 IP 按以下优先级选择：

1. `LOCAL_NETWORK_IP` 环境变量
2. `rest_discovery.registration.ip`
3. `rest_discovery.nacos.discovery_ip`
4. 全部缺失则 fail-fast

`registration.enabled = false` 时只作消费者，不注册自己。

## feature

真实 Nacos 后端需要启用 `nacos-sdk` 或 `live-nacos` feature；不开时底层 Nacos 连接会返回明确错误。

## 完整 YML 配置

生产推荐把服务发现独立放在 `rest_discovery:` 根节点，不和配置中心 `nacos:` 混用。完整形状如下：

```yaml
rest_discovery:
  enabled: true
  provider: nacos
  nacos:
    server_addr: 127.0.0.1:8848
    namespace: ""
    group: DEFAULT_GROUP
    app_name: order-service
    username: ${NACOS_USERNAME:}
    password: ${NACOS_PASSWORD:}
    discovery_ip: 127.0.0.1
  registration:
    enabled: true
    service_name: order-service
    ip: 127.0.0.1
    port: 8080
    ephemeral: true
    healthy: true
    weight: 1.0
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
    heuristic:
      refresh_interval_ms: 30000
      removed_service_grace_ms: 60000
    watch:
      poll_interval_ms: 2000
      ttl_fallback_ms: 3000
      stale_if_error_ms: 10000
      restart_backoff_min_ms: 1000
      restart_backoff_max_ms: 30000
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
| `enabled` | `false` | `false` 时只初始化 external-only HTTP 客户端，不注册也不消费服务发现。 |
| `provider` | `nacos` | 后端类型；当前只支持 `nacos`。 |
| `nacos.server_addr` | `""` | Nacos SDK 地址。 |
| `nacos.namespace` | `""` | namespace ID；public 空间留空。 |
| `nacos.group` | `""` | 注册和发现使用的 group；通常填 `DEFAULT_GROUP`。 |
| `nacos.app_name` | `""` | Nacos 端展示和审计用应用名。 |
| `nacos.username/password` | `""` | 鉴权参数；密码建议用环境变量覆盖。 |
| `nacos.discovery_ip` | `null` | 注册 IP 的第三优先级兜底。 |
| `registration.enabled` | `true` | `false` 表示只作消费者。 |
| `registration.service_name` | `""` | 为空时回退到 `AppRegistrationInfo.service_name`。 |
| `registration.ip` | `""` | 注册 IP 第二优先级；第一优先级是 `LOCAL_NETWORK_IP`。 |
| `registration.port` | `0` | 0 时回退到 `AppRegistrationInfo.port`。 |
| `registration.ephemeral` | `true` | 是否注册临时实例。 |
| `registration.healthy` | `true` | 初始健康状态。 |
| `registration.weight` | `1.0` | 实例权重。 |
| `rest.*` | 见上表 | 直接映射到 `rest-discovery` 运行选项。 |

最小启动代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    rest_discovery: nasa::discovery::DiscoveryConfig,
}

let app = nasa::discovery::AppRegistrationInfo::new(
    "order-service",
    "0.0.0.0",
    8080,
);
let handle = nasa::discovery::init_from_config(&cfg.rest_discovery, app).await?;
```

优雅停机顺序：先 `handle.deregister().await?` 摘除实例，再停止接收新请求，最后等待 HTTP 连接 drain。

## 主要边界

- 真实连接必须启用 `nacos-sdk`；stub 层只保证 API 可编译，`enabled=true` 时会明确失败。
- 注册 IP 缺失或不安全时拒绝启动，不自动选择不可审计的网卡地址。
- `registration.enabled=false` 只安装出站 runtime，不注册当前实例。
- 分段生命周期必须保持“装出站 -> listener Ready -> 注册 -> 摘流 -> drain -> 关 runtime”的顺序。
- provider 异常时 watch 保留 last-good 的时长由 `rest.watch.stale_if_error_ms` 限定。
