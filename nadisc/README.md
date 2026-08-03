# nadisc

`nadisc` 是服务发现的 provider-neutral 合同层，并提供不依赖第三方注册中心的 `StaticDiscovery` 与
`DnsDiscovery`。Nacos 等外部注册中心仍由独立 adapter 实现。

它定义：

- `Instance`：服务实例的中性表示。
- `is_traffic_instance`：统一判断实例是否可承载业务流量。
- `DiscoveryClient`：服务发现读侧接口。
- `ServiceRegistry`：服务注册写侧接口。
- `ServiceWatch`：实例变化订阅结果。
- `StaticDiscovery`：可原子替换、失败保留 last-good 的进程内静态 provider。
- `DnsDiscovery`：基于系统 DNS 的 A/AAAA 查询与有界周期 watch。

后端 crate 例如 Nacos 负责实现这些 trait；上层 REST 负载均衡只依赖本 crate 的抽象，不绑定具体 SDK。

业务项目通过门面开启 `discovery`：

```toml
[dependencies]
nasa = { version = "1", features = ["discovery"] }
```

```rust
use nasa::discovery::{is_traffic_instance, Instance};

let inst = Instance::new("10.0.0.8", 8080)
    .with_weight(10.0)
    .with_metadata("zone", "ap-southeast");

assert!(is_traffic_instance(&inst));
```

流量过滤规则：实例必须启用、健康、权重大于 0、IP 非空且无首尾空白、端口非 0。

## 流量过滤规则(实测)

`is_traffic_instance` 拒绝以下任一情况:`enabled=false`、`healthy=false`、weight ≤0 / NaN / ∞、IP 为空或带首尾空白、`port == 0`。负载均衡应只消费过滤后的实例。

## Watch 契约

`ServiceWatch` 的 drop 取消订阅由各后端 guard 的 `Drop` 实现 best-effort 保证(异步注销无法在 Drop 中等待);需要确定性清理时显式 `unsubscribe().await`。`WatchOptions::poll_interval` 不允许为零(`validate()` 拒绝)。

## YML 配置与使用

`nadisc` 是抽象层，不连接具体注册中心，因此没有自己的固定 yml 根节点。后端组件可以把实例和 watch 选项映射到本 crate 的中性类型。

推荐配置示例：

```yaml
discovery:
  watch:
    poll_interval_ms: 5000
  instance:
    ip: 127.0.0.1
    port: 8080
    enabled: true
    healthy: true
    weight: 1.0
    metadata:
      zone: local
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `watch.poll_interval_ms` | `5000` | 后端 watch 的轮询兜底间隔，可映射为 `WatchOptions::with_poll_interval`。 |
| `instance.ip` | 必填 | 注册或过滤时使用的实例 IP。 |
| `instance.port` | 必填 | 实例端口，0 不可承载流量。 |
| `instance.enabled` | `true` | 是否启用实例。 |
| `instance.healthy` | `true` | 实例健康状态。 |
| `instance.weight` | `1.0` | 负载均衡权重，必须大于 0 才承载流量。 |
| `instance.metadata` | `{}` | 透传给后端或负载均衡策略的元数据。 |

代码映射：

```rust
let inst = nasa::discovery::Instance::new(
    &cfg.discovery.instance.ip,
    cfg.discovery.instance.port,
)
    .with_weight(cfg.discovery.instance.weight);

let opts = nasa::discovery::WatchOptions::new()
    .with_poll_interval(std::time::Duration::from_millis(
        cfg.discovery.watch.poll_interval_ms,
    ));
```

如果业务已经使用 `nanacos` 或 `rest-discovery-nacos`，优先使用它们的配置结构，不需要单独定义 `discovery:`。

## 主要边界

- 本 crate 不连接 Nacos 等第三方注册中心；`DnsDiscovery::discover*` 会执行 DNS 查询，启动 watch
  后会拥有一个周期轮询任务，显式 `unsubscribe` 或 guard Drop 会终止该任务。
- 负载均衡只能消费 `is_traffic_instance` 过滤后的实例。
- watch 更新必须以完整快照替换，后端异常时由上层决定 last-good 和 stale 窗。
- 需要确定性取消时显式 `unsubscribe().await`，不能只依赖 Drop 的 best-effort 清理。
