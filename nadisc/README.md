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
nasa = { version = "2.0.3", features = ["discovery"] }
```

```rust
use nasa::discovery::{is_traffic_instance, Instance};

let inst = Instance::new("10.0.0.8", 8080)
    .with_weight(10.0)
    .with_metadata("zone", "ap-southeast");

assert!(is_traffic_instance(&inst));
```

流量过滤规则：实例必须启用、健康、权重大于 0、IP 非空且无首尾空白、端口非 0。

## 受管观察与发送许可

`nadisc::managed` 定义 provider 能力、完整快照、证据等级、稳定错误类别、注册事实和实际传输提交许可。它不创建宿主生命周期；多连接与清理由 [nadisc-runtime](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-runtime/README.md) 持有。现有 `DiscoveryClient` 实现保持兼容，新增准入方法具有默认行为；需要动态撤权的客户端必须在实际发送处调用受管句柄的准入方法，不能仅复制 IP 列表。

`IncompleteSnapshot` 表示本轮分页、解码或传输未完成，`CapacityExceeded` 表示容量拒绝；两者不发布部分候选，普通读取仅可沿用配置宽限内的完整旧证据，严格读取失败。`InvalidSnapshot` 表示已确认候选或对象身份非法，与权限撤销、身份冲突一样立即撤销旧证据，不能借宽限继续发送。

provider 实例身份与地址不同：Consul 使用节点及 Service ID，Kubernetes 保留 Service/目标对象 UID。明确空集合、授权拒绝和不完整快照分别表达，错误不能伪装成健康空列表。

`DiscoveryError` 保留稳定的 `code` 和静态 `reason`；配置错误可通过 `config_path` 定位到具体连接或注册字段。路径只包含字段与已校验名称，不包含配置值、认证材料或远端响应正文。

`ManagedProvider::supports_idempotent_renewal` 默认返回 false。只有可对已确认注册身份重复相同健康 payload、且不重建身份或覆盖管理属性的 adapter 才能声明此能力。它允许 owner 协调响应丢失后的续约，不代表旧请求已终止，也不授权重放首次注册或相反写入。


## 流量过滤规则

`is_traffic_instance` 拒绝以下任一情况:`enabled=false`、`healthy=false`、weight ≤0 / NaN / ∞、IP 为空或带首尾空白、`port == 0`。负载均衡应只消费过滤后的实例。

## Watch 契约

`ServiceWatch` 的 drop 取消订阅由各后端 guard 的 `Drop` 实现 best-effort 保证(异步注销无法在 Drop 中等待);需要确定性清理时显式 `unsubscribe().await`。`WatchOptions::poll_interval` 不允许为零(`validate()` 拒绝)。

## 配置归属

本 crate 定义中性类型与 provider 合同，不读取应用配置。使用多注册中心时，由
`nadisc-runtime::assembly::DiscoveryConfig` 解析 `discovery.connections` 和 `registrations`，
napp 负责把完成占位符解析的配置交给该组件；不要把中性 `Instance` 字段当成独立配置根。

连接通过 `qualifier` 选择，默认连接名为 `primary`。Static/DNS 在公共 runtime 中分别使用
`static.services` 与 `dns.services`；Nacos、Consul、Kubernetes 由各自 adapter 提供配置。
普通 `DiscoveryClient` 接口不拥有多连接目录，也不替调用方执行注册或关闭。

[完整配置与调用示例](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md)。

## 主要边界

- 本 crate 不连接 Nacos 等第三方注册中心；`DnsDiscovery::discover*` 会执行 DNS 查询，启动 watch
  后会拥有一个周期轮询任务，显式 `unsubscribe` 或 guard Drop 会终止该任务。
- 负载均衡只能消费 `is_traffic_instance` 过滤后的实例。
- watch 更新必须以完整快照替换，后端异常时由上层决定 last-good 和 stale 窗。
- 需要确定性取消时显式 `unsubscribe().await`，不能只依赖 Drop 的 best-effort 清理。
