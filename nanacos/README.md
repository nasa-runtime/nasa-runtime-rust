# nanacos

`nanacos` 提供 Nacos 配置传输与服务注册发现；受管 HTTP adapter 支持多注册中心中的独立连接、
精确注册身份核验与有界调用，兼容 SDK 入口提供配置订阅和单 Nacos 会话。

旧单 Nacos API 的传输由 [nanacos-sdk](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nanacos-sdk/README.md) 提供，保留原 SDK 类型和 feature；双向控制流失效会撤销旧连接身份并进入原有重连流程。新多注册中心 owner 的 `managed` feature 使用独立 HTTP adapter，不同时启用 SDK 的注册监督。

`nanacos` 是 Nacos 配置中心和注册中心传输门面。它只提供连接、鉴权、配置拉取、配置 watch、服务注册、发现和订阅能力，不认识业务 `AppConfig`，也不做配置合并。

业务项目通过门面开启真实传输：

```toml
[dependencies]
nasa = { version = "2.0.3", features = ["nacos-sdk"] }
```

## 多连接受管注册

`managed` feature 提供 `managed::ManagedNacos`，使用 Nacos 3 的 HTTP API，与旧 SDK 入口分开。发现、注册、心跳与删除使用 client API，注册所有权核验使用限定服务范围的 admin 实例详情。`nacos` feature 使用源自 nacos-sdk 0.8.3 的 `nanacos-sdk 1.0.0` 兼容传输；新 HTTP owner 路径不依赖 SDK 的后台 redo 生命周期，也不把释放旧 SDK 引用表述为终止其全部任务。旧配置中心和单 Nacos 用法保持原有入口。

`ManagedNacosConfig` 为每个连接固定 `server_addr`、`namespace`、`group`、`username`/`password`、`services` 及可选 `ca_pem`。`server_addr` 支持同一集群的逗号分隔服务器 URL；显式 HTTPS 开启校验，禁止 URL 用户信息、重定向和环境代理。多个地址不能混装不同集群，namespace 使用真实标识。服务别名绑定 `{name, cluster}`，`cluster` 默认省略，表示不额外过滤集群。查询枚举只返回显式绑定，不需要全局服务目录权限。

默认请求 3000 毫秒，响应最多四 MiB、4096 个实例。HTTP 登录 token 在过期前刷新；401/403 或权限拒绝使当前连接旧证据失效，不借其他连接身份。错误不回显密码、token 或原始响应。

受管连接将空 namespace 与 `public` 统一为默认空间，并对规范化服务器 URL 排序去重；请求轮换、注册键和查询键共用该服务器集合。两个连接若指向同一服务器集合、namespace、group、service 和端点，冻结注册计划时即拒绝重复目标，地址顺序或重复项不改变身份。HTTP 注册不能以 cluster 区分同端点的独立写入，因此跨 cluster 的同端点计划也拒绝；发现查询仍按配置的 cluster 独立筛选。身份字段独立转义并保留边界，合法名称中的冒号、引号等字符不会混淆不同 namespace、group 或 service。

client 流量列表可能隐藏禁用实例，不能用于证明注册不存在。注册、续约、健康回查和注销先读取 `/nacos/v3/admin/ns/instance` 的精确实例详情，再核对 cluster/IP/port、实例类型和 owner。只有 HTTP 404 同时携带 Nacos 资源缺失码 `20004` 才表示该精确记录不存在；普通 404 或缺少有效数据不能证明缺席。

首次 POST 还必须读取该服务的 `/nacos/v3/admin/ns/instance/list` 管理列表，不附加 cluster 筛选，并设置 `healthyOnly=false`。任何 cluster 中相同 IP/port 的既有记录都阻止写入，包括禁用、不健康、零权重或相同 owner 的记录，避免新注册覆盖既有管理意图。列表必须完整且在容量内；若列表以 HTTP 404/code `30000` 返回服务缺失候选，还需 `/nacos/v3/admin/ns/service` 返回 HTTP 400/code `21008` 才能确认新服务尚不存在。一般性 404 不能当成空集合。

请求仅访问配置绑定的 namespace/group/service，不枚举全局目录。自注册凭据须具备该服务的实例详情、管理列表和服务详情读取权限，以及所需写权限；权限不足、接口不可用、数据不完整或响应超限时明确失败，不退回流量列表，不覆盖已有记录，也不报告已删除。只发现连接仍使用 client API。该读写序列不提供后端 CAS 或跨副本线性一致性；同键进程交叠仍需部署侧隔离，不承诺同一服务混用临时和持久实例。

注册默认临时实例、`DEFAULT` cluster、weight 1；健康同时要求 enabled、healthy 和正权重。临时实例以 heartbeat 续期，持久实例没有崩溃自动清除保证。回查缺失或 disabled 后不盲目重新注册，避免覆盖管理端摘流。停止发布后明确注销，但超时仍可能残留；同 IP/端口键上的不同进程交叠也不能由 metadata 实现数据库级 fencing。

受管 HTTP adapter 声明临时实例心跳支持同身份幂等续约。心跳提交后响应丢失时，owner 可在退避后重复相同心跳，每次仍复验所有权及管理资格，不重发完整注册属性。续约恢复不证明此前请求已停止执行；存在此类未决写入时，后续注销仍保留残留报告。

[多 Nacos YAML、公共 owner 与 napp 装配](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-runtime/README.md)。此 adapter 的公开 HTTP 合同要求 Nacos 3；不能据 SDK 可连接其他服务端就推定这条路径兼容 Nacos 2。


## 选择受管连接

业务通过 `nasa` 开启 `application,web,discovery-nacos,rest-client`，声明
`#[nasa::application("web", "discovery")]`。在 `discovery.connections.primary.nacos` 中设置
HTTP `server_addr`、namespace、group 和服务别名；多个 Nacos 分别增加具名连接。
`primary` 是默认 qualifier，方法上的 `qualifier = "nacos-order"` 只选择同名连接。
注册清单使用 `qualifier` 单独选定发布目标，不因增加连接而自动注册。

调用 Google 等互联网 API 使用 `#[GetMapping(url = "https://dns.google/resolve")]`，
不配置 qualifier，不把公网域名放入 Nacos 服务绑定。
[完整 YML、客户端注入与 API 示例](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md)。下文 `NacosProps` 与 `nacos:` 示例
描述兼容 SDK 和配置中心接入，其地址形状不能直接替代受管 HTTP URL。

## 配置中心拉取

```rust
use nasa::config::nacos::{NacosConfigClient, NacosProps};

async fn fetch() -> anyhow::Result<String> {
    let props = NacosProps::new("127.0.0.1:8848")
        .with_namespace("")
        .with_group("DEFAULT_GROUP")
        .with_app_name("demo-app");

    let client = NacosConfigClient::connect(&props).await?;
    client.fetch("application.yml", "DEFAULT_GROUP").await
}
```

## 批量拉取和 watch

适合启动时一次性拉多个 import，并在热刷新时拿到 `ConfigBundle`。

```rust
let refs = vec![
    nasa::config::nacos::ConfigRef::required("base.yml")
        .with_group("DEFAULT_GROUP")
        .with_file_extension("yaml"),
];

let bundle = client.fetch_many(&refs).await?;
let (_guard, mut rx) = client.watch_many_channel(refs).await?; // guard 存活期间订阅有效

while rx.changed().await.is_ok() {
    let bundle = rx.borrow_and_update().clone();
    let _ = bundle;
}
```

`required` 引用在配置缺失或拉取失败时立即返回错误。`optional` 只在服务端明确确认配置不存在时
跳过；鉴权失败、网络中断和协议错误仍会终止本次拉取或刷新，避免把基础设施异常当成正常缺省。

## 服务注册与下线

```rust
use nasa::discovery::{nacos::{NacosDiscoveryClient, NacosProps}, Instance};

async fn register() -> anyhow::Result<()> {
    let props = NacosProps::new("127.0.0.1:8848")
        .with_group("DEFAULT_GROUP")
        .with_app_name("order-service");
    let client = NacosDiscoveryClient::connect(&props).await?;
    let inst = Instance::new("10.0.0.10", 8080).with_weight(1.0);
    let guard = client.register("order-service", inst).await?;

    guard.deregister().await?;
    Ok(())
}
```

## 服务发现

```rust
let instances = client.discover("order-service").await?;
for inst in instances {
    println!("{}:{}", inst.ip, inst.port);
}
```

## 边界

- 关 `nacos` feature 时，真实连接入口会返回清晰错误，避免误以为已连上 Nacos。
- 配置内容只以裸文本返回；格式解析和合并交给 `naml` / `config-boot`。
- 注册生命周期推荐显式持有 guard，并在优雅停机时先 deregister 再 drain HTTP。

## YML 配置与使用

`nanacos` 本身不反序列化固定根节点，推荐应用把连接参数写成一个 `nacos:` 段，再转换为 `NacosProps`。只用配置中心、只用注册中心、或两者都用时都可以复用同一组连接参数。

完整示例：

```yaml
nacos:
  server_addr: 127.0.0.1:8848
  namespace: ""
  group: DEFAULT_GROUP
  app_name: order-service
  discovery_ip: 127.0.0.1
  username: ${NACOS_USERNAME:}
  password: ${NACOS_PASSWORD:}
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `server_addr` | 必填 | Nacos SDK 地址，支持 `host:port[,host:port]`。 |
| `namespace` | `""` | namespace ID；public 空间留空。 |
| `group` | `DEFAULT_GROUP` | 配置拉取和服务注册的默认分组。 |
| `app_name` | `""` | Nacos 端来源标识。 |
| `discovery_ip` | `null` | 注册实例对外 IP；只影响注册中心。 |
| `username` | `""` | 开启鉴权时填写。 |
| `password` | `""` | 鉴权密码；不要写入公开仓库。 |

强类型配置和连接代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    nacos: NacosYml,
}

#[derive(serde::Deserialize)]
struct NacosYml {
    server_addr: String,
    #[serde(default)]
    namespace: String,
    #[serde(default = "default_group")]
    group: String,
    #[serde(default)]
    app_name: String,
    #[serde(default)]
    discovery_ip: Option<String>,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
}

fn default_group() -> String {
    "DEFAULT_GROUP".to_string()
}

let props = nasa::config::nacos::NacosProps::new(&cfg.nacos.server_addr)
    .with_namespace(&cfg.nacos.namespace)
    .with_group(&cfg.nacos.group)
    .with_app_name(&cfg.nacos.app_name)
    .with_discovery_ip_opt(cfg.nacos.discovery_ip.as_deref())
    .with_auth(&cfg.nacos.username, &cfg.nacos.password);
```

需要多配置 import、optional、文件格式和热刷新时，优先用 `config-boot` 管理配置形状，`nanacos` 只作为底层传输。
