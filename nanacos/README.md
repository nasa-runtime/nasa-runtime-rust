# nanacos

`nanacos` 是 Nacos 配置中心和注册中心传输门面。它只提供连接、鉴权、配置拉取、配置 watch、服务注册、发现和订阅能力，不认识业务 `AppConfig`，也不做配置合并。

业务项目通过门面开启真实传输：

```toml
[dependencies]
nasa = { version = "1", features = ["nacos-sdk"] }
```

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
