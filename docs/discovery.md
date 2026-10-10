# 多注册中心与声明式 HTTP 调用

同一应用可以连接多个 Nacos、Consul 或 Kubernetes 环境，并通过 `qualifier` 精确选择连接。
连接隔离凭据、服务绑定、发现证据和故障；注册清单单独决定当前应用发布到哪些环境。
普通微服务无需启用 Saga 或数据库。互联网 API 使用完整 `url`，无需注册到任何后端。

## 能力与生命周期

通过 `nasa` 启用 `application`、`web`、`rest-client`，并按后端增加 `discovery-nacos`、
`discovery-consul` 或 `discovery-kubernetes`。入口使用 `#[nasa::application("web", "discovery")]`。
示例代码另外使用 `anyhow` 和 `serde_json`。`rest-client` 包含 HTTP 运行时和声明式宏；
只启用 `rest-discovery` 时使用请求构建器，不包含 Mapping 宏。

```text
discovery.connections → 每连接独立 provider → owner 共享发现证据
                                                     ↓
Mapping 的 qualifier → 选定 HTTP 客户端 → service 别名 → 当前实例 → 提交前复验
Mapping 的完整 url   → 默认 HTTP 客户端 → 公网 DNS / HTTPS → 外部 API
```

Application 负责连接准备、注册确认、就绪贡献、摘流、业务排空和最终关闭。内部服务请求在实际
提交处复验实例与证据，不能只缓存 IP 后绕过撤权；已提交请求仍可能在远端执行。
使用 `app.discovery()` 的业务代码应在发现目录完成装配后执行，例如 Ready 后的请求处理函数。

## 两个 Nacos 连接

在进程工作目录的 `zcf/application.yml` 中配置：

```yaml
application:
  name: order-client
server:
  host: 127.0.0.1
  port: 8080
discovery:
  enabled: true
  connections:
    primary:
      provider: nacos
      nacos:
        server_addr: http://nacos-business:8848/nacos/
        namespace: public
        group: DEFAULT_GROUP
        username: ${BUSINESS_NACOS_USERNAME:}
        password: ${BUSINESS_NACOS_PASSWORD:}
        services:
          orders: {name: order-service}
      rest:
        request_timeout_ms: 5000
        connect_timeout_ms: 1000
    nacos-order:
      provider: nacos
      nacos:
        server_addr: http://nacos-order:8848/nacos/
        namespace: public
        group: DEFAULT_GROUP
        username: ${ORDER_NACOS_USERNAME:}
        password: ${ORDER_NACOS_PASSWORD:}
        services:
          orders: {name: archived-order-service}
```

将地址和服务绑定替换为实际环境；开启鉴权时提供成对凭据。受管 Nacos HTTP adapter 要求 Nacos 3。
这里没有 `registrations`，因此只发现和调用，不注册当前应用。需要发布自身时增加独立注册清单，
为各目标设置可达 IP、映射端口和 `qualifier`，见 [完整注册配置](../nadisc-runtime/README.md#napp-自动接管与多-nacos)。

`primary` 是区分大小写的默认连接名。`nacos-order` 是业务自定的 qualifier，与 provider 类型、
Nacos cluster 和服务名不同。`orders` 是所选连接中的服务别名，可以映射到不同远端服务名。

## 内部服务调用

```rust
use nasa::{discovery::rest::rest_client, Application};
use serde_json::Value;

#[rest_client(service = "orders")]
pub trait OrderApi {
    /// 业务作用：通过默认连接查询订单。
    /// 参数说明：id 为订单标识。
    /// 返回：订单 JSON；发现、请求或解码失败时返回错误。
    #[GetMapping(path = "/orders/{id}")]
    async fn get(&self, #[PathVariable("id")] id: u64) -> anyhow::Result<Value>;

    /// 业务作用：通过指定连接查询归档订单。
    /// 参数说明：id 为订单标识。
    /// 返回：归档订单 JSON；目标连接不可用时返回错误。
    #[GetMapping(path = "/orders/{id}", qualifier = "nacos-order")]
    async fn archived(&self, #[PathVariable("id")] id: u64) -> anyhow::Result<Value>;
}

/// 业务作用：在已装配的应用中读取指定环境的归档订单。
/// 参数说明：app 提供受管客户端；id 为订单标识。
/// 返回：归档接口响应；客户端获取或调用失败时返回错误。
pub async fn read_archived(app: &Application, id: u64) -> anyhow::Result<Value> {
    let orders = OrderApiClient::with_clients(app.discovery()?.rest_clients()?);
    orders.archived(id).await
}
```

`GetMapping`、`PostMapping`、`PutMapping`、`PatchMapping`、`DeleteMapping` 均支持 qualifier。
也可在 trait 上写 `#[rest_client(service = "orders", qualifier = "nacos-order")]` 作为默认值，
方法上的 qualifier 优先。属性值必须带双引号，不能写成 `qualifier = nacos-order`。

未知名称在提交前失败；已选连接无实例、失权或不可用时，不切换到另一环境或普通 DNS。
没有 `primary` 的目录可以显式具名调用，但未选择 qualifier 的调用失败，不按连接数量或排序猜选。

## Nacos 环境中的互联网 API

下面调用 Google Public DNS 的 [JSON API](https://developers.google.com/speed/public-dns/docs/doh/json)。
不需要把 `dns.google` 加到 Nacos 或上述 `services` 中。

```rust
#[rest_client]
pub trait GoogleDnsApi {
    /// 业务作用：通过 Google 公网接口查询 DNS 记录。
    /// 参数说明：domain 为域名；record_type 为 A、AAAA 等记录类型。
    /// 返回：原始 DNS JSON；请求或解码失败时返回错误，DNS 状态由调用方判断。
    #[GetMapping(url = "https://dns.google/resolve", response = "json")]
    async fn resolve(
        &self,
        #[RequestParam("name")] domain: String,
        #[RequestParam("type")] record_type: String,
    ) -> anyhow::Result<Value>;
}

/// 业务作用：在已装配的应用中查询 Google 域名的 A 记录。
/// 参数说明：app 提供受管 HTTP 客户端。
/// 返回：Google JSON 响应；客户端获取或调用失败时返回错误。
pub async fn query_google(app: &Application) -> anyhow::Result<Value> {
    let google = GoogleDnsApiClient::with_clients(app.discovery()?.rest_clients()?);
    google.resolve("www.google.com".into(), "A".into()).await
}
```

该代码接在前一个 Rust 片段的导入之后。实际请求为
`GET https://dns.google/resolve?name=www.google.com&type=A`；HTTP 成功不等于 DNS 查询成功，
业务还需判断响应的 `Status` 和 `Answer`。

URL 模式通过 `external_request` 复用默认 `primary` HTTP 客户端的连接池与超时策略，不读取 Nacos 实例列表。
即使注入的客户端开启服务名启发式，也保持原 HTTP(S) 目标；其他协议在发送前失败。
它不能配置或继承 qualifier，也不能在同一 Mapping 中配置 `service`、`path`、`context_path`
或 `scheme`。需要访问不同环境的内部服务时使用 service 模式；外部接口建议放在独立 trait 中。
没有默认项时，集合注入的 URL 调用同样失败；也可用 `GoogleDnsApiClient::new(selected_client)`
显式注入一个已有 HTTP 客户端，该注入方式只支持没有 qualifier 的方法。

宏不读取 YML，`url = "${google.url}"` 不会替换配置。需要运行期地址时，从应用取得客户端后使用
`.external_request(Method::GET, configured_url)`。JSON 接口使用默认 `response = "json"`；普通文本或 HTML
须显式设置 `response = "text"`，`Result<String>` 本身不会切换响应格式。Bearer 凭据可通过
`#[RequestHeader("Authorization")] authorization: String` 在调用时传入，不写入宏常量。

外部请求仍受客户端关闭和请求期限限制。它不查询 Nacos，并不意味着应用启动不需要 Nacos：
配置连接的 `startup_required` 默认 true，必需连接准备失败仍会阻止应用启动。

## 兼容、观测与运维

标准配置使用 `primary` 和 `qualifier`。旧 `default_connection` 仅作为显式默认引用兼容，优先于
`primary`；它不会改变显式 qualifier。注册目标和 Saga 目标接受旧 `connection_ref` 别名，
但不能与 `qualifier` 同时声明。注册目标省略 qualifier 时固定为 `primary`，Saga 省略时跟随
owner 的默认选择。旧单客户端 `Client::new` 不会自动获得任意具名身份。

旧单 Nacos 使用 `nacos-discovery` 组件及 `rest_discovery` 配置根；多连接使用 `discovery`。
新旧配置根不能并存，即使一项 disabled。连接、注册和调用策略冻结到启动；napp 收到包含
`discovery` 变化的热更新时拒绝整份候选，保留原配置与运行连接。

通过 owner observer、注册状态及 `napp_discovery_*` 指标观察当前证据与容量。准备成功不等于
业务实例健康；完整空集、撤权和关闭立即撤销旧路由，网络失败只能使用显式有限宽限。
`OutcomeUnknown`、`Residual` 和关闭预算耗尽需要运维核对，不能用注销 ACK 证明迟到写入不会生效。

更多合同见 [公共运行时](../nadisc-runtime/README.md)、[REST 宏](../rest-client-macro/README.md)、
[REST 传输](../rest-discovery/README.md) 和 [动态 gRPC](../nagrpc/README.md#动态发现调用器)。
