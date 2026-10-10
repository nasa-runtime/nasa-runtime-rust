# rest-client-macro

`rest-client-macro` 提供声明式 HTTP 客户端宏。`#[rest_client]` 读取 trait，生成 `{Trait}Client` 和
`async_trait` impl；方法上的 `#[GetMapping]`、`#[PostMapping]`、`#[PutMapping]`、
`#[PatchMapping]`、`#[DeleteMapping]` 描述 HTTP 元数据。
内部服务通过 `qualifier` 选择注册中心，默认连接名为 `primary`；互联网 API 使用完整 `url`。
生成客户端共享注入的 HTTP 资源，宏本身不创建连接池、不读取 YML，也不管理注册或关闭。

业务通常从 `nasa::discovery::rest::rest_client` 使用宏，启用 `rest-client` feature。
独立宿主直接集成时，生成代码必须能解析到 `rest-discovery` 运行时，因此同时依赖：

```toml
[dependencies]
rest-client-macro = "2.0.0"
rest-discovery = "2.0.0"
```

若由上层门面重导出，宏也能识别被 Cargo 重命名后的门面路径。`rest-client-macro` 自身不发请求、不持有连接池。

## 多注册中心与 qualifier

`qualifier` 选择具名注册中心连接，`service` 选择该连接内的服务别名。它不指定 provider 类型、
Nacos cluster 或远端服务名。五种 Mapping 都支持此属性；方法级值覆盖 trait 级默认值。

```rust,ignore
#[rest_client(service = "orders", qualifier = "nacos-order")]
trait OrderApi {
    #[GetMapping(path = "/orders/{id}")]
    async fn get(&self, #[PathVariable] id: u64) -> anyhow::Result<OrderDto>;

    #[PostMapping(path = "/orders", qualifier = "consul-order")]
    async fn create(&self, #[RequestBody] body: CreateOrder) -> anyhow::Result<OrderDto>;
}

let api = OrderApiClient::with_clients(app.discovery()?.rest_clients()?);
```

属性值必须是字符串字面量，例如 `qualifier = "nacos-order"`，不能写成 `qualifier = nacos-order`。
名称为 1–64 个 ASCII 字母、数字、`_` 或 `-`，首字符必须是字母或数字；重复声明与非法值在编译期拒绝。
未声明 qualifier 的方法使用名为 `primary` 的连接；显式名称不存在或缺少默认项时，在提交 HTTP 请求前失败。
被选连接权限不足、不可用或服务为空时，继续遵循该连接的失败语义，不回退到其他连接、全局单例或普通 DNS。

独立宿主可用 `RestClientSet::new(clients, None)` 注入
`BTreeMap<String, Arc<RestDiscoveryClient>>`；默认选择 `primary`，即使只有一个其他名称也不猜选。
第二个参数仅供兼容宿主显式默认引用，正常使用传 `None`。集合共享原客户端，
不创建连接池或后台任务，宿主负责关闭。现有 `Client::new(Arc<RestDiscoveryClient>)` 保留，
只满足未声明 qualifier 的方法；单客户端注入不会自动获得任意具名身份。

## service 模式

适合内部服务调用，生成代码会走 `RestDiscoveryClient::service_request(service, method, path)`，不会拼普通 URL。

```rust ignore
use rest_client_macro::{rest_client, GetMapping};

#[rest_client(service = "order-service", context_path = "/api")]
trait OrderApi {
    #[GetMapping("/orders/{id}")]
    async fn get_order(
        &self,
        #[PathVariable("id")] id: i64,
        #[RequestParam("includeItems")] include_items: bool,
    ) -> anyhow::Result<OrderDto>;
}
```

## url 模式

适合完整 HTTP(S) 外部地址。`url` 模式不能配置或继承 qualifier，否则编译失败；它使用注入的默认客户端。
宏调用 `external_request`，即使该客户端开启服务名启发式，也不会改写目标；其他协议在发送前失败。
在 Nacos 环境中调用 Google 等公网 API，不需要把域名注册到 Nacos 或加入服务别名表。

```rust ignore
use nasa::{discovery::rest::rest_client, Application};
use serde_json::Value;

#[rest_client]
pub trait GoogleDnsApi {
    /// 业务作用：通过 Google 公网接口查询 DNS 记录。
    /// 参数说明：domain 为域名；record_type 为 A、AAAA 等记录类型。
    /// 返回：原始 DNS JSON；请求或解析失败时返回错误，DNS 状态由调用方判断。
    #[GetMapping(url = "https://dns.google/resolve", response = "json")]
    async fn resolve(
        &self,
        #[RequestParam("name")] domain: String,
        #[RequestParam("type")] record_type: String,
    ) -> anyhow::Result<Value>;
}

/// 业务作用：在已装配的应用中调用 Google DNS API。
/// 参数说明：app 提供受管 HTTP 客户端。
/// 返回：Google JSON 响应；客户端获取或调用失败时返回错误。
pub async fn query_google(app: &Application) -> anyhow::Result<Value> {
    let api = GoogleDnsApiClient::with_clients(app.discovery()?.rest_clients()?);
    api.resolve("www.google.com".into(), "A".into()).await
}
```

需要门面 `application,web,discovery-nacos,rest-client` 和 `anyhow,serde_json` 依赖，应用使用
`#[nasa::application("web", "discovery")]`；业务在目录已装配后调用。集合注入默认使用 `primary`
的 HTTP 连接池与超时配置，实际目标为 `https://dns.google/resolve?name=www.google.com&type=A`，
不查询 Nacos。没有默认项时失败；已有单客户端可用 `GoogleDnsApiClient::new(selected_client)` 注入。
响应中的 DNS `Status` 与 `Answer` 由业务判断，HTTP 成功不代表 DNS 查询成功。

URL Mapping 也不能同时填写 `service`、`context_path`、`path` 或 `scheme`。
宏不展开 `${...}` 配置占位符；运行期 URL 使用请求构建器。默认按 JSON 解码，普通文本或 HTML
须显式声明 `response = "text"`，不能仅把返回类型改成 `String`。
Bearer 凭据通过 `#[RequestHeader("Authorization")] authorization: String` 在调用时传入。

[完整 Nacos 配置与内外部调用](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md)
· [Google JSON API](https://developers.google.com/speed/public-dns/docs/doh/json)。

## 请求体和表单

```rust ignore
#[rest_client(service = "user-service")]
trait UserApi {
    #[PostMapping(path = "/users", consumes = "application/json")]
    async fn create(&self, #[RequestBody] body: CreateUserReq) -> anyhow::Result<UserDto>;

    #[PostMapping(path = "/login", consumes = "application/x-www-form-urlencoded")]
    async fn login(&self, #[FormBody] form: LoginForm) -> anyhow::Result<TokenDto>;
}
```

## Header 和 QueryMap

```rust ignore
#[GetMapping("/search")]
async fn search(
    &self,
    #[RequestHeader("X-Trace-Id")] trace_id: String,
    #[QueryMap] query: SearchQuery,
) -> anyhow::Result<Vec<ItemDto>>;
```

## 约束

- `#[GetMapping]` 等方法宏只能放在 `#[rest_client]` trait 方法上，由外层宏统一消费。
- 参数属性是 inert attr，由 `#[rest_client]` 解析并从输出删除。
- trait 方法必须是 `async fn`，返回类型为 `anyhow::Result<T>`、`Result<T>` 或 `Result<T, E>`；自定义错误须实现 `From<RestDiscoveryError>`。
- service 模式的逻辑路径必须以 `/` 开头；完整 HTTP(S) 地址只能使用 url 模式。
- `Host` 等连接层 header 不能由声明式接口注入；header 名和值会在宏展开期校验。
- `PathVariable` 必须与模板占位双向一一对应，重复、缺失或未绑定都会在编译期拒绝。

## 属性范围

| 层级 | 支持项 |
| --- | --- |
| trait | `service`、`qualifier`、`context_path`、`scheme`、`client` |
| mapping | `path`/`value`/`remote`、`service`、`qualifier`、`context_path`、`url`、`scheme`、`produces`、`consumes`、`response`、`unwrap`、`headers` |
| 参数 | `PathVariable`、`RequestParam`、`RequestHeader`、`RequestHeaders`、`QueryMap`、`RequestBody`、`RequestBody(raw)`、`FormBody` |

`GET`/`DELETE` 不接受 body；form、JSON 和 raw body 的 `consumes` 必须与绑定方式一致。`unwrap = "data"`
只支持 JSON 对象的单层字段，并且不适用于 `Result<()>`。

## YML 配置与使用

宏属性是编译期常量，宏本身不读取 YML。napp 使用 `discovery.connections` 装配客户端，
默认连接直接命名为 `primary`，无需额外默认配置字段。例如两个独立 Nacos：

```yaml
discovery:
  enabled: true
  connections:
    primary:
      provider: nacos
      nacos:
        server_addr: http://nacos-order:8848/nacos/
        services:
          orders: {name: order-service}
    nacos-order:
      provider: nacos
      nacos:
        server_addr: http://nacos-archive:8848/nacos/
        services:
          orders: {name: archived-order-service}
```

启用 `nasa` 的 `application`、`web`、`discovery-nacos`、`rest-client`，通过
`#[nasa::application("web", "discovery")]` 接管生命周期。宏与客户端集合本身不依赖某个
provider；Consul、Kubernetes、Static、DNS 采用相同选择规则。需要运行期切换服务名或 URL 时，
使用 `RestClientSet::client(Some(qualifier))` 取得客户端，再调用请求构建器。
