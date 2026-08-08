# rest-client-macro

`rest-client-macro` 提供声明式 HTTP 客户端宏。`#[rest_client]` 读取 trait，生成 `{Trait}Client` 和
`async_trait` impl；方法上的 `#[GetMapping]`、`#[PostMapping]`、`#[PutMapping]`、
`#[PatchMapping]`、`#[DeleteMapping]` 描述 HTTP 元数据。

本包只提供编译期展开；生成代码必须能解析到 `rest-discovery` 运行时。应用直接集成时同时依赖：

```toml
[dependencies]
rest-client-macro = "1"
rest-discovery = "1"
```

若由上层门面重导出，宏也能识别被 Cargo 重命名后的门面路径。`rest-client-macro` 自身不发请求、不持有连接池。

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

适合外部 HTTP 地址。

```rust ignore
#[rest_client]
trait ExternalApi {
    #[GetMapping(url = "https://api.example.com/v1/health")]
    async fn health(&self) -> anyhow::Result<String>;
}
```

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
- trait 方法必须是 `async fn`，返回类型必须是单参数别名形状的 `anyhow::Result<T>` 或 `Result<T>`。
- service 模式的逻辑路径必须以 `/` 开头；完整 HTTP(S) 地址只能使用 url 模式。
- `Host` 等连接层 header 不能由声明式接口注入；header 名和值会在宏展开期校验。
- `PathVariable` 必须与模板占位双向一一对应，重复、缺失或未绑定都会在编译期拒绝。

## 属性范围

| 层级 | 支持项 |
| --- | --- |
| trait | `service`、`context_path`、`scheme`、`client` |
| mapping | `path`/`value`/`remote`、`service`、`context_path`、`url`、`scheme`、`produces`、`consumes`、`response`、`unwrap`、`headers` |
| 参数 | `PathVariable`、`RequestParam`、`RequestHeader`、`RequestHeaders`、`QueryMap`、`RequestBody`、`RequestBody(raw)`、`FormBody` |

`GET`/`DELETE` 不接受 body；form、JSON 和 raw body 的 `consumes` 必须与绑定方式一致。`unwrap = "data"`
只支持 JSON 对象的单层字段，并且不适用于 `Result<()>`。

## YML 配置与使用

`rest-client-macro` 本身不读取 yml。内部服务地址来自 `rest-discovery` 初始化，外部 URL 可以写在属性里，也可以由业务配置选择不同 client。

推荐配置：

```yaml
rest_clients:
  order:
    service: order-service
    context_path: /api
    timeout_ms: 3000
  external_price:
    base_url: https://price.example.com
    timeout_ms: 5000
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `service` | 内部服务名；对应 `#[rest_client(service = "...")]`。 |
| `context_path` | trait 级路径前缀；对应 `context_path` 属性。 |
| `base_url` | 外部 HTTP 地址；可用于选择不同 url 模式 client。 |
| `timeout_ms` | 请求超时；由调用方在生成 client 或请求 builder 上设置。 |

典型启动顺序：

```rust
use rest_discovery::RestDiscovery;

RestDiscovery::init_with_discovery(discovery, opts).await?;
let api = OrderApiClient::new(RestDiscovery::get());
```

宏属性仍是编译期常量。需要运行期切换服务名或 URL 时，应在业务层封装多个 client 或直接使用 `RestDiscoveryClient` 请求构建器。
