# naopenapi

`naopenapi` 从已经通过启动审计的静态路由事实生成排序稳定的 OpenAPI 3.1 JSON。它不会扫描 handler
源码，也不会猜测 DTO；请求和响应 schema 必须由业务显式提供。

业务通过门面开启 `openapi`，通常同时开启 `application` 和 `web`：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "openapi", "web"] }
```

```rust
#[nasa::web::post_mapping(
    path = "/orders",
    consumes = "application/json",
    produces = "application/json",
    request_schema = CreateOrder,
    response_schema = OrderView,
    query_parameters = OrderQueryParameters,
    header_parameters = OrderHeaderParameters,
    success_status = 201,
    responses = OrderResponses
)]
async fn create_order() -> impl axum::response::IntoResponse {
    // ...
}
```

参数集实现 `ApiParameters`，额外响应集实现 `ApiResponses`。框架只传递这些显式静态描述，不从
extractor 或返回类型猜测 query/header、状态码和错误响应。流式端点另声明
`streaming = true`，并必须同时给出 `produces`。

业务 DTO 通过 `ApiSchema` 提供稳定 component name 和 JSON Schema。Web Ready 后可从
`Application::openapi_document` 取得最终文档；它包含 context path、路径参数、媒体类型、
query/header 参数、成功与额外响应、streaming 标记、Problem 响应和 Bearer security。

## 直接生成

框架集成方也可调用 `nasa::openapi::generate(title, version, routes)`。输入 route 必须是已经冻结的
`RouteContract`，不能混入运行时动态路由猜测。

## YML 配置

生成器本身不读取 yml。文档标题和版本由应用启动配置或代码提供；路由合同来自 mapping 元数据。

```yaml
application:
  name: order-service
  version: 1.4.0
```

## 主要边界

- method/path、operation ID、媒体类型和 schema name 非法时拒绝生成。
- 同一 method/path 或 operation ID 重复会失败，不做覆盖。
- 同名 schema 只有结构完全一致才能复用；异构同名会失败。
- schema 文本有 1 MiB 硬上限；请求 schema 必须同时声明 `consumes`。
- header 参数名遵守 HTTP token；204、205 和 304 响应不能声明 body。
- 手工 `configure_router` 注入的不透明动态路由无法自动进入文档。
