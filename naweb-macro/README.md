# naweb-macro

`naweb-macro` 提供服务端 MVC 风格路由宏：`mvc_router!`、五个 `#[*_mapping]` 路由宏，以及
`#[interceptor]`。路由宏不改写 handler，只通过 `linkme` 在编译期收集路由注册项，启动时一次性
装配到 axum `Router`；interceptor 宏生成类型化 marker 和 binding helper，只有显式
`global = true` 时才额外进入自动全局收集表，全程不做字符串反射分发。

业务通常通过 `nasa::web` 使用；只有底层框架集成层才直接依赖实现 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["web"] }
```

## 生成收集器

每个应用 crate 根只调用一次 `mvc_router!(StateType)`。

```rust
nasa::web::mvc_router!(crate::AppState);

#[derive(Clone)]
pub struct AppState {
    pub name: String,
}
```

无状态服务写 `()`：

```rust
nasa::web::mvc_router!(());
```

## GET 路由

```rust
use axum::Json;
use nasa::web::get_mapping;

#[get_mapping("/api/ping")]
async fn ping() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}
```

## POST 路由

`post_mapping` 默认要求 `Content-Type` 以 `application/json` 开头。接收表单或其它媒体类型时要显式写 `consumes`。

```rust
use axum::Json;
use nasa::web::post_mapping;

#[post_mapping(path = "/api/order", produces = "application/json")]
async fn create_order(Json(req): Json<serde_json::Value>) -> Json<serde_json::Value> {
    Json(req)
}

#[post_mapping(path = "/api/form", consumes = "application/x-www-form-urlencoded")]
async fn submit_form() {}
```

## PUT / PATCH / DELETE 路由

这些宏参数与 `get_mapping` 一致，不默认 `consumes`。

```rust
use nasa::web::{delete_mapping, patch_mapping, put_mapping};

#[put_mapping("/api/order/{id}")]
async fn replace_order() {}

#[patch_mapping(path = "/api/order/{id}", consumes = "application/json")]
async fn patch_order() {}

#[delete_mapping("/api/order/{id}")]
async fn delete_order() {}
```

## 装配路由

```rust
use axum::Router;

fn app(state: crate::AppState) -> Router {
    Router::new()
        .merge(crate::__mvc::register_all(Router::new()))
        .with_state(state)
}
```

`produces` 会通过中间件设置响应 `Content-Type`，`consumes` 会在进入 handler 前校验请求 `Content-Type`，不符合时返回 415。

## Interceptor

完整属性如下：

| 属性 | 必填 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `id = "..."` | 是 | 无 | 1..=64 字节安全 ASCII ID；同一路由 effective plan 内必须唯一，推荐应用内唯一 |
| `kind = "..."` | 否 | `"edge"` | 固定阶段：`edge`、`auth` 或 `plaintext` |
| `order = ...` | 否 | `0` | 同阶段、同作用域内数值越小越先执行 |
| `before = "a,b"` | 否 | 空 | 排在同阶段、同作用域的指定 ID 之前 |
| `after = "a,b"` | 否 | 空 | 排在同阶段、同作用域的指定 ID 之后 |
| `auth_runtime = true` | 否 | `false` | 调用共享 `AuthRuntime`；只能用于 `kind = "auth"` |
| `global = true` | 否 | `false` | 自动覆盖当前 crate 中符合阶段合同的全部 `*_mapping` 端点 |

被标注项必须是非泛型 `async fn`。最后两个函数参数固定为 `Request, Next`；它们之前可以声明
`State<T>`、`Extension<T>`、`InterceptorContext` 等 `FromRequestParts` extractor。`Json`、`Form`、
另一份 `Request` 等 Body extractor 会在编译期被拒绝，避免抢先消费尚未解密的请求体。

### 默认行为：只声明，手动装配

`global` 缺省或显式写成 `false` 时，宏不会让 interceptor 自动执行，只生成 descriptor 和
`binding`/`binding_with` helper。下面保留了精确端点装配和手动全局装配两种独立用法：

```rust
use nasa::web::{get_mapping, interceptor, InterceptorContext};
use nasa::web::{Next, Request, Response, State};

#[interceptor(id = "token", kind = "auth", order = 100)]
async fn token(
    State(state): State<AppState>,
    context: InterceptorContext,
    request: Request,
    next: Next,
) -> Response {
    authenticate(state, context, request, next).await
}

#[get_mapping(path = "/profile", auth = "required", interceptors(token))]
async fn profile() {}
```

上面的 `token` 只在声明了 `interceptors(token)` 的路由执行。如果需要由启动代码手动覆盖全部
mapping 端点，则不要改宏属性，改为把 binding 放进计划：

```rust
#[interceptor(id = "request-audit", kind = "edge", order = 10)]
async fn request_audit(request: Request, next: Next) -> Response {
    audit(request, next).await
}

app.configure_mapping(|plan| Ok(plan.global(request_audit::binding())))?;
```

还可使用 `MappingPlan::scope` 覆盖一个静态路径层级，或使用 `when_route`/selector 做条件选择。
需要 `binding_with::<RootState>(narrow_state)`、动态配置开关或业务启动期构造 State 时，也必须使用
这种手动计划。

### `global = true`：自动全局装配

显式设置后，宏把 binding 注册到当前 crate 由 `mvc_router!` 生成的 `GLOBAL_INTERCEPTORS`；
`try_register_all` 会在路由审计前自动合并，`main` 不再登记它：

```rust
#[interceptor(id = "automatic-audit", kind = "edge", order = 10, global = true)]
async fn automatic_audit(request: Request, next: Next) -> Response {
    audit(request, next).await
}

// 不要再调用 plan.global(automatic_audit::binding())。
```

自动全局只覆盖当前 crate 的 `*_mapping` 端点，不覆盖 `configure_router`/Axum 手写路由、`/healthz`、
`/readyz` 等框架探针。它支持无 State 或与 Router 根 State 相同的 `State<T>`；窄 State、scope、
`when_route` 和动态开关继续手动装配。相同 ID 同时自动和手动装配时，effective-plan 审计会明确拒绝
启动，不会静默去重或执行两次。自动项按 `stage + order + id + handler` 排序，不依赖 linkme 链接顺序。
`kind = "auth"` 的自动项只进入 `auth = "required|optional"` 路由；public 或未声明 auth 的路由会排除
全局/scope auth，避免全局声明偷偷改变公开端点的身份合同。

带 `State<T>` 的声明生成 `binding()` 和 `binding_with::<RootState>(T)`；无 State 声明生成泛型
`binding::<RootState>()`。自动 global、端点 `interceptors(...)`、`MappingPlan::global` 和
`MappingPlan::scope` 最终进入同一个 effective plan。阶段顺序由 naweb 固定，任何 order 或依赖
声明都不能让 auth 跑到 request decrypt 之后。

带安全元数据或 interceptor 的应用必须使用可失败入口：

```rust
let plan = naweb::MappingPlan::new();
let router = crate::__mvc::try_register_all(
    axum::Router::new(),
    plan.runtime_or_default(),
    plan,
    state.clone(),
)?;
```

纯普通路由仍可使用兼容的 `register_all`；它遇到安全路由、端点 interceptor 或自动 global
interceptor 会拒绝启动。

## YML 配置与使用

`naweb-macro` 没有运行期 yml。它只读取 Rust 属性宏，编译期注册路由。应用级监听、context path、
body limit 和跨域由业务 yml 与 Axum 装配负责；鉴权业务由应用 interceptor 实现，naweb 负责阶段编排。

推荐配置：

```yaml
server:
  addr: 0.0.0.0:8080
  context_path: /order
  cors:
    enabled: true
  body_limit_bytes: 10485760
```

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| 路由 path、HTTP method | `#[get_mapping]` 等属性 |
| `produces` / `consumes` | 路由属性 |
| 监听地址、context path | 应用 yml |
| CORS、body limit | 应用 yml + axum layer |
| Token、Session、业务白名单 | 应用 `#[interceptor(kind = "auth")]` |

示例：

```rust
let router = crate::__mvc::register_all(axum::Router::new());
let app = axum::Router::new().nest(&cfg.server.context_path, router);
```
