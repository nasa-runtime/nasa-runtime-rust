# nafana

`nafana` 为 Axum 接口提供监控、并发隔离、超时和降级能力，并通过 Prometheus 指标与 Grafana
接口墙展示运行状态。它不依赖 Grafana 第三方插件，也不输出 SSE。

## 1. 能力与边界

- `#[grafana]`：按 handler 监控请求，可选并发上限、超时、TPS 权重和降级响应。
- `Command`：保护任意异步执行体，不要求 handler 使用 mapping 宏。
- 进程级全局降级：统一处理未声明局部策略的并发拒绝和执行超时。
- 配置驱动隔离：通过 `IsolationRule`、`init_isolation` 和 `dispatch` 按路由模式保护接口。
- `/metrics`：Prometheus 文本指标入口。
- Grafana Dashboard：直接查询 Prometheus；选择多个实例时，接口折线、错误率、结果、并发和延迟均按集群聚合。

`nafana` 当前不实现 circuit breaker。Dashboard 中的 `Protecting` 只表示最近十秒出现了拒绝、超时或
执行取消，不是熔断器的 Open 状态。

## 2. 依赖与业务路由

业务项目推荐只依赖 `nasa` 门面。使用本仓 mapping 宏时同时开启 `web`：

```toml
[dependencies]
nasa = { version = "1.0.0", features = ["grafana", "web"] }
```

只使用原生 Axum 路由时不需要 `web` feature：

```toml
[dependencies]
nasa = { version = "1.0.0", features = ["grafana"] }
axum = "0.8"
```

内部迁移或明确不使用门面时也可以直接依赖运行时：

```toml
[dependencies]
nafana = "1.0.0"
axum = "0.8"
```

使用 `nasa` 门面时从 `nasa::grafana` 导入；直接依赖时把路径改为 `nafana`：

```rust
use nasa::grafana::grafana;
```

### 2.1 纯宏接入

纯 `#[grafana]` handler 只需暴露指标入口，不要求挂 `dispatch`：

```rust
use axum::{routing::get, Router};

let api = Router::new()
    .route("/orders/{id}", get(get_order))
    .route("/metrics", get(nasa::grafana::metrics));
```

### 2.2 带 context path

如果服务统一挂在 `/order-service` 下，应把业务接口和指标入口放进同一个被嵌套路由：

```rust
use axum::{routing::get, Router};

let service = Router::new()
    .route("/orders/{id}", get(get_order))
    .route("/metrics", get(nasa::grafana::metrics));

let app = Router::new().nest("/order-service", service);
```

最终指标地址为 `/order-service/metrics`。

## 3. `#[grafana]` 完整参数

宏只能用于 `async fn`。所有参数都可省略。

| 参数 | 类型 | 省略时 | 作用 |
|---|---|---|---|
| `name` | 字符串 | mapping path；没有 mapping 时为函数名 | Dashboard 卡片名和 `command` 标签 |
| `max_concurrent` | 非负整数 | 不限并发 | 同时执行的最大请求数；满时立即拒绝，不排队 |
| `timeout_ms` | 非负整数 | 不设超时 | handler 执行超时毫秒数 |
| `tps` | 非负整数 | 不计入 TPS | 每次请求对 `nafana_tps_total` 增加的权重 |
| `reject_response` | JSON 字符串 | 默认 429 JSON | 并发被拒时返回自定义 JSON，HTTP 状态固定为 200 |
| `reject_fn` | 无参异步函数路径 | 默认 429 JSON | 并发被拒时调用函数，由函数决定 HTTP 状态和 body |
| `timeout_response` | JSON 字符串 | 默认 504 JSON | 超时时返回自定义 JSON，HTTP 状态固定为 200 |
| `timeout_fn` | 无参异步函数路径 | 默认 504 JSON | 超时时调用函数，由函数决定 HTTP 状态和 body |

规则：

- `max_concurrent = 0` 等同于不限制并发。
- `timeout_ms = 0` 等同于不设置超时。
- 显式 `Command` 的并发数和 `Duration` 超出运行时范围时分别收敛到 Tokio semaphore 上限和
  365 天，避免构造或 deadline 运算 panic。
- `tps = 0` 表示显式参与 TPS 语义但权重为零；通常应使用 `tps = 1` 或直接省略。
- `reject_response` 与 `reject_fn` 互斥。
- `timeout_response` 与 `timeout_fn` 互斥。
- 自定义 JSON 在编译期校验；非法 JSON 会导致编译失败。
- 同一进程内 `name` 应唯一。重复名称会复用第一次注册的命令实例；后注册端点的参数、真实路径、静态响应
  和降级函数都不会覆盖首次值。宏路径发现同名时会输出警告，但不能替业务选择正确端点名。
- 不同模块里同名函数若都省略 `name` 且没有 mapping，也会冲突；此时必须显式配置不同的 `name`。
- `name` 会成为 Prometheus `command` 标签和 Dashboard 接口筛选值。应使用稳定、低基数的名称，禁止把订单号、
  用户 ID 等运行时值放入名称。

## 4. `#[grafana]` 全部使用场景

下面覆盖每个独立参数、两类降级方式、全部合法的降级组合、mapping、原生 Axum 和编译失败场景。
除两组互斥项外，`name`、`max_concurrent`、`timeout_ms`、`tps` 与两条降级分支可以任意组合；
不需要为每一种数值排列分别定义新语义。

### 4.1 空参数：只监控

不限制并发、不设置超时、不计 TPS，只记录请求结局、并发和延迟：

```rust
use nasa::grafana::grafana;
use nasa::web::get_mapping;

#[grafana]
#[get_mapping("/orders/{id}")]
async fn get_order() -> &'static str {
    "ok"
}
```

卡片名默认为 `/orders/{id}`。

### 4.2 `name`：自定义卡片名

`name` 只改变展示名和 `command` 标签；mapping path 仍写入 `nafana_command_info{path=...}`：

```rust
#[grafana(name = "order-query")]
#[get_mapping("/orders/{id}")]
async fn get_order() -> &'static str {
    "ok"
}
```

### 4.3 `max_concurrent`：只做并发隔离

第 17 个并发请求会被立即拒绝，默认返回 HTTP 429：

```rust
#[grafana(max_concurrent = 16)]
#[get_mapping("/inventory/snapshot")]
async fn inventory_snapshot() -> &'static str {
    "ok"
}
```

### 4.4 `timeout_ms`：只做超时保护

执行超过 800ms 时取消 handler future，默认返回 HTTP 504：

```rust
#[grafana(timeout_ms = 800)]
#[get_mapping("/reports/daily")]
async fn daily_report() -> &'static str {
    "ok"
}
```

### 4.5 `tps`：只计 TPS

每个请求给 TPS counter 增加 1；它仍然同时计入普通 QPS：

```rust
#[grafana(tps = 1)]
#[get_mapping("/payments/confirm")]
async fn confirm_payment() -> &'static str {
    "ok"
}
```

权重大于 1 时，每个请求按给定权重累加：

```rust
#[grafana(tps = 5)]
#[get_mapping("/batch/settle")]
async fn settle_batch() -> &'static str {
    "ok"
}
```

没有业务 TPS 含义的接口不要配置 `tps`。

### 4.6 显式零值

以下写法仍然只监控；并发和超时关闭，TPS 增量为零：

```rust
#[grafana(max_concurrent = 0, timeout_ms = 0, tps = 0)]
#[get_mapping("/diagnostics")]
async fn diagnostics() -> &'static str {
    "ok"
}
```

### 4.7 `reject_response`：静态拒绝 JSON

静态响应适合业务通过 body 中的业务码表示繁忙的场景。HTTP 状态固定为 200：

```rust
#[grafana(
    max_concurrent = 16,
    reject_response = r#"{"code":42901,"message":"service busy","data":null}"#
)]
#[get_mapping("/search")]
async fn search() -> &'static str {
    "ok"
}
```

不配置 `max_concurrent` 时永远不会触发拒绝分支，因此单独配置 `reject_response` 没有实际效果。

### 4.8 `timeout_response`：静态超时 JSON

静态响应的 HTTP 状态同样固定为 200：

```rust
#[grafana(
    timeout_ms = 800,
    timeout_response = r#"{"code":50401,"message":"request timed out","data":null}"#
)]
#[get_mapping("/recommendations")]
async fn recommendations() -> &'static str {
    "ok"
}
```

不配置 `timeout_ms` 时不会触发超时分支。

### 4.9 `reject_fn`：动态拒绝降级

降级函数必须是无参数 `async fn`，返回值必须实现 `IntoResponse`。这种方式可以自定义 HTTP 状态：

下面示例使用 `serde_json::json!`，业务项目需有 `serde_json = "1"` 依赖。

```rust
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

async fn busy_fallback() -> impl IntoResponse {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({"code": 42901, "message": "service busy"})),
    )
}

#[grafana(max_concurrent = 16, reject_fn = busy_fallback)]
#[get_mapping("/search")]
async fn search() -> &'static str {
    "ok"
}
```

也可以使用模块路径：

```rust
#[grafana(max_concurrent = 16, reject_fn = crate::fallback::busy)]
```

### 4.10 `timeout_fn`：动态超时降级

```rust
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

async fn timeout_fallback() -> impl IntoResponse {
    (
        StatusCode::GATEWAY_TIMEOUT,
        Json(json!({"code": 50401, "message": "upstream timed out"})),
    )
}

#[grafana(timeout_ms = 800, timeout_fn = timeout_fallback)]
#[get_mapping("/recommendations")]
async fn recommendations() -> &'static str {
    "ok"
}
```

### 4.11 同时配置拒绝与超时降级

两条分支可以同时使用函数：

```rust
#[grafana(
    max_concurrent = 32,
    timeout_ms = 1200,
    reject_fn = busy_fallback,
    timeout_fn = timeout_fallback
)]
#[get_mapping("/checkout")]
async fn checkout() -> &'static str {
    "ok"
}
```

也可以同时使用两个静态 JSON：

```rust
#[grafana(
    max_concurrent = 32,
    timeout_ms = 1200,
    reject_response = r#"{"code":42901,"message":"busy"}"#,
    timeout_response = r#"{"code":50401,"message":"timeout"}"#
)]
#[get_mapping("/checkout")]
async fn checkout() -> &'static str {
    "ok"
}
```

还可以一条分支使用静态 JSON，另一条分支使用函数。下面两种方向都合法：

```rust
#[grafana(
    max_concurrent = 32,
    timeout_ms = 1200,
    reject_response = r#"{"code":42901,"message":"busy"}"#,
    timeout_fn = timeout_fallback
)]
#[get_mapping("/mixed/static-reject")]
async fn static_reject_dynamic_timeout() -> &'static str {
    "ok"
}

#[grafana(
    max_concurrent = 32,
    timeout_ms = 1200,
    reject_fn = busy_fallback,
    timeout_response = r#"{"code":50401,"message":"timeout"}"#
)]
#[get_mapping("/mixed/dynamic-reject")]
async fn dynamic_reject_static_timeout() -> &'static str {
    "ok"
}
```

### 4.12 完整组合

```rust
#[grafana(
    name = "checkout-submit",
    max_concurrent = 32,
    timeout_ms = 1200,
    tps = 1,
    reject_fn = busy_fallback,
    timeout_fn = timeout_fallback
)]
#[post_mapping(path = "/checkout", consumes = "application/json")]
async fn checkout() -> &'static str {
    "ok"
}
```

执行顺序为：尝试获取并发许可 → 执行并统计 inflight → 应用超时 → 分类成功、失败或超时 → 必要时执行降级。
被拒请求不执行 handler，但会计入 rejected、fallback 和配置过的 TPS。

### 4.13 与所有 mapping 宏组合

`#[grafana]` 支持 `get_mapping`、`post_mapping`、`put_mapping`、`delete_mapping` 和
`patch_mapping`。它必须放在 mapping 宏上方，才能读取路由作为默认卡片名：

```rust
use nasa::web::{
    delete_mapping, get_mapping, patch_mapping, post_mapping, put_mapping,
};

#[grafana]
#[get_mapping("/items/{id}")]
async fn get_item() -> &'static str { "ok" }

#[grafana]
#[post_mapping(path = "/items", consumes = "application/json")]
async fn create_item() -> &'static str { "ok" }

#[grafana]
#[put_mapping("/items/{id}")]
async fn replace_item() -> &'static str { "ok" }

#[grafana]
#[patch_mapping("/items/{id}")]
async fn patch_item() -> &'static str { "ok" }

#[grafana]
#[delete_mapping("/items/{id}")]
async fn delete_item() -> &'static str { "ok" }
```

不要交换顺序。mapping 宏放在外层时会在编译期报错，因为它会先消费路由属性，导致 `#[grafana]` 无法读取
真实路径：

```rust
// 编译失败：请把 #[grafana] 移到 #[get_mapping] 上方。
#[get_mapping("/items/{id}")]
#[grafana]
async fn get_item() -> &'static str { "ok" }
```

### 4.14 原生 Axum handler，不使用 mapping

没有 mapping 属性时，默认卡片名为函数名：

```rust
use axum::{routing::get, Router};

#[grafana]
async fn health() -> &'static str {
    "ok"
}

let app = Router::new().route("/health", get(health));
```

此时 `command="health"`，组件无法从原生 `Router::route` 调用中自动获取 `/health`。建议显式设置名称：

```rust
#[grafana(name = "/health")]
async fn health() -> &'static str {
    "ok"
}
```

### 4.15 带 extractor、状态和不同响应类型

宏保留原 handler 的参数，并把任何实现 `IntoResponse` 的返回值转换为 Axum `Response`：

```rust
use axum::{extract::{Path, State}, Json};
use serde_json::{json, Value};
use std::sync::Arc;

#[grafana(timeout_ms = 500, tps = 1)]
#[get_mapping("/users/{id}")]
async fn get_user(
    State(_state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Json<Value> {
    Json(json!({"id": id}))
}
```

返回具体 `Result<T, E>` 的 handler 也受支持，只要 `T` 和 `E` 组成的 `Result` 实现 `IntoResponse`。
这包括在函数体内使用 `?` 的常见写法：

```rust
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug)]
struct AppError;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

#[grafana]
#[get_mapping("/users/{id}")]
async fn get_user(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<Value>, AppError> {
    let user = state.load_user(id).await?;
    Ok(Json(user))
}
```

返回 `impl IntoResponse` 仍按原返回类型正常编译。

### 4.16 与其它执行监控属性叠加

`#[grafana]` 不识别、不禁止也不合并其它执行监控属性。同一 `async fn` 主动叠加两种监控属性时，
两层包装各自执行并生成两套独立指标；属性顺序决定哪一层在外。业务应确认双重计数、双重超时或双重并发
限制正是预期结果。`nafana` 只阻止同一端点重复标注 `#[grafana]`。

叠加时两层的**结局可能不一致**：外层超时会丢弃内层的执行 future，于是同一次请求在外层记 `timeout`、
在内层记 `canceled`；外层并发拒绝时内层则完全没有记录。排查双层端点时应以外层结局为准，内层的
`canceled` 只说明"它是被上面那层掐掉的"。

### 4.17 不支持和编译失败的写法

以下情况都会在编译期报错：

- 把宏用于非 `async fn`。
- 同一端点重复标注 `#[grafana]`。
- 把 `#[*_mapping]` 写在 `#[grafana]` 上方（顺序写反，见 4.13）。
- 同时配置 `reject_response` 与 `reject_fn`。
- 同时配置 `timeout_response` 与 `timeout_fn`。
- 静态响应不是合法 JSON。
- 使用参数表之外的参数名，或为整数参数提供负数、浮点数、字符串等错误类型。

错误示例：

```rust,compile_fail
#[grafana(
    max_concurrent = 16,
    reject_response = r#"{"message":"busy"}"#,
    reject_fn = busy_fallback
)]
async fn invalid() -> &'static str {
    "never compiled"
}
```

### 4.18 进程级全局降级

大量端点采用同一种业务降级协议时，可以声明一个 Nafana 全局终态处理器。推荐用
`#[nasa::grafana::global_fallback]` 自动收集唯一入口，无需在启动函数手动注册；它与其它隔离组件不共享状态。

```rust
use axum::{http::StatusCode, response::IntoResponse, Json};
use nasa::grafana::{FallbackCause, FallbackContext};

#[nasa::grafana::global_fallback]
fn service_fallback(context: FallbackContext) -> impl IntoResponse {
    let cause = match context.cause() {
        FallbackCause::BulkheadRejected { .. } => "busy",
        FallbackCause::ExecutionTimeout { .. } => "timeout",
        _ => "unavailable",
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "code": "SERVICE_DEGRADED",
            "cause": cause,
            "command": context.command(),
            "path": context.path(),
            "transaction_weight": context.transaction_weight(),
        })),
    )
}
```

`#[global_fallback]` 不接受参数，只能标注恰好接收一个 `FallbackContext` 的同步函数，返回类型可以是任意
Axum `IntoResponse`。它是故障路径的终态响应生成器，应只做本地、确定性、常量时间的响应组装，不应阻塞
线程或访问数据库、缓存、RPC 等外部资源。Nafana 不会给它再配置 `max_concurrent` 或 `timeout_ms`，也不会
在它失败后调用第二个业务降级；配置冲突、panic 或递归只会收敛到内置 429/504。

固定优先级为：端点局部函数 → 端点局部静态响应 → Nafana 全局处理器 → 内置 429/504。同一组件只能
收集一个属性入口；首次需要时自动初始化。希望在开放流量前检查唯一性时，可调用
`initialize_global_fallback()`。没有使用属性宏时，仍可实现同步 `GlobalFallbackHandler` 并调用
`install_global_fallback(Arc<_>)`；手动实现可返回 `FallbackDecision::UseBuiltin`，但不能覆盖自动收集项。

`tps` 表示 REST 事务权重。主请求在进入 Nafana 时已经按权重记账一次，全局降级只读取
`FallbackContext::transaction_weight()`，不会重复增加 TPS。`nafana_global_fallback_total` 按
`handled`、`builtin`、`failed` 区分全局处理结局；终态处理器不暴露第二层并发容量。

## 5. 显式 `Command`

不使用属性宏时，可用 `Command` 保护任意返回 Axum `Response` 的异步执行体。下面示例同时设置并发、超时、
TPS 权重、真实路径、日志附加信息和动态超时降级：

```rust
use axum::{http::StatusCode, response::IntoResponse};
use nasa::grafana::{Command, FallbackFn};
use std::time::Duration;

let command = Command::with_tps(
    "report-generate",
    "reporting",
    8,
    Duration::from_millis(800),
    1,
);
command.set_path("/reports/generate");
command.set_extra(|period_ms| format!("period_ms={period_ms}"));

let timeout_fallback: FallbackFn = Box::new(|| {
    Box::pin(async { StatusCode::GATEWAY_TIMEOUT.into_response() })
});
command.set_timeout_fn(timeout_fallback);

let response = command
    .clone()
    .run_fn(|| async {
        generate_report().await;
        StatusCode::OK.into_response()
    })
    .await;
```

构造与设置 API 的完整语义：

| API | 作用 |
|---|---|
| `Command::new` | 创建或复用命令，不计 TPS；`0` 并发和零时长表示关闭对应限制 |
| `Command::with_tps` | 与 `new` 相同，但每次请求按 `weight` 累加 TPS |
| `Command::monitor` | 参数均可选的通用构造器；同名异参时警告并返回首次实例 |
| `Command::try_monitor` | 同名异参时返回 `MonitorConflict` 和首次实例，由调用方决定是否终止启动 |
| `set_path` | 设置日志与 `nafana_command_info` 中的真实路径；只能成功设置一次 |
| `set_extra` | 设置十秒聚合日志的附加文本生成器；只能成功设置一次 |
| `set_reject_response_str` / `set_timeout_response_str` | 设置 HTTP 200 的静态 JSON 降级；非法 JSON 被忽略 |
| `try_set_reject_response_str` / `try_set_timeout_response_str` | 可感知非法 JSON 或重复设置的静态降级版本 |
| `set_reject_fn` / `set_timeout_fn` | 设置动态 `FallbackFn`；函数优先于静态 JSON，只能成功设置一次 |
| `run_fn` | 保护任意 `Future<Output = Response>` |
| `run` | 保护 Axum `Request + Next`，用于自定义中间件 |
| `current_tps` | 返回所有带 TPS 权重命令最近十秒的进程内平均 TPS |

把 `Command::run` 用作 Axum 中间件时：

```rust
use axum::{middleware, Router};
use nasa::grafana::Command;
use std::{sync::Arc, time::Duration};

fn protect_router(command: Arc<Command>, routes: Router) -> Router {
    routes.layer(middleware::from_fn(move |request, next| {
        let command = command.clone();
        async move { command.run(request, next).await }
    }))
}

let command = Command::new(
    "admin-api",
    "middleware",
    16,
    Duration::from_secs(2),
);
let app = protect_router(command, service_routes);
```

`set_*` 使用一次性槽位：重复调用保留第一次设置。启动配置来自外部输入时，优先使用 `try_monitor` 和
`try_set_*_response_str`，让错误中止启动，而不是带着被忽略的配置继续运行。

## 6. 配置驱动隔离

不方便给每个 handler 加宏时，可以按路由配置。配置字段使用 `tps_weight`，不是宏参数名 `tps`：

```yaml
grafana:
  isolation:
    "/downloads/*":
      max_concurrent: 8
      timeout_ms: 30000
      tps_weight: 1
    "/reports/daily":
      max_concurrent: 4
      timeout_ms: 2000
```

业务配置反序列化成 `HashMap<String, IsolationRule>` 后初始化，并挂全局中间件：

```rust
use axum::{middleware, Router};
use nasa::grafana::{dispatch, init_isolation, IsolationRule};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct AppConfig {
    grafana: GrafanaConfig,
}

#[derive(Deserialize)]
struct GrafanaConfig {
    #[serde(default)]
    isolation: HashMap<String, IsolationRule>,
}

let rules: HashMap<String, IsolationRule> = app_config.grafana.isolation;
init_isolation(&rules, "/order-service");

let app = Router::new()
    .nest("/order-service", service_routes)
    .layer(middleware::from_fn(dispatch));
```

- `/*` 后缀表示 catch-all，例如 `/downloads/*`。
- `context_path` 传空串表示没有统一前缀。
- 没有规则或没有调用 `init_isolation` 时，`dispatch` 全部放行。
- 字段名严格使用 `max_concurrent`、`timeout_ms`、`tps_weight`。未知字段会让反序列化失败，例如
  `timeoutMs` 不会被静默忽略。**后果是整段配置解析失败**：本地 yml 写错会导致应用启动失败，
  配置中心下发的配置写错会导致该次热更新整体失败。这是刻意的 fail-fast——它比"保护被静默关闭、
  面板照常出卡片"安全，但改配置前应先在预发环境验证。
- 不要让同一接口同时命中配置驱动规则并标注 `#[grafana]`，否则会发生双重保护和双重指标：外层
  (`dispatch`) 超时会把内层宏命令记成 `canceled`，两张卡的结局对不上。
- 配置驱动路径使用默认 429/504 响应；需要按端点自定义降级时使用宏或显式 `Command`。

## 7. 指标语义

Prometheus 抓取后会自动附加 `job` 和 `instance`，运行时自身输出 `command` 与 `group`。

| 指标 | 说明 |
|---|---|
| `nafana_requests_total` | `success`、`failure`、`timeout`、`rejected`、`canceled` 单调计数 |
| `nafana_fallback_total` | 拒绝或超时后进入降级分支的单调计数 |
| `nafana_tps_total` | 按 `tps`/`tps_weight` 累加的单调计数 |
| `nafana_inflight` | 当前执行中的请求数 |
| `nafana_inflight_rolling_max` | 最近十秒并发峰值 |
| `nafana_inflight_lifetime_max` | 进程生命周期并发峰值 |
| `nafana_max_concurrent` | 静态并发上限；0 表示不限，用于 PromQL 饱和度计算 |
| `nafana_timeout_ms` | 静态超时毫秒；0 表示不超时 |
| `nafana_tps_weight` | 静态 TPS 权重；0 表示未配置或权重为 0 |
| `nafana_latency_seconds_*` | 可跨实例聚合的 Prometheus histogram |
| `nafana_command_info` | 卡片名、分组和真实路由元信息 |

handler 返回 5xx 时记为 `failure`；其它 handler 响应，包括业务主动返回的 4xx，记为 `success`。
组件自身产生的并发拒绝和超时分别记为 `rejected`、`timeout`。调用方中止请求任务、客户端断开导致
执行 future 被丢弃，或执行期间发生 panic 时记为 `canceled`；它计入 QPS、错误率和配置过的 TPS，但不进入
延迟 histogram，也不触发拒绝/超时降级。

命令在第一次请求时注册。因此从未调用过的 `#[grafana]` handler 不会出现在 `/metrics` 或 Dashboard 中。

### 7.1 延迟直方图边界

- `nafana_latency_seconds` 是进程生命周期单调 histogram，可跨实例聚合。运行时不缓存进程内延迟样本，
  Dashboard 的 Mean 和各分位统一由 Prometheus 合并所选实例的 bucket/sum/count 后计算。
- histogram 最大有限桶为 10 秒；更慢的请求
  只进入 `+Inf`、`sum` 和 `count`，用有限桶算出的高分位会在 10 秒处饱和。
- `rejected` 和 `canceled` 没有完整业务执行耗时，不进入延迟 histogram。
- `nafana_timeout_ms` 是隔离配置 gauge，不是请求延迟；它保留毫秒单位便于运维直接阅读。

### 7.2 指标基数与容量

当前每个命令约导出 30 条 time series；实际 `/metrics` 文本大小还取决于 `command`、`group`、`path` 标签
长度。端点很多时应在上线前按
“命令数 × 实例数 × 抓取频率”评估 Prometheus 存储、网络和查询成本，并限制动态命令名；不要把用户 ID、
订单号等无界值放入 `name`、`group` 或 `path`。

## 8. Dashboard 兼容性

[`dashboards/nafana-interfaces.json`](dashboards/nafana-interfaces.json) 已在 Grafana **12.1.0** 验证。
它使用该版本的实验性 `dashboard.grafana.app/v2alpha1` Dashboard schema v2，以及内置的 Text、Stat、
Time series、RowsLayout 和 AutoGridLayout，不需要第三方插件。

不要把“12.1.0 已验证”理解为“所有更高版本都保证兼容”。Grafana 的 alpha API 和 schema 可能变化。
使用其它 Grafana 版本时，应先在预发环境导入；若该版本不再提供 `v2alpha1`，应使用目标版本导出兼容的
V2 Resource，并同步修改 JSON 的 `apiVersion` 和导入 API 路径。

Grafana 12.1.0 需要：

```text
GF_FEATURE_TOGGLES_ENABLE=kubernetesDashboards,dashboardNewLayouts
```

## 9. `dashboards` 目录与复制后的最小改动

| 文件 | 用途 | 复制后需要处理 |
|---|---|---|
| `nafana-interfaces.json` | Grafana V2 Dashboard | 标准接入不修改 |
| `prometheus.yml` | 通用双实例抓取模板 | 只修改 target `host:port` |
| `grafana-datasource.yml` | Grafana Prometheus 数据源 provisioning | 容器名不是 `prometheus` 时只改 URL 的 `host:port` |
| `grafana.env.example` | Grafana 12.1.0 feature 示例 | 合并到现有 feature 列表并重启 Grafana |
| `README.md` | Dashboard 部署速查 | 不参与运行 |

样本已经统一使用以下稳定约定：

- `job_name: nafana-app`
- `metrics_path: /metrics`
- Grafana 数据源显示名 `Prometheus`
- Grafana 数据源 UID `prometheus`

复制标准示例时只需要把 `prometheus.yml` 中的 target 改成真实节点 `host:port`。Grafana 无法通过
`http://prometheus:9090` 访问 Prometheus 时，再改 `grafana-datasource.yml` 中这一处 URL。Dashboard
不包含业务节点地址，因此不需要改 JSON 或 PromQL。

### 9.1 业务应用必须确认

1. Cargo 开启 `grafana`；使用 mapping 注解时再开启 `web`。
2. 给目标 handler 选择一种接入方式：`#[grafana]`、配置驱动或显式 `Command`。
3. 推荐在根路由暴露 `/metrics`，或保证实际 context path 与 Prometheus `metrics_path` 一致。
4. 每个 Prometheus target 指向一个独立进程、容器或 Pod。
5. 确认指标入口的网络和鉴权策略。
6. 确认自定义 `name` 唯一、稳定且低基数。

### 9.2 Dashboard JSON 的高级定制

标准接入不修改 `nafana-interfaces.json`。只有共享 Grafana 中资源名冲突、数据源没有使用仓库默认 UID，
或需要修改页面名称时才检查：

1. `metadata.name`：当前 namespace 内唯一的资源名。
2. `spec.title`：页面标题。
3. `DS_PROMETHEUS.current.text`：Grafana 数据源显示名。
4. `DS_PROMETHEUS.current.value`：Grafana 数据源 UID，不是 Prometheus URL。

普通接入不需要逐条修改 PromQL。所有查询统一引用 `${DS_PROMETHEUS}` 和 Dashboard 顶部的 `$job`、
`$instance`、`$group`、`$command` 变量。

### 9.3 多实例与集群口径

同一应用的所有实例应配置为同一个 Prometheus `job_name`，由 Prometheus 自动添加不同的 `instance` 标签。
Dashboard 的 `$instance` 默认选择 `All`，此时每张接口 Time series 都按 `command, group` 对所有选中实例求和：

这里的“实例”必须是**独立进程、容器或 Pod**，每个 target 必须导出自己独立的计数器。地址不同并不等于
实例不同。例如一个进程监听 `0.0.0.0:8080` 时，同时配置 `127.0.0.1:8080` 和
`<同一主机的网卡地址>:8080` 只是在抓取同一个进程两次。Prometheus 仍会生成两个 `instance` 标签，
Dashboard 也会按两个实例求和，结果是 Hosts、QPS、TPS、各结局和 histogram 全部重复计算。

以下部署才是两个有效实例：

- 两台主机各运行一个进程，例如 `order-service-1:8080` 与 `order-service-2:8080`。
- 同一主机运行两个独立进程，分别监听不同端口，例如 `127.0.0.1:8081` 与 `127.0.0.1:8082`。
- 两个容器或 Pod 各自运行进程，并使用各自可路由的地址。

`sum(up{job="..."}) == 2` 只能证明 Prometheus 配置了两个可抓取 target，不能证明背后一定是两个独立进程。

- QPS、TPS、各请求结局和 fallback：先对各实例 counter 求 `rate`，再求和。
- Error：所有选中实例的非成功速率之和除以总请求速率之和，不是实例错误率的平均值。
- Inflight：所有存活实例的当前执行数之和。
- Hosts：同时存在该接口指标且 `up == 1` 的实例数。
- Mean、P50、P90、P99、P99.5：合并各实例 histogram bucket/sum/count 后计算，代表集群请求总体。

选择一个 `$instance` 时，同一套查询自然收窄为单实例。Dashboard 不再从业务进程加载图片或其它单节点数据，
因此不会发生数字代表集群、折线却代表某一个节点的情况。

## 10. Prometheus 配置

Dashboard 的速率、错误率与最近状态查询使用 10 秒窗口，因此目标 job 的 `scrape_interval` **必须不大于
5 秒**，保证正常情况下窗口内至少有两个样本。仓库样本使用与具体业务工程无关的根路径 `/metrics`。
已有 Prometheus 时不要覆盖整份配置，只合并下面的 job，并只替换 target `host:port`：

```yaml
scrape_configs:
  - job_name: "nafana-app"
    scrape_interval: 5s
    scrape_timeout: 4s
    metrics_path: "/metrics"
    scheme: "http"

    static_configs:
      - targets:
          - "10.0.0.11:18081"
          - "10.0.0.12:18081"
        labels:
          name: "nafana-app"
          env: "production"
```

按推荐根指标入口部署时只修改：

- `targets`：Prometheus 进程能够访问的 `host:port`，不带协议和路径。

以下字段已经提供可工作的默认值；只有业务改变默认契约时才修改：

- `job_name`：默认 `nafana-app`，也是 Dashboard 的“应用 (job)”选项。
- `scrape_interval`：默认 `5s`，必须 `<= 5s`；更大时面板会间歇性显示 0 或 `—`。
- `metrics_path`：真实指标路径，包含 context path。
- `scheme`：`http` 或 `https`。
- `name`、`env`：可选自定义标签；不需要时可以删除。

同一应用的所有实例使用相同 `job_name`，每个 target 会得到不同的 `instance`。

每个 target 必须对应独立的运行时进程。不要把同一监听端口的回环地址、主机网卡地址、域名别名或代理地址
同时加入同一个 job；地址别名会让同一份单调计数器被重复抓取和重复聚合。同一主机部署多实例时，应让进程
监听不同端口，例如：

```yaml
static_configs:
  - targets:
      - "host.docker.internal:18081"
      - "host.docker.internal:18082"
```

Prometheus 在 Docker、应用在宿主机时，macOS/Windows 通常使用
`host.docker.internal:<port>`。Linux Docker 可在 Compose 中添加：

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

原生 Prometheus 进程访问本机应用时可使用 `127.0.0.1:<port>`。

修改后检查并重新加载：

```bash
promtool check config /etc/prometheus/prometheus.yml
curl --fail --request POST "${PROMETHEUS_URL}/-/reload"
```

`/-/reload` 要求 Prometheus 启动时开启 `--web.enable-lifecycle`；否则重启 Prometheus。

重新加载后先用 PromQL 验证实例与聚合口径：

```promql
# Prometheus 当前成功抓取的 target 数；它不是独立进程身份证明
sum(up{job="nafana-app"})

# 每个实例的接口 QPS
sum by (instance) (
  rate(nafana_requests_total{job="nafana-app",command="/orders"}[10s])
)

# Dashboard 中该接口的 Cluster QPS
sum(
  rate(nafana_requests_total{job="nafana-app",command="/orders"}[10s])
)
```

用已知流量验算最可靠：若两个独立实例各收到 `9/s`，第二条查询应分别得到约 `9/s`，第三条应得到约
`18/s`。若两个 instance 都显示约 `18/s`、集群显示约 `36/s`，通常是两个 target 实际指向同一进程，
而该进程同时收到了两路 `9/s` 流量。

## 11. Grafana 与 Prometheus 数据源配置

### 11.1 Grafana 12.1.0 Docker 配置

```yaml
services:
  grafana:
    image: grafana/grafana:12.1.0
    environment:
      GF_FEATURE_TOGGLES_ENABLE: kubernetesDashboards,dashboardNewLayouts
    ports:
      - "3000:3000"
```

已有 `GF_FEATURE_TOGGLES_ENABLE` 时追加两个名称，不要覆盖原有 feature。修改后必须重启 Grafana。

### 11.2 Provisioning 添加 Prometheus 数据源

直接复制 [`dashboards/grafana-datasource.yml`](dashboards/grafana-datasource.yml) 到 Grafana 的
`provisioning/datasources/`。文件内容为：

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

- `url` 是 Grafana 服务端访问 Prometheus 的地址，不是浏览器地址。
- `uid: prometheus` 与公开 Dashboard 模板的默认 `DS_PROMETHEUS.current.value` 一致。
- Docker Compose 服务名为 `prometheus` 时无需修改；否则只修改 `url` 的 `host:port`。
- 不要随意修改 `name` 或 `uid`；保持默认值时 Dashboard JSON 可以零修改导入。

Docker Compose 挂载示例：

```yaml
services:
  grafana:
    volumes:
      - ./provisioning/datasources:/etc/grafana/provisioning/datasources:ro
```

## 12. 导入和更新 Dashboard

以下示例不写入账号密码。为 Grafana 创建最小权限 service account token，并只放在本机环境变量或 secret
manager 中：

```bash
export GRAFANA_URL="https://grafana.example.com"
export GRAFANA_TOKEN="<SERVICE_ACCOUNT_TOKEN>"
export GRAFANA_NAMESPACE="default"
```

不要把真实 token、Cookie 或 Authorization header 提交到 Git。

### 12.1 首次创建

```bash
curl --fail-with-body \
  --header "Authorization: Bearer ${GRAFANA_TOKEN}" \
  --header "Content-Type: application/json" \
  --request POST \
  "${GRAFANA_URL}/apis/dashboard.grafana.app/v2alpha1/namespaces/${GRAFANA_NAMESPACE}/dashboards" \
  --data-binary @dashboards/nafana-interfaces.json
```

### 12.2 更新已有 Dashboard

不能只把 POST 改成 PUT。`v2alpha1` 更新需要先读取服务端资源，并保留当前 `metadata`，尤其是
`metadata.resourceVersion`：

```bash
export DASHBOARD_NAME="nafana-interfaces"
export DASHBOARD_API="${GRAFANA_URL}/apis/dashboard.grafana.app/v2alpha1/namespaces/${GRAFANA_NAMESPACE}/dashboards"

curl --fail-with-body \
  --header "Authorization: Bearer ${GRAFANA_TOKEN}" \
  "${DASHBOARD_API}/${DASHBOARD_NAME}" \
  --output current-dashboard.json

jq --slurpfile template dashboards/nafana-interfaces.json \
  '.spec = $template[0].spec' \
  current-dashboard.json > dashboard-update.json

curl --fail-with-body \
  --header "Authorization: Bearer ${GRAFANA_TOKEN}" \
  --header "Content-Type: application/json" \
  --request PUT \
  "${DASHBOARD_API}/${DASHBOARD_NAME}" \
  --data-binary @dashboard-update.json
```

`current-dashboard.json` 和 `dashboard-update.json` 是临时文件，不应提交到 Git。若线上有手工修改，替换整个
`spec` 会覆盖这些修改，应先 review diff。

## 部署接入核对

先验证业务入口：

```bash
curl --fail "${APP_BASE_URL}/metrics" | grep '^nafana_'
```

再按顺序检查：

1. 至少调用一次目标 `#[grafana]` handler。
2. Prometheus Targets 中目标为 `UP`，并确认实际抓取间隔不大于 5 秒；每个 target 必须对应独立进程，
   不能只是同一监听端口的多个地址别名。
3. Prometheus 能查询到 `nafana_requests_total`，且 10 秒窗口查询不是间歇性空值。
4. Grafana 选择正确的 Prometheus 数据源和 `job`。
5. 5～10 秒后接口卡出现折线；鼠标移入折线时出现十字定位、时间和完整指标列表。
6. 给每个实例发送一段已知速率的流量，确认 `sum by (instance)(rate(...))` 等于各实例实际速率，Cluster QPS
   等于这些速率之和；不要只凭 `Hosts` 判断集群是否正确。
7. `$instance=All` 时，对照 Prometheus 查询确认 Cluster QPS、Error、Hosts 和延迟分位是所选实例的聚合值。
8. 停止全部应用实例后，QPS/TPS/Hosts 归零，错误率和无新样本的延迟显示 `—`。

## 14. 常见问题

### 没有任何接口卡

宏命令在第一次调用时才注册。先请求业务接口，再检查 Prometheus target、`metrics_path`、数据源和 `$job`。

### 接口在线，但数字间歇性变成 0 或 `—`

检查 Prometheus Targets 页面显示的实际抓取间隔。Dashboard 使用 `[10s]` 查询窗口，目标 job 的
`scrape_interval` 必须不大于 5 秒；只修改全局配置但被 job 级配置覆盖，同样会出现这个问题。

### 错误率不为零，但成功/失败/超时/拒绝都是 0

看接口折线的十字悬停列表中的 `Canceled /s`。客户端断开连接、上游或外层包装取消请求、handler panic 都会记 `canceled`，
它计入 QPS、错误率并让状态变成 `Protecting`，但不属于其它四种结局。`canceled` 常常来自客户端行为
（用户刷走页面、网关或压测工具超时），服务端不一定有问题；需要只看服务端错误时，用
`outcome!~"success|canceled"` 单独建面板。

### 有数字但接口折线没有变化

确认时间范围包含实际请求、Prometheus 抓取间隔不大于 5 秒，并在 Prometheus 中直接执行同一条 `rate(...[10s])`
查询。折线完全来自 Prometheus，不需要浏览器访问业务应用。

### 出现 `Field not found`

确认使用的是仓库当前 Dashboard JSON，并确认 Grafana 版本和 `v2alpha1` schema 与验证基线一致。

### Hosts 或 Cluster QPS 不正确

同一应用的实例应使用相同 `job_name`，同时保留 Prometheus 自动生成的不同 `instance` 标签。

若 Hosts 比实际进程数大，或所有 QPS/TPS/结局/延迟计数都恰好按整数倍放大，优先检查是否把同一进程配置成
了多个 target。常见错误是一个进程监听 `0.0.0.0:8080`，Prometheus 却同时抓
`127.0.0.1:8080` 和该主机的网卡地址。两个 target 都会 `UP`，但导出的原始 counter 完全相同，聚合时会
被计算两次。删除地址别名，或真正启动监听不同端口的第二进程。

### 为什么接口 QPS 只有几十

Dashboard 展示的是实际到达接口的请求速率，不是服务的理论吞吐上限。若压测器给每个实例的某接口只发送
`9/s`，两个实例的 Cluster QPS 就应约为 `18/s`。需要验证高吞吐时，应明确设置压测并发、持续时间和目标
速率，并以压测器完成数与 `sum by (instance)(rate(...))` 相互校验。

### 自定义静态 JSON 为什么是 HTTP 200

这是 `reject_response` 和 `timeout_response` 的既定语义。需要 429、504 或其它状态时，改用
`reject_fn` 或 `timeout_fn`。

## 15. 安全说明

- `/metrics` 会暴露接口名、路由和运行指标，不应直接暴露到不可信公网。
- 如果 `/metrics` 需要鉴权，必须在 Prometheus scrape job 中配置对应凭据；Grafana 用户浏览器不需要直接访问
  业务应用，Grafana 只通过已配置的数据源查询 Prometheus。
- 不要把生产域名、私网 IP、账号、密码、token、Cookie 或临时 API 响应写入 Dashboard 模板和 README。

Dashboard 文件的简明复制清单见 [`dashboards/README.md`](dashboards/README.md)。
