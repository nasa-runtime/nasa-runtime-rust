# hystrix

`hystrix` 提供路由级 bulkhead 隔离、超时和 Hystrix Dashboard 指标流。业务通常通过门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["hystrix"] }
```

```rust
use nasa::hystrix::hystrix;

#[hystrix(name = "order-detail", max_concurrent = 50, timeout_ms = 800, tps = 1)]
async fn detail() -> impl axum::response::IntoResponse {
    "ok"
}
```

返回具体 `Result<T, E>` 的 handler 同样受支持，只要 `Result<T, E>` 实现 Axum `IntoResponse`；函数体内
可以正常使用 `?`：

```rust
use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};

#[derive(Debug)]
struct AppError;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

#[hystrix(name = "order-detail")]
async fn detail() -> Result<Json<serde_json::Value>, AppError> {
    let order = load_order().await?;
    Ok(Json(order))
}
```

返回 `impl IntoResponse`、`Result<impl IntoResponse, E>` 和无返回值的写法同样受支持：宏把原函数体搬进
一个内层 `async fn`，原返回类型仍写在返回位置，因此 `?` 与 `Ok(..)` 的类型推断不受影响。

`#[hystrix]` 与本仓路由宏组合时**必须写在 `#[*_mapping]` 上方**，否则路由属性会先被消费掉，
监控层读不到真实路由；顺序写反会直接编译报错，不会静默退化成函数名。

## 能力边界

本 crate 做的是：

- per-command 信号量隔离，超过并发上限立即拒绝。
- 单请求超时。
- 局部降级与进程级全局终态降级；全局处理器同步生成一次最终响应，不叠加第二层并发或超时保护。
- rolling window 指标、延迟分位、当前并发、TPS。
- `/hystrix.stream` SSE 输出，兼容 Hystrix Dashboard。

零值语义：`max_concurrent = 0` 表示不限并发、`timeout_ms = 0`（或零时长 `Duration`）表示不设超时。
注解、显式 `Command` 和 yml 配置三条路径统一在构造时归一化，不会建出 0 许可信号量或 0 毫秒超时。
显式构造器传入超过 Tokio semaphore 范围的并发数或超过 365 天的 `Duration` 时，会收敛到对应
运行时上限，不会因边界运算触发 panic。

请求结局：正常响应按 5xx / 非 5xx 分成 `failure` / `success`，组件自身产生的并发拒绝和超时分别是
`rollingCountSemaphoreRejected` / `rollingCountTimeout`。执行 future 在产生上述结局前被丢弃时
（客户端断连、外层包装取消、handler panic）记 `rollingCountCanceled`：它计入 `requestCount` 与
`errorPercentage`，但不产生延迟样本，也不触发降级。官方 Hystrix Dashboard 不认识这个字段会忽略它，
错误率仍会如实体现。

本 crate 不做错误率触发的 Open/HalfOpen/Closed 熔断状态机。下游持续失败时，它保护的是本服务并发和超时边界，不会自动短路。

指标口径注意:`rollingMaxConcurrentExecutionCount` 是**进程生命周期内的并发峰值**(只增不减),不是滚动窗口内峰值——一次流量尖峰后 Dashboard 会持续显示该值。其余 rollingCount* 为 10s 滚动窗口。同名 Command 重复构造会在 Dashboard 出现重复圈(各自独立统计),注解宏路径已按 handler 缓存避免;
手动 `Command::new` 请自行复用实例——重复构造同 (group, name) 时会打一条 `warn` 提示。

## 全局降级

业务端点没有配置局部降级函数或静态响应时，可以由一个进程级处理器统一接管并发拒绝和执行超时。
推荐用 `#[nasa::hystrix::global_fallback]` 自动收集唯一入口，无需在启动函数手动注册：

```rust
use axum::{http::StatusCode, response::IntoResponse, Json};
use nasa::hystrix::{FallbackCause, FallbackContext};

#[nasa::hystrix::global_fallback]
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
线程或访问数据库、缓存、RPC 等外部资源。组件不会再给它配置 `max_concurrent` 或 `timeout_ms`，也不会在
它失败后调用第二个业务降级；配置冲突、panic 或递归只会收敛到内置 429/504。

固定优先级为：端点局部函数 → 端点局部静态响应 → 本组件全局处理器 → 内置 429/504。同一组件只能
收集一个属性入口；首次需要时自动初始化。希望在开放流量前检查唯一性时，可调用
`initialize_global_fallback()`，多个声明会返回按源码位置排序的
`GlobalFallbackInstallError::MultipleCollectedHandlers`。没有使用属性宏时，仍可实现同步
`GlobalFallbackHandler` 并调用 `install_global_fallback(Arc<_>)`；手动实现可返回
`FallbackDecision::UseBuiltin`，但不能覆盖已经收集的属性入口。

`FallbackContext::transaction_weight()` 只传递端点声明的 REST 事务权重。主请求进入组件时已经按该权重
完成一次 TPS 记账，执行全局降级不会重复增加 TPS。Dashboard 的
`rollingCountFallbackSuccess` 记录局部或全局成功产出的降级响应，`rollingCountFallbackFailure` 记录全局
配置冲突、panic 或递归。由于终态处理器没有第二层隔离舱，
`rollingCountFallbackRejection` 与 `propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests` 恒为 0。

## 显式 Command

```rust
let cmd = nasa::hystrix::Command::new(
    "heavy-api",
    "api",
    20,
    std::time::Duration::from_millis(500),
);

let response = cmd
    .run_fn(|| async {
        axum::response::IntoResponse::into_response("ok")
    })
    .await;
```

## Dashboard

把 SSE endpoint 挂到路由：

```rust
let app = axum::Router::new()
    .route("/hystrix.stream", axum::routing::get(nasa::hystrix::hystrix_stream));
```

指标上报来自全局 `Command` 注册表。宏注解和显式 `Command` 都会注册进去。

## YML 配置与使用

`hystrix` 支持把路由级隔离规则从 yml 反序列化为 `IsolationRule`，再用 `init_isolation` 初始化全局匹配表。适合不想为每个接口手写 `Command` 的服务。

完整示例：

```yaml
server:
  context_path: /order

hystrix:
  isolation:
    /api/orders/{id}:
      max_concurrent: 50
      timeout_ms: 800
      tps_weight: 1
    /api/export/*:
      max_concurrent: 2
      timeout_ms: 30000
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `hystrix.isolation.<pattern>.max_concurrent` | 必填 | 并发上限；0 表示不启用并发隔离。 |
| `hystrix.isolation.<pattern>.timeout_ms` | 必填 | 单请求超时；0 表示不启用超时保护。 |
| `hystrix.isolation.<pattern>.tps_weight` | `null` | 计入全局 TPS 的权重；不配置则不计入。 |
| `server.context_path` | `""` | 初始化时传给 `init_isolation`，请求匹配前会剥离该前缀。 |

启动代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    server: ServerConfig,
    hystrix: HystrixConfig,
}

#[derive(serde::Deserialize)]
struct HystrixConfig {
    #[serde(default)]
    isolation: std::collections::HashMap<String, hystrix::IsolationRule>,
}

hystrix::init_isolation(&cfg.hystrix.isolation, &cfg.server.context_path);

let app = axum::Router::new()
    .route("/hystrix.stream", axum::routing::get(hystrix::hystrix_stream))
    .layer(axum::middleware::from_fn(hystrix::dispatch));
```

匹配规则支持普通路径和尾部 `*`，例如 `/api/export/*` 会转换成 catch-all。配置为空时中间件全放行。

字段名严格按上表书写：`IsolationRule` 拒绝未知字段，`timeoutMs` 这类拼写错误会让整段配置解析失败
（应用启动失败或该次热更新失败），而不是被静默忽略成"保护未启用"。
