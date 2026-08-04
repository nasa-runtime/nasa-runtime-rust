# hystrix-macro

`hystrix-macro` 提供 `#[hystrix]` 与 `#[global_fallback]` 属性宏。业务通常从 `nasa::hystrix` 门面使用，
不直接依赖本宏 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["hystrix"] }
```

```rust
use nasa::hystrix::hystrix;

#[hystrix(max_concurrent = 20, timeout_ms = 800)]
async fn handler() -> impl axum::response::IntoResponse {
    "ok"
}
```

参数：

| 参数 | 含义 |
| --- | --- |
| `name` | Dashboard 圈名和日志名；不写时优先取下方 mapping path，否则取函数名 |
| `max_concurrent` | 并发上限；不写表示不限流 |
| `timeout_ms` | 单请求超时；不写表示不超时 |
| `tps` | TPS 权重；`0` 或不写表示不计入顶栏 TPS |
| `reject_response` | bulkhead 拒绝时返回的 JSON 字符串 |
| `timeout_response` | 超时时返回的 JSON 字符串 |
| `reject_fn` | bulkhead 拒绝时调用的零参 async 降级函数 |
| `timeout_fn` | 超时时调用的零参 async 降级函数 |

与 mapping 一起用时，`#[hystrix]` 要写在 mapping 注解上面，宏才能读取路由 path：

```rust
#[hystrix(timeout_ms = 500)]
#[nasa::web::get_mapping("/spot/kline")]
async fn kline() -> impl axum::response::IntoResponse {
    "ok"
}
```

`reject_fn` 与 `reject_response` 互斥，`timeout_fn` 与 `timeout_response` 互斥。

进程级终态响应使用独立属性宏自动收集：

```rust
use nasa::hystrix::{FallbackCause, FallbackContext};

#[nasa::hystrix::global_fallback]
fn service_fallback(context: FallbackContext) -> impl axum::response::IntoResponse {
    match context.cause() {
        FallbackCause::BulkheadRejected { .. } => "service busy",
        FallbackCause::ExecutionTimeout { .. } => "service timeout",
        _ => "service unavailable",
    }
}
```

`#[global_fallback]` 不接受属性参数，只能标注恰好接收一个 `FallbackContext` 的同步函数；返回值可以是任意
Axum `IntoResponse`。处理函数是故障路径的最终响应生成器，不配置自身并发或超时，也不访问数据库、缓存或 RPC。

## YML 配置与使用

`hystrix-macro` 没有运行期 yml。宏参数是编译期固定策略；如果需要通过 yml 调整隔离规则，请使用 `hystrix::init_isolation` 的配置驱动模式。

宏方式：

```rust
#[hystrix(name = "order-detail", max_concurrent = 50, timeout_ms = 800, tps = 1)]
async fn detail() -> impl axum::response::IntoResponse {
    "ok"
}
```

配置驱动方式：

```yaml
hystrix:
  isolation:
    /api/orders/{id}:
      max_concurrent: 50
      timeout_ms: 800
      tps_weight: 1
```

选择建议：

| 场景 | 推荐方式 |
| --- | --- |
| 单个 handler 策略固定 | `#[hystrix(...)]` |
| 希望按环境调整并发或超时 | `hystrix.isolation` yml + `dispatch` 中间件 |
| 需要自定义拒绝/超时响应函数 | `reject_fn` / `timeout_fn` |

## 主要边界

- 只支持异步函数；宏参数中的静态响应必须是合法 JSON。
- 同一路径的函数降级和静态降级互斥。
- 同一组件只能声明一个 `#[global_fallback]`，多个声明会被运行时确定性拒绝。
- mapping 组合时属性顺序固定为 `#[hystrix]` 在上、`#[*_mapping]` 在下。
- 宏只提供并发隔离和超时，不实现错误率驱动的熔断状态机。
