# hystrix-macro

`hystrix-macro` 提供 `#[hystrix]` 与 `#[global_fallback]` 属性宏。业务通常从 `nasa::hystrix` 门面使用，
不直接依赖本宏 crate。
与 `application,hystrix` 的受管配置组合后，静态属性描述在 Ready 前进入本代命令目录，宏只缓存
带应用代次的弱引用。下一实例重新取得命令，避免静态强引用保留已经关闭的运行资源。

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["hystrix"] }
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

`hystrix-macro` 本身不读取运行期 yml，属性参数是编译期固定策略。按环境配置路由隔离时，
可由 `application,hystrix` 配合 `hystrix.enabled: true` 安装固定规则，受管 Web 自动接入 `dispatch`；
独立应用使用 `hystrix::init_isolation` 并自行安装中间件。两种 owner 不能同时安装。

宏方式：

```rust
#[hystrix(name = "order-detail", max_concurrent = 50, timeout_ms = 800, tps = 1)]
async fn detail() -> impl axum::response::IntoResponse {
    "ok"
}
```

Application 配置驱动方式：

```yaml
hystrix:
  enabled: true
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

## 应用代次与静态描述

```text
#[hystrix] → 编译期静态描述 → Prepare 装配本代 Command
        → 调用时按当前代次取得弱引用缓存 → run_fn 执行业务与降级
宿主关闭 → 等待在途责任 → 撤销 owner；下一实例重新解析命令
```

宏生成的进程级描述只保存命令构造函数和代码身份。独立模式按 handler 缓存实例；显式启用
Application 的 `hystrix.enabled` 后，在 Ready 前构造本代命令并检查重复和容量，运行缓存仅保留
代次与弱引用。同一进程再次启动应用会重建宏命令，旧受管槽在没有当前 owner 时返回 503。
静态 `#[global_fallback]` 函数描述可以复用；带运行资源的手工全局 handler 不纳入可撤销 owner。
宏不持有连接、监督任务或关闭流程；这些责任由运行时 owner 承担。规则与命令容量冻结到启动，
配置变化报告 `RestartRequired`，不能通过重新调用同一属性函数热替换本代规则。
