# hystrix

`hystrix` 提供路由级 bulkhead 隔离、超时和 Hystrix Dashboard 指标流。业务通常通过门面使用：

它提供独立命令面，也可由 Application 管理配置、命令目录与周期观测：Web 限流负责入站配额，
REST 发现客户端的 bulkhead/circuit 负责传输级失败，hystrix 负责业务判定失败、慢成功和非 REST
出站依赖的隔离。三者不会自动互相调用或共享状态。
受管模式把固定规则、显式命令和属性命令绑定到本代 Application，统一周期观测，业务收尾后等待
在途调用退出再撤销目录。属性宏随实例重建命令，旧句柄永久返回 503，避免沿用已经关闭的隔离资源。

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["hystrix"] }
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

## 组合边界

- REST 调用外层再包 hystrix 时，`Command` timeout 必须大于客户端的完整重试预算；否则外层已判超时，
  内层跨实例尝试仍可能占用连接。
- 包裹真实执行体，不包裹 napart 或 `#[Async]` 的提交动作；hystrix 不感知队列等待、任务终态或取消合同。
- 命令名必须是代码常量级低基数，不得拼接租户、订单等业务值。

指标口径注意:`rollingMaxConcurrentExecutionCount` 是**命令实例生命周期内的并发峰值**(只增不减),不是滚动窗口内峰值——一次流量尖峰后 Dashboard 会持续显示该值。其余 rollingCount* 为 10s 滚动窗口。独立模式同名 Command 重复构造会在 Dashboard 出现重复圈(各自独立统计),注解宏路径已按 handler 缓存避免;
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

## Application 受管目录

### 运行架构与关闭顺序

```text
固定规则 + 命名计划 + 静态属性描述 → Prepare 安装唯一 ManagedRuntime
        → 本代命令目录 + 一个周期观测任务 → 初始化 / 业务调用 / 业务收尾
        → 关闭新调用 → 等待在途业务、降级与观测退出 → 撤销规则与全局入口
下一实例 → 新 owner 与命令目录；旧 Command 永久拒绝执行
```

owner 的存活只证明本地隔离与观测职责存在，不代表被保护的数据库、远端服务或业务结果健康。
取消等待或执行器退出也不能代替在途业务 future 的释放证明。

组合 `application,hystrix` 并配置 `hystrix.enabled: true`，无需新的组件字符串。
`hystrix.isolation` 安装固定规则；受管 Web 自动在业务路由使用 dispatch，框架探针不纳入隔离。
`hystrix.commands.<name>` 预装配显式命令，业务使用 `app.hystrix_command(name).await`。
静态属性描述在 Ready 前创建本代命令，宏只缓存带代次的弱引用。配置和容量变更需要重启。
宿主在 Ready 发布前复验 owner 准入并持有保护到发布完成，已退出的周期任务阻止启动。
自建宿主可用 `ManagedRuntime::with_running` 提交短小同步的接流裁决；闭包不得阻塞或重入命令 owner。
完整 YAML 见 [napp](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#hystrix-命令-owner)。

领域入口 `ManagedRuntime::start(rules, context_path, max_commands).await` 返回唯一 owner，
`command(name, group, rule)` 明确报告重复／超量；需要自行装配时应等待 `shutdown()`。
受管目录最多 4096 条，统一一个周期观测任务。手工安装的独立隔离表、独立 Command 或运行态全局
fallback 与受管 owner 冲突；静态全局 fallback 描述可跨应用复用。直接构造的受管 Command 若身份、
容量或数值不合法，执行时返回 503，不登记新任务。独立模式仍保持原有警告和参数归一化行为。

Service 业务收尾之后才关闭命令准入，已经进入的业务和降级继续持有本代责任。所有在途调用和观察
任务退出后撤销规则与目录，关闭等待被取消仍由独立 owner 完成。下一代使用自己的规则和宏实例，
旧 Command 永久返回 503；业务自写的静态 `Arc<Command>` 不会自动切换代次，应改用命名受管句柄
或属性宏。该模式不增加错误率 Open/HalfOpen/Closed 状态机。

执行器销毁时，任务 future 的释放守卫关闭准入并记录失败；最后一个任务和在途业务 future
归还后才撤销本代全局引用。仍由外部持有的业务 future 会继续阻止新 owner 安装，不能把执行器
结束直接当作业务完成。`shutdown()` 对强制销毁返回 `TaskFailed`，旧命令仍返回 503。
