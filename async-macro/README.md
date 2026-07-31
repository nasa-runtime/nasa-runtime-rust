# async-macro

`async-macro` 提供 `#[Async]`、`#[scheduled]`、`#[EnableScheduling]`/`#[EnableAsync]` 过程宏。它只负责编译期改写和注册，真正运行、启动、停机、leader gate、misfire、recorder 都在 `nasched` 运行时里。

业务项目通过 `nasa` 门面使用这些宏：

```toml
[dependencies]
nasa = { version = "1", features = ["scheduling"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
```

## `#[Async]`: 调用即后台执行

适合把一个 `async fn` 改成“调用就 `tokio::spawn`，立即返回 `JoinHandle<T>`”的接口。函数参数和返回值必须满足 `Send + 'static`；方法场景请用 owned `self` 或 `self: Arc<Self>`，不要用 `&self`。

```rust
use nasa::scheduling::Async;

#[Async]
async fn send_mail(to: String) -> bool {
    // 调用方不等待时,邮件发送在后台跑。
    true
}

async fn demo() -> anyhow::Result<()> {
    send_mail("ops@example.com".to_string());

    let ok = send_mail("audit@example.com".to_string()).await?;
    assert!(ok);
    Ok(())
}
```

批量改写 inherent impl 块：

```rust
use std::sync::Arc;
use nasa::scheduling::Async;

struct Worker;

#[Async]
impl Worker {
    async fn refresh(self: Arc<Self>, tenant_id: String) {
        // 后台刷新租户缓存。
    }
}
```

## `#[scheduled]`: 自动注册定时任务

适合无人直接调用、由调度器按时间触发的零参 `async fn`。宏会把任务登记到调度运行时，但必须在
应用启动时调用 `nasa::scheduling::start_scheduled()`，或在 main 上使用 `#[EnableScheduling]`。

固定频率：按开始时间触发，任务慢时可重叠。

```rust
use nasa::scheduling::scheduled;

#[scheduled(fixed_rate_ms = 5_000)]
async fn push_heartbeat() {
    // 每 5 秒推一次心跳,默认启动后立即首跑。
}
```

固定频率限流：适合任务可能比周期更慢的场景。

```rust
use nasa::scheduling::scheduled;

#[scheduled(fixed_rate_ms = 1_000, skip_if_running = true)]
async fn pull_orders() {
    // 上一次未完成时跳过本拍,避免堆积重复拉取。
}

#[scheduled(fixed_rate_ms = 1_000, max_in_flight = 3)]
async fn sync_items() {
    // 最多允许 3 个并发同步任务。
}
```

固定延迟：适合必须串行处理的 drain/补偿任务。

```rust
use nasa::scheduling::scheduled;

#[scheduled(fixed_delay_ms = 2_000)]
async fn drain_retry_queue() {
    // 每次跑完后再等 2 秒,不会重叠。
}
```

一次性任务：适合启动预热。

```rust
use nasa::scheduling::scheduled;

#[scheduled(initial_delay_ms = 3_000)]
async fn warm_indexes() {
    // 应用启动 3 秒后跑一次。
}
```

duration string：适合让时间表达更接近业务语义。

```rust
use nasa::scheduling::scheduled;

#[scheduled(fixed_rate_string = "1m30s")]
async fn refresh_dashboard_cache() {}

#[scheduled(fixed_delay = 5, time_unit = "seconds")]
async fn scan_expired_sessions() {}
```

cron：必须是 6 字段含秒，`zone` 支持 `UTC`、IANA 时区和固定 offset。

```rust
use nasa::scheduling::scheduled;

#[scheduled(cron = "0 0/5 * * * *", zone = "Asia/Shanghai")]
async fn rebuild_summary() -> anyhow::Result<()> {
    Ok(())
}
```

标准 `Result` 返回值会被识别，`Err` 会记录任务失败；普通返回值会被忽略。为了让宏稳定识别，请写显式路径或 `anyhow::Result`，不要只写裸名类型别名。

## `#[EnableScheduling]`: main 入口自动启动

适合简单服务，main 一开始就启动所有 `#[scheduled]` 任务。宏必须放在 `#[tokio::main]` 上面。

```rust
use nasa::scheduling::{scheduled, EnableScheduling};

#[scheduled(fixed_rate_ms = 10_000)]
async fn collect_metrics() {}

#[EnableScheduling]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // start_scheduled().await? 已被注入到函数体最前面。
    Ok(())
}
```

如果业务需要先初始化日志、配置、leader gate、recorder 或 FireLog，不要用
`#[EnableScheduling]`，改为在初始化完成后手动调用
`nasa::scheduling::start_scheduled_with(...)`。

## YML 配置与使用

`async-macro` 不读取 yml，宏参数全部写在 Rust 属性里。使用
`#[nasa::application("scheduling")]` 时，受管运行时读取下面的精确配置：

```yaml
scheduling:
  cluster: local
  leader_period_ms: 1000
  node_id: node-a
```

属性和 yml 的分工：

| 事项 | 位置 |
| --- | --- |
| `fixed_rate_ms`、`fixed_delay_ms`、`cron`、`zone` | `#[scheduled(...)]` 属性 |
| `max_in_flight`、`skip_if_running`、`timeout_ms`、`misfire` | `#[scheduled(...)]` 属性 |
| 是否启动调度器 | 是否声明 `"scheduling"`，或是否显式调用启动 API |
| `cluster`、`leader_key`、`leader_period_ms`、`node_id` | 受管 `scheduling:` 配置 |
| 自定义 gate、FireLog、recorder | 手工构造 `nasa::scheduling::SchedulerOptions` |

集群模式把 `cluster` 改为 `leader`，并提供 `leader_key`；同时必须启用
`scheduling-cluster` feature 并声明 `"redis"`。需要业务自定义 gate 或 recorder 时，不使用受管组件，
而是在启动完成后显式构造 `SchedulerOptions`。

```rust
nasa::scheduling::start_scheduled_with(opts).await?;
```

## 主要边界

- `#[Async]` 会创建后台任务；参数、返回值和捕获值必须满足 `Send + 'static`，调用方仍要持有或等待
  `JoinHandle` 才能观测失败。
- `#[scheduled]` 只接受零参数异步函数；触发方式、并发限制和集群语义在编译期校验。
- `#[EnableScheduling]` 会在 main 函数体最前启动调度器，不适合需要先完成日志、配置或 leader
  注入的应用；这类应用应显式调用 `start_scheduled_with`。
- 宏只登记任务，不拥有进程停机；正式应用优先让 `"scheduling"` 组件统一启动和反向排空。
