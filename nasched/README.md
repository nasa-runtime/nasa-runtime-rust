# nasched

`nasched` 是异步任务和定时任务运行时。它重新导出 `async-macro` 的 `#[Async]`、`#[scheduled]`、`#[EnableScheduling]`，并提供调度启动、停机、leader gate、运行记录、misfire claim 等运行期能力。

业务项目通过 `nasa` 门面开启调度能力：

```toml
[dependencies]
nasa = { version = "1", features = ["scheduling"] }
```

## 什么时候用

- 用 `#[Async]` 做“调用即后台跑”的异步入口。
- 用 `#[scheduled]` 做无人调用、按固定频率、固定延迟、一次性延迟或 cron 自动触发的任务。
- 用 `start_scheduled()` 做本地单机调度。
- 用 `start_scheduled_with(SchedulerOptions::clustered_with_id(...))` 做 leader-only 集群调度。
- 用 `FireLog` + `misfire = "fire_once"`/`"claim_only"` 做 cron 同一拍跨节点去重。
- 用 `ExecutionRecorder` 采集任务 Started/Finished/Skipped 事件。

## 本地启动

简单服务可以使用 `#[EnableScheduling]`：

```rust
use nasa::scheduling::{scheduled, EnableScheduling};

#[scheduled(fixed_rate_ms = 5_000)]
async fn heartbeat() {}

#[EnableScheduling]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Ok(())
}
```

需要先初始化日志或配置时，手动启动：

```rust
use nasa::scheduling::{scheduled, start_scheduled};

#[scheduled(fixed_delay_ms = 30_000)]
async fn clean_temp_files() {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // tracing_subscriber::fmt::init();
    start_scheduled().await?;
    tokio::signal::ctrl_c().await?;
    nasa::scheduling::shutdown_scheduled().await?;
    Ok(())
}
```

## 固定频率、固定延迟、一次性任务

固定频率按“开始时间”触发，默认允许重叠：

```rust
use nasa::scheduling::scheduled;

#[scheduled(fixed_rate_ms = 1_000, max_in_flight = 2)]
async fn sync_inventory() {
    // 每秒触发,但最多 2 个并发。
}
```

固定延迟按“完成后再等”触发，天然不重叠：

```rust
use nasa::scheduling::scheduled;

#[scheduled(fixed_delay_string = "10s")]
async fn rebuild_local_cache() {
    // 本轮完成后 10 秒再跑下一轮。
}
```

一次性任务只在启动后延迟执行一次：

```rust
use nasa::scheduling::scheduled;

#[scheduled(initial_delay_ms = 3_000)]
async fn warmup() {}
```

## cron 任务

cron 表达式要求 6 字段含秒。`zone` 支持 `UTC`、IANA 名和固定 offset。

```rust
use nasa::scheduling::scheduled;

#[scheduled(cron = "0 0 2 * * *", zone = "Asia/Shanghai")]
async fn daily_report() -> anyhow::Result<()> {
    Ok(())
}
```

`cron = "-"` 表示禁用任务，适合灰度期间保留代码但不注册。

## leader-only 集群调度

`cluster = "leader"` 表示只有当前 leader 节点触发。调度层通过 `LeaderGate` 读取本地 leader 状态，不直接绑定 Redis 或其它选主实现。

```rust
use std::sync::Arc;
use nasa::scheduling::{scheduled, start_scheduled_with, LeaderGate, SchedulerOptions};

struct StaticLeader(bool);

impl LeaderGate for StaticLeader {
    fn is_leader(&self) -> bool {
        self.0
    }
}

#[scheduled(fixed_rate_ms = 5_000, cluster = "leader")]
async fn leader_heartbeat() {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gate = Arc::new(StaticLeader(true));
    let opts = SchedulerOptions::clustered_with_id("demo-leader-lock", gate)
        .with_node_id("node-a");
    start_scheduled_with(opts).await?;
    Ok(())
}
```

分组 gate 适合把不同任务族分散到不同 leader：

```rust
use std::sync::Arc;
use nasa::scheduling::{scheduled, LeaderGate, SchedulerOptions};

struct Gate;
impl LeaderGate for Gate {
    fn is_leader(&self) -> bool { true }
}

#[scheduled(fixed_rate_ms = 1_000, cluster = "leader", group = "orders")]
async fn sync_orders() {}

#[scheduled(fixed_rate_ms = 1_000, cluster = "leader", group = "billing")]
async fn sync_billing() {}

fn options() -> SchedulerOptions {
    SchedulerOptions::clustered_with_id("default-lock", Arc::new(Gate))
        .with_group_gate_id("orders", "orders-lock", Arc::new(Gate))
        .with_group_gate_id("billing", "billing-lock", Arc::new(Gate))
}
```

## misfire 与同拍去重

`cluster = "leader"` 只表示触发瞬间当前节点认为自己是 leader，不保证 exactly-once。

`try_claim_fire` 的 `stale_before_ms` 是覆盖窗口:存储的上次触发时间**小于**该值才允许本次 claim。同一名义拍两节点并发 claim 恰有一个成功;窗口若覆盖到上一拍(取值过大)会把新拍当重复拒绝——应与 misfire 容差同量级(默认 5s)。对 cron 任务，如果同一名义触发时刻只能由一个节点执行，要加 `misfire = "claim_only"` 或 `misfire = "fire_once"`，并配置 `FireLog`。

```rust
use nasa::scheduling::scheduled;

#[scheduled(
    cron = "0 */1 * * * *",
    cluster = "leader",
    misfire = "claim_only",
    timeout_ms = 30_000
)]
async fn export_minute_snapshot() {}
```

- `claim_only`: 每一拍先原子 claim，同一 `scheduled_at` 跨节点只触发一次，不补漏。
- `fire_once`: 每一拍原子 claim，并额外用低频巡检补偿 leader 无主窗口漏掉的一拍。

`FireLog` 是业务注入的共享存储接口：

```rust
use std::future::Future;
use std::pin::Pin;
use nasa::scheduling::FireLog;

struct DbFireLog;

impl FireLog for DbFireLog {
    fn last_fire<'a>(&'a self, task: &'a str)
        -> Pin<Box<dyn Future<Output = anyhow::Result<Option<i64>>> + Send + 'a>>
    {
        Box::pin(async move {
            let _ = task;
            Ok(None)
        })
    }

    fn try_claim_fire<'a>(
        &'a self,
        task: &'a str,
        scheduled_at_ms: i64,
        stale_before_ms: i64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let _ = (task, scheduled_at_ms, stale_before_ms);
            Ok(true)
        })
    }

    fn fingerprint(&self) -> Option<String> {
        Some("db-fire-log".to_string())
    }
}
```

生产实现需要用 Redis Lua、数据库唯一键或条件更新保证 `try_claim_fire` 原子性。启用门面的
`scheduling-cluster` feature 后可直接使用 `NadisFireLog`。

## 运行记录

`ExecutionRecorder` 接收 Started、Finished、Skipped 事件。实现必须同步、非阻塞，内部应该用 channel 或后台任务落库。

```rust
use nasa::scheduling::{ExecutionRecorder, RunEvent};

struct MetricsRecorder;

impl ExecutionRecorder for MetricsRecorder {
    fn record(&self, event: &RunEvent<'_>) {
        let _task = event.task;
        let _phase = &event.phase;
        // 只做内存计数或 try_send,不要在这里阻塞访问 DB。
    }
}
```

多个记录器可以用 `CompositeRecorder` 扇出：

```rust
use std::sync::Arc;
use nasa::scheduling::{CompositeRecorder, ExecutionRecorder, SchedulerOptions};

fn with_recorders(a: Arc<dyn ExecutionRecorder>, b: Arc<dyn ExecutionRecorder>) -> SchedulerOptions {
    SchedulerOptions::local()
        .with_recorder(Arc::new(CompositeRecorder::new(vec![a, b])))
}
```

## nadis 集成

开启门面的 `scheduling-cluster` feature 后，可使用 `NadisLeaderGate`、`NadisFireLog`、
`NadisExecutionRecorder` 和 `start_scheduled_clustered*`。

```toml
[dependencies]
nasa = { version = "1", features = ["scheduling-cluster"] }
```

```rust
use std::sync::Arc;
use nasa::scheduling::{
    start_scheduled_with, NadisFireLog, NadisLeaderGate, SchedulerOptions,
};

async fn start(
    client: Arc<nasa::redis::RedisClient>,
    leader: Arc<nasa::redis::Leader>,
) -> anyhow::Result<()> {
    let gate = Arc::new(NadisLeaderGate::new(leader));
    let fire_log = Arc::new(NadisFireLog::new(client, "myapp:scheduling:firelog"));

    let opts = SchedulerOptions::clustered_with_id("myapp:leader", gate)
        .with_fire_log(fire_log)
        .with_node_id("node-a");

    start_scheduled_with(opts).await
}
```

## 停机

`shutdown_scheduled()` 会 abort 非 cron 后台 loop、关闭 cron scheduler，并复位启动指纹。手工装配时
由唯一生命周期 owner 在优雅停机阶段调用；受管 `"scheduling"` 组件会自动执行。

```rust
nasa::scheduling::shutdown_scheduled().await?;
```

## YML 配置与使用

`nasched` 的任务声明来自属性宏。使用 `#[nasa::application("scheduling")]` 时，受管组件只接受下面
的稳定配置：

```yaml
scheduling:
  cluster: leader
  leader_key: lock:{order}:scheduler
  leader_period_ms: 1000
  node_id: node-a
```

字段合同：

| 键 | 说明 |
| --- | --- |
| `scheduling.cluster` | `local` 或 `leader`；`leader` 需要 `"redis"` 组件与 `scheduling-cluster` feature。 |
| `scheduling.leader_key` | leader 模式必填；同一调度组的副本必须一致。 |
| `scheduling.leader_period_ms` | 选主和在任检查周期，必须大于零。 |
| `scheduling.node_id` | 当前节点 ID；用于运行记录和排查。 |

自定义 gate、FireLog、recorder 和 Redis key 不属于受管 yml schema。需要这些扩展时，业务可定义
自己的配置投影，再显式构造 `SchedulerOptions`：

```rust
let lock = std::sync::Arc::new(nasa::redis::DistributedLock::new(redis.clone()));
let leader = std::sync::Arc::new(nasa::redis::Leader::elect(
    lock,
    &cfg.scheduler_extension.leader_lock_key,
    std::time::Duration::from_secs(1),
));
let gate = std::sync::Arc::new(nasa::scheduling::NadisLeaderGate::new(leader));
let fire_log = std::sync::Arc::new(nasa::scheduling::NadisFireLog::new(
    redis.clone(),
    &cfg.scheduler_extension.fire_log_key,
));

let opts = nasa::scheduling::SchedulerOptions::clustered_with_id(
    &cfg.scheduler_extension.gate_id,
    gate,
)
.with_fire_log(fire_log)
.with_node_id(&cfg.scheduler_extension.node_id);

nasa::scheduling::start_scheduled_with(opts).await?;
```

任务本身仍在代码里声明。需要按环境关闭某个 cron 任务时，把属性里的 `cron = "-"` 作为禁用值，或在业务函数开头读取配置自行短路。

## 主要边界

- `fixed_rate` 默认允许重叠；需要串行时使用 `fixed_delay` 或显式并发上限。
- leader gate 只决定本拍是否可执行，不替代业务幂等和 fencing。
- misfire 补漏只适用于声明了 `fire_once` 的 cron 任务。
- 调度器和后台任务必须由唯一 owner 显式停机，不能遗留 detached task。
