# napart

`napart` 是同 key 串行执行器。它保证相同 key 的任务按提交顺序串行执行，不同 key 分散到多个分区并发执行。适合订单、账户、用户维度的异步事件处理。

直接依赖：

```toml
[dependencies]
napart = "1"
```

## 基本使用

```rust
use napart::{PartitionExecutor, SubmitError};

async fn run() -> Result<(), SubmitError> {
    let executor = PartitionExecutor::with_partitions(16);

    executor.submit("user:1001", || async {
        // 同一个 user key 下的任务会串行。
    })?;

    executor.submit("user:1002", || async {
        // 不同 key 可能并发。
    })?;

    executor.shutdown().await;
    Ok(())
}
```

## 有界队列和背压

适合保护内存，避免提交速度远超消费速度。

```rust
let executor = napart::PartitionExecutor::with_partitions_and_capacity(32, 1024);

match executor.submit("order:1", || async {}) {
    Ok(()) => {}
    Err(napart::SubmitError::QueueFull) => {
        // 调用方可选择重试、降级或返回 429。
    }
    Err(e) => return Err(e),
}
```

`submit_async` 会等待容量，适合内部后台生产者使用真背压：

```rust
executor.submit_async("order:1", || async {}).await?;
```

## panic 隔离和停机

worker 会隔离单个任务 panic，避免一个坏任务打掉整个执行器。`shutdown().await` 会停止接收新任务，并尽量排空已提交任务。

```rust
executor.shutdown().await;
```

## 选型建议

- 需要同 key 顺序一致时，用 `napart`。
- 只需要普通并发，不关心 key 顺序时，直接用 `tokio::spawn` 或普通队列。
- 提交入口要处理 `SubmitError`，不要把满队列当成成功。

## 行为边界

- 同 key 严格 FIFO:乱耗时任务仍按提交序完成;不同 key 按分区并发。
- `submit`/`submit_sync` 非阻塞,队满返回 `QueueFull`;`submit_async` 等待容量,**永不** QueueFull。
- **再入死锁警告**:不要在本执行器的任务内部对**同一(已满)分区**调用 `submit_async(...).await`——任务等容量、worker 等任务完成,互等死锁。任务内派生工作请用非阻塞 `submit`(满则快速失败)或提交到不同 key / 独立执行器。
- 单任务 panic 被隔离:不杀 worker、不影响同分区后续任务、不计入 `dead_partitions`。
- `shutdown` 语义:正常路径等全部已准入任务跑完再返回;`stop_timeout` 超时会 abort 残余 worker **并连带取消在途任务**,返回后不再有本执行器任务在后台运行。
- `is_healthy()` = 仍在运行且无死分区;`shutdown` 之后恒为 false(设计如此,监控口径注意)。

## YML 配置与使用

`napart` 不固定 yml 根节点。推荐应用按业务队列定义 `partition_executor:`，启动时构造 `PartitionExecutor`。

完整示例：

```yaml
partition_executor:
  partitions: 64
  queue_capacity: 1024
  shutdown_timeout_ms: 5000
  full_policy: reject
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `partitions` | `2 × CPU` 后向上取 2 的幂 | 显式值也会至少取 1 并向上取 2 的幂。 |
| `queue_capacity` | `65536` | 每个分区的有界容量；最小按 1 处理，满时 `submit` 返回错误。 |
| `shutdown_timeout_ms` | `2000` | 排空与 join 的总等待上限；超时会 abort 残余 worker 和在途任务。 |
| `full_policy` | `reject` | 满队列策略；本组件返回 `QueueFull`，重试/降级由业务决定。 |

启动代码：

```rust
use std::time::Duration;

let executor = napart::PartitionExecutor::with_partitions_and_capacity(
    cfg.partition_executor.partitions,
    cfg.partition_executor.queue_capacity,
)
.with_stop_timeout(Duration::from_millis(
    cfg.partition_executor.shutdown_timeout_ms,
));
```

同 key 的任务必须使用稳定 key，例如 `order:{id}`、`account:{id}`。不要把随机值、时间戳或请求 ID 放进 key，否则会破坏串行语义。
