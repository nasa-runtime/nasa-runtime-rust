# nabudget

`nabudget` 是 provider-neutral 的绝对请求预算与取消合同。它让入口、服务发现、重试和单次 I/O
共享同一个 deadline，避免每层重新开始超时后让总耗时突破入口 SLA。

业务通常从门面已有模块取得 `RequestBudget`：

```toml
[dependencies]
nasa = { version = "1", features = ["rest-discovery"] }
```

```rust
use std::time::Duration;
use nasa::discovery::rest::RequestBudget;

let budget = RequestBudget::from_now(Duration::from_secs(2));
let response = nasa::discovery::rest::RestDiscovery::get()
    .get("lb://inventory/items/42")
    .budget(&budget)
    .send_json::<Item>()
    .await?;
```

子调用用 `budget.child(maximum)` 收紧局部上限，但不能延长父 deadline。等待 I/O 前用
`operation_timeout(maximum)` 取得剩余预算；返回 `None` 表示预算已经耗尽。

## YML 配置

本 crate 不读取 yml。总预算由入口组件或业务配置转换为 `Duration`，下游只接收已经构造好的绝对预算。

```yaml
server:
  request_timeout_ms: 2000
```

字段由 Web 组件解释，不应让每个 adapter 再定义一套独立总超时。

## 主要边界

- deadline 使用单调时钟，不可序列化，也不用于跨进程传输。
- `cancel()` 会取消当前预算及其子预算；子预算不能反向取消父预算。
- 超过一年会收敛到框架硬上限，避免单调时钟溢出。
- 重试等待、`Retry-After` 和 discovery 都必须消耗同一预算。
