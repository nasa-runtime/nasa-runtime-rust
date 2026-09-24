# nabudget

`nabudget` 是 provider-neutral 的绝对请求预算与取消合同。标准接线覆盖 Web handler/响应体、REST、
DB acquire 与显式只读操作、Redis 只读 helper 和 gRPC deadline 桥接，避免每层重新开始超时。

业务通常从门面已有模块取得 `RequestBudget`：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["rest-discovery"] }
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

## 调用链架构

```text
入口绝对 deadline 与取消令牌 → discovery / acquire → 请求与重试 → 完整响应正文
                                  各阶段共享剩余预算，不重新计时
```

子预算只能收紧父期限。Web handler 返回并不代表响应已经结束，因此预算延续到正文完成、出错或
丢弃。gRPC 将剩余时长写入协议 metadata，本地取消仍须显式等待；跨进程停止执行没有隐含保证。
数据库写与 COMMIT 不能被普通读取超时包装改写为确定失败，未知结果仍须按业务持久事实对账。

## YML 配置

本 crate 不读取 yml。总预算由入口组件或业务配置转换为 `Duration`，下游只接收已经构造好的绝对预算。

```yaml
server:
  request_deadline_ms: 2000
```

字段由 Web 组件解释，不应让每个 adapter 再定义一套独立总超时。

## 主要边界

- deadline 使用单调时钟，不可序列化，也不用于跨进程传输。
- `cancel()` 会取消当前预算及其子预算；子预算不能反向取消父预算。
- 超过一年会收敛到框架硬上限，避免单调时钟溢出。
- 重试等待、`Retry-After` 和 discovery 都必须消耗同一预算。
- `run` 在调用准入和等待中区分取消与到期；`cancel_on_drop` 将当前任务的释放传给预算。
- Web 正文结束、错误或丢弃会取消预算。响应头前到期返回 504，响应头后只能中止正文。
- gRPC `Deadline::request_budget` 保留原截止点；`propagate_request_budget` 与 `call_with_budget`
  分别负责下游 metadata 和本地取消，不承诺跨进程取消树。
- 只读语义由调用入口明确约定，不按 SQL 前缀推断；事务写、COMMIT、XADD 不由通用预算包装改写结果。
- 本地停止等待不证明远端未执行，也不授权自动重放。
