# natelemetry

`natelemetry` 提供 W3C Trace Context、子 span、非阻塞有界导出队列和停机 flush。它不依赖
`napp`，也不绑定具体遥测 SDK；`napp` 的 `"telemetry"` 组件负责日志或 OTLP/HTTP sink 生命周期。

```toml
[dependencies]
nasa = { version = "1", features = ["application", "telemetry", "web"] }
```

```rust
#[nasa::application("telemetry", "web")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

Web 入口自动生成 Server span。业务要记录子操作时，复用请求扩展中的 `TraceContext`：

```rust
use nasa::application::TraceContext;

fn record_lookup(app: &nasa::Application, parent: &TraceContext) {
    app.record_span("inventory.lookup", parent);
}
```

REST/Kafka 等领域组件会在需要向下游传播时使用 `SpanRecorder` 派生 Client、Producer 或 Consumer
span。低层集成方直接依赖本 crate 时，guard 未显式 `finish` 也会在 Drop 提交一次无状态码 span，
覆盖提前返回和取消路径。

## YML 配置

```yaml
telemetry:
  enabled: true
  service_name: order-service
  queue_capacity: 2048
  otlp_endpoint: http://127.0.0.1:4318/v1/traces
  otlp_encoding: protobuf
  root_sample_ratio: 0.25
```

未配置 `otlp_endpoint` 时使用结构化日志 sink。编码可选 `json` 或 `protobuf`。
`root_sample_ratio` 只裁决没有上游 `traceparent` 的新根链路，范围为 `0.0..=1.0`；已有上游
上下文始终沿用其 sampled 位，未采样 span 只传播且不计入 dropped。

## 主要边界

- 请求路径只做 `try_send`；队列满或关闭时丢弃并计数，不反向阻塞业务。
- `BoundedSpanExporter::channel` 的非 fallible 容量参数收敛到 `1..=Semaphore::MAX_PERMITS`，
  零值或极端值不会让 Tokio channel 构造 panic。
- span 名必须低基数，不能包含用户 ID、对象 ID 或完整 URL。
- `TraceContext::parse_traceparent` 对非法或全零 ID 返回 `None`。
- 根采样率在 exporter 发布前冻结；运行中不会因半更新配置让同一批请求使用两套采样规则。
- 停机 flush 使用统一剩余预算；超时后把未导出数量计入 dropped 并继续退出。
- `ExporterSnapshot` 只暴露 pending/dropped，不暴露 endpoint 或业务属性。
