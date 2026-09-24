# natelemetry

`natelemetry` 提供 W3C Trace Context、子 span、非阻塞有界导出队列和停机 flush。它不依赖
`napp`，也不绑定具体遥测 SDK；`napp` 的 `"telemetry"` 组件负责日志或 OTLP/HTTP sink 生命周期。

## 核心价值与传播架构

核心价值是把传播权与采样权分开：合法上游 `traceparent` 的 flags 原样继承；没有合法上游时，只有
已安装 exporter 的 sampler 可以决定新根 sampled 位。仅具备传播能力的入口创建未采样根，因此不会
替下游宣布全量采样。已采样 span 经有界队列异步导出，队列满或停机预算耗尽只增加 dropped，不阻塞
业务请求。

```text
合法 traceparent ──继承 flags──┐
                               ├─ TraceContext ─→ child span ─→ 有界 exporter
缺失/非法 ── exporter sampler ─┘                         └─→ REST/Kafka header
缺失/非法且无 exporter ── 未采样根 ──────────────────────────→ 只传播
```

本 crate 不建立全局 tracer provider，不热切根采样规则，也不把 payload、凭据或无界业务标识写入
span。OTLP 编码、HTTP sink 和生命周期由 `napp` 受管组件装配。

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "telemetry", "web"] }
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

没有上游上下文的后台执行应使用 `SpanRecorder::start_root`，由 exporter 冻结的 sampler 决定
sampled 位。该入口可携带已脱敏、有界的字符串属性；调度运行时用它记录稳定任务名和名义触发时刻。
只有传播能力、没有 exporter 的入口不能替下游声明已采样，应建立未采样根并继续传播。

## 环境(task-local)trace 上下文

`ambient()` / `with_ambient(context, future)` / `with_ambient_sync` 提供进程内"当前链路"的
task-local 载体(26 字节 Copy 值，无分配无锁)：入口层(HTTP 中间件、Kafka 消费、napart 任务、
`#[Async]`/`#[scheduled]`)建立作用域，出站层(REST 客户端、Kafka producer)在业务未显式绑定时
回退读取环境值——显式绑定始终优先，漏穿线的调用点不再静默断链。

语义合同：作用域随 future 生命周期，嵌套内层覆盖、离开恢复，跨 `.await` 稳定；显式
`with_ambient(None, ..)` 是清空而不是未设置；`tokio::spawn` **不隐式继承**——跨任务延续必须
显式捕获再重建作用域，这是有意设计，隐式跨任务继承会让"哪些后台工作挂在本请求链路上"变得
不可审计。

## YML 配置

```yaml
telemetry:
  enabled: true
  service_name: order-service
  service_instance_id: order-service-az1-01
  queue_capacity: 2048
  otlp_endpoint: http://127.0.0.1:4318/v1/traces
  otlp_metrics_endpoint: http://127.0.0.1:4318/v1/metrics
  metrics_interval_ms: 10000
  otlp_encoding: protobuf
  root_sample_ratio: 0.25
```

未配置 `otlp_endpoint` 时使用结构化日志 sink；未配置 `otlp_metrics_endpoint` 时
指标 OTLP 出口关闭，Prometheus 文本不受影响。指标快照来自 `nametrics-core`，实际 HTTP
导出与停机管理由 `napp` 完成，不在本 crate 内建立第二份 registry。编码可选 `json` 或
`protobuf`。
`root_sample_ratio` 只裁决没有上游 `traceparent` 的新根链路，范围为 `0.0..=1.0`；已有上游
上下文始终沿用其 sampled 位，未采样 span 只传播且不计入 dropped。

## 主要边界

Application 的 telemetry owner 在业务停机任务和业务资源收口后执行最终 flush；业务不应再用
`register_graceful_shutdown` 提前关闭同一 exporter。最终导出仍受剩余全局预算约束，不能因安排了
收尾任务就宣称所有 span 必然送达远端。

- 请求路径只做 `try_send`；队列满或关闭时丢弃并计数，不反向阻塞业务。
- `BoundedSpanExporter::channel` 的非 fallible 容量参数收敛到 `1..=Semaphore::MAX_PERMITS`，
  零值或极端值不会让 Tokio channel 构造 panic。
- span 名必须低基数，不能包含用户 ID、对象 ID 或完整 URL。
- span 属性名必须稳定，值必须有界且不得包含凭据、请求正文或无界对象内容。
- `TraceContext::parse_traceparent` 对非法或全零 ID 返回 `None`。
- 根采样率在 exporter 发布前冻结；运行中不会因半更新配置让同一批请求使用两套采样规则。
- 停机 flush 使用统一剩余预算；超时后把未导出数量计入 dropped 并继续退出。
- `ExporterSnapshot` 只暴露 pending/dropped，不暴露 endpoint 或业务属性。
