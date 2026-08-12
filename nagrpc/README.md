# nagrpc

`nagrpc` 是实验性 gRPC listener 生命周期层。它提供只接受 HTTP/2 的有界 server builder、严格
连接 permit、预绑定 listener、health/reflection 门面、只读观察句柄和有预算的 graceful drain；
业务 proto 和 generated service 仍归业务项目。与 `nasa` 的 `application` 组合时，listener 可由
`#[nasa::application("grpc")]` 统一托管。

```toml
[dependencies]
nagrpc = "1"
```

## 初始化与使用

```rust
use std::net::SocketAddr;
use nagrpc::{health, reflection, GrpcServerConfig};

let config = GrpcServerConfig::default();
let (_reporter, health_service) = health::server::health_reporter();
let reflection_service = reflection::server::Builder::configure()
    .build_v1()?;

let router = config
    .server_builder()?
    .add_service(nagrpc::apply_message_limits!(
        config.message_limits,
        health_service
    ))
    .add_service(nagrpc::apply_message_limits!(
        config.message_limits,
        reflection_service
    ))
    .add_service(nagrpc::apply_message_limits!(
        config.message_limits,
        my_service
    ));

let handle = config
    .start(router, "127.0.0.1:50051".parse::<SocketAddr>()?)
    .await?;

// 停机 owner：
handle.shutdown().await?;
```

generated service 必须通过 `apply_message_limits!` 同时应用 `config.message_limits` 的编码/解码
上限；漏掉这一步属于装配错误。tonic 把 codec 放在 generated service 内，server transport 没有
等价的全局消息上限开关。

## Application 受管模式

启用 `application` 与 `grpc-experimental` 后，业务在 UserHook 提交只含 Router 工厂的计划。工厂
直到全部 initializer 成功后的 Ready 阶段才执行；Application 负责绑定、关键任务监督、readiness、
反向停机和全局预算。工厂必须使用收到的同一份配置建立 builder 并应用消息上限。

```rust
use nasa::application::{
    ApplicationError, ApplicationPhase, ComponentId, GrpcApplicationPlan,
};

#[nasa::application("grpc")]
async fn main(application: nasa::Application) -> anyhow::Result<()> {
    application.configure_grpc(GrpcApplicationPlan::new(|config| {
        let (_reporter, health_service) = nasa::grpc::health::server::health_reporter();
        let router = config
            .server_builder()
            .map_err(|error| ApplicationError::with_source(
                ComponentId::Grpc,
                ApplicationPhase::Ready,
                "gRPC Router configuration was rejected",
                error,
            ))?
            .add_service(nasa::grpc::apply_message_limits!(
                config.message_limits,
                health_service
            ));
        Ok(router)
    }))?;
    Ok(())
}
```

受管组件读取固定 `grpc` 配置根：

```yaml
grpc:
  bind: 127.0.0.1:50051
  max_connections: 1024
  concurrency_limit_per_connection: 256
  request_timeout_ms: 30000
  keepalive_interval_ms: 30000
  keepalive_timeout_ms: 10000
  max_concurrent_streams: 256
  drain_timeout_ms: 20000
  max_decoding_bytes: 4194304
  max_encoding_bytes: 4194304
```

| 键 | `GrpcServerConfig` 默认值 | 约束 |
| --- | --- | --- |
| `bind` | `127.0.0.1:50051` | IP socket 地址；域名解析不属于启动期隐式行为 |
| `max_connections` | `1024` | `1..=1000000`，先取 permit 再 accept |
| `concurrency_limit_per_connection` | `256` | `1..=65535` |
| `request_timeout_ms` | `30000` | 大于 0，最长一年 |
| `keepalive_interval_ms` | `30000` | 大于 0，最长一年 |
| `keepalive_timeout_ms` | `10000` | 大于 0，最长一年 |
| `max_concurrent_streams` | `256` | 大于 0 |
| `drain_timeout_ms` | `20000` | 大于 0，最长一年 |
| `max_decoding_bytes` | `4194304` | `1..=67108864` |
| `max_encoding_bytes` | `4194304` | `1..=67108864` |

## 成熟度与边界

- 本能力和 `"grpc"` 组件字符串都是实验 API，不进入 `full`；稳定合同等待真实业务使用收敛。
- 受管模式由 Application 独占 shutdown；独立模式由 `GrpcServerHandle` 独占 shutdown。
- drain 超时会 abort serve task，不遗留 detached listener。
- `accept()` 遇到文件描述符压力或连接建立期瞬态错误时会释放本轮 permit、有界退避并继续持有
  listener；资源恢复后自动接流，不把瞬时错误记成干净关闭。
- `Drop` 只能执行取消和 abort 兜底，不能替代显式的异步排空。
- `GrpcServerObserver` 只暴露地址、状态、在途连接数、累计/连续 accept 失败数和连续失败时长，
  不开放取消权。生命周期 `Running` 表示 serve 任务仍持有 listener；持续接流健康应结合
  `accept_failure_duration()` 判断，成功 accept 后连续计数与时长归零。
- reflection 是否开放、业务 service 健康状态、TLS 和 proto 兼容门禁由业务负责。
- 连接、单消息、每连接并发、stream 数和所有 duration 都有硬上限，零值或越界配置会被拒绝。
