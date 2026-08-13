# nagrpc

`nagrpc` 是实验性 gRPC listener 生命周期层。它提供只接受 HTTP/2 的有界 server builder、严格
连接 permit、预绑定 listener、health/reflection 门面、只读观察句柄和有预算的 graceful drain；
业务 proto 和 generated service 仍归业务项目。与 `nasa` 的 `application` 组合时，listener 可由
`#[nasa::application("grpc")]` 统一托管。

```toml
[dependencies]
nasa = { version = "1", features = ["grpc-experimental"] }
```

## 运行架构与所有权

listener 有两种互斥的所有权模式，二者共用同一份 `GrpcServerConfig`、连接准入与排空合同：

| 模式 | 唯一 shutdown owner | 启动顺序 | 观测入口 |
| --- | --- | --- | --- |
| 独立模式 | 业务持有的 `GrpcServerHandle` | 校验配置 → 构造 Router → 预绑定 → 启动 serve | `GrpcServerObserver::snapshot()` |
| Application 受管模式 | `"grpc"` 组件 | UserHook 提交工厂 → Prepare 封口 → initializer 完成 → Ready 构造并绑定 | `Application::grpc()` 与 `napp_grpc_*` |

连接 permit 必须在 `accept()` 之前取得，因此配置的 `max_connections` 同时约束已经受理和等待交给
tonic 的连接，不会先接受 socket 再等待容量。每个 generated service 仍必须显式应用消息编解码上限；
tonic 的 codec 位于 service 内，transport builder 无法替业务隐式补上这条门禁。

独立模式和受管模式都先停止新连接准入，再等待 tonic 处理中的 RPC 排空。排空超时会终止 serve task
并发布 `Failed`，不会遗留 detached listener；同步 `Drop` 只承担异常兜底，正常停机必须等待异步
shutdown。health 与 reflection 只是显式可装配的 adapter，不会默认开放，业务 service、proto 兼容、
TLS 身份和方法级授权仍由应用负责。

## 初始化与使用

```rust
use std::net::SocketAddr;
use nasa::grpc::{health, reflection, GrpcServerConfig};

let config = GrpcServerConfig::default();
let (_reporter, health_service) = health::server::health_reporter();
let reflection_service = reflection::server::Builder::configure()
    .build_v1()?;

let router = config
    .server_builder()?
    .add_service(nasa::grpc::apply_message_limits!(
        config.message_limits,
        health_service
    ))
    .add_service(nasa::grpc::apply_message_limits!(
        config.message_limits,
        reflection_service
    ))
    .add_service(nasa::grpc::apply_message_limits!(
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

```toml
[dependencies]
nasa = { version = "1", features = ["application", "grpc-experimental"] }
```

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

## 观测

受管模式下 listener 的接流事实进入 Application 的唯一指标目录，随 Prometheus 文本端点与 OTLP
指标导出同时发布，无需业务另接观测面。全部为无 label 族，基数与监听数一致，不随连接数或对端增长：

| family | 类型 | 含义 |
| --- | --- | --- |
| `napp_grpc_serving` | gauge | listener 是否处于 `Running` 并对新连接开放准入（1/0） |
| `napp_grpc_connections_active` | gauge | 已交给 tonic 且尚未关闭的连接数 |
| `napp_grpc_connections_accepted_total` | counter | 进入 listener 所有权的连接总数 |
| `napp_grpc_accept_failures_total` | counter | `accept` 返回错误的累计次数 |
| `napp_grpc_accept_consecutive_failures` | gauge | 最近一次成功 accept 之后的连续失败次数 |
| `napp_grpc_accept_stall_seconds` | gauge | 当前连续失败已持续的秒数，无失败段时为 0 |

`accepted_total` 与 `connections_active` 一起区分"没有流量"和"接不进来"：前者不动而后者为零
表示外部没有建连，两者都不动但 `accept_failures_total` 在涨则表示本机资源不足以受理。
`accept_stall_seconds` 越过受管摘流阈值时 `grpc:listener` readiness 会进入 NotReady，但 serve
任务仍持有 listener 并继续退避重试，资源恢复后自行回到 Ready。

独立模式（不声明 `"grpc"` 组件）没有容器指标目录，同一组事实由
`GrpcServerObserver::snapshot()` 一次性返回，调用方自行接入自己的观测面；逐个 getter 分别读取
会得到互相矛盾的组合，导出面必须使用单次快照。

## 成熟度与边界

- 本能力和 `"grpc"` 组件字符串都是实验 API，不进入 `full`；稳定合同等待真实业务使用收敛。
- 当前合同稳定覆盖单 listener、HTTP/2、连接/stream/RPC/消息上限、health/reflection 显式装配、
  accept 失败恢复与有预算排空；它不是 service mesh、API gateway 或 proto registry。
- 受管模式由 Application 独占 shutdown；独立模式由 `GrpcServerHandle` 独占 shutdown。
- drain 超时会 abort serve task，不遗留 detached listener。
- `accept()` 遇到文件描述符压力或连接建立期瞬态错误时会释放本轮 permit、有界退避并继续持有
  listener；资源恢复后自动接流，不把瞬时错误记成干净关闭。
- `Drop` 只能执行取消和 abort 兜底，不能替代显式的异步排空。
- `GrpcServerObserver` 只暴露地址、状态、在途连接数、累计/连续 accept 失败数和连续失败时长，
  不开放取消权。生命周期 `Running` 表示 serve 任务仍持有 listener；持续接流健康应结合
  `accept_failure_duration()` 判断，成功 accept 后连续计数与时长归零。
- reflection 是否开放、业务 service 健康状态、TLS 和 proto 兼容门禁由业务负责。
- 多 listener 编排、证书热轮换、客户端连接池、负载均衡、限流策略和方法级鉴权不在本 crate 合同内。
- 连接、单消息、每连接并发、stream 数和所有 duration 都有硬上限，零值或越界配置会被拒绝。
