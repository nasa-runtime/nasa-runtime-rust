# nagrpc

`nagrpc` 是实验性 gRPC listener 生命周期层。它提供只接受 HTTP/2 的有界 server builder、预绑定
listener、health/reflection 门面、状态查询和有预算的 graceful drain；业务 proto 和 generated
service 仍归业务项目。

```toml
[dependencies]
nasa = { version = "1", features = ["grpc-experimental"] }
```

## 初始化与使用

```rust
use std::net::SocketAddr;
use nasa::grpc::{health, reflection, GrpcServerConfig, GrpcServerHandle};

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

let handle = GrpcServerHandle::start(
    router,
    "127.0.0.1:50051".parse::<SocketAddr>()?,
    config.drain_timeout,
)
.await?;

// 停机 owner：
handle.shutdown().await?;
```

generated service 必须通过 `apply_message_limits!` 同时应用 `config.message_limits` 的编码/解码
上限；漏掉这一步属于装配错误。tonic 把 codec 放在 generated service 内，server transport 没有
等价的全局消息上限开关。

## YML 配置

当前没有 `"grpc"` 应用组件字符串，也没有由 `napp` 读取的固定配置根。业务可以把非敏感 transport
参数反序列化为自己的配置，再构造 `GrpcServerConfig`。

```yaml
grpc:
  bind: 0.0.0.0:50051
  request_timeout_ms: 30000
  drain_timeout_ms: 20000
  max_decoding_bytes: 4194304
  max_encoding_bytes: 4194304
```

该形状是业务投影，不是稳定框架 schema。

## 成熟度与边界

- 本能力是实验 API，不进入 `full`；稳定组件合同等待两个真实业务项目收敛。
- `GrpcServerHandle` 是唯一 shutdown owner；正常路径必须显式 `shutdown()`。
- drain 超时会 abort serve task，不遗留 detached listener。
- reflection 是否开放、业务 service 健康状态、TLS 和 proto 兼容门禁由业务负责。
- 单消息、并发、stream 数和所有 duration 都有硬上限，零值或越界配置会被拒绝。
