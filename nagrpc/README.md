# nagrpc

`nagrpc` 把 tonic 的 codegen 身份、service 装配、HTTP/2 listener、安全边界、健康、反射、指标和停机
收进一个稳定合同。业务只定义 proto、实现生成的 trait，并把生成的 server 登记给
`#[nasa::application("grpc")]`；业务不构造 tonic Router，不逐个 service 套消息限制，也不直接选择
`tonic`、`prost`、codec 或 build crate 的版本。

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "grpc"] }

[build-dependencies]
nagrpc-build = "1.0.0"
```

## 核心价值

直接使用 tonic 时，业务项目通常要同时维护 protobuf 编译器、tonic/prost 版本对齐、Router、health、
reflection、每个 generated service 的消息上限、TLS、listener、readiness、指标和 graceful shutdown。
这些工作一旦分散到每个服务，遗漏一个步骤就会形成不同的运行合同。

稳定接入把责任划分为：

| 业务负责 | `nagrpc` / `napp` 负责 |
| --- | --- |
| `.proto`、兼容基线和 RPC 业务语义 | vendored HOST `protoc`、tonic/prost/codec 单一身份、descriptor 与摘要 |
| 实现 generated service trait | Router、消息/连接/RPC/stream/HTTP/2 资源边界 |
| 在 UserHook 登记 generated server | health、可选 reflection、TLS/mTLS、readiness、发现 metadata、指标 |
| 决定方法授权和容量策略 | 在 handler 前执行身份、并发和速率门禁 |
| 独立模式下持有唯一 handle | Application 模式的绑定、监督、摘流、排空和反向停机 |

`nagrpc` 不替业务设计 proto，也不替代 service mesh、API gateway、Schema Registry、客户端负载均衡或
业务权限模型。它解决的是同一进程内 gRPC transport 与 generated service 的一致装配和唯一生命周期。

## 最小业务接入

协议文件仍属于业务项目：

```proto
syntax = "proto3";
package order.v1;

service OrderService {
  rpc GetOrder(GetOrderRequest) returns (GetOrderResponse);
}

message GetOrderRequest { string order_id = 1; }
message GetOrderResponse { string order_id = 1; string status = 2; }
```

`build.rs` 只有统一生成入口：

```rust
fn main() {
    nagrpc_build::compile("proto/order.proto").expect("订单 gRPC 协议必须生成成功");
}
```

业务模块包含生成代码并实现 trait：

```rust
pub mod proto {
    nasa::grpc::include_proto!("order.v1");
}

#[derive(Default)]
struct OrderApi;

#[nasa::grpc::async_trait]
impl proto::order_service_server::OrderService for OrderApi {
    async fn get_order(
        &self,
        request: nasa::grpc::Request<proto::GetOrderRequest>,
    ) -> Result<nasa::grpc::Response<proto::GetOrderResponse>, nasa::grpc::Status> {
        let order_id = request.into_inner().order_id;
        Ok(nasa::grpc::Response::new(proto::GetOrderResponse {
            order_id,
            status: "created".to_owned(),
        }))
    }
}
```

`main.rs` 的 UserHook 只登记 service：

```rust
#[nasa::application("grpc")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register_grpc_service(
        proto::order_service_server::OrderServiceServer::new(OrderApi),
    )?;
    Ok(())
}
```

登记动作没有网络副作用。Application 在 Prepare 封口 registry，等全部 initializer 成功后才在 Ready
阶段校验 descriptor 与方法策略、装配 health/reflection、绑定 listener 并开放 readiness。缺少业务
service、重复 service、codegen ABI 不一致、descriptor 冲突、未知方法策略或端口绑定失败都会在接流前
拒绝启动。`grpc.health_only: true` 是显式允许没有业务 service 的唯一例外。

### 为什么使用 `nasa::grpc::include_proto!`

`tonic::include_proto!` 只把一个生成的 `.rs` 文件包含进当前模块；它不知道 NASA 的 descriptor、摘要、
codegen ABI 和运行时门面。`nasa::grpc::include_proto!` 同时包含：

- 归一化到 `nasa::grpc::codegen` 的生成代码，业务无需直接依赖 tonic/prost；
- 与生成代码同一次构建得到的 `FILE_DESCRIPTOR_SET`；
- descriptor 的 `FILE_DESCRIPTOR_SHA256`；
- `nagrpc-build` 为 generated server 生成的 `ManagedGrpcService` 适配；
- 同次构建的规范 protobuf package 模块树；跨 package import 不要求业务手写嵌套 `mod`，well-known
  types 统一使用门面重导出的 `prost_types`。

这些元数据让 registry 能在 listener 开放前验证 service 名、完整方法路径、四种 streaming 形态、
descriptor 冲突和 ABI，并让 reflection、方法指标和方法策略使用同一份固定目录。因此它不是对 tonic
宏的无意义包裹；改回 `tonic::include_proto!` 会绕过受管 codegen 合同，而且业务又必须自行维护第三方
依赖和装配步骤。

## Application 受管模式

受管模式的唯一 owner 是 `"grpc"` 组件：

```text
UserHook 登记 generated service
  -> Prepare 封口 registry
  -> initializer 全部成功
  -> Ready 校验 descriptor/策略并装配 health、reflection、TLS
  -> 预绑定 listener，发布 readiness、observer 与发现 metadata
  -> 停机先从发现中心注销，再关闭新连接准入
  -> 两阶段 GOAWAY 排空，超时后终止 serve task
```

最小配置只需绑定地址；其它字段都有有界默认值：

```yaml
application:
  name: order-service

grpc:
  bind: 127.0.0.1:50051
  reflection:
    enabled: false
```

非 loopback 明文默认拒绝。仅在明确接受网络风险时设置 `allow_insecure_remote: true`；生产网络应使用
TLS 或 mTLS。Application 从同一代 secret 快照取得 PEM，普通配置只保存 locator：

```yaml
grpc:
  bind: 0.0.0.0:50051
  authority: grpc.order.internal
  tls:
    mode: mutual
    certificate: secret://grpc/server-cert
    private_key: secret://grpc/server-key
    client_ca: secret://grpc/client-ca
    certificate_warning_window_ms: 2592000000
    certificate_minimum_remaining_ms: 86400000
    clock_skew_ms: 300000
```

`authority` 是服务发现发布给 generated client 的 TLS DNS/IP identity，不含 scheme、端口或路径；省略时
resolver 回退到注册 IP。`mode` 只能是 `disabled`、`server` 或 `mutual`。private key 进入清零容器且不会出现在 Debug 或错误文本；
证书链、密钥匹配、serverAuth、有效期和 ALPN h2 在 bind 前校验。mTLS 验证通过后，请求 extension 中的
`PeerIdentity` 来自 client leaf certificate 的 SHA-256 指纹，不信任客户端自报 metadata。

### 方法级策略

方法键必须是 descriptor 中的完整路径。策略只能收紧全局边界，且在 handler 前执行：

```yaml
grpc:
  methods:
    /order.v1.OrderService/GetOrder:
      require_peer_identity: true
      max_inflight_rpcs: 64
      requests_per_second: 500
      burst: 1000
```

`require_peer_identity` 应与 `tls.mode: mutual` 组合。速率与 burst 必须同时配置，容量不足返回
`ResourceExhausted`，身份缺失返回 `Unauthenticated`；两者都不进入业务 handler。未知方法不会被静默
忽略，而是在端口绑定前阻止 Ready。

### 配置边界

`grpc` 根使用 `deny_unknown_fields`。常用配置分为：

| 类别 | 配置键 | 默认语义 |
| --- | --- | --- |
| listener | `bind`、`authority`、`allow_insecure_remote`、`max_connections` | loopback `127.0.0.1:50051`，最多 256 条受管连接 |
| RPC | `concurrency_limit_per_connection`、`unary_timeout_ms`、`max_inflight_rpcs` | 每连接 128，并按进程共享 1024 个 RPC permit |
| 消息 | `max_decoding_bytes`、`max_encoding_bytes`、`max_inflight_message_bytes` | 单消息 4 MiB，请求与响应共享 128 MiB 实际在途 permit；未发送的声明长度不预扣 |
| streaming | `stream_idle_timeout_ms`、`max_stream_duration_ms`、`max_*_messages_per_stream`、`max_*_stream_bytes` | 空闲、总时长、消息数和双向累计字节都有限 |
| HTTP/2 | `initial_*_window_size`、`max_frame_size`、`http2_*` | 限制窗口、header、HPACK、发送缓冲、reset 与控制帧速率 |
| TCP/连接 | `connection_handshake_timeout_ms`、`first_request_timeout_ms`、`idle_connection_timeout_ms`、`tcp_*` | 慢握手、空闲连接与失效对端不能无限占用资源 |
| 轮转/停机 | `max_connection_age_ms`、`connection_eviction_grace_ms`、`drain_timeout_ms` | 首请求/空闲/年龄驱逐使用逐连接 grace；停机把整个 drain 预算交给已接纳 RPC |
| 内存 | `managed_memory_budget_bytes` | 连接、stream、RPC 与消息预算必须共同落在受管内存门禁内 |
| 协议能力 | `health_only`、`reflection.enabled`、`methods` | health 自动装配；reflection 默认关闭；`health_only + reflection` 在 bind 前拒绝 |

零值、超过硬上限、内存预算不自洽或字段组合不合法都会在 bind 前失败。显式设置
`application.shutdown_timeout_ms` 时，它必须覆盖发现注销预留、gRPC drain 和 Application 收尾；省略时
容器会自动提高到最低安全预算。

## 服务发现

同时声明 `"nacos-discovery"` 时，gRPC listener 必须先 Ready，随后注册组件才发布实际端口。Web 与
gRPC 共存时，实例主端口保持 Web 端口，gRPC 端点只通过固定 metadata 发布：

| metadata | 含义 |
| --- | --- |
| `nasa.grpc.protocol` | 固定为 `grpc` |
| `nasa.grpc.port` | listener 实际端口，支持 `bind: ...:0` |
| `nasa.grpc.tls_mode` | `disabled`、`server` 或 `mutual` |
| `nasa.grpc.authority` | TLS authority；没有显式 authority 时为空 |

调用方使用 `NacosDiscoveryHandle::grpc_endpoint(service)` 取得
`GrpcDiscoveredEndpoint`，不要把 REST 主端口猜成 gRPC 端口。停机顺序先注销发现实例，再停止 gRPC
准入，避免把新流量导向正在排空的 listener。

## 指标与结局

Application 把固定 descriptor 目录写入统一 Prometheus/OTLP 指标中心：

| family | labels | 含义 |
| --- | --- | --- |
| `napp_grpc_serving` | 无 | listener 是否 Running 并开放准入 |
| `napp_grpc_connections_active` | 无 | 当前受管连接数 |
| `napp_grpc_connections_accepted_total` | 无 | listener 累计受理连接数 |
| `napp_grpc_accept_failures_total` | 无 | accept 累计失败数 |
| `napp_grpc_accept_consecutive_failures` | 无 | 最近成功 accept 后的连续失败数 |
| `napp_grpc_accept_stall_seconds` | 无 | 当前连续失败持续时间 |
| `napp_grpc_tls_certificate_expiry_timestamp_seconds` | 无 | 已发布证书链最早到期时间；明文模式不产生样本 |
| `napp_grpc_rpcs_active` | `service,method,rpc_type` | 当前在途 RPC |
| `napp_grpc_rpcs_started_total` | `service,method,rpc_type` | 已接纳 RPC 总数 |
| `napp_grpc_rpcs_rejected_total` | `service,method,rpc_type,reason` | handler 前按连接/进程/方法容量、方法速率或身份门禁分类的拒绝总数 |
| `napp_grpc_rpcs_completed_total` | `service,method,rpc_type,outcome` | 每个已接纳 RPC 的唯一最终结局 |

`rpc_type` 是 `unary`、`client_streaming`、`server_streaming` 或 `bidirectional_streaming`。
`reason` 使用固定集合：`connection_concurrency`、`process_concurrency`、`method_concurrency`、
`method_rate`、`peer_identity`。`outcome` 使用固定集合：`ok`、标准 gRPC code、`cancelled`、
`client_deadline_exceeded`、`server_timeout`、`server_stream_duration`、`server_stream_idle`、
`stream_received_messages`、`stream_sent_messages`、`stream_received_bytes`、`stream_sent_bytes`、
`transport_lost`。请求与响应 body 共享首个结局槽，请求方向越界不会被响应侧通用 status 覆盖；只有
响应 owner 归还 RPC permit、递减 active，因此每个 started 调用最终只进入一个完成结局。

Application 在 bind 前按 sealed service/method 目录、五类拒绝原因和全部完成结局计算最坏公开序列数。
gRPC 最多预留 20,000 条序列，并与 `nametrics-core` 的进程级 100,000 条预算原子登记；容量不足时
descriptor、指标源和预留计数都保持原状，listener 不会启动。

`max_inflight_message_bytes` 约束进程内实际进入 body、跨 frame 拼接或等待下游消费的消息字节，不是
单条 stream 的累计流量上限。未发送 payload 的 length prefix 不预留整条消息；完整消息被下游消费后
释放 permit，长流累计流量只受双方向 `max_*_messages_per_stream` 与 `max_*_stream_bytes` 约束。压缩消息
在完整到达、即将交给 codec 前按解压上限取得保守权重。

持续 accept 失败会把 `grpc:listener` 摘为 NotReady，但 serve task 继续持有 socket 并有界退避；资源
恢复并成功 accept 后自动恢复 Ready。证书进入 warning window 时 readiness 为 Degraded，到期后为
NotReady。serve 所有权丢失或生命周期进入 Failed 会触发 Application 统一停机。

独立模式没有 Application 指标目录，调用方应以一次 `GrpcServerObserver::snapshot()` 读取连接事实，并
用 `rpc_snapshot()` 读取固定方法目录；分别读取多个 getter 可能跨越状态迁移。

## 独立 listener

不使用 `#[nasa::application]` 的程序可显式持有唯一 handle：

```rust
use std::net::SocketAddr;

let handle = nasa::grpc::ServerPlan::new()
    .add_service(proto::order_service_server::OrderServiceServer::new(OrderApi))?
    .reflection(false)
    .start("127.0.0.1:50051".parse::<SocketAddr>()?)
    .await?;

let observer = handle.observer();
// 进程收到自己的停机信号后：
handle.shutdown().await?;
```

独立 TLS 通过 `GrpcTlsIdentity::server(...)` 或 `GrpcTlsIdentity::mutual(...)` 提交已解析 PEM；调用方负责
secret owner 和证书轮换。正常停机必须等待 `shutdown().await`，同步 Drop 只负责停止准入和终止任务的
异常兜底。独立模式和 Application 模式不能共同拥有同一个 listener。

## 明确边界

- 单个 Application 组件只拥有一个 listener；一个 listener 可登记最多 64 个业务 service、256 个方法。
- gRPC-Web、HTTP/1.1 fallback、多 listener、客户端连接池、客户端负载均衡和 service mesh 不在本合同内。
- reflection 默认关闭；开启后以 sealed generated service registry 作为编译期 full-name allowlist。
  `ListServices` 只列出实际装配的业务 service，descriptor 中未登记的相邻 service 也不能通过 symbol
  查询旁路暴露；业务不手工登记 descriptor。
- health 自动提供 transport 存活与 service 名目录，不替业务判断数据库、下游或业务数据是否健康。
- `GrpcTlsAcceptorSource` 可为每次新握手提供一份已校验的 TLS 快照；来源负责原子发布与旧信任根期限。
  Application 的 Saga 安全快照结合 `nacos-config` 提供证书热轮换与有界重叠窗口，非法候选保留当前材料。
  既有连接不会重新握手；Saga 会在每个 RPC 上复验证书 principal 的当前授权，通用业务需自行定义撤权语义。
- 兼容门禁保护 protobuf wire 与 RPC cardinality，不替业务判断字段语义、授权范围或数据保留政策。
- drain 先关闭新准入，再发送两阶段 GOAWAY，并让已接纳调用使用整个 `drain_timeout_ms`；首请求、空闲
  和连接年龄驱逐才使用 `connection_eviction_grace_ms`。预算耗尽会终止 serve task，不遗留 detached listener。
