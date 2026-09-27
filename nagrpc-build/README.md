# nagrpc-build

`nagrpc-build` 是 NASA Rust gRPC 协议的唯一 codegen 入口。它把 HOST `protoc`、tonic/prost/codec
身份、descriptor、SHA-256 摘要、generated service 适配和兼容门禁冻结为同一构建合同，使业务项目
不需要直接选择 `tonic-build`、`tonic-prost-build`、`prost-build` 或系统 `protoc`。

## 核心价值

业务只声明自己的 `.proto`、include 根和兼容 baseline；`nagrpc-build` 负责把协议编译成可被
`nasa::grpc` 直接包含、可登记到受管 listener、并能在发布前比较 wire/RPC 兼容性的完整产物。
它消除构建机 `protoc` 差异、tonic/prost 类型身份分裂、跨 package Rust 模块缺口，以及业务逐个
server 手写运行时适配的重复工作。

| 协议所有者负责 | `nagrpc-build` 负责 |
| --- | --- |
| `.proto` 的业务语义、package/service 身份与字段演进 | vendored HOST `protoc`、固定 codegen ABI 与规范运行时路径 |
| 纳入归档的 imports 和已批准 baseline | 完整 descriptor、内容摘要与 protobuf/RPC 兼容门禁 |
| 选择生成 client、server 或二者 | generated service 的 `ManagedGrpcService` 适配 |
| 对不兼容演进安排新身份和调用方迁移 | well-known types 映射与跨 package 的规范 Rust 模块树 |

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["grpc"] }

[build-dependencies]
nagrpc-build = "1.0.0"
```

最小 `build.rs`：

```rust
fn main() {
    nagrpc_build::compile("proto/order.proto").expect("订单 gRPC 协议必须生成成功");
}
```

运行时从业务模块包含同一次构建的全部产物：

```rust
pub mod proto {
    nasa::grpc::include_proto!("order.v1");
}
```

package 字符串必须与 `.proto` 的 `package` 完整一致。该宏同时提供生成类型、
`FILE_DESCRIPTOR_SET` 和 `FILE_DESCRIPTOR_SHA256`；server 生成物自动实现 `ManagedGrpcService`，可直接
登记到 Application 或独立 `ServerPlan`。

## 构建架构

一次 `compile` 按固定顺序执行：

1. 规范化 proto、include 和可选 baseline 路径；不存在或类型不符时立即失败。
2. 选择当前构建 HOST 对应的 vendored `protoc`，不读取目标架构，也不在构建期间下载工具。
3. 生成 client、server 与完整 descriptor set，并把 prost、tonic 和 codec 路径指向
   `nasa::grpc::codegen`。
4. 校验 package、service、method 和 descriptor 的规模上限。
5. 存在 baseline 时比较 protobuf wire 与 RPC cardinality 合同。
6. 只对实际生成 Rust 源码的业务 package 写入代码、descriptor 与 SHA-256，并追加受管 server 适配；
   `google.protobuf` 等映射到 `prost_types` 的 package 只保留在完整 descriptor 中。
7. 为同一次构建生成规范 package 模块树，使跨 package 的 `super::` 相对类型路径在业务只调用一次
   `include_proto!` 时仍解析到唯一 Rust 类型身份。

所有生成物只写 Cargo `OUT_DIR`：

| 产物 | 用途 |
| --- | --- |
| `<package>.rs` | `include_proto!` 使用的入口包装；重导出入口 package 并包含规范 package 模块树 |
| `<package>.generated.rs` | 归一化后的 message/client/server 与 `ManagedGrpcService` 适配；由入口包装内部包含 |
| `<package>.descriptor.bin` | reflection、service/method 目录与运行时冲突校验 |
| `<package>.descriptor.sha256` | 发布归档、部署和诊断可比较的协议内容身份 |

业务不应提交或手工改写 `OUT_DIR`。需要发布的源 proto、imports、兼容 baseline、README 和许可证必须
由协议 crate 自己纳入归档；codegen 不会从工作区外隐式复制这些公开合同。

协议可以直接导入 `google/protobuf/timestamp.proto`、`duration.proto`、`any.proto`、`struct.proto`、
`field_mask.proto` 等 well-known types；生成字段统一引用 `nasa::grpc::codegen::prost_types`。跨 package
import 也无需业务手写 Rust 模块树：入口 package 的 `include_proto!` 会包含同次构建的规范树，并把入口
package 重新导出到当前业务模块。确需显式构造被导入类型时，可从生成的
`__nagrpc_packages::<完整 package 路径>` 访问；它与入口类型字段使用的是同一身份。

## 多协议与生成选择

多个根协议使用 `Builder`：

```rust
fn main() {
    nagrpc_build::Builder::new()
        .compile(
            &["proto/order.proto", "proto/payment.proto"],
            &["proto"],
        )
        .expect("业务 gRPC 协议必须生成成功");
}
```

默认同时生成 client 和 server。纯服务端协议可以 `.build_client(false)`；纯客户端协议可以
`.build_server(false)`，此时不会生成 `ManagedGrpcService` 适配。`.runtime_path(...)` 仅供直接依赖
`nagrpc` 或 Cargo 重命名场景使用，必须是绝对 Rust 模块路径；业务经 `nasa` 门面时保持默认值。

单次构建最多处理 256 个 proto 文件、64 个 service、256 个 method，每个 service 最多 128 个 method，
descriptor 最大 16 MiB。规模越界会使构建失败，不会生成一个运行期才部分登记的协议集合。

## Descriptor 兼容门禁

协议所有者可以保存一个已批准 descriptor，并在每次生成时比较：

```rust
fn main() {
    nagrpc_build::Builder::new()
        .descriptor_baseline("proto/order.baseline.bin")
        .compile(&["proto/order.proto"], &["proto"])
        .expect("订单 gRPC 协议必须保持兼容");
}
```

baseline 应随协议 crate 归档并由协议所有者更新，只能使用可进入归档的 crate 相对路径，不能指向
个人机器绝对路径或仓库外文件。门禁递归索引
跨文件和 nested message/enum/service 的全限定 symbol，拒绝：

- 删除既有 message、enum、service 或 RPC method；
- 用同一字段号替换字段名、类型、label、oneof 或 presence；
- 删除字段却没有 reserve 原名称或编号；
- 复用 enum 数字，或删除 enum 值却没有 reserve 原名称或数字；
- 改变 RPC input/output 类型；
- 在 unary、client streaming、server streaming 与 bidirectional streaming 之间改变 cardinality。

失败返回稳定的 `GRPC_COMPAT_*` 分类，错误展示不包含本机绝对路径或 proto 内容。独立协议流水线也可
直接调用 `check_descriptor_compatibility(baseline, candidate)`，与 `Builder` 使用同一规则。

路径、生成、规模或兼容检查任一阶段失败时，本次调用不形成可用协议集合；调用方必须让 `build.rs`
失败，不能继续使用残缺或上一次构建遗留的输出。面向构建日志的错误展示保持脱敏，完整原因链可通过
标准 `Error::source` 在受控本地环境诊断。

这是 wire 和 RPC 形态门禁，不判断字段业务语义、授权范围、默认值解释、跨字段约束或数据迁移策略。
新增字段仍须遵守 protobuf 的兼容设计；有意做不兼容变更时应使用新的 package/service 身份并由调用方
显式迁移，而不是删除 baseline。

## 可复现性与边界

- vendored `protoc` 按 HOST 选择，支持交叉编译时在构建机运行；构建期间不访问网络。
- 生成代码只引用 NASA 运行时门面，避免业务依赖图出现两份不兼容的 tonic/prost 类型身份。
- `CODEGEN_ABI` 同时写入 generated adapter 和运行时；主合同不一致时在编译期或登记期拒绝。
- descriptor 摘要标识生成输入的规范 descriptor 内容，不是业务 API 版本号，也不替代兼容检查。
- 生成器不提供 proto registry、远端 schema 拉取、breaking-change 审批或发布编排。
- 发布前必须从 `cargo package` 的最终归档确认 proto、imports、baseline 和 README 实际随包交付。
