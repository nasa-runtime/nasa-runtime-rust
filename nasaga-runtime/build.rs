/// 业务作用：在启用 gRPC transport 时生成框架自带的 command/result 收据协议与受管 service adapter。
///
/// 参数说明: 无。
///
/// 返回：feature 未启用时不产生协议输出；启用后生成失败会终止构建，禁止交付半份 transport。
fn main() {
    if std::env::var_os("CARGO_FEATURE_GRPC_TRANSPORT").is_none() {
        return;
    }
    nagrpc_build::Builder::new()
        .runtime_path("::nagrpc")
        .compile(&["proto/saga_transport.proto"], &["proto"])
        .expect("Saga gRPC transport 协议必须生成成功");
}
