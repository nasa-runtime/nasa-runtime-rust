# nanacos-sdk

Nacos 配置中心与注册中心的兼容传输 SDK，供 `nanacos` 的旧单 Nacos 接口使用。保持连接身份、临时实例注册和订阅在会话重连时一致；双向控制流建立失败或结束后撤销连接健康状态，后续请求由既有重连状态机重新建立会话。

此组件源自 [nacos-sdk 0.8.3](https://crates.io/crates/nacos-sdk/0.8.3)，保留原 SDK 的公开类型与 feature 名称，Rust 库名为 `nacos_sdk`。原始实现由 nacos-group 及其贡献者提供，使用 Apache-2.0 许可证；来源与版权见 [NOTICE](NOTICE) 和 [LICENSE](LICENSE)。它是独立维护的派生组件，不代表上游发行版。

## 接入与能力边界

业务通常通过 `nanacos` 或 NASA 门面启用 `nacos-sdk` feature，无需直接依赖本包。新多注册中心 owner 使用 `nanacos::managed` 的 HTTP adapter，不能和此 SDK 同时监督同一个注册目标。

直接使用时保留原 SDK 的库名称：

```toml
[dependencies]
nacos-sdk = { package = "nanacos-sdk", version = "1.0.0" }
```

默认启用 `config`、`naming` 和 `auth-by-http`。可选 `tls` 提供 TLS 通道，`auth-by-aliyun` 提供 RAM 签名认证，`tracing-log` 安装 SDK 日志 subscriber。连接参数仍使用 `api::props::ClientProps`，并通过 `ConfigServiceBuilder` 或 `NamingServiceBuilder` 构造对应服务；不同凭据和 namespace 使用独立服务实例。

## 会话与资源归属

配置和 Naming 服务各自持有 SDK 会话。框架的 qualifier 由上层连接目录解释，
不是此 SDK 的连接参数；一个 SDK 会话不会根据 Mapping 属性切换 namespace 或凭据。

控制流失效后，旧连接不能继续提供有效会话身份。连接状态机按既有退避策略重新连接，Naming 注册与订阅记录按固定业务身份重新发布，不额外建立第二套心跳或订阅调度器。网络已经提交的请求仍可能到达服务端；重连不提供数据库 fencing，也不能撤销远端已经发生的业务效果。

鉴权失败、请求失败和连接变化通过 SDK 返回值及 tracing 日志观察。注册 guard 的显式注销与应用停机预算由 `nanacos` 和宿主承担；崩溃、不可达注销和端点复用的所有权限制仍然存在。

## License

Apache-2.0。
