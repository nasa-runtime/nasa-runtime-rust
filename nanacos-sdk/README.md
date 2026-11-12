# nanacos-sdk

Nacos 配置中心与注册中心的兼容传输 SDK，供 `nanacos` 的旧单 Nacos 接口使用。保持连接身份、临时实例注册和订阅在会话重连时一致；双向控制流建立失败或结束后撤销连接健康状态，后续请求由既有重连状态机重新建立会话。

此组件源自 [nacos-sdk 0.8.3](https://crates.io/crates/nacos-sdk/0.8.3)，保留原 SDK 的公开类型与 feature 名称，Rust 库名为 `nacos_sdk`。原始实现由 nacos-group 及其贡献者提供，使用 Apache-2.0 许可证；来源与版权见 [NOTICE](NOTICE) 和 [LICENSE](LICENSE)。它是独立维护的派生组件，不代表上游发行版。

## 接入与能力边界

业务通常通过 `nanacos` 或 NASA 门面启用 `nacos-sdk` feature，无需直接依赖本包。新多注册中心 owner 使用 `nanacos::managed` 的 HTTP adapter，不能和此 SDK 同时监督同一个注册目标。

直接使用时保留原 SDK 的库名称：

```toml
[dependencies]
nacos-sdk = { package = "nanacos-sdk", version = "1.0.1" }
```

默认启用 `config`、`naming` 和 `auth-by-http`。可选 `tls` 提供 TLS 通道，`auth-by-aliyun` 提供 RAM 签名认证，`tracing-log` 安装 SDK 日志 subscriber。连接参数仍使用 `api::props::ClientProps`，并通过 `ConfigServiceBuilder` 或 `NamingServiceBuilder` 构造对应服务；不同凭据和 namespace 使用独立服务实例。

## 会话与资源归属

配置和 Naming 服务各自持有 SDK 会话。框架的 qualifier 由上层连接目录解释，
不是此 SDK 的连接参数；一个 SDK 会话不会根据 Mapping 属性切换 namespace 或凭据。

控制流失效后，旧连接不能继续提供有效会话身份。连接状态机按既有退避策略重新连接，Naming 注册与订阅记录按固定业务身份重新发布，不额外建立第二套心跳或订阅调度器。网络已经提交的请求仍可能到达服务端；重连不提供数据库 fencing，也不能撤销远端已经发生的业务效果。

鉴权失败、请求失败和连接变化通过 SDK 返回值及 tracing 日志观察。注册 guard 的显式注销与应用停机预算由 `nanacos` 和宿主承担；崩溃、不可达注销和端点复用的所有权限制仍然存在。

## 磁盘缓存

缓存写入在访问时等待目录准备，并为每次尝试排他创建随机临时文件。完整内容同步到磁盘后才原子替换目标文件；同键、不同扩展名的键和多个存储实例不共享临时文件。普通 I/O 失败有界重试，并清理本次持有的临时文件；取消或进程崩溃可留下临时文件，加载时跳过这些文件。并发缓存更新不提供业务版本排序，磁盘缓存也不替代服务端当前配置与注册资格。

English documentation: [README.en.md](README.en.md).

模块与 namespace 使用固定 JSON 元组计算 SHA-256 子目录，逻辑键使用 SHA-256 文件名。配置的 dataId、group、namespace 使用可逆 JSON 数组身份，分隔符、Unicode、绝对路径形式和 `.tmp` 结尾都只是业务字符。记录中保存并复验原始作用域和键；损坏、文件名不符、其它作用域和符号链接均不加载、不覆盖、不删除。

根目录由部署者独占管理；SDK 在访问时规范化根目录，拒绝作用域目录和记录的符号链接，并在替换或删除前复验目录边界。该策略防止业务名称路径穿越，不隔离拥有根目录写权限的本机恶意进程；此类部署需要操作系统权限或容器卷隔离。旧的原文件名缓存布局不自动加载或迁移，升级后从 Nacos 重新取得内容；旧文件保留给部署者处理。磁盘故障只影响缓存，不能回滚内存中的撤销结果或作为离线可用性的证明。

## License

Apache-2.0。
