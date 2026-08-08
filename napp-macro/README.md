# napp-macro

`#[nasa::application(...)]` 属性宏的实现 crate。业务项目不直接依赖它，经 `nasa` 门面的 `application`
feature 使用；运行时语义见 [napp](../napp/README.md) 与 [运维指南](../docs/operations.md)。

宏把业务的异步 `main` 改写为统一进程入口：

- 生成静态 `ApplicationSpec`（组件声明顺序、编译期缺省应用名）并调用 `napp::run`，真实 `main` 返回 `std::process::ExitCode`。
- 声明 `"web"` 时在 crate 根自动生成 `mvc_router!(nasa::Application)` 收集端，并把业务 crate 内 nominal 的路由项投影为运行时稳定的 `RouteMeta`；业务不得再手写 `mvc_router!`（会因 `crate::__mvc` 重复定义编译失败）。
- 业务 `main` 变成启动 Hook：零参数或接收一个 `Application`，返回 `anyhow::Result<()>`；成功返回后资源封存，运行由 Runner 接管。

编译期校验（与运行期同口径，先在宏上失败）：

- 组件白名单：`log`、`nacos-config`、`telemetry`、`db`、`redis`、`cache`、`saga`、`kafka`、
  `outbox`、`auth`、`web`、`ws`、`nacos-discovery`、`scheduling`；未知或重复组件拒绝。
- 业务可按任意顺序书写；宏固定规范为 `log -> nacos-config -> telemetry -> db -> redis ->
  cache -> saga -> kafka -> outbox -> auth -> web -> ws -> nacos-discovery -> scheduling`。
- `saga` 隐式加入 DB 与 Outbox；独立 `outbox` 隐式加入 DB。Inbox 是事务内原语，没有组件字符串；
  Kafka 或其它 transport 不由 Saga 推断。
- 隐式依赖只补齐缺项；显式同时声明 `saga`、`db`、`outbox` 与只声明 `saga` 生成同一组件图。
- 组合约束：`auth` 必须和 `web` 同时声明；其余依赖关系由运行期根据最终配置继续校验。
- 每个声明组件都会生成 feature 探测常量引用，能力未启用时在业务 crate 编译阶段直接失败。
- 入口契约：必须是 crate 根的 `async fn main`，非泛型、至多一个 `Application` 参数、返回
  `anyhow::Result<()>`；生成的类型门禁同时要求 Hook future 为 `Send + 'static`。入口不能再叠加
  `#[tokio::main]` 或 `#[EnableScheduling]` / `#[EnableAsync]`，因为 Application 已拥有 runtime 与调度
  生命周期。
- 生成 crate 根锚点模块：属性放错位置时错误直接指向宏调用处。

宏内路径解析复用 `macro-support`（直接依赖优先、门面回退、Cargo 重命名兼容）。

## 使用示例

```toml
[dependencies]
nasa = { version = "1", features = ["application", "log", "redis", "cache", "web"] }
```

```rust
#[nasa::application("web", "cache", "redis", "log")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

虽然源码按 `web, cache, redis, log` 书写，生成的规范启动顺序仍是 `log -> redis -> cache -> web`。

## YML 配置与边界

本宏不读取 yml；它只生成 `ApplicationSpec`。`zcf/application.yml` 和各组件配置由 `napp` 运行时读取。

- 属性只能放在 crate 根异步 `main` 上。
- 声明 `"web"` 后宏会生成唯一的路由收集模块，业务不能再手工生成同名收集器。
- Hook 返回成功后资源登记入口封口，运行期不能继续修改组件图。
- feature 缺失、重复组件、未知组件和非法组合都在编译期拒绝。
