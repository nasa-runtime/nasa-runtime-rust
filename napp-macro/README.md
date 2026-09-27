# napp-macro

`#[nasa::application(...)]` 与 `#[nasa::initializer(...)]` 属性宏的实现 crate。业务项目不直接依赖它，
经 `nasa` 门面的 `application` feature 使用。initializer 会在 migration 和出站依赖准备完成后、
入站能力开放前参与三轮全局初始化屏障；运行时语义见
[napp](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/napp/README.md) 与
[运维指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/docs/operations.md)。

宏把业务的异步 `main` 改写为统一进程入口：

- 生成静态 `ApplicationSpec`（组件声明顺序、编译期缺省应用名）并调用 `napp::run`，真实 `main` 返回 `std::process::ExitCode`。
- 声明 `"web"` 时在 crate 根自动生成 `mvc_router!(nasa::Application)` 收集端，并把业务 crate 内 nominal 的路由项投影为运行时稳定的 `RouteMeta`；业务不得再手写 `mvc_router!`（会因 `crate::__mvc` 重复定义编译失败）。
- 业务 `main` 变成启动 Hook：零参数或接收一个 `Application`，返回 `anyhow::Result<()>`；成功返回后资源封存，运行由 Runner 接管。
- 完整 `Initialization` trait impl 可被登记为静态 initializer，与 Service 启动 Hook 动态登记项合并
  为同一份冻结依赖计划。

编译期校验（与运行期同口径，先在宏上失败）：

- 组件白名单：`log`、`nacos-config`、`telemetry`、`db`、`redis`、`cache`、`partition`、`saga`、
  `kafka`、`outbox`、`redis-job`、`grpc`、`auth`、`web`、`ws`、`nacos-discovery`、`scheduling`；未知或
  重复组件拒绝。
- 业务可按任意顺序书写；宏固定规范为 `log -> nacos-config -> telemetry -> db -> redis ->
  cache -> partition -> saga -> kafka -> outbox -> redis-job -> grpc -> auth -> web -> ws ->
  nacos-discovery -> scheduling`。
- `saga` 隐式加入 DB 与 Outbox；独立 `outbox` 隐式加入 DB。Inbox 是事务内原语，没有组件字符串；
  `redis-job` 隐式加入 Redis；Kafka 或其它 transport 不由 Saga 推断。
- 隐式依赖只补齐缺项；显式同时声明 `saga`、`db`、`outbox` 与只声明 `saga` 生成同一组件图。
- 组合约束：`auth` 必须和 `web` 同时声明；其余依赖关系由运行期根据最终配置继续校验。
- 每个声明组件都会生成 feature 探测常量引用，能力未启用时在业务 crate 编译阶段直接失败。
- 入口契约：必须是 crate 根的 `async fn main`，非泛型、至多一个 `Application` 参数、返回
  `anyhow::Result<()>`；生成的类型门禁同时要求 Hook future 为 `Send + 'static`。入口不能再叠加
  `#[tokio::main]` 或 `#[EnableScheduling]` / `#[EnableAsync]`，因为 Application 已拥有 runtime 与调度
  生命周期。
- 生成 crate 根锚点模块：属性放错位置时错误直接指向宏调用处。
- initializer 只能标注安全、正向、非泛型的完整 `Initialization` trait impl；固有 impl、单个方法、
  `unsafe`、负向 impl 与其它 trait 都会在编译期拒绝。

宏内路径解析复用 `macro-support`（直接依赖优先、门面回退、Cargo 重命名兼容）。

## 受管单源与多源边界

宏只声明生命周期组件，不解析连接参数。MySQL、PostgreSQL、Redis 与 Kafka 的单源或多源配置由 `napp` 在启动期
从最终 YAML 创建并冻结；业务 Hook 只能取得受管句柄或提交 publisher、consumer、Handler 与 Saga
定义等业务计划，不能借宏属性建立第二张连接表。

| 资源 | 单源根 | 多源根 | 命名选择 |
| --- | --- | --- | --- |
| MySQL | `database` | `datasources.<name>` | Application getter、`datasource_ref` 与具名持久适配器 |
| PostgreSQL | `database` | `datasources.<name>` | `pg_datasource` getter、`datasource_ref` 与 PostgreSQL 持久适配器 |
| Redis | 扁平 `redis` | `redis.properties.<qualifier>` | Application getter 与 `redis_ref` |
| Kafka | `kafka` | `kafkas.<client>` | Application getter、consumer/producer client name |

同类资源的单源根与多源根互斥，引用未知名称会在 Ready 前拒绝，不会回退到默认或唯一实例。完整字段、
同源事务要求和停机边界见
[napp 的单源与多源章节](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/napp/README.md#yaml-创建单源与多源)。

Saga 的 managed 角色与 datasource 由运行时读取最终配置，宏不推断默认库。可靠 client 的 append
和 dispatcher 使用同一 `saga.client.datasource_ref`；显式 `outbox.datasource_ref` 冲突在 Ready 前
拒绝。宏只生成组件图，不能绕过运行期同源门禁。

## 使用示例

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["application", "log", "redis", "cache", "web"] }
```

```rust
#[nasa::application("web", "cache", "redis", "log")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

虽然源码按 `web, cache, redis, log` 书写，生成的规范启动顺序仍是 `log -> redis -> cache -> web`。

## 业务优雅停机任务

生成的启动 Hook 接收同一个 `Application`，可直接调用
`app.register_graceful_shutdown(priority, name, future)`；不需要增加宏属性、组件字符串或 feature。
登记只在 Service 或 Batch 的 UserHook 开放，Hook 结束后名称和任务集合封口。

宏不执行停机 future，也不另外生成信号处理器。Runner 在受监督任务收口后、UserHook 业务资源
释放前执行任务；Service 的 initializer 先清理，Batch 的静态 initializer 则在业务资源之后清理。
数值较小的 priority 先执行，同优先级按登记顺序执行。任务返回 `()` 或
`Result<(), E>`，其中 `E: Into<anyhow::Error>`；错误、超时和可隔离展开只记为次要失败。
初始化失败时，已经成功登记的任务仍沿统一停机路径处理；预算耗尽的未开始项不再 poll。
直接取消 Runner 不等同于请求优雅停机，析构隔离也不保证执行异步收尾。
此时实例退出 Ready 并撤销新资源借用和本实例全局入口；经过任务门时仍存活的受监督 future 保留后续
清理所有权，最后一个 future 析构后才释放依赖。保留 Application 副本不会重新开放入口。

完整名称、数量、共享期限和资源所有权约束见
[业务优雅停机任务](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/napp/README.md#业务优雅停机任务)。

## `#[nasa::initializer]`

属性入口适合无需在启动 Hook 中手工构造的静态 initializer。省略 `name` 时，宏从实现类型名派生
canonical kebab-case；省略 `order` 时使用 `100000`。数值越小越先执行，但 `requires` 依赖边始终
优先；同一可执行集合再按名称稳定裁决。

```rust
#[derive(Default)]
struct SchemaInitialization;

#[nasa::initializer(name = "schema", order = 300)]
impl nasa::application::Initialization for SchemaInitialization {}

#[derive(Default)]
struct RoutesInitialization;

#[nasa::initializer(order = 200, requires = ["schema"])]
impl nasa::application::Initialization for RoutesInitialization {
    fn initialize<'a>(
        &'a mut self,
        _context: &'a mut nasa::application::InitializationContext<'_>,
    ) -> nasa::application::ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
```

可选 `factory = path` 接收 `Application` 并异步返回 `ApplicationResult<Option<T>>`；`None` 表示当前配置
不启用该项，`Err` 阻止 Ready。`kind = "one-shot"` 只执行有界初始化；`"hosted"` 仅允许 Service，
可暂存 Ready 后才激活的长期任务和 readiness。Runner 在 `Prepare` 后严格执行全部 `before`、全部
`initialize`、全部 `after`，三轮成功后才进入 `Seal` 与 `Ready`。
Hosted 任务与组件终端、受管 Redis 消费和派生发送、出站 Client 共用启动许可；关键本地资源、
健康证据和启动期限复验通过后才发布 Ready。initializer 可保存已装配的发送句柄，统一放行前调用
会被拒绝；UserHook 中普通 `spawn_background` / `spawn_critical` 不隐式等待该许可。

派生名称也是依赖、日志和指标 label 使用的稳定身份；实现类型重命名会改变该身份，需要跨发布保持
连续性时应显式填写 `name`。initializer 失败、panic、启动超时或取消都会阻止入站能力开放，并进入
统一逆序清理；已经提交到外部系统的事实不会被本地清理撤销，业务实现必须保证可安全重跑。

## YML 配置与边界

本宏不读取 yml；它只生成 `ApplicationSpec`。`zcf/application.yml` 和各组件配置由 `napp` 运行时读取。

- 属性只能放在 crate 根异步 `main` 上。
- 声明 `"web"` 后宏会生成唯一的路由收集模块，业务不能再手工生成同名收集器。
- Hook 返回成功后资源登记入口封口，运行期不能继续修改组件图。
- feature 缺失、重复组件、未知组件和非法组合都在编译期拒绝。
- initializer 依赖缺失、重复名称、条件禁用后仍被依赖或依赖环由运行时在调用工厂前拒绝。
