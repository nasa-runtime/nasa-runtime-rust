# nasa

`nasa` 是 `nasa-runtime-rust` 的唯一业务门面。应用只依赖本 crate，通过 feature 选择能力，再从
`nasa::<module>` 使用稳定入口；实现 crate 和宏 crate 由门面按需引入。

本 crate 属于独立开源项目，与美国国家航空航天局不存在隶属、赞助、认可或官方项目关系；完整
声明随包交付于 `NOTICE`。

```toml
[dependencies]
nasa = { version = "1", features = [
    "application",
    "tx",
    "mapper",
    "redis",
    "cache",
    "web",
] }
```

```rust
use nasa::mapper::{Mapper, Query};
use nasa::tx::transactional;

#[Mapper]
trait OrderMapper {
    #[Query("select id from orders where id = #{id}")]
    async fn find_id(&self, id: i64) -> sqlx::Result<Option<i64>>;
}

#[transactional]
async fn save_order() -> anyhow::Result<()> {
    Ok(())
}
```

## 应用入口

`application` feature 提供声明式入口。组件字符串可以任意书写；宏会去重校验后按唯一规范顺序
`log -> nacos-config -> telemetry -> db -> redis -> cache -> kafka -> auth -> web -> ws ->
nacos-discovery -> scheduling` 启动，并严格反序停机。

```rust
#[nasa::application("web", "cache", "redis", "log")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register(MyService::new())?;
    Ok(())
}
```

声明组件时必须启用对应 feature。`auth` 必须与 `web` 同时声明；`hystrix`、`grafana`、`mapper`、
`openapi` 等是函数级或门面能力，不是组件字符串。完整生命周期合同见
[napp README](../napp/README.md)。

## Feature 总表

默认 feature 为空。只开启业务实际使用的能力：

| feature | 业务入口 | 说明 |
| --- | --- | --- |
| `application` | `nasa::application`、`nasa::Application` | 生命周期、配置快照、资源和受管任务 |
| `tx` | `nasa::tx` | ambient MySQL 事务和 `#[transactional]` |
| `mapper` | `nasa::mapper` | 声明式 SQL Mapper，蕴含 `tx` |
| `mapper-redis-cache` | `nasa::mapper` | Mapper Redis Hash L2 |
| `mapper-cache-grouped` | `nasa::mapper` | Mapper 接 `GroupedCache` |
| `inbox` | `nasa::inbox` | 与业务 MySQL 副作用同事务的消费去重 |
| `outbox` | `nasa::outbox` | 与业务写同事务的事件落库和顺序投递 |
| `idempotency` | `nasa::idempotency` | Provider-neutral 幂等状态机和进程内 store |
| `idempotency-mysql` / `idempotency-redis` | `nasa::idempotency` | MySQL 强一致或 Redis response-cache 后端 |
| `audit` | `nasa::audit` | 与业务写同事务的 Outbox 审计 |
| `openapi` | `nasa::openapi` | 确定性 OpenAPI 3.1 合同 |
| `redis` | `nasa::redis` | Redis 命令、pipeline、stream、lock |
| `redis-search` / `redis-derive` | `nasa::redis` | 搜索封装和文档派生 |
| `cache` | `nasa::cache` | 两级缓存、失效广播和缓存宏 |
| `kafka` | `nasa::kafka` | 发布、消费、路由、确认和健康 |
| `kafka-tls` / `kafka-gssapi` / `kafka-zstd` | `nasa::kafka` | Kafka 传输安全与压缩子能力 |
| `kafka-schema-registry` | `nasa::kafka` | 实验性 schema adapter，不进入 `full` |
| `hystrix` | `nasa::hystrix` | 并发隔离、超时和 Dashboard 流 |
| `grafana` | `nasa::grafana` | 接口隔离、Prometheus 指标和面板 |
| `telemetry` | `nasa::application` | 受管 span 队列、OTLP/HTTP 导出和停机 flush |
| `web` | `nasa::web` | 路由宏、interceptor 和 HTTP 运行时 |
| `web-auth` | `nasa::web::auth` | 路由身份合同 |
| `web-crypto` | `nasa::web::crypto` | 双协议密码处理和重放保护 |
| `web-crypto-legacy-rsa` | `nasa::web::crypto` | 受控迁移的 legacy RSA 私钥路径，不进入 `full` |
| `web-security` | `nasa::web` | 身份、解密、重放、handler、加密固定流水线 |
| `oauth` | `nasa::oauth` | JWT、JWKS 与授权服务器 metadata |
| `secret` | `nasa::secret` | secret 分片、快照和两阶段轮换 |
| `secret-http` / `secret-vault` | `nasa::secret` | TLS client 和 KV v2 provider |
| `object-store-experimental` | `nasa::object` | 实验性有界对象存储合同，不进入 `full` |
| `grpc-experimental` | `nasa::grpc` | 实验性独立 gRPC listener，不进入 `full` |
| `scheduling` | `nasa::scheduling` | 异步与定时任务 |
| `scheduling-cluster` | `nasa::scheduling` | Redis leader gate 和集群调度 |
| `partition` | `nasa::partition` | 同 key 串行、有界分区执行器 |
| `ws` | `nasa::ws` | TCP/WebSocket 长连接 |
| `ws-redis` / `ws-socketio` / `ws-kafka` | `nasa::ws` | 长连接集群与协议子能力 |
| `log` | `nasa::log` | tracing、滚动文件和级别热切 |
| `yml` | `nasa::yml` | 分层 YAML、overlay、环境变量和占位符 |
| `config-boot` / `nacos-config` | `nasa::yml::nacos` | 配置中心引导与应用组件桥 |
| `discovery` | `nasa::discovery` | 静态/DNS/provider-neutral 发现合同 |
| `nacos` / `nacos-sdk` | `nasa::config::nacos`、`nasa::discovery::nacos` | API 层与真实传输层 |
| `rest-discovery` | `nasa::discovery::rest` | 服务发现 REST 负载均衡 |
| `rest-discovery-nacos` / `nacos-discovery` | `nasa::discovery` | 注册发现装配与应用组件桥 |
| `rest-client` / `rest-client-nacos` | `nasa::discovery::rest` | 声明式 REST client |
| `base` / `crypto` / `numeric` / `date` / `image` | 对应同名模块 | 基础类型和纯工具 |
| `crypto-legacy-rsa` | `nasa::crypto` | 受控迁移的 RSA 私钥兼容入口，不进入 `full` |
| `full` | 上述稳定能力的组合 | 非默认；不包含实验能力 |

`nacos` 和 `rest-discovery-nacos` 只保证 API 可编译；真正连接后端必须同时启用 `nacos-sdk`。
实验能力已有资源上限和显式生命周期，但在两个真实业务项目形成共同合同之前不承诺稳定 API。

## YML 配置与使用

门面本身不读取 yml。声明式应用要求 `zcf/application.yml` 存在，内容可为 `{}`；具体根节点由对应
组件读取。

```yaml
application:
  name: order-service
  mode: service
  shutdown_timeout_ms: 20000

database:
  url: ${APP_MYSQL_URL}
  max_connections: 16

redis:
  url: ${APP_REDIS_URL}

cache:
  mode: two_level
  redis_ref: default

server:
  host: 0.0.0.0
  port: 8080
```

| 根节点 | 负责组件 |
| --- | --- |
| `application` | `napp` |
| `log` | `nalog` |
| `nacos` | `config-boot` / `nanacos` |
| `telemetry` | `napp` telemetry 组件 |
| `database` / `datasources` | `natx` / `namigrate` / `namapper` |
| `redis` | `nadis` |
| `cache` | `cacheable` 受管组件 |
| `kafka` / `kafkas` | `nafka` 受管组件 |
| `auth` | OAuth/JWKS 认证组件 |
| `server` | Web 组件 |
| `ws` | `naws` |
| `rest_discovery` | 注册发现组件 |
| `scheduling` | `nasched` |

## 主要边界

- `nasa` 只做模块组织与重导出，不承载业务状态，也不提供全量 prelude。
- feature 是编译期能力；组件字符串是运行期生命周期 owner，两者不能混用。
- `full` 不应设为默认，也不能用来隐式启用实验能力。
- 宏会识别门面被 Cargo 重命名的情况；业务无需直接依赖宏实现 crate。
- 具体失败语义、配置默认值和资源上限以各组件 README 为准。
