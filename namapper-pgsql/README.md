# namapper-pgsql

`namapper-pgsql` 提供 PostgreSQL 声明式 Mapper 运行时，使用 `$1..$n` prepared 占位符，并接入
`natx-pgsql` 的命名 datasource 与 ambient transaction。
所有生成方法默认采集调用与数据库原子指标，区分连接等待、缓存命中、真实执行与流消费；
受管应用通过 YAML 开启开发 SQL/参数日志、慢操作告警、异步通知和指标出口。

## SQL 观测

同时启用 `nasa` 的 `application` 与 `mapper-pgsql` 时，观测 source、连接选项、通知 worker 和指标
出口自动进入 Application 生命周期。所有可调策略均有 YAML 入口，非必要项递归补齐默认值。
逐条 SQL 与参数默认关闭；参数只允许显式开发环境，敏感名字强制脱敏，未知类型用占位。

MySQL/PostgreSQL 使用同一固定词表、指标桶和通知规则，但 driver 与数据源身份严格区分。
SQLx 错误在擦除前分类；通知失败不改变 SQL 返回、事务结果或数据库 readiness。
完整默认配置与边界见 [后端中立 SQL 观测](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/namapper-core/README.md#sql-观测与配置)。

### 阈值通知与执行边界

宏冻结方法身份，运行时先经过事务与连接门禁，再围绕真实 SQLx 调用记录耗时。逻辑方法包含缓存
处理，数据库执行不包含连接等待；Stream 只把底层活跃取行时间用于慢阈值，消费者处理另计生命周期。
缓存命中和未 poll 的流不伪造数据库调用。

业务通过 `nasa::application::notifications::init` 安装 `Notify`，配置
`sql.observability.slow_sql.threshold_ms`、`alerts.slow_sql.enabled=true` 与
`alerts.slow_sql.cooldown_ms=0`，即可让每次原始耗时达到或超过阈值的 SQL 尝试入队。
默认阈值为 1000 ms，通知冷却为 60000 ms；慢日志开关独立。未安装实现则忽略，队列满、下游失败
或停机可能丢弃通知。发送发生于受管 worker，通知微服务的协议由业务适配器负责。

## SQL 与返回能力

它支持静态 SQL、`if`/`choose`/`foreach`/`trim` 动态 SQL、列表 bind、`RETURNING`、分页、白名单排序、
流式结果、`EnumOrdinal`、JSON 和 L2 cache。占位符只由结构化 bind 节点产生；字符串、quoted identifier、
块注释和 PostgreSQL JSON 操作符中的 `?` 保持原文。

## 使用

```toml
[dependencies]
namapper-pgsql = "1.0.0"
natx-pgsql = "1.0.0"
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres"] }
```

```rust
use namapper_pgsql::{Mapper, Query};

#[derive(sqlx::FromRow)]
struct OrderRow {
    id: i64,
    note: Option<String>,
}

#[Mapper(key = "orders", datasource = "reporting", cache = false)]
trait OrderMapper {
    #[Query(
        "SELECT id, note FROM orders WHERE id = #{id}",
        tx = "mandatory"
    )]
    async fn find(&self, id: i64) -> anyhow::Result<Option<OrderRow>>;
}

natx_pgsql::try_init_datasource("reporting", pool)?;
let mapper = OrderMapperClient::new();
let row = natx_pgsql::run_for("reporting", async { mapper.find(42).await }).await?;
# let _ = row;
```

使用门面时开启 `mapper-pgsql`，并从 `nasa::mapper::pgsql` 导入相同名称的宏与运行时类型。本 crate
不读取 Application YAML；同时开启 `application` 时，`tx-pgsql` 由 `napp` 从 YAML 建立受管 PgPool，
Mapper 通过同一 `natx-pgsql` registry 使用该 pool。

## 事务与数据源

- `tx = "auto"` 在同 datasource 的 ambient transaction 内复用连接，事务外从对应 pool 获取连接。
- `tx = "mandatory"` 要求调用发生在同名 PostgreSQL transaction 内；缺少事务或 datasource 不一致时，
  在执行 SQL 前拒绝。
- `tx = "never"` 拒绝在 ambient transaction 内调用，取连接前显式失败——供副作用不允许随外层事务
  回滚的语句(如自治审计写)使用。
- trait 级 `datasource` 决定默认连接和缓存身份。启用缓存或写后失效时，方法级 datasource 不能偏离 trait
  声明，避免同一缓存 namespace 指向多个数据库。
- PostgreSQL 事务只覆盖一个 driver 的一个 datasource，不提供跨 datasource 或跨数据库原子事务。

## 缓存合同

缓存 namespace 固定包含 `postgresql:<datasource>:` 前缀；`clear_also`、`clear_when` 使用同一命名规则，
从而与 MySQL 或其它 PostgreSQL datasource 隔离。事务内写操作只在最外层 COMMIT 被明确确认后执行失效；
回滚或提交结果不确定时不执行 after-commit 失效。

`MapperL2Cache`、codec、指标、进程默认注册槽与 MySQL Mapper 由 `namapper-core` 单点定义。混配应用的
同一个缓存实现只需实现一次合同，分别传给两侧 client 或安装为共享进程默认值。

启用 `redis-cache` 可直接使用 Redis Hash L2、进程内 single-flight 与 Redis 分布式 single-flight：

```toml
[dependencies]
namapper-pgsql = { version = "1.0.0", features = ["redis-cache"] }
redis = { version = "1", features = ["tokio-comp", "cluster-async"] }
```

使用 `nasa` 门面时开启 `mapper-redis-cache-pgsql`，并在启动 Hook 中显式构造和安装
`nasa::mapper::pgsql::RedisMapperL2Cache`。与 `application` 组合时，Service 会在 Ready 前确认所有声明
缓存的 Mapper 已安装默认 L2；运行时不会根据 Redis 配置猜测缓存 namespace、TTL 或 strict/best-effort
策略。`grouped-cache` 对应门面 feature 为 `mapper-cache-grouped-pgsql`。

## 边界

- SQL 必须是 PostgreSQL 方言或双方共同子集；不会翻译 `INSERT IGNORE`、`ON DUPLICATE KEY`、反引号
  identifier 等 MySQL 语法。
- SQL 模板禁止 `${...}` 和行注释。动态标识符只能通过 Mapper 白名单排序合同进入 SQL。
- 空列表 bind 会拒绝执行，业务应在调用前给出明确的空集合语义。
- `MapperStream` 持有池连接或 ambient 事务连接槽；调用方必须持续消费或及时释放流，避免长期占用连接。
