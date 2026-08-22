# namapper-pgsql

`namapper-pgsql` 提供 PostgreSQL 声明式 Mapper 运行时，使用 `$1..$n` prepared 占位符，并接入
`natx-pgsql` 的命名 datasource 与 ambient transaction。

它支持静态 SQL、`if`/`choose`/`foreach`/`trim` 动态 SQL、列表 bind、`RETURNING`、分页、白名单排序、
流式结果、`EnumOrdinal`、JSON 和 L2 cache。占位符只由结构化 bind 节点产生；字符串、quoted identifier、
块注释和 PostgreSQL JSON 操作符中的 `?` 保持原文。

## 使用

```toml
[dependencies]
namapper-pgsql = "1"
natx-pgsql = "1"
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
namapper-pgsql = { version = "1", features = ["redis-cache"] }
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
- `MapperStream` 拥有池连接生命周期；调用方必须持续消费或及时释放流，避免长期占用连接。
