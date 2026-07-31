# namapper

`namapper` 是 MyBatis 风格的声明式 Mapper。业务用 `trait + 属性宏` 声明 SQL，宏生成 `*Client`，运行时基于 `sqlx` 执行 MySQL SQL，并接入 `natx` / `nasa::tx` ambient 事务和可选二级缓存。

推荐业务项目通过门面 crate 使用：

```rust
use nasa::mapper::{Mapper, Query, Insert, Update, Delete, Execute};
```

如果项目直接依赖 `namapper`，把下面示例里的 `nasa::mapper::...` 换成 `namapper::...` 即可。

## 适用场景

适合：

- Repository 层固定 SQL、动态条件 SQL、分页、排序、批量写入。
- 需要和现有 `#[transactional]` 事务打通的 MySQL/TiDB 访问。
- 需要 MyBatis namespace 风格二级缓存、缓存失效、Redis Hash 存储的查询。
- 希望 SQL 留在 Rust 代码里，接受编译期宏校验，而不是运行时 XML 扫描。

不适合：

- 需要运行时拼任意表名、列名、SQL 片段的场景。`namapper` 明确禁止 `${...}` 和裸 `?`。
- 跨多个 datasource 的同一个分布式事务。
- 首版 `StreamQuery` 的动态 SQL、IN 列表或事务内流式读取。

## 安装

业务项目建议只依赖 `nasa` 门面：

```toml
[dependencies]
nasa = { version = "1.0.0", features = ["mapper"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "tls-rustls", "mysql", "chrono", "json"] }
tokio = { version = "1", features = ["full"] }
anyhow = "1"
serde = { version = "1", features = ["derive"] }
```

需要 Redis Hash 二级缓存：

```toml
nasa = { version = "1.0.0", features = ["mapper-redis-cache"] }
redis = { version = "1", features = ["tokio-comp", "cluster-async"] }
```

直接依赖 `namapper`：

```toml
namapper = { version = "1.0.0" }
natx = { version = "1.0.0" }
```

## 启动初始化

Mapper client 不持有 `MySqlPool`。所有 SQL 默认从 `nasa::tx` / `natx` 全局连接池取连接；在 `#[transactional]` 作用域内自动加入当前事务。

```rust
use sqlx::mysql::MySqlPoolOptions;

async fn init_db() -> anyhow::Result<()> {
    let pool = MySqlPoolOptions::new()
        .max_connections(10)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    nasa::tx::try_init(pool)?;
    Ok(())
}
```

在 `#[application]` 运行时下，以上装配自动完成，业务入口不再写启动代码：

- 声明 `db` 组件：按 `database` / `datasources.<name>` 配置先单连接探测、再建池，并注入本运行时（等价 `try_init` / `try_init_datasource`）。
- 声明 `cache` 组件（`nasa` feature 含 `mapper-redis-cache`）：自动安装默认 Mapper L2（`RedisMapperL2Cache`，与 `#[cached]` 的 L2 共用同一条 cluster 连接）。
- Service 模式在对外提供服务之前自动调用 `assert_l2_cache_installed_for_cached_queries()`：存在 `cache = true` 查询却没有任何 L2 安装路径（组件或启动 Hook 显式装配）时启动失败，而不是生产静默绕过缓存。

下文的手工装配说明面向不使用应用运行时的项目。

## 最小可用示例

普通查询示例建议显式 `cache = false`。`namapper` 当前 trait 级默认是 `cache = true`；如果项目没有安装 L2 cache，又不希望查询具备缓存语义，就在 Mapper 或方法上显式关闭。

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserRow {
    pub id: i64,
    pub name: String,
    pub amount: i32,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for UserRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            amount: row.try_get("amount")?,
        })
    }
}

#[nasa::mapper::Mapper(key = "user", cache = false)]
trait UserMapper {
    #[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id = #{id}")]
    async fn find_by_id(&self, id: i64) -> anyhow::Result<Option<UserRow>>;

    #[nasa::mapper::Insert("INSERT INTO user(name, amount) VALUES(#{name}, #{amount})")]
    async fn insert(&self, name: &str, amount: i32) -> anyhow::Result<u64>;
}

pub async fn demo() -> anyhow::Result<()> {
    let mapper = UserMapperClient::new();
    let _rows = mapper.insert("alice", 100).await?;
    let _user = mapper.find_by_id(1).await?;
    Ok(())
}
```

生成规则：

- `UserMapper` 生成 `UserMapperClient`。
- `#{id}` 编译成 prepared statement 的 `?`，并按 SQL 出现顺序 `.bind(...)`。
- SQL 里的裸 `?`、`${...}` 会被拒绝。
- `Query` 返回值必须包在 `anyhow::Result<T>` 或兼容的 `Result<T, E>` 里。

## 注解速查

Trait 级 `#[Mapper(...)]`：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `key` | trait 全路径 | 二级缓存 namespace / Redis Hash key |
| `cache` | `true` | trait 下 `Query` 默认是否走 L2 cache |
| `cache_in_tx` | `false` | trait 下 `cache = true` 的 `Query` 是否允许在 ambient 事务内读写 L2 |
| `cache_ttl_ms` | `None` | 默认缓存 TTL，单位毫秒 |
| `cache_errors` | `"bypass"` | 缓存失败时绕过 DB 继续，或 `"strict"` 直接返回 Err |
| `datasource` | `default` | 默认命名 datasource |
| `strict_params` | `false` | 编译期拒绝未使用方法参数 |
| `clear_also` | `[]` | 清当前 key 时额外清理的 key |
| `clear_when` | `[]` | 清其它 key 时也清当前 key |
| `cache_codec` | 默认 JSON | trait 级缓存编解码器工厂 |
| `client` | `TraitNameClient` | 自定义生成 client 名称 |

方法级 `#[Query/Insert/Update/Delete/Execute(...)]`：

| 参数 | 适用 | 说明 |
|---|---|---|
| `sql` / 位置字符串 | 全部 | SQL 模板 |
| `fetch` | `Query` | `"all"`、`"optional"`、`"one"`、`"scalar"` |
| `cache` | `Query` | 覆盖 trait 级缓存开关 |
| `cache_in_tx` | `Query` | 覆盖 trait 级事务内缓存开关；只对最终 `cache = true` 的查询生效 |
| `cache_ttl_ms` | `Query` | 覆盖 TTL |
| `hash_key_suffix` | `Query` | 自定义缓存参数后缀 |
| `cache_errors` | `Query` | `"bypass"` 或 `"strict"` |
| `cache_codec` | `Query` | 方法级 value codec |
| `typed_cache_codec` | `Query` | 方法级强类型 codec |
| `flush_cache` | 全部 | 写操作默认 `true`，Query 默认 `false` |
| `flush_refs` | 全部 | 是否展开 `clear_also` / `clear_when` |
| `tx` | 非 StreamQuery | `"auto"` 或 `"mandatory"`，`"never"` 首版不支持 |
| `datasource` | 全部 | 覆盖 trait 级 datasource |
| `strict_params` | 全部 | 覆盖 trait 级严格参数检查 |
| `checked` | 静态 Query | 使用 `sqlx::query!` / `query_as!` 编译期校验 |

## 场景 1：Query 返回多行

返回 `Vec<T>` 时默认按 `fetch_all` 执行。

```rust
#[nasa::mapper::Mapper(key = "user", cache = false)]
trait UserMapper {
    #[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE amount >= #{min_amount} ORDER BY id")]
    async fn find_rich(&self, min_amount: i32) -> anyhow::Result<Vec<UserRow>>;
}
```

## 场景 2：Query 返回 Optional

返回 `Option<T>` 时默认按 `fetch_optional` 执行。

```rust
#[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id = #{id}")]
async fn find_by_id(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
```

## 场景 3：Query 返回单行

返回非 `Vec`、非 `Option`、非标量类型时按 `fetch_one` 执行；无行会返回 `sqlx::Error::RowNotFound`。

```rust
#[nasa::mapper::Query(sql = "SELECT id, name, amount FROM user WHERE id = #{id}", fetch = "one")]
async fn must_find(&self, id: i64) -> anyhow::Result<UserRow>;
```

## 场景 4：Query 返回标量

标量查询必须显式 `fetch = "scalar"`。

```rust
#[nasa::mapper::Query(sql = "SELECT COUNT(*) FROM user WHERE amount >= #{min_amount}", fetch = "scalar")]
async fn count_rich(&self, min_amount: i32) -> anyhow::Result<i64>;
```

## 场景 5：Insert

返回 `u64` 时是 `rows_affected()`。

```rust
#[nasa::mapper::Insert("INSERT INTO user(name, amount) VALUES(#{name}, #{amount})")]
async fn insert(&self, name: &str, amount: i32) -> anyhow::Result<u64>;
```

需要 `last_insert_id()` 时返回 `sqlx::mysql::MySqlQueryResult`。

```rust
#[nasa::mapper::Insert("INSERT INTO user(name, amount) VALUES(#{name}, #{amount})")]
async fn insert_result(
    &self,
    name: &str,
    amount: i32,
) -> anyhow::Result<sqlx::mysql::MySqlQueryResult>;

async fn create_user(mapper: &UserMapperClient) -> anyhow::Result<i64> {
    let result = mapper.insert_result("alice", 100).await?;
    Ok(i64::try_from(result.last_insert_id())?)
}
```

写操作的返回形态由**返回类型**决定（`()` / `u64` / `MySqlQueryResult`），不需要额外注解。为兼容 MyBatis 写法，`returning` / `result` 参数会被接受但忽略，不改变行为。

## 场景 6：Update

```rust
#[nasa::mapper::Update("UPDATE user SET amount = #{amount} WHERE id = #{id}")]
async fn update_amount(&self, id: i64, amount: i32) -> anyhow::Result<u64>;
```

写操作默认 `flush_cache = true`。如果 client 安装了 L2 cache，会在事务提交后清理当前 Mapper key；无事务时立即清理。

## 场景 7：Delete

```rust
#[nasa::mapper::Delete("DELETE FROM user WHERE id = #{id}")]
async fn delete_by_id(&self, id: i64) -> anyhow::Result<u64>;
```

## 场景 8：Execute 建表、DDL、维护 SQL

返回 `()` 时只检查执行成功。

```rust
#[nasa::mapper::Execute(
    "CREATE TABLE IF NOT EXISTS user(
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(128) NOT NULL,
        amount INT NOT NULL
    ) DEFAULT CHARSET=utf8mb4"
)]
async fn ensure_table(&self) -> anyhow::Result<()>;
```

返回 `u64` 时也可以拿 `rows_affected()`。

```rust
#[nasa::mapper::Execute("TRUNCATE TABLE user")]
async fn truncate_user(&self) -> anyhow::Result<u64>;
```

## 场景 9：嵌套参数绑定

`#{page.limit}`、`#{filter.name}` 支持字段路径。字段必须可访问。

```rust
#[derive(serde::Serialize)]
pub struct UserFilter {
    pub name: Option<String>,
    pub min_amount: i32,
}

#[nasa::mapper::Query(
    "SELECT id, name, amount FROM user
     WHERE amount >= #{filter.min_amount}
     LIMIT #{page.limit} OFFSET #{page.offset}"
)]
async fn search(
    &self,
    filter: UserFilter,
    page: nasa::mapper::PageRequest,
) -> anyhow::Result<Vec<UserRow>>;
```

## 场景 10：严格参数检查

新 Mapper 建议打开 `strict_params = true`，防止方法参数加了但 SQL 没用上。

```rust
#[nasa::mapper::Mapper(key = "user", cache = false, strict_params = true)]
trait StrictUserMapper {
    #[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id = #{id}")]
    async fn find(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
}
```

下面这种会编译失败，因为 `unused` 没有参与 SQL bind、动态 test、`<foreach>`、`<order_by>` 或 `hash_key_suffix`：

```rust,ignore
#[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id = #{id}")]
async fn bad(&self, id: i64, unused: i64) -> anyhow::Result<Option<UserRow>>;
```

## 场景 11：IN 列表

简单 IN 查询可以直接写 `IN (#{ids})`。参数必须是 `Vec<T>`、slice 或数组等集合；空集合返回 Err，不访问 DB。

```rust
#[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id IN (#{ids}) ORDER BY id")]
async fn find_in(&self, ids: Vec<i64>) -> anyhow::Result<Vec<UserRow>>;
```

复杂场景可以用 `<foreach>` 自己控制括号和分隔符：

```rust
#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user WHERE id IN
<foreach collection="ids" item="id" open="(" separator="," close=")">
#{id}
</foreach>
ORDER BY id"#
)]
async fn find_in_foreach(&self, ids: Vec<i64>) -> anyhow::Result<Vec<UserRow>>;
```

## 场景 12：动态 where

`<where>` 在子片段非空时输出 `WHERE`，并移除开头 `AND` / `OR`。

```rust
#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user
<where>
<if test="name != null">
AND name = #{name}
</if>
<if test="min_amount != null">
AND amount >= #{min_amount}
</if>
</where>
ORDER BY id"#
)]
async fn find_dynamic(
    &self,
    name: Option<String>,
    min_amount: Option<i32>,
) -> anyhow::Result<Vec<UserRow>>;
```

`<if test>` 支持：

- Rust bool 表达式：`amount > 0`、`name.is_some()`、`flag && enabled`。
- MyBatis 风格逻辑词：`flag and enabled`、`a or b`，只会在字符串字面量外转换。
- null 简写：`name != null`、`name == null`、`null != name`、`null == name`。

如果 test 里需要写字符串字面量，建议属性用单引号：

```rust
#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user
<where>
<if test='label == "a and b" or label == "a or b"'>
AND label = #{label}
</if>
</where>"#
)]
async fn find_by_label(&self, label: &str) -> anyhow::Result<Vec<UserRow>>;
```

## 场景 13：choose / when / otherwise

`<choose>` 内至少一个 `<when>`，最多一个 `<otherwise>`。

```rust
#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user
<where>
<choose>
<when test="high_only">
AND amount >= #{threshold}
</when>
<otherwise>
AND amount >= 0
</otherwise>
</choose>
</where>
ORDER BY id"#
)]
async fn find_by_level(
    &self,
    high_only: bool,
    threshold: i32,
) -> anyhow::Result<Vec<UserRow>>;
```

## 场景 14：动态 set

`<set>` 在子片段非空时输出 `SET`，并移除开头或末尾逗号。它不会自动校验至少更新一列；调用方应保证至少一个字段为 `Some`。

```rust
#[nasa::mapper::Update(
    r#"UPDATE user
<set>
<if test="name != null">
name = #{name},
</if>
<if test="amount != null">
amount = #{amount},
</if>
</set>
WHERE id = #{id}"#
)]
async fn update_dynamic(
    &self,
    id: i64,
    name: Option<String>,
    amount: Option<i32>,
) -> anyhow::Result<u64>;
```

## 场景 15：trim

`<trim>` 是通用裁剪标签，只支持 `prefix`、`suffix`、`prefixOverrides`、`suffixOverrides`。

```rust
#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user
<trim prefix="WHERE" prefixOverrides="AND|OR">
<if test="name != null">
AND name = #{name}
</if>
<if test="min_amount != null">
AND amount >= #{min_amount}
</if>
</trim>
ORDER BY id"#
)]
async fn find_with_trim(
    &self,
    name: Option<String>,
    min_amount: Option<i32>,
) -> anyhow::Result<Vec<UserRow>>;
```

标签属性是编译期 SQL 片段，不能写 `#{...}`、`${...}`、裸 `?` 或注释。

## 场景 16：批量插入

用 `<foreach>` 生成多行 values。

```rust
#[derive(Debug, Clone)]
pub struct NewUser {
    pub name: String,
    pub amount: i32,
}

#[nasa::mapper::Insert(
    r#"INSERT INTO user(name, amount) VALUES
<foreach collection="rows" item="row" separator=",">
(#{row.name}, #{row.amount})
</foreach>"#
)]
async fn batch_insert(&self, rows: Vec<NewUser>) -> anyhow::Result<u64>;
```

大批量建议业务自己拆 chunk，明确事务边界。

```rust
pub async fn import_users(mapper: &UserMapperClient, rows: &[NewUser]) -> anyhow::Result<u64> {
    let mut total = 0;
    for chunk in nasa::mapper::batch_chunks(rows, 500)? {
        total += mapper.batch_insert(chunk.to_vec()).await?;
    }
    Ok(total)
}
```

## 场景 17：分页

`PageRequest` 是 1-based 页码，构造后暴露 `limit` / `offset` 给 SQL 绑定。

```rust
#[nasa::mapper::Query(
    "SELECT id, name, amount FROM user ORDER BY id LIMIT #{page.limit} OFFSET #{page.offset}"
)]
async fn page(&self, page: nasa::mapper::PageRequest) -> anyhow::Result<Vec<UserRow>>;

pub async fn page_demo(mapper: &UserMapperClient) -> anyhow::Result<Vec<UserRow>> {
    let page = nasa::mapper::PageRequest::new(2, 20)?;
    mapper.page(page).await
}
```

需要自定义最大 page size：

```rust
let page = nasa::mapper::PageRequest::with_max_page_size(1, 200, 500)?;
```

已知 `offset` / `limit`（如游标或前端直接传）时可跳过页码换算：

```rust
let page = nasa::mapper::PageRequest::from_offset_limit(40, 20)?;
```

## 场景 18：安全动态排序

禁止把用户输入字符串直接拼进 `ORDER BY`。排序必须用白名单 enum。

```rust
#[derive(Copy, Clone, Debug, PartialEq, Eq, nasa::mapper::MapperOrderField)]
enum UserSortField {
    Id,
    Amount,
    #[mapper_order_field("u.created_at")]
    CreatedAt,
}

#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user u
<order_by value="order"/>
LIMIT #{page.limit} OFFSET #{page.offset}"#
)]
async fn find_ordered(
    &self,
    order: nasa::mapper::OrderBy<UserSortField>,
    page: nasa::mapper::PageRequest,
) -> anyhow::Result<Vec<UserRow>>;

pub async fn order_demo(mapper: &UserMapperClient) -> anyhow::Result<Vec<UserRow>> {
    let order = nasa::mapper::OrderBy::desc(UserSortField::Amount);
    let page = nasa::mapper::PageRequest::new(1, 20)?;
    mapper.find_ordered(order, page).await
}
```

多个排序字段：

```rust
#[nasa::mapper::Query(
    r#"SELECT id, name, amount FROM user u
<order_by value="orders"/>"#
)]
async fn find_ordered_many(
    &self,
    orders: Vec<nasa::mapper::OrderBy<UserSortField>>,
) -> anyhow::Result<Vec<UserRow>>;

pub async fn order_many_demo(mapper: &UserMapperClient) -> anyhow::Result<Vec<UserRow>> {
    // 渲染成 `ORDER BY amount DESC, id ASC`。
    let orders = vec![
        nasa::mapper::OrderBy::desc(UserSortField::Amount),
        nasa::mapper::OrderBy::asc(UserSortField::Id),
    ];
    mapper.find_ordered_many(orders).await
}
```

`String`、`&str` 不实现 `MapperOrderBy`，不能作为 `<order_by>` 参数。

## 场景 19：事务自动加入

默认 `tx = "auto"`。无事务时从全局 pool 拿普通连接；在 `#[transactional]` 内自动复用当前事务连接。

```rust
#[nasa::mapper::Mapper(key = "user", cache = false)]
trait UserTxMapper {
    #[nasa::mapper::Update("UPDATE user SET amount = amount + #{delta} WHERE id = #{id}")]
    async fn add_amount(&self, id: i64, delta: i32) -> anyhow::Result<u64>;
}

pub struct UserService {
    mapper: UserTxMapperClient,
}

impl UserService {
    #[nasa::tx::transactional]
    pub async fn transfer_amount(&self, from: i64, to: i64, amount: i32) -> anyhow::Result<()> {
        self.mapper.add_amount(from, -amount).await?;
        self.mapper.add_amount(to, amount).await?;
        Ok(())
    }
}
```

写操作的 cache clear 会在事务 commit 后执行；rollback 不清。

## 场景 20：强制事务和 FOR UPDATE

行锁查询必须绕过缓存，并建议强制事务。

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user WHERE id = #{id} FOR UPDATE",
    cache = false,
    flush_cache = true,
    tx = "mandatory"
)]
async fn find_for_update(&self, id: i64) -> anyhow::Result<UserRow>;
```

`tx = "mandatory"` 在无事务调用时返回 Err。`tx = "never"` 首版不支持。

## 场景 21：多数据源

启动时注入命名 datasource：

```rust
async fn init_datasources(default_url: &str, reporting_url: &str) -> anyhow::Result<()> {
    let default_pool = sqlx::mysql::MySqlPoolOptions::new().connect(default_url).await?;
    let reporting_pool = sqlx::mysql::MySqlPoolOptions::new().connect(reporting_url).await?;

    nasa::tx::try_init(default_pool)?;
    nasa::tx::try_init_datasource("reporting", reporting_pool)?;
    Ok(())
}
```

trait 级 datasource：

```rust
#[nasa::mapper::Mapper(key = "report_user", cache = false, datasource = "reporting")]
trait ReportUserMapper {
    #[nasa::mapper::Query("SELECT id, name, amount FROM user_report WHERE id = #{id}")]
    async fn find_report(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
}
```

方法级覆盖：

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user_archive WHERE id = #{id}",
    datasource = "archive",
    cache = false
)]
async fn find_archive(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
```

事务内 datasource 必须一致。默认事务不能调用 `datasource = "reporting"` 的 Mapper；应使用 `#[transactional(datasource = "reporting")]`。

## 场景 22：Query 二级缓存

当前 `Mapper` trait 默认 `cache = true`，但只有注入 L2 cache 后才会真正读写缓存；没有注入时会绕过缓存查库。生产建议启动时调用 `assert_l2_cache_installed_for_cached_queries()`，防止以为启用了缓存但实际没有安装；`#[application]` 运行时的 Service 模式会在就绪前自动执行该断言，声明 `cache` 组件即自动安装默认 L2。

> **事务内缓存必须显式 opt-in**：`cache = true` 的 `Query` 在普通无事务场景读写 L2；在 `#[transactional]` ambient 事务内默认绕过 L2，避免把未提交视图写入共享缓存。业务确认某个查询在事务内也可以读写 L2 时，显式写 `cache_in_tx = true`。事务内需要读实时/未提交数据、行锁、或不想污染共享缓存的方法继续写 `cache = false`（`FOR UPDATE` / 行锁查询见场景 20）。写操作的失效仍在 commit 后执行、rollback 不清。

`cache_in_tx` 只对启用 L2 的 `Query` 有意义。方法级同时写 `cache = false, cache_in_tx = true` 会编译失败；写操作上写 `cache_in_tx` 也会编译失败。Trait 级 `cache_in_tx = true` 只是默认值，可配合个别方法显式 `cache = true` 使用。

```rust
#[nasa::mapper::Mapper(key = "user", cache = true, cache_errors = "strict")]
trait CachedUserMapper {
    #[nasa::mapper::Query(
        sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
        cache_ttl_ms = 60_000
    )]
    async fn find_by_id(&self, id: i64) -> anyhow::Result<Option<UserRow>>;

    #[nasa::mapper::Query(
        sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
        cache_in_tx = true,
        cache_ttl_ms = 60_000
    )]
    async fn find_by_id_even_in_tx(&self, id: i64) -> anyhow::Result<Option<UserRow>>;

    #[nasa::mapper::Query(
        sql = "SELECT id, name, amount FROM user WHERE id = #{id} FOR UPDATE",
        cache = false,
        tx = "mandatory"
    )]
    async fn lock_by_id(&self, id: i64) -> anyhow::Result<UserRow>;
}
```

手动给单个 client 注入：

```rust
use std::sync::Arc;

async fn use_cache(cache: Arc<dyn nasa::mapper::MapperL2Cache>) -> anyhow::Result<()> {
    let mapper = CachedUserMapperClient::new().with_l2_cache(cache);
    let _ = mapper.find_by_id(1).await?;
    Ok(())
}
```

进程级默认 L2 cache：

```rust
use std::sync::Arc;

fn install_default_cache(cache: Arc<dyn nasa::mapper::MapperL2Cache>) -> anyhow::Result<()> {
    nasa::mapper::set_default_l2_cache(cache)?;
    nasa::mapper::assert_l2_cache_installed_for_cached_queries()?;
    Ok(())
}
```

## 场景 23：Redis Hash 二级缓存

打开 `mapper-redis-cache` feature 后可使用内置 Redis Hash cache。Redis key 是 Mapper `key`，Hash field 是 `sql:{normalized_sql}:...`。`#[application]` 运行时下声明 `cache` 组件即自动完成本场景的安装，无需手写以下代码。

```rust
use std::sync::Arc;

async fn install_redis_mapper_cache(redis_url: String) -> anyhow::Result<()> {
    let client = redis::cluster::ClusterClient::new(vec![redis_url])?;
    let conn = client.get_async_connection().await?;

    let redis_cache = Arc::new(nasa::mapper::RedisMapperL2Cache::new(conn.clone()));
    redis_cache.assert_hash_field_ttl_supported().await?;

    let cache: Arc<dyn nasa::mapper::MapperL2Cache> = Arc::new(
        nasa::mapper::RedisDistributedSingleFlightMapperL2Cache::new(
            redis_cache,
            conn,
        ),
    );
    nasa::mapper::set_default_l2_cache(cache)?;
    nasa::mapper::assert_l2_cache_installed_for_cached_queries()?;
    Ok(())
}
```

说明：

- `cache_ttl_ms` 优先使用 Redis `HPEXPIRE` field TTL。
- Redis 不支持 `HPEXPIRE` 时，`RedisMapperL2Cache` 会降级到整个 Hash key 的 `PEXPIRE`。
- 如果业务要求必须 field TTL，启动时调用 `assert_hash_field_ttl_supported()`。
- `RedisDistributedSingleFlightMapperL2Cache` 用 Redis 短锁（`SET NX PX` + Lua `GET==token` CAS 释放）降低跨进程缓存击穿；抢到锁的调用方执行 loader，其它调用方轮询底层 cache，命中后返回 `HitAfterWait`。
- 短锁参数可调：`.with_lock_ttl_ms(5_000)`（锁 TTL，过短会让慢 loader 重复执行，过长会放大进程崩溃后的等待）、`.with_wait_timeout_ms(5_000)`（单轮等待超时后重新抢锁）、`.with_poll_interval_ms(25)`（等待者轮询间隔）。三个非 fallible 参数统一收敛到 1ms–365 天。
- `RedisMapperL2Cache::put` 在写入 Hash field 前校验 TTL；`Some(0)` 或超过 365 天会直接返回错误，不会留下已经写入但没有有效过期策略的字段。
- 只需单进程防击穿（不跨进程）时用 `SingleFlightMapperL2Cache`（见场景 24），无需 Redis 短锁。

## 场景 24：自定义 L2 cache

任何实现 `MapperL2Cache` 的类型都可接入。

```rust
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
struct MemoryMapperCache {
    values: Mutex<HashMap<(String, String), Vec<u8>>>,
}

#[nasa::mapper::async_trait]
impl nasa::mapper::MapperL2Cache for MemoryMapperCache {
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.values.lock().unwrap().get(&(key.into(), hash_key.into())).cloned())
    }

    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        _ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        self.values.lock().unwrap().insert((key.into(), hash_key.into()), value.to_vec());
        Ok(())
    }

    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        self.values.lock().unwrap().remove(&(key.into(), hash_key.into()));
        Ok(())
    }

    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        self.values.lock().unwrap().retain(|(k, _), _| k != key);
        Ok(())
    }
}
```

如需单进程防击穿，可包一层：

```rust
let cache = std::sync::Arc::new(MemoryMapperCache::default());
let cache = std::sync::Arc::new(nasa::mapper::SingleFlightMapperL2Cache::new(cache));
```

## 场景 25：缓存 key 和 hash_key

缓存结构：

```text
key      = Mapper namespace，来自 #[Mapper(key = "...")]
hash_key = sql:{normalized_sql}:{sha256(args...)}
value    = 查询结果序列化后的 bytes
```

默认 hash_key 包含规范化 SQL 明文和参数 JSON 的 SHA-256。需要人类可读参数后缀时：

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
    hash_key_suffix = "id:{id}"
)]
async fn find_with_readable_key(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
```

嵌套参数：

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user LIMIT #{page.limit} OFFSET #{page.offset}",
    hash_key_suffix = "page:{page.limit}:{page.offset}"
)]
async fn page_with_key(&self, page: nasa::mapper::PageRequest) -> anyhow::Result<Vec<UserRow>>;
```

`hash_key_suffix` 只能引用 SQL 里已有的标量 bind，不能引用集合或对象。

## 场景 26：缓存失效关系

写操作默认清当前 Mapper key。`clear_also` 表示清当前 key 时额外清理其它 key；`clear_when` 表示其它 key 被清时也清当前 key。

```rust
#[nasa::mapper::Mapper(
    key = "user",
    clear_also = ["user_wallet"],
    clear_when = ["account"]
)]
trait UserMapper {
    #[nasa::mapper::Update("UPDATE user SET name = #{name} WHERE id = #{id}")]
    async fn update_name(&self, id: i64, name: &str) -> anyhow::Result<u64>;
}

#[nasa::mapper::Mapper(key = "account", clear_also = ["wallet"])]
trait AccountMapper {
    #[nasa::mapper::Delete("DELETE FROM account WHERE id = #{id}")]
    async fn delete_account(&self, id: i64) -> anyhow::Result<u64>;
}
```

效果：

- `UserMapper::update_name` 清 `user` 和 `user_wallet`。
- `AccountMapper::delete_account` 清 `account`、`wallet`，并因为 `UserMapper.clear_when = ["account"]` 额外清 `user`。
- 失效关系只展开一层，不递归级联。

写操作默认 `flush_refs = true`（展开 `clear_also` / `clear_when`）。某次写只想清当前 key、不牵动关联 key 时，方法级写 `flush_refs = false`：

```rust
#[nasa::mapper::Update(
    sql = "UPDATE user SET name = #{name} WHERE id = #{id}",
    flush_refs = false
)]
async fn update_name_only(&self, id: i64, name: &str) -> anyhow::Result<u64>;
```

## 场景 27：实时查询并清缓存

`Query(flush_cache = true)` 定义为实时查询并清理缓存：不读 L2、不写 L2，DB 查询成功后按写操作规则清理 key。

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
    cache = false,
    flush_cache = true
)]
async fn realtime_find_and_clear(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
```

`Query(cache = true, flush_cache = true)` 会编译失败，避免语义歧义。

## 场景 28：缓存编解码器

默认使用 JSON 编码查询结果。需要兼容旧缓存格式或做压缩/加密时，可以配置 codec。

```rust
use std::sync::Arc;

#[derive(Debug, Default)]
struct PrefixCodec;

impl nasa::mapper::MapperCacheCodec for PrefixCodec {
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        let mut out = b"v1:".to_vec();
        out.extend_from_slice(&serde_json::to_vec(value)?);
        Ok(out)
    }

    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        let json = bytes
            .strip_prefix(b"v1:")
            .ok_or_else(|| anyhow::anyhow!("missing v1 prefix"))?;
        Ok(serde_json::from_slice(json)?)
    }
}

fn prefix_codec() -> Arc<dyn nasa::mapper::MapperCacheCodec> {
    Arc::new(PrefixCodec)
}

#[nasa::mapper::Mapper(key = "user", cache_codec = prefix_codec)]
trait CodecUserMapper {
    #[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id = #{id}")]
    async fn find(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
}
```

方法级覆盖：

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
    cache_codec = prefix_codec
)]
async fn find_with_codec(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
```

强类型 codec 适合不想经过 `serde_json::Value` 的场景：

```rust
#[derive(Debug, Default)]
struct OptionalUserCodec;

impl nasa::mapper::MapperTypedCacheCodec<Option<UserRow>> for OptionalUserCodec {
    fn encode_typed(&self, value: &Option<UserRow>) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    fn decode_typed(&self, bytes: &[u8]) -> anyhow::Result<Option<UserRow>> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

fn optional_user_codec() -> OptionalUserCodec {
    OptionalUserCodec
}

#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
    typed_cache_codec = optional_user_codec
)]
async fn find_typed_cache(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
```

版本化 codec 可用于灰度迁移旧格式。写入固定带 `namapper:codec:v1:<name>:` 前缀；`with_named_fallback` 注册旧 codec 名以便读旧数据，命中 fallback / legacy 时框架会 best-effort 回写为当前版本：

```rust
fn versioned_codec() -> Arc<dyn nasa::mapper::MapperCacheCodec> {
    Arc::new(
        nasa::mapper::VersionedMapperCacheCodec::new("v2", prefix_codec())
            .expect("valid codec version")
            // 读到旧的 "v1" 命名数据时用旧 codec 解码。
            .with_named_fallback("v1", Arc::new(nasa::mapper::JsonMapperCacheCodec))
            .expect("valid fallback name"),
    )
}

// 迁移完成、确定不再有无前缀 legacy JSON 时，可关掉 legacy 兜底：
fn versioned_codec_strict() -> Arc<dyn nasa::mapper::MapperCacheCodec> {
    Arc::new(
        nasa::mapper::VersionedMapperCacheCodec::new("v2", prefix_codec())
            .expect("valid codec version")
            .without_legacy_json_fallback(),
    )
}
```

`FallbackMapperCacheCodec` 写入固定用 primary，读取时先 primary 后 fallback 依次尝试；命中 fallback 时可回写为 primary 格式，用于平滑迁移：

```rust
fn fallback_codec() -> Arc<dyn nasa::mapper::MapperCacheCodec> {
    Arc::new(
        nasa::mapper::FallbackMapperCacheCodec::new(prefix_codec())
            .with_fallback(Arc::new(nasa::mapper::JsonMapperCacheCodec)),
    )
}
```

codec 解析优先级（从高到低）：方法级 `cache_codec` > client `.with_cache_codec(...)` / trait 级 `cache_codec` > 进程级默认 > 内置 `JsonMapperCacheCodec`。

client 级注入（覆盖 trait 级 codec，和 `with_l2_cache` 一样是链式 builder）：

```rust
use std::sync::Arc;

async fn use_codec(codec: Arc<dyn nasa::mapper::MapperCacheCodec>) -> anyhow::Result<()> {
    let mapper = CodecUserMapperClient::new().with_cache_codec(codec);
    let _ = mapper.find(1).await?;
    Ok(())
}
```

进程级默认 codec（在产生任何缓存 value 之前调用一次）：

```rust
use std::sync::Arc;

fn install_default_codec(codec: Arc<dyn nasa::mapper::MapperCacheCodec>) -> anyhow::Result<()> {
    nasa::mapper::set_default_mapper_cache_codec(codec)
}
```

codec 只作用于缓存 value 的 bytes 编码，**不参与 hash_key 生成**（hash_key 固定由规范化 SQL + 参数 JSON hash 派生），避免切换 codec 造成 key 语义漂移。

## 场景 29：JSON 字段

`nasa::mapper::Json<T>` 透传 `sqlx::types::Json<T>`。

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Profile {
    pub level: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UserProfileRow {
    pub id: i64,
    pub profile: nasa::mapper::Json<Profile>,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for UserProfileRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            profile: row.try_get("profile")?,
        })
    }
}

#[nasa::mapper::Mapper(key = "user_profile", cache = false)]
trait UserProfileMapper {
    #[nasa::mapper::Insert("INSERT INTO user_profile(id, profile) VALUES(#{id}, #{profile})")]
    async fn insert_profile(
        &self,
        id: i64,
        profile: nasa::mapper::Json<Profile>,
    ) -> anyhow::Result<u64>;

    #[nasa::mapper::Query("SELECT id, profile FROM user_profile WHERE id = #{id}")]
    async fn find_profile(&self, id: i64) -> anyhow::Result<Option<UserProfileRow>>;
}
```

## 场景 30：枚举 ordinal

对齐历史 `EnumIntegerHandler` 的 ordinal 语义。

```rust
#[derive(Copy, Clone, Debug, PartialEq, Eq, nasa::mapper::MapperEnum)]
enum OrderStatus {
    Created,
    Running,
    Closed,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderRow {
    pub id: i64,
    pub status: nasa::mapper::EnumOrdinal<OrderStatus>,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for OrderRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            status: row.try_get("status")?,
        })
    }
}

#[nasa::mapper::Mapper(key = "orders", cache = false)]
trait OrderMapper {
    #[nasa::mapper::Insert("INSERT INTO orders(id, status) VALUES(#{id}, #{status})")]
    async fn insert_status(
        &self,
        id: i64,
        status: nasa::mapper::EnumOrdinal<OrderStatus>,
    ) -> anyhow::Result<u64>;

    #[nasa::mapper::Query("SELECT id, status FROM orders WHERE status = #{status}")]
    async fn find_by_status(
        &self,
        status: nasa::mapper::EnumOrdinal<OrderStatus>,
    ) -> anyhow::Result<Vec<OrderRow>>;
}
```

注意：derive 默认按 enum 声明顺序生成 `0/1/2...`。如果业务码要求长期稳定，手写 `MapperEnum`，不要依赖变体顺序。

## 场景 31：StreamQuery

首版 `StreamQuery` 只支持静态 SQL、标量 bind、无动态标签、无 IN 列表、无缓存、无事务参数。返回 `MapperStream<T>`。

```rust
#[nasa::mapper::Mapper(key = "user_stream", cache = false)]
trait UserStreamMapper {
    #[nasa::mapper::StreamQuery("SELECT id, name, amount FROM user WHERE id > #{cursor} ORDER BY id")]
    async fn stream_after(
        &self,
        cursor: i64,
    ) -> anyhow::Result<nasa::mapper::MapperStream<UserRow>>;
}

pub async fn stream_demo(mapper: &UserStreamMapperClient) -> anyhow::Result<()> {
    let mut stream = mapper.stream_after(0).await?;
    while let Some(row) = stream.next().await {
        let row = row?;
        tracing::info!(id = row.id, "loaded row");
    }
    Ok(())
}
```

## 场景 32：checked = true 编译期 SQL 校验

`checked = true` 使用 `sqlx::query!` / `query_as!`。它只支持静态 `Query` 和标量 bind，不支持动态 SQL 标签、IN 列表、`StreamQuery`。需要按 sqlx 要求配置 `DATABASE_URL` 或 sqlx offline 元数据。

```rust
#[nasa::mapper::Query(
    sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
    checked = true
)]
async fn checked_find(&self, id: i64) -> anyhow::Result<Option<UserRow>>;

#[nasa::mapper::Query(
    sql = "SELECT COUNT(*) FROM user",
    fetch = "scalar",
    checked = true
)]
async fn checked_count(&self) -> anyhow::Result<i64>;
```

## 场景 33：自定义 client 名称

默认 client 名是 `TraitNameClient`。可以显式指定：

```rust
#[nasa::mapper::Mapper(key = "user", cache = false, client = UserRepository)]
trait UserMapper {
    #[nasa::mapper::Query("SELECT id, name, amount FROM user WHERE id = #{id}")]
    async fn find(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
}

let mapper = UserRepository::new();
```

## 场景 34：Mapper 指标（可观测）

Mapper 在缓存相关路径会发出指标事件。实现 `MapperMetrics` 可接入 Prometheus、日志或诊断探针，用 `set_default_mapper_metrics` 进程级安装一次即可（`record` 内部应避免 panic 和长阻塞）。

```rust
use std::sync::Arc;

struct LogMapperMetrics;

impl nasa::mapper::MapperMetrics for LogMapperMetrics {
    fn record(&self, metric: nasa::mapper::MapperMetric<'_>) {
        tracing::info!(
            kind = ?metric.kind,
            mapper_key = metric.mapper_key,
            hash_key = metric.hash_key,
            sql = metric.sql,
            detail = metric.detail,
            "mapper metric",
        );
    }
}

fn install_metrics() -> anyhow::Result<()> {
    nasa::mapper::set_default_mapper_metrics(Arc::new(LogMapperMetrics))
}
```

`MapperMetricKind` 覆盖的事件：`CacheBypass`（未注入 L2 cache，`detail = "no_l2_cache"`；事务内未显式 `cache_in_tx = true`，`detail = "in_transaction"`）、`CacheHit` / `CacheHitAfterWait` / `CacheMiss`、`CacheLoad`（本次调用回源并写缓存）、`CachePut`，以及各类失败：`CacheHashKeyError` / `CacheGetError` / `CacheLoadError` / `CacheDecodeError` / `CacheEncodeError` / `CachePutError`。指标只在进程级安装，client 不单独持有 metrics。

## 场景 35：缓存错误策略 cache_errors

缓存路径出错（hash_key 构造 / get / decode / encode / put 失败）时的处理策略。默认 `"bypass"`：只记日志和指标，继续用 DB 结果，不影响业务返回；`"strict"`：把这些错误当查询错误上抛。

```rust
#[nasa::mapper::Mapper(key = "user", cache = true)]
trait UserCacheErrMapper {
    // 默认 bypass：Redis 抖动时降级查库，接口照常返回。
    #[nasa::mapper::Query(
        sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
        cache_errors = "bypass"
    )]
    async fn find_lenient(&self, id: i64) -> anyhow::Result<Option<UserRow>>;

    // strict：缓存读写异常直接上抛，用于对缓存故障零容忍的读路径。
    #[nasa::mapper::Query(
        sql = "SELECT id, name, amount FROM user WHERE id = #{id}",
        cache_errors = "strict"
    )]
    async fn find_strict(&self, id: i64) -> anyhow::Result<Option<UserRow>>;
}
```

注意：源查询已成功、只是 encode/put 失败时，`bypass` 直接返回这次源查询结果，不会为绕过缓存再查一次库；写操作 after-commit 清理失败无法回滚已提交事务，只能告警。

## 场景 36：运行时辅助函数

Mapper 方法内部由宏自动取连接。业务想在同一 ambient 事务里夹一段原生 sqlx 时，用 `nasa::mapper::conn()`（无事务时取普通池连接，事务内复用当前事务连接）：

```rust
pub async fn raw_alongside_mapper(id: i64) -> anyhow::Result<()> {
    let mut c = nasa::mapper::conn().await?;
    sqlx::query("UPDATE user SET amount = amount + 1 WHERE id = ?")
        .bind(id)
        .execute(c.as_mut())
        .await?;
    Ok(())
}
```

其它入口：`conn_for("reporting")` 从命名 datasource 取连接；`mandatory_conn()` / `mandatory_conn_for(..)` 无事务时返回 Err（强制事务）；`pool_for("default")` 取 owned `MySqlPool`（给需要独立持有连接池生命周期的无事务流式查询）；`in_transaction()` / `current_datasource()` 做事务内省：

```rust
pub fn tx_state() -> Option<&'static str> {
    if nasa::mapper::in_transaction() {
        nasa::mapper::current_datasource()
    } else {
        None
    }
}
```

## SQL 安全边界

必须遵守：

- 值参数只用 `#{name}`，会变成 prepared statement bind。
- 禁止 `${name}`，禁止裸 `?`。
- 表名、列名、排序字段不能来自用户字符串。
- 排序使用 `<order_by>` + `MapperOrderField` 白名单。
- 动态标签只控制静态 SQL 片段是否出现，不把用户字符串拼成 SQL 结构。
- 行注释 `--` / `#` 在 SQL 明文区禁止；字符串字面量里的 `--` / `#` 允许。
- `/* ... */` 块注释允许，动态标签在字符串、双引号、反引号、块注释里不会被解析；块注释内容（含撇号 / 引号 / 逗号）按原样保留，不会影响注释之外 SQL 的空白规范化与缓存 key。
- `<foreach>`、`<trim>` 等标签属性只允许编译期固定 SQL 片段，不能放占位符或注释。

示例：下面会编译失败。

```rust,ignore
#[nasa::mapper::Query("SELECT id FROM user WHERE name = ${name}")]
async fn bad_dollar(&self, name: &str) -> anyhow::Result<Vec<UserRow>>;

#[nasa::mapper::Query("SELECT id FROM user WHERE name = ?")]
async fn bad_question(&self, name: &str) -> anyhow::Result<Vec<UserRow>>;

#[nasa::mapper::Query("SELECT id FROM user ORDER BY #{order_by}")]
async fn bad_order(&self, order_by: &str) -> anyhow::Result<Vec<UserRow>>;
```

## 本地校验

Mapper trait 的 SQL 解析、bind、动态标签展开都在编译期完成，业务项目接入后建议至少跑：

```bash
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
```

需要对真实 MySQL + Redis 做后端验收时，用业务项目自己的验收入口或脚本装配环境：通过 `MySqlPoolOptions` 建池后调用 `nasa::tx::try_init(pool)`，再用 `set_default_l2_cache(...)` 安装 L2（Redis Hash 用场景 23 的内置适配器），最后调用生成的 `*Client` 验证查询、事务、缓存命中和失效行为。连接串、账号、密码必须来自环境变量或外部配置，不要写进 README、源码或配置样例。

## 常见问题

### 为什么普通 Query 默认 cache = true？

这是为了对齐 MyBatis namespace 二级缓存语义。新项目如果暂时不用缓存，建议在 trait 上写 `cache = false`。生产项目如果依赖缓存，启动时安装默认 L2 cache，并调用 `assert_l2_cache_installed_for_cached_queries()`。

### Row 为什么要实现 Serialize / Deserialize？

启用缓存的 Query 需要把返回值写入 L2 cache，因此返回类型和参与 hash_key 的参数需要可序列化。不走缓存的 Mapper 可以只实现 `sqlx::FromRow`。

### 能不能动态表名？

不能。动态表名属于 SQL 结构拼接，不走 prepared bind。请用硬编码分支、多个 Mapper 方法，或在上层选择不同方法。

### `Option<T>` 怎么判断 null？

在 `<if test>` 中写 `field != null` / `field == null`，或 Rust 风格 `field.is_some()` / `field.is_none()`。

### 空 IN 列表会怎样？

返回 Err，不访问 DB。调用方应在 service 层提前处理空集合。

### 多个 Mapper 想共享同一个缓存 namespace 怎么办？

给它们配置相同的 `#[Mapper(key = "...")]`。`clear_also` / `clear_when` 只表达失效关系，不表达共享 namespace。

## 开源前检查

如果要把包含 `namapper` 的仓库开源，请先确认：

- README、源码、配置和脚本里没有内网 IP、账号、密码、token。
- 真实后端地址只通过环境变量或外部配置读取，不硬编码。
- SQL 初始化脚本不包含生产数据。
- CI 不依赖内网 MySQL、Redis 或私有 Nacos；需要真实后端的验收步骤由受控环境显式触发。

## YML 配置与使用

`namapper` 的 SQL、缓存开关、事务要求、datasource 名称都写在 trait 属性上；运行期 yml 只负责提供 MySQL 连接池、命名 datasource、L2 缓存和发布前校验开关。使用 `#[application]` 运行时的项目不需要下面的手工启动代码：连接池来自 `database` / `datasources` 配置根（db 组件），L2 安装与就绪前断言由 `cache` 组件与运行时接管。

推荐配置：

```yaml
mysql:
  default:
    url: ${APP_MYSQL_URL}
    max_connections: 16
    min_connections: 1
    acquire_timeout_ms: 3000
  reporting:
    url: ${APP_REPORTING_MYSQL_URL}
    max_connections: 8

mapper:
  assert_l2_cache: true
  default_cache_ttl_ms: 300000
  redis_cache:
    enabled: true
    key_prefix: mapper
    hash_field_ttl_supported: true
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `mysql.<name>.url` | 命名数据源连接串；`default` 对应未显式声明 datasource 的 Mapper。 |
| `mysql.<name>.max_connections` | MySQL pool 最大连接数。 |
| `mysql.<name>.min_connections` | MySQL pool 最小连接数。 |
| `mysql.<name>.acquire_timeout_ms` | 获取连接超时。 |
| `mapper.assert_l2_cache` | 启动时是否调用 `assert_l2_cache_installed_for_cached_queries()`。 |
| `mapper.default_cache_ttl_ms` | 业务默认缓存 TTL；方法上 `cache_ttl_ms` 可覆盖。 |
| `mapper.redis_cache.enabled` | 是否安装 Redis Hash L2 cache。 |
| `mapper.redis_cache.key_prefix` | 业务给 Redis cache key 预留的前缀；具体拼接由注入的 cache 实现决定。 |
| `mapper.redis_cache.hash_field_ttl_supported` | 本地 Redis 是否支持 hash field TTL；支持时可调用探测接口确认。 |

启动代码：

```rust
let default_pool = sqlx::mysql::MySqlPoolOptions::new()
    .max_connections(cfg.mysql.default.max_connections)
    .connect(&cfg.mysql.default.url)
    .await?;
nasa::tx::try_init(default_pool)?;

let reporting_pool = sqlx::mysql::MySqlPoolOptions::new()
    .max_connections(cfg.mysql.reporting.max_connections)
    .connect(&cfg.mysql.reporting.url)
    .await?;
nasa::tx::try_init_datasource("reporting", reporting_pool)?;

if cfg.mapper.redis_cache.enabled {
    let l2 = std::sync::Arc::new(nasa::mapper::RedisMapperL2Cache::new(redis_conn));
    nasa::mapper::set_default_l2_cache(l2)?;
}

if cfg.mapper.assert_l2_cache {
    nasa::mapper::assert_l2_cache_installed_for_cached_queries()?;
}
```

配置和属性的职责边界：

| 事项 | 放在哪里 |
| --- | --- |
| SQL 文本、动态 SQL、`cache`、`cache_in_tx`、`flush_cache` | Mapper trait / 方法属性 |
| datasource 选择 | Mapper 属性中的 `datasource = "..."` |
| MySQL URL、连接池大小 | yml |
| 是否安装 L2 cache、Redis 地址、缓存全局策略 | yml + 启动代码 |
| 事务边界 | `#[transactional]` 属性和 `natx` 运行时 |

生产建议：不用缓存的 Mapper 在 trait 上写 `cache = false`；依赖缓存的服务在启动时安装 L2 并打开 `assert_l2_cache`。
