# namapper-sqlite

SQLite 业务继续使用 `#[Mapper]`、`#[Query]`、`#[Insert]`、`#[Update]`、`#[Delete]`、`#[Execute]` 和 `#[StreamQuery]`。宏负责绑定和映射，`natx-sqlite` 负责文件权威、SQL 权限、事务和生命周期，`napp` 的同一个 `"db"` 组件负责受管启动与停机。

## 宏与执行架构

```text
Mapper trait + SQL 方法属性 → 编译期 bind、返回映射、静态观测身份
  → 生成的 Client 固定 datasource / owner → 事务与缓存门禁
  → natx-sqlite 受控 executor → 原生 prepare 与执行 → 有界结果交付
```

业务不需要另建 SQLx pool 或手写连接管理。编译期导入路径决定 driver 类型，运行期配置必须匹配；字段绑定、动态 SQL 和缓存不会绕过原生权限检查。缓存命中仍验证来源与只读性。完整的配置、迁移和业务入口见 [SQLite 接入指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/sqlite.md)。

## 门面用法

Cargo 启用 `nasa` 的 `application`、`mapper-sqlite`，宏从 `nasa::mapper::sqlite` 导入。仅使用组件时从 `namapper_sqlite` 导入。

```rust
use nasa::mapper::sqlite::{Mapper, PageRequest};

#[Mapper(datasource = "local", cache = false)]
trait Records {
    /// 业务作用：读取记录总数。
    /// 参数说明：无。
    /// 返回：查询成功的总数，表不存在或来源不可用时失败。
    #[Query(sql = "SELECT count(*) FROM records", fetch = "scalar")]
    async fn count(&self) -> anyhow::Result<i64>;

    /// 业务作用：新增一条记录。
    /// 参数说明：
    /// - `name`：记录名称。
    /// 返回：成功影响行数，约束或数据库失败时返回错误。
    #[Insert("INSERT INTO records(name) VALUES (#{name})")]
    async fn insert(&self, name: &str) -> anyhow::Result<u64>;

    /// 业务作用：按稳定主键顺序读取指定分页的记录。
    /// 参数说明：
    /// - `page`：已在业务入口确认最大页大小的分页输入。
    /// 返回：本页的主键和名称；数据库或来源不可用时失败。
    #[Query("SELECT id, name FROM records ORDER BY id LIMIT #{page.limit} OFFSET #{page.offset}")]
    async fn page(&self, page: PageRequest) -> anyhow::Result<Vec<(i64, String)>>;
}

/// 业务作用：在外部分页请求进入数据库前落实业务允许的单页上限。
/// 参数说明：
/// - `mapper`：绑定 local 来源的业务 Mapper。
/// - `page`：由构造器或反序列化得到的分页输入。
/// 返回：不超过 500 条的本页记录；非法页大小在 SQL 执行前拒绝。
async fn list_records(
    mapper: &RecordsClient,
    page: PageRequest,
) -> anyhow::Result<Vec<(i64, String)>> {
    page.validate_with_max_page_size(500)?;
    mapper.page(page).await
}
```

`RecordsClient::new()` 在首次调用时固定受管 datasource 的 owner。standalone 可用 `RecordsClient::new().with_datasource(owner.datasource())?` 显式绑定。名称必须与 Mapper 声明一致。旧 client 在停机后立即拒绝新调用，不能静默连接同名新实例。

配置中的 `driver` 选择实际数据库；宏导入路径选择编译期行类型和绑定方式。MySQL 用 `nasa::mapper`，PostgreSQL 用 `nasa::mapper::pgsql`，SQLite 用 `nasa::mapper::sqlite`。三者可以同时编入一个应用；不能只改 driver 就把 SQLite 专有 SQL、类型或冲突处理自动翻译成其它方言。

## SQL 与事务

`#{value}` 生成匿名参数；`IN (#{ids})` 展开有界列表；动态 `if/choose/where/set/trim/foreach` 与声明式安全排序沿用共享合同。SQLite 字符串不把反斜线当引号转义。字符串和注释中的占位文本保持字面含义；SQL 代码中的原始 `?`、`:name`、`@name`、`$name` 被拒绝。`Vec<u8>`、字节 slice/array 在普通值位置绑定为 BLOB；显式 `IN (#{ids})` 列表位置按元素展开，包括字节列表。BLOB 列表使用 `Vec<Vec<u8>>`，单个 BLOB 的分组 IN 值可写为 `IN ((#{payload}))`。

BLOB 单值规则同时适用于静态 SQL、动态分支及重复占位符；空 BLOB 与 NULL 保持不同值。动态分支中的显式 IN 和 foreach 仍按元素展开，启用分支时的空列表被拒绝。普通非字节集合不能放入单值占位符。

`PageRequest` 保留 `#{page.limit}`、`#{page.offset}` 字段绑定及原有 JSON 结构，但字段只读，非法反序列化在 SQL 执行前拒绝。业务更新使用 `set_offset_limit`，失败保留原值；不支持 `PageRequest` 结构体字面量或直接字段赋值。`new` 默认最大页大小 1000，`with_max_page_size` 允许显式自定上限，反序列化输入应在业务入口调用 `page.validate_with_max_page_size(业务上限)?`。普通整数参数不会被隐式改写。完整用法见 [分页输入与更新](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/namapper-core/README.md#分页输入与更新)。

`Query` 默认只读。写 RETURNING 必须声明 `cache = false, flush_cache = true`，成功返回前会耗尽全部结果；写后映射失败会使环境事务 rollback-only，并隔离可能不可信的缓存。`StreamQuery` 仅只读，限制总行数、字节数、消费空闲和执行时间。流离开事务体前必须释放。

静态 `Query(checked = true)` 使用与 SQLite schema 对应的 SQLx 编译期校验或 offline 元数据；动态模板和 StreamQuery 不支持该组合。构建资料必须来自应用自己的迁移计划，运行时仍使用同一受控 executor。普通 SQL、类型和冲突策略不会在 driver 之间自动翻译。

`StreamQuery` 的消费预算在流返回给业务前登记。`stream_idle_timeout_ms` 覆盖未开始消费、逐行消费间隔和已进入缓冲的小结果集；等待数据库执行下一行不计作消费者停顿。到期后返回一次错误并释放内部流，执行单元负责真实原生关闭；旧流仍被业务持有也不能永久占用池的借用容量。事务内消费到期不允许外层继续提交。

每次正常交付还会核验 SQL 总期限与来源排空、隔离和关闭状态，已有缓冲不能越过这些边界。忙 VM 和原生锁等待也合作式检查消费许可；主动等待数据库产生下一行不会被误判为空闲。已确认的数据库失败保留原始类型和错误码，终止错误之后继续轮询只得到流结束。

`tx = "auto"` 加入同源环境事务，否则独立执行；`mandatory` 要求已存在事务；`never` 拒绝任何 driver 的环境事务。缓存读取之前也执行这些门禁。事务边界使用 `nasa::tx::sqlite::{run_for, transactional}`，不会跨 spawn 自动传播或跨库原子提交。

## 缓存与观测

共享 `MapperL2Cache`、codec、静态方法目录、指标与告警合同。Redis L2 启用 `mapper-redis-cache-sqlite`，GroupedCache 启用 `mapper-cache-grouped-sqlite`。Application 通过 `mapper_cache` 配置安装缓存，仍需对应 Redis 组件和 datasource 配置。

SQLite 缓存键隔离 driver、datasource、Mapper 和运行期 owner；缓存值携带该 owner 的提交序号。同源任意受控写入和事务提交使旧结果不可命中，延迟写回也不能恢复旧代际。底层 single-flight 只用于已确认只读的加载。每次缓存命中前用原生 prepare 验证最终 SQL 的只读性与文件身份；命中仍会短暂借用连接。

事务内默认绕过共享 L2。显式 `cache_in_tx = true` 允许读取已有共享结果，其弱一致语义不代表当前事务快照，也不保证读到事务内刚写入的值；事务中的未命中查询直接读取数据库，不进入 single-flight，也不将未提交快照写回共享缓存。提交结果不确定或失效失败时，该 owner 余下生命周期旁路共享缓存。默认日志不输出参数；受管 `sql.observability` 配置控制固定 driver 标签 `sqlite` 下的方法、数据库执行、流和连接观测。

文件部署、只读限制、备份与恢复详见 [natx-sqlite](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/natx-sqlite/README.md)。SQLite 不提供 Saga、Outbox、Inbox 持久后端。
