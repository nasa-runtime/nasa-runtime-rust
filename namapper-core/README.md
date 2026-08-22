# namapper-core

`namapper-core` 定义后端中立的 Mapper SQL 渲染、分页、排序、缓存和观测合同。数据库连接与 SQLx
类型实现在 `namapper` 和 `namapper-pgsql` 中。

## SQL 与缓存合同

结构化 SQL 使用 `SqlNode::{Text, Bind, BindList}` 表达文本与参数边界。`render_postgres` 只在 bind
节点产生连续 `$n`，不会扫描或改写文本中的 `?`，因此字符串、quoted identifier、块注释和 JSON
操作符不会与参数编号混淆。空列表没有统一 SQL 业务语义，会明确返回错误。

分页通过有界 `PageRequest` 提供可直接 bind 的 `limit`/`offset`；动态排序通过 `MapperOrderField`
白名单输出列名；`MapperCacheMeta`、`MapperL2Cache`、codec、metrics 和 after-commit 清理目标用于让不同
数据库 adapter 共享同一缓存语义。进程内 single-flight、版本化/fallback codec 和默认注册槽也在本
crate 单点实现，因此同一个手写缓存、codec 或指标实现可同时供 MySQL 与 PostgreSQL Mapper 使用。

启用 `redis-cache` 后还提供 `RedisMapperL2Cache` 与
`RedisDistributedSingleFlightMapperL2Cache`。Redis 实现只依赖缓存合同，不依赖 SQLx driver；
PostgreSQL-only 应用使用它不会引入 MySQL runtime。

## 主要边界

该 crate 不创建数据库连接、不执行 SQL，也不根据数据库 URL 选择后端。普通应用应使用 `namapper` 或
`namapper-pgsql`；adapter 与宏实现可直接依赖本 crate 的稳定合同。Redis 连接仍由业务或受管应用创建，
随后显式安装为默认 Mapper L2。
