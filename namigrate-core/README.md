# namigrate-core

`namigrate-core` 定义数据库 migration 的后端中立裁决合同。它只处理版本、checksum、可逆性、事务属性和
已应用状态，不保存或输出 SQL 正文，也不依赖具体数据库驱动。

MySQL 与 PostgreSQL adapter 使用同一套结论：pending、extra、checksum drift、dirty、锁等待超时和
后端失败。数据库锁、catalog 查询以及 migration 执行留在对应 adapter 中。

## 裁决模型

adapter 把嵌入项转换为 `EmbeddedMigration`，把 catalog 行转换为 `AppliedMigration`，再调用
`compare_migrations`。结果中的 `pending`、`extra`、checksum 不一致和未成功记录互斥表达不同风险；
`ensure_valid` 与 `ensure_applicable` 把结果收敛为稳定 `MigrationError`，不暴露 SQL、schema 或 endpoint。

`MigrationSettings` 同时保留 `Disabled`、`Validate`、`Apply` 三种运行模式以及有界锁等待设置。core 只
校验合同，不决定锁类型、事务边界、catalog 名称或非事务 DDL 的恢复方式。

## 边界

- 不依赖 SQLx 的 MySQL/PostgreSQL feature，也不持有连接池。
- 不解析 SQL 文本，不推断 migration 是否能安全重放。
- `allow_dirty = true` 没有通用安全语义，因此会稳定拒绝；业务必须先依据数据库事实处理未完成状态。
- 普通应用应依赖对应数据库 adapter，由 adapter 保证锁、catalog、执行和连接生命周期。

该 crate 适合 adapter 作者直接依赖；普通应用应使用 `namigrate` 或 `namigrate-pgsql`。
