# namigrate-sqlite

在 SQLite 业务开放前执行 schema 门禁。`validate` 只读比较完整迁移计划，`apply` 在一个 `BEGIN IMMEDIATE` 事务中提交全部待应用脚本与历史；任意脚本失败时整批回滚。

通过 napp 的 `"db"` 组件复用与 Mapper 相同的 datasource 和文件 owner；业务显式提供 SQLx `Migrator`，配置只选择门禁策略。门禁不通过时 Application 不开放业务，Batch 不执行工作负载。

## 迁移架构与原子边界

```text
完整嵌入计划 → 检查 ID、选项、体积和 SQL/checksum 一致性
  → 同文件维护门与有限锁等待 → 读取标准历史和前缀校验
  → validate：同一只读快照比较
  → apply：BEGIN IMMEDIATE → 待应用 SQL + 历史写入 → 校验 → COMMIT
```

文件 owner 和维护门持续覆盖实际原生访问；取消或关闭超时不提前交出权威。一次 apply 只覆盖一个 SQLite 数据源，跨 datasource 的先前成功迁移不会随其它来源失败而撤销。迁移不生成业务 SQL、不自动发现文件，也不提供跨库事务或 SQL 方言转换。

## 配置与计划

通过 `nasa` 的 `application + tx-sqlite` 或 `application + mapper-sqlite` 使用同一 `"db"` 组件：

```yaml
datasources:
  local:
    driver: sqlite
    sqlite:
      path: ./data/local.db
      create_if_missing: true
      create_parent_directories: true
    migrations:
      mode: apply
      lock_timeout_ms: 5000
      execution_timeout_ms: 30000
```

默认模式为 `validate`。配置策略不等于 SQL 计划：Service 在 UserHook 调用 `app.configure_migrations("local", migrator)`；Batch 用 `MIGRATION_PLANS` 静态登记计划，在业务 main 之前执行 Prepare。显式配置非 disabled 的 SQLite migrations 却没有登记计划会阻止启动；无迁移的数据库可登记显式空计划。

使用 SQLx `Migrator::with_migrations` 和 `Migration::new` 构造完整计划；文件采用 `create_records.sql` 等语义名称，通过 `include_str!` 提供 SQL。`Migration::new` 中的正整数是持久迁移标识，不必编码到文件名。standalone 调用 `run_gate(&source, &migrator, &settings)` 或 `run_gate_with_timeout`。

可直接运行的语义文件、计划工厂与 Mapper 示例见 [SQLite 接入指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/sqlite.md)。默认锁预算为 30000 ms，执行预算为 30000 ms；`execution_timeout_ms` 从取得迁移事务后计整个批次，外层 Application 启动期限仍可提前终止等待。

迁移入口在访问历史或执行 SQL 前，按 SQLx `Migration::new` 的算法重新计算全部 up 项 SQL 原文的 checksum，并与计划携带的值核对。修改公开 `sql` 字段而保留旧 checksum、空摘要或忽略字符后与原文不一致的摘要均被拒绝，不改写历史；空白、换行和 Unicode 内容属于原文。`disabled` 跳过数据库门禁，仍要求计划自身合法。该校验不重建业务 schema，也不替代历史漂移检查。

## 历史与失败合同

框架只接受 main 数据库的标准 `_sqlx_migrations` 表，检查列、类型、主键、约束行形态及 checksum。历史必须是计划中已应用迁移的连续前缀；未知记录、失败记录、缺失中间项、checksum 漂移、同名 view、异常 trigger/index 和非法历史值都拒绝继续。

仅支持事务迁移，拒绝单项或全局 `no_tx`、`ignore_missing`、关闭 locking、自定义历史表及 create_schemas 选项。脚本不得控制 BEGIN/COMMIT/ROLLBACK/SAVEPOINT、修改迁移历史、ATTACH、TEMP、PRAGMA 或调用危险函数。迁移创建历史表、业务 DDL/DML 和历史记账同事务，提交前再检查历史和外键。计划最多 1024 项、SQL 总量 16 MiB，锁和执行预算必须有限。

validate 使用只读事务快照，缺失历史且存在待应用计划时失败，不偷偷建表。只读 source 禁止 apply。锁竞争、磁盘故障、提交未确认不自动重放 SQL；Application 不进入 Ready，调用方检查持久事实后决定下一步。维护与迁移共享同文件串行门，原生连接关闭前持续持有 owner。

`MigrationReport` 提供模式、嵌入数量和已应用数量；`disabled` 的跳过摘要不代表历史验证通过。迁移错误与 SQL 执行、取连接和锁等待分别保留边界，默认不泄露 SQL 或文件路径。部署应观察启动失败原因和 source 的隔离/readiness 状态，并在重新开放前确认持久事实。
