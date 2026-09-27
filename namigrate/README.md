# namigrate

`namigrate` 提供 MySQL migration 启动门禁。它支持 `disabled`、`validate` 和 `apply` 三种模式，
在监听流量前检查 pending、extra、checksum drift 和 dirty 状态；`apply` 还使用同 session advisory lock
串行化多个实例。

版本、checksum 与已应用状态的比较由 `namigrate-core` 提供；`namigrate` 保留 MySQL catalog、锁和执行，
`run_gate` 使用默认数据源身份，`run_gate_for` 接受显式名称以区分受管连接观测。

## 门禁架构与连接观测

`validate` 取得池连接后只读 catalog 并校验，不获取 advisory lock、不执行迁移。
`apply` 先取得池连接和 advisory lock，再在同一 session 读取 catalog、校验并执行迁移；
仅在确认解锁后归还连接，未确认解锁的路径关闭物理连接，避免把 session lock 带回连接池。
池获取按 `purpose=migration` 记录等待、结果与取消；取得连接即结束等待计时，不把 catalog 查询、
锁竞争或 DDL 执行混入连接耗时，也不产生 Mapper 方法指标。

Application 从 `sql.observability` 装配等待日志与连接超时通知。超时通知默认只选 `mapper`，需要
迁移通知时显式在 `alerts.acquire_timeout.purposes` 加入 `migration` 并安装业务 `Notify`。
通知失败不改变 migration 门禁结果；锁预算仍从获取池连接开始计算，不因观测重置。

## 初始化与使用

直接依赖并在开放业务流量前执行门禁：

```toml
[dependencies]
namigrate = "2.0.0"
sqlx = { version = "0.9", default-features = false, features = ["macros", "migrate", "mysql"] }
```

```rust
use namigrate::{run_gate, MigrationMode, MigrationSettings, Migrator};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

let settings = MigrationSettings {
    mode: MigrationMode::Validate,
    ..Default::default()
};
let report = run_gate(&pool, &MIGRATOR, &settings).await?;
```

`namigrate` 不启动连接池，也不读取应用配置。调用方负责创建 `MySqlPool`、嵌入 migration，并保证
`run_gate` 成功发生在 listener Ready 和任何业务读写之前。

## YML 配置

下列形状由应用映射为 `MigrationSettings`，不是本 crate 自动读取的固定 schema。

```yaml
database:
  url: ${APP_MYSQL_URL}
  migrations:
    mode: validate
    lock_timeout_ms: 30000
    allow_dirty: false
```

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `mode` | `validate` | `disabled`、`validate` 或 `apply`。 |
| `lock_timeout_ms` | `30000` | `apply` 获取池连接和竞争锁的统一绝对预算；`0` 使用无显式上限合同。 |
| `allow_dirty` | `false` | `true` 永远被拒绝；保留字段只为给旧配置稳定报错。 |

## 主要边界

- 生产常态使用 `validate`；`apply` 只用于单实例、本地环境或专门迁移任务。
- dirty 代表 DDL 可能部分提交，运行时无法安全推断恢复动作，必须人工检查。
- advisory lock、查询和执行绑定同一 MySQL session；取消时关闭 session 兜底释放锁。
- `lock_timeout_ms` 从获取池连接开始约束锁取得阶段，不截断已经开始的 catalog 查询或 migration 执行；
  `0` 明确表示锁取得不设置外层截止时间。
- 公开错误只含版本和稳定分类，不包含 SQL、schema 正文或连接信息。
