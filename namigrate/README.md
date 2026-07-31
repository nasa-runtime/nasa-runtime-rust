# namigrate

`namigrate` 提供 MySQL migration 启动门禁。它支持 `disabled`、`validate` 和 `apply` 三种模式，
在监听流量前检查 pending、checksum drift 和 dirty 状态；`apply` 还使用同 session advisory lock
串行化多个实例。

业务通过应用门面登记嵌入式 migrator：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "tx", "web"] }
sqlx = { version = "0.8", features = ["migrate", "mysql"] }
```

```rust
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[nasa::application("db", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_migrations("default", MIGRATOR)?;
    Ok(())
}
```

不使用 Service 生命周期的批处理可显式调用 `run_gate`，并自行保证它发生在业务读写之前。

## YML 配置

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
| `lock_timeout_ms` | `30000` | `apply` 获取池连接、计算锁 ID 和竞争锁的统一绝对预算；`0` 使用无显式上限合同。 |
| `allow_dirty` | `false` | `true` 永远被拒绝；保留字段只为给旧配置稳定报错。 |

## 主要边界

- 生产常态使用 `validate`；`apply` 只用于单实例、本地环境或专门迁移任务。
- dirty 代表 DDL 可能部分提交，运行时无法安全推断修复动作，必须人工检查。
- advisory lock、查询和执行绑定同一 MySQL session；取消时关闭 session 兜底释放锁。
- 公开错误只含版本和稳定分类，不包含 SQL、schema 正文或连接信息。
