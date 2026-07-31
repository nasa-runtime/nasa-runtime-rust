# naidempotency-mysql

`naidempotency-mysql` 实现持久化 `IdempotencyStore`。事务内调用时记录与业务写共享 `natx`
ambient MySQL 事务；事务外调用时提供跨重启、跨副本的持久响应重放。

该 adapter 当前不是独立门面 feature，应用按需直接引入，同时仍从 `nasa::application` 使用公共合同：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "tx", "web"] }
naidempotency-mysql = { version = "1" }
```

```rust
use std::sync::Arc;

#[nasa::application("db", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.set_idempotency_store(Arc::new(
        naidempotency_mysql::MySqlIdempotencyStore::new(),
    ))?;
    Ok(())
}
```

## YML 配置

本 crate 不新增配置根，复用 `database:` / `datasources:` 和 `natx` 默认 datasource。

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 16
  migrations:
    mode: validate
```

生产环境由 migration 创建 `idempotency_record_v2`；`ensure_schema` 只用于本地自举，不是发布期
schema 管理接口。

## 主要边界

- 记录主键包含 tenant、subject、route 和 client key，任一分量变化都不是同一请求。
- 在途 lease 默认五分钟后允许新 owner 接管；最长业务执行时间必须与该合同匹配。
- 同事务强幂等要求 store 调用和业务 SQL 使用同一个 datasource。
- 事务外中间件路径是持久 response-cache 语义，不能替代业务数据库唯一约束。
- 数据库错误统一脱敏，不回显 SQL、凭据或请求正文。
