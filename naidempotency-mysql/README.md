# naidempotency-mysql

`naidempotency-mysql` 实现持久化 `IdempotencyStore`。事务内调用时记录与业务写共享 `natx`
ambient MySQL 事务；事务外调用时提供跨重启、跨副本的持久响应重放。

通过门面 `idempotency-mysql` feature 使用 adapter，公共状态机类型位于 `nasa::idempotency`：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "idempotency-mysql"] }
```

```rust
#[nasa::application("db")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let store = app.idempotency_store_named("orders").await?;
    // store 可由无 Web 的 Service 或 Batch 工作负载使用。
    Ok(())
}
```

上述入口用于 Batch 工作负载；Service 在 initializer 或 `serve_when_ready` 内取得 store，
UserHook 仅登记计划，此时标准 adapter 尚未装配。

## YML 配置

Application 用 `idempotency_stores` 选择命名 datasource，在迁移后验证 schema、登记关闭 owner。
独立构造器 `new()` 绑定 `default`；`with_datasource(name)` 固定事务内外使用的命名库。

```yaml
application:
  mode: batch
datasources:
  identity:
    url: ${APP_IDENTITY_MYSQL_URL}
    max_connections: 16
    migrations:
      mode: validate
idempotency_stores:
  orders:
    enabled: true
    driver: mysql
    source: identity
```

生产环境由 migration 创建 `idempotency_record_v2`；`ensure_schema` 只用于本地自举，不是发布期
schema 管理接口。

声明 Web 时可以设置 `web_default: true`，无需业务再安装 store；与手工安装冲突时拒绝启动。
取消或响应失败不证明业务已回滚，Web 不因此自动释放执行占位。停机后旧受管句柄拒绝新调用。
Batch 通过 `MIGRATION_PLANS` 在工作负载前执行迁移。

## 主要边界

- 记录主键包含 tenant、subject、route 和 client key，任一分量变化都不是同一请求。
- 在途 lease 默认五分钟后允许新 owner 接管；最长业务执行时间必须与该合同匹配。
- 同事务强幂等要求 store 调用和业务 SQL 使用同一个 datasource。
- 事务外中间件路径是持久 response-cache 语义，不能替代业务数据库唯一约束。
- 数据库错误统一脱敏，不回显 SQL、凭据或请求正文。
