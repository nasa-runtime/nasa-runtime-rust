# naidempotency-redis

`naidempotency-redis` 实现带 TTL 的跨副本响应重放 store。`begin` 用 `SET NX PX` 原子占位，
`complete` / `abort` 用脚本同时校验 fingerprint 和 lease，避免旧 owner 覆盖新请求。

通过门面 `idempotency-redis` feature 使用 adapter；公共状态机类型位于 `nasa::idempotency`：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["application", "idempotency-redis"] }
```

```rust
#[nasa::application("redis")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let store = app.idempotency_store_named("orders").await?;
    Ok(())
}
```

上述入口用于 Batch 工作负载；Service 在 initializer 或 `serve_when_ready` 内取得 store，
UserHook 仅登记计划，此时标准 adapter 尚未装配。

## YML 配置

连接配置归 `redis:`；命名 store 复用来源，标准装配使用 adapter 默认 TTL。
独立构造器 `with_ttls` 可显式指定 in-flight 和 completed TTL。

```yaml
application:
  mode: batch
redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2
idempotency_stores:
  orders:
    enabled: true
    driver: redis
    source: default
```

平铺 `redis` 配置注册为 `default`；使用 `redis.properties.<name>` 时，`source` 必须改为对应名称。

## 执行与重放架构

`begin` 原子竞争占位后，业务按公共状态机处理首次执行、在途冲突或响应重放。只有持有相同
fingerprint 与 lease 的 owner 才能 `complete` 或 `abort`。租约失效不代表业务副作用不存在，
HTTP 取消也不能作为释放占位的证据；重要业务仍须依赖数据库中的持久幂等事实。

## 主要边界

- 这是 response-cache 语义，不适合作为资金、库存等强幂等的最终事实源。
- in-flight TTL 必须大于最长处理时间；completed TTL 定义允许重放的业务窗口。
- TTL 必须能转换为正的 Redis 毫秒值，零值和溢出会失败。
- 损坏记录、连接错误和脚本失败都返回脱敏错误，调用方应 fail closed。
- key 使用完整业务命名空间；含旧分隔符的输入会切换到长度定界摘要，避免碰撞。
- 无 Web 的 Service/Batch 可以直接取得 store；声明 Web 时可设置 `web_default: true`。
- 标准与手工 Web store 不能重复安装；停机后旧受管句柄拒绝调用。
- HTTP 取消或响应失败不会自动释放执行占位，结果未知时不能据此重放业务副作用。
