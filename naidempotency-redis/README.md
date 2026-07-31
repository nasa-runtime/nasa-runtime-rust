# naidempotency-redis

`naidempotency-redis` 实现带 TTL 的跨副本响应重放 store。`begin` 用 `SET NX PX` 原子占位，
`complete` / `abort` 用脚本同时校验 fingerprint 和 lease，避免旧 owner 覆盖新请求。

该 adapter 当前按需直接引入；公共状态机类型从 `nasa::application` 使用：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "redis", "web"] }
naidempotency-redis = { version = "1" }
```

```rust
use std::sync::Arc;
use std::time::Duration;

#[nasa::application("redis", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let redis = app.redis("default").await?;
    let store = naidempotency_redis::RedisIdempotencyStore::with_ttls(
        redis,
        Duration::from_secs(300),
        Duration::from_secs(86_400),
    );
    app.set_idempotency_store(Arc::new(store))?;
    Ok(())
}
```

## YML 配置

连接配置归 `redis:`；in-flight 和 completed TTL 由构造器显式传入。

```yaml
redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
```

## 主要边界

- 这是 response-cache 语义，不适合作为资金、库存等强幂等的最终事实源。
- in-flight TTL 必须大于最长处理时间；completed TTL 定义允许重放的业务窗口。
- TTL 必须能转换为正的 Redis 毫秒值，零值和溢出会失败。
- 损坏记录、连接错误和脚本失败都返回脱敏错误，调用方应 fail closed。
- key 使用完整业务命名空间；含旧分隔符的输入会切换到长度定界摘要，避免碰撞。
