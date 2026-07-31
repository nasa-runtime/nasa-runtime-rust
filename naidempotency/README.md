# naidempotency

`naidempotency` 定义 provider-neutral 幂等状态机：首次执行、完成后重放、并发在途冲突和同 key
不同请求指纹冲突。它不把响应缓存称为 exactly-once；关键业务必须把幂等记录与业务副作用放进
同一业务数据库事务。

应用通过 `nasa::application` 使用核心类型并注入 store：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "web"] }
```

```rust
use std::sync::Arc;
use nasa::application::InMemoryIdempotencyStore;

#[nasa::application("web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.set_idempotency_store(Arc::new(InMemoryIdempotencyStore::new()))?;
    Ok(())
}
```

Web 中间件按 `(tenant, subject, route_id, client_key)` 建命名空间，并用请求等价类 SHA-256 指纹
区分合法重放和 key 误用。

## 状态合同

`begin` 返回：

- `FirstExecution`：当前 owner 获得带随机 lease 的在途记录，可以执行业务。
- `Replay`：返回已保存响应，不再执行业务。
- `ConcurrentInFlight`：同指纹请求仍在执行，通常映射为 409。
- `FingerprintConflict`：同 key 对应不同请求，通常映射为 422。

`complete` / `abort` 必须同时匹配 fingerprint 和 lease，旧请求不能覆盖或删除新 owner 的记录。

## YML 配置

核心 crate 不读取 yml。请求体和可重放响应上限由 Web 治理层配置；持久后端连接由对应 adapter 配置。

## 主要边界

- `InMemoryIdempotencyStore` 只适合允许重启后丢失记录的非关键场景。
- 保存响应前必须限制状态码、header 白名单和 body 大小。
- store 故障应 fail closed，不能在幂等不可用时继续执行副作用。
- 资金、库存等强幂等优先使用数据库唯一键和同事务 MySQL store。
