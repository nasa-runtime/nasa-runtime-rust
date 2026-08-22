# naidempotency-pgsql

`naidempotency-pgsql` 为 `naidempotency::IdempotencyStore` 提供 PostgreSQL 持久实现。事务内调用时，
幂等记录与业务写复用同一命名 datasource 的 `natx-pgsql` ambient transaction；事务外调用时提供跨重启、
跨副本的响应重放。

主键由 tenant、subject、route 与 client key 共同组成。首次执行通过目标主键的
`ON CONFLICT ... DO NOTHING` 竞争；在途租约到期后，新 owner 会递增 generation 并更换随机 lease，
旧 owner 的 complete/abort 因 fingerprint 与 lease 条件不再成立，不能覆盖或删除新记录。

```toml
[dependencies]
naidempotency = "1"
naidempotency-pgsql = "1"
natx-pgsql = "1"
```

业务经门面使用时开启 `idempotency-pgsql`，从 `nasa::idempotency::pgsql` 取得 store，并从
`nasa::idempotency` 取得公共状态类型；与 `application` 组合时复用受管 PostgreSQL datasource。

## 初始化与使用

```rust
use naidempotency::{ExecutionLease, IdempotencyStore, RequestFingerprint};
use naidempotency_pgsql::PgIdempotencyStore;

natx_pgsql::try_init_datasource("identity", pool)?;
let store = PgIdempotencyStore::with_datasource("identity")?;

let outcome = natx_pgsql::run_for("identity", async {
    store.begin(&key, RequestFingerprint(fingerprint), ExecutionLease(lease)).await
}).await?;
let _ = outcome;
```

生产结构由 migration 创建；`ensure_schema` 只用于显式自举，不读取 Application YAML。

本 crate 不新增配置根或后台任务。调用方按 `FirstExecution`、`Replay`、`ConcurrentInFlight`、
`FingerprintConflict` 与事务结果分类观测业务结论；tenant、subject、route 与 client key 不应直接作为
无限指标 label。

## 事务结果边界

`run_decided` 把 PostgreSQL 提交阶段映射为 `PgIdempotencyTransactionError`。`OutcomeUnknown` 表示 COMMIT
已经发出但结果无法确认；调用方不得据此重新执行有副作用闭包，只能按同一 idempotency key 重查持久事实。

## 边界

- 只有 `idempotency_record_v2_pkey` 由目标化 `ON CONFLICT` 吸收为同一幂等 key 竞争；其它约束失败保持
  数据库错误。
- 租约到期只允许相同 fingerprint 接管；不同 payload 的 fingerprint 冲突不会随时间失效。
- 时间以数据库生成的 epoch 毫秒保存，不引入 session 时区语义。
- store 不记录 SQL、连接信息、请求体或响应内容；数据库失败只返回脱敏分类。
- 请求体与可重放响应大小仍由使用 `naidempotency` 公共合同的治理层限制，adapter 不扩大这些上限。
