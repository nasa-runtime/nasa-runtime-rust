# nainbox-core

`nainbox-core` 定义 Inbox 消费去重的后端中立合同。`InboxStore` 只表达 ambient transaction 内的
claim，不暴露 MySQL/PostgreSQL 连接类型；`InboxProcess` 和 `InboxTransactionError` 区分已提交首次处理、
已提交重复消息、明确回滚与提交结果不确定。它没有连接池、全局状态或运行期配置。

数据库 adapter 和自定义 transport 可直接依赖本合同：

```toml
[dependencies]
nainbox-core = "1"
```

```rust
use nainbox_core::{InboxClaim, InboxStore};

async fn consume(store: &dyn InboxStore) -> anyhow::Result<()> {
    match store.claim("order-projection", "message-42").await? {
        InboxClaim::Claimed => apply_business_change().await?,
        InboxClaim::Duplicate => {}
    }
    Ok(())
}
```

也可使用 `claim.should_process()` 简化分支，但首次副作用仍必须保持在取得 claim 的同一事务内。

## YML 配置

本 crate 不读取 yml。持久实现使用的 datasource、表和事务由 adapter 负责。

## 主要边界

- `Duplicate` 表示此前成功事务已经提交，调用方应跳过副作用并正常确认消息。
- `Claimed` 不是永久锁；若当前事务回滚，后续重投仍可再次取得。
- `CommitRejected`、`CommitUncertain` 与 `RollbackFailed` 禁止按普通有限预算转入死信，原消息必须持续重投。
- Inbox 只保护同一数据库事务内的副作用；外部 HTTP 或消息发布需使用目标幂等键或 Outbox。
