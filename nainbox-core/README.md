# nainbox-core

`nainbox-core` 定义 Inbox 消费去重的最小裁决 `InboxClaim`。它没有连接、全局状态和运行期配置，
只表达“本事务是否首次取得消息”。

业务通过门面的 `inbox` feature 使用：

```toml
[dependencies]
nasa = { version = "1", features = ["inbox"] }
```

```rust
use nasa::inbox::InboxClaim;

match claim {
    InboxClaim::Claimed => apply_business_change().await?,
    InboxClaim::Duplicate => {}
}
```

也可使用 `claim.should_process()` 简化分支，但首次副作用仍必须保持在取得 claim 的同一事务内。

## YML 配置

本 crate 不读取 yml。持久实现使用的 datasource、表和事务由 adapter 负责。

## 主要边界

- `Duplicate` 表示此前成功事务已经提交，调用方应跳过副作用并正常确认消息。
- `Claimed` 不是永久锁；若当前事务回滚，后续重投仍可再次取得。
- Inbox 只保护同一数据库事务内的副作用；外部 HTTP 或消息发布需使用目标幂等键或 Outbox。
