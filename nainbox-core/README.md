# nainbox-core

`nainbox-core` 定义 Inbox 消费去重的后端中立合同。`InboxStore` 只表达 ambient transaction 内的
claim，不暴露 MySQL/PostgreSQL 连接类型；`InboxProcess` 和 `InboxTransactionError` 区分已提交首次处理、
已提交重复消息、明确回滚与提交结果不确定。它没有连接池、全局状态或运行期配置。

数据库 adapter 和自定义 transport 可直接依赖本合同：

```toml
[dependencies]
nainbox-core = "2.0.0"
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

## 去重标记的保留清理合同

去重标记不再只增不减。`InboxRetentionPolicy` 定义按消费命名空间的过期清理，
`DurableInboxRetention` 由持有 owner 的执行者每轮调用。使用 Application 时，推荐通过
`InboxRetentionPlan` 把计划交给唯一生命周期所有者；Application 只在 Ready 后启动串行
fixed-delay 循环，停机先停止新轮次，再等待当前轮次按预算收口。直接使用 adapter 的调用方仍可自行
驱动轮次，但必须保持同一计划不并发重叠：

- `redelivery_horizon_ms` **必填无默认**——它是消息源的最大重投视界(如 Kafka 源 topic 保留期
  与消费重置策略中的较大者)，只有业务能声明；`validate()` 强制
  `processed_min_age_ms >= redelivery_horizon_ms`，删除窗口小于重投视界必然让窗口外重投的同
  `message_id` 再次判为首见造成二次消费，在校验期拦截。
- 实现以后端原生互斥保证同一 `consumer_name` 同时至多一个清理者，竞争失败以
  `claim_contended` 报告而不是并发删除；cutoff 取整轮开始时刻且轮内不随时间推进；分批删除至
  空批或时间预算耗尽。时间预算覆盖取连接、取锁、删除、统计和解锁的完整墙上时间，而不只是删除循环。
  轮次取消、超时或解锁结果不确定时，adapter 必须关闭物理连接，不能把可能仍持有会话锁的连接放回池。
  轮次必须在环境事务外运行，删除批次各自提交；否则报告会早于外层事务结果，owner 锁也可能在删除
  提交前释放。
- 轮次报告四项(deleted、claim_contended、budget_exhausted、oldest_candidate_age_ms)全部
  进入观测，不允许静默轮次。
- 超过视界的重复消息本就无法判重，属既有合同边界而不是清理引入的损失。

Application 入口需要启用 `nasa` 的 `application,inbox` 或 `application,inbox-pgsql`，并在 UserHook
显式登记：

```rust
use std::time::Duration;
use nasa::application::InboxRetentionPlan;
use nasa::inbox::InboxRetentionPolicy;

let policy = InboxRetentionPolicy {
    redelivery_horizon_ms: 86_400_000,
    processed_min_age_ms: 86_400_000,
    batch_limit: 500,
    round_time_budget_ms: 2_000,
};
app.configure_inbox_retention(InboxRetentionPlan::new(
    "orders",
    "order-projection",
    policy,
    Duration::from_secs(30),
)?).await?;
```

受管循环公开六个无标签指标：`napp_inbox_retention_rounds_total`、
`napp_inbox_retention_deleted_total`、`napp_inbox_retention_claim_contended_total`、
`napp_inbox_retention_budget_exhausted_total`、`napp_inbox_retention_failed_rounds_total` 和
`napp_inbox_retention_oldest_candidate_age_ms`。datasource、consumer 和 message id 不进入 label；进程内
需要账目时可读取 `Application::inbox_retention_snapshot()`。

## YML 配置

本 crate 不读取 yml。持久实现使用的 datasource、表和事务由 adapter 负责。

## 主要边界

- `Duplicate` 表示此前成功事务已经提交，调用方应跳过副作用并正常确认消息。
- `Claimed` 不是永久锁；若当前事务回滚，后续重投仍可再次取得。
- `CommitRejected`、`CommitUncertain` 与 `RollbackFailed` 禁止按普通有限预算转入死信，原消息必须持续重投。
- Inbox 只保护同一数据库事务内的副作用；外部 HTTP 或消息发布需使用目标幂等键或 Outbox。
