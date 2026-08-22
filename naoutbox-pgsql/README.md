# naoutbox-pgsql

`naoutbox-pgsql` 把业务事实与待发布事件写入同一个命名 PostgreSQL datasource 和 ambient transaction，
消除“业务已提交但消息没有持久化”的窗口。dispatcher 以数据库 owner 租约和递增 fencing token 取得
global 或单 lane 权威，再通过 `FOR UPDATE SKIP LOCKED` 按稳定 `id` 升序领取候选；只有下游已确认的
成功前缀会按精确 `id + event_id` 标记完成。

## 初始化与使用

```toml
[dependencies]
naoutbox-core = "1"
naoutbox-pgsql = "1"
natx-pgsql = "1"
```

```rust
use naoutbox_core::OutboxEvent;
use naoutbox_pgsql::PgOutbox;

#[natx_pgsql::transactional(datasource = "orders")]
async fn create_order(order_id: i64) -> anyhow::Result<()> {
    insert_order(order_id).await?;
    PgOutbox::with_datasource("orders")?
        .append_transactional(&OutboxEvent::new(
            "Order",
            order_id.to_string(),
            "OrderCreated",
            serde_json::to_vec(&order_id)?,
        ))
        .await?;
    Ok(())
}
```

## 架构与顺序

- 写侧固定 default 或命名 datasource；关键双写使用 `append_transactional`，缺少 ambient transaction
  或 datasource 不一致时在 SQL 前拒绝。
- `ON CONFLICT` 只声明 `outbox_event_event_id_key`；其它约束错误不会被当成幂等追加成功。
- global dispatcher 与每个 lane 使用独立 owner 行。取得 owner 会递增 fencing token；失权 owner 不能
  标记成功、累计死信预算或执行保留删除。
- 每条发布前都会续租并复验 fencing token；单条发布等待上限为 25 秒，超时按 `Transient` 保留重投。
  每个已取得权威的批次按 `id` 升序发布，遇首个失败停止，只确认本轮成功前缀。
- 取消、断连或提交结果不确定时，未确认行保持可重投；下游必须按 `event_id` 幂等去重。
- append 唤醒只在最外层事务明确提交后产生。回滚与提交结果不确定不产生唤醒，数据库轮询始终是最终事实。
- global dispatcher 与 lane dispatcher 是两种互斥运行模式，不能同时处理同一张 `outbox_event` 表；
  lane 模式必须为每个已配置 lane 启动对应 dispatcher。

## lane、配额与保留

lane 路由和租户配额都是显式安装的进程级冻结策略。未安装路由时全部事件进入 `global`；列入配额的租户
必须先在同源事务中完成账本对账，随后 append 才能原子预留名额。投递或进入死信与名额释放同事务提交。
同一 `event_id` 的幂等追加不新增待投递事实、不重复占用配额，也不产生无效提交唤醒。

保留清理只处理达到最小年龄的已投递行，或具备独立批准和归档收据的死信。待投递行永不进入候选；
删除使用有界批次、精确主键集合和 retention owner fencing，不运行无界 `DELETE`。owner 竞争、候选查询、
归档等待和提交前删除步骤都受单轮时间预算约束；预算耗尽时源行保留，下一轮先复验可能已经生成的归档收据。

## 配置与观测

生产结构由 migration 创建；`ensure_schema` 只用于显式自举。时间统一存 epoch milliseconds，业务布尔
使用 PostgreSQL `BOOLEAN`，payload 使用 `BYTEA`。

本 crate 不读取 Application YAML，也不自行启动后台任务。standalone 宿主需要显式初始化
`natx-pgsql` registry、安装冻结策略并拥有 dispatcher 生命周期；通过 `nasa` 同时开启 `application` 与
`outbox-pgsql` 时，`napp` 根据 `datasource_ref` 选择本后端并托管 dispatcher、readiness 与停机。

standalone 观测可调用 `pending_count`、`dead_count` 与 `pending_count_channel` 读取已提交低基数事实；
Application 受管模式会把积压、失败轮次、owner、lane、配额和 retention 状态汇入统一指标与 readiness。
指标标签不包含 event id、tenant、payload 或 datasource endpoint。
