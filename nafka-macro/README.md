# nafka-macro

`nafka-macro` 实现 `#[kafka_consumer]`，把无状态异步消费函数转换为 `nafka` consumer 实现并登记到
静态收集表。业务通过 `nasa::kafka::kafka_consumer` 使用，不直接依赖本宏 crate。

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["kafka"] }
```

## 生成与运行架构

```text
消费函数 + 属性 ──→ 编译期签名校验 ──→ 静态 consumer descriptor
                                             │
                                             └─→ nafka / Application 启动与消费
```

宏只生成类型适配和静态登记；非法参数组合或不支持的函数签名在编译期拒绝。连接、拉取、确认、重试、
停机和消费指标都由 `nafka` 运行时拥有，宏本身没有运行期状态或独立观测入口。

```rust
use nasa::kafka::kafka_consumer;

#[kafka_consumer(
    topics = ["orders"],
    event = "ORDER_CREATED",
    group = "order-worker",
    client = "default"
)]
async fn on_order(message: OrderCreated) -> anyhow::Result<()> {
    handle(message).await
}
```

支持四种唯一参数形态：`T`、`KafkaRecord<T>`、`Vec<T>` 和 `Vec<KafkaRecord<T>>`。需要手动确认时
必须使用带 `KafkaRecord` 的形态：

```rust
#[kafka_consumer(
    topics = ["payments"],
    event = "PAYMENT_CAPTURED",
    ack = "manual"
)]
async fn on_payment(record: nasa::kafka::KafkaRecord<Payment>) -> anyhow::Result<()> {
    apply(&record.value).await?;
    record.ack()?;
    Ok(())
}
```

## YML 配置

宏不读取 yml。`topics`、`event`、group 规则和目标 `client` 是编译期路由合同；broker、认证、
consumer 参数和受管 readiness 位于 `kafka:` / `kafkas:`。

```yaml
kafka:
  client_name: default
  bootstrap_servers: ${APP_KAFKA_BROKERS}
  group_id: order-worker
  container:
    consumers: collected
```

## 主要边界

- 只能标注无 receiver 的异步自由函数。
- `ack = "manual"` 不能用于 payload-only 参数。
- 普通 `group` 与广播 group 互斥。
- `client` 必须与受管配置名一致；未知或 disabled client 会在 Ready 前拒绝。
- 宏只生成静态登记，消费线程、重试、提交和停机由 `nafka` / `napp` 拥有。
