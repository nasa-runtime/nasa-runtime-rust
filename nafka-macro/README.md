# nafka-macro

`nafka-macro` 实现 `#[kafka_consumer]`，把无状态异步消费函数转换为 `nafka` consumer 实现并登记到
静态收集表。业务通过 `nasa::kafka::kafka_consumer` 使用，不直接依赖本宏 crate。

声明同时固定 topic、精确 event、目标 client、消费组与确认模式，减少手写类型适配，并在连接 broker 前拒绝不合法的路由声明。组名、事件名和确认责任与 Java `nasa-kafka` 的声明式消费保持共同业务合同；宏只负责声明，实际所有权由运行时管理。

```toml
[dependencies]
nasa = { version = "2.0.3", features = ["kafka"] }
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

/// 业务作用：按已登记路由处理订单事件。参数说明：message 为解码后的订单。返回：成功允许 Auto 确认，失败交由消费策略处理。
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
/// 业务作用：业务完成后确认付款输入。参数说明：record 为含当前确认权威的付款记录。返回：业务与确认均成功才完成本次调用。
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
- `event` 按精确 UTF-8 原文匹配，不裁剪或归一化；空串和仅含事件空白字符的名称在编译期拒绝。空白集合为 U+0009–000D、U+001C–0020、U+1680、U+2000–2006、U+2008–200A、U+2028–2029、U+205F、U+3000，与发布和消费一致；非空白原文中的首尾空格保留。
- `ack = "manual"` 不能用于 payload-only 参数。
- `execution = "partition_ordered"` 是默认顺序域；单条函数可使用 `execution = "key_ordered"`。
- 按键模式允许同分区不同原始 key 并发，同 key 串行，null key 构成分区屏障；批参数在编译期拒绝。
- 同组执行模式必须一致，显式 trait 注册也会执行同样的启动门禁；容量和超时由 nafka 的 behavior 配置约束。
- 普通 `group` 与广播 group 互斥。显式组名须为1至1024个 UTF-8 字节、不含 NUL，使用与 `event` 相同的纯空白判定；NEL、NBSP、U+2007、U+202F 有效。为避免原生配置改变身份，首尾含 U+0000–0020 时拒绝；其它合法原文不裁剪或归一化。省略 `group` 沿用源默认组，显式空字符串则拒绝。
- `broadcast_scope = "业务作用域"` 使用与 Java 一致的广播身份算法，`instance_id = "实例身份"` 可选；省略实例时沿用源配置或进程身份。两者不依赖函数路径。
- `broadcast_scope` 与 `group`、旧 `broadcast` 互斥；`instance_id` 只能与 `broadcast_scope` 同时声明。
- 旧 `broadcast` 保留原组名算法，不会自动迁移位点；稳定 ID 重复部署会产生组内竞争，临时 ID 不承诺重启接续。
- `client` 必须与受管配置名一致；未知或 disabled client 会在 Ready 前拒绝。
- 宏只生成静态登记，消费线程、重试、提交和停机由 `nafka` / `napp` 拥有。

运行时仍会复验配置默认组、显式 trait 注册和受管来源；通过宏编译不证明 broker 可达、分区已分配或资源授权成立。组名长度按 UTF-8 字节计算，合法名称始终原样使用；旧组不满足共同输入域时需要显式迁移，宏不会复制位点或自动改名。实际就绪、容量与失败分类见 [nafka](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md)。
