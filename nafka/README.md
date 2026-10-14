# nafka

`nafka` 是与 Java `nasa-kafka` 共享线路、身份和恢复合同的多数据源 Kafka 事件运行时，提供类型化发布与消费、属性宏消费者收集、
Auto/Manual Ack、组内多消费者、有界按键并发、消费组控制、同集群消费与输出事务、持久有限回放、可恢复的单目标转发、有限DLT回投与原文隔离、独立 producer lane，以及面向 `naws` 的借用式少拷贝
passthrough。
可选 Schema Registry 能力在同一门面下提供 Confluent wire envelope、schema ID 白名单、
有界正负缓存以及显式兼容性检查/注册，把消息编解码所需的远端 schema 访问与 Kafka 生命周期解耦。

当前公开配置结构、错误类型和默认策略要求调用方完成源码与配置迁移；`2.0.3` 不能按补丁号推断源码兼容。迁移动作见[接入与升级](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.md#kafka-源码与配置迁移)。

## 核心价值与边界

每个 `KafkaProxy` 绑定一个命名源，多个实例可在同一进程连接不同集群；受管应用通过 `kafkas.<name>` 装配，并使用 `app.kafka(name)` 获取绑定该源的业务句柄。JSON 事件、key、有序 header、DLT 来源字段与持久任务格式对应 Java `nasa-kafka`。

| 能力 | 业务价值 | 必须保留的边界 |
|---|---|---|
| 多数据源与独立发布通道 | 一个应用隔离业务、审计等集群；控制消息与批量消息使用独立 producer 和额度 | 显式源名不回退；进程内存、CPU 和 broker 仍是共享的物理资源 |
| 按键有序与组内多消费者 | 同一来源、组、分区内同 key 保序，不同 key 或分区并行；增加消费槽分担拉取与执行 | 只提交连续安全前缀；慢调用实际退出前继续占用顺序和容量，不提供跨进程业务键锁 |
| 有界输入与失败治理 | 进程、源、组、分区、key 共同限制输入责任，重试与 DLT 保留明确责任 | 输入预留不等于整个进程 RSS 上限；未知交付不能当作失败已撤销 |
| 持久回放与跨集群转发 | 冻结历史范围、固定资源身份，Java/Rust 可按共同 checkpoint 接续 | 目标确认先于进度持久化；跨资源可能重复，须使用稳定事件 ID 幂等 |
| Kafka 事务 | 同集群输入、批准输出与消费组位点原子提交 | 不覆盖数据库、HTTP 或另一个 Kafka 集群 |
| 受管接入与可查询关闭 | 命名句柄、只读业务管理、当前 Ready、健康、指标和关闭责任贯通 | 宿主停止不等于资源已退出，资源退出也不等于全部业务成功 |

普通消费由唯一访问原生 consumer 的 owner 维护 poll、分配代次和提交；解码与业务执行使用有界资源。发布由独立 lane 观察真实交付。有限任务另持有 checkpoint 推进权，并随来源和目标的实际依赖一起排干。取消 future 或超时只结束调用方等待，不提前释放仍在执行的责任。

## 运行架构与确认时刻

```text
nasa::kafka / Application 命名配置 → KafkaProxy 单源资源
                                      ├─ KafkaHandle 受管业务权限
                                      ├─ producer lane → native producer → delivery observer
                                      ├─ registry → 组内 owner 槽 → poll → 有界路由执行
                                      │                                  ↓
                                      │                  Ack → 连续安全水位 → commit
                                      ├─ TransactionPlan → 专属 consumer / producer
                                      └─ Replay / Transfer / DLT 回投 → checkpoint
```

底层 librdkafka 类型收敛于内部适配层，业务使用组件自有类型。命名源冻结连接与安全配置，lane 独立保留发布责任，普通组的 owner 槽独占 consumer；增加消费槽不扩大组的共享额度。业务结果只有在当前 assignment 代次仍有效时才能推进提交前缀，失权 Ack 不能确认新 owner 的输入。

| 路径 | 成功证据 | 未知或中断的处理 |
| --- | --- | --- |
| 普通发布 | broker delivery 确认 | `fire` 受理不是送达；取消 future 不撤销在途发送 |
| 普通消费 | 业务确认形成安全前缀，broker 确认提交后释放输入责任 | 失败前缀阻止越过；外部副作用需要幂等 |
| 同集群事务 | 完整输入位点与批准输出的 commit 确认 | commit 未知时保护 owner，复用稳定身份恢复 |
| 有限任务 | 处理或目标确认、持久 checkpoint、进度发布依次完成 | checkpoint 前退出可能重投，不承诺跨资源原子性 |

Ready 是当前资源和分配快照；两段停机保留仍在执行的调用与交付责任，实际退出后才释放身份。容量、运行错误和关闭结果分别可观测，不能用超时或停止请求代替实际排干。

## Java 与 Rust 的共同合同

Java `KafkaClients` / `KafkaHandle` 与 Rust 的命名 `KafkaProxy` / Application 句柄提供对应的源选择和业务权限；listener / annotation 与 trait / macro 使用各自语言习惯。事件线路、普通及事务组身份、稳定事务 ID、DLT 来源和有限任务持久格式共享合同，适合跨语言交换事件及任务接续。

接续必须使用同一物理资源身份、相容计划和有效 checkpoint，事务恢复必须复用稳定部署身份。组名不裁剪或归一化，null/空 key 与 tombstone 不合并；配置键拼写、Future/CompletionStage 和原生参数不要求一致。完整 Java 入口见 [nasa-kafka](https://github.com/nasa-runtime/nasa-kafka)。数据库 Outbox/Inbox 适配不属于本组件，数据库与 Kafka 或两个 Kafka 集群之间也没有隐式原子事务。

## 业务入口

业务项目统一通过 `nasa::kafka` 使用，不直接依赖实现 crate：

```toml
[dependencies]
nasa = { version = "2.0.3", features = ["kafka"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

相关文档：

- [naws Kafka 集成](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naws/README.md)：WebSocket、socket.io 与 Kafka passthrough。
- [napp 受管生命周期](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#kafka-受管模式)：组件配置、Ready、健康和两段停机。
- [Schema Registry](#schema-registry)：wire、缓存、控制面、观测与能力边界。

## 能力概览

- JSON 与 ProtocolBytes 类型化发布/消费；二进制支持 `JSON_BYTES`、`VARINT_TLV`、
  `BITPACK_TLV`，`FAST_FIXED` 暂不开放。
- `#[kafka_consumer]` 收集无状态 async free function；trait 注册有状态消费者。
- route 级 `AckMode::Auto` / `AckMode::Manual`，统一由 owner 推进连续安全提交水位。
- `ExecutionMode::KeyOrdered` 允许同分区不同原始 key 并发，保留失败前缀后已完成的业务结果。
- publish、raw publish、tombstone、批发布、key、partition、timestamp、有序重复 header。
- 默认 lane 与命名 lane 物理隔离；每个 lane 独立 producer、queue、admission 和 delivery observer。
- 默认 group、命名 group、进程实例广播 group和固定分区 assign。
- 固定分区支持 `assign` 单条及 `assign_batch` 连续批处理器；句柄绑定具体 owner，旧句柄不能停止同名新任务。
- 可配置的业务尝试预算、退避与可靠 DLT；`max_consume_attempts=0` 不限制业务尝试次数，必需 DLT 保留失败输入持续重试，停止后不再开启新轮次。
- 控制命令、当前 Ready 门禁、健康状态和可注入指标出口。
- `PassthroughConsumer` 同步借用 Kafka payload/header，支持 Kafka 到本地 socket outbox 的少拷贝路径。
- 可选 Confluent-compatible Schema Registry client，默认只读、有界缓存并拒绝未批准 schema ID。
- `ReplayPlan` 冻结 LSO 范围，`ReplayHandle` 管理独立任务，持久 checkpoint 可与 Java 接续。
- `TransferPlan` 固定目标身份、分区映射和 schema 策略；目标真实确认后持久推进来源，保留共同事件 ID 和有界路径。
- `DltReplayPlan` 显式筛选死信来源并限定回投次数；无效证据停止或可靠隔离，保留原文与稳定事件身份。
- `TransactionPlan` 将同集群完整输入批次、批准输出与 group offsets 原子提交，专属 producer 和持续 poll owner 隔离普通发布。

## 同集群消费与输出事务

`KafkaProxy::transact(plan, Arc<dyn TransactionProcessor>).await` 启动专属 subscribe consumer 和事务 producer。`TransactionPlan` 明确命名源、实际物理 cluster ID、稳定部署身份、专属 group、输入 topic、输出分区白名单、Auto/Manual 确认及资源预算。受管应用通过 `KafkaHandle::transact` 使用同一合同。

事务 `group` 与普通实际组使用同一输入域：1至1024个 UTF-8 字节、不含 NUL、非共同纯空白，首尾不含 U+0000–0020；合法名称原样保留。`TransactionPlan::validate` 可独立预检，`transact` 对传入计划重新验证。非法组及构造专属原生配置时的框架预检失败返回 `TransactionError::InvalidConfiguration`，隔离冲突保留 `IsolationMismatch`；这些预检拒绝发生在事务身份登记、专属原生资源创建和业务处理启动之前。

同步回调通过 `TransactionContext::records` 读取原文，使用 `send(TransactionOutput)` 暂存批准目标的输出，Manual 模式还须 `ack(index)` 确认每条输入。`dead_letter` 在当前事务中保存失败原文和 DLT 来源证据，并确认对应输入。回调正常返回、整批确认且组 metadata 仍有效后，才发送输出、登记全部输入位点并 commit。发送回调成功只属于事务内部证据。

每条输出计入64字节固定开销、key、payload 与完整有序 header 字节，null 与空字节保持区别。额度超限、目标未批准、非法时间或头名等准入失败会锁存到整批，业务捕获错误不能让先前部分输出封存成功。上下文不能跨线程移动；已封存或失权时拒绝继续确认和发送。

`TransactionIdentity { application, slot, owner }` 的每个字段为1至256 UTF-8字节且不能包含NUL；按与 Java 相同的 `NSKTXI01` 长度前缀编码派生 `nasa-tx-` 加 SHA-256 的 transactional ID。同一部署槽恢复必须复用，正常活动槽必须互异；跨进程重复身份由 broker fencing。进程内仍活动的同集群身份会被拒绝，同源普通提交协调器也不能与事务 owner 共用 group。不要在该组混入外部普通非事务消费者。

broker 的 inter-broker listener 必须在 broker 所在网络内可达，包括事务协调器写入提交/中止标记的连接。仅有客户端地址可达不足以完成事务恢复；旧事务标记无法写入时，同一身份初始化可能持续返回并发事务或超时错误，任务不会进入 Ready，也不会读取新业务输入。初始化诊断记录静态阶段和原生错误码，不包含连接凭据或 broker 自由文本。
broker 的 `transaction.state.log.replication.factor` 与 `transaction.state.log.min.isr` 还必须满足实际可用副本条件；集群无法创建或写入 `__transaction_state` 时，普通消息可用并不能证明事务可用，初始化可能超时并保持未就绪。

输入强制 `read_committed`，显式冲突配置在登记前拒绝；下游消费者也须使用 `read_committed` 排除未提交输出。默认单批输入100条/8MiB、输出1000条/16MiB，回调10秒、事务60秒、原生操作10秒，最多3次已确认中止后的重试。每源最多16个事务 owner，每个 owner 同时执行一个同步批次；本模式不支持手动 assign、异步 handler 或 `KeyOrdered`。

专属 producer 继承源级默认 producer 配置，再固定事务身份、幂等、all ACK 和本计划预算，不复用普通 lane 实例。事务路径拒绝共同、consumer 或 producer 配置中的非空 `plugin.library.paths`，避免原生扩展绕过冻结的输入责任和输出白名单。

确认中止后，如果组代次变化、网络或原生等待期限使本批需要重读，会先关闭旧专属 producer，再由独立责任线程核对物理集群并初始化同一事务身份。consumer 在此期间持续 poll，Ready 保持 false；实际资源退出前不会释放任务身份，初始化失败会终止该 owner。

失败、缺确认、超时或失权会使完整批次中止，只有收到 abort 确认才允许重读。新定位代次必须重新取得组位置，不能沿用旧 metadata。commit 已进入完成过程后，取消不会切换到 abort；`CommitUnknown` 或 `AbortUnknown` 会保护 owner 并保留输入范围，由同一稳定身份初始化恢复，再按 broker 提交位置接续。

`TransactionHandle::status` 提供当前阶段、Ready、assignment epoch、输入范围和已确认计数；`await_ready` 与 `await_drained` 使用绝对期限。回调超时不证明线程退出，关闭等待超时不会释放仍在执行的身份；两段停机等待回调、producer 和 consumer 实际退出。普通 lane、其它数据源、数据库、HTTP 和 socket 操作不属于本次 Kafka 原子范围。

## Feature

| `nasa` feature | 用途 |
|---|---|
| `kafka` | 基础 Kafka 能力与 `#[kafka_consumer]` |
| `kafka-schema-registry` | Schema Registry client、wire envelope 与其凭据类型；同时启用 `kafka` 和 `secret` |
| `kafka-mysql-checkpoint` | 单主 MySQL 共享进度及跨主机接管；显式引入 SQLx，进入 `full` |
| `kafka-tls` | TLS、SASL_SSL、SCRAM；静态内嵌 OpenSSL |
| `kafka-gssapi` | Kerberos/GSSAPI，同时包含 TLS 支持 |
| `kafka-zstd` | Zstandard 压缩 |
| `ws-kafka` | `nasa::ws` 与 Kafka control/data plane 集成 |

启用 `kafka-gssapi` 时，Cyrus SASL 由目标系统提供。macOS 使用系统自带的动态库；
Linux 构建环境需要安装发行版提供的 Cyrus SASL 开发包，例如 Debian 系的
`libsasl2-dev` 或 Fedora 系的 `cyrus-sasl-devel`。该边界让 GSS/Kerberos 符号由
目标系统的认证栈统一解析，避免静态 SASL 插件与系统认证框架失配。

## 快速开始

下面示例使用属性宏收集消费者。`client` 必须与 `KafkaConfig.client_name` 一致；
`start()` 只表示本地 owner 已构造，接流量前再用 `await_group_ready()` 等待真实 broker assignment。

```rust
use std::time::{Duration, Instant};

use nasa::kafka::{
    kafka_consumer, AdminConfig, BehaviorConfig, ConsumerConfig, KafkaConfig, KafkaProxy,
    ProducerConfig, ReadyRequirement, Result, SecurityConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct OrderCreated {
    id: String,
}

#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-worker",
    client = "order-service"
)]
/// 业务作用：展示订单消费入口并输出订单标识。参数说明：message 为解码订单。返回：成功允许 Auto 确认，此例未持久化业务数据。
async fn consume_order(message: OrderCreated) -> Result<()> {
    println!("order={}", message.id);
    Ok(())
}

#[tokio::main]
/// 业务作用：建立订单事件源，等待消费就绪后发布一条消息并关闭资源。参数说明：无。返回：各步骤成功后退出，否则向调用方返回错误。
async fn main() -> Result<()> {
    let kafka = KafkaProxy::connect(KafkaConfig {
        client_name: "order-service".into(),
        bootstrap_servers: "127.0.0.1:9092".into(),
        group_id: Some("order-service".into()),
        client_id_prefix: Some("nasa".into()),
        producer: ProducerConfig::default(),
        consumer: ConsumerConfig::default(),
        admin: AdminConfig::default(),
        behavior: BehaviorConfig::default(),
        security: SecurityConfig::default(),
        properties: Default::default(),
    })?;

    let _consumers = kafka
        .consumers()
        .with_collected()?
        .start()
        .await?;

    kafka
        .await_group_ready(
            "order-worker",
            ReadyRequirement::AssignedTopics(vec!["orders".into()]),
            Instant::now() + Duration::from_secs(30),
        )
        .await?;

    let order = OrderCreated { id: "O-1001".into() };
    kafka
        .publish("orders", &order)
        .event("created")
        .key(&order.id)
        .send()
        .await?;

    kafka.shutdown().await
}
```

配置字段使用 `snake_case`，可以直接嵌入应用自己的配置结构：

```yaml
kafka:
  client_name: order-service
  bootstrap_servers: 127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096
  group_id: order-service
  client_id_prefix: nasa
  producer:
    acks: all
    retries: 3
    enable_idempotence: true
    max_in_flight_requests_per_connection: 1
  behavior:
    max_consume_attempts: 3
    dead_letter_topic_suffix: .DLT
    dead_letter_required: true
```

框架固定关闭 `enable.auto.commit` 和 `enable.auto.offset.store`。这两个值属于正确性不变量，
不能通过 raw properties 覆盖。

producer 默认使用 `acks=all`、`enable_idempotence=true` 和单连接一个在途请求；broker 的
ISR、最小副本配置与业务幂等仍决定完整可靠性边界。消费上下文包含逻辑 `source_id`、实际
`group`、`assignment_epoch`、总投递次数和普通失败预算次数；逻辑源名不等于物理集群 ID。

动态订阅明确区分 `replace_subscriptions`、`add_subscriptions`、`remove_subscriptions` 和
`clear_subscriptions`。集合修改在 owner 内计算，清空订阅保留 owner；`stop_group` 等待实际退出
后按实例身份注销。原有 `subscribe_group` 保持完整替换语义。`seek_offsets` 接受分区位点 map，
多个 native 定位不构成原子事务，操作失败后应查询实际位置。

## Application 受管模式

Service 同时开启 `application` 与 `kafka` 后，可以把 Kafka 声明成正式容器组件。此时业务不再手写
`connect`、registry start、broker Ready 或 shutdown：

```toml
nasa = { version = "2.0.3", features = ["application", "kafka", "web"] }
```

```rust
#[nasa::application("kafka", "web")]
/// 业务作用：在启动窗口追加订单消费者。参数说明：app 为受管应用。返回：登记成功；实际 Ready 与关闭由容器负责。
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.configure_kafka("order-service", |registry| {
        registry.register(OrderConsumer::new())
    })?;
    Ok(())
}
```

受管配置在数据面字段之外增加严格 `container` 段；单 client 使用 `kafka`，多 client 使用互斥的
`kafkas.<name>`。完整字段、ReadyRule 和顺序见
[napp README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md)。属性 consumer 的
`client` 必须命中一个 `consumers: collected` client；容器在 UserHook 后一次性冻结 registry，并在对外
Ready 前等待真实 join/assignment 或 producer metadata。

业务通过 `app.kafka(name)` 取得 `KafkaHandle`。它可返回 `ProducerLane`、健康/assignment/position，
并开放 pause/resume、seek、subscribe/unsubscribe、restart 和只读 metadata，但不会暴露原始
`KafkaProxy`、`consumers()` 或关闭权。`handle.admin()` 始终只读；只有宿主按 client 显式设置 `container.admin_write: true` 后，`handle.administration()` 才授予独立管理写视图，仍需 broker ACL。独立批处理与运维工具继续使用本 README 其他章节的
显式 `KafkaProxy` 模式，不声明 Application 的 `"kafka"` 组件。

业务自有收尾可在 UserHook 使用 `app.register_graceful_shutdown`：受管 consumer 已在 Ready action
收口，业务任务在最终 producer flush 和关闭之前执行，可以继续使用已取得的受管发布句柄。
任务结果不会替代 broker 的真实确认；期限内没有取得确认仍属结果未知。不要再次登记 KafkaProxy 的
shutdown，否则会与组件的两段关闭 owner 重叠。

### 两段停机 API

容器使用接收绝对 deadline 的入口，保证所有 action 共享同一预算：

```rust
proxy.stop_consumers_until(deadline).await?; // 停 owner，producer 仍可用于退出收尾
proxy.shutdown_until(deadline).await?;       // 关闭准入、flush lane、关闭 admin
```

`stop_consumers_until` 与 consumer start、全量 shutdown 共用生命周期门禁；只有全部 owner 确认退出后才清
group 表、指标和静态收集租约。超时会保留这些所有权，后续 `shutdown_until` 可以继续收尾。成功 drain 后
再执行 final shutdown 会把空 group 表视为正常路径，继续关闭 producer、Admin 与观测线程；两者及其并发重复调用均幂等。
这里的消费停止还覆盖事务、Replay、Transfer 和 DLT 回投；实际任务未退出时保留其身份及所持 lease。
`close_report()` 返回无需建连的结构化责任快照；`shutdown_report_until(deadline)` 按同一关闭协议推进，返回 `Completed` 或 `Incomplete`。取消 future 不产生虚假的完成报告，后续仍可继续等待。需要原始关闭错误分类时使用 `shutdown_until`，随后读取报告；`napp::KafkaHandle.close_report()` 只读开放停机诊断，不授予活跃宿主整源关闭权。

报告分别呈现实际 owner/handler、保留输入、停止期间未确认释放、lane 未决和累计终态未知交付、DLT 真实任务及信封、回放/转发/事务资源、Admin 原生借用和观测线程。组名单最多64项，完整未退出组数单独保留；消费组表清理前只封存固定数量的纯计数，不保留原文或原生客户端。`resources_closed` 只在全源 Stopped 时成立，其它字段为独立采样。有限任务的业务结局和 checkpoint 继续从对应任务句柄查询。

`released_unverified_records` 统计停止期间未获提交确认的释放观察次数，不是跨 epoch 去重后的业务事件数，不能与其它责任计数相加当作事件总量。本地释放和 offset 保留范围都不能证明具体记录仍存活或 topic 没有被同名重建。普通关闭不执行这种恢复证明，`returned_for_redelivery` 保持零；实际来源恢复应独立核实。lane 的 `terminal_delivery_unknown` 在资源关闭与重复等待后保留；仅调用方超时、后来原生确认成功不计入该终态累计量。

独立模式的 `shutdown()` 保持兼容，它用 `behavior.shutdown_timeout_ms` 计算一次绝对 deadline 后委托
`shutdown_until`。

最终 lane 关闭会先封闭全部准入，再等待实际回执和所有原生 producer 代次析构。长期持有的
`ProducerLane`、发布 builder 或已完成任务句柄只保留统计与拒绝状态，不阻止 producer 线程和连接释放。
同一 lane 的阻塞 flush 同时最多执行一次；取消或超时仅结束调用方等待，实际工作仍受关闭责任跟踪。
原生释放未完成时仍报告 lane 关闭超时，后续 `shutdown_until` 可继续收口。

普通 DLT 的并发额度覆盖实际原生交付，发布等待超时不会提前释放它。rebalance 撤销旧任务的来源提交权，但已经发送的请求仍属于源；消费停止和最终关闭都等待这些旧分配任务、信封和完成通知退出。`DltShutdownTimeout` 报告仍活动的任务数、保留记录及字节；重复关闭继续等待，不开启新的 DLT 重试。

## 消费者使用场景

消费者按 `(group, topic, event)` 路由。producer 未显式设置 `event` 时使用 `DEFAULT`；consumer 的
`event()` 或宏中的 `event` 必须与消息 header 一致。同一 group 内同一个 `(topic, event)` 只能注册一个
handler，注册表在 `start()` 前完成冲突检查并冻结。

普通实际消费组的共同输入域为：**1–1024 个 UTF-8 字节、不含 NUL、不是纯空白，首尾不含 U+0000–0020**。空白集合为
U+0009–000D、U+001C–0020、U+1680、U+2000–2006、U+2008–200A、U+2028–2029、U+205F、U+3000；
NEL、NBSP、U+2007、U+202F 是有效组名字符。ASCII 首尾控制字符和空格会在原生字符串配置中被裁剪，因此直接拒绝；名称内部空格及有效非 ASCII 边缘字符保留。合法身份不裁剪、替换或做 Unicode
归一化。`?`、中文、emoji 以及组合形式不同的文本保持各自身份；长度按 UTF-8 字节计算，允许
超过255字节的组在共同上限内原名接续。`String`/`&str` 只表达有效 UTF-8。

默认 `group_id` 的 None/空串表示未指定，允许纯发布源；`GroupSpec::Default` 在消费启动前仍须
解析出有效组。`GroupSpec::Named`、宏中的显式 `group`、按组并发覆盖键、assign、Ready、管理查询
和迁移候选共用上述边界。非法身份以 Config 拒绝，宏字面量在编译期拒绝；配置验证和路由准入发生在
owner 或原生请求之前。名字只读取并冻结，不依赖后续业务对象变化。
业务和上层容器可用 `KafkaConfig::validate_group_id` 在装配前校验实际组名；`napp` 的
`container.readiness.groups` 键也复用此合同，按精确身份匹配就绪规则。

1024字节上限保证原组身份能完整进入现有 DLT 来源证据和恢复合同。超过上限、含 NUL 或带 U+0000–0020 首尾字符的既有组不在
受管消费及管理查询的支持域，不能直接接续；须继续使用能处理原身份的客户端，或显式规划迁移并
核对位点。框架不会截短、改名或复制 committed offset 来自动迁移。

| 场景 | 推荐入口 | 消息所有权 | 确认方式 |
|---|---|---|---|
| 无状态单条业务消费 | `#[kafka_consumer] async fn(T)` | 拥有型 payload | Auto |
| 需要 key/header/offset | `#[kafka_consumer] async fn(KafkaRecord<T>)` | 拥有型 record | Auto/Manual |
| 无状态批消费 | `async fn(Vec<T>)` / `Vec<KafkaRecord<T>>` | 拥有型连续 run | Auto/Manual |
| 有依赖注入 | `SingleConsumer` / `BatchConsumer` | 拥有型 record | Auto/Manual |
| 每个实例都要收到 | `broadcast` / `GroupSpec::Broadcast` | 拥有型 record | Auto/Manual |
| 固定分区回放或运维任务 | `KafkaProxy::assign` | 拥有型单条 record | Auto/Manual |
| Kafka → socket 热路径 | `PassthroughConsumer` | 同步借用 record | disposition/Manual |

### 1. 属性宏：单条 payload

最简场景只关心解码后的业务对象。默认使用 `AckMode::Auto`：函数只有返回 `Ok(())` 才确认。

```rust
use nasa::kafka::prelude::*;

#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-worker",
    client = "order-service"
)]
/// 业务作用：保存订单后允许来源确认。参数说明：message 为解码订单。返回：保存成功后返回成功，失败交由消费策略处理。
async fn consume_order(message: OrderCreated) -> Result<()> {
    save_order(&message).await?;
    Ok(())
}
```

`save_order()` 返回错误时直接 `?` 即可；handler 的 `Err` 不会产生 ack，也不会推进 offset。
一个 handler 可以声明多个 topic，例如 `topics = ["orders", "orders-priority"]`；两个 topic 使用相同
message codec、event 和处理函数，实际来源通过 `KafkaRecord.ctx.topic` 区分。

### 2. 属性宏：单条完整 record

需要 topic、partition、offset、timestamp、key、重复 header 或 passthrough 上下文时使用
`KafkaRecord<T>`：

```rust
#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "audit",
    client = "order-service"
)]
/// 业务作用：连同来源坐标保存订单审计。参数说明：record 为订单及消费上下文。返回：审计成功才允许 Auto 确认。
async fn audit_order(record: KafkaRecord<OrderCreated>) -> Result<()> {
    let traceparent = record
        .ctx
        .headers
        .last("traceparent")
        .and_then(|header| header.value.as_deref());

    audit(
        &record.value,
        &record.ctx.topic,
        record.ctx.partition,
        record.ctx.offset,
        record.ctx.key.as_deref(),
        traceparent,
    )
    .await?;
    Ok(())
}
```

`KafkaHeaders` 不是 Map：`iter()` 保留原始顺序，`last(name)` 取最后一个同名值，`all(name)` 遍历
全部重复项；header value 的 `None`、`Some(&[])` 和非空字节是三种不同事实。
`record.topic_partition()` 返回自有 `Tp`；只想拆出 payload 和上下文、不保留确认能力时使用
`let (value, ctx) = record.into_parts()`。

### 3. 属性宏：批消费

批参数必须直接写成下面两种规范形态：

```rust
#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-index",
    client = "order-service"
)]
/// 业务作用：批量写入订单索引。参数说明：messages 为连续输入的解码值。返回：整批写入成功才允许 Auto 确认。
async fn payload_batch(messages: Vec<OrderCreated>) -> Result<()> {
    write_index_batch(&messages).await?;
    Ok(())
}

#[kafka_consumer(
    topics = ["orders"],
    event = "created",
    group = "order-audit",
    client = "order-service"
)]
/// 业务作用：连同来源坐标批量保存订单审计。参数说明：records 为连续输入记录。返回：整批审计成功才允许 Auto 确认。
async fn record_batch(records: Vec<KafkaRecord<OrderCreated>>) -> Result<()> {
    for record in &records {
        println!("partition={} offset={}", record.ctx.partition, record.ctx.offset);
    }
    write_audit_batch(&records).await?;
    Ok(())
}
```

一次回调拿到的是同一 route、同一 partition 上按 offset 排列的连续 run，大小受
`max_batch_records` 和 `max_batch_bytes` 限制。不跨 poll 等待凑满数量或时间窗口；当前 poll 有多少安全连续记录
就派发多少。Auto 批回调返回 `Err`、panic 或 timeout 时，整次 run 都不确认。

### 按键并发与责任边界

默认 `ExecutionMode::PartitionOrdered` 保持分区内串行。单条 trait 消费者可覆盖
`execution_mode()` 返回 `ExecutionMode::KeyOrdered`；属性宏使用
`execution = "key_ordered"`，默认值为 `"partition_ordered"`。同组全部 typed 路由必须采用同一模式；
按键模式在宏和显式注册入口都拒绝批处理器。

顺序域由 `(topic, partition, 原始 key 字节)` 决定，null key 是分区屏障，空 key 是合法独立 key。
`ConsumeCtx.raw_key` 无损保留二进制 key；`ctx.key` 只提供合法 UTF-8 的便利视图，非 UTF-8 时为 None。
类型化与 raw 发布 builder 均支持 `key_bytes`，DLT 同样保留来源原始 key。同一 builder 混用
`key` 与 `key_bytes` 会在发送前返回 Config，不能静默更换分区身份。

框架路由只读取有界消息头和冻结元数据；自定义 typed 解码、单条和批量 handler 在独立 worker 上执行。
分区模式同分区至多一个实际调用，不同分区可并行；按键模式允许不同 key 并行，两者共同受组级
`max_in_flight_handlers` 约束。每个 worker 内构造业务 future，因此 trait 不额外要求
future 为 Send；owner 是原生 consumer 的唯一访问线程，并在业务阻塞期间继续维护 poll。
自定义解码与 handler 在同一次 worker 调用内执行；codec 阻塞时查询和暂停仍可响应，停止、超时或失权后不再开启本批后续解码，
也不将迟到解码结果交给 handler。正在执行的 codec 必须实际返回才能收回责任，关闭超时会保留该责任。
同 key 后继必须等待前驱实际退出且得到安全结果；其它 key 可复用退出的 worker 名额。
`handler_timeout_ms` 是失败预算：owner 在预算到期时撤销 Manual Ack 权限，同步阻塞的调用必须实际返回
才能归还名额，迟到成功仍按超时失败。rebalance 同样立即作废旧 AckToken，不等待业务线程主动返回。
typed 超时预算独立于 `max_poll_interval_ms`，从具体执行取得 worker 准入时开始，解码、业务与等待业务 future 共用一个截止时刻。框架路由准备和等待按键顺序的尚未派发输入不提前消耗调用预算；慢 codec 与业务都不占用 owner 的 poll 等待。批量调用的全部解码共享同一预算，任一解码失败时不执行业务，失败 offset 按原顺序进入消息策略，正常前后缀保留再次准入资格。
异步 future 被取消不代表它自行派生的外部任务或远端副作用已回滚，业务仍需幂等处理。

完成顺序不决定提交顺序。owner 按真实 offset 顺序应用结果，前缀失败时保留后续 key 的完成证据，
本地重投不会重新调用这些 handler；只有连续安全前缀才允许提交。该证据不持久化，rebalance、人工定位
和会话重建会使其失效，跨这些边界仍是 at-least-once。跨分区、跨进程及扩分区后的同 key 不提供全局顺序。

| 配置 | 默认值 | 责任范围 |
|---|---:|---|
| `behavior.max_in_flight_handlers` | 64 | 每组尚未实际退出的 typed 准备与业务调用 |
| `behavior.max_group_progress_records` | 65536 | 全组输入责任记录上限；每槽进度和按键完成证据另受同值约束 |
| `behavior.max_group_pending_bytes` | 67108864 | 全组输入原文副本预留字节上限；每槽完成证据另按原消息字节加 2048 字节诊断余量保守计费 |
| `behavior.max_process_pending_records` | 1048576 | 本组件进程内全部源的普通消费输入记录总量 |
| `behavior.max_process_pending_bytes` | 1073741824 | 本组件进程内全部源的普通消费原文副本预留字节总量 |
| `behavior.max_source_pending_records` | 262144 | 同一源全部普通消费组的输入记录总量 |
| `behavior.max_source_pending_bytes` | 268435456 | 同一源全部普通消费组的原文副本预留字节总量 |
| `behavior.max_partition_pending_bytes` | 67108864 | 同一组同一分区的输入原文副本预留字节总量 |
| `behavior.max_key_pending_records` | 1024 | 同一组同一分区同一 key 的输入责任记录数 |
| `behavior.max_key_pending_bytes` | 16777216 | 同一组同一分区同一 key 的输入原文副本预留字节总量 |
| `behavior.max_batch_records` / `max_batch_bytes` | 500 / 16777216 | 当前已物化输入批次，独立于完成证据预算 |

字节预算必须至少为 `2 * (max_record_bytes + 2048)`；乱序结果始终给最早空洞保留一个最大记录额度，
避免成功后继占尽重试空间。原生队列、解码对象和线程栈不属于 payload 字节预算。每槽独占一个 owner，
owner 跨 poll 保留有界输入和实际调用，按分区轮转派发。一个分区的慢调用不阻塞同槽其它分区的后续拉取、调用和提交；按键模式也允许后来取入的不同 key 使用空闲额度。同 key 与 null key 屏障继续生效。可用并发取决于共享执行与输入额度，不承诺实时调度期限或特定吞吐。DLT 信封池满时只暂停等待死信的分区，健康分区继续处理。

在途调用期间，查询和用户 pause/resume 仍通过 owner 邮箱执行。seek 的目标仍有输入、实际调用或退避责任时返回确定未执行；
其它空闲分区可独立定位。restart 和订阅替换不能覆盖未退出责任。停止命令关闭后续派发并等待实际调用退出。
rebalance 立即撤销旧 Ready，新分配可独立建立新的 Ready；旧调用继续持有实际执行额度，结果不能用于新的 assignment。
按键重试保留原文，只关闭失败 key 的派发；其它 key 可以在输入和执行额度内继续处理。用户 resume 不缩短退避期限，
完成结果仍须等待连续安全前缀才能提交。
健康快照提供全部 typed 解码与业务调用的 `active_handlers`，批调用计为一个；按键模式另外提供
`key_active_handlers`、`key_pending_records`、`key_pending_bytes`。`retained_records` 和 `retained_bytes` 则统计输入批、实际调用、重试和待确认提交仍持有的原始输入责任，记录数量只计一次，字节为原始 key、payload 和全部有序头字节和的八倍预留。相同名称的指标为周期采样，多槽先汇总后发布，业务阻塞期间继续采样。

### 组内多消费者

`consumer.concurrency` 为源内每个订阅组的 owner 槽数，默认 1，范围 1–64；
`consumer.group_concurrency` 按解析完成的实际 group.id 精确覆盖，包括广播模式最终组名。
每槽独占 native consumer，共享组内冻结路由，由 Kafka 分配分区。
`assign` / `assign_batch` 始终单槽，避免重复读取显式范围。

```toml
[kafka.consumer]
concurrency = 3
[kafka.consumer.group_concurrency]
order-service = 2
[kafka.behavior]
max_record_bytes = 1048576
max_group_pending_bytes = 67108864
max_in_flight_handlers = 12
```

槽数创建后不再变化；全部槽本地构造成功才开放同一启动门。多槽为 consumer 的
`client.id` 和显式 `group.instance.id` 增加 `-slot-N` 身份片段；
静态成员原始身份仍须在不同进程之间唯一。连接和 producer lane 资源继续归属原源。

组内全部 owner 共用 `max_in_flight_handlers` 执行额度和持久线程池，空闲名额可被任一槽使用；
owner 数量可以超过同时执行的 handler 数。业务顺序凭证也由全组共享：本地新 owner 接管分区后，等待旧同 key 或分区独占调用实际退出才派发；旧 Ack 在失权时立即无效。该凭证不跨进程，对外部副作用仍需业务幂等或目标系统 fencing。

输入记录数、原文副本预留字节和同一分区记录数由全组共享凭证在物化前裁决，不为闲置槽预留份额。
输入批、实际调用、重试和待确认提交保留同一凭证；broker 确认或责任实际交回后才归还，超时和失权本身不会提前腾出容量。
输入不足只暂停触发分区，已计费重试复用原额度；`InputBackpressure` 与进度结果门禁分别显示。
每槽逻辑批仍受 `max_batch_records` / `max_batch_bytes` 约束，提交触发值不随槽数缩小。
普通 typed 消费按数量或时间阈值提交；本地输入已全部安全完成且实际调用已退出时，尾批提前提交，不等待凑满阈值。该行为只缩短已完成责任的保留时间，不越过失败、未确认或仍在执行的输入；提交结果未知时仍进入保护与重试流程。业务完成通知可以立即唤醒调度，原生 poll 和协调维护保持有界周期。
`consumer.max_assignment_partitions` 默认 10000，约束全部槽当前分配的分区并集；本地接管同一分区只计一次。固定范围在原生 assign 前校验，订阅分配在发布 Ready 与派发前校验，超限的 owner 进入 `Crashed/AssignmentTooLarge`。`behavior.max_retry_entries` 默认 100000，按保留普通失败、延期或缺失确认次数的输入记录计费，所有槽共享；批失败原子接纳全部新增记录，容量不足保护失败分区且不提交失败输入，健康分区仍可继续。次数在成功、DLT 确认、失权或明确定位后清除，不是持久重试次数。两项均须为 1–2147483647；空闲槽不预留份额。
DLT pipeline 共享源级记录数和字节额度，不复制为每槽或每组队列；失败批按来源顺序逐条转移，因此业务批可以大于 DLT 池的记录容量。原输入凭证保留有界的失败原因与可信交付次数；等待额度期间只保留转移意图，重新取入时绕过已执行的业务。额度恢复后必须取回原坐标，不能用后继记录替代已经丢失的原记录；发现坐标不符时停止该分区且不越过原记录提交。意图仅在当前消费会话内有效，失权或进程退出后允许 broker 重投。

复制 DLT 信封原文前先保留记录数和字节额度。字节额度采用原输入、topic/group 的 UTF-8 字节数及 4096 字节来源证据上界之和，物化后再次检查实际大小；单条超过空池上限时保留未确认来源并停止该分区。这个保守上界使等待、交付及重试期间的信封原文始终受源级总量约束。
输入字节额度采用共同保守口径：原始 payload、key、完整有序头的字节和为 `R`，每条预留 `8 × R`，记录数量仍只计一次。预留覆盖原输入、拥有型上下文、头映射与完整键顺序域的必要副本，物化前一次取得并保持到最后真实责任退出。原始记录和只读头在内部共享引用；解码类型及业务主动保留的副本由业务约束。`max_record_bytes` 和 `max_batch_bytes` 继续按原始 `R` 计量；例如输入预算 8 MiB 最多接纳合计 1 MiB 原文。单条完整预留超过任一空池上限时停止该分区，不能等原输入占满后再等待副本额度。固定框架元数据受记录数上限约束。

普通 `subscribe` / `assign` 的 typed 与借用输入还同时受源和进程共享数量、原文副本预留字节额度限制。
首次有效源构造冻结本组件进程上限，后续源声明不同值会在创建线程或原生资源前返回配置错误；
改变进程边界需要重启。各层同时裁决，重试复用原凭证；取消或失权后的实际输入批仍持有额度，
最后责任退出才归还。单条原文超过任一层级空池字节边界时进入 `Halt(OversizeRecord)`，不无限等待容量。
分区和分区内 key 同时具有数量及原文副本预留字节边界，null 与空 key 独立计费，不同分区同名 key 相互隔离。
准入使用固定长度摘要保存等待身份，碰撞仅保守合并额度，不替代执行层完整原始 key 的顺序裁决。
key 满额时保留原记录重读坐标，已准入的其它 key 可继续完成；尚未取入的同分区后续输入等待边界记录获得容量。
这些限额不等于进程总内存上限，不包含 native 队列、解码对象的额外堆内存或线程开销。
事务、有限回放和转发使用各自计划的独立额度，producer lane 与 DLT 信封也独立计费。
控制邮箱的 `command_queue_capacity` 按每槽计费，总邮箱容量最多为该值乘以槽数。

`group_slots_health(group)` 按稳定槽号返回独立快照；`group_health(group)` 汇总分区和活动量，
poll 时间取最慢槽、保护态优先。所有槽 Ready 且无重复分区归属才发布组 Ready，
已完成 join 的空闲槽可以没有分区。组 epoch 随完整槽 epoch 向量变化，不能与单槽 epoch 比较或作为外部租约。
组 gauge 汇总各槽最近一次采样；counter 保留同组累计语义。

分区控制先解析完整目标集合，再分发给实际 owner，并在 native 副作用前复验 assignment epoch 与归属。
订阅操作向全部槽发送；restart 只选择允许恢复的槽。多槽控制不具有原子性，既有整体返回接口在部分失败时返回
`ControlOutcomeUnknown`，应查询各槽后收敛；需要逐目标证据时使用 `control_partitions`。单槽保留原控制错误类型。
`position` 保留 librdkafka 的可空观测，暂停后 seek 尚未重新取入时可能为 None。
关闭使用共同 deadline 等待全部槽，等待超时不释放真实业务责任。
停止撤销在途 Manual Ack 权限，只有业务调用实际退出才完成停止；超时停止可以继续等待收口。
Auto 路线同样不能用停止后的迟到成功推进提交。停止前临时 `ack()` 但尚未纳入 owner 安全水位的调用
也需要重投；实际退出前保留执行责任，外部副作用仍需业务幂等。已经形成的安全前缀和已经发出的、最终
获得 broker 确认的 DLT 可以收口提交，停止后不再开启新的业务或 DLT 重试。

typed 单条、批量和按键处理共用组内持久业务线程池。池按实际并发惰性增长，不超过全组
`max_in_flight_handlers` 额度；后续调用复用已空闲的线程，业务 future 始终在同一线程构造和执行，
保持非 `Send` future 兼容。逻辑超时只撤销确认，不归还实际执行名额；停止和会话异常均等待闭包及
其捕获资源退出；一个 owner 的异常收尾只等待本槽责任，全部槽退出后关闭共享线程。等待组执行额度时仍维持 poll 和控制，不将暂时饱和当作会话错误。该额度不包括 librdkafka 和异步运行时的后台线程。
批调用的全部确认槽共享当前窗口权威，失权后的整批结果不能进入新的 assignment。同步借用式
`PassthroughConsumer` 保持 owner 栈内执行，业务回调必须短小且不能阻塞。

### 路由头的歧义处理

`X-Nasa-Event`、`X-Nasa-Payload-Codec`、`X-Nasa-Payload-Mode` 按精确大小写识别。
`behavior.strict_headers` 默认 true，typed 与借用消费均在匹配路由前拒绝重复、null 和非法 UTF-8；
false 仅允许重复声明取最后值，选中值仍须合法。对应 Java `behavior.strict-headers`。
只有缺失 event 才使用 DEFAULT，空串和纯空白事件按 `invalid_record_policy` 处置。发布、注册和消费共用事件空白集合：U+0009–000D、U+001C–0020、U+1680、U+2000–2006、U+2008–200A、U+2028–2029、U+205F、U+3000；NEL、NBSP、U+2007、U+202F 不在此集合。非空白事件保留首尾空格及 Unicode 原文，不裁剪、不归一化。typed、raw、tombstone 与批发布的非法事件在 native 准入前以 `NafkaError::Config` 拒绝；Java 对应 `CONFIG`，同时拒绝不能精确编码为 UTF-8 的孤立 UTF-16 代理项。批发布逐条准入，后续条目失败不撤销此前已发送的条目。

codec 只接受 `protocol-bytes`，事件未匹配也不能绕过未知 codec 校验。JSON 路由要求 codec 和 mode
均缺失；ProtocolBytes 要求 codec，mode 缺失时沿用静态类型声明，存在时须匹配。借用路线只验证声明
的基本格式，payload 与 mode 的业务含义由 adapter 处理。业务重复头与 null 值仍无损保留，
trace 和透传上下文的容错规则不受这三个路由头的严格模式影响。

### 4. 属性宏约束与显式激活

宏支持的四种签名为 `T`、`KafkaRecord<T>`、`Vec<T>` 和 `Vec<KafkaRecord<T>>`：

```rust
use nasa::kafka::prelude::*;

// 假设 OrderCreated、PaymentCreated 已实现 serde 解码，persist 是业务持久化函数。

#[kafka_consumer(topics = ["orders"], event = "created")]
/// 业务作用：展示消费参数或组声明。参数说明：message 为解码输入。返回：示例直接成功并允许 Auto 确认，实际业务须在成功前完成处理。
async fn payload_single(message: OrderCreated) -> Result<()> {
    Ok(())
}

#[kafka_consumer(topics = ["orders"], event = "created")]
/// 业务作用：展示消费参数或组声明。参数说明：record 为含来源上下文的输入。返回：示例直接成功并允许 Auto 确认，实际业务须在成功前完成处理。
async fn record_single(record: KafkaRecord<OrderCreated>) -> Result<()> {
    Ok(())
}

#[kafka_consumer(topics = ["orders"], event = "created", group = "indexer")]
/// 业务作用：展示批量 payload 签名。参数说明：messages 为连续输入的解码值。返回：示例直接允许整批 Auto 确认，实际业务须先完成处理。
async fn payload_batch(messages: Vec<OrderCreated>) -> Result<()> {
    Ok(())
}

#[kafka_consumer(topics = ["payments"], event = "created", ack = "manual")]
/// 业务作用：逐条持久化付款并记录本次临时确认。参数说明：records 为含确认权威的批记录。返回：整次调用成功才保留连续确认前缀，错误使本次确认失效。
async fn record_batch(records: Vec<KafkaRecord<PaymentCreated>>) -> Result<()> {
    for record in &records {
        persist(&record.value).await?;
        record.ack()?;
    }
    Ok(())
}
```

约束如下：

- 只能标注 async free function，不接受 receiver 或泛型函数。
- `topics` 至少一个；`topics`、`client` 不接受首尾空白。`group` 使用普通组的共同输入边界；`event` 使用精确事件合同并允许非纯空白原文中的首尾空格。
- `group` 与 `broadcast` 互斥。
- `ack` 只允许 `auto` 或 `manual`；Manual 必须使用 `KafkaRecord<T>` 形态。
- 批量参数直接写 `Vec<T>` 或 `Vec<KafkaRecord<T>>`，不要用类型别名表达批形态。
- 宏只负责静态发现；必须显式调用 `with_collected()` 或 `with_collected_for()` 才会激活。

`client` 用于在同一进程存在多个 `KafkaProxy` 时选择静态消费者集合：

```rust
let primary_runtime = primary
    .consumers()
    .with_collected_for("order-service")?
    .start()
    .await?;

let audit_runtime = audit_cluster
    .consumers()
    .with_collected_for("audit-service")?
    .start()
    .await?;
```

`with_collected()` 等价于使用当前 `KafkaConfig.client_name`。宏发现不会隐式连接 broker、启动线程或
开始消费；真正启动点始终是 `start().await`。同一个 collected client 在同一进程内只能被一个活动 runtime
持有；重复激活会 fail-fast，前一个 runtime 完成 shutdown 并释放 activation lease 后才能再次启动。

### 5. group：默认、命名与广播

三种 group 语义：

```rust
// 未写 group/broadcast：使用 KafkaConfig.group_id。
#[kafka_consumer(topics = ["orders"], event = "created")]
/// 业务作用：展示消费参数或组声明。参数说明：message 为解码输入。返回：示例直接成功并允许 Auto 确认，实际业务须在成功前完成处理。
async fn default_group(message: OrderCreated) -> Result<()> {
    Ok(())
}

// 所有实例竞争同一个稳定 group，消息由 Kafka 分摊。
#[kafka_consumer(topics = ["orders"], event = "created", group = "order-worker")]
/// 业务作用：展示消费参数或组声明。参数说明：message 为解码输入。返回：示例直接成功并允许 Auto 确认，实际业务须在成功前完成处理。
async fn named_group(message: OrderCreated) -> Result<()> {
    Ok(())
}

// 每个进程实例派生独占 group，因此每个实例都会收到完整消息流。
#[kafka_consumer(topics = ["sys-config"], event = "reload", broadcast = "config-cache")]
/// 业务作用：为本广播实例应用配置事件。参数说明：message 为配置变更。返回：应用成功才允许 Auto 确认。
async fn broadcast_group(message: ConfigChanged) -> Result<()> {
    reload_config(message).await?;
    Ok(())
}
```

裸 `broadcast` 会以 module path + function name 作为 scope；多个 handler 需要共享稳定广播语义时使用
`broadcast = "scope"`。`group` 与 `broadcast` 不能同时出现。

需要与 Java 使用相同广播身份时，显式采用 `GroupSpec::StableBroadcast { scope, instance_id }`，
宏使用 `broadcast_scope = "config-cache", instance_id = "node-a"`。`instance_id` 可省略，沿用
`behavior.broadcast_instance_id`、`NASA_KAFKA_BROADCAST_INSTANCE_ID` 或本进程临时身份。
`broadcast_scope` 与普通 `group`、旧 `broadcast` 互斥；没有作用域时不允许声明 `instance_id`。

`broadcast_group(scope, instance_id)` 返回可用于 Ready 和控制的实际组名，与 Java
`KafkaProxy.broadcastGroup(scope, instanceId)` 相同。每个分量要求 1–4096 个 UTF-8 字节，不含
Unicode 控制字符及首尾 ASCII 空格，不执行净化、裁剪或 Unicode 归一化。短组名为
`nasa-kafka-broadcast-b<scope小写hex>.<instance小写hex>`；超过 255 ASCII 字节时使用
`nasa-kafka-broadcast-h<sha256小写hex>`。摘要输入依次为 `nasa.kafka.broadcast` 的 UTF-8、一个
零字节、scope 的四字节大端长度及原文、instance 的四字节大端长度及原文。不同标点不因净化而
合并；长身份的区分依赖 SHA-256 的碰撞抗性。

稳定实例 ID 接续同一组已提交位点；临时 ID 重启后改变，新组起点服从 `auto_offset_reset`。
两个活动进程使用同一稳定 ID 会竞争同一份输入，不能各收一份。发布启动通知前应按实际组名等待
`await_group_ready`；离线期间的缓存一致性仍依靠重建或业务快照。

既有 `GroupSpec::Broadcast` 和宏 `broadcast` 保留旧组算法。迁移可以先使用 `GroupSpec::Named`
固定旧组，再停止旧消费者并明确选择迁移提交位置或按新组起点重读；框架不自动更名或复制位点。

### 配置迁移与滚动部署

`inspect_legacy_config(source_id, legacy, routes)` 接受单个结构化 KafkaConfig JSON 与完整 `MigrationRoute { topic, event, group }`，只读生成 `kafkas.<source_id>` 候选、默认差异和人工决策项。它不需要 Tokio、不建连、不改 group 位点或 topic；路由目录由调用方提供，不能从配置猜测业务路由。未知配置字段及无效身份拒绝。

候选保留单源格式的 `acks=1`、幂等关闭、producer 队列 100000 条/1048576 KiB 与 DLT 队列 268435456 字节；当前默认分别为 `all`、开启、10000 条/65536 KiB、67108864 字节。显式输入优先，检查器不自动增强可靠性或缩减原配额。`strict_headers=false` 仅允许保留头取末值，非法 UTF-8、null 或空事件仍受当前合同约束。所有默认差异与本地约束应在部署前核对；构造诊断同时显示实际配置合同。

凭据、证书路径和原生 properties 值用 `<redacted>` 遮蔽，部署环境必须重新绑定。`locally_valid` 只说明脱敏前候选通过本地解析和约束，不能证明 ACL、业务等价或滚动兼容。命名源通过 `app.kafka(name)` 选择；源名改变须同步修改 consumer 的 client 声明。受管 Admin 始终默认只读，写入须按源授权 `container.admin_write` 并调用 `administration()`。

滚动部署前先冻结相同可靠配置；所有组成员具备新增 event 路由后才允许发布新事件。typed tombstone、严格保留头和未知路由策略必须显式核对。`max_consume_attempts=0` 表示无限重试；广播切换算法不会继承另一个 group 的位点。

公共 enum 增加状态和错误类别时，穷尽 `match` 必须更新，包括 `GroupSpec`、`GroupState`、`InvalidRecordPolicy`、`NafkaError`、`PauseReason`、`ReadyRequirement` 和 `SchemaRegistryError`。`BehaviorConfig`、`ConsumerConfig`、`ConsumeCtx`、`GroupHealth`、`ProducerLaneOverride`、`ProducerLaneStats`、`ConfluentRegistryOptions` 的完整 struct literal 需补齐字段；有 Default 的配置可采用结构更新语法，再显式设置业务选项。源码签名保留不意味着所有调用方都无需改动。

回退前停止新能力并排干实际任务，保留 checkpoint 与数据库状态。没有 Replay/Transfer/checkpoint 能力的旧实现不能接管这些状态；需要支持相同持久格式与计划指纹的实现恢复，或显式制定重放和去重方案。线路互通不等于完整业务状态回退，不能通过删除进度宣称回退完成。
trait 消费者的等价写法是 `fn group(&self) -> GroupSpec { GroupSpec::Broadcast("scope".into()) }`。
使用 `GroupSpec::Default` 或宏中省略 `group` 时，`KafkaConfig.group_id` 必须存在；命名 group 与广播 group
不依赖这个默认值。

### 6. 有状态单条消费者

需要依赖注入时，实现 `SingleConsumer` 或 `BatchConsumer`，再通过 `register()` / `register_batch()`
注册实例：

```rust
use std::sync::Arc;
use nasa::kafka::{GroupSpec, KafkaRecord, Result, SingleConsumer};

// 假设 Db 是业务数据库句柄，OrderCreated 已实现 serde 解码。
struct OrderConsumer {
    db: Arc<Db>,
}

impl SingleConsumer for OrderConsumer {
    type Message = OrderCreated;

    /// 业务作用：声明该消费者订阅的业务 topic。参数说明：无。返回：注册时冻结的 topic 集合。
    fn topics(&self) -> Vec<String> { vec!["orders".into()] }
    /// 业务作用：限定该路由接收的事件。参数说明：无。返回：与发布端精确匹配的事件原文。
    fn event(&self) -> String { "created".into() }
    /// 业务作用：确定该处理器的消费与提交身份。参数说明：无。返回：明确的命名组规则。
    fn group(&self) -> GroupSpec { GroupSpec::Named("order-worker".into()) }

    /// 业务作用：使用注入的数据库保存订单。参数说明：record 为当前订单记录。返回：保存结果决定是否允许 Auto 确认，外部写入须可幂等重试。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()> {
        self.db.save(&record.value).await
    }
}
```

```rust
let runtime = kafka
    .consumers()
    .with_collected()? // 可以和宏消费者同时注册
    .register(OrderConsumer { db: db.clone() })?
    .start()
    .await?;

println!("resolved groups: {:?}", runtime.groups());
```

有状态消费者需要 Manual Ack 时覆盖 `ack_mode()`；确认规则与宏消费者完全相同：

```rust
use nasa::kafka::{AckMode, GroupSpec, KafkaRecord, Result, SingleConsumer};

struct PaymentConsumer {
    db: Arc<Db>,
}

impl SingleConsumer for PaymentConsumer {
    type Message = PaymentCreated;

    /// 业务作用：声明该消费者订阅的业务 topic。参数说明：无。返回：注册时冻结的 topic 集合。
    fn topics(&self) -> Vec<String> { vec!["payments".into()] }
    /// 业务作用：限定该路由接收的事件。参数说明：无。返回：与发布端精确匹配的事件原文。
    fn event(&self) -> String { "created".into() }
    /// 业务作用：确定该处理器的消费与提交身份。参数说明：无。返回：明确的命名组规则。
    fn group(&self) -> GroupSpec { GroupSpec::Named("settlement".into()) }
    /// 业务作用：要求业务持久化后显式确认。参数说明：无。返回：Manual，回调最终成功才保留有效确认。
    fn ack_mode(&self) -> AckMode { AckMode::Manual }

    /// 业务作用：保存付款并手动确认来源。参数说明：record 为付款记录。返回：保存与确认均成功才完成，错误使本次临时确认失效。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()> {
        self.db.save(&record.value).await?;
        record.ack()?;
        Ok(())
    }
}
```

### 7. 有状态批消费者

```rust
use nasa::kafka::{BatchConsumer, GroupSpec, KafkaRecord, Result};

struct RebuildConsumer {
    index: Arc<SearchIndex>,
}

impl BatchConsumer for RebuildConsumer {
    type Message = OrderCreated;

    /// 业务作用：声明该消费者订阅的业务 topic。参数说明：无。返回：注册时冻结的 topic 集合。
    fn topics(&self) -> Vec<String> { vec!["orders".into()] }
    /// 业务作用：限定该路由接收的事件。参数说明：无。返回：与发布端精确匹配的事件原文。
    fn event(&self) -> String { "created".into() }
    /// 业务作用：确定该处理器的消费与提交身份。参数说明：无。返回：明确的命名组规则。
    fn group(&self) -> GroupSpec { GroupSpec::Named("search-index".into()) }

    /// 业务作用：以当前批订单更新搜索索引。参数说明：records 为连续输入记录。返回：整批更新成功才允许确认，重复调用需由索引写入语义处理。
    async fn consume_batch(
        &self,
        records: Vec<KafkaRecord<Self::Message>>,
    ) -> Result<()> {
        self.index.replace_batch(records.iter().map(|record| &record.value)).await?;
        Ok(())
    }
}

let runtime = kafka
    .consumers()
    .register_batch(RebuildConsumer { index })?
    .start()
    .await?;
```

### 8. Auto/Manual Ack

`AckMode` 是 route 级业务确认策略，不是 Kafka 客户端的 auto commit 开关。两种模式最后都进入
同一个 partition 连续安全水位，由 owner 批量提交 broker offset。

| 模式 | handler 结果 | 本次记录是否安全确认 |
|---|---|---|
| Auto | `Ok(())` | 是 |
| Auto | `Err`、panic、timeout/cancel | 否 |
| Manual | 已 `ack()` 且最终 `Ok(())` | 是，仅计算连续已确认前缀 |
| Manual | 未 `ack()` 但返回 `Ok(())` | 否 |
| Manual | 已 `ack()` 后返回 `Err`、panic、timeout/cancel | 否，临时确认作废 |

Manual Ack 推荐写法：先完成具有业务意义的持久化，再同步调用 `ack()`，最后返回 `Ok(())`。
`ack()` 只记录本次回调内的临时确认，不访问 broker，也不能确认任意 topic/partition/offset。
不要把 `AckToken` 移交给后台任务后让 handler 提前返回。

```rust
#[kafka_consumer(
    topics = ["payments"],
    event = "created",
    group = "settlement",
    ack = "manual"
)]
/// 业务作用：付款持久化后确认当前输入。参数说明：record 为带确认权威的付款。返回：持久化与确认成功后完成。
async fn settle(record: KafkaRecord<PaymentCreated>) -> Result<()> {
    persist(&record.value).await?;
    record.ack()?;
    Ok(())
}
```

需要取得 payload、上下文和 token 的所有权时使用 `into_manual_parts()`，但 token 仍只在当前 handler
invocation 内有效：

```rust
/// 业务作用：拆出付款与来源信息完成持久化，再使用当前回调的 token 确认。参数说明：record 为手动确认记录。返回：持久化和确认均成功才完成。
async fn settle_owned(record: KafkaRecord<PaymentCreated>) -> Result<()> {
    let (payment, ctx, ack) = record.into_manual_parts()?;
    persist_with_source(&payment, &ctx).await?;
    ack.ack()?;
    Ok(())
}
```

Auto route 调用 `record.ack()` 或 `into_manual_parts()` 会返回 `NafkaError::AckNotManual`，不会改变 offset。

批消费允许非连续 ack，但只有从本批第一条开始的连续前缀能够推进提交水位；空洞之后的 ack
只作为审计信息保留，不能越过未确认记录。

Manual 批消费全部成功时可以使用 `AckBatchExt::ack_all()`：

```rust
use nasa::kafka::{AckBatchExt, AckMode, BatchConsumer, KafkaRecord, Result};

impl BatchConsumer for SettlementBatchConsumer {
    type Message = PaymentCreated;

    /// 业务作用：声明该消费者订阅的业务 topic。参数说明：无。返回：注册时冻结的 topic 集合。
    fn topics(&self) -> Vec<String> { vec!["payments".into()] }
    /// 业务作用：限定该路由接收的事件。参数说明：无。返回：与发布端精确匹配的事件原文。
    fn event(&self) -> String { "created".into() }
    /// 业务作用：要求业务持久化后显式确认。参数说明：无。返回：Manual，回调最终成功才保留有效确认。
    fn ack_mode(&self) -> AckMode { AckMode::Manual }

    /// 业务作用：批量持久化付款后确认全部输入。参数说明：records 为当前连续批记录。返回：持久化与全部确认成功才完成，任一错误使本次临时确认失效。
    async fn consume_batch(
        &self,
        records: Vec<KafkaRecord<Self::Message>>,
    ) -> Result<()> {
        persist_all(&records).await?;
        records.ack_all()?;
        Ok(())
    }
}
```

如果只确认下标 `0、1、3`，安全前缀只到 `1`；下标 `2` 是空洞，`3` 不能被提交越过。即使已调用
`ack()`/`ack_all()`，后续返回 `Err`、panic 或 timeout 时，本次所有临时确认仍全部作废。

### 9. ProtocolBytes 消费者

业务类型通过 `Proto<T>` 选择二进制 codec，并用 `ProtoMode` 把类型固定到一种 wire mode。下面使用
`ws-kafka` feature 中重导出的 `Mode`/`WireCodec`；纯 JSON 消费者不需要这些类型。

```rust
use nasa::kafka::{kafka_consumer, KafkaRecord, Proto, ProtoMode, Result};
use nasa::ws::{Mode, WireCodec};

struct BitpackOrder(OrderWire);

impl WireCodec for BitpackOrder {
    /// 业务作用：按指定线路模式编码订单。参数说明：mode 为调用方选择的模式。返回：原 codec 的字节或编码错误。
    fn encode(&self, mode: Mode) -> nasa::ws::proto::Result<Vec<u8>> {
        self.0.encode(mode)
    }

    /// 业务作用：按线路声明恢复订单类型。参数说明：mode 为线路模式，bytes 为正文。返回：订单包装值或解码错误。
    fn decode(mode: Mode, bytes: &[u8]) -> nasa::ws::proto::Result<Self> {
        OrderWire::decode(mode, bytes).map(Self)
    }
}

impl ProtoMode for BitpackOrder {
    const MODE: Mode = Mode::BitpackTlv;
}

#[kafka_consumer(
    topics = ["orders-binary"],
    event = "created",
    group = "binary-worker"
)]
/// 业务作用：处理按已声明模式解码的二进制订单。参数说明：record 为 Proto 包装的业务记录。返回：业务处理成功才允许确认。
async fn consume_binary(record: KafkaRecord<Proto<BitpackOrder>>) -> Result<()> {
    handle_wire_order(&record.value.0.0).await?;
    Ok(())
}
```

producer 和 consumer 必须对同一业务类型使用相同 mode；nafka 会通过 codec/mode header 校验，未知或不匹配
的 mode 不会被猜测解码。可用 mode 为 `JsonBytes`、`VarintTlv`、`BitpackTlv`。

### 10. 同步借用式 passthrough

`PassthroughConsumer` 用于 payload 已经是最终业务字节、不希望先反序列化成拥有型对象的路径：

```rust
use nasa::kafka::{
    BorrowedKafkaRecord, GroupSpec, InvalidRecordReason, PassthroughConsumer,
    PassthroughDisposition, PassthroughFailure, PassthroughRetryReason,
};

struct RawOutboxAdapter {
    outbox: LocalOutbox,
}

impl PassthroughConsumer for RawOutboxAdapter {
    /// 业务作用：声明该消费者订阅的业务 topic。参数说明：无。返回：注册时冻结的 topic 集合。
    fn topics(&self) -> Vec<String> { vec!["socket-data".into()] }
    /// 业务作用：限定该路由接收的事件。参数说明：无。返回：与发布端精确匹配的事件原文。
    fn event(&self) -> String { "NAWS_PUSH".into() }
    /// 业务作用：确定该处理器的消费与提交身份。参数说明：无。返回：明确的命名组规则。
    fn group(&self) -> GroupSpec { GroupSpec::Named("socket-node-a.data".into()) }

    /// 业务作用：将借用正文同步送入本地 outbox。参数说明：record 为当前 poll 有效的消息借用。返回：本地准入结果，不代表 socket 远端已接收；无正文拒绝，队列故障请求重试。
    fn consume_borrowed(
        &self,
        record: BorrowedKafkaRecord<'_>,
    ) -> std::result::Result<PassthroughDisposition, PassthroughFailure> {
        let payload = record.payload.ok_or_else(|| {
            PassthroughFailure::invalid(
                InvalidRecordReason::MissingPayload,
                "data topic 不允许 tombstone",
            )
        })?;

        let queued = self.outbox.enqueue_borrowed(payload).map_err(|_| {
            PassthroughFailure::retryable(
                PassthroughRetryReason::SocketEnqueue,
                "本地 outbox 暂时不可入队",
            )
        })?;
        Ok(PassthroughDisposition::Handled { queued })
    }
}

let runtime = kafka
    .consumers()
    .register_passthrough(RawOutboxAdapter { outbox })?
    .start()
    .await?;
```

`LocalOutbox` 是示例中的业务同步适配层；生产 WebSocket 集群应直接使用 `nasa::ws::kafka` 提供的
`HeaderPassthroughAdapter` 和 `WsKafkaRuntime`。借用 record 为 `!Send + !Sync`，不能放入 async task、线程或
channel。回调必须同步返回强类型结果：

- `Handled`：全部目标已进入本地有界 outbox。
- `Skipped`：无本地目标、节点不匹配、回环或旧来源等安全跳过。
- `BestEffortDropped`：显式 best-effort 下允许部分丢弃。
- `PassthroughFailure::retryable/invalid/dead_letter/halt/fatal`：分别进入唯一对应的框架状态机。

普通 passthrough 默认 Auto，成功 disposition 就产生安全结果。自定义 adapter 也可以返回
`AckMode::Manual` 并在同步回调内调用 `record.ack()`；Kafka→naws 的标准 adapter 使用 disposition 表达本地
outbox 决策，不需要业务 Manual token。这里的确认边界是“本地 outbox 已完成入队决策”，不是客户端已经收到。

`Skipped` 是明确的安全跳过事实，即使配置 Manual 也不要求额外 Ack；`Handled` 和
`BestEffortDropped` 在 Manual 下缺少确认会进入普通重试预算。临时容量拒绝同样消耗普通失败预算，
显式 DLT 必须取得 broker 确认后才能推进来源。分区硬暂停保留失败位点，致命不变量失败关闭会话路由。
部分 fan-out 入队后返回失败可能重投，目的端需要稳定业务身份识别重复。

回调只允许有界解析与非阻塞本地准入，不能等待网络、锁或 Registry；违约阻塞会延迟 owner poll，
调用方应同时监测 `last_poll_at`，不能把线程存在视为持续健康。Java 对应 `KafkaPassthroughListener`
使用运行时只读访问域并在有界 worker 执行，内存限制不等同于 Rust 的编译期借用保证。

### 11. 无匹配 route、坏消息与 tombstone

这三类记录不会按普通业务失败重试：

- `(topic, event)` 没有注册 route：按 `unmatched_policy` 执行 `skip`、`dead_letter` 或 `halt`。
- event 非 UTF-8、未知 codec、mode 不匹配或解码失败：按 `invalid_record_policy` 执行
  `skip`、`dead_letter` 或 `halt`，不调用业务 handler，也不消耗 `max_consume_attempts`。
  默认 `dead_letter`；显式 `skip` 放弃输入且不写死信，仍服从分区连续水位和连续解码失败保护。
- typed route 收到 tombstone：`behavior.tombstone_policy` 默认 `skip`，不调用反序列化和业务 handler，但仍受 partition 连续水位约束；
  passthrough route 会收到 `payload: None`，由 adapter 明确决定。socket data adapter 将其分类为
  `MissingPayload`。

消费 compacted topic 时，普通 value 继续使用 typed consumer；需要观察删除事件时显式选择可空 typed
交付或处理 `payload: None` 的借用 adapter。tombstone 不会被构造成 `T` 的某个默认业务对象。

`tombstone_policy` 还支持 `deliver`、`dead_letter`、`halt`。`deliver` 通过 `DecodePayload::decode_tombstone`
交付给显式可空类型：JSON 可以使用 `Option<T>`，任意 codec 可使用 `NullablePayload<T>`，包括
`NullablePayload<Proto<T>>`；非可空类型拒绝时仍按无效记录策略处置。`dead_letter` 要求非空 DLT 后缀，
确认交付后才推进来源水位；`halt` 保留未提交输入并暂停分区。

`KafkaRecord.ctx.tombstone` 表示原始 value 是否为 Kafka null，JSON `null` 不设置该标志；零长字节仍按
codec 解码。借用消费继续接收原始 `Option<&[u8]>` 并自行裁决，不受 typed 策略影响。无匹配路由或非法
路由头仍由对应策略先裁决，null 不绕过路由身份检查。
删除标记没有载荷编码，因此不执行目标 codec/mode 匹配；保留头唯一性、UTF-8 和已知 codec 名称的基础检查仍生效。

### 12. 固定分区 assign

回放、迁移和运维任务可以绕过 group subscription，固定消费指定分区：

```rust
use nasa::kafka::{StartOffset, Tp};

let assignment = kafka
    .assign(
        "order-replay",
        [Tp::new("orders", 0), Tp::new("orders", 1)],
        StartOffset::Timestamp(replay_from_epoch_ms),
        OrderConsumer { db: db.clone() },
    )
    .await?;

println!("partitions={:?}", assignment.partitions().await?);
println!("health={:?}", assignment.health().await?);

assignment.stop().await?;
```

起点支持：

- `Earliest`：当前 retention 范围内最早位置。
- `Latest`：启动时末尾，只消费之后的新消息。
- `Timestamp(epoch_ms)`：第一条 timestamp 不小于目标时间的记录，查不到时到末尾。
- `Offset(offset)`：显式 offset。
- `Committed`：沿用 group 已提交位置；无提交时回退 `auto_offset_reset`。

assign group 与 subscribe group 共用同一命名空间，不能重名；`AssignmentHandle::stop()` 幂等，Drop 不会
隐式停止任务。

## 生产者使用场景

生产者 API 分为类型化、ProtocolBytes、raw、tombstone 和批量五类；`KafkaProxy` 便捷方法固定使用
`default` lane，`ProducerLane` 提供相同的单条发布入口。

| 场景 | 入口 | 等待 broker 结果 | payload 处理 |
|---|---|---|---|
| JSON 业务对象 | `publish(topic, &value)` | `send()` 是；`fire()` 否 | serde JSON |
| ProtocolBytes | `publish(topic, &Proto(value))` | 可选 | 固定二进制 mode |
| 已编码原始字节 | `publish_raw(topic, bytes)` | 可选 | 不经过 serde |
| compacted topic 删除标记 | `tombstone(topic, key)` | 可选 | Kafka null value |
| 有序批量业务对象 | `publish_batch(topic, items)` | 是，逐条 | 保留成功前缀 |
| control/data 隔离 | `producer_lane(name)` | 可选 | 独立物理 producer |

### 1. 类型化 JSON：发送并等待 delivery

实现 `serde::Serialize` 的类型自动走 JSON codec：

```rust
let delivery = kafka
    .publish("orders", &order)
    .event("created")
    .key(&order.id)
    .timestamp(epoch_ms)
    .trace_context(&trace_context)
    .send()
    .await?;

println!(
    "broker partition={} offset={}",
    delivery.partition,
    delivery.offset,
);
```

builder 字段语义：

- `event(name)`：写入 `X-Nasa-Event`；不设置时使用 `DEFAULT`。
- `key(string)`：参与 Kafka 分区；默认 `murmur2_random` 与常见跨语言 producer 对字符串 key 的结果一致。
- `partition(n)`：显式指定非负分区，设置后不再由 key 选择。
- `timestamp(epoch_ms)`：设置 Kafka record timestamp。
- `header(name, value)`：追加 header；允许同名重复，`None` 表示 Kafka null header value。
- `ctx(&record.ctx)`：复制消费上下文中的 passthrough Map，适合消费后继续发布。
- `passthrough(key, value)?`：追加一个可 JSON 序列化的跨服务上下文字段。
- `trace_context(parent)`：显式绑定父 trace context，优先于当前任务的环境 context。

业务 header 保留顺序、重复名和 null value。`X-Nasa-*` 框架 header 由类型化 builder 生成，业务不能
通过通用 `header()` 覆盖。

业务未显式调用 `trace_context` 时，类型化、raw 与 tombstone builder 会读取当前 task-local 环境
context，派生 producer 子 context 并写入唯一 W3C `traceparent`；当前任务没有环境 context 时不凭空
建立链路。consumer 只接纳唯一且名称精确匹配的 `traceparent`，version 00、小写十六进制与非零身份均需有效。
重复、null、非法字节或不支持的版本只放弃外来 trace，合法业务消息继续处理；`trace_invalid_total` 记录有限的
malformed/duplicate 原因。有效单条父节点派生 consumer 子 context；缺失时按 exporter 的冻结采样策略建立新根。
未安装 recorder 时仍创建不采样的传播上下文。handler 内未显式绑定的 REST/Kafka 出站调用继续使用该环境 context；
同一消息重投时从原 header 延续同一 trace，DLT 也保留该 header。

批调用建立独立根；`KafkaTraceLinks::current()` 在真实业务 future 内提供最多128个不同有效父链接，
`omitted` 统计容量不足时未保留的有效输入数。各条消息的身份仍保存在 `KafkaRecord.ctx`。
应用遥测适配器可从该结构构造 span links；当前 `natelemetry::SpanRecord` 不含原生 links 字段，默认 exporter
只输出批调用根，不能宣称它已自动输出全部因果边。`BorrowedKafkaRecord::trace_context()` 为同步透传提供同一校验规则。
task-local 作用域跨 await 保留并在退出后恢复；新 Tokio task 必须显式使用捕获的上下文及 `natelemetry::with_ambient`，
批链接也应按应用需要显式传递。Java 的跨 Executor continuation 同样需要显式绑定。

消费 span 随实际 worker 结束；producer span 随独立原生交付观察者结束，fire 返回、取消发送 future 或调用方等待超时
都不能提前结束原生在途 span。SpanRecord 描述本地执行寿命，不提供 Kafka 提交或成功交付裁决；结果未知仍应从交付结果、
lane 状态及 `CloseReport` 查询。`SpanRecorder` 使用应用提供的 `BoundedSpanExporter`，源不拥有其最终 flush 或关闭权，
应用应在 Kafka 责任退出后关闭出口。trace 队列饱和可丢弃观测，不改变业务确认。
`kafka.consume` 与 `kafka.consume_batch` span 的 `trace_id` 字段固定表示当前 W3C trace ID；业务
passthrough 中名为 `traceId` 的自定义字段不参与该日志字段的取值。

### 2. `send()` 与 `fire()`

`send()` 编码、入队并等待 delivery report，返回实际 partition/offset。调用方需要确认 broker 结果时使用：

```rust
let delivery = kafka
    .publish("orders", &order)
    .event("created")
    .key(&order.id)
    .send()
    .await?;
```

`fire()` 完成有界 observer 准入和同步入队后立即返回，后台仍会统计最终 delivery；适合 presence、通知等
不阻塞当前调用的场景：

```rust
kafka
    .publish("presence", &presence)
    .event("online")
    .key(&presence.user_id)
    .fire()?;
```

`fire() == Ok(())` 只表示消息已进入受观察的本地投递流程，不表示 broker 已确认，更不表示消费者已处理。
用 `kafka.producer_lane("default")?.stats()` 观察累计 delivery、queue 和 generation 状态；停机时
`kafka.shutdown().await` 会排空所有 lane。

`send()` 与 `fire()` 共用每 lane 的 `fire_observer_capacity` 硬上限。发送前先取得观察额度，
不足时返回 `ProducerQueueFull`；本地队列字节与记录数仍由独立 native 配置限制。
取消 `send()` future 或等待期限到达只结束调用方等待，已入队记录继续由独立任务等待 native 回执，
其 observer 和 active 责任不会提前释放。`OutcomeUnknown` 不能视为“没有发送”，直接重试可能重复。
停机超时保留这些真实责任，可继续调用关闭入口等待收口。

### 3. 消费后继续发布并透传上下文

有状态消费者可以持有 `KafkaProxy`，把 trace 等 passthrough 上下文传给下游消息：

```rust
struct OrderPipeline {
    kafka: KafkaProxy,
}

impl SingleConsumer for OrderPipeline {
    type Message = OrderCreated;

    /// 业务作用：声明该消费者订阅的业务 topic。参数说明：无。返回：注册时冻结的 topic 集合。
    fn topics(&self) -> Vec<String> { vec!["orders".into()] }
    /// 业务作用：限定该路由接收的事件。参数说明：无。返回：与发布端精确匹配的事件原文。
    fn event(&self) -> String { "created".into() }
    /// 业务作用：确定该处理器的消费与提交身份。参数说明：无。返回：明确的命名组规则。
    fn group(&self) -> GroupSpec { GroupSpec::Named("order-pipeline".into()) }

    /// 业务作用：转换订单并等待下游交付后完成来源处理。参数说明：record 为订单和透传上下文。返回：下游确认才成功；发布与来源提交分离，重投可能重复。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()> {
        let indexed = build_index_event(&record.value)?;
        self.kafka
            .publish("order-index", &indexed)
            .event("upsert")
            .key(&record.value.id)
            .ctx(&record.ctx)
            .passthrough("source_offset", record.ctx.offset)?
            .send()
            .await?;
        Ok(())
    }
}
```

该例中下游 publish 失败会让当前 handler 返回 `Err`，因此来源记录不 ack，重投时下游可能重复收到；下游应使用
业务 key/幂等键去重。

### 4. 原始字节 `publish_raw`

payload 已经编码完成时使用 raw builder。它不会添加 codec/mode header，也不会先经过 serde 或构造中间
`Vec<u8>`：

```rust
let delivery = kafka
    .publish_raw("socket-data", payload)
    .event("cluster-data")
    .key(source_node)
    .timestamp(epoch_ms)
    .header("content-type", Some(b"application/octet-stream"))
    .send()
    .await?;
```

也可以调用 `.fire()?` 使用异步观察投递。空 slice 是合法的 `Some(&[])`，与 tombstone 的 null value 不同。
raw builder 只提供 `event/key/partition/timestamp/header`；它不提供 typed builder 的 `ctx()` 与
`passthrough()` JSON 上下文。已有编码 bytes 直接使用 raw；只有业务对象才使用 typed publish，避免把
`Vec<u8>` 误序列化成 JSON 数字数组。

Kafka→WebSocket 数据面不要手写 `X-Nasa-WS-*` header；使用 `nasa::ws::kafka::WsKafkaPublisher` 或
`KafkaClusterDataPublisher`，由安全 builder 生成路由、来源、目标节点、sink 和 message mode。

### 5. ProtocolBytes 发布

ProtocolBytes 类型需要实现 `WireCodec + ProtoMode`，发布时用透明包装 `Proto<T>`：

```rust
let wire = Proto(BitpackOrder(OrderWire::from(&order)));
let delivery = kafka
    .publish("orders-binary", &wire)
    .event("created")
    .key(&order.id)
    .send()
    .await?;
```

nafka 会自动写 `X-Nasa-Payload-Codec=protocol-bytes` 和匹配的 `X-Nasa-Payload-Mode`。同一业务类型应固定
一种 mode，consumer 使用相同的 `Proto<T>` 类型；不要在运行时根据消息内容猜 codec。

### 6. tombstone

compacted topic 需要删除某个 key 时显式发布 tombstone：

```rust
let delivery = kafka
    .tombstone("order-state", "order/O-1001")
    .event("deleted")
    .header("reason", Some(b"expired"))
    .send()
    .await?;
```

tombstone 必须有字符串 key，允许空字符串并精确保留为零长度字节键，value 固定为 Kafka null。空键与缺失键不同；Rust 的 `&str` 参数不能表达缺失键，Java 显式 tombstone 对 null key 返回 `CONFIG`。`proxy.publish_raw_nullable(topic, None)` 和同名 lane 入口发布 raw null value，对应 Java `publishRaw(topic, null)`；raw 仍独立支持缺失、空字节和任意二进制 key。`Some(&[])` 与 `publish_raw(topic, &[])` 均保留为零长度 value，不变成 null。匹配到 typed route 时默认安全跳过，显式 `deliver`
才交付给可空业务类型。tombstone builder 只提供 `event/header/send/fire`，不接受 payload；同样
支持 `.fire()?`。需要观察墓碑的专用同步路径应使用明确处理 `payload: None` 的 passthrough adapter，或改用
携带业务删除对象的普通事件。

### 7. 顺序批发布

`publish_batch()` 按输入顺序逐条等待 delivery，在首个失败或结果未知处停止：

```rust
use nasa::kafka::PublishItem;

let items = orders.into_iter().map(|order| {
    let key = order.id.clone();
    PublishItem::new(order)
        .key(key)
        .event("created")
});

match kafka.publish_batch("orders", items).await {
    Ok(deliveries) => {
        println!("全部发布成功 count={}", deliveries.len());
    }
    Err(error) => {
        eprintln!(
            "成功前缀={} 首个失败下标={} 原因={}",
            error.delivered.len(),
            error.failed_index,
            error.source,
        );
    }
}
```

这是确定的顺序便利 API，不是 Kafka transaction：已确认成功的前缀不会回滚。批量项支持 payload、key、event；
需要 partition/timestamp/custom header 时逐条使用 builder。

### 8. 命名 producer lane

命名 lane 用于 control/data、实时/批处理或不同可靠性等级的物理隔离：

```rust
let control = kafka.producer_lane("control")?;
let delivery = control
    .publish_raw("socket-control", bytes)
    .key(node_id)
    .send()
    .await?;

let stats = control.stats();
control.flush().await?;
```

每个 lane 必须在 `KafkaConfig.producer.lanes` 中预先声明，运行中不能动态创建。每个 lane 都有独立 native
producer、queue、admission、observer 和 client.id；`KafkaProxy` 上的 `publish/publish_raw/tombstone/
publish_batch` 始终使用 `default` lane。

lane 还可以分别覆盖 `compression`、linger、batch size、超时、重试和 stalled-generation 重建策略。

`stalled_rebuild_after_ms`、`stalled_rebuild_min_failures`、`stalled_rebuild_cooldown_ms` 默认分别为60000ms、2次、60000ms；只有当前代次的连续终态失败与无成功时间同时达到条件才触发切换，成功会清空当前窗口。旧代次继续完成自己的交付，其迟到成功或永久错误不能改变新代次的恢复窗口。当前代次明确认证、授权或fencing错误进入永久保护，未知消息不会自动重发，幂等序列不跨代次延续。`ProducerLaneStats`的`native_generation`、`native_restarted`、`native_stalled_restarted`、`terminal_fatal`、`live_native`与`consecutive_failures`分别描述代次、累计重建、无进展重建、永久保护、真实原生资源数和当前连续失败数。
使用 `compression: zstd` 时编译开启 `kafka-zstd`；control lane 通常保持低 linger，data lane 再按吞吐目标调优。

默认每lane最多10000条发送责任、65536KiB和10000个交付观察槽，以先耗尽的额度限制准入。原生客户端、压缩及连接开销不包含在该字节上限内。需要更高额度时显式设置`queue_buffering_max_messages`与`queue_buffering_max_kbytes`，不能依赖原生库默认值。命名lane分别继承源配置，彼此不继承；`retry_backoff_ms`也可逐lane覆盖，零表示原生重试不主动延迟。

框架原文字节为 key、payload、所有 header 名的 UTF-8 字节与非 null 头值之和，重复头逐项计费；普通发布和内部 DLT/Transfer 共用准入门禁。条数、字节和 observer 一直保留到实际交付责任结束，调用方超时或取消不会归还，旧原生代次也继续计费。原生客户端自身的缓冲限制是另一层边界，框架额度不代表进程内存硬上限。`ProducerQueue::Native` 包含框架条数/字节额度和原生队列拒绝，具体框架原因由 `publish_admission_rejected_total` 的 `reason` 标签区分。

`behavior.max_record_bytes` 同时限制单条发布原文；包含框架追加头的完整输出超限时，在 native 准入前返回 `NafkaError::Publish`。事务输出使用 `TransactionLimits` 的独立批次条数与字节合同。类型化、raw 和 tombstone 发布在实际执行开始、编码之前冻结绝对期限；编码、准入退避和调用方结果等待共用 `publish_timeout_ms`。同步 codec 不保证可抢占，返回时已超期则拒绝 native 准入；消息可能发出后的等待超时仍是 `OutcomeUnknown`，观察者继续承担责任。

文本 key 使用精确 UTF-8；`key_bytes` 保留任意原始字节，null 与空 key 不同。双端支持的 header 名必须非空、为合法 UTF-8 且不含 NUL。普通发布、事务输出和框架内部转发都在 native 转换前验证；头值允许任意字节和 null，顺序、重复名称与大小写保持。外部 producer 也必须遵守名称合同：Kafka broker 允许存储带 NUL 的头名，而当前 librdkafka 头迭代器仅提供 NUL 前缀，不能据此还原或保证识别完整原名。这类外部输入不在精确保留与 strict 路由校验的支持范围；应在写入 Kafka 前约束生产者或验证原始输入。`strict_headers` 不认证写入者，也不能替代 topic 写权限和可信来源边界。

`KafkaConfig`及producer、consumer、behavior、security、admin、lane均拒绝未知字段。默认lane和所有命名lane的有效确认策略、幂等、重试顺序、容量及超时均在建立资源前校验，未使用的lane也不能隐藏非法声明。扩展原生参数放入相应`properties`映射，仍受保留键和认证域门禁约束。

共同配置毫秒上限为2147483647，原生客户端还会校验各自更细的字段限制；API接受的Duration有独立边界。`max_consume_attempts=0`表示无限业务重试；`handler_timeout_ms`为`null`或零表示不限调用确认时长，`decode_failure_halt_streak`为`null`或零关闭连续解码保护。Java对应后两项使用零。普通业务重投`consume_retry_backoff_ms`必须为正；producer重试退避、linger、fetch等待和启动延迟允许零。

Admin的`default_api_timeout_ms`未设置时继承`behavior.publish_timeout_ms`，`request_timeout_ms`未设置时继承该API预算，默认均为120000ms。请求预算不能超过API预算。需要独立管理时限时显式声明，不依赖原生库的默认超时。

以下列出共同字段的默认值；Java列为源下扁平配置键，Rust列为`KafkaConfig`内路径。容量字段的计费范围仍以对应消费、发送和DLT合同为准，数值相同不表示原生缓冲区相同。

| Java键 | Rust路径 | 默认值 |
|---|---|---|
| `admin-default-api-timeout-ms` | `admin.default_api_timeout_ms` | `120000` ms |
| `admin-request-timeout-ms` | `admin.request_timeout_ms` | `120000` ms |
| `control-queue-capacity` | `behavior.command_queue_capacity` | `1024` |
| `commit-batch-records` | `behavior.commit_batch_records` | `1000` |
| `commit-interval-ms` | `behavior.commit_interval_ms` | `1000` ms |
| `consume-restart-budget-times` | `behavior.consume_restart_budget_times` | `10` |
| `consume-restart-budget-window-ms` | `behavior.consume_restart_budget_window_ms` | `300000` ms |
| `consume-restart-delay-ms` | `behavior.consume_restart_delay_ms` | `3000` ms |
| `consume-restart-max-delay-ms` | `behavior.consume_restart_max_delay_ms` | `60000` ms |
| `consumer-retry-ms` | `behavior.consume_retry_backoff_ms` | `500` ms |
| `control-timeout-ms` | `behavior.control_timeout_ms` | `30000` ms |
| `dead-letter-required` | `behavior.dead_letter_required` | `true` |
| `dead-letter-topic-suffix` | `behavior.dead_letter_topic_suffix` | `.DLT` |
| `decode-failure-halt-streak` | `behavior.decode_failure_halt_streak` | `1000` |
| `dlt-producer-lane` | `behavior.dlt_producer_lane` | `default` |
| `dlt-queue-capacity` | `behavior.dlt_queue_capacity` | `1024` |
| `dlt-queue-max-bytes` | `behavior.dlt_queue_max_bytes` | `67108864` 字节 |
| `dlt-max-in-flight` | `behavior.dlt_max_in_flight` | `16` |
| `dlt-retry-initial-ms` | `behavior.dlt_retry_initial_ms` | `1000` ms |
| `dlt-retry-max-ms` | `behavior.dlt_retry_max_ms` | `60000` ms，指数基准上限 |
| `dlt-degraded-after-attempts` | `behavior.dlt_degraded_after_attempts` | `10` |
| `handler-timeout-ms` | `behavior.handler_timeout_ms` | Java `0` / Rust `null` ms |
| `invalid-record-policy` | `behavior.invalid_record_policy` | `dead_letter` |
| `max-poll-records` | `behavior.max_batch_records` | `500` |
| `max-batch-bytes` | `behavior.max_batch_bytes` | `16777216` 字节 |
| `max-consume-attempts` | `behavior.max_consume_attempts` | `3` |
| `max-progress-bytes-per-group` | `behavior.max_group_pending_bytes` | `67108864` 字节 |
| `max-progress-records-per-process` | `behavior.max_process_pending_records` | 1048576 |
| `max-progress-bytes-per-process` | `behavior.max_process_pending_bytes` | 1073741824 字节 |
| `max-progress-records-per-source` | `behavior.max_source_pending_records` | 262144 |
| `max-progress-bytes-per-source` | `behavior.max_source_pending_bytes` | 268435456 字节 |
| `max-progress-bytes-per-partition` | `behavior.max_partition_pending_bytes` | 67108864 字节 |
| `max-progress-records-per-key` | `behavior.max_key_pending_records` | 1024 |
| `max-progress-bytes-per-key` | `behavior.max_key_pending_bytes` | 16777216 字节 |
| `max-progress-records-per-group` | `behavior.max_group_progress_records` | `65536` |
| `max-handler-in-flight` | `behavior.max_in_flight_handlers` | `64` |
| `consumer.max-assignment-partitions` | `consumer.max_assignment_partitions` | `10000`，全组不同分区 |
| `behavior.max-retry-entries` | `behavior.max_retry_entries` | `100000`，全组失败记录 |
| `max-progress-records-per-partition` | `behavior.max_progress_records` | `2048` |
| `max-record-bytes` | `behavior.max_record_bytes` | `16777216` 字节 |
| `poll-timeout-ms` | `behavior.poll_timeout_ms` | `500` ms |
| `publish-timeout-ms` | `behavior.publish_timeout_ms` | `120000` ms |
| `shutdown-timeout-ms` | `behavior.shutdown_timeout_ms` | `30000` ms |
| `start-consume-delay-ms` | `behavior.start_consume_delay_ms` | `1000` ms |
| `strict-headers` | `behavior.strict_headers` | `true` |
| `tombstone-policy` | `behavior.tombstone_policy` | `skip` |
| `unmatched-policy` | `behavior.unmatched_policy` | `skip` |
| `auto-offset-reset` | `consumer.auto_offset_reset` | `latest` |
| `consumer-concurrency` | `consumer.concurrency` | `1` |
| `fetch-min-bytes` | `consumer.fetch_min_bytes` | `1` 字节 |
| `fetch-max-wait-ms` | `consumer.fetch_wait_max_ms` | `500` ms |
| `group-concurrency` | `consumer.group_concurrency` | `{}` |
| `heartbeat-interval-ms` | `consumer.heartbeat_interval_ms` | `10000` ms |
| `max-partition-fetch-bytes` | `consumer.max_partition_fetch_bytes` | `1048576` 字节 |
| `max-poll-interval-ms` | `consumer.max_poll_interval_ms` | `300000` ms |
| `session-timeout-ms` | `consumer.session_timeout_ms` | `30000` ms |
| `acks` | `producer.acks` | `all` |
| `batch-size` | `producer.batch_size` | `16384` 字节 |
| `compression-type` | `producer.compression` | `none` |
| `delivery-timeout-ms` | `producer.delivery_timeout_ms` | `120000` ms |
| `enable-idempotence` | `producer.enable_idempotence` | `true` |
| `fire-observer-capacity` | `producer.fire_observer_capacity` | `10000` |
| `linger-ms` | `producer.linger_ms` | `5` ms |
| `max-inflight-requests-per-connection` | `producer.max_in_flight_requests_per_connection` | `1` |
| `queue-buffering-max-kbytes` | `producer.queue_buffering_max_kbytes` | `65536` KiB |
| `stalled-rebuild-after-ms` | `producer.stalled_rebuild_after_ms` | `60000` ms |
| `stalled-rebuild-min-failures` | `producer.stalled_rebuild_min_failures` | `2` |
| `stalled-rebuild-cooldown-ms` | `producer.stalled_rebuild_cooldown_ms` | `60000` ms |
| `queue-buffering-max-messages` | `producer.queue_buffering_max_messages` | `10000` |
| `request-timeout-ms` | `producer.request_timeout_ms` | `30000` ms |
| `retries` | `producer.retries` | `3` |
| `retry-backoff-ms` | `producer.retry_backoff_ms` | `100` ms |

示例配置：

```yaml
kafka:
  producer:
    acks: all
    retries: 3
    enable_idempotence: true
    max_in_flight_requests_per_connection: 1
    lanes:
      control:
        acks: all
        enable_idempotence: true
        max_in_flight_requests_per_connection: 1
        queue_buffering_max_messages: 10000
        queue_buffering_max_kbytes: 65536
      data:
        acks: all
        enable_idempotence: true
        max_in_flight_requests_per_connection: 5
        queue_buffering_max_messages: 100000
        queue_buffering_max_kbytes: 262144
```

### 9. 发布错误分类

不要按错误字符串判断是否重试，使用 `NafkaError` variant：

```rust
use nasa::kafka::NafkaError;

match kafka.publish("orders", &order).event("created").send().await {
    Ok(delivery) => mark_sent(delivery),
    Err(NafkaError::ProducerQueueFull { queue, .. }) => {
        // 可以确认尚未入队，按容量退避后重试。
        backoff_for_queue(queue).await;
    }
    Err(NafkaError::Publish { .. }) => {
        // 确定失败；是否重试由业务幂等策略决定。
        schedule_retry(&order).await?;
    }
    Err(NafkaError::OutcomeUnknown { .. }) => {
        // 消息可能已经成功，禁止盲目重发；先按业务幂等键核对或进入人工/对账流程。
        mark_reconciliation_required(&order.id).await?;
    }
    Err(NafkaError::Lifecycle(_)) => {
        // runtime 已关闭或正在关闭，不应继续发布。
    }
    Err(error) => return Err(error),
}
```

### 10. flush 与停机

- `ProducerLane::flush().await`：冻结指定 lane 当前准入的责任，等待其交付观察退出。
- `KafkaProxy::flush().await`：在第一次等待前冻结全部 lane 的责任，并发等待各屏障，不停止消费者。
- 屏障包含旧原生代次的交付，不等待之后新发送；其中存在失败时返回错误，各消息保留具体结果。调用方超时或取消不撤销实际交付。停机另行等待原生 flush 与全部代次资源析构。
- `KafkaProxy::shutdown().await`：先停止新消费及任务准入，等待已接纳的有限任务和事务；再关闭普通发布准入，排干消费与 DLT、关闭全部 lane，最后释放 Admin 与观测资源。

业务正常停机使用 `shutdown()`；不要只 Drop `KafkaProxy`，也不要在 Tokio runtime 已停止后再尝试异步排空。

## 生命周期与 ready

推荐顺序：

1. `KafkaProxy::connect(config)`：校验配置，创建各 lane 的原生 producer 与后台资源；后台可开始连接，但返回成功不证明 broker 可用。
2. `kafka.consumers()`：注册静态、trait 或 passthrough 消费者。
3. `start().await`：冻结 registry，启动每个 group 的 owner。
4. `await_group_ready()` / `await_groups_ready()`：等待真实 join 和 assignment。
5. 对外开放 HTTP/WS ingress。
6. 停机时调用 `kafka.shutdown().await`，停止 owner 并排空全部 producer lane。

可选 ready 条件：

- `ReadyRequirement::Joined`：已完成一次 join，允许竞争组处于空 assignment standby。
- `ReadyRequirement::Assigned { min_partitions }`：至少持有指定数量的分区。
- `ReadyRequirement::AssignedTopics(topics)`：每个要求的 topic 至少分配一个分区。
- `ReadyRequirement::AssignedPartitions(partitions)`：必须拥有每个明确的 `Tp`，同 topic 的其它分区不能代替。

ready 返回的是带 assignment epoch 的时点快照；rebalance 后应以最新健康状态为准。

### 同时等待多个消费组

socket control/data 或一个服务内存在多个业务 group 时，共享同一个绝对 deadline：

```rust
let deadline = Instant::now() + Duration::from_secs(30);
let snapshots = kafka
    .await_groups_ready(
        vec![
            (
                "socket-node-a.control".into(),
                ReadyRequirement::AssignedTopics(vec!["socket-control".into()]),
            ),
            (
                "socket-node-a.data".into(),
                ReadyRequirement::AssignedTopics(vec!["socket-data".into()]),
            ),
        ],
        deadline,
    )
    .await?;
```

等待每轮复验全部绑定的 group 实例，只有同一轮全部满足才返回，不锁存较早成功的组，也不接管同名重建实例。
空列表、重复组名或非法要求返回 `Config`；已到期的 deadline 只观察一次。fatal、硬保护或停止立即失败；
deadline 到期返回 `GroupReadyTimeout`，其 `readiness` 保存所有未满足组的原要求、源名、稳定原因和同次健康，
`groups` 与 `last_health` 保留组名及健康列表。快照不提供返回之后的永久权威。

`group_readiness(group, requirement)` 可不等待地查询 `ReadyStatus`；`ReadyRequirement::reason` 与
`satisfied_by` 共用判定，区分启动、加入、失权、恢复、重试、提交阻塞、DLT/进度背压、硬保护、停止及分配缺口。
用户主动暂停不撤销分配就绪。原生瞬态错误等待会话收口或自动重建时为 `Recovering`，期间 Ready 关闭但允许继续等待；
`Degraded` 表示需要人工处置的保护，不会被当作正常重建退避。

### 查询 assignment、position 和健康状态

```rust
use nasa::kafka::Tp;

let group = "order-worker";
let assignment = kafka.assignment(group).await?;
let positions = kafka.position(group, assignment.clone()).await?;
let health = kafka.group_health(group).await?;

for tp in assignment {
    println!(
        "{}:{} position={:?}",
        tp.topic,
        tp.partition,
        positions.get(&tp).copied().flatten(),
    );
}
println!("state={:?} paused={:?}", health.state, health.paused);
```

`position` 是当前 owner 的消费位置；broker 已提交 offset 和 lag 使用 admin 查询：

```rust
let tps = vec![Tp::new("orders", 0), Tp::new("orders", 1)];
let committed = kafka.admin().committed_offsets(group, tps.clone()).await?;
let lag = kafka.admin().committed_lag(group, tps).await?;
```

### pause、resume 与 seek

```rust
use std::collections::BTreeMap;
use nasa::kafka::Tp;

let tp0 = Tp::new("orders", 0);

kafka.pause("order-worker", vec![tp0.clone()]).await?;
kafka.resume("order-worker", vec![tp0.clone()]).await?;

kafka.seek("order-worker", tp0.clone(), 1200).await?;
kafka.seek_to_beginning("order-worker", vec![tp0.clone()]).await?;
kafka.seek_to_end("order-worker", vec![tp0.clone()]).await?;
kafka.seek_to_committed("order-worker", vec![tp0.clone()]).await?;
kafka
    .seek_to_timestamp(
        "order-worker",
        BTreeMap::from([(tp0, replay_from_epoch_ms)]),
    )
    .await?;
```

对整个 group 暂停/恢复使用 `pause_all(group)` / `resume_all(group)`。seek 会清理目标分区的本地
progress/retry 状态；存在未完成 DLT 等冲突时会被拒绝，不能用 seek 越过未安全处理的记录。

`control_partitions(group, actions)` 提供逐分区 `ControlReport`，保存逻辑源、最终组名和完整请求目标的结局：

| `ControlOutcome` | 执行证据 |
| --- | --- |
| `Applied` | owner 已确认目标完成，之后仍可能发生 rebalance |
| `NotApplied` | 尚未派发或在副作用前拒绝/取消 |
| `Unknown` | 已可能生效，须查询位置或暂停状态后决定，不能盲目重发 |
| `OwnerChanged` | 缺少唯一 owner 或冻结代次失效，旧意图未执行 |

```rust
use nasa::kafka::{PartitionControl, Tp};
let report = kafka.control_partitions("order-worker", [
    (Tp::new("orders", 0), PartitionControl::Pause),
    (Tp::new("orders", 1), PartitionControl::Seek(100)),
].into()).await?;
```

`PartitionControl` 还支持 `Resume`、`Beginning`、`End`、`Timestamp` 和 `Committed`。非空请求先完整验证声明和归属，缺席或重复归属时整批不派发。准入与执行共用 `control_timeout_ms`，每个命令携带开始时冻结的 owner 代次；已确认成功的目标不会被后续超时覆盖。未知结果后停止本次后继派发，报告不承诺跨分区原子性、执行顺序、回滚或自动重路由。取消等待会撤销尚未开始的命令，已开始的 native 操作仍由原 owner 收口；丢弃 future 不会交付报告，调用方须查询已有副作用。

显式 `Committed` 和 `seek_to_committed` 在无提交历史时拒绝；固定分配首次 `StartOffset::Committed` 仍遵循 `auto_offset_reset`。特殊边界先冻结为绝对坐标，再执行定位，不会在稍后恢复 fetch 时重新计算末尾。定位只清理目标分区的账本与完成证据；其它分区的重试前缀和已经完成的后继保持有效。恢复只移除 User 原因，内部保护继续暂停分区。

### 动态订阅与显式恢复

```rust
kafka
    .subscribe_group(
        "order-worker",
        vec!["orders".into(), "orders-priority".into()],
    )
    .await?;

kafka.unsubscribe_group("order-worker").await?;

// 仅用于允许恢复的 Degraded/Crashed 状态，不是正常流量下的重启按钮。
kafka.restart_group("order-worker").await?;
```

动态 subscribe 只改变 topic 集合，route 表仍是在 `start()` 时冻结的；新 topic 上没有匹配 event 的记录按
`unmatched_policy` 处理。固定 assign owner 不支持 subscribe/unsubscribe。

固定分配只在首次初始化时解释 `Earliest`、`Latest`、时间戳或 `Committed`；同一 owner 自动或显式重建时使用保存的绝对恢复坐标。broker 确认提交后才推进恢复点，成功显式 seek 则替换目标分区的位置，内部重读不覆盖它。恢复坐标已经超出保留范围时保持保护，不自动跳到新的起点。未确认提交的业务仍可能重复执行。

原生临时连接错误撤销旧 Ready，并在 `consume_restart_budget_window_ms` 窗口内使用有限重建预算和退避；预算耗尽进入 `Crashed`，broker 返回不会自行解除保护。metadata 响应只证明该查询可用，组协调器和交付路径可能尚未恢复。停止会打断恢复退避并阻止后续会话启动。

控制命令错误需要按语义处理：

- `CommandQueueFull`：owner 正忙，稍后退避重试。
- `ControlNotApplied`：在副作用前被拒绝或取消，解除原因后可以重试。
- `ControlOwnerChanged`：旧分区代次失效，命令未执行，须重新确认归属。
- `ControlOutcomeUnknown`：owner 已开始执行，先查询 `position()` / `assignment()` 再决定，不能盲目重发命令。
- `NoSuchGroup`：group 未启动或已经停止。

### 消费失败策略配置

```yaml
kafka:
  consumer:
    auto_offset_reset: latest
    session_timeout_ms: 30000
    heartbeat_interval_ms: 10000
    max_poll_interval_ms: 300000
  behavior:
    handler_timeout_ms: 30000
    max_consume_attempts: 3       # 0 表示无限业务重试
    consume_retry_backoff_ms: 500
    invalid_record_policy: dead_letter
    unmatched_policy: skip
    dead_letter_topic_suffix: .DLT
    dead_letter_required: true
    dlt_queue_capacity: 1024
    dlt_queue_max_bytes: 67108864
    dlt_max_in_flight: 16
    commit_interval_ms: 1000
    commit_batch_records: 1000
```

- 业务 handler `Err`/panic/timeout：按 `max_consume_attempts` 和退避策略重试，耗尽后进入 DLT/Halt。
- 受审计控制态暂不允许处理时可返回 `NafkaError::HandlerDeferred`：仍 pause/seek/退避，但不消耗
  `max_consume_attempts`；只有确有外部恢复动作的门禁可使用，普通故障不得借此无限重投。
- 确定坏格式、缺 payload、codec 不匹配：不消耗业务重试次数，直接应用 `invalid_record_policy`。
- topic/event 没有 route：应用 `unmatched_policy`。
- DLT 成功完成后来源记录才获得安全结果；DLT required 场景不会因 DLT 暂时不可用而静默跳过。

## 投递与失败模型

- 普通消费语义为 at-least-once；[同集群事务](#同集群消费与输出事务)另行提供批准输出与输入位点的原子提交，不覆盖外部副作用。
- 默认 `PartitionOrdered` 在分区内串行；`KeyOrdered` 允许同分区不同原始 key 并发，同 key 串行，null key 形成分区屏障。两种模式都只提交连续安全前缀，健康分区可独立推进。
- Auto 模式只在 handler `Ok(())` 后产生安全确认。
- Manual 模式还要求业务显式 ack；最终提交仍不能越过未确认空洞。
- 业务 `Err`、panic 和 timeout 进入相同的失败状态机，按配置重试或进入 DLT。
- `HandlerDeferred` 保留来源 offset 并退避，但与普通毒消息预算隔离；恢复后同一消息重新进入 handler。
- `ConsumeCtx.delivery_attempt` 统计当前会话内的全部真实 handler 投递，包含 `HandlerDeferred`，并进入
  DLT 证据；`ConsumeCtx.retry_attempt` 只统计普通失败预算，暂停期间保持不变。业务重试策略必须使用
  后者，不能因暂停次数追溯性地耗尽毒消息预算。
- 格式确定无效的记录按 `InvalidRecordPolicy` 进入 DLT 或 Halt，不消耗业务重试次数。
- DLT 使用记录数和 retained bytes 双重有界队列。默认 `dead_letter_required=true` 必须取得实际交付确认才允许来源推进；显式设为 false 时可在 DLT 失败后丢弃并推进，须接受失败记录无法回投的后果。
- revoke、shutdown 和控制命令各自遵守所配置或传入的绝对期限；超时不释放仍在执行的责任，结果未知不推定为未执行。

必需 DLT 的失败重试独立于普通业务退避。`dlt_retry_initial_ms` 的指数基准逐次翻倍至
`dlt_retry_max_ms`，再按来源 offset 加入约正负百分之十的稳定抖动。
同一责任连续失败达到 `dlt_degraded_after_attempts` 后增加 `DltRetryDegraded` 分区原因并显示
Degraded；其它分区提交成功或用户 resume 不能将其清除。交付确认并完成来源安全提交后解除，
失权的旧交付也不能向新 assignment 写入该原因。停止不启动新的重试轮次，未确认来源保留原提交位置；后续能否重投仍取决于日志保留和资源身份。

## 少拷贝 passthrough

`PassthroughConsumer` 面向 Kafka payload 已经是最终 socket 业务字节的场景。回调接收
`BorrowedKafkaRecord<'_>`，其 topic、key、headers 和 payload 都借用当前 Kafka 消息：

- `BorrowedKafkaRecord` 明确为 `!Send + !Sync`，不能逃逸到异步任务、线程或 channel。
- 回调必须同步完成路由解析和本地 outbox 入队决策。
- NASA 二进制路径不构造拥有型 Kafka record，也不先复制 payload 到中间 `Vec`。
- payload 与信封一次写入最终 `Bytes`；fan-out 时仅 `Bytes::clone()`，多个 outbox 共享同一底层分配。
- 无本地目标时不构造最终 frame。
- librdkafka producer 入队仍会执行自己的 `RD_KAFKA_MSG_F_COPY`，因此 broker 发布侧定义为
  “框架零中间复制”，不是端到端绝对零复制。
- socket.io 必须执行 JSON/base64 文本转换，单独计入 one-copy/转码路径，不属于 NASA binary 少拷贝口径。

Kafka header 只表达逻辑路由和消息元数据，不能指定任意 IP、host 或端口。最终出站只能选择预注册的
本地 `Sender`、session 和白名单 sink。完整的 control/data plane、header 契约和 frame 流程见
[naws README](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naws/README.md#websocket--kafka-集群推送与少拷贝)。

## 安全配置

TLS/SCRAM 使用 `nasa` 的 `kafka-tls` feature：

```toml
nasa = { version = "2.0.3", features = ["kafka-tls"] }
```

```yaml
kafka:
  bootstrap_servers: kafka-1:9192,kafka-2:9194,kafka-3:9196
  security:
    protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: ${KAFKA_USERNAME}
    sasl_password: ${KAFKA_PASSWORD}
    ssl_ca_location: /etc/credentials/kafka/ca.crt
```

每个 source 冻结同一认证域，producer、consumer、Admin 和所有 lane 不允许通过各自 raw properties
改写 principal。源级 `properties` 可以提供原生安全属性，强类型 security 字段优先。密码与 key material
在配置 Debug 和原生构造错误中保持脱敏，普通消费健康不传播 broker 自由文本中的认证原因。
Registry 与 checkpoint 数据库独立配置；Kafka 凭据不会自动传递给它们。

协议精确使用 `PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL`，机制精确使用
`PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/GSSAPI`。PLAIN 需可信传输，SASL_PLAINTEXT 不加密链路。
SSL 默认验证证书链和主机名；mTLS 通过 `ssl_certificate_location` 与 `ssl_key_location` 提供客户端 PEM
证书和私钥，`ssl_key_password` 用于加密私钥。Java 对应 truststore/keystore，不要求材料格式同名。
GSSAPI 还需要目标系统 Cyrus SASL/Kerberos 实现、principal、票据或 keytab、服务名以及正确的 DNS/时钟；
`gssapi` 同时启用 TLS。OAuth 与任意 Java callback 不属于共同认证能力。

`KafkaCapabilities::supported_capabilities()` 查询实际 librdkafka 构建与当前 crate feature 的交集，
不创建客户端或连接。`proxy.capabilities()?` 的 `supported`、`configured`、`lane_compression` 与普通组
`readiness` 分别表示本地实现、冻结启用项、通道算法和当前 Joined 要求的证据。
`napp::KafkaHandle::capabilities()` 提供相同只读快照。空 readiness 不表示 Ready；已配置 TLS 或已构造源
不能证明远端身份、权限或 group join，事务仍查询独立 TransactionHandle。

压缩精确使用 `none/gzip/snappy/lz4/zstd`，只通过 `producer.compression` 或 lane `compression` 声明；
raw compression 属性禁止绕过合同。全部声明 lane 在创建源前检查，包括尚未初始化的 lane。
没有 `tls` 时拒绝 SSL/SCRAM，没有 `zstd` 时拒绝 zstd，没有 `gssapi` 时拒绝 GSSAPI，
缺少真实原生实现同样返回 `UnsupportedCapability`，不静默回退。nasa 门面分别使用
`kafka-tls/kafka-zstd/kafka-gssapi`；Java 缺少可加载 codec 依赖时也在启动前拒绝。

明确的 TLS、SASL 与 topic/group 授权失败令普通组 Crashed，不自动重建错误身份；首个永久连接错误
由所属原生会话保留，后续断连通知不能覆盖。mTLS 对端拒绝客户端证书有时仅报告 BrokerTransportFailure，
此时不能据文本推定永久认证失败，采用有限重建预算且始终不 Ready。恢复 ACL 后显式重建组；
变更凭据或信任根须等待旧 source 实际关闭后创建新 source，再验证 Ready，不提供无损认证热替换。

| 使用角色 | Kafka 资源与最小操作 |
| --- | --- |
| 普通 producer | 目标 topic Write、Describe |
| 普通消费组 | 来源 topic Read、Describe，最终 group Read；查询组位点还涉及 Describe |
| 普通 DLT writer | DLT topic Write、Describe、DescribeConfigs 与来源消费权限；配置证据不足不发送、不推进来源 |
| 有限 Replay | 来源 topic Read、Describe；直接读取分区，不加入消费组、不提交组位点 |
| Transfer / DLT 回投 | 来源 topic Read、Describe；目标及隔离 topic Write、Describe；外部代次目录另行授权 |
| 只读诊断 | 对应 topic/group Describe；配置查询另需 DescribeConfigs，不授予写入和管理 |
| Topic 管理 | 创建 Create，删除 Delete，增分区 Alter，改配置 AlterConfigs，逐次由 broker 授权 |
| 同集群事务 | 输入 topic Read、输出/DLT topic Write、对应 Describe、group Read、派生 transactional.id Write；事务 DLT 另需 DescribeConfigs |

此权限合同对应 Kafka 4.1.0；其它 broker 或集群级诊断的权限按部署另行核对。Admin 句柄是显式管理能力，
仅需发布的业务模块应使用 producer lane。认证成功不代表具备所有权限，缺少 DescribeConfigs 不解释为
topic 不存在，普通 Ready 不自动建 topic、修改 ACL/配置或关闭主机名校验。

明确的 broker ACL 拒绝保留为 `NafkaError::Authorization`、`ReplayError::Authorization`、
`TransferError::Authorization`、`DltReplayError::Authorization` 或 `TransactionError::Authorization`。
转发的来源读取失败位于 `status.replay.failure`，目标失败位于外层 `failure`。拒绝的新工作不会推进 checkpoint 或输入提交，
已确认前缀仍可保留。恢复授权后等待旧任务实际退出，再以原计划和持久进度启动新任务；永久保护的 producer 需关闭旧 source 后重建。
撤销 ACL 不会撤回客户端已经取得的输入或取消在途副作用，权限边界以 broker 对具体请求的校验为准。

`alter_topic_config` 仅增量设置传入键，检查目标资源自身的错误码，不替换其它配置。
等待结束而没有确定确认时为 `AdminOutcomeUnknown`，再次修改前应回读真实配置；
broker 不支持该操作时为 `UnsupportedCapability`，不降级为整组配置替换。
共同运行范围为 Kafka 4.1.0 KRaft、rdkafka 0.39/librdkafka 2.12.1 与 Java client 3.8.1/JDK 21，
classic group 协议；本机平台为 macOS arm64，broker 为 Linux arm64。其它平台、broker 或新 group 协议
需要各自的运行证据，本地 GSSAPI 实现存在不能证明 Kerberos 登录成功。

## 有限回放计划

`proxy.admin().replay_observation(topics, timestamp_millis, timeout).await` 使用不加入消费组的专用客户端查询完整分区、真实 cluster ID、保留起点和 `read_committed` LSO。所有请求共用一个正预算，最大五分钟；不创建缺失 topic，不提交线上 offset。这里使用实时 ListOffsets，不能用缓存 high watermark 替代稳定末尾。`timestamp_millis=None` 不查询时间；缺少时间匹配保留为 `None`。

`ReplayPlan::freeze` 接受 `ReplayStart::Earliest`、完整逐分区 `Offsets` 或 `Timestamp`，把范围固定为 `[start,end)`。时间无匹配由 `MissingTimestamp::FreezeEnd` 或 `Reject` 决定，晚于本轮 LSO 的结果截断到冻结 end。最多 4096 个分区；后续写入与事务完成不会扩大原计划。`ReplayLimits` 独立声明条数/字节速率和实际在途容量，速率零表示不节流。身份采用有界可见 ASCII。

计划包含 task/source/cluster、`topic_generations`、`pipeline_fingerprint`、完整范围及独立预算。外部 topic 代次必须来自能识别删除重建的资源目录，不能用名称或分区数替代；当前安全客户端不暴露原生 topic UUID，调用方负责该目录与实际 Kafka 资源一致。处理合同摘要必须覆盖 schema、转换、过滤、目的路由和成功条件，语义变化必须更换摘要。

`encode`/`decode` 使用与 Java 相同的 `NSKRPL01` 完整计划格式及 SHA-256，最大 8 MiB；损坏、截断、非规范排序或额外尾部为 `CorruptPlan`。摘要检测损坏，不承担认证。`initial_checkpoint` 生成未接管声明，`validate_resume` 用新观测、重新查询的外部代次及原 checkpoint 拒绝错连、同名重建、分区变化、保留缺口、末尾回退和处理合同变化，不重新计算 earliest/latest。`TopicMissing` 与 `ObservationFailed` 区分确定缺失和本轮不可观测。

这些入口只负责规划与恢复证据，不执行 handler，也不授予任务执行权；执行需要显式调用下面的受管入口。压缩日志只能提供仍保留的记录，offset 空洞不能证明每次历史业务变更都可恢复。

## 受管有限回放

`proxy.replay(plan, store, identity, processor)` 返回 `ReplayHandle`，其中后三项分别为 `Arc<dyn CheckpointStore>`、`Arc<dyn ReplayIdentityProvider>`、`Arc<dyn ReplayProcessor>`。任务适合有限历史补数和索引重建，专属 consumer 使用 assign/read_committed，不加入或提交线上 group。每个分区串行，不同分区可并发；拥有型 `ReplayRecord` 保留二进制 key、tombstone、时间和有序重复 header，并携带物理来源坐标。

`ReplayProcessor::fingerprint` 必须与计划一致；同步 `process(record)` 返回成功，表示本条全部实际副作用已成功完成，不能派生未等待的后台工作。框架随后合并分区进度、条件持久化，再发布成功快照。处理失败或 panic 停止新准入；checkpoint 写入失败封禁后续写入，状态分别记录执行类别与存储类别。相同计划再次执行从最后确认位置恢复。目标已成功而 checkpoint 未确认时可能重复，目的端仍需稳定事件 ID 或来源坐标幂等处理。

执行前与每条处理准入后重新查询来源和外部资源代次，元数据及目录请求每轮共用十秒预算。目录扩展必须提供当前完整证据并遵守剩余预算；查询只发现可观测变化，不与目标副作用形成跨资源原子提交。提供 POSIX 本地文件后端与单主 MySQL 共享后端，各自的所有权及部署边界见持久进度说明。

保留期清理或显式日志截断使当前 beginning 越过 checkpoint 时返回 `DataGap`，保留最后确认进度，不跳到剩余日志继续发送。已经准入的实际处理仍可完成并持久推进；后续准入重新检查。压缩产生的数值空洞由原生消费位置跨越，范围内部或末尾没有存活记录也可完成，前提是来源身份、保留边界和已交付记录的成功条件均成立。Transfer 使用相同来源门禁，只搬运当前仍存活的记录；它不能补回已压缩的事件或已过期的 tombstone。

每来源最多十六个活动任务、六十四个分区 worker，每任务另有一个协调线程。实际并行数为 concurrency 与分区数的较小值，每个 worker 至多驻留一条记录；单条额度不超过源 `max_record_bytes` 和任务字节额度除以 worker 数的较小值。计费包括 key、正文、UTF-8 topic/header 和固定元数据，超额为 `Capacity`。全任务条数/字节速率在处理准入处共享，最多一条初始突发。原生 fetch 缓冲、压缩解压及业务自行复制独立于驻留计数，它不是进程内存硬上限。

`pause` 关闭新准入，`Pausing` 等待已接纳责任，`Paused` 可以保留尚未派发的驻留记录；`resume` 仅恢复未停止实例。`cancel` 永久停止本实例的新处理，不能撤回在途副作用。`status` 无 I/O 返回快照，`await_drained(timeout).await` 只等待真实退出，取消等待或超时不会归还同名任务及 lease。worker、consumer 和存储关闭全部真实完成后才发布 `Completed`、`Cancelled` 或 `Failed`。

`stop_consumers_until` 同时封闭回放准入，保留 producer/Admin；最终停机也先排干已接纳回放，再关闭其依赖。回放仍有实际责任时返回 `ReplayShutdownTimeout`，保留任务与独占权供后续等待。`napp::KafkaHandle::replay` 提供相同受管入口，Application 开始退出后拒绝新任务。

## 可恢复的单目标转发

每个 Transfer 或 DLT 回投任务最多持有一个只读目标探测 consumer，并在实际任务退出时销毁。只复用连接，每条发送仍重新查询目标物理身份、完整分区集合与范围；并发分区等待探测连接的时间计入同一操作期限。保留已结束的控制句柄不会保留该原生客户端。

`TransferPlan::freeze(source_ranges, target, routes, payload_policy, max_hops)` 保留有限 Replay 来源范围，将处理指纹替换为内置转发合同。`TransferTarget::observed` 固定目标逻辑源、物理 cluster ID、lane、topic 代次和完整分区集合；`TransferRoute` 逐一批准来源与目标分区。计划规范编码最大16 MiB，可与 Java 交换，旧 handler checkpoint 不自动成为转发进度。

`source.transfer(plan, &destination, store, source_identity, destination_identity, registries)` 返回 `TransferHandle`，两个 identity 分别是当前外部资源目录。目标 lane 必须为 `acks=all` 或 `-1`。先预登记目标依赖，再绑定来源任务并开放执行；任一端停止都关闭下一条准入，已经进入 native queue 的消息由同步 worker 等到实际回执终态。发布等待预算只约束入队前等待，入队后不因取消或业务等待超时释放来源 lease。每个目标最多十六项依赖，来源与 Replay 共用任务和 worker 容量。`pause/resume/cancel/status/await_drained` 保持 Replay 的实际责任语义；`TransferStatus.replay` 是持久进度，`delivered_records` 是本实例目标成功回执数量，后者可领先于前者，`failure` 保留目标独立失败类别。

`TransferPayloadPolicy::Opaque` 保留二进制 key、tombstone、空值、有序重复 header 和固定分区映射，业务承担原文在目的端的解释。每来源分区串行，多分区合流不产生全局顺序。目标使用 `LogAppendTime` 时存储时间可被 broker 改写，原始时间保存在 `x-nasa-transfer`。`TransferTrace::read` 读取原始 cluster/topic/generation/partition/offset、时间、稳定 `event_id` 和物理 topic 路径。事件 ID 不随任务名或目标 offset 改变；最大跳数1..16，总消息头最多256项、64 KiB，重复、大小写变体、损坏证据或回环拒绝。完整性摘要不认证写入者，信任仍依赖 Kafka 写权限和资源目录，消息头不能决定连接地址。

启用 `schema-registry` 后，可用 `TransferPayloadPolicy::confluent` 固定两个 Registry 名和每个来源 ID 的批准目标 ID、schema 类型及正文 SHA-256，并通过 `TransferRegistries` 绑定实际客户端。两端 ID、类型和 UTF-8 schema 正文摘要全部符合才替换四字节 ID，其余 envelope（含 Protobuf message-indexes）保持不变；tombstone 不查询 Registry。转发不自动注册、演进或重新编码 schema。未启用 feature 的构建拒绝 Confluent 转发。客户端由宿主持有，关闭必须晚于实际任务退出；`Opaque` 不自动解释或迁移 Registry ID。

schema 与每条目标身份复验共用十秒预算。错连、topic 代次或完整分区集合变化在可观测时拒绝，目标日志增长不改变合同；查询与发送不形成原子操作，需要禁止变更窗口时由运维控制面协调资源变更。真实目标确认先于来源持久 checkpoint，后者未知或崩溃可重复，目标按稳定事件 ID 或来源坐标去重。原生交付超时或连接失败可返回 `OutcomeUnknown`，结束本地责任不证明远端没有写入，进度不会推进。当前仅提供单目标 at-least-once；同集群也不会自动启用事务，不迁移 group 位点、ACL、topic 配置或提供自动灾备切换。共享进度可采用下述 MySQL 后端；DLT 重放和多目标交付账本不属于此入口。

`napp::KafkaHandle::transfer` 具有相同参数与成功合同，目标通过另一个 `KafkaHandle` 明确传入；Application 退出后拒绝新任务，实际转发由双方 Kafka 组件排干。

## 有限 DLT 回投与原文隔离

`source.replay_dlt(plan, &destination, store, source_identity, destination_identity, registries)` 返回 `DltReplayHandle`，支持 `pause/resume/cancel/status/await_drained`；`napp::KafkaHandle::replay_dlt` 提供相同受管入口。计划复用冻结的 `ReplayPlan`、独占 checkpoint 和实际任务生命周期；不同主机可使用 MySQL 后端接续，不修改线上消费组位点。来源与目标停机都关闭下一条准入，并等待已经接纳的目标实际结果；超时保留任务及 lease。回投 lane 必须为 `acks=all` 或 `-1`，确认范围由 broker 的 ISR 和 `min.insync.replicas` 决定，不等于所有配置副本始终在线；目标资源与 schema 复验沿用单目标 Transfer 合同。

`DltReplayPlan` 将原业务 topic/partition 映射、原消费组集合、目标物理身份及完整分区、schema 策略、无效输入策略和最大回投次数全部绑定到持久指纹。路由依据必须是计划明确批准的映射，消息头不能决定连接或任意目标。最多4096条路由、256个原组；空组集合表示全部组。计划中的读取分区属于实际DLT，路由来源分区属于其报告的原业务，两者无需相同。只有证据完整且未选中的输入才计为 skipped 并推进。

`DltEvidence` 严格读取六个规范DLT字段：原topic、partition、offset、group、reason和DLT delivery attempts。缺失部分字段、重复、ASCII大小写别名、null、非法UTF-8、非规范数字均拒绝。DLT attempts范围1..4294967295，当前 `X-Nasa-Delivery-Attempt` 可因再次消费而不同；不要求与原DLT次数相等。原group为1..1024字节UTF-8，reason可空且最多1024字节。完整消息头最多256项、64KiB，超额不会静默裁剪。

可选 `DltOrigin` 将 `X-Nasa-DLT-Origin-Source` 与 `X-Nasa-DLT-Origin-Cluster` 作为完整一对读取，各为1至1024个UTF-8字节且不含NUL；缺少一项、别名、重复或非法文本拒绝。新普通/事务DLT都写入实际 `client_name` 和broker物理集群身份，旧六字段记录的 `origin` 为 `None`。结构体字面量需显式提供 `origin: None` 或实际身份。源别名不等于物理身份，集群/topic/partition/offset仍不能区分同名topic删除重建后的日志代次，字段本身也不认证写入者。

回投证据无此扩展时保持 `NSKDRH01` 原字节，有扩展时使用 `NSKDRH02` 并保存首次来源身份；再次失败与回投不覆盖这份首次证据。滚动升级时应先升级回投证据读取端，旧读取端无法解释 `NSKDRH02`。Java 的旧六参数构造方法和已有字段访问方法继续可用。

普通DLT每次发送前查询来源物理集群和目标有效配置，事务 `dead_letter` 在本批任何输出发送前验证全部死信目标。目标必须预先创建，`cleanup.policy` 必须仅为 `delete`，`retention.ms` 和 `retention.bytes` 必须为正或 `-1`；拒绝 `compact`、`compact,delete`、立即删除、缺失或不可读取的配置。所用身份需要目标的 Describe/DescribeConfigs 权限。查询失败不会被解释为允许自动建topic，默认保留输入等待重投；显式可用性优先策略仍允许丢弃。查询和真实交付属于有界DLT责任，关闭等待实际完成，不能因异步等待取消就释放源位点责任。

该门禁是发送前配置快照，不能阻止管理员随后更改策略，也不保证无限历史。时间保留按broker的实际时间戳策略计算，保留原始时间的旧输入可能已接近清理期限；容量保留也会淘汰旧segment。运维应按排障和回投窗口配置保留时间/容量并控制变更权限。框架保留原key、null payload、零长字节与业务头，既不把tombstone包装为其它载荷，也不改写目标配置。

默认无效输入停止并保留失败坐标。显式 `DltInvalidPolicy::Quarantine(Tp)` 将缺少证据、证据非法或达到回投上限的原文发送到固定隔离分区；该分区不得与任何业务回投目标分区相同。隔离保留原key、payload、时间和全部原始错误头，追加 `x-nasa-dlt-replay-failure`；`DltQuarantineTrace` 提供实际DLT坐标、事件ID和拒绝原因。已含隔离标记或追加后超出头预算时停止，避免覆盖历史证据。schema不匹配、目录不可观测、目标身份变化、发送失败或结果未知不会转为隔离成功；只有隔离真实确认才推进checkpoint。

正常回投保留原文、tombstone、空值、有序重复业务头和DLT来源字段，并写入 `x-nasa-dlt-replay`。`DltReplayTrace` 保存首次实际DLT物理资源和坐标、原DLT报告、最近批准目标和回投次数。首次ID来自实际DLT坐标；若存在合法Transfer来源，保留其原事件ID，将旧Transfer证据完整封装并移除活动 `x-nasa-transfer` 头。再次业务失败不会清空已有回投ID或计数，次数上限为1..16。允许显式回投原业务topic，但禁止向当前同集群被读取的DLT topic直接输出。普通Transfer的回环保护不因此改变。

这些摘要只检测格式与内容变化，不认证写入者；过滤、原始业务来源与计数依赖受控DLT写权限和可信资源目录。旧DLT不含原业务topic物理代次，框架不会伪造该身份。回投得到新的Kafka offset，不恢复旧日志位置。目标已确认而checkpoint未确认时允许可识别重复，目的端仍需幂等处理。`DltReplayStatus` 的 forwarded/skipped/quarantined 计数只属于本执行实例，持久进度仍以其 replay 字段为准。

## 持久任务进度

`ReplayCheckpoint`、`CheckpointStore` 和 `CheckpointLease` 提供独立的恢复进度合同。快照保存稳定任务名、合同 SHA-256、所有权 `generation`、条件更新 `revision` 和最多 4096 个完整分区范围。范围采用 `[start_offset, end_offset)`，`next_offset` 不允许回退或越界，完成不要求实际存在 `end_offset-1` 的记录。指纹必须绑定物理集群、topic 代次、分区、schema/转换策略、目标和结束边界，不能只摘要 topic 名。

Unix 可使用 `FileCheckpointStore::new(path)`，其它存储实现 `CheckpointStore`。`claim` 接受序号为零、所有进度在起点的冻结声明，在独占权成立并落盘新代次后返回 lease；已有合同不同为 `ContractMismatch`，活动任务为 `Busy`。`advance(expected_revision, positions)` 复验磁盘证据和条件序号，成功落盘后才更新快照。旧序号为 `StaleRevision`，证据变化为 `Fenced`；写入失败按 `OutcomeUnknown` 处理，当前 lease 随后拒绝推进。

`CheckpointLease::verify()` 在不增加 revision 的情况下复验当前所有权和完整持久证据，失败后禁止该 lease 继续推进。自定义后端必须实现此门禁，受管 Replay/Transfer 在每次业务调用前调用它。检查与远端副作用不是原子操作，已在途调用仍可能重复；`checkpoint()` 只返回历史确认快照，不授予当前执行权。

文件后端使用与 Java 相同的 `NSKCP001` 大端格式和 SHA-256 完整性校验，固定锁文件使用 POSIX 记录锁并辅以进程内独占登记。完整正文同步后才原子替换文件并同步父目录；进程崩溃由 OS 释放锁。目录和锁文件必须由组件独占管理，不允许外部删除、重建或另行打开锁文件。摘要检测损坏，不是文件认证；此后端仅承诺本地单机恢复，不承诺 NFS 或跨主机接管。共享后端必须提供原子 claim/CAS 和成功回执对应的真实持久化。

存储调用可能等待磁盘或 SQL，应在独立有界执行资源内调用；它们不读取 Kafka，不执行 handler 或转发，也不判定目标成功。必须先确认目的端成功，再推进持久 checkpoint，之后公开业务进度。关闭或丢弃 lease 前必须证明在途业务已退出；目标已成功但 checkpoint 未确认的崩溃允许可识别重复，不能据此承诺跨资源原子提交。

### MySQL 共享 checkpoint

门面 feature `kafka-mysql-checkpoint` 提供 `nasa::kafka::MySqlCheckpointStore`，实现 feature 为 `mysql-checkpoint`；普通 `kafka` 不引入 SQLx。配合 `application`，可将该 store 传给 `KafkaHandle::replay` / `transfer`，连接责任随实际任务排干。它与 Java 共用表、锁名及正文，允许跨语言、跨主机互斥接管。

```rust,ignore
use nasa::kafka::MySqlCheckpointStore;
use std::time::Duration;
let store = MySqlCheckpointStore::new(
    &checkpoint_url, "orders", Duration::from_secs(5), 64,
)?;
store.initialize()?;
```

URL 为单端点 `mysql://user:password@host:3306/database`，凭据须按 URL 规则转义并从受控配置读取；不要记录 URL。数据库预先创建，名称为 1–64 个 ASCII 字母数字、下划线、美元符号或横线。仅接受 `ssl-mode`、`ssl-ca`、`ssl-cert`、`ssl-key` 查询项；重复及未知项拒绝。TLS 默认 `VERIFY_IDENTITY`，只另行接受显式 `DISABLED`，CA 和 mTLS 文件由后面三个参数指定。普通 Kafka broker 的 TLS 配置与这个 SQL 连接独立。

固定 InnoDB 表 `nasa_kafka_checkpoint` 使用二进制主键 `(namespace, task_id)`、有符号 BIGINT 序号及最大 2 MiB 的 `NSKCP001` 正文。namespace 与 task ID 均遵循 1–128 个 ASCII 字母数字、点、横线、下划线且首字符为字母数字的规则；接管者必须使用相同数据库名、表及 namespace。共同 SHA-256 锁名包含各身份的长度前缀。每份 lease 持有专用物理会话与 `GET_LOCK` 命名锁；竞争立即返回 `Busy`。首次 generation=1/revision=0，接续时两者加一并保留进度。CAS 同时比较会话、锁所有者、完整旧正文和原序号，新进度只在一行写入确认后发布。

仅支持 MySQL 8 或更高版本的单一可写主节点、自动提交和 `innodb_flush_log_at_trx_commit=1`；binlog 开启时要求 `sync_binlog=1`、会话 `binlog_format=ROW`。显式 `initialize()` 创建并验证表，普通 load/claim 不运行 DDL。错误结构、额外索引和可见触发器拒绝；表须专供此协议，运维控制直接改写、DDL 与触发器权限，允许运行账号读取结构证据。初始化另需 CREATE 权限，日常任务需要 SELECT/INSERT/UPDATE 和命名锁权限；活动任务期间不能改变表结构或持久设置。

容量范围 1–1024，同时覆盖建连、初始化、读取和 lease；额度不足立即返回 `Capacity`。每个实际会话由一个专用线程及私有 Tokio 运行时持有，串行命令通道容量为一；同步 API 不嵌套业务 Tokio 运行时。单次 SQL 序列预算为 100ms–60s，关闭使用独立同长预算，失败清理可能增加调用耗时。等待取消不释放仍在退出的线程或连接额度；本地资源真正退出后才释放容量。网络断开后服务端可能仍持有命名锁，后继只能等待原会话结束，不能绕过 `Busy`。

会话中断、超时或确认丢失永久封闭旧 lease；写入未知为 `OutcomeUnknown`，新 claim 重读真实持久状态，不自动重连或按旧快照重试。`verify()` 与 Kafka 副作用不构成原子区间，已在途写入仍可重复。命名锁只作用于单个 MySQL 服务；外部故障切换必须先保证旧主失去写权限，不提供多主共识、自动防脑裂或跨资源 exactly-once。

## Schema Registry

`schema-registry` feature 提供 Confluent-compatible 的 registry client 与 wire envelope
（`nasa` 门面对应 `kafka-schema-registry`）。它是 Kafka 的 codec 子能力：不启动 registry 服务端，
独立 client 由业务显式构造并持有；配合 `application` 时，Application 在 Prepare 按
`schema_registries.<name>` 装配命名 client，无需声明 Kafka 消费组件或新增组件字符串。
`kafka-schema-registry` 自带 `secret`，因此 `SchemaRegistryAuth` 的 token 与密码类型无需业务再补 feature。

生产默认禁止自动注册。数据面只按已批准的 schema ID 拉取，命中结果进有界正缓存，确认不存在的 ID
进有界负缓存，两者各有独立 TTL 与统一条目上限；`auto_register` 必须显式开启才允许写入新修订。

查询只交付没有引用或 `references: []` 的定义；非空引用、非法引用字段返回 `InvalidResponse`，不隐式下载依赖。subject 命名、兼容级别、Protobuf message-indexes、schema GUID 与引用解析不属于四字节 ID envelope 的自动处理范围。

数据面冷缓存查询按 schema ID singleflight：同一 ID 的并发未命中只放行一个 leader 访问
Registry，其余调用等待该轮结束后重查缓存——请求成功即全体命中，请求失败后等待者逐个
串行接替。取消某个等待不结束客户端持有的请求轮次，也不释放实际 HTTP 额度。冷启动与 Registry 故障期间对 Registry 的请求量因此以"每 ID 同时至多一个"为上界，
不随解码并发放大；等待者的查询结局按其真实路径记账(经缓存命中的记 `cache_hit`)。

### 运行架构与安全合同

Schema Registry 不参与 Kafka consumer group、offset 或 producer 所有权。推荐的数据面路径是先用业务
发布的 `ApprovedSchemaIds` 校验 Confluent envelope，再按其中的正 schema ID 查询 client：

```text
Kafka record bytes
  -> magic byte / payload 上限 / schema ID 白名单
  -> 正缓存或 404 负缓存
  -> GET /schemas/ids/{id}（仅缓存未命中）
  -> 有界 JSON 读取
  -> RegisteredSchema + 借用原 record 的 payload
```

兼容性检查与注册是独立控制面，不会混入数据面缓存命中率。兼容性结论 `false` 是合法业务结果；注册
还必须通过 `auto_register` 本地授权门禁。adapter 不决定 subject 命名、兼容级别、schema 演进或
部署审批，业务应先完成这些治理，再把批准后的 ID 发布到数据面白名单。

endpoint 只接受 HTTPS 或 loopback HTTP，拒绝 userinfo、query、fragment、重定向和不能作为 base URL
的地址。HTTP body 同时受 `Content-Length` 和流式累计上限约束；认证信息、endpoint、subject 与 schema
正文不会进入公开错误。Bearer token 和 Basic password 使用 `SecretBytes`，不应写入 yml。

`ConfluentSchemaRegistry::new_with_tls(options, SchemaRegistryTls { ca_pem, identity_pem })`
接受独立 PEM 材料，两个字段均为可选。`ca_pem: Option<Vec<u8>>` 声明时替代 HTTP 客户端默认根，
未声明时使用 rustls 默认公开根；`identity_pem: Option<SecretBytes>` 包含客户端证书链和一个未加密
PKCS#8 私钥。每份最多 1 MiB、证书最多十六个，非法或不匹配身份在连接前拒绝。
TLS 入口只接受 HTTPS，最低 TLS 1.2，始终验证证书链和主机名；不读取 Kafka SASL/SSL 配置。
TLS 失败是脱敏 `Transport`，401/403 是 `RemoteStatus`，这些失败均不写负缓存。

### 配置与使用

独立模式通过 `ConfluentRegistryOptions` 显式构造 client，由业务持有生命周期：

```rust
use std::sync::Arc;
use std::time::Duration;

use nasa::kafka::{
    ApprovedSchemaIds, ConfluentEnvelope, ConfluentRegistryOptions,
    ConfluentSchemaRegistry, SchemaId, SchemaRegistryAuth, SchemaRegistryClient,
};
use nasa::secret::SecretBytes;

let mut options = ConfluentRegistryOptions::new("https://registry.example.com");
options.auth = Some(SchemaRegistryAuth::Bearer(SecretBytes::new(registry_token)));
options.request_timeout = Duration::from_secs(3);
options.cache_capacity = 256;
options.cache_ttl = Duration::from_secs(300);
options.negative_cache_ttl = Duration::from_secs(5);

let registry = Arc::new(ConfluentSchemaRegistry::new(options)?);
let approved_id = SchemaId::new(17)?;
let mut approved = ApprovedSchemaIds::new();
approved.insert(approved_id);

let envelope = ConfluentEnvelope::decode(record_payload, 4 * 1024 * 1024, &approved)?;
let schema = registry.schema_by_id(envelope.schema_id).await?;
```

| `ConfluentRegistryOptions` 字段 | 默认值 | 约束与用途 |
| --- | --- | --- |
| `auth` | 无 | `Bearer` 或 Basic；credential 由 `SecretBytes` 承载 |
| `request_timeout` | 3 秒 | 大于 0，最长一年；覆盖单次 HTTP 总时长 |
| `max_response_bytes` | 1 MiB | 大于 0，框架硬上限 16 MiB；也约束待提交 schema 文本 |
| `cache_capacity` | 256 | 大于 0；正负缓存共享 LRU 容量，同时约束不同冷查询 ID 与实际 HTTP 并发数 |
| `max_pending_lookups` | 4096 | 1–65536；包含同 ID 等待者的 schema 查询调用上限，超额返回 `Capacity` |
| `cache_ttl` | 300 秒 | 成功 schema 的本地有效期 |
| `negative_cache_ttl` | 5 秒 | Registry 明确 404 的短期负缓存；传输失败不会写入 |
| `auto_register` | `false` | 只有显式授权的部署或管理路径才能注册新修订 |

Application 受管模式开启 `application,kafka-schema-registry`，采用命名配置：

```yaml
schema_registries:
  primary:
    enabled: true
    endpoint: https://registry.example.com
    bearer: secret://registry_token
    tls:
      ca: secret://registry_ca
      identity: secret://registry_identity
    request_timeout_ms: 3000
    max_response_bytes: 1048576
    cache_capacity: 256
    max_pending_lookups: 4096
    auto_register: false
```

`bearer` 引用 `secrets` 中的名字；Basic 认证使用 `username` 和 `password: secret://...`，不能与
Bearer 同时配置。可选 `tls.ca` 与 `tls.identity` 同样只接受 `secret://` PEM 材料引用；不需要私有信任锚或 mTLS 时省略相应字段或整个 `tls`。Prepare 从同代视图绑定材料并登记资源，Service 在后续 initializer 或 Ready 后任务中
使用 `app.schema_registry("primary").await`；Batch 在工作负载前完成装配，无需启动 Web 或 Kafka 消费者。
未声明或禁用的计划不解析其独占凭据、不访问远端。受管计划最多 64 个，请求超时上限 300 秒、缓存容量
上限 65536；上述 options 的一年超时上限仅适用于独立构造。配置与凭据变化报告 `RestartRequired`。
构造不查询 Registry、不注册 schema，也不将远端视为 Ready；停机关闭新调用，通知并等待客户端持有的 HTTP 任务与业务等待退出，旧句柄返回 `Closed`。

独立客户端用 `close()` 永久关闭准入并取消本地读取，`await_idle(deadline)` 等待实际责任归还；
返回 `false` 仅表示预算耗尽。`in_flight()` 观测实际请求数，不含共享查询的等待者。
`pending_lookups()` 观测所有等待中的 schema 查询，取消 future 后归还这部分额度；HTTP 额度仍由实际请求持有。
取消业务 future 不取消已经接纳的 HTTP 请求，直到响应完成、请求总预算耗尽或显式关闭才归还额度。
取消的注册不会自动重试，也不能推定远端没有创建修订；`close()` 同样不撤回远端副作用。

### 观测

`metrics_snapshot()` 一次读取即得到全部当前事实，读取不清零。查询结局分六类分别记账，
覆盖 `schema_by_id` 的全部返回路径：

| family | 类型 | label | 含义 |
| --- | --- | --- | --- |
| `nafka_schema_lookups_total` | counter | `outcome` | 按缓存与拉取结局分类的累计查询次数 |
| `nafka_schema_cache_entries` | gauge | 无 | 正负缓存合计占用的条目数 |
| `nafka_schema_cache_capacity` | gauge | 无 | 已登记 client 的缓存配置条目上限总和 |
| `nafka_schema_control_requests_total` | counter | `operation`、`outcome` | 兼容性检查与注册的累计请求数 |

`outcome` 取 `cache_hit`（正缓存命中）、`negative_cache_hit`（负缓存命中）、`fetched`（拉取成功）、
`fetched_missing`（registry 确认不存在）、`fetch_failed`（传输、状态码或应答格式失败）、
`cancelled`（future 在返回结局前被业务超时、客户端断连或任务取消丢弃）。取消时远端是否收到或完成
请求未知，但这次已经开始的查询仍计入调用总数。
schema ID、subject 与远端状态码**不进 label**。

分母非零时，容量判断使用完成查询的正命中率 `cache_hit / (cache_hit + fetched)`；`cancelled` 没有查询结论，
`fetch_failed` 没有 schema 结果，负缓存两类表示不存在的 ID，均不进入该比率。正命中率偏低且
`cache_entries` 接近 `cache_capacity` 时，说明数据面正在受容量驱逐影响；取消应通过
`outcome="cancelled"` 单独观察。负命中率异常升高说明上游在按不存在的 ID 解码；
`fetch_failed` 上升而缓存占用不变说明失败没有污染缓存——一次网络抖动不会把 schema 永久标记为不存在。
`cache_entries` 贴近 `cache_capacity` 时，驱逐来自容量而非 TTL。

控制面 `operation` 取 `compatibility`/`register`，`outcome` 取 `success`/`rejected`/`transport`/
`remote_status`/`invalid_response`/`cancelled`。兼容性结论为 false 仍是成功应答；`rejected` 表示
subject、容量或 `auto_register` 门禁在本地拒绝；`cancelled` 表示调用已经开始但没有返回业务结局。
数据面查询与控制面请求分族，注册流量不会稀释缓存命中率。

独立 client 与 `nasa` 的 `application` 组合时，在 UserHook 接入统一指标目录，之后 Prometheus 文本端点与 OTLP 指标导出
共用同一份快照：

```rust
let registry = std::sync::Arc::new(nasa::kafka::ConfluentSchemaRegistry::new(options)?);
app.register_metrics_source(nasa::kafka::schema_metrics::metrics_source(registry.clone()))?;
```

同一进程连接多个 Registry 集群时，每个 family 仍只能登记一个 owner。使用聚合源按封闭操作与结局
求和，并把各 client 的缓存占用和容量相加；endpoint 与 subject 不进入 label：

```rust
app.register_metrics_source(nasa::kafka::schema_metrics::metrics_source_many([
    primary_registry.clone(),
    archive_registry.clone(),
]))?;
```

`schema_registries` 标准受管路径已自动登记一个聚合指标源，业务无需且不应为这些实例重复登记。

### 能力边界

- `kafka-schema-registry` 保持显式 feature 并进入 `full`；独立模式开放 client/codec 合同，
  配合 `application` 后由显式启用的 `schema_registries` 计划建立受管资源，无需额外组件字符串。
- 稳定公开合同覆盖 Confluent 5 字节 envelope、Avro/Protobuf/JSON Schema 类型、按 ID 查询、
  兼容性检查、显式注册、有界正负缓存和低基数观测；不提供 codec 代码生成或 payload 语义校验。
- client 不启动后台刷新，不主动扫描 subject/version，也不把 Registry 健康伪装成 Kafka broker Ready。
- 同一 client 内相同 ID 的冷查询合并请求；缓存、不同在途 ID 与 HTTP 并发均受 `cache_capacity` 约束，超限返回 `Capacity`。不同 client 或进程不共享缓存与请求合并权威。
- schema 删除、引用解析、兼容级别修改、subject ACL、跨 Registry 复制和灾备切换不在本合同内。
- 调用 future 取消后远端结果未知；注册重试必须依赖 Registry 的 schema 去重语义与业务发布流程，
  不能把本地取消解释为远端未执行。

## 健康、指标与管理面

- `KafkaProxy::group_health(group).await`：group 状态、assignment、pause 原因、commit 与重启累计值。
- `KafkaProxy::producer_lane(name)?.stats()`：物理 lane 的 in-flight、queue、delivery 与 generation 状态。
- `connect_with_metrics()`：注入 `MetricsSink`，通过每源独立的有界线程对接应用监控出口；`connect_with_metrics_options()` 可指定队列和事件字节上限。
- `admin()`：topic 创建、删除、描述、配置与 metadata 操作；业务 API 仍不暴露 rdkafka 类型。
- `admin().read_only()`：将当前视图永久收窄为只读，克隆和附加 access scope 不会恢复写权限；修改操作在建连前返回 `Authorization`。`is_read_only()` 仅描述本地视图限制，不证明 broker 已授予 ACL。资源所有者的其它管理视图不受该收窄影响。
- `admin()` 与 `admin().until_shutdown()`：管理句柄及克隆均绑定到所属源最终关闭边界，关闭后返回 `Lifecycle`，回放观测返回 `Closed`；只停止消费者及退出收尾阶段仍可用。最终关闭会等待已接纳的管理操作归还资源和原生线程实际退出；期限耗尽返回 `AdminShutdownTimeout`，重复关闭继续等待同一责任，取消等待不会取消远端修改。需要继续运维时使用独立且仍开放的 source。

`KafkaAccessScope` 为宿主导出的 `KafkaAdmin`、`ProducerLane` 提供额外单向准入域。通过 `with_access_scope(&scope)` 绑定，`scope.close()` 后所有继承该域的克隆和未发送 builder 拒绝新操作；再次绑定其它开放域不能解除已有边界。关闭域不取消已准入请求、不关闭底层客户端，也不证明业务完成。`KafkaProxy::is_closed()` 仅在资源全部退出后为真。napp 自动为导出句柄绑定此域，最终清理失败或被放弃也会封闭准入；宿主结束后的剩余责任可通过 `KafkaHandle::await_closed(deadline)` 继续等待。

`request_consumer_stop()` 同步冻结消费登记，向所有槽与有限任务发布不可撤销的停止意图；它不等待线程退出，也不释放租约或原文。`stop_consumers_until` 和 `shutdown_until` 在计算剩余等待时间前执行同一门禁，期限已过仍会撤销新派发与迟到确认。普通单组控制命令的排队取消合同保持独立。napp 放弃最终清理 action 时也发布这一停止意图；实际退出必须继续等待，不能用通知成功替代资源关闭。
- 控制面支持 seek、beginning/end/committed/timestamp、pause/resume、subscribe/unsubscribe 和显式 restart。

`create_if_absent_result(TopicSpec)` 返回 `TopicCreation::Created` 或 `AlreadyExists`；旧 `create_if_absent` 对两者都返回成功。已存在不会重新应用初始配置，也不表示规格相符。创建、删除、扩分区和配置修改检查唯一目标资源的确认；权限拒绝为 `Authorization`，提交后的超时、缺失或错配响应为 `AdminOutcomeUnknown`。取消等待不能撤销 broker 已接纳的操作，应回读实际资源再决定后续动作。删除不存在的 topic 会明确失败，扩分区是否增加由 broker 在同一请求中裁决。

`alter_topic_config` 增量设置单项；`alter_topic_configs(topic, BTreeMap<String, Option<String>>)` 可一次设置多项，`None` 恢复 broker 默认值，未声明项不变。不支持增量修改的 broker 返回 `UnsupportedCapability`，不会退回整组替换。`describe_topic(topic, false)` 只请求元数据，不需要 DescribeConfigs；`topic_exists` 仅将明确不存在转为 `false`，权限拒绝与不完整结果继续报错。组位点无历史为 `None`，查询拒绝或缺项不能转为空。

描述加配置、逐分区水位以及 `committed_lag` 中的组位点加水位共享一次 `admin.default_api_timeout_ms` 预算，包括阻塞任务排队和 coordinator 重试。lag 是分次观测，不是同一时刻的原子快照；水位不能代替事务 `read_committed` 的可读边界。管理关闭成功意味着原生线程已退出，单独丢弃调用 future 不代表资源退出。

`observe_offsets(group, tps, isolation, timestamp_millis, timeout)` 为最多 10000 个明确且唯一的分区返回 `OffsetObservation`。`OffsetIsolation::ReadCommitted` 的末尾为实际 LSO，`ReadUncommitted` 为普通日志末尾；未完成事务可能使两者不同。报告带 source、可选 group、读取隔离、起止时刻以及每个字段各自的 `SystemTime`，不承诺跨分区或跨字段原子快照。

每个保留起点、末尾、提交位置、时间定位和 lag 都用 `ObservedOffset` 区分有效非负值与 `OffsetMissing`：`NotRequested`、`NoCommittedOffset`、`NoTimestampMatch`、`ResourceMissing`、`Authorization`、`Timeout`、`Interrupted`、`Unavailable`、`Inconsistent`。`Interrupted` 对应共同合同中的同步等待中断；Rust future 被取消时没有返回报告。未请求 group 时不计算 lag；lag 为 `max(0, end - committed)` 的 offset 距离，包含日志空洞、事务控制记录和可能已过期的前缀，不等于业务消息数或本地未完成数。有效零不会充当缺失。

所有阶段共用正的 `timeout`，上限为 `i32::MAX` 毫秒；期限耗尽保留已取得的部分证据，返回后晚到结果不改写报告。丢弃 future 撤销后继查询，已开始的原生查询及析构仍由 source 关闭追踪。查询不加入消费组、不提交、不创建 topic 或更改配置。`napp::KafkaHandle::observe_offsets` 提供同样的受管只读入口。

`verify_topic(TopicSpec, timeout)` 只读核对分区集合、副本数和配置子集；空配置不要求 DescribeConfigs。匹配时返回实际描述，漂移为 `TopicContract`，权限及查询失败保留原分类。核验不会创建或改写 topic，返回后资源仍可变化，需要部署权限和运行期保护共同约束。

指标回调不在 owner、producer 交付或 DLT 线程执行。默认最多排队 256 个事件，单事件指标名和全部标签
最多 4096 UTF-8 字节；另有至多一个活动回调。固定 `source` 标签由框架加入，调用点最多携带七个其它标签。
队列满、准入锁竞争或事件过大时直接丢弃观测，不阻塞业务且不截断身份。counter 因此是尽力而为的
运行观测，不适合计费或替代业务账本；gauge 由后续采样更新。

`metrics_export_stats()` 独立返回 queued/active/exported/dropped/failed 和 worker_exited。
用户回调 panic 只增加 failed；exported 只表示回调正常返回，不证明监控服务已经持久保存。
关闭先收口 Kafka 资源，再关闭观测准入并丢弃排队事件；活动用户回调不能强制取消。
其未在共同 deadline 内返回时，关闭报告 `MetricsShutdownTimeout` 并保持 ShuttingDown；后端返回后可再次
`shutdown()` 收口。未安装指标后端的 `connect()` 不创建导出线程。`napp::KafkaHandle.metrics_export_stats()`
遵守容器 Ready/Draining 准入，监控后端自身故障不会改变 Kafka ACK、DLT 或提交结果。

所有指标包含 `source`；组指标再带 `group`，lane 压力带 `lane`，交付计数带 `lane,topic`。
`napp` 默认适配器保留完整标签，缺失、重复、额外标签及错误类型的样本拒绝记录，避免同名组跨源合并。
counter 在对应后端实例生命周期内累加；复用后端和相同源名时不会自动清零。源直接统计与导出 counter
可能因观测丢弃而不同，排障应结合 `metrics_export_stats()` 与 lane/组健康。

| 指标 | 类型与单位 | 额外标签与含义 |
| --- | --- | --- |
| `publish_admitted_total` | counter，消息 | lane/topic；native 已入队 |
| `published_total` / `publish_failed_total` | counter，消息 | lane/topic；真实 native 回执成功或未确认成功，fire 入队不增加成功数 |
| `publish_admission_rejected_total` | counter，调用 | lane/reason；observer_capacity、message_capacity 或 byte_capacity 表示框架额度拒绝，原生入队拒绝另见 lane stats |
| `publish_wait_unknown_total` | counter，调用 | lane；框架等待超时或观察结果不可取得；外部直接丢弃 future 不在此计数 |
| `producer_lane_in_flight` / `producer_lane_queue_utilization` | gauge，条 / 百分比 | lane；实际 active / 已占 observer 除以观察容量 |
| `group_ready` / `group_ready_wait_millis` | gauge，0或1 / 毫秒 | group；全槽 Running 且有分配代次 / 最近成功等待耗时 |
| `group_ready_total` / `group_ready_timeout_total` | counter，调用 | group；等待 API 成功 / 超时，非状态迁移次数 |
| `assigned_partitions` / `paused_partitions` | gauge，分区 | group；所有槽汇总，暂停分区按唯一坐标计数 |
| `active_handlers` / `key_active_handlers` | gauge，调用 | group；实际未退出解码和业务 / 其中按键业务，不包含 DLT |
| `key_pending_records` / `key_pending_bytes` | gauge，条 / 字节 | group；按键排队及完成证据，不等同 broker lag |
| `retained_records` / `retained_bytes` | gauge，条 / 字节 | group；实际输入责任，取入至确认提交或实际交回，数量只计一次，字节包含八倍副本预留 |
| `owner_session_started_total` / `owner_session_restarted_total` | counter，会话 | group；原生会话开始 / 首次后的开始 |
| `commit_attempted_total` / `commit_succeeded_total` / `commit_failed_total` | counter，调用 | group；原生提交尝试 / 确认 / 未确认 |
| `consumed_total` / `manual_acked_total` / `manual_ack_missing_total` / `manual_ack_after_gap_total` | counter，条 | group/consumer_id；typed 分区窗口的有效前缀 / Manual 前缀 / 缺口后缀 / 缺口后已 Ack；不表示 broker 已提交 |
| `handler_dead_letter_total` / `handler_deferred_total` / `handler_failed_total` | counter，调用 | group/consumer_id；typed 分区窗口的业务裁决 |
| `decode_failed_total` | counter，条次 | group/consumer_id；owner 按序接管的 typed 解码失败，预取后未被接管的尾部不计数 |
| `retried_total` | counter，条次 | group/topic；typed 重试 run 的记录数，含延期 |
| `dlt_queue_jobs` / `dlt_queue_bytes` | gauge，记录 / 字节 | 无；源级 dispatcher 已准入保留量 |
| `dlt_total` / `dlt_failed_total` | counter，来源记录 | group/topic；有效 DLT completion 成功 / 失败，不计已失权的旧结果 |
| `dlt_attempt_failed_total` | counter，来源记录 × 尝试 | group/topic；实际查询或交付失败；可靠模式继续保留输入时也计数，停止后的准入取消不计数 |
| `passthrough_calls_total` / `passthrough_duration_micros_total` | counter，调用 / 微秒 | group；实际退出的借用回调，包括错误返回 |
| `registry_consumers` / `registry_groups` | gauge，路由 / 组 | client；源注册表规模 |
| `dlt_in_flight` | gauge，调用 | group；实际尚未结束的 DLT worker，含仍在退出的旧分配责任 |
| `publish_delivery_unknown_total` | counter，条 | lane；本地原生责任已终结、远端结果仍不明 |
| `control_target_total` | counter，目标 | group/outcome；控制报告的 applied/not_applied/unknown/owner_changed |
| `rebalance_assigned_total` | counter，回调 | group；分配完成，含首次加入 |
| `trace_invalid_total` | counter，输入次 | group/reason；malformed 或 duplicate，不包含原始头 |
| `broker_lag_offsets` / `broker_lag_partitions` / `broker_lag_missing_partitions` / `broker_lag_observed_at_millis` | gauge，offset距离 / 分区 / 分区 / Unix毫秒 | group/isolation；最近显式位点查询的范围与完整性 |
| `source_stop_requested` / `source_resources_closed` | gauge，0或1 | 无；永久停止意图 / 全部源资源退出 |
| `close_active_owners` / `close_active_handlers` / `close_retained_records` / `close_released_unverified_records` | gauge，个 / 次 / 条 / 次 | 无；关闭报告的实际剩余责任与未核实恢复的释放次数 |
| `replay_active_tasks` / `transfer_active_dependencies` / `transaction_active_tasks` / `dlt_active_tasks` / `admin_active_borrows` | gauge，个 | 无；各执行域尚未退出的真实责任 |
| `metrics_queued` / `metrics_active` | gauge，事件 / 回调 | 无；指标出口的排队与真实执行量 |

`METRIC_CATALOG` 提供与 Java `MetricCatalog` 相同的64项名称、类型、单位、描述及标签，napp 使用同一目录。
源级责任与注册规模由宿主显式调用 `sample_metrics()` 采样；入口不执行网络查询，不创建定时线程。
组内量每秒汇总多个 owner；按键指标覆盖所有仍保留原文的排队、运行、重试及完成证据，字节为八倍副本预留。
`GroupHealth.key_pending_*` 仍专指已退出调用的完成证据窗口，与此处全体按键输入指标不同。
关闭后 `KafkaHandle::sample_metrics()` 仍可读取报告，关闭出口不保证送达最后一个 gauge，最终退出以 `CloseReport` 为准。

broker lag 四项仅由显式 `observe_offsets(...)` 结果更新，表示同一 group/isolation 最近请求的分区集合，
不自动覆盖组内全部分区。任何请求分区缺少 lag 证据时 `broker_lag_offsets` 为 -1，必须同时读取分区数、缺失数及时间戳。
read_committed 使用 LSO，read_uncommitted 使用普通末尾；offset 距离包含空洞及控制记录，不等于业务条数。
本地 `retained_records/bytes` 独立统计实际输入责任，不由 consumer position 推算；观测丢弃不改变二者的业务状态。

Ready 必须同时满足所选 assignment 条件与全部 owner 的 Running 状态。保留 assignment epoch 的
保护态仍不能通过 Ready；等待者的超时不会把 owner 发布的 `group_ready` gauge 改为零。
`ReadyRequirement::satisfied_by(&health)` 可用于受管监控，和启动等待使用同一判定；返回 true 只代表该快照，不提供永久的消费权威。

开发或预发环境创建 topic：

```rust
use nasa::kafka::TopicSpec;

let admin = kafka.admin();
admin
    .create_if_absent(
        TopicSpec::new("orders", 12, 3)
            .config("min.insync.replicas", "2")
            .config("cleanup.policy", "delete")
            .config("unclean.leader.election.enable", "false"),
    )
    .await?;

let description = admin.describe_topic("orders", true).await?;
let exists = admin.topic_exists("orders").await?;
```

生产部署通常预建 topic，并在启动阶段调用 `describe_topic()` 校验分区数、RF 和关键配置。在线增加分区会改变
key→partition 映射，只能在业务明确接受顺序边界变化时调用 `increase_partitions()`；删除 topic 和修改配置也应由
受控运维流程执行。

## Application 接入

Schema Registry 的独立构造与 `schema_registries` 标准装配见上文[配置与使用](#配置与使用)。
受管入口使用 `app.schema_registry(name).await`，由 Application 负责材料、指标与关闭，无需 Kafka 消费组件。
WS Kafka 通过 `configure_ws_kafka` 绑定受管来源，来源必须关闭 collected consumers，避免 consumer owner 重复。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
