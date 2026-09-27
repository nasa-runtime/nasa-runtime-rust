# nanotify-core

渠道中立的可选通知接口：业务实现 `Notify` 后主动调用 `nanotify_core::init` 安装；使用时已有实现就调用，没有实现就忽略。框架不提供渠道客户端，不解释下游通信协议或机器人凭据。

组件同时提供长度受限的 `Notification`、固定容量队列与原子指标，不依赖 Mapper、数据库或具体 transport。受管 SQL 告警只做非阻塞入队，通知失败不会改变 SQL 结果。

## 业务实现与通知微服务

业务 `Notify` 实现负责调用独立的通知微服务；例如由 `telegram-bot` 服务集中持有机器人凭据并向
Telegram 发消息，其它业务服务只调用该服务。gRPC、HTTP REST、请求结构、鉴权、服务发现和下游
幂等策略均由业务定义，不属于本组件合同，也不由框架生成客户端或安装微服务。
机器人连接的单一所有权由通知微服务保证，业务副本扩容不会各自创建机器人连接。

```text
受管 SQL 事件 → 有界 dispatcher → 业务实现的 Notify → 通知微服务 → 外部消息渠道
```

`NotifyReceipt.accepted=true` 只表示业务适配器确认下游接受，不表示最终用户已收到或读取消息。
受管 worker 把 `accepted=false` 计为拒绝；直接 `notify` 则原样返回回执，由调用方判断该字段。
请求超时但是否被下游接受未知时返回不可重试失败；不能把不确定结果当作未发送。

## 主动初始化与使用

```rust,ignore
// 业务构造客户端并实现 Notify；框架不选择调用通知微服务的协议。
nanotify_core::init(std::sync::Arc::new(business_notifier))?;
let receipt = nanotify_core::notify(&notification).await?;
```

| 接口 | 语义 |
| --- | --- |
| `init(Arc<dyn Notify>)` | 原子安装进程唯一实现；重复初始化返回 `AlreadyInitialized`，不覆盖旧实现 |
| `get()` | 已安装时返回共享引用，否则返回 `None`；不等待、不创建客户端 |
| `notify(&Notification)` | 未安装返回 `Ok(None)`；已安装直接调用，返回 `Ok(Some(receipt))` 或实现的错误 |

初始化可以在 Application 启动前或业务初始化阶段进行。实现属于整个进程，不属于单个 Application；
没有自动替换或重置接口。未初始化时忽略的消息不会在稍后初始化后补发。

直接调用 `notify` 不自动排队、重试、计量或启动任务，调用者负责超时与异常隔离。受管 SQL 告警则使用
默认路由和宿主 worker，不在 SQL 完成路径调用业务实现。统一门面也可通过
`nasa::application::notifications` 使用这些接口。
该门面模块要求同时启用 `application` 与 `mapper`/`mapper-pgsql`；直接使用 `nanotify-core` 不依赖
这些 feature，也不会自动获得 Application 的 SQL 通知 worker。

## 运行边界

宿主在启动时冻结 provider 目录，使用 `NotificationQueue::bounded` 创建一个队列，再通过 `NotificationProducer::route` 取得具体 `AlertRoute`。`try_send` 不等待 worker，也不调用用户代码；容量不足时丢弃新消息。未启用通知的宿主不应创建队列。

受管 SQL 通知只在宿主监督的异步 worker 内调用 `Notify`；直接通知调用的执行边界由业务负责。队列本身不启动线程或任务，也不负责持久化、集群去重和可靠事件传递。进程停止或队列拥塞会丢弃通知；业务事件需要可靠传递时使用持久化事件设施。

custom provider 必须使用协作式、取消安全的异步 I/O，不得同步阻塞 executor 或自行派生失去监督的任务。异步超时依赖 Future 让出执行权，不能强制中断同步阻塞代码。
受管 dispatcher 隔离 provider Future 的轮询、超时取消、完成与停机析构。析构展开统一记为 `panic`，
覆盖原先的接受、超时或取消候选结果，并只记录一个终态；`panic=abort` 或用户代码内部双重展开不在隔离范围内。

停止时先调用 `NotificationProducer::close` 撤销所有旧 route 的生产权，再由接收端关闭并在有限预算内排空。通知文本在 `Notification::new` 内清理控制字符并按 Unicode 字符边界截断；SQL 最多 4096 字符，方法身份最多 512 字符，其它文本最多 128 字符。消息不得包含绑定参数、凭据或数据库原始错误。

## 渠道合同

```rust,ignore
#[async_trait::async_trait]
impl nanotify_core::Notify for BusinessChannel {
    /// 业务作用：把已裁决通知移交给业务渠道，不参与触发事件的事务。
    /// 参数说明：`notification` 是已清理且长度受限的纯文本消息。
    /// 返回：渠道回执或脱敏失败，结果未知时禁止重发。
    async fn notify(
        &self,
        notification: &nanotify_core::Notification,
    ) -> Result<nanotify_core::NotifyReceipt, nanotify_core::NotifyError> {
        // 按业务渠道协议异步投递；只返回稳定分类，不保留原始响应正文。
        self.deliver(notification).await
    }
}
```

`NotifyError` 分类为 `timeout`、`unavailable`、`rate_limited`、`rejected`、`authentication`、`invalid_request`、`other`。默认禁止重发；仅当连接未发送或服务明确拒绝且允许稍后重试时，provider 才能声明 `RetrySafety::Safe`。未知结果的超时不能视为未发送。

## 调度配置

`DispatcherConfig` 在受管应用中对应 `sql.observability.dispatcher`，空对象递归采用默认值，未知字段与非法范围拒绝启动。

| 字段 | 默认值 | 范围 |
| --- | --- | --- |
| `queue_capacity` | 512 | 1..=65536 |
| `max_in_flight` | 4 | 1..=32 |
| `delivery_timeout_ms` | 3000 | 100..=30000 |
| `shutdown_drain_timeout_ms` | 3000 | 0..=30000 |
| `max_attempts` | 1 | 1..=5，包含初次尝试 |
| `retry_initial_backoff_ms` | 250 | 50..=5000 |
| `retry_max_backoff_ms` | 2000 | initial..=10000 |

以上为宿主共同使用的配置合同；具体并发、超时和重试由宿主执行，而不是队列自行启动 worker。
`delivery_timeout_ms` 从 worker 开始投递时计算，覆盖同一消息的全部尝试与退避，不包含此前排队
或等待 Application 放行的时间。队列不提供消息 TTL，不能把该值当作从 SQL 完成到最终收件的上限。
`shutdown_drain_timeout_ms` 是停止后的队列排空预算，仍受 Application 剩余停机期限约束。

默认无需 `notifications.providers` 或 `provider_ref`；业务安装进程实现后，只需启用所需告警：

```yaml
sql:
  observability:
    alerts:
      execution_error:
        enabled: true
```

省略 `provider_ref` 或显式指定 `default` 都引用进程实现。`default_route().try_send` 在未安装时返回
`EnqueueOutcome::Ignored`，不入队、不计入丢弃或投递失败；安装后仅新通知进入受管队列。
Service 在全部 Ready 门禁通过后投递；Batch 在工作负载前激活通知 worker，可在工作负载内安装实现。
Service 的组件 Ready 装配与 initializer 任务工厂全部完成后，还须通过最终静态检查和共享启动预算
复验；发布 Application Ready 后才统一放行 worker。仅完成某个组件的 Ready 不会提前发送通知。
所有告警关闭时不创建队列；告警启用而尚未安装实现时，保留有界调度资源以支持稍后初始化。

需要多个具名业务实现时，可以使用可选命名路由，在 Service UserHook 调用
`app.register_notify_provider("ops", provider).await`；这不是默认通知的前置条件：

```yaml
notifications:
  providers:
    ops:
      kind: custom
      enabled: true
sql:
  observability:
    alerts:
      execution_error:
        enabled: true
        provider_ref: ops
```

`notifications.providers` 默认空，最多 32 个名称；`default` 为保留名，不允许声明或通过命名接口注册。
实际活跃路由（含默认路由）最多 32 个。名称为 1–64 个 ASCII 字符，以字母开头，后续
允许字母、数字、`_`、`.`、`-`。`kind` 必填且只接受 `custom`，`enabled` 默认 true；不接受 endpoint、
bot token 或协议字段。业务适配器的配置与 secret locator 放在业务自己的配置子树，框架不解释其语义。

显式命名引用必须存在且启用；非法名称、未知 kind、禁用或不存在的配置引用仍拒绝启动。
Prepare 冻结命名目录；未提交实现的命名路由忽略投递，不让可选通知阻断应用。
与默认路由的入队前忽略不同，命名路由缺少实现时，候选仍可入队，由 worker 取出后忽略，
因此入队计数可能增长而没有对应投递终态；应先核对命名实现是否已在 UserHook 提交。
Batch 没有前置 UserHook 命名注册窗口，因此只支持进程默认通知；活跃命名路由在数据库动作前拒绝。

## 逐条慢 SQL 通知

业务已安装实现时，以下配置让每次达到或超过 1000 ms 的 SQL 尝试入队，不用额外 provider 声明：

```yaml
sql:
  observability:
    slow_sql:
      threshold_ms: 1000
    alerts:
      slow_sql:
        enabled: true
        cooldown_ms: 0
```

默认慢通知冷却为 60000 ms；设为 0 才不抑制连续事件。慢日志开关不控制通知，阈值不计连接等待、
缓存处理和消费者处理时间。同一次慢执行失败且已启用错误路由时，优先走错误规则及其冷却并携带
`slow` 事实；未启用错误路由时仍可命中慢通知，取消不发送。
“逐条”描述规则命中后的入队策略，不是持久投递保证；应同时观察入队、丢弃和投递终态。
非零冷却在尝试入队前占用，默认实现缺失、队列满或停止导致消息未入队时也不会撤销。
稍后安装实现不补发旧消息，也不重置规则冷却；需要逐条提交时保持 `cooldown_ms=0`。

## 指标

`NotificationProducer::metrics()` 返回统一结构化指标源，`series_budget()` 提供精确最坏系列数。生产与投递路径只更新预分配原子单元，抓取或导出时才构造样本。指标包括 `nanotify_enqueued_total`、`nanotify_dropped_total`、`nanotify_deliveries_total`、`nanotify_delivery_duration_seconds`、`nanotify_queue_depth`、`nanotify_queue_capacity`。

label 只包含启动时冻结的 provider、固定 event、outcome 和 reason，不包含 SQL、方法、消息 id 或原始错误。冷却和队列均为单进程语义；跨实例唯一告警应交由外部聚合平台。
