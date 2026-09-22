# 应用部署指南

本文约束使用 `#[nasa::application]` 的业务进程如何构建、配置、接流和停机。保留独立入口的项目继续
按自身启动协议部署。

## 构建

在业务工程根目录使用锁文件生成发布二进制：

```bash
cargo build --locked --release
```

工作区中的单个二进制使用 package 名选择：

```bash
cargo build --locked --release -p <package>
```

镜像只复制最终二进制、`zcf/` 下的配置和业务必需的证书或静态资源。进程工作目录必须能解析
`zcf/application.yml`；该文件必须存在，内容可以是 `{}`。

## 启动配置

```yaml
application:
  name: order-service
  mode: service
  startup_timeout_ms: 30000
  shutdown_timeout_ms: 15000
```

- `mode: service` 用于常驻进程；无 Web 的后台服务必须显式设置。
- `mode: batch` 用于任务完成后正常退出的批处理。
- `mode: auto` 在声明 Saga、Kafka、Outbox、Web、长连接、服务发现或调度组件时解析为常驻服务；
  其它组合解析为批处理。
- `startup_timeout_ms` 是组件、业务 Hook、initializer、最终检查和延后服务注册共享的启动期限。
- `shutdown_timeout_ms` 是全部反向清理共享的总预算。

配置优先级从低到高为主文件、显式 profile、远端 overlay 和 `APP__...` 环境覆盖。凭据只通过部署
平台的 secret 注入能力提供，不写入配置文件、镜像层、命令历史或普通日志。

## 容器与进程监督

容器入口使用 exec 形式，让业务进程直接成为容器主进程并收到 SIGTERM：

```dockerfile
ENTRYPOINT ["/app/order-service"]
```

部署平台的强制终止宽限期应大于 `application.shutdown_timeout_ms`，并预留摘流、日志刷新和调度抖动。
首次终止信号触发正常停机；Stopping 阶段再次收到终止信号会立即退出。监督器依据退出码和 Ready 状态
决定重启，不能只判断端口是否存在。

业务异步 close、flush、归还和注销应在 UserHook 通过 `register_graceful_shutdown` 登记，并由上述同一个
信号入口驱动。Service 先摘流、收口受监督任务与 initializer，再按 priority 执行业务停机任务，随后关闭
业务资源和更早启动的组件。Batch 正常完成工作负载也会进入收口，不需要额外信号；它在受监督任务之后
执行业务停机任务、释放业务资源，再清理静态 initializer 和更早启动的组件。
业务任务组会为资源清理预留尾部预算；增加任务数量不会增加总停机时长上限。
摘要中的 `business_shutdown_abandoned`、`business_shutdown_timed_out`、`business_shutdown_panicked`
非零表示收尾不完整；不能仅凭 Service 正常信号退出码 0 判断所有业务操作已完成。

SIGKILL、进程 abort、同步阻塞或直接丢弃 Runner 不能视为优雅停机。析构保护不保证业务 future 已执行，
需要跨崩溃保证的副作用仍应依赖持久事务、Outbox/Inbox 或可恢复业务状态，而不是内存中的退出 callback。

## 健康端点

声明 `web` 组件且 `server.health=true` 时，运行时提供：

- `<context_path>/healthz`：进程存活状态；
- `<context_path>/readyz`：业务接流状态。

readiness 是负载均衡和滚动部署的接流条件。自行在 UserHook 中托管 HTTP 服务的项目不会自动获得
这些端点，必须提供自己的管理入口或等价健康信号。

Service 在所有 Ready 装配、initializer 任务工厂和最终静态检查成功后，发布生命周期 Ready 并统一
放行受管终端任务。gRPC 绑定状态 `Bound` 只表示占有端口，不处理 health 或业务 RPC；Nacos 只在
统一放行后注册，gRPC 端点还须等待 Running。远端注册确认前动态 readiness 仍不可用，不能用
生命周期 Ready、TCP connect 成功或单个组件 Ready 代替接流证据。

`server.port: 0` 的真实端口在 bind 后产生，可通过应用运行时的监听地址能力读取；需要固定服务端口的
部署不应使用该设置。

## Web 协议与排空

声明 `#[nasa::application("web")]` 且编译 `application,web` feature 后，Application 独占 Web TCP
listener。默认只接受 HTTP/1；需要同一明文端口接受 h2c prior knowledge 时配置：

```yaml
server:
  http2:
    enabled: true
```

连接、stream、流控、header/frame、发送缓冲、reset 与 PING 参数均有受校验默认值，业务通常无需配置。
该 listener 不实现 `Upgrade: h2c`，也不终止 TLS；h2 over TLS 由具备证书与 ALPN 合同的上游代理终止。
停机先停止 accept，再对 HTTP/1 关闭 keep-alive、对 HTTP/2 发送 GOAWAY，并在组件与 Application 共享的
预算内等待已接纳请求结束。滚动部署必须给该预算预留足够的强制终止宽限期。
编入统一观测能力时，协议、连接、容量拒绝和连接错误指标由 `grafana.observability` 配置的出口暴露，
与 `server.health` 独立。未编入该能力时，`server.health=true` 才提供兼容的 `<context_path>/metrics`。
完整字段、范围和指标合同见
[napp Web listener 受管模式](../napp/README.md#web-http-listener-受管模式)。

## SQL 通知与监控平台

部署前为通知适配器提供业务配置，在启动前或初始化阶段调用
`nasa::application::notifications::init`。框架不保存机器人凭据、不选择 HTTP REST/gRPC，也不
安装通知微服务。未提供实现时忽略通知，不阻断应用启动；若业务要求通知必须可用，应由业务
初始化检查承担该条件，不能依赖可选接口隐式保证。

逐条慢 SQL 通知需开启 `sql.observability.alerts.slow_sql.enabled` 并设 `cooldown_ms=0`；
阈值由 `slow_sql.threshold_ms` 决定，达到或超过即命中。生产保持参数输出关闭。队列与下游失败
不影响数据库 readiness，需单独观察通知丢弃与失败指标。

`grafana.observability.enabled` 默认 false。开启后选择独立或 Web scrape、remote write，并配置
环境、集群与实例身份；平台 controller 单独部署，只给它 Grafana 与控制面写权限。
remote write 失联规则引用外部平台的期望实例指标，平台负责随扩缩容更新并保障其连续性；
框架不维护期望实例列表。权限和完整 YAML 见 [观测出口与平台适配](../nafana/OBSERVABILITY.md)。

## Redis 分区消费容量

按每个 Redis 源分别选择 `partition.executor.scope`：`source` 默认共享一个 Runner，`group`
按逻辑组隔离，`stream` 按物理 Stream 隔离。多源配置放在
`redis.properties.<qualifier>.partition` 下，不与 Application 根级 `partition.runners` 混用。

容量规划使用完整配置拓扑，包括当前未持锁的分区。Runner 数不得超过 `max_runners`，每 Runner
规范化槽数之和不得超过 `max_total_partitions`。所有域合计至少需要各域一批记录及
`batch_size × max_record_bytes` 正文，还需每域一个读取批次和删除位置；不足时启动失败。
最低正文预留只覆盖线格式，需按解码对象权重留出执行余量。固定份额不借出，调整后须重建运行时。

生产者须以相同规范化业务键路由到同一物理 Stream，才能依赖跨进程的分区顺序；业务副作用保持
幂等。执行隔离不会隔离共享 Redis 后端或阻塞线程。业务创建的 `RunningPartition` 必须在受管
Redis 客户端释放前收口，停机期限不足不能宣告消费责任已完成。
配置示例与容量算法见 [nadis](../nadis/README.md#执行域与消费架构) 和
[分区消费说明](../nadis/docs/partition.md#容量与配置)。

## 部署顺序

1. 使用锁文件在受控依赖源中完成构建。
2. 确认生产配置包含正确的应用模式、超时、监听地址、组件开关和 secret 引用。
3. 完成数据库扩展、外部权限和下游资源准备。
4. 先启动少量实例，确认 Ready、配置修订、下游连接和错误率，再扩大批次。
5. 回退时仍发送 SIGTERM 并等待摘流、任务排空和资源释放。

具体条件见 [交付就绪清单](release-checklist.md)，运行期处置见 [应用运维指南](operations.md)。

## Saga 部署

`#[nasa::application("saga")]` 会隐式纳入 DB 与受管 Outbox；managed 模式按 `saga.role` 和受信配置
自动构造运行计划与发布端，业务通过步骤和 workflow descriptor 提供领域定义。direct client 不创建
本地 DB/Outbox；可靠 client 的业务事实、start-intent 与 dispatcher 固定使用
`saga.client.datasource_ref`，显式 `outbox.datasource_ref` 冲突时在 Ready 前拒绝。省略后者或只设置
轮询预算不改变绑定，两个数据源都已建表也不允许拆开写入与扫描。

Kafka 数据面声明 `"kafka"`，Redis Streams 数据面声明 `"redis"` 并开启对应门面 feature；managed
模式不再由业务提交 transport 计划。HTTP 入站由 `"web"` 组件拥有；Saga gRPC 入站自动把框架
generated service 登记到 `"grpc"` 组件的唯一 listener，纯出站 client 不声明该组件。选择 custom
模式或独立宿主时，调用方才自行提交计划并拥有 transport、Ready 和停机。发布和消费两端必须成对
具备确认、重领、认证与 durable DLT/收据语义。

Saga 采用 expand-first，部署顺序固定为：

1. 按 Saga 与 Outbox 迁移清单扩展每个本地事务域，保存 DDL、行数、索引与校验事实。
2. 准备 transport 路由、consumer identity、ACL/mTLS/HMAC、DLT、消息保留期和 replay horizon。
3. 滚动启动可读取新结构但尚不产生不兼容状态的 binary，确认 Ready、定义摘要、descriptor 与历史实例。
4. 需要租户配额时，先升级全部写入方，再事务内对账并置初始化标记，最后启用上限。
5. 需要 `MANUALLY_CLOSED` 时，先确认全部副本都能解析该终态，再打开 `enable_manual_close`。
6. 观察 timer、Inbox/Outbox、transport、配额、人工介入和提交不确定指标后再扩大流量。

只有旧 binary 已完全退出、审计已导出且 replay horizon 允许时，才执行结构回退。已产生新终态或新
持久字段后，不得通过回退旧二进制假装兼容；应保持入口 NotReady，先完成数据与读者兼容评估。

Saga binary collation 脚本没有通用 down。执行前记录原 collation，并单独评估大表重建、metadata
lock、复制延迟、磁盘余量和完成时间。Ready 前完成 `Orchestrator::verify_startup`；任何活跃定义或
descriptor 漂移都拒绝接流。

每个 Orchestrator 副本的 timer owner 必须唯一且重启稳定；Redis `(stream, group, consumer)` 同样
逐循环唯一。扩缩容不能复制旧副本的运行实例 nonce、fencing capability 或 consumer identity。
正常下线先 NotReady/摘流，再停止领取 timer 和新消息，排空已接管事务，最后关闭 Outbox、transport
与数据库。强制终止后依赖 durable timer、PEL/offset、Inbox 与 Outbox 事实接管，不能手工 ACK 或删除
记录制造“已排空”。

本地容器能够确认 MySQL 提升、Kafka 多 broker、ACL、消息重投和故障恢复语义，但不能替代生产网络、
Redis Cluster 槽迁移、磁盘、容量和灾难恢复批准。gRPC 业务链路还必须由具体服务证明 listener 与方法
资源上限、已验证 peer identity、deadline、证书轮换和 drain。完整边界见
[Saga 生产运行指南](saga-production.md)。
