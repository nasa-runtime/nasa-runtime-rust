# 应用运维指南

本文说明应用模式进程的日常观测、配置刷新、停机语义和故障处置。业务端口、下游依赖和数据恢复
流程由对应项目手册补充。

## 运行状态

```text
Bootstrap → Starting → UserHook → Ready → Running → Stopping → Stopped
                                                        └────→ Failed
```

- 生命周期 `Ready` 在组件装配、业务 Hook、initializer 工厂与最终静态检查完成后提交；发布前在关键
  领域的本地状态保护内复验任务责任、认证连接、健康证据与启动预算，同次发布统一启动许可。
  公开 Ready 时受管终端、Redis 派生入口与 Client 已获许可。gRPC `Bound` 不是 Running，服务注册
  待确认时 `app.is_ready()` 仍为 false。
- 任一关键任务在 Running 阶段意外结束会提交失败意图，进程进入统一停机。
- 进入 Stopping 后 readiness 先变为 false，再摘流、停止 accept、排空任务并反向释放资源。
- `Failed` 是终态，不会回到 Running。

## 退出码

| 场景 | 结果 |
| --- | --- |
| 常驻服务收到终止信号并在预算内清理完成 | `0` |
| 启动失败或关键任务先发生故障 | 非零 |
| 批处理尚未完成时收到信号 | `128 + signal` |
| 批处理业务结果已提交后在清理阶段强退 | `0` |
| Stopping 阶段再次收到终止信号 | 立即退出，按已提交终止意图决定 |

监督器同时记录退出码和停机前的 primary error。清理阶段的后续错误进入 shutdown report，不覆盖更早
提交的根因。

## 日常检查

- 应用名称、模式和最终配置修订符合部署批次。
- `/readyz`、`/healthz` 与真实 listener 状态一致。
- 服务发现注册地址与实际监听地址一致。
- 关键任务没有提前退出，后台任务数量和队列长度没有持续增长。
- 数据库池、缓存、配置监听和服务发现处于最后一次成功状态。
- 配置刷新没有 `ApplyFailed` 或 `RestartRequired` 长时间未处理。
- 日志中没有连接串、访问令牌、业务 payload 或控制 token。

## 命名资源与配置应用状态

配置刷新应同时检查 `ConfigView` 的期望配置与目标应用状态。候选材料或新观察集准备失败时保留
旧视图；已经发布的新 YAML 也不代表所有运行资源都采用了新参数。

| 事实 | 含义与处理 |
| --- | --- |
| `Applied` | 目标采用对应配置，按记录的实际版本观察 |
| `RestartRequired` | 现有资源保持运行，按部署流程重启后才采用变化 |
| `ApplyFailed` | 热应用失败，保留最后成功版本，先定位固定错误分类 |
| 旧受管句柄返回 `Closed` / `RuntimeClosed` | 所属实例已收回准入，不能用重试重新打开 |
| 对象存储 `NotReady` / `Degraded` | 结合 `critical`、探测策略与证据时效判断，不把 HEAD 成功当作全部对象权限 |

日志和命名 TLS HTTP 支持各自已声明的热应用；连接来源、对象存储、Schema Registry 与普通 REST
参数变化要求重启。`redis_streams`、`redis_proxies`、`redis_pipelines`、`ws_clients` 与受管
`hystrix` 的名称、规则、容量和认证材料也冻结到启动，变化报告 `RestartRequired`。
无关更新不会清除失败或重启要求；相同 fingerprint 且材料未变不会隐式重试。
仅由禁用计划引用的材料不解析、不监听，共享材料存在活跃消费者时仍会观察。

`diagnostic_snapshot(limit)` 只读取已有状态，limit 为 1..=256；它不执行网络探测，也不是跨组件原子
快照。Schema Registry 构造不证明远端 readiness，对象存储仅按所选健康策略收集证据。

组件 `id` 与静态依赖在阶段执行前读取并冻结；元数据读取展开会报告 Bootstrap 错误，不执行组件
启动阶段。阶段或清理失败保留首次终止原因，后续异常进入次要报告；同步阻塞与 `panic=abort`
不受异步期限和展开隔离保护。

## Redis 派生计划、出站 Client 与隔离命令

| 现象 | 判断与处理边界 |
| --- | --- |
| Redis 派生计划没有业务进展 | 先核对启动许可、来源连接和实际任务数；有效空轮询可以形成读取证据，空闲不等于消费失败 |
| 微批排队参数字节持续增长 | 检查生产速率、Redis 延迟与配置容量；B＋M 只是单批参数字节边界，不是进程内存上限 |
| 微批返回 `ExecutionUnknown` | 通过业务幂等事实对账，不能按失败自动重放写入 |
| Proxy 清理为 `Pending` | 已知 PEL 仍有消息，保留 consumer；本地任务结束不表示这些消息已成功处理 |
| Proxy 清理为 `Unavailable` / `Deadline` | 查看次要停机失败，保留证据；删除回包超时不能证明远端未执行 |
| 关键 Client 断连或重连 | 动态 readiness 为 NotReady；重新认证后恢复，永久 owner 退出触发停机 |
| 可选 Client 断连 | 动态 readiness 为 Degraded；发送仍可能被拒绝，不能把宿主可接流当作 Client 可用 |
| 旧 hystrix 命令返回 503 | 本代 owner 已关闭；使用当前 Application 的命名命令或属性宏，不保留跨实例静态强引用 |

`redis_derived_observations()` 的完成次数包含失败与未知结果，不等于唯一消息数、PEL 数或业务成功数。
`ws_client_observations()` 区分连接事实、认证/PONG 间隔和失败类别；发送返回 true 仅表示本地入队。
Redis 派生计划的健康证据 15 秒过期；Client 每秒采样、5 秒过期，失败与恢复阈值均为 1。
协议检测与采样有延迟，尚未被本地观察到的断连可能晚于 Ready 发布。关闭后的晚到认证不会恢复准入。

hystrix 只有一个受管周期观测任务；其健康证据 5 秒过期，任务意外退出触发停机。Service 业务收尾
仍可调用命令，之后才关闭准入并等待在途执行。上一代未真实退出时的新 owner 冲突需要排查未归还
的业务 future，不能通过清空全局引用跳过等待。hystrix 不提供错误率熔断状态机。

## SQL、通知与指标出口

SQL 日志、阈值与通知策略在启动时冻结，按 `method > datasource > global` 逐叶覆盖；可以通过
`app.sql_observability_effective(datasource, method).await` 查询有效值。日志级别热刷新不能改变已建
连接的 SQLx 选项。Mapper 方法耗时、真实数据库调用、Pool acquire、事务连接槽和流生命周期不能混算。

发现慢 SQL 未通知时，依次检查业务是否已经 `init(Notify)`、告警是否启用、有效阈值与冷却、队列
丢弃和 provider 投递结果。默认慢通知冷却为 60000 ms；逐条通知必须设为 0，且原始耗时达到阈值
即命中。错误通知路由启用时，同一次慢失败按错误规则及其冷却裁决，不重复发送慢通知。
`RowNotFound` 不触发执行错误通知，取消与析构不发送通知；裸 SQLx 不产生 Mapper 慢 SQL 通知。
非零冷却在入队前占用，即使实现缺失或队列满也不撤销，稍后安装实现不会清零冷却。
冷却抑制没有进入队列，不应从 `nanotify_dropped_total` 寻找对应丢弃。命名路由未提交实现时，
worker 会忽略已入队的候选，需区分默认路由的入队前忽略与命名路由的消费时忽略。

`nanotify_enqueued_total` 只表示入队，`nanotify_deliveries_total{outcome="accepted"}` 只表示业务
适配器确认接受，均不是最终用户收件证明。结合 `nanotify_dropped_total`、队列深度与容量、投递
耗时判断阻塞点，不通过让 SQL 等待下游或无限重试掩盖通知故障。
投递耗时与 `delivery_timeout_ms` 都从 worker 开始处理时计算，不包含排队或启动屏障等待。
队列没有消息 TTL，单条投递预算不是端到端收件期限。

统一出口通过 `grafana.observability` 显式启用，不由 `server.health` 自动开放。scrape 失败、
remote write 失败、无调用和无数据需要区分。remote write 失联信号不能区分进程退出、网络中断
与 receiver 拒绝；应检查外部平台期望指标是否持续更新，期望源缺失不能解释为实例全部退役。
平台资源只由独立 controller 写入，业务副本不需要 Grafana token 或 Kubernetes 写权限。

详细默认值见 [SQL 观测](../namapper-core/README.md#sql-观测与配置)，出口与平台含义见
[统一观测说明](../nafana/OBSERVABILITY.md)。

## 授权与链路观测

- `napp_authz_routes_uncovered` 必须与发布时批准的公开 route 覆盖一致；deny 模式下未覆盖鉴权 route
  会阻止 Ready，不应通过临时放宽全局缺省绕过。
- `napp_authz_unmatched_observed_total` 用于评估从 observe 收紧到 deny 的真实影响；
  `napp_authz_unmatched_denied_total` 增长时先核对稳定 route ID、策略 generation 和公开 route 豁免。
- 合法入站 `traceparent` 必须继承原 sampled 位。没有 exporter 的服务只传播未采样根；不得把下游
  span 数量上升解释为本服务已导出。
- 调度 span 只代表已经通过 leader gate 与 FireLog claim 的业务运行。Skipped 没有执行 span 是正常
  语义，应结合运行记录判断 leader 缺席、重复 claim 或 misfire 决策。
- `ExporterSnapshot` 的 pending 持续接近队列上限或 dropped 增长时，优先检查 OTLP endpoint、停机
  预算和采样率；不得把 payload、完整业务身份或凭据加入 span 属性辅助排查。

## Redis 分区消费观测

先从 `RunningPartition::snapshot()` 读取 `executor_scope` 与 `execution_domains`，确认隔离
粒度及各域的固定份额。聚合计数表示整个源实例的责任，各域计数用于定位积压；同一域的空闲额度
不会自动借给其它域。不同 Redis 源的 Runner、预算和停止控制相互独立。

| 观测事实 | 含义与处理边界 |
| --- | --- |
| 某域 record、payload 或 batch 达到份额 | 本域背压；检查批量大小、解码权重和 handler 延迟，不能仅增加 Runner 数就期望总容量扩大 |
| `unknown_commits` 持续非零 | 已进入 ACK 阶段但确认结果未知，可能来自成功业务、毒消息 Drop 或 tombstone 清理；保留提交对账，不据此重放业务 |
| `parked_sources` / `parked_records` 非零 | 来源被人工处置门禁冻结；同计划同 key 后继仍等待，按管理 API 处置 |
| `runner_degraded` 或域降级 | 所属来源停止接纳新业务，已有成功责任仍需收口；整体消费 readiness 为 false |
| `record_debt` 或协议异常 | 读取合同失效；保留责任，停止并重建运行时，不能靠清零 gauge 恢复准入 |
| `async_delete_pending()` 持续积压 | ACK 后空间回收背压；检查删除 owner 与 Redis 可用性，不能据此再次执行业务 |
| `delete_retained_records` 增长 | 删除 owner 退出后正文可能留存；ACK 事实不撤销，回收依赖保留策略或运维处理 |

执行域共享 Redis 客户端、外部后端和 Tokio runtime。多个域同时变慢时需检查共享连接、Redis
响应和同步阻塞；同 key 跨域等待也可能是预期的顺序屏障。消费器的 `snapshot().ready` 不自动改变
Application `/readyz`，业务必须显式接入健康策略。

正常停机在关闭 Redis 客户端前调用 `shutdown_until(deadline)`，检查 `converged` 与 `remaining`。
未收敛可再次等待同一操作；只有明确接受中止后果时才使用 `force_shutdown_until`。
直接 Drop、发出 cancel 或停止续租都不能代替 handler、I/O 和锁的退出证明。
详细状态与配置见 [Redis 分区消费](../nadis/docs/partition.md)。

## 配置刷新

配置视图把期望快照和每个组件的应用状态放在同一次发布动作中。运维必须同时查看配置修订与状态：

- `Applied`：组件已应用目标快照；
- `ApplyFailed`：目标快照已更新，组件仍运行在最后一次成功状态；
- `RestartRequired`：字段不能热切，需要滚动重启。

远端配置不能动态改变进程身份、模式、线程数或全局 deadline。启动阶段发生冲突时拒绝 Ready，运行期
冲突标记为需要重启。候选快照失败不得撤销当前可用资源。

## 停机与超时

Service 正常停机只发送一次 SIGTERM，然后等待进程自行退出：

```text
NotReady / 摘流
  → 停止 listener 接收
  → 排空 Web、受监督任务与 initializer
  → 执行业务停机任务
  → 关闭业务托管资源
  → 关闭数据库、缓存和配置监听
  → 刷新日志并退出
```

所有异步清理共享一个绝对截止时间。超时后运行时记录未完成阶段并中止可取消任务；不让出执行权的
代码或无法取消的阻塞工作仍由部署平台的强制终止上限兜底。

### 业务停机任务

业务在 UserHook 登记的 `register_graceful_shutdown` 任务位于受监督任务收口和业务资源释放之间。
Service 先关闭新流量与 Ready action，收口受监督任务和 initializer action，再按 `priority` 升序执行
业务任务，同一优先级按登记顺序执行，随后释放业务资源。
Batch 在工作负载完成或失败后收口受监督任务，再执行业务停机任务、释放业务资源，最后才撤销静态
initializer 及更早启动的组件；静态初始化早于工作负载，因此其清理位置与 Service 不同。
UserHook 结束时关闭登记门，Seal 时封存任务集合；名称是当前 Application
内的稳定唯一身份，最多 128 个 UTF-8 字节，单个 Application 最多 256 项。

每项任务共享全局停机 deadline。运行时为任务组预留后续清理的尾部，再按剩余时间和未执行项数分配公平份额。
任务返回错误、超时或 panic 时继续执行后续项；未开始的任务记为 `Abandoned`。这些结果不会改变首次停机
原因或退出码，运维应关注 shutdown summary 中的
`business_shutdown_registered`、`business_shutdown_attempted`、`business_shutdown_completed`、
`business_shutdown_failed`、`business_shutdown_timed_out`、`business_shutdown_panicked` 和
`business_shutdown_abandoned`。

业务停机 future 从登记接管到最终释放持续具有一次性析构隔离，覆盖尚在注册表、尚未执行和正在执行的项。
直接取消 Runner 不等同于优雅停机请求，不保证运行异步任务主体或返回退出报告；此时任务析构异常只输出
固定的 `application shutdown warning`，不截断其它任务释放，也不生成或追改 shutdown summary。
Runner 先同步关闭登记门，将 Starting/Ready 置为 Stopping，再沿实际激活栈逆序释放剩余 action、
停机任务和所属资源。Service 的 initializer 先于业务停机任务释放，Batch 的静态 initializer 晚于
业务资源释放；两种模式均保持停机任务先于业务资源，不取决于 Application 副本数量。
任务门已经出栈但尚未完成时仍然约束后续释放：存活的受监督 future 接管剩余栈和组件所有权，
最后一个 future 析构后才释放尾部；取消调用方不等待 join，也不创建额外异步清理任务。
延迟释放前立即关闭新资源借用并清除本实例全局入口，尾部析构不能发起新借用；没有存活任务时
在栈清理后撤销入口。不让出执行权的任务会同时延迟 abort 和依赖释放。已有 Stopped/Failed 保持不变，
保留的 Application 不再接受新资源借用，也不继续占用全局槽。
已借出的资源随借用归还释放；此前复制的外部客户端句柄不在同步撤销范围内。
Stopping 表示异步收尾结果未获确认。没有 summary 的直接取消不能按正常停机计数解释；持久副作用
是否完成仍需查询业务事实。

错误对象的 `Display`、`source` 和 `Drop` 的单次 panic 被独立隔离，不覆盖已经确定的退出语义。
重复错误链、超过 32 层或接收正文超过 16 KiB 会输出固定分类；不完整格式化正文不进入报告。
同步错误报告先脱敏，再对控制字符、Unicode 行/段分隔符、不可见格式字符和反斜杠进行可见转义，
普通多语言文本与错误链分隔信息保留。包含固定前缀和唯一末尾 LF 的单条报告最多 2 KiB，截断不拆开字符或转义。
采集端按物理行的固定前缀识别报告；正文中提及 `application shutdown summary:` 不构成新的摘要行。
所有者释放错误对象时若发生析构异常，另记 `application error release warning`，
不回写已经输出的 shutdown summary 计数。同步错误回调和析构必须有限时间返回；死循环、阻塞、
`panic=abort` 及同一次展开中的再次 panic 无法由这些边界隔离。

业务、组件和 initializer 受管资源共用逆登记清理路径；同步 future 创建、poll、future 释放和资源值析构的
单次展开式 panic 分别隔离，追加稳定次要失败后在剩余预算内继续，不覆盖首次原因或退出码。
原清理错误、超时与后续析构异常分别保留。已有借用未归还时不会强行关闭资源；外层取消或最后一个借用延迟
归还后的析构异常单独同步告警，不追写已发布摘要。同步回调和析构必须有限时间返回，非协作阻塞、
`panic=abort` 和同一次展开中的再次 panic 不在可隔离范围内。

initializer/component 局部资源清理仅撤销各自的 key，其它资源仍可由业务停机任务查找。业务任务结束后，
进入业务资源步骤才发布全局 `Closing`；排队取锁的查找会复验 key 和阶段，不能返回已经撤销的资源。
两类 `ShutdownAction` 的 label、future 创建、poll、future 析构和 action 析构分别隔离；label 展开时跳过
该 action 的 shutdown，错误与其它独立析构异常分别计入次要失败。全局预算耗尽后不再调用业务回调，
只逐项释放未执行 action，并在摘要前累计可观测的析构失败；直接取消时的异常单独告警，不追改历史摘要。

实际取得执行机会的任务会输出低基数 `debug` 事件。`shutdown_sequence` 与 active step 事件共享同一条
本次停机事件序列并单调递增，字段还包括
`shutdown_step=business-shutdown-task`、`shutdown_task`、`shutdown_priority`、`duration_seconds`、
`outcome` 和 `failures_added`。任务名不能放入租户、请求标识的动态值、凭据语义 token、URL、地址或其它高基数内容。
形态门禁不能证明实际业务基数，也不按普通身份词前缀猜测未知字母串；调用方必须使用固定业务名称，
不得拼接租户、请求或对象身份。错误正文按统一脱敏规则处理；赋值、连接词、括号、引号值和认证方案
在外层引用正文中同样生效，解析不会越过当前字符串的结束位置。Unicode 空白及全角冒号、等号与 ASCII
形式使用相同规则；没有明确值语法的普通诊断词保留。
Authorization 值中的 Basic/Bearer 凭据按完整引号内容隐藏；Digest 的逗号分隔参数列表整体隐藏，
不把内部引号或扩展参数当作公开字段。解析不越过外层 JSON 字符串或当前 header 行；无法区分的同一行
Digest 尾部字段保守一并隐藏，因此需要保留的诊断信息应使用独立 JSON 字段或下一行 header。
独立 PEM 私钥块不依赖外层字段名即可整体隐藏；明确私钥缺少匹配结束标记时隐藏剩余正文。
普通 PEM 证书与公钥不因块标记被隐藏。
敏感键比较忽略 Unicode Format 类、组合字形连接符和变体选择符，并采用可完全分解为 ASCII 字母数字的兼容字形；
脱敏阶段只替换原文对应值区间，随后同步报告出口执行上述单行编码。兼容逗号、引号等标点不会形成新的值结束边界，
不承诺识别任意视觉混淆字符。
任务名直接拒绝上述不可见字符及 Unicode 行、段分隔符，连续四位 Unicode 十进制数字（含混合书写系统）同样拒绝；
可见多语言名称、普通组合音标和短固定编号可用。

## 常见启动故障

| 现象 | 优先检查 |
| --- | --- |
| 找不到主配置 | 工作目录与 `zcf/application.yml` 是否随产物部署 |
| 无 Web 后进程在 Hook 返回后退出 | `application.mode` 是否显式为 `service` |
| 配置反序列化失败 | 环境键层级、字段类型和冲突键 |
| 声明组件但提示 feature 缺失 | 业务 manifest 是否开启对应门面 feature |
| listener 启动超时 | 地址、端口和业务就绪屏障 |
| 数据库或缓存失败 | 脱敏目标主机、端口、模式和超时 |
| 可靠 Saga client 提示数据源冲突 | `saga.client.datasource_ref` 与显式 `outbox.datasource_ref` 必须相同；只需轮询预算时省略后者 |
| Ready 后很快退出 | 首个 critical task 错误 |
| SIGTERM 后超过预算 | 未 join 任务、长期资源借用、无限流式响应或阻塞析构 |

## Saga 值班

可靠 client 的启动诊断若同时指出 `saga.client.datasource_ref` 和 `outbox.datasource_ref`，先统一
写入与扫描的事务域，再重新启动；给另一个库建同名表不能消除配置冲突。受管可靠计划固定绑定 client
指定库，省略 Outbox 数据源或仅配置预算不会改变扫描目标。

运行期应区分本地已受理、事件已投递和远端流程完成。`enqueue_start` 返回后还需等待外层事务明确
提交才能表示本地已受理；Ready 200 与业务 202 都不证明远端已完成。远端不可用或收据丢失时保留原
事件身份，结合下列指标及远端实例状态确认恢复，不手工标记 dispatched 或删除 intent。

参与方 command 路由缺席但存在已提交 result 时，先区分 command readiness 与结果恢复资格。共享
Catalog、冻结 definition 或结果 producer 信任任一无效都会返回可重投裁决；发送端不得 ACK、改写
event ID 或计入永久隔离。仅 route/健康缺席且结果证据仍有效时，原事件应继续收敛，但应用整体仍可
保持 NotReady。安全材料发布期间的在途请求可能因 generation 变化回滚，等待新代确认后使用同一事件
重投；不要通过清理 Inbox、gate 或 Outbox 消除积压。

先查看 `nasaga_manual_intervention`、`nasaga_waiting_resolution`、`nasaga_due_timer`、
`nasaga_conflict_total`、`napp_outbox_pending`、`napp_outbox_dead`、`napp_outbox_published_total`、
`napp_outbox_failed_rounds_total`、`napp_outbox_retention_commit_uncertain_total`、
`nasaga_quota_rejections_total`、`nasaga_action_rate_rejections_total`，再关联 Kafka 的
`nasaga_kafka_command_*` / `nasaga_kafka_result_*`、Redis Streams 的 `napp_saga_stream_*` 或具体
HTTP transport 的认证/replay/DLT 指标。gRPC 受管 listener 使用 `napp_grpc_*` 观察连接、TLS、方法准入
与最终结局；四类 Saga 收据及其 Outbox/DLT 裁决由具体 publisher 的低基数业务指标补充，不能从
transport `ok` 推断 Saga 已提交。

- `nasaga_http_*_replay_authentication_failed_total` 上升：核对凭据、时钟和报文完整性；
- `nasaga_http_*_replay_rejected_total` 上升：追踪重复来源，不清空仍有效 nonce；
- `nasaga_http_*_replay_capacity_rejected_total` 上升：隔离异常 producer/path，并核对该信任边的容量预算；
- `nasaga_http_command_dlt_total` 上升：核对原 envelope、冻结定义摘要和部署快照；
- `napp_outbox_pending` 非零且发布量不增长：核对首个未确认事件、唯一 publisher 路由和下游确认；
- `napp_outbox_dead` 非零：按已批准的毒丸策略处理，不得直接删除事件绕过顺序；
- `napp_outbox_retention_commit_uncertain_total` 上升：先查数据库候选和处置收据，允许下一轮按持久
  事实收敛；不得按本轮返回值补记删除，也不得立即重写归档；
- `napp_saga_stream_deleted_pending_total` 非零：entry 在确认前被外部删除，立即停止清剪并核对全部
  group 的 PEL/frontier；不得用空 PEL 推断业务已处理；
- `napp_saga_stream_oldest_pel_age_ms` 持续增长：检查 consumer 存活、XAUTOCLAIM、handler 预算、
  DLT/marker 同槽与 Redis ACL；不要直接 XACK；
- `napp_grpc_rpcs_rejected_total` 上升：按固定 `reason` 区分连接/进程/方法并发、方法速率和 peer identity，
  先核对 listener 容量与 mTLS principal 绑定，不把准入拒绝当作业务确定性拒绝；
- `napp_grpc_rpcs_completed_total` 中 deadline、transport 或 stream 结局上升：关联同一时段 Outbox 积压
  与 publisher 收据指标；回包缺失和 `Retryable` 必须保留原事件重投；
- `nasaga_quota_rejections_total` 上升：区分正常租户上限和账本未初始化；精确用量只经受权管理查询，
  不给 Prometheus 增加 tenant label；
- `nasaga_action_rate_rejections_total` 上升：核对单租户恢复动作流量、数据库窗口和审批；只读检索不应
  消耗预算，不能通过绕过管理入口执行状态写入；
- Unknown 积压：检查 typed resolver、外部事实和 resolution budget，不能手改状态；
- timer fencing 丢失：旧 worker 已失权，停止推进，由新 owner 重新领取。

人工恢复先读取 attempt、迁移、控制、管理和冲突审计，再沿冻结计划使用稳定
`operation_id/effect_id` 操作。`manual_close` 只用于外部人工事实已经完成且全部副本通过终态兼容门禁的
场景；完成后核对实例离开非终态集合、`nasaga_manual_intervention` 回落并且
`nasaga_manually_closed_total` 增加一次。禁止删除 Inbox、participant gate 或 DLT 来消除告警。

部署边界见 [应用部署指南](deployment.md)，Saga 细节见 [Saga 生产运行指南](saga-production.md)。
