# 应用运维指南

本文说明应用模式进程的日常观测、配置刷新、停机语义和故障处置。业务端口、下游依赖和数据恢复
流程由对应项目手册补充。

## 运行状态

```text
Bootstrap → Starting → UserHook → Ready → Running → Stopping → Stopped
                                                        └────→ Failed
```

- `Ready` 只在组件启动、业务 Hook 成功、资源封存和必要 listener 绑定后提交。
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
Runner 先同步关闭登记门，再在锁外释放注册表中的任务，不等待 UserHook 或任务捕获的 Application
副本全部销毁。没有 summary 的直接取消不能按正常停机计数解释；持久副作用是否完成仍需查询业务事实。

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
| Ready 后很快退出 | 首个 critical task 错误 |
| SIGTERM 后超过预算 | 未 join 任务、长期资源借用、无限流式响应或阻塞析构 |

## Saga 值班

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
