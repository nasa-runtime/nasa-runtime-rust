# Saga 生产运行指南

本文描述 `nasaga-core`、`nasaga-mysql`、`nasaga-runtime` 与 `nasaga-macro` 的生产接线、故障收敛和
值班边界。Saga 的一致性基础是本地事务、Outbox 至少一次投递、Inbox 幂等、持久化状态机和显式补偿；
它不提供跨服务 ACID 或并发隔离。

## 部署拓扑

每个 Orchestrator 副本运行相同的流程定义集合，并连接同一 Orchestrator MySQL 主写端。每个参与方
拥有独立数据库，业务事实、participant gate、Inbox 和 result Outbox 位于同一事务域。

当前内置的托管消息适配器使用 Kafka，并至少提供 command、result 和 DLT 三类 topic：

- command 按业务聚合键分区，由 Orchestrator Outbox 发布，参与方消费；
- result 按 `saga_id` 分区，由参与方 Outbox 发布，Orchestrator 消费；
- DLT 保存原 topic、partition、offset、group、投递次数和稳定原因码。

候选拓扑应使用多 broker、副本因子与最小同步副本约束，关闭 unclean leader election。生产者身份、
消费者组和运维主体使用独立凭据，ACL 默认拒绝，仅开放所需 topic 与动作。

Saga 核心不依赖 Kafka。Outbox 发布端是 provider-neutral 接口，HTTP 或 Redis Streams 都可以承载
command/result。Redis Streams 接入必须补齐消费组读取、稳定消息身份、ACK、pending reclaim、消费者
失联后的 `XAUTOCLAIM`、先落 DLT 后 ACK、积压观测和优雅停机；只把 `XADD` 当作发布成功而没有对应
消费闭环，不满足生产合同。当前仓库提供 Kafka 托管消费适配器，Redis Streams 需要实现等价 connector，
不能仅替换配置键。

## 启动顺序

使用 Application 托管时只需声明 `#[nasa::application("saga", "web")]`；Saga 会隐式加入 DB 与
Outbox。选择内置 Kafka transport 时再增加 `"kafka"`，不使用 Kafka 时不会建立 broker 连接。UserHook
通过 `configure_saga` 一次提交运行角色和发布端，宏固定 `db -> saga -> transport -> outbox -> web`
顺序；只承载参与方时不会启动 Orchestrator timer。

1. 应用读取受信、不可变的流程定义快照。
2. 数据库迁移完成，连接池与所选 transport 建立。
3. Orchestrator 注册所有仍有活跃实例的定义，并执行 `verify_startup`。
4. 参与方冻结 `ParticipantCommandTrust`，将 workflow、`definition_version`、摘要、步骤 owner 与
   Orchestrator 身份精确绑定。
5. timer worker 获取逐副本唯一且重启稳定的 owner；运行实例创建私有随机 nonce。
6. 消费循环、Outbox dispatcher 和需要的 timer poller 就绪后，最后开放业务路由。

任一步缺少定义、摘要不一致、凭据无效、数据库结构不完整或信任投影为空，都必须拒绝进入 Ready。
需要在启动钩子先创建隔离库时可选择 `database_bootstrap=user_hook`。DB 组件仍在 Start 阶段预占关闭
所有权，Ready 前接管默认池并完成探针；停机必须先收割 timer、dispatcher 等受监督任务，再关闭连接池。

## 本地事务与 ACK

Orchestrator 每次推进在同一 MySQL 事务中完成 Inbox claim、attempt journal、实例 CAS、迁移事实、下一
command Outbox 和 durable timer。参与方在同一事务中完成 Inbox、gate、业务事实和 result Outbox。

ACK 规则固定如下：

- COMMIT 明确成功后 ACK；
- Inbox 明确返回 duplicate 时 ACK；
- 回滚成功且错误可重试时保留 offset，并按有界退避重投；
- COMMIT 或回滚结果不确定时保留 offset，持续重投直到由 Inbox 和事实表收敛；
- 确定性协议拒绝先提交 DLT，DLT 成功后才允许推进源 offset 或 Outbox。

DLT 不可达时不得跳过毒消息，也不得提交同分区后续 offset。恢复后先补交 DLT，再恢复正常推进。

## 状态推进与补偿

状态迁移只由 Orchestrator 裁决，参与方只返回类型化业务结果。`effect_id` 跨 attempt 稳定，目标系统
以它作为幂等键；`command_id` 只标识单次投递。

- 确定失败进入逆序补偿。
- Unknown 进入有界 resolve，不直接按失败补偿。
- resolve 应答到达时重新读取已提交步骤投影；正向或补偿已经成功的事实优先于旧查询应答，原始应答
  继续保留在 attempt journal，后续动作仍服从该步骤的超时策略。
- 超时先进入取消屏障；只有真实取消或裁决结论允许继续。
- 不可补偿 pivot 只能位于正向流程末端。
- 补偿计划冻结后不能插入新成员；计划外迟到成功转为人工介入。
- `HALTED` 不会被普通 timer 或新 attempt 自动解除。

人工重开补偿或裁决必须使用已认证管理主体、唯一 `operation_id`、权限、actor 和 reason。PAUSED 状态
下先恢复控制态，再使用新的 operation 发起后续动作；不能直接修改实例状态、删除 Inbox 或改写 journal。

## 参与方信任

所有 phase 入口都接收已认证 producer，不暴露可由调用方自报身份的旁路。授权过程按以下顺序执行：

1. transport 校验 topic owner、mTLS 或消息签名；
2. replay 组件原子占用 nonce；
3. runtime 重新派生并核对 envelope 身份；
4. `ParticipantCommandTrust` 精确匹配 workflow、定义版本、摘要、步骤和 Orchestrator；
5. 进入 Inbox 与 participant gate 事务。

HTTP 类 connector 的签名覆盖 producer、固定 path、时间戳、nonce 和原始正文。多副本入口必须使用共享
强一致 nonce claim 或等价认证网关；进程内 replay guard 不能承担跨副本重放保护。每条 producer/path
信任边使用独立认证器、容量和指标，避免某一来源耗尽其它来源或恢复动作的配额。

## Timer fencing 与多副本

timer 领取由数据库租约、owner、运行实例 nonce 和不可复制的 fencing capability 共同约束。
`claim_due_timers` 消费 capability，返回的 `TimerClaimBatch` 独占该批次权威；完成或交还只能借用批次
持有的 token。

在执行 timer 副作用前必须重新核对 owner、fencing token、租约期限、实例版本和 timer generation。
任何一项失配都表示当前 worker 已失权，必须立即停止，不得写迁移事实或发布 Outbox。两个
Orchestrator 并发竞争同一实例时，只有数据库 CAS 胜者能够推进。

## 数据库迁移

生产环境按 [Saga 数据库迁移](../nasaga-mysql/migrations/README.md) 的固定顺序执行 SQL。排序规则转换
可能重建大表，应在目标规模副本上确定 metadata lock、复制延迟、磁盘余量与完成时间预算。

每个承载 `outbox_event` 的 Orchestrator 或参与方数据库还必须执行
[Outbox 数据库迁移](../naoutbox-mysql/migrations/README.md)。`idx_dispatchable (dispatched, dead, id)`
保证待投递计数覆盖读取，并让领取查询只定位真实候选后按主键回表，不会反复扫描本地死信；
`idx_dead (dead, id)` 保证指标抓取不随历史已投递行数退化。两项索引必须在开放 dispatcher 与指标
抓取前完成。

参与方摘要列采用两阶段封口：先添加可空列并从受信定义记录回填，再添加格式 `CHECK` 和 `NOT NULL`。
摘要来源不唯一、影响行数异常或残留非法值时停止发布。历史请求无法重建的摘要保持未知并由运行时
fail-closed，不能为了通过启动检查伪造数据。

## 容量与背压

容量计划至少覆盖：

- MySQL 业务请求、消费事务、Outbox、timer 和管理查询的连接峰值；
- 所选 transport 的正常流量、retry storm、DLT 积压和后端故障后的追赶速率；
- 每条认证信任边在完整 replay 窗口内的 nonce 数量；
- Inbox、journal、gate、Outbox、DLT 和审计表在 replay horizon 内的存储增长；
- timer 与 dispatcher 的批量上限、轮询频率、执行耗时和停机排空预算。

Outbox 已投递行和本地死信的保留期、归档目标、分批清理上限与清理失败告警必须由数据治理负责人批准。
不得清理仍未确认的普通事件，也不得让大批删除与 Saga 控制面事务争用锁和日志空间。

本地容器结果只能证明故障语义，不能代表候选硬件容量。峰值、余量、告警阈值和恢复时间目标需要由
部署负责人基于目标拓扑批准。

## 指标与告警

`SagaOperationalMetrics::render_prometheus` 输出固定、低基数指标。指标和日志不得包含租户、业务键、
payload、凭据、完整 reason、effect id 或 command id。

至少关注以下信号：

- Unknown、Manual、HALTED 和 conflict 持续增长；
- due timer、Outbox 或 DLT 积压超过处理预算；
- Outbox 有积压但发布计数停止增长，或 dispatcher 失败轮次持续增加；
- COMMIT/回滚结果不确定；
- producer 认证、replay 或 capacity 拒绝；
- timer fencing 丢失、CAS 冲突异常升高；
- transport 分区或消费组停滞、MySQL 复制延迟和连接池耗尽。

告警规则位于 [Saga Prometheus rules](alerts/saga-prometheus.yml)。值班路由需要区分业务拒绝、认证异常、
容量耗尽和基础设施不可达，不能用统一重启掩盖根因。

受管 Saga 默认使用 `Block` 毒丸策略：首个未确认事件会阻止同一 `outbox_event` 表中所有后续事件，
以免网络瞬态错误被次数预算误判为可越过的业务 command/result。若审计或其它领域事件写入同一张表，唯一
publisher 必须能路由并可靠确认表内全部事件类型；任一类型永久失败的停摆半径都是整张表和该数据库承载的
全部 Saga。无法共享发布合同的事件必须使用独立事务数据库和独立受管 Outbox。通用 `DeadLetter` 只适用于
已经批准允许越过失败事件的独立事件流，不能作为 Saga 的默认恢复手段。

## 停机与恢复

正常停机按以下顺序执行：关闭业务入口，停止领取新 timer，停止拉取新消息，等待已接管事务结束，
刷新 Outbox 与指标，最后关闭 transport 和数据库连接。超出停机预算时保留 durable 事实，由新副本接管，
不应强制确认未完成消息。

灾难恢复需要保存 MySQL point-in-time recovery、transport frontier 与保留策略、流程定义快照、ACL、
密钥材料和告警配置。恢复后先隔离业务入口，核对数据库时间点、定义摘要、consumer group frontier、
Outbox/DLT 积压和 timer 租约，再逐步恢复消费与写流量。

## 生产批准边界

代码仓库提供协议、安全不变量和本地故障场景；以下结论必须由实际部署环境负责人签署：

- 候选 MySQL 副本拓扑、提升流程、备份恢复点与数据丢失目标；
- Kafka broker 拓扑、副本配置、ACL、凭据轮换和跨可用区故障策略；
- 目标峰值、积压清空速率、连接池与存储余量；
- 在线 DDL 维护窗、回退方案和审批记录；
- 灾难恢复演练结果、恢复时间目标和值班升级链路。

在这些批准完成前，可以确认实现具备生产所需的安全闭环，但不能声明目标环境容量或灾备结果已经成立。
