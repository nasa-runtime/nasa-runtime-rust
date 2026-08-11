# Saga 生产运行指南

本文描述 `nasaga-core`、`nasaga-mysql`、`nasaga-runtime` 与 `nasaga-macro` 的生产接线、故障收敛和
值班边界。Saga 的一致性基础是本地事务、Outbox 至少一次投递、Inbox 幂等、持久化状态机和显式补偿；
它不提供跨服务 ACID 或并发隔离。

## 核心价值与组件边界

Saga 面向“一个业务意图需要跨多个本地事务完成”的场景。它不尝试锁住所有服务，而是持久化每一步的
业务效果身份、尝试、裁决和补偿事实，使重复投递、结果未知、进程退出和多副本接管都能从同一事实源
继续。使用者获得的是可恢复、可审计的最终一致性流程，不是远程调用的透明事务包装。

| 组件 | 公开职责 | 不承担的职责 |
| --- | --- | --- |
| `nasaga-core` | definition、摘要、身份、typed outcome、封闭状态机、冻结补偿计划 | I/O、线程、连接、重试循环 |
| `nasaga-mysql` | 实例/attempt/journal/timer/gate/审计/配额事实、CAS 与 fencing | 跨库事务、transport、业务裁决 |
| `nasaga-runtime` | Orchestrator、参与方事务 wrapper、恢复管理、trace、调度批次和 connector 裁决 | Application listener、部署 ACL、业务资源并发控制 |
| `nasaga-macro` | `#[saga]` 声明检查、descriptor 收集、类型化 adapter | 全局流程定义、运行期状态推进 |
| `napp` / `nasa` | Ready 门禁、组件所有权、timer/consumer/Outbox 监督与门面重导出 | 替代本地事务或自动选择 transport |

```text
                  Orchestrator 本地事务域
start/result ──→ Inbox + instance CAS + attempt/journal + timer + command Outbox
                                                               │
                                          at-least-once transport
                                                               │
                  参与方本地事务域                              ▼
command ─────→ Inbox + participant gate + business fact + result Outbox
```

跨越 transport 的消息允许重复；`effect_id`、Inbox 和业务目标系统幂等键吸收重复。任何文档、配置或
connector 若跳过这两个本地事务域，都不属于本产品合同。

## 部署拓扑

每个 Orchestrator 副本运行相同的流程定义集合，并连接同一 Orchestrator MySQL 主写端。每个参与方
拥有独立数据库，业务事实、participant gate、Inbox 和 result Outbox 位于同一事务域。

选择 Kafka 托管适配器时，至少提供 command、result 和 DLT 三类 topic：

- command 按业务聚合键分区，由 Orchestrator Outbox 发布，参与方消费；
- result 按 `saga_id` 分区，由参与方 Outbox 发布，Orchestrator 消费；
- DLT 保存原 topic、partition、offset、group、投递次数和稳定原因码。

候选拓扑应使用多 broker、副本因子与最小同步副本约束，关闭 unclean leader election。生产者身份、
消费者组和运维主体使用独立凭据，ACL 默认拒绝，仅开放所需 topic 与动作。

Saga 核心不依赖 Kafka。Outbox 发布端是 provider-neutral 接口，HTTP、Kafka 或 Redis Streams 都可以
承载 command/result。当前仓库提供两个托管消费适配器:Kafka(`saga-kafka`)与 Redis Streams
(`saga-redis-stream`,见下文"Redis Streams 受管接入")——后者已具备消费组读取、稳定消息身份、
显式 ACK、pending reclaim、`XAUTOCLAIM` 重领、先落 DLT 后 ACK 的原子脚本、积压观测与优雅停机。
仍然成立的边界:只把 `XADD` 当作发布成功而没有对应消费闭环,不满足生产合同;选择哪个 transport
是业务的显式声明,不能仅替换配置键。

| transport | 发布确认 | 重投/重领权威 | 确定性拒绝 | 产品状态 |
| --- | --- | --- | --- | --- |
| Kafka | broker 确认 + consumer 手动 ACK | partition offset 与 Inbox | 先持久化 DLT，再提交 offset | 稳定受管 connector |
| Redis Streams | XADD 确认 + XACK | PEL、XAUTOCLAIM 与 Inbox | Lua 先写 DLT，再 marker，最后 XACK | 稳定受管 connector |
| HTTP | 业务定义的明确收据 | 发布端 Outbox 与共享 nonce/Inbox | 必须有 durable DLT；认证构件由 runtime 提供 | 宿主自行拥有 listener 与循环 |
| gRPC | `Committed` / `Duplicate` 封闭收据 | 发布端 Outbox 与 Inbox | `DeterministicReject` 交发布策略裁决 | 实验 connector，不含 listener |

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

### 配置面与冻结时机

- `saga:` YAML 只控制 Application 的 timer 轮询、退避、单轮上限、连续失败摘流阈值和数据库引导时机；
  未知字段、零值和越界值在启动期拒绝。
- workflow definition、`OrchestratorConfig`、参与方信任、tenant quota/action rate 和 transport 路由由
  业务从受信配置构造，在 UserHook 提交后冻结，不属于可随远端 overlay 热切的运行参数。
- timer owner、consumer identity、producer owner、HMAC key id 和路由身份都是控制权威，必须来自
  部署身份或受信配置；不能从 payload、自报 header 或随机进程默认值推断。
- `database_bootstrap=user_hook` 只服务先建隔离库等受控场景，仍要由 DB 组件在 Ready 前接管连接、
  migration 和关闭所有权，不能用来绕过结构检查。

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

## 链路追踪

Saga 发起入口可显式接收已校验的 W3C `TraceContext`（`start_saga_traced`），canonical
`traceparent` 随实例同事务持久化；此后每条自动命令（含 timer 与崩溃恢复发出的）都从已提交实例
派生同 trace-id 的新 child span 写入命令 Outbox，经 transport header 传给参与方。参与方结果
事件从命令收据派生子上下文；Orchestrator 消费已认证结果时把收据 trace 在同一推进事务内落库为
实例最新因果上下文。框架不读取任何 ambient trace 状态——缺少上下文时保持 `None` 并正常投递，
trace 是观测面而非投递前置条件。trace 属性不携带业务 payload、完整业务键或凭据。

## 调度驱动的发起

周期性对账、清算与批量处置经集群调度器触发：只有持有领导权且取得该名义调度时刻 FireLog
claim 的副本发起批次。leader-only 只是触发门禁而非 exactly-once——真正的去重由
`任务名 + 名义调度时刻 + 对象稳定身份` 派生的业务幂等键（`derive_scheduled_business_key`）
与创建请求 canonical 摘要共同承担：重复触发被唯一键吸收为同一实例，同 key 摘要漂移
fail-closed，不把另一批输入伪装成重复。批次内数量与时间预算有界
（`start_scheduled_batch`），未处理对象由下一次触发对已提交业务事实的重扫继续，进程内不
保留跨周期状态；失去领导权立即停止领取新项，已提交创建的实例由 Saga 自身推进。claim 成功
但进程崩溃时，同样由重扫补漏，不依赖内存批次续跑。

## 人工介入的处置闭环

- **定位**：`list_instances`（权限 `saga.instance.list`，与全部写动作分离）按租户 + 状态 +
  创建时间窗做 keyset 分页检索，响应只含身份、状态、当前步骤、版本、稳定原因码与时间戳，
  不回显业务 payload；跨租户实例的存在性不经该入口泄漏。
- **处置**：系统外完成人工处理后，用 `manual_close`（权限 `saga.manual_close`）把
  `MANUAL_INTERVENTION` 实例关闭为终态 `MANUALLY_CLOSED`。动作要求已认证主体、强制原因与
  一次性 operation id，审计与状态迁移同事务提交；同一 operation 重放幂等返回，不产生第二次
  迁移。关闭只表达"自动化已由人工关闭"，不得也无法写成 `COMPLETED`/`COMPENSATED`；未完成的
  补偿计划保留为快照。
- **滚动升级门禁**：`MANUALLY_CLOSED` 一旦落库不可回退，旧二进制读到未知状态会按数据损坏停止
  推进。上线顺序固定为：先把**全部**副本升级为可解析该状态的版本（`enable_manual_close`
  默认关闭，即"可读不产生"读者），确认无旧副本后再显式开启该配置放行管理动作。
- **闭环信号**：处置后实例离开非终态集合，`nasaga_manual_intervention` gauge 回落、对应告警
  自动解除，`nasaga_manually_closed_total` 计入一次；这是判断处置真正闭环的客观信号，
  不要只看管理动作返回成功。

受管 Saga 默认使用 `Block` 毒丸策略：首个未确认事件会阻止同一 `outbox_event` 表中所有后续事件，
以免网络瞬态错误被次数预算误判为可越过的业务 command/result。若审计或其它领域事件写入同一张表，唯一
publisher 必须能路由并可靠确认表内全部事件类型；任一类型永久失败的停摆半径都是整张表和该数据库承载的
全部 Saga。无法共享发布合同的事件必须使用独立事务数据库和独立受管 Outbox。通用 `DeadLetter` 只适用于
已经批准允许越过失败事件的独立事件流，不能作为 Saga 的默认恢复手段。

## Redis Streams 受管接入

Saga 的 Redis Streams consumer 可交给 Application 生命周期托管
(`SagaApplicationPlan::with_redis_stream_transport`,组合声明须含 `redis` 角色,建立顺序
`DB -> Redis -> Saga -> Outbox`、停机逆序)。Ready 前用真实客户端统一探测:PING、消费配置合同、
group 幂等创建(兼 ACL 探测),任何失败拒绝 Ready。消费循环由 Runner 监督,任一循环异常退出
触发统一停机;停机语义固定为**先关领取**(不再发起新的 XREADGROUP/XAUTOCLAIM)、在途轮次内
已接管消息由单轮预算排空(handler 完成或超时留 PEL)、未确认消息留在 PEL 交由重启后重领,
Redis 连接最后由 Redis 组件释放。`(stream, group, consumer)` 身份在计划内必须唯一,同一身份
双循环会互相踩踏 PEL。运行指标按冻结的 (stream, group) 低基数导出
(`napp_saga_stream_*`);`napp_saga_stream_deleted_pending_total` 非零表示 entry 在确认前被
外部删除,必须告警。

Cluster 部署要求 stream/DLT/marker 同 hash tag(构造期校验),客户端跟随 MOVED/ASK;上线前必须在
候选多主拓扑核对在线槽迁移期间的 MOVED/ASK 跟随与消息收敛。安全清剪
`safe_trim_by_group_frontier` 的证据采集与删除在**一条 Lua 脚本内原子执行**:按全部 group
的 last-delivered 与 PEL 最小 id 取全体最小值做 MINID;任一 group 证据不完整整轮零删除,
PEL 非空而最小 pending id 不可读同样零删除,原子性使"快照与删除之间新建回放 group"的
窗口按构造不存在。消息签名覆盖 stream、事件身份、payload 与 `traceparent`(存在标志区分
缺失与空值):持有流写权限但没有密钥的主体无法通过替换 trace 伪造因果链。DLT 脚本按
"先落死信、再置 marker、最后 XACK"排列:死信未持久化前源消息绝不确认。

## gRPC 收据 connector（实验）

`saga-grpc-experimental` 提供 `SagaGrpcCommandServer`、`SagaGrpcResultServer`、
`SagaGrpcPeerIdentity`、`SagaGrpcReceipt` 与发布端收据映射；它不生成 protobuf service，也不启动
listener。业务 generated service 必须先从已验证 mTLS 证书或端到端签名得到 peer identity，再把原始
envelope 与显式 trace 交给裁决器。metadata 中自报的服务名不可信。

收据只有四类：`Committed`、`Duplicate`、`DeterministicReject`、`Retryable`。deadline、断连、回包
丢失和显式 `Retryable` 都是结果不确定，发布端保留 Outbox 行并以同一 `event_id` 重投；只有前两类
允许标记已投递。确定性拒绝能否越过由发布端已经批准的 Block/DLT 策略决定，connector 本身不删除
Outbox 行。只有同时具备受管 listener、有界并发/payload/deadline、mTLS 或等价签名、优雅 drain 与
真实下游门禁后，具体业务链路才能申请生产批准；实验 feature 本身不是这份批准。

## 多租户配额

`TenantId` 贯穿实例身份与管理面鉴权,可在此基础上限制每租户在飞实例。配额分两档:未列入
`OrchestratorConfig::tenant_quotas` 的租户"只观测不拒绝"——账本仍在创建/终态事务内精确记账,
但不设上限;列入的租户在创建事务内原子预留(`in_flight < cap` 条件自增,并发在租户行锁上串行,
无锁计数不会穿透上限),超限以稳定原因码 `saga_tenant_quota_exceeded` 拒绝新建。配额只作用于
创建:已在飞实例不受影响,实例进入任一终态时同事务释放名额。拒绝与系统故障可区分,错误不携带
其它租户用量;`nasaga_quota_rejections_total` 只导出拒绝总数、不带租户标签,精确用量只经
`tenant_quota_usage`(权限 `saga.audit.read`)返回。账本漂移由 `reconcile_tenant_quota` 按已提交
非终态事实有界收敛,不在请求路径扫描整表;对账必须在事务内调用,经账本行锁与在线路径串行化。本项与 Outbox 多通道分片是同一问题的两个层次:通道
分片限制故障扩散,配额限制资源挤占;只做其一时剩余风险应明确。

### 管理动作速率

变更类管理动作(pause/resume/retry-compensation/retry-resolution/manual-close)会向恢复通道注入
外部副作用;单租户刷重试类动作不能挤占其它租户的恢复能力。`OrchestratorConfig::tenant_action_rates`
按租户配置固定窗口速率(`max_actions`/`window_ms`,`max_actions = 0` 表示完全封禁),窗口边界由
**数据库时钟**对齐,多副本共享同一套窗口。预算与动作事务同提交:失败回滚退还、幂等重放计数;
超限以稳定原因码 `saga_tenant_action_rate_exceeded` 拒绝且动作零副作用。只读动作(检索、审计、
用量查询)不占预算。未配置的租户不限速也不写账本。`nasaga_action_rate_rejections_total` 只导出
拒绝总数;当前窗口用量只经 `tenant_action_rate_usage`(权限 `saga.audit.read`)返回。
配置了速率的租户,其全部变更类管理动作会在同一行账本上串行化——这是限速的固有代价,
只应由显式选择限速的租户承担;单个动作的耗时因此直接决定该租户管理面的实际并发。

### Outbox 在飞事件配额与受信写入上下文

`outbox_event` 携带受信租户归因列:租户身份只能由已认证业务上下文经
`append_transactional_with_context(OutboxWriteContext, ...)` 写入,绝不从 payload、`aggregate_id`
或自报 header 解析;未携带上下文的历史 append 路径固定映射 `system` 租户。Saga 的 command/result
append 已填实例租户。

每租户在飞事件配额是进程级冻结的显式 opt-in(`install_outbox_tenant_quotas` /
napp `OutboxApplicationPlan::with_tenant_quotas`):列出的租户在受信 append 事务内原子预留
(拒绝时事件行从未写入、业务回滚时预留退还),投递标记与死信裁决在同一条语句/事务内释放——
行离开可投递集合的瞬间账本回落,不产生"已投递仍占额"的幽灵占用;不反向更改已受理事件的投递
命运。超限以稳定原因码 `outbox_tenant_quota_exceeded` 拒绝。未列出的租户不记账不设限——账本
行锁会把同租户并发 append 串行化,这是配额的固有代价,只应由显式选择配额的租户承担。
**未启用配额的部署零足迹**:投递标记与死信裁决保持单表原子语句,不引用配额账本,也不
依赖该表存在;`outbox_tenant_quota` 只是启用方的结构前置,由受管组件在 Ready 前校验。
**存量库启用纪律**:两侧账本都带初始化标记,仅事务内对账置位;给租户配置上限前必须先在
全部写入方运行记账版本之后的受控窗口执行一次对账,把存量非终态实例/待投递行入账——未
置位时预留与启动预检都按部署错误拒绝,存量事实的终态/投递释放才不会扣掉新账本名额。

发布失败按封闭类别贯通到投递裁决:确定性拒绝(Terminal)才进入死信预算;瞬态与结果
不确定(Transient——gRPC Retryable/deadline/断连/回包丢失、Redis XADD 往返失败)保留
重投、不消耗预算、永不自动标死。死信保留期以 `dead_at`(进入死信的时刻)为唯一时间
依据,历史死信行 `dead_at` 缺失时永不进入清理候选;每条被清理的死信在删除事务内写入
`outbox_dead_disposal`(批准标识+收据身份),人工处置可事后复验。**把某
租户纳入配额前,该租户的全部写入必须已改走受信上下文入口**,否则释放路径造成的账本漂移只能由
`reconcile_outbox_tenant_quota` 有界对账收敛(须在事务内调用,经账本行锁与在线路径串行化)。

## 停机与恢复

正常停机按以下顺序执行：关闭业务入口，停止领取新 timer，停止拉取新消息，等待已接管事务结束，
刷新 Outbox 与指标，最后关闭 transport 和数据库连接。超出停机预算时保留 durable 事实，由新副本接管，
不应强制确认未完成消息。

Outbox 保留清理在提交前步骤使用单轮 deadline；删除 `COMMIT` 发出后等待数据库应答，不由正常受管
轮次自行取消。若连接在提交后中断，`napp_outbox_retention_commit_uncertain_total` 记录结果不确定，
该轮不虚增已确认删除数，也不刷新最后成功时刻；下一轮以持久候选事实继续收敛。全局停机 deadline
仍拥有最终进程收割权，shutdown report 必须保留未优雅收束的证据。

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

## 明确不解决的问题

- 不提供跨服务 ACID、全局锁、快照隔离或读者不可见的中间状态。
- 不提供物理 exactly-once；业务目标系统仍须按稳定 `effect_id` 幂等。
- 不从业务异常文本猜测 Terminal/Transient，也不从 payload 猜测 producer、tenant 或 route。
- 不自动生成补偿业务逻辑；补偿只是另一项需要幂等、鉴权和容量预算的真实业务操作。
- 不用本地容器结果替代候选拓扑容量、真实 ACL、在线 DDL、备份恢复或灾难恢复批准。
