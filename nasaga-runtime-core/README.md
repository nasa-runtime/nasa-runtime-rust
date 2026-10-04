# nasaga-runtime-core

`nasaga-runtime-core` 是 MySQL 与 PostgreSQL Saga 包装共同使用的唯一运行状态机。Orchestrator、
Participant、definition registry、durable timer、恢复、补偿、管理动作、认证 envelope 与投递裁决都在
本 crate 实现；数据库包装只提供 `SagaBackend`，不会复制一份按后端分叉的流程逻辑。
结果恢复 API 支持宿主冻结并持续复验同一次动态资格，使已经提交的参与方事件在 command 路由缺席时
仍能收敛，同时保持新业务入口关闭。

## 运行不变量

- Orchestrator 在一个本地事务中提交 Inbox、实例 CAS、journal、timer 与 command Outbox；参与方在另一个
  同源事务中提交 Inbox、gate、业务事实与 result Outbox。
- `effect_id` 标识跨 attempt 稳定的业务效果，`command_id` 标识一次投递；重复 envelope 由 Inbox 和
  participant gate 吸收。
- 无法确认的外部效果进入 resolve 或人工介入，无法确认的数据库提交不会降级为普通重试。
- timer 领取、续租、完成和交还由 store 提供的 fencing capability 约束；失权 worker 不能继续推进。
- 定义摘要、参与方 owner、producer 信任和消息身份在 Inbox claim 前复验，越权消息不能占用去重键。

本 crate 不依赖 SQLx driver，不创建连接池，也不负责 schema。数据库行模型与能力来自
`nasaga-backend`；MySQL 包装位于 `nasaga-runtime`，PostgreSQL 包装位于 `nasaga-runtime-pgsql`。

`Orchestrator::start_saga_authorized_traced` 接受宿主提供的同步执行资格检查，在使用 definition 快照前、
创建事务每次恢复执行时和交给事务层提交前复验；检查失败进入普通事务回滚路径。宿主负责把租约期限、
撤销与生命周期约束封装在检查中。若加入调用方已有事务，最终提交仍由外层事务负责，外层必须保留自己的
提交门禁。COMMIT 发出后的成功或结果不明继续按数据库事务语义返回，不因资格随后到期而改称确定回滚。
不需要额外资格约束的独立宿主可以继续使用 `start_saga` 或 `start_saga_traced`。

## 结果恢复事务资格

`Orchestrator::handle_authenticated_result_authorized_traced` 为结果接收提供相同的事务资格边界。宿主先冻结本次结果
资格，包含不可逆安全发布代际与合同摘要，再通过 `authorize` 复验原期限、撤销代际、安全材料和生命周期。
仅比较当前材料内容无法识别 A→B→A，不能作为权限复验依据。认证和 Inbox claim 之前、事务每次恢复执行时、
实例读取之后及交还提交裁决前均检查。失权回滚整笔结果事务；`SagaResultProcessingError::AuthorityUnavailable` 属于
持续保留原事件的 Defer，不进入有界隔离预算。已有结果 API 继续适用于不依赖外部动态执行资格的宿主，业务事实能否
支持成功、拒绝或屏障仍由参与方在提交与发送前证明，协调端不能替代跨数据库的业务证据核验。

## Definition Catalog 合同

`DefinitionArtifact` 承载 workflow owner 发布的完整、有序 definition seal；canonical document 与
SHA-256 digest 共同形成不可变版本。同一 tenant、workflow、definition_version 与相同 digest 的发布
幂等，不同 digest 冲突。受管操作提供 candidate→active→deprecated→retired。deprecated 只阻止新 start，
已运行实例继续使用创建时冻结的 version/digest；数据库包装在 Catalog 串行事务中检查保留实例、迁移、
审计、Outbox 与有效 capability 租约，引用未释放就拒绝退休。retired 不再装载，不能新建或续租；artifact
仍可查询。退休不自动删除事实，外部消息保留与数据库引用释放必须遵守同一数据保留政策。
deprecated 期间，已提交 Start 的同摘要重放仍返回 `AlreadyExists`，不同摘要仍拒绝为冲突，
运行中和终态实例适用同一裁决。新建准入在受权事务内与数据库唯一键裁决配合；被拒绝的新建完整回滚，
不留下实例、初始 transition、配额、首步命令或 timer。认证与执行资格检查同样约束重复收据。

`CapabilityDescriptor` 描述认证 owner 的逐实例运行能力，包括 replica、workflow/version/step、
transport、origin、effective_saga_base_path、result 凭据或后端摘要、route generation 与租约。route
generation 由 Catalog 在 capability 主键行锁内单调分配并随收据返回，墙上时钟和调用方自报值不承担
代际权威；不确定重报
会取得已提交代际。route 在登记、candidate 激活和 publisher 构造时共用同一 transport 合同：HTTP
只接受纯 origin 和规范的有效基础路径，gRPC 只接受纯 HTTPS origin，Kafka 与 Redis Streams 分别执行
协议名称边界，非 HTTP transport 不接受 HTTP 专属路径。capability 到期只使当前实例不可路由，不改变
definition。`DynamicCatalogSnapshot` 把同一 generation 的 registry 与有效 capability 组合，wrapper
负责从共享数据库装载，Application 负责先发布 route 再切换 registry。`select_capability_routes` 为
HTTP/gRPC 返回完整逐实例 route 集合；Kafka/Redis 仍复验唯一消息目标。`select_capability_route` 用于
要求唯一目标的场景，不能用于把多个合法直接副本压缩成一个地址。

`StartSagaRequest::first_command_raw_payload` 保存 `SagaPayload` 的原始字节和媒体/schema 合同，
参与领域摘要并随首步 execute 的 Outbox 重投；`first_command_payload` 保持旧 JSON 入口，两者互斥。
JSON 校验不会改写原始字节，非 JSON 需要显式 schema；definition 与参与方共同复验合同。
后续步骤、resolve 与补偿依 Saga 身份读取业务事实，不自动复制首步输入。

## 公开协议

发布归档包含两个职责分离的原始协议：`proto/saga_transport.proto` 只承载内部 command/result 投递；
`proto/saga_orchestrator.proto` 承载业务 start/get/query/audit、管理操作与 Definition Registry。
build.rs 从这两个源文件生成 Rust client/server 与同一 descriptor set，crate 分别通过 `grpc_proto`
和 `orchestrator_proto` 导出。业务 API 的 tenant 是授权目标而不是身份来源；actor、owner 与权限必须
由宿主的认证上下文生成。

## Transport

可选 feature 把后端无关消息裁决编入同一核心：

| feature | 能力 | 失败边界 |
| --- | --- | --- |
| `kafka` | command/result consumer、手动 ACK、分区退避与耐久 DLT | 只有本地提交或重复吸收后确认；瞬态失败保留重投 |
| `redis-stream` | XREADGROUP、XAUTOCLAIM、签名、原子 DLT+XACK、积压与安全清剪 | 未确认消息保留在 PEL；清剪不越过任何 group 的未确认前沿 |
| `grpc-transport` | generated command/result service、mTLS principal 绑定与封闭收据 | 未认证请求在 handler 前拒绝；`Retryable` 不伪装为已提交 |

这些 connector 只负责 envelope 认证、调用共享状态机并把结果映射为封闭投递裁决。Application 的
listener、consumer 循环、readiness 与停机所有权由 `napp` 托管；独立宿主需要自行提供等价生命周期。

## 观测

进程级 transport、治理和处理计数只有一份原子来源，混合数据库 Application 不会重复登记同名指标。
数据库已提交的实例、attempt、timer、冲突和配额事实仍由所选 backend store 读取，再与进程快照组合。
`nasaga_stage_duration_seconds` 以五个固定阶段统计 handler、参与方事务、start 事务、transition 事务和
timer lateness。前四项包含失败或取消尝试；timer 指标在领取权威复验后采集调度滞后。Prometheus 和
结构化快照使用同一有界桶来源。指标不使用 saga id、tenant、datasource endpoint 或消息正文作为 label。

该运行时提供可恢复、可审计的最终一致性，不提供跨服务 ACID、物理 exactly-once、跨 datasource 原子
提交或多个并发 Saga 之间的业务隔离。
