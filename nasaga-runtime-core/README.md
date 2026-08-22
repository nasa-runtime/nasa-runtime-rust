# nasaga-runtime-core

`nasaga-runtime-core` 是 MySQL 与 PostgreSQL Saga 包装共同使用的唯一运行状态机。Orchestrator、
Participant、definition registry、durable timer、恢复、补偿、管理动作、认证 envelope 与投递裁决都在
本 crate 实现；数据库包装只提供 `SagaBackend`，不会复制一份按后端分叉的流程逻辑。

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
指标不使用 saga id、tenant、datasource endpoint 或消息正文作为 label。

该运行时提供可恢复、可审计的最终一致性，不提供跨服务 ACID、物理 exactly-once、跨 datasource 原子
提交或多个并发 Saga 之间的业务隔离。
