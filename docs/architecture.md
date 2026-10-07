# 架构说明

[中文](architecture.md) | [English](architecture.en.md)

`nasa-runtime-rust` 面向 Rust 服务端应用，统一管理应用生命周期，并组合持久事务、消息投递和有序执行
来支持可恢复业务。业务通过 `nasa` 选择能力；Application 负责资源与任务的所有权，数据库和消息系统
负责持久事实。两者边界保持显式，不把进程内状态当作跨系统提交证据。

## 能力分层

| 层次 | 职责 | 代表入口 |
| --- | --- | --- |
| 业务门面 | feature 选择、公共模块与宏入口 | `nasa` |
| 应用生命周期 | 配置、命名资源、初始化、接流许可、监督与停机 | `napp`、`napp-macro` |
| 持久业务 | 本地事务、Inbox/Outbox、幂等、Saga、审计 | `natx-*`、`nainbox-*`、`naoutbox-*`、`nasaga-*` |
| 消息与执行 | broker 交互、消费责任、本地调度与容量 | `nadis`、`nafka`、`napart` |
| 协议与支持能力 | HTTP、gRPC、配置、发现、密钥和观测 | `naweb`、`nagrpc`、`naml` 等 |

组件可以显式装配。脱离 Application 后，调用方承担启动门禁、健康监督和释放顺序。
Application 建立在 Tokio 之上；本项目不实现新的异步执行器，也不替代数据库或消息 broker。

## 生命周期与接流

```text
feature + 组件声明 + 本地/远端配置
                |
       最终 YAML 校验与命名资源探测
                |
       Service 启动 Hook 登记业务计划
                |
       Prepare：迁移门禁与资源装配
                |
       initializer：before -> initialize -> after
                |
       Seal 与 Ready 装配、任务工厂构造
                |
       静态检查、关键健康与共享启动期限复验
                |
       本地状态保护内提交 Ready 与启动许可
                |
       受管入口运行；服务发现确认后对外就绪
```

此图描述主要业务顺序；具体组件可以在不同阶段准备内部资源。initializer 的依赖边优先于数值
`order`，三轮屏障完成前不放行业务入口。失败阻止 Ready，已取得所有权的资源沿激活顺序反向清理。
初始化中的外部提交不能由本地清理撤销，业务应使用事务或稳定幂等键。

端口绑定只证明进程持有监听资源。gRPC 的 `Bound` 阶段不处理 RPC；服务发现注册在统一放行后进行，
注册确认前动态 readiness 仍不可用。Ready 依赖本地观察到的健康证据，不保证尚未观察到的远端故障不存在。

启动 Hook 中普通 `spawn_background` 和 `spawn_critical` 不隐式等待 Ready。自管 listener 使用
`serve_when_ready`；initializer 任务工厂只构造 future，不自行派生任务或开放监听。

Batch 在工作负载前完成资源装配与静态初始化，开放所选出站资源；工作负载完成后进入清理。
Batch 不发布 Service Ready，也不接受仅适用于长期服务的消费与回调计划。

## 事务、消息与 Saga

```text
同源本地事务：Inbox claim + 业务写入 + Outbox 意图
                                |
                              COMMIT
                                |
                    dispatcher 持久投递与重试
                                |
                    下游 Inbox + 下游本地业务事务
```

一个原子链只绑定一个数据库 driver 与 datasource。连接多个数据库不构成跨库事务，after-commit
回调也不提供持久双写保证。Outbox 允许重复投递，下游需要稳定消息身份与事务内 Inbox 或等价幂等策略。

Saga Orchestrator 在同一事务中提交 Inbox、实例 CAS、journal、timer 与 command Outbox；Participant
在另一同源事务中提交 Inbox、gate、业务事实与 result Outbox。定义摘要和身份在占用去重键前复验。
无法确认的外部效果进入 resolve 或人工介入，数据库提交不明不能直接改为普通重试。

已提交 result 使用独立于 command 路由的 Catalog 资格。共享目录、在途实例冻结定义和 producer 信任
仍须完整，缺少 command route 时只允许原结果收敛，不开放新 Start、timer claim、管理操作或 Ready。
请求冻结证据期限、撤销身份、安全发布代际与合同摘要，并在异步恢复、实例锁后和事务交还前持续复验；
失权完整回滚，原事件保持可重投。安全材料即使经历 A→B→A，旧资格也不会恢复。

timer 领取、续租和完成由 fencing capability 约束，旧 owner 不能沿用失效权威推进。
可靠 client 的业务写与 start-intent 使用 `saga.client.datasource_ref` 对应事务，dispatcher 固定扫描
同一来源；显式 Outbox 来源冲突在接流前拒绝。事务内取得事件 ID 不证明外层提交，更不证明远端流程完成。

这些合同提供可恢复的最终一致性，不提供跨服务 ACID、物理 exactly-once 或不同 Saga 的业务隔离。
完整角色与 transport 合同见 [Saga 生产指南](saga-production.md)。

## 有序消费与执行隔离

Redis 的租约、PEL 和 fencing 决定持久接管权；`napart` Runner 决定进程内如何执行。
两者分别管理持久责任与本地调度，消费器拥有自己的 Runner 集合，不复用 Application 的命名 Runner。

不同 Redis 源始终独立。同源通过 `partition.executor.scope` 选择 `source`、`group` 或 `stream`：
每个执行域使用源级预算中固定、不借出的份额，容量不足以覆盖完整拓扑时拒绝启动。
同计划、同实例、同业务键的顺序跨域覆盖 handler、ACK 和精确重试。ACK 结果不明时只对账，不重跑
已经成功的 handler。

跨进程同 key 保序要求生产者将规范化业务键路由到同一物理 Stream。交付仍为至少一次，业务必须幂等。
域间共享 Tokio runtime 和 Redis 客户端，本地调度隔离不代表线程、连接或后端服务隔离。
独立 `napart` 只提供本地执行，不持久化消息。详见 [分区消费](../nadis/docs/partition.md)。

## 配置、身份与观测

严格配置通过启动前工厂显式选择，分为三个所有者：`naml` 生成有界配置候选和来源解释，
`config-boot` 提供按声明匹配的 Nacos 文本，`napp` 负责材料、观察集和运行态发布。
主文件、profile、有序 imports、环境覆盖完成后才求值业务表达式；远端首拉之前只求值必要引导依赖。
每条本地通配符按完整文件名自然排序，后文件覆盖前文件，不同声明位置保持不变。

来源权限固定到启动：环境快照、imports、模式、连接和 provider 信任根不能由运行期候选扩大。
固定模式内的增删及内容变化可重新装配；来源身份在读取与发布边界复验。内存文档入口只使用
调用方提供的材料，不自动打开 imports。嵌套默认值按分支求值，空环境值命中和环境别名规则保持。

来源观察、期望配置和组件应用是三个不同状态。相同值仍需对账来源，推进 `config_observation`
的序号但不伪造业务配置版本。候选失败保留旧视图与观察；发布后组件可以分别为 `Applied`、
`ApplyFailed`、`RestartRequired`，没有跨组件统一回滚事务。完整读取和类型边界见
[naml](../naml/README.md)，迁移入口见[接入与升级](migration.md#选择严格配置装配)。

命名资源绑定明确来源；未知名称或 driver 错配不回退默认连接。配置候选先准备后发布，候选失败保留
旧视图。读到新 YAML 不表示资源已更新，应同时检查应用状态；冻结字段变化报告 `RestartRequired`。

认证层提供已验证身份，授权层按请求冻结策略与 generation；消息正文自报身份不能建立信任。
部署仍需显式配置 ACL、TLS/mTLS、secret、容量与租户边界。

SQL 观测区分方法、实际数据库执行、连接等待和流消费。参数输出默认关闭；开发输出有长度和脱敏约束。
阈值通知通过有界队列调用业务实现，失败不改变 SQL 或事务结果；队列不保证持久送达。
指标出口由 `grafana.observability` 显式配置。框架不替业务建立生产容量或推断目标平台期望实例。

## 停机与失败边界

Service 先摘流并关闭新准入，收口受监督任务和 initializer，再执行业务停机任务、释放业务资源与
更早启动的组件。Batch 在受监督任务之后执行业务停机任务、释放业务资源，再清理静态 initializer。
同一对象只有一个最终关闭 owner；全部清理共享绝对期限。

直接取消 Runner 会立即撤销新资源借用与全局入口，但不保证执行异步收尾。尚未析构的受监督 future
可以继续持有后续资源的释放责任，避免任务仍存活时先释放依赖。已经复制到外部的客户端句柄不受统一
借用撤销控制。`Stopping` 不等于清理完成，退出码也不能代替业务停机报告。

跨崩溃业务保证应依赖持久事实。SIGKILL、`panic=abort`、同步阻塞和不让出执行权的任务不受异步期限保证。
配置与观察入口见 [部署](deployment.md)、[运维](operations.md) 与 [受管能力](managed-capabilities.md)。
