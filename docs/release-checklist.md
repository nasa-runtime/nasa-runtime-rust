# 交付就绪清单

生成公开归档或生产制品前，逐项确认当前能力、资产边界和运行前提。

## 仓库内容

- [ ] `LICENSE`、`LICENSE-MIT`、`LICENSE-APACHE` 与 `NOTICE` 完整，并进入每个公开归档。
- [ ] 根 README 能索引全部组件，组件 README 均包含用途、接入、初始化、yml 和主要边界。
- [ ] `SECURITY.md`、`CONTRIBUTING.md` 与当前实现一致。
- [ ] 公开文档、rustdoc、源码注释和 manifest 注释只描述当前业务能力、配置、边界和失败后果。
- [ ] 凭据、证书、临时脚本和内部路径不进入产品归档。
- [ ] 文档与示例不包含真实密钥、私有地址、内部主机名或业务数据。

## 归档与依赖

- [ ] 每个公开 crate 的离线归档构建成功，归档内容仅包含产品源码、公开文档和再分发所需文件。
- [ ] 直接从每个 `.crate` 归档读取 README 和规范化 manifest；核心价值在 README 首屏、独立架构章节、
      crate rustdoc 与 description 中一致，keywords/categories 能支持正确搜索与选型。
- [ ] 前置 crate 已能从 registry 解析；下游 manifest 已删除指向其公开版本的 `path`，锁文件已按纯线上
      依赖重新生成。

## 组件边界

- [ ] feature 单独开启与常用组合都能编译，门面路径在依赖改名后仍正确。
- [ ] 对每个最终可执行制品运行 `cargo tree -i natx`、`cargo tree -i nafka`、
      `cargo tree -i nainbox-mysql`、`cargo tree -i naoutbox-core` 和 `cargo tree -i naoutbox-mysql`；每个包只能解析出一个 package ID
      和一个来源，禁止 registry 与本地路径副本同时进入同一进程。
- [ ] 配置未知字段、非法零值、冲突配置和缺少凭据均在开放流量前失败。
- [ ] MySQL 写路径的事务归属、提交结果不确定处理和回滚语义明确。
- [ ] Redis、Kafka、WebSocket 和后台任务具有队列、并发、超时或批量上限。
- [ ] 认证、授权、重放保护、密钥轮换和敏感信息脱敏符合 `SECURITY.md`。
- [ ] 指标 label 维持低基数，日志不暴露凭据、payload 或完整业务身份。

## Saga 运行条件

- [ ] Orchestrator、参与方、Inbox、Outbox 和业务表按本地事务边界部署，不存在伪跨库原子提交。
- [ ] 所有活跃流程定义来自同一受信、不可变快照；Ready 前完成摘要和 descriptor 对齐。
- [ ] command、result 与 DLT topic 的 owner、路由、consumer group 和默认拒绝 ACL 已批准。
- [ ] ACK 只发生在 COMMIT 明确成功或 Inbox 明确重复之后；提交结果不确定时保留原消息。
- [ ] 确定性拒绝先持久化 DLT，再推进源 offset 或 Outbox；DLT 不可达时同分区不前移。
- [ ] 同一 `outbox_event` 表内全部事件类型都有唯一 publisher 路由；毒丸策略、整表停摆半径、积压与失败
      轮次告警已经批准，无法共享发布合同的领域使用独立事务数据库和独立 Outbox。
- [ ] Unknown、取消屏障、冻结补偿计划、HALTED 与人工重开路径符合封闭状态机。
- [ ] 每个参与方 phase 入口都要求已认证 producer，并精确绑定 workflow、定义版本、摘要与 owner。
- [ ] 多副本 HTTP 类入口使用共享强一致 nonce claim；每条信任边和恢复动作拥有独立配额。
- [ ] 每个 Orchestrator 副本使用唯一且重启稳定的 owner；timer 副作用前重新核对租约、token、
      generation 与实例版本。
- [ ] Kafka、Redis Streams、HTTP 或 gRPC 的实际选择与 feature、Application 组件、发布端、消费端、
      身份来源和 DLT/收据合同逐项一致；实验 gRPC 不被描述为完整受管服务。
- [ ] Redis Streams 的 `(stream, group, consumer)` 唯一，Cluster key 同槽，PEL/XAUTOCLAIM、消息签名、
      原子 DLT 与安全清剪告警已经批准。
- [ ] trace 只从已验证显式上下文传播；缺失 trace 不阻断投递，日志和 span 不携带 payload、完整业务键
      或凭据。
- [ ] 启用租户实例配额或 Outbox 在飞配额前，全部写入方已升级且存量账本已在事务内对账并初始化；
      管理动作速率使用数据库窗口，精确用量只经受权查询读取。
- [ ] 产生 `MANUALLY_CLOSED` 前全部副本都能解析该终态，`enable_manual_close` 的放行批次和回退边界已批准。
- [ ] 数据库迁移按固定顺序执行；摘要封口、在线 DDL、排序规则转换和回退方案均有批准记录。
- [ ] 每个 Outbox 表具备 `(dispatched, dead, id)` 与 `(dead, id)` 索引，待投递、死信计数和领取查询的
      执行计划不随历史总行数退化；已投递行与死信的保留、归档和分批清理策略已经批准。
- [ ] retention 提交应答不确定、预算耗尽、行锁竞争和归档收据丢失有独立指标与处置流程；不确定提交
      不计入已确认删除，也不刷新最近成功时刻。
- [ ] replay horizon 覆盖消息最大保留期；Inbox、participant gate、journal、DLT 和审计事实不会过早清理。
- [ ] 峰值、retry storm、timer/Outbox/DLT 积压、连接池与存储余量有明确预算和告警阈值。
- [ ] `docs/alerts/saga-prometheus.yml` 已接入 Prometheus 和值班路由。

## 生产环境批准

- [ ] MySQL 主从拓扑、提升流程、备份恢复点、复制延迟阈值和数据丢失目标已签署。
- [ ] Kafka broker 拓扑、副本因子、最小同步副本、ACL、凭据轮换和故障策略已签署。
- [ ] 候选硬件上的目标峰值、积压清空速率、资源余量和服务等级目标已签署。
- [ ] 在线 DDL 的锁等待、总耗时、磁盘余量、维护窗和回退条件已签署。
- [ ] 灾难恢复流程、恢复时间目标、值班升级链路和演练记录已签署。

本地容器环境可以确认协议与故障语义，不能替代候选拓扑容量、真实 ACL 或灾难恢复签字。
