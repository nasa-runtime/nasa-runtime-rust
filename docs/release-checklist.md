# 交付就绪清单

生成公开归档或生产制品前，逐项确认当前能力、资产边界和运行前提。

## 仓库内容

- [ ] `LICENSE`、`LICENSE-MIT`、`LICENSE-APACHE` 与 `NOTICE` 完整，并进入每个公开归档。
- [ ] 根 README 能索引全部组件，组件 README 均包含用途、接入、初始化、yml 和主要边界。
- [ ] `SECURITY.md`、`CONTRIBUTING.md` 与当前实现一致。
- [ ] 公开文档、rustdoc、源码注释和 manifest 注释只描述当前业务能力、配置、边界和失败后果。
- [ ] 根级质量工程、fixture、故障注入工具、凭据和证书不进入产品归档。
- [ ] 文档与示例不包含真实密钥、私有地址、内部主机名或业务数据。

## 代码质量

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `RUSTDOCFLAGS="-D missing_docs -D warnings" cargo doc --workspace --no-deps`
- [ ] `cargo check -p nasa --features full --all-targets`
- [ ] `cargo deny check`
- [ ] 每个公开 crate 的离线归档构建成功，归档内容仅包含产品源码、公开文档和再分发所需文件。

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
- [ ] 数据库迁移按固定顺序执行；摘要封口、在线 DDL、排序规则转换和回退方案均有批准记录。
- [ ] 每个 Outbox 表具备 `(dispatched, dead, id)` 与 `(dead, id)` 索引，待投递、死信计数和领取查询的
      执行计划不随历史总行数退化；已投递行与死信的保留、归档和分批清理策略已经批准。
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
