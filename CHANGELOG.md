# 当前变更说明

源码沿革由 Git 提交记录保存。本文件只概括当前工作区相对既有公开能力的业务变化，不记录内部质量
过程、审查过程或工具归因。

## Saga

- 新增 `nasaga-core`、`nasaga-mysql`、`nasaga-runtime` 与 `nasaga-macro`，通过门面 feature 提供流程
  定义、持久化 Orchestrator、参与方事务 adapter 和可选 Kafka transport。
- 建立 `effect_id`、`command_id`、目标业务效果身份与定义摘要的确定性派生，阻止身份漂移造成重复
  外部副作用。
- Orchestrator 将 Inbox、状态 CAS、attempt、迁移事实、下一 command Outbox 和 timer 放入同一本地
  事务；参与方将 Inbox、gate、业务事实和 result Outbox 放入自己的本地事务。
- ACK 仅发生在提交明确成功或 Inbox 明确重复之后；提交结果不确定时持续重投，确定性拒绝先提交 DLT。
- Unknown 使用 typed resolve，超时经过取消屏障，补偿计划冻结，HALTED 由已认证且可审计的人工动作
  重开。
- 参与方授权精确绑定 producer、route、workflow、定义版本、摘要、步骤和 owner；管理主体来自受信
  身份上下文。
- timer 使用数据库租约、逐副本 owner、运行实例随机 nonce 和不可复制 capability，失权 worker 无法
  继续提交副作用。
- 增加低基数运行指标、管理审计、冲突事实、durability-first DLT 和 Prometheus 告警规则。
- 数据库迁移采用语义化文件名；参与方摘要执行两阶段封口，排序规则转换具有独立维护窗边界。

## 应用运行时

- 完善受管组件的启动、Ready、反向停机、配置快照切换和资源 owner 语义。
- `#[nasa::application("saga")]` 隐式纳入数据库与受管 Outbox，业务通过单一计划提交运行角色、
  timer owner 与发布端；Kafka 保持显式 transport 选择。
- Outbox 可以独立声明，提供持久化积压、死信、发布量和失败轮次观测；事务提交会立即唤醒本进程
  dispatcher，固定轮询只承担跨进程与崩溃恢复兜底。
- 数据库、Redis、Kafka、WebSocket、调度和 telemetry 后台任务具备显式容量、超时、取消与排空边界。
- 配置解析拒绝未知字段和冲突组合，secret 与连接信息默认脱敏。

## 数据一致性

- ambient MySQL 事务明确 rollback-only、after-commit、多 datasource 和任务继承边界。
- Inbox 提供 claim、业务处理和提交一体化入口，并把提交不确定与回滚失败保留为 transport 可判定的
  封闭错误类别。
- Inbox、Outbox、幂等、审计和缓存失效能力按本地事务与至少一次投递组合。
- Outbox 待投递与死信计数使用覆盖索引，批次领取先定位真实候选再回表读取事件列；生产结构由语义化
  迁移文件治理。
- 缓存不作为资金、库存或权限事实源，跨副本失效能力与降级行为显式配置。

## 传输与安全

- Kafka 消费支持 manual ACK、分区退避、暂停语义、DLT 和提交不确定状态收敛。
- Web、长连接和 REST client 统一资源上限、身份、重放保护、敏感 header 与停机行为。
- 现代认证加密入口使用可失败 OS 熵源；历史兼容路径受独立 feature 和运行期风险控制。

## 文档与资产

- 组件 README 统一说明用途、接入、初始化、yml、正常用法和主要边界。
- 公开文档与注释只描述当前能力、安全不变量、配置和失败后果。
- 根级质量工程只在本地保留，不进入组件源码或产品归档。
