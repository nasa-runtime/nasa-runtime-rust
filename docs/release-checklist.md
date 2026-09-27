# 交付就绪清单

生成公开归档或生产制品前，逐项确认当前能力、内容边界和运行前提。

## 仓库内容

- [ ] 根目录保留双许可证说明 `LICENSE`；每个公开归档包含完整的 `LICENSE-MIT`、`LICENSE-APACHE`
      和 `NOTICE`，与 manifest 的 `MIT OR Apache-2.0` 一致，不能只携带指向仓库外文件的说明。
- [ ] 根 README 能索引全部组件，组件 README 均包含用途、接入、初始化、yml 和主要边界。
- [ ] 从业务使用者视角逐项说明解决的问题、核心价值、运行架构、安全与顺序不变量、非目标和观测方式；
      不能用零散指标、API 名称或一句实现摘要代替核心能力的独立说明。
- [ ] `SECURITY.md`、`CONTRIBUTING.md` 与当前实现一致。
- [ ] 公开文档、rustdoc、源码注释和 manifest 注释准确描述当前业务能力、配置、边界和失败后果。
- [ ] 全部公开文本通过仓库文本规则检查，不含问题处置标签、内部里程碑、工作流水、工具归因、外部
      技术栈类比或临时诊断内容。
- [ ] 凭据、证书、临时脚本和内部路径不进入产品归档。
- [ ] 文档与示例不包含真实密钥、私有地址、内部主机名或业务数据。

## 归档与依赖

- [ ] 每个公开 crate 的离线归档构建成功，归档内容仅包含产品源码、公开文档和再分发所需文件。
- [ ] 使用 `cargo package` 生成真实归档，直接核对文件清单、README、规范化 manifest、许可证与版本；
      `cargo package --list` 或工作树内容不能代替最终归档检查，离线依赖缺失时不得宣称归档已完成。
- [ ] 直接从每个 `.crate` 归档读取 README 和规范化 manifest；核心价值在 README 首屏、独立架构章节、
      crate rustdoc 与 description 中一致，keywords/categories 能支持正确搜索与选型。
- [ ] crate README 不引用归档外部的 `../docs` 等本地相对路径；归档必须独立提供 README 承诺的入口。
- [ ] 前置 crate 已能从 registry 解析；下游 manifest 已删除指向其公开版本的 `path`，锁文件已按纯线上
      依赖重新生成。
- [ ] `release-crates.sh --versioned-plan` 中每个 `crate@version` 与 manifest 完全一致；已有组件采用
      crates.io 当前最高稳定版本的下一个补丁号，首次公开的组件使用 `1.0.0`。
- [ ] 每个批次发布并回读后，在干净工作树运行 `prepare-next-release-batch.sh <completed-batch>`；只删除
      已上线 crate 的根级 `[patch.crates-io]` 本地覆盖，审阅并提交 `Cargo.toml` 与 `Cargo.lock` 后才启动
      下一批。
- [ ] 下一批 workflow 已通过 `verify-release-transition.sh`，确认所有前置 crate 都不再使用根级 path
      patch；`cargo package --locked` 在该状态下从 registry 解析前置版本和 feature。
- [ ] 默认流程严格按“完成提交 → 推送目标分支 → 远端 CI 全绿 → 核对远端提交 SHA → 获得明确上传授权
      → 上传 registry → 回读 registry 元数据与 README”推进；本地 dry-run 或归档通过不构成上传授权。
- [ ] 公开版本不可原地替换；归档遗漏或文档合同不完整时停止当前批次，以新的补丁版本承载后续内容，
      registry 的实际归档内容必须与对外说明一致。

## 组件边界

- [ ] feature 单独开启与常用组合都能编译，门面路径在依赖改名后仍正确。
- [ ] 对每个最终可执行制品检查实际进入依赖图的 transaction、Mapper core/adapter、Inbox/Outbox core
      与 adapter、幂等 store、Saga backend/runtime 和 `nafka`；每个包只能解析出一个 package ID 和一个
      来源，禁止 registry 与本地路径副本同时进入同一进程。
- [ ] PostgreSQL-only 组合（含 `full-pgsql`）的依赖图不包含 MySQL transaction、Mapper、Inbox、
      Outbox 或 Saga runtime；混合组合只包含业务显式选择的两组能力。
- [ ] 配置未知字段、非法零值、冲突配置和缺少凭据均在开放流量前失败。
- [ ] MySQL 与 PostgreSQL 写路径各自的事务归属、提交结果不确定处理和回滚语义明确；两种 ambient
      transaction 不嵌套，也不宣称跨 driver 原子提交。
- [ ] 同一 Application 可同时配置两种 driver，datasource 名称全局唯一，typed pool 查询拒绝 driver
      mismatch，启动失败与停机均覆盖全部已受管 pool。
- [ ] Service 为每个业务 schema 显式登记构建期嵌入的 migrator；`disabled`、`validate` 与 `apply` 符合
      部署策略。PostgreSQL schema 和 connection topology 即使省略 `migrations` 段也不会丢失；事务级代理
      使用独立 session endpoint，并在 advisory lock 前与业务 pool 复验 database/schema 身份。
- [ ] Redis、Kafka、WebSocket 和后台任务具有队列、并发、超时或批量上限。
- [ ] 普通 Stream、Proxy、AutoPipeline、原生 TCP Client 与 hystrix 的定位在根 README、组件 README、
      门面、crate rustdoc 和 manifest 元数据中一致；独立 API 与 Application 受管入口分别说明。
- [ ] Service 的消费、派生发送、Client 与宿主终端共用启动许可；关键本地任务、认证连接、健康证据
      和启动预算复验持续保护到许可发布。公开 Ready 不被描述为远端送达或未来调用成功保证。
- [ ] Batch 只开放支持的微批与出站发送，不接受长期消费或回调计划；微批参数字节边界、Proxy PEL
      清理证据、Client 断连健康与 hystrix 旧命令拒绝语义均有明确说明。
- [ ] Redis 分区消费的 source/group/stream 模式、源间独立性、固定域份额、全源业务键顺序、ACK
      不确定与停机失败语义在根 README、nadis、napart、门面、rustdoc 和 manifest 元数据中一致。
- [ ] 分区消费的域数与总槽数按完整拓扑有界；每域整批预算充足，未持锁来源仍计入份额，解码权重
      与线格式上限分别说明。观测入口不得把消费器本地 readiness 描述为自动接入 Application。
- [ ] 业务停机任务在根 README、门面、运行时、宏入口和运维文档中的合同一致：UserHook 登记、稳定名称、
      priority 顺序、共享绝对期限、先任务后业务资源、失败计数和直接取消边界均有明确描述。
- [ ] Runner 直接取消会撤销本实例全局入口和新资源借用；经过任务门时仍存活的受监督 future 保留
      后续资源依赖，最后一个 future 析构后才释放尾部，不将 Stopping 描述成异步收尾完成。
- [ ] 一次性收尾、受管资源关闭与组件关闭责任不重叠；析构隔离不被描述为持久执行保证，也不掩盖
      `panic=abort`、同步阻塞、未归还借用和全局期限耗尽的后果。
- [ ] `rate-limit` feature、`full` 组合、门面路径和 README 一致；跨副本配额显式复用受管 Redis，没有
      隐式组件字符串或配置根。默认 fail-open、键空间上界、主体身份和 `429` / `Retry-After` 映射符合
      业务风险选择，且不与单实例令牌桶混为同一计数。
- [ ] 受管 Web listener 的启用前提、HTTP/1/h2c 开关、连接与 stream 上限、GOAWAY 排空、
      `<context_path>/metrics` 暴露条件、协议指标及 TLS 非目标与最终 YAML、README、rustdoc 和 manifest
      定位一致。
- [ ] 认证、授权、重放保护、密钥轮换和敏感信息脱敏符合 `SECURITY.md`。
- [ ] 指标 label 维持低基数，日志不暴露凭据、payload 或完整业务身份。

## SQL 与统一观测合同

- [ ] 根 README、门面、Mapper、事务连接、通知与指标出口的定位、初始化、默认配置和失败后果一致。
- [ ] 慢 SQL 判定为原始耗时达到或超过有效阈值，连接等待和消费者处理不混入；逐条通知明确要求
      业务安装 `Notify`、启用告警并关闭通知冷却，日志开关不控制通知。
- [ ] 没有实现时忽略、重复安装拒绝、队列满和投递失败不影响 SQL；入队、适配器接受和最终收件
      不能混为同一确认，框架不内置机器人连接或指定通知微服务协议。
- [ ] Service 的全部 Ready 装配、initializer 工厂、最终静态容量检查与共享期限复验先于统一放行；
      gRPC `Bound` 不处理协议，服务发现注册确认前动态 readiness 不可用。
- [ ] 开发参数输出仅允许明确开发环境并脱敏，SQL、参数、URL 和错误正文不进入指标标签。
- [ ] 统一指标出口显式启用，Web metrics 与 health 开关独立；remote write 容量覆盖已提交原生序列与
      显式预留，不把检查后的动态 label 增长描述为已覆盖。
- [ ] 平台 controller 与应用凭据隔离，期望实例指标由外部平台持续提供，框架只引用，不从应用心跳
      推断库存；期望源缺失保持 NoData。

## Saga 运行条件

- [ ] Orchestrator、参与方、Inbox、Outbox 和业务表按本地事务边界部署，不存在伪跨库原子提交。
- [ ] 可靠 client 的业务事务、start-intent 与 dispatcher 固定绑定 `saga.client.datasource_ref`；
      显式 `outbox.datasource_ref` 冲突在 Ready 前拒绝，省略该字段或只设置轮询预算不改变绑定。
- [ ] 发起入口明确区分事务内追加、外层事务提交、事件投递与远端流程完成；不以 `event_id`、Ready
      或本地已受理响应代替远端完成证明，收据丢失时保留原事件身份重投。
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
      身份来源和 DLT/收据合同逐项一致；Saga gRPC generated service/client、mTLS principal、纯出站
      owner 与入站 listener 生命周期的责任分界准确。
- [ ] Redis Streams 的 `(stream, group, consumer)` 唯一，Cluster key 同槽，PEL/XAUTOCLAIM、消息签名、
      原子 DLT 与安全清剪告警已经批准。
- [ ] trace 只从已验证显式上下文传播；缺失 trace 不阻断投递，日志和 span 不携带 payload、完整业务键
      或凭据。
- [ ] 启用租户实例配额或 Outbox 在飞配额前，全部写入方已升级且存量账本已在事务内对账并初始化；
      管理动作速率使用数据库窗口，精确用量只经受权查询读取。
- [ ] 产生 `MANUALLY_CLOSED` 前全部副本都能解析该终态，`enable_manual_close` 的放行批次和回退边界已批准。
- [ ] MySQL Saga 增量迁移与 PostgreSQL Orchestrator/participant 语义化迁移按各自合同执行；摘要门禁、
      在线 DDL、锁预算和回退方案均有批准记录。
- [ ] 每个 Outbox 表具备匹配所选后端查询的 dispatch、dead 与 retention 索引；MySQL 复合索引和
      PostgreSQL 部分索引分别以实际 SQL 的执行计划复验，待投递、死信计数和领取查询不随历史总行数
      退化；已投递行与死信的保留、归档和分批清理策略已经批准。
- [ ] retention 提交应答不确定、预算耗尽、行锁竞争和归档收据丢失有独立指标与处置流程；不确定提交
      不计入已确认删除，也不刷新最近成功时刻。
- [ ] replay horizon 覆盖消息最大保留期；Inbox、participant gate、journal、DLT 和审计事实不会过早清理。
- [ ] 峰值、retry storm、timer/Outbox/DLT 积压、连接池与存储余量有明确预算和告警阈值。
- [ ] `docs/alerts/saga-prometheus.yml` 已接入 Prometheus 和值班路由。

## 生产环境批准

- [ ] 实际选用的 MySQL 和/或 PostgreSQL 主从拓扑、提升流程、备份恢复点、复制延迟阈值和数据丢失
      目标已签署。
- [ ] Kafka broker 拓扑、副本因子、最小同步副本、ACL、凭据轮换和故障策略已签署。
- [ ] 候选硬件上的目标峰值、积压清空速率、资源余量和服务等级目标已签署。
- [ ] 在线 DDL 的锁等待、总耗时、磁盘余量、维护窗和回退条件已签署。
- [ ] 灾难恢复流程、恢复时间目标、值班升级链路和演练记录已签署。

本地容器环境可以确认协议与故障语义，不能替代候选拓扑容量、真实 ACL 或灾难恢复签字。
