# nasaga-runtime-pgsql

`nasaga-runtime-pgsql` 将 PostgreSQL Saga store、Inbox、Outbox 与 ambient transaction
绑定到同一命名 datasource，并把它们交给 `nasaga-runtime-core` 中唯一的 Orchestrator、Participant、
timer、补偿、恢复和 fencing 状态机。
Application 受管可靠 client 同样把业务事实、start-intent 和 dispatcher 绑定到一个 PostgreSQL
事务域，远端暂不可用时由已提交事件承担恢复，不把本地已受理等同于流程完成。
受管 Orchestrator 使用独立 Catalog 资格恢复已提交 result；command route 缺席不会单独阻断原事件，
也不会因此开放新 Start、timer claim 或 Ready。

共享核心保留 `SagaPayload` 的原始字节、媒体类型与 schema；PostgreSQL Catalog 支持完整 definition
生命周期。`deprecated → retired` 必须确认实例、迟到结果所依赖的事实、保留 Outbox 与审计均无引用，
且没有有效 capability 租约；退休不删除历史事实或代替数据保留策略。受管 HTTP/gRPC API、Nacos
发现、HMAC/mTLS 轮换与分段延迟观测由 `napp` 统一装配，配置见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。

## 核心价值

分布式步骤在进程崩溃、至少一次重投和多副本竞争下，从 PostgreSQL 已提交事实继续推进；无法确定的
外部效果进入类型化裁决或人工介入，不猜测成功或失败。该运行时提供最终一致性，不提供跨服务 ACID、
物理 exactly-once、跨 datasource 原子提交或并发 Saga 隔离。

## 运行架构

```text
命名 PgPool / ambient transaction
        ├─ client：业务事实 + start-intent → 同源 dispatcher → 远端 Orchestrator
        ├─ Orchestrator：result Inbox + 实例 CAS + journal + timer + command Outbox
        └─ participant：command Inbox + gate + 业务事实 + result Outbox
```

每个分支只在自身 datasource 内原子提交；不同服务通过耐久事件和明确收据衔接，不共享数据库事务。
`napp` 拥有配置、角色装配、Ready、后台循环和停机，本 crate 不自行启动 listener 或持续轮询任务。

## 已提交结果恢复

受管 HTTP、gRPC、Kafka 与 Redis Streams 入口冻结同一次 Catalog 证据期限、撤销身份、安全发布
generation 与合同摘要。共享 Catalog、在途实例冻结 definition 和 producer 信任必须继续有效；只有
command route 或健康证据缺席时，原 result 仍可进入 PostgreSQL 状态事务，Application 不恢复业务
Ready。异步等待、实例行锁取得后和事务交还前继续复验，watcher 后续续期不能延长原请求。
失权回滚 result Inbox、journal、实例迁移、timer 与后续 command Outbox，原事件保持可重投；安全材料
A→B→A 不恢复旧资格。COMMIT 发出后仍按 PostgreSQL 明确收据或提交结果不明处理。独立宿主需要调用
authorized 结果 API 才取得该动态权限边界。

## 初始化与受管角色

直接依赖包装层时，调用方可以构造 PgOrchestrator::with_datasource 或 PgParticipantRuntime，并自行拥有
schema、Catalog、transport、timer 与停机。普通业务通过 nasa 门面声明 Saga 组件，由 napp 按角色调用
本 crate 的命名数据源入口。

~~~yaml
datasources:
  saga-control:
    driver: postgresql
    url: ${SAGA_DATABASE_URL}
    schema: saga_checkout

outbox:
  datasource_ref: saga-control

saga:
  role: orchestrator
  plan_mode: managed
  service_identity: checkout-orchestrator
  replica_identity: ${SAGA_REPLICA_ID}
  orchestrator:
    datasource_ref: saga-control
  definition_catalog:
    mode: dynamic
    datasource_ref: saga-control
~~~

orchestrator 只在 saga-control 创建全局状态、result Inbox、command Outbox、timer、Definition Catalog、
capability registry、审计与配额结构；participant 只创建 command Inbox、gate 与 result Outbox；
reliable client 只创建 start-intent Outbox。driver、datasource 或 schema 身份不一致时拒绝 Ready，
不会回退 MySQL 或 default。
可靠 client 的写入与 dispatcher 固定使用 `saga.client.datasource_ref`；显式
`outbox.datasource_ref` 必须与之相同，冲突在 Ready 前拒绝，省略该字段不改变扫描数据源。
Orchestrator 与 participant 的全部受管 DDL、回填和最终复验复用取得 advisory lock 的同一连接，
`max_connections = 1` 的合法低资源池不会因启动门禁再申请第二条连接。

Catalog 自举在同一 PostgreSQL schema 权威内先迁移已识别的直接前代结构，再核对 CHECK 的
`pg_get_constraintdef`、时间默认值、audit identity、索引谓词与列序。capability tenant 只能从唯一匹配
definition 推导并同步写入 descriptor 后重算摘要；加列未回填和 capability 暂时无主键等已识别中间态
会在后续启动继续收敛，非 candidate definition 必须具有激活时间。归属歧义、弱约束或未知结构都会
阻止 Ready。

共享 Catalog 对 definition key/digest 执行不可变与幂等裁决，capability lease 只影响当前路由，不删除
持久 definition。route generation 在 capability 行锁内由数据库单调分配，同路由重报沿用已提交代际，
主机时钟回拨不会阻塞续租或端点变化。PgOrchestrator 在实例创建时冻结 definition version/digest，后续 registry generation
切换不会改变在途 timer、result、resolve 或补偿语义。

HTTP、gRPC、Kafka 与 Redis Streams connector feature 复用 nasaga-runtime-core 的 envelope 和裁决器，
并由 `napp` 按角色构造完整 managed 数据面。多 datasource participant 以 descriptor binding 精确选择
PostgreSQL datasource，并为每个事务域建立独立 Inbox/gate/Outbox runtime。一个事务只能使用一种
driver 和一个 datasource；跨 driver 的耐久写入必须通过源库 Outbox 与目标库 Inbox 收敛，after-commit
只允许发送进程内唤醒。

## 可靠 client 的事务与恢复

业务在 `saga.client.datasource_ref` 对应 PostgreSQL 事务内调用 `SagaRemoteClient::enqueue_start`，
业务事实与 start-intent 同时提交或回滚。返回 `event_id` 只表示事务内追加成功，外层事务明确提交后
才可表示本地已受理；dispatcher 固定扫描该库，显式 Outbox 数据源冲突在 Ready 前拒绝。
省略 Outbox 段或只配置轮询预算不改变此绑定。

远端不可用、超时或已提交收据丢失时保留原 `event_id` 重投；只有 `Committed` 或 `Duplicate` 才
标记投递完成。Ready 不保证队列已排空，投递完成也不保证整个 Saga 已到终态。运维分别查看
`napp_outbox_pending`、`napp_outbox_published_total`、`napp_outbox_dead` 和远端实例查询。

## 观测

运行核心提供唯一的 transport、治理与处理计数；`PgSagaStore::load_operational_metrics` 提供数据库已提交
的实例、attempt、timer、冲突和配额聚合。Application 会把两类来源组合进统一指标目录，混合 MySQL 与
PostgreSQL 时不会按 driver 重复登记相同进程指标。高基数业务身份、消息正文与 datasource endpoint
不进入指标 label。
