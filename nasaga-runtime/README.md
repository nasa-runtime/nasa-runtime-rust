# nasaga-runtime

`nasaga-runtime` 将 `nasaga-core` 合同与 MySQL store 组装为持久化 Orchestrator、参与方事务
adapter、管理与恢复入口、运行指标以及可选 transport connector。业务通过 `nasa` 门面启用。
在 Application 受管模式下，可靠 client 的业务事实与 start-intent 在同一 MySQL 事务提交，
dispatcher 固定扫描该数据源；本地已受理与远端流程完成是两个独立边界。

共享核心保留 `SagaPayload` 的原始字节、媒体类型与 schema；MySQL Catalog 支持完整 definition
生命周期。`deprecated → retired` 必须确认实例、迟到结果所依赖的事实、保留 Outbox 与审计均无引用，
且没有有效 capability 租约；退休不删除历史事实或代替数据保留策略。受管 HTTP/gRPC API、Nacos
发现、HMAC/mTLS 轮换与分段延迟观测由 `napp` 统一装配，配置见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/docs/saga-production.md)。

## 核心价值

本 crate 让分布式步骤在进程崩溃、至少一次重投和多副本竞争下仍由已提交事实继续推进，并在无法确定
结果时停在可裁决状态，而不是猜测成功或失败。业务获得的是可恢复、可审计的最终一致性运行时，不是
跨服务 ACID、物理 exactly-once 或并发隔离。

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["saga-runtime"] }
# Kafka 托管消费入口使用 features = ["saga-kafka"]
# Redis Streams 托管消费入口使用 features = ["saga-redis-stream"]
# gRPC generated service/client 与封闭收据：features = ["saga-grpc"]
```

## 运行架构

```text
业务入口 / 调度触发
        │
        ▼
Orchestrator ──同一本地事务── Inbox + CAS/journal + timer + command Outbox
        │                                                     │
        │                                      Kafka / Redis Streams / HTTP / gRPC
        │                                                     │
        └──────── result consumer ◀── result Outbox + 业务事实 + gate + Inbox
                                             同一本地事务
```

- Orchestrator 是唯一状态裁决者；参与方只执行类型化 phase 并返回业务结果。
- `effect_id` 跨重投稳定，`command_id` 标识单次投递；目标系统用前者去重。
- durable timer、实例 CAS 和 Inbox/Outbox 事实承担崩溃恢复，运行时不依赖内存续跑。
- Application 只拥有生命周期、Ready 门禁和后台循环；事务正确性仍由 runtime/store 合同约束。

## Application 受管入口

普通服务经 nasa 门面声明 Saga 组件，napp 根据 saga.role 和角色作用域内的 datasource_ref 调用本 crate。
orchestrator 启动时会在精确 MySQL datasource 上确保全局状态、result Inbox、command Outbox、timer、
Definition Catalog、capability registry、管理审计与配额结构；participant 只确保 command Inbox、gate
与 result Outbox；reliable client 只确保 start-intent Outbox。角色不需要的全局表不会成为启动依赖。
可靠 client 的写入与 dispatcher 固定使用 `saga.client.datasource_ref`；显式
`outbox.datasource_ref` 必须与之相同，冲突在 Ready 前拒绝，省略该字段不改变扫描数据源。
Orchestrator 与 participant 的全部受管 DDL、回填和最终复验复用取得 schema lock 的同一连接，
`max_connections = 1` 的合法低资源池不会因启动门禁再申请第二条连接。
Catalog 自举会在 schema 锁内迁移已识别的直接前代列、tenant 主键与审计字段，再核对命名 CHECK 的
真实表达式、时间默认/自动更新、collation 和索引列序；无法唯一推导 capability 租户或最终合同漂移时
拒绝 Ready。加列、回填、审计索引和主键目标按各自结构事实独立续跑；已识别的 DDL 中间态可在重启后
继续收敛，非 candidate definition 缺少 `activated_at` 不能通过最终门禁。

~~~rust
#[nasa::application("saga", "web")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
~~~

动态模式从共享数据库读取 active 和仍被在途实例引用的 definition，并把 canonical digest 冻结进实例。
相同 definition key 与 digest 的发布幂等，不同 digest 冲突；deprecated 只阻止新 start。参与方
capability 以认证 owner、replica、workflow/version/step、endpoint、effective Saga path 与租约持久化，
route generation 在 capability 行锁内由数据库分配并随登记收据返回，不依赖参与方墙上时钟；租约到期
不删除 definition。napp 的 watcher 先发布完整 route 快照，再切换运行时 registry。

managed 模式下，本 crate 提供注册、查询和生命周期存储入口，`napp` 拥有 HTTP/gRPC/Kafka/Redis
Streams 的认证、RBAC、publisher、consumer、listener、readiness 与停机。start 在单个 MySQL 事务内
提交幂等事实、instance、首条 command
Outbox 和 timer；result 重复由 Inbox 吸收，状态迁移、下一条 command 与 timer 同事务提交。网络结果
不明不改变源 Outbox，只有明确的 Committed 或 Duplicate 收据允许发送端前移。

## 可靠 client 的事务与恢复

可靠 client 由 `napp` 构造，不在本进程创建 Orchestrator。业务通过门面取得 `SagaRemoteClient`，在
`saga.client.datasource_ref` 对应事务中先写业务事实，再调用 `enqueue_start`；二者同时提交或回滚。
返回的稳定 `event_id` 表示事务内追加成功，只有外层事务明确提交后才可表示本地已受理。

dispatcher 与 append 固定使用同一 MySQL 数据源。显式 `outbox.datasource_ref` 不一致时在 Ready 前
拒绝；省略该字段或仅配置轮询预算不改变绑定。远端不可用或已提交收据丢失时保持原事件重投，只有
`Committed` 或 `Duplicate` 才标记投递完成，不重新创建本地业务事实。
`napp_outbox_pending`、`napp_outbox_published_total` 与 `napp_outbox_dead` 描述投递侧状态，远端查询
描述流程状态；Ready 不保证队列为空或流程完成。独立宿主须自行维护相同的事务与扫描边界。

## 独立与 custom 使用

不使用 Application 或明确配置 plan_mode: custom 时，可以直接构造 Orchestrator::with_datasource、
ParticipantRuntime::with_datasource 和 provider-neutral publisher。调用方必须自行拥有 schema 门禁、
共享 Catalog、transport、Inbox/Outbox dispatcher、timer、Ready、服务发现和反向停机；这些入口不会
自行创建 listener 或后台任务。managed 与 configure_saga 不能同时成为权威。

### transport 构件

| feature | 能力 | managed 边界 |
| --- | --- | --- |
| kafka / 门面 saga-kafka | command/result consumer、手动 ACK、分区退避与耐久 DLT | napp 按角色构造 topic、consumer、publisher 与 broker ACK 门禁；connector 也可用于 custom |
| redis-stream / 门面 saga-redis-stream | XREADGROUP、XAUTOCLAIM、key id 到认证主体的验签、原子 DLT 与积压观测 | napp 按角色构造 stream publisher/consumer、group、动态 route 与停机排空 |
| provider-neutral HTTP | canonical HMAC、producer 绑定、共享重放存储与封闭收据 | napp 已构造完整 managed HTTP command/result 与业务 API |
| grpc-transport / 门面 saga-grpc | generated command/result service/client、mTLS principal 绑定与四值收据 | napp 自动构造 gRPC API、按角色登记 service、探测 health 并创建出站 publisher |

nasaga-runtime-core 的发布归档包含 saga_transport.proto 与 saga_orchestrator.proto，并从同一源生成
Rust client/server 和 descriptor。原始协议文件可用于非 Rust 客户端；`napp` 复用同一生成物完成
service、channel、mTLS/RBAC、health、reflection 与 discovery 装配，不维护第二份手写 wire 合同。

## 参与方接入

`#[saga]` 生成的 `saga_handle_command` 必须进入 `ParticipantRuntime`。运行时在 Ready 前冻结非空
`ParticipantCommandTrust`，精确绑定 workflow、`definition_version`、摘要与 Orchestrator 身份。
execute、cancel、compensate 和 resolve 都在 Inbox、gate、业务事实与 result Outbox 的本地事务内完成。

Kafka 入口使用 topic owner、route 与认证身份完成授权。其它 connector 应使用
`SagaHttpMessageAuthenticator`、`SagaHttpReplayGuard` 或等价的 mTLS 与共享 nonce claim，并为每条
producer/path 信任边分配独立容量。

## YML 配置

本 crate 不直接读取 yml；以下字段由 napp 解析后投影到 MySQL runtime。角色数据源必须显式位于角色
作用域，不能用 service-wide fallback 替代。

受管 Kafka 数据面同时依赖 Application Kafka client 的耐久 DLT 合同和 Saga topic 模板：

```yaml
kafka:
  client_name: saga-data
  bootstrap_servers: 127.0.0.1:9092
  group_id: saga-workers
  producer:
    acks: all
    enable_idempotence: true
  behavior:
    dead_letter_topic_suffix: .DLT
    dead_letter_required: true

saga:
  transport:
    address_policies:
      saga-participant-routing:
        kafka_topic_prefixes: [saga.commands]
    command_result:
      kind: kafka
      kafka:
        client_ref: saga-data
        command_topic: saga.commands
        result_topic: saga.results
        result_group: saga-orchestrator-results
        result_dlt_topic: saga.results.{owner}.DLT
        routing:
          mode: capability-registry
          address_policy_ref: saga-participant-routing
```

`result_dlt_topic` 中的 `{owner}` 是受管模板，不是字面消费 topic；实际 result topic 按认证 owner 派生，
DLT 必须等于该 topic 加 Kafka client 的 `dead_letter_topic_suffix`。broker 结果不明时保留原 Outbox，
不能把生产请求已进入本地队列当作 broker ACK。

```yaml
datasources:
  workflow:
    driver: mysql
    url: ${APP_WORKFLOW_MYSQL_URL}

outbox:
  datasource_ref: workflow

saga:
  role: orchestrator
  plan_mode: managed
  service_identity: checkout-orchestrator
  replica_identity: ${SAGA_REPLICA_ID}
  orchestrator:
    datasource_ref: workflow
    timer_poll_interval_ms: 500
    timer_error_backoff_ms: 1000
    timer_operation_timeout_ms: 5000
  definition_catalog:
    mode: dynamic
    datasource_ref: workflow
```

replica_identity 必须逐副本唯一且重启稳定，用于 timer 租约和审计；实际 fencing capability 还绑定
运行实例生成的随机 nonce，不能从配置复制。

## 提交与投递

- 每次推进把 Inbox、attempt journal、实例 CAS、迁移事实、下一 command Outbox 和 timer 放在同一
  本地事务。
- 只有提交已确认或 Inbox 判定重复时才能 ACK；提交结果不确定时保留原消息继续收敛。
- 确定性拒绝必须先持久化 DLT，再推进源 Outbox 或 offset。
- PAUSED 不消耗普通毒消息预算；恢复后仍从独立失败预算开始。
- Unknown 只能进入类型化 resolve 流程，不能直接按失败执行补偿。
- resolve 查询与原业务提交发生竞态时，Orchestrator 以重新读取的已提交成功投影推进；迟到查询应答
  仍进入 attempt journal，但不能把真实正向或补偿成功降级为人工介入。

## 管理与观测

管理调用方必须从 JWT 或 mTLS 构造 `SagaManagementContext`，不能信任请求正文自报 actor 或权限。
暂停、恢复和人工重开均使用唯一 `operation_id`，并在状态改变前写入可归因审计事实。

`SagaOperationalMetrics::render_prometheus` 输出固定、低基数指标，不包含 saga、租户、业务键、payload
或错误原文。数据库事实由当前 MySQL store 读取；配额、管理动作和 transport 的进程级计数来自
`nasaga-runtime-core` 的单一共享快照，mixed backend 宿主不能为每个 wrapper 重复登记同名计数。日志只
记录必要身份摘要、阶段、attempt、状态和操作主体。

管理面把读写权限分离：`list_instances` 使用 `saga.instance.list`，审计和精确租户用量使用
`saga.audit.read`；pause、resume、重开补偿、重开裁决和人工关闭分别要求对应写权限、已认证 actor、
reason 与唯一 `operation_id`。人工关闭只能把 `MANUAL_INTERVENTION` 收敛为
`MANUALLY_CLOSED`，不能伪装成自动完成或已补偿；启用前必须先升级全部读者，再打开
`OrchestratorConfig::enable_manual_close`。

实例列表把 tenant、可选 workflow、状态、创建时间窗与 saga_id cursor 全部下推到存储层。MySQL 对每个
状态沿 tenant/status 前导复合索引分别读取至多一页，再按全局 keyset 顺序有界合并；无状态条件和
PostgreSQL 使用各自与排序匹配的索引查询。稀疏状态位于历史尾部或不存在时，小页请求也不会扫描租户
全部实例。

控制态在初次读取时已经不允许 pause/resume 属于前置条件失败；初次快照允许动作而提交 CAS 失去竞争
属于可重试并发。协议适配器应分别映射为 `FAILED_PRECONDITION` 与 `ABORTED`，后者要求调用方重新加载
权威快照后再裁决。

## Trace、调度与租户治理

- `start_saga_traced` 显式接收已校验 `TraceContext`，实例保存 canonical `traceparent`；自动命令和
  结果从已提交上下文派生新 child span。缺少 trace 不影响投递，runtime 不读取 ambient trace。
- `derive_scheduled_business_key` 和 `start_scheduled_batch` 把任务名、名义调度时刻与对象稳定身份
  收敛为创建幂等键。领导权只控制领取新项，exactly-once 仍由数据库唯一事实承担。
- `tenant_quotas` 在创建事务内预留在飞实例名额，终态事务释放；未列出租户只观测不拒绝。存量库
  必须先事务内对账并置初始化标记，再启用上限。
- `tenant_action_rates` 使用数据库时钟的固定窗口限制变更类管理动作；预算与动作同事务提交，失败
  回滚退还，只读查询不占预算。完全相同的已提交 `operation_id` 在预留前返回，不重复计数；
  `max_actions = 0` 表示完全封禁新 operation。

运维至少关注 `nasaga_manual_intervention`、`nasaga_waiting_resolution`、`nasaga_due_timer`、
`nasaga_conflict_total`、`nasaga_quota_rejections_total`、`nasaga_action_rate_rejections_total`，再叠加
所选 transport 与受管 Outbox 的 ACK、重试、DLT、积压、保留清理和提交不确定指标。

## 明确边界

- 公开保证是本地 ACID、Outbox 至少一次、Inbox 幂等、持久化状态机和显式补偿组成的最终一致性。
- 不承诺物理 exactly-once、跨服务 ACID 或并发 Saga 隔离。
- `SagaHttpReplayGuard` 只适合单进程入口；多副本必须使用共享强一致 nonce claim 或等价网关能力。
- 远端定义摘要无法仅凭参与方本地投影推断；要求启动期拒绝漂移时，所有服务必须读取同一份受信、
  不可变定义快照。
- timer、dispatcher 和 consumer 的生命周期由宿主拥有，停机时应先关入口，再排空已接管工作。
- gRPC generated service 只有进入受管 listener、取得已验证 peer identity，并配置有界 deadline/drain
  后才构成完整入站链路；单独使用裁决器或 generated client 不会自行创建这些运行边界。

部署、恢复和容量边界见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/docs/saga-production.md)。
