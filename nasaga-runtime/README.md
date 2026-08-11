# nasaga-runtime

`nasaga-runtime` 将 `nasaga-core` 合同与 MySQL store 组装为持久化 Orchestrator、参与方事务
adapter、管理与恢复入口、运行指标以及可选 transport connector。它的核心价值是让分布式步骤在
进程崩溃、至少一次重投和多副本竞争下仍由已提交事实继续推进，并在无法确定结果时停在可裁决状态，
而不是猜测成功或失败。业务通过 `nasa` 门面启用：

```toml
[dependencies]
nasa = { version = "1", features = ["saga-runtime"] }
# Kafka 托管消费入口使用 features = ["saga-kafka"]
# Redis Streams 托管消费入口使用 features = ["saga-redis-stream"]
# gRPC 收据裁决仍为实验能力：features = ["saga-grpc-experimental"]
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

## Orchestrator 初始化

进程开放业务路由前必须注册所有仍有活跃实例的 `WorkflowDefinition`，再调用
`Orchestrator::verify_startup`。摘要漂移、descriptor 不一致或缺少定义时拒绝进入 Ready。

```rust
use nasa::saga::{DefinitionRegistry, Orchestrator, OrchestratorConfig};

let mut registry = DefinitionRegistry::new();
registry.register(checkout_definition)?;

let orchestrator = Orchestrator::new(registry, OrchestratorConfig::default())?;
orchestrator.verify_startup().await?;
```

使用 `#[nasa::application("saga")]` 时，将运行时提交给 Application，合同校验、历史实例门禁、timer
轮询和停机保护态由组件统一执行：

```rust
let publisher = std::sync::Arc::new(build_command_publisher()?);
app.configure_saga(
    nasa::application::SagaApplicationPlan::orchestrator(
        std::sync::Arc::new(orchestrator),
        "checkout-orchestrator-a",
    )?
    .with_event_publisher(publisher)?,
)?;
```

声明 `"saga"` 会隐式纳入数据库与 Outbox 生命周期，但不会隐式选择 transport。发布端只依赖
`OutboxPublisher`，可映射到 Kafka、Redis Streams、HTTP 或 gRPC；消费侧必须提供与自身介质匹配的
ACK/收据、重领和 DLT 安全语义。

| feature | 能力 | 稳定性与边界 |
| --- | --- | --- |
| `kafka` / 门面 `saga-kafka` | command/result 托管 consumer、手动 ACK、分区退避、durability-first DLT | 稳定；topic owner、group 与 ACL 由部署显式配置 |
| `redis-stream` / 门面 `saga-redis-stream` | XREADGROUP、XAUTOCLAIM、显式 ACK、原子 DLT、签名、积压与安全清剪 | 稳定；Application 托管时还要声明 `redis` 组件 |
| provider-neutral HTTP 认证类型 | canonical HMAC、显式 producer、replay 与容量观测 | 只提供认证和裁决构件；listener、共享 nonce store 与路由由宿主持有 |
| `grpc-transport` / 门面 `saga-grpc-experimental` | command/result 服务端裁决器和封闭收据 | 实验；不生成 service，也不拥有 listener、mTLS、deadline 或 drain |

不使用 Application 组件的宿主仍需自行拥有消息消费循环、timer 轮询循环和停机顺序；本 crate
不自行启动无限后台任务。

## 参与方接入

`#[saga]` 生成的 `saga_handle_command` 必须进入 `ParticipantRuntime`。运行时在 Ready 前冻结非空
`ParticipantCommandTrust`，精确绑定 workflow、`definition_version`、摘要与 Orchestrator 身份。
execute、cancel、compensate 和 resolve 都在 Inbox、gate、业务事实与 result Outbox 的本地事务内完成。

Kafka 入口使用 topic owner、route 与认证身份完成授权。其它 connector 应使用
`SagaHttpMessageAuthenticator`、`SagaHttpReplayGuard` 或等价的 mTLS 与共享 nonce claim，并为每条
producer/path 信任边分配独立容量。

## YML 配置

本 crate 不直接读取 yml。宿主应从受信配置构造数据库、流程定义、transport 路由和运行预算。推荐
投影如下：

```yaml
database:
  url: ${APP_MYSQL_URL}
  max_connections: 32

kafka:
  brokers: ${APP_KAFKA_BROKERS}
  security_protocol: SASL_SSL

saga:
  database_bootstrap: application
  timer_poll_interval_ms: 500
  timer_error_backoff_ms: 1000
  timer_operation_timeout_ms: 5000
  timer_failure_threshold: 3
```

timer owner 通过 `SagaApplicationPlan::orchestrator` 提交，需要逐副本唯一且重启稳定，用于租约归属
和审计；实际 fencing capability 还绑定运行实例的随机 nonce，不能从配置注入。

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
或错误原文。日志只记录必要身份摘要、阶段、attempt、状态和操作主体。

管理面把读写权限分离：`list_instances` 使用 `saga.instance.list`，审计和精确租户用量使用
`saga.audit.read`；pause、resume、重开补偿、重开裁决和人工关闭分别要求对应写权限、已认证 actor、
reason 与唯一 `operation_id`。人工关闭只能把 `MANUAL_INTERVENTION` 收敛为
`MANUALLY_CLOSED`，不能伪装成自动完成或已补偿；启用前必须先升级全部读者，再打开
`OrchestratorConfig::enable_manual_close`。

## Trace、调度与租户治理

- `start_saga_traced` 显式接收已校验 `TraceContext`，实例保存 canonical `traceparent`；自动命令和
  结果从已提交上下文派生新 child span。缺少 trace 不影响投递，runtime 不读取 ambient trace。
- `derive_scheduled_business_key` 和 `start_scheduled_batch` 把任务名、名义调度时刻与对象稳定身份
  收敛为创建幂等键。领导权只控制领取新项，exactly-once 仍由数据库唯一事实承担。
- `tenant_quotas` 在创建事务内预留在飞实例名额，终态事务释放；未列出租户只观测不拒绝。存量库
  必须先事务内对账并置初始化标记，再启用上限。
- `tenant_action_rates` 使用数据库时钟的固定窗口限制变更类管理动作；预算与动作同事务提交，失败
  回滚退还，只读查询不占预算。`max_actions = 0` 表示完全封禁。

运维至少关注 `nasaga_manual_intervention`、`nasaga_waiting_resolution`、`nasaga_due_timer`、
`nasaga_conflict_total`、`nasaga_quota_rejections_total`、`nasaga_action_rate_rejections_total`，再叠加
所选 transport 与受管 Outbox 的 ACK、重试、DLT、积压、保留清理和提交不确定指标。

## 主要边界

- 公开保证是本地 ACID、Outbox 至少一次、Inbox 幂等、持久化状态机和显式补偿组成的最终一致性。
- 不承诺物理 exactly-once、跨服务 ACID 或并发 Saga 隔离。
- `SagaHttpReplayGuard` 只适合单进程入口；多副本必须使用共享强一致 nonce claim 或等价网关能力。
- 远端定义摘要无法仅凭参与方本地投影推断；要求启动期拒绝漂移时，所有服务必须读取同一份受信、
  不可变定义快照。
- timer、dispatcher 和 consumer 的生命周期由宿主拥有，停机时应先关入口，再排空已接管工作。
- gRPC connector 不等于 gRPC 服务端；没有受管 listener、已验证 peer identity 和有界 deadline/drain
  时，不构成可投产链路。

部署、恢复和容量边界见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。
