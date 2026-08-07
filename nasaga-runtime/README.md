# nasaga-runtime

`nasaga-runtime` 将 `nasaga-core` 合同与 MySQL store 组装为持久化 Orchestrator、参与方事务
adapter、管理审计、运行指标以及可选的 Kafka command/result consumer。业务通过 `nasa` 门面启用：

```toml
[dependencies]
nasa = { version = "1", features = ["saga-runtime"] }
# Kafka 托管消费入口使用 features = ["saga-kafka"]
```

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

宿主负责拥有 Kafka 消费循环、timer 轮询循环和停机顺序；本 crate 不自行启动无限后台任务。

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
  orchestrator_id: checkout-orchestrator-a
  timer_poll_interval_ms: 200
  timer_lease_ms: 5000
  command_retry_limit: 16
```

`orchestrator_id` 需要逐副本唯一且重启稳定，用于租约归属和审计；实际 fencing capability 还绑定
运行实例的随机 nonce，不能从配置注入。

## 提交与投递

- 每次推进把 Inbox、attempt journal、实例 CAS、迁移事实、下一 command Outbox 和 timer 放在同一
  本地事务。
- 只有提交已确认或 Inbox 判定重复时才能 ACK；提交结果不确定时保留原消息继续收敛。
- 确定性拒绝必须先持久化 DLT，再推进源 Outbox 或 offset。
- PAUSED 不消耗普通毒消息预算；恢复后仍从独立失败预算开始。
- Unknown 只能进入类型化 resolve 流程，不能直接按失败执行补偿。

## 管理与观测

管理调用方必须从 JWT 或 mTLS 构造 `SagaManagementContext`，不能信任请求正文自报 actor 或权限。
暂停、恢复和人工重开均使用唯一 `operation_id`，并在状态改变前写入可归因审计事实。

`SagaOperationalMetrics::render_prometheus` 输出固定、低基数指标，不包含 saga、租户、业务键、payload
或错误原文。日志只记录必要身份摘要、阶段、attempt、状态和操作主体。

## 主要边界

- 公开保证是本地 ACID、Outbox 至少一次、Inbox 幂等、持久化状态机和显式补偿组成的最终一致性。
- 不承诺物理 exactly-once、跨服务 ACID 或并发 Saga 隔离。
- `SagaHttpReplayGuard` 只适合单进程入口；多副本必须使用共享强一致 nonce claim 或等价网关能力。
- 远端定义摘要无法仅凭参与方本地投影推断；要求启动期拒绝漂移时，所有服务必须读取同一份受信、
  不可变定义快照。
- timer、dispatcher 和 consumer 的生命周期由宿主拥有，停机时应先关入口，再排空已接管工作。

部署、恢复和容量边界见 [Saga 生产运行指南](../docs/saga-production.md)。
