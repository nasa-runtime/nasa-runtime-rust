# nasaga-core

`nasaga-core` 是无 I/O 的 Saga 合同层，负责流程定义、稳定身份、结果分类、封闭状态机和冻结补偿计划。
业务应用通常通过 `nasa` 门面的 `saga` feature 使用；只有自定义运行时或持久化适配器
需要直接依赖本 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["saga"] }
```

## 定义流程

```rust
use std::time::Duration;
use nasa::saga::{
    CancelMode, Compensation, DefinitionVersion, ResolutionMode, ResolutionSpec,
    ServiceIdentity, StepDefinition, StepName, TimeoutPolicy, WorkflowDefinition, WorkflowName,
};

let payment = StepDefinition::new(
    StepName::new("capture_payment")?,
    ServiceIdentity::new("payment-service")?,
    Compensation::NonCompensable,
    CancelMode::ResolveOnly,
    ResolutionSpec::allowed(ResolutionMode::Poll, Duration::from_secs(120)),
    Duration::from_secs(15),
    TimeoutPolicy::AcceptLateSuccess,
);

let definition = WorkflowDefinition::new(
    WorkflowName::new("checkout")?,
    DefinitionVersion::new(1)?,
    vec![payment],
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

流程定义发布后，同一 `definition_version` 对应的步骤、owner、取消策略、裁决预算和顺序必须保持
不变。任何语义变化都使用新的定义版本，运行中实例始终按创建时绑定的摘要恢复和补偿。

## 身份语义

- `saga_id` 标识一次流程实例，`business_key` 负责业务创建幂等。
- `effect_id` 标识当前阶段的业务效果，跨投递尝试保持稳定。
- `command_id` 标识单次投递，不能替代业务效果身份。
- 取消与裁决通过 `target_phase()` 和 `target_effect_id()` 指向原始操作，不能使用当前控制命令的身份
  查询外部事实。

## YML 配置

本 crate 不读取 yml。流程定义应来自受信、不可变的应用配置或控制面快照，并在进程进入 Ready 前
注册到运行时。

## 主要边界

- `LocalFenceable` 不允许产生 Unknown 结果。
- `ResolveOnly` 必须声明有界裁决策略；当前运行时支持 Poll。
- 不可补偿的 pivot 只能位于流程末端，避免补偿计划包含无法撤销的已成功步骤。
- Saga 提供本地事务、可靠消息和补偿组成的最终一致性，不提供跨服务 ACID 或并发隔离。
- 资源竞争仍需由业务唯一键、条件更新、语义锁或可交换操作保护。

部署与恢复边界见 [Saga 生产运行指南](../docs/saga-production.md)。
