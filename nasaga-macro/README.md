# nasaga-macro

`nasaga-macro` 提供参与方 `#[saga]` 属性宏，在编译期检查步骤声明，并生成静态 descriptor 与事务
adapter。业务通过 `nasa` 门面的 `saga-runtime` feature 使用，不需要直接依赖宏 crate。

宏的价值是把“声明了某种取消/裁决能力”和“类型确实实现该能力”绑定在一起，并把本地步骤投影加入
启动预检；它不会把业务方法包装成一个缺少 Inbox、gate 或 Outbox 的半事务入口。

```toml
[dependencies]
nasa = { version = "1", features = ["saga-runtime"] }
```

## 声明步骤

```rust
use nasa::saga::{saga, SagaStep};

#[saga(
    workflow = "checkout",
    version = 1,
    step = "reserve_inventory",
    compensable = true,
    cancel_mode = "local-fenceable",
    allow_unknown = false
)]
impl SagaStep for InventoryService {
    // SagaStep 的关联类型与业务方法由服务实现。
}
```

宏生成的 descriptor 会在进程进入 Ready 前与 `WorkflowDefinition` 精确比对，包括 workflow、
`definition_version`、摘要、步骤名、补偿能力、取消模式和 Unknown 策略。生成的
`saga_handle_command` 把命令交给 `ParticipantRuntime` 持有的完整事务边界，业务方法不得自行提交
结果消息。

```text
#[saga] 声明
  ├─ 编译期：属性组合 + trait 能力检查
  ├─ 链接期：descriptor 进入静态收集表
  ├─ Ready：descriptor 与受信 WorkflowDefinition 精确对齐
  └─ 运行期：saga_handle_command → ParticipantRuntime 完整事务 wrapper
```

## 合法声明

| `cancel_mode` | `allow_unknown` | 类型合同 |
| --- | --- | --- |
| `local-fenceable` | `false` | 实现 `SagaStep` |
| `resolve-only` | `true` | 同一类型实现 `SagaResolveStep`，裁决模式为 Poll |
| `externally-cancellable` | `true` | 同一类型实现 `SagaCancelStep + SagaResolveStep`，裁决模式为 Poll |

缺少必要的 typed adapter、使用不支持的组合或把属性标在错误的 impl 形态上会直接编译失败。

## YML 配置

本 crate 不读取 yml。属性中的名称、定义版本和能力声明属于静态业务合同；transport 身份、数据库、
topic 路由和投递预算由运行时与部署配置负责。

## 主要边界

- 宏只约束当前参与方的公开类型与声明，不负责全局状态推进或恢复。
- 远程副作用必须使用稳定 `effect_id`、受限 Service API 和目标系统幂等键。
- 业务方法不能绕过 `ParticipantRuntime` 另开事务，否则 gate、业务事实与结果 Outbox 无法原子提交。
- `externally-cancellable` 的取消结论必须来自真实业务裁决，不能由 adapter 推测。

完整运行合同见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/saga-production.md)。
