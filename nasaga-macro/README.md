# nasaga-macro

`nasaga-macro` 提供 MySQL/PostgreSQL 参与方 `#[saga]` 属性宏，在编译期检查步骤声明，并生成静态
descriptor 与对应后端的事务 adapter。业务通过 `nasa` 门面的 `saga-runtime` 或
`saga-runtime-pgsql` feature 使用，不需要直接依赖宏 crate。

宏的价值是把“声明了某种取消/裁决能力”和“类型确实实现该能力”绑定在一起，并把本地步骤投影加入
启动预检；它不会把业务方法包装成一个缺少 Inbox、gate 或 Outbox 的半事务入口。

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["saga-runtime"] }
```

PostgreSQL 把 feature 换为 `saga-runtime-pgsql`，并从 `nasa::saga::pgsql::saga` 导入属性宏；
`SagaStep` 等纯逻辑合同仍从 `nasa::saga` 导入。宏在编译期固定事务后端，不根据连接 URL 猜测 driver。

## 声明步骤

```rust
use nasa::saga::{saga, SagaStep};

#[saga(
    workflow = "checkout",
    version = 1,
    step = "reserve_inventory",
    compensable = true,
    cancel_mode = "local-fenceable",
    allow_unknown = false,
    managed = true
)]
impl SagaStep for InventoryService {
    // SagaStep 的关联类型与业务方法由服务实现。
}
```

宏生成的 descriptor 会在进程进入 Ready 前与 `WorkflowDefinition` 精确比对，包括 workflow、
`definition_version`、摘要、步骤名、补偿能力、取消模式和 Unknown 策略。生成的
`saga_handle_command` 把命令交给 `ParticipantRuntime` 持有的完整事务边界，业务方法不得自行提交
结果消息。`managed = true` 还生成由 napp 构造 handler 所需的只读工厂；participant 的业务 main 不
登记 Router、Inbox、Outbox、publisher 或 capability。未声明 managed 的 adapter 保留给 custom 计划。

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

`content_type` 与 `schema_id` 可声明步骤接收的字节合同，例如
`content_type = "application/octet-stream", schema_id = "inventory-command"`；省略时保持
`application/json` 与空 schema。两者进入 descriptor、受管工厂与 capability，并与 definition 中
`SagaPayloadContract` 精确比对。业务从步骤上下文读取原始字节；宏不会推测 schema 或转换媒体类型。

## 完整流程定义

`#[nasa::saga_workflow]` 标记 workflow owner 提供的 definition factory。返回值必须是
`anyhow::Result<WorkflowDefinition>`；宏把 factory 放入链接期只读集合，napp 在 managed dynamic
Catalog 模式下按配置租户生成 canonical artifact、digest 与完整 seal，再幂等发布。该 artifact 必须
显式包含步骤顺序、owner、补偿/pivot、取消、resolve 与 timeout，Orchestrator 不从零散 capability
推断流程是否完整。

```rust
#[nasa::saga_workflow]
fn checkout() -> anyhow::Result<nasa::saga::WorkflowDefinition> {
    build_checkout_definition()
}
```

业务 main 不调用 register 或 publish。动态远程发布还由 `napp` 使用 secret reference 指向的 Ed25519
私钥签名 canonical artifact，Registry 从独立受信公钥表复验 key id、签名与认证 owner。同一 definition
key 的内容变化必须提高 definition_version；已激活版本不能原地覆盖。

## YML 配置

本 crate 不读取 yml。属性中的名称、定义版本和能力声明属于静态业务合同；transport 身份、数据库、
topic 路由和投递预算由运行时与部署配置负责。

## 主要边界

- 宏只约束当前参与方的公开类型与声明，不负责全局状态推进或恢复。
- 远程副作用必须使用稳定 `effect_id`、受限 Service API 和目标系统幂等键。
- 业务方法不能绕过 `ParticipantRuntime` 另开事务，否则 gate、业务事实与结果 Outbox 无法原子提交。
- `externally-cancellable` 的取消结论必须来自真实业务裁决，不能由 adapter 推测。

完整运行合同见
[Saga 生产运行指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/main/docs/saga-production.md)。
