# naauthz

`naauthz` 提供 route scope 决策、校验后原子发布的 policy registry，以及对象级授权的 fail-closed
请求快照。它只处理授权，不负责 token 验签；认证后的 `Principal` 必须由上游身份层提供。

应用通过 `nasa::application` 取得这些类型，并在启动 Hook 注入 registry：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "oauth", "web-security"] }
```

```rust
use std::collections::BTreeSet;
use std::sync::Arc;
use nasa::application::{PolicyRegistry, PolicySet, RequireMode, RoutePolicy};

#[nasa::application("auth", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    let policies = PolicySet::build(vec![RoutePolicy {
        route_id: "POST /orders/{id}/cancel".into(),
        required_scopes: BTreeSet::from(["orders.cancel".into()]),
        mode: RequireMode::All,
    }])?;
    app.set_authz_registry(Arc::new(PolicyRegistry::new(policies)))?;
    Ok(())
}
```

动态更新使用 `PolicyRegistry::reload`。候选校验失败时保留 last-good，成功时 generation 单调递增。

## 对象级授权

实现 `ObjectAuthorizer` 后通过 `Application::set_object_authorizer` 注入。Web 请求边界会冻结
principal、policy set、generation、provider 和超时，handler 内应复用同一个
`RequestSecurityContext`，不能在请求中途重新读取全局 registry。

## YML 配置

本 crate 不规定策略 yml。静态策略可由业务配置反序列化后构造 `PolicySet`；远程策略由业务 provider
拉取、完整校验后再 `reload`。身份组件自身的 issuer、audience 和 JWKS 配置位于 `auth:`。

## 主要边界

- route ID 使用 `METHOD /path/{param}` 模板，不使用带实际对象 ID 的原始路径。
- `PolicySet::decide` 对未配置 route 返回 Permit；需要保护的 route 必须在启动审计中确认已命中策略。
- 对象 provider 缺失、拒绝、错误或超时都必须 fail closed。
- `RequestSecurityContext` 的非 fallible 对象超时参数最长按 365 天执行，极端 `Duration` 不进入
  不可表示的 Tokio deadline。
- 对象 ID 不得进入日志、指标标签或错误正文。
