# naauthz

`naauthz` 提供 route scope 决策、校验后原子发布的 policy registry，以及对象级授权的 fail-closed
请求快照。它只处理授权，不负责 token 验签；认证后的 `Principal` 必须由上游身份层提供。

## 核心价值与裁决架构

核心价值是让 route 策略、未命中缺省、公开 route 豁免和 generation 只以完整快照发布与使用，避免
Web 边界、低层 registry 调用和 handler 在同一请求中得到不同裁决。候选策略或覆盖账目校验失败时
保留 last-good；显式策略优先于未命中豁免，对象 provider 缺失或不能确认授权时拒绝访问。

```text
候选 PolicySet + UnmatchedRoutePolicy + routes/protected routes
                    │ 完整校验
                    v
   原子发布 policy + unmatched + exemptions + generation
                    │
       ┌────────────┼────────────┐
       v            v            v
 Web route      registry      handler 对象授权
```

本 crate 不认证 token、不决定业务对象归属，也不从 URL 实例值生成策略 ID。接入层必须提供已验证
`Principal`、稳定 route 模板和公开 route/健康探针覆盖合同。

应用通过 `nasa::application` 取得这些类型，并在启动 Hook 注入 registry：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["application", "oauth", "web-security"] }
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

Web Ready 会把静态路由与显式动态路由安装为覆盖合同。动态更新使用 `PolicyRegistry::reload`：
候选除结构校验外还会复验悬空策略与当前 fail-closed 覆盖面，失败时保留 last-good，成功时
generation 单调递增。需要同时切换三态缺省时使用 `reload_with_unmatched`，策略与缺省只会作为
同一 generation 整帧发布，不存在新策略搭配旧缺省的窗口。

## 未命中缺省(三态)

route 未命中任何策略时的缺省由 `UnmatchedRoutePolicy` 承载，词表 `permit`/`observe`/`deny`：
`permit` 是兼容缺省(未配置即放行)；`observe` 放行但为每次"若翻转为 deny 将被拒绝"的命中留下
证据，供存量业务在真实流量下清点漏配面；`deny` 把缺省翻转为 fail-closed，与对象授权的失败语义
对齐。接入层(如 `napp` Web)负责为声明公开的路由与健康探针提供显式豁免、执行启动期覆盖对账
(悬空策略阻断 Ready、deny 下未覆盖鉴权路由阻断 Ready)并暴露计数；安装覆盖合同后，本 crate
会把非保护 route 的豁免集合并入同一裁决快照，在每次 reload 前执行相同复验，并让覆盖账目随成功
generation 更新。显式策略仍先于豁免裁决，因此策略可收紧原本公开的 route。

## 对象级授权

实现 `ObjectAuthorizer` 后通过 `Application::set_object_authorizer` 注入。Web 请求边界会冻结
principal、policy set、generation、provider 和超时，handler 内应复用同一个
`RequestSecurityContext`，不能在请求中途重新读取全局 registry。

## YML 配置

本 crate 不规定策略 yml。静态策略可由业务配置反序列化后构造 `PolicySet`；远程策略由业务 provider
拉取、完整校验后再 `reload`。身份组件自身的 issuer、audience 和 JWKS 配置位于 `auth:`。

## 主要边界

- route ID 使用 `METHOD /path/{param}` 模板，不使用带实际对象 ID 的原始路径。
- `PolicySet::decide_explicit_policy` 只裁决显式策略，未命中返回 `None`；完整授权必须使用
  `PolicyDecisionSnapshot::decide`、`PolicyRegistry::decide` 或请求内的
  `RequestSecurityContext::decide_route`。这三个入口冻结并应用同代 `UnmatchedRoutePolicy` 与公开 route
  豁免；已安装覆盖合同的 registry 仍会在启动和每次 reload 时拒绝悬空策略，并在 deny 下拒绝未覆盖
  route。
- 对象 provider 缺失、拒绝、错误或超时都必须 fail closed。
- `RequestSecurityContext` 的非 fallible 对象超时参数最长按 365 天执行，极端 `Duration` 不进入
  不可表示的 Tokio deadline。
- 对象 ID 不得进入日志、指标标签或错误正文。
