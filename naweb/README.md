# naweb

`naweb` 是 NASA 的 Web 路由、interceptor 与端点安全运行时。业务通常不直接依赖它，而是通过
`nasa::web` 门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["web"] }
```

```rust
use nasa::web::{get_mapping, mvc_router};

#[get_mapping("/health")]
async fn health() -> &'static str {
    "ok"
}

// crate 根声明一次,生成 crate::__mvc 收集模块
mvc_router!(());

let app = crate::__mvc::register_all(axum::Router::new());
```

本 crate 的稳定职责是：

- 重导出 `naweb-macro` 的路由注解、`#[interceptor]` 和 `mvc_router!`；
- 提供 `MappingPlan`、`MappingRuntime`、静态路由合同、effective plan 与启动/热更新审计；
- 按 feature 提供 auth gate、请求/响应加解密、replay、密钥运行时和低基数安全指标；
- 提升常用 Axum Web 类型，并通过 `__private` 为宏展开桥接依赖。

端口监听、context path、探针、请求排空和优雅停机属于 `napp` 的 Web 组件，不属于 `naweb`。

业务代码不要依赖 `naweb::__private`，它只服务于宏展开。

`#[application("web")]` 的项目**不要**再手写 `mvc_router!`：属性入口会在 crate 根自动生成收集端（再手写会因 `crate::__mvc` 重复定义而编译失败），路由装配、监听与优雅停机由应用运行时接管，业务定制经 `app.configure_router(...)` 注入。

`nasa::web` 直接提供稳定 Web 类型：`Json` / `Form` / `Path` / `Query` / `State` / `HeaderMap` / `StatusCode` / `Router` / `get`…`delete` / `from_fn` / `from_fn_with_state` / `Request` / `Next` 等，业务写 handler 与中间件不必直连 Axum 内部路径。

## YML 配置与使用

`naweb` 不读取 yml。路由路径、HTTP 方法、`produces`、`consumes` 都写在属性宏上；服务监听地址、context path 和中间件开关由业务应用配置。

推荐应用配置：

```yaml
server:
  host: 0.0.0.0
  port: 8080
  context_path: /order
  request_body_limit_bytes: 10485760
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `server.host` | axum 监听 host。 |
| `server.port` | axum 监听端口。 |
| `server.context_path` | 应用统一前缀；可在业务装配 Router 时 nest。 |
| `request_body_limit_bytes` | 请求体大小上限；由业务中间件配置。 |

使用代码：

```rust
let router = crate::__mvc::register_all(axum::Router::new());
let app = axum::Router::new().nest(&cfg.server.context_path, router);
```

路由本身继续写在函数属性上，例如 `#[get_mapping("/health")]`。

常见漏项：

| 现象 | 检查点 |
| --- | --- |
| 路由函数编译了但没有注册 | crate 根是否调用过 `mvc_router!(())`。 |
| 加了统一前缀后 404 | `register_all` 生成的是无前缀 Router,业务装配时再 `nest`。 |
| 宏路径找不到 | 应用是否开启 `nasa` 的 `web` feature。 |

## 端点安全特性

`naweb` 的身份与密码依赖均按 feature 隔离：

| 门面 feature | 能力 | 额外依赖 |
| --- | --- | --- |
| `web` | 路由注解、媒体类型、Web 类型和收集注册 | 不编译身份和密码实现 |
| `web-auth` | `AuthProvider`、`AuthCondition`、`AuthContext` | 异步身份运行时 |
| `web-crypto` | legacy-v1、modern-v2、key ring、replay、资源治理 | `ncrypto`、JSON、随机数、清零容器 |
| `web-crypto-legacy-rsa` | 受控迁移的 legacy RSA 私钥路径 | 编译期风险门；仍需 provider 运行时显式允许 |
| `web-security` | 固定 auth/crypto endpoint composer | 同时启用前两项 |

业务依赖示例：

```toml
[dependencies]
nasa = { version = "1", features = ["web-security"] }
```

`web-security` 默认不开放 legacy RSA 私钥运算，也不会因 `full` 隐式开放。只有仍需历史 RSA
线协议的迁移服务才同时启用 `web-crypto-legacy-rsa`；`LocalCryptoProvider` 的运行时允许开关
仍须显式为真，路由审计会拒绝能力与 key ring 不匹配的配置。

安全应用必须使用可失败注册入口，并在监听端口前完成全路由审计：

```rust
let runtime = std::sync::Arc::new(mapping_runtime);
let router = crate::__mvc::try_register_all(
    axum::Router::new(),
    runtime,
    naweb::MappingPlan::new(),
    state.clone(),
)?;
```

旧 `register_all` 只服务完全没有 auth/crypto 元数据的兼容路由；遇到安全路由会拒绝启动，不能静默忽略策略。

## 通用 Interceptor

`#[interceptor]` 把业务函数声明成可被 naweb 编排的 Axum interceptor。naweb 只拥有阶段、
作用域、排序、启动审计和 `AuthContext` gate；Token Header、Session DTO、Redis Key、白名单和
principal 都留在业务 crate。这样业务可以实现 Token、签名或设备认证，又不会绕开固定安全顺序。

函数签名不是固定三个参数：**最后两个参数必须依次是 `Request, Next`**，前面可以放任意
`FromRequestParts` extractor，例如 `State<T>`、`Extension<T>` 和 `InterceptorContext`。Body extractor
（`Json`、`Form` 或另一份 `Request`）不能放在前面，因为它会与后续解密及 handler 争抢请求体。

```rust
use nasa::web::{get_mapping, interceptor, InterceptorContext};
use nasa::web::{Next, Request, Response, State};

#[interceptor(id = "account-token", kind = "auth", order = 100)]
async fn account_token(
    State(state): State<AppState>,
    context: InterceptorContext,
    mut request: Request,
    next: Next,
) -> Response {
    // Token 校验属于应用；成功后把 AuthContext 写入 request extensions。
    let _route_id = context.route_id();
    // 示例 helper 在失败时直接构造 401；成功时把 AuthContext 写入 extensions。
    authenticate_and_insert_context(&state, &mut request).await;
    next.run(request).await
}

#[get_mapping(
    path = "/account/profile",
    auth = "required",
    interceptors(account_token)
)]
async fn profile() {
    // required gate 已经确认 AuthContext 存在。
}
```

`kind` 只允许以下三个固定阶段，业务 `order`、`before`、`after` 都不能跨阶段改写它们：

| `kind` | 入站位置 | 常见用途 |
| --- | --- | --- |
| `edge` | 整个安全流水线之前 | 请求关联、最外层审计、最终响应观察 |
| `auth` | `AuthContext` gate 和 request decrypt 之前 | Token、签名、设备身份认证 |
| `plaintext` | request decrypt 之后、handler 之前 | 需要读取明文语义的业务检查 |

`#[interceptor]` 默认只声明、不激活。业务可以选择四种装配方式：

- 路由属性 `interceptors(name, ...)`：精确绑定被标注的那一条 `*_mapping` 路由；
- `MappingPlan::scope`：绑定一个静态路径层级；
- `MappingPlan::global`：在启动 Hook 手动绑定全部自动 mapping 端点；
- `#[interceptor(..., global = true)]`：由 `mvc_router!` 链接期收集并自动全局绑定。

`global` 默认为 `false`，因此已有 interceptor 不会因为升级依赖而改变覆盖范围。自动全局只覆盖当前
crate 的 `*_mapping` 端点，不覆盖手写 Axum 路由和框架探针；要求当前 crate 已调用 `mvc_router!`，
并且函数无 State 或使用与 Router 根 State 相同的 `State<T>`。需要 `binding_with` 注入窄 State、
需要 scope/`when_route` 或动态开关时仍然手动装配。
`kind = "auth"` 的自动项仍只参与 `auth = "required|optional"` 路由；public 或未声明 auth 的路由会
排除 global/scope auth，不会因为全局声明而被偷偷改成受保护端点。

手动 global（缺省 `global = false`）与自动 global 是两种独立写法：

```rust
#[interceptor(id = "manual-audit", kind = "edge", order = 10)]
async fn manual_audit(request: Request, next: Next) -> Response {
    audit(request, next).await
}

let plan = naweb::MappingPlan::new().global(manual_audit::binding());
```

```rust
#[interceptor(id = "automatic-audit", kind = "edge", order = 10, global = true)]
async fn automatic_audit(request: Request, next: Next) -> Response {
    audit(request, next).await
}

// automatic_audit 不要再放入 MappingPlan，try_register_all 会自动合并它。
```

自动 global 与手动 binding 最终进入同一个 effective plan。若同一 ID 在同一路由重叠，启动审计会
明确报重复错误，不会静默去重或执行两遍。自动收集项先按 `stage + order + id + handler` 稳定排序，
不依赖 linkme 的链接顺序。

静态路径 scope 的手动写法如下：

```rust
let plan = naweb::MappingPlan::new()
    .scope("/account", account_scope::binding())?;

let router = crate::__mvc::try_register_all(
    axum::Router::new(),
    plan.runtime_or_default(),
    plan,
    state.clone(),
)?;
```

effective plan 的入站顺序固定为
`edge -> auth -> AuthContext gate -> decrypt/replay -> plaintext -> handler`，出站按 Tower 反向返回。
同阶段再按 `global -> 外层 scope -> 内层 scope -> endpoint` 排列，`order/before/after` 只调整同一
scope 内的顺序。public 路由会在任何请求级判断之前移除所有 auth binding；required 路由没有可建立
`AuthContext` 的 auth binding 时会在监听前失败。普通 auth interceptor 不能与同一路由的
`auth_provider/auth_condition` 混用；若应用要由新宏调用可热更新 AuthRuntime 注册表，必须在
`#[interceptor]` 显式声明 `auth_runtime = true`。该声明会让 provider/condition 依赖继续参与
启动、readiness 和 last-good 替换审计，不是第二条请求期鉴权链。

带 `State<T>` 的 interceptor 会生成两种 helper：`binding()` 要求 `T` 就是 Router 根 State；
`binding_with::<RootState>(state)` 把启动期构造的一份窄 State 绑定到根 Router。后者适合高频路径，
但窄 State 只能保存 Application 受管资源的 clone handle，不能取得关闭权。自定义 Tower Layer 可用
`InterceptorBinding::new`，仍然必须进入同一 effective plan 和启动审计。

## `*_mapping` 路由宏使用手册

`naweb` 提供以下五个属性宏。它们使用同一组属性，区别只是注册的 HTTP 方法以及
`consumes` 的默认值：

| 宏 | HTTP 方法 | 普通路由未填写 `consumes` 时的行为 | 是否允许 `decrypt = true` |
| --- | --- | --- | --- |
| `#[get_mapping(...)]` | GET | 不检查请求 Content-Type | 否 |
| `#[post_mapping(...)]` | POST | 普通路由默认要求 `application/json` | 是 |
| `#[put_mapping(...)]` | PUT | 不检查请求 Content-Type | 是 |
| `#[patch_mapping(...)]` | PATCH | 不检查请求 Content-Type | 是 |
| `#[delete_mapping(...)]` | DELETE | 不检查请求 Content-Type | 是 |

这里的“允许解密”只表示宏允许该 HTTP 方法携带需要解密的请求体。是否真的执行解密仍由
`decrypt` 决定。GET 当前不接受请求体解密，但可以用于普通查询、身份认证或 legacy-v1
响应加密。

### 基本写法

只有路径时可以使用单字符串简写：

```rust
#[get_mapping("/health")]
async fn health() -> &'static str {
    "ok"
}
```

需要设置其它属性时使用 `key = value`：

```rust
#[post_mapping(
    path = "/orders",
    consumes = "application/json",
    produces = "application/json",
    auth = "required",
    auth_provider = "session"
)]
async fn create_order() {
    // 业务处理
}
```

`path` 和 `value` 完全同义，下面两种写法会注册同一条路由：

```rust
#[get_mapping(path = "/health")]
#[get_mapping(value = "/health")]
```

同一个函数应只选择其中一种写法，不要重复声明路径；同时填写时当前实现会采用后解析的值，
这种写法语义不清晰且不作为稳定合同。路径必须以 `/` 开头，并且是相对于应用 context path
的路由模板；例如应用统一挂载在 `/order`，宏中写 `/health`，最终访问地址是 `/order/health`。

### 五个 HTTP 宏分别怎么用

`get_mapping` 用于读取资源。GET 没有默认请求媒体类型，也不能启用请求体解密：

```rust
#[get_mapping(path = "/orders/:id", produces = "application/json")]
async fn get_order() {
    // 查询单个订单
}
```

`post_mapping` 通常用于创建资源或执行命令。它是唯一一个在普通路由上默认要求
`application/json` 的宏：

```rust
#[post_mapping(path = "/orders")]
async fn create_order() {
    // 即使省略 consumes，客户端也必须发送 application/json
}
```

`put_mapping` 通常用于完整替换资源。它不会自动要求 JSON，需要时应显式声明：

```rust
#[put_mapping(path = "/orders/:id", consumes = "application/json")]
async fn replace_order() {
    // 完整替换订单
}
```

`patch_mapping` 通常用于部分修改资源，同样不提供默认 `consumes`：

```rust
#[patch_mapping(path = "/orders/:id", consumes = "application/json")]
async fn update_order_fields() {
    // 部分更新订单字段
}
```

`delete_mapping` 用于删除资源。普通 DELETE 通常没有 body，所以默认不检查 Content-Type；如果业务
确实接收 JSON body，应显式填写 `consumes`：

```rust
#[delete_mapping(path = "/orders/:id")]
async fn delete_order() {
    // 删除订单
}
```

这些宏只注册 HTTP 方法，不替业务判断幂等性、权限或事务语义。当前没有单独的 HEAD、OPTIONS
路由属性；这两类路由应由应用 Router 显式装配。

### 全部属性总览

| 属性 | 类型 | 默认值 | 什么时候填写 |
| --- | --- | --- | --- |
| `path` / `value` | 字符串 | 无，必填 | 声明相对于 context path 的路由路径 |
| `consumes` | 字符串 | 仅普通 POST 默认为 `application/json` | 限制客户端请求的 Content-Type |
| `produces` | 字符串 | 不强制覆盖 | 强制设置响应 Content-Type |
| `auth` | 字符串枚举 | `unspecified` | 声明公开、可选登录或必须登录 |
| `auth_provider` | 静态 ID | 使用运行时默认 provider | 选择具体身份认证实现 |
| `auth_condition` | 静态 ID | 不动态收窄 | 根据可信元数据收窄身份要求 |
| `interceptors(...)` | Rust 路径列表 | 空 | 绑定当前端点的业务 interceptor |
| `decrypt` | 布尔值 | `false` | 强制解密请求体 |
| `encrypt` | 布尔值 | `false` | 强制加密响应体 |
| `crypto_protocol` | 字符串枚举 | 无 | 任一密码方向启用时必填 |
| `crypto_provider` | 静态 ID | 无 | 任一密码方向启用时必填 |
| `crypto_key_scope` | 静态 ID | 无 | 任一密码方向启用时必填 |
| `crypto_condition` | 静态 ID | 不动态关闭 | 受控地关闭当前请求的已声明密码方向 |
| `replay` | 字符串枚举 | 见下文 | 控制 modern-v2 请求重放保护 |
| `audience` | 字符串 | 包名、方法和路径组成的稳定值 | 显式固定 AAD 业务受众 |
| `error_profile` | 字符串枚举 | `http-standard` | 选择身份失败的 HTTP/业务码合同 |
| `response_contract` | 字符串枚举 | 无 | legacy-v1 响应加密时必填 |

静态 ID 指 `auth_provider`、`auth_condition`、`crypto_provider`、`crypto_key_scope` 和
`crypto_condition` 使用的注册表标识。它们必须是
1 至 64 个 ASCII 字节，只能包含英文字母、数字、点、横线和下划线。ID 是配置定位符，
不能填写地址、token、密钥或其它敏感值。`error_profile` 与 `response_contract` 是宏内置的封闭
枚举，只能填写各自章节列出的值，不会动态查找注册表。

### `path` / `value`：路由路径

- `path = "/orders/:id"`：具名写法，适合还要填写其它属性的端点。
- `value = "/orders/:id"`：`path` 的兼容别名，行为完全相同。
- `#[get_mapping("/orders/:id")]`：只有路径时的简写。
- 路径不以 `/` 开头会在编译期报错。
- 宏记录的是不含 context path 的模板，应用通过 `Router::nest` 统一添加前缀。
- 同一 HTTP 方法和路径不能重复注册；`try_register_all` 会在服务监听前拒绝重复项。

### `consumes`：请求媒体类型

`consumes` 要求请求必须携带 Content-Type，并且其值以配置文本开头；不满足时在业务函数执行前
返回 HTTP 415。例如：

```rust
#[post_mapping(
    path = "/form/login",
    consumes = "application/x-www-form-urlencoded"
)]
async fn form_login() {
    // 只接收表单请求
}
```

不同设置的含义：

| 设置 | 实际行为 |
| --- | --- |
| POST 未设置 | 默认要求 `application/json`，没有请求体但缺少 Content-Type 时也会返回 415 |
| GET/PUT/PATCH/DELETE 未设置 | naweb 不额外检查 Content-Type |
| `application/json` | 接受 `application/json` 以及 `application/json; charset=utf-8` 等参数形式 |
| `application/x-www-form-urlencoded` | 只接受匹配前缀的表单媒体类型 |
| modern-v2 且 `decrypt = true` | 必须是 `application/vnd.nasa.crypto+json;v=2`；省略时宏自动补齐 |
| legacy-v1 且 `decrypt = true` | 省略时宏自动补为 `application/json` |

显式给 modern-v2 配置其它 `consumes` 会编译失败。加密路由禁止 multipart、文件、事件流、
WebSocket 和其它流式媒体类型。`consumes` 不能为空，也不能包含控制字符。

### `produces`：响应媒体类型

`produces` 在业务函数返回后覆盖响应 Content-Type，只影响当前路由：

```rust
#[get_mapping(path = "/report", produces = "text/csv; charset=utf-8")]
async fn report() -> &'static str {
    "id,amount\n1,20\n"
}
```

不同设置的含义：

| 设置 | 实际行为 |
| --- | --- |
| 未设置 | 保留 handler 或框架生成的响应 Content-Type |
| 普通媒体类型 | 无论 handler 原先填写什么，都覆盖为该静态值 |
| modern-v2 且 `encrypt = true` | 必须是 `application/vnd.nasa.crypto+json;v=2`；省略时宏自动补齐 |
| legacy-v1 且 `encrypt = true` | 最终由 legacy 协议返回 JSON，同时必须声明响应结构合同 |

加密路由同样禁止 multipart、文件、事件流、WebSocket 和其它流式响应。`produces` 不能为空，
也不能包含控制字符。

### `auth`：端点身份要求

| 值 | 含义 | 没有凭证 | 携带无效凭证 | 是否访问身份后端 |
| --- | --- | --- | --- | --- |
| `"required"` | 必须登录 | 拒绝 | 拒绝 | 是 |
| `"optional"` | 允许匿名，但会识别合法身份 | 匿名继续 | 拒绝 | 是 |
| `"public"` | 明确公开 | 匿名继续 | 不检查 | 否 |
| 未设置 | 兼容旧路由的未声明状态 | 不执行身份中间件 | 不检查 | 否 |

生产配置建议启用 `require_explicit_auth`。启用后，任何未填写 `auth` 的路由都会在
`try_register_all` 审计阶段拒绝启动；因此公开端点也应显式填写 `auth = "public"`，不要依赖
“未设置”等同公开。

认证先于请求解密执行，当前合同要求 token 等凭证位于可信 header，而不是加密 body 内。

### `auth_provider`：身份认证实现

该值是应用启动时注册的 `AuthProvider::id()`，例如 `"session"`。它决定到哪里读取并如何验证凭证，
但宏本身不读取 Redis、数据库或业务会话。普通新 auth interceptor 不得把该属性当成
第二条鉴权入口；仅 `auth_runtime = true` 的 auth interceptor 可以把它作为可审计 provider
选择元数据。省略时使用 `AuthRuntime` 的默认 provider。

- `auth = "required"` 或 `auth = "optional"` 时可以填写。
- 省略时使用 `AuthRuntime` 配置的默认 provider；没有默认项会拒绝启动。
- `auth = "public"` 时禁止填写，因为公开路由保证不访问身份后端。
- 引用未注册的 ID 会在监听端口前审计失败。

```rust
#[get_mapping(
    path = "/account/profile",
    auth = "required",
    auth_provider = "session"
)]
async fn profile() {
    // 可以从 request extensions 读取已认证身份
}
```

### `auth_condition`：动态收窄身份要求

该值是应用注册的 `AuthCondition::id()`。condition 只能读取方法、稳定 route ID、path、header
和可信 request extensions，不能读取 body；它只能把静态身份要求收窄，不能暗中扩大权限合同。

例如路由静态声明 `required`，可信白名单 condition 可以针对明确的公开路径把本次请求收窄为
`public`：

```rust
#[get_mapping(
    path = "/common/server-time",
    auth = "required",
    auth_provider = "session",
    auth_condition = "public-whitelist"
)]
async fn server_time() {
    // 不在白名单时仍必须登录
}
```

- 必须和 `auth` 一起声明。
- `auth = "public"` 禁止配置 provider 或 condition。
- `required` 可被收窄为 required、optional 或 public。
- `optional` 只能保持 optional 或收窄为 public。
- condition 返回超出静态上限的结果时请求会被关闭。

### `decrypt` / `encrypt`：加解密方向

两个属性都是布尔字面量，只允许 `true` 或 `false`。省略与显式填写 `false` 完全相同：

| `decrypt` | `encrypt` | 请求 | 响应 |
| --- | --- | --- | --- |
| `false`/省略 | `false`/省略 | 业务 handler 直接接收普通请求 | 返回普通响应 |
| `true` | `false` | 必须先成功解密，handler 接收解密后的 JSON | 返回普通响应 |
| `false` | `true` | handler 接收普通请求 | handler 返回后必须加密 |
| `true` | `true` | 必须先解密 | 必须加密返回 |

`true` 表示强制合同，不是“能够处理就处理”。请求明文、协议不匹配、解密失败或资源不足都会
关闭请求，不会尝试其它协议，也不会回退为明文。

方向组合还受协议约束：

- `decrypt = true` 只允许 POST、PUT、PATCH、DELETE。
- modern-v2 支持“只解密请求”或“请求解密并加密响应”。它不支持只加密响应，因为响应的
  `rid`、key、target 和 audience 必须绑定同一份已认证请求上下文。
- 因此 GET 当前不能使用 modern-v2 加密：GET 不能解密请求，而 modern-v2 又不能脱离已解密
  请求单独加密响应。
- legacy-v1 可以分别启用请求解密或响应加密，所以 GET 只能在 legacy 兼容场景使用
  `decrypt = false, encrypt = true`。
- 两个方向都关闭时，禁止填写任何 `crypto_*`、`replay`、`audience` 或 `response_contract`，
  防止形成“写了密码配置但实际没有执行”的假保护。

### `crypto_protocol`：线协议

任一密码方向启用时必填，只允许以下值：

| 值 | 作用 | 请求 Content-Type | 适用范围 |
| --- | --- | --- | --- |
| `"modern-v2"` | A256GCM、方向派生 key、严格信封、AAD 和 rid 重放保护 | `application/vnd.nasa.crypto+json;v=2` | 新端点首选 |
| `"legacy-v1"` | 兼容既有 AES、RSA 或组合信封 | `application/json` | 仅旧客户端迁移 |

协议是路由静态合同，不根据请求内容自动探测。modern-v2 请求不会尝试 legacy-v1，任何加密路由
也不会在协议失败后按明文继续。legacy-v1 的私钥操作受安全门禁限制，不能因为填写该属性就绕过
运行时能力检查。

### `crypto_provider`：密码实现

该值是应用注入的 `CryptoProvider::id()`，例如示例应用使用的 `"local-web"`。provider 决定使用
本地算法还是外部密钥能力。属性中只填写有限静态 ID，不填写连接地址、密钥内容或客户端提供的
`kid`。

- 任一密码方向启用时必须显式填写。
- provider 必须在当前安全快照中注册，并支持所选协议与 key 算法。
- provider 缺失、能力不匹配或处于不可安全服务状态时，启动审计或 readiness 会失败。

### `crypto_key_scope`：路由密钥域

该值不是密钥，而是路由绑定的稳定业务域，例如 `"order-api"`。运行时使用“已认证 tenant +
key scope”选择有限 key ring；public 或匿名路由使用运行时配置的固定匿名 tenant。

- 任一密码方向启用时必须显式填写。
- 启动时至少要存在一个匹配该 scope 的 key ring。
- 客户端信封中的 `kid` 只能在已选中的有限 key ring 内定位，不能借此选择任意 provider 或密钥源。
- 修改 scope 会改变 key ring 选择，必须与客户端迁移、旧 key 保留窗和热更新步骤一起评审。

### `crypto_condition`：受控关闭密码方向

该值是已注册的 `CryptoCondition::id()`。condition 只能根据方法、route ID、path、header 和可信
extensions 返回已声明方向的子集；它可以关闭方向，但不能把宏中 `false` 的方向改成 `true`。

当前内置 `"legacy-disable-header"` 只用于有截止日期的 legacy-v1 迁移。只有入口已经剥离公网
同名 header、写入 `TrustedIngress` 证明，并且 header secret 精确匹配时，才会关闭本次请求的
legacy 加解密方向。伪造、来源不可信或值不匹配都会拒绝请求。

- `legacy-disable-header` 绑定 modern-v2 会在启动审计时失败。
- 不配置 condition 时，静态声明的方向始终生效。
- 实际发生旁路会记录低基数安全指标，但不会记录 header secret。

### `replay`：请求重放保护

| 值 | 含义 |
| --- | --- |
| `"required"` | modern-v2 请求完成认证解密后，必须在共享 guard 中原子占位；重复或后端故障都拒绝请求 |
| `"disabled"` | 不执行 rid 占位；modern 写路由会被启动审计列为高风险 |
| 未设置 | modern-v2 的 POST/PUT/PATCH/DELETE 自动使用 `required`，其它情况使用 `disabled` |

legacy-v1 信封没有 rid，因此不能配置 `replay = "required"`。多实例部署必须让 required 路由使用
同一份原子共享存储；仅使用各实例内存无法阻止跨实例重放。

### `audience`：AAD 业务受众

modern-v2 把 audience 纳入请求和响应 AAD，用来阻止有效密文被复制到其它业务端点。省略时默认值
为：

```text
<Cargo 包名>:<大写 HTTP 方法> <宏中的 path>
```

例如包名为 `order-service` 的 `POST /orders` 默认得到
`order-service:POST /orders`。显式填写时必须是 1 至 256 字节且不含控制字符：

```rust
audience = "order-service:order:create"
```

客户端和服务端必须使用完全相同的 audience。发布后修改会导致旧客户端请求认证失败，因此建议在
跨语言协议中显式固定。audience 不是 secret，但不能来自客户端输入。该属性对 legacy-v1 没有
协议收益，legacy 路由应省略；未启用加解密时禁止填写。

### `error_profile`：身份失败响应合同

| 值 | 缺失/无效身份 | 账户不可用 | 身份后端或策略故障 |
| --- | --- | --- | --- |
| 省略或 `"http-standard"` | HTTP 401，业务码 1401 | HTTP 403，业务码 1403 | HTTP 503，业务码 1503 |
| `"fore-rest-legacy"` | HTTP 200，业务码 1000 | HTTP 200，业务码 400 | HTTP 503，业务码 1503 |

响应结构统一为 `{"code": ..., "msg": ...}` 并携带 `Cache-Control: no-store`。只允许使用上表
两个 profile；其它文本会在宏展开阶段编译失败，不存在静默回退到标准分支的行为。

`fore-rest-legacy` 仅用于必须维持旧业务码合同的迁移端点；新端点应使用 `http-standard`。
该属性必须与 `auth = "required"` 或 `auth = "optional"` 同时声明；公开路由或没有 `auth` 的
路由填写它会编译失败。

### `response_contract`：legacy 响应结构合同

当前唯一有意义的值是 `"base-response-v1"`，表示 legacy-v1 只加密统一响应对象的 `data` 字段，
保留外层 `code` 和 `msg` 兼容旧客户端。

- legacy-v1 设置 `encrypt = true` 时必须填写，否则编译失败。
- modern-v2 加密完整响应，不使用该属性，应省略。
- 未启用加解密时禁止填写。

### 推荐组合

普通公开查询：

```rust
#[get_mapping(path = "/common/server-time", auth = "public")]
async fn server_time() {
    // 普通请求与普通响应
}
```

必须登录但不加密业务 body：

```rust
#[get_mapping(
    path = "/account/profile",
    auth = "required",
    auth_provider = "session"
)]
async fn profile() {
    // 身份认证成功后执行
}
```

modern-v2 只解密请求、返回普通响应：

```rust
#[post_mapping(
    path = "/orders/import",
    auth = "required",
    auth_provider = "session",
    decrypt = true,
    encrypt = false,
    crypto_protocol = "modern-v2",
    crypto_provider = "local-web",
    crypto_key_scope = "order-api",
    replay = "required",
    audience = "order-service:orders:import"
)]
async fn import_orders() {
    // handler 接收解密后的 JSON，响应不再加密
}
```

modern-v2 双向加密：

```rust
#[post_mapping(
    path = "/orders",
    auth = "required",
    auth_provider = "session",
    decrypt = true,
    encrypt = true,
    crypto_protocol = "modern-v2",
    crypto_provider = "local-web",
    crypto_key_scope = "order-api",
    replay = "required",
    audience = "order-service:orders:create"
)]
async fn create_order() {
    // handler 接收解密后的 JSON，返回值由框架加密成完整 modern-v2 信封
}
```

legacy-v1 仅加密响应：

```rust
#[get_mapping(
    path = "/legacy/account",
    auth = "required",
    auth_provider = "session",
    decrypt = false,
    encrypt = true,
    crypto_protocol = "legacy-v1",
    crypto_provider = "local-web",
    crypto_key_scope = "legacy-account",
    response_contract = "base-response-v1"
)]
async fn legacy_account() {
    // 只用于旧客户端迁移；框架仅加密 BaseResponse.data
}
```

### 常见错误配置

| 配置 | 结果 | 原因 |
| --- | --- | --- |
| `#[get_mapping(..., decrypt = true)]` | 编译失败 | GET 不允许请求体解密 |
| `decrypt = true` 但没有 `crypto_protocol` | 编译失败 | 无法确定线协议 |
| 任一方向为 `true`，但缺少 provider 或 key scope | 编译失败 | 路由密码合同不完整 |
| 两个方向都是 `false`，却填写 `crypto_*` | 编译失败 | 防止产生未实际执行的假保护 |
| modern-v2 只设置 `encrypt = true` | 启动审计失败 | 响应缺少已认证请求上下文 |
| modern-v2 写路由显式关闭 replay | 可以启动但列为高风险 | 无法阻止同一有效请求重复执行 |
| legacy-v1 设置 required replay | 编译失败 | legacy 信封没有 rid |
| legacy-v1 加密响应但未声明合同 | 编译失败 | 无法安全确定要改写的响应字段 |
| `auth = "public"` 又填写 provider/condition | 编译失败 | public 保证不访问身份后端 |
| 严格模式下省略 `auth` | 启动审计失败 | 所有端点必须显式声明身份合同 |
| provider、condition 或 key ring ID 未注册 | 启动审计失败 | 静态声明无法解析到运行时能力 |

### 与其它属性宏的顺序

如果同一个 handler 还使用执行监控或隔离属性，监控属性必须写在 `#[*_mapping]` 上方，确保它能
读取真实路由信息：

```rust
#[grafana]
#[hystrix]
#[post_mapping(path = "/orders", auth = "public")]
async fn create_order() {
    // 业务处理
}
```

把 `#[grafana]` 或 `#[hystrix]` 放到路由宏下方会在编译期报错，避免指标退化为只有函数名而丢失
方法与路径。

modern-v2 请求必须精确使用 `application/vnd.nasa.crypto+json;v=2`；legacy-v1 使用
`application/json`。协议不匹配、字段缺失、重复、未知、带填充 Base64URL、tag 错误或时间窗错误
都会关闭请求，不尝试另一协议或明文。

### 固定执行顺序

```text
edge interceptor -> auth interceptor -> AuthContext gate/legacy auth
                 -> crypto condition -> decrypt -> replay -> plaintext interceptor
                 -> handler/short circuit -> encrypt -> auth/edge interceptor egress
```

**身份认证永远先于请求解密。** 当前合同要求凭证位于可信 header，不能从待解密 body 读取。
请求开始时固定持有一个 `MappingRuntimeSnapshot`，响应沿用同一 key、kid、target、audience 和 rid；
热加载不会让单次请求跨代。`target` 优先读取 Axum 在嵌套路由改写前保存的可信 `OriginalUri`，
因此必须包含应用 context path 与 query；不会信任客户端传入的 `X-Original-*` header。

业务 handler 在独立任务中执行。业务错误、panic 或端点总 deadline 发生在认证解密之后时，固定
响应上下文会继续生成加密 4xx/5xx；总超时取消任务时，请求明文内存 permit 跟随任务真实销毁，
不会提前归还。已经完成的重放占位不会因 handler 失败、超时或客户端断开而删除。

### 就绪探测与热刷新状态

宿主可用同一份完整 route policy 调用异步就绪探测：

```rust
let audit = runtime.readiness(&route_policies).await?;
let health = runtime.health();
```

`readiness` 在固定快照上重新审计 route、active key 和 provider，并对 required replay guard 执行
有界探测；失败应让负载均衡停止分发新流量，但不会销毁 last-good 或中断在途请求。远程
`ReplayGuard` 必须覆盖 `readiness`，内存实现使用默认成功结果。

配置解析、组件构建或路由审计失败时，宿主调用 `record_reload_failure()`；成功 `replace()` 更高代次
后会自动清除。`health()` 只返回 generation、配置年龄和未恢复刷新错误年龄，不包含 kid、secret
来源、路由清单或后端地址。

### 运行时由应用构建

`naweb` 不直接读取 YML，也不持有业务 Redis、KMS 或会话 DTO。宿主应用负责：

1. 合并本地 YML、可信远端配置和环境变量；
2. 从 secret 来源读取密钥，构建不可变 `KeyRing`；
3. 注入 `AuthRuntime`、`CryptoRuntime`、共享 `ReplayGuard` 和资源预算；
4. 调用 `try_register_all` 审计全部静态 route policy；
5. 审计成功后再 bind/listen。

完整逐字段 YML、环境变量映射和真实 Fore/Redis adapter 见 `rust-simple-mvc/README.md` 与 `rust-simple-mvc/zcf/application.yml`。

### 安全限制

- TLS 必须始终启用，应用层加密不替代 TLS、授权、限流和审计。
- modern-v2 使用请求/响应方向隔离 key、12 字节随机 nonce、规范化 AAD 和共享重放存储。
- legacy-v1 只用于迁移；RSA 私钥路径受未修复依赖风险影响，默认能力门关闭。
- CPU 密集工作经有界 blocking 执行器；超时或取消不会提前释放仍在运行闭包的 permit。
- 请求和响应同时受单请求上限与全进程加权内存预算限制。
- 错误、日志、指标和调试输出不得包含密钥、token、明文或完整密文。

## 安全指标

启用 `web-security` 后，每条静态路由在注册时获得固定指标槽位。`MappingRuntime::metrics().render_prometheus()` 返回可与应用现有 `/metrics` 文本直接拼接的 Prometheus 片段，覆盖端点结果、身份、双向密码、required replay、受控旁路、阶段延迟、热更新结果和快照代次。

指标只使用编译期 `route_id`、静态 protocol/condition、固定 direction/operation/outcome 标签。运行时 path 参数、query、subject、tenant、rid、token、kid、密钥来源、明文和完整密文都不会进入标签或 HELP 文本。热更新复用同一个注册表，计数不会因 ArcSwap 替换快照而清零。
