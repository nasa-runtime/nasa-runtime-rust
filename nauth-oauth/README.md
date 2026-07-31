# nauth-oauth

`nauth-oauth` 提供 OAuth Resource Server 的 JWT access token 校验、JWKS last-good registry 和
RFC 8414 授权服务器 metadata 客户端。当前签名能力明确限定为 RSA/RS256。

业务通过门面开启 `oauth`；使用配置驱动认证组件时同时开启 `application` 和 `web`：

```toml
[dependencies]
nasa = { version = "1", features = ["application", "oauth", "web-security"] }
```

```rust
#[nasa::application("auth", "web")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

## YML 配置

key 来源必须且只能选择一个：静态 `jwks`、直接 `jwks_uri` 或 `metadata_uri`。

```yaml
auth:
  issuer: https://identity.example.com
  audience: order-api
  allowed_algorithms: [RS256]
  leeway_secs: 60
  metadata_uri: https://identity.example.com/.well-known/oauth-authorization-server
  jwks_refresh_secs: 300
  jwks_timeout_ms: 3000
  jwks_max_bytes: 1048576
  jwks_max_keys: 128
  jwks_allowed_hosts:
    - identity.example.com
  jwks_stale_secs: 3600
```

远程模式在 Ready 首拉失败时拒绝启动。运行期刷新失败保留 last-good；连续失败超过 stale 窗后才让
readiness 转为不可用。

## 直接校验

低层调用使用 `verify_access_token(token, jwks, policy, now)`，它按 `kid` 选 key、验证 RS256
签名，再检查 `typ=at+jwt`、issuer、audience、`exp`、`nbf` 和 `iat`。`parse_unverified` 只允许
内部选择 key，返回的 claims 不能直接建立身份。

## 主要边界

- 算法白名单当前必须精确为 `RS256`，`none` 和其它算法被拒绝。
- metadata 返回的 issuer 必须与配置精确相等。
- 非 loopback 远程 URL 只允许 HTTPS，拒绝重定向并受 host allowlist、超时、body 和 key 数上限保护。
- JWKS 候选必须非空、`kid` 唯一，并且每个 key 都是可用 RSA signing key。
- 错误不回显 token、claims、key material、URL 或远端响应正文。
