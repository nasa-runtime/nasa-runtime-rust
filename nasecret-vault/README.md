# nasecret-vault

`nasecret-vault` 是 `nasecret::SecretProvider` 的 Vault/OpenBao KV v2 adapter。它只读取单个字符串字段，
对 URL、host、超时、响应大小、路径段和重定向执行严格边界检查。

```toml
[dependencies]
nasa = { version = "1", features = ["secret-vault"] }
```

```rust
use nasa::secret::{SecretBytes, VaultKvV2Provider, VaultOptions};

let provider = VaultKvV2Provider::new(
    "https://vault.example.com",
    "kv",
    SecretBytes::new(bootstrap_token),
    VaultOptions::default(),
)?;
```

注册到 `SecretProviderRegistry` 后，provider fragment 的 key 使用 `team/service#field`：`#` 前是 KV v2
路径，后面是 `data.data` 下的字符串字段。

## YML 配置

adapter 不拥有固定应用配置根。推荐只配置非敏感 endpoint、mount 和 allowlist；bootstrap token
必须来自 env、文件或部署信任根，不能写入 yml，也不能由该 provider 自己解析。

```yaml
secret_providers:
  primary:
    endpoint: https://vault.example.com
    mount: kv
    timeout_ms: 3000
    max_response_bytes: 262144
    allowed_hosts:
      - vault.example.com
```

## 主要边界

- 非 loopback 只允许 HTTPS；URL userinfo、query、fragment 和重定向被拒绝。
- host allowlist 非空时必须精确命中。
- mount、KV path 和 field 只允许有界安全 ASCII 段。
- 响应在有无 `Content-Length` 时都执行总字节上限。
- 错误不携带 token、secret、响应正文或路径值。
