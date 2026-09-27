# nasecret-vault

`nasecret-vault` 是 `nasecret::SecretProvider` 的 Vault/OpenBao KV v2 adapter。它只读取单个字符串字段，
对 URL、host、超时、响应大小、路径段和重定向执行严格边界检查。
与 `application` 组合时，provider 由 `secret_providers` 声明，远端材料在消费者启动前解析，并与
配置快照同代发布；引导 token 必须来自独立的环境变量或文件。

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["secret-vault"] }
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

独立 adapter 通过 `VaultOptions` 显式装配。下面是门面同时开启 `application,secret-vault` 时的
标准配置；bootstrap token 只引用环境变量或文件，不能把 token 明文写入 YAML，也不能由同一
provider 解析自己的引导凭据。

```yaml
secret_providers:
  primary:
    enabled: true
    kind: vault_kv2
    endpoint: https://vault.example.com
    mount: kv
    token: { env: VAULT_BOOTSTRAP_TOKEN }
    timeout_ms: 3000
    max_response_bytes: 262144
secrets:
  billing-key:
    encoding: raw
    max_bytes: 4096
    fragments:
      - provider: { provider: primary, key: team/billing#key }
```

## 主要边界

- 非 loopback 只允许 HTTPS；URL userinfo、query、fragment 和重定向被拒绝。
- host allowlist 非空时必须精确命中。
- mount、KV path 和 field 只允许有界安全 ASCII 段。
- 响应在有无 `Content-Length` 时都执行总字节上限。
- 错误不携带 token、secret、响应正文或路径值。

## Application 接入

`secret_providers.<name>` 支持 `vault_kv2` 与 `openbao_kv2`，最多 16 个 provider、256 个 secret。
受管装配按 endpoint 固定目标；独立 `VaultOptions` 还可显式设置 host allowlist，受管 YAML 不提供
`allowed_hosts` 字段。禁用计划的独占凭据及无活跃消费者的引导文件不读取、不建立观察。

```text
独立 env/file 引导凭据 → Vault/OpenBao 读取 → 校验有界材料 → 候选 SecretSnapshot
                                                               ↓
                                                消费者准备成功 → 发布 ConfigView
```

远端读取在发布锁外，总准备预算 15 秒。配置中心与 provider 的引导必须有独立信任根；失败保留
旧视图。保留旧材料不证明其仍未被远端撤销，远端权限与撤销策略仍由部署方管理。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
