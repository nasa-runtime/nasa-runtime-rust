# nasecret-http

`nasecret-http` 把 `nasecret` 快照转换为可两阶段轮换的 reqwest TLS/mTLS client。证书、私钥、
信任根解析和 client 构造全部发生在 prepare；commit 只原子发布已经验证的客户端指针。

业务通过门面开启 `secret-http`：

```toml
[dependencies]
nasa = { version = "1", features = ["secret-http"] }
```

```rust
use nasa::secret::{
    RotatingTlsHttpClient, TlsHttpClientConfig, TlsIdentityRef, TrustBundleRef,
};

let mut config = TlsHttpClientConfig::new("billing-client");
config.identity = Some(TlsIdentityRef {
    certificate_chain: "billing-cert".into(),
    private_key: "billing-key".into(),
});
config.trust = Some(TrustBundleRef {
    certificates: "billing-ca".into(),
});

let client = RotatingTlsHttpClient::new(&initial_snapshot, config)?;
let request_client = client.current();
request_client
    .client()
    .get("https://billing.example.com/health")
    .send()
    .await?;
```

一次请求必须固定同一个 `TlsHttpClientSnapshot`，不能在请求中途重新读取 `current()` 混用代际。

## YML 配置

本 crate 不直接反序列化 yml。identity 和 trust 字段引用 `secrets:` 中的稳定 ID，endpoint 等业务
HTTP 配置由调用方管理。

```yaml
secrets:
  billing-cert:
    encoding: raw
    max_bytes: 1048576
    fragments:
      - file: /run/secrets/billing-cert.pem
  billing-key:
    encoding: raw
    max_bytes: 1048576
    fragments:
      - file: /run/secrets/billing-key.pem
  billing-ca:
    encoding: raw
    max_bytes: 1048576
    fragments:
      - file: /run/secrets/billing-ca.pem
```

## 主要边界

- client 固定为 HTTPS-only、拒绝重定向，并有正请求超时。
- 显式 trust bundle 不会暗中叠加系统根证书。
- PEM、URL 和底层 TLS 错误不会进入公开错误正文。
- participant ID 和 secret 引用必须是有界安全标识。
- 正常轮换应把 client 作为 `SecretRotationParticipant` 交给统一协调器。
