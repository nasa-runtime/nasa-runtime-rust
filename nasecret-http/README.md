# nasecret-http

`nasecret-http` 把 `nasecret` 快照转换为可两阶段轮换的 reqwest TLS/mTLS client。证书、私钥、
信任根解析和 client 构造全部发生在 prepare；commit 只原子发布已经验证的客户端指针。
与 `application` 组合时，`http_clients.<name>` 统一管理 HTTPS 目标、材料轮换、正文预算和关闭门禁，
业务通过 `app.http_client(name)` 取得与配置视图绑定的客户端。

## 轮换架构

```text
SecretSnapshot 候选 ──→ prepare：解析证书、私钥、信任根并构造 client
                                      │
                                      ├─ 失败：拒绝候选，保留 current
                                      └─ 成功 ──→ commit：原子发布新快照
请求开始 ──→ 固定 current 快照 ──→ 请求结束
```

prepare 不改变对外可见客户端；只有所有材料通过校验后才允许 commit。调用方通过统一轮换协调器观察
prepare/commit 结果和当前代际，单次请求始终使用同一快照。

业务通过门面开启 `secret-http`：

```toml
[dependencies]
nasa = { version = "1.0.3", features = ["secret-http"] }
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

独立构造器使用上面的稳定 secret ID；Application 同时开启 `application,secret-http` 后读取
以下配置，命名 client 的材料字段必须使用 `secret://` 引用。证书与私钥须成对配置，trust 可独立配置。

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
http_clients:
  billing:
    enabled: true
    base_url: https://billing.example.com
    certificate: secret://billing-cert
    private_key: secret://billing-key
    trust: secret://billing-ca
    request_timeout_ms: 3000
    max_body_bytes: 1048576
```

## 主要边界

- client 固定为 HTTPS-only、拒绝重定向，并有正请求超时。
- 显式 trust bundle 不会暗中叠加系统根证书。
- PEM、URL 和底层 TLS 错误不会进入公开错误正文。
- participant ID 和 secret 引用必须是有界安全标识。
- 正常轮换应把 client 作为 `SecretRotationParticipant` 交给统一协调器。

## Application 接入

命名集合最多 64 项，在启动时冻结。Service 在 initializer 或 Ready 后取得客户端，Batch 在工作负载
开始前完成装配。TLS 资源与 `ConfigView` 同点发布；调用固定一次视图，完整正文受预算与大小限制。
已有名称的参数与材料可以准备新候选，名称不支持热增删；参数或材料准备失败保留整代旧资源。

停机后旧受管 client 拒绝新调用，不接受跨 origin 请求或重定向。取消本地等待不证明远端未执行；
有副作用请求的重放策略仍由业务决定。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
