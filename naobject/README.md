# naobject

`naobject` 是实验性 provider-neutral 对象存储合同，并提供 path-style S3-compatible SigV4 adapter。
当前实现只处理有硬上限的单对象缓冲，不伪装成 multipart 或无限流式上传。

```toml
[dependencies]
nasa = { version = "1", features = ["object-store-experimental", "secret"] }
```

## 初始化与使用

```rust
use nasa::object::{
    ObjectKey, ObjectStore, PutMode, PutObject, S3Credentials, S3ObjectStore, S3Options,
};
use nasa::secret::SecretBytes;

let credentials = S3Credentials {
    access_key_id: SecretBytes::new(access_key_id),
    secret_access_key: SecretBytes::new(secret_access_key),
    session_token: None,
};
let options = S3Options::new(
    "https://objects.example.com",
    "exports",
    "ap-southeast-1",
    credentials,
);
let store = S3ObjectStore::new(options)?;

let metadata = store
    .put(PutObject {
        key: ObjectKey::new("reports/2026-07/orders.csv")?,
        body: csv_bytes,
        content_type: Some("text/csv".into()),
        mode: PutMode::CreateOnly,
    })
    .await?;
```

`CreateOnly` 使用条件写避免静默覆盖；`delete` 对不存在对象保持幂等。默认要求上传写入并在下载时复核
SHA-256 metadata，ETag 不作为内容摘要。

## YML 配置

当前没有受管对象存储组件和固定 yml schema。endpoint、bucket、region、请求超时、对象大小上限和
checksum 策略由业务配置映射到 `S3Options`；credential 必须来自 `nasecret` 快照或等价信任根。

```yaml
object_store:
  endpoint: https://objects.example.com
  bucket: exports
  region: ap-southeast-1
  max_object_bytes: 16777216
  require_checksum: true
```

不要把 access key 或 session token 写入该配置。

## 成熟度与边界

- 本能力是实验 API，不进入 `full`；稳定合同等待两个真实上传、导出或归档项目收敛。
- key 拒绝绝对路径、空段、`.`、`..`、控制字符和超长输入。
- 非 loopback 明文 HTTP、重定向、userinfo 和非法 endpoint 会被拒绝。
- 当前上传和下载都完整缓冲，默认上限 16 MiB，框架硬上限 256 MiB。
- 错误不回显 endpoint、credential、对象 key 或远端响应正文。
- multipart、STS 自动刷新、range read 和对象版本语义尚不在合同内。
