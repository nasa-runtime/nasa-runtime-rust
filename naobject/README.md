# naobject

`naobject` 是稳定的 provider-neutral 对象存储合同，并提供 path-style S3-compatible SigV4 adapter。
当前实现只处理有硬上限的单对象缓冲，不伪装成 multipart 或无限流式上传。
它面向报表导出、审计归档和附件等“单对象可完整装入内存”的业务，把 key 校验、条件创建、内容完整性、
错误脱敏和容量门禁收敛为同一合同，避免每个业务分别拼接 S3 请求与失败语义。

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["object-store"] }
```

## 运行架构与安全合同

`ObjectStore` 是 provider-neutral 业务边界，`S3ObjectStore` 是当前唯一 adapter。独立调用方拥有实例；
Application 用 `object_stores` 标准配置建立命名资源、凭据、健康与关闭 owner。每次调用
都沿同一条有界路径执行：

```text
业务 key/body
  -> 本地 key、配置、容量门禁
  -> path-style URL + SigV4 签名
  -> 禁止重定向的单次 HTTP 请求
  -> 有界响应读取 + 元数据校验
  -> 封闭错误或已验证业务结果
```

`CreateOnly` 用 `If-None-Match: *` 把“不覆盖已有对象”交给远端原子裁决；`delete` 把远端不存在折叠为
成功，便于补偿和清理安全重试。上传写入 SHA-256 metadata，默认下载必须复核该摘要；ETag 不承担
内容完整性。调用 future 被丢弃时只说明本地不再等待，远端是否已经收到或完成请求未知，业务若要安全
重试写入，应使用 `CreateOnly`、稳定 key 或自己的幂等协议。

endpoint 只允许 HTTPS，或仅供本机使用的 loopback HTTP；拒绝 userinfo、query、fragment、重定向和
不能作为 base URL 的地址。credential、endpoint、对象 key 与远端响应正文不会进入公开错误文本或
指标 label。

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

## 配置投影

开启 `application,object-store` 后，无需新增组件字符串。命名计划使用同代 SecretSnapshot 凭据，
通过 `app.object_store(name).await` 取得受管 `ObjectStore`；Service 与无 Web 的 Batch 均可使用。

```yaml
object_stores:
  exports:
    enabled: true
    provider: s3
    endpoint: https://objects.example.com
    bucket: exports
    region: ap-southeast-1
    access_key: secret://object_access_key
    secret_key: secret://object_secret_key
    request_timeout_ms: 10000
    max_object_bytes: 16777216
    require_checksum: true
    health_probe: head_bucket
    critical: true
```

access_key/secret_key/session_token 必须使用 `secret://` 引用 `secrets` 中的名字，不能填写凭据正文。
标准计划最多 64 个，业务请求超时上限 300 秒。`head_bucket` 在启动时进行只读探测，随后由宿主监督的任务
每 15 秒并发探测各命名来源，单次最多 5 秒；60 秒无新证据时健康过期。HEAD 证明 bucket 可达与凭据可用，
不能保证每个对象操作都具备权限。传输、远端拒绝和响应完整性失败使关键资源 `NotReady`，非关键资源
`Degraded`；成功调用或确定性的 `NotFound`/`AlreadyExists` 恢复健康。
非关键资源也可选择 `on_request`：不创建探测任务，仅记录最后一次实际调用结果，不将空闲视为故障，
也不承诺持续可达。参数或凭据变化报告 `RestartRequired`；停机取消并等待探测任务退出，关闭新调用、
等待在途业务调用，旧句柄返回 `Closed`。下面的默认值与一年超时上限只描述独立 `S3Options`。

| `S3Options` 字段 | 默认值 | 约束与失败语义 |
| --- | --- | --- |
| `request_timeout` | 10 秒 | 大于 0，最长一年；超时归入 `Transport`，远端结果未知 |
| `max_object_bytes` | 16 MiB | 大于 0，框架硬上限 256 MiB；上传前拒绝，下载可在读到元数据或流后停止 |
| `require_checksum` | `true` | 下载缺少或不匹配 SHA-256 metadata 时拒绝返回对象 |
| `endpoint` | 无 | HTTPS 或 loopback HTTP；path-style bucket，不跟随重定向 |
| `bucket` / `region` | 无 | 必须通过构造期校验；不从环境隐式猜测 |

## 观测

adapter 自行累计四类操作的结局、耗时与成功传输字节，`metrics_snapshot()` 一次读取即得到全部
当前事实，读取不清零。记账点在 `ObjectStore` 实现的包装层上，本地拒绝、传输失败与远端非成功
状态共用同一出口；调用 future 被业务超时、客户端断连或任务取消丢弃时由完成守卫记为 `cancelled`。
因此本地拒绝和已经开始但未返回的调用都不会在观测面上等同于“没有请求”。

| family | 类型 | label | 含义 |
| --- | --- | --- | --- |
| `naobject_requests_total` | counter | `operation`、`outcome` | 按操作与封闭结局分类的累计请求数 |
| `naobject_transferred_bytes_total` | counter | `direction` | 成功上传或下载的对象字节总数 |
| `naobject_request_duration_seconds` | histogram | `operation` | 从进入 adapter 到返回业务结局的耗时，不含调用方取消 |

`operation` 取 `put`/`get`/`head`/`delete`，`outcome` 取
`success`/`not_found`/`too_large`/`already_exists`/`rejected`/`transport`/`remote_status`/
`invalid_response`/`checksum`/`cancelled`。`already_exists` 表示 `CreateOnly` 请求已到达远端且对象存在；
`rejected` 只表示本地输入或配置门禁拒绝。`too_large` 同时覆盖上传前的本地容量门禁和下载收到远端
元数据或流后触发的容量门禁，结合 `operation` 判断方向，不表达请求是否已经发出。
`cancelled` 表示调用已经开始，但 future 在业务结果返回前被丢弃，远端是否收到或完成请求未知；
这类调用计入请求数，不计入时延和成功传输字节。取消耗时由调用方预算决定，不能代表 adapter 或后端
完成一次操作的耗时；取消趋势应直接使用 `naobject_requests_total{outcome="cancelled"}` 观察。
时延族只覆盖完整返回的调用：取消占比高时，histogram 的样本数会低于同期请求数，分位数仅代表完成
子集；某操作只有取消而没有完整返回时，该操作不会产生时延样本，不能把无数据解释为零时延或依赖健康。
运维应同时观察取消比例与完整返回覆盖率。在窗口内存在请求时，可分别按下列公式判读：

```promql
(
  sum by (operation) (rate(naobject_requests_total{outcome="cancelled"}[5m]))
  or
  0 * sum by (operation) (rate(naobject_requests_total[5m]))
)
/
(sum by (operation) (rate(naobject_requests_total[5m])) > 0)

(
  sum by (operation) (rate(naobject_request_duration_seconds_count[5m]))
  or
  0 * sum by (operation) (rate(naobject_requests_total[5m]))
)
/
(sum by (operation) (rate(naobject_requests_total[5m])) > 0)
```

第一项是取消比例，第二项是完整返回覆盖率；前者升高或后者下降时，不能单独使用 histogram 分位数
判断依赖健康。两式都以请求序列为基准补零，只展示窗口内请求速率大于零的操作；没有取消的操作显示
0% 取消比例，只有取消的操作显示 0% 完整返回覆盖率，而不是无数据。告警还应覆盖“请求速率大于零但
对应时延序列缺席或不推进”的情形。
远端状态码**不进 label**：`RemoteStatus` 统一折叠为 `remote_status`，避免把 0..=599 的取值域
变成指标基数；需要具体状态码时读错误本身。bucket、key 与对端地址同样不进 label。

与 `nasa` 的 `application` 组合时，业务在 UserHook 一行接入统一指标目录，之后 Prometheus 文本
端点与 OTLP 指标导出共用同一份快照：

```rust
let store = std::sync::Arc::new(nasa::object::S3ObjectStore::new(options)?);
app.register_metrics_source(nasa::object::metrics::metrics_source(store.clone()))?;
```

统一目录要求一个 family 只有一个 owner。同一进程使用多个 bucket、endpoint 或凭据域时只登记一个
聚合源，不把这些实例维度放进 label：

```rust
app.register_metrics_source(nasa::object::metrics::metrics_source_many([
    audit_store.clone(),
    attachment_store.clone(),
]))?;
```

聚合源逐操作、逐结局、逐时延桶求和，成功传输字节同样求和。以上手工指标入口适用于独立 adapter；
`object_stores` 标准路径自动登记一个聚合源，由 Application 负责准入、健康与关闭，避免重复登记。

## 能力边界

- 本能力进入 `full`，通过命名 `object_stores` 接入 Application，无需新增组件名；业务决定对象生命周期与数据政策。
- 稳定使用范围是有界 `put/get/head/delete`、`Overwrite/CreateOnly`、path-style SigV4 与
  SHA-256 metadata 完整性；provider-neutral trait 不表示所有对象存储的高级语义已经统一。
- key 拒绝绝对路径、空段、`.`、`..`、控制字符和超长输入。
- 非 loopback 明文 HTTP、重定向、userinfo 和非法 endpoint 会被拒绝。
- 当前上传和下载都完整缓冲，默认上限 16 MiB，框架硬上限 256 MiB。
- 错误不回显 endpoint、credential、对象 key 或远端响应正文。
- multipart、流式读写、range read、list、presigned URL、STS 自动刷新、服务端加密策略和对象版本语义
  不在合同内；需要这些能力时应由业务选择专用 client，而不是绕过现有门禁扩展本 adapter。
