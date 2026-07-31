# nasecret

`nasecret` 提供有序 secret 分片、拼接后一次性解码、零化字节容器、同代脱敏快照和
prepare/commit/abort 两阶段轮换。它不依赖应用运行时，也不把真实 material 放进普通配置快照。

```toml
[dependencies]
nasa = { version = "1", features = ["application", "secret"] }
```

## 应用配置

`napp` 从原始候选树的 `secrets.<id>` 解析 `config_path`、`env` 和 `file` 分片；解析成功后，
被引用的配置标量会在 `Application::config()` 中替换为固定脱敏值，真实字节只存在于
`Application::secrets()`。

```yaml
security:
  crypto:
    key_prefix: ${APP_KEY_PREFIX}

secrets:
  api_key:
    encoding: raw
    max_bytes: 256
    fragments:
      - config_path: security.crypto.key_prefix
      - env: APP_KEY_SUFFIX
```

```rust
let secrets = app.secrets();
let api_key = secrets
    .get("api_key")
    .ok_or_else(|| anyhow::anyhow!("api_key is missing"))?;
send_with_key(api_key.expose()).await?;
```

## 直接解析

框架集成方可构造 `SecretSpec` 并调用 `SecretSnapshot::resolve`；含外部 provider 分片时使用
`resolve_async` 和启动期冻结的 `SecretProviderRegistry`。

## 两阶段轮换

`RotatingSecretStore::rotate_prepared` 先让所有受影响 participant 完成 prepare，全部成功后再
执行无失败内存 commit。任一 prepare 失败或协调 future 被取消时，已准备项按反序 abort，旧快照
保持 last-good。

DB、Redis、Kafka、OTLP 等强类型资源可复用 `RotatingSecretResource<R>`：业务 adapter 通过
`SecretResourceFactory<R>` 在 prepare 阶段完成解析、建连和 TLS 校验，commit 只交换 `Arc`。
每次业务操作先调用 `current()` 固定一个 `SecretResourceSnapshot<R>`，避免在一次操作中混用两代
连接。框架会强制校验每一代仍包含全部 watched secret。

## 主要边界

- secret ID、fragment 数、引用元数据和最终字节数都有硬上限。
- 分片严格按声明顺序拼接，不 trim、不补换行，再只解码一次。
- `Debug` 和公开错误只展示 ID、长度、generation 与稳定分类。
- `SecretBytes::expose()` 只应在一次受控调用期间借用，不能复制到日志或长期缓存。
- 多个不同资源的指针不可能成为一次 CPU 原子写；两阶段协议保证全部 prepare 成功后才发布，
  调用方仍须按请求固定各自 generation。
- provider 的 bootstrap credential 不能反向依赖该 provider 自己解析。
