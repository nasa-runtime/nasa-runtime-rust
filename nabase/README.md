# nabase

`nabase` 是轻量公共工具 crate，业务通常通过门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["base"] }
```

## 响应壳

```rust
use nasa::base::BaseResponse;

let ok = BaseResponse::ok("done");
let err: BaseResponse<()> = BaseResponse::err(40001, "参数错误");
```

## 容量配置

`ByteSize` 支持 `"500MB"`、`"30GB"`、纯字节数等配置形式：

```rust
use nasa::base::ByteSize;

let size = ByteSize::parse("128MB")?;
assert_eq!(size.bytes(), 128 * 1024 * 1024);
```

## ID 生成

```rust
use nasa::base::{SnowflakeConfig, IdGenerate};

let gen = SnowflakeConfig::default().build_local()?;
let id = gen.next_id();
```

默认 `Snowflake` 是本地算法，不做 Redis workerId 分配；分布式 workerId 分配由 Redis 组件负责。

## 字符串和环境变量

```rust
let key = nasa::base::env::relaxed_env_key("app.redis-url");
assert_eq!(key, "APP_REDIS_URL");
```

`strings` 模块用于配置项、请求参数和环境变量的空白清洗。

## 翻译

翻译模块只提供机制：语言归一、缓存、底层翻译器注入和失败回原文。

```rust
use std::sync::Arc;
use nasa::base::translator;

translator::set_engine(Arc::new(translator::engine_from_fn(|from, to, text| {
    Ok(Some(format!("{from}->{to}:{text}")))
})));

translator::enable();
let text = translator::translate_to("en-US", "你好");
```

业务需要自己注入 DB、Redis、外部翻译服务或内部词库 adapter。

## YML 配置与使用

`nabase` 是基础工具包，只有少量类型适合直接从 yml 读取；它不主动读取配置文件。

推荐配置示例：

```yaml
base:
  snowflake:
    worker_id: 1
    base_time: 1640995200000
    worker_id_bits: 10
    seq_bits: 12
  upload_limit: 500MB
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `base.snowflake.worker_id` | `1` | 当前节点 worker id，必须落在 `worker_id_bits` 允许范围内。 |
| `base.snowflake.base_time` | 内置 epoch | 雪花算法基准时间戳，单位 ms。 |
| `base.snowflake.worker_id_bits` | `10` | worker id 位数。 |
| `base.snowflake.seq_bits` | `12` | 同毫秒序列号位数。 |
| `base.upload_limit` | 无默认 | 可反序列化为 `ByteSize`，支持 `KB`/`MB`/`GB` 或纯字节数。 |

使用代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    base: BaseConfig,
}

#[derive(serde::Deserialize)]
struct BaseConfig {
    snowflake: nasa::base::SnowflakeConfig,
    upload_limit: nasa::base::ByteSize,
}

let id_gen = cfg.base.snowflake.build_local()?;
let max_upload_bytes = cfg.base.upload_limit.bytes();
```

`BaseResponse`、字符串工具、环境变量工具和翻译器没有固定 yml；翻译器是否启用、使用哪个 engine 应由业务启动代码显式注入。

## 主要边界

- `BaseResponse` 是序列化外壳，不替业务定义错误码、HTTP 状态或国际化策略。
- Snowflake 的 worker ID 必须由部署保证唯一；本地生成器不提供跨副本协调。
- `ByteSize` 只做解析和换算，调用方仍须为上传、内存和响应体设置业务硬上限。
- 翻译器是进程级可替换引擎；不要把用户文本、凭据或远端错误直接写入低基数日志和指标。
