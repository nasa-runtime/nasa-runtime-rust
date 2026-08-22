# nabase

`nabase` 提供业务公共响应、日期时间、容量、ID、字符串、环境变量和翻译抽象。业务通常通过
`nasa` 的 `base` feature 一次引入全部能力：

```toml
[dependencies]
nasa = { version = "1", features = ["base"] }
```

模块入口如下：

| 模块或类型 | 业务作用 |
| --- | --- |
| `BaseResponse` | 统一成功和失败响应外壳 |
| `date` | epoch 毫秒、固定偏移、日期模式、日历位移、区间枚举和双时钟抽象 |
| `ByteSize` | 容量文本解析和 serde 反序列化 |
| `Snowflake` / `SnowflakeConfig` | 单进程雪花 ID 生成 |
| `strings` / `env` | 空白清洗和宽松环境变量读取 |
| `translator` | 可注入翻译引擎、缓存和语言归一机制 |

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

let generator = SnowflakeConfig::default().build_local()?;
let id = generator.next_id();
```

默认 `Snowflake` 是本地算法，不做 Redis workerId 分配；分布式 workerId 分配由 Redis 组件负责。

## 字符串和环境变量

```rust
let key = nasa::base::env::relaxed_env_key("app.redis-url");
assert_eq!(key, "APP_REDIS_URL");
```

`strings` 模块用于配置项、请求参数和环境变量的空白清洗。

## 日期时间

日期能力统一位于 `nasa::base::date`。启用 `base` 后也可以通过 `nasa::date` 使用同一组类型和函数。
所有日期时刻以 `i64` epoch 毫秒表示；未显式传入固定偏移时使用 GMT+8。

```rust
use nasa::base::date;

let ms = date::parse("2024-01-01 08:00:00", date::F_Y_M_D_H_M_S)?;
let label = date::format(ms, date::F_Y_M_D)?;
let next_month = date::add_months(ms, 1)?;
```

### 日期模式

| token | 含义 | 示例 |
| --- | --- | --- |
| `yyyy` | 四位年份 | `2026` |
| `yy` | 两位年份 | `26` |
| `MM` | 两位月份 | `08` |
| `dd` | 两位日 | `22` |
| `HH` | 24 小时制小时 | `14` |
| `mm` | 两位分钟 | `05` |
| `ss` | 两位秒 | `09` |
| `SSS` | 三位毫秒 | `123` |
| `'文本'` | 字面量 | `yyyy'年'MM'月'` |

预置常量覆盖 `yyyy-MM`、`yyyy-MM-dd`、`yyyy-MM-dd HH:mm:ss`、紧凑格式和路径格式。
`parse_auto` 只识别以下格式：`yyyy-MM-dd HH:mm:ss`、`yyyy-MM-dd`、`yyyy-MM`、
`yyyyMM`、`yyyyMMdd`、`yyyyMMddHHmmss`、`yyyyMMddHH`、`yyyyMMddHHmm`、
`yyyy/MM/dd HH:mm:ss`、`yyyy/MM/dd`、`yyyy/MM`、`yyyy`、`HH:mm:ss`、
`yyyy-MM-dd HH:mm` 和 `yyyy-MM-dd HH`。需要其它格式时必须显式调用 `parse` 或
`parse_offset`。缺失字段按年 `1970`、月日 `01`、时分秒 `00` 补齐。

### 日期位移和区间

```rust
let tomorrow = date::add_days(ms, 1)?;
let days = date::all_days(ms, date::add_days(ms, 7)?, date::F_Y_M_D)?;
```

毫秒到周的位移使用固定时长；自然月和自然年按 GMT+8 日历推进。目标月份没有对应日期时会收缩到
该月最后一天，例如一月三十一日加一个月落到二月最后一天。`all_time`、`all_days` 和
`all_months` 先产出对齐后的起点，再推进并判断终点，因此 `start == end` 返回包含起点的一个元素，
`start > end` 返回空列表。对齐后的首个时间点可能早于 `start`；返回列表的大小随区间长度线性增长，
调用方必须为外部输入设置区间上限。

### 单调时钟与墙钟

```rust
use nasa::base::date::{MonotonicClock, SystemClock, UtcClock};

let clock = SystemClock::new();
let started = MonotonicClock::now(&clock);
let wall_time = UtcClock::now(&clock);
let elapsed = MonotonicClock::now(&clock).saturating_duration_since(started);
```

`MonotonicClock` 用于 deadline、TTL、退避和耗时，只能比较同一进程时钟族产生的时刻；它不能持久化、
跨进程比较或充当分布式 lease/fencing 权威。`UtcClock` 用于协议时间窗和审计时间戳，系统校时可能使
它回拨，因此不能替代单调时钟。

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

`nabase` 不主动读取 yml，也不声明 `base` 配置根。下例是业务自己定义的配置投影；
`date` 和 `upload_limit` 键不是本 crate 的固定配置合同。

推荐配置示例：

```yaml
base:
  snowflake:
    worker_id: 1
    base_time: 1704038400000
    worker_id_bits: 6
    seq_bits: 6
  upload_limit: 500MB
  date:
    zone_offset_hours: 8
    datetime_pattern: yyyy-MM-dd HH:mm:ss
    date_pattern: yyyy-MM-dd
    retention_days: 30
```

字段说明：

| 示例键 | 省略语义 | 说明 |
| --- | --- | --- |
| `base.snowflake.worker_id` | `SnowflakeConfig` 内默认为 `1` | 当前节点 worker id，必须落在 `worker_id_bits` 允许范围内。 |
| `base.snowflake.base_time` | `SnowflakeConfig` 内默认为 `1704038400000` | 雪花算法基准时间戳，单位 ms；同一 ID 域必须保持一致。 |
| `base.snowflake.worker_id_bits` | `SnowflakeConfig` 内默认为 `6` | worker id 位数，合法范围为 1 到 15。 |
| `base.snowflake.seq_bits` | `SnowflakeConfig` 内默认为 `6` | 同毫秒序列号位数，合法范围为 3 到 21，且与 worker 位数之和不能超过 22。 |
| `base.upload_limit` | 无 `nabase` 默认值 | 可反序列化为 `ByteSize`，支持 `B`/`KB`/`KiB`/`MB`/`MiB`/`GB`/`GiB` 或纯字节数。 |
| `base.date.zone_offset_hours` | 无固定配置键；本例使用 `8` | 相对 UTC 的固定小时偏移；`date::offset_hours` 接受 -23 到 23。 |
| `base.date.datetime_pattern` | 无固定配置键；本例使用 `yyyy-MM-dd HH:mm:ss` | 业务日期时间输入输出模式；未识别的字符按字面量处理。 |
| `base.date.date_pattern` | 无固定配置键；本例使用 `yyyy-MM-dd` | 业务日期输入输出模式。 |
| `base.date.retention_days` | 无 `nabase` 默认值 | 保留窗口天数；传给 `date::add_days` 时按固定 24 小时计算。 |

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
    date: DateConfig,
}

#[derive(serde::Deserialize)]
struct DateConfig {
    zone_offset_hours: i32,
    datetime_pattern: String,
    date_pattern: String,
    retention_days: i64,
}

let id_gen = cfg.base.snowflake.build_local()?;
let max_upload_bytes = cfg.base.upload_limit.bytes();
let offset = nasa::base::date::offset_hours(cfg.base.date.zone_offset_hours)?;
let start = nasa::base::date::parse_offset(
    offset,
    "2026-08-22 00:00:00",
    &cfg.base.date.datetime_pattern,
)?;
let end = nasa::base::date::add_days(start, cfg.base.date.retention_days)?;
```

`BaseResponse`、字符串工具、环境变量工具和翻译器没有固定 yml；翻译器是否启用、使用哪个 engine 应由业务启动代码显式注入。

`SnowflakeConfig` 使用 `#[serde(default)]`，当 `snowflake` 对象已进入该类型后，其内部缺失字段会使用
上表默认值。如果业务还要允许整个 `snowflake` 键缺失，必须在外层字段上声明
`#[serde(default)]`。

## 主要边界

- `BaseResponse` 是序列化外壳，不替业务定义错误码、HTTP 状态或国际化策略。
- Snowflake 的 worker ID 必须由部署保证唯一；本地生成器不提供跨副本协调。
- `ByteSize` 只做解析和换算，调用方仍须为上传、内存和响应体设置业务硬上限。
- 翻译器是进程级可替换引擎；不要把用户文本、凭据或远端错误直接写入低基数日志和指标。
- 日期工具默认使用固定 GMT+8，不维护地域时区数据库，也不处理夏令时变化。
- `now_ms` 和 `UtcClock` 是墙钟，不能用于 deadline、租约控制权或跨节点顺序裁决。
