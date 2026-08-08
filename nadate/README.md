# nadate

`nadate` 是日期时间工具 crate，对齐历史 `DateUtils` 的常用能力。统一使用 `i64 epoch ms`，默认时区为 GMT+8；同时提供可注入的单调时钟与墙钟抽象。

```toml
[dependencies]
nadate = "1"
```

## 格式化

```rust
let s = nadate::format(1_704_067_200_000, nadate::F_Y_M_D_H_M_S)?;
let day = nadate::format_y_m_d(1_704_067_200_000)?;
```

格式使用历史日期格式风格，例如 `yyyy-MM-dd HH:mm:ss`。

## 解析

```rust
let ms = nadate::parse("2024-01-01 08:00:00", nadate::F_Y_M_D_H_M_S)?;
let ms2 = nadate::parse_auto("20240101")?;
```

缺失字段会按默认值补齐，例如只给年月日时，时间部分补 `00:00:00`。

## 加减和区间

```rust
let tomorrow = nadate::add_days(ms, 1)?;
let next_month = nadate::add_months(ms, 1)?;

let days = nadate::all_days(
    ms,
    nadate::add_days(ms, 7)?,
    nadate::F_Y_M_D,
)?;
```

自然月/自然年按 GMT+8 日历推进，月末会按 chrono/Calendar 规则收缩。

## 当下

```rust
let now = nadate::now()?;
let today = nadate::today()?;
let yesterday = nadate::yesterday()?;
```

## 时钟抽象

需要 deadline、退避或耗时计算时使用 `MonotonicClock`，不要用可能被系统校时拨动的 UTC 墙钟；协议时间戳和审计时间使用 `UtcClock`。

```rust
use nadate::{MonotonicClock, SystemClock, UtcClock};

let clock = SystemClock::new();
let started = MonotonicClock::now(&clock);
let wall_time = UtcClock::now(&clock);
let elapsed = MonotonicClock::now(&clock).saturating_duration_since(started);
```

`MonotonicInstant` 只表示当前进程统一基准下的相对时刻，不能持久化或跨进程比较。

## 行为边界

- 默认 GMT+8:`format(0, "yyyy-MM-dd HH:mm:ss")` = `1970-01-01 08:00:00`;负 epoch(1970 前)可正常格式化。
- 两位年 `yy` 可正常解析:`parse("230501", "yyMMdd")` → 2023-05-01。
- 缺省字段补齐:只含日期的输入解析后时间为 `00:00:00`;`earliest(ms, "yyyy-MM-dd")` 截到当天零点。
- `add_months` 月末收缩:1-31 加一月 → 2-28(闰年 2-29);`add_years` 对闰日同样收缩;跨年进位正确。
- `all_time`/`all_days`/`all_months` 是 do-while 语义:先产出对齐后的起点、再步进判界,因此 `start == end` 返回含起点的 1 个元素;`start > end` 返回空。
- 非法日期(2 月 30、13 月)与 `parse_auto` 无法识别的输入返回错误,不 panic。

## YML 配置与使用

`nadate` 不读取 yml。业务可以把格式、时区偏移和时间窗口写入配置，再调用本 crate 的解析/格式化函数。

推荐配置示例：

```yaml
date:
  zone: GMT+8
  datetime_pattern: yyyy-MM-dd HH:mm:ss
  date_pattern: yyyy-MM-dd
  business_day_start: "00:00:00"
  retention_days: 30
```

字段建议：

| 键 | 说明 |
| --- | --- |
| `zone` | 业务时区说明；当前工具默认按 GMT+8 语义处理。 |
| `datetime_pattern` | 日期时间格式，例如 `yyyy-MM-dd HH:mm:ss`。 |
| `date_pattern` | 日期格式，例如 `yyyy-MM-dd`。 |
| `business_day_start` | 业务日切点；框架不自动读取，由业务计算窗口时使用。 |
| `retention_days` | 保留天数，适合日志、报表或缓存清理。 |

使用代码：

```rust
let start = nadate::parse("2026-07-18 00:00:00", &cfg.date.datetime_pattern)?;
let end = nadate::add_days(start, cfg.date.retention_days)?;
let label = nadate::format(start, &cfg.date.date_pattern)?;
```

配置中的时间戳建议统一用 epoch ms 或明确格式字符串；不要在同一字段里混用秒级和毫秒级。
