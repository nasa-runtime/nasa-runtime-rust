# nanum

`nanum` 是定点精确算术工具，面向金额、价格、撮合 tick 等场景。业务通常通过门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["numeric"] }
```

核心表示：

```text
真实值 = mantissa * 10^-scale
```

默认 scale 为 8，对齐历史 `Numeric.DEFAULT_FIXED_SCALE`。

## 定点运算

```rust
use nasa::numeric::{to_fixed_str, to_plain_string, multiply, divide};

let price = to_fixed_str("123.45", 8)?;
let qty = to_fixed_str("2", 8)?;

let notional = multiply(price, qty, 8)?;
assert_eq!(to_plain_string(notional, 8)?, "246.9");

let half = divide(price, to_fixed_str("2", 8)?, 8)?;
```

`multiply` / `divide` 逐值对齐历史实现：整数部分走整数，余数部分走 f64 中转和兼容舍入。

## 精度对齐

```rust
use nasa::numeric::{align_down, align_up};

let value = to_fixed_str("0.20021365", 8)?;
let tick_down = align_down(value, 8, 4)?;
let tick_up = align_up(value, 8, 4)?;
```

`align_down` 常用于买卖盘价格向 tick 对齐；`align_up` 适合需要向上补齐的场景。

## BigDecimal 路径

scale 大于 8 或需要任意精度时使用 `decimal` 模块：

```rust
use nasa::numeric::decimal;

let a = decimal::parse("1")?;
let b = decimal::parse("3")?;
let v = decimal::divide(&a, &b, 30)?;
```

BigDecimal 运算有 `MAX_DECIMAL_EXPANSION` 防 OOM 边界，极端 scale 会返回错误而不是 panic。

## f64 便捷路径

```rust
let v = nasa::numeric::float::divide(1.0, 3.0, 8)?;
```

f64 路径是便捷入口，内部仍走定点化以减少普通浮点误差；涉及 NaN 比较时使用 `eq_f64` / `gt_f64` 等兼容比较函数。

## 行为边界(实测)

- `to_fixed_str` 字符串先经 f64(逐值对齐原实现 parseDouble+round):有效数字 **≥16 位**开始失真;超过 `scale` 位的小数被**静默舍入**(`to_fixed_str("0.000000001", 8) = Ok(0)`)。需要任意精度的精确字符串解析请用 `decimal` 模块。
- `to_plain_string_display` / `to_plain_string_raw_display` 负值舍到 0 时输出 `"-0"`(逐值对齐原实现);两参 `to_plain_string` 则有 `-0` 守卫。展示层不接受 `-0` 时请自行归一。
- `align*` 语义:对齐到 `to_scale` 精度但**仍保持 `from_scale` 体系表示**(如 `align(20021365, 8, 4) = 20020000`,即 0.20021365 → 0.2002 仍 ×10^8),配合 `is_aligned` 做撮合 tick 校验。
- 定点四则无浮点误差(0.1+0.2 精确等于 0.3);除零、scale>8、i128 溢出、非有限 f64、`Unnecessary` 需舍入等均返回错误,不 panic。

## YML 配置与使用

`nanum` 不读取 yml。业务通常在配置里声明金额、价格、数量、tick 和 scale，再在启动或请求处理时转换为定点整数。

推荐配置示例：

```yaml
numeric:
  money_scale: 8
  price_scale: 8
  quantity_scale: 8
  price_tick: "0.0001"
  min_notional: "10"
  rounding: HalfUp
```

字段建议：

| 键 | 说明 |
| --- | --- |
| `money_scale` | 金额定点 scale，常用 8。 |
| `price_scale` | 价格定点 scale。 |
| `quantity_scale` | 数量定点 scale。 |
| `price_tick` | 价格 tick 字符串，启动时用 `to_fixed_str` 转成整数。 |
| `min_notional` | 最小名义金额字符串。 |
| `rounding` | 舍入模式，可映射为 `RoundingMode`。 |

使用代码：

```rust
let price_tick = nanum::to_fixed_str(&cfg.numeric.price_tick, cfg.numeric.price_scale)?;
let min_notional = nanum::to_fixed_str(&cfg.numeric.min_notional, cfg.numeric.money_scale)?;
let price = nanum::align_down(input_price, cfg.numeric.price_scale, cfg.numeric.price_scale)?;
```

配置里的小数建议保持字符串，不要用 yml 浮点数承载金额或价格，避免解析阶段先损失精度。
