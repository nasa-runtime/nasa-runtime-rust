//! 编译期异步与定时任务属性宏。
//!
//! 本 crate 只负责改写被标注的函数和登记调度元数据，运行时由 `nasched`
//! 提供任务启动、停机、错过触发处理和集群去重。
// ============================================================================
// async-macro —— #[Async] / #[scheduled] / #[EnableAsync](注解驱动的异步 & 定时任务)
//
// 注解语义严格区分两个概念:
//   #[Async]            ≈ @Async      —— 简单异步执行:贴在 async fn 上(可带 owned 参数;属性本身无参),调用即【后台 spawn】、立即返回
//                                        JoinHandle(Future),不阻塞调用方。需要有人【调用】它。
//   #[scheduled]        ≈ @Scheduled  —— 定时任务:贴在【零参】async fn 上,被【自动注册+调度】跑,没人调它。
//                                        触发源 cron / fixed_rate / fixed_delay(各支持 _ms、数字+time_unit、_string 三写法;
//                                        旧别名 every_ms/delay_ms)+ initial_delay + cron 的 zone;
//                                        fixed_rate 限流 max_in_flight/skip_if_running;支持 repeatable(同函数多个 #[scheduled])。
//   #[EnableScheduling] ≈ @EnableScheduling —— 贴在 main 上(在 #[tokio::main] 之上),启动 #[scheduled] 调度。
//                                        (#[EnableAsync] 为兼容别名,新代码用 #[EnableScheduling]。)
//
// 机制:#[Async] 是【编译期改写】(把 async fn 改成"调用即 spawn 返回 JoinHandle");
//       #[scheduled] 是【收集式】(linkme 登记进 scheduling::SCHEDULED_TASKS,EnableScheduling 启动时拉起);
//       cron:misfire=skip 走 tokio-cron-scheduler,misfire=fire_once 走自管 CronPlan driver;非 cron 用 tokio::time。
//       运行端见 scheduling crate(start_scheduled)。
// ============================================================================

#![allow(non_snake_case)] // #[Async](async 是关键字,保留首字母大写)/ #[EnableAsync](同族保留);#[scheduled] 已转 snake_case

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, ItemFn, LitBool, LitInt, LitStr, ReturnType};

// ════════════════════════════════════════════════════════════════════════════
// 时间换算辅助(time_unit + 字面量 duration string)
// 仅处理【编译期字面量】:占位符/表达式(${...}/#{...})需配置/容器,显式拒绝。
// ════════════════════════════════════════════════════════════════════════════

/// `time_unit` 名 → 规范化单位记号(`ms/s/m/h/d/us/ns`)。供数字版与 string 无单位 fallback 共用。
///
/// # 参数
/// - `u`: 注解里填写的 `time_unit` 原始字符串,允许大小写和常见别名。
fn canonical_time_unit(u: &str) -> Result<&'static str, String> {
    match u.trim().to_ascii_lowercase().as_str() {
        "ms" | "milli" | "millis" | "millisecond" | "milliseconds" => Ok("ms"),
        "s" | "sec" | "secs" | "second" | "seconds" => Ok("s"),
        "m" | "min" | "mins" | "minute" | "minutes" => Ok("m"),
        "h" | "hr" | "hour" | "hours" => Ok("h"),
        "d" | "day" | "days" => Ok("d"),
        "us" | "micro" | "micros" | "microsecond" | "microseconds" => Ok("us"),
        "ns" | "nano" | "nanos" | "nanosecond" | "nanoseconds" => Ok("ns"),
        other => Err(format!(
            "time_unit \"{other}\" 不支持(用 ms/s/m/h/days/us/ns)"
        )),
    }
}

/// `value × 单位` → 毫秒。`us`/`ns` 仅当能**无损**折算(整除 1000 / 1_000_000)才接受,否则报错(调度精度毫秒,不静默截断)。
///
/// # 参数
/// - `value`: 注解中的数值部分,例如 `fixed_rate = 5` 或 duration string 中的 `5s` 的 `5`。
/// - `unit`: 已规范化的单位记号(`ms/s/m/h/d/us/ns`)。
/// - `ctx`: 错误定位上下文,通常是 string 原文或"数字版"。
fn unit_part_to_ms(value: u64, unit: &str, ctx: &str) -> Result<u64, String> {
    let mul = |f: u64| {
        value
            .checked_mul(f)
            .ok_or_else(|| format!("{ctx} 折算毫秒溢出"))
    };
    match unit {
        "ms" => Ok(value),
        "s" => mul(1_000),
        "m" => mul(60_000),
        "h" => mul(3_600_000),
        "d" => mul(86_400_000),
        "us" if value.is_multiple_of(1_000) => Ok(value / 1_000),
        "ns" if value.is_multiple_of(1_000_000) => Ok(value / 1_000_000),
        "us" | "ns" => Err(format!(
            "{ctx}:{value}{unit} 不能无损折算到毫秒(调度精度为毫秒)"
        )),
        other => Err(format!("{ctx}:不支持单位 \"{other}\"(用 d/h/m/s/ms/us/ns)")),
    }
}

/// 单位粒度排序:d>h>m>s>ms>us>ns。COMPOSITE 段必须严格从大到小(避免歧义与重复)。
///
/// # 参数
/// - `unit`: duration string 中已经转小写的单位段。
fn unit_rank(unit: &str) -> Option<u8> {
    match unit {
        "d" => Some(0),
        "h" => Some(1),
        "m" => Some(2),
        "s" => Some(3),
        "ms" => Some(4),
        "us" => Some(5),
        "ns" => Some(6),
        _ => None,
    }
}

/// 字面量 duration string → 毫秒(对照原 `*String` 语义)。`fallback_unit` = 无显式单位纯整数时采用的单位(来自 time_unit,默认 `ms`)。
/// 支持**编译期字面量**:可选 `+` 正号;纯整数(走 fallback);SIMPLE/COMPOSITE 段式 `1s500ms`/`1h12m27s`/`1d 2h 3m`(单位
/// `d/h/m/s/ms/us/ns`,从大到小);ISO-8601 `P1DT2H`/`PT1M30S`/`PT0.5S`。负号与占位符/表达式拒绝。
///
/// # 参数
/// - `s`: 注解里的 duration 字面量,例如 `fixed_rate_string = "1s500ms"`。
/// - `fallback_unit`: 纯整数 duration 没写单位时使用的单位,来自同一注解的 `time_unit`。
fn duration_str_to_ms(s: &str, fallback_unit: &str) -> Result<u64, String> {
    let t0 = s.trim();
    if t0.contains("${") || t0.contains("#{") {
        return Err(format!(
            "duration \"{t0}\" 含占位符/表达式,不支持(那需配置/容器,非编译期能力)"
        ));
    }
    if t0.is_empty() {
        return Err("duration 不能为空".into());
    }
    // 可选符号:`+` 正号放行;`-` 负号拒绝(调度周期/延迟不能为负)。
    let t = if let Some(r) = t0.strip_prefix('+') {
        r
    } else if t0.starts_with('-') {
        return Err(format!("duration \"{t0}\" 为负;调度周期/延迟不能为负"));
    } else {
        t0
    };
    if t.is_empty() {
        return Err(format!("duration \"{t0}\" 缺数值"));
    }
    // ISO-8601:大小写不限的 P 前缀(`P...`/`P...T...`)。**不接受外层括号**(对照原 parser:括号仅属 COMPOSITE)。
    if t.starts_with('P') || t.starts_with('p') {
        return parse_iso8601_duration(t);
    }
    // 纯整数:无显式单位,按 fallback_unit(time_unit)折算。**不接受外层括号**(纯数字属 SIMPLE,不带括号)。
    if t.bytes().all(|b| b.is_ascii_digit()) {
        let n: u64 = t
            .parse()
            .map_err(|_| format!("duration \"{t0}\" 数值非法"))?;
        return unit_part_to_ms(n, fallback_unit, &format!("duration \"{t0}\""));
    }
    // SIMPLE(单段)/ COMPOSITE(多段从大到小,可含空格)。**仅此分支允许可选外层括号** `(1s500ms)`(对照原 COMPOSITE):
    // 成对则剥掉,只有一侧则报错;剥后内容仍须是带单位段(`(100)`/`(PT1S)` 因无单位段/被前面分支拦截而落到这里报错)。
    let seg = if let Some(inner) = t.strip_prefix('(').and_then(|x| x.strip_suffix(')')) {
        inner.trim()
    } else if t.starts_with('(') || t.ends_with(')') {
        return Err(format!("duration \"{t0}\" 括号不匹配"));
    } else {
        t
    };
    let cs: Vec<char> = seg.chars().collect();
    let mut i = 0;
    let mut total: u64 = 0;
    let mut last_rank: Option<u8> = None;
    let mut seen = false;
    while i < cs.len() {
        if cs[i].is_whitespace() {
            i += 1;
            continue;
        }
        let ns = i;
        while i < cs.len() && cs[i].is_ascii_digit() {
            i += 1;
        }
        if i == ns {
            return Err(format!(
                "duration \"{t0}\" 解析失败(单位前缺数值;用 100 / 1s500ms / P1DT2H)"
            ));
        }
        let num: u64 = cs[ns..i]
            .iter()
            .collect::<String>()
            .parse()
            .map_err(|_| format!("duration \"{t0}\" 数值非法"))?;
        let us = i;
        while i < cs.len() && cs[i].is_ascii_alphabetic() {
            i += 1;
        }
        if i == us {
            return Err(format!(
                "duration \"{t0}\" 数值 {num} 缺单位(d/h/m/s/ms/us/ns)"
            ));
        }
        let unit = cs[us..i].iter().collect::<String>().to_ascii_lowercase();
        let rank = unit_rank(&unit).ok_or_else(|| {
            format!("duration \"{t0}\" 不支持单位 \"{unit}\"(用 d/h/m/s/ms/us/ns)")
        })?;
        if let Some(lr) = last_rank {
            if rank <= lr {
                return Err(format!(
                    "duration \"{t0}\" 单位须从大到小且不重复(如 1h30m,不能 30m1h / 1h1h)"
                ));
            }
        }
        last_rank = Some(rank);
        total = total
            .checked_add(unit_part_to_ms(num, &unit, &format!("duration \"{t0}\""))?)
            .ok_or_else(|| format!("duration \"{t0}\" 折算毫秒溢出"))?;
        seen = true;
    }
    if !seen {
        return Err(format!("duration \"{t0}\" 解析失败"));
    }
    Ok(total)
}

/// 解析 ISO-8601 时间段 `P[nD][T[nH][nM][n[.fffffffff]S]]`:日期段只取天 `D`,时间段(`T` 之后)取 `H`/`M`/`S`,
/// **仅秒可带小数(1~9 位,按纳秒解析)**,且须能**无损折算到毫秒**否则拒绝(同 us/ns 策略)。大小写不限。
/// 不支持年/月/周(`Y`/`W`/月,需历法不定长)。
///
/// # 参数
/// - `t`: 已确认以 `P`/`p` 开头的 ISO-8601 duration 字符串。
fn parse_iso8601_duration(t: &str) -> Result<u64, String> {
    let body = &t[1..]; // 去掉 P
                        // 以 'T'/'t' 分日期段与时间段。
    let (date_part, time_part) = match body.find(['T', 't']) {
        Some(i) => (&body[..i], &body[i + 1..]),
        None => (body, ""),
    };
    if date_part.is_empty() && time_part.is_empty() {
        return Err(format!(
            "ISO-8601 \"{t}\" 缺时间段(如 P1D / PT1S / PT1M30S)"
        ));
    }
    let mut total: u64 = 0;
    let add = |total: u64, n: u64, unit_ms: u64| -> Result<u64, String> {
        total
            .checked_add(
                n.checked_mul(unit_ms)
                    .ok_or_else(|| format!("ISO-8601 \"{t}\" 溢出"))?,
            )
            .ok_or_else(|| format!("ISO-8601 \"{t}\" 溢出"))
    };

    // 日期段:只允许整数 D。
    let mut num = String::new();
    for c in date_part.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        if num.is_empty() {
            return Err(format!("ISO-8601 \"{t}\" 单位 {c} 前缺数值"));
        }
        let n: u64 = num
            .parse()
            .map_err(|_| format!("ISO-8601 \"{t}\" 数值非法"))?;
        match c.to_ascii_uppercase() {
            'D' => total = add(total, n, 86_400_000)?,
            'Y' | 'W' => return Err(format!("ISO-8601 \"{t}\" 不支持年/周段(长度不定)")),
            other => return Err(format!("ISO-8601 \"{t}\" 日期段不支持单位 {other}(仅 D)")),
        }
        num.clear();
    }
    if !num.is_empty() {
        return Err(format!("ISO-8601 \"{t}\" 日期段数值 {num} 缺单位(D)"));
    }

    // 时间段:H/M 整数,S 可带 1~9 位小数(按纳秒解析,须能无损折算到毫秒)。
    num.clear();
    let mut frac: Option<String> = None; // S 的小数部分
    for c in time_part.chars() {
        if c.is_ascii_digit() {
            match &mut frac {
                Some(f) => f.push(c),
                None => num.push(c),
            }
            continue;
        }
        if c == '.' {
            if frac.is_some() {
                return Err(format!("ISO-8601 \"{t}\" 小数点重复"));
            }
            frac = Some(String::new());
            continue;
        }
        if num.is_empty() {
            return Err(format!("ISO-8601 \"{t}\" 单位 {c} 前缺数值"));
        }
        let n: u64 = num
            .parse()
            .map_err(|_| format!("ISO-8601 \"{t}\" 数值非法"))?;
        match c.to_ascii_uppercase() {
            'H' if frac.is_none() => total = add(total, n, 3_600_000)?,
            'M' if frac.is_none() => total = add(total, n, 60_000)?,
            'S' => {
                total = add(total, n, 1_000)?;
                if let Some(f) = frac.take() {
                    // 小数秒按纳秒精度(最多 9 位)解析,再要求能**无损**折算到毫秒(同 us/ns 策略);否则报错。
                    if f.is_empty() || f.len() > 9 {
                        return Err(format!("ISO-8601 \"{t}\" 秒的小数须为 1~9 位"));
                    }
                    let nanos: u64 = format!("{f:0<9}").parse().unwrap(); // 右补零到 9 位 = 纳秒
                    if !nanos.is_multiple_of(1_000_000) {
                        return Err(format!(
                            "ISO-8601 \"{t}\" 小数秒不能无损折算到毫秒(调度精度为毫秒)"
                        ));
                    }
                    let ms = nanos / 1_000_000;
                    total = total
                        .checked_add(ms)
                        .ok_or_else(|| format!("ISO-8601 \"{t}\" 溢出"))?;
                }
            }
            'H' | 'M' => return Err(format!("ISO-8601 \"{t}\" 只有秒可带小数")),
            other => {
                return Err(format!(
                    "ISO-8601 \"{t}\" 时间段不支持单位 {other}(仅 H/M/S)"
                ))
            }
        }
        num.clear();
    }
    if !num.is_empty() || frac.is_some() {
        return Err(format!("ISO-8601 \"{t}\" 末尾数值缺单位(H/M/S)"));
    }
    Ok(total)
}

/// 从多个来源里收敛出**最多一个**(>1 个即互斥冲突),返回 (选中来源名, 毫秒值)。
///
/// # 参数
/// - `family`: 参数族名称,用于错误文案,例如 `fixed_rate` 或 `fixed_delay`。
/// - `cands`: 已解析成功的候选来源列表,元素为 `(来源名, 毫秒值)`。
fn pick_one<'a>(
    family: &str,
    cands: Vec<(&'a str, u64)>,
) -> Result<Option<(&'a str, u64)>, String> {
    if cands.len() > 1 {
        let names: Vec<&str> = cands.iter().map(|(n, _)| *n).collect();
        return Err(format!(
            "{family} 多个来源互斥,只能给一个:{}",
            names.join(" / ")
        ));
    }
    Ok(cands.into_iter().next())
}

/// 返回类型是否是【可解构 / 可 `?` 的标准 Result】:**仅带明确路径前缀**的 `std::result::Result` / `core::result::Result` /
/// `anyhow::Result`(且末段带角括号泛型参数)。
/// **刻意不收裸名 `Result`**:过程宏无法解析裸名真实来源,业务可能有自定义 `struct Result<T,E>` 或别名,把它当标准 Result
/// 解构(`.is_err()` / `?`)会破坏合法代码编译;且"返回值被忽略"本就是默认契约,Err 自动记日志只是增强。
/// 故裸名 `Result<…>`、类型别名、`custom::Result` 一律按**普通返回值忽略**——保证"任意返回类型都能用、绝不因命名碰撞编译失败"。
/// 想让调度器自动记 `Err`:用 `anyhow::Result` 或 `std::result::Result` / `core::result::Result` 的**显式路径**返回。
///
/// # 参数
/// - `output`: 被 `#[scheduled]` 标注函数的返回类型 AST。
fn is_result_return(output: &ReturnType) -> bool {
    let ReturnType::Type(_, ty) = output else {
        return false;
    };
    let syn::Type::Path(tp) = &**ty else {
        return false;
    };
    // 末段必须带角括号泛型类型参数(真 Result 必有 `<…>`)。
    let Some(last) = tp.path.segments.last() else {
        return false;
    };
    let has_type_arg = matches!(&last.arguments, syn::PathArguments::AngleBracketed(a)
        if a.args.iter().any(|x| matches!(x, syn::GenericArgument::Type(_))));
    if !has_type_arg {
        return false;
    }
    let segs: Vec<String> = tp
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();
    matches!(
        segs.iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        ["anyhow", "Result"] | ["std", "result", "Result"] | ["core", "result", "Result"]
    )
}

/// 返回类型是否是 `anyhow::Result<_>`。**专供 `#[EnableScheduling]` 判断能否注入 `?`**:
/// `start_scheduled()` 返回 `anyhow::Result<()>`,`?` 需把其 `anyhow::Error` 转成 main 的错误类型;只有 main 也返回
/// `anyhow::Result<_>` 时这一转换**必然成立**(同类型)。其它显式路径 Result(如 `std::result::Result<_, String>`,
/// `String` 不实现 `From<anyhow::Error>`)若注入 `?` 会编译失败——过程宏无法静态求解 `From`,故只认 anyhow,其余走 expect。
///
/// # 参数
/// - `output`: 被 `#[EnableScheduling]` 标注 main 函数的返回类型 AST。
fn is_anyhow_result_return(output: &ReturnType) -> bool {
    let ReturnType::Type(_, ty) = output else {
        return false;
    };
    let syn::Type::Path(tp) = &**ty else {
        return false;
    };
    let Some(last) = tp.path.segments.last() else {
        return false;
    };
    let has_type_arg = matches!(&last.arguments, syn::PathArguments::AngleBracketed(a)
        if a.args.iter().any(|x| matches!(x, syn::GenericArgument::Type(_))));
    if !has_type_arg {
        return false;
    }
    let segs: Vec<String> = tp
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();
    matches!(
        segs.iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        ["anyhow", "Result"]
    )
}

// ════════════════════════════════════════════════════════════════════════════
// #[Async] —— 简单异步执行(调用即后台跑,返回 JoinHandle)
// ════════════════════════════════════════════════════════════════════════════
/// # `#[Async]` —— 简单异步执行
///
/// 贴在【带参/零参均可】的 `async fn foo(args) -> T` 上。编译期改写成【同步函数】
/// `fn foo(args) -> JoinHandle<T>`:调用即 `tokio::spawn` 原体到后台、立即返回 `JoinHandle<T>`
/// (实现 Future,可 `.await` 取结果,也可丢弃 = fire-and-forget),不阻塞调用方。
///
/// ## 参数
/// **无**(`#[Async]` 不带任何参数)。
///
/// ## 用法示例
/// ```ignore
/// #[Async]
/// async fn send_mail(to: String) -> bool { /* ... */ }   // 改写后:fn send_mail(to: String) -> JoinHandle<bool>
///
/// send_mail("a@b.com".into());                 // fire-and-forget:调用即后台跑,立即返回
/// let ok = send_mail("a@b.com".into()).await?; // 也可 .await 拿结果(JoinHandle 实现 Future)
/// ```
/// ⚠️ 走 `tokio::spawn`:参数与返回值 `T` 都须 `Send + 'static`(参数取自有值,如 `String` 而非 `&str`)。
///    需要有人【调用】才会执行(对照 `#[scheduled]` 是自动跑)。
///
/// ## 实例方法 / impl 块
/// 可用于满足 `Send + 'static` 的方法:owned `self` 或 `self: Arc<Self>` 接收者 + owned 参数。
/// **不可**用于 `&self` / `&mut self`(后台任务不能捕获借用接收者)——宏会直接 compile_error 提示改用 `self: Arc<Self>`。
/// 也可贴在【inherent impl 块】上,批量改写块内所有 async 方法(对照原 @Async 的 type-level;块内出现同步方法会 compile_error;
/// const/type 等非方法项原样保留;**impl 级与方法级 #[Async] 不可叠加**,块内方法别再单独标 #[Async])。
/// **trait impl 不支持**(改返回类型会破坏 trait 签名);标在 `struct`/`trait` 定义上也不支持。
/// ```ignore
/// #[Async]
/// impl Worker {
///     async fn run(self: std::sync::Arc<Self>) { /* ... */ }   // 改写为 fn run(..) -> JoinHandle<()>
///     async fn poll() -> u32 { 0 }                              // 关联函数同样改写
///     // async fn bad(&self) {}                                  // 编译错误:请改 Arc<Self>
/// }
/// ```
///
/// # 参数
///
/// - `attr`: `#[Async(...)]` 括号内的编译期参数；当前必须为空。
/// - `item`: 被注解的 async 函数或 inherent impl 块的 token stream。
#[proc_macro_attribute]
pub fn Async(attr: TokenStream, item: TokenStream) -> TokenStream {
    // 当前不支持 executor qualifier;非空参数显式报错,避免用户误以为已经按名字选择执行器。
    // 如后续要支持多执行器,应设计为可编译期展开的路径参数,而不是运行期字符串查找。
    if !attr.is_empty() {
        let item2: proc_macro2::TokenStream = item.into();
        return quote! {
            ::core::compile_error!("#[Async] 暂不支持【属性参数】(executor qualifier 尚未实现);请直接写 #[Async](被标注函数可带 owned 参数)");
            #item2
        }
        .into();
    }
    // 运行时根路径发现:门面 re-export → 对应门面路径;直接依赖 → ::scheduling。
    let root = match nasa_macro_support::runtime_root("scheduling", "nasched") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };

    // 单个 async fn → "调用即 spawn、返回 JoinHandle" 的同步 fn。Ok=改写结果;Err=compile_error 片段。
    // 形态校验:① 只接受 async fn(同步 fn 丢进 spawn 会卡死 worker,应另走 spawn_blocking);
    //          ② 拒绝 `&self`/`&mut self`(借用接收者无法满足 spawn 要求的 'static,给清晰报错而非底层 borrow escape)。
    let rewrite = |attrs: &[syn::Attribute],
                   vis: &syn::Visibility,
                   sig: &syn::Signature,
                   block: &syn::Block|
     -> Result<proc_macro2::TokenStream, proc_macro2::TokenStream> {
        if sig.asyncness.is_none() {
            return Err(syn::Error::new_spanned(
                &sig.ident,
                "#[Async] 只能用于 async fn(同步阻塞任务请另行用 spawn_blocking,不要静默丢进异步 worker)",
            )
            .to_compile_error());
        }
        if let Some(syn::FnArg::Receiver(r)) = sig.inputs.first() {
            if r.reference.is_some() {
                return Err(syn::Error::new_spanned(
                    r,
                    "#[Async] 不能用于 &self / &mut self 方法(后台任务不能捕获借用接收者);请改用 self: Arc<Self> 或 owned self",
                )
                .to_compile_error());
            }
        }
        let out_ty = match &sig.output {
            ReturnType::Default => quote! { () },
            ReturnType::Type(_, t) => quote! { #t },
        };
        let mut new_sig = sig.clone();
        new_sig.asyncness = None; // 改成同步函数:立即返回 handle
        new_sig.output = parse_quote!(-> #root::__private::tokio::task::JoinHandle<#out_ty>);
        Ok(quote! {
            #(#attrs)*
            #vis #new_sig {
                // 调用即 spawn:`async move #block` 按值捕获(参数/self 须 Send+'static),丢后台跑、立即返回 JoinHandle。
                #root::__private::tokio::spawn(async move #block)
            }
        })
    };

    // 入口形态:支持贴在【自由/关联 async fn】或【impl 块】上(对照原 @Async 的 METHOD/TYPE 目标——
    // impl 块是 Rust 可做的 type-level 近似:改写本块内所有 async 方法;标在 struct/trait 定义上做不到,不做)。
    let parsed = parse_macro_input!(item as syn::Item);
    match parsed {
        syn::Item::Fn(f) => match rewrite(&f.attrs, &f.vis, &f.sig, &f.block) {
            Ok(ts) => ts.into(),
            Err(e) => e.into(),
        },
        syn::Item::Impl(imp) => {
            // trait impl 拒绝:trait 方法签名必须与 trait 定义一致,改写返回类型为 JoinHandle 会破坏签名(展开后底层报
            // "incompatible type for trait")。只支持 inherent impl;trait 抽象由用户在具体方法上自行设计异步入口。
            if imp.trait_.is_some() {
                return syn::Error::new_spanned(
                    &imp.self_ty,
                    "#[Async] 不支持 trait impl(会破坏 trait 方法签名);请贴在 inherent impl(impl Type {})或具体 async fn 上",
                )
                .to_compile_error()
                .into();
            }
            // 逐个改写 impl 块内的方法;非 async 方法直接报错(避免"以为全异步化、实则静默跳过");const/type 等原样保留。
            let mut out_items: Vec<proc_macro2::TokenStream> = Vec::new();
            for it in &imp.items {
                match it {
                    syn::ImplItem::Fn(m) => {
                        // impl 级 #[Async] 已批量改写本块所有方法;若某方法又单独标了 #[Async],外层展开会保留它,
                        // 待其再处理一个【已被改写成同步】的 fn,只会报"只能用于 async fn"——指向一个看起来仍是 async 的
                        // 方法,误导性极强。这里前置拦截,给明确"二选一"提示(impl 级与方法级 #[Async] 不能叠加)。
                        if m.attrs.iter().any(|a| {
                            a.path().segments.last().is_some_and(|s| s.ident == "Async")
                        }) {
                            return syn::Error::new_spanned(
                                &m.sig.ident,
                                "#[Async] impl 块内的方法不要再单独标 #[Async](impl 级已批量改写,二选一)",
                            )
                            .to_compile_error()
                            .into();
                        }
                        match rewrite(&m.attrs, &m.vis, &m.sig, &m.block) {
                            Ok(ts) => out_items.push(ts),
                            Err(e) => return e.into(),
                        }
                    }
                    other => out_items.push(quote! { #other }),
                }
            }
            let imp_attrs = &imp.attrs;
            let (ig, _tg, wc) = imp.generics.split_for_impl();
            let self_ty = &imp.self_ty;
            let trait_part = imp.trait_.as_ref().map(|(bang, path, for_)| quote! { #bang #path #for_ });
            quote! {
                #(#imp_attrs)*
                impl #ig #trait_part #self_ty #wc {
                    #(#out_items)*
                }
            }
            .into()
        }
        other => syn::Error::new_spanned(
            other,
            "#[Async] 只能贴在 async fn 或 impl 块上(标在 struct/trait 定义上无法改写散落的 impl,不支持)",
        )
        .to_compile_error()
        .into(),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// #[scheduled] —— 定时任务(零参,自动注册 + 调度)
// ════════════════════════════════════════════════════════════════════════════
/// # `#[scheduled]` —— 定时任务
///
/// 贴在【零参 async fn】上,被自动注册并由调度器跑(没人调它)。**返回类型不限**——值被忽略(对照"返回值被调度器忽略")。
/// 若返回**带显式路径的标准 Result**(`std::result::Result<_,_>` / `core::result::Result<_,_>` / `anyhow::Result<_>`),
/// `Err` 会记 **error 日志**(任务名 + "返回 Err";**不格式化错误值,故不要求 `E: Debug`**)。
/// **裸名 `Result<_,_>`、类型别名、`custom::Result` 一律按普通返回值忽略**(过程宏无法解析裸名真实来源,为不破坏自定义同名类型而保守处理)。
/// 想要 `Err` 自动记日志,请用上述显式路径返回(或在任务体内自行 log)。★ 需 main 上有 `#[EnableScheduling]` 才会启动。
/// **真毫秒精度**:非 cron 走 `tokio::time`(不经 cron 引擎的秒级截断)。函数必须 `async` + 零参,否则编译期报错。
///
/// ## 触发参数(snake_case;cron / fixed_rate / fixed_delay 恰好给一类)
/// | 语义 | 来源(三选一,等价) | 说明 |
/// |---|---|---|
/// | 固定频率 | `fixed_rate_ms` / `fixed_rate`+`time_unit` / `fixed_rate_string`(旧别名 `every_ms`) | 两次**开始**间隔,**按开始时间触发**(>0) |
/// | 固定延迟 | `fixed_delay_ms` / `fixed_delay`+`time_unit` / `fixed_delay_string` | 上次**完成**后再等(>0,串行不重叠) |
/// | 首跑延迟 | `initial_delay_ms` / `initial_delay`+`time_unit` / `initial_delay_string`(旧别名 `delay_ms`) | 配合 rate/delay;**单独用 = 一次性任务** |
/// | cron | `cron`(`"-"`=禁用) | 6 字段(含秒)`秒 分 时 日 月 周` |
/// | cron 时区 | `zone`(仅配 cron) | `"UTC"`(默认)/ IANA 如 `"Asia/Shanghai"` / 固定 offset 如 `"+08:00"` |
/// | 任务名 | `name` | 默认函数名,用于日志 |
///
/// 时间写法:`*_ms` 直接毫秒;数字版 `fixed_rate=5, time_unit="seconds"`(单位 `ms/s/m/h/days/us/ns`;作用于数字版,也作
/// string 纯整数 fallback;`us/ns` 须能无损折算成毫秒);字符串版 `fixed_rate_string=` 支持:`"100"`(无单位走 fallback)/
/// 单段 `"500ms"`/`"5s"`/`"1d"`/ 组合 `"1h12m27s"`/`"1s500ms"`(单位从大到小,可含空格,可加外层括号 `"(1s500ms)"`)/
/// ISO-8601 `"P1DT2H"`/`"PT1M30S"`/`"PT0.5S"`(秒小数最多 9 位,须能无损折算到毫秒);可带 `+` 正号;负号、占位符
/// `${...}`/`#{...}` 拒绝。
///
/// ## fixed_rate 过载保护(仅固定频率可重叠,故仅它支持)
/// | 参数 | 含义 |
/// |---|---|
/// | `max_in_flight = n` | 在跑数达 n 时**跳过本拍**(限并发,防慢任务堆积);默认不限=允许重叠 |
/// | `skip_if_running = true` | 等价 `max_in_flight = 1`(上次没跑完就跳过本拍) |
///
/// 互斥:三类触发源恰好一个;同一家族的多个来源(`_ms`/数字+unit/`_string`/旧别名)只能给一个;`cron` 不可配
/// `initial_delay`/`zone` 只配 cron;`max_in_flight`↔`skip_if_running` 二选一且只配 `fixed_rate`。
/// 首跑默认:`fixed_rate`(含 `_ms`/`_string`)**立即**(对齐原 fixedRate);旧别名 `every_ms` **一个周期后**(兼容)。
///
/// ## 用法示例
/// ```ignore
/// #[scheduled(initial_delay_ms = 3000)]                          async fn warmup() {}    // 启动后 3s 跑一次
/// #[scheduled(fixed_rate_ms = 5000)]                             async fn heartbeat() {} // 每 5s(立即首跑)
/// #[scheduled(fixed_rate_ms = 5000, initial_delay_ms = 1000)]    async fn poll() {}      // 1s 后开跑,之后每 5s
/// #[scheduled(fixed_rate = 5, time_unit = "seconds")]            async fn beat2() {}     // = fixed_rate_ms = 5000
/// #[scheduled(fixed_rate_string = "500ms")]                      async fn beat3() {}     // = fixed_rate_ms = 500
/// #[scheduled(fixed_rate_ms = 1000, skip_if_running = true)]     async fn slow() {}      // 上次没跑完就跳过本拍
/// #[scheduled(fixed_rate_ms = 1000, max_in_flight = 3)]          async fn capped() {}    // 最多 3 个并发
/// #[scheduled(fixed_delay_ms = 2000)]                            async fn drain() {}     // 完成后等 2s 再跑
/// #[scheduled(cron = "0/10 * * * * *", zone = "Asia/Shanghai")]  async fn tick() -> anyhow::Result<()> { Ok(()) }
/// // 旧别名仍可用:#[scheduled(every_ms = 5000)] / #[scheduled(delay_ms = 3000, every_ms = 5000)]
/// ```
///
/// # 参数
///
/// - `attr`: `#[scheduled(...)]` 括号内的调度配置 token stream。
/// - `item`: 被注解的零参 async 函数 token stream。
#[proc_macro_attribute]
pub fn scheduled(attr: TokenStream, item: TokenStream) -> TokenStream {
    // ⓪ 运行时根路径发现(同 #[Async])。
    let root = match nasa_macro_support::runtime_root("scheduling", "nasched") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    // 报错小工具:站点级 compile_error(从本函数返回)。
    macro_rules! bail {
        ($msg:expr) => {
            return syn::Error::new(proc_macro2::Span::call_site(), $msg)
                .to_compile_error()
                .into()
        };
    }

    // ① 解析参数(snake_case 主名;every_ms/delay_ms 为旧别名)。
    // 这里只接受字面量键值,让未知键、表达式和占位符在编译期失败;运行期配置应由业务显式转换后调用底层 API。
    let mut cron: Option<String> = None;
    let mut every_ms: Option<u64> = None; // 旧别名 = fixed_rate_ms
    let mut delay_ms: Option<u64> = None; // 旧别名 = initial_delay_ms
    let mut fixed_rate_ms: Option<u64> = None;
    let mut fixed_delay_ms: Option<u64> = None;
    let mut initial_delay_ms: Option<u64> = None;
    let mut name_attr: Option<String> = None;
    let mut zone: Option<String> = None; // cron 时区(仅与 cron 同用)
                                         // 数字版 + time_unit(对照原 timeUnit):值×单位因子折成 ms。time_unit 默认毫秒。
    let mut fixed_rate: Option<u64> = None;
    let mut fixed_delay: Option<u64> = None;
    let mut initial_delay: Option<u64> = None;
    let mut time_unit: Option<String> = None;
    // 字面量 duration string(对照原 *String,但只收编译期字面量):"500ms"/"5s"/"2m"/"1h"。
    let mut fixed_rate_string: Option<String> = None;
    let mut fixed_delay_string: Option<String> = None;
    let mut initial_delay_string: Option<String> = None;
    // 过载保护(仅 fixed_rate 有意义):max_in_flight=并发上限;skip_if_running=true 等价 max_in_flight=1。
    let mut max_in_flight: Option<u64> = None;
    let mut skip_if_running: Option<bool> = None;
    // 集群执行模式(d.md):"local"(默认,每节点跑)/ "leader"(仅 leader 触发)。
    let mut cluster: Option<String> = None;
    // 漏触发补偿(d.md):"skip"(默认)/ "fire_once"(仅 cron 有意义)。
    let mut misfire: Option<String> = None;
    // 任务级超时 ms(d.md #7):tokio::time::timeout 协作式;超时记 RunStatus::TimedOut(不强杀阻塞 I/O)。
    let mut timeout_ms: Option<u64> = None;
    // 分组 leader(d.md):仅 cluster=leader 有意义;决定用哪个 group gate。
    let mut group: Option<String> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("cron") {
            cron = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("zone") {
            zone = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("every_ms") {
            every_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("delay_ms") {
            delay_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("fixed_rate_ms") {
            fixed_rate_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("fixed_delay_ms") {
            fixed_delay_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("initial_delay_ms") {
            initial_delay_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("fixed_rate") {
            fixed_rate = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("fixed_delay") {
            fixed_delay = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("initial_delay") {
            initial_delay = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("time_unit") {
            time_unit = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("fixed_rate_string") {
            fixed_rate_string = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("fixed_delay_string") {
            fixed_delay_string = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("initial_delay_string") {
            initial_delay_string = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("name") {
            name_attr = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("max_in_flight") {
            max_in_flight = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("skip_if_running") {
            skip_if_running = Some(meta.value()?.parse::<LitBool>()?.value());
        } else if meta.path.is_ident("cluster") {
            cluster = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("misfire") {
            misfire = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("timeout_ms") {
            timeout_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("group") {
            group = Some(meta.value()?.parse::<LitStr>()?.value());
        } else {
            return Err(meta.error(
                "#[scheduled]: 支持 cron / zone / fixed_rate(_ms/_string) / fixed_delay(_ms/_string) / initial_delay(_ms/_string) / time_unit / every_ms / delay_ms / name / max_in_flight / skip_if_running / cluster / misfire / timeout_ms / group",
            ));
        }
        Ok(())
    });
    parse_macro_input!(attr with parser);

    // ② 解析被注解 fn(本体原样保留)+ 形态校验:定时任务必须 async、零参。
    let func = parse_macro_input!(item as ItemFn);
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&func.sig.ident, "#[scheduled] 只能用于 async fn")
            .to_compile_error()
            .into();
    }
    if !func.sig.inputs.is_empty() {
        return syn::Error::new_spanned(&func.sig.inputs, "#[scheduled] 只能用于【零参】async fn")
            .to_compile_error()
            .into();
    }

    let fn_ident = func.sig.ident.clone();
    let name = name_attr.unwrap_or_else(|| fn_ident.to_string());
    // 稳定 task id —— FireOnce/ClaimOnly 的 claim key:跨节点一致、**路径无关**(不含 #root),且 repeatable
    // 「同名 + 不同 cron」不会撞同一 claim field(调度设计约束)。cron 任务 = `name|cron|expr|zone`;
    // 非 cron = `name`(非 cron 不参与 claim)。RunEvent 仍用 name 作展示名;id 仅供 FireLog claim/去重。
    // zone 归一(调度设计约束 #5):UTC 等价字面量(空 / UTC / GMT / Z,忽略大小写)→ "UTC",
    // 保证语义相同的 zone 产出同一 task id(claim key 稳定);IANA / 固定 offset 原样保留。
    let zone_id: String = match zone.as_deref().map(str::trim) {
        None | Some("") => "UTC".to_string(),
        Some(z) if z.eq_ignore_ascii_case("UTC") || z.eq_ignore_ascii_case("GMT") || z == "Z" => {
            "UTC".to_string()
        }
        Some(z) => z.to_string(),
    };
    let id = match &cron {
        Some(c) => format!("{name}|cron|{c}|{zone_id}"),
        None => name.clone(),
    };

    // ②.5 归一化 + 触发源选择:cron / fixed_rate / fixed_delay 恰好一个;initial_delay 仅配合或单独=一次性。
    // 每"家族"可由多种来源给(_ms 主名 / 旧别名 / 数字+time_unit / duration string),最终都折算成 ms,且最多一个来源。
    // time_unit 规范化单位(默认毫秒)。对齐原语义,它的作用对象有两类:
    //   ① 数字版 fixed_rate/fixed_delay/initial_delay → 值按该单位折成 ms;
    //   ② *_string 里**无显式单位**的纯整数(如 "1")→ 用该单位作 fallback(显式单位/ISO 的 string 忽略它)。
    // 因此只要给了 time_unit,就必须有这两类作用对象之一,否则是误配(纯 _ms / cron 等没有可作用目标)。
    let funit: &str = match &time_unit {
        Some(u) => match canonical_time_unit(u) {
            Ok(f) => f,
            Err(m) => bail!(format!("#[scheduled]: {m}")),
        },
        None => "ms",
    };
    let has_numeric = fixed_rate.is_some() || fixed_delay.is_some() || initial_delay.is_some();
    let has_string = fixed_rate_string.is_some()
        || fixed_delay_string.is_some()
        || initial_delay_string.is_some();
    if time_unit.is_some() && !has_numeric && !has_string {
        bail!("#[scheduled]: time_unit 需配合数字版 fixed_rate/fixed_delay/initial_delay 或 *_string 使用(_ms 已自带单位)");
    }
    // 把 Result<u64,String>(单位换算 / 字符串解析)落地为值或 compile_error。
    macro_rules! ms {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(m) => bail!(format!("#[scheduled]: {m}")),
            }
        };
    }
    // 数字版折算:值按 time_unit 单位 → ms(us/ns 要求无损)。
    let mul = |v: u64| unit_part_to_ms(v, funit, "数字版");
    macro_rules! pick {
        ($family:expr, $c:expr) => {
            match pick_one($family, $c) {
                Ok(p) => p,
                Err(m) => bail!(format!("#[scheduled]: {m}")),
            }
        };
    }

    // rate 家族:fixed_rate_ms / every_ms(旧别名) / fixed_rate(+time_unit) / fixed_rate_string
    let mut rate_c: Vec<(&str, u64)> = Vec::new();
    if let Some(v) = fixed_rate_ms {
        rate_c.push(("fixed_rate_ms", v));
    }
    if let Some(v) = every_ms {
        rate_c.push(("every_ms", v));
    }
    if let Some(v) = fixed_rate {
        rate_c.push(("fixed_rate", ms!(mul(v))));
    }
    if let Some(s) = &fixed_rate_string {
        rate_c.push(("fixed_rate_string", ms!(duration_str_to_ms(s, funit))));
    }
    let rate_pick = pick!("fixed_rate", rate_c);
    let rate = rate_pick.map(|(_, v)| v);
    // 旧别名 every_ms 默认"一个周期后首跑";其余 rate 来源(fixed_rate(_ms/_string))默认立即首跑(对齐原语义)。
    let rate_is_every = matches!(rate_pick, Some(("every_ms", _)));

    // delay 家族:fixed_delay_ms / fixed_delay(+time_unit) / fixed_delay_string
    let mut delay_c: Vec<(&str, u64)> = Vec::new();
    if let Some(v) = fixed_delay_ms {
        delay_c.push(("fixed_delay_ms", v));
    }
    if let Some(v) = fixed_delay {
        delay_c.push(("fixed_delay", ms!(mul(v))));
    }
    if let Some(s) = &fixed_delay_string {
        delay_c.push(("fixed_delay_string", ms!(duration_str_to_ms(s, funit))));
    }
    let delay = pick!("fixed_delay", delay_c).map(|(_, v)| v);

    // initial 家族:initial_delay_ms / delay_ms(旧别名) / initial_delay(+time_unit) / initial_delay_string
    let mut init_c: Vec<(&str, u64)> = Vec::new();
    if let Some(v) = initial_delay_ms {
        init_c.push(("initial_delay_ms", v));
    }
    if let Some(v) = delay_ms {
        init_c.push(("delay_ms", v));
    }
    if let Some(v) = initial_delay {
        init_c.push(("initial_delay", ms!(mul(v))));
    }
    if let Some(s) = &initial_delay_string {
        init_c.push(("initial_delay_string", ms!(duration_str_to_ms(s, funit))));
    }
    let initial = pick!("initial_delay", init_c).map(|(_, v)| v);

    if rate == Some(0) {
        bail!("#[scheduled]: fixed_rate 必须 > 0(为 0 会导致后台 interval panic)");
    }
    if delay == Some(0) {
        bail!("#[scheduled]: fixed_delay 必须 > 0");
    }
    if cron.is_some() && (rate.is_some() || delay.is_some() || initial.is_some()) {
        bail!("#[scheduled]: cron 与 fixed_rate/fixed_delay/initial_delay 互斥(cron 不支持 initial_delay)");
    }
    if rate.is_some() && delay.is_some() {
        bail!("#[scheduled]: fixed_rate 与 fixed_delay 互斥(三类触发源恰好一个)");
    }
    // zone 只对 cron 有意义(它是 cron 触发时区);用在非 cron 上是配置错误。
    if zone.is_some() && cron.is_none() {
        bail!("#[scheduled]: zone 仅用于 cron(时区只影响 cron 触发计算)");
    }

    // 过载保护归一化:skip_if_running=true ≡ max_in_flight=1(二者并存即冲突);仅 fixed_rate(可重叠)才有意义。
    // 注:"是否显式写了 skip_if_running"用 is_some() 判,**不看 true/false**——显式写 false 也是无效参数(放非 fixed_rate
    // 上应报错),避免 no-op 配置静默通过、降低可审计性。
    let skip_present = skip_if_running.is_some();
    if skip_present && max_in_flight.is_some() {
        bail!("#[scheduled]: skip_if_running 与 max_in_flight 二选一(skip_if_running=true 即 max_in_flight=1)");
    }
    let max_in_flight = match skip_if_running {
        Some(true) => Some(1),
        Some(false) | None => max_in_flight, // 显式 false / 未给 → 不限流(用 max_in_flight 原值)
    };
    if max_in_flight == Some(0) {
        bail!("#[scheduled]: max_in_flight 必须 > 0");
    }
    // 只要显式写了 skip_if_running(含 false)或 max_in_flight,就必须配 fixed_rate。
    if (skip_present || max_in_flight.is_some()) && rate.is_none() {
        bail!("#[scheduled]: max_in_flight/skip_if_running 仅用于 fixed_rate(fixed_delay/一次性不会重叠,无需限流)");
    }

    let init_tok = match initial {
        Some(d) => quote! { ::core::option::Option::Some(#d) },
        None => quote! { ::core::option::Option::None },
    };
    let mif_tok = match max_in_flight {
        Some(m) => quote! { ::core::option::Option::Some(#m) },
        None => quote! { ::core::option::Option::None },
    };
    // fixed_rate 首跑策略(对齐原 fixedRate 默认 initialDelay=0,即启动后立即首跑):
    //   - 显式 initial_delay_ms(含旧 delay_ms)→ 用它;
    //   - 主名 fixed_rate_ms 未给 initial → 默认 Some(0)【立即首跑】;
    //   - 旧别名 every_ms 未给 initial → 保持 None【一个周期后首跑】,兼容历史行为。
    // 故运行时不能只看归一后的 rate,这里按来源决定 FixedRate 的首跑 token。
    let fixed_rate_init_tok = match initial {
        Some(d) => quote! { ::core::option::Option::Some(#d) },
        None if rate_is_every => quote! { ::core::option::Option::None }, // every_ms 旧别名:一个周期后首跑
        None => quote! { ::core::option::Option::Some(0u64) }, // fixed_rate(_ms/_string):立即首跑
    };
    let zone_tok = match zone {
        Some(z) => quote! { ::core::option::Option::Some(#z) },
        None => quote! { ::core::option::Option::None },
    };
    let is_cron = cron.is_some();
    // 展开后的 Schedule 只包含静态常量和毫秒数;所有互斥、单位换算、首跑策略都在宏期收敛,
    // 运行端不再猜测属性来源,只按 enum 语义执行。
    let schedule = if let Some(expr) = cron {
        quote! { #root::Schedule::Cron { expr: #expr, zone: #zone_tok } }
    } else if let Some(r) = rate {
        quote! { #root::Schedule::FixedRate { period_ms: #r, initial_delay_ms: #fixed_rate_init_tok, max_in_flight: #mif_tok } }
    } else if let Some(x) = delay {
        quote! { #root::Schedule::FixedDelay { delay_ms: #x, initial_delay_ms: #init_tok } }
    } else if let Some(d) = initial {
        quote! { #root::Schedule::OneShot { delay_ms: #d } }
    } else {
        bail!("#[scheduled] 必须给调度:cron / fixed_rate_ms(every_ms) / fixed_delay_ms / initial_delay_ms(delay_ms)");
    };

    // 返回值包装(对照"返回值被调度器忽略"语义,**不限制返回类型、不附加任何约束**):
    //   - 标准 Result(见 is_result_return,仅 std/core/anyhow 形态)→ `Err` 打 error 日志(带任务名)、`Ok(_)` 忽略;
    //   - 其它任意返回类型(含 `()`/`u32`/自定义 `xxx::Result`/类型别名)→ `let _ =` 忽略。
    // 注:Err 分支**不格式化错误值**(不打 `{:?}`),以免给 `E` 强加 `Debug` 约束而破坏"返回值忽略";要错误详情请在任务体内自行记录。
    // run 的 async 体须产出 #root::RunOutcome(供 ExecutionRecorder 区分 Success/Failed,d.md):
    //   - 标准 Result → Ok=Success / Err=Failed(并打 error 日志;**不格式化错误值**,不要求 E: Debug);
    //   - 其它任意返回类型 → 一律 Success(非 Result 任务不表达失败)。
    // 用显式 ::core::result::Result::{Ok,Err} 模式:std/core/anyhow::Result 都是 core::result::Result,均可匹配。
    let run_body = if is_result_return(&func.sig.output) {
        quote! {
            match #fn_ident().await {
                ::core::result::Result::Ok(_) => #root::RunOutcome::Success,
                ::core::result::Result::Err(_) => {
                    #root::__private::tracing::error!("[scheduled] 任务 {} 执行返回 Err", #name);
                    #root::RunOutcome::Failed
                }
            }
        }
    } else {
        quote! {
            let _ = #fn_ident().await;
            #root::RunOutcome::Success
        }
    };

    // cluster 模式 token(默认 local;未知值编译期报错)。与触发源正交,任意 schedule 都可配。
    let cluster_tok = match cluster.as_deref() {
        None | Some("local") => quote! { #root::ClusterMode::Local },
        Some("leader") => quote! { #root::ClusterMode::Leader },
        Some(other) => bail!(format!(
            "#[scheduled]: cluster 仅支持 \"local\" 或 \"leader\"(给了 {other:?})"
        )),
    };

    // misfire token(默认 skip;fire_once 仅 cron 有意义,编译期拦非 cron;未知值报错)。
    let misfire_tok = match misfire.as_deref() {
        None | Some("skip") => quote! { #root::MisfirePolicy::Skip },
        Some("fire_once") => {
            if !is_cron {
                bail!("#[scheduled]: misfire=\"fire_once\" 仅用于 cron(fixed_rate/fixed_delay/initial_delay 无应触发时刻)");
            }
            // fire_once 补的是 leader 无主窗口漏触发,必须配 cluster="leader"(d.md)。提前到编译期拦,
            // 而非等运行期 start_scheduled_with 才报(FireLog 依赖仍只能运行期校验)。
            if cluster.as_deref() != Some("leader") {
                bail!("#[scheduled]: misfire=\"fire_once\" 需配合 cluster=\"leader\"(它补的是 leader 无主窗口漏触发;Local 任务每节点各跑、无此问题)");
            }
            quote! { #root::MisfirePolicy::FireOnce }
        }
        Some("claim_only") => {
            // claim_only:每拍 claim 去重(同一 scheduled_at 跨节点只触发一次),不补漏。同 fire_once 需 cron + leader。
            if !is_cron {
                bail!("#[scheduled]: misfire=\"claim_only\" 仅用于 cron(fixed_rate/fixed_delay/initial_delay 无应触发时刻)");
            }
            if cluster.as_deref() != Some("leader") {
                bail!("#[scheduled]: misfire=\"claim_only\" 需配合 cluster=\"leader\"(它做的是同一 scheduled_at 跨节点去重)");
            }
            quote! { #root::MisfirePolicy::ClaimOnly }
        }
        Some(other) => bail!(format!(
            "#[scheduled]: misfire 仅支持 \"skip\" / \"fire_once\" / \"claim_only\"(给了 {other:?})"
        )),
    };

    // group 仅 cluster=leader 有意义(local 任务每节点各跑、不看 gate)→ 编译期拦误配。
    if group.is_some() && cluster.as_deref() != Some("leader") {
        bail!("#[scheduled]: group=... 仅配合 cluster=\"leader\"(它选 group leader gate;local 任务不看 gate)");
    }
    // timeout_ms=0 会让每拍 tokio::time::timeout(0) 立即超时(全部 TimedOut)→ 编译期拦(调度设计约束 #3)。
    if timeout_ms == Some(0) {
        bail!("#[scheduled]: timeout_ms 必须 > 0(0 会让每次执行立即超时)");
    }

    // timeout token(None / Some(ms));`#ms`(u64)经 quote 会带 `u64` 后缀,直接是 Option<u64>。
    let timeout_tok = match timeout_ms {
        Some(ms) => quote! { ::core::option::Option::Some(#ms) },
        None => quote! { ::core::option::Option::None },
    };
    // group token(None / Some("g"))。
    let group_tok = match &group {
        Some(g) => quote! { ::core::option::Option::Some(#g) },
        None => quote! { ::core::option::Option::None },
    };

    // ③ 原 fn + linkme 注册项。
    // repeatable schedules:同一函数可贴多个 `#[scheduled]`。
    // 把注册 static 放进**匿名 `const _: () = {}`**(static 名局部、`const _` 可重复),避免同名 item 冲突(原
    // `static __SCHEDULED_fn` 会 E0428)。栈式属性展开:外层 emit 的 `#func` 仍含内层 `#[scheduled]` 属性 → 再展开 →
    // 每个 `#[scheduled]` 各生成一个匿名 const 注册块,函数本体最终只定义一次。
    // 注册项只保存函数指针、展示名、稳定 id 和调度常量;运行端通过 distributed_slice 读取,不需要运行期扫描模块。
    let expanded = quote! {
        #func

        const _: () = {
            // linkme 经运行时 __private 桥引用;`#[linkme(crate = ...)]` 让 linkme 自身展开也走该路径。
            #[#root::__private::linkme::distributed_slice(#root::SCHEDULED_TASKS)]
            #[linkme(crate = #root::__private::linkme)]
            static __SCHEDULED: #root::ScheduledTask = #root::ScheduledTask {
                name: #name,
                id: #id,
                schedule: #schedule,
                run: || ::std::boxed::Box::pin(async { #run_body }),
                cluster: #cluster_tok,
                misfire: #misfire_tok,
                timeout_ms: #timeout_tok,
                group: #group_tok,
            };
        };
    };
    expanded.into()
}

// ════════════════════════════════════════════════════════════════════════════
// #[EnableScheduling] / #[EnableAsync] —— 在 main 上启动 #[scheduled] 调度
// ════════════════════════════════════════════════════════════════════════════

// 共享实现:在 main 体首部注入 start_scheduled(),让所有 #[scheduled] 在 main 一开始就拉起。
// 错误传播(启动失败不可吞):仅 main 返回 **`anyhow::Result<_>`**(is_anyhow_result_return)时注入 `start_scheduled().await?`
// ——因为 start_scheduled() 返回 anyhow::Result,`?` 仅在 main 错误类型也是 anyhow::Error 时必然可转换(其它如
// `std::result::Result<_, String>` 注入 `?` 会因 `String: !From<anyhow::Error>` 编译失败,过程宏无法静态求解)。
// 其余一切(`()`、无返回、`ExitCode`、裸名/其它 Result)→ `expect` **fail-fast panic**(不进 body),不吞失败。
// `#[Enable*(...)]` 非空参数 → compile_error(本注解无参)。
///
/// # 参数
/// - `attr`: `#[EnableScheduling]`/`#[EnableAsync]` 括号内的 token stream,当前必须为空。
/// - `item`: 被注解的 async main 函数 token stream。
/// - `what`: 当前宏的展示名,用于生成精确的 compile_error 文案。
fn gen_enable(attr: TokenStream, item: TokenStream, what: &str) -> TokenStream {
    if !attr.is_empty() {
        let item2: proc_macro2::TokenStream = item.into();
        let msg = format!("#[{what}] 不接受参数");
        return quote! { ::core::compile_error!(#msg); #item2 }.into();
    }
    let root = match nasa_macro_support::runtime_root("scheduling", "nasched") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    let func = parse_macro_input!(item as ItemFn);
    // 形态校验:必须 async fn(注入的 start_scheduled().await 需要 async 上下文)。否则前置报错,免得编译器只给底层报错。
    if func.sig.asyncness.is_none() {
        let msg = format!("#[{what}] 只能用于 async fn(贴在 #[tokio::main] 之上的 async fn main)");
        return syn::Error::new_spanned(&func.sig.ident, msg)
            .to_compile_error()
            .into();
    }
    let attrs = &func.attrs;
    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;

    // main 返回类型决定错误处理,启动失败不能被吞掉:
    //   - 仅 **`anyhow::Result<_>`**(is_anyhow_result_return)→ `?` 传播(转换必成立);
    //   - 其它一切(`()`、`ExitCode`、裸名/其它路径 Result 如 `Result<_, String>`、无返回)→ **fail-fast `expect`**
    //     (panic、不进 body),避免 logger 未就绪时错误不可见 + 业务带病启动。推荐业务 main 返回 `anyhow::Result<()>`。
    // (用 is_anyhow_result_return 而非 is_result_return:对非 anyhow 的 Result 注 `?` 会因 `E: !From<anyhow::Error>` 底层编译失败。)
    // 注入点固定在用户 main block 最前面;需要先初始化日志或自定义 options 时,不要用本注解,手动调用 start_scheduled_with。
    let start = if is_anyhow_result_return(&sig.output) {
        quote! { #root::start_scheduled().await?; }
    } else {
        quote! { #root::start_scheduled().await.expect("启动 #[scheduled] 调度失败"); }
    };

    quote! {
        #(#attrs)*
        #vis #sig {
            #start
            #block
        }
    }
    .into()
}

/// 贴在 main 上(★ 必须在 `#[tokio::main]` 之【上】),启动时拉起所有 #[scheduled] 定时任务。
/// (#[Async] 是编译期改写、调用即生效,不需要本注解;本注解只负责启动【定时】调度。)
///
/// **错误传播**:仅 main 返回 **`anyhow::Result<_>`** 时,调度启动失败 `?` 传播(start 返回 anyhow::Result,转换必成立);
/// 其它一切返回类型(`()`、无显式返回、`std::process::ExitCode`、裸名 `Result`、`std::result::Result<_, E>` 等)→ **panic
/// fail-fast**(`expect`)。原因:对非 anyhow 错误类型注 `?` 会因 `E: !From<anyhow::Error>` 编译失败,过程宏无法静态求解。
/// 推荐业务 main 返回 `anyhow::Result<()>` 以显式传播启动错误。
///
/// ```ignore
/// #[EnableScheduling]   // ← 在上(先展开,看到的是 async fn main 原体)
/// #[tokio::main]        // ← 在下
/// async fn main() -> anyhow::Result<()> { /* ... */ }
/// ```
///
/// ⚠️ 注入点在 main 体【最前面】(早于 `log::init()` 等),"注册 N 个任务"那几行启动日志可能打不出来(subscriber 未装);
///   任务本身正常注册/运行。想看注册日志可不用本注解,在日志初始化后手动 `scheduling::start_scheduled().await?;`。
///
/// # 参数
///
/// - `attr`: `#[EnableScheduling(...)]` 括号内的 token stream；当前必须为空。
/// - `item`: 被注解的 async main 函数 token stream。
#[proc_macro_attribute]
pub fn EnableScheduling(attr: TokenStream, item: TokenStream) -> TokenStream {
    gen_enable(attr, item, "EnableScheduling")
}

/// `#[EnableScheduling]` 的**兼容别名**(旧名;实际启动的是 #[scheduled] 定时调度,与 #[Async] 无关)。新代码建议用
/// `#[EnableScheduling]`。
///
/// # 参数
///
/// - `attr`: `#[EnableAsync(...)]` 括号内的 token stream；当前必须为空。
/// - `item`: 被注解的 async main 函数 token stream。
#[proc_macro_attribute]
pub fn EnableAsync(attr: TokenStream, item: TokenStream) -> TokenStream {
    gen_enable(attr, item, "EnableAsync")
}
