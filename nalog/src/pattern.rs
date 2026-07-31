//! logback `LOG_PATTERN` 子集:解析(compile)+ 渲染(render),对照 原实现 logback-原框架.xml 的
//! `%d{...} %highlight([%-5level]) %magenta([%thread]) %cyan(%logger{30}[%line]) - %msg%n`。
//!
//! **仅实现当前 XML 用到的子集**(`%d{}`/`%d`/`%level`/`%-5level`/`%thread`/`%logger{N}`/`%line`/`%msg`/`%n`/`%%`/
//! `%highlight`/`%magenta`/`%cyan`);未知 `%xxx` 直接报错,**不静默忽略**。

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// 默认 pattern(逐字对照 原工具包 logback-原框架.xml 的 `LOG_PATTERN`)。`LogConfig.pattern=None` 时使用。
pub const DEFAULT_LOG_PATTERN: &str =
    "%d{yyyy-MM-dd HH:mm:ss.SSS} %highlight([%-5level]) %magenta([%thread]) %cyan(%logger{30}[%line]) - %msg%n";

/// 默认日期格式(对照 XML;bare `%d` 用)。
const DEFAULT_DATE_PATTERN: &str = "yyyy-MM-dd HH:mm:ss.SSS";

// ── 颜色(对照 logback %highlight/%magenta/%cyan)──
const C_RESET: &str = "\x1b[0m";
const C_MAGENTA: &str = "\x1b[35m";
const C_CYAN: &str = "\x1b[36m";

/// 返回日志级别颜色码；用于控制台输出时区分严重程度。
///
/// # 参数
/// - `level`: 日志级别。
fn level_color(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "\x1b[1;31m", // 粗红
        Level::WARN => "\x1b[33m",    // 黄
        Level::INFO => "\x1b[34m",    // 蓝
        Level::DEBUG => "\x1b[36m",   // 青
        Level::TRACE => "\x1b[2m",    // 暗
    }
}

/// pattern 解析错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogPatternError {
    /// 未知转换符 `%xxx`。
    UnknownConversion(String),
    /// `%d{` 未闭合。
    UnclosedDate,
    /// `%highlight(`/`%magenta(`/`%cyan(` 未闭合。
    UnclosedWrapper(&'static str),
    /// `%logger{...}` 内非数字或未闭合。
    InvalidLoggerLength(String),
    /// 不支持的 原实现 日期 token(超出 yyyy/MM/dd/HH/mm/ss/SSS 子集)。
    UnsupportedDatePattern(String),
    /// 空 wrapper,如 `%highlight()`。
    EmptyWrapper(&'static str),
}

impl std::fmt::Display for LogPatternError {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::UnknownConversion(s) => write!(f, "unknown conversion: %{s}"),
            Self::UnclosedDate => write!(f, "unclosed %d{{...}}"),
            Self::UnclosedWrapper(w) => write!(f, "unclosed %{w}(...)"),
            Self::InvalidLoggerLength(s) => write!(f, "invalid %logger length: {{{s}}}"),
            Self::UnsupportedDatePattern(s) => write!(f, "unsupported date token: {s}"),
            Self::EmptyWrapper(w) => write!(f, "empty %{w}()"),
        }
    }
}

impl std::error::Error for LogPatternError {}

/// 编译后的 pattern(token 序列),可热替换。对照 logback 编码后的 encoder。
#[derive(Debug, Clone)]
pub struct CompiledLogPattern {
    items: Vec<PatternItem>,
}

/// 日志 pattern 编译后的最小渲染单元。
///
/// 运行时只遍历该序列输出日志行,避免每条日志重复解析 `%d/%p/%logger` 等占位符。
#[derive(Debug, Clone)]
enum PatternItem {
    /// 原样写入日志行的固定文本。
    Literal(String),
    /// 日期时间片段,内容已映射为 chrono 格式串。
    Date(String),
    /// 日志级别,支持 logback 风格的左对齐和宽度控制。
    Level {
        /// 是否使用左对齐填充级别文本。
        left_align: bool,
        /// 级别字段的最小显示宽度。
        width: Option<usize>,
    },
    /// 当前线程名。
    Thread,
    /// logger 名称,可按配置截断。
    Logger {
        /// logger 名称最大保留长度。
        max_len: Option<usize>,
    },
    /// 调用点行号。
    Line,
    /// 日志消息正文。
    Message,
    /// 换行符。
    Newline,
    /// 按日志级别自动着色的嵌套 pattern。
    Highlight(Vec<PatternItem>),
    /// 使用品红色渲染的嵌套 pattern。
    Magenta(Vec<PatternItem>),
    /// 使用青色渲染的嵌套 pattern。
    Cyan(Vec<PatternItem>),
}

impl CompiledLogPattern {
    /// 解析 pattern 字符串;未知/非法 token 返 `Err`。
    ///
    /// # 参数
    /// - `s`: 用户配置的日志输出 pattern,例如时间、级别、线程、logger 和 message 组合。
    pub fn parse(s: &str) -> Result<Self, LogPatternError> {
        let mut p = Parser {
            chars: s.chars().collect(),
            pos: 0,
        };
        let items = p.parse_seq(false)?;
        Ok(Self { items })
    }

    /// 渲染一条事件(被 `LogFormatter` 调用)。`ansi` 决定 `%highlight/%magenta/%cyan` 是否输出 ANSI 转义。
    pub(crate) fn render<S, N>(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
        ansi: bool,
    ) -> std::fmt::Result
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        let level = *event.metadata().level();
        render_items(&self.items, ctx, &mut writer, event, ansi, &level)
    }
}

/// 渲染 render items 内容；用于生成最终输出文本。
///
/// # 参数
/// - `items`: 已编译的日志 pattern 片段列表。
/// - `ctx`: 本次格式化、日志或运行阶段的上下文。
/// - `writer`: 日志或格式化内容的目标 writer。
/// - `event`: 当前 tracing 日志事件,提供字段、target、level 和源码位置。
/// - `ansi`: 是否输出 ANSI 颜色控制码。
/// - `level`: 日志级别。
fn render_items<S, N>(
    items: &[PatternItem],
    ctx: &FmtContext<'_, S, N>,
    writer: &mut Writer<'_>,
    event: &Event<'_>,
    ansi: bool,
    level: &Level,
) -> std::fmt::Result
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    for it in items {
        match it {
            PatternItem::Literal(s) => writer.write_str(s)?,
            PatternItem::Newline => writeln!(writer)?,
            PatternItem::Message => ctx.field_format().format_fields(writer.by_ref(), event)?,
            PatternItem::Date(fmt) => write!(writer, "{}", chrono::Local::now().format(fmt))?,
            PatternItem::Thread => {
                let t = std::thread::current();
                write!(writer, "{}", t.name().unwrap_or("unnamed"))?
            }
            PatternItem::Line => write!(writer, "{}", event.metadata().line().unwrap_or(0))?,
            PatternItem::Logger { max_len } => write!(
                writer,
                "{}",
                abbreviate_target(event.metadata().target(), *max_len)
            )?,
            PatternItem::Level { left_align, width } => {
                let s = level.to_string();
                match width {
                    Some(w) if *left_align => write!(writer, "{s:<w$}", w = *w)?,
                    Some(w) => write!(writer, "{s:>w$}", w = *w)?,
                    None => writer.write_str(&s)?,
                }
            }
            PatternItem::Highlight(ch) => {
                render_wrapped(ch, ctx, writer, event, ansi, level, level_color(level))?
            }
            PatternItem::Magenta(ch) => {
                render_wrapped(ch, ctx, writer, event, ansi, level, C_MAGENTA)?
            }
            PatternItem::Cyan(ch) => render_wrapped(ch, ctx, writer, event, ansi, level, C_CYAN)?,
        }
    }
    Ok(())
}

/// 渲染 render wrapped 内容；用于生成最终输出文本。
///
/// # 参数
/// - `children`: 包装型日志 pattern 的子片段。
/// - `ctx`: 本次格式化、日志或运行阶段的上下文。
/// - `writer`: 日志或格式化内容的目标 writer。
/// - `event`: 当前 tracing 日志事件,供子片段读取 message、target、line 等元数据。
/// - `ansi`: 是否输出 ANSI 颜色控制码。
/// - `level`: 日志级别。
/// - `color`: 日志级别对应的 ANSI 颜色值。
#[allow(clippy::too_many_arguments)]
fn render_wrapped<S, N>(
    children: &[PatternItem],
    ctx: &FmtContext<'_, S, N>,
    writer: &mut Writer<'_>,
    event: &Event<'_>,
    ansi: bool,
    level: &Level,
    color: &str,
) -> std::fmt::Result
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    if ansi {
        writer.write_str(color)?;
        render_items(children, ctx, writer, event, ansi, level)?;
        writer.write_str(C_RESET)
    } else {
        render_items(children, ctx, writer, event, ansi, level)
    }
}

/// 原实现 风格包名缩写(对照 `%logger{N}`):除末段外只留首字符、`::`→`.`;若仍超 `max_len` 则**硬截断**到 `max_len`
/// (保证 `%logger{N}` 的 `N` 生效;不强求 100% 复刻 logback,但保持原默认输出 + 长度上限)。
///
/// # 参数
/// - `target`: tracing metadata target,通常是模块路径。
/// - `max_len`: 目标名称缩写后的最大长度。
fn abbreviate_target(target: &str, max_len: Option<usize>) -> String {
    let parts: Vec<&str> = target.split("::").collect();
    let mut out = if parts.len() <= 1 {
        target.to_string()
    } else {
        let mut s = String::with_capacity(target.len());
        for (i, p) in parts.iter().enumerate() {
            if i + 1 < parts.len() {
                if let Some(c) = p.chars().next() {
                    s.push(c);
                }
                s.push('.');
            } else {
                s.push_str(p);
            }
        }
        s
    };
    if let Some(max) = max_len {
        if out.chars().count() > max {
            out = out.chars().take(max).collect();
        }
    }
    out
}

// ── 解析器 ──

/// 保存解析器游标和输入；用于逐步解析文本内容。
struct Parser {
    chars: Vec<char>,
    pos: usize,
}

impl Parser {
    /// 查看模式解析器的下一个字符；用于分支判断但不消耗输入。
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    /// 解析 read number 输入；用于把文本或语法节点转换为内部结构。
    fn read_number(&mut self) -> Option<usize> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == start {
            None
        } else {
            self.chars[start..self.pos]
                .iter()
                .collect::<String>()
                .parse()
                .ok()
        }
    }

    /// 解析 read ident 输入；用于把文本或语法节点转换为内部结构。
    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_alphabetic()) {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    /// 读取 `{...}` 中的参数内容。
    ///
    /// 返回值不包含右花括号；如果模板里花括号未闭合则返回 `None`,让上层把格式串视为无效片段。
    fn read_braced(&mut self) -> Option<String> {
        if self.peek() != Some('{') {
            return None;
        }
        self.pos += 1;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == '}' {
                let inner: String = self.chars[start..self.pos].iter().collect();
                self.pos += 1; // 消费 '}'
                return Some(inner);
            }
            self.pos += 1;
        }
        None // 未闭合
    }

    /// 解析 parse seq 输入；用于把文本或语法节点转换为内部结构。
    ///
    /// # 参数
    /// - `stop_at_paren`: 解析 pattern 时是否在右括号处停止。
    fn parse_seq(&mut self, stop_at_paren: bool) -> Result<Vec<PatternItem>, LogPatternError> {
        let mut items = Vec::new();
        let mut lit = String::new();
        while let Some(c) = self.peek() {
            if stop_at_paren && c == ')' {
                break; // 留给 wrapper 调用方消费
            }
            if c == '%' {
                if !lit.is_empty() {
                    items.push(PatternItem::Literal(std::mem::take(&mut lit)));
                }
                self.pos += 1;
                items.push(self.parse_conversion()?);
            } else {
                lit.push(c);
                self.pos += 1;
            }
        }
        if !lit.is_empty() {
            items.push(PatternItem::Literal(lit));
        }
        Ok(items)
    }

    /// 解析 parse conversion 输入；用于把文本或语法节点转换为内部结构。
    fn parse_conversion(&mut self) -> Result<PatternItem, LogPatternError> {
        let Some(c) = self.peek() else {
            return Err(LogPatternError::UnknownConversion(String::new()));
        };
        if c == '%' {
            self.pos += 1;
            return Ok(PatternItem::Literal("%".to_string()));
        }
        // 格式修饰符(仅 level 支持):`-`? digits?
        let left_align = c == '-';
        if left_align {
            self.pos += 1;
        }
        let width = self.read_number();
        let word = self.read_ident();
        if word.is_empty() {
            let mods = format!(
                "{}{}",
                if left_align { "-" } else { "" },
                width.map(|w| w.to_string()).unwrap_or_default()
            );
            return Err(LogPatternError::UnknownConversion(mods));
        }
        if word == "level" {
            return Ok(PatternItem::Level { left_align, width });
        }
        if left_align || width.is_some() {
            // 修饰符只对 level 有意义
            return Err(LogPatternError::UnknownConversion(format!(
                "modifier+{word}"
            )));
        }
        match word.as_str() {
            "d" => {
                let legacy = match self.peek() {
                    Some('{') => self.read_braced().ok_or(LogPatternError::UnclosedDate)?,
                    _ => DEFAULT_DATE_PATTERN.to_string(),
                };
                Ok(PatternItem::Date(map_legacy_date(&legacy)?))
            }
            "thread" => Ok(PatternItem::Thread),
            "line" => Ok(PatternItem::Line),
            "msg" => Ok(PatternItem::Message),
            "n" => Ok(PatternItem::Newline),
            "logger" => {
                let max_len = match self.peek() {
                    Some('{') => {
                        let inner = self
                            .read_braced()
                            .ok_or_else(|| LogPatternError::InvalidLoggerLength(String::new()))?;
                        let n = inner
                            .trim()
                            .parse::<usize>()
                            .map_err(|_| LogPatternError::InvalidLoggerLength(inner.clone()))?;
                        Some(n)
                    }
                    _ => None,
                };
                Ok(PatternItem::Logger { max_len })
            }
            "highlight" => self.parse_wrapper("highlight").map(PatternItem::Highlight),
            "magenta" => self.parse_wrapper("magenta").map(PatternItem::Magenta),
            "cyan" => self.parse_wrapper("cyan").map(PatternItem::Cyan),
            other => Err(LogPatternError::UnknownConversion(other.to_string())),
        }
    }

    /// 解析 parse wrapper 输入；用于把文本或语法节点转换为内部结构。
    ///
    /// # 参数
    /// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
    fn parse_wrapper(&mut self, name: &'static str) -> Result<Vec<PatternItem>, LogPatternError> {
        if self.peek() != Some('(') {
            return Err(LogPatternError::UnclosedWrapper(name));
        }
        self.pos += 1; // 消费 '('
        let children = self.parse_seq(true)?;
        if self.peek() != Some(')') {
            return Err(LogPatternError::UnclosedWrapper(name));
        }
        self.pos += 1; // 消费 ')'
        if children.is_empty() {
            return Err(LogPatternError::EmptyWrapper(name));
        }
        Ok(children)
    }
}

/// 原实现 日期 pattern → chrono 格式串(仅 yyyy/MM/dd/HH/mm/ss/SSS 子集;其余字母 token 报错)。
/// `SSS → %3f`(3 位毫秒不带前导点),使 `ss.SSS` → `%S.%3f` 渲染为 `07.298`,与旧硬编码 `%S%.3f` 一致。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn map_legacy_date(s: &str) -> Result<String, LogPatternError> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_ascii_alphabetic() {
            let start = i;
            while i < chars.len() && chars[i] == c {
                i += 1;
            }
            let len = i - start;
            let mapped = map_date_token(c, len).ok_or_else(|| {
                LogPatternError::UnsupportedDatePattern(c.to_string().repeat(len))
            })?;
            out.push_str(mapped);
        } else {
            out.push(c); // 字面量(空格 / - / : / .)直接透传
            i += 1;
        }
    }
    Ok(out)
}

/// 映射 date token 配置；用于转换为运行时需要的类型。
///
/// # 参数
/// - `c`: 日期 pattern 中当前解析到的格式字符。
/// - `len`: 同一格式字符连续出现的次数。
fn map_date_token(c: char, len: usize) -> Option<&'static str> {
    match (c, len) {
        ('y', 4) => Some("%Y"),
        ('M', 2) => Some("%m"),
        ('d', 2) => Some("%d"),
        ('H', 2) => Some("%H"),
        ('m', 2) => Some("%M"),
        ('s', 2) => Some("%S"),
        ('S', 3) => Some("%3f"),
        _ => None,
    }
}
