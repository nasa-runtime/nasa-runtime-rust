//! `LOG_PATTERN` 的解析与渲染，支持
//! `%d{...} %highlight([%-5level]) %magenta([%thread]) %cyan(%logger{30}[%line]) - %msg%n`。
//!
//! **支持以下转换符**(`%d{}`/`%d`/`%level`/`%-5level`/`%thread`/`%logger{N}`/`%line`/`%msg`/`%n`/`%%`/
//! `%highlight`/`%magenta`/`%cyan`);未知 `%xxx` 直接报错,**不静默忽略**。

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// 默认 `LOG_PATTERN`；`LogConfig.pattern=None` 时使用。
pub const DEFAULT_LOG_PATTERN: &str =
    "%d{yyyy-MM-dd HH:mm:ss.SSS} %highlight([%-5level]) %magenta([%thread]) %cyan(%logger{30}[%line]) - %msg%n";

/// 未显式指定格式的 `%d` 使用的默认日期格式。
const DEFAULT_DATE_PATTERN: &str = "yyyy-MM-dd HH:mm:ss.SSS";

// 颜色标记由输出层决定是否转换为 ANSI，文件默认保持纯文本。
const C_RESET: &str = "\x1b[0m";
const C_MAGENTA: &str = "\x1b[35m";
const C_CYAN: &str = "\x1b[36m";

/// 业务作用：返回日志级别颜色码；用于控制台输出时区分严重程度。
///
/// 参数说明：
/// - `level`：日志级别。
///
/// 返回：返回级别对应的静态 ANSI 转义串，不检查目标输出是否允许颜色。
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
    /// 不支持的日期 token(超出 yyyy/MM/dd/HH/mm/ss/SSS 子集)。
    UnsupportedDatePattern(String),
    /// 空 wrapper,如 `%highlight()`。
    EmptyWrapper(&'static str),
}

impl std::fmt::Display for LogPatternError {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// 参数说明：
    /// - `f`：Debug 或 Display 输出使用的标准格式化器。
    ///
    /// 返回：成功输出对应语法分类及其中保存的原字符串；格式化写出失败返回错误，不自动脱敏。
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

/// 编译后的格式 token 序列，可随配置原子替换。
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
    /// 日志级别，支持左对齐与宽度控制。
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
    /// 业务作用：解析 pattern 字符串;未知/非法 token 返 `Err`。
    ///
    /// 参数说明：
    /// - `s`：用户配置的日志输出 pattern,例如时间、级别、线程、logger 和 message 组合。
    ///
    /// 返回：成功返回编译片段；未知转换、括号未闭合、空颜色包装、非法 logger 长度或不支持的日期 token 返回错误。允许空模板，嵌套深度和输出宽度没有额外预算限制。
    pub fn parse(s: &str) -> Result<Self, LogPatternError> {
        let mut p = Parser {
            chars: s.chars().collect(),
            pos: 0,
        };
        let items = p.parse_seq(false)?;
        Ok(Self {
            items,
        })
    }

    /// 业务作用：渲染一条事件(被 `LogFormatter` 调用)。`ansi` 决定 `%highlight/%magenta/%cyan` 是否输出 ANSI 转义。
    ///
    /// 参数说明：
    /// - `ctx`：提供事件字段格式化器及 span 查询能力的 tracing 上下文。
    /// - `writer`：本条日志的输出 writer。
    /// - `event`：提供级别、target、行号和字段的事件。
    /// - `ansi`：是否为颜色包装输出 ANSI 转义。
    ///
    /// 返回：依次写出编译片段，底层写出或字段格式化失败时返回错误，可能已经写出部分内容；不额外补换行。
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

/// 业务作用：按已编译片段渲染日志文本与事件字段。
///
/// 参数说明：
/// - `items`：已编译的日志 pattern 片段列表。
/// - `ctx`：本次格式化、日志或运行阶段的上下文。
/// - `writer`：日志或格式化内容的目标 writer。
/// - `event`：当前 tracing 日志事件,提供字段、target、level 和源码位置。
/// - `ansi`：是否输出 ANSI 颜色控制码。
/// - `level`：日志级别。
///
/// 返回：按片段顺序写出内容；写出或字段格式化错误立即传播，已写出的前缀不回滚。日期使用渲染时的本地时间，线程名缺失时使用 unnamed。
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
            PatternItem::Logger {
                max_len,
            } => write!(
                writer,
                "{}",
                abbreviate_target(event.metadata().target(), *max_len)
            )?,
            PatternItem::Level {
                left_align,
                width,
            } => {
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

/// 业务作用：按输出颜色策略包裹日志子片段。
///
/// 参数说明：
/// - `children`：包装型日志 pattern 的子片段。
/// - `ctx`：本次格式化、日志或运行阶段的上下文。
/// - `writer`：日志或格式化内容的目标 writer。
/// - `event`：当前 tracing 日志事件,供子片段读取 message、target、line 等元数据。
/// - `ansi`：是否输出 ANSI 颜色控制码。
/// - `level`：日志级别。
/// - `color`：日志级别对应的 ANSI 颜色值。
///
/// 返回：禁用 ANSI 时只渲染子片段；启用时先写颜色、渲染后复位。任何写出失败立即返回，子渲染失败时可能尚未写入复位串。
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

/// 业务作用：缩短日志模块路径，除末段外只保留首字符，并把 `::` 转为 `.`。 `%logger{N}` 的 `N` 限制缩写后的长度；省略上限时保留完整缩写，不额外截断。
///
/// 参数说明：
/// - `target`：tracing metadata target,通常是模块路径。
/// - `max_len`：目标名称缩写后的最大长度。
///
/// 返回：返回缩写后的模块名；有限上限按 Unicode 字符数从左侧截取，上限零返回空串，不按字节切断字符。
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
    /// 业务作用：查看模式解析器的下一个字符；用于分支判断但不消耗输入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在未消费字符时返回该字符，否则返回 None，不改变游标。
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    /// 业务作用：读取格式字段的连续十进制宽度。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费连续 ASCII 数字并尝试解析 usize；无数字或数值溢出返回 None，溢出时数字已消费，不回退游标。
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

    /// 业务作用：读取日志转换符名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费连续 ASCII 字母并返回原样名称；当前位置不是字母时返回空串，不消费该字符。
    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_alphabetic()) {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    /// 业务作用：读取 `{...}` 中的参数内容。 返回值不包含右花括号；如果模板里花括号未闭合则返回 `None`,让上层把格式串视为无效片段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前位置不是左花括号时返回 None 且不推进；否则返回首个右花括号前的内容并消费括号，未闭合时推进到末尾并返回 None，不解析嵌套花括号。
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

    /// 业务作用：解析当前日志片段序列，允许颜色包装递归读取子片段。
    ///
    /// 参数说明：
    /// - `stop_at_paren`：解析 pattern 时是否在右括号处停止。
    ///
    /// 返回：成功返回固定文本与转换片段序列；遇右括号且开启停止标志时保留该括号不消费。转换失败传播错误且不恢复游标。
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

    /// 业务作用：在百分号已被消费后解析单个日志转换符及其参数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时消费一个受支持转换并返回对应片段；名称、修饰符、日期或包装非法时返回错误，不恢复游标。宽度解析溢出会得到 None，由后续分支按无宽度处理。
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
                if left_align {
                    "-"
                } else {
                    ""
                },
                width.map(|w| w.to_string()).unwrap_or_default()
            );
            return Err(LogPatternError::UnknownConversion(mods));
        }
        if word == "level" {
            return Ok(PatternItem::Level {
                left_align,
                width,
            });
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
                Ok(PatternItem::Logger {
                    max_len,
                })
            }
            "highlight" => self.parse_wrapper("highlight").map(PatternItem::Highlight),
            "magenta" => self.parse_wrapper("magenta").map(PatternItem::Magenta),
            "cyan" => self.parse_wrapper("cyan").map(PatternItem::Cyan),
            other => Err(LogPatternError::UnknownConversion(other.to_string())),
        }
    }

    /// 业务作用：解析颜色转换符的括号子模板。
    ///
    /// 参数说明：
    /// - `name`：用于定位颜色包装错误的静态名称。
    ///
    /// 返回：存在左括号、可解析的非空子序列和右括号时返回子片段并消费括号；否则返回包装或子转换错误，不回退游标。
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

/// 业务作用：把支持的日期格式字母组映射为 chrono 格式串。
///
/// 参数说明：
/// - `s`：日期转换符大括号内的原始日期模板。
///
/// 返回：支持的连续 ASCII 日期字母组转换为 chrono 占位符，其余字符原样保留；遇到不支持的 ASCII 字母组返回错误，不处理日期引号转义。
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

/// 业务作用：映射一个受支持的日期格式字母组。
///
/// 参数说明：
/// - `c`：日期 pattern 中当前解析到的格式字符。
/// - `len`：同一格式字符连续出现的次数。
///
/// 返回：仅 yyyy、MM、dd、HH、mm、ss 和 SSS 返回对应 chrono 占位符；其它字符或重复次数返回 None。
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
