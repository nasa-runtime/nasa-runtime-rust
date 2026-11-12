use std::panic::PanicHookInfo;

use crate::report::write_stderr;

/// 进程级 panic 标记的总字节上限，包含固定前缀、位置和换行。
pub(crate) const PANIC_MARKER_MAX_BYTES: usize = 512;

/// 业务作用：安装只输出受控代码位置的进程级 panic hook。
///
/// 该 hook 不读取 payload、线程名和回溯，也不调用旧 hook，从源头阻止未脱敏业务文本先于 Runner 泄漏。
///
/// 执行应用生命周期的每个入口都必须先调用本函数——`catch_unwind` 发生在 panic hook 之后，
/// 只捕获错误无法阻止默认 hook 先把业务 payload 写到 stderr。多入口重复调用由 `Once` 收敛，
/// 保证首次安装最早生效且不重复替换。
///
/// 参数说明：无。
///
/// 返回：首次调用替换进程 hook，后续调用不重复安装；不停止 panic 展开或 abort。
/// hook 没有自动恢复步骤，但进程内其他代码仍可再次替换它，应用须统一管理此进程级入口。
pub(crate) fn install_process_panic_hook() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(install_hook);
}

/// 业务作用：执行一次实际的 hook 替换。
///
/// 参数说明: 无。
///
/// 返回：无返回值；替换后进程内任何 panic 只输出受控位置标记。
fn install_hook() {
    std::panic::set_hook(Box::new(|info: &PanicHookInfo<'_>| {
        let marker = match info.location() {
            Some(location) => panic_marker(location.file(), location.line(), location.column()),
            None => panic_marker("<unknown>", 0, 0),
        };
        write_stderr(&marker);
    }));
}

/// 业务作用：构造有界单行 panic 位置标记，不包含 panic payload、线程名或回溯。
///
/// 参数说明：
///
/// - `file`：panic 位置对应的源文件路径。
/// - `line`：源文件行号。
/// - `column`：源文件列号。
///
/// 返回：含固定前缀、压缩路径、行列号和一个末尾换行的字符串，总长度不超过 512 字节。
/// 路径裁剪保留完整 UTF-8 字符，不读取文件或验证源码位置存在。
fn panic_marker(file: &str, line: u32, column: u32) -> String {
    let file = compact_source_path(file);
    let suffix = format!(":{line}:{column}\n");
    let prefix = "application panic observed at ";
    let available = PANIC_MARKER_MAX_BYTES
        .saturating_sub(prefix.len())
        .saturating_sub(suffix.len());
    let file = tail_at_char_boundary(&file, available);
    format!("{prefix}{file}{suffix}")
}

/// 业务作用：清洗控制字符并移除绝对路径前缀，只保留最多三个源码路径尾段。
///
/// 参数说明：
///
/// - `file`：编译器提供的源文件位置；该值可能包含平台分隔符或控制字符。
///
/// 返回：控制字符替换为问号，去除根分隔符和独立 Windows 盘符段，以斜杠连接最多三个非空尾段。
/// 空输入或仅根、盘符的输入返回空串；不解析相对目录，也不保证剩余目录名不含业务信息。
fn compact_source_path(file: &str) -> String {
    let sanitized = file
        .chars()
        .map(|character| {
            if character.is_control() {
                '?'
            } else if character == '\\' {
                '/'
            } else {
                character
            }
        })
        .collect::<String>();
    let segments = sanitized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    // 短绝对路径同样需要去除根和盘符，不能仅在段数超限时才压缩来源身份。
    let segments = if segments.first().is_some_and(|segment| {
        let bytes = segment.as_bytes();
        bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
    }) {
        &segments[1..]
    } else {
        &segments[..]
    };
    segments[segments.len().saturating_sub(3)..].join("/")
}

/// 业务作用：保留字符串末尾不超过指定字节数的完整 UTF-8 字符。
///
/// 保留末尾可以让超长路径仍包含文件名，而不是只留下无定位价值的目录前缀。
///
/// 参数说明：
///
/// - `value`：已经清洗的源文件路径。
/// - `max_bytes`：该路径在完整标记中可占用的最大字节数。
///
/// 返回：原字符串末尾的借用切片，长度不超过上限且不截断 UTF-8 字符；零上限返回空切片。
/// 本函数不分配、不清洗内容，也不保证截断点恰好是路径段边界。
fn tail_at_char_boundary(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut start = value.len().saturating_sub(max_bytes);
    while !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}
