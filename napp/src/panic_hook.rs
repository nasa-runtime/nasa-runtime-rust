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
/// # 参数
///
/// 本函数无参数；调用后 hook 持续生效到进程退出。
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

/// 业务作用：构造有界、单行且不含绝对构建路径的 panic 标记。
///
/// # 参数
///
/// - `file`：panic 位置对应的源文件路径。
/// - `line`：源文件行号。
/// - `column`：源文件列号。
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

/// 业务作用：清洗控制字符并把绝对路径压缩成末尾三个路径段。
///
/// # 参数
///
/// - `file`：编译器提供的源文件位置；该值可能包含平台分隔符或控制字符。
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
    if segments.len() <= 3 {
        sanitized
    } else {
        segments[segments.len() - 3..].join("/")
    }
}

/// 业务作用：保留字符串末尾不超过指定字节数的完整 UTF-8 字符。
///
/// 保留末尾可以让超长路径仍包含文件名，而不是只留下无定位价值的目录前缀。
///
/// # 参数
///
/// - `value`：已经清洗的源文件路径。
/// - `max_bytes`：该路径在完整标记中可占用的最大字节数。
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
