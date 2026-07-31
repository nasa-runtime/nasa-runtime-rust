use std::io::Write;

use crate::ApplicationError;

/// 单条同步诊断允许写出的最大字节数，防止异常文本无限放大 stderr。
const REPORT_MAX_BYTES: usize = 2_048;

/// 以 best-effort 方式直接写入 stderr，不依赖尚未启动或已经停止的日志组件。
///
/// # 参数
///
/// - `message`：已经完成脱敏和长度限制的固定诊断文本。
pub(crate) fn write_stderr(message: &str) {
    let _ = std::io::stderr().lock().write_all(message.as_bytes());
}

/// 输出同步预检阶段的唯一失败报告。
///
/// # 参数
///
/// - `error`：本地配置读取、设置校验或 runtime 创建过程中产生的错误。
pub(crate) fn report_preflight(error: &ApplicationError) {
    let message = bounded(&format!(
        "application preflight failed: {}\n",
        redact(&error_chain(error))
    ));
    write_stderr(&message);
}

/// 输出异步生命周期阶段的唯一主失败报告。
///
/// # 参数
///
/// - `error`：Runner 已经归类、即将进入反向清理的主错误。
pub(crate) fn report_runtime(error: &ApplicationError) {
    let message = bounded(&format!(
        "application runtime failed: {}\n",
        redact(&error_chain(error))
    ));
    write_stderr(&message);
}

/// 输出不会覆盖首次终态的次要清理失败。
///
/// # 参数
///
/// - `error`：active stack 某个清理步骤产生的次要错误。
pub(crate) fn report_shutdown(error: &ApplicationError) {
    let message = bounded(&format!(
        "application shutdown warning: {}\n",
        redact(&error_chain(error))
    ));
    write_stderr(&message);
}

/// 展开框架错误摘要及其完整底层链，供统一脱敏管道消费。
///
/// `ApplicationError` 的 `Display` 只写组件、阶段和稳定摘要；`with_source` 保存的根因
/// （bind/DB/Redis/config 等）挂在 `#[source]` 上，必须显式沿 `std::error::Error::source`
/// 逐级拼接，否则运维只能看到泛化摘要而丢失真正的失败原因。拼接后的整段文本再交给
/// `redact` 脱敏、`bounded` 截断。
///
/// # 参数
///
/// - `error`：框架层主错误或次要清理错误。
pub(crate) fn error_chain(error: &ApplicationError) -> String {
    let mut output = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        output.push_str(": ");
        output.push_str(&cause.to_string());
        source = cause.source();
    }
    output
}

/// 对常见 URI 凭据和敏感键赋值执行保守替换。
///
/// # 参数
///
/// - `input`：可能来自配置加载器或错误链的未信任文本。
pub(crate) fn redact(input: &str) -> String {
    let mut output = redact_uri_userinfo(input);
    for key in [
        "password",
        "passwd",
        "secret",
        "token",
        "access_key",
        "private_key",
    ] {
        output = redact_assignment(&output, key);
    }
    output
}

/// 隐去 URI authority 中 `@` 之前的 userinfo，保留 scheme 和 host 便于定位。
///
/// # 参数
///
/// - `input`：可能包含一个或多个 URI 的文本。
fn redact_uri_userinfo(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(scheme_index) = rest.find("://") {
        let authority_start = scheme_index + 3;
        output.push_str(&rest[..authority_start]);
        let authority = &rest[authority_start..];
        let authority_end = authority
            .find(|character: char| character == '/' || character.is_whitespace())
            .unwrap_or(authority.len());
        let prefix = &authority[..authority_end];
        if let Some(at_index) = prefix.rfind('@') {
            output.push_str("***@");
            output.push_str(&authority[at_index + 1..authority_end]);
        } else {
            output.push_str(prefix);
        }
        rest = &authority[authority_end..];
    }
    output.push_str(rest);
    output
}

/// 隐去一个常见敏感键后紧邻的标量值。
///
/// # 参数
///
/// - `input`：待处理文本。
/// - `key`：以 ASCII 大小写不敏感方式匹配的敏感键名。
fn redact_assignment(input: &str, key: &str) -> String {
    let mut output = input.to_owned();
    let mut search_from = 0;
    loop {
        let lower = output.to_ascii_lowercase();
        let Some(relative) = lower[search_from..].find(key) else {
            break;
        };
        let key_start = search_from + relative;
        let after_key = key_start + key.len();
        let bytes = output.as_bytes();
        let mut value_start = after_key;
        while value_start < bytes.len() && matches!(bytes[value_start], b' ' | b'\t' | b'\'' | b'"')
        {
            value_start += 1;
        }
        if value_start >= bytes.len() || !matches!(bytes[value_start], b'=' | b':') {
            search_from = after_key;
            continue;
        }
        value_start += 1;
        while value_start < bytes.len() && matches!(bytes[value_start], b' ' | b'\t' | b'\'' | b'"')
        {
            value_start += 1;
        }
        let mut value_end = value_start;
        while value_end < bytes.len()
            && !matches!(
                bytes[value_end],
                b' ' | b'\t' | b'\r' | b'\n' | b',' | b'}' | b']' | b'&' | b'\'' | b'"'
            )
        {
            value_end += 1;
        }
        if value_end == value_start {
            search_from = after_key;
            continue;
        }
        output.replace_range(value_start..value_end, "***");
        search_from = value_start + 3;
    }
    output
}

/// 在 UTF-8 字符边界内限制单条诊断长度。
///
/// # 参数
///
/// - `input`：已完成脱敏、准备写入 stderr 的文本。
fn bounded(input: &str) -> String {
    if input.len() <= REPORT_MAX_BYTES {
        return input.to_owned();
    }
    let mut end = REPORT_MAX_BYTES.saturating_sub(1);
    while !input.is_char_boundary(end) {
        end -= 1;
    }
    let mut output = input[..end].to_owned();
    output.push('\n');
    output
}
