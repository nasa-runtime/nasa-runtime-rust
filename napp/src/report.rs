use std::io::Write;

use crate::ApplicationError;

/// 一次 active stack 清理的有界结构化摘要。
pub(crate) struct ShutdownSummary {
    /// 首次停机原因的稳定分类。
    pub(crate) reason: &'static str,
    /// 清理开始时栈内的步骤数。
    pub(crate) planned_steps: usize,
    /// 实际取得执行机会的步骤数。
    pub(crate) attempted_steps: usize,
    /// 全局 deadline 耗尽后未执行的步骤数。
    pub(crate) abandoned_steps: usize,
    /// 组件 action 步骤数。
    pub(crate) component_actions: usize,
    /// initializer action 步骤数。
    pub(crate) initializer_actions: usize,
    /// Supervisor 全局任务排空门步骤数。
    pub(crate) task_gates: usize,
    /// 业务资源清理步骤数。
    pub(crate) business_resources: usize,
    /// 组件资源清理步骤数。
    pub(crate) component_resources: usize,
    /// initializer 资源清理步骤数。
    pub(crate) initializer_resources: usize,
    /// 清理链累计的失败数。
    pub(crate) failures: usize,
    /// 是否进入过任务强制 abort 路径。
    pub(crate) task_abort_attempted: bool,
    /// 是否因共享停机预算耗尽而留下失败或未执行步骤。
    pub(crate) deadline_exhausted: bool,
    /// 完整 active stack 清理耗时。
    pub(crate) duration: std::time::Duration,
}

/// 单条同步诊断允许写出的最大字节数，防止异常文本无限放大 stderr。
const REPORT_MAX_BYTES: usize = 2_048;

/// 业务作用：以 best-effort 方式直接写入 stderr，不依赖尚未启动或已经停止的日志组件。
///
/// # 参数
///
/// - `message`：已经完成脱敏和长度限制的固定诊断文本。
pub(crate) fn write_stderr(message: &str) {
    let _ = std::io::stderr().lock().write_all(message.as_bytes());
}

/// 业务作用：输出同步预检阶段的唯一失败报告。
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

/// 业务作用：输出异步生命周期阶段的唯一主失败报告。
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

/// 业务作用：输出不会覆盖首次终态的次要清理失败。
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

/// 业务作用：在日志组件可能已经关闭后，仍以有界、可机器解析的单行文本报告成功或失败的停机收口。
///
/// 参数说明：
/// - `summary`：只含稳定分类、计数、布尔结果与耗时的停机摘要，不携带业务数据或配置值。
///
/// 返回：无返回值；stderr 写入采用 best-effort，不能反向改变已经确定的退出语义。
pub(crate) fn report_shutdown_summary(summary: &ShutdownSummary) {
    let outcome = if summary.abandoned_steps > 0 || summary.deadline_exhausted {
        "deadline-exhausted"
    } else if summary.failures > 0 {
        "completed-with-failures"
    } else {
        "completed"
    };
    let message = bounded(&format!(
        "application shutdown summary: outcome={outcome} reason={} planned_steps={} attempted_steps={} abandoned_steps={} component_actions={} initializer_actions={} task_gates={} business_resources={} component_resources={} initializer_resources={} failures={} task_abort_attempted={} deadline_exhausted={} duration_ms={}\n",
        summary.reason,
        summary.planned_steps,
        summary.attempted_steps,
        summary.abandoned_steps,
        summary.component_actions,
        summary.initializer_actions,
        summary.task_gates,
        summary.business_resources,
        summary.component_resources,
        summary.initializer_resources,
        summary.failures,
        summary.task_abort_attempted,
        summary.deadline_exhausted,
        summary.duration.as_millis(),
    ));
    write_stderr(&message);
}

/// 业务作用：展开框架错误摘要及其完整底层链，供统一脱敏管道消费。
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

/// 业务作用：对常见 URI 凭据和敏感键赋值执行保守替换。
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

/// 业务作用：隐去 URI authority 中 `@` 之前的 userinfo，保留 scheme 和 host 便于定位。
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

/// 业务作用：隐去一个常见敏感键后紧邻的标量值。
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

/// 业务作用：在 UTF-8 字符边界内限制单条诊断长度。
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
