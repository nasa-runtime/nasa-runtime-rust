use std::io::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};

use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

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
    /// UserHook 成功登记的业务停机任务数。
    pub(crate) business_shutdown_registered: usize,
    /// 实际取得 poll 机会的业务停机任务数。
    pub(crate) business_shutdown_attempted: usize,
    /// 正常完成的业务停机任务数。
    pub(crate) business_shutdown_completed: usize,
    /// 返回错误的业务停机任务数。
    pub(crate) business_shutdown_failed: usize,
    /// 超出公平子预算的业务停机任务数。
    pub(crate) business_shutdown_timed_out: usize,
    /// panic 后被隔离的业务停机任务数。
    pub(crate) business_shutdown_panicked: usize,
    /// 因业务任务组预算耗尽而未取得 poll 机会的任务数。
    pub(crate) business_shutdown_abandoned: usize,
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
/// 错误展开先独立限制内存，再由统一通道脱敏并限制最终输出长度。
const ERROR_CHAIN_MAX_BYTES: usize = 16_384;
const ERROR_CHAIN_MAX_DEPTH: usize = 32;

struct ErrorText {
    text: String,
    limit: usize,
    exceeded: bool,
}

impl std::fmt::Write for ErrorText {
    /// 业务作用：在错误正文生成期间限制接收字节数，防止先无界分配再截断。
    ///
    /// 参数说明：
    /// - `text`：业务 Display 提供的下一个文本片段。
    ///
    /// 返回：预算内完整追加；超限时拒绝整个片段并要求格式化器停止，调用方随后丢弃不完整正文。
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if self.exceeded || text.len() > self.limit.saturating_sub(self.text.len()) {
            self.exceeded = true;
            return Err(std::fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

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
/// 参数说明：
/// - `error`：本地配置读取、设置校验或 runtime 创建过程中产生的错误。
///
/// 返回：以 best-effort 写出一条有界单行诊断，不改变预检失败结果。
pub(crate) fn report_preflight(error: &ApplicationError) {
    let message = error_report_line("application preflight failed: ", error);
    write_stderr(&message);
}

/// 业务作用：输出异步生命周期阶段的唯一主失败报告。
///
/// 参数说明：
/// - `error`：Runner 已经归类、即将进入反向清理的主错误。
///
/// 返回：以 best-effort 写出一条有界单行诊断，不改变首次主失败。
pub(crate) fn report_runtime(error: &ApplicationError) {
    let message = error_report_line("application runtime failed: ", error);
    write_stderr(&message);
}

/// 业务作用：输出不会覆盖首次终态的次要清理失败。
///
/// 参数说明：
/// - `error`：active stack 某个清理步骤产生的次要错误。
///
/// 返回：以 best-effort 写出一条有界单行诊断，不改变停机计数、首次原因或退出码。
pub(crate) fn report_shutdown(error: &ApplicationError) {
    let message = error_report_line("application shutdown warning: ", error);
    write_stderr(&message);
}

/// 业务作用：将不受信任的错误链封装为不能另起诊断行或操纵终端显示的同步报告。
///
/// 参数说明：
/// - `prefix`：框架固定的单行报告前缀，不得包含业务输入，长度必须小于单条报告上限。
/// - `error`：需要有界展开、脱敏和编码的业务错误。
///
/// 返回：包含前缀和唯一末尾 LF、最多 2 KiB 的 UTF-8 文本；截断不会拆开字符或可见转义。
pub(crate) fn error_report_line(prefix: &'static str, error: &ApplicationError) -> String {
    // 先按原文识别秘密边界，再编码显示字符，避免转义改变凭据键、引号或 header 的含义。
    let body = redact(&error_chain(error));
    let mut message = String::with_capacity(REPORT_MAX_BYTES);
    message.push_str(prefix);
    for character in body.chars() {
        let encoded = match character {
            '\\' => "\\\\".to_owned(),
            '\n' => "\\n".to_owned(),
            '\r' => "\\r".to_owned(),
            '\t' => "\\t".to_owned(),
            character
                if character.is_control()
                    || matches!(character, '\u{2028}' | '\u{2029}')
                    || is_invisible_diagnostic_character(character) =>
            {
                character.escape_unicode().to_string()
            }
            character => character.to_string(),
        };
        // 为框架的行终止符留出空间；不输出半个转义，防止采集端误读边界。
        if message.len() + encoded.len() >= REPORT_MAX_BYTES {
            break;
        }
        message.push_str(&encoded);
    }
    message.push('\n');
    message
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
        "application shutdown summary: outcome={outcome} reason={} planned_steps={} attempted_steps={} abandoned_steps={} component_actions={} initializer_actions={} task_gates={} business_resources={} component_resources={} initializer_resources={} business_shutdown_registered={} business_shutdown_attempted={} business_shutdown_completed={} business_shutdown_failed={} business_shutdown_timed_out={} business_shutdown_panicked={} business_shutdown_abandoned={} failures={} task_abort_attempted={} deadline_exhausted={} duration_ms={}\n",
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
        summary.business_shutdown_registered,
        summary.business_shutdown_attempted,
        summary.business_shutdown_completed,
        summary.business_shutdown_failed,
        summary.business_shutdown_timed_out,
        summary.business_shutdown_panicked,
        summary.business_shutdown_abandoned,
        summary.failures,
        summary.task_abort_attempted,
        summary.deadline_exhausted,
        summary.duration.as_millis(),
    ));
    write_stderr(&message);
}

/// 业务作用：在独立展开边界和空间上限内读取框架错误链，供统一脱敏管道消费。
///
/// Display 与 source 都属于业务回调；单次 panic、重复节点、深度及接收文本超限降级为固定分类。
/// 同步回调自身的阻塞或不协作循环无法被此边界抢占，调用方仍须保证其有限时间返回。
///
/// 参数说明：
/// - `error`：框架层主错误或次要清理错误。
///
/// 返回：至多 32 层、16 KiB 的待脱敏文本；异常或不完整正文不会作为成功格式化结果保留。
pub(crate) fn error_chain(error: &ApplicationError) -> String {
    use std::fmt::Write as _;

    let mut output = String::new();
    let mut seen = Vec::new();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(cause) = source {
        let marker = if seen.iter().any(|previous| std::ptr::eq(*previous, cause)) {
            Some("[error chain cycle]")
        } else if seen.len() == ERROR_CHAIN_MAX_DEPTH {
            Some("[error chain depth limit]")
        } else if output.len() + 2 + 128 > ERROR_CHAIN_MAX_BYTES {
            Some("[error chain text limit]")
        } else {
            None
        };
        // 不继续调用重复或预算外节点，防止错误诊断阻止后续清理和已确定的进程退出。
        if let Some(marker) = marker {
            output.push_str(": ");
            output.push_str(marker);
            break;
        }
        seen.push(cause as *const (dyn std::error::Error + 'static));
        if !output.is_empty() {
            output.push_str(": ");
        }
        let mut text = ErrorText {
            text: String::new(),
            // 给后续固定分类保留空间；最终统一报告仍有更小的输出上限。
            limit: ERROR_CHAIN_MAX_BYTES - output.len() - 128,
            exceeded: false,
        };
        match catch_unwind(AssertUnwindSafe(|| write!(&mut text, "{cause}"))) {
            Ok(Ok(())) if !text.exceeded => output.push_str(&text.text),
            Ok(_) => output.push_str("[error formatting incomplete]"),
            Err(payload) => {
                crate::shutdown::release_shutdown_panic_payload(payload);
                output.push_str("[error display panicked]");
            }
        }
        // source 也可能运行任意业务代码，必须与 Display 分别隔离，且不读取 panic payload。
        source = match catch_unwind(AssertUnwindSafe(|| cause.source())) {
            Ok(source) => source,
            Err(payload) => {
                crate::shutdown::release_shutdown_panic_payload(payload);
                output.push_str(": [error source panicked]");
                None
            }
        };
    }
    output
}

/// 业务作用：识别不可见格式控制及不应进入稳定诊断身份的无字形修饰符。
///
/// 参数说明：
/// - `character`：原始诊断文本中的单个 Unicode 字符。
///
/// 返回：Unicode Format 类、组合字形连接符或变体选择符返回 `true`，普通可见多语言字符返回 `false`。
pub(crate) fn is_invisible_diagnostic_character(character: char) -> bool {
    character.general_category() == GeneralCategory::Format
        || matches!(character, '\u{034f}' | '\u{fe00}'..='\u{fe0f}' | '\u{e0100}'..='\u{e01ef}')
}

/// 业务作用：建立敏感语义比较文本及原文坐标，避免兼容字形或格式控制拆散凭据键。
///
/// 参数说明：
/// - `input`：保留原始大小写和 UTF-8 编码的诊断输入。
///
/// 返回：比较文本和逐字节原文区间；仅采用完全落入 ASCII 字母数字的兼容分解，保留其它可见字符原样。
pub(crate) fn diagnostic_comparison(input: &str) -> (String, Vec<(usize, usize)>) {
    let mut comparison = String::with_capacity(input.len());
    let mut offsets = Vec::with_capacity(input.len());
    for (start, character) in input.char_indices() {
        if is_invisible_diagnostic_character(character) {
            continue;
        }
        let mut compatible = String::new();
        unicode_normalization::char::decompose_compatible(character, |part| compatible.push(part));
        // 不归一化值中的兼容标点，避免凭空产生逗号或引号边界而把秘密尾部当成公开字段。
        if !compatible.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            compatible.clear();
            compatible.push(character);
        }
        comparison.push_str(&compatible);
        offsets.extend(std::iter::repeat_n(
            (start, start + character.len_utf8()),
            compatible.len(),
        ));
    }
    (comparison, offsets)
}

/// 业务作用：对常见 URI 凭据、敏感键赋值、PEM 私钥和自然语言错误中的秘密执行统一替换。
///
/// 参数说明：
///
/// - `input`：可能来自配置加载器或错误链的未信任文本。
///
/// 返回：返回完成统一替换的文本；已识别且处于支持格式中的敏感标量不会保留在结果中。
pub(crate) fn redact(input: &str) -> String {
    let output = redact_uri_userinfo(input);
    let (comparison, offsets) = diagnostic_comparison(&output);
    let lowercase = comparison.to_ascii_lowercase();
    // 所有识别都使用比较文本，但替换必须映射回原文，不能用兼容分解后的字节下标切割业务文本。
    let mut ranges: Vec<_> = collect_sensitive_value_ranges(&comparison, &lowercase)
        .into_iter()
        .filter(|(start, end)| start < end)
        .map(|(start, end)| (offsets[start].0, offsets[end - 1].1))
        .collect();
    // 私钥自带明确的秘密边界，不依赖外层字段名；与字段范围合并后才能避免裸正文漏出。
    ranges.extend(pem_private_key_ranges(&output));
    apply_redaction_ranges(&output, ranges)
}

/// 业务作用：识别独立 PEM 私钥块，防止没有字段前缀的密钥正文进入诊断。
///
/// 参数说明：
/// - `input`：需要按原始字节坐标隐藏私钥的错误正文。
///
/// 返回：完整私钥包含起止标记的范围；明确私钥缺少匹配结束标记时覆盖剩余正文，证书和公钥不产生范围。
fn pem_private_key_ranges(input: &str) -> Vec<(usize, usize)> {
    const BEGIN: &str = "-----BEGIN ";
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while let Some(relative_start) = input[cursor..].find(BEGIN) {
        let start = cursor + relative_start;
        let label_start = start + BEGIN.len();
        cursor = label_start;
        let Some(label_len) = input[label_start..].find("-----") else {
            break;
        };
        let label = &input[label_start..label_start + label_len];
        if !(label == "PRIVATE KEY" || label.ends_with(" PRIVATE KEY"))
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b' ')
        {
            continue;
        }
        let body_start = label_start + label_len + 5;
        let end_marker = format!("-----END {label}-----");
        // 起始标记已经确认私钥身份；缺少结束证据时不能把剩余密钥行当成公开诊断。
        let end = input[body_start..]
            .find(&end_marker)
            .map_or(input.len(), |offset| body_start + offset + end_marker.len());
        ranges.push((start, end));
        cursor = end;
    }
    ranges
}

/// 统一识别的敏感语义词与常见无分隔符字段；业务前缀由词边界解析，不需要枚举。
/// `basic` 和 `digest` 只作为 `authorization` 值的一部分处理，不能把认证方案名后的普通诊断词当成秘密。
const SENSITIVE_REDACTION_KEYS: &[&str] = &[
    "bearer",
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "credentials",
    "access_key",
    "access-key",
    "private_key",
    "private-key",
    "secret_key",
    "secret-key",
    "api_key",
    "api-key",
    "api key",
    "apikey",
    "access key",
    "accesskey",
    "private key",
    "privatekey",
    "secret key",
    "secretkey",
    "authorization",
];

/// 业务作用：按常见分隔符、camelCase 和 acronym 边界拆分诊断字段，保留普通词的完整语义。
///
/// 参数说明：
/// - `name`：任务名称或错误中的字段名。
///
/// 返回：小写语义词集合；不按任意子串拆分，因此 `tokenizer` 等普通单词不会成为凭据词。
pub(crate) fn diagnostic_words(name: &str) -> Vec<String> {
    let mut words = Vec::new();
    for part in name.split(|character: char| !character.is_alphanumeric()) {
        let mut start = 0;
        let bytes = part.as_bytes();
        for index in 1..bytes.len() {
            if bytes[index].is_ascii_uppercase()
                && (bytes[index - 1].is_ascii_lowercase()
                    || bytes[index - 1].is_ascii_digit()
                    || (bytes[index - 1].is_ascii_uppercase()
                        && bytes.get(index + 1).is_some_and(u8::is_ascii_lowercase)))
            {
                words.push(part[start..index].to_ascii_lowercase());
                start = index;
            }
        }
        if start < part.len() {
            words.push(part[start..].to_ascii_lowercase());
        }
    }
    words
}

/// 业务作用：识别带任意业务前缀的凭据字段，使任务名门禁与错误脱敏采用同一套语义边界。
///
/// 参数说明：
/// - `name`：尚未丢失大小写边界的字段或任务名。
///
/// 返回：包含完整凭据语义词、无分隔符凭据后缀或 API/access/private key 组合时返回 `true`。
pub(crate) fn is_sensitive_diagnostic_name(name: &str) -> bool {
    let words = diagnostic_words(name);
    words.iter().any(|word| {
        SENSITIVE_REDACTION_KEYS.contains(&word.as_str())
            || [
                "password",
                "passwd",
                "secret",
                "token",
                "credential",
                "credentials",
                "authorization",
                "apikey",
                "accesskey",
                "privatekey",
                "secretkey",
            ]
            .iter()
            .any(|suffix| word.ends_with(suffix))
    }) || words.windows(2).any(|pair| {
        matches!(pair[0].as_str(), "api" | "access" | "private" | "secret") && pair[1] == "key"
    })
}

/// 自然语言错误中可跳过的连接词；最终仍会把后续标量替换掉，避免错误模板改变脱敏结果。
const NATURAL_LANGUAGE_REDACTION_CONNECTORS: &[&str] = &[
    "is", "was", "were", "equals", "equal", "set", "to", "as", "value",
];

/// 赋值与自然语言前缀共用的明确分隔符，避免本地化正文在不同语法分支产生不同含义。
const SENSITIVE_ASSIGNMENT_SEPARATORS: &[char] = &[':', '=', '：', '＝'];

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

/// 业务作用：在统一比较文本上收集所有敏感值范围，保证敏感标记串联不会因处理顺序漏检。
///
/// 参数说明：
///
/// - `input`：已完成 URI userinfo 替换及受控 Unicode 比较归一的文本。
/// - `lowercase`：与 `input` 字节长度一致的 ASCII 小写文本。
///
/// 返回：返回比较文本坐标中的非空敏感值范围集合；调用方映射回原文后替换，无法确认值边界的标记不产生范围。
fn collect_sensitive_value_ranges(input: &str, lowercase: &str) -> Vec<(usize, usize)> {
    let mut candidates = Vec::new();
    // 先按完整字段识别敏感语义，避免业务前缀遮蔽 camelCase 中的凭据词，也保留 JSON 字段分隔符。
    let mut offset = 0;
    for field in input
        .split(|character: char| !character.is_alphanumeric() && !matches!(character, '_' | '-'))
    {
        let key_start = offset;
        let key_end = key_start + field.len();
        if is_sensitive_diagnostic_name(field) {
            if let Some(range) = sensitive_value_range(input, lowercase, field, key_end) {
                candidates.push((key_start, range.0, range.1));
            }
        }
        // split 的分隔符可能是多字节字符；使用实际字符宽度保持后续字段坐标有效。
        offset = key_end + input[key_end..].chars().next().map_or(0, char::len_utf8);
    }
    for key in SENSITIVE_REDACTION_KEYS {
        let mut search_from = 0;
        while search_from < lowercase.len() {
            let Some(relative) = lowercase[search_from..].find(key) else {
                break;
            };
            let key_start = search_from + relative;
            let key_end = key_start + key.len();
            if sensitive_key_matches_boundary(input, key_start, key_end) {
                if let Some(range) = sensitive_value_range(input, lowercase, key, key_end) {
                    candidates.push((key_start, range.0, range.1));
                }
            }
            search_from = key_end;
        }
    }
    candidates.sort_unstable();
    let mut ranges = Vec::new();
    for (key_start, start, end) in candidates {
        let covered_or_outside_scalar = ranges.iter().any(|&(outer_start, outer_end)| {
            if !(outer_start <= key_start && key_start < outer_end) {
                return false;
            }
            // 只有内层秘密也被完整覆盖时才去重，部分重叠必须保留并在输出时合并。
            if outer_start <= start && end <= outer_end {
                return true;
            }
            // 完整引用值和 header 行具有独立词法边界；不能把值内的词与边界外的公开正文拼成新秘密。
            let bytes = input.as_bytes();
            let quoted = outer_start > 0
                && matches!(bytes[outer_start - 1], b'\'' | b'"')
                && bytes.get(outer_end) == Some(&bytes[outer_start - 1]);
            let line_boundary = input[outer_end..]
                .trim_start_matches(|character: char| {
                    character.is_whitespace() && !matches!(character, '\r' | '\n')
                })
                .starts_with(['\r', '\n']);
            quoted || line_boundary
        });
        if !covered_or_outside_scalar {
            ranges.push((start, end));
        }
    }
    ranges
}

/// 业务作用：确认敏感键两侧处于字段边界，避免把普通单词内部的字母序列当成敏感键。
///
/// 参数说明：
/// - `input`：待扫描的原始错误文本。
/// - `key_start`：敏感键起点。
/// - `key_end`：敏感键结束位置。
///
/// 返回：键前后不是 ASCII 字母或数字时返回 `true`；嵌入普通单词时返回 `false`。
fn sensitive_key_matches_boundary(input: &str, key_start: usize, key_end: usize) -> bool {
    input[..key_start]
        .chars()
        .next_back()
        .is_none_or(|character| !character.is_ascii_alphanumeric())
        && input[key_end..]
            .chars()
            .next()
            .is_none_or(|character| !character.is_ascii_alphanumeric())
}

/// 业务作用：解析单个敏感键后的赋值、自然语言、括号或引号值，返回可整体替换的范围。
///
/// 参数说明：
/// - `input`：用于识别敏感语义的统一比较文本。
/// - `lowercase`：与 `input` 同长度的 ASCII 小写文本，用于识别连接词。
/// - `key`：当前敏感键名。
/// - `key_end`：当前敏感键结束位置。
///
/// 返回：返回敏感值在比较文本中的半开区间；没有值或边界不明确时返回 `None`。
fn sensitive_value_range(
    input: &str,
    lowercase: &str,
    key: &str,
    key_end: usize,
) -> Option<(usize, usize)> {
    let authorization = key.to_ascii_lowercase().ends_with("authorization");
    let key_start = key_end - key.len();
    let scalar_quote = sensitive_key_quoted_scalar(input, key_start, key_end);
    // 引用正文仍使用完整的值语法，但任何分支都只能读取当前标量，不能把下一个 JSON 字段当成秘密。
    let scalar_end = scalar_quote
        .and_then(|quote| quoted_value_end(input, key_end, quote))
        .unwrap_or(input.len());
    // 认证字段和 Bearer 值不能跨物理 header 行；即使内部引号不完整，也必须保留下一行的独立诊断。
    let scalar_end = if authorization || key.eq_ignore_ascii_case("bearer") {
        input[key_end..scalar_end]
            .find(['\r', '\n'])
            .map_or(scalar_end, |offset| key_end + offset)
    } else {
        scalar_end
    };
    let input = &input[..scalar_end];
    let lowercase = &lowercase[..scalar_end];
    let bytes = input.as_bytes();
    let mut cursor = key_end;
    let mut natural_quote = None;
    let mut explicit_value = false;
    while cursor < bytes.len() {
        let character = input[cursor..].chars().next()?;
        if character.is_whitespace() {
            cursor += character.len_utf8();
            continue;
        }
        match bytes[cursor] {
            b'-' => cursor += 1,
            b'(' | b'[' | b'{' => {
                explicit_value = true;
                cursor += 1;
            }
            b'\'' | b'"' => {
                natural_quote = Some(bytes[cursor]);
                cursor += 1;
            }
            _ => break,
        }
    }
    if cursor >= bytes.len() {
        return None;
    }

    if let Some(separator_len) = sensitive_assignment_separator_len(&input[cursor..]) {
        cursor += separator_len;
        cursor = input.len() - input[cursor..].trim_start().len();
        return sensitive_scalar_value_range(input, cursor, authorization);
    }

    if let Some(quote) = natural_quote {
        let value_start = cursor;
        let value_end = quoted_value_end(input, value_start, quote).unwrap_or(input.len());
        return (value_end > value_start).then_some((value_start, value_end));
    }

    let mut value_start = cursor;
    for _ in 0..8 {
        let word_end = natural_language_word_end(input, value_start);
        if word_end == value_start
            || !NATURAL_LANGUAGE_REDACTION_CONNECTORS
                .iter()
                .any(|connector| *connector == &lowercase[value_start..word_end])
        {
            break;
        }
        value_start = skip_natural_language_prefix(input, word_end);
        explicit_value = true;
        if value_start >= bytes.len() {
            return None;
        }
    }
    if matches!(bytes[value_start], b'\'' | b'"')
        || (bytes[value_start] == b'\\'
            && matches!(bytes.get(value_start + 1), Some(b'\'') | Some(b'"')))
    {
        return sensitive_scalar_value_range(input, value_start, authorization);
    }
    if authorization
        && !explicit_value
        && !authorization_value_starts_with_scheme(input, value_start)
    {
        return None;
    }
    // 普通敏感名词后的诊断词不是秘密；仅明确值语法或 Bearer 认证方案可以省略赋值符。
    if !explicit_value && !key.eq_ignore_ascii_case("bearer") && !authorization {
        return None;
    }
    sensitive_scalar_value_range(input, value_start, authorization)
}

/// 业务作用：为所有明确值语法提供同一替换边界，避免连接词或认证方案绕过引号值处理。
///
/// 参数说明：
/// - `input`：已限制到外层标量结束之前的比较文本，未引用时为完整正文。
/// - `start`：已确认的值起点，允许等于正文末尾以表示空值。
/// - `authorization`：该敏感字段是否携带 Authorization 语义，决定是否按认证方案解析整体值。
///
/// 返回：非空敏感值的半开区间；空值返回 `None`。转义或未闭合的引号值保守覆盖当前标量余部。
fn sensitive_scalar_value_range(
    input: &str,
    start: usize,
    authorization: bool,
) -> Option<(usize, usize)> {
    let bytes = input.as_bytes();
    let first = *bytes.get(start)?;
    if first == b'\\' && matches!(bytes.get(start + 1), Some(b'\'') | Some(b'"')) {
        // 转义引号与其中的结构字符都属于秘密，不能在逗号处留下同一值的后半段。
        return Some((start, input.len()));
    }
    if matches!(first, b'\'' | b'"') {
        let value_start = start + 1;
        let value_end = quoted_value_end(input, value_start, first).unwrap_or(input.len());
        return (value_end > value_start).then_some((value_start, value_end));
    }
    if authorization && authorization_value_starts_with_scheme(input, start) {
        let end = authorization_value_end(input, start);
        return (end > start).then_some((start, end));
    }
    let end = unquoted_sensitive_value_end(input, start);
    (end > start).then_some((start, end))
}

/// 业务作用：整体隐藏已识别认证方案的凭据，避免内部引号或 Digest 参数分隔符暴露认证内容。
///
/// 参数说明：
/// - `input`：已被外层字符串边界限制的比较文本；没有外层字符串时仍可能包含多行 header。
/// - `start`：Basic、Bearer 或 Digest 方案名的起点。
///
/// 返回：认证值的结束坐标。Digest 覆盖当前 header 行的整个参数列表；Basic/Bearer 保留引号内部标点，
/// 只在引号外识别结构分隔符。缺失或编码后的引号边界保守覆盖当前行，不越过外层字符串。
fn authorization_value_end(input: &str, start: usize) -> usize {
    let line_end = input[start..]
        .find(['\r', '\n'])
        .map_or(input.len(), |offset| start + offset);
    let input = &input[..line_end];
    let scheme_end = natural_language_word_end(input, start);
    let digest = input[start..scheme_end].eq_ignore_ascii_case("digest");
    let bytes = input.as_bytes();
    let mut quote = None;
    let mut escaped = false;
    let mut end = start;
    while end < bytes.len() {
        let byte = bytes[end];
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            // 编码后的引号属于当前标量；没有独立解码层时不能把其中的逗号误认为外部字段边界。
            if quote.is_none() && matches!(bytes.get(end + 1), Some(b'\'') | Some(b'"')) {
                return input.trim_end().len();
            }
            escaped = true;
        } else if let Some(current_quote) = quote {
            if byte == current_quote {
                quote = None;
            }
        } else if matches!(byte, b'\'' | b'"') {
            quote = Some(byte);
        } else if byte == b',' {
            // Digest 扩展参数也属于认证证据；只看显式参数赋值，不按参数名白名单截断。
            if !digest || !authorization_parameter_follows(input, end + 1) {
                break;
            }
        } else if !digest && matches!(byte, b';' | b'}' | b']' | b'&' | b')') {
            break;
        }
        end += 1;
    }
    input[..end].trim_end().len()
}

/// 业务作用：判断认证值中的逗号是否继续引入参数，保留扩展参数并避免吞掉明确的普通诊断词。
///
/// 参数说明：
/// - `input`：已经限制到当前 header 行及外层字符串的比较文本。
/// - `start`：逗号之后的候选参数起点。
///
/// 返回：非空 token 参数名后紧随可选空白和等号时返回 `true`；没有赋值语法时返回 `false`。
fn authorization_parameter_follows(input: &str, start: usize) -> bool {
    let rest = input[start..].trim_start();
    let name_end = rest
        .find(|character: char| {
            !character.is_ascii_alphanumeric()
                && !matches!(
                    character,
                    '!' | '#'
                        | '$'
                        | '%'
                        | '&'
                        | '\''
                        | '*'
                        | '+'
                        | '-'
                        | '.'
                        | '^'
                        | '_'
                        | '`'
                        | '|'
                        | '~'
                )
        })
        .unwrap_or(rest.len());
    name_end > 0 && rest[name_end..].trim_start().starts_with(['=', '＝'])
}

/// 业务作用：统一识别错误正文中的明确赋值符，使本地化文本与 ASCII 字段使用相同脱敏边界。
///
/// 参数说明：
/// - `input`：从候选赋值符开始的原文切片。
///
/// 返回：首字符为 ASCII 或全角冒号、等号时返回其 UTF-8 字节数，否则返回 `None`。
fn sensitive_assignment_separator_len(input: &str) -> Option<usize> {
    input
        .chars()
        .next()
        .filter(|character| SENSITIVE_ASSIGNMENT_SEPARATORS.contains(character))
        .map(char::len_utf8)
}

/// 业务作用：识别敏感词所在的外层引号标量，使值解析不会把普通诊断误认为字段或跨越结构边界。
///
/// 参数说明：
/// - `input`：未改写的错误文本。
/// - `key_start`：敏感词起点。
/// - `key_end`：敏感词结束位置。
///
/// 返回：敏感词位于引号标量内部且该引号不是字段名时返回对应引号；字段名或未处于引号标量时返回 `None`。
fn sensitive_key_quoted_scalar(input: &str, key_start: usize, key_end: usize) -> Option<u8> {
    let bytes = input.as_bytes();
    let mut quote = None;
    let mut escaped = false;
    for (index, byte) in bytes[..key_start].iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if matches!(*byte, b'\'' | b'"') {
            // 英文所有格和缩写中的撇号不建立字符串状态，否则会遮蔽后续明确赋值的秘密。
            if *byte == b'\''
                && index > 0
                && bytes[index - 1].is_ascii_alphanumeric()
                && bytes.get(index + 1).is_some_and(u8::is_ascii_alphanumeric)
            {
                continue;
            }
            if quote == Some(*byte) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(*byte);
            }
        }
    }
    let quote = quote?;
    let mut cursor = key_end;
    let mut escaped = false;
    while cursor < bytes.len() {
        if escaped {
            escaped = false;
        } else if bytes[cursor] == b'\\' {
            escaped = true;
        } else if bytes[cursor] == quote {
            return sensitive_assignment_separator_len(input[cursor + 1..].trim_start())
                .is_none()
                .then_some(quote);
        }
        cursor += 1;
    }
    Some(quote)
}

/// 业务作用：寻找引号包裹值的结束位置，确保含空格的秘密不会只替换第一个词。
///
/// 参数说明：
/// - `input`：待扫描文本。
/// - `start`：引号内容起点。
/// - `quote`：包裹值的 ASCII 引号。
///
/// 返回：返回未转义结束引号的位置；没有找到结束引号时返回 `None`。
fn quoted_value_end(input: &str, start: usize, quote: u8) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut escaped = false;
    for (offset, byte) in bytes.iter().enumerate().skip(start) {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == quote {
            return Some(offset);
        }
    }
    None
}

/// 业务作用：定位明确敏感字段的完整未加引号值，覆盖多词口令、认证 scheme 和 PEM 私钥。
///
/// 参数说明：
/// - `input`：待扫描文本。
/// - `start`：已经由赋值符、连接词或认证方案确认的值起点。
///
/// 返回：返回 PEM 结束标记之后或结构分隔符之前的位置；不完整的 PEM 覆盖剩余文本，防止泄露后续行。
fn unquoted_sensitive_value_end(input: &str, start: usize) -> usize {
    let rest = &input[start..];
    if let Some(header) = rest.strip_prefix("-----BEGIN ") {
        if let Some(label_end) = header.find("-----") {
            let end_marker = format!("-----END {}-----", &header[..label_end]);
            return rest
                .find(&end_marker)
                .map_or(input.len(), |end| start + end + end_marker.len());
        }
        // 明确的 PEM 开头缺少边界时不能只删除首个单词，否则正文仍会进入错误报告。
        return input.len();
    }
    let bytes = input.as_bytes();
    let mut end = start;
    while end < bytes.len()
        && !matches!(
            bytes[end],
            b'\r' | b'\n' | b',' | b';' | b'}' | b']' | b'&' | b')' | b'\'' | b'"'
        )
    {
        end += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    end
}

/// 业务作用：确认没有字段分隔符的 `authorization` 后面是否明确开始了认证方案值。
///
/// 参数说明：
/// - `input`：待扫描文本。
/// - `start`：候选授权值的起点。
///
/// 返回：首个词是 `Basic`、`Digest` 或 `Bearer` 时返回 `true`；普通诊断词返回 `false`，避免被整段替换。
fn authorization_value_starts_with_scheme(input: &str, start: usize) -> bool {
    let end = natural_language_word_end(input, start);
    ["basic", "digest", "bearer"]
        .iter()
        .any(|scheme| input[start..end].eq_ignore_ascii_case(scheme))
}

/// 业务作用：跳过自然语言连接词后的标点和空白，找到下一段待脱敏内容。
///
/// 参数说明：
/// - `input`：待扫描文本。
/// - `start`：连接词结束位置。
///
/// 返回：返回下一个值或连接词的 UTF-8 字符边界；文本结束时返回长度。
fn skip_natural_language_prefix(input: &str, start: usize) -> usize {
    input.len()
        - input[start..]
            .trim_start_matches(|character: char| {
                character.is_whitespace()
                    || SENSITIVE_ASSIGNMENT_SEPARATORS.contains(&character)
                    || matches!(character, '-' | '(' | '[' | '{')
            })
            .len()
}

/// 业务作用：找到自然语言错误中第一个单词的边界，用于识别连接词而不截断实际秘密。
///
/// 参数说明：
/// - `input`：待处理文本。
/// - `start`：位于 UTF-8 字符边界的单词起点。
///
/// 返回：返回遇到空白或自然语言分隔标点的位置；该位置仍是 UTF-8 字符边界。
fn natural_language_word_end(input: &str, start: usize) -> usize {
    input[start..]
        .find(|character: char| {
            character.is_whitespace()
                || SENSITIVE_ASSIGNMENT_SEPARATORS.contains(&character)
                || matches!(
                    character,
                    ',' | ';' | '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}'
                )
        })
        .map_or(input.len(), |offset| start + offset)
}

/// 业务作用：按原文坐标应用所有脱敏范围，使嵌套或重叠敏感标记只生成一次替换。
///
/// 参数说明：
/// - `input`：待输出的原始错误文本。
/// - `ranges`：原文中的敏感值半开区间。
///
/// 返回：返回替换后的文本；没有有效范围时返回输入副本。
fn apply_redaction_ranges(input: &str, mut ranges: Vec<(usize, usize)>) -> String {
    ranges.retain(|(start, end)| start < end);
    ranges.sort_unstable();
    let mut merged = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, previous_end)) = merged.last_mut() {
            if start <= *previous_end {
                *previous_end = (*previous_end).max(end);
                continue;
            }
        }
        merged.push((start, end));
    }

    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    for (start, end) in merged {
        output.push_str(&input[cursor..start]);
        output.push_str("***");
        cursor = end;
    }
    output.push_str(&input[cursor..]);
    output
}

/// 业务作用：在 UTF-8 字符边界内限制单条诊断长度。
///
/// 参数说明：
/// - `input`：已完成脱敏、准备进入诊断通道的文本。
///
/// 返回：至多 2 KiB 的有效 UTF-8 文本；超长时按字符边界截断并补换行。
pub(crate) fn bounded(input: &str) -> String {
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
