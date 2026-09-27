//! 显式启用的开发参数输出；不改变 Mapper 参数所需 trait，未知类型仅显示类型名。

use super::{config::LogLevel, MapperMethodMeta};
use std::fmt::{self, Debug, Write};

/// 只在单次 bind 之前借用参数，不保留数据到异步任务。
pub struct Capture<'a, T: ?Sized>(pub &'a T);

/// 自动借用选择可显示类型；泛型或未知类型始终保留无附加 bound 的占位路径。
pub trait CaptureParameter {
    /// 业务作用：把一个真实 bind 依次追加到受限开发事件，不改变绑定对象。
    /// 参数说明：`buffer` 为当前语句预算，`name` 为编译期参数路径。
    /// 返回：无；关闭、敏感字段或超过数量时不调用值的格式化实现。
    fn capture(self, buffer: &mut ParameterBuffer, name: &str);
}

impl<T: ?Sized + Debug> CaptureParameter for &Capture<'_, T> {
    /// 业务作用：仅为允许的普通值类型生成有界显示，敏感名字优先脱敏。
    /// 参数说明：`buffer` 为语句输出预算，`name` 为 bind 名称。
    /// 返回：追加一个参数；任何格式化展开均退回安全占位。
    fn capture(self, buffer: &mut ParameterBuffer, name: &str) {
        buffer.record(name, std::any::type_name::<T>(), Some(self));
    }
}
impl<T: ?Sized> CaptureParameter for &&Capture<'_, T> {
    /// 业务作用：为不满足显示能力的类型保留原有 Mapper 编译合同。
    /// 参数说明：`buffer` 为语句输出预算，`name` 为 bind 名称。
    /// 返回：仅写类型占位，不要求 Debug、Serialize 或 Clone。
    fn capture(self, buffer: &mut ParameterBuffer, name: &str) {
        buffer.record(name, std::any::type_name::<T>(), None);
    }
}
impl<T: ?Sized + Debug> Debug for Capture<'_, T> {
    /// 业务作用：将白名单值的显示写入已有受限格式化器。
    /// 参数说明：`formatter` 由有界字符 writer 提供。
    /// 返回：格式化状态，不创建完整中间字符串。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// 一条 SQL 的开发输出；关闭时 String 保持无分配。
pub struct ParameterBuffer {
    meta: &'static MapperMethodMeta,
    enabled: bool,
    count: usize,
    omitted: bool,
    text: String,
}
impl ParameterBuffer {
    /// 业务作用：在实际构造 SQLx 查询时读取冻结的开发参数开关。
    /// 参数说明：`meta` 为已验证环境与策略的方法单元。
    /// 返回：默认关闭且不分配文本的缓冲。
    pub fn new(meta: &'static MapperMethodMeta) -> Self {
        let policy = &meta.policy().console;
        // 关闭参数诊断时不进入 subscriber；生产默认路径只读取冻结开关。
        let enabled = policy.enabled
            && policy.include_parameters
            && super::diagnostic(|| match policy.statement_level {
                LogLevel::Trace => {
                    tracing::enabled!(target: "namapper::parameters", tracing::Level::TRACE)
                }
                _ => tracing::enabled!(target: "namapper::parameters", tracing::Level::DEBUG),
            })
            .unwrap_or(false);
        Self {
            meta,
            enabled,
            count: 0,
            omitted: false,
            text: String::new(),
        }
    }

    /// 业务作用：先施加名称、类型与数量门禁，再按字符预算显示允许的参数。
    /// 参数说明：`name` 为静态 bind 路径，`type_name` 为编译期类型，`value` 只在允许时格式化。
    /// 返回：追加受限文本；超限仅记录省略标志。
    fn record(&mut self, name: &str, type_name: &str, value: Option<&dyn Debug>) {
        if !self.enabled {
            return;
        }
        let policy = &self.meta.policy().console;
        if self.count >= policy.max_parameters {
            self.omitted = true;
            return;
        }
        self.count += 1;
        if self.count > 1 {
            self.text.push_str(", ");
        }
        self.text.push_str(&super::clean_text(name, 128));
        self.text.push('=');
        if sensitive_name(name) {
            self.text.push_str("<redacted>");
            return;
        }
        if let Some(value) = value.filter(|_| displayable_type(type_name)) {
            let mut writer = BoundedText {
                text: String::new(),
                remaining: policy.max_parameter_chars,
            };
            let result = super::diagnostic(|| write!(&mut writer, "{value:?}"));
            if result.is_some() {
                self.text.push_str(&writer.text);
            } else {
                self.text.push_str("<unavailable>");
            }
        } else {
            self.text.push_str("<opaque>");
        }
        self.text.push('(');
        self.text.push_str(&super::clean_text(type_name, 128));
        self.text.push(')');
    }

    /// 业务作用：按语句级别输出 bind 顺序与类型，prepared SQL 仍由 SQLx 统一输出。
    /// 参数说明：无。
    /// 返回：消费缓冲，事件包含静态方法身份；日志处理异常不影响 SQL。
    pub fn emit(self) {
        if !self.enabled {
            return;
        }
        super::diagnostic(|| {
            macro_rules! event { ($level:expr) => { tracing::event!(target: "namapper::parameters", $level,
                method = self.meta.method, datasource = self.meta.datasource, driver = self.meta.driver,
                parameters = %self.text, truncated = self.omitted, "Parameters") }; }
            match self.meta.policy().console.statement_level {
                LogLevel::Trace => event!(tracing::Level::TRACE),
                _ => event!(tracing::Level::DEBUG),
            }
        });
    }
}

/// 业务作用：对大小写、分隔符及属性路径保持一致的敏感名称强制脱敏。
/// 参数说明：`name` 为源码 bind 参数路径。
/// 返回：名字包含凭据语义时禁止显示值。
fn sensitive_name(name: &str) -> bool {
    let normalized: String = name
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    [
        "password",
        "passwd",
        "pwd",
        "token",
        "secret",
        "credential",
        "authorization",
        "auth",
        "apikey",
        "privatekey",
        "密钥",
        "密码",
        "令牌",
        "认证",
    ]
    .iter()
    .any(|part| normalized.contains(part))
}

/// 业务作用：只格式化已知无业务自定义回调的常见标量、字符串和标准容器。
/// 参数说明：`name` 为 Rust 类型名称。
/// 返回：允许显示时为真；新类型默认使用占位。
fn displayable_type(name: &str) -> bool {
    let name = name.trim_start_matches('&').trim_start_matches("mut ");
    if [
        "bool",
        "char",
        "str",
        "alloc::string::String",
        "i8",
        "i16",
        "i32",
        "i64",
        "i128",
        "isize",
        "u8",
        "u16",
        "u32",
        "u64",
        "u128",
        "usize",
        "f32",
        "f64",
    ]
    .contains(&name)
    {
        return true;
    }
    ["core::option::Option<", "alloc::vec::Vec<"]
        .iter()
        .any(|prefix| {
            name.strip_prefix(prefix)
                .and_then(|tail| tail.strip_suffix('>'))
                .is_some_and(displayable_type)
        })
}

struct BoundedText {
    text: String,
    remaining: usize,
}
impl Write for BoundedText {
    /// 业务作用：在格式化过程中限制字符量，避免先分配完整大参数。
    /// 参数说明：`value` 为格式化器当前文本片段。
    /// 返回：预算耗尽立即停止格式化；控制字符替换为空格。
    fn write_str(&mut self, value: &str) -> fmt::Result {
        for character in value.chars() {
            if self.remaining == 0 {
                return Err(fmt::Error);
            }
            self.remaining -= 1;
            self.text.push(if character.is_control() {
                ' '
            } else {
                character
            });
        }
        Ok(())
    }
}
