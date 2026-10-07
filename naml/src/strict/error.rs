use std::fmt;

use super::ConfigPath;

/// 不含来源正文、环境原值或底层错误链的稳定错误类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// 来源或加载声明不满足约定格式。
    Declaration,
    /// 必需来源或引用字段不存在。
    Missing,
    /// 来源无法打开、读取或取得必要元数据。
    Unreadable,
    /// 来源不是允许读取的普通文件。
    FileType,
    /// 来源文本或身份无法按要求编码。
    Encoding,
    /// 文档不能按声明格式解析。
    Format,
    /// 同一文档含重复键，或来源身份重复。
    Duplicate,
    /// 字段路径与已有节点结构冲突。
    PathConflict,
    /// 请求的格式、结构或声明能力不受支持。
    Unsupported,
    /// 已读取的来源身份、内容或匹配集合发生变化。
    SourceChanged,
    /// 来源或候选不满足固定的读取与装配策略。
    PolicyChanged,
    /// 读取、解析、展开或观察超过资源预算。
    Limit,
    /// 占位符或字段引用语法不合法。
    Expression,
    /// 引用不能求值，且没有可用默认值。
    Unresolved,
    /// 配置引用形成循环依赖。
    Cycle,
    /// 候选值不能绑定到要求的业务类型。
    Binding,
    /// 候选未通过调用方提供的业务校验。
    Validation,
    /// 宿主已撤销本轮装配。
    Cancelled,
    /// 来源观察计划无法建立或维持。
    Observation,
}

/// 安全诊断仅包含错误类别、数值来源身份与配置字段路径。
#[derive(Clone, Eq, PartialEq)]
pub struct ConfigError {
    /// 可供调用方分类处理的稳定拒绝原因。
    pub kind: ErrorKind,
    /// 本轮有序文档编号；未关联来源时为空。
    pub source_id: Option<usize>,
    /// 失败字段的结构化位置，不含字段原值。
    pub path: Option<ConfigPath>,
    /// 解析器可提供时保留的文本行列。
    pub position: Option<SourcePosition>,
    /// 检出的循环引用路径；默认格式化不输出完整链。
    pub cycle: Vec<ConfigPath>,
}

/// 严格装配不会返回部分候选。
pub type Result<T> = std::result::Result<T, ConfigError>;

impl ConfigError {
    /// 业务作用：创建不会携带原值的拒绝原因。
    /// 参数说明：`kind` 指定失败类别。
    /// 返回：尚未关联来源或字段的错误。
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            source_id: None,
            path: None,
            position: None,
            cycle: Vec::new(),
        }
    }

    /// 业务作用：保留解析器提供的位置而丢弃其包含原文的错误信息。
    /// 参数说明：`line/column` 是从一开始的来源位置。
    /// 返回：带行列信息的安全错误。
    pub fn positioned(mut self, line: usize, column: usize) -> Self {
        self.position = Some(SourcePosition { line, column });
        self
    }

    /// 业务作用：关联本轮文档身份，避免输出文件或远端地址。
    /// 参数说明：`source_id` 是本轮有序文档编号。
    /// 返回：带来源身份的错误。
    pub fn in_source(mut self, source_id: usize) -> Self {
        self.source_id = Some(source_id);
        self
    }

    /// 业务作用：关联失败字段供调用方定位配置。
    /// 参数说明：`path` 是结构化字段路径。
    /// 返回：带字段位置的安全错误。
    pub fn at(mut self, path: &ConfigPath) -> Self {
        self.path = Some(path.clone());
        self
    }
}

impl fmt::Display for ConfigError {
    /// 业务作用：仅输出稳定类别和受限字段信息。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：格式化结果，不输出任何字段值。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "naml: {:?}", self.kind)?;
        if let Some(id) = self.source_id {
            write!(f, " source={id}")?;
        }
        if let Some(path) = &self.path {
            write!(f, " field={path}")?;
        }
        if let Some(position) = &self.position {
            write!(f, " line={} column={}", position.line, position.column)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

/// 从一开始的文本位置，不包含失败行正文。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourcePosition {
    /// 从一开始的行号。
    pub line: usize,
    /// 从一开始的列号。
    pub column: usize,
}

impl fmt::Debug for ConfigError {
    /// 业务作用：默认调试输出沿用有界安全摘要，循环完整路径只经显式字段访问。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：无原值、无底层错误链的摘要。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
