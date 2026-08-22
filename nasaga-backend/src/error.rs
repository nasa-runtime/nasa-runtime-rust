//! Saga adapter 向运行核心公开的封闭错误分类。

/// 运行核心可以安全穷举的 Saga 后端失败类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaBackendErrorKind {
    /// 目标持久事实不存在，调用方可以按业务状态重新裁决。
    NotFound,
    /// 乐观并发、唯一身份或互斥事实冲突，调用方必须重读已提交状态。
    Conflict,
    /// owner、租约或 fencing token 已失效，旧执行者必须立即停止。
    Fenced,
    /// 明确未提交的瞬态存储失败，可以按既定预算重试。
    Retryable,
    /// COMMIT 已发出但结果无法确认，禁止直接重执行业务闭包。
    OutcomeUnknown,
    /// 配置、连接、编码或持久结构失败，不能按业务冲突吸收。
    Infrastructure,
}

/// Saga 后端中立错误；只携带稳定类别和不含业务内容的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaBackendError {
    kind: SagaBackendErrorKind,
    reason: String,
}

impl SagaBackendError {
    /// 业务作用：由 adapter 用封闭类别和脱敏原因构造运行核心错误。
    ///
    /// 参数说明：
    /// - `kind`：可穷举的失败类别。
    /// - `reason`：不含 SQL、endpoint、凭据或业务正文的稳定原因。
    ///
    /// 返回：可跨数据库后端统一裁决的错误。
    pub fn new(kind: SagaBackendErrorKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }

    /// 业务作用：读取运行核心用于重试、停止或重查裁决的封闭类别。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时指定的错误类别。
    pub fn kind(&self) -> SagaBackendErrorKind {
        self.kind
    }

    /// 业务作用：读取允许进入日志和低基数观测的脱敏原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含数据库与业务敏感信息的原因文本。
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl std::fmt::Display for SagaBackendError {
    /// 业务作用：输出后端中立的脱敏 Saga 错误摘要。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：摘要写入成功时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "saga backend error: {}", self.reason)
    }
}

impl std::error::Error for SagaBackendError {}
