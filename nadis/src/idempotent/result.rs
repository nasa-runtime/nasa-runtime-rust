//! 幂等计数脚本返回码与结构化拒绝；业务按枚举分支，不匹配 Redis 错误文本。

use std::fmt;

/// 幂等脚本第一元素返回码；文本是脚本协议的一部分，不可改动。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdempotentResultCode {
    /// 首次请求已执行并登记凭证与 TTL。
    Applied,
    /// 首次请求已执行且凭证存在，但字段/键 TTL 未确认；按成功返回并降级健康。
    AppliedTtlMissing,
    /// 窗口内重复 nonce，返回首次结果，无新副作用。
    Duplicate,
    /// 账本 key 类型被占用为非 Hash，拒绝且不改目标。
    RejectedLedgerType,
    /// 未知操作或非法脚本配置，拒绝且不改目标。
    RejectedOperation,
    /// 原生命令因类型、数值、NaN 或 int64 越界被拒绝，未改目标。
    RejectedCommand,
}

impl IdempotentResultCode {
    /// 业务作用：把脚本返回的码文本解析为封闭枚举；未知码 fail-closed，不当作成功。
    ///
    /// 参数说明：
    /// - `code`: 脚本返回数组第一元素文本。
    ///
    /// 返回：已知码返回对应枚举；未知返回 `None`，调用方按协议错误处理。
    pub(crate) fn parse(code: &str) -> Option<Self> {
        Some(match code {
            "APPLIED" => Self::Applied,
            "APPLIED_TTL_MISSING" => Self::AppliedTtlMissing,
            "DUPLICATE" => Self::Duplicate,
            "REJECTED_LEDGER_TYPE" => Self::RejectedLedgerType,
            "REJECTED_OPERATION" => Self::RejectedOperation,
            "REJECTED_COMMAND" => Self::RejectedCommand,
            _ => return None,
        })
    }
}

/// 幂等计数向业务暴露的封闭拒绝原因；不含 Redis 原始错误文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotentRejection {
    /// 账本 key 被非 Hash 类型占用。
    LedgerType,
    /// 操作类型非法或脚本配置无效。
    Operation,
    /// 原生计数命令拒绝（WRONGTYPE、非数值、越界等）。
    Command,
}

impl IdempotentRejection {
    /// 业务作用：返回指标 `reason` 使用的封闭低基数标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`ledger_type`/`operation`/`command`。
    pub fn label(self) -> &'static str {
        match self {
            Self::LedgerType => "ledger_type",
            Self::Operation => "operation",
            Self::Command => "command",
        }
    }
}

/// 幂等计数拒绝错误；`detail` 仅供诊断，业务分支应基于 `code`。
#[derive(Debug, Clone)]
pub struct IdempotentCounterError {
    /// 封闭拒绝原因。
    pub code: IdempotentRejection,
    /// 脚本或命令返回的诊断文本，不含业务结果。
    pub detail: String,
}

impl fmt::Display for IdempotentCounterError {
    /// 业务作用：输出封闭原因与诊断文本，便于日志定位而不泄漏业务值。
    ///
    /// 参数说明：
    /// - `formatter`: 目标格式化缓冲区。
    ///
    /// 返回：写入成功时完成，否则透传格式化错误。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "idempotent counter rejected: {} ({})",
            self.code.label(),
            self.detail
        )
    }
}

impl std::error::Error for IdempotentCounterError {}
