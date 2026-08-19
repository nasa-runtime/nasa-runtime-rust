//! Run 记录的只读投影：按固定字段顺序还原 Run 的身份、状态、执行权、结果、参数与 Fanout intent。
//!
//! 字段是观测值；owner/attemptToken 只用于诊断，任何副作用提交仍须通过写脚本按状态重新复验，不能凭读投影
//! 直接改状态。

use crate::error::{NasaRedisError, Result};
use crate::job::model::JobState;

/// 单个 Run 的只读投影。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRun {
    /// Run 标识。
    pub run_id: String,
    /// 任务名。
    pub job_name: String,
    /// Worker 能力名。
    pub worker_name: String,
    /// Run 状态。
    pub state: JobState,
    /// 逻辑触发时刻毫秒。
    pub logical_fire_at: i64,
    /// 实际观测触发时刻毫秒。
    pub triggered_at: i64,
    /// 当前 attempt 序号。
    pub attempt: i64,
    /// 当前 attempt 的 fencing token；无 owner 时为空。
    pub attempt_token: String,
    /// 当前执行 owner；未领取时为空。
    pub owner: String,
    /// 服务端租约截止毫秒；未领取时为 0。
    pub lease_until: i64,
    /// 结果码；未完成时为空。
    pub result_code: String,
    /// 结果摘要；未完成时为空。
    pub result_summary: String,
    /// 错误类型；无错误时为空。
    pub error_type: String,
    /// 错误摘要；无错误时为空。
    pub error_summary: String,
    /// Base64 参数载荷。
    pub parameter_payload: String,
    /// schema 标识。
    pub schema_id: String,
    /// 线编码名称。
    pub wire_codec: String,
    /// 关联 Fanout 标识；非 Fanout 根时为空。
    pub fanout_id: String,
    /// 兼容能力快照标识；非 Fanout 时为空。
    pub snapshot_id: String,
    /// 根 attempt；非 Fanout 时为 0。
    pub root_attempt: i64,
    /// Fanout 创建截止毫秒；非 Fanout 时为 0。
    pub create_deadline_at: i64,
}

impl JobRun {
    /// 业务作用：把 `read_run` 的固定顺序字段数组还原为 Run 投影；缺字段或状态非法时 fail-closed。
    ///
    /// 参数说明：
    /// - `fields`: `read_run.lua` 按声明顺序返回的 21 个字段（缺失为 nil）。
    ///
    /// 返回：字段完整且状态合法时返回投影；数量不足或 state 非法时返回协议错误。
    pub(crate) fn from_fields(fields: &[redis::Value]) -> Result<Self> {
        if fields.len() != 21 {
            return Err(protocol("read_run 返回字段数不符合合同"));
        }
        let text = |index: usize| optional_text(&fields[index]);
        let number = |index: usize| -> Result<i64> { optional_i64(&fields[index]) };
        let state = JobState::parse(&text(3)).ok_or_else(|| protocol("read_run state 字段非法"))?;
        Ok(Self {
            run_id: text(0),
            job_name: text(1),
            worker_name: text(2),
            state,
            logical_fire_at: number(4)?,
            triggered_at: number(5)?,
            attempt: number(6)?,
            attempt_token: text(7),
            owner: text(8),
            lease_until: number(9)?,
            result_code: text(10),
            result_summary: text(11),
            error_type: text(12),
            error_summary: text(13),
            parameter_payload: text(14),
            schema_id: text(15),
            wire_codec: text(16),
            fanout_id: text(17),
            snapshot_id: text(18),
            root_attempt: number(19)?,
            create_deadline_at: number(20)?,
        })
    }
}

/// 业务作用：把一个 Run 字段值归一化为文本；nil 与非字符串取空串。
///
/// 参数说明：
/// - `value`: Run 字段的原始值。
///
/// 返回：字符串或整数的文本，其它为空串。
fn optional_text(value: &redis::Value) -> String {
    match value {
        redis::Value::Nil => String::new(),
        redis::Value::Int(n) => n.to_string(),
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        redis::Value::SimpleString(text) => text.clone(),
        _ => String::new(),
    }
}

/// 业务作用：把一个数值 Run 字段解释为 i64；nil 或空取 0，非法文本 fail-closed。
///
/// 参数说明：
/// - `value`: Run 字段的原始值。
///
/// 返回：合法数值或 0；非数值文本返回协议错误。
fn optional_i64(value: &redis::Value) -> Result<i64> {
    match value {
        redis::Value::Nil => Ok(0),
        redis::Value::Int(n) => Ok(*n),
        redis::Value::BulkString(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            if text.is_empty() {
                Ok(0)
            } else {
                text.parse::<i64>()
                    .map_err(|_| protocol("read_run 数值字段非法"))
            }
        }
        redis::Value::SimpleString(text) if text.is_empty() => Ok(0),
        redis::Value::SimpleString(text) => text
            .parse::<i64>()
            .map_err(|_| protocol("read_run 数值字段非法")),
        _ => Err(protocol("read_run 数值字段类型非法")),
    }
}

/// 业务作用：构造 Job 协议错误。参数说明：`message` 摘要。返回：协议错误。
fn protocol(message: &str) -> NasaRedisError {
    crate::job::JobError::Protocol(message.to_owned()).into()
}
