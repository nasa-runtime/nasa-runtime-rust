//! Inbox 消费去重核心：重复消息裁决必须与业务副作用处于同一事务。

#![forbid(unsafe_code)]

/// 一次事务内 Inbox 去重裁决。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxClaim {
    /// 本事务首次取得消息，调用方可以继续执行业务副作用。
    Claimed,
    /// 唯一标记已经由此前成功事务提交；本次必须跳过业务副作用并正常确认消息。
    Duplicate,
}

impl InboxClaim {
    /// 业务作用：是否允许当前事务执行一次业务副作用。
    pub fn should_process(self) -> bool {
        matches!(self, Self::Claimed)
    }
}
