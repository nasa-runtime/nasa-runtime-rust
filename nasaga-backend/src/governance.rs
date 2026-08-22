//! Saga 租户配额和管理动作速率的后端中立裁决。

/// 人工关闭管理动作写入审计的稳定名称。
pub const MANUAL_CLOSE_ACTION: &str = "manual_close";

/// 业务作用：区分租户在飞实例配额预留成功和稳定超限拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaReservation {
    /// 预留成功，实例创建可以继续。
    Reserved,
    /// 该租户在飞实例已达上限，调用方必须回滚创建事务。
    Exceeded,
}

/// 业务作用：区分当前窗口管理动作预算预留成功和稳定超限拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionRateReservation {
    /// 当前窗口仍有预算，动作可以继续。
    Reserved,
    /// 当前窗口预算已耗尽，调用方必须回滚动作事务。
    Exceeded,
}
