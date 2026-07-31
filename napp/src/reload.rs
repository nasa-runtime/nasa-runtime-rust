//! 组件配置热重应用协议。
//!
//! 配置热刷新驱动（配置中心组件的 watch driver）对每一帧候选树先做整帧校验，再逐组件判定：
//! 相关配置段未变化 → 直接推进 applied_version；变化且组件登记过本协议的句柄 → 调用句柄重应用，
//! 成功记 `Applied`、失败保留 last-known-good 并记 `ApplyFailed`；变化但没有句柄 → 如实记
//! `RestartRequired`。登记发生在组件启动阶段，驱动创建于 Ready，之后只读。

use serde_json::Value;

use crate::{ApplicationResult, ComponentId};

/// 可热刷组件提交给配置热刷新驱动的重应用句柄。
///
/// 实现者必须满足 last-known-good 契约：apply 失败时不得破坏当前运行态（由底层实现保证
/// "先备好再原子换"，如 nalog）；驱动只把结果如实记入 ReloadStatus，不做补偿或重试。
///
/// apply 是同步调用：当前唯一实现（log）只做快速的过滤器与 appender 换装；出现慢速或必须
/// 异步的重应用时再扩展协议，不预先制造异步表面。
// 驱动侧（nacos-config）单独关闭时，本协议只有登记方在编译，消费入口空置属于预期形态。
#[cfg_attr(not(feature = "nacos-config"), allow(dead_code))]
pub(crate) trait ConfigApplier: Send + Sync {
    /// 返回该句柄负责的组件身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；驱动用它把重应用结果记到正确的 `ReloadTarget::Component` 上。
    fn component(&self) -> ComponentId;

    /// 对尚未发布的候选配置树执行一次重应用。
    ///
    /// # 参数
    ///
    /// - `candidate`：合并、插值并通过整帧校验的候选配置树；实现只读取自己的配置段。
    fn apply(&self, candidate: &Value) -> ApplicationResult<()>;
}
