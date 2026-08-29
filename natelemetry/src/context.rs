//! 环境级(task-local)当前 trace 上下文。
//!
//! 入口层(HTTP 中间件、消息消费、任务执行)以 [`with_ambient`] 建立作用域；
//! 出站层(REST 客户端、Kafka producer)在业务未显式绑定时回退读取
//! [`ambient`]。显式绑定始终优先于环境值，纯异步入口在 spawn 边界显式重建作用域。
//!
//! 语义合同:
//! - 作用域随 future 生命周期,嵌套时内层覆盖、离开恢复,跨 `.await` 稳定;
//! - `tokio::spawn` 出去的子任务**不继承**环境值——spawn 边界必须显式捕获再重建作用域,
//!   这是有意设计:隐式跨任务继承会让"哪些后台工作挂在本请求链路上"变得不可审计;
//! - 本模块只承载 26 字节 Copy 值,无分配、无锁;读取失败(不在任何作用域内)返回 `None`。

use crate::TraceContext;
use std::future::Future;

tokio::task_local! {
    /// 当前 task 作用域内的 trace 上下文;只经 [`with_ambient`] 建立,业务代码不直接触碰。
    static AMBIENT_TRACE: Option<TraceContext>;
}

/// 业务作用：读取当前作用域的环境 trace 上下文，供出站客户端在业务未显式绑定时回退取值。
///
/// 参数说明: 无。
///
/// 返回：处于 [`with_ambient`] 作用域内且该作用域携带上下文时返回 `Some`；
/// 不在任何作用域内、或作用域显式携带 `None` 时返回 `None`。
pub fn ambient() -> Option<TraceContext> {
    AMBIENT_TRACE.try_with(|context| *context).ok().flatten()
}

/// 业务作用：以给定上下文为当前环境值运行 future，是入口层建立链路作用域的唯一入口。
///
/// 参数说明：
/// - `context`: 本作用域的环境上下文；`None` 表示显式清空(子作用域读不到外层值)，
///   用于"确定不属于任何链路"的隔离段，而不是简单不设置。
/// - `future`: 在该作用域内执行的业务 future。
///
/// 返回：future 的输出；作用域随 future 结束而结束，外层原值自动恢复。
pub async fn with_ambient<F: Future>(context: Option<TraceContext>, future: F) -> F::Output {
    AMBIENT_TRACE.scope(context, future).await
}

/// 业务作用：在同步代码段内建立环境作用域，供非 async 的入口(如同步派发点)在调用业务闭包前恢复上下文。
///
/// 参数说明：
/// - `context`: 本作用域的环境上下文，语义同 [`with_ambient`]。
/// - `f`: 在该作用域内执行的同步闭包。
///
/// 返回：闭包的返回值；作用域随闭包返回而结束。
pub fn with_ambient_sync<T>(context: Option<TraceContext>, f: impl FnOnce() -> T) -> T {
    AMBIENT_TRACE.sync_scope(context, f)
}
