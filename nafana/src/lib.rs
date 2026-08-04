//! 接口级隔离监控:bulkhead + 超时 + 降级 + Prometheus `/metrics` 出口。
//!
//! 执行面提供信号量隔离、单请求超时、降级、配置驱动 isolation 和周期请求汇总日志；
//! 观测面提供 Prometheus 文本 `/metrics` 与 Grafana 接口墙。
//!
//! 三个接入面:
//! - 注解:`#[grafana(max_concurrent = 50, timeout_ms = 800, tps = 1)]`(空参 = 只监控)。
//! - 显式:[`Command::new`] / [`Command::monitor`] / [`Command::with_tps`] + `run`/`run_fn`。
//! - 配置驱动:yml 根 `grafana.isolation` + [`init_isolation`] + [`dispatch`] 全局中间件。
//!
//! 观测出口:把 [`metrics`] 挂到业务路由供 Prometheus 抓取；Grafana 直接从 Prometheus 查询并聚合
//! 全部实例。面板资源见 `dashboards/nafana-interfaces.json`。
#![recursion_limit = "512"]

mod command;
mod counters;
mod fallback;
mod isolation;
mod prometheus;
mod registry;
mod rolling;

pub use command::{current_tps, Command, FallbackFn};
pub use fallback::{
    global_fallback_installed, initialize_global_fallback, install_global_fallback, FallbackCause,
    FallbackContext, FallbackDecision, GlobalFallbackHandler, GlobalFallbackInstallError,
};
pub use isolation::{dispatch, init_isolation, IsolationRule};
pub use prometheus::{metrics, render_metrics};
pub use registry::MonitorConflict;

// ── re-export 过程宏 ──
pub use nafana_macro::{global_fallback, grafana};

/// 宏展开专用的第三方依赖桥:`#[grafana]` 生成代码经
/// `<运行时根>::__private::axum` 引用 axum——业务只依赖 `nasa` 时无需再直接声明 axum。
/// **不属于稳定业务 API**,随时可能变化。
#[doc(hidden)]
pub mod __private {
    pub use crate::fallback::{CollectedGlobalFallback, NAFANA_COLLECTED_GLOBAL_FALLBACKS};
    pub use axum;
    pub use linkme;
}
