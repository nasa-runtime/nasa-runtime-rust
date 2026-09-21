//! 接口级隔离监控:bulkhead + 超时 + 降级 + Prometheus `/metrics` 出口。
//!
//! 执行面提供信号量隔离、单请求超时、降级、配置驱动 isolation 和周期请求汇总日志；
//! 观测面提供同源指标出口、集群 Dashboard 与独立单 owner 平台 controller。
//!
//! 三个接入面:
//! - 注解:`#[grafana(max_concurrent = 50, timeout_ms = 800, tps = 1)]`(空参 = 只监控)。
//! - 显式:[`Command::new`] / [`Command::monitor`] / [`Command::with_tps`] + `run`/`run_fn`。
//! - 配置驱动:yml 根 `grafana.isolation` + [`init_isolation`] + [`dispatch`] 全局中间件。
//!
//! 受管 Application 通过 `grafana.observability` 自动装配出口，不需要业务挂 Router；独立 Axum
//! 可以使用 [`metrics`]。[`observability`] 提供冻结身份、共享配置、Dashboard 与平台边界。
//! controller 独立持有平台写权限，业务副本只持有指标数据面凭据。remote write 失联规则只引用
//! 外部平台持续提供的期望实例指标，不维护库存，也不从应用心跳推断实例本应在线。
#![recursion_limit = "512"]

mod command;
mod counters;
mod fallback;
mod isolation;
mod metrics_source;
pub mod observability;
mod prometheus;
mod registry;
mod rolling;
pub use metrics_source::metrics_source;

pub use command::{current_tps, Command, FallbackFn};
pub use fallback::{
    global_fallback_installed, initialize_global_fallback, install_global_fallback, FallbackCause,
    FallbackContext, FallbackDecision, GlobalFallbackHandler, GlobalFallbackInstallError,
};
pub use isolation::{dispatch, init_isolation, IsolationRule};
pub use prometheus::{
    metrics, render_metrics, structured_metrics_snapshot, PrometheusMetricSample,
    PrometheusMetricValue,
};
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
