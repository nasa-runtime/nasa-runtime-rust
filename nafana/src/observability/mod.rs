//! 冻结的观测配置、同源指标出口与单 owner 平台调和。
//!
//! 应用只创建指标出口；平台凭据仅交给独立 controller。所有失败均与业务事务结果隔离。

mod assets;
mod config;
mod exporter;
pub mod platform;

pub use assets::{dashboards, discovery_config, owner_id, pod_monitor, prometheus_rules};
pub use config::*;
pub use exporter::{encode_remote_write, identity_samples, Exporter, ExporterState, MetricRefresh};
