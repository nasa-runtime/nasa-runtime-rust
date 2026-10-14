//! 显式采样源级责任；监控出口关闭后仍可通过返回报告读取真实寿命。

use crate::{CloseReport, KafkaProxy, OffsetIsolation, OffsetObservation};

impl KafkaProxy {
    /// 业务作用：由宿主按自身周期采样源级关闭责任，不增加定时器或 broker 请求。
    /// 参数说明：无。
    /// 返回：独立采样的关闭报告；指标有界异步入队，出口拥塞或关闭可以丢弃，不影响资源裁决。
    pub fn sample_metrics(&self) -> CloseReport {
        let report = self.close_report();
        let (routes, groups) = {
            let groups = self
                .inner
                .groups
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                groups.values().map(|group| group.route_count()).sum(),
                groups.len(),
            )
        };
        crate::consumer::registry::publish_registry_gauges(&self.inner, routes, groups);
        let values = [
            ("source_stop_requested", u64::from(report.stop_requested)),
            (
                "source_resources_closed",
                u64::from(report.resources_closed),
            ),
            ("close_active_owners", report.consumers.active_owners as u64),
            (
                "close_active_handlers",
                report.consumers.active_handlers as u64,
            ),
            (
                "close_retained_records",
                report.consumers.retained_records as u64,
            ),
            (
                "close_released_unverified_records",
                report.consumers.released_unverified_records,
            ),
            ("replay_active_tasks", report.replay_tasks as u64),
            (
                "transfer_active_dependencies",
                report.transfer_dependencies as u64,
            ),
            ("transaction_active_tasks", report.transaction_tasks as u64),
            ("dlt_active_tasks", report.dlt_active_tasks as u64),
            (
                "admin_active_borrows",
                report.auxiliary_resources_active as u64,
            ),
            ("metrics_queued", report.metrics_queued as u64),
            ("metrics_active", report.metrics_active as u64),
        ];
        for (name, value) in values {
            self.inner
                .gauge(name, i64::try_from(value).unwrap_or(i64::MAX), &[]);
        }
        report
    }
    /// 业务作用：把已封存的 broker 提交距离送入有界观测出口，缺项不会被补成完整零积压。
    /// 参数说明：report 为显式请求范围和读取隔离下的证据。
    /// 返回：无组时不输出；任一缺项使 lag=-1，并同时报告范围大小、缺项及观测时刻。
    pub(crate) fn sample_offset_metrics(&self, report: &OffsetObservation) {
        let Some(group) = report.group.as_deref() else {
            return;
        };
        let mut missing = 0i64;
        let mut sum = 0i64;
        for p in &report.partitions {
            if let Some(value) = p.lag.value {
                sum = sum.saturating_add(value);
            } else {
                missing += 1;
            }
        }
        let isolation = match report.isolation {
            OffsetIsolation::ReadCommitted => "read_committed",
            OffsetIsolation::ReadUncommitted => "read_uncommitted",
        };
        let labels = [("group", group), ("isolation", isolation)];
        let timestamp = report
            .completed_at
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        for (name, value) in [
            (
                "broker_lag_offsets",
                if missing == 0 {
                    sum
                } else {
                    -1
                },
            ),
            ("broker_lag_partitions", report.partitions.len() as i64),
            ("broker_lag_missing_partitions", missing),
            ("broker_lag_observed_at_millis", timestamp),
        ] {
            self.inner.gauge(name, value, &labels);
        }
    }
}
