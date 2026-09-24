//! 既有运行快照的只读、有界组合，不创建网络管理端点。

use crate::{
    Application, ApplicationError, ApplicationPhase, ApplicationResult, ApplicationState,
    ComponentId, ReloadState, ReloadTarget,
};
use std::time::Instant;

/// 单个领域快照的采样时刻；不同项之间没有全局事务保证。
#[derive(Debug)]
pub struct Sampled<T> {
    pub sampled_at: Instant,
    pub value: T,
}

/// 已脱敏的目标配置状态，不包含配置正文或失败链。
#[derive(Debug)]
pub struct ConfigStatusSummary {
    pub target: String,
    pub state: &'static str,
    pub applied_version: u64,
}

/// 聚合诊断的条数上限适用于每个领域；所有动态名称最多 256 个字符。
#[derive(Debug)]
pub struct DiagnosticSnapshot {
    pub started_at: Instant,
    pub finished_at: Instant,
    pub state: ApplicationState,
    pub readiness: Sampled<crate::readiness::ReadinessSnapshot>,
    pub config_version: u64,
    pub config_statuses: Sampled<Vec<ConfigStatusSummary>>,
    pub truncated: bool,
    #[cfg(feature = "telemetry")]
    pub telemetry: Sampled<Option<natelemetry::ExporterSnapshot>>,
    #[cfg(feature = "redis")]
    pub redis_partitions: Sampled<Vec<crate::RedisPartitionObservation>>,
    #[cfg(feature = "mapper-observability")]
    pub sql: Vec<
        Sampled<(
            String,
            namapper_core::observability::config::EffectivePolicy,
        )>,
    >,
}

impl Application {
    /// 业务作用：组合已有健康、配置、遥测、SQL 策略和 Redis 分区快照，不执行后端探测。
    /// 参数说明：`limit` 限定每个领域最多返回 1..=256 条。
    /// 返回：带各自采样时间的有界脱敏摘要；超限明确标记，不含原始配置、secret、SQL 或业务载荷。
    pub async fn diagnostic_snapshot(&self, limit: usize) -> ApplicationResult<DiagnosticSnapshot> {
        if !(1..=256).contains(&limit) {
            return Err(ApplicationError::new(
                ComponentId::Application,
                ApplicationPhase::Running,
                "diagnostic limit must be between 1 and 256",
            ));
        }
        let started_at = Instant::now();
        let mut readiness = Sampled {
            sampled_at: Instant::now(),
            value: self.readiness_snapshot(),
        };
        let mut truncated = readiness.value.entries.len() > limit;
        readiness.value.entries = readiness
            .value
            .entries
            .iter()
            .take(limit)
            .cloned()
            .map(|mut entry| {
                entry.name = clean(&entry.name).into();
                entry.component = clean(&entry.component).into();
                entry
            })
            .collect();
        let config = self.config_view();
        let mut statuses = config
            .reload_statuses()
            .iter()
            .map(|(target, status)| ConfigStatusSummary {
                target: match target {
                    ReloadTarget::Application => "application".to_owned(),
                    ReloadTarget::Component(component) => component.to_string(),
                    ReloadTarget::Managed(name) => format!("managed/{}", clean(name)),
                    ReloadTarget::UserHook(name) => format!("hook/{}", clean(name)),
                },
                state: match status.state {
                    ReloadState::Applied => "applied",
                    ReloadState::RestartRequired { .. } => "restart_required",
                    ReloadState::ApplyFailed { .. } => "apply_failed",
                },
                applied_version: status.applied_version,
            })
            .collect::<Vec<_>>();
        statuses.sort_by(|a, b| a.target.cmp(&b.target));
        truncated |= statuses.len() > limit;
        statuses.truncate(limit);
        let config_statuses = Sampled {
            sampled_at: Instant::now(),
            value: statuses,
        };
        #[cfg(feature = "telemetry")]
        let telemetry = Sampled {
            sampled_at: Instant::now(),
            value: self.telemetry_snapshot(),
        };
        #[cfg(feature = "redis")]
        let redis_partitions = {
            let mut values = self.redis_partition_observations();
            truncated |= values.len() > limit;
            values.truncate(limit);
            for value in &mut values {
                value.source = clean(&value.source);
                truncated |= value.partition.execution_domains.len() > limit;
                value.partition.execution_domains.truncate(limit);
                for domain in &mut value.partition.execution_domains {
                    domain.group = domain.group.as_deref().map(clean);
                }
            }
            Sampled {
                sampled_at: Instant::now(),
                value: values,
            }
        };
        #[cfg(feature = "mapper-observability")]
        let sql = {
            let mut values = Vec::new();
            if let Ok(runtime) = self
                .resources()
                .get::<std::sync::Arc<crate::sql_observability::SqlObservabilityRuntime>>()
                .await
            {
                let (policies, omitted) = runtime.diagnostic_policies(limit);
                truncated |= omitted;
                for (name, policy) in policies {
                    values.push(Sampled {
                        sampled_at: Instant::now(),
                        value: (clean(&name), policy),
                    });
                }
            }
            values
        };
        Ok(DiagnosticSnapshot {
            started_at,
            finished_at: Instant::now(),
            state: self.state(),
            readiness,
            config_version: config.snapshot().version(),
            config_statuses,
            truncated,
            #[cfg(feature = "telemetry")]
            telemetry,
            #[cfg(feature = "redis")]
            redis_partitions,
            #[cfg(feature = "mapper-observability")]
            sql,
        })
    }
}

/// 业务作用：使诊断名称遵循宿主脱敏策略与长度限制。
/// 参数说明：`value` 为资源或目标名称。
/// 返回：不超过 256 个字符的可展示摘要。
fn clean(value: &str) -> String {
    crate::report::redact(value).chars().take(256).collect()
}
