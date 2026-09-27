//! 既有运行快照的只读、有界组合，不创建网络管理端点。

use crate::{
    Application, ApplicationError, ApplicationPhase, ApplicationResult, ApplicationState,
    ComponentId, ReloadState, ReloadTarget,
};
use std::time::Instant;

/// 单个领域快照的采样时刻；不同项之间没有全局事务保证。
#[derive(Debug)]
pub struct Sampled<T> {
    /// 读取此领域快照的本地单调时刻，不表示远端事实的发生时间。
    pub sampled_at: Instant,
    /// 该次采样取得的领域值，与其它领域不构成原子快照。
    pub value: T,
}

/// 已脱敏的目标配置状态，不包含配置正文或失败链。
#[derive(Debug)]
pub struct ConfigStatusSummary {
    /// 已脱敏的配置应用目标名称。
    pub target: String,
    /// 固定状态分类：applied、restart_required 或 apply_failed。
    pub state: &'static str,
    /// 此目标最后成功采用的配置版本，可能落后于期望视图。
    pub applied_version: u64,
}

/// 聚合诊断的条数上限适用于每个领域；所有动态名称最多 256 个字符。
#[derive(Debug)]
pub struct DiagnosticSnapshot {
    /// 本次聚合采样开始时刻。
    pub started_at: Instant,
    /// 各领域读取完成时刻；与开始时刻共同界定采样窗口。
    pub finished_at: Instant,
    /// 聚合结束时读取的宿主生命周期状态。
    pub state: ApplicationState,
    /// 有界健康明细与既有 readiness 结论，不发起额外健康探测。
    pub readiness: Sampled<crate::readiness::ReadinessSnapshot>,
    /// 本次固定配置视图的期望版本。
    pub config_version: u64,
    /// 各配置目标的真实应用状态，按名称排序并受条数限制。
    pub config_statuses: Sampled<Vec<ConfigStatusSummary>>,
    /// 至少一个领域的明细因条数上限被省略，聚合结论不因此变为完整清单。
    pub truncated: bool,
    /// 遥测出口的现有统计；未创建出口时内部值为 None。
    #[cfg(feature = "telemetry")]
    pub telemetry: Sampled<Option<natelemetry::ExporterSnapshot>>,
    /// 按 Redis 来源采样的分区运行事实，来源与执行域明细均受条数限制。
    #[cfg(feature = "redis")]
    pub redis_partitions: Sampled<Vec<crate::RedisPartitionObservation>>,
    /// 数据源或方法身份与有效 SQL 观测策略，不包含 SQL 正文或参数。
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
