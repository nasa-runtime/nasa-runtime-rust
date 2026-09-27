//! 冻结执行域拓扑与容量份额；不同 Redis 源不共享注册表或执行权。

use super::{PartitionExecutorCfg, PartitionExecutorScope, PartitionLimits, PreparedGroup};
use crate::error::{NasaRedisError, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Debug, Clone)]
pub(super) struct DomainQuota {
    pub records: usize,
    pub bytes: usize,
    pub batches: usize,
    pub deletes: usize,
}

pub(super) struct DomainSpec {
    pub group: Option<String>,
    pub partition: Option<u32>,
    pub quota: DomainQuota,
}

pub(super) struct ExecutionPlan {
    pub scope: PartitionExecutorScope,
    pub specs: Vec<DomainSpec>,
    pub mapping: HashMap<String, Vec<usize>>,
    pub runner_config: napart::RunnerConfig,
}

impl ExecutionPlan {
    /// 业务作用：按完整物理拓扑冻结执行域，预先证明每域至少能够取得一次整批读取。
    /// 参数说明：`groups` 为已解析组；`config` 为本地执行策略；`limits` 为源级总额；`plans` 为计划数量。
    /// 返回：所有域的配额之和等于源级上限；拓扑、算术或资源不足时拒绝激活。
    pub fn build(
        groups: &[PreparedGroup],
        config: &PartitionExecutorCfg,
        limits: &PartitionLimits,
        plans: usize,
    ) -> Result<Self> {
        let runner_config = config.runner_config(plans)?;
        let mut ordered: Vec<_> = groups.iter().collect();
        ordered.sort_by(|a, b| a.id.cmp(&b.id));
        let count = match config.scope {
            PartitionExecutorScope::Source => 1,
            PartitionExecutorScope::Group => groups.len(),
            PartitionExecutorScope::Stream => groups
                .iter()
                .try_fold(0usize, |sum, group| sum.checked_add(group.count as usize))
                .ok_or_else(|| NasaRedisError::Config("execution domain count overflow".into()))?,
        };
        if count == 0
            || config.max_runners == 0
            || config.max_runners > 4096
            || count > config.max_runners
            || config.max_total_partitions == 0
            || count
                .checked_mul(runner_config.partitions)
                .is_none_or(|slots| slots > config.max_total_partitions)
        {
            // 拓扑资源必须在创建任一 Runner 前可证明有界，不能随持锁变化临时扩容。
            return Err(NasaRedisError::Config(
                "execution domains exceed max_runners or max_total_partitions".into(),
            ));
        }
        let mut identities = Vec::with_capacity(count);
        let mut batches = Vec::with_capacity(count);
        let mut mapping = HashMap::new();
        if config.scope == PartitionExecutorScope::Source {
            identities.push((None, None));
            batches.push(
                groups
                    .iter()
                    .map(|g| g.stream_cfg.batch_size)
                    .max()
                    .unwrap_or(0),
            );
        }
        for group in ordered {
            let mut ids = Vec::with_capacity(group.count as usize);
            if config.scope == PartitionExecutorScope::Group {
                identities.push((Some(group.id.clone()), None));
                batches.push(group.stream_cfg.batch_size);
            }
            for partition in 0..group.count {
                if config.scope == PartitionExecutorScope::Stream {
                    identities.push((Some(group.id.clone()), Some(partition)));
                    batches.push(group.stream_cfg.batch_size);
                }
                ids.push(identities.len() - 1);
            }
            mapping.insert(group.layout.prefix.clone(), ids);
        }
        let min_bytes = batches
            .iter()
            .map(|batch| batch.checked_mul(limits.max_record_bytes))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                NasaRedisError::Config("execution domain payload minimum overflow".into())
            })?;
        let records = allocate(limits.record_limit(), &batches, "records")?;
        let bytes = allocate(
            limits.max_inflight_payload_bytes,
            &min_bytes,
            "payload bytes",
        )?;
        let read_batches = allocate(limits.max_active_batches, &vec![1; count], "active batches")?;
        let deletes = allocate(
            limits.max_async_delete_records,
            &vec![1; count],
            "async delete records",
        )?;
        let specs = identities
            .into_iter()
            .enumerate()
            .map(|(id, (group, partition))| DomainSpec {
                group,
                partition,
                quota: DomainQuota {
                    records: records[id],
                    bytes: bytes[id],
                    batches: read_batches[id],
                    deletes: deletes[id],
                },
            })
            .collect();
        Ok(Self {
            scope: config.scope,
            specs,
            mapping,
            runner_config,
        })
    }
}

/// 业务作用：先保留每域最低需求，再均分余量，防止一个域消耗其他域的保证份额。
/// 参数说明：`total` 为源级限额；`minimum` 为各域整批需求；`resource` 为配置错误的资源名称。
/// 返回：固定、不借出的份额；总额不足或算术越界时返回配置错误。
fn allocate(total: usize, minimum: &[usize], resource: &str) -> Result<Vec<usize>> {
    let required = minimum
        .iter()
        .try_fold(0usize, |sum, n| sum.checked_add(*n));
    let Some(spare) = required.and_then(|required| total.checked_sub(required)) else {
        return Err(NasaRedisError::Config(format!(
            "execution domain {resource} budget cannot reserve every domain's minimum"
        )));
    };
    if minimum.is_empty() || minimum.contains(&0) {
        return Err(NasaRedisError::Config(
            "execution domain minimum must be positive".into(),
        ));
    }
    let count = minimum.len();
    Ok(minimum
        .iter()
        .enumerate()
        .map(|(id, minimum)| minimum + spare / count + usize::from(id < spare % count))
        .collect())
}

pub(super) struct ExecutionDomain {
    pub spec: DomainSpec,
    pub runner: napart::PartitionRunner,
    pub degraded: AtomicBool,
    pub delete_records: AtomicUsize,
}

pub(super) struct ExecutionDomains {
    pub scope: PartitionExecutorScope,
    pub domains: Vec<ExecutionDomain>,
    mapping: HashMap<String, Vec<usize>>,
}

impl ExecutionDomains {
    /// 业务作用：在启动前登记本源全部 Runner，名称只在本源私有注册表中解析。
    /// 参数说明：`plan` 为已完成拓扑和预算校验的域表。
    /// 返回：尚未启动的完整执行器集合；失败不会产生后台任务。
    pub fn new(plan: ExecutionPlan) -> Result<Self> {
        let registry = napart::PartitionRunnerRegistry::builder()
            .max_runners(plan.specs.len())
            .build()
            .map_err(|e| NasaRedisError::Config(e.to_string()))?;
        let mut domains = Vec::with_capacity(plan.specs.len());
        for (id, spec) in plan.specs.into_iter().enumerate() {
            let runner = registry
                .get_or_create(format!("redis-partition-{id}"), plan.runner_config.clone())
                .map_err(|e| NasaRedisError::Config(e.to_string()))?;
            domains.push(ExecutionDomain {
                spec,
                runner,
                degraded: AtomicBool::new(false),
                delete_records: AtomicUsize::new(0),
            });
        }
        Ok(Self {
            scope: plan.scope,
            domains,
            mapping: plan.mapping,
        })
    }

    /// 业务作用：为物理来源取得不随 claim epoch 改变的执行域身份。
    /// 参数说明：`group` 为物理组前缀；`partition` 为组内编号。
    /// 返回：启动期已登记的域索引；内部拓扑不一致时拒绝继续执行。
    pub fn domain(&self, group: &str, partition: u32) -> usize {
        self.mapping[group][partition as usize]
    }
}
