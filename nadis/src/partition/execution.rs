//! 冻结执行域拓扑与容量份额；不同 Redis 源不共享注册表或执行权。

use super::{PartitionExecutorCfg, PartitionExecutorScope, PartitionLimits, PreparedGroup};
use crate::error::{NasaRedisError, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Debug, Clone)]
pub(super) struct DomainQuota {
    /// 本域独占的非终态记录与后继责任份额。
    pub records: usize,
    /// 本域可持有的正文预留与解码估算字节总额。
    pub bytes: usize,
    /// 本域可同时持有的读取批次预留数。
    pub batches: usize,
    /// 本域交给异步删除持有者的记录数上限。
    pub deletes: usize,
}

pub(super) struct DomainSpec {
    /// 组级和 Stream 级域的逻辑组身份；源级域为 None。
    pub group: Option<String>,
    /// 仅 Stream 级域具有固定物理分区编号。
    pub partition: Option<u32>,
    /// 冻结后不跨域借用的最低保证与剩余份额。
    pub quota: DomainQuota,
}

pub(super) struct ExecutionPlan {
    /// 全部域采用的隔离粒度。
    pub scope: PartitionExecutorScope,
    /// 稳定排序的域身份和预算，索引也是运行时域身份。
    pub specs: Vec<DomainSpec>,
    /// 完整物理组前缀到每个物理分区所属域索引的映射。
    pub mapping: HashMap<String, Vec<usize>>,
    /// 经规范化和类型容量校验后，各域共同使用的 Runner 参数。
    pub runner_config: napart::RunnerConfig,
}

impl ExecutionPlan {
    /// 业务作用：按完整物理拓扑冻结执行域，预先证明每域至少能够取得一次整批读取。
    ///
    /// 参数说明：
    /// - `groups`: 已校验且物理前缀唯一的组，每组包含固定分区数和最终读取批量。
    /// - `config`: 执行域划分策略、Runner 数量上限与各 Runner 的冻结配置。
    /// - `limits`: 当前来源共同遵守的记录、正文、读取批次与异步删除预算。
    /// - `plans`: 已冻结的消费计划总数，用于证明任务类型状态容量充足。
    ///
    /// 返回：各域份额总和等于对应源级预算，记录总额取所有记录责任容量的共同最小值；
    /// 执行域数量、槽数、整批最低预算不足或算术溢出时返回配置错误，不创建 Runner。
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
        // 拓扑资源必须在创建任一 Runner 前可证明有界，不能随持锁变化临时扩容。
        if count == 0
            || config.max_runners == 0
            || config.max_runners > 4096
            || count > config.max_runners
            || config.max_total_partitions == 0
            || count
                .checked_mul(runner_config.partitions)
                .is_none_or(|slots| slots > config.max_total_partitions)
        {
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
        // 固定组排序让余数份额与物理分区映射稳定；后续 claim 变化不能重分预算或改变执行域。
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
///
/// 参数说明：
/// - `total`: 当前资源在整个来源范围内允许分配的总额。
/// - `minimum`: 按稳定域顺序排列的正数最低需求，记录和字节需求必须能容纳各自整批读取。
/// - `resource`: 由内部调用点提供的固定资源名称，用于区分预算不足的配置错误。
///
/// 返回：与最低需求等长且总和恰为 total 的固定份额，余数从首域起逐个分配；
/// 没有域、最低需求为零、最低总额溢出或总预算不足时返回配置错误。
fn allocate(total: usize, minimum: &[usize], resource: &str) -> Result<Vec<usize>> {
    let required = minimum
        .iter()
        .try_fold(0usize, |sum, n| sum.checked_add(*n));
    // 最低保证尚不能覆盖所有域时拒绝分配，避免前面的域抢占预算而使后面的域永久无法读取。
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
    /// 启动前冻结的域身份与资源份额。
    pub spec: DomainSpec,
    /// 仅为本域服务的本地任务执行器。
    pub runner: napart::PartitionRunner,
    /// 本域不能继续安全接纳任务时发布的降级状态。
    pub degraded: AtomicBool,
    /// 本域当前尚未终结的异步删除责任计数。
    pub delete_records: AtomicUsize,
}

pub(super) struct ExecutionDomains {
    /// 当前来源冻结的执行隔离粒度。
    pub scope: PartitionExecutorScope,
    /// 所有物理来源共同引用的稳定域表。
    pub domains: Vec<ExecutionDomain>,
    /// 与计划一致的物理来源映射，运行中不因租约变化而重新分配。
    mapping: HashMap<String, Vec<usize>>,
}

impl ExecutionDomains {
    /// 业务作用：在启动前登记本源全部 Runner，名称只在本源私有注册表中解析。
    ///
    /// 参数说明：
    /// - `plan`: 已完成数量、槽数与全部最低配额校验的不可变执行域表，其所有权转移到新集合。
    ///
    /// 返回：全部 Runner 登记成功后返回尚未启动的完整集合；任一登记失败返回配置错误，
    /// 已创建但未启动的句柄随局部集合释放，不发布部分执行拓扑。
    pub fn new(plan: ExecutionPlan) -> Result<Self> {
        let registry = napart::PartitionRunnerRegistry::builder()
            .max_runners(plan.specs.len())
            .build()
            .map_err(|e| NasaRedisError::Config(e.to_string()))?;
        // 所有 Runner 都先在私有集合中准备；只有完整构造成功后，启动者才能接管并开放执行。
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
    ///
    /// 参数说明：
    /// - `group`: 启动前已登记的物理组前缀，必须属于当前来源的冻结拓扑。
    /// - `partition`: 该物理组中已校验范围的分区编号。
    ///
    /// 返回：与初始拓扑一致的执行域索引；未知前缀或越界编号违反内部调用不变量并触发 panic。
    pub fn domain(&self, group: &str, partition: u32) -> usize {
        self.mapping[group][partition as usize]
    }
}
