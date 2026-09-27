//! 同一 RedisPartition 全部消费组共享的资源合同。

use crate::error::{NasaRedisError, Result};
use serde::{Deserialize, Serialize};

/// 消费责任与发布责任的硬数量上限；正文预算衡量估算存活字节，不代表进程 RSS。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionLimits {
    /// 同一发布 owner 可保留的发布票据与未回收发送任务上限。
    pub max_inflight_publishes: usize,
    /// 所有在途发布共同占用的序列化正文预留上限，单位字节。
    pub max_inflight_publish_bytes: usize,
    /// 全部消费组共享的非终态记录及读取预留上限。
    pub max_inflight_records: usize,
    /// 消费侧线格式与解码对象的共享估算存活字节上限。
    pub max_inflight_payload_bytes: usize,
    /// 同时持有读取预留的批次数上限。
    pub max_active_batches: usize,
    /// 等待读取预算的请求数上限。
    pub max_read_waiters: usize,
    /// 单条原始 Envelope 的线格式上限；解码对象估算另外占用共享 payload byte 预算。
    pub max_record_bytes: usize,
    /// 消费任务责任上限，参与整批记录准入容量计算。
    pub max_inflight_tasks: usize,
    /// 后继处理责任上限，读取前随记录一并预留。
    pub max_continuations: usize,
    /// 等待提交的责任上限，参与整批预留以避免执行后无处登记。
    pub max_pending_commits: usize,
    /// 提交记录责任上限，参与共享记录容量计算。
    pub max_commit_records: usize,
    /// 已确认提交后交给异步删除 owner 的记录数上限。
    pub max_async_delete_records: usize,
    /// 重试责任票据上限，在初始读取时纳入最坏情况预留。
    pub max_retry_tickets: usize,
    /// 同时保留的业务顺序键上限，参与共享记录容量计算。
    pub max_ordered_keys: usize,
    /// 单个物理来源可登记的阻塞业务键上限。
    pub max_blocked_keys_per_source: usize,
    /// 单个顺序键可保留的后继记录上限，同时约束整批准入。
    pub max_deferred_records_per_key: usize,
    /// 等待顺序键前驱完成的记录总上限。
    pub max_deferred_records: usize,
    /// 不可路由记录的责任上限，读取前即参与共享容量预留。
    pub max_unroutable_records: usize,
}

impl Default for PartitionLimits {
    /// 业务作用：给出共享预算默认值，使正常批次可在执行前取得所有后继责任。
    /// 参数说明: 无。
    /// 返回：默认最多保留 4096 条非终态记录与 256 MiB 估算正文。
    fn default() -> Self {
        Self {
            max_inflight_publishes: 128,
            max_inflight_publish_bytes: 64 << 20,
            max_inflight_records: 4096,
            max_inflight_payload_bytes: 256 << 20,
            max_active_batches: 256,
            max_read_waiters: 4096,
            max_record_bytes: 1 << 20,
            max_inflight_tasks: 4096,
            max_continuations: 4096,
            max_pending_commits: 4096,
            max_commit_records: 4096,
            max_async_delete_records: 4096,
            max_retry_tickets: 4096,
            max_ordered_keys: 4096,
            max_blocked_keys_per_source: 4096,
            max_deferred_records_per_key: 4096,
            max_deferred_records: 4096,
            max_unroutable_records: 4096,
        }
    }
}

impl PartitionLimits {
    /// 业务作用：计算整批预留策略下可同时保证所有执行后继的记录数。
    /// 参数说明: 无。
    /// 返回：每个记录各预留一个可能后继对象时的共同上限。
    pub(crate) fn record_limit(&self) -> usize {
        [
            self.max_inflight_records,
            self.max_inflight_tasks,
            self.max_continuations,
            self.max_pending_commits,
            self.max_commit_records,
            self.max_retry_tickets,
            self.max_ordered_keys,
            self.max_deferred_records,
            self.max_deferred_records_per_key,
            self.max_unroutable_records,
        ]
        .into_iter()
        .min()
        .unwrap_or(0)
    }

    /// 业务作用：拒绝无法预留一批合法响应的配置，避免读取后才发现后继责任无处登记。
    /// 参数说明：`batch` 为解析组覆盖后的 COUNT。
    /// 返回：所有数量非零且最坏批次可预留时成功；乘法溢出或预算不足返回配置错误。
    pub(crate) fn validate(&self, batch: usize) -> Result<()> {
        let bytes = batch.checked_mul(self.max_record_bytes);
        if batch == 0
            || self.record_limit() < batch
            || self.max_record_bytes == 0
            || self.max_active_batches == 0
            || self.max_read_waiters == 0
            || self.max_inflight_publishes == 0
            || self.max_async_delete_records == 0
            || self.max_blocked_keys_per_source < batch
            || self.max_deferred_records_per_key < batch
            || bytes.is_none_or(|n| n > self.max_inflight_payload_bytes)
            || self.max_record_bytes > self.max_inflight_publish_bytes
        {
            return Err(NasaRedisError::Config(
                "partition limits cannot reserve one complete batch and its continuations".into(),
            ));
        }
        Ok(())
    }
}

/// Redis 消费专属 runner 的本地调度配置；不参与 Redis wire 或物理分区布局。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionExecutorScope {
    /// 本源实例的全部组共用执行域。
    #[default]
    Source,
    /// 每个逻辑分区组使用独立执行域。
    Group,
    /// 每个物理 Stream 使用独立执行域。
    Stream,
}

/// 各执行域采用相同的本地槽与队列配置，总域数及槽数在激活前有界校验。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionExecutorCfg {
    /// 本地执行域按源、组或物理 Stream 划分的方式。
    pub scope: PartitionExecutorScope,
    /// 整个 RedisPartition 实例允许创建的执行域总数上限。
    pub max_runners: usize,
    /// 全部执行域的本地执行槽总数上限。
    pub max_total_partitions: usize,
    /// 每个执行域的本地执行槽数，不改变 Redis 物理分区。
    pub partitions: usize,
    /// 每个执行域内每种任务类型的队列容量。
    pub queue_capacity_per_type: usize,
    /// 单个执行域内所有任务类型共享的在途任务容量。
    pub global_inflight: usize,
    /// 单个执行域可保留的任务类型状态数量上限。
    pub max_type_states: usize,
}

impl Default for PartitionExecutorCfg {
    /// 业务作用：为 Redis 消费提供独立且有界的默认执行域。
    /// 参数说明: 无。
    /// 返回：八个本地执行槽，最多 4096 个任务和类型状态。
    fn default() -> Self {
        Self {
            scope: PartitionExecutorScope::Source,
            max_runners: 256,
            max_total_partitions: 4096,
            partitions: 8,
            queue_capacity_per_type: 256,
            global_inflight: 4096,
            max_type_states: 4096,
        }
    }
}

impl PartitionExecutorCfg {
    /// 业务作用：冻结 runner 配置并验证全部计划的最坏类型状态占用。
    /// 参数说明：`plans` 为不可变消费计划总数。
    /// 返回：规范化配置；空计划、数量溢出或类型容量不足时拒绝激活。
    pub(crate) fn runner_config(&self, plans: usize) -> Result<napart::RunnerConfig> {
        let config = napart::RunnerConfig {
            partitions: self.partitions,
            queue_capacity_per_type: self.queue_capacity_per_type,
            global_inflight: self.global_inflight,
            max_type_states: self.max_type_states,
            ..Default::default()
        }
        .validated()
        .map_err(|e| NasaRedisError::Config(e.to_string()))?;
        if plans == 0
            || config
                .partitions
                .checked_mul(plans)
                .and_then(|n| n.checked_mul(2))
                .is_none_or(|n| n > config.max_type_states)
        {
            return Err(NasaRedisError::Config(
                "partition executor max_type_states cannot cover all consumer plans".into(),
            ));
        }
        Ok(config)
    }
}
