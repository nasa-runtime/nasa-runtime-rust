//! Job 运行时配置：容量、时间与 Fanout 参数，以及一组必须在绑定端口前成立的启动硬门禁。
//!
//! `namespace + shard_count + fanout_bucket_count` 是持久布局，已有数据时任一变化都要求新 namespace。
//! 时间不等式保证租约续期、可见性提升与恢复之间不会出现"旧 owner 仍在写、新 attempt 已开始"的窗口。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{NasaRedisError, Result};
use crate::job::model::{JobPubSubMode, JobSerialOverflowPolicy};

/// 首版只支持的协议版本；registry 无协议范围、共享 Stream 无兼容消费者选择，禁止靠调高版本滚动升级。
const SUPPORTED_PROTOCOL_VERSION: u32 = 1;

/// Job 运行时配置；字段与既有 Job 属性对齐，缺省即生产可用的保守边界。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobConfig {
    /// 命名空间；持久布局的一部分。
    pub namespace: String,
    /// 协议版本；首版只接受 1。
    pub protocol_version: u32,
    /// 调度分片数；持久布局。
    pub shard_count: u32,
    /// Fanout 桶数；持久布局。
    pub fanout_bucket_count: u32,
    /// 稳定应用名，与实例身份共同构成节点身份。
    pub application_name: String,
    /// 稳定实例身份；受管 Fanout 模式要求显式配置。
    pub instance_identity: String,
    /// 单连接每秒扫描的最小间隔。
    pub min_scan_interval_ms: u64,
    /// 无到期任务时的最大扫描间隔。
    pub max_scan_interval_ms: u64,
    /// 单次扫描批量。
    pub scan_batch_size: u32,
    /// 调度判定允许的 Redis 往返预算。
    pub schedule_rtt_allowance_ms: u64,
    /// 单轮因并发推进而重新计算候选时刻的次数上限。
    pub need_recompute_max_retries: u32,
    /// 本节点在途 RPC/handler 的执行器容量。
    pub executor_capacity: u32,
    /// 并发 Handler 上限，必须不超过 `executor_capacity`。
    pub handler_capacity: u32,
    /// 服务端租约时长。
    pub lease_ms: u64,
    /// 租约续期周期。
    pub lease_renew_ms: u64,
    /// 续期 RTT 预算。
    pub renew_rtt_allowance_ms: u64,
    /// 时钟漂移预算。
    pub clock_drift_allowance_ms: u64,
    /// 允许的最大本地 GC 停顿预算。
    pub max_tolerated_gc_pause_ms: u64,
    /// 可见性超时；必须不小于两倍 `lease_ms`。
    pub visibility_timeout_ms: u64,
    /// 心跳周期。
    pub heartbeat_ms: u64,
    /// 执行器过期时长；必须不小于两倍 `heartbeat_ms`。
    pub executor_expire_ms: u64,
    /// PEL 接管的最小空闲阈值。
    pub xautoclaim_min_idle_ms: u64,
    /// 注册表 GC 宽限期。
    pub registry_gc_grace_ms: u64,
    /// Dispatch Stream 消费组名；同一能力 Stream 的全部执行器共用它接管 PEL。
    pub dispatch_group: String,
    /// 单个 Run 允许的最大执行时长；重试延迟抖动上限被它钳制。
    pub max_run_duration_ms: u64,
    /// Run 结果摘要写入前的最大字节，超出截断。
    pub max_result_summary_bytes: u32,
    /// 可见性提升重建 Dispatch 消息的最大派发次数，超出转终态。
    pub max_dispatch_attempts: u32,
    /// 串行队列积压上限；仅 `SERIAL_QUEUE` 并发生效。
    pub max_serial_backlog: u32,
    /// 串行队列溢出策略；仅 `SERIAL_QUEUE` 并发生效。
    pub serial_overflow_policy: JobSerialOverflowPolicy,
    /// Fanout 通知/回执 Pub/Sub 模式；决定投递用 SPUBLISH 还是 PUBLISH。
    pub pubsub_mode: JobPubSubMode,
    /// 已接收未 start 的 shard 普通唤醒上限；超出后按失败策略处置。
    pub ready_max_wakeups: u32,
    /// 当前目标连续等待本地执行容量的最长时间；超出后重新开放根失败策略出口。
    pub fanout_capacity_wait_ms: u64,
    /// 单个 shard 允许的最大重分配次数；超出即判无可用执行器。
    pub fanout_max_assignments: u32,
    /// 判定稳定节点 Fanout 失联所需的不同根证据数；必须大于 1，避免单根误判。
    pub node_unready_evidence_count: u32,
    /// Run 历史保留期。
    pub run_retention_ms: u64,
    /// Completion Stream 的最小时间保留窗口。
    pub completion_retention_ms: u64,
    /// tombstone 保留期。
    pub tombstone_retention_ms: u64,
    /// Fanout 单批最大成员数。
    pub fanout_max_members: u32,
    /// Fanout 投递批量。
    pub fanout_delivery_batch_size: u32,
    /// Fanout 创建超时；必须小于 `fanout_max_wait_ms`。
    pub fanout_create_timeout_ms: u64,
    /// Fanout 根等待上限；必须小于 `registry_gc_grace_ms`。
    pub fanout_max_wait_ms: u64,
    /// Fanout 保留期。
    pub fanout_retention_ms: u64,
    /// Fanout 清理批量。
    pub fanout_cleanup_batch_size: u32,
    /// 单个 payload 参数最大字节。
    pub max_parameter_bytes: u32,
    /// 单轮误触发补偿最多建立的 Run 数。
    pub max_catch_up_runs: u32,
    /// 允许补偿的历史逻辑时刻窗口。
    pub max_catch_up_window_ms: u64,
    /// Fanout 全批参数累计上限；必须不小于 `max_parameter_bytes`。
    pub fanout_max_total_parameter_bytes: u32,
    /// Completion Stream 的长度上界。
    pub completion_max_len: u64,
    /// Completion Stream 单轮时间裁剪的删除上限。
    pub completion_trim_batch_size: u32,
    /// JSON 线编码是否允许默认类型信息；固定为 false，出现 true 拒绝启动。
    pub json_default_typing: bool,
    /// 每个实际使用 source 的稀疏覆盖；未被定义引用的条目不创建运行时。
    #[serde(default, skip_serializing)]
    pub sources: BTreeMap<String, JobSourceConfig>,
}

/// 单个 Job source 的稀疏配置覆盖；未知字段在合并后按 `JobConfig` 合同拒绝。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct JobSourceConfig {
    /// 显式关闭的 source 被计划引用时拒绝启动。
    pub enabled: Option<bool>,
    /// 除 `enabled` 外的 JobConfig 字段覆盖。
    #[serde(flatten)]
    pub overrides: BTreeMap<String, serde_json::Value>,
}

/// Job 根配置的拥有式 builder；默认值与反序列化路径相同，最终仍统一经过 `validate`。
pub struct JobConfigBuilder {
    config: JobConfig,
}

/// 单个 source 稀疏覆盖 builder；只记录业务显式设置的字段，未设置项继承根配置。
pub struct JobSourceConfigBuilder {
    source: JobSourceConfig,
}

impl Default for JobConfig {
    /// 业务作用：提供容量、时间与 Fanout 都有界的保守默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：满足全部启动不等式的默认 Job 配置。
    fn default() -> Self {
        Self {
            namespace: String::new(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION,
            shard_count: 64,
            fanout_bucket_count: 32,
            application_name: "application".to_owned(),
            instance_identity: String::new(),
            min_scan_interval_ms: 50,
            max_scan_interval_ms: 30_000,
            scan_batch_size: 100,
            schedule_rtt_allowance_ms: 100,
            need_recompute_max_retries: 3,
            executor_capacity: 128,
            handler_capacity: 8,
            lease_ms: 30_000,
            lease_renew_ms: 10_000,
            renew_rtt_allowance_ms: 1_000,
            clock_drift_allowance_ms: 1_000,
            max_tolerated_gc_pause_ms: 1_000,
            visibility_timeout_ms: 60_000,
            heartbeat_ms: 10_000,
            executor_expire_ms: 30_000,
            xautoclaim_min_idle_ms: 30_000,
            registry_gc_grace_ms: 3_600_000,
            dispatch_group: "redis-job-executor".to_owned(),
            max_run_duration_ms: 3_600_000,
            max_result_summary_bytes: 4_096,
            max_dispatch_attempts: 20,
            max_serial_backlog: 1_000,
            serial_overflow_policy: JobSerialOverflowPolicy::SkipOldest,
            pubsub_mode: JobPubSubMode::Sharded,
            ready_max_wakeups: 3,
            fanout_capacity_wait_ms: 10_000,
            fanout_max_assignments: 5,
            node_unready_evidence_count: 3,
            run_retention_ms: 604_800_000,
            completion_retention_ms: 604_800_000,
            tombstone_retention_ms: 604_800_000,
            fanout_max_members: 512,
            fanout_delivery_batch_size: 64,
            fanout_create_timeout_ms: 60_000,
            fanout_max_wait_ms: 1_800_000,
            fanout_retention_ms: 604_800_000,
            fanout_cleanup_batch_size: 100,
            max_parameter_bytes: 65_536,
            max_catch_up_runs: 50,
            max_catch_up_window_ms: 3_600_000,
            fanout_max_total_parameter_bytes: 16 * 1024 * 1024,
            completion_max_len: 100_000,
            completion_trim_batch_size: 1_000,
            json_default_typing: false,
            sources: BTreeMap::new(),
        }
    }
}

impl JobConfig {
    /// 业务作用：创建使用保守默认值的拥有式配置 builder。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可设置根级字段与逐 source 稀疏覆盖的 builder。
    pub fn builder() -> JobConfigBuilder {
        JobConfigBuilder {
            config: Self::default(),
        }
    }

    /// 业务作用：在建立任何连接或后台任务前校验全部持久布局与时间不等式，任一不成立拒绝启动。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部门禁成立时成功；协议版本、容量、时间关系或类型开关非法时返回配置错误。
    pub fn validate(&self) -> Result<()> {
        for (qualifier, source) in &self.sources {
            crate::job::source::JobSourceId::new(qualifier)?;
            if source.overrides.contains_key("application_name")
                || source.overrides.contains_key("instance_identity")
            {
                return Err(cfg(
                    "job source 不得覆盖 application_name 或 instance_identity；执行器身份必须在进程内一致",
                ));
            }
        }
        if self.protocol_version != SUPPORTED_PROTOCOL_VERSION {
            return Err(cfg("job.protocol_version 首版只支持 1"));
        }
        if self.shard_count == 0 || self.fanout_bucket_count == 0 {
            return Err(cfg("job.shard_count 与 fanout_bucket_count 必须大于 0"));
        }
        // 默认类型信息会让 JSON 反序列化根据 @class 动态选类型，是远程代码执行面，固定禁用。
        if self.json_default_typing {
            return Err(cfg("job.json_default_typing 必须为 false"));
        }
        if self.executor_capacity == 0
            || self.handler_capacity == 0
            || self.handler_capacity > self.executor_capacity
        {
            return Err(cfg(
                "job.executor_capacity 与 handler_capacity 必须满足 1 <= handler_capacity <= executor_capacity",
            ));
        }
        if self.min_scan_interval_ms == 0
            || self.max_scan_interval_ms < self.min_scan_interval_ms
            || self.scan_batch_size == 0
            || self.need_recompute_max_retries == 0
        {
            return Err(cfg("job 扫描间隔、批量与重新计算上限非法"));
        }
        if self.fanout_max_members == 0
            || self.fanout_delivery_batch_size == 0
            || self.fanout_delivery_batch_size > self.fanout_max_members
            || self.fanout_max_assignments == 0
        {
            return Err(cfg("job Fanout 成员、投递批量与 assignment 上限非法"));
        }
        if self.max_parameter_bytes == 0
            || self.fanout_max_total_parameter_bytes == 0
            || self.max_parameter_bytes > self.fanout_max_total_parameter_bytes
        {
            return Err(cfg("job Fanout 参数字节上限非法"));
        }
        if self.fanout_capacity_wait_ms == 0
            || self.node_unready_evidence_count <= 1
            || self.fanout_cleanup_batch_size == 0
        {
            return Err(cfg("job Fanout 容量等待、失联证据与清理批量非法"));
        }
        if self.fanout_create_timeout_ms == 0
            || self.fanout_max_wait_ms <= self.fanout_create_timeout_ms
            || self.fanout_retention_ms == 0
            || self.registry_gc_grace_ms <= self.fanout_max_wait_ms
        {
            return Err(cfg("job Fanout 时限与注册表回收宽限关系非法"));
        }
        // 租约必须覆盖两次续期加一次可容忍停顿，否则续期尚未完成租约已过期，旧 owner 会被误判失权。
        if self.lease_ms == 0 || self.lease_renew_ms == 0 {
            return Err(cfg("job.lease_ms 与 lease_renew_ms 必须大于 0"));
        }
        let two_renew = self
            .lease_renew_ms
            .checked_mul(2)
            .and_then(|v| v.checked_add(self.max_tolerated_gc_pause_ms))
            .ok_or_else(|| cfg("job 租约时间关系溢出"))?;
        if self.lease_ms <= two_renew {
            return Err(cfg(
                "job.lease_ms 必须大于 2*lease_renew_ms + max_tolerated_gc_pause_ms",
            ));
        }
        // 续期 RTT 与时钟漂移预算之和必须远小于租约，给本地保守持权截止点留出安全余量。
        let authority_allowance = self
            .renew_rtt_allowance_ms
            .checked_add(self.clock_drift_allowance_ms)
            .ok_or_else(|| cfg("job 续期与时钟预算相加溢出"))?;
        if authority_allowance >= self.lease_ms / 3 {
            return Err(cfg("job renew_rtt+clock_drift 预算必须小于 lease_ms/3"));
        }
        let two_leases = self
            .lease_ms
            .checked_mul(2)
            .ok_or_else(|| cfg("job.visibility_timeout_ms 时间关系溢出"))?;
        if self.visibility_timeout_ms < two_leases {
            return Err(cfg("job.visibility_timeout_ms 必须不小于 2*lease_ms"));
        }
        if self.heartbeat_ms == 0 {
            return Err(cfg("job.heartbeat_ms 必须大于 0"));
        }
        let two_heartbeats = self
            .heartbeat_ms
            .checked_mul(2)
            .ok_or_else(|| cfg("job.executor_expire_ms 时间关系溢出"))?;
        if self.executor_expire_ms < two_heartbeats {
            return Err(cfg("job.executor_expire_ms 必须不小于 2*heartbeat_ms"));
        }
        // 扫描间隔、PEL 接管阈值与可见性超时必须严格递增，避免同一 Run 被扫描与接管路径重复处理。
        if !(self.min_scan_interval_ms < self.xautoclaim_min_idle_ms
            && self.xautoclaim_min_idle_ms < self.visibility_timeout_ms)
        {
            return Err(cfg(
                "job 必须满足 min_scan_interval_ms < xautoclaim_min_idle_ms < visibility_timeout_ms",
            ));
        }
        // 执行时长上限用于钳制重试延迟抖动，为零会让抖动上溢并可能压过 base，因此必须为正。
        if self.max_run_duration_ms == 0 {
            return Err(cfg("job.max_run_duration_ms 必须大于 0"));
        }
        if self.max_result_summary_bytes == 0 {
            return Err(cfg("job.max_result_summary_bytes 必须大于 0"));
        }
        if self.run_retention_ms == 0
            || self.completion_retention_ms == 0
            || self.tombstone_retention_ms == 0
            || self.max_catch_up_runs == 0
            || self.max_catch_up_window_ms == 0
        {
            return Err(cfg("job 的 Run 保留期与补偿边界必须大于 0"));
        }
        if self.max_dispatch_attempts == 0 {
            return Err(cfg("job.max_dispatch_attempts 必须大于 0"));
        }
        if self.max_serial_backlog == 0 {
            return Err(cfg("job.max_serial_backlog 必须大于 0"));
        }
        if self.dispatch_group.is_empty() {
            return Err(cfg("job.dispatch_group 不能为空"));
        }
        if self.completion_retention_ms < self.fanout_max_wait_ms
            || self.completion_max_len == 0
            || self.completion_trim_batch_size == 0
        {
            return Err(cfg(
                "job Completion 时间窗必须覆盖 fanout_max_wait_ms，长度与裁剪批量必须大于 0",
            ));
        }
        Ok(())
    }

    /// 业务作用：把根级默认与指定 source 的稀疏覆盖合并成独立冻结配置。
    ///
    /// 参数说明：
    /// - `qualifier`: 计划实际引用的 canonical source id。
    /// - `client_namespace`: 该 RedisClient 自身命名空间；根级未配置 namespace 时作为默认值。
    ///
    /// 返回：覆盖合并且全部门禁通过时返回配置；source 被禁用、字段未知或类型非法时返回配置错误。
    pub fn resolve_source(&self, qualifier: &str, client_namespace: &str) -> Result<Self> {
        let source = self.sources.get(qualifier);
        if source.and_then(|value| value.enabled) == Some(false) {
            return Err(crate::job::JobError::SourceDisabled(qualifier.to_owned()).into());
        }
        let mut value = serde_json::to_value(self)
            .map_err(|error| cfg(&format!("job 根配置无法规范化: {error}")))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| cfg("job 根配置必须是对象"))?;
        if self.namespace.trim().is_empty() {
            object.insert(
                "namespace".to_owned(),
                serde_json::Value::String(client_namespace.to_owned()),
            );
        }
        if let Some(source) = source {
            for (key, override_value) in &source.overrides {
                object.insert(key.clone(), override_value.clone());
            }
        }
        let mut resolved: JobConfig = serde_json::from_value(value)
            .map_err(|error| cfg(&format!("job source {qualifier} 配置非法: {error}")))?;
        resolved.sources.clear();
        resolved.validate()?;
        Ok(resolved)
    }
}

impl JobConfigBuilder {
    /// 业务作用：设置全部实际 source 的默认 Job namespace。
    ///
    /// 参数说明：`namespace` 为持久键布局的一部分。
    ///
    /// 返回：更新后的 builder。
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.config.namespace = namespace.into();
        self
    }

    /// 业务作用：设置执行器稳定应用名，参与节点身份派生。
    ///
    /// 参数说明：`application_name` 为部署稳定名称。
    ///
    /// 返回：更新后的 builder。
    pub fn application_name(mut self, application_name: impl Into<String>) -> Self {
        self.config.application_name = application_name.into();
        self
    }

    /// 业务作用：设置跨进程重启稳定的实例身份，供执行器与 Fanout 定向路由使用。
    ///
    /// 参数说明：`instance_identity` 为当前副本稳定标识。
    ///
    /// 返回：更新后的 builder。
    pub fn instance_identity(mut self, instance_identity: impl Into<String>) -> Self {
        self.config.instance_identity = instance_identity.into();
        self
    }

    /// 业务作用：为一个 canonical source 追加稀疏覆盖，不复制根级默认或建立 Redis 连接。
    ///
    /// 参数说明：
    /// - `qualifier`: 计划使用的 source id。
    /// - `configure`: 从空覆盖 builder 生成显式覆盖字段。
    ///
    /// 返回：更新后的根配置 builder；source 重复或名称非法时返回错误。
    pub fn source<F>(mut self, qualifier: &str, configure: F) -> Result<Self>
    where
        F: FnOnce(JobSourceConfigBuilder) -> JobSourceConfigBuilder,
    {
        let source_id = crate::job::source::JobSourceId::new(qualifier)?;
        if self.config.sources.contains_key(source_id.as_str()) {
            return Err(cfg(&format!("job source {} 重复配置", source_id.as_str())));
        }
        let source = configure(JobSourceConfigBuilder {
            source: JobSourceConfig::default(),
        })
        .source;
        self.config
            .sources
            .insert(source_id.as_str().to_owned(), source);
        Ok(self)
    }

    /// 业务作用：冻结根配置；完整逐 source 校验延后到绑定各 RedisClient namespace 后执行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：根级不变量成立时返回配置；容量或时间门禁不成立时返回错误。
    pub fn build(self) -> Result<JobConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}

impl JobSourceConfigBuilder {
    /// 业务作用：显式启用或禁用当前 source；被计划引用且禁用时 prepare 拒绝。
    ///
    /// 参数说明：`enabled` 为部署门禁。
    ///
    /// 返回：更新后的稀疏 builder。
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.source.enabled = Some(enabled);
        self
    }

    /// 业务作用：覆盖当前 source 的 Job namespace，不影响 RedisClient 自身通用 namespace。
    ///
    /// 参数说明：`namespace` 为该 source 的持久 Job 布局名称。
    ///
    /// 返回：更新后的稀疏 builder。
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.source.overrides.insert(
            "namespace".to_owned(),
            serde_json::Value::String(namespace.into()),
        );
        self
    }

    /// 业务作用：覆盖当前 source 的本地 Handler 并发上限。
    ///
    /// 参数说明：`capacity` 为正整数容量。
    ///
    /// 返回：更新后的稀疏 builder；最终与 executor_capacity 的关系由 `build/resolve_source` 校验。
    pub fn handler_capacity(mut self, capacity: u32) -> Self {
        self.source.overrides.insert(
            "handler_capacity".to_owned(),
            serde_json::Value::from(capacity),
        );
        self
    }
}

/// 业务作用：构造带 `job` 前缀的配置错误，便于业务定位启动门禁问题。
///
/// 参数说明：
/// - `message`: 稳定错误摘要。
///
/// 返回：`JobError::Config` 错误。
fn cfg(message: &str) -> NasaRedisError {
    crate::job::JobError::Config(message.to_owned()).into()
}
