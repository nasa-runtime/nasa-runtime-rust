//! 组件配置:强类型字段 + 三端透传 + 启动期 fail-fast 校验。
//!
//! 命名约定:字段按 librdkafka 原生语义设计(与参照实现字段名的差异);
//! 不做 serde rename,YAML 键 = 字段名 snake_case(与 nadis 一致)。
//! 默认值全部继承参照实现,运维平迁零认知成本。

use std::collections::BTreeMap;
use std::fmt;

use serde::Deserialize;

use crate::error::{NafkaError, Result};

/// [`KafkaConfig`] 可由上层受管容器传入的数据面字段全集。
///
/// 上层容器复用这份常量做严格字段校验，确保新增配置字段时只有一个需要同步的定义，
/// 同时仍能在 Kafka 连接发生前拒绝拼写错误。
#[doc(hidden)]
pub const KAFKA_CONFIG_FIELDS: &[&str] = &[
    "client_name",
    "bootstrap_servers",
    "group_id",
    "client_id_prefix",
    "producer",
    "consumer",
    "admin",
    "behavior",
    "security",
    "properties",
];

/// nafka 顶层配置,应用把它内嵌进自己的 AppConfig 后交给 `KafkaProxy::connect`。
///
/// 配置应用顺序(后者优先):common 透传 → 端级透传 → 强类型字段 → 框架不变量;
/// 框架不变量(enable.auto.commit=false 等)不可被任何透传覆盖,validate 直接拒绝。
#[derive(Clone, Deserialize)]
pub struct KafkaConfig {
    /// 本 Kafka 客户端实例名:多集群/多 KafkaProxy 时区分 linkme 收集项归属,
    /// 并参与 client.id 派生。默认 "default"。
    #[serde(default = "default_client_name")]
    pub client_name: String,
    /// 集群地址,逗号分隔,必填非空。
    pub bootstrap_servers: String,
    /// 默认消费 group:consumer 声明 GroupSpec::Default 时回退此值;两者皆空在 start() fail-fast。
    #[serde(default)]
    pub group_id: Option<String>,
    /// client.id 前缀;最终 client.id = 前缀 + client_name + lane 名/端角色 稳定派生。
    #[serde(default)]
    pub client_id_prefix: Option<String>,
    /// producer 侧配置(含命名 lane)。
    #[serde(default)]
    pub producer: ProducerConfig,
    /// consumer 侧配置。
    #[serde(default)]
    pub consumer: ConsumerConfig,
    /// admin 侧配置。
    #[serde(default)]
    pub admin: AdminConfig,
    /// 框架行为配置(非 librdkafka 键):聚批、重试、DLT、commit 批量等。
    #[serde(default)]
    pub behavior: BehaviorConfig,
    /// 安全配置;Debug 输出对凭据打码。
    #[serde(default)]
    pub security: SecurityConfig,
    /// 三端(producer/consumer/admin)共同透传的原生键;不得覆盖框架不变量。
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// 为反序列化缺省项提供稳定客户端名。
fn default_client_name() -> String {
    "default".to_string()
}

/// producer 配置:顶层字段构成 `default` lane,命名 lane 经 [`ProducerLaneOverride`] 继承并覆盖。
///
/// 每个命名 lane 是独立的原生 producer/队列/admission/observer(物理隔离),
/// 实现不得为省连接把多个 lane 合并回同一个原生 producer。
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct ProducerConfig {
    /// ack 策略("0"/"1"/"all");默认 "1" 与参照实现一致。需要副本级持久性的场景显式配 "all"。
    pub acks: String,
    /// 发送失败重试次数；默认值必须显式设为 3，避免继承底层库的 i32::MAX 而静默偏离。
    pub retries: u32,
    /// 重试退避毫秒。
    pub retry_backoff_ms: u64,
    /// 攒批等待毫秒:>0 提升吞吐、增加延迟。
    pub linger_ms: u64,
    /// 单批字节上限,达到即发不等 linger。
    pub batch_size: usize,
    /// 单请求超时毫秒。
    pub request_timeout_ms: u64,
    /// 投递总超时毫秒(含重试);validate 要求 >= request_timeout_ms + linger_ms。
    pub delivery_timeout_ms: u64,
    /// 连续没有成功 delivery 达到该时长后，允许把非 fatal 失活实例切到新 generation。
    pub stalled_rebuild_after_ms: u64,
    /// 触发非 fatal generation 切换前至少观察到的连续终态 delivery failure 数。
    pub stalled_rebuild_min_failures: u32,
    /// 两次非 fatal generation 切换之间的最短冷却毫秒数。
    pub stalled_rebuild_cooldown_ms: u64,
    /// 分区器;默认 murmur2_random 与参照实现的默认分区器逐 key 一致,
    /// 改动会导致同 key 跨语言落不同分区,覆盖时 validate 打高可见告警。
    pub partitioner: String,
    /// 压缩算法(none/gzip/snappy/lz4/zstd;zstd 需开同名 feature)。
    pub compression: String,
    /// 幂等 producer 开关；开启时 validate 校验 acks/retries/in-flight 约束。
    pub enable_idempotence: bool,
    /// 单连接最大并行在途请求数；默认 1，在启用重试时锁住同分区发送顺序。
    pub max_in_flight_requests_per_connection: u32,
    /// fire() 的 delivery 观察队列容量:先预留后入队,防"已发送却不可观察"窗口。
    pub fire_observer_capacity: usize,
    /// 原生入队队列的最大消息数；队满时退避重试至 publish 超时，避免无限阻塞。
    pub queue_buffering_max_messages: usize,
    /// 原生入队队列的最大 KiB 数;socket lane 必须显式配置而非继承 1 GiB 通用默认。
    pub queue_buffering_max_kbytes: usize,
    /// 命名 lane 覆盖表;键为 lane 名("default" 保留,不得出现在此表)。
    pub lanes: BTreeMap<String, ProducerLaneOverride>,
    /// 只透传给 producer 的原生键。
    pub properties: BTreeMap<String, String>,
}

impl Default for ProducerConfig {
    /// 返回兼容基线且受框架上限约束的 default lane 配置。
    fn default() -> Self {
        Self {
            acks: "1".into(),
            retries: 3,
            retry_backoff_ms: 100,
            linger_ms: 5,
            batch_size: 16384,
            request_timeout_ms: 30_000,
            delivery_timeout_ms: 120_000,
            stalled_rebuild_after_ms: 60_000,
            stalled_rebuild_min_failures: 2,
            stalled_rebuild_cooldown_ms: 60_000,
            partitioner: "murmur2_random".into(),
            compression: "none".into(),
            enable_idempotence: false,
            max_in_flight_requests_per_connection: 1,
            fire_observer_capacity: 10_000,
            queue_buffering_max_messages: 100_000,
            queue_buffering_max_kbytes: 1_048_576,
            lanes: BTreeMap::new(),
            properties: BTreeMap::new(),
        }
    }
}

/// 命名 lane 的覆盖项:全部可选,未设置的字段继承 default lane。
///
/// 只开放 producer 相关字段:lane 是发布侧物理隔离概念,不承载 consumer/admin 语义。
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct ProducerLaneOverride {
    /// 覆盖 ack 策略;socket control lane 生产环境强制 "all"。
    pub acks: Option<String>,
    /// 覆盖重试次数。
    pub retries: Option<u32>,
    /// 覆盖攒批等待毫秒。
    pub linger_ms: Option<u64>,
    /// 覆盖单批字节上限。
    pub batch_size: Option<usize>,
    /// 覆盖单请求超时毫秒。
    pub request_timeout_ms: Option<u64>,
    /// 覆盖投递总超时毫秒。
    pub delivery_timeout_ms: Option<u64>,
    /// 覆盖非 fatal 无成功 delivery 的 generation 切换时长。
    pub stalled_rebuild_after_ms: Option<u64>,
    /// 覆盖非 fatal generation 切换前的连续失败数。
    pub stalled_rebuild_min_failures: Option<u32>,
    /// 覆盖非 fatal generation 切换冷却时长。
    pub stalled_rebuild_cooldown_ms: Option<u64>,
    /// 覆盖压缩算法。
    pub compression: Option<String>,
    /// 覆盖幂等开关。
    pub enable_idempotence: Option<bool>,
    /// 覆盖单连接最大并行在途请求数。
    pub max_in_flight_requests_per_connection: Option<u32>,
    /// 覆盖 fire 观察队列容量。
    pub fire_observer_capacity: Option<usize>,
    /// 覆盖原生队列最大消息数。
    pub queue_buffering_max_messages: Option<usize>,
    /// 覆盖原生队列最大 KiB。
    pub queue_buffering_max_kbytes: Option<usize>,
    /// 只作用于本 lane 的原生键透传。
    pub properties: BTreeMap<String, String>,
}

/// consumer 配置:group 会话/拉取参数 + assignment 规模上限。
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct ConsumerConfig {
    /// 会话超时毫秒:超过未心跳被踢出 group。
    pub session_timeout_ms: u64,
    /// 心跳间隔毫秒;validate 要求 < session_timeout_ms。
    pub heartbeat_interval_ms: u64,
    /// 两次 poll 最大间隔毫秒:组内 handler 累计耗时的容量预算上限。
    pub max_poll_interval_ms: u64,
    /// 无提交位点时的起始策略(latest/earliest/none);广播 group 每次启动都命中此值。
    pub auto_offset_reset: String,
    /// 单次 fetch 最小字节。
    pub fetch_min_bytes: usize,
    /// 单次 fetch 最大等待毫秒;注意原生键名词序为 fetch.wait.max.ms。
    pub fetch_wait_max_ms: u64,
    /// 单分区单次 fetch 最大字节。
    pub max_partition_fetch_bytes: usize,
    /// 单 group 可接受的最大 assignment 分区数:超过即 Crashed/AssignmentTooLarge,
    /// 防止意外订阅巨型 topic 导致 per-partition 状态内存放大。
    pub max_assignment_partitions: usize,
    /// 只透传给 consumer 的原生键。
    pub properties: BTreeMap<String, String>,
}

impl Default for ConsumerConfig {
    /// 返回兼容基线的 group 会话、拉取与 assignment 上限配置。
    fn default() -> Self {
        Self {
            session_timeout_ms: 30_000,
            heartbeat_interval_ms: 10_000,
            max_poll_interval_ms: 300_000,
            auto_offset_reset: "latest".into(),
            fetch_min_bytes: 1,
            fetch_wait_max_ms: 500,
            max_partition_fetch_bytes: 1024 * 1024,
            max_assignment_partitions: 10_000,
            properties: BTreeMap::new(),
        }
    }
}

/// admin 配置:topic 管理与元数据查询的超时链。
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    /// admin 单请求超时毫秒;None 回退 default_api_timeout_ms → publish_timeout_ms。
    pub request_timeout_ms: Option<u64>,
    /// admin API 总超时毫秒;None 回退 publish_timeout_ms。
    pub default_api_timeout_ms: Option<u64>,
    /// 只透传给 admin 的原生键。
    pub properties: BTreeMap<String, String>,
}

/// 原生透传表的安全 Debug 视图；键用于定位，所有值统一打码，避免未知扩展键泄密。
struct MaskedProperties<'a>(&'a BTreeMap<String, String>);

impl fmt::Debug for MaskedProperties<'_> {
    /// 只输出属性键和固定掩码。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = formatter.debug_map();
        for key in self.0.keys() {
            map.entry(key, &"***");
        }
        map.finish()
    }
}

impl fmt::Debug for KafkaConfig {
    /// 输出可排障的顶层配置，同时让每层原生透传值都经过打码。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaConfig")
            .field("client_name", &self.client_name)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("group_id", &self.group_id)
            .field("client_id_prefix", &self.client_id_prefix)
            .field("producer", &self.producer)
            .field("consumer", &self.consumer)
            .field("admin", &self.admin)
            .field("behavior", &self.behavior)
            .field("security", &self.security)
            .field("properties", &MaskedProperties(&self.properties))
            .finish()
    }
}

impl fmt::Debug for ProducerConfig {
    /// 输出 producer 强类型参数并遮蔽透传值。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerConfig")
            .field("acks", &self.acks)
            .field("retries", &self.retries)
            .field("retry_backoff_ms", &self.retry_backoff_ms)
            .field("linger_ms", &self.linger_ms)
            .field("batch_size", &self.batch_size)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("delivery_timeout_ms", &self.delivery_timeout_ms)
            .field("stalled_rebuild_after_ms", &self.stalled_rebuild_after_ms)
            .field(
                "stalled_rebuild_min_failures",
                &self.stalled_rebuild_min_failures,
            )
            .field(
                "stalled_rebuild_cooldown_ms",
                &self.stalled_rebuild_cooldown_ms,
            )
            .field("partitioner", &self.partitioner)
            .field("compression", &self.compression)
            .field("enable_idempotence", &self.enable_idempotence)
            .field(
                "max_in_flight_requests_per_connection",
                &self.max_in_flight_requests_per_connection,
            )
            .field("fire_observer_capacity", &self.fire_observer_capacity)
            .field(
                "queue_buffering_max_messages",
                &self.queue_buffering_max_messages,
            )
            .field(
                "queue_buffering_max_kbytes",
                &self.queue_buffering_max_kbytes,
            )
            .field("lanes", &self.lanes)
            .field("properties", &MaskedProperties(&self.properties))
            .finish()
    }
}

impl fmt::Debug for ProducerLaneOverride {
    /// 输出 lane 覆盖参数并遮蔽透传值。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerLaneOverride")
            .field("acks", &self.acks)
            .field("retries", &self.retries)
            .field("linger_ms", &self.linger_ms)
            .field("batch_size", &self.batch_size)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("delivery_timeout_ms", &self.delivery_timeout_ms)
            .field("stalled_rebuild_after_ms", &self.stalled_rebuild_after_ms)
            .field(
                "stalled_rebuild_min_failures",
                &self.stalled_rebuild_min_failures,
            )
            .field(
                "stalled_rebuild_cooldown_ms",
                &self.stalled_rebuild_cooldown_ms,
            )
            .field("compression", &self.compression)
            .field("enable_idempotence", &self.enable_idempotence)
            .field(
                "max_in_flight_requests_per_connection",
                &self.max_in_flight_requests_per_connection,
            )
            .field("fire_observer_capacity", &self.fire_observer_capacity)
            .field(
                "queue_buffering_max_messages",
                &self.queue_buffering_max_messages,
            )
            .field(
                "queue_buffering_max_kbytes",
                &self.queue_buffering_max_kbytes,
            )
            .field("properties", &MaskedProperties(&self.properties))
            .finish()
    }
}

impl fmt::Debug for ConsumerConfig {
    /// 输出 consumer 强类型参数并遮蔽透传值。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerConfig")
            .field("session_timeout_ms", &self.session_timeout_ms)
            .field("heartbeat_interval_ms", &self.heartbeat_interval_ms)
            .field("max_poll_interval_ms", &self.max_poll_interval_ms)
            .field("auto_offset_reset", &self.auto_offset_reset)
            .field("fetch_min_bytes", &self.fetch_min_bytes)
            .field("fetch_wait_max_ms", &self.fetch_wait_max_ms)
            .field("max_partition_fetch_bytes", &self.max_partition_fetch_bytes)
            .field("max_assignment_partitions", &self.max_assignment_partitions)
            .field("properties", &MaskedProperties(&self.properties))
            .finish()
    }
}

impl fmt::Debug for AdminConfig {
    /// 输出 admin 超时参数并遮蔽透传值。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminConfig")
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("default_api_timeout_ms", &self.default_api_timeout_ms)
            .field("properties", &MaskedProperties(&self.properties))
            .finish()
    }
}

/// 无匹配 route 消息的处理策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnmatchedPolicy {
    /// 跳过并前移（默认）。不前移会让 group 位点永久卡住，因此不保留该缺陷语义。
    Skip,
    /// 转发 DLT 后前移:需要审计"谁在发无主消息"的场景。
    DeadLetter,
    /// 暂停该分区并保留位点:严格环境下的显式人工介入模式。
    Halt,
}

/// 格式/协议确定无效记录(坏路由、缺 payload 等)的处理策略:
/// 这类记录重试必然再失败,不消耗业务 handler 的重试次数。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidRecordPolicy {
    /// 直接进 DLT(默认);要求 DLT 后缀非空,仍受 dead_letter_required 约束。
    DeadLetter,
    /// 暂停分区等待人工处理。
    Halt,
}

/// 框架行为配置:聚批/重试/DLT/commit 批量/关停等,全部为 nafka 自有语义(非透传键)。
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct BehaviorConfig {
    /// 单个逻辑 poll 批的最大记录数(客户端聚批,替代原生缺失的 max.poll.records)。
    pub max_batch_records: usize,
    /// 单个逻辑 poll 批的最大保留字节(RawRecord::retained_bytes 口径)。
    pub max_batch_bytes: usize,
    /// 单条记录的最大保留字节:超限不复制不提交,分区置 Halt(OversizeRecord)。
    pub max_record_bytes: usize,
    /// 每分区 in-flight outcome 上限;validate 要求 >= commit_batch_records + max_batch_records。
    pub max_progress_records: usize,
    /// 每 group in-flight outcome 总上限(跨分区 permit 池);validate 要求 >= max_progress_records。
    pub max_group_progress_records: usize,
    /// 正常流量的 commit 时间触发毫秒；安全水位逐条推进，broker commit 按数量或时间批量提交。
    pub commit_interval_ms: u64,
    /// 正常流量的 commit 记录数触发。
    pub commit_batch_records: usize,
    /// poll 首条等待毫秒(无消息时回循环顶处理命令/订阅变更)。
    pub poll_timeout_ms: u64,
    /// start() 后延迟开始拉取的毫秒,留出业务组件完全就绪的时间;等待可被 stop 打断。
    pub start_consume_delay_ms: u64,
    /// transient 崩溃后重建 consumer 的初始退避毫秒(指数退避 + 抖动)。
    pub consume_restart_delay_ms: u64,
    /// 重建退避的最大毫秒。
    pub consume_restart_max_delay_ms: u64,
    /// 滑动窗口内允许的重启次数;超出进入 Crashed 等待显式 restart。
    pub consume_restart_budget_times: u32,
    /// 重启预算的滑动窗口毫秒。
    pub consume_restart_budget_window_ms: u64,
    /// 失败分区重投前的暂停毫秒。
    pub consume_retry_backoff_ms: u64,
    /// revoke 窗口内同步 commit 的超时毫秒;validate 要求 < session_timeout_ms。
    pub rebalance_commit_timeout_ms: u64,
    /// 单个失败单元的最大消费尝试次数;0 = 无限重试不进 DLT。
    pub max_consume_attempts: u32,
    /// attempt 状态表硬上限:打满说明状态泄漏或毒消息洪峰,group 置 Degraded。
    pub max_retry_entries: usize,
    /// DLT topic 后缀;空串 = 禁用 DLT 转发。
    pub dead_letter_topic_suffix: String,
    /// DLT 转发使用的 producer lane;必须存在,且 socket 场景不得等于 control lane。
    pub dlt_producer_lane: String,
    /// durability-first 开关:true 时 DLT 未确认绝不前移原 offset;false 显式选择可用性优先(丢消息)。
    pub dead_letter_required: bool,
    /// DLT 整条 pipeline 保留的 envelope 数量上限，覆盖 queue、in-flight、completion 与 retry。
    pub dlt_queue_capacity: usize,
    /// DLT 全 pipeline 保留 payload 字节上限;validate 要求 >= max(max_batch_bytes, max_record_bytes)。
    pub dlt_queue_max_bytes: usize,
    /// DLT 并行投递上限。
    pub dlt_max_in_flight: usize,
    /// DLT 失败外层重试的初始退避毫秒。
    pub dlt_retry_initial_ms: u64,
    /// DLT 失败外层重试的封顶退避毫秒。
    pub dlt_retry_max_ms: u64,
    /// DLT 连续失败达到该次数后 group 置 Degraded 持续告警(仍按封顶间隔重试)。
    pub dlt_degraded_after_attempts: u32,
    /// 无匹配 route 消息的策略。
    pub unmatched_policy: UnmatchedPolicy,
    /// 确定无效记录的策略。
    pub invalid_record_policy: InvalidRecordPolicy,
    /// 连续解码失败达到该次数后把分区 Halt，防止"解码器坏了"被当成"每条记录都无效"。
    ///
    /// `invalid_record_policy` 默认是 DeadLetter，判据对"畸形单条 payload"成立；但对
    /// 「新解码器对**所有**记录都失败」同样成立，于是整个 topic 会以满速排进
    /// `<topic>.DLT`、offset 一路前进、group 还是 Running——等发现时数据已经搬完了。
    /// 该阈值把这种整体性故障从"静默排干"拉回"停下待人工"。任一次解码成功即清零，
    /// 因此零星坏消息永远不会累积到阈值。`None` = 关闭该保护（保持旧行为）。
    pub decode_failure_halt_streak: Option<u64>,
    /// 单次 handler 调用的超时毫秒;None = 不限时。仅协作式取消,应明显小于 max_poll_interval_ms。
    pub handler_timeout_ms: Option<u64>,
    /// 每 group 控制命令队列容量(有界,满即 CommandQueueFull)。
    pub command_queue_capacity: usize,
    /// 控制命令的调用端等待超时毫秒。
    pub control_timeout_ms: u64,
    /// shutdown 总 deadline 毫秒(贯穿全部步骤,不逐步重置)。
    pub shutdown_timeout_ms: u64,
    /// 同步发布等待 delivery 的总超时毫秒;也是 admin 超时链的最终回退。
    pub publish_timeout_ms: u64,
    /// 广播实例 ID 覆盖;缺省依次取 env NASA_KAFKA_BROADCAST_INSTANCE_ID → 进程随机值。
    pub broadcast_instance_id: Option<String>,
}

impl Default for BehaviorConfig {
    /// 返回批量、提交、重试、DLT 与关闭行为的安全基线。
    fn default() -> Self {
        Self {
            max_batch_records: 500,
            max_batch_bytes: 16 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            max_progress_records: 2048,
            max_group_progress_records: 65_536,
            commit_interval_ms: 1000,
            commit_batch_records: 1000,
            poll_timeout_ms: 500,
            start_consume_delay_ms: 1000,
            consume_restart_delay_ms: 3000,
            consume_restart_max_delay_ms: 60_000,
            consume_restart_budget_times: 10,
            consume_restart_budget_window_ms: 300_000,
            consume_retry_backoff_ms: 500,
            rebalance_commit_timeout_ms: 5000,
            max_consume_attempts: 3,
            max_retry_entries: 100_000,
            dead_letter_topic_suffix: ".DLT".into(),
            dlt_producer_lane: "default".into(),
            dead_letter_required: true,
            dlt_queue_capacity: 1024,
            dlt_queue_max_bytes: 256 * 1024 * 1024,
            dlt_max_in_flight: 16,
            dlt_retry_initial_ms: 1000,
            dlt_retry_max_ms: 60_000,
            dlt_degraded_after_attempts: 10,
            unmatched_policy: UnmatchedPolicy::Skip,
            invalid_record_policy: InvalidRecordPolicy::DeadLetter,
            decode_failure_halt_streak: Some(1000),
            handler_timeout_ms: None,
            command_queue_capacity: 1024,
            control_timeout_ms: 30_000,
            shutdown_timeout_ms: 30_000,
            publish_timeout_ms: 120_000,
            broadcast_instance_id: None,
        }
    }
}

/// 安全配置;Debug 手写打码,凭据字段永不进日志。
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// 安全协议(PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL);含 SSL 时要求编译开启 feature `tls`。
    pub protocol: Option<String>,
    /// SASL 机制(PLAIN/SCRAM-SHA-256/SCRAM-SHA-512);GSSAPI 需 feature `gssapi`。
    pub sasl_mechanism: Option<String>,
    /// SASL 用户名。
    pub sasl_username: Option<String>,
    /// SASL 密码;Debug 打码。
    pub sasl_password: Option<String>,
    /// CA 证书路径(PEM;与参照实现的 JKS 不同)。
    pub ssl_ca_location: Option<String>,
    /// 客户端证书路径(PEM/PKCS12)。
    pub ssl_certificate_location: Option<String>,
    /// 客户端私钥路径;路径本身可见,内容永不读入配置。
    pub ssl_key_location: Option<String>,
    /// 私钥口令;Debug 打码。
    pub ssl_key_password: Option<String>,
}

impl fmt::Debug for SecurityConfig {
    /// 输出可排障但不包含凭据值的配置快照。
    ///
    /// - `f`: 调用方提供的格式化缓冲区。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 打码原则:存在性可见(便于排障"配没配"),值不可见(凭据不进任何日志/panic 消息)。
        /// 把可选凭据压缩为“存在/不存在”的安全文本。
        ///
        /// - `v`: 配置中的可选凭据，只读取存在性。
        fn mask(v: &Option<String>) -> &'static str {
            if v.is_some() {
                "***"
            } else {
                "None"
            }
        }
        f.debug_struct("SecurityConfig")
            .field("protocol", &self.protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &mask(&self.sasl_password))
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("ssl_certificate_location", &self.ssl_certificate_location)
            .field("ssl_key_location", &self.ssl_key_location)
            .field("ssl_key_password", &mask(&self.ssl_key_password))
            .finish()
    }
}

/// 任何透传 map 都不得携带的保留键:框架不变量,覆盖即破坏 at-least-once 地基。
///
/// 每项列出 canonical 名与底层**别名**：librdkafka 多个键名指向同一字段，而 rdkafka 的
/// 配置表是 HashMap、按哈希序逐个下发，只拦 canonical 名时别名可以概率性地推翻不变量。
const FORBIDDEN_RAW_KEYS: [&[&str]; 2] = [&["enable.auto.commit"], &["enable.auto.offset.store"]];

/// 仅 producer 侧保留的键:consumer/admin 没有对应不变量,不做过度限制。
///
/// `max.in.flight` 是 librdkafka 对 `max.in.flight.requests.per.connection` 的别名
/// (rdkafka_conf.c)，两者指向同一底层字段，必须一起拦。
const FORBIDDEN_PRODUCER_RAW_KEYS: [&[&str]; 4] = [
    &["max.in.flight.requests.per.connection", "max.in.flight"],
    &["delivery.timeout.ms", "message.timeout.ms"],
    &["acks", "request.required.acks"],
    &["compression.type", "compression.codec"],
];

impl KafkaConfig {
    /// 启动期全量校验:任何一条不满足立即返回 [`NafkaError::Config`],绝不带病启动。
    ///
    /// 校验清单与协议合同一一对应;与运行环境相关的少数项(幂等参数与底层库约束的
    /// 交叉校验)在构建 producer 时补齐；缺席只会放宽本应拒绝的配置，不产生隐式副作用。
    ///
    /// # 错误
    /// 首个违反项以 `Config(说明)` 返回;说明含字段名与实际值,便于运维直接定位。
    pub fn validate(&self) -> Result<()> {
        // -- 基础字段 --
        if self.bootstrap_servers.trim().is_empty() {
            return Err(cfg_err("bootstrap_servers 不能为空"));
        }
        validate_name_charset("client_name", &self.client_name)?;

        // -- producer 时序关系:delivery 必须覆盖一次请求 + 攒批等待,否则必然先超时 --
        let p = &self.producer;
        for (field, millis) in [
            ("producer.retry_backoff_ms", p.retry_backoff_ms),
            ("producer.linger_ms", p.linger_ms),
            ("producer.request_timeout_ms", p.request_timeout_ms),
            ("producer.delivery_timeout_ms", p.delivery_timeout_ms),
            (
                "producer.stalled_rebuild_after_ms",
                p.stalled_rebuild_after_ms,
            ),
            (
                "producer.stalled_rebuild_cooldown_ms",
                p.stalled_rebuild_cooldown_ms,
            ),
            (
                "consumer.session_timeout_ms",
                self.consumer.session_timeout_ms,
            ),
            (
                "consumer.heartbeat_interval_ms",
                self.consumer.heartbeat_interval_ms,
            ),
            (
                "consumer.max_poll_interval_ms",
                self.consumer.max_poll_interval_ms,
            ),
            (
                "consumer.fetch_wait_max_ms",
                self.consumer.fetch_wait_max_ms,
            ),
        ] {
            validate_millis_upper_bound(field, millis)?;
        }
        for (field, millis) in [
            ("admin.request_timeout_ms", self.admin.request_timeout_ms),
            (
                "admin.default_api_timeout_ms",
                self.admin.default_api_timeout_ms,
            ),
        ] {
            if let Some(millis) = millis {
                if millis == 0 {
                    return Err(cfg_err(&format!("{field} 必须 > 0")));
                }
                validate_millis_upper_bound(field, millis)?;
            }
        }
        let minimum_delivery_timeout = p
            .request_timeout_ms
            .checked_add(p.linger_ms)
            .ok_or_else(|| cfg_err("producer.request_timeout_ms + linger_ms 溢出"))?;
        if p.delivery_timeout_ms < minimum_delivery_timeout {
            return Err(cfg_err(&format!(
                "producer.delivery_timeout_ms({}) 必须 >= request_timeout_ms({}) + linger_ms({})",
                p.delivery_timeout_ms, p.request_timeout_ms, p.linger_ms
            )));
        }
        if p.stalled_rebuild_after_ms == 0
            || p.stalled_rebuild_min_failures == 0
            || p.stalled_rebuild_cooldown_ms == 0
            || p.max_in_flight_requests_per_connection == 0
        {
            return Err(cfg_err(
                "producer stalled rebuild 的时长、失败阈值与冷却均必须 > 0",
            ));
        }
        if p.partitioner != "murmur2_random" {
            // 高可见告警而非拒绝:允许显式覆盖,但同 key 跨语言分区一致性由调用方自负。
            tracing::warn!(partitioner = %p.partitioner, "producer.partitioner 偏离 murmur2_random, 同 key 跨语言可能落不同分区");
        }

        // -- consumer 时序关系 --
        let c = &self.consumer;
        if c.heartbeat_interval_ms >= c.session_timeout_ms {
            return Err(cfg_err(
                "consumer.heartbeat_interval_ms 必须 < session_timeout_ms",
            ));
        }

        // -- behavior:数值下界与相互关系 --
        let b = &self.behavior;
        for (field, millis) in [
            ("behavior.commit_interval_ms", b.commit_interval_ms),
            ("behavior.poll_timeout_ms", b.poll_timeout_ms),
            ("behavior.start_consume_delay_ms", b.start_consume_delay_ms),
            (
                "behavior.consume_restart_delay_ms",
                b.consume_restart_delay_ms,
            ),
            (
                "behavior.consume_restart_max_delay_ms",
                b.consume_restart_max_delay_ms,
            ),
            (
                "behavior.consume_restart_budget_window_ms",
                b.consume_restart_budget_window_ms,
            ),
            (
                "behavior.consume_retry_backoff_ms",
                b.consume_retry_backoff_ms,
            ),
            (
                "behavior.rebalance_commit_timeout_ms",
                b.rebalance_commit_timeout_ms,
            ),
            ("behavior.dlt_retry_initial_ms", b.dlt_retry_initial_ms),
            ("behavior.dlt_retry_max_ms", b.dlt_retry_max_ms),
            ("behavior.control_timeout_ms", b.control_timeout_ms),
            ("behavior.shutdown_timeout_ms", b.shutdown_timeout_ms),
            ("behavior.publish_timeout_ms", b.publish_timeout_ms),
        ] {
            validate_millis_upper_bound(field, millis)?;
        }
        if let Some(millis) = b.handler_timeout_ms {
            validate_millis_upper_bound("behavior.handler_timeout_ms", millis)?;
        }
        for (name, v) in [
            ("behavior.max_batch_records", b.max_batch_records),
            ("behavior.max_batch_bytes", b.max_batch_bytes),
            ("behavior.max_record_bytes", b.max_record_bytes),
            ("behavior.max_progress_records", b.max_progress_records),
            (
                "behavior.max_group_progress_records",
                b.max_group_progress_records,
            ),
            ("behavior.commit_batch_records", b.commit_batch_records),
            ("behavior.max_retry_entries", b.max_retry_entries),
            ("behavior.dlt_queue_capacity", b.dlt_queue_capacity),
            ("behavior.dlt_queue_max_bytes", b.dlt_queue_max_bytes),
            ("behavior.dlt_max_in_flight", b.dlt_max_in_flight),
            ("behavior.command_queue_capacity", b.command_queue_capacity),
            (
                "consumer.max_assignment_partitions",
                c.max_assignment_partitions,
            ),
            ("producer.fire_observer_capacity", p.fire_observer_capacity),
            (
                "producer.queue_buffering_max_messages",
                p.queue_buffering_max_messages,
            ),
            (
                "producer.queue_buffering_max_kbytes",
                p.queue_buffering_max_kbytes,
            ),
        ] {
            if v == 0 {
                return Err(cfg_err(&format!("{name} 必须 > 0")));
            }
        }
        for (name, v) in [
            ("behavior.commit_interval_ms", b.commit_interval_ms),
            ("behavior.poll_timeout_ms", b.poll_timeout_ms),
            ("behavior.control_timeout_ms", b.control_timeout_ms),
            ("behavior.shutdown_timeout_ms", b.shutdown_timeout_ms),
            ("behavior.publish_timeout_ms", b.publish_timeout_ms),
            (
                "behavior.rebalance_commit_timeout_ms",
                b.rebalance_commit_timeout_ms,
            ),
            ("behavior.dlt_retry_initial_ms", b.dlt_retry_initial_ms),
            ("behavior.dlt_retry_max_ms", b.dlt_retry_max_ms),
        ] {
            if v == 0 {
                return Err(cfg_err(&format!("{name} 必须 > 0")));
            }
        }
        if b.dlt_degraded_after_attempts == 0
            || b.consume_restart_budget_times == 0
            || b.consume_restart_budget_window_ms == 0
        {
            return Err(cfg_err(
                "restart budget 与 dlt_degraded_after_attempts 的次数/窗口均必须 > 0",
            ));
        }
        if b.commit_interval_ms >= c.max_poll_interval_ms {
            return Err(cfg_err(
                "behavior.commit_interval_ms 必须 < consumer.max_poll_interval_ms",
            ));
        }
        // commit 触发点前必须还能原子接纳一个最大连续 run,否则 progress 上限会卡死正常流量。
        let progress_floor = b
            .commit_batch_records
            .checked_add(b.max_batch_records)
            .ok_or_else(|| cfg_err("commit_batch_records + max_batch_records 溢出"))?;
        if b.max_progress_records < progress_floor {
            return Err(cfg_err(&format!(
                "behavior.max_progress_records({}) 必须 >= commit_batch_records + max_batch_records({progress_floor})",
                b.max_progress_records
            )));
        }
        if b.max_group_progress_records < b.max_progress_records {
            return Err(cfg_err(
                "behavior.max_group_progress_records 必须 >= max_progress_records",
            ));
        }
        // 任一合法单 DltJob 必须能拿到 byte permit,否则 durability-first 下永久死锁。
        if b.dlt_queue_max_bytes < b.max_batch_bytes.max(b.max_record_bytes) {
            return Err(cfg_err(
                "behavior.dlt_queue_max_bytes 必须 >= max(max_batch_bytes, max_record_bytes)",
            ));
        }
        if b.dlt_queue_max_bytes > u32::MAX as usize {
            return Err(cfg_err("behavior.dlt_queue_max_bytes 不能超过 u32::MAX"));
        }
        let semaphore_max = tokio::sync::Semaphore::MAX_PERMITS;
        for (name, value) in [
            ("behavior.dlt_queue_capacity", b.dlt_queue_capacity),
            ("behavior.dlt_queue_max_bytes", b.dlt_queue_max_bytes),
            ("producer.fire_observer_capacity", p.fire_observer_capacity),
        ] {
            if value > semaphore_max {
                return Err(cfg_err(&format!(
                    "{name} 超过 Tokio semaphore 容量上限 {semaphore_max}"
                )));
            }
        }
        // poll task 的 DLT 控制通道容量为 `dlt_max_in_flight + 1`，必须把额外槽位也计入。
        if b.dlt_max_in_flight >= semaphore_max {
            return Err(cfg_err(&format!(
                "behavior.dlt_max_in_flight 必须小于 Tokio semaphore 容量上限 {semaphore_max}"
            )));
        }
        if b.consume_restart_delay_ms > b.consume_restart_max_delay_ms {
            return Err(cfg_err(
                "behavior.consume_restart_delay_ms 必须 <= consume_restart_max_delay_ms",
            ));
        }
        // revoke 窗口必须先于 session 判死结束,否则 commit 还没超时成员先被踢。
        if b.rebalance_commit_timeout_ms >= c.session_timeout_ms {
            return Err(cfg_err(
                "behavior.rebalance_commit_timeout_ms 必须 < consumer.session_timeout_ms",
            ));
        }
        if b.dlt_retry_initial_ms > b.dlt_retry_max_ms {
            return Err(cfg_err(
                "behavior.dlt_retry_initial_ms 必须 <= dlt_retry_max_ms",
            ));
        }
        if let Some(ht) = b.handler_timeout_ms {
            if ht == 0 {
                return Err(cfg_err("behavior.handler_timeout_ms 若配置必须 > 0"));
            }
            // handler 超时预算必须明显小于两次 poll 的最大间隔:否则单条卡死的 handler 就能
            // 耗光整个 poll 预算,成员被协调器踢出并触发 rebalance,而框架侧只看到一次 handler
            // timeout,运维很难把"被踢出组"归因到该配置。这里把"明显小于"固化为 2 倍余量,
            // 让误配在启动期就失败而不是运行期才暴露。
            if ht.saturating_mul(2) > c.max_poll_interval_ms {
                return Err(cfg_err(&format!(
                    "behavior.handler_timeout_ms({ht}) 必须明显小于 consumer.max_poll_interval_ms({}): 要求 handler_timeout_ms * 2 <= max_poll_interval_ms",
                    c.max_poll_interval_ms
                )));
            }
        }
        if !b.dead_letter_required {
            // 显式可用性优先:允许,但必须在启动日志留下高可见痕迹。
            tracing::warn!("dead_letter_required=false: 显式 availability-first, DLT 不可达时坏消息将被跳过丢弃");
        }
        if matches!(b.invalid_record_policy, InvalidRecordPolicy::DeadLetter)
            && b.dead_letter_topic_suffix.trim().is_empty()
        {
            return Err(cfg_err(
                "invalid_record_policy=dead_letter 要求 dead_letter_topic_suffix 非空",
            ));
        }
        if matches!(b.unmatched_policy, UnmatchedPolicy::DeadLetter)
            && b.dead_letter_topic_suffix.trim().is_empty()
        {
            return Err(cfg_err(
                "unmatched_policy=dead_letter 要求 dead_letter_topic_suffix 非空",
            ));
        }

        // -- lane 规则 --
        for (lane, ov) in &p.lanes {
            validate_name_charset("producer.lanes 键", lane)?;
            if lane == "default" {
                return Err(cfg_err(
                    "producer.lanes 不得重复声明保留 lane 名 \"default\"",
                ));
            }
            for (name, v) in [
                ("fire_observer_capacity", ov.fire_observer_capacity),
                (
                    "queue_buffering_max_messages",
                    ov.queue_buffering_max_messages,
                ),
                ("queue_buffering_max_kbytes", ov.queue_buffering_max_kbytes),
            ] {
                if v == Some(0) {
                    return Err(cfg_err(&format!("producer.lanes.{lane}.{name} 必须 > 0")));
                }
            }
            if ov
                .fire_observer_capacity
                .is_some_and(|value| value > semaphore_max)
            {
                return Err(cfg_err(&format!(
                    "producer.lanes.{lane}.fire_observer_capacity 超过 Tokio semaphore 容量上限 {semaphore_max}"
                )));
            }
            if ov.max_in_flight_requests_per_connection == Some(0) {
                return Err(cfg_err(&format!(
                    "producer.lanes.{lane}.max_in_flight_requests_per_connection 必须 > 0"
                )));
            }
            for (field, millis) in [
                ("linger_ms", ov.linger_ms),
                ("request_timeout_ms", ov.request_timeout_ms),
                ("delivery_timeout_ms", ov.delivery_timeout_ms),
                ("stalled_rebuild_after_ms", ov.stalled_rebuild_after_ms),
                (
                    "stalled_rebuild_cooldown_ms",
                    ov.stalled_rebuild_cooldown_ms,
                ),
            ] {
                if let Some(millis) = millis {
                    validate_millis_upper_bound(&format!("producer.lanes.{lane}.{field}"), millis)?;
                }
            }
            forbid_reserved_producer_raw(
                &ov.properties,
                &format!("producer.lanes.{lane}.properties"),
            )?;
        }
        if b.dlt_producer_lane != "default" && !p.lanes.contains_key(&b.dlt_producer_lane) {
            return Err(cfg_err(&format!(
                "behavior.dlt_producer_lane 指向不存在的 lane: {}",
                b.dlt_producer_lane
            )));
        }

        // -- 透传保留键(框架不变量不可覆盖) --
        forbid_reserved_producer_raw(&self.properties, "properties")?;
        forbid_reserved_producer_raw(&p.properties, "producer.properties")?;
        forbid_reserved_raw(&c.properties, "consumer.properties")?;
        forbid_reserved_raw(&self.admin.properties, "admin.properties")?;

        // -- 安全协议与编译 feature 匹配 --
        if let Some(proto) = &self.security.protocol {
            let mechanism = self
                .security
                .sasl_mechanism
                .as_deref()
                .unwrap_or_default()
                .to_ascii_uppercase();
            let needs_tls =
                proto.to_ascii_uppercase().contains("SSL") || mechanism.starts_with("SCRAM-");
            if needs_tls && cfg!(not(feature = "tls")) {
                return Err(cfg_err(&format!(
                    "security.protocol={proto} 需要编译 feature `tls`(内嵌静态 TLS), 当前未开启"
                )));
            }
        }
        let has_ssl_material = self.security.ssl_ca_location.is_some()
            || self.security.ssl_certificate_location.is_some()
            || self.security.ssl_key_location.is_some()
            || self.security.ssl_key_password.is_some();
        if has_ssl_material
            && !self
                .security
                .protocol
                .as_deref()
                .is_some_and(|value| value.to_ascii_uppercase().contains("SSL"))
        {
            return Err(cfg_err(
                "SSL 证书或私钥字段要求 security.protocol 显式包含 SSL",
            ));
        }
        // enable_idempotence=true 与 acks/retries/max.in.flight 的交叉约束在组装原生
        // producer 配置时校验(需要与底层库约束逐项对照);此处不做即"更宽松",
        // 不会产生额外副作用，但底层构建阶段仍会收紧配置。
        Ok(())
    }
}

/// 构造配置错误的简写;集中一处保证报错前缀风格一致。
///
/// - `msg`: 已脱敏且能定位字段的配置错误说明。
fn cfg_err(msg: &str) -> NafkaError {
    NafkaError::Config(msg.to_string())
}

/// Kafka 原生配置使用有符号 32 位毫秒值；同一批参数也会进入内部 `Instant` deadline。
/// 在统一配置边界拒绝更大值，避免底层隐式拒绝或 Rust 运行时加法溢出。
fn validate_millis_upper_bound(field: &str, millis: u64) -> Result<()> {
    if millis > i32::MAX as u64 {
        return Err(cfg_err(&format!(
            "{field}({millis}) 不能超过 i32::MAX 毫秒"
        )));
    }
    Ok(())
}

/// 校验名字类字段的字符集:仅 ASCII 字母/数字/./_/-,且非空(client_name、lane 名共用规则)。
///
/// # 参数
/// - `field`: 字段名,用于报错定位。
/// - `value`: 待校验值。
///
/// # 错误
/// 值为空或包含允许集合之外的字符时返回配置错误。
fn validate_name_charset(field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(cfg_err(&format!("{field} 不能为空")));
    }
    let ok = value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'));
    if !ok {
        return Err(cfg_err(&format!(
            "{field} 只允许 ASCII 字母/数字/./_/-,实际: {value}"
        )));
    }
    Ok(())
}

/// 拒绝透传 map 中的框架不变量键:出现即报错,而非静默剔除——
/// 静默剔除会让运维以为覆盖生效,埋下"以为开了自动提交"的认知事故。
///
/// # 参数
/// - `props`: 待检查的透传键值表。
/// - `where_`: map 位置描述,用于报错定位。
///
/// # 错误
/// 透传表包含自动提交或自动位点存储键时返回配置错误。
fn forbid_reserved_raw(props: &BTreeMap<String, String>, where_: &str) -> Result<()> {
    forbid_keys(props, where_, &FORBIDDEN_RAW_KEYS)
}

/// 校验 producer 侧透传 map：在通用保留键之外，再拦下会推翻 producer 不变量的键与别名。
///
/// # 参数
///
/// - `props`: 待校验的原生透传表。
/// - `where_`: 出错时用于定位的配置路径。
///
/// # 错误
///
/// 命中任一保留键或其别名时返回配置错误。
fn forbid_reserved_producer_raw(props: &BTreeMap<String, String>, where_: &str) -> Result<()> {
    forbid_reserved_raw(props, where_)?;
    forbid_keys(props, where_, &FORBIDDEN_PRODUCER_RAW_KEYS)
}

/// 按"canonical 名 + 全部别名"逐组校验透传表。
///
/// # 参数
///
/// - `props`: 待校验的原生透传表。
/// - `where_`: 出错时用于定位的配置路径。
/// - `groups`: 每组的第一个元素是 canonical 名，其余是底层别名。
///
/// # 错误
///
/// 命中任一键时返回配置错误，错误文本给出 canonical 名便于定位。
fn forbid_keys(props: &BTreeMap<String, String>, where_: &str, groups: &[&[&str]]) -> Result<()> {
    for group in groups {
        let canonical = group[0];
        for key in *group {
            if props.contains_key(*key) {
                let alias = if *key == canonical {
                    String::new()
                } else {
                    format!("(别名 {key})")
                };
                return Err(cfg_err(&format!(
                    "{where_} 不得携带框架不变量键 {canonical}{alias}：该值由 nafka 统一裁决，透传会破坏语义地基"
                )));
            }
        }
    }
    Ok(())
}
