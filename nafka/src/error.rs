//! nafka 统一错误域。
//!
//! 设计约束:公共错误不得携带/暴露 rdkafka 类型,底层错误在 rd/ 模块内
//! 转成分类字符串;调用方依赖 variant 语义而非错误文本做分支。

/// producer 同步入队前的独立有界容量域。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducerQueue {
    /// `fire()` 的 delivery observer permit 已耗尽，消息尚未交给 librdkafka。
    DeliveryObserver,
    /// librdkafka 本地 native producer queue 已满，消息未入队。
    Native,
}

impl std::fmt::Display for ProducerQueue {
    /// 输出稳定的低基数队列域名称，供错误文本与指标标签复用。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeliveryObserver => formatter.write_str("delivery-observer"),
            Self::Native => formatter.write_str("native"),
        }
    }
}

/// nafka 全部公共 API 的错误类型。
///
/// variant 划分原则:调用方"下一步该做什么"不同就必须是不同 variant——
/// 例如 [`NafkaError::OutcomeUnknown`] 表示发布结果未知、禁止盲目重试，
/// [`NafkaError::Publish`] 表示确定失败、可按业务策略重试，两者绝不能合并。
#[derive(thiserror::Error, Debug)]
pub enum NafkaError {
    /// 配置无效:connect/validate 阶段 fail-fast,携带首个违反项的说明。
    #[error("Kafka 配置无效: {0}")]
    Config(String),

    /// 消费者注册失败:重复 (group,topic,event)、空 event、group 无法解析等,start() 前一次性暴露。
    #[error("consumer 注册失败: {0}")]
    Registry(String),

    /// payload 编解码失败:JSON/ProtocolBytes 编码或解码错误,消费侧走 undecodable 策略。
    #[error("payload codec 失败: {0}")]
    Codec(String),

    /// Kafka→socket 路由 header 非法(重复 singleton、非 UTF-8、数量/字节超限等)。
    #[error("Kafka→socket 路由 header 非法: {0}")]
    MalformedRoute(String),

    /// passthrough 消息缺 payload(Kafka tombstone):socket 数据面不允许 null value。
    #[error("Kafka→socket raw 消息缺少 payload(Kafka tombstone)")]
    MissingPayload,

    /// 已经来自 DLT 的消息再次请求 DLT；为避免无限死信链必须 Halt。
    #[error("禁止递归 DLT")]
    RecursiveDlt,

    /// 单条消息 fan-out 展开超过上限:容量问题,不是坏消息,默认 Halt。
    #[error("Kafka→socket 单条 fan-out 超过上限 max={max}")]
    RouteExpansionTooLarge {
        /// 配置的 max_fanout_targets 上限(唯一 session 数)。
        max: usize,
    },

    /// socket outbox 未全部入队(RequireAllEnqueued 策略下的失败)。
    #[error("socket outbox 未全部入队 dropped={dropped} closed={closed}")]
    SocketEnqueue {
        /// 因队列满被丢弃的目标数。
        dropped: usize,
        /// 因连接已关闭投递失败的目标数。
        closed: usize,
    },

    /// 发布确定失败(编码失败、入队被拒且未发出、broker 明确拒绝)。
    #[error("发布失败 lane={lane} topic={topic}: {message}")]
    Publish {
        /// 发生失败的 producer lane。
        lane: String,
        /// 目标 topic,用于日志定位。
        topic: String,
        /// 分类后的失败说明(不含敏感数据)。
        message: String,
    },

    /// producer 有界容量已满且可以确定消息尚未入队；调用方可按容量退避，不能与其它发布错误混淆。
    #[error("producer 队列已满 lane={lane} topic={topic} queue={queue}")]
    ProducerQueueFull {
        /// 发生容量拒绝的物理 lane。
        lane: String,
        /// 目标 topic。
        topic: String,
        /// observer 或 native queue 容量域。
        queue: ProducerQueue,
    },

    /// 发布结果未知(超时时消息可能已入队):调用方不能盲目重试,否则可能重复。
    #[error("发布结果未知 lane={lane} topic={topic}: {message}")]
    OutcomeUnknown {
        /// 发生超时或中断的 producer lane。
        lane: String,
        /// 目标 topic。
        topic: String,
        /// 超时/中断的上下文说明。
        message: String,
    },

    /// 指定 group 不存在或未运行(控制/查询类 API)。
    #[error("group 未运行: {0}")]
    NoSuchGroup(String),

    /// 指定 producer lane 未配置;lane 必须在 connect 时冻结,不能按调用动态创建。
    #[error("producer lane 未配置: {0}")]
    NoSuchProducerLane(String),

    /// 控制命令队列已满:handler 卡顿时的背压信号,不是永久错误。
    #[error("consumer 命令队列已满 group={0}")]
    CommandQueueFull(String),

    /// 控制命令超时且已确认未执行(CAS Queued→Cancelled 成功):可安全重试。
    #[error("consumer 控制命令超时且确认未执行 command_id={0}")]
    ControlNotApplied(u64),

    /// 控制命令结果未知(owner 已开始执行):必须用 position/assignment 等读 API 核对后再决策。
    #[error("consumer 控制命令结果未知,需查询实际状态 command_id={0}")]
    ControlOutcomeUnknown(u64),

    /// 业务 handler panic(catch_unwind 兜住):按消费失败进入重试/DLT 状态机。
    #[error("consumer handler panic: {0}")]
    HandlerPanic(String),

    /// 在 AckMode::Auto 的 route 上调用了手动 ack:配置误用,尽早暴露。
    #[error("当前 consumer 未启用手动 ack")]
    AckNotManual,

    /// ack 晚于回调有效期(handler 已返回、token 已封存):不改变 offset。
    #[error("ack 已超过当前 handler invocation 有效期")]
    AckExpired,

    /// group 级不可恢复错误(认证/授权 fatal、框架不变量被破坏):不自动重启。
    #[error("consumer group 发生不可恢复错误 group={group}: {message}")]
    GroupFatal {
        /// 出错的 group.id。
        group: String,
        /// fatal 分类说明。
        message: String,
    },

    /// assignment 分区数超过 max_assignment_partitions:内存上限保护,group 置 Crashed。
    #[error("assignment 分区数超限 group={group} actual={actual} max={max}")]
    AssignmentTooLarge {
        /// 超限的 group.id。
        group: String,
        /// 实际分配到的分区数。
        actual: usize,
        /// 配置上限 consumer.max_assignment_partitions。
        max: usize,
    },

    /// await_group(s)_ready 在 deadline 内未满足 ReadyRequirement。
    #[error("group ready 等待超时 groups={groups:?}")]
    GroupReadyTimeout {
        /// 截止时仍未满足要求的 group 列表。
        groups: Vec<String>,
        /// 每个未就绪 group 在 deadline 时的最后健康快照。
        last_health: Vec<crate::health::GroupHealth>,
    },

    /// TopicContract 校验失败(topic 缺失/分区数不符/配置漂移/无权限)。
    #[error("topic 契约校验失败: {0}")]
    TopicContract(String),

    /// broker 侧操作失败(分类后的传输/协调错误;不携带底层客户端类型)。
    #[error("Kafka broker 操作失败: {0}")]
    Broker(String),

    /// 生命周期状态不允许该操作(如 Running 后注册、shutdown 后发布)。
    #[error("生命周期状态不允许该操作: {0}")]
    Lifecycle(String),

    /// shutdown 总 deadline 到期仍有 group 未退出:状态保持 ShuttingDown,可再次 await。
    #[error("关闭超时,未退出 groups={groups:?},未排空 producer_lanes={producer_lanes:?}")]
    ShutdownTimeout {
        /// 仍未退出的 consumer group。
        groups: Vec<String>,
        /// 仍未排空的 producer lane。
        producer_lanes: Vec<String>,
    },

    /// connect 时当前线程不在 tokio runtime 内:poll 线程派发依赖 Handle,必须 fail-fast。
    #[error("当前不在 Tokio runtime 中")]
    NoRuntime,
}

/// nafka 统一 Result 别名;全部公共 API 与内部模块使用同一错误域。
pub type Result<T> = std::result::Result<T, NafkaError>;
