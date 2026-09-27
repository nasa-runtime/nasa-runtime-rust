//! Job 的封闭枚举：状态、结果码、并发、误触发、触发类型、调度类型、Fanout 失败策略与线编码。
//!
//! 每个值的 `wire_name()` 都进入 definitionDigest、Lua ARGV 与 Redis 字段，是持久线协议：改名或改集合
//! 会让多实现对同一状态迁移产生分歧。未知线名一律 fail-closed，不静默降级成默认值。

/// 业务作用：为一个封闭枚举生成线协议名称与解析；名称集合是持久协议的一部分，不可改动。
macro_rules! wire_enum {
    ($(#[$emeta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $wire:literal),+ $(,)? }) => {
        $(#[$emeta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($(#[$vmeta])* $variant),+ }

        impl $name {
            /// 业务作用：返回该值的线协议名称；进入 definitionDigest、Lua ARGV 与 Redis 字段，不可改动。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：固定大写下划线名称。
            pub fn wire_name(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }

            /// 业务作用：从线协议名称解析枚举；未知名称 fail-closed，不静默降级。
            ///
            /// 参数说明：
            /// - `text`: 线协议名称。
            ///
            /// 返回：已知名称返回对应值；未知返回 `None`。
            pub fn parse(text: &str) -> Option<Self> {
                Some(match text { $($wire => Self::$variant,)+ _ => return None })
            }
        }
    };
}

wire_enum! {
    /// Run 状态机的封闭状态集合。
    JobState {
        /// Run 已创建，尚未进入执行队列。
        Created => "CREATED",
        /// Run 已排队，等待执行器领取。
        Queued => "QUEUED",
        /// Run 受并发门禁限制，等待前序执行释放资格。
        Blocked => "BLOCKED",
        /// 当前 attempt 已取得执行权。
        Running => "RUNNING",
        /// 当前 attempt 结束后等待重试时刻。
        RetryWait => "RETRY_WAIT",
        /// Fanout 根正在建立子任务集合。
        FanoutCreating => "FANOUT_CREATING",
        /// Fanout 根等待子任务结果聚合。
        WaitingChildren => "WAITING_CHILDREN",
        /// 子任务尚未找到满足要求的可用执行能力。
        AwaitingCapability => "AWAITING_CAPABILITY",
        /// 子任务已定向投递，等待目标执行器回执。
        AwaitingReceipt => "AWAITING_RECEIPT",
        /// 目标执行器已确认接收，尚未完成业务执行。
        Received => "RECEIVED",
        /// 业务执行或子任务聚合成功终结。
        Succeeded => "SUCCEEDED",
        /// 业务执行或聚合按失败结果终结。
        Failed => "FAILED",
        /// 执行已耗尽允许的重试或存活预算。
        Dead => "DEAD",
        /// 调度或并发策略决定跳过本次 Run。
        Skipped => "SKIPPED",
        /// Run 已按取消请求终结。
        Cancelled => "CANCELLED",
    }
}

wire_enum! {
    /// 任务定义状态；`CONFLICT` 表示同修订号出现语义分歧、已封闭调度，需显式管理动作选择。
    JobDefinitionState {
        /// 定义存在但尚未允许调度。
        Disabled => "DISABLED",
        /// 定义允许按触发策略创建 Run。
        Enabled => "ENABLED",
        /// 定义已暂停调度，保留管理状态。
        Paused => "PAUSED",
        /// 同修订号存在语义分歧，调度被封闭。
        Conflict => "CONFLICT",
        /// 定义已进入删除状态，旧修订号不能使其复活。
        Deleted => "DELETED",
    }
}

wire_enum! {
    /// Handler 返回结果码；框架异常默认按可重试失败处理。
    JobResultCode {
        /// Handler 已完成本次业务工作。
        Success => "SUCCESS",
        /// 本次执行失败，允许在重试预算内再次尝试。
        Retry => "RETRY",
        /// 业务明确拒绝后续自动重试。
        FailPermanent => "FAIL_PERMANENT",
        /// 本次执行因取消而结束。
        Cancelled => "CANCELLED",
        /// 本次执行超过允许时长。
        Timeout => "TIMEOUT",
    }
}

wire_enum! {
    /// 并发策略；不含协作式取消策略，避免旧 Handler 未确认退出前开放第二个执行权。
    JobConcurrency {
        /// 同一任务依次执行，后续 Run 进入串行积压队列。
        SerialQueue => "SERIAL_QUEUE",
        /// 已有执行占用时跳过新 Run。
        DiscardIfRunning => "DISCARD_IF_RUNNING",
        /// 允许同一任务的多个 Run 并行执行。
        Parallel => "PARALLEL",
    }
}

wire_enum! {
    /// 误触发策略。
    JobMisfire {
        /// 跳过已经错过的逻辑触发时刻。
        DoNothing => "DO_NOTHING",
        /// 将错过的触发合并为一次当前执行。
        FireOnceNow => "FIRE_ONCE_NOW",
        /// 在补偿预算内依次补建错过的触发。
        CatchUp => "CATCH_UP",
    }
}

wire_enum! {
    /// 触发类型；`FANOUT_ONLY` 只作为 Fanout 根，不进入普通调度扫描。
    JobTrigger {
        /// 参与普通任务调度的根触发。
        Scheduled => "SCHEDULED",
        /// 仅作为 Fanout 根使用，不进入普通调度扫描。
        FanoutOnly => "FANOUT_ONLY",
    }
}

wire_enum! {
    /// 调度类型。
    JobScheduleType {
        /// 按 Cron 表达式计算逻辑触发时刻。
        Cron => "CRON",
        /// 按固定时间间隔推进逻辑触发时刻。
        FixedRate => "FIXED_RATE",
        /// 从前次执行完成时刻起等待固定延迟再触发。
        FixedDelay => "FIXED_DELAY",
        /// 只接受显式手工触发。
        Manual => "MANUAL",
        /// 仅作为 Fanout 根，不创建普通周期触发。
        FanoutOnly => "FANOUT_ONLY",
    }
}

wire_enum! {
    /// Fanout 失败策略；不含线程/task 取消类策略。
    JobFanoutFailurePolicy {
        /// 目标失效后允许重新选择满足能力要求的执行器。
        ReassignOnFailure => "REASSIGN_ON_FAILURE",
        /// 保持触发时的执行器快照约束，不能任意替换缺席目标。
        StrictSnapshot => "STRICT_SNAPSHOT",
        /// 无法继续投递的子任务允许按跳过结果聚合。
        BestEffort => "BEST_EFFORT",
    }
}

wire_enum! {
    /// Fanout 通知与回执的 Pub/Sub 模式；`Sharded` 用 SPUBLISH（同 slot 定向），`Broadcast` 用 PUBLISH。
    JobPubSubMode {
        /// 使用 SPUBLISH 向频道所在 slot 的订阅者投递。
        Sharded => "SHARDED",
        /// 使用 PUBLISH 向普通频道订阅者广播。
        Broadcast => "BROADCAST",
    }
}

wire_enum! {
    /// 执行器登记状态；`Draining` 停止进入新能力快照但保留已有 attempt 的续期权。
    JobExecutorState {
        /// 执行器可进入新能力快照并接受任务分配。
        Active => "ACTIVE",
        /// 停止接受新分配，但保留已有 attempt 的续期权。
        Draining => "DRAINING",
    }
}

wire_enum! {
    /// 串行队列积压溢出策略；只在 `SERIAL_QUEUE` 并发下生效，决定超过 backlog 上限时丢弃哪一端。
    JobSerialOverflowPolicy {
        /// 积压超过上限时跳过队列中最旧的待执行 Run。
        SkipOldest => "SKIP_OLDEST",
        /// 积压超过上限时跳过最新加入的 Run。
        SkipNewest => "SKIP_NEWEST",
    }
}

/// 业务作用：从配置的线协议名反序列化串行溢出策略；未知名称 fail-closed，不静默取默认。
///
/// 参数说明：
/// - `deserializer`: 配置来源的反序列化器，期望一个线协议名字符串。
///
/// 返回：已知名称返回对应策略；未知名称返回反序列化错误。
impl<'de> serde::Deserialize<'de> for JobSerialOverflowPolicy {
    /// 业务作用：从配置的线协议名反序列化串行溢出策略，未知值不会落到默认策略。
    ///
    /// 参数说明：`deserializer` 提供待解析的线协议字符串。
    ///
    /// 返回：已知名称返回对应策略；未知名称或输入类型错误时返回反序列化错误。
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::parse(&text)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom("未知的串行溢出策略线协议名"))
    }
}

impl serde::Serialize for JobSerialOverflowPolicy {
    /// 业务作用：把串行溢出策略写成持久配置使用的线协议名称。
    ///
    /// 参数说明：`serializer` 为目标配置序列化器。
    ///
    /// 返回：序列化成功返回目标值；底层写出失败返回序列化错误。
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.wire_name())
    }
}

impl JobPubSubMode {
    /// 业务作用：返回该模式对应的 Redis 发布命令名；投递、回执与唤醒脚本据此选择广播还是同 slot 定向。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Sharded` 返回 `SPUBLISH`，`Broadcast` 返回 `PUBLISH`。
    pub fn publish_command(self) -> &'static str {
        match self {
            Self::Sharded => "SPUBLISH",
            Self::Broadcast => "PUBLISH",
        }
    }
}

/// 业务作用：从配置的线协议名反序列化 Pub/Sub 模式；未知名称 fail-closed，不静默取默认。
///
/// 参数说明：
/// - `deserializer`: 配置来源的反序列化器，期望一个线协议名字符串。
///
/// 返回：已知名称返回对应模式；未知名称返回反序列化错误。
impl<'de> serde::Deserialize<'de> for JobPubSubMode {
    /// 业务作用：从配置的线协议名反序列化 Pub/Sub 模式，避免节点静默形成混合通知拓扑。
    ///
    /// 参数说明：`deserializer` 提供待解析的线协议字符串。
    ///
    /// 返回：已知名称返回对应模式；未知名称或输入类型错误时返回反序列化错误。
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::parse(&text)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom("未知的 Pub/Sub 模式线协议名"))
    }
}

impl serde::Serialize for JobPubSubMode {
    /// 业务作用：把 Pub/Sub 模式写成持久配置使用的线协议名称。
    ///
    /// 参数说明：`serializer` 为目标配置序列化器。
    ///
    /// 返回：序列化成功返回目标值；底层写出失败返回序列化错误。
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.wire_name())
    }
}

wire_enum! {
    /// payload 线编码；JSON 走独立无类型注册表路径，Protobuf/RAW 原样透传字节。
    JobWireCodec {
        /// 以 JSON 表示业务正文，不依赖类型注册表。
        Json => "JSON",
        /// 原样传递 Protobuf 编码字节，由业务匹配消息类型。
        Protobuf => "PROTOBUF",
        /// 原样传递不作结构解释的业务字节。
        Raw => "RAW",
    }
}
