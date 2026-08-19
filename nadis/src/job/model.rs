//! Job 的封闭枚举：状态、结果码、并发、误触发、触发类型、调度类型、Fanout 失败策略与线编码。
//!
//! 每个值的 `wire_name()` 都进入 definitionDigest、Lua ARGV 与 Redis 字段，是持久线协议：改名或改集合
//! 会让多实现对同一状态迁移产生分歧。未知线名一律 fail-closed，不静默降级成默认值。

/// 业务作用：为一个封闭枚举生成线协议名称与解析；名称集合是持久协议的一部分，不可改动。
macro_rules! wire_enum {
    ($(#[$emeta:meta])* $name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        $(#[$emeta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }

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
        Created => "CREATED",
        Queued => "QUEUED",
        Blocked => "BLOCKED",
        Running => "RUNNING",
        RetryWait => "RETRY_WAIT",
        FanoutCreating => "FANOUT_CREATING",
        WaitingChildren => "WAITING_CHILDREN",
        AwaitingCapability => "AWAITING_CAPABILITY",
        AwaitingReceipt => "AWAITING_RECEIPT",
        Received => "RECEIVED",
        Succeeded => "SUCCEEDED",
        Failed => "FAILED",
        Dead => "DEAD",
        Skipped => "SKIPPED",
        Cancelled => "CANCELLED",
    }
}

wire_enum! {
    /// 任务定义状态；`CONFLICT` 表示同修订号出现语义分歧、已封闭调度，需显式管理动作选择。
    JobDefinitionState {
        Disabled => "DISABLED",
        Enabled => "ENABLED",
        Paused => "PAUSED",
        Conflict => "CONFLICT",
        Deleted => "DELETED",
    }
}

wire_enum! {
    /// Handler 返回结果码；框架异常默认按可重试失败处理。
    JobResultCode {
        Success => "SUCCESS",
        Retry => "RETRY",
        FailPermanent => "FAIL_PERMANENT",
        Cancelled => "CANCELLED",
        Timeout => "TIMEOUT",
    }
}

wire_enum! {
    /// 并发策略；不含协作式取消策略，避免旧 Handler 未确认退出前开放第二个执行权。
    JobConcurrency {
        SerialQueue => "SERIAL_QUEUE",
        DiscardIfRunning => "DISCARD_IF_RUNNING",
        Parallel => "PARALLEL",
    }
}

wire_enum! {
    /// 误触发策略。
    JobMisfire {
        DoNothing => "DO_NOTHING",
        FireOnceNow => "FIRE_ONCE_NOW",
        CatchUp => "CATCH_UP",
    }
}

wire_enum! {
    /// 触发类型；`FANOUT_ONLY` 只作为 Fanout 根，不进入普通调度扫描。
    JobTrigger {
        Scheduled => "SCHEDULED",
        FanoutOnly => "FANOUT_ONLY",
    }
}

wire_enum! {
    /// 调度类型。
    JobScheduleType {
        Cron => "CRON",
        FixedRate => "FIXED_RATE",
        FixedDelay => "FIXED_DELAY",
        Manual => "MANUAL",
        FanoutOnly => "FANOUT_ONLY",
    }
}

wire_enum! {
    /// Fanout 失败策略；不含线程/task 取消类策略。
    JobFanoutFailurePolicy {
        ReassignOnFailure => "REASSIGN_ON_FAILURE",
        StrictSnapshot => "STRICT_SNAPSHOT",
        BestEffort => "BEST_EFFORT",
    }
}

wire_enum! {
    /// Fanout 通知与回执的 Pub/Sub 模式；`Sharded` 用 SPUBLISH（同 slot 定向），`Broadcast` 用 PUBLISH。
    JobPubSubMode {
        Sharded => "SHARDED",
        Broadcast => "BROADCAST",
    }
}

wire_enum! {
    /// 执行器登记状态；`Draining` 停止进入新能力快照但保留已有 attempt 的续期权。
    JobExecutorState {
        Active => "ACTIVE",
        Draining => "DRAINING",
    }
}

wire_enum! {
    /// 串行队列积压溢出策略；只在 `SERIAL_QUEUE` 并发下生效，决定超过 backlog 上限时丢弃哪一端。
    JobSerialOverflowPolicy {
        SkipOldest => "SKIP_OLDEST",
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
        Json => "JSON",
        Protobuf => "PROTOBUF",
        Raw => "RAW",
    }
}
