//! Redis 客户端、分布式锁、管线、发布订阅和分区消费基础层。
//!
//! 分区消费通过 Redis 租约与 PEL 完成持久接管，由实例独占的 napart Runner 集合执行业务键有序任务。
//! 本地调度支持 source、group、stream 隔离，跨执行域的同计划同业务键仍保持顺序屏障。
//! 成功业务与 ACK 分离，未知确认保留提交责任；源级总预算覆盖读取、执行、重试和发布，
//! group/stream 将消费容量划为固定域份额，跨域不借用；发布仍使用源级独立预算。
//! 正常停机报告未收敛时可以继续等待同一操作，显式强停才请求有损中止。
//!
//! 业务通常通过 `nasa::redis` 门面接入；本 crate 承载单点/集群 Redis 的类型化命令、
//! 连接治理和分布式协作能力。
// 核心模块分别拥有连接与拓扑、类型化命令、显式 pipeline、锁、分区消费和搜索边界。
// 写出后断线统一归类为 ExecutionUnknown；Drop 只承担 best-effort，涉及权威释放时必须使用显式入口。

/// Redis 连接建立、拓扑识别和统一执行入口。
pub mod client;
/// Redis 值序列化包装类型。
pub mod codec;
/// 类型化 Redis 命令集合。
pub mod commands;
/// Redis 连接、命令、锁、stream 和 partition 配置。
pub mod config;
/// Redis 组件统一错误类型。
pub mod error;
/// nonce 幂等计数：String/Hash/ZSet 计数的 nonce 窗口幂等，账本身份为稳定线协议。
pub mod idempotent;
/// RedisJob 分布式任务运行时(feature "job")：调度、执行器注册、租约、Fanout 与有预算停机。
#[cfg(feature = "job")]
pub mod job;
/// Redis Cluster hash tag 与 slot 计算工具。
pub mod keytag;
/// 基于分布式锁的 leader 选举辅助。
pub mod leader;
/// Redis 分布式锁与本地重入守卫。
pub mod lock;
/// 基于 Redis Stream 的分区消费、重平衡和处置控制。
pub mod partition;
/// 显式 pipeline、typed ticket 和自动微批管线。
pub mod pipeline;
/// Redis Stream 代理消费运行时。
pub mod proxy;
/// Redis pub/sub 订阅和发布封装。
pub mod pubsub;
/// RediSearch/RedisJSON 封装(feature "search")——只用 lock/pipeline/
/// partition 的服务不编译它(门面侧经 "redis-search" 透传)。
#[cfg(feature = "search")]
pub mod search;
/// 雪花 ID 生成器(对照 原实现 `JdkSnowflake`):纯算法 [`Snowflake`] + Redis ZSET 分配 workerId。
pub mod snowflake;
/// 普通 stream(event-field wire)的 publish/subscribe。
pub mod stream;

pub use client::{RedisClient, RedisRegistry};
pub use codec::Json;
pub use commands::KeyTtl;
pub use config::{CompatibilityProfile, PartitionGroupCfg, RedisConfig};
pub use error::NasaRedisError;
pub use idempotent::{
    IdempotentCounterCfg, IdempotentCounterError, IdempotentCounterSnapshot,
    IdempotentPipelineSession, IdempotentRejection, IdempotentTicket, IdempotentTtlMode,
};
// crate 根导出统一 Result:README/业务惯用 `nadis::Result<T>`,免去 `nadis::error::Result` 长路径。
pub use error::Result;
pub use keytag::{effective_tag, redis_slot, synthetic_tag};
pub use leader::Leader;
pub use lock::{DistributedLock, HoldStatus, LockGuard};
/// #[derive(RedisDocument)](feature "derive";宏与同名 trait 分属 macro/type
/// 两个命名空间,可同名共存——`use nadis::RedisDocument` 同时引入两者)。
#[cfg(feature = "derive")]
pub use nadis_derive::RedisDocument;
pub use partition::{
    compat_double_to_string, compat_long_hash, compat_string_hash, route_i64, route_str,
    ExecutionDomainSnapshot, PartitionExecutorCfg, PartitionExecutorScope, PartitionLimits,
    PartitionRecord, PartitionShutdownReport, PartitionSnapshot, PreparedPartition,
    PublisherSnapshot, RecordIdentity, RunningPartition,
};
pub use pipeline::{AutoPipeline, MicroBatchCfg, PipelineSession, Ticket};
pub use proxy::{PreparedProxy, ProxyCfg, ProxyPoison, ProxyStartOffset, RunningProxy};
pub use pubsub::{Message, Subscription};
#[cfg(feature = "search")]
pub use search::actuator::{IndexPolicy, SearchActuator};
#[cfg(feature = "search")]
pub use search::array::JsonArrayOps;
#[cfg(feature = "search")]
pub use search::query::{Aggregate, GeoUnit, Query as SearchQuery, Reducer};
#[cfg(feature = "search")]
pub use search::{
    check_part, to_json_omit_null, DataType, DocMeta, FieldMeta, FieldType, KeySeg, RedisDocument,
};
pub use snowflake::{Snowflake, SnowflakeConfig, SnowflakeError, WorkerIdLease};
pub use stream::{
    StreamAckPolicy, StreamEntry, StreamEnvelope, StreamEvent, StreamField, StreamGroupStart,
    StreamMode, StreamPublishItem, StreamStart, StreamSubscribeCfg, StreamSubscriber,
    StreamSubscription, StreamTypedEvent, STREAM_EVENT,
};
