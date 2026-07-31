//! Redis 客户端、分布式锁、管线、发布订阅和分区消费基础层。
//!
//! 业务通常通过 `nasa::redis` 门面接入；本 crate 承载单点/集群 Redis 的类型化命令、
//! 连接治理和分布式协作能力。
// ============================================================================
// nadis —— Redis 五件套移植的基础层(架构设计文档)。
//
// 对照 原实现 原框架-boot-starter-原工具包 的 cache/redis 包:
//   RedisProxy            → client::RedisClient + commands(类型化命令层)
//   LettucePipeline       → pipeline::PipelineSession(显式批,typed ticket)
//   LettuceDistributedLock→ lock::DistributedLock(4 Lua 逐字节照搬,V1 可与 原实现 互锁)
//   RedisPartition        → (R4,reducer 阶段后)
//   RediSearch            → (R5)
//
// 本轮已实现(R1+R2+R3):
//   config  —— RedisConfig + CompatibilityProfile(无默认值,缺失即配置错)
//   error   —— 统一错误(含 ExecutionUnknown:写出后断线="可能已执行")
//   keytag  —— redis_slot 逐字节复刻(CRC16+有效 hash tag 规则)+ synthetic tag 表
//   codec   —— Json<T> 包装(serde_json;原实现 互通 codec,为显式 feature 预留)
//   client  —— connect(协议标记 SET NX/校验)/registry/shutdown
//   commands—— kv/hash/zset/通用 子集(KeyTtl 类型化返回;KEYS 不提供,SCAN 流式)
//   pipeline—— PipelineSession(本地 buffer,到 max_commands/max_bytes 阈值滚动 auto-flush 续接,move 语义)
//   lock    —— try_lock/lock/with_lock + 本地重入(StillReentered)+ 看门狗 + HoldStatus 三态
//
// 设计红线(文档 差异表):同步 API→async;对象池→move;threadId 重入→Guard 显式重入;
// publish 静默→Err;Drop 仅 best-effort,正确性入口 = 显式 unlock/with_lock。
// ============================================================================

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
/// RediSearch/RedisJSON 封装(feature "search";R5)——只用 lock/pipeline/
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
    PreparedPartition, RunningPartition,
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
