//! 消息队列承载的长连接 data plane。

mod consumer;
mod data_publisher;
mod headers;
mod publisher;
mod runtime;
mod topic_contract;

pub use consumer::{HeaderPassthroughAdapter, SenderSlot, SourceFence};
pub use data_publisher::KafkaClusterDataPublisher;
pub use headers::{
    parse_route, BorrowedWsRoute, DeliveryPolicy, FanoutOverflowPolicy, GroupMode,
    PassthroughConfig, SourceIdentityPolicy, HEADER_WS_EVENT, HEADER_WS_EXCLUDE_UID,
    HEADER_WS_FROM_CLIENT, HEADER_WS_FROM_UID, HEADER_WS_GROUP, HEADER_WS_MESSAGE_MODE,
    HEADER_WS_PAYLOAD_LAYOUT, HEADER_WS_ROUTER, HEADER_WS_SINK_ID, HEADER_WS_SOURCE_INCARNATION,
    HEADER_WS_SOURCE_NODE, HEADER_WS_TARGET_NODE, HEADER_WS_UID, NAWS_PUSH_EVENT,
    PAYLOAD_LAYOUT_RAW,
};
pub use publisher::{WsKafkaPublishBuilder, WsKafkaPublisher};
pub use runtime::{
    KafkaNotifier, WsKafkaPlaneStats, WsKafkaRuntime, WsKafkaRuntimeConfig, WsKafkaRuntimeHealth,
    DEFAULT_PROTOCOL_HEADER_BUDGET_BYTES, NAWS_CONTROL_EVENT,
};
pub use topic_contract::{
    TopicContractLimits, TopicManagement, WsKafkaTopicContract, WsKafkaTopicSpec,
};
