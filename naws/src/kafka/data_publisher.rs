//! Cluster data plane 到固定 Kafka topic/lane 的少拷贝发布器。

use nafka::{NafkaError, ProducerLane};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::{PassthroughConfig, SourceIdentityPolicy, WsKafkaPublishBuilder, WsKafkaPublisher};
use crate::cluster::{ClusterDataPublisher, ClusterSourceRef, DataPublishOutcome};
use crate::proto::{Message, Mode};

/// 把 `Message` 的 payload 直接作为 Kafka value、其余路由写入固定 header 的发布器。
#[derive(Clone)]
pub struct KafkaClusterDataPublisher {
    /// 已绑定 data 物理 lane 的安全 publisher。
    publisher: WsKafkaPublisher,
    /// runtime 配置声明的本节点 ID；与 Cluster 传入的来源身份做一次性交叉校验。
    expected_local_node: Option<String>,
    /// 身份不一致告警只打一次，避免热路径日志风暴（Arc 以保持 Clone 语义共享同一次性标记）。
    identity_checked: Arc<std::sync::atomic::AtomicBool>,
    /// 冻结 data topic。
    topic: String,
    /// WsKafkaRuntime 注入的可选 ingress ready 门禁。
    ingress_ready: Option<Arc<AtomicBool>>,
}

impl KafkaClusterDataPublisher {
    /// 业务作用：构造 cluster relay 专用 data publisher。
    ///
    /// # 参数
    ///
    /// - `lane`: 固定 data producer lane。
    /// - `topic`: 固定 data topic。
    /// - `config`: 必须要求完整来源身份的 header 契约。
    ///
    /// # 错误
    ///
    /// topic 为空、来源策略不是 Required 或 passthrough 配置非法时返回文本。
    pub fn new(
        lane: ProducerLane,
        topic: impl Into<String>,
        config: PassthroughConfig,
    ) -> Result<Self, String> {
        let topic = topic.into();
        if topic.trim().is_empty() {
            return Err("cluster data topic 不能为空".into());
        }
        if config.source_identity_policy != SourceIdentityPolicy::Required {
            return Err("cluster data publisher 必须要求来源身份".into());
        }
        Ok(Self {
            publisher: WsKafkaPublisher::new(lane, config)?,
            expected_local_node: None,
            identity_checked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            topic,
            ingress_ready: None,
        })
    }

    /// 业务作用：构造受 runtime readiness 控制的 cluster data publisher。
    pub(crate) fn new_gated(
        lane: ProducerLane,
        topic: impl Into<String>,
        config: PassthroughConfig,
        ingress_ready: Arc<AtomicBool>,
        expected_local_node: impl Into<String>,
    ) -> Result<Self, String> {
        let mut publisher = Self::new(lane, topic, config)?;
        publisher.ingress_ready = Some(ingress_ready);
        publisher.expected_local_node = Some(expected_local_node.into());
        Ok(publisher)
    }

    /// 业务作用：把拥有型可空字符串列表追加到 builder，空元素确定拒绝。
    fn append_values<'a>(
        mut builder: WsKafkaPublishBuilder<'a>,
        values: Option<&[Option<String>]>,
        append: impl Fn(WsKafkaPublishBuilder<'a>, &str) -> WsKafkaPublishBuilder<'a>,
    ) -> Result<WsKafkaPublishBuilder<'a>, ()> {
        if let Some(values) = values {
            for value in values {
                let value = value.as_deref().ok_or(())?;
                builder = append(builder, value);
            }
        }
        Ok(builder)
    }

    /// 业务作用：把 nafka fire 失败压缩成 Cluster data 稳定结果域。
    fn map_error(error: NafkaError) -> DataPublishOutcome {
        match error {
            NafkaError::Lifecycle(_) => DataPublishOutcome::Closed,
            NafkaError::ProducerQueueFull { .. } => DataPublishOutcome::Full,
            _ => DataPublishOutcome::Rejected,
        }
    }
}

impl KafkaClusterDataPublisher {
    /// 业务作用：一次性校验「egress 使用的来源身份」与「ingress 用来判回环的本节点 ID」是否一致。
    ///
    /// 二者来自两个独立配置：egress 的 `Source-Node` 由 Cluster 提供，
    /// ingress 的回环判定用 `WsKafkaRuntimeConfig.local_node`。不一致时 Loopback 永不命中，
    /// **本节点发出的每条消息都会被自己再投递一次**，且没有任何报错——
    /// 首次发布时必须比对该值并显式告警，避免编解码配置不一致导致静默数据损坏。
    ///
    /// 不拒绝发布：此刻拒绝等于让一次配置疏漏直接切断数据面，代价大于重复投递。
    ///
    /// # 参数
    ///
    /// - `actual`: Cluster 本次发布使用的来源节点 ID。
    fn check_identity_once(&self, actual: &str) {
        let Some(expected) = self.expected_local_node.as_deref() else {
            return;
        };
        if self
            .identity_checked
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if expected != actual {
            tracing::error!(
                expected,
                actual,
                "本节点身份不一致：egress 的 Source-Node 与 ingress 回环判定用的 local_node 不同，\
                 本节点消息将被自己重复投递(E11 #10)。请统一 Cluster 与 WsKafkaRuntimeConfig 的节点 ID"
            );
        }
    }
}

impl ClusterDataPublisher for KafkaClusterDataPublisher {
    /// 业务作用：不构造 ClusterEvent，直接借用 payload 并同步进入 data producer。
    fn publish(
        &self,
        source: ClusterSourceRef<'_>,
        message: &Message,
        mode: Mode,
        target_nodes: Option<&[&str]>,
    ) -> DataPublishOutcome {
        if self
            .ingress_ready
            .as_ref()
            .is_some_and(|ready| !ready.load(Ordering::Acquire))
        {
            return DataPublishOutcome::Closed;
        }
        self.check_identity_once(source.node);
        let Some(payload) = message.payload.as_deref() else {
            return DataPublishOutcome::Rejected;
        };
        let mut builder = self
            .publisher
            .publish(&self.topic, payload)
            .source_identity(source.node, source.incarnation)
            .message_mode(mode)
            .key(source.node);
        builder = match Self::append_values(builder, message.events.as_deref(), |item, value| {
            item.client_event(value)
        }) {
            Ok(builder) => builder,
            Err(()) => return DataPublishOutcome::Rejected,
        };
        builder = match Self::append_values(builder, message.routers.as_deref(), |item, value| {
            item.router(value)
        }) {
            Ok(builder) => builder,
            Err(()) => return DataPublishOutcome::Rejected,
        };
        builder = match Self::append_values(builder, message.uids.as_deref(), |item, value| {
            item.uid(value)
        }) {
            Ok(builder) => builder,
            Err(()) => return DataPublishOutcome::Rejected,
        };
        builder = match Self::append_values(builder, message.excludes.as_deref(), |item, value| {
            item.exclude_uid(value)
        }) {
            Ok(builder) => builder,
            Err(()) => return DataPublishOutcome::Rejected,
        };
        if let Some(group) = message.group.as_deref() {
            builder = builder.group(group);
        }
        if let Some(from_uid) = message.from_uid.as_deref() {
            builder = builder.from_uid(from_uid);
        }
        if let Some(from_client) = message.from_client.as_deref() {
            builder = builder.from_client(from_client);
        }
        if let Some(target_nodes) = target_nodes {
            for node in target_nodes {
                builder = builder.target_node(*node);
            }
        }
        match builder.fire() {
            Ok(()) => DataPublishOutcome::Queued,
            Err(error) => Self::map_error(error),
        }
    }
}
