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
    /// 参数说明：
    ///
    /// - `lane`: 固定 data producer lane。
    /// - `topic`: 固定 data topic。
    /// - `config`: 必须要求完整来源身份的 header 契约。
    ///
    /// # 错误
    ///
    /// topic 为空、来源策略不是 Required 或 passthrough 配置非法时返回文本。
    ///
    /// 返回：固定 topic 且要求来源身份的发布器；失败时不发布记录。
    /// 此构造入口不绑定 readiness 或预期本节点身份，也不查询 broker topic 是否存在。
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
    ///
    /// 参数说明：
    /// - `lane`: 运行时已选定的数据生产通道。
    /// - `topic`: 固定数据 topic，非空约束由基础构造过程检查。
    /// - `config`: 与消费端一致且要求完整来源身份的路由头策略。
    /// - `ingress_ready`: 与数据接入共享的就绪标志，发布时为 false 则拒绝进入生产者。
    /// - `expected_local_node`: 接入侧用于识别本节点回环的节点 ID，供首次发布时交叉比较。
    ///
    /// 返回：基础配置校验通过后附带就绪标志和节点身份期望的发布器；不修改共享就绪状态。
    /// 无效基础配置返回错误，节点身份文本不在此额外验证。
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
    ///
    /// 参数说明：
    /// - `builder`: 本次待发布记录的临时构造器。
    /// - `values`: 可缺失的字符串列表；列表内的 None 为非法元素。
    /// - `append`: 将一个已存在的字符串追加到对应路由字段的操作。
    ///
    /// 返回：全部元素存在时返回追加后的构造器，列表缺失时原样返回；遇到 None 则丢弃临时构造器并报错。
    /// 空字符串会传给追加操作，最终有效性仍由发布校验决定；本方法不发布记录。
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
    ///
    /// 参数说明：
    /// - `error`: 校验或同步提交到生产者时产生的 nafka 错误。
    ///
    /// 返回：生命周期错误映射为 `Closed`，生产队列容量错误映射为 `Full`，其余为 `Rejected`。
    /// 不保留原错误详情，不重试，也不能表示异步 broker 确认结果。
    fn map_error(error: NafkaError) -> DataPublishOutcome {
        match error {
            NafkaError::Lifecycle(_) => DataPublishOutcome::Closed,
            NafkaError::ProducerQueueFull {
                ..
            } => DataPublishOutcome::Full,
            _ => DataPublishOutcome::Rejected,
        }
    }
}

impl KafkaClusterDataPublisher {
    /// 业务作用：首次发布时对比来源节点与接入侧回环识别节点，暴露可能导致重复投递的配置不一致。
    /// `Source-Node` 来自 Cluster，期望值来自 `WsKafkaRuntimeConfig.local_node`；所有克隆共享一次性标记。
    ///
    /// 参数说明：
    ///
    /// - `actual`: Cluster 本次发布使用的来源节点 ID。
    ///
    /// 返回：未配置期望值或已经检查过时不做操作；首次检查先占用标记，不一致时输出错误日志。
    /// 不拒绝当前发布，不复验后续来源，也不构成节点身份认证或就绪门禁。
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
    ///
    /// 参数说明：
    /// - `source`: 本次集群来源节点与实例标识，节点名同时用作 Kafka 分区键。
    /// - `message`: 业务信封；payload 必须存在，其余支持的路由字段写入 header。
    /// - `mode`: 下游编码消息时使用的模式，须在发布器允许范围内。
    /// - `target_nodes`: 可选的逻辑目标节点列表，供消费端筛选本地投递。
    ///
    /// 返回：就绪门禁或生产者已关闭时为 `Closed`，队列容量不足为 `Full`；
    /// 负载缺失、列表含 None、路由或模式不合法等情况为 `Rejected`；同步接纳为 `Queued`。
    /// 不等待 broker 确认，路由字符串会复制到 header，payload 不再包入 ClusterEvent 信封。
    fn publish(
        &self,
        source: ClusterSourceRef<'_>,
        message: &Message,
        mode: Mode,
        target_nodes: Option<&[&str]>,
    ) -> DataPublishOutcome {
        // 与接入侧共享就绪状态，防止目录或数据接入尚不可用时继续向集群发布。
        if self
            .ingress_ready
            .as_ref()
            .is_some_and(|ready| !ready.load(Ordering::Acquire))
        {
            return DataPublishOutcome::Closed;
        }
        self.check_identity_once(source.node);
        // 缺失与空负载语义不同；缺失负载不能静默变成可投递的空记录。
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
