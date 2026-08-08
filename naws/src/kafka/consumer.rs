//! 严格 header 路由到本地 Sender 的同步借用式消费者。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nafka::{
    BorrowedKafkaRecord, GroupSpec, InvalidRecordReason, PassthroughConsumer,
    PassthroughDeadLetterReason, PassthroughDisposition, PassthroughFailure, PassthroughHaltReason,
    PassthroughRetryReason, PassthroughSkipReason,
};

use super::headers::{
    parse_route, DeliveryPolicy, FanoutOverflowPolicy, GroupMode, PassthroughConfig,
    SourceIdentityPolicy, NAWS_PUSH_EVENT,
};
use crate::proto::{CodecError, MessageRef};
use crate::{BorrowedSendResult, NodeRegistry, SendReport, Sender};

/// 来源节点 fencing 接口。
pub trait SourceFence: Send + Sync + 'static {
    /// 业务作用：判断来源 incarnation 是否仍有效；false 表示旧实例迟到数据。
    ///
    /// # 参数
    ///
    /// - `node`: 来源逻辑节点。
    /// - `incarnation`: 已解析的非零版本号。
    fn accept(&self, node: &str, incarnation: u128) -> bool;
}

impl SourceFence for NodeRegistry {
    /// 业务作用：复用节点目录的数值 incarnation 围栏，并只在通过时刷新活跃时间。
    fn accept(&self, node: &str, incarnation: u128) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
            });
        self.touch_fenced(node, incarnation, now)
    }
}

/// 进程内预注册的 `sink_id → Sender` 白名单。
#[derive(Clone)]
pub struct SenderSlot {
    /// 未指定 sink id 时使用的默认 Sender。
    default: Arc<Sender>,
    /// 多个监听服务的显式白名单。
    sinks: Arc<BTreeMap<String, Arc<Sender>>>,
}

impl SenderSlot {
    /// 业务作用：返回白名单内所有 Sender 的最小单帧上限。
    ///
    /// 交叉校验必须取最小值：任何一个目标 Sender 装不下，那条记录就会 Halt 数据面。
    pub(crate) fn min_max_frame(&self) -> usize {
        std::iter::once(&self.default)
            .chain(self.sinks.values())
            .map(|sender| sender.max_frame())
            .min()
            .unwrap_or(usize::MAX)
    }

    /// 业务作用：以一个默认 Sender 构造空白名单。
    ///
    /// # 参数
    ///
    /// - `default`: 缺省本地投递目标。
    pub fn new(default: Arc<Sender>) -> Self {
        Self {
            default,
            sinks: Arc::new(BTreeMap::new()),
        }
    }

    /// 业务作用：追加一个启动期固定 sink。
    ///
    /// # 参数
    ///
    /// - `id`: 非空逻辑标识，不能包含控制字符。
    /// - `sender`: 对应已配置本地监听服务的 Sender。
    ///
    /// # 错误
    ///
    /// id 非法或重复时返回文本。
    pub fn with_sink(mut self, id: impl Into<String>, sender: Arc<Sender>) -> Result<Self, String> {
        let id = id.into();
        if id.trim().is_empty() || id.chars().any(char::is_control) {
            return Err("sink id 不能为空或包含控制字符".into());
        }
        let sinks = Arc::make_mut(&mut self.sinks);
        if sinks.insert(id, sender).is_some() {
            return Err("sink id 重复".into());
        }
        Ok(self)
    }

    /// 业务作用：只从预注册白名单解析 Sender。
    ///
    /// # 参数
    ///
    /// - `id`: 可选 header 值。
    fn resolve(&self, id: Option<&str>) -> Option<&Arc<Sender>> {
        match id {
            Some(id) => self.sinks.get(id),
            None => Some(&self.default),
        }
    }
}

/// 把 raw value 与路由 headers 同步推入本地有界 outbox 的适配器。
pub struct HeaderPassthroughAdapter {
    /// 唯一数据 topic。
    topic: String,
    /// 当前节点逻辑 ID。
    local_node: String,
    /// 冻结配置。
    config: PassthroughConfig,
    /// 预注册 Sender 白名单。
    senders: SenderSlot,
    /// 可选来源 fencing 实现。
    source_fence: Option<Arc<dyn SourceFence>>,
    /// 注册时解析好的 group 规则。
    group: GroupSpec,
}

impl HeaderPassthroughAdapter {
    /// 业务作用：构造并校验一个数据面 adapter。
    ///
    /// # 参数
    ///
    /// - `topic`: 非空 data topic。
    /// - `local_node`: 当前节点逻辑 ID，只参与过滤，绝不作为网络地址。
    /// - `config`: 路由、容量、mode 与 group 配置。
    /// - `senders`: 启动期预注册 Sender 白名单。
    /// - `source_fence`: Optional/Required 来源策略下必须提供的 fencing 实现。
    ///
    /// # 错误
    ///
    /// 配置、topic、node 或 fencing 装配不满足不变量时返回文本。
    pub fn new(
        topic: impl Into<String>,
        local_node: impl Into<String>,
        config: PassthroughConfig,
        senders: SenderSlot,
        source_fence: Option<Arc<dyn SourceFence>>,
    ) -> Result<Self, String> {
        config.validate()?;
        let topic = topic.into();
        let local_node = local_node.into();
        if topic.trim().is_empty() || local_node.trim().is_empty() {
            return Err("data topic 与 local node 不能为空".into());
        }
        if config.source_identity_policy != SourceIdentityPolicy::Forbidden
            && source_fence.is_none()
        {
            return Err("允许来源身份时必须注入 SourceFence".into());
        }
        let group = match &config.group_mode {
            GroupMode::EphemeralNode { scope } => {
                GroupSpec::Broadcast(format!("{scope}-{local_node}"))
            }
            GroupMode::Durable { group } => GroupSpec::Named(group.clone()),
        };
        Ok(Self {
            topic,
            local_node,
            config,
            senders,
            source_fence,
            group,
        })
    }

    /// 业务作用：把发送报告映射成配置化成功或重试结果。
    ///
    /// # 参数
    ///
    /// - `report`: 本地 Sender 的聚合结果。
    fn disposition(
        &self,
        report: SendReport,
    ) -> Result<PassthroughDisposition, PassthroughFailure> {
        if report.dropped == 0 && report.closed == 0 {
            return Ok(PassthroughDisposition::Handled {
                queued: report.queued,
            });
        }
        match self.config.delivery_policy {
            DeliveryPolicy::RequireAllEnqueued => Err(PassthroughFailure::retryable(
                PassthroughRetryReason::SocketEnqueue,
                format!(
                    "outbox 未全部入队 dropped={} closed={}",
                    report.dropped, report.closed
                ),
            )),
            DeliveryPolicy::BestEffort => Ok(PassthroughDisposition::BestEffortDropped {
                queued: report.queued,
                dropped: report.dropped,
                closed: report.closed,
            }),
        }
    }
}

impl PassthroughConsumer for HeaderPassthroughAdapter {
    /// 业务作用：返回唯一 data topic。
    fn topics(&self) -> Vec<String> {
        vec![self.topic.clone()]
    }

    /// 业务作用：返回固定 adapter 路由事件。
    fn event(&self) -> String {
        NAWS_PUSH_EVENT.into()
    }

    /// 业务作用：返回构造时冻结的独占或稳定 group。
    fn group(&self) -> GroupSpec {
        self.group.clone()
    }

    /// 业务作用：在 owner 栈内完成 header 校验、目标门禁、最终 frame 编码与有界 outbox 入队。
    fn consume_borrowed(
        &self,
        record: BorrowedKafkaRecord<'_>,
    ) -> Result<PassthroughDisposition, PassthroughFailure> {
        let started = Instant::now();
        let route = parse_route(&record.headers, &self.config).map_err(|error| {
            PassthroughFailure::invalid(InvalidRecordReason::MalformedRoute, error)
        })?;
        let payload = record.payload.ok_or_else(|| {
            PassthroughFailure::invalid(
                InvalidRecordReason::MissingPayload,
                "data topic 不允许 tombstone",
            )
        })?;

        if !route.target_nodes.is_empty()
            && !route
                .target_nodes
                .iter()
                .any(|node| *node == self.local_node)
        {
            return Ok(PassthroughDisposition::Skipped {
                reason: PassthroughSkipReason::TargetNodeMismatch,
            });
        }
        if route.source_node == Some(self.local_node.as_str()) {
            return Ok(PassthroughDisposition::Skipped {
                reason: PassthroughSkipReason::Loopback,
            });
        }
        if let (Some(node), Some(incarnation), Some(fence)) = (
            route.source_node,
            route.source_incarnation,
            &self.source_fence,
        ) {
            if !fence.accept(node, incarnation) {
                return Ok(PassthroughDisposition::Skipped {
                    reason: PassthroughSkipReason::StaleSource,
                });
            }
        }
        let sender = self.senders.resolve(route.sink_id).ok_or_else(|| {
            PassthroughFailure::invalid(
                InvalidRecordReason::MalformedRoute,
                "sink id 未在启动期白名单注册",
            )
        })?;
        let message = MessageRef {
            events: Some(route.events.as_slice()),
            from_uid: route.from_uid,
            from_client: route.from_client,
            routers: (!route.routers.is_empty()).then_some(route.routers.as_slice()),
            group: route.group,
            uids: (!route.uids.is_empty()).then_some(route.uids.as_slice()),
            excludes: (!route.excludes.is_empty()).then_some(route.excludes.as_slice()),
            payload: Some(payload),
            ack_id: 0,
        };
        let report = match sender
            .send_local_borrowed_bounded(
                &message,
                route.message_mode,
                self.config.max_fanout_targets,
            )
            .map_err(map_codec_error)?
        {
            BorrowedSendResult::NoLocalTarget => {
                return Ok(PassthroughDisposition::Skipped {
                    reason: PassthroughSkipReason::NoLocalTarget,
                })
            }
            BorrowedSendResult::TooManyTargets => {
                return Err(match self.config.fanout_overflow_policy {
                    FanoutOverflowPolicy::Halt => PassthroughFailure::halt(
                        PassthroughHaltReason::RouteExpansionTooLarge,
                        "fan-out targets 超过配置上限",
                    ),
                    FanoutOverflowPolicy::DeadLetter => PassthroughFailure::dead_letter(
                        PassthroughDeadLetterReason::RouteExpansionTooLarge,
                        "fan-out targets 超过配置上限",
                    ),
                })
            }
            BorrowedSendResult::Sent(report) => report,
        };
        let targets = report.queued + report.dropped + report.closed;
        let disposition = self.disposition(report)?;
        if started.elapsed().as_millis() > u128::from(self.config.callback_warn_ms) {
            tracing::warn!(
                topic = record.topic,
                partition = record.partition,
                elapsed_ms = started.elapsed().as_millis(),
                targets,
                "消息队列到 socket 的同步 callback 超过时长预算"
            );
        }
        Ok(disposition)
    }
}

/// 业务作用：把协议编码错误映射为类型化 passthrough 失败。
///
/// # 参数
///
/// - `error`: 借用 frame 编码错误。
fn map_codec_error(error: CodecError) -> PassthroughFailure {
    match error {
        CodecError::LengthExceeds
        | CodecError::LengthOverflow
        | CodecError::BufferTooSmall { .. } => PassthroughFailure::halt(
            PassthroughHaltReason::FrameTooLarge,
            "最终 frame 超过容量上限",
        ),
        CodecError::Unsupported(_) => PassthroughFailure::invalid(
            InvalidRecordReason::UnsupportedMessageMode,
            "消息 mode 尚未实现",
        ),
        _ => PassthroughFailure::invalid(
            InvalidRecordReason::MalformedRoute,
            "消息信封无法按声明 mode 编码",
        ),
    }
}
