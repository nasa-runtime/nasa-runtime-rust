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
    /// 参数说明：
    ///
    /// - `node`: 来源逻辑节点。
    /// - `incarnation`: 已解析的非零版本号。
    ///
    /// 返回：允许本次来源实例继续投递时为 `true`，判定为过期来源时为 `false`；
    /// 实现可更新节点活跃观测，但不得将此结果代替生产者或业务目标的权限校验。
    fn accept(&self, node: &str, incarnation: u128) -> bool;
}

impl SourceFence for NodeRegistry {
    /// 业务作用：复用节点目录的数值 incarnation 围栏，并只在通过时刷新活跃时间。
    ///
    /// 参数说明：
    /// - `node`: 记录声明的来源逻辑节点。
    /// - `incarnation`: 已在路由解析阶段检查为非零的来源实例代际。
    ///
    /// 返回：来源不旧于目录中实例时接受并刷新本机毫秒时间；旧实例返回 `false` 且不续期。
    /// 未知节点可建立目录记录，较新代际可替换旧状态；此判断不认证来源声明。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认 Sender 与所有显式 sink 中最小的单帧上限，用于启动前交叉校验。
    /// 不估算特定消息编码长度，不改变任何 Sender 的配置。
    pub(crate) fn min_max_frame(&self) -> usize {
        std::iter::once(&self.default)
            .chain(self.sinks.values())
            .map(|sender| sender.max_frame())
            .min()
            .unwrap_or(usize::MAX)
    }

    /// 业务作用：以一个默认 Sender 构造空白名单。
    ///
    /// 参数说明：
    ///
    /// - `default`: 缺省本地投递目标。
    ///
    /// 返回：没有显式 sink 映射的选择表；记录未带 sink ID 时始终选择所给默认 Sender。
    pub fn new(default: Arc<Sender>) -> Self {
        Self {
            default,
            sinks: Arc::new(BTreeMap::new()),
        }
    }

    /// 业务作用：追加一个启动期固定 sink。
    ///
    /// 参数说明：
    ///
    /// - `id`: 非空逻辑标识，不能包含控制字符。
    /// - `sender`: 对应已配置本地监听服务的 Sender。
    ///
    /// # 错误
    ///
    /// id 非法或重复时返回文本。
    ///
    /// 返回：包含新增 sink 的选择表；标识为空白、含控制字符或已存在时返回错误并丢弃本次接收的表。
    /// 映射采用写时复制，其它已克隆选择表保持原样；不替换默认 Sender，也不在线查询目标服务。
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
    /// 参数说明：
    ///
    /// - `id`: 可选 header 值。
    ///
    /// 返回：未指定 ID 时借用默认 Sender；显式 ID 只查白名单，未知值返回 `None`，不回退到默认目标。
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
    /// 参数说明：
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
    ///
    /// 返回：保存固定 topic、节点、Sender 选择表与消费组规则的同步适配器；无效配置返回错误。
    /// 不启动 Kafka 消费、不创建 topic，也不替代运行时的容量交叉校验和来源认证。
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
        // 允许来源声明时必须同时装配代际判定，避免仅凭文本身份放行旧进程记录。
        if config.source_identity_policy != SourceIdentityPolicy::Forbidden
            && source_fence.is_none()
        {
            return Err("允许来源身份时必须注入 SourceFence".into());
        }
        let group = match &config.group_mode {
            GroupMode::EphemeralNode {
                scope,
            } => GroupSpec::Broadcast(format!("{scope}-{local_node}")),
            GroupMode::Durable {
                group,
            } => GroupSpec::Named(group.clone()),
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
    /// 参数说明：
    ///
    /// - `report`: 本地 Sender 的聚合结果。
    ///
    /// 返回：没有丢弃和关闭目标时为 `Handled`；部分失败按策略返回可重试错误或带计数的尽力投递结果。
    /// 不撤回已入队消息，记录重试可能再次投递给此前成功的目标；成功也只确认入队，不确认客户端处理。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含构造时 topic 名称的拥有型列表，供 nafka 注册订阅，不在此向 broker 发起订阅。
    fn topics(&self) -> Vec<String> {
        vec![self.topic.clone()]
    }

    /// 业务作用：返回固定 adapter 路由事件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`NAWS_PUSH` 的拥有型字符串，用于将 Kafka 记录分派到此适配器；不代表客户端事件名称。
    fn event(&self) -> String {
        NAWS_PUSH_EVENT.into()
    }

    /// 业务作用：返回构造时冻结的独占或稳定 group。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已保存的广播组或具名组规则副本；最终组 ID 解析与 Kafka 消费位置管理由 nafka 承担。
    fn group(&self) -> GroupSpec {
        self.group.clone()
    }

    /// 业务作用：在 owner 栈内完成 header 校验、目标门禁、最终 frame 编码与有界 outbox 入队。
    ///
    /// 参数说明：
    /// - `record`: 仅在当前回调期间有效的 Kafka 记录借用视图，payload 必须存在。
    ///
    /// 返回：按目标与来源过滤结果跳过记录，或返回本地入队处理结果；畸形路由、容量及编码失败按类型报告。
    /// 不自行提交 offset，不等待客户端确认，不把 payload 反序列化为业务对象。
    /// 部分入队后的重试不会回滚已成功目标；慢回调告警只覆盖走到末尾的正常结果路径。
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

        // 节点过滤先于本地展开，只能在记录明确允许的逻辑节点执行投递。
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
        // 本节点已承担原始本地投递，避免经集群再回送同一份数据。
        if route.source_node == Some(self.local_node.as_str()) {
            return Ok(PassthroughDisposition::Skipped {
                reason: PassthroughSkipReason::Loopback,
            });
        }
        // 代际判断在目标选择和入队之前执行，旧实例迟到消息不能继续影响本地连接。
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
        // 显式但未知的 sink 必须拒绝，不能回退到默认监听服务扩大投递范围。
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
                });
            }
            BorrowedSendResult::TooManyTargets => {
                // 超出展开容量时按配置停止分区或转死信，不悄悄截取部分目标当作成功。
                return Err(match self.config.fanout_overflow_policy {
                    FanoutOverflowPolicy::Halt => PassthroughFailure::halt(
                        PassthroughHaltReason::RouteExpansionTooLarge,
                        "fan-out targets 超过配置上限",
                    ),
                    FanoutOverflowPolicy::DeadLetter => PassthroughFailure::dead_letter(
                        PassthroughDeadLetterReason::RouteExpansionTooLarge,
                        "fan-out targets 超过配置上限",
                    ),
                });
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
/// 参数说明：
///
/// - `error`: 借用 frame 编码错误。
///
/// 返回：长度或缓冲容量错误映射为停止处理的帧超限原因，未支持模式映射为无效模式，其余为畸形路由。
/// 只生成分类与固定说明，不提交 offset、不执行死信发送，也不暴露原始负载。
fn map_codec_error(error: CodecError) -> PassthroughFailure {
    match error {
        CodecError::LengthExceeds
        | CodecError::LengthOverflow
        | CodecError::BufferTooSmall {
            ..
        } => PassthroughFailure::halt(
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
