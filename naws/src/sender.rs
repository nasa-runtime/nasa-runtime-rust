// ============================================================================
// src/sender.rs —— 出站 fan-out。编一次帧、`Bytes::clone` 给 N 个 outbox(零拷贝)。
// 路由优先级 + 去重见 架构说明。
// ============================================================================

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use naws_proto::{Message, MessageRef, Mode, WireCodec};

use crate::bridge::{json_quote, PayloadBridge};
use crate::session::{Protocol, SendOutcome, Session, SessionRegistry};
use crate::wire::{encode_event_frame_ref, encode_frame, frame_type};

/// 独立构造发送器时采用的默认单帧上限。
const DEFAULT_MAX_FRAME: usize = 16 * 1024 * 1024;

/// 集群发布钩子:集群启用时,send/relay 在本地 fan-out 后再跨节点 publish。
pub type ClusterPublish = Arc<dyn Fn(&Message, Mode) + Send + Sync>;

/// 本地 fan-out 投递报告(不再吞 SendOutcome,聚合可观测)。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SendReport {
    /// 成功入队的目标数。
    pub queued: usize,
    /// 因背压丢弃的目标数。
    pub dropped: usize,
    /// 因连接已关闭未投递的目标数。
    pub closed: usize,
}

/// 借用热路径在编码前完成有界目标解析后的投递结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowedSendResult {
    /// 当前节点没有匹配且就绪的本地 session，未分配最终 frame。
    NoLocalTarget,
    /// 唯一目标数超过调用方硬上限，未分配最终 frame且未写任何 outbox。
    TooManyTargets,
    /// 已对有界目标集合完成本地 outbox 入队决策。
    Sent(SendReport),
}

impl SendReport {
    /// 记录 record 事件；用于保存运行过程中的观测数据。
    ///
    /// # 参数
    /// - `outcome`: 命令、发送或任务执行结果。
    fn record(&mut self, outcome: SendOutcome) {
        match outcome {
            SendOutcome::Queued => self.queued += 1,
            SendOutcome::Dropped => self.dropped += 1,
            SendOutcome::Closed => self.closed += 1,
        }
    }
}

/// endpoint 当前代际解析器(Server 装配注入):出站投递的兜底校验用。
pub(crate) type EndpointGenResolver = Arc<dyn Fn(&str) -> Option<u64> + Send + Sync>;

/// 持有本地会话表和集群通知器；用于统一发送点对点、群组和广播消息。
pub struct Sender {
    registry: Arc<SessionRegistry>,
    cluster_publish: Mutex<Option<ClusterPublish>>,
    bridge: Arc<dyn PayloadBridge>,
    /// endpoint 代际解析器(未注入 = 不校验;手工裸装配时为 None)。
    endpoint_gen: Mutex<Option<EndpointGenResolver>>,
    /// 借用式编码路径的分配前单帧上限。
    max_frame: usize,
}

impl Sender {
    /// 构造发送器。
    ///
    /// # 参数
    /// - `registry`: 本节点会话注册表,用于本地 fan-out 到 uid/group/endpoint/session。
    /// - `bridge`: payload 桥接器,用于 socket.io 会话的出站 payload 转换。
    pub fn new(registry: Arc<SessionRegistry>, bridge: Arc<dyn PayloadBridge>) -> Arc<Sender> {
        Self::new_with_max_frame(registry, bridge, DEFAULT_MAX_FRAME)
    }

    /// 返回本 Sender 允许的单帧 body 上限。
    ///
    /// 供 kafka 装配层做启动期交叉校验：Kafka 记录经 TLV 信封编码后才成为 frame，
    /// 若 topic 允许的最大消息不小于本值，一条**合法**的大记录就会在编码期
    /// `FrameTooLarge` → `Halt`（不可重试、不进 DLT），等于外部可停掉数据面。
    #[cfg(feature = "kafka")]
    pub(crate) fn max_frame(&self) -> usize {
        self.max_frame
    }

    /// 按服务端生效配置构造发送器。
    ///
    /// # 参数
    ///
    /// - `registry`: 本节点会话注册表。
    /// - `bridge`: 不同客户端协议之间的 payload 转换器。
    /// - `max_frame`: 单条 frame body 的最大字节数。
    pub(crate) fn new_with_max_frame(
        registry: Arc<SessionRegistry>,
        bridge: Arc<dyn PayloadBridge>,
        max_frame: usize,
    ) -> Arc<Sender> {
        Arc::new(Sender {
            registry,
            cluster_publish: Mutex::new(None),
            bridge,
            endpoint_gen: Mutex::new(None),
            max_frame: max_frame.max(1),
        })
    }

    /// 框架内部:集群启动后注入跨节点发布钩子。
    pub(crate) fn set_cluster_publish(&self, f: ClusterPublish) {
        *self.cluster_publish.lock().unwrap() = Some(f);
    }

    /// 框架内部:注入 endpoint 代际解析器(出站投递兜底)。
    pub(crate) fn set_endpoint_gen_resolver(&self, f: EndpointGenResolver) {
        *self.endpoint_gen.lock().unwrap() = Some(f);
    }

    /// 客户端入站消息触发的自动转发(本地 + 跨节点)。返回**本地** fan-out 报告。
    ///
    /// # 参数
    /// - `msg`: 客户端入站的业务消息信封。
    /// - `mode`: `msg` 的编码模式,跨节点转发时需要保持一致。
    pub fn relay(&self, msg: &Message, mode: Mode) -> SendReport {
        let report = self.fan_out(msg, mode);
        self.publish_cluster(msg, mode);
        report
    }

    /// 服务端主动推送(本地 + 跨节点;不经 InboundPolicy)。返回**本地** fan-out 报告。
    ///
    /// # 参数
    /// - `msg`: 服务端要投递的业务消息信封。
    /// - `mode`: `msg` 的编码模式,跨节点转发时需要保持一致。
    pub fn send(&self, msg: &Message, mode: Mode) -> SendReport {
        let report = self.fan_out(msg, mode);
        self.publish_cluster(msg, mode);
        report
    }

    /// **仅本地** fan-out,不再跨节点 publish。集群收到对端事件后调,防 ping-pong 回环。
    ///
    /// # 参数
    /// - `msg`: 要在本节点本地投递的业务消息信封。
    /// - `mode`: `msg` 的编码模式,用于按目标会话协议编码出站帧。
    pub fn send_local(&self, msg: &Message, mode: Mode) -> SendReport {
        self.fan_out(msg, mode)
    }

    /// 把借用业务信封仅投递到本节点，不构造拥有型中间消息。
    ///
    /// 原生协议目标共享同一个最终 [`Bytes`]；文本协议目标按 namespace 转码，二者都不会
    /// 把借用数据带出本次同步调用。该边界适合消息队列消费回调直接转发底层 payload。
    ///
    /// # 参数
    ///
    /// - `msg`: 生命周期仅覆盖当前调用的借用业务信封。
    /// - `mode`: 原生协议目标采用的编码模式。
    ///
    /// # 错误
    ///
    /// 字段非法、编码模式未实现或消息在分配前已超过单帧上限时返回错误；此时不会投递
    /// 任何原生协议 frame。
    pub fn send_local_borrowed(
        &self,
        msg: &MessageRef<'_>,
        mode: Mode,
    ) -> naws_proto::Result<SendReport> {
        let targets = self.resolve_targets_ref(msg);
        self.send_borrowed_to_targets(msg, mode, targets)
    }

    /// 在单次有界目标解析后完成借用消息的本地投递。
    ///
    /// 该入口把 fan-out 上限检查放在最终 frame 编码和 outbox 写入之前，并避免先计数再重新解析。
    ///
    /// # 参数
    ///
    /// - `msg`: 当前同步调用内有效的借用业务信封。
    /// - `mode`: 原生协议目标采用的编码模式。
    /// - `max_targets`: 本次允许的本地唯一 session 数；零表示只允许无目标结果。
    ///
    /// # 错误
    ///
    /// 字段非法、编码模式未实现或最终 frame 超过单帧上限时返回协议错误。
    pub fn send_local_borrowed_bounded(
        &self,
        msg: &MessageRef<'_>,
        mode: Mode,
        max_targets: usize,
    ) -> naws_proto::Result<BorrowedSendResult> {
        let targets = match self.resolve_targets_ref_bounded(msg, max_targets) {
            Ok(targets) if targets.is_empty() => return Ok(BorrowedSendResult::NoLocalTarget),
            Ok(targets) => targets,
            Err(()) => return Ok(BorrowedSendResult::TooManyTargets),
        };
        self.send_borrowed_to_targets(msg, mode, targets)
            .map(BorrowedSendResult::Sent)
    }

    /// 对已经解析且有界的 session 集合编码一次并完成本地投递。
    ///
    /// # 参数
    ///
    /// - `msg`: 路由和 payload 借用视图。
    /// - `mode`: 原生协议目标采用的编码模式。
    /// - `targets`: 已去重、过滤并通过容量门禁的目标集合。
    ///
    /// # 错误
    ///
    /// 最终 frame 编码失败时返回协议错误；已经进入 outbox 的其他协议目标不会回滚。
    fn send_borrowed_to_targets(
        &self,
        msg: &MessageRef<'_>,
        mode: Mode,
        targets: Vec<Arc<Session>>,
    ) -> naws_proto::Result<SendReport> {
        let mut report = SendReport::default();
        let mut nasa_frame: Option<Bytes> = None;
        let mut sio_arg: Option<String> = None;
        for session in targets {
            match session.protocol() {
                Protocol::Nasa => {
                    let frame = match &nasa_frame {
                        Some(frame) => frame.clone(),
                        None => {
                            let frame = encode_event_frame_ref(msg, mode, self.max_frame)?;
                            nasa_frame = Some(frame.clone());
                            frame
                        }
                    };
                    report.record(session.send_business(frame));
                }
                Protocol::SocketIo => {
                    let arg = sio_arg.get_or_insert_with(|| self.sio_arg_ref(msg));
                    let text = build_sio_event_ref(msg, session.sio_namespace(), arg);
                    report.record(session.send_business(Bytes::from(text)));
                }
            }
        }
        Ok(report)
    }

    /// 只解析借用路由并返回本地唯一目标数，不编码 frame 或写 outbox。
    ///
    /// 消息队列适配器用它在 payload 物化前执行 fan-out 硬上限门禁。返回值是当前瞬时快照，
    /// 随后的实际投递仍可能因连接关闭而减少。
    ///
    /// # 参数
    ///
    /// - `msg`: 借用业务信封。
    pub fn local_target_count_borrowed(&self, msg: &MessageRef<'_>) -> usize {
        self.resolve_targets_ref(msg).len()
    }

    /// 发布跨节点发送请求；用于把非本地目标交给集群广播。
    ///
    /// # 参数
    /// - `msg`: 业务消息体或事件载荷。
    /// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
    fn publish_cluster(&self, msg: &Message, mode: Mode) {
        let hook = self.cluster_publish.lock().unwrap().clone();
        if let Some(f) = hook {
            f(msg, mode);
        }
    }

    /// 向匹配会话扇出消息；用于统计本地和集群发送结果。
    ///
    /// # 参数
    /// - `msg`: 业务消息体或事件载荷。
    /// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
    fn fan_out(&self, msg: &Message, mode: Mode) -> SendReport {
        let mut report = SendReport::default();
        let targets = self.resolve_targets(msg);
        if targets.is_empty() {
            return report;
        }
        // NASA 整帧:**懒编码**——只有存在 NASA 目标时才编一次(全 socket.io 目标时不浪费)。
        let mut nasa_frame: Option<Bytes> = None;
        // socket.io 目标:文本包按**目标会话的 namespace** 另编(不与 NASA 共享字节)。
        // bridge.outbound 只依赖 msg,可跨 namespace 复用(只 namespace 前缀不同)。
        let mut sio_arg: Option<String> = None;
        for s in targets {
            match s.protocol() {
                Protocol::Nasa => {
                    let frame = match &nasa_frame {
                        Some(f) => f.clone(),
                        None => {
                            let payload = match msg.encode(mode) {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::warn!("fan_out encode failed: {e}");
                                    return report;
                                }
                            };
                            let f = encode_frame(frame_type::EVENT, mode.ordinal(), &payload);
                            nasa_frame = Some(f.clone());
                            f
                        }
                    };
                    report.record(s.send_business(frame));
                }
                Protocol::SocketIo => {
                    let arg = sio_arg.get_or_insert_with(|| self.sio_arg(msg));
                    let text = build_sio_event(msg, s.sio_namespace(), arg);
                    report.record(s.send_business(Bytes::from(text)));
                }
            }
        }
        report
    }

    /// 用 PayloadBridge 把 msg.payload 翻成 socket.io EVENT 的 arg(JSON 文本)。
    ///
    /// # 参数
    /// - `msg`: 业务消息体或事件载荷。
    fn sio_arg(&self, msg: &Message) -> String {
        let event = sio_event_name(msg);
        let payload = msg.payload.as_deref().unwrap_or(&[]);
        self.bridge.outbound(event, payload)
    }

    /// 用借用消息直接生成文本协议参数，避免先复制成拥有型消息。
    ///
    /// # 参数
    ///
    /// - `msg`: 当前同步调用内有效的消息视图。
    fn sio_arg_ref(&self, msg: &MessageRef<'_>) -> String {
        let event = sio_event_name_ref(msg);
        let payload = msg.payload.unwrap_or(&[]);
        self.bridge.outbound(event, payload)
    }

    /// 路由解析:group 优先 → routers 过滤 → excludes(按 uid)→ fromClient(按 session id),按 SessionId 去重。
    ///
    /// # 参数
    /// - `msg`: 业务消息体或事件载荷。
    fn resolve_targets(&self, msg: &Message) -> Vec<Arc<Session>> {
        let reg = &self.registry;

        // 1) 选 id 集合(group > uids > routers),按 SessionId 去重。
        let mut ids: HashSet<String> = HashSet::new();
        let group = msg.group.as_deref().filter(|g| !g.is_empty());
        if let Some(g) = group {
            ids.extend(reg.ids_by_group(g));
        } else if let Some(uids) = present(&msg.uids) {
            for uid in uids {
                ids.extend(reg.ids_by_uid(uid));
            }
        } else if let Some(routers) = present(&msg.routers) {
            for r in routers {
                ids.extend(reg.ids_by_endpoint(r));
            }
        } else {
            return Vec::new();
        }

        // 2) 过滤器
        let router_set: Option<HashSet<&str>> =
            present(&msg.routers).map(|v| v.into_iter().collect());
        let exclude_set: Option<HashSet<&str>> =
            present(&msg.excludes).map(|v| v.into_iter().collect());
        let from_client = msg.from_client.as_deref();

        // endpoint 代际兜底:endpoint 已 replace/unregister 的旧代际会话
        // 不再接收服务端推送(主动关闭是第一道;此处兜底竞态窗口/漏关)。
        // 当前代际按 path 缓存,每条消息每个 path 最多查一次 registry。
        let gen_resolver = self.endpoint_gen.lock().unwrap().clone();
        let mut gen_cache: std::collections::HashMap<String, Option<u64>> =
            std::collections::HashMap::new();

        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(s) = reg.by_id_arc(&id) else {
                continue;
            };
            // 生命周期门禁:仅 **ready**(authenticated 且阶段就绪)
            // 会话可达 fan-out——鉴权 pending 与 **Connecting(on_connect 未完成)** 都跳过,
            // 本地 Sender 与 Redis 跨节点 send_local 共用此闸:业务初始化完成前不得收到
            // uid/router/group 定向消息。
            if !s.ready_for_fanout() {
                continue;
            }
            if let (Some(resolve), Some(bound_gen), Some(path)) =
                (&gen_resolver, s.bound_generation(), s.endpoint())
            {
                let current = gen_cache
                    .entry(path.to_string())
                    .or_insert_with(|| resolve(path));
                if *current != Some(bound_gen) {
                    continue; // 旧代际:不投递(主动关闭随后/已经到达)
                }
            }
            if let Some(rs) = &router_set {
                match s.endpoint() {
                    Some(ep) if rs.contains(ep) => {}
                    _ => continue,
                }
            }
            if let Some(es) = &exclude_set {
                if let Some(uid) = s.uid() {
                    if es.contains(uid) {
                        continue;
                    }
                }
            }
            if let Some(fc) = from_client {
                if s.id() == fc {
                    continue;
                }
            }
            out.push(s);
        }
        out
    }

    /// 按借用路由字段解析本地目标，规则与拥有型路径完全一致。
    ///
    /// # 参数
    ///
    /// - `msg`: 路由字段借用视图。
    fn resolve_targets_ref(&self, msg: &MessageRef<'_>) -> Vec<Arc<Session>> {
        match self.resolve_targets_ref_bounded(msg, usize::MAX) {
            Ok(targets) => targets,
            Err(()) => unreachable!("usize::MAX 目标上限不可被本进程内 Vec 超过"),
        }
    }

    /// 按借用路由解析本地目标，并在收集第 `max_targets + 1` 个目标前立即停止。
    ///
    /// # 参数
    ///
    /// - `msg`: 路由字段借用视图。
    /// - `max_targets`: 可返回的最大唯一 session 数，大于零。
    ///
    /// # 错误
    ///
    /// 实际匹配目标超过上限时返回单位错误，且不返回部分集合，调用方不得开始投递。
    fn resolve_targets_ref_bounded(
        &self,
        msg: &MessageRef<'_>,
        max_targets: usize,
    ) -> Result<Vec<Arc<Session>>, ()> {
        let reg = &self.registry;

        let mut ids: HashSet<String> = HashSet::new();
        let group = msg.group.filter(|group| !group.is_empty());
        if let Some(group) = group {
            ids.extend(reg.ids_by_group(group));
        } else if let Some(uids) = present_ref(msg.uids) {
            for uid in uids {
                ids.extend(reg.ids_by_uid(uid));
            }
        } else if let Some(routers) = present_ref(msg.routers) {
            for router in routers {
                ids.extend(reg.ids_by_endpoint(router));
            }
        } else {
            return Ok(Vec::new());
        }

        let router_set: Option<HashSet<&str>> =
            present_ref(msg.routers).map(|values| values.into_iter().collect());
        let exclude_set: Option<HashSet<&str>> =
            present_ref(msg.excludes).map(|values| values.into_iter().collect());
        let from_client = msg.from_client;
        let gen_resolver = self.endpoint_gen.lock().unwrap().clone();
        let mut gen_cache: std::collections::HashMap<String, Option<u64>> =
            std::collections::HashMap::new();

        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(session) = reg.by_id_arc(&id) else {
                continue;
            };
            if !session.ready_for_fanout() {
                continue;
            }
            if let (Some(resolve), Some(bound_gen), Some(path)) = (
                &gen_resolver,
                session.bound_generation(),
                session.endpoint(),
            ) {
                let current = gen_cache
                    .entry(path.to_string())
                    .or_insert_with(|| resolve(path));
                if *current != Some(bound_gen) {
                    continue;
                }
            }
            if let Some(routers) = &router_set {
                match session.endpoint() {
                    Some(endpoint) if routers.contains(endpoint) => {}
                    _ => continue,
                }
            }
            if let Some(excludes) = &exclude_set {
                if let Some(uid) = session.uid() {
                    if excludes.contains(uid) {
                        continue;
                    }
                }
            }
            if from_client.is_some_and(|client| session.id() == client) {
                continue;
            }
            if out.len() == max_targets {
                return Err(());
            }
            out.push(session);
        }
        Ok(out)
    }
}

/// socket.io 事件名 = events[0](首个非空);缺省 "message"。
pub(crate) fn sio_event_name(msg: &Message) -> &str {
    msg.events
        .as_ref()
        .and_then(|v| v.iter().flatten().find(|s| !s.is_empty()))
        .map(|s| s.as_str())
        .unwrap_or("message")
}

/// 借用消息的文本协议事件名；缺省为 `message`。
///
/// # 参数
///
/// - `msg`: 借用业务信封。
fn sio_event_name_ref<'a>(msg: &'a MessageRef<'a>) -> &'a str {
    msg.events
        .and_then(|events| events.iter().find(|event| !event.is_empty()).copied())
        .unwrap_or("message")
}

/// 拼 socket.io EVENT 文本帧:`42[/ns,]["<event>",<arg>]`。
/// 默认 namespace "/" 不加前缀;非默认形如 `42/ws/chat,["tick",<arg>]`
/// (客户端按 namespace 分发,namespaced 会话必须带前缀)。
pub(crate) fn build_sio_event(msg: &Message, namespace: &str, arg: &str) -> String {
    let event = sio_event_name(msg);
    if namespace == "/" {
        format!("42[{},{}]", json_quote(event), arg)
    } else {
        format!("42{},[{},{}]", namespace, json_quote(event), arg)
    }
}

/// 用借用信封拼接文本协议 EVENT 帧。
///
/// # 参数
///
/// - `msg`: 借用业务信封。
/// - `namespace`: 目标会话 namespace。
/// - `arg`: 已由桥接器转换的 JSON 参数。
pub fn build_sio_event_ref(msg: &MessageRef<'_>, namespace: &str, arg: &str) -> String {
    let event = sio_event_name_ref(msg);
    if namespace == "/" {
        format!("42[{},{}]", json_quote(event), arg)
    } else {
        format!("42{},[{},{}]", namespace, json_quote(event), arg)
    }
}

/// 取数组里"非 null 非空"的元素;整个字段缺失/空数组 → None。
///
/// # 参数
/// - `v`: 待转换的值。
fn present(v: &Option<Vec<Option<String>>>) -> Option<Vec<&str>> {
    let arr = v.as_ref()?;
    let collected: Vec<&str> = arr
        .iter()
        .filter_map(|e| e.as_deref())
        .filter(|s| !s.is_empty())
        .collect();
    if collected.is_empty() {
        None
    } else {
        Some(collected)
    }
}

/// 过滤借用字符串数组中的空元素。
///
/// # 参数
///
/// - `value`: 可选借用字符串数组。
fn present_ref<'a>(value: Option<&'a [&'a str]>) -> Option<Vec<&'a str>> {
    let collected: Vec<&str> = value?
        .iter()
        .copied()
        .filter(|item| !item.is_empty())
        .collect();
    if collected.is_empty() {
        None
    } else {
        Some(collected)
    }
}
