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
    /// 本次帧被认证、长度或队列容量等入队约束拒绝的目标数。
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
    /// 业务作用：把单个目标的出站入队结果累加到本次本地投递报告。
    ///
    /// 参数说明：
    /// - `outcome`: 目标会话返回的入队、拒绝或关闭结果。
    ///
    /// 返回：对应计数增加一次；不追踪已淘汰的旧帧、网络送达或跨节点发布结果。
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

/// 持有本地会话表、负载桥与可选集群回调，按用户、订阅或端点筛选投递目标。
pub struct Sender {
    /// 本节点会话权威表与反查索引。
    registry: Arc<SessionRegistry>,
    /// 本地投递后执行的可选集群发布入口，其结果不计入本地报告。
    cluster_publish: Mutex<Option<ClusterPublish>>,
    /// NASA 负载到 Socket.IO JSON 参数的转换接口。
    bridge: Arc<dyn PayloadBridge>,
    /// endpoint 代际解析器(未注入 = 不校验;手工裸装配时为 None)。
    endpoint_gen: Mutex<Option<EndpointGenResolver>>,
    /// 借用式编码路径的分配前单帧上限。
    max_frame: usize,
}

impl Sender {
    /// 业务作用：构造发送器。
    ///
    /// 参数说明：
    /// - `registry`: 本节点会话注册表,用于本地 fan-out 到 uid/group/endpoint/session。
    /// - `bridge`: payload 桥接器,用于 socket.io 会话的出站 payload 转换。
    ///
    /// 返回：使用默认十六 MiB 借用帧上限的共享发送器；尚未安装集群发布或端点代际解析回调。
    /// 不建立监听器、不注册会话，也不自动赋予业务调用方投递权限。
    pub fn new(registry: Arc<SessionRegistry>, bridge: Arc<dyn PayloadBridge>) -> Arc<Sender> {
        Self::new_with_max_frame(registry, bridge, DEFAULT_MAX_FRAME)
    }

    /// 业务作用：返回本 Sender 允许的单帧 body 上限。
    ///
    /// 供 kafka 装配层做启动期交叉校验：Kafka 记录经 TLV 信封编码后才成为 frame，
    /// 若 topic 允许的最大消息不小于本值，一条**合法**的大记录就会在编码期
    /// `FrameTooLarge` → `Halt`（不可重试、不进 DLT），等于外部可停掉数据面。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用 NASA 帧编码前检查的 body 长度限制，包含类型和模式，不含四字节外层长度头。
    /// 该值不是当前队列剩余容量，也不代替各会话的最终入队检查。
    #[cfg(feature = "kafka")]
    pub(crate) fn max_frame(&self) -> usize {
        self.max_frame
    }

    /// 业务作用：按服务端生效配置构造发送器。
    ///
    /// 参数说明：
    ///
    /// - `registry`: 本节点会话注册表。
    /// - `bridge`: 不同客户端协议之间的 payload 转换器。
    /// - `max_frame`: 单条 frame body 的最大字节数。
    ///
    /// 返回：绑定所给会话表和桥接器的共享发送器；零上限按一处理，集群发布与代际回调保持未设置。
    /// 不确认该限制与所有已存在会话一致，装配方须保证配置匹配。
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

    /// 业务作用：框架内部:集群启动后注入跨节点发布钩子。
    ///
    /// 参数说明：
    /// - `f`: 在本地扇出完成后接受同一消息与编码模式的同步发布回调。
    ///
    /// 返回：替换后续 send/relay 使用的集群回调；不补发旧消息，不取消已经取得旧回调的调用。
    pub(crate) fn set_cluster_publish(&self, f: ClusterPublish) {
        *self.cluster_publish.lock().unwrap() = Some(f);
    }

    /// 业务作用：框架内部:注入 endpoint 代际解析器(出站投递兜底)。
    ///
    /// 参数说明：
    /// - `f`: 按路径查询当前端点代际的回调，路径不在线时返回 None。
    ///
    /// 返回：保存后续目标筛选所用的代际查询器；不立即扫描或关闭现存会话，不替代端点变更清理回调。
    pub(crate) fn set_endpoint_gen_resolver(&self, f: EndpointGenResolver) {
        *self.endpoint_gen.lock().unwrap() = Some(f);
    }

    /// 业务作用：客户端入站消息触发的自动转发(本地 + 跨节点)。返回**本地** fan-out 报告。
    ///
    /// 参数说明：
    /// - `msg`: 客户端入站的业务消息信封。
    /// - `mode`: `msg` 的编码模式,跨节点转发时需要保持一致。
    ///
    /// 返回：本地实际尝试入队的计数；本地处理结束后调用集群发布回调，其结果不进入报告。
    /// 不自行执行 InboundPolicy 或业务鉴权，调用方必须先确认客户端是否允许转发及其目标权限。
    pub fn relay(&self, msg: &Message, mode: Mode) -> SendReport {
        let report = self.fan_out(msg, mode);
        self.publish_cluster(msg, mode);
        report
    }

    /// 业务作用：服务端主动推送(本地 + 跨节点;不经 InboundPolicy)。返回**本地** fan-out 报告。
    ///
    /// 参数说明：
    /// - `msg`: 服务端要投递的业务消息信封。
    /// - `mode`: `msg` 的编码模式,跨节点转发时需要保持一致。
    ///
    /// 返回：仅本地会话的入队报告；即使没有本地目标或本地编码提前结束，仍尝试调用集群回调。
    /// 不合并跨节点结果、不等待客户端确认；业务调用方应在进入本方法前完成发布与目标权限校验。
    pub fn send(&self, msg: &Message, mode: Mode) -> SendReport {
        let report = self.fan_out(msg, mode);
        self.publish_cluster(msg, mode);
        report
    }

    /// 业务作用：**仅本地** fan-out,不再跨节点 publish。集群收到对端事件后调,防 ping-pong 回环。
    ///
    /// 参数说明：
    /// - `msg`: 要在本节点本地投递的业务消息信封。
    /// - `mode`: `msg` 的编码模式,用于按目标会话协议编码出站帧。
    ///
    /// 返回：本地实际尝试的入队计数；不发布到集群。
    /// 无目标时全零，编码失败时保留已完成部分的报告，不能仅凭丢弃计数为零判断完整投递成功。
    pub fn send_local(&self, msg: &Message, mode: Mode) -> SendReport {
        self.fan_out(msg, mode)
    }

    /// 业务作用：把借用业务信封仅投递到本节点，二进制编码路径直接写入最终帧。
    ///
    /// 原生协议目标共享同一个最终 [`Bytes`]；文本协议目标按 namespace 转码，二者都不会
    /// 把借用数据带出本次同步调用。该边界适合消息队列消费回调直接转发底层 payload。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 生命周期仅覆盖当前调用的借用业务信封。
    /// - `mode`: 原生协议目标采用的编码模式。
    ///
    /// # 错误
    ///
    /// 字段非法、编码模式未实现或消息在分配前已超过单帧上限时返回错误；此时不会投递
    /// 任何原生协议 frame。
    ///
    /// 返回：成功时为本地入队报告，无目标时全零；NASA 编码失败返回错误，已入队的 Socket.IO 消息不回滚。
    /// JSON 编码仍会构造拥有型中间数据；本入口没有目标数量上限，也不执行集群发布或业务鉴权。
    pub fn send_local_borrowed(
        &self,
        msg: &MessageRef<'_>,
        mode: Mode,
    ) -> naws_proto::Result<SendReport> {
        let targets = self.resolve_targets_ref(msg);
        self.send_borrowed_to_targets(msg, mode, targets)
    }

    /// 业务作用：在单次有界目标解析后完成借用消息的本地投递。
    ///
    /// 该入口把 fan-out 上限检查放在最终 frame 编码和 outbox 写入之前，并避免先计数再重新解析。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 当前同步调用内有效的借用业务信封。
    /// - `mode`: 原生协议目标采用的编码模式。
    /// - `max_targets`: 本次允许的本地唯一 session 数；零表示只允许无目标结果。
    ///
    /// # 错误
    ///
    /// 字段非法、编码模式未实现或最终 frame 超过单帧上限时返回协议错误。
    ///
    /// 返回：没有目标为 `NoLocalTarget`，超过最终目标数量为 `TooManyTargets`，二者都不写出站队列；
    /// 其余返回入队报告或编码错误，已有其它协议入队不会回滚。
    /// 数量门禁限制最终目标，不限制候选 ID 索引复制与预分配总量；无目标时不验证消息编码。
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

    /// 业务作用：对已解析的会话集合复用 NASA 编码帧及 Socket.IO 参数，执行各会话的本地入队。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 路由和 payload 借用视图。
    /// - `mode`: 原生协议目标采用的编码模式。
    /// - `targets`: 已去重并筛选的目标集合；是否设置数量上限由调用入口决定。
    ///
    /// # 错误
    ///
    /// 最终 frame 编码失败时返回协议错误；已经进入 outbox 的其他协议目标不会回滚。
    ///
    /// 返回：所有目标完成入队决策后返回计数；首次 NASA 编码失败则立即报错，不退回先前入队结果。
    /// NASA 帧首次需要时才编码并共享缓冲，Socket.IO 参数只转换一次而文本按目标命名空间分别构造。
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

    /// 业务作用：只解析借用路由并返回本地唯一目标数，不编码 frame 或写 outbox。
    ///
    /// 该查询完整解析一次目标集合后计数，后续再次解析时可能因连接或订阅变化增减。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 借用业务信封。
    ///
    /// 返回：本次解析得到的去重且就绪目标数；不预留投递资格，不限制查找分配，也不构成后续发送的硬上限保证。
    pub fn local_target_count_borrowed(&self, msg: &MessageRef<'_>) -> usize {
        self.resolve_targets_ref(msg).len()
    }

    /// 业务作用：把完整消息交给已安装的集群发布回调，由集群决定跨节点目标。
    ///
    /// 参数说明：
    /// - `msg`: 本次投递的完整业务信封，不删除已经本地投递的目标字段。
    /// - `mode`: 跨节点转发需保留的消息编码模式。
    ///
    /// 返回：已安装回调时释放回调锁后同步调用，否则不做操作；不返回发布结果、不隔离回调 panic。
    fn publish_cluster(&self, msg: &Message, mode: Mode) {
        let hook = self.cluster_publish.lock().unwrap().clone();
        if let Some(f) = hook {
            f(msg, mode);
        }
    }

    /// 业务作用：向匹配的本地会话扇出拥有型消息，按连接协议复用可共享的编码内容。
    ///
    /// 参数说明：
    /// - `msg`: 同时包含目标筛选字段、事件和 payload 的消息信封。
    /// - `mode`: NASA 目标使用的信封编码模式，Socket.IO 目标经负载桥转换。
    ///
    /// 返回：已执行入队尝试的本地计数；无目标时全零，NASA 编码失败时记录警告并提前返回部分报告。
    /// 没有独立编码错误返回值，未尝试的目标不会计入 dropped；实际网络送达和集群结果不在此统计。
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

    /// 业务作用：用 PayloadBridge 把 msg.payload 翻成 socket.io EVENT 的 arg(JSON 文本)。
    ///
    /// 参数说明：
    /// - `msg`: 提供事件名候选和业务 payload 的拥有型信封。
    ///
    /// 返回：负载桥产生的参数文本；缺失 payload 作为空字节，事件名缺省为 `message`。
    /// 不验证返回文本是否为合法 JSON，不携带 namespace 或确认 ID。
    fn sio_arg(&self, msg: &Message) -> String {
        let event = sio_event_name(msg);
        let payload = msg.payload.as_deref().unwrap_or(&[]);
        self.bridge.outbound(event, payload)
    }

    /// 业务作用：用借用消息直接生成文本协议参数，避免先复制成拥有型消息。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 当前同步调用内有效的消息视图。
    ///
    /// 返回：负载桥以选定事件名和借用 payload 生成的 JSON 参数文本；缺失负载使用空字节。
    /// 只转换参数，不构造拥有型信封，也不校验桥接器输出的 JSON 语法。
    fn sio_arg_ref(&self, msg: &MessageRef<'_>) -> String {
        let event = sio_event_name_ref(msg);
        let payload = msg.payload.unwrap_or(&[]);
        self.bridge.outbound(event, payload)
    }

    /// 业务作用：路由解析:group 优先 → routers 过滤 → excludes(按 uid)→ fromClient(按 session id),按 SessionId 去重。
    ///
    /// 参数说明：
    /// - `msg`: 提供订阅、用户、端点、排除用户和来源会话条件的消息。
    ///
    /// 返回：按订阅优先、其次用户、最后端点选择，并经过就绪与排除条件筛选的本地会话集合；无目标为空。
    /// 顺序不稳定，只有解析器与绑定信息齐备时才复验端点代际；这不是原子授权快照或投递权预留。
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

    /// 业务作用：按借用路由字段解析本地目标，规则与拥有型路径完全一致。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 路由字段借用视图。
    ///
    /// 返回：使用 usize 最大值作为数量上限得到的完整本地候选集合，可能为空；不编码或入队。
    /// 字符串内容借用原信封，但 ID 集合和返回向量仍会分配内存，不提供业务级扇出容量约束。
    fn resolve_targets_ref(&self, msg: &MessageRef<'_>) -> Vec<Arc<Session>> {
        match self.resolve_targets_ref_bounded(msg, usize::MAX) {
            Ok(targets) => targets,
            Err(()) => unreachable!("usize::MAX 目标上限不可被本进程内 Vec 超过"),
        }
    }

    /// 业务作用：按借用路由解析本地目标，并在收集第 `max_targets + 1` 个目标前立即停止。
    ///
    /// 参数说明：
    ///
    /// - `msg`: 路由字段借用视图。
    /// - `max_targets`: 可返回的最大唯一会话数；零只允许没有匹配目标。
    ///
    /// # 错误
    ///
    /// 实际匹配目标超过上限时返回单位错误，且不返回部分集合，调用方不得开始投递。
    ///
    /// 返回：通过当前就绪、代际和排除条件的候选集合，超过数量上限时返回 `Err(())`。
    /// 在检查最终匹配数量前已收集候选 ID 并预分配向量，因此上限不约束所有查找内存。
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
            // 初始化未完成或清理已开始的会话不能成为投递候选。
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
            // 整条消息拒绝超限集合，不返回可被误用来部分投递的截断列表。
            if out.len() == max_targets {
                return Err(());
            }
            out.push(session);
        }
        Ok(out)
    }
}

/// 业务作用：为 Socket.IO 单个事件包选择信封中的首个非空事件名。
///
/// 参数说明：
/// - `msg`: 可包含多个可空事件名的拥有型消息信封。
///
/// 返回：第一个非 None 且非空字符串的借用；没有可用名称时返回 `message`，不合并其它事件名。
pub(crate) fn sio_event_name(msg: &Message) -> &str {
    msg.events
        .as_ref()
        .and_then(|v| v.iter().flatten().find(|s| !s.is_empty()))
        .map(|s| s.as_str())
        .unwrap_or("message")
}

/// 业务作用：借用消息的文本协议事件名；缺省为 `message`。
///
/// 参数说明：
///
/// - `msg`: 借用业务信封。
///
/// 返回：借用事件列表中的首个非空名称，没有可用名称时返回 `message`；不分配字符串，也不处理后续事件名。
fn sio_event_name_ref<'a>(msg: &'a MessageRef<'a>) -> &'a str {
    msg.events
        .and_then(|events| events.iter().find(|event| !event.is_empty()).copied())
        .unwrap_or("message")
}

/// 业务作用：拼 socket.io EVENT 文本帧:`42[/ns,]["<event>",<arg>]`。
/// 默认 namespace "/" 不加前缀;非默认形如 `42/ws/sample,["event",<arg>]`
/// (客户端按 namespace 分发,namespaced 会话必须带前缀)。
///
/// 参数说明：
/// - `msg`: 用于选择首个非空事件名的拥有型信封。
/// - `namespace`: 目标会话已绑定的命名空间。
/// - `arg`: 负载桥提供的完整 JSON 参数文本，调用方保证语法有效。
///
/// 返回：带 Engine.IO MESSAGE 前缀的 Socket.IO EVENT 文本；事件名经过 JSON 转义，参数和命名空间直接拼接。
/// 不验证 namespace 或参数，不附带消息的确认 ID，不发送到连接。
pub(crate) fn build_sio_event(msg: &Message, namespace: &str, arg: &str) -> String {
    let event = sio_event_name(msg);
    if namespace == "/" {
        format!("42[{},{}]", json_quote(event), arg)
    } else {
        format!("42{},[{},{}]", namespace, json_quote(event), arg)
    }
}

/// 业务作用：用借用信封拼接文本协议 EVENT 帧。
///
/// 参数说明：
///
/// - `msg`: 借用业务信封。
/// - `namespace`: 目标会话 namespace。
/// - `arg`: 已由桥接器转换的 JSON 参数。
///
/// 返回：按目标命名空间构造的完整文本事件；只取首个非空事件名，根命名空间省略前缀。
/// 事件名转义，参数直接嵌入；不校验参数或命名空间语法，也不写入确认 ID。
pub fn build_sio_event_ref(msg: &MessageRef<'_>, namespace: &str, arg: &str) -> String {
    let event = sio_event_name_ref(msg);
    if namespace == "/" {
        format!("42[{},{}]", json_quote(event), arg)
    } else {
        format!("42{},[{},{}]", namespace, json_quote(event), arg)
    }
}

/// 业务作用：取数组里"非 null 非空"的元素;整个字段缺失/空数组 → None。
///
/// 参数说明：
/// - `v`: 可缺失、且元素可为 None 的路由字符串列表。
///
/// 返回：保留原顺序和重复项的非空字符串借用列表；缺失或过滤后为空时返回 `None`。
/// 向量会分配，但不复制字符串；不裁剪空白或判断字段作为目标时的业务含义。
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

/// 业务作用：过滤借用字符串数组中的空元素。
///
/// 参数说明：
///
/// - `value`: 可选借用字符串数组。
///
/// 返回：去除空字符串后的借用向量，保留顺序和重复项；缺失或过滤后为空时返回 `None`。
/// 不复制字符串内容，但仍分配向量；空白文本保持原样。
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
