// ============================================================================
// src/session.rs —— 连接状态对象 + 出站队列(背压接口)+ 会话反查表。
// 架构说明(Session 内部可变性)、(背压接口)、(Registry/SessionId 索引)。
// ============================================================================

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bytes::Bytes;
use dashmap::DashMap;
use naws_proto::{Message, Mode, WireCodec};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::bridge::PayloadBridge;
use crate::endpoint::Endpoint;
use crate::wire::{encode_frame, frame_type, wire_mode};

/// 生命周期阶段:保证 `on_connect` / `on_disconnect` 对同一会话**严格串行**
/// 且 on_disconnect exactly once。所有转换都在 `lifecycle` 锁内完成(回调本身在锁外执行)。
///
/// ```text
/// Pending → Connecting → Active → Closed
///               └→ ClosingDuringConnect → Closed
/// ```
pub(crate) mod phase {
    /// 初始(未开始 on_connect;手工裸装配会停留在此)。
    pub const PENDING: u8 = 0;
    /// on_connect 正在执行(尚未返回)。
    pub const CONNECTING: u8 = 1;
    /// on_connect 已完成,会话正常活跃。
    pub const ACTIVE: u8 = 2;
    /// on_connect 执行期间被外部下线:逻辑注销已完成,on_disconnect **延迟**到
    /// on_connect 返回后由 `finish_connect` 触发(绝不与 on_connect 并发)。
    pub const CLOSING_DURING_CONNECT: u8 = 3;
    /// 终态。
    pub const CLOSED: u8 = 4;
}

/// 会话在**鉴权时绑定**的 endpoint 快照:on_connect/on_disconnect 与
/// EVENT 分派始终用它,绝不在断连时按 path 重新解析当前 endpoint——保证 hook 成对、
/// 旧会话不执行新 endpoint 代码。
pub(crate) struct BoundEndpoint {
    /// 绑定时的 registry 代际;EVENT 分派/出站投递据此做"已被 replace/unregister"兜底。
    pub generation: u64,
    pub endpoint: Arc<Endpoint>,
}

/// 会话接入传输类型,用于区分原生 TCP 连接和 WebSocket 连接。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// 原生 TCP 长连接。
    Tcp,
    /// WebSocket 长连接。
    Ws,
}

/// 连接所用的 wire 协议:NASA 原生二进制帧 / socket.io 文本包。
/// fan-out 时按它决定给该会话发二进制帧还是 socket.io 文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// NASA 二进制 frame 协议。
    Nasa,
    /// socket.io / engine.io 兼容协议。
    SocketIo,
}

/// 出站背压策略。队列满时如何取舍**业务帧**;控制帧不受此限,保证不丢。
/// 不提供 Block 策略:fan-out 是单生产者顺序遍历 N 个 outbox,阻塞会让一个慢接收端
/// 拖垮所有接收端(队头阻塞),不适合热路径。需要合流(conflation)由业务层按 key 做。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// 队列满:丢弃新到的业务帧(默认,保留较旧帧的连续性)。
    #[default]
    DropNew,
    /// 队列满:淘汰队首最旧未发帧,给新帧腾位(行情快照场景:最新优先)。
    DropOldest,
}

/// 出站发送结果(`send` 不能忽略返回)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// 业务帧已进入该会话出站队列。
    Queued,
    /// 队列满,按策略丢弃了某一帧(新帧或被淘汰的旧帧)。
    Dropped,
    /// 连接已关闭(所有发送端已 drop)。
    Closed,
}

/// 控制帧队列的(条数, 字节)上限。控制帧极少且小;溢出意味着对端已不读(连接实为死),
/// 此时淘汰队首旧控制帧(保留较新的 CLOSE 等),并计数。避免无界堆积形成 DoS。
const CONTROL_CAP: usize = 1024;
const CONTROL_BYTE_CAP: usize = 8 * 1024 * 1024;

/// 出站双队列:control 优先 + business 有界(条数 + 字节双限,DropNew/DropOldest)。
struct Queues {
    control: VecDeque<Bytes>,
    control_bytes: usize,
    business: VecDeque<Bytes>,
    business_bytes: usize,
}

/// 保存内部共享状态；用于在多个调用路径之间复用数据。
struct OutboxShared {
    q: Mutex<Queues>,
    /// business 条数上限。
    cap: usize,
    /// business 字节上限(防小帧多条/大帧少条两类积压)。
    byte_cap: usize,
    policy: BackpressurePolicy,
    notify: Notify,
    closed: AtomicBool,
    senders: AtomicUsize,
    dropped: AtomicU64,
}

/// 每连接出站队列的**发送端**(可 clone 给 Registry/Sender)。
/// 自管双队列(非 mpsc):控制帧优先且不被业务淘汰;业务帧条数+字节双限 + DropNew/DropOldest;
/// 暴露丢弃指标。
pub struct Outbox {
    shared: Arc<OutboxShared>,
}

impl Clone for Outbox {
    /// 业务作用：复制当前句柄；用于共享同一底层资源或配置快照。
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Outbox {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Outbox {
    /// 业务作用：释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        // 最后一个发送端 drop → 标记关闭并唤醒 writer 让它收尾(对齐 mpsc 语义)。
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.closed.store(true, Ordering::Release);
            self.shared.notify.notify_one();
        }
    }
}

impl Outbox {
    /// 业务作用：`cap` = business 条数上限;`byte_cap` = business 字节上限;`policy` = 满时取舍。
    ///
    /// # 参数
    /// - `cap`: 业务出站队列允许缓存的帧数量上限。
    /// - `byte_cap`: 业务出站队列允许缓存的总字节上限。
    /// - `policy`: 业务队列满时的丢弃策略。
    pub fn new(
        cap: usize,
        byte_cap: usize,
        policy: BackpressurePolicy,
    ) -> (Outbox, OutboxReceiver) {
        let shared = Arc::new(OutboxShared {
            q: Mutex::new(Queues {
                control: VecDeque::new(),
                control_bytes: 0,
                business: VecDeque::with_capacity(cap.min(1024)),
                business_bytes: 0,
            }),
            cap: cap.max(1),
            byte_cap: byte_cap.max(1),
            policy,
            notify: Notify::new(),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            dropped: AtomicU64::new(0),
        });
        (
            Outbox {
                shared: shared.clone(),
            },
            OutboxReceiver { shared },
        )
    }

    /// 业务作用：业务帧:超 条数/字节 上限则按策略取舍(DropNew 丢新 / DropOldest 淘汰队首业务帧)。
    /// **不会**淘汰控制帧(两队列分离)。
    ///
    /// # 参数
    /// - `frame`: 已编码的业务出站帧。
    pub fn send_business(&self, frame: Bytes) -> SendOutcome {
        if self.shared.closed.load(Ordering::Acquire) {
            return SendOutcome::Closed;
        }
        let len = frame.len();
        {
            let mut q = self.shared.q.lock().unwrap();
            let over = |q: &Queues| {
                q.business.len() >= self.shared.cap || q.business_bytes + len > self.shared.byte_cap
            };
            if over(&q) {
                match self.shared.policy {
                    BackpressurePolicy::DropNew => {
                        self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                        return SendOutcome::Dropped;
                    }
                    BackpressurePolicy::DropOldest => {
                        // 淘汰队首业务帧直到放得下;若单帧本身超字节上限则放弃新帧。
                        while over(&q) {
                            match q.business.pop_front() {
                                Some(old) => {
                                    q.business_bytes -= old.len();
                                    self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                                }
                                None => {
                                    self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                                    return SendOutcome::Dropped; // 单帧 > byte_cap,放不下
                                }
                            }
                        }
                    }
                }
            }
            q.business_bytes += len;
            q.business.push_back(frame);
        }
        self.shared.notify.notify_one();
        SendOutcome::Queued
    }

    /// 业务作用：控制帧(AUTH_RESP/PONG/CLOSE):进**优先**队列、writer 先发;有独立(大)上限保证正常
    /// 情况下不丢,溢出(对端不读)时淘汰队首旧控制帧防 DoS。
    ///
    /// # 参数
    /// - `frame`: 已编码的控制出站帧。
    pub fn send_control(&self, frame: Bytes) -> SendOutcome {
        if self.shared.closed.load(Ordering::Acquire) {
            return SendOutcome::Closed;
        }
        let len = frame.len();
        {
            let mut q = self.shared.q.lock().unwrap();
            while q.control.len() >= CONTROL_CAP || q.control_bytes + len > CONTROL_BYTE_CAP {
                match q.control.pop_front() {
                    Some(old) => {
                        q.control_bytes -= old.len();
                        self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    None => return SendOutcome::Dropped, // 单帧 > 控制上限
                }
            }
            q.control_bytes += len;
            q.control.push_back(frame);
        }
        self.shared.notify.notify_one();
        SendOutcome::Queued
    }

    /// 业务作用：累计因背压丢弃的帧数(业务被丢 + 控制溢出淘汰;可观测)。
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// 业务作用：主动关闭:置 closed + 唤醒 writer。writer 把已入队帧(含 CLOSE)排空后 recv 返回 None 退出
    /// ——不必等所有发送端 Arc 自然 drop。
    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notify.notify_one();
    }
}

/// 每连接出站队列的**接收端**(writer task 独占)。
pub struct OutboxReceiver {
    shared: Arc<OutboxShared>,
}

impl OutboxReceiver {
    /// 业务作用：取下一帧:**控制帧优先**,其次业务帧;两者皆空则挂起。
    /// 所有发送端 drop 且队列排空后返回 None。
    pub async fn recv(&mut self) -> Option<Bytes> {
        loop {
            // 先登记 notified() 再查队列:防「查空 → 漏掉 notify → 永久挂起」的丢唤醒。
            let notified = self.shared.notify.notified();
            {
                let mut q = self.shared.q.lock().unwrap();
                if let Some(f) = q.control.pop_front() {
                    q.control_bytes -= f.len();
                    return Some(f);
                }
                if let Some(f) = q.business.pop_front() {
                    q.business_bytes -= f.len();
                    return Some(f);
                }
                if self.shared.closed.load(Ordering::Acquire) {
                    return None;
                }
            }
            notified.await;
        }
    }
}

/// 连接会话(纯状态)。全程以 `Arc<Session>` 流转。
pub struct Session {
    id: String,
    transport: Transport,
    protocol: Protocol,
    uid: OnceLock<String>,
    endpoint: OnceLock<String>,
    /// 鉴权时绑定的 endpoint 快照(generation + Arc)。
    bound: OnceLock<BoundEndpoint>,
    /// socket.io 会话连接的 namespace(出站事件按此成帧);NASA 会话不设,默认 "/"。
    sio_namespace: OnceLock<String>,
    authenticated: AtomicBool,
    /// 连接已进入清理(cleanup 先置位再清索引):置位后拒绝 join_group/send 等,
    /// 防晚结束的 async handler 重建幽灵 group / 写失效索引。
    closed: AtomicBool,
    /// **连接级关闭通知**:首位 cleanup 调用者 cancel 它,读循环
    /// (NASA dispatch_loop / socket.io 主循环)select 之即退——endpoint 下线等外部
    /// cleanup 不再只是逻辑注销,reader task 与 socket 同步关闭,不依赖心跳超时或
    /// 客户端配合(恶意客户端狂发 PING 也无法保活)。
    closed_token: CancellationToken,
    /// 生命周期阶段
    /// 保证 on_connect/on_disconnect 严格串行、on_disconnect exactly once。
    phase: AtomicU8,
    /// 生命周期锁:join_group/leave_group 与 cleanup(置 closed + 注销)互斥,消除
    /// "join 检查通过 → 被切走 → cleanup 注销 → join 续写 by_group" 的幽灵索引竞态。
    lifecycle: Mutex<()>,
    groups: Mutex<HashSet<String>>,
    outbox: Outbox,
    /// 指回 Registry(Weak 避免 Arc 环);用于 join/leave 同步 by_group 索引。
    registry: Weak<SessionRegistry>,
    /// payload 桥接:`send_event` 据**会话协议**编码出站(NASA 帧 / socket.io 文本)。
    bridge: Arc<dyn PayloadBridge>,
    /// 单帧出站上限(= ServerConfig.max_frame):所有业务发送入口在入队前统一拦截,
    /// 否则超过接收端 max_frame 的大帧会让整批客户端协议断开。
    max_frame: usize,
    /// 因超过 max_frame 被拦截的帧数(**业务帧 + 控制帧**都计;可观测,warn 按 2 的幂限频)。
    oversize_dropped: AtomicU64,
    /// 本连接在飞 async handler 计数(per-conn 配额):与全局 `handler_sem` 叠加,
    /// 全局限总量、这里限单连接占比,防单连接狂发事件抢占全局池、饿死其它连接。
    inflight_handlers: AtomicUsize,
}

impl Session {
    /// 业务作用：**框架内部构造**(`pub(crate)`):业务不得自行造 Session 并注册,
    /// 否则可绕过鉴权/生命周期不变量。
    pub(crate) fn new(
        id: String,
        transport: Transport,
        protocol: Protocol,
        outbox: Outbox,
        registry: &Arc<SessionRegistry>,
        bridge: Arc<dyn PayloadBridge>,
        max_frame: usize,
    ) -> Arc<Session> {
        Arc::new(Session {
            id,
            transport,
            protocol,
            uid: OnceLock::new(),
            endpoint: OnceLock::new(),
            bound: OnceLock::new(),
            sio_namespace: OnceLock::new(),
            authenticated: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            closed_token: CancellationToken::new(),
            phase: AtomicU8::new(phase::PENDING),
            lifecycle: Mutex::new(()),
            groups: Mutex::new(HashSet::new()),
            outbox,
            registry: Arc::downgrade(registry),
            bridge,
            max_frame: max_frame.max(1),
            oversize_dropped: AtomicU64::new(0),
            inflight_handlers: AtomicUsize::new(0),
        })
    }

    /// 业务作用：尝试为一个 async handler 预留【本连接】的在飞名额:当前在飞 < `cap` 时 +1 并返回 RAII 释放守卫;
    /// 已达 `cap` 返回 `None`(调用方据此拒绝该事件)。`cap == 0` 视为不限制(返回守卫但不计数)。
    /// 与全局 `handler_sem` 叠加:全局限总量、本方法限单连接占比,防单连接抢占全局池饿死其它连接。
    pub(crate) fn try_reserve_handler(self: &Arc<Self>, cap: usize) -> Option<HandlerReservation> {
        if cap == 0 {
            return Some(HandlerReservation {
                session: self.clone(),
                counted: false,
            });
        }
        // CAS 循环:仅在 < cap 时占名额(无锁,读循环单线程调用但 handler 任务并发释放)。
        let mut cur = self.inflight_handlers.load(Ordering::Acquire);
        loop {
            if cur >= cap {
                return None;
            }
            match self.inflight_handlers.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(HandlerReservation {
                        session: self.clone(),
                        counted: true,
                    })
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// 业务作用：按**会话协议**编码并发一条业务 EVENT 给本会话(供 `SessionHandle::send_event` 复用,
    /// 与 fan_out 的单会话编码保持一致：
    /// - NASA → **JSON_BYTES** EVENT 帧(浏览器 SDK 只收 JSON;Rust Client 也接受);
    /// - socket.io → 经 `PayloadBridge` + namespace 生成标准 `42[...]` 文本。
    pub(crate) fn encode_and_send_event(&self, event: &str, payload: &[u8]) -> SendOutcome {
        // 早退顺序按语义:先 closed(→Closed)/ 未认证(→Dropped),
        // 再谈 oversize——已关闭会话发大 payload 应得 Closed 而非 Dropped。
        if self.is_closed() {
            return SendOutcome::Closed;
        }
        if !self.authenticated() {
            return SendOutcome::Dropped;
        }
        // **编码前**预检**仅限 NASA**:NASA 路径 payload 原样进 JSON 信封,
        // 编码后只会更大,预检无误杀、免"先大分配再丢弃";socket.io **必须先过 bridge**
        // (bridge 可压缩/摘要/替换 payload,按原始大小预判会误杀),最终文本由
        // send_business 精确检查。
        if matches!(self.protocol, Protocol::Nasa) && payload.len() > self.max_frame {
            self.note_oversize(payload.len());
            return SendOutcome::Dropped;
        }
        let msg = Message {
            events: Some(vec![Some(event.to_string())]),
            payload: Some(payload.to_vec()),
            ..Default::default()
        };
        let frame: Bytes = match self.protocol {
            Protocol::Nasa => {
                // 编码失败 → 丢帧 + warn(对齐 fan_out),**不发**空 payload 的"毒帧"——
                // 空字节不是合法 JSON,客户端会当协议错误断线。
                let body = match msg.encode(Mode::JsonBytes) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("send_event encode failed on {}: {e}", self.id);
                        return SendOutcome::Dropped;
                    }
                };
                encode_frame(frame_type::EVENT, wire_mode::JSON, &body)
            }
            Protocol::SocketIo => {
                let arg = self.bridge.outbound(event, payload);
                Bytes::from(crate::sender::build_sio_event(
                    &msg,
                    self.sio_namespace(),
                    &arg,
                ))
            }
        };
        self.send_business(frame)
    }

    /// 业务作用：返回会话 ID；用于调用方定位单个连接。
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 业务作用：读取 transport 状态；用于向调用方暴露当前运行信息。
    pub fn transport(&self) -> Transport {
        self.transport
    }

    /// 业务作用：返回会话协议类型；用于区分连接的编码和行为。
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// 业务作用：读取 uid 状态；用于向调用方暴露当前运行信息。
    pub fn uid(&self) -> Option<&str> {
        self.uid.get().map(String::as_str)
    }

    /// 业务作用：读取 endpoint 状态；用于向调用方暴露当前运行信息。
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.get().map(String::as_str)
    }

    /// 业务作用：返回会话认证状态；用于在处理消息前判断是否已通过鉴权。
    pub fn authenticated(&self) -> bool {
        self.authenticated.load(Ordering::Acquire)
    }

    /// 业务作用：返回会话已加入的群组；用于外部观测和权限判断。
    pub fn groups(&self) -> Vec<String> {
        self.groups.lock().unwrap().iter().cloned().collect()
    }

    /// 业务作用：本会话当前群数(零分配,供上限检查)。
    pub fn group_count(&self) -> usize {
        self.groups.lock().unwrap().len()
    }

    /// 业务作用：框架内部:鉴权时设一次 uid。**`pub(crate)`**:业务(尤其鉴权回调)不得改 uid——否则
    /// 回调先 set_uid、框架随后 set 因 OnceLock 已占用而静默失败,注册 uid 与鉴权返回不一致
    ///。
    pub(crate) fn set_uid(&self, uid: String) {
        let _ = self.uid.set(uid);
    }

    /// 业务作用：框架内部:握手/鉴权时设一次 endpoint。
    pub(crate) fn set_endpoint(&self, ep: String) {
        let _ = self.endpoint.set(ep);
    }

    /// 业务作用：框架内部:鉴权时绑定 endpoint 快照(generation + Arc)。
    pub(crate) fn bind_endpoint(&self, generation: u64, endpoint: Arc<Endpoint>) {
        let _ = self.bound.set(BoundEndpoint {
            generation,
            endpoint,
        });
    }

    /// 业务作用：鉴权时绑定的 endpoint 快照(hook/分派恒用它,不按 path 重新解析)。
    pub(crate) fn bound_endpoint(&self) -> Option<&BoundEndpoint> {
        self.bound.get()
    }

    /// 业务作用：绑定代际(出站投递兜底校验用)。
    pub(crate) fn bound_generation(&self) -> Option<u64> {
        self.bound.get().map(|b| b.generation)
    }

    /// 业务作用：endpoint 下线/替换时向本会话发协议化的"endpoint removed"关闭通知
    /// (NASA → CLOSE(ENDPOINT_REMOVED) 帧;socket.io → `41` DISCONNECT)。
    /// 须在 cleanup **之前**调用(cleanup 置 closed 后控制帧不再入队)。
    pub(crate) fn send_endpoint_removed_close(&self) {
        match self.protocol {
            Protocol::Nasa => {
                let cr = naws_proto::CloseReason {
                    code: naws_proto::CloseReason::CODE_ENDPOINT_REMOVED,
                    reason: Some("endpoint removed or replaced".into()),
                };
                if let Ok(body) = cr.encode(Mode::VarintTlv) {
                    let _ = self.send_control(encode_frame(
                        frame_type::CLOSE,
                        wire_mode::VARINT,
                        &body,
                    ));
                }
            }
            #[cfg(feature = "socketio")]
            Protocol::SocketIo => {
                let _ = self.send_control(Bytes::from(crate::socketio::sio_disconnect()));
            }
            #[cfg(not(feature = "socketio"))]
            Protocol::SocketIo => {} // 未编译 socketio 时不存在此类会话
        }
    }

    /// 业务作用：socket.io 会话连接的 namespace(默认 "/")。出站 sio 事件按此成帧。
    pub fn sio_namespace(&self) -> &str {
        self.sio_namespace.get().map(String::as_str).unwrap_or("/")
    }

    /// 业务作用：框架内部:socket.io CONNECT 时设一次 namespace。仅 socketio 路径调用。
    #[cfg_attr(not(feature = "socketio"), allow(dead_code))]
    pub(crate) fn set_sio_namespace(&self, ns: String) {
        let _ = self.sio_namespace.set(ns);
    }

    /// 业务作用：框架内部:鉴权结果落地。业务不得自行改认证态。
    pub(crate) fn set_authenticated(&self, v: bool) {
        self.authenticated.store(v, Ordering::Release);
    }

    /// 业务作用：是否已进入清理(closed)。closed 后 join_group/send 等被拒。
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// 业务作用：连接级关闭令牌克隆(level-triggered):读循环 select 之,外部 cleanup 即退
    ///。
    pub(crate) fn closed_token(&self) -> CancellationToken {
        self.closed_token.clone()
    }

    /// 业务作用：进入 `Connecting`(即将执行 on_connect)。须在 `set_authenticated`
    /// **之前**调——保证"authenticated=true 而 phase 仍 Pending"的窗口不存在,cleanup 的
    /// 阶段裁决因此完备。返回 false = 会话已被并发下线,调用方跳过 on_connect(也无配对
    /// disconnect 需要触发)。
    pub(crate) fn begin_connect(&self) -> bool {
        let _life = self.lifecycle.lock().unwrap();
        if self.is_closed() {
            return false;
        }
        self.phase.store(phase::CONNECTING, Ordering::Release);
        true
    }

    /// 业务作用：on_connect 返回后调
    /// - `Connecting → Active`:正常路径,返回 false;
    /// - `ClosingDuringConnect → Closed`:on_connect 期间被外部下线(逻辑注销已完成、
    ///   on_disconnect 被延迟)——返回 **true**,调用方**此刻**触发 exactly-once 的
    ///   on_disconnect(绝不与 on_connect 并发,也不会有第二次)。
    #[must_use = "返回 true 时调用方必须补触发 on_disconnect"]
    pub(crate) fn finish_connect(&self) -> bool {
        let _life = self.lifecycle.lock().unwrap();
        match self.phase.load(Ordering::Acquire) {
            phase::CONNECTING => {
                self.phase.store(phase::ACTIVE, Ordering::Release);
                false
            }
            phase::CLOSING_DURING_CONNECT => {
                self.phase.store(phase::CLOSED, Ordering::Release);
                true
            }
            _ => false,
        }
    }

    /// 业务作用：会话是否可作为 **fan-out 目标**:authenticated + 未关闭 + 阶段就绪。
    /// `Connecting`(on_connect 尚未完成)**不可达**——业务初始化(入群/订阅/上下文)未
    /// 完成前,本地 Sender 与 Redis 跨节点 `send_local` 都不得投递业务消息;
    /// on_connect **自己**经 `SessionHandle::send_event` 发欢迎帧不受此限(不走 resolve_targets,
    /// 且 AUTH_RESP/connect_ok 已先入 control 队,顺序不破)。
    /// `Pending` + authenticated 仅手工裸装配可达(真实路径 begin_connect 先于
    /// set_authenticated),按就绪兼容(与 cleanup 的 Pending 分支同源)。
    pub(crate) fn ready_for_fanout(&self) -> bool {
        if !self.authenticated() || self.is_closed() {
            return false;
        }
        matches!(
            self.phase.load(Ordering::Acquire),
            phase::ACTIVE | phase::PENDING
        )
    }

    /// 业务作用：框架内部:cleanup 主动关闭出站队列(writer 排空已入队帧后退出)。
    pub(crate) fn close_outbox(&self) {
        self.outbox.close();
    }

    /// 业务作用：发**控制**帧(AUTH_RESP/PONG/CLOSE/握手等):进优先队列,writer 先发。
    /// closed 后一律 `Closed`,防晚到的 handler 往已关连接排队。
    /// **`pub(crate)`**:控制帧只由框架发——否则鉴权回调/业务 handler 拿到 `Arc<Session>` 后
    /// 可伪造 `AUTH_RESP` 等控制帧。业务发消息用 `Sender` / `send_business`。
    pub(crate) fn send_control(&self, frame: Bytes) -> SendOutcome {
        if self.is_closed() {
            return SendOutcome::Closed;
        }
        // 控制帧也执行协议对应上限(原先完全绕过 → 小 max_frame 配置下服务端
        // 发出客户端必拒的 AUTH_RESP/CLOSE)。配合 build 期 `MIN_MAX_FRAME` 校验 + 动态
        // reason 截断,正常路径不会触发;触发即响亮丢弃,不发"客户端必拒"的帧。
        if self.frame_over_limit(frame.len()) {
            self.note_oversize(frame.len());
            return SendOutcome::Dropped;
        }
        self.outbox.send_control(frame)
    }

    /// 业务作用：发**业务**帧(整帧)。**`pub(crate)`**:业务只能经 `SessionHandle::send_event`(框架编码、
    /// 帧类型恒 EVENT),不能直接塞整帧(否则可把类型设成 CLOSE/AUTH_RESP)。
    ///
    /// 两道统一闸门(所有业务发送入口在此收口):
    /// 1. **鉴权 pending 拒绝**:register 之后、成功 control(AUTH_RESP/connect_ok)入队并
    ///    `set_authenticated(true)` 之前,session 已对 fan-out 可见但**不得**接收业务帧——
    ///    否则并发投递的 EVENT 会抢在鉴权成功帧之前上线,Rust Client 直接握手失败。
    /// 2. **出站 max_frame**:超过单帧上限的业务帧入队前拦截(接收端同上限会协议断开,
    ///    一条大广播会形成全群断线风暴。+4 为 NASA 帧 4B 长度头(与 FrameCodec 的
    ///    `len ≤ max_frame` 语义对齐);socket.io 文本帧同上限。
    pub(crate) fn send_business(&self, frame: Bytes) -> SendOutcome {
        if self.is_closed() {
            return SendOutcome::Closed;
        }
        if !self.authenticated() {
            return SendOutcome::Dropped; // 鉴权 pending:业务帧不可达
        }
        if self.frame_over_limit(frame.len()) {
            self.note_oversize(frame.len());
            return SendOutcome::Dropped;
        }
        self.outbox.send_business(frame)
    }

    /// 业务作用：出站单帧上限,**按协议区分口径**(不再用无协议区分的 `+4`)
    /// - NASA:`max_frame` 约束**内层 declared 长度**(type+mode+payload,与入站 FrameCodec
    ///   完全同语义);整帧 = declared + 4B 长度头,故上限 = max_frame + 4;
    /// - socket.io:文本无长度头,**文本字节数即口径**(= engine.io OPEN 广告的 maxPayload),
    ///   不得放宽 4 字节。
    ///
    /// # 参数
    /// - `total_len`: 当前消息体或帧的总字节长度。
    fn frame_over_limit(&self, total_len: usize) -> bool {
        match self.protocol {
            Protocol::Nasa => total_len > self.max_frame.saturating_add(4),
            Protocol::SocketIo => total_len > self.max_frame,
        }
    }

    /// 业务作用：超限丢弃:计数 + 限频 warn(1,2,4,8,...,持续超限不刷日志)。
    ///
    /// # 参数
    /// - `len`: 本次待发送出站帧的字节数。
    fn note_oversize(&self, len: usize) {
        let n = self.oversize_dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_power_of_two() {
            tracing::warn!(
                "session {}: outbound frame {len}B exceeds max_frame {}B, dropped (total {n})",
                self.id,
                self.max_frame
            );
        }
    }

    /// 业务作用：因超过出站 max_frame 被拦截的帧累计数(业务帧 + 控制帧都计;可观测)。
    pub fn oversize_dropped(&self) -> u64 {
        self.oversize_dropped.load(Ordering::Relaxed)
    }

    /// 业务作用：加入群组:同步更新自身集合 + Registry by_group 索引(groups 只经此改)。
    /// 若该群在本节点是 0→1 跃迁(首个成员),触发 group_change(Gained)→ 集群增量 presence。
    ///
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    pub fn join_group(self: &Arc<Self>, group: &str) {
        // 在 lifecycle 锁内决定 gained:closed 检查 + by_id 确认 + 本地集合 + by_group 写,
        // 与 cleanup(置 closed + 注销)互斥,杜绝幽灵索引竞态。hook 放锁后再触发。
        let fire = {
            let _life = self.lifecycle.lock().unwrap();
            if self.is_closed() {
                return;
            }
            let Some(reg) = self.registry.upgrade() else {
                return;
            };
            // **先**确认权威表中该 ID 就是本 Session(Arc::ptr_eq)——不仅是"存在该 ID",
            // 否则一个同 ID、注册被拒的 Session 可冒充权威者把自己写进 by_group。
            // 之后注册再 join 因本地已有而早退的问题由身份校验一并杜绝。
            let is_authoritative =
                matches!(reg.by_id.get(self.id.as_str()), Some(e) if Arc::ptr_eq(e.value(), self));
            if !is_authoritative {
                return;
            }
            if !self.groups.lock().unwrap().insert(group.to_string()) {
                return;
            }
            // 在 by_group 桶写锁内插入 + 判跃迁 + **取版本号**:同一 group 的并发 Gained/Lost
            // 因此被桶锁串行化,版本顺序 == 真实状态变更顺序,远端 LWW 不会得到错误最终态
            //。返回 Some(version) 即 0→1 跃迁,需锁外发 hook。
            reg.group_insert_locked(group, &self.id)
                .map(|v| (reg.clone(), v))
        };
        if let Some((reg, version)) = fire {
            reg.fire_group_change(group, GroupDelta::Gained, version);
        }
    }

    /// 业务作用：让会话离开指定群组；用于停止接收该群组广播。
    ///
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    pub fn leave_group(self: &Arc<Self>, group: &str) {
        let fire = {
            let _life = self.lifecycle.lock().unwrap();
            if self.is_closed() {
                return;
            }
            if !self.groups.lock().unwrap().remove(group) {
                return;
            }
            self.registry
                .upgrade()
                .and_then(|reg| reg.group_remove_locked(group, &self.id).map(|v| (reg, v)))
        };
        if let Some((reg, version)) = fire {
            reg.fire_group_change(group, GroupDelta::Lost, version);
        }
    }

    /// 业务作用：框架内部:连接清理——在 lifecycle 锁内置 closed + 注销(与 join/leave 互斥)。
    /// 返回"调用方是否应**此刻**触发 on_disconnect"。裁决规则
    /// - 仅**首位关闭者**有资格(exactly-once 第一道闸);
    /// - 阶段 `Active` → 返回 true(正常路径,调用方立即触发);
    /// - 阶段 `Connecting`(on_connect 尚在执行)→ 置 `ClosingDuringConnect` 并返回
    ///   **false**:逻辑注销同步完成,但 on_disconnect **延迟**到 on_connect 返回后由
    ///   `finish_connect` 触发——绝不与 on_connect 并发倒序(否则 disconnect
    ///   先释放、connect 随后创建的资源永久泄漏);
    /// - 阶段 `Pending`(真实路径=未认证;手工裸装配可能已 set_authenticated)→ 按
    ///   authenticated 兼容旧语义。
    ///
    /// on_disconnect 与群 Lost hook 均在锁外触发。
    pub(crate) fn cleanup(self: &Arc<Self>) -> bool {
        let (was_auth, reg, lost) = {
            let _life = self.lifecycle.lock().unwrap();
            let first_closer = !self.closed.swap(true, Ordering::AcqRel);
            if first_closer {
                // 唤醒读循环立即退出(外部 cleanup 必须连带关闭连接任务)。
                self.closed_token.cancel();
            }
            let was_auth = first_closer
                && match self.phase.load(Ordering::Acquire) {
                    phase::ACTIVE => {
                        self.phase.store(phase::CLOSED, Ordering::Release);
                        true // authenticated 必为 true(Active 仅经 begin/finish_connect 达成)
                    }
                    phase::CONNECTING => {
                        // on_connect 执行中:延迟给 finish_connect,本调用方不触发。
                        self.phase
                            .store(phase::CLOSING_DURING_CONNECT, Ordering::Release);
                        false
                    }
                    phase::CLOSING_DURING_CONNECT | phase::CLOSED => false,
                    _ => self.authenticated(), // Pending:兼容手工裸装配(真实路径必未认证)
                };
            let reg = self.registry.upgrade();
            // **始终**做身份安全注销(不再以 was_auth 为条件):未认证却已注册的 Session 也要回收;
            // 非权威条目则 unregister_locked 自动 no-op。was_auth 只决定是否触发 on_disconnect
            //。Lost hook 收集出来锁外发(不占 lifecycle 锁)。
            let lost = match &reg {
                Some(reg) => reg.unregister_locked(self),
                None => Vec::new(),
            };
            (was_auth, reg, lost)
        };
        if let Some(reg) = reg {
            for (group, version) in lost {
                reg.fire_group_change(&group, GroupDelta::Lost, version);
            }
        }
        was_auth
    }
}

/// per-conn async handler 名额的 RAII 释放守卫:Drop 时归还本连接在飞计数。
/// 随 handler 任务一起持有,任务正常结束或 panic 都释放(计数不泄漏)。
pub(crate) struct HandlerReservation {
    session: Arc<Session>,
    /// `cap == 0`(不限制)时为 false:未占计数,Drop 不归还。
    counted: bool,
}

impl Drop for HandlerReservation {
    /// 业务作用：归还本连接在飞名额;用于 handler 任务结束(或 panic 展开)时释放配额。
    fn drop(&mut self) {
        if self.counted {
            self.session
                .inflight_handlers
                .fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// **受控会话句柄**:传给业务 endpoint handler / 生命周期 hook 的对外类型。只暴露**安全**操作
/// (查询元数据、join/leave 群、发业务帧);**不暴露** `Session::new`/`set_*`/`cleanup`/`send_control`
/// 等框架内部能力——业务既不能篡改会话生命周期,也不能伪造控制帧。
#[derive(Clone)]
pub struct SessionHandle {
    inner: Arc<Session>,
}

impl SessionHandle {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub(crate) fn new(inner: Arc<Session>) -> SessionHandle {
        SessionHandle { inner }
    }

    /// 业务作用：返回会话 ID；用于调用方定位单个连接。
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// 业务作用：读取 uid 状态；用于向调用方暴露当前运行信息。
    pub fn uid(&self) -> Option<&str> {
        self.inner.uid()
    }

    /// 业务作用：读取 endpoint 状态；用于向调用方暴露当前运行信息。
    pub fn endpoint(&self) -> Option<&str> {
        self.inner.endpoint()
    }

    /// 业务作用：读取 transport 状态；用于向调用方暴露当前运行信息。
    pub fn transport(&self) -> Transport {
        self.inner.transport()
    }

    /// 业务作用：返回会话协议类型；用于区分连接的编码和行为。
    pub fn protocol(&self) -> Protocol {
        self.inner.protocol()
    }

    /// 业务作用：返回 Socket.IO 命名空间；用于按命名空间隔离事件。
    pub fn sio_namespace(&self) -> &str {
        self.inner.sio_namespace()
    }

    /// 业务作用：返回会话已加入的群组；用于外部观测和权限判断。
    pub fn groups(&self) -> Vec<String> {
        self.inner.groups()
    }

    /// 业务作用：返回当前对象的 closed 状态。
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// 业务作用：让会话加入指定群组；用于接收后续群组广播。
    ///
    /// # 参数
    /// - `group`: 要加入的群组名。
    pub fn join_group(&self, group: &str) {
        self.inner.join_group(group);
    }

    /// 业务作用：让会话离开指定群组；用于停止接收该群组广播。
    ///
    /// # 参数
    /// - `group`: 要离开的群组名。
    pub fn leave_group(&self, group: &str) {
        self.inner.leave_group(group);
    }

    /// 业务作用：返回会话群组数量；用于观测连接订阅规模。
    pub fn group_count(&self) -> usize {
        self.inner.group_count()
    }

    /// 业务作用：发**业务事件**给本会话:由**框架按会话协议**编码(NASA→JSON EVENT 帧 / socket.io→`42[...]`
    /// 文本)。**只接 event+payload**,不接整帧——业务无法把帧类型设成 CLOSE/AUTH_RESP
    /// 注入控制帧。
    ///
    /// # 参数
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    pub fn send_event(&self, event: &str, payload: &[u8]) -> SendOutcome {
        self.inner.encode_and_send_event(event, payload)
    }

    /// 业务作用：因超过出站 max_frame 被拦截的帧累计数(计数器须经公开句柄可读,
    /// 仅 warn 不等于指标可观测)。
    pub fn oversize_dropped(&self) -> u64 {
        self.inner.oversize_dropped()
    }
}

/// 本节点某 group 本地成员数的跨 0/1 边界事件(集群增量 presence 用)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupDelta {
    /// 本节点首次拥有该群成员(0→1)。
    Gained,
    /// 本节点最后一个该群成员离开(1→0)。
    Lost,
}

/// 群成员 0/1 跃迁回调(集群装配时注入,把变化即时广播为增量 presence)。
/// 第三参为该次跃迁的**单调版本**(presence_seq):接收端按 (node,group) 做 LWW 去旧。
pub type GroupChangeHook = Arc<dyn Fn(&str, GroupDelta, i64) + Send + Sync>;

/// 会话反查表。`by_id` 唯一权威表持 `Arc<Session>`;其余索引只存 SessionId。
#[derive(Default)]
pub struct SessionRegistry {
    by_id: DashMap<String, Arc<Session>>,
    by_uid: DashMap<String, HashSet<String>>,
    by_endpoint: DashMap<String, HashSet<String>>,
    by_group: DashMap<String, HashSet<String>>,
    /// 群 0/1 跃迁回调(集群增量 presence;非集群部署为 None)。
    group_change: Mutex<Option<GroupChangeHook>>,
    /// presence 真相:**版本 + 当前非空群集合**,在群桶写锁内做 0/1 跃迁判定后于此锁内
    /// 原子更新(version+1 且增/删群)。FULL 只 lock+clone 一次即得一致快照——不再扫 by_group
    /// 重试,**高 churn 下也有界完成**,不会被无关热点群饿死。
    presence: Mutex<PresenceState>,
}

/// presence 一致状态:版本与"本节点非空群集合"始终一起更新(同一锁),供 FULL 一次性快照。
#[derive(Default)]
struct PresenceState {
    version: i64,
    groups: HashSet<String>,
}

impl SessionRegistry {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub fn new() -> Arc<SessionRegistry> {
        Arc::new(SessionRegistry::default())
    }

    /// 业务作用：注册会话。在 `lifecycle` 锁内 + `by_id` **单 shard 写锁内**完成"检查并插入",形成原子、
    /// 不变量闭合的协议
    /// - **已 closed** 的 session 拒绝(cleanup 后再 register 不留幽灵条目);
    /// - `by_id` 的 `entry`:`Vacant` 才插入;`Occupied` 仅当 `Arc::ptr_eq` 同一 Session 视为幂等,
    ///   否则(同 ID 不同 Session)返回冲突——杜绝两个并发 register 同时成功;
    /// - 二级索引(uid/endpoint)只在确认本 Session 成为**权威条目**后才更新。
    ///
    /// 返回 `false` 表示注册被拒,调用方须据此关闭该连接。
    #[must_use = "注册可能被拒(closed/ID 冲突),调用方必须处理失败"]
    pub(crate) fn register(&self, s: &Arc<Session>) -> bool {
        use dashmap::mapref::entry::Entry;
        let _life = s.lifecycle.lock().unwrap();
        if s.is_closed() {
            return false;
        }
        // 单 shard 写锁内原子 check-and-insert:权威表唯一性不变量在此闭合。
        match self.by_id.entry(s.id().to_string()) {
            Entry::Occupied(e) => {
                if !Arc::ptr_eq(e.get(), s) {
                    return false; // 同 ID 不同 Session → 冲突
                }
                // 幂等:已是自己,继续确保二级索引。
            }
            Entry::Vacant(e) => {
                e.insert(s.clone());
            }
        }
        // 确认权威后再更新二级索引。
        if let Some(uid) = s.uid() {
            self.by_uid
                .entry(uid.to_string())
                .or_default()
                .insert(s.id().to_string());
        }
        if let Some(ep) = s.endpoint() {
            self.by_endpoint
                .entry(ep.to_string())
                .or_default()
                .insert(s.id().to_string());
        }
        true
    }

    /// 业务作用：统一清理。**仅供 `Session::cleanup`(已持 lifecycle 锁)调用**——不对外暴露,
    /// 杜绝"并发 unregister 绕过生命周期锁制造幽灵桶"。返回需锁外触发的
    /// Lost 跃迁 (group, version);取号在 by_group 桶锁内完成(与 join 串行,版本不逆序)。
    ///
    /// **按 Arc 身份删除**:仅当权威条目确实是本 Session 时才回收——否则
    /// 一个注册失败的同 ID Session B 调 cleanup 会误删有效的 A。权威条目未被本 Session 占用时,
    /// 不动任何二级索引。
    ///
    /// **删除顺序**:先确认身份但**保留 by_id 权威条目**,在 old 仍占 by_id 时
    /// 清二级索引——此刻同 ID 的新 Session register 必失败(Occupied),不会与清理交错;
    /// 二级索引全清完后**最后**才按 Arc 身份 `remove_if` 摘除 by_id。否则先删 by_id 会让新
    /// Session 抢注 + 加群,旧 cleanup 再按字符串 id 把新 Session 的索引一并清理(ID 复用窗口)。
    pub(crate) fn unregister_locked(&self, s: &Arc<Session>) -> Vec<(String, i64)> {
        // 1) 确认权威身份(不清理 by_id):old 仍占位 → 新同 ID Session 此刻无法注册。
        let is_authoritative =
            matches!(self.by_id.get(s.id()), Some(e) if Arc::ptr_eq(e.value(), s));
        if !is_authoritative {
            return Vec::new(); // 不是权威条目 → 不回收任何索引
        }
        // 2) old 仍在 by_id 期间清二级索引(只会删到 old 自己的条目)。
        if let Some(uid) = s.uid() {
            self.remove_from(&self.by_uid, uid, s.id());
        }
        if let Some(ep) = s.endpoint() {
            self.remove_from(&self.by_endpoint, ep, s.id());
        }
        let mut lost = Vec::new();
        for g in s.groups() {
            if let Some(v) = self.group_remove_locked(&g, s.id()) {
                lost.push((g, v));
            }
        }
        // 3) 最后按 Arc 身份摘除权威条目(此后新同 ID Session 才能注册)。
        self.by_id.remove_if(s.id(), |_, cur| Arc::ptr_eq(cur, s));
        lost
    }

    /// 业务作用：按 ID 取**受控句柄**(对外只读视图)。不再返回 `Arc<Session>`——否则业务可借此拿到完整
    /// Session 绕过收口。框架内部用 `by_id_arc`。
    ///
    /// # 参数
    /// - `id`: 框架分配的 session ID。
    pub fn by_id(&self, id: &str) -> Option<SessionHandle> {
        self.by_id.get(id).map(|r| SessionHandle::new(r.clone()))
    }

    /// 业务作用：框架内部:取 `Arc<Session>`(fan-out / 生命周期)。
    pub(crate) fn by_id_arc(&self, id: &str) -> Option<Arc<Session>> {
        self.by_id.get(id).map(|r| r.clone())
    }

    /// 业务作用：按用户 ID 查询会话 ID；用于向同一用户的多个连接发送消息。
    ///
    /// # 参数
    /// - `uid`: 业务用户 ID。
    pub fn ids_by_uid(&self, uid: &str) -> Vec<String> {
        self.by_uid
            .get(uid)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 业务作用：按端点查询会话 ID；用于向某个入口下的连接发送消息。
    ///
    /// # 参数
    /// - `ep`: endpoint 路径。
    pub fn ids_by_endpoint(&self, ep: &str) -> Vec<String> {
        self.by_endpoint
            .get(ep)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 业务作用：按群组查询会话 ID；用于群发前定位目标连接。
    ///
    /// # 参数
    /// - `g`: 群组名。
    pub fn ids_by_group(&self, g: &str) -> Vec<String> {
        self.by_group
            .get(g)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 业务作用：本地有成员的 group 列表(集群 presence 广播用,给对端建群目录)。
    pub fn local_groups(&self) -> Vec<String> {
        self.by_group
            .iter()
            .filter(|e| !e.value().is_empty())
            .map(|e| e.key().clone())
            .collect()
    }

    /// 业务作用：全量 presence 快照:返回 **(版本, 本地群集合)**,二者天然一致——直接 lock+clone
    /// `PresenceState`(版本与群集合在群桶锁内一起更新)。一次完成、**有界**,不再 seqlock 重试,
    /// 不会被高 churn 的无关热点群饿死。
    ///
    pub fn presence_snapshot(&self) -> (i64, Vec<String>) {
        let p = self.presence.lock().unwrap();
        (p.version, p.groups.iter().cloned().collect())
    }

    /// 业务作用：返回在线会话数量；用于监控和容量判断。
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// 业务作用：返回当前对象的 empty 状态。
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// 业务作用：注入群 0/1 跃迁回调(集群装配时调)。**`pub(crate)`**:业务不得覆盖集群 presence hook
    /// (否则静默切断跨节点 presence)。
    pub(crate) fn set_group_change_hook(&self, hook: GroupChangeHook) {
        *self.group_change.lock().unwrap() = Some(hook);
    }

    /// 业务作用：仅发 hook(版本已在桶锁内取好)。慢 I/O 不占任何锁。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    /// - `delta`: 计数、score 或 hash field 的增量。
    /// - `version`: presence、配置或协议版本号。
    fn fire_group_change(&self, group: &str, delta: GroupDelta, version: i64) {
        let hook = self.group_change.lock().unwrap().clone();
        if let Some(h) = hook {
            h(group, delta, version);
        }
    }

    /// 业务作用：在 PresenceState 锁内原子更新「版本 +1 且增/删群」,返回新版本。**调用方须正持有该
    /// group 的 by_group 桶写锁**——使同一 group 的跃迁被桶锁串行化,版本顺序 == 真实状态变更
    /// 顺序;且 version 与 groups 一起更新,FULL 一次 clone 即一致。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    /// - `present`: 当前群组或实例是否存在。
    fn bump_presence(&self, group: &str, present: bool) -> i64 {
        let mut p = self.presence.lock().unwrap();
        p.version += 1;
        if present {
            p.groups.insert(group.to_string());
        } else {
            p.groups.remove(group);
        }
        p.version
    }

    /// 业务作用：在 by_group 桶写锁内:插入 id;**0→1 跃迁则取版本号**(无条件,不以是否装 hook 为条件)。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    /// - `id`: 业务标识,用于定位具体对象或记录。
    fn group_insert_locked(&self, group: &str, id: &str) -> Option<i64> {
        let mut bucket = self.by_group.entry(group.to_string()).or_default();
        let was_empty = bucket.is_empty();
        bucket.insert(id.to_string());
        was_empty.then(|| self.bump_presence(group, true))
    }

    /// 业务作用：在 by_group 桶写锁内:移除 id;**1→0 跃迁则在同一临界区取号**,返回 Some(version)。
    /// 取号后(锁外)尝试删空桶:若放锁窗口内有新 join 插入(它已取更大号),桶非空 → 不删,
    /// 不误发;其 Gained 版本必 > 本 Lost 版本,远端 LWW 最终为在席。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    /// - `id`: 业务标识,用于定位具体对象或记录。
    fn group_remove_locked(&self, group: &str, id: &str) -> Option<i64> {
        let version = if let Some(mut bucket) = self.by_group.get_mut(group) {
            bucket.remove(id);
            bucket.is_empty().then(|| self.bump_presence(group, false))
        } else {
            None
        }; // 桶写锁在此释放
        if version.is_some() {
            self.by_group.remove_if(group, |_, v| v.is_empty());
        }
        version
    }

    /// 业务作用：从某个二级索引桶移除 id,桶空则删桶(避免泄漏空 set)。
    ///
    /// # 参数
    /// - `map`: 当前函数读取或更新的键值映射。
    /// - `key`: 二级索引桶 key,例如 uid、group 或 router。
    /// - `id`: 业务标识,用于定位具体对象或记录。
    fn remove_from(&self, map: &DashMap<String, HashSet<String>>, key: &str, id: &str) {
        if let Some(mut set) = map.get_mut(key) {
            set.remove(id);
            if set.is_empty() {
                drop(set);
                map.remove_if(key, |_, v| v.is_empty());
            }
        }
    }
}
