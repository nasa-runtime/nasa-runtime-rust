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
    /// 鉴权时确认的端点定义，连接生命周期内保留其处理器和回调。
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

/// 出站背压策略。决定业务队列满时保留哪些业务帧；控制帧使用独立容量与淘汰规则。
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
    /// 当前帧已进入该会话出站队列，不表示网络写出或对端确认。
    Queued,
    /// 当前帧未入队；可能被认证、长度、编码或容量约束拒绝。
    Dropped,
    /// 发送入口观察到会话或出站队列已关闭。
    Closed,
}

/// 控制帧队列的(条数, 字节)上限。控制帧极少且小;溢出意味着对端已不读(连接实为死),
/// 此时淘汰队首旧控制帧(保留较新的 CLOSE 等),并计数。避免无界堆积形成 DoS。
const CONTROL_CAP: usize = 1024;
const CONTROL_BYTE_CAP: usize = 8 * 1024 * 1024;

/// 出站双队列:control 优先 + business 有界(条数 + 字节双限,DropNew/DropOldest)。
struct Queues {
    /// 接收端优先提取的控制帧，队列内部保持入队顺序。
    control: VecDeque<Bytes>,
    /// 控制帧尚未移出队列的字节总量。
    control_bytes: usize,
    /// 受业务背压策略约束的待发帧。
    business: VecDeque<Bytes>,
    /// 业务帧尚未移出队列的字节总量。
    business_bytes: usize,
}

/// 一个连接的发送端与唯一接收端共享的队列、容量和关闭通知。
struct OutboxShared {
    /// 两类帧及其字节计数由同一把锁维护，保证容量判断与入队一致。
    q: Mutex<Queues>,
    /// business 条数上限。
    cap: usize,
    /// business 字节上限(防小帧多条/大帧少条两类积压)。
    byte_cap: usize,
    /// 只用于业务队列的满载取舍规则。
    policy: BackpressurePolicy,
    /// 入队或关闭时唤醒独占接收端。
    notify: Notify,
    /// 主动关闭或最后一个发送端释放后置位，供发送与接收检查。
    closed: AtomicBool,
    /// 存活的发送句柄数，不包含接收端持有的共享引用。
    senders: AtomicUsize,
    /// 业务丢弃与旧控制帧淘汰的累计次数。
    dropped: AtomicU64,
}

/// 每连接出站队列的**发送端**(可 clone 给 Registry/Sender)。
/// 自管双队列(非 mpsc):控制帧优先且不被业务淘汰;业务帧条数+字节双限 + DropNew/DropOldest;
/// 暴露丢弃指标。
pub struct Outbox {
    /// 所有克隆发送端共用的队列与发送端持有计数。
    shared: Arc<OutboxShared>,
}

impl Clone for Outbox {
    /// 业务作用：为同一连接增加出站发送句柄，保持队列和容量计数共享。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享原队列的新发送端；增加发送端计数，不重新开放已关闭的队列。
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Outbox {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Outbox {
    /// 业务作用：归还发送端持有计数，最后一个发送端释放时通知接收端结束等待。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：计数递减；最后一次释放会关闭队列并唤醒接收端，已入队帧仍可排空。
    fn drop(&mut self) {
        // 最后一个发送端 drop → 标记关闭并唤醒 writer 让它收尾(对齐 mpsc 语义)。
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.closed.store(true, Ordering::Release);
            self.shared.notify.notify_one();
        }
    }
}

impl Outbox {
    /// 业务作用：为一个连接建立控制帧优先、业务帧有界的出站队列。
    ///
    /// 参数说明：
    /// - `cap`: 业务队列的帧数量上限；零按一处理。
    /// - `byte_cap`: 业务队列的总字节上限；零按一处理。
    /// - `policy`: 业务队列满时的丢弃策略。
    ///
    /// 返回：共享同一队列的发送端与唯一接收端；控制队列使用独立的固定容量限制。
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
            OutboxReceiver {
                shared,
            },
        )
    }

    /// 业务作用：业务帧:超 条数/字节 上限则按策略取舍(DropNew 丢新 / DropOldest 淘汰队首业务帧)。
    /// **不会**淘汰控制帧(两队列分离)。
    ///
    /// 参数说明：
    /// - `frame`: 已编码的业务出站帧。
    ///
    /// 返回：观察到队列关闭时为 `Closed`；新帧无法保留时为 `Dropped`；
    /// 新帧入队为 `Queued`，即使为它淘汰过旧帧也返回此值，不代表网络送达。
    /// `DropOldest` 遇到单帧超过字节容量时，可能先淘汰已有业务帧再丢弃新帧。
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
    /// 参数说明：
    /// - `frame`: 已编码的控制出站帧。
    ///
    /// 返回：观察到队列关闭时为 `Closed`；单帧超过控制队列字节容量时为 `Dropped`；
    /// 其余情况淘汰必要的旧控制帧后入队并返回 `Queued`，不保证对端接收。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本队列累计丢弃的业务帧与淘汰的旧控制帧数量；
    /// 不包含超大新控制帧的拒绝数，也不包含会话入队前的长度或认证拒绝。
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// 业务作用：主动关闭:置 closed + 唤醒 writer。writer 把已入队帧(含 CLOSE)排空后 recv 返回 None 退出
    /// ——不必等所有发送端 Arc 自然 drop。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：关闭标记已发布且接收端已获唤醒通知；不等待网络写出，也不撤回已经通过关闭检查的并发发送。
    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notify.notify_one();
    }
}

/// 每连接出站队列的**接收端**(writer task 独占)。
pub struct OutboxReceiver {
    /// 与发送端共用的待发队列，只有本接收端消费。
    shared: Arc<OutboxShared>,
}

impl OutboxReceiver {
    /// 业务作用：取下一帧:**控制帧优先**,其次业务帧;两者皆空则挂起。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：有待发帧时移出一帧并归还其队列字节容量；队列已关闭且为空时返回 `None`。
    /// 主动关闭和最后一个发送端释放都能结束等待；读取成功不代表该帧已写入连接。
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
    /// 框架为本连接确定的会话标识，注册表以此定位权威实例。
    id: String,
    /// 接入时确定的 TCP 或 WebSocket 传输类型。
    transport: Transport,
    /// 连接使用的帧协议，决定控制帧编码和长度限制口径。
    protocol: Protocol,
    /// 握手流程首次确认的用户标识，建立用户索引后不可修改。
    uid: OnceLock<String>,
    /// 握手流程首次确认的端点路径，与绑定定义共同约束后续分派。
    endpoint: OnceLock<String>,
    /// 鉴权时绑定的 endpoint 快照(generation + Arc)。
    bound: OnceLock<BoundEndpoint>,
    /// socket.io 会话连接的 namespace(出站事件按此成帧);NASA 会话不设,默认 "/"。
    sio_namespace: OnceLock<String>,
    /// 握手成功响应入队并进入连接初始化阶段后发布的认证标志。
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
    /// 会话记录的订阅名称；注销回收注册表索引时保留此本地快照。
    groups: Mutex<HashSet<String>>,
    /// 本连接的有界出站队列发送端。
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
    ///
    /// 参数说明：
    /// - `id`: 本连接的会话标识；注册时还会检查与现存会话是否冲突。
    /// - `transport`: 接入时确定的 TCP 或 WebSocket 传输类型。
    /// - `protocol`: 决定帧编码和长度口径的连接协议。
    /// - `outbox`: 本连接的有界出站发送端。
    /// - `registry`: 后续注册和订阅索引所属的会话表；会话仅保留弱引用。
    /// - `bridge`: Socket.IO 事件负载转换接口。
    /// - `max_frame`: 协议帧长度限制；零按一处理，NASA 帧外层长度头另计。
    ///
    /// 返回：未认证、未注册且处于 `Pending` 的会话；不建立用户或订阅索引。
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
    ///
    /// 参数说明：
    /// - `cap`: 本连接允许同时执行的异步处理器数量；零表示不在此层计数和限流。
    ///
    /// 返回：容量可用时返回释放即归还名额的守卫；已达上限时返回 `None`，调用方应拒绝该事件。
    /// 此方法只预留容量，不检查认证态、关闭状态或全局配额。
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
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// 业务作用：按**会话协议**编码并发一条业务 EVENT 给本会话(供 `SessionHandle::send_event` 复用,
    /// 与 fan_out 的单会话编码保持一致：
    /// - NASA → **JSON_BYTES** EVENT 帧；此便捷发送入口固定采用 JSON 信封；
    /// - socket.io → 经 `PayloadBridge` + namespace 生成标准 `42[...]` 文本。
    ///
    /// 参数说明：
    /// - `event`: 当前会话接收的业务事件名。
    /// - `payload`: 事件负载字节；NASA 放入信封，Socket.IO 交由负载桥转换。
    ///
    /// 返回：已关闭返回 `Closed`；未认证、编码失败、长度或队列容量拒绝返回 `Dropped`；
    /// 入队成功返回 `Queued`。本入口不选择其它序列化模式，也不等待对端确认。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建会话时确定且不再变更的连接标识；不表示该会话当前仍在注册表中。
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 业务作用：提供连接的传输类型，供处理器区分 TCP 与 WebSocket 接入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建会话时固定的传输类型；与业务负载序列化方式无关。
    pub fn transport(&self) -> Transport {
        self.transport
    }

    /// 业务作用：返回会话协议类型；用于区分连接的编码和行为。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本连接固定使用的 NASA 帧协议或 Socket.IO 协议；不代表某一种负载序列化模式。
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// 业务作用：读取握手流程绑定的用户标识，供用户索引与处理器定位连接所属用户。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已经绑定的标识，尚未绑定时为 `None`；本查询不验证标识内容或当前认证态。
    pub fn uid(&self) -> Option<&str> {
        self.uid.get().map(String::as_str)
    }

    /// 业务作用：读取会话绑定的端点路径，供端点索引与生命周期处理定位入口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次设置的端点路径，尚未设置时为 `None`；不重新查询端点是否在线。
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.get().map(String::as_str)
    }

    /// 业务作用：返回会话认证状态；用于在处理消息前判断是否已通过鉴权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：框架发布的认证标志；`true` 不代表连接仍开放或 `on_connect` 已完成。
    pub fn authenticated(&self) -> bool {
        self.authenticated.load(Ordering::Acquire)
    }

    /// 业务作用：取得会话持有的本地订阅名称，供观测与注销时回收索引。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无排序保证的集合副本；它是本地订阅记录，不证明业务授权仍然有效。
    /// 关闭会话后该记录可能保留，是否可投递还须检查关闭状态和权威注册表。
    pub fn groups(&self) -> Vec<String> {
        self.groups.lock().unwrap().iter().cloned().collect()
    }

    /// 业务作用：读取会话记录的订阅数量，供接入策略执行容量判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地订阅集合当前大小，不分配集合副本；查询本身不预留订阅名额。
    pub fn group_count(&self) -> usize {
        self.groups.lock().unwrap().len()
    }

    /// 业务作用：框架内部:鉴权时设一次 uid。**`pub(crate)`**:业务(尤其鉴权回调)不得改 uid——否则
    /// 回调先 set_uid、框架随后 set 因 OnceLock 已占用而静默失败,注册 uid 与鉴权返回不一致。
    ///
    /// 参数说明：
    /// - `uid`: 握手流程确认的用户标识；调用方负责内容校验并在注册索引前设置。
    ///
    /// 返回：首次调用保存标识，后续调用保留原值；不执行认证、不更新已建立的用户索引。
    pub(crate) fn set_uid(&self, uid: String) {
        let _ = self.uid.set(uid);
    }

    /// 业务作用：框架内部:握手/鉴权时设一次 endpoint。
    ///
    /// 参数说明：
    /// - `ep`: 接入流程解析并确认的端点路径，须在注册端点索引前设置。
    ///
    /// 返回：首次调用保存路径，重复调用不覆盖；不查询端点注册表，也不自动绑定处理器快照。
    pub(crate) fn set_endpoint(&self, ep: String) {
        let _ = self.endpoint.set(ep);
    }

    /// 业务作用：框架内部:鉴权时绑定 endpoint 快照(generation + Arc)。
    ///
    /// 参数说明：
    /// - `generation`: 本次获取端点定义时对应的注册代际，用于后续投递复验。
    /// - `endpoint`: 与该代际匹配的端点定义，生命周期回调持续使用此实例。
    ///
    /// 返回：首次调用同时保存代际和定义；重复绑定保留原快照，不重新确认端点在线状态。
    pub(crate) fn bind_endpoint(&self, generation: u64, endpoint: Arc<Endpoint>) {
        let _ = self.bound.set(BoundEndpoint {
            generation,
            endpoint,
        });
    }

    /// 业务作用：鉴权时绑定的 endpoint 快照(hook/分派恒用它,不按 path 重新解析)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次绑定的端点及其代际，未绑定为 `None`；端点下线或替换不会改写此快照。
    pub(crate) fn bound_endpoint(&self) -> Option<&BoundEndpoint> {
        self.bound.get()
    }

    /// 业务作用：绑定代际(出站投递兜底校验用)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：会话绑定时的端点代际，尚未绑定为 `None`；调用方须与当前注册代际比较后决定是否投递。
    pub(crate) fn bound_generation(&self) -> Option<u64> {
        self.bound.get().map(|b| b.generation)
    }

    /// 业务作用：endpoint 下线/替换时向本会话发协议化的"endpoint removed"关闭通知
    /// (NASA → CLOSE(ENDPOINT_REMOVED) 帧;socket.io → `41` DISCONNECT)。
    /// 须在 cleanup **之前**调用(cleanup 置 closed 后控制帧不再入队)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尝试排入关闭控制帧；编码或入队失败不会向调用方报错，也不直接注销会话。
    /// 调用方仍须执行清理和出站关闭，不能依赖通知送达来完成端点下线。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已绑定的命名空间；尚未设置时返回 `/`，NASA 会话也使用此查询默认值。
    pub fn sio_namespace(&self) -> &str {
        self.sio_namespace.get().map(String::as_str).unwrap_or("/")
    }

    /// 业务作用：框架内部:socket.io CONNECT 时设一次 namespace。仅 socketio 路径调用。
    ///
    /// 参数说明：
    /// - `ns`: CONNECT 报文中已由握手流程解析的命名空间。
    ///
    /// 返回：首次调用固定出站 Socket.IO 事件的命名空间；后续调用不覆盖，不在此校验路径或权限。
    #[cfg_attr(not(feature = "socketio"), allow(dead_code))]
    pub(crate) fn set_sio_namespace(&self, ns: String) {
        let _ = self.sio_namespace.set(ns);
    }

    /// 业务作用：框架内部:鉴权结果落地。业务不得自行改认证态。
    ///
    /// 参数说明：
    /// - `v`: 待发布的认证标志；置为 `true` 前须完成身份绑定、注册、成功响应入队和 `begin_connect`。
    ///
    /// 返回：以 Release 顺序发布标志；不自动完成握手步骤，也不改变连接阶段或关闭状态。
    pub(crate) fn set_authenticated(&self, v: bool) {
        self.authenticated.store(v, Ordering::Release);
    }

    /// 业务作用：是否已进入清理(closed)。closed 后 join_group/send 等被拒。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：是否已有清理调用发布关闭标记；`true` 不代表注销、断连回调或网络排空全部完成。
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// 业务作用：向协议读循环提供连接级取消通知，使外部下线能结束正在等待输入的任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享同一取消状态的令牌；取消后再取得的令牌也能观察到关闭请求，不创建新会话。
    pub(crate) fn closed_token(&self) -> CancellationToken {
        self.closed_token.clone()
    }

    /// 业务作用：进入 `Connecting`(即将执行 on_connect)。须在 `set_authenticated`
    /// **之前**调——保证"authenticated=true 而 phase 仍 Pending"的窗口不存在,cleanup 的
    /// 阶段裁决因此完备。返回 false = 会话已被并发下线,调用方跳过 on_connect(也无配对
    /// disconnect 需要触发)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未关闭时进入 `Connecting` 并返回 `true`；已关闭时返回 `false`。
    /// 每条连接的握手流程只应调用一次，本方法不校验重复进入或其它前置阶段。
    pub(crate) fn begin_connect(&self) -> bool {
        let _life = self.lifecycle.lock().unwrap();
        // 与清理共用生命周期锁，关闭者先取得锁后不能再开启连接回调。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：延迟的断连回调应由当前调用方执行时返回 `true`；正常进入 `Active` 或无需处理时返回 `false`。
    /// 调用前必须保证 `on_connect` 已返回，本方法本身不执行业务回调。
    #[must_use = "返回 true 时调用方必须补触发 on_disconnect"]
    pub(crate) fn finish_connect(&self) -> bool {
        let _life = self.lifecycle.lock().unwrap();
        match self.phase.load(Ordering::Acquire) {
            phase::CONNECTING => {
                self.phase.store(phase::ACTIVE, Ordering::Release);
                false
            }
            phase::CLOSING_DURING_CONNECT => {
                // 清理已经完成，当前调用方接管唯一的断连回调责任后再发布终态。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅认证已发布、未观察到关闭且阶段允许扇出时为 `true`；
    /// 这是一次状态筛选，不预留投递权，实际入队仍须复验会话和队列状态。
    pub(crate) fn ready_for_fanout(&self) -> bool {
        // 初始化尚未完成或已进入清理的会话不得成为广播目标。
        if !self.authenticated() || self.is_closed() {
            return false;
        }
        matches!(
            self.phase.load(Ordering::Acquire),
            phase::ACTIVE | phase::PENDING
        )
    }

    /// 业务作用：框架内部:cleanup 主动关闭出站队列(writer 排空已入队帧后退出)。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：向出站队列发布关闭标记并唤醒接收端；不等待排空，也不注销会话索引或触发断连回调。
    pub(crate) fn close_outbox(&self) {
        self.outbox.close();
    }

    /// 业务作用：发**控制**帧(AUTH_RESP/PONG/CLOSE/握手等):进优先队列,writer 先发。
    /// closed 后一律 `Closed`,防晚到的 handler 往已关连接排队。
    /// **`pub(crate)`**:控制帧只由框架发——否则鉴权回调/业务 handler 拿到 `Arc<Session>` 后
    /// 可伪造 `AUTH_RESP` 等控制帧。业务发消息用 `Sender` / `send_business`。
    ///
    /// 参数说明：
    /// - `frame`: 已由框架编码的完整控制帧，长度须符合当前连接协议。
    ///
    /// 返回：观察到会话或队列关闭返回 `Closed`；长度或队列容量拒绝返回 `Dropped`；
    /// 进入优先队列返回 `Queued`。认证前允许发送握手控制帧，不承诺网络送达。
    pub(crate) fn send_control(&self, frame: Bytes) -> SendOutcome {
        // 已开始清理的会话不能接纳新的控制帧，避免延迟处理器继续保活连接。
        if self.is_closed() {
            return SendOutcome::Closed;
        }
        // 控制帧同样受协议长度上限约束，避免服务端发送客户端必然拒收的 AUTH_RESP/CLOSE。
        // build 期最小容量校验与 reason 截断约束正常输出，超限时记录丢弃而不进入发送队列。
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
    ///
    /// 参数说明：
    /// - `frame`: 与当前会话协议相符的完整业务帧；帧类型由框架调用方保证。
    ///
    /// 返回：观察到会话或队列关闭返回 `Closed`；未认证、超长或背压拒绝返回 `Dropped`；
    /// 入队成功返回 `Queued`。不解析整帧校验事件类型，也不等待客户端确认。
    pub(crate) fn send_business(&self, frame: Bytes) -> SendOutcome {
        // 关闭与认证在入队前复验，晚完成的业务处理器不能凭旧状态直接发送。
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
    /// 参数说明：
    /// - `total_len`: 已编码完整帧的字节数，包含 NASA 外层长度头或完整 Socket.IO 文本。
    ///
    /// 返回：超出对应协议长度限制时为 `true`；恰好等于上限时允许，不检查帧内容是否合法。
    fn frame_over_limit(&self, total_len: usize) -> bool {
        match self.protocol {
            Protocol::Nasa => total_len > self.max_frame.saturating_add(4),
            Protocol::SocketIo => total_len > self.max_frame,
        }
    }

    /// 业务作用：超限丢弃:计数 + 限频 warn(1,2,4,8,...,持续超限不刷日志)。
    ///
    /// 参数说明：
    /// - `len`: 触发限制的字节数；编码前预检时为原始负载长度，其余入口为完整帧长度。
    ///
    /// 返回：超限计数增加一次，累计值为二的幂时输出警告；不关闭连接、不操作发送队列。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本会话在编码前负载预检或入队前整帧检查中拒绝的超限发送次数，不含队列背压丢弃。
    pub fn oversize_dropped(&self) -> u64 {
        self.oversize_dropped.load(Ordering::Relaxed)
    }

    /// 业务作用：为已注册的会话添加订阅，同步本地集合和注册表的订阅索引。
    /// 本节点首次拥有该订阅成员时，在锁外通知集群发布 `Gained` 变化。
    ///
    /// 参数说明：
    /// - `group`: 要订阅的名称；业务处理器须先完成授权和容量校验。
    ///
    /// 返回：有效且尚未订阅时更新索引；已关闭、注册表已释放、身份不匹配或重复订阅时不变。
    /// 本方法不返回是否添加成功，也不自行执行业务鉴权。
    pub fn join_group(self: &Arc<Self>, group: &str) {
        // 在 lifecycle 锁内决定 gained:closed 检查 + by_id 确认 + 本地集合 + by_group 写,
        // 与 cleanup(置 closed + 注销)互斥,杜绝幽灵索引竞态。hook 放锁后再触发。
        let fire = {
            let _life = self.lifecycle.lock().unwrap();
            // 清理已经取得控制权后，不允许晚到的订阅重建索引。
            if self.is_closed() {
                return;
            }
            // 所属注册表已释放，无法维持本地记录与权威索引一致，拒绝添加。
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

    /// 业务作用：取消会话的指定订阅，并在本节点最后一个成员离开时发布 `Lost` 变化。
    ///
    /// 参数说明：
    /// - `group`: 要取消的订阅名称。
    ///
    /// 返回：移除本地记录和仍存活注册表中的成员索引；未订阅或已关闭时不变。
    /// 注册表已释放时只移除本地记录，所有变化通知都在生命周期锁外执行。
    pub fn leave_group(self: &Arc<Self>, group: &str) {
        let fire = {
            let _life = self.lifecycle.lock().unwrap();
            // 已关闭会话的索引由清理流程统一回收，晚到的取消操作不再产生额外状态变化。
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
    /// `on_disconnect` 与订阅 `Lost` 回调均在锁外触发。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前调用方须立即执行断连回调时为 `true`；回调延迟、未认证或重复清理时为 `false`。
    /// 无论是否返回 `true`，都会尝试按实例身份注销索引；不会清空会话的订阅名称快照，
    /// 也不会直接关闭出站队列或执行断连回调，这两项由连接流程负责。
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
    /// 预留名额所属的连接；守卫存活期间保留会话以归还同一个计数器。
    session: Arc<Session>,
    /// `cap == 0`(不限制)时为 false:未占计数,Drop 不归还。
    counted: bool,
}

impl Drop for HandlerReservation {
    /// 业务作用：归还本连接在飞名额;用于 handler 任务结束(或 panic 展开)时释放配额。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预留时实际占用计数的守卫归还一个名额；不限流守卫不修改计数，不改变连接生命周期。
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
    /// 受控接口背后的会话实例；不向业务暴露身份写入或控制帧发送入口。
    inner: Arc<Session>,
}

impl SessionHandle {
    /// 业务作用：将框架会话包装为业务可持有的受控句柄，限制身份与生命周期写权限。
    ///
    /// 参数说明：
    /// - `inner`: 框架创建的会话实例，句柄与它共享身份、订阅和出站状态。
    ///
    /// 返回：受控会话句柄；不创建新连接，不改变认证或注册状态。
    pub(crate) fn new(inner: Arc<Session>) -> SessionHandle {
        SessionHandle {
            inner,
        }
    }

    /// 业务作用：返回会话 ID；用于调用方定位单个连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层会话固定的连接标识；句柄存在不保证注册表仍保留该连接。
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// 业务作用：向业务处理器提供会话已绑定的用户标识，不开放身份修改能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次绑定的用户标识，未绑定为 `None`；调用方不能据此推断当前连接仍在线。
    pub fn uid(&self) -> Option<&str> {
        self.inner.uid()
    }

    /// 业务作用：向业务处理器提供当前句柄所属的端点路径。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：会话首次设置的端点路径，未设置为 `None`；不反映端点注册表的后续上下线变化。
    pub fn endpoint(&self) -> Option<&str> {
        self.inner.endpoint()
    }

    /// 业务作用：向业务处理器提供 TCP 或 WebSocket 接入类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层连接固定的传输类型，不触发协议协商或连接状态变更。
    pub fn transport(&self) -> Transport {
        self.inner.transport()
    }

    /// 业务作用：返回会话协议类型；用于区分连接的编码和行为。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层连接的 NASA 或 Socket.IO 帧协议，不表示业务负载采用的具体序列化模式。
    pub fn protocol(&self) -> Protocol {
        self.inner.protocol()
    }

    /// 业务作用：返回 Socket.IO 命名空间；用于按命名空间隔离事件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：连接绑定的命名空间，未设置时为 `/`；不新增订阅，也不执行命名空间授权。
    pub fn sio_namespace(&self) -> &str {
        self.inner.sio_namespace()
    }

    /// 业务作用：向业务处理器提供会话记录的订阅名称快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无排序保证的名称副本；连接关闭后可能仍有记录，不能作为在线状态或业务授权的证明。
    pub fn groups(&self) -> Vec<String> {
        self.inner.groups()
    }

    /// 业务作用：让业务处理器判断会话是否已进入清理流程。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层会话的关闭标记；请求断连与清理实际开始之间可能仍为 `false`，不代表连接可继续写出。
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// 业务作用：撤销本连接的业务写出并通知协议循环结束会话。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发出关闭信号；会话索引和断连回调由原协议循环按既定顺序清理。
    pub fn disconnect(&self) {
        // 先收回出站容量再唤醒读循环，授权撤销后不再接受新的业务消息入队。
        self.inner.close_outbox();
        self.inner.closed_token.cancel();
    }

    /// 业务作用：请求会话添加指定订阅，以便后续按订阅名称选择投递目标。
    ///
    /// 参数说明：
    /// - `group`: 已由业务处理器完成授权和容量检查的订阅名称。
    ///
    /// 返回：委托底层会话同步本地记录与索引；关闭、身份失效或重复订阅时不变，不提供成功确认。
    pub fn join_group(&self, group: &str) {
        self.inner.join_group(group);
    }

    /// 业务作用：请求会话取消指定订阅，移除后续订阅投递的索引依据。
    ///
    /// 参数说明：
    /// - `group`: 要取消的订阅名称。
    ///
    /// 返回：委托底层会话移除记录及索引；已关闭或未订阅时不变，不撤回已经入队的消息。
    pub fn leave_group(&self, group: &str) {
        self.inner.leave_group(group);
    }

    /// 业务作用：提供会话记录的订阅数量，供业务容量判断和观测使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地订阅集合大小；不预留新订阅名额，不表示集群所有节点上的订阅总数。
    pub fn group_count(&self) -> usize {
        self.inner.group_count()
    }

    /// 业务作用：发**业务事件**给本会话:由**框架按会话协议**编码(NASA→JSON EVENT 帧 / socket.io→`42[...]`
    /// 文本)。**只接 event+payload**,不接整帧——业务无法把帧类型设成 CLOSE/AUTH_RESP
    /// 注入控制帧。
    ///
    /// 参数说明：
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    ///
    /// 返回：编码并进入本会话业务队列时为 `Queued`；会话或队列关闭为 `Closed`；
    /// 认证、编码、长度或容量约束拒绝为 `Dropped`。不等待客户端确认，不投递给其它会话。
    pub fn send_event(&self, event: &str, payload: &[u8]) -> SendOutcome {
        self.inner.encode_and_send_event(event, payload)
    }

    /// 业务作用：向业务观测提供本会话因出站长度限制而被拒绝的发送次数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：编码前负载预检及整帧长度拒绝的累计次数，包含业务和控制发送，不含队列背压丢弃。
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
    /// 会话 ID 对应的唯一权威实例，二级索引的候选 ID 必须回到此表解析。
    by_id: DashMap<String, Arc<Session>>,
    /// 用户标识到本节点会话 ID 的索引，可包含同一用户的多个连接。
    by_uid: DashMap<String, HashSet<String>>,
    /// 端点路径到会话 ID 的索引，供端点下线时枚举并复验绑定代际。
    by_endpoint: DashMap<String, HashSet<String>>,
    /// 订阅名称到本节点成员会话 ID 的索引，空桶在移除后复验回收。
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
    /// 本节点订阅成员跨越空与非空边界时递增的 presence 版本。
    version: i64,
    /// 与当前版本同时发布的非空订阅集合。
    groups: HashSet<String>,
}

impl SessionRegistry {
    /// 业务作用：创建本节点会话的权威表及用户、端点、订阅反查索引。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：没有会话或订阅、presence 版本为零且未安装变化回调的共享注册表。
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
    /// 参数说明：
    /// - `s`: 身份和端点已由握手流程绑定的会话实例；认证成功响应须在注册成功后再发送。
    ///
    /// 返回：新注册或同一实例的幂等注册为 `true`；会话已关闭或同 ID 被其它实例占用为 `false`，
    /// 调用方须关闭被拒绝的连接。本方法不执行鉴权，也不将会话标记为已认证。
    #[must_use = "注册可能被拒(closed/ID 冲突),调用方必须处理失败"]
    pub(crate) fn register(&self, s: &Arc<Session>) -> bool {
        use dashmap::mapref::entry::Entry;
        let _life = s.lifecycle.lock().unwrap();
        // 与清理串行检查，防止清理完成后重新建立会话索引。
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
    ///
    /// 参数说明：
    /// - `s`: 正在清理的会话；调用方必须持有其生命周期锁并已发布关闭状态。
    ///
    /// 返回：本节点失去最后一个订阅成员时产生的名称和 presence 版本，供调用方锁外通知。
    /// 非权威实例返回空列表且不删除索引；成功注销也可能返回空列表。
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
    /// 参数说明：
    /// - `id`: 框架分配的 session ID。
    ///
    /// 返回：当前权威表中对应实例的受控句柄；不存在时为 `None`。
    /// 取得句柄不保证后续仍在线，发送和订阅操作会再次检查底层状态。
    pub fn by_id(&self, id: &str) -> Option<SessionHandle> {
        self.by_id.get(id).map(|r| SessionHandle::new(r.clone()))
    }

    /// 业务作用：框架内部:取 `Arc<Session>`(fan-out / 生命周期)。
    ///
    /// 参数说明：
    /// - `id`: 要在本节点权威表中查询的会话标识。
    ///
    /// 返回：查询时对应的共享会话，不存在为 `None`；调用方仍须复验投递资格或清理时的实例身份。
    pub(crate) fn by_id_arc(&self, id: &str) -> Option<Arc<Session>> {
        self.by_id.get(id).map(|r| r.clone())
    }

    /// 业务作用：按用户 ID 查询会话 ID；用于向同一用户的多个连接发送消息。
    ///
    /// 参数说明：
    /// - `uid`: 业务用户 ID。
    ///
    /// 返回：该用户在本节点索引中的会话 ID 副本，无记录时为空；不排序，不保证返回时仍可投递。
    pub fn ids_by_uid(&self, uid: &str) -> Vec<String> {
        self.by_uid
            .get(uid)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 业务作用：按端点查询会话 ID；用于向某个入口下的连接发送消息。
    ///
    /// 参数说明：
    /// - `ep`: endpoint 路径。
    ///
    /// 返回：本节点该端点索引中的会话 ID 副本，无记录时为空；
    /// 列表不绑定某个端点代际，下线处理仍须逐个复验会话绑定的定义。
    pub fn ids_by_endpoint(&self, ep: &str) -> Vec<String> {
        self.by_endpoint
            .get(ep)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 业务作用：按订阅名称查询本节点会话 ID，为订阅投递筛选候选连接。
    ///
    /// 参数说明：
    /// - `g`: 要查询的订阅名称。
    ///
    /// 返回：本地订阅索引中的会话 ID 副本，无记录时为空；
    /// 不排序，也不替代发送前的会话存活、认证和端点代际检查。
    pub fn ids_by_group(&self, g: &str) -> Vec<String> {
        self.by_group
            .get(g)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 业务作用：枚举本节点当前有成员的订阅名称，供本地观测使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：遍历订阅索引得到的无序名称列表；并发变更下不保证来自同一时刻。
    /// 发布带版本的集群 presence 时应使用 `presence_snapshot`，避免版本与集合错配。
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
    /// 参数说明: 无。
    ///
    /// 返回：同一临界区复制的 presence 版本和本节点非空订阅集合；集合不排序。
    /// 不等待网络发布或对端确认，复制完成后的变化由后续快照或增量通知表达。
    pub fn presence_snapshot(&self) -> (i64, Vec<String>) {
        let p = self.presence.lock().unwrap();
        (p.version, p.groups.iter().cloned().collect())
    }

    /// 业务作用：提供本节点权威会话表的条目数，供监控注册规模使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前表长度，包含已注册但认证发布或连接回调尚未完成的条目，不是可投递连接数。
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// 业务作用：判断本节点是否还保留已注册的会话条目。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：权威表为空时为 `true`；不表示监听器已停止或所有连接任务都已退出。
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// 业务作用：注入群 0/1 跃迁回调(集群装配时调)。**`pub(crate)`**:业务不得覆盖集群 presence hook
    /// (否则静默切断跨节点 presence)。
    ///
    /// 参数说明：
    /// - `hook`: 接收订阅首个成员加入或最后一个成员退出通知的集群回调。
    ///
    /// 返回：替换后续通知使用的回调；不重放既有成员状态，也不取消已经取走旧回调的通知。
    pub(crate) fn set_group_change_hook(&self, hook: GroupChangeHook) {
        *self.group_change.lock().unwrap() = Some(hook);
    }

    /// 业务作用：仅发 hook(版本已在桶锁内取好)。慢 I/O 不占任何锁。
    ///
    /// 参数说明：
    /// - `group`: 本节点成员存在状态发生变化的订阅名称。
    /// - `delta`: 首个成员加入的 `Gained` 或最后一个成员退出的 `Lost`。
    /// - `version`: 在对应订阅桶锁内为此次变化分配的 presence 版本。
    ///
    /// 返回：安装回调时同步调用一次，否则不做操作；不持有注册表锁执行回调，也不捕获其 panic。
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
    /// 参数说明：
    /// - `group`: 已在其订阅桶写锁下确认发生成员存在状态变化的名称。
    /// - `present`: 变化后本节点是否仍有该订阅成员。
    ///
    /// 返回：递增后的 presence 版本；同一临界区更新非空订阅集合，不发送集群通知。
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
    /// 参数说明：
    /// - `group`: 要添加成员的订阅名称。
    /// - `id`: 已确认权威身份的会话 ID；调用方须持有该会话生命周期锁。
    ///
    /// 返回：原桶为空时返回此次 `Gained` 的版本；已有成员时返回 `None`。
    /// 插入与版本分配在同一桶写锁内完成，调用方在释放生命周期锁后通知集群。
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
    /// 参数说明：
    /// - `group`: 要移除成员的订阅名称。
    /// - `id`: 待移除的会话 ID；调用方须持有该会话生命周期锁并保证索引归属。
    ///
    /// 返回：找到桶且移除后为空时返回新 presence 版本；无桶或仍有成员时为 `None`。
    /// 解锁后只删除仍为空的桶，避免删除同时加入的新成员；通知由调用方锁外执行。
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
    /// 参数说明：
    /// - `map`: 待清理的用户或端点二级索引。
    /// - `key`: 对应的用户标识或端点路径。
    /// - `id`: 已经确认归属的会话 ID；调用方须在清理期间保留其主索引占位。
    ///
    /// 返回：从对应桶移除会话 ID，仅在复验仍为空时删除桶；不存在的桶不变，不触发 presence 通知。
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
