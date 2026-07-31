// ============================================================================
// src/cluster.rs —— 集群:跨节点 fan-out + 防回环(R0.6)。
// 架构说明:ClusterEvent 显式带 message_mode(已在 proto)。
//
// 分层:Notifier(transport 接缝,可换内存实现/Redis/Kafka)+ Cluster(transport 无关编排)。
// 数据流:本地 Sender.send → cluster.publish(包 ClusterEvent)→ Notifier 广播 →
//   对端 Cluster.on_received(防回环 + 目标过滤)→ Sender.send_local(本地分发)。
// 防回环:ClusterEvent.source_node == 本节点 → 丢弃(faithful:广播会把自己发的也读回来)。
// ============================================================================

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use naws_proto::{ClusterEvent, Message, Mode, WireCodec};

/// 集群公开的非 fallible timer API 统一使用的安全上限。
const MAX_CLUSTER_RUNTIME_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 周期任务不能接受零间隔；极端时长也必须在进入 Tokio timer 前收敛。
fn bounded_period(duration: Duration) -> Duration {
    duration.clamp(Duration::from_millis(1), MAX_CLUSTER_RUNTIME_DURATION)
}

/// 墙钟参数来自公开观测 API；饱和差值避免极端或回拨时间戳触发整数溢出。
fn within_ttl(now_local: i64, last_seen_local: i64, ttl_millis: i64) -> bool {
    now_local.saturating_sub(last_seen_local) <= ttl_millis
}

// Redis transport belongs to the public cluster seam. Keeping the module private while re-exporting
// its stable constructor prevents business crates from depending on the internal file layout.
#[cfg(feature = "redis")]
mod redis_notifier;
#[cfg(feature = "redis")]
pub use redis_notifier::{RedisNotifier, RedisNotifierConfig};

/// 收到一条对端 payload 时的回调(Notifier 在接收线程调)。
pub type OnMessage = Arc<dyn Fn(Bytes) + Send + Sync>;

/// 集群启动失败原因(ready 必须能表达失败,不能"假就绪")。
#[derive(Debug, Clone)]
pub enum StartError {
    /// 在期限内未就绪(连不上 / 认证失败 / 取消)。
    NotReady(String),
    /// 同一 notifier 重复 start。
    AlreadyStarted,
}

/// `start` 返回的就绪 future:`Ok` 表示收发已就绪(reader 已连上并确定起始游标);
/// `Err` 表示初始化失败(连接/认证失败、被取消、重复 start)——不再"假就绪"。
pub type ReadyFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), StartError>> + Send>>;

/// 跨节点发布结果(publish 不再吞结果,可观测)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// 已入发布队列(at-most-once:入队后仍可能因重连窗口/XADD 失败丢)。
    Queued,
    /// 发布队列已满,丢弃。
    Full,
    /// transport 已关闭。
    Closed,
}

/// data plane 发布结果；本地校验拒绝与 transport 容量失败保持可区分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPublishOutcome {
    /// 已同步进入 producer 且 delivery observer 已登记。
    Queued,
    /// data producer 的有界准入或原生队列已满。
    Full,
    /// data producer 已进入关闭阶段。
    Closed,
    /// payload、路由或 mode 在本地被确定拒绝。
    Rejected,
}

/// data publisher 所需的受控来源身份借用视图。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClusterSourceRef<'a> {
    /// 当前稳定逻辑节点 ID。
    pub node: &'a str,
    /// 外部单调来源生成的非零 incarnation。
    pub incarnation: u128,
}

/// 跨节点 data plane 的可替换少拷贝发布接缝。
pub trait ClusterDataPublisher: Send + Sync {
    /// 直接发布借用业务信封，不先编码中间 ClusterEvent。
    ///
    /// # 参数
    ///
    /// - `source`: 受控来源节点与 incarnation。
    /// - `message`: 当前业务消息信封。
    /// - `mode`: 客户端最终消息编码模式。
    /// - `target_nodes`: 可选精确目标逻辑节点。
    fn publish(
        &self,
        source: ClusterSourceRef<'_>,
        message: &Message,
        mode: Mode,
        target_nodes: Option<&[&str]>,
    ) -> DataPublishOutcome;
}

/// 跨节点 transport 抽象(单一 transport,不混用)。换 Redis/Kafka/Peer-TCP 只换它。
pub trait Notifier: Send + Sync {
    /// 启动收发;返回 ready future,await 它直到就绪。
    ///
    /// # 参数
    /// - `on_message`: transport 收到跨节点 payload 后调用的回调。
    fn start(&self, on_message: OnMessage) -> ReadyFuture;

    /// 发布一条 payload(ClusterEvent 编码后的字节),返回入队结果。
    ///
    /// # 参数
    /// - `payload`: 已编码的 ClusterEvent 字节。
    fn publish(&self, payload: Bytes) -> PublishOutcome;

    /// 关闭集群通知器；用于释放节点广播相关资源。
    fn shutdown(&self) {}
    /// transport 的后台任务跟踪器(供 graceful shutdown join);无后台任务返回 None。
    fn task_tracker(&self) -> Option<&TaskTracker> {
        None
    }
}

/// 默认节点存活 TTL:超过这段时间没"听到"某节点(数据事件 / presence)即判为下线。
pub(crate) const DEFAULT_NODE_TTL: Duration = Duration::from_secs(30);

// presence 类型(仅当 message_bytes=None 时,复用 ClusterEvent.message_mode 字段表达):
//   FULL = 全量快照(周期对账,整体替换该节点群集合);
//   ADD/DEL = 增量(群成员 0↔1 跃迁即时广播,只带变化的群)。
const PRESENCE_FULL: i8 = 0;
const PRESENCE_ADD: i8 = 1;
const PRESENCE_DEL: i8 = 2;

/// 集群编排器。transport 无关:包/解 ClusterEvent + 防回环 + 目标过滤 + 本地分发 + 节点存活。
/// 本节点全量 presence 快照提供者:返回 **(版本, 本地群集合)**,二者天然一致——
/// SessionRegistry::presence_snapshot 直接 lock+clone PresenceState(版本与群集合一起更新),
/// 一次完成有界、不饿死。
pub type PresenceProvider = Arc<dyn Fn() -> (i64, Vec<String>) + Send + Sync>;

/// 集群规模只读快照(经 [`crate::RunningServer::cluster_stats`] 暴露,不泄漏可变 Cluster)。
/// node/alive/tombstone 三值在**同一把锁内同一时刻**计算,口径一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterStats {
    /// 节点条目总数(含 tombstone)。
    pub node_count: usize,
    /// 当前存活(TTL 内)的对端节点数。
    pub alive_count: usize,
    /// 过期但为 fencing 保留 incarnation 的 tombstone 数;**部署须监控其增长**。
    pub tombstone_count: usize,
    /// 数据事件 publish 被 transport 拒(Full/Closed)的累计数。
    pub publish_dropped: u64,
    /// presence publish 被拒的累计数(周期 FULL 自愈,但应可观测;R20 P3)。
    pub presence_dropped: u64,
    /// data plane 因 producer 容量不足被拒的累计数。
    pub data_full: u64,
    /// data plane 因 producer 已关闭被拒的累计数。
    pub data_closed: u64,
    /// data plane 因本地路由或 payload 非法被拒的累计数。
    pub data_rejected: u64,
}

/// 跨节点集群编排核心。
///
/// `Cluster` 只负责编码/解码、回环过滤、目标群过滤、本地分发和 presence 对账;
/// 具体广播介质由 [`Notifier`] 注入。这样 Redis Stream、内存实现或其它 transport
/// 都遵守同一套 at-most-once 与 graceful shutdown 边界。
pub struct Cluster {
    local_node: String,
    notifier: Arc<dyn Notifier>,
    /// 收到对端消息后的本地分发(= Sender::send_local,不再二次 publish,防 ping-pong)。
    on_local: Arc<dyn Fn(Message, Mode) + Send + Sync>,
    /// 对端节点存活表 + 群成员目录(数据事件刷新存活、presence 刷新存活+群)。
    nodes: Arc<NodeRegistry>,
    /// 本节点全量 presence 快照提供者(取 (版本,本地群));未注入则广播空快照(仅当心跳)。
    presence: Mutex<Option<PresenceProvider>>,
    /// 关闭标志:停周期 presence 任务(+ 转发给 notifier 停其收发任务)。
    shutdown: Arc<AtomicBool>,
    /// 本进程 boot id:同 node_id 重启会变 → 接收端据此重置旧目录,不被旧序号拒绝。
    incarnation: String,
    /// 取消信号:周期 presence 任务 select 它,shutdown 时立即退出(不必等满一个 tick)。
    cancel: CancellationToken,
    /// 集群自有后台任务跟踪器(presence);graceful 时 close+wait 真正 join。
    tasks: TaskTracker,
    /// 可选 data plane 少拷贝 publisher；presence 永远不使用它。
    data_publisher: std::sync::OnceLock<Arc<dyn ClusterDataPublisher>>,
    /// 数据事件 publish 被 transport 拒(Full/Closed)的累计数;warn 按 2 的幂限频。
    publish_dropped: std::sync::atomic::AtomicU64,
    /// presence publish 被拒的累计数(原先完全静默;周期 FULL 自愈,但需可观测;R20 P3)。
    presence_dropped: std::sync::atomic::AtomicU64,
    /// data producer 容量拒绝计数。
    data_full: std::sync::atomic::AtomicU64,
    /// data producer 关闭拒绝计数。
    data_closed: std::sync::atomic::AtomicU64,
    /// data 本地确定拒绝计数。
    data_rejected: std::sync::atomic::AtomicU64,
}

impl Cluster {
    /// 用**注入的、已校验的** `Incarnation` 构造。`Incarnation` 保证非空 + 纯 ASCII 十进制 + 正整数
    /// (空串/非正会破坏 fencing,已在类型层挡住;R12 P1)。同 node_id 须用**同一定宽方案**
    /// (`Incarnation::from_epoch` 恒 20 位)使字典序 == 数值序。
    ///
    /// # 参数
    /// - `local_node`: 本节点稳定 ID,用于防回环、presence 目录和 fencing 分组。
    /// - `notifier`: 跨节点广播 transport,负责真正收发 ClusterEvent 字节。
    /// - `on_local`: 接收对端消息后执行的本地投递回调,通常是 `Sender::send_local`。
    /// - `incarnation`: 本进程 boot id,用于同 node_id 重启后的新旧实例围栏。
    pub fn with_incarnation(
        local_node: impl Into<String>,
        notifier: Arc<dyn Notifier>,
        on_local: Arc<dyn Fn(Message, Mode) + Send + Sync>,
        incarnation: Incarnation,
    ) -> Arc<Cluster> {
        Arc::new(Cluster {
            local_node: local_node.into(),
            notifier,
            on_local,
            nodes: NodeRegistry::new(DEFAULT_NODE_TTL),
            presence: Mutex::new(None),
            shutdown: Arc::new(AtomicBool::new(false)),
            incarnation: incarnation.into_string(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
            data_publisher: std::sync::OnceLock::new(),
            publish_dropped: std::sync::atomic::AtomicU64::new(0),
            presence_dropped: std::sync::atomic::AtomicU64::new(0),
            data_full: std::sync::atomic::AtomicU64::new(0),
            data_closed: std::sync::atomic::AtomicU64::new(0),
            data_rejected: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// 在启动前一次性注入 data plane publisher。
    ///
    /// # 参数
    ///
    /// - `publisher`: 绑定固定 data lane/topic 的发布器。
    ///
    /// # 错误
    ///
    /// 重复注入时返还传入的 publisher；已安装实例保持不变。
    pub fn set_data_publisher(
        &self,
        publisher: Arc<dyn ClusterDataPublisher>,
    ) -> Result<(), Arc<dyn ClusterDataPublisher>> {
        self.data_publisher.set(publisher)
    }

    /// 注入全量 presence 快照提供者(框架装配时从 SessionRegistry::presence_snapshot 取)。
    ///
    /// # 参数
    /// - `f`: 返回 `(版本, 本地群集合)` 的快照闭包,两者必须来自同一时刻。
    pub fn set_presence_provider(&self, f: PresenceProvider) {
        *self.presence.lock().unwrap() = Some(f);
    }

    /// 返回本节点标识；用于过滤或标记集群消息来源。
    pub fn local_node(&self) -> &str {
        &self.local_node
    }

    /// 对端节点存活表(可观测 / 精确路由前置)。
    pub fn nodes(&self) -> &Arc<NodeRegistry> {
        &self.nodes
    }

    /// 集群规模快照(只读 DTO)。`tombstone_count` 的持续增长说明 node_id 不稳定,见
    /// [`NodeRegistry::tombstone_count`](经标准 ServerBuilder 集成路径可达)。
    /// 三个节点计数在 NodeRegistry **单锁内一次**算出,不再三次取锁拼出跨时刻 DTO。
    pub fn stats(&self) -> ClusterStats {
        let (node_count, alive_count, tombstone_count) = self.nodes.stats(now_millis());
        ClusterStats {
            node_count,
            alive_count,
            tombstone_count,
            publish_dropped: self.publish_dropped.load(Ordering::Relaxed),
            presence_dropped: self.presence_dropped.load(Ordering::Relaxed),
            data_full: self.data_full.load(Ordering::Relaxed),
            data_closed: self.data_closed.load(Ordering::Relaxed),
            data_rejected: self.data_rejected.load(Ordering::Relaxed),
        }
    }

    /// 启动接收循环;**等就绪后返回**(reader 连上并定好起始游标),消除节点启动丢消息窗口。
    /// 返回 Result:就绪 Ok;失败/超时 Err(由调用方按 FailFast/Degraded 处置)。
    ///
    pub async fn start(self: &Arc<Self>) -> Result<(), StartError> {
        self.start_with_timeout(Duration::from_secs(5)).await
    }

    /// 启动接收循环并使用调用方提供的就绪超时。
    ///
    /// # 参数
    ///
    /// - `ready_timeout`: transport 完成认证、游标和 assignment 的总等待时间。
    pub async fn start_with_timeout(
        self: &Arc<Self>,
        ready_timeout: Duration,
    ) -> Result<(), StartError> {
        let ready_timeout = ready_timeout.min(MAX_CLUSTER_RUNTIME_DURATION);
        let me = Arc::clone(self);
        let ready = self
            .notifier
            .start(Arc::new(move |bytes| me.on_received(bytes)));
        match tokio::time::timeout(ready_timeout, ready).await {
            Ok(r) => r,
            Err(_) => Err(StartError::NotReady(format!(
                "notifier not ready within {}ms",
                ready_timeout.as_millis()
            ))),
        }
    }

    /// 关闭集群通知器；用于释放节点广播相关资源。
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed); // 停周期 presence(标志兜底)
        self.cancel.cancel(); // presence 任务立即退出(不必等满一个 tick)
        self.notifier.shutdown(); // 停 notifier 收发(RedisNotifier 等)
    }

    /// graceful:cancel 后在**绝对截止点 `end`** 之前 join 集群自有任务(presence)+ notifier
    /// 后台任务(Redis reader/publisher)。各阶段共用同一截止点(不是各给一份完整 deadline),
    /// 故总耗时受 `end` 约束。返回是否在 `end` 前全部退出。
    ///
    /// # 参数
    /// - `end`: 优雅关闭的绝对截止时间,所有 join 阶段共同受它约束。
    pub async fn shutdown_join(&self, end: Instant) -> bool {
        self.shutdown();
        self.tasks.close();
        let mut ok = tokio::time::timeout_at(end, self.tasks.wait())
            .await
            .is_ok();
        if let Some(tracker) = self.notifier.task_tracker() {
            tracker.close();
            ok &= tokio::time::timeout_at(end, tracker.wait()).await.is_ok();
        }
        ok
    }

    /// 本地 Sender.send/relay 调:把一条 Message 跨节点投递——**一律广播**,
    /// 由接收端按本地 by_group/uid 过滤(send_local)。
    /// 不做"查群目录→只发匹配节点"的精确群播:presence 是 at-most-once,某节点 presence 丢失时
    /// 目录非空但不完整,精确路由会漏投该节点直到下次 FULL——IM 不可漏。
    /// 群目录(`nodes()`)仍维护,供可观测;真要省带宽的精确群播需"所有存活节点都提交过当前
    /// FULL"的 readiness 才安全,留作后续优化。点对点精确投递用 `publish_to`(调用方自负目标)。
    ///
    /// # 参数
    /// - `msg`: 要跨节点转发的业务消息信封。
    /// - `mode`: `msg` 的编码模式,会写入 ClusterEvent 并供接收端解码。
    pub fn publish(&self, msg: &Message, mode: Mode) {
        self.publish_inner(msg, mode, None);
    }

    /// **精确路由**:只投给指定节点(接收端按 target_nodes 过滤,其余节点丢弃)。
    ///
    /// # 参数
    /// - `nodes`: 目标节点 ID 列表,接收端只在本节点命中时投递。
    /// - `msg`: 要跨节点转发的业务消息信封。
    /// - `mode`: `msg` 的编码模式,会写入 ClusterEvent 并供接收端解码。
    pub fn publish_to(&self, nodes: &[&str], msg: &Message, mode: Mode) {
        let targets = nodes.iter().map(|n| Some(n.to_string())).collect();
        self.publish_inner(msg, mode, Some(targets));
    }

    /// 发布内部集群消息；用于按广播模式选择目标节点。
    ///
    /// # 参数
    /// - `msg`: 业务消息体或事件载荷。
    /// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
    /// - `target_nodes`: 需要发送集群消息的目标节点列表。
    fn publish_inner(&self, msg: &Message, mode: Mode, target_nodes: Option<Vec<Option<String>>>) {
        if let Some(publisher) = self.data_publisher.get() {
            let Some(incarnation) = Incarnation::parse_value(&self.incarnation) else {
                self.data_rejected.fetch_add(1, Ordering::Relaxed);
                tracing::error!("cluster incarnation 在构造后变为非法，data 发布已拒绝");
                return;
            };
            let targets: Option<Vec<&str>> = target_nodes.as_ref().map(|items| {
                items
                    .iter()
                    .filter_map(Option::as_deref)
                    .collect::<Vec<_>>()
            });
            let outcome = publisher.publish(
                ClusterSourceRef {
                    node: &self.local_node,
                    incarnation,
                },
                msg,
                mode,
                targets.as_deref(),
            );
            match outcome {
                DataPublishOutcome::Queued => {}
                DataPublishOutcome::Full => {
                    self.data_full.fetch_add(1, Ordering::Relaxed);
                }
                DataPublishOutcome::Closed => {
                    self.data_closed.fetch_add(1, Ordering::Relaxed);
                }
                DataPublishOutcome::Rejected => {
                    self.data_rejected.fetch_add(1, Ordering::Relaxed);
                }
            }
            return;
        }
        let message_bytes = match msg.encode(mode) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("cluster publish encode failed: {e}");
                return;
            }
        };
        let ev = ClusterEvent {
            source_node: Some(self.local_node.clone()),
            timestamp: now_millis(),
            trace_id: None,
            message_mode: mode.ordinal() as i8,
            message_bytes: Some(message_bytes),
            target_nodes, // None = 全广播;Some = 仅这些节点处理
            source_incarnation: Some(self.incarnation.clone()), // 数据事件也带,接收端统一围栏
        };
        let bytes = ev.encode(Mode::BitpackTlv).expect("ClusterEvent encode");
        if self.notifier.publish(Bytes::from(bytes)) != PublishOutcome::Queued {
            // 限频 warn(1,2,4,8,...):Redis 故障期持续丢弃不刷日志。
            let n = self.publish_dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_power_of_two() {
                tracing::warn!("cluster data publish dropped (transport full/closed), total {n}");
            }
        }
    }

    /// 全量 presence(周期对账):广播本节点**当前全部**本地群 + 该快照的一致版本。
    /// 接收端按 (node,group) LWW 整体对齐——比本群已知版本更新才覆盖,绝不回退更新的 delta;
    /// 缺席群在 ≤version 时下沉为 tombstone 并被水位回收。丢一条不致命,下个周期全量自愈。
    pub fn publish_presence(&self) {
        // 无 provider → 空快照(纯心跳,version=0)。provider 一次 lock+clone 即得一致快照,
        // 有界完成,不会被高 churn 饿死。
        let (version, groups) = match self.presence.lock().unwrap().as_ref() {
            None => (0, Vec::new()),
            Some(f) => f(),
        };
        self.publish_presence_event(PRESENCE_FULL, &groups, version);
    }

    /// 增量 presence(群 0↔1 跃迁即时广播):present=true 发 ADD、false 发 DEL,**单群单版本**
    /// (一条事件一个 timestamp,故一次只携带一个群)。version 为跃迁处取的号。
    /// 让对端目录在网络延迟级内更新,避免"刚加入群却收不到跨节点消息"的窗口。
    ///
    /// # 参数
    /// - `group`: 本节点发生 0↔1 成员跃迁的群组名。
    /// - `present`: `true` 表示本节点开始拥有该群成员,`false` 表示不再拥有。
    /// - `version`: 跃迁时生成的 presence 版本号,接收端用它做 LWW 去旧。
    pub fn publish_presence_delta(&self, group: &str, present: bool, version: i64) {
        let groups = [group.to_string()];
        self.publish_presence_event(
            if present { PRESENCE_ADD } else { PRESENCE_DEL },
            &groups,
            version,
        );
    }

    /// 组装并广播一条 presence(message_bytes=None;message_mode=kind;target_nodes=群名)。
    /// timestamp 复用为**该事件版本**(FULL=快照版本,delta=跃迁号):接收端据此做 LWW 去旧。
    ///
    /// # 参数
    /// - `kind`: 集群同步事件的类型标识。
    /// - `groups`: 本次同步涉及的业务分组集合。
    /// - `version`: presence、配置或协议版本号。
    fn publish_presence_event(&self, kind: i8, groups: &[String], version: i64) {
        let target_nodes = if groups.is_empty() {
            None
        } else {
            Some(groups.iter().cloned().map(Some).collect())
        };
        let ev = ClusterEvent {
            source_node: Some(self.local_node.clone()),
            timestamp: version, // presence:事件版本,非时间戳
            trace_id: None,
            message_mode: kind, // presence:复用为类型(FULL/ADD/DEL)
            message_bytes: None,
            target_nodes,
            source_incarnation: Some(self.incarnation.clone()), // 围栏 token(专用字段)
        };
        let bytes = ev.encode(Mode::BitpackTlv).expect("ClusterEvent encode");
        if self.notifier.publish(Bytes::from(bytes)) != PublishOutcome::Queued {
            // presence 丢弃:周期 FULL 自愈,但不再完全静默——计数 + 限频 warn。
            let n = self.presence_dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_power_of_two() {
                tracing::warn!(
                    "cluster presence publish dropped (transport full/closed), total {n}"
                );
            }
        }
    }

    /// 启动周期全量 presence 对账 + 过期节点清理(需在 tokio 运行时内)。
    ///
    ///
    /// # 参数
    /// - `interval`: 后台轮询、心跳或重试任务的执行间隔。
    pub fn start_presence(self: &Arc<Self>, interval: Duration) {
        let interval = bounded_period(interval);
        let me = Arc::clone(self);
        let cancel = self.cancel.clone();
        self.tasks.spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // 跳过立即触发的首拍
            while !me.shutdown.load(Ordering::Relaxed) {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break, // graceful 立即退出,不等满一拍
                    _ = tick.tick() => {
                        me.publish_presence();
                        me.nodes.prune(now_millis());
                    }
                }
            }
        });
    }

    /// Notifier 收到对端 payload(也会收到自己发的)→ 防回环 + 目标过滤 + 本地分发。
    ///
    /// # 参数
    /// - `bytes`: 原始字节切片。
    fn on_received(&self, bytes: Bytes) {
        let ev = match ClusterEvent::decode(Mode::BitpackTlv, &bytes) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("cluster decode failed: {e}");
                return;
            }
        };
        // 防回环:自己发的丢弃。
        if ev.source_node.as_deref() == Some(self.local_node.as_str()) {
            return;
        }
        let src = match &ev.source_node {
            Some(s) => s.clone(),
            None => return,
        };

        // 统一**解析并校验** source_incarnation 为数值(非空 + 纯 ASCII 十进制 + ≤u128);
        // 非法/缺失一律拒绝,不续命、不更新目录、不投递(不 fail-open;数值比较避免变长错序,
        //R9 P1 / R14 P1)。
        let inc = match ev
            .source_incarnation
            .as_deref()
            .and_then(Incarnation::parse_value)
        {
            Some(v) => v,
            None => {
                tracing::warn!(
                    "cluster: event from {src} with missing/invalid source_incarnation; rejected"
                );
                return;
            }
        };

        // 存活按**本机**接收时间记;远端 ev.timestamp 仅用于 presence 乱序判定。
        let now_local = now_millis();
        // presence(message_bytes=None):message_mode 为 presence 类型,target_nodes 为群名。
        //   刷新存活 + 按类型更新群目录(旧事件忽略)后即返回(非数据事件,不分发)。
        if ev.message_bytes.is_none() {
            let groups: HashSet<String> = ev
                .target_nodes
                .iter()
                .flatten()
                .flatten()
                .cloned()
                .collect();
            let ver = ev.timestamp; // presence 的 timestamp = 事件版本
            match ev.message_mode {
                PRESENCE_FULL => self.nodes.apply_full(&src, groups, inc, ver, now_local),
                // delta 为单群单版本(约束 C);防御性地对每个群套用同一版本(发布端只发一个)。
                PRESENCE_ADD => {
                    for g in &groups {
                        self.nodes.apply_delta(&src, g, true, inc, ver, now_local);
                    }
                }
                PRESENCE_DEL => {
                    for g in &groups {
                        self.nodes.apply_delta(&src, g, false, inc, ver, now_local);
                    }
                }
                // 未知 presence 类型:拒绝,绝不当 FULL 替换目录。
                other => {
                    tracing::warn!("cluster: unknown presence kind {other} from {src}; ignored")
                }
            }
            return;
        }

        // 数据事件:**先 incarnation 围栏**——旧实例(更小 incarnation)的迟到数据不得续命、
        // 不得投递。围栏通过才刷新存活 + 分发。
        if !self.nodes.touch_fenced(&src, inc, now_local) {
            return;
        }
        // 目标过滤:`None` = 广播(全收);`Some(list)` = 显式路由列表,不含本节点即丢。
        //
        // 这里刻意**不再**要求"列表非空才过滤"：`publish_to(&[])` 会编成 `Some(vec![])`，
        // 而空列表若被当成广播，"发给零个节点"就变成了"发给所有节点"——路由原语失败开放。
        // 全空/全空串的列表同理（空串在 wire 上编成零长元素、解回 None）。
        if let Some(targets) = &ev.target_nodes {
            if !targets.iter().flatten().any(|t| t == &self.local_node) {
                return;
            }
        }
        let Some(payload) = ev.message_bytes else {
            return;
        };
        // 用 ClusterEvent 携带的 mode 解内层 Message(不猜 BITPACK)。
        let Some(mode) = Mode::from_ordinal(ev.message_mode as u8) else {
            tracing::warn!("cluster: bad message_mode {}", ev.message_mode);
            return;
        };
        let msg = match Message::decode(mode, &payload) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("cluster inner message decode failed: {e}");
                return;
            }
        };
        (self.on_local)(msg, mode);
    }
}

/// 读取当前毫秒时间；用于节点心跳和过期判断。
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `Incarnation` 解析/构造错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncarnationError {
    /// 空串或含非 ASCII 数字字符。
    NotDecimal,
    /// 数值非正(0 或负)——会破坏定宽字典序的 fencing 语义。
    NonPositive,
}

impl std::fmt::Display for IncarnationError {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncarnationError::NotDecimal => {
                write!(f, "incarnation must be non-empty ASCII decimal")
            }
            IncarnationError::NonPositive => write!(f, "incarnation must be a positive integer"),
        }
    }
}
impl std::error::Error for IncarnationError {}

/// **已校验的 incarnation fencing token**:保证非空、纯 ASCII 十进制、表示**正整数**。
/// 接收端按字符串字典序比较同 node_id 的实例(更大=更新),故同 node_id 必须使用**同一定宽方案**
/// ——`from_epoch` 恒 20 位零填充,使字典序 == 数值序。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incarnation(String);

impl Incarnation {
    /// 从持久单调 epoch(如 Redis `INCR`)构造定宽(20 位零填充)token。epoch 须 > 0。
    ///
    /// # 参数
    /// - `epoch`: 外部持久单调递增序号,必须大于 0。
    pub fn from_epoch(epoch: i64) -> Result<Incarnation, IncarnationError> {
        if epoch <= 0 {
            return Err(IncarnationError::NonPositive);
        }
        Ok(Incarnation(format!("{epoch:020}")))
    }

    /// 从已有字符串解析(再水合):必须非空、全 ASCII 数字、表示正整数(去前导零后非空)、且 ≤ u128。
    ///
    /// # 参数
    /// - `s`: 持久化或配置中读取到的 incarnation token 字符串。
    pub fn parse(s: impl Into<String>) -> Result<Incarnation, IncarnationError> {
        let s = s.into();
        match Self::parse_value(&s) {
            Some(_) => Ok(Incarnation(s)),
            None => Err(if s.bytes().all(|b| b.is_ascii_digit()) && !s.is_empty() {
                IncarnationError::NonPositive
            } else {
                IncarnationError::NotDecimal
            }),
        }
    }

    /// 解析 incarnation 字符串为**数值**(供 fence **按数值比较**,避免变长字典序错序,R14 P1)。
    /// 要求:非空、全 ASCII 十进制、数值 > 0、且不溢出 u128。否则返回 None(接收端据此拒绝)。
    ///
    /// # 参数
    /// - `s`: 待验证和转换的 ASCII 十进制 incarnation 字符串。
    pub fn parse_value(s: &str) -> Option<u128> {
        // 长度上限按既有的 `{epoch:020}` 契约（from_epoch 恒定产出 20 位）收紧。
        //
        // 不收紧的话，一条携带 39 位巨值 incarnation 的事件会把该 node id 的围栏永久顶到天花板：
        // 此后真实节点的所有事件都因 incarnation 更小被静默拒绝，而 tombstone 按设计永不过期，
        // 恢复要重启**所有对端**进程。文档里那条"固定 20 位"的约定此前只在构造侧成立，
        // 接收侧从不校验——这里把它变成真正的不变量。
        const MAX_INCARNATION_DIGITS: usize = 20;
        if s.is_empty()
            || s.len() > MAX_INCARNATION_DIGITS
            || !s.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        match s.parse::<u128>() {
            Ok(v) if v > 0 => Some(v),
            _ => None,
        }
    }

    /// 返回节点标识字符串；用于构造消息和日志输出。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 消费节点标识并返回字符串；用于把强类型节点 ID 交给存储层。
    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

// ════════════════════════════════════════════════════════════════════════════
// NodeRegistry —— 集群节点存活表 + 群成员目录。node_id → (last_seen ms, 该节点拥有
// 本地成员的 group 集合)。存活 = (now - last_seen) ≤ ttl。
// 靠数据事件刷新 last_seen、靠 presence 刷新 last_seen + groups(精确群播的目录来源)。
// 时间用显式 `at` 传入,避免内部时钟耦合。
// ════════════════════════════════════════════════════════════════════════════
/// 单个群在某节点的 presence 槽:per-(node,group) LWW 的最小单元。
struct GroupSlot {
    /// 最近一次令该群状态变化的事件版本(FULL 快照版本 / delta 跃迁号)。
    version: i64,
    /// 该群当前在/不在:false 为 tombstone(保留到 version ≤ full_watermark 时被全量回收)。
    present: bool,
}

/// 保存集群节点心跳信息；用于判断节点是否仍可接收广播。
struct NodeInfo {
    /// 本机接收时间(ms):仅用于存活 TTL,避免远端时钟漂移导致永不过期/立即过期。
    last_seen_local: i64,
    /// 远端 incarnation(boot id)的**数值**:同 node_id 重启会变(更大)→ 重置该节点目录。
    /// **按数值比较**(非字符串字典序),避免 "9" > "10" 的变长错序。
    incarnation: Option<u128>,
    /// 最近一次成功应用的 FULL 的版本(全量基线水位):delta 须 version > full_watermark 才被接受
    /// ——否则属于"早于上次全量基线"的陈旧增量,拒绝;tombstone 也以此为回收线。
    full_watermark: i64,
    /// 每群独立 (version, present):per-(node,group) LWW。不同群的乱序 delta 互不影响(约束 C)。
    groups: HashMap<String, GroupSlot>,
    /// 是否已被 prune 标记为过期 tombstone(只清了目录、保留 incarnation+水位)。仅"首次进入过期"
    /// 计数 + 清目录,避免每次 prune 重复计数/重复清;收到新事件刷新存活后复位。
    expired: bool,
}

impl NodeInfo {
    /// 创建新的节点心跳记录；用于节点首次出现或刷新时初始化时间。
    ///
    /// # 参数
    /// - `now_local`: 用于时间窗口判断的当前本地时间。
    fn fresh(now_local: i64) -> NodeInfo {
        NodeInfo {
            last_seen_local: now_local,
            incarnation: None,
            full_watermark: i64::MIN,
            groups: HashMap::new(),
            expired: false,
        }
    }

    /// incarnation 围栏(有序 fencing token):
    /// - **更新**(更大)→ 对端重启了新实例:重置目录与水位,纳入新实例,返回 true;
    /// - **相同** → 同实例,放行(版本去旧交给 per-group / 水位);
    /// - **更旧**(更小)→ 迟到的旧实例消息,拒绝(不让目录回切)。
    ///
    /// 返回 true 表示该事件应继续按版本规则应用。
    ///
    /// # 参数
    /// - `incarnation`: 节点成员关系的版本号,用于过滤过期状态。
    fn fence(&mut self, incarnation: u128) -> bool {
        match self.incarnation {
            Some(cur) if incarnation == cur => true,
            Some(cur) if incarnation > cur => {
                self.reset_to(incarnation);
                true
            }
            Some(_) => false, // 更旧的 incarnation → 迟到的旧实例,拒绝
            None => {
                self.reset_to(incarnation);
                true
            }
        }
    }

    /// 切到新 incarnation:清空群槽,水位归 MIN(新实例的 presence_seq 从 0 起,
    /// 不能被旧实例的高水位拒掉)。
    ///
    /// # 参数
    /// - `incarnation`: 节点成员关系的版本号,用于过滤过期状态。
    fn reset_to(&mut self, incarnation: u128) {
        self.incarnation = Some(incarnation);
        self.full_watermark = i64::MIN;
        self.groups.clear();
    }

    /// 应用一条全量快照 @version:在 snapshot 内的群置 present、缺席群下沉 tombstone
    /// (均不回退更新的 delta:仅当 version ≥ 槽版本才动),推进水位并回收 ≤水位 的 tombstone。
    ///
    /// # 参数
    /// - `snapshot`: presence 或服务实例快照。
    /// - `version`: presence、配置或协议版本号。
    fn apply_full(&mut self, snapshot: &HashSet<String>, version: i64) {
        if version < self.full_watermark {
            return; // 比已应用的全量更旧 → 忽略
        }
        // 在快照内的群:置 present@version(不回退版本更高的 delta)。
        for g in snapshot {
            let slot = self.groups.entry(g.clone()).or_insert(GroupSlot {
                version: i64::MIN,
                present: false,
            });
            if version >= slot.version {
                slot.version = version;
                slot.present = true;
            }
        }
        // 缺席群:若槽不比 version 新 → tombstone@version(随后被水位回收)。
        for (g, slot) in self.groups.iter_mut() {
            if !snapshot.contains(g) && version >= slot.version {
                slot.version = version;
                slot.present = false;
            }
        }
        self.full_watermark = self.full_watermark.max(version);
        // 回收 version ≤ 水位 的 tombstone:被本次全量基线覆盖,陈旧 ADD(≤水位)会被水位拒,
        // 不会复活;version > 水位 的 tombstone(更新的 DEL delta)保留到后续 FULL(约束 B)。
        let wm = self.full_watermark;
        self.groups.retain(|_, s| s.present || s.version > wm);
    }

    /// 应用一条单群 delta @version:水位门控(陈旧增量拒) + per-group LWW(仅更新才覆盖)。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    /// - `present`: 当前群组或实例是否存在。
    /// - `version`: presence、配置或协议版本号。
    fn apply_delta(&mut self, group: &str, present: bool, version: i64) {
        if version <= self.full_watermark {
            return; // 早于上次全量基线 → 陈旧增量,拒(约束 B:防 tombstone 被陈旧 ADD 复活)
        }
        let slot = self.groups.entry(group.to_string()).or_insert(GroupSlot {
            version: i64::MIN,
            present: false,
        });
        if version > slot.version {
            slot.version = version;
            slot.present = present;
        }
    }

    /// 该节点当前是否拥有 group 的本地成员(present 槽)。
    ///
    /// # 参数
    /// - `group`: 消费组、服务分组或任务分组名称。
    fn has_group(&self, group: &str) -> bool {
        self.groups.get(group).is_some_and(|s| s.present)
    }
}

/// 维护集群节点表；用于记录活跃节点并清理过期节点。
pub struct NodeRegistry {
    ttl_millis: i64,
    nodes: Mutex<HashMap<String, NodeInfo>>,
}

impl NodeRegistry {
    /// 构造节点注册表。
    ///
    /// # 参数
    /// - `ttl`: 节点存活 TTL,超过该时间未收到事件或 presence 即视为过期。
    pub fn new(ttl: Duration) -> Arc<NodeRegistry> {
        let ttl_millis = ttl
            .min(MAX_CLUSTER_RUNTIME_DURATION)
            .as_millis()
            .try_into()
            .expect("bounded cluster TTL must fit i64 milliseconds");
        Arc::new(NodeRegistry {
            ttl_millis,
            nodes: Mutex::new(HashMap::new()),
        })
    }

    /// 数据事件:**先 incarnation 围栏再刷新存活**(用本机时间 `now_local`),不动 groups。
    /// 数据事件:**先 incarnation 围栏再刷新存活**。`incarnation` 必须**非空**(调用方 on_received
    /// 已强制,缺失/空一律在更上层拒绝;不 fail-open,R9 P1)。返回是否接受(被拒=旧实例
    /// 迟到数据,调用方应丢弃、不分发)。
    ///
    /// # 参数
    /// - `node`: 发送数据事件的对端节点 ID。
    /// - `incarnation`: 发送方 boot id 的数值形态,用于拒绝旧实例迟到消息。
    /// - `now_local`: 本机收到事件时的毫秒时间,用于刷新 TTL。
    pub fn touch_fenced(&self, node: &str, incarnation: u128, now_local: i64) -> bool {
        let mut m = self.nodes.lock().unwrap();
        let info = m
            .entry(node.to_string())
            .or_insert_with(|| NodeInfo::fresh(now_local));
        if info.fence(incarnation) {
            info.last_seen_local = now_local;
            info.expired = false; // 收到新事件 → 复活(不再是 tombstone)
            true
        } else {
            false // 更旧 incarnation → 拒绝(不续命、不分发)
        }
    }

    /// 全量 presence:刷新存活;incarnation 围栏通过后按 (node,group) LWW 对齐快照。
    ///
    /// # 参数
    /// - `node`: 发送 FULL presence 的对端节点 ID。
    /// - `groups`: 该节点当前拥有本地成员的群组全集。
    /// - `incarnation`: 发送方 boot id 的数值形态。
    /// - `version`: FULL 快照版本,用于 LWW 去旧。
    /// - `now_local`: 本机收到 presence 时的毫秒时间,用于刷新 TTL。
    pub fn apply_full(
        &self,
        node: &str,
        groups: HashSet<String>,
        incarnation: u128,
        version: i64,
        now_local: i64,
    ) {
        let mut m = self.nodes.lock().unwrap();
        let info = m
            .entry(node.to_string())
            .or_insert_with(|| NodeInfo::fresh(now_local));
        // **先围栏再刷新存活**:被拒的旧实例(更小 incarnation)不得给当前新实例续命,
        // 否则旧实例残留目录长期不过期。新建条目 incarnation=None,fence 必接受。
        if info.fence(incarnation) {
            info.last_seen_local = now_local;
            info.expired = false;
            info.apply_full(&groups, version);
        }
    }

    /// 增量 presence(单群):刷新存活;incarnation 围栏通过后按水位门控 + per-group LWW 应用。
    ///
    /// # 参数
    /// - `node`: 发送 delta presence 的对端节点 ID。
    /// - `group`: 发生成员 0↔1 跃迁的群组名。
    /// - `present`: `true` 表示节点拥有该群成员,`false` 表示不再拥有。
    /// - `incarnation`: 发送方 boot id 的数值形态。
    /// - `version`: delta 事件版本,用于水位门控和 per-group LWW。
    /// - `now_local`: 本机收到 presence 时的毫秒时间,用于刷新 TTL。
    pub fn apply_delta(
        &self,
        node: &str,
        group: &str,
        present: bool,
        incarnation: u128,
        version: i64,
        now_local: i64,
    ) {
        let mut m = self.nodes.lock().unwrap();
        let info = m
            .entry(node.to_string())
            .or_insert_with(|| NodeInfo::fresh(now_local));
        // 先围栏再刷新存活(同 apply_full;R6 P1)。
        if info.fence(incarnation) {
            info.last_seen_local = now_local;
            info.expired = false;
            info.apply_delta(group, present, version);
        }
    }

    /// now_local 时刻仍存活的节点列表(按本机时钟判 TTL)。
    ///
    /// # 参数
    /// - `now_local`: 本机当前毫秒时间,用于和节点 last_seen 比较 TTL。
    pub fn alive_nodes(&self, now_local: i64) -> Vec<String> {
        let m = self.nodes.lock().unwrap();
        m.iter()
            .filter(|(_, i)| within_ttl(now_local, i.last_seen_local, self.ttl_millis))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// 返回当前对象的 alive 状态。
    ///
    /// # 参数
    /// - `node`: 要检查的节点 ID。
    /// - `now_local`: 本机当前毫秒时间,用于和节点 last_seen 比较 TTL。
    pub fn is_alive(&self, node: &str, now_local: i64) -> bool {
        self.nodes
            .lock()
            .unwrap()
            .get(node)
            .is_some_and(|i| within_ttl(now_local, i.last_seen_local, self.ttl_millis))
    }

    /// 统计指定时间点仍活跃的节点；用于观测集群可用规模。
    ///
    /// # 参数
    /// - `at`: 本机毫秒时间,用于判断每个节点是否仍在 TTL 内。
    pub fn alive_count(&self, at: i64) -> usize {
        self.alive_nodes(at).len()
    }

    /// 节点条目总数(含已过期的 incarnation tombstone)。可观测/容量监控用。
    pub fn node_count(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }

    /// 当前 **tombstone 数**(已过期但为 fencing 保留 incarnation 的节点)。**部署须监控其增长**:
    /// 永久 tombstone 是 fencing 正确性的取舍,要求 node_id 稳定且有界(StatefulSet ordinal 等);
    /// 若该值随时间无界上升,说明用了每次启动都变的随机 node_id,需改用稳定 ID 或外部持久 epoch
    ///。
    ///
    /// # 参数
    /// - `now_local`: 本机当前毫秒时间,用于判断节点是否已经超过 TTL。
    pub fn tombstone_count(&self, now_local: i64) -> usize {
        let m = self.nodes.lock().unwrap();
        m.values()
            .filter(|i| !within_ttl(now_local, i.last_seen_local, self.ttl_millis))
            .count()
    }

    /// (节点总数, 存活数, tombstone 数) **单锁内一次**算出——三值同一时刻、口径一致,
    /// 不再让调用方三次取锁拼出跨时刻快照。
    ///
    /// # 参数
    /// - `now_local`: 本机当前毫秒时间,用于同一口径计算 alive 与 tombstone。
    pub fn stats(&self, now_local: i64) -> (usize, usize, usize) {
        let m = self.nodes.lock().unwrap();
        let node_count = m.len();
        let alive_count = m
            .values()
            .filter(|i| within_ttl(now_local, i.last_seen_local, self.ttl_millis))
            .count();
        (node_count, alive_count, node_count - alive_count)
    }

    /// now_local 时刻"拥有该 group 本地成员"的存活节点(精确群播的目标集)。
    ///
    /// # 参数
    /// - `group`: 要查询的群组名。
    /// - `now_local`: 本机当前毫秒时间,用于排除 TTL 外节点。
    pub fn nodes_for_group(&self, group: &str, now_local: i64) -> Vec<String> {
        let m = self.nodes.lock().unwrap();
        m.iter()
            .filter(|(_, i)| {
                within_ttl(now_local, i.last_seen_local, self.ttl_millis) && i.has_group(group)
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// 处理 now_local 时刻已过期的节点;返回**本次新过期**的节点数(已是 tombstone 的不重复计)。
    /// 过期节点**降级为永久 tombstone**:首次过期才计数 + 清出群目录,且 `full_watermark` 抬到
    /// `max(watermark, 所有 GroupSlot.version)`——既挡更旧 incarnation 回切,也挡**同一 incarnation**
    /// 的旧 FULL(<水位)/旧 delta(≤水位)复活目录。同存活进程后续发当前版本
    /// FULL 仍可重建。
    ///
    /// # 参数
    /// - `now_local`: 本机当前毫秒时间,用于判断哪些节点超过 TTL。
    ///
    /// **incarnation 围栏永不按时间回收**:按时删 tombstone 会让暂停/分区数小时后
    /// 恢复的旧实例被当作全新节点重新接管目录。代价是节点条目数 = **不同 node_id 数**——这要求
    /// 部署用**稳定且有界**的 node_id(如 StatefulSet ordinal)。若用每次启动都变的随机 Pod ID
    /// (本就不复用、fencing 无意义),需由部署侧用外部持久 `node_id→max_incarnation` 策略回收,
    /// 不在核心库内按猜测的 TTL 删除(防"删了围栏→旧实例复活")。
    pub fn prune(&self, now_local: i64) -> usize {
        let mut m = self.nodes.lock().unwrap();
        let mut newly_expired = 0;
        for info in m.values_mut() {
            if within_ttl(now_local, info.last_seen_local, self.ttl_millis) {
                info.expired = false; // 仍存活(收到新事件时也会复位,这里兜底)
                continue;
            }
            if !info.expired {
                info.expired = true;
                newly_expired += 1;
                // 抬高水位到已见最大版本,再清目录(顺序不可换)。
                let retired = info
                    .groups
                    .values()
                    .map(|s| s.version)
                    .max()
                    .unwrap_or(i64::MIN)
                    .max(info.full_watermark);
                info.full_watermark = retired;
                info.groups.clear();
            }
            // 永久保留 incarnation + 水位 tombstone(不删条目)。
        }
        newly_expired
    }
}
