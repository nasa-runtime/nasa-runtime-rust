// ============================================================================
// src/cluster.rs —— 集群:跨节点 fan-out + 防回环。
// 架构说明:ClusterEvent 显式带 message_mode(已在 proto)。
//
// 分层:Notifier(transport 接缝,可换内存实现/Redis/Kafka)+ Cluster(transport 无关编排)。
// 数据流:本地 Sender.send → cluster.publish(包 ClusterEvent)→ Notifier 广播 →
//   对端 Cluster.on_received(防回环 + 目标过滤)→ Sender.send_local(本地分发)。
// 防回环:ClusterEvent.source_node == 本节点 → 丢弃，防止广播回送再次本地投递。
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

/// 业务作用：周期任务不能接受零间隔；极端时长也必须在进入 Tokio timer 前收敛。
///
/// 参数说明：
/// - `duration`：调用方要求的 presence 广播周期。
///
/// 返回：限制在 1 毫秒至 365 天之间的周期，不启动任务或等待。
fn bounded_period(duration: Duration) -> Duration {
    duration.clamp(Duration::from_millis(1), MAX_CLUSTER_RUNTIME_DURATION)
}

/// 业务作用：墙钟参数来自公开观测 API；饱和差值避免极端或回拨时间戳触发整数溢出。
///
/// 参数说明：
/// - `now_local`：本机当前毫秒时间。
/// - `last_seen_local`：本机最近接受该节点事件的毫秒时间。
/// - `ttl_millis`：允许的最大静默时长，单位为毫秒。
///
/// 返回：差值不大于 TTL 时为 true，包含边界；墙钟回拨产生负差值时也会保持存活判断。
fn within_ttl(now_local: i64, last_seen_local: i64, ttl_millis: i64) -> bool {
    now_local.saturating_sub(last_seen_local) <= ttl_millis
}

// Redis 传输通过公开集群接口提供；只重导出稳定构造入口，避免业务依赖内部文件布局。
#[cfg(feature = "redis")]
mod redis_notifier;
#[cfg(feature = "redis")]
pub use redis_notifier::{RedisNotifier, RedisNotifierConfig};

/// 收到一条对端 payload 时的回调(Notifier 在接收线程调)。
pub type OnMessage = Arc<dyn Fn(Bytes) + Send + Sync>;

/// transport 无法满足启动条件或拒绝重复启动的原因。
#[derive(Debug, Clone)]
pub enum StartError {
    /// 在期限内未就绪(连不上 / 认证失败 / 取消)。
    NotReady(String),
    /// 同一 notifier 重复 start。
    AlreadyStarted,
}

/// `start` 返回的就绪 future，成功条件由具体 transport 定义。
/// Redis 实现确认 reader 连接与起始游标，不同时证明 publisher 可写；后续故障仍须单独观测。
pub type ReadyFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), StartError>> + Send>>;

/// 跨节点本地发布准入结果，不等于消息到达远端。
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
    /// 业务作用：直接发布借用业务信封，不先编码中间 ClusterEvent。
    ///
    /// 参数说明：
    ///
    /// - `source`: 受控来源节点与 incarnation。
    /// - `message`: 当前业务消息信封。
    /// - `mode`: 客户端最终消息编码模式。
    /// - `target_nodes`: 可选精确目标逻辑节点。
    ///
    /// 返回：数据面本地准入结果；Queued 不等于 broker 确认或客户端送达，其余变体区分容量、关闭与参数拒绝。
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
    /// 业务作用：启动收发;返回 ready future,await 它直到就绪。
    ///
    /// 参数说明：
    /// - `on_message`: transport 收到跨节点 payload 后调用的回调。
    ///
    /// 返回：表达本实现启动就绪或失败的 future；具体就绪范围由 transport 定义，不证明以后持续可用。
    fn start(&self, on_message: OnMessage) -> ReadyFuture;

    /// 业务作用：发布一条 payload(ClusterEvent 编码后的字节),返回入队结果。
    ///
    /// 参数说明：
    /// - `payload`: 已编码的 ClusterEvent 字节。
    ///
    /// 返回：入队、容量拒绝或关闭结果；Queued 不表示广播介质写入成功，也不确认对端处理。
    fn publish(&self, payload: Bytes) -> PublishOutcome;

    /// 业务作用：请求 transport 结束收发，由持有资源的实现覆盖默认行为。
    ///
    /// 参数说明：无。
    ///
    /// 返回：默认实现不执行动作；有后台任务的实现应发起取消，实际退出须另行等待 task_tracker。
    fn shutdown(&self) {}
    /// 业务作用：transport 的后台任务跟踪器(供 graceful shutdown join);无后台任务返回 None。
    ///
    /// 参数说明：无。
    ///
    /// 返回：可借用的任务跟踪器，默认 None；该缺省值不证明自定义实现的外部任务已经退出。
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

/// 本节点全量 presence 快照来源，必须返回同一时刻的 `(版本, 本地订阅集合)`。
/// SessionRegistry::presence_snapshot 在同锁中读取两者；自定义来源须自行保证一致性和容量边界。
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
    /// presence publish 被拒的累计数(周期 FULL 自愈,但应可观测)。
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
/// 具体广播介质由 [`Notifier`] 注入；编排层不重试也不去重。
/// 重复、丢失及关闭边界还取决于 transport，不能仅根据本地入队推断投递保证。
pub struct Cluster {
    /// 用于来源标识和回环过滤的稳定逻辑节点 ID。
    local_node: String,
    /// presence 与未分离数据通道时的消息广播介质。
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
    /// presence publish 被拒的累计数；周期 FULL 可恢复目录，拒绝次数仍需用于观测背压。
    presence_dropped: std::sync::atomic::AtomicU64,
    /// data producer 容量拒绝计数。
    data_full: std::sync::atomic::AtomicU64,
    /// data producer 关闭拒绝计数。
    data_closed: std::sync::atomic::AtomicU64,
    /// data 本地确定拒绝计数。
    data_rejected: std::sync::atomic::AtomicU64,
}

impl Cluster {
    /// 业务作用：使用已校验的 Incarnation 构造集群运行时，按节点身份与数值代次拒绝旧实例事件。
    /// 同 node_id 的代次必须持久单调递增；文本宽度和前导零不改变围栏比较结果。
    ///
    /// 参数说明：
    /// - `local_node`: 本节点稳定 ID,用于防回环、presence 目录和 fencing 分组。
    /// - `notifier`: 跨节点广播 transport,负责真正收发 ClusterEvent 字节。
    /// - `on_local`: 接收对端消息后执行的本地投递回调,通常是 `Sender::send_local`。
    /// - `incarnation`: 已通过格式检查的实例代次，调用方负责其持久单调性与来源可信性。
    ///
    /// 返回：尚未启动的共享集群对象；节点表默认 TTL 为 30 秒，不验证节点 ID，不获取持久序号。
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

    /// 业务作用：一次性指定业务数据的独立发布通道，presence 仍经 notifier 广播。
    ///
    /// 参数说明：
    ///
    /// - `publisher`: 绑定固定 data lane/topic 的发布器。
    ///
    /// 返回：首次安装为 Ok，重复安装返还传入的 publisher 并保留原实例。
    /// 应在启动前装配；本函数不检查运行状态，运行中安装可能使并发发布使用不同通道。
    pub fn set_data_publisher(
        &self,
        publisher: Arc<dyn ClusterDataPublisher>,
    ) -> Result<(), Arc<dyn ClusterDataPublisher>> {
        self.data_publisher.set(publisher)
    }

    /// 业务作用：注入全量 presence 快照提供者(框架装配时从 SessionRegistry::presence_snapshot 取)。
    ///
    /// 参数说明：
    /// - `f`: 同步返回 `(版本, 本地订阅集合)` 的快照闭包，两者须一致；执行期间持有 provider 锁，不得重入本方法。
    ///
    /// 返回：替换快照来源，不立即广播；闭包的容量、执行时长和 panic 由提供方负责。
    pub fn set_presence_provider(&self, f: PresenceProvider) {
        *self.presence.lock().unwrap() = Some(f);
    }

    /// 业务作用：返回本节点标识；用于过滤或标记集群消息来源。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造时保存的稳定逻辑节点标识借用；本函数不查询注册中心或验证身份。
    pub fn local_node(&self) -> &str {
        &self.local_node
    }

    /// 业务作用：提供对端实例围栏、存活与订阅目录的共享访问入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地 NodeRegistry 的共享句柄借用；目录可因事件延迟或丢失而不完整，不构成业务授权依据。
    pub fn nodes(&self) -> &Arc<NodeRegistry> {
        &self.nodes
    }

    /// 业务作用：汇总本地节点目录规模与发布拒绝计数，供容量及集群状态观测。
    ///
    /// 参数说明：无。
    ///
    /// 返回：三个节点计数来自同锁快照，发布计数独立读取；不保证跨全部字段的原子一致性或对端业务健康。
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

    /// 业务作用：以默认五秒时限等待 notifier 启动就绪，供宿主决定是否开放业务接入。
    ///
    /// 参数说明：无。
    ///
    /// 返回：传播 transport 的启动结果，超时返回 NotReady；失败不自动关闭 transport，也不恢复启动前的历史消息。
    pub async fn start(self: &Arc<Self>) -> Result<(), StartError> {
        self.start_with_timeout(Duration::from_secs(5)).await
    }

    /// 业务作用：启动接收循环并使用调用方提供的就绪超时。
    ///
    /// 参数说明：
    ///
    /// - `ready_timeout`: 等待 notifier 就绪 future 的时长，上限为 365 天；零值保留，不为各启动阶段重置。
    ///
    /// 返回：notifier 就绪或启动失败结果；等待超时返回 NotReady，丢弃 future 不等于停止已创建的 transport 任务。
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

    /// 业务作用：请求周期 presence 与 notifier 收发任务停止，结束本集群的运行责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：设置关闭标志并发出取消，不等待任务退出；独立 data publisher 的生命周期仍由其宿主管理。
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed); // 停周期 presence(标志兜底)
        self.cancel.cancel(); // presence 任务立即退出(不必等满一个 tick)
        self.notifier.shutdown(); // 停 notifier 收发(RedisNotifier 等)
    }

    /// 业务作用：graceful:cancel 后在**绝对截止点 `end`** 之前 join 集群自有任务(presence)+ notifier
    /// 后台任务(Redis reader/publisher)。各阶段共用同一截止点(不是各给一份完整 deadline),
    /// 故总耗时受 `end` 约束。返回是否在 `end` 前全部退出。
    ///
    /// 参数说明：
    /// - `end`: 优雅关闭的绝对截止时间,所有 join 阶段共同受它约束。
    ///
    /// 返回：本集群 tracker 与 notifier 暴露的 tracker 均完成等待时为 true；超时为 false，不强制中止剩余任务或等待独立 data publisher。
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

    /// 业务作用：向跨节点通道提交未限制节点的广播，由各接收端按本地会话过滤。
    /// presence 可能丢失，因此不使用其不完整目录裁剪广播目标；订阅目录不能作为投递权限证明。
    ///
    /// 参数说明：
    /// - `msg`: 要跨节点转发的业务消息信封。
    /// - `mode`: 内层消息编码模式；使用独立 data publisher 时作为其下行编码元数据。
    ///
    /// 返回：完成一次发布尝试，不返回送达确认；编码失败记录日志，通道拒绝记入对应计数，无自动重试。
    pub fn publish(&self, msg: &Message, mode: Mode) {
        self.publish_inner(msg, mode, None);
    }

    /// 业务作用：为跨节点消息携带显式目标节点，接收端只处理命中自身的消息。
    ///
    /// 参数说明：
    /// - `nodes`: 目标节点 ID 列表,接收端只在本节点命中时投递。
    /// - `msg`: 要跨节点转发的业务消息信封。
    /// - `mode`: 内层信封编码模式或独立 data publisher 使用的下行编码元数据。
    ///
    /// 返回：仅完成发布尝试，不保证介质定向传输或节点在线；空列表不应扩大为广播，目标完整性由调用方负责。
    pub fn publish_to(&self, nodes: &[&str], msg: &Message, mode: Mode) {
        let targets = nodes.iter().map(|n| Some(n.to_string())).collect();
        self.publish_inner(msg, mode, Some(targets));
    }

    /// 业务作用：优先使用独立数据发布器，否则编码 ClusterEvent，经单一通道发布一次。
    ///
    /// 参数说明：
    /// - `msg`: 已由上层完成权限判断的业务信封，本函数不执行业务授权。
    /// - `mode`: 内层信封编码或 data publisher 的下行编码元数据。
    /// - `target_nodes`: None 表示广播，Some 保留显式节点范围。
    ///
    /// 返回：记录所选通道的拒绝计数；编码失败只记录日志，不重试、不回退到另一通道，也不检查 Cluster 的关闭标志。
    fn publish_inner(&self, msg: &Message, mode: Mode, target_nodes: Option<Vec<Option<String>>>) {
        if let Some(publisher) = self.data_publisher.get() {
            // 来源代次必须可解析，缺少有效围栏时不得将业务消息交给独立数据通道。
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

    /// 业务作用：广播本地订阅集合与其一致版本，用全量快照校正对端目录中的增量遗漏。
    ///
    /// 参数说明：无。
    ///
    /// 返回：完成一次 presence 发布尝试；未注入 provider 时发送版本为零的空快照，失败不重试，后续成功的全量可恢复目录。
    pub fn publish_presence(&self) {
        // 版本与集合必须由 provider 一次取得；持有 provider 锁调用，避免替换与本次取样交错。
        let (version, groups) = match self.presence.lock().unwrap().as_ref() {
            None => (0, Vec::new()),
            Some(f) => f(),
        };
        self.publish_presence_event(PRESENCE_FULL, &groups, version);
    }

    /// 业务作用：将本地订阅从无到有或从有到无的变化广播为单标识 presence 增量。
    ///
    /// 参数说明：
    /// - `group`: 本节点发生订阅数量 0↔1 变化的标识。
    /// - `present`: true 表示本节点存在订阅者，false 表示最后一个订阅者已离开。
    /// - `version`: 跃迁时生成的 presence 版本号,接收端用它做 LWW 去旧。
    ///
    /// 返回：完成一次增量发布尝试；无法确认对端收到或目录同步，遗漏依赖后续全量快照校正。
    pub fn publish_presence_delta(&self, group: &str, present: bool, version: i64) {
        let groups = [group.to_string()];
        self.publish_presence_event(
            if present {
                PRESENCE_ADD
            } else {
                PRESENCE_DEL
            },
            &groups,
            version,
        );
    }

    /// 业务作用：组装并广播一条 presence(message_bytes=None;message_mode=kind;target_nodes=群名)。
    /// timestamp 复用为**该事件版本**(FULL=快照版本,delta=跃迁号):接收端据此做 LWW 去旧。
    ///
    /// 参数说明：
    /// - `kind`: FULL、ADD 或 DEL 的 presence 类型，内部调用方保证其有效性。
    /// - `groups`: 该快照或增量涉及的订阅标识，编码在 target_nodes 字段中。
    /// - `version`: 本地订阅变化序号或一致快照序号，不是墙钟时间。
    ///
    /// 返回：编码后交给 notifier，拒绝时计数并限频记录日志；不使用 data publisher，不重试或确认目录已同步。
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

    /// 业务作用：启动周期全量 presence 对账 + 过期节点清理(需在 tokio 运行时内)。
    ///
    /// 参数说明：
    /// - `interval`: 周期全量广播与过期目录清理间隔，限制在 1 毫秒至 365 天。
    ///
    /// 返回：登记一个后台任务，首轮等待一个周期；无重复启动门禁，调用方须避免多次启动，shutdown 请求其退出。
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

    /// 业务作用：Notifier 收到对端 payload(也会收到自己发的)→ 防回环 + 目标过滤 + 本地分发。
    ///
    /// 参数说明：
    /// - `bytes`: notifier 交付的 BitpackTlv ClusterEvent 字节，来源可信性须由传输与部署侧保证。
    ///
    /// 返回：presence 更新目录后结束；数据通过实例围栏后刷新存活，再检查目标与解码并同步回调。
    /// 旧实例、非法信封和非本节点目标不投递；本地回调 panic 不在此隔离，也不提供消息去重。
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
            // 无来源节点无法选择正确的实例围栏，不能继续更新目录或交付消息。
            None => return,
        };

        // 只接受 1 至 20 位 ASCII 十进制正数；缺失或非法代次不能续命、更新目录或投递。
        // 数值比较消除文本宽度与前导零的影响，但不验证发送方身份。
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
                // 发布端每次只发一个订阅标识；接收多个时逐项使用同一版本，保持各标识独立比较。
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

/// 业务作用：读取当前毫秒时间；用于节点心跳和过期判断。
///
/// 参数说明：无。
///
/// 返回：Unix epoch 后的本机墙钟毫秒数，早于 epoch 时取零；不提供单调时钟或跨节点时钟一致性。
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
    /// epoch 非正，或十进制文本不满足接收范围；零值和超长纯数字文本均归入此类。
    NonPositive,
}

impl std::fmt::Display for IncarnationError {
    /// 业务作用：以固定文本解释实例代次校验失败，避免输出原始输入材料。
    ///
    /// 参数说明：
    /// - `f`: 接收错误说明的 Display 格式化器。
    ///
    /// 返回：写入成功时为 Ok，输出目标拒绝写入时传播格式化错误；不改变错误类别。
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

/// 已校验的 incarnation fencing token：1 至 20 位 ASCII 十进制正数。
/// 接收端按数值比较同 node_id 的实例，更大表示更新；from_epoch 使用 20 位零填充，
/// parse 也接受合法的变长文本，前导零不改变数值权威。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incarnation(String);

impl Incarnation {
    /// 业务作用：从持久单调 epoch(如 Redis `INCR`)构造定宽(20 位零填充)token。epoch 须 > 0。
    ///
    /// 参数说明：
    /// - `epoch`: 外部持久单调递增序号,必须大于 0。
    ///
    /// 返回：正数转为 20 位零填充 token，非正数返回 NonPositive；本函数不分配序号，也不验证其单调来源。
    pub fn from_epoch(epoch: i64) -> Result<Incarnation, IncarnationError> {
        if epoch <= 0 {
            return Err(IncarnationError::NonPositive);
        }
        Ok(Incarnation(format!("{epoch:020}")))
    }

    /// 业务作用：从持久化文本恢复 incarnation token，验证正数语义和接收宽度。
    ///
    /// 返回：1 至 20 位 ASCII 十进制正数返回 token；其它输入返回 IncarnationError。
    ///
    /// 参数说明：
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

    /// 业务作用：解析 incarnation 字符串为**数值**(供 fence **按数值比较**,避免变长字典序错序)。
    ///
    /// 返回：1 至 20 位 ASCII 十进制正数返回其数值，其它输入返回 None，接收端据此拒绝。
    ///
    /// 参数说明：
    /// - `s`: 待验证和转换的 ASCII 十进制 incarnation 字符串。
    pub fn parse_value(s: &str) -> Option<u128> {
        // 接收宽度与 from_epoch 的输出上限一致，防止异常大值永久抬高围栏、阻断正常节点事件。
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

    /// 业务作用：借用已校验的实例代次文本，供集群信封携带来源围栏。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造或解析时保存的十进制文本，保留合法前导零；这是实例代次，不是节点 ID。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 业务作用：将已校验的实例代次文本移交给集群运行状态保存。
    ///
    /// 参数说明：无。
    ///
    /// 返回：原十进制字符串的所有权，不复制、不分配新代次，也不写入外部存储。
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
    /// 各订阅独立保存 (version, present)，不同标识的乱序增量不互相覆盖。
    groups: HashMap<String, GroupSlot>,
    /// 是否已被 prune 标记为过期 tombstone(只清了目录、保留 incarnation+水位)。仅"首次进入过期"
    /// 计数 + 清目录,避免每次 prune 重复计数/重复清;收到新事件刷新存活后复位。
    expired: bool,
}

impl NodeInfo {
    /// 业务作用：为首次观察到的逻辑节点建立空目录，等待当前事件设置实例围栏。
    ///
    /// 参数说明：
    /// - `now_local`: 本机收到该节点事件的毫秒时间，作为初始存活依据。
    ///
    /// 返回：无 incarnation、空订阅表且全量水位最低的记录；调用方仍须执行 fence 才能应用事件。
    fn fresh(now_local: i64) -> NodeInfo {
        NodeInfo {
            last_seen_local: now_local,
            incarnation: None,
            full_watermark: i64::MIN,
            groups: HashMap::new(),
            expired: false,
        }
    }

    /// 业务作用：incarnation 围栏(有序 fencing token):
    /// - **更新**(更大)→ 对端重启了新实例:重置目录与水位,纳入新实例,返回 true;
    /// - **相同** → 同实例,放行(版本去旧交给 per-group / 水位);
    /// - **更旧**(更小)→ 迟到的旧实例消息,拒绝(不让目录回切)。
    ///
    /// 参数说明：
    /// - `incarnation`: 已由接入层确认来源并校验为非零的实例代次。
    ///
    /// 返回：首次或相同、更大代次为 true，更小代次为 false 且状态不变；新代次清空目录和水位，本函数不刷新存活时间。
    fn fence(&mut self, incarnation: u128) -> bool {
        match self.incarnation {
            Some(cur) if incarnation == cur => true,
            Some(cur) if incarnation > cur => {
                // 新实例不能继承旧进程的订阅和快照水位，否则新事件会被旧目录错误约束。
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

    /// 业务作用：切到新 incarnation:清空群槽,水位归 MIN(新实例的 presence_seq 从 0 起,
    /// 不能被旧实例的高水位拒掉)。
    ///
    /// 参数说明：
    /// - `incarnation`: fence 已决定接受的新实例代次。
    ///
    /// 返回：替换代次并清空订阅槽、重置全量水位；存活时间和 expired 标志由外层接受事件的流程更新。
    fn reset_to(&mut self, incarnation: u128) {
        self.incarnation = Some(incarnation);
        self.full_watermark = i64::MIN;
        self.groups.clear();
    }

    /// 业务作用：应用一条全量快照 @version:在 snapshot 内的群置 present、缺席群下沉 tombstone
    /// (均不回退更新的 delta:仅当 version ≥ 槽版本才动),推进水位并回收 ≤水位 的 tombstone。
    ///
    /// 参数说明：
    /// - `snapshot`: 该实例在快照版本时存在本地订阅者的完整标识集合。
    /// - `version`: 与集合一致采集的本实例 presence 序号。
    ///
    /// 返回：低于全量水位时不变；否则对齐不比快照更新的槽、推进水位并回收已被水位覆盖的删除槽。
    fn apply_full(&mut self, snapshot: &HashSet<String>, version: i64) {
        // 旧快照不能覆盖已经确认的全量基线，否则可能恢复已删除的订阅。
        if version < self.full_watermark {
            return;
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
        // 不会复活；version > 水位的 tombstone 保留到后续 FULL，避免迟到 ADD 恢复该订阅。
        let wm = self.full_watermark;
        self.groups.retain(|_, s| s.present || s.version > wm);
    }

    /// 业务作用：应用一条单群 delta @version:水位门控(陈旧增量拒) + per-group LWW(仅更新才覆盖)。
    ///
    /// 参数说明：
    /// - `group`: 本次增量改变的订阅标识。
    /// - `present`: 当前实例是否存在该标识的本地订阅者。
    /// - `version`: 订阅数量跨过零点时生成的 presence 序号。
    ///
    /// 返回：仅当版本高于全量水位且高于该标识已知版本时替换状态；旧事件不恢复删除槽或回退目录。
    fn apply_delta(&mut self, group: &str, present: bool, version: i64) {
        // 已被全量基线覆盖的增量不能复活已经回收的删除槽。
        if version <= self.full_watermark {
            return;
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

    /// 业务作用：查询该实例的订阅槽是否标记为存在，供目录筛选使用。
    ///
    /// 参数说明：
    /// - `group`: 待查询的订阅标识。
    ///
    /// 返回：存在且 present 为 true 时返回 true；不检查节点 TTL、实际连接或业务权限。
    fn has_group(&self, group: &str) -> bool {
        self.groups.get(group).is_some_and(|s| s.present)
    }
}

/// 维护集群节点表；用于记录活跃节点并清理过期节点。
pub struct NodeRegistry {
    /// 基于本地接收时间判断存活的整毫秒 TTL，不负责清除实例围栏。
    ttl_millis: i64,
    /// 按稳定节点 ID 保存目录、存活事实与永久实例围栏的串行状态。
    nodes: Mutex<HashMap<String, NodeInfo>>,
}

impl NodeRegistry {
    /// 业务作用：构造节点注册表。
    ///
    /// 参数说明：
    /// - `ttl`: 节点存活 TTL,超过该时间未收到事件或 presence 即视为过期。
    ///
    /// 返回：空的共享节点表，TTL 上限为 365 天并截取整毫秒；零值保留，不启动清理任务或限制节点数量。
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

    /// 业务作用：数据事件先接受实例围栏裁决，再刷新来源节点的本地存活事实。
    ///
    /// 参数说明：
    /// - `node`: 发送数据事件的对端节点 ID。
    /// - `incarnation`: 接入层已校验来源与非零约束的实例代次，本方法只比较大小。
    /// - `now_local`: 本机收到事件时的毫秒时间,用于刷新 TTL。
    ///
    /// 返回：首次、相同或更新实例返回 true 并续期，更旧实例返回 false 且不续期；新实例会清空旧订阅与水位。
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

    /// 业务作用：全量 presence 先通过实例围栏，再续期节点并按订阅版本对齐目录。
    ///
    /// 参数说明：
    /// - `node`: 发送 FULL presence 的对端节点 ID。
    /// - `groups`: 快照版本对应的本地订阅标识全集。
    /// - `incarnation`: 已由接入层确认来源并验证为非零的发送实例代次。
    /// - `version`: FULL 快照版本,用于 LWW 去旧。
    /// - `now_local`: 本机收到 presence 时的毫秒时间,用于刷新 TTL。
    ///
    /// 返回：旧实例不续期也不更新；有效实例先刷新存活，即使快照版本低于水位而未改变目录，也保留此次存活事实。
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

    /// 业务作用：单订阅增量先通过实例围栏，再续期节点并按全量水位与该订阅版本决定是否应用。
    ///
    /// 参数说明：
    /// - `node`: 发送 delta presence 的对端节点 ID。
    /// - `group`: 本地订阅者数量发生 0↔1 变化的标识。
    /// - `present`: true 表示存在本地订阅者，false 表示最后一个订阅者已离开。
    /// - `incarnation`: 接入层已校验来源及非零约束的发送实例代次。
    /// - `version`: delta 事件版本,用于水位门控和 per-group LWW。
    /// - `now_local`: 本机收到 presence 时的毫秒时间,用于刷新 TTL。
    ///
    /// 返回：旧实例事件无效；有效实例先续期，只有高于全量水位及该订阅已知版本的增量才能改变目录。
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
        // 先围栏再刷新存活(同 apply_full)。
        if info.fence(incarnation) {
            info.last_seen_local = now_local;
            info.expired = false;
            info.apply_delta(group, present, version);
        }
    }

    /// 业务作用：now_local 时刻仍存活的节点列表(按本机时钟判 TTL)。
    ///
    /// 参数说明：
    /// - `now_local`: 本机当前毫秒时间,用于和节点 last_seen 比较 TTL。
    ///
    /// 返回：同锁采样时 TTL 内节点 ID 的无序副本；不查询远端，墙钟回拨可能延长存活判断。
    pub fn alive_nodes(&self, now_local: i64) -> Vec<String> {
        let m = self.nodes.lock().unwrap();
        m.iter()
            .filter(|(_, i)| within_ttl(now_local, i.last_seen_local, self.ttl_millis))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// 业务作用：依据本地最近接收时间判断指定节点是否仍在存活 TTL 内。
    ///
    /// 参数说明：
    /// - `node`: 要检查的节点 ID。
    /// - `now_local`: 本机当前毫秒时间,用于和节点 last_seen 比较 TTL。
    ///
    /// 返回：已知节点且静默时长不大于 TTL 时为 true；未知或超时为 false，不发起远端探测。
    pub fn is_alive(&self, node: &str, now_local: i64) -> bool {
        self.nodes
            .lock()
            .unwrap()
            .get(node)
            .is_some_and(|i| within_ttl(now_local, i.last_seen_local, self.ttl_millis))
    }

    /// 业务作用：统计指定时间点仍活跃的节点；用于观测集群可用规模。
    ///
    /// 参数说明：
    /// - `at`: 本机毫秒时间,用于判断每个节点是否仍在 TTL 内。
    ///
    /// 返回：本地 TTL 内节点数量；当前实现先生成节点列表再计数，不代表可投递连接数量。
    pub fn alive_count(&self, at: i64) -> usize {
        self.alive_nodes(at).len()
    }

    /// 业务作用：节点条目总数(含已过期的 incarnation tombstone)。可观测/容量监控用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：所有已记录逻辑节点的数量；过期清理保留实例围栏，因此该值不会随 TTL 到期自动减少。
    pub fn node_count(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }

    /// 业务作用：统计 TTL 外仍保留实例围栏的节点，供部署监控目录长期增长。
    /// 节点 ID 必须稳定且数量受控；过期条目不因时间流逝而删除，避免旧实例重新获得接收资格。
    ///
    /// 参数说明：
    /// - `now_local`: 本机当前毫秒时间,用于判断节点是否已经超过 TTL。
    ///
    /// 返回：当前 TTL 判断为过期的节点数量，不要求 prune 已运行，也不修改目录或 expired 标志。
    pub fn tombstone_count(&self, now_local: i64) -> usize {
        let m = self.nodes.lock().unwrap();
        m.values()
            .filter(|i| !within_ttl(now_local, i.last_seen_local, self.ttl_millis))
            .count()
    }

    /// 业务作用：(节点总数, 存活数, tombstone 数) **单锁内一次**算出——三值同一时刻、口径一致,
    /// 不再让调用方三次取锁拼出跨时刻快照。
    ///
    /// 参数说明：
    /// - `now_local`: 本机当前毫秒时间,用于同一口径计算 alive 与 tombstone。
    ///
    /// 返回：同锁快照中的 `(节点总数, TTL 内数量, TTL 外数量)`，后两项之和等于总数，不执行清理。
    pub fn stats(&self, now_local: i64) -> (usize, usize, usize) {
        let m = self.nodes.lock().unwrap();
        let node_count = m.len();
        let alive_count = m
            .values()
            .filter(|i| within_ttl(now_local, i.last_seen_local, self.ttl_millis))
            .count();
        (node_count, alive_count, node_count - alive_count)
    }

    /// 业务作用：查询本地目录中同时满足存活与指定订阅存在条件的节点。
    ///
    /// 参数说明：
    /// - `group`: 要查询的订阅标识。
    /// - `now_local`: 本机当前毫秒时间,用于排除 TTL 外节点。
    ///
    /// 返回：无序节点 ID 副本；presence 延迟或丢失可能使结果不完整，不能据此保证广播目标完整或授予订阅权限。
    pub fn nodes_for_group(&self, group: &str, now_local: i64) -> Vec<String> {
        let m = self.nodes.lock().unwrap();
        m.iter()
            .filter(|(_, i)| {
                within_ttl(now_local, i.last_seen_local, self.ttl_millis) && i.has_group(group)
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// 业务作用：处理 now_local 时刻已过期的节点;返回**本次新过期**的节点数(已是 tombstone 的不重复计)。
    /// 过期节点**降级为永久 tombstone**:首次过期才计数 + 清出群目录,且 `full_watermark` 抬到
    /// `max(watermark, 所有 GroupSlot.version)`——既挡更旧 incarnation 回切,也挡**同一 incarnation**
    /// 的旧 FULL(<水位)/旧 delta(≤水位)复活目录。同存活进程后续发当前版本
    /// FULL 仍可重建。
    ///
    /// 参数说明：
    /// - `now_local`: 本机当前毫秒时间,用于判断哪些节点超过 TTL。
    ///
    /// 返回：本次首次进入过期状态的节点数；先推进水位再清空订阅，保留实例围栏，已过期条目不重复计数。
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
