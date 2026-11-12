// ============================================================================
// src/cluster/redis_notifier.rs —— Redis Stream transport(feature = "redis")。
//
// 全广播 fanout 语义：所有节点共享同一 stream key，各自从首次查询的流尾 ID 开始，
// 同进程重连保留游标，不使用会在节点间分摊记录的 consumer group。
//
// 发布走【独立任务 + 有界 mpsc】(不在连接读任务里同步等 Redis)
// 接收走【独立任务】XREAD BLOCK 循环,解出 "d" 字段交给 on_message。
//
// 投递语义：XADD 报错后不重发，发布队列满、关闭、流裁剪或新进程启动均可能丢失通知。
// 上层可用 presence 周期对账恢复状态；本通道不提供持久化确认或业务事件可靠交付。
// ============================================================================

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::{Notifier, OnMessage};

/// 保存 RedisNotifierConfig 配置项；用于把外部参数集中传入运行时。
#[derive(Clone)]
pub struct RedisNotifierConfig {
    /// Redis 连接 URI。必须包含认证和库编号时,直接写在 URI 里。
    pub uri: String,
    /// 集群广播使用的 stream key。所有节点必须使用同一个 key 才能 fan-out。
    pub stream_key: String,
    /// 单次 XREAD BLOCK 的最长等待毫秒数，必须大于零；关闭信号另行取消等待。
    pub read_block_ms: usize,
    /// 单次 XREAD 最多读取的消息数。只影响每轮吞吐,不改变 at-most-once 语义。
    pub read_count: usize,
    /// 发布端 XADD 的近似 MAXLEN。`None` 表示不裁剪,由外部自行治理 stream 长度。
    pub maxlen: Option<usize>,
}

impl Default for RedisNotifierConfig {
    /// 业务作用：为 Redis Stream 广播提供本机连接、读取批次和近似裁剪的默认配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：使用固定 stream key、五秒阻塞读取、每次最多 256 条及约一万条保留上限的配置。
    /// 不创建连接或 stream，也不自动配置 Redis 认证。
    fn default() -> Self {
        Self {
            uri: "redis://127.0.0.1:6379".into(),
            stream_key: "nasa:cluster:events".into(),
            read_block_ms: 5000,
            read_count: 256,
            maxlen: Some(10_000),
        }
    }
}

/// 基于 Redis Stream 的跨节点广播 transport。
///
/// 发布不重试，首次启动跳过已有记录，同进程重连从保留游标续读。
/// 队列满、关闭或流裁剪可能造成缺失；通知依赖上层对账恢复，不提供可靠队列语义。
pub struct RedisNotifier {
    /// 启动后固定使用的连接、读取和裁剪配置。
    config: RedisNotifierConfig,
    /// 供独立读写任务各自建立异步连接的客户端。
    client: redis::Client,
    /// 本地有界发布队列入口；启动前和关闭后为 None。
    pub_tx: Mutex<Option<mpsc::Sender<Bytes>>>,
    /// 供后台循环观察的关闭状态，与取消信号共同结束任务。
    shutdown: Arc<AtomicBool>,
    /// 单次启动门禁:重复 start 不再生成第二套 reader/publisher(避免重复消费)。
    started: AtomicBool,
    /// 累计发布丢弃数(队列满/已关闭)。可观测 / health。
    dropped: AtomicU64,
    /// 发布任务观察到 XADD 返回错误的累计次数，不包含关闭取消等待。
    xadd_failed: Arc<AtomicU64>,
    /// reader/publisher 后台任务跟踪器;graceful shutdown 经 Cluster 暴露后 join。
    tasks: TaskTracker,
    /// 取消信号:shutdown 时 cancel,reader 的 XREAD BLOCK / 重连 sleep / connect 在 select 中
    /// 立即被打断退出,不必等满 read_block_ms。
    cancel: CancellationToken,
    /// reader 已建立连接且初始游标已确定的观测，失败或任务退出时撤销。
    reader_connected: Arc<AtomicBool>,
    /// publisher 最近建立连接的观测，不代表当前每条 XADD 都会成功。
    publisher_connected: Arc<AtomicBool>,
}

impl RedisNotifier {
    /// 业务作用：构造 Redis Stream 集群通知器并校验运行参数。
    ///
    /// 参数说明：
    /// - `config`: Redis Stream transport 配置,包含连接 URI、stream key、XREAD/XADD 限制和裁剪策略。
    ///
    /// 返回：参数和 URI 可用时返回尚未启动的共享通知器；无效读取限制、裁剪值或 URI 返回 Redis 错误。
    /// 不尝试联网，不验证认证、ACL、stream 存在性或 key 类型。
    pub fn new(config: RedisNotifierConfig) -> redis::RedisResult<Arc<RedisNotifier>> {
        // 不允许配置永久阻塞读取或零批次/裁剪值，保持读取周期和保留策略明确；关闭另由取消信号打断。
        if config.read_block_ms == 0 {
            return Err(cfg_err("read_block_ms must be > 0 (0 = block forever)"));
        }
        if config.read_count == 0 {
            return Err(cfg_err("read_count must be > 0"));
        }
        if matches!(config.maxlen, Some(0)) {
            return Err(cfg_err("maxlen must be None or > 0"));
        }
        let client = redis::Client::open(config.uri.clone())?;
        Ok(Arc::new(RedisNotifier {
            config,
            client,
            pub_tx: Mutex::new(None),
            shutdown: Arc::new(AtomicBool::new(false)),
            started: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
            xadd_failed: Arc::new(AtomicU64::new(0)),
            tasks: TaskTracker::new(),
            cancel: CancellationToken::new(),
            reader_connected: Arc::new(AtomicBool::new(false)),
            publisher_connected: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// 业务作用：累计发布丢弃数(入队前:队列满 / 已关闭),供 health/metrics 上报。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未启动、队列满或发布通道关闭造成的同步拒绝次数；不含 XADD 失败或关闭时丢弃的已入队消息。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 业务作用：统计发布任务观察到 XADD 返回错误的次数，供监控区分同步入队拒绝与写入异常。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：XADD 错误累计数；对应记录不重试，网络错误不证明 Redis 未接收该记录。
    /// 关闭时取消正在等待的 XADD 不计入此值。
    pub fn xadd_failed(&self) -> u64 {
        self.xadd_failed.load(Ordering::Relaxed)
    }

    /// 业务作用：报告广播读写两侧当前连接是否均可用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已就绪且未关闭、两项任务仍持有连接时返回真；不承诺消息可靠交付。
    pub fn is_healthy(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.reader_connected.load(Ordering::Acquire)
            && self.publisher_connected.load(Ordering::Acquire)
    }
}

struct ConnectionObservation(Arc<AtomicBool>);

impl Drop for ConnectionObservation {
    /// 业务作用：任务退出或异常展开后撤销历史连接成功观测。
    ///
    /// 参数说明：无。
    ///
    /// 返回：健康立即变为未连接。
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// 业务作用：把通知器本地参数约束失败转换为 Redis 客户端配置错误。
///
/// 参数说明：
/// - `msg`: 不满足读取或裁剪配置要求的原因。
///
/// 返回：带稳定分类 `InvalidClientConfig` 和原因文本的 Redis 错误，不尝试建立连接。
fn cfg_err(msg: &str) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::InvalidClientConfig,
        "bad RedisNotifierConfig",
        msg.to_string(),
    ))
}

/// 业务作用：reader 起始游标:解析一次为**具体 stream id**(流末尾的 id),之后只读更新的消息。
/// 不每轮用 `$`——`$` 在两次 XREAD 间会重新以"当时最新"为起点,留漏消息窗口。
/// 流为空(查询成功但无记录)→ "0-0"(从头;此时无历史,等价于只读新)。
/// **查询出错**(认证/ACL/超时/断连)→ 返回 Err,由调用方重连重试,**绝不**退化成 0-0
/// 从头重放整条 Stream。
///
/// 参数说明：
/// - `conn`: 用于查询 stream 尾部 ID 的 Redis 异步连接。
/// - `key`: 集群通知使用的 Redis Stream key。
///
/// 返回：查询时最新一条记录的具体 ID；查询成功但无记录时为 `0-0`。
/// Redis 查询失败原样返回错误，不创建 stream、不消费记录，也不以默认游标掩盖失败。
async fn resolve_start_id(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
) -> redis::RedisResult<String> {
    let r: StreamRangeReply = conn.xrevrange_count(key, "+", "-", 1).await?;
    Ok(r.ids
        .first()
        .map(|e| e.id.clone())
        .unwrap_or_else(|| "0-0".to_string()))
}

impl Notifier for RedisNotifier {
    /// 业务作用：单次启动 Redis 广播的独立发布与读取任务，并提供首次读取就绪通知。
    ///
    /// 参数说明：
    /// - `on_message`: 对合法非空 `d` 字段执行的同步回调；阻塞会拖延读取，panic 不在此隔离。
    ///
    /// 返回：reader 首次连通并解析具体起始游标后成功的 future；关闭或 reader 提前退出返回未就绪错误，重复启动返回错误。
    /// future 成功不保证 publisher 已连通；丢弃 future 不停止后台任务，须显式关闭通知器。
    /// 首次从流尾开始，同进程重连沿用游标；关闭不排空发布队列，裁剪或故障期间仍可能丢失通知。
    fn start(&self, on_message: OnMessage) -> super::ReadyFuture {
        // 关闭是终态，不能通过再次启动恢复旧通知器的后台任务。
        if self.cancel.is_cancelled() {
            return Box::pin(async {
                Err(super::StartError::NotReady(
                    "redis notifier is closed".into(),
                ))
            });
        }
        // 单次启动:重复 start 返回 AlreadyStarted,不生成第二套 reader。
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::warn!("RedisNotifier::start called more than once");
            return Box::pin(async { Err(super::StartError::AlreadyStarted) });
        }
        // ready:reader 连上并定好起始游标后触发,start 的 ready future 据此 resolve。
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        // ---- 发布任务:有界 mpsc → XADD;连接失败/断开自动重连 ----
        let (tx, mut rx) = mpsc::channel::<Bytes>(1024);
        *self.pub_tx.lock().unwrap() = Some(tx);
        let client = self.client.clone();
        let key = self.config.stream_key.clone();
        let maxlen = self.config.maxlen;
        let sd = self.shutdown.clone();
        let xadd_failed = self.xadd_failed.clone();
        let cancel_pub = self.cancel.clone();
        let publisher_connected = self.publisher_connected.clone();
        let observation = ConnectionObservation(publisher_connected.clone());
        self.tasks.spawn(async move {
            let _observation = observation;
            'outer: while !sd.load(Ordering::Relaxed) {
                publisher_connected.store(false, Ordering::Release);
                // connect 也与 cancel 二选一:shutdown 立即退出,不被连接握手拖住。
                let conn_fut = client.get_multiplexed_async_connection();
                let mut conn = tokio::select! {
                    () = cancel_pub.cancelled() => break 'outer,
                    r = conn_fut => match r {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("redis publisher connect failed: {e}; retry in 1s");
                            tokio::select! {
                                () = cancel_pub.cancelled() => break 'outer,
                                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                            }
                        }
                    },
                };
                publisher_connected.store(true, Ordering::Release);
                loop {
                    // recv 与 cancel 二选一:shutdown 立即退出,不等下一条 payload。
                    let payload = tokio::select! {
                        () = cancel_pub.cancelled() => break 'outer,
                        p = rx.recv() => match p {
                            Some(p) => p,
                            None => break 'outer,
                        },
                    };
                    let mut cmd = redis::cmd("XADD");
                    cmd.arg(&key);
                    if let Some(ml) = maxlen {
                        cmd.arg("MAXLEN").arg("~").arg(ml);
                    }
                    cmd.arg("*").arg("d").arg(payload.as_ref());
                    // 关闭时停止等待 XADD 并退出，不排空余下队列；已发给 Redis 的命令无法据此撤回。
                    let xadd = cmd.query_async::<redis::Value>(&mut conn);
                    let res = tokio::select! {
                        () = cancel_pub.cancelled() => break 'outer,
                        r = xadd => r,
                    };
                    if let Err(e) = res {
                        // 写入结果失败时记录异常且不重发；网络失败可能发生在 Redis 已执行之后。
                        xadd_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("redis XADD failed: {e}; reconnecting");
                        continue 'outer; // 连接坏了 → 重连
                    }
                }
            }
        });

        // ---- 接收任务:解析一次起始游标,XREAD BLOCK 循环;断开重连;shutdown 退出 ----
        let client = self.client.clone();
        let key = self.config.stream_key.clone();
        let block = self.config.read_block_ms;
        let count = self.config.read_count;
        let sd = self.shutdown.clone();
        let cancel = self.cancel.clone();
        let reader_connected = self.reader_connected.clone();
        let observation = ConnectionObservation(reader_connected.clone());
        self.tasks.spawn(async move {
            let _observation = observation;
            // 游标只解析一次(首连);重连时**保留** last_id 续读,不跳过断连期间的消息。
            let mut last_id: Option<String> = None;
            let mut ready_tx = Some(ready_tx);
            while !sd.load(Ordering::Relaxed) {
                reader_connected.store(false, Ordering::Release);
                // 阻塞型 XREAD BLOCK 必须关掉 multiplexed 连接默认 500ms 响应超时(否则 BLOCK 被误判超时)。
                let conn_cfg = redis::AsyncConnectionConfig::new().set_response_timeout(None);
                let conn_fut = client.get_multiplexed_async_connection_with_config(&conn_cfg);
                let mut conn = tokio::select! {
                    () = cancel.cancelled() => return,
                    // 连接握手也服从取消信号，避免关闭被重连等待拖延。
                    r = conn_fut => match r {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("redis reader connect failed: {e}; retry in 1s");
                            tokio::select! {
                                () = cancel.cancelled() => return,
                                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                            }
                        }
                    },
                };
                if last_id.is_none() {
                    let resolve = tokio::select! {
                        () = cancel.cancelled() => return,
                        r = resolve_start_id(&mut conn, &key) => r,
                    };
                    match resolve {
                        Ok(id) => last_id = Some(id),
                        Err(e) => {
                            // 游标查询失败:重连重试,**不**退化成 0-0 重放整条流。
                            tracing::warn!("redis resolve start id failed: {e}; retry in 1s");
                            tokio::select! {
                                () = cancel.cancelled() => return,
                                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                            }
                        }
                    }
                }
                // 首次游标确定后才发布读取就绪；后续从具体 ID 续读，仍受 stream 裁剪与故障影响。
                reader_connected.store(true, Ordering::Release);
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(());
                }
                let mut cursor = last_id.clone().unwrap();
                loop {
                    if sd.load(Ordering::Relaxed) {
                        return;
                    }
                    let opts = StreamReadOptions::default().block(block).count(count);
                    let keys = [key.as_str()];
                    let cursors = [cursor.as_str()];
                    // XREAD BLOCK 与 cancel 二选一:shutdown 时 drop 该 future(取消请求),立即退出,
                    // 不必等满 read_block_ms。
                    let reply: redis::RedisResult<StreamReadReply> = tokio::select! {
                        () = cancel.cancelled() => return,
                        r = conn.xread_options(&keys, &cursors, &opts) => r,
                    };
                    match reply {
                        Ok(r) => {
                            for skey in r.keys {
                                for entry in skey.ids {
                                    cursor = entry.id.clone(); // 始终推进为具体 id,无 $ 漏窗
                                    last_id = Some(cursor.clone()); // 跨重连保留
                                    if let Some(v) = entry.map.get("d") {
                                        if let Ok(bytes) =
                                            redis::from_redis_value::<Vec<u8>>(v.clone())
                                        {
                                            if !bytes.is_empty() {
                                                on_message(Bytes::from(bytes));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            reader_connected.store(false, Ordering::Release);
                            tracing::warn!("redis XREAD error: {e}; reconnecting");
                            break; // 保留 last_id 继续读取，重连时不重新解析流尾。
                        }
                    }
                }
                // 重连前退避:cancel 时立即退出,不空等 1s。
                tokio::select! {
                    () = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                }
            }
        });

        // ready future:等 reader 定好起始游标。oneshot 被取消(reader 在就绪前退出,如
        // 连不上/认证失败/shutdown)→ Err(NotReady),不再"假就绪"。
        Box::pin(async move {
            ready_rx.await.map_err(|_| {
                super::StartError::NotReady("redis reader stopped before ready".into())
            })
        })
    }

    /// 业务作用：发布集群通知消息；用于把本节点事件广播到其他节点。
    ///
    /// 参数说明：
    /// - `payload`: 已编码完成的集群通知消息字节。
    ///
    /// 返回：进入容量为 1024 的本地发布队列时为 `Queued`；队列满为 `Full`；未启动或通道关闭为 `Closed`。
    /// 被拒绝的调用计入 dropped；成功不证明 XADD 已执行或其它节点已经读取，也不校验负载内容。
    fn publish(&self, payload: Bytes) -> super::PublishOutcome {
        use super::PublishOutcome;
        let guard = self.pub_tx.lock().unwrap();
        let Some(tx) = guard.as_ref() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return PublishOutcome::Closed; // 未 start / 已 shutdown
        };
        match tx.try_send(payload) {
            Ok(()) => PublishOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("redis publish queue full, dropped");
                PublishOutcome::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                PublishOutcome::Closed
            }
        }
    }

    /// 业务作用：关闭集群通知任务；用于停止订阅和后台资源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发布关闭状态、取消读取及发布等待并移除发送通道；不等待任务退出，不排空队列。
    /// 重复调用不重新启动任务；已被 Redis 执行的命令不会回滚，完成等待由任务跟踪器承担。
    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.cancel.cancel(); // 立即打断 reader 的 XREAD BLOCK / sleep / connect
        *self.pub_tx.lock().unwrap() = None; // drop 发送端 → publisher recv 返回 None 退出
    }

    /// 业务作用：返回任务跟踪器；用于服务关闭时等待后台任务退出。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终提供本通知器 reader 和 publisher 共用的任务跟踪器；
    /// 查询本身不发出关闭信号，调用方须按停机流程取消任务并关闭跟踪器后等待。
    fn task_tracker(&self) -> Option<&TaskTracker> {
        Some(&self.tasks)
    }
}
