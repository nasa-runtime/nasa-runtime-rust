// ============================================================================
// src/cluster/redis_notifier.rs —— Redis Stream transport(feature = "redis")。
//
// 全广播 fanout 语义:所有节点共享同一 stream key,各自独立 `$` 游标(不重放历史、
// 不用 consumer group——那是负载均衡,不适合 fanout)。架构说明。
//
// 发布走【独立任务 + 有界 mpsc】(不在连接读任务里同步等 Redis)
// 接收走【独立任务】XREAD BLOCK 循环,解出 "d" 字段交给 on_message。
//
// 投递语义:**at-most-once**——XADD 失败 / 发布队列满 / 连接重连窗口内的消息可能丢失;
// 上层(presence 周期对账、行情"最新覆盖")据此设计。需 at-least-once 见任务 #21。
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
    /// 单次 XREAD BLOCK 的最长等待时间。必须大于 0,否则 shutdown 会被永久 BLOCK 卡住。
    pub read_block_ms: usize,
    /// 单次 XREAD 最多读取的消息数。只影响每轮吞吐,不改变 at-most-once 语义。
    pub read_count: usize,
    /// 发布端 XADD 的近似 MAXLEN。`None` 表示不裁剪,由外部自行治理 stream 长度。
    pub maxlen: Option<usize>,
}

impl Default for RedisNotifierConfig {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
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
/// 它不是可靠队列:发布队列满、XADD 失败、重连窗口内的事件都会按 at-most-once 丢弃;
/// 上层必须依赖 presence 周期对账或业务最新值覆盖来恢复最终状态。
pub struct RedisNotifier {
    config: RedisNotifierConfig,
    client: redis::Client,
    pub_tx: Mutex<Option<mpsc::Sender<Bytes>>>,
    shutdown: Arc<AtomicBool>,
    /// 单次启动门禁:重复 start 不再生成第二套 reader/publisher(避免重复消费)。
    started: AtomicBool,
    /// 累计发布丢弃数(队列满/已关闭)。可观测 / health。
    dropped: AtomicU64,
    /// 累计 XADD 实际失败丢弃数(连接坏/WRONGTYPE 等;队列已出但写 Redis 失败)。。
    xadd_failed: Arc<AtomicU64>,
    /// reader/publisher 后台任务跟踪器;graceful shutdown 经 Cluster 暴露后 join。
    tasks: TaskTracker,
    /// 取消信号:shutdown 时 cancel,reader 的 XREAD BLOCK / 重连 sleep / connect 在 select 中
    /// 立即被打断退出,不必等满 read_block_ms。
    cancel: CancellationToken,
}

impl RedisNotifier {
    /// 构造 Redis Stream 集群通知器并校验运行参数。
    ///
    /// # 参数
    /// - `config`: Redis Stream transport 配置,包含连接 URI、stream key、XREAD/XADD 限制和裁剪策略。
    pub fn new(config: RedisNotifierConfig) -> redis::RedisResult<Arc<RedisNotifier>> {
        // 启动期配置校验:read_block_ms=0 在 Redis 表示**永久** BLOCK(shutdown
        // 只能等连接返回);read_count/maxlen 为 0 也无意义 → 构造即拒绝。
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
        }))
    }

    /// 累计发布丢弃数(入队前:队列满 / 已关闭),供 health/metrics 上报。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 累计 XADD 实际写失败丢弃数(入队后写 Redis 失败),供 health/metrics 上报。
    pub fn xadd_failed(&self) -> u64 {
        self.xadd_failed.load(Ordering::Relaxed)
    }
}

/// 构造集群通知配置错误；用于在缺失参数时提前失败。
///
/// # 参数
/// - `msg`: 业务消息体或事件载荷。
fn cfg_err(msg: &str) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::InvalidClientConfig,
        "bad RedisNotifierConfig",
        msg.to_string(),
    ))
}

/// reader 起始游标:解析一次为**具体 stream id**(流末尾的 id),之后只读更新的消息。
/// 不每轮用 `$`——`$` 在两次 XREAD 间会重新以"当时最新"为起点,留漏消息窗口。
/// 流为空(查询成功但无记录)→ "0-0"(从头;此时无历史,等价于只读新)。
/// **查询出错**(认证/ACL/超时/断连)→ 返回 Err,由调用方重连重试,**绝不**退化成 0-0
/// 从头重放整条 Stream。
///
/// # 参数
/// - `conn`: 用于查询 stream 尾部 ID 的 Redis 异步连接。
/// - `key`: 集群通知使用的 Redis Stream key。
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
    /// 启动 start 流程；用于初始化后台任务或运行时。
    ///
    /// # 参数
    /// - `on_message`: Redis notifier 收到跨节点消息时调用的回调。
    fn start(&self, on_message: OnMessage) -> super::ReadyFuture {
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
        self.tasks.spawn(async move {
            'outer: while !sd.load(Ordering::Relaxed) {
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
                    }
                };
                loop {
                    // recv 与 cancel 二选一:shutdown 立即退出,不等下一条 payload。
                    let payload = tokio::select! {
                        () = cancel_pub.cancelled() => break 'outer,
                        p = rx.recv() => match p { Some(p) => p, None => break 'outer },
                    };
                    let mut cmd = redis::cmd("XADD");
                    cmd.arg(&key);
                    if let Some(ml) = maxlen {
                        cmd.arg("MAXLEN").arg("~").arg(ml);
                    }
                    cmd.arg("*").arg("d").arg(payload.as_ref());
                    // in-flight XADD 也与 cancel 二选一:截止点到达即取消该写、退出。
                    // 注:这会丢弃尚未发出的已入队事件,符合 at-most-once 契约;本通道不做 drain。
                    let xadd = cmd.query_async::<redis::Value>(&mut conn);
                    let res = tokio::select! {
                        () = cancel_pub.cancelled() => break 'outer,
                        r = xadd => r,
                    };
                    if let Err(e) = res {
                        // 入队后写 Redis 失败 → 这条消息丢失,计入 xadd_failed。
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
        self.tasks.spawn(async move {
            // 游标只解析一次(首连);重连时**保留** last_id 续读,不跳过断连期间的消息。
            let mut last_id: Option<String> = None;
            let mut ready_tx = Some(ready_tx);
            while !sd.load(Ordering::Relaxed) {
                // 阻塞型 XREAD BLOCK 必须关掉 multiplexed 连接默认 500ms 响应超时(否则 BLOCK 被误判超时)。
                let conn_cfg = redis::AsyncConnectionConfig::new().set_response_timeout(None);
                let conn_fut = client.get_multiplexed_async_connection_with_config(&conn_cfg);
                let mut conn = tokio::select! {
                    () = cancel.cancelled() => return, // shutdown:立即退出,不阻塞重连
                    r = conn_fut => match r {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("redis reader connect failed: {e}; retry in 1s");
                            tokio::select! {
                                () = cancel.cancelled() => return,
                                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                            }
                        }
                    }
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
                // 起始游标已定(= 当前流末尾)→ 触发 ready;此后 id>cursor 的新消息必被读到,不丢。
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
                            tracing::warn!("redis XREAD error: {e}; reconnecting");
                            break; // 连接异常 → 跳出内层重连(保留 last_id 由 resolve 重新对齐)
                        }
                    }
                }
                // 重连前退避:cancel 时立即退出,不空等 1s。
                tokio::select! {
                    () = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
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

    /// 发布集群通知消息；用于把本节点事件广播到其他节点。
    ///
    /// # 参数
    /// - `payload`: 已编码完成的集群通知消息字节。
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

    /// 关闭集群通知任务；用于停止订阅和后台资源。
    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.cancel.cancel(); // 立即打断 reader 的 XREAD BLOCK / sleep / connect
        *self.pub_tx.lock().unwrap() = None; // drop 发送端 → publisher recv 返回 None 退出
    }

    /// 返回任务跟踪器；用于服务关闭时等待后台任务退出。
    fn task_tracker(&self) -> Option<&TaskTracker> {
        Some(&self.tasks)
    }
}
