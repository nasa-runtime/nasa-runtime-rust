//! 按分区键串行、跨分区并行的异步执行器。
//!
//! 适合订单、撮合、账户余额等要求同一业务键严格有序处理，而不同键可以并发推进的任务流。
// ============================================================================
// nasa-partition —— 同 key 串行消费执行器(从 原实现 TimingWheel.partition 移植)
//
// 独立发布入口:
//   napart = "1"
//   use napart::PartitionExecutor;
//
// 解决的核心问题(资金安全):撮合主线程把「按 makerId 顺序更新 Redis JSON $.v」之类的
// 操作扔进来,要求【同一个 key 的多次操作严格按提交顺序落地】,否则大 maker 被多个 taker
// 并发蚕食时 setField 乱序 → 重启读 $.v 多卖币。
//
// ── 语义保证 ────────────────────────────────────────────────────────────────
//   · 同 key(hash 路由到同一 partition)→ 同一 worker → FIFO 串行:提交顺序 == 执行顺序。
//   · 不同 key → 落不同 partition → 最大并发(碰撞到同 partition 仍 FIFO 串行)。
//   · 任务 panic 隔离:单笔 panic 不杀 worker(tokio::spawn(job).await)。
//
// ── 三层保证(对标 原实现 TimingWheel.partition)────────────────────────────────
//   1. inflight 准入闸门 + 二次检查 started:杜绝「producer 的 send 落进将被遗弃通道」的竞态。
//   2. 不丢任务的优雅停机 shutdown().await:关闸 → 等 inflight 清零 → 通知 drain → join worker;
//      返回即「所有已准入任务跑完、所有 worker 退出」。
//   3. worker 死亡检测 + 黑洞拒收 + 健康面板:worker 异常死亡(loop panic/cancel/drop)或 send
//      失败时标记该 partition 死亡 + 告警,后续同 key submit 直接拒收(不静默滞留资金任务);
//      对外暴露 is_healthy() / dead_partitions()(对标 原实现 isHealthy / deadPartitionCount)。
//
// ── 背压(bounded + Result 提交)─────────────────────────────────────────────
//   · 通道有界(每分区默认 DEFAULT_QUEUE_CAPACITY,可 with_partitions_and_capacity 调),满不再 OOM。
//   · submit/submit_sync 非阻塞 try_send,返回 Result<(), SubmitError>:满→QueueFull(可重试/降级/告警),
//     停机→ShuttingDown,分区死→PartitionDead。资金任务被拒对调用方【可见可监控】,不再静默丢。
//   · submit_async 走 send().await 等容量腾出=真背压(生产端愿意等时用),永不 QueueFull。
//
// ── 仍未做(诚实标注)─────────────────────────────────────────────────────────
//   · 上下文透传:Rust 不做 原实现 那种 AnyHolder 隐式透传,需要的上下文请显式 capture 进闭包。
//
// ⚠️ 必须在 Tokio 运行时内构造(内部 tokio::spawn worker)。shutdown 是 async。
// ============================================================================

#![forbid(unsafe_code)]

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst; // 闸门需要 StoreLoad 顺序,统一用 SeqCst(最简单且足够强)
use std::sync::Arc;
use std::sync::Mutex; // std 同步锁:仅在 shutdown 时取一次 worker 句柄,不跨 .await 持有
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// 被投递的任务:类型擦除、堆上 Pin 住的 Send future(同 partition.rs)。
type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// 提交被拒的原因(资金任务被拒对调用方【可见】,便于重试/降级/告警,不再静默丢)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// 执行器已 shutdown / 正在停机,拒收新任务。
    ShuttingDown,
    /// 目标 partition 的 worker 已死亡(异常退出或接收端关闭),该 slot 永久拒收。
    PartitionDead,
    /// 目标 partition 队列已满(有界背压)。非阻塞 submit 专有;可退避重试或改用 submit_async 等容量。
    QueueFull,
}

impl std::fmt::Display for SubmitError {
    /// 业务作用: 把提交拒绝原因格式化为稳定文本，供调用方记录和分类处理。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubmitError::ShuttingDown => write!(f, "partition executor is shutting down"),
            SubmitError::PartitionDead => write!(f, "target partition worker is dead"),
            SubmitError::QueueFull => write!(f, "target partition queue is full"),
        }
    }
}

impl std::error::Error for SubmitError {}

/// 同 key 串行执行器(生产硬化版)。
pub struct PartitionExecutor {
    /// 各 partition 的提交端(有界)。
    txs: Vec<mpsc::Sender<Job>>,
    /// 路由掩码 = partition 数 - 1。
    mask: usize,
    /// 启停闸门:false = 拒收新任务。
    started: Arc<AtomicBool>,
    /// 在途 producer 计数:submit 期间 +1,完成 -1。stop 据此判断「还有没有 producer 正在投递」。
    inflight: Arc<AtomicI64>,
    /// 关停广播:shutdown 发 true,worker 据此 drain 残余并退出。
    shutdown_tx: watch::Sender<bool>,
    /// worker 句柄:shutdown 时取走并 join(保证「返回即所有 worker 已退出」)。
    /// 用 Option 实现幂等:take 过即为 None。
    workers: Mutex<Option<Vec<JoinHandle<()>>>>,
    /// 每个 partition 的存活标记(#1 防黑洞):worker 正常运行=true;
    /// worker 异常死亡(loop panic / 被 cancel / future 被 drop)或 send 失败时置 false。
    /// 死后该 slot 的 submit 直接拒收 + 告警, 而不是把同 key 资金任务静默滞留成黑洞。
    alive: Vec<Arc<AtomicBool>>,
    /// 已死亡 partition 计数(对外健康面板用), 对标 原实现 deadPartitions。
    dead_partitions: Arc<AtomicI64>,
    /// 停机 worker-join 上界(对标 原实现 `nasa.timing-wheel.stop-timeout-ms`,默认 2s)。
    /// shutdown 至多等这么久让 worker drain 残余;超时 → error 告警 + abort 残余 worker(避免某个
    /// 永不完成的在途任务把优雅停机永久挂死)。见 [`shutdown`]。
    stop_timeout: Duration,
}

/// 停机 worker-join 默认上界(对标 原实现 stopTimeoutMs 默认 2000ms)。
const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// 每个 partition 的有界队列默认容量。撮合场景单 key 串行,积压通常很短;取较大值兼顾突发,
/// 满则由 submit 返回 QueueFull 触发调用方背压(而非无界 OOM)。可 with_partitions_and_capacity 调整。
const DEFAULT_QUEUE_CAPACITY: usize = 65_536;

impl PartitionExecutor {
    /// 业务作用: 默认 partition 数(2 × CPU,向上取 2 的幂)+ 默认队列容量构造并启动。
    pub fn new() -> Self {
        Self::with_partitions(default_partitions())
    }

    /// 业务作用: 指定 partition 数(向上取整为 2 的幂)+ 默认队列容量构造并启动。
    ///
    /// # 参数
    /// - `n`: 调用方期望的分区数量,会至少取 1 并向上取整为 2 的幂以便位运算路由。
    pub fn with_partitions(n: usize) -> Self {
        Self::with_partitions_and_capacity(n, DEFAULT_QUEUE_CAPACITY)
    }

    /// 业务作用: 指定 partition 数 + 每分区有界队列容量构造并启动。
    ///
    /// # 参数
    /// - `n`: 分区数量(至少 1,向上取整为 2 的幂)。
    /// - `queue_capacity`: 每个 partition 的有界队列容量(至少 1)。满时非阻塞 submit 返回
    ///   [`SubmitError::QueueFull`],[`submit_async`](Self::submit_async) 则等容量腾出(真背压)。
    pub fn with_partitions_and_capacity(n: usize, queue_capacity: usize) -> Self {
        let n = n.max(1).next_power_of_two();
        let cap = queue_capacity.max(1);
        let mask = n - 1;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = Arc::new(AtomicBool::new(true));

        let dead_partitions = Arc::new(AtomicI64::new(0));
        let mut txs = Vec::with_capacity(n);
        let mut alive = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for slot in 0..n {
            let (tx, rx) = mpsc::channel::<Job>(cap);
            txs.push(tx);
            // 每 slot 一个存活标记, 一份给 worker 的死亡哨兵(DeathGuard), 一份留在 executor 供 submit 查。
            let slot_alive = Arc::new(AtomicBool::new(true));
            alive.push(slot_alive.clone());
            // 收集 JoinHandle,供 shutdown join —— 这是「优雅」相比「fire-and-forget」的关键
            handles.push(tokio::spawn(worker(
                slot,
                rx,
                shutdown_rx.clone(),
                slot_alive,
                dead_partitions.clone(),
            )));
        }

        tracing::info!("PartitionExecutor started: partitions={}", n);
        PartitionExecutor {
            txs,
            mask,
            started,
            inflight: Arc::new(AtomicI64::new(0)),
            shutdown_tx,
            workers: Mutex::new(Some(handles)),
            alive,
            dead_partitions,
            stop_timeout: DEFAULT_STOP_TIMEOUT,
        }
    }

    /// 业务作用: 设停机 worker-join 上界(对标 原实现 `stop-timeout-ms`;默认 2s)。链式:
    /// `PartitionExecutor::new().with_stop_timeout(Duration::from_secs(5))`。
    /// 调大=更耐心等 drain;调小=更快放弃挂住的在途任务。`Duration::MAX` ≈ 旧的无上界行为。
    ///
    /// # 参数
    /// - `d`: 优雅停机等待 worker drain 的最长时间,超时后会告警并 abort 残余 worker。
    pub fn with_stop_timeout(mut self, d: Duration) -> Self {
        self.stop_timeout = d;
        self
    }

    /// 业务作用: 是否健康:仍在运行 且 无死亡 partition(对标 原实现 isHealthy)。
    /// 任一 partition worker 异常死亡后返回 false,监控/oncall 据此决策。
    pub fn is_healthy(&self) -> bool {
        self.started.load(SeqCst) && self.dead_partitions.load(SeqCst) == 0
    }

    /// 业务作用: 已死亡 partition 数(0 = 全部健康),对标 原实现 deadPartitionCount。
    pub fn dead_partitions(&self) -> i64 {
        self.dead_partitions.load(SeqCst)
    }

    /// 业务作用: 把某 slot 标记为死亡(true→false 仅记一次,避免重复计数), 并告警。
    ///
    /// # 参数
    /// - `slot`: 分区执行器或 Redis partition 的槽位编号。
    fn mark_dead(&self, slot: usize) {
        if self.alive[slot].swap(false, SeqCst) {
            self.dead_partitions.fetch_add(1, SeqCst);
            tracing::error!(
                "PartitionExecutor partition {} worker gone — marked dead, further same-key submits will be rejected",
                slot
            );
        }
    }

    /// 业务作用: 非阻塞入队:映射 tokio 有界通道 try_send 结果为 SubmitError。
    /// 满 → QueueFull(非任务丢失,可背压重试);接收端关闭 → 标记死亡 + PartitionDead。
    ///
    /// # 参数
    /// - `slot`: 分区执行器或 Redis partition 的槽位编号。
    /// - `job`: 分区任务的业务执行闭包。
    fn try_enqueue(&self, slot: usize, job: Job) -> Result<(), SubmitError> {
        match self.txs[slot].try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(SubmitError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.mark_dead(slot);
                tracing::error!(
                    "PartitionExecutor partition {} send failed (closed); task rejected",
                    slot
                );
                Err(SubmitError::PartitionDead)
            }
        }
    }

    /// 业务作用: 读取 partitions 状态；用于向调用方暴露当前运行信息。
    pub fn partitions(&self) -> usize {
        self.mask + 1
    }

    /// 业务作用: 返回执行器是否仍接受任务，用于准入检查和运行状态探测。
    pub fn is_started(&self) -> bool {
        self.started.load(SeqCst)
    }

    /// 业务作用: 提交一个【异步】任务(非阻塞):同 key 严格按提交顺序串行执行,不同 key 最大并发。
    ///
    /// 返回 `Result`:被拒对调用方【可见可监控】(资金任务不静默丢)——
    ///   `Err(ShuttingDown)` 已停机;`Err(PartitionDead)` 目标分区 worker 死;`Err(QueueFull)` 队列满(可退避重试)。
    /// 队列满时**不阻塞**调用方(撮合主线程不能阻塞);要「等容量腾出」的真背压请用 [`submit_async`](Self::submit_async)。
    ///
    /// 准入闸门(不丢任务的核心):
    ///   先 inflight+1,再【二次检查】started。配合 shutdown「先关 started、再等 inflight 清零」,
    ///   保证不存在「producer 的 send 落进将被遗弃的通道」的窗口。soundness 见 [`Self::shutdown`] 注释。
    ///
    ///
    /// # 参数
    /// - `key`: 用于计算分区槽位的业务 key;同 key 保证串行。
    /// - `f`: 需要投递到目标分区串行执行的异步业务任务。
    pub fn submit<K, F, Fut>(&self, key: K, f: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // 快速短路:已关闭直接拒(省掉 inflight 抖动)
        if !self.started.load(SeqCst) {
            return Err(SubmitError::ShuttingDown);
        }
        // 进闸:占一个在途名额
        self.inflight.fetch_add(1, SeqCst);
        // 二次检查:stop 可能在 fetch_add 后才关 started;此时不再投递(让 worker 干净排空已发布任务)
        if !self.started.load(SeqCst) {
            self.inflight.fetch_sub(1, SeqCst);
            return Err(SubmitError::ShuttingDown);
        }
        let slot = self.slot_of(key);
        // #1 防黑洞:该 slot 的 worker 已死则拒收 + 告警(不静默吞同 key 资金任务)
        if !self.alive[slot].load(SeqCst) {
            tracing::error!(
                "PartitionExecutor partition {} dead; rejecting submit (avoid black hole)",
                slot
            );
            self.inflight.fetch_sub(1, SeqCst);
            return Err(SubmitError::PartitionDead);
        }
        let job: Job = Box::pin(async move { f().await });
        // 此刻 shutdown 必在等 inflight 清零(它已观测到我们的 +1)→ 通道仍有消费者(未 Closed)。
        // #2 满→QueueFull(非丢失,可背压);Closed→标记死亡+PartitionDead,均对调用方可见。
        let res = self.try_enqueue(slot, job);
        // 出闸
        self.inflight.fetch_sub(1, SeqCst);
        res
    }

    /// 业务作用: 提交一个【异步】任务并【等待队列容量】(真背压):队列满时 `await` 直到有空位,**永不** QueueFull。
    /// 适合愿意等待的异步生产端;撮合主线程等场景请用非阻塞 [`submit`](Self::submit)。
    /// 仅在停机(ShuttingDown)或目标分区 worker 死(PartitionDead)时返回 `Err`。
    ///
    /// **再入死锁警告**:不要在**本执行器的任务内部**对**同一个 key(同分区)**调用
    /// `submit_async` 并 `await`——队列满时该任务等容量、而唯一能腾容量的 worker 正在等
    /// 该任务完成,互等永久死锁。任务内派生工作请用非阻塞 [`submit`](Self::submit)
    /// (满则 QueueFull 快速失败)或提交到不同 key/独立执行器。
    ///
    /// # 参数
    /// - `key`: 用于计算分区槽位的业务 key;同 key 保证串行。
    /// - `f`: 需要投递到目标分区串行执行、并允许调用方等待队列容量的异步业务任务。
    pub async fn submit_async<K, F, Fut>(&self, key: K, f: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if !self.started.load(SeqCst) {
            return Err(SubmitError::ShuttingDown);
        }
        self.inflight.fetch_add(1, SeqCst);
        if !self.started.load(SeqCst) {
            self.inflight.fetch_sub(1, SeqCst);
            return Err(SubmitError::ShuttingDown);
        }
        let slot = self.slot_of(key);
        if !self.alive[slot].load(SeqCst) {
            tracing::error!(
                "PartitionExecutor partition {} dead; rejecting submit_async (avoid black hole)",
                slot
            );
            self.inflight.fetch_sub(1, SeqCst);
            return Err(SubmitError::PartitionDead);
        }
        let job: Job = Box::pin(async move { f().await });
        // send().await:队列满则等 worker 消费腾位(真背压)。只在接收端关闭(worker 已没)时失败。
        let res = match self.txs[slot].send(job).await {
            Ok(()) => Ok(()),
            Err(_) => {
                self.mark_dead(slot);
                tracing::error!(
                    "PartitionExecutor partition {} send failed (closed); task rejected",
                    slot
                );
                Err(SubmitError::PartitionDead)
            }
        };
        self.inflight.fetch_sub(1, SeqCst);
        res
    }

    /// 业务作用: 提交一个【同步】短任务的便捷版(非阻塞,返回语义同 [`submit`](Self::submit))。
    ///
    ///
    /// # 参数
    /// - `key`: 用于计算分区槽位的业务 key;同 key 保证串行。
    /// - `f`: 需要投递到目标分区串行执行的同步业务任务。
    pub fn submit_sync<K, F>(&self, key: K, f: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        if !self.started.load(SeqCst) {
            return Err(SubmitError::ShuttingDown);
        }
        self.inflight.fetch_add(1, SeqCst);
        if !self.started.load(SeqCst) {
            self.inflight.fetch_sub(1, SeqCst);
            return Err(SubmitError::ShuttingDown);
        }
        let slot = self.slot_of(key);
        if !self.alive[slot].load(SeqCst) {
            tracing::error!(
                "PartitionExecutor partition {} dead; rejecting submit_sync (avoid black hole)",
                slot
            );
            self.inflight.fetch_sub(1, SeqCst);
            return Err(SubmitError::PartitionDead);
        }
        let job: Job = Box::pin(async move { f() });
        let res = self.try_enqueue(slot, job);
        self.inflight.fetch_sub(1, SeqCst);
        res
    }

    /// 业务作用: 优雅停机(不丢任务)。返回时:所有已准入任务跑完、所有 worker 已退出。幂等。
    ///
    /// 步骤 & soundness:
    ///   1. `started = false`(关闸,拒新)。
    ///   2. 等 `inflight` 清零。为什么不丢任务 —— 经典 double-check(SeqCst 提供 StoreLoad 顺序):
    ///      producer 若在「+1 后的二次检查」读到 started=false → 自己 -1 退出,没 send,无损失;
    ///      若二次检查读到 started=true(发生在本步关闸之前)→ 它一定 send 成功,且它的 +1 对本步的
    ///      inflight 读可见(SeqCst 全序),故本步会等它 -1 → 不会在它 send 途中就放行。两种情形都不
    ///      存在「send 落进无人消费通道」的窗口。
    ///   3. 广播 shutdown=true。此刻 inflight=0 ⇒ 无在途 send ⇒ 通道内容已全部就位。
    ///   4. worker 收到信号后用 try_recv 把残余完全排空(无 in-flight hole),再退出。
    ///   5. join 所有 worker:返回即「真的都跑完、都退出了」。
    pub async fn shutdown(&self) {
        // 幂等:已关过就直接返回(swap 返回旧值;旧值已是 false 说明别人在关/关过了)
        if !self.started.swap(false, SeqCst) {
            return;
        }
        // 2. 等在途 producer 清零(bounded,防卡死)
        self.await_inflight_drained().await;
        // 3. 通知 worker drain 残余并退出
        let _ = self.shutdown_tx.send(true);
        // 5. 取走并 join 所有 worker(Option::take 保证只 join 一次)。
        //    ★ 上界(对标 原实现 stopTimeoutMs):用**共享 deadline** 把整个停机封顶 stop_timeout——
        //    worker 收到信号后并行 drain,这里逐个 join。若任一在途任务**永不完成**(业务 future 死循环/
        //    永远 pending),无上界 join 会让优雅停机永久挂死(资金系统真实风险)。超时 → error 告警 +
        //    **abort 残余 worker**:tokio 中 drop JoinHandle 只 detach 不 cancel,必须显式 abort。
        //    abort worker 本身只取消 worker task;在途 job 因 `run_job` 内 spawn 而独立——由 run_job 的
        //    abort-on-drop guard 在 worker future 被 drop 时连带 abort 在途 job,两者一起取消,
        //    shutdown 返回后不再有本执行器任务在后台运行。原实现 同样超时后 error 返回(必返回)。
        let handles = self.workers.lock().unwrap().take();
        if let Some(hs) = handles {
            // `Duration::MAX` 是公开 builder 明确支持的“近似无界”等待；不可直接与 Instant 相加。
            let deadline = Instant::now().checked_add(self.stop_timeout);
            for h in hs {
                let abort = h.abort_handle();
                let timed_out = if let Some(deadline) = deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    tokio::time::timeout(remaining, h).await.is_err()
                } else {
                    let _ = h.await;
                    false
                };
                // timeout 内 Ok=worker 已退出(忽略 JoinError:worker 内已 panic 隔离);Err=超时
                if timed_out {
                    tracing::error!(
                        "PartitionExecutor worker join timeout ({:?}); abandoning — 残余在途任务可能未 drain 完",
                        self.stop_timeout
                    );
                    abort.abort(); // 真正取消卡住的 worker(drop handle 不会 cancel)
                }
            }
        }
        tracing::info!("PartitionExecutor shutdown complete");
    }

    /// 业务作用: 等 inflight 归零;bounded 2s 防卡死。对 `submit`/`submit_sync`(try_send 非阻塞)在途窗口
    /// 仅「+1→try_send→-1」纳秒级,正常瞬间清零;**`submit_async` 例外**——它跨 `send().await`
    /// 持有 inflight,背压满队时可等任意久,可能触发本处 2s 超时告警(该 submit_async 随后会
    /// 因通道关闭得到 `PartitionDead`,任务不落队)。
    async fn await_inflight_drained(&self) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.inflight.load(SeqCst) > 0 {
            if Instant::now() >= deadline {
                tracing::error!(
                    "PartitionExecutor await_inflight_drained timeout (2s), {} producers still in-flight; their tasks may be lost",
                    self.inflight.load(SeqCst)
                );
                break;
            }
            // 让出执行权 + 极短退避;不空转烧核
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    }

    /// 业务作用: 根据键计算分片槽位；用于把同一键稳定路由到同一分区。
    ///
    /// # 参数
    /// - `key`: 用于哈希路由的业务 key。
    fn slot_of<K: Hash>(&self, key: K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        spread(hasher.finish()) & self.mask
    }
}

impl Default for PartitionExecutor {
    /// 业务作用: 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self::new()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// worker
// ════════════════════════════════════════════════════════════════════════════

/// worker 死亡哨兵(#1):靠 Drop 捕获 worker 的【异常退出】——
/// loop 体 panic、被 cancel、整个 worker future 被 drop,都会触发 Drop。
/// 正常退出(收到 shutdown / 通道关闭后 break)会先把 `clean=true`,Drop 不再判死。
/// 死亡时 alive true→false(仅记一次)+ dead_partitions+1 + 告警,使 submit 后续拒收该 slot。
struct DeathGuard {
    slot: usize,
    alive: Arc<AtomicBool>,
    dead: Arc<AtomicI64>,
    clean: bool,
}

impl Drop for DeathGuard {
    /// 业务作用: 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        if self.clean {
            return; // 正常停机, 不算死亡
        }
        if self.alive.swap(false, SeqCst) {
            self.dead.fetch_add(1, SeqCst);
            tracing::error!(
                "PartitionExecutor partition {} worker died abnormally (panic/cancel/drop) — marked dead to avoid black hole",
                self.slot
            );
        }
    }
}

/// 业务作用: Partition worker:顺序消费保证同 key 串行;收到关停信号后【drain 残余】再退出。
///
/// # 参数
/// - `slot`: 分区执行器或 Redis partition 的槽位编号。
/// - `rx`: 后台任务接收消息的通道。
/// - `shutdown`: 运行时关闭信号,用于通知后台任务停止。
/// - `alive`: 健康且可继续调度的节点集合。
/// - `dead`: 失联或不可调度的节点集合。
async fn worker(
    slot: usize,
    mut rx: mpsc::Receiver<Job>,
    mut shutdown: watch::Receiver<bool>,
    alive: Arc<AtomicBool>,
    dead: Arc<AtomicI64>,
) {
    // 异常退出哨兵:正常 break 后置 clean=true 解除;否则 Drop 判死。
    let mut guard = DeathGuard {
        slot,
        alive,
        dead,
        clean: false,
    };
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    // 关停时 inflight 已清零(shutdown 先等清零再发信号)→ 无在途 send →
                    // try_recv 可把通道残余完全排空,不丢任何已准入任务。
                    while let Ok(job) = rx.try_recv() {
                        run_job(slot, job).await;
                    }
                    break;
                }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(job) => run_job(slot, job).await,
                    None => break, // 所有 Sender 被 drop(未调 shutdown 而整个执行器被丢弃时的兜底)
                }
            }
        }
    }
    guard.clean = true; // 走到这里=正常退出, 解除死亡判定
    tracing::debug!("PartitionExecutor worker {} stopped", slot);
}

/// 业务作用: 执行单个任务,带 panic 隔离(spawn+await:崩溃不杀 worker,await 保串行序)。
///
/// job 经 `tokio::spawn` 成为独立 task(panic 隔离所需),但独立即 detached——若 worker 在
/// `await` 中被 `shutdown` 超时 abort,job 本身不会被取消。这里用 abort-on-drop guard 补上:
/// worker future 被 drop(abort)时连带 `abort()` 在途 job,保证 `shutdown` 返回后不再有
/// 本执行器的任务在后台运行(资金系统"停机即静默"要求)。
///
/// # 参数
/// - `slot`: 分区槽位号,仅用于日志定位。
/// - `job`: 待执行的业务任务(已被 spawn 隔离 panic)。
async fn run_job(slot: usize, job: Job) {
    /// worker 被 abort 时连带取消在途 job;正常完成路径 disarm。
    struct AbortOnDrop(Option<tokio::task::AbortHandle>);
    impl Drop for AbortOnDrop {
        /// 业务作用: guard 离开作用域时取消仍在运行的分区任务,避免停机后后台继续执行业务逻辑。
        fn drop(&mut self) {
            if let Some(h) = self.0.take() {
                h.abort();
            }
        }
    }

    let handle = tokio::spawn(job);
    let mut guard = AbortOnDrop(Some(handle.abort_handle()));
    let result = handle.await;
    guard.0 = None; // 正常路径:job 已终态,解除 abort-on-drop
    if let Err(e) = result {
        tracing::error!("PartitionExecutor partition {} job failed: {}", slot, e);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 工具
// ════════════════════════════════════════════════════════════════════════════

/// 业务作用: 返回默认分片数；用于未配置时提供稳定的分区基线。
fn default_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        * 2
}

/// 业务作用: 扩散哈希值的高低位；用于降低槽位分布偏斜。
///
/// # 参数
/// - `hash`: 原始哈希值,通常来自业务 key。
fn spread(hash: u64) -> usize {
    ((hash ^ (hash >> 16)) & 0x7fff_ffff) as usize
}
