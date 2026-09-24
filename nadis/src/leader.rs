// ============================================================================
// src/leader.rs：me-leader 租约选举。
//
// **lease-based 选举,建在 DistributedLock 之上**:leader = 持有某 well-known lock key 的节点;
// 锁看门狗持续续租 = 维持领导权;leader 进程死/失联 → lease 过期 → 别的节点 try_lock 抢到 → 改选。
//
// 语义为 **leader-local-once**：
//   · 同一 leader 任内,`run_if_leader` 的副作用按调用方自己的节流执行;
//   · **换主后**新 leader 仍可能重跑同名逻辑——**不是集群范围恰好一次**;
//   · 故 leader 上跑的任务必须**幂等**(autoTrim 的 XTRIM MINID 天然幂等)。
//
// 旧 leader 观察到 lost 之前可能与新 leader 并存；通知延迟受运行时调度影响，没有固定墙钟上界。
// 本地取消不能撤回已发送的远端请求；需要排他写的业务必须另外使用 fencing。
// ============================================================================

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::MAX_REDIS_RUNTIME_DURATION_MS;
use crate::lock::{DistributedLock, LockGuard};

/// me-leader 选举句柄。后台循环周期竞选 + 维持;`is_leader()` 读当前身份。
pub struct Leader {
    is_leader: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    cancel: CancellationToken,
    /// **按任期**的失主信号:成为 leader 时换一枚新 token,失主/退位时 cancel 它。
    /// `run_if_leader_cancellable` 据此中断本地等待；已经发送的远端动作仍可能完成。
    term_lost: Arc<Mutex<CancellationToken>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    finished: CancellationToken,
}

/// 竞选任务的退出责任独立于显式停机路径，包含中止和 panic 展开。
struct ElectionExit {
    is_leader: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    term_lost: Arc<Mutex<CancellationToken>>,
    finished: CancellationToken,
}

impl Drop for ElectionExit {
    /// 业务作用：竞选任务退出时撤销本地任期权威，唤醒在途业务并发布退出证明。
    /// 参数说明：无。
    /// 返回：不再接纳任期工作；已有工作收到取消后停止本地等待，远端副作用仍须 fencing。
    fn drop(&mut self) {
        // 任期信号必须先于退出证明发布，避免宿主已观察到任务结束而旧工作仍继续使用领导权。
        self.is_leader.store(false, Ordering::Release);
        self.running.store(false, Ordering::Release);
        self.term_lost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancel();
        self.finished.cancel();
    }
}

impl Leader {
    /// 业务作用：启动后台选举循环。`key` 是业务 key(`DistributedLock` 会加锁前缀);`period` = 竞选/检测周期
    /// 默认 1 秒，应小于 lock.lease_ms 以便及时改选；返回后开始竞选。
    ///
    /// 参数说明：
    /// - `lock`: 用于抢占 leader 互斥锁的分布式锁组件。
    /// - `key`: leader 选举使用的业务锁 key。
    /// - `period`: 竞选和检测周期,应小于锁 lease。
    /// 返回：持有后台任务退出责任的句柄；竞选尚未成功时不授予领导权。
    pub fn elect(
        lock: Arc<DistributedLock>,
        key: impl Into<String>,
        period: Duration,
    ) -> Arc<Self> {
        let key = key.into();
        // `tokio::time::interval` 最终会把 period 加到 Instant；极端 Duration 可溢出并 panic。
        // 该既有构造器不能返回配置错误，因此把 0 收敛为 1ms、超大值收敛到统一的一年上限。
        let period = period.clamp(
            Duration::from_millis(1),
            Duration::from_millis(MAX_REDIS_RUNTIME_DURATION_MS),
        );
        let is_leader = Arc::new(AtomicBool::new(false));
        let running = Arc::new(AtomicBool::new(false));
        let cancel = CancellationToken::new();
        // 初始非 leader:term token 起手即 cancelled(run_if_leader_cancellable 不会误判在任)。
        let term_lost = Arc::new(Mutex::new({
            let t = CancellationToken::new();
            t.cancel();
            t
        }));
        let finished = CancellationToken::new();
        // 在 spawn 前建立退出责任，任务尚未首次轮询就被中止时也必须发布完成信号。
        let completion = ElectionExit {
            is_leader: is_leader.clone(),
            running: running.clone(),
            term_lost: term_lost.clone(),
            finished: finished.clone(),
        };
        let election = election_loop(
            lock,
            key,
            period,
            Arc::clone(&is_leader),
            Arc::clone(&term_lost),
            cancel.clone(),
        );
        let handle = tokio::spawn(async move {
            let _completion = completion;
            _completion.running.store(true, Ordering::Release);
            election.await;
        });
        Arc::new(Self {
            is_leader,
            running,
            cancel,
            term_lost,
            handle: Mutex::new(Some(handle)),
            finished,
        })
    }

    /// 业务作用：当前是否为 leader(无 Redis 往返,读本地标志)。
    /// 参数说明：无。
    /// 返回：竞选仍在运行且本地任期有效时为 true；不能替代外部 fencing。
    pub fn is_leader(&self) -> bool {
        self.is_running() && self.is_leader.load(Ordering::Acquire)
    }

    /// 业务作用：让宿主区分仍在运行的竞选任务和已经退出的任务，不用领导权归属代替任务健康。
    /// 参数说明：无。
    /// 返回：任务已开始且没有停机或退出时为 true；此观测不证明 Redis 可达或当前拥有租约。
    pub fn is_running(&self) -> bool {
        !self.cancel.is_cancelled() && self.running.load(Ordering::Acquire)
    }

    /// 业务作用：内层 election 任务的 AbortHandle(供宿主把它纳入统一 abort 集——否则宿主
    /// (如 autotrim)被 abort 时 `shutdown()` 不执行、election_loop 成孤儿、leader 锁看门狗继续续租,
    /// 阻塞新 leader 接任至 lease 过期)。`None` = 已 shutdown(handle 被 take)。
    pub fn abort_handle(&self) -> Option<tokio::task::AbortHandle> {
        self.handle
            .lock()
            .expect("leader handle")
            .as_ref()
            .map(|h| h.abort_handle())
    }

    /// 业务作用：leader 才执行 `f`(否则跳过)。leader-local-once:换主后新 leader 仍可能重跑,`f` 须幂等。
    /// ⚠ **不中断**:进入后 `f` 一跑到底,即便中途失主也不打断(长任务请用 `run_if_leader_cancellable`)。
    ///
    /// # 参数
    /// - `f`: 仅在当前节点持有 leader 身份时执行的异步业务任务。
    pub async fn run_if_leader<F, Fut, T>(&self, f: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        if self.is_leader() {
            Some(f().await)
        } else {
            None
        }
    }

    /// 业务作用：leader 才执行 `f`,且**失主即中断**。`f` 收到本任期的 `CancellationToken`
    /// (失主/退位时触发),可据此协作退出;同时本方法在 `f` 与失主信号间 `select`——一旦失主,
    /// 不等 `f` 完成直接返回 `None`；通知依赖运行时调度，不承诺固定时间内停止远端副作用。
    /// 返回 `None` = 非 leader 或执行中失主;`Some(T)` = 任内跑完。`f` 仍应幂等(换主后新 leader 会重跑)。
    ///
    /// # 参数
    /// - `f`: 持有 leader 身份时执行的异步任务,入参为失主时触发的取消信号。
    pub async fn run_if_leader_cancellable<F, Fut, T>(&self, f: F) -> Option<T>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        if !self.is_leader() {
            return None;
        }
        let tok = self.term_lost.lock().expect("leader term_lost").clone();
        // 进入瞬间可能已失主(token 已 cancelled)→ 直接返回 None。
        if tok.is_cancelled() {
            return None;
        }
        tokio::select! {
            biased;
            _ = tok.cancelled() => None,
            out = f(tok.clone()) => Some(out),
        }
    }

    /// 业务作用：显式退位 + 停后台循环:cancel → 等循环退出(退出时 unlock leader 锁,别的节点可立即接任)。
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        // 完成信号独立于等待者；等待 future 被取消后，后续 owner 仍能取得真实退出证明。
        self.finished.cancelled().await;
        let h = self.handle.lock().expect("leader handle").take();
        if let Some(h) = h {
            let _ = h.await;
        }
    }

    /// 业务作用：永久停止竞选准入并立刻撤销本地任期权威。
    /// 参数说明：无。
    /// 返回：同步提出停止请求；shutdown 等待 Redis 解锁和循环退出。
    pub fn begin_shutdown(&self) {
        self.cancel.cancel();
        self.term_lost.lock().expect("leader term_lost").cancel();
        self.is_leader.store(false, Ordering::Release);
    }
}

impl Drop for Leader {
    /// 业务作用：最后一个 owner 未显式 shutdown 时立即停止竞选任务，避免锁看门狗继续续租。
    fn drop(&mut self) {
        self.cancel.cancel();
        self.term_lost.lock().expect("leader term_lost").cancel();
        let handle = self.handle.lock().expect("leader handle").take();
        if let Some(mut handle) = handle {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                // 让 election_loop 观察 cancel 并走显式 unlock；只 abort 会直接 drop LockGuard，
                // 退化为另起任务的 best-effort unlock，不能保证在 lease 到期前释放。
                runtime.spawn(async move {
                    if tokio::time::timeout(Duration::from_secs(2), &mut handle)
                        .await
                        .is_err()
                    {
                        handle.abort();
                        let _ = handle.await;
                    }
                });
            } else {
                handle.abort();
            }
        }
        self.is_leader.store(false, Ordering::Release);
    }
}

/// 业务作用：周期竞选 Redis 租约，并在租约失效或停机时撤销当前任期。
/// 参数说明：`lock` 为分布式锁；`key` 为业务锁名；`period` 为竞选周期；
/// `is_leader` 发布本地身份；`term_lost` 通知任期工作退出；`cancel` 请求停止竞选。
/// 返回：停机时等待一次显式解锁；异常退出由外层退出守卫撤销本地权威。
async fn election_loop(
    lock: Arc<DistributedLock>,
    key: String,
    period: Duration,
    is_leader: Arc<AtomicBool>,
    term_lost: Arc<Mutex<CancellationToken>>,
    cancel: CancellationToken,
) {
    // 成为 leader:换一枚未取消的新 term token(供 run_if_leader_cancellable 观察本任期)。
    let begin_term = |term_lost: &Mutex<CancellationToken>| {
        *term_lost.lock().expect("term_lost") = CancellationToken::new();
    };
    // 失主/退位:cancel 当前 term token(唤醒在跑的可中断任务)。
    let end_term = |term_lost: &Mutex<CancellationToken>| {
        term_lost.lock().expect("term_lost").cancel();
    };

    let mut guard: Option<LockGuard> = None;
    //成为 leader 时持有**一个** lost receiver 复用,不再每 tick `lost()` 新建 receiver。
    let mut lost_rx: Option<tokio::sync::watch::Receiver<bool>> = None;
    let mut tick = tokio::time::interval(period.max(Duration::from_millis(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            //leader 时**即时退位**——监听 lost watch 变化,失主即退,不再等满一个
            // period 才在下一 tick 发现(把双主窗口从 ≤1 选举 period 压回 watch 唤醒延迟)。非 leader 时
            // (lost_rx=None)该分支用 `if` 关掉。watch 已变(sender drop / 置 true)→ changed() 立即返回。
            res = async { lost_rx.as_mut().expect("lost_rx").changed().await }, if lost_rx.is_some() => {
                let lost = res.is_err() || lost_rx.as_ref().map(|r| *r.borrow()).unwrap_or(true);
                if lost {
                    tracing::warn!(key = %key, "leader 租约丢失,即时退位改选");
                    is_leader.store(false, Ordering::Release);
                    end_term(&term_lost); // 中断在跑的可中断任务
                    guard = None; // drop → best-effort unlock
                    lost_rx = None;
                }
            }
            _ = tick.tick() => {
                // 非 leader:周期竞选(try_lock 抢 well-known key)。leader 时 tick 无操作(失主由上方即时分支处理)。
                if guard.is_none() {
                    match lock.try_lock(&key).await {
                        Ok(Some(g)) => {
                            tracing::info!(key = %key, "竞选成功,成为 leader");
                            begin_term(&term_lost);
                            lost_rx = Some(g.lost()); // 本任期复用同一 receiver
                            is_leader.store(true, Ordering::Release);
                            guard = Some(g);
                        }
                        Ok(None) => is_leader.store(false, Ordering::Release), // 他人持有
                        Err(e) => {
                            is_leader.store(false, Ordering::Release);
                            tracing::warn!(key = %key, err = %e, "竞选 try_lock 失败,下轮重试");
                        }
                    }
                }
            }
        }
    }
    // 退出:显式退位(unlock leader 锁,别的节点可立即接任,不必等 lease 过期)
    is_leader.store(false, Ordering::Release);
    end_term(&term_lost);
    if let Some(g) = guard.take() {
        //退位 unlock 失败不再静默吞——记 warn(与 lock 超时路径一致);
        // 失败时 leader 锁靠 lease 过期自愈,新 leader 接任会延迟 ≤lease,留日志便于排障。
        if let Err(e) = g.unlock().await {
            tracing::warn!(key = %key, err = %e, "leader 退位 unlock 失败(锁靠 lease 过期自愈,新 leader 接任延迟 ≤lease)");
        }
    }
}
