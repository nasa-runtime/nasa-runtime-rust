// ============================================================================
// src/leader.rs：me-leader 租约选举。
//
// **lease-based 选举,建在 DistributedLock 之上**:leader = 持有某 well-known lock key 的节点;
// 锁看门狗持续续租 = 维持领导权;leader 进程死/失联 → lease 过期 → 别的节点 try_lock 抢到 → 改选。
//
// 语义为 **leader-local-once**，对齐既有 `execIfIsMeOnce` 的 ONCE_CON 行为：
//   · 同一 leader 任内,`run_if_leader` 的副作用按调用方自己的节流执行;
//   · **换主后**新 leader 仍可能重跑同名逻辑——**不是集群范围恰好一次**;
//   · 故 leader 上跑的任务必须**幂等**(autoTrim 的 XTRIM MINID 天然幂等)。
//
// 短暂双主窗口:旧 leader 观察到 lost 之前(≤ 一个 tick)与新 leader 可能并存——lease 选举固有,
// leader-local-once 已接受;对幂等任务无害。
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
    cancel: CancellationToken,
    /// **按任期**的失主信号:成为 leader 时换一枚新 token,失主/退位时 cancel 它。
    /// `run_if_leader_cancellable` 据此在长任务执行中途**及时中断**,把双主窗口从"整个 f 时长"压回"≤1 tick"。
    term_lost: Arc<Mutex<CancellationToken>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Leader {
    /// 业务作用：启动后台选举循环。`key` 是业务 key(`DistributedLock` 会加锁前缀);`period` = 竞选/检测周期
    /// (对照 原实现 `clusterMePeriod` 1s,应 < lock.lease_ms 以便及时改选)。返回即开始竞选。
    ///
    /// # 参数
    /// - `lock`: 用于抢占 leader 互斥锁的分布式锁组件。
    /// - `key`: leader 选举使用的业务锁 key。
    /// - `period`: 竞选和检测周期,应小于锁 lease。
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
        let cancel = CancellationToken::new();
        // 初始非 leader:term token 起手即 cancelled(run_if_leader_cancellable 不会误判在任)。
        let term_lost = Arc::new(Mutex::new({
            let t = CancellationToken::new();
            t.cancel();
            t
        }));
        let handle = tokio::spawn(election_loop(
            lock,
            key,
            period,
            Arc::clone(&is_leader),
            Arc::clone(&term_lost),
            cancel.clone(),
        ));
        Arc::new(Self {
            is_leader,
            cancel,
            term_lost,
            handle: Mutex::new(Some(handle)),
        })
    }

    /// 业务作用：当前是否为 leader(无 Redis 往返,读本地标志)。
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
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
    /// 不等 `f` 完成直接返回 `None`,把"双主并发跑长任务"窗口从整个 `f` 时长压回 ≤1 tick。
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
        self.cancel.cancel();
        let h = self.handle.lock().expect("leader handle").take();
        if let Some(h) = h {
            let _ = h.await;
        }
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

// Runs periodic leader election until cancellation.
///
/// # 参数
/// 业务作用：- `lock`: Redis 分布式锁的持有状态。
/// - `key`: leader 选举使用的 Redis lock key。
/// - `period`: 任务调度周期。
/// - `is_leader`: 当前实例是否仍拥有 leader 身份。
/// - `term_lost`: leader 任期是否已经失效。
/// - `cancel`: 后台任务使用的取消信号。
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
