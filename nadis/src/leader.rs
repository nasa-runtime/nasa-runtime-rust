//! 基于 DistributedLock 的 Redis 租约选举；持有约定锁的节点在本地发布领导权。
//!
//! 同一任期内多次调用也会重复执行，换主后新任期同样可能重跑，业务必须自行节流和保证幂等。
//! 旧节点观察到失权之前可能与新 leader 并存，通知延迟受运行时调度影响，没有固定墙钟上界。
//! 本地取消不能撤回已发送的远端请求，需要排他写的业务必须另外使用 fencing。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::MAX_REDIS_RUNTIME_DURATION_MS;
use crate::lock::{DistributedLock, LockGuard};

/// 租约选举句柄，拥有后台任务退出责任，并提供本地领导权和任期取消信号。
pub struct Leader {
    /// 竞选循环发布的本地任期状态，不能替代远端 fencing。
    is_leader: Arc<AtomicBool>,
    /// 后台竞选任务已开始且尚未退出的观测标记。
    running: Arc<AtomicBool>,
    /// 显式停机和析构共用的不可逆停止信号。
    cancel: CancellationToken,
    /// **按任期**的失主信号:成为 leader 时换一枚新 token,失主/退位时 cancel 它。
    /// `run_if_leader_cancellable` 据此中断本地等待；已经发送的远端动作仍可能完成。
    term_lost: Arc<Mutex<CancellationToken>>,
    /// 可被一次取走等待的竞选任务，独立完成信号保留多次等待能力。
    handle: Mutex<Option<JoinHandle<()>>>,
    /// 任务退出守卫发布的完成证明，不依赖 shutdown 等待者是否仍存活。
    finished: CancellationToken,
}

/// 竞选任务的退出责任独立于显式停机路径，包含中止和 panic 展开。
struct ElectionExit {
    /// 退出时撤销本地领导权。
    is_leader: Arc<AtomicBool>,
    /// 退出时撤销后台任务运行标记。
    running: Arc<AtomicBool>,
    /// 退出时取消当前任期，唤醒协作业务。
    term_lost: Arc<Mutex<CancellationToken>>,
    /// 在任期权威撤销后发布任务退出事实。
    finished: CancellationToken,
}

impl Drop for ElectionExit {
    /// 业务作用：竞选任务退出时撤销本地任期权威，唤醒在途业务并发布退出证明。
    ///
    /// 参数说明：无。
    ///
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
    /// 业务作用：启动后台租约选举并转交退出责任，尚未取得锁时不授予本地领导权。
    ///
    /// 参数说明：
    /// - `lock`: 用于抢占 leader 互斥锁的分布式锁组件。
    /// - `key`: leader 选举使用的业务锁 key。
    /// - `period`：竞选周期，应小于锁租约；零值收敛到一毫秒，超大值收敛到组件统一时长上限。
    ///
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：竞选仍在运行且本地任期有效时为 true；不能替代外部 fencing。
    pub fn is_leader(&self) -> bool {
        self.is_running() && self.is_leader.load(Ordering::Acquire)
    }

    /// 业务作用：让宿主区分仍在运行的竞选任务和已经退出的任务，不用领导权归属代替任务健康。
    ///
    /// 参数说明：无。
    ///
    /// 返回：任务已开始且没有停机或退出时为 true；此观测不证明 Redis 可达或当前拥有租约。
    pub fn is_running(&self) -> bool {
        !self.cancel.is_cancelled() && self.running.load(Ordering::Acquire)
    }

    /// 业务作用：向宿主提供后台竞选任务的中止入口，避免宿主异常退出后继续竞选或维持租约。
    ///
    /// 参数说明：无。
    ///
    /// 返回：任务句柄尚未被取走时返回 AbortHandle，否则返回 None；发出中止不等于任务已退出或锁已释放。
    pub fn abort_handle(&self) -> Option<tokio::task::AbortHandle> {
        self.handle
            .lock()
            .expect("leader handle")
            .as_ref()
            .map(|h| h.abort_handle())
    }

    /// 业务作用：仅在调用时观测到本地领导权才执行任务，适用于自行保证幂等与排他的业务操作。
    ///
    /// 参数说明：
    /// - `f`: 仅在当前节点持有 leader 身份时执行的异步业务任务。
    ///
    /// 返回：未观测到领导权则不构造任务并返回 None，完成时返回 Some；本方法不因中途失权取消任务。
    /// 同一任期多次调用和换主后的调用都可能重复执行，不提供恰好一次保证。
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

    /// 业务作用：在本地任期内执行可取消任务，优先响应所捕获任期的失权信号并结束本地等待。
    ///
    /// 参数说明：
    /// - `f`: 持有 leader 身份时执行的异步任务,入参为失主时触发的取消信号。
    ///
    /// 返回：未取得本地任期或取消分支先就绪时返回 None，任务先完成时返回 Some。
    /// 取消受调度影响且不能撤回远端请求；业务仍须幂等并对排他副作用使用 fencing。
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

    /// 业务作用：永久停止竞选、撤销本地任期并等待后台退出，由循环尝试释放仍持有的租约。
    ///
    /// 参数说明：无。
    ///
    /// 返回：取得竞选任务退出证明后完成；解锁失败只记录日志，残留锁等待租约到期，不保证调用后其它节点立即取得锁。
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：同步提出停止请求；shutdown 等待 Redis 解锁和循环退出。
    pub fn begin_shutdown(&self) {
        self.cancel.cancel();
        self.term_lost.lock().expect("leader term_lost").cancel();
        self.is_leader.store(false, Ordering::Release);
    }
}

impl Drop for Leader {
    /// 业务作用：最后一个所有者释放时撤销本地任期，并安排后台停止以避免无人管理的竞选继续维持租约。
    ///
    /// 参数说明：无。
    ///
    /// 返回：同步提交取消；有运行时时另行等待最多两秒后请求中止，无运行时时直接请求中止。
    /// 析构不等待任务实际退出或远端锁释放。
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
///
/// 参数说明：
/// - `lock`：提供租约抢占、续期和显式释放的分布式锁组件。
/// - `key`：所有候选节点共同约定的业务锁名。
/// - `period`：未持有租约时再次尝试竞选的周期。
/// - `is_leader`：向业务发布本地观测到的领导权状态。
/// - `term_lost`：每个任期独立的取消信号，失权后通知任期工作退出。
/// - `cancel`：永久停止竞选的取消令牌。
///
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
            // 任期失效由 watch 唤醒处理，不等待下一次竞选 tick；实际失权观察仍取决于运行时调度。
            res = async {
                lost_rx.as_mut().expect("lost_rx").changed().await
            }, if lost_rx.is_some() => {
                let lost = res.is_err() || lost_rx.as_ref().map(|r| *r.borrow()).unwrap_or(true);
                if lost {
                    tracing::warn!(key = %key, "leader 租约丢失,即时退位改选");
                    is_leader.store(false, Ordering::Release);
                    end_term(&term_lost); // 中断在跑的可中断任务
                    // 旧任期守卫析构后尝试释放远端锁，不能据此承诺已经释放。
                    guard = None;
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
    // 先撤销本地任期，再尝试释放远端锁；释放失败时不能恢复领导权。
    is_leader.store(false, Ordering::Release);
    end_term(&term_lost);
    if let Some(g) = guard.take() {
        // 解锁失败保留日志并等待租约到期；这不是其它候选立即接任的证明。
        if let Err(e) = g.unlock().await {
            tracing::warn!(key = %key, err = %e, "leader 退位 unlock 失败(锁靠 lease 过期自愈,新 leader 接任延迟 ≤lease)");
        }
    }
}
