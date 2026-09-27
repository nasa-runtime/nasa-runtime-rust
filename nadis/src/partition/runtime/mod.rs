//! 一个物理组的来源控制与共享消费运行时接线。
//!
//! 每个来源只有一个读取监督者，全部组通过共享账本提交到各自执行域的 napart Runner。
//! 业务成功与 Redis 确认分离；停止根准入后，既有任务仍能移交成功提交责任。
//! 来源读取、业务与提交全部收口后才能释放锁。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use futures::FutureExt;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::KeyLayout;
use super::{command, disposition, fencing, retryop};
use crate::client::RedisClient;
use crate::config::PartitionCfg;
use crate::error::{NasaRedisError, Result};
use crate::lock::{DistributedLock, LockGuard};

/// 业务作用：使监督任务在全部组的启动事务提交后才接触 Redis，未激活回滚可以直接退出。
///
/// 参数说明：
/// - `gate`: 全部组共用的一次性发布信号。
/// - `cancel`: 当前任务所属生命周期的停止信号。
/// - `core`: 共享消费运行时，监督者异常结束时关闭根准入。
/// - `task`: 已登记句柄但尚未开始 poll 的任务。
///
/// 返回：取消先到达时丢弃尚未执行的任务；激活先到达时交给原任务负责正常停止。
async fn run_when_activated(
    gate: CancellationToken,
    cancel: CancellationToken,
    core: Arc<engine::RedisPartitionRuntime>,
    task: impl std::future::Future<Output = ()>,
) {
    // 取消优先于同轮到达的激活，避免回滚任务越过已经关闭的准入。
    tokio::select! {
        biased;
        _ = cancel.cancelled() => return,
        _ = gate.cancelled() => {}
    }
    let result = std::panic::AssertUnwindSafe(task).catch_unwind().await;
    if result.is_err() || !cancel.is_cancelled() {
        // 必需监督者停止后不能继续接受新责任；既有票据仍留在注册表供停机核对。
        core.degraded.store(true, Ordering::Release);
        core.close_roots();
    }
}

mod identity;
use identity::BatchIdentity;
pub(super) mod engine;
mod source;
mod wire;
pub use engine::{ExecutionDomainSnapshot, PartitionSnapshot};

/// 一个持锁期的来源控制资源，不拥有业务 mailbox。
struct ClaimSlot {
    source: Arc<engine::SourceAuthority>,
    guard: Option<LockGuard>,
    reader: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ClaimSlot {
    /// 业务作用：异常退出时撤销权威并停止续租，缺少 join 证明时不主动解锁。
    /// 参数说明: 无。
    /// 返回：请求读者停止；服务端租约自行到期，析构不宣告排干成功。
    fn drop(&mut self) {
        self.source.lose();
        if let Some(reader) = self.reader.take() {
            reader.abort();
            if let Some(rt) = self.source.group.upgrade() {
                rt.core.abandoned_reader(reader);
            }
        }
        if let Some(guard) = self.guard.take() {
            if let Some(rt) = self.source.group.upgrade() {
                rt.core.abandoned_source(&self.source);
            }
            guard.abandon();
        }
    }
}

struct AsyncDeleteBatch {
    partition: u32,
    ids: Vec<String>,
}

enum Event {
    ClaimAcquired { p: u32, guard: LockGuard },
    RetainShare { target: usize },
    ParkResolved { p: u32 },
}

// ─────────────────────────────────────────────────────────────────────────
// GroupRuntime:一个分区组的全部运行时资产
// ─────────────────────────────────────────────────────────────────────────

/// 保存 GroupRuntime 运行状态；用于管理后台任务和共享资源。
pub struct GroupRuntime {
    pub(super) client: Arc<RedisClient>,
    pub(super) lock: Arc<DistributedLock>,
    pub(super) layout: KeyLayout,
    pub(super) count: u32,
    pub(super) cfg: PartitionCfg,
    /// 按本组覆盖解析后的读取批量、轮询周期、读取并发与异步删除参数快照。
    pub(super) stream_cfg: crate::config::StreamCfg,
    node_id: String,
    /// 来源取得、让出与人工处置的有界事件入口。
    event_tx: mpsc::Sender<Event>,
    /// 来源控制停止信号，触发关闭读取并等待本地责任排干。
    cancel: CancellationToken,
    /// 后台任务停机令牌(rebalance/wake/control/sweep 用**独立**令牌,
    /// 停机时**先 cancel 后台任务并等其退出、coordinator 仍持锁**,再 cancel coordinator
    /// drain+unlock——避免 control(resume/drop/dlq)/sweep(XAUTOCLAIM)在失锁后继续改 Redis)。
    bg_cancel: CancellationToken,
    /// ACK 后异步 XDEL 的发送端。`None` 表示配置为 0，保留 entry 给 autoTrim 处理。
    async_delete_tx: Option<mpsc::Sender<AsyncDeleteBatch>>,
    /// 同步串行化删除交接与 owner 退出，避免关闭接收端时把未受理预留计入终态。
    async_delete_admission: std::sync::Mutex<Vec<usize>>,
    /// 删除监督保留到全部提交交接完成，才能执行最终 flush。
    async_delete_cancel: CancellationToken,
    /// 排干期间主动刷新删除积压，不让配置周期延迟 Commit 的删除责任交接。
    async_delete_flush: tokio::sync::Notify,
    /// 异步 XDEL owner 的唯一 join 句柄，普通停机等待其最终 flush。
    async_delete_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 异步 XDEL owner 当前仍待删除的 entry 数；供运行时 gauge 抓取，不参与消费裁决。
    async_delete_pending: AtomicUsize,
    /// 诊断:当前持有分区列表(coordinator 维护副本)。
    claimed: Arc<std::sync::RwLock<Vec<u32>>>,
    /// round-robin 发布计数器；partition=null 时全局轮转均摊。
    rr_counter: AtomicU64,
    /// 启动阶段冻结的 (topic,event) 消费计划，所有来源只读共享。
    plans: super::plan::PlanMap,
    pub(super) core: Arc<engine::RedisPartitionRuntime>,
    pub(super) publisher: Arc<super::publisher::PublisherCoordinator>,
    /// V2 fencing 运行参数(profile==RustV2 时 start 内 bootstrap 后注入;
    /// LegacyV1 = None,ACK 走 holds 双检查 + 裸 XACK 的 V1 协议)。
    fence_meta: Option<super::fencing::FenceMeta>,
    /// 再平衡 single-flight 守卫(周期 rebalance 与 wake rebalance 都直接调
    /// rebalance_once,无保护时会并发心跳/扫描/抢锁;try_lock
    /// 拿不到即跳过,合并并发触发)。
    rebalance_lock: tokio::sync::Mutex<()>,
    /// 管理任务的退出证明，来源解锁前必须等待它们停止 Redis 副作用。
    bg_tracker: tokio_util::task::TaskTracker,
    /// 显式强停使用的监督取消入口，普通等待超时不调用这些句柄。
    bg_aborts: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
    /// 来源控制唯一 join 句柄，即使显式强停也保留给排干操作等待。
    coordinator_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 组资源关闭单飞，防止多个清理入口重复取得同一 join 句柄。
    shutdown_lock: tokio::sync::Mutex<()>,
    /// 本节点当前持有分区的 owner 凭据:coordinator 在 ClaimAcquired
    /// 写入 `(holder, counter)`、释放/失锁时移除。管理路径据此构造 owner-fenced 转换凭据，非 owner / 失锁后 owner_ctx 无该
    /// 分区 → 拒绝;即便残留,fenced Lua 还会校验 holder 持锁 + fence 任期 counter 二次兜底。
    pub(super) owner_ctx: std::sync::Mutex<std::collections::HashMap<u32, OwnerCtx>>,
}

/// 分区 owner 凭据(holder + 当前任期 counter;counter 仅 RustV2 fence 有效)。
pub(super) struct OwnerCtx {
    pub holder: String,
    pub counter: u64,
}

/// 构造 owner-fenced 转换凭据所需的**自有**数据(OwnerLease 借用它)。`operation_id` 是本次
/// 管理操作的稳定身份:command 路径用命令的 operation_id,direct API 每次生成一个新的。
pub(super) struct OwnerFence {
    operation_id: String,
    holder: String,
    lock_key: String,
    fence_key: String,
    round: u64,
    nonce: String,
    p: u32,
    counter: u64,
}

impl OwnerFence {
    /// 业务作用：返回当前 owner 的借用式租约视图，供后续 fencing 操作复验权威。
    pub(super) fn lease(&self) -> super::disposition::OwnerLease<'_> {
        super::disposition::OwnerLease {
            partition: self.p,
            operation_id: &self.operation_id,
            holder: &self.holder,
            lock_key: &self.lock_key,
            fence_key: &self.fence_key,
            round: self.round,
            nonce: &self.nonce,
            counter: self.counter,
        }
    }
}

impl GroupRuntime {
    /// 业务作用：非阻塞交接一条已确认记录给有界删除 owner，失败时原 Commit 继续持有责任。
    /// 参数说明：`partition` 为 Stream 分区；`id` 为已经确认 ACK 的记录 ID。
    /// 返回：禁用删除或交接成功返回 true；容量不足不改变待删计数。
    pub(super) fn try_enqueue_async_delete(&self, partition: u32, id: &str) -> bool {
        let Some(tx) = &self.async_delete_tx else {
            return true;
        };
        let mut counts = self
            .async_delete_admission
            .lock()
            .expect("delete admission");
        if self.core.roots_closed.load(Ordering::Acquire) {
            self.async_delete_flush.notify_one();
        }
        if tx.is_closed() {
            // 删除 owner 已退出时不再等待不可达的接收端，确认事实保持终态并报告正文留存。
            self.core
                .delete_retained_records
                .fetch_add(1, Ordering::AcqRel);
            return true;
        }
        let domain = &self.core.executions.domains
            [self.core.executions.domain(&self.layout.prefix, partition)];
        if domain
            .delete_records
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(1)
                    .filter(|next| *next <= domain.spec.quota.deletes)
            })
            .is_err()
        {
            return false;
        }
        counts[partition as usize] += 1;
        self.core
            .async_delete_records
            .fetch_add(1, Ordering::AcqRel);
        self.async_delete_pending.fetch_add(1, Ordering::AcqRel);
        let result = tx
            .try_send(AsyncDeleteBatch {
                partition,
                ids: vec![id.to_string()],
            })
            .is_ok();
        if !result {
            counts[partition as usize] -= 1;
            domain.delete_records.fetch_sub(1, Ordering::AcqRel);
            self.async_delete_pending.fetch_sub(1, Ordering::AcqRel);
            self.core
                .async_delete_records
                .fetch_sub(1, Ordering::AcqRel);
        }
        result
    }

    /// 业务作用：校验并启动一个分区组的 claim、调度和消费任务，返回拥有全部后台任务的运行时句柄。
    ///
    /// 参数说明：
    /// - `client`: 当前代理共享的 Redis 客户端。
    /// - `lock`: 分区锁与续租组件。
    /// - `layout`: 当前组的固定 Redis key 布局。
    /// - `count`: 组内物理分区数。
    /// - `plans`: 已校验的不可变消费计划。
    /// - `core`: 所有组共享的消费执行域、账本和容量。
    /// - `publisher`: 所有组共享的发布监督者。
    /// - `node_id`: 全组共享的本次进程 incarnation。
    /// - `cfg`: 解析组覆盖后的分区策略。
    /// - `stream_cfg`: 解析组覆盖后的读取与删除策略。
    /// - `start_gate`: 完整启动事务提交前保持关闭的共享消费屏障。
    ///
    /// 返回：fencing 就绪且全部监督句柄登记后返回 dormant 组；bootstrap 失败不启动消费任务。
    #[allow(clippy::too_many_arguments)] // 多组:layout/count/cfg/stream_cfg 都 per-group,内部入口不另封包
    pub(super) async fn start(
        client: Arc<RedisClient>,
        lock: Arc<DistributedLock>,
        layout: KeyLayout,
        count: u32,
        plans: super::plan::PlanMap,
        core: Arc<engine::RedisPartitionRuntime>,
        publisher: Arc<super::publisher::PublisherCoordinator>,
        node_id: String,
        cfg: PartitionCfg,
        // 调用方按组覆盖优先、全局配置兜底解析，保持各组读取和删除策略独立。
        stream_cfg: crate::config::StreamCfg,
        start_gate: CancellationToken,
    ) -> Result<Arc<GroupRuntime>> {
        // 事件通道容量 = claims×2 + 控制余量；发送端始终等待容量，容量只影响吞吐而不承担正确性。
        // 该上限同时避免极端配置把 channel 元数据本身放大。
        let (event_tx, event_rx) = mpsc::channel::<Event>(count as usize * 2 + 16);
        // 删除 ID 同时受共享记录预算与通道容量约束；队列满时 Commit 保留交接责任。
        let (async_delete_tx, async_delete_rx) = if stream_cfg.async_del_record_period_ms == 0 {
            (None, None)
        } else {
            let capacity = stream_cfg.inflight_max.clamp(1, 4_096);
            let (tx, rx) = mpsc::channel(capacity);
            (Some(tx), Some(rx))
        };

        // V2 fencing bootstrap(profile 驱动,:LegacyV1 无 fence 概念)
        let fence_meta = if matches!(
            client.profile(),
            crate::config::CompatibilityProfile::RustV2
        ) {
            let tmp_layout = layout.clone();
            Some(super::fencing::bootstrap(&client, &tmp_layout, 1).await?)
        } else {
            None
        };

        let rt = Arc::new(GroupRuntime {
            client,
            lock,
            layout,
            count,
            cfg,
            stream_cfg,
            node_id,
            event_tx,
            cancel: CancellationToken::new(),
            bg_cancel: CancellationToken::new(),
            async_delete_tx,
            async_delete_admission: std::sync::Mutex::new(vec![0; count as usize]),
            async_delete_cancel: CancellationToken::new(),
            async_delete_flush: tokio::sync::Notify::new(),
            async_delete_handle: std::sync::Mutex::new(None),
            async_delete_pending: AtomicUsize::new(0),
            claimed: Arc::new(std::sync::RwLock::new(Vec::new())),
            rr_counter: AtomicU64::new(0),
            plans,
            core,
            publisher,
            fence_meta,
            rebalance_lock: tokio::sync::Mutex::new(()),
            bg_tracker: tokio_util::task::TaskTracker::new(),
            bg_aborts: std::sync::Mutex::new(Vec::new()),
            coordinator_handle: std::sync::Mutex::new(None),
            shutdown_lock: tokio::sync::Mutex::new(()),
            owner_ctx: std::sync::Mutex::new(std::collections::HashMap::new()),
        });
        // 来源监督句柄先登记，再开放启动屏障；普通停机等待真实 join。
        let coord = tokio::spawn(run_when_activated(
            start_gate.clone(),
            rt.cancel.clone(),
            rt.core.clone(),
            coordinator_loop(Arc::clone(&rt), event_rx),
        ));
        rt.bg_aborts
            .lock()
            .expect("owner aborts")
            .push(coord.abort_handle());
        *rt.coordinator_handle.lock().expect("coordinator_handle") = Some(coord);
        // 管理副作用纳入统一监督，来源解锁前必须取得后台任务退出证明。
        {
            let mut aborts = rt.bg_aborts.lock().expect("bg_aborts");
            aborts.push(
                rt.bg_tracker
                    .spawn(run_when_activated(
                        start_gate.clone(),
                        rt.bg_cancel.clone(),
                        rt.core.clone(),
                        rebalance_loop(Arc::clone(&rt)),
                    ))
                    .abort_handle(),
            ); // 心跳+再平衡
            aborts.push(
                rt.bg_tracker
                    .spawn(run_when_activated(
                        start_gate.clone(),
                        rt.bg_cancel.clone(),
                        rt.core.clone(),
                        wake_loop(Arc::clone(&rt)),
                    ))
                    .abort_handle(),
            ); // wake 订阅
            aborts.push(
                rt.bg_tracker
                    .spawn(run_when_activated(
                        start_gate.clone(),
                        rt.bg_cancel.clone(),
                        rt.core.clone(),
                        control_loop(Arc::clone(&rt)),
                    ))
                    .abort_handle(),
            ); // 管理命令
            aborts.push(
                rt.bg_tracker
                    .spawn(run_when_activated(
                        start_gate.clone(),
                        rt.bg_cancel.clone(),
                        rt.core.clone(),
                        sweep_loop(Arc::clone(&rt)),
                    ))
                    .abort_handle(),
            ); // orphan sweep
            if rt.stream_cfg.auto_trim_rate_ms != 0 {
                aborts.push(
                    rt.bg_tracker
                        .spawn(run_when_activated(
                            start_gate.clone(),
                            rt.bg_cancel.clone(),
                            rt.core.clone(),
                            auto_trim_loop(Arc::clone(&rt)),
                        ))
                        .abort_handle(),
                );
            }
        }
        // 删除监督保留到全部提交交接完成，才能执行最终 flush。
        if let Some(rx) = async_delete_rx {
            let handle = tokio::spawn(run_when_activated(
                start_gate,
                rt.async_delete_cancel.clone(),
                rt.core.clone(),
                async_delete_loop(Arc::clone(&rt), rx),
            ));
            rt.bg_aborts
                .lock()
                .expect("owner aborts")
                .push(handle.abort_handle());
            *rt.async_delete_handle.lock().expect("async_delete_handle") = Some(handle);
        }

        Ok(rt)
    }

    /// 业务作用：激活消费前幂等复验每条 Stream 和 consumer group，阻止 prepare 后远端拓扑变化被忽略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部分区合同存在或已补齐时成功；错误类型和失败原因保留给启动回滚。
    pub(super) async fn ensure_stream_contract(&self) -> Result<()> {
        for partition in 0..self.count {
            let result: redis::RedisResult<String> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(self.layout.stream(partition))
                .arg(self.layout.group())
                .arg("0")
                .arg("MKSTREAM")
                .query_async(&mut self.client.conn())
                .await;
            match result {
                Ok(_) => {}
                Err(error) if error.code() == Some("BUSYGROUP") => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// 业务作用：无物理路由键时按组内 round-robin 选择分区，交给发布监督者执行 XADD。
    /// 参数说明：`topic`、`event` 为路由；`data` 为可序列化业务正文。
    /// 返回：成功返回 entry id；发送后失败返回结果未知，取消等待不撤销发布责任。
    pub(super) async fn publish_round_robin<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        data: &T,
    ) -> Result<String> {
        let p = (self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.count as u64) as u32;
        self.publish(topic, event, p, data, None).await
    }

    /// 业务作用：把业务 payload 发布到指定分区 stream，并返回服务端生成的 entry id。
    /// 参数说明：`topic`、`event` 为路由；`p` 为物理分区；`data` 为正文；`passthrough` 为显式上下文。
    /// 返回：监督者确认成功时返回 id，准入或序列化失败不发送，发送后失败返回结果未知。
    pub(super) async fn publish<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        p: u32,
        data: &T,
        passthrough: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<String> {
        self.publisher
            .publish(
                self.client.clone(),
                self.layout.stream(p),
                topic,
                event,
                data,
                passthrough,
            )
            .await
    }

    /// 业务作用：返回当前运行时已经取得所有权的分区快照。
    pub(super) async fn claimed_partitions(&self) -> Vec<u32> {
        self.claimed.read().expect("claimed 锁中毒").clone()
    }

    /// 业务作用：返回本组已进入异步删除管道但尚未确认 XDEL 成功的 entry 数。
    pub(super) fn async_delete_pending(&self) -> usize {
        self.async_delete_pending.load(Ordering::Acquire)
    }

    /// 业务作用：未走异步 drain 时的最后防线。
    ///
    /// 关闭准入并请求监督任务取消；Claim 析构仅停止续租，缺少退出证明时不主动解锁。
    pub(super) fn cancel_best_effort(&self) {
        self.bg_cancel.cancel();
        self.cancel.cancel();
        self.async_delete_cancel.cancel();
        for abort in self.bg_aborts.lock().expect("bg_aborts").iter() {
            abort.abort();
        }
        if let Some(handle) = self
            .coordinator_handle
            .lock()
            .expect("coordinator_handle")
            .as_ref()
        {
            handle.abort();
        }
        if let Some(handle) = self
            .async_delete_handle
            .lock()
            .expect("async_delete_handle")
            .as_ref()
        {
            handle.abort();
        }
    }

    /// 业务作用：枚举尚未终结的组监督者，使等待超时报告包含管理与来源控制责任。
    /// 参数说明: 无。
    /// 返回：固定监督集合中尚未完成的任务数，取消请求不会移除计数。
    pub(super) fn unfinished_background_tasks(&self) -> usize {
        self.bg_aborts
            .lock()
            .expect("bg_aborts")
            .iter()
            .filter(|task| !task.is_finished())
            .count()
    }

    /// 业务作用：`RunningPartition::drop` 使用的异步清理入口。
    ///
    /// 先同步关闭 admission，再由当前 Tokio runtime 执行与显式 shutdown 相同的 drain/unlock；
    /// 若已离开 runtime，只能退回立即 abort + lease 兜底。
    pub(super) fn shutdown_on_drop(self: &Arc<Self>) {
        // 显式 shutdown 已取走 coordinator handle，Drop 不再重复启动第二轮网络清理。
        if self
            .coordinator_handle
            .lock()
            .expect("coordinator_handle")
            .is_none()
        {
            return;
        }
        self.bg_cancel.cancel();
        self.cancel.cancel();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let owner = Arc::clone(self);
            runtime.spawn(async move {
                owner.shutdown().await;
            });
        } else {
            self.cancel_best_effort();
        }
    }

    /// 业务作用：完成当前组的后台、来源、删除与心跳排干，期限由外层等待者管理。
    /// 参数说明: 无。
    /// 返回：真实 join 完成后返回；普通等待超时不会取消此操作或提前释放锁。
    pub(super) async fn shutdown(&self) {
        let _guard = self.shutdown_lock.lock().await;
        self.bg_cancel.cancel();
        self.bg_tracker.close();
        self.bg_tracker.wait().await;
        // 管理与恢复副作用停止后，来源才能等待记录收口并释放锁。
        self.cancel.cancel();
        let handle = self.coordinator_handle.lock().expect("coordinator").take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        self.async_delete_cancel.cancel();
        let handle = self
            .async_delete_handle
            .lock()
            .expect("async delete")
            .take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        let _ = self
            .client
            .z_rem(&self.layout.nodes(), self.node_id.as_str())
            .await;
        let _: redis::RedisResult<i64> = redis::cmd("PUBLISH")
            .arg(self.layout.wake())
            .arg("")
            .query_async(&mut self.client.conn())
            .await;
    }
}

// ─────────────────────────────────────────────────────────────────────────
// coordinator:调度状态机(只做调度/Redis poll/demux,不 await 业务)
// ─────────────────────────────────────────────────────────────────────────

impl GroupRuntime {
    /// 业务作用：构造分区 owner-fenced 转换凭据:本节点非该分区 owner(owner_ctx 无记录)→
    /// None,调用方据此拒绝管理操作。RustV2 带 fence 三元组 + 任期 counter;LegacyV1 fence_key 空,
    /// 只校验 holder 持锁。
    pub(super) fn owner_fence(&self, p: u32, operation_id: String) -> Option<OwnerFence> {
        let oc = self.owner_ctx.lock().expect("owner_ctx");
        let c = oc.get(&p)?;
        let (fence_key, round, nonce) = match &self.fence_meta {
            Some(fm) => (
                super::fencing::fence_key(&self.layout),
                fm.round,
                fm.nonce.clone(),
            ),
            None => (String::new(), 0, String::new()),
        };
        Some(OwnerFence {
            operation_id,
            holder: c.holder.clone(),
            lock_key: format!(
                "{}{}",
                self.client.config().lock.prefix,
                self.layout.lock_business_key(p)
            ),
            fence_key,
            round,
            nonce,
            p,
            counter: c.counter,
        })
    }

    /// 业务作用：command 路径专用:用命令的稳定 operation_id 构造凭据(:同 op_id 重试幂等续作)。
    pub(super) fn require_owner_for_op(&self, p: u32, operation_id: &str) -> Result<OwnerFence> {
        self.owner_fence(p, operation_id.to_string())
            .ok_or_else(|| {
                NasaRedisError::OwnershipChanged(format!(
                    "分区 {p} 非本节点持有(或锁已丢失),管理命令交新 owner"
                ))
            })
    }

    /// 业务作用：direct API 的 owner 凭据:op_id **稳定派生自 (p, marker.park_id)**
    /// 同一宗 Park 的 direct 重试复用同一 op_id，使崩溃重入与 marker 中的操作身份一致。
    /// 缺少 park_id 时生成新 UUID，后续处置仍须验证 Park 记录存在才允许转换。
    ///
    /// # 参数
    /// - `p`: 分区编号或当前协议步骤中的短名参数。
    async fn require_owner_direct(&self, p: u32) -> Result<OwnerFence> {
        let park_id: Option<String> = self
            .client
            .h_get(&super::disposition::marker_key(&self.layout, p), "park_id")
            .await?;
        let op_id = match park_id {
            Some(pk) if !pk.is_empty() => format!("direct:{p}:{pk}"),
            _ => new_operation_id(),
        };
        self.owner_fence(p, op_id).ok_or_else(|| {
            //owner 不在 = OwnershipChanged(非业务拒绝)——保留 PEL 交新 owner
            NasaRedisError::OwnershipChanged(format!(
                "分区 {p} 非本节点持有(或锁已丢失),管理操作交新 owner"
            ))
        })
    }

    /// 业务作用：resume 后通知 coordinator 恢复消费(Parked → Ready)。
    pub(super) async fn resume_parked(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids = super::disposition::resume(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }

    /// 业务作用：drop 后通知 coordinator 恢复消费(后续新消息照常)。
    pub(super) async fn drop_parked(&self, p: u32) -> Result<()> {
        let of = self.require_owner_direct(p).await?;
        super::disposition::drop_parked(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(())
    }

    /// 业务作用：dlq 处置(owner-fenced;RunningPartition::dlq_parked 经此)。
    pub(super) async fn dlq_parked(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::dlq_from_parked(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }

    /// 业务作用：查询分区 Park 状态(None = 未 Park)。
    pub(super) async fn parked_id(&self, p: u32) -> Result<Option<String>> {
        super::disposition::parked_id(&self.client, &self.layout, p).await
    }

    /// 业务作用：ForceRepublish(仅 ResumeIndeterminate;显式接受重复/变序风险)成功后恢复消费。
    pub(super) async fn force_republish(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::force_republish(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }

    /// 业务作用：通用 liveness-takeover：原 op 已死时强制接管卡死的在途 *Publishing/Dropping。
    /// 用稳定 `direct:{p}:{park_id}` op 覆写后续作——崩溃重入用同 op 续作幂等。
    pub(super) async fn force_takeover(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::force_takeover(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }
}

/// 业务作用：生成一个管理操作的 operation_id(direct API 用)。
fn new_operation_id() -> String {
    format!("op-{}", uuid::Uuid::new_v4().simple())
}

impl GroupRuntime {
    /// 业务作用：提供本组解析后的 XREADGROUP COUNT，使读取预留与请求批量使用同一合同。
    /// 参数说明: 无。
    /// 返回：本组 stream.batch_size，启动校验保证其为正且能预留完整批次。
    pub(super) fn cfg_batch(&self) -> usize {
        self.stream_cfg.batch_size
    }
}

// ─────────────────────────────────────────────────────────────────────────
// 不可变来源读取配置
// ─────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────
// 再平衡:心跳 + fair 配额 + 抢占/让出
// ─────────────────────────────────────────────────────────────────────────

// ── 子模块（各文件精确 import，无 use super::* 之外的
//    blanket allow——)──
mod asyncdel;
mod autotrim;
mod control;
#[path = "claim.rs"]
mod coordinator;

mod rebalance;

use asyncdel::*;
use autotrim::*;
use control::*;
use coordinator::*;

use rebalance::*;
