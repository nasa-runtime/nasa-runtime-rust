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
    ///
    /// 参数说明: 无。
    ///
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
    ClaimAcquired {
        p: u32,
        guard: LockGuard,
    },
    RetainShare {
        target: usize,
    },
    ParkResolved {
        p: u32,
    },
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

/// 转换凭据持有操作身份和 owner 权威，OwnerLease 借用这些数据进行原子复验。
/// command 使用命令的稳定身份，direct API 按分区和捕获的 Park 批次派生稳定身份。
pub(super) struct OwnerFence {
    operation_id: String,
    /// 已捕获的批次必须传入每次副作用校验；空串表示捕获时没有有效批次。
    expected_park_id: Option<String>,
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：保留操作、分区及可选的捕获批次；后续副作用必须同时复验这些身份和当前权威。
    pub(super) fn lease(&self) -> super::disposition::OwnerLease<'_> {
        super::disposition::OwnerLease {
            partition: self.p,
            operation_id: &self.operation_id,
            expected_park_id: self.expected_park_id.as_deref(),
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
    ///
    /// 参数说明：
    /// - `partition`: 已确认记录所属的有效物理分区，索引与本组启动配置一致。
    /// - `id`: 已完成 ACK、可以回收正文的 Stream entry ID。
    ///
    /// 返回：禁用删除、交接成功或接收端已退出并登记正文留存时返回 true。
    /// 容量不足或通道不能受理时返回 false，原 Commit 保留责任，已预留计数全部归还。
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
        // 先占用该执行域的删除预算，再登记队列责任，避免高负载分区挤占其它域的删除容量。
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
            // 未交给唯一删除 owner 的责任仍属于原 Commit，不能留下无人归还的预算和待删计数。
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
            );
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
    ///
    /// 参数说明：
    /// - `topic`: 已选定本物理组的业务主题，写入消息信封供消费者路由。
    /// - `event`: 主题内的事件名。
    /// - `data`: 待序列化的业务正文。
    ///
    /// 返回：确认发布时返回 entry ID；准入或序列化失败不发送，发送后失败表示结果未知。
    /// 本组分区数已在启动时保证为正；计数器回绕只影响分配起点，不提供同业务键顺序保证。
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
    ///
    /// 参数说明：
    /// - `topic`: 已路由到本组的业务主题。
    /// - `event`: 主题内的事件名，供消费计划匹配。
    /// - `p`: 调用方按本组分区数计算的有效物理分区编号。
    /// - `data`: 写入信封的可序列化业务正文。
    /// - `passthrough`: 显式透传的上下文字段；None 表示没有上下文，不从任务局部状态补取。
    ///
    /// 返回：监督者确认成功时返回 ID，准入或序列化失败不发送，发送后失败表示结果未知。
    /// 发布责任由监督者持有，调用方取消等待不会撤销已经受理的发送。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：复制本地 claimed 列表；不发起 Redis 校验，列表中的分区仍须在副作用前复验持有权。
    pub(super) async fn claimed_partitions(&self) -> Vec<u32> {
        self.claimed.read().expect("claimed 锁中毒").clone()
    }

    /// 业务作用：采样本组异步删除 owner 仍持有的 entry 责任数，供排干与诊断判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已受理且尚未由删除成功或正文留存终态收口的数量；不包含尚未交接的 Commit。
    pub(super) fn async_delete_pending(&self) -> usize {
        self.async_delete_pending.load(Ordering::Acquire)
    }

    /// 业务作用：无法继续异步排干时请求停止组监督任务，避免析构后后台任务继续接纳工作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发出取消与 abort 请求后立即返回，不证明任务退出或远端副作用停止。
    /// Claim 析构停止续租；没有退出证明时锁通过租期回收，不提前释放给下一持有者。
    pub(super) fn cancel_best_effort(&self) {
        // 先通知各监督者关闭其工作循环，再请求中止；abort 本身不构成业务退出证明。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定监督集合中尚未完成的任务数，取消请求不会移除计数。
    pub(super) fn unfinished_background_tasks(&self) -> usize {
        self.bg_aborts
            .lock()
            .expect("bg_aborts")
            .iter()
            .filter(|task| !task.is_finished())
            .count()
    }

    /// 业务作用：为析构中的组资源安排异步排干，避免半完成激活留下无人管理的后台任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：协调器句柄已取走时不重复清理；否则先请求组循环停止，再在当前 Tokio runtime 中排干。
    /// 没有可用 runtime 时只请求中止并依赖租期回收；此同步函数不证明清理已完成。
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
        // 析构后不再接受管理与消费调度，已有责任由随后启动的排干任务收口。
        self.bg_cancel.cancel();
        self.cancel.cancel();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let owner = Arc::clone(self);
            runtime.spawn(async move {
                owner.shutdown().await;
            });
        } else {
            // 无运行时可承载网络排干，只能停止续租与监督，不能声称锁已安全主动释放。
            self.cancel_best_effort();
        }
    }

    /// 业务作用：完成当前组的后台、来源、删除与心跳排干，期限由外层等待者管理。
    ///
    /// 参数说明: 无。
    ///
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
    /// 业务作用：捕获本节点的分区 owner 凭据，供管理副作用在 Redis 中复验当前权威。
    ///
    /// 参数说明：
    /// - `p`: 要处置的分区编号。
    /// - `operation_id`: 本次管理操作的稳定身份，重试时应保持不变。
    ///
    /// 返回：本地没有 owner 记录时返回 None；否则返回尚未绑定 Park 批次的凭据。
    /// RustV2 携带 fence 任期与计数，LegacyV1 只携带 holder；本地记录不能替代服务端校验。
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
            expected_park_id: None,
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

    /// 业务作用：为管理命令保留已登记的操作身份，构造后续原子处置所需的 owner 凭据。
    ///
    /// 参数说明：
    /// - `p`: 命令目标分区编号。
    /// - `operation_id`: 命令入口已校验并持久化的稳定操作身份，重试必须复用原值。
    ///
    /// 返回：本地 owner 记录存在时返回凭据；否则返回 OwnershipChanged，命令交由当前 owner 处理。
    /// 返回成功仅完成本地捕获，副作用仍需在 Redis 中复验 holder、任期和处置身份。
    pub(super) fn require_owner_for_op(&self, p: u32, operation_id: &str) -> Result<OwnerFence> {
        self.owner_fence(p, operation_id.to_string())
            .ok_or_else(|| {
                NasaRedisError::OwnershipChanged(format!(
                    "分区 {p} 非本节点持有(或锁已丢失),管理命令交新 owner"
                ))
            })
    }

    /// 业务作用：为 direct API 捕获 Park 批次并绑定 owner 凭据，同批次重试复用稳定操作身份。
    ///
    /// 参数说明：
    /// - `p`: 本次管理操作所属的分区编号。
    ///
    /// 返回：读取失败或本地无 owner 时返回错误；成功凭据只允许处置捕获的批次。
    /// 缺少有效批次时保留空批次约束，后续入口拒绝缺失记录或后来出现的新批次。
    async fn require_owner_direct(&self, p: u32) -> Result<OwnerFence> {
        let park_id: Option<String> = self
            .client
            .h_get(&super::disposition::marker_key(&self.layout, p), "park_id")
            .await?;
        let op_id = match park_id.as_deref() {
            Some(pk) if !pk.is_empty() => format!("direct:{p}:{pk}"),
            _ => new_operation_id(),
        };
        // 本节点已失去 owner 记录时不能继续管理；持久消息责任留给当前持有者。
        let mut owner = self.owner_fence(p, op_id).ok_or_else(|| {
            NasaRedisError::OwnershipChanged(format!(
                "分区 {p} 非本节点持有(或锁已丢失),管理操作交新 owner"
            ))
        })?;
        // 读取与后续网络操作之间可能已经换批次，操作身份中的文本不能代替显式批次校验。
        owner.expected_park_id = Some(park_id.unwrap_or_default());
        Ok(owner)
    }

    /// 业务作用：按 direct 入口捕获的批次恢复隔离消息，持久处置完成后通知协调器解除本地冻结。
    ///
    /// 参数说明：
    /// - `p`: 本节点持有、需要恢复隔离消息的分区编号。
    ///
    /// 返回：持久恢复成功时返回新 entry ID；失权、批次冲突、结果不确定或 Redis 失败时返回错误。
    /// 处置成功后尽力投递本地通知，通知通道关闭不撤销持久结果，返回不证明协调器已处理通知。
    pub(super) async fn resume_parked(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids = super::disposition::resume(&self.client, &self.layout, p, &of.lease()).await?;
        // 必须先取得持久处置成功，再请求开放本地消费，避免恢复发布尚未完成就放行后继消息。
        let _ = self
            .event_tx
            .send(Event::ParkResolved {
                p,
            })
            .await;
        Ok(ids)
    }

    /// 业务作用：永久丢弃 direct 入口捕获的隔离批次，完成持久清理后请求协调器继续消费。
    ///
    /// 参数说明：
    /// - `p`: 本节点持有、已明确选择丢弃隔离消息的分区编号。
    ///
    /// 返回：持久丢弃成功时返回成功；失权、批次冲突或 Redis 失败时返回错误，通信失败不证明没有执行。
    /// 成功后尽力投递恢复通知；通道关闭不撤销 Dropped 终态，也不表示本地已重新开放消费。
    pub(super) async fn drop_parked(&self, p: u32) -> Result<()> {
        let of = self.require_owner_direct(p).await?;
        super::disposition::drop_parked(&self.client, &self.layout, p, &of.lease()).await?;
        // 确认旧批次已终结后才请求开放路由，丢弃失败时继续保留本地保护态。
        let _ = self
            .event_tx
            .send(Event::ParkResolved {
                p,
            })
            .await;
        Ok(())
    }

    /// 业务作用：把捕获的隔离批次转入 DLQ，持久完成后请求协调器解除本地冻结。
    ///
    /// 参数说明：
    /// - `p`: 本节点持有、需要转入 DLQ 的分区编号。
    ///
    /// 返回：确认完成时返回 DLQ entry ID；失权、批次冲突、发布不确定或 Redis 失败时返回错误。
    /// 本地恢复通知仅在持久成功后投递；通道关闭不撤销结果，返回不证明协调器已经恢复消费。
    pub(super) async fn dlq_parked(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::dlq_from_parked(&self.client, &self.layout, p, &of.lease()).await?;
        // DLQ 尚未确认完成时不能开放源分区，避免隔离责任与后续消费同时推进。
        let _ = self
            .event_tx
            .send(Event::ParkResolved {
                p,
            })
            .await;
        Ok(ids)
    }

    /// 业务作用：读取分区的持久冻结事实，供管理端区分当前隔离批次与已完成处置。
    ///
    /// 参数说明：
    /// - `p`: 要查询的分区编号，不要求当前节点持有该分区。
    ///
    /// 返回：冻结或在途处置返回批次 ID，未隔离或已终结返回 None；记录损坏和 Redis 失败返回错误。
    /// 返回值是查询时的快照，不能作为后续管理副作用的持有权或批次授权。
    pub(super) async fn parked_id(&self, p: u32) -> Result<Option<String>> {
        super::disposition::parked_id(&self.client, &self.layout, p).await
    }

    /// 业务作用：按捕获批次强制重发不确定的预约记录，显式接受重复与变序风险后请求恢复消费。
    ///
    /// 参数说明：
    /// - `p`: 本节点持有、处于 ResumeIndeterminate 或同操作 ForcePublishing 续作状态的分区。
    ///
    /// 返回：持久重发完成时返回 entry ID；状态不允许、证据损坏、失权或副作用不确定时返回错误。
    /// 本地通知在持久成功之后投递；返回成功不证明协调器已处理通知，也不消除先前发布的重复风险。
    pub(super) async fn force_republish(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::force_republish(&self.client, &self.layout, p, &of.lease()).await?;
        // 接受重发风险不授权提前开放消费，必须先完成本批次的持久发布与终态提交。
        let _ = self
            .event_tx
            .send(Event::ParkResolved {
                p,
            })
            .await;
        Ok(ids)
    }

    /// 业务作用：在外部确认原处置已停止后接管在途发布或丢弃，使用捕获批次的稳定 direct 身份续作。
    ///
    /// 参数说明：
    /// - `p`: 本节点持有、原操作已无法继续推进的分区编号。
    ///
    /// 返回：接管并完成原动作时返回发布 ID，丢弃动作返回空列表；身份变化、状态不允许或 Redis 失败返回错误。
    /// 本函数不证明原操作已经退出；调用方负责接管决策，持久成功后才尽力通知本地恢复。
    pub(super) async fn force_takeover(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::force_takeover(&self.client, &self.layout, p, &of.lease()).await?;
        // 身份移交本身不足以解除冻结，原处置已完成后才能请求协调器开放消费。
        let _ = self
            .event_tx
            .send(Event::ParkResolved {
                p,
            })
            .await;
        Ok(ids)
    }
}

/// 业务作用：为未捕获到有效 Park 批次的 direct 请求生成独立操作身份，避免与既有处置身份混用。
///
/// 参数说明: 无。
///
/// 返回：带 op- 前缀的随机 UUID 文本；生成身份不授予持有权，也不允许处置后来出现的新批次。
fn new_operation_id() -> String {
    format!("op-{}", uuid::Uuid::new_v4().simple())
}

impl GroupRuntime {
    /// 业务作用：提供本组解析后的 XREADGROUP COUNT，使读取预留与请求批量使用同一合同。
    ///
    /// 参数说明: 无。
    ///
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
