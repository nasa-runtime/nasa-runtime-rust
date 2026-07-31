// ============================================================================
// src/partition/runtime.rs —— 分区运行时:PollCoordinator / Claim / Worker / 再平衡。
// (架构设计文档 的 R4.1 子集;对照 原实现 ManagedRunner + PollLifecycle)
//
// 核心不变量(每条都对应文档审定结论):
//   1. 【串行】同一分区同时最多一个在途批次:ClaimState 含 generation,worker mailbox
//      容量=1,前一批 WorkFinished 前该分区不再进 poll(原实现 complete=false 同义);
//   2. 【背压】拉取前 batch permit(Semaphore=max_inflight_batches):拿不到 permit 的
//      Ready 分区本轮不拉——消息留在 Redis,不拉进进程内存
//   3. 【NOBLOCK 合批】所有 Ready 分区合成一条多 stream XREADGROUP(NOBLOCK,连接瞬借);
//      poll_timeout 只是空轮后的冷流 Backoff 间隔,不是 BLOCK 参数
//   4. 【fencing】业务调用前 + ACK 前 holds 双检查(V1 协议;Unknown/Lost 一律不 ACK);
//   5. 【失败重投走 PEL】HandlerFailed → RetryBackoff{ids}(不持 permit),到期先
//      try_acquire permit 再逐 ID `XPENDING 精确查 owner + XCLAIM 0`(ExactTargetRetry
//      的无 marker 简化版;Unknown 对账的 retry-op marker = R4.2),重投期间禁 `>`;
//   6. 【再平衡】心跳 ZADD(墙钟 now+3*period,V1)→ 清过期 → fair=ceil(count/alive);
//      欠额 tryLock 抢占,超额释放(释放后才 pub wake);wake 订阅只是加速,周期兜底;
//   7. 【停机】关 admission → 等 in-flight 归零 → 逐分区 unlock → ZREM 心跳 → pub wake。
//
// ┌─ 实现状态清单(读源码前先看这里;每条标注文档 出处)──────────────┐
// │ ✅ 已实现(R4.1)                                                            │
// │   串行不变量/ batch permit 拉取前准入          │
// │   NOBLOCK 合批 poll + demux                      │
// │   holds 双检查 fencing,Unknown 不 ACK                  │
// │   RetryBackoff 不持 permit + XCLAIM 0 逐 ID PEL 重投 │
// │   再平衡:墙钟心跳/fair/抢占/让出/wake                      │
// │   停机:admission→drain→unlock→摘心跳→wake               │
// │   generation 过滤迟到 WorkFinished                  │
// │ ✅ 已实现(R4.2a,本轮)                                                     │
// │   PoisonPolicy { Drop, Park, Dlq }(config 可配,**默认 Park** fail-closed)  │
// │   disposition marker(HASH,Parking/Parked/Resumed/Dropped 子集)│
// │   quarantine 确定性 HASH {tag}:quarantine:{park_id}            │
// │   fenced park Lua(校验 holder→转存→XACK 源→写 marker,恢复协议雏形)      │
// │   管理 API:list_parked / resume / drop_parked(单 owner 进程内直调)        │
// │ ✅ 已实现(R4.2b,本轮)                                                     │
// │   retry-op marker CAS 五规则(retryop.rs:意图先落盘,重放幂等不调低 count) │
// │   resume planned-ID reservations(disposition.rs:显式 ID + 重入判定表)     │
// │   ResumeIndeterminate 持久态 + force_republish 审计链               │
// │   AckTicket 语义【隐式满足】:worker 在 process_batch 内重试 ACK 期间,      │
// │     WorkBatch(含 permit)未释放、ClaimState 维持 InFlight → ACK 收敛天然    │
// │     占预算且被停机 drain 等待——独立 AckTicket 结构留待 ACK 与 worker 解耦   │
// │     的场景(当前串行语义下无收益)                                           │
// │ ✅ 已实现(R4.2c,本轮)                                                     │
// │   management command outbox/result key(command.rs:定向 stream + admin      │
// │     group;control loop 独立于 Parked——修 Resume 自锁;at-least-once +      │
// │     result 去重;Redis TIME deadline;先 result 后 ACK+XDEL)                │
// │   attempt 权威值 = max(本地 attempt, PEL delivery count)             │
// │ ✅ 已实现(R4.2d,本轮)                                                     │
// │   PoisonPolicy::Dlq + CmdAction::Dlq(= fenced Park 转存 + planned-ID 发布   │
// │     到 {prefix}:dlq;中途崩溃回退 Parked 可管理态)                           │
// │   V2 FencingStamp/bootstrap(fencing.rs:全局三态 Initializing→Activating→  │
// │     Ready + per-tag fence HASH 五规则 + per-partition counter stamp +        │
// │     **原子 fenced ACK Lua**——堵住 V1 双检查与 XACK 之间的失锁窗口;          │
// │     profile 驱动:RustV2 走 fenced Lua,原实现V1 仍 holds 双检查+裸 XACK)     │
// │   bootstrap owner TTL lease + owner_term 接管(fencing.rs:已实现单 owner    │
// │     推进 + 接管递增 owner_term;多进程并发抢 lease 的完整对账仍待 R4.2e+)     │
// │ ✅ 已修复                                 │
// │ 新 owner 接管 Recovering + XAUTOCLAIM(0)收编残留 PEL,完成前禁 `>`    │
// │ 管理命令基础设施错误保留 PEL 重试(CmdAck::Retry),不再误 ACK+XDEL    │
// │ quarantine 缺 payload fail-closed(协议损坏错误,禁空字节替代)        │
// │ Park Lua 写前 TYPE 预检(消除 WRONGTYPE 半提交)                       │
// │ drain_timeout_ms 绝对 deadline 生效(超时强制取消 worker+释锁)        │
// │ ConnectionManager 自动重连(client.rs)                               │
// │ stream.batch_size/poll_timeout_ms/inflight_max 接线;count 上限 16384 │
// │ release 候选只 Ready/Backoff; wake 在 unlock 之后; rebalance │
// │     single-flight                                                           │
// │ ✅ 已修复                                       │
// │ 丢锁后停 poll(看门狗 lost watch 检测移除 Claim)+ 周期 orphan sweep   │
// │ retry-op Pending 绑定 ID 集身份(集合不一致即弃用重算,防吞 ID)       │
// │ parked index 读失败 fail-closed; worker process_batch 期间监听    │
// │     cancel + 停机 await worker 退出再回 ack                                  │
// │   P1 错误分类(WRONGTYPE 等不重试)/ parser fail-closed / 假配置标注 / 注释   │
// │ ✅ 已修复                                 │
// │   P0 异构 PEL **逐 ID 毒消息判定**(effective(id)=max(attempt, pel_count(id)),│
// │     poison/retryable 分组处置,不再用批次最大值连坐 Drop/Park/DLQ 正常消息)  │
// │   P0 接管读 **disposition marker(权威)+ index** 归约(marker=Parked 而 index │
// │     缺失不再当未 Park 消费;不一致 fail-closed 放弃接管)                      │
// │   P1 shutdown 单一绝对 deadline(一次 cancel + 并发 join + 剩余预算 unlock)   │
// │   P1 orphan sweep 移出 coordinator 主循环 → 后台 sweep_loop actor(多页+错误  │
// │     可见,经 Event::OrphansClaimed 回灌);retry-op 覆盖语义订正(非 fail-closed)│
// │ ✅ 已修复        │
// │   P0 正常 poll 批次**子集结果**(process_batch 逐 ID 分桶;坏信封/失败桶只影响 │
// │     本桶 IDs,ACK 成功子集——不再整批连坐 Drop 正常消息)                       │
// │   P0 retry-op ID 集不一致改**合并保留旧 desired**(非覆盖重算,杜绝提前 Drop)  │
// │   P0 ParkResolved → Recovering(先 XAUTOCLAIM 收编 retryable,不直接 Ready 越过)│
// │   P0 marker 缺失+index 存在 / 非终态缺 park_id → Inconsistent fail-closed;     │
// │     parked() API 改以 marker 为权威                                          │
// │   P1 shutdown 端到端绝对 deadline(删隐式宽限,全程 timeout_at);后台任务入     │
// │     TaskTracker 停机 deadline 内 wait                                        │
// │ ✅ 已修复        │
// │   P0 业务类型 T 解码逐 ID(ErasedHandler 收 Vec<(id,Value)> 返失败子集;坏 T/ │
// │     handler 失败/桶内 panic 只影响相关 ID,不连坐整桶/整批)                   │
// │   P0 poison 以**实际 PEL count** 判定(不信 marker desired);count<desired-1  │
// │     = 损坏 → 冻结整分区不 Drop/Park/DLQ                                       │
// │   P0 停机拆 cancel/bg_cancel:先停后台任务(持锁期内)再 coordinator drain+unlock│
// │   诊断 parked() 损坏态返回 Err(复用 takeover_disposition,不误报 None)        │
// │ ✅ 顺序模型对齐 原实现(文档)                                            │
// │   worker 批内分桶改**首见插入序**(Vec 替 HashMap,对齐 原实现 RecycleLinkedMap)│
// │   → per-(topic,event) FIFO + 同 key 跨事件首见序;单消费 task(mailbox=1)串行 │
// │ ✅ 补完未实现项(文档)                                                 │
// │   handler 强制 timeout(handler_timeout_ms;超时整桶留 PEL)                   │
// │   EntryDeleted 墓碑 fenced ACK(retry RESOLVED 清 PEL 残留)                   │
// │   shutdown 单一传入 deadline + 后台任务 abort_all(超 deadline)              │
// │   sweep SweepTicket generation(只扫 Ready/Backoff,应用前校验 generation)    │
// │ ✅ retry-op operation_id 键控(文档 Phase 1):marker 按 op_id 键控,      │
// │   消除"陈旧/损坏 marker 串扰别宗";finish 删 marker + TTL 兜底  │
// │ ✅ 已补齐(文档)                                                      │
// │   disposition marker version + transition_operation_id;bootstrap owner TTL │
// │   lease/owner_term 接管;Cluster 同槽布局;EntryDeleted fenced ACK;leader     │
// │   autoTrim + ACK 后有界异步 XDEL(停机末次 flush)                            │
// │ ⬜ 后续扩展(不影响当前正确性)                                              │
// │   单组跨多个 Cluster slot 的 PollDomain 分片(当前整个组固定在一个 slot);    │
// │   独立 GroupConsumer lease 抽象;按 control/poll/fencing 等角色拆更多连接 lane│
// └──────────────────────────────────────────────────────────────────────┘
// ============================================================================

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{Envelope, HandlerMap, KeyLayout, DATA_FIELD};
// 同级模块 re-export(R2 拆分修正):runtime 子文件(coordinator/worker/retry/...)
// 中的 `super::fencing::X` / `super::disposition::X` 等路径,在拆进 runtime/ 子目录后
// super=runtime;把 partition 的这些兄弟模块引入 runtime 命名空间,子文件经
// `super::<mod>` 即解析到此别名(子模块可见父私有 use)。
#[allow(unused_imports)]
use super::{command, disposition, fencing, retryop};
use crate::client::RedisClient;
use crate::config::PartitionCfg;
use crate::error::{NasaRedisError, Result};
use crate::lock::{DistributedLock, LockGuard};

/// ACK 不确定态的 worker 内有限重试次数。
///
/// XACK 幂等，返回值可从 1 收敛到 0；当前串行 worker 在重试期间继续持有批次 permit 和
/// `InFlight` 状态，因此停机会等待该确认过程。只有未来把 ACK 与 worker 解耦时才需要独立 ticket。
const ACK_RETRY: u32 = 3;

// ─────────────────────────────────────────────────────────────────────────
// 状态机与事件
// ─────────────────────────────────────────────────────────────────────────

/// Claim 状态(coordinator 独占写;generation = 本地单调,过滤迟到事件)。
enum ClaimState {
    /// 可进 `>` poll。
    Ready,
    /// 新 owner 接管后正在收编旧 owner 残留 PEL——完成前**禁 `>`**,
    /// 否则旧消息永远读不到(`>` 不返回 PEL)。XAUTOCLAIM(min_idle=0)把 PEL 收到本
    /// consumer,完成后转 RetryBackoff 走 permit 门控的重投/毒消息路径;失败保持本态退避。
    Recovering { generation: u64 },
    /// 已派发给 worker,等 WorkFinished(同义 原实现 complete=false)。
    InFlight { generation: u64 },
    /// 空轮冷流:到期才再进 poll(poll_timeout 语义)。
    Backoff { until: tokio::time::Instant },
    /// 失败批次待重投:**不持 permit**,到期先拿 permit 再走 PEL 路径;期间禁 `>`。
    RetryBackoff {
        ids: Vec<String>,
        attempt: u32,
        until: tokio::time::Instant,
    },
    /// 毒消息已 Park:停止一切 poll(持锁续租,等管理 resume/drop)。
    /// 顺序承诺在此让位于人工介入——该分区后续消息也被冻结(这正是 Park 的语义)。
    Parked {
        /// 诊断携带(resume/drop 经 Redis 索引取 park_id,本字段仅 Debug/日志用)。
        #[allow(dead_code)]
        park_id: String,
    },
}

/// 分区子任务的异常路径 owner。
///
/// Tokio `JoinHandle` 被直接丢弃会 detach 任务；coordinator 若被外层 deadline 强制 abort，
/// 整个 `slots` map 会直接 drop，不能再执行正常 drain。该包装保证这种路径至少 abort worker /
/// recovery，禁止 handler 或 XAUTOCLAIM 在运行时句柄消失后继续执行。
struct AbortOnDropTask(Option<tokio::task::JoinHandle<()>>);

impl AbortOnDropTask {
    /// 接管一个必须在 owner drop 时结束的任务。
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(handle))
    }

    /// 正常 drain 路径取回 join handle；取出后由调用方负责 await 或 abort。
    fn take(&mut self) -> tokio::task::JoinHandle<()> {
        self.0.take().expect("claim task handle 只能取一次")
    }

    /// 异常所有权丢失路径立即 abort；重复调用无副作用。
    fn abort(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

impl Drop for AbortOnDropTask {
    /// owner 未走正常 drain 时禁止任务 detach。
    fn drop(&mut self) {
        self.abort();
    }
}

/// 一个被本节点持有的分区。
struct ClaimSlot {
    state: ClaimState,
    /// 分区锁守卫(unlock 时整个 slot 退场)。
    guard: LockGuard,
    /// 串行 worker 的投递口(容量 1:状态机已保证同时只发一批,满 = bug)。
    work_tx: mpsc::Sender<WorkBatch>,
    /// worker task 的取消令牌(release/停机时取消;worker 在 process_batch 期间亦监听)。
    worker_cancel: CancellationToken,
    /// worker task 句柄(停机需 await 其**真正退出**再回 ack,
    /// 协作取消超时则 abort——不能只 cancel 完就返回,留 task 在后台跑 handler)。
    worker_handle: AbortOnDropTask,
    /// 本地单调代数:每次进 InFlight 自增,WorkFinished 必须带相同值才被接受。
    generation: u64,
    /// 本地连续失败计数(Acked 清零，作为 RetryBackoff 的 attempt 输入)。
    ///
    /// 毒消息判定不会单独信任该值；重投路径逐 ID 读取 PEL delivery count，并取两者最大值。
    retry_attempt: u32,
    /// 本任期 fencing stamp(V2;None=原实现V1)。coordinator 侧的 fenced 操作
    /// (poison Drop 的 ACK 等)与 worker 共用同一任期凭据。
    stamp: Option<super::fencing::FencingStamp>,
    /// PEL recovery 任务句柄(此前裸 spawn detached,JoinHandle 未保存——
    /// 释锁/让出/停机只 cancel token 不等其退出,旧 recovery 的 XAUTOCLAIM 可在解锁后才在服务端
    /// 改 PEL owner。现在存 slot:优雅 drain/release **await 它退出再 unlock**,失锁则 abort)。
    recovery_handle: Option<AbortOnDropTask>,
}

/// 投给 worker 的一批消息(permit 随批移交,处理完随 WorkFinished 释放)。
struct WorkBatch {
    generation: u64,
    /// (entry_id, envelope JSON 文本)。
    records: Vec<(String, Vec<u8>)>,
    /// 全局背压预算(drop 即归还 Semaphore)。
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// ACK 成功后交给异步删除 owner 的一批 entry。
///
/// 队列按批次有界；发送端在队列满时等待，因此不会为了优化 stream 空间而静默丢失待删 ID。
struct AsyncDeleteBatch {
    partition: u32,
    ids: Vec<String>,
}

/// worker → coordinator 的终态(事件可重复,转移按 generation 幂等)。
enum WorkOutcome {
    /// 全批已 fenced ACK。
    Acked,
    /// handler/decode 失败:ids 留 PEL,转 RetryBackoff。
    Failed { ids: Vec<String> },
    /// fencing 判定所有权丢失:slot 转 Lost(释放本地资源,锁已不属于我们)。
    LostOwnership,
}

/// coordinator 统一事件通道(单 event_rx)。
enum Event {
    WorkFinished {
        p: u32,
        generation: u64,
        outcome: WorkOutcome,
    },
    /// 再平衡任务抢到新分区(锁已持有,移交 coordinator 建 slot)。
    ClaimAcquired { p: u32, guard: LockGuard },
    /// 新 owner PEL 接管恢复完成:ids = 收编到本 consumer 的全部
    /// 残留 PEL 条目(含 tombstone),交 RetryBackoff 后续处置;generation 过滤迟到事件。
    RecoveryFinished {
        p: u32,
        generation: u64,
        ids: Vec<String>,
    },
    /// 后台 orphan sweep 收编到滞留 PEL(sweep 的 Redis I/O 移出
    /// coordinator 主循环,改后台 actor 经本事件回灌)。coordinator 仅在 slot 仍 Ready/Backoff
    /// **且 generation 匹配**时应用,否则忽略(丢锁移除/在途/已 Parked/换代 → 作废)。
    OrphansClaimed {
        p: u32,
        generation: u64,
        ids: Vec<String>,
    },
    /// 再平衡要求释放 n 个分区(超额)。
    ReleaseExcess { n: usize },
    /// 管理 API 完成 resume/drop:Parked → Ready(恢复消费)。
    ParkResolved { p: u32 },
    /// 停机屏障：coordinator 完成受管 slot 收口后通过 `done` 确认，shutdown 才能继续释放资源。
    Shutdown { done: oneshot::Sender<()> },
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
    cfg: PartitionCfg,
    /// stream 运行参数快照(batch_size / poll_timeout_ms / inflight_max
    /// 接线于此,不再硬编码 100 / count / holds_check_interval)。
    pub(super) stream_cfg: crate::config::StreamCfg,
    node_id: String,
    /// coordinator 事件入口(publish 不经此;仅 worker/rebalance/shutdown)。
    event_tx: mpsc::Sender<Event>,
    /// coordinator 停机令牌(coordinator/worker drain 用)。
    cancel: CancellationToken,
    /// 后台任务停机令牌(rebalance/wake/control/sweep 用**独立**令牌,
    /// 停机时**先 cancel 后台任务并等其退出、coordinator 仍持锁**,再 cancel coordinator
    /// drain+unlock——避免 control(resume/drop/dlq)/sweep(XAUTOCLAIM)在失锁后继续改 Redis)。
    bg_cancel: CancellationToken,
    /// ACK 后异步 XDEL 的发送端。`None` 表示配置为 0，保留 entry 给 autoTrim 处理。
    async_delete_tx: Option<mpsc::Sender<AsyncDeleteBatch>>,
    /// 异步 XDEL 独立于常规后台任务停机：它必须活到 coordinator 排空 worker 之后，才能接住
    /// 最后一批已确认 ID，再执行末次 flush。
    async_delete_cancel: CancellationToken,
    /// 异步 XDEL owner 的唯一任务句柄；shutdown 取出并等待，超出统一 deadline 时 abort。
    async_delete_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 异步 XDEL owner 当前仍待删除的 entry 数；供运行时 gauge 抓取，不参与消费裁决。
    async_delete_pending: AtomicUsize,
    /// 诊断:当前持有分区列表(coordinator 维护副本)。
    claimed: Arc<std::sync::RwLock<Vec<u32>>>,
    /// 发布序号(诊断用)。
    pub_seq: AtomicU64,
    /// round-robin 发布计数器(F6:partition=null 时全局轮转均摊)。
    rr_counter: AtomicU64,
    /// (topic,event) → handler 路由表(start 时一次性注入,此后只读——
    /// coordinator 与各 worker 共享查表,无锁)。
    handlers_cell: std::sync::OnceLock<HandlerMap>,
    /// V2 fencing 运行参数(profile==RustV2 时 start 内 bootstrap 后注入;
    /// 原实现V1 = None,ACK 走 holds 双检查 + 裸 XACK 的 V1 协议)。
    fence_meta: Option<super::fencing::FenceMeta>,
    /// 再平衡 single-flight 守卫(周期 rebalance 与 wake rebalance 都直接调
    /// rebalance_once,无保护时会并发心跳/扫描/抢锁并重复发 ReleaseExcess;try_lock
    /// 拿不到即跳过,合并并发触发)。
    rebalance_lock: tokio::sync::Mutex<()>,
    /// 后台任务跟踪(rebalance/wake/control/sweep 不能只 detached——
    /// 它们会改 Redis(抢锁/XAUTOCLAIM/管理副作用),停机必须 cancel 后**在 deadline 内
    /// 等其退出**)。配合 `bg_aborts`:wait 超 deadline 仍未退出(挂在 Redis I/O)→ abort_all。
    bg_tracker: tokio_util::task::TaskTracker,
    /// 后台任务的 AbortHandle(TaskTracker.wait() 超时不 abort,挂在 Redis I/O
    /// 的任务要强制 abort——存其 AbortHandle,停机 deadline 到则逐个 abort)。
    bg_aborts: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
    /// coordinator 唯一 join 句柄。正常停机必须 await 它真正退出；超 deadline 时 abort 后再
    /// join，避免只丢 `JoinHandle` 把仍持有 Claim/worker 的任务 detach。
    coordinator_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 停机单飞。`RunningPartition::shutdown` 接收 `&self`，同一实例可能被多个宿主清理路径并发调用；
    /// 若不串行，多个 Shutdown 事件会互相覆盖 oneshot 确认，后来的调用还可能提前停 asyncDel。
    shutdown_lock: tokio::sync::Mutex<()>,
    /// 停机绝对 deadline(**shutdown 入口生成一次**,coordinator drain 读同一个
    /// 值,不再各自以 cfg.drain_timeout_ms 起算——避免 coordinator 卡在 Redis I/O 时 deadline
    /// 比 API deadline 晚秒级)。
    shutdown_deadline: std::sync::Mutex<Option<tokio::time::Instant>>,
    /// sweep 票据(**只对 Ready/Backoff 分区生成**,带 claim generation)。
    /// coordinator 每轮发布,sweep_loop 据此扫描——**不再扫 InFlight/Recovering/RetryBackoff/
    /// Parked**(避免对它们 XAUTOCLAIM 改 PEL owner/idle);收编结果带 generation 回校验。
    pub(super) sweepable: std::sync::Mutex<Vec<(u32, u64)>>,
    /// 本节点当前持有分区的 owner 凭据:coordinator 在 ClaimAcquired
    /// 写入 `(holder, counter)`、释放/失锁时移除。管理路径(direct API / control loop)在 worker
    /// **之外**调 disposition,据此构造 owner-fenced 转换凭据——非 owner / 失锁后 owner_ctx 无该
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
    /// Returns a lease view for the owned partitions.
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
    /// 校验并启动一个分区组的 claim、调度和消费任务，返回拥有全部后台任务的运行时句柄。
    #[allow(clippy::too_many_arguments)] // 多组:layout/count/cfg/stream_cfg 都 per-group,内部入口不另封包
    pub(super) async fn start(
        client: Arc<RedisClient>,
        lock: Arc<DistributedLock>,
        layout: KeyLayout,
        count: u32,
        handlers: HandlerMap,
        node_id: String,
        cfg: PartitionCfg,
        // **per-group resolved StreamCfg**(多组:不再读全局,由调用方按组 override→全局 resolve 后传入)。
        stream_cfg: crate::config::StreamCfg,
    ) -> Result<Arc<GroupRuntime>> {
        // 事件通道容量:claims×2 + 控制余量( 容量公式的 R4.1 取值;
        // 发送端一律 send().await,容量只影响吞吐不作正确性依据)
        let (event_tx, event_rx) = mpsc::channel::<Event>(count as usize * 2 + 16);
        // 队列按在飞批次预算定界；额外封顶避免极端配置把 channel 元数据本身放大。
        // 每个已确认批次在完成前仍持有 inflight permit，因此正常路径不会产生无界发送者。
        let (async_delete_tx, async_delete_rx) = if stream_cfg.async_del_record_period_ms == 0 {
            (None, None)
        } else {
            let capacity = stream_cfg.inflight_max.clamp(1, 4_096);
            let (tx, rx) = mpsc::channel(capacity);
            (Some(tx), Some(rx))
        };

        // V2 fencing bootstrap(profile 驱动,:原实现V1 无 fence 概念)
        let fence_meta = if matches!(
            client.profile(),
            crate::config::CompatibilityProfile::RustV2
        ) {
            let tmp_layout = layout.clone();
            Some(super::fencing::bootstrap(&client, &tmp_layout, 1).await?)
        } else {
            None
        };

        // stream_cfg 现由参数传入(per-group resolved),不再读全局 client.config().stream。
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
            async_delete_cancel: CancellationToken::new(),
            async_delete_handle: std::sync::Mutex::new(None),
            async_delete_pending: AtomicUsize::new(0),
            claimed: Arc::new(std::sync::RwLock::new(Vec::new())),
            pub_seq: AtomicU64::new(0),
            rr_counter: AtomicU64::new(0),
            handlers_cell: std::sync::OnceLock::new(),
            fence_meta,
            rebalance_lock: tokio::sync::Mutex::new(()),
            bg_tracker: tokio_util::task::TaskTracker::new(),
            bg_aborts: std::sync::Mutex::new(Vec::new()),
            coordinator_handle: std::sync::Mutex::new(None),
            shutdown_lock: tokio::sync::Mutex::new(()),
            shutdown_deadline: std::sync::Mutex::new(None),
            sweepable: std::sync::Mutex::new(Vec::new()),
            owner_ctx: std::sync::Mutex::new(std::collections::HashMap::new()),
        });
        // handler 路由表一次性注入(此后只读;worker 经 rt.handlers() 查表)
        rt.handlers_cell
            .set(handlers)
            .ok()
            .expect("handlers 重复注入");

        // ① coordinator(每组一个, 默认值)——句柄由 runtime 唯一持有，停机必须真正 join；
        //    超 deadline 时 abort，触发 ClaimSlot 的任务最后防线并释放锁。
        let coord = tokio::spawn(coordinator_loop(Arc::clone(&rt), event_rx));
        *rt.coordinator_handle.lock().expect("coordinator_handle") = Some(coord);
        // ②-⑤ 后台任务统一入 bg_tracker(停机 cancel 后 deadline 内 wait;超时 abort_all):
        {
            let mut aborts = rt.bg_aborts.lock().expect("bg_aborts");
            aborts.push(
                rt.bg_tracker
                    .spawn(rebalance_loop(Arc::clone(&rt)))
                    .abort_handle(),
            ); // 心跳+再平衡
            aborts.push(
                rt.bg_tracker
                    .spawn(wake_loop(Arc::clone(&rt)))
                    .abort_handle(),
            ); // wake 订阅
            aborts.push(
                rt.bg_tracker
                    .spawn(control_loop(Arc::clone(&rt)))
                    .abort_handle(),
            ); // 管理命令
            aborts.push(
                rt.bg_tracker
                    .spawn(sweep_loop(Arc::clone(&rt)))
                    .abort_handle(),
            ); // orphan sweep
            aborts.push(
                rt.bg_tracker
                    .spawn(auto_trim_loop(Arc::clone(&rt)))
                    .abort_handle(),
            ); // ⑥ autoTrim(leader)
        }
        // asyncDel 不进 bg_tracker：常规后台任务先停，coordinator/worker 再 drain；只有业务 ACK
        // 全部收口后才取消并等待本任务，保证停机窗口内最后一批 ACKed ID 也能进入末次 flush。
        if let Some(rx) = async_delete_rx {
            let handle = tokio::spawn(async_delete_loop(Arc::clone(&rt), rx));
            *rt.async_delete_handle.lock().expect("async_delete_handle") = Some(handle);
        }

        Ok(rt)
    }

    /// 发布:路由已算好(partition 编号),组信封 → XADD。
    /// F6 round-robin 发布:无路由 key 时全局轮转均摊到各分区(对照 原实现 publish(partition==null))。
    pub(super) async fn publish_round_robin<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        data: &T,
    ) -> Result<String> {
        let p = (self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.count as u64) as u32;
        self.publish(topic, event, p, data, None).await
    }

    /// Publishes a payload to the selected partition stream.
    pub(super) async fn publish<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        p: u32,
        data: &T,
        passthrough: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<String> {
        // 守门(不沿用 原实现 的静默 null)
        if topic.is_empty() || event.is_empty() {
            return Err(NasaRedisError::Config(format!(
                "InvalidPublish: topic/event 为空(topic={topic:?}, event={event:?})"
            )));
        }
        let env = Envelope {
            topic: topic.to_string(),
            event: event.to_string(),
            data: serde_json::to_value(data).map_err(|e| NasaRedisError::Codec(e.to_string()))?,
            passthrough,
        };
        let body = serde_json::to_vec(&env).map_err(|e| NasaRedisError::Codec(e.to_string()))?;
        self.pub_seq.fetch_add(1, Ordering::Relaxed);
        // XADD {stream} * data {json}(envelope 编码在入队时完成, publish_partition 同则)
        let id: String = redis::cmd("XADD")
            .arg(self.layout.stream(p))
            .arg("*")
            .arg(DATA_FIELD)
            .arg(&body)
            .query_async(&mut self.client.conn())
            .await?;
        Ok(id)
    }

    /// Returns the partitions currently owned by this runtime.
    pub(super) async fn claimed_partitions(&self) -> Vec<u32> {
        self.claimed.read().expect("claimed 锁中毒").clone()
    }

    /// 把已确认的 stream entry 交给单一异步删除 owner。
    ///
    /// XDEL 只负责回收 stream 空间，不参与消费正确性；配置为 0 时 entry 留在 stream，
    /// 仅在 autoTrim 同时启用时才会按保留窗回收。队列满时等待形成背压，避免 ACK 已成功后
    /// 静默遗失待删 ID。
    pub(super) async fn enqueue_async_delete(&self, partition: u32, ids: &[String]) {
        let Some(tx) = &self.async_delete_tx else {
            return;
        };
        if ids.is_empty() {
            return;
        }
        // 从 ACK 后进入删除管道就计入 gauge，覆盖 channel 排队、owner 本地缓冲和反压等待；
        // 只有 XDEL 成功或 owner 去重相同 ID 时扣减。owner 退出后的未删 entry 继续保留计数。
        let _ = self.async_delete_pending.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_add(ids.len())),
        );
        let batch = AsyncDeleteBatch {
            partition,
            ids: ids.to_vec(),
        };
        if let Err(err) = tx.send(batch).await {
            tracing::warn!(
                partition,
                count = err.0.ids.len(),
                auto_trim_enabled = self.stream_cfg.auto_trim_rate_ms > 0,
                "异步 XDEL owner 已退出；entry 保留在 stream，启用 autoTrim 时由保留窗继续回收"
            );
        }
    }

    /// 返回本组已进入异步删除管道但尚未确认 XDEL 成功的 entry 数。
    pub(super) fn async_delete_pending(&self) -> usize {
        self.async_delete_pending.load(Ordering::Acquire)
    }

    /// 未走异步 drain 时的最后防线。
    ///
    /// `RunningPartition::drop` 无法等待网络 I/O，但必须关闭 admission 并 abort 所有 owner task；
    /// coordinator 被 abort 后会 drop `ClaimSlot`，其中的任务包装继续 abort worker/recovery。
    pub(super) fn cancel_best_effort(&self) {
        self.bg_cancel.cancel();
        self.cancel.cancel();
        self.async_delete_cancel.cancel();
        for abort in self.bg_aborts.lock().expect("bg_aborts").drain(..) {
            abort.abort();
        }
        if let Some(handle) = self
            .coordinator_handle
            .lock()
            .expect("coordinator_handle")
            .take()
        {
            handle.abort();
        }
        if let Some(handle) = self
            .async_delete_handle
            .lock()
            .expect("async_delete_handle")
            .take()
        {
            handle.abort();
        }
    }

    /// `RunningPartition::drop` 使用的异步清理入口。
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

    /// 停机
    /// ① **先停后台任务**(bg_cancel:control/sweep/rebalance/wake)并等其退出——此期间
    ///    coordinator **仍持锁运行、watchdog 续租**,避免 control(resume/drop/dlq)、sweep
    ///    (XAUTOCLAIM)、rebalance(tryLock)在失锁后继续改 Redis;
    /// ② **再**关 coordinator(cancel):drain 在途批次 → 逐分区 unlock;
    /// ③ 取消 asyncDel owner 并做末次 flush；
    /// ④ 摘心跳 ZREM + pub wake。全程**绝对 deadline**(timeout_at),过期即返回靠 lease+周期
    /// 兜底,无隐式宽限。(coordinator drain 用其自身以 cfg.drain_timeout_ms 起算的 deadline,
    /// 与本处对齐在毫秒级;统一传同一 deadline 的纯化、挂 Redis I/O 任务的强制 abort 列后续。)
    pub(super) async fn shutdown(&self) {
        let _shutdown_guard = self.shutdown_lock.lock().await;
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(self.cfg.drain_timeout_ms);
        // 单一绝对 deadline:入口生成一次,coordinator drain 读同一个值
        *self.shutdown_deadline.lock().expect("shutdown_deadline") = Some(deadline);
        // ① 先停后台任务并等退出(coordinator 仍持锁,管理/sweep 副作用在持锁期内完成);
        //    deadline 内未退出(挂在 Redis I/O,如 CLIENT PAUSE)→ 强制 abort_all
        self.bg_cancel.cancel();
        self.bg_tracker.close();
        if tokio::time::timeout_at(deadline, self.bg_tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("停机后台任务超 deadline 未退出,强制 abort");
            for a in self.bg_aborts.lock().expect("bg_aborts").drain(..) {
                a.abort();
            }
        }
        // ② 再关 coordinator → drain + unlock
        self.cancel.cancel();
        let (done_tx, done_rx) = oneshot::channel();
        // 入队也受同一绝对 deadline 约束：event channel 满且 coordinator 卡在 Redis I/O 时，
        // 裸 `send().await` 会让 shutdown 在开始等待 drain 之前就永久挂住。
        let send_result = tokio::time::timeout_at(
            deadline,
            self.event_tx.send(Event::Shutdown { done: done_tx }),
        )
        .await;
        let mut force_abort = false;
        match send_result {
            Ok(Ok(())) => match tokio::time::timeout_at(deadline, done_rx).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    force_abort = true;
                    tracing::warn!(
                        group = %self.layout.prefix,
                        "coordinator 未发送停机确认便退出，强制 join 收口"
                    );
                }
                Err(_) => {
                    force_abort = true;
                    tracing::warn!(
                        group = %self.layout.prefix,
                        "coordinator 停机超 deadline 未 drain 完(疑似卡 Redis I/O),abort 释放分区锁"
                    );
                }
            },
            Ok(Err(_)) => {
                // 接收端已关闭，coordinator 已退出；下方 join 收口最终状态。
            }
            Err(_) => {
                force_abort = true;
                tracing::warn!(
                    group = %self.layout.prefix,
                    "coordinator 停机事件入队超 deadline，abort 释放分区锁"
                );
            }
        }
        let coordinator_handle = self
            .coordinator_handle
            .lock()
            .expect("coordinator_handle")
            .take();
        if let Some(mut handle) = coordinator_handle {
            if force_abort || tokio::time::Instant::now() >= deadline {
                handle.abort();
                let _ = handle.await;
            } else if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                tracing::warn!(
                    group = %self.layout.prefix,
                    "coordinator 已回 drain 结果但任务未在 deadline 内退出，强制 abort"
                );
                handle.abort();
                let _ = handle.await;
            }
        }
        // ③ coordinator/worker 已无新 ACK：取消 asyncDel owner，末次吸干队列并 flush。
        self.async_delete_cancel.cancel();
        let delete_handle = self
            .async_delete_handle
            .lock()
            .expect("async_delete_handle")
            .take();
        if let Some(mut handle) = delete_handle {
            if tokio::time::Instant::now() < deadline {
                if tokio::time::timeout_at(deadline, &mut handle)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        group = %self.layout.prefix,
                        auto_trim_enabled = self.stream_cfg.auto_trim_rate_ms > 0,
                        "异步 XDEL 末次 flush 超 deadline，强制结束；entry 保留在 stream，启用 autoTrim 时由保留窗继续回收"
                    );
                    handle.abort();
                }
            } else {
                handle.abort();
                tracing::warn!(
                    group = %self.layout.prefix,
                    auto_trim_enabled = self.stream_cfg.auto_trim_rate_ms > 0,
                    "停机已过 deadline，跳过异步 XDEL 末次 flush；entry 保留在 stream，启用 autoTrim 时由保留窗继续回收"
                );
            }
        }
        // ④ 摘心跳 + 唤醒兄弟(剩余预算;过期即跳过——他节点靠心跳 TTL/周期兜底接管)
        if tokio::time::Instant::now() < deadline {
            let _ = tokio::time::timeout_at(
                deadline,
                self.client
                    .z_rem(&self.layout.nodes(), self.node_id.as_str()),
            )
            .await;
            let _ = tokio::time::timeout_at(deadline, async {
                let _: std::result::Result<i64, _> = redis::cmd("PUBLISH")
                    .arg(self.layout.wake())
                    .arg("")
                    .query_async(&mut self.client.conn())
                    .await;
            })
            .await;
        } else {
            tracing::warn!(node = %self.node_id, "停机已过 deadline,跳过 ZREM/PUBLISH(靠心跳 TTL + 周期兜底)");
        }
        tracing::info!(group = %self.layout.prefix, node = %self.node_id, "partition 组已停机");
    }
}

// ─────────────────────────────────────────────────────────────────────────
// coordinator:调度状态机(只做调度/Redis poll/demux,不 await 业务)
// ─────────────────────────────────────────────────────────────────────────

impl GroupRuntime {
    /// 构造分区 owner-fenced 转换凭据:本节点非该分区 owner(owner_ctx 无记录)→
    /// None,调用方据此拒绝管理操作。RustV2 带 fence 三元组 + 任期 counter;原实现V1 fence_key 空,
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

    /// command 路径专用:用命令的稳定 operation_id 构造凭据(:同 op_id 重试幂等续作)。
    pub(super) fn require_owner_for_op(&self, p: u32, operation_id: &str) -> Result<OwnerFence> {
        self.owner_fence(p, operation_id.to_string())
            .ok_or_else(|| {
                NasaRedisError::OwnershipChanged(format!(
                    "分区 {p} 非本节点持有(或锁已丢失),管理命令交新 owner"
                ))
            })
    }

    /// direct API 的 owner 凭据:op_id **稳定派生自 (p, marker.park_id)**
    /// ——同一宗 Park 的 direct 重试得到**同一** op_id,崩溃重入(marker 已记 op_id)不再因每次新
    /// uuid 自撞 `OperationConflict` 永久冻结分区。无 park_id(无 Park 记录)→ 退回新 uuid(mutator
    /// 随后会以"无 Park 记录"报错,不进任何转换)。
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

    /// resume 后通知 coordinator 恢复消费(Parked → Ready)。
    pub(super) async fn resume_parked(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids = super::disposition::resume(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }

    /// drop 后通知 coordinator 恢复消费(后续新消息照常)。
    pub(super) async fn drop_parked(&self, p: u32) -> Result<()> {
        let of = self.require_owner_direct(p).await?;
        super::disposition::drop_parked(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(())
    }

    /// dlq 处置(owner-fenced;RunningPartition::dlq_parked 经此)。
    pub(super) async fn dlq_parked(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::dlq_from_parked(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }

    /// 查询分区 Park 状态(None = 未 Park)。
    pub(super) async fn parked_id(&self, p: u32) -> Result<Option<String>> {
        super::disposition::parked_id(&self.client, &self.layout, p).await
    }

    /// ForceRepublish(仅 ResumeIndeterminate;显式接受重复/变序风险)成功后恢复消费。
    pub(super) async fn force_republish(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::force_republish(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }

    /// 通用 liveness-takeover(①.4):强制接管卡死的在途 *Publishing/Dropping(原 op 已死)。
    /// 用稳定 `direct:{p}:{park_id}` op 覆写后续作——崩溃重入用同 op 续作幂等。
    pub(super) async fn force_takeover(&self, p: u32) -> Result<Vec<String>> {
        let of = self.require_owner_direct(p).await?;
        let ids =
            super::disposition::force_takeover(&self.client, &self.layout, p, &of.lease()).await?;
        let _ = self.event_tx.send(Event::ParkResolved { p }).await;
        Ok(ids)
    }
}

/// 生成一个管理操作的 operation_id(direct API 用)。
fn new_operation_id() -> String {
    format!("op-{}", uuid::Uuid::new_v4().simple())
}

/// 分区锁的完整 Redis key(= 锁前缀 + 业务 key;fenced ACK Lua 的 hexists 用——
/// worker 不持 guard,经配置重组;与 guard.lock_key() 等价)。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
fn lock_key_full(rt: &Arc<GroupRuntime>, p: u32) -> String {
    format!(
        "{}{}",
        rt.client.config().lock.prefix,
        rt.layout.lock_business_key(p)
    )
}

impl GroupRuntime {
    /// XREADGROUP COUNT(接线 stream.batch_size,不再硬编码 100)。
    pub(super) fn cfg_batch(&self) -> usize {
        self.stream_cfg.batch_size
    }
}

// ─────────────────────────────────────────────────────────────────────────
// worker:每分区一个,串行消费(decode → holds → handler → holds → fenced ACK)
// ─────────────────────────────────────────────────────────────────────────

impl GroupRuntime {
    /// 返回启动阶段注入的事件 handler 注册表。
    ///
    /// worker 消费分区消息时通过该表按 `(topic,event)` 定位业务处理器；未注入说明 runtime 未按协议启动。
    fn handlers(&self) -> &HandlerMap {
        self.handlers_cell
            .get()
            .expect("handlers 必在 start 时注入")
    }
}

// ─────────────────────────────────────────────────────────────────────────
// 再平衡:心跳 + fair 配额 + 抢占/让出(对照 原实现 rebalance;V1 墙钟语义)
// ─────────────────────────────────────────────────────────────────────────

// ── 子模块(架构自检 R2:runtime.rs 拆分;各文件精确 import,无 use super::* 之外的
//    blanket allow——)──
mod asyncdel;
mod autotrim;
mod control;
mod coordinator;
mod poll;
mod rebalance;
mod retry;
mod worker;
use asyncdel::*;
use autotrim::*;
use control::*;
use coordinator::*;
use poll::*;
use rebalance::*;
use retry::*;
use worker::*;
