// ============================================================================
// 共享 Redis Stream 的 PROXY 竞争消费与待确认消息回收。
//
// **共享 group 多 consumer 并行消费**(高吞吐、无序),区别于 PARTITION(每分区串行 + owner 锁 +
// fenced ACK):PROXY 是**单共享 stream + 一个 consumer group**,N 个 consumer 并行 `XREADGROUP >`,
// Redis 自动把消息分发给空闲 consumer;**无 owner/fence**(无需互斥,本就并行)。
// at-least-once:handler 失败 → 不 ACK,留 group PEL;`XAUTOCLAIM` 周期回收 idle pending(含崩溃
// consumer 的 + 本节点失败的)重投;超 `max_redeliver` → poison(Drop=XACK 丢弃 / Dlq=转 DLQ stream)。
//
// 发布沿用 Envelope 信封，按主题与事件匹配处理器；逐 ID 解码失败或展开只保留对应身份，
// 处理器构造、轮询或结果失败则保留已交付的整桶身份。XADD 写入共享 Stream，由同组消费者竞争领取，
// 不提供分区排他权威。
// ============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::config::{MAX_REDIS_NAME_BYTES, MAX_REDIS_RUNTIME_DURATION_MS, MAX_STREAM_BATCH_SIZE};
use crate::error::{NasaRedisError, Result};
use crate::partition::{Envelope, ErasedHandler, HandlerMap, DATA_FIELD};

/// 单实例 PROXY consumer 任务上限。
///
/// 每个 consumer 都是独立 Tokio task，且持有 Redis 轮询状态；更高并行度应通过多实例横向扩展，
/// 不能让一个配置值在启动时无界 spawn。
const MAX_PROXY_CONSUMERS: usize = 256;

/// PROXY 消费配置。
///
/// handler 必须幂等：本路径没有 owner-fence，XAUTOCLAIM 可以重新分配闲置 PEL。
/// handler 超时小于回收阈值只约束一次本地异步等待，不能排除批内排队、执行器停顿或已产生的外部副作用；
/// Future 取消也不能强制中断不让出执行权的同步代码。因此同一消息可能被重复或重叠处理。
/// 启动校验要求有界 handler 超时及更长的回收闲置阈值，不提供恰好一次执行保证。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyCfg {
    /// 每节点并行 consumer 任务数(>=1)。
    pub consumers: usize,
    /// 每次 XREADGROUP COUNT。
    pub batch_size: usize,
    /// 冷流再 poll 间隔毫秒(**NOBLOCK 轮询** + 空轮 sleep——不在共享多路复用连接上用 BLOCK,
    /// 否则 BLOCK 命令会卡住整条 multiplexed 连接的其它命令,对齐 partition 托管模式 NOBLOCK 语义)。
    pub poll_idle_ms: u64,
    /// 回收时允许的累计投递次数上限，超过后按毒消息策略处置；必须至少为一。
    pub max_redeliver: u32,
    /// XAUTOCLAIM 回收 idle 阈值毫秒(pending 闲置超此即可被重投)。
    pub reclaim_min_idle_ms: u64,
    /// 毒消息策略。
    pub poison: ProxyPoison,
    /// 每个 handler 桶的异步等待超时毫秒，超时后整桶保留在 PEL 待重投。
    /// 必须小于 reclaim_min_idle_ms；此约束不包含领取到调用之间的排队，也不撤销已发生的业务副作用。
    pub handler_timeout_ms: u64,
    /// publish 时给共享 stream 的近似上限。
    /// `Some(n)` → `XADD MAXLEN ~ n`,防 ACK 只出 PEL、entry 留存的无界增长 OOM。
    pub max_stream_len: Option<u64>,
    /// 未注册 (topic,event) 的处置
    /// `false`(默认)= 本节点无 handler 即 **XACK 丢弃**(发 warn 日志);
    /// `true` = **留 PEL**(不 ACK),让 XAUTOCLAIM 把它交给注册了 handler 的其它节点,
    ///   始终无人处理则最终 poison。
    ///
    /// ⚠ **异构多节点 = 静默丢消息**:默认 `false` 假设**全节点注册同一
    /// handler 集**(同构)。若节点 B 没注册 event X 而先读到 X 的消息,B 会**直接 XACK 丢弃**,注册了 X 的
    /// 节点 A **永远收不到** → 静默丢失(仅一条 warn)。**多节点共享组务必全节点注册同一 handler 集**;
    /// **资金类/不可丢的共享组应显式置 `true`**(留 PEL 交注册节点,真无人处理才 poison,不静默丢)。
    ///
    /// ⚠ **`true` 是"降低静默丢失风险",不是可靠路由**:无 handler 节点留 PEL 后,
    /// **它自己的 reclaim loop 也会再 claim 到这条**;若 `max_redeliver` 小、`reclaim_min_idle_ms` 激进,
    /// 注册了 handler 的节点未必先抢到 → 最终仍可能 poison。要**严格路由**请按 handler 集**拆 stream/group**
    /// (异构节点各消费各自的 stream),`requeue_unregistered=true` 只是同构假设破裂时的兜底,非保证。
    pub requeue_unregistered: bool,
    /// 停机排干预算毫秒，到期请求 abort 并继续等待真实任务退出。
    /// 不能强制中断不让出执行权的同步 handler，因此预算不构成所有宿主代码的退出时间保证。
    pub drain_deadline_ms: u64,
}

impl Default for ProxyCfg {
    /// 业务作用：构造共享组消费的默认容量、轮询、重投和退出策略。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回单消费者、100 条批量、重投上限 5、10 秒 handler 超时及 30 秒回收闲置阈值的配置；默认毒消息丢弃、未注册事件确认，handler 仍须幂等。
    fn default() -> Self {
        Self {
            consumers: 1,
            batch_size: 100,
            poll_idle_ms: 200,
            max_redeliver: 5,
            reclaim_min_idle_ms: 30_000,
            poison: ProxyPoison::Drop,
            // 本地等待需早于回收阈值到期，为合作式异步处理留出退出余量；重复执行仍由业务幂等性承担。
            handler_timeout_ms: 10_000,
            max_stream_len: Some(1_000_000),
            requeue_unregistered: false,
            drain_deadline_ms: 10_000,
        }
    }
}

/// PROXY 毒消息策略(无 fence/quarantine,比 partition 简化)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyPoison {
    /// 超上限 → XACK 丢弃(记 error 日志)。
    Drop,
    /// 超上限 → 转存到 `{stream}:dlq` + XACK 源。
    Dlq,
}

/// 新建 consumer group 的起始位点，仅首次建组生效；BUSYGROUP 不改变已有位点。
/// [`PreparedProxy::prepare`] 默认只消费建组后的新消息，需要历史 backlog 时显式使用
/// [`PreparedProxy::prepare_with_offset`] 并选择 History。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProxyStartOffset {
    /// `$`：只消费建组后的新消息，是 [`prepare`](PreparedProxy::prepare) 的默认值。
    #[default]
    New,
    /// `0-0`:从头消费(含历史 backlog)。durable-replay 语义,需要重放历史时显式选用。
    History,
}

impl ProxyStartOffset {
    /// 业务作用：把首次建组的消费策略映射为 Redis 起始位点。
    ///
    /// 参数说明：无。
    ///
    /// 返回：New 返回 $，History 返回 0-0；已有组的位点不由此值重置。
    fn redis_id(self) -> &'static str {
        match self {
            ProxyStartOffset::New => "$",
            ProxyStartOffset::History => "0-0",
        }
    }
}

/// 阶段一:已建 group,未启消费(register 只在此阶段)。
pub struct PreparedProxy {
    /// 本次准备与后续消费固定使用的 Redis 来源。
    client: Arc<RedisClient>,
    /// 竞争领取消息的共享 Stream。
    stream: String,
    /// 已确认存在且不重置位点的消费组。
    group: String,
    /// 启动前可登记的主题与事件处理目录。
    handlers: HandlerMap,
    /// 已通过准备校验的消费与处置边界。
    cfg: ProxyCfg,
}

impl PreparedProxy {
    /// 业务作用：为共享 Stream 准备竞争消费组，默认只领取首次建组之后的新消息。
    ///
    /// 参数说明：
    /// - `client`：固定 Redis 来源的共享连接入口。
    /// - `stream`：共享 Stream 名，须非空、无首尾空白且在名称字节上限内。
    /// - `group`：竞争消费组名，受同样的名称约束。
    /// - `cfg`：消费并发、容量、重投、毒消息与停机策略，准备时校验。
    ///
    /// 返回：建组成功或识别为已有组时返回尚未启动的计划；配置或其它 Redis 错误返回失败。需要首次读取历史消息时使用 prepare_with_offset 和 History。
    pub async fn prepare(
        client: Arc<RedisClient>,
        stream: impl Into<String>,
        group: impl Into<String>,
        cfg: ProxyCfg,
    ) -> Result<Self> {
        Self::prepare_with_offset(client, stream, group, cfg, ProxyStartOffset::New).await
    }

    /// 业务作用：校验共享组运行约束并按指定起点尝试建组，不重置已有消费进度。
    ///
    /// 参数说明：
    /// - `client`：固定 Redis 来源的共享客户端。
    /// - `stream`：共享 Stream 的合法业务名称。
    /// - `group`：本组固定名称，用于领取、确认与回收。
    /// - `cfg`：有界消费者数量、批量、轮询、handler 超时和回收闲置阈值等约束。
    /// - `start_offset`：仅首次建组生效的起点；New 使用 $，History 使用 0-0。
    ///
    /// 返回：配置不满足边界时在建组前返回错误；仅接受 OK 回包或精确 BUSYGROUP 错误码作为组已就绪的证据。
    /// 成功返回空 handler 目录的准备态；其它回包或 Redis 错误均拒绝交付，不重置已有组位点。
    pub async fn prepare_with_offset(
        client: Arc<RedisClient>,
        stream: impl Into<String>,
        group: impl Into<String>,
        cfg: ProxyCfg,
        start_offset: ProxyStartOffset,
    ) -> Result<Self> {
        if cfg.consumers == 0 || cfg.batch_size == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.consumers/batch_size 必须 > 0".into(),
            ));
        }
        if cfg.consumers > MAX_PROXY_CONSUMERS {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg.consumers 过大(上限 {MAX_PROXY_CONSUMERS})"
            )));
        }
        if cfg.batch_size > MAX_STREAM_BATCH_SIZE {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg.batch_size 过大(上限 {MAX_STREAM_BATCH_SIZE})"
            )));
        }
        if cfg.reclaim_min_idle_ms == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.reclaim_min_idle_ms 必须 > 0".into(),
            ));
        }
        if cfg.handler_timeout_ms == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.handler_timeout_ms 必须 > 0".into(),
            ));
        }
        if cfg.drain_deadline_ms == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.drain_deadline_ms 必须 > 0".into(),
            ));
        }
        if cfg.max_stream_len == Some(0) {
            return Err(NasaRedisError::Config(
                "ProxyCfg.max_stream_len 配置后必须 > 0".into(),
            ));
        }
        if cfg.poll_idle_ms == 0
            || cfg.poll_idle_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || cfg.reclaim_min_idle_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || cfg.handler_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || cfg.drain_deadline_ms > MAX_REDIS_RUNTIME_DURATION_MS
        {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg 的 poll/reclaim/handler/drain 时长必须位于 1..={MAX_REDIS_RUNTIME_DURATION_MS}ms"
            )));
        }
        // 回收查询使用累计投递次数，零额度会使第一次回收就失去重投机会。
        if cfg.max_redeliver == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.max_redeliver 必须 >= 1".into(),
            ));
        }
        // 回收阈值至少应晚于一次本地等待超时；这只是基本时长约束，不授予排他处理或跨消费者确认权威。
        if cfg.handler_timeout_ms > 0 && cfg.reclaim_min_idle_ms <= cfg.handler_timeout_ms {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg.reclaim_min_idle_ms({}) 必须 > handler_timeout_ms({})，为本地异步等待预留退出余量",
                cfg.reclaim_min_idle_ms, cfg.handler_timeout_ms
            )));
        }
        let stream = stream.into();
        let group = group.into();
        if stream.trim().is_empty()
            || group.trim().is_empty()
            || stream != stream.trim()
            || group != group.trim()
            || stream.len() > MAX_REDIS_NAME_BYTES
            || group.len() > MAX_REDIS_NAME_BYTES
        {
            return Err(NasaRedisError::Config(format!(
                "PROXY stream / group 必须无首尾空白、非空且不超过 {MAX_REDIS_NAME_BYTES} 字节"
            )));
        }
        // 位点只决定首次建组的消费起点；已有组不能因进程重启而重置进度。
        let r: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg(start_offset.redis_id())
            .arg("MKSTREAM")
            .query_async(&mut client.conn())
            .await;
        match r {
            Ok(reply) if reply == "OK" => {}
            // 只有准确的已存在事实才能复用消费组，错误正文或相似错误码不能授权后续消费。
            Err(e) if e.code() == Some("BUSYGROUP") => {}
            Ok(_) => {
                return Err(NasaRedisError::Parsing(
                    "PROXY 建组回复不是预期的 OK".into(),
                ));
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Self {
            client,
            stream,
            group,
            handlers: HashMap::new(),
            cfg,
        })
    }

    /// 业务作用：为主题和事件登记批量业务处理器，并把类型解码或异步处理失败归还为待重投 ID。
    ///
    /// 参数说明：
    /// - `topic`：信封中的业务主题，与 event 共同选择本地处理器。
    /// - `event`：主题内的事件名；相同主题和事件再次登记会替换原处理器。
    /// - `f`：接收本桶成功解码的消息，返回整桶成功或失败的异步处理器。
    ///
    /// 返回：返回当前准备态供链式登记；类型解码错误或展开保留各自 ID，处理器构造 Future、轮询或返回结果失败时保留全部已交付 ID。
    /// 已发生的业务副作用不回滚，失败 ID 留待重投；本函数显式记录的处理失败日志不包含业务错误正文。
    pub fn register<T, F, Fut>(&mut self, topic: &str, event: &str, f: F) -> &mut Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: ErasedHandler = Arc::new(move |items: Vec<(String, serde_json::Value)>| {
            let f = Arc::clone(&f);
            Box::pin(async move {
                let mut typed: Vec<T> = Vec::with_capacity(items.len());
                let mut good_ids: Vec<String> = Vec::with_capacity(items.len());
                let mut failed: Vec<String> = Vec::new();
                for (id, v) in items {
                    // Deserialize 可执行业务自定义代码；单条解码展开只归还该身份，不能使整组消费责任意外退出。
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        serde_json::from_value::<T>(v)
                    })) {
                        Ok(Ok(t)) => {
                            typed.push(t);
                            good_ids.push(id);
                        }
                        Ok(Err(e)) => {
                            tracing::error!(id, category = ?e.classify(), "PROXY 业务类型解码失败,该 ID 进重投/毒");
                            failed.push(id);
                        }
                        Err(_) => {
                            tracing::error!(id, "PROXY 业务类型解码展开,该 ID 保留重投");
                            failed.push(id);
                        }
                    }
                }
                if typed.is_empty() {
                    return failed;
                }
                // 调用处理器构造 Future 时已经可能发生副作用或展开，保护范围必须早于该调用。
                let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    f(typed)
                })) {
                    Ok(future) => {
                        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(future)).await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(Ok(())) => failed,
                    Ok(Err(_)) => {
                        tracing::warn!("PROXY handler 失败,本桶 ID 转重投");
                        failed.extend(good_ids);
                        failed
                    }
                    Err(_) => {
                        tracing::error!("PROXY handler 构造或轮询展开,本桶 ID 转重投");
                        failed.extend(good_ids);
                        failed
                    }
                }
            })
        });
        self.handlers
            .insert((topic.to_string(), event.to_string()), h);
        self
    }

    /// 业务作用：提供消费路由目录规模，用于放行前拒绝空计划和超量计划。
    ///
    /// 参数说明：无。
    ///
    /// 返回：去重后的主题与事件组合数量。
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }

    /// 业务作用：建立并立即激活独立竞争消费，保持原有独立入口行为。
    ///
    /// 参数说明：无。
    ///
    /// 返回：消费者与回收任务的唯一关闭句柄；配置与准备失败时不开放消费。
    pub async fn start(self) -> Result<RunningProxy> {
        let running = self.start_suspended().await?;
        running.activate();
        Ok(running)
    }

    /// 业务作用：建立由激活屏障保护的消费任务及独立关闭 owner。
    ///
    /// 参数说明：无。
    ///
    /// 返回：准备完成但不会读取、回收或调用 handler 的运行句柄。
    pub async fn start_suspended(self) -> Result<RunningProxy> {
        self.start_suspended_with_activation(CancellationToken::new())
            .await
    }

    /// 业务作用：建立等待宿主统一许可的消费者、回收任务及独立关闭责任。
    ///
    /// 参数说明：
    /// - `ready`：取消表示开放本代消费；共享许可时由宿主统一取消，不逐项调用 activate。
    ///
    /// 返回：取得有效 Tokio 上下文并维持完整任务责任时返回运行句柄；缺少上下文或交付前已失去任务责任时返回错误。
    /// 任务关闭优先于激活，在许可之前不读取或调用 handler；错误不会回滚准备期建组或已经获许可执行的业务副作用。
    pub async fn start_suspended_with_activation(
        self,
        ready: CancellationToken,
    ) -> Result<RunningProxy> {
        // 派发前取得执行器，避免缺少上下文时在建立部分本地责任后展开。
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| NasaRedisError::Config("PROXY 启动需要有效的 Tokio 执行上下文".into()))?;
        let cancel = CancellationToken::new();
        let activity = Arc::new(crate::activity::Activity::default());
        let shared = Arc::new(ProxyShared {
            activity: activity.clone(),
            client: self.client.clone(),
            stream: self.stream.clone(),
            group: self.group.clone(),
            handlers: self.handlers,
            cfg: self.cfg.clone(),
        });
        let mut handles = Vec::new();
        let mut names = Vec::with_capacity(self.cfg.consumers + 1);
        for i in 0..=self.cfg.consumers {
            let consumer = format!("c-{}-{i}", uuid::Uuid::new_v4().simple());
            names.push(consumer.clone());
            let shared = shared.clone();
            let cancel = cancel.clone();
            let ready = ready.clone();
            let reclaim = i == self.cfg.consumers;
            let active = activity.task(if reclaim {
                crate::activity::TaskKind::Reclaimer
            } else {
                crate::activity::TaskKind::Consumer
            });
            handles.push(runtime.spawn(async move {
                let _active = active;
                // 连接准备不授予消费权；取消优先于激活，禁止启动回滚时晚到消费。
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = ready.cancelled() => {},
                }
                if reclaim {
                    reclaim_loop(shared, consumer, cancel).await;
                } else {
                    consumer_loop(shared, consumer, cancel).await;
                }
            }));
        }
        let (deadline, _) = tokio::sync::watch::channel(None);
        let (report, outcome) = tokio::sync::watch::channel(ProxyStopReport::default());
        let owner = ProxyCloseOwner {
            client: self.client.clone(),
            stream: self.stream.clone(),
            group: self.group.clone(),
            names,
            aborts: handles.iter().map(JoinHandle::abort_handle).collect(),
            handles,
            cancel: cancel.clone(),
            deadline: deadline.clone(),
            budget: Duration::from_millis(self.cfg.drain_deadline_ms),
            report: activity.completion(report),
        };
        // 任务句柄从创建时移交 owner，任何调用方取消都不会取得或分离唯一等待责任。
        runtime.spawn(owner.run());
        let running = RunningProxy {
            activity,
            client: self.client,
            stream: self.stream,
            cancel,
            ready,
            deadline,
            outcome,
            max_stream_len: self.cfg.max_stream_len,
            drain_deadline_ms: self.cfg.drain_deadline_ms,
            consumers: self.cfg.consumers,
        };
        // 已关闭的执行器可能同步销毁派发任务；失去责任时关闭准入，不能交付假就绪句柄。
        if running.with_running(|| ()).is_none() {
            running.begin_shutdown();
            return Err(NasaRedisError::Config(
                "PROXY 启动时未取得完整任务责任，已请求关闭".into(),
            ));
        }
        Ok(running)
    }
}

/// 本代竞争消费者与回收任务共同使用的冻结目录、连接和本地责任证据。
struct ProxyShared {
    /// 本地活动任务及已领取处理次数，不替代远端 PEL。
    activity: Arc<crate::activity::Activity>,
    /// 各领取、回收、确认与处置调用的同一来源。
    client: Arc<RedisClient>,
    /// 同组竞争读取的共享 Stream。
    stream: String,
    /// 领取及确认使用的固定消费组。
    group: String,
    /// 启动后冻结的主题与事件处理器。
    handlers: HandlerMap,
    /// 所有消费者共同遵守的容量、超时与毒消息策略。
    cfg: ProxyCfg,
}

/// Proxy 清理的证据分类；不能确认结果时不宣告完成，已发出的清理可能已经删除空消费者。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProxyCleanup {
    /// 尚未执行或任务非正常退出，不进行删除。
    #[default]
    Skipped,
    /// 各消费者经原子复验确认没有 pending，相应清理调用均已取得完成回复。
    Complete,
    /// 存在 pending，相关 consumer 被保留。
    Pending,
    /// 查询、解析或删除未获确定结果。
    Unavailable,
    /// 共用截止时间已耗尽，停止发起清理。
    Deadline,
}

/// Proxy 本地任务退出与 PEL 清理报告，不表示远端 handler 副作用被撤销。
#[derive(Debug, Clone, Copy, Default)]
pub struct ProxyStopReport {
    /// 所有 consumer 与 reclaim 已取得 join 证据。
    pub terminated: bool,
    /// 排干预算耗尽后请求过强制终止。
    pub forced: bool,
    /// 任务异常或未收到关闭请求便退出。
    pub failed: bool,
    /// consumer 清理结果。
    pub cleanup: ProxyCleanup,
}

/// 运行态发布、激活和关闭句柄；退出证据由独立 owner 持有。
pub struct RunningProxy {
    /// 准备时确定的普通消费者数，最终接流裁决不接受缺席任务。
    consumers: usize,
    /// 与任务退出串行化的本代接流和进展证据。
    activity: Arc<crate::activity::Activity>,
    /// 发布及后台收口仍需持有的 Redis 依赖。
    client: Arc<RedisClient>,
    /// 本入口允许追加消息的共享 Stream。
    stream: String,
    /// 永久关闭本代发布、领取和回收的信号。
    cancel: CancellationToken,
    /// 宿主发布的消费许可，不覆盖关闭信号。
    ready: CancellationToken,
    /// 多次关闭取较短预算的共享绝对截止。
    deadline: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
    /// 独立 owner 保留的任务退出及清理证据。
    outcome: tokio::sync::watch::Receiver<ProxyStopReport>,
    /// 发布时采用的可选近似长度上限，不保护 pending 正文。
    max_stream_len: Option<u64>,
    /// 本领域最大排干等待预算，宿主可以进一步缩短。
    drain_deadline_ms: u64,
}

/// 业务作用：解析具体 Redis Stream 坐标，为写入回包和扫描边界提供整数证据。
///
/// 参数说明：
/// - `id`：由连字符分隔毫秒与序号两个分量的文本身份。
///
/// 返回：两个非空 ASCII 十进制分量均位于 u64 范围时返回坐标，否则返回 None；0-0 是合法扫描坐标，写入成功须另行排除。
fn parse_stream_id(id: &str) -> Option<(u64, u64)> {
    let (milliseconds, sequence) = id.split_once('-')?;
    if milliseconds.is_empty()
        || sequence.is_empty()
        || !milliseconds.bytes().all(|byte| byte.is_ascii_digit())
        || !sequence.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((milliseconds.parse().ok()?, sequence.parse().ok()?))
}

impl RunningProxy {
    /// 业务作用：在固定消费与回收责任完整时执行宿主的同步接流裁决，与活动任务退出串行化。
    ///
    /// 参数说明：
    /// - `publish`：短小、不阻塞且不重入本 Proxy 的同步发布动作。
    ///
    /// 返回：全部固定任务仍存在且本代未关闭时返回 Some；否则不执行，不据此证明远端 Redis 可用或 handler 成功。
    pub fn with_running<T>(&self, publish: impl FnOnce() -> T) -> Option<T> {
        self.activity
            .with_tasks(self.consumers, 1, 0, || {
                let report = self.stop_report();
                if self.cancel.is_cancelled() || report.terminated || report.failed {
                    None
                } else {
                    Some(publish())
                }
            })
            .flatten()
    }
    /// 业务作用：读取 consumer、reclaim 和本地处理次数，不将其替代远端 PEL 证据。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地工作快照，取消或异常退出时没有结束证据的处理继续计入未完成。
    pub fn observation(&self) -> crate::RedisTaskObservation {
        self.activity.snapshot()
    }

    /// 业务作用：把主题、事件和业务内容编码为标准 Envelope，经 data 字段追加到共享 Stream。
    ///
    /// 参数说明：
    /// - `topic`：非空业务主题，供消费者选择处理器。
    /// - `event`：主题内的非空事件名。
    /// - `data`：可序列化的业务内容，写入信封的 data 字段。
    ///
    /// 返回：身份为空、入口关闭、序列化失败或展开时在发送前返回错误；成功只接受合法非零 Stream ID，不表示消费完成。
    /// XADD 调用失败或回包身份不合法时返回 PublishOutcomeUnknown，不自动重发；错误摘要不携带业务正文。
    /// 追加可按配置近似裁剪，不复验 PEL；其它生产者必须遵守信封格式，无法解析的内容按毒消息策略处置。
    pub async fn publish<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        data: &T,
    ) -> Result<String> {
        if topic.is_empty() || event.is_empty() {
            return Err(NasaRedisError::Config(
                "Proxy 发布的 topic/event 不能为空".into(),
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(NasaRedisError::NotExecuted("Proxy 已关闭".into()));
        }
        // 自定义 Serialize 属于业务代码，返回错误和展开都须在构造写入前收束，且不把原始错误正文带给调用方。
        let body = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let env = Envelope {
                topic: topic.to_string(),
                event: event.to_string(),
                data: serde_json::to_value(data)?,
                passthrough: None,
            };
            serde_json::to_vec(&env)
        })) {
            Ok(Ok(body)) => body,
            Ok(Err(error)) => {
                return Err(NasaRedisError::Codec(format!(
                    "Proxy 序列化失败: {:?}, line={}, column={}",
                    error.classify(),
                    error.line(),
                    error.column()
                )));
            }
            Err(_) => {
                return Err(NasaRedisError::Codec("Proxy 序列化展开".into()));
            }
        };
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.stream);
        //可选近似上限,防共享 stream 无界增长 OOM(ACK 只出 PEL,entry 留存)。
        if let Some(n) = self.max_stream_len {
            cmd.arg("MAXLEN").arg("~").arg(n);
        }
        cmd.arg("*").arg(DATA_FIELD).arg(body);
        let id: String = cmd.query_async(&mut self.client.conn()).await.map_err(
            |error: redis::RedisError| {
                NasaRedisError::PublishOutcomeUnknown(format!(
                    "Proxy XADD 未取得有效回包: {:?}",
                    error.kind()
                ))
            },
        )?;
        // 命令已经交给连接，异常成功值不能证明写入或未写入；不得授权调用方按确定未发送自动重放。
        if !matches!(parse_stream_id(&id), Some((milliseconds, sequence)) if milliseconds != 0 || sequence != 0)
        {
            return Err(NasaRedisError::PublishOutcomeUnknown(
                "Proxy XADD 返回非法条目身份".into(),
            ));
        }
        Ok(id)
    }

    /// 业务作用：宿主完成准备后一次性开放本代消费。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未关闭时开放成功；关闭后永久拒绝。
    pub fn activate(&self) -> bool {
        if self.cancel.is_cancelled() {
            return false;
        }
        self.ready.cancel();
        true
    }

    /// 业务作用：按宿主预算与本领域预算中较早的截止，永久关闭本代接流并通知独立收尾任务。
    ///
    /// 参数说明：
    /// - `deadline`：宿主分配的绝对截止；重复关闭只能缩短已有截止。
    ///
    /// 返回：关闭准入并发布停止信号；不等待退出或 PEL 清理，不把取消信号当作完成证据。
    pub fn begin_shutdown_until(&self, deadline: tokio::time::Instant) {
        self.activity.close();
        let local = tokio::time::Instant::now() + Duration::from_millis(self.drain_deadline_ms);
        let deadline = deadline.min(local);
        self.deadline.send_modify(|current| {
            *current = Some(current.map_or(deadline, |old| old.min(deadline)))
        });
        self.cancel.cancel();
    }

    /// 业务作用：使用领域预算启动关闭，不将取消信号视为完成证据。
    ///
    /// 参数说明：无。
    ///
    /// 返回：新发布和新消费已关闭。
    pub fn begin_shutdown(&self) {
        self.begin_shutdown_until(
            tokio::time::Instant::now() + Duration::from_millis(self.drain_deadline_ms),
        );
    }

    /// 业务作用：读取本代任务及清理的确定性状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：terminated 仅在取得全部任务退出证据后为 true；证据通道丢失时 failed 为 true。
    pub fn stop_report(&self) -> ProxyStopReport {
        let disconnected = self.outcome.has_changed().is_err();
        let mut report = *self.outcome.borrow();
        if disconnected && !report.terminated {
            report.failed = true;
        }
        report
    }

    /// 业务作用：等待 owner 发布最终退出结果，取消此等待不影响收口责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：所有任务已退出的报告；证据通道提前关闭时报告失败且不伪报退出。
    pub async fn wait_closed(&self) -> ProxyStopReport {
        let mut outcome = self.outcome.clone();
        loop {
            let report = *outcome.borrow_and_update();
            if report.terminated {
                return report;
            }
            if outcome.changed().await.is_err() {
                return ProxyStopReport {
                    failed: true,
                    ..report
                };
            }
        }
    }

    /// 业务作用：兼容独立入口，关闭并等待本代任务与可选 PEL 清理。
    ///
    /// 参数说明：无。
    ///
    /// 返回：退出结果通过日志记录；需要裁决时使用 begin_shutdown 与 wait_closed。
    pub async fn shutdown(self) {
        self.begin_shutdown();
        let report = self.wait_closed().await;
        if report.forced
            || report.failed
            || matches!(
                report.cleanup,
                ProxyCleanup::Unavailable | ProxyCleanup::Deadline
            )
        {
            tracing::warn!(
                forced = report.forced,
                failed = report.failed,
                cleanup = ?report.cleanup,
                "Proxy 非正常排干"
            );
        }
    }
}

impl Drop for RunningProxy {
    /// 业务作用：最后释放运行句柄时收回准入，关闭 owner 继续等待任务和保留必要 PEL。
    ///
    /// 参数说明：无。
    ///
    /// 返回：触发关闭，不以 Drop 作为退出证据。
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct ProxyCloseOwner {
    /// 保留到任务退出和清理裁决结束的连接来源。
    client: Arc<RedisClient>,
    /// 本代消费者所属 Stream。
    stream: String,
    /// 本代消费者所属共享组。
    group: String,
    /// 仅本次创建的消费者身份，清理不删除其它实例的消费者。
    names: Vec<String>,
    /// 唯一等待的消费与回收任务句柄。
    handles: Vec<JoinHandle<()>>,
    /// 预算耗尽或 owner 消失时提出终止请求的句柄。
    aborts: Vec<tokio::task::AbortHandle>,
    /// 关闭业务接流与后台新轮次的信号。
    cancel: CancellationToken,
    /// 宿主可持续缩短的共享绝对截止。
    deadline: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
    /// 无显式宿主截止时采用的领域排干预算。
    budget: Duration,
    /// 发布最终报告前一直保留的完成责任。
    report: crate::activity::CompletionOwner<ProxyStopReport>,
}

impl ProxyCloseOwner {
    /// 业务作用：独占本代 consumer/reclaim 的等待责任，在关闭预算内排干并保守清理 PEL。
    ///
    /// 参数说明：无。
    ///
    /// 返回：所有任务真实退出后发布报告；任务未退出期间继续持有 Redis 依赖。
    async fn run(mut self) {
        use futures::StreamExt;
        let mut tasks: futures::stream::FuturesUnordered<_> = self.handles.drain(..).collect();
        let mut report = ProxyStopReport::default();
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {},
            _ = tasks.next() => {
                // 消费循环没有正常自退合同，任一意外退出都关闭本组，不能继续报告健康。
                report.failed = true;
                self.cancel.cancel();
            },
        }
        self.report.send_replace(report);
        let deadline =
            (*self.deadline.borrow()).unwrap_or_else(|| tokio::time::Instant::now() + self.budget);
        while !tasks.is_empty() {
            match self.within_deadline(deadline, tasks.next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(_))) => report.failed = true,
                Ok(None) => break,
                Err(_) => {
                    report.forced = true;
                    for abort in &self.aborts {
                        abort.abort();
                    }
                    // abort 只是请求；owner 保留句柄与连接直到取得真实退出证据。
                    while let Some(result) = tasks.next().await {
                        if result.is_err_and(|error| !error.is_cancelled()) {
                            report.failed = true;
                        }
                    }
                    break;
                }
            }
        }
        if !report.forced && !report.failed {
            report.cleanup = self.cleanup(deadline).await;
        }
        report.terminated = true;
        self.report.send_replace(report);
    }

    /// 业务作用：以可缩短的共享截止约束同一个在途动作，预算更新时继续轮询原 Future。
    ///
    /// 参数说明：
    /// - `deadline`：本轮不可延长的初始绝对截止。
    /// - `future`：唯一在途操作；预算更新不重新创建或重发它。
    ///
    /// 返回：在截止前完成时返回原结果，到期返回 Err 并停止等待；不据此撤销已提交的外部副作用。
    async fn within_deadline<T>(
        &self,
        deadline: tokio::time::Instant,
        future: impl std::future::Future<Output = T>,
    ) -> std::result::Result<T, ()> {
        let mut changes = self.deadline.subscribe();
        tokio::pin!(future);
        loop {
            let current = changes
                .borrow_and_update()
                .unwrap_or(deadline)
                .min(deadline);
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(current) => return Err(()),
                result = &mut future => return Ok(result),
                _ = changes.changed() => {},
            }
        }
    }

    /// 业务作用：本代任务正常退出后，在同一 Redis 脚本内复验每个消费者没有 pending，再删除其元数据。
    ///
    /// 参数说明：
    /// - `deadline`：排干与清理共用的截止，后续更短预算同样生效。
    ///
    /// 返回：返回完成、存在 pending、证据不可用或到期分类；脚本或命令权限不足、回复异常均不视为空 PEL。
    /// 已完成的删除不会因后续失败回滚；脚本只保证每次复验与删除原子，不保证外部调用方之后不能重建同名消费者。
    async fn cleanup(&self, deadline: tokio::time::Instant) -> ProxyCleanup {
        // 空摘要与删除分开执行会留下重新分配 PEL 的窗口；原子复验只查询当前消费者的一条待确认记录。
        let script = redis::Script::new(
            r#"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2])
if type(pending) ~= 'table' then
    return redis.error_reply('PROXY pending reply is invalid')
end
if next(pending) ~= nil then
    return 1
end
local removed = redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
if removed ~= 0 then
    return redis.error_reply('PROXY consumer deletion reply is invalid')
end
return 0
"#,
        );
        let mut outcome = ProxyCleanup::Complete;
        for name in &self.names {
            // 排干和清理共用截止；预算耗尽后不再发起可能产生外部副作用的脚本。
            if tokio::time::Instant::now()
                >= self.deadline.borrow().unwrap_or(deadline).min(deadline)
            {
                return ProxyCleanup::Deadline;
            }
            let result: std::result::Result<redis::Value, _> = match self
                .within_deadline(
                    deadline,
                    script
                        .key(&self.stream)
                        .arg(&self.group)
                        .arg(name)
                        .invoke_async(&mut self.client.conn()),
                )
                .await
            {
                Ok(result) => result,
                Err(_) => return ProxyCleanup::Deadline,
            };
            // 只有脚本定义的两个整数状态有效，不能把任意成功回包转换成清理完成。
            match result {
                Ok(redis::Value::Int(0)) => {}
                Ok(redis::Value::Int(1)) => outcome = ProxyCleanup::Pending,
                _ => return ProxyCleanup::Unavailable,
            }
        }
        outcome
    }
}

impl Drop for ProxyCloseOwner {
    /// 业务作用：收尾 future 被销毁时收回发布准入并请求终止所有消费任务。
    ///
    /// 参数说明：无。
    ///
    /// 返回：不再允许旧句柄发布；未获得 join 结果时保留失败和未知退出语义，不清理 PEL。
    fn drop(&mut self) {
        // 先关闭业务准入，再请求子任务终止，不能因 owner 丢失而继续接受无消费者归属的工作。
        self.cancel.cancel();
        for abort in &self.aborts {
            abort.abort();
        }
    }
}

/// 无 `data` 字段或无法解码 Envelope 的 entry；保留身份、失败原因和原始字段交给 [`dispose_bad`]。
/// 此类 entry 已进入 PEL，确定性解析失败时直接按毒消息策略处置，避免无效重投长期占用回收名额。
struct BadEntry {
    /// 本轮取得的条目身份，用于源确认与 DLQ 关联。
    id: String,
    /// 无法恢复为标准信封的原因。
    reason: String,
    /// 原始 field/value 字节对，保留顺序、重复 field 和二进制字节，供 DLQ 完整还原消息。
    raw: Vec<(Vec<u8>, Vec<u8>)>,
}

/// `parse_entries` 结果:成功 `(id, Envelope)` 与坏 entry 分流。
struct ParsedEntries {
    /// 可交给业务目录的信封及来源身份。
    ok: Vec<(String, Envelope)>,
    /// 已取得身份但无法解码的条目，按毒消息策略处理。
    bad: Vec<BadEntry>,
}

/// 业务作用：从 Redis 条目集合分离可解码的 Envelope 和需要按毒消息策略处置的条目。
///
/// 参数说明：
/// - `entries`：XREADGROUP、XAUTOCLAIM 或精确重读中的条目集合。
/// - `allow_deleted`：仅两元素 XAUTOCLAIM 回复可包含没有身份的已删除条目占位。
///
/// 返回：完整验证条目结构、递增非零身份和原始字段后，返回可解码信封及坏信封；协议异常整批返回错误，不能据此授权 ACK 或 DLQ。
/// 允许的匿名删除占位仅跳过，不证明其 PEL 已清理，也不产生可确认身份。
fn parse_entries(entries: &redis::Value, allow_deleted: bool) -> Result<ParsedEntries> {
    let mut ok = Vec::new();
    let mut bad = Vec::new();
    let invalid = || NasaRedisError::Parsing("PROXY 条目回复结构或身份无效".into());
    let redis::Value::Array(items) = entries else {
        return Err(invalid());
    };
    let mut previous = (0, 0);
    for item in items {
        // 早期 XAUTOCLAIM 可能只返回匿名 Nil；没有来源身份时不能补造 ACK 或 DLQ。
        if allow_deleted && matches!(item, redis::Value::Nil) {
            continue;
        }
        let redis::Value::Array(pair) = item else {
            return Err(invalid());
        };
        let [redis::Value::BulkString(id_bytes), redis::Value::Array(fields)] = pair.as_slice()
        else {
            return Err(invalid());
        };
        let id = std::str::from_utf8(id_bytes).map_err(|_| invalid())?;
        let coordinate = parse_stream_id(id).ok_or_else(invalid)?;
        // 三种读取均按 ID 升序返回；重复或倒序身份意味着不能确定本批的责任集合。
        if coordinate <= previous || fields.is_empty() || fields.len() % 2 != 0 {
            return Err(invalid());
        }
        previous = coordinate;
        let mut data_bytes: Option<&[u8]> = None;
        let mut duplicate_data = false;
        let mut raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for pair in fields.chunks_exact(2) {
            let [redis::Value::BulkString(field), redis::Value::BulkString(value)] = pair else {
                return Err(invalid());
            };
            if field == DATA_FIELD.as_bytes() {
                duplicate_data |= data_bytes.replace(value).is_some();
            }
            raw.push((field.clone(), value.clone()));
        }
        let id = id.to_owned();
        // Redis 允许重复字段，但多个 data 不能唯一决定业务信封；保留全部原始字节供毒消息策略处理。
        if duplicate_data {
            bad.push(BadEntry {
                id,
                reason: "存在多个 data 字段".into(),
                raw,
            });
            continue;
        }
        let Some(bytes) = data_bytes else {
            bad.push(BadEntry {
                id,
                reason: "缺 data 字段(非标准 Envelope wire)".to_string(),
                raw,
            });
            continue;
        };
        match serde_json::from_slice::<Envelope>(bytes) {
            Ok(env) => ok.push((id, env)),
            Err(e) => {
                tracing::error!(id, category = ?e.classify(), "PROXY 信封无法解码，进入毒消息策略");
                bad.push(BadEntry {
                    id,
                    reason: format!(
                        "envelope 解码失败: {:?}, line={}, column={}",
                        e.classify(),
                        e.line(),
                        e.column()
                    ),
                    raw,
                });
            }
        }
    }
    Ok(ParsedEntries {
        ok,
        bad,
    })
}

/// 业务作用：将已领取消息按主题和事件分桶，逐桶执行对应 handler 并汇总未完成的 ID。
///
/// 参数说明：
/// - `shared`：本组固定 handler 目录、超时和未注册事件策略。
/// - `batch`：本轮已领取且信封可解码的消息，不是待发送 Redis 命令。
///
/// 返回：返回处理失败、超时或按配置保留的未注册消息 ID；其余 ID 由调用方尝试确认。桶之间没有全局顺序保证，超时不会撤销已发生的业务副作用。
async fn dispatch(shared: &ProxyShared, batch: Vec<(String, Envelope)>) -> Vec<String> {
    let mut buckets: HashMap<(String, String), Vec<(String, serde_json::Value)>> = HashMap::new();
    for (id, env) in batch {
        buckets
            .entry((env.topic, env.event))
            .or_default()
            .push((id, env.data));
    }
    let mut failed = Vec::new();
    for (key, items) in buckets {
        match shared.handlers.get(&key) {
            Some(h) => {
                let bucket_ids: Vec<String> = items.iter().map(|(id, _)| id.clone()).collect();
                //handler 超时强制——超时整桶留 PEL(转 failed),不让挂起 handler 永占 task。
                let dur = Duration::from_millis(shared.cfg.handler_timeout_ms);
                match tokio::time::timeout(dur, h(items)).await {
                    Ok(f) => failed.extend(f),
                    Err(_) => {
                        tracing::warn!(topic = %key.0, event = %key.1, timeout_ms = shared.cfg.handler_timeout_ms, "PROXY handler 超时,整桶转重投(留 PEL)");
                        failed.extend(bucket_ids);
                    }
                }
            }
            None => {
                // 共享组中当前节点缺少处理器并不代表其它节点也没有；按配置保留 PEL 可以等待再次竞争，
                // 但不保证下一次认领者拥有该处理器，不能把此策略当成可靠路由。
                tracing::warn!(topic = %key.0, event = %key.1, requeue = shared.cfg.requeue_unregistered, "PROXY 本节点无注册 handler");
                if shared.cfg.requeue_unregistered {
                    failed.extend(items.into_iter().map(|(id, _)| id));
                }
            }
        }
    }
    failed
}

/// 业务作用：确认本轮可结束的共享组 PEL 责任，空集合不发送命令。
///
/// 参数说明：
/// - `shared`：提供固定来源、Stream 和消费组。
/// - `ids`：本轮准备从 PEL 确认移除的条目身份。
///
/// 返回：等待 XACK 结束，对非整数或超出本次数量的回复记录固定告警，传输错误只记录类别；不返回逐条确认结果。
/// ACK 不删除 Stream 正文；错误或非法回复不证明确认未发生，不能保证同一身份后续一定重投。
async fn ack(shared: &ProxyShared, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    let mut cmd = redis::cmd("XACK");
    cmd.arg(&shared.stream).arg(&shared.group);
    for id in ids {
        cmd.arg(id);
    }
    let r: std::result::Result<redis::Value, redis::RedisError> =
        cmd.query_async(&mut shared.client.conn()).await;
    // Redis 的确认数量只能是本次身份集合内的非负整数，通用数值转换会掩盖异常回复类型。
    match r {
        Ok(redis::Value::Int(count))
            if usize::try_from(count).is_ok_and(|count| count <= ids.len()) => {}
        Ok(_) => tracing::warn!("PROXY XACK 回复计数无效，确认结果未知"),
        Err(e) => tracing::warn!(kind = ?e.kind(), "PROXY XACK 未取得有效回复，确认结果未知"),
    }
}

/// 业务作用：竞争领取本组新消息，依次处置坏条目、调用业务并确认成功子集，维护本地处理责任。
///
/// 参数说明：
/// - `shared`：冻结的来源、处理器和消费策略。
/// - `consumer`：本代独立消费者身份。
/// - `cancel`：在轮次边界停止新领取；已开始的读取及其后续处理由收尾任务等待或强停。
///
/// 返回：关闭后等待已开始的领取回包和本轮处置，空闲与退避等待可立即结束；收尾预算耗尽时仍可强停。
/// 正常轮次记录处理结束，强停后已登记但未结束的责任保留；尚未收到领取回复时不能据本地计数推断远端 PEL。
async fn consumer_loop(shared: Arc<ProxyShared>, consumer: String, cancel: CancellationToken) {
    loop {
        // 领取可能已改变远端 PEL；关闭只阻止下一轮，不能丢弃在途回复后提前进入消费者清理。
        if cancel.is_cancelled() {
            break;
        }
        let idle = match read_new(&shared, &consumer).await {
            Ok(parsed) if !parsed.ok.is_empty() || !parsed.bad.is_empty() => {
                let count = parsed.ok.len() + parsed.bad.len();
                shared.activity.accept(count, 0);
                // 坏信封仍占用 PEL，须按配置完成本轮处置，不能仅因停止新领取而遗失其身份。
                if !parsed.bad.is_empty() {
                    dispose_bad(&shared, &parsed.bad).await;
                }
                if !parsed.ok.is_empty() {
                    let ids: Vec<String> = parsed.ok.iter().map(|(id, _)| id.clone()).collect();
                    let failed = dispatch(&shared, parsed.ok).await;
                    // 仅确认成功子集；失败身份保留 PEL，集合查询避免大批量筛选的平方成本。
                    let failed: std::collections::HashSet<String> = failed.into_iter().collect();
                    let ok: Vec<String> = ids.into_iter().filter(|i| !failed.contains(i)).collect();
                    ack(&shared, &ok).await;
                }
                shared.activity.finish(count, false);
                continue;
            }
            Ok(_) => {
                shared.activity.finish(0, false);
                Duration::from_millis(shared.cfg.poll_idle_ms.max(1))
            }
            Err(e) => {
                tracing::warn!(err = %e, "PROXY XREADGROUP 失败,短暂退避");
                Duration::from_millis(200)
            }
        };
        // 此时没有本轮处置在途，取消空闲或退避不放弃消息责任，也不消耗剩余排干预算。
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(idle) => {},
        }
    }
}

/// 业务作用：以非阻塞 XREADGROUP 为当前消费者领取共享 Stream 的新消息。
///
/// 参数说明：
/// - `shared`：固定 Stream、消费组、批量与 Redis 连接入口。
/// - `consumer`：本次领取写入 PEL 的消费者身份。
///
/// 返回：Nil 或空集合表示本轮没有新消息；仅接受当前 Stream 的完整单项回复及不超批量的条目集合。
/// 传输、来源身份或条目解析失败返回错误，不把协议异常记作有效空轮，也不授权后续 ACK 或 DLQ。
async fn read_new(shared: &ProxyShared, consumer: &str) -> Result<ParsedEntries> {
    // 不使用 BLOCK，避免一次冷流等待占用共享多路复用连接。
    let v: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&shared.group)
        .arg(consumer)
        .arg("COUNT")
        .arg(shared.cfg.batch_size)
        .arg("STREAMS")
        .arg(&shared.stream)
        .arg(">")
        .query_async(&mut shared.client.conn())
        .await?;
    let empty = || ParsedEntries {
        ok: Vec::new(),
        bad: Vec::new(),
    };
    let invalid = || NasaRedisError::Parsing("PROXY XREADGROUP 回复来源或结构无效".into());
    let (stream, entries) = match &v {
        redis::Value::Nil => return Ok(empty()),
        redis::Value::Array(streams) => {
            if streams.is_empty() {
                return Ok(empty());
            }
            let [redis::Value::Array(pair)] = streams.as_slice() else {
                return Err(invalid());
            };
            let [stream, entries] = pair.as_slice() else {
                return Err(invalid());
            };
            (stream, entries)
        }
        redis::Value::Map(pairs) => {
            if pairs.is_empty() {
                return Ok(empty());
            }
            let [(stream, entries)] = pairs.as_slice() else {
                return Err(invalid());
            };
            (stream, entries)
        }
        _ => return Err(invalid()),
    };
    // 查询只授权一个固定来源；错误来源或超量回包不能进入业务处理与确认路径。
    if !matches!(stream, redis::Value::BulkString(name) if name == shared.stream.as_bytes())
        || !matches!(entries, redis::Value::Array(items) if items.len() <= shared.cfg.batch_size)
    {
        return Err(invalid());
    }
    parse_entries(entries, false)
}

/// 业务作用：按有界周期扫描闲置 PEL，将每轮回收交给重投与毒消息处置流程。
///
/// 参数说明：
/// - `shared`：共享来源、处理器与闲置阈值等冻结配置。
/// - `consumer`：本代回收消费者身份，与普通新消息消费者区分。
/// - `cancel`：终止后续回收轮次的关闭信号，不撤销已开始的处置。
///
/// 返回：观察到关闭后退出；连续使用已验证的扫描游标，服务器返回 0-0 后从头扫描。
/// 轮次失败记录后等待下一周期，错过的 tick 跳过，不堆积补跑；取消不打断已进入的单轮处置。
async fn reclaim_loop(shared: Arc<ProxyShared>, consumer: String, cancel: CancellationToken) {
    let period = Duration::from_millis((shared.cfg.reclaim_min_idle_ms / 2).clamp(200, 10_000));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = "0-0".to_owned();
    loop {
        // 上一轮可能耗尽整个周期；tick 与关闭同时就绪时须先停止，不能再认领新的 PEL。
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                if let Err(e) = reclaim_once(&shared, &consumer, &mut cursor).await {
                    tracing::warn!(err = %e, "PROXY reclaim 失败,下轮重试");
                }
            }
        }
    }
}

/// 业务作用：接管一批闲置 PEL，查询投递次数并分别执行重投、毒消息处置和成功子集确认。
///
/// 参数说明：
/// - `shared`：本组来源、处理器、闲置阈值和毒消息策略。
/// - `consumer`：本轮 XAUTOCLAIM 接管目标身份。
/// - `cursor`：本代连续扫描起点；完整领取回复验证后推进，0-0 表示下一轮从头开始。
///
/// 返回：读取、回包结构与计数查询错误向上传播；回包完整验证前不执行消息处置。
/// 正常返回的本轮处理包括计数查询失败均结束本地记账，不承诺逐条 ACK 或 DLQ 成功；Future 中途销毁的责任保留未完成。
async fn reclaim_once(shared: &ProxyShared, consumer: &str, cursor: &mut String) -> Result<()> {
    let start = parse_stream_id(cursor)
        .ok_or_else(|| NasaRedisError::Config("PROXY 回收扫描起点无效".into()))?;
    // 连续传递服务器扫描位置，避免有限扫描预算反复耗在尚不满足 idle 的前缀。
    let v: redis::Value = redis::cmd("XAUTOCLAIM")
        .arg(&shared.stream)
        .arg(&shared.group)
        .arg(consumer)
        .arg(shared.cfg.reclaim_min_idle_ms)
        .arg(cursor.as_str())
        .arg("COUNT")
        .arg(shared.cfg.batch_size)
        .query_async(&mut shared.client.conn())
        .await?;
    let invalid = || NasaRedisError::Parsing("PROXY XAUTOCLAIM 回复结构或身份无效".into());
    let redis::Value::Array(parts) = &v else {
        return Err(invalid());
    };
    if parts.len() != 2 && parts.len() != 3 {
        return Err(invalid());
    }
    let redis::Value::BulkString(next_cursor) = &parts[0] else {
        return Err(invalid());
    };
    let next_cursor = std::str::from_utf8(next_cursor).map_err(|_| invalid())?;
    let next = parse_stream_id(next_cursor).ok_or_else(invalid)?;
    if next != (0, 0) && next <= start {
        return Err(invalid());
    }
    let entries = &parts[1];
    if !matches!(entries, redis::Value::Array(items) if items.len() <= shared.cfg.batch_size) {
        return Err(invalid());
    }
    let parsed = parse_entries(entries, parts.len() == 2)?;
    let delivered: std::collections::HashSet<_> = parsed
        .ok
        .iter()
        .map(|(id, _)| id)
        .chain(parsed.bad.iter().map(|entry| &entry.id))
        .filter_map(|id| parse_stream_id(id))
        .collect();
    // 交付身份必须落在本轮已扫描区间；区间矛盾不能授权处置，也不能跳过尚未证实的后续位置。
    if delivered
        .iter()
        .any(|id| *id < start || (next != (0, 0) && *id >= next))
    {
        return Err(invalid());
    }
    // 新协议的删除列表只能说明相应正文已缺席；它不能与实际交付消息重叠，也不能提供毒消息处置身份。
    if let Some(deleted) = parts.get(2) {
        let redis::Value::Array(deleted) = deleted else {
            return Err(invalid());
        };
        let mut previous = (0, 0);
        for id in deleted {
            let redis::Value::BulkString(id) = id else {
                return Err(invalid());
            };
            let coordinate = std::str::from_utf8(id)
                .ok()
                .and_then(parse_stream_id)
                .ok_or_else(invalid)?;
            if coordinate <= previous
                || coordinate < start
                || (next != (0, 0) && coordinate >= next)
                || delivered.contains(&coordinate)
            {
                return Err(invalid());
            }
            previous = coordinate;
        }
    }
    // 完整回复已确认扫描进度；后续处理失败的消息仍在 PEL，可在下一次扫描回绕后再次竞争。
    *cursor = next_cursor.to_owned();
    let count = parsed.ok.len() + parsed.bad.len();
    shared.activity.accept(count, 0);
    // 坏 entry(无 data / 非 Envelope JSON,可能来自崩溃 consumer 的 PEL)立即处置,不卡 PEL。
    if !parsed.bad.is_empty() {
        dispose_bad(shared, &parsed.bad).await;
    }
    let batch = parsed.ok;
    if batch.is_empty() {
        shared.activity.finish(count, false);
        return Ok(());
    }
    let ids: Vec<String> = batch.iter().map(|(id, _)| id.clone()).collect();
    // 查每条重投次数(XPENDING),分流 poison vs 重投
    let counts = match pending_counts(shared, &ids).await {
        Ok(counts) => counts,
        Err(error) => {
            // 本轮已明确结束，源消息仍可重投；本地完成次数不能与远端确认成功混为一谈。
            // 仅正常错误分支结束记账，中途取消尚无结束证据时不执行此步骤。
            shared.activity.finish(count, false);
            return Err(error);
        }
    };
    let mut to_dispatch = Vec::new();
    let mut poison_ids = Vec::new();
    for (id, env) in batch {
        // 缺少计数时不以猜测授权 Drop 或 DLQ，当前路径按一进入普通重投。
        // 该缺省不证明 PEL 持有者，也不能保证实际投递次数；持续缺失必须保留可观测性。
        let n = match counts.get(&id).copied() {
            Some(n) => n,
            None => {
                tracing::warn!(
                    id,
                    "PROXY XPENDING 未返回该 id 的重投计数(协议异常/竞态),保守按 1 重投"
                );
                1
            }
        };
        // 计数保持服务器返回的完整宽度；窄化可能把已经超限的消息重新送入普通重投。
        if n > u64::from(shared.cfg.max_redeliver) {
            poison_ids.push(id);
        } else {
            to_dispatch.push((id, env));
        }
    }
    if !poison_ids.is_empty() {
        handle_poison(shared, &poison_ids).await;
    }
    if !to_dispatch.is_empty() {
        let d_ids: Vec<String> = to_dispatch.iter().map(|(id, _)| id.clone()).collect();
        let failed = dispatch(shared, to_dispatch).await;
        let failed: std::collections::HashSet<String> = failed.into_iter().collect();
        let ok: Vec<String> = d_ids.into_iter().filter(|i| !failed.contains(i)).collect();
        ack(shared, &ok).await;
    }
    shared.activity.finish(count, false);
    Ok(())
}

/// 业务作用：对本批每个 ID 精确指定 XPENDING 查询范围，供重投与毒消息策略读取投递次数。
///
/// 参数说明：
/// - `shared`：提供同一 Stream 和消费组的连接入口。
/// - `ids`：本轮待核对的条目身份，查询结果按命令顺序关联。
///
/// 返回：空输入返回空映射；每个回复必须为零行或一条身份一致、idle/投递次数非负的完整行，异常整批返回错误。
/// 合法空行不进入映射；投递次数保留为 u64，不窄化为配置上限的 u32。结果只是查询时刻的 PEL 快照，不提供持有权或原子处置保证。
async fn pending_counts(shared: &ProxyShared, ids: &[String]) -> Result<HashMap<String, u64>> {
    let mut out = HashMap::new();
    if ids.is_empty() {
        return Ok(out);
    }
    let mut pipe = redis::pipe();
    for id in ids {
        pipe.cmd("XPENDING")
            .arg(&shared.stream)
            .arg(&shared.group)
            .arg(id)
            .arg(id)
            .arg(1);
    }
    let mut conn = shared.client.conn();
    let results: Vec<redis::Value> =
        redis::aio::ConnectionLike::req_packed_commands(&mut conn, &pipe, 0, ids.len()).await?;
    let invalid = || NasaRedisError::Parsing("PROXY XPENDING 回复结构、身份或计数无效".into());
    // 查询槽位和身份必须同时一致，否则错误行可能授权丢弃另一条源消息。
    if results.len() != ids.len() {
        return Err(invalid());
    }
    for (id, res) in ids.iter().zip(results) {
        let redis::Value::Array(rows) = res else {
            return Err(invalid());
        };
        if rows.is_empty() {
            continue;
        }
        let [redis::Value::Array(columns)] = rows.as_slice() else {
            return Err(invalid());
        };
        let [redis::Value::BulkString(reply_id), redis::Value::BulkString(_consumer), redis::Value::Int(idle), redis::Value::Int(deliveries)] =
            columns.as_slice()
        else {
            return Err(invalid());
        };
        if reply_id != id.as_bytes() || *idle < 0 {
            return Err(invalid());
        }
        let deliveries = u64::try_from(*deliveries).map_err(|_| invalid())?;
        out.insert(id.clone(), deliveries);
    }
    Ok(out)
}

/// 业务作用：按配置确认丢弃超限消息，或先尝试把可解析正文转存 DLQ 再确认源 PEL。
///
/// 参数说明：
/// - `shared`：本组来源、固定毒消息策略及 DLQ 命名依据。
/// - `ids`：本轮判为超过投递上限的源条目身份。
///
/// 返回：空输入不发送；Drop 直接尝试 ACK。Dlq 逐项验证重读身份、唯一正文及写入回复后才尝试 ACK，失败时保留源待确认责任。
/// 转存与确认非事务，部分写入或 ACK 失败可能留下重复 DLQ，接收方须幂等。
async fn handle_poison(shared: &ProxyShared, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    match shared.cfg.poison {
        ProxyPoison::Drop => {
            tracing::error!(?ids, "PROXY 毒消息超重投上限,按 Drop 策略 XACK 丢弃");
            ack(shared, ids).await;
        }
        ProxyPoison::Dlq => {
            // 转存 payload 到 {stream}:dlq 后 XACK 源(best-effort)。**批量化**——
            // 循环内逐条 XRANGE+XADD 会放大往返并制造半完成窗口，因此先用一条 pipeline 批量读取全部原 entry，
            // ② 一条 pipeline 批量 XADD 写 DLQ(同 stream/同 dlq 单 key,cluster 同 slot)。
            let dlq = format!("{}:dlq", shared.stream);
            let mut conn = shared.client.conn();
            // ① 批量 XRANGE
            let mut rpipe = redis::pipe();
            for id in ids {
                rpipe.cmd("XRANGE").arg(&shared.stream).arg(id).arg(id);
            }
            let ranges: Vec<redis::Value> = match redis::aio::ConnectionLike::req_packed_commands(
                &mut conn,
                &rpipe,
                0,
                ids.len(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    // 整批读失败:**不 ACK**(源留 PEL 下轮重投),不静默丢。
                    tracing::warn!(?ids, err = %e, "PROXY DLQ 批量 XRANGE 失败,本批不 ACK 待重投");
                    return;
                }
            };
            // 每条 XRANGE 都只授权对应的源 ID；总数相同不能替代逐项身份一致。
            if ranges.len() != ids.len() {
                tracing::warn!("PROXY DLQ 重读回复数量异常，保留源 PEL");
                return;
            }
            let mut bodies: Vec<Vec<u8>> = Vec::new();
            for (id, range) in ids.iter().zip(&ranges) {
                let parsed = match parse_entries(range, false) {
                    Ok(parsed) => parsed,
                    Err(_) => {
                        tracing::warn!(id, "PROXY DLQ 重读协议异常，保留源 PEL");
                        return;
                    }
                };
                let [(entry_id, env)] = parsed.ok.as_slice() else {
                    // 正文缺席或不能唯一解码时没有完整转存证据，不能用 ACK 丢弃源责任。
                    tracing::warn!(id, "PROXY DLQ 重读没有唯一信封，保留源 PEL");
                    return;
                };
                if !parsed.bad.is_empty() || entry_id != id {
                    tracing::warn!(id, "PROXY DLQ 重读身份不一致，保留源 PEL");
                    return;
                }
                match serde_json::to_vec(env) {
                    Ok(body) => bodies.push(body),
                    Err(e) => {
                        tracing::warn!(id, category = ?e.classify(), "PROXY DLQ 信封编码失败，保留源 PEL");
                        return;
                    }
                }
            }
            // ③ 批量 XADD 到 DLQ(此时 bodies 非空且数量 == ids)
            let mut apipe = redis::pipe();
            for b in &bodies {
                apipe
                    .cmd("XADD")
                    .arg(&dlq)
                    .arg("*")
                    .arg(DATA_FIELD)
                    .arg(b.as_slice());
            }
            // 每个槽位必须给出有效写入身份；服务器错误或畸形成功值都不足以解除源责任。
            match redis::aio::ConnectionLike::req_packed_commands(
                &mut conn,
                &apipe,
                0,
                bodies.len(),
            )
            .await
            {
                Ok(v) if all_pipeline_replies_ok(&v, bodies.len()) => {}
                Ok(_) => {
                    tracing::warn!(?ids, "PROXY DLQ XADD 缺少完整有效的写入回复，保留源 PEL");
                    return;
                }
                Err(e) => {
                    tracing::warn!(?ids, kind = ?e.kind(), "PROXY DLQ XADD 未取得完整回复，保留源 PEL");
                    return;
                }
            }
            tracing::error!(?ids, dlq = %dlq, "PROXY 毒消息转存回复通过，开始确认源 PEL");
            ack(shared, ids).await;
        }
    }
}

/// 业务作用：核对每条 DLQ 写入均取得有效 XADD 身份，供处置流程决定是否可以继续确认源消息。
///
/// 参数说明：
/// - `values`：按物理批次顺序取得的 Redis 回复。
/// - `expected`：本批预期命令数，必须与实际回复数一致。
///
/// 返回：数量相符且每项都是合法非零 Stream ID 时返回 true；错误、缺失或畸形回复返回 false，源消息继续保留。
/// 回复缺乏证据不代表目标没有写入，下一次转存仍可能重复，不能据此自动回滚或认定未执行。
fn all_pipeline_replies_ok(values: &[redis::Value], expected: usize) -> bool {
    values.len() == expected
        && values.iter().all(|value| {
            let id = match value {
                redis::Value::BulkString(bytes) => match std::str::from_utf8(bytes) {
                    Ok(id) => id,
                    Err(_) => return false,
                },
                redis::Value::SimpleString(id) => id.as_str(),
                _ => return false,
            };
            // 非错误回复也可能没有提供写入身份，只有完整非零坐标才能继续源 ACK。
            matches!(parse_stream_id(id), Some((ms, sequence)) if ms != 0 || sequence != 0)
        })
}

/// 业务作用：把 DLQ 中的原始字段字节编码为可逆的标准 Base64 文本。
///
/// 参数说明：
/// - `bytes`：需要原样保留的任意字段或值字节。
///
/// 返回：返回 RFC 4648 字母表和等号填充的编码；空输入返回空字符串，不进行 UTF-8 转换或丢失字节。
fn base64_encode(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(T[b0 >> 2] as char);
        out.push(T[((b0 & 0b11) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 {
            T[((b1 & 0b1111) << 2) | (b2 >> 6)] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[b2 & 0b111111] as char
        } else {
            '='
        });
    }
    out
}

/// 业务作用：按毒消息策略处置不能解码为标准信封的条目，DLQ 使用身份、原因及可逆原始字段记录。
///
/// 参数说明：
/// - `shared`：固定来源、Stream、消费组及毒消息策略。
/// - `bad`：已经取得身份且判定无法解析的条目集合，包含原始字段和失败原因。
///
/// 返回：空集合不发送；Drop 尝试确认丢弃，Dlq 在批量转存回复检查通过后才尝试 ACK。转存与确认不原子，失败可能部分完成或重复写入，不能自动解释为无副作用。
async fn dispose_bad(shared: &ProxyShared, bad: &[BadEntry]) {
    if bad.is_empty() {
        return;
    }
    let ids: Vec<String> = bad.iter().map(|b| b.id.clone()).collect();
    match shared.cfg.poison {
        ProxyPoison::Drop => {
            tracing::error!(
                ?ids,
                "PROXY 不可解析 entry(无 data/非 Envelope),按 Drop 策略 XACK 丢弃"
            );
        }
        ProxyPoison::Dlq => {
            let dlq = format!("{}:dlq", shared.stream);
            let mut conn = shared.client.conn();
            let mut apipe = redis::pipe();
            for b in bad {
                //**lossless** raw——pairs 数组保序/保重复 field;`*_utf8`(lossy)排障友好,
                // `*_b64`(base64)是权威值(还原二进制/非 UTF-8 字节)。
                let raw: Vec<serde_json::Value> = b
                    .raw
                    .iter()
                    .map(|(k, v)| {
                        serde_json::json!({
                            "field_utf8": String::from_utf8_lossy(k),
                            "field_b64": base64_encode(k),
                            "value_utf8": String::from_utf8_lossy(v),
                            "value_b64": base64_encode(v),
                        })
                    })
                    .collect();
                let body = serde_json::to_vec(&serde_json::json!({
                    "_proxy_dlq": "unparseable",
                    "reason": b.reason,
                    "stream": shared.stream,
                    "group": shared.group,
                    "id": b.id,
                    "raw": raw,
                }))
                .unwrap_or_default();
                apipe
                    .cmd("XADD")
                    .arg(&dlq)
                    .arg("*")
                    .arg(DATA_FIELD)
                    .arg(body);
            }
            // 原始字段必须取得逐条有效写入身份后才能确认源 PEL；不完整回复不能视为未执行或成功。
            match redis::aio::ConnectionLike::req_packed_commands(&mut conn, &apipe, 0, bad.len())
                .await
            {
                Ok(v) if all_pipeline_replies_ok(&v, bad.len()) => {}
                Ok(_) => {
                    tracing::warn!(?ids, "PROXY 不可解析条目转存缺少完整有效回复，保留源 PEL");
                    return;
                }
                Err(e) => {
                    tracing::warn!(?ids, kind = ?e.kind(), "PROXY 不可解析条目转存未取得完整回复，保留源 PEL");
                    return;
                }
            }
            tracing::error!(?ids, dlq = %dlq, "PROXY 不可解析条目转存回复通过，开始确认源 PEL");
        }
    }
    ack(shared, &ids).await;
}
