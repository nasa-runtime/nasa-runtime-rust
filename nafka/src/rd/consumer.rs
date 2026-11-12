//! 底层 BaseConsumer、消息借用视图与 owner 控制适配。

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::config::KafkaConfig;
use crate::error::{NafkaError, Result};
use crate::metrics::MetricsSink;
use crate::types::{StartOffset, Tp};

use super::config::consumer_config;

/// poll loop 可恢复性分类后的底层错误。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RdPollError {
    /// 临时传输、协调器或 metadata 错误；owner 应按预算重建 consumer。
    Transient(String),
    /// 认证、授权、fencing 或底层 fatal 错误；owner 不得自动重建。
    Fatal(String),
}

impl std::fmt::Display for RdPollError {
    /// 业务作用：输出不暴露底层类型的分类文本。
    ///
    /// 参数说明：
    /// - `formatter`: 接收错误分类与已有原因文本的格式化目标。
    ///
    /// 返回：写出临时或致命前缀和保存的原因；目标写入失败时传播格式错误，不额外脱敏原因文本。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(message) => write!(formatter, "transient poll error: {message}"),
            Self::Fatal(message) => write!(formatter, "fatal poll error: {message}"),
        }
    }
}

/// owner 可观察的 rebalance 结果类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RdRebalanceKind {
    /// 新 assignment 已由底层安装完成。
    Assigned,
    /// revoke 已由底层执行完成。
    Revoked,
    /// 协调器返回 rebalance 错误。
    Error,
}

/// 单槽 rebalance mailbox 事件。
#[derive(Clone, Debug)]
pub(crate) struct RdRebalanceEvent {
    /// 每次 callback 单调递增的 owner epoch。
    pub(crate) epoch: u64,
    /// 事件类型。
    pub(crate) kind: RdRebalanceKind,
    /// callback 提供的实际分区集合。
    pub(crate) partitions: Vec<Tp>,
    /// 错误事件的安全诊断文本。
    pub(crate) error: Option<String>,
    /// revoke 窗口内已同步提交成功的安全前沿。
    pub(crate) committed: BTreeMap<Tp, i64>,
}

/// pre-rebalance 提交结果，等待 post callback 合并进单槽事件。
struct PreRebalanceResult {
    /// 成功时的已提交 offsets；失败时为空。
    committed: BTreeMap<Tp, i64>,
    /// 提交失败或超过时长预算的诊断文本。
    error: Option<String>,
}

/// revoke 窗口内有硬截止时间的底层提交结果。
#[derive(Clone, Debug, PartialEq, Eq)]
enum BoundedCommitOutcome {
    /// broker 已在窗口内确认全部安全前沿。
    Committed,
    /// broker 或客户端在窗口内返回确定错误。
    Failed(String),
    /// 专用结果队列在截止时间内没有得到提交结果。
    TimedOut,
}

/// 后台 revoke commit 生命周期守卫，确保成功、错误或 panic 都释放单飞门禁。
struct PreCommitGuard {
    /// 被任务持有到提交结束的 owner consumer。
    owner: Arc<BaseConsumer<OwnerConsumerContext>>,
}

impl Drop for PreCommitGuard {
    /// 业务作用：释放当前 consumer 的 revoke commit 单飞门禁。
    ///
    /// 参数说明：无。
    ///
    /// 返回：实际后台提交作用域退出时允许后续提交，包括异常展开路径；不把已经超时的旧结果重新确认为成功。
    fn drop(&mut self) {
        self.owner
            .context()
            .pre_commit_in_flight
            .store(false, Ordering::Release);
    }
}

/// 底层 consumer context，只把合并后的 rebalance 事实交给 owner。
struct OwnerConsumerContext {
    /// 下一次 callback epoch。
    epoch: AtomicU64,
    /// 当前 poll 周期最后一个事件；容量恒为一。
    mailbox: Mutex<Option<RdRebalanceEvent>>,
    /// owner 最近发布的安全提交快照。
    pending_offsets: Mutex<BTreeMap<Tp, i64>>,
    /// Revoke 前置提交结果。
    pre_rebalance: Mutex<Option<PreRebalanceResult>>,
    /// revoke 同步提交时长预算，用于越界观测。
    rebalance_commit_timeout: Duration,
    /// 构造完成后回填的 owner consumer 弱引用，供有界后台提交安全持有生命周期。
    owner_consumer: OnceLock<Weak<BaseConsumer<OwnerConsumerContext>>>,
    /// 每个 consumer 最多允许一个超时后仍在收尾的 revoke commit。
    pre_commit_in_flight: AtomicBool,
    /// librdkafka 后台线程最近的固定错误码，供 `GroupHealth` 读侧合并；永久错误另存会话邮箱。
    global_error: Arc<Mutex<Option<String>>>,
    /// 当前原生会话首个永久连接错误；不能被随后出现的断连通知覆盖。
    terminal_fatal: Mutex<Option<String>>,
}

impl OwnerConsumerContext {
    /// 业务作用：建立单个原生会话的协调与永久错误证据，复用跨会话健康错误槽。
    ///
    /// 参数说明：
    ///
    /// - `rebalance_commit_timeout`: revoke 同步提交预算。
    /// - `global_error`: 与 supervisor 共享的 broker 级错误单槽(跨会话重建持续存活)。
    ///
    /// 返回：代次从零开始、协调及提交快照为空的上下文；不会清除共享健康槽，也不创建 consumer 或发起提交。
    fn new(rebalance_commit_timeout: Duration, global_error: Arc<Mutex<Option<String>>>) -> Self {
        Self {
            epoch: AtomicU64::new(0),
            mailbox: Mutex::new(None),
            pending_offsets: Mutex::new(BTreeMap::new()),
            pre_rebalance: Mutex::new(None),
            rebalance_commit_timeout,
            owner_consumer: OnceLock::new(),
            pre_commit_in_flight: AtomicBool::new(false),
            global_error,
            terminal_fatal: Mutex::new(None),
        }
    }

    /// 业务作用：在 consumer 构造完成后回填自身弱引用，且只允许成功一次。
    ///
    /// 参数说明：
    ///
    /// - `consumer`: owner 唯一 consumer 的弱引用。
    ///
    /// 返回：首次写入供 revoke 后台提交升级使用，重复调用保留已有引用；本函数不延长 consumer 生命周期，也不报告重复写入错误。
    fn set_owner_consumer(&self, consumer: Weak<BaseConsumer<OwnerConsumerContext>>) {
        let _ = self.owner_consumer.set(consumer);
    }

    /// 业务作用：覆盖写入当前 callback 结果。
    ///
    /// 参数说明：
    ///
    /// - `kind`: rebalance 类型。
    /// - `partitions`: callback 提供的分区。
    /// - `error`: 可选错误文本。
    /// - `committed`: 本次撤权窗口内已经确认的安全前沿，未确认时为空。
    ///
    /// 返回：分配新的本会话事件代次并覆盖单槽事件，旧的未取事件被替换；只发布事实，不向外开放 Ready 或执行新的提交。
    fn publish(
        &self,
        kind: RdRebalanceKind,
        partitions: Vec<Tp>,
        error: Option<String>,
        committed: BTreeMap<Tp, i64>,
    ) {
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        *self
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RdRebalanceEvent {
            epoch,
            kind,
            partitions,
            error,
            committed,
        });
    }

    /// 业务作用：取走当前合并事件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有事件时转移其所有权并清空单槽，没有时返回 None；取走本身不更新 owner 的分配或提交状态。
    fn take(&self) -> Option<RdRebalanceEvent> {
        self.mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// 业务作用：判断 mailbox 是否已有待处理事件但不取走它。
    ///
    /// 参数说明：无。
    ///
    /// 返回：锁内观察到事件为 true；不消费事件，false 也不保证后续操作期间不会发生分配变化。
    fn has_event(&self) -> bool {
        self.mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// 业务作用：替换 owner 当前可以安全提交的 offsets 快照。
    ///
    /// 参数说明：
    ///
    /// - `offsets`: 按分区合并后的安全前沿。
    ///
    /// 返回：在写锁内以完整副本替换协调回调可见的前沿；空表清除旧证据，不向 broker 提交，也不自行验证前沿安全性。
    fn set_pending_offsets(&self, offsets: &BTreeMap<Tp, i64>) {
        *self
            .pending_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offsets.clone();
    }
}

impl ClientContext for OwnerConsumerContext {
    /// 业务作用：保留永久连接失败的停止证据，并向健康单槽报告固定原生错误码。
    ///
    /// 参数说明：
    /// - `error`：error 为客户端级错误。
    /// - `_reason`：_reason 为可能含认证服务文本的原生描述。
    ///
    /// 共同约束：不对外传播。
    ///
    /// 返回：只写有界邮箱；owner 下一次 poll 复验永久证据后关闭消费准入，不在回调线程重建或停止资源。
    fn error(&self, error: KafkaError, _reason: &str) {
        if let Some(code) = error.rdkafka_error_code() {
            let detail = format!("Kafka connection error: {code:?}");
            // 认证与证书错误不能通过重连取得新权限；保留首个永久原因直到整代会话退出。
            if is_fatal_code(code) {
                self.terminal_fatal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert(detail.clone());
            }
            if !is_global_health_code(code) {
                return;
            }
            *self
                .global_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(detail);
        }
    }
}

/// 业务作用：判定一个错误码是否值得进入 `GroupHealth.last_error` 的 broker 级单槽。
///
/// 参数说明：
///
/// - `code`: librdkafka 错误码。
///
/// 返回：全 broker 不可用、认证、证书或组/主题/集群授权错误为 true；其它错误不写该健康单槽，永久停止分类另行判断。
fn is_global_health_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::UnsupportedSASLMechanism
            | RDKafkaErrorCode::SSL
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
    )
}

impl ConsumerContext for OwnerConsumerContext {
    /// 业务作用：在撤权回调窗口内对仍有效的安全前沿发起提交，并有界等待确认。
    ///
    /// 参数说明：
    /// - `consumer`: 当前原生会话，用于复验失权状态及提交来源。
    /// - `rebalance`: 本次协调事件，只有 Revoke 进入提交路径。
    ///
    /// 返回：保存已确认前沿或失败原因供 post 回调合并；已失权或无前沿不提交，超时不确认成功，后台实际工作可继续收尾。
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(partitions) = rebalance else {
            return;
        };
        let revoked = elements_to_tps(partitions);
        let snapshot = self
            .pending_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let offsets = select_revoke_offsets(&revoked, &snapshot, consumer.assignment_lost());
        let result = if offsets.is_empty() {
            PreRebalanceResult {
                committed: BTreeMap::new(),
                error: None,
            }
        } else {
            match commit_offsets_bounded(consumer, &offsets, self.rebalance_commit_timeout) {
                BoundedCommitOutcome::Committed => PreRebalanceResult {
                    committed: offsets,
                    error: None,
                },
                BoundedCommitOutcome::Failed(error) => PreRebalanceResult {
                    committed: BTreeMap::new(),
                    error: Some(error),
                },
                BoundedCommitOutcome::TimedOut => PreRebalanceResult {
                    committed: BTreeMap::new(),
                    error: Some(format!(
                        "revoke commit 超过硬截止时间: budget_ms={}",
                        self.rebalance_commit_timeout.as_millis()
                    )),
                },
            }
        };
        *self
            .pre_rebalance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
    }

    /// 业务作用：默认 assign/unassign 完成后发布事实，避免用启动前的空 assignment 冒充已 join。
    ///
    /// 参数说明：
    /// - `consumer`: 已应用底层分配动作的会话，用于查询完整 assignment。
    /// - `rebalance`: 本次分配、撤权或协调错误事件。
    ///
    /// 返回：覆盖发布完整分配、撤权范围及前置提交结果，查询失败转为错误事件；不在回调内发布 Ready，初始化位置仍由 owner 完成。
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(_) => {
                // cooperative callback 只列新增分区，owner 的容量与权限必须基于完整实际 assignment。
                match consumer.assignment() {
                    Ok(partitions) => self.publish(
                        RdRebalanceKind::Assigned,
                        elements_to_tps(&partitions),
                        None,
                        BTreeMap::new(),
                    ),
                    Err(error) => self.publish(
                        RdRebalanceKind::Error,
                        Vec::new(),
                        Some(error.to_string()),
                        BTreeMap::new(),
                    ),
                }
            }
            Rebalance::Revoke(partitions) => {
                let result = self
                    .pre_rebalance
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .unwrap_or(PreRebalanceResult {
                        committed: BTreeMap::new(),
                        error: None,
                    });
                self.publish(
                    RdRebalanceKind::Revoked,
                    elements_to_tps(partitions),
                    result.error,
                    result.committed,
                );
            }
            Rebalance::Error(error) => self.publish(
                RdRebalanceKind::Error,
                Vec::new(),
                Some(error.to_string()),
                BTreeMap::new(),
            ),
        }
    }
}

/// 底层 header 的只读借用视图。
#[derive(Clone, Copy)]
pub(crate) struct RdHeaderRef<'a> {
    /// header 名称。
    pub(crate) name: &'a str,
    /// 可空 header 值。
    pub(crate) value: Option<&'a [u8]>,
}

/// poll 返回消息的底层隔离借用视图。
pub(crate) struct RdMessage<'a> {
    /// 只在本模块出现的底层借用消息。
    inner: rdkafka::message::BorrowedMessage<'a>,
}

impl RdMessage<'_> {
    /// 业务作用：提供真实来源 topic，供框架路由及失败定位使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：借用底层消息名称，不复制数据或采用外部 header 声明覆盖来源。
    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    /// 业务作用：提供消息的真实分区身份，支撑分区顺序与位点责任归属。
    ///
    /// 参数说明：无。
    ///
    /// 返回：底层消息所属分区编号，不表示当前 owner 仍拥有该分区。
    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    /// 业务作用：暴露消息自身的来源位置，用于确认、重试及 DLT 关联。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前记录 offset，不是下一条拉取位置，也不表示 broker 已提交此记录。
    pub(crate) fn offset(&self) -> i64 {
        self.inner.offset()
    }

    /// 业务作用：返回消息时间戳毫秒，缺失时为 -1。
    ///
    /// 参数说明：无。
    ///
    /// 返回：创建时间或日志追加时间的 Unix 毫秒原值，未提供时为 -1；该视图不区分两种时间来源，也不补入本机当前时间。
    pub(crate) fn timestamp(&self) -> i64 {
        match self.inner.timestamp() {
            Timestamp::NotAvailable => -1,
            Timestamp::CreateTime(value) | Timestamp::LogAppendTime(value) => value,
        }
    }

    /// 业务作用：返回原始 key 借用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：原始可空字节切片，保留无 key 与零长度 key 的区别；不要求 UTF-8，不复制或重编码分区身份。
    pub(crate) fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    /// 业务作用：返回原始 payload 借用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：消息存活期间可用的原文字节切片；None 保留 Kafka null 语义，Some 空切片仍是存在的正文，不进行解码。
    pub(crate) fn payload(&self) -> Option<&[u8]> {
        self.inner.payload()
    }

    /// 业务作用：物化轻量 header 描述符，header 名和值本身仍借用底层消息。
    ///
    /// rust-rdkafka 0.39 的借用 header 迭代器会对非 UTF-8 名称执行 `unwrap`。Kafka wire
    /// 并不保证外部生产者写入的名称是 UTF-8，因此必须在隔离层截获该 panic，并把它交给
    /// `invalid_record_policy`，不能让一条毒消息击穿 owner 会话。
    ///
    /// 参数说明：无。
    ///
    /// 返回：保持原顺序、重复项和可空值的借用描述符列表，无头部时为空；迭代 panic 转为 MalformedRoute，不复制头部字节，应用 panic hook 仍按原设置执行。
    pub(crate) fn headers(&self) -> Result<Vec<RdHeaderRef<'_>>> {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.inner
                .headers()
                .map(|headers| {
                    headers
                        .iter()
                        .map(|header| RdHeaderRef {
                            name: header.key,
                            value: header.value,
                        })
                        .collect()
                })
                .unwrap_or_default()
        }))
        .map_err(|_| NafkaError::MalformedRoute("header 名不是 UTF-8".into()))
    }

    /// 业务作用：返回复制为拥有型记录前的主要保留字节估算。
    ///
    /// 参数说明：无。
    ///
    /// 返回：key、正文与可解析头部字节之和；头部解析失败时不计头部，不含 topic、分配器或结构体开销，不能作为完整内存占用值。
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes_with(self.headers().as_ref().ok())
    }

    /// 业务作用：用已解析好的 headers 计量保留字节，避免对同一条消息重复解析。
    ///
    /// 参数说明：
    ///
    /// - `headers`: 已解析的 header 描述符；`None` 表示 header 名非 UTF-8 无法计量。
    ///
    /// 返回：正文、key 与传入头部名称和值的字节总数，None 头部按零计；不再解析头部，不计对象开销，额外容量余量由调用方负责。
    pub(crate) fn retained_bytes_with(&self, headers: Option<&Vec<RdHeaderRef<'_>>>) -> usize {
        self.payload().map_or(0, <[u8]>::len)
            + self.key().map_or(0, <[u8]>::len)
            + headers.map_or(0, |headers| {
                headers
                    .iter()
                    .map(|header| header.name.len() + header.value.map_or(0, <[u8]>::len))
                    .sum::<usize>()
            })
    }
}

/// 只允许 owner 线程持有的底层 consumer 包装。
pub(crate) struct ConsumerClient {
    /// 唯一底层 BaseConsumer。
    consumer: Arc<BaseConsumer<OwnerConsumerContext>>,
    /// owner 控制与 commit 请求超时。
    timeout: Duration,
    /// 所属运行时的后端无关指标出口。
    metrics: Arc<dyn MetricsSink>,
    /// 最终 group id，仅用作低基数指标标签。
    group: Arc<str>,
    /// 固定分配跨会话保留的绝对恢复坐标；订阅消费保持为空，由组协议决定恢复位置。
    fixed_offsets: Arc<Mutex<BTreeMap<Tp, i64>>>,
}

impl ConsumerClient {
    /// 业务作用：构造一个禁用自动提交与自动存储的 consumer。
    ///
    /// 参数说明：
    ///
    /// - `config`: 冻结顶层配置。
    /// - `group`: 最终 group.id。
    /// - `metrics`: 后端无关指标出口。
    /// - `global_error`: 与 supervisor 共享的 broker 级错误单槽。
    /// - `fixed_offsets`: owner 生命周期内的固定分配恢复坐标，只接纳成功定位或确认提交。
    ///
    /// # 错误
    ///
    /// 原生配置或 consumer 构造失败时返回错误。
    ///
    /// 返回：绑定同一 owner 恢复账本的本地会话；构造成功本身不发布 Ready 或推进坐标。
    pub(crate) fn create(
        config: &KafkaConfig,
        group: &str,
        metrics: Arc<dyn MetricsSink>,
        global_error: Arc<Mutex<Option<String>>>,
        fixed_offsets: Arc<Mutex<BTreeMap<Tp, i64>>>,
    ) -> Result<Self> {
        let consumer = consumer_config(config, group)?
            .create_with_context::<_, BaseConsumer<OwnerConsumerContext>>(
                OwnerConsumerContext::new(
                    Duration::from_millis(config.behavior.rebalance_commit_timeout_ms),
                    global_error,
                ),
            )
            .map_err(|error| {
                NafkaError::Config(format!(
                    "consumer 构造失败: {}",
                    super::config::redact(&error)
                ))
            })?;
        let consumer = Arc::new(consumer);
        consumer
            .context()
            .set_owner_consumer(Arc::downgrade(&consumer));
        Ok(Self {
            consumer,
            timeout: Duration::from_millis(config.behavior.control_timeout_ms),
            metrics,
            group: Arc::from(group),
            fixed_offsets,
        })
    }

    /// 业务作用：发出一个带 group 标签的 consumer counter。
    ///
    /// 参数说明：
    /// - `name`: 框架定义的固定指标名，不从消息内容生成。
    /// - `delta`: 本次事件增量，交给指标出口累计。
    ///
    /// 返回：调用指标出口并隔离其 panic，失败仅记录固定诊断；不改变提交结果，出口未完成也不产生新的消费确认。
    pub(crate) fn counter(&self, name: &'static str, delta: u64) {
        // 用户实现的 panic 必须被隔离：本函数在 owner 线程上调用（commit 路径），
        // 一次 panic 会穿到 run_owner 的 catch_unwind 把 group 打成 Crashed。
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.metrics.counter(name, delta, &[("group", &self.group)]);
        }))
        .is_err()
        {
            tracing::error!(
                metric = name,
                "MetricsSink 实现 panic，已隔离并忽略本次上报"
            );
        }
    }

    /// 业务作用：订阅一组 topic。
    ///
    /// 参数说明：
    ///
    /// - `topics`: 非空 topic 列表。
    ///
    /// 返回：底层接受订阅声明时成功，拒绝时转换为 Broker 错误；成功不代表已完成组协调或取得 assignment。
    pub(crate) fn subscribe(&self, topics: &[String]) -> Result<()> {
        let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        self.consumer.subscribe(&refs).map_err(broker_error)
    }

    /// 业务作用：取消当前 subscribe 订阅。
    ///
    /// 参数说明：无。
    ///
    /// 返回：向原生消费者发出取消订阅，不提供 broker 完成回执；owner 的 Ready、重试与本地责任由上层另行收尾。
    pub(crate) fn unsubscribe(&self) {
        self.consumer.unsubscribe();
    }

    /// 业务作用：首次固定分配解析绝对起点，后续会话沿用确认提交或显式定位留下的恢复坐标。
    ///
    /// 参数说明：
    ///
    /// - `starts`: 每个固定分区的起始策略。
    /// - `auto_offset_reset`: Committed 无历史位点时的回退策略。
    ///
    /// 返回：全部分区定位成功才发布恢复坐标；解析、assign 失败或恢复范围已被保留期删除时拒绝接流量。
    pub(crate) fn assign(
        &self,
        starts: &BTreeMap<Tp, StartOffset>,
        auto_offset_reset: &str,
    ) -> Result<()> {
        let resolution_deadline = Instant::now() + self.timeout;
        let recovery = self
            .fixed_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let recovered_starts;
        let starts = if recovery.is_empty() {
            starts
        } else {
            if recovery.len() != starts.len() || starts.keys().any(|tp| !recovery.contains_key(tp))
            {
                return Err(NafkaError::Lifecycle("固定分配恢复范围不完整".into()));
            }
            // 已取得责任的坐标不能重新解释成 Earliest/Latest；保留期缺口必须保持关闭。
            for (tp, offset) in &recovery {
                let low = self.absolute_watermark(tp, false, resolution_deadline)?;
                let high = self.absolute_watermark(tp, true, resolution_deadline)?;
                if !matches!((low, high), (Offset::Offset(low), Offset::Offset(high)) if *offset >= low && *offset <= high)
                {
                    return Err(NafkaError::Lifecycle(
                        "固定分配恢复坐标超出 broker 保留范围".into(),
                    ));
                }
            }
            recovered_starts = recovery
                .into_iter()
                .map(|(tp, offset)| (tp, StartOffset::Offset(offset)))
                .collect();
            &recovered_starts
        };
        let mut direct = TopicPartitionList::with_capacity(starts.len());
        let mut timestamps = TopicPartitionList::with_capacity(starts.len());
        let mut committed_query = TopicPartitionList::with_capacity(starts.len());
        let mut has_timestamp = false;
        let mut has_committed = false;
        for (tp, start) in starts {
            match start {
                StartOffset::Timestamp(value) => {
                    timestamps
                        .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*value))
                        .map_err(broker_error)?;
                    has_timestamp = true;
                }
                StartOffset::Committed => {
                    committed_query.add_partition(&tp.topic, tp.partition);
                    has_committed = true;
                }
                StartOffset::Earliest => direct
                    .add_partition_offset(
                        &tp.topic,
                        tp.partition,
                        self.absolute_watermark(tp, false, resolution_deadline)?,
                    )
                    .map_err(broker_error)?,
                StartOffset::Latest => direct
                    .add_partition_offset(
                        &tp.topic,
                        tp.partition,
                        self.absolute_watermark(tp, true, resolution_deadline)?,
                    )
                    .map_err(broker_error)?,
                StartOffset::Offset(value) => direct
                    .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*value))
                    .map_err(broker_error)?,
            }
        }
        if has_committed {
            let committed = self
                .consumer
                .committed_offsets(committed_query, Timeout::After(self.timeout))
                .map_err(broker_error)?;
            for element in committed.elements() {
                let target = match element.offset() {
                    Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                    _ => match auto_offset_reset {
                        "earliest" => self.absolute_watermark(
                            &Tp::new(element.topic(), element.partition()),
                            false,
                            resolution_deadline,
                        )?,
                        "latest" => self.absolute_watermark(
                            &Tp::new(element.topic(), element.partition()),
                            true,
                            resolution_deadline,
                        )?,
                        "none" => {
                            return Err(NafkaError::Broker(format!(
                                "分区 {}:{} 没有 committed offset，且 auto.offset.reset=none",
                                element.topic(),
                                element.partition()
                            )));
                        }
                        other => {
                            return Err(NafkaError::Config(format!(
                                "未知 auto_offset_reset: {other}"
                            )));
                        }
                    },
                };
                direct
                    .add_partition_offset(element.topic(), element.partition(), target)
                    .map_err(broker_error)?;
            }
        }
        if has_timestamp {
            let resolved = self
                .consumer
                .offsets_for_times(timestamps, Timeout::After(self.timeout))
                .map_err(broker_error)?;
            for element in resolved.elements() {
                let offset = match element.offset() {
                    Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                    _ => self.absolute_watermark(
                        &Tp::new(element.topic(), element.partition()),
                        true,
                        resolution_deadline,
                    )?,
                };
                direct
                    .add_partition_offset(element.topic(), element.partition(), offset)
                    .map_err(broker_error)?;
            }
        }
        let resolved: BTreeMap<Tp, i64> = direct
            .elements()
            .iter()
            .filter_map(|element| match element.offset() {
                Offset::Offset(offset) if offset >= 0 => {
                    Some((Tp::new(element.topic(), element.partition()), offset))
                }
                _ => None,
            })
            .collect();
        if resolved.len() != starts.len() {
            return Err(NafkaError::Lifecycle("固定分配未取得完整绝对起点".into()));
        }
        self.consumer.assign(&direct).map_err(broker_error)?;
        let results = self
            .consumer
            .seek_partitions(direct, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for element in results.elements() {
            element.error().map_err(broker_error)?;
        }
        // 原生分配与定位全部成功后才发布初始责任，防止下一次恢复跨过未完成初始化。
        *self
            .fixed_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = resolved;
        Ok(())
    }

    /// 业务作用：在 fixed assign 启动总 deadline 内把逻辑 Beginning/End 冻结为绝对 offset。
    ///
    /// 参数说明：
    ///
    /// - `tp`: 目标分区。
    /// - `high`: true 取当前末尾，false 取 retention 后的最早位置。
    /// - `deadline`: 整次多分区起点解析共享的绝对截止点。
    ///
    /// 返回：broker 查询到的最早或末尾绝对 offset；共享期限耗尽或查询失败返回错误，不执行定位，也不保证查询后边界保持不变。
    fn absolute_watermark(&self, tp: &Tp, high: bool, deadline: Instant) -> Result<Offset> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(NafkaError::Broker(format!(
                "fixed assign 起点解析超时: {}:{}",
                tp.topic, tp.partition
            )));
        }
        let (low, high_value) = self
            .consumer
            .fetch_watermarks(&tp.topic, tp.partition, Timeout::After(remaining))
            .map_err(broker_error)?;
        Ok(Offset::Offset(if high {
            high_value
        } else {
            low
        }))
    }

    /// 业务作用：维护原生消费者并取得一条借用输入，在交出消息前复验永久错误证据。
    ///
    /// 参数说明：
    ///
    /// - `timeout`: 单次 poll 最长等待。
    ///
    /// 返回：合法消息为 Some，等待未取到消息或 PartitionEOF 为 None；永久证据优先返回 Fatal，其它错误统一分为 Fatal 或 Transient，成功不等于取得 Ready。
    pub(crate) fn poll(
        &self,
        timeout: Duration,
    ) -> std::result::Result<Option<RdMessage<'_>>, RdPollError> {
        // 全局错误可能只经 callback 上报；在取记录前后都复验，避免在已知失去认证后交出业务输入。
        if let Some(error) = self
            .consumer
            .context()
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Err(RdPollError::Fatal(error));
        }
        let result = self.consumer.poll(timeout);
        if let Some(error) = self
            .consumer
            .context()
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Err(RdPollError::Fatal(error));
        }
        match result {
            Some(Ok(message)) => Ok(Some(RdMessage {
                inner: message,
            })),
            Some(Err(KafkaError::PartitionEOF(_))) => Ok(None),
            Some(Err(error)) => Err(classify_poll_error(error)),
            None => Ok(None),
        }
    }

    /// 业务作用：同步批量提交每个分区唯一安全的 next-offset。
    ///
    /// 参数说明：
    ///
    /// - `values`: 非空分区安全前沿表。
    ///
    /// 返回：broker 确认后更新固定分配恢复点；列表构造或同步 commit 失败时保留旧恢复边界。
    pub(crate) fn commit_offsets(&self, values: &BTreeMap<Tp, i64>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let mut offsets = TopicPartitionList::with_capacity(values.len());
        for (tp, next_offset) in values {
            offsets
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*next_offset))
                .map_err(broker_error)?;
        }
        self.consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(broker_error)?;
        // broker 确认先于本地恢复点推进；超时或未知交付保留旧坐标，允许安全重投。
        for (tp, offset) in values {
            self.remember_fixed_offset(tp, *offset);
        }
        Ok(())
    }

    /// 业务作用：推进已存在的固定分区恢复坐标，不为订阅分区建立本地权威。
    ///
    /// 参数说明：
    /// - `tp`：tp 为已分配分区。
    /// - `offset`：offset 为确认提交或成功显式定位的绝对坐标。
    ///
    /// 返回：后续会话从该坐标继续；非固定分区不产生状态变化。
    fn remember_fixed_offset(&self, tp: &Tp, offset: i64) {
        let mut positions = self
            .fixed_offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(position) = positions.get_mut(tp) {
            *position = offset;
        }
    }

    /// 业务作用：执行显式定位并逐分区保存已生效的恢复坐标。
    ///
    /// 参数说明：
    /// - `offsets`：offsets 为通过控制门禁的目标表。
    ///
    /// 返回：全部定位成功时完成；部分失败保留已生效前缀，内部重读不调用此入口。
    pub(crate) fn seek_control_offsets(&self, offsets: &BTreeMap<Tp, i64>) -> Result<()> {
        for (tp, offset) in offsets {
            let resolved = self.seek_one(tp, Offset::Offset(*offset))?;
            self.remember_fixed_offset(tp, resolved);
        }
        Ok(())
    }

    /// 业务作用：主循环取走最近一次实际 rebalance callback 结果。
    ///
    /// 参数说明：无。
    ///
    /// 返回：转移最新单槽事件并清空邮箱，无事件时为 None；具体失权、位置初始化及 Ready 发布仍由 owner 按事件裁决。
    pub(crate) fn take_rebalance(&self) -> Option<RdRebalanceEvent> {
        self.consumer.context().take()
    }

    /// 业务作用：判断 owner 是否应先退出 commit 重试并处理 rebalance。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地已有待处理协调事件为 true；不消费事件，false 不授予持续的分区控制权。
    pub(crate) fn has_rebalance(&self) -> bool {
        self.consumer.context().has_event()
    }

    /// 业务作用：更新供 pre-rebalance callback 使用的安全提交快照。
    ///
    /// 参数说明：
    ///
    /// - `offsets`: owner 当前安全前沿。
    ///
    /// 返回：整体复制替换回调可见前沿，空表撤销旧证据；不会执行提交或推进固定分配的恢复账本。
    pub(crate) fn set_rebalance_offsets(&self, offsets: &BTreeMap<Tp, i64>) {
        self.consumer.context().set_pending_offsets(offsets);
    }

    /// 业务作用：主动清空当前 assignment。
    ///
    /// 参数说明：无。
    ///
    /// 返回：底层接受解除分配时成功，否则转换为 Broker 错误；不清理 owner 的处理证据、Ready 或在途调用。
    pub(crate) fn unassign(&self) -> Result<()> {
        self.consumer.unassign().map_err(broker_error)
    }

    /// 业务作用：返回当前 assignment 快照。
    ///
    /// 参数说明：无。
    ///
    /// 返回：按 topic 与分区排序的当前原生分配列表，查询失败为 Broker 错误；空表可能是合法待机状态，快照不替代后续权威复验。
    pub(crate) fn assignment(&self) -> Result<Vec<Tp>> {
        let list = self.consumer.assignment().map_err(broker_error)?;
        Ok(elements_to_tps(&list))
    }

    /// 业务作用：查询指定分区 position。
    ///
    /// 参数说明：
    ///
    /// - `tps`: 目标分区；空列表使用底层 position 查询列出的全部分区。
    ///
    /// 返回：每个目标的非负消费位置，未出现在快照或尚无具体位置时为 None；底层查询失败返回 Broker 错误，不查询已提交位点。
    pub(crate) fn position(&self, tps: &[Tp]) -> Result<BTreeMap<Tp, Option<i64>>> {
        let positions = self.consumer.position().map_err(broker_error)?;
        let requested = if tps.is_empty() {
            elements_to_tps(&positions)
        } else {
            tps.to_vec()
        };
        Ok(requested
            .into_iter()
            .map(|tp| {
                let offset =
                    positions
                        .find_partition(&tp.topic, tp.partition)
                        .and_then(|element| match element.offset() {
                            Offset::Offset(value) if value >= 0 => Some(value),
                            _ => None,
                        });
                (tp, offset)
            })
            .collect())
    }

    /// 业务作用：在 assignment 后显式冻结每个分区的初始消费位置。
    ///
    /// committed offset 优先；没有提交时按配置使用 logical Beginning/End，并通过一次批量
    /// `seek_partitions` 冻结全部位置。该步骤完成前不能发布 broker ready，否则 latest group 可能
    /// 越过 ready 后立即生产的首条消息。
    ///
    /// 参数说明：
    ///
    /// - `tps`: 本次完整 assignment。
    /// - `auto_offset_reset`: 无提交记录时采用的起点策略，可为 earliest、latest 或 none。
    ///
    /// 返回：空分配直接成功；其余先确定全部目标再批量 seek 并检查各分区结果。查询、策略或定位失败返回错误，可能已有部分定位生效，成功仍须等待后续 poll 才发布 Ready。
    pub(crate) fn prime_assignment_positions(
        &self,
        tps: &[Tp],
        auto_offset_reset: &str,
    ) -> Result<()> {
        if tps.is_empty() {
            return Ok(());
        }
        let mut query = TopicPartitionList::with_capacity(tps.len());
        for tp in tps {
            query.add_partition(&tp.topic, tp.partition);
        }
        let committed = self
            .consumer
            .committed_offsets(query, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        let mut targets = TopicPartitionList::with_capacity(tps.len());
        for tp in tps {
            let stored = committed
                .find_partition(&tp.topic, tp.partition)
                .and_then(|element| match element.offset() {
                    Offset::Offset(value) if value >= 0 => Some(value),
                    _ => None,
                });
            let target = match (stored, auto_offset_reset) {
                (Some(value), _) => Offset::Offset(value),
                (None, "earliest") => Offset::Beginning,
                (None, "latest") => Offset::End,
                (None, "none") => {
                    return Err(NafkaError::Broker(format!(
                        "分区 {}:{} 没有 committed offset，且 auto.offset.reset=none",
                        tp.topic, tp.partition
                    )));
                }
                (None, other) => {
                    return Err(NafkaError::Config(format!(
                        "未知 auto_offset_reset: {other}"
                    )));
                }
            };
            targets
                .add_partition_offset(&tp.topic, tp.partition, target)
                .map_err(broker_error)?;
        }
        let results = self
            .consumer
            .seek_partitions(targets, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for element in results.elements() {
            element.error().map_err(broker_error)?;
        }
        Ok(())
    }

    /// 业务作用：暂停指定分区；空列表表示当前 assignment。
    ///
    /// 参数说明：
    ///
    /// - `tps`: 目标分区。
    ///
    /// 返回：底层接受暂停后返回实际请求的目标；分配查询或暂停失败返回 Broker 错误，不修改上层暂停原因，也不撤销已执行的业务调用。
    pub(crate) fn pause(&self, tps: &[Tp]) -> Result<Vec<Tp>> {
        let targets = self.targets_or_assignment(tps)?;
        self.consumer
            .pause(&tps_to_list(&targets))
            .map_err(broker_error)?;
        Ok(targets)
    }

    /// 业务作用：只恢复已经通过暂停原因门禁的显式分区，空列表没有副作用。
    ///
    /// 参数说明：
    /// - `tps`：tps 为可恢复范围。
    ///
    /// 返回：实际请求恢复的分区；native 拒绝时返回错误，不扩大到其它保护分区。
    pub(crate) fn resume(&self, tps: &[Tp]) -> Result<Vec<Tp>> {
        if tps.is_empty() {
            return Ok(Vec::new());
        }
        let targets = tps.to_vec();
        self.consumer
            .resume(&tps_to_list(&targets))
            .map_err(broker_error)?;
        Ok(targets)
    }

    /// 业务作用：seek 一组显式 offsets。
    ///
    /// 参数说明：
    ///
    /// - `offsets`: 每分区目标 offset。
    ///
    /// 返回：全部目标定位成功时完成，首个失败立即返回且保留已生效前缀；空表无副作用，不改变固定分配恢复账本，适用于内部重读。
    pub(crate) fn seek_offsets(&self, offsets: &BTreeMap<Tp, i64>) -> Result<()> {
        for (tp, offset) in offsets {
            self.seek_one(tp, Offset::Offset(*offset))?;
        }
        Ok(())
    }

    /// 业务作用：尽力回退每个分区的位置：单个分区失败不影响其余分区。
    ///
    /// 批级回退表里**故意**包含预期会失败的分区（刚被 revoke 的、已不在 assignment 里的），
    /// 而 `BTreeMap<Tp, _>` 按 `(topic, partition)` 排序遍历。若沿用 fail-fast 语义，
    /// 一个排序靠前的失败分区会取消掉后面所有分区的回退——那些仍被本 owner 持有、
    /// 已 poll 未派发的记录就被跳过并被后续安全前沿提交越过（静默丢失）。
    ///
    /// 参数说明：
    ///
    /// - `offsets`: 每分区目标 offset。
    ///
    /// 返回：按尝试顺序收集的失败分区列表，所有目标都会尝试；调用方需复验失败目标的当前归属并保持保护，不能只告警后继续越过未处理记录。
    pub(crate) fn rewind_offsets(&self, offsets: &BTreeMap<Tp, i64>) -> Vec<Tp> {
        let mut failed = Vec::new();
        for (tp, offset) in offsets {
            if let Err(error) = self.seek_one(tp, Offset::Offset(*offset)) {
                tracing::debug!(
                    topic = %tp.topic,
                    partition = tp.partition,
                    offset,
                    error = %error,
                    "批级回退未生效"
                );
                failed.push(tp.clone());
            }
        }
        failed
    }

    /// 业务作用：seek 一组分区到指定特殊 offset。
    ///
    /// 只接受 Beginning/End 与绝对 offset。`StartOffset::Committed` 必须走
    /// [`Self::seek_to_committed`]：底层的 `Offset::Stored` 读的是客户端本地 offset store，
    /// 而框架强制 `enable.auto.offset.store=false`，该 store 恒为空。
    ///
    /// 参数说明：
    ///
    /// - `tps`: 目标分区。
    /// - `offset`: Beginning/End 或绝对 offset。
    ///
    /// # 错误
    ///
    /// 传入 Committed 或任一 seek 失败时返回错误。
    ///
    /// 返回：成功目标更新固定分配恢复坐标；部分失败保留已经生效的目标，不声称批量原子。
    pub(crate) fn seek_special(&self, tps: &[Tp], offset: StartOffset) -> Result<()> {
        if matches!(offset, StartOffset::Committed) {
            return Err(NafkaError::Config(
                "Committed 起点必须经 seek_to_committed 解析为绝对 offset".into(),
            ));
        }
        let native = start_offset(offset);
        for tp in tps {
            let resolved = self.seek_one(tp, native)?;
            self.remember_fixed_offset(tp, resolved);
        }
        Ok(())
    }

    /// 业务作用：把一组分区定位回 broker 已提交位点。
    ///
    /// 为什么不能直接 seek 到底层的 `Offset::Stored`：框架把
    /// `enable.auto.offset.store=false` 定为不可覆盖的不变量，
    /// 框架自身也从不写入本地 offset store，因此该 store 恒为空，seek 会直接返回
    /// `Invalid argument`——即该路径在任何配置下都不可能成功。这里改为与 fixed assign
    /// 起点解析（[`Self::assign`]）同一套做法：先读 broker 的 committed offset，
    /// 再 seek 到解析出的绝对位置。
    ///
    /// 参数说明：
    ///
    /// - `tps`: 目标分区；空列表直接成功。
    /// - `auto_offset_reset`: 分区没有提交历史时的回退策略（`earliest`/`latest`/`none`）。
    ///
    /// # 错误
    ///
    /// committed 查询失败、`none` 模式遇到无提交，或任一 seek 失败时返回错误。
    ///
    /// 返回：成功定位的固定分区保存绝对恢复点，无提交历史的拒绝发生在任何定位前。
    pub(crate) fn seek_to_committed(&self, tps: &[Tp], auto_offset_reset: &str) -> Result<()> {
        if tps.is_empty() {
            return Ok(());
        }
        let mut query = TopicPartitionList::with_capacity(tps.len());
        for tp in tps {
            query.add_partition(&tp.topic, tp.partition);
        }
        let committed = self
            .consumer
            .committed_offsets(query, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        // 显式定位的缺席证据必须在任何分区移动前裁决，不能先移动前缀再声称整批未执行。
        if auto_offset_reset == "none"
            && tps.iter().any(|tp| {
                committed
                    .find_partition(&tp.topic, tp.partition)
                    .is_none_or(
                        |element| !matches!(element.offset(), Offset::Offset(value) if value >= 0),
                    )
            })
        {
            return Err(NafkaError::ControlNotApplied(0));
        }
        for tp in tps {
            let stored = committed
                .find_partition(&tp.topic, tp.partition)
                .and_then(|element| match element.offset() {
                    Offset::Offset(value) if value >= 0 => Some(value),
                    _ => None,
                });
            let target = match (stored, auto_offset_reset) {
                (Some(value), _) => Offset::Offset(value),
                (None, "earliest") => Offset::Beginning,
                (None, "latest") => Offset::End,
                (None, "none") => {
                    return Err(NafkaError::Broker(format!(
                        "分区 {}:{} 没有 committed offset，且 auto.offset.reset=none",
                        tp.topic, tp.partition
                    )));
                }
                (None, other) => {
                    return Err(NafkaError::Config(format!(
                        "未知 auto_offset_reset: {other}"
                    )));
                }
            };
            let resolved = self.seek_one(tp, target)?;
            self.remember_fixed_offset(tp, resolved);
        }
        Ok(())
    }

    /// 业务作用：按时间戳解析并 seek 一组分区。
    ///
    /// 参数说明：
    ///
    /// - `times`: 每分区目标时间戳。
    ///
    /// # 错误
    ///
    /// broker 查询或任一 seek 失败时返回错误。
    ///
    /// 返回：成功目标保存解析后的固定恢复坐标；以后重建不重新解释时间戳。
    pub(crate) fn seek_times(&self, times: &BTreeMap<Tp, i64>) -> Result<()> {
        let mut query = TopicPartitionList::with_capacity(times.len());
        for (tp, timestamp) in times {
            query
                .add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*timestamp))
                .map_err(broker_error)?;
        }
        let resolved = self
            .consumer
            .offsets_for_times(query, Timeout::After(self.timeout))
            .map_err(broker_error)?;
        for element in resolved.elements() {
            let tp = Tp::new(element.topic(), element.partition());
            let offset = match element.offset() {
                Offset::Offset(value) if value >= 0 => Offset::Offset(value),
                _ => Offset::End,
            };
            let resolved = self.seek_one(&tp, offset)?;
            self.remember_fixed_offset(&tp, resolved);
        }
        Ok(())
    }

    /// 业务作用：返回底层是否判定当前 assignment 已丢失。
    ///
    /// 参数说明：无。
    ///
    /// 返回：底层失权标记；true 要求停止使用旧代次提交或派发，false 仍需结合待处理协调事件及当前分配复验权威。
    pub(crate) fn assignment_lost(&self) -> bool {
        self.consumer.assignment_lost()
    }

    /// 业务作用：将空目标列表替换为当前 assignment。
    ///
    /// 参数说明：
    ///
    /// - `tps`: 调用方目标列表。
    ///
    /// 返回：非空输入直接复制，空输入查询当前分配并传播查询错误；不校验显式目标归属，调用方负责控制权限门禁。
    fn targets_or_assignment(&self, tps: &[Tp]) -> Result<Vec<Tp>> {
        if tps.is_empty() {
            self.assignment()
        } else {
            Ok(tps.to_vec())
        }
    }

    /// 业务作用：冻结特殊边界为绝对坐标后定位，防止暂停期间的 latest 在稍后 fetch 时重新求值。
    ///
    /// 参数说明：
    /// - `tp`：tp 为目标。
    /// - `offset`：offset 为绝对坐标或 Beginning/End。
    ///
    /// 返回：native 接受的绝对坐标；边界查询或定位失败返回错误。
    fn seek_one(&self, tp: &Tp, offset: Offset) -> Result<i64> {
        // 控制成功回执必须对应此刻查询的边界，不能让随后生产的输入被迟到求值的 End 越过。
        let offset = match offset {
            Offset::Beginning => {
                self.absolute_watermark(tp, false, Instant::now() + self.timeout)?
            }
            Offset::End => self.absolute_watermark(tp, true, Instant::now() + self.timeout)?,
            other => other,
        };
        let Offset::Offset(resolved) = offset else {
            return Err(NafkaError::Lifecycle("定位未解析为绝对坐标".into()));
        };
        self.consumer
            .seek(
                &tp.topic,
                tp.partition,
                offset,
                Timeout::After(self.timeout),
            )
            .map_err(broker_error)?;
        Ok(resolved)
    }
}

/// 业务作用：把公开起始策略映射为底层 offset 表达。
///
/// `Committed` 不在此映射：它需要一次 broker 查询才能变成绝对 offset，
/// 由 [`ConsumerClient::seek_to_committed`] 负责；这里返回 Beginning 只是为了让
/// match 完整，调用方在进入本函数前已经拒绝该分支。
///
/// 参数说明：
///
/// - `offset`: 非时间戳、非 Committed 的起始策略。
///
/// 返回：最早与末尾映射到逻辑边界，绝对位置原值传递；不会查询 broker，Timestamp 分支仅按数值转换，调用方必须把时间查询交给专用入口。
fn start_offset(offset: StartOffset) -> Offset {
    match offset {
        StartOffset::Earliest | StartOffset::Committed => Offset::Beginning,
        StartOffset::Latest => Offset::End,
        StartOffset::Timestamp(value) | StartOffset::Offset(value) => Offset::Offset(value),
    }
}

/// 业务作用：从 owner 安全快照中只选取本次实际被收回的分区。
///
/// 参数说明：
///
/// - `revoked`: callback 提供的实际 revoke 分区。
/// - `snapshot`: owner 最近发布的安全 next-offset。
/// - `assignment_lost`: 底层是否已判定 assignment 丢失。
///
/// 返回：仍有权威时返回撤权范围与安全快照的交集；已经失权则返回空表，禁止据旧证据发起新提交。
fn select_revoke_offsets(
    revoked: &[Tp],
    snapshot: &BTreeMap<Tp, i64>,
    assignment_lost: bool,
) -> BTreeMap<Tp, i64> {
    // 分配已经丢失时旧前沿不再具有提交权威，不能借撤权回调重新取得控制权。
    if assignment_lost {
        return BTreeMap::new();
    }
    revoked
        .iter()
        .filter_map(|tp| snapshot.get(tp).map(|offset| (tp.clone(), *offset)))
        .collect()
}

/// 业务作用：将同步提交交给独立线程，使协调回调只进行有界的结果等待。
///
/// 参数说明：
///
/// - `timeout`: 线程创建后接收结果的最长等待，不包含创建与调度开销。
/// - `work`: 后台执行的提交工作。
///
/// 返回：确认成功、任务错误或等待超时的分类；创建失败及任务 panic 也转为失败，超时不取消后台工作，不等于 broker 未提交。
fn wait_bounded_commit<F>(timeout: Duration, work: F) -> BoundedCommitOutcome
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let spawned = std::thread::Builder::new()
        .name("nafka-revoke-commit".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
                .map_err(|_| "revoke commit 任务 panic".to_owned())
                .and_then(|result| result.map_err(|error| error.to_string()));
            let _ = sender.send(result);
        });
    if let Err(error) = spawned {
        return BoundedCommitOutcome::Failed(format!("无法启动 revoke commit 任务: {error}"));
    }
    match receiver.recv_timeout(timeout) {
        Ok(Ok(())) => BoundedCommitOutcome::Committed,
        Ok(Err(error)) => BoundedCommitOutcome::Failed(error),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => BoundedCommitOutcome::TimedOut,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            BoundedCommitOutcome::Failed("revoke commit 任务异常退出".into())
        }
    }
}

/// 业务作用：以每会话单飞后台任务提交 revoke 安全前沿，并限制回调等待时间。
///
/// 高层同步提交在协调器不可用时可能等待完整 session timeout，无法满足 callback 的硬边界；这里让
/// 后台任务持有 owner consumer 生命周期，而 callback 只等待调用方预算。超时后的晚到成功不再被本地
/// 当成已确认；即使 broker 最终接受，它也只包含安全前沿，因此最坏结果仍是重放而不是丢消息。
///
/// 参数说明：
///
/// - `consumer`: 当前 generation 的底层 consumer。
/// - `offsets`: 被 revoke 分区的安全 next-offset。
/// - `timeout`: 后台线程创建后等待结果的时长预算。
///
/// 返回：已有提交或生命周期引用缺失则拒绝；其余返回确认、失败或超时。超时保持单飞门禁，实际工作退出后释放，迟到结果不再补记为已确认。
fn commit_offsets_bounded(
    consumer: &BaseConsumer<OwnerConsumerContext>,
    offsets: &BTreeMap<Tp, i64>,
    timeout: Duration,
) -> BoundedCommitOutcome {
    let context = consumer.context();
    // 超时只结束回调等待，旧提交仍可能运行；先取得单飞权，防止后台工作无界叠加。
    if context
        .pre_commit_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return BoundedCommitOutcome::Failed("已有 revoke commit 在后台收尾".into());
    }
    // 后台线程必须持有真实会话生命周期；弱引用失效时释放刚取得的门禁并拒绝提交。
    let Some(owner) = context
        .owner_consumer
        .get()
        .and_then(std::sync::Weak::upgrade)
    else {
        context.pre_commit_in_flight.store(false, Ordering::Release);
        return BoundedCommitOutcome::Failed("owner consumer 生命周期引用尚未就绪".into());
    };
    let owned_offsets = offsets.clone();
    let outcome = wait_bounded_commit(timeout, move || {
        let _guard = PreCommitGuard {
            owner: Arc::clone(&owner),
        };
        offsets_to_list(&owned_offsets)
            .and_then(|list| owner.commit(&list, CommitMode::Sync).map_err(broker_error))
    });
    // 超时后的门禁只能由实际工作守卫释放，不能因调用方离开而允许另一提交重叠。
    if outcome != BoundedCommitOutcome::TimedOut {
        context.pre_commit_in_flight.store(false, Ordering::Release);
    }
    outcome
}

/// 业务作用：把自有分区列表转换为底层列表。
///
/// 参数说明：
///
/// - `tps`: 自有分区列表。
///
/// 返回：按输入顺序添加 topic 与分区的原生请求列表；不设置具体 offset，也不查询这些分区是否存在或属于当前 owner。
fn tps_to_list(tps: &[Tp]) -> TopicPartitionList {
    let mut list = TopicPartitionList::with_capacity(tps.len());
    for tp in tps {
        list.add_partition(&tp.topic, tp.partition);
    }
    list
}

/// 业务作用：把安全 offset 表转换为底层提交列表。
///
/// 参数说明：
///
/// - `offsets`: 每分区 next-offset。
///
/// 返回：全部条目构造成功时得到原生提交列表，失败时返回 Broker 错误并丢弃局部列表；不发送提交，安全前沿必须由调用方保证。
fn offsets_to_list(offsets: &BTreeMap<Tp, i64>) -> Result<TopicPartitionList> {
    let mut list = TopicPartitionList::with_capacity(offsets.len());
    for (tp, offset) in offsets {
        list.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*offset))
            .map_err(broker_error)?;
    }
    Ok(list)
}

/// 业务作用：把底层列表转换为稳定排序自有分区列表。
///
/// 参数说明：
///
/// - `list`: 底层分区列表。
///
/// 返回：复制 topic 与分区身份并按其自然顺序排序的列表；不保留原生 offset 或逐项错误，不去重或证明当前归属。
fn elements_to_tps(list: &TopicPartitionList) -> Vec<Tp> {
    let mut tps: Vec<Tp> = list
        .elements()
        .into_iter()
        .map(|element| Tp::new(element.topic(), element.partition()))
        .collect();
    tps.sort();
    tps
}

/// 业务作用：把底层 consumer 错误收敛为公共 broker 错误。
///
/// 参数说明：
///
/// - `error`: 底层错误。
///
/// 返回：携带原 Display 文本的 Broker 错误，不额外脱敏或判定可重试性；操作失败不据此推断远端没有生效。
fn broker_error(error: impl std::fmt::Display) -> NafkaError {
    NafkaError::Broker(error.to_string())
}

/// 业务作用：集中把底层 poll 错误分成 fatal 或 transient，禁止调用点自行猜测。
///
/// 参数说明：
///
/// - `error`: 底层 poll 返回的具体错误。
///
/// 返回：原生致命消费、配置、NUL 或固定永久错误码归为 Fatal，其余为 Transient；保留错误文本，实际是否重建仍受 owner 预算和状态控制。
fn classify_poll_error(error: KafkaError) -> RdPollError {
    let fatal = matches!(
        error,
        KafkaError::MessageConsumptionFatal(_) | KafkaError::ClientConfig(..) | KafkaError::Nul(_)
    ) || error.rdkafka_error_code().is_some_and(is_fatal_code);
    if fatal {
        RdPollError::Fatal(error.to_string())
    } else {
        RdPollError::Transient(error.to_string())
    }
}

/// 业务作用：识别禁止当前会话自动重建的错误码，避免通过重连绕过认证、fencing 或能力限制。
///
/// 参数说明：
///
/// - `code`: 底层稳定错误码。
///
/// 返回：命中固定永久错误集合为 true，其余为 false；只提供分类，不直接停止资源，后续显式恢复由上层控制。
fn is_fatal_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::InvalidArgument
            | RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::SSL
            | RDKafkaErrorCode::Fatal
            | RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::UnsupportedFeature
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            | RDKafkaErrorCode::UnsupportedSASLMechanism
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
            | RDKafkaErrorCode::FencedInstanceId
            | RDKafkaErrorCode::ProducerFenced
            | RDKafkaErrorCode::FencedMemberEpoch
            | RDKafkaErrorCode::UnsupportedAssignor
    )
}
