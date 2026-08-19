//! 业务 Handler 合同：框架取得当前 attempt 执行权后调用 Handler 处理任务，Handler 返回结果码与摘要。
//!
//! Handler 只在框架 CAS 取得执行权后被调用，同一 attempt 最多一次；返回的结果码决定 Run 进入成功、重试或终态。
//! 上下文携带已解码参数字节与 attempt token；副作用写外部资源前应携带 token 做 fencing，避免失权 attempt 越权写。

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::job::coordinator::{FanoutBuilder, FanoutService};
use crate::job::model::{JobResultCode, JobWireCodec};
use crate::job::payload::decode_json_payload;

/// Fanout shard 的冻结业务上下文；身份跨重发保持稳定，assignment 字段随重分配代次变化。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutContext {
    /// Fanout 批次标识。
    pub fanout_id: String,
    /// 根 Run 标识。
    pub root_run_id: String,
    /// 冻结能力快照标识。
    pub snapshot_id: String,
    /// 分片在批次内的下标。
    pub shard_index: i64,
    /// 批次分片总数。
    pub shard_total: i64,
    /// 跨投递稳定序号。
    pub seq: i64,
    /// 业务外部副作用使用的稳定幂等键。
    pub execution_key: String,
    /// 当前目标稳定节点身份。
    pub target_node_identity: String,
    /// 当前 assignment 代次。
    pub assignment_epoch: i64,
}

/// Handler 执行结果：结果码与业务摘要；摘要过长由提交脚本按上限截断。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutcome {
    /// 结果码，决定 Run 下一状态（成功、重试或终态）。
    pub code: JobResultCode,
    /// 结果摘要，写入 Run 记录供观测。
    pub summary: String,
}

impl JobOutcome {
    /// 业务作用：构造无摘要的成功结果。参数说明: 无。返回：`SUCCESS` 结果。
    pub fn success() -> Self {
        Self {
            code: JobResultCode::Success,
            summary: String::new(),
        }
    }
    /// 业务作用：构造带摘要的成功结果。参数说明：`summary` 结果摘要。返回：`SUCCESS` 结果。
    pub fn success_with_summary(summary: impl Into<String>) -> Self {
        Self {
            code: JobResultCode::Success,
            summary: summary.into(),
        }
    }
    /// 业务作用：构造可重试失败结果。参数说明：`summary` 失败摘要。返回：`RETRY` 结果。
    pub fn retry(summary: impl Into<String>) -> Self {
        Self {
            code: JobResultCode::Retry,
            summary: summary.into(),
        }
    }
    /// 业务作用：构造永久失败结果。参数说明：`summary` 失败摘要。返回：`FAIL_PERMANENT` 结果。
    pub fn fail_permanent(summary: impl Into<String>) -> Self {
        Self {
            code: JobResultCode::FailPermanent,
            summary: summary.into(),
        }
    }
}

/// Handler 执行上下文：当前 Run 的身份、执行权与已解码参数字节。
#[derive(Debug, Clone)]
pub struct JobExecution {
    /// 当前运行时冻结的语言无关 Redis source id。
    pub qualifier: String,
    /// 当前 source 冻结的 Job namespace。
    pub namespace: String,
    /// Run 标识。
    pub run_id: String,
    /// 任务名。
    pub job_name: String,
    /// Worker 能力名。
    pub worker_name: String,
    /// 本 Run 的权威逻辑触发时刻。
    pub logical_fire_at: i64,
    /// Run 首次落库时 Redis 观测到的触发时刻。
    pub triggered_at: i64,
    /// 当前 attempt 序号。
    pub attempt: i64,
    /// 当前 attempt 的 fencing token；写外部资源时应携带以拒绝失权 attempt。
    pub attempt_token: i64,
    /// schema 标识。
    pub schema_id: String,
    /// 线编码名。
    pub wire_codec: String,
    /// 已从 Base64 解码的参数字节。
    pub parameter_payload: Vec<u8>,
    /// Fanout shard 执行时存在的冻结上下文；普通 Run 为 `None`。
    pub fanout: Option<FanoutContext>,
    /// 当前 attempt 的本地权威门禁；业务只能通过公开查询/checkpoint 使用。
    pub(crate) authority: Arc<JobExecutionAuthority>,
    /// 普通根 Run 的 Fanout 创建服务；Fanout shard 与未托管上下文为 `None`。
    pub(crate) fanout_service: Option<Arc<FanoutService>>,
}

/// Handler 使用的公开结果名称；底层状态提交仍由统一 `JobOutcome` 实现承载。
pub type JobResult = JobOutcome;

/// 当前 attempt 的本地保守权威；续期更新截止点，失权、取消或 future 被停止时永久关闭。
#[derive(Debug)]
pub(crate) struct JobExecutionAuthority {
    active: AtomicBool,
    cancellation_requested: AtomicBool,
    fanout_transferred: AtomicBool,
    deadline: Mutex<tokio::time::Instant>,
}

impl JobExecutionAuthority {
    /// 业务作用：根据 start 脚本返回的服务端时刻建立本地保守持权截止点。
    ///
    /// 参数说明：`redis_now`/`lease_until` 为同次脚本响应，`safety_ms` 为 RTT 与漂移安全余量。
    ///
    /// 返回：可随续期推进、失权后不可重新开放的共享门禁。
    pub(crate) fn new(redis_now: i64, lease_until: i64, safety_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            active: AtomicBool::new(true),
            cancellation_requested: AtomicBool::new(false),
            fanout_transferred: AtomicBool::new(false),
            deadline: Mutex::new(local_deadline(redis_now, lease_until, safety_ms)),
        })
    }

    /// 业务作用：续期成功后推进本地保守截止点；只允许当前仍有效的 attempt 更新。
    ///
    /// 参数说明：`redis_now`/`lease_until` 为续期响应，`safety_ms` 为安全余量。
    ///
    /// 返回：仍有效时更新并返回 `true`；已经关闭时返回 `false`，不会复活旧权威。
    pub(crate) fn renew(&self, redis_now: i64, lease_until: i64, safety_ms: u64) -> bool {
        if !self.active.load(Ordering::Acquire) {
            return false;
        }
        match self.deadline.lock() {
            Ok(mut deadline) => {
                *deadline = local_deadline(redis_now, lease_until, safety_ms);
                true
            }
            Err(_) => {
                self.active.store(false, Ordering::Release);
                false
            }
        }
    }

    /// 业务作用：永久关闭当前 attempt 的本地副作用门禁，并可同时记录持久取消请求。
    ///
    /// 参数说明：`cancelled` 表示关闭原因包含业务取消。
    ///
    /// 返回：无返回值；关闭后任何续期都不能重新开放。
    pub(crate) fn revoke(&self, cancelled: bool) {
        if cancelled {
            self.cancellation_requested.store(true, Ordering::Release);
        }
        self.active.store(false, Ordering::Release);
    }

    /// 业务作用：记录根 Run 已把唯一完成权威移交给持久 Fanout 对账流程，并永久关闭普通 attempt 门禁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；移交后 dispatcher 不再续期或调用普通 `finish_run`。
    pub(crate) fn transfer_to_fanout(&self) {
        // 先发布移交事实再关闭普通权威，使并发续期观察到 inactive 时能够区分合法移交与失权。
        self.fanout_transferred.store(true, Ordering::Release);
        self.active.store(false, Ordering::Release);
    }

    /// 业务作用：判断根 Run 的完成权威是否已经持久移交给 Fanout 对账流程。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：prepare_fanout_root 已成功或结局未知时为真；普通失权为假。
    pub(crate) fn is_fanout_transferred(&self) -> bool {
        self.fanout_transferred.load(Ordering::Acquire)
    }

    /// 业务作用：判断当前 attempt 是否仍处于本地保守持权窗口内。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：门禁开启且单调时钟未越过截止点时为真；锁中毒时 fail-closed。
    fn still_owns(&self) -> bool {
        if !self.active.load(Ordering::Acquire) {
            return false;
        }
        self.deadline
            .lock()
            .map(|deadline| tokio::time::Instant::now() < *deadline)
            .unwrap_or(false)
    }
}

impl JobExecution {
    /// 业务作用：返回当前 Handler 所属的冻结 Redis source id，供日志、下游路由和审计使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：canonical qualifier。
    pub fn qualifier(&self) -> &str {
        &self.qualifier
    }

    /// 业务作用：返回当前 source 冻结的 Job namespace，供业务构造可审计的幂等身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不可变 namespace。
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// 业务作用：返回 Handler 收到的原始 payload 字节，不做隐式反序列化或类型选择。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 Run 的 payload 借用。
    pub fn payload(&self) -> &[u8] {
        &self.parameter_payload
    }

    /// 业务作用：按当前 Worker 合同把 JSON 参数解码为业务类型，支持集合、映射和嵌套泛型。
    ///
    /// 参数说明：无显式参数；类型参数 `T` 由调用方的目标变量或 turbofish 指定。
    ///
    /// 返回：JSON 通过重复键、深度和节点预算后返回 `T`；非 JSON 编码或结构不合法时返回 `InvalidPayload`。
    pub fn parameter<T: DeserializeOwned>(&self) -> Result<T> {
        if self.wire_codec != JobWireCodec::Json.wire_name() {
            return Err(crate::job::JobError::InvalidPayload(
                "非 JSON 参数只能通过 payload() 按声明的线编码处理".to_owned(),
            )
            .into());
        }
        decode_json_payload(&self.parameter_payload)
            .map_err(|error| crate::job::JobError::InvalidPayload(error).into())
    }

    /// 业务作用：返回当前执行对应的 Fanout shard 身份，普通 Run 不伪造批次信息。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Fanout Worker 为 `Some`，普通调度或手工 Run 为 `None`。
    pub fn fanout_context(&self) -> Option<&FanoutContext> {
        self.fanout.as_ref()
    }

    /// 业务作用：创建绑定当前根 attempt 的显式 Fanout 构建器，业务只声明 Worker 与分片而不接触 Redis 状态机。
    ///
    /// 参数说明：`worker_name` 为目标 FANOUT_ONLY 或兼容 Worker 能力名。
    ///
    /// 返回：普通且仍持权的根返回一次性 builder；Fanout shard、失权上下文或未装配服务返回错误。
    pub fn fanout(&self, worker_name: impl Into<String>) -> Result<FanoutBuilder> {
        let service = self.fanout_service.as_ref().ok_or_else(|| {
            crate::error::NasaRedisError::from(crate::job::JobError::Config(
                "当前 JobContext 不允许创建 Fanout".to_owned(),
            ))
        })?;
        service.builder(self, worker_name)
    }

    /// 业务作用：判断续期响应是否已捎带持久取消请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已观察到取消请求时为真；该状态一旦为真不再回退。
    pub fn is_cancellation_requested(&self) -> bool {
        self.authority
            .cancellation_requested
            .load(Ordering::Acquire)
    }

    /// 业务作用：保守判断当前 attempt 是否仍可执行外部副作用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：续期门禁开启且未越过单调截止点时为真，否则为假。
    pub fn still_owns_execution(&self) -> bool {
        self.authority.still_owns()
    }

    /// 业务作用：在业务外部副作用前统一检查取消、失权与本地持权截止，调用方无需接触租约实现。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仍持权且未取消时成功；否则返回结构化 `JobExecutionStopped`。
    pub fn checkpoint(&self) -> Result<()> {
        if self.is_cancellation_requested() {
            return Err(crate::job::JobError::ExecutionStopped(
                "当前 attempt 已收到取消请求".to_owned(),
            )
            .into());
        }
        if !self.still_owns_execution() {
            return Err(crate::job::JobError::ExecutionStopped(
                "当前 attempt 已失权或超过本地保守截止".to_owned(),
            )
            .into());
        }
        Ok(())
    }
}

/// 编程式 Handler 使用的上下文名称；与底层执行投影为同一冻结类型。
pub type JobContext = JobExecution;

/// 业务 Handler 常见返回形态到封闭 `JobOutcome` 的转换合同。
pub trait IntoJobHandlerResult {
    /// 业务作用：把业务返回值归一化为 Job 状态机可提交的结果。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：封闭结果码与有界前摘要。
    fn into_job_handler_result(self) -> JobOutcome;
}

impl IntoJobHandlerResult for JobOutcome {
    /// 业务作用：保留业务显式给出的 Job 结果。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原结果。
    fn into_job_handler_result(self) -> JobOutcome {
        self
    }
}

impl IntoJobHandlerResult for () {
    /// 业务作用：把无返回值 Handler 解释为成功完成。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：摘要为空的成功结果。
    fn into_job_handler_result(self) -> JobOutcome {
        JobOutcome::success()
    }
}

impl<E> IntoJobHandlerResult for std::result::Result<JobOutcome, E>
where
    E: std::fmt::Display,
{
    /// 业务作用：把统一错误返回保守归一为可重试结果，避免基础设施失败被误记为永久成功。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功分支保留业务结果；错误分支返回不暴露业务载荷的可重试摘要。
    fn into_job_handler_result(self) -> JobOutcome {
        self.unwrap_or_else(|error| JobOutcome::retry(error.to_string()))
    }
}

impl<E> IntoJobHandlerResult for std::result::Result<(), E>
where
    E: std::fmt::Display,
{
    /// 业务作用：把常见的无值业务 Result 归一为 Job 成功或可重试失败。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Ok(())` 为成功；错误为带稳定文本的可重试结果。
    fn into_job_handler_result(self) -> JobOutcome {
        match self {
            Ok(()) => JobOutcome::success(),
            Err(error) => JobOutcome::retry(error.to_string()),
        }
    }
}

/// Handler 返回的 future 别名，便于 trait 对象声明。
pub type JobHandlerFuture<'a> = Pin<Box<dyn Future<Output = JobOutcome> + Send + 'a>>;

/// 业务 Handler 合同；框架取得执行权后调用一次，返回结果码与摘要。
pub trait JobHandler: Send + Sync {
    /// 业务作用：处理一个已取得执行权的 attempt；实现应在写外部资源前用 `attempt_token` 做 fencing。
    ///
    /// 参数说明：
    /// - `execution`: 当前执行上下文，含身份、执行权与已解码参数。
    ///
    /// 返回：解析出的结果 future，决定 Run 进入成功、重试或终态。
    fn handle<'a>(&'a self, execution: &'a JobExecution) -> JobHandlerFuture<'a>;
}

/// 闭包 Handler 适配器；上下文按值克隆，使返回 future 不借用调度器内部记录。
struct ClosureJobHandler<F>(F);

impl<F, Fut, R> JobHandler for ClosureJobHandler<F>
where
    F: Fn(JobContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoJobHandlerResult + Send + 'static,
{
    /// 业务作用：把已取得执行权的上下文交给业务闭包，并归一化其异步返回值。
    ///
    /// 参数说明：
    /// - `execution`: 当前冻结执行上下文。
    ///
    /// 返回：不借用内部记录的 Handler future。
    fn handle<'a>(&'a self, execution: &'a JobExecution) -> JobHandlerFuture<'a> {
        let future = (self.0)(execution.clone());
        Box::pin(async move { future.await.into_job_handler_result() })
    }
}

/// 业务作用：把编程式异步闭包封装为统一 Handler trait object。
///
/// 参数说明：
/// - `handler`: 接收 `JobContext` 的异步业务函数。
///
/// 返回：可登记到运行时的共享 Handler。
pub(crate) fn handler_from_fn<F, Fut, R>(handler: F) -> std::sync::Arc<dyn JobHandler>
where
    F: Fn(JobContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoJobHandlerResult + Send + 'static,
{
    std::sync::Arc::new(ClosureJobHandler(handler))
}

/// 业务作用：把服务端租约跨度转换为扣除安全余量后的本地单调截止，墙钟跳变不影响持权判断。
///
/// 参数说明：`redis_now`、`lease_until` 与安全余量毫秒。
///
/// 返回：不早于当前单调时刻的保守截止点。
fn local_deadline(redis_now: i64, lease_until: i64, safety_ms: u64) -> tokio::time::Instant {
    let safety_ms = i64::try_from(safety_ms).unwrap_or(i64::MAX);
    let usable_ms = lease_until
        .saturating_sub(redis_now)
        .saturating_sub(safety_ms)
        .max(0) as u64;
    tokio::time::Instant::now()
        .checked_add(Duration::from_millis(usable_ms))
        .unwrap_or_else(tokio::time::Instant::now)
}
