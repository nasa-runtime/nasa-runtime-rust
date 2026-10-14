//! 解码与业务调用的实际退出责任及 owner 维护窗口。

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// 一次实际调用的责任；逻辑超时只撤销确认权限，实际闭包退出之前不释放调度名额。
pub(super) struct HandlerWorker {
    authority: Arc<AtomicBool>,
    started: Instant,
    timeout: Option<Duration>,
    ticket: Option<crate::consumer::worker::WorkerTicket<InvocationOutcome>>,
}

impl HandlerWorker {
    /// 业务作用：让自定义解码和 single/batch 业务在同一持久 worker 内共用准入预算，保留非 Send future 兼容。
    ///
    /// 参数说明：`runtime`、`route` 为冻结资源；`records` 为本次连续原始输入；
    /// `ambient` 为消费链路上下文；`span` 为本次调用的结构化追踪作用域；permit 为已取得的组执行额度。
    /// 返回：拥有实际退出责任的 worker；执行额度或线程不可用时不执行业务并返回致命结果。
    pub fn spawn(
        runtime: &Arc<OwnerRuntime>,
        route: &Arc<dyn crate::consumer::erased::ErasedConsumer>,
        records: Vec<RawRecord>,
        ambient: Option<natelemetry::TraceContext>,
        span: tracing::Span,
        permit: crate::consumer::worker::HandlerPermit,
    ) -> std::result::Result<Self, InvocationOutcome> {
        let authority = Arc::new(AtomicBool::new(true));
        let started = Instant::now();
        // 显式零与未配置同样关闭调用限时，不能生成已经到期的deadline撤销正常业务确认。
        let timeout = runtime
            .config
            .behavior
            .handler_timeout_ms
            .filter(|millis| *millis > 0)
            .map(Duration::from_millis);
        let task_runtime = Arc::clone(runtime);
        let route = Arc::clone(route);
        let input_permits = records
            .iter()
            .map(RawRecord::input_permit)
            .collect::<Vec<_>>();
        let deadline = timeout.map(|timeout| started + timeout);
        let task_authority = Arc::clone(&authority);
        let ticket = runtime
            .handlers
            .submit(permit, move || {
                // 解码可以释放 RawRecord，但业务仍持有拥有型上下文；整个调用退出才归还输入责任。
                let _input_permits = input_permits;
                span.in_scope(|| {
                    let mut decoded = Vec::with_capacity(records.len());
                    let mut failures = Vec::new();
                    for raw in records {
                        // 超时或失权后只等待正在执行的 codec 退出，不再开启后继解码或业务副作用。
                        if !task_authority.load(Ordering::Acquire)
                            || task_runtime.stop_requested.load(Ordering::Acquire)
                            || deadline.is_some_and(|end| Instant::now() >= end)
                        {
                            return InvocationOutcome::Failed(NafkaError::Broker(
                                "consumer execution authority expired".into(),
                            ));
                        }
                        match route.decode(&raw) {
                            Ok(mut record) => {
                                record.authority = Some(Arc::clone(&task_authority));
                                record.stopping = Some(Arc::clone(&task_runtime.stop_requested));
                                decoded.push(record);
                            }
                            Err(error) => failures.push((
                                raw.ctx.offset,
                                error.to_string().chars().take(256).collect(),
                            )),
                        }
                    }
                    // 最后一个 codec 也可能耗尽预算，不能仅凭它开始时有效就调用业务。
                    if !task_authority.load(Ordering::Acquire)
                        || task_runtime.stop_requested.load(Ordering::Acquire)
                        || deadline.is_some_and(|end| Instant::now() >= end)
                    {
                        return InvocationOutcome::Failed(NafkaError::Broker(
                            "consumer execution authority expired".into(),
                        ));
                    }
                    if !failures.is_empty() {
                        return InvocationOutcome::DecodeFailed { failures };
                    }
                    invoke_typed_handler(&task_runtime, &route, decoded, ambient, deadline)
                })
            })
            .map_err(InvocationOutcome::Fatal)?;
        Ok(Self {
            authority,
            started,
            timeout,
            ticket: Some(ticket),
        })
    }

    /// 业务作用：把 owner 的失权、停止或总超时事实传播给所有手动确认槽。
    ///
    /// 参数说明：`authorized` 表示当前窗口仍拥有消费权威。
    /// 返回：无；确认权限只能撤销，实际执行名额仍由完成票据持有。
    pub fn fence(&self, authorized: bool) {
        if !authorized
            || self
                .timeout
                .is_some_and(|budget| self.started.elapsed() >= budget)
        {
            self.authority.store(false, Ordering::Release);
        }
    }

    /// 业务作用：确认业务闭包已经实际返回，防止把逻辑超时当作执行完成。
    ///
    /// 参数说明：无。
    /// 返回：仅实际调用及其捕获资源退出时为 true；池线程可以继续服务后继调用。
    pub fn is_finished(&self) -> bool {
        self.ticket.as_ref().expect("调用尚未交回").is_finished()
    }

    /// 业务作用：收回实际调用责任，拒绝超过准入总预算的迟到成功。
    ///
    /// 参数说明：无。
    /// 返回：业务结论、超时失败或业务异常；确认权限在返回前永久关闭。
    pub fn finish(mut self) -> InvocationOutcome {
        self.authority.store(false, Ordering::Release);
        let (outcome, completed) = self.ticket.take().expect("调用只能交回一次").finish();
        let outcome = outcome.unwrap_or_else(InvocationOutcome::Fatal);
        // 使用业务实际返回时刻，owner 维护 poll 的等待不能计入已完成调用的耗时。
        if self
            .timeout
            .is_some_and(|budget| completed.duration_since(self.started) >= budget)
            && !matches!(outcome, InvocationOutcome::Fatal(_))
        {
            InvocationOutcome::Failed(NafkaError::Broker("consumer handler timeout".into()))
        } else {
            outcome
        }
    }
}

impl Drop for HandlerWorker {
    /// 业务作用：owner 异常展开窗口时立即撤销旧调用的确认权限。
    /// 参数说明：无。
    /// 返回：旧 Ack 不再有效；实际执行仍由线程池保留，不能凭票据析构报告停止。
    fn drop(&mut self) {
        self.authority.store(false, Ordering::Release);
    }
}

/// 业务作用：在每次派发前复验用户暂停，防止预取窗口越过已接受的暂停命令。
///
/// 参数说明：`health` 为共享快照；`tp` 为即将派发的分区。
/// 返回：存在 User 原因时为 true；框架保护原因由各自责任状态机解除。
pub(super) fn user_paused(health: &Arc<RwLock<GroupHealth>>, tp: &Tp) -> bool {
    health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .paused
        .iter()
        .any(|(current, reason)| current == tp && *reason == PauseReason::User)
}
