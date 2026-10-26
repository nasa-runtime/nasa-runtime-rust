use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::native::{protocol, NativeState};

struct Waiting {
    deadline: Option<Instant>,
    operation_deadline: Option<Instant>,
    failure: Option<sqlx::Error>,
    expired: bool,
    finished: bool,
}

pub(crate) struct Consumer {
    idle: Duration,
    waiting: Mutex<Waiting>,
}

impl Consumer {
    /// 业务作用：核验未终结流的操作总期限及消费空闲期限，使已有缓冲不能无限保留执行许可。
    /// 参数说明：无。
    /// 返回：一旦到期持续返回真，不允许旧流通过再次 poll 恢复。
    fn expired(&self) -> bool {
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !waiting.finished
            && [waiting.deadline, waiting.operation_deadline]
                .into_iter()
                .flatten()
                .any(|deadline| Instant::now() >= deadline)
        {
            waiting.expired = true;
        }
        waiting.expired
    }

    /// 业务作用：结果可供消费时开始空闲计时，重复生产不会延长尚未消费的等待期限。
    /// 参数说明：无。
    /// 返回：保留最早未消费期限；正常终结后不重新启用。
    fn ready(&self) {
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !waiting.finished && !waiting.expired && waiting.deadline.is_none() {
            waiting.deadline = Some(Instant::now() + self.idle);
        }
    }
}

/// 同一受控查询的消费预算；由连接创建，不能解除已到期流的限制。
#[derive(Clone)]
pub struct SqliteStreamBudget {
    consumer: Arc<Consumer>,
    state: Arc<NativeState>,
}

impl SqliteStreamBudget {
    /// 业务作用：将流的消费责任登记到原生执行单元，调用方不再轮询时仍能触发物理收口。
    /// 参数说明：
    /// - `state`：本次独占借用的连接状态。
    /// - `idle`：允许的最长消费间隔。
    /// 返回：共享同一查询预算；已终结查询不会给新查询遗留期限。
    pub(crate) fn new(state: Arc<NativeState>, idle: Duration) -> Self {
        let consumer = {
            let mut slot = state
                .consumer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let current = slot.upgrade().filter(|consumer| {
                !consumer
                    .waiting
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .finished
            });
            current.unwrap_or_else(|| {
                let consumer = Arc::new(Consumer {
                    idle,
                    waiting: Mutex::new(Waiting {
                        deadline: Some(Instant::now() + idle),
                        operation_deadline: None,
                        failure: None,
                        expired: false,
                        finished: false,
                    }),
                });
                *slot = Arc::downgrade(&consumer);
                consumer
            })
        };
        Self {
            consumer,
            state,
        }
    }

    /// 业务作用：在取下一行前检查消费期限，并把数据库等待从消费者停顿中分离。
    /// 参数说明：无。
    /// 返回：预算仍有效才允许轮询；到期返回错误并撤销本连接继续执行的权利。
    pub fn poll_started(&self) -> Result<(), sqlx::Error> {
        self.check_delivery()?;
        let mut waiting = self
            .consumer
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !waiting.finished
            && waiting
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            waiting.expired = true;
        }
        if waiting.expired {
            drop(waiting);
            // 只撤销仍绑定本流的连接，已结束流不能影响连接之后的独立借用。
            self.state.consumer_expired();
            return Err(self
                .take_failure()
                .unwrap_or_else(|| protocol("SQLite stream consumer idle deadline exceeded")));
        }
        // 复验与进入数据库等待必须在同一锁内，避免并发到期后被新的 poll 清除等待期限。
        waiting.deadline = None;
        Ok(())
    }

    /// 业务作用：把本次 SQL 的绝对期限绑定到消费责任，缓冲行不能取得新的执行时间。
    /// 参数说明：
    /// - `deadline`：排队、执行和交付共同使用的单调截止点。
    /// 返回：后续轮询及交付按同一截止点核验。
    pub(crate) fn start_operation(&self, deadline: Instant) {
        self.consumer
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .operation_deadline
            .get_or_insert(deadline);
    }

    /// 业务作用：在正常结果交付前复验 SQL 总期限和来源资格，拒绝失效后的缓冲结果。
    /// 参数说明：无。
    /// 返回：资格有效时成功；否则撤销当前查询的继续执行与提交权，优先交付已确认的原始数据库错误。
    pub fn check_delivery(&self) -> Result<(), sqlx::Error> {
        let expired = self
            .consumer
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .operation_deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        let reason = if expired {
            Some("SQLite operation deadline exceeded")
        } else if self.state.business_expired() {
            Some("SQLite datasource is isolated or its drain deadline expired")
        } else {
            None
        };
        if let Some(reason) = reason {
            // 只撤销本流仍占用的连接；旧预算不能影响同一原生连接的后续独立查询。
            if self
                .state
                .consumer
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .upgrade()
                .is_some_and(|current| Arc::ptr_eq(&current, &self.consumer))
            {
                self.state.poisoned.store(true, Ordering::Release);
                if self.state.transaction.load(Ordering::Acquire) {
                    self.state.rollback_only.store(true, Ordering::Release);
                }
            }
            return Err(self.take_failure().unwrap_or_else(|| protocol(reason)));
        }
        Ok(())
    }

    /// 业务作用：保存 worker 已确认的失败收据，不让缓冲队列或交付门禁覆盖数据库错误分类。
    /// 参数说明：
    /// - `error`：事务连续性和写入状态已发布后的原始失败。
    /// 返回：当前查询持有唯一错误，后续只转移给一个消费终态。
    pub(crate) fn record_failure(&self, error: sqlx::Error) {
        self.consumer
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .failure = Some(error);
    }

    /// 业务作用：转移当前查询的原始失败收据，避免多层流重复交付同一错误。
    /// 参数说明：无。
    /// 返回：尚未交付的数据库错误；没有确认失败或已交付时为 None。
    pub(crate) fn take_failure(&self) -> Option<sqlx::Error> {
        self.consumer
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .failure
            .take()
    }

    /// 业务作用：行交付或结果进入缓冲后开始等待消费，不因生产速度延长旧期限。
    /// 参数说明：无。
    /// 返回：下一次消费必须在配置的空闲期限内发生。
    pub fn row_ready(&self) {
        self.consumer.ready();
    }

    /// 业务作用：正常结束或交付错误后解除本流的空闲计时。
    /// 参数说明：无。
    /// 返回：不清除已发生的到期事实，也不恢复连接或事务提交权。
    pub fn finish(&self) {
        let mut waiting = self
            .consumer
            .waiting
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        waiting.finished = true;
        waiting.deadline = None;
    }
}

impl NativeState {
    /// 业务作用：在执行单元内检查消费者是否失去继续持有连接的时间许可。
    /// 参数说明：无。
    /// 返回：到期时标记连接不可复用、事务只能回滚，并要求物理关闭。
    pub(crate) fn consumer_expired(&self) -> bool {
        let consumer = self
            .consumer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade();
        if consumer.is_some_and(|consumer| consumer.expired()) {
            self.poisoned.store(true, Ordering::Release);
            if self.transaction.load(Ordering::Acquire) {
                self.rollback_only.store(true, Ordering::Release);
            }
            return true;
        }
        false
    }

    /// 业务作用：worker 产生结果时登记消费等待，覆盖等待 future 被取消后再无人轮询的情况。
    /// 参数说明：无。
    /// 返回：只更新当前查询预算，不持有消费者或延长连接生命周期。
    pub(crate) fn consumer_ready(&self) {
        if let Some(consumer) = self
            .consumer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
        {
            consumer.ready();
        }
    }
}
