//! 每服务 bulkhead/熔断与单实例异常摘除。
//!
//! 所有状态以服务名或稳定实例地址为 key，热路径只持有短同步锁；任何锁都不会跨网络 `await`。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nadisc::Instance;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{RestDiscoveryError, Result};
use crate::options::RestResilienceOptions;

/// 防止调用方用无界动态服务名把隔离状态表撑满。
const MAX_SERVICE_STATES: usize = 4096;
/// 单进程允许跟踪的实例异常状态硬上限。
const MAX_OUTLIER_STATES: usize = 65_536;

/// 单个逻辑服务的熔断状态；同步锁只保护计数与时间点，不跨网络等待。
#[derive(Debug, Default)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    half_open_in_flight: bool,
}

/// 单个服务实例的连续失败与临时摘除窗口。
#[derive(Debug, Default)]
struct OutlierState {
    consecutive_failures: u32,
    ejected_until: Option<Instant>,
}

/// 客户端拥有的隔离状态集合。
pub(crate) struct ResilienceRuntime {
    admission: Mutex<()>,
    bulkheads: DashMap<String, Arc<Semaphore>>,
    circuits: DashMap<String, Arc<Mutex<CircuitState>>>,
    outliers: DashMap<String, Arc<Mutex<OutlierState>>>,
}

impl ResilienceRuntime {
    /// 业务作用：创建空的有界隔离状态集合；状态只会在服务首次调用时按需分配。
    pub(crate) fn new() -> Self {
        Self {
            admission: Mutex::new(()),
            bulkheads: DashMap::new(),
            circuits: DashMap::new(),
            outliers: DashMap::new(),
        }
    }

    /// 业务作用：为一个逻辑请求取得 per-service bulkhead permit；无等待队列，满载立即拒绝。
    pub(crate) fn acquire_bulkhead(
        &self,
        service: &str,
        options: &RestResilienceOptions,
    ) -> Result<OwnedSemaphorePermit> {
        if service.is_empty() || service.len() > 256 {
            return Err(RestDiscoveryError::ResilienceStateLimit);
        }
        let semaphore = match self.bulkheads.get(service) {
            Some(existing) => Arc::clone(existing.value()),
            None => {
                let _admission = self
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if self.bulkheads.len() >= MAX_SERVICE_STATES {
                    return Err(RestDiscoveryError::ResilienceStateLimit);
                }
                Arc::clone(
                    self.bulkheads
                        .entry(service.to_owned())
                        .or_insert_with(|| {
                            Arc::new(Semaphore::new(options.max_concurrent_per_service))
                        })
                        .value(),
                )
            }
        };
        semaphore
            .try_acquire_owned()
            .map_err(|_| RestDiscoveryError::BulkheadRejected {
                service: service.to_owned(),
            })
    }

    /// 业务作用：在发送前进入服务熔断器；打开窗口内拒绝，窗口到期只允许一个 half-open 探针。
    pub(crate) fn begin_circuit(
        &self,
        service: &str,
        options: &RestResilienceOptions,
    ) -> Result<CircuitAttempt> {
        let state = match self.circuits.get(service) {
            Some(existing) => Arc::clone(existing.value()),
            None => {
                let _admission = self
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if self.circuits.len() >= MAX_SERVICE_STATES {
                    return Err(RestDiscoveryError::ResilienceStateLimit);
                }
                Arc::clone(
                    self.circuits
                        .entry(service.to_owned())
                        .or_insert_with(|| Arc::new(Mutex::new(CircuitState::default())))
                        .value(),
                )
            }
        };
        let now = Instant::now();
        let half_open = {
            let mut current = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(until) = current.open_until {
                if now < until || current.half_open_in_flight {
                    return Err(RestDiscoveryError::CircuitOpen {
                        service: service.to_owned(),
                    });
                }
                current.half_open_in_flight = true;
                true
            } else {
                false
            }
        };
        Ok(CircuitAttempt {
            state,
            threshold: options.circuit_failure_threshold,
            open_duration: options.circuit_open_duration,
            submitted: false,
            half_open,
            completed: false,
        })
    }

    /// 业务作用：判断实例是否仍处于异常摘除窗口；窗口到期时原地恢复候选资格。
    pub(crate) fn is_ejected(&self, service: &str, instance: &Instance, now: Instant) -> bool {
        let key = instance_key(service, instance);
        let Some(state) = self.outliers.get(&key) else {
            return false;
        };
        let mut current = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match current.ejected_until {
            Some(until) if now < until => true,
            Some(_) => {
                current.ejected_until = None;
                current.consecutive_failures = 0;
                false
            }
            None => false,
        }
    }

    /// 业务作用：记录一次实例尝试结果；失败达到阈值时临时摘除，成功立即清零。
    pub(crate) fn record_instance(
        &self,
        service: &str,
        instance: &Instance,
        success: bool,
        options: &RestResilienceOptions,
    ) {
        let key = instance_key(service, instance);
        let state = match self.outliers.get(&key) {
            Some(existing) => Arc::clone(existing.value()),
            None => {
                let _admission = self
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if self.outliers.len() >= MAX_OUTLIER_STATES {
                    return;
                }
                Arc::clone(
                    self.outliers
                        .entry(key)
                        .or_insert_with(|| Arc::new(Mutex::new(OutlierState::default())))
                        .value(),
                )
            }
        };
        let mut current = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if success {
            current.consecutive_failures = 0;
            current.ejected_until = None;
            return;
        }
        current.consecutive_failures = current.consecutive_failures.saturating_add(1);
        if current.consecutive_failures >= options.outlier_failure_threshold {
            current.ejected_until = Some(Instant::now() + options.outlier_ejection_duration);
            current.consecutive_failures = 0;
        }
    }
}

/// 一个逻辑请求对服务熔断器的完成凭证。
///
/// 已提交请求被取消时按失败处理；发现或准入拒绝仅释放 half-open 占位。
pub(crate) struct CircuitAttempt {
    state: Arc<Mutex<CircuitState>>,
    threshold: u32,
    open_duration: Duration,
    submitted: bool,
    half_open: bool,
    completed: bool,
}

impl CircuitAttempt {
    /// 业务作用：记录逻辑请求已进入 transport，允许后续失败或取消影响业务熔断。
    /// 参数说明：无。
    /// 返回：后续发现撤权不会抹去已经提交过业务请求的事实。
    pub(crate) fn mark_submitted(&mut self) {
        self.submitted = true;
    }

    /// 业务作用：仅用已提交业务请求的最终结果结算熔断状态，并阻止 Drop 重复计数。
    /// 参数说明：`success` 表示已提交请求是否取得成功的业务传输结果。
    /// 返回：未提交时只归还 half-open 占位，不延长打开窗口或清零失败记录。
    pub(crate) fn complete(mut self, success: bool) {
        self.settle(success);
        self.completed = true;
    }

    /// 业务作用：区分业务传输结果与发现、准入阶段终止，避免注册中心故障污染业务熔断。
    /// 参数说明：`success` 是已经提交请求的最终结果。
    /// 返回：所有路径均释放 half-open 占位，只有已提交请求更新成功或失败统计。
    fn settle(&self, success: bool) {
        if self.submitted {
            update_circuit(&self.state, self.threshold, self.open_duration, success);
        } else if self.half_open {
            // 没有业务传输就没有实例失败证据；仍须归还探针占位，让恢复后的请求可以重新尝试。
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .half_open_in_flight = false;
        }
    }
}

impl Drop for CircuitAttempt {
    /// 业务作用：请求取消或提前返回时归还探针占位，仅已提交业务的调用按失败结算。
    /// 参数说明：无。
    /// 返回：发现失败不影响业务失败统计，已提交请求的取消仍触发失败策略。
    fn drop(&mut self) {
        if !self.completed {
            self.settle(false);
        }
    }
}

/// 业务作用：在线性化锁内完成成功复位或失败开窗，保证 half-open 标记不会遗留。
fn update_circuit(
    state: &Mutex<CircuitState>,
    threshold: u32,
    open_duration: Duration,
    success: bool,
) {
    let mut current = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    current.half_open_in_flight = false;
    if success {
        current.consecutive_failures = 0;
        current.open_until = None;
        return;
    }
    current.consecutive_failures = current.consecutive_failures.saturating_add(1);
    if current.open_until.is_some() || current.consecutive_failures >= threshold {
        current.open_until = Some(Instant::now() + open_duration);
        current.consecutive_failures = 0;
    }
}

/// 业务作用：用不可混淆的内部分隔符组合服务与实例地址，避免不同文本元组共享摘除状态。
fn instance_key(service: &str, instance: &Instance) -> String {
    format!("{service}\0{}\0{}", instance.ip, instance.port)
}
