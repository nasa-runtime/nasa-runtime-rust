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
    /// 业务作用：创建由当前客户端管理的隔离、熔断及实例摘除状态表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回空状态集合和分配准入锁，具体服务及实例状态按调用惰性建立。
    pub(crate) fn new() -> Self {
        Self {
            admission: Mutex::new(()),
            bulkheads: DashMap::new(),
            circuits: DashMap::new(),
            outliers: DashMap::new(),
        }
    }

    /// 业务作用：为逻辑服务无等待地取得一次并发许可。
    ///
    /// 参数说明：
    /// - `service`：非空且不超过 256 字节的服务状态键。
    /// - `options`：首次创建该服务信号量时使用的并发上限。
    ///
    /// 返回：有容量时返回释放即归还的拥有式许可；非法服务名或目录满返回 ResilienceStateLimit，无可用许可返回 BulkheadRejected，不建立排队等待。
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

    /// 业务作用：在发起逻辑请求前检查服务熔断窗口，并独占半开探针。
    ///
    /// 参数说明：
    /// - `service`：服务熔断状态的隔离键。
    /// - `options`：本次尝试携带的失败阈值及开放时长。
    ///
    /// 返回：关闭态或唯一到期探针取得结算凭证；窗口未到期或已有探针返回 CircuitOpen，状态容量不足返回 ResilienceStateLimit。此入口不代表传输已提交。
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
                // 到期只开放一个恢复探针，锁内复验并占位，防止并发请求同时冲向故障后端。
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

    /// 业务作用：检查实例摘除窗口，并在到期后清除旧失败状态。
    ///
    /// 参数说明：
    /// - `service`：实例所属逻辑服务。
    /// - `instance`：以地址端口定位的候选实例。
    /// - `now`：当前单调观察时刻。
    ///
    /// 返回：未过期返回 true；未登记、无窗口或已到期返回 false，到期时同步清空窗口和连续失败数。
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

    /// 业务作用：根据一次实例传输结果更新连续失败及摘除期限。
    ///
    /// 参数说明：
    /// - `service`：本次请求所属逻辑服务。
    /// - `instance`：提供状态地址端口身份的实例。
    /// - `success`：是否取得成功业务传输结果。
    /// - `options`：失败摘除阈值和窗口期限。
    ///
    /// 返回：成功清空失败及摘除状态；失败达到阈值时开窗并清零计数。新实例目录满时直接返回且不建立记录，不报告成功或错误值。
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
                // 不为新身份越过固定容量；缺少统计记录不应被调用方解释为已确认健康。
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：后续发现撤权不会抹去已经提交过业务请求的事实。
    pub(crate) fn mark_submitted(&mut self) {
        self.submitted = true;
    }

    /// 业务作用：仅用已提交业务请求的最终结果结算熔断状态，并阻止 Drop 重复计数。
    ///
    /// 参数说明：
    /// - `success`：表示已提交请求是否取得成功的业务传输结果。
    ///
    /// 返回：未提交时只归还 half-open 占位，不延长打开窗口或清零失败记录。
    pub(crate) fn complete(mut self, success: bool) {
        self.settle(success);
        self.completed = true;
    }

    /// 业务作用：区分业务传输结果与发现、准入阶段终止，避免注册中心故障污染业务熔断。
    ///
    /// 参数说明：
    /// - `success`：是已经提交请求的最终结果。
    ///
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：发现失败不影响业务失败统计，已提交请求的取消仍触发失败策略。
    fn drop(&mut self) {
        if !self.completed {
            self.settle(false);
        }
    }
}

/// 业务作用：在同一状态锁内结算业务请求并释放半开探针占位。
///
/// 参数说明：
/// - `state`：要结算的服务熔断状态。
/// - `threshold`：关闭态连续失败触发开窗的阈值。
/// - `open_duration`：新建或延长熔断窗口的时长。
/// - `success`：已提交业务请求的最终结果。
///
/// 返回：成功关闭窗口并清零失败；失败在已有窗口或达到阈值时重新开窗并清零计数。所有路径清除半开在途标记，不执行传输。
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

/// 业务作用：用内部零字节边界组合服务与端点的摘除状态身份。
///
/// 参数说明：
/// - `service`：调用方已验证的逻辑服务名。
/// - `instance`：提供地址和端口的已验证候选。
///
/// 返回：返回由服务、地址、端口构成的文本键；不校验输入中的分隔字节，不包含集群或 metadata。
fn instance_key(service: &str, instance: &Instance) -> String {
    format!("{service}\0{}\0{}", instance.ip, instance.port)
}
