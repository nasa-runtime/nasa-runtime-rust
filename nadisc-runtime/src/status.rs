//! 只读诊断按已冻结配置连接聚合，不把任意服务输入或实例地址变为指标标签。

use crate::{lock, Core, OwnerPhase, RegistrationStatus};
use nadisc::managed::DiscoveryError;
use std::sync::{atomic::Ordering, Weak};
use tokio::time::Instant;

#[derive(Clone)]
pub struct RuntimeObserver {
    pub(crate) core: Weak<Core>,
}

#[derive(Clone, Debug)]
pub struct ConnectionStatus {
    pub name: String,
    pub prepared: bool,
    pub preparation_error: Option<DiscoveryError>,
    pub observations: usize,
    pub usable_observations: usize,
    pub oldest_observation_age_ms: Option<u64>,
    pub queries: u64,
    pub query_errors: u64,
    pub query_micros: u64,
}

#[derive(Clone, Debug)]
pub struct RuntimeStatus {
    pub phase: OwnerPhase,
    pub supervisor_failed: bool,
    pub default_connection: Option<String>,
    pub connections: Vec<ConnectionStatus>,
    pub registrations: Vec<RegistrationStatus>,
    pub retained_instances: usize,
    pub snapshot_bytes: usize,
    pub transport_tasks: usize,
    pub shutdown_remaining_ms: Option<u64>,
}

impl RuntimeObserver {
    /// 业务作用：同步读取容量、注册和连接证据摘要，不在探针或指标抓取时发起网络查询。
    /// 参数说明：无。
    /// 返回：owner 尚有状态时给出按当前身份代次裁决的有限摘要；所有资源释放后为空，不展示凭据、URL 或实例 metadata。
    pub fn snapshot(&self) -> Option<RuntimeStatus> {
        let core = self.core.upgrade()?;
        let gate = lock(&core.gate);
        let now = Instant::now();
        let connections = core
            .connections
            .values()
            .map(|connection| {
                let slots = lock(&connection.slots);
                let mut usable = 0;
                let mut oldest = None;
                for slot in slots.values() {
                    let state = lock(&slot.state);
                    if !gate.failed
                        && !gate.close_started
                        && crate::observation::check(connection, &state, false).is_ok()
                    {
                        usable += 1;
                    }
                    if let Some(observed) = state.observed_at {
                        let age = now
                            .saturating_duration_since(observed)
                            .as_millis()
                            .min(u64::MAX as u128) as u64;
                        oldest = Some(oldest.map_or(age, |old: u64| old.max(age)));
                    }
                }
                ConnectionStatus {
                    name: connection.name.clone(),
                    prepared: *connection.prepared.borrow(),
                    preparation_error: lock(&connection.error).clone(),
                    observations: slots.len(),
                    usable_observations: usable,
                    oldest_observation_age_ms: oldest,
                    queries: connection.queries.load(Ordering::Relaxed),
                    query_errors: connection.query_errors.load(Ordering::Relaxed),
                    query_micros: connection.query_micros.load(Ordering::Relaxed),
                }
            })
            .collect();
        let registrations = gate
            .registrations
            .values()
            .map(|record| record.current_status(&gate))
            .collect();
        let quota = lock(&core.quota);
        Some(RuntimeStatus {
            phase: gate.phase,
            supervisor_failed: gate.failed,
            default_connection: core.default_connection.clone(),
            connections,
            registrations,
            retained_instances: quota.instances,
            snapshot_bytes: quota.bytes,
            transport_tasks: core.limits.max_transport_tasks - core.transports.available_permits(),
            shutdown_remaining_ms: gate.shutdown_deadline.map(|deadline| {
                deadline
                    .saturating_duration_since(now)
                    .as_millis()
                    .min(u64::MAX as u128) as u64
            }),
        })
    }
}
