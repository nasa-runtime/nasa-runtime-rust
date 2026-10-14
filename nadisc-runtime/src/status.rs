//! 只读诊断按已冻结配置连接聚合，不把任意服务输入或实例地址变为指标标签。

use crate::{lock, Core, OwnerPhase, RegistrationStatus};
use nadisc::managed::DiscoveryError;
use std::sync::{atomic::Ordering, Weak};
use tokio::time::Instant;

/// 不拥有关闭权的运行状态观察入口，不因采集状态触发网络访问。
#[derive(Clone)]
pub struct RuntimeObserver {
    pub(crate) core: Weak<Core>,
}

/// 单个 qualifier 的准备状态、观察容量和累计查询统计。
#[derive(Clone, Debug)]
pub struct ConnectionStatus {
    /// 连接的本地 qualifier，不是后端服务名称。
    pub name: String,
    /// 连接是否已达到所需作用域的访问条件。
    pub prepared: bool,
    /// 最近一次连接准备错误，成功恢复后可清除。
    pub preparation_error: Option<DiscoveryError>,
    /// 该连接当前持有的服务观察数量。
    pub observations: usize,
    /// 按当前身份和时效规则仍可用于路由的观察数量。
    pub usable_observations: usize,
    /// 保留证据中的最大年龄，单位为毫秒；没有证据时为空。
    pub oldest_observation_age_ms: Option<u64>,
    /// 该连接累计发起的查询次数。
    pub queries: u64,
    /// 该连接累计查询错误次数。
    pub query_errors: u64,
    /// 该连接查询累计耗时，单位为微秒。
    pub query_micros: u64,
}

/// 同一 owner 下的连接、注册、资源使用和关闭进度摘要。
#[derive(Clone, Debug)]
pub struct RuntimeStatus {
    /// owner 当前生命周期阶段。
    pub phase: OwnerPhase,
    /// 后台监督是否异常退出；异常时停止使用现有路由证据。
    pub supervisor_failed: bool,
    /// 已解析的默认连接 qualifier；未配置 primary 且无显式覆盖时为空。
    pub default_connection: Option<String>,
    /// 以 qualifier 为键的连接目录，不按顺序推断默认连接。
    pub connections: Vec<ConnectionStatus>,
    /// 注册目标集合，包含各目标独立的状态或发布策略。
    pub registrations: Vec<RegistrationStatus>,
    /// 所有保留快照累计占用的实例数量。
    pub retained_instances: usize,
    /// 所有保留快照占用的估算字节数。
    pub snapshot_bytes: usize,
    /// 当前占用容量许可的传输任务数量。
    pub transport_tasks: usize,
    /// 冻结的关闭总期限剩余毫秒数；尚未关闭时为空。
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
