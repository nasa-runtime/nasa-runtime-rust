//! 与配置加载器和宿主框架无关的容量、时效与注册策略。

use nadisc::managed::{DiscoveryError, ErrorCode, Result};
use serde::{Deserialize, Serialize};

/// owner 共享的资源上限；超出容量时拒绝新增工作。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// 同时保留的连接数量上限。
    pub max_connections: usize,
    /// owner 可持有的注册目标数量上限。
    pub max_registrations: usize,
    /// 每个连接可同时观察的服务数量上限。
    pub max_service_watches_per_connection: usize,
    /// owner 后端操作的并发上限。
    pub max_parallel_operations: usize,
    /// owner 同时持有的后端 watch 流数量上限。
    pub max_watch_streams: usize,
    /// 单个服务快照允许包含的实例数量上限。
    pub max_instances_per_service: usize,
    /// owner 所有保留快照的实例总数上限。
    pub max_total_instances: usize,
    /// owner 所有保留快照的估算字节总数上限。
    pub max_snapshot_bytes: usize,
    /// 连接驱动与正文等传输任务的共享容量上限。
    pub max_transport_tasks: usize,
}

impl Default for Limits {
    /// 业务作用：为连接、后台工作与被持有的快照设置有限容量。
    /// 参数说明：无。
    /// 返回：所有限制均为正值的默认容量。
    fn default() -> Self {
        Self {
            max_connections: 16,
            max_registrations: 64,
            max_service_watches_per_connection: 1024,
            max_parallel_operations: 4,
            max_watch_streams: 64,
            max_instances_per_service: 4096,
            max_total_instances: 65536,
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_transport_tasks: 4096,
        }
    }
}

impl Limits {
    /// 业务作用：拒绝无法执行或可能溢出的容量设置。
    /// 参数说明：无。
    /// 返回：合法容量可构造资源池；零容量和异常大容量返回配置错误。
    pub fn validate(&self) -> Result<()> {
        for value in [
            self.max_connections,
            self.max_registrations,
            self.max_service_watches_per_connection,
            self.max_parallel_operations,
            self.max_watch_streams,
            self.max_instances_per_service,
            self.max_total_instances,
            self.max_snapshot_bytes,
            self.max_transport_tasks,
        ] {
            if value == 0 || value > u32::MAX as usize {
                return Err(invalid("capacity is outside the supported range"));
            }
        }
        if self.max_instances_per_service > self.max_total_instances {
            return Err(invalid("per-service capacity exceeds total capacity"));
        }
        if self.max_connections > 256
            || self.max_registrations > 65536
            || self.max_service_watches_per_connection > 65536
            || self.max_parallel_operations > 4096
            || self.max_transport_tasks > 65536
            || self.max_watch_streams > 4096
            || self
                .max_connections
                .saturating_mul(self.max_service_watches_per_connection)
                > 65536
        {
            return Err(invalid(
                "task or connection capacity exceeds the hard limit",
            ));
        }
        Ok(())
    }
}

/// 单个连接的准备、查询、证据时效和回收预算。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectionPolicy {
    /// 是否要求启动阶段确认成功；失败时阻止宿主完成启动。
    pub startup_required: bool,
    /// 一次请求的最大耗时，单位为毫秒。
    pub request_timeout_ms: u64,
    /// 后台轮询间隔，单位为毫秒，必须留出请求和证据时效预算。
    pub poll_interval_ms: u64,
    /// 发现证据的最大年龄，单位为毫秒。
    pub max_age_ms: u64,
    /// 查询异常后的旧证据宽限，单位为毫秒；零表示禁止异常复用。
    pub stale_if_error_ms: u64,
    /// 无人使用的服务观察保留时长，单位为毫秒。
    pub idle_timeout_ms: u64,
    /// 连接恢复的初始退避，单位为毫秒。
    pub initial_backoff_ms: u64,
    /// 连接恢复退避的上限，单位为毫秒。
    pub max_backoff_ms: u64,
}

impl Default for ConnectionPolicy {
    /// 业务作用：默认要求连接可用，并在查询异常时停止使用旧证据。
    /// 参数说明：无。
    /// 返回：操作、轮询、证据年龄与空闲回收的有限预算。
    fn default() -> Self {
        Self {
            startup_required: true,
            request_timeout_ms: 3000,
            poll_interval_ms: 5000,
            max_age_ms: 15000,
            stale_if_error_ms: 0,
            idle_timeout_ms: 30000,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
        }
    }
}

impl ConnectionPolicy {
    /// 业务作用：校验一次操作与观察周期能否落在证据有效期内。
    /// 参数说明：无。
    /// 返回：预算不合理时在任何网络活动之前拒绝配置。
    pub fn validate(&self) -> Result<()> {
        for value in [
            self.request_timeout_ms,
            self.poll_interval_ms,
            self.max_age_ms,
            self.idle_timeout_ms,
            self.initial_backoff_ms,
            self.max_backoff_ms,
        ] {
            if value == 0 || value > 86_400_000 {
                return Err(invalid("time budget is outside the supported range"));
            }
        }
        if self.stale_if_error_ms > self.max_age_ms
            || self.poll_interval_ms + self.request_timeout_ms >= self.max_age_ms
            || self.initial_backoff_ms > self.max_backoff_ms
        {
            return Err(invalid("observation and retry budgets are inconsistent"));
        }
        Ok(())
    }
}

/// 服务发布的首次确认要求、就绪影响和摘流预算。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RegistrationPolicy {
    /// 是否要求启动阶段确认成功；失败时阻止宿主完成启动。
    pub startup_required: bool,
    /// 远端注册失去有效健康证据时是否影响宿主整体就绪。
    pub readiness_critical: bool,
    /// 注册健康回查间隔，单位为毫秒。
    pub monitor_interval_ms: u64,
    /// 注册健康证据的有效窗口，单位为毫秒。
    pub observation_grace_ms: u64,
    /// 单项注销预算，单位为毫秒，仍受宿主关闭总期限限制。
    pub deregister_timeout_ms: u64,
}

impl Default for RegistrationPolicy {
    /// 业务作用：区分首次发布确认与运行期整体就绪影响。
    /// 参数说明：无。
    /// 返回：首次必须确认、运行期默认降级的注册策略。
    fn default() -> Self {
        Self {
            startup_required: true,
            readiness_critical: false,
            monitor_interval_ms: 5000,
            observation_grace_ms: 15000,
            deregister_timeout_ms: 3000,
        }
    }
}

impl RegistrationPolicy {
    /// 业务作用：保证回查与注销不会使用无界或相互矛盾的期限。
    /// 参数说明：`connection` 提供一次请求的最大预算。
    /// 返回：合法策略可用于冻结计划；不合法策略拒绝发布。
    pub fn validate(&self, connection: &ConnectionPolicy) -> Result<()> {
        if self.monitor_interval_ms == 0
            || self.deregister_timeout_ms == 0
            || self.monitor_interval_ms > 86_400_000
            || self.deregister_timeout_ms > 86_400_000
            || self.observation_grace_ms > 86_400_000
            || self.monitor_interval_ms + connection.request_timeout_ms >= self.observation_grace_ms
        {
            return Err(invalid("registration observation budgets are inconsistent"));
        }
        Ok(())
    }
}

/// 业务作用：约束本地连接及注册标识，避免含混引用与无界诊断维度。
/// 参数说明：`name` 是宿主提供的本地标识，不是后端服务名。
/// 返回：符合有限 ASCII 名称规则时成功。
pub fn validate_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
    {
        return Err(invalid("invalid local discovery name"));
    }
    Ok(())
}

/// 业务作用：统一生成不含配置原文的预检错误。
/// 参数说明：`reason` 是固定诊断原因。
/// 返回：调用方可安全展示的配置错误。
pub(crate) fn invalid(reason: &'static str) -> DiscoveryError {
    DiscoveryError::new(ErrorCode::InvalidConfig, reason)
}
