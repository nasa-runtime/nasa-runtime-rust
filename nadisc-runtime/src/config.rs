//! 与配置加载器和宿主框架无关的容量、时效与注册策略。

use nadisc::managed::{DiscoveryError, ErrorCode, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_connections: usize,
    pub max_registrations: usize,
    pub max_service_watches_per_connection: usize,
    pub max_parallel_operations: usize,
    pub max_watch_streams: usize,
    pub max_instances_per_service: usize,
    pub max_total_instances: usize,
    pub max_snapshot_bytes: usize,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectionPolicy {
    pub startup_required: bool,
    pub request_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub max_age_ms: u64,
    pub stale_if_error_ms: u64,
    pub idle_timeout_ms: u64,
    pub initial_backoff_ms: u64,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RegistrationPolicy {
    pub startup_required: bool,
    pub readiness_critical: bool,
    pub monitor_interval_ms: u64,
    pub observation_grace_ms: u64,
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
