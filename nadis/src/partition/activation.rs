//! 注册校验与全部分区组的一次性消费激活屏障。

use std::collections::HashSet;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{runtime::GroupRuntime, PreparedGroup};
use crate::config::MAX_REDIS_NAME_BYTES;
use crate::error::{NasaRedisError, Result};

/// 业务作用：拒绝无法稳定匹配的路由名称，避免注册和消费对空白或超长名称产生不同解释。
///
/// 参数说明：
/// - `topic`: 注册的业务主题。
/// - `event`: 注册的业务事件。
///
/// 返回：名称满足边界时成功；否则返回配置错误且不发布 handler。
pub(super) fn validate_route(topic: &str, event: &str) -> Result<()> {
    for (field, value) in [("topic", topic), ("event", event)] {
        if value.is_empty()
            || value.trim() != value
            || value.len() > MAX_REDIS_NAME_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(NasaRedisError::Config(format!(
                "partition {field} must be nonempty, trimmed, without control characters and at most {MAX_REDIS_NAME_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

/// 业务作用：在任何后台资源启动前复验完整路由集合，拒绝空消费运行时及跨组重复路由。
///
/// 参数说明：
/// - `groups`: prepare 冻结的全部物理组和候选 handler。
///
/// 返回：存在至少一条且全部路由唯一时成功；失败不产生消费副作用。
pub(super) fn validate_routes(groups: &[PreparedGroup]) -> Result<()> {
    let mut routes = HashSet::new();
    for group in groups {
        for (topic, event) in group.plans.keys() {
            validate_route(topic, event)?;
            if !routes.insert((topic.as_str(), event.as_str())) {
                return Err(NasaRedisError::Config(
                    "partition duplicate route at activation".into(),
                ));
            }
        }
    }
    if routes.is_empty() {
        return Err(NasaRedisError::Config(
            "partition requires at least one consumer plan".into(),
        ));
    }
    Ok(())
}

/// 未提交的激活票据拥有全部 dormant 任务；取消启动 Future 也不能让它们开始消费。
pub(super) struct PlanActivation {
    gate: CancellationToken,
    groups: Vec<Arc<GroupRuntime>>,
    committed: bool,
    core: Option<Arc<super::runtime::engine::RedisPartitionRuntime>>,
}

impl PlanActivation {
    /// 业务作用：建立初始关闭的共享消费屏障。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未激活且尚无后台任务的唯一事务 owner。
    pub(super) fn new() -> Self {
        Self {
            gate: CancellationToken::new(),
            groups: Vec::new(),
            committed: false,
            core: None,
        }
    }

    /// 业务作用：让每个组的监督任务等待同一个一次性发布信号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅供内部启动路径等待的共享屏障，不授予外部调用方发布权。
    pub(super) fn start_gate(&self) -> CancellationToken {
        self.gate.clone()
    }

    /// 业务作用：在下一次可取消等待前接管一个已创建组，确保回滚能枚举全部任务。
    ///
    /// 参数说明：
    /// - `group`: 已登记监督句柄但尚未开放消费的组。
    ///
    /// 返回：组的启动清理责任转移给当前票据。
    pub(super) fn track(&mut self, group: Arc<GroupRuntime>) {
        self.groups.push(group);
    }

    /// 业务作用：在路由与远端合同均就绪后只做本地内存发布，一次性允许全部组开始消费。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费准入开放；后续生命周期由 RunningPartition 接管。
    pub(super) fn commit(mut self) {
        self.committed = true;
        // 可失败的远端就绪必须先于此信号，任何一个组都不能提前观察半套注册集合。
        self.gate.cancel();
    }

    /// 业务作用：关闭未激活组并等待其监督任务结束，原始启动错误由调用方保留。
    ///
    /// 参数说明：`cause` 为激活失败的原始原因。
    ///
    /// 返回：资源收口后返回原始错误；等待超时返回结构化剩余责任，清理操作继续受监督。
    pub(super) async fn rollback(&mut self, cause: NasaRedisError) -> NasaRedisError {
        let Some(core) = self.core.take() else {
            return cause;
        };
        let timeout_ms = self
            .groups
            .first()
            .map_or(35_000, |group| group.cfg.drain_timeout_ms);
        let publisher = super::publisher::PublisherCoordinator::new(core.limits.clone());
        let cleanup = super::shutdown::ShutdownOperation::new(
            core,
            publisher,
            std::mem::take(&mut self.groups),
        );
        let report = cleanup
            .wait(std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms))
            .await;
        if report.converged {
            cause
        } else {
            NasaRedisError::StartRollbackNotConverged {
                cause: cause.to_string(),
                remaining: Box::new(report),
            }
        }
    }

    /// 业务作用：在启动的可取消等待前接管全部执行域的 Runner 回滚责任。
    /// 参数说明：`core` 为已建立的共享运行时。
    /// 返回：未提交退出时该执行域随激活事务关闭。
    pub(super) fn track_core(&mut self, core: Arc<super::runtime::engine::RedisPartitionRuntime>) {
        self.core = Some(core);
    }
}

impl Drop for PlanActivation {
    /// 业务作用：启动等待被取消时关闭未激活 owner，防止消费任务游离到调用方生命周期之外。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同步关闭准入并请求监督清理；析构不声称 Redis I/O 已完成 join。
    fn drop(&mut self) {
        if !self.committed {
            if let Some(core) = self.core.take() {
                core.close_roots();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        core.stop().await;
                    });
                }
            }
            for group in &self.groups {
                group.shutdown_on_drop();
            }
        }
    }
}
