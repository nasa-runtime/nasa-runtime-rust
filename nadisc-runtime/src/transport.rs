//! 调用层向共同 owner 登记连接与正文任务，使无人读取的流也服从停机和期限。

use crate::{closed, lock, spawn_owned, Core, DiscoveryHandle};
use nadisc::managed::{DiscoveryError, ErrorCode, Result};
use std::{
    future::Future,
    sync::{Arc, Weak},
};
use tokio::{sync::OwnedSemaphorePermit, task::AbortHandle, time::Instant};
use tokio_util::sync::CancellationToken;

/// 一次传输任务的容量许可，绑定 owner 的关闭通知与任务归属。
pub struct TransportLease {
    core: Weak<Core>,
    stop: CancellationToken,
    permit: OwnedSemaphorePermit,
}

impl DiscoveryHandle {
    /// 业务作用：在打开传输前预留 owner 级有限容量，关闭和容量不足时不启动连接。
    /// 参数说明：无。
    /// 返回：一次连接或正文任务的额度；释放后可由其他调用复用。
    pub fn reserve_transport(&self) -> Result<TransportLease> {
        let core = self.core.upgrade().ok_or_else(closed)?;
        let gate = lock(&core.gate);
        if gate.failed || gate.close_started {
            return Err(closed());
        }
        let permit = core.transports.clone().try_acquire_owned().map_err(|_| {
            DiscoveryError::new(
                ErrorCode::CapacityExceeded,
                "owner transport task capacity reached",
            )
        })?;
        Ok(TransportLease {
            core: Arc::downgrade(&core),
            stop: core.stop.child_token(),
            permit,
        })
    }
}

impl TransportLease {
    /// 业务作用：让连接、排队和流消费等待响应同一 owner 的关闭。
    /// 参数说明：无。
    /// 返回：owner 最终关闭后完成，不授予调用方关闭 owner 的权限。
    pub async fn cancelled(&self) {
        self.stop.cancelled().await;
    }

    /// 业务作用：将有限传输任务交给 owner 接管，等待者丢弃不造成后台任务失去归属。
    /// 参数说明：`deadline` 是绝对期限，`future` 是不超过该期限的连接或正文处理。
    /// 返回：只控制本任务的终止句柄；关闭时禁止启动，任务结束自动归还容量。
    pub fn spawn(
        self,
        deadline: Instant,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<AbortHandle> {
        let core = self.core.upgrade().ok_or_else(closed)?;
        let gate = lock(&core.gate);
        if gate.failed || gate.close_started {
            return Err(closed());
        }
        if deadline <= Instant::now() {
            return Err(crate::timeout());
        }
        Ok(spawn_owned(&core, async move {
            let _permit = self.permit;
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => {},
                _ = tokio::time::sleep_until(deadline) => {},
                _ = future => {},
            }
        }))
    }
}
