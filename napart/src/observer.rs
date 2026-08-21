//! Runner 集中负载观察与可复用窃取请求。

use crate::runner::RunnerInner;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 空闲目标提交给繁忙源 worker 的窃取请求。
pub(crate) struct StealRequest {
    target: u32,
    observation_epoch: u64,
    pending: Arc<AtomicBool>,
}

impl StealRequest {
    /// 业务作用：创建一笔只允许源 worker 完成的盗洞安装请求。
    ///
    /// 参数说明：
    /// - `target`: 希望承担负载的空闲 slot。
    /// - `observation_epoch`: 本轮负载观察代次。
    /// - `pending`: 当前目标复用的唯一请求门禁。
    ///
    /// 返回：成功占用目标门禁时返回请求；已有请求尚未完成时返回 None。
    pub(crate) fn try_new(
        target: u32,
        observation_epoch: u64,
        pending: Arc<AtomicBool>,
    ) -> Option<Arc<Self>> {
        pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(Arc::new(Self {
            target,
            observation_epoch,
            pending,
        }))
    }

    /// 业务作用：读取目标 slot，源 worker 安装前必须复验目标仍属于同代。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：观察时选择的目标下标。
    pub(crate) fn target(&self) -> u32 {
        self.target
    }

    /// 业务作用：读取观察代次，源 worker 可识别迟到请求而不接受 observer 的候选判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Runner 内单调观察序号。
    pub(crate) fn observation_epoch(&self) -> u64 {
        self.observation_epoch
    }

    /// 业务作用：判断请求是否仍等待源 worker 处理，重复队列引用不能重复安装盗洞。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未完成返回 true。
    pub(crate) fn pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// 业务作用：由源 worker 在安装成功或明确拒绝后单向结束请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次完成返回 true；重复处理返回 false。
    pub(crate) fn complete(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }
}

/// 业务作用：按 Runner 配置的低频间隔集中观察负载，只发布请求，不直接修改源类型路由。
///
/// 参数说明：
/// - `inner`: 当前 generation 的 Runner 内核；受监督任务退出前保持代次可达。
///
/// 返回：生命周期停止或 Runner 释放后退出。
pub(crate) async fn run(inner: Arc<RunnerInner>) {
    loop {
        if inner.observer_should_exit() {
            return;
        }
        if inner.observer_should_run() {
            inner.observe_load();
        }
        let interval = if inner.observer_should_run() {
            inner.config().load_observer_interval
        } else {
            std::time::Duration::from_millis(1)
        };
        let wake = inner.observer_wake();
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = wake.notified() => {}
        }
    }
}
