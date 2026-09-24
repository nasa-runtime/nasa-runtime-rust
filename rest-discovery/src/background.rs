//! REST 运行时的永久关闭门禁与任务退出证明。

use std::future::Future;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tokio_util::task::TaskTracker;

use crate::error::{RestDiscoveryError, Result};

pub(crate) struct BackgroundOwner {
    closed: Mutex<bool>,
    cancel: CancellationToken,
    tasks: TaskTracker,
}

impl BackgroundOwner {
    /// 业务作用：建立一个实例独立的 REST 准入和退出 owner。
    /// 参数说明：无。
    /// 返回：未关闭、没有后台任务的新 owner。
    pub(crate) fn new() -> Self {
        Self {
            closed: Mutex::new(false),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    /// 业务作用：将短同步登记与运行时关闭按同一个门禁串行裁决。
    /// 参数说明：`install` 为不含异步 I/O 的登记动作。
    /// 返回：尚开放时返回登记结果；永久关闭后拒绝。
    pub(crate) fn with_open<T>(&self, install: impl FnOnce() -> Result<T>) -> Result<T> {
        let closed = self
            .closed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *closed {
            return Err(RestDiscoveryError::RuntimeClosed);
        }
        install()
    }

    /// 业务作用：登记已接纳调用，停机等待将包含调用 future 的实际退出。
    /// 参数说明：无。
    /// 返回：调用完成或被丢弃时归还的登记；关闭后拒绝。
    pub(crate) fn enter(&self) -> Result<TaskTrackerToken> {
        self.with_open(|| Ok(self.tasks.token()))
    }

    /// 业务作用：在登记责任后启动受当前运行时取消约束的后台任务。
    /// 参数说明：`future` 为订阅或索引循环。
    /// 返回：供单服务回收使用的中止句柄；关闭后不启动任务。
    pub(crate) fn spawn<F>(&self, future: F) -> Result<tokio::task::AbortHandle>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.with_open(|| {
            let cancel = self.cancel.clone();
            let task = self.tasks.spawn(async move {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {},
                    _ = future => {},
                }
            });
            Ok(task.abort_handle())
        })
    }

    /// 业务作用：使在途发现和出站等待响应运行时关闭。
    /// 参数说明：`operation` 为已登记到 owner 的异步操作。
    /// 返回：关闭时停止本地等待，不声明远端操作未执行。
    pub(crate) async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(RestDiscoveryError::RuntimeClosed),
            result = operation => result,
        }
    }

    /// 业务作用：永久关闭新任务和调用准入，并通知所有已登记任务退出。
    /// 参数说明：无。
    /// 返回：幂等地发出关闭信号；实际完成由 wait 确认。
    pub(crate) fn close(&self) {
        let mut closed = self
            .closed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // 先关闭登记，再取消任务，确保晚到的 watch 无法逃出退出集合。
        *closed = true;
        self.tasks.close();
        self.cancel.cancel();
    }

    /// 业务作用：等待当前实例已登记任务和调用真正释放其守卫。
    /// 参数说明：`deadline` 为宿主共享的绝对停机截止点。
    /// 返回：全部退出时成功；超过截止点报告尚未排干，owner 仍保持关闭。
    pub(crate) async fn wait(&self, deadline: tokio::time::Instant) -> Result<()> {
        tokio::time::timeout_at(deadline, self.tasks.wait())
            .await
            .map_err(|_| RestDiscoveryError::ShutdownIncomplete)
    }
}
