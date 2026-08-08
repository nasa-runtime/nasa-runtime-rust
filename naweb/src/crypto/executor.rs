//! CPU 密集密码操作的有界阻塞执行器。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::MappingBuildError;

use super::{CryptoError, CryptoErrorKind, MAX_CRYPTO_RUNTIME_DURATION};

/// 阻塞密码任务的并发、排队和超时配置。
#[derive(Debug, Clone, Copy)]
pub struct CryptoCpuConfig {
    /// 同时真正运行的阻塞密码任务数量。
    pub max_concurrency: usize,
    /// 等待运行许可的最大任务数量，不含正在运行的任务。
    pub queue_capacity: usize,
    /// 等待进入阻塞执行阶段的最长时间。
    pub queue_timeout: Duration,
    /// 调用方等待单次阻塞任务结果的最长时间。
    pub operation_timeout: Duration,
}

impl Default for CryptoCpuConfig {
    /// 业务作用：建立适合普通服务进程的保守默认配置。
    ///
    /// # 返回
    ///
    /// 返回 8 个并发、64 个排队项和短超时配置。
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            queue_capacity: 64,
            queue_timeout: Duration::from_millis(20),
            operation_timeout: Duration::from_secs(1),
        }
    }
}

/// 把同步密码函数隔离到有界 blocking 任务中的执行器。
#[derive(Clone)]
pub struct CryptoCpuExecutor {
    concurrency: Arc<Semaphore>,
    queued: Arc<Semaphore>,
    config: CryptoCpuConfig,
}

impl std::fmt::Debug for CryptoCpuExecutor {
    /// 业务作用：输出容量和剩余 permit，不输出任务输入或结果。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CryptoCpuExecutor")
            .field("config", &self.config)
            .field("running_slots", &self.concurrency.available_permits())
            .field("queue_slots", &self.queued.available_permits())
            .finish_non_exhaustive()
    }
}

impl CryptoCpuExecutor {
    /// 业务作用：建立有界阻塞执行器。
    ///
    /// # 参数
    ///
    /// - `config`: 启动期验证的并发、队列与超时配置。
    ///
    /// # 返回
    ///
    /// 配置合法时返回执行器；零容量或零超时会拒绝启动。
    pub fn new(config: CryptoCpuConfig) -> Result<Self, MappingBuildError> {
        if config.max_concurrency == 0
            || config.queue_capacity == 0
            || config.queue_timeout.is_zero()
            || config.operation_timeout.is_zero()
            || config.queue_timeout > MAX_CRYPTO_RUNTIME_DURATION
            || config.operation_timeout > MAX_CRYPTO_RUNTIME_DURATION
        {
            return Err(MappingBuildError::new(
                "crypto CPU executor 配置含零值或超长时限",
            ));
        }
        if config.max_concurrency > Semaphore::MAX_PERMITS
            || config.queue_capacity > Semaphore::MAX_PERMITS
        {
            return Err(MappingBuildError::new(
                "crypto CPU executor 容量超过 Tokio semaphore 上限",
            ));
        }
        Ok(Self {
            concurrency: Arc::new(Semaphore::new(config.max_concurrency)),
            queued: Arc::new(Semaphore::new(config.queue_capacity)),
            config,
        })
    }

    /// 业务作用：在有界 blocking 任务中执行一个拥有全部输入的同步闭包。
    ///
    /// # 类型参数
    ///
    /// - `T`: 任务成功返回且可跨线程移动的非借用结果。
    /// - `F`: 拥有所有敏感输入的同步闭包；闭包必须先由调用方限制输入大小。
    ///
    /// # 参数
    ///
    /// - `operation`: 不访问请求借用的同步密码操作，内部错误需已收敛为 [`CryptoError`]。
    ///
    /// # 返回
    ///
    /// 正常完成返回结果；排队、执行超时或 join 故障均 fail closed。执行超时不会强制终止已经
    /// 启动的 blocking 闭包，因此运行 permit 由闭包持有到真正结束，避免取消造成无界孤儿任务。
    pub async fn execute<T, F>(&self, operation: F) -> Result<T, CryptoError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, CryptoError> + Send + 'static,
    {
        let queue_permit = self
            .queued
            .clone()
            .try_acquire_owned()
            .map_err(|_| CryptoError::new(CryptoErrorKind::Overloaded, "cpu-queue-full"))?;
        let run_permit = timeout(
            self.config.queue_timeout,
            self.concurrency.clone().acquire_owned(),
        )
        .await
        .map_err(|_| CryptoError::new(CryptoErrorKind::Overloaded, "cpu-queue-timeout"))?
        .map_err(|_| CryptoError::new(CryptoErrorKind::Internal, "cpu-executor-closed"))?;
        drop(queue_permit);

        let task = tokio::task::spawn_blocking(move || {
            // permit 必须跟随真实阻塞任务，而不是跟随调用方 future；客户端取消也不会提前放大并发。
            let _run_permit = run_permit;
            operation()
        });
        timeout(self.config.operation_timeout, task)
            .await
            .map_err(|_| CryptoError::new(CryptoErrorKind::Timeout, "cpu-operation-timeout"))?
            .map_err(|_| CryptoError::new(CryptoErrorKind::Internal, "cpu-operation-join"))?
    }
}
