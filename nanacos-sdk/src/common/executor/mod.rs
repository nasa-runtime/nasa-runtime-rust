use crate::api::error::Result;
use futures::Future;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
    time::{Duration, interval, sleep},
};
use tracing::{Instrument, error};

/// 首次使用时冻结的公共工作线程数，非法或非正环境值回退为一。
static COMMON_THREAD_CORES: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var(crate::api::constants::ENV_NACOS_CLIENT_COMMON_THREAD_CORES)
        .ok()
        .and_then(|v| v.parse::<usize>().ok().filter(|n| *n > 0))
        .unwrap_or(1)
});

/// SDK 共享的进程级 Tokio 运行时，负责网络、定时和后台任务执行。
static RT: std::sync::LazyLock<Runtime> = std::sync::LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .thread_name("nacos-client-thread-pool")
        .worker_threads(*COMMON_THREAD_CORES)
        .build()
        .expect("Thread pool should build successfully with valid configuration")
});

/// 业务作用：把 SDK 后台任务提交到进程内共享运行时，使其不依赖调用方当前执行器。
///
/// 参数说明：
/// - `future`：拥有所需捕获值的异步任务，输出也必须可跨线程传递。
///
/// 返回：可等待或中止任务的句柄；丢弃句柄不会自动取消任务，共享运行时首次建立失败时产生 panic。
pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    RT.spawn(future)
}

/// 业务作用：延迟启动一次异步动作，供 SDK 安排不需要固定周期的后台工作。
///
/// 参数说明：
/// - `future`：延迟结束后执行的拥有所有权的任务。
/// - `delay`：任务被运行时轮询后开始计时的等待时长，不保证精确到时调度。
///
/// 返回：覆盖等待和动作执行的任务句柄；中止会取消尚未完成的等待或动作，丢弃句柄不会取消。
#[allow(dead_code)]
pub(crate) fn schedule<F>(future: F, delay: Duration) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    RT.spawn(async move {
        sleep(delay).await;
        future.await
    })
}

/// 业务作用：串行重复执行同步任务，每次成功结束后再等待间隔，避免重叠调用。
///
/// 参数说明：
/// - `task`：每轮执行的同步操作；不得长时间阻塞共享运行时线程。
/// - `duration`：每轮成功之后的休眠时长，不包含本轮执行时间。
///
/// 返回：周期任务句柄；首次立即执行，任一轮返回错误时记录并结束，panic 由任务句柄呈现。
#[allow(dead_code)]
pub(crate) fn schedule_at_fixed_rate(
    task: impl Fn() -> Result<()> + Send + Sync + 'static,
    duration: Duration,
) -> JoinHandle<()> {
    RT.spawn(
        async move {
            loop {
                let ret = async { task() }.await;
                if let Err(e) = ret {
                    error!("schedule_at_fixed_rate occur an error: {e}");
                    break;
                }
                sleep(duration).await;
            }
        }
        .in_current_span(),
    )
}

/// 业务作用：按 Tokio interval 的节拍串行执行同步任务，沿用其默认错过节拍处理方式。
///
/// 参数说明：
/// - `task`：每个到期节拍执行的同步操作，不能假定运行在线程隔离的阻塞池。
/// - `duration`：固定节拍间隔，必须非零；耗时超过间隔时可能连续补执行。
///
/// 返回：周期任务句柄；首个节拍立即到达，业务错误记录后结束，零间隔或任务 panic 使后台任务失败。
#[allow(dead_code)]
pub(crate) fn schedule_at_fixed_delay(
    task: impl Fn() -> Result<()> + Send + Sync + 'static,
    duration: Duration,
) -> JoinHandle<()> {
    RT.spawn(
        async move {
            let mut interval = interval(duration);
            loop {
                interval.tick().await;
                let ret = async { task() }.await;
                if let Err(e) = ret {
                    error!("schedule_at_fixed_delay occur an error: {e}");
                    break;
                }
            }
        }
        .in_current_span(),
    )
}
