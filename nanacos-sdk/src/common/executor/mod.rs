use crate::api::error::Result;
use futures::Future;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
    time::{Duration, interval, sleep},
};
use tracing::{Instrument, error};

static COMMON_THREAD_CORES: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var(crate::api::constants::ENV_NACOS_CLIENT_COMMON_THREAD_CORES)
        .ok()
        .and_then(|v| v.parse::<usize>().ok().filter(|n| *n > 0))
        .unwrap_or(1)
});

static RT: std::sync::LazyLock<Runtime> = std::sync::LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .thread_name("nacos-client-thread-pool")
        .worker_threads(*COMMON_THREAD_CORES)
        .build()
        .expect("Thread pool should build successfully with valid configuration")
});

pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    RT.spawn(future)
}

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
