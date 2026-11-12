use std::str::FromStr;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

/// 进程级日志写入守卫，首次初始化后持续保留，避免非阻塞写入线程提前结束。
#[allow(dead_code)]
pub static LOG_GUARD: std::sync::OnceLock<Vec<WorkerGuard>> = std::sync::OnceLock::new();

/// 业务作用：按首次调用参数初始化 SDK 自带的日志输出，并持有异步写入守卫。
///
/// 参数说明：
/// - `log_path`：日滚动日志文件所在目录，首次初始化后不再切换。
/// - `log_level`：普通日志层的级别文本，无法解析时使用 INFO。
///
/// 返回：首次调用完成初始化，后续调用保持既有守卫；已有全局 subscriber 时记录失败而不替换，文件输出创建失败可能产生 panic。
#[allow(dead_code)]
pub fn init(log_path: String, log_level: String) {
    LOG_GUARD.get_or_init(|| init_logging(log_path, log_level));
}

/// 业务作用：创建普通及错误文件输出，并尝试安装受环境过滤控制的全局日志订阅器。
///
/// 参数说明：
/// - `log_path`：两个日滚动日志文件共同使用的目录。
/// - `log_level`：普通文件及开发构建控制台的级别，非法文本回退 INFO；环境过滤仍可进一步限制输出。
///
/// 返回：维持非阻塞写入器存活的守卫集合；安装全局订阅器失败也返回守卫，不替换宿主日志系统，文件创建失败可能产生 panic。
#[allow(dead_code)]
fn init_logging(log_path: String, log_level: String) -> Vec<WorkerGuard> {
    let log_level = LevelFilter::from_str(&log_level).unwrap_or(LevelFilter::INFO);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    // 各输出层与写入守卫配对保存，避免函数返回时提前终止后台文件写入。
    let mut layers = Vec::with_capacity(3);
    let mut guards = Vec::with_capacity(3);

    if cfg!(debug_assertions) {
        // 仅启用调试断言的构建增加控制台层，常规发行构建使用文件输出。
        let (console_writer, console_guard) = tracing_appender::non_blocking(std::io::stdout());
        let console_layer = fmt::layer()
            .with_writer(console_writer)
            .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
            .with_ansi(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_filter(log_level);
        layers.push(console_layer);
        guards.push(console_guard);
    }

    // 创建按日滚动的文件日志记录器 1
    let file_appender1 =
        RollingFileAppender::new(Rotation::DAILY, log_path.clone(), "nacos-common.log");
    let (non_blocking1, file_guard1) = tracing_appender::non_blocking(file_appender1);
    let common_file_layer = fmt::layer()
        .with_writer(non_blocking1)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_ansi(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_filter(log_level);
    layers.push(common_file_layer);
    guards.push(file_guard1);

    // 创建按日滚动的文件日志记录器 2
    let file_appender2 = RollingFileAppender::new(Rotation::DAILY, log_path, "nacos-error.log");
    let (non_blocking2, file_guard2) = tracing_appender::non_blocking(file_appender2);
    let err_file_layer = fmt::layer()
        .with_writer(non_blocking2)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_ansi(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_filter(LevelFilter::ERROR);
    layers.push(err_file_layer);
    guards.push(file_guard2);

    let subscriber = tracing_subscriber::registry().with(env_filter).with(layers);

    let _result = subscriber.try_init();
    if _result.is_err() {
        tracing::error!("Failed to set nacos-sdk subscriber, please config and set it.");
    }

    guards
}
