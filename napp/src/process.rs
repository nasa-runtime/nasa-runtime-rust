use std::{
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    process::ExitCode,
};

use tokio::runtime::{Builder, Runtime};

use crate::{
    panic_hook::install_process_panic_hook,
    preflight::Preflight,
    report::{report_preflight, report_runtime, report_shutdown},
    Application, ApplicationComponent, ApplicationError, ApplicationPhase, ApplicationRunner,
    ApplicationSpec, ComponentId,
};

#[cfg(feature = "web")]
use crate::web::WebComponent;

/// 从必需本地配置完成同步预检、创建 owned runtime，并执行完整应用生命周期。
///
/// 该函数是进程入口的唯一所有权边界：正常返回和 Runner unwind 都会消费 runtime 并调用零等待退场，避免普通析构无限等待残留 blocking work。
///
/// # 参数
///
/// - `spec`：属性入口生成的静态应用名和组件声明。
/// - `user_hook`：完成业务资源装配并返回受监督 future 的启动闭包。
pub fn run<F, Fut, E>(spec: ApplicationSpec, user_hook: F) -> ExitCode
where
    F: FnOnce(Application) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<anyhow::Error> + 'static,
{
    // hook 必须早于配置读取和 worker 创建，默认 hook 才没有机会先输出未脱敏 payload。
    install_process_panic_hook();

    // 未声明 log 组件时装最小 fmt subscriber，让 `#[application("web")]` 仍能看到
    // "listening on ..." 与 `[mvc] 收集到路由` 清单；log 组件存在时由其两阶段
    // 日志统一接管，这里不安装以免抢占全局 subscriber。
    if !spec.components().contains(&ComponentId::Log) {
        install_fallback_subscriber();
    }

    let preflight = match Preflight::load_standard(&spec) {
        Ok(preflight) => preflight,
        Err(error) => {
            report_preflight(&error);
            return ExitCode::FAILURE;
        }
    };
    // 预读到的整个 `application.*` 在这里被固定下来；组件在合并远端 overlay 后按它做 bootstrap-only 判定。
    let pinned_application = preflight.pinned_application.clone();

    let runtime = match build_runtime(preflight.worker_threads) {
        Ok(runtime) => runtime,
        Err(error) => {
            report_preflight(&error);
            return ExitCode::FAILURE;
        }
    };
    let mut runner = ApplicationRunner::new(preflight.info, preflight.initial_config)
        .startup_timeout(preflight.startup_timeout)
        .shutdown_timeout(preflight.shutdown_timeout)
        .with_process_signals();
    // 逐个声明组件构造运行对象；声明了却未编译/未支持的组件必须 fail-fast，
    // 不允许“通过宏校验但运行期静默无视”，避免装配配置在运行期失效。
    for component in spec.components() {
        let component = match build_component(*component, &spec, pinned_application.as_ref()) {
            Ok(component) => component,
            Err(error) => {
                report_preflight(&error);
                runtime.shutdown_background();
                return ExitCode::FAILURE;
            }
        };
        runner = runner.with_component(component);
    }

    // catch 位于 runtime 所有权作用域内；无论 future 正常返回还是 unwind，下一行都会消费 runtime。
    let outcome = catch_unwind(AssertUnwindSafe(|| runtime.block_on(runner.run(user_hook))));
    runtime.shutdown_background();

    match outcome {
        Ok(Ok(exit)) => {
            for failure in exit.shutdown_failures() {
                report_shutdown(failure);
            }
            ExitCode::from(exit.code())
        }
        Ok(Err(error)) => {
            if !error.was_reported() {
                report_runtime(&error);
            }
            ExitCode::FAILURE
        }
        Err(payload) => {
            // payload 不参与格式化，且主动遗忘可避免其异常析构在应急出口再次触发 panic。
            std::mem::forget(payload);
            ExitCode::FAILURE
        }
    }
}

/// 业务作用：把声明组件构造成运行对象，确保声明的能力真实生效或在副作用前明确失败。
///
/// 该分发是"声明即生效或立即报错"的唯一入口：既不静默无视声明，也不为未启用 feature 的
/// 组件伪造行为。每个组件在其对应 feature 关闭时给出指向 nasa feature 的定向错误。
///
/// 参数说明：
/// - `id`：宏按声明顺序生成、已通过白名单校验的组件身份。
/// - `spec`：提供 Web 工厂等运行期绑定的静态应用描述。
/// - `pinned_application`：同步预读到的原始 `application.*`，供配置中心组件做 bootstrap-only 判定。
///
/// 返回：能力已编入时返回对应组件；内部身份或缺失能力返回定向启动错误。
fn build_component(
    id: ComponentId,
    spec: &ApplicationSpec,
    pinned_application: Option<&serde_json::Value>,
) -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    match id {
        ComponentId::Web => build_web_component(spec),
        ComponentId::Log => build_log_component(),
        ComponentId::NacosConfig => build_nacos_config_component(spec, pinned_application),
        ComponentId::Db => build_db_component(),
        ComponentId::Redis => build_redis_component(),
        ComponentId::Telemetry => build_telemetry_component(),
        ComponentId::Cache => build_cache_component(),
        ComponentId::Saga => build_saga_component(),
        ComponentId::Kafka => build_kafka_component(),
        ComponentId::Outbox => build_outbox_component(),
        ComponentId::Auth => build_auth_component(),
        ComponentId::Ws => build_ws_component(),
        ComponentId::Scheduling => build_scheduling_component(),
        ComponentId::NacosDiscovery => build_nacos_discovery_component(),
        ComponentId::Application
        | ComponentId::Config
        | ComponentId::Resources
        | ComponentId::Supervisor
        | ComponentId::UserHook => Err(non_declarable_component_error(id)),
    }
}

/// 构造 Web 组件；运行能力关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// - `spec`：提供业务二进制生成的路由元数据和路由工厂。
#[cfg_attr(not(feature = "web"), allow(unused_variables))]
fn build_web_component(
    spec: &ApplicationSpec,
) -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "web")]
    {
        Ok(Box::new(WebComponent::from_spec(spec)?))
    }
    #[cfg(not(feature = "web"))]
    {
        Err(feature_missing_error(ComponentId::Web, "web"))
    }
}

/// 构造缓存组件；`cache` feature 关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入缓存组件由编译期 `cache` feature 决定。
fn build_cache_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "cache")]
    {
        Ok(Box::new(crate::cache::CacheComponent::new()))
    }
    #[cfg(not(feature = "cache"))]
    {
        Err(feature_missing_error(ComponentId::Cache, "cache"))
    }
}

/// 构造遥测组件；`telemetry` feature 关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入遥测组件由编译期 `telemetry` feature 决定。
fn build_telemetry_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "telemetry")]
    {
        Ok(Box::new(crate::telemetry::TelemetryComponent::new()))
    }
    #[cfg(not(feature = "telemetry"))]
    {
        Err(feature_missing_error(ComponentId::Telemetry, "telemetry"))
    }
}

/// 构造认证组件；auth 能力随 Web 编入,`web` feature 关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入认证组件由编译期 `web` feature 决定。
fn build_auth_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "web")]
    {
        Ok(Box::new(crate::auth::AuthComponent::new()))
    }
    #[cfg(not(feature = "web"))]
    {
        Err(feature_missing_error(ComponentId::Auth, "web"))
    }
}

/// 构造 log 组件；`log` feature 关闭时给出指向 nasa feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入 log 组件由编译期 feature 决定。
fn build_log_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "log")]
    {
        Ok(Box::new(crate::log::LogComponent::new()))
    }
    #[cfg(not(feature = "log"))]
    {
        Err(feature_missing_error(ComponentId::Log, "log"))
    }
}

/// 构造配置中心组件；运行能力关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// - `spec`：提供声明组件列表，供热刷新构造同版本 reload 状态表。
/// - `pinned_application`：同步预读到的原始 `application.*`，作为 bootstrap-only 冲突判定基准。
#[cfg_attr(not(feature = "nacos-config"), allow(unused_variables))]
fn build_nacos_config_component(
    spec: &ApplicationSpec,
    pinned_application: Option<&serde_json::Value>,
) -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "nacos-config")]
    {
        Ok(Box::new(crate::nacos_config::NacosConfigComponent::new(
            spec.components().to_vec(),
            pinned_application.cloned(),
        )))
    }
    #[cfg(not(feature = "nacos-config"))]
    {
        // 业务侧看到的是 nasa 门面 feature 名；napp 的组件 feature 由门面弱依赖桥打开。
        Err(feature_missing_error(
            ComponentId::NacosConfig,
            "nacos-config",
        ))
    }
}

/// 构造 db 组件；napp 的 `db` feature 关闭时给出指向 nasa feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入数据源组件由编译期 feature 决定。
fn build_db_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "db")]
    {
        Ok(Box::new(crate::db::DbComponent::new()))
    }
    #[cfg(not(feature = "db"))]
    {
        Err(feature_missing_error(ComponentId::Db, "tx"))
    }
}

/// 业务作用：构造 Saga 生命周期组件；能力未编入时返回指向门面 feature 的定向错误。
///
/// 参数说明: 无。
///
/// 返回：启用 `saga-runtime` 时返回受管组件，否则返回启动配置错误。
fn build_saga_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "saga")]
    {
        Ok(Box::new(crate::saga::SagaComponent::new()))
    }
    #[cfg(not(feature = "saga"))]
    {
        Err(feature_missing_error(ComponentId::Saga, "saga-runtime"))
    }
}

/// 业务作用：构造受管 Outbox dispatcher；能力未编入时拒绝把声明静默降级成手工轮询。
///
/// 参数说明: 无。
///
/// 返回：启用 `outbox` 时返回生命周期组件，否则返回指向门面 feature 的启动错误。
fn build_outbox_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "outbox")]
    {
        Ok(Box::new(crate::outbox::OutboxComponent::new()))
    }
    #[cfg(not(feature = "outbox"))]
    {
        Err(feature_missing_error(ComponentId::Outbox, "outbox"))
    }
}

/// 构造 redis 组件；napp 的 `redis` feature 关闭时给出指向 nasa feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入 Redis 组件由编译期 feature 决定。
fn build_redis_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "redis")]
    {
        Ok(Box::new(crate::redis::RedisComponent::new()))
    }
    #[cfg(not(feature = "redis"))]
    {
        Err(feature_missing_error(ComponentId::Redis, "redis"))
    }
}

/// 构造 Kafka 组件；napp 的 `kafka` feature 关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入 Kafka 生命周期组件由编译期 feature 决定。
fn build_kafka_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "kafka")]
    {
        Ok(Box::new(crate::kafka::KafkaComponent::new()))
    }
    #[cfg(not(feature = "kafka"))]
    {
        Err(feature_missing_error(ComponentId::Kafka, "kafka"))
    }
}

/// 构造 scheduling 组件；napp 的 `scheduling` feature 关闭时给出指向 nasa feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入调度组件由编译期 feature 决定。
fn build_scheduling_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "scheduling")]
    {
        Ok(Box::new(crate::scheduling::SchedulingComponent::new()))
    }
    #[cfg(not(feature = "scheduling"))]
    {
        Err(feature_missing_error(ComponentId::Scheduling, "scheduling"))
    }
}

/// 构造服务发现组件；运行能力关闭时给出指向门面 feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入服务发现组件由编译期 feature 决定。
fn build_nacos_discovery_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "nacos-discovery")]
    {
        Ok(Box::new(crate::discovery::NacosDiscoveryComponent::new()))
    }
    #[cfg(not(feature = "nacos-discovery"))]
    {
        Err(feature_missing_error(
            ComponentId::NacosDiscovery,
            "nacos-discovery",
        ))
    }
}

/// 构造 ws 组件；napp 的 `ws` feature 关闭时给出指向 nasa feature 的定向错误。
///
/// # 参数
///
/// 本函数无参数；是否编入长连接组件由编译期 feature 决定。
fn build_ws_component() -> Result<Box<dyn ApplicationComponent>, ApplicationError> {
    #[cfg(feature = "ws")]
    {
        Ok(Box::new(crate::ws::WsComponent::new()))
    }
    #[cfg(not(feature = "ws"))]
    {
        Err(feature_missing_error(ComponentId::Ws, "ws"))
    }
}

/// 构造不可由属性宏声明的内部组件身份时返回的稳定错误。
///
/// # 参数
///
/// - `id`：只属于运行时内部、不应出现在组件声明中的身份。
fn non_declarable_component_error(id: ComponentId) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Application,
        ApplicationPhase::Bootstrap,
        format!("`{id}` is an internal runtime identity and cannot be declared as a component"),
    )
}

/// 构造组件被声明但对应 nasa feature 未启用时的定向错误。
///
/// # 参数
///
/// - `id`：被声明的组件身份。
/// - `feature`：需要在 nasa 依赖中启用的 feature 名称。
// 组件 feature 全开时没有任何分支会调用它；这不是缺陷，而是"每个组件都已编入"的正常结果。
#[allow(dead_code)]
fn feature_missing_error(id: ComponentId, feature: &str) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Application,
        ApplicationPhase::Bootstrap,
        format!(
            "application component `{id}` requested, but nasa feature `{feature}` is disabled; \
             add `{feature}` to nasa.features in Cargo.toml"
        ),
    )
}

/// 在没有 log 组件时安装最小 fmt 日志 subscriber。
///
/// 使用 `try_init`：若业务侧已安装全局 subscriber，`AlreadySet` 被忽略而不是当作启动
/// 失败。默认级别为 `info`，`RUST_LOG` 存在时按其过滤。
///
/// # 参数
///
/// 本函数无参数；调用后全局默认 subscriber 持续生效到进程退出。
fn install_fallback_subscriber() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// 创建由同步入口独占的多线程 runtime。
///
/// # 参数
///
/// - `worker_threads`：本地配置显式指定的 worker 数；`None` 保留运行库默认策略。
fn build_runtime(worker_threads: Option<usize>) -> Result<Runtime, ApplicationError> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    if let Some(worker_threads) = worker_threads {
        builder.worker_threads(worker_threads);
    }
    builder.build().map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Application,
            ApplicationPhase::Bootstrap,
            "cannot build application async runtime",
            error,
        )
    })
}
