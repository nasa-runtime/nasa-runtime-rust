use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nadis::{RedisClient, RedisConfig};

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ShutdownAction,
    ShutdownContext, StartContext,
};

/// Redis 健康 monitor 的固定 PING 间隔。
const REDIS_MONITOR_INTERVAL: Duration = Duration::from_secs(5);
const MAX_MANAGED_REDIS_SOURCES: usize = 64;
pub(crate) const MAX_MANAGED_REDIS_NAME_BYTES: usize = 256;
const IDEMPOTENT_METRIC_SERIES_PER_SOURCE: usize = 11;
const IDEMPOTENT_METRIC_SERIES_BUDGET: usize = 10_000;

macro_rules! idempotent_metric_descriptor {
    ($ident:ident, $name:literal, $kind:ident, $labels:expr) => {
        static $ident: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
            name: $name,
            help: concat!("Redis nonce 幂等计数 ", $name, " 的受管运行快照。"),
            unit: "",
            kind: nametrics_core::MetricKind::$kind,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}

idempotent_metric_descriptor!(
    IDEMPOTENT_APPLIED,
    "redis_idempotent_counter_applied_total",
    Counter,
    &["qualifier"]
);
idempotent_metric_descriptor!(
    IDEMPOTENT_DUPLICATE,
    "redis_idempotent_counter_duplicate_total",
    Counter,
    &["qualifier"]
);
idempotent_metric_descriptor!(
    IDEMPOTENT_REJECTED,
    "redis_idempotent_counter_rejected_total",
    Counter,
    &["qualifier", "reason"]
);
idempotent_metric_descriptor!(
    IDEMPOTENT_TTL_MISSING,
    "redis_idempotent_counter_ttl_missing_total",
    Counter,
    &["qualifier"]
);
idempotent_metric_descriptor!(
    IDEMPOTENT_PROBE_FAILURES,
    "redis_idempotent_counter_probe_failures_total",
    Counter,
    &["qualifier"]
);
idempotent_metric_descriptor!(
    IDEMPOTENT_RESOLVED_MODE,
    "redis_idempotent_counter_resolved_ttl_mode",
    Gauge,
    &["qualifier", "mode"]
);
idempotent_metric_descriptor!(
    IDEMPOTENT_LAYOUT_MARKER,
    "redis_idempotent_counter_layout_marker",
    Gauge,
    &["qualifier"]
);

static IDEMPOTENT_METRIC_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 7] = [
    &IDEMPOTENT_APPLIED,
    &IDEMPOTENT_DUPLICATE,
    &IDEMPOTENT_REJECTED,
    &IDEMPOTENT_TTL_MISSING,
    &IDEMPOTENT_PROBE_FAILURES,
    &IDEMPOTENT_RESOLVED_MODE,
    &IDEMPOTENT_LAYOUT_MARKER,
];

/// 资源容器沿用的默认 Redis 本地别名；持久协议中的 canonical qualifier 始终是 `primary`。
pub(crate) const DEFAULT_REDIS: &str = "default";

/// Redis 组件：用统一客户端建连，并把 `Arc<RedisClient>` 交给资源容器。
///
/// 建连、配置校验、cluster/standalone 探测与协议 profile 都由 `nadis` 负责；组件只做编排与
/// 生命周期挂接。客户端不再另立注册表（nadis 自带的 `RedisRegistry` 有意不用），
/// 资源容器是唯一真理来源。
pub(crate) struct RedisComponent {
    /// Ready 后交由 Runner 按关键任务监督的 Redis 健康 monitor；未建连时为 None。
    critical_task: Option<ApplicationFuture<'static>>,
    metric_clients: Arc<std::sync::Mutex<Vec<Arc<RedisClient>>>>,
    configured_idempotent_clients: Vec<Arc<RedisClient>>,
}

impl RedisComponent {
    /// 业务作用：创建尚未建连的 Redis 组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；客户端与健康 monitor 在 Start 阶段按最终配置建立。
    pub(crate) fn new() -> Self {
        Self {
            critical_task: None,
            metric_clients: Arc::new(std::sync::Mutex::new(Vec::new())),
            configured_idempotent_clients: Vec::new(),
        }
    }
}

/// 业务作用：Redis 就绪策略:默认**非关键**——Redis 故障使实例 Degraded(仍 200)而非摘流;
/// 会话/锁强依赖 Redis 的部署可自行改为 critical。连续 3 次 PING 失败才降级,一次成功即恢复。
fn redis_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy {
        affects_ready: false,
        failure_threshold: 3,
        recovery_threshold: 1,
        stale_after: None,
    }
}

/// 业务作用：运行期 Redis 健康 monitor:进入 Ready 后按固定间隔 PING 判活并发布就绪观测。
///
/// # 参数
///
/// - `application`:读取全局生命周期状态;进入停机态时发布未就绪并优雅退出。
/// - `client`:受监督的 Redis 客户端(只读 PING,不改数据/拓扑)。
/// - `contributor`:Redis 就绪贡献句柄。
///
/// # 返回
///
/// Application 进入停机态时返回 `Ok(())`;PING 失败发布 Degraded 而非返回错误(非关键依赖)。
async fn run_redis_monitor(
    application: Application,
    clients: Vec<(Arc<RedisClient>, ReadinessContributor)>,
) -> ApplicationResult<()> {
    let mut states = application.subscribe_state();
    loop {
        let state = *states.borrow_and_update();
        match state {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                for (_, contributor) in &clients {
                    contributor.observe(
                        DependencyState::NotReady,
                        reason::NOT_READY,
                        Instant::now(),
                    );
                }
                return Ok(());
            }
            ApplicationState::Starting => {
                let _ = states.changed().await;
                continue;
            }
            ApplicationState::Ready => {
                application.redis_partitions().observe()?;
                application.redis_tasks().observe()?;
                application.redis_derived().observe()?;
            }
        }

        // 停机状态优先结束只读探测，不让周期等待占用派生任务的排干预算。
        let results = tokio::select! {
            biased;
            _ = states.changed() => continue,
            results = futures_util::future::join_all(clients.iter().map(|(client, _)| client.ping())) => results,
        };
        for ((client, contributor), result) in clients.iter().zip(results) {
            let now = Instant::now();
            match result {
                Ok(()) => {
                    let degraded = client
                        .idempotent_counter_snapshot()
                        .is_some_and(|snapshot| snapshot.degraded);
                    if degraded {
                        contributor.observe(DependencyState::Degraded, reason::DEGRADED, now);
                    } else {
                        contributor.observe(DependencyState::Ready, reason::HEALTHY, now);
                    }
                }
                // 单个 source 的探测失败只降级自身贡献项，不遮蔽其它 Redis source 的健康事实。
                Err(_) => contributor.observe(DependencyState::Degraded, reason::DEGRADED, now),
            }
        }
        tokio::select! {
            biased;
            _ = states.changed() => {},
            _ = tokio::time::sleep(REDIS_MONITOR_INTERVAL) => {},
        }
    }
}

impl ApplicationComponent for RedisComponent {
    /// 业务作用：返回 Redis 组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类 Redis 错误与资源所有者。
    fn id(&self) -> ComponentId {
        ComponentId::Redis
    }

    /// 业务作用：读取最终配置的 `redis` 段并建立客户端。
    ///
    /// 建连成功后先登记资源再压栈清理动作：这样任何后续启动失败都能沿同一条逆序链显式关闭客户端，
    /// 而不是依赖最后一个 `Arc` 何时释放。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置、组件资源登记入口和 active stack 的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let configs = read_redis_configs(&application)?;
            let mut monitored = Vec::with_capacity(configs.len());
            let mut metric_clients = Vec::with_capacity(configs.len());
            for (qualifier, config) in configs {
                let idempotent_configured = config.idempotent_counter.is_some();
                let namespace = config.namespace.clone();
                let client = RedisClient::connect(config).await.map_err(|error| {
                    redis_error_src(
                        ApplicationPhase::Start,
                        format!("redis instance `{qualifier}` startup probe failed in namespace `{namespace}`"),
                        error,
                    )
                })?;
                // 建连成功即登记逆序清理；后续别名或其它 source 登记失败也能沿统一 active stack 释放。
                context.activate(Box::new(RedisShutdown {
                    client: Some(client.clone()),
                }));
                context.register_resource(Some(&qualifier), client.clone())?;
                if qualifier == "primary" {
                    // `default` 只存在于资源查询边界，client 与持久 Job/nonce 身份仍报告 `primary`。
                    context.register_resource(Some(DEFAULT_REDIS), client.clone())?;
                }

                let contributor = application.register_readiness(
                    ComponentId::Redis,
                    Arc::<str>::from(format!("redis:{qualifier}")),
                    redis_readiness_policy(),
                )?;
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                if idempotent_configured {
                    self.configured_idempotent_clients.push(client.clone());
                }
                metric_clients.push(client.clone());
                monitored.push((client, contributor));
            }
            *self
                .metric_clients
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = metric_clients;
            self.critical_task = Some(Box::pin(run_redis_monitor(application, monitored)));
            install_snowflakes(context).await?;
            #[cfg(any(feature = "mapper-cache", feature = "mapper-cache-pgsql"))]
            crate::mapper_cache::install_managed(context).await?;
            Ok(())
        })
    }

    /// 业务作用：为全部受管 Redis source 预留固定幂等指标，并只为显式配置的 source 完成账本布局门禁。
    ///
    /// 参数说明：`context` 提供统一指标目录；客户端已由 Start 建立并登记。
    ///
    /// 返回：容量与 descriptor 无冲突且全部显式账本完成 marker/能力复验时成功；否则拒绝进入 Ready。
    fn prepare<'a>(&'a mut self, context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let source_count = self
                .metric_clients
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            let worst_case_series = source_count
                .checked_mul(IDEMPOTENT_METRIC_SERIES_PER_SOURCE)
                .ok_or_else(|| {
                    redis_error(
                        ApplicationPhase::Prepare,
                        "idempotent metric series calculation overflowed",
                    )
                })?;
            if worst_case_series > IDEMPOTENT_METRIC_SERIES_BUDGET {
                return Err(redis_error(
                    ApplicationPhase::Prepare,
                    "idempotent metric series budget is exceeded",
                ));
            }
            context
                .application()
                .metrics_hub()
                .register_legacy_source_reserved(
                    Arc::new(IdempotentMetricsSource {
                        clients: self.metric_clients.clone(),
                    }),
                    worst_case_series,
                )
                .map_err(|error| match error {
                    nametrics_core::MetricSourceRegistrationError::Conflict(conflict) => {
                        redis_error(
                            ApplicationPhase::Prepare,
                            format!(
                                "idempotent metric descriptor `{}` conflicts with an existing registration",
                                conflict.name
                            ),
                        )
                    }
                    nametrics_core::MetricSourceRegistrationError::SeriesBudgetExceeded => {
                        redis_error(
                            ApplicationPhase::Prepare,
                            "idempotent metric series reservation exceeds the process budget",
                        )
                    }
                })?;

            // 显式配置意味着部署要求在接流量前证明共享布局；未配置 source 仍保持首次业务调用时惰性解析。
            let results = futures_util::future::join_all(
                self.configured_idempotent_clients
                    .iter()
                    .map(|client| client.prepare_idempotent_counter()),
            )
            .await;
            for (client, result) in self.configured_idempotent_clients.iter().zip(results) {
                result.map_err(|error| {
                    redis_error_src(
                        ApplicationPhase::Prepare,
                        format!(
                            "idempotent counter preparation failed for Redis source `{}`",
                            client.qualifier()
                        ),
                        error,
                    )
                })?;
            }
            context
                .application()
                .redis_partitions()
                .prepare(context)
                .await?;
            context.application().redis_tasks().prepare(context).await?;
            context
                .application()
                .redis_derived()
                .prepare(context)
                .await?;
            Ok(())
        })
    }

    /// 业务作用：取出 Redis 健康 monitor,交由 Runner 按关键任务监督。
    ///
    /// # 返回
    ///
    /// 建连成功后首次调用返回 monitor 任务;未建连或重复调用返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("redis-health-monitor", task))
    }
}

/// 受管 Redis source 的 nonce 幂等计数指标桥；快照只读取进程内原子量，不访问 Redis。
struct IdempotentMetricsSource {
    clients: Arc<std::sync::Mutex<Vec<Arc<RedisClient>>>>,
}

impl nametrics_core::LegacyMetricsSource for IdempotentMetricsSource {
    /// 业务作用：返回幂等计数的固定 family 目录，供启动期冲突与容量审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含冻结 qualifier、拒绝原因和 TTL 模式维度的 descriptor。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &IDEMPOTENT_METRIC_DESCRIPTORS
    }

    /// 业务作用：把全部受管 Redis source 的本地幂等事实投影为固定 series。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：每个 source 恒定十一条样本；未使用幂等 API 时保持全零且不初始化账本运行时。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut samples = Vec::with_capacity(
            clients
                .len()
                .saturating_mul(IDEMPOTENT_METRIC_SERIES_PER_SOURCE),
        );
        for client in clients {
            let qualifier = client.qualifier().to_owned();
            let snapshot = client.idempotent_counter_snapshot();
            samples.push(idempotent_counter_sample(
                &IDEMPOTENT_APPLIED,
                vec![("qualifier", qualifier.clone())],
                snapshot.as_ref().map_or(0, |value| value.applied_total),
            ));
            samples.push(idempotent_counter_sample(
                &IDEMPOTENT_DUPLICATE,
                vec![("qualifier", qualifier.clone())],
                snapshot.as_ref().map_or(0, |value| value.duplicate_total),
            ));
            for (rejection, total) in [
                (
                    "ledger_type",
                    snapshot
                        .as_ref()
                        .map_or(0, |value| value.rejected_ledger_type_total),
                ),
                (
                    "operation",
                    snapshot
                        .as_ref()
                        .map_or(0, |value| value.rejected_operation_total),
                ),
                (
                    "command",
                    snapshot
                        .as_ref()
                        .map_or(0, |value| value.rejected_command_total),
                ),
            ] {
                samples.push(idempotent_counter_sample(
                    &IDEMPOTENT_REJECTED,
                    vec![
                        ("qualifier", qualifier.clone()),
                        ("reason", rejection.to_owned()),
                    ],
                    total,
                ));
            }
            samples.push(idempotent_counter_sample(
                &IDEMPOTENT_TTL_MISSING,
                vec![("qualifier", qualifier.clone())],
                snapshot.as_ref().map_or(0, |value| value.ttl_missing_total),
            ));
            samples.push(idempotent_counter_sample(
                &IDEMPOTENT_PROBE_FAILURES,
                vec![("qualifier", qualifier.clone())],
                snapshot
                    .as_ref()
                    .map_or(0, |value| value.probe_failures_total),
            ));
            let resolved = snapshot.as_ref().and_then(|value| value.resolved_ttl_mode);
            for (label, selected) in [
                ("unresolved", resolved.is_none()),
                (
                    "hash_field",
                    resolved == Some(nadis::IdempotentTtlMode::HashField),
                ),
                (
                    "hash_bucket",
                    resolved == Some(nadis::IdempotentTtlMode::HashBucket),
                ),
            ] {
                samples.push(idempotent_gauge_sample(
                    &IDEMPOTENT_RESOLVED_MODE,
                    vec![("qualifier", qualifier.clone()), ("mode", label.to_owned())],
                    if selected {
                        1.0
                    } else {
                        0.0
                    },
                ));
            }
            samples.push(idempotent_gauge_sample(
                &IDEMPOTENT_LAYOUT_MARKER,
                vec![("qualifier", qualifier)],
                if snapshot
                    .as_ref()
                    .is_some_and(|value| !value.layout_marker.is_empty())
                {
                    1.0
                } else {
                    0.0
                },
            ));
        }
        Some(samples)
    }

    /// 业务作用：保留兼容 trait 入口，Prometheus 文本由统一 metrics hub 渲染结构化快照。
    ///
    /// 参数说明：`_output` 是不由本源直接写入的兼容缓冲区。
    ///
    /// 返回：无。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：建立一个 nonce 幂等 counter 样本，标签只来自冻结 source 与封闭枚举。
///
/// 参数说明：`descriptor`、`labels` 与 `value` 分别指定固定 family、低基数维度和单调累计。
///
/// 返回：统一 metrics hub 可校验并渲染的结构化样本。
fn idempotent_counter_sample(
    descriptor: &'static nametrics_core::MetricDescriptor,
    labels: Vec<(&'static str, String)>,
    value: u64,
) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name: descriptor.name,
        labels,
        value: nametrics_core::MetricValue::Counter(value),
    }
}

/// 业务作用：建立一个 nonce 幂等 gauge 样本，表达已解析模式或 marker 确认事实。
///
/// 参数说明：`descriptor`、`labels` 与 `value` 分别指定固定 family、低基数维度和当前值。
///
/// 返回：统一 metrics hub 可校验并渲染的结构化样本。
fn idempotent_gauge_sample(
    descriptor: &'static nametrics_core::MetricDescriptor,
    labels: Vec<(&'static str, String)>,
    value: f64,
) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name: descriptor.name,
        labels,
        value: nametrics_core::MetricValue::Gauge(value),
    }
}

/// 持有 Redis 客户端并在停机时显式调用其 shutdown 的可逆 action。
struct RedisShutdown {
    client: Option<Arc<RedisClient>>,
}

impl ShutdownAction for RedisShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含连接串或命名空间取值。
    fn label(&self) -> &'static str {
        "redis-client"
    }

    /// 业务作用：调用客户端的显式停机，再释放组件侧强引用。
    ///
    /// # 参数
    ///
    /// - `_context`：共享停机预算；该动作只做常数时间通知，不做网络等待。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(client) = self.client.take() {
                client.shutdown().await;
            }
            Ok(())
        })
    }
}

/// 业务作用：从最终配置读取 `redis` 段。
///
/// 段缺失是明确错误而不是默认值：`profile` 与 `namespace` 在 nadis 里无默认，静默兜底只会把
/// 配置缺失推迟成第一条命令的运行期故障。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
pub(crate) fn read_redis_configs(
    application: &Application,
) -> ApplicationResult<Vec<(String, RedisConfig)>> {
    let snapshot = application.config();
    let Some(section) = snapshot.value().get("redis") else {
        return Err(redis_error(
            ApplicationPhase::Start,
            "component `redis` is declared but the `redis` configuration section is missing",
        ));
    };
    parse_redis_configs(section, ApplicationPhase::Start)
}

/// 业务作用：在不建立连接的前提下校验候选配置树中的 `redis` 段。
///
/// 供启动期初始校验与配置热刷新使用；`profile` 必填正是在这一步暴露的。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_redis_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("redis") else {
        return Ok(());
    };
    parse_redis_configs(section, phase)?;
    Ok(())
}

/// 业务作用：解析互斥的单实例与 `properties.<qualifier>` 多实例形态，并把本地边界别名规范化为 client qualifier。
///
/// 参数说明：
/// - `section`: 最终配置树中的 `redis` 对象。
/// - `phase`: 错误所属生命周期阶段。
///
/// 返回：按 qualifier 稳定排序的非空客户端配置；形态混用、重复身份或任一配置非法时整体拒绝。
fn parse_redis_configs(
    section: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<Vec<(String, RedisConfig)>> {
    let object = section
        .as_object()
        .ok_or_else(|| redis_error(phase, "`redis` configuration section must be an object"))?;
    let mut configs = BTreeMap::new();
    if let Some(properties) = object.get("properties") {
        if object
            .keys()
            .any(|key| key != "properties" && key != "job" && key != "snowflake")
        {
            return Err(redis_error(
                phase,
                "`redis.properties` multi-instance form cannot be mixed with flat Redis fields",
            ));
        }
        let properties = properties
            .as_object()
            .ok_or_else(|| redis_error(phase, "`redis.properties` must be a non-empty object"))?;
        if properties.is_empty() {
            return Err(redis_error(phase, "`redis.properties` must not be empty"));
        }
        if properties.len() > MAX_MANAGED_REDIS_SOURCES {
            return Err(redis_error(
                phase,
                "configured Redis source count exceeds the managed limit",
            ));
        }
        for (boundary_name, value) in properties {
            let qualifier = canonical_qualifier(boundary_name);
            if !is_canonical_redis_qualifier(&qualifier) {
                return Err(redis_error(
                    phase,
                    "redis.properties contains a non-canonical qualifier",
                ));
            }
            let mut config = decode_redis_config(value.clone(), phase, &qualifier)?;
            let explicit = value.get("qualifier").is_some();
            if explicit && canonical_qualifier(&config.qualifier) != qualifier {
                return Err(redis_error(
                    phase,
                    format!(
                        "redis property key `{qualifier}` conflicts with its explicit qualifier"
                    ),
                ));
            }
            config.qualifier = qualifier.clone();
            validate_managed_config(&config, phase)?;
            if configs.insert(qualifier.clone(), config).is_some() {
                return Err(redis_error(
                    phase,
                    format!("redis qualifier `{qualifier}` is configured more than once"),
                ));
            }
        }
    } else {
        let mut flat = section.clone();
        if let Some(object) = flat.as_object_mut() {
            object.remove("job");
            object.remove("snowflake");
        }
        let mut config = decode_redis_config(flat, phase, "primary")?;
        if canonical_qualifier(&config.qualifier) != "primary" {
            return Err(redis_error(
                phase,
                "flat `redis` configuration represents only qualifier `primary` (`default` is its compatibility alias)",
            ));
        }
        config.qualifier = "primary".to_owned();
        validate_managed_config(&config, phase)?;
        configs.insert("primary".to_owned(), config);
    }
    Ok(configs.into_iter().collect())
}

/// 业务作用：统一受管 Redis source 与内置消费计划的 qualifier 字符集和长度边界。
///
/// 参数说明：`value` 是配置 map key、兼容别名或计划引用给出的 Redis 名称。
///
/// 返回：名称非空、无首尾空白、只含 ASCII 字母数字及 `.`、`_`、`-`，且长度有界时返回真。
pub(crate) fn is_canonical_redis_qualifier(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= MAX_MANAGED_REDIS_NAME_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// 业务作用：反序列化一个 RedisClient 配置并保留 phase/source 上下文，不暴露连接凭据。
///
/// 参数说明：`value`、生命周期阶段与脱敏 qualifier。
///
/// 返回：配置对象；字段类型或必填项错误时返回组件错误。
fn decode_redis_config(
    value: serde_json::Value,
    phase: ApplicationPhase,
    qualifier: &str,
) -> ApplicationResult<RedisConfig> {
    serde_json::from_value(value).map_err(|error| {
        redis_error_src(
            phase,
            format!(
                "invalid Redis configuration for `{qualifier}` (profile is required: LegacyV1 | RustV2)"
            ),
            error,
        )
    })
}

/// 业务作用：执行受管模式额外门禁，确保每个 source 都有命令 deadline 且基础布局合法。
///
/// 参数说明：`config` 为单个 source 配置，`phase` 为错误阶段。
///
/// 返回：全部门禁成立时成功；否则返回带 source 身份但不含 endpoint 凭据的错误。
fn validate_managed_config(config: &RedisConfig, phase: ApplicationPhase) -> ApplicationResult<()> {
    config.validate().map_err(|error| {
        redis_error_src(
            phase,
            format!(
                "invalid Redis configuration values for `{}`",
                config.qualifier
            ),
            error,
        )
    })?;
    if config.command.response_timeout_ms == 0 {
        return Err(redis_error(
            phase,
            format!(
                "redis source `{}` must set command.response_timeout_ms greater than zero in managed mode",
                config.qualifier
            ),
        ));
    }
    Ok(())
}

/// 业务作用：只在受管资源边界把 `default` 归一成协议默认 source `primary`。
///
/// 参数说明：`value` 为 properties map key 或引用该来源的 qualifier。
///
/// 返回：canonical qualifier 文本。
pub(crate) fn canonical_qualifier(value: &str) -> String {
    if value == DEFAULT_REDIS {
        "primary".to_owned()
    } else {
        value.to_owned()
    }
}

/// 业务作用：按 qualifier 借出一个已注册的 Redis 客户端句柄。
///
/// 返回 `Arc` clone 是该客户端本身的共享语义，不是把资源移出容器。
///
/// # 参数
///
/// - `application`：持有组件资源的共享应用上下文。
/// - `name`：Redis 实例 qualifier；`default` 只作为 `primary` 的本地查询别名。
pub(crate) async fn redis_handle(
    application: &Application,
    name: &str,
) -> ApplicationResult<Arc<RedisClient>> {
    let client = application.named_resource::<Arc<RedisClient>>(name).await?;
    Ok(client.clone())
}

/// 业务作用：创建 Redis 组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含连接串和口令的稳定摘要。
fn redis_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Redis, phase, message)
}

/// 业务作用：创建带底层错误链的 Redis 错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含连接串和口令的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
fn redis_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Redis, phase, message, source)
}

/// Redis 子能力下的命名生成器装配计划；账本初始化不属于应用启动动作。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnowflakePlan {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_snowflake_redis")]
    redis_ref: String,
    #[serde(default)]
    config: nadis::snowflake::SnowflakeConfig,
    namespace: Option<nadis::snowflake::WorkerIdNamespace>,
}

/// 业务作用：为未指定来源的生成器绑定标准 Redis 默认资源。
/// 参数说明：无。
/// 返回：默认来源的 canonical qualifier。
fn default_snowflake_redis() -> String {
    "primary".to_owned()
}

/// 业务作用：所有 Redis 来源建立后装配显式启用的非复用生成器。
/// 参数说明：`context` 提供资源登记和逆序清理责任。
/// 返回：所有启用计划可领取编号时发布命名资源；失败终止启动，已登记 owner 随回滚关闭。
async fn install_snowflakes(context: &mut StartContext<'_>) -> ApplicationResult<()> {
    let application = context.application().clone();
    let snapshot = application.config();
    let Some(section) = snapshot
        .value()
        .get("redis")
        .and_then(|redis| redis.get("snowflake"))
    else {
        return Ok(());
    };
    let plans: BTreeMap<String, SnowflakePlan> =
        serde_json::from_value(section.clone()).map_err(|error| {
            redis_error_src(
                ApplicationPhase::Start,
                "invalid managed snowflake plans",
                error,
            )
        })?;
    if plans.len() > MAX_MANAGED_REDIS_SOURCES {
        return Err(redis_error(
            ApplicationPhase::Start,
            "managed snowflake plan count exceeds limit",
        ));
    }
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        if name.is_empty() || name.len() > MAX_MANAGED_REDIS_NAME_BYTES {
            return Err(redis_error(
                ApplicationPhase::Start,
                "invalid managed snowflake resource name",
            ));
        }
        let namespace = plan.namespace.ok_or_else(|| {
            redis_error(
                ApplicationPhase::Start,
                "managed snowflake requires an initialized namespace identity",
            )
        })?;
        let client = redis_handle(&application, &canonical_qualifier(&plan.redis_ref)).await?;
        // 启动只允许领取已获管理授权的现有账本，缺失时禁止自动创建。
        let generator = Arc::new(
            nadis::snowflake::ManagedSnowflake::allocate(&client, &plan.config, &namespace)
                .await
                .map_err(|error| {
                    redis_error_src(
                        ApplicationPhase::Start,
                        "managed snowflake allocation rejected",
                        error,
                    )
                })?,
        );
        context.activate(Box::new(SnowflakeShutdown(generator.clone())));
        context.register_resource(Some(&name), generator)?;
    }
    Ok(())
}

/// 发号权限由应用 owner 控制，即使调用方保留 Arc 也不能越过停机。
struct SnowflakeShutdown(Arc<nadis::snowflake::ManagedSnowflake>);

impl ShutdownAction for SnowflakeShutdown {
    /// 业务作用：提供稳定的关闭责任名称。
    /// 参数说明：无。
    /// 返回：不包含动态来源或资源名的固定名称。
    fn label(&self) -> &'static str {
        "redis-snowflake"
    }

    /// 业务作用：在 Redis 客户端关闭前收回当前生成器的发号权限。
    /// 参数说明：`context` 为宿主停机上下文；本动作不执行外部 I/O。
    /// 返回：发号门禁关闭后成功，不回收已分配 workerId。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.close();
        Box::pin(async { Ok(()) })
    }
}

impl Drop for SnowflakeShutdown {
    /// 业务作用：启动取消或清理 owner 释放时收回发号权限。
    /// 参数说明：无。
    /// 返回：同步关闭，不生成新的后台任务。
    fn drop(&mut self) {
        self.0.close();
    }
}

impl Application {
    /// 业务作用：取得由 redis 子能力装配的命名 Snowflake 生成器。
    /// 参数说明：`name` 为 redis.snowflake 中显式启用的计划名。
    /// 返回：成功取得共享句柄；未配置或尚未装配时返回资源错误，停机后句柄拒绝发号。
    pub async fn snowflake(
        &self,
        name: &str,
    ) -> ApplicationResult<Arc<nadis::snowflake::ManagedSnowflake>> {
        Ok(self
            .named_resource::<Arc<nadis::snowflake::ManagedSnowflake>>(name)
            .await?
            .clone())
    }
}
