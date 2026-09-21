//! 连接等待、事务连接槽和拒绝门禁的有界原子观测；连接池状态只在指标出口读取。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use nametrics_core::atomic::{add, AtomicHistogram, LATENCY_SECONDS};
use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};
use nanotify_core::{AlertRoute, EventKind, Notification, NotificationFields, Severity};

use crate::{DatabaseDriver, MAX_MANAGED_DATASOURCES};

/// 受管连接的语句日志策略；关闭时 SQLx 不产生逐条语句事件。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementLogging {
    #[default]
    Disabled,
    Debug,
    Trace,
}

/// 调用方显式声明的连接用途，不从任务、SQL 或错误文本推断。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionPurpose {
    Mapper,
    Migration,
    Probe,
    Direct,
}

impl ConnectionPurpose {
    /// 业务作用：返回连接用途对应的固定指标值。
    /// 参数说明：无。
    /// 返回：不会包含业务输入的静态标识。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mapper => "mapper",
            Self::Migration => "migration",
            Self::Probe => "probe",
            Self::Direct => "direct",
        }
    }
}

/// 连接获取的稳定完成分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquireOutcome {
    Ok,
    Timeout,
    Closed,
    Worker,
    Cancelled,
    Other,
}

impl AcquireOutcome {
    /// 业务作用：把详细完成分类投影到延迟分布的固定状态。
    /// 参数说明：无。
    /// 返回：成功、失败或调用方取消的数组位置。
    const fn status(self) -> usize {
        match self {
            Self::Ok => 0,
            Self::Cancelled => 2,
            _ => 1,
        }
    }
}

/// SQL 执行前拒绝原因，不与数据库调用失败混合。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionRejection {
    TxRequired,
    TxForbidden,
    CrossDatasource,
    CrossDriver,
    UnknownDatasource,
    RegistryUnavailable,
    TxConnectionBusy,
}

/// 等待日志的固定级别。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WaitLogLevel {
    Info,
    #[default]
    Warn,
}

/// 启动期冻结的成功等待日志策略，不关闭基础指标。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WaitObservationPolicy {
    pub threshold: Duration,
    pub log_enabled: bool,
    pub log_level: WaitLogLevel,
    pub log_cooldown: Duration,
}

/// 启动期冻结的连接超时通知路由；只持有具体有界队列，不接受任意回调。
pub struct AcquireAlertPolicy {
    pub route: AlertRoute,
    pub severity: Severity,
    pub cooldown: Duration,
    pub purposes: Vec<ConnectionPurpose>,
    pub service: String,
    pub instance: String,
    pub environment: Option<String>,
    pub cluster: Option<String>,
}

impl Default for WaitObservationPolicy {
    /// 业务作用：为连接等待提供默认关闭日志的稳定策略。
    /// 参数说明：无。
    /// 返回：250 毫秒阈值与 60 秒冷却，基础统计仍全量记录。
    fn default() -> Self {
        Self {
            threshold: Duration::from_millis(250),
            log_enabled: false,
            log_level: WaitLogLevel::Warn,
            log_cooldown: Duration::from_secs(60),
        }
    }
}

struct WaitCells {
    outcomes: [AtomicU64; 6],
    durations: [AtomicHistogram; 3],
    in_flight: AtomicU64,
}

impl WaitCells {
    /// 业务作用：预分配等待路径的全部计数与分布。
    /// 参数说明：无。
    /// 返回：零值单元；后续完成路径无需动态构造指标。
    const fn new() -> Self {
        Self {
            outcomes: [const { AtomicU64::new(0) }; 6],
            durations: [const { AtomicHistogram::new(LATENCY_SECONDS) }; 3],
            in_flight: AtomicU64::new(0),
        }
    }
}

/// 进程生命周期连接统计槽；未知名称共用固定回退槽。
pub struct ConnectionSlot {
    driver: DatabaseDriver,
    datasource: Box<str>,
    acquire: [WaitCells; 4],
    transaction: WaitCells,
    rejections: [AtomicU64; 7],
    policies: OnceLock<(WaitObservationPolicy, WaitObservationPolicy)>,
    log_next: [AtomicU64; 2],
    acquire_alert: OnceLock<AcquireAlertPolicy>,
    alert_next: AtomicU64,
    alert_sequence: AtomicU64,
}

impl ConnectionSlot {
    /// 业务作用：在启动期建立一个 datasource 的固定容量观测槽。
    /// 参数说明：`driver` 和 `datasource` 为冻结的连接身份。
    /// 返回：未写入任何调用事实的原子单元。
    fn new(driver: DatabaseDriver, datasource: &str) -> Self {
        Self {
            driver,
            datasource: datasource.into(),
            acquire: [const { WaitCells::new() }; 4],
            transaction: WaitCells::new(),
            rejections: [const { AtomicU64::new(0) }; 7],
            policies: OnceLock::new(),
            log_next: [const { AtomicU64::new(0) }; 2],
            acquire_alert: OnceLock::new(),
            alert_next: AtomicU64::new(0),
            alert_sequence: AtomicU64::new(0),
        }
    }

    /// 业务作用：记录连接发放之前的安全门禁拒绝。
    /// 参数说明：`reason` 是由连接权威判定的固定分类。
    /// 返回：无；不产生数据库 operation 或连接等待样本。
    pub fn reject(&self, reason: ConnectionRejection) {
        add(&self.rejections[reason as usize], 1);
    }

    /// 业务作用：开始观察普通 Pool 获取，保证取消时回收 in-flight。
    /// 参数说明：`purpose` 是调用方明确声明的连接用途。
    /// 返回：持有进程静态统计槽的等待守卫。
    pub fn acquire(&'static self, purpose: ConnectionPurpose) -> WaitGuard {
        WaitGuard::new(self, purpose, false)
    }

    /// 业务作用：开始观察 ambient transaction 的连接槽等待。
    /// 参数说明：无。
    /// 返回：与 Pool acquire 独立计时的取消安全守卫。
    pub fn transaction_slot(&'static self) -> WaitGuard {
        WaitGuard::new(self, ConnectionPurpose::Direct, true)
    }
}

static SLOTS: [OnceLock<ConnectionSlot>; MAX_MANAGED_DATASOURCES * 2] =
    [const { OnceLock::new() }; MAX_MANAGED_DATASOURCES * 2];
static REGISTRATION: Mutex<()> = Mutex::new(());
static UNKNOWN_MYSQL: OnceLock<ConnectionSlot> = OnceLock::new();
static UNKNOWN_PGSQL: OnceLock<ConnectionSlot> = OnceLock::new();
static CLOCK: OnceLock<Instant> = OnceLock::new();

/// 业务作用：在启动期为已知 datasource 预分配槽，不允许运行时名称扩大指标基数。
/// 参数说明：`driver` 与 `datasource` 来自已验证的连接配置。
/// 返回：首次或同名重复注册返回固定槽；名称或总量越界返回错误。
pub fn register_datasource(
    driver: DatabaseDriver,
    datasource: &str,
) -> anyhow::Result<&'static ConnectionSlot> {
    crate::validate_datasource_name(datasource)?;
    let _registration = REGISTRATION
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    CLOCK.get_or_init(Instant::now);
    fallback(driver);
    if let Some(slot) = known_slot(driver, datasource) {
        return Ok(slot);
    }
    let cell = SLOTS
        .iter()
        .find(|cell| cell.get().is_none())
        .ok_or_else(|| anyhow::anyhow!("connection observation datasource capacity exceeded"))?;
    Ok(cell.get_or_init(|| ConnectionSlot::new(driver, datasource)))
}

/// 业务作用：冻结 datasource 的等待日志策略，拒绝运行期静默替换。
/// 参数说明：`driver`、`datasource` 定位槽；`acquire` 和 `slot` 分别控制两种等待。
/// 返回：首次安装或相同策略重复安装成功；策略变化要求重启进程。
pub fn configure_datasource(
    driver: DatabaseDriver,
    datasource: &str,
    acquire: WaitObservationPolicy,
    slot: WaitObservationPolicy,
) -> anyhow::Result<()> {
    let cells = register_datasource(driver, datasource)?;
    let policy = (acquire, slot);
    anyhow::ensure!(
        *cells.policies.get_or_init(|| policy) == policy,
        "connection observation policy is frozen; restart is required"
    );
    Ok(())
}

/// 业务作用：将已验证的通知引用绑定到固定 datasource 槽。
/// 参数说明：`driver` 与 `datasource` 指定槽；`policy` 包含具体队列、冷却与实例身份。
/// 返回：首次安装成功；用途为空、重复或已安装时拒绝，不进行网络操作。
pub fn configure_acquire_alert(
    driver: DatabaseDriver,
    datasource: &str,
    policy: AcquireAlertPolicy,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !policy.purposes.is_empty() && policy.purposes.len() <= 4,
        "acquire alert purposes must be nonempty and bounded"
    );
    let mut seen = [false; 4];
    for purpose in &policy.purposes {
        anyhow::ensure!(!seen[*purpose as usize], "duplicate acquire alert purpose");
        seen[*purpose as usize] = true;
    }
    register_datasource(driver, datasource)?
        .acquire_alert
        .set(policy)
        .map_err(|_| anyhow::anyhow!("acquire alert policy is frozen; restart is required"))
}

/// 业务作用：查询已预分配槽，未知名称只归入固定回退身份。
/// 参数说明：`driver` 和 `datasource` 是本次连接请求的身份。
/// 返回：进程静态槽；不会注册动态名称。
pub fn connection_slot(driver: DatabaseDriver, datasource: &str) -> &'static ConnectionSlot {
    known_slot(driver, datasource).unwrap_or_else(|| fallback(driver))
}

/// 业务作用：在固定目录中查找连接槽，不获取全局锁。
/// 参数说明：`driver` 与 `datasource` 为候选身份。
/// 返回：已登记身份的静态单元或空值。
fn known_slot(driver: DatabaseDriver, datasource: &str) -> Option<&'static ConnectionSlot> {
    SLOTS
        .iter()
        .filter_map(OnceLock::get)
        .find(|slot| slot.driver == driver && slot.datasource.as_ref() == datasource)
}

/// 业务作用：把未知 datasource 的拒绝归入固定低基数槽。
/// 参数说明：`driver` 指定后端。
/// 返回：不会包含用户输入名称的静态槽。
fn fallback(driver: DatabaseDriver) -> &'static ConnectionSlot {
    match driver {
        DatabaseDriver::MySql => &UNKNOWN_MYSQL,
        DatabaseDriver::PostgreSql => &UNKNOWN_PGSQL,
    }
    .get_or_init(|| ConnectionSlot::new(driver, "__unknown__"))
}

/// 连接等待生命周期守卫；显式完成与取消只裁决一次。
pub struct WaitGuard {
    slot: &'static ConnectionSlot,
    purpose: ConnectionPurpose,
    transaction: bool,
    started: Instant,
    finished: bool,
}

impl WaitGuard {
    /// 业务作用：进入等待状态并增加对应的在途数量。
    /// 参数说明：`slot` 为固定身份；`purpose` 为用途；`transaction` 区分事务槽。
    /// 返回：尚未裁决的等待守卫。
    fn new(slot: &'static ConnectionSlot, purpose: ConnectionPurpose, transaction: bool) -> Self {
        let guard = Self {
            slot,
            purpose,
            transaction,
            started: Instant::now(),
            finished: false,
        };
        guard.cells().in_flight.fetch_add(1, Ordering::Relaxed);
        guard
    }

    /// 业务作用：选取当前等待语义的固定原子单元。
    /// 参数说明：无。
    /// 返回：Pool 用途单元或事务连接槽单元。
    fn cells(&self) -> &WaitCells {
        if self.transaction {
            &self.slot.transaction
        } else {
            &self.slot.acquire[self.purpose as usize]
        }
    }

    /// 业务作用：记录连接等待的唯一终态，日志不会改写连接结果。
    /// 参数说明：`outcome` 必须在驱动错误类型被擦除前确定。
    /// 返回：无；重复完成不会重复计数。
    pub fn finish(&mut self, outcome: AcquireOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.started.elapsed();
        let cells = self.cells();
        add(&cells.outcomes[outcome as usize], 1);
        cells.durations[outcome.status()].observe(elapsed);
        cells.in_flight.fetch_sub(1, Ordering::Relaxed);
        if outcome == AcquireOutcome::Ok {
            self.log_wait(elapsed);
        }
        if outcome == AcquireOutcome::Timeout && !self.transaction {
            self.notify_timeout(elapsed);
        }
    }

    /// 业务作用：仅在超时规则与原子冷却均命中后构造一个有界通知。
    /// 参数说明：`elapsed` 是本次连接获取时长。
    /// 返回：无；入队拥塞和停机只影响通知自己的指标，不能改变连接错误。
    fn notify_timeout(&self, elapsed: Duration) {
        let Some(policy) = self.slot.acquire_alert.get() else {
            return;
        };
        if !policy.purposes.contains(&self.purpose) {
            return;
        }
        let now = CLOCK
            .get_or_init(Instant::now)
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        if !policy.cooldown.is_zero()
            && self
                .slot
                .alert_next
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    (value <= now).then(|| {
                        now.saturating_add(policy.cooldown.as_millis().min(u64::MAX as u128) as u64)
                    })
                })
                .is_err()
        {
            return;
        }
        let sequence = self.slot.alert_sequence.fetch_add(1, Ordering::Relaxed);
        let occurred_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        use std::hash::{Hash, Hasher};
        let mut identity = std::collections::hash_map::DefaultHasher::new();
        (
            policy.instance.as_str(),
            driver_name(self.slot.driver),
            self.slot.datasource.as_ref(),
        )
            .hash(&mut identity);
        let notification = Notification::new(NotificationFields {
            // 来源文本可能很长；固定长度指纹保留末尾序列，避免消息截断抹去同槽通知的区分信息。
            id: format!(
                "acquire-{:x}-{occurred_at_unix_ms}-{sequence}",
                identity.finish()
            ),
            event: EventKind::AcquireTimeout,
            severity: policy.severity,
            occurred_at_unix_ms,
            service: policy.service.clone(),
            instance: policy.instance.clone(),
            environment: policy.environment.clone(),
            cluster: policy.cluster.clone(),
            driver: driver_name(self.slot.driver).to_owned(),
            datasource: self.slot.datasource.to_string(),
            operation: "acquire".to_owned(),
            purpose: Some(self.purpose.as_str().to_owned()),
            duration: elapsed,
            outcome: "timeout".to_owned(),
            error_kind: Some("timeout".to_owned()),
            ..NotificationFields::default()
        });
        let _ = policy.route.try_send(notification);
    }

    /// 业务作用：只为成功且超阈值的等待输出有冷却的结构化日志。
    /// 参数说明：`elapsed` 是单调时钟测得的完整等待时长。
    /// 返回：无；默认不输出，且不携带 SQL、参数或错误原文。
    fn log_wait(&self, elapsed: Duration) {
        let Some(policies) = self.slot.policies.get() else {
            return;
        };
        let policy = if self.transaction {
            policies.1
        } else {
            policies.0
        };
        if !policy.log_enabled || elapsed < policy.threshold {
            return;
        }
        let next = &self.slot.log_next[usize::from(self.transaction)];
        let now = CLOCK
            .get_or_init(Instant::now)
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        if policy.log_cooldown != Duration::ZERO
            && next
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    (value <= now).then(|| {
                        now.saturating_add(
                            policy.log_cooldown.as_millis().min(u64::MAX as u128) as u64
                        )
                    })
                })
                .is_err()
        {
            return;
        }
        let event = if self.transaction {
            "transaction_slot_wait"
        } else {
            "connection_acquire_wait"
        };
        // 连接已取得且指标终态已提交，日志 subscriber 的展开不能撤销该业务结果。
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match policy.log_level {
                WaitLogLevel::Info => {
                    tracing::info!(target: "natx::connection", event, datasource = self.slot.datasource.as_ref(), driver = driver_name(self.slot.driver), purpose = self.purpose.as_str(), duration_seconds = elapsed.as_secs_f64(), "数据库连接等待超过阈值")
                }
                WaitLogLevel::Warn => {
                    tracing::warn!(target: "natx::connection", event, datasource = self.slot.datasource.as_ref(), driver = driver_name(self.slot.driver), purpose = self.purpose.as_str(), duration_seconds = elapsed.as_secs_f64(), "数据库连接等待超过阈值")
                }
            }
        }));
        if let Err(payload) = result {
            // 异常载荷的自定义析构同样属于外部代码，二次展开也必须在诊断边界内终止。
            if let Err(nested) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
            {
                std::mem::forget(nested);
            }
        }
    }
}

impl Drop for WaitGuard {
    /// 业务作用：在 Future 取消或展开时回收在途计数并记录取消。
    /// 参数说明：无。
    /// 返回：无；已完成守卫不再产生样本。
    fn drop(&mut self) {
        self.finish(AcquireOutcome::Cancelled);
    }
}

/// 业务作用：返回数据库后端的固定指标身份。
/// 参数说明：`driver` 是已解析的后端枚举。
/// 返回：与配置 driver 一致的固定字符串。
fn driver_name(driver: DatabaseDriver) -> &'static str {
    match driver {
        DatabaseDriver::MySql => "mysql",
        DatabaseDriver::PostgreSql => "postgresql",
    }
}

const OUTCOMES: [&str; 6] = ["ok", "timeout", "closed", "worker", "cancelled", "other"];
const STATUSES: [&str; 3] = ["success", "failure", "cancelled"];
const PURPOSES: [&str; 4] = ["mapper", "migration", "probe", "direct"];
const REJECTIONS: [&str; 7] = [
    "tx_required",
    "tx_forbidden",
    "cross_datasource",
    "cross_driver",
    "unknown_datasource",
    "registry_unavailable",
    "tx_connection_busy",
];

macro_rules! descriptor {
    ($name:ident, $text:literal, $kind:ident, $labels:expr, $bounds:expr) => {
        static $name: MetricDescriptor = MetricDescriptor {
            name: $text,
            help: "数据库连接与等待状态。",
            unit: if matches!(MetricKind::$kind, MetricKind::Histogram) {
                "seconds"
            } else {
                ""
            },
            kind: MetricKind::$kind,
            label_names: $labels,
            histogram_bounds: $bounds,
        };
    };
}
descriptor!(
    ACQUIRES,
    "natx_connection_acquires_total",
    Counter,
    &["datasource", "driver", "purpose", "outcome"],
    &[]
);
descriptor!(
    ACQUIRE_DURATION,
    "natx_connection_acquire_duration_seconds",
    Histogram,
    &["datasource", "driver", "purpose", "status"],
    LATENCY_SECONDS
);
descriptor!(
    ACQUIRE_IN_FLIGHT,
    "natx_connection_acquire_in_flight",
    Gauge,
    &["datasource", "driver", "purpose"],
    &[]
);
descriptor!(
    SLOT_WAITS,
    "natx_transaction_slot_waits_total",
    Counter,
    &["datasource", "driver", "outcome"],
    &[]
);
descriptor!(
    SLOT_DURATION,
    "natx_transaction_slot_wait_duration_seconds",
    Histogram,
    &["datasource", "driver", "status"],
    LATENCY_SECONDS
);
descriptor!(
    SLOT_IN_FLIGHT,
    "natx_transaction_slot_wait_in_flight",
    Gauge,
    &["datasource", "driver"],
    &[]
);
descriptor!(
    REJECTION,
    "natx_connection_rejections_total",
    Counter,
    &["datasource", "driver", "reason"],
    &[]
);
descriptor!(
    POOL_CONNECTIONS,
    "natx_pool_connections",
    Gauge,
    &["datasource", "driver", "state"],
    &[]
);
descriptor!(
    POOL_MAX,
    "natx_pool_max_connections",
    Gauge,
    &["datasource", "driver"],
    &[]
);
static CONNECTION_DESCRIPTORS: &[&MetricDescriptor] = &[
    &ACQUIRES,
    &ACQUIRE_DURATION,
    &ACQUIRE_IN_FLIGHT,
    &SLOT_WAITS,
    &SLOT_DURATION,
    &SLOT_IN_FLIGHT,
    &REJECTION,
];
static POOL_DESCRIPTORS: &[&MetricDescriptor] = &[&POOL_CONNECTIONS, &POOL_MAX];

/// 按后端冻结槽目录的原子等待指标源，Prometheus 与 OTLP 共享同一快照与系列边界。
pub struct ConnectionMetricsSource {
    slots: Box<[&'static ConnectionSlot]>,
}

impl ConnectionMetricsSource {
    /// 业务作用：在指标源注册前冻结指定后端的槽目录，后续注册不扩大已预留的系列预算。
    /// 参数说明：`driver` 限定该源拥有的已登记槽及固定未知身份槽。
    /// 返回：不创建后台 worker 的指标源；原子值持续更新，目录不再变化。
    pub fn new(driver: DatabaseDriver) -> Self {
        // 与槽登记共用门禁形成一致的目录边界，预算与所有后续快照必须消费同一组静态引用。
        let _registration = REGISTRATION
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let slots = SLOTS
            .iter()
            .filter_map(OnceLock::get)
            .filter(|slot| slot.driver == driver)
            .chain(std::iter::once(fallback(driver)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    /// 业务作用：计算当前冻结 datasource 目录的最坏 Prometheus 系列预算。
    /// 参数说明：无。
    /// 返回：包含固定未知身份和全部 counter、gauge、桶、sum、count 的系列数。
    pub fn series_budget(&self) -> usize {
        self.slots.len() * (5 * (6 + 3 * (LATENCY_SECONDS.len() + 3) + 1) + 7)
    }
}

impl LegacyMetricsSource for ConnectionMetricsSource {
    /// 业务作用：发布等待与门禁族的统一描述符。
    /// 参数说明：无。
    /// 返回：固定且与结构化快照一致的目录。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        CONNECTION_DESCRIPTORS
    }

    /// 业务作用：在出口侧读取原子单元并构造低基数样本。
    /// 参数说明：无。
    /// 返回：冻结目录内的实时 counter、gauge 和非累积 Histogram 快照，不包含源创建后登记的槽。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut samples = Vec::new();
        for slot in &self.slots {
            let base = vec![
                ("datasource", slot.datasource.to_string()),
                ("driver", driver_name(slot.driver).to_owned()),
            ];
            for (purpose, cells) in PURPOSES.iter().zip(&slot.acquire) {
                let mut labels = base.clone();
                labels.push(("purpose", (*purpose).to_owned()));
                snapshot_wait(
                    &mut samples,
                    cells,
                    labels,
                    &ACQUIRES,
                    &ACQUIRE_DURATION,
                    &ACQUIRE_IN_FLIGHT,
                );
            }
            snapshot_wait(
                &mut samples,
                &slot.transaction,
                base.clone(),
                &SLOT_WAITS,
                &SLOT_DURATION,
                &SLOT_IN_FLIGHT,
            );
            for (reason, cell) in REJECTIONS.iter().zip(&slot.rejections) {
                let mut labels = base.clone();
                labels.push(("reason", (*reason).to_owned()));
                samples.push(MetricSample {
                    name: REJECTION.name,
                    labels,
                    value: MetricValue::Counter(cell.load(Ordering::Relaxed)),
                });
            }
        }
        Some(samples)
    }

    /// 业务作用：由 MetricHub 统一渲染结构化快照，避免重复文本系列。
    /// 参数说明：`_output` 为兼容文本缓冲区。
    /// 返回：无；该源不使用旧文本回退路径。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：把一种等待路径的全部固定单元投影为出口样本。
/// 参数说明：`samples` 接收样本；`cells` 为原子槽；`labels` 为冻结身份；其余参数为对应描述符。
/// 返回：无；分配只发生在快照侧。
fn snapshot_wait(
    samples: &mut Vec<MetricSample>,
    cells: &WaitCells,
    labels: Vec<(&'static str, String)>,
    counter: &'static MetricDescriptor,
    histogram: &'static MetricDescriptor,
    gauge: &'static MetricDescriptor,
) {
    for (outcome, cell) in OUTCOMES.iter().zip(&cells.outcomes) {
        let mut labels = labels.clone();
        labels.push(("outcome", (*outcome).to_owned()));
        samples.push(MetricSample {
            name: counter.name,
            labels,
            value: MetricValue::Counter(cell.load(Ordering::Relaxed)),
        });
    }
    for (status, cell) in STATUSES.iter().zip(&cells.durations) {
        let mut labels = labels.clone();
        labels.push(("status", (*status).to_owned()));
        samples.push(MetricSample {
            name: histogram.name,
            labels,
            value: cell.snapshot(),
        });
    }
    samples.push(MetricSample {
        name: gauge.name,
        labels,
        value: MetricValue::Gauge(cells.in_flight.load(Ordering::Relaxed) as f64),
    });
}

/// SQLx Pool 的近似瞬时状态；各字段不是事务式同时读取。
pub struct PoolState {
    pub total: u32,
    pub idle: u32,
    pub max: u32,
}

/// 后端只在抓取时提供连接池状态，不参与业务热路径。
pub trait PoolSnapshot: Send + Sync {
    /// 业务作用：读取连接池当前规模与配置上限。
    /// 参数说明：无。
    /// 返回：允许并发变化的近似瞬时状态。
    fn snapshot(&self) -> PoolState;
}

/// 持有只读 Pool 句柄的结构化指标源。
pub struct PoolMetricsSource {
    driver: DatabaseDriver,
    pools: Vec<(String, Arc<dyn PoolSnapshot>)>,
}

impl PoolMetricsSource {
    /// 业务作用：从启动期连接目录建立 Pool 状态出口。
    /// 参数说明：`driver` 指定后端；`pools` 为固定名称与只读句柄。
    /// 返回：名称合法且不重复时返回源，未创建任何采样线程。
    pub fn new(
        driver: DatabaseDriver,
        pools: Vec<(String, Arc<dyn PoolSnapshot>)>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            pools.len() <= MAX_MANAGED_DATASOURCES,
            "pool observation capacity exceeded"
        );
        let mut names = std::collections::BTreeSet::new();
        for (name, _) in &pools {
            crate::validate_datasource_name(name)?;
            anyhow::ensure!(names.insert(name), "duplicate pool observation datasource");
        }
        Ok(Self { driver, pools })
    }

    /// 业务作用：预留每个 Pool 的固定 gauge 系列预算。
    /// 参数说明：无。
    /// 返回：total、idle、in_use 和上限的系列总数。
    pub fn series_budget(&self) -> usize {
        self.pools.len() * 4
    }
}

impl LegacyMetricsSource for PoolMetricsSource {
    /// 业务作用：发布后端中立的 Pool gauge 描述符。
    /// 参数说明：无。
    /// 返回：状态规模和配置上限两个固定族。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        POOL_DESCRIPTORS
    }

    /// 业务作用：抓取时读取 Pool 并用饱和差值计算使用数。
    /// 参数说明：无。
    /// 返回：不因 total 与 idle 并发变化而下溢的近似快照。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut samples = Vec::with_capacity(self.series_budget());
        for (name, pool) in &self.pools {
            let state = pool.snapshot();
            let base = vec![
                ("datasource", name.clone()),
                ("driver", driver_name(self.driver).to_owned()),
            ];
            for (label, value) in [
                ("total", state.total),
                ("idle", state.idle),
                ("in_use", state.total.saturating_sub(state.idle)),
            ] {
                let mut labels = base.clone();
                labels.push(("state", label.to_owned()));
                samples.push(MetricSample {
                    name: POOL_CONNECTIONS.name,
                    labels,
                    value: MetricValue::Gauge(value as f64),
                });
            }
            samples.push(MetricSample {
                name: POOL_MAX.name,
                labels: base,
                value: MetricValue::Gauge(state.max as f64),
            });
        }
        Some(samples)
    }

    /// 业务作用：保持所有出口统一消费结构化 Pool 样本。
    /// 参数说明：`_output` 为旧文本回退缓冲区。
    /// 返回：无；由 MetricHub 完成文本编码。
    fn render_prometheus(&self, _output: &mut String) {}
}
