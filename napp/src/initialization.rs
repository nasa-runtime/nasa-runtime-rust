use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use nametrics_core::MetricRecorder;
use tokio_util::sync::CancellationToken;

use crate::{
    supervisor::{ManagedTaskFuture, TaskKind},
    Application, ApplicationError, ApplicationFuture, ApplicationMode, ApplicationPhase,
    ApplicationResult, ComponentId,
};

/// 单个应用允许冻结的 initializer 总数上限。
pub const MAX_INITIALIZERS: usize = 256;

/// 单个 initializer 允许声明的直接依赖数上限。
pub const MAX_INITIALIZER_REQUIRES: usize = 32;

/// 未显式声明优先级时使用的 initializer 稳定顺序。
pub const DEFAULT_INITIALIZER_ORDER: i32 = 100_000;

/// initializer 工厂与三轮屏障的延迟分布。
static INITIALIZER_DURATION_SECONDS: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_initializer_duration_seconds",
        help: "Application initializer 各阶段执行时长。",
        unit: "seconds",
        kind: nametrics_core::MetricKind::Histogram,
        label_names: &["initializer", "stage"],
        histogram_bounds: &[
            0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
        ],
    };

/// initializer 按阶段和稳定分类累计的失败数。
static INITIALIZER_FAILURES_TOTAL: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_initializer_failures_total",
        help: "Application initializer 按阶段和稳定分类累计的失败数。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &["initializer", "stage", "kind"],
        histogram_bounds: &[],
    };

/// 业务作用：在 Application 容器创建时注册 initializer 的两组有界指标描述。
///
/// 参数说明：
/// - `hub`：当前进程唯一的指标目录与存储。
///
/// 返回：两组 descriptor 与现有指标无冲突时成功；语义冲突时返回首个冲突。
pub(crate) fn register_metrics(
    hub: &nametrics_core::MetricHub,
) -> Result<(), nametrics_core::MetricConflict> {
    hub.register(&INITIALIZER_DURATION_SECONDS)?;
    hub.register(&INITIALIZER_FAILURES_TOTAL)
}

/// 业务作用：记录一次 initializer 工厂或屏障阶段的有界时长观测。
///
/// 参数说明：
/// - `application`：持有进程唯一指标 hub 的容器。
/// - `initializer`：冻结计划中的 canonical 身份。
/// - `stage`：工厂、三轮屏障或激活阶段。
/// - `duration`：用单调时钟测得的执行时长。
///
/// 返回：无返回值；指标标签只来自已校验身份与封闭枚举。
pub(crate) fn record_duration(
    application: &Application,
    initializer: &str,
    stage: InitializerStage,
    duration: std::time::Duration,
) {
    application.metrics_hub().histogram(
        &INITIALIZER_DURATION_SECONDS,
        duration.as_secs_f64(),
        &[initializer, stage_name(stage)],
    );
}

/// 业务作用：按 initializer、阶段和封闭分类累计一次启动失败。
///
/// 参数说明：
/// - `application`：持有进程唯一指标 hub 的容器。
/// - `initializer`：冻结计划中的 canonical 身份。
/// - `stage`：失败被观察到的阶段。
/// - `kind`：不依赖底层错误文本的稳定分类。
///
/// 返回：无返回值；不使用 URL、租户、配置值或错误文本作标签。
pub(crate) fn record_failure(
    application: &Application,
    initializer: &str,
    stage: InitializerStage,
    kind: InitializerFailureKind,
) {
    application.metrics_hub().counter(
        &INITIALIZER_FAILURES_TOTAL,
        1,
        &[initializer, stage_name(stage), failure_kind_name(kind)],
    );
}

/// 业务作用：把 initializer 阶段转换为指标所需的静态低基数标签。
///
/// 参数说明：
/// - `stage`：封闭 initializer 阶段。
///
/// 返回：与 `Display` 语义一致的静态小写标签。
const fn stage_name(stage: InitializerStage) -> &'static str {
    match stage {
        InitializerStage::Factory => "factory",
        InitializerStage::Before => "before",
        InitializerStage::Initialize => "initialize",
        InitializerStage::After => "after",
        InitializerStage::Activation => "activation",
    }
}

/// 业务作用：把 initializer 失败分类转换为指标所需的静态低基数标签。
///
/// 参数说明：
/// - `kind`：封闭失败分类。
///
/// 返回：与 `Display` 语义一致的静态小写标签。
const fn failure_kind_name(kind: InitializerFailureKind) -> &'static str {
    match kind {
        InitializerFailureKind::Error => "error",
        InitializerFailureKind::Panicked => "panicked",
        InitializerFailureKind::TimedOut => "timed_out",
        InitializerFailureKind::Cancelled => "cancelled",
        InitializerFailureKind::ModeViolation => "mode_violation",
    }
}

/// initializer 是否只做有界初始化，或还会在 Ready 后持有长期任务。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitializerKind {
    /// 只执行三阶段初始化，不登记长期任务或 readiness。
    OneShot,
    /// 允许暂存受管长期任务和 readiness，且要求 Service 模式。
    Hosted,
}

impl Default for InitializerKind {
    /// 业务作用：返回不改变进程生存模型的保守缺省类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定为 [`InitializerKind::OneShot`]。
    fn default() -> Self {
        Self::OneShot
    }
}

/// initializer 工厂和三轮屏障中的稳定阶段身份。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitializerStage {
    /// 条件判断、句柄解析和实例构造阶段。
    Factory,
    /// 全局 `before` 屏障。
    Before,
    /// 全局 `initialize` 屏障。
    Initialize,
    /// 全局 `after` 屏障。
    After,
    /// Ready 后激活暂存任务的阶段。
    Activation,
}

impl fmt::Display for InitializerStage {
    /// 业务作用：写出可用于日志和低基数指标的稳定阶段名。
    ///
    /// 参数说明：
    /// - `f`：接收阶段名的格式化缓冲区。
    ///
    /// 返回：写入成功返回 `Ok`，格式化失败时透传错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Factory => "factory",
            Self::Before => "before",
            Self::Initialize => "initialize",
            Self::After => "after",
            Self::Activation => "activation",
        })
    }
}

/// initializer 失败的稳定分类，不把 panic payload 或底层配置值当作分类标签。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitializerFailureKind {
    /// initializer future 返回业务错误。
    Error,
    /// initializer future 发生 panic，payload 已丢弃。
    Panicked,
    /// 共享启动截止时间耗尽。
    TimedOut,
    /// 启动取消或进程信号终止了当前阶段。
    Cancelled,
    /// initializer 类型与 Service/Batch 模式不兼容。
    ModeViolation,
}

impl fmt::Display for InitializerFailureKind {
    /// 业务作用：写出低基数失败分类名。
    ///
    /// 参数说明：
    /// - `f`：接收分类名的格式化缓冲区。
    ///
    /// 返回：写入成功返回 `Ok`，格式化失败时透传错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Error => "error",
            Self::Panicked => "panicked",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::ModeViolation => "mode_violation",
        })
    }
}

/// 带稳定 initializer 身份、阶段和分类的公开失败证据。
#[derive(Debug, thiserror::Error)]
#[error("initializer `{name}` failed during {stage}: {kind}")]
pub struct InitializerFailure {
    name: Arc<str>,
    stage: InitializerStage,
    kind: InitializerFailureKind,
    #[source]
    source: Option<anyhow::Error>,
}

impl InitializerFailure {
    /// 业务作用：创建不携带底层秘密或 panic payload 的 initializer 失败证据。
    ///
    /// 参数说明：
    /// - `name`：冻结计划中的 canonical initializer 身份。
    /// - `stage`：失败发生或被观察到的阶段。
    /// - `kind`：稳定失败分类。
    ///
    /// 返回：可安全进入 Application 错误链的失败证据。
    pub(crate) fn new(
        name: Arc<str>,
        stage: InitializerStage,
        kind: InitializerFailureKind,
    ) -> Self {
        Self {
            name,
            stage,
            kind,
            source: None,
        }
    }

    /// 业务作用：创建带已脱敏底层错误链的 initializer 失败证据。
    ///
    /// 参数说明：
    /// - `name`：冻结计划中的 canonical initializer 身份。
    /// - `stage`：返回业务错误的执行阶段。
    /// - `source`：保留给统一诊断管道的底层错误。
    ///
    /// 返回：分类固定为 `Error` 且保留 source 链的证据。
    pub(crate) fn with_source(
        name: Arc<str>,
        stage: InitializerStage,
        source: anyhow::Error,
    ) -> Self {
        Self {
            name,
            stage,
            kind: InitializerFailureKind::Error,
            source: Some(source),
        }
    }

    /// 业务作用：读取失败所属 initializer 的 canonical 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与失败证据共同存活的名称借用。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 业务作用：读取失败所属三阶段或激活阶段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定阶段枚举。
    pub fn stage(&self) -> InitializerStage {
        self.stage
    }

    /// 业务作用：读取失败的稳定分类。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不依赖错误文本解析的分类枚举。
    pub fn kind(&self) -> InitializerFailureKind {
        self.kind
    }
}

/// 三轮业务初始化行为；元数据和构造方式由登记层独立保存。
pub trait Initialization: Send + 'static {
    /// 业务作用：在全部 initializer 的构造完成后延迟解析句柄并准备前置条件。
    ///
    /// 参数说明：
    /// - `_context`：当前 initializer 独占的受控初始化上下文。
    ///
    /// 返回：准备完成返回成功；失败时阻止后续 initializer 和应用接流。
    fn before<'a>(
        &'a mut self,
        _context: &'a mut InitializationContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：建立动态业务设置、路由或注册表，不启动长期任务和入站监听。
    ///
    /// 参数说明：
    /// - `_context`：当前 initializer 独占的受控初始化上下文。
    ///
    /// 返回：本项动态设置完整建立时成功；失败时停止全局 initialize 屏障。
    fn initialize<'a>(
        &'a mut self,
        _context: &'a mut InitializationContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    /// 业务作用：在全部 initialize 成功后完成恢复、回填、预热并暂存长期任务。
    ///
    /// 参数说明：
    /// - `_context`：当前 initializer 独占的受控初始化上下文。
    ///
    /// 返回：接流前工作全部完成时成功；失败时不执行 Ready 和暂存任务。
    fn after<'a>(
        &'a mut self,
        _context: &'a mut InitializationContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

/// initializer 的稳定身份、优先级、依赖和生存类型。
#[derive(Debug, Clone)]
pub struct InitializerSpec {
    name: Arc<str>,
    order: i32,
    requires: Vec<Arc<str>>,
    kind: InitializerKind,
}

impl InitializerSpec {
    /// 业务作用：创建尚未校验、使用保守缺省顺序和类型的 initializer 元数据。
    ///
    /// 参数说明：
    /// - `name`：应用内唯一的 canonical initializer 身份。
    ///
    /// 返回：`order=100000`、无依赖且类型为 `one-shot` 的规格；登记时统一完成校验。
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            order: DEFAULT_INITIALIZER_ORDER,
            requires: Vec::new(),
            kind: InitializerKind::OneShot,
        }
    }

    /// 业务作用：设置无依赖节点之间的稳定优先级。
    ///
    /// 参数说明：
    /// - `order`：越小越先；依赖边始终高于本数值。
    ///
    /// 返回：更新后的规格，便于链式构造。
    pub fn order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }

    /// 业务作用：设置同阶段内必须先执行的 initializer 身份集合。
    ///
    /// 参数说明：
    /// - `requires`：最多 32 个 canonical 名称；重复、自依赖和缺失项在冻结时拒绝。
    ///
    /// 返回：更新后的规格，便于链式构造。
    pub fn requires<I, S>(mut self, requires: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.requires = requires.into_iter().map(Into::into).collect();
        self
    }

    /// 业务作用：声明 initializer 是否允许暂存长期任务和 readiness。
    ///
    /// 参数说明：
    /// - `kind`：`OneShot` 或只允许 Service 使用的 `Hosted`。
    ///
    /// 返回：更新后的规格，便于链式构造。
    pub fn kind(mut self, kind: InitializerKind) -> Self {
        self.kind = kind;
        self
    }

    /// 业务作用：读取 initializer 的 canonical 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与规格共同存活的名称借用。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 业务作用：读取无依赖节点之间的数值优先级。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：冻结计划使用的 `i32` 顺序值。
    pub fn order_value(&self) -> i32 {
        self.order
    }

    /// 业务作用：读取同阶段依赖名称列表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：保持登记顺序的依赖切片；拓扑裁决不依赖该原始顺序。
    pub fn required_initializers(&self) -> &[Arc<str>] {
        &self.requires
    }

    /// 业务作用：读取 initializer 的生存类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`OneShot` 或 `Hosted`。
    pub fn initializer_kind(&self) -> InitializerKind {
        self.kind
    }
}

/// 静态属性入口擦除后的异步工厂类型。
pub type ErasedInitializerFactory =
    fn(Application) -> ApplicationFuture<'static, Option<Box<dyn Initialization>>>;

/// 业务二进制内由 `#[nasa::initializer]` 生成的静态登记描述。
pub struct InitializerDescriptor {
    name: &'static str,
    order: i32,
    requires: &'static [&'static str],
    kind: InitializerKind,
    factory: ErasedInitializerFactory,
    source: &'static str,
}

impl InitializerDescriptor {
    /// 业务作用：供过程宏以常量表达式建立静态 initializer 描述。
    ///
    /// 参数说明：
    /// - `name`：canonical 稳定身份。
    /// - `order`：无依赖节点之间的优先级。
    /// - `requires`：同阶段依赖名称切片。
    /// - `kind`：one-shot 或 hosted。
    /// - `factory`：类型擦除后的条件异步工厂。
    /// - `source`：仅供重复登记定位的源码位置。
    ///
    /// 返回：可放入 linkme 分布式切片的不可变描述。
    #[doc(hidden)]
    pub const fn __new(
        name: &'static str,
        order: i32,
        requires: &'static [&'static str],
        kind: InitializerKind,
        factory: ErasedInitializerFactory,
        source: &'static str,
    ) -> Self {
        Self {
            name,
            order,
            requires,
            kind,
            factory,
            source,
        }
    }

    /// 业务作用：读取静态描述是否会改变进程生存模型，供 Auto 模式在创建 runtime 前裁决。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：描述声明为 hosted 时返回 true；条件工厂结果不改变该保守结论。
    pub(crate) const fn is_hosted(&self) -> bool {
        matches!(self.kind, InitializerKind::Hosted)
    }

    /// 业务作用：把静态借用元数据复制成冻结计划拥有的规格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：名称和依赖转为共享字符串后的运行时规格。
    fn to_spec(&self) -> InitializerSpec {
        InitializerSpec {
            name: Arc::from(self.name),
            order: self.order,
            requires: self.requires.iter().map(|name| Arc::from(*name)).collect(),
            kind: self.kind,
        }
    }
}

/// 当前 binary 内静态收集的全部 initializer 描述。
#[linkme::distributed_slice]
pub static COLLECTED_INITIALIZERS: [InitializerDescriptor];

/// 业务作用：判断静态 initializer 是否要求 Auto 模式保守选择 Service。
///
/// 参数说明: 无。
///
/// 返回：任一静态描述为 hosted 时返回 true；不调用条件工厂。
pub(crate) fn has_hosted_static_initializer() -> bool {
    COLLECTED_INITIALIZERS
        .iter()
        .any(InitializerDescriptor::is_hosted)
}

/// UserHook 期间动态登记、尚未进入冻结计划的 initializer。
pub(crate) struct InitializerRegistration {
    spec: InitializerSpec,
    initializer: Box<dyn Initialization>,
}

/// 独立于资源和 UserHook 状态的 initializer 登记门。
pub(crate) struct InitializerRegistry {
    open: AtomicBool,
    registrations: Mutex<Option<Vec<InitializerRegistration>>>,
}

impl InitializerRegistry {
    /// 业务作用：创建默认关闭且尚未冻结的 initializer 登记表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅 Runner 可在 Service UserHook 前开放的空登记表。
    pub(crate) fn new() -> Self {
        Self {
            open: AtomicBool::new(false),
            registrations: Mutex::new(Some(Vec::new())),
        }
    }

    /// 业务作用：仅在 Service UserHook 即将被 poll 时开放运行时 initializer 登记。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；重复开放仅在调试构建中暴露生命周期错误。
    pub(crate) fn open(&self) {
        let was_open = self.open.swap(true, Ordering::AcqRel);
        debug_assert!(
            !was_open,
            "initializer registration gate must open only once"
        );
    }

    /// 业务作用：线性化关闭运行时登记并取走全部已受理项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次冻结返回登记项；重复冻结返回稳定阶段错误。
    pub(crate) fn freeze(&self) -> ApplicationResult<Vec<InitializerRegistration>> {
        self.open.store(false, Ordering::Release);
        self.registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| initialization_error("initializer registry was already frozen"))
    }

    /// 业务作用：在开放窗口内登记一个已经构造完成的运行时 initializer。
    ///
    /// 参数说明：
    /// - `spec`：独立元数据，冻结时与静态描述合并校验。
    /// - `initializer`：三轮间保持可变状态的唯一实例。
    ///
    /// 返回：登记被完整写入返回成功；阶段关闭或规格非法时不取得实例所有权之外的副作用。
    pub(crate) fn register(
        &self,
        spec: InitializerSpec,
        initializer: Box<dyn Initialization>,
    ) -> ApplicationResult<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(initialization_error(
                "initializer registration is closed; runtime initializers are only accepted during the Service user hook",
            ));
        }
        validate_spec(&spec)?;
        let mut registrations = self
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = registrations.as_mut().ok_or_else(|| {
            initialization_error("initializer registration is closed after plan freeze")
        })?;
        if entries.len() >= MAX_INITIALIZERS {
            return Err(initialization_error(format!(
                "initializer count cannot exceed {MAX_INITIALIZERS}"
            )));
        }
        entries.push(InitializerRegistration { spec, initializer });
        Ok(())
    }
}

/// 冻结但尚未调用条件工厂的一项 initializer。
pub(crate) enum FrozenInitializer {
    Static {
        spec: InitializerSpec,
        factory: ErasedInitializerFactory,
    },
    Runtime {
        spec: InitializerSpec,
        initializer: Box<dyn Initialization>,
    },
}

impl FrozenInitializer {
    /// 业务作用：读取冻结项元数据，供工厂顺序和失败归因共用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无论来源均返回同一规格借用。
    pub(crate) fn spec(&self) -> &InitializerSpec {
        match self {
            Self::Static { spec, .. } | Self::Runtime { spec, .. } => spec,
        }
    }
}

/// 元数据校验完成、来源冲突已排除但条件工厂尚未执行的计划。
pub(crate) struct FrozenInitializerPlan {
    pub(crate) entries: Vec<FrozenInitializer>,
}

/// 条件工厂完成后实际启用的一项 initializer。
pub(crate) struct EnabledInitializer {
    pub(crate) spec: InitializerSpec,
    pub(crate) initializer: Box<dyn Initialization>,
}

/// 业务作用：冻结静态描述与运行时登记，确保任何工厂调用前完成身份和模式校验。
///
/// 参数说明：
/// - `mode`：preflight 已固定的 Service 或 Batch 模式。
/// - `runtime`：Service UserHook 已受理的运行时登记；Batch 固定为空。
///
/// 返回：按 `(order, name)` 排列的工厂计划；重名、非法元数据或模式冲突时无业务工厂被调用。
pub(crate) fn freeze_plan(
    mode: ApplicationMode,
    runtime: Vec<InitializerRegistration>,
) -> ApplicationResult<FrozenInitializerPlan> {
    let total = COLLECTED_INITIALIZERS.len().saturating_add(runtime.len());
    if total > MAX_INITIALIZERS {
        return Err(initialization_error(format!(
            "initializer count {total} exceeds the limit {MAX_INITIALIZERS}"
        )));
    }

    let mut names = HashMap::<Arc<str>, &'static str>::new();
    let mut entries = Vec::with_capacity(total);
    for descriptor in COLLECTED_INITIALIZERS {
        let spec = descriptor.to_spec();
        validate_spec(&spec)?;
        if mode == ApplicationMode::Batch && spec.kind == InitializerKind::Hosted {
            return Err(mode_error(&spec.name));
        }
        if let Some(first) = names.insert(spec.name.clone(), descriptor.source) {
            return Err(initialization_error(format!(
                "initializer `{}` is registered more than once (first at {first}, second at {})",
                spec.name, descriptor.source
            )));
        }
        entries.push(FrozenInitializer::Static {
            spec,
            factory: descriptor.factory,
        });
    }
    for registration in runtime {
        validate_spec(&registration.spec)?;
        if mode == ApplicationMode::Batch && registration.spec.kind == InitializerKind::Hosted {
            return Err(mode_error(&registration.spec.name));
        }
        if let Some(first) = names.insert(registration.spec.name.clone(), "runtime user hook") {
            return Err(initialization_error(format!(
                "initializer `{}` is registered more than once (first at {first}, second at runtime user hook)",
                registration.spec.name
            )));
        }
        entries.push(FrozenInitializer::Runtime {
            spec: registration.spec,
            initializer: registration.initializer,
        });
    }
    // 完全未声明的依赖是纯元数据错误，必须在任何条件工厂可产生副作用之前拒绝。
    // 已声明但工厂返回 None 的依赖只能在工厂完成后按“必须同时启用”合同裁决。
    for entry in &entries {
        for required in entry.spec().required_initializers() {
            if !names.contains_key(required) {
                return Err(initialization_error(format!(
                    "initializer `{}` requires undeclared initializer `{required}`",
                    entry.spec().name()
                )));
            }
        }
    }
    entries.sort_by(|left, right| {
        let left = left.spec();
        let right = right.spec();
        (left.order, left.name.as_ref()).cmp(&(right.order, right.name.as_ref()))
    });
    Ok(FrozenInitializerPlan { entries })
}

/// 业务作用：对实际启用集合执行稳定拓扑排序，依赖边优先于 order，名称消除全部平局。
///
/// 参数说明：
/// - `enabled`：静态条件工厂和运行时登记合并后的实际实例集合。
///
/// 返回：三轮屏障共用的唯一顺序；缺失依赖、自依赖或环按 fail-closed 拒绝。
pub(crate) fn order_enabled(
    enabled: Vec<EnabledInitializer>,
) -> ApplicationResult<Vec<EnabledInitializer>> {
    let mut by_name = BTreeMap::<Arc<str>, EnabledInitializer>::new();
    for initializer in enabled {
        let name = initializer.spec.name.clone();
        if by_name.insert(name.clone(), initializer).is_some() {
            return Err(initialization_error(format!(
                "enabled initializer `{name}` appeared more than once"
            )));
        }
    }

    let mut indegree = HashMap::<Arc<str>, usize>::new();
    let mut dependents = HashMap::<Arc<str>, Vec<Arc<str>>>::new();
    for (name, initializer) in &by_name {
        indegree.insert(name.clone(), initializer.spec.requires.len());
        for required in &initializer.spec.requires {
            if required == name {
                return Err(initialization_error(format!(
                    "initializer `{name}` cannot require itself"
                )));
            }
            if !by_name.contains_key(required) {
                return Err(initialization_error(format!(
                    "initializer `{name}` requires disabled or missing initializer `{required}`"
                )));
            }
            dependents
                .entry(required.clone())
                .or_default()
                .push(name.clone());
        }
    }
    for values in dependents.values_mut() {
        values.sort();
    }

    let mut ready = BTreeSet::<(i32, Arc<str>)>::new();
    for (name, initializer) in &by_name {
        if indegree.get(name).copied().unwrap_or_default() == 0 {
            ready.insert((initializer.spec.order, name.clone()));
        }
    }

    let mut ordered_names = Vec::with_capacity(by_name.len());
    while let Some((order, name)) = ready.pop_first() {
        let _ = order;
        ordered_names.push(name.clone());
        if let Some(next_items) = dependents.get(&name) {
            for next in next_items {
                let degree = indegree
                    .get_mut(next)
                    .expect("dependent initializer must have an indegree entry");
                *degree -= 1;
                if *degree == 0 {
                    let next_initializer = by_name
                        .get(next)
                        .expect("dependent initializer must exist in enabled map");
                    ready.insert((next_initializer.spec.order, next.clone()));
                }
            }
        }
    }

    if ordered_names.len() != by_name.len() {
        let mut cycle = indegree
            .into_iter()
            .filter_map(|(name, degree)| (degree != 0).then_some(name))
            .collect::<Vec<_>>();
        cycle.sort();
        return Err(initialization_error(format!(
            "initializer dependency graph contains a cycle involving: {}",
            cycle
                .iter()
                .map(AsRef::<str>::as_ref)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    Ok(ordered_names
        .into_iter()
        .map(|name| {
            by_name
                .remove(&name)
                .expect("topological name must resolve to an enabled initializer")
        })
        .collect())
}

/// Ready 前只保存任务工厂、不构造业务 future 的暂存项。
pub(crate) struct StagedInitializerTask {
    pub(crate) initializer: Arc<str>,
    pub(crate) name: Arc<str>,
    pub(crate) kind: TaskKind,
    pub(crate) factory: Box<dyn FnOnce(CancellationToken) -> ManagedTaskFuture + Send + 'static>,
}

/// 当前三阶段调用共享的受控上下文。
pub struct InitializationContext<'a> {
    pub(crate) application: &'a Application,
    pub(crate) initializer: Arc<str>,
    pub(crate) kind: InitializerKind,
    pub(crate) stage: InitializerStage,
    pub(crate) active: &'a mut crate::component::ActiveStack,
    pub(crate) staged_tasks: &'a mut Vec<StagedInitializerTask>,
    pub(crate) deadline: tokio::time::Instant,
    pub(crate) cancellation: CancellationToken,
}

impl<'a> InitializationContext<'a> {
    /// 业务作用：读取当前 initializer 的 canonical 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与上下文共同存活的名称借用。
    pub fn initializer_name(&self) -> &str {
        &self.initializer
    }

    /// 业务作用：读取当前执行的三阶段身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Before`、`Initialize` 或 `After`。
    pub fn stage(&self) -> InitializerStage {
        self.stage
    }

    /// 业务作用：读取完整启动流程共享的绝对截止时刻。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Runner 固定且不得由 initializer 延长的时刻。
    pub fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    /// 业务作用：读取启动取消令牌，使 initializer 内部可把子操作与统一停止边界关联。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只能观察或派生子令牌的取消句柄。
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// 业务作用：读取当前不可变配置快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：跨 await 保持同版本的共享快照。
    pub fn config(&self) -> Arc<crate::ConfigSnapshot> {
        self.application.config()
    }

    /// 业务作用：从当前配置快照反序列化指定业务段。
    ///
    /// 参数说明：
    /// - `path`：使用 `.` 或 `/` 分隔的配置节点路径。
    ///
    /// 返回：段存在且结构合法时返回拥有所有权的配置；否则返回初始化失败。
    pub fn config_section<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> ApplicationResult<T> {
        self.application.config_section(path)
    }

    /// 业务作用：借用此前组件或 initializer 已登记的无 qualifier 资源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：资源存在且仍允许借用时返回受生命周期约束的只读守卫。
    pub async fn resource<T>(&mut self) -> ApplicationResult<crate::ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        self.application.resource().await
    }

    /// 业务作用：借用此前组件或 initializer 已登记的具名资源。
    ///
    /// 参数说明：
    /// - `qualifier`：登记时使用的非空稳定名称。
    ///
    /// 返回：资源存在且仍允许借用时返回只读守卫。
    pub async fn named_resource<T>(
        &mut self,
        qualifier: impl AsRef<str>,
    ) -> ApplicationResult<crate::ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        self.application.named_resource(qualifier).await
    }

    /// 业务作用：登记由当前 initializer 拥有的普通资源，并建立独立逆序清理步骤。
    ///
    /// 参数说明：
    /// - `qualifier`：同类型多实例使用的可选稳定名称。
    /// - `value`：交给 Application 容器持有的线程安全资源。
    ///
    /// 返回：登记成功时资源可被后续 initializer 读取；冲突或封口后返回错误。
    pub fn register_resource<T>(
        &mut self,
        qualifier: Option<&str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        self.active
            .ensure_initializer_resources(self.initializer.clone());
        self.application.resources().register_initializer(
            self.initializer.clone(),
            qualifier,
            value,
        )
    }

    /// 业务作用：登记由当前 initializer 拥有且需要显式异步关闭的资源。
    ///
    /// 参数说明：
    /// - `qualifier`：同类型多实例使用的可选稳定名称。
    /// - `value`：实现 ManagedResource 且 Drop 非阻塞的资源。
    ///
    /// 返回：登记成功时进入 initializer 专属逆序清理链；失败时不发布资源。
    pub fn register_managed_resource<T>(
        &mut self,
        qualifier: Option<&str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: crate::ManagedResource,
    {
        self.active
            .ensure_initializer_resources(self.initializer.clone());
        self.application.resources().register_initializer_managed(
            self.initializer.clone(),
            qualifier,
            value,
        )
    }

    /// 业务作用：在副作用完整成功后立即把 initializer action 压入统一反向清理栈。
    ///
    /// 参数说明：
    /// - `action`：持有撤销副作用所需句柄的清理动作。
    ///
    /// 返回：无返回值；清理归因固定使用当前 initializer 名称。
    pub fn activate(&mut self, action: Box<dyn crate::ShutdownAction>) {
        self.active
            .activate_initializer(self.initializer.clone(), action);
    }

    /// 业务作用：为 hosted initializer 的长期能力登记一个动态就绪贡献项。
    ///
    /// 参数说明：
    /// - `name`：initializer 内唯一的 canonical 子项名称。
    /// - `policy`：连续失败/恢复阈值、stale 窗口和是否影响接流的策略。
    ///
    /// 返回：hosted Service 且名称唯一时返回贡献项更新句柄；模式、名称或封口冲突时返回错误。
    pub fn register_readiness(
        &mut self,
        name: impl Into<Arc<str>>,
        policy: crate::ReadinessPolicy,
    ) -> ApplicationResult<crate::ReadinessContributor> {
        if self.kind != InitializerKind::Hosted
            || self.application.info().mode() != ApplicationMode::Service
        {
            return Err(initialization_error(format!(
                "initializer `{}` cannot register readiness unless it is hosted in Service mode",
                self.initializer
            )));
        }
        let name = name.into();
        validate_canonical_name(&name, "initializer readiness")?;
        let full_name: Arc<str> = Arc::from(format!("initializer/{}/{}", self.initializer, name));
        self.application
            .register_initializer_readiness(self.initializer.clone(), full_name, policy)
    }

    /// 业务作用：暂存一个异常退出只记录报告、不主动终止 Service 的长期任务工厂。
    ///
    /// 参数说明：
    /// - `name`：当前 initializer 内唯一的 canonical 子任务名。
    /// - `task`：Ready 后才接收取消令牌并构造业务 future 的一次性工厂。
    ///
    /// 返回：hosted Service 且名称合法时登记成功；one-shot、Batch 或重名时返回稳定错误。
    pub fn stage_background<N, F, Fut>(&mut self, name: N, task: F) -> ApplicationResult<()>
    where
        N: Into<Arc<str>>,
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.stage_task(name.into(), TaskKind::Background, task)
    }

    /// 业务作用：暂存一个提前退出即触发 Service 故障停机的长期任务工厂。
    ///
    /// 参数说明：
    /// - `name`：当前 initializer 内唯一的 canonical 子任务名。
    /// - `task`：Ready 后才接收取消令牌并构造业务 future 的一次性工厂。
    ///
    /// 返回：hosted Service 且名称合法时登记成功；one-shot、Batch 或重名时返回稳定错误。
    pub fn stage_critical<N, F, Fut>(&mut self, name: N, task: F) -> ApplicationResult<()>
    where
        N: Into<Arc<str>>,
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.stage_task(name.into(), TaskKind::Critical, task)
    }

    /// 业务作用：校验 hosted 任务边界并把未构造的工厂写入 pending 列表。
    ///
    /// 参数说明：
    /// - `name`：initializer 内部子任务名称。
    /// - `kind`：后台或关键任务分类。
    /// - `task`：只允许 Ready 激活路径消费的一次性工厂。
    ///
    /// 返回：名称唯一且模式合法时成功；失败时工厂不会被 poll。
    fn stage_task<F, Fut>(
        &mut self,
        name: Arc<str>,
        kind: TaskKind,
        task: F,
    ) -> ApplicationResult<()>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if self.kind != InitializerKind::Hosted
            || self.application.info().mode() != ApplicationMode::Service
        {
            return Err(initialization_error(format!(
                "initializer `{}` cannot stage managed tasks unless it is hosted in Service mode",
                self.initializer
            )));
        }
        validate_canonical_name(&name, "initializer task")?;
        let full_name: Arc<str> = Arc::from(format!("initializer/{}/{}", self.initializer, name));
        if self
            .staged_tasks
            .iter()
            .any(|candidate| candidate.name == full_name)
        {
            return Err(initialization_error(format!(
                "managed task `{full_name}` is already staged"
            )));
        }
        self.staged_tasks.push(StagedInitializerTask {
            initializer: self.initializer.clone(),
            name: full_name,
            kind,
            factory: Box::new(move |token| Box::pin(task(token))),
        });
        Ok(())
    }
}

/// 业务作用：校验 initializer 规格的 canonical 身份、依赖上限和重复依赖。
///
/// 参数说明：
/// - `spec`：宏或运行时入口归一后的元数据。
///
/// 返回：局部元数据合法时成功；跨项缺失和环由实际启用集合排序时检查。
fn validate_spec(spec: &InitializerSpec) -> ApplicationResult<()> {
    validate_canonical_name(&spec.name, "initializer")?;
    if spec.requires.len() > MAX_INITIALIZER_REQUIRES {
        return Err(initialization_error(format!(
            "initializer `{}` has {} dependencies; the limit is {MAX_INITIALIZER_REQUIRES}",
            spec.name,
            spec.requires.len()
        )));
    }
    let mut seen = HashSet::new();
    for required in &spec.requires {
        validate_canonical_name(required, "initializer dependency")?;
        if required == &spec.name {
            return Err(initialization_error(format!(
                "initializer `{}` cannot require itself",
                spec.name
            )));
        }
        if !seen.insert(required.as_ref()) {
            return Err(initialization_error(format!(
                "initializer `{}` repeats dependency `{required}`",
                spec.name
            )));
        }
    }
    Ok(())
}

/// 业务作用：执行 initializer 与任务共用的 canonical 名称合同。
///
/// 参数说明：
/// - `name`：待校验名称。
/// - `kind`：只用于稳定错误归因的名称类别。
///
/// 返回：1..=128 字节且只含小写 ASCII、数字、`_`、`-`、`.` 时成功。
pub(crate) fn validate_canonical_name(name: &str, kind: &str) -> ApplicationResult<()> {
    if name.is_empty() || name.len() > 128 {
        return Err(initialization_error(format!(
            "{kind} name must contain between 1 and 128 bytes"
        )));
    }
    if !name.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
    }) {
        return Err(initialization_error(format!(
            "{kind} name must contain only lowercase ASCII letters, digits, `_`, `-`, or `.`"
        )));
    }
    Ok(())
}

/// 业务作用：构造 Application 初始化阶段的稳定错误形状。
///
/// 参数说明：
/// - `message`：不得包含配置值或业务秘密的摘要。
///
/// 返回：归因到 Application/Initialization 的错误。
pub(crate) fn initialization_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Application,
        ApplicationPhase::Initialization,
        message,
    )
}

/// 业务作用：构造 hosted initializer 进入 Batch 时的模式拒绝。
///
/// 参数说明：
/// - `name`：冻结计划中的 canonical initializer 身份。
///
/// 返回：带稳定模式分类源的 Application 错误。
fn mode_error(name: &Arc<str>) -> ApplicationError {
    ApplicationError::with_source(
        ComponentId::Application,
        ApplicationPhase::Initialization,
        format!("hosted initializer `{name}` is not allowed in Batch mode"),
        InitializerFailure::new(
            name.clone(),
            InitializerStage::Factory,
            InitializerFailureKind::ModeViolation,
        ),
    )
}
