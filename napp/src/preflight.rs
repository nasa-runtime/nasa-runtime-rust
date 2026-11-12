use std::{collections::HashMap, sync::Arc, time::Duration};

use naml::YmlLoader;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    runner::MAX_LIFECYCLE_TIMEOUT, ApplicationError, ApplicationInfo, ApplicationMode,
    ApplicationPhase, ApplicationResult, ApplicationSpec, ComponentId, ConfigView, ReloadStatus,
    ReloadTarget,
};

/// 本地配置允许声明的运行模式设置，auto 会在创建 runtime 前解析完成。
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ApplicationModeSetting {
    /// 按组件及入口声明在创建运行时之前选择模式。
    #[default]
    Auto,
    /// 持续服务模式，须满足 Service 模式的组件与入口约束。
    Service,
    /// 有终止结果的批处理模式，须满足 Batch 模式约束。
    Batch,
}

/// 同步 preflight 唯一认可的 `application.*` 强类型设置。
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
struct ApplicationSettings {
    /// 显式应用名，省略时采用 ApplicationSpec 的默认名称。
    name: Option<String>,
    /// 自动推导或显式运行模式，在线配置不能改写已采用的启动模式。
    mode: ApplicationModeSetting,
    /// 可选工作线程数，必须非零；缺省交由运行时构造入口决定。
    worker_threads: Option<usize>,
    /// 全部启动阶段共享的预算毫秒数，必须非零且不超过 365 天。
    startup_timeout_ms: u64,
    /// 停机共享预算毫秒数，显式值还须满足所声明组件的最低要求。
    shutdown_timeout_ms: u64,
}

impl Default for ApplicationSettings {
    /// 业务作用：返回空 YAML 文档对应的固定设置。
    ///
    /// 参数说明：无。
    ///
    /// 返回：名称与工作线程数未指定、模式为自动、启动预算 30 秒、停机预算 15 秒的纯配置。
    /// 实际名称和模式随后结合 ApplicationSpec 解析，声明组件还可能提高默认停机预算。
    fn default() -> Self {
        Self {
            name: None,
            mode: ApplicationModeSetting::Auto,
            worker_threads: None,
            startup_timeout_ms: 30_000,
            shutdown_timeout_ms: 15_000,
        }
    }
}

/// 同步配置读取和校验形成的 runtime 唯一输入。
pub(crate) struct Preflight {
    /// 严格加载时保留的来源身份和复验入口，普通加载时为空。
    pub(crate) source: Option<Arc<crate::config_source::StrictSource>>,
    /// 校验完成的应用名称、profile 和实际运行模式。
    pub(crate) info: ApplicationInfo,
    /// 运行时构造时采用的显式线程数，None 保留构造入口的默认决策。
    pub(crate) worker_threads: Option<usize>,
    /// 生命周期共享的启动预算，不为每个组件重新计时。
    pub(crate) startup_timeout: Duration,
    /// 已考虑声明组件最低要求的停机总预算。
    pub(crate) shutdown_timeout: Duration,
    /// 配置、秘密和初始目标状态同属版本 1 的视图，后续引导可用最终来源替换。
    pub(crate) initial_config: Arc<ConfigView>,
    /// 同步预读到的整个原始 `application.*`；bootstrap-only 冲突判定的唯一基准。
    ///
    /// 保存的是**原始 section** 而不是已反序列化的 `ApplicationSettings`，这样远端新增或拼错的未知字段
    /// 也会先被判定为 bootstrap-only 冲突，而不是被 serde 错误遮蔽。
    pub(crate) pinned_application: Option<Value>,
}

impl Preflight {
    /// 业务作用：使用标准必需路径和 profile 环境语义执行同步 preflight。
    ///
    /// 参数说明：
    ///
    /// - `spec`：提供组件声明和编译期缺省名的静态应用描述。
    ///
    /// 返回：使用显式严格加载器、发现组件所需的严格默认加载器，或标准 YmlLoader 形成启动输入。
    /// 加载器构造、来源准备或前置校验失败即拒绝，不创建 Tokio runtime 或启动业务组件。
    pub(crate) fn load_standard(spec: &ApplicationSpec) -> ApplicationResult<Self> {
        #[cfg(feature = "discovery-runtime")]
        if spec.config_loader.is_none() && spec.components().contains(&ComponentId::Discovery) {
            // 多连接名称具有权限含义，必须在原始来源阶段拒绝重复键，不能依赖已覆盖的 Value 检查。
            let loader = naml::strict::ConfigLoader::standard()
                .map_err(|_| settings_error("strict discovery configuration rejected"))?;
            let profile = loader
                .selected_profile()
                .map_err(|_| settings_error("invalid strict profile"))?;
            let (source, tree) = crate::config_source::StrictSource::start(loader, spec)?;
            let mut preflight = Self::from_tree(spec, tree, profile)?;
            preflight.source = Some(source);
            return Ok(preflight);
        }
        if let Some(factory) = spec.config_loader {
            let loader =
                factory().map_err(|_| settings_error("strict configuration factory rejected"))?;
            let profile = loader
                .selected_profile()
                .map_err(|_| settings_error("invalid strict profile"))?;
            let (source, tree) = crate::config_source::StrictSource::start(loader, spec)?;
            let mut preflight = Self::from_tree(spec, tree, profile)?;
            preflight.source = Some(source);
            return Ok(preflight);
        }
        Self::load(spec, &YmlLoader::standard(), standard_profile())
    }

    /// 业务作用：在运行时创建前按指定加载器读取本地配置并形成启动输入。
    ///
    /// 参数说明：
    ///
    /// - `spec`：提供组件声明、auto 模式输入和缺省名的静态描述。
    /// - `loader`：负责 base、profile、环境合并和占位符解析的配置加载器。
    /// - `profile`：已经按加载器语义解析的显式 profile 名称。
    ///
    /// 返回：应用声明、绑定和配置合法时形成 Preflight；读取或校验失败返回对应错误。
    /// profile 用于应用身份记录，不反向改变 loader 的加载选择，调用方必须保证两者一致。
    pub(crate) fn load(
        spec: &ApplicationSpec,
        loader: &YmlLoader,
        profile: Option<String>,
    ) -> ApplicationResult<Self> {
        spec.validate()?;
        spec.validate_runtime_bindings()?;
        let tree = loader.load_tree().map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Config,
                ApplicationPhase::Bootstrap,
                "required local application configuration could not be loaded",
                error,
            )
        })?;
        Self::from_tree(spec, tree, profile)
    }

    /// 业务作用：将完整本地配置或受限引导投影转换为 runtime 启动参数。
    ///
    /// 参数说明：
    /// - `spec`：提供声明组件、默认名称、模式选择和运行时绑定约束。
    /// - `tree`：已由来源准备的本地树或受限引导投影，原始 application 段将冻结为首拉比较基准。
    /// - `profile`：来源已选择的 profile，作为应用身份的一部分记录。
    ///
    /// 返回：声明、运行模式、预算和初始秘密解析通过后形成版本为 1 的视图及 runtime 参数。
    /// 显式停机预算不足以满足已声明 gRPC 组件时拒绝；未显式填写则可提高默认预算。
    /// 初始 Applied 状态表示配置初始化，不证明组件已经启动或远端依赖可用。
    fn from_tree(
        spec: &ApplicationSpec,
        tree: Value,
        profile: Option<String>,
    ) -> ApplicationResult<Self> {
        spec.validate()?;
        spec.validate_runtime_bindings()?;
        let pinned_application = tree.get("application").cloned();
        let settings_value = pinned_application
            .clone()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let settings: ApplicationSettings =
            naml::strict::bind(settings_value).map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Config,
                    ApplicationPhase::Bootstrap,
                    "invalid local `application` configuration",
                    error,
                )
            })?;
        validate_settings(&settings)?;
        let shutdown_timeout = Duration::from_millis(settings.shutdown_timeout_ms);
        #[cfg(feature = "grpc")]
        let shutdown_timeout = {
            let shutdown_timeout_explicit = pinned_application
                .as_ref()
                .and_then(Value::as_object)
                .is_some_and(|section| section.contains_key("shutdown_timeout_ms"));
            let mut resolved = shutdown_timeout;
            if spec.components().contains(&ComponentId::Grpc) {
                let minimum = crate::grpc::minimum_shutdown_timeout(
                    &tree,
                    spec.components().contains(&ComponentId::NacosDiscovery)
                        || spec.components().contains(&ComponentId::Discovery),
                )?;
                if shutdown_timeout_explicit && resolved < minimum {
                    return Err(settings_error(format!(
                        "application.shutdown_timeout_ms is below the declared component shutdown requirement of {} ms",
                        minimum.as_millis()
                    )));
                }
                if !shutdown_timeout_explicit {
                    resolved = resolved.max(minimum);
                }
            }
            resolved
        };

        let mode = match settings.mode {
            ApplicationModeSetting::Auto => spec.resolve_auto_mode(),
            ApplicationModeSetting::Service => ApplicationMode::Service,
            ApplicationModeSetting::Batch => ApplicationMode::Batch,
        };
        spec.validate_mode(mode)?;

        let name = settings
            .name
            .as_deref()
            .map(str::trim)
            .unwrap_or_else(|| spec.default_name().trim());
        let info = ApplicationInfo::new(name, profile.as_deref(), mode)?;

        // 初始视图同时含 Application 与所有已声明组件的 version=1 状态，
        // 使 `reload_status()` 对每个已声明目标从初始化完成起就有确定值。
        let mut reload_statuses = spec
            .components()
            .iter()
            .copied()
            .map(|component| (ReloadTarget::Component(component), ReloadStatus::applied(1)))
            .collect::<HashMap<_, _>>();
        reload_statuses.insert(ReloadTarget::Application, ReloadStatus::applied(1));

        // 本地启动没有“远端全量重拉”，来源清单为空；File 哨兵会让订阅者误判本轮含可重拉文档。
        // secret 在脱敏前解析:发布树里 fragment 已 `<redacted>`,material 只进视图 secrets;
        // 结构非法/解析失败在 Bootstrap 即 fail-closed。
        let initial_config = crate::config::resolve_view(1, tree, Vec::new(), reload_statuses)
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Config,
                    ApplicationPhase::Bootstrap,
                    "secret resolution failed while building the initial config view",
                    error,
                )
            })?;

        Ok(Self {
            source: None,
            info,
            worker_threads: settings.worker_threads,
            startup_timeout: Duration::from_millis(settings.startup_timeout_ms),
            shutdown_timeout,
            initial_config,
            pinned_application,
        })
    }
}

/// 业务作用：校验会影响线程数和 deadline 的本地设置。
///
/// 参数说明：
///
/// - `settings`：已经通过未知字段拒绝规则的 ApplicationSettings。
///
/// 返回：显式名称非空白、显式线程数非零，且两项期限均在零到 365 天的开闭区间内时成功。
/// 任一约束不满足返回配置错误；不校验线程数是否适合机器资源，也不在此解析自动模式。
fn validate_settings(settings: &ApplicationSettings) -> ApplicationResult<()> {
    if settings
        .name
        .as_deref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(settings_error("application.name cannot be empty"));
    }
    if settings.worker_threads == Some(0) {
        return Err(settings_error(
            "application.worker_threads must be greater than zero",
        ));
    }
    if settings.startup_timeout_ms == 0 {
        return Err(settings_error(
            "application.startup_timeout_ms must be greater than zero",
        ));
    }
    if Duration::from_millis(settings.startup_timeout_ms) > MAX_LIFECYCLE_TIMEOUT {
        return Err(settings_error(
            "application.startup_timeout_ms cannot exceed 365 days",
        ));
    }
    if settings.shutdown_timeout_ms == 0 {
        return Err(settings_error(
            "application.shutdown_timeout_ms must be greater than zero",
        ));
    }
    if Duration::from_millis(settings.shutdown_timeout_ms) > MAX_LIFECYCLE_TIMEOUT {
        return Err(settings_error(
            "application.shutdown_timeout_ms cannot exceed 365 days",
        ));
    }
    Ok(())
}

/// 业务作用：创建本地 ApplicationSettings 的 Bootstrap 错误。
///
/// 参数说明：
///
/// - `message`：不包含配置原文的稳定校验摘要。
///
/// 返回：归属于 Config 组件和 Bootstrap 阶段的错误，不附加来源链，也不自动清洗输入摘要。
fn settings_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Config, ApplicationPhase::Bootstrap, message)
}

/// 业务作用：按标准加载器语义读取显式非空 profile。
///
/// 参数说明：无。
///
/// 返回：APP_PROFILE 去除首尾空白后的非空值；未设置、非 Unicode 或全空白时返回 None。
/// 不读取文件、不验证该 profile 文件存在，也不修改环境变量。
fn standard_profile() -> Option<String> {
    std::env::var("APP_PROFILE")
        .ok()
        .map(|profile| profile.trim().to_owned())
        .filter(|profile| !profile.is_empty())
}
