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
    #[default]
    Auto,
    Service,
    Batch,
}

/// 同步 preflight 唯一认可的 `application.*` 强类型设置。
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
struct ApplicationSettings {
    name: Option<String>,
    mode: ApplicationModeSetting,
    worker_threads: Option<usize>,
    startup_timeout_ms: u64,
    shutdown_timeout_ms: u64,
}

impl Default for ApplicationSettings {
    /// 返回空 YAML 文档对应的固定设置。
    ///
    /// # 参数
    ///
    /// 本方法无参数；默认值与设计文档和配置示例保持一致。
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
    pub(crate) info: ApplicationInfo,
    pub(crate) worker_threads: Option<usize>,
    pub(crate) startup_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) initial_config: Arc<ConfigView>,
    /// 同步预读到的整个原始 `application.*`；bootstrap-only 冲突判定的唯一基准。
    ///
    /// 保存的是**原始 section** 而不是已反序列化的 `ApplicationSettings`，这样远端新增或拼错的未知字段
    /// 也会先被判定为 bootstrap-only 冲突，而不是被 serde 错误遮蔽。
    pub(crate) pinned_application: Option<Value>,
}

impl Preflight {
    /// 使用标准必需路径和 profile 环境语义执行同步 preflight。
    ///
    /// # 参数
    ///
    /// - `spec`：提供组件声明和编译期缺省名的静态应用描述。
    pub(crate) fn load_standard(spec: &ApplicationSpec) -> ApplicationResult<Self> {
        Self::load(spec, &YmlLoader::standard(), standard_profile())
    }

    /// 使用指定加载器执行可重复验证的同步 preflight。
    ///
    /// # 参数
    ///
    /// - `spec`：提供组件声明、auto 模式输入和缺省名的静态描述。
    /// - `loader`：负责 base、profile、环境合并和占位符解析的配置加载器。
    /// - `profile`：已经按加载器语义解析的显式 profile 名称。
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
        let pinned_application = tree.get("application").cloned();
        let settings_value = pinned_application
            .clone()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let settings: ApplicationSettings =
            serde_json::from_value(settings_value).map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Config,
                    ApplicationPhase::Bootstrap,
                    "invalid local `application` configuration",
                    error,
                )
            })?;
        validate_settings(&settings)?;

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
        // 使 `reload_status()` 对已声明目标从第一版起就有确定值。
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
            info,
            worker_threads: settings.worker_threads,
            startup_timeout: Duration::from_millis(settings.startup_timeout_ms),
            shutdown_timeout: Duration::from_millis(settings.shutdown_timeout_ms),
            initial_config,
            pinned_application,
        })
    }
}

/// 校验会影响线程数和 deadline 的本地设置。
///
/// # 参数
///
/// - `settings`：已经通过未知字段拒绝规则的 ApplicationSettings。
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

/// 创建本地 ApplicationSettings 的 Bootstrap 错误。
///
/// # 参数
///
/// - `message`：不包含配置原文的稳定校验摘要。
fn settings_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Config, ApplicationPhase::Bootstrap, message)
}

/// 按标准加载器语义读取显式非空 profile。
///
/// # 参数
///
/// 本函数无参数；未设置或空白环境值返回 `None`。
fn standard_profile() -> Option<String> {
    std::env::var("APP_PROFILE")
        .ok()
        .map(|profile| profile.trim().to_owned())
        .filter(|profile| !profile.is_empty())
}
