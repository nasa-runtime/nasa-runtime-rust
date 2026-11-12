//! 来源中立的配置候选准备、安装和单点发布。

use crate::{
    reload::ConfigApplier, Application, ApplicationPhase, ApplicationResult, ComponentId,
    ConfigSource, ConfigView, ReloadStatus, ReloadTarget,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// 由单个配置来源驱动串行使用的候选准备和发布状态。
pub(crate) struct CandidatePublisher {
    /// 当前应用、取消信号与最终版本发布门禁的共享入口。
    application: Application,
    /// 本驱动负责校验和跟踪的组件身份。
    components: Vec<ComponentId>,
    /// 启动时固定的 application 原始段，在线变化只记录重启要求。
    pinned_application: Option<Value>,
    /// 构造时冻结的组件热应用入口，不在每次发布时重新发现。
    appliers: HashMap<ComponentId, Arc<dyn ConfigApplier>>,
    /// 各组件最后被标记成功的视图，用于检测仍未生效的差异。
    effective: HashMap<ComponentId, Arc<ConfigView>>,
    /// 启动时视图，用于比较冻结的资源名称、配置和秘密材料。
    initial: Arc<ConfigView>,
}

impl CandidatePublisher {
    /// 业务作用：冻结已声明目标和实际初始配置，供一个来源驱动串行处理后续候选。
    ///
    /// 参数说明：
    /// - `application`：提供当前视图、热应用入口和最终发布门禁的宿主句柄。
    /// - `components`：本驱动负责的已声明组件，用于段校验、变化检测和目标状态登记。
    /// - `pinned_application`：启动时固定的 application 原始段，用于识别只能重启生效的变更。
    ///
    /// 返回：以一次当前视图初始化各组件有效配置，并冻结 applier 映射与初始材料的发布器；不创建任务或改变配置。
    /// 该映射不会因后续登记自动更新，调用方应在启动配置已固定后构造并串行驱动。
    pub(crate) fn new(
        application: Application,
        components: Vec<ComponentId>,
        pinned_application: Option<Value>,
    ) -> Self {
        let current = application.config_view();
        let effective = components
            .iter()
            .map(|component| (*component, current.clone()))
            .collect();
        let appliers = application
            .config_appliers()
            .into_iter()
            .map(|applier| (applier.component(), applier))
            .collect();
        Self {
            application,
            components,
            pinned_application,
            appliers,
            effective,
            initial: current,
        }
    }

    /// 业务作用：完成有界候选的材料准备后，在既有发布门禁内安装并发布最终状态。
    ///
    /// 参数说明：
    /// - `merged`：尚未脱敏的完整合并候选，编码结果不得超过 1 MiB，声明组件与受管资源段须先通过校验。
    /// - `sources`：本次候选的远端配置文档来源，随准备后的快照保存。
    /// - `cancel`：来源驱动的取消信号；材料准备时可中断，最终同步安装前再次检查，安装开始后不再中断。
    ///
    /// 返回：候选及秘密均未变化时为 None；整批安装成功后才发布新版本，取消时拒绝候选。
    /// 任一校验、准备或来源复验失败均保留旧态；不支持热应用的变更拒绝整份候选。
    /// 安装失败时逆序恢复旧运行态且不发布；恢复不完整时撤销配置准入并请求宿主停机。
    pub(crate) async fn publish(
        &mut self,
        merged: Value,
        sources: Vec<ConfigSource>,
        cancel: &CancellationToken,
    ) -> ApplicationResult<Option<u64>> {
        // 候选过大时不进入资源准备；上限检查发生在序列化之后，不保证此前树及编码的峰值内存。
        if serde_json::to_vec(&merged).map_or(true, |bytes| {
            zeroize::Zeroizing::new(bytes).len() > 1024 * 1024
        }) {
            return Err(crate::ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "configuration candidate exceeds 1 MiB",
            ));
        }
        // 先对候选整帧做内置组件段校验：任一段非法都不发布，旧快照继续有效。
        // `application` 已在启动时冻结，后续按原始段比较并拒绝任何变化，不能热迁移进程身份。
        crate::sections::validate_declared_sections(
            &self.components,
            &merged,
            ApplicationPhase::Running,
        )?;
        crate::managed_adapters::validate(&merged, &self.components)?;
        // owner 冻结连接身份、注册目标和发送策略；拒绝整帧以避免新配置视图配上仍在运行的旧客户端。
        if self.components.contains(&ComponentId::Discovery)
            && merged.get("discovery") != self.initial.snapshot().value().get("discovery")
        {
            return Err(crate::ApplicationError::new(
                ComponentId::Discovery,
                ApplicationPhase::Running,
                "discovery configuration changes require application restart",
            ));
        }
        // 命名 HTTP 资源在启动时冻结；只允许已有名称的材料与参数按同一视图轮换。
        if enabled_names(merged.get("http_clients"))
            != enabled_names(self.initial.snapshot().value().get("http_clients"))
        {
            return Err(crate::ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "HTTP client names require application restart",
            ));
        }

        let current = self.application.config_view();
        let current_version = current.snapshot().version();
        // 在日志准备和安装之前完成所有 secret 与安全资源准备。
        let prepare =
            self.application
                .prepare_reloaded_config(current_version, merged.clone(), sources);
        let prepared = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(cancelled()),
            _ = self.application.cancellation_token().cancelled_owned() => return Err(cancelled()),
            result = prepare => result?,
        };
        let secret_changes = prepared.secrets.changed_ids(current.secrets());
        // 材料准备可能等待外部 I/O；同值候选也必须先复验来源才能切换观察记录。
        if let Some(source) = self.application.strict_source() {
            crate::read_config_candidate(move || source.revalidate()).await??;
        }
        // 复读期间可能进入停机；同值候选也不能借无发布分支更新已失权的来源观察。
        if cancel.is_cancelled() || self.application.cancellation_token().is_cancelled() {
            return Err(cancelled());
        }
        if crate::secret::candidate_fingerprint(&merged) == current.candidate_fingerprint()
            && secret_changes.is_empty()
        {
            // 来源已复验且内容、秘密均未变化，不推进期望版本，也不重复安装组件。
            return Ok(None);
        }
        let next_version = prepared.snapshot.version();
        let changed: std::collections::HashSet<ComponentId> = self
            .components
            .iter()
            .copied()
            .filter(|component| {
                let effective = self.effective.get(component).unwrap_or(&current);
                let changed_material = prepared.secrets.changed_ids(effective.secrets());
                crate::sections::sections_changed(
                    *component,
                    effective.snapshot().value(),
                    prepared.snapshot.value(),
                ) || crate::sections::secrets_affect_component(
                    *component,
                    &merged,
                    &changed_material,
                ) || crate::sections::secrets_affect_component(
                    *component,
                    effective.snapshot().value(),
                    &changed_material,
                )
            })
            .collect();
        let fixed_material_changes = prepared.secrets.changed_ids(self.initial.secrets());
        let statuses = self.collect_statuses(
            &current,
            &merged,
            next_version,
            &changed,
            &fixed_material_changes,
        );
        // 不能把要求重启的字段与可热更字段混合发布，否则同一配置版本会表达两套运行状态。
        if statuses
            .values()
            .any(|status| matches!(status.state, crate::ReloadState::RestartRequired { .. }))
        {
            return Err(crate::ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "configuration candidate changes resources that require restart",
            ));
        }
        let appliers: Vec<_> = self
            .components
            .iter()
            .filter(|component| changed.contains(component))
            .filter_map(|component| self.appliers.get(component).cloned())
            .collect();
        let source = self.application.strict_source();
        // 日志文件打开与来源正文复读都可能阻塞；后台只准备资源，不能取得配置安装或发布权威。
        let mut applications = crate::read_config_candidate(move || {
            let mut applications = Vec::with_capacity(appliers.len());
            for applier in appliers {
                // 所有目标先准备完成，任一失败都不调用 install，已准备候选也在当前阻塞线程回收。
                applications.push(applier.prepare(&merged)?);
            }
            // 准备期间来源可能改变；复读仍在发布锁外，拒绝时保留全部旧运行态。
            if let Some(source) = source {
                source.revalidate()?;
            }
            Ok::<_, crate::ApplicationError>(applications)
        })
        .await??;
        let version =
            self.application
                .publish_prepared_config(current_version, prepared, cancel, || {
                    if let Err(error) = crate::apply_config_transaction(&mut applications) {
                        if !error.rollback_complete() {
                            // 无法证明旧态完整恢复时禁止继续接收配置，交由宿主退出，不能假报旧配置仍安全可用。
                            self.application.cancellation_token().cancel();
                            let _ = self.application.shutdown();
                        }
                        return Err(crate::ApplicationError::with_source(
                            ComponentId::Config,
                            ApplicationPhase::Running,
                            "configuration transaction rejected",
                            error,
                        ));
                    }
                    Ok(statuses)
                });
        if version.is_ok() {
            // 发布成功后立即同步本驱动的有效视图，不能在后续回收等待期间保留旧的应用状态记录。
            let installed = self.application.config_view();
            for component in &self.components {
                if installed
                    .reload_statuses()
                    .get(&ReloadTarget::Component(*component))
                    .is_some_and(|status| matches!(status.state, crate::ReloadState::Applied))
                {
                    self.effective.insert(*component, installed.clone());
                }
            }
        }
        // 文件 writer 回收可能等待刷盘线程；事务结束后只移交候选持有的退出资源，不切换当前输出。
        crate::read_config_candidate(move || drop(applications)).await?;
        version.map(Some)
    }

    /// 业务作用：在准备与安装前确定整批配置是否支持热应用，并构造成功提交后使用的状态表。
    ///
    /// 参数说明：
    /// - `current`：安装前的已发布视图，用于保留目标最后成功版本。
    /// - `candidate`：已通过校验的原始候选，供比对固定引导段及受管资源设置。
    /// - `next_version`：当前候选的期望版本，仅成功应用或无需变化的目标才能推进到它。
    /// - `changed`：相对各组件最后有效配置或依赖材料发生变化的目标集合。
    /// - `fixed_material_changes`：必须重启才能生效的固定材料变化集合。
    ///
    /// 返回：可热应用的目标预记成功，固定段变化标记要求重启；调用方必须先拒绝重启项，再准备并整体提交。
    /// 本方法不安装资源或发布状态；状态表只有全部安装成功后才对外可见。
    fn collect_statuses(
        &self,
        current: &ConfigView,
        candidate: &Value,
        next_version: u64,
        changed: &std::collections::HashSet<ComponentId>,
        fixed_material_changes: &std::collections::BTreeSet<Arc<str>>,
    ) -> std::collections::HashMap<ReloadTarget, ReloadStatus> {
        let mut statuses = std::collections::HashMap::new();

        let application_status = if candidate.get("application") == self.pinned_application.as_ref()
        {
            ReloadStatus::applied(next_version)
        } else {
            ReloadStatus::restart_required(
                applied_version_of(current, &ReloadTarget::Application),
                "remote overlays changed bootstrap-only `application.*`",
            )
        };
        statuses.insert(ReloadTarget::Application, application_status);
        for section in [
            "mapper_cache",
            "grouped_caches",
            "idempotency_stores",
            "audit_sinks",
            "object_stores",
            "rest_clients",
            "redis_leaders",
            "redis_subscriptions",
            "redis_streams",
            "redis_proxies",
            "redis_pipelines",
            "ws_clients",
            "hystrix",
            "schema_registries",
        ] {
            let target = ReloadTarget::Managed(Arc::from(section));
            let initial = self.initial.snapshot().value().get(section);
            let desired = candidate.get(section);
            let secret_dependent = initial
                .is_some_and(|value| references_changed_secret(value, fixed_material_changes));
            let status = if initial == desired && !secret_dependent {
                ReloadStatus::applied(next_version)
            } else {
                ReloadStatus::restart_required(
                    applied_version_of(current, &target),
                    "managed resource configuration requires restart",
                )
            };
            statuses.insert(target, status);
        }

        for component in &self.components {
            let target = ReloadTarget::Component(*component);
            // 无变化或存在当前构建可用的应用器才允许接纳候选，否则必须要求重启，避免假报生效。
            let status = if !changed.contains(component) || self.appliers.contains_key(component) {
                ReloadStatus::applied(next_version)
            } else {
                ReloadStatus::restart_required(
                    applied_version_of(current, &target),
                    "component configuration changed but this runtime build cannot hot-apply it",
                )
            };
            statuses.insert(target, status);
        }
        statuses
    }
}

/// 业务作用：将停机期间的候选准备标为拒绝，避免来源驱动把取消误认成同值成功并更新观察。
///
/// 参数说明：无。
///
/// 返回：不包含来源正文的配置取消错误，当前配置及观察保持有效。
fn cancelled() -> crate::ApplicationError {
    crate::ApplicationError::new(
        ComponentId::Config,
        ApplicationPhase::Running,
        "configuration candidate cancelled before publication",
    )
}

/// 业务作用：提取显式启用的资源名称，供调用方识别热刷新是否改变已冻结的命名目录。
///
/// 参数说明：
/// - `value`：配置中按名称索引的资源计划段，可能缺失或不为对象。
///
/// 返回：只包含 enabled 恰为布尔 true 的名称集合；缺失、类型不符或未启用时不计入，不自行校验整段或执行拒绝动作。
fn enabled_names(value: Option<&Value>) -> std::collections::BTreeSet<String> {
    value
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|plans| plans.iter())
        .filter(|(_, plan)| plan.get("enabled").and_then(Value::as_bool) == Some(true))
        .map(|(name, _)| name.clone())
        .collect()
}

/// 业务作用：判断固定装配的子能力是否引用了已改变的 secret 身份。
///
/// 参数说明：
/// - `value`：待检查的配置值树，递归查看字符串、数组元素和对象的值，不检查对象键。
/// - `changed`：相对固定初始材料发生变化的秘密 ID 集合。
///
/// 返回：任一字符串以 secret:// 开头且剩余部分精确命中变化 ID 时为 true；否则为 false，不解析或读取秘密。
/// 仅提供依赖事实，是否要求重启由调用方写入目标状态。
fn references_changed_secret(
    value: &Value,
    changed: &std::collections::BTreeSet<Arc<str>>,
) -> bool {
    match value {
        Value::String(value) => value
            .strip_prefix("secret://")
            .is_some_and(|id| changed.contains(id)),
        Value::Array(values) => values
            .iter()
            .any(|value| references_changed_secret(value, changed)),
        Value::Object(values) => values
            .values()
            .any(|value| references_changed_secret(value, changed)),
        _ => false,
    }
}

/// 业务作用：读取某个目标在当前视图中最后一次成功 apply 的版本。
///
/// 未出现在当前状态表中的目标按初始版本 1 处理，保证 `RestartRequired` 一定携带可比较的版本号。
///
/// 参数说明：
///
/// - `current`：当前已发布的同版本配置视图。
/// - `target`：需要查询的配置应用目标。
///
/// 返回：状态表中的最后成功版本，缺失时固定返回 1；不把当前期望版本当作已应用，也不改写目标状态。
fn applied_version_of(current: &ConfigView, target: &ReloadTarget) -> u64 {
    current
        .reload_statuses()
        .get(target)
        .map(|status| status.applied_version)
        .unwrap_or(1)
}
