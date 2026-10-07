//! 来源中立的配置候选准备、安装和单点发布。

use crate::{
    reload::ConfigApplier, Application, ApplicationPhase, ApplicationResult, ComponentId,
    ConfigSource, ConfigView, ReloadStatus, ReloadTarget,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) struct CandidatePublisher {
    application: Application,
    components: Vec<ComponentId>,
    pinned_application: Option<Value>,
    appliers: HashMap<ComponentId, Arc<dyn ConfigApplier>>,
    effective: HashMap<ComponentId, Arc<ConfigView>>,
    initial: Arc<ConfigView>,
}

impl CandidatePublisher {
    /// 业务作用：冻结已声明目标和实际初始配置，供一个来源驱动串行处理后续候选。
    /// 参数说明：`application` 为宿主；`components` 为目标集合；`pinned_application` 为引导配置。
    /// 返回：未创建后台任务的候选发布器，实际投影与期望快照分别保存。
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
    /// 参数说明：`merged` 为完整原始候选；`sources` 为来源摘要；`cancel` 在同步安装前生效。
    /// 返回：无变化时 None；成功返回新代次；准备或权威校验失败保留旧视图。
    pub(crate) async fn publish(
        &mut self,
        merged: Value,
        sources: Vec<ConfigSource>,
        cancel: &CancellationToken,
    ) -> ApplicationResult<Option<u64>> {
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
        // `application` 段被有意排除——它 bootstrap-only，只做原始 section 比较并记 RestartRequired，
        // 否则远端新增/拼错 application 字段会因 deny_unknown_fields 把整帧候选一起否掉。
        crate::sections::validate_declared_sections(
            &self.components,
            &merged,
            ApplicationPhase::Running,
        )?;
        crate::managed_adapters::validate(&merged, &self.components)?;
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
            _ = cancel.cancelled() => return Ok(None),
            _ = self.application.cancellation_token().cancelled_owned() => return Ok(None),
            result = prepare => result?,
        };
        let secret_changes = prepared.secrets.changed_ids(current.secrets());
        // 材料准备可能等待外部 I/O；同值候选也必须先复验来源才能切换观察记录。
        if let Some(source) = self.application.strict_source() {
            source.revalidate()?;
        }
        if crate::secret::candidate_fingerprint(&merged) == current.candidate_fingerprint()
            && secret_changes.is_empty()
        {
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
        let mut applications = std::collections::HashMap::new();
        for component in &changed {
            if let Some(applier) = self.appliers.get(component) {
                applications.insert(*component, applier.prepare(&merged));
            }
        }
        let fixed_material_changes = prepared.secrets.changed_ids(self.initial.secrets());
        // 组件预备资源仍在发布锁外；已知来源变化时丢弃整批预备资源并保持旧视图。
        if let Some(source) = self.application.strict_source() {
            source.revalidate()?;
        }
        let version =
            self.application
                .publish_prepared_config(current_version, prepared, cancel, || {
                    self.install_and_collect_statuses(
                        &current,
                        &merged,
                        next_version,
                        &changed,
                        &mut applications,
                        &fixed_material_changes,
                    )
                });
        // 候选仍拥有被替换或未安装的日志 guard；发布锁已经释放，回收不阻塞订阅创建。
        drop(applications);
        let version = version?;
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
        Ok(Some(version))
    }

    /// 业务作用：安装已准备资源，并保留尚未生效目标的状态及最后成功版本。
    /// 参数说明：`current` 为当前视图；`candidate` 为原始候选；`next_version` 为本次代次；
    /// `changed` 为配置或依赖材料变化目标；`applications` 持有准备结果及锁外回收责任。
    /// 返回：根据真实安装结果生成的状态表，不执行准备 I/O 或日志输出。
    fn install_and_collect_statuses(
        &self,
        current: &ConfigView,
        candidate: &Value,
        next_version: u64,
        changed: &std::collections::HashSet<ComponentId>,
        applications: &mut std::collections::HashMap<
            ComponentId,
            ApplicationResult<Box<dyn crate::reload::PreparedConfigApply>>,
        >,
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
            let status = if !changed.contains(component) {
                ReloadStatus::applied(next_version)
            } else if let Some(prepared) = applications.get_mut(component) {
                match prepared {
                    Ok(prepared) => match prepared.install() {
                        Ok(()) => ReloadStatus::applied(next_version),
                        Err(error) => ReloadStatus::apply_failed(
                            applied_version_of(current, &target),
                            crate::report::redact(&crate::report::error_chain(&error)),
                        ),
                    },
                    Err(error) => ReloadStatus::apply_failed(
                        applied_version_of(current, &target),
                        crate::report::redact(&crate::report::error_chain(error)),
                    ),
                }
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

/// 业务作用：提取启动期资源身份，禁止热刷新扩大或撤销已冻结的命名目录。
/// 参数说明：`value` 为命名计划段。
/// 返回：仅包含显式启用名称的有序集合。
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
/// 参数说明：`value` 为脱敏配置结构；`changed` 为材料变化集合。
/// 返回：存在依赖时要求重启，避免把未轮换客户端标成已生效。
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
/// # 参数
///
/// - `current`：当前已发布的同版本配置视图。
/// - `target`：需要查询的配置应用目标。
fn applied_version_of(current: &ConfigView, target: &ReloadTarget) -> u64 {
    current
        .reload_statuses()
        .get(target)
        .map(|status| status.applied_version)
        .unwrap_or(1)
}
