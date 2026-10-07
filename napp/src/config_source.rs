//! 单个应用冻结的严格来源权威，启动与重载复用同一环境和本地路径。

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ApplicationSpec, ComponentId};
use naml::{
    strict::{CheckReport, ConfigLoader, ConfigPath, LoadedConfig, PreparedLoad, WatchPlan},
    YmlImport,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) struct StrictSource {
    #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
    pub(crate) loader: ConfigLoader,
    #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
    imports: Vec<YmlImport>,
    #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
    authority: Value,
    initial: Mutex<Option<PreparedLoad>>,
    watch: Mutex<WatchPlan>,
    observation: Mutex<ObservationState>,
}

/// 严格来源的独立观察序号与安全规模摘要，不代表所有组件已应用期望快照。
#[derive(Clone, Debug)]
pub struct ConfigObservation {
    /// 已受理来源身份变化时推进的观察序号，与业务配置版本独立。
    pub revision: u64,
    /// 最近受理候选的安全规模摘要，不包含路径、正文或凭据。
    pub report: CheckReport,
}

#[derive(Default)]
struct ObservationState {
    pending: Option<([u8; 32], CheckReport)>,
    fingerprint: Option<[u8; 32]>,
    accepted: Option<ConfigObservation>,
}

impl StrictSource {
    /// 业务作用：在 runtime 创建前固定环境和来源权限，配置中心启用时仅求值引导依赖。
    /// 参数说明：`loader` 是业务显式工厂的产物；`spec` 声明能够连接配置中心的组件。
    /// 返回：初始视图及唯一来源 owner；远端业务表达式延后到完整装配。
    pub(crate) fn start(
        loader: ConfigLoader,
        spec: &ApplicationSpec,
    ) -> ApplicationResult<(Arc<Self>, Value)> {
        let plan = prepare(&loader)?;
        #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
        let authority = sections(&plan, &["yml", "nacos", "secret_providers"])?;
        let remote = plan
            .imports()
            .iter()
            .any(|import| matches!(import, YmlImport::Nacos(_)));
        if remote && !spec.components().contains(&ComponentId::NacosConfig) {
            return Err(error(
                "strict remote imports require the nacos-config component",
            ));
        }
        let owner = Arc::new(Self {
            #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
            imports: plan.imports().to_vec(),
            #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
            loader,
            #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
            authority,
            initial: Mutex::new(None),
            watch: Mutex::new(WatchPlan::default()),
            observation: Mutex::new(ObservationState::default()),
        });
        let tree = if remote {
            let tree = sections(
                &plan,
                &[
                    "application",
                    "nacos",
                    "yml",
                    "secret_providers",
                    "secrets",
                    "config_watch",
                ],
            )?;
            *owner
                .initial
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(plan);
            tree
        } else {
            owner.accept(
                plan.finish(&BTreeMap::new())
                    .map_err(|_| error("strict local configuration rejected"))?,
            )
        };
        owner.commit_observation();
        Ok((owner, tree))
    }

    /// 业务作用：取出同步预读文档用于唯一首拉，随后每轮重读并复验冻结来源权限。
    /// 参数说明：`initial` 表示配置中心首拉阶段。
    /// 返回：没有改变订阅清单、模式或信任根的候选计划。
    #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
    pub(crate) fn prepare(&self, initial: bool) -> ApplicationResult<PreparedLoad> {
        if initial {
            if let Some(plan) = self
                .initial
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                return Ok(plan);
            }
        }
        let plan = prepare(&self.loader)?;
        if plan.imports() != self.imports
            || sections(&plan, &["yml", "nacos", "secret_providers"])? != self.authority
        {
            // 来源权限先于 I/O 放行；改变模式或信任根必须重启，不能沿用旧订阅安装新权限。
            return Err(error(
                "configuration source policy changed; restart required",
            ));
        }
        Ok(plan)
    }

    /// 业务作用：本地重载从空基线重新装配，删除的来源不留下旧值。
    /// 参数说明：无。
    /// 返回：新候选树；不会自行发布应用视图。
    #[cfg(feature = "config-watch")]
    pub(crate) fn load_local(&self) -> ApplicationResult<Value> {
        let loaded = self
            .prepare(false)?
            .finish(&BTreeMap::new())
            .map_err(|_| error("strict local configuration rejected"))?;
        Ok(self.accept(loaded))
    }

    /// 业务作用：记录候选实际来源供观察准备，业务发布仍由既有 publisher 决定。
    /// 参数说明：`loaded` 是完整且复验通过的候选。
    /// 返回：候选树；旧活动 watcher 在发布完成前保持不变。
    pub(crate) fn accept(&self, loaded: LoadedConfig) -> Value {
        self.observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending = Some((loaded.source_fingerprint(), loaded.report()));
        *self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = loaded.watch.clone();
        loaded.into_tree()
    }

    /// 业务作用：来源候选受理后单独推进观察序号，同值来源变化不冒充业务版本提交。
    /// 参数说明：无。
    /// 返回：只提交最近完整候选的来源摘要，失败候选由下一轮完整结果覆盖。
    pub(crate) fn commit_observation(&self) {
        let mut state = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((fingerprint, report)) = state.pending.take() {
            if state.fingerprint != Some(fingerprint) {
                let revision = state
                    .accepted
                    .as_ref()
                    .map_or(1, |prior| prior.revision.saturating_add(1));
                state.accepted = Some(ConfigObservation { revision, report });
                state.fingerprint = Some(fingerprint);
            }
        }
    }

    /// 业务作用：向宿主提供已经受理来源的安全观察状态。
    /// 参数说明：无。
    /// 返回：独立观察序号，不包含路径、环境名称或值摘要。
    pub(crate) fn observation(&self) -> Option<ConfigObservation> {
        self.observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepted
            .clone()
    }

    /// 业务作用：取得最近完整候选的观察计划，独立于值指纹。
    /// 参数说明：无。
    /// 返回：来源计划副本，不公开原始材料。
    #[cfg(feature = "config-watch")]
    pub(crate) fn watch_plan(&self) -> WatchPlan {
        self.watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：材料和组件资源准备后再次拒绝已知过期来源，避免安装旧候选。
    /// 参数说明：无。
    /// 返回：完整候选来源仍成立时成功，不在配置发布锁内读取文件。
    #[cfg(any(feature = "config-watch", feature = "nacos-config"))]
    pub(crate) fn revalidate(&self) -> ApplicationResult<()> {
        let plan = self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        plan.revalidate(self.loader.load_policy()).map_err(|_| {
            ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "strict configuration sources changed before publication",
            )
        })
    }
}

/// 业务作用：按编入能力选择同一 naml 计划，远端适配器只补充中立声明。
/// 参数说明：`loader` 已固定读取约束。
/// 返回：尚未求值业务叶子的有序计划。
fn prepare(loader: &ConfigLoader) -> ApplicationResult<PreparedLoad> {
    #[cfg(feature = "nacos-config")]
    let plan = config_boot::strict::prepare(loader)
        .map(|(plan, _)| plan)
        .map_err(|_| error("strict source declaration rejected"))?;
    #[cfg(not(feature = "nacos-config"))]
    let plan = loader
        .prepare()
        .map_err(|_| error("strict source declaration rejected"))?;
    let mut plan = plan;
    let root = ConfigPath::default().key("secret_providers");
    for provider in plan
        .children(&root)
        .map_err(|_| error("invalid provider declarations"))?
    {
        let enabled = provider.key("enabled");
        let active = if plan.contains(&enabled) {
            let values = plan
                .bootstrap(std::slice::from_ref(&enabled))
                .map_err(|_| error("invalid provider enablement"))?;
            naml::strict::bind::<bool>(values[&enabled].clone())
                .map_err(|_| error("invalid provider enablement"))?
        } else {
            false
        };
        // 未启用的 provider 不访问连接材料；enabled 自身仍须求值为明确布尔值。
        if !active {
            plan = plan.protect(provider);
        }
        plan = plan.with_hint(enabled, naml::strict::ValueHint::Scalar);
    }
    Ok(plan)
}

/// 业务作用：从已读取文档中仅解析指定引导段及其依赖闭包。
/// 参数说明：`plan` 是来源计划；`names` 是宿主需要的顶层段。
/// 返回：仅含已声明且完成求值的段，不暴露半解析业务树。
fn sections(plan: &PreparedLoad, names: &[&str]) -> ApplicationResult<Value> {
    let mut tree = serde_json::Map::new();
    for name in names {
        let path = ConfigPath::default().key(name);
        if plan.contains(&path) {
            let values = plan
                .bootstrap(std::slice::from_ref(&path))
                .map_err(|_| error("strict bootstrap dependency rejected"))?;
            tree.insert(name.to_string(), values[&path].clone());
        }
    }
    Ok(Value::Object(tree))
}

/// 业务作用：将来源拒绝转换为不回显配置值的宿主错误。
/// 参数说明：`message` 是固定诊断摘要。
/// 返回：由生命周期阶段统一处理的错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Config, ApplicationPhase::Bootstrap, message)
}
