use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::watch;

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 提供远端配置文档的配置中心类别。
///
/// 该枚举是 provider-neutral 的：core 只记录来源类别，不依赖任何配置中心 SDK 类型。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigProvider {
    /// Nacos 配置中心来源。
    Nacos,
}

/// 本轮远端全量重拉所包含的单个配置文档描述，不保存凭据或原文。
///
/// 只描述**远端重拉**来源：纯本地启动没有可重拉的文档，来源清单为空数组而不是塞入本地文件哨兵。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSource {
    /// 提供该文档的配置中心类别。
    pub provider: ConfigProvider,
    /// 配置中心内的文档标识（Nacos 为 dataId）。
    pub id: Arc<str>,
    /// 文档所属分组；未显式声明时为 `None`。
    pub group: Option<Arc<str>>,
}

impl ConfigSource {
    /// 创建一条 Nacos 配置文档来源。
    ///
    /// # 参数
    ///
    /// - `data_id`：配置中心内的文档标识。
    /// - `group`：文档所属分组；调用方未声明时传 `None`。
    pub fn nacos(data_id: impl Into<Arc<str>>, group: Option<Arc<str>>) -> Self {
        Self {
            provider: ConfigProvider::Nacos,
            id: data_id.into(),
            group,
        }
    }
}

/// 一个版本固定、值树不可变的完整配置快照。
#[derive(Debug)]
pub struct ConfigSnapshot {
    version: u64,
    value: Arc<Value>,
    reloaded_sources: Arc<[ConfigSource]>,
}

impl ConfigSnapshot {
    /// 创建一个拥有完整配置树和来源清单的快照。
    ///
    /// # 参数
    ///
    /// - `version`：进程内严格递增的配置版本。
    /// - `value`：已经合并、插值并校验 bootstrap-only 约束的完整树。
    /// - `reloaded_sources`：形成本次版本的来源摘要。
    pub fn new(version: u64, value: Value, reloaded_sources: Vec<ConfigSource>) -> Self {
        Self {
            version,
            value: Arc::new(value),
            reloaded_sources: reloaded_sources.into(),
        }
    }

    /// 返回快照版本。
    ///
    /// # 参数
    ///
    /// 本方法无参数；版本在快照生命周期内不会变化。
    pub fn version(&self) -> u64 {
        self.version
    }

    /// 返回完整只读配置树。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回借用与当前快照共同存活。
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// 返回形成本版本的来源摘要。
    ///
    /// # 参数
    ///
    /// 本方法无参数；清单不包含配置原文。
    pub fn reloaded_sources(&self) -> &[ConfigSource] {
        &self.reloaded_sources
    }

    /// 从整个快照反序列化拥有所有字段的目标类型。
    ///
    /// # 参数
    ///
    /// 本方法无显式参数；类型 `T` 决定校验和返回结构。
    pub fn deserialize<T: DeserializeOwned>(&self) -> ApplicationResult<T> {
        serde_json::from_value((*self.value).clone()).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Config,
                ApplicationPhase::Running,
                format!("cannot deserialize config snapshot v{}", self.version),
                error,
            )
        })
    }

    /// 读取配置路径，并在同一个不可变快照上完成反序列化。
    ///
    /// # 参数
    ///
    /// - `path`：以 `.` 或 `/` 分隔的配置节点路径。
    pub fn section<T: DeserializeOwned>(&self, path: &str) -> ApplicationResult<T> {
        let mut current = self.value.as_ref();
        for segment in path.split(['.', '/']).filter(|segment| !segment.is_empty()) {
            current = current.get(segment).ok_or_else(|| {
                ApplicationError::new(
                    ComponentId::Config,
                    ApplicationPhase::Running,
                    format!(
                        "config section `{path}` does not exist in v{}",
                        self.version
                    ),
                )
            })?;
        }
        serde_json::from_value(current.clone()).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Config,
                ApplicationPhase::Running,
                format!(
                    "cannot deserialize config section `{path}` in v{}",
                    self.version
                ),
                error,
            )
        })
    }
}

/// 可以独立应用配置版本的运行期目标。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReloadTarget {
    /// 由应用整体消费的配置。
    Application,
    /// 由指定内置组件消费的配置。
    Component(ComponentId),
    /// 由给定稳定名称标识的业务钩子消费的配置。
    UserHook(Arc<str>),
}

/// 一个配置目标对当前期望版本的运行态落地结果。
///
/// `RestartRequired` 与 `ApplyFailed` 都保留该目标**最后一次成功 apply 的版本**，可观测性的核心是它与
/// 快照 `version` 的差值：只有枚举态无法量化“配置已变但运行态还旧”。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadState {
    /// 目标已成功应用当前期望版本。
    Applied,
    /// 目标尝试在线应用配置但失败,运行态继续保留最后成功版本。
    ApplyFailed {
        /// 不含配置秘密的稳定失败摘要。
        summary: Arc<str>,
    },
    /// 该变更不支持在线应用,需要重启进程才能生效。
    RestartRequired {
        /// 不含配置秘密的稳定重启原因。
        reason: Arc<str>,
    },
}

/// 一个配置目标对当前期望版本的应用状态及其最后成功版本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadStatus {
    /// 该目标对最新期望快照的落地结论。
    pub state: ReloadState,
    /// 该目标最后一次成功 apply 的快照版本。
    pub applied_version: u64,
}

impl ReloadStatus {
    /// 构造“运行态已跟上该版本”的状态。
    ///
    /// # 参数
    ///
    /// - `version`：本次成功 apply 的快照版本。
    pub fn applied(version: u64) -> Self {
        Self {
            state: ReloadState::Applied,
            applied_version: version,
        }
    }

    /// 构造“配置已变但只能重启生效”的状态。
    ///
    /// # 参数
    ///
    /// - `applied_version`：该目标最后一次成功 apply 的版本，不随本次发布推进。
    /// - `reason`：不含配置值的稳定说明。
    pub fn restart_required(applied_version: u64, reason: impl Into<Arc<str>>) -> Self {
        Self {
            state: ReloadState::RestartRequired {
                reason: reason.into(),
            },
            applied_version,
        }
    }

    /// 构造“本次 apply 失败、运行态保留 last-known-good”的状态。
    ///
    /// # 参数
    ///
    /// - `applied_version`：该目标最后一次成功 apply 的版本。
    /// - `summary`：已脱敏的失败摘要。
    pub fn apply_failed(applied_version: u64, summary: impl Into<Arc<str>>) -> Self {
        Self {
            state: ReloadState::ApplyFailed {
                summary: summary.into(),
            },
            applied_version,
        }
    }
}

/// 把期望快照、同 generation secret 快照和各目标应用状态绑定到同一版本的只读视图。
///
/// `snapshot` 里的配置树是**脱敏后**的(secret fragment → `<redacted>`);真实 material 只在
/// `secrets` 里。二者同版本原子发布,`config()` 与 `secrets()` 永远同代,不会分两次看到半旧半新。
#[derive(Debug)]
pub struct ConfigView {
    snapshot: Arc<ConfigSnapshot>,
    reload_statuses: Arc<HashMap<ReloadTarget, ReloadStatus>>,
    /// 与本代 config 同 generation 的已解析 secret 集合;真实值只在此。
    secrets: Arc<nasecret::SecretSnapshot>,
    /// 对**原始**候选树求得的私有 fingerprint,供 reload 无变化判断(不对外)。
    ///
    /// 只有 nacos-config reload 驱动读取它;无该特性的构建仍存储(始终随视图一起构造),故 allow。
    #[cfg_attr(not(feature = "nacos-config"), allow(dead_code))]
    candidate_fingerprint: [u8; 32],
}

impl ConfigView {
    /// 创建不含 secret 的配置视图(空 secret 快照)。
    ///
    /// # 参数
    ///
    /// - `snapshot`：当前期望配置的完整不可变快照。
    /// - `reload_statuses`：每个组件或用户 Hook 对该版本的应用状态。
    pub fn new(
        snapshot: Arc<ConfigSnapshot>,
        reload_statuses: HashMap<ReloadTarget, ReloadStatus>,
    ) -> Self {
        let generation = snapshot.version();
        Self {
            snapshot,
            reload_statuses: Arc::new(reload_statuses),
            secrets: Arc::new(nasecret::SecretSnapshot::builder(generation).build()),
            candidate_fingerprint: [0; 32],
        }
    }

    /// 创建携带同 generation secret 快照的配置视图(secret 已在脱敏前解析)。
    ///
    /// # 参数
    ///
    /// - `snapshot`：**脱敏后**的配置快照。
    /// - `reload_statuses`：各目标对该版本的应用状态。
    /// - `secrets`：同 generation 已解析 secret 集合。
    /// - `candidate_fingerprint`：对原始候选树求得的私有 fingerprint。
    pub(crate) fn with_secrets(
        snapshot: Arc<ConfigSnapshot>,
        reload_statuses: HashMap<ReloadTarget, ReloadStatus>,
        secrets: Arc<nasecret::SecretSnapshot>,
        candidate_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            snapshot,
            reload_statuses: Arc::new(reload_statuses),
            secrets,
            candidate_fingerprint,
        }
    }

    /// 返回与本代 config 同 generation 的 secret 快照。
    ///
    /// # 参数
    ///
    /// 本方法无参数;返回借用不能越过当前视图。secret 消费者据此取 material、判 `changed_ids`。
    pub fn secrets(&self) -> &Arc<nasecret::SecretSnapshot> {
        &self.secrets
    }

    /// 返回原始候选树的私有 fingerprint,供 reload 无变化判断。
    #[cfg(feature = "nacos-config")]
    pub(crate) fn candidate_fingerprint(&self) -> [u8; 32] {
        self.candidate_fingerprint
    }

    /// 返回当前期望快照。
    ///
    /// # 参数
    ///
    /// 本方法无参数；快照与状态来自同一次视图发布。
    pub fn snapshot(&self) -> &Arc<ConfigSnapshot> {
        &self.snapshot
    }

    /// 返回所有配置应用目标的同版本状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回借用不能越过当前视图。
    pub fn reload_statuses(&self) -> &HashMap<ReloadTarget, ReloadStatus> {
        &self.reload_statuses
    }
}

/// 使用 ArcSwap 原子发布 ConfigView，并向订阅者广播版本变化。
pub struct ConfigStore {
    current: ArcSwap<ConfigView>,
    updates: watch::Sender<Arc<ConfigView>>,
    /// 串行化发布与新订阅创建，保证新 receiver 的初值不会落后于已经可见的 ArcSwap 视图。
    publication_gate: Mutex<()>,
}

impl ConfigStore {
    /// 使用版本 1 视图初始化配置存储和 watch 通道。
    ///
    /// # 参数
    ///
    /// - `initial`：同步或异步引导完成后的首个完整视图。
    pub fn new(initial: Arc<ConfigView>) -> Self {
        let (updates, _) = watch::channel(initial.clone());
        Self {
            current: ArcSwap::from(initial),
            updates,
            publication_gate: Mutex::new(()),
        }
    }

    /// 原子加载当前完整视图。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回 Arc 可跨 await 保持同一版本。
    pub fn load(&self) -> Arc<ConfigView> {
        self.current.load_full()
    }

    /// 创建一个从当前视图开始的配置更新订阅。
    ///
    /// # 参数
    ///
    /// 本方法无参数；receiver 只保留最新完整视图。
    pub fn subscribe(&self) -> watch::Receiver<Arc<ConfigView>> {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.updates.subscribe()
    }

    /// 先原子替换读取视图，再通知 watch 订阅者。
    ///
    /// # 参数
    ///
    /// - `next`：已经通过整帧校验的新版本视图。
    pub(crate) fn publish(&self, next: Arc<ConfigView>) {
        // store 与 send_replace 构成一次发布事务；订阅创建持同一把锁，不能卡在两步之间拿到旧初值。
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.current.store(next.clone());
        self.updates.send_replace(next);
    }
}

/// 从**原始**候选树解析 secret、脱敏,构造携带同 generation secret 快照的 [`ConfigView`]。
///
/// secret resolver 在脱敏前工作:返回视图的 `snapshot().value()` 里 fragment 已替换为 `<redacted>`,
/// 真实 material 只进 `secrets()`;候选 fingerprint 记入视图供 reload 无变化判断。
///
/// # 参数
///
/// - `version`:本次发布代号(与 config 版本一致)。
/// - `raw`:合并 profile/Nacos/env 后的原始候选树(未脱敏)。
/// - `sources`:形成本版本的来源摘要。
/// - `reload_statuses`:各目标对该版本的应用状态。
///
/// # 错误
///
/// `secrets` 段结构非法或任一 secret 解析失败时返回 [`crate::secret::SecretResolveError`]。
pub(crate) fn resolve_view(
    version: u64,
    raw: Value,
    sources: Vec<ConfigSource>,
    reload_statuses: HashMap<ReloadTarget, ReloadStatus>,
) -> Result<Arc<ConfigView>, crate::secret::SecretResolveError> {
    let resolution = crate::secret::resolve_and_redact(&raw, version)?;
    let snapshot = Arc::new(ConfigSnapshot::new(version, resolution.redacted, sources));
    Ok(Arc::new(ConfigView::with_secrets(
        snapshot,
        reload_statuses,
        Arc::new(resolution.snapshot),
        resolution.candidate_fingerprint,
    )))
}
