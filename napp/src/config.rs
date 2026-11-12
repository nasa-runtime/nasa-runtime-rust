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
    /// 业务作用：创建一条 Nacos 配置文档来源。
    ///
    /// 参数说明：
    ///
    /// - `data_id`：配置中心内的文档标识。
    /// - `group`：文档所属分组；调用方未声明时传 `None`。
    ///
    /// 返回：带 Nacos 类别的来源描述，不访问配置中心、不验证标识或填充分组默认值，也不持有配置正文。
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
    /// 调用方分配并随快照固定的期望配置版本。
    version: u64,
    /// 只读配置树，脱敏与校验在构造前由候选准备流程完成。
    value: Arc<Value>,
    /// 本次远端重拉的来源摘要，纯本地来源可为空。
    reloaded_sources: Arc<[ConfigSource]>,
}

impl ConfigSnapshot {
    /// 业务作用：创建一个拥有完整配置树和来源清单的快照。
    ///
    /// 参数说明：
    ///
    /// - `version`：调用方分配的配置版本，发布流程负责保证递增，本构造函数不比较旧版本。
    /// - `value`：调用方已完成合并、插值、约束校验及必要脱敏的完整配置树。
    /// - `reloaded_sources`：形成本次版本的远端来源摘要，纯本地配置可为空。
    ///
    /// 返回：独立持有配置树和来源清单的不可变快照；不再次校验、脱敏或发布，也不改变应用实际运行配置。
    pub fn new(version: u64, value: Value, reloaded_sources: Vec<ConfigSource>) -> Self {
        Self {
            version,
            value: Arc::new(value),
            reloaded_sources: reloaded_sources.into(),
        }
    }

    /// 业务作用：返回快照版本。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造时保存的固定配置版本，不代表全部组件已应用该版本，也不查询当前全局视图。
    pub fn version(&self) -> u64 {
        self.version
    }

    /// 业务作用：返回完整只读配置树。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前快照内配置树的借用，不复制或反序列化内容；脱敏须在构造快照前完成，读取时不再过滤字段。
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// 业务作用：返回形成本版本的来源摘要。
    ///
    /// 参数说明：无。
    ///
    /// 返回：随快照冻结的远端文档来源切片，不含配置正文；为空可表示纯本地来源，不据此判断加载失败。
    pub fn reloaded_sources(&self) -> &[ConfigSource] {
        &self.reloaded_sources
    }

    /// 业务作用：从整个快照反序列化拥有所有字段的目标类型。
    ///
    /// 参数说明：无。
    ///
    /// 返回：复制配置树后按 T 的字段类型与 Serde 规则绑定的独立值；绑定失败返回带快照版本的 Running 错误，
    /// 底层错误隐藏原字段值。不会读取后续版本或补回已脱敏的秘密材料。
    pub fn deserialize<T: DeserializeOwned>(&self) -> ApplicationResult<T> {
        naml::strict::bind((*self.value).clone()).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Config,
                ApplicationPhase::Running,
                format!("cannot deserialize config snapshot v{}", self.version),
                error,
            )
        })
    }

    /// 业务作用：读取配置路径，并在同一个不可变快照上完成反序列化。
    ///
    /// 参数说明：
    ///
    /// - `path`：以 `.` 或 `/` 分隔的对象键路径；空段被忽略，空路径选择整棵树，不提供数组下标或分隔符转义。
    ///
    /// 返回：同一快照内目标节点复制后绑定到 T 的值；节点缺失或绑定失败返回 Running 错误，保留路径与版本而不输出字段原值。
    /// 路径文本本身进入错误摘要，调用方不得把秘密值放入路径。
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
        naml::strict::bind(current.clone()).map_err(|error| {
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
    /// 由宿主管理但没有独立组件身份的配置段。
    Managed(Arc<str>),
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
    /// 业务作用：构造“运行态已跟上该版本”的状态。
    ///
    /// 参数说明：
    ///
    /// - `version`：本次成功 apply 的快照版本。
    ///
    /// 返回：Applied 与所给成功版本的状态值，不实际应用配置；调用方只能在目标安装成功后使用该结论。
    pub fn applied(version: u64) -> Self {
        Self {
            state: ReloadState::Applied,
            applied_version: version,
        }
    }

    /// 业务作用：构造“配置已变但只能重启生效”的状态。
    ///
    /// 参数说明：
    ///
    /// - `applied_version`：该目标最后一次成功 apply 的版本，不随本次发布推进。
    /// - `reason`：不含配置值的稳定说明。
    ///
    /// 返回：保留最后成功版本的 RestartRequired 状态，不重启应用或推进运行版本；原因文本原样保存，调用方负责脱敏。
    pub fn restart_required(applied_version: u64, reason: impl Into<Arc<str>>) -> Self {
        Self {
            state: ReloadState::RestartRequired {
                reason: reason.into(),
            },
            applied_version,
        }
    }

    /// 业务作用：构造“本次 apply 失败、运行态保留 last-known-good”的状态。
    ///
    /// 参数说明：
    ///
    /// - `applied_version`：该目标最后一次成功 apply 的版本。
    /// - `summary`：已脱敏的失败摘要。
    ///
    /// 返回：ApplyFailed 与最后成功版本的状态值，不执行回滚或重试；运行态保留旧值应由目标实现保证，摘要不再自动过滤。
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
/// 标准准备流程将声明的 ConfigPath 秘密片段脱敏，解析材料保存在 `secrets`；异步引导前的原始候选
/// 另存于不对业务开放的待准备容器。公开构造入口不自动脱敏，调用方须先完成材料边界处理。
/// 同一视图内配置与秘密保持同代；两次独立的 Application getter 之间仍可能发生重载，一致读取须持有同一个视图。
#[derive(Debug)]
pub struct ConfigView {
    /// 本视图对应的不可变期望配置，目标运行状态可能落后于其版本。
    snapshot: Arc<ConfigSnapshot>,
    /// 异步秘密引导尚未完成时保留的原始候选，业务 getter 不开放该材料。
    pending_bootstrap: Option<Arc<BootstrapMaterials>>,
    /// 与本视图秘密共同准备的具名 HTTP 客户端，用于同代凭据读取。
    #[cfg(feature = "secret-http")]
    pub(crate) http_clients: Arc<std::collections::BTreeMap<String, crate::tls_http::PreparedHttp>>,
    /// 各目标对本代期望配置的应用结果，保留最后成功版本以表达运行态落后。
    reload_statuses: Arc<HashMap<ReloadTarget, ReloadStatus>>,
    /// 与本代 config 同 generation 的已解析秘密集合；尚未完成引导时可为空。
    secrets: Arc<nasecret::SecretSnapshot>,
    /// 随本代秘密冻结的 Saga 安全资源，初始视图可能尚未准备。
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
    saga_security: Option<Arc<crate::saga::security::SagaSecuritySnapshot>>,
    /// 对**原始**候选树求得的私有 fingerprint,供 reload 无变化判断(不对外)。
    ///
    /// 本地或配置中心重载驱动据此判断候选是否变化；未启用观察特性时仍随视图保存。
    #[cfg_attr(
        not(any(feature = "nacos-config", feature = "config-watch")),
        allow(dead_code)
    )]
    candidate_fingerprint: [u8; 32],
}

impl ConfigView {
    /// 业务作用：把配置和目标状态组装为尚未准备秘密及派生资源的初始视图。
    ///
    /// 参数说明：
    ///
    /// - `snapshot`：调用方已准备且完成必要脱敏的期望配置快照，本入口不检查或改写其字段。
    /// - `reload_statuses`：每个组件或用户 Hook 对该版本的应用状态。
    ///
    /// 返回：secret generation 与快照版本一致但内容为空的视图，HTTP 客户端为空、候选指纹全零；不解析秘密、不安装运行态或发布通知。
    pub fn new(
        snapshot: Arc<ConfigSnapshot>,
        reload_statuses: HashMap<ReloadTarget, ReloadStatus>,
    ) -> Self {
        let generation = snapshot.version();
        Self {
            snapshot,
            pending_bootstrap: None,
            #[cfg(feature = "secret-http")]
            http_clients: Arc::new(std::collections::BTreeMap::new()),
            reload_statuses: Arc::new(reload_statuses),
            secrets: Arc::new(nasecret::SecretSnapshot::builder(generation).build()),
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            saga_security: None,
            candidate_fingerprint: [0; 32],
        }
    }

    /// 业务作用：返回与本代 config 同 generation 的 secret 快照。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本视图所持秘密快照 Arc 的借用；与本视图配置保持同代，不主动解析、轮换或暴露材料。
    /// 初始未准备视图的集合可能为空，跨字段一致读取应始终使用同一个 ConfigView。
    pub fn secrets(&self) -> &Arc<nasecret::SecretSnapshot> {
        &self.secrets
    }

    /// 业务作用：读取与当前 secret 同代发布的全部 Saga 安全资源。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已准备的凭据快照；初始装配期间为空，由组件持有初始材料。
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
    pub(crate) fn saga_security(&self) -> Option<Arc<crate::saga::security::SagaSecuritySnapshot>> {
        self.saga_security.clone()
    }

    /// 业务作用：返回原始候选树的私有 fingerprint,供 reload 无变化判断。
    ///
    /// 参数说明：无。
    ///
    /// 返回：随视图冻结的原始候选指纹副本；尚未准备的初始视图为全零，不在此重新计算或作为公开配置内容返回。
    #[cfg(any(feature = "nacos-config", feature = "config-watch"))]
    pub(crate) fn candidate_fingerprint(&self) -> [u8; 32] {
        self.candidate_fingerprint
    }

    /// 业务作用：返回当前期望快照。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本视图持有的配置快照 Arc 的借用，后续全局发布不改变它；各目标是否已跟上该版本应另读本视图的状态表。
    pub fn snapshot(&self) -> &Arc<ConfigSnapshot> {
        &self.snapshot
    }

    /// 业务作用：返回所有配置应用目标的同版本状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与本视图期望配置同时保存的目标状态表借用；目标的最后成功版本可落后于期望版本，缺失目标不自动视为 Applied。
    pub fn reload_statuses(&self) -> &HashMap<ReloadTarget, ReloadStatus> {
        &self.reload_statuses
    }
}

/// 使用 ArcSwap 原子发布 ConfigView，并向订阅者广播版本变化。
pub struct ConfigStore {
    /// 读取方一次获取完整视图的原子槽，持有 Arc 后不随下一次发布改变。
    current: ArcSwap<ConfigView>,
    /// 仅保存最新视图的通知通道，不充当逐版本事件日志或应用回执。
    updates: watch::Sender<Arc<ConfigView>>,
    /// 串行化发布与新订阅创建，保证新 receiver 的初值不会落后于已经可见的 ArcSwap 视图。
    publication_gate: Mutex<()>,
}

impl ConfigStore {
    /// 业务作用：在既有发布门禁内复验版本、安装运行态并发布同代配置。
    ///
    /// 参数说明：
    /// - `expected`：准备候选时读取的现有配置版本，用于在发布锁内复验本次候选仍有发布资格。
    /// - `finish`：匹配版本后执行的短时同步安装及视图组装动作，不能递归获取发布锁或进行阻塞 I/O。
    ///
    /// 返回：版本不匹配时为 Ok(false)，不调用安装；匹配且组装成功后先更新读取槽再通知 watch，返回 Ok(true)。
    /// 闭包错误直接传播且不发布新视图，但本函数不回滚闭包已经执行的外部副作用，也不复验新版本是否递增。
    #[cfg(any(feature = "nacos-config", feature = "config-watch"))]
    pub(crate) fn publish_with(
        &self,
        expected: u64,
        finish: impl FnOnce() -> crate::ApplicationResult<Arc<ConfigView>>,
    ) -> crate::ApplicationResult<bool> {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 先复验再调用安装，避免失去候选权威后仍改变日志运行态。
        if self.current.load().snapshot().version() != expected {
            return Ok(false);
        }
        let next = finish()?;
        self.current.store(next.clone());
        self.updates.send_replace(next);
        Ok(true)
    }

    /// 业务作用：以同一初始视图建立无锁读取槽和最新值订阅通道。
    ///
    /// 参数说明：
    ///
    /// - `initial`：同步或异步引导完成后的首个完整视图。
    ///
    /// 返回：读取槽与 watch 初值一致的配置存储，保留传入版本而不强制为 1；不再次解析配置、安装组件或通知已有订阅者。
    pub fn new(initial: Arc<ConfigView>) -> Self {
        let (updates, _) = watch::channel(initial.clone());
        Self {
            current: ArcSwap::from(initial),
            updates,
            publication_gate: Mutex::new(()),
        }
    }

    /// 业务作用：原子加载当前完整视图。
    ///
    /// 参数说明：无。
    ///
    /// 返回：一次原子读取获得的完整视图 Arc，可跨 await 固定配置、秘密和目标状态；两次独立调用之间仍可能发生发布。
    pub fn load(&self) -> Arc<ConfigView> {
        self.current.load_full()
    }

    /// 业务作用：创建一个从当前视图开始的配置更新订阅。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与发布过程串行创建的 watch 接收端，初值不会落后于创建时已发布的读取槽；初值视为已见，需主动 borrow 读取。
    /// 慢订阅者只保留最新视图，不保证逐个观察中间版本，也不构成发布已被应用的确认。
    pub fn subscribe(&self) -> watch::Receiver<Arc<ConfigView>> {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.updates.subscribe()
    }

    /// 业务作用：先原子替换读取视图，再通知 watch 订阅者。
    ///
    /// 参数说明：
    ///
    /// - `next`：已经通过整帧校验的新版本视图。
    ///
    /// 返回：在发布锁内先替换读取槽，再更新 watch 最新值；不等待订阅者处理，不自动校验版本递增或重新验证视图。
    /// 调用方负责候选权威与安装顺序；两种读取入口的一致初值由 subscribe 共用发布锁保证。
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

/// 业务作用：为引导阶段构造配置视图；同步秘密立即准备，需要异步解析时先保留有界候选等待后续阶段完成。
///
/// 返回视图的普通配置树经过秘密结构脱敏；异步分支的原始候选仅存入私有可清零字节容器，不进入 Debug 输出。
///
/// 参数说明：
///
/// - `version`:本次发布代号(与 config 版本一致)。
/// - `raw`:合并 profile/Nacos/env 后的原始候选树(未脱敏)。
/// - `sources`:形成本版本的来源摘要。
/// - `reload_statuses`:各目标对该版本的应用状态。
///
/// 返回：同步来源成功时得到包含真实秘密与候选指纹的完整视图；需要异步解析时返回空秘密集合、零指纹及待准备材料的初始视图。
/// 结构非法、同步解析失败、候选序列化失败或异步分支编码结果超过 1 MiB 时返回 SecretResolveError；不发布或安装运行态。
pub(crate) fn resolve_view(
    version: u64,
    raw: Value,
    sources: Vec<ConfigSource>,
    reload_statuses: HashMap<ReloadTarget, ReloadStatus>,
) -> Result<Arc<ConfigView>, crate::secret::SecretResolveError> {
    if crate::secret::requires_async(&raw)? {
        // 外部秘密尚未取得，先隐藏已声明的明文片段，不能把原始候选作为普通配置视图公开。
        let tree = crate::secret::redact_structure(&raw)?;
        let encoded = zeroize::Zeroizing::new(serde_json::to_vec(&raw).map_err(|_| {
            crate::secret::SecretResolveError::MalformedSpec {
                id: Arc::from("secrets"),
                detail: "cannot encode bootstrap candidate".into(),
            }
        })?);
        // 只允许保存有界的待引导材料；该检查发生在编码之后，不表示编码过程的峰值内存也受此限制。
        if encoded.len() > 1024 * 1024 {
            return Err(crate::secret::SecretResolveError::MalformedSpec {
                id: Arc::from("secrets"),
                detail: "bootstrap candidate exceeds 1 MiB".into(),
            });
        }
        let mut view = ConfigView::new(
            Arc::new(ConfigSnapshot::new(version, tree, sources)),
            reload_statuses,
        );
        view.pending_bootstrap = Some(Arc::new(BootstrapMaterials(encoded)));
        return Ok(Arc::new(view));
    }
    Ok(resolve_candidate(version, raw, sources)?.finish(reload_statuses))
}

/// 尚未安装的候选材料，不包含临时应用状态，不能被配置订阅者读取。
pub(crate) struct ResolvedCandidate {
    /// 已完成秘密片段脱敏的配置快照，尚未发布到 ConfigStore。
    pub(crate) snapshot: Arc<ConfigSnapshot>,
    /// 根据本次秘密准备的客户端集合，随最终视图一同交接。
    #[cfg(feature = "secret-http")]
    http_clients: Arc<std::collections::BTreeMap<String, crate::tls_http::PreparedHttp>>,
    /// 与候选版本一致的秘密材料，失败候选释放时不覆盖旧视图。
    pub(crate) secrets: Arc<nasecret::SecretSnapshot>,
    /// 原始候选的私有内容指纹，用于重载去重。
    candidate_fingerprint: [u8; 32],
    /// 后续安全准备阶段填充的 Saga 资源，完成前不安装到运行态。
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
    pub(crate) saga_security: Option<Arc<crate::saga::security::SagaSecuritySnapshot>>,
}

impl ResolvedCandidate {
    /// 业务作用：安装完成后把真实目标状态与准备材料组装为完整视图。
    ///
    /// 参数说明：
    /// - `statuses`：调用方根据本次同步安装结果形成的目标状态表，已有 http_clients 项会被候选的成功准备版本覆盖。
    ///
    /// 返回：消耗候选并形成配置、秘密、安全资源和状态共同持有的视图，清除待引导材料；不执行额外安装或向存储发布。
    /// HTTP 客户端状态记录候选准备完成的版本，不表示网络请求已经成功。
    pub(crate) fn finish(
        self,
        mut statuses: HashMap<ReloadTarget, ReloadStatus>,
    ) -> Arc<ConfigView> {
        // HTTP 客户端已随候选完整准备，调用句柄从此视图取资源；初始装配与每次发布共享同一成功代次。
        // 准备失败的候选不会到达此处，不能提前推进旧视图中的最后成功版本。
        statuses.insert(
            ReloadTarget::Managed(Arc::from("http_clients")),
            ReloadStatus::applied(self.snapshot.version()),
        );
        Arc::new(ConfigView {
            snapshot: self.snapshot,
            pending_bootstrap: None,
            #[cfg(feature = "secret-http")]
            http_clients: self.http_clients,
            reload_statuses: Arc::new(statuses),
            secrets: self.secrets,
            candidate_fingerprint: self.candidate_fingerprint,
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            saga_security: self.saga_security,
        })
    }
}

/// 业务作用：在任何运行态改变之前解析 secret 并保存脱敏后的候选材料。
///
/// 参数说明：
/// - `version`：本次候选与秘密快照共用的版本，递增关系由上层发布流程保证。
/// - `raw`：尚未脱敏的完整合并配置，用于解析活跃秘密、派生客户端及计算候选指纹。
/// - `sources`：随候选保存的远端文档来源，不用于本函数重新拉取配置。
///
/// 返回：本地秘密全部解析、脱敏及启用的 HTTP 客户端准备成功后返回未安装材料；结构、材料或客户端配置错误时失败。
/// 可读取本地环境或文件，不拉取外部 provider，不发布配置；Saga 安全材料留给后续准备阶段填充。
pub(crate) fn resolve_candidate(
    version: u64,
    raw: Value,
    sources: Vec<ConfigSource>,
) -> Result<ResolvedCandidate, crate::secret::SecretResolveError> {
    let resolution = crate::secret::resolve_and_redact(&raw, version)?;
    #[cfg(feature = "secret-http")]
    let http_clients = crate::tls_http::prepare(&resolution.redacted, &resolution.snapshot)?;
    Ok(ResolvedCandidate {
        #[cfg(feature = "secret-http")]
        http_clients: Arc::new(http_clients),
        snapshot: Arc::new(ConfigSnapshot::new(version, resolution.redacted, sources)),
        secrets: Arc::new(resolution.snapshot),
        candidate_fingerprint: resolution.candidate_fingerprint,
        #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
        saga_security: None,
    })
}

/// 异步引导尚未解析的有界原始材料；只在引导阶段保留且不参与诊断。
/// 字段 `0` 保存编码后的原始候选，容器释放时清零该字节缓冲，不代表所有上游副本都已清零。
struct BootstrapMaterials(zeroize::Zeroizing<Vec<u8>>);
impl std::fmt::Debug for BootstrapMaterials {
    /// 业务作用：禁止配置视图 Debug 输出引导候选明文。
    ///
    /// 参数说明：
    /// - `formatter`：接收固定隐藏标记的格式化目标，不允许从该入口读取候选字节。
    ///
    /// 返回：隐藏标记写入成功时为 Ok；格式化目标错误向上传播，不遍历或输出任何候选字段。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted bootstrap material>")
    }
}

impl ConfigView {
    /// 业务作用：为首次异步引导借出原始候选，业务 getter 不暴露该材料。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有私有材料时解码为独立候选；无私有材料且指纹为零时复制现有配置树；已准备的非零指纹视图返回 None。
    /// 解码失败返回不含原文的 Bootstrap 错误，不消费保存材料，调用方必须限制候选的存活和输出范围。
    pub(crate) fn bootstrap_candidate(&self) -> crate::ApplicationResult<Option<Value>> {
        if self.pending_bootstrap.is_none() && self.candidate_fingerprint == [0; 32] {
            return Ok(Some(self.snapshot.value().clone()));
        }
        self.pending_bootstrap
            .as_ref()
            .map(|raw| {
                serde_json::from_slice(&raw.0).map_err(|_| {
                    crate::ApplicationError::new(
                        crate::ComponentId::Config,
                        crate::ApplicationPhase::Bootstrap,
                        "invalid retained bootstrap candidate",
                    )
                })
            })
            .transpose()
    }
}

/// 业务作用：在宿主预算内准备本地和外部秘密，产物尚不能被订阅者观察。
///
/// 参数说明：
/// - `version`：候选配置与秘密共用的代次，本函数不与当前已发布版本比较。
/// - `raw`：未脱敏的完整候选树，包含秘密声明和 provider 引导设置。
/// - `sources`：随本次候选冻结的远端配置来源摘要，不包含配置正文或凭据。
///
/// 返回：活跃秘密及启用的 HTTP 客户端全部准备成功后的脱敏材料；结构、provider、解析或客户端构造失败时拒绝整个候选。
/// 外部秘密读取使用解析器自己的有界预算，宿主可在外层施加更短期限；不执行运行态安装，Saga 安全材料仍待后续填充。
pub(crate) async fn resolve_candidate_async(
    version: u64,
    raw: Value,
    sources: Vec<ConfigSource>,
) -> Result<ResolvedCandidate, crate::secret::SecretResolveError> {
    let resolution = crate::secret::resolve_async(&raw, version).await?;
    #[cfg(feature = "secret-http")]
    let http_clients = crate::tls_http::prepare(&resolution.redacted, &resolution.snapshot)?;
    Ok(ResolvedCandidate {
        #[cfg(feature = "secret-http")]
        http_clients: Arc::new(http_clients),
        snapshot: Arc::new(ConfigSnapshot::new(version, resolution.redacted, sources)),
        secrets: Arc::new(resolution.snapshot),
        candidate_fingerprint: resolution.candidate_fingerprint,
        #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
        saga_security: None,
    })
}
