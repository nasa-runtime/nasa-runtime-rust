//! 数据库后端中立的 datasource 身份、受管所有权与事务裁决合同。
//!
//! 本 crate 不创建连接，也不依赖 SQLx 的数据库 feature。MySQL 与 PostgreSQL driver crate
//! 通过同一进程协调器发布 typed registry，确保同一 Application 只有一张名称与 driver catalog。

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// 默认 datasource 的规范名称。
pub const DEFAULT_DATASOURCE: &str = "default";
/// datasource qualifier 的最大 UTF-8 字节数。
pub const MAX_DATASOURCE_NAME_BYTES: usize = 128;
/// 单个受管 Application 可发布的 datasource 总数上限。
pub const MAX_MANAGED_DATASOURCES: usize = 64;

/// 业务作用：标识 datasource 使用的数据库协议与事务实现。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseDriver {
    /// MySQL 或兼容协议。
    MySql,
    /// PostgreSQL 协议。
    PostgreSql,
}

/// 业务作用：声明当前编译产物实际包含的数据库后端，避免把可解析 driver 误当成已编入能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DatabaseCapabilities(u8);

impl DatabaseCapabilities {
    const MYSQL: u8 = 1;
    const POSTGRESQL: u8 = 1 << 1;

    /// 业务作用：构造不包含任何数据库后端的能力集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可继续通过 [`Self::with`] 组合的空集合。
    pub const fn empty() -> Self {
        Self(0)
    }

    /// 业务作用：把一个已经编入的 typed driver 加入能力集合。
    ///
    /// 参数说明：`driver` 是由对应 Cargo feature 实际编入的后端。
    ///
    /// 返回：包含原集合和指定 driver 的新集合。
    pub const fn with(self, driver: DatabaseDriver) -> Self {
        let flag = match driver {
            DatabaseDriver::MySql => Self::MYSQL,
            DatabaseDriver::PostgreSql => Self::POSTGRESQL,
        };
        Self(self.0 | flag)
    }

    /// 业务作用：复验配置声明的 driver 是否已经编入当前运行产物。
    ///
    /// 参数说明：`driver` 是配置边界解析出的后端。
    ///
    /// 返回：对应 typed runtime 存在时返回真。
    pub const fn contains(self, driver: DatabaseDriver) -> bool {
        let flag = match driver {
            DatabaseDriver::MySql => Self::MYSQL,
            DatabaseDriver::PostgreSql => Self::POSTGRESQL,
        };
        self.0 & flag != 0
    }
}

impl std::fmt::Display for DatabaseDriver {
    /// 业务作用：输出稳定、低基数的 driver 名称。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：名称写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MySql => "mysql",
            Self::PostgreSql => "postgresql",
        })
    }
}

/// 业务作用：描述 datasource qualifier 在配置边界被拒绝的稳定原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DatasourceNameError {
    /// 名称为空。
    #[error("datasource name cannot be empty")]
    Empty,
    /// 名称包含首尾空白。
    #[error("datasource name cannot contain leading or trailing whitespace")]
    SurroundingWhitespace,
    /// 名称超长或包含不受支持的字符。
    #[error("datasource name must use ASCII letters, digits, '.', '_' or '-' and stay within the byte limit")]
    InvalidCharacters,
}

/// 业务作用：携带经过统一校验的 datasource 身份，使各 driver 不依赖临时字符串解释名称。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatasourceRef(Arc<str>);

impl DatasourceRef {
    /// 业务作用：以既有兼容签名校验并固定一个 datasource qualifier。
    ///
    /// 参数说明：`name` 是配置边界或业务计划给出的数据源名。
    ///
    /// 返回：名称符合有界 canonical 合同时返回拥有型引用；失败时在 anyhow 错误链保留具名原因。
    pub fn new(name: impl AsRef<str>) -> anyhow::Result<Self> {
        Self::try_new(name).map_err(anyhow::Error::new)
    }

    /// 业务作用：为 checked 配置与事务入口校验 datasource qualifier 并保留结构化失败分类。
    ///
    /// 参数说明：`name` 是配置边界或业务计划给出的数据源名。
    ///
    /// 返回：名称符合有界 canonical 合同时返回拥有型引用，否则返回 [`DatasourceNameError`]。
    pub fn try_new(name: impl AsRef<str>) -> Result<Self, DatasourceNameError> {
        let name = name.as_ref();
        validate_datasource_name(name)?;
        Ok(Self(Arc::from(name)))
    }

    /// 业务作用：返回默认 datasource 的规范化身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：名称固定为 `default` 的拥有型引用。
    pub fn default_ref() -> Self {
        Self(Arc::from(DEFAULT_DATASOURCE))
    }

    /// 业务作用：读取 datasource 的规范化 qualifier。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与该引用同生命周期的名称切片。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for DatasourceRef {
    /// 业务作用：为兼容无参入口选择默认 datasource。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认 datasource 引用。
    fn default() -> Self {
        Self::default_ref()
    }
}

impl AsRef<str> for DatasourceRef {
    /// 业务作用：让事务与连接 API 消费已校验 datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：规范化 datasource 名称。
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for DatasourceRef {
    /// 业务作用：输出不含连接信息的 datasource qualifier。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：名称写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// 业务作用：校验 datasource 名称可安全用于配置键、指标 label 与诊断字段。
///
/// 参数说明：`name` 是待校验的 qualifier。
///
/// 返回：名称合法时返回 `Ok`，否则返回不包含输入内容的稳定分类。
pub fn validate_datasource_name(name: &str) -> Result<(), DatasourceNameError> {
    if name.is_empty() {
        return Err(DatasourceNameError::Empty);
    }
    if name.trim() != name {
        return Err(DatasourceNameError::SurroundingWhitespace);
    }
    if name.len() > MAX_DATASOURCE_NAME_BYTES
        || !name
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'_' | b'-'))
    {
        return Err(DatasourceNameError::InvalidCharacters);
    }
    Ok(())
}

/// 业务作用：承载两种数据库连接池共同使用的启动配置，不解释具体 URL scheme。
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSourcePoolConfig {
    /// 数据库连接串；driver 包装层负责校验 scheme。
    pub url: String,
    /// 连接池上限。
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// 连接池下限。
    #[serde(default)]
    pub min_connections: u32,
    /// 从池中获取连接的等待上限，毫秒。
    #[serde(default = "default_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,
    /// 建立单条连接的等待上限，毫秒。
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// 启动时是否先执行真实单连接探测。
    #[serde(default = "default_probe_on_start")]
    pub probe_on_start: bool,
}

impl std::fmt::Debug for DataSourcePoolConfig {
    /// 业务作用：输出不含用户名和口令的池配置诊断视图。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：脱敏字段写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataSourcePoolConfig")
            .field("url", &redact_url(&self.url))
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout_ms", &self.acquire_timeout_ms)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("probe_on_start", &self.probe_on_start)
            .finish()
    }
}

/// 业务作用：描述公共池参数在任何网络 I/O 前被拒绝的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PoolConfigError {
    /// URL 为空。
    #[error("datasource url cannot be empty")]
    EmptyUrl,
    /// 连接池上限为零。
    #[error("datasource max_connections must be greater than zero")]
    ZeroMaxConnections,
    /// 连接池下限超过上限。
    #[error("datasource min_connections cannot exceed max_connections")]
    InvalidConnectionRange,
    /// 获取连接超时为零。
    #[error("datasource acquire_timeout_ms must be greater than zero")]
    ZeroAcquireTimeout,
    /// 建连超时为零。
    #[error("datasource connect_timeout_ms must be greater than zero")]
    ZeroConnectTimeout,
}

impl DataSourcePoolConfig {
    /// 业务作用：校验与 driver 无关的池容量和超时合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：公共配置可用于建池时返回 `Ok`；失败时不产生连接副作用。
    pub fn validate_common(&self) -> Result<(), PoolConfigError> {
        if self.url.trim().is_empty() {
            return Err(PoolConfigError::EmptyUrl);
        }
        if self.max_connections == 0 {
            return Err(PoolConfigError::ZeroMaxConnections);
        }
        if self.min_connections > self.max_connections {
            return Err(PoolConfigError::InvalidConnectionRange);
        }
        if self.acquire_timeout_ms == 0 {
            return Err(PoolConfigError::ZeroAcquireTimeout);
        }
        if self.connect_timeout_ms == 0 {
            return Err(PoolConfigError::ZeroConnectTimeout);
        }
        Ok(())
    }

    /// 业务作用：返回可安全写入日志的 host、端口与 database 定位信息。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：移除 scheme、userinfo、query 与 fragment 后的 endpoint；无法识别时返回固定占位符。
    pub fn endpoint(&self) -> String {
        redacted_endpoint(&self.url)
    }
}

/// 业务作用：去掉 URL 中的 userinfo，同时保留 scheme 供 Debug 识别 driver。
///
/// 参数说明：`url` 是可能包含凭据的数据库连接串。
///
/// 返回：不包含用户名和口令的字符串；scheme 或 endpoint 无法安全识别时返回固定占位符。
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<redacted-url>".to_owned();
    };
    if scheme.is_empty()
        || !scheme
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'+' | b'-' | b'.'))
    {
        return "<redacted-url>".to_owned();
    }
    let endpoint = redacted_endpoint(url);
    if endpoint == "<unknown-endpoint>" {
        return "<redacted-url>".to_owned();
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    if authority.contains('@') {
        format!("{scheme}://***@{endpoint}")
    } else {
        format!("{scheme}://{endpoint}")
    }
}

/// 业务作用：生成不包含 scheme、用户信息和连接参数的数据库 endpoint。
///
/// 参数说明：`url` 是 driver 已校验或即将校验的连接串。
///
/// 返回：可用于脱敏诊断的 endpoint；格式不完整时返回固定占位符。
pub fn redacted_endpoint(url: &str) -> String {
    let Some((_, rest)) = url.split_once("://") else {
        return "<unknown-endpoint>".to_owned();
    };
    let authority_and_path = rest.rsplit_once('@').map_or(rest, |(_, value)| value);
    let endpoint = authority_and_path
        .split(['?', '#'])
        .next()
        .unwrap_or(authority_and_path);
    if endpoint.is_empty() {
        "<unknown-endpoint>".to_owned()
    } else {
        endpoint.to_owned()
    }
}

/// 业务作用：返回连接池上限的公共缺省值。
///
/// 参数说明: 无。
///
/// 返回：面向普通服务实例的连接数上限。
fn default_max_connections() -> u32 {
    10
}

/// 业务作用：返回获取连接等待上限的公共缺省值。
///
/// 参数说明: 无。
///
/// 返回：毫秒单位的有限等待预算。
fn default_acquire_timeout_ms() -> u64 {
    2_000
}

/// 业务作用：返回建立连接等待上限的公共缺省值。
///
/// 参数说明: 无。
///
/// 返回：毫秒单位的握手预算。
fn default_connect_timeout_ms() -> u64 {
    5_000
}

/// 业务作用：返回是否默认在启动期探测真实连接。
///
/// 参数说明: 无。
///
/// 返回：固定为真，使地址、凭据和 database 错误在启动期暴露。
fn default_probe_on_start() -> bool {
    true
}

static NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
/// 业务作用：为单个 Application 的数据库 registry 所有权提供不可由数值相等伪造的 Arc 身份。
struct OwnerIdentity {
    id: u64,
}

/// 业务作用：标识唯一 Application 对 catalog 与 typed registry 的共同所有权。
#[derive(Clone)]
pub struct ManagedRegistryOwner(Arc<OwnerIdentity>);

impl ManagedRegistryOwner {
    /// 业务作用：为一次 Application 数据库启动创建不可伪造的进程内所有权身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可安全 clone、按 Arc 身份复验的 owner。
    pub fn new() -> Self {
        Self(Arc::new(OwnerIdentity {
            id: NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed),
        }))
    }

    /// 业务作用：比较两个 owner 是否代表同一次 Application 启动。
    ///
    /// 参数说明：`other` 是待复验的 owner。
    ///
    /// 返回：底层身份相同时返回真。
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// 业务作用：返回不包含资源信息的进程内诊断编号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本进程单调分配的 owner 编号，不作为跨进程协议值。
    pub fn diagnostic_id(&self) -> u64 {
        self.0.id
    }

    /// 业务作用：把 owner 降级为不延长 Application 生命周期的进程模式身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：owner 最后一个强引用释放后无法升级的弱身份。
    fn downgrade(&self) -> Weak<OwnerIdentity> {
        Arc::downgrade(&self.0)
    }
}

impl Default for ManagedRegistryOwner {
    /// 业务作用：兼容需要 Default 构造的启动 staging，仍创建唯一 owner。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：新的独立 owner。
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ManagedRegistryOwner {
    /// 业务作用：输出不包含连接资源的 owner 诊断视图。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：诊断编号写入成功时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ManagedRegistryOwner")
            .field(&self.diagnostic_id())
            .finish()
    }
}

/// 业务作用：区分常规配置建池与 UserHook 延后 default 引导。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapKind {
    /// Application 根据冻结配置建立全部 datasource。
    Configured,
    /// UserHook 只安装一个 default，Prepare 再接管。
    DeferredDefault,
}

/// 业务作用：证明调用方持有当前 Bootstrapping owner，可执行关闭态安装与状态迁移。
#[derive(Clone, Debug)]
pub struct ManagedInstallationToken {
    owner: ManagedRegistryOwner,
    kind: BootstrapKind,
}

impl ManagedInstallationToken {
    /// 业务作用：读取 token 绑定的 Application owner。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只读 owner 引用，供 typed registry 绑定同一身份。
    pub fn owner(&self) -> &ManagedRegistryOwner {
        &self.owner
    }

    /// 业务作用：读取本次启动采用的引导路径。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置建池或延后 default 类型。
    pub fn kind(&self) -> BootstrapKind {
        self.kind
    }
}

/// 业务作用：作为单个 Application 的冻结 datasource 名称与 driver 权威。
pub struct DataSourceCatalog {
    owner: ManagedRegistryOwner,
    entries: BTreeMap<DatasourceRef, DatabaseDriver>,
    lifecycle: AtomicU8,
}

const CATALOG_STAGING: u8 = 0;
const CATALOG_OPEN: u8 = 1;
const CATALOG_STOPPED: u8 = 2;

impl DataSourceCatalog {
    /// 业务作用：构造尚未对 getter 开放的冻结 catalog。
    ///
    /// 参数说明：
    /// - `owner`：本次 Application 的共同 owner。
    /// - `entries`：datasource qualifier 与 driver 的完整集合。
    ///
    /// 返回：名称合法、不重复、非空且数量有界时返回关闭态 catalog；失败不发布部分表。
    pub fn try_new(
        owner: ManagedRegistryOwner,
        entries: impl IntoIterator<Item = (String, DatabaseDriver)>,
    ) -> Result<Self, CatalogBuildError> {
        let mut catalog = BTreeMap::new();
        for (name, driver) in entries {
            if catalog.len() >= MAX_MANAGED_DATASOURCES {
                return Err(CatalogBuildError::TooManyDatasources);
            }
            let reference = DatasourceRef::try_new(name).map_err(CatalogBuildError::InvalidName)?;
            if catalog.insert(reference.clone(), driver).is_some() {
                return Err(CatalogBuildError::Duplicate(reference));
            }
        }
        if catalog.is_empty() {
            return Err(CatalogBuildError::Empty);
        }
        Ok(Self {
            owner,
            entries: catalog,
            lifecycle: AtomicU8::new(CATALOG_STAGING),
        })
    }

    /// 业务作用：读取 catalog 绑定的 Application owner。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共同 owner 的只读引用。
    pub fn owner(&self) -> &ManagedRegistryOwner {
        &self.owner
    }

    /// 业务作用：返回冻结 catalog 的 datasource 数量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：跨 driver 合计的条目数。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 业务作用：判断 catalog 是否没有 datasource。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造成功后固定为假；该入口供通用容器检查使用。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 业务作用：复制冻结后的 datasource 与 driver 表，供资源登记和观测快照使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 datasource 名排序的拥有型条目。
    pub fn entries(&self) -> Vec<(DatasourceRef, DatabaseDriver)> {
        self.entries
            .iter()
            .map(|(reference, driver)| (reference.clone(), *driver))
            .collect()
    }

    /// 业务作用：在 catalog 已开放时解析名称并复验调用方期待的 driver。
    ///
    /// 参数说明：
    /// - `datasource`：待解析名称。
    /// - `expected`：typed getter 对应的 driver。
    ///
    /// 返回：名称和 driver 同时匹配时返回规范引用；关闭、缺失或错配时返回结构化错误。
    pub fn resolve(
        &self,
        datasource: &str,
        expected: DatabaseDriver,
    ) -> Result<DatasourceRef, DataSourceLookupError> {
        match self.lifecycle.load(Ordering::Acquire) {
            CATALOG_OPEN => {}
            CATALOG_STOPPED => return Err(DataSourceLookupError::Stopped),
            _ => return Err(DataSourceLookupError::RegistryUnavailable),
        }
        let reference =
            DatasourceRef::new(datasource).map_err(|_| DataSourceLookupError::InvalidName)?;
        let actual = self.entries.get(&reference).copied().ok_or_else(|| {
            DataSourceLookupError::NotFound {
                datasource: reference.clone(),
            }
        })?;
        if actual != expected {
            return Err(DataSourceLookupError::DriverMismatch {
                datasource: reference,
                expected,
                actual,
            });
        }
        Ok(reference)
    }

    /// 业务作用：在全部 typed registry 与资源容器提交后开放 catalog 查询。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；Release 写入保证后续 getter 看见完整冻结条目。
    fn open(&self) {
        self.lifecycle.store(CATALOG_OPEN, Ordering::Release);
    }

    /// 业务作用：在关闭连接池前永久封口 catalog，阻止停机期发放新资源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；后续查询得到 stopped 分类。
    fn stop_accepting(&self) {
        self.lifecycle.store(CATALOG_STOPPED, Ordering::Release);
    }
}

/// 业务作用：描述冻结 catalog 构造失败的无连接副作用原因。
#[derive(Debug, thiserror::Error)]
pub enum CatalogBuildError {
    /// 没有 datasource。
    #[error("managed datasource catalog cannot be empty")]
    Empty,
    /// 数量超过受管上限。
    #[error("managed datasource count exceeds the supported limit")]
    TooManyDatasources,
    /// 名称非法。
    #[error("invalid datasource name")]
    InvalidName(#[source] DatasourceNameError),
    /// 名称重复。
    #[error("datasource `{0}` is configured more than once")]
    Duplicate(DatasourceRef),
}

/// 业务作用：为 typed getter 提供稳定的 datasource 查找失败分类。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DataSourceLookupError {
    /// 名称格式非法。
    #[error("invalid datasource name")]
    InvalidName,
    /// catalog 尚未安装、尚未开放或已失去受管身份。
    #[error("datasource registry is unavailable")]
    RegistryUnavailable,
    /// catalog 已完成封口，旧 Application 不再发放连接。
    #[error("datasource registry is stopped")]
    Stopped,
    /// 名称不在当前 Application catalog。
    #[error("datasource `{datasource}` is not managed by this application")]
    NotFound {
        /// 已校验的 datasource 名称。
        datasource: DatasourceRef,
    },
    /// 名称存在但属于另一数据库 driver。
    #[error("datasource `{datasource}` uses {actual}, not {expected}")]
    DriverMismatch {
        /// 已校验的 datasource 名称。
        datasource: DatasourceRef,
        /// typed getter 期待的 driver。
        expected: DatabaseDriver,
        /// catalog 中的实际 driver。
        actual: DatabaseDriver,
    },
}

/// 业务作用：对显式事务裁决的全部失败阶段进行封闭分类。
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxDecision<T, E> {
    /// 当前执行已得到允许提交的领域结果。
    Commit(T),
    /// 当前执行没有可提交结果，必须回滚。
    Rollback(E),
}

/// 业务作用：保留物理回滚失败之前的原始裁决来源。
#[derive(Debug)]
pub enum TxRollbackCause<E> {
    /// 最外层事务体显式要求回滚。
    Decision(E),
    /// 内层事务要求 rollback-only，但外层吞掉了原始错误。
    RollbackOnly {
        /// 首次置位时保存的脱敏原因。
        reason: String,
    },
}

/// 业务作用：封闭提交、回滚和基础设施失败分类，供消息 ACK 与重试策略裁决。
#[derive(Debug)]
pub enum TxRunError<E> {
    /// 业务要求回滚，且数据库确认回滚完成。
    Rollback(E),
    /// 内层回滚要求被外层吞掉，最外层已确认整体回滚。
    RollbackOnly {
        /// 首次置位 rollback-only 时保存的脱敏原因。
        reason: String,
    },
    /// 数据库明确拒绝 COMMIT。
    CommitRejected {
        /// 稳定、脱敏的失败分类。
        reason: String,
    },
    /// COMMIT 后连接或协议状态不确定。
    CommitUncertain {
        /// 稳定、脱敏的失败分类。
        reason: String,
    },
    /// 物理回滚失败。
    RollbackFailed {
        /// 触发回滚的原始裁决。
        cause: TxRollbackCause<E>,
        /// 稳定、脱敏的基础设施分类。
        reason: String,
    },
    /// 在事务开始前或内核执行中发生基础设施错误。
    Infrastructure {
        /// 稳定、脱敏的失败分类。
        reason: String,
    },
}

/// 业务作用：让加法事务入口分别保留 datasource 查找与事务执行失败，避免调用方解析文本。
#[derive(Debug)]
pub enum TxEntryError<E> {
    /// 事务开始前的 datasource 名称、状态或 driver 校验失败。
    Lookup(DataSourceLookupError),
    /// 已进入对应 typed runtime 后的事务裁决失败。
    Run(TxRunError<E>),
}

impl<E: std::fmt::Display> std::fmt::Display for TxEntryError<E> {
    /// 业务作用：输出 checked 事务入口的脱敏失败阶段和稳定原因。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：分类写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lookup(error) => {
                write!(formatter, "transaction datasource lookup failed: {error}")
            }
            Self::Run(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl<E> std::error::Error for TxEntryError<E> where E: std::fmt::Debug + std::fmt::Display {}

/// 业务作用：描述数据库对 COMMIT 的可确认结果，供 driver 映射协议级错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// 数据库明确确认提交完成。
    Committed,
    /// 数据库返回明确拒绝，事务没有获得成功确认。
    Rejected,
    /// 请求发出后连接或协议中断，无法确认服务端最终结果。
    OutcomeUnknown,
}

impl<E: std::fmt::Display> std::fmt::Display for TxRunError<E> {
    /// 业务作用：输出不含 SQL、连接串和 payload 的事务失败分类。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：分类写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rollback(error) => write!(formatter, "transaction rolled back: {error}"),
            Self::RollbackOnly { reason } => {
                write!(formatter, "transaction was rollback-only: {reason}")
            }
            Self::CommitRejected { reason } => write!(formatter, "commit rejected: {reason}"),
            Self::CommitUncertain { reason } => write!(formatter, "commit uncertain: {reason}"),
            Self::RollbackFailed { reason, .. } => write!(formatter, "rollback failed: {reason}"),
            Self::Infrastructure { reason } => {
                write!(formatter, "transaction infrastructure failed: {reason}")
            }
        }
    }
}

impl<E> std::error::Error for TxRunError<E> where E: std::fmt::Debug + std::fmt::Display {}

/// 业务作用：描述 driver task-local 的跨后端嵌套拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("ambient transaction driver mismatch: active={active}, requested={requested}")]
pub struct DriverScopeError {
    /// 当前任务已绑定的 driver。
    pub active: DatabaseDriver,
    /// 本次调用请求的 driver。
    pub requested: DatabaseDriver,
}

tokio::task_local! {
    static ACTIVE_DRIVER: DatabaseDriver;
}

/// 业务作用：读取当前任务的 ambient transaction driver 哨兵。
///
/// 参数说明: 无。
///
/// 返回：事务体内返回对应 driver；普通任务与新 spawn 的任务返回 `None`。
pub fn current_driver() -> Option<DatabaseDriver> {
    ACTIVE_DRIVER.try_with(|driver| *driver).ok()
}

/// 业务作用：在执行 SQL 或进入 driver 事务前拒绝跨 driver 复用。
///
/// 参数说明：`requested` 是本次 typed 入口对应的 driver。
///
/// 返回：当前无哨兵或 driver 一致时成功；错配时返回两侧身份。
pub fn ensure_driver(requested: DatabaseDriver) -> Result<(), DriverScopeError> {
    match current_driver() {
        Some(active) if active != requested => Err(DriverScopeError { active, requested }),
        _ => Ok(()),
    }
}

/// 业务作用：仅在事务业务体执行期间发布 driver 哨兵，阻止嵌套进入另一后端。
///
/// 参数说明：
/// - `driver`：本地事务使用的数据库后端。
/// - `future`：需要在该后端事务上下文中执行的业务 future。
///
/// 返回：无外层 driver 或与外层一致时返回业务输出；错配时不执行 future。
pub async fn scope_driver<F>(
    driver: DatabaseDriver,
    future: F,
) -> Result<F::Output, DriverScopeError>
where
    F: Future,
{
    match current_driver() {
        Some(active) if active != driver => Err(DriverScopeError {
            active,
            requested: driver,
        }),
        Some(_) => Ok(future.await),
        None => Ok(ACTIVE_DRIVER.scope(driver, future).await),
    }
}

/// 业务作用：对进程级 datasource 模式变化给出稳定拒绝原因。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryCoordinationError {
    /// 另一 driver 已占用 standalone 模式。
    #[error("standalone datasource driver conflicts with the active driver")]
    StandaloneDriverConflict,
    /// 受管或引导模式拒绝普通 standalone 注册。
    #[error("standalone datasource registration conflicts with managed application state")]
    ManagedModeConflict,
    /// 延后引导只允许 default。
    #[error("deferred datasource bootstrap accepts only the default datasource")]
    DeferredOnlyDefault,
    /// 延后引导已记录另一 driver。
    #[error("deferred datasource bootstrap already uses another driver")]
    DeferredDriverConflict,
    /// 当前模式不能开始新的 Application 引导。
    #[error("datasource registry is already occupied")]
    RegistryOccupied,
    /// owner token 不属于当前启动。
    #[error("managed registry owner token does not match the active bootstrap")]
    OwnerMismatch,
    /// catalog 与 typed registry 没有使用同一 owner。
    #[error("datasource catalog owner does not match the installation token")]
    CatalogOwnerMismatch,
    /// catalog 实例与已发布实例不同。
    #[error("datasource catalog identity does not match the active managed catalog")]
    CatalogIdentityMismatch,
    /// 延后引导没有安装对应 driver 的 default。
    #[error("deferred datasource bootstrap did not install the expected default driver")]
    DeferredDriverMissing,
    /// 清理前仍有延后 standalone 资源。
    #[error("deferred datasource resources must be removed before bootstrap cleanup")]
    DeferredResourcesRemain,
    /// 无 token 的公开 adopt 不能越过 Bootstrapping。
    #[error("public standalone adopt is unavailable during application bootstrap")]
    PublicAdoptDuringBootstrap,
    /// 模式已失去 owner，但仍记录待清理资源。
    #[error("orphaned deferred datasource resources require explicit cleanup")]
    OrphanedDeferredResources,
    /// catalog 不符合延后 default 的单 driver 合同。
    #[error("deferred datasource catalog must contain only the installed default driver")]
    InvalidDeferredCatalog,
}

#[derive(Debug)]
enum RegistryMode {
    Empty,
    Standalone(DatabaseDriver),
    Bootstrapping {
        owner: Weak<OwnerIdentity>,
        kind: BootstrapKind,
        deferred_driver: Option<DatabaseDriver>,
    },
    Managed {
        owner: Weak<OwnerIdentity>,
        catalog: Weak<DataSourceCatalog>,
    },
}

#[derive(Debug)]
/// 业务作用：保存进程级 datasource 安装模式与当前 owner 弱引用，线性化独立和受管入口。
struct RegistryState {
    mode: RegistryMode,
}

impl Default for RegistryState {
    /// 业务作用：初始化尚无任何 datasource 权威的进程协调状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：模式为 Empty 的协调器内部状态。
    fn default() -> Self {
        Self {
            mode: RegistryMode::Empty,
        }
    }
}

static REGISTRY_STATE: OnceLock<Mutex<RegistryState>> = OnceLock::new();

/// 业务作用：暴露进程 datasource 模式的低基数诊断快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryModeSnapshot {
    /// 没有 datasource 权威。
    Empty,
    /// standalone driver 已占用进程入口。
    Standalone(DatabaseDriver),
    /// Application 正在构造受管资源。
    Bootstrapping {
        /// 启动路径。
        kind: BootstrapKind,
        /// UserHook 已安装的 default driver。
        deferred_driver: Option<DatabaseDriver>,
    },
    /// catalog 已安装，可能仍处于关闭态。
    Managed,
    /// owner 已失效但 driver 资源尚待精确清理。
    OrphanedDeferred(DatabaseDriver),
}

/// 业务作用：在唯一协调锁内组合 core 模式与 driver typed registry 的状态变化。
///
/// 参数说明：`operation` 必须只获取当前 driver 的 registry 锁，并保持 core → driver 的锁顺序。
///
/// 返回：原样返回闭包结果；协调锁 poison 时取回内部状态继续执行。
#[doc(hidden)]
pub fn coordinate<T>(operation: impl FnOnce(&mut RegistryCoordinator<'_>) -> T) -> T {
    let mut state = REGISTRY_STATE
        .get_or_init(|| Mutex::new(RegistryState::default()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let mut coordinator = RegistryCoordinator { state: &mut state };
    coordinator.sanitize();
    operation(&mut coordinator)
}

/// 业务作用：在唯一协调锁内执行受管模式迁移，供 typed driver crate 原子组合本地资源变化。
#[doc(hidden)]
pub struct RegistryCoordinator<'a> {
    state: &'a mut RegistryState,
}

impl RegistryCoordinator<'_> {
    /// 业务作用：清除不再持有资源的失效弱 owner，使同一进程能够启动新的 Application。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；仍记录 deferred pool 时保留模式并要求精确清理。
    fn sanitize(&mut self) {
        match &self.state.mode {
            RegistryMode::Bootstrapping {
                owner,
                deferred_driver: None,
                ..
            } if owner.upgrade().is_none() => {
                self.state.mode = RegistryMode::Empty;
            }
            RegistryMode::Managed { owner, catalog }
                if owner.upgrade().is_none() && catalog.upgrade().is_none() =>
            {
                self.state.mode = RegistryMode::Empty;
            }
            _ => {}
        }
    }

    /// 业务作用：登记一个 typed standalone datasource，并阻止跨 driver 或受管模式并存。
    ///
    /// 参数说明：
    /// - `driver`：执行登记的 typed driver。
    /// - `datasource`：即将写入 driver 本地表的名称。
    ///
    /// 返回：当前模式允许本地原子写入时成功；失败时调用方不得修改 pool 表。
    pub fn register_standalone(
        &mut self,
        driver: DatabaseDriver,
        datasource: &str,
    ) -> Result<(), RegistryCoordinationError> {
        match &mut self.state.mode {
            RegistryMode::Empty => {
                self.state.mode = RegistryMode::Standalone(driver);
                Ok(())
            }
            RegistryMode::Standalone(active) if *active == driver => Ok(()),
            RegistryMode::Standalone(_) => Err(RegistryCoordinationError::StandaloneDriverConflict),
            RegistryMode::Bootstrapping {
                owner,
                kind: BootstrapKind::DeferredDefault,
                deferred_driver,
            } => {
                if owner.upgrade().is_none() && deferred_driver.is_some() {
                    return Err(RegistryCoordinationError::OrphanedDeferredResources);
                }
                if datasource != DEFAULT_DATASOURCE {
                    return Err(RegistryCoordinationError::DeferredOnlyDefault);
                }
                match deferred_driver {
                    Some(active) if *active != driver => {
                        Err(RegistryCoordinationError::DeferredDriverConflict)
                    }
                    Some(_) => Ok(()),
                    slot @ None => {
                        *slot = Some(driver);
                        Ok(())
                    }
                }
            }
            RegistryMode::Bootstrapping { .. } | RegistryMode::Managed { .. } => {
                Err(RegistryCoordinationError::ManagedModeConflict)
            }
        }
    }

    /// 业务作用：在 driver 已确认本地 standalone 表为空后释放进程模式。
    ///
    /// 参数说明：`driver` 是刚完成精确清理的 typed driver。
    ///
    /// 返回：模式属于该 driver 时成功；另一 driver 或受管状态返回冲突。
    pub fn release_standalone(
        &mut self,
        driver: DatabaseDriver,
    ) -> Result<(), RegistryCoordinationError> {
        match &mut self.state.mode {
            RegistryMode::Standalone(active) if *active == driver => {
                self.state.mode = RegistryMode::Empty;
                Ok(())
            }
            RegistryMode::Bootstrapping {
                owner,
                kind: BootstrapKind::DeferredDefault,
                deferred_driver: Some(active),
            } if *active == driver => {
                let owner_alive = owner.upgrade().is_some();
                if owner_alive {
                    if let RegistryMode::Bootstrapping {
                        deferred_driver, ..
                    } = &mut self.state.mode
                    {
                        *deferred_driver = None;
                    }
                } else {
                    self.state.mode = RegistryMode::Empty;
                }
                Ok(())
            }
            RegistryMode::Empty => Ok(()),
            _ => Err(RegistryCoordinationError::ManagedModeConflict),
        }
    }

    /// 业务作用：开始一次配置建池或延后 default 的受管启动。
    ///
    /// 参数说明：
    /// - `owner`：本次 Application 唯一 owner。
    /// - `kind`：启动路径类型。
    ///
    /// 返回：进程入口为空时返回 owner token；已有资源时不改变模式。
    pub fn begin_bootstrap(
        &mut self,
        owner: &ManagedRegistryOwner,
        kind: BootstrapKind,
    ) -> Result<ManagedInstallationToken, RegistryCoordinationError> {
        if !matches!(self.state.mode, RegistryMode::Empty) {
            return Err(RegistryCoordinationError::RegistryOccupied);
        }
        self.state.mode = RegistryMode::Bootstrapping {
            owner: owner.downgrade(),
            kind,
            deferred_driver: None,
        };
        Ok(ManagedInstallationToken {
            owner: owner.clone(),
            kind,
        })
    }

    /// 业务作用：把普通 standalone 转入旧公开 adopt 使用的受管安装窗口。
    ///
    /// 参数说明：
    /// - `owner`：旧入口内部新建的 owner。
    /// - `driver`：必须与当前 standalone driver 一致。
    ///
    /// 返回：普通 standalone 时返回 token；Bootstrapping 明确拒绝无 token 越权接管。
    pub fn begin_legacy_adopt(
        &mut self,
        owner: &ManagedRegistryOwner,
        driver: DatabaseDriver,
    ) -> Result<ManagedInstallationToken, RegistryCoordinationError> {
        match &self.state.mode {
            RegistryMode::Standalone(active) if *active == driver => {
                self.state.mode = RegistryMode::Bootstrapping {
                    owner: owner.downgrade(),
                    kind: BootstrapKind::Configured,
                    deferred_driver: None,
                };
                Ok(ManagedInstallationToken {
                    owner: owner.clone(),
                    kind: BootstrapKind::Configured,
                })
            }
            RegistryMode::Bootstrapping { .. } => {
                Err(RegistryCoordinationError::PublicAdoptDuringBootstrap)
            }
            _ => Err(RegistryCoordinationError::RegistryOccupied),
        }
    }

    /// 业务作用：旧 adopt 发布失败时恢复调用前的 standalone 模式。
    ///
    /// 参数说明：
    /// - `token`：旧 adopt 创建的安装 token。
    /// - `driver`：恢复后的 standalone driver。
    ///
    /// 返回：owner 仍匹配时恢复；状态已被其它 owner 替换时拒绝。
    pub fn restore_legacy_standalone(
        &mut self,
        token: &ManagedInstallationToken,
        driver: DatabaseDriver,
    ) -> Result<(), RegistryCoordinationError> {
        self.verify_bootstrap_owner(token)?;
        self.state.mode = RegistryMode::Standalone(driver);
        Ok(())
    }

    /// 业务作用：复验延后 default 已由同一 owner 安装指定 driver，允许关闭态接管。
    ///
    /// 参数说明：
    /// - `token`：Application 在 Start 领取的 token。
    /// - `driver`：typed adopt 对应的 driver。
    ///
    /// 返回：owner、kind 与 deferred driver 全部匹配时成功。
    pub fn verify_deferred_adopt(
        &mut self,
        token: &ManagedInstallationToken,
        driver: DatabaseDriver,
    ) -> Result<(), RegistryCoordinationError> {
        self.verify_bootstrap_owner(token)?;
        match &self.state.mode {
            RegistryMode::Bootstrapping {
                kind: BootstrapKind::DeferredDefault,
                deferred_driver: Some(active),
                ..
            } if *active == driver => Ok(()),
            _ => Err(RegistryCoordinationError::DeferredDriverMissing),
        }
    }

    /// 业务作用：复验 typed registry 安装者持有当前 Bootstrapping owner token。
    ///
    /// 参数说明：`token` 是 driver crate 收到的安装凭据。
    ///
    /// 返回：owner 与启动类型同时匹配时成功；错配时不得修改 driver 本地槽。
    pub fn verify_installation_token(
        &self,
        token: &ManagedInstallationToken,
    ) -> Result<(), RegistryCoordinationError> {
        self.verify_bootstrap_owner(token)
    }

    /// 业务作用：把关闭态 catalog 安装为进程唯一受管权威，但尚不开放 getter。
    ///
    /// 参数说明：
    /// - `token`：当前 Application token。
    /// - `catalog`：已冻结且 owner 相同的完整 catalog。
    ///
    /// 返回：模式和 owner 匹配时转入 Managed；失败时 catalog 保持关闭。
    pub fn install_catalog(
        &mut self,
        token: &ManagedInstallationToken,
        catalog: &Arc<DataSourceCatalog>,
    ) -> Result<(), RegistryCoordinationError> {
        self.verify_bootstrap_owner(token)?;
        if !catalog.owner().ptr_eq(token.owner()) {
            return Err(RegistryCoordinationError::CatalogOwnerMismatch);
        }
        if let RegistryMode::Bootstrapping {
            kind: BootstrapKind::DeferredDefault,
            deferred_driver: Some(driver),
            ..
        } = &self.state.mode
        {
            let entries = catalog.entries();
            if entries.len() != 1
                || entries[0].0.as_str() != DEFAULT_DATASOURCE
                || entries[0].1 != *driver
            {
                return Err(RegistryCoordinationError::InvalidDeferredCatalog);
            }
        }
        self.state.mode = RegistryMode::Managed {
            owner: token.owner.downgrade(),
            catalog: Arc::downgrade(catalog),
        };
        Ok(())
    }

    /// 业务作用：在 typed registry 与 Application 资源表全部提交后开放唯一 catalog。
    ///
    /// 参数说明：
    /// - `token`：当前 Application token。
    /// - `catalog`：必须是已安装的同一 Arc 实例。
    ///
    /// 返回：身份复验通过时开放 getter；错配时保持关闭。
    pub fn open_catalog(
        &mut self,
        token: &ManagedInstallationToken,
        catalog: &Arc<DataSourceCatalog>,
    ) -> Result<(), RegistryCoordinationError> {
        match &self.state.mode {
            RegistryMode::Managed {
                owner,
                catalog: active,
            } => {
                let Some(active_owner) = owner.upgrade() else {
                    return Err(RegistryCoordinationError::OwnerMismatch);
                };
                if !Arc::ptr_eq(&active_owner, &token.owner.0) {
                    return Err(RegistryCoordinationError::OwnerMismatch);
                }
                let Some(active_catalog) = active.upgrade() else {
                    return Err(RegistryCoordinationError::CatalogIdentityMismatch);
                };
                if !Arc::ptr_eq(&active_catalog, catalog) {
                    return Err(RegistryCoordinationError::CatalogIdentityMismatch);
                }
                catalog.open();
                Ok(())
            }
            _ => Err(RegistryCoordinationError::OwnerMismatch),
        }
    }

    /// 业务作用：按 owner 封口并撤销当前 managed catalog，阻止旧 Application 清理新实例。
    ///
    /// 参数说明：`owner` 是正在逆序停机的 Application owner。
    ///
    /// 返回：owner 命中时撤销；不命中时保持现有权威并返回错误。
    pub fn clear_managed(
        &mut self,
        owner: &ManagedRegistryOwner,
    ) -> Result<(), RegistryCoordinationError> {
        match &self.state.mode {
            RegistryMode::Managed {
                owner: active,
                catalog,
            } => {
                let Some(active_owner) = active.upgrade() else {
                    return Err(RegistryCoordinationError::OwnerMismatch);
                };
                if !Arc::ptr_eq(&active_owner, &owner.0) {
                    return Err(RegistryCoordinationError::OwnerMismatch);
                }
                if let Some(catalog) = catalog.upgrade() {
                    catalog.stop_accepting();
                }
                self.state.mode = RegistryMode::Empty;
                Ok(())
            }
            _ => Err(RegistryCoordinationError::OwnerMismatch),
        }
    }

    /// 业务作用：在 typed deferred pool 已被精确取走后清除 driver 记录。
    ///
    /// 参数说明：
    /// - `token`：当前 Application token。
    /// - `driver`：刚完成本地清理的 driver。
    ///
    /// 返回：owner 与 driver 匹配时清除记录；错配时保留模式。
    pub fn clear_deferred_driver(
        &mut self,
        token: &ManagedInstallationToken,
        driver: DatabaseDriver,
    ) -> Result<(), RegistryCoordinationError> {
        self.verify_bootstrap_owner(token)?;
        match &mut self.state.mode {
            RegistryMode::Bootstrapping {
                kind: BootstrapKind::DeferredDefault,
                deferred_driver: Some(active),
                ..
            } if *active == driver => {
                if let RegistryMode::Bootstrapping {
                    deferred_driver, ..
                } = &mut self.state.mode
                {
                    *deferred_driver = None;
                }
                Ok(())
            }
            _ => Err(RegistryCoordinationError::DeferredDriverMissing),
        }
    }

    /// 业务作用：在本轮 typed 资源全部清空后释放 Bootstrapping 占用。
    ///
    /// 参数说明：`token` 是待撤销启动的 owner token。
    ///
    /// 返回：owner 匹配且没有 deferred 资源时回到 Empty；否则保留模式。
    pub fn abort_bootstrap(
        &mut self,
        token: &ManagedInstallationToken,
    ) -> Result<(), RegistryCoordinationError> {
        self.verify_bootstrap_owner(token)?;
        match &self.state.mode {
            RegistryMode::Bootstrapping {
                deferred_driver: Some(_),
                ..
            } => Err(RegistryCoordinationError::DeferredResourcesRemain),
            RegistryMode::Bootstrapping { .. } => {
                self.state.mode = RegistryMode::Empty;
                Ok(())
            }
            _ => Err(RegistryCoordinationError::OwnerMismatch),
        }
    }

    /// 业务作用：返回协调锁保护下的低基数模式快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不暴露 owner、URL 或 datasource 名称的模式分类。
    pub fn snapshot(&self) -> RegistryModeSnapshot {
        match &self.state.mode {
            RegistryMode::Empty => RegistryModeSnapshot::Empty,
            RegistryMode::Standalone(driver) => RegistryModeSnapshot::Standalone(*driver),
            RegistryMode::Bootstrapping {
                owner,
                kind,
                deferred_driver,
            } => match (owner.upgrade(), deferred_driver) {
                (None, Some(driver)) => RegistryModeSnapshot::OrphanedDeferred(*driver),
                _ => RegistryModeSnapshot::Bootstrapping {
                    kind: *kind,
                    deferred_driver: *deferred_driver,
                },
            },
            RegistryMode::Managed { .. } => RegistryModeSnapshot::Managed,
        }
    }

    /// 业务作用：复验 token 仍代表当前 Bootstrapping owner 与引导类型，阻止旧实例越权迁移状态。
    ///
    /// 参数说明：`token` 是 typed driver 收到的安装凭据。
    ///
    /// 返回：owner Arc 身份与 kind 同时匹配时成功；其它状态返回 owner mismatch。
    fn verify_bootstrap_owner(
        &self,
        token: &ManagedInstallationToken,
    ) -> Result<(), RegistryCoordinationError> {
        match &self.state.mode {
            RegistryMode::Bootstrapping { owner, kind, .. } => {
                let Some(active) = owner.upgrade() else {
                    return Err(RegistryCoordinationError::OwnerMismatch);
                };
                if Arc::ptr_eq(&active, &token.owner.0) && *kind == token.kind {
                    Ok(())
                } else {
                    Err(RegistryCoordinationError::OwnerMismatch)
                }
            }
            _ => Err(RegistryCoordinationError::OwnerMismatch),
        }
    }
}

/// 业务作用：读取当前进程 datasource 模式，供启动门禁和诊断复验。
///
/// 参数说明: 无。
///
/// 返回：已经执行弱 owner 自愈后的低基数模式快照。
pub fn registry_mode() -> RegistryModeSnapshot {
    coordinate(|coordinator| coordinator.snapshot())
}

/// 业务作用：为 Application 领取唯一 owner 并进入配置建池或延后 default 的启动模式。
///
/// 参数说明：`kind` 指明 datasource 来自冻结配置还是 UserHook。
///
/// 返回：进程入口为空时返回 owner 与同身份 token；已有资源时不改变当前权威。
pub fn begin_managed_bootstrap(
    kind: BootstrapKind,
) -> Result<(ManagedRegistryOwner, ManagedInstallationToken), RegistryCoordinationError> {
    let owner = ManagedRegistryOwner::new();
    let token = coordinate(|coordinator| coordinator.begin_bootstrap(&owner, kind))?;
    Ok((owner, token))
}

/// 业务作用：安装完整关闭态 catalog，形成尚不可读的 Managed 权威。
///
/// 参数说明：
/// - `token`：当前 Application 安装 token。
/// - `catalog`：已冻结且绑定同 owner 的 catalog。
///
/// 返回：owner 与模式匹配时完成安装；失败时 catalog 保持关闭。
pub fn install_managed_catalog(
    token: &ManagedInstallationToken,
    catalog: &Arc<DataSourceCatalog>,
) -> Result<(), RegistryCoordinationError> {
    coordinate(|coordinator| coordinator.install_catalog(token, catalog))
}

/// 业务作用：在 typed registry 与资源容器全部提交后开放 managed catalog。
///
/// 参数说明：
/// - `token`：当前 Application 安装 token。
/// - `catalog`：必须是此前安装的同一 Arc。
///
/// 返回：身份复验通过时开放；失败时 getter 继续返回不可用。
pub fn open_managed_catalog(
    token: &ManagedInstallationToken,
    catalog: &Arc<DataSourceCatalog>,
) -> Result<(), RegistryCoordinationError> {
    coordinate(|coordinator| coordinator.open_catalog(token, catalog))
}

/// 业务作用：在 driver 资源已封口后按 owner 撤销 managed catalog。
///
/// 参数说明：`owner` 是正在停机的 Application owner。
///
/// 返回：owner 命中时撤销；旧 owner 或非 Managed 模式返回错误且不影响新实例。
pub fn clear_managed_catalog(
    owner: &ManagedRegistryOwner,
) -> Result<(), RegistryCoordinationError> {
    coordinate(|coordinator| coordinator.clear_managed(owner))
}

/// 业务作用：在 typed 资源尚未发布或已经清空时释放 Bootstrapping。
///
/// 参数说明：`token` 是待撤销启动的 owner token。
///
/// 返回：没有延后资源时回到 Empty；仍有资源时拒绝，要求先由对应 driver 精确清理。
pub fn abort_managed_bootstrap(
    token: &ManagedInstallationToken,
) -> Result<(), RegistryCoordinationError> {
    coordinate(|coordinator| coordinator.abort_bootstrap(token))
}

/// 业务作用：解析全局 datasource 并复验 typed getter 的 driver 身份。
///
/// 参数说明：
/// - `datasource`：业务请求的 qualifier。
/// - `expected`：调用入口对应的 driver。
///
/// 返回：standalone 或开放 managed catalog 允许该 driver 时返回规范引用；其它模式返回结构化错误。
pub fn resolve_datasource(
    datasource: &str,
    expected: DatabaseDriver,
) -> Result<DatasourceRef, DataSourceLookupError> {
    coordinate(|coordinator| match &coordinator.state.mode {
        RegistryMode::Empty => {
            // 尚未安装任何 runtime 时只规范化名称，具体 typed registry 继续给出既有 NotFound 语义；
            // Bootstrapping 则必须保持不可读，不能把正在建立的权威误判成普通未初始化。
            DatasourceRef::new(datasource).map_err(|_| DataSourceLookupError::InvalidName)
        }
        RegistryMode::Bootstrapping { .. } => Err(DataSourceLookupError::RegistryUnavailable),
        RegistryMode::Standalone(actual) if *actual == expected => {
            DatasourceRef::new(datasource).map_err(|_| DataSourceLookupError::InvalidName)
        }
        RegistryMode::Standalone(actual) => {
            let reference =
                DatasourceRef::new(datasource).map_err(|_| DataSourceLookupError::InvalidName)?;
            Err(DataSourceLookupError::DriverMismatch {
                datasource: reference,
                expected,
                actual: *actual,
            })
        }
        RegistryMode::Managed { catalog, .. } => catalog
            .upgrade()
            .ok_or(DataSourceLookupError::RegistryUnavailable)?
            .resolve(datasource, expected),
    })
}

/// 业务作用：读取当前开放的 managed catalog，供 Application getter 与观测聚合使用。
///
/// 参数说明: 无。
///
/// 返回：Managed 且 catalog owner 仍存活时返回同一 Arc；其它模式返回不可用。
pub fn managed_catalog() -> Result<Arc<DataSourceCatalog>, DataSourceLookupError> {
    coordinate(|coordinator| match &coordinator.state.mode {
        RegistryMode::Managed { catalog, .. } => catalog
            .upgrade()
            .ok_or(DataSourceLookupError::RegistryUnavailable)
            .and_then(|catalog| match catalog.lifecycle.load(Ordering::Acquire) {
                CATALOG_OPEN => Ok(catalog),
                CATALOG_STOPPED => Err(DataSourceLookupError::Stopped),
                _ => Err(DataSourceLookupError::RegistryUnavailable),
            }),
        _ => Err(DataSourceLookupError::RegistryUnavailable),
    })
}
