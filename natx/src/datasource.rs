//! 数据源配置、连通性探测与连接池创建。
//!
//! 事务运行时本身只消费现成的 `MySqlPool`；本模块把"从配置造池"这件事收敛到 natx，
//! 让应用运行时只做编排，不再各自复制 SQLx 建池细节，也不必直接依赖 SQLx。

use std::str::FromStr;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{ConnectOptions, Connection, MySqlConnection, MySqlPool};

pub use natx_core::observability::StatementLogging;
pub use natx_core::DataSourcePoolConfig;

/// 单个数据源的连接与池化参数。
///
/// 该结构体是业务 YAML(`database` / `datasources.<name>`)的反序列化目标，也是探测和建池的唯一输入。
/// 它只在启动阶段被读取一次；池创建完成后由调用方持有 `MySqlPool`，本结构体不再参与运行期决策。
///
/// `Debug` 手工实现并对 `url` 脱敏：连接串通常内嵌密码，派生 Debug 会让任何
/// `tracing::info!(?cfg)` 直接把数据库口令写进日志。
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSourceConfig {
    /// MySQL/TiDB 连接串，形如 `mysql://user:password@host:port/database`。
    pub url: String,
    /// 连接池上限。
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// 连接池下限（预热并保持的空闲连接数）。
    #[serde(default)]
    pub min_connections: u32,
    /// 从池中获取连接的等待上限，毫秒。
    #[serde(default = "default_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,
    /// 建立单条 TCP/握手连接的上限，毫秒；同时作为启动探测的上限。
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// 启动时是否先用单连接探测真实连通性。
    ///
    /// 池是惰性的：跳过探测会把"地址写错/口令过期/库不存在"推迟到第一个请求，并且只表现为
    /// 模糊的 pool timeout。默认开启，用真实错误换取几十毫秒启动时间。
    #[serde(default = "default_probe_on_start")]
    pub probe_on_start: bool,
}

/// 业务作用：返回连接池上限的缺省值。
///
/// # 参数
///
/// 本函数无参数；缺省值面向单实例中等负载服务。
///
/// 返回：默认连接数上限。
fn default_max_connections() -> u32 {
    10
}

/// 业务作用：返回获取连接等待上限的缺省毫秒数。
///
/// # 参数
///
/// 本函数无参数；缺省值保证请求不会无限期排队等待连接。
///
/// 返回：毫秒单位的默认获取连接等待预算。
fn default_acquire_timeout_ms() -> u64 {
    2_000
}

/// 业务作用：返回建连上限的缺省毫秒数。
///
/// # 参数
///
/// 本函数无参数；缺省值覆盖常见跨机房握手耗时。
///
/// 返回：毫秒单位的默认建连预算。
fn default_connect_timeout_ms() -> u64 {
    5_000
}

/// 业务作用：返回是否默认执行启动探测。
///
/// # 参数
///
/// 本函数无参数；默认开启以便启动期就暴露真实连接错误。
///
/// 返回：固定为真。
fn default_probe_on_start() -> bool {
    true
}

impl std::fmt::Debug for DataSourceConfig {
    /// 业务作用：输出不含连接串凭据的调试视图。
    ///
    /// # 参数
    ///
    /// - `f`：Debug 输出使用的标准格式化器。
    ///
    /// 返回：脱敏字段写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataSourceConfig")
            .field("url", &natx_core::redact_url(&self.url))
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout_ms", &self.acquire_timeout_ms)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("probe_on_start", &self.probe_on_start)
            .finish()
    }
}

impl DataSourceConfig {
    /// 业务作用：校验会影响连通性与池行为的取值。
    ///
    /// 调用时机是建池之前；失败表示配置本身不可用，不会留下任何连接副作用。
    ///
    /// # 参数
    ///
    /// 本方法无显式参数；校验只读取自身字段，不访问网络。
    ///
    /// 返回：公共池参数与 MySQL scheme 均合法时成功，否则返回无连接副作用的配置错误。
    pub fn validate(&self) -> anyhow::Result<()> {
        match DataSourcePoolConfig::from(self).validate_common() {
            Ok(()) => {}
            Err(natx_core::PoolConfigError::EmptyUrl) => {
                anyhow::bail!("datasource url 不能为空")
            }
            Err(natx_core::PoolConfigError::ZeroMaxConnections) => {
                anyhow::bail!("datasource max_connections 必须大于 0")
            }
            Err(natx_core::PoolConfigError::InvalidConnectionRange) => {
                anyhow::bail!("datasource min_connections 不能大于 max_connections")
            }
            Err(natx_core::PoolConfigError::ZeroAcquireTimeout) => {
                anyhow::bail!("datasource acquire_timeout_ms 必须大于 0")
            }
            Err(natx_core::PoolConfigError::ZeroConnectTimeout) => {
                anyhow::bail!("datasource connect_timeout_ms 必须大于 0")
            }
        }
        anyhow::ensure!(
            self.url.starts_with("mysql://"),
            "datasource url 必须以 mysql:// 开头"
        );
        Ok(())
    }

    /// 业务作用：返回可安全写入日志和错误消息的定位信息（host:port/database）。
    ///
    /// 只保留 authority 中 `@` 之后的部分与路径，因此不会带出用户名或口令。
    ///
    /// # 参数
    ///
    /// 本方法无参数；无法解析时返回固定占位符而不是原始连接串。
    ///
    /// 返回：移除 scheme、userinfo、query 与 fragment 后的 endpoint。
    pub fn endpoint(&self) -> String {
        DataSourcePoolConfig::from(self).endpoint()
    }
}

impl From<&DataSourceConfig> for DataSourcePoolConfig {
    /// 业务作用：把既有 MySQL 配置投影为 driver 中立池参数，不改变公开 struct 字段。
    ///
    /// 参数说明：`config` 是已反序列化的 MySQL datasource 配置。
    ///
    /// 返回：字段逐一对应的公共池配置；URL scheme 仍由 MySQL 包装层校验。
    fn from(config: &DataSourceConfig) -> Self {
        Self {
            url: config.url.clone(),
            max_connections: config.max_connections,
            min_connections: config.min_connections,
            acquire_timeout_ms: config.acquire_timeout_ms,
            connect_timeout_ms: config.connect_timeout_ms,
            probe_on_start: config.probe_on_start,
        }
    }
}

/// 业务作用：用单条连接探测数据源真实可用性。
///
/// 这里刻意不复用连接池：池的获取超时会把 `Connection refused` / `Access denied` /
/// `Unknown database` 统一压成一个模糊的 acquire timeout，启动期因此看不到真正的失败原因。
/// 单连接握手把原始 SQLx 错误原样返回给调用方，由上层决定如何脱敏输出。
///
/// # 参数
///
/// - `config`：待探测的数据源配置；入口会再次校验，其 `connect_timeout_ms` 同时作为探测上限。
///
/// 返回：配置和握手均成功且探测连接已关闭时成功；否则返回连接或配置错误。
pub async fn probe(config: &DataSourceConfig) -> anyhow::Result<()> {
    // 公开探测入口自行复验配置，确保错误 driver 和无效预算不会触发网络动作。
    config.validate()?;
    let connect = MySqlConnection::connect(&config.url);
    let connection =
        tokio::time::timeout(Duration::from_millis(config.connect_timeout_ms), connect)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "连接 {} 超时({}ms)",
                    config.endpoint(),
                    config.connect_timeout_ms
                )
            })??;
    // 探测连接只用于确认握手，立即显式关闭，避免把一条连接遗留到池外。
    connection.close().await?;
    Ok(())
}

/// 业务作用：按配置创建惰性连接池。
///
/// 返回的池尚未建立任何连接（`min_connections > 0` 时由 SQLx 后台补足）；连通性应先由
/// [`probe`] 确认。池的所有权交给调用方，停机时必须显式 `close().await`。
///
/// # 参数
///
/// - `config`：待建池的数据源配置；入口会再次执行完整校验。
///
/// 返回：配置可被 SQLx 解析时返回惰性连接池；调用方负责在停机时关闭。
pub fn build_pool(config: &DataSourceConfig) -> anyhow::Result<MySqlPool> {
    // 惰性池不会立即建连，但仍必须在交出句柄前执行完整配置门禁。
    config.validate()?;
    let pool = MySqlPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .connect_lazy(&config.url)?;
    Ok(pool)
}

/// 业务作用：按受管语句日志策略探测真实 MySQL 连接。
/// 参数说明：`config` 是连接配置；`logging` 控制 SQLx 语句事件。
/// 返回：握手和关闭成功时完成；非法配置或连接失败返回错误。
pub async fn probe_with_logging(
    config: &DataSourceConfig,
    logging: StatementLogging,
) -> anyhow::Result<()> {
    config.validate()?;
    let options = logging_options(config, logging)?;
    let connection = tokio::time::timeout(
        Duration::from_millis(config.connect_timeout_ms),
        MySqlConnection::connect_with(&options),
    )
    .await
    .map_err(|_| anyhow::anyhow!("MySQL datasource connection timeout"))?
    .map_err(|_| anyhow::anyhow!("MySQL datasource connection failed"))?;
    // 探测会话不会进入业务池，成功后立即关闭，避免持有池外连接。
    connection
        .close()
        .await
        .map_err(|_| anyhow::anyhow!("MySQL datasource probe close failed"))?;
    Ok(())
}

/// 业务作用：建立由应用统一控制语句日志的 MySQL Pool。
/// 参数说明：`config` 是池配置；`logging` 是启动期冻结的日志开关。
/// 返回：配置合法时交出惰性池；调用方负责停机关闭。
pub fn build_pool_with_logging(
    config: &DataSourceConfig,
    logging: StatementLogging,
) -> anyhow::Result<MySqlPool> {
    config.validate()?;
    let options = logging_options(config, logging)?;
    Ok(MySqlPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .connect_lazy_with(options))
}

/// 业务作用：统一逐条语句事件并关闭 SQLx 自身的独立慢语句升级。
/// 参数说明：`config` 是待解析连接配置；`logging` 指定关闭或语句事件级别。
/// 返回：不保留 SQLx 默认慢日志策略的连接选项，URL 错误不包含凭据。
fn logging_options(
    config: &DataSourceConfig,
    logging: StatementLogging,
) -> anyhow::Result<MySqlConnectOptions> {
    let options = MySqlConnectOptions::from_str(&config.url)
        .map_err(|_| anyhow::anyhow!("MySQL datasource URL 无法解析"))?;
    let level = match logging {
        StatementLogging::Disabled => return Ok(options.disable_statement_logging()),
        StatementLogging::Debug => log::LevelFilter::Debug,
        StatementLogging::Trace => log::LevelFilter::Trace,
    };
    // 慢操作由 Mapper 业务阈值拥有，避免 SQLx 再输出一次不同级别的慢日志。
    Ok(options
        .log_statements(level)
        .log_slow_statements(level, Duration::MAX))
}
