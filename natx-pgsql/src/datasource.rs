//! PostgreSQL 数据源配置、连通性探测与连接池创建。

use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};

pub use natx_core::DataSourcePoolConfig;

/// 业务作用：承载 PostgreSQL datasource 的连接串与公共池参数。
///
/// 本类型与 MySQL 配置保持字段同形，但只接受 `postgres://` 和 `postgresql://`。
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSourceConfig {
    /// PostgreSQL 连接串。
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
    /// 启动时是否执行真实单连接探测。
    #[serde(default = "default_probe_on_start")]
    pub probe_on_start: bool,
}

impl std::fmt::Debug for DataSourceConfig {
    /// 业务作用：输出不包含 PostgreSQL 用户名和口令的配置视图。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：脱敏字段写入成功时返回 `Ok`，否则返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataSourceConfig")
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
    /// 业务作用：在网络 I/O 前校验公共池参数与 PostgreSQL URL scheme。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置可用于 PostgreSQL 建池时成功；非法参数返回脱敏错误。
    pub fn validate(&self) -> anyhow::Result<()> {
        DataSourcePoolConfig::from(self)
            .validate_common()
            .map_err(anyhow::Error::new)?;
        anyhow::ensure!(
            self.url.starts_with("postgres://") || self.url.starts_with("postgresql://"),
            "datasource url 必须以 postgres:// 或 postgresql:// 开头"
        );
        Ok(())
    }

    /// 业务作用：返回可安全写入日志的 PostgreSQL endpoint。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：移除 scheme、userinfo 和查询参数后的 host、端口与 database。
    pub fn endpoint(&self) -> String {
        DataSourcePoolConfig::from(self).endpoint()
    }
}

impl From<&DataSourceConfig> for DataSourcePoolConfig {
    /// 业务作用：把 PostgreSQL 包装配置投影成 backend 中立的池参数。
    ///
    /// 参数说明：`config` 是 PostgreSQL datasource 配置。
    ///
    /// 返回：字段逐一对应的公共池配置；scheme 仍由包装层负责。
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

/// 业务作用：用单条连接探测 PostgreSQL 地址、凭据与 database 的真实可用性，并沿用服务端会话默认值。
///
/// 参数说明：`config` 是待探测的配置；入口会再次执行完整校验。
///
/// 返回：配置和握手均成功且探测连接已关闭时返回 `Ok`；连接或关闭失败返回脱敏错误。
pub async fn probe(config: &DataSourceConfig) -> anyhow::Result<()> {
    // 省略业务 schema 时不得改写服务端 search_path，既有按登录角色选取对象的部署依赖这一语义。
    config.validate()?;
    let connect = PgConnection::connect(&config.url);
    let connection =
        tokio::time::timeout(Duration::from_millis(config.connect_timeout_ms), connect)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "连接 {} 超时({}ms)",
                    config.endpoint(),
                    config.connect_timeout_ms
                )
            })?
            .map_err(|_| anyhow::anyhow!("PostgreSQL 连接 {} 失败", config.endpoint()))?;
    // 探测连接不进入业务池，必须立即关闭以免启动期泄漏独立 session。
    connection
        .close()
        .await
        .map_err(|_| anyhow::anyhow!("PostgreSQL 探测连接 {} 关闭失败", config.endpoint()))?;
    Ok(())
}

/// 业务作用：用单条连接探测 PostgreSQL 地址、凭据、database 与指定业务 schema 的真实可用性。
///
/// 参数说明：
/// - `config`：待探测的公共连接与池配置。
/// - `schema`：业务连接与 migration 应共用的对象作用域。
///
/// 返回：握手后确认 `current_schema()` 与配置一致并显式关闭时返回 `Ok`；连接、schema 或关闭失败返回错误。
pub async fn probe_in_schema(config: &DataSourceConfig, schema: &str) -> anyhow::Result<()> {
    // 公开探测入口自行复验配置，确保错误 driver 和无效预算不会触发网络动作。
    let options = configured_options(config, schema)?;
    let connect = PgConnection::connect_with(&options);
    let mut connection =
        tokio::time::timeout(Duration::from_millis(config.connect_timeout_ms), connect)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "连接 {} 超时({}ms)",
                    config.endpoint(),
                    config.connect_timeout_ms
                )
            })?
            .map_err(|_| anyhow::anyhow!("PostgreSQL 连接 {} 失败", config.endpoint()))?;
    let schema_result = verify_current_schema(&mut connection, schema)
        .await
        .map_err(|_| anyhow::anyhow!("PostgreSQL datasource schema 不存在或不可用"));
    // 探测连接不进入业务池，必须立即关闭以免启动期泄漏独立 session。
    connection
        .close()
        .await
        .map_err(|_| anyhow::anyhow!("PostgreSQL 探测连接 {} 关闭失败", config.endpoint()))?;
    schema_result
}

/// 业务作用：按 PostgreSQL 配置创建惰性连接池，并保留服务端为新会话选择的默认 `search_path`。
///
/// 参数说明：`config` 是待建池的配置；入口会再次执行完整校验。
///
/// 返回：配置可被 SQLx 解析时返回 PgPool；调用方负责在停机时显式关闭。
pub fn build_pool(config: &DataSourceConfig) -> anyhow::Result<PgPool> {
    // 省略业务 schema 的兼容入口保留 URL 与服务端会话参数，不附加隐式 search_path。
    config.validate()?;
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .connect_lazy(&config.url)
        .map_err(|_| anyhow::anyhow!("PostgreSQL datasource URL 无法解析"))?;
    Ok(pool)
}

/// 业务作用：按 PostgreSQL 配置创建惰性连接池，并让每条业务连接固定使用指定 schema。
///
/// 参数说明：
/// - `config`：待建池的公共连接与池配置。
/// - `schema`：业务连接与 migration 应共用的对象作用域。
///
/// 返回：配置可被 SQLx 解析时返回 PgPool；新连接无法进入目标 schema 时拒绝加入池，调用方负责显式关闭。
pub fn build_pool_in_schema(config: &DataSourceConfig, schema: &str) -> anyhow::Result<PgPool> {
    // 惰性池不会立即建连，但仍必须在交出句柄前执行完整配置门禁。
    let options = configured_options(config, schema)?;
    let schema = schema.to_owned();
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .after_connect(move |connection, _metadata| {
            let schema = schema.clone();
            Box::pin(async move { verify_current_schema(connection, &schema).await })
        })
        .connect_lazy_with(options);
    Ok(pool)
}

/// 业务作用：把 datasource schema 写入 PostgreSQL 启动参数，使业务 SQL 与 migration 使用同一对象作用域。
///
/// 参数说明：
/// - `config`：已声明连接串和池预算的数据源配置。
/// - `schema`：已通过调用边界传入的目标 schema。
///
/// 返回：完整配置合法且 URL 可解析时返回连接选项；错误不会包含凭据或 schema 名称。
fn configured_options(config: &DataSourceConfig, schema: &str) -> anyhow::Result<PgConnectOptions> {
    config.validate()?;
    validate_schema_identifier(schema)?;
    let options = PgConnectOptions::from_str(&config.url)
        .map_err(|_| anyhow::anyhow!("PostgreSQL datasource URL 无法解析"))?;
    // schema 必须在握手阶段成为 session 默认值，避免 migration 成功后业务 SQL 回落到 `public`。
    Ok(options.options([("search_path", schema)]))
}

/// 业务作用：复验服务端实际选择的 schema，阻止不存在的 search_path 以成功连接进入业务池。
///
/// 参数说明：
/// - `connection`：刚完成握手、尚未承载业务请求的连接。
/// - `expected`：已通过 identifier 校验的目标 schema。
///
/// 返回：`current_schema()` 与配置一致时成功；schema 缺失、无权限或查询失败时拒绝该连接。
async fn verify_current_schema(
    connection: &mut PgConnection,
    expected: &str,
) -> Result<(), sqlx::Error> {
    let current: Option<String> = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(connection)
        .await?;
    if current.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(sqlx::Error::Protocol(
            "configured PostgreSQL datasource schema is unavailable".to_owned(),
        ))
    }
}

/// 业务作用：校验 schema 可安全作为单一 PostgreSQL identifier 与连接启动参数。
///
/// 参数说明：
/// - `schema`：配置提供的业务对象作用域。
///
/// 返回：ASCII 普通 identifier 且不超过 PostgreSQL 63 字节上限时成功，其它输入在网络动作前拒绝。
fn validate_schema_identifier(schema: &str) -> anyhow::Result<()> {
    let mut bytes = schema.bytes();
    let Some(first) = bytes.next() else {
        anyhow::bail!("datasource schema identifier is invalid");
    };
    if schema.len() > 63
        || !(first == b'_' || first.is_ascii_alphabetic())
        || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
    {
        anyhow::bail!("datasource schema identifier is invalid");
    }
    Ok(())
}

/// 业务作用：返回 PostgreSQL 连接池上限的缺省值。
///
/// 参数说明: 无。
///
/// 返回：普通服务实例使用的连接数上限。
fn default_max_connections() -> u32 {
    10
}

/// 业务作用：返回获取 PostgreSQL 连接的缺省等待预算。
///
/// 参数说明: 无。
///
/// 返回：毫秒单位的有限等待预算。
fn default_acquire_timeout_ms() -> u64 {
    2_000
}

/// 业务作用：返回 PostgreSQL 握手的缺省等待预算。
///
/// 参数说明: 无。
///
/// 返回：毫秒单位的建连预算。
fn default_connect_timeout_ms() -> u64 {
    5_000
}

/// 业务作用：返回是否默认执行启动探测。
///
/// 参数说明: 无。
///
/// 返回：固定为真，使连接配置错误在启动期失败。
fn default_probe_on_start() -> bool {
    true
}
