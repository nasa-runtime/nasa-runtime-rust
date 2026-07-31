//! 数据源配置、连通性探测与连接池创建。
//!
//! 事务运行时本身只消费现成的 `MySqlPool`；本模块把"从配置造池"这件事收敛到 natx，
//! 让应用运行时只做编排，不再各自复制 SQLx 建池细节，也不必直接依赖 SQLx。

use std::time::Duration;

use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Connection, MySqlConnection, MySqlPool};

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

/// 返回连接池上限的缺省值。
///
/// # 参数
///
/// 本函数无参数；缺省值面向单实例中等负载服务。
fn default_max_connections() -> u32 {
    10
}

/// 返回获取连接等待上限的缺省毫秒数。
///
/// # 参数
///
/// 本函数无参数；缺省值保证请求不会无限期排队等待连接。
fn default_acquire_timeout_ms() -> u64 {
    2_000
}

/// 返回建连上限的缺省毫秒数。
///
/// # 参数
///
/// 本函数无参数；缺省值覆盖常见跨机房握手耗时。
fn default_connect_timeout_ms() -> u64 {
    5_000
}

/// 返回是否默认执行启动探测。
///
/// # 参数
///
/// 本函数无参数；默认开启以便启动期就暴露真实连接错误。
fn default_probe_on_start() -> bool {
    true
}

impl std::fmt::Debug for DataSourceConfig {
    /// 输出不含连接串凭据的调试视图。
    ///
    /// # 参数
    ///
    /// - `f`：Debug 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataSourceConfig")
            .field("url", &redact_url(&self.url))
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout_ms", &self.acquire_timeout_ms)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("probe_on_start", &self.probe_on_start)
            .finish()
    }
}

impl DataSourceConfig {
    /// 校验会影响连通性与池行为的取值。
    ///
    /// 调用时机是建池之前；失败表示配置本身不可用，不会留下任何连接副作用。
    ///
    /// # 参数
    ///
    /// 本方法无显式参数；校验只读取自身字段，不访问网络。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.url.trim().is_empty(), "datasource url 不能为空");
        anyhow::ensure!(
            self.url.starts_with("mysql://"),
            "datasource url 必须以 mysql:// 开头"
        );
        anyhow::ensure!(
            self.max_connections > 0,
            "datasource max_connections 必须大于 0"
        );
        anyhow::ensure!(
            self.min_connections <= self.max_connections,
            "datasource min_connections 不能大于 max_connections"
        );
        anyhow::ensure!(
            self.acquire_timeout_ms > 0,
            "datasource acquire_timeout_ms 必须大于 0"
        );
        anyhow::ensure!(
            self.connect_timeout_ms > 0,
            "datasource connect_timeout_ms 必须大于 0"
        );
        Ok(())
    }

    /// 返回可安全写入日志和错误消息的定位信息（host:port/database）。
    ///
    /// 只保留 authority 中 `@` 之后的部分与路径，因此不会带出用户名或口令。
    ///
    /// # 参数
    ///
    /// 本方法无参数；无法解析时返回固定占位符而不是原始连接串。
    pub fn endpoint(&self) -> String {
        let Some(rest) = self.url.strip_prefix("mysql://") else {
            return "<unknown-endpoint>".to_owned();
        };
        let authority_and_path = match rest.rfind('@') {
            Some(index) => &rest[index + 1..],
            None => rest,
        };
        let trimmed = authority_and_path
            .split(['?', '#'])
            .next()
            .unwrap_or(authority_and_path);
        if trimmed.is_empty() {
            "<unknown-endpoint>".to_owned()
        } else {
            trimmed.to_owned()
        }
    }
}

/// 用单条连接探测数据源真实可用性。
///
/// 这里刻意不复用连接池：池的获取超时会把 `Connection refused` / `Access denied` /
/// `Unknown database` 统一压成一个模糊的 acquire timeout，启动期因此看不到真正的失败原因。
/// 单连接握手把原始 SQLx 错误原样返回给调用方，由上层决定如何脱敏输出。
///
/// # 参数
///
/// - `config`：已经通过 [`DataSourceConfig::validate`] 的数据源配置；其 `connect_timeout_ms`
///   同时作为本次探测的上限，超时按连接失败处理。
pub async fn probe(config: &DataSourceConfig) -> anyhow::Result<()> {
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
    // 探测连接只用于验证握手，立即显式关闭，避免把一条连接遗留到池外。
    connection.close().await?;
    Ok(())
}

/// 按配置创建惰性连接池。
///
/// 返回的池尚未建立任何连接（`min_connections > 0` 时由 SQLx 后台补足）；连通性应先由
/// [`probe`] 确认。池的所有权交给调用方，停机时必须显式 `close().await`。
///
/// # 参数
///
/// - `config`：已经通过 [`DataSourceConfig::validate`] 的数据源配置。
pub fn build_pool(config: &DataSourceConfig) -> anyhow::Result<MySqlPool> {
    let pool = MySqlPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .connect_lazy(&config.url)?;
    Ok(pool)
}

/// 去掉连接串中的 userinfo，用于脱敏展示。
///
/// # 参数
///
/// - `url`：可能内嵌用户名和口令的原始连接串。
fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_owned();
    };
    let after = scheme_end + 3;
    match url[after..].find('@') {
        Some(at) => format!("{}***{}", &url[..after], &url[after + at..]),
        None => url.to_owned(),
    }
}
