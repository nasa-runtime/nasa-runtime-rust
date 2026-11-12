use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::time::Instant;
use url::Url;

use crate::api::error::Error;
use crate::api::props::ClientProps;

#[async_trait::async_trait]
/// 为连接选择提供可共享的地址快照，不承诺远端健康状态。
pub trait ServerListProvider: Send + Sync {
    /// 业务作用：为连接选择提供当前候选地址快照，允许实现自行刷新目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享地址列表；连接初始化要求至少一个地址，读取成功不证明远端可用，刷新失败的处理由实现决定。
    async fn current_server_list(&self) -> Arc<Vec<String>>;
}

pub(crate) struct StaticServerListProvider {
    /// 构造时确定、后续不会被替换的服务目录。
    server_list: Arc<Vec<String>>,
}

impl StaticServerListProvider {
    /// 业务作用：冻结已解析的静态服务目录，使连接选择共享同一地址集合。
    ///
    /// 参数说明：
    /// - `server_list`：调用方已校验的候选地址；构造器不拒绝空集合。
    ///
    /// 返回：持有目录所有权的提供者，不访问网络或补充缺省地址。
    pub(crate) fn new(server_list: Vec<String>) -> Self {
        Self {
            server_list: Arc::new(server_list),
        }
    }
}

#[async_trait::async_trait]
impl ServerListProvider for StaticServerListProvider {
    /// 业务作用：向本次连接选择提供构造时冻结的静态目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原目录的共享引用，不执行刷新，也不复制地址正文。
    async fn current_server_list(&self) -> Arc<Vec<String>> {
        Arc::clone(&self.server_list)
    }
}

pub(crate) struct EndpointServerListProvider {
    /// 按需刷新时使用的目录 URL。
    endpoint_url: String,
    /// 两次刷新尝试结束与下一次检查之间的最低间隔。
    refresh_interval: Duration,
    /// 最近成功解析的完整目录，刷新失败时继续保留。
    cached_server_list: ArcSwap<Vec<String>>,
    /// 构造时刻或最近一次刷新尝试结束时刻，独立于目录发布。
    last_refresh_at: ArcSwap<Instant>,
}

impl EndpointServerListProvider {
    /// 业务作用：以已获取的目录建立 endpoint 提供者，采用三十秒刷新间隔。
    ///
    /// 参数说明：
    /// - `endpoint_url`：后续刷新使用的完整 HTTP 地址。
    /// - `initial_server_list`：调用方已获取并校验的初始目录。
    ///
    /// 返回：带初始缓存的提供者；首次刷新时间从构造时刻计算，不立即重复请求。
    pub(crate) fn new(endpoint_url: String, initial_server_list: Vec<String>) -> Self {
        Self::with_refresh_interval(endpoint_url, initial_server_list, Duration::from_secs(30))
    }

    /// 业务作用：建立使用指定刷新间隔的 endpoint 目录缓存。
    ///
    /// 参数说明：
    /// - `endpoint_url`：刷新时请求的完整 URL，此入口不验证其合法性。
    /// - `initial_server_list`：首次刷新之前可用的目录，原样移入缓存。
    /// - `refresh_interval`：两次刷新检查之间的最低经过时间；零值允许每次读取尝试刷新。
    ///
    /// 返回：初始化缓存和计时状态的提供者；不创建后台定时任务或并发刷新互斥。
    pub(crate) fn with_refresh_interval(
        endpoint_url: String,
        initial_server_list: Vec<String>,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            endpoint_url,
            refresh_interval,
            cached_server_list: ArcSwap::from_pointee(initial_server_list),
            last_refresh_at: ArcSwap::from_pointee(Instant::now()),
        }
    }

    /// 业务作用：判断目录读取是否已达到下一次刷新尝试的时间条件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：距构造或最近刷新尝试结束已达间隔时为真；此判断不预占刷新权，并发读取可能同时通过。
    fn should_refresh(&self) -> bool {
        self.last_refresh_at.load().elapsed() >= self.refresh_interval
    }
}

#[async_trait::async_trait]
impl ServerListProvider for EndpointServerListProvider {
    /// 业务作用：按需刷新 endpoint 目录并向连接选择提供最近可用的地址缓存。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未到期直接共享缓存；到期请求成功后替换目录，失败保留旧目录。两种结果均更新尝试结束时间，不保证并发调用只刷新一次。
    async fn current_server_list(&self) -> Arc<Vec<String>> {
        if !self.should_refresh() {
            tracing::debug!(
                "Server list not yet due for refresh from endpoint {}, returning cached",
                self.endpoint_url
            );
            return Arc::clone(&self.cached_server_list.load());
        }

        match fetch_server_list(&self.endpoint_url).await {
            Ok(server_list) => {
                let cached = self.cached_server_list.load();
                let changed = !crate::common::util::unordered_eq(&cached, &server_list);
                if changed {
                    tracing::info!(
                        "Server list refreshed from endpoint {}: {:?} -> {:?}",
                        self.endpoint_url,
                        *cached,
                        server_list
                    );
                }
                // 只有获取和解析成功的完整目录才能替换缓存，连接选择不观察半成品。
                self.cached_server_list.store(Arc::new(server_list));
            }
            Err(e) => {
                // endpoint 暂不可用不撤销已有候选地址，仍由实际连接判断它们是否可达。
                tracing::error!(
                    "Failed to refresh server list from endpoint {}: {}",
                    self.endpoint_url,
                    e
                );
            }
        }
        // 失败也推进刷新间隔，避免每次连接选择立即重复请求故障 endpoint。
        self.last_refresh_at.store(Arc::new(Instant::now()));

        Arc::clone(&self.cached_server_list.load())
    }
}

/// 业务作用：根据客户端配置选择 endpoint 动态目录或静态地址目录，完成连接初始化前的首次校验。
///
/// 参数说明：
/// - `client_props`：提供 endpoint、命名空间及静态 server_addr；endpoint 存在时优先使用。
///
/// 返回：初始目录获取或解析成功时返回提供者；失败直接返回错误，不回退到低优先级配置。非法 endpoint URL 的构造可能 panic。
pub(crate) async fn create_server_list_provider(
    client_props: &ClientProps,
) -> crate::api::error::Result<Arc<dyn ServerListProvider>> {
    // 显式 endpoint 决定目录来源，获取失败不能悄悄连接另一份静态目录。
    if let Some(endpoint) = client_props.get_endpoint() {
        let namespace = client_props.get_namespace_default_if_empty();
        let endpoint_url = build_endpoint_url(&endpoint, &namespace);
        let initial_list = fetch_server_list(&endpoint_url).await?;
        tracing::info!(
            "Create EndpointServerListProvider with server_list={:?}, endpoint_url={}",
            initial_list,
            endpoint_url
        );
        let provider = EndpointServerListProvider::new(endpoint_url, initial_list);
        Ok(Arc::new(provider))
    } else {
        let server_addr = client_props.get_server_addr();
        let server_list = super::parse_server_list(&server_addr)?;
        tracing::info!(
            "Create StaticServerListProvider with server_list={:?}",
            server_list
        );
        Ok(Arc::new(StaticServerListProvider::new(server_list)))
    }
}

/// 业务作用：通过共享 HTTP 客户端获取 endpoint 目录，校验响应后归一地址及端口。
///
/// 参数说明：
/// - `endpoint_url`：待请求的完整目录地址，GET 成功后按文本解析地址列表。
///
/// 返回：非空的归一地址列表；网络、HTTP 错误状态、正文读取或列表解析失败均返回错误，不修改调用方缓存。
async fn fetch_server_list(endpoint_url: &str) -> crate::api::error::Result<Vec<String>> {
    tracing::debug!("Fetching server list from {}", endpoint_url);
    let http_client = crate::common::remote::http_client();
    let response = http_client.get(endpoint_url).send().await.map_err(|err| {
        Error::WrongServerAddress(format!(
            "Failed to fetch endpoint server list from {}: {}",
            endpoint_url, err
        ))
    })?;

    let response = response.error_for_status().map_err(|err| {
        Error::WrongServerAddress(format!(
            "Failed to fetch endpoint server list from {}: {}",
            endpoint_url, err
        ))
    })?;

    let body = response.text().await.map_err(|err| {
        Error::WrongServerAddress(format!(
            "Failed to read endpoint server list from {}: {}",
            endpoint_url, err
        ))
    })?;

    super::parse_server_list(&body)
}

/// 业务作用：把 endpoint 配置转换为目录请求 URL，并补充尚未显式指定的命名空间。
///
/// 参数说明：
/// - `endpoint`：完整 HTTP/HTTPS URL 或主机地址；裸地址使用 HTTP、默认 8080 端口及 /nacos/serverlist 路径。
/// - `namespace`：非空且 URL 尚无 namespace 查询项时追加的作用域，保留已有查询值。
///
/// 返回：规范化 URL；完整 URL 保留原路径和端口。URL 解析失败会 panic，此入口不执行网络请求。
pub(crate) fn build_endpoint_url(endpoint: &str, namespace: &str) -> String {
    const DEFAULT_ENDPOINT_PORT: u16 = 8080;
    const DEFAULT_ENDPOINT_URI: &str = "/nacos/serverlist";

    let endpoint = endpoint.trim();
    let mut url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Url::parse(endpoint).expect("endpoint URL should be valid")
    } else {
        // 只有裸地址补齐目录路径，完整 URL 的路径由调用方明确控制。
        let (host, port) = super::parse_host_port(endpoint);
        let port = port.unwrap_or(DEFAULT_ENDPOINT_PORT);
        Url::parse(&format!("http://{}:{}{}", host, port, DEFAULT_ENDPOINT_URI))
            .expect("constructed endpoint URL should be valid")
    };

    let namespace = namespace.trim();
    if !namespace.is_empty() && !url.query_pairs().any(|(k, _)| k == "namespace") {
        url.query_pairs_mut().append_pair("namespace", namespace);
    }

    url.to_string()
}
