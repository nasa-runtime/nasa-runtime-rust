//! `RestDiscoveryClient`:真实 HTTP client(reqwest + 客户端负载均衡 + URL 重写)。
//!
//! 三档识别见 [`crate::classify`]。热路径只读 watch 快照 + RR 选址,在 `send().await` 时才解析 URL、选实例、重写。

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nabudget::RequestBudget;
use nadisc::{is_traffic_instance, DiscoveryClient, Instance};
use natelemetry::{random_span_id, SpanKind, TraceContext};
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::classify::{self, Classified};
use crate::error::{RestDiscoveryError, Result};
use crate::index::ServiceNameIndex;
use crate::instances::InstanceSource;
use crate::lb::{LoadBalancer, RoundRobinLoadBalancer, WeightedRoundRobinLoadBalancer};
use crate::metrics::{RestMetrics, RestMetricsSnapshot};
use crate::options::{
    HeuristicHttpMode, InstanceScheme, LbStrategy, RestDiscoveryOptions, RestHttpOptions,
    SchemePolicy, StartupPolicy, UnknownHostPolicy,
};
use crate::resilience::ResilienceRuntime;

/// 带服务发现 + 负载均衡的 HTTP client。`discovery=None` 即 external-only(`rest_discovery.enabled=false`)。
pub struct RestDiscoveryClient {
    http: reqwest::Client,
    background: Arc<crate::background::BackgroundOwner>,
    /// `None` = external-only:`service_request`/`lb://` 返回 `DiscoveryDisabledForInternalCall`。
    /// `Arc`:索引刷新任务持 `Weak` 以在服务名超 grace 移除时回调 `mark_removed`(churn 回收)。
    source: Option<Arc<InstanceSource>>,
    lb: Arc<dyn LoadBalancer>,
    options: RestDiscoveryOptions,
    /// 裸 http(s) 启发式服务名索引;仅 `connect()` + `heuristic_http=Enabled` 时为 `Some`。
    index: Option<Arc<ServiceNameIndex>>,
    /// 索引周期刷新任务句柄;`shutdown_background` 时 abort。
    index_refresh_task: Option<tokio::task::AbortHandle>,
    /// 分流决策指标(各档命中 / 无实例 / 发现失败 / watch 降级 / 重试)。
    metrics: Arc<RestMetrics>,
    /// 每服务 retry token bucket；只在失败路径短暂加锁，不跨 await。
    retry_budgets: DashMap<String, std::sync::Mutex<RetryBucket>>,
    /// 每服务 bulkhead/熔断与实例异常摘除状态。
    resilience: ResilienceRuntime,
}

impl std::fmt::Debug for RestDiscoveryClient {
    /// 业务作用：展示 REST 客户端的运行模式与协议配置。
    ///
    /// 参数说明：
    /// - `f`：接收调试表示的格式化目标。
    ///
    /// 返回：输出是否外部专用、是否有启发式索引及默认协议，格式化失败传播；不输出发现后端内部对象。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestDiscoveryClient")
            .field("external_only", &self.source.is_none())
            .field("heuristic_index", &self.index.is_some())
            .field(
                "default_instance_scheme",
                &self.options.default_instance_scheme,
            )
            .finish_non_exhaustive()
    }
}

impl Drop for RestDiscoveryClient {
    /// 业务作用：在客户端释放时关闭并终止其后台观察责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用统一后台关闭入口，不同步等待索引刷新或 watch 任务全部退出。
    fn drop(&mut self) {
        self.shutdown_background();
    }
}

impl RestDiscoveryClient {
    /// 业务作用：同步构造支持显式内部服务调用的 REST 客户端。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    ///
    /// 返回：配置有效时返回客户端，失败 panic；不执行异步服务名首拉，即使配置启发式识别也不建立该索引。可恢复构造失败应使用 try_new。
    pub fn new(discovery: Arc<dyn DiscoveryClient>, options: RestDiscoveryOptions) -> Self {
        Self::try_new(discovery, options).expect("rest-discovery: 非法 RestDiscoveryOptions")
    }

    /// 业务作用：校验选项后同步组装内部服务 REST 客户端。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    ///
    /// 返回：返回采用内置选址算法的客户端，非法选项返回错误；不启动启发式服务名索引，HTTP 连接池构造失败仍可 panic。
    pub fn try_new(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
    ) -> Result<Self> {
        let lb = build_lb(options.lb_strategy);
        Self::assemble_sync(discovery, options, lb)
    }

    /// 业务作用：同步组装使用自定义选址算法的内部客户端。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    /// - `load_balancer`：取代内置 lb_strategy 的共享选址算法。
    ///
    /// 返回：校验成功返回客户端，非法选项返回错误；不创建启发式索引，HTTP 连接池构造失败仍可 panic。
    pub fn try_new_with_load_balancer(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        load_balancer: Arc<dyn LoadBalancer>,
    ) -> Result<Self> {
        Self::assemble_sync(discovery, options, load_balancer)
    }

    /// 业务作用：复用选项校验、实例目录与连接池装配以构造同步内部客户端。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    /// - `lb`：已经选定的共享负载均衡算法。
    ///
    /// 返回：返回未建立服务名索引及实例订阅的客户端；非法运行策略返回错误，HTTP 连接池构造失败 panic。
    fn assemble_sync(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        lb: Arc<dyn LoadBalancer>,
    ) -> Result<Self> {
        // `new()` 只服务显式内部调用,不会启动服务名索引;因此只校验实际会被使用的 http/watch 字段。
        options.validate_for_new()?;
        if options.heuristic_http == HeuristicHttpMode::Enabled {
            tracing::warn!(
                "rest-discovery: RestDiscoveryClient::new 不会启动启发式服务名索引(heuristic_http=Enabled 被忽略);\
                 裸 http(s) 仍按外部直连。需启发式请用 connect() 或 RestDiscovery::init_with_discovery"
            );
        }
        let metrics = Arc::new(RestMetrics::default());
        let background = Arc::new(crate::background::BackgroundOwner::new());
        let source = Arc::new(InstanceSource::new(
            discovery,
            options.watch.clone(),
            metrics.clone(),
            options.no_instance,
            background.clone(),
        ));
        Ok(Self {
            http: build_http_client(&options.http),
            source: Some(source),
            background,
            lb,
            options,
            index: None,
            index_refresh_task: None,
            metrics,
            retry_budgets: DashMap::new(),
            resilience: ResilienceRuntime::new(),
        })
    }

    /// 业务作用：异步构造内部客户端，并按配置首拉及维护服务名索引。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    ///
    /// 返回：返回使用内置选址算法的客户端；配置非法、强制首拉失败、索引容量或任务接纳失败返回错误。允许空索引策略可容忍枚举失败；连接池构造失败仍可 panic。
    pub async fn connect(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
    ) -> Result<Self> {
        let lb = build_lb(options.lb_strategy);
        Self::connect_inner(discovery, options, lb).await
    }

    /// 业务作用：按显式选址算法初始化内部客户端及可选启发式索引。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    /// - `load_balancer`：替代内置策略并供内部请求共享的算法。
    ///
    /// 返回：初始化成功返回客户端，配置、必须成功的枚举或任务建立失败传播；允许空索引策略保留原语义，连接池构造失败仍可 panic。
    pub async fn connect_with_load_balancer(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        load_balancer: Arc<dyn LoadBalancer>,
    ) -> Result<Self> {
        Self::connect_inner(discovery, options, load_balancer).await
    }

    /// 业务作用：在发布客户端前完成配置检查、必要的索引首拉及后台刷新接管。
    ///
    /// 参数说明：
    /// - `discovery`：提供服务实例发现和订阅的共享后端。
    /// - `options`：HTTP、观察、重试及保护运行策略。
    /// - `lb`：当前客户端将固定使用的选址器。
    ///
    /// 返回：启用启发式时按启动策略处理首拉，再登记索引刷新任务并返回客户端；其它情况不建立索引。配置、索引或接纳失败返回错误，不承诺后端始终存在可用实例。
    async fn connect_inner(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        lb: Arc<dyn LoadBalancer>,
    ) -> Result<Self> {
        // `connect()` 会同步首拉服务名并启动索引刷新;启发式索引用到的间隔字段必须在建任务前校验。
        options.validate_for_connect()?;
        let metrics = Arc::new(RestMetrics::default());
        // InstanceSource 是内部调用的事实入口:显式 service、lb://、启发式命中都会先经它取 watch 快照。
        // 它必须在索引刷新任务启动前创建,这样刷新任务移除服务时才能回调 mark_removed 回收对应 watch。
        let background = Arc::new(crate::background::BackgroundOwner::new());
        let source = Arc::new(InstanceSource::new(
            discovery.clone(),
            options.watch.clone(),
            metrics.clone(),
            options.no_instance,
            background.clone(),
        ));

        let (index, index_refresh_task) = if options.heuristic_http == HeuristicHttpMode::Enabled {
            let index = Arc::new(ServiceNameIndex::new(
                options.service_match,
                options.heuristic.removed_service_grace,
                options.watch.max_services,
            ));
            // 首拉必须发生在 client 发布前:否则首批裸 http(s) 请求会看到空索引,把内部服务误判为外部。
            match discovery.list_services().await {
                // 首拉时旧索引为空 → 不会有 removed,丢弃返回值。
                Ok(names) => {
                    index.refresh(names, Instant::now())?;
                }
                Err(e) => match options.startup {
                    StartupPolicy::RequireInitialServiceListWhenHeuristicEnabled => {
                        return Err(RestDiscoveryError::DiscoveryFailed {
                            service: "<list_services>".to_string(),
                            source: e,
                        });
                    }
                    StartupPolicy::AllowEmptyServiceIndex => {
                        tracing::warn!(error = %e, "rest-discovery: 首次 list_services 失败,按配置允许空索引启动");
                    }
                },
            }
            // 刷新任务只持 Weak,避免后台循环延长 client 生命周期;服务名超 grace 移出索引时回收对应 watch。
            let task = background.spawn(index_refresh_loop(
                discovery.clone(),
                index.clone(),
                options.heuristic.refresh_interval,
                Arc::downgrade(&source),
            ))?;
            (Some(index), Some(task))
        } else {
            (None, None)
        };

        Ok(Self {
            http: build_http_client(&options.http),
            source: Some(source),
            background,
            lb,
            options,
            index,
            index_refresh_task,
            metrics,
            retry_budgets: DashMap::new(),
            resilience: ResilienceRuntime::new(),
        })
    }

    /// 业务作用：构造不含发现后端的外部 HTTP 客户端便利入口。
    ///
    /// 参数说明：
    /// - `options`：外部请求使用的配置，非法时本入口 panic。
    ///
    /// 返回：成功返回客户端；配置错误或连接池构造失败 panic，显式内部调用仍被拒绝。
    pub fn external_only(options: RestDiscoveryOptions) -> Self {
        Self::try_external_only(options).expect("rest-discovery: 非法 RestDiscoveryOptions")
    }

    /// 业务作用：校验外部请求预算、重试及保护选项并构造外部客户端。
    ///
    /// 参数说明：
    /// - `options`：外部客户端运行策略，不要求发现或启发式索引配置可用。
    ///
    /// 返回：合法配置返回无实例源及索引的客户端，非法配置返回错误；连接池构造失败仍可 panic，不连接发现后端。
    pub fn try_external_only(options: RestDiscoveryOptions) -> Result<Self> {
        options.validate_for_external_only()?;
        // external-only 不会进入 send_internal,这里保留一个内置 LB 只是为了结构体字段完整;
        // 显式内部调用会在取 source 时先返回 DiscoveryDisabledForInternalCall。
        let lb = build_lb(options.lb_strategy);
        Ok(Self {
            http: build_http_client(&options.http),
            source: None,
            background: Arc::new(crate::background::BackgroundOwner::new()),
            lb,
            options,
            index: None,
            index_refresh_task: None,
            metrics: Arc::new(RestMetrics::default()),
            retry_budgets: DashMap::new(),
            resilience: ResilienceRuntime::new(),
        })
    }

    /// 业务作用：检查客户端是否没有内部实例来源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：没有实例源时返回 true，否则 false；不查询后端健康状态。
    pub fn is_external_only(&self) -> bool {
        self.source.is_none()
    }

    /// 业务作用：取得当前分流及发现运行计数的快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回共享指标在本次读取时的快照，不清零计数，不提供所有原子项同一瞬间一致的保证。
    pub fn metrics(&self) -> RestMetricsSnapshot {
        self.metrics.snapshot()
    }

    // ── 请求入口(receiver 为 `&Arc<Self>`:builder 持 `Arc<Self>` 以便 send 时回调) ──

    /// 业务作用：创建延迟执行的通用 HTTP 请求构造器。
    ///
    /// 参数说明：
    /// - `method`：真正发送时使用的 HTTP 方法。
    /// - `url`：支持 lb、HTTP 或 HTTPS 的 URL 文本，在发送时解析。
    ///
    /// 返回：返回持有客户端 Arc 的构造器；此时不发现实例、不解析 URL 或发送请求。
    pub fn request(self: &Arc<Self>, method: Method, url: impl AsRef<str>) -> RestRequestBuilder {
        RestRequestBuilder::new(
            self.clone(),
            method,
            BuilderInput::Url(url.as_ref().to_string()),
        )
    }

    /// 业务作用：按完整 HTTP(S) 地址发起外部调用，不使用服务名索引或注册中心选址。
    ///
    /// 参数说明：
    /// - `method`：为 HTTP 方法
    /// - `url`：为外部 HTTP(S) 地址，可包含 query。
    ///
    /// 返回：共享本客户端连接池、请求期限和关闭门禁的构建器；发送时拒绝非法地址与其他协议。
    pub fn external_request(
        self: &Arc<Self>,
        method: Method,
        url: impl AsRef<str>,
    ) -> RestRequestBuilder {
        RestRequestBuilder::new(
            self.clone(),
            method,
            BuilderInput::ExternalUrl(url.as_ref().to_string()),
        )
    }

    /// 业务作用：创建延迟执行的 GET 请求构造器。
    ///
    /// 参数说明：
    /// - `url`：发送时解析的 lb、HTTP 或 HTTPS URL 文本。
    ///
    /// 返回：返回持有客户端的构造器，此时不发送请求或读取远端数据。
    pub fn get(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::GET, url)
    }

    /// 业务作用：创建延迟执行的 POST 请求构造器。
    ///
    /// 参数说明：
    /// - `url`：发送时解析的 lb、HTTP 或 HTTPS URL 文本。
    ///
    /// 返回：返回持有客户端的构造器，此时不发送请求或读取远端数据。
    pub fn post(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::POST, url)
    }

    /// 业务作用：创建延迟执行的 PUT 请求构造器。
    ///
    /// 参数说明：
    /// - `url`：发送时解析的 lb、HTTP 或 HTTPS URL 文本。
    ///
    /// 返回：返回持有客户端的构造器，此时不发送请求或读取远端数据。
    pub fn put(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::PUT, url)
    }

    /// 业务作用：创建延迟执行的 DELETE 请求构造器。
    ///
    /// 参数说明：
    /// - `url`：发送时解析的 lb、HTTP 或 HTTPS URL 文本。
    ///
    /// 返回：返回持有客户端的构造器，此时不发送请求或读取远端数据。
    pub fn delete(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::DELETE, url)
    }

    /// 业务作用：为显式逻辑服务创建延迟执行的内部请求。
    ///
    /// 参数说明：
    /// - `service`：将在发送时校验并发现的逻辑服务名。
    /// - `method`：目标业务 HTTP 方法。
    /// - `path`：发送时要求以斜杠开头的逻辑路径及可选查询串。
    ///
    /// 返回：返回请求构造器，此时不访问后端；发送时保持显式内部路由语义，不因无实例回退为外部 DNS。
    pub fn service_request(
        self: &Arc<Self>,
        service: impl Into<String>,
        method: Method,
        path: impl Into<String>,
    ) -> RestRequestBuilder {
        RestRequestBuilder::new(
            self.clone(),
            method,
            BuilderInput::Service {
                service: service.into(),
                path: path.into(),
            },
        )
    }

    /// 业务作用：永久关闭当前实例准入并通知索引和订阅任务退出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：关闭立即生效；实际任务退出由 shutdown 等待确认。
    pub fn shutdown_background(&self) {
        self.background.close();
        if let Some(h) = &self.index_refresh_task {
            h.abort();
        }
        if let Some(source) = &self.source {
            source.abort_all();
        }
    }

    /// 业务作用：关闭当前实例并在宿主截止点内等待其调用和后台任务退出。
    ///
    /// 参数说明：
    /// - `deadline`：为共享的绝对停机截止点。
    ///
    /// 返回：任务和调用全部退出时成功；超时返回未排干，旧句柄持续拒绝新工作。
    /// 原始 send 返回的 reqwest 响应体归调用方持有；便捷解码方法的响应体由本 owner 管理。
    pub async fn shutdown(&self, deadline: tokio::time::Instant) -> Result<()> {
        self.shutdown_background();
        self.background.wait(deadline).await
    }

    // ── 解析与选址(send 时调用) ──

    /// 业务作用：解析请求输入并形成内部服务计划或外部 URL。
    ///
    /// 参数说明：
    /// - `input`：显式服务、自动分类 URL 或强制外部 URL 输入。
    /// - `scheme_override`：只作用于显式内部服务和 lb 输入的协议覆盖。
    /// - `query_parts`：待拼接的已编码查询片段。
    ///
    /// 返回：返回待选址的内部计划或外部地址；非法 URL、服务名、路径或禁止的未知 host 返回错误。仅查现有服务名索引，不在此选择实例或请求发现后端。
    fn resolve_plan(
        &self,
        input: &BuilderInput,
        scheme_override: Option<InstanceScheme>,
        query_parts: &[String],
    ) -> Result<RequestPlan> {
        match input {
            BuilderInput::ExternalUrl(raw) => {
                let url = append_external_query(raw, query_parts)?;
                // 显式外部地址不能被同名注册服务接管，也不能借 lb 协议进入内部发现路径。
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(RestDiscoveryError::InvalidUrl {
                        url: raw.clone(),
                        reason: "外部请求只接受带 host 的 http/https 地址".into(),
                    });
                }
                self.metrics.external();
                Ok(RequestPlan::External(url))
            }
            // 档1:显式 service。
            BuilderInput::Service {
                service,
                path,
            } => {
                classify::validate_service(service).map_err(|reason| {
                    RestDiscoveryError::InvalidUrl {
                        url: format!("service_request({service})"),
                        reason,
                    }
                })?;
                classify::validate_logical_path(path).map_err(|reason| {
                    RestDiscoveryError::InvalidUrl {
                        url: path.clone(),
                        reason,
                    }
                })?;
                self.metrics.explicit_service();
                let scheme = scheme_override.unwrap_or(self.options.default_instance_scheme);
                let (p, existing_q) = split_path_query(path);
                tracing::debug!(
                    service,
                    scheme = scheme.as_str(),
                    "rest-discovery: 档1 显式 service 调用"
                );
                Ok(RequestPlan::Internal {
                    service: service.clone(),
                    scheme,
                    path: p.to_string(),
                    query: merge_query(existing_q, query_parts),
                    // service_request 无原始 URL host → 不保留 Host(由 reqwest 生成 ip:port)。
                    original_host: None,
                })
            }
            // 档2/档3:字符串 URL。
            BuilderInput::Url(raw) => match classify::classify(raw)? {
                Classified::Lb {
                    service,
                    path_and_query,
                } => {
                    self.metrics.lb_scheme();
                    let scheme = scheme_override.unwrap_or(self.options.default_instance_scheme);
                    let (p, existing_q) = split_path_query(&path_and_query);
                    tracing::debug!(
                        service,
                        scheme = scheme.as_str(),
                        "rest-discovery: 档2 lb:// 调用"
                    );
                    Ok(RequestPlan::Internal {
                        // lb://service/... 的原始 host 即 service 名。
                        original_host: Some(service.clone()),
                        service,
                        scheme,
                        path: p.to_string(),
                        query: merge_query(existing_q, query_parts),
                    })
                }
                Classified::Http {
                    original,
                    scheme,
                    host,
                    path_and_query,
                } => {
                    // heuristic 开 + 有索引 → host 命中走 LB,未命中按 UnknownHostPolicy;关 → 外部直连。
                    // 注意:try_new() 即使传入 heuristic_http=Enabled 也不会创建 index,因此仍会落到外部直连;
                    // 只有 connect()/门面 init 路径会先播种索引并启用裸 URL 的服务名判断。
                    if self.options.heuristic_http == HeuristicHttpMode::Enabled {
                        if let Some(index) = &self.index {
                            if let Some(canonical) = index.lookup(&host) {
                                self.metrics.heuristic_hit();
                                // 启发式裸 http(s) 命中:实例协议只按 scheme_policy / 原始 URL scheme,
                                // **不**受请求级 `.scheme()`(scheme_override)影响 —— `.scheme()` 仅是显式内部入口
                                // (service_request / lb://)的补充。要强制裸 URL 命中走 https 请用 SchemePolicy::ForceHttps。
                                // 裸 http(s) 启发式默认保留原始 scheme;`.scheme()` 只影响显式内部入口。
                                let inst_scheme = self.heuristic_instance_scheme(&scheme);
                                let (p, existing_q) = split_path_query(&path_and_query);
                                tracing::debug!(service = %canonical, host = %host, scheme = inst_scheme.as_str(), "rest-discovery: 档3 启发式命中 → LB");
                                return Ok(RequestPlan::Internal {
                                    service: canonical,
                                    scheme: inst_scheme,
                                    path: p.to_string(),
                                    query: merge_query(existing_q, query_parts),
                                    // 启发式命中:保留裸 http(s) URL 的原始 host。
                                    original_host: Some(host),
                                });
                            }
                            return match self.options.unknown_host {
                                UnknownHostPolicy::ExternalHttp => {
                                    self.metrics.external();
                                    tracing::debug!(host = %host, "rest-discovery: 档3 host 未命中索引 → 外部直连");
                                    Ok(RequestPlan::External(append_external_query(
                                        &original,
                                        query_parts,
                                    )?))
                                }
                                UnknownHostPolicy::Error => {
                                    Err(RestDiscoveryError::UnknownServiceHost {
                                        host,
                                    })
                                }
                            };
                        }
                    }
                    self.metrics.external();
                    tracing::debug!(url = %original, "rest-discovery: 档3 外部直连");
                    Ok(RequestPlan::External(append_external_query(
                        &original,
                        query_parts,
                    )?))
                }
            },
        }
    }

    /// 业务作用：按客户端协议策略决定启发式内部请求使用的协议。
    ///
    /// 参数说明：
    /// - `original_scheme`：普通 URL 中的原协议文本。
    ///
    /// 返回：强制策略返回指定协议；保留策略仅将 https 识别为 HTTPS，其余返回 HTTP，不使用显式内部 scheme_override。
    fn heuristic_instance_scheme(&self, original_scheme: &str) -> InstanceScheme {
        match self.options.scheme_policy {
            SchemePolicy::ForceHttp => InstanceScheme::Http,
            SchemePolicy::ForceHttps => InstanceScheme::Https,
            SchemePolicy::Preserve => {
                if original_scheme.eq_ignore_ascii_case("https") {
                    InstanceScheme::Https
                } else {
                    InstanceScheme::Http
                }
            }
        }
    }

    /// 业务作用：在服务隔离、熔断及发现准入边界内选址并执行受限重试。
    ///
    /// 参数说明：
    /// - `method`：决定默认幂等资格的 HTTP 方法。
    /// - `service`：内部发现及保护状态使用的服务名。
    /// - `scheme`：访问实例时使用的 HTTP 或 HTTPS 协议。
    /// - `path`：已解析的逻辑路径。
    /// - `query`：已合并的查询串，不含起始问号。
    /// - `headers`：发送前会过滤和解析的请求头列表。
    /// - `body`：可供额外尝试重放的可选请求正文。
    /// - `timeout`：可选的单次尝试超时覆盖。
    /// - `budget`：由外层监督的可选整次调用预算。
    /// - `trace_context`：用于出站 traceparent 的可选追踪上下文。
    /// - `explicitly_idempotent`：调用方对非 GET/HEAD 请求重放安全性的显式声明。
    /// - `original_host`：启用保留 Host 时使用的原始主机名。
    ///
    /// 返回：返回收到的响应，包括未重试的非成功状态；无实例、保护拒绝、发现准入、预算或传输失败返回错误。额外尝试同时受次数、未尝试端点和令牌限制，受管来源在重试时重读；仅在首次轮询传输时复验准入并登记已提交事实。
    #[allow(clippy::too_many_arguments)]
    async fn send_internal(
        &self,
        method: Method,
        service: &str,
        scheme: InstanceScheme,
        path: &str,
        query: Option<&str>,
        headers: &[(String, String)],
        body: &Option<RestBody>,
        timeout: Option<Duration>,
        budget: Option<&RequestBudget>,
        trace_context: Option<&TraceContext>,
        explicitly_idempotent: bool,
        original_host: Option<&str>,
    ) -> Result<reqwest::Response> {
        let source = self.source.as_ref().ok_or_else(|| {
            RestDiscoveryError::DiscoveryDisabledForInternalCall {
                service: service.to_string(),
            }
        })?;
        let _bulkhead = self
            .resilience
            .acquire_bulkhead(service, &self.options.resilience)
            .map_err(|error| {
                if matches!(error, RestDiscoveryError::BulkheadRejected { .. }) {
                    self.metrics.bulkhead_rejected();
                }
                error
            })?;
        let mut circuit = self
            .resilience
            .begin_circuit(service, &self.options.resilience)
            .map_err(|error| {
                if matches!(error, RestDiscoveryError::CircuitOpen { .. }) {
                    self.metrics.circuit_open_rejected();
                }
                error
            })?;
        // 旧 provider 保留请求内快照语义；受管源在每次 attempt 读取共享事实且不另起后端轮询。
        let snapshot = source.instances(service).await?;

        // 仅 GET/HEAD + 开启重试时多尝试;其余 method 恒 1 次。
        // 带 body 或业务副作用的方法默认不自动重放,把幂等性判断留给调用方。
        let retryable = method == Method::GET || method == Method::HEAD || explicitly_idempotent;
        let retry_enabled = self.options.retry.retry_get_head_on_transport_error
            || self.options.retry.retry_get_head_on_retryable_status;
        let max_attempts = if retry_enabled && retryable {
            self.options.retry.max_attempts.max(1)
        } else {
            1
        };

        let mut attempted = std::collections::HashSet::new();
        let mut last_err: Option<reqwest::Error> = None;
        for attempt in 0..max_attempts {
            // 退避后重读当前证据，已撤销的旧路由不能进入下一次重试。
            let current = if attempt != 0 && source.managed() {
                source.instances(service).await?
            } else {
                snapshot.clone()
            };
            let now = Instant::now();
            let avail: Vec<&Instance> = current
                .iter()
                .filter(|instance| is_traffic_instance(instance))
                .filter(|instance| {
                    let ejected = self.resilience.is_ejected(service, instance, now);
                    if ejected {
                        self.metrics.outlier_ejected_skipped();
                    }
                    !ejected
                })
                .collect();
            if avail.is_empty() {
                self.metrics.no_instance();
                return Err(RestDiscoveryError::NoAvailableInstance {
                    service: service.into(),
                });
            }
            let mut tried: Vec<_> = avail
                .iter()
                .map(|instance| attempted.contains(&(instance.ip.clone(), instance.port)))
                .collect();
            // 每轮优先尊重负载均衡器选择,但如果它选到已失败实例,就线性找下一个未试实例。
            // 这样既保留正常分布,又避免一次请求内反复打同一个坏实例。
            let idx = match self.pick_untried(service, &avail, &tried) {
                Some(i) => i,
                None => break, // 实例都试过了
            };
            tried[idx] = true;
            let inst = avail[idx];
            attempted.insert((inst.ip.clone(), inst.port));
            let url = build_instance_url(scheme, &inst.ip, inst.port, path, query)?;
            tracing::debug!(service, ip = %inst.ip, port = inst.port, attempt, "rest-discovery: 选中实例");
            let attempt_timeout =
                effective_timeout(timeout.unwrap_or(self.options.http.timeout), budget)?;
            let req = self.build_req(
                method.clone(),
                url,
                RequestBuildContext {
                    headers,
                    body,
                    timeout: attempt_timeout,
                    trace_context,
                    original_host,
                },
            );
            // 构造 Future 尚未提交传输；首次轮询仍须在当前实例准入保护范围内完成。
            let mut sending = std::pin::pin!(req.send());
            let mut submitted = false;
            let result = std::future::poll_fn(|cx| {
                use std::future::Future;
                if submitted {
                    return sending.as_mut().poll(cx).map(Ok);
                }
                let mut result = std::task::Poll::Pending;
                let admission = source.submit(service, inst, &mut || {
                    // 只有通过发现门禁并开始 transport 的请求才提供业务熔断证据。
                    circuit.mark_submitted();
                    submitted = true;
                    result = sending.as_mut().poll(cx);
                });
                match admission {
                    Ok(()) => result.map(Ok),
                    Err(error) => std::task::Poll::Ready(Err(error)),
                }
            })
            .await?;
            match result {
                Ok(resp) => {
                    let can_retry_status = self.options.retry.retry_get_head_on_retryable_status
                        && is_retryable_status(resp.status())
                        && attempt + 1 < max_attempts
                        && tried.iter().any(|tried| !tried);
                    if can_retry_status {
                        self.resilience.record_instance(
                            service,
                            inst,
                            false,
                            &self.options.resilience,
                        );
                        let delay = retry_after_delay(&resp).unwrap_or_default();
                        // Retry-After 是不可信远端输入。即使调用方没绑定绝对预算，也不能让一个响应
                        // 把客户端挂起任意久；超过本次配置的 HTTP 上限就把原响应交还调用方裁决。
                        let retry_wait_limit = budget
                            .map(RequestBudget::remaining)
                            .unwrap_or_else(|| timeout.unwrap_or(self.options.http.timeout));
                        if delay >= retry_wait_limit || !self.consume_retry_budget(service) {
                            circuit.complete(false);
                            return Ok(resp);
                        }
                        self.metrics.retry_attempt();
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        continue;
                    }
                    let success = !is_circuit_failure_status(resp.status());
                    self.resilience.record_instance(
                        service,
                        inst,
                        success,
                        &self.options.resilience,
                    );
                    circuit.complete(success);
                    return Ok(resp);
                }
                Err(e) => {
                    let transport = is_retryable_transport(&e);
                    // 传输失败只影响业务实例统计；受管证据仍由公共 owner 独立复验。
                    if transport {
                        source.mark_transport_error(service);
                    }
                    self.resilience
                        .record_instance(service, inst, false, &self.options.resilience);
                    if attempt + 1 < max_attempts
                        && transport
                        && self.options.retry.retry_get_head_on_transport_error
                        && tried.iter().any(|tried| !tried)
                        && self.consume_retry_budget(service)
                    {
                        self.metrics.retry_attempt();
                        tracing::warn!(service, attempt, error = %e, "rest-discovery: 传输错误,重试下一个实例");
                        last_err = Some(e);
                        continue;
                    }
                    circuit.complete(false);
                    return Err(RestDiscoveryError::Http(e));
                }
            }
        }
        circuit.complete(false);
        Err(last_err.map(RestDiscoveryError::Http).unwrap_or_else(|| {
            RestDiscoveryError::NoAvailableInstance {
                service: service.to_string(),
            }
        }))
    }

    /// 业务作用：在后台准入开放时为服务扣取一次额外尝试令牌。
    ///
    /// 参数说明：
    /// - `service`：按逻辑服务隔离的令牌桶键。
    ///
    /// 返回：成功扣取返回 true；目录满、已关闭或令牌不足返回 false。首次桶以配置容量填满，后续按单调经过时间补充。
    fn consume_retry_budget(&self, service: &str) -> bool {
        self.background
            .with_open(|| {
                if !self.retry_budgets.contains_key(service)
                    && self.retry_budgets.len() >= self.options.watch.max_services
                {
                    return Ok(false);
                }
                let now = Instant::now();
                let capacity = self.options.retry.budget_capacity;
                let refill = self.options.retry.budget_refill_per_second;
                let entry = self
                    .retry_budgets
                    .entry(service.to_owned())
                    .or_insert_with(|| std::sync::Mutex::new(RetryBucket::new(capacity, now)));
                let allowed = entry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .try_take(capacity, refill, now);
                Ok(allowed)
            })
            .unwrap_or(false)
    }

    /// 业务作用：优先采用选址器建议，并退回首个尚未尝试的候选位置。
    ///
    /// 参数说明：
    /// - `service`：供选址器隔离状态的服务名。
    /// - `avail`：本轮可用实例引用列表。
    /// - `tried`：与候选列表位置一一对应的已尝试标记。
    ///
    /// 返回：有效且未尝试的建议被接受，否则返回首个 false 位置；全部已尝试返回 None。列表与标记对齐由调用方保证。
    fn pick_untried(&self, service: &str, avail: &[&Instance], tried: &[bool]) -> Option<usize> {
        match self.lb.pick(service, avail) {
            Some(i) if !tried.get(i).copied().unwrap_or(true) => Some(i),
            _ => tried.iter().position(|t| !t),
        }
    }

    /// 业务作用：将目标、可重放正文和受控头部组装为底层请求。
    ///
    /// 参数说明：
    /// - `method`：出站 HTTP 方法。
    /// - `url`：已经解析并选定的具体目标 URL。
    /// - `context`：头部、正文、尝试超时、追踪及原始 Host 的共享构造信息。
    ///
    /// 返回：返回尚未发送的 reqwest 构造器；禁止或无效头部静默跳过，可选可信 Host 及 traceparent 覆盖对应头部。未提供尝试超时时沿用连接池默认超时。
    fn build_req(
        &self,
        method: Method,
        url: reqwest::Url,
        context: RequestBuildContext<'_>,
    ) -> reqwest::RequestBuilder {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue, HOST};

        let mut map = HeaderMap::new();
        for (k, v) in context.headers {
            if is_forbidden_header(k) {
                continue;
            }
            // 非法 header 名/值跳过(宽松:不因一个坏头炸整个请求)。insert = last-wins。
            if let (Ok(name), Ok(val)) = (
                HeaderName::from_bytes(k.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                map.insert(name, val);
            }
        }
        // 仅 RestDiscovery 内部可设 Host(业务传入的 Host 已被 is_forbidden_header 过滤)。
        if self.options.preserve_original_host_header {
            if let Some(host) = context.original_host {
                if let Ok(val) = HeaderValue::from_str(host) {
                    map.insert(HOST, val);
                }
            }
        }
        // 自动传播当前 W3C context；框架值覆盖业务手写 traceparent，避免伪造/重复头破坏链路。
        if let Some(trace_context) = context.trace_context {
            if let Ok(value) = HeaderValue::from_str(&trace_context.to_traceparent()) {
                map.insert(HeaderName::from_static("traceparent"), value);
            }
        }

        let mut req = self.http.request(method, url).headers(map);
        match context.body {
            // reqwest 的 .json() 会序列化并设 Content-Type: application/json。
            Some(RestBody::Json(v)) => req = req.json(v),
            // form 已 urlencode;手动设 body + Content-Type(避免再走 reqwest .form 二次编码)。
            Some(RestBody::Form(s)) => {
                req = req
                    .header(
                        reqwest::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .body(s.clone());
            }
            // raw:原样字节;不自动设 Content-Type。header map 已在上面合并完成,
            // 因此 consumes/显式 header 的优先级不会被 body 分支再次改写。
            Some(RestBody::Raw(b)) => req = req.body(b.clone()),
            None => {}
        }
        if let Some(t) = context.timeout {
            req = req.timeout(t);
        }
        req
    }
}

/// 业务作用：确定收到的 HTTP 状态是否计入服务故障保护。
///
/// 参数说明：
/// - `status`：本次传输取得的 HTTP 状态。
///
/// 返回：任意服务端 5xx 或 429 返回 true，其它返回 false，不等同于是否允许重试。
fn is_circuit_failure_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

/// 业务作用：建立使用显式期限且禁用隐式重定向和环境代理的连接池。
///
/// 参数说明：
/// - `http`：调用方已校验的请求及建连超时配置。
///
/// 返回：返回可复用 reqwest 客户端，构造失败 panic；不会立即建立业务连接。
fn build_http_client(http: &RestHttpOptions) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(http.timeout)
        .connect_timeout(http.connect_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .expect("rest-discovery: 构造 reqwest::Client 失败")
}

/// 业务作用：周期全量读取服务名索引，并回收已确认移除服务的观察目录。
///
/// 参数说明：
/// - `discovery`：提供完整服务名枚举的后端。
/// - `index`：仅成功枚举且容量有效时更新的共享索引。
/// - `interval`：刷新间隔，内部至少采用一毫秒。
/// - `source`：实例目录的弱引用，避免循环本身延长其存活。
///
/// 返回：来源释放时退出，否则持续刷新直到外部终止；枚举或容量失败保留旧索引。成功发布后标记待回收观察项，再修剪上一轮墓碑，不把失败枚举当作空列表。
async fn index_refresh_loop(
    discovery: Arc<dyn DiscoveryClient>,
    index: Arc<ServiceNameIndex>,
    interval: Duration,
    source: Weak<InstanceSource>,
) {
    // 内部不变量兜底 clamp:interval=0 会让 tokio::time::interval panic(且在 spawned task 内,表现为后台任务静默死掉)。
    // 公开构造入口已 fail-fast;此处只兜底防御未经过校验的内部路径。
    let interval = interval.max(Duration::from_millis(1));
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // 吃掉立即触发的首拍(初值已在 connect 同步播种)
    loop {
        ticker.tick().await;
        // InstanceSource 已随 client drop → 不必再拉 list_services,直接退出(client 的 Drop 也会 abort 本任务,双保险)。
        let Some(source) = source.upgrade() else {
            tracing::debug!("rest-discovery: InstanceSource 已释放,停止服务名索引刷新");
            break;
        };
        match discovery.list_services().await {
            Ok(names) => {
                let outcome = match index.refresh(names, Instant::now()) {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        tracing::warn!("rest-discovery: service index capacity rejected; retaining current index");
                        continue;
                    }
                };
                let just_marked: std::collections::HashSet<&str> =
                    outcome.removed.iter().map(String::as_str).collect();
                for name in &outcome.removed {
                    source.mark_removed(name);
                }
                // 二阶段 prune:清理上一轮已标记 removed 的墓碑,本轮刚 mark 的留一轮 grace。
                // 这样既能保留短暂抖动的服务状态,也能避免大量 churn 下墓碑 key 无界堆积。
                source.prune_removed_except(&just_marked);
            }
            Err(e) => {
                tracing::warn!(error = %e, "rest-discovery: list_services 周期刷新失败,保留旧索引")
            }
        }
    }
}

/// builder 的输入：显式内部服务、遵循启发式策略的通用 URL，或保持原目标的外部 HTTP(S) 地址。
#[derive(Debug, Clone)]
enum BuilderInput {
    Service {
        service: String,
        path: String,
    },
    Url(String),
    ExternalUrl(String),
}

/// `resolve_plan` 的结果:内部服务(待选实例)或已定的外部 URL。
enum RequestPlan {
    Internal {
        service: String,
        scheme: InstanceScheme,
        path: String,
        query: Option<String>,
        /// 原始 URL host(`lb://`=service 名、启发式=URL host;`service_request`=None)。
        /// 仅 `preserve_original_host_header` 开启时用于回写 `Host` header。
        original_host: Option<String>,
    },
    External(reqwest::Url),
}

/// 业务作用：按内置策略建立共享负载均衡器。
///
/// 参数说明：
/// - `strategy`：选择普通轮询或平滑加权轮询。
///
/// 返回：返回独立算法实例的 Arc，尚未登记服务状态。
fn build_lb(strategy: LbStrategy) -> Arc<dyn LoadBalancer> {
    match strategy {
        LbStrategy::RoundRobin => Arc::new(RoundRobinLoadBalancer::new()),
        LbStrategy::Weighted => Arc::new(WeightedRoundRobinLoadBalancer::new()),
    }
}

/// 业务作用：判断传输错误是否属于受支持的跨实例重试类别。
///
/// 参数说明：
/// - `e`：底层 reqwest 请求错误。
///
/// 返回：建连或超时错误返回 true，其它 false；并不单独授权重放请求。
fn is_retryable_transport(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout()
}

/// 业务作用：按固定集合识别可重试 HTTP 状态。
///
/// 参数说明：
/// - `status`：已收到的响应状态。
///
/// 返回：429、502、503 或 504 返回 true，其它 false；实际重试还须经过方法、次数和预算门禁。
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::TOO_MANY_REQUESTS
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

/// 业务作用：从响应中读取 Retry-After 的秒数或 HTTP 日期。
///
/// 参数说明：
/// - `response`：可能携带重试提示的响应。
///
/// 返回：可解析时返回等待时长，过去日期为零；缺失或非法返回 None，不在此限制最大延迟。
fn retry_after_delay(response: &reqwest::Response) -> Option<Duration> {
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let when = httpdate::parse_http_date(value).ok()?;
    Some(
        when.duration_since(std::time::SystemTime::now())
            .unwrap_or_default(),
    )
}

/// 业务作用：在单次尝试期限与外层调用预算之间选择请求超时覆盖。
///
/// 参数说明：
/// - `per_attempt`：本次尝试预期使用的超时。
/// - `budget`：已由外层监督执行的可选整体预算。
///
/// 返回：无预算或尝试期限更短时返回 Some；预算剩余不更长时返回 None，依赖外层预算和连接池默认超时。预算已尽返回 BudgetExhausted。
fn effective_timeout(
    per_attempt: Duration,
    budget: Option<&RequestBudget>,
) -> Result<Option<Duration>> {
    match budget {
        Some(budget) => {
            let remaining = budget.remaining();
            if remaining.is_zero() {
                Err(RestDiscoveryError::BudgetExhausted)
            } else if per_attempt < remaining {
                Ok(Some(per_attempt))
            } else {
                // 调用链 deadline 更早：由 send() 外层 timeout_at 统一裁决为 BudgetExhausted，
                // 避免 reqwest 自身 timeout 抢先把同一事件误分类成普通 Http error。
                Ok(None)
            }
        }
        None => Ok(Some(per_attempt)),
    }
}

/// 一个服务的 retry token bucket。
struct RetryBucket {
    tokens: f64,
    last_refill: Instant,
}

impl RetryBucket {
    /// 业务作用：为服务重试建立初始满额令牌桶。
    ///
    /// 参数说明：
    /// - `capacity`：初始令牌数及期望桶容量。
    /// - `now`：本次桶建立的单调时刻。
    ///
    /// 返回：返回令牌数等于容量、补充起点为 now 的桶，不校验容量。
    fn new(capacity: u32, now: Instant) -> Self {
        Self {
            tokens: f64::from(capacity),
            last_refill: now,
        }
    }

    /// 业务作用：按单调经过时间补充令牌后尝试消费一个。
    ///
    /// 参数说明：
    /// - `capacity`：本次允许保留的最大令牌数。
    /// - `refill_per_second`：调用方已保证有限且为正的每秒补充率。
    /// - `now`：当前单调时刻，早于上次时刻时经过量按零计算。
    ///
    /// 返回：补充后至少一枚则扣取并返回 true，否则 false；每次都更新补充起点，不验证配置范围。
    fn try_take(&mut self, capacity: u32, refill_per_second: f64, now: Instant) -> bool {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.tokens = (self.tokens + elapsed * refill_per_second).min(f64::from(capacity));
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// 请求体(互斥):JSON(`application/json`) / form(`application/x-www-form-urlencoded`) / raw bytes。
#[derive(Clone)]
enum RestBody {
    Json(serde_json::Value),
    Form(String),
    /// 原样字节 body(`#[RequestBody(raw)]`)。不做 JSON/urlencoded 编码;Content-Type 仅由 headers/consumes 决定。
    Raw(bytes::Bytes),
}

/// 单次 attempt 构造请求所需的借用 header/body/timeout/trace/host 上下文。
struct RequestBuildContext<'a> {
    headers: &'a [(String, String)],
    body: &'a Option<RestBody>,
    timeout: Option<Duration>,
    trace_context: Option<&'a TraceContext>,
    original_host: Option<&'a str>,
}

/// 请求 builder。`send().await` 时才解析 URL、选实例、发请求。
pub struct RestRequestBuilder {
    client: Arc<RestDiscoveryClient>,
    method: Method,
    input: BuilderInput,
    /// 覆盖 `service_request`/`lb://` 的实例协议。
    scheme_override: Option<InstanceScheme>,
    headers: Vec<(String, String)>,
    /// 累积的 query 片段(各自已 urlencoded),send 时并入最终 URL。
    query_parts: Vec<String>,
    /// 请求体(json / form / raw 互斥,后设的覆盖先设的)。
    body: Option<RestBody>,
    timeout: Option<Duration>,
    /// 跨整个发现、重试、等待和单次发送的绝对预算。
    budget: Option<RequestBudget>,
    /// 自动注入下游的 W3C trace context。
    trace_context: Option<TraceContext>,
    /// POST/PATCH 等只有业务显式声明幂等后才进入自动重试。
    explicitly_idempotent: bool,
    /// 构造阶段(query/body 序列化)失败延迟到 send 才报。
    build_error: Option<RestDiscoveryError>,
}

impl RestRequestBuilder {
    /// 业务作用：保存延迟请求的客户端、方法和目标输入。
    ///
    /// 参数说明：
    /// - `client`：需一直持有到发送结束的共享客户端。
    /// - `method`：待发送 HTTP 方法。
    /// - `input`：发送时解析的目标类别及文本。
    ///
    /// 返回：返回尚无头部、正文、查询、预算、追踪或构造错误的请求构造器，不执行网络。
    fn new(client: Arc<RestDiscoveryClient>, method: Method, input: BuilderInput) -> Self {
        Self {
            client,
            method,
            input,
            scheme_override: None,
            headers: Vec::new(),
            query_parts: Vec::new(),
            body: None,
            timeout: None,
            budget: None,
            trace_context: None,
            explicitly_idempotent: false,
            build_error: None,
        }
    }

    /// 业务作用：保存显式内部请求的实例协议覆盖。
    ///
    /// 参数说明：
    /// - `scheme`：显式服务和 lb 调用使用的目标协议。
    ///
    /// 返回：返回更新后的构造器；普通 HTTP URL 的协议由原 URL 及客户端启发式策略决定，不在此发送请求。
    pub fn scheme(mut self, scheme: InstanceScheme) -> Self {
        self.scheme_override = Some(scheme);
        self
    }

    /// 业务作用：追加待发送时处理的请求头文本。
    ///
    /// 参数说明：
    /// - `key`：请求头名称，发送前会过滤禁止项并校验语法。
    /// - `value`：请求头值，发送前会检查可表示性。
    ///
    /// 返回：返回追加后的构造器，此时不报头部格式错误；发送时无效或禁止头部可被跳过，同名有效头最终由后值覆盖。
    pub fn header(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.headers
            .push((key.as_ref().to_string(), value.as_ref().to_string()));
        self
    }

    /// 业务作用：从已有头表追加可转换为文本的请求头。
    ///
    /// 参数说明：
    /// - `map`：提供候选头部名称和值的 HeaderMap。
    ///
    /// 返回：返回更新后的构造器，非文本值跳过；发送时仍执行禁止项过滤和头部解析。
    pub fn headers_from_map(mut self, map: &reqwest::header::HeaderMap) -> Self {
        for (name, value) in map.iter() {
            if let Ok(v) = value.to_str() {
                self.headers
                    .push((name.as_str().to_string(), v.to_string()));
            }
        }
        self
    }

    /// 业务作用：将可序列化值编码为待追加的查询片段。
    ///
    /// 参数说明：
    /// - `q`：按 URL 表单规则序列化的查询值。
    ///
    /// 返回：成功时追加非空片段；失败保存首个构造错误供发送时返回，后续成功不会清除该错误。不执行请求。
    pub fn query<T: Serialize + ?Sized>(mut self, q: &T) -> Self {
        match serde_urlencoded::to_string(q) {
            Ok(s) if !s.is_empty() => self.query_parts.push(s),
            Ok(_) => {}
            Err(e) => {
                self.build_error
                    .get_or_insert(RestDiscoveryError::RequestBuildFailed {
                        reason: format!("query 序列化失败:{e}"),
                    });
            }
        }
        self
    }

    /// 业务作用：将一个可展示值编码为单个命名查询参数。
    ///
    /// 参数说明：
    /// - `name`：待编码的查询参数名。
    /// - `value`：先转为显示文本再编码的参数值。
    ///
    /// 返回：返回追加片段后的构造器；编码失败保存首个构造错误，不覆盖已有同名查询项。
    pub fn query_pair(mut self, name: &str, value: impl std::fmt::Display) -> Self {
        match serde_urlencoded::to_string([(name, value.to_string())]) {
            Ok(s) if !s.is_empty() => self.query_parts.push(s),
            Ok(_) => {}
            Err(e) => {
                self.build_error
                    .get_or_insert(RestDiscoveryError::RequestBuildFailed {
                        reason: format!("query 参数 {name} 序列化失败:{e}"),
                    });
            }
        }
        self
    }

    /// 业务作用：按输入顺序追加多个同名查询参数。
    ///
    /// 参数说明：
    /// - `name`：每个参数共同使用的名称。
    /// - `values`：逐项转为文本再编码的有限值序列。
    ///
    /// 返回：返回追加后的构造器，空序列不新增片段；编码错误沿用首错保存规则，不合并同名值。
    pub fn query_pairs<I, V>(mut self, name: &str, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: std::fmt::Display,
    {
        for v in values {
            self = self.query_pair(name, v);
        }
        self
    }

    /// 业务作用：把业务值预编码为可重放 JSON 正文。
    ///
    /// 参数说明：
    /// - `body`：交由 serde_json 转为 JSON 值的业务内容。
    ///
    /// 返回：成功时替换已有正文，失败保留首个构造错误供发送时拒绝；返回构造器，不发送请求，自定义 Serialize 展开仍传播。
    pub fn json<T: Serialize + ?Sized>(mut self, body: &T) -> Self {
        match serde_json::to_value(body) {
            Ok(v) => self.body = Some(RestBody::Json(v)),
            Err(e) => {
                self.build_error
                    .get_or_insert(RestDiscoveryError::RequestBuildFailed {
                        reason: format!("json body 序列化失败:{e}"),
                    });
            }
        }
        self
    }

    /// 业务作用：把业务值预编码为可重放 URL 表单正文。
    ///
    /// 参数说明：
    /// - `body`：按表单规则序列化的业务内容。
    ///
    /// 返回：成功时替换正文，失败保存首个构造错误；返回构造器，不执行网络。
    pub fn form<T: Serialize + ?Sized>(mut self, body: &T) -> Self {
        match serde_urlencoded::to_string(body) {
            Ok(s) => self.body = Some(RestBody::Form(s)),
            Err(e) => {
                self.build_error
                    .get_or_insert(RestDiscoveryError::RequestBuildFailed {
                        reason: format!("form body 序列化失败:{e}"),
                    });
            }
        }
        self
    }

    /// 业务作用：复制原始字节作为可重放请求正文。
    ///
    /// 参数说明：
    /// - `body`：由调用方限制大小的原始内容。
    ///
    /// 返回：返回正文已替换的构造器，复制为拥有式字节，不自动设置内容类型。
    pub fn raw_body<B: AsRef<[u8]>>(mut self, body: B) -> Self {
        // builder 必须拥有请求体,不能保存调用方引用;这里拷一次换取 &str/String/Vec/Bytes
        // 四类入参统一可用,也避免 async 发送阶段出现借用生命周期约束。
        self.body = Some(RestBody::Raw(bytes::Bytes::copy_from_slice(body.as_ref())));
        self
    }

    /// 业务作用：保存本请求的单次尝试超时覆盖。
    ///
    /// 参数说明：
    /// - `d`：原样保存的超时值，本方法不拒绝零。
    ///
    /// 返回：返回更新后的构造器；实际发送还受调用预算约束，不直接延长已有预算。
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// 业务作用：保存调用方预算供发送时监督整次操作。
    ///
    /// 参数说明：
    /// - `budget`：需要克隆保存的请求预算。
    ///
    /// 返回：返回保存预算克隆的构造器，不启动计时任务或发送请求。
    pub fn budget(mut self, budget: &RequestBudget) -> Self {
        self.budget = Some(budget.clone());
        self
    }

    /// 业务作用：保存出站请求将使用的追踪父上下文。
    ///
    /// 参数说明：
    /// - `trace_context`：发送时生成子上下文及 traceparent 的来源。
    ///
    /// 返回：返回更新后的构造器，不建立 span 或执行 HTTP。
    pub fn trace_context(mut self, trace_context: &TraceContext) -> Self {
        self.trace_context = Some(*trace_context);
        self
    }

    /// 业务作用：同时保存整次操作预算与追踪上下文。
    ///
    /// 参数说明：
    /// - `budget`：发送时监督操作的预算。
    /// - `trace_context`：出站追踪父上下文。
    ///
    /// 返回：返回两项上下文均已设置的构造器，不发起请求。
    pub fn request_context(self, budget: &RequestBudget, trace_context: &TraceContext) -> Self {
        self.budget(budget).trace_context(trace_context)
    }

    /// 业务作用：记录调用方允许对当前请求进行受策略约束重放的声明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回显式幂等标志为 true 的构造器；本方法不证明业务实际幂等，也不会单独开启重试或越过次数和令牌预算。
    pub fn idempotent(mut self) -> Self {
        self.explicitly_idempotent = true;
        self
    }

    /// 业务作用：发送并返回原始 `reqwest::Response`(不做 `error_for_status`)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：绝对预算内完成解析、选址与全部尝试时返回原始响应；取消、超时、选址或传输失败返回脱敏错误。
    pub async fn send(mut self) -> Result<reqwest::Response> {
        let background = self.client.background.clone();
        let _call = background.enter()?;
        if self
            .client
            .source
            .as_ref()
            .is_some_and(|source| source.managed())
        {
            let maximum = self.timeout.unwrap_or(self.client.options.http.timeout);
            self.budget = Some(
                self.budget
                    .as_ref()
                    .map(|budget| budget.child(maximum))
                    .unwrap_or_else(|| RequestBudget::from_now(maximum)),
            );
        }
        if let Some(budget) = &self.budget {
            budget.check()?;
        }
        // 业务未显式绑定时回退环境上下文(HTTP 入站中间件、消费/任务入口建立的 task-local 作用域),
        // 使漏穿线的调用点不再静默断链;显式绑定始终优先,既有行为不变。
        if self.trace_context.is_none() {
            self.trace_context = natelemetry::ambient();
        }
        // 出站传播必须使用新的 client span-id，不能把入站 server span-id 原样复用。即便 telemetry
        // 未启用也派生子上下文，保证下游看到的 parent 是本次调用；启用 recorder 时同一 context
        // 同时用于 wire header 和导出记录。
        let mut span = self.trace_context.as_ref().and_then(|parent| {
            self.client.options.span_recorder.as_ref().map(|recorder| {
                recorder.start(
                    format!("HTTP {}", self.method.as_str()),
                    parent,
                    SpanKind::Client,
                )
            })
        });
        if let Some(parent) = self.trace_context {
            self.trace_context = Some(
                span.as_ref()
                    .map(natelemetry::SpanGuard::context)
                    .unwrap_or_else(|| parent.child(random_span_id())),
            );
        }
        let budget = self.budget.clone();
        let operation = self.send_with_context();
        let result = background
            .run(async move {
                match budget {
                    Some(budget) => budget
                        .run(operation)
                        .await
                        .map_err(RestDiscoveryError::from)?,
                    None => operation.await,
                }
            })
            .await;
        if let Some(span) = span.take() {
            let status = result
                .as_ref()
                .ok()
                .map(|response| response.status().as_u16());
            let _ = span.finish(status);
        }
        result
    }

    /// 业务作用：将响应头及后续流式正文纳入同一次受管请求期限与停机排空。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有界流式响应；正文未被主动读取时仍响应截止时间和运行时关闭。
    pub async fn send_managed(mut self) -> Result<crate::ManagedResponse> {
        let maximum = self.timeout.unwrap_or(self.client.options.http.timeout);
        let budget = self
            .budget
            .as_ref()
            .map(|parent| parent.child(maximum))
            .unwrap_or_else(|| RequestBudget::from_now(maximum));
        self.budget = Some(budget.clone());
        let background = self.client.background.clone();
        let permit = background.reserve_response()?;
        let response = self.send().await?;
        crate::ManagedResponse::new(response, background, budget, permit)
    }

    /// 业务作用：先拒绝已有构造错误，再按目标计划执行一次外部或内部发送。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回收到的响应，HTTP 非成功状态可原样返回；构造、解析、发现或传输失败传播。外部直连不使用内部实例重试流程，完整正文消费由上层决定。
    async fn send_with_context(self) -> Result<reqwest::Response> {
        let RestRequestBuilder {
            client,
            method,
            input,
            scheme_override,
            headers,
            query_parts,
            body,
            timeout,
            budget,
            trace_context,
            explicitly_idempotent,
            build_error,
        } = self;

        if let Some(e) = build_error {
            return Err(e);
        }

        match client.resolve_plan(&input, scheme_override, &query_parts)? {
            // 外部直连:单发,不跨实例重试;Host 由 reqwest 按 URL 自动生成,不回写。
            RequestPlan::External(url) => {
                let attempt_timeout = effective_timeout(
                    timeout.unwrap_or(client.options.http.timeout),
                    budget.as_ref(),
                )?;
                let req = client.build_req(
                    method,
                    url,
                    RequestBuildContext {
                        headers: &headers,
                        body: &body,
                        timeout: attempt_timeout,
                        trace_context: trace_context.as_ref(),
                        original_host: None,
                    },
                );
                Ok(req.send().await?)
            }
            // 内部调用:选址 + 可选 GET/HEAD 重试。
            RequestPlan::Internal {
                service,
                scheme,
                path,
                query,
                original_host,
            } => {
                client
                    .send_internal(
                        method,
                        &service,
                        scheme,
                        &path,
                        query.as_deref(),
                        &headers,
                        &body,
                        timeout,
                        budget.as_ref(),
                        trace_context.as_ref(),
                        explicitly_idempotent,
                        original_host.as_deref(),
                    )
                    .await
            }
        }
    }

    /// 业务作用：发送并把 2xx body 反序列化为 `T`;非 2xx 只返回状态码，不读取或公开远端正文。
    ///
    /// 错误分类:body 读取(传输层)失败 → `Http`;读到了但 JSON 反序列化失败(2xx 响应体不符合约定)
    /// → `ResponseDecodeFailed`,与 `send_json_unwrap` 一致(传输失败 vs 响应契约失败彻底分开)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：2xx 且 JSON 契约匹配时返回 `T`；非 2xx、传输失败或反序列化失败返回对应稳定错误。
    pub async fn send_json<T: DeserializeOwned>(self) -> Result<T> {
        let bytes = self.send_bytes().await?;
        serde_json::from_slice(&bytes).map_err(|e| RestDiscoveryError::ResponseDecodeFailed {
            reason: format!("响应体 JSON 反序列化失败:{e}"),
        })
    }

    /// 业务作用：发送并把 2xx JSON 响应的【顶层字段 `field`】解包后反序列化为 `T`(供宏 `unwrap = "data"` 生成)。
    /// 非 2xx → `HttpStatus`;body 非 JSON / 缺字段 / 字段类型不匹配 → `ResponseDecodeFailed`(契约错误,区别于传输层 `Http`)。
    ///
    /// 参数说明：
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    ///
    /// 返回：2xx 响应含指定顶层字段且其类型匹配时返回 `T`；状态、JSON 或字段合同不满足时返回脱敏错误。
    pub async fn send_json_unwrap<T: DeserializeOwned>(self, field: &str) -> Result<T> {
        let bytes = self.send_bytes().await?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            RestDiscoveryError::ResponseDecodeFailed {
                reason: format!("响应不是合法 JSON:{e}"),
            }
        })?;
        // `take` 把字段值移出 Value,避免为了解包顶层字段再 clone 一份响应体子树。
        let taken = value
            .get_mut(field)
            .map(serde_json::Value::take)
            .ok_or_else(|| RestDiscoveryError::ResponseDecodeFailed {
                reason: format!("响应缺少 unwrap 字段 `{field}`"),
            })?;
        serde_json::from_value(taken).map_err(|e| RestDiscoveryError::ResponseDecodeFailed {
            reason: format!("unwrap 字段 `{field}` 反序列化失败:{e}"),
        })
    }

    /// 业务作用：发送并返回 2xx 文本;非 2xx 返回 `HttpStatus`。
    ///
    /// 参数说明：无。
    ///
    /// 返回：2xx 时返回完整文本；非 2xx 不读取敏感正文并返回状态错误。
    pub async fn send_text(self) -> Result<String> {
        self.consume_response(|response| async move { Ok(response.text().await?) })
            .await
    }

    /// 业务作用：发送并只校验 2xx、丢弃 body(供宏 `-> anyhow::Result<()>` 生成)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：2xx 时丢弃正文并成功；非 2xx 返回不包含正文的状态错误。
    pub async fn send_ok(self) -> Result<()> {
        let resp = self.send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(RestDiscoveryError::HttpStatus {
                status,
            });
        }
        Ok(())
    }

    /// 业务作用：发送并返回 2xx 响应体字节(供宏 `response = "bytes"` 生成)。
    ///
    /// 参数说明：无。
    ///
    /// 返回：2xx 时返回响应字节；非 2xx 不读取正文并返回状态错误。
    pub async fn send_bytes(self) -> Result<bytes::Bytes> {
        self.consume_response(|response| async move { Ok(response.bytes().await?) })
            .await
    }

    /// 业务作用：让响应头与完整响应体消费使用同一个预算和运行时 owner。
    ///
    /// 参数说明：
    /// - `consume`：为成功响应体的消费方法。
    ///
    /// 返回：完整消费成功时返回业务值；关闭、取消、到期或传输失败时保留错误分类。
    async fn consume_response<T, F, Fut>(mut self, consume: F) -> Result<T>
    where
        F: FnOnce(reqwest::Response) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let maximum = self.timeout.unwrap_or(self.client.options.http.timeout);
        let budget = self
            .budget
            .as_ref()
            .map(|parent| parent.child(maximum))
            .unwrap_or_else(|| RequestBudget::from_now(maximum));
        self.budget = Some(budget.clone());
        let background = self.client.background.clone();
        let _call = background.enter()?;
        background
            .run(async move {
                budget
                    .run(async move {
                        let response = self.send().await?;
                        let status = response.status();
                        if !status.is_success() {
                            return Err(RestDiscoveryError::HttpStatus {
                                status,
                            });
                        }
                        consume(response).await
                    })
                    .await
                    .map_err(RestDiscoveryError::from)?
            })
            .await
    }
}

// ── URL 重写 helper ──

/// 业务作用：按选定实例的原始地址、端口、路径与查询组装访问 URL。
///
/// 参数说明：
/// - `scheme`：访问实例的 HTTP 或 HTTPS 协议。
/// - `ip`：由调用方校验、含冒号时会加方括号的原始地址。
/// - `port`：目标业务端口。
/// - `path`：已经按逻辑路径约束检查的路径文本。
/// - `query`：可选已编码查询串，不带起始问号。
///
/// 返回：URL 库可解析时返回 URL，否则返回 InvalidUrl；仅完成文本组装及解析，不独立证明最终主机与原始地址身份一致。
fn build_instance_url(
    scheme: InstanceScheme,
    ip: &str,
    port: u16,
    path: &str,
    query: Option<&str>,
) -> Result<reqwest::Url> {
    let host = if ip.contains(':') {
        format!("[{ip}]")
    } else {
        ip.to_string()
    };
    let mut s = format!("{}://{}:{}{}", scheme.as_str(), host, port, path);
    if let Some(q) = query {
        if !q.is_empty() {
            s.push('?');
            s.push_str(q);
        }
    }
    reqwest::Url::parse(&s).map_err(|e| RestDiscoveryError::InvalidUrl {
        url: s,
        reason: e.to_string(),
    })
}

/// 业务作用：在原始外部 URL 文本末尾追加编码查询片段后解析。
///
/// 参数说明：
/// - `url`：待追加的原始 URL 文本。
/// - `query_parts`：将按 & 连接的已编码片段。
///
/// 返回：成功返回解析后的 URL，非法文本返回 InvalidUrl。按原文是否含问号选择分隔符，不先拆分 fragment，因此不能保证带 fragment 的输入把片段加入真正查询部分。
fn append_external_query(url: &str, query_parts: &[String]) -> Result<reqwest::Url> {
    if query_parts.is_empty() {
        return reqwest::Url::parse(url).map_err(|e| RestDiscoveryError::InvalidUrl {
            url: url.to_string(),
            reason: e.to_string(),
        });
    }
    let joined = query_parts.join("&");
    let sep = if url.contains('?') {
        '&'
    } else {
        '?'
    };
    let full = format!("{url}{sep}{joined}");
    reqwest::Url::parse(&full).map_err(|e| RestDiscoveryError::InvalidUrl {
        url: full,
        reason: e.to_string(),
    })
}

/// 业务作用：合并已有查询串和新增编码片段，保留输入顺序。
///
/// 参数说明：
/// - `existing`：原路径提供的可选查询串，不带问号。
/// - `parts`：逐项追加的已编码查询片段。
///
/// 返回：忽略空串后用 & 拼接，全部为空返回 None；不去重或重新编码。
fn merge_query(existing: Option<&str>, parts: &[String]) -> Option<String> {
    let mut pieces: Vec<&str> = Vec::new();
    if let Some(q) = existing {
        if !q.is_empty() {
            pieces.push(q);
        }
    }
    for p in parts {
        if !p.is_empty() {
            pieces.push(p);
        }
    }
    if pieces.is_empty() {
        None
    } else {
        Some(pieces.join("&"))
    }
}

/// 业务作用：按第一个问号划分逻辑路径和原始查询文本。
///
/// 参数说明：
/// - `pq`：需要拆分的路径及可选查询串。
///
/// 返回：无问号返回原文和 None，有问号返回两侧切片；空查询串保留为 Some，不解析 fragment。
fn split_path_query(pq: &str) -> (&str, Option<&str>) {
    match pq.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (pq, None),
    }
}

/// 业务作用：连接层 / Host header:不允许从业务参数或入站请求透传到下游。
///
/// 参数说明：
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
///
/// 返回：名称属于连接级或 Host 保留 header 时为 `true`，普通业务 header 为 `false`。
fn is_forbidden_header(name: &str) -> bool {
    const FORBIDDEN: &[&str] = &[
        "host",
        "content-length",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];
    FORBIDDEN.iter().any(|f| name.eq_ignore_ascii_case(f))
}
