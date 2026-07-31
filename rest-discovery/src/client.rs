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

/// 响应错误摘要保留的最大字符数(避免日志打爆/泄露大响应)。
const BODY_SNIPPET_MAX: usize = 512;

/// 带服务发现 + 负载均衡的 HTTP client。`discovery=None` 即 external-only(`rest_discovery.enabled=false`)。
pub struct RestDiscoveryClient {
    http: reqwest::Client,
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
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
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
    /// 直接用 `RestDiscoveryClient::connect()` 的底层路径(无 `RemoteRuntime`)drop 时也要收掉后台任务
    /// (索引刷新 + watch pump),否则它们成 detached 任务泄漏(`AbortHandle` drop 不会 abort)。
    /// 与 `RemoteRuntime::drop` 的显式 `shutdown_background` 不冲突:abort 幂等。
    fn drop(&mut self) {
        self.shutdown_background();
    }
}

impl RestDiscoveryClient {
    /// 带 provider 的内部模式:`service_request`/`lb://` 走服务发现 + LB。
    ///
    /// **不启动裸 http(s) 启发式索引**(即便 `options.heuristic_http=Enabled`):索引首拉是 async,
    /// 需用 [`connect`](Self::connect)(或经 `RestDiscovery::init_with_discovery`)。本同步构造给「只用档1/档2」场景。
    /// 同步内部模式构造,**非法 options 直接 panic**(便利入口;库代码/可能配错的路径请用 [`try_new`](Self::try_new))。
    ///
    /// # 参数
    ///
    /// - `discovery`: 服务发现 provider，用于按服务名获取实例快照。
    /// - `options`: rest-discovery 运行选项，非法时本便利入口会 panic。
    pub fn new(discovery: Arc<dyn DiscoveryClient>, options: RestDiscoveryOptions) -> Self {
        Self::try_new(discovery, options).expect("rest-discovery: 非法 RestDiscoveryOptions")
    }

    /// 同步内部模式构造的 fail-fast 版:非法 options 返回 `InvalidOptions`,不 panic。
    ///
    /// **不启动裸 http(s) 启发式索引**(即便 `heuristic_http=Enabled`,见 [`new`](Self::new) 说明)。
    ///
    /// # 参数
    ///
    /// - `discovery`: 服务发现 provider，用于按服务名获取实例快照。
    /// - `options`: rest-discovery 运行选项，非法时返回错误。
    pub fn try_new(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
    ) -> Result<Self> {
        let lb = build_lb(options.lb_strategy);
        Self::assemble_sync(discovery, options, lb)
    }

    /// 注入自定义 [`LoadBalancer`] 的内部模式构造(fail-fast):**忽略 `options.lb_strategy`**,用传入算法选址。
    /// 配置只表达内置策略(round_robin/weighted);任意业务算法(按 cluster/metadata/灰度/外部状态)走本程序化入口。
    /// 同样**不启动**启发式索引(见 [`new`](Self::new));需要索引请用 [`connect_with_load_balancer`](Self::connect_with_load_balancer)。
    ///
    /// # 参数
    ///
    /// - `discovery`: 服务发现 provider，用于按服务名获取实例快照。
    /// - `options`: rest-discovery 运行选项，非法时返回错误。
    /// - `load_balancer`: 调用方注入的选址算法，优先于 `options.lb_strategy`。
    pub fn try_new_with_load_balancer(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        load_balancer: Arc<dyn LoadBalancer>,
    ) -> Result<Self> {
        Self::assemble_sync(discovery, options, load_balancer)
    }

    /// `try_new` / `try_new_with_load_balancer` 共用:校验 + 装配(不启动索引)。
    ///
    /// # 参数
    /// - `discovery`: 服务发现客户端或运行时实例。
    /// - `options`: 运行选项,用于控制客户端或调度器行为。
    /// - `lb`: 请求路由使用的负载均衡器。
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
        let source = Arc::new(InstanceSource::new(
            discovery,
            options.watch.clone(),
            metrics.clone(),
            options.no_instance,
        ));
        Ok(Self {
            http: build_http_client(&options.http),
            source: Some(source),
            lb,
            options,
            index: None,
            index_refresh_task: None,
            metrics,
            retry_budgets: DashMap::new(),
            resilience: ResilienceRuntime::new(),
        })
    }

    /// 带 provider 的内部模式,并按 `heuristic_http` 决定是否启动服务名索引(async:首拉 `list_services` + 周期刷新)。
    /// `heuristic_http=Enabled` + `RequireInitialServiceListWhenHeuristicEnabled` 时,首拉失败即返回 `Err`(防空索引误判)。
    ///
    /// # 参数
    ///
    /// - `discovery`: 服务发现 provider，用于实例发现、watch 和服务名首拉。
    /// - `options`: rest-discovery 运行选项，包含启发式索引和 watch 配置。
    pub async fn connect(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
    ) -> Result<Self> {
        let lb = build_lb(options.lb_strategy);
        Self::connect_inner(discovery, options, lb).await
    }

    /// 注入自定义 [`LoadBalancer`] 的 `connect`:**忽略 `options.lb_strategy`**,用传入算法选址
    /// (仍按 `heuristic_http` 决定是否首拉 `list_services` + 启动索引刷新)。
    ///
    /// # 参数
    ///
    /// - `discovery`: 服务发现 provider，用于实例发现、watch 和服务名首拉。
    /// - `options`: rest-discovery 运行选项，包含启发式索引和 watch 配置。
    /// - `load_balancer`: 调用方注入的选址算法，优先于 `options.lb_strategy`。
    pub async fn connect_with_load_balancer(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        load_balancer: Arc<dyn LoadBalancer>,
    ) -> Result<Self> {
        Self::connect_inner(discovery, options, load_balancer).await
    }

    /// `connect` / `connect_with_load_balancer` 共用:校验 + 首拉 + 装配 + 启动索引刷新。
    ///
    /// # 参数
    /// - `discovery`: 服务发现客户端或运行时实例。
    /// - `options`: 运行选项,用于控制客户端或调度器行为。
    /// - `lb`: 请求路由使用的负载均衡器。
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
        let source = Arc::new(InstanceSource::new(
            discovery.clone(),
            options.watch.clone(),
            metrics.clone(),
            options.no_instance,
        ));

        let (index, index_refresh_task) = if options.heuristic_http == HeuristicHttpMode::Enabled {
            let index = Arc::new(ServiceNameIndex::new(
                options.service_match,
                options.heuristic.removed_service_grace,
            ));
            // 首拉必须发生在 client 发布前:否则首批裸 http(s) 请求会看到空索引,把内部服务误判为外部。
            match discovery.list_services().await {
                // 首拉时旧索引为空 → 不会有 removed,丢弃返回值。
                Ok(names) => {
                    index.refresh(names, Instant::now());
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
            let task = tokio::spawn(index_refresh_loop(
                discovery.clone(),
                index.clone(),
                options.heuristic.refresh_interval,
                Arc::downgrade(&source),
            ));
            (Some(index), Some(task.abort_handle()))
        } else {
            (None, None)
        };

        Ok(Self {
            http: build_http_client(&options.http),
            source: Some(source),
            lb,
            options,
            index,
            index_refresh_task,
            metrics,
            retry_budgets: DashMap::new(),
            resilience: ResilienceRuntime::new(),
        })
    }

    /// external-only 模式:只作普通 HTTP client;显式内部调用返回清晰错误,不退化成 DNS。
    /// **非法 options 直接 panic**(便利入口;fail-fast 版见 [`try_external_only`](Self::try_external_only))。
    ///
    /// # 参数
    ///
    /// - `options`: external-only HTTP client 运行选项，非法时本便利入口会 panic。
    pub fn external_only(options: RestDiscoveryOptions) -> Self {
        Self::try_external_only(options).expect("rest-discovery: 非法 RestDiscoveryOptions")
    }

    /// `external_only` 的 fail-fast 版:非法 options 返回 `InvalidOptions`,不 panic。
    /// 只用底层 http client,故只校验 http(不连 provider、不 watch、不建索引)。
    ///
    /// # 参数
    ///
    /// - `options`: external-only HTTP client 运行选项，非法时返回错误。
    pub fn try_external_only(options: RestDiscoveryOptions) -> Result<Self> {
        options.validate_for_external_only()?;
        // external-only 不会进入 send_internal,这里保留一个内置 LB 只是为了结构体字段完整;
        // 显式内部调用会在取 source 时先返回 DiscoveryDisabledForInternalCall。
        let lb = build_lb(options.lb_strategy);
        Ok(Self {
            http: build_http_client(&options.http),
            source: None,
            lb,
            options,
            index: None,
            index_refresh_task: None,
            metrics: Arc::new(RestMetrics::default()),
            retry_budgets: DashMap::new(),
            resilience: ResilienceRuntime::new(),
        })
    }

    /// 是否为 external-only(discovery 禁用)。
    pub fn is_external_only(&self) -> bool {
        self.source.is_none()
    }

    /// 取分流决策指标快照。
    pub fn metrics(&self) -> RestMetricsSnapshot {
        self.metrics.snapshot()
    }

    // ── 请求入口(receiver 为 `&Arc<Self>`:builder 持 `Arc<Self>` 以便 send 时回调) ──

    /// 任意 method;`url` 支持 `lb://service/path`(档2)与 `http(s)://host/path`(档3)。
    ///
    ///
    /// # 参数
    /// - `method`: trait 方法或 HTTP 方法描述。
    /// - `url`: 外部 URL 或连接地址。
    pub fn request(self: &Arc<Self>, method: Method, url: impl AsRef<str>) -> RestRequestBuilder {
        RestRequestBuilder::new(
            self.clone(),
            method,
            BuilderInput::Url(url.as_ref().to_string()),
        )
    }

    /// 读取 get 数据；用于查询当前缓存、配置或远端状态。
    ///
    ///
    /// # 参数
    /// - `url`: 外部 URL 或连接地址。
    pub fn get(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::GET, url)
    }

    /// 创建 POST 请求构造器；用于通过发现客户端发起写入类调用。
    ///
    ///
    /// # 参数
    /// - `url`: 外部 URL 或连接地址。
    pub fn post(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::POST, url)
    }

    /// 写入 put 数据；用于更新缓存、配置或远端状态。
    ///
    ///
    /// # 参数
    /// - `url`: 外部 URL 或连接地址。
    pub fn put(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::PUT, url)
    }

    /// 创建 DELETE 请求构造器；用于通过发现客户端发起删除类调用。
    ///
    ///
    /// # 参数
    /// - `url`: 外部 URL 或连接地址。
    pub fn delete(self: &Arc<Self>, url: impl AsRef<str>) -> RestRequestBuilder {
        self.request(Method::DELETE, url)
    }

    /// 档1 显式内部服务调用:`service` 是注册中心服务名,`path` 是以 `/` 开头的逻辑路径(非完整 URL)。
    /// 不查服务名索引、不外部 fallback;无实例直接 `NoAvailableInstance`。
    ///
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `method`: trait 方法或 HTTP 方法描述。
    /// - `path`: 以 `/` 开头的服务内逻辑请求路径。
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

    /// abort 所有后台任务(索引刷新 + watch pump)(`RemoteRuntime` drop / 运行时 reset 用)。
    pub(crate) fn shutdown_background(&self) {
        if let Some(h) = &self.index_refresh_task {
            h.abort();
        }
        if let Some(source) = &self.source {
            source.abort_all();
        }
    }

    // ── 解析与选址(send 时调用) ──

    /// 把 builder 输入解析成请求计划(分类 + 路径/query 构建 + 分流 metrics)。**不选实例**——
    /// 选址留给 [`send_internal`](Self::send_internal) 的重试循环。
    ///
    /// # 参数
    /// - `input`: 宏或解析器收到的原始输入。
    /// - `scheme_override`: 调用方显式指定的 HTTP scheme 覆盖值。
    /// - `query_parts`: URL query 片段集合。
    fn resolve_plan(
        &self,
        input: &BuilderInput,
        scheme_override: Option<InstanceScheme>,
        query_parts: &[String],
    ) -> Result<RequestPlan> {
        match input {
            // 档1:显式 service。
            BuilderInput::Service { service, path } => {
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
                                    Err(RestDiscoveryError::UnknownServiceHost { host })
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

    /// 档3 启发式命中后的实例协议:只按 `scheme_policy`(Preserve 保留原 http/https)。
    /// 不接受 `scheme_override` —— 请求级 `.scheme()` 不影响启发式裸 http(s) 命中。
    ///
    /// # 参数
    /// - `original_scheme`: 请求 URL 中原始的 scheme。
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

    /// 内部调用:取实例集 + 选址 + 发送;GET/HEAD 传输错误按 `RetryOptions` 重试下一个实例
    /// (POST/PUT/PATCH/DELETE 默认不重试)。
    ///
    /// # 参数
    /// - `method`: trait 方法 AST 或 HTTP 方法。
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `scheme`: HTTP/HTTPS scheme。
    /// - `path`: 发送给目标实例的服务内请求路径。
    /// - `query`: 查询对象或 query 参数集合。
    /// - `headers`: 需要透传到下游请求的 HTTP 头。
    /// - `body`: 请求体、响应体或待处理原始内容。
    /// - `timeout`: 等待或执行超时时间,用于控制阻塞边界。
    /// - `original_host`: 请求进入负载均衡前的原始 Host。
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
        let circuit = self
            .resilience
            .begin_circuit(service, &self.options.resilience)
            .map_err(|error| {
                if matches!(error, RestDiscoveryError::CircuitOpen { .. }) {
                    self.metrics.circuit_open_rejected();
                }
                error
            })?;
        // 一次请求只取一份实例快照。GET/HEAD 的传输错误重试只在这份快照里换未试过的实例;
        // mark_transport_error 打开的 discover+TTL 窗口服务于后续请求,避免同一次请求在错误路径反复拉注册中心。
        let snapshot = source.instances(service).await?;
        // provider 通常已经过滤健康实例,这里仍统一调用 `is_traffic_instance` 做最后防线:
        // disabled、非健康、权重非法、地址为空等实例不能进入负载均衡器。
        let now = Instant::now();
        let avail: Vec<&Instance> = snapshot
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
                service: service.to_string(),
            });
        }

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

        let mut tried = vec![false; avail.len()];
        let mut last_err: Option<reqwest::Error> = None;
        for attempt in 0..max_attempts {
            // 每轮优先尊重负载均衡器选择,但如果它选到已失败实例,就线性找下一个未试实例。
            // 这样既保留正常分布,又避免一次请求内反复打同一个坏实例。
            let idx = match self.pick_untried(service, &avail, &tried) {
                Some(i) => i,
                None => break, // 实例都试过了
            };
            tried[idx] = true;
            let inst = avail[idx];
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
            match req.send().await {
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
                    // 传输错误(连接/超时)→ 触发该 service 快速刷新/恢复,不等下一轮 poll。
                    // 语义边界:「快速刷新/恢复」开的是 discover+TTL 降级窗口,作用于【后续请求】
                    // (下一个请求即走刷新后的 discover 快照),**不是**本次请求立即重新 discover;本次请求只在最初
                    // `avail` 快照里换「未试过的实例」重试(见上方 `avail`:整条请求复用同一份快照,不在重试内重取 instances)。
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

    /// 为一个额外 attempt 消耗每服务 retry token，防止下游故障时所有请求同时倍增流量。
    fn consume_retry_budget(&self, service: &str) -> bool {
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
        allowed
    }

    /// 选一个未试过的实例下标:优先 LB 选址;若 LB 选到已试过的(或 `None`),线性取第一个未试的。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `avail`: 当前仍可尝试的健康实例集合。
    /// - `tried`: 本次请求已经尝试过的实例集合。
    fn pick_untried(&self, service: &str, avail: &[&Instance], tried: &[bool]) -> Option<usize> {
        match self.lb.pick(service, avail) {
            Some(i) if !tried.get(i).copied().unwrap_or(true) => Some(i),
            _ => tried.iter().position(|t| !t),
        }
    }

    /// 用已解析 URL 构造 reqwest 请求。重试时每次重建。
    ///
    /// header 语义:同名 **后写覆盖先写**(last-wins,用 `HeaderMap::insert`);
    /// 过滤 `Host`/hop-by-hop/`Content-Length`(`is_forbidden_header`)。
    /// `Host` 只由 RestDiscovery 内部设置:仅 `preserve_original_host_header` 开启且有 `original_host` 时回写。
    ///
    /// # 参数
    /// - `method`: trait 方法 AST 或 HTTP 方法。
    /// - `url`: 外部 URL 或连接地址。
    /// - `context`: header/body/timeout/trace/original-host 的一次 attempt 快照。
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

/// 判定一次下游响应是否应计入服务熔断与实例异常摘除。
fn is_circuit_failure_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

/// 构造内部使用的 reqwest client,挂上单请求总超时 + 连接超时(默认 10s / 2s,防请求长挂)。
///
/// 安全默认(2026-07-13):
/// - `redirect(none)`:LB 自己按注册实例选目标,不该追随后端返回的 3xx——否则一个有 bug/被攻陷的
///   上游实例可用 `Location:` 把请求弹到任意 host(如云元数据 `169.254.169.254`),破坏「只连注册
///   实例」隔离;且 reqwest 跨 host 重定向只剥 `Authorization`,业务经 `.header()` 传的鉴权 token 会
///   被重放到重定向目标。
/// - `no_proxy()`:内部实例 IP 直连不该被 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量劫持(最小惊讶 + 防外泄)。
///
/// # 参数
/// - `http`: REST 底层 HTTP client 的超时配置。
fn build_http_client(http: &RestHttpOptions) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(http.timeout)
        .connect_timeout(http.connect_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .expect("rest-discovery: 构造 reqwest::Client 失败")
}

/// 服务名索引周期刷新任务:每 `interval` 拉一次 `list_services` 刷新;失败保留旧索引并 warn。
/// 刷新后把【超 grace 移出索引】的服务名交给 `InstanceSource::mark_removed` 回收其 watch(churn cleanup)。
/// 持 `Weak<InstanceSource>`:client 已 drop 时 upgrade 失败 → 本任务也会被 `shutdown_background` abort。
///
/// # 参数
/// - `discovery`: 服务发现客户端或运行时实例。
/// - `index`: 列表下标、归档序号或字段位置。
/// - `interval`: 后台轮询、心跳或重试任务的执行间隔。
/// - `source`: 实例列表更新的来源标识。
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
                let outcome = index.refresh(names, Instant::now());
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

/// builder 的输入:要么显式 service(档1),要么字符串 URL(档2/3)。
#[derive(Debug, Clone)]
enum BuilderInput {
    Service { service: String, path: String },
    Url(String),
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

/// 按策略构造负载均衡器。
///
/// # 参数
/// - `strategy`: 实例选择和重试使用的负载均衡策略。
fn build_lb(strategy: LbStrategy) -> Arc<dyn LoadBalancer> {
    match strategy {
        LbStrategy::RoundRobin => Arc::new(RoundRobinLoadBalancer::new()),
        LbStrategy::Weighted => Arc::new(WeightedRoundRobinLoadBalancer::new()),
    }
}

/// 可安全重试下一个实例的传输错误(连接失败 / 超时)。响应已收到的状态错误不在此列。
///
/// # 参数
/// - `e`: 错误对象或外部错误值。
fn is_retryable_transport(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout()
}

/// 允许自动重试的瞬态 HTTP 状态；普通业务 4xx 永不重试。
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::TOO_MANY_REQUESTS
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

/// 解析 `Retry-After` 的 delta-seconds 或 HTTP-date；非法值视为未提供。
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

/// 将单次 attempt timeout 收敛到调用链剩余预算。
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
    /// 以满 token 状态创建指定容量的服务级 retry bucket。
    fn new(capacity: u32, now: Instant) -> Self {
        Self {
            tokens: f64::from(capacity),
            last_refill: now,
        }
    }

    /// 按单调时间补充 token，并原子语义地尝试消费一次 retry 配额。
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
    /// 构造新实例；用于集中初始化内部字段和默认状态。
    ///
    /// # 参数
    /// - `client`: 底层客户端或连接句柄。
    /// - `method`: trait 方法 AST 或 HTTP 方法。
    /// - `input`: 宏或解析器收到的原始输入。
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

    /// 覆盖实例协议(仅对 `service_request`/`lb://` 生效;外部 URL 保留原 scheme)。
    ///
    /// # 参数
    ///
    /// - `scheme`: 显式内部调用或 `lb://` 请求访问实例时使用的协议。
    pub fn scheme(mut self, scheme: InstanceScheme) -> Self {
        self.scheme_override = Some(scheme);
        self
    }

    /// 追加一个请求头(`Host`/hop-by-hop 会在 send 时被过滤)。
    ///
    /// # 参数
    ///
    /// - `key`: HTTP header 名称。
    /// - `value`: HTTP header 值。
    pub fn header(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.headers
            .push((key.as_ref().to_string(), value.as_ref().to_string()));
        self
    }

    /// 批量并入一张 HeaderMap(供宏 `#[RequestHeaders]` 生成;`Host`/hop-by-hop 在 send 时统一过滤)。
    ///
    /// # 参数
    ///
    /// - `map`: 待并入请求 builder 的 header 集合。
    pub fn headers_from_map(mut self, map: &reqwest::header::HeaderMap) -> Self {
        for (name, value) in map.iter() {
            if let Ok(v) = value.to_str() {
                self.headers
                    .push((name.as_str().to_string(), v.to_string()));
            }
        }
        self
    }

    /// 追加 query 参数(`&[(k,v)]` / 实现 `Serialize` 的结构体均可)。
    ///
    /// # 参数
    /// - `q`: 查询对象或 query 参数集合。
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

    /// 追加单个 query 参数(供宏 `#[RequestParam]` 生成;`Display` 值)。
    ///
    /// # 参数
    ///
    /// - `name`: query 参数名。
    /// - `value`: query 参数值，会按 `Display` 转为字符串后 URL 编码。
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

    /// 追加一组同名 query(`Vec<T>` 等多值 → **重复 key**:`name=v1&name=v2`;供宏 `#[RequestParam] Vec<T>` 生成)。
    ///
    /// # 参数
    /// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
    /// - `values`: 待校验、写入或比较的值列表。
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

    /// 设置 JSON body(`Content-Type: application/json` 由 reqwest 处理)。
    ///
    /// # 参数
    /// - `body`: 请求体、响应体或待处理原始内容。
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

    /// 设置 form body(`application/x-www-form-urlencoded`,`serde_urlencoded` 编码)。供宏 `#[FormBody]` 生成。
    ///
    /// # 参数
    /// - `body`: 请求体、响应体或待处理原始内容。
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

    /// 设置原样字节 body(供宏 `#[RequestBody(raw)]` 生成)。不做 JSON/urlencoded 编码;
    /// Content-Type 由 `consumes` / `#[RequestHeader]` 决定,不配置就不写。`AsRef<[u8]>` 统一接 `Bytes`/`Vec<u8>`/`String`/`&str`。
    ///
    /// # 参数
    /// - `body`: 请求体、响应体或待处理原始内容。
    pub fn raw_body<B: AsRef<[u8]>>(mut self, body: B) -> Self {
        // builder 必须拥有请求体,不能保存调用方引用;这里拷一次换取 &str/String/Vec/Bytes
        // 四类入参统一可用,也避免 async 发送阶段出现借用生命周期约束。
        self.body = Some(RestBody::Raw(bytes::Bytes::copy_from_slice(body.as_ref())));
        self
    }

    /// 单请求超时。
    ///
    /// # 参数
    ///
    /// - `d`: 覆盖本次请求的总超时时长。
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// 绑定调用链绝对预算。预算覆盖 discovery、Retry-After 等待和全部 attempts。
    pub fn budget(mut self, budget: &RequestBudget) -> Self {
        self.budget = Some(budget.clone());
        self
    }

    /// 绑定当前 W3C trace context；发送时自动覆盖并注入唯一 `traceparent`。
    pub fn trace_context(mut self, trace_context: &TraceContext) -> Self {
        self.trace_context = Some(*trace_context);
        self
    }

    /// 同时绑定 Web 请求扩展中的预算与 trace。
    pub fn request_context(self, budget: &RequestBudget, trace_context: &TraceContext) -> Self {
        self.budget(budget).trace_context(trace_context)
    }

    /// 显式声明本请求具备业务幂等合同，允许非 GET/HEAD 方法按已配置策略自动重试。
    ///
    /// 调用方必须有幂等键、幂等 store 或等价证明；仅因“希望成功”而打开会造成重复写。
    pub fn idempotent(mut self) -> Self {
        self.explicitly_idempotent = true;
        self
    }

    /// 发送并返回原始 `reqwest::Response`(不做 `error_for_status`)。
    pub async fn send(mut self) -> Result<reqwest::Response> {
        if self
            .budget
            .as_ref()
            .is_some_and(RequestBudget::is_exhausted)
        {
            return Err(RestDiscoveryError::BudgetExhausted);
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
        let result = match budget {
            Some(budget) => {
                tokio::select! {
                    _ = budget.cancelled() => Err(RestDiscoveryError::Cancelled),
                    result = tokio::time::timeout_at(budget.deadline(), operation) => {
                        result.map_err(|_| RestDiscoveryError::BudgetExhausted)?
                    }
                }
            }
            None => operation.await,
        };
        if let Some(span) = span.take() {
            let status = result
                .as_ref()
                .ok()
                .map(|response| response.status().as_u16());
            let _ = span.finish(status);
        }
        result
    }

    /// 已由 [`Self::send`] 施加绝对 deadline/cancellation 后执行解析、发现与发送。
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

    /// 发送并把 2xx body 反序列化为 `T`;非 2xx 返回 `HttpStatus`(带 body 摘要)。
    ///
    /// 错误分类:body 读取(传输层)失败 → `Http`;读到了但 JSON 反序列化失败(2xx 响应体不符合约定)
    /// → `ResponseDecodeFailed`,与 `send_json_unwrap` 一致(传输失败 vs 响应契约失败彻底分开)。
    pub async fn send_json<T: DeserializeOwned>(self) -> Result<T> {
        let resp = self.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RestDiscoveryError::HttpStatus {
                status,
                body_snippet: body_snippet(&body),
            });
        }
        // 先读字节(传输失败 → Http),再本地反序列化(契约失败 → ResponseDecodeFailed)。
        let bytes = resp.bytes().await?;
        serde_json::from_slice(&bytes).map_err(|e| RestDiscoveryError::ResponseDecodeFailed {
            reason: format!("响应体 JSON 反序列化失败:{e}"),
        })
    }

    /// 发送并把 2xx JSON 响应的【顶层字段 `field`】解包后反序列化为 `T`(供宏 `unwrap = "data"` 生成)。
    /// 非 2xx → `HttpStatus`;body 非 JSON / 缺字段 / 字段类型不匹配 → `ResponseDecodeFailed`(契约错误,区别于传输层 `Http`)。
    ///
    /// # 参数
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    pub async fn send_json_unwrap<T: DeserializeOwned>(self, field: &str) -> Result<T> {
        let resp = self.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RestDiscoveryError::HttpStatus {
                status,
                body_snippet: body_snippet(&body),
            });
        }
        // 先读字节(传输失败 → Http),再本地解析 Value 取顶层字段;声明式客户端场景可接受一次中转分配。
        let bytes = resp.bytes().await?;
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

    /// 发送并返回 2xx 文本;非 2xx 返回 `HttpStatus`。
    pub async fn send_text(self) -> Result<String> {
        let resp = self.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(RestDiscoveryError::HttpStatus {
                status,
                body_snippet: body_snippet(&body),
            });
        }
        Ok(body)
    }

    /// 发送并只校验 2xx、丢弃 body(供宏 `-> anyhow::Result<()>` 生成)。
    pub async fn send_ok(self) -> Result<()> {
        let resp = self.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RestDiscoveryError::HttpStatus {
                status,
                body_snippet: body_snippet(&body),
            });
        }
        Ok(())
    }

    /// 发送并返回 2xx 响应体字节(供宏 `response = "bytes"` 生成)。
    pub async fn send_bytes(self) -> Result<bytes::Bytes> {
        let resp = self.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RestDiscoveryError::HttpStatus {
                status,
                body_snippet: body_snippet(&body),
            });
        }
        Ok(resp.bytes().await?)
    }
}

// ── URL 重写 helper ──

/// 内部档:`scheme://ip:port` + path + 已合并的 query。IPv6 ip 自动加方括号。
///
/// # 参数
/// - `scheme`: HTTP/HTTPS scheme。
/// - `ip`: 服务实例或目标节点的 IP 地址。
/// - `port`: 服务实例暴露给调用方访问的端口。
/// - `path`: 服务实例上的 HTTP path。
/// - `query`: 查询对象或 query 参数集合。
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

/// 外部档:原 URL 直发,只把 builder 累积的 query 追加上去。
///
/// # 参数
/// - `url`: 外部 URL 或连接地址。
/// - `query_parts`: URL query 片段集合。
fn append_external_query(url: &str, query_parts: &[String]) -> Result<reqwest::Url> {
    if query_parts.is_empty() {
        return reqwest::Url::parse(url).map_err(|e| RestDiscoveryError::InvalidUrl {
            url: url.to_string(),
            reason: e.to_string(),
        });
    }
    let joined = query_parts.join("&");
    let sep = if url.contains('?') { '&' } else { '?' };
    let full = format!("{url}{sep}{joined}");
    reqwest::Url::parse(&full).map_err(|e| RestDiscoveryError::InvalidUrl {
        url: full,
        reason: e.to_string(),
    })
}

/// 合并「原有 query」与「builder 累积 query 片段」,空则 `None`。
///
/// # 参数
/// - `existing`: Redis 中已存在的协议标记或缓存值。
/// - `parts`: 路径、占位符或 SQL 片段拆分后的部分。
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

/// 拆 `/path?query` → `("/path", Some("query"))`。
///
/// # 参数
/// - `pq`: 按权重或失败次数排序的优先队列。
fn split_path_query(pq: &str) -> (&str, Option<&str>) {
    match pq.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (pq, None),
    }
}

/// 连接层 / Host header:不允许从业务参数或入站请求透传到下游。
///
/// # 参数
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
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

/// 截取响应 body 前 N 字符作错误摘要(按 char 边界,避免切断多字节)。
///
/// # 参数
/// - `body`: 请求体、响应体或待处理原始内容。
fn body_snippet(body: &str) -> String {
    if body.chars().count() <= BODY_SNIPPET_MAX {
        body.to_string()
    } else {
        let s: String = body.chars().take(BODY_SNIPPET_MAX).collect();
        format!("{s}…")
    }
}
