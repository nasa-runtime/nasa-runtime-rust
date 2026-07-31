//! 带服务发现和客户端负载均衡的 HTTP client。
//!
//! 面向 `nadisc::DiscoveryClient` 抽象工作，支持 `lb://service/path`、显式服务请求和外部 URL 直连。
// ============================================================================
// rest-discovery —— 带服务发现 + 客户端负载均衡的 HTTP client。
//
// 设计边界(实现基准)
//   · 只面向 `nadisc::DiscoveryClient` 抽象做 LB + URL 重写,**绝不依赖 nacos crate**。
//     后端由调用方在 main 里 connect 后装成 `Arc<dyn DiscoveryClient>` 注入(换 eureka/consul 本 crate 不动)。
//   · 三档识别:
//       档1 service_request(service, method, path)  → 显式内部直连(不查索引、不外部 fallback)
//       档2 lb://service/path                        → 显式内部直连
//       档3 http(s)://host/path                      → 默认(heuristic_http=Disabled)普通外部 HTTP;
//                                                       heuristic_http=Enabled 时 host 命中服务名索引才走内部 LB,
//                                                       未命中按 unknown_host(外部直连 / UnknownServiceHost)
//   · 实例事实源单一化:`watch_with_options` 快照为主,`discover + 短 TTL` 仅作 watch 失败/中断的降级。
//   · 负载均衡:round-robin,每服务计数器随机起种,避免冷启动请求集中打到同一实例。
//   · 装配:手动装配为默认(`Arc::new(XxxClient::new(RestDiscovery::get()))`);不引 DI 容器 / linkme 注册表。
//
// 全局门面:`RestDiscovery`(经 nasa 重导出为 `nasa::nadisc::RestDiscovery`)。
//   main: RestDiscovery::init_with_discovery(Arc::new(nacos_client), opts).await?;  // 或 init_external_only
//   任意位置: RestDiscovery::get().request(Method::GET, "lb://svc/path").send().await?;
// ============================================================================
#![forbid(unsafe_code)]

mod classify;
mod client;
mod error;
mod facade;
mod index;
mod instances;
mod lb;
mod metrics;
mod options;
mod resilience;
mod runtime;

/// 声明式客户端宏(`rest-client-macro`)展开专用的 runtime helper + 第三方桥接。不属于稳定业务 API。
#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;

pub use crate::client::{RestDiscoveryClient, RestRequestBuilder};
pub use crate::error::{RestDiscoveryError, Result};
pub use crate::facade::RestDiscovery;
pub use crate::lb::{LoadBalancer, RoundRobinLoadBalancer, WeightedRoundRobinLoadBalancer};
pub use crate::metrics::{RestMetrics, RestMetricsSnapshot};
pub use crate::options::{
    HeuristicHttpMode, InstanceScheme, LbStrategy, NoInstancePolicy, RestDiscoveryOptions,
    RestHeuristicOptions, RestHttpOptions, RestResilienceOptions, RestWatchOptions, RetryOptions,
    SchemePolicy, ServiceMatchMode, StartupPolicy, UnknownHostPolicy,
};
pub use crate::runtime::RemoteRuntime;
pub use nabudget::RequestBudget;
pub use natelemetry::{SpanRecorder, TraceContext};

// 便利重导出:业务写 `lb://...` 调用需要 `reqwest::Method`,经此免去单独依赖 reqwest 的写法。
pub use reqwest::{self, Method, StatusCode};
