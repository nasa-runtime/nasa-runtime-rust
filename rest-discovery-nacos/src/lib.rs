//! RestDiscovery 与 Nacos 的装配便利层。
//! 本组件保留单 Nacos 装配；多注册中心由 nadisc-runtime 的具名连接目录持有。
//!
//! 将 Nacos 服务发现、注册守卫和 HTTP 负载均衡客户端组合成业务启动期的一站式入口。
//! [`DiscoverySession`] 把出站准备、本实例注册、摘流和运行时关闭分开：先准备出站客户端，
//! listener 就绪后注册，停机先注销实例、等待业务排空，再等待客户端后台任务退出。
//! Application 持有唯一会话；独立调用方负责相同顺序与关闭预算，不得把关闭通知当作退出证明。
//! 真实后端需要 `nacos-sdk`；本组件不选择多注册中心，也不把发现地址当作业务健康证明。
// ============================================================================
// rest-discovery-nacos —— RestDiscovery + Nacos 一键装配便利层。
//
//   业务 main 只调一个入口(经 nasa 门面):
//     装配入口为 `nasa::discovery::init_from_config`，参数是发现配置和应用注册信息。
//   优雅停机:disc.deregister().await(先摘流)→ 再 drain HTTP server → 最后 drop。
//
// 边界:rest-discovery 默认路径面向 provider-neutral 抽象;本 crate 收敛 Nacos 装配 +
//   DiscoveryConfig(yml 强类型)→ RestDiscoveryOptions 映射 + 注册生命周期(DiscoveryHandle 交 main 持有)。
// ============================================================================
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nanacos::{NacosDiscoveryClient, NacosProps, RegistrationGuard};
use rest_discovery::{
    HeuristicHttpMode, InstanceScheme, LbStrategy, LoadBalancer, NoInstancePolicy, RemoteRuntime,
    RestDiscovery, RestDiscoveryOptions, RestHeuristicOptions, RestHttpOptions, RestWatchOptions,
    RetryOptions, SchemePolicy, ServiceMatchMode, SpanRecorder, StartupPolicy, UnknownHostPolicy,
};
use serde::Deserialize;

/// provider-neutral 实例快照，供上层 typed resolver 读取固定协议元数据。
pub use nanacos::Instance;
/// 带服务发现和负载均衡的底层 HTTP 客户端类型。
///
/// 便利层重导出该类型，使宿主容器无需额外绑定实现 crate 路径也能提供强类型能力入口。
pub use rest_discovery::RestDiscoveryClient;

// ── 强类型配置(对齐) ──

/// 服务发现总配置(对外 yml 根 `rest_discovery`;RestDiscovery 是统一远程访问门面,不只注册中心,
/// 故根名不用易误解的 `discovery`)。配置中心与注册中心解耦:本配置独立于 config-center 的 nacos 段。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    /// 总开关:false → external-only(普通 http 可用,lb:// 报清晰错误)。
    pub enabled: bool,
    /// 后端类型(目前仅 nacos)。
    pub provider: ProviderKind,
    /// 注册中心连接参数(可与 config-center 的 nacos 重复填写)。
    pub nacos: NacosConnConfig,
    /// 本实例注册(`enabled=false` → 只作消费者,不注册自己)。
    pub registration: RegistrationConfig,
    /// RestDiscoveryClient 运行选项。
    pub rest: RestConfig,
}

impl Default for DiscoveryConfig {
    /// 业务作用：返回默认配置；用于未显式设置时提供稳定基线。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回关闭发现、使用 Nacos 提供方及默认子配置的值；不会连接、注册或发布全局运行时。
    fn default() -> Self {
        Self {
            enabled: false,
            provider: ProviderKind::Nacos,
            nacos: NacosConnConfig::default(),
            registration: RegistrationConfig::default(),
            rest: RestConfig::default(),
        }
    }
}

/// 后端类型；当前仅支持 Nacos，未知类型会在配置解析阶段被拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ProviderKind {
    /// 使用 Nacos 作为服务注册发现后端。
    #[default]
    Nacos,
}

/// 注册中心连接参数。
///
/// `Debug` 手写脱敏:`username`/`password` 只标记是否已设,不打印明文。`DiscoveryConfig` 内嵌本结构体,
/// 其 derive(Debug) 会委托到这里,故一处脱敏即覆盖整条配置的日志路径(公共库默认防御)。
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct NacosConnConfig {
    /// Nacos SDK 地址,通常是 `host:8848`。
    pub server_addr: String,
    /// Nacos namespace;为空表示 public namespace。
    pub namespace: String,
    /// 服务注册和发现使用的 group。
    pub group: String,
    /// SDK 客户端应用名,用于连接标识和日志。
    pub app_name: String,
    /// Nacos 用户名;为空表示不启用用户名密码鉴权。
    pub username: String,
    /// Nacos 密码;Debug 输出会脱敏。
    pub password: String,
    /// 注册 IP 的【第三优先级】fallback(对外 yml 路径 `rest_discovery.nacos.discovery_ip`)。
    /// 只参与 `resolve_registration` 的优先级链,**不写入** [`NacosProps::discovery_ip`]
    /// (否则底层 `register` 会用第三优先级覆盖前两级;`NacosProps.discovery_ip` 保留给直接用 nacos 组件的调用方)。
    pub discovery_ip: Option<String>,
}

impl std::fmt::Debug for NacosConnConfig {
    /// 业务作用：展示 Nacos 连接配置，隐藏用户名和密码正文。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的标准格式化器。
    ///
    /// 返回：成功写出摘要或返回格式化错误；不查询注册中心，不能作为远端健康确认。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NacosConnConfig")
            .field("server_addr", &self.server_addr)
            .field("namespace", &self.namespace)
            .field("group", &self.group)
            .field("app_name", &self.app_name)
            .field(
                "username",
                &if self.username.is_empty() {
                    "<empty>"
                } else {
                    "<set>"
                },
            )
            .field(
                "password",
                &if self.password.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .field("discovery_ip", &self.discovery_ip)
            .finish()
    }
}

/// 本实例注册参数。`service_name`/`port` 的空/0 回退到 [`AppRegistrationInfo`](运行期 app 自报);
/// `ip` 不回退 app 自报,走独立优先级链(见 [`RegistrationConfig::ip`])。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RegistrationConfig {
    /// 是否注册本实例(false = 只作消费者,不读取也不校验注册 IP)。
    pub enabled: bool,
    /// 服务名(空 → 用 app 自报)。
    pub service_name: String,
    /// 对外 IP,注册 IP 的【第二优先级】
    /// `LOCAL_NETWORK_IP` 环境变量 → 本字段 → `rest_discovery.nacos.discovery_ip` → fail-fast。
    /// `LOCAL_NETWORK_IP` 未设置时,本字段若仍是未解析占位符(如 `${local.network.ip}`)
    /// 视为显式配置错误,fail-fast 不 fallback(env 命中则直接胜出,不看本字段)。
    pub ip: String,
    /// 端口(0 → 用 app 自报)。
    pub port: u16,
    /// 是否注册为临时实例;临时实例会随心跳/连接断开自动摘除。
    pub ephemeral: bool,
    /// 初始健康状态;通常保持 true,由健康检查或治理侧后续调整。
    pub healthy: bool,
    /// 负载均衡权重;越大越容易被客户端选中。
    pub weight: f64,
    /// 本实例从注册中心健康实例集消失(心跳失联/被驱逐)时,napp 运行期 monitor 是否升级为**关键**就绪失败
    ///:默认 `false` = 非关键 Degraded
    /// (本地注册 guard 仍持、SDK 心跳自愈,不摘流);`true` = monitor 确认失联时置 NotReady → `/readyz` 503,
    /// 交由编排替换本实例。回查本身失败(provider 抖动、失联未确认)始终只 Degraded、不因此旋钮升级。
    pub readiness_critical: bool,
}

impl Default for RegistrationConfig {
    /// 业务作用：返回默认配置；用于未显式设置时提供稳定基线。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回启用注册、临时且健康、权重为 1 的配置；服务名与端口待应用补齐，地址待显式解析，注册丢失默认非关键降级。
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: String::new(),
            ip: String::new(),
            port: 0,
            ephemeral: true,
            healthy: true,
            weight: 1.0,
            readiness_critical: false,
        }
    }
}

/// RestDiscoveryClient 选项(字符串枚举 + ms 调优;空/缺省走 `RestDiscoveryOptions` 默认)。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RestConfig {
    /// 是否启用裸 http(s) URL 的服务名启发式识别。
    pub heuristic_http: bool,
    /// 服务名匹配方式,控制 host 与服务列表如何对应。
    pub service_match: String,
    /// 裸 http(s) URL 入口的 scheme 策略:preserve / force_http / force_https。
    pub scheme_policy: String,
    /// `service_request`/`lb://` 无原始 scheme 时连实例的默认协议:http / https。
    pub default_instance_scheme: String,
    /// 客户端负载均衡策略,例如 round_robin。
    pub lb_strategy: String,
    /// 启发式启动策略:require_initial_service_list_when_heuristic_enabled / allow_empty_service_index。
    pub startup: String,
    /// 未知 host 策略:报错或保留外部直连。
    pub unknown_host: String,
    /// 服务无可用实例时的处理策略。
    pub no_instance: String,
    /// 转发到实例时是否保留原始 Host header。
    pub preserve_original_host_header: bool,
    /// 裸 http(s) 启发式索引调优(刷新间隔 / 服务名缺席 grace)。
    pub heuristic: HeuristicConfig,
    /// 服务实例 watch 缓存与失败降级参数。
    pub watch: WatchConfig,
    /// 下游请求重试参数。
    pub retry: RetryConfig,
    /// HTTP client 超时参数。
    pub http: HttpConfig,
}

/// 保存 HeuristicConfig 配置项；用于把外部参数集中传入运行时。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct HeuristicConfig {
    /// 服务名索引刷新间隔(默认 30s);> 0。
    pub refresh_interval_ms: Option<u64>,
    /// 服务名从 list_services 消失后从索引移除的 grace(默认 60s);> 0。
    pub removed_service_grace_ms: Option<u64>,
}

/// 保存 WatchConfig 配置项；用于把外部参数集中传入运行时。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct WatchConfig {
    /// 允许保留的服务订阅身份上限，缺省为 1024。
    pub max_services: Option<usize>,
    /// watch 不可用时的轮询间隔毫秒。
    pub poll_interval_ms: Option<u64>,
    /// 发现结果的兜底 TTL 毫秒数。
    pub ttl_fallback_ms: Option<u64>,
    /// watch/刷新失败时允许继续使用旧实例列表的最长毫秒数。
    pub stale_if_error_ms: Option<u64>,
    /// watch 重启最小退避毫秒数。
    pub restart_backoff_min_ms: Option<u64>,
    /// watch 重启最大退避毫秒数。
    pub restart_backoff_max_ms: Option<u64>,
}

/// 保存 RetryConfig 配置项；用于把外部参数集中传入运行时。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    /// GET/HEAD 在网络错误时是否允许重试。
    pub get_head_on_transport_error: bool,
    /// GET/HEAD 在 429/502/503/504 上按 Retry-After 重试。
    pub get_head_on_retryable_status: bool,
    /// 单次请求最大尝试次数;None 使用运行时默认值。
    pub max_attempts: Option<usize>,
    /// 每服务额外 attempt 的 token bucket 容量。
    pub budget_capacity: Option<u32>,
    /// 每服务每秒恢复 token 数。
    pub budget_refill_per_second: Option<f64>,
}

/// 保存 HttpConfig 配置项；用于把外部参数集中传入运行时。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    /// 整体请求超时毫秒数;None 使用 HTTP client 默认值。
    pub timeout_ms: Option<u64>,
    /// 建连超时毫秒数;None 使用 HTTP client 默认值。
    pub connect_timeout_ms: Option<u64>,
}

/// 运行期 app 自报的注册信息。[`RegistrationConfig`] 的 `service_name`/`port` 空/0 时回退到它;
/// **`ip` 不再作为注册 IP fallback**(app 常传监听 host,`0.0.0.0`/`::` 会绕过优先级规则;
/// 字段保留仅为兼容调用签名。
#[derive(Debug, Clone)]
pub struct AppRegistrationInfo {
    /// 应用自报服务名。
    pub service_name: String,
    /// 应用自报监听地址,不参与注册 IP 自动选择。
    pub ip: String,
    /// 应用自报监听端口。
    pub port: u16,
    /// 由框架生成、需要随实例一同发布的封闭 endpoint 元数据。
    pub metadata: HashMap<String, String>,
}

impl AppRegistrationInfo {
    /// 业务作用：构造运行期应用注册信息,供 discovery 初始化时作为配置缺省值。
    ///
    /// 参数说明:
    /// - `service_name`: 应用自报服务名,当配置中的注册服务名为空时作为回退。
    /// - `ip`: 应用自报监听地址,仅保留在结构中供兼容展示,不再作为注册 IP 优先级回退。
    /// - `port`: 应用自报监听端口,当配置中的注册端口为 0 时作为回退。
    ///
    /// 返回：元数据为空、其余字段保留调用方语义的注册信息。
    pub fn new(service_name: impl Into<String>, ip: impl Into<String>, port: u16) -> Self {
        Self {
            service_name: service_name.into(),
            ip: ip.into(),
            port,
            metadata: HashMap::new(),
        }
    }

    /// 业务作用：追加一条由上层组件生成的实例元数据，供 provider adapter 原样发布。
    ///
    /// 参数说明:
    /// - `key`: provider 可持久化的稳定元数据键。
    /// - `value`: 不含凭据或动态业务身份的元数据值。
    ///
    /// 返回：带新增元数据的注册信息；同名键以后一次调用为准。
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// 装配句柄:由业务 main 持有,控制注册实例的生命周期(= 流量生命周期)。
///
/// 优雅停机:`deregister().await`【先摘流】→ 业务再 drain HTTP → 最后 drop。
/// `Drop` 只做 best-effort 兜底(`RegistrationGuard` 自带),**不能依赖 Drop 完成正式下线**。
/// `deregister` 不影响 `RestDiscovery::get()` 处理正在 drain 的请求(LB client 由全局 runtime 持有,与本 handle 无关)。
pub struct DiscoveryHandle {
    /// 命名服务连接保活(RestDiscovery 全局 runtime 也持有一份;此处冗余保活,使 handle 自洽)。
    _client: Option<Arc<NacosDiscoveryClient>>,
    registration: Option<RegistrationGuard>,
}

impl DiscoveryHandle {
    /// 业务作用：创建仅表示外部调用装配完成的空生命周期句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回没有提供方连接与注册句柄的装配句柄；本方法不创建 HTTP 客户端或发布全局状态。
    fn external_only() -> Self {
        Self {
            _client: None,
            registration: None,
        }
    }

    /// 业务作用：取走注册句柄并显式等待注销，不关闭共享出站客户端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：有句柄时先取走并等待注销，返回具体结果；失败或取消后本地也不再持有句柄，Drop 仅尽力接力。后续调用直接成功，不证明之前注销已完成。
    pub async fn deregister(&mut self) -> anyhow::Result<()> {
        if let Some(reg) = self.registration.take() {
            reg.deregister().await?;
        }
        Ok(())
    }

    /// 业务作用：读取本地注册句柄的持有状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅当本地仍持有注册句柄时返回 true，不向注册中心核验存在性或健康状态。
    pub fn is_registered(&self) -> bool {
        self.registration.is_some()
    }
}

impl std::fmt::Debug for DiscoveryHandle {
    /// 业务作用：展示本地是否仍持有注册句柄。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的标准格式化器。
    ///
    /// 返回：成功写出摘要或返回格式化错误；不查询注册中心，不能作为远端健康确认。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryHandle")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}

// ── 分段生命周期入口(供 Application 编排) ──

/// 分段装配会话:把"连 provider + 安装出站 runtime"和"注册本实例"拆成两步。
///
/// 应用容器需要这三段互相独立:UserHook 之前出站 `lb://` 就要可用(预热/建表可能调下游),
/// 本实例只能在监听端口就绪且 Hook 成功后才注册,停机则必须先摘流、等在途请求 drain 完,
/// 最后才关客户端后台任务。一步到位的 [`init_from_config`] 无法表达这个顺序。
pub struct DiscoverySession {
    config: DiscoveryConfig,
    client: Option<Arc<NacosDiscoveryClient>>,
    runtime: Option<Arc<RemoteRuntime>>,
    registration: Option<RegistrationGuard>,
    /// 注册成功时保存的本实例身份 `(service, ip, port)`,供运行期回查
    /// [`self_registration_healthy`](Self::self_registration_healthy) 在注册中心的健康实例集里比对定位;
    /// 未注册 / 纯消费者时为 `None`。`deregister` 后清空。
    registered_identity: Option<(String, String, u16)>,
}

impl DiscoverySession {
    /// 业务作用：返回当前会话安装的共享出站 HTTP 客户端。 clone 只增加客户端共享引用；会话关闭运行时时仍会显式停止索引刷新和监听任务，因此外部句柄 不会阻止宿主执行停机协议，但停机后不得再发起新请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：会话仍保留运行时时返回共享客户端，否则返回 None；关闭未排干但保留 owner 时仍可能返回已关闭准入的客户端，不能凭 Some 推断可发新请求。
    pub fn rest_client(&self) -> Option<Arc<RestDiscoveryClient>> {
        self.runtime.as_ref().map(|runtime| runtime.rest())
    }

    /// 业务作用：读取本地注册句柄的持有状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅当本地仍持有注册句柄时返回 true，不向注册中心核验存在性或健康状态。
    pub fn is_registered(&self) -> bool {
        self.registration.is_some()
    }

    /// 业务作用：本配置是否要求注册本实例。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发现总开关与实例注册开关同时开启时返回 true，否则返回 false。
    pub fn wants_registration(&self) -> bool {
        self.config.enabled && self.config.registration.enabled
    }

    /// 业务作用：在调用方确认监听及业务准备完成后，按配置登记实例并保存回查身份。
    ///
    /// 参数说明：
    /// - `app`：服务名与端口缺省值，以及需随实例发布的元数据；app.ip 不参与注册地址选择。
    ///
    /// 返回：无需注册或已持有句柄时直接成功；否则解析身份并等待远端登记，确认后保存句柄与回查身份。缺少提供方、配置非法或远端失败时返回错误；成功不证明业务端点健康或实例承载流量。
    pub async fn register(&mut self, app: AppRegistrationInfo) -> anyhow::Result<()> {
        // 配置未开放注册或已经持有句柄时不再次登记，避免同一会话产生重复生命周期责任。
        if !self.wants_registration() || self.registration.is_some() {
            return Ok(());
        }
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("rest-discovery-nacos: provider 未连接,无法注册本实例")
        })?;
        let (service, ip, port) = resolve_registration(
            &self.config.registration,
            &app,
            self.config.nacos.discovery_ip.as_deref(),
        )?;
        let instance = Instance::new(ip.clone(), port)
            .with_ephemeral(self.config.registration.ephemeral)
            .with_healthy(self.config.registration.healthy)
            .with_weight(self.config.registration.weight)
            .with_metadata_map(app.metadata);
        self.registration = Some(client.register(&service, instance).await?);
        // 远端确认后保存本次提交的身份供回查；地址来源校验并不证明网络实际可达。
        self.registered_identity = Some((service.clone(), ip, port));
        tracing::info!(service = %service, "rest-discovery-nacos: 本实例已注册");
        Ok(())
    }

    /// 业务作用：消费本地注册句柄并等待远端摘流，不关闭出站运行时。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取走句柄后等待远端注销，确认成功才清空回查身份；失败或取消时本地句柄已失去但身份可能保留，Drop 尽力接力。无句柄时直接成功，不证明远端已删除。
    pub async fn deregister(&mut self) -> anyhow::Result<()> {
        if let Some(registration) = self.registration.take() {
            registration.deregister().await?;
            self.registered_identity = None;
            tracing::info!("rest-discovery-nacos: 本实例已摘流");
        }
        Ok(())
    }

    /// 业务作用：根据保存的注册身份回查该地址是否仍在注册中心的流量候选集合中。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无保存身份时返回 None；否则查询可承载流量实例，以 IP 和端口匹配返回 Some(bool)。缺少提供方或查询失败返回错误；不检查本地句柄存在，不复验集群或业务健康。
    pub async fn self_registration_healthy(&self) -> anyhow::Result<Option<bool>> {
        let Some((service, ip, port)) = self.registered_identity.as_ref() else {
            return Ok(None);
        };
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("rest-discovery-nacos: provider 未连接,无法回查本实例注册状态")
        })?;
        let instances = client.discover(service).await?;
        let present = instances
            .iter()
            .any(|instance| instance.port == *port && instance.ip == *ip);
        Ok(Some(present))
    }

    /// 业务作用：读取某服务当前可承载流量的实例及其 provider 元数据，供上层 typed resolver 选择协议端点。
    ///
    /// 参数说明:
    /// - `service`: 注册中心内的服务名。
    ///
    /// 返回：provider 已连接时返回经过统一健康过滤的实例；连接缺失或查询失败时返回错误。
    pub async fn discover_instances(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("rest-discovery-nacos: provider 未连接,无法查询服务实例")
        })?;
        client.discover(service).await
    }

    /// 业务作用：关闭出站客户端运行时(幂等)。 必须最后调用:在途请求、用户任务和业务资源清理都可能仍在调下游。只有当全局槽仍指向本会话 安装的运行时才会取下,避免误关后来安装的实例。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以当前时刻加五秒为截止点执行关闭，排干成功后释放会话依赖；失败保留运行时以便再次等待，入口已经可能撤下，不注销实例。
    pub async fn shutdown_runtime(&mut self) -> anyhow::Result<()> {
        self.shutdown_runtime_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    }

    /// 业务作用：撤下本会话出站入口，并等待当前代 REST 任务和调用退出。
    ///
    /// 参数说明：
    /// - `deadline`：为宿主统一停机截止点。
    ///
    /// 返回：排干后释放会话依赖；未排干保留 owner 以便再次等待，不影响后续实例。
    pub async fn shutdown_runtime_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<()> {
        if let Some(runtime) = &self.runtime {
            RestDiscovery::shutdown_if_current(runtime);
            runtime.rest().shutdown(deadline).await?;
        }
        self.runtime = None;
        self.client = None;
        Ok(())
    }
}

/// 业务作用：只连接 provider 并安装出站 RestDiscovery runtime,不注册本实例。 `enabled=false` 时安装 external-only 客户端:普通 http(s) 可用,`lb://` 得到明确错误而不是退化成 DNS。
///
/// 参数说明：
/// - `cfg`：discovery/nacos/rest 配置源;注册相关字段留到 [`DiscoverySession::register`] 才校验。
///
/// 返回：成功时发布全局出站运行时并返回尚未注册的会话；总开关关闭时只支持外部 URL。配置、连接或运行时初始化失败返回错误，不在本入口校验实例注册地址。
pub async fn prepare_from_config(cfg: &DiscoveryConfig) -> anyhow::Result<DiscoverySession> {
    prepare_from_config_with_span_recorder(cfg, None).await
}

/// 业务作用：准备提供方与全局出站客户端，并可接入应用提供的 span 写入器。
///
/// 参数说明：
/// - `cfg`：发现、Nacos 与 REST 配置；注册参数延迟到 register 检查。
/// - `span_recorder`：可选的共享 span 写入器，不提供关闭或排干权。
///
/// 返回：成功时发布全局出站运行时并返回尚未注册的会话；总开关关闭时只支持外部 URL。配置、连接或运行时初始化失败返回错误，不在本入口校验实例注册地址。
pub async fn prepare_from_config_with_span_recorder(
    cfg: &DiscoveryConfig,
    span_recorder: Option<SpanRecorder>,
) -> anyhow::Result<DiscoverySession> {
    let mut rest_opts = map_rest_options(&cfg.rest)?;
    if let Some(recorder) = span_recorder {
        rest_opts = rest_opts.with_span_recorder(recorder);
    }
    if !cfg.enabled {
        RestDiscovery::init_external_only(rest_opts).await?;
        tracing::info!("rest-discovery-nacos: discovery disabled → external-only");
        return Ok(DiscoverySession {
            config: cfg.clone(),
            client: None,
            runtime: Some(RestDiscovery::runtime()?),
            registration: None,
            registered_identity: None,
        });
    }

    match cfg.provider {
        ProviderKind::Nacos => {
            let props = build_props(&cfg.nacos);
            let client = Arc::new(NacosDiscoveryClient::connect(&props).await?);
            RestDiscovery::init_with_discovery(client.clone(), rest_opts).await?;
            tracing::info!(
                "rest-discovery-nacos: 出站 RestDiscovery runtime 已装配(尚未注册本实例)"
            );
            Ok(DiscoverySession {
                config: cfg.clone(),
                client: Some(client),
                runtime: Some(RestDiscovery::runtime()?),
                registration: None,
                registered_identity: None,
            })
        }
    }
}

// ── 入口 ──

/// 业务作用：按 [`DiscoveryConfig`] 一键装配 RestDiscovery(+ 可选注册本实例)。返回的 [`DiscoveryHandle`] 须由 main 持有。
///
/// 参数说明：
/// - `cfg`：发现、注册及出站客户端配置。
/// - `app`：补齐服务名与端口的应用信息；本入口不随实例发布 app.metadata。
///
/// 返回：成功时发布全局出站运行时并返回需持有的注册生命周期句柄；关闭总开关时只安装外部 URL 模式。配置或连接失败返回错误；注册后运行时初始化失败时限时尝试注销再返回原错误，不能保证注销成功。
pub async fn init_from_config(
    cfg: &DiscoveryConfig,
    app: AppRegistrationInfo,
) -> anyhow::Result<DiscoveryHandle> {
    init_from_config_impl(cfg, app, None).await
}

/// 业务作用：同 [`init_from_config`],但**注入自定义 [`LoadBalancer`]**:`cfg.rest.lb_strategy` 仍解析校验但被忽略, `enabled=true` 时三类内部调用共享传入算法。`enabled=false`(external-only)不选址,传入算法被忽略(会 warn)。
///
/// 参数说明：
/// - `cfg`：发现、注册及出站客户端配置，lb_strategy 仍解析。
/// - `app`：补齐服务名与端口的应用信息；app.ip 和 metadata 不参与登记。
/// - `load_balancer`：发现启用时使用的共享选址器；外部 URL 模式下忽略。
///
/// 返回：成功时发布全局出站运行时并返回需持有的注册生命周期句柄；关闭总开关时只安装外部 URL 模式。配置或连接失败返回错误；注册后运行时初始化失败时限时尝试注销再返回原错误，不能保证注销成功。
pub async fn init_from_config_with_load_balancer(
    cfg: &DiscoveryConfig,
    app: AppRegistrationInfo,
    load_balancer: Arc<dyn LoadBalancer>,
) -> anyhow::Result<DiscoveryHandle> {
    init_from_config_impl(cfg, app, Some(load_balancer)).await
}

/// 业务作用：按配置初始化发现客户端；用于串联本地配置、注册中心和负载均衡。
///
/// 参数说明：
/// - `cfg`：发现、注册及出站客户端配置。
/// - `app`：服务名与端口的应用缺省值。
/// - `load_balancer`：覆盖配置选址策略的可选共享实现。
///
/// 返回：成功时发布全局出站运行时并返回需持有的注册生命周期句柄；关闭总开关时只安装外部 URL 模式。配置或连接失败返回错误；注册后运行时初始化失败时限时尝试注销再返回原错误，不能保证注销成功。
async fn init_from_config_impl(
    cfg: &DiscoveryConfig,
    app: AppRegistrationInfo,
    load_balancer: Option<Arc<dyn LoadBalancer>>,
) -> anyhow::Result<DiscoveryHandle> {
    // 先把配置映射成运行时 options,让枚举拼写、超时、backoff 等错误在连接注册中心前暴露。
    // 即便后面是 external-only,HTTP client 仍会使用其中的 http 配置。
    let rest_opts = map_rest_options(&cfg.rest)?;

    if !cfg.enabled {
        // discovery 关闭时不连接 provider、不注册实例、不启动 watch/index。此时只有普通 HTTP 可用,
        // 显式内部入口会由 RestDiscoveryClient 返回 typed error,避免把服务名当 DNS host 打出去。
        if load_balancer.is_some() {
            tracing::warn!(
                "rest-discovery-nacos: discovery disabled → external-only;传入的自定义 LoadBalancer 被忽略(external-only 不选址)"
            );
        }
        RestDiscovery::init_external_only(rest_opts).await?;
        tracing::info!("rest-discovery-nacos: discovery disabled → external-only");
        return Ok(DiscoveryHandle::external_only());
    }

    match cfg.provider {
        ProviderKind::Nacos => {
            // 先解析 + 校验最终注册参数(fail-fast,在连 Nacos【之前】)，避免无效配置浪费连接。
            // 只在 registration.enabled=true 时读取/校验注册 IP:纯消费者不应因注册 IP 缺失失败。
            let reg_params = if cfg.registration.enabled {
                Some(resolve_registration(
                    &cfg.registration,
                    &app,
                    cfg.nacos.discovery_ip.as_deref(),
                )?)
            } else {
                None
            };

            // provider 连接只在 enabled=true 后发生;RestDiscovery 本体只依赖 DiscoveryClient trait,
            // 具体后端的连接、认证、group/namespace 组装都收敛在本便利层。
            let props = build_props(&cfg.nacos);
            let dc = Arc::new(NacosDiscoveryClient::connect(&props).await?);

            // 先注册本实例,再发布 RestDiscovery runtime。若后续 runtime 初始化失败,下面会主动摘除该实例,
            // 避免注册中心里留下一个无法处理内部调用的进程记录。
            let mut registration = if let Some((service, ip, port)) = reg_params {
                let inst = Instance::new(ip, port)
                    .with_ephemeral(cfg.registration.ephemeral)
                    .with_healthy(cfg.registration.healthy)
                    .with_weight(cfg.registration.weight);
                let reg = dc.register(&service, inst).await?;
                tracing::info!(service = %service, "rest-discovery-nacos: 本实例已注册");
                Some(reg)
            } else {
                tracing::info!(
                    "rest-discovery-nacos: registration.enabled=false → 仅作消费者,不注册本实例"
                );
                None
            };

            // init_with_discovery 内部按 heuristic_http 决定是否首拉 list_services + 启动索引(走 connect)。
            // 传入自定义 LoadBalancer 时改走 init_with_discovery_and_load_balancer(忽略 cfg.rest.lb_strategy)。
            // 失败时必须显式回滚注册：已注册的实例先 deregister 摘流再返回错误，不能依赖 Drop 正式下线。
            // 否则会留下一个「已注册但 RestDiscovery 未装配」的僵尸实例,只能等 Drop best-effort。
            let init_result = match load_balancer {
                Some(lb) => {
                    RestDiscovery::init_with_discovery_and_load_balancer(dc.clone(), rest_opts, lb)
                        .await
                }
                None => RestDiscovery::init_with_discovery(dc.clone(), rest_opts).await,
            };
            if let Err(e) = init_result {
                if let Some(reg) = registration.take() {
                    match tokio::time::timeout(Duration::from_secs(3), reg.deregister()).await {
                        Ok(Ok(())) => tracing::warn!(
                            "rest-discovery-nacos: RestDiscovery 初始化失败,已回滚 deregister 注册实例"
                        ),
                        Ok(Err(de)) => tracing::warn!(
                            "rest-discovery-nacos: 初始化失败 + 回滚 deregister 也失败: {de}"
                        ),
                        Err(_) => tracing::warn!(
                            "rest-discovery-nacos: 初始化失败 + 回滚 deregister 超时(3s)"
                        ),
                    }
                }
                return Err(e.into());
            }
            tracing::info!("rest-discovery-nacos: RestDiscovery 已装配(discovery enabled)");
            Ok(DiscoveryHandle {
                _client: Some(dc),
                registration,
            })
        }
    }
}

/// 业务作用：转换连接配置并保留本层注册地址优先级。
///
/// 参数说明：
/// - `nacos`：提供方连接参数与凭据。
///
/// 返回：返回经构造器去除非敏感字段首尾空白的 Nacos 配置，空分组保留 DEFAULT_GROUP。故意不传 discovery_ip，避免覆盖已经选定的更高优先级地址；不连接或完整校验。
fn build_props(nacos: &NacosConnConfig) -> NacosProps {
    // 刻意不把 nacos.discovery_ip 写进 NacosProps.discovery_ip:它是注册 IP 的第三优先级,
    // 进了 props 底层 register 会拿它覆盖 LOCAL_NETWORK_IP / registration.ip 前两级。
    let mut p = NacosProps::new(&nacos.server_addr)
        .with_namespace(&nacos.namespace)
        .with_app_name(&nacos.app_name)
        .with_auth(&nacos.username, &nacos.password);
    // group 空 → 保留 NacosProps 默认 "DEFAULT_GROUP"。
    if !nacos.group.trim().is_empty() {
        p = p.with_group(&nacos.group);
    }
    p
}

/// 业务作用：`a` trim 后非空 → 用 `a`;否则用 `b`。
///
/// 参数说明：
/// - `a`：优先使用的配置值。
/// - `b`：优先值为空时使用的应用缺省值。
///
/// 返回：a 去除首尾空白后非空则返回该值，否则复制 b 且不去空白。
fn first_nonempty(a: &str, b: &str) -> String {
    let a = a.trim();
    if a.is_empty() {
        b.to_string()
    } else {
        a.to_string()
    }
}

/// 业务作用：`s` trim 后非空 → Some(owned);空白/None → None。
///
/// 参数说明：
/// - `s`：要解析的输入字符串。
///
/// 返回：Some 去除首尾空白后非空则返回新字符串；None 或空白返回 None。
fn non_blank_owned(s: Option<&str>) -> Option<String> {
    s.map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// 业务作用：读取优先级最高的进程注册地址环境配置。
///
/// 参数说明: 无。
///
/// 返回：先读取 LOCAL_NETWORK_IP，未提供非空白 Unicode 值时尝试 LOCAL__NETWORK__IP；均未命中返回 None，不验证 IP 格式或可达性。
fn local_network_ip_from_env() -> Option<String> {
    std::env::var("LOCAL_NETWORK_IP")
        .ok()
        .and_then(|v| non_blank_owned(Some(v.as_str())))
        .or_else(|| {
            std::env::var("LOCAL__NETWORK__IP")
                .ok()
                .and_then(|v| non_blank_owned(Some(v.as_str())))
        })
}

/// 业务作用：解析最终注册参数。注册 IP 优先级 `LOCAL_NETWORK_IP` env → `rest_discovery.registration.ip` → `nacos.discovery_ip` → fail-fast; `AppRegistrationInfo.ip` 不参与(监听 host 兜底会让 `0.0.0.0`/`::` 绕过规则)。
///
/// 参数说明：
/// - `cfg`：实例注册配置及流量属性。
/// - `app`：服务名与端口的缺省来源，监听 IP 不参与回退。
/// - `nacos_discovery_ip`：环境和注册配置均未提供地址时的最后回退。
///
/// 返回：成功返回服务名、地址与非零端口；环境地址优先于注册配置，再回退 Nacos 地址。未解析占位符、缺失地址、两个指定的未指定地址字面量或非法权重被拒绝；不完整解析 IP 或证明可达性。
fn resolve_registration(
    cfg: &RegistrationConfig,
    app: &AppRegistrationInfo,
    nacos_discovery_ip: Option<&str>,
) -> anyhow::Result<(String, String, u16)> {
    let service = first_nonempty(&cfg.service_name, &app.service_name);
    let port = if cfg.port != 0 {
        cfg.port
    } else {
        app.port
    };

    anyhow::ensure!(
        !service.trim().is_empty(),
        "rest_discovery.registration.service_name 不能为空"
    );
    anyhow::ensure!(port != 0, "rest_discovery.registration.port 不能为 0");

    // LOCAL_NETWORK_IP 是真正的最高优先级:命中即胜出,不再看 cfg.ip(
    // 占位符校验放它之后——直接构造配置调 init_from_config 时 env 也必须能压过一切)。
    // env 缺席时,cfg.ip 若仍是未解析占位符(加载期对应环境变量缺失)= 显式配置错误:
    // fail-fast,不 fallback 到 nacos.discovery_ip(静默换 IP 更难排查),更不能把字面量注册出去。
    let cfg_ip = cfg.ip.trim();
    let ip = match local_network_ip_from_env() {
        Some(ip) => ip,
        None => {
            anyhow::ensure!(
                !cfg_ip.contains("${"),
                "rest_discovery.registration.ip 是未解析占位符 {cfg_ip:?};请设置对应环境变量(如 LOCAL_NETWORK_IP)或改成真实 IP"
            );
            non_blank_owned(Some(cfg_ip))
                .or_else(|| non_blank_owned(nacos_discovery_ip))
                .ok_or_else(|| anyhow::anyhow!("rest_discovery.registration.ip 不能为空"))?
        }
    };

    anyhow::ensure!(
        ip != "0.0.0.0" && ip != "::",
        "rest_discovery.registration.ip 不能是监听地址 {ip};请配置真实可路由 IP"
    );
    anyhow::ensure!(
        cfg.weight.is_finite() && cfg.weight >= 0.0,
        "rest_discovery.registration.weight 必须非负且有限"
    );

    Ok((service, ip, port))
}

// ── RestConfig → RestDiscoveryOptions(字符串枚举解析 + 数值校验) ──

/// 业务作用：将外部 REST 配置映射为客户端选项，在连接前暴露可识别的配置错误。
///
/// 参数说明：
/// - `rest`：RestDiscovery 运行时配置。
///
/// 返回：返回枚举已解析并通过各局部映射检查的选项；遇到非法值返回错误。不执行完整运行时 validate，重试预算和部分时长边界由初始化入口继续检查。
fn map_rest_options(rest: &RestConfig) -> anyhow::Result<RestDiscoveryOptions> {
    let mut o = RestDiscoveryOptions::new();
    if rest.heuristic_http {
        o = o.with_heuristic_http(HeuristicHttpMode::Enabled);
    }
    o = o.with_service_match(parse_service_match(&rest.service_match)?);
    o = o.with_scheme_policy(parse_scheme_policy(&rest.scheme_policy)?);
    o = o.with_default_instance_scheme(parse_instance_scheme(&rest.default_instance_scheme)?);
    o = o.with_lb_strategy(parse_lb_strategy(&rest.lb_strategy)?);
    o = o.with_startup(parse_startup(&rest.startup)?);
    o = o.with_unknown_host(parse_unknown_host(&rest.unknown_host)?);
    o = o.with_no_instance(parse_no_instance(&rest.no_instance)?);
    o = o.with_preserve_original_host_header(rest.preserve_original_host_header);
    o = o.with_heuristic_options(map_heuristic(&rest.heuristic)?);
    o = o.with_watch(map_watch(&rest.watch)?);
    o = o.with_retry(map_retry(&rest.retry));
    o = o.with_http(map_http(&rest.http)?);
    Ok(o)
}

/// 业务作用：解析外部 URL 转发到实例时的协议策略。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值或 preserve 返回 Preserve；force_http、force_https 返回对应策略；其它值返回配置错误。
fn parse_scheme_policy(s: &str) -> anyhow::Result<SchemePolicy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "preserve" => Ok(SchemePolicy::Preserve),
        "force_http" => Ok(SchemePolicy::ForceHttp),
        "force_https" => Ok(SchemePolicy::ForceHttps),
        other => anyhow::bail!(
            "rest_discovery.rest.scheme_policy 非法值 {other:?}(只支持 preserve / force_http / force_https)"
        ),
    }
}

/// 业务作用：解析显式内部服务调用的默认实例协议。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值或 http 返回 Http，https 返回 Https；其它值返回配置错误。
fn parse_instance_scheme(s: &str) -> anyhow::Result<InstanceScheme> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "http" => Ok(InstanceScheme::Http),
        "https" => Ok(InstanceScheme::Https),
        other => {
            anyhow::bail!(
                "rest_discovery.rest.default_instance_scheme 非法值 {other:?}(只支持 http / https)"
            )
        }
    }
}

/// 业务作用：解析服务名启发式索引的启动策略。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值、require_initial_service_list_when_heuristic_enabled 或 require_initial 要求初始列表，allow_empty_service_index 或 allow_empty 允许空索引；其它值返回配置错误。
fn parse_startup(s: &str) -> anyhow::Result<StartupPolicy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "require_initial_service_list_when_heuristic_enabled" | "require_initial" => {
            Ok(StartupPolicy::RequireInitialServiceListWhenHeuristicEnabled)
        }
        "allow_empty_service_index" | "allow_empty" => {
            Ok(StartupPolicy::AllowEmptyServiceIndex)
        }
        other => anyhow::bail!(
            "rest_discovery.rest.startup 非法值 {other:?}(只支持 require_initial_service_list_when_heuristic_enabled / allow_empty_service_index)"
        ),
    }
}

/// 业务作用：转换启发式服务名索引的刷新周期和移除宽限。
///
/// 参数说明：
/// - `heuristic`：yml 中 `rest_discovery.rest.heuristic` 的刷新和移除宽限配置。
///
/// 返回：返回保留缺省并应用显式值的选项；显式刷新周期或移除宽限为零时失败，较大时长留给运行时完整校验。
fn map_heuristic(heuristic: &HeuristicConfig) -> anyhow::Result<RestHeuristicOptions> {
    let mut o = RestHeuristicOptions::new();
    if let Some(ms) = heuristic.refresh_interval_ms {
        anyhow::ensure!(
            ms > 0,
            "rest_discovery.rest.heuristic.refresh_interval_ms 必须 > 0"
        );
        o = o.with_refresh_interval(Duration::from_millis(ms));
    }
    if let Some(ms) = heuristic.removed_service_grace_ms {
        anyhow::ensure!(
            ms > 0,
            "rest_discovery.rest.heuristic.removed_service_grace_ms 必须 > 0"
        );
        o = o.with_removed_service_grace(Duration::from_millis(ms));
    }
    Ok(o)
}

/// 业务作用：解析服务名与 URL 主机名的大小写匹配规则。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值或 case_insensitive 返回忽略大小写模式，case_sensitive 返回精确模式；其它值返回配置错误。
fn parse_service_match(s: &str) -> anyhow::Result<ServiceMatchMode> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "case_insensitive" => Ok(ServiceMatchMode::CaseInsensitive),
        "case_sensitive" => Ok(ServiceMatchMode::CaseSensitive),
        other => anyhow::bail!(
            "rest_discovery.rest.service_match 非法值 {other:?}(只支持 case_insensitive / case_sensitive)"
        ),
    }
}

/// 业务作用：解析客户端负载均衡算法选择。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值或 round_robin 返回轮询，weighted 返回加权模式；其它值返回配置错误。
fn parse_lb_strategy(s: &str) -> anyhow::Result<LbStrategy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "round_robin" => Ok(LbStrategy::RoundRobin),
        "weighted" => Ok(LbStrategy::Weighted),
        other => anyhow::bail!(
            "rest_discovery.rest.lb_strategy 非法值 {other:?}(只支持 round_robin / weighted)"
        ),
    }
}

/// 业务作用：解析启发式索引未识别主机时的处理策略。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值或 external_http 允许外部直连，error 返回拒绝策略；其它值返回配置错误。
fn parse_unknown_host(s: &str) -> anyhow::Result<UnknownHostPolicy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "external_http" => Ok(UnknownHostPolicy::ExternalHttp),
        "error" => Ok(UnknownHostPolicy::Error),
        other => anyhow::bail!(
            "rest_discovery.rest.unknown_host 非法值 {other:?}(只支持 external_http / error)"
        ),
    }
}

/// 业务作用：解析服务无流量实例时的处理策略。
///
/// 参数说明：
/// - `s`：来自配置的策略名称。
///
/// 返回：先去除首尾空白并转为 ASCII 小写；空值或 error 返回报错策略，stale_if_discovery_error 返回允许发现失败时使用旧值的策略；其它值返回配置错误。
fn parse_no_instance(s: &str) -> anyhow::Result<NoInstancePolicy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "error" => Ok(NoInstancePolicy::Error),
        "stale_if_discovery_error" => Ok(NoInstancePolicy::StaleIfDiscoveryError),
        other => anyhow::bail!(
            "rest_discovery.rest.no_instance 非法值 {other:?}(只支持 error / stale_if_discovery_error)"
        ),
    }
}

/// 业务作用：转换实例监听的容量、轮询、缓存与重建退避配置。
///
/// 参数说明：
/// - `watch`：yml 中 `rest_discovery.rest.watch` 的轮询和退避配置。
///
/// 返回：返回覆盖显式字段后的选项；订阅容量越界、轮询为零、重建退避为零或最小值大于最大值时失败。TTL 与旧值容忍可为零，完整时长范围由运行时继续校验。
fn map_watch(watch: &WatchConfig) -> anyhow::Result<RestWatchOptions> {
    let mut o = RestWatchOptions::new();
    if let Some(max_services) = watch.max_services {
        anyhow::ensure!(
            (1..=65536).contains(&max_services),
            "rest_discovery.rest.watch.max_services must be between 1 and 65536"
        );
        o.max_services = max_services;
    }
    if let Some(ms) = watch.poll_interval_ms {
        anyhow::ensure!(
            ms > 0,
            "rest_discovery.rest.watch.poll_interval_ms 必须 > 0(0 会让轮询 panic)"
        );
        o = o.with_poll_interval(Duration::from_millis(ms));
    }
    if let Some(ms) = watch.ttl_fallback_ms {
        o = o.with_ttl_fallback(Duration::from_millis(ms));
    }
    if let Some(ms) = watch.stale_if_error_ms {
        o = o.with_stale_if_error(Duration::from_millis(ms));
    }
    if watch.restart_backoff_min_ms.is_some() || watch.restart_backoff_max_ms.is_some() {
        let min = watch
            .restart_backoff_min_ms
            .map(Duration::from_millis)
            .unwrap_or(o.restart_backoff_min);
        let max = watch
            .restart_backoff_max_ms
            .map(Duration::from_millis)
            .unwrap_or(o.restart_backoff_max);
        // fail-fast:min=0 会让 watch 重建后 `sleep(0)`,且 `backoff=(backoff*2).min(max)` 从 0 起恒为 0 →
        // watch 持续失败时高频空转打爆 discovery provider;max=0 同样把退避压成 0;min>max 语义反直觉。
        anyhow::ensure!(
            !min.is_zero(),
            "rest_discovery.rest.watch.restart_backoff_min_ms 必须 > 0(0 会让 watch 失败后高频空转)"
        );
        anyhow::ensure!(
            !max.is_zero(),
            "rest_discovery.rest.watch.restart_backoff_max_ms 必须 > 0"
        );
        anyhow::ensure!(
            min <= max,
            "rest_discovery.rest.watch.restart_backoff_min_ms({min:?})不能大于 restart_backoff_max_ms({max:?})"
        );
        o = o.with_restart_backoff(min, max);
    }
    Ok(o)
}

/// 业务作用：转换幂等请求的重试触发条件及每服务预算。
///
/// 参数说明：
/// - `retry`：yml 中 `rest_discovery.rest.retry` 的幂等重试策略。
///
/// 返回：状态重试开关优先，启用时也允许网络错误重试；否则按网络开关选择，仅在启用时使用尝试数和预算配置。都关闭时返回禁用选项，预算合法性由运行时校验。
fn map_retry(retry: &RetryConfig) -> RetryOptions {
    if retry.get_head_on_retryable_status {
        RetryOptions::get_head_on_transient_failure(
            retry.max_attempts.unwrap_or(2),
            retry.budget_capacity.unwrap_or(20),
            retry.budget_refill_per_second.unwrap_or(10.0),
        )
    } else if retry.get_head_on_transport_error {
        let mut options =
            RetryOptions::get_head_on_transport_error(retry.max_attempts.unwrap_or(2));
        options.budget_capacity = retry.budget_capacity.unwrap_or(20);
        options.budget_refill_per_second = retry.budget_refill_per_second.unwrap_or(10.0);
        options
    } else {
        RetryOptions::new()
    }
}

/// 业务作用：转换出站请求整体超时与连接超时。
///
/// 参数说明：
/// - `http`：yml 中 `rest_discovery.rest.http` 的请求总超时和连接超时配置。
///
/// 返回：返回应用显式超时后的选项；显式请求或连接超时为零时失败，上界由运行时继续校验。
fn map_http(http: &HttpConfig) -> anyhow::Result<RestHttpOptions> {
    let mut o = RestHttpOptions::new();
    if let Some(ms) = http.timeout_ms {
        anyhow::ensure!(
            ms > 0,
            "rest_discovery.rest.http.timeout_ms 必须 > 0(0 会立即超时)"
        );
        o = o.with_timeout(Duration::from_millis(ms));
    }
    if let Some(ms) = http.connect_timeout_ms {
        anyhow::ensure!(
            ms > 0,
            "rest_discovery.rest.http.connect_timeout_ms 必须 > 0"
        );
        o = o.with_connect_timeout(Duration::from_millis(ms));
    }
    Ok(o)
}
