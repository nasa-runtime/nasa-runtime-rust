use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

macro_rules! config {
    ($(#[$meta:meta])* $name:ident { $($(#[$field_meta:meta])* $field:ident : $ty:ty = $value:expr),* $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
        #[serde(default, deny_unknown_fields)]
        pub struct $name { $($(#[$field_meta])* pub $field: $ty),* }
        impl Default for $name {
            /// 业务作用：为缺省及空配置子树提供一致的递归默认值。
            /// 参数说明：无。
            /// 返回：不创建资源、不解析凭据的配置值。
            fn default() -> Self { Self { $($field: $value),* } }
        }
    };
}

macro_rules! choices {
    ($(#[$meta:meta])* $name:ident, $(#[$first_meta:meta])* $first:ident $(,$(#[$other_meta:meta])* $other:ident)* $(,)?) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $(#[$first_meta])* #[default] $first, $($(#[$other_meta])* $other),* }
    };
}
choices!(
    /// 指标对外传输方式；出口只有显式启用后才建立。
    ExportMode,
    /// 由 Prometheus 拉取指标。
    Scrape,
    /// 按配置周期主动推送样本。
    RemoteWrite,
    /// 同时开放抓取与主动推送，查询端须避免重复采集。
    Both,
);
choices!(
    /// Prometheus 抓取入口的 listener 归属。
    ListenerMode,
    /// 使用出口独占的监听端口。
    Dedicated,
    /// 复用受管 Web listener 的指标路由。
    Web,
);
choices!(
    /// 指标抓取或推送使用的认证策略。
    AuthMode,
    /// 不添加 Bearer 认证，访问边界由部署网络承担。
    None,
    /// 从 secret 引用取得 Bearer token。
    Bearer,
);
choices!(
    /// 观测平台资源的装配职责。
    ProvisioningMode,
    /// 不创建或调和平台资源。
    Disabled,
    /// 由独立 controller 在平台授权范围内调和资源。
    Platform,
);
choices!(
    /// controller 为指标采集选择的主发现路径。
    Discovery,
    /// 推送专用出口使用 remote write，其余按 Kubernetes binding 或 Docker DNS 选择。
    Auto,
    /// 通过 PodMonitor 发现各 Pod 的指标端口。
    KubernetesPod,
    /// 通过逐容器 DNS 地址配置 Prometheus 抓取。
    DockerDns,
    /// 复用平台已有的抓取配置。
    Existing,
    /// 使用主动推送路径，不创建抓取发现资源。
    RemoteWrite,
);
choices!(
    /// Prometheus 访问抓取目标的协议。
    Scheme,
    /// 使用明文 HTTP，由部署网络提供隔离。
    Http,
    /// 使用 HTTPS，目标须具备相应 TLS 终止能力。
    Https,
);
choices!(
    /// Grafana datasource 的管理归属。
    DatasourceMode,
    /// controller 创建并调和自身拥有的 datasource。
    Managed,
    /// 按 UID 引用平台已有 datasource，不接管其配置。
    Existing,
);

config!(
    /// 统一观测出口与独立平台 controller 的配置根，禁用时不建立出口。
    ObservabilityConfig {
    /// 是否建立统一观测出口；关闭时不创建出口资源。
    enabled: bool = false,
    /// 跨出口一致的服务与实例身份。
    identity: IdentityConfig = IdentityConfig::default(),
    /// Prometheus 传输与采集策略。
    prometheus: PrometheusConfig = PrometheusConfig::default(),
    /// 独立控制面负责的平台资源装配计划。
    provisioning: ProvisioningConfig = ProvisioningConfig::default(),
});
config!(
    /// 各出口共用的服务与部署身份，启动时校验并固定指标标签。
    IdentityConfig {
    /// 稳定服务名；为空时按宿主提供的应用名称解析。
    service_name: String = String::new(),
    /// 部署环境身份，用于区分指标和平台资源。
    environment: String = String::new(),
    /// 部署集群身份，防止不同集群的同名服务混合。
    cluster: String = String::new(),
    /// 可选区域标签。
    region: Option<String> = None,
    /// 可选可用区标签。
    zone: Option<String> = None,
    /// 稳定实例身份；只有 local 环境允许使用宿主提供的本地缺省值。
    instance_id: String = String::new(),
    /// 可选业务制品版本标签。
    service_version: Option<String> = None,
});
config!(
    /// 抓取与 remote write 的传输选择及各自资源边界。
    PrometheusConfig {
    /// 选择抓取、主动推送或同时使用两者。
    export_mode: ExportMode = ExportMode::Scrape,
    /// 被动抓取的监听、认证和请求预算。
    scrape: ScrapeConfig = ScrapeConfig::default(),
    /// 主动推送的目标、队列和排干预算。
    remote_write: RemoteWriteConfig = RemoteWriteConfig::default(),
});
config!(
    /// 认证方式与 secret 引用，不保存凭据明文。
    AuthConfig {
    /// 该出口是否要求 Bearer token。
    mode: AuthMode = AuthMode::None,
    /// Bearer token 的 secret:// 引用，实际材料由消费方解析。
    token: Option<String> = None,
});
config!(
    /// Prometheus 抓取路由、并发准入和单请求期限。
    ScrapeConfig {
    /// 选择独立端口或受管 Web 路由。
    listener: ListenerMode = ListenerMode::Dedicated,
    /// 显式监听地址；省略时由运行模式决定默认绑定范围。
    bind: Option<String> = None,
    /// 抓取路由的字面路径，不接受模板、查询参数或路径穿越。
    path: String = "/metrics".into(),
    /// 同时处理的抓取请求上限，超限拒绝准入。
    max_concurrent_requests: usize = 4,
    /// 单次网络请求的最长等待时间，单位毫秒。
    request_timeout_ms: u64 = 2000,
    /// 该出口的认证模式与凭据引用。
    auth: AuthConfig = AuthConfig::default(),
});
config!(
    /// 周期指标推送的有界队列、批量、重试和停机排干策略。
    RemoteWriteConfig {
    /// remote write 接收地址，启用推送时必须提供。
    endpoint: Option<String> = None,
    /// 该出口的认证模式与凭据引用。
    auth: AuthConfig = AuthConfig::default(),
    /// remote write 的采样和推送周期，单位毫秒。
    interval_ms: u64 = 10_000,
    /// 单次网络请求的最长等待时间，单位毫秒。
    request_timeout_ms: u64 = 3000,
    /// 等待 remote write 投递的批次数上限。
    queue_capacity: usize = 8,
    /// 每批允许的样本数，同时约束启动期静态容量校验。
    max_batch_samples: usize = 5000,
    /// 单批投递的最大尝试次数，包含首次发送。
    retry_max_attempts: usize = 1,
    /// 失败后再次尝试前的退避时间，单位毫秒。
    retry_backoff_ms: u64 = 500,
    /// 停机时继续投递队列的预算，单位毫秒。
    shutdown_drain_timeout_ms: u64 = 3000,
});
config!(
    /// 独立 controller 的调和周期、权威租约与平台资源计划。
    ProvisioningConfig {
    /// 关闭平台装配或交给独立 controller。
    mode: ProvisioningMode = ProvisioningMode::Disabled,
    /// 调和失败时结束 controller；关闭此项则记录失败并等待下一轮。
    required: bool = false,
    /// controller 调和平台资源的周期，单位毫秒。
    reconcile_interval_ms: u64 = 30_000,
    /// 单次网络请求的最长等待时间，单位毫秒。
    request_timeout_ms: u64 = 3000,
    /// 平台动作租约的目标有效期，必须覆盖调和与请求预算。
    lease_duration_ms: u64 = 120_000,
    /// 平台授权的连接、凭据来源与资源位置，不交给业务副本。
    bindings: super::platform::PlatformBindings = super::platform::PlatformBindings::default(),
    /// controller 为 Prometheus 生成的目标发现与抓取计划。
    prometheus: DiscoveryConfig = DiscoveryConfig::default(),
    /// Grafana datasource、面板和告警的装配计划。
    grafana: GrafanaConfig = GrafanaConfig::default(),
});
config!(
    /// Prometheus 对目标实例的发现方式、协议和采集周期。
    DiscoveryConfig {
    /// 指标采集的主发现方式。
    discovery: Discovery = Discovery::Auto,
    /// Prometheus 抓取任务名；空值使用按服务身份生成的名称。
    job_name: String = String::new(),
    /// Prometheus 连接抓取目标时使用的协议。
    scheme: Scheme = Scheme::Http,
    /// Prometheus 抓取目标的周期，单位毫秒。
    scrape_interval_ms: u64 = 15_000,
    /// Prometheus 等待一次抓取的期限，不能短于出口自身的请求预算。
    scrape_timeout_ms: u64 = 10_000,
});
config!(
    /// Grafana 管理端点、资源位置、面板与告警计划。
    GrafanaConfig {
    /// Grafana 管理 API 地址，可由平台 binding 补充。
    endpoint: Option<String> = None,
    /// Grafana API token 的 secret:// 引用，仅由 controller 解析。
    api_token: Option<String> = None,
    /// Grafana 组织 ID，限定资源操作的组织范围。
    organization_id: u32 = 1,
    /// Grafana 面板和告警的目录名称；空值使用服务范围名称。
    folder: String = String::new(),
    /// Grafana 查询 datasource 的装配或引用策略。
    datasource: DatasourceConfig = DatasourceConfig::default(),
    /// 选择要生成的观测面板。
    dashboards: DashboardConfig = DashboardConfig::default(),
    /// 告警集合、窗口与评估周期。
    alert_rules: AlertRulesConfig = AlertRulesConfig::default(),
});
config!(
    /// Grafana datasource 的归属、身份与 Prometheus 查询入口。
    DatasourceConfig {
    /// 管理自身 datasource 或只引用平台已有 UID。
    mode: DatasourceMode = DatasourceMode::Managed,
    /// 稳定 datasource UID；existing 模式要求显式指定。
    uid: String = String::new(),
    /// Grafana datasource 使用的 Prometheus 查询地址。
    prometheus_url: Option<String> = None,
});
config!(
    /// controller 创建的业务观测面板集合。
    DashboardConfig {
    /// 是否生成接口调用与隔离指标面板。
    interfaces: bool = true,
    /// 是否生成 Mapper 逻辑调用和数据库操作面板。
    mapper: bool = true,
    /// 是否生成连接池、连接等待与事务资源面板。
    datasource: bool = true,
    /// 是否生成通知排队和 provider 投递面板。
    notifications: bool = true,
});
config!(
    /// 业务失败率告警的聚合窗口、比例阈值与持续时间。
    RatioRule {
    /// 是否生成业务失败率告警。
    enabled: bool = true,
    /// 聚合观测窗口，单位毫秒，至少覆盖两个告警评估周期。
    window_ms: u64 = 300_000,
    /// 触发规则的比例阈值，范围为 0 到 1。
    threshold_ratio: f64 = 0.02,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 600_000,
});
config!(
    /// 成功调用 P99 延迟告警的聚合窗口和毫秒阈值。
    LatencyRule {
    /// 是否生成成功调用 P99 延迟告警。
    enabled: bool = true,
    /// 聚合观测窗口，单位毫秒，至少覆盖两个告警评估周期。
    window_ms: u64 = 300_000,
    /// 触发规则的延迟阈值，单位毫秒。
    threshold_ms: u64 = 1000,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 600_000,
});
config!(
    /// 连接池使用率告警的比例阈值与持续时间。
    SaturationRule {
    /// 是否生成连接池使用率告警。
    enabled: bool = true,
    /// 触发规则的比例阈值，范围为 0 到 1。
    threshold_ratio: f64 = 0.9,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 300_000,
});
config!(
    /// 连接获取超时告警的窗口内计数阈值。
    CountRule {
    /// 是否生成连接获取超时计数告警。
    enabled: bool = true,
    /// 聚合观测窗口，单位毫秒，至少覆盖两个告警评估周期。
    window_ms: u64 = 300_000,
    /// 窗口内触发规则所需的计数阈值。
    threshold_count: u64 = 1,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 0,
});
config!(
    /// 流消费取消比例告警，低于最小调用量时不按比例触发。
    StreamRule {
    /// 是否生成流消费取消比例告警。
    enabled: bool = true,
    /// 聚合观测窗口，单位毫秒，至少覆盖两个告警评估周期。
    window_ms: u64 = 300_000,
    /// 触发规则的比例阈值，范围为 0 到 1。
    threshold_ratio: f64 = 0.05,
    /// 计算取消比例前要求达到的窗口内调用量。
    minimum_calls: u64 = 20,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 300_000,
});
config!(
    /// 通知 provider 失败率告警，低于最小投递量时不按比例触发。
    ProviderRule {
    /// 是否生成通知 provider 失败率告警。
    enabled: bool = true,
    /// 聚合观测窗口，单位毫秒，至少覆盖两个告警评估周期。
    window_ms: u64 = 300_000,
    /// 触发规则的比例阈值，范围为 0 到 1。
    threshold_ratio: f64 = 0.10,
    /// 计算失败比例前要求达到的窗口内投递量。
    minimum_deliveries: u64 = 10,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 300_000,
});
config!(
    /// 实例失联告警，期望实例集合必须来自外部平台。
    PresenceRule {
    /// 是否生成基于外部期望实例集合的失联告警。
    enabled: bool = true,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 120_000,
    /// 平台提供的期望实例指标名，不接受 PromQL 或应用自报库存替代。
    expected_instances_metric: String = "platform_expected_instance_info".into(),
});
config!(
    /// 事务连接槽等待的 P99 延迟告警。
    SlotRule {
    /// 是否生成事务连接槽等待告警。
    enabled: bool = true,
    /// 聚合观测窗口，单位毫秒，至少覆盖两个告警评估周期。
    window_ms: u64 = 300_000,
    /// 触发规则的延迟阈值，单位毫秒。
    threshold_ms: u64 = 250,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 300_000,
});
config!(
    /// 通知队列占用告警的比例阈值与持续时间。
    QueueRule {
    /// 是否生成通知队列占用告警。
    enabled: bool = true,
    /// 触发规则的比例阈值，范围为 0 到 1。
    threshold_ratio: f64 = 0.8,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 300_000,
});
config!(
    /// 重复实例身份告警，保留异常实例供平台定位。
    CollisionRule {
    /// 是否生成重复实例身份告警。
    enabled: bool = true,
    /// 条件持续满足后才触发告警的等待时间，0 表示不追加持续时间。
    for_ms: u64 = 60_000,
});
config!(
    /// 服务与实例告警的集中评估策略，不创建通知联系渠道。
    AlertRulesConfig {
    /// 是否为该服务生成告警规则集合。
    enabled: bool = true,
    /// 平台评估全部规则的周期，单位毫秒。
    evaluation_interval_ms: u64 = 60_000,
    /// 平台既有通知 policy 的路由标签，不创建联系渠道。
    notification_policy_ref: Option<String> = None,
    /// 逻辑调用失败率规则。
    error_rate: RatioRule = RatioRule::default(),
    /// 成功调用 P99 延迟规则。
    p99: LatencyRule = LatencyRule::default(),
    /// 连接池使用率规则。
    pool_saturation: SaturationRule = SaturationRule::default(),
    /// 连接获取超时计数规则。
    acquire_timeout: CountRule = CountRule::default(),
    /// 事务连接槽等待延迟规则。
    transaction_slot_wait: SlotRule = SlotRule::default(),
    /// 流消费取消比例规则。
    stream_cancel_rate: StreamRule = StreamRule::default(),
    /// 通知队列占用率规则。
    notification_queue: QueueRule = QueueRule::default(),
    /// 通知 provider 投递失败比例规则。
    provider_failure_rate: ProviderRule = ProviderRule::default(),
    /// 按外部期望实例集合判断失联的规则。
    instance_down: PresenceRule = PresenceRule::default(),
    /// 检测同一部署范围内实例身份重复的规则。
    identity_collision: CollisionRule = CollisionRule::default(),
});

/// 已冻结的进程身份，所有出口共用同一组 label。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// 已校验的服务、部署环境、集群与实例标签；全部出口使用同一身份。
    pub labels: BTreeMap<&'static str, String>,
}

impl ObservabilityConfig {
    /// 业务作用：从同一应用 YAML 提取并严格校验观测配置，拒绝未知字段和显式空值。
    /// 参数说明：`root` 为已合并环境与配置源的应用配置。
    /// 返回：无网络副作用的递归默认配置；错误只给出固定配置路径。
    pub fn from_root(root: &Value) -> Result<Self, String> {
        if root.get("grafana").is_some_and(|v| !v.is_object()) {
            return Err("grafana must be an object".into());
        }
        let Some(value) = root.get("grafana").and_then(|v| v.get("observability")) else {
            return Ok(Self::default());
        };
        reject_null(value)?;
        let config: Self = serde_json::from_value(value.clone())
            .map_err(|_| "grafana.observability: invalid configuration structure".to_owned())?;
        config.validate()?;
        Ok(config)
    }

    /// 业务作用：为独立 controller 投影应用实际抓取地址，避免 Web 端口和路由前缀漂移。
    /// 参数说明：`root` 为与业务进程相同的 YAML；仅读取受管 server 的 port 与 context_path。
    /// 返回：dedicated 配置保持原样；Web 模式将服务端口和前缀冻结到发现配置，临时端口被拒绝。
    pub fn platform_projection(root: &Value) -> Result<Self, String> {
        let mut config = Self::from_root(root)?;
        if !config.enabled
            || config.provisioning.mode != ProvisioningMode::Platform
            || config.primary_discovery() == Discovery::RemoteWrite
            || !config.scrape_enabled()
            || config.prometheus.scrape.listener != ListenerMode::Web
        {
            return Ok(config);
        }
        let port = match root.pointer("/server/port") {
            Some(value) => value.as_u64().ok_or("server.port must be an integer")?,
            None => 8080,
        };
        if !(1..=65535).contains(&port) {
            return Err("platform Web discovery requires a stable server.port".into());
        }
        let prefix = match root.pointer("/server/context_path") {
            Some(value) => value
                .as_str()
                .ok_or("server.context_path must be a string")?,
            None => "",
        };
        if !prefix.is_empty() && (!prefix.starts_with('/') || prefix.ends_with('/')) {
            return Err("server.context_path is invalid".into());
        }
        config.prometheus.scrape.bind = Some(format!("0.0.0.0:{port}"));
        config.prometheus.scrape.path = format!("{prefix}{}", config.prometheus.scrape.path);
        config.validate()?;
        Ok(config)
    }

    /// 业务作用：在创建任何出口之前完成数值、路径与条件必填门禁。
    /// 参数说明：无。
    /// 返回：配置可供当前能力使用时成功；禁用能力不要求凭据或端点。
    pub fn validate(&self) -> Result<(), String> {
        let s = &self.prometheus.scrape;
        range(
            s.max_concurrent_requests as u64,
            1,
            32,
            "scrape.max_concurrent_requests",
        )?;
        range(
            s.request_timeout_ms,
            100,
            10_000,
            "scrape.request_timeout_ms",
        )?;
        if !s.path.starts_with('/')
            || s.path.len() > 128
            || s.path.contains("..")
            || s.path.contains(['?', '#', '{', '}', '*'])
            || s.path.chars().any(char::is_control)
        {
            return Err("scrape.path: invalid literal path".into());
        }
        if let Some(bind) = &s.bind {
            bind.parse::<std::net::SocketAddr>()
                .map_err(|_| "scrape.bind: invalid socket address")?;
        }
        let rw = &self.prometheus.remote_write;
        range(rw.interval_ms, 1000, 300_000, "remote_write.interval_ms")?;
        range(
            rw.request_timeout_ms,
            100,
            30_000,
            "remote_write.request_timeout_ms",
        )?;
        if rw.request_timeout_ms >= rw.interval_ms {
            return Err("remote_write.request_timeout_ms must be less than interval_ms".into());
        }
        range(
            rw.queue_capacity as u64,
            1,
            64,
            "remote_write.queue_capacity",
        )?;
        range(
            rw.max_batch_samples as u64,
            100,
            50_000,
            "remote_write.max_batch_samples",
        )?;
        range(
            rw.retry_max_attempts as u64,
            1,
            3,
            "remote_write.retry_max_attempts",
        )?;
        range(
            rw.retry_backoff_ms,
            100,
            5000,
            "remote_write.retry_backoff_ms",
        )?;
        range(
            rw.shutdown_drain_timeout_ms,
            0,
            30_000,
            "remote_write.shutdown_drain_timeout_ms",
        )?;
        let p = &self.provisioning;
        if !p.prometheus.job_name.is_empty() {
            identifier(
                &p.prometheus.job_name,
                128,
                "provisioning.prometheus.job_name",
            )?;
        }
        if !p.grafana.datasource.uid.is_empty() {
            identifier(
                &p.grafana.datasource.uid,
                128,
                "provisioning.grafana.datasource.uid",
            )?;
        }
        if p.grafana.folder.len() > 128 || p.grafana.folder.chars().any(char::is_control) {
            return Err("provisioning.grafana.folder is invalid".into());
        }
        if let Some(policy) = &p.grafana.alert_rules.notification_policy_ref {
            identifier(policy, 128, "alert_rules.notification_policy_ref")?;
        }
        range(
            p.reconcile_interval_ms,
            5000,
            300_000,
            "provisioning.reconcile_interval_ms",
        )?;
        range(
            p.request_timeout_ms,
            100,
            30_000,
            "provisioning.request_timeout_ms",
        )?;
        range(
            p.lease_duration_ms,
            p.reconcile_interval_ms
                .saturating_mul(3)
                .max(p.request_timeout_ms * 2),
            3_600_000,
            "provisioning.lease_duration_ms",
        )?;
        range(
            p.prometheus.scrape_interval_ms,
            1000,
            300_000,
            "provisioning.prometheus.scrape_interval_ms",
        )?;
        range(
            p.prometheus.scrape_timeout_ms,
            100,
            p.prometheus.scrape_interval_ms,
            "provisioning.prometheus.scrape_timeout_ms",
        )?;
        if p.prometheus.scrape_timeout_ms < s.request_timeout_ms {
            return Err("provisioning scrape timeout is shorter than exporter timeout".into());
        }
        range(
            p.grafana.organization_id as u64,
            1,
            i32::MAX as u64,
            "grafana.organization_id",
        )?;
        p.grafana.alert_rules.validate()?;
        if !self.enabled {
            return Ok(());
        }
        if p.mode == ProvisioningMode::Platform {
            // 平台发现必须匹配实际开启的出口，否则资源调和成功也无法形成指标数据路径。
            match (self.prometheus.export_mode, p.prometheus.discovery) {
                (ExportMode::Scrape, Discovery::RemoteWrite)
                | (
                    ExportMode::RemoteWrite,
                    Discovery::KubernetesPod | Discovery::DockerDns | Discovery::Existing,
                ) => {
                    return Err(
                        "prometheus export_mode and provisioning discovery are incompatible".into(),
                    );
                }
                _ => {}
            }
            if let Some(locator) = &p.grafana.api_token {
                secret_id(locator)?;
            }
        }
        if self.scrape_enabled() {
            s.auth.validate()?;
        }
        if self.remote_write_enabled() {
            rw.auth.validate()?;
            validate_url(
                rw.endpoint
                    .as_deref()
                    .ok_or("remote_write.endpoint is required")?,
                self.identity.environment == "local",
            )?;
        }
        Ok(())
    }

    /// 业务作用：冻结跨 exporter 的统一身份，防止同一进程被识别为不同服务。
    /// 参数说明：`application` 为应用名，`local_instance` 为本进程唯一标识，`root` 用于校验 telemetry 别名。
    /// 返回：启用时的完整身份；非本地缺失身份或别名冲突时拒绝启动。
    pub fn freeze_identity(
        &self,
        application: &str,
        local_instance: &str,
        root: &Value,
    ) -> Result<Identity, String> {
        let i = &self.identity;
        let service = if i.service_name.is_empty() {
            application
        } else {
            &i.service_name
        };
        identifier(service, 63, "identity.service_name")?;
        identifier(&i.environment, 32, "identity.environment")?;
        identifier(&i.cluster, 63, "identity.cluster")?;
        let instance = if i.instance_id.is_empty() && i.environment == "local" {
            local_instance
        } else {
            &i.instance_id
        };
        identifier(instance, 128, "identity.instance_id")?;
        let mut labels = BTreeMap::from([
            ("service_name", service.to_owned()),
            ("deployment_environment", i.environment.clone()),
            ("cluster", i.cluster.clone()),
            ("service_instance_id", instance.to_owned()),
        ]);
        for (key, value, max) in [
            ("region", &i.region, 63),
            ("zone", &i.zone, 63),
            ("service_version", &i.service_version, 64),
        ] {
            if let Some(value) = value {
                identifier(value, max, key)?;
                labels.insert(key, value.clone());
            }
        }
        if let Some(t) = root.get("telemetry") {
            for (alias, value) in [("service_name", service), ("service_instance_id", instance)] {
                if t.get(alias)
                    .and_then(Value::as_str)
                    .is_some_and(|v| !v.is_empty() && v != value)
                {
                    return Err(format!(
                        "telemetry.{alias} conflicts with grafana.observability.identity"
                    ));
                }
            }
        }
        Ok(Identity { labels })
    }

    /// 业务作用：计算专用 listener 的默认绑定地址。
    /// 参数说明：无。
    /// 返回：platform 模式对平台网络开放，其它模式默认仅本机。
    pub fn bind(&self) -> &str {
        self.prometheus.scrape.bind.as_deref().unwrap_or(
            if self.provisioning.mode == ProvisioningMode::Platform {
                "0.0.0.0:9464"
            } else {
                "127.0.0.1:9464"
            },
        )
    }
    /// 业务作用：判断当前启用的出口是否包含 scrape。
    /// 参数说明：无。
    /// 返回：禁用配置始终返回 false。
    pub fn scrape_enabled(&self) -> bool {
        self.enabled && self.prometheus.export_mode != ExportMode::RemoteWrite
    }
    /// 业务作用：判断当前启用的出口是否包含 remote write。
    /// 参数说明：无。
    /// 返回：禁用配置始终返回 false。
    pub fn remote_write_enabled(&self) -> bool {
        self.enabled && self.prometheus.export_mode != ExportMode::Scrape
    }

    /// 业务作用：为发现资源与可用性规则选择同一条主数据路径。
    /// 参数说明：无。
    /// 返回：显式发现保持不变；auto 在 remote-write-only 时选择推送，其余情况按平台环境选择抓取。
    pub fn primary_discovery(&self) -> Discovery {
        let discovery = self.provisioning.prometheus.discovery;
        if discovery != Discovery::Auto {
            return discovery;
        }
        if self.prometheus.export_mode == ExportMode::RemoteWrite {
            Discovery::RemoteWrite
        } else if self.provisioning.bindings.kubernetes_api.is_some() {
            Discovery::KubernetesPod
        } else {
            Discovery::DockerDns
        }
    }
}

impl AuthConfig {
    /// 业务作用：只在 bearer 生效时要求合法 secret locator。
    /// 参数说明：无。
    /// 返回：合法引用或不包含材料的配置错误。
    pub fn validate(&self) -> Result<(), String> {
        if self.mode == AuthMode::Bearer {
            secret_id(
                self.token
                    .as_deref()
                    .ok_or("bearer token locator is required")?,
            )?;
        }
        Ok(())
    }
}

impl AlertRulesConfig {
    /// 业务作用：校验聚合规则的固定字段、外部期望指标引用与采样窗口。
    /// 参数说明：无。
    /// 返回：指标名无表达式注入、窗口足以覆盖两次 evaluation，且各规则阈值有效时成功。
    fn validate(&self) -> Result<(), String> {
        let metric = &self.instance_down.expected_instances_metric;
        // 这里只接受指标名，不接受任意 PromQL；缺少实例拓扑的应用自报指标不能充当外部期望证据。
        if metric.is_empty()
            || metric.len() > 128
            || !metric.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_alphabetic()
                    || b"_:".contains(&byte)
                    || (index > 0 && byte.is_ascii_digit())
            })
            || matches!(
                metric.as_str(),
                "napp_instance_info" | "napp_observability_heartbeat_unixtime_seconds"
            )
        {
            return Err(
                "instance_down.expected_instances_metric must name an external platform metric"
                    .into(),
            );
        }
        range(
            self.evaluation_interval_ms,
            10_000,
            300_000,
            "alert_rules.evaluation_interval_ms",
        )?;
        for (window, hold) in [
            (self.error_rate.window_ms, self.error_rate.for_ms),
            (self.p99.window_ms, self.p99.for_ms),
            (self.acquire_timeout.window_ms, self.acquire_timeout.for_ms),
            (
                self.transaction_slot_wait.window_ms,
                self.transaction_slot_wait.for_ms,
            ),
            (
                self.stream_cancel_rate.window_ms,
                self.stream_cancel_rate.for_ms,
            ),
            (
                self.provider_failure_rate.window_ms,
                self.provider_failure_rate.for_ms,
            ),
        ] {
            range(
                window,
                60_000.max(self.evaluation_interval_ms * 2),
                3_600_000,
                "alert rule window_ms",
            )?;
            range(hold, 0, 3_600_000, "alert rule for_ms")?;
        }
        for hold in [
            self.pool_saturation.for_ms,
            self.notification_queue.for_ms,
            self.instance_down.for_ms,
            self.identity_collision.for_ms,
        ] {
            range(hold, 0, 3_600_000, "alert rule for_ms")?;
        }
        for ratio in [
            self.error_rate.threshold_ratio,
            self.pool_saturation.threshold_ratio,
            self.stream_cancel_rate.threshold_ratio,
            self.notification_queue.threshold_ratio,
            self.provider_failure_rate.threshold_ratio,
        ] {
            if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
                return Err("alert rule threshold_ratio must be between zero and one".into());
            }
        }
        for v in [
            self.p99.threshold_ms,
            self.transaction_slot_wait.threshold_ms,
        ] {
            range(v, 1, 300_000, "alert rule threshold_ms")?;
        }
        for v in [
            self.acquire_timeout.threshold_count,
            self.stream_cancel_rate.minimum_calls,
            self.provider_failure_rate.minimum_deliveries,
        ] {
            range(v, 1, 1_000_000, "alert rule count")?;
        }
        Ok(())
    }
}

/// 业务作用：提取凭据标识而不接触凭据材料。
/// 参数说明：`locator` 必须是 secret:// 引用。
/// 返回：合法 ID；非法引用只返回固定说明。
pub fn secret_id(locator: &str) -> Result<&str, String> {
    let id = locator
        .strip_prefix("secret://")
        .ok_or("credential must use secret locator")?;
    if id.is_empty()
        || id.len() > 128
        || id.split('/').any(|s| {
            s.is_empty()
                || s == "."
                || s == ".."
                || !s
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        })
    {
        return Err("invalid secret identifier".into());
    }
    Ok(id)
}
/// 业务作用：限制部署身份的稳定字符集和长度。
/// 参数说明：`value` 为身份，`max` 为上限，`field` 为安全字段名。
/// 返回：合法时成功，否则只包含字段名。
pub fn identifier(value: &str, max: usize, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
    {
        Err(format!("{field}: expected bounded ASCII identifier"))
    } else {
        Ok(())
    }
}
/// 业务作用：拒绝明文跨网络凭据传输和带用户信息的 URL。
/// 参数说明：`url` 为目标，`local` 允许本机回环 HTTP。
/// 返回：校验后的 URL；错误不回显输入。
pub fn validate_url(url: &str, local: bool) -> Result<reqwest::Url, String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "invalid observability endpoint")?;
    let loopback = parsed.host_str().is_some_and(|h| {
        h == "localhost"
            || h.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.query().is_some()
        || !(parsed.scheme() == "https" || (local && loopback && parsed.scheme() == "http"))
    {
        return Err(
            "observability endpoint must be HTTPS without credentials, query or fragment".into(),
        );
    }
    Ok(parsed)
}
/// 业务作用：拒绝显式 null，防止可选字段被静默重置。
/// 参数说明：`value` 为观测配置子树。
/// 返回：整棵树无 null 时成功。
fn reject_null(value: &Value) -> Result<(), String> {
    match value {
        Value::Null => Err("grafana.observability: explicit null is not allowed".into()),
        Value::Array(v) => v.iter().try_for_each(reject_null),
        Value::Object(v) => v.values().try_for_each(reject_null),
        _ => Ok(()),
    }
}
/// 业务作用：统一拒绝超出资源预算的配置数字。
/// 参数说明：`value` 为值，`min/max` 为闭区间，`field` 为字段。
/// 返回：范围内成功，否则固定字段错误。
fn range(value: u64, min: u64, max: u64, field: &str) -> Result<(), String> {
    if value < min || value > max {
        Err(format!("{field}: outside allowed range"))
    } else {
        Ok(())
    }
}
