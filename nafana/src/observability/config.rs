use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

macro_rules! config {
    ($name:ident { $($field:ident : $ty:ty = $value:expr),* $(,)? }) => {
        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
        #[serde(default, deny_unknown_fields)]
        pub struct $name { $(pub $field: $ty),* }
        impl Default for $name {
            /// 业务作用：为缺省及空配置子树提供一致的递归默认值。
            /// 参数说明：无。
            /// 返回：不创建资源、不解析凭据的配置值。
            fn default() -> Self { Self { $($field: $value),* } }
        }
    };
}

macro_rules! choices {
    ($name:ident, $first:ident $(,$other:ident)* $(,)?) => {
        #[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { #[default] $first, $($other),* }
    };
}
choices!(ExportMode, Scrape, RemoteWrite, Both);
choices!(ListenerMode, Dedicated, Web);
choices!(AuthMode, None, Bearer);
choices!(ProvisioningMode, Disabled, Platform);
choices!(
    Discovery,
    Auto,
    KubernetesPod,
    DockerDns,
    Existing,
    RemoteWrite
);
choices!(Scheme, Http, Https);
choices!(DatasourceMode, Managed, Existing);

config!(ObservabilityConfig {
    enabled: bool = false,
    identity: IdentityConfig = IdentityConfig::default(),
    prometheus: PrometheusConfig = PrometheusConfig::default(),
    provisioning: ProvisioningConfig = ProvisioningConfig::default(),
});
config!(IdentityConfig {
    service_name: String = String::new(), environment: String = String::new(),
    cluster: String = String::new(), region: Option<String> = None,
    zone: Option<String> = None, instance_id: String = String::new(),
    service_version: Option<String> = None,
});
config!(PrometheusConfig {
    export_mode: ExportMode = ExportMode::Scrape,
    scrape: ScrapeConfig = ScrapeConfig::default(),
    remote_write: RemoteWriteConfig = RemoteWriteConfig::default(),
});
config!(AuthConfig { mode: AuthMode = AuthMode::None, token: Option<String> = None });
config!(ScrapeConfig {
    listener: ListenerMode = ListenerMode::Dedicated,
    bind: Option<String> = None, path: String = "/metrics".into(),
    max_concurrent_requests: usize = 4, request_timeout_ms: u64 = 2000,
    auth: AuthConfig = AuthConfig::default(),
});
config!(RemoteWriteConfig {
    endpoint: Option<String> = None, auth: AuthConfig = AuthConfig::default(),
    interval_ms: u64 = 10_000, request_timeout_ms: u64 = 3000,
    queue_capacity: usize = 8, max_batch_samples: usize = 5000,
    retry_max_attempts: usize = 1, retry_backoff_ms: u64 = 500,
    shutdown_drain_timeout_ms: u64 = 3000,
});
config!(ProvisioningConfig {
    mode: ProvisioningMode = ProvisioningMode::Disabled,
    required: bool = false,
    reconcile_interval_ms: u64 = 30_000,
    request_timeout_ms: u64 = 3000,
    lease_duration_ms: u64 = 120_000,
    bindings: super::platform::PlatformBindings = super::platform::PlatformBindings::default(),
    prometheus: DiscoveryConfig = DiscoveryConfig::default(),
    grafana: GrafanaConfig = GrafanaConfig::default(),
});
config!(DiscoveryConfig {
    discovery: Discovery = Discovery::Auto,
    job_name: String = String::new(),
    scheme: Scheme = Scheme::Http,
    scrape_interval_ms: u64 = 15_000,
    scrape_timeout_ms: u64 = 10_000,
});
config!(GrafanaConfig {
    endpoint: Option<String> = None, api_token: Option<String> = None,
    organization_id: u32 = 1, folder: String = String::new(),
    datasource: DatasourceConfig = DatasourceConfig::default(),
    dashboards: DashboardConfig = DashboardConfig::default(),
    alert_rules: AlertRulesConfig = AlertRulesConfig::default(),
});
config!(DatasourceConfig {
    mode: DatasourceMode = DatasourceMode::Managed, uid: String = String::new(),
    prometheus_url: Option<String> = None,
});
config!(DashboardConfig {
    interfaces: bool = true,
    mapper: bool = true,
    datasource: bool = true,
    notifications: bool = true,
});
config!(RatioRule {
    enabled: bool = true,
    window_ms: u64 = 300_000,
    threshold_ratio: f64 = 0.02,
    for_ms: u64 = 600_000
});
config!(LatencyRule {
    enabled: bool = true,
    window_ms: u64 = 300_000,
    threshold_ms: u64 = 1000,
    for_ms: u64 = 600_000
});
config!(SaturationRule {
    enabled: bool = true,
    threshold_ratio: f64 = 0.9,
    for_ms: u64 = 300_000
});
config!(CountRule {
    enabled: bool = true,
    window_ms: u64 = 300_000,
    threshold_count: u64 = 1,
    for_ms: u64 = 0
});
config!(StreamRule {
    enabled: bool = true,
    window_ms: u64 = 300_000,
    threshold_ratio: f64 = 0.05,
    minimum_calls: u64 = 20,
    for_ms: u64 = 300_000
});
config!(ProviderRule {
    enabled: bool = true,
    window_ms: u64 = 300_000,
    threshold_ratio: f64 = 0.10,
    minimum_deliveries: u64 = 10,
    for_ms: u64 = 300_000
});
config!(PresenceRule {
    enabled: bool = true,
    for_ms: u64 = 120_000,
    expected_instances_metric: String = "platform_expected_instance_info".into()
});
config!(SlotRule {
    enabled: bool = true,
    window_ms: u64 = 300_000,
    threshold_ms: u64 = 250,
    for_ms: u64 = 300_000
});
config!(QueueRule {
    enabled: bool = true,
    threshold_ratio: f64 = 0.8,
    for_ms: u64 = 300_000
});
config!(CollisionRule {
    enabled: bool = true,
    for_ms: u64 = 60_000
});
config!(AlertRulesConfig {
    enabled: bool = true, evaluation_interval_ms: u64 = 60_000,
    notification_policy_ref: Option<String> = None,
    error_rate: RatioRule = RatioRule::default(), p99: LatencyRule = LatencyRule::default(),
    pool_saturation: SaturationRule = SaturationRule::default(), acquire_timeout: CountRule = CountRule::default(),
    transaction_slot_wait: SlotRule = SlotRule::default(),
    stream_cancel_rate: StreamRule = StreamRule::default(),
    notification_queue: QueueRule = QueueRule::default(),
    provider_failure_rate: ProviderRule = ProviderRule::default(), instance_down: PresenceRule = PresenceRule::default(),
    identity_collision: CollisionRule = CollisionRule::default(),
});

/// 已冻结的进程身份，所有出口共用同一组 label。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
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
