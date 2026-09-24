//! 独立 controller 的平台边界；应用副本不得持有本模块的控制面凭据。

use super::{
    assets::{datasource_uid, eligible_instances, scoped},
    dashboards, discovery_config, owner_id, pod_monitor, prometheus_rules, DatasourceMode,
    Discovery, Identity, ObservabilityConfig, ProvisioningMode,
};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{fs::File, path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;

/// 平台管理员提供的部署 binding，不注入业务进程。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct PlatformBindings {
    pub grafana_endpoint: Option<String>,
    pub grafana_token_secret: Option<String>,
    pub prometheus_url: Option<String>,
    pub docker_dns: Option<String>,
    pub prometheus_config: Option<PathBuf>,
    pub prometheus_reload_url: Option<String>,
    pub scrape_token_file: Option<String>,
    pub scrape_token_secret: Option<String>,
    pub kubernetes_api: Option<String>,
    pub kubernetes_token_file: Option<PathBuf>,
    pub kubernetes_ca_file: Option<PathBuf>,
    pub namespace: Option<String>,
    pub owner_lock_file: Option<PathBuf>,
}

/// 仅由控制面进程持有的 owner 权威和认证。
pub struct PlatformController {
    config: ObservabilityConfig,
    identity: Identity,
    bindings: PlatformBindings,
    client: Client,
    token: nasecret::SecretBytes,
    kube_token: Option<nasecret::SecretBytes>,
    holder: String,
    _lock: Option<File>,
}

impl PlatformController {
    /// 业务作用：只为单 owner controller 构造受限 HTTP 客户端和控制面凭据。
    /// 参数说明：`config/identity` 来自同一 YAML，`bindings` 为平台授权范围，`token` 为 Grafana secret 材料。
    /// 返回：可调和对象；业务出口不调用此入口，关闭 platform 时拒绝构造。
    pub fn new(
        mut config: ObservabilityConfig,
        identity: Identity,
        bindings: PlatformBindings,
        token: nasecret::SecretBytes,
    ) -> Result<Self, String> {
        if !config.enabled || config.provisioning.mode != ProvisioningMode::Platform {
            return Err("platform controller is disabled".into());
        }
        config.provisioning.bindings = bindings.clone();
        config.validate()?;
        let grafana = &mut config.provisioning.grafana;
        if grafana.endpoint.is_none() {
            grafana.endpoint = bindings.grafana_endpoint.clone();
        }
        if grafana.datasource.prometheus_url.is_none() {
            grafana.datasource.prometheus_url = bindings.prometheus_url.clone();
        }
        super::validate_url(
            grafana
                .endpoint
                .as_deref()
                .ok_or("Grafana endpoint binding is required")?,
            config.identity.environment == "local",
        )?;
        if grafana.datasource.mode == DatasourceMode::Managed {
            let url = grafana
                .datasource
                .prometheus_url
                .as_deref()
                .ok_or("Prometheus query binding is required")?;
            let parsed =
                reqwest::Url::parse(url).map_err(|_| "invalid Prometheus query binding")?;
            if !["http", "https"].contains(&parsed.scheme())
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
            {
                return Err("invalid Prometheus query binding".into());
            }
        } else {
            super::identifier(&grafana.datasource.uid, 128, "grafana.datasource.uid")?;
        }
        if let Some(endpoint) = &bindings.kubernetes_api {
            super::validate_url(endpoint, config.identity.environment == "local")?;
            super::identifier(
                bindings
                    .namespace
                    .as_deref()
                    .ok_or("Kubernetes namespace binding is required")?,
                63,
                "Kubernetes namespace binding",
            )?;
        }
        if token.is_empty() || token.len() > 4096 || std::str::from_utf8(token.expose()).is_err() {
            return Err("Grafana token material is invalid".into());
        }
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(
                config.provisioning.request_timeout_ms,
            ));
        let kube_token = if let Some(path) = &bindings.kubernetes_token_file {
            Some(nasecret::SecretBytes::new(
                std::fs::read(path).map_err(|_| "Kubernetes token binding cannot be read")?,
            ))
        } else {
            None
        };
        if let Some(path) = &bindings.kubernetes_ca_file {
            let bytes = std::fs::read(path).map_err(|_| "Kubernetes CA binding cannot be read")?;
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&bytes)
                    .map_err(|_| "Kubernetes CA binding is invalid")?,
            );
        }
        let client = builder
            .build()
            .map_err(|_| "platform client initialization failed")?;
        let holder = format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let lock = if bindings.kubernetes_api.is_none() {
            let path = bindings
                .owner_lock_file
                .as_ref()
                .ok_or("Compose controller requires a shared owner lock file binding")?;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|_| "owner lock binding cannot be opened")?;
            // 本地或 Compose 的所有副本必须共享同一 inode；持锁 fd 随 controller 生命周期释放。
            file.try_lock()
                .map_err(|_| "another platform owner holds the lock")?;
            Some(file)
        } else {
            None
        };
        Ok(Self {
            config,
            identity,
            bindings,
            client,
            token,
            kube_token,
            holder,
            _lock: lock,
        })
    }

    /// 业务作用：循环调和本 owner 资源，平台失败不接触业务 readiness。
    /// 参数说明：`stop` 为 controller 停机信号。
    /// 返回：取消时成功；required 首次失败返回部署侧阻断。
    pub async fn run(&self, stop: CancellationToken) -> Result<(), String> {
        let mut tick = tokio::time::interval(Duration::from_millis(
            self.config.provisioning.reconcile_interval_ms,
        ));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {_=stop.cancelled()=>return Ok(()),_=tick.tick()=>{if let Err(error)=self.reconcile().await{tracing::warn!(target:"nafana::platform",event="platform_reconcile_failed",reason=%error,"平台观测资源尚未收敛");if self.config.provisioning.required{return Err(error);}}}}
        }
    }

    /// 业务作用：在复验 owner 后依次调和逐实例采集、datasource、Dashboard 和规则。
    /// 参数说明：无。
    /// 返回：所有本轮资源已收敛时成功；未知归属资源始终保持原状。
    pub async fn reconcile(&self) -> Result<(), String> {
        self.acquire_lease().await?;
        self.reconcile_discovery().await?;
        let ds_uid = datasource_uid(&self.config, &self.identity);
        let ds_path = format!("/api/datasources/uid/{ds_uid}");
        let existing = self.grafana(Method::GET, &ds_path, None).await?;
        let owner = owner_id(&self.identity, "owner");
        if self.config.provisioning.grafana.datasource.mode == DatasourceMode::Existing {
            let value = existing.ok_or("existing Grafana datasource is missing")?;
            if value.get("type").and_then(Value::as_str) != Some("prometheus") {
                return Err("existing datasource is not Prometheus".into());
            }
        } else {
            if existing.as_ref().is_some_and(|v| {
                v.pointer("/jsonData/nasa_owner").and_then(Value::as_str) != Some(&owner)
            }) {
                return Err("Grafana datasource belongs to another owner".into());
            }
            let body = json!({"uid":ds_uid,"name":ds_uid,"type":"prometheus","access":"proxy","url":self.config.provisioning.grafana.datasource.prometheus_url,"isDefault":false,"jsonData":{"nasa_owner":owner,"httpMethod":"POST"}});
            self.grafana(
                if existing.is_some() {
                    Method::PUT
                } else {
                    Method::POST
                },
                if existing.is_some() {
                    &ds_path
                } else {
                    "/api/datasources"
                },
                Some(body),
            )
            .await?;
        }
        let folder_uid = owner_id(&self.identity, "folder");
        let folder_path = format!("/api/folders/{folder_uid}");
        let folder = &self.config.provisioning.grafana.folder;
        let folder_title = format!(
            "{} [nasa-owner:{owner}]",
            if folder.is_empty() {
                format!(
                    "nasa-runtime/{}",
                    self.identity
                        .labels
                        .get("service_name")
                        .map(String::as_str)
                        .unwrap_or_default()
                )
            } else {
                folder.clone()
            }
        );
        if let Some(existing) = self.grafana(Method::GET, &folder_path, None).await? {
            if existing.get("title").and_then(Value::as_str) != Some(&folder_title) {
                return Err("Grafana folder belongs to another owner".into());
            }
        } else {
            self.grafana(
                Method::POST,
                "/api/folders",
                Some(json!({"uid":folder_uid,"title":folder_title})),
            )
            .await?;
        }
        for mut dashboard in dashboards(&self.config, &self.identity) {
            let uid = dashboard["uid"].as_str().unwrap_or_default();
            if let Some(existing) = self
                .grafana(Method::GET, &format!("/api/dashboards/uid/{uid}"), None)
                .await?
            {
                let expected = format!("nasa-owner:{owner}");
                if !existing
                    .pointer("/dashboard/tags")
                    .and_then(Value::as_array)
                    .is_some_and(|tags| tags.iter().any(|tag| tag.as_str() == Some(&expected)))
                {
                    return Err("Grafana Dashboard belongs to another owner".into());
                }
                dashboard["version"] = existing
                    .pointer("/dashboard/version")
                    .cloned()
                    .unwrap_or(json!(0));
                dashboard["id"] = existing
                    .pointer("/dashboard/id")
                    .cloned()
                    .unwrap_or(Value::Null);
            }
            // 乐观并发版本来自刚读取的本 owner 文档；人工或其它 owner 的面板不允许覆盖。
            self.grafana(
                Method::POST,
                "/api/dashboards/db",
                Some(json!({"dashboard":dashboard,"folderUid":folder_uid,"overwrite":false})),
            )
            .await?;
        }
        if self.config.provisioning.grafana.alert_rules.enabled {
            let rules = prometheus_rules(&self.config, &self.identity);
            // 没有启用的规则时不创建缺少逐规则归属证据的空组；禁用配置不接管或删除既有平台资源。
            if rules["groups"][0]["rules"]
                .as_array()
                .is_some_and(Vec::is_empty)
            {
                return Ok(());
            }
            let group_name = owner_id(&self.identity, "rules");
            let path = format!("/api/v1/provisioning/folder/{folder_uid}/rule-groups/{group_name}");
            if let Some(existing) = self.grafana(Method::GET, &path, None).await? {
                if existing
                    .get("rules")
                    .and_then(Value::as_array)
                    .is_none_or(|rules| {
                        rules.is_empty()
                            || rules.iter().any(|rule| {
                                rule.pointer("/labels/nasa_owner").and_then(Value::as_str)
                                    != Some(&owner)
                            })
                    })
                {
                    return Err("Grafana rule group belongs to another owner".into());
                }
            }
            let rules=rules["groups"][0]["rules"].as_array().into_iter().flatten().map(|rule|{
                let name=rule["alert"].as_str().unwrap_or_default();
                let mut labels=rule["labels"].clone();if labels.get("notification_policy_ref")==Some(&Value::Null){labels.as_object_mut().map(|v|v.remove("notification_policy_ref"));}
                json!({"uid":owner_id(&self.identity,name),"title":name,"condition":"C","for":rule["for"],"noDataState":"NoData","execErrState":"Error","labels":labels,"annotations":rule["annotations"],"data":[{"refId":"A","relativeTimeRange":{"from":self.config.provisioning.grafana.alert_rules.evaluation_interval_ms/1000*2,"to":0},"datasourceUid":ds_uid,"model":{"refId":"A","expr":grafana_condition(name,rule["expr"].as_str().unwrap_or_default(),&self.config,&self.identity),"instant":true,"range":false}},{"refId":"C","relativeTimeRange":{"from":0,"to":0},"datasourceUid":"__expr__","model":{"refId":"C","type":"threshold","expression":"A","conditions":[{"evaluator":{"type":"gt","params":[0]},"operator":{"type":"and"},"reducer":{"type":"last","params":[]},"type":"query"}]}}]})
            }).collect::<Vec<_>>();
            self.grafana(Method::PUT,&path,Some(json!({"name":group_name,"folderUid":folder_uid,"interval":self.config.provisioning.grafana.alert_rules.evaluation_interval_ms/1000,"rules":rules}))).await?;
        }
        Ok(())
    }

    /// 业务作用：向 Grafana 发出受预算约束的请求，每次写入前复验领导权。
    /// 参数说明：`method/path/body` 由内建模板构造，不接受任意业务 URL。
    /// 返回：404 为缺失；其它失败只返回稳定原因，不读取错误正文。
    async fn grafana(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Option<Value>, String> {
        let read = method == Method::GET;
        if method != Method::GET {
            self.ensure_leader().await?;
        }
        let endpoint = self
            .config
            .provisioning
            .grafana
            .endpoint
            .as_deref()
            .unwrap_or_default()
            .trim_end_matches('/');
        let mut request = self
            .client
            .request(method, format!("{endpoint}{path}"))
            .bearer_auth(
                std::str::from_utf8(self.token.expose())
                    .map_err(|_| "invalid Grafana credential")?,
            )
            .header(
                "X-Grafana-Org-Id",
                self.config.provisioning.grafana.organization_id,
            );
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| "Grafana request unavailable")?;
        if read && response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err("Grafana rejected platform request".into());
        }
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(Some(Value::Null));
        }
        response
            .json()
            .await
            .map(Some)
            .map_err(|_| "Grafana response is invalid".into())
    }

    /// 业务作用：使用 Kubernetes resourceVersion CAS 取得或续约单 owner 权威。
    /// 参数说明：无。
    /// 返回：当前持有有效租约时成功；其它持有者存在时不产生平台写入。
    async fn acquire_lease(&self) -> Result<(), String> {
        if self.bindings.kubernetes_api.is_none() {
            return Ok(());
        }
        let namespace = self
            .bindings
            .namespace
            .as_deref()
            .ok_or("Kubernetes namespace binding is required")?;
        let name = owner_id(&self.identity, "leader");
        let collection = format!("/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases");
        let path = format!("{collection}/{name}");
        let old = self.kubernetes(Method::GET, &path, None).await?;
        if let Some(old) = &old {
            let holder = old
                .pointer("/spec/holderIdentity")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if holder != self.holder && lease_valid(old, 0) {
                return Err("another platform leader holds authority".into());
            }
        }
        // 协议只接受整秒，向上覆盖已校验的毫秒窗口，避免其它 owner 早于配置承诺接管。
        let lease_seconds = self.config.provisioning.lease_duration_ms.div_ceil(1000);
        let mut body = json!({"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":name,"namespace":namespace,"labels":{"nasa-runtime-owner":owner_id(&self.identity,"owner")}},"spec":{"holderIdentity":self.holder,"leaseDurationSeconds":lease_seconds,"renewTime":chrono::Utc::now().to_rfc3339()}});
        if let Some(old) = &old {
            if old
                .pointer("/metadata/labels/nasa-runtime-owner")
                .and_then(Value::as_str)
                != Some(&owner_id(&self.identity, "owner"))
            {
                return Err("platform Lease belongs to another owner".into());
            }
            body["metadata"]["resourceVersion"] = old["metadata"]["resourceVersion"].clone();
        }
        self.kubernetes(
            if old.is_some() {
                Method::PUT
            } else {
                Method::POST
            },
            if old.is_some() {
                &path
            } else {
                &collection
            },
            Some(body),
        )
        .await?;
        Ok(())
    }

    /// 业务作用：在每次外部副作用前重新确认租约仍属于本进程且覆盖请求预算。
    /// 参数说明：无。
    /// 返回：失权、证据缺失或剩余时间不足时拒绝动作。
    async fn ensure_leader(&self) -> Result<(), String> {
        if self.bindings.kubernetes_api.is_none() {
            return if self._lock.is_some() {
                Ok(())
            } else {
                Err("platform owner lock is absent".into())
            };
        }
        let namespace = self
            .bindings
            .namespace
            .as_deref()
            .ok_or("missing Kubernetes namespace")?;
        let lease = self
            .kubernetes(
                Method::GET,
                &format!(
                    "/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases/{}",
                    owner_id(&self.identity, "leader")
                ),
                None,
            )
            .await?
            .ok_or("platform Lease is missing")?;
        if lease
            .pointer("/spec/holderIdentity")
            .and_then(Value::as_str)
            != Some(&self.holder)
            || !lease_valid(&lease, self.config.provisioning.request_timeout_ms)
        {
            return Err("platform leadership is no longer valid".into());
        }
        Ok(())
    }

    /// 业务作用：在平台授权命名空间内访问 Kubernetes API。
    /// 参数说明：`method/path/body` 为固定资源 API，不包含原始错误正文。
    /// 返回：解析后的资源或稳定失败；不跟随认证重定向。
    async fn kubernetes(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Option<Value>, String> {
        let read = method == Method::GET;
        let endpoint = self
            .bindings
            .kubernetes_api
            .as_deref()
            .ok_or("missing Kubernetes API binding")?
            .trim_end_matches('/');
        let mut request = self.client.request(method, format!("{endpoint}{path}"));
        if let Some(token) = &self.kube_token {
            request = request.bearer_auth(
                std::str::from_utf8(token.expose())
                    .map_err(|_| "invalid Kubernetes token")?
                    .trim(),
            );
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| "Kubernetes API unavailable")?;
        if read && response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err("Kubernetes API rejected owner action".into());
        }
        response
            .json()
            .await
            .map(Some)
            .map_err(|_| "Kubernetes API response invalid".into())
    }

    /// 业务作用：根据平台 binding 调和逐 Pod 或逐容器采集，不写入业务 Pod 权限。
    /// 参数说明：无。
    /// 返回：采集资源已发布时成功；已有企业平台与 remote write 无采集写入。
    async fn reconcile_discovery(&self) -> Result<(), String> {
        let discovery = self.config.primary_discovery();
        match discovery {
            Discovery::Existing | Discovery::RemoteWrite => Ok(()),
            Discovery::KubernetesPod => {
                let namespace = self
                    .bindings
                    .namespace
                    .as_deref()
                    .ok_or("Kubernetes namespace binding is required")?;
                let mut desired = pod_monitor(
                    &self.config,
                    &self.identity,
                    namespace,
                    self.bindings.scrape_token_secret.as_deref(),
                )?;
                let collection =
                    format!("/apis/monitoring.coreos.com/v1/namespaces/{namespace}/podmonitors");
                let path = format!("{collection}/{}", owner_id(&self.identity, "podmonitor"));
                let old = self.kubernetes(Method::GET, &path, None).await?;
                if let Some(old) = &old {
                    if old
                        .pointer("/metadata/labels/nasa-runtime-owner")
                        .and_then(Value::as_str)
                        != Some(&owner_id(&self.identity, "owner"))
                    {
                        return Err("PodMonitor belongs to another owner".into());
                    }
                    desired["metadata"]["resourceVersion"] =
                        old["metadata"]["resourceVersion"].clone();
                }
                self.ensure_leader().await?;
                self.kubernetes(
                    if old.is_some() {
                        Method::PUT
                    } else {
                        Method::POST
                    },
                    if old.is_some() {
                        &path
                    } else {
                        &collection
                    },
                    Some(desired),
                )
                .await?;
                Ok(())
            }
            Discovery::DockerDns => {
                let dns = self
                    .bindings
                    .docker_dns
                    .as_deref()
                    .ok_or("Docker DNS binding is required")?;
                let config = discovery_config(
                    &self.config,
                    &self.identity,
                    dns,
                    self.bindings.scrape_token_file.as_deref(),
                )?;
                let path = self
                    .bindings
                    .prometheus_config
                    .as_ref()
                    .ok_or("Prometheus config file binding is required")?;
                let reload = self
                    .bindings
                    .prometheus_reload_url
                    .as_deref()
                    .ok_or("Prometheus reload URL binding is required")?;
                let parsed =
                    reqwest::Url::parse(reload).map_err(|_| "invalid Prometheus reload binding")?;
                if !["http", "https"].contains(&parsed.scheme())
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err("invalid Prometheus reload binding".into());
                }
                let marker = format!("# nasa-owner:{}\n", owner_id(&self.identity, "owner"));
                if path.exists() {
                    let existing = std::fs::read_to_string(path)
                        .map_err(|_| "Prometheus config cannot be read")?;
                    if !existing.starts_with(&marker) {
                        return Err("Prometheus config belongs to another owner".into());
                    }
                }
                self.ensure_leader().await?;
                let encoded = serde_yaml::to_string(&config)
                    .map_err(|_| "Prometheus config encoding failed")?;
                let temp = path.with_extension(format!("{}.pending", self.holder));
                std::fs::write(&temp, format!("{marker}{encoded}"))
                    .map_err(|_| "Prometheus config cannot be written")?;
                std::fs::rename(&temp, path).map_err(|_| "Prometheus config publication failed")?;
                self.ensure_leader().await?;
                let response = self
                    .client
                    .post(reload)
                    .send()
                    .await
                    .map_err(|_| "Prometheus reload unavailable")?;
                if !response.status().is_success() {
                    return Err("Prometheus reload rejected".into());
                }
                Ok(())
            }
            Discovery::Auto => Err("unresolved platform discovery".into()),
        }
    }
}

/// 业务作用：确认租约证据覆盖后续一次外部请求的预算。
/// 参数说明：`lease` 为控制面返回的资源，`margin_ms` 为请求所需剩余时间。
/// 返回：过期、字段缺失或非法时间均视为无权执行。
fn lease_valid(lease: &Value, margin_ms: u64) -> bool {
    let Some(renew) = lease
        .pointer("/spec/renewTime")
        .and_then(Value::as_str)
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
    else {
        return false;
    };
    let Some(seconds) = lease
        .pointer("/spec/leaseDurationSeconds")
        .and_then(Value::as_i64)
    else {
        return false;
    };
    renew
        .timestamp_millis()
        .saturating_add(seconds.saturating_mul(1000))
        > chrono::Utc::now()
            .timestamp_millis()
            .saturating_add(margin_ms as i64)
}

/// 业务作用：把 Prometheus 告警过滤表达式转成 Grafana 有数据时的零/一条件。
/// 参数说明：`name` 选择同域存在性基线，`expression` 是告警条件，`config/identity` 限定同一数据集合。
/// 返回：健康时仅从真实样本导出零；缺失样本仍为无数据，不伪造全局零值。
fn grafana_condition(
    name: &str,
    expression: &str,
    config: &ObservabilityConfig,
    identity: &Identity,
) -> String {
    let selector = identity
        .labels
        .iter()
        .filter(|(k, _)| ["service_name", "deployment_environment", "cluster"].contains(k))
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect::<Vec<_>>()
        .join(",");
    let group = "deployment_environment,cluster,service_name,driver,datasource,method";
    let eligible = eligible_instances(&selector);
    let vector = |metric: &str| scoped(format!("{metric}{{{selector}}}"), &eligible);
    // 业务基线在聚合前复用告警的实例门禁，不能把被排除实例残留的样本转换成健康零值。
    let baseline=match name{
        "DatasourcePoolSaturation"=>format!("sum by (deployment_environment,cluster,service_name,driver,datasource) ({})", vector("natx_pool_max_connections")),
        "DatasourceAcquireTimeout"=>format!("sum by (deployment_environment,cluster,service_name,driver,datasource) ({})", vector("natx_connection_acquires_total")),
        "TransactionSlotWaitP99"=>format!("sum by ({group}) ({})", vector("natx_transaction_slot_waits_total")),
        "MapperStreamCancellation"=>format!("sum by ({group}) ({})", vector("namapper_streams_total")),
        "NotificationQueueSaturation"=>format!("sum by (deployment_environment,cluster,service_name) ({})", vector("nanotify_queue_capacity")),
        "NotificationProviderFailureRate"=>format!("sum by (deployment_environment,cluster,service_name,provider) ({})", vector("nanotify_deliveries_total")),
        // 可用性与身份告警必须保留异常实例，不能套用会将其排除的业务门禁。
        "ObservabilityInstanceDown"=>if config.primary_discovery()==Discovery::RemoteWrite { super::assets::availability_heartbeat(config,&selector) } else {format!("up{{{selector}}}")},
        "ObservabilityIdentityCollision"=>format!("count by (deployment_environment,cluster,service_name,service_instance_id) (napp_instance_info{{{selector}}})"),
        _=>format!("sum by ({group}) ({})", vector("namapper_db_client_operations_total")),
    };
    format!("(({expression}) * 0 + 1) or (({baseline}) * 0)")
}
