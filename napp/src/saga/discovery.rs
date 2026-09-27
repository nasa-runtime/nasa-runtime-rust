//! 协调侧发现合同：一次选择始终绑定同一实例的身份、端点、路径与代次。

use super::*;
use std::sync::atomic::AtomicUsize;

/// 业务作用：把逻辑发现引用限定到受信服务、长期身份和地址政策。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaDiscoveryTargetSettings {
    service: String,
    service_identity: String,
    address_policy_ref: String,
}

/// 业务作用：保存单一实例同代发布的全部线上路由字段，禁止跨实例拼接路径。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SagaDiscoveredRoute {
    incarnation: String,
    generation: u64,
    pub(super) base: String,
}

/// 业务作用：在每次投递前读取受信发现快照，并阻止同一实例的代次回退或同代内容漂移。
pub(super) struct ManagedSagaDiscovery {
    #[cfg(feature = "nacos-discovery")]
    handle: crate::capabilities::NacosDiscoveryHandle,
    #[cfg(feature = "nacos-discovery")]
    target: SagaDiscoveryTargetSettings,
    #[cfg(feature = "nacos-discovery")]
    policy: SagaAddressPolicySettings,
    #[cfg(feature = "nacos-discovery")]
    protocol: SagaClientProtocol,
    #[cfg(feature = "nacos-discovery")]
    seen: tokio::sync::Mutex<BTreeMap<String, SagaDiscoveredRoute>>,
    next: AtomicUsize,
}

impl ManagedSagaDiscovery {
    /// 业务作用：校验发现引用和出站地址边界；固定 URL 保持显式配置语义。
    /// 参数说明：`application` 提供受管发现会话，`settings` 提供信任政策，`reference` 选择逻辑服务，`protocol` 固定线上协议。
    /// 返回：固定地址返回空；已配置发现引用返回解析器，缺少服务身份、地址政策或发现组件时拒绝装配。
    fn from_reference(
        application: &Application,
        settings: &SagaSettings,
        reference: &str,
        protocol: SagaClientProtocol,
    ) -> ApplicationResult<Option<Arc<Self>>> {
        if reference.starts_with("http://") || reference.starts_with("https://") {
            return Ok(None);
        }
        let target = settings
            .discovery
            .get(reference)
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "Saga discovery reference is unknown",
                )
            })?
            .clone();
        if settings
            .client
            .orchestrator_identity
            .as_deref()
            .or(settings.participant.orchestrator_identity.as_deref())
            .is_some_and(|expected| expected != target.service_identity)
        {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "Saga discovery identity conflicts with the configured Orchestrator",
            ));
        }
        nasaga_runtime::ServiceIdentity::new(&target.service_identity).map_err(|_| {
            saga_error(
                ApplicationPhase::Ready,
                "Saga discovery service identity is invalid",
            )
        })?;
        if target.service.is_empty()
            || target.service.len() > 256
            || target.service.trim() != target.service
            || target.service.chars().any(char::is_control)
        {
            return Err(saga_error(
                ApplicationPhase::Ready,
                "Saga discovery service name is invalid",
            ));
        }
        let policy = settings
            .transport
            .address_policies
            .get(&target.address_policy_ref)
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "Saga discovery address policy is unknown",
                )
            })?
            .clone();
        validate_managed_address_policy(
            &policy,
            match protocol {
                SagaClientProtocol::Http => SagaTransportKind::Http,
                SagaClientProtocol::Grpc => SagaTransportKind::Grpc,
            },
            ApplicationPhase::Ready,
        )?;
        #[cfg(feature = "nacos-discovery")]
        {
            Ok(Some(Arc::new(Self {
                handle: application.nacos_discovery()?,
                target,
                policy,
                protocol,
                seen: tokio::sync::Mutex::new(BTreeMap::new()),
                next: AtomicUsize::new(0),
            })))
        }
        #[cfg(not(feature = "nacos-discovery"))]
        {
            let _ = (application, target, policy);
            Err(saga_error(
                ApplicationPhase::Ready,
                "Saga service discovery requires nacos-discovery",
            ))
        }
    }

    /// 业务作用：在原调用期限内查询并校验所有当前成员，拒绝使用已撤下或越界的路由。
    /// 参数说明：`deadline` 同时约束发现等待、后续连接与收据读取。
    /// 返回：至少一个合法成员时返回完整集合；失败、空集合、代次回退或同代漂移时关闭本次投递。
    pub(super) async fn routes(
        &self,
        deadline: tokio::time::Instant,
    ) -> ApplicationResult<Vec<SagaDiscoveredRoute>> {
        #[cfg(not(feature = "nacos-discovery"))]
        {
            let _ = deadline;
            Err(saga_error(
                ApplicationPhase::Running,
                "Saga discovery is unavailable",
            ))
        }
        #[cfg(feature = "nacos-discovery")]
        {
            tokio::time::timeout_at(deadline, async {
                // 串行查询与发布，避免先发起的旧查询在新快照之后覆盖观察到的代次。
                let mut seen = self.seen.lock().await;
                let instances = self.handle.saga_instances(&self.target.service).await?;
                if instances.len() > 4096 {
                    return Err(saga_error(
                        ApplicationPhase::Running,
                        "Saga discovery member limit exceeded",
                    ));
                }
                let mut routes = BTreeMap::new();
                for instance in instances {
                    if !instance.healthy || !instance.enabled {
                        continue;
                    }
                    let Some(route) = self.parse_route(&instance.metadata) else {
                        continue;
                    };
                    for previous in seen
                        .get(&route.incarnation)
                        .into_iter()
                        .chain(routes.get(&route.incarnation))
                    {
                        if route.generation < previous.generation
                            || (route.generation == previous.generation && route != *previous)
                        {
                            return Err(saga_error(
                                ApplicationPhase::Running,
                                "Saga discovery route generation is inconsistent",
                            ));
                        }
                    }
                    routes.insert(route.incarnation.clone(), route);
                }
                if routes.is_empty() {
                    return Err(saga_error(
                        ApplicationPhase::Running,
                        "Saga discovery has no authorized instance",
                    ));
                }
                // 同一存活实例不允许代次回退；已消失实例不保留连接或投递权威。
                *seen = routes.clone();
                Ok(routes.into_values().collect())
            })
            .await
            .map_err(|_| saga_error(ApplicationPhase::Running, "Saga discovery deadline elapsed"))?
        }
    }

    /// 业务作用：把一个实例的 provider 元数据校验为受信协议端点和有效路径。
    /// 参数说明：`metadata` 必须来自同一注册实例，不能从服务级默认值补齐安全字段。
    /// 返回：身份、地址范围、代次与规范路径均合法时返回路由；其它实例不进入候选集合。
    #[cfg(feature = "nacos-discovery")]
    fn parse_route(
        &self,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Option<SagaDiscoveredRoute> {
        if metadata.get("nasa.saga.identity")? != &self.target.service_identity
            || metadata.get("nasa.saga.role")? != "orchestrator"
        {
            return None;
        }
        let incarnation = metadata.get("nasa.saga.incarnation")?.clone();
        if incarnation.is_empty()
            || incarnation.len() > 128
            || !incarnation
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return None;
        }
        let generation = metadata
            .get("nasa.saga.generation")?
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)?;
        let key = match self.protocol {
            SagaClientProtocol::Http => "nasa.saga.http.origin",
            SagaClientProtocol::Grpc => "nasa.saga.grpc.origin",
        };
        let origin = metadata.get(key)?;
        let mut url = reqwest::Url::parse(origin).ok()?;
        if normalize_managed_capability_endpoint(origin, ApplicationPhase::Running)
            .ok()?
            .trim_end_matches('/')
            != origin.trim_end_matches('/')
        {
            return None;
        }
        let host = url.host_str()?.to_ascii_lowercase();
        let port = url.port_or_known_default()?;
        let allowed = match self.protocol {
            SagaClientProtocol::Http => {
                self.policy
                    .http_schemes
                    .iter()
                    .any(|value| value == url.scheme())
                    && self.policy.http_hosts.contains(&host)
                    && (self.policy.http_ports.is_empty() || self.policy.http_ports.contains(&port))
            }
            SagaClientProtocol::Grpc => {
                url.scheme() == "https"
                    && self.policy.grpc_hosts.contains(&host)
                    && (self.policy.grpc_ports.is_empty() || self.policy.grpc_ports.contains(&port))
            }
        };
        if !allowed {
            return None;
        }
        if self.protocol == SagaClientProtocol::Http {
            let path = metadata.get("nasa.saga.http.base_path")?;
            validate_saga_base_path(path, ApplicationPhase::Running).ok()?;
            url.set_path(path);
            // URL 规范化不能悄悄改变待签名路径。
            if url.path() != path {
                return None;
            }
        }
        Some(SagaDiscoveredRoute {
            incarnation,
            generation,
            base: url.to_string().trim_end_matches('/').to_owned(),
        })
    }
}

impl ManagedHttpTarget {
    /// 业务作用：为本次请求取得同一实例的拨号地址与签名路径，并计算剩余传输预算。
    /// 参数说明：无。
    /// 返回：静态目标或当前合法发现目标及剩余期限；发现失败时不发送请求。
    pub(super) async fn resolve(&self) -> ApplicationResult<(Self, Duration)> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let Some(discovery) = &self.discovery else {
            return Ok((self.clone(), self.timeout));
        };
        let routes = discovery.routes(deadline).await?;
        let selected = &routes[discovery.next.fetch_add(1, Ordering::Relaxed) % routes.len()];
        let operation = self
            .signed_path
            .strip_prefix("/discovery/")
            .ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Running,
                    "Saga discovery operation is invalid",
                )
            })?;
        let resolved = managed_http_target(&selected.base, operation, self.authenticator.clone())?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(saga_error(
                ApplicationPhase::Running,
                "Saga discovery deadline elapsed",
            ));
        }
        Ok((resolved, remaining))
    }
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl grpc_target::GrpcDiscoveryResolver for ManagedSagaDiscovery {
    /// 业务作用：向 mTLS 连接池投影受信成员与完整实例代次，保留同次发现绑定。
    /// 参数说明：`deadline` 是原调用期限。
    /// 返回：身份代次和端点成对返回，发现失败时拒绝连接池发布。
    fn routes(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ApplicationResult<Vec<(String, String)>>> + Send + '_>,
    > {
        Box::pin(async move {
            Ok(ManagedSagaDiscovery::routes(self, deadline)
                .await?
                .into_iter()
                .map(|route| {
                    (
                        format!("{}:{}", route.incarnation, route.generation),
                        route.base,
                    )
                })
                .collect())
        })
    }
}

/// 业务作用：统一 client、result 与 Registry 的静态地址和受信发现引用装配。
/// 参数说明：应用、设置与 reference 决定目标；operation 为固定相对操作名，authenticator 为调用身份凭据，timeout 为总预算。
/// 返回：固定地址或发现目标；逻辑引用缺少受信绑定时拒绝装配。
pub(super) fn managed_http_discovery_target(
    application: &Application,
    settings: &SagaSettings,
    reference: &str,
    operation: &str,
    authenticator: nasaga_runtime::SagaHttpMessageAuthenticator,
    timeout: Duration,
) -> ApplicationResult<ManagedHttpTarget> {
    let discovery = ManagedSagaDiscovery::from_reference(
        application,
        settings,
        reference,
        SagaClientProtocol::Http,
    )?;
    let mut target = managed_http_target(
        if discovery.is_some() {
            "http://discovery.invalid/discovery"
        } else {
            reference
        },
        operation,
        authenticator,
    )?;
    target.discovery = discovery;
    target.timeout = timeout;
    Ok(target)
}

/// 业务作用：为所有协调侧 gRPC 调用绑定受信发现和当前 mTLS 材料。
/// 参数说明：应用、设置与 reference 选择服务；timeout 为完整期限，credential 为经配置平面校验的客户端材料。
/// 返回：固定或动态 gRPC 目标；发现合同不完整时拒绝装配。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
pub(super) fn managed_grpc_discovery_target(
    application: &Application,
    settings: &SagaSettings,
    reference: &str,
    timeout: Duration,
    credential: &ManagedGrpcCredentialMaterial,
) -> ApplicationResult<ManagedGrpcTarget> {
    match ManagedSagaDiscovery::from_reference(
        application,
        settings,
        reference,
        SagaClientProtocol::Grpc,
    )? {
        Some(discovery) => Ok(grpc_target::build_discovered_grpc_target(
            discovery,
            timeout,
            credential.clone(),
        )),
        None => build_managed_grpc_target(reference, timeout, credential),
    }
}

/// 业务作用：只在受管 listener 已建立后公布本协调实例的实际端点和有效 Saga 路径。
/// 参数说明：`application` 提供当前角色与 listener，`host` 和 `port` 来自本次实际服务注册。
/// 返回：协调角色的框架元数据；其它角色返回空，实际路径或端点不可用时拒绝注册。
#[cfg(feature = "nacos-discovery")]
pub(crate) fn registration_metadata(
    application: &Application,
    host: &str,
    port: u16,
) -> ApplicationResult<BTreeMap<String, String>> {
    if application.config().value().get("saga").is_none() {
        return Ok(BTreeMap::new());
    }
    let settings = read_saga_settings(application, ApplicationPhase::Ready)?;
    if !matches!(
        settings.role,
        Some(SagaRole::Orchestrator | SagaRole::Combined)
    ) {
        return Ok(BTreeMap::new());
    }
    let mut values = BTreeMap::from([
        (
            "nasa.saga.identity".into(),
            settings.service_identity.clone().ok_or_else(|| {
                saga_error(
                    ApplicationPhase::Ready,
                    "Saga discovery identity is missing",
                )
            })?,
        ),
        ("nasa.saga.role".into(), "orchestrator".into()),
        (
            "nasa.saga.incarnation".into(),
            nasaga_runtime::SagaHttpMessageAuthenticator::issue_nonce(),
        ),
        ("nasa.saga.generation".into(), "1".into()),
    ]);
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    #[cfg(feature = "web")]
    if settings.api.http.enabled
        || settings
            .transport
            .command_result
            .as_ref()
            .is_some_and(|value| value.kind == Some(SagaTransportKind::Http))
    {
        let web = application.web()?;
        let path = format!(
            "{}{}",
            web.context_path().trim_end_matches('/'),
            settings.http.base_path.as_deref().unwrap_or("/_nasa/saga")
        );
        validate_saga_base_path(&path, ApplicationPhase::Ready)?;
        values.insert(
            "nasa.saga.http.origin".into(),
            format!("http://{host}:{port}"),
        );
        values.insert("nasa.saga.http.base_path".into(), path);
    }
    #[cfg(feature = "grpc")]
    if let Some(endpoint) = application.grpc_runtime().endpoint_registration() {
        if endpoint.tls_mode == "mutual" {
            values.insert(
                "nasa.saga.grpc.origin".into(),
                format!("https://{host}:{}", endpoint.port),
            );
        }
    }
    Ok(values)
}
