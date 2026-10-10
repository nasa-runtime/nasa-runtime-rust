//! Saga 身份和地址校验完成后，连接与实际提交仍由通用发现 owner 管理。

use super::*;
use futures_util::{stream::FuturesUnordered, StreamExt};
use nagrpc::codegen::tonic::{
    body::Body,
    codegen::{http, Service},
};
use nagrpc::discovery::{DiscoveryClientOptions, DiscoveryGrpcService};
use tokio::time::Instant as TokioInstant;

pub(in crate::saga) struct ManagedSagaGrpc {
    discovery: Arc<ManagedSagaDiscovery>,
    timeout: Duration,
    credential: Arc<ManagedGrpcCredentialMaterial>,
    pool: tokio::sync::Mutex<Vec<Entry>>,
}

struct Entry {
    instance: nadisc::Instance,
    credential: Arc<ManagedGrpcCredentialMaterial>,
    service: DiscoveryGrpcService,
}

impl ManagedSagaGrpc {
    /// 业务作用：保存受信解析器和凭据来源，尚未取得发现证据时不建立连接。
    /// 参数说明：`discovery` 提供完整实例合同，`timeout` 为总预算，`credential` 提供 mTLS 身份。
    /// 返回：共享有限池的 Saga 调用器。
    pub(in crate::saga) fn new(
        discovery: Arc<ManagedSagaDiscovery>,
        timeout: Duration,
        credential: ManagedGrpcCredentialMaterial,
    ) -> Self {
        Self {
            discovery,
            timeout,
            credential: Arc::new(credential),
            pool: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    /// 业务作用：读取完整凭据快照，使证书轮换和信任窗口变化撤销旧连接的新调用权。
    /// 参数说明：无。
    /// 返回：同代共享 Arc；旧在途请求只保留原始预算。
    fn credential(&self) -> Arc<ManagedGrpcCredentialMaterial> {
        self.credential
            .source
            .as_ref()
            .map_or_else(|| self.credential.clone(), |source| source.credential())
    }

    /// 业务作用：用同一预算完成发现、有界并发 health 和单目标业务提交，最后在 HTTP/2 层复验注册事实。
    /// 参数说明：`request` 是已认证业务请求，`deadline` 从外层调用入口计时。
    /// 返回：完整受管响应流；发现、凭据或健康资格变化均拒绝新提交。
    pub(in crate::saga) async fn call(
        &self,
        request: http::Request<Body>,
        deadline: TokioInstant,
    ) -> Result<http::Response<Body>, nagrpc::Status> {
        let budget = nabudget::RequestBudget::until(deadline);
        budget
            .run(self.call_before(request, budget.clone()))
            .await
            .map_err(|_| nagrpc::Status::deadline_exceeded("Saga gRPC deadline elapsed"))?
    }

    /// 业务作用：在原期限内选择同代候选，有限池只保存仍受当前身份政策认可的连接。
    /// 参数说明：`request` 为待发 RPC，`budget` 约束锁等待、health、握手和正文。
    /// 返回：受管响应；已提交操作失败保持结果不确定。
    async fn call_before(
        &self,
        request: http::Request<Body>,
        budget: nabudget::RequestBudget,
    ) -> Result<http::Response<Body>, nagrpc::Status> {
        let unavailable =
            || nagrpc::Status::unavailable("Saga discovery has no authorized instance");
        let routes = self
            .discovery
            .routes(budget.deadline())
            .await
            .map_err(|_| unavailable())?;
        let credential = self.credential();
        let offset = self.discovery.next.fetch_add(1, Ordering::Relaxed) % routes.len();
        let mut probes = FuturesUnordered::new();
        // 单个失联副本不能占用全部期限；仅探测有限候选，健康成功后只向其中一个提交业务请求。
        let probe_deadline = TokioInstant::now() + budget.remaining().mul_f64(0.75);
        for selected in routes
            .iter()
            .cycle()
            .skip(offset)
            .take(routes.len().min(16))
        {
            let credential = credential.clone();
            let current_routes = &routes;
            let probe_budget = nabudget::RequestBudget::until(probe_deadline);
            probes.push(async move {
                let service = self
                    .service_for(selected, current_routes, credential)
                    .await?;
                let mut health = nagrpc::health::health_client::HealthClient::new(
                    service.clone().with_budget(probe_budget),
                );
                let response = health
                    .check(nagrpc::health::HealthCheckRequest {
                        service: String::new(),
                    })
                    .await?
                    .into_inner();
                if response.status != nagrpc::health::ServingStatus::Serving as i32 {
                    return Err(nagrpc::Status::unavailable(
                        "Saga gRPC target is not serving",
                    ));
                }
                Ok(service)
            });
        }
        let service = loop {
            match probes.next().await {
                Some(Ok(service)) => break service,
                Some(Err(_)) => continue,
                None => return Err(unavailable()),
            }
        };
        drop(probes);
        // health 等待期间可能发生证书撤销；旧成功不能给新业务请求授予过期身份。
        if !Arc::ptr_eq(&credential, &self.credential()) {
            return Err(nagrpc::Status::unavailable("Saga gRPC credential changed"));
        }
        let mut service = service.with_budget(budget);
        futures_util::future::poll_fn(|cx| service.poll_ready(cx)).await?;
        service.call(request).await
    }

    /// 业务作用：复验同一候选的协议与身份，取得绑定当前凭据的有限连接池入口。
    /// 参数说明：`selected` 是严格发现候选，`current_routes` 撤除旧成员，`credential` 是本次调用的完整凭据快照。
    /// 返回：可执行 health 的受管入口；地址漂移或身份不一致时拒绝。
    async fn service_for(
        &self,
        selected: &SagaDiscoveredRoute,
        current_routes: &[SagaDiscoveredRoute],
        credential: Arc<ManagedGrpcCredentialMaterial>,
    ) -> Result<DiscoveryGrpcService, nagrpc::Status> {
        let unavailable =
            || nagrpc::Status::unavailable("Saga discovery has no authorized instance");
        let gate = selected.gate.as_ref().ok_or_else(unavailable)?;
        let origin = reqwest::Url::parse(&selected.base).map_err(|_| unavailable())?;
        let authority = origin.host().ok_or_else(unavailable)?.to_string();
        let authority = authority.trim_start_matches('[').trim_end_matches(']');
        let instance = &gate.instance;
        // 业务身份、拨号地址和协议字段必须属于同一候选，不能借 metadata 改拨另一个端点。
        if origin.scheme() != "https"
            || authority != instance.ip
            || origin.port_or_known_default() != Some(instance.port)
            || instance
                .metadata
                .get("nasa.grpc.port")
                .and_then(|port| port.parse::<u16>().ok())
                != Some(instance.port)
            || instance
                .metadata
                .get("nasa.grpc.authority")
                .map(String::as_str)
                != Some(authority)
            || instance
                .metadata
                .get("nasa.grpc.tls_mode")
                .map(String::as_str)
                != Some("mutual")
        {
            return Err(nagrpc::Status::permission_denied(
                "Saga instance protocol binding is inconsistent",
            ));
        }
        let service = {
            let mut pool = self.pool.lock().await;
            pool.retain(|entry| {
                Arc::ptr_eq(&entry.credential, &credential)
                    && current_routes.iter().any(|route| {
                        route
                            .gate
                            .as_ref()
                            .is_some_and(|gate| gate.instance == entry.instance)
                    })
            });
            if let Some(entry) = pool.iter().find(|entry| entry.instance == *instance) {
                entry.service.clone()
            } else {
                if pool.len() >= 16 {
                    pool.remove(0);
                }
                let options = DiscoveryClientOptions {
                    request_timeout: self.timeout,
                    tls: Some(nagrpc::discovery::mutual_tls(
                        credential.ca_certificate_pem.as_bytes(),
                        credential.identity_certificate_pem.as_bytes(),
                        credential.identity_private_key_pem.as_bytes(),
                    )?),
                    tls_server_name: credential.domain_name.clone(),
                    allowed_authorities: [authority.to_owned()].into_iter().collect(),
                    ..Default::default()
                };
                let current = self.credential.clone();
                let admitted = credential.clone();
                let service = DiscoveryGrpcService::new(
                    gate.handle.clone(),
                    self.discovery.target.service.clone(),
                    options,
                )?
                .for_instance(instance.clone())
                .with_admission(Arc::new(move || {
                    let latest = current
                        .source
                        .as_ref()
                        .map_or_else(|| current.clone(), |source| source.credential());
                    Arc::ptr_eq(&admitted, &latest)
                }));
                pool.push(Entry {
                    instance: instance.clone(),
                    credential: credential.clone(),
                    service: service.clone(),
                });
                service
            }
        };
        Ok(service)
    }
}
