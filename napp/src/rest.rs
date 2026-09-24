//! 不依赖注册中心的命名出站客户端装配。

use crate::{
    Application, ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult,
    ComponentId, PrepareContext, ShutdownAction, ShutdownContext,
};
use rest_discovery::{RemoteRuntime, RestDiscovery, RestDiscoveryClient, RestDiscoveryOptions};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    #[serde(default)]
    enabled: bool,
    provider: Option<String>,
    #[serde(default)]
    global_default: bool,
    timeout_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    max_services: Option<usize>,
    #[serde(default)]
    services: BTreeMap<String, Vec<Endpoint>>,
    provider_ref: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    host: String,
    port: u16,
}

struct RestOwner(Arc<RemoteRuntime>);

impl ShutdownAction for RestOwner {
    /// 业务作用：标识命名 REST 运行态清理。
    /// 参数说明：无。
    /// 返回：固定能力名称。
    fn label(&self) -> &'static str {
        "rest-client"
    }

    /// 业务作用：撤销本代全局入口并等待客户端任务和在途调用退出。
    /// 参数说明：`context` 为宿主共享截止点。
    /// 返回：获得实际退出证明才成功；旧句柄不能再接纳工作。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        RestDiscovery::shutdown_if_current(&self.0);
        self.0.rest().shutdown_background();
        Box::pin(async move {
            self.0
                .rest()
                .shutdown(context.deadline().into())
                .await
                .map_err(|_| error("REST runtime did not drain"))
        })
    }
}

impl Drop for RestOwner {
    /// 业务作用：启动失败或外层取消时同步收回出站准入。
    /// 参数说明：无。
    /// 返回：仅撤销本代全局入口，不影响新应用。
    fn drop(&mut self) {
        RestDiscovery::shutdown_if_current(&self.0);
        self.0.rest().shutdown_background();
    }
}

/// 业务作用：在 initializer 前装配显式命名的 REST 计划，不注册本实例。
/// 参数说明：`context` 提供资源表与清理责任。
/// 返回：所有启用计划已经构造；无效 provider、错误引用或重复全局 owner 时拒绝启动。
pub(crate) async fn prepare(context: &mut PrepareContext<'_>) -> ApplicationResult<()> {
    let app = context.application().clone();
    let view = app.config();
    let Some(value) = view.value().get("rest_clients") else {
        return Ok(());
    };
    let plans: BTreeMap<String, Plan> = serde_json::from_value(value.clone())
        .map_err(|_| error("invalid rest_clients declaration"))?;
    if plans.len() > 64 {
        return Err(error("REST client count exceeds 64"));
    }
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        if name.is_empty() || name.len() > 128 || name.trim() != name {
            return Err(error("invalid REST client name"));
        }
        let mut options = RestDiscoveryOptions::default();
        for duration in [plan.timeout_ms, plan.connect_timeout_ms]
            .into_iter()
            .flatten()
        {
            if !(1..=300_000).contains(&duration) {
                return Err(error("REST timeout is outside supported range"));
            }
        }
        if let Some(timeout) = plan.timeout_ms {
            options.http.timeout = Duration::from_millis(timeout);
        }
        if let Some(timeout) = plan.connect_timeout_ms {
            options.http.connect_timeout = Duration::from_millis(timeout);
        }
        options.watch.max_services = plan.max_services.unwrap_or(1024);
        if plan.services.len() > options.watch.max_services
            || plan.services.values().any(|entries| {
                entries.len() > 128 || entries.iter().any(|entry| entry.host.len() > 253)
            })
        {
            return Err(error("REST configured service table exceeds limits"));
        }
        let provider: Option<Arc<dyn nadisc::DiscoveryClient>> =
            match plan.provider.as_deref().unwrap_or("external") {
                "external" => {
                    if !plan.services.is_empty() || plan.provider_ref.is_some() {
                        return Err(error("external REST cannot declare discovery inputs"));
                    }
                    None
                }
                "static" => {
                    if plan.provider_ref.is_some() {
                        return Err(error("static REST cannot declare provider_ref"));
                    }
                    let services = plan
                        .services
                        .into_iter()
                        .map(|(name, endpoints)| {
                            (
                                name,
                                endpoints
                                    .into_iter()
                                    .map(|endpoint| {
                                        nadisc::Instance::new(endpoint.host, endpoint.port)
                                    })
                                    .collect(),
                            )
                        })
                        .collect();
                    Some(Arc::new(
                        nadisc::StaticDiscovery::new(services)
                            .map_err(|_| error("invalid static discovery table"))?,
                    ))
                }
                "dns" => {
                    if plan.provider_ref.is_some() {
                        return Err(error("DNS REST cannot declare provider_ref"));
                    }
                    let mut services = BTreeMap::new();
                    for (service, mut endpoints) in plan.services {
                        if endpoints.len() != 1 {
                            return Err(error("DNS service requires exactly one host and port"));
                        }
                        let endpoint = endpoints.pop().expect("one DNS target");
                        services.insert(
                            service,
                            nadisc::DnsService {
                                host: endpoint.host,
                                port: endpoint.port,
                            },
                        );
                    }
                    Some(Arc::new(
                        nadisc::DnsDiscovery::new(services)
                            .map_err(|_| error("invalid DNS discovery table"))?,
                    ))
                }
                "custom" => {
                    if !plan.services.is_empty() {
                        return Err(error("custom REST cannot declare a static service table"));
                    }
                    let qualifier = plan
                        .provider_ref
                        .as_deref()
                        .ok_or_else(|| error("custom discovery requires provider_ref"))?;
                    Some(
                        app.named_resource::<Arc<dyn nadisc::DiscoveryClient>>(qualifier)
                            .await?
                            .clone(),
                    )
                }
                _ => return Err(error("unknown REST provider")),
            };
        let rest = Arc::new(
            match &provider {
                Some(provider) => RestDiscoveryClient::connect(provider.clone(), options).await,
                None => RestDiscoveryClient::try_external_only(options),
            }
            .map_err(|_| error("REST client preparation failed"))?,
        );
        let runtime = Arc::new(RemoteRuntime::new(rest.clone(), provider));
        // 在全局发布与资源登记前接管 owner，后续任何失败均能收回已建立的后台任务。
        context.activate(Box::new(RestOwner(runtime.clone())));
        if plan.global_default {
            RestDiscovery::install_runtime(runtime)
                .map_err(|_| error("REST default runtime is already owned"))?;
        }
        context.register_resource(Some(&name), rest)?;
    }
    Ok(())
}

impl Application {
    /// 业务作用：取得当前应用的命名受管 REST 客户端。
    /// 参数说明：`name` 为 rest_clients 中的名称。
    /// 返回：共享客户端；生命周期结束后保留的旧句柄拒绝新调用。
    pub async fn rest_client(&self, name: &str) -> ApplicationResult<Arc<RestDiscoveryClient>> {
        Ok(self
            .named_resource::<Arc<RestDiscoveryClient>>(name)
            .await?
            .clone())
    }
}

/// 业务作用：归类出站运行态装配与关闭失败，不暴露地址和凭据。
/// 参数说明：`message` 为稳定原因。
/// 返回：宿主能力错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}
