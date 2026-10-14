//! Schema Registry 的命名配置、材料绑定与完整调用责任。

use crate::managed_adapters::{AdapterShutdown, ManagedAdapter};
use crate::{
    Application, ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult,
    ComponentId, PrepareContext, ShutdownAction, ShutdownContext,
};
use nafka::{
    ConfluentRegistryOptions, ConfluentSchemaRegistry, RegisteredSchema, RegistrySchemaType,
    SchemaId, SchemaRegistryAuth, SchemaRegistryClient, SchemaRegistryError, SchemaRegistryTls,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    #[serde(default)]
    enabled: bool,
    endpoint: String,
    bearer: Option<String>,
    username: Option<String>,
    password: Option<String>,
    tls: Option<TlsPlan>,
    request_timeout_ms: Option<u64>,
    max_response_bytes: Option<usize>,
    cache_capacity: Option<usize>,
    max_pending_lookups: Option<usize>,
    #[serde(default)]
    auto_register: bool,
}

/// Registry 独立的 TLS 材料引用，禁止把 Kafka 凭据或未解析明文当作客户端身份。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsPlan {
    /// 可选信任锚 PEM 的 secret:// 引用，提供时替代 HTTP 默认根。
    ca: Option<String>,
    /// 可选证书链与未加密 PKCS#8 私钥 PEM 的 secret:// 引用。
    identity: Option<String>,
}

/// 业务作用：从同一已解析视图装配命名 Registry client，不隐式注册 schema。
/// 参数说明：`context` 持有资源、secret 与停机责任。
/// 返回：显式计划全部通过；禁用计划不解析认证或连接远端。
pub(crate) fn prepare(context: &mut PrepareContext<'_>) -> ApplicationResult<()> {
    let app = context.application().clone();
    let view = app.config_view();
    let Some(value) = view.snapshot().value().get("schema_registries") else {
        return Ok(());
    };
    let plans: BTreeMap<String, Plan> = serde_json::from_value(value.clone())
        .map_err(|_| error("invalid schema_registries declaration"))?;
    if plans.len() > 64 {
        return Err(error("schema registry count exceeds 64"));
    }
    let mut metrics = Vec::new();
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        if name.is_empty() || name.len() > 128 || name.trim() != name {
            return Err(error("invalid schema registry name"));
        }
        let mut options = ConfluentRegistryOptions::new(plan.endpoint);
        options.request_timeout = Duration::from_millis(plan.request_timeout_ms.unwrap_or(3000));
        options.max_response_bytes = plan.max_response_bytes.unwrap_or(1024 * 1024);
        options.cache_capacity = plan.cache_capacity.unwrap_or(256);
        options.max_pending_lookups = plan.max_pending_lookups.unwrap_or(4096);
        options.auto_register = plan.auto_register;
        if options.request_timeout > Duration::from_secs(300) || options.cache_capacity > 65536 {
            return Err(error(
                "schema registry resource limit exceeds supported range",
            ));
        }
        options.auth = match (plan.bearer, plan.username, plan.password) {
            (None, None, None) => None,
            (Some(token), None, None) => Some(SchemaRegistryAuth::Bearer(material(
                view.secrets(),
                &token,
            )?)),
            (None, Some(username), Some(password)) => Some(SchemaRegistryAuth::Basic {
                username: username.into(),
                password: material(view.secrets(), &password)?,
            }),
            _ => {
                return Err(error(
                    "schema registry authentication is ambiguous or incomplete",
                ))
            }
        };
        let client = match plan.tls {
            Some(tls) => {
                let ca_pem = tls
                    .ca
                    .as_deref()
                    .map(|reference| material(view.secrets(), reference))
                    .transpose()?
                    .map(|secret| secret.expose().to_vec());
                let identity_pem = tls
                    .identity
                    .as_deref()
                    .map(|reference| material(view.secrets(), reference))
                    .transpose()?;
                ConfluentSchemaRegistry::new_with_tls(
                    options,
                    SchemaRegistryTls {
                        ca_pem,
                        identity_pem,
                    },
                )
            }
            None => ConfluentSchemaRegistry::new(options),
        }
        .map_err(|_| error("schema registry options rejected"))?;
        let client = Arc::new(client);
        metrics.push(client.clone());
        let managed = ManagedAdapter::new(client.clone());
        context.activate(Box::new(RegistryShutdown {
            adapter: AdapterShutdown(managed.clone()),
            client,
        }));
        let shared: Arc<dyn SchemaRegistryClient> = managed;
        context.register_resource(Some(&name), shared)?;
    }
    if !metrics.is_empty() {
        app.metrics_hub()
            .register_legacy_source_reserved(
                nafka::schema_metrics::metrics_source_many(metrics),
                32,
            )
            .map_err(|_| error("schema registry metrics registration failed"))?;
    }
    Ok(())
}

/// Registry 关闭责任同时涵盖业务等待和取消等待后仍由客户端持有的 HTTP 请求。
struct RegistryShutdown {
    adapter: AdapterShutdown<ConfluentSchemaRegistry>,
    client: Arc<ConfluentSchemaRegistry>,
}

impl ShutdownAction for RegistryShutdown {
    /// 业务作用：为独立 Registry 清理提供稳定名称。
    /// 参数说明：无。
    /// 返回：固定名称，不包含 endpoint、subject 或认证材料。
    fn label(&self) -> &'static str {
        "schema-registry"
    }

    /// 业务作用：收回 Registry 准入并在宿主截止点内归还全部 HTTP 与业务等待责任。
    /// 参数说明：context 为所有清理步骤共享的停止预算。
    /// 返回：实际请求和旧调用均退出时成功；超时保留关闭状态，不推定远端注册已撤销。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        // 先关闭外部门面与原生请求准入，旧等待退出期间不能补发新的请求。
        self.adapter.0.close();
        self.client.close();
        Box::pin(async move {
            if !self.client.await_idle(context.deadline()).await {
                return Err(ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Stopping,
                    "schema registry requests did not drain",
                ));
            }
            self.adapter.shutdown(context).await
        })
    }
}

impl Drop for RegistryShutdown {
    /// 业务作用：在启动回滚或清理责任提前退出时关闭 Registry 的内部请求门禁。
    /// 参数说明：无。
    /// 返回：已接纳请求收到关闭通知；远端副作用结果仍可能未知。
    fn drop(&mut self) {
        self.client.close();
    }
}

/// 业务作用：只从本代 SecretSnapshot 读取 Registry 凭据，不接受明文认证配置。
/// 参数说明：`secrets` 为初始已解析材料；`reference` 为 secret:// 身份。
/// 返回：拥有清零责任的材料副本；不存在或格式错误时拒绝。
fn material(
    secrets: &nasecret::SecretSnapshot,
    reference: &str,
) -> ApplicationResult<nasecret::SecretBytes> {
    let id = reference
        .strip_prefix("secret://")
        .ok_or_else(|| error("schema registry credential must reference secret material"))?;
    secrets
        .get(id)
        .map(|secret| nasecret::SecretBytes::new(secret.expose().to_vec()))
        .ok_or_else(|| error("schema registry credential is unavailable"))
}

#[async_trait::async_trait]
impl SchemaRegistryClient for ManagedAdapter<ConfluentSchemaRegistry> {
    /// 业务作用：在关闭门禁内获取已批准 schema，保留底层有界缓存与查询失败语义。
    /// 参数说明：`id` 为正 schema ID。
    /// 返回：查询结果；关闭后拒绝，不建立新的 HTTP 调用。
    async fn schema_by_id(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| SchemaRegistryError::Closed)?;
        guard
            .as_ref()
            .expect("admitted registry")
            .schema_by_id(id)
            .await
    }
    /// 业务作用：在完整调用责任内检查 schema 兼容性。
    /// 参数说明：`subject`、`version` 为管理目标；`schema_type`、`schema` 为业务明确提交的候选。
    /// 返回：领域兼容裁决；关闭或远端拒绝按原合同传播。
    async fn is_compatible(
        &self,
        subject: &str,
        version: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<bool, SchemaRegistryError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| SchemaRegistryError::Closed)?;
        guard
            .as_ref()
            .expect("admitted registry")
            .is_compatible(subject, version, schema_type, schema)
            .await
    }
    /// 业务作用：仅在显式开启 auto_register 时执行受管 schema 注册。
    /// 参数说明：`subject` 为命名主题；`schema_type`、`schema` 为待注册定义。
    /// 返回：远端确认的 ID；取消或传输失败不擅自重试未知结果。
    async fn register(
        &self,
        subject: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<SchemaId, SchemaRegistryError> {
        let guard = self
            .enter()
            .await
            .map_err(|_| SchemaRegistryError::Closed)?;
        guard
            .as_ref()
            .expect("admitted registry")
            .register(subject, schema_type, schema)
            .await
    }
}

impl Application {
    /// 业务作用：取得不要求启动 Kafka 消费者的命名 Registry client。
    /// 参数说明：`name` 为 schema_registries 名称。
    /// 返回：受管客户端；不存在时拒绝，不临时建连。
    pub async fn schema_registry(
        &self,
        name: &str,
    ) -> ApplicationResult<Arc<dyn SchemaRegistryClient>> {
        Ok(self
            .named_resource::<Arc<dyn SchemaRegistryClient>>(name)
            .await?
            .clone())
    }
}

/// 业务作用：输出不含 endpoint、认证材料或 schema 的固定装配错误。
/// 参数说明：`message` 为稳定摘要。
/// 返回：宿主 Prepare 错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}
