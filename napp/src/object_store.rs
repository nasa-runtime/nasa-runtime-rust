//! 命名对象存储的标准装配与调用期间所有权。

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId, PrepareContext};
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(feature = "object-store"), allow(dead_code))]
struct ObjectPlan {
    #[serde(default)]
    enabled: bool,
    provider: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    request_timeout_ms: Option<u64>,
    max_object_bytes: Option<usize>,
    require_checksum: Option<bool>,
    health_probe: Option<String>,
    #[serde(default)]
    critical: bool,
}

/// 业务作用：装配显式选择的有界命名对象存储；未启用时不读取凭据、不访问网络。
/// 参数说明：`context` 提供同代材料、统一指标目录和生命周期所有权。
/// 返回：启用计划满足启动门禁后返回待监督的健康任务；没有周期探测时为空，装配失败时拒绝启动。
pub(crate) async fn prepare(
    context: &mut PrepareContext<'_>,
) -> ApplicationResult<Option<crate::ApplicationFuture<'static>>> {
    let app = context.application().clone();
    let snapshot = app.config();
    let Some(value) = snapshot.value().get("object_stores") else {
        return Ok(None);
    };
    let plans: BTreeMap<String, ObjectPlan> = serde_json::from_value(value.clone())
        .map_err(|_| error("invalid object_stores structure"))?;
    if plans.len() > 64 {
        return Err(error("object store count exceeds 64"));
    }
    #[cfg(feature = "object-store")]
    let mut metrics = Vec::new();
    #[cfg(feature = "object-store")]
    let mut monitored = Vec::new();
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        if name.is_empty() || name.len() > 128 || name.trim() != name {
            return Err(error("invalid object store name"));
        }
        #[cfg(not(feature = "object-store"))]
        {
            let _ = (&name, &plan);
            return Err(error("object_stores requires the object-store feature"));
        }
        #[cfg(feature = "object-store")]
        {
            use crate::readiness::{reason, DependencyState, ReadinessPolicy};
            use std::sync::Arc;
            use std::time::{Duration, Instant};
            if plan.provider.as_deref() != Some("s3") {
                return Err(error("object store provider must be s3"));
            }
            let secrets = app.secrets();
            let credentials = naobject::S3Credentials {
                access_key_id: material(&secrets, plan.access_key.as_deref())?,
                secret_access_key: material(&secrets, plan.secret_key.as_deref())?,
                session_token: plan
                    .session_token
                    .as_deref()
                    .map(|value| material(&secrets, Some(value)))
                    .transpose()?,
            };
            let mut options = naobject::S3Options::new(
                plan.endpoint
                    .ok_or_else(|| error("object endpoint is required"))?,
                plan.bucket
                    .ok_or_else(|| error("object bucket is required"))?,
                plan.region
                    .ok_or_else(|| error("object region is required"))?,
                credentials,
            );
            if let Some(timeout) = plan.request_timeout_ms {
                if timeout == 0 || timeout > 300_000 {
                    return Err(error("object request timeout must be within 1..=300000 ms"));
                }
                options.request_timeout = Duration::from_millis(timeout);
            }
            if let Some(max) = plan.max_object_bytes {
                if max == 0 || max > 256 * 1024 * 1024 {
                    return Err(error("object size limit must be within 1..=256 MiB"));
                }
                options.max_object_bytes = max;
            }
            options.require_checksum = plan.require_checksum.unwrap_or(true);
            let store = Arc::new(
                naobject::S3ObjectStore::new(options)
                    .map_err(|_| error("invalid S3 object store options"))?,
            );
            let probed = match plan.health_probe.as_deref() {
                Some("head_bucket") => {
                    store
                        .health_check()
                        .await
                        .map_err(|_| error("S3 HEAD bucket readiness probe failed"))?;
                    true
                }
                Some("on_request") if !plan.critical => false,
                _ => {
                    return Err(error(
                        "object health_probe must select head_bucket or noncritical on_request",
                    ))
                }
            };
            let health = app.register_readiness(
                ComponentId::Application,
                Arc::<str>::from(format!("object-store:{name}")),
                ReadinessPolicy {
                    affects_ready: plan.critical,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    // HEAD 由周期任务持续刷新，过期时拒绝沿用旧证据；on_request 只描述最后一次调用。
                    stale_after: probed.then_some(Duration::from_secs(60)),
                },
            )?;
            health.observe(
                if probed {
                    DependencyState::Ready
                } else {
                    DependencyState::Degraded
                },
                if probed {
                    reason::HEALTHY
                } else {
                    reason::DEGRADED
                },
                Instant::now(),
            );
            metrics.push(store.clone());
            let owner = crate::managed_adapters::ManagedAdapter::new(store);
            context.activate(Box::new(crate::managed_adapters::AdapterShutdown(
                owner.clone(),
            )));
            let handle = Arc::new(ObjectHandle {
                owner,
                health,
                critical: plan.critical,
            });
            if probed {
                monitored.push(handle.clone());
            }
            let handle: Arc<dyn naobject::ObjectStore> = handle;
            context.register_resource(Some(&name), handle)?;
        }
    }
    #[cfg(feature = "object-store")]
    if !metrics.is_empty() {
        app.metrics_hub()
            .register_legacy_source_reserved(naobject::metrics::metrics_source_many(metrics), 64)
            .map_err(|_| error("object metrics registration conflicts with existing source"))?;
    }
    #[cfg(feature = "object-store")]
    if !monitored.is_empty() {
        // 只移交尚未轮询的任务；宿主在最终启动屏障后放行，失败回滚不会留下独立探测循环。
        return Ok(Some(Box::pin(monitor(monitored, app.subscribe_state()))));
    }
    Ok(None)
}

/// 业务作用：为全部命名 HEAD bucket 计划刷新远端证据，避免空闲资源依赖业务请求维持健康。
/// 参数说明：`handles` 是已通过启动探测的有界集合；`lifecycle` 接收全部原因触发的宿主状态转换。
/// 返回：停止时取消本轮只读探测并退出，由宿主监督器等待真实完成；各来源独立发布探测结果。
#[cfg(feature = "object-store")]
async fn monitor(
    handles: Vec<std::sync::Arc<ObjectHandle>>,
    mut lifecycle: tokio::sync::watch::Receiver<crate::ApplicationState>,
) -> ApplicationResult<()> {
    // 显式请求、信号、Batch 完成和任务失败都以状态转换收口，不能只监听业务主动发出的停止请求。
    let stopping = async move {
        loop {
            if matches!(
                *lifecycle.borrow_and_update(),
                crate::ApplicationState::Stopping
                    | crate::ApplicationState::Stopped
                    | crate::ApplicationState::Failed
            ) {
                return;
            }
            if lifecycle.changed().await.is_err() {
                return;
            }
        }
    };
    tokio::pin!(stopping);
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + std::time::Duration::from_secs(15),
        std::time::Duration::from_secs(15),
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = &mut stopping => return Ok(()),
            _ = interval.tick() => {},
        }
        // 来源并发且单次最多等待 5 秒，慢端点不能阻塞其它来源的证据；停机先取消只读工作再归还资源。
        tokio::select! {
            biased;
            _ = &mut stopping => return Ok(()),
            _ = futures_util::future::join_all(handles.iter().map(|handle| handle.probe())) => {},
        }
    }
}

/// 业务作用：产生固定结构错误，不将配置或凭据拼入错误正文。
/// 参数说明：`message` 为稳定原因。
/// 返回：阻止适配器进入 Ready 的准备错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}

/// 业务作用：复制同一秘密快照中的 S3 认证材料到自动清零容器。
/// 参数说明：`snapshot` 为当前代次；`locator` 必须是显式 secret:// 引用。
/// 返回：材料存在时返回独立受保护字节；缺失或明文配置拒绝。
#[cfg(feature = "object-store")]
fn material(
    snapshot: &nasecret::SecretSnapshot,
    locator: Option<&str>,
) -> ApplicationResult<nasecret::SecretBytes> {
    let id = locator
        .and_then(|value| value.strip_prefix("secret://"))
        .ok_or_else(|| error("object credentials require secret locators"))?;
    snapshot
        .get(id)
        .map(|value| nasecret::SecretBytes::new(value.expose().to_vec()))
        .ok_or_else(|| error("object credential material is missing"))
}

#[cfg(feature = "object-store")]
struct ObjectHandle {
    owner: std::sync::Arc<crate::managed_adapters::ManagedAdapter<naobject::S3ObjectStore>>,
    health: crate::ReadinessContributor,
    critical: bool,
}

#[cfg(feature = "object-store")]
impl ObjectHandle {
    /// 业务作用：用实际调用结局更新远端可用证据，按声明关键性决定是否摘流。
    /// 参数说明：`result` 为领域结果，未读取远端错误正文。
    /// 返回：保持原结果；远端或完整性失败使关键依赖 NotReady、非关键依赖 Degraded，本地拒绝不改观测。
    fn observe<T>(
        &self,
        result: Result<T, naobject::ObjectStoreError>,
    ) -> Result<T, naobject::ObjectStoreError> {
        use crate::readiness::{reason, DependencyState};
        match &result {
            Ok(_)
            | Err(
                naobject::ObjectStoreError::NotFound | naobject::ObjectStoreError::AlreadyExists,
            ) => self.health.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            ),
            Err(
                naobject::ObjectStoreError::Transport
                | naobject::ObjectStoreError::RemoteStatus(_)
                | naobject::ObjectStoreError::InvalidResponse
                | naobject::ObjectStoreError::MissingChecksum
                | naobject::ObjectStoreError::ChecksumMismatch,
            ) => {
                // 关键依赖的已知失败必须立即参与摘流，不能仅刷新 Degraded 时间而无限保留全局 Ready。
                self.health.observe(
                    if self.critical {
                        DependencyState::NotReady
                    } else {
                        DependencyState::Degraded
                    },
                    if self.critical {
                        reason::NOT_READY
                    } else {
                        reason::DEGRADED
                    },
                    std::time::Instant::now(),
                );
            }
            _ => {}
        }
        result
    }

    /// 业务作用：在受管调用边界内执行不产生对象副作用的周期探测。
    /// 参数说明：无。
    /// 返回：最多占用 5 秒，超时按传输失败更新健康；关闭后的旧 owner 不发起网络调用。
    async fn probe(&self) {
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let guard = self
                .owner
                .enter()
                .await
                .map_err(|_| naobject::ObjectStoreError::Closed)?;
            guard.as_ref().expect("admitted store").health_check().await
        })
        .await
        .unwrap_or(Err(naobject::ObjectStoreError::Transport));
        let _ = self.observe(result);
    }
}

#[cfg(feature = "object-store")]
#[async_trait::async_trait]
impl naobject::ObjectStore for ObjectHandle {
    /// 业务作用：在宿主完整调用责任内上传对象，发送后不自动重放。
    /// 参数说明：`request` 为有界正文及原有条件写约束。
    /// 返回：保持领域确认和未知结果语义；关闭后拒绝新调用。
    async fn put(
        &self,
        request: naobject::PutObject,
    ) -> Result<naobject::ObjectMetadata, naobject::ObjectStoreError> {
        let guard = self
            .owner
            .enter()
            .await
            .map_err(|_| naobject::ObjectStoreError::Closed)?;
        self.observe(guard.as_ref().expect("admitted store").put(request).await)
    }

    /// 业务作用：在宿主完整调用责任内下载并校验有界正文。
    /// 参数说明：`key` 为已经校验的对象身份。
    /// 返回：完整正文或原领域错误；关闭后拒绝新调用。
    async fn get(
        &self,
        key: &naobject::ObjectKey,
    ) -> Result<naobject::GetObject, naobject::ObjectStoreError> {
        let guard = self
            .owner
            .enter()
            .await
            .map_err(|_| naobject::ObjectStoreError::Closed)?;
        self.observe(guard.as_ref().expect("admitted store").get(key).await)
    }

    /// 业务作用：在宿主完整调用责任内读取对象元数据。
    /// 参数说明：`key` 为已经校验的对象身份。
    /// 返回：原领域元数据和错误；关闭后拒绝新调用。
    async fn head(
        &self,
        key: &naobject::ObjectKey,
    ) -> Result<naobject::ObjectMetadata, naobject::ObjectStoreError> {
        let guard = self
            .owner
            .enter()
            .await
            .map_err(|_| naobject::ObjectStoreError::Closed)?;
        self.observe(guard.as_ref().expect("admitted store").head(key).await)
    }

    /// 业务作用：在宿主完整调用责任内删除对象，不改变已存在的幂等边界。
    /// 参数说明：`key` 为已经校验的对象身份。
    /// 返回：远端确认或领域错误；关闭后拒绝新调用，不隐式重放。
    async fn delete(&self, key: &naobject::ObjectKey) -> Result<(), naobject::ObjectStoreError> {
        let guard = self
            .owner
            .enter()
            .await
            .map_err(|_| naobject::ObjectStoreError::Closed)?;
        self.observe(guard.as_ref().expect("admitted store").delete(key).await)
    }
}

#[cfg(feature = "object-store")]
impl crate::Application {
    /// 业务作用：取得当前应用命名对象存储，所有克隆共享永久关闭门禁。
    /// 参数说明：`name` 为 object_stores 中显式启用的资源名称。
    /// 返回：已验证的领域调用入口；未知名称或关闭时拒绝。
    pub async fn object_store(
        &self,
        name: &str,
    ) -> ApplicationResult<std::sync::Arc<dyn naobject::ObjectStore>> {
        Ok(self
            .named_resource::<std::sync::Arc<dyn naobject::ObjectStore>>(name)
            .await?
            .clone())
    }
}
