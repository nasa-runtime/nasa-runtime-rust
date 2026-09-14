//! Saga gRPC 出站目标的统一 mTLS、请求期限和多副本连接合同。

use super::{saga_error, saga_source_error};
use crate::{ApplicationPhase, ApplicationResult};
use futures_util::{future::poll_fn, stream::FuturesUnordered, StreamExt};
use nagrpc::codegen::tonic::{
    body::Body,
    codegen::{http, Bytes, Service},
};
use serde::Deserialize;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, Sleep};

/// 业务作用：保存已完成 URI、TLS 与 deadline 校验的可复用 gRPC channel。
#[derive(Clone)]
pub(super) struct ManagedGrpcTarget {
    pub(super) channel: ManagedGrpcChannel,
    timeout: Duration,
    replicas: Option<Arc<ManagedGrpcReplicas>>,
}

/// 业务作用：在全部 generated client 的公共传输边界限制就绪、响应头、正文及 trailers 的总等待时间。
#[derive(Clone)]
pub(super) struct ManagedGrpcChannel {
    active: Arc<RwLock<Vec<nagrpc::Channel>>>,
    next: Arc<AtomicUsize>,
    timeout: Duration,
    discovery: Option<Arc<ManagedGrpcRefresh>>,
}

impl ManagedGrpcChannel {
    /// 业务作用：建立可原子切换已验证副本集合的受管调用入口。
    /// 参数说明：`channel` 是单目标连接，空值关闭投递；`timeout` 是完整调用期限。
    /// 返回：共享连接快照的入口，克隆者在下一次调用时读取同一快照。
    fn new(channel: Option<nagrpc::Channel>, timeout: Duration) -> Self {
        Self {
            active: Arc::new(RwLock::new(channel.into_iter().collect())),
            next: Arc::new(AtomicUsize::new(0)),
            timeout,
            discovery: None,
        }
    }
}

impl Service<http::Request<Body>> for ManagedGrpcChannel {
    type Response = http::Response<Body>;
    type Error = nagrpc::Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// 业务作用：把底层就绪等待移入有期限的调用，避免 generated client 在计时开始前无限等待。
    /// 参数说明：`_context` 是执行器唤醒上下文，本入口不在此阶段等待网络。
    /// 返回：允许进入调用阶段；连接不可用仍由有期限的调用返回错误。
    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// 业务作用：用同一截止时刻约束一次受管 RPC，并把剩余预算传播到远端。
    /// 参数说明：`request` 保留 generated client 的方法、认证与追踪元数据。
    /// 返回：带同一期限的响应正文；无健康副本、连接失败或到期返回非成功状态，不能充当持久收据。
    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let timeout = match managed_grpc_request_timeout(request.headers(), self.timeout) {
            Ok(timeout) => timeout,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let deadline = Instant::now() + timeout;
        let active = Arc::clone(&self.active);
        let next = Arc::clone(&self.next);
        let discovery = self.discovery.clone();
        Box::pin(async move {
            if let Some(discovery) = discovery {
                // 发现、逐成员 health、连接与正文共用原期限；失败不能使用已经撤下的旧池。
                discovery.refresh(&active, deadline).await?;
            }
            // 路由撤下后克隆入口也必须停止新投递，不能回退到未通过 health 的原始成员集合。
            let mut channel = {
                let members = active
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if members.is_empty() {
                    return Err(nagrpc::Status::unavailable(
                        "managed Saga gRPC has no serving replica",
                    ));
                }
                // 复用逐成员探测过的真实连接，共享游标使已确认健康的成员都能承担新请求。
                members[next.fetch_add(1, Ordering::Relaxed) % members.len()].clone()
            };
            let response = tokio::time::timeout_at(deadline, async {
                poll_fn(|context| channel.poll_ready(context))
                    .await
                    .map_err(|error| nagrpc::Status::from_error(Box::new(error)))?;
                let mut budget = nagrpc::Request::new(());
                budget.set_timeout(deadline.saturating_duration_since(Instant::now()));
                request
                    .headers_mut()
                    .extend(budget.into_parts().0.into_headers());
                channel
                    .call(request)
                    .await
                    .map_err(|error| nagrpc::Status::from_error(Box::new(error)))
            })
            .await
            .map_err(|_| {
                nagrpc::Status::deadline_exceeded("managed Saga gRPC request deadline elapsed")
            })??;
            // 响应头并不证明收据已完整到达；正文与最终 trailers 继续使用原截止时刻，不能重置预算。
            Ok(response.map(|body| {
                Body::new(ManagedGrpcBody {
                    body,
                    deadline: Box::pin(tokio::time::sleep_until(deadline)),
                    finished: false,
                })
            }))
        })
    }
}

/// 业务作用：复用当前成员与凭据快照的已探测连接，任一变化时完整重建候选池。
struct ManagedGrpcRefresh {
    resolver: Option<Arc<dyn GrpcDiscoveryResolver>>,
    endpoints: Vec<String>,
    credential: Arc<ManagedGrpcCredentialMaterial>,
    timeout: Duration,
    snapshot: tokio::sync::Mutex<Option<ManagedGrpcSnapshot>>,
    completed: AtomicUsize,
}

/// 业务作用：把成员、凭据及一轮完整健康结果绑定，重叠调用只能复用同一权威下的结果。
struct ManagedGrpcSnapshot {
    routes: Vec<(String, String)>,
    credential: Arc<ManagedGrpcCredentialMaterial>,
    target: ManagedGrpcTarget,
    completion: Option<usize>,
}

/// 业务作用：让协议连接池只接受受信发现解析器产生的完整端点代次，避免自行解释业务配置。
pub(super) trait GrpcDiscoveryResolver: Send + Sync {
    /// 业务作用：在请求原期限内取得同一次发现的成员身份代次与完整端点。
    /// 参数说明：`deadline` 是调用者原截止时刻。
    /// 返回：受信非空端点集合；空集或发现失败不能提供投递权威。
    fn routes(
        &self,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = ApplicationResult<Vec<(String, String)>>> + Send + '_>>;
}

impl ManagedGrpcRefresh {
    /// 业务作用：逐请求复验发现与凭据，合并同代重叠探测并串行发布可用 channel。
    /// 参数说明：`active` 是所有克隆调用入口共享的连接集合，`deadline` 是本次请求原期限。
    /// 返回：至少一个当前健康成员时成功；持锁刷新失败时关闭投递，等锁超时只终止该等待者。
    async fn refresh(
        &self,
        active: &Arc<RwLock<Vec<nagrpc::Channel>>>,
        deadline: Instant,
    ) -> Result<(), nagrpc::Status> {
        // 进入刷新时记录已完成轮次；等锁期间完成的同代结果可以共享，后续独立请求必须重新探测。
        let observed = self.completed.load(Ordering::Acquire);
        let mut snapshot = tokio::time::timeout_at(deadline, self.snapshot.lock())
            .await
            .map_err(|_| nagrpc::Status::deadline_exceeded("Saga discovery deadline elapsed"))?;
        // 锁和定时器同时就绪时仍以原期限为准，迟到的等待者不能取得发布或撤下路由的权力。
        if Instant::now() >= deadline {
            return Err(nagrpc::Status::deadline_exceeded(
                "Saga discovery deadline elapsed",
            ));
        }
        // 只有持锁者能发布或撤下集合，短期限等待者不能清空另一请求刚确认的路由。
        let result = tokio::time::timeout_at(
            deadline,
            self.refresh_snapshot(&mut snapshot, observed, deadline),
        )
        .await
        .unwrap_or_else(|_| {
            Err(nagrpc::Status::deadline_exceeded(
                "Saga discovery deadline elapsed",
            ))
        });
        if result.is_ok() {
            let target = &snapshot
                .as_ref()
                .expect("managed gRPC snapshot is confirmed")
                .target;
            let channels = target
                .channel
                .active
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            *active
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = channels;
        } else {
            // 失败与撤下在同一锁内完成，不能让较早的失败覆盖较晚的成功发布。
            if let Some(current) = snapshot.as_mut() {
                current.completion = None;
            }
            active
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        result
    }

    /// 业务作用：在发布锁内复验同代健康证据，成员或凭据改变时丢弃旧证据并重新握手探测。
    /// 参数说明：`snapshot` 是待发布快照；`observed` 为请求进入时已完成轮次；`deadline` 为原期限。
    /// 返回：成功结果可供重叠请求共享；失败、取消或到期不留下可共享结果，下一持锁者可重新探测。
    async fn refresh_snapshot(
        &self,
        snapshot: &mut Option<ManagedGrpcSnapshot>,
        observed: usize,
        deadline: Instant,
    ) -> Result<(), nagrpc::Status> {
        let unavailable = || nagrpc::Status::unavailable("Saga discovery has no serving replica");
        let (mut routes, mut credential) =
            self.authority(deadline).await.map_err(|_| unavailable())?;
        loop {
            if snapshot.as_ref().is_none_or(|previous| {
                previous.routes != routes || !Arc::ptr_eq(&previous.credential, &credential)
            }) {
                // 旧权威的探测不能用于新成员或新信任窗口；构造失败和取消也不得留下可复用的旧结果。
                *snapshot = None;
                let endpoints = routes
                    .iter()
                    .map(|(_, endpoint)| endpoint.clone())
                    .collect::<Vec<_>>();
                let mut material = (*credential).clone();
                material.source = None;
                let target = build_managed_grpc_targets(&endpoints, self.timeout, &material)
                    .map_err(|_| unavailable())?;
                *snapshot = Some(ManagedGrpcSnapshot {
                    routes,
                    credential,
                    target,
                    completion: None,
                });
            }
            let current = snapshot
                .as_mut()
                .expect("managed gRPC snapshot is prepared");
            if Instant::now() >= deadline {
                return Err(nagrpc::Status::deadline_exceeded(
                    "Saga discovery deadline elapsed",
                ));
            }
            if current
                .completion
                .is_some_and(|completed| completed != observed)
            {
                return Ok(());
            }
            // 清除旧完成标记后由当前请求驱动探测；取消会释放锁和网络 future，不留下后台刷新任务。
            current.completion = None;
            let result = probe_managed_grpc_target_before(
                &current.target,
                deadline,
                ApplicationPhase::Running,
                true,
            )
            .await
            .map_err(|_| unavailable());
            // 网络等待期间发现可能撤下成员、旧 CA 窗口也可能到期；发布前必须再次取得完整权威。
            (routes, credential) = self.authority(deadline).await.map_err(|_| unavailable())?;
            if current.routes != routes || !Arc::ptr_eq(&current.credential, &credential) {
                continue;
            }
            // 到期只属于当前调用者，不能把短请求的期限失败传播给仍有预算的等待者。
            if Instant::now() >= deadline {
                return Err(nagrpc::Status::deadline_exceeded(
                    "Saga discovery deadline elapsed",
                ));
            }
            result?;
            let completed = self
                .completed
                .fetch_add(1, Ordering::Release)
                .wrapping_add(1);
            current.completion = Some(completed);
            return Ok(());
        }
    }

    /// 业务作用：取得当前受信成员与凭据，避免发现等待消耗凭据有效窗口后仍选用旧材料。
    /// 参数说明：`deadline` 限制发现查询，属于业务请求原期限。
    /// 返回：完整成员与不可变凭据快照；发现失败时不提供投递权威。
    async fn authority(
        &self,
        deadline: Instant,
    ) -> ApplicationResult<(Vec<(String, String)>, Arc<ManagedGrpcCredentialMaterial>)> {
        let routes = match &self.resolver {
            Some(resolver) => resolver.routes(deadline).await?,
            None => self
                .endpoints
                .iter()
                .map(|endpoint| (endpoint.clone(), endpoint.clone()))
                .collect(),
        };
        let credential = self
            .credential
            .source
            .as_ref()
            .map(|source| source.credential())
            .unwrap_or_else(|| self.credential.clone());
        Ok((routes, credential))
    }
}

/// 业务作用：建立每次调用都复验受信服务发现的 gRPC 入口，初始没有投递权威。
/// 参数说明：`resolver` 固定服务身份与地址政策，`timeout` 为完整预算，`credential` 为客户端 mTLS 材料。
/// 返回：尚未开放投递的目标；首次调用通过发现和逐成员 health 后发布可用池。
pub(super) fn build_discovered_grpc_target(
    resolver: Arc<dyn GrpcDiscoveryResolver>,
    timeout: Duration,
    credential: ManagedGrpcCredentialMaterial,
) -> ManagedGrpcTarget {
    build_refreshing_grpc_target(Vec::new(), Some(resolver), timeout, credential)
}

/// 业务作用：为固定地址或发现成员建立统一凭据轮换入口，发布前必须取得当前 TLS 与 health 证据。
/// 参数说明：`endpoints` 与 `resolver` 二选一；`timeout` 为总预算，`credential` 保留当前凭据来源。
/// 返回：按请求原子刷新成员与凭据的目标，未确认的集合不参与投递。
fn build_refreshing_grpc_target(
    endpoints: Vec<String>,
    resolver: Option<Arc<dyn GrpcDiscoveryResolver>>,
    timeout: Duration,
    credential: ManagedGrpcCredentialMaterial,
) -> ManagedGrpcTarget {
    let mut channel = ManagedGrpcChannel::new(None, timeout);
    channel.discovery = Some(Arc::new(ManagedGrpcRefresh {
        resolver,
        endpoints,
        credential: Arc::new(credential),
        timeout,
        snapshot: tokio::sync::Mutex::new(None),
        completed: AtomicUsize::new(0),
    }));
    ManagedGrpcTarget {
        channel,
        timeout,
        replicas: None,
    }
}

/// 业务作用：把请求已有的更短 gRPC 期限与受管传输上限合并，避免转发时延长调用者预算。
/// 参数说明：`headers` 携带标准 grpc-timeout，`configured` 是受管目标的最大等待时间。
/// 返回：两者中较短的期限；没有请求期限时使用配置值，非法协议值返回 InvalidArgument。
fn managed_grpc_request_timeout(
    headers: &http::HeaderMap,
    configured: Duration,
) -> Result<Duration, nagrpc::Status> {
    let Some(value) = headers.get("grpc-timeout") else {
        return Ok(configured);
    };
    // 非法期限不能退回默认预算，否则可能延长调用者明确要求的等待边界。
    let value = value
        .to_str()
        .map_err(|_| nagrpc::Status::invalid_argument("invalid gRPC timeout"))?;
    if !(2..=9).contains(&value.len())
        || !value.as_bytes()[..value.len() - 1]
            .iter()
            .all(u8::is_ascii_digit)
    {
        return Err(nagrpc::Status::invalid_argument("invalid gRPC timeout"));
    }
    let amount: u64 = value[..value.len() - 1]
        .parse()
        .map_err(|_| nagrpc::Status::invalid_argument("invalid gRPC timeout"))?;
    let timeout = match value.as_bytes()[value.len() - 1] {
        b'H' => Duration::from_secs(amount * 3600),
        b'M' => Duration::from_secs(amount * 60),
        b'S' => Duration::from_secs(amount),
        b'm' => Duration::from_millis(amount),
        b'u' => Duration::from_micros(amount),
        b'n' => Duration::from_nanos(amount),
        _ => return Err(nagrpc::Status::invalid_argument("invalid gRPC timeout")),
    };
    Ok(configured.min(timeout))
}

/// 业务作用：保留 gRPC data/trailers 流式处理，并在原调用期限到达时终止不完整收据。
struct ManagedGrpcBody {
    body: Body,
    deadline: Pin<Box<Sleep>>,
    finished: bool,
}

impl http_body::Body for ManagedGrpcBody {
    type Data = Bytes;
    type Error = nagrpc::Status;

    /// 业务作用：在交付每个正文或 trailers frame 前复验原期限，迟到内容不能成为成功收据。
    /// 参数说明：`context` 用于登记网络 frame 与期限计时器的唤醒。
    /// 返回：原 frame 或待唤醒状态；到期返回 DeadlineExceeded 并释放剩余响应流，完整结束后返回空。
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, nagrpc::Status>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        // 到期先于 frame 可用性裁决，防止已经迟到的 Committed 或 trailers 被误认作及时确认。
        if self.deadline.as_mut().poll(context).is_ready() {
            self.finished = true;
            self.body = Body::empty();
            return Poll::Ready(Some(Err(nagrpc::Status::deadline_exceeded(
                "managed Saga gRPC receipt deadline elapsed",
            ))));
        }
        let next = Pin::new(&mut self.body).poll_frame(context);
        if matches!(next, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.finished = true;
        }
        next
    }
}

/// 业务作用：保存独立的成员探测连接，并串行发布每轮确认的可服务副本集合。
struct ManagedGrpcReplicas {
    members: Vec<ManagedGrpcChannel>,
    published: tokio::sync::Mutex<Vec<usize>>,
}

/// 业务作用：解析 gRPC client mTLS secret 的受控 JSON 形态，为同一计划的出站 channel 冻结认证材料。
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManagedGrpcCredentialMaterial {
    pub(super) ca_certificate_pem: String,
    pub(super) identity_certificate_pem: String,
    pub(super) identity_private_key_pem: String,
    pub(super) domain_name: Option<String>,
    #[serde(skip)]
    pub(super) source: Option<Arc<dyn GrpcCredentialSource>>,
}

/// 业务作用：把长期 client 句柄绑定到同代证书、私钥和信任根快照。
pub(super) trait GrpcCredentialSource: Send + Sync {
    /// 业务作用：读取一次新 RPC 使用的完整 mTLS 材料，旧请求保留原连接身份。
    /// 参数说明：无。
    /// 返回：已经准备校验的不可变材料；同一代返回同一 Arc，材料或重叠窗口变化时替换 Arc。
    fn credential(&self) -> Arc<ManagedGrpcCredentialMaterial>;
}

/// 业务作用：在配置发布前完成客户端证书、私钥和 CA 的语法及密码学匹配检查。
/// 参数说明：`credential` 为未发布的完整客户端材料，不发起网络连接。
/// 返回：材料可构造 mTLS channel 时成功；非法 PEM、私钥或 CA 拒绝候选。
pub(super) fn validate_grpc_credential(
    credential: &ManagedGrpcCredentialMaterial,
) -> ApplicationResult<()> {
    nagrpc::client_certificate_principal(credential.identity_certificate_pem.as_bytes()).map_err(
        |_| {
            saga_error(
                ApplicationPhase::Running,
                "Saga gRPC client identity is invalid or expired",
            )
        },
    )?;
    build_managed_grpc_endpoint("https://localhost", Duration::from_secs(5), credential).map(|_| ())
}

/// 业务作用：把一个 HTTPS endpoint 与 client mTLS 材料冻结为带 deadline 的惰性复用 channel。
///
/// 参数说明：`endpoint` 是受信发现结果，`timeout` 限制连接建立、已连接请求及完整 health 探测，`credential` 来自 SecretSnapshot。
///
/// 返回：URI 与 TLS 配置可构造时返回 target；明文 endpoint 或证书参数错误拒绝 Ready。
pub(super) fn build_managed_grpc_target(
    endpoint: &str,
    timeout: Duration,
    credential: &ManagedGrpcCredentialMaterial,
) -> ApplicationResult<ManagedGrpcTarget> {
    if credential.source.is_some() {
        build_managed_grpc_endpoint(endpoint, timeout, credential)?;
        return Ok(build_refreshing_grpc_target(
            vec![endpoint.to_owned()],
            None,
            timeout,
            credential.clone(),
        ));
    }
    Ok(ManagedGrpcTarget {
        channel: ManagedGrpcChannel::new(
            Some(build_managed_grpc_endpoint(endpoint, timeout, credential)?.connect_lazy()),
            timeout,
        ),
        timeout,
        replicas: None,
    })
}

/// 业务作用：为单实例或平衡 channel 构造同一 HTTPS、mTLS 与 deadline 合同。
/// 参数说明：`endpoint` 是已授权目标，`timeout` 同时限制连接建立和已连接请求，`credential` 是冻结认证材料。
/// 返回：可供 channel 使用的 Endpoint；明文地址、非法 URI 或 TLS 配置拒绝发布。
fn build_managed_grpc_endpoint(
    endpoint: &str,
    timeout: Duration,
    credential: &ManagedGrpcCredentialMaterial,
) -> ApplicationResult<nagrpc::Endpoint> {
    // 出站身份必须由 TLS 对端证书证明，不能向明文地址发送受管 Saga 请求。
    if !endpoint.starts_with("https://") {
        return Err(saga_error(
            ApplicationPhase::Ready,
            "managed Saga gRPC endpoint must use HTTPS with mTLS",
        ));
    }
    let mut tls = nagrpc::ClientTlsConfig::new()
        .ca_certificate(nagrpc::Certificate::from_pem(
            credential.ca_certificate_pem.as_bytes(),
        ))
        .identity(nagrpc::Identity::from_pem(
            credential.identity_certificate_pem.as_bytes(),
            credential.identity_private_key_pem.as_bytes(),
        ));
    if let Some(domain_name) = credential.domain_name.as_deref() {
        tls = tls.domain_name(domain_name.to_owned());
    }
    let endpoint = nagrpc::Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga gRPC endpoint URI is invalid",
                error,
            )
        })?
        // 连接建立包含 TLS 握手，必须有独立上限，避免惰性连接长期占用探测和投递资源。
        .connect_timeout(timeout)
        .timeout(timeout)
        .tls_config(tls)
        .map_err(|error| {
            saga_source_error(
                ApplicationPhase::Ready,
                "managed Saga gRPC client TLS configuration is invalid",
                error,
            )
        })?;
    Ok(endpoint)
}

/// 业务作用：在发布 Ready 或切换 route 前通过标准 gRPC health 复验 mTLS channel 与远端服务状态。
///
/// 参数说明：`target` 已冻结 URI、证书与请求 deadline，`phase` 标识首次门禁或运行期 route 切换。
///
/// 返回：单目标 Serving 或副本集合中至少一个成员 Serving 时成功；未确认成员不参与投递，全部失败时撤下集合。
pub(super) async fn probe_managed_grpc_target(
    target: &ManagedGrpcTarget,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let deadline = Instant::now() + target.timeout;
    probe_managed_grpc_target_before(target, deadline, phase, false).await
}

/// 业务作用：在调用原期限内收集 Serving 成员，投递前复验取得健康证据后为实际 RPC 留出预算。
/// 参数说明：`target` 是受信成员集合，`deadline` 是不可延长的原期限，`phase` 标识运行阶段；
/// `reserve_delivery_budget` 表示本次探测之后还需完成同一预算内的业务调用。
/// 返回：仅发布本轮已确认 Serving 的成员；无成员通过时撤下集合并拒绝投递，未完成探测可在下次重试。
async fn probe_managed_grpc_target_before(
    target: &ManagedGrpcTarget,
    deadline: Instant,
    phase: ApplicationPhase,
    reserve_delivery_budget: bool,
) -> ApplicationResult<()> {
    let Some(replicas) = target.replicas.as_ref() else {
        return probe_managed_grpc_channel(&target.channel, deadline, phase).await;
    };
    // 串行复验同一集合，避免较早开始的探测在新结果发布后覆盖成员状态；排队也消耗本轮预算。
    let mut published = tokio::time::timeout_at(deadline, replicas.published.lock())
        .await
        .map_err(|error| {
            saga_source_error(
                phase,
                "managed Saga gRPC health probe deadline elapsed",
                error,
            )
        })?;
    let mut probes: FuturesUnordered<_> = replicas
        .members
        .iter()
        .enumerate()
        .map(|(index, channel)| async move {
            (
                index,
                probe_managed_grpc_channel(channel, deadline, phase).await,
            )
        })
        .collect();
    let mut serving = Vec::new();
    let mut failure = None;
    let mut collection_deadline = deadline;
    // 逐成员并发检查且共享绝对期限；单个 NotServing 或停滞连接不能代表另一个成员的健康状态。
    while let Ok(Some((index, result))) =
        tokio::time::timeout_at(collection_deadline, probes.next()).await
    {
        match result {
            Ok(()) => {
                if reserve_delivery_budget && serving.is_empty() {
                    // 已有可投递成员后，剩余探测最多使用当前余量的一半；停滞成员不能耗尽收据读取预算。
                    // 只收紧一次，后续成功成员不能反复延后边界；单健康成员冷启动仍可使用原期限取得证据。
                    let now = Instant::now();
                    collection_deadline = deadline - deadline.saturating_duration_since(now) / 2;
                }
                serving.push(index);
            }
            Err(error) => failure = Some(error),
        }
    }
    // 到达收集边界便取消未完成的 health 调用；这些成员不进入发布集合，其复用连接留给后续轮次复验。
    drop(probes);
    serving.sort_unstable();
    if *published != serving {
        let next = serving
            .iter()
            .flat_map(|&index| {
                replicas.members[index]
                    .active
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .collect();
        // 完整可用集合一次发布；未确认副本不能继续承接新投递，已在途请求仍按原期限与收据裁决。
        *target
            .channel
            .active
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        *published = serving;
    }
    if published.is_empty() {
        // 缺少任何 Serving 证据时保持路由关闭，由调用方撤销 Catalog 资格并在后续轮次重试。
        return Err(
            failure.unwrap_or_else(|| saga_error(phase, "managed Saga gRPC target is not serving"))
        );
    }
    Ok(())
}

/// 业务作用：通过独立成员连接取得该成员的标准 health 证据，避免平衡抽样替代成员状态。
/// 参数说明：`channel` 是冻结 mTLS 的单成员入口，`deadline` 是整轮共同期限，`phase` 标识门禁阶段。
/// 返回：完整标准响应为 Serving 时成功；超时、协议错误或非服务态均不能作为可用成员证据。
async fn probe_managed_grpc_channel(
    channel: &ManagedGrpcChannel,
    deadline: Instant,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let response = tokio::time::timeout_at(deadline, async {
        let mut request = nagrpc::Request::new(nagrpc::health::HealthCheckRequest {
            service: String::new(),
        });
        request.set_timeout(deadline.saturating_duration_since(Instant::now()));
        nagrpc::health::health_client::HealthClient::new(channel.clone())
            .check(request)
            .await
    })
    .await
    .map_err(|error| {
        saga_source_error(
            phase,
            "managed Saga gRPC health probe deadline elapsed",
            error,
        )
    })?
    .map_err(|error| saga_source_error(phase, "managed Saga gRPC health probe failed", error))?
    .into_inner();
    // 连接与协议成功还不能证明业务已就绪，只有 Serving 可纳入投递集合。
    if response.status != nagrpc::health::ServingStatus::Serving as i32 {
        return Err(saga_error(phase, "managed Saga gRPC target is not serving"));
    }
    Ok(())
}

/// 业务作用：保存同一步骤的合法副本，在逐成员 health 复验后发布复用原连接的轮询分流集合。
/// 参数说明：`endpoints` 是完整步骤身份筛选并经地址政策验证的 URI 集合，`timeout` 与 `credential` 对全部副本一致。
/// 返回：非空集合返回尚未开放投递的目标；空集合或非法地址拒绝构造，后续探测决定可用成员。
pub(super) fn build_managed_grpc_targets(
    endpoints: &[String],
    timeout: Duration,
    credential: &ManagedGrpcCredentialMaterial,
) -> ApplicationResult<ManagedGrpcTarget> {
    // 没有步骤成员就无法取得 Serving 证据，拒绝构造可被误发布的空目标。
    if endpoints.is_empty() {
        return Err(saga_error(
            ApplicationPhase::Running,
            "managed Saga gRPC step has no replica",
        ));
    }
    if credential.source.is_some() {
        for endpoint in endpoints {
            build_managed_grpc_endpoint(endpoint, timeout, credential)?;
        }
        return Ok(build_refreshing_grpc_target(
            endpoints.to_vec(),
            None,
            timeout,
            credential.clone(),
        ));
    }
    let members = endpoints
        .iter()
        .map(|endpoint| build_managed_grpc_endpoint(endpoint, timeout, credential))
        .collect::<ApplicationResult<Vec<_>>>()?
        .into_iter()
        .map(|endpoint| ManagedGrpcChannel::new(Some(endpoint.connect_lazy()), timeout))
        .collect();
    // 构造成功仅证明地址和证书合同合法，尚无应用健康证据时不得提前将任一成员用于业务投递。
    Ok(ManagedGrpcTarget {
        channel: ManagedGrpcChannel::new(None, timeout),
        timeout,
        replicas: Some(Arc::new(ManagedGrpcReplicas {
            members,
            published: tokio::sync::Mutex::new(Vec::new()),
        })),
    })
}
