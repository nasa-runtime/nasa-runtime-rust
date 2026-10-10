//! 按当前发现证据提交每个 RPC 的 Tower Service。连接和双向正文由同一 discovery owner 回收。

mod body;
mod pool;
use nabudget::RequestBudget;
use nadisc_runtime::DiscoveryHandle;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tonic::{
    body::Body,
    codegen::http::{Request, Response},
    Status,
};
use tower_service::Service;

/// gRPC 调用端的有限资源与连接存活策略，排队、建连和正文共用 request_timeout。
#[derive(Clone)]
pub struct DiscoveryClientOptions {
    pub max_inflight: usize,
    pub max_connections: usize,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_connection_age: Duration,
    pub initial_connect_backoff: Duration,
    pub max_connect_backoff: Duration,
    pub max_frame_bytes: usize,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub tls_server_name: Option<String>,
    pub allowed_authorities: std::collections::BTreeSet<String>,
    pub protocol: Option<DiscoveryProtocol>,
}

/// 无协议 metadata 的 DNS/Kubernetes 服务采用完整显式绑定，端口来自当前发现实例。
#[derive(Clone)]
pub struct DiscoveryProtocol {
    pub authority: String,
    pub tls_mode: DiscoveryTlsMode,
}

#[derive(Clone, Copy)]
pub enum DiscoveryTlsMode {
    Disabled,
    Server,
    Mutual,
}

impl DiscoveryTlsMode {
    /// 业务作用：将调用方冻结的 TLS 政策投影为与注册 metadata 一致的协议值。
    /// 参数说明：无。
    /// 返回：固定模式名称，不从端口或服务名推测 TLS。
    fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Server => "server",
            Self::Mutual => "mutual",
        }
    }
}
impl Default for DiscoveryClientOptions {
    /// 业务作用：提供有限并发和连接容量，明文必须与实例明确声明一致。
    /// 参数说明：无。
    /// 返回：默认五秒总调用期限、三十秒空闲回收和五分钟连接年龄。
    fn default() -> Self {
        Self {
            max_inflight: 128,
            max_connections: 16,
            request_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            max_connection_age: Duration::from_secs(300),
            initial_connect_backoff: Duration::from_millis(100),
            max_connect_backoff: Duration::from_secs(5),
            max_frame_bytes: 8 * 1024 * 1024,
            tls: None,
            tls_server_name: None,
            allowed_authorities: Default::default(),
            protocol: None,
        }
    }
}

struct Shared {
    discovery: DiscoveryHandle,
    service: String,
    options: DiscoveryClientOptions,
    slots: Arc<Semaphore>,
    dials: Arc<Semaphore>,
    pool: Mutex<pool::Pool>,
    cursor: std::sync::atomic::AtomicUsize,
}
type PendingReservation = Pin<Box<dyn Future<Output = Result<Arc<Reservation>, Status>> + Send>>;

/// 可直接交给 generated Client::new 的动态调用器；clone 共享池但不复制 poll_ready 的预约。
pub struct DiscoveryGrpcService {
    shared: Arc<Shared>,
    budget: Option<RequestBudget>,
    selected: Option<nadisc::Instance>,
    admission: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ready: Option<Arc<Reservation>>,
    waiting: Option<PendingReservation>,
}
impl Clone for DiscoveryGrpcService {
    /// 业务作用：共享连接和总容量，保留每个 Tower Service 对象独立的就绪预约。
    /// 参数说明：无。
    /// 返回：新对象没有消耗或复制已有预约。
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            budget: self.budget.clone(),
            selected: self.selected.clone(),
            admission: self.admission.clone(),
            ready: None,
            waiting: None,
        }
    }
}
impl Drop for DiscoveryGrpcService {
    /// 业务作用：业务放弃一个服务对象时立即归还尚未用于 RPC 的容量预约。
    /// 参数说明：无。
    /// 返回：其他 clone 的调用不受影响。
    fn drop(&mut self) {
        if let Some(ready) = self.ready.take() {
            ready.release();
        }
    }
}

impl DiscoveryGrpcService {
    /// 业务作用：绑定精确发现连接与服务，避免不同环境共用连接池。
    /// 参数说明：`discovery` 是只读受管句柄，`service` 为明确服务，`options` 固定容量及 TLS 政策。
    /// 返回：可直接注入 generated client 的 Service；非法预算和 TLS 身份政策拒绝构造。
    pub fn new(
        discovery: DiscoveryHandle,
        service: impl Into<String>,
        options: DiscoveryClientOptions,
    ) -> Result<Self, Status> {
        let service = service.into();
        if service.is_empty()
            || service.len() > 512
            || options.max_inflight == 0
            || options.max_inflight > 65536
            || options.max_connections == 0
            || options.max_connections > 1024
            || options.max_frame_bytes == 0
            || options.max_frame_bytes > 16 * 1024 * 1024
            || [
                options.request_timeout,
                options.idle_timeout,
                options.max_connection_age,
                options.initial_connect_backoff,
                options.max_connect_backoff,
            ]
            .iter()
            .any(|value| value.is_zero() || *value > Duration::from_secs(3600))
            || options.initial_connect_backoff > options.max_connect_backoff
            || (options.tls.is_some() && options.allowed_authorities.is_empty())
        {
            return Err(Status::invalid_argument(
                "invalid managed gRPC client configuration",
            ));
        }
        Ok(Self {
            shared: Arc::new(Shared {
                discovery,
                service,
                slots: Arc::new(Semaphore::new(options.max_inflight)),
                dials: Arc::new(Semaphore::new(options.max_connections)),
                options,
                pool: Mutex::new(pool::Pool::default()),
                cursor: std::sync::atomic::AtomicUsize::new(0),
            }),
            budget: None,
            selected: None,
            admission: None,
            ready: None,
            waiting: None,
        })
    }

    /// 业务作用：在 poll_ready 前绑定原调用链预算，让排队也计入同一期限。
    /// 参数说明：`budget` 为调用方原预算，子请求取消不会反向取消父预算。
    /// 返回：清除当前对象未使用的预约，后续调用服从新预算。
    pub fn with_budget(mut self, budget: RequestBudget) -> Self {
        if let Some(ready) = self.ready.take() {
            ready.release();
        }
        self.waiting = None;
        self.budget = Some(budget);
        self
    }

    /// 业务作用：将业务层已认证的单个实例绑定到本次调用，连接复用仍须取得当前发现许可。
    /// 参数说明：`instance` 必须来自该服务的完整观察，所有身份和协议字段按原样比较。
    /// 返回：同地址的另一实例、元数据变化或宽限证据均不能替代该候选。
    pub fn for_instance(mut self, instance: nadisc::Instance) -> Self {
        if let Some(ready) = self.ready.take() {
            ready.release();
        }
        self.waiting = None;
        self.selected = Some(instance);
        self
    }

    /// 业务作用：在实际流提交点复验业务身份资格，避免握手等待期间的凭据撤销被旧连接绕过。
    /// 参数说明：`check` 必须是无等待、不重入发现 owner 的同步判断，不得执行网络操作。
    /// 返回：新请求只有在发现资格和此判断同时成立时才提交，既有请求仍服从原预算。
    pub fn with_admission(mut self, check: Arc<dyn Fn() -> bool + Send + Sync>) -> Self {
        self.admission = Some(check);
        self
    }
}

struct Reservation {
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    budget: RequestBudget,
    done: CancellationToken,
}
impl Reservation {
    /// 业务作用：读取预约是否仍有效，超时后不能继续占用容量或发送。
    /// 参数说明：无。
    /// 返回：拥有额度且原预算未结束才有效。
    fn valid(&self) -> bool {
        !self.budget.is_exhausted()
            && self
                .permit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
    }
    /// 业务作用：结束调用或过期预约，取消相关双向正文并归还容量。
    /// 参数说明：无。
    /// 返回：幂等关闭本次调用，不影响父预算和其他 RPC。
    fn release(&self) {
        self.budget.cancel();
        self.permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.done.cancel();
    }
}
struct Release(Arc<Reservation>);
impl Drop for Release {
    /// 业务作用：在正常完成、取消、关闭或超时时统一释放预约。
    /// 参数说明：无。
    /// 返回：释放后不允许继续使用本次调用许可。
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Service<Request<Body>> for DiscoveryGrpcService {
    type Response = Response<Body>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Body>, Status>> + Send>>;

    /// 业务作用：预约一次 RPC 的容量，等待和未使用预约均受原预算及 owner 关闭约束。
    /// 参数说明：`cx` 提供当前任务唤醒器。
    /// 返回：Ready 表示一个有效预约；耗尽预算或 owner 关闭返回错误。
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        if let Some(ready) = &self.ready {
            if ready.valid() {
                return Poll::Ready(Ok(()));
            }
            self.ready.take();
            return Poll::Ready(Err(Status::deadline_exceeded("gRPC reservation expired")));
        }
        if self.waiting.is_none() {
            let shared = self.shared.clone();
            let budget = self
                .budget
                .as_ref()
                .map(|budget| budget.child(shared.options.request_timeout))
                .unwrap_or_else(|| RequestBudget::from_now(shared.options.request_timeout));
            self.waiting = Some(Box::pin(async move {
                let owner = shared
                    .discovery
                    .reserve_transport()
                    .map_err(discovery_error)?;
                let permit = tokio::select! {
                    biased;
                    _ = owner.cancelled() => return Err(Status::unavailable("discovery owner closed")),
                    result = budget.run(shared.slots.clone().acquire_owned()) => result.map_err(budget_error)?.map_err(|_| Status::unavailable("gRPC capacity closed"))?,
                };
                let reservation = Arc::new(Reservation {
                    permit: Mutex::new(Some(permit)),
                    budget,
                    done: CancellationToken::new(),
                });
                let release = Release(reservation.clone());
                let done = reservation.done.clone();
                // 即使调用方在 Ready 后永不调用 call，后台期限也会归还预约。
                owner
                    .spawn(reservation.budget.deadline(), async move {
                        let _release = release;
                        done.cancelled().await;
                    })
                    .map_err(discovery_error)?;
                Ok(reservation)
            }));
        }
        match self.waiting.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.waiting = None;
                self.ready = Some(result?);
                Poll::Ready(Ok(()))
            }
        }
    }

    /// 业务作用：用一次预约重新选址，并在 HTTP/2 提交处复验当前发现事实。
    /// 参数说明：`request` 可以是任一 gRPC 调用类型；正文不缓存为完整消息。
    /// 返回：响应正文仍受同一预算及 owner 控制；原期限到期报告 DeadlineExceeded，错误不声称远端未执行。
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let shared = self.shared.clone();
        let selected = self.selected.clone();
        let admission = self.admission.clone();
        let ready = self.ready.take();
        Box::pin(async move {
            let reservation = ready.ok_or_else(|| {
                Status::failed_precondition("poll_ready must reserve gRPC capacity before call")
            })?;
            let release = Release(reservation.clone());
            if !reservation.valid() {
                return Err(Status::deadline_exceeded("gRPC reservation expired"));
            }
            let owner = shared
                .discovery
                .reserve_transport()
                .map_err(discovery_error)?;
            let maximum = request_timeout(request.headers(), reservation.budget.remaining())?;
            let budget = reservation.budget.child(maximum);
            let operation = pool::call(
                shared,
                reservation,
                request,
                release,
                budget.clone(),
                selected,
                admission,
            );
            tokio::select! {
                biased;
                _ = owner.cancelled() => Err(Status::unavailable("discovery owner closed")),
                result = budget.run(operation) => result.map_err(|error| {
                    // 到期预约的后台回收也会取消子预算；以原截止点裁决，避免把排队超时误报为主动取消。
                    if budget.remaining().is_zero() {
                        Status::deadline_exceeded("gRPC request deadline elapsed")
                    } else {
                        budget_error(error)
                    }
                })?,
            }
        })
    }
}

/// 业务作用：保留调用方更短的协议期限，禁止畸形 grpc-timeout 被悄悄替换为较长配置值。
/// 参数说明：`headers` 是协议头，`maximum` 已包含就绪排队消耗。
/// 返回：两者较短的剩余时长；重复或非法期限拒绝发送。
fn request_timeout(
    headers: &tonic::codegen::http::HeaderMap,
    maximum: Duration,
) -> Result<Duration, Status> {
    let mut values = headers.get_all("grpc-timeout").iter();
    let Some(value) = values.next() else {
        return Ok(maximum);
    };
    let invalid = || Status::invalid_argument("invalid gRPC timeout");
    if values.next().is_some() {
        return Err(invalid());
    }
    let value = value.to_str().map_err(|_| invalid())?;
    if !(2..=9).contains(&value.len())
        || !value.as_bytes()[..value.len() - 1]
            .iter()
            .all(u8::is_ascii_digit)
    {
        return Err(invalid());
    }
    let amount: u64 = value[..value.len() - 1].parse().map_err(|_| invalid())?;
    let timeout = match value.as_bytes()[value.len() - 1] {
        b'H' => Duration::from_secs(amount * 3600),
        b'M' => Duration::from_secs(amount * 60),
        b'S' => Duration::from_secs(amount),
        b'm' => Duration::from_millis(amount),
        b'u' => Duration::from_micros(amount),
        b'n' => Duration::from_nanos(amount),
        _ => return Err(invalid()),
    };
    Ok(maximum.min(timeout))
}

/// 业务作用：为受管发现调用构造只信任显式 CA 的 mTLS 配置，不读取系统或环境中的信任材料。
/// 参数说明：`ca` 是完整 CA PEM，`certificate` 与 `key` 是客户端身份材料。
/// 返回：完整材料合法且密钥匹配时返回 TLS 配置；任一材料失败拒绝整个候选。
pub fn mutual_tls(
    ca: &[u8],
    certificate: &[u8],
    key: &[u8],
) -> Result<Arc<rustls::ClientConfig>, Status> {
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    let invalid = || Status::invalid_argument("invalid managed gRPC TLS material");
    let roots = CertificateDer::pem_slice_iter(ca)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid())?;
    let mut trust = rustls::RootCertStore::empty();
    let (accepted, rejected) = trust.add_parsable_certificates(roots);
    if accepted == 0 || rejected != 0 {
        return Err(invalid());
    }
    let identity = CertificateDer::pem_slice_iter(certificate)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid())?;
    let key = PrivateKeyDer::from_pem_slice(key).map_err(|_| invalid())?;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| invalid())?
    .with_root_certificates(trust)
    .with_client_auth_cert(identity, key)
    .map_err(|_| invalid())?;
    Ok(Arc::new(config))
}

/// 业务作用：把公共发现故障转换为不会包含 provider 地址或凭据的协议错误。
/// 参数说明：`error` 是中性稳定类别。
/// 返回：容量不足与不可用保持不同的 gRPC 状态。
fn discovery_error(error: nadisc::managed::DiscoveryError) -> Status {
    use nadisc::managed::ErrorCode;
    match error.code {
        ErrorCode::CapacityExceeded => Status::resource_exhausted("discovery capacity exhausted"),
        ErrorCode::DeadlineExceeded => Status::deadline_exceeded("discovery deadline elapsed"),
        _ => Status::unavailable("discovery evidence is unavailable"),
    }
}
/// 业务作用：保留调用链显式取消与期限耗尽的不同结果。
/// 参数说明：`error` 是预算裁决。
/// 返回：不包含请求内容的协议状态。
fn budget_error(error: nabudget::BudgetError) -> Status {
    match error {
        nabudget::BudgetError::Cancelled => Status::cancelled("gRPC call cancelled"),
        nabudget::BudgetError::DeadlineExceeded => {
            Status::deadline_exceeded("gRPC call deadline elapsed")
        }
    }
}
