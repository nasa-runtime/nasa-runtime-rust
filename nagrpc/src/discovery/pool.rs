//! 连接复用只减少握手，不保留发现发送权。每个 RPC 都在 HTTP/2 提交处复验。

use super::*;
use hyper::{body::Incoming, client::conn::http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use nadisc::Instance;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tonic::codegen::http::Uri;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

#[derive(Default)]
pub(super) struct Pool {
    connections: Vec<Arc<Connection>>,
    failures: Vec<ConnectFailure>,
    connecting: Vec<Arc<Connecting>>,
}
struct ConnectFailure {
    instance: Instance,
    until: Instant,
    delay: Duration,
}
struct Connecting {
    instance: Instance,
    done: CancellationToken,
}
struct Dial {
    shared: Arc<Shared>,
    connecting: Arc<Connecting>,
    retry_delay: Duration,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Dial {
    /// 业务作用：在握手成功、失败或调用取消时释放端点建连权并唤醒同端点等待者。
    /// 参数说明：无。
    /// 返回：取消不遗留建连占位；未成功发布的端点从握手结束时继续退避，不能由等待者立即重拨。
    fn drop(&mut self) {
        let mut pool = self
            .shared
            .pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pool.connecting
            .retain(|entry| !Arc::ptr_eq(entry, &self.connecting));
        // 成功发布已清除失败记录；其余退出路径延续退避，包含 future 在任意 await 被丢弃的情况。
        if let Some(failure) = pool
            .failures
            .iter_mut()
            .find(|failure| failure.instance == self.connecting.instance)
        {
            failure.until = Instant::now() + self.retry_delay;
        }
        self.connecting.done.cancel();
    }
}

enum Acquire {
    Wait(CancellationToken),
    Dial(Dial),
}

pub(super) struct Connection {
    pub instance: Instance,
    sender: tokio::sync::Mutex<http2::SendRequest<Body>>,
    driver: tokio::task::AbortHandle,
    active: Arc<Activity>,
    pub authority: String,
    pub secure: bool,
    expires: Instant,
}
struct Activity {
    streams: AtomicUsize,
    touched: Mutex<Instant>,
}
impl Drop for Connection {
    /// 业务作用：最后一个池或在途 RPC 引用释放时结束该连接 driver。
    /// 参数说明：无。
    /// 返回：实例被移除时仍由在途正文持有连接，直到排空或预算结束。
    fn drop(&mut self) {
        self.driver.abort();
    }
}
struct Active(Arc<Connection>);
impl Drop for Active {
    /// 业务作用：RPC 结束时更新连接空闲事实。
    /// 参数说明：无。
    /// 返回：最后一个流结束后开始计算空闲回收时间。
    fn drop(&mut self) {
        self.0.active.streams.fetch_sub(1, Ordering::AcqRel);
        *self
            .0
            .active
            .touched
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
    }
}

/// 业务作用：用当前实例完整协议字段选址，并在实际 HTTP/2 提交前再次核对同一候选。
/// 参数说明：`shared` 固定作用域，`reservation` 与 `budget` 保留原期限，`request` 是双向流，`release` 保证回收；`selected` 绑定已认证实例，`admission` 复验业务身份。
/// 返回：带受管正文的响应，不重试已提交或可能提交的 RPC。
pub(super) async fn call(
    shared: Arc<Shared>,
    reservation: Arc<Reservation>,
    mut request: Request<Body>,
    release: Release,
    budget: RequestBudget,
    selected: Option<Instance>,
    admission: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
) -> Result<Response<Body>, Status> {
    let observation = shared
        .discovery
        .observe(&shared.service, budget.deadline())
        .await
        .map_err(discovery_error)?;
    let snapshot = observation
        .snapshot(selected.is_some())
        .map_err(discovery_error)?;
    let instances: Vec<_> = snapshot
        .instances()
        .iter()
        .filter(|instance| nadisc::is_traffic_instance(instance))
        .filter(|instance| {
            selected
                .as_ref()
                .is_none_or(|selected| selected == *instance)
        })
        .cloned()
        .collect();
    if instances.is_empty() {
        return Err(Status::unavailable("no eligible gRPC instance"));
    }
    let index = shared.cursor.fetch_add(1, Ordering::Relaxed) % instances.len();
    let instance = instances[index].clone();
    let connection = pooled_connection(
        &shared,
        &observation,
        &instance,
        &reservation,
        &budget,
        selected.is_some(),
    )
    .await?;
    let path = request
        .uri()
        .path_and_query()
        .map_or("/", |path| path.as_str());
    let uri: Uri = format!(
        "{}://{}{path}",
        if connection.secure {
            "https"
        } else {
            "http"
        },
        connection.authority
    )
    .parse()
    .map_err(|_| Status::invalid_argument("invalid gRPC request path"))?;
    *request.uri_mut() = uri;
    *request.version_mut() = tonic::codegen::http::Version::HTTP_2;
    let milliseconds = budget.remaining().as_millis().max(1);
    request.headers_mut().insert(
        "grpc-timeout",
        format!("{milliseconds}m")
            .parse()
            .map_err(|_| Status::invalid_argument("invalid gRPC deadline"))?,
    );
    let (parts, incoming) = request.into_parts();
    let (outgoing, input_task) = body::pump_request(&shared, incoming, budget.clone())?;
    let input = body::AbortOnDrop(input_task);
    let request = Request::from_parts(parts, outgoing);
    let mut sender = connection.sender.lock().await;
    sender
        .ready()
        .await
        .map_err(|_| Status::unavailable("gRPC HTTP/2 sender closed"))?;
    if !reservation.valid() || budget.is_exhausted() {
        return Err(Status::deadline_exceeded("gRPC reservation expired"));
    }
    connection.active.streams.fetch_add(1, Ordering::AcqRel);
    let active = Active(connection.clone());
    // Sender 已取得流提交能力；在同一发现门禁内调用 send_request，不使用隐藏的 Tower buffer。
    let submit = || {
        if admission.as_ref().is_some_and(|check| !check()) {
            return Err(Status::permission_denied("gRPC business admission expired"));
        }
        Ok(sender.send_request(request))
    };
    let response = if selected.is_some() {
        observation.submit_if_current_strict(&instance, submit)
    } else {
        observation.submit_if_current(&instance, submit)
    }
    .map_err(discovery_error)??;
    drop(sender);
    let response: Response<Incoming> = response
        .await
        .map_err(|_| Status::unavailable("gRPC response headers unavailable"))?;
    let (parts, incoming) = response.into_parts();
    let body = body::pump_response(&shared, incoming, budget, async move {
        let _owned = (release, active, input);
        std::future::pending::<()>().await;
    })?;
    Ok(Response::from_parts(parts, body))
}

/// 业务作用：复用健康连接或取得单端点建连权，让不同端点的握手互不持有全局池锁。
/// 参数说明：`shared` 固定池和拨号上限，`observation` 与 `instance` 保留候选身份；`reservation`、`budget` 保留原期限，`strict` 禁止失败证据宽限。
/// 返回：当前候选的连接；同端点等待者复用成功结果，失败或取消保留退避，容量不足不启动拨号。
async fn pooled_connection(
    shared: &Arc<Shared>,
    observation: &nadisc_runtime::Observation,
    instance: &Instance,
    reservation: &Reservation,
    budget: &RequestBudget,
    strict: bool,
) -> Result<Arc<Connection>, Status> {
    loop {
        let snapshot = observation.snapshot(strict).map_err(discovery_error)?;
        // 等待另一调用握手期间候选可能已撤下；不能因端点占位完成就重新取得发送资格。
        if !snapshot.instances().contains(instance) || !nadisc::is_traffic_instance(instance) {
            return Err(Status::unavailable("gRPC endpoint is no longer eligible"));
        }
        let acquire = {
            let mut pool = shared
                .pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pool.connections.retain(|connection| {
                !connection.driver.is_finished()
                    && Instant::now() < connection.expires
                    && snapshot.instances().contains(&connection.instance)
            });
            if let Some(connection) = pool
                .connections
                .iter()
                .find(|entry| entry.instance == *instance)
            {
                return Ok(connection.clone());
            }
            if let Some(connecting) = pool
                .connecting
                .iter()
                .find(|entry| entry.instance == *instance)
            {
                Acquire::Wait(connecting.done.clone())
            } else {
                let now = Instant::now();
                // 保留在途握手的失败槽位，使同端点取消后的退避不会被其他端点挤掉。
                let Pool {
                    failures,
                    connecting,
                    ..
                } = &mut *pool;
                failures.retain(|failure| {
                    snapshot.instances().contains(&failure.instance)
                        || connecting
                            .iter()
                            .any(|entry| entry.instance == failure.instance)
                });
                let position = pool
                    .failures
                    .iter()
                    .position(|failure| failure.instance == *instance);
                if position.is_some_and(|index| now < pool.failures[index].until) {
                    return Err(Status::unavailable(
                        "gRPC endpoint is in connection backoff",
                    ));
                }
                let permit = shared
                    .dials
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Status::resource_exhausted("gRPC dial capacity exhausted"))?;
                // 已发布与正在建立的连接共用池容量；只淘汰没有调用持有的空闲连接。
                if pool.connections.len() + pool.connecting.len() >= shared.options.max_connections
                {
                    if let Some(index) = pool.connections.iter().position(|connection| {
                        connection.active.streams.load(Ordering::Acquire) == 0
                            && Arc::strong_count(connection) == 1
                    }) {
                        pool.connections.remove(index);
                    } else {
                        return Err(Status::resource_exhausted("gRPC connection pool exhausted"));
                    }
                }
                let delay = position.map_or(shared.options.initial_connect_backoff, |index| {
                    pool.failures[index]
                        .delay
                        .saturating_mul(2)
                        .min(shared.options.max_connect_backoff)
                });
                if let Some(index) = position {
                    pool.failures.remove(index);
                } else if pool.failures.len() >= shared.options.max_connections {
                    if let Some(index) = pool.failures.iter().position(|failure| {
                        now >= failure.until
                            && !pool
                                .connecting
                                .iter()
                                .any(|entry| entry.instance == failure.instance)
                    }) {
                        pool.failures.remove(index);
                    } else {
                        return Err(Status::resource_exhausted(
                            "gRPC connection backoff capacity exhausted",
                        ));
                    }
                }
                // 握手前先登记退避与独占占位；等待者只共享结果，不创建第二次拨号。
                let jitter = shared.cursor.fetch_add(1, Ordering::Relaxed) % 51 + 50;
                let retry_delay = delay.mul_f64(jitter as f64 / 100.0);
                pool.failures.push(ConnectFailure {
                    instance: instance.clone(),
                    until: now + retry_delay,
                    delay,
                });
                let connecting = Arc::new(Connecting {
                    instance: instance.clone(),
                    done: CancellationToken::new(),
                });
                pool.connecting.push(connecting.clone());
                Acquire::Dial(Dial {
                    shared: shared.clone(),
                    connecting,
                    retry_delay,
                    _permit: permit,
                })
            }
        };
        match acquire {
            Acquire::Wait(done) => done.cancelled().await,
            Acquire::Dial(dial) => {
                let connection = connect(shared, instance.clone()).await?;
                let published = {
                    let mut pool = shared
                        .pool
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    // 握手成功不代表仍有发布权；原预算和发现门禁必须同时允许该完整实例。
                    if !reservation.valid() || budget.is_exhausted() {
                        return Err(Status::deadline_exceeded(
                            "gRPC connection reservation expired",
                        ));
                    }
                    let publish = || {
                        if let Some(existing) = pool
                            .connections
                            .iter()
                            .find(|entry| entry.instance == *instance)
                        {
                            return Ok(existing.clone());
                        }
                        if pool.connections.len() >= shared.options.max_connections {
                            return Err(Status::resource_exhausted(
                                "gRPC connection pool exhausted",
                            ));
                        }
                        if connection.driver.is_finished() || Instant::now() >= connection.expires {
                            return Err(Status::unavailable(
                                "gRPC connection expired before publication",
                            ));
                        }
                        pool.connections.push(connection.clone());
                        Ok(connection)
                    };
                    let connection = if strict {
                        observation.submit_if_current_strict(instance, publish)
                    } else {
                        observation.submit_if_current(instance, publish)
                    }
                    .map_err(discovery_error)??;
                    pool.failures
                        .retain(|failure| failure.instance != *instance);
                    connection
                };
                drop(dial);
                return Ok(published);
            }
        }
    }
}

/// 业务作用：按明确协议与 TLS 信任策略建立一个由 discovery owner 托管的 HTTP/2 连接。
/// 参数说明：`shared` 是冻结调用配置，`instance` 提供本代完整端点事实。
/// 返回：握手成功且 driver 已登记才发布池条目；失败不留下独立重连任务。
async fn connect(shared: &Shared, instance: Instance) -> Result<Arc<Connection>, Status> {
    let metadata = &instance.metadata;
    if shared.options.protocol.is_none()
        && metadata.get("nasa.grpc.protocol").map(String::as_str) != Some("grpc")
    {
        return Err(Status::failed_precondition(
            "instance has no explicit gRPC protocol",
        ));
    }
    // 两套协议来源互斥，避免地址取自配置而 TLS 身份悄悄来自另一份 metadata。
    if shared.options.protocol.is_some() && metadata.keys().any(|key| key.starts_with("nasa.grpc."))
    {
        return Err(Status::failed_precondition(
            "gRPC protocol binding conflicts with instance metadata",
        ));
    }
    let port = shared
        .options
        .protocol
        .as_ref()
        .map(|_| instance.port)
        .or_else(|| {
            metadata
                .get("nasa.grpc.port")
                .and_then(|port| port.parse::<u16>().ok())
        })
        .filter(|port| *port > 0)
        .ok_or_else(|| Status::failed_precondition("instance gRPC port is absent"))?;
    let authority = shared
        .options
        .protocol
        .as_ref()
        .map(|protocol| &protocol.authority)
        .or_else(|| metadata.get("nasa.grpc.authority"))
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 253
                && !value
                    .chars()
                    .any(|c| c.is_whitespace() || matches!(c, '/' | '@' | '?' | '#'))
        })
        .ok_or_else(|| Status::failed_precondition("instance gRPC authority is invalid"))?;
    let mode = shared
        .options
        .protocol
        .as_ref()
        .map(|protocol| protocol.tls_mode.as_str())
        .or_else(|| metadata.get("nasa.grpc.tls_mode").map(String::as_str));
    if mode == Some("mutual")
        && !shared
            .options
            .tls
            .as_ref()
            .is_some_and(|tls| tls.client_auth_cert_resolver.has_certs())
    {
        return Err(Status::permission_denied(
            "gRPC mutual TLS requires a client identity",
        ));
    }
    let secure = match mode {
        Some("disabled") if shared.options.tls.is_none() => false,
        Some("server" | "mutual")
            if shared.options.tls.is_some()
                && shared.options.allowed_authorities.contains(authority) =>
        {
            true
        }
        _ => {
            return Err(Status::permission_denied(
                "instance TLS mode or identity violates client policy",
            ))
        }
    };
    let owner = shared
        .discovery
        .reserve_transport()
        .map_err(discovery_error)?;
    let socket = TcpStream::connect((instance.ip.as_str(), port))
        .await
        .map_err(|_| Status::unavailable("gRPC endpoint connection failed"))?;
    socket
        .set_nodelay(true)
        .map_err(|_| Status::unavailable("cannot configure gRPC connection"))?;
    let io: Box<dyn Io> = if secure {
        let name = rustls::pki_types::ServerName::try_from(
            shared
                .options
                .tls_server_name
                .as_ref()
                .unwrap_or(authority)
                .clone(),
        )
        .map_err(|_| Status::failed_precondition("invalid gRPC TLS authority"))?;
        let mut config = shared.options.tls.as_ref().unwrap().as_ref().clone();
        config.alpn_protocols = vec![b"h2".to_vec()];
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, socket)
            .await
            .map_err(|_| Status::unavailable("gRPC TLS handshake failed"))?;
        if stream.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
            return Err(Status::unavailable("gRPC peer did not negotiate HTTP/2"));
        }
        Box::new(stream)
    } else {
        Box::new(socket)
    };
    let (sender, driver) = http2::Builder::new(TokioExecutor::new())
        .max_concurrent_reset_streams(32)
        .handshake::<_, Body>(TokioIo::new(io))
        .await
        .map_err(|_| Status::unavailable("gRPC HTTP/2 handshake failed"))?;
    let activity = Arc::new(Activity {
        streams: AtomicUsize::new(0),
        touched: Mutex::new(Instant::now()),
    });
    let watching = activity.clone();
    let idle = shared.options.idle_timeout;
    let expires = Instant::now() + shared.options.max_connection_age;
    let driver = owner.spawn(expires, async move {
        tokio::pin!(driver);
        loop {
            tokio::select! {
                _ = &mut driver => break,
                _ = tokio::time::sleep(idle.min(Duration::from_secs(1))) => {
                    if watching.streams.load(Ordering::Acquire) == 0 && watching.touched.lock().unwrap_or_else(std::sync::PoisonError::into_inner).elapsed() >= idle { break; }
                }
            }
        }
    }).map_err(discovery_error)?;
    let host = if authority.contains(':') {
        format!("[{authority}]")
    } else {
        authority.clone()
    };
    Ok(Arc::new(Connection {
        instance,
        sender: tokio::sync::Mutex::new(sender),
        driver,
        active: activity,
        authority: format!("{host}:{port}"),
        secure,
        expires,
    }))
}
