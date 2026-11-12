use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;
use std::{collections::HashMap, pin::Pin, sync::Arc};

use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use futures::{Future, future};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::sleep;
use tower::buffer::Buffer;
use tower::{MakeService, Service};
use tracing::{Instrument, debug, debug_span, error, info, instrument, warn};

use crate::api::error::Error::ErrResult;
use crate::api::error::Error::GrpcBufferRequest;
use crate::common::executor;
use crate::common::remote::grpc::handlers::default_handler::DefaultHandler;
use crate::common::remote::grpc::message::request::{
    ConnectionSetupRequest, HealthCheckRequest, NacosClientAbilities, ServerCheckRequest,
};
use crate::common::remote::grpc::message::response::{HealthCheckResponse, ServerCheckResponse};
use crate::common::remote::grpc::message::{GrpcMessage, GrpcMessageBuilder};
use crate::common::remote::grpc::nacos_grpc_service::GrpcStream;
use crate::common::remote::grpc::utils;
use crate::{api::error::Error, nacos_proto::v2::Payload};

use super::nacos_grpc_service::NacosGrpcCall;
use super::nacos_grpc_service::ServerRequestHandler;

type ConnectedListener = Arc<dyn Fn(String) + Send + Sync + 'static>;
type DisconnectedListener = Arc<dyn Fn(String) + Send + Sync + 'static>;

type HandlerMap = HashMap<String, Arc<dyn ServerRequestHandler>>;
const MAX_RETRY: u32 = 6;

const HEALTH_CHECK_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

const HEALTH_CHECK_RETRY_TIMES: u32 = 3;

/// 业务作用：提供连接活动记录的 Unix 毫秒时间，用于判定空闲后是否需要健康探测。
///
/// 参数说明: 无。
///
/// 返回：当前 Unix 毫秒数；系统时间早于纪元时使用零。此值不是单调时钟，调用方需处理时钟回拨。
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// 业务作用：为连接失败计算有上限的指数退避，避免持续立即重连。
///
/// 参数说明：
/// - `retry_count`：本轮累计失败次数；通常在记录一次失败后传入。
///
/// 返回：2 的失败次数次方秒，最高 64 秒；零次为 1 秒，此函数不等待且不加入随机抖动。
fn sleep_time(retry_count: u32) -> u32 {
    if retry_count > MAX_RETRY {
        1 << MAX_RETRY
    } else {
        1 << retry_count
    }
}

pub(crate) struct NacosGrpcConnection<M>
where
    M: MakeService<(), NacosGrpcCall>,
{
    /// 客户端实例的连接诊断标识。
    id: String,
    /// 每次初始化握手声明的客户端实现版本。
    client_version: String,
    /// 握手 tenant 所属命名空间。
    namespace: String,
    /// 重连时复用的客户端标签快照。
    labels: HashMap<String, String>,
    /// 握手声明的客户端能力集合。
    client_abilities: NacosClientAbilities,
    /// 控制流协议类型对应的处理器目录。
    handler_map: Arc<HandlerMap>,
    /// 每次重连创建传输服务的工厂。
    mk_service: M,
    /// 唯一连接生命周期状态，决定是否允许请求提交。
    state: State<M::Future, M::Service>,
    /// 初始化授予、控制流或探测失败可撤销的健康证据。
    health: Arc<AtomicBool>,
    /// 最近请求回调或控制流消息的 Unix 毫秒时间。
    last_active: Arc<AtomicU64>,
    /// 最近成功初始化取得的连接身份，退避期间不立即清空此字段。
    connection_id: Option<String>,
    /// 当前连续失败次数，成功初始化或首次阈值返回时清零。
    retry_count: u32,
    /// 对外发布身份出现或撤销的观察通道，可能合并快速更新。
    connection_id_watcher: (
        watch::Sender<Option<String>>,
        watch::Receiver<Option<String>>,
    ),
    /// 仅首次成功初始化前生效的失败尝试阈值。
    max_retries: Option<u32>,
    /// 是否曾成功完成初始化，决定首次尝试阈值是否继续生效。
    is_initialized: bool,
}

impl<M> NacosGrpcConnection<M>
where
    M: MakeService<(), NacosGrpcCall, Error = Error, MakeError = Error> + Send,
    M::Error: Send + 'static,
    M::Response: Send + 'static,
    M::Future: Send + 'static,
    M::Service: Send + 'static,
    <M::Service as Service<NacosGrpcCall>>::Future: Send + 'static,
{
    /// 业务作用：建立尚未连接的 Nacos 状态机，集中持有握手资料、控制流处理和健康证据。
    ///
    /// 参数说明：
    /// - `id`：客户端实例的诊断关联标识。
    /// - `mk_service`：按需创建底层传输服务的工厂。
    /// - `handler_map`：按协议类型分派服务端推送的处理器集合。
    /// - `client_version`：握手声明的客户端实现版本。
    /// - `namespace`：握手 tenant 使用的命名空间。
    /// - `labels`：每次握手复用的客户端标签。
    /// - `client_abilities`：每次握手声明的能力集合。
    /// - `max_retries`：首次初始化失败阈值，未指定时使用 1；成功初始化后不再约束重连。
    ///
    /// 返回：Idle 状态、健康为假且尚无连接身份的状态机，不启动连接或后台任务。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: String,
        mk_service: M,
        handler_map: HandlerMap,
        client_version: String,
        namespace: String,
        labels: HashMap<String, String>,
        client_abilities: NacosClientAbilities,
        max_retries: Option<u32>,
    ) -> Self {
        let connection_id_watcher = watch::channel(None);

        Self {
            id,
            handler_map: Arc::new(handler_map),
            mk_service,
            client_version,
            namespace,
            labels,
            client_abilities,
            state: State::Idle,
            health: Arc::new(AtomicBool::new(false)),
            last_active: Arc::new(AtomicU64::new(now_millis())),
            connection_id: None,
            retry_count: 0,
            connection_id_watcher,
            max_retries,
            is_initialized: false,
        }
    }

    /// 业务作用：启动连接身份观察任务，在观察值由空变为有值时通知上层。
    ///
    /// 参数说明：
    /// - `listener`：接收新连接 ID 的同步回调，运行于观察任务内，应避免阻塞。
    ///
    /// 返回：原连接对象；观察任务在 watch 关闭时结束。watch 可合并快速更新，不能作为完整状态迁移日志。
    pub(crate) fn connected_listener(self, listener: ConnectedListener) -> Self {
        let mut rx = self.connection_id_watcher.1.clone();
        let watch_fu = async move {
            let mut previous_id = None;
            while rx.changed().await.is_ok() {
                let current_id = { rx.borrow().clone() };

                // 仅在已观察的缺失身份变为有效身份时通知，持续存在的身份不会重复触发建连回调。
                if previous_id.is_none()
                    && let Some(ref id) = current_id
                {
                    listener(id.clone());
                }
                previous_id = current_id;
            }
            debug!("connected listener quit.");
        };
        executor::spawn(watch_fu);
        self
    }

    /// 业务作用：观察已知连接身份的撤销，通知上层清理对应会话状态。
    ///
    /// 参数说明：
    /// - `listener`：接收此前连接 ID 的同步回调，运行于后台观察任务。
    ///
    /// 返回：原连接对象；只处理已观察的有值到空迁移，watch 关闭本身不补发断连通知。
    pub(crate) fn disconnected_listener(self, listener: DisconnectedListener) -> Self {
        let mut rx = self.connection_id_watcher.1.clone();
        let watch_fu = async move {
            let mut previous_id: Option<String> = None;
            while rx.changed().await.is_ok() {
                let current_id = { rx.borrow().clone() };

                // 必须曾观察到连接身份才能归属断连通知，避免把初始未连接状态当作已断开的会话。
                if current_id.is_none()
                    && let Some(ref id) = previous_id
                {
                    listener(id.clone());
                }
                previous_id = current_id;
            }
            debug!("disconnect listener quit.");
        };
        executor::spawn(watch_fu);
        self
    }

    /// 业务作用：把连接状态机移入共享缓冲发送入口，并启动空闲健康探测。
    ///
    /// 参数说明：
    /// - `id`：缓冲发送及健康任务使用的诊断标识。
    ///
    /// 返回：拥有状态机的故障转移入口，后台任务从此开始运行；包装完成不代表连接已建立。
    pub(crate) fn into_failover_connection(
        self,
        id: String,
    ) -> FailoverConnection<NacosGrpcConnection<M>> {
        let svc_health = self.health.clone();
        let svc_last_active = self.last_active.clone();
        FailoverConnection::new(id, self, svc_health, svc_last_active)
    }

    /// 业务作用：建立控制流，尝试健康探测，再取得并移交服务端连接身份。
    ///
    /// 参数说明：
    /// - `service`：新建的底层传输服务，初始化完成后交回状态机。
    /// - `client_version`：本次握手的客户端实现版本。
    /// - `namespace`：握手所属命名空间。
    /// - `labels`：客户端标签快照。
    /// - `client_abilities`：客户端能力声明。
    /// - `handler_map`：服务端控制推送处理器集合。
    /// - `health`：控制流失效时需要撤销的共享健康标记。
    /// - `last_active`：接收推送时更新的 Unix 毫秒活动记录。
    ///
    /// 返回：控制流和服务检查成功后返回服务及连接 ID；握手、服务检查或身份移交失败返回错误。四次健康探测均失败也仍执行服务检查，不单独据此拒绝初始化。
    #[allow(clippy::too_many_arguments)]
    async fn init_connection(
        mut service: M::Service,
        client_version: String,
        namespace: String,
        labels: HashMap<String, String>,
        client_abilities: NacosClientAbilities,
        handler_map: Arc<HandlerMap>,
        health: Arc<AtomicBool>,
        last_active: Arc<AtomicU64>,
    ) -> Result<(M::Service, String), Error> {
        // 先创建双向控制流，避免只凭单次请求成功发布缺少控制通道的会话。
        let conn_id_sender = NacosGrpcConnection::<M>::setup(
            handler_map,
            &mut service,
            health,
            last_active,
            client_version,
            namespace,
            labels,
            client_abilities,
        )
        .in_current_span()
        .await?;

        // 给新控制流预留探测重试时间，最终初始化结果仍由后面的服务检查及身份移交裁决。
        for i in 0..4 {
            let health_check = NacosGrpcConnection::<M>::connection_health_check(&mut service)
                .in_current_span()
                .await;
            if health_check.is_err() {
                // 逐次增加短暂等待，避免握手传播期间连续立即探测。
                sleep(Duration::from_millis(300 << i)).await;
                continue;
            }
            break;
        }

        // 连接身份必须由当前传输上的服务检查返回，不能沿用此前会话 ID。
        let connection_id = NacosGrpcConnection::<M>::check_server(&mut service)
            .in_current_span()
            .await?;

        let conn_id_send_ret = conn_id_sender.send(connection_id.clone());
        if let Err(e) = conn_id_send_ret {
            // 控制流任务已退出，不能把单次检查成功当作完整会话发布。
            error!(
                "send connection id to bi stream task occur an error. please check connection state. {e}"
            );
            return Err(ErrResult("the bi stream task has already quit, because connection id sender send id occur an error".to_string()));
        }

        // 这里只交回候选身份，外层状态机随后负责发布观察值和健康标记。
        Ok((service, connection_id))
    }

    /// 业务作用：建立当前 Nacos 会话的双向控制流，并把控制流失效交回唯一重连状态机。
    ///
    /// 参数说明：
    /// - `server_stream_handlers`：按协议类型处理推送的处理器目录。
    /// - `service`：当前通道请求接口。
    /// - `health`：当前会话有效性证据，控制流失效时撤销。
    /// - `last_active`：当前会话最后活动的时间证据。
    /// - `client_version`：客户端协议版本标识。
    /// - `namespace`：会话或配置的命名空间。
    /// - `labels`：本次会话的固定客户端标签。
    /// - `client_abilities`：本次会话公开的客户端能力。
    ///
    /// 返回：发送流首次被轮询或启动通知被释放后，返回连接身份移交句柄，仍需服务检查确认初始化；编码及提交失败返回错误。接收流建立失败或已取得身份后的接收循环退出会撤销健康。
    #[allow(clippy::too_many_arguments)]
    async fn setup(
        server_stream_handlers: Arc<HandlerMap>,
        service: &mut M::Service,
        health: Arc<AtomicBool>,
        last_active: Arc<AtomicU64>,
        client_version: String,
        namespace: String,
        labels: HashMap<String, String>,
        client_abilities: NacosClientAbilities,
    ) -> Result<oneshot::Sender<String>, Error> {
        info!("setup connection");

        let setup_request = ConnectionSetupRequest {
            client_version,
            labels,
            tenant: namespace,
            abilities: client_abilities,
            ..Default::default()
        };

        let (local_sender, mut local_receiver) = mpsc::channel::<Payload>(1024);
        let local_sender = Arc::new(local_sender);
        let local_sender_clone = local_sender.clone();

        let payload = utils::convert(
            GrpcMessageBuilder::new(setup_request)
                .build()
                .into_payload(),
            "setup message convert to grpc message occur an error",
        )?;

        let send_ret = local_sender.send(payload).await;
        if let Err(e) = send_ret {
            error!("bi stream hash been closed. {}", e);
            return Err(ErrResult("bi stream hash been closed".to_string()));
        }

        let (notifier, waiter) = oneshot::channel::<()>();
        let local_stream = GrpcStream::<Payload>::new(Box::pin(stream! {
            // 通知只证明发送流开始被消费，不代表服务端已接收握手或认可连接身份。
            let _ = notifier.send(());
            debug!("open local stream.");
            while let Some(request) = local_receiver.recv().await {
                debug!("local stream send message to server");
                yield request
            }
            warn!("local stream closed!");
        }));

        let (cb, rx, mut tk) =
            utils::create_grpc_callback::<Result<GrpcStream<Result<Payload, Error>>, Error>>();
        let call = NacosGrpcCall::BIRequestService((local_stream, cb));
        executor::spawn(service.call(call).in_current_span());

        let (conn_id_sender, conn_id_receiver) = oneshot::channel::<String>();
        executor::spawn(
            async move {
                tk.want();
                let server_stream =
                    match utils::recv_response(rx.await, "server stream callback failed") {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(e)) => {
                            error!("can't open server stream. {}", e);
                            warn!("server stream closed!");
                            // 控制流建立失败后旧会话身份不再有效，必须交由既有状态机重连后重放原注册与订阅。
                            health.store(false, Ordering::Release);
                            return;
                        }
                        Err(_) => {
                            warn!("server stream closed!");
                            // 回调通道结束同样表示控制流丢失，不能让仍可发送的单次请求维持虚假的健康状态。
                            health.store(false, Ordering::Release);
                            return;
                        }
                    };

                // 必须先取得初始化任务确认的连接身份，再处理该会话的服务端推送。
                let conn_id = match utils::recv_response(
                    conn_id_receiver.await,
                    "server stream has already opened, but cannot get connection id",
                ) {
                    Ok(id) => id,
                    Err(_) => return,
                };

                // 后续控制消息与本次连接 ID 关联，便于区分不同会话的处理过程。
                let span = debug_span!("bi_stream", conn_id = conn_id);
                async {
                    let mut server_stream = Box::pin(server_stream);
                    while let Some(Ok(response)) = server_stream.next().await {
                        last_active.store(now_millis(), Ordering::Release);
                        debug!("server stream receive message from server");
                        let Some(handler_key) = response
                            .metadata
                            .as_ref()
                            .map(|meta_data| meta_data.r#type.clone())
                        else {
                            // 缺失 metadata 时无法选择协议处理器，不能把消息按某个业务合同执行。
                            debug!("response payload type field is empty, skip.");
                            continue;
                        };
                        debug!("server stream handler: {}", handler_key);
                        let handler = server_stream_handlers.get(&handler_key).cloned();
                        let handler = handler.unwrap_or_else(|| Arc::new(DefaultHandler));
                        let Some(ret) = handler.request_reply(response).in_current_span().await
                        else {
                            debug!(
                                "handler no response, don't need to send to server. skip. key:{}",
                                handler_key
                            );
                            continue;
                        };
                        let ret = local_sender_clone.send(ret).await;
                        if let Err(e) = ret {
                            error!("send grpc message to server occur an error, {}", e);
                            break;
                        }
                    }
                    warn!("server stream closed!");
                    // 控制流结束后撤销健康，后续就绪轮询负责销毁旧传输并进入重连。
                    let _ =
                        health.compare_exchange(true, false, Ordering::SeqCst, Ordering::Acquire);
                }
                .instrument(span)
                .await;
            }
            .in_current_span(),
        );

        let _ = waiter.await;
        Ok(conn_id_sender)
    }

    /// 业务作用：在初始化期间发送健康请求，以有限响应等待探测新传输是否能交换协议消息。
    ///
    /// 参数说明：
    /// - `service`：本次初始化的传输服务；调用方负责提供可接受请求的服务。
    ///
    /// 返回：三秒内收到且可按 HealthCheckResponse 解码时成功；超时、回调关闭、传输或解码失败返回错误，不额外判断正文业务结果码。
    async fn connection_health_check(service: &mut M::Service) -> Result<(), Error> {
        info!("connection health check");

        let request = utils::convert(
            GrpcMessageBuilder::new(HealthCheckRequest::default())
                .build()
                .into_payload(),
            "health check request message convert to grpc message occur an error",
        )?;

        let (cb, rx, mut tk) = utils::create_grpc_callback::<Result<Payload, Error>>();
        let grpc_call = NacosGrpcCall::RequestService((request, cb));
        executor::spawn(service.call(grpc_call));

        tk.want();
        let response = match tokio::time::timeout(HEALTH_CHECK_REQUEST_TIMEOUT, rx).await {
            Err(_) => {
                warn!(
                    "connection health check timeout after {:?}",
                    HEALTH_CHECK_REQUEST_TIMEOUT
                );
                return Err(ErrResult(
                    "connection health check request timeout".to_string(),
                ));
            }
            Ok(resp) => utils::convert(
                utils::recv_response(resp, "grpc request callback failed")?,
                "connection health check failed",
            )?,
        };

        let response = GrpcMessage::<HealthCheckResponse>::from_payload(response);
        if let Err(e) = response {
            let err_msg = "connection health check failed, convert to grpc message failed";
            warn!(
                "{}. If the retry is successful, please ignore it: {}",
                err_msg, e
            );
            return Err(ErrResult(err_msg.to_string()));
        }

        Ok(())
    }

    /// 业务作用：通过服务检查请求获取当前连接身份，作为初始化发布的必要输入。
    ///
    /// 参数说明：
    /// - `service`：已发起控制流的当前传输服务。
    ///
    /// 返回：响应可解码且 connection_id 为 Some 时移交 ID；编码、回调、传输、解码或身份缺失返回错误。不额外校验空字符串或设置独立等待超时。
    async fn check_server(service: &mut M::Service) -> Result<String, Error> {
        info!("check server");

        let request = utils::convert(
            GrpcMessageBuilder::new(ServerCheckRequest::new())
                .build()
                .into_payload(),
            "server check request message convert to grpc message occur an error",
        )?;

        let (cb, rx, mut tk) = utils::create_grpc_callback::<Result<Payload, Error>>();
        let grpc_call = NacosGrpcCall::RequestService((request, cb));
        executor::spawn(service.call(grpc_call));

        tk.want();
        let response = utils::convert(
            utils::recv_response(rx.await, "grpc request callback failed")?,
            "check server failed",
        )?;

        let response = utils::convert(
            GrpcMessage::<ServerCheckResponse>::from_payload(response),
            "check server failed convert to grpc message failed",
        )?;

        let connection_id = utils::unwrap_option(
            response.into_body().connection_id,
            "check server failed connection id is empty",
        )?;

        Ok(connection_id)
    }
}

pub(crate) enum State<F, S> {
    /// 尚未启动下一次传输建立。
    Idle,
    /// 正在通过工厂建立传输服务。
    Connecting(Pin<Box<F>>),
    /// 正在初始化控制流并取得连接身份。
    Initializing(Pin<Box<dyn Future<Output = Result<(S, String), Error>> + Send>>),
    /// 已完成初始化的服务，发送前仍须检查传输和健康状态。
    Connected(S),
    /// 失败后的退避等待，完成后重新进入 Idle。
    Retry(Pin<Box<dyn Future<Output = ()> + Send>>),
}

impl<M> Service<Payload> for NacosGrpcConnection<M>
where
    M: MakeService<(), NacosGrpcCall, MakeError = Error, Error = Error> + Send + 'static,
    M::Error: Send + 'static,
    M::Response: Send + 'static,
    M::Future: Send + 'static,
    M::Service: Send + 'static,
    <M::Service as Service<NacosGrpcCall>>::Future: Send + 'static,
{
    type Response = Payload;

    type Error = Error;

    type Future = ResponseFuture;

    /// 业务作用：推进连接建立、初始化、健康判断和退避状态，决定请求能否继续提交。
    ///
    /// 参数说明：
    /// - `cx`：等待底层传输、握手或退避完成时使用的唤醒上下文。
    ///
    /// 返回：正常已连接且健康时就绪，未完成步骤时等待；首次初始化达到阈值也返回就绪供 call 报告未连接。连接失败进入退避，成功后发布身份和健康，后续重连不受首次阈值限制。
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let _span_enter =
            debug_span!(parent: None, "grpc_connection", id = self.id.clone()).entered();

        loop {
            if !self.is_initialized {
                let max_retries = self.max_retries.unwrap_or(1);
                if self.retry_count >= max_retries {
                    error!(
                        "Connection initialization failed after {} attempts. \
                         The Nacos server may be unreachable. \
                         Please check server address and network connectivity.",
                        max_retries
                    );
                    // 应急启动允许连接尚未就绪时继续装配；poll_ready 成功不保证后续 call 可发送。
                    self.retry_count = 0;
                    return Poll::Ready(Ok(()));
                }
            }

            match self.state {
                State::Idle => {
                    // 开始下一次连接尝试前撤销已发布身份，让观察者能识别旧会话结束。
                    info!("create new connection.");
                    let send_ret = self.connection_id_watcher.0.send(None);
                    if let Err(e) = send_ret {
                        // 观察通知失败不产生可用身份，仍继续建立新传输并由初始化决定是否发布。
                        warn!(
                            "connection id watch channel exception, send to receiver error: {}",
                            e
                        );
                    }
                    let mk_fut = self.mk_service.make_service(());
                    self.state = State::Connecting(Box::pin(mk_fut));
                    continue;
                }
                State::Connecting(ref mut fut) => {
                    let ret = fut.as_mut().poll(cx);
                    match ret {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(ret)) => {
                            let init_future = Box::pin(NacosGrpcConnection::<M>::init_connection(
                                ret,
                                self.client_version.clone(),
                                self.namespace.clone(),
                                self.labels.clone(),
                                self.client_abilities.clone(),
                                self.handler_map.clone(),
                                self.health.clone(),
                                self.last_active.clone(),
                            ));
                            self.state = State::Initializing(init_future);
                            continue;
                        }
                        Poll::Ready(Err(e)) => {
                            // 建立失败不能进入可发送状态，递增失败计数并退避后重新选择地址。
                            self.retry_count += 1;
                            let sleep_time = sleep_time(self.retry_count);
                            error!(
                                "create connection error, this operate will be retry after {} sec, retry count:{}. {}",
                                sleep_time, self.retry_count, e
                            );
                            self.state = State::Retry(Box::pin(sleep(Duration::from_secs(
                                sleep_time as u64,
                            ))));
                            continue;
                        }
                    }
                }
                State::Initializing(ref mut init) => {
                    info!("the new connection is initializing.");

                    let ret = init.as_mut().poll(cx);
                    match ret {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok((service, connection_id))) => {
                            info!(
                                "the new connection init completed. connection id :{}",
                                connection_id
                            );

                            // 先发送身份观察值，再更新本地健康与状态；这些独立写入不是原子发布。
                            let send_ret = self
                                .connection_id_watcher
                                .0
                                .send(Some(connection_id.clone()));
                            if let Err(e) = send_ret {
                                // 观察者通知失败会被记录，但不替代已经取得的握手结果。
                                warn!(
                                    "connection id watch channel exception, send connection id:{} to receiver error: {}",
                                    connection_id, e
                                );
                            }

                            self.retry_count = 0;
                            self.health.store(true, Ordering::Release);
                            self.last_active.store(now_millis(), Ordering::Release);
                            self.is_initialized = true;
                            self.state = State::Connected(service);
                            self.connection_id = Some(connection_id);
                            continue;
                        }
                        Poll::Ready(Err(e)) => {
                            // 未完成初始化的服务不可用于业务发送，等待后重新执行完整握手。
                            self.retry_count += 1;
                            let sleep_time = sleep_time(self.retry_count);
                            error!(
                                "initializing connection error, this operate will be retry after {} sec, retry count:{}. {}",
                                sleep_time, self.retry_count, e
                            );
                            self.state = State::Retry(Box::pin(sleep(Duration::from_secs(
                                sleep_time as u64,
                            ))));
                            continue;
                        }
                    }
                }
                State::Connected(ref mut service) => {
                    let ready = service.poll_ready(cx);
                    match ready {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(_)) => {
                            if !self.health.load(Ordering::Acquire) {
                                // 底层可写不能覆盖控制流失效或健康探测失败的证据，丢弃旧服务后重连。
                                self.retry_count += 1;
                                let sleep_time = sleep_time(self.retry_count);
                                error!(
                                    "the connection {:?} is not in health, destroy connection and retry, this operate will be retry after {} sec, retry count:{}.",
                                    self.connection_id, sleep_time, self.retry_count
                                );
                                self.state = State::Retry(Box::pin(sleep(Duration::from_secs(
                                    sleep_time as u64,
                                ))));
                                continue;
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(Err(e)) => {
                            // 传输已不允许请求时先撤销健康，再丢弃服务进入重连退避。
                            self.health.store(false, Ordering::Release);
                            self.retry_count += 1;
                            let sleep_time = sleep_time(self.retry_count);
                            error!(
                                "connection {:?} not ready, destroy connection and retry, this operate will be retry after {} sec, retry count:{}. {}",
                                self.connection_id, sleep_time, self.retry_count, e
                            );
                            self.state = State::Retry(Box::pin(sleep(Duration::from_secs(
                                sleep_time as u64,
                            ))));
                            continue;
                        }
                    }
                }

                State::Retry(ref mut sleep) => {
                    let ret = sleep.as_mut().poll(cx);
                    if ret == Poll::Pending {
                        return Poll::Pending;
                    }
                    self.state = State::Idle;
                    continue;
                }
            }
        }
    }

    /// 业务作用：把已连接状态下的请求交给传输任务，并将回调响应映射为等待 Future。
    ///
    /// 参数说明：
    /// - `req`：待发送 Payload；调用方须先完成 poll_ready，call 不重新检查健康标记。
    ///
    /// 返回：已连接时启动发送并等待回调；收到回调结果后更新活动时间，即使结果是传输错误。其它状态返回未连接错误，不缓存请求等待重连。
    fn call(&mut self, req: Payload) -> Self::Future {
        let conn_id = if let Some(ref conn_id) = self.connection_id {
            conn_id.clone()
        } else {
            "None".to_string()
        };

        let _span_enter = debug_span!("grpc_connection", conn_id = conn_id).entered();

        match self.state {
            State::Connected(ref mut service) => {
                let (cb, rx, mut tk) = utils::create_grpc_callback::<Result<Payload, Error>>();
                let grpc_call = NacosGrpcCall::RequestService((req, cb));
                let call_task = service.call(grpc_call).in_current_span();
                let last_active = self.last_active.clone();
                let response_fut = async move {
                    tk.want();
                    let response = utils::recv_response(rx.await, "sender has been drop")?;
                    last_active.store(now_millis(), Ordering::Release);
                    response
                }
                .in_current_span();
                executor::spawn(call_task);
                ResponseFuture::new(response_fut)
            }
            _ => {
                // 初始化阈值可能让就绪检查提前返回，但未连接状态仍必须拒绝实际发送。
                let fut: Pin<Box<dyn Future<Output = Result<Payload, Error>> + Send>> = Box::pin(
                    async move { Err(ErrResult("the connection is not connected".to_string())) }
                        .in_current_span(),
                );
                ResponseFuture::new(fut)
            }
        }
    }
}

pub(crate) struct ResponseFuture {
    /// 单次请求的回调等待或未连接拒绝结果。
    inner: Pin<Box<dyn Future<Output = Result<Payload, Error>> + Send>>,
}

impl ResponseFuture {
    /// 业务作用：统一连接请求的响应等待类型，隐藏实际回调或拒绝分支的 Future 实现。
    ///
    /// 参数说明：
    /// - `inner`：最终返回 Payload 或连接错误的异步任务。
    ///
    /// 返回：固定地址的响应包装器，不轮询任务、重发请求或转换业务正文。
    pub(crate) fn new<F>(inner: F) -> Self
    where
        F: Future<Output = Result<Payload, Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Future for ResponseFuture {
    type Output = Result<Payload, Error>;

    /// 业务作用：推进请求响应等待，将底层完成状态原样交给调用方。
    ///
    /// 参数说明：
    /// - `cx`：等待回调响应任务的唤醒上下文。
    ///
    /// 返回：底层等待、响应或错误状态，不额外延长超时或修改活动记录。
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

pub(crate) struct FailoverConnection<S>
where
    S: Service<Payload, Error = Error, Response = Payload> + Send + 'static,
    S::Future: Send + 'static,
{
    /// 共享发送入口的实例诊断标识。
    id: String,
    /// 容量为 1024 的请求缓冲，worker 独占底层连接状态机。
    inner: Buffer<Payload, S::Future>,
    /// 外部故障切换和探测任务可撤销的共享健康标记。
    svc_health: Arc<AtomicBool>,
    /// 健康任务是否允许继续下一轮探测。
    active_health_check: Arc<AtomicBool>,
}

impl<S> FailoverConnection<S>
where
    S: Service<Payload, Error = Error, Response = Payload> + Send + 'static,
    S::Future: Send + 'static,
{
    /// 业务作用：用有界缓冲连接共享调用方与状态机，并启动空闲健康探测任务。
    ///
    /// 参数说明：
    /// - `id`：发送入口和健康任务使用的实例诊断标识。
    /// - `svc`：拥有连接生命周期的单一底层服务。
    /// - `svc_health`：底层服务判断是否需要重连的共享健康标记。
    /// - `svc_last_active`：请求和控制流更新的 Unix 毫秒活动记录。
    ///
    /// 返回：容量为 1024 的共享发送入口；启动缓冲 worker 和健康任务，但不等待建连完成，需要可用异步执行环境。
    pub(crate) fn new(
        id: String,
        svc: S,
        svc_health: Arc<AtomicBool>,
        svc_last_active: Arc<AtomicU64>,
    ) -> Self {
        let (inner, work) = Buffer::pair(svc, 1024);
        executor::spawn(work);

        let active_health_check = Arc::new(AtomicBool::new(true));

        // 健康任务复用同一缓冲入口，所有请求仍由唯一连接状态机决定何时发送。
        executor::spawn(
            FailoverConnection::<S>::health_check(
                inner.clone(),
                active_health_check.clone(),
                svc_health.clone(),
                svc_last_active.clone(),
            )
            .instrument(debug_span!("health_check", id = id)),
        );

        Self {
            id,
            inner,
            svc_health,
            active_health_check,
        }
    }

    /// 业务作用：撤销当前健康证据，使后续就绪检查触发底层连接重建。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：健康为真时置为假，已为假则保持；不立即关闭 Channel、通知断连或等待重连完成。
    #[allow(dead_code)]
    #[instrument(fields(id = self.id), skip_all)]
    pub(crate) fn failover(&self) {
        // 仅撤销健康，不在调用线程并行创建连接，避免绕开状态机的生命周期管理。
        let _ = self
            .svc_health
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::Acquire);
    }

    /// 业务作用：连接空闲后经同一发送入口探测健康，连续失败时撤销健康证据供状态机重连。
    ///
    /// 参数说明：
    /// - `svc`：共享缓冲发送入口，探测与业务请求经过同一连接状态机。
    /// - `active_health_check`：每轮开始读取的任务继续标记，关闭不立即中断当前等待。
    /// - `svc_health`：探测失败时撤销的健康标记；成功探测不在此重新授予健康。
    /// - `last_active`：Unix 毫秒活动记录，用饱和减法处理系统时间回拨。
    ///
    /// 返回：继续标记关闭后于后续循环结束；空闲满五秒才探测，最多三次、每次响应等待三秒。就绪等待不受该超时约束，响应可解码即视为成功，不另查业务码。
    async fn health_check(
        mut svc: Buffer<Payload, S::Future>,
        active_health_check: Arc<AtomicBool>,
        svc_health: Arc<AtomicBool>,
        last_active: Arc<AtomicU64>,
    ) {
        while active_health_check.load(Ordering::Acquire) {
            debug!("health check.");

            // 有近期请求或推送活动时延后探测，减少活跃连接上的额外心跳负担。
            let idle_millis = now_millis().saturating_sub(last_active.load(Ordering::Acquire));
            if idle_millis < HEALTH_CHECK_INTERVAL.as_millis() as u64 {
                sleep(HEALTH_CHECK_INTERVAL).await;
                continue;
            }

            let mut healthy = false;
            for attempt in 0..HEALTH_CHECK_RETRY_TIMES {
                if attempt > 0 {
                    // 重试加入短随机间隔，降低多个客户端同时探测故障节点的集中压力。
                    let jitter = Duration::from_millis(rand::random_range(..500_u64));
                    sleep(jitter).await;
                }

                let Ok(health_check_request) =
                    GrpcMessageBuilder::new(HealthCheckRequest::default())
                        .build()
                        .into_payload()
                else {
                    error!("health check failed, grpc message can not convert to payload. retry.");
                    break;
                };

                let ready = futures_util::future::poll_fn(|cx| svc.poll_ready(cx))
                    .in_current_span()
                    .await;
                if ready.is_err() {
                    // 发送入口已拒绝请求，不再继续构造探测，以失败证据交给连接状态机处理。
                    warn!("connection not ready, wait.");
                    break;
                }

                let call_result = tokio::time::timeout(
                    HEALTH_CHECK_REQUEST_TIMEOUT,
                    svc.call(health_check_request).in_current_span(),
                )
                .await;

                match call_result {
                    Err(_) => {
                        error!(
                            "health check failed, request timeout after {:?}, retry.",
                            HEALTH_CHECK_REQUEST_TIMEOUT
                        );
                    }
                    Ok(Err(e)) => {
                        error!("health check failed, send health check request failed, retry. {e}");
                    }
                    Ok(Ok(response)) => {
                        if GrpcMessage::<HealthCheckResponse>::from_payload(response).is_ok() {
                            healthy = true;
                            break;
                        }
                        error!("health check failed, error response, retry.");
                    }
                }
            }

            if healthy {
                // 成功仅延后下一轮探测，不能覆盖控制流已经撤销的健康标记。
                last_active.store(now_millis(), Ordering::Release);
            } else {
                // 撤销健康后由状态机统一重连，探测任务不另起并行连接。
                let _ =
                    svc_health.compare_exchange(true, false, Ordering::SeqCst, Ordering::Acquire);
            }

            sleep(HEALTH_CHECK_INTERVAL).await;
        }

        warn!("stop health check task.");
    }
}

impl<S> Drop for FailoverConnection<S>
where
    S: Service<Payload, Error = Error, Response = Payload> + Send + 'static,
    S::Future: Send + 'static,
{
    /// 业务作用：发送入口释放后请求健康任务停止继续启动下一轮探测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：关闭继续标记；不会等待或中断正在进行的就绪、响应及休眠，也不保证底层资源已立即排干。
    fn drop(&mut self) {
        self.active_health_check.store(false, Ordering::Release);
    }
}

#[async_trait]
pub(crate) trait SendRequest: Send {
    /// 业务作用：通过共享连接入口提交一条协议请求，并等待对应响应。
    ///
    /// 参数说明：
    /// - `request`：已经编码的请求 Payload，其所有权交给发送实现。
    ///
    /// 返回：可用于上层解码的响应，或连接、发送、等待失败；成功不直接表示 Nacos 业务操作成功。
    async fn send_request(&self, request: Payload) -> Result<Payload, Error>;
}

#[async_trait]
impl<S> SendRequest for FailoverConnection<S>
where
    S: Service<Payload, Error = Error, Response = Payload> + Send + 'static,
    S::Future: Send + 'static,
{
    /// 业务作用：等待缓冲入口可提交后发送请求，复用连接状态机的就绪与故障转移行为。
    ///
    /// 参数说明：
    /// - `request`：移入缓冲入口的已编码请求，不在本方法复制或重放。
    ///
    /// 返回：收到的 Payload；就绪错误向上传递，调用错误映射为 GrpcBufferRequest。不添加独立请求超时。
    #[instrument(fields(id = self.id), skip_all)]
    async fn send_request(&self, request: Payload) -> Result<Payload, Error> {
        let mut svc = self.inner.clone();
        let _ = future::poll_fn(|cx| svc.poll_ready(cx))
            .in_current_span()
            .await?;
        let ret = svc.call(request).in_current_span().await;
        ret.map_err(GrpcBufferRequest)
    }
}
