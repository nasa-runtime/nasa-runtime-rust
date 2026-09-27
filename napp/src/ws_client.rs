//! 现有 TCP 帧协议出站客户端的命名配置、回调屏障和子任务退出责任。

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationError, ApplicationFuture, ApplicationMode, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ShutdownAction,
    ShutdownContext,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    #[serde(default)]
    enabled: bool,
    address: Option<String>,
    endpoint: Option<String>,
    token: Option<String>,
    device_id: Option<String>,
    version: Option<String>,
    connect_timeout_ms: Option<u64>,
    reconnect_min_ms: Option<u64>,
    reconnect_max_ms: Option<u64>,
    queue_capacity: Option<usize>,
    max_frame_bytes: Option<usize>,
    #[serde(default)]
    auto_reconnect: bool,
    #[serde(default)]
    critical: bool,
}
#[derive(Default)]
struct Registry {
    sealed: bool,
    handlers: BTreeMap<String, BTreeMap<String, naws::client::EventCallback>>,
}
struct Entry {
    name: String,
    handle: ManagedWsClient,
    health: Option<ReadinessContributor>,
    critical: bool,
}
#[derive(Default)]
pub(crate) struct WsClients {
    registry: Mutex<Registry>,
    entries: Mutex<Vec<Entry>>,
    closed: AtomicBool,
}

/// 出站 TCP 客户端业务句柄；不暴露 connect、close 或可变回调登记权。
#[derive(Clone)]
pub struct ManagedWsClient {
    client: naws::Client,
    closed: Arc<AtomicBool>,
}
impl ManagedWsClient {
    /// 业务作用：向已认证并激活的连接提交有界业务消息。
    /// 参数说明：`message` 为原协议消息，业务继续负责远端确认与幂等。
    /// 返回：true 仅表示本地入队；未激活、关闭、超限或队列满时为 false。
    pub fn send_message(&self, message: &naws::proto::Message) -> bool {
        !self.closed.load(Ordering::Acquire) && self.client.send_message(message)
    }
    /// 业务作用：发送当前 endpoint 的业务事件。
    /// 参数说明：`event` 为协议事件名；`payload` 为原始消息正文。
    /// 返回：本地入队是否成功，不代表远端执行或送达。
    pub fn send(&self, event: &str, payload: &[u8]) -> bool {
        !self.closed.load(Ordering::Acquire) && self.client.send(event, payload)
    }
    /// 业务作用：读取最近认证连接的本地状态，不触发探测或重连。
    /// 参数说明：无。
    /// 返回：准入未关闭且当前连接存在时为 true。
    pub fn is_connected(&self) -> bool {
        !self.closed.load(Ordering::Acquire) && self.client.is_connected()
    }
    /// 业务作用：读取连接子任务的实际存活数。
    /// 参数说明：无。
    /// 返回：supervisor、writer、heartbeat 的当前数量。
    pub fn active_tasks(&self) -> usize {
        self.client.active_tasks()
    }
    /// 业务作用：观察已隔离的业务回调异常次数，便于定位本地处理失败。
    /// 参数说明：无。
    /// 返回：当前客户端累计的回调 panic 次数，不含消息正文。
    pub fn callback_failures(&self) -> usize {
        self.client.callback_failures()
    }

    /// 业务作用：读取连接、认证、心跳及失败的独立本地证据。
    /// 参数说明：无。
    /// 返回：不包含地址、会话或凭据的客户端快照。
    pub fn observation(&self) -> naws::client::ClientObservation {
        self.client.observation()
    }
}
impl WsClients {
    /// 业务作用：保留固定关键目录的健康句柄，供取得所有领域保护后检查新鲜度。
    /// 参数说明：无。
    /// 返回：准备阶段封存的关键证据集合，不包含可选依赖。
    pub(crate) fn critical_health(&self) -> Vec<ReadinessContributor> {
        self.entries
            .lock()
            .expect("outbound clients")
            .iter()
            .filter(|entry| entry.critical)
            .filter_map(|entry| entry.health.clone())
            .collect()
    }
    /// 业务作用：持有关键连接的本地状态保护直到唯一 Ready 发布，防止使用准备阶段旧证据。
    /// 参数说明：`publish` 只推进宿主状态和公共许可，不执行业务或阻塞操作。
    /// 返回：任一关键连接失效或证据过期时拒绝启动，否则返回发布结果。
    pub(crate) fn publish_ready(
        &self,
        publish: &mut dyn FnMut() -> ApplicationResult<()>,
    ) -> ApplicationResult<()> {
        let entries = self.entries.lock().expect("outbound clients");
        // 关闭权威已生效时不再接受任何启动发布，旧连接即使迟到认证也不能恢复本代。
        if self.closed.load(Ordering::Acquire) {
            return Err(ApplicationError::new(
                ComponentId::Application,
                ApplicationPhase::Ready,
                "outbound clients already closing",
            ));
        }
        Self::guard_entries(&entries, &mut || {
            if entries.iter().any(|entry| {
                entry.critical
                    && entry
                        .health
                        .as_ref()
                        .is_some_and(|health| !health.ready_now())
            }) {
                return Err(ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Ready,
                    "outbound client readiness evidence expired",
                ));
            }
            publish()
        })
    }

    /// 业务作用：按固定目录顺序嵌套连接保护，使较早复验的连接在最终发布前不能失效。
    /// 参数说明：`entries` 为最多 64 个已封存条目；`publish` 为后续领域和状态发布。
    /// 返回：已失去认证的关键连接阻止后续执行；可选连接只贡献降级状态。
    fn guard_entries(
        entries: &[Entry],
        publish: &mut dyn FnMut() -> ApplicationResult<()>,
    ) -> ApplicationResult<()> {
        let Some((entry, rest)) = entries.split_first() else {
            return publish();
        };
        let continue_ready = &mut || {
            if let Some(health) = &entry.health {
                health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            }
            Self::guard_entries(rest, publish)
        };
        if entry.critical {
            // 本地断连与 Ready 使用同一保护范围；远端尚未被观察到的故障按运行期事件处理。
            entry
                .handle
                .client
                .with_authenticated_connection(continue_ready)
                .unwrap_or_else(|| {
                    if let Some(health) = &entry.health {
                        health.observe(DependencyState::NotReady, reason::DEGRADED, Instant::now());
                    }
                    Err(ApplicationError::new(
                        ComponentId::Application,
                        ApplicationPhase::Ready,
                        "critical outbound client lost authenticated connection before Ready",
                    ))
                })
        } else {
            if let Some(health) = &entry.health {
                let healthy =
                    entry.handle.client.is_connected() && entry.handle.client.is_running();
                health.observe(
                    if healthy {
                        DependencyState::Ready
                    } else {
                        DependencyState::Degraded
                    },
                    if healthy {
                        reason::HEALTHY
                    } else {
                        reason::DEGRADED
                    },
                    Instant::now(),
                );
            }
            Self::guard_entries(rest, publish)
        }
    }
    /// 业务作用：冻结前登记有界业务事件目录，不创建任何连接。
    /// 参数说明：`name` 是命名客户端；`event` 是协议事件名；`handler` 必须同步非阻塞。
    /// 返回：唯一合法登记成功，重复、超量或封口后拒绝。
    pub(crate) fn register(
        &self,
        name: &str,
        event: &str,
        handler: naws::client::EventCallback,
    ) -> ApplicationResult<()> {
        let mut registry = self.registry.lock().expect("outbound client registrations");
        if registry.sealed
            || !valid_name(name)
            || !valid_name(event)
            || registry.handlers.len() >= 64 && !registry.handlers.contains_key(name)
        {
            return Err(error("invalid outbound client registration"));
        }
        let handlers = registry.handlers.entry(name.into()).or_default();
        if handlers.len() >= 256 || handlers.contains_key(event) {
            return Err(error("duplicate or excessive outbound event registration"));
        }
        handlers.insert(event.into(), handler);
        Ok(())
    }
    /// 业务作用：从启动快照创建固定材料的出站客户端，支持无入站服务的 Service 与 Batch。
    /// 参数说明：`context` 提供材料、命名资源和清理栈。
    /// 返回：全部启用计划完成首连认证后成功；准备失败由已登记 owner 关闭。
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        context: &mut PrepareContext<'_>,
    ) -> ApplicationResult<Option<ApplicationFuture<'static>>> {
        let app = context.application().clone();
        let plans: BTreeMap<String, Plan> = serde_json::from_value(
            app.config()
                .value()
                .get("ws_clients")
                .cloned()
                .unwrap_or(serde_json::json!({})),
        )
        .map_err(|_| error("invalid ws_clients declaration"))?;
        if plans.len() > 64 {
            return Err(error("outbound client count exceeds 64"));
        }
        let mut handlers = {
            let mut registry = self.registry.lock().expect("outbound client registrations");
            registry.sealed = true;
            std::mem::take(&mut registry.handlers)
        };
        let batch = app.info().mode() == ApplicationMode::Batch;
        if batch && !handlers.is_empty() {
            return Err(error(
                "Batch outbound clients do not accept persistent callback plans",
            ));
        }
        let mut total = 0usize;
        for (name, plan) in plans.iter().filter(|(_, p)| p.enabled) {
            if !valid_name(name)
                || plan.address.as_ref().is_none_or(|a| {
                    a.is_empty() || a.len() > 512 || a.contains('@') || a.contains("://")
                })
            {
                return Err(error("invalid outbound client identity"));
            }
            let timeout = plan.connect_timeout_ms.unwrap_or(5000);
            let min = plan.reconnect_min_ms.unwrap_or(200);
            let max = plan.reconnect_max_ms.unwrap_or(5000);
            let count = plan.queue_capacity.unwrap_or(256);
            let bytes = plan.max_frame_bytes.unwrap_or(65536);
            if !(1..=300000).contains(&timeout)
                || min == 0
                || min > max
                || max > 60000
                || !(1..=4096).contains(&count)
                || !(1..=16 * 1024 * 1024).contains(&bytes)
            {
                return Err(error("invalid outbound client capacity or duration"));
            }
            for value in [&plan.endpoint, &plan.device_id, &plan.version]
                .into_iter()
                .flatten()
            {
                if value.len() > 512 {
                    return Err(error("outbound protocol field exceeds limit"));
                }
            }
            total = total
                .checked_add(
                    count
                        .checked_mul(bytes)
                        .ok_or_else(|| error("outbound capacity overflow"))?,
                )
                .ok_or_else(|| error("outbound capacity overflow"))?;
        }
        if total > 256 * 1024 * 1024 {
            return Err(error("outbound frame capacity exceeds 256 MiB"));
        }
        // 首连 future 可能被启动取消打断，聚合关闭动作必须先持有候选客户端。
        context.activate(Box::new(WsClientsShutdown(self.clone())));
        for (name, plan) in plans {
            if !plan.enabled {
                continue;
            }
            let token = match plan.token.as_deref() {
                None => String::new(),
                Some(locator) => {
                    let id = locator
                        .strip_prefix("secret://")
                        .ok_or_else(|| error("outbound token requires secret locator"))?;
                    let secrets = app.secrets();
                    let bytes = secrets
                        .get(id)
                        .ok_or_else(|| error("outbound credential is missing"))?;
                    let token = std::str::from_utf8(bytes.expose())
                        .map_err(|_| error("outbound credential is not UTF-8"))?;
                    if token.len() > 16384 {
                        return Err(error("outbound credential exceeds limit"));
                    }
                    token.to_owned()
                }
            };
            let client = naws::Client::builder(plan.address.unwrap())
                .endpoint(plan.endpoint.unwrap_or_else(|| "/ws".into()))
                .token(token)
                .device_id(plan.device_id.unwrap_or_default())
                .version(plan.version.unwrap_or_else(|| "1.0".into()))
                .connect_timeout(Duration::from_millis(
                    plan.connect_timeout_ms.unwrap_or(5000),
                ))
                .auto_reconnect(plan.auto_reconnect)
                .reconnect_backoff(
                    Duration::from_millis(plan.reconnect_min_ms.unwrap_or(200)),
                    Duration::from_millis(plan.reconnect_max_ms.unwrap_or(5000)),
                )
                .capacity(
                    plan.queue_capacity.unwrap_or(256),
                    plan.max_frame_bytes.unwrap_or(65536),
                )
                .activation_barrier(if batch {
                    tokio_util::sync::CancellationToken::new()
                } else {
                    app.startup_activation()
                })
                .build();
            for (event, handler) in handlers.remove(&name).unwrap_or_default() {
                client.on_event(event, move |message| handler(message));
            }
            let handle = ManagedWsClient {
                client: client.clone(),
                closed: Arc::new(AtomicBool::new(false)),
            };
            self.entries.lock().expect("outbound clients").push(Entry {
                name: name.clone(),
                handle: handle.clone(),
                health: None,
                critical: plan.critical,
            });
            client
                .connect()
                .await
                .map_err(|_| error("outbound client initial authentication failed"))?;
            if batch {
                client.activate();
            } else {
                let health = app.register_readiness(
                    ComponentId::Application,
                    Arc::<str>::from(format!("ws-client:{name}")),
                    ReadinessPolicy {
                        affects_ready: plan.critical,
                        failure_threshold: 1,
                        recovery_threshold: 1,
                        stale_after: Some(Duration::from_secs(5)),
                    },
                )?;
                health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                self.entries
                    .lock()
                    .expect("outbound clients")
                    .last_mut()
                    .unwrap()
                    .health = Some(health);
            }
            context.register_resource(Some(&name), handle)?;
        }
        if !handlers.is_empty() {
            return Err(error(
                "outbound callback references unknown or disabled client",
            ));
        }
        if batch || self.entries.lock().expect("outbound clients").is_empty() {
            return Ok(None);
        }
        let owner = self.clone();
        Ok(Some(Box::pin(async move { owner.monitor(app).await })))
    }
    /// 业务作用：统一 Ready 后持续根据认证连接与任务状态更新健康。
    /// 参数说明：`app` 提供状态订阅，不参与业务消息处理。
    /// 返回：宿主停止时正常结束，关键 owner 意外退出时报告失败。
    async fn monitor(self: Arc<Self>, app: Application) -> ApplicationResult<()> {
        let mut states = app.subscribe_state();
        loop {
            match *states.borrow_and_update() {
                ApplicationState::Starting => {}
                ApplicationState::Ready => {
                    let entries = self.entries.lock().expect("outbound clients");
                    if self.closed.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    for entry in entries.iter() {
                        let healthy = entry.handle.client.is_connected();
                        if let Some(health) = &entry.health {
                            health.observe(
                                if healthy {
                                    DependencyState::Ready
                                } else if entry.critical {
                                    DependencyState::NotReady
                                } else {
                                    DependencyState::Degraded
                                },
                                if healthy {
                                    reason::HEALTHY
                                } else {
                                    reason::DEGRADED
                                },
                                Instant::now(),
                            );
                        }
                        if !entry.handle.client.is_running() && entry.critical {
                            return Err(ApplicationError::new(
                                ComponentId::Application,
                                ApplicationPhase::Running,
                                "critical outbound client owner exited",
                            ));
                        }
                    }
                }
                _ => return Ok(()),
            }
            tokio::select! {biased;_=states.changed()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
        }
    }
    /// 业务作用：同时关闭所有客户端发送与重连准入，旧句柄不能复活。
    /// 参数说明：无。
    /// 返回：已发关闭请求，实际退出由连接 owner 继续等待。
    fn close(&self) {
        let entries = self.entries.lock().expect("outbound clients");
        self.closed.store(true, Ordering::Release);
        for entry in entries.iter() {
            entry.handle.closed.store(true, Ordering::Release);
            entry.handle.client.close();
            if let Some(health) = &entry.health {
                health.observe(DependencyState::NotReady, reason::DEGRADED, Instant::now());
            }
        }
    }
    /// 业务作用：并发等待全部客户端的 supervisor、writer、heartbeat 退出。
    /// 参数说明：无。
    /// 返回：所有运行门禁释放且任务计数归零后成功。
    async fn drain(&self) {
        let clients: Vec<_> = self
            .entries
            .lock()
            .expect("outbound clients")
            .iter()
            .map(|e| e.handle.client.clone())
            .collect();
        futures_util::future::join_all(clients.iter().map(naws::Client::wait_closed)).await;
    }
    /// 业务作用：读取固定命名目录下的连接和子任务观测。
    /// 参数说明：无。
    /// 返回：命名连接状态与活动子任务数，不包含地址、session 或 token。
    pub(crate) fn observations(&self) -> Vec<(String, bool, usize)> {
        self.entries
            .lock()
            .expect("outbound clients")
            .iter()
            .map(|e| {
                (
                    e.name.clone(),
                    e.handle.is_connected(),
                    e.handle.active_tasks(),
                )
            })
            .collect()
    }
}
struct WsClientsShutdown(Arc<WsClients>);
impl ShutdownAction for WsClientsShutdown {
    /// 业务作用：标识出站客户端聚合关闭动作。
    /// 参数说明：无。
    /// 返回：固定动作名。
    fn label(&self) -> &'static str {
        "ws-clients"
    }
    /// 业务作用：先关闭全部客户端，再用剩余预算取得真实退出证明。
    /// 参数说明：`context` 为宿主截止时间。
    /// 返回：正常收口成功，超时明确报告任务未全部退出。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.close();
        Box::pin(async move {
            tokio::time::timeout_at(context.deadline().into(), self.0.drain())
                .await
                .map_err(|_| {
                    ApplicationError::new(
                        ComponentId::Application,
                        ApplicationPhase::Stopping,
                        "outbound client shutdown incomplete",
                    )
                })?;
            Ok(())
        })
    }
}
impl Drop for WsClientsShutdown {
    /// 业务作用：启动回滚或取消关闭等待时收回全部出站准入。
    /// 参数说明：无。
    /// 返回：独立连接 owner 继续等待全部子任务退出。
    fn drop(&mut self) {
        self.0.close();
    }
}
/// 业务作用：限制命名客户端和事件目录身份。
/// 参数说明：`name` 为固定配置或协议名称。
/// 返回：非空且无首尾空白的有界名称有效。
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128 && name.trim() == name
}
/// 业务作用：提供不泄露认证材料的固定装配错误。
/// 参数说明：`message` 为静态原因。
/// 返回：标准宿主准备错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Prepare, message)
}
