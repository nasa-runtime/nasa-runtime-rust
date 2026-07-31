// ============================================================================
// src/endpoint.rs —— 端点 + 事件 handler(闭包 builder)。架构说明。
// handler 拆 Inline(同步,不可 await/阻塞)与 Async(提交 tokio,后续加并发上限)。
// 不用注解/反射(贴合地道 Rust 决定)。
// ============================================================================

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

use crate::session::SessionHandle;

/// endpoint async handler 统一装箱后的 future 类型。
pub type BoxFut = Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// 事件处理器。对齐 原实现 sync=true / sync=false 的两种执行语义。
#[derive(Clone)]
pub enum EventHandler {
    /// 读循环内同步执行:只允许短 CPU 操作,**不可 await/阻塞**。
    Inline(Arc<dyn Fn(SessionHandle, Bytes) + Send + Sync>),
    /// 提交 tokio 后台执行:可 await;不阻塞读循环(R0.2 直接 spawn,并发上限后续加)。
    Async(Arc<dyn Fn(SessionHandle, Bytes) -> BoxFut + Send + Sync>),
}

/// 生命周期 hook(同步,通常很短;join_group 等都是同步的)。
pub type LifecycleHandler = Arc<dyn Fn(SessionHandle) + Send + Sync>;

/// 端点:path + 事件表 + onConnect/onDisconnect。不可变快照。
pub struct Endpoint {
    path: String,
    events: HashMap<String, EventHandler>,
    on_connect: Option<LifecycleHandler>,
    on_disconnect: Option<LifecycleHandler>,
}

impl Endpoint {
    /// 创建 endpoint builder。
    ///
    /// # 参数
    /// - `path`: 连接入口路径,用于鉴权后绑定会话和分发入站事件。
    pub fn builder(path: impl Into<String>) -> EndpointBuilder {
        EndpointBuilder {
            path: path.into(),
            events: HashMap::new(),
            on_connect: None,
            on_disconnect: None,
        }
    }

    /// 返回端点路径；用于按连接入口查找事件处理器。
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 查找端点事件处理器；用于把入站事件分派到业务回调。
    ///
    /// # 参数
    /// - `name`: 业务事件名。
    pub fn event(&self, name: &str) -> Option<&EventHandler> {
        self.events.get(name)
    }

    /// 返回连接建立回调；用于新会话创建后执行业务初始化。
    pub fn on_connect(&self) -> Option<&LifecycleHandler> {
        self.on_connect.as_ref()
    }

    /// 返回连接断开回调；用于会话释放时执行业务清理。
    pub fn on_disconnect(&self) -> Option<&LifecycleHandler> {
        self.on_disconnect.as_ref()
    }
}

/// 保存端点构造参数；用于注册路径、事件和生命周期回调。
pub struct EndpointBuilder {
    path: String,
    events: HashMap<String, EventHandler>,
    on_connect: Option<LifecycleHandler>,
    on_disconnect: Option<LifecycleHandler>,
}

impl EndpointBuilder {
    /// 返回连接建立回调；用于新会话创建后执行业务初始化。
    ///
    /// # 参数
    /// - `f`: 会话建立后执行的业务回调,入参是新建的会话句柄。
    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(SessionHandle) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// 返回连接断开回调；用于会话释放时执行业务清理。
    ///
    /// # 参数
    /// - `f`: 会话断开后执行的业务回调,入参是断开的会话句柄。
    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(SessionHandle) + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    /// 同步事件(对齐 原实现 sync=true)。事件名重复 / 空 → panic(配置期编程错误,快速失败)。
    ///
    /// # 参数
    /// - `event`: endpoint 内注册的业务事件名,用于匹配入站 `Message.events`。
    /// - `f`: 收到该事件时同步执行的处理器,入参为会话句柄和 payload。
    pub fn on_event_inline<F>(mut self, event: impl Into<String>, f: F) -> Self
    where
        F: Fn(SessionHandle, Bytes) + Send + Sync + 'static,
    {
        self.put_event(event.into(), EventHandler::Inline(Arc::new(f)));
        self
    }

    /// 异步事件(对齐 原实现 sync=false)。`f` 返回一个 future。事件名重复 / 空 → panic。
    ///
    ///
    /// # 参数
    /// - `event`: endpoint 内注册的业务事件名,用于匹配入站 `Message.events`。
    /// - `f`: 收到该事件时异步执行的处理器,入参为会话句柄和 payload。
    pub fn on_event_async<F, Fut>(mut self, event: impl Into<String>, f: F) -> Self
    where
        F: Fn(SessionHandle, Bytes) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let wrapped: Arc<dyn Fn(SessionHandle, Bytes) -> BoxFut + Send + Sync> =
            Arc::new(move |s, p| Box::pin(f(s, p)) as BoxFut);
        self.put_event(event.into(), EventHandler::Async(wrapped));
        self
    }

    /// 插入事件 handler:空名或重名 → panic(不静默覆盖)。
    ///
    /// # 参数
    /// - `event`: endpoint 内唯一的业务事件名。
    /// - `handler`: 业务处理函数,在匹配事件或任务时被调用。
    fn put_event(&mut self, event: String, handler: EventHandler) {
        assert!(
            !event.is_empty(),
            "event name must not be empty (endpoint {})",
            self.path
        );
        assert!(
            !self.events.contains_key(&event),
            "duplicate event handler '{event}' on endpoint {}",
            self.path
        );
        self.events.insert(event, handler);
    }

    /// 完成 builder 装配并返回可运行对象。
    pub fn build(self) -> Endpoint {
        Endpoint {
            path: self.path,
            events: self.events,
            on_connect: self.on_connect,
            on_disconnect: self.on_disconnect,
        }
    }
}

/// endpoint path / socket.io namespace 的最大字节数:CONNECT ok =
/// `40<ns>,{"sid":"<id>"}` 必须装进 `MIN_MAX_FRAME`(512)——256 之外给固定开销 +
/// sessionId(`s{seq}-{32hex}` ≤ ~60B)预留充足空间。注册期与 CONNECT 期都按此校验,
/// 杜绝"已注册 endpoint 的成功控制帧必超限 → 永远连不上且只见静默断开"的半可用配置。
pub const MAX_ENDPOINT_LEN: usize = 256;

/// endpoint path 不变量(`register` 与 `replace` **共用**,任何写入口都不得绕过;
///R23 P2):非空 + ≤ `MAX_ENDPOINT_LEN`(path 即 socket.io namespace,
/// 过长会让 CONNECT ok 超 max_frame)。
///
/// # 参数
/// - `path`: NASA endpoint path 或 socket.io namespace。
fn validate_endpoint_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("endpoint path must not be empty".to_string());
    }
    if path.len() > MAX_ENDPOINT_LEN {
        return Err(format!(
            "endpoint path too long ({} > {MAX_ENDPOINT_LEN} bytes): {path}",
            path.len()
        ));
    }
    Ok(())
}

/// registry 槽位:endpoint + **代际号**(热替换 + 版本号)。
/// generation 由 registry 级单调计数器分配,`unregister → register` 同 path **绝不复用**
/// ——否则旧会话可能与新 endpoint 代际碰撞。
#[derive(Clone)]
struct EndpointSlot {
    generation: u64,
    endpoint: Arc<Endpoint>,
}

/// endpoint 变更(replace/unregister)后的回调(Server 装配注入,pub(crate)):
/// 入参 = 变更的 path。**hook 自己在执行时读取当前代际**(而非携带快照)——并发多次
/// 变更的 hook 乱序到达时,迟到的 hook 也只会按"最新代际"清旧,不会误关新会话。
pub(crate) type EndpointMutationHook = Arc<dyn Fn(&str) + Send + Sync>;

/// 端点注册表。重复注册默认拒绝,热替换走 `replace`。
/// **代际语义**:每次 register/replace 取新 generation;会话在鉴权时绑定
/// (generation, `Arc<Endpoint>`) 快照;replace/unregister 经 mutation hook 主动关闭旧代际会话。
#[derive(Default)]
pub struct EndpointRegistry {
    endpoints: DashMap<String, EndpointSlot>,
    /// registry 级单调代际计数器(从 1 起;0 保留为"无")。
    gen_seq: std::sync::atomic::AtomicU64,
    /// endpoint 变更回调(Server 装配时注入;未注入时变更只影响新连接)。
    mutation_hook: std::sync::Mutex<Option<EndpointMutationHook>>,
}

impl EndpointRegistry {
    /// 构造新实例；用于集中初始化内部字段和默认状态。
    pub fn new() -> Arc<EndpointRegistry> {
        Arc::new(EndpointRegistry::default())
    }

    /// 生成端点注册版本号；用于通知监听方配置已变化。
    fn next_gen(&self) -> u64 {
        self.gen_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// 框架内部:注入变更回调(Server 装配,关闭旧代际会话)。
    pub(crate) fn set_mutation_hook(&self, hook: EndpointMutationHook) {
        *self.mutation_hook.lock().unwrap() = Some(hook);
    }

    /// 发布端点变更事件；用于唤醒等待路由表更新的任务。
    ///
    /// # 参数
    /// - `path`: 发生注册、替换或注销的 endpoint path。
    fn fire_mutation(&self, path: &str) {
        let hook = self.mutation_hook.lock().unwrap().clone();
        if let Some(h) = hook {
            h(path);
        }
    }

    /// 注册;path 空或已存在则返回 Err(不静默覆盖)。用 DashMap entry **原子**判重+插入,
    /// 避免 contains_key→insert 的竞态。分配**全新 generation**
    /// (unregister 后重 register 不复用旧代际;R25 P1)。
    ///
    /// # 参数
    /// - `ep`: 要注册的 endpoint 快照,其 path 作为路由表 key。
    pub fn register(&self, ep: Endpoint) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;
        let path = ep.path().to_string();
        validate_endpoint_path(&path)?;
        match self.endpoints.entry(path.clone()) {
            Entry::Occupied(_) => Err(format!("endpoint already registered: {path}")),
            Entry::Vacant(v) => {
                v.insert(EndpointSlot {
                    generation: self.next_gen(),
                    endpoint: Arc::new(ep),
                });
                Ok(())
            }
        }
    }

    /// 显式热替换。**与 `register` 同一套 path 校验**;校验失败返回 `Err`
    /// 且**不动**旧 endpoint。成功后分配新 generation 并触发 mutation hook
    /// ——Server 装配的 hook 会向该 path 的**旧代际**会话发 CLOSE 并注销。
    ///
    /// # 参数
    /// - `ep`: 新 endpoint 快照,其 path 决定被替换的注册项。
    pub fn replace(&self, ep: Endpoint) -> Result<Option<Arc<Endpoint>>, String> {
        validate_endpoint_path(ep.path())?;
        let path = ep.path().to_string();
        let old = self
            .endpoints
            .insert(
                path.clone(),
                EndpointSlot {
                    generation: self.next_gen(),
                    endpoint: Arc::new(ep),
                },
            )
            .map(|s| s.endpoint);
        self.fire_mutation(&path);
        Ok(old)
    }

    /// 注销。成功后触发 mutation hook——Server 装配的 hook 会主动关闭该 path 的**所有**
    /// 会话(不再是"只挡新连接"的假下线;R25 P1)。
    ///
    /// # 参数
    /// - `path`: 要注销的 endpoint 路径。
    pub fn unregister(&self, path: &str) -> Option<Arc<Endpoint>> {
        let old = self.endpoints.remove(path).map(|(_, s)| s.endpoint);
        if old.is_some() {
            self.fire_mutation(path);
        }
        old
    }

    /// 读取已注册 endpoint。
    ///
    /// # 参数
    /// - `path`: 要查询的 endpoint 路径。
    pub fn get(&self, path: &str) -> Option<Arc<Endpoint>> {
        self.endpoints.get(path).map(|r| r.endpoint.clone())
    }

    /// 当前槽位快照 (generation, endpoint):鉴权时绑定到 Session 用。
    pub(crate) fn slot(&self, path: &str) -> Option<(u64, Arc<Endpoint>)> {
        self.endpoints
            .get(path)
            .map(|r| (r.generation, r.endpoint.clone()))
    }

    /// 当前代际(注册后复核 / EVENT 分派兜底用);未注册返回 None。
    pub(crate) fn current_generation(&self, path: &str) -> Option<u64> {
        self.endpoints.get(path).map(|r| r.generation)
    }

    /// 判断路径是否已注册；用于启动或热更新时避免重复端点。
    ///
    /// # 参数
    /// - `path`: 要检查的 endpoint 路径。
    pub fn contains(&self, path: &str) -> bool {
        self.endpoints.contains_key(path)
    }
}
