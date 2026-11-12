// ============================================================================
// src/endpoint.rs —— 端点 + 事件 handler(闭包 builder)。架构说明。
// handler 分为 Inline（同步且不可 await/阻塞）与 Async（提交 tokio 并受服务端并发门禁约束）。
// 事件与生命周期回调在构造阶段集中登记，运行期按不可变定义查询。
// ============================================================================

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

use crate::session::SessionHandle;

/// endpoint async handler 统一装箱后的 future 类型。
pub type BoxFut = Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// 事件处理器，支持同步返回与异步 future 两种业务执行方式。
#[derive(Clone)]
pub enum EventHandler {
    /// 读循环内同步执行:只允许短 CPU 操作,**不可 await/阻塞**。
    Inline(Arc<dyn Fn(SessionHandle, Bytes) + Send + Sync>),
    /// 提交 Tokio 后台执行：可 await，不阻塞读循环，并受服务端统一并发门禁约束。
    Async(Arc<dyn Fn(SessionHandle, Bytes) -> BoxFut + Send + Sync>),
}

/// 生命周期 hook(同步,通常很短;join_group 等都是同步的)。
pub type LifecycleHandler = Arc<dyn Fn(SessionHandle) + Send + Sync>;

/// 端点:path + 事件表 + onConnect/onDisconnect。不可变快照。
pub struct Endpoint {
    /// 注册表用于定位此定义的入口路径。
    path: String,
    /// 事件名到同步或异步处理器的固定映射。
    events: HashMap<String, EventHandler>,
    /// 会话初始化时执行的可选同步回调。
    on_connect: Option<LifecycleHandler>,
    /// 与本定义建连回调配对的可选清理回调。
    on_disconnect: Option<LifecycleHandler>,
}

impl Endpoint {
    /// 业务作用：创建 endpoint builder。
    ///
    /// 参数说明：
    /// - `path`: 连接入口路径,用于鉴权后绑定会话和分发入站事件。
    ///
    /// 返回：事件表为空、没有生命周期回调的端点构造器；路径合法性在注册或替换时检查。
    pub fn builder(path: impl Into<String>) -> EndpointBuilder {
        EndpointBuilder {
            path: path.into(),
            events: HashMap::new(),
            on_connect: None,
            on_disconnect: None,
        }
    }

    /// 业务作用：返回端点路径；用于按连接入口查找事件处理器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端点构造时确定的路径；不查询注册表，因此不表示此定义当前在线。
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 业务作用：查找端点事件处理器；用于把入站事件分派到业务回调。
    ///
    /// 参数说明：
    /// - `name`: 业务事件名。
    ///
    /// 返回：当前不可变定义中精确匹配的处理器引用，未注册为 `None`；不执行回调或校验调用权限。
    pub fn event(&self, name: &str) -> Option<&EventHandler> {
        self.events.get(name)
    }

    /// 业务作用：返回连接建立回调；用于新会话创建后执行业务初始化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：此端点定义绑定的建连回调，未配置时为 `None`；查询不会执行初始化或改变会话阶段。
    pub fn on_connect(&self) -> Option<&LifecycleHandler> {
        self.on_connect.as_ref()
    }

    /// 业务作用：返回连接断开回调；用于会话释放时执行业务清理。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：此端点快照的断连回调，未配置为 `None`；由生命周期流程负责与建连回调配对及执行一次。
    pub fn on_disconnect(&self) -> Option<&LifecycleHandler> {
        self.on_disconnect.as_ref()
    }
}

/// 保存端点构造参数；用于注册路径、事件和生命周期回调。
pub struct EndpointBuilder {
    /// 等待注册时校验的端点路径。
    path: String,
    /// 尚在构造阶段登记的事件表，空名与重复名禁止插入。
    events: HashMap<String, EventHandler>,
    /// 构造阶段选择的建连回调，重复设置时覆盖。
    on_connect: Option<LifecycleHandler>,
    /// 构造阶段选择的断连回调，冻结后与定义一起绑定会话。
    on_disconnect: Option<LifecycleHandler>,
}

impl EndpointBuilder {
    /// 业务作用：为端点设置同步建连回调，供认证流程完成后的业务初始化使用。
    ///
    /// 参数说明：
    /// - `f`: 会话建立后执行的业务回调,入参是新建的会话句柄。
    ///
    /// 返回：保存回调后的构造器，重复设置覆盖旧回调；不立即执行，回调运行时不应阻塞协议处理。
    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(SessionHandle) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// 业务作用：为端点设置同步断连回调，供会话清理流程释放业务侧资源。
    ///
    /// 参数说明：
    /// - `f`: 会话断开后执行的业务回调,入参是断开的会话句柄。
    ///
    /// 返回：保存回调后的构造器，重复设置覆盖旧回调；实际执行由会话绑定的端点快照和生命周期流程决定。
    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(SessionHandle) + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    /// 业务作用：同步事件。事件名重复 / 空 → panic(配置期编程错误,快速失败)。
    ///
    /// 参数说明：
    /// - `event`: endpoint 内注册的业务事件名,用于匹配入站 `Message.events`。
    /// - `f`: 收到该事件时同步执行的处理器,入参为会话句柄和 payload。
    ///
    /// 返回：保存同步处理器的构造器；事件名为空或重复时 panic，不覆盖已有处理器。
    /// 回调在读循环中执行，应仅完成短时操作；此注册方法不触发回调。
    pub fn on_event_inline<F>(mut self, event: impl Into<String>, f: F) -> Self
    where
        F: Fn(SessionHandle, Bytes) + Send + Sync + 'static,
    {
        self.put_event(event.into(), EventHandler::Inline(Arc::new(f)));
        self
    }

    /// 业务作用：异步事件。`f` 返回一个 future。事件名重复 / 空 → panic。
    ///
    /// 参数说明：
    /// - `event`: endpoint 内注册的业务事件名,用于匹配入站 `Message.events`。
    /// - `f`: 收到该事件时异步执行的处理器,入参为会话句柄和 payload。
    ///
    /// 返回：保存已装箱 future 工厂的构造器；空名或重名 panic，不启动任务。
    /// 实际调度、并发配额及取消由服务端事件流程负责，本方法不提供同一用户的执行顺序保证。
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

    /// 业务作用：插入事件 handler:空名或重名 → panic(不静默覆盖)。
    ///
    /// 参数说明：
    /// - `event`: endpoint 内唯一的业务事件名。
    /// - `handler`: 业务处理函数,在匹配事件或任务时被调用。
    ///
    /// 返回：名称非空且未注册时插入处理器；否则在插入前 panic，保持原映射。
    /// 不裁剪名称或校验业务权限，空白名称不会因本检查被拒绝。
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

    /// 业务作用：冻结路径、事件表和生命周期回调，生成供注册表持有的端点定义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不可变端点快照；不校验路径、不注册路由、不建立监听器或执行业务回调。
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

/// 业务作用：endpoint path 不变量由 `register` 与 `replace` **共用**，任何写入口都不得绕过：
/// 非空且不超过 `MAX_ENDPOINT_LEN`（path 即 socket.io namespace，
/// 过长会让 CONNECT ok 超 max_frame)。
///
/// 参数说明：
/// - `path`: NASA endpoint path 或 socket.io namespace。
///
/// 返回：路径非空且 UTF-8 字节数不超过上限时成功，否则返回原因文本。
/// 不规范化路径，不校验斜线、空白或其它命名空间语法，调用方仍须保证协议中的路径有效。
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
    /// 此次发布定义时分配的代际，与会话绑定值比较以识别旧定义。
    generation: u64,
    /// 当前槽位持有的不可变端点定义。
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
    /// 路径对应的当前端点定义及代际，作为接入和事件分派的权威视图。
    endpoints: DashMap<String, EndpointSlot>,
    /// registry 级单调代际计数器(从 1 起;0 保留为"无")。
    gen_seq: std::sync::atomic::AtomicU64,
    /// endpoint 变更回调(Server 装配时注入;未注入时变更只影响新连接)。
    mutation_hook: std::sync::Mutex<Option<EndpointMutationHook>>,
}

impl EndpointRegistry {
    /// 业务作用：创建用于动态端点注册与替换的本地共享注册表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无端点、代际计数从零开始且尚未安装变更回调的注册表；不会自动监听或接入连接。
    pub fn new() -> Arc<EndpointRegistry> {
        Arc::new(EndpointRegistry::default())
    }

    /// 业务作用：为注册或替换分配本注册表内的代际，区分同一路径的不同端点定义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：计数递增后的代际，从一开始；只分配序号，不发布端点或触发变更回调。
    /// 代际不跨进程持久化，也不保证不同线程取号后按取号顺序写入注册表。
    fn next_gen(&self) -> u64 {
        self.gen_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// 业务作用：框架内部:注入变更回调(Server 装配,关闭旧代际会话)。
    ///
    /// 参数说明：
    /// - `hook`: 端点替换或注销后读取当前代际并清理旧会话的回调。
    ///
    /// 返回：保存后续变更所使用的回调，替换原回调；不重放过去的变更，不执行现有会话清理。
    pub(crate) fn set_mutation_hook(&self, hook: EndpointMutationHook) {
        *self.mutation_hook.lock().unwrap() = Some(hook);
    }

    /// 业务作用：端点表更新后同步通知会话清理回调，使旧定义绑定的连接及时退出。
    ///
    /// 参数说明：
    /// - `path`: 已经完成替换或注销的端点路径。
    ///
    /// 返回：已安装回调时在释放回调锁后调用一次，否则不做操作；不捕获回调 panic，也不回滚已完成的表变更。
    fn fire_mutation(&self, path: &str) {
        let hook = self.mutation_hook.lock().unwrap().clone();
        if let Some(h) = hook {
            h(path);
        }
    }

    /// 业务作用：注册;path 空或已存在则返回 Err(不静默覆盖)。用 DashMap entry **原子**判重+插入,
    /// 避免 contains_key→insert 的竞态。分配**全新 generation**
    /// (unregister 后重 register 不复用旧代际)。
    ///
    /// 参数说明：
    /// - `ep`: 要注册的 endpoint 快照,其 path 作为路由表 key。
    ///
    /// 返回：路径合法且此前未注册时写入新快照并成功；路径非法或重名返回错误，不覆盖原项。
    /// 新注册不触发变更回调，端点表出现条目不表示某条连接已完成认证。
    pub fn register(&self, ep: Endpoint) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;
        let path = ep.path().to_string();
        validate_endpoint_path(&path)?;
        match self.endpoints.entry(path.clone()) {
            Entry::Occupied(_) => Err(format!("endpoint already registered: {path}")),
            Entry::Vacant(v) => {
                // 持有该路径的写入权后才分配并发布新定义，重复注册不能绕过原子判重。
                v.insert(EndpointSlot {
                    generation: self.next_gen(),
                    endpoint: Arc::new(ep),
                });
                Ok(())
            }
        }
    }

    /// 业务作用：显式热替换。**与 `register` 同一套 path 校验**;校验失败返回 `Err`
    /// 且**不动**旧 endpoint。成功后分配新 generation 并触发 mutation hook
    /// ——Server 装配的 hook 会向该 path 的**旧代际**会话发 CLOSE 并注销。
    ///
    /// 参数说明：
    /// - `ep`: 新 endpoint 快照,其 path 决定被替换的注册项。
    ///
    /// 返回：替换后返回旧定义；原路径不存在时插入并返回 `Ok(None)`；路径无效时返回错误且不改表。
    /// 先发布新槽位再调用变更回调；未安装回调时不主动清理已有会话，回调 panic 也不撤回新槽位。
    pub fn replace(&self, ep: Endpoint) -> Result<Option<Arc<Endpoint>>, String> {
        // 拒绝无效路径必须发生在任何表变更之前，保留旧入口的可用定义。
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
        // 新定义已经可见，清理回调须读取当前代际，避免并发替换时误关更新后的会话。
        self.fire_mutation(&path);
        Ok(old)
    }

    /// 业务作用：移除端点入口并通知变更回调，供服务端关闭已经失去有效定义的会话。
    ///
    /// 参数说明：
    /// - `path`: 要注销的 endpoint 路径。
    ///
    /// 返回：存在时返回被移除的定义并触发回调，不存在时为 `None` 且不通知。
    /// 回调按当前代际清理旧会话，并发重新注册的新定义应保留；未装配回调时本方法只移除入口。
    pub fn unregister(&self, path: &str) -> Option<Arc<Endpoint>> {
        // 先关闭入口再通知连接清理，后续鉴权不能继续绑定刚移除的定义。
        let old = self.endpoints.remove(path).map(|(_, s)| s.endpoint);
        if old.is_some() {
            self.fire_mutation(path);
        }
        old
    }

    /// 业务作用：读取已注册 endpoint。
    ///
    /// 参数说明：
    /// - `path`: 要查询的 endpoint 路径。
    ///
    /// 返回：查询时该路径的共享定义，不存在为 `None`；快照可在后续替换或注销后继续存活，不代表仍可接入。
    pub fn get(&self, path: &str) -> Option<Arc<Endpoint>> {
        self.endpoints.get(path).map(|r| r.endpoint.clone())
    }

    /// 业务作用：当前槽位快照 (generation, endpoint):鉴权时绑定到 Session 用。
    ///
    /// 参数说明：
    /// - `path`: 连接申请绑定的端点路径。
    ///
    /// 返回：同一次表项读取获得的代际与定义，不存在为 `None`；释放读取保护后仍可能发生替换，调用方须复验代际。
    pub(crate) fn slot(&self, path: &str) -> Option<(u64, Arc<Endpoint>)> {
        self.endpoints
            .get(path)
            .map(|r| (r.generation, r.endpoint.clone()))
    }

    /// 业务作用：当前代际(注册后复核 / EVENT 分派兜底用);未注册返回 None。
    ///
    /// 参数说明：
    /// - `path`: 需要与会话绑定代际比较的端点路径。
    ///
    /// 返回：当前槽位的代际，不存在为 `None`；只是一次读取，不为后续操作保留端点或阻止并发下线。
    pub(crate) fn current_generation(&self, path: &str) -> Option<u64> {
        self.endpoints.get(path).map(|r| r.generation)
    }

    /// 业务作用：判断路径是否已注册；用于启动或热更新时避免重复端点。
    ///
    /// 参数说明：
    /// - `path`: 要检查的 endpoint 路径。
    ///
    /// 返回：读取时存在对应槽位则为 `true`；结果不构成后续写入的互斥保证，注册仍须执行原子判重。
    pub fn contains(&self, path: &str) -> bool {
        self.endpoints.contains_key(path)
    }
}
