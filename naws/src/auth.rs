// ============================================================================
// src/auth.rs —— 鉴权 SPI + 路由授权策略(都用闭包注入,不用 trait object 反射)。
// 架构说明(InboundPolicy 默认 deny)、(鉴权)。
// ============================================================================

use std::pin::Pin;
use std::sync::Arc;

use naws_proto::Message;

use crate::session::{Protocol, Session, Transport};

/// 鉴权结果。
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// 通过,产出业务 uid(不可空/空白)。
    Ok {
        /// 鉴权通过后绑定到 session 的业务用户 ID。
        uid: String,
    },
    /// 失败,带原因。
    Fail {
        /// 失败原因,会返回给客户端并写入服务端日志。
        reason: String,
    },
}

impl AuthResult {
    /// 业务作用：构造鉴权通过结果。
    ///
    /// # 参数
    /// - `uid`: 业务用户 ID,通过后会绑定到当前 session,不得为空或空白。
    pub fn ok(uid: impl Into<String>) -> Self {
        AuthResult::Ok { uid: uid.into() }
    }

    /// 业务作用：构造鉴权失败结果。
    ///
    /// # 参数
    /// - `reason`: 鉴权失败原因,会写入鉴权响应供客户端和日志排查。
    pub fn fail(reason: impl Into<String>) -> Self {
        AuthResult::Fail {
            reason: reason.into(),
        }
    }
}

/// 鉴权回调返回的异步结果类型。
pub type AuthFut = Pin<Box<dyn std::future::Future<Output = AuthResult> + Send>>;

/// 鉴权上下文:传给 authorize 的**只读**连接快照。不再给整个 `Arc<Session>`
/// ——否则鉴权回调可 `set_uid`/`send_control` 伪造控制帧。业务只读这些元数据 + AuthRequest。
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// 框架分配的会话 ID。
    pub session_id: String,
    /// 当前连接的底层传输类型。
    pub transport: Transport,
    /// 当前连接采用的上层协议。
    pub protocol: Protocol,
    /// 连接的 endpoint(WS 来自 URL path;TCP 来自 AUTH 帧),鉴权时已确定。
    pub endpoint: Option<String>,
}

impl AuthContext {
    /// 业务作用：从 session 构造结果；用于统一输入适配。
    pub(crate) fn from_session(s: &Session) -> AuthContext {
        AuthContext {
            session_id: s.id().to_string(),
            transport: s.transport(),
            protocol: s.protocol(),
            endpoint: s.endpoint().map(String::from),
        }
    }
}

/// 业务鉴权:收到 AUTH 帧时调(在专用执行/超时保护下,不阻塞读循环)。
/// 入参 = 解出的 AuthRequest(proto)+ **只读** AuthContext(不暴露可变 Session)。
pub type AuthorizeProvider =
    Arc<dyn Fn(naws_proto::AuthRequest, AuthContext) -> AuthFut + Send + Sync>;

/// relay 授权决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteDecision {
    /// 拒绝:丢弃整条带路由消息,handler 也不调。
    Deny,
    /// 只调本地 handler,不 relay。
    HandleOnly,
    /// relay + handler。
    RelayAndHandle,
}

/// relay 授权的**只读连接上下文**(不暴露 `Arc<Session>`,业务无法借策略闭包
/// 拿到完整 Session 绕过收口)。私有持 Session 引用,只暴露只读访问器。
pub struct PolicyContext<'a> {
    sess: &'a Arc<Session>,
}

impl<'a> PolicyContext<'a> {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub(crate) fn new(sess: &'a Arc<Session>) -> PolicyContext<'a> {
        PolicyContext { sess }
    }

    /// 业务作用：读取 uid 状态；用于向调用方暴露当前运行信息。
    pub fn uid(&self) -> Option<&str> {
        self.sess.uid()
    }

    /// 业务作用：读取 endpoint 状态；用于向调用方暴露当前运行信息。
    pub fn endpoint(&self) -> Option<&str> {
        self.sess.endpoint()
    }

    /// 业务作用：读取 transport 状态；用于向调用方暴露当前运行信息。
    pub fn transport(&self) -> Transport {
        self.sess.transport()
    }

    /// 业务作用：发送者当前是否在群 `g`(供按成员/群授权)。
    ///
    /// # 参数
    /// - `g`: 要检查的群组名。
    pub fn is_in_group(&self, g: &str) -> bool {
        self.sess.groups().iter().any(|x| x == g)
    }
}

/// 客户端入站消息的路由授权。**默认 deny**(对齐修复后 原实现 RoutePolicy.denyAll)。
/// 只作用于客户端触发的自动 relay;服务端直接 `Sender::send` 不经过它。
pub type InboundPolicy = Arc<dyn Fn(&PolicyContext, &Message) -> RouteDecision + Send + Sync>;

/// 业务作用：默认全拒绝:客户端带路由字段默认不自动 relay,业务须显式放行。
pub fn deny_all() -> InboundPolicy {
    Arc::new(|_ctx, _m| RouteDecision::Deny)
}

/// 业务作用：全放行(对齐 原实现 RoutePolicy.allowAll);仅在确需开放客户端路由时显式用。
pub fn allow_all() -> InboundPolicy {
    Arc::new(|_ctx, _m| RouteDecision::RelayAndHandle)
}
