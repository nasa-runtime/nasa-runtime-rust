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
    /// 参数说明：
    /// - `uid`: 业务用户 ID,通过后会绑定到当前 session,不得为空或空白。
    ///
    /// 返回：待接入流程消费的通过结果；此构造器不校验 UID，也不直接改变会话认证状态。
    pub fn ok(uid: impl Into<String>) -> Self {
        AuthResult::Ok {
            uid: uid.into(),
        }
    }

    /// 业务作用：构造鉴权失败结果。
    ///
    /// 参数说明：
    /// - `reason`: 鉴权失败原因,会写入鉴权响应供客户端和日志排查。
    ///
    /// 返回：携带拒绝原因的结果，不建立认证身份；原因不得包含凭据或内部敏感信息。
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
    /// 业务作用：向鉴权回调提供连接身份与协议的只读快照，避免暴露会话写入能力。
    ///
    /// 参数说明：
    /// - `s`：已确定传输和入口的待认证会话。
    ///
    /// 返回：复制的会话标识和入口元数据；回调不能通过该值设置 UID 或发送控制帧。
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
    /// 允许按路由转发，并继续调用本地事件处理器。
    RelayAndHandle,
}

/// relay 授权的**只读连接上下文**(不暴露 `Arc<Session>`,业务无法借策略闭包
/// 拿到完整 Session 绕过收口)。私有持 Session 引用,只暴露只读访问器。
pub struct PolicyContext<'a> {
    /// 仅通过受限访问器读取会话身份与订阅归属，不向策略调用方移交完整会话。
    sess: &'a Arc<Session>,
}

impl<'a> PolicyContext<'a> {
    /// 业务作用：为本次入站路由裁决绑定只读会话视图。
    ///
    /// 参数说明：
    /// - `sess`：触发入站消息的会话；视图借用不得超出该会话引用的生命期。
    ///
    /// 返回：可查询发送者身份和订阅关系的上下文，不授予会话写入能力。
    pub(crate) fn new(sess: &'a Arc<Session>) -> PolicyContext<'a> {
        PolicyContext {
            sess,
        }
    }

    /// 业务作用：向路由策略提供当前会话已经绑定的业务身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已认证会话的 UID 借用；未绑定身份时为 None，不能按匿名身份扩大权限。
    pub fn uid(&self) -> Option<&str> {
        self.sess.uid()
    }

    /// 业务作用：向路由策略提供当前连接所属入口，供入口间权限隔离。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已确定的入口借用；尚未确定时为 None，不查询或修改路由。
    pub fn endpoint(&self) -> Option<&str> {
        self.sess.endpoint()
    }

    /// 业务作用：让路由策略按实际底层传输执行接入限制。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前会话固定的传输类型，不代表该传输已获得业务路由授权。
    pub fn transport(&self) -> Transport {
        self.sess.transport()
    }

    /// 业务作用：查询发送者的本地订阅归属，供路由策略作为裁决条件。
    ///
    /// 参数说明：
    /// - `g`: 要检查的订阅名称。
    ///
    /// 返回：本地会话订阅集合包含该名称时为 true；不替代远程业务权限校验。
    pub fn is_in_group(&self, g: &str) -> bool {
        self.sess.groups().iter().any(|x| x == g)
    }
}

/// 客户端入站消息的路由授权。**默认 deny**。
/// 只作用于客户端触发的自动 relay;服务端直接 `Sender::send` 不经过它。
pub type InboundPolicy = Arc<dyn Fn(&PolicyContext, &Message) -> RouteDecision + Send + Sync>;

/// 业务作用：默认全拒绝:客户端带路由字段默认不自动 relay,业务须显式放行。
///
/// 参数说明: 无。
///
/// 返回：始终给出 Deny 的策略；带路由消息既不转发，也不交给本地 handler。
pub fn deny_all() -> InboundPolicy {
    Arc::new(|_ctx, _m| RouteDecision::Deny)
}

/// 业务作用：全放行;仅在确需开放客户端路由时显式用。
///
/// 参数说明: 无。
///
/// 返回：始终给出 RelayAndHandle 的策略；调用方须在启用前承担目标授权责任。
pub fn allow_all() -> InboundPolicy {
    Arc::new(|_ctx, _m| RouteDecision::RelayAndHandle)
}
