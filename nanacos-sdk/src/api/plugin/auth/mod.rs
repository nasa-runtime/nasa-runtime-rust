#[cfg(feature = "auth-by-http")]
mod auth_by_http;
#[cfg(feature = "auth-by-http")]
pub use auth_by_http::*;

#[cfg(feature = "auth-by-aliyun")]
mod auth_by_aliyun_ram;
#[cfg(feature = "auth-by-aliyun")]
pub use auth_by_aliyun_ram::*;

use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{Instrument, debug, debug_span, info};

use crate::common::executor;
use crate::common::remote::server_list::ServerListProvider;

/// Auth plugin in Client.
/// This api may change in the future, please forgive me if you customize the implementation.
#[async_trait::async_trait]
pub trait AuthPlugin: Send + Sync {
    /// Login with [`AuthContext`], Note that this method will be scheduled continuously.
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>);

    /// Get the [`LoginIdentityContext`].
    fn get_login_identity(&self, resource: RequestResource) -> LoginIdentityContext;
}

/// 认证插件共享的登录参数上下文。
#[derive(Clone, Default)]
pub struct AuthContext {
    pub(crate) params: HashMap<String, String>,
}

impl AuthContext {
    /// Add the param.
    pub fn add_param(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.params.insert(key.into(), val.into());
        self
    }

    /// Add the params.
    pub fn add_params(mut self, map: HashMap<String, String>) -> Self {
        self.params.extend(map);
        self
    }
}

/// 认证成功后提供给请求签名的身份上下文。
#[derive(Clone, Default)]
pub struct LoginIdentityContext {
    pub(crate) contexts: HashMap<String, String>,
}

impl LoginIdentityContext {
    /// Add the context.
    pub fn add_context(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.contexts.insert(key.into(), val.into());
        self
    }

    /// Add the contexts.
    pub fn add_contexts(mut self, map: HashMap<String, String>) -> Self {
        self.contexts.extend(map);
        self
    }
}

/// Noop AuthPlugin.
#[derive(Default)]
pub(crate) struct NoopAuthPlugin {
    login_identity: LoginIdentityContext,
}

#[async_trait::async_trait]
impl AuthPlugin for NoopAuthPlugin {
    #[allow(unused_variables)]
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {
        // noop
    }

    fn get_login_identity(&self, _: RequestResource) -> LoginIdentityContext {
        // noop
        self.login_identity.clone()
    }
}

pub(crate) async fn init_auth_plugin(
    auth_plugin: Arc<dyn AuthPlugin>,
    server_list_provider: Arc<dyn ServerListProvider>,
    auth_params: HashMap<String, String>,
    id: String,
) {
    info!("init auth task");
    let auth_context = Arc::new(AuthContext::default().add_params(auth_params));
    let server_list = server_list_provider.current_server_list().await;
    // First login
    auth_plugin
        .login(server_list.clone(), auth_context.clone())
        .in_current_span()
        .await;
    info!("init auth finish");

    executor::spawn(
        async move {
            // Periodic refresh
            info!("auth plugin task start.");
            loop {
                let server_list = server_list_provider.current_server_list().await;
                auth_plugin
                    .login(server_list.clone(), auth_context.clone())
                    .in_current_span()
                    .await;
                debug!("auth_plugin schedule at fixed delay");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
        .instrument(debug_span!("auth_task", id = id)),
    );
}

/// 认证插件用于判定请求作用域和签名资源的描述。
#[derive(Debug, Default)]
pub struct RequestResource {
    /// 被认证请求的业务类型。
    pub request_type: String,
    /// 目标所在的命名空间。
    pub namespace: Option<String>,
    /// 目标服务或配置所属的分组。
    pub group: Option<String>,
    /// 请求作用域中的具体资源标识。
    pub resource: Option<String>,
}
