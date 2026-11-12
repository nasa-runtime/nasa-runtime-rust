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

/// 客户端认证扩展，分别负责周期登录和逐请求身份材料生成。
#[async_trait::async_trait]
pub trait AuthPlugin: Send + Sync {
    /// 业务作用：建立或刷新请求认证材料，供后续调用取得身份；调度器会重复调用，插件须自行限制刷新频率。
    ///
    /// 参数说明：
    /// - `server_list`：当前发现的服务端地址快照，后续刷新可得到不同节点列表。
    /// - `auth_context`：共享登录参数；具体密钥字段及校验方式由插件约定。
    ///
    /// 返回：本轮刷新结束，不返回成功状态；实现需自行保留或撤销身份，不能把方法完成视为远端认证已成功。
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>);

    /// 业务作用：为当前请求资源取得身份或签名材料，由传输层附加到请求。
    ///
    /// 参数说明：
    /// - `resource`：当前请求的协议类别、命名空间、分组及资源，供插件限定签名作用域。
    ///
    /// 返回：该请求使用的身份上下文；空上下文不表示已授权，最终接受与否由服务端判断。
    fn get_login_identity(&self, resource: RequestResource) -> LoginIdentityContext;
}

/// 认证插件共享的登录参数上下文。
#[derive(Clone, Default)]
pub struct AuthContext {
    /// 插件识别的原始登录参数，可含密钥，不能作为诊断文本输出。
    pub(crate) params: HashMap<String, String>,
}

impl AuthContext {
    /// 业务作用：构造登录参数，将单个身份或凭据字段纳入插件初始化上下文。
    ///
    /// 参数说明：
    /// - `key`：插件识别的参数名称，已有同名参数会被覆盖。
    /// - `val`：参数原值，可能包含秘密；此入口只存储，不校验或脱敏。
    ///
    /// 返回：包含该参数的上下文；不触发登录或网络请求。
    pub fn add_param(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.params.insert(key.into(), val.into());
        self
    }

    /// 业务作用：合并一组插件登录参数，允许调用方统一传入已准备的身份材料。
    ///
    /// 参数说明：
    /// - `map`：要合并的参数集合，同名键以此集合中的值为准。
    ///
    /// 返回：合并后的上下文；不检查凭据有效性，也不执行登录。
    pub fn add_params(mut self, map: HashMap<String, String>) -> Self {
        self.params.extend(map);
        self
    }
}

/// 请求认证字段集合；是否有效及是否获权仍由接收端裁决。
#[derive(Clone, Default)]
pub struct LoginIdentityContext {
    /// 传输层附加到请求的令牌或签名字段。
    pub(crate) contexts: HashMap<String, String>,
}

impl LoginIdentityContext {
    /// 业务作用：将一个请求认证字段加入身份上下文，供传输层发送令牌或签名。
    ///
    /// 参数说明：
    /// - `key`：线上协议使用的认证字段名称。
    /// - `val`：该字段的原值，同名字段被覆盖；调用方负责避免将秘密写入日志。
    ///
    /// 返回：更新后的身份上下文；字段存在不等于服务端已认可其权限。
    pub fn add_context(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.contexts.insert(key.into(), val.into());
        self
    }

    /// 业务作用：合并一次请求的整组身份或签名字段。
    ///
    /// 参数说明：
    /// - `map`：已按协议构造的认证字段集合，覆盖上下文中的同名键。
    ///
    /// 返回：包含合并结果的上下文；不发送请求或验证签名。
    pub fn add_contexts(mut self, map: HashMap<String, String>) -> Self {
        self.contexts.extend(map);
        self
    }
}

/// 不生成凭据的默认认证插件，供无需客户端认证材料的部署使用。
#[derive(Default)]
pub(crate) struct NoopAuthPlugin {
    /// 默认空身份；读取时复制，不依据请求资源扩大权限。
    login_identity: LoginIdentityContext,
}

#[async_trait::async_trait]
impl AuthPlugin for NoopAuthPlugin {
    /// 业务作用：在未配置认证插件时保持空身份，不发起远端登录。
    ///
    /// 参数说明：
    /// - `server_list`：调度器提供的服务端快照，此实现不读取。
    /// - `auth_context`：调度器提供的登录参数，此实现不读取或保存。
    ///
    /// 返回：立即完成，不修改身份；服务端仍可拒绝后续未认证请求。
    #[allow(unused_variables)]
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {}

    /// 业务作用：为未启用认证的请求提供空身份上下文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认身份的副本；忽略请求资源，不生成令牌或签名。
    fn get_login_identity(&self, _: RequestResource) -> LoginIdentityContext {
        self.login_identity.clone()
    }
}

/// 业务作用：先执行首轮认证，再启动持续刷新任务，使连接后续请求可读取插件更新的身份。
///
/// 参数说明：
/// - `auth_plugin`：承担登录及身份生成职责的共享插件。
/// - `server_list_provider`：每轮刷新前提供当前服务端列表，避免永久使用启动时的地址。
/// - `auth_params`：冻结的登录参数集合，所有刷新轮次共享同一份上下文。
/// - `id`：刷新任务的客户端追踪标识，不参与认证授权。
///
/// 返回：首轮调用结束且后台任务已提交；不确认登录成功，也不返回任务关闭句柄。每轮刷新结束后等待 30 秒。
pub(crate) async fn init_auth_plugin(
    auth_plugin: Arc<dyn AuthPlugin>,
    server_list_provider: Arc<dyn ServerListProvider>,
    auth_params: HashMap<String, String>,
    id: String,
) {
    info!("init auth task");
    let auth_context = Arc::new(AuthContext::default().add_params(auth_params));
    let server_list = server_list_provider.current_server_list().await;
    // 先让插件完成首次身份准备；接口没有成功收据，不能据此宣布服务端已接受凭据。
    auth_plugin
        .login(server_list.clone(), auth_context.clone())
        .in_current_span()
        .await;
    info!("init auth finish");

    executor::spawn(
        async move {
            // 每轮重新读取服务端地址；插件自行判定是否到达令牌刷新时刻。
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
