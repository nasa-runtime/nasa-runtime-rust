use arc_swap::ArcSwap;
use rand::RngExt;
use std::ops::{Add, Deref};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use crate::api::plugin::{AuthContext, AuthPlugin, LoginIdentityContext};

use super::RequestResource;

/// HTTP 登录参数中的用户名键。
pub const USERNAME: &str = "username";

/// HTTP 登录参数中的密码键。
pub const PASSWORD: &str = "password";

/// 登录响应与后续请求携带访问令牌的协议键。
pub(crate) const ACCESS_TOKEN: &str = "accessToken";

/// 登录响应中令牌有效时长的协议键，单位为秒。
#[allow(dead_code)]
pub(crate) const TOKEN_TTL: &str = "tokenTtl";

/// 使用用户名及密码换取访问令牌的 HTTP 认证插件。
pub struct HttpLoginAuthPlugin {
    /// 最近一次登录响应提供的令牌，刷新失败不会自动清空。
    login_identity: ArcSwap<LoginIdentityContext>,
    /// 允许下一次登录的单调时钟时刻，不等于令牌到期时刻。
    next_login_refresh: ArcSwap<Instant>,
}

impl Default for HttpLoginAuthPlugin {
    /// 业务作用：创建尚未登录的 HTTP 认证插件，允许后续调度进行首次刷新。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空身份及当前刷新时刻；构造不创建网络连接，不保证已有可用令牌。
    fn default() -> Self {
        Self {
            login_identity: ArcSwap::from_pointee(LoginIdentityContext::default()),
            next_login_refresh: ArcSwap::from_pointee(Instant::now()),
        }
    }
}

#[async_trait::async_trait]
impl AuthPlugin for HttpLoginAuthPlugin {
    /// 业务作用：达到刷新时刻后向一个 Nacos 节点登录，更新后续请求使用的令牌。
    ///
    /// 参数说明：
    /// - `server_list`：供随机选择的非空服务端地址列表，不在同一轮遍历重试所有节点。
    /// - `auth_context`：必须包含 username 与 password 的登录上下文。
    ///
    /// 返回：未到刷新时刻直接完成；取得可解析响应后先替换身份，再按 tokenTtl 的十分之一设置下次刷新时刻。
    /// 请求或 JSON 解析失败保留原身份；缺少凭据、地址为空或读取响应正文失败会产生 panic。
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {
        let now_instant = Instant::now();
        if now_instant.le(self.next_login_refresh.load().deref()) {
            tracing::debug!("Http login return because now_instant lte next_login_refresh.");
            return;
        }

        let username = auth_context
            .params
            .get(USERNAME)
            .expect("Username parameter should exist for HTTP auth")
            .to_owned();
        let password = auth_context
            .params
            .get(PASSWORD)
            .expect("Password parameter should exist for HTTP auth")
            .to_owned();

        let server_addr = {
            // 单轮只选择一个节点，失败后的再次尝试交给外层刷新调度，避免一轮放大登录请求。
            server_list
                .get(rand::rng().random_range(0..server_list.len()))
                .expect("Server list should not be empty")
                .to_string()
        };

        let scheme = if cfg!(feature = "tls") {
            "https"
        } else {
            "http"
        };
        let login_url = format!("{scheme}://{server_addr}/nacos/v1/auth/login");

        // 登录凭据可能出现在请求 URL 或服务端回显中；诊断只记录固定动作与数值状态。
        tracing::debug!("Nacos HTTP 登录请求开始");

        let login_response = {
            let resp = crate::common::remote::http_client()
                .post(login_url)
                .query(&[(USERNAME, username), (PASSWORD, password)])
                .send()
                .await;
            match resp {
                Err(error) => {
                    tracing::error!(
                        timeout = error.is_timeout(),
                        connect = error.is_connect(),
                        "Nacos HTTP 登录请求失败"
                    );
                    None
                }
                Ok(resp) => {
                    tracing::debug!(status = resp.status().as_u16(), "Nacos HTTP 登录响应已到达");
                    let resp_text = resp
                        .text()
                        .await
                        .expect("Response text conversion should succeed");
                    let resp_obj = serde_json::from_str::<HttpLoginResponse>(&resp_text);
                    match resp_obj {
                        Err(error) => {
                            tracing::error!(
                                line = error.line(),
                                column = error.column(),
                                "Nacos HTTP 登录响应格式无效"
                            );
                            None
                        }
                        Ok(resp_obj) => Some(resp_obj),
                    }
                }
            }
        };

        if let Some(login_response) = login_response {
            let delay_sec = login_response.token_ttl / 10;
            let new_login_identity = Arc::new(
                LoginIdentityContext::default()
                    .add_context(ACCESS_TOKEN, login_response.access_token),
            );
            self.login_identity.store(new_login_identity);

            self.next_login_refresh
                .store(Arc::new(Instant::now().add(Duration::from_secs(delay_sec))));
        }
    }

    /// 业务作用：取得最近一次成功解析登录响应后保存的令牌上下文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份副本；忽略请求资源且不复验令牌到期时间，初次登录前为空，远端仍可拒绝令牌。
    fn get_login_identity(&self, _: RequestResource) -> LoginIdentityContext {
        self.login_identity.load().deref().deref().to_owned()
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpLoginResponse {
    /// 接收端签发的访问令牌，属于秘密材料。
    access_token: String,
    /// 服务端声明的令牌有效秒数，用于计算提前刷新间隔。
    token_ttl: u64,
}
