//! Nacos 配置管理的内部实现。
//!
//! 缓存、服务端变化通知与周期摘要比较共同驱动配置刷新；过滤器负责请求和响应转换。
//!
//! - `cache`：配置正文、元数据及本地监听状态。
//! - `handler`：服务端变化通知的接收和回执。
//! - `message`：配置协议的请求与响应结构。
//! - `util`：配置身份键的构造和解析。
//! - `worker`：远端读写、摘要比较及异步变化消费。
//!
//! 业务使用入口为 [`crate::api::config::ConfigService`]；远端回执不表示本地缓存或业务回调已完成。

mod cache;
mod handler;
mod message;
mod util;
mod worker;

use std::sync::atomic::AtomicU64;

use tracing::instrument;

use crate::api::plugin::{AuthPlugin, ConfigFilter};
use crate::api::props::ClientProps;
use crate::config::worker::ConfigWorker;

pub(crate) struct NacosConfigService {
    /// 统一处理配置缓存、远端读写及通知的工作器。
    client_worker: ConfigWorker,
    /// 当前配置服务的实例诊断标识。
    client_id: String,
}

impl std::fmt::Debug for NacosConfigService {
    /// 业务作用：展示配置服务所属命名空间和客户端标识，避免展开连接与认证配置。
    ///
    /// 参数说明：
    /// - `f`：接收诊断文本的格式化目标。
    ///
    /// 返回：输出成功或目标写入错误，不展示配置正文或认证参数。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NacosConfigService")
            .field(
                "namespace",
                &self.client_worker.client_props.get_namespace(),
            )
            .field("client_id", &self.client_id)
            .finish()
    }
}

const MODULE_NAME: &str = "config";
static SEQ: AtomicU64 = AtomicU64::new(1);

impl NacosConfigService {
    /// 业务作用：为配置服务分配客户端身份，并装配缓存、认证连接及后台变化处理。
    ///
    /// 参数说明：
    /// - `client_props`：地址目录、作用域、缓存和启动策略等参数。
    /// - `auth_plugin`：负责登录及请求身份材料的插件。
    /// - `config_filters`：按顺序执行的配置请求与响应过滤器。
    ///
    /// 返回：工作器构造成功后返回服务；目录或连接失败向上传递，是否允许应急启动由配置决定，不保证所有缓存已同步。
    pub async fn new(
        client_props: ClientProps,
        auth_plugin: std::sync::Arc<dyn AuthPlugin>,
        config_filters: Vec<Box<dyn ConfigFilter>>,
    ) -> crate::api::error::Result<Self> {
        let client_id = crate::common::util::generate_client_id(
            MODULE_NAME,
            &client_props.get_address_identifier(),
            &client_props.get_namespace_default_if_empty(),
            &SEQ,
        );
        let client_worker =
            ConfigWorker::new(client_props, auth_plugin, config_filters, client_id.clone()).await?;
        Ok(Self {
            client_worker,
            client_id,
        })
    }
}

impl NacosConfigService {
    /// 业务作用：在当前客户端作用域内读取配置，并保留客户端及配置身份的调用关联。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    ///
    /// 返回：过滤后的缓存或远端响应；缓存命中不重新验证新鲜度，远端读取错误向上传递，必需正文元数据缺失时可能 panic。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn get_config(
        &self,
        data_id: String,
        group: String,
    ) -> crate::api::error::Result<crate::api::config::ConfigResponse> {
        self.client_worker.get_config(data_id, group).await
    }

    /// 业务作用：通过工作器过滤并发布配置，统一认证和远端结果判断。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    /// - `content`：待发布正文。
    /// - `content_type`：可选正文格式，原样交给发布流程。
    ///
    /// 返回：远端业务接受时为真，传输或业务失败返回错误；不等待缓存同步和监听回调。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn publish_config(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
    ) -> crate::api::error::Result<bool> {
        self.client_worker
            .publish_config(data_id, group, content, content_type)
            .await
    }

    /// 业务作用：以旧内容摘要约束配置发布，由服务端判定条件是否满足。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    /// - `content`：待过滤并提交的新正文。
    /// - `content_type`：可选正文格式。
    /// - `cas_md5`：要求服务端匹配的旧内容 MD5。
    ///
    /// 返回：条件发布被远端接受时为真，否则返回错误；不使用本地缓存代替 CAS 判断。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn publish_config_cas(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: String,
    ) -> crate::api::error::Result<bool> {
        self.client_worker
            .publish_config_cas(data_id, group, content, content_type, cas_md5)
            .await
    }

    /// 业务作用：向指定灰度范围发布配置，接收范围由服务端解释。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    /// - `content`：待发布正文。
    /// - `content_type`：可选正文格式。
    /// - `beta_ips`：灰度接收地址文本，当前入口不解析地址集合。
    ///
    /// 返回：远端业务接受时为真，传输或业务拒绝返回错误；不在本地筛选监听者。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn publish_config_beta(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        beta_ips: String,
    ) -> crate::api::error::Result<bool> {
        self.client_worker
            .publish_config_beta(data_id, group, content, content_type, beta_ips)
            .await
    }

    /// 业务作用：携带扩展参数发布配置，保持统一过滤与专用字段优先规则。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    /// - `content`：待过滤的正文。
    /// - `content_type`：显式正文格式，存在时覆盖扩展参数同名字段。
    /// - `cas_md5`：可选条件写入摘要。
    /// - `params`：自定义扩展字段，其加密密钥值会被过滤结果覆盖。
    ///
    /// 返回：远端业务成功时为真，否则返回错误；扩展字段的实际支持范围由服务端决定。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn publish_config_param(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: Option<String>,
        params: std::collections::HashMap<String, String>,
    ) -> crate::api::error::Result<bool> {
        self.client_worker
            .publish_config_param(data_id, group, content, content_type, cas_md5, params)
            .await
    }

    /// 业务作用：删除当前命名空间内的指定配置，交由工作器验证远端业务结果。
    ///
    /// 参数说明：
    /// - `data_id`：待删除配置标识。
    /// - `group`：配置分组。
    ///
    /// 返回：远端删除结果成功时为真，否则返回错误；不自动删除本地缓存或取消本地监听。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn remove_config(
        &self,
        data_id: String,
        group: String,
    ) -> crate::api::error::Result<bool> {
        self.client_worker.remove_config(data_id, group).await
    }

    /// 业务作用：把配置接收者加入工作器，按需建立缓存和异步远端监听。
    ///
    /// 参数说明：
    /// - `data_id`：待监听配置标识。
    /// - `group`：配置分组。
    /// - `listener`：按 Arc 实例身份去重的接收者。
    ///
    /// 返回：工作器本地步骤结束后返回成功，不等待远端订阅确认，也不将首次查询失败作为该返回值的错误。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn add_listener(
        &self,
        data_id: String,
        group: String,
        listener: std::sync::Arc<dyn crate::api::config::ConfigChangeListener>,
    ) -> crate::api::error::Result<()> {
        self.client_worker
            .add_listener(data_id, group, listener)
            .await;
        Ok(())
    }

    /// 业务作用：移除指定配置的本地接收者，停止为该实例安排后续通知。
    ///
    /// 参数说明：
    /// - `data_id`：监听配置标识。
    /// - `group`：配置分组。
    /// - `listener`：原注册 Arc 实例，按身份匹配。
    ///
    /// 返回：完成本地移除尝试后成功，原本不存在也成功；不取消远端订阅或已经安排的业务回调。
    #[instrument(fields(client_id = &self.client_id, group = group, data_id = data_id), skip_all)]
    pub(crate) async fn remove_listener(
        &self,
        data_id: String,
        group: String,
        listener: std::sync::Arc<dyn crate::api::config::ConfigChangeListener>,
    ) -> crate::api::error::Result<()> {
        self.client_worker
            .remove_listener(data_id, group, listener)
            .await;
        Ok(())
    }
}
