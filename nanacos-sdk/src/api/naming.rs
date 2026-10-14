use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

use crate::api::plugin;
use crate::{api::error::Result, naming::NacosNamingService};
use serde::{Deserialize, Serialize};

use super::props::ClientProps;

const DEFAULT_CLUSTER_NAME: &str = "DEFAULT";

/// ServiceInstance for api.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInstance {
    /// 服务端分配的可选实例标识。
    pub instance_id: Option<String>,

    /// 对外可达地址；注册配置可显式覆盖宿主监听地址。
    pub ip: String,

    /// 实例监听端口。
    pub port: i32,

    /// Nacos 实例选择权重。
    pub weight: f64,

    /// 后端报告的健康状态。
    pub healthy: bool,

    /// 实例是否允许承载流量。
    pub enabled: bool,

    /// 是否发布 Nacos 临时实例，由客户端维持其存活。
    pub ephemeral: bool,

    /// 实例所属的可选集群名称。
    pub cluster_name: Option<String>,

    /// 实例或事件所属的服务名称。
    pub service_name: Option<String>,

    /// 随注册写入的实例属性，不得携带凭据。
    pub metadata: HashMap<String, String>,
}

impl ServiceInstance {
    /// 业务作用：供业务读取服务端分配的可选实例标识。
    /// 参数说明：无。
    /// 返回：返回当前记录中的服务端分配的可选实例标识，不触发网络查询。
    pub fn instance_id(&self) -> Option<&String> {
        self.instance_id.as_ref()
    }

    /// 业务作用：供业务读取实例的网络地址。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例的网络地址，不触发网络查询。
    pub fn ip(&self) -> &str {
        &self.ip
    }

    /// 业务作用：供业务读取实例监听端口。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例监听端口，不触发网络查询。
    pub fn port(&self) -> i32 {
        self.port
    }

    /// 业务作用：供业务读取实例选择权重。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例选择权重，不触发网络查询。
    pub fn weight(&self) -> f64 {
        self.weight
    }

    /// 业务作用：供业务读取后端报告的健康标志。
    /// 参数说明：无。
    /// 返回：返回当前记录中的后端报告的健康标志，不触发网络查询。
    pub fn healthy(&self) -> bool {
        self.healthy
    }

    /// 业务作用：供业务读取实例是否启用的标志。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例是否启用的标志，不触发网络查询。
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// 业务作用：供业务读取实例是否为临时实例的标志。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例是否为临时实例的标志，不触发网络查询。
    pub fn ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// 业务作用：供业务读取实例所属的可选集群。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例所属的可选集群，不触发网络查询。
    pub fn cluster_name(&self) -> Option<&String> {
        self.cluster_name.as_ref()
    }

    /// 业务作用：供业务读取实例所属的可选服务名。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例所属的可选服务名，不触发网络查询。
    pub fn service_name(&self) -> Option<&String> {
        self.service_name.as_ref()
    }

    /// 业务作用：供业务读取实例的扩展属性映射。
    /// 参数说明：无。
    /// 返回：返回当前记录中的实例的扩展属性映射，不触发网络查询。
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// 业务作用：生成实例地址和端口的展示文本。
    /// 参数说明：无。
    /// 返回：返回以冒号拼接的文本，不额外校验地址或执行解析。
    pub fn ip_and_port(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    /// 业务作用：判断两个实例记录的全部可见属性是否一致。
    /// 参数说明：`other` 为待比较的实例。
    /// 返回：所有实例属性及 metadata 一致时为 true，不仅比较地址。
    pub fn is_same_instance(&self, other: &ServiceInstance) -> bool {
        self.instance_id == other.instance_id
            && self.ip == other.ip
            && self.port == other.port
            && self.weight == other.weight
            && self.healthy == other.healthy
            && self.enabled == other.enabled
            && self.ephemeral == other.ephemeral
            && self.cluster_name == other.cluster_name
            && self.service_name == other.service_name
            && self.metadata == other.metadata
    }
}

impl Default for ServiceInstance {
    fn default() -> Self {
        Self {
            instance_id: Default::default(),
            ip: Default::default(),
            port: Default::default(),
            weight: 1.0,
            healthy: true,
            enabled: true,
            ephemeral: true,
            cluster_name: Some(DEFAULT_CLUSTER_NAME.to_owned()),
            service_name: Default::default(),
            metadata: Default::default(),
        }
    }
}

/// NamingChangeEvent when Instance change.
#[derive(Clone, Debug)]
pub struct NamingChangeEvent {
    /// 实例或事件所属的服务名称。
    pub service_name: String,
    /// Nacos 服务所属分组。
    pub group_name: String,
    /// 此次变更事件对应的集群范围。
    pub clusters: String,
    /// 变更后的可选实例集合。
    pub instances: Option<Vec<ServiceInstance>>,
}

/// 从候选实例中选择单个调用目标的扩展契约。
pub trait InstanceChooser {
    /// 业务作用：从候选集合选择一个实例供调用方使用。
    /// 参数说明：无。
    /// 返回：有合适候选时返回实例；没有候选时返回 None。
    fn choose(self) -> Option<ServiceInstance>;
}

/// The NamingEventListener receive an event of [`NamingChangeEvent`].
pub trait NamingEventListener: Send + Sync + 'static {
    /// 业务作用：向业务监听器交付实例集合变化。
    /// 参数说明：`event` 为共享的服务与实例变化数据。
    /// 返回：监听器处理通知；回调没有返回值。
    fn event(&self, event: Arc<NamingChangeEvent>);
}

/// Api [`NamingService`].
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> nacos_sdk::api::error::Result<()> {
/// let naming_service = nacos_sdk::api::naming::NamingServiceBuilder::new(
///       nacos_sdk::api::props::ClientProps::new()
///          .server_addr("127.0.0.1:8848")
///          // Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
///          .namespace("")
///          .app_name("todo-your-app-name"),
///  )
///  .build()
///  .await?;
/// # Ok(())
/// # }
/// ```
#[doc(alias("naming", "sdk", "api"))]
#[derive(Clone, Debug)]
pub struct NamingService {
    inner: Arc<NacosNamingService>,
}

impl NamingService {
    /// 业务作用：向指定 Nacos 服务发布单个实例。
    /// 参数说明：`service_name` 为服务名，`group_name` 为可选分组，`service_instance` 为实例属性。
    /// 返回：服务端接受注册时成功；非法名称、认证或传输失败返回错误。
    pub async fn register_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .register_instance(service_name, group_name, service_instance)
            .await
    }

    /// 业务作用：从指定 Nacos 服务注销实例。
    /// 参数说明：`service_name` 为服务名，`group_name` 为可选分组，`service_instance` 标识目标实例。
    /// 返回：服务端接受注销时成功；非法名称或远端操作失败返回错误。
    pub async fn deregister_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .deregister_instance(service_name, group_name, service_instance)
            .await
    }

    /// 业务作用：向同一 Nacos 服务批量发布实例。
    /// 参数说明：`service_name` 为服务名，`group_name` 为可选分组，`service_instances` 为发布列表。
    /// 返回：服务端接受请求时成功；校验或远端操作失败返回错误。
    pub async fn batch_register_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instances: Vec<ServiceInstance>,
    ) -> Result<()> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .batch_register_instance(service_name, group_name, service_instances)
            .await
    }

    /// 业务作用：读取指定服务及集群范围中的实例。
    /// 参数说明：`service_name` 与 `group_name` 定位服务，`clusters` 限定集群，`subscribe` 控制是否订阅变化。
    /// 返回：返回该范围的实例集合；参数或后端访问失败返回错误。
    pub async fn get_all_instances(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
    ) -> Result<Vec<ServiceInstance>> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .get_all_instances(service_name, group_name, clusters, subscribe)
            .await
    }

    /// 业务作用：按健康状态筛选指定服务实例。
    /// 参数说明：`service_name` 与 `group_name` 定位服务，`clusters` 限定集群，`subscribe` 控制订阅，`healthy` 指定健康状态。
    /// 返回：返回符合状态的实例集合；参数或后端访问失败返回错误。
    pub async fn select_instances(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
        healthy: bool,
    ) -> Result<Vec<ServiceInstance>> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .select_instances(service_name, group_name, clusters, subscribe, healthy)
            .await
    }

    /// 业务作用：为一次业务调用选择健康实例。
    /// 参数说明：`service_name` 与 `group_name` 定位服务，`clusters` 限定集群，`subscribe` 控制订阅。
    /// 返回：返回一个健康候选；没有候选或访问失败返回错误。
    pub async fn select_one_healthy_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
    ) -> Result<ServiceInstance> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .select_one_healthy_instance(service_name, group_name, clusters, subscribe)
            .await
    }

    /// 业务作用：分页读取指定分组的服务目录。
    /// 参数说明：`page_no` 与 `page_size` 指定分页，`group_name` 为可选分组。
    /// 返回：返回服务名列表与服务总数；后端访问失败返回错误。
    pub async fn get_service_list(
        &self,
        page_no: i32,
        page_size: i32,
        group_name: Option<String>,
    ) -> Result<(Vec<String>, i32)> {
        self.inner
            .get_service_list(page_no, page_size, group_name)
            .await
    }

    /// 业务作用：为指定服务范围登记实例变化监听器。
    /// 参数说明：`service_name` 与 `group_name` 定位服务，`clusters` 限定集群，`event_listener` 接收变化。
    /// 返回：成功后持续交付变化；非法名称或订阅失败返回错误。
    pub async fn subscribe(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        event_listener: Arc<dyn NamingEventListener>,
    ) -> Result<()> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .subscribe(service_name, group_name, clusters, event_listener)
            .await
    }

    /// 业务作用：解除指定服务范围的实例变化监听器。
    /// 参数说明：`service_name` 与 `group_name` 定位服务，`clusters` 限定集群，`event_listener` 标识要移除的监听器。
    /// 返回：成功后不再通过该订阅交付变化；参数或取消失败返回错误。
    pub async fn unsubscribe(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        event_listener: Arc<dyn NamingEventListener>,
    ) -> Result<()> {
        crate::common::util::check_not_blank(&service_name, "service_name")?;
        self.inner
            .unsubscribe(service_name, group_name, clusters, event_listener)
            .await
    }
}

/// Builder of api [`NamingService`].
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> nacos_sdk::api::error::Result<()> {
/// let naming_service = nacos_sdk::api::naming::NamingServiceBuilder::new(
///       nacos_sdk::api::props::ClientProps::new()
///          .server_addr("127.0.0.1:8848")
///          // Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
///          .namespace("")
///          .app_name("todo-your-app-name"),
///  )
///  .build()
///  .await?;
/// # Ok(())
/// # }
/// ```
#[doc(alias("naming", "builder"))]
pub struct NamingServiceBuilder {
    client_props: ClientProps,
    auth_plugin: Option<Arc<dyn plugin::AuthPlugin>>,
}

impl NamingServiceBuilder {
    /// 业务作用：使用显式连接属性创建 Nacos 客户端构建器。
    /// 参数说明：`client_props` 为服务端地址、命名空间和客户端属性。
    /// 返回：返回尚未建立连接的构建器。
    pub fn new(client_props: ClientProps) -> Self {
        NamingServiceBuilder {
            client_props,
            auth_plugin: None,
        }
    }

    /// 业务作用：为客户端选择 Nacos HTTP 登录认证。
    /// 参数说明：无。
    /// 返回：返回已绑定 HTTP 认证插件的构建器，不立即登录。
    #[cfg(feature = "auth-by-http")]
    pub fn enable_auth_plugin_http(self) -> Self {
        self.with_auth_plugin(Arc::new(plugin::HttpLoginAuthPlugin::default()))
    }

    /// 业务作用：为客户端选择 Aliyun RAM 签名认证。
    /// 参数说明：无。
    /// 返回：返回已绑定 RAM 认证插件的构建器，不立即发起请求。
    #[cfg(feature = "auth-by-aliyun")]
    pub fn enable_auth_plugin_aliyun(self) -> Self {
        self.with_auth_plugin(Arc::new(plugin::AliyunRamAuthPlugin::default()))
    }

    /// Set [`plugin::AuthPlugin`]
    pub fn with_auth_plugin(mut self, auth_plugin: Arc<dyn plugin::AuthPlugin>) -> Self {
        self.auth_plugin = Some(auth_plugin);
        self
    }

    /// 业务作用：按配置和认证插件建立 Nacos 命名客户端。
    /// 参数说明：无。
    /// 返回：成功返回持有后台连接的客户端；初始化失败返回错误。
    pub async fn build(self) -> Result<NamingService> {
        #[cfg(feature = "tracing-log")]
        {
            // $HOME/logs/nacos
            let log_path = crate::common::util::HOME_DIR.to_owned() + "/logs/nacos";
            let log_level = "INFO".to_string();
            crate::common::log::init(log_path, log_level);
        }

        let auth_plugin = match self.auth_plugin {
            None => Arc::new(plugin::NoopAuthPlugin::default()),
            Some(plugin) => plugin,
        };
        let inner = NacosNamingService::new(self.client_props, auth_plugin).await?;
        let inner = Arc::new(inner);
        Ok(NamingService { inner })
    }
}

impl Default for NamingServiceBuilder {
    fn default() -> Self {
        NamingServiceBuilder {
            client_props: ClientProps::new(),
            auth_plugin: None,
        }
    }
}
