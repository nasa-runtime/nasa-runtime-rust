//! Nacos 服务注册、发现与实例变更通知的内部实现。
//!
//! 实例缓存由服务端推送与定期查询共同维护；注册和订阅请求保留可恢复的重试状态。
//!
//! - `chooser`：从可用实例中执行加权随机选择。
//! - `dto`：承载服务与实例快照。
//! - `handler`：接收服务端实例变更推送。
//! - `message`：定义注册、查询和订阅使用的 gRPC 请求与响应。
//! - `observable`：更新实例缓存并分派变更通知。
//! - `redo`：保存注册和订阅意图，在连接恢复后重新提交。
//! - `updater`：为订阅服务保留后台查询路径。
//!
//! 对外入口为 [`crate::api::naming::NamingService`]；缓存命中不表示服务端订阅已确认。

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tracing::{debug, info, instrument};

use crate::api::error::Error::ErrResult;
use crate::api::error::Result;
use crate::api::naming::{InstanceChooser, NamingEventListener, ServiceInstance};
use crate::api::plugin::AuthPlugin;
use crate::api::props::ClientProps;

use crate::common::cache::{Cache, CacheBuilder};
use crate::common::executor;
use crate::common::remote::grpc::NacosGrpcClient;
use crate::common::remote::grpc::NacosGrpcClientBuilder;
use crate::common::remote::grpc::message::GrpcRequestMessage;
use crate::common::remote::grpc::message::GrpcResponseMessage;
use crate::common::remote::server_list::create_server_list_provider;
use crate::naming::message::request::*;
use crate::naming::message::response::*;

use self::chooser::RandomWeightChooser;
use self::dto::ServiceInfo;
use self::handler::NamingPushRequestHandler;
use self::observable::service_info_observable::ServiceInfoObserver;
use self::redo::{AutomaticRequest, NamingRedoTask, RedoTask, RedoTaskExecutor};
use self::updater::ServiceInfoUpdater;

mod chooser;
mod dto;
mod handler;
mod message;
mod observable;
mod redo;
mod updater;

/// 同一命名空间内的注册请求、发现缓存和订阅生命周期。
pub(crate) struct NacosNamingService {
    /// 共享的认证与 gRPC 传输入口，负责连接状态通知。
    nacos_grpc_client: Arc<NacosGrpcClient>,
    /// 已归一化的命名空间，加入所有注册和发现请求。
    namespace: String,
    /// 按请求身份保存的重试任务，重连时重新激活。
    redo_task_executor: Arc<RedoTaskExecutor>,
    /// 订阅服务的周期查询器，为推送之外提供刷新来源。
    service_info_updater: ServiceInfoUpdater,
    /// 本客户端的诊断身份，区分同进程内的多个发现入口。
    client_id: String,
    /// 当前已知的服务实例快照，可按配置从磁盘恢复。
    naming_cache: Arc<Cache<ServiceInfo>>,
    /// 将缓存变化分派给已注册接收者的观察器。
    observer: ServiceInfoObserver,
}

impl std::fmt::Debug for NacosNamingService {
    /// 业务作用：用命名空间和客户端身份区分发现入口，避免展开传输与认证对象。
    ///
    /// 参数说明：
    /// - `f`：接收诊断文本的格式化目标，不写入服务实例清单或认证信息。
    ///
    /// 返回：两个身份字段写入成功时完成；目标写入失败时返回格式化错误。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NacosNamingService")
            .field("namespace", &self.namespace)
            .field("client_id", &self.client_id)
            .finish()
    }
}

const MODULE_NAME: &str = "naming";
static SEQ: AtomicU64 = AtomicU64::new(1);

impl NacosNamingService {
    /// 业务作用：装配同一命名空间的发现缓存、连接状态处理和订阅刷新入口。
    ///
    /// 参数说明：
    /// - `client_props`：服务器来源、命名空间、缓存恢复、推送保护、认证上下文和重试等客户端策略。
    /// - `auth_plugin`：用于远端请求认证的共享插件，由 gRPC 客户端持有并调用。
    ///
    /// 返回：已装配并启动后台处理的发现服务；服务器来源或客户端初始化错误直接传播，允许缓存启动时不保证远端已经连接。
    pub(crate) async fn new(
        client_props: ClientProps,
        auth_plugin: Arc<dyn AuthPlugin>,
    ) -> Result<Self> {
        let namespace = client_props.get_namespace_default_if_empty();

        let client_id = crate::common::util::generate_client_id(
            MODULE_NAME,
            &client_props.get_address_identifier(),
            &namespace,
            &SEQ,
        );

        // 重试目录先于连接回调建立，连接恢复后才能重新提交本客户端保存的注册与订阅意图。
        let redo_task_executor = Arc::new(RedoTaskExecutor::new(client_id.clone()));
        let redo_task_executor_on_connected = redo_task_executor.clone();
        let redo_task_executor_on_disconnected = redo_task_executor.clone();

        // 是否接受启动时的磁盘快照由配置决定；恢复出的实例并不构成远端在线的证明。
        let naming_cache: Cache<ServiceInfo> = CacheBuilder::naming(namespace.clone())
            .load_cache_at_start(client_props.get_naming_load_cache_at_start())
            .disk_store(client_props.get_cache_dir())
            .build()
            .await;
        let naming_cache = Arc::new(naming_cache);

        // 缓存变更经同一个观察器分派，空推送保护由客户端策略明确决定。
        let (observer, emitter) = observable::service_info_observable::create(
            client_id.clone(),
            naming_cache.clone(),
            client_props.get_naming_push_empty_protection(),
        );

        let server_request_handler = NamingPushRequestHandler::new(emitter.clone());

        let server_list_provider = create_server_list_provider(&client_props).await?;

        let nacos_grpc_client = NacosGrpcClientBuilder::new(server_list_provider)
            .port(client_props.get_remote_grpc_port())
            .namespace(namespace.clone())
            .client_version(client_props.get_client_version())
            .support_remote_connection(true)
            .support_config_remote_metrics(true)
            .support_naming_delta_push(false)
            .support_naming_remote_metric(false)
            .add_labels(client_props.get_labels())
            .add_label(
                crate::api::constants::common_remote::LABEL_SOURCE.to_owned(),
                crate::api::constants::common_remote::LABEL_SOURCE_SDK.to_owned(),
            )
            .add_label(
                crate::api::constants::common_remote::LABEL_MODULE.to_owned(),
                crate::api::constants::common_remote::LABEL_MODULE_NAMING.to_owned(),
            )
            .app_name(client_props.get_app_name())
            .register_server_request_handler::<NotifySubscriberRequest>(Arc::new(
                server_request_handler,
            ))
            .connected_listener(move |connection_id| {
                info!("connection {} connected.", connection_id);
                let redo = redo_task_executor_on_connected.clone();
                // 新连接不能继承旧连接的注册状态，异步重新激活保存的意图。
                executor::spawn(async move {
                    redo.on_grpc_client_reconnect().await;
                });
            })
            .disconnected_listener(move |connection_id| {
                info!("connection {} disconnected.", connection_id);
                let redo = redo_task_executor_on_disconnected.clone();
                // 失联时暂停周期重试，等待下一次连接通知后恢复；不在连接回调内等待目录锁。
                executor::spawn(async move {
                    redo.on_grpc_client_disconnect().await;
                });
            })
            .auth_context(client_props.get_auth_context())
            .auth_plugin(auth_plugin)
            .max_retries(client_props.get_max_retries())
            .emergency_start(client_props.get_naming_load_cache_at_start())
            .build(client_id.clone())
            .await?;

        let nacos_grpc_client = Arc::new(nacos_grpc_client);

        // 周期查询与推送共用缓存和事件入口，使无推送期间仍有实例刷新来源。
        let service_info_updater =
            ServiceInfoUpdater::new(emitter, naming_cache.clone(), nacos_grpc_client.clone());

        Ok(NacosNamingService {
            redo_task_executor,
            nacos_grpc_client,
            namespace,
            service_info_updater,
            client_id,
            naming_cache,
            observer,
        })
    }

    /// 业务作用：经当前客户端发送发现请求，统一使用其认证、连接和传输重试策略。
    ///
    /// 参数说明：
    /// - `request`：具有明确 gRPC 消息类型的请求，所有权交给传输入口。
    ///
    /// 返回：解码为指定响应类型的消息或传输错误；业务响应码由上层操作另行校验，本函数不创建 redo 任务。
    async fn request_to_server<R, P>(&self, request: R) -> Result<P>
    where
        R: GrpcRequestMessage + 'static,
        P: GrpcResponseMessage + 'static,
    {
        self.nacos_grpc_client.send_request::<R, P>(request).await
    }

    /// 业务作用：先保存可重试的注册意图，再等待本次远端确认，连接恢复时仍能重新提交注册。
    ///
    /// 参数说明：
    /// - `request`：可克隆且具有稳定重试身份的注册请求，副本用于后续 redo。
    /// - `operation`：业务响应校验时使用的操作名称，用于区分失败来源。
    ///
    /// 返回：请求和业务响应均成功后暂停该重试任务；失败时返回错误并保留任务，后续连接通知和调度可继续处理。
    async fn execute_by_register_with_redo<R, P>(&self, request: R, operation: &str) -> Result<()>
    where
        R: GrpcRequestMessage + AutomaticRequest + Clone + 'static,
        P: GrpcResponseMessage + 'static,
    {
        let auto_request: Arc<dyn AutomaticRequest> = Arc::new(request.clone());
        let redo_task = NamingRedoTask::new(self.nacos_grpc_client.clone(), auto_request);
        let redo_task = Arc::new(redo_task);

        // 必须在首次请求前登记意图，否则连接中断会丢失恢复注册所需的信息。
        redo_task.active();
        self.redo_task_executor.add_task(redo_task.clone()).await;

        let response = self.request_to_server::<R, P>(request).await?;
        crate::common::error::handle_response(&response, operation)?;

        // 只有远端明确确认才能暂停重试，任务仍保留供后续重连重新激活。
        redo_task.frozen();
        Ok(())
    }

    /// 业务作用：在远端确认注销后移除相同身份的注册重试任务，避免成功注销后再次恢复注册。
    ///
    /// 参数说明：
    /// - `request`：注销请求，其自动请求身份用于定位此前保存的注册任务。
    /// - `operation`：响应校验使用的操作名称，用于携带注销失败的上下文。
    ///
    /// 返回：远端确认且本地任务移除后成功；传输或业务响应失败时保留原任务，不将未知结果视为注销完成。
    async fn execute_by_deregister_with_redo<R, P>(&self, request: R, operation: &str) -> Result<()>
    where
        R: GrpcRequestMessage + AutomaticRequest + Clone + 'static,
        P: GrpcResponseMessage + 'static,
    {
        let auto_request: Arc<dyn AutomaticRequest> = Arc::new(request.clone());
        let redo_task = NamingRedoTask::new(self.nacos_grpc_client.clone(), auto_request);

        let response = self.request_to_server::<R, P>(request).await?;
        crate::common::error::handle_response(&response, operation)?;

        // 注销确认之前仍保留原注册意图；失败分支不会悄悄删除恢复依据。
        self.redo_task_executor
            .remove_task(redo_task.task_key().as_str())
            .await;
        Ok(())
    }
}

impl NacosNamingService {
    /// 业务作用：通过临时实例协议登记服务端地址，并保留连接恢复时重新注册的请求。
    ///
    /// 参数说明：
    /// - `service_name`：当前命名空间内接收该实例的服务名称。
    /// - `group_name`：服务分组，缺省或空串时归一为默认分组，不裁剪空白字符。
    /// - `service_instance`：要登记的地址、端口、集群、权重及元数据，由调用方选择临时实例路径。
    ///
    /// 返回：远端接受注册时成功；失败返回错误并保留 redo 意图，不代表实例已经可被发现。
    async fn register_ephemeral_instance_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        info!(
            "register ephemeral instance: service_name: {service_name}, group_name: {group_name:?}"
        );
        let namespace = Some(self.namespace.clone());
        let group_name = crate::common::util::normalize_group_name(group_name);
        let request = InstanceRequest::register(
            service_instance,
            Some(service_name),
            namespace,
            Some(group_name),
        );

        self.execute_by_register_with_redo::<InstanceRequest, InstanceResponse>(
            request,
            "register_ephemeral_instance",
        )
        .await
    }

    /// 业务作用：使用持久实例请求登记服务地址，并保存可由连接恢复重新提交的注册意图。
    ///
    /// 参数说明：
    /// - `service_name`：实例所属服务名称，按本客户端命名空间发送。
    /// - `group_name`：服务分组；缺省或空串使用默认分组，不裁剪其它内容。
    /// - `service_instance`：待持久注册的实例描述，由上层按 ephemeral 标志分派到本路径。
    ///
    /// 返回：远端确认注册时成功；失败传播并保留 redo 任务，持久实例的存续由服务端管理。
    async fn register_persistent_instance_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        info!(
            "register persistent instance: service_name: {service_name}, group_name: {group_name:?}"
        );
        let namespace = Some(self.namespace.clone());
        let group_name = crate::common::util::normalize_group_name(group_name);
        let request = PersistentInstanceRequest::register(
            service_instance,
            Some(service_name),
            namespace,
            Some(group_name),
        );

        self.execute_by_register_with_redo::<PersistentInstanceRequest, InstanceResponse>(
            request,
            "register_persistent_instance",
        )
        .await
    }

    /// 业务作用：请求撤销临时实例注册，确认后停止本客户端对该实例的自动恢复注册。
    ///
    /// 参数说明：
    /// - `service_name`：待注销实例所属的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `service_instance`：待注销的实例身份，应与此前注册的地址、端口及集群对应。
    ///
    /// 返回：远端确认且相同身份的 redo 任务移除后成功；失败保留原任务并返回错误。
    async fn deregister_ephemeral_instance_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        info!(
            "deregister ephemeral instance: service_name: {service_name}, group_name: {group_name:?}"
        );

        let namespace = Some(self.namespace.clone());
        let group_name = crate::common::util::normalize_group_name(group_name);
        let request = InstanceRequest::deregister(
            service_instance,
            Some(service_name),
            namespace,
            Some(group_name),
        );

        self.execute_by_deregister_with_redo::<InstanceRequest, InstanceResponse>(
            request,
            "deregister_ephemeral_instance",
        )
        .await
    }

    /// 业务作用：通过持久实例协议请求注销，远端确认后删除对应的本地恢复意图。
    ///
    /// 参数说明：
    /// - `service_name`：持久实例所属的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `service_instance`：需要删除的持久实例身份，不在本方法内改写其地址或集群。
    ///
    /// 返回：服务端接受注销且 redo 目录清理后成功；失败保留原注册任务，不能据此认定远端实例已经消失。
    async fn deregister_persistent_instance_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        info!(
            "deregister persistent instance: service_name: {service_name}, group_name: {group_name:?}"
        );
        let namespace = Some(self.namespace.clone());
        let group_name = crate::common::util::normalize_group_name(group_name);
        let request = PersistentInstanceRequest::deregister(
            service_instance,
            Some(service_name),
            namespace,
            Some(group_name),
        );

        self.execute_by_deregister_with_redo::<PersistentInstanceRequest, InstanceResponse>(
            request,
            "deregister_persistent_instance",
        )
        .await
    }

    /// 业务作用：把同一服务的一组实例合成单个批量注册请求，并以整体请求保留重试意图。
    ///
    /// 参数说明：
    /// - `service_name`：该批实例共同归属的服务名称。
    /// - `group_name`：整批请求使用的服务分组，缺省或空串使用默认分组。
    /// - `service_instances`：要发送的实例列表，本方法不逐项分派临时与持久协议或拆分批次。
    ///
    /// 返回：批量请求得到业务成功响应后完成；失败保留整批 redo，不提供逐实例成功结果。
    async fn batch_register_instance_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instances: Vec<ServiceInstance>,
    ) -> Result<()> {
        let namespace = Some(self.namespace.clone());
        let group_name = crate::common::util::normalize_group_name(group_name);
        let request = BatchInstanceRequest::new(
            service_instances,
            namespace,
            Some(service_name),
            Some(group_name),
        );

        self.execute_by_register_with_redo::<BatchInstanceRequest, BatchInstanceResponse>(
            request,
            "batch_register_instance",
        )
        .await
    }

    /// 业务作用：按调用策略读取服务实例快照，订阅模式优先使用缓存，直接查询模式等待远端响应。
    ///
    /// 参数说明：
    /// - `service_name`：要查询的服务名称，与分组组合定位发现缓存。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `clusters`：服务端集群筛选列表，按原顺序以逗号连接，不在本地验证集群存在性。
    /// - `subscribe`：为真时读取缓存或建立订阅；为假时直接查询且不限制健康状态。
    ///
    /// 返回：快照中的全部实例；订阅失败或 hosts 缺失返回空列表，直接查询失败传播错误，不将缓存视为实时可用性证明。
    async fn get_all_instances_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
    ) -> Result<Vec<ServiceInstance>> {
        let cluster_str = clusters.join(",");
        let group_name = crate::common::util::normalize_group_name(group_name);

        let service_info;
        if subscribe {
            let grouped_name = ServiceInfo::get_grouped_service_name(&service_name, &group_name);
            let key = ServiceInfo::get_key(&grouped_name, &cluster_str);

            if let Some(cached_info) = self.naming_cache.get(&key) {
                info!("get_all_instances returned cached service_info, group_key={key}");
                service_info = Some(cached_info.clone());
            } else {
                let subscribe_service_info = self
                    .subscribe_async(service_name, Some(group_name), clusters, None)
                    .await;
                if let Ok(subscribe_service_info) = subscribe_service_info {
                    service_info = Some(subscribe_service_info);
                } else {
                    // 订阅路径将本次远端失败折叠为空发现结果；保留的后台订阅仍可能稍后获得实例。
                    service_info = None;
                }
            }
        } else {
            let request = ServiceQueryRequest {
                cluster: cluster_str,
                group_name: Some(group_name),
                healthy_only: false,
                udp_port: 0,
                namespace: Some(self.namespace.clone()),
                service_name: Some(service_name),
                ..Default::default()
            };

            let response = self
                .request_to_server::<ServiceQueryRequest, QueryServiceResponse>(request)
                .await?;
            crate::common::error::handle_response(&response, "get_all_instances")?;
            service_info = Some(response.service_info);
        }
        let Some(service_info) = service_info else {
            return Ok(Vec::default());
        };
        let instances = service_info.hosts;
        let Some(instances) = instances else {
            return Ok(Vec::default());
        };
        Ok(instances)
    }

    /// 业务作用：从发现快照中筛选指定健康状态且已启用、权重大于零的实例。
    ///
    /// 参数说明：
    /// - `service_name`：参与选择的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `clusters`：传给发现查询的集群列表，不额外改变返回实例顺序。
    /// - `subscribe`：是否允许缓存及后台订阅参与获取候选实例。
    /// - `healthy`：需要匹配的健康标志；为假时选择不健康实例，仍排除禁用或非正权重实例。
    ///
    /// 返回：满足全部条件的实例列表，可以为空；沿用实例获取路径的错误及空结果语义。
    async fn select_instances_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
        healthy: bool,
    ) -> Result<Vec<ServiceInstance>> {
        let group_name = crate::common::util::normalize_group_name(group_name);

        let all_instance = self
            .get_all_instances_async(service_name, Some(group_name), clusters, subscribe)
            .await?;
        let ret: Vec<ServiceInstance> = all_instance
            .into_iter()
            .filter(|instance| {
                healthy == instance.healthy && instance.enabled && instance.weight > 0.0
            })
            .collect();
        Ok(ret)
    }

    /// 业务作用：从健康、已启用且权重为正的发现结果中按权重随机选择一个实例。
    ///
    /// 参数说明：
    /// - `service_name`：用于查询和无可用实例错误上下文的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `clusters`：限制查询范围的集群列表。
    /// - `subscribe`：是否通过缓存与订阅取得候选实例；缓存健康标志不代表本次调用完成了探活。
    ///
    /// 返回：一个候选实例；查询、权重选择器构造失败或没有可选实例时返回错误，不主动连接实例。
    async fn select_one_healthy_instance_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
    ) -> Result<ServiceInstance> {
        let group_name = crate::common::util::normalize_group_name(group_name);
        let service_name_for_tip = service_name.clone();

        let ret = self
            .select_instances_async(
                service_name.clone(),
                Some(group_name),
                clusters,
                subscribe,
                true,
            )
            .await?;
        let chooser = RandomWeightChooser::new(service_name, ret)?;
        let Some(instance) = chooser.choose() else {
            return Err(ErrResult(format!(
                "no available {service_name_for_tip} service instance can be selected"
            )));
        };
        Ok(instance)
    }

    /// 业务作用：向服务端分页读取当前命名空间及分组中的服务名称，不创建实例订阅。
    ///
    /// 参数说明：
    /// - `page_no`：请求页码，原样传给服务端，本层不校验区间。
    /// - `page_size`：请求每页容量，原样发送，不在本地自动翻页。
    /// - `group_name`：查询分组，缺省或空串使用默认分组。
    ///
    /// 返回：服务端提供的名称列表及 count；传输或业务响应失败时返回错误，不从本地缓存补齐。
    async fn get_service_list_async(
        &self,
        page_no: i32,
        page_size: i32,
        group_name: Option<String>,
    ) -> Result<(Vec<String>, i32)> {
        let group_name = crate::common::util::normalize_group_name(group_name);
        let namespace = Some(self.namespace.clone());

        let request = ServiceListRequest {
            page_no,
            page_size,
            group_name: Some(group_name),
            namespace,
            ..Default::default()
        };

        let response = self
            .request_to_server::<ServiceListRequest, ServiceListResponse>(request)
            .await?;
        crate::common::error::handle_response(&response, "get_service_list")?;

        Ok((response.service_names, response.count))
    }

    /// 业务作用：建立本地刷新、监听及恢复意图，并按缓存是否命中选择远端订阅的等待方式。
    ///
    /// 参数说明：
    /// - `service_name`：需要持续获取实例变化的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `clusters`：服务端订阅与刷新任务的集群筛选；监听器及本方法缓存读取使用不含集群的服务键。
    /// - `event_listener`：可选变更接收者；为空时仍安排刷新任务和远端订阅。
    ///
    /// 返回：缓存命中时立即交付快照并在后台发送请求；未命中时等待远端结果，失败保留本地任务和监听器，不回滚订阅意图。
    async fn subscribe_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        event_listener: Option<Arc<dyn NamingEventListener>>,
    ) -> Result<ServiceInfo> {
        let clusters = clusters.join(",");
        let group_name = crate::common::util::normalize_group_name(group_name);
        // 此处监听器与缓存共用不含集群的服务键；集群限制仍随请求发送，不把它误当作独立监听身份。
        let key = ServiceInfo::get_key_without_clusters(&service_name, &group_name);

        // 先保留周期查询来源，使推送尚未可用或短时失联时仍能尝试刷新。
        self.service_info_updater
            .schedule_update(
                self.namespace.clone(),
                service_name.clone(),
                group_name.clone(),
                clusters.clone(),
            )
            .await;

        // 接收者在发送订阅请求前登记，避免远端推送先到而本地尚无接收者。
        if let Some(event_listener) = event_listener {
            self.observer.subscribe(key.clone(), event_listener).await;
        }

        let request = SubscribeServiceRequest::new(
            true,
            clusters,
            Some(service_name),
            Some(self.namespace.clone()),
            Some(group_name),
        );

        let auto_request: Arc<dyn AutomaticRequest> = Arc::new(request.clone());
        let redo_task = NamingRedoTask::new(self.nacos_grpc_client.clone(), auto_request);

        let redo_task = Arc::new(redo_task);
        // 网络请求前保存可恢复意图；本次调用失败不会撤销后续重连恢复。
        redo_task.active();
        self.redo_task_executor.add_task(redo_task.clone()).await;

        // 缓存可能来自磁盘恢复或运行中更新；命中只提供已有快照，不证明远端订阅已建立。
        if let Some(cached_info) = self.naming_cache.get(&key) {
            info!("subscribe returned cached service_info, group_key={key}");

            // 命中缓存时不让本次读取等待远端；后台失败保留 redo，成功才暂停重复请求。
            let remote_client = self.nacos_grpc_client.clone();
            executor::spawn(async move {
                let response = remote_client
                    .send_request::<SubscribeServiceRequest, SubscribeServiceResponse>(request)
                    .await;
                if let Ok(resp) = response
                    && crate::common::error::handle_response(&resp, "subscribe").is_ok()
                {
                    debug!("subscribe the {resp:?}");
                    redo_task.frozen();
                }
            });

            return Ok(cached_info.clone());
        }

        let response = self
            .request_to_server::<SubscribeServiceRequest, SubscribeServiceResponse>(request)
            .await?;
        crate::common::error::handle_response(&response, "subscribe")?;

        debug!("subscribe the {response:?}");
        // 没有缓存时必须先确认业务响应；只有成功响应才能暂停本次订阅的重试。
        redo_task.frozen();

        Ok(response.service_info)
    }

    /// 业务作用：先停止本地刷新与指定监听，再向远端取消订阅，确认后移除恢复订阅的任务。
    ///
    /// 参数说明：
    /// - `service_name`：待取消订阅的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `clusters`：停止刷新及远端取消请求使用的集群范围。
    /// - `event_listener`：需要从不含集群的服务键下移除的接收者；为空时不修改观察器列表。
    ///
    /// 返回：远端确认取消且 redo 移除后成功；失败返回错误，本地停止动作不回滚，原 redo 仍保留，缓存不在此删除。
    async fn unsubscribe_async(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        event_listener: Option<Arc<dyn NamingEventListener>>,
    ) -> Result<()> {
        let clusters = clusters.join(",");
        let group_name = crate::common::util::normalize_group_name(group_name);

        // 本地先停止周期刷新，防止取消请求期间继续安排该范围的查询；远端失败不会恢复此任务。
        self.service_info_updater
            .stop_update(service_name.clone(), group_name.clone(), clusters.clone())
            .await;

        // 不再向该接收者安排新事件；此操作不等待此前已投递回调结束。
        if let Some(event_listener) = event_listener {
            // 使用与登记时相同的服务键，不能改用含集群的键而遗留原监听器。
            let key = ServiceInfo::get_key_without_clusters(&service_name, &group_name);
            self.observer.unsubscribe(key, event_listener).await;
        }

        let request = SubscribeServiceRequest::new(
            false,
            clusters,
            Some(service_name),
            Some(self.namespace.clone()),
            Some(group_name),
        );

        let auto_request: Arc<dyn AutomaticRequest> = Arc::new(request.clone());
        let redo_task = NamingRedoTask::new(self.nacos_grpc_client.clone(), auto_request);

        let response = self
            .request_to_server::<SubscribeServiceRequest, SubscribeServiceResponse>(request)
            .await?;
        crate::common::error::handle_response(&response, "unsubscribe")?;
        debug!("unsubscribe the {response:?}");
        // 远端确认之后才删除恢复依据；错误路径保留原 redo，调用方不能把本地停止当作远端取消成功。
        self.redo_task_executor
            .remove_task(redo_task.task_key().as_str())
            .await;
        Ok(())
    }
}

impl NacosNamingService {
    /// 业务作用：按实例的 ephemeral 标志选择临时或持久注册协议，并记录调用身份。
    ///
    /// 参数说明：
    /// - `service_name`：实例所属服务名称，由对外 API 负责校验非空。
    /// - `group_name`：服务分组，缺省或空串由具体注册路径补入默认值。
    /// - `service_instance`：实例地址及属性，其中 ephemeral 决定本次请求类型。
    ///
    /// 返回：所选注册操作的远端确认结果；失败时保留注册恢复意图，不隐式切换另一种协议。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn register_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        if service_instance.ephemeral {
            self.register_ephemeral_instance_async(service_name, group_name, service_instance)
                .await
        } else {
            self.register_persistent_instance_async(service_name, group_name, service_instance)
                .await
        }
    }

    /// 业务作用：按实例类型选择对应注销协议，使成功注销与本地恢复任务清理保持同一路径。
    ///
    /// 参数说明：
    /// - `service_name`：要注销实例所在的服务名称。
    /// - `group_name`：实例登记时的分组，缺省或空串使用默认分组。
    /// - `service_instance`：待注销实例描述，ephemeral 应与其注册类型一致。
    ///
    /// 返回：所选注销请求的确认结果；失败仍保留原注册 redo，调用方不能据此认为注销已经生效。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn deregister_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: ServiceInstance,
    ) -> Result<()> {
        if service_instance.ephemeral {
            self.deregister_ephemeral_instance_async(service_name, group_name, service_instance)
                .await
        } else {
            self.deregister_persistent_instance_async(service_name, group_name, service_instance)
                .await
        }
    }

    /// 业务作用：提供带调用身份的批量注册入口，整批实例共用一次请求和恢复意图。
    ///
    /// 参数说明：
    /// - `service_name`：整批实例所属服务名称。
    /// - `group_name`：请求使用的服务分组，缺省或空串使用默认分组。
    /// - `service_instances`：原样交给批量注册协议的实例集合，不在此拆成独立注册操作。
    ///
    /// 返回：批量响应的成功或失败；不承诺逐项确认，失败时保留整批重试请求。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn batch_register_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instances: Vec<ServiceInstance>,
    ) -> Result<()> {
        self.batch_register_instance_async(service_name, group_name, service_instances)
            .await
    }

    /// 业务作用：提供发现快照读取入口，允许调用方选择持续订阅或单次远端查询。
    ///
    /// 参数说明：
    /// - `service_name`：目标服务名称。
    /// - `group_name`：查询分组，缺省或空串使用默认分组。
    /// - `clusters`：要发送给服务端的集群筛选列表。
    /// - `subscribe`：为真时使用缓存或订阅路径，为假时直接查询服务端。
    ///
    /// 返回：未按健康状态过滤的实例列表；订阅失败或 hosts 缺失给出空列表，直接查询的错误向调用方传播。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn get_all_instances(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
    ) -> Result<Vec<ServiceInstance>> {
        self.get_all_instances_async(service_name, group_name, clusters, subscribe)
            .await
    }

    /// 业务作用：提供按健康标志筛选发现实例的入口，统一排除禁用和非正权重实例。
    ///
    /// 参数说明：
    /// - `service_name`：需要筛选实例的服务名称。
    /// - `group_name`：发现分组，缺省或空串使用默认分组。
    /// - `clusters`：查询候选实例时使用的集群限制。
    /// - `subscribe`：是否使用缓存及订阅结果作为候选来源。
    /// - `healthy`：要求匹配的健康标志，为假也不会包含禁用或非正权重实例。
    ///
    /// 返回：匹配实例或空列表；传播发现路径中未被折叠的查询错误，不执行额外探活。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn select_instances(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
        healthy: bool,
    ) -> Result<Vec<ServiceInstance>> {
        self.select_instances_async(service_name, group_name, clusters, subscribe, healthy)
            .await
    }

    /// 业务作用：为一次业务调用选择一个按发现状态可用的实例，采用权重随机策略。
    ///
    /// 参数说明：
    /// - `service_name`：目标服务名称。
    /// - `group_name`：查询分组，缺省或空串使用默认分组。
    /// - `clusters`：候选实例的集群范围。
    /// - `subscribe`：是否读取订阅缓存，缓存状态可能早于本次调用。
    ///
    /// 返回：一个健康、已启用且权重为正的实例；查询或选择失败返回错误，不保证该地址此刻可以建立业务连接。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn select_one_healthy_instance(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        subscribe: bool,
    ) -> Result<ServiceInstance> {
        self.select_one_healthy_instance_async(service_name, group_name, clusters, subscribe)
            .await
    }

    /// 业务作用：提供带客户端诊断上下文的服务名称分页查询入口。
    ///
    /// 参数说明：
    /// - `page_no`：交给服务端解释的页码，本入口不作范围检查。
    /// - `page_size`：单次请求的页容量，不触发自动翻页。
    /// - `group_name`：目标分组，缺省或空串使用默认分组。
    ///
    /// 返回：服务端名称列表与总数 count，或传输及业务响应错误；不建立订阅或合并缓存。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn get_service_list(
        &self,
        page_no: i32,
        page_size: i32,
        group_name: Option<String>,
    ) -> Result<(Vec<String>, i32)> {
        self.get_service_list_async(page_no, page_size, group_name)
            .await
    }

    /// 业务作用：登记服务变更接收者并保留持续订阅意图，不把首次远端可达性作为本地登记的成功条件。
    ///
    /// 参数说明：
    /// - `service_name`：需要持续观察的服务名称。
    /// - `group_name`：服务分组，缺省或空串使用默认分组。
    /// - `clusters`：远端订阅与刷新使用的集群筛选，不参与监听器服务键。
    /// - `event_listener`：接收后续实例变化的共享回调对象。
    ///
    /// 返回：内部订阅尝试结束后返回 Ok，即使首次请求失败；初始快照及该次错误不会交给调用方，保留的 redo 用于后续重试。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn subscribe(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        event_listener: Arc<dyn NamingEventListener>,
    ) -> Result<()> {
        // 对外成功仅表示本地意图已登记，不能据此宣告远端订阅确认或已经获得实例快照。
        let _ = self
            .subscribe_async(service_name, group_name, clusters, Some(event_listener))
            .await;
        Ok(())
    }

    /// 业务作用：移除指定接收者并取消对应刷新和远端订阅，将远端取消结果交给调用方。
    ///
    /// 参数说明：
    /// - `service_name`：取消观察的服务名称。
    /// - `group_name`：原服务分组，缺省或空串使用默认分组。
    /// - `clusters`：要停止刷新并向远端取消的集群范围。
    /// - `event_listener`：此前登记的回调实例，仅移除身份相同的接收者。
    ///
    /// 返回：远端取消确认且 redo 清理后成功；失败时本地监听和刷新已停止，原 redo 仍可能在后续连接恢复时提交。
    #[instrument(fields(client_id = &self.client_id, group = group_name), skip_all)]
    pub(crate) async fn unsubscribe(
        &self,
        service_name: String,
        group_name: Option<String>,
        clusters: Vec<String>,
        event_listener: Arc<dyn NamingEventListener>,
    ) -> Result<()> {
        self.unsubscribe_async(service_name, group_name, clusters, Some(event_listener))
            .await
    }
}
