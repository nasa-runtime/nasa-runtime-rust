use crate::api::config::ConfigResponse;
use crate::api::plugin::{AuthPlugin, ConfigFilter, ConfigReq, ConfigResp};
use crate::api::props::ClientProps;
use crate::common::cache::{Cache, CacheBuilder};
use crate::common::remote::grpc::message::GrpcResponseMessage;
use crate::common::remote::grpc::{NacosGrpcClient, NacosGrpcClientBuilder};
use crate::common::remote::server_list::create_server_list_provider;
use crate::config::cache::CacheData;
use crate::config::handler::ConfigChangeNotifyHandler;
use crate::config::message::request::*;
use crate::config::message::response::*;
use crate::config::util;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{Instrument, instrument};

#[derive(Clone)]
pub(crate) struct ConfigWorker {
    /// 配置目录、作用域、缓存与启动策略的客户端参数。
    pub(crate) client_props: ClientProps,
    /// 查询、发布、删除及监听请求共用的认证连接入口。
    remote_client: Arc<NacosGrpcClient>,
    /// 配置原始内容和运行期监听状态的缓存。
    unified_cache: Arc<Cache<CacheData>>,
    /// 顺序应用于请求与响应的共享过滤器列表。
    config_filters: Arc<Vec<Box<dyn ConfigFilter>>>,
}

impl ConfigWorker {
    /// 业务作用：装配配置缓存、认证连接和变化刷新任务，使查询、发布与监听共用同一客户端。
    ///
    /// 参数说明：
    /// - `client_props`：目录、命名空间、缓存路径和启动策略等配置。
    /// - `auth_plugin`：登录及请求身份生成插件。
    /// - `config_filters`：按提供顺序执行的请求与响应过滤器。
    /// - `client_id`：连接和认证任务使用的实例诊断标识。
    ///
    /// 返回：目录及连接装配成功后返回工作器；相关失败向上传递，应急策略只放宽首次连接探测。成功后启动变化消费和周期监听任务，不等待所有配置同步完成。
    pub(crate) async fn new(
        client_props: ClientProps,
        auth_plugin: Arc<dyn AuthPlugin>,
        config_filters: Vec<Box<dyn ConfigFilter>>,
        client_id: String,
    ) -> crate::api::error::Result<Self> {
        let cache_ns = client_props.get_namespace_default_if_empty();
        // 缓存作用域先按缺省命名空间归一，持久化内容不包含运行时监听器与过滤器。
        let unified_cache: Cache<CacheData> = CacheBuilder::config(cache_ns)
            .load_cache_at_start(client_props.get_config_load_cache_at_start())
            .disk_store(client_props.get_cache_dir())
            .build()
            .await;
        let unified_cache = Arc::new(unified_cache);
        let config_filters = Arc::new(config_filters);

        // 变化队列只传配置身份，正文通过后续查询取得，避免将推送当作可信内容。
        let (notify_change_tx, notify_change_rx) = tokio::sync::mpsc::channel(16);
        let notify_change_tx_clone = notify_change_tx.clone();

        let server_list_provider = create_server_list_provider(&client_props).await?;

        let remote_client = NacosGrpcClientBuilder::new(server_list_provider)
            .port(client_props.get_remote_grpc_port())
            .namespace(client_props.get_namespace())
            .app_name(client_props.get_app_name())
            .client_version(client_props.get_client_version())
            .support_remote_connection(true)
            .support_config_remote_metrics(true)
            .support_naming_delta_push(false)
            .support_naming_remote_metric(false)
            .add_label(
                crate::api::constants::common_remote::LABEL_SOURCE.to_owned(),
                crate::api::constants::common_remote::LABEL_SOURCE_SDK.to_owned(),
            )
            .add_label(
                crate::api::constants::common_remote::LABEL_MODULE.to_owned(),
                crate::api::constants::common_remote::LABEL_MODULE_CONFIG.to_owned(),
            )
            .add_labels(client_props.get_labels())
            .register_server_request_handler::<ConfigChangeNotifyRequest>(Arc::new(
                ConfigChangeNotifyHandler {
                    notify_change_tx,
                },
            ))
            .auth_plugin(auth_plugin)
            .auth_context(client_props.get_auth_context())
            .max_retries(client_props.get_max_retries())
            .emergency_start(client_props.get_config_load_cache_at_start())
            .build(client_id)
            .await?;

        let remote_client = Arc::new(remote_client);
        // 客户端装配成功后启动变化消费，再启动周期摘要比较，统一汇入同一刷新队列。
        crate::common::executor::spawn(Self::notify_change_to_cache_data(
            Arc::clone(&remote_client),
            Arc::clone(&unified_cache),
            notify_change_rx,
        ));

        crate::common::executor::spawn(Self::list_ensure_cache_data_newest(
            Arc::clone(&remote_client),
            Arc::clone(&unified_cache),
            notify_change_tx_clone,
        ));

        Ok(Self {
            client_props,
            remote_client,
            unified_cache,
            config_filters,
        })
    }
}

impl ConfigWorker {
    /// 业务作用：优先读取具有正文和摘要的缓存快照，否则从服务端查询，并执行响应过滤链。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识，与当前命名空间及分组共同定位缓存和远端配置。
    /// - `group`：配置分组，调用方负责缺省值归一。
    ///
    /// 返回：经过过滤的配置响应；远端查询失败返回错误。缓存命中不重新验证新鲜度，远端成功响应缺少正文、类型或摘要时可能 panic，直接查询不填充缓存。
    #[instrument(skip_all)]
    pub(crate) async fn get_config(
        &self,
        data_id: String,
        group: String,
    ) -> crate::api::error::Result<ConfigResponse> {
        let namespace = self.client_props.get_namespace();
        let group_key = util::group_key(&data_id, &group, &namespace);

        // 先复制独立快照并释放缓存守卫，再等待可能访问外部资源的响应过滤器。
        let cached_snapshot = self.unified_cache.get(&group_key).and_then(|r| {
            if !r.content.is_empty() && !r.md5.is_empty() {
                Some(r.resp_snapshot())
            } else {
                None
            }
        });
        if let Some(snapshot) = cached_snapshot {
            tracing::info!("get_config from cache, group_key={}", group_key);
            return Ok(CacheData::filtered_response(snapshot).await);
        }

        // 正文或摘要为空时不能采用当前缓存路径，转为查询；合法空正文也会走此分支。
        let config_resp = Self::get_config_inner_async(
            self.remote_client.clone(),
            data_id.clone(),
            group.clone(),
            namespace.clone(),
        )
        .await?;

        // 远端正文按注册顺序过滤，但类型和摘要仍沿用服务端原值。
        let mut conf_resp = ConfigResp::new(
            data_id,
            group,
            namespace,
            config_resp
                .content
                .expect("Config content should exist in response"),
            config_resp.encrypted_data_key.unwrap_or_default(),
        );
        for config_filter in self.config_filters.iter() {
            config_filter.filter(None, Some(&mut conf_resp)).await;
        }

        Ok(ConfigResponse::new(
            conf_resp.data_id,
            conf_resp.group,
            conf_resp.namespace,
            conf_resp.content,
            config_resp
                .content_type
                .expect("Config content_type should exist in response"),
            config_resp
                .md5
                .expect("Config md5 should exist in response"),
        ))
    }

    /// 业务作用：经请求过滤链发布配置正文，不附加独立 CAS 或灰度条件。
    ///
    /// 参数说明：
    /// - `data_id`：待发布配置标识。
    /// - `group`：配置分组。
    /// - `content`：待过滤及提交的正文。
    /// - `content_type`：可选正文格式，省略时由服务端按其规则处理。
    ///
    /// 返回：远端业务结果成功时为真，否则返回传输或业务错误；不直接更新本地缓存或等待监听回调。
    #[instrument(skip_all)]
    pub(crate) async fn publish_config(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
    ) -> crate::api::error::Result<bool> {
        self.publish_config_inner(data_id, group, content, content_type, None, None, None)
            .await
    }

    /// 业务作用：以旧内容摘要为条件发布配置，交由服务端裁决并发修改冲突。
    ///
    /// 参数说明：
    /// - `data_id`：待发布配置标识。
    /// - `group`：配置分组。
    /// - `content`：待过滤和发布的新正文。
    /// - `content_type`：可选正文格式。
    /// - `cas_md5`：服务端必须匹配的旧内容摘要，过滤后仍原样提交。
    ///
    /// 返回：远端接受条件写入时为真，传输失败或业务拒绝返回错误，不以本地缓存替代条件校验。
    #[instrument(skip_all)]
    pub(crate) async fn publish_config_cas(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: String,
    ) -> crate::api::error::Result<bool> {
        self.publish_config_inner(
            data_id,
            group,
            content,
            content_type,
            Some(cas_md5),
            None,
            None,
        )
        .await
    }

    /// 业务作用：向指定灰度接收范围发布配置，正文仍经过统一请求过滤链。
    ///
    /// 参数说明：
    /// - `data_id`：待发布配置标识。
    /// - `group`：配置分组。
    /// - `content`：待发布正文。
    /// - `content_type`：可选正文格式。
    /// - `beta_ips`：服务端识别的灰度地址文本，此处不解析或验证地址集合。
    ///
    /// 返回：远端业务成功时为真，否则返回错误；不在本地执行灰度筛选或更新订阅集合。
    #[instrument(skip_all)]
    pub(crate) async fn publish_config_beta(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        beta_ips: String,
    ) -> crate::api::error::Result<bool> {
        self.publish_config_inner(
            data_id,
            group,
            content,
            content_type,
            None,
            Some(beta_ips),
            None,
        )
        .await
    }

    /// 业务作用：携带自定义扩展参数发布配置，同时保留专用参数的优先级。
    ///
    /// 参数说明：
    /// - `data_id`：待发布配置标识。
    /// - `group`：配置分组。
    /// - `content`：待过滤的正文。
    /// - `content_type`：显式正文格式，存在时覆盖扩展参数同名值。
    /// - `cas_md5`：独立 CAS 条件，省略时不设置该条件。
    /// - `params`：自定义扩展键值；加密密钥字段最终由过滤结果覆盖。
    ///
    /// 返回：远端业务成功时为真，传输或业务失败返回错误，不保证扩展参数均被服务端支持。
    #[instrument(skip_all)]
    pub(crate) async fn publish_config_param(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: Option<String>,
        params: HashMap<String, String>,
    ) -> crate::api::error::Result<bool> {
        self.publish_config_inner(
            data_id,
            group,
            content,
            content_type,
            cas_md5,
            None,
            Some(params),
        )
        .await
    }

    /// 业务作用：汇总发布路径，先顺序执行请求过滤器，再提交过滤后的配置身份及正文。
    ///
    /// 参数说明：
    /// - `data_id`：初始配置标识，过滤器可调整。
    /// - `group`：初始配置分组，过滤器可调整。
    /// - `content`：初始正文，允许过滤器加密或转换。
    /// - `content_type`：独立正文格式设置，不由请求过滤对象修改。
    /// - `cas_md5`：独立旧内容摘要条件，原样交给发送层。
    /// - `beta_ips`：可选灰度地址文本。
    /// - `params`：可选扩展参数集合。
    ///
    /// 返回：远端发布结果；过滤器没有独立错误返回通道，异常或等待由其实现决定。发送失败不回滚过滤器副作用。
    #[allow(clippy::too_many_arguments)]
    async fn publish_config_inner(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: Option<String>,
        beta_ips: Option<String>,
        params: Option<HashMap<String, String>>,
    ) -> crate::api::error::Result<bool> {
        let namespace = self.client_props.get_namespace();

        let mut conf_req = ConfigReq::new(data_id, group, namespace, content, "".to_string());
        for config_filter in self.config_filters.iter() {
            config_filter.filter(Some(&mut conf_req), None).await;
        }

        Self::publish_config_inner_async(
            self.remote_client.clone(),
            conf_req.data_id,
            conf_req.group,
            conf_req.namespace,
            conf_req.content,
            content_type,
            conf_req.encrypted_data_key,
            cas_md5,
            beta_ips,
            params,
        )
        .await
    }

    /// 业务作用：在当前命名空间内请求删除指定配置。
    ///
    /// 参数说明：
    /// - `data_id`：待删除配置标识。
    /// - `group`：配置分组。
    ///
    /// 返回：远端业务成功时为真，失败返回错误；此入口不执行过滤器、不移除本地缓存或监听器。
    #[instrument(skip_all)]
    pub(crate) async fn remove_config(
        &self,
        data_id: String,
        group: String,
    ) -> crate::api::error::Result<bool> {
        let namespace = self.client_props.get_namespace();
        Self::remove_config_inner_async(self.remote_client.clone(), data_id, group, namespace).await
    }

    /// 业务作用：为配置建立本地缓存和变更接收者，首次发现配置时尝试读取并异步建立远端监听。
    ///
    /// 参数说明：
    /// - `data_id`：待监听配置标识。
    /// - `group`：配置分组。
    /// - `listener`：按 Arc 实例身份去重的本地接收者。
    ///
    /// 返回：尝试加入本地监听后结束；首次查询失败仅记录，异步订阅不等待结果。首次填充不立即通知，已有缓存直接加监听，远端就绪与持久化完成不由此保证。
    #[instrument(skip_all)]
    pub(crate) async fn add_listener(
        &self,
        data_id: String,
        group: String,
        listener: Arc<dyn crate::api::config::ConfigChangeListener>,
    ) {
        let namespace = self.client_props.get_namespace();
        let group_key = util::group_key(&data_id, &group, &namespace);

        if !self.unified_cache.contains_key(&group_key) {
            let mut cache_data =
                CacheData::new(self.config_filters.clone(), data_id, group, namespace);

            // 先尝试建立内容摘要基线；查询失败仍保留待监听项，允许后续周期比较获取内容。
            let config_resp = Self::get_config_inner_async(
                self.remote_client.clone(),
                cache_data.data_id.clone(),
                cache_data.group.clone(),
                cache_data.namespace.clone(),
            )
            .in_current_span()
            .await;
            match config_resp {
                Ok(config_resp) => {
                    Self::fill_data_and_notify(&mut cache_data, config_resp).await;
                }
                Err(e) => {
                    tracing::error!("get_config_inner_async, config_resp err={e:?}");
                }
            }
            let req = ConfigBatchListenRequest::new(true).add_config_listen_context(
                ConfigListenContext::new(
                    cache_data.data_id.clone(),
                    cache_data.group.clone(),
                    cache_data.namespace.clone(),
                    cache_data.md5.clone(),
                ),
            );
            let remote_client_clone = self.remote_client.clone();
            crate::common::executor::spawn(
                async move {
                    let _ = remote_client_clone
                        .send_request::<ConfigBatchListenRequest, ConfigChangeBatchListenResponse>(
                            req,
                        )
                        .await;
                }
                .in_current_span(),
            );
            // 先发布本地缓存并交给缓存组件安排持久化，不等待磁盘完成再添加接收者。
            self.unified_cache.insert(group_key.clone(), cache_data);
        }
        // 接收者按实例身份加入当前缓存；并发初始化不由本方法提供整体互斥。
        if let Some(mut cache_ref_mut) = self.unified_cache.get_mut(&group_key) {
            cache_ref_mut.add_listener(listener);
        }
    }

    /// 业务作用：从指定配置的本地通知集合中移除一个接收者。
    ///
    /// 参数说明：
    /// - `data_id`：监听配置标识。
    /// - `group`：配置分组。
    /// - `listener`：原注册的 Arc 实例，只有实例身份匹配才移除。
    ///
    /// 返回：缓存和接收者存在时移除，否则保持原状；不取消远端订阅、删除缓存或中断已安排的通知任务。
    #[instrument(skip_all)]
    pub(crate) async fn remove_listener(
        &self,
        data_id: String,
        group: String,
        listener: Arc<dyn crate::api::config::ConfigChangeListener>,
    ) {
        let namespace = self.client_props.get_namespace();
        let group_key = util::group_key(&data_id, &group, &namespace);

        if let Some(mut cache_ref_mut) = self.unified_cache.get_mut(&group_key) {
            cache_ref_mut.remove_listener(listener);
        }
    }
}

impl ConfigWorker {
    /// 业务作用：周期提交全部缓存配置的摘要，补充推送之外的变化检测并安排重新查询。
    ///
    /// 参数说明：
    /// - `remote_client`：发送批量监听请求的共享客户端。
    /// - `unified_cache`：提供本轮配置身份及摘要快照的缓存。
    /// - `notify_change_tx`：变化身份的有界刷新队列，满时等待容量。
    ///
    /// 返回：持续循环，每轮完成后等待六十秒；只有成功业务响应中的变化列表进入队列。无监听器的缓存仍参与比较，当前方法没有独立停止信号。
    #[instrument(skip_all)]
    async fn list_ensure_cache_data_newest(
        remote_client: Arc<NacosGrpcClient>,
        unified_cache: Arc<Cache<CacheData>>,
        notify_change_tx: tokio::sync::mpsc::Sender<String>,
    ) {
        tracing::info!("list_ensure_cache_data_newest started");
        loop {
            tracing::debug!("list_ensure_cache_data_newest refreshing");
            // 目录以缓存为准，本地接收者为空不会在此自动取消远端监听或删除配置。
            let mut listen_context_vec = Vec::with_capacity(6);
            {
                // 只在同步遍历中复制身份和摘要，网络请求不持有缓存分片守卫。
                unified_cache.for_each(|_key, c| {
                    listen_context_vec.push(ConfigListenContext::new(
                        c.data_id.clone(),
                        c.group.clone(),
                        c.namespace.clone(),
                        c.md5.clone(),
                    ));
                });
            }
            if !listen_context_vec.is_empty() {
                tracing::debug!("list_ensure_cache_data_newest context={listen_context_vec:?}");
                let req =
                    ConfigBatchListenRequest::new(true).config_listen_context(listen_context_vec);

                let resp = remote_client
                    .send_request::<ConfigBatchListenRequest, ConfigChangeBatchListenResponse>(req)
                    .in_current_span()
                    .await;

                if let Ok(resp) = resp
                    && resp.is_success()
                    && let Some(change_context_vec) = resp.changed_configs
                {
                    for context in change_context_vec {
                        // 摘要比较仅表明需要重新读取，不把响应中的身份当作配置正文。
                        let group_key =
                            util::group_key(&context.data_id, &context.group, &context.namespace);
                        let _ = notify_change_tx.send(group_key).await;
                    }
                }
            }
            tracing::debug!("list_ensure_cache_data_newest finish");

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    /// 业务作用：同步覆盖缓存中的服务端内容及元数据，并区分首次基线建立和后续变化。
    ///
    /// 参数说明：
    /// - `cache_data`：待更新缓存，此函数不跨异步等待持有它。
    /// - `config_resp`：成功读取的协议响应，正文、类型和摘要必须存在。
    ///
    /// 返回：首次填充清除 initializing 并返回假，后续填充返回真供通知流程判断摘要；必需字段缺失会 panic，之前已赋值字段不会回滚。
    fn fill_data(cache_data: &mut CacheData, config_resp: ConfigQueryResponse) -> bool {
        cache_data.content_type = config_resp
            .content_type
            .expect("Config content_type missing in response");
        cache_data.content = config_resp
            .content
            .expect("Config content missing in response");
        cache_data.md5 = config_resp.md5.expect("Config md5 missing in response");
        // 对端省略加密密钥字段时使用空串，保持未加密配置的读取合同。
        cache_data.encrypted_data_key = config_resp.encrypted_data_key.unwrap_or_default();
        cache_data.last_modified = config_resp.last_modified;
        // 配置正文与加密密钥可能包含秘密，只记录规模和状态，不展开缓存展示文本。
        tracing::info!(
            content_bytes = cache_data.content.len(),
            encrypted = !cache_data.encrypted_data_key.is_empty(),
            initializing = cache_data.initializing,
            "Nacos 配置缓存内容已填充"
        );
        if cache_data.initializing {
            cache_data.initializing = false;
            false
        } else {
            true
        }
    }

    /// 业务作用：填充独立缓存对象，首次基线之后才进入过滤和监听通知流程。
    ///
    /// 参数说明：
    /// - `cache_data`：调用方独占的缓存对象，不能是跨等待持有的缓存分片守卫。
    /// - `config_resp`：必须包含正文、类型和摘要的服务端响应。
    ///
    /// 返回：填充并按需安排通知后结束，不等待接收者回调；缺少必需字段会 panic，过滤器行为由其实现决定。
    async fn fill_data_and_notify(cache_data: &mut CacheData, config_resp: ConfigQueryResponse) {
        if Self::fill_data(cache_data, config_resp) {
            // 首次基线不触发变更通知，后续通知再按各接收者已安排的摘要去重。
            cache_data.notify_listener().await;
        }
    }

    /// 业务作用：串行消费配置变化身份，查询最新正文并在释放缓存守卫后执行过滤与通知。
    ///
    /// 参数说明：
    /// - `remote_client`：远端配置读取入口。
    /// - `unified_cache`：需要更新的缓存，身份不存在时忽略该通知。
    /// - `notify_change_rx`：来自服务端推送和周期比较的变化队列。
    ///
    /// 返回：所有发送端关闭后退出；查询失败保留旧缓存。成功查询先更新缓存，再复制响应快照供异步过滤，最终通知不等待业务回调完成。
    #[instrument(skip_all)]
    async fn notify_change_to_cache_data(
        remote_client: Arc<NacosGrpcClient>,
        unified_cache: Arc<Cache<CacheData>>,
        mut notify_change_rx: tokio::sync::mpsc::Receiver<String>,
    ) {
        loop {
            match notify_change_rx.recv().await {
                None => {
                    tracing::warn!(
                        "notify_change_to_cache_data break, notify_change_rx be dropped(shutdown).",
                    );
                    break;
                }
                Some(group_key) => {
                    // 查询和过滤可能等待外部动作，先复制身份并释放分片守卫，防止持锁等待阻塞缓存访问。
                    let snapshot = unified_cache
                        .get(&group_key)
                        .map(|r| (r.data_id.clone(), r.group.clone(), r.namespace.clone()));
                    let Some((data_id, group, namespace)) = snapshot else {
                        // 配置已不在本地目录时不查询任意远端身份，等待后续明确的监听注册。
                        continue;
                    };

                    let config_resp = Self::get_config_inner_async(
                        remote_client.clone(),
                        data_id,
                        group,
                        namespace,
                    )
                    .in_current_span()
                    .await;

                    match config_resp {
                        Ok(config_resp) => {
                            let should_notify = match unified_cache.get_mut(&group_key) {
                                Some(mut cache_ref_mut) => {
                                    Self::fill_data(&mut cache_ref_mut, config_resp)
                                }
                                None => false,
                            };
                            if should_notify {
                                // 缓存写入先结束，再取独立快照过滤；异步边界不携带分片守卫。
                                let snapshot =
                                    unified_cache.get(&group_key).map(|r| r.resp_snapshot());
                                if let Some(snapshot) = snapshot {
                                    let resp = CacheData::filtered_response(snapshot).await;
                                    if let Some(cache_ref) = unified_cache.get(&group_key) {
                                        cache_ref.dispatch_notify(resp);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // 查询失败不能用不完整结果覆盖旧内容，保留缓存等待后续变化或周期重查。
                            tracing::error!("get_config_inner_async, config_resp err={e:?}");
                        }
                    }
                }
            }
        }
    }
}

impl ConfigWorker {
    /// 业务作用：读取远端配置并把协议失败归类为不存在、查询冲突或其它业务错误。
    ///
    /// 参数说明：
    /// - `remote_client`：已经装配认证与连接策略的客户端。
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    /// - `namespace`：配置所属命名空间。
    ///
    /// 返回：业务结果码成功时移交原响应，不检查可选正文是否齐全；传输、解码及业务拒绝返回对应错误，不过滤内容或更新缓存。
    async fn get_config_inner_async(
        remote_client: Arc<NacosGrpcClient>,
        data_id: String,
        group: String,
        namespace: String,
    ) -> crate::api::error::Result<ConfigQueryResponse> {
        let req = ConfigQueryRequest::new(data_id, group, namespace);
        let resp = remote_client
            .send_request::<ConfigQueryRequest, ConfigQueryResponse>(req)
            .await?;

        if resp.is_success() {
            return Ok(resp);
        }

        let err_str = crate::common::error::to_err_msg(&resp, "get_config");
        if resp.is_not_found() {
            Err(crate::api::error::Error::ConfigNotFound(err_str))
        } else if resp.is_query_conflict() {
            Err(crate::api::error::Error::ConfigQueryConflict(err_str))
        } else {
            Err(crate::api::error::Error::ErrResult(err_str))
        }
    }

    /// 业务作用：组装最终发布参数并提交配置，以远端业务结果判断发布是否被接受。
    ///
    /// 参数说明：
    /// - `remote_client`：负责身份和传输的共享客户端。
    /// - `data_id`：经过请求过滤后的配置标识。
    /// - `group`：经过请求过滤后的分组。
    /// - `namespace`：经过请求过滤后的命名空间。
    /// - `content`：经过请求过滤后的正文。
    /// - `content_type`：显式正文格式，存在时覆盖扩展字段同名值。
    /// - `encrypted_data_key`：过滤结果中的密钥材料，始终覆盖扩展字段同名值。
    /// - `cas_md5`：可选条件写入摘要，独立于扩展参数。
    /// - `beta_ips`：显式灰度范围，存在时覆盖扩展字段同名值。
    /// - `params`：优先级较低的自定义扩展参数。
    ///
    /// 返回：远端业务成功时为真，传输、解码或业务拒绝返回错误；不等待配置传播或更新本地快照。
    #[allow(clippy::too_many_arguments)]
    async fn publish_config_inner_async(
        remote_client: Arc<NacosGrpcClient>,
        data_id: String,
        group: String,
        namespace: String,
        content: String,
        content_type: Option<String>,
        encrypted_data_key: String,
        cas_md5: Option<String>,
        beta_ips: Option<String>,
        params: Option<HashMap<String, String>>,
    ) -> crate::api::error::Result<bool> {
        let mut req =
            ConfigPublishRequest::new(data_id, group, namespace, content).cas_md5(cas_md5);

        // 先写扩展参数，再覆盖专用参数，防止自定义同名字段替换过滤器确定的加密材料。
        if let Some(params) = params {
            req.add_addition_params(params);
        }
        if let Some(content_type) = content_type {
            req.add_addition_param(
                crate::api::config::constants::KEY_PARAM_CONTENT_TYPE,
                content_type,
            );
        }
        if let Some(beta_ips) = beta_ips {
            req.add_addition_param(crate::api::config::constants::KEY_PARAM_BETA_IPS, beta_ips);
        }
        req.add_addition_param(
            crate::api::config::constants::KEY_PARAM_ENCRYPTED_DATA_KEY,
            encrypted_data_key,
        );
        let resp = remote_client
            .send_request::<ConfigPublishRequest, ConfigPublishResponse>(req)
            .await?;

        crate::common::error::handle_response(&resp, "publish_config").map(|_| true)
    }

    /// 业务作用：发送配置删除请求并检查远端业务结果，避免把传输成功当作删除成功。
    ///
    /// 参数说明：
    /// - `remote_client`：负责认证与发送的共享客户端。
    /// - `data_id`：待删除配置标识。
    /// - `group`：配置分组。
    /// - `namespace`：配置所属命名空间。
    ///
    /// 返回：业务成功时为真，传输、解码或业务失败返回错误；不删除本地缓存或取消监听。
    async fn remove_config_inner_async(
        remote_client: Arc<NacosGrpcClient>,
        data_id: String,
        group: String,
        namespace: String,
    ) -> crate::api::error::Result<bool> {
        let req = ConfigRemoveRequest::new(data_id, group, namespace);
        let resp = remote_client
            .send_request::<ConfigRemoveRequest, ConfigRemoveResponse>(req)
            .await?;

        crate::common::error::handle_response(&resp, "remove_config").map(|_| true)
    }
}
