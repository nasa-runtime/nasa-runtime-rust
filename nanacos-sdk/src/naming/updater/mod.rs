use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Mutex, time::sleep};
use tracing::{Instrument, debug, error, info, instrument, warn};

use crate::common::{
    cache::Cache,
    executor,
    remote::{
        generate_request_id,
        grpc::{NacosGrpcClient, message::GrpcResponseMessage},
    },
};

use super::{
    dto::ServiceInfo,
    message::{request::ServiceQueryRequest, response::QueryServiceResponse},
    observable::service_info_observable::ServiceInfoEmitter,
};

pub(crate) struct ServiceInfoUpdater {
    /// 查询和推送共用的快照发布入口。
    service_info_emitter: Arc<ServiceInfoEmitter>,
    /// 查询前读取刷新时间的服务目录缓存。
    cache: Arc<Cache<ServiceInfo>>,
    /// 主动查询所用的认证客户端。
    nacos_grpc_client: Arc<NacosGrpcClient>,
    /// 按服务、分组和集群键登记的查询任务；锁串行化创建与移除。
    task_map: Mutex<HashMap<String, ServiceInfoUpdateTask>>,
}

impl ServiceInfoUpdater {
    /// 业务作用：建立服务快照主动查询任务的管理器，作为推送之外的目录更新来源。
    ///
    /// 参数说明：
    /// - `service_info_emitter`：统一校验、缓存和通知的发布器。
    /// - `cache`：主动查询前用于判断是否已有更新的目录缓存。
    /// - `nacos_grpc_client`：查询服务快照的认证客户端。
    ///
    /// 返回：没有活动任务的管理器，不立即查询服务或改变缓存。
    pub(crate) fn new(
        service_info_emitter: Arc<ServiceInfoEmitter>,
        cache: Arc<Cache<ServiceInfo>>,
        nacos_grpc_client: Arc<NacosGrpcClient>,
    ) -> Self {
        Self {
            service_info_emitter,
            cache,
            nacos_grpc_client,
            task_map: Mutex::new(HashMap::default()),
        }
    }

    /// 业务作用：为服务、分组和集群范围创建唯一的本地周期查询任务。
    ///
    /// 参数说明：
    /// - `namespace`：远端查询使用的命名空间，不参与当前管理器的任务键。
    /// - `service_name`：需要更新的服务名称。
    /// - `group_name`：服务分组。
    /// - `cluster`：查询范围的集群文本。
    ///
    /// 返回：任务键不存在时启动并登记，存在时保持原任务；同一管理器应保持命名空间一致，不等待首次查询成功。
    #[instrument(fields(group_name = group_name, cluster = cluster), skip_all)]
    pub(crate) async fn schedule_update(
        &self,
        namespace: String,
        service_name: String,
        group_name: String,
        cluster: String,
    ) {
        let task_key = ServiceInfoUpdater::update_task_key(&service_name, &group_name, &cluster);
        let mut lock = self.task_map.lock().await;

        let is_exist = lock.contains_key(&task_key);
        if !is_exist {
            let update_task = ServiceInfoUpdateTask::new(
                service_name,
                namespace,
                group_name,
                cluster,
                self.cache.clone(),
                self.nacos_grpc_client.clone(),
                self.service_info_emitter.clone(),
            );
            update_task.start();

            let _ = lock.insert(task_key, update_task);
        }
    }

    /// 业务作用：移除指定服务范围的主动查询任务，并请求其停止后续循环。
    ///
    /// 参数说明：
    /// - `service_name`：任务所属服务名称。
    /// - `group_name`：服务分组。
    /// - `cluster`：创建任务时使用的集群文本。
    ///
    /// 返回：存在时移除并关闭运行标记，不存在时无变化；不等待在途查询结束，也不撤销已经开始的快照发布。
    #[instrument(fields(group_name = group_name, cluster = cluster), skip_all)]
    pub(crate) async fn stop_update(
        &self,
        service_name: String,
        group_name: String,
        cluster: String,
    ) {
        let task_key = ServiceInfoUpdater::update_task_key(&service_name, &group_name, &cluster);
        let mut lock = self.task_map.lock().await;
        let ret = lock.remove(&task_key);
        if let Some(task) = ret {
            task.stop();
        }
    }

    /// 业务作用：为主动查询任务生成与其服务范围对应的登记和撤销键。
    ///
    /// 参数说明：
    /// - `service_name`：服务名称，可能已经包含分组前缀。
    /// - `group_name`：需要补充的服务分组。
    /// - `cluster`：集群筛选文本，非空时参与键生成。
    ///
    /// 返回：沿用 ServiceInfo 键规则的文本，不包含命名空间或执行状态。
    fn update_task_key(service_name: &str, group_name: &str, cluster: &str) -> String {
        let group_service_name = ServiceInfo::get_grouped_service_name(service_name, group_name);
        ServiceInfo::get_key(&group_service_name, cluster)
    }
}

struct ServiceInfoUpdateTask {
    /// 允许后台任务继续循环的标记，不是立即取消令牌。
    running: Arc<AtomicBool>,
    /// 查询服务名称。
    service_name: String,
    /// 远端查询使用的命名空间。
    namespace: String,
    /// 查询服务所属分组。
    group_name: String,
    /// 集群筛选文本，参与任务键及查询。
    cluster: String,
    /// 用于判断是否已有较新快照的缓存。
    cache: Arc<Cache<ServiceInfo>>,
    /// 共享请求发送与认证入口。
    nacos_grpc_client: Arc<NacosGrpcClient>,
    /// 查询成功后处理缓存和监听通知的发布器。
    service_info_emitter: Arc<ServiceInfoEmitter>,
}

impl ServiceInfoUpdateTask {
    const DEFAULT_DELAY: u64 = 1000;
    const DEFAULT_UPDATE_CACHE_TIME_MULTIPLE: u8 = 6;
    const MAX_FAILED: u8 = 6;

    /// 业务作用：冻结一次服务范围的周期查询依赖，准备由管理器启动任务。
    ///
    /// 参数说明：
    /// - `service_name`：查询服务名称。
    /// - `namespace`：远端查询的命名空间。
    /// - `group_name`：查询服务分组。
    /// - `cluster`：查询集群范围文本。
    /// - `cache`：用于读取已有刷新时间的本地目录。
    /// - `nacos_grpc_client`：负责发送查询的共享客户端。
    /// - `service_info_emitter`：查询成功后接收快照的发布器。
    ///
    /// 返回：运行标记为假的任务，不启动网络或缓存操作。
    fn new(
        service_name: String,
        namespace: String,
        group_name: String,
        cluster: String,
        cache: Arc<Cache<ServiceInfo>>,
        nacos_grpc_client: Arc<NacosGrpcClient>,
        service_info_emitter: Arc<ServiceInfoEmitter>,
    ) -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            service_name,
            namespace,
            group_name,
            cluster,
            cache,
            nacos_grpc_client,
            service_info_emitter,
        }
    }

    /// 业务作用：启动周期服务查询，在缓存没有更新证据时向远端获取快照并统一发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：安排后台任务后立即结束；已标记运行时不重复安排。初始等待一秒，失败指数退避最多六十秒，成功按服务端缓存时长调整；运行标记的检查和设置不是并发互斥，管理器须串行启动。
    fn start(&self) {
        let running = self.running.clone();
        if self.running.load(Ordering::Acquire) {
            // 管理器在目录锁内调用此入口，已有运行任务不再次安排；此检查本身不构成 CAS 抢占。
            return;
        }
        self.running.store(true, Ordering::Release);

        let cluster = self.cluster.clone();
        let group_name = self.group_name.clone();
        let namespace = self.namespace.clone();
        let service_name = self.service_name.clone();

        let cache = self.cache.clone();
        let grpc_client = self.nacos_grpc_client.clone();
        let service_info_emitter = self.service_info_emitter.clone();

        executor::spawn(async move {
            let mut delay_time = ServiceInfoUpdateTask::DEFAULT_DELAY;

            let mut last_refresh_time = u64::MAX;

            let mut failed_count = 0;

            let request = ServiceQueryRequest {
                cluster,
                group_name: Some(group_name),
                healthy_only: false,
                udp_port: 0,
                namespace: Some(namespace),
                service_name: Some(service_name),
                ..Default::default()
            };

            let log_tag = format!(
                "{}:{}:{}:{}",
                request.namespace.as_deref().unwrap_or_default(),
                request.group_name.as_deref().unwrap_or_default(),
                request.service_name.as_deref().unwrap_or_default(),
                request.cluster
            );

            info!("{log_tag}:ServiceInfoUpdateTask started");

            while running.load(Ordering::Acquire) {
                let delay_time_millis = Duration::from_millis(
                    (delay_time << failed_count).min(ServiceInfoUpdateTask::DEFAULT_DELAY * 60),
                );
                debug!("{log_tag}:ServiceInfoUpdateTask delay sleep {delay_time_millis:?}");
                sleep(delay_time_millis).await;

                if !running.load(Ordering::Acquire) {
                    // 停止可能发生在休眠期间，发起下一次查询前必须再次确认任务仍被允许运行。
                    warn!("{log_tag}:ServiceInfoUpdateTask has been already stopped!");
                    break;
                }

                debug!("{log_tag}:ServiceInfoUpdateTask refreshing");

                let service_info = {
                    let group_name = request.group_name.as_deref().unwrap_or_default();
                    let service_name = request.service_name.as_deref().unwrap_or_default();
                    let cluster = &request.cluster;

                    let grouped_name =
                        ServiceInfo::get_grouped_service_name(service_name, group_name);
                    let key = ServiceInfo::get_key(&grouped_name, cluster);
                    cache.get(&key).map(|data| data.clone())
                };

                let mut need_query_service_info = true;

                if let Some(service_info) = service_info {
                    let is_outdate = last_refresh_time >= (service_info.last_ref_time as u64);
                    if is_outdate {
                        need_query_service_info = true;
                    } else {
                        need_query_service_info = false;
                        last_refresh_time = service_info.last_ref_time as u64;
                    }
                }

                if !need_query_service_info {
                    // 缓存已经包含更晚刷新时间，本轮不重复请求；下一轮仍根据自身进度重新判断。
                    warn!("{log_tag}:ServiceInfoUpdateTask don't need to refresh service info");
                    continue;
                }

                let request = ServiceQueryRequest {
                    request_id: Some(generate_request_id()),
                    ..request.clone()
                };

                let ret = grpc_client
                    .send_request::<ServiceQueryRequest, QueryServiceResponse>(request)
                    .in_current_span()
                    .await;
                let Ok(response) = ret else {
                    // 网络或解码失败不发布部分目录，保留旧缓存并增加后续等待。
                    error!("{log_tag}:ServiceInfoUpdateTask occur an error: {ret:?}");
                    if failed_count < ServiceInfoUpdateTask::MAX_FAILED {
                        failed_count += 1;
                    }
                    continue;
                };
                debug!("{log_tag}:ServiceInfoUpdateTask query service info response: {response:?}");
                if !response.is_success() {
                    // 可解码的业务拒绝不能当作实例目录，继续退避而不是覆盖缓存。
                    let result_code = response.result_code;
                    let error_code = response.error_code;
                    let ret_message = response.message.unwrap_or_default();
                    error!("{log_tag}:ServiceInfoUpdateTask query services failed: resultCode: {result_code}, errorCode:{error_code}, message:{ret_message}");
                    if failed_count < ServiceInfoUpdateTask::MAX_FAILED {
                        failed_count += 1;
                    }
                    continue;
                }
                let service_info = response.service_info;
                last_refresh_time = service_info.last_ref_time as u64;
                delay_time = (service_info.cache_millis
                    * (ServiceInfoUpdateTask::DEFAULT_UPDATE_CACHE_TIME_MULTIPLE as i64))
                    as u64;

                service_info_emitter.emit(service_info).in_current_span().await;

                failed_count = 0;
                debug!("{log_tag}:ServiceInfoUpdateTask finish");
            }

            warn!("{log_tag}:ServiceInfoUpdateTask is stopped");
        }.in_current_span());
    }

    /// 业务作用：撤销主动查询任务继续下一轮的资格，供管理器移除任务时使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：运行标记置为假，不取消已经发出的请求、异步发布或当前休眠；任务在后续检查点退出。
    fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }
}
