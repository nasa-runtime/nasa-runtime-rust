use std::{collections::HashMap, sync::Arc};

use tokio::sync::{
    RwLock,
    mpsc::{Receiver, Sender, channel},
};
use tracing::{Instrument, Span, debug_span, error, info, instrument, warn};

use crate::{
    api::naming::{NamingChangeEvent, NamingEventListener},
    common::{
        cache::{Cache, CacheRef},
        executor,
    },
    naming::dto::ServiceInfo,
};

use super::service_info_diff::{ServiceInfoDiff, is_outdated_data};

/// 业务作用：建立实例快照发布与监听分派通道，使服务端推送和主动查询共用变化处理。
///
/// 参数说明：
/// - `id`：后台观察任务的实例诊断标识。
/// - `cache`：发布器比较和更新的服务目录缓存。
/// - `push_empty_protection`：是否拒绝不能提供可用实例证据的推送，具体判断由 ServiceInfo::validate 决定。
///
/// 返回：本地订阅入口与共享发布器，通道容量为 1024 并启动观察任务；不建立远端订阅。
pub(crate) fn create(
    id: String,
    cache: Arc<Cache<ServiceInfo>>,
    push_empty_protection: bool,
) -> (ServiceInfoObserver, Arc<ServiceInfoEmitter>) {
    let (sender, receiver) = channel::<(ServiceInfo, Span)>(1024);
    let service_info_observer = ServiceInfoObserver::new(id, receiver);
    let service_info_emitter = ServiceInfoEmitter::new(sender, cache, push_empty_protection);
    (service_info_observer, Arc::new(service_info_emitter))
}

type ListenerRegistry = Arc<RwLock<HashMap<String, Vec<Arc<dyn NamingEventListener>>>>>;
pub(crate) struct ServiceInfoObserver {
    /// 按服务及分组键登记的接收者列表，不包含远端订阅状态。
    registry: ListenerRegistry,
}

impl ServiceInfoObserver {
    /// 业务作用：创建本地监听注册表并启动服务快照的后台分派任务。
    ///
    /// 参数说明：
    /// - `id`：观察任务诊断上下文的实例标识。
    /// - `receiver`：携带服务快照和来源 span 的有界接收端。
    ///
    /// 返回：共享注册表的订阅入口；接收端由后台任务持有，发送端全部关闭后任务退出。
    fn new(id: String, receiver: Receiver<(ServiceInfo, Span)>) -> Self {
        let registry: ListenerRegistry = Default::default();
        executor::spawn(
            ServiceInfoObserver::observe(receiver, registry.clone())
                .instrument(debug_span!("ServiceInfoObserver", id = id)),
        );
        Self {
            registry,
        }
    }
}

impl ServiceInfoObserver {
    /// 业务作用：以接收者实例身份定位订阅项，使注册去重和取消使用相同匹配规则。
    ///
    /// 参数说明：
    /// - `vec`：当前服务键对应的监听器列表。
    /// - `target`：待定位的 Arc 实例，克隆同一 Arc 仍视为同一接收者。
    ///
    /// 返回：首个身份相同项的下标，没有匹配项时为空，不执行业务回调。
    fn index_of_listener(
        vec: &[Arc<dyn NamingEventListener>],
        target: &Arc<dyn NamingEventListener>,
    ) -> Option<usize> {
        vec.iter()
            .position(|subscriber| Arc::ptr_eq(subscriber, target))
    }
}

impl ServiceInfoObserver {
    /// 业务作用：串行读取服务快照，为当前已登记的接收者分别安排异步通知。
    ///
    /// 参数说明：
    /// - `receiver`：快照和来源 span 的接收端。
    /// - `registry`：按服务及分组键索引的本地接收者集合。
    ///
    /// 返回：发送端全部关闭且队列排空后结束；未注册或空接收者集合被跳过。回调独立执行，不等待结果或保证完成顺序，取消订阅不撤销已安排任务。
    async fn observe(mut receiver: Receiver<(ServiceInfo, Span)>, registry: ListenerRegistry) {
        info!("service info observe task start!");
        while let Some((service_info, span)) = receiver.recv().await {
            // 注册表按服务与分组索引，不把 clusters 拼入键，必须与本地订阅入口保持一致。
            let key =
                ServiceInfo::get_key_without_clusters(&service_info.name, &service_info.group_name);

            let map = registry.read().await;
            let Some(listeners) = map.get(&key) else {
                // 没有明确接收者时不广播给其它服务的监听器。
                warn!("the key {key:?} is not subscribed.");
                continue;
            };
            if listeners.is_empty() {
                warn!("the subscriber listener set of key {key:?} is empty.");
                continue;
            }

            let naming_event = NamingChangeEvent {
                service_name: service_info.name,
                group_name: service_info.group_name,
                clusters: service_info.clusters,
                instances: service_info.hosts,
            };

            let naming_event = Arc::new(naming_event);

            for listener in listeners {
                let naming_event = naming_event.clone();
                let listener = listener.clone();
                info!("notify listener: {key:?}, notify data: {naming_event:?}");
                executor::spawn(
                    async move { listener.event(naming_event) }.instrument(span.clone()),
                );
            }
        }
        info!("service info observe task quit!");
    }

    /// 业务作用：为服务键登记本地接收者，避免同一 Arc 实例在该键下重复投递。
    ///
    /// 参数说明：
    /// - `key`：与事件分派一致的服务及分组键。
    /// - `listener`：本地接收者实例，同实例已存在时先移除再放到列表末尾。
    ///
    /// 返回：注册表更新后结束，不发送远端订阅、重放旧快照或立即通知接收者。
    #[instrument(fields(subscribe_key = key), skip_all)]
    pub(crate) async fn subscribe(&self, key: String, listener: Arc<dyn NamingEventListener>) {
        info!("subscribe {key:?}");
        let mut map = self.registry.write().await;
        let listeners = map.get_mut(&key);
        if let Some(listeners) = listeners {
            let index = Self::index_of_listener(listeners, &listener);
            if let Some(index) = index {
                warn!("listener has already exist, remove old listener and then add new listener.");
                listeners.remove(index);
            }
            listeners.push(listener);
        } else {
            let listeners = vec![listener];
            map.insert(key, listeners);
        }
    }

    /// 业务作用：停止为指定服务键下的一个接收者安排后续事件。
    ///
    /// 参数说明：
    /// - `key`：原订阅使用的服务及分组键。
    /// - `listener`：原注册 Arc 实例，按身份精确匹配。
    ///
    /// 返回：存在时移除，不存在时保持原状；空列表键仍保留，不取消远端订阅或已启动回调。
    #[instrument(fields(unsubscribe_key = key), skip_all)]
    pub(crate) async fn unsubscribe(&self, key: String, listener: Arc<dyn NamingEventListener>) {
        info!("unsubscribe {key:?}");

        let mut map = self.registry.write().await;
        let Some(listeners) = map.get_mut(&key) else {
            return;
        };

        let Some(index) = Self::index_of_listener(listeners, &listener) else {
            warn!("listener {key:?} doesn't exist");
            return;
        };
        listeners.remove(index);
    }
}

#[derive(Clone)]
pub(crate) struct ServiceInfoEmitter {
    /// 向观察任务提交快照及来源诊断上下文的有界通道。
    sender: Sender<(ServiceInfo, Span)>,
    /// 当前已接受的服务实例目录。
    cache: Arc<Cache<ServiceInfo>>,
    /// 是否拒绝不能通过可用实例检查的推送。
    push_empty_protection: bool,
}

impl ServiceInfoEmitter {
    /// 业务作用：建立统一实例快照发布器，绑定目录缓存与事件接收通道。
    ///
    /// 参数说明：
    /// - `sender`：观察任务的有界发送端，承载快照和调用上下文。
    /// - `cache`：比较旧快照和写入新快照的共享缓存。
    /// - `push_empty_protection`：拒绝缺少可用实例证据快照的策略开关。
    ///
    /// 返回：保存发布依赖的对象，不改动缓存或启动额外后台任务。
    fn new(
        sender: Sender<(ServiceInfo, Span)>,
        cache: Arc<Cache<ServiceInfo>>,
        push_empty_protection: bool,
    ) -> Self {
        Self {
            sender,
            cache,
            push_empty_protection,
        }
    }

    /// 业务作用：先判断并发布目录变化，再把可通知快照交给本地观察任务。
    ///
    /// 参数说明：
    /// - `service_info`：来自推送或主动查询的完整服务快照。
    ///
    /// 返回：无有效变化时直接结束，有变化时等待通知入队；队列关闭只记录错误，不回滚已更新缓存，也不等待业务回调。
    #[instrument(skip_all)]
    pub(crate) async fn emit(&self, service_info: ServiceInfo) {
        let notify = self
            .process_service_info(service_info.clone())
            .in_current_span()
            .await;
        let span = Span::current();
        if notify {
            // 缓存先于通知入队更新；发送失败不能被解释为缓存写入失败或自动回滚。
            let send_ret = self.sender.send((service_info, span)).await;
            if let Err(e) = send_ret {
                error!("notify observer object failed: {e}");
            }
        }
    }

    /// 业务作用：在目录更新前判断快照是否缺少正文或触发空推送保护。
    ///
    /// 参数说明：
    /// - `service_info`：待接收快照，hosts 缺失始终拒绝。
    ///
    /// 返回：hosts 缺失，或开启保护且 validate 未通过时为真；关闭保护允许存在但为空的 hosts，不代替其它身份或时间校验。
    fn is_empty_or_error_push(&self, service_info: &ServiceInfo) -> bool {
        service_info.hosts.is_none() || (self.push_empty_protection && !service_info.validate())
    }

    /// 业务作用：过滤受保护推送，比较实例快照并在有变化时替换服务目录缓存。
    ///
    /// 参数说明：
    /// - `service_info`：待比较的新快照，缓存键只包含服务及分组。
    ///
    /// 返回：快照被接受且实例发生变化时为真并更新缓存；其它情况为假。仅刷新时间变化不会替换缓存，比较与写入不是单次原子操作。
    async fn process_service_info(&self, service_info: ServiceInfo) -> bool {
        // 先执行空推送保护，避免缺少可用实例证据的响应直接清空既有目录。
        if self.is_empty_or_error_push(&service_info) {
            warn!("empty or error push: {service_info:?}");
            return false;
        }

        // 共享缓存按服务与分组定位，集群范围保存在快照正文，不能在此擅自改变键合同。
        let key =
            ServiceInfo::get_key_without_clusters(&service_info.name, &service_info.group_name);

        let changed = {
            let old_service = self.cache.get(&key);
            Self::is_changed_service_info(old_service, &service_info)
        };

        if changed {
            info!(
                "current ips:({}) service: {} -> {}",
                service_info.ip_count(),
                key,
                service_info.hosts_to_json()
            );
            let _ = self.cache.insert(key, service_info);
        }
        changed
    }

    /// 业务作用：比较缓存与待发布快照，拒绝过期数据并识别需要通知的实例变化。
    ///
    /// 参数说明：
    /// - `old_service`：缓存守卫，缺失时视为首次目录发布。
    /// - `new_service`：待接收快照，由调用方先执行空推送保护。
    ///
    /// 返回：首次出现、hosts 存在性变化或实例属性变化时为真；明确过期及相同实例集合为假，不修改缓存或安排通知。
    fn is_changed_service_info(
        old_service: Option<CacheRef<'_, ServiceInfo>>,
        new_service: &ServiceInfo,
    ) -> bool {
        let key = ServiceInfo::get_key(
            &ServiceInfo::get_grouped_service_name(&new_service.name, &new_service.group_name),
            &new_service.clusters,
        );

        // 无历史目录时建立首次快照，不能把初始非空目录误判为无需通知。
        let old_service = match old_service {
            None => {
                info!(
                    "init new ips({}) service: {key} -> {}",
                    new_service.ip_count(),
                    new_service.hosts_to_json()
                );
                return true;
            }
            Some(s) => s,
        };

        // 服务端刷新时间倒退时保留已知目录，防止旧响应覆盖新状态。
        if is_outdated_data(&old_service, new_service) {
            warn!(
                "out of date data received, old-t: {}, new-t: {}",
                old_service.last_ref_time, new_service.last_ref_time
            );
            return false;
        }

        let old_hosts = old_service.hosts.as_ref();
        let new_hosts = new_service.hosts.as_ref();

        if new_hosts.is_none() && old_hosts.is_none() {
            return false;
        }
        if new_hosts.is_none() || old_hosts.is_none() {
            return true;
        }
        // 时间不倒退后才比较实例身份与属性，只有实际实例变化触发目录替换。
        let diff = ServiceInfoDiff::calculate(
            old_service.hosts.as_deref().unwrap_or(&[]),
            new_service.hosts.as_deref().unwrap_or(&[]),
        );

        diff.log_changes(&key);

        diff.changed
    }
}
