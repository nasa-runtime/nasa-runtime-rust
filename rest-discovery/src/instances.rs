//! 实例事实源。
//!
//! **稳态唯一事实源 = `DiscoveryClient::watch_with_options` 的 watch 快照**(nacos 层已播种 + 轮询兜底 +
//! `is_traffic_instance` 过滤 + 稳定排序)。watch 启动失败 / pump 退出时,**短期降级到 `discover + TTL cache`**,
//! 并由 pump 自身按退避重建 watch。两套事实源不长期并行。
//!
//! 关键纪律:
//! - 并发首请求用 `tokio::sync::OnceCell` 去重,只启动一次 watch;**watch 启动失败绝不写进 cell**(不毒化),
//!   下次可重试。
//! - watch 成功推空 `Vec` = 权威无实例,交给 client 返回 `NoAvailableInstance`;只有 discover **出错**才允许短暂 stale。
//! - 所有 pump task 句柄登记在册,`RemoteRuntime` drop / 运行时 reset 时统一 abort,不泄漏后台任务。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use nadisc::{DiscoveryClient, Instance, ServiceWatch, ServiceWatchGuard, WatchOptions};
use tokio::sync::watch;
use tokio::sync::OnceCell;

use crate::error::{RestDiscoveryError, Result};
use crate::metrics::RestMetrics;
use crate::options::{NoInstancePolicy, RestWatchOptions};

/// 一条活跃订阅:guard(持有以保订阅存活)+ 实例快照接收端。
type ActiveSub = (Box<dyn ServiceWatchGuard>, watch::Receiver<Vec<Instance>>);

/// 单个服务的 watch 状态:最新快照 + 降级标志 + pump 任务句柄。
struct WatchState {
    /// 最新「可承载流量实例」快照(nacos 层已过滤+排序;读多写少用 ArcSwap)。
    snapshot: Arc<ArcSwap<Vec<Instance>>>,
    /// pump 处于「订阅中断、退避重建中」时为 true:此时改用 discover+TTL 兜底。
    degraded: Arc<AtomicBool>,
    /// 传输错误触发的【短期】降级窗口截止时刻(`None`=无)。窗口内走 discover+TTL,过【后自动回 watch 快照热路径】
    /// (discover+TTL 只作短期降级)。锁只在读/写瞬间持有,不跨 await。
    transport_degraded_until: Mutex<Option<Instant>>,
    /// pump 任务(reset/drop 时 abort)。
    task: tokio::task::AbortHandle,
}

/// 一个服务的 watch 入口:`removed` 标志 + 去重 `OnceCell`。
///
/// 清理纪律(,杜绝 init-in-flight orphan):
/// - [`mark_removed`](InstanceSource::mark_removed) 只【标记】`removed` + abort 已 init 的 task;**不删 map entry**
///   (删了会和正在 `get_or_try_init` 的 `instances()` 抢,漏掉那条 task)。
/// - `instances()` 在 init 完成后【必须复查】`removed`:若已被标记,立刻 abort 自己刚建的 task → 杜绝 orphan。
/// - 复活:被标记 `removed` 的 entry 再被请求时,用 Arc 身份 CAS 换上一个【全新 entry】(旧 entry 的 task 此时已 abort)。
struct WatchEntry {
    removed: AtomicBool,
    cell: OnceCell<WatchState>,
}

impl WatchEntry {
    /// 业务作用：为一个服务建立尚未初始化订阅的共享目录项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回 removed 为 false、订阅单元为空的 Arc；不调用发现后端或创建任务。
    fn new() -> Arc<Self> {
        Arc::new(Self {
            removed: AtomicBool::new(false),
            cell: OnceCell::new(),
        })
    }
}

/// 业务作用：检查实例观察当前是否仍处于传输错误触发的短期降级窗口。
///
/// 参数说明：
/// - `state`：持有降级截止点的订阅状态。
///
/// 返回：存在截止点且当前单调时刻严格早于它时返回 true，否则 false；锁只短暂持有，锁中毒时 panic。
fn transport_window_active(state: &WatchState) -> bool {
    state
        .transport_degraded_until
        .lock()
        .unwrap()
        .is_some_and(|until| Instant::now() < until)
}

/// 降级 TTL cache 的一条记录。
#[derive(Clone)]
struct CachedInstances {
    instances: Arc<Vec<Instance>>,
    /// 过期时刻(超过则需重新 discover)。
    expires_at: Instant,
    /// discover **出错**时允许沿用到的时刻(stale-if-error)。
    stale_until: Instant,
}

/// 实例事实源:watch 快照为主,discover+TTL 为降级。
pub(crate) struct InstanceSource {
    discovery: Arc<dyn DiscoveryClient>,
    background: Arc<crate::background::BackgroundOwner>,
    /// canonical service name → watch 入口(`OnceCell` 去重首次启动 + `removed` 标志支持 churn 回收)。
    watch_states: DashMap<String, Arc<WatchEntry>>,
    /// 降级 TTL cache(key 同样是 canonical service name)。
    ttl_cache: DashMap<String, CachedInstances>,
    watch: RestWatchOptions,
    metrics: Arc<RestMetrics>,
    /// discover 出错时是否允许短暂沿用 stale 实例(`StaleIfDiscoveryError` 才允许;默认 `Error` 不允许)。
    no_instance: NoInstancePolicy,
}

impl InstanceSource {
    /// 业务作用：识别已经由公共 owner 管理有效期的观察源。
    ///
    /// 参数说明：无。
    ///
    /// 返回：受管源要求每个 attempt 重新选址并复验发送许可。
    pub(crate) fn managed(&self) -> bool {
        self.discovery.requires_admission()
    }

    /// 业务作用：委托发现后端在其候选资格边界内同步提交传输。
    ///
    /// 参数说明：
    /// - `service`：本次选中的逻辑服务名。
    /// - `instance`：需要由后端复验的目标实例。
    /// - `submit`：仅同步启动传输、不阻塞或重入发现接口的回调。
    ///
    /// 返回：后端准入成功时返回成功，失败映射为带服务名的 DiscoveryFailed；本层不额外实现候选时效校验，旧后端可直接执行回调。
    pub(crate) fn submit(
        &self,
        service: &str,
        instance: &Instance,
        submit: &mut dyn FnMut(),
    ) -> Result<()> {
        self.discovery
            .submit_if_current(service, instance, submit)
            .map_err(|source| RestDiscoveryError::DiscoveryFailed {
                service: service.into(),
                source,
            })
    }

    /// 业务作用：组装服务实例观察目录、缓存策略和后台任务持有者。
    ///
    /// 参数说明：
    /// - `discovery`：负责发现及订阅的共享后端。
    /// - `watch`：服务容量、轮询、缓存及重建策略。
    /// - `metrics`：记录观察及降级事件的共享指标。
    /// - `no_instance`：决定发现失败时是否可使用非空旧快照的策略。
    /// - `background`：统一管理准入、在途调用及观察任务关闭的持有者。
    ///
    /// 返回：返回空订阅目录和空 TTL 缓存；保存依赖，不校验选项或启动订阅。
    pub(crate) fn new(
        discovery: Arc<dyn DiscoveryClient>,
        watch: RestWatchOptions,
        metrics: Arc<RestMetrics>,
        no_instance: NoInstancePolicy,
        background: Arc<crate::background::BackgroundOwner>,
    ) -> Self {
        Self {
            discovery,
            background,
            watch_states: DashMap::new(),
            ttl_cache: DashMap::new(),
            watch,
            metrics,
            no_instance,
        }
    }

    /// 业务作用：按后端能力取得服务快照，并在本地订阅回收后重新建立观察。
    ///
    /// 参数说明：
    /// - `service`：本次逻辑请求需要实例的服务名。
    ///
    /// 返回：受管准入后端直接发现；其它后端优先使用有效 watch，否则使用发现缓存。返回可为空的共享列表；关闭、容量或发现失败返回错误。初始化后复验 removed，发现已回收时终止刚生成的任务后重取目录项。
    pub(crate) async fn instances(&self, service: &str) -> Result<Arc<Vec<Instance>>> {
        let _call = self.background.enter()?;
        // 受管后端自行持有时效与撤权证据，绕过本地 watch 缓存，避免旧快照延长路由资格。
        if self.discovery.requires_admission() {
            // 受管源已经合并观察并维护证据期限，不能再叠加 REST 缓存宽限。
            return self
                .discovery
                .discover(service)
                .await
                .map(Arc::new)
                .map_err(|source| RestDiscoveryError::DiscoveryFailed {
                    service: service.into(),
                    source,
                });
        }
        loop {
            let entry = self.background.with_open(|| {
                if let Some(entry) = self.watch_states.get(service) {
                    return Ok(entry.clone());
                }
                if self.watch_states.len() >= self.watch.max_services {
                    return Err(RestDiscoveryError::DiscoveryStateLimit);
                }
                let entry = WatchEntry::new();
                self.watch_states.insert(service.to_owned(), entry.clone());
                Ok(entry)
            })?;

            // entry 已被标记移除(上一轮 churn 清理过)→ 复活:换全新 entry 再来。
            if entry.removed.load(Ordering::Acquire) {
                self.revive(service, &entry);
                continue;
            }

            // 已有 watch:复查 removed(可能在读到此处前刚被标记)→ 收掉 task + 复活;否则读快照 / 走降级。
            if let Some(state) = entry.cell.get() {
                if entry.removed.load(Ordering::Acquire) {
                    state.task.abort();
                    self.revive(service, &entry);
                    continue;
                }
                // 降级条件:pump 订阅中断(degraded)或 处于传输错误的短期降级窗口内 → 走 discover+TTL;否则读 watch 快照。
                // 传输窗口是【时间界】的:过窗自动回热路径,不会因一次瞬时抖动永久降级(短期降级)。
                if !state.degraded.load(Ordering::Acquire) && !transport_window_active(state) {
                    return Ok(state.snapshot.load_full());
                }
                return self.discover_ttl(service).await;
            }

            // 首次:OnceCell 去重启动 watch(并发首请求只跑一个);失败不毒化 → 降级。
            match entry
                .cell
                .get_or_try_init(|| self.try_start_watch(service))
                .await
            {
                Ok(state) => {
                    // in-flight init 期间被 mark_removed/abort_all 标记 → 收掉刚建的 task,复活重试(杜绝 orphan)。
                    if entry.removed.load(Ordering::Acquire) {
                        state.task.abort();
                        self.revive(service, &entry);
                        continue;
                    }
                    return Ok(state.snapshot.load_full());
                }
                Err(e) => {
                    self.background.with_open(|| Ok(()))?;
                    tracing::warn!(service, error = %e, "rest-discovery: watch 启动失败,降级 discover+TTL");
                    return self.discover_ttl(service).await;
                }
            }
        }
    }

    /// 业务作用：仅在服务目录仍持有已观察的旧项时替换为待初始化的新项。
    ///
    /// 参数说明：
    /// - `service`：需恢复观察的服务名。
    /// - `removed_entry`：调用方观察到已被回收的旧目录项。
    ///
    /// 返回：指针仍匹配时替换，目录已变化或缺失时无操作，不覆盖其它调用已建立的新项。
    fn revive(&self, service: &str, removed_entry: &Arc<WatchEntry>) {
        if let dashmap::mapref::entry::Entry::Occupied(mut occ) =
            self.watch_states.entry(service.to_string())
        {
            if Arc::ptr_eq(occ.get(), removed_entry) {
                occ.insert(WatchEntry::new());
            }
        }
    }

    /// 业务作用：在后台持有者的执行边界内建立订阅并接管其消费任务。
    ///
    /// 参数说明：
    /// - `service`：需建立实例变化订阅的服务名。
    ///
    /// 返回：成功返回初始共享快照及任务终止句柄；后端建立或任务接纳失败返回错误。订阅 guard 转入消费任务，不在此保证初始化期间服务尚未被回收。
    async fn try_start_watch(&self, service: &str) -> anyhow::Result<WatchState> {
        let opts = WatchOptions::new().with_poll_interval(self.watch.poll_interval);
        let ServiceWatch {
            guard,
            receiver,
        } = self
            .background
            .run(async {
                self.discovery
                    .watch_with_options(service, opts.clone())
                    .await
                    .map_err(|source| RestDiscoveryError::DiscoveryFailed {
                        service: service.to_owned(),
                        source,
                    })
            })
            .await?;

        // 返回前 receiver 已被后端播种为当前健康快照。
        let snapshot = Arc::new(ArcSwap::from_pointee(receiver.borrow().clone()));
        let degraded = Arc::new(AtomicBool::new(false));

        let task = self.background.spawn(pump_loop(
            self.discovery.clone(),
            service.to_string(),
            opts,
            self.watch.restart_backoff_min,
            self.watch.restart_backoff_max,
            snapshot.clone(),
            degraded.clone(),
            self.metrics.clone(),
            guard,
            receiver,
        ))?;

        tracing::debug!(service, "rest-discovery: watch 已启动");
        Ok(WatchState {
            snapshot,
            degraded,
            transport_degraded_until: Mutex::new(None),
            task,
        })
    }

    /// 业务作用：在 watch 退化时按 TTL 缓存发现结果，并按策略使用错误宽限。
    ///
    /// 参数说明：
    /// - `service`：要查询或复用缓存的逻辑服务。
    ///
    /// 返回：有效缓存直接返回；发现成功缓存完整列表，包括空列表。失败仅在策略允许且旧列表非空、宽限仍覆盖本次调用起始时刻时复用，否则返回错误。期限与比较采用发起前时刻，不承诺返回时仍处于该期限内。
    async fn discover_ttl(&self, service: &str) -> Result<Arc<Vec<Instance>>> {
        self.background.with_open(|| Ok(()))?;
        // 进入降级即记一次 watch_fallback(watch 不可用 / 中断)。
        self.metrics.watch_fallback();
        let now = Instant::now();

        // 未过期缓存直接用。
        if let Some(c) = self.ttl_cache.get(service) {
            if c.expires_at > now {
                return Ok(c.instances.clone());
            }
        }

        match self
            .background
            .run(async {
                self.discovery.discover(service).await.map_err(|source| {
                    RestDiscoveryError::DiscoveryFailed {
                        service: service.to_owned(),
                        source,
                    }
                })
            })
            .await
        {
            // discover 已是「可承载流量」实例(nacos discover_core 用 is_traffic_instance 过滤)。
            Ok(list) => {
                let arc = Arc::new(list);
                self.ttl_cache.insert(
                    service.to_string(),
                    CachedInstances {
                        instances: arc.clone(),
                        expires_at: now + self.watch.ttl_fallback,
                        stale_until: now + self.watch.stale_if_error,
                    },
                );
                Ok(arc)
            }
            Err(e) => {
                self.background.with_open(|| Ok(()))?;
                self.metrics.discovery_failure();
                // 仅 NoInstancePolicy::StaleIfDiscoveryError 才允许 discover 出错时短暂沿用 stale;
                // 默认 Error 直接返回 DiscoveryFailed(成功空列表在上游已恒返回 NoAvailableInstance,绝不 stale)。
                if self.no_instance == NoInstancePolicy::StaleIfDiscoveryError {
                    if let Some(c) = self.ttl_cache.get(service) {
                        if c.stale_until > now && !c.instances.is_empty() {
                            tracing::debug!(service, error = %e, "rest-discovery: discover 失败,按 StaleIfDiscoveryError 短暂沿用 stale 实例");
                            return Ok(c.instances.clone());
                        }
                    }
                }
                Err(e)
            }
        }
    }

    /// 业务作用：标记服务观察已移除，终止已发布任务并清理发现缓存。
    ///
    /// 参数说明：
    /// - `service`：索引已确认需要回收的服务规范名。
    ///
    /// 返回：已有项设置 removed，已初始化任务发送 abort，TTL 缓存移除；不直接删除目录项，使在途初始化仍能复验回收标记。
    pub(crate) fn mark_removed(&self, service: &str) {
        if let Some(entry) = self.watch_states.get(service) {
            entry.removed.store(true, Ordering::Release);
            if let Some(state) = entry.cell.get() {
                state.task.abort();
            }
        }
        self.ttl_cache.remove(service);
    }

    /// 业务作用：在回收目录时为本轮刚标记项保留一轮墓碑。
    ///
    /// 参数说明：
    /// - `just_marked`：本轮刚标记、需要暂留回收证据的服务名集合。
    ///
    /// 返回：删除其它已标记项，保留未移除和集合内项；不执行网络或等待任务退出。
    pub(crate) fn prune_removed_except(&self, just_marked: &std::collections::HashSet<&str>) {
        self.watch_states
            .retain(|k, e| !e.removed.load(Ordering::Acquire) || just_marked.contains(k.as_str()));
    }

    /// 业务作用：为发生传输错误的服务开启临时发现路径并撤销 TTL 快照。
    ///
    /// 参数说明：
    /// - `service`：本次连接或超时失败涉及的服务名。
    ///
    /// 返回：已有观察状态时设置长度为 ttl_fallback 的降级窗口，清除该服务缓存并记指标；不改变 pump 的 degraded 状态，窗口到期可回 watch。
    pub(crate) fn mark_transport_error(&self, service: &str) {
        if let Some(entry) = self.watch_states.get(service) {
            if let Some(state) = entry.cell.get() {
                let until = Instant::now() + self.watch.ttl_fallback;
                *state.transport_degraded_until.lock().unwrap() = Some(until);
            }
        }
        self.ttl_cache.remove(service);
        self.metrics.transport_error_refresh();
    }

    /// 业务作用：关闭后台调用准入并标记全部服务观察为已回收。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：先关闭后台持有者，再标记目录并请求终止已初始化任务；在途初始化由调用方完成后的 removed 复验收口，不在此等待异步任务全部退出。
    pub(crate) fn abort_all(&self) {
        // 先关闭准入，防止遍历回收目录时又有新调用登记尚未纳入清理的观察任务。
        self.background.close();
        for entry in self.watch_states.iter() {
            entry.value().removed.store(true, Ordering::Release);
            if let Some(state) = entry.value().cell.get() {
                state.task.abort();
            }
        }
    }
}

/// 业务作用：持续接收服务订阅快照，在订阅中断后按有界退避重新建立。
///
/// 参数说明：
/// - `discovery`：用于重建订阅的共享发现后端。
/// - `service`：固定的目标服务名。
/// - `opts`：重建时复用的后端订阅选项。
/// - `backoff_min`：重建最小退避，内部至少采用一毫秒。
/// - `backoff_max`：重建最大退避，内部至少等于最小退避。
/// - `snapshot`：向请求路径发布完整实例列表的共享快照。
/// - `degraded`：控制请求路径是否改用发现缓存的状态标志。
/// - `metrics`：记录订阅中断等事件的共享指标。
/// - `init_guard`：首条已建立订阅的生命周期句柄。
/// - `init_rx`：首条订阅的初值和后续最新快照接收端。
///
/// 返回：正常持续运行直到外部取消或异常展开；建立成功先发布初值再解除降级，断线或重建失败进入降级并退避。收到更新且连接已稳定后才重置退避，丢弃 guard 按后端合同取消订阅。
#[allow(clippy::too_many_arguments)]
async fn pump_loop(
    discovery: Arc<dyn DiscoveryClient>,
    service: String,
    opts: WatchOptions,
    backoff_min: Duration,
    backoff_max: Duration,
    snapshot: Arc<ArcSwap<Vec<Instance>>>,
    degraded: Arc<AtomicBool>,
    metrics: Arc<RestMetrics>,
    init_guard: Box<dyn ServiceWatchGuard>,
    init_rx: watch::Receiver<Vec<Instance>>,
) {
    // 内部不变量兜底 clamp:即便上游(direct API 未校验)传入 0,也不至于 sleep(0) 空转/零间隔重订阅打爆 provider。
    // 这是防御性下界,不是主校验入口;公开构造路径会先返回 typed error。
    let backoff_min = backoff_min.max(Duration::from_millis(1));
    let backoff_max = backoff_max.max(backoff_min);
    // 订阅「稳定存活」窗口:活过此窗口才认为订阅健康、可把退避重置回 min。
    // 刻意 ≥ 1s 而非等于 backoff_min —— 否则「交付一次事件 / 活过极短 backoff_min 后即关」的 flapping
    // 仍会被判为稳定、绕过退避。
    let stable_reset_after = backoff_min.max(Duration::from_secs(1));

    // 首轮用传入的订阅;之后每轮 None → 重建。
    let mut pending: Option<ActiveSub> = Some((init_guard, init_rx));
    let mut backoff = backoff_min;

    loop {
        let (guard, mut rx) = match pending.take() {
            // 首轮:用传入订阅(connect 已确认 watch 可用且已播种)。
            Some(sub) => sub,
            None => match discovery.watch_with_options(&service, opts.clone()).await {
                Ok(ServiceWatch {
                    guard,
                    receiver,
                }) => (guard, receiver),
                Err(e) => {
                    tracing::warn!(service = %service, error = %e, "rest-discovery: watch 重建失败,退避后重试");
                    degraded.store(true, Ordering::Release);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(backoff_max);
                    continue;
                }
            },
        };

        // 订阅活跃:先刷一次当前值,清降级标志;guard 持有到本轮订阅结束。
        // 初值必须先发布再开放 watch 热路径，避免请求读到上一条订阅遗留的快照。
        snapshot.store(Arc::new(rx.borrow_and_update().clone()));
        degraded.store(false, Ordering::Release);
        let _hold_guard = guard;

        // 订阅消费循环:每收一次更新刷快照;一旦【活过稳定窗口】就把退避重置回 min(健康订阅断开后能快速重连)。
        // changed() 返回 Err 表示 sender(后端 poll 任务)已 drop,订阅结束。
        let started = Instant::now();
        while rx.changed().await.is_ok() {
            snapshot.store(Arc::new(rx.borrow_and_update().clone()));
            if started.elapsed() >= stable_reset_after {
                backoff = backoff_min;
            }
        }

        // 订阅结束(receiver 关闭 / pump 中断):**无条件**降级 + 退避后重建,_hold_guard 在作用域末尾 drop(释放后端订阅)。
        // 硬约束:receiver 关闭即 watch failure,必须按 backoff 重建,不能零间隔重订阅。
        // 退避是否重置回 min 只取决于上面的「是否活过稳定窗口」,不取决于「是否交付过事件」。
        degraded.store(true, Ordering::Release);
        metrics.watch_receiver_closed();
        tracing::warn!(service = %service, ?backoff, "rest-discovery: watch receiver 关闭,退避后重建");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
}
