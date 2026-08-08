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
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    fn new() -> Arc<Self> {
        Arc::new(Self {
            removed: AtomicBool::new(false),
            cell: OnceCell::new(),
        })
    }
}

/// 业务作用：传输错误短期降级窗口是否仍在生效(`now < until`)。锁只在此瞬间持有,不跨 await。
///
/// # 参数
/// - `state`: 实例索引当前持有的快照状态。
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
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub(crate) fn new(
        discovery: Arc<dyn DiscoveryClient>,
        watch: RestWatchOptions,
        metrics: Arc<RestMetrics>,
        no_instance: NoInstancePolicy,
    ) -> Self {
        Self {
            discovery,
            watch_states: DashMap::new(),
            ttl_cache: DashMap::new(),
            watch,
            metrics,
            no_instance,
        }
    }

    /// 业务作用：取某服务当前实例快照(可能为空 = 权威无实例;由 client 决定是否 `NoAvailableInstance`)。
    ///
    /// 循环是为支持「churn 回收后复活」:若取到的 entry 已被 [`mark_removed`](Self::mark_removed) 标记,
    /// 就换一个全新 entry 重来。init 完成后复查 `removed`,杜绝 init-in-flight 的 orphan task。
    pub(crate) async fn instances(&self, service: &str) -> Result<Arc<Vec<Instance>>> {
        loop {
            let entry = self
                .watch_states
                .entry(service.to_string())
                .or_insert_with(WatchEntry::new)
                .clone();

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
                    tracing::warn!(service, error = %e, "rest-discovery: watch 启动失败,降级 discover+TTL");
                    return self.discover_ttl(service).await;
                }
            }
        }
    }

    /// 业务作用：把被标记 `removed` 的 entry 换成全新 entry —— 仅当 map 里【还是它】(Arc 身份 CAS),避免覆盖别人刚装的新 entry。
    /// 旧 entry 的 task 在调用本函数前已被 abort(mark_removed 或 instances 的复查)。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `removed_entry`: 本轮刷新中被移除的实例条目。
    fn revive(&self, service: &str, removed_entry: &Arc<WatchEntry>) {
        if let dashmap::mapref::entry::Entry::Occupied(mut occ) =
            self.watch_states.entry(service.to_string())
        {
            if Arc::ptr_eq(occ.get(), removed_entry) {
                occ.insert(WatchEntry::new());
            }
        }
    }

    /// 业务作用：启动一次 watch:成功则播种快照 + spawn 自带退避重建的 pump,返回 `WatchState`;失败返回 `Err`(不写 cell)。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    async fn try_start_watch(&self, service: &str) -> anyhow::Result<WatchState> {
        let opts = WatchOptions::new().with_poll_interval(self.watch.poll_interval);
        let ServiceWatch { guard, receiver } = self
            .discovery
            .watch_with_options(service, opts.clone())
            .await?;

        // 返回前 receiver 已被后端播种为当前健康快照。
        let snapshot = Arc::new(ArcSwap::from_pointee(receiver.borrow().clone()));
        let degraded = Arc::new(AtomicBool::new(false));

        let task = tokio::spawn(pump_loop(
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
        ));

        tracing::debug!(service, "rest-discovery: watch 已启动");
        Ok(WatchState {
            snapshot,
            degraded,
            transport_degraded_until: Mutex::new(None),
            task: task.abort_handle(),
        })
    }

    /// 业务作用：降级路径:discover + 短 TTL cache(+ stale-if-error)。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    async fn discover_ttl(&self, service: &str) -> Result<Arc<Vec<Instance>>> {
        // 进入降级即记一次 watch_fallback(watch 不可用 / 中断)。
        self.metrics.watch_fallback();
        let now = Instant::now();

        // 未过期缓存直接用。
        if let Some(c) = self.ttl_cache.get(service) {
            if c.expires_at > now {
                return Ok(c.instances.clone());
            }
        }

        match self.discovery.discover(service).await {
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
                Err(RestDiscoveryError::DiscoveryFailed {
                    service: service.to_string(),
                    source: e,
                })
            }
        }
    }

    /// 业务作用：服务名超 grace 移出启发式索引时回收其 watch(,由 client 的 index 刷新循环驱动):
    /// 标记 `removed` + abort 已 init 的 pump(其 `task` 被 cancel → `ServiceWatchGuard` 随之 drop,释放后端订阅);
    /// **in-flight init**(cell 尚空)的 task 由该 init 完成后 [`instances`](Self::instances) 的 removed 复查自行 abort。
    /// 同时清掉降级 TTL 缓存。**不删 map entry**(删了会和正在 init 的 get 抢、漏 task);被请求时 `instances()` 会复活。
    pub(crate) fn mark_removed(&self, service: &str) {
        if let Some(entry) = self.watch_states.get(service) {
            entry.removed.store(true, Ordering::Release);
            if let Some(state) = entry.cell.get() {
                state.task.abort();
            }
        }
        self.ttl_cache.remove(service);
    }

    /// 业务作用：二阶段安全 prune(墓碑回收):删除「已标记 `removed` **且非本轮刚标记**」的 entry —— 给一轮
    /// refresh 的 grace,避开本轮刚 mark、可能正被显式调用复活的 entry。`retain` 在 shard 锁下与 `revive` 串行,
    /// 配合 `instances()` 的「持 Arc 复查 removed 自 abort」杜绝 init-in-flight orphan(与「立即 remove」的老路不同)。
    pub(crate) fn prune_removed_except(&self, just_marked: &std::collections::HashSet<&str>) {
        self.watch_states
            .retain(|k, e| !e.removed.load(Ordering::Acquire) || just_marked.contains(k.as_str()));
    }

    /// 业务作用：内部调用对该 service 发生【传输错误】(连接/超时)时调用(传输错误应触发该 service 快速刷新/恢复,
    /// 而非等下一轮 poll)。开一个【短期降级窗口】(长度 = `ttl_fallback`):窗口内后续请求走 discover+TTL 拿新数据,
    /// **过窗自动回 watch 快照热路径** —— 不动 pump 自己的 `degraded`,避免一次瞬时抖动把 service 永久降级。
    /// 同时清 TTL 缓存避免继续用旧数据。空实例列表不是错误,不在此触发(由调用方过滤)。
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

    /// 业务作用：abort 所有 watch pump(`RemoteRuntime` drop / 运行时 reset 调用),避免后台任务泄漏。
    /// 同时把每个 entry 标记 `removed`:让此刻正在 init(cell 尚空)的 get 完成后自 abort,收掉 init-in-flight 的 task。
    pub(crate) fn abort_all(&self) {
        for entry in self.watch_states.iter() {
            entry.value().removed.store(true, Ordering::Release);
            if let Some(state) = entry.value().cell.get() {
                state.task.abort();
            }
        }
    }
}

/// 业务作用：watch pump:消费一条订阅直到中断,然后按退避重建。期间持有 `ServiceWatchGuard`(只要求 Send)保订阅存活。
///
/// `degraded` 在「订阅中断、等待重建」时置 true,让 `instances()` 暂走 discover+TTL;重建成功置回 false。
///
/// # 参数
/// - `discovery`: 服务发现客户端或运行时实例。
/// - `service`: 服务名,用于服务发现或注册中心查询。
/// - `opts`: 运行选项,用于控制客户端或调度器行为。
/// - `backoff_min`: 实例刷新失败后的最小退避时间。
/// - `backoff_max`: 实例刷新失败后的最大退避时间。
/// - `snapshot`: presence 或服务实例快照。
/// - `degraded`: 当前实例列表是否处于降级状态。
/// - `metrics`: 实例刷新和路由过程记录的指标对象。
/// - `init_guard`: 控制首次初始化只执行一次的保护标记。
/// - `init_rx`: 等待首次实例加载完成的通知接收端。
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
                Ok(ServiceWatch { guard, receiver }) => (guard, receiver),
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
