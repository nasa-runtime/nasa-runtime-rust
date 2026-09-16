use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    ops::Deref,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, RwLock},
};

use futures_util::FutureExt;
use tokio::{
    sync::{OwnedRwLockReadGuard, RwLock as AsyncRwLock},
    time::timeout,
};

use crate::{
    ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult, ComponentId,
    ShutdownContext,
};

type ErasedResource = ResourceCleanup<Box<dyn Any + Send + Sync>>;
type ErasedShutdown =
    for<'a> fn(&'a mut ErasedResource, &'a ShutdownContext) -> ApplicationFuture<'a>;

/// 需要异步释放的业务资源。同步回调与 `Drop` 必须有限时间返回；需要等待的清理由可让出执行权的 future 完成。
pub trait ManagedResource: Send + Sync + 'static {
    /// 业务作用：在容器分配的剩余预算内完成需要等待的资源清理。
    ///
    /// 参数说明：
    /// - `context`：携带首次停机原因和不晚于全局 deadline 的当前资源清理上下文。
    ///
    /// 返回：成功表示异步清理完成；错误或单次展开式 panic 由容器记录为次要失败，不截断后续资源。
    /// future 创建、poll 和释放分别隔离；阻塞、`panic=abort` 或同一次展开中的再次 panic 无法安全抢占。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a>;
}

/// 资源注册表对登记和新借用开放程度的生命周期阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourcePhase {
    /// 允许组件登记资源和调用方借用资源。
    Open,
    /// 禁止继续登记,但仍允许借用已发布资源。
    Sealed,
    /// 拒绝新的资源借用，正在清理或等待受管任务归还依赖保留权。
    Closing,
    /// 资源清理已经完成。
    Closed,
}

/// 区分框架组件资源和 UserHook 手工登记业务资源的所有者。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResourceOwner {
    Component(ComponentId),
    Initializer(Arc<str>),
    Business,
}

/// 由 Rust TypeId 和可选 qualifier 组成的唯一资源 key。
#[derive(Clone, PartialEq, Eq, Hash)]
struct ResourceKey {
    type_id: TypeId,
    qualifier: Option<Arc<str>>,
}

impl ResourceKey {
    /// 业务作用：为目标类型和 qualifier 创建资源 key。
    ///
    /// # 参数
    ///
    /// - `qualifier`：已经 trim 并确认非空的可选共享名称。
    fn of<T: 'static>(qualifier: Option<Arc<str>>) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            qualifier,
        }
    }
}

/// 保存单个擦除类型资源的锁、所有者、登记顺序和可选清理函数。
struct ResourceEntry {
    type_name: &'static str,
    value: Arc<AsyncRwLock<ErasedResource>>,
    registration_order: u64,
    owner: ResourceOwner,
    shutdown: Option<ErasedShutdown>,
}

/// 隔离资源值与清理 future 的析构，覆盖显式释放、外层取消和最后一个借用归还。
struct ResourceCleanup<T> {
    value: Option<T>,
    type_name: &'static str,
    operation: &'static str,
}

impl<T> ResourceCleanup<T> {
    /// 业务作用：接管需要在独立展开边界内释放的资源所有权。
    ///
    /// 参数说明：
    /// - `value`：资源值或借用资源的清理 future。
    /// - `type_name`：编译期资源类型名，不包含实例或配置值。
    /// - `operation`：框架固定的释放阶段说明。
    ///
    /// 返回：持有一次性释放权的守卫；外层取消时也会尝试释放。
    fn new(value: T, type_name: &'static str, operation: &'static str) -> Self {
        Self {
            value: Some(value),
            type_name,
            operation,
        }
    }

    /// 业务作用：一次性释放所有权并隔离析构异常，供仍能回传报告的清理路径归类。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本次析构 panic 时返回 true；重复调用或正常释放返回 false，不读取异常正文。
    fn release(&mut self) -> bool {
        // 先移出所有权，析构展开后也不能让守卫再次尝试释放同一对象。
        let value = self.value.take();
        match catch_unwind(AssertUnwindSafe(|| drop(value))) {
            Ok(()) => false,
            Err(payload) => {
                crate::shutdown::release_shutdown_panic_payload(payload);
                true
            }
        }
    }
}

impl<T> Drop for ResourceCleanup<T> {
    /// 业务作用：在取消或延迟归还所有权时兜底释放资源，避免析构展开截断其它清理。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无返回值；无法回传到清理报告的析构异常单独同步告警，不追改已经发布的终态。
    fn drop(&mut self) {
        if self.release() {
            crate::report::report_shutdown(&resource_panic_error(self.type_name, self.operation));
        }
    }
}

/// 业务作用：暂存一组异类型组件资源，全部 key 预检通过后再在单次写锁内发布。
pub(crate) struct StagedResourceBatch {
    entries: Vec<(ResourceKey, ResourceEntry)>,
}

impl StagedResourceBatch {
    /// 业务作用：创建尚未包含任何公开资源的暂存批次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可继续加入不同类型和 qualifier 的批次。
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// 业务作用：把普通组件资源加入本地暂存区，不触碰 Application 可见资源表。
    ///
    /// 参数说明：`qualifier` 是可选稳定名称，`value` 是待转交资源。
    ///
    /// 返回：名称合法时完成暂存；非法名称返回错误且批次尚未发布。
    #[cfg(feature = "db-pgsql")]
    pub(crate) fn push<T>(&mut self, qualifier: Option<&str>, value: T) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        let qualifier = qualifier.map(normalize_qualifier).transpose()?;
        self.entries.push((
            ResourceKey::of::<T>(qualifier),
            ResourceEntry {
                type_name: type_name::<T>(),
                value: Arc::new(AsyncRwLock::new(ResourceCleanup::new(
                    Box::new(value),
                    type_name::<T>(),
                    "releasing its value",
                ))),
                registration_order: 0,
                owner: ResourceOwner::Business,
                shutdown: None,
            },
        ));
        Ok(())
    }
}

/// 在同一同步锁下维护资源阶段、顺序号和 key 集合的一致状态。
struct RegistryState {
    phase: ResourcePhase,
    next_order: u64,
    entries: HashMap<ResourceKey, ResourceEntry>,
}

/// Application 拥有的类型资源容器，支持组件、UserHook 和 initializer 登记，以及运行期
/// 只读借用和逆序清理。
pub struct ResourceRegistry {
    state: RwLock<RegistryState>,
}

impl Default for ResourceRegistry {
    /// 业务作用：创建处于 Open 阶段的空资源注册表。
    ///
    /// # 参数
    ///
    /// 本方法无参数；行为与 `new` 相同。
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRegistry {
    /// 业务作用：创建处于 Open 阶段的空资源注册表。
    ///
    /// # 参数
    ///
    /// 本方法无参数；登记顺序从零开始。
    pub fn new() -> Self {
        Self {
            state: RwLock::new(RegistryState {
                phase: ResourcePhase::Open,
                next_order: 0,
                entries: HashMap::new(),
            }),
        }
    }

    /// 业务作用：返回当前资源生命周期阶段。
    ///
    /// # 参数
    ///
    /// 本方法无参数；读取与 key 集合使用同一同步锁。
    pub fn phase(&self) -> ResourcePhase {
        read_unpoisoned(&self.state).phase
    }

    /// 业务作用：登记一个无 qualifier 的普通业务资源。
    ///
    /// # 参数
    ///
    /// - `value`：所有权交给 Application 的线程安全资源。
    pub fn register<T>(&self, value: T) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        self.register_inner(None, value, ResourceOwner::Business, None)
    }

    /// 业务作用：登记一个带 qualifier 的普通业务资源。
    ///
    /// # 参数
    ///
    /// - `qualifier`：区分同类型实例的非空名称。
    /// - `value`：所有权交给 Application 的线程安全资源。
    pub fn register_named<T>(&self, qualifier: impl AsRef<str>, value: T) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        self.register_inner(
            Some(normalize_qualifier(qualifier.as_ref())?),
            value,
            ResourceOwner::Business,
            None,
        )
    }

    /// 业务作用：登记一个无 qualifier 的受管业务资源。
    ///
    /// # 参数
    ///
    /// - `value`：需要显式异步 shutdown 且 Drop 非阻塞的资源。
    pub fn register_managed<T>(&self, value: T) -> ApplicationResult<()>
    where
        T: ManagedResource,
    {
        self.register_inner(
            None,
            value,
            ResourceOwner::Business,
            Some(shutdown_managed::<T>),
        )
    }

    /// 业务作用：登记一个带 qualifier 的受管业务资源。
    ///
    /// # 参数
    ///
    /// - `qualifier`：区分同类型受管实例的非空名称。
    /// - `value`：需要显式异步 shutdown 且 Drop 非阻塞的资源。
    pub fn register_named_managed<T>(
        &self,
        qualifier: impl AsRef<str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: ManagedResource,
    {
        self.register_inner(
            Some(normalize_qualifier(qualifier.as_ref())?),
            value,
            ResourceOwner::Business,
            Some(shutdown_managed::<T>),
        )
    }

    /// 业务作用：借用一个无 qualifier 的资源并返回 owned mapped read guard。
    ///
    /// # 参数
    ///
    /// 本方法无显式参数；类型 `T` 决定资源 key 和返回目标。
    pub async fn get<T>(&self) -> ApplicationResult<ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        self.get_inner(None).await
    }

    /// 业务作用：借用一个指定 qualifier 的资源。
    ///
    /// # 参数
    ///
    /// - `qualifier`：登记时使用的非空名称。
    pub async fn get_named<T>(
        &self,
        qualifier: impl AsRef<str>,
    ) -> ApplicationResult<ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        self.get_inner(Some(normalize_qualifier(qualifier.as_ref())?))
            .await
    }

    /// 业务作用：由阶段上下文登记一个普通组件资源。
    ///
    /// # 参数
    ///
    /// - `component`：拥有资源并决定清理阶段的组件。
    /// - `qualifier`：同类型多实例的可选名称。
    /// - `value`：所有权交给组件资源容器的值。
    pub(crate) fn register_component<T>(
        &self,
        component: ComponentId,
        qualifier: Option<&str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        let qualifier = qualifier.map(normalize_qualifier).transpose()?;
        self.register_inner(qualifier, value, ResourceOwner::Component(component), None)
    }

    /// 业务作用：由阶段上下文登记一个受管组件资源。
    ///
    /// # 参数
    ///
    /// - `component`：拥有资源并决定清理阶段的组件。
    /// - `qualifier`：同类型多实例的可选名称。
    /// - `value`：需要显式异步 shutdown 的组件资源。
    pub(crate) fn register_component_managed<T>(
        &self,
        component: ComponentId,
        qualifier: Option<&str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: ManagedResource,
    {
        let qualifier = qualifier.map(normalize_qualifier).transpose()?;
        self.register_inner(
            qualifier,
            value,
            ResourceOwner::Component(component),
            Some(shutdown_managed::<T>),
        )
    }

    /// 业务作用：原子发布一组异类型组件资源，避免启动期观察到跨 driver 半张表。
    ///
    /// 参数说明：`component` 是统一所有者，`batch` 是尚未公开的完整资源集合。
    ///
    /// 返回：阶段开放且全部 key 唯一时一次性提交；任一冲突时不登记任何条目。
    pub(crate) fn register_component_batch(
        &self,
        component: ComponentId,
        mut batch: StagedResourceBatch,
    ) -> ApplicationResult<()> {
        let mut state = write_unpoisoned(&self.state);
        if state.phase != ResourcePhase::Open {
            return Err(resource_error(format!(
                "resource registration is closed in {:?} phase",
                state.phase
            )));
        }
        let mut batch_keys = std::collections::HashSet::with_capacity(batch.entries.len());
        for (key, entry) in &batch.entries {
            if !batch_keys.insert(key.clone()) || state.entries.contains_key(key) {
                return Err(resource_error(format!(
                    "resource `{}` is already registered",
                    entry.type_name
                )));
            }
        }
        for (key, mut entry) in batch.entries.drain(..) {
            entry.registration_order = state.next_order;
            entry.owner = ResourceOwner::Component(component);
            state.next_order = state.next_order.saturating_add(1);
            state.entries.insert(key, entry);
        }
        Ok(())
    }

    /// 业务作用：由受控上下文登记一个 initializer 拥有的普通资源。
    ///
    /// 参数说明：
    /// - `initializer`：冻结计划中的 canonical 所有者身份。
    /// - `qualifier`：同类型多实例的可选名称。
    /// - `value`：所有权交给容器的线程安全值。
    ///
    /// 返回：登记成功后可供后续 initializer 借用；重名或封口后返回错误。
    pub(crate) fn register_initializer<T>(
        &self,
        initializer: Arc<str>,
        qualifier: Option<&str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        let qualifier = qualifier.map(normalize_qualifier).transpose()?;
        self.register_inner(
            qualifier,
            value,
            ResourceOwner::Initializer(initializer),
            None,
        )
    }

    /// 业务作用：由受控上下文登记一个 initializer 拥有的受管资源。
    ///
    /// 参数说明：
    /// - `initializer`：冻结计划中的 canonical 所有者身份。
    /// - `qualifier`：同类型多实例的可选名称。
    /// - `value`：需要显式异步 shutdown 的资源。
    ///
    /// 返回：登记成功后纳入对应 initializer 的逆序清理；否则返回错误。
    pub(crate) fn register_initializer_managed<T>(
        &self,
        initializer: Arc<str>,
        qualifier: Option<&str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: ManagedResource,
    {
        let qualifier = qualifier.map(normalize_qualifier).transpose()?;
        self.register_inner(
            qualifier,
            value,
            ResourceOwner::Initializer(initializer),
            Some(shutdown_managed::<T>),
        )
    }

    /// 业务作用：在业务初始化成功且任务登记关闭后封存资源 key 集合。
    ///
    /// # 参数
    ///
    /// 本方法无参数；封存后只允许已有资源借用。
    pub(crate) fn seal(&self) -> ApplicationResult<()> {
        let mut state = write_unpoisoned(&self.state);
        if state.phase != ResourcePhase::Open {
            return Err(resource_error(format!(
                "cannot seal resource registry in {:?} phase",
                state.phase
            )));
        }
        state.phase = ResourcePhase::Sealed;
        Ok(())
    }

    /// 业务作用：按逆登记顺序清理所有业务资源。
    ///
    /// # 参数
    ///
    /// - `context`：限制所有锁等待和 managed shutdown 的全局清理上下文。
    pub(crate) async fn shutdown_business(
        &self,
        context: &ShutdownContext,
    ) -> Vec<ApplicationError> {
        self.shutdown_matching(ResourceOwner::Business, context)
            .await
    }

    /// 业务作用：按逆登记顺序清理指定组件拥有的资源。
    ///
    /// # 参数
    ///
    /// - `component`：需要移除资源的组件身份。
    /// - `context`：限制锁等待和 managed shutdown 的全局清理上下文。
    pub(crate) async fn shutdown_component(
        &self,
        component: ComponentId,
        context: &ShutdownContext,
    ) -> Vec<ApplicationError> {
        self.shutdown_matching(ResourceOwner::Component(component), context)
            .await
    }

    /// 业务作用：按逆登记顺序清理指定 initializer 拥有的资源。
    ///
    /// 参数说明：
    /// - `initializer`：需要移除资源的 canonical 所有者身份。
    /// - `context`：限制锁等待和 managed shutdown 的全局清理上下文。
    ///
    /// 返回：所有可观测的清理失败，单项失败不会中断后续清理。
    pub(crate) async fn shutdown_initializer(
        &self,
        initializer: Arc<str>,
        context: &ShutdownContext,
    ) -> Vec<ApplicationError> {
        self.shutdown_matching(ResourceOwner::Initializer(initializer), context)
            .await
    }

    /// 业务作用：把注册表置为 Closed 并释放尚未移除的容器所有权。
    ///
    /// 参数说明：无。
    ///
    /// 返回：先关闭登记与借用，再在注册表锁外释放条目；析构异常由所有权守卫单独报告。
    pub(crate) fn close(&self) {
        let entries = {
            let mut state = write_unpoisoned(&self.state);
            // 先关闭新登记和借用，再移交所有权；业务析构不能在持有注册表写锁时重入。
            state.phase = ResourcePhase::Closed;
            std::mem::take(&mut state.entries)
        };
        drop(entries);
    }

    /// 业务作用：直接取消且任务尚未释放时立即撤销新借用，但保留资源供已有任务安全析构。
    ///
    /// 参数说明：无。
    ///
    /// 返回：登记与借用均关闭，条目所有权仍按后续 active stack 步骤释放；Closed 不会被重新开放。
    pub(crate) fn close_borrowing(&self) {
        let mut state = write_unpoisoned(&self.state);
        // 必须先封住排队查找的复验边界，再把资源释放责任移交给任务存活守卫。
        if state.phase != ResourcePhase::Closed {
            state.phase = ResourcePhase::Closing;
        }
    }

    /// 业务作用：在 Runner 直接取消时按当前激活步骤同步归还指定所有者的资源。
    ///
    /// 参数说明：
    /// - `owner`：当前步骤拥有的资源集合，不影响更早步骤仍需持有的其它资源。
    ///
    /// 返回：撤销本批次 key 后在锁外按逆登记顺序释放容器所有权，不调用异步 shutdown；
    /// 已有借用可延迟最终析构，单项析构异常由资源守卫隔离并同步告警。
    pub(crate) fn release_owner(&self, owner: ResourceOwner) {
        for entry in self.take_matching(owner) {
            drop(entry);
        }
    }

    /// 业务作用：在同一写锁临界区完成阶段检查、重复检查和顺序号分配。
    ///
    /// 参数说明：
    /// - `qualifier`：已经规范化的可选资源名称。
    /// - `value`：要擦除类型并交给注册表的资源所有权。
    /// - `owner`：决定该条目进入哪个 active 清理步骤的所有者。
    /// - `shutdown`：受管资源的类型擦除清理函数，普通资源为 `None`。
    ///
    /// 返回：成功时发布携带析构隔离守卫的资源；阶段关闭或 key 重复时不接管资源。
    fn register_inner<T>(
        &self,
        qualifier: Option<Arc<str>>,
        value: T,
        owner: ResourceOwner,
        shutdown: Option<ErasedShutdown>,
    ) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        let mut state = write_unpoisoned(&self.state);
        if state.phase != ResourcePhase::Open {
            return Err(resource_error(format!(
                "resource registration is closed in {:?} phase",
                state.phase
            )));
        }

        let key = ResourceKey::of::<T>(qualifier);
        if state.entries.contains_key(&key) {
            return Err(resource_error(format!(
                "resource `{}` is already registered",
                type_name::<T>()
            )));
        }

        let registration_order = state.next_order;
        state.next_order = state.next_order.saturating_add(1);
        state.entries.insert(
            key,
            ResourceEntry {
                type_name: type_name::<T>(),
                value: Arc::new(AsyncRwLock::new(ResourceCleanup::new(
                    Box::new(value),
                    type_name::<T>(),
                    "releasing its value",
                ))),
                registration_order,
                owner,
                shutdown,
            },
        );
        Ok(())
    }

    /// 业务作用：解析资源 key、克隆内部锁并映射为目标类型只读守卫。
    ///
    /// 参数说明：
    /// - `qualifier`：已经规范化的可选资源名称。
    ///
    /// 返回：取得仍有效的资源借用；未登记、已关闭或等待期间被清理的资源返回错误。
    async fn get_inner<T>(
        &self,
        qualifier: Option<Arc<str>>,
    ) -> ApplicationResult<ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        let key = ResourceKey::of::<T>(qualifier);
        let value = {
            let state = read_unpoisoned(&self.state);
            if matches!(state.phase, ResourcePhase::Closing | ResourcePhase::Closed) {
                return Err(resource_error(format!(
                    "resource lookup is closed in {:?} phase",
                    state.phase
                )));
            }
            state
                .entries
                .get(&key)
                .map(|entry| entry.value.clone())
                .ok_or_else(|| {
                    resource_error(format!("resource `{}` is not registered", type_name::<T>()))
                })?
        };

        let guard = value.clone().read_owned().await;
        // 等待读锁期间 owner 可能已撤销 key，或全局已进入 Closing；必须复验同一条目的
        // 发布权，不能仅凭仍存活的 Arc 或尚未释放的资源值向调用方发出新借用。
        let still_published = {
            let state = read_unpoisoned(&self.state);
            !matches!(state.phase, ResourcePhase::Closing | ResourcePhase::Closed)
                && state
                    .entries
                    .get(&key)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.value, &value))
        };
        if !still_published {
            return Err(resource_error(format!(
                "resource `{}` was withdrawn while acquiring its borrow",
                type_name::<T>()
            )));
        }
        let guard = OwnedRwLockReadGuard::try_map(guard, |erased| {
            erased
                .value
                .as_ref()
                .and_then(|value| value.downcast_ref::<T>())
        })
        .map_err(|_| {
            resource_error(format!(
                "resource `{}` failed its internal type check",
                type_name::<T>()
            ))
        })?;
        Ok(ResourceRef {
            guard,
            application_lifetime: PhantomData,
        })
    }

    /// 业务作用：为正常停机与直接取消提供同一资源撤销边界，按逆登记顺序移交指定所有者条目。
    ///
    /// 参数说明：
    /// - `owner`：本次 active step 负责清理的资源所有者。
    ///
    /// 返回：已在写锁内撤销 key 的逆序条目；局部步骤保留其它资源查找，业务资源步骤关闭全部新借用，
    /// 已关闭时返回空集合。调用者在锁外决定异步清理或同步释放，已有借用不会被强制撤销。
    fn take_matching(&self, owner: ResourceOwner) -> Vec<ResourceEntry> {
        let mut entries = {
            let mut state = write_unpoisoned(&self.state);
            // 已关闭时不能重新取得清理权，也不能退回允许借用的阶段。
            if state.phase == ResourcePhase::Closed {
                return vec![];
            }
            // 局部 owner 清理只撤销自己的 key，业务停机任务仍需借用其它资源。
            // 启动中止时也先封住登记；仅业务资源步骤取得全局关闭权，且不重新开放既有 Closing。
            if owner == ResourceOwner::Business {
                state.phase = ResourcePhase::Closing;
            } else if state.phase == ResourcePhase::Open {
                state.phase = ResourcePhase::Sealed;
            }
            let keys = state
                .entries
                .iter()
                .filter_map(|(key, entry)| (entry.owner == owner).then_some(key.clone()))
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| state.entries.remove(&key))
                .collect::<Vec<_>>()
        };
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.registration_order));
        entries
    }

    /// 业务作用：原子移除指定所有者条目，再在锁外按逆序执行显式清理。
    ///
    /// 先从 key 集合移除可以关闭新查找；随后写锁等待保证已有 ResourceRef 先释放。
    ///
    /// 参数说明：
    /// - `owner`：本次 active step 负责清理的资源所有者。
    /// - `context`：所有条目共享的绝对清理预算。
    ///
    /// 返回：按逆序收集锁等待、显式清理失败、单项超时与展开式 panic；单项异常不截断同批次后续清理。
    async fn shutdown_matching(
        &self,
        owner: ResourceOwner,
        context: &ShutdownContext,
    ) -> Vec<ApplicationError> {
        let entries = self.take_matching(owner);

        let entry_count = entries.len();
        let mut failures = Vec::new();
        for (index, entry) in entries.into_iter().enumerate() {
            // 每个资源都是公开扩展点；锁等待与显式清理必须共用提前截止的子上下文，
            // 公平份额同时为同批次后续条目与后续 active step 保留实际执行机会。
            let remaining_entries = entry_count.saturating_sub(index).max(1);
            let fair_share =
                context.remaining() / u32::try_from(remaining_entries).unwrap_or(u32::MAX);
            let entry_context = context.child_context(fair_share);
            let mut value =
                match timeout(entry_context.remaining(), entry.value.clone().write_owned()).await {
                    Ok(value) => value,
                    Err(_) => {
                        failures.push(ApplicationError::new(
                            ComponentId::Resources,
                            ApplicationPhase::Stopping,
                            format!(
                                "timed out waiting for outstanding borrows of resource `{}`",
                                entry.type_name
                            ),
                        ));
                        continue;
                    }
                };
            if let Some(shutdown) = entry.shutdown {
                failures.extend(
                    shutdown_resource(shutdown, &mut value, &entry_context, entry.type_name).await,
                );
            }
            // 清理 future 先结束并释放借用，才释放资源值；单项析构异常不能跳过后续条目。
            if value.release() {
                failures.push(resource_panic_error(entry.type_name, "releasing its value"));
            }
        }
        failures
    }
}

/// 业务作用：分别隔离受管资源的 future 创建、poll 与释放，使任一业务展开都成为次要失败。
///
/// 参数说明：
/// - `shutdown`：资源类型对应的同步 future 工厂。
/// - `value`：写锁独占保护下的资源值。
/// - `context`：创建与执行共用的资源绝对清理期限。
/// - `type_name`：用于稳定归因的编译期类型名。
///
/// 返回：保留原始清理错误或超时，并附加独立的析构异常；取消时守卫仍释放 future 并单独报告异常。
async fn shutdown_resource(
    shutdown: ErasedShutdown,
    value: &mut ErasedResource,
    context: &ShutdownContext,
    type_name: &'static str,
) -> Vec<ApplicationError> {
    // 工厂本身也是同步业务回调，必须在 future 存在之前建立展开边界。
    let future = match catch_unwind(AssertUnwindSafe(move || {
        let owned_borrow = value;
        shutdown(owned_borrow, context)
    })) {
        Ok(future) => future,
        Err(payload) => {
            crate::shutdown::release_shutdown_panic_payload(payload);
            return vec![resource_panic_error(
                type_name,
                "creating its shutdown future",
            )];
        }
    };
    let mut future = ResourceCleanup::new(future, type_name, "releasing its shutdown future");
    let mut failures = Vec::new();
    // timeout 与 catch_unwind 只借用 future；完成、超时及 poll panic 均不在包装器析构时释放业务捕获对象。
    let result = timeout(
        context.remaining(),
        AssertUnwindSafe(future.value.as_mut().expect("shutdown future is owned")).catch_unwind(),
    )
    .await;
    match result {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => failures.push(ApplicationError::with_source(
            ComponentId::Resources,
            ApplicationPhase::Stopping,
            format!("failed to stop resource `{type_name}`"),
            error,
        )),
        Ok(Err(payload)) => {
            crate::shutdown::release_shutdown_panic_payload(payload);
            failures.push(resource_panic_error(
                type_name,
                "polling its shutdown future",
            ));
        }
        Err(_) => failures.push(ApplicationError::new(
            ComponentId::Resources,
            ApplicationPhase::Stopping,
            format!("resource `{type_name}` shutdown exceeded its derived deadline"),
        )),
    }
    // 正常返回路径将释放异常纳入同一报告；外层直接取消时则由守卫的 Drop 兜底。
    if future.release() {
        failures.push(resource_panic_error(
            type_name,
            "releasing its shutdown future",
        ));
    }
    failures
}

/// 业务作用：为资源清理的展开式异常生成不含 payload 的稳定次要失败。
///
/// 参数说明：
/// - `type_name`：编译期资源类型名。
/// - `operation`：框架固定的清理阶段。
///
/// 返回：Stopping 阶段的 Resources 错误，不携带资源值或异常对象。
fn resource_panic_error(type_name: &'static str, operation: &'static str) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Resources,
        ApplicationPhase::Stopping,
        format!("resource `{type_name}` panicked while {operation}"),
    )
}

/// 资源借用通过 owned mapped guard 绑定到 `Application` 的借用期，不泄露内部锁。
pub struct ResourceRef<'app, T: ?Sized> {
    guard: OwnedRwLockReadGuard<ErasedResource, T>,
    application_lifetime: PhantomData<&'app ()>,
}

impl<T: ?Sized> Deref for ResourceRef<'_, T> {
    type Target = T;

    /// 业务作用：返回 mapped read guard 指向的资源借用。
    ///
    /// # 参数
    ///
    /// 本方法无参数；借用不能越过 ResourceRef 生命周期。
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// 业务作用：把擦除类型资源恢复为目标类型并调用其显式 shutdown。
///
/// 参数说明：
/// - `erased`：写锁独占保护下的擦除资源。
/// - `context`：当前全局清理上下文。
///
/// 返回：资源仍存在且类型匹配时返回其清理 future；否则返回稳定的内部类型错误。
fn shutdown_managed<'a, T>(
    erased: &'a mut ErasedResource,
    context: &'a ShutdownContext,
) -> ApplicationFuture<'a>
where
    T: ManagedResource,
{
    match erased
        .value
        .as_mut()
        .and_then(|value| value.downcast_mut::<T>())
    {
        Some(resource) => resource.shutdown(context),
        None => Box::pin(async {
            Err(resource_error(
                "managed resource failed its internal type check",
            ))
        }),
    }
}

/// 业务作用：trim 并校验资源 qualifier。
///
/// # 参数
///
/// - `value`：调用方提供的资源名称。
fn normalize_qualifier(value: &str) -> ApplicationResult<Arc<str>> {
    let value = value.trim();
    if value.is_empty() {
        return Err(resource_error("resource qualifier cannot be empty"));
    }
    Ok(Arc::from(value))
}

/// 业务作用：创建资源容器的稳定运行期错误。
///
/// # 参数
///
/// - `message`：不包含资源值的错误摘要。
fn resource_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Resources, ApplicationPhase::Running, message)
}

/// 业务作用：从同步状态锁取得读守卫，并在先前 panic 污染时继续保护内部值。
///
/// # 参数
///
/// - `lock`：保护注册表结构状态的同步读写锁。
fn read_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 业务作用：从同步状态锁取得写守卫，并在先前 panic 污染时继续保护内部值。
///
/// # 参数
///
/// - `lock`：保护注册表结构状态的同步读写锁。
fn write_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
