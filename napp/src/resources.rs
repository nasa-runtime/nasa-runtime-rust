use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, RwLock},
};

use tokio::sync::{OwnedRwLockReadGuard, RwLock as AsyncRwLock};

use crate::{
    ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult, ComponentId,
    ShutdownContext,
};

type ErasedResource = Box<dyn Any + Send + Sync>;
type ErasedShutdown =
    for<'a> fn(&'a mut ErasedResource, &'a ShutdownContext) -> ApplicationFuture<'a>;

/// 需要异步释放的业务资源。`Drop` 必须保持非阻塞；需要等待的清理由此方法完成。
pub trait ManagedResource: Send + Sync + 'static {
    /// 业务作用：在全局剩余预算内完成需要等待的资源清理。
    ///
    /// # 参数
    ///
    /// - `context`：携带首次停机原因和绝对 deadline 的清理上下文。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a>;
}

/// 资源注册表对登记和新借用开放程度的生命周期阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourcePhase {
    /// 允许组件登记资源和调用方借用资源。
    Open,
    /// 禁止继续登记,但仍允许借用已发布资源。
    Sealed,
    /// 正在关闭,拒绝新的资源借用。
    Closing,
    /// 资源清理已经完成。
    Closed,
}

/// 区分框架组件资源和 UserHook 手工登记业务资源的所有者。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceOwner {
    Component(ComponentId),
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

/// 在同一同步锁下维护资源阶段、顺序号和 key 集合的一致状态。
struct RegistryState {
    phase: ResourcePhase,
    next_order: u64,
    entries: HashMap<ResourceKey, ResourceEntry>,
}

/// Application 拥有的类型资源容器，支持 UserHook 登记、运行期只读借用和逆序清理。
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

    /// 业务作用：在 UserHook 成功且任务登记关闭后封存资源 key 集合。
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

    /// 业务作用：把注册表置为 Closed 并释放尚未移除的容器所有权。
    ///
    /// # 参数
    ///
    /// 本方法无参数；调用前受监督任务应已退出，Drop 必须保持非阻塞。
    pub(crate) fn close(&self) {
        let mut state = write_unpoisoned(&self.state);
        state.phase = ResourcePhase::Closed;
        state.entries.clear();
    }

    /// 业务作用：在同一写锁临界区完成阶段检查、重复检查和顺序号分配。
    ///
    /// # 参数
    ///
    /// - `qualifier`：已经规范化的可选资源名称。
    /// - `value`：要擦除类型并交给注册表的资源所有权。
    /// - `owner`：决定该条目进入哪个 active 清理步骤的所有者。
    /// - `shutdown`：受管资源的类型擦除清理函数，普通资源为 `None`。
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
                value: Arc::new(AsyncRwLock::new(Box::new(value))),
                registration_order,
                owner,
                shutdown,
            },
        );
        Ok(())
    }

    /// 业务作用：解析资源 key、克隆内部锁并映射为目标类型只读守卫。
    ///
    /// # 参数
    ///
    /// - `qualifier`：已经规范化的可选资源名称。
    async fn get_inner<T>(
        &self,
        qualifier: Option<Arc<str>>,
    ) -> ApplicationResult<ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
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
                .get(&ResourceKey::of::<T>(qualifier))
                .map(|entry| entry.value.clone())
                .ok_or_else(|| {
                    resource_error(format!("resource `{}` is not registered", type_name::<T>()))
                })?
        };

        let guard = value.read_owned().await;
        let guard = OwnedRwLockReadGuard::try_map(guard, |erased| erased.downcast_ref::<T>())
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

    /// 业务作用：原子移除指定所有者条目，再在锁外按逆序执行显式清理。
    ///
    /// 先从 key 集合移除可以关闭新查找；随后写锁等待保证已有 ResourceRef 先释放。
    ///
    /// # 参数
    ///
    /// - `owner`：本次 active step 负责清理的资源所有者。
    /// - `context`：所有条目共享的绝对清理预算。
    async fn shutdown_matching(
        &self,
        owner: ResourceOwner,
        context: &ShutdownContext,
    ) -> Vec<ApplicationError> {
        let mut entries = {
            let mut state = write_unpoisoned(&self.state);
            if state.phase == ResourcePhase::Closed {
                return vec![];
            }
            state.phase = ResourcePhase::Closing;
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

        let mut failures = Vec::new();
        for entry in entries {
            let mut value = entry.value.clone().write_owned().await;
            if let Some(shutdown) = entry.shutdown {
                if let Err(error) = shutdown(&mut value, context).await {
                    failures.push(ApplicationError::with_source(
                        ComponentId::Resources,
                        ApplicationPhase::Stopping,
                        format!("failed to stop resource `{}`", entry.type_name),
                        error,
                    ));
                }
            }
        }
        failures
    }
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
/// # 参数
///
/// - `erased`：写锁独占保护下的擦除资源。
/// - `context`：当前全局清理上下文。
fn shutdown_managed<'a, T>(
    erased: &'a mut ErasedResource,
    context: &'a ShutdownContext,
) -> ApplicationFuture<'a>
where
    T: ManagedResource,
{
    match erased.downcast_mut::<T>() {
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
