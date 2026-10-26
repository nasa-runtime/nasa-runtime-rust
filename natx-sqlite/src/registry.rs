use crate::SqliteDataSource;
use natx_core::{
    observability, BootstrapKind, DataSourceCatalog, DataSourceLookupError, DatabaseDriver,
    DatasourceRef, ManagedInstallationToken, ManagedRegistryOwner, MAX_MANAGED_DATASOURCES,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
static MANAGED_DATASOURCES: OnceLock<RwLock<Weak<SqliteDataSourceRegistry>>> = OnceLock::new();
static STANDALONE: Mutex<BTreeMap<String, SqliteDataSource>> = Mutex::new(BTreeMap::new());

/// 业务作用：作为单个 Application 的冻结 datasource 表，统一事务、Mapper 与受管持久适配器的 pool 身份。
pub struct SqliteDataSourceRegistry {
    pools: BTreeMap<DatasourceRef, SqliteDataSource>,
    accepting: AtomicBool,
    owner: OnceLock<ManagedRegistryOwner>,
    catalog: OnceLock<Arc<DataSourceCatalog>>,
}

impl SqliteDataSourceRegistry {
    /// 业务作用：从启动期已建立的 pool 一次性构造冻结命名表。
    ///
    /// 参数说明：
    /// - `pools` 为 qualifier 与已受管连接池的集合。
    ///
    /// 返回：名称全部合法、不重复且数量有界时返回 registry；失败时不发布部分表。
    pub fn try_new(
        pools: impl IntoIterator<Item = (String, SqliteDataSource)>,
    ) -> anyhow::Result<Self> {
        let mut registry = BTreeMap::new();
        for (name, pool) in pools {
            if registry.len() >= MAX_MANAGED_DATASOURCES {
                anyhow::bail!(
                    "managed datasource count exceeds the supported limit of {MAX_MANAGED_DATASOURCES}"
                );
            }
            let reference = DatasourceRef::new(name)?;
            anyhow::ensure!(
                pool.name() == reference.as_str(),
                "SQLite source name differs from registry key"
            );
            if registry.insert(reference.clone(), pool).is_some() {
                anyhow::bail!("datasource `{reference}` is configured more than once");
            }
        }
        anyhow::ensure!(
            !registry.is_empty(),
            "managed datasource registry cannot be empty"
        );
        for name in registry.keys() {
            observability::register_datasource(DatabaseDriver::Sqlite, name.as_str())?;
        }
        Ok(Self {
            pools: registry,
            accepting: AtomicBool::new(true),
            owner: OnceLock::new(),
            catalog: OnceLock::new(),
        })
    }

    /// 业务作用：为 Application 构造绑定 owner、尚未对 getter 开放的 SQLite registry。
    ///
    /// 参数说明：
    /// - `pools`：启动期已经建立完成的 SQLite pool 集合。
    /// - `token`：本次 Application 从 `natx-core` 领取的安装 token。
    ///
    /// 返回：名称合法且 owner 唯一时返回关闭态 registry；失败时不发布任何全局状态。
    pub fn try_new_managed(
        pools: impl IntoIterator<Item = (String, SqliteDataSource)>,
        token: &ManagedInstallationToken,
    ) -> anyhow::Result<Self> {
        let registry = Self::try_new(pools)?;
        // 启动 staging 先封口 typed getter，避免 owner/catalog 尚未绑定时暴露部分资源表。
        registry.accepting.store(false, Ordering::Release);
        registry
            .owner
            .set(token.owner().clone())
            .map_err(|_| anyhow::anyhow!("managed registry owner is already bound"))?;
        Ok(registry)
    }

    /// 业务作用：在 registry 尚未停机时选择指定连接池。
    ///
    /// 参数说明：
    /// - `datasource` 是已校验或配置边界传入的 qualifier。
    ///
    /// 返回：运行期内命中时返回同一 pool 的 clone；停机或名称缺失时失败且不回退默认库。
    pub fn datasource(&self, datasource: &str) -> Result<SqliteDataSource, DataSourceLookupError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(DataSourceLookupError::Stopped);
        }
        let reference =
            DatasourceRef::new(datasource).map_err(|_| DataSourceLookupError::InvalidName)?;
        if self.owner.get().is_some() {
            let expected = self
                .catalog
                .get()
                .ok_or(DataSourceLookupError::RegistryUnavailable)?;
            let active = natx_core::managed_catalog()?;
            if !Arc::ptr_eq(expected, &active)
                || !active.owner().ptr_eq(
                    self.owner
                        .get()
                        .ok_or(DataSourceLookupError::RegistryUnavailable)?,
                )
            {
                return Err(DataSourceLookupError::RegistryUnavailable);
            }
        }
        self.pools
            .get(&reference)
            .cloned()
            .ok_or(DataSourceLookupError::NotFound {
                datasource: reference,
            })
    }

    /// 业务作用：确认命名 datasource 是否属于该冻结表。
    ///
    /// 参数说明：
    /// - `datasource` 是待复验的 qualifier。
    ///
    /// 返回：registry 尚开放且名称存在时返回真。
    pub fn contains(&self, datasource: &str) -> bool {
        if !self.accepting.load(Ordering::Acquire) {
            return false;
        }
        if self.owner.get().is_some()
            && !self.catalog.get().is_some_and(|expected| {
                natx_core::managed_catalog()
                    .ok()
                    .is_some_and(|active| Arc::ptr_eq(expected, &active))
            })
        {
            return false;
        }
        DatasourceRef::new(datasource)
            .ok()
            .is_some_and(|reference| self.pools.contains_key(&reference))
    }

    /// 业务作用：在连接池关闭前封口新的事务与连接选择。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；已借出的 pool 仍由 Application 停机预算排空。
    pub fn stop_accepting(&self) {
        // 先撤销本 typed registry 的连接选择；即使共享 catalog 仍可见，普通 getter 也不能绕过本门禁。
        self.accepting.store(false, Ordering::Release);
    }

    /// 业务作用：复验 registry、catalog 与安装 token 的 owner 身份后开放 typed 标志。
    ///
    /// 参数说明：
    /// - `token`：必须属于构造该 registry 的 Application。
    ///
    /// 返回：身份一致时开放 typed 标志，业务 getter 仍受共享 catalog 开放及实例一致性检查约束；错配时不修改标志。
    pub fn start_accepting(&self, token: &ManagedInstallationToken) -> anyhow::Result<()> {
        let owner = self
            .owner
            .get()
            .ok_or_else(|| anyhow::anyhow!("managed registry owner is not bound"))?;
        // 安装 token 必须属于本 registry 的 Application，不能让其它实例取得该资源表的开放权。
        anyhow::ensure!(
            owner.ptr_eq(token.owner()),
            "managed registry owner does not match installation token"
        );
        let catalog = self
            .catalog
            .get()
            .ok_or_else(|| anyhow::anyhow!("managed datasource catalog is not bound"))?;
        // catalog 与 typed registry 必须共享 owner，避免把一张资源表绑定到另一实例的发布权威。
        anyhow::ensure!(
            catalog.owner().ptr_eq(token.owner()),
            "managed datasource catalog owner does not match installation token"
        );
        // 这里只开放 typed 标志；共享 catalog 尚未最终开放或 Arc/owner 不匹配时，普通 getter 仍不可见。
        self.accepting.store(true, Ordering::Release);
        Ok(())
    }

    /// 业务作用：读取 registry 绑定的 Application owner，供按身份撤销全局槽。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已安装或以 managed 构造时返回 owner；普通未安装 registry 返回 `None`。
    pub fn managed_owner(&self) -> Option<&ManagedRegistryOwner> {
        self.owner.get()
    }

    /// 业务作用：生成该 SQLite registry 对应的 catalog 条目。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 datasource 名排序且 driver 固定为 SQLite 的拥有型条目。
    pub fn catalog_entries(&self) -> Vec<(String, DatabaseDriver)> {
        self.pools
            .keys()
            .map(|reference| (reference.as_str().to_owned(), DatabaseDriver::Sqlite))
            .collect()
    }

    /// 业务作用：让持有同 owner token 的 Application staging 读取关闭态 pool 以登记资源和执行门禁。
    ///
    /// 参数说明：
    /// - `datasource`：待读取的 SQLite datasource。
    /// - `token`：当前 Application 安装 token。
    ///
    /// 返回：owner 与名称都匹配时返回 pool clone；该入口不开放普通业务 getter。
    #[doc(hidden)]
    pub fn managed_datasource(
        &self,
        datasource: &str,
        token: &ManagedInstallationToken,
    ) -> anyhow::Result<SqliteDataSource> {
        let owner = self
            .owner
            .get()
            .ok_or_else(|| anyhow::anyhow!("managed registry owner is not bound"))?;
        anyhow::ensure!(
            owner.ptr_eq(token.owner()),
            "managed registry owner does not match installation token"
        );
        let reference = DatasourceRef::new(datasource)?;
        self.pools
            .get(&reference)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("datasource `{reference}` is not in this registry"))
    }

    /// 业务作用：把 SQLite typed registry 绑定到唯一 Application owner，阻止跨实例复用资源表。
    ///
    /// 参数说明：
    /// - `owner` 是构造 catalog 与其它 typed registry 共同使用的身份。
    ///
    /// 返回：首次绑定或同身份重复绑定成功；另一 owner 已占用时保持原身份并失败。
    fn bind_owner(&self, owner: &ManagedRegistryOwner) -> anyhow::Result<()> {
        if let Some(existing) = self.owner.get() {
            anyhow::ensure!(
                existing.ptr_eq(owner),
                "managed registry is already bound to another owner"
            );
            return Ok(());
        }
        self.owner
            .set(owner.clone())
            .map_err(|_| anyhow::anyhow!("managed registry owner binding raced"))
    }

    /// 业务作用：把 SQLite typed registry 绑定到同 owner 的共享 catalog，供开放前后复验全局权威。
    ///
    /// 参数说明：
    /// - `catalog`：跨 driver 完整 datasource catalog。
    /// - `token`：当前 Application 安装 token。
    ///
    /// 返回：owner 和 Arc 身份一致时成功；错配时不替换已有 catalog。
    fn bind_catalog(
        &self,
        catalog: &Arc<DataSourceCatalog>,
        token: &ManagedInstallationToken,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            catalog.owner().ptr_eq(token.owner()),
            "managed catalog owner does not match installation token"
        );
        if let Some(existing) = self.catalog.get() {
            anyhow::ensure!(
                Arc::ptr_eq(existing, catalog),
                "managed registry is already bound to another catalog"
            );
            return Ok(());
        }
        self.catalog
            .set(catalog.clone())
            .map_err(|_| anyhow::anyhow!("managed catalog binding raced"))
    }
}

/// 业务作用：发布当前 Application 拥有的唯一 datasource registry。
///
/// 参数说明：
/// - `registry` 是 DB 组件在全部数据源建立成功后冻结的命名表。
///
/// 返回：当前进程没有独立注册表且没有其它 live Application 时成功；冲突时保留原权威并失败。
pub fn install_managed_registry(registry: &Arc<SqliteDataSourceRegistry>) -> anyhow::Result<()> {
    bind_sources(registry)?;
    let owner = registry
        .managed_owner()
        .cloned()
        .unwrap_or_else(ManagedRegistryOwner::new);
    registry.bind_owner(&owner)?;
    let catalog = match registry.catalog.get() {
        Some(catalog) => catalog.clone(),
        None => {
            let catalog = Arc::new(DataSourceCatalog::try_new(
                owner.clone(),
                registry.catalog_entries(),
            )?);
            registry
                .catalog
                .set(catalog.clone())
                .map_err(|_| anyhow::anyhow!("managed catalog binding raced"))?;
            catalog
        }
    };

    natx_core::coordinate(|coordinator| {
        ensure_local_standalone_empty()?;
        let token = coordinator
            .begin_bootstrap(&owner, BootstrapKind::Configured)
            .map_err(anyhow::Error::new)?;
        if let Err(error) = install_managed_slot_locked(registry) {
            let _ = coordinator.abort_bootstrap(&token);
            return Err(error);
        }
        registry.bind_catalog(&catalog, &token)?;
        if let Err(error) = coordinator.install_catalog(&token, &catalog) {
            clear_managed_slot_locked(registry);
            let _ = coordinator.abort_bootstrap(&token);
            return Err(anyhow::Error::new(error));
        }
        if let Err(error) = registry.start_accepting(&token) {
            clear_managed_slot_locked(registry);
            let _ = coordinator.clear_managed(&owner);
            return Err(error);
        }
        if let Err(error) = coordinator.open_catalog(&token, &catalog) {
            registry.stop_accepting();
            clear_managed_slot_locked(registry);
            let _ = coordinator.clear_managed(&owner);
            return Err(anyhow::Error::new(error));
        }
        Ok(())
    })
}

/// 业务作用：为 napp 领取受管 datasource owner 与安装 token，但不创建或开放连接池。
///
/// 参数说明：
/// - `kind` 区分配置建池与 UserHook 延后 default。
///
/// 返回：进程入口为空时进入 Bootstrapping；已有 standalone 或 managed 权威时失败。
#[doc(hidden)]
pub fn begin_managed_bootstrap(
    kind: BootstrapKind,
) -> anyhow::Result<(ManagedRegistryOwner, ManagedInstallationToken)> {
    natx_core::begin_managed_bootstrap(kind).map_err(anyhow::Error::new)
}

/// 业务作用：把关闭态 SQLite registry 安装到当前 owner 的 typed 槽，但不开放全局 getter。
///
/// 参数说明：
/// - `registry`：由 [`SqliteDataSourceRegistry::try_new_managed`] 构造的关闭态表。
/// - `token`：当前 Application 安装 token。
///
/// 返回：owner 匹配、无 standalone 且 typed 槽空闲时成功；失败不改变原权威。
#[doc(hidden)]
pub fn install_closed_managed_registry(
    registry: &Arc<SqliteDataSourceRegistry>,
    token: &ManagedInstallationToken,
) -> anyhow::Result<()> {
    registry.bind_owner(token.owner())?;
    bind_sources(registry)?;
    natx_core::coordinate(|coordinator| {
        coordinator
            .verify_installation_token(token)
            .map_err(anyhow::Error::new)?;
        ensure_local_standalone_empty()?;
        install_managed_slot_locked(registry)
    })
}

/// 业务作用：把共享 catalog 绑定到已安装的 SQLite registry，保证资源持有同一 owner。
///
/// 参数说明：
/// - `registry`：当前 Application 的 SQLite typed registry。
/// - `catalog`：跨 driver 完整 catalog。
/// - `token`：当前 Application 安装 token。
///
/// 返回：三者 owner 一致时成功；错配时 registry 保持关闭。
#[doc(hidden)]
pub fn attach_managed_catalog(
    registry: &Arc<SqliteDataSourceRegistry>,
    catalog: &Arc<DataSourceCatalog>,
    token: &ManagedInstallationToken,
) -> anyhow::Result<()> {
    registry.bind_catalog(catalog, token)
}

/// 业务作用：在共享 catalog 即将开放前开放 SQLite typed registry。
///
/// 参数说明：
/// - `registry`：已经绑定共享 catalog 的 registry。
/// - `token`：当前 Application 安装 token。
///
/// 返回：owner 匹配时开放 typed 标志，普通 getter 仍等待共享 catalog 开放；错配时不改变标志。
#[doc(hidden)]
pub fn open_managed_registry(
    registry: &Arc<SqliteDataSourceRegistry>,
    token: &ManagedInstallationToken,
) -> anyhow::Result<()> {
    registry.start_accepting(token)
}

/// 业务作用：在 catalog 封口后按 registry Arc 身份撤销 SQLite typed 槽。
///
/// 参数说明：
/// - `registry` 是正在停机或启动失败清理的资源表。
///
/// 返回：无；旧实例不命中当前槽时不修改新实例。
#[doc(hidden)]
pub fn clear_managed_registry_slot(registry: &Arc<SqliteDataSourceRegistry>) {
    natx_core::coordinate(|_| {
        registry.stop_accepting();
        clear_managed_slot_locked(registry);
    });
}

/// 业务作用：在全局数据源协调锁内发布唯一受管 registry。
///
/// 参数说明：
/// - `registry` 是已冻结且由 Application 持有的资源表。
///
/// 返回：独立入口为空且无其它 live registry 时成功；冲突时不改变全局槽。
fn install_managed_slot_locked(registry: &Arc<SqliteDataSourceRegistry>) -> anyhow::Result<()> {
    let slot = MANAGED_DATASOURCES.get_or_init(|| RwLock::new(Weak::new()));
    let mut current = slot.write().unwrap_or_else(|error| error.into_inner());
    if current.upgrade().is_some() {
        anyhow::bail!("another managed datasource registry is already active");
    }
    *current = Arc::downgrade(registry);
    Ok(())
}

/// 业务作用：按 Arc 身份清除 SQLite typed registry 弱槽，防止旧实例撤销新实例。
///
/// 参数说明：
/// - `registry` 是正在失败清理或停机的 registry。
///
/// 返回：无；槽为空或身份不匹配时保持当前状态。
fn clear_managed_slot_locked(registry: &Arc<SqliteDataSourceRegistry>) {
    let Some(slot) = MANAGED_DATASOURCES.get() else {
        return;
    };
    let mut current = slot.write().unwrap_or_else(|error| error.into_inner());
    if current
        .upgrade()
        .is_some_and(|active| Arc::ptr_eq(&active, registry))
    {
        *current = Weak::new();
    }
}

/// 业务作用：仅在全局槽仍指向目标 Application registry 时封口并撤销发布。
///
/// 参数说明：
/// - `registry` 是正在进入逆序停机的资源表。
///
/// 返回：无；指针复验防止旧 Application 误清理后续实例的槽位。
pub fn clear_managed_registry(registry: &Arc<SqliteDataSourceRegistry>) {
    natx_core::coordinate(|coordinator| {
        registry.stop_accepting();
        clear_managed_slot_locked(registry);
        if let Some(owner) = registry.managed_owner() {
            let _ = coordinator.clear_managed(owner);
        }
    });
}

/// 业务作用：检查 standalone 与 Application 两种资源权威是否冲突。
/// 参数说明：无。
/// 返回：独立注册表为空才允许受管安装。
fn ensure_local_standalone_empty() -> anyhow::Result<()> {
    anyhow::ensure!(
        STANDALONE
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty(),
        "standalone SQLite registry is already initialized"
    );
    Ok(())
}

/// 业务作用：安装 standalone 数据源使用句柄，关闭责任仍由调用方持有。
/// 参数说明：
/// - `source`：已经完成打开验证的数据源。
/// 返回：名称首次注册且进程模式一致时成功；不会替换已有来源。
pub fn try_init_datasource(source: SqliteDataSource) -> anyhow::Result<()> {
    natx_core::coordinate(|coordinator| {
        let mut sources = STANDALONE.lock().unwrap_or_else(|error| error.into_inner());
        anyhow::ensure!(
            !sources.contains_key(source.name()),
            "SQLite datasource is already registered"
        );
        coordinator.register_standalone(DatabaseDriver::Sqlite, source.name())?;
        sources.insert(source.name().to_owned(), source);
        Ok(())
    })
}

/// 业务作用：通过共享 catalog 和 typed registry 取得一致的数据源身份。
/// 参数说明：
/// - `name`：业务选择的 datasource 名称。
/// 返回：名称、driver 与生命周期均匹配时返回句柄；绝不回退默认来源。
pub fn datasource_for(name: &str) -> Result<SqliteDataSource, DataSourceLookupError> {
    let reference = natx_core::resolve_datasource(name, DatabaseDriver::Sqlite)?;
    if let Some(registry) = MANAGED_DATASOURCES.get().and_then(|slot| {
        slot.read()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
    }) {
        return registry.datasource(reference.as_str());
    }
    STANDALONE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(reference.as_str())
        .cloned()
        .ok_or(DataSourceLookupError::NotFound {
            datasource: reference,
        })
}

/// 业务作用：让直接持有的 source 也遵循受管目录的开放与停机门禁。
/// 参数说明：
/// - `registry`：即将发布的唯一来源表。
/// 返回：所有来源都绑定同一 owner 后成功；来源被其它目录占用时拒绝。
fn bind_sources(registry: &Arc<SqliteDataSourceRegistry>) -> anyhow::Result<()> {
    for source in registry.pools.values() {
        if let Some(existing) = source.0.registry.get() {
            anyhow::ensure!(
                existing
                    .upgrade()
                    .is_some_and(|existing| Arc::ptr_eq(&existing, registry)),
                "SQLite datasource belongs to another registry"
            );
        } else {
            source
                .0
                .registry
                .set(Arc::downgrade(registry))
                .map_err(|_| anyhow::anyhow!("SQLite datasource registry binding raced"))?;
        }
    }
    registry.stop_accepting();
    Ok(())
}
