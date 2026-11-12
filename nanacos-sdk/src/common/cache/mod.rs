use core::ops::{Deref, DerefMut};
use std::{borrow::Cow, collections::HashMap, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use dashmap::{
    DashMap,
    mapref::one::{Ref, RefMut},
};
use tracing::{Instrument, info};

use crate::common::cache::disk::DiskStore;

use super::executor;

mod disk;

pub(crate) struct Cache<V> {
    /// 当前作用域的共享内存目录，借用由分片锁保护。
    inner: Arc<DashMap<String, V>>,
    /// 可选完整值持久来源，异步保存不改变内存操作结果。
    store: Option<Arc<dyn Store<V>>>,
}

impl<V> Cache<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    /// 业务作用：建立内存缓存，并仅在显式启用时加载磁盘基线。
    ///
    /// 参数说明：
    /// - `store`：可选持久缓存来源。
    /// - `load_cache_at_start`：是否在构造期间读取缓存。
    ///
    /// 返回：可并发访问的缓存；磁盘缺失或不可用时保留空内存目录。
    async fn new(store: Option<Arc<dyn Store<V>>>, load_cache_at_start: bool) -> Self {
        let inner = match &store {
            Some(store) => {
                let map: HashMap<String, V> = if load_cache_at_start {
                    info!("Loading cache by {}", store.name());
                    store.load().await
                } else {
                    info!("Skip loading cache by {}", store.name());
                    HashMap::new()
                };

                let dash_map: DashMap<String, V> = DashMap::with_capacity(map.len());
                for (k, v) in map {
                    dash_map.insert(k, v);
                }
                Arc::new(dash_map)
            }
            None => {
                info!("Creating memory-only cache (no disk store)");
                Arc::new(DashMap::new())
            }
        };

        Self {
            inner,
            store,
        }
    }

    /// 业务作用：按完整逻辑键借用缓存，保护引用期间的读一致性。
    ///
    /// 参数说明：
    /// - `key`：当前作用域内的准确逻辑键。
    ///
    /// 返回：存在时返回受锁保护的借用，不复制缓存值；缺失返回 None。
    pub(crate) fn get(&self, key: &String) -> Option<CacheRef<'_, V>> {
        self.inner.get(key).map(|dash_map_ref| CacheRef {
            dash_map_ref,
        })
    }

    /// 业务作用：取得可修改缓存借用，释放时异步保存该键的最新完整值。
    ///
    /// 参数说明：
    /// - `key`：当前作用域内的准确逻辑键。
    ///
    /// 返回：存在时返回修改责任；缺失返回 None，不创建空记录。
    pub(crate) fn get_mut(&self, key: &String) -> Option<CacheRefMut<'_, V>> {
        self.inner.get_mut(key).map(|dash_map_ref_mut| CacheRefMut {
            dash_map_ref_mut,
            store: self.store.clone(),
            inner: self.inner.clone(),
        })
    }

    /// 业务作用：先安排完整快照的异步保存，再更新当前内存值。
    ///
    /// 参数说明：
    /// - `key`：不可歧义的逻辑键，不作为文件路径。
    /// - `value`：本次完整缓存内容。
    ///
    /// 返回：被替换的旧值或 None；磁盘任务没有业务版本排序，不作为提交确认。
    pub(crate) fn insert(&self, key: String, value: V) -> Option<V> {
        if let Some(ref store) = self.store
            && let Ok(bytes) = serde_json::to_vec(&value)
        {
            let store = store.clone();
            let key_str = key.clone();
            executor::spawn(async move { store.save(&key_str, bytes).await }.in_current_span());
        }

        self.inner.insert(key, value)
    }

    #[allow(dead_code)]
    /// 业务作用：移除失效逻辑键，并安排对应磁盘记录删除。
    ///
    /// 参数说明：
    /// - `key`：失效的完整逻辑键。
    ///
    /// 返回：内存旧值或 None；磁盘失败不恢复内存资格。
    pub(crate) fn remove(&self, key: &String) -> Option<V> {
        let ret = self.inner.remove(key)?;

        if let Some(ref store) = self.store {
            let key_str = ret.0.clone();
            let store = store.clone();

            executor::spawn(
                async move {
                    store.remove(&key_str).await;
                }
                .in_current_span(),
            );
        }

        Some(ret.1)
    }

    /// 业务作用：检查当前缓存是否存在准确身份，不触发加载或远程查询。
    ///
    /// 参数说明：
    /// - `key`：当前作用域内的完整逻辑键。
    ///
    /// 返回：存在为 true，不代表内容仍有实时资格。
    pub(crate) fn contains_key(&self, key: &String) -> bool {
        self.inner.contains_key(key)
    }

    /// 业务作用：在分片读保护下遍历缓存，避免为观察复制整个目录。
    ///
    /// 参数说明：
    /// - `f`：针对借用键和值执行的短回调，不得重入同一分片修改。
    ///
    /// 返回：回调逐项完成；遍历不是跨分片的原子快照。
    pub(crate) fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&String, &V),
    {
        for item in self.inner.iter() {
            f(item.key(), item.value());
        }
    }
}

pub(crate) struct CacheRef<'a, V> {
    /// 当前值的分片读责任，引用不能超出此借用。
    dash_map_ref: Ref<'a, String, V>,
}

impl<V> Deref for CacheRef<'_, V> {
    type Target = V;

    /// 业务作用：提供受缓存借用保护的值引用，不转移底层锁责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与当前借用同寿命的值引用。
    fn deref(&self) -> &Self::Target {
        self.dash_map_ref.value()
    }
}

pub(crate) struct CacheRefMut<'a, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    /// 当前值的独占修改责任，释放后安排最新快照。
    dash_map_ref_mut: RefMut<'a, String, V>,
    /// 可选完整值持久来源，异步保存不改变内存操作结果。
    store: Option<Arc<dyn Store<V>>>,
    /// 当前作用域的共享内存目录，借用由分片锁保护。
    inner: Arc<DashMap<String, V>>,
}

impl<V> Deref for CacheRefMut<'_, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    type Target = V;

    /// 业务作用：提供受缓存借用保护的值引用，不转移底层锁责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与当前借用同寿命的值引用。
    fn deref(&self) -> &Self::Target {
        self.dash_map_ref_mut.value()
    }
}

impl<V> DerefMut for CacheRefMut<'_, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    /// 业务作用：在唯一修改借用内开放缓存值修改。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可变值引用；借用释放后按当前内容安排持久保存。
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dash_map_ref_mut.value_mut()
    }
}

impl<V> Drop for CacheRefMut<'_, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    /// 业务作用：在修改责任结束后安排最新值快照，异步任务不持有原分片写锁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原借用释放；键已删除或序列化失败时不保存，也不重新插入该键。
    fn drop(&mut self) {
        if let Some(store) = self.store.take() {
            let inner = self.inner.clone();
            let key = self.dash_map_ref_mut.key().clone();

            executor::spawn(
                async move {
                    let bytes = inner
                        .get(&key)
                        .and_then(|data| serde_json::to_vec(data.value()).ok());
                    if let Some(bytes) = bytes {
                        store.save(&key, bytes).await;
                    }
                }
                .in_current_span(),
            );
        }
    }
}

pub(crate) struct CacheBuilder<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    /// 固定缓存值类型，构建器不提前持有实际业务值。
    _mark: PhantomData<V>,
    /// 原始逻辑命名空间，不作为磁盘路径片段。
    namespace: String,
    /// 配置与发现的隔离标识，防止不同值类型共享目录。
    module: String,
    /// 是否读取磁盘启动基线；不改变实时状态的权威来源。
    load_cache_at_start: bool,
    /// 可选完整值持久来源，异步保存不改变内存操作结果。
    store: Option<Arc<dyn Store<V>>>,
}

const CONFIG_MODULE: &str = "config";
const NAMING_MODULE: &str = "naming";

impl<V> CacheBuilder<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    /// 业务作用：创建配置模块的独立缓存作用域，默认不加载磁盘。
    ///
    /// 参数说明：
    /// - `namespace`：原始配置 namespace，不按路径解释。
    ///
    /// 返回：尚未绑定磁盘的构建器。
    pub(crate) fn config(namespace: String) -> Self {
        Self {
            _mark: Default::default(),
            namespace,
            module: CONFIG_MODULE.to_owned(),
            load_cache_at_start: false,
            store: None,
        }
    }

    /// 业务作用：创建发现模块的独立缓存作用域，避免与配置缓存混用。
    ///
    /// 参数说明：
    /// - `namespace`：原始发现 namespace，不按路径解释。
    ///
    /// 返回：默认不加载磁盘的构建器。
    pub(crate) fn naming(namespace: String) -> Self {
        Self {
            _mark: Default::default(),
            namespace,
            module: NAMING_MODULE.to_owned(),
            load_cache_at_start: false,
            store: None,
        }
    }

    /// 业务作用：显式选择是否在构造时读取已有磁盘基线。
    ///
    /// 参数说明：
    /// - `load_cache_at_start`：启动加载开关，不授予离线内容实时资格。
    ///
    /// 返回：保留其它设置的构建器。
    pub(crate) fn load_cache_at_start(self, load_cache_at_start: bool) -> Self {
        Self {
            load_cache_at_start,
            ..self
        }
    }

    /// 业务作用：在可信根下按模块和 namespace 摘要建立磁盘作用域。
    ///
    /// 参数说明：
    /// - `cache_dir`：部署者独占管理的根目录，省略时使用用户目录下 nacos。
    ///
    /// 返回：绑定磁盘的构建器；实际目录在访问时准备，业务名称不能改变路径根。
    pub(crate) fn disk_store(self, cache_dir: Option<std::path::PathBuf>) -> Self {
        let disk_path = cache_dir.unwrap_or_else(|| {
            std::path::PathBuf::from(crate::common::util::HOME_DIR.to_owned()).join("nacos")
        });

        let disk_store = Arc::new(DiskStore::new(disk_path, &self.module, &self.namespace));

        Self {
            store: Some(disk_store),
            ..self
        }
    }

    /// 业务作用：按已冻结的来源和启动策略创建缓存。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：完整内存缓存；启用预加载时等待加载结束，不执行远程授权。
    pub(crate) async fn build(self) -> Cache<V> {
        Cache::new(self.store, self.load_cache_at_start).await
    }
}

// 存储读写使用各自所需的类型约束，避免读取能力强迫调用方提供序列化实现。
#[async_trait]
trait Store<V>: Send + Sync {
    /// 业务作用：提供存储来源名称用于诊断，不公开业务键或凭据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定存储类型名称。
    fn name(&self) -> Cow<'_, str>;

    /// 业务作用：恢复可用的完整缓存基线，损坏记录不授予资格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功加载的逻辑键和值；不可用来源返回空集合。
    async fn load(&self) -> HashMap<String, V>
    where
        V: serde::de::DeserializeOwned;

    /// 业务作用：保存当前完整逻辑记录，业务键不能取得文件路径权限。
    ///
    /// 参数说明：
    /// - `key`：准确逻辑身份。
    /// - `value`：完整序列化缓存内容。
    ///
    /// 返回：保存结束；失败由实现记录，不回滚调用方内存状态。
    async fn save(&self, key: &str, value: Vec<u8>);

    /// 业务作用：删除准确逻辑记录，不允许业务名称越过存储作用域。
    ///
    /// 参数说明：
    /// - `key`：应删除的原逻辑键。
    ///
    /// 返回：删除结束；失败不恢复上层已撤销资格。
    async fn remove(&self, key: &str);
}
