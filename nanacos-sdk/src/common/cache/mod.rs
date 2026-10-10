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
    inner: Arc<DashMap<String, V>>,
    store: Option<Arc<dyn Store<V>>>,
}

impl<V> Cache<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
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

        Self { inner, store }
    }

    pub(crate) fn get(&self, key: &String) -> Option<CacheRef<'_, V>> {
        self.inner
            .get(key)
            .map(|dash_map_ref| CacheRef { dash_map_ref })
    }

    pub(crate) fn get_mut(&self, key: &String) -> Option<CacheRefMut<'_, V>> {
        self.inner.get_mut(key).map(|dash_map_ref_mut| CacheRefMut {
            dash_map_ref_mut,
            store: self.store.clone(),
            inner: self.inner.clone(),
        })
    }

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

    pub(crate) fn contains_key(&self, key: &String) -> bool {
        self.inner.contains_key(key)
    }

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
    dash_map_ref: Ref<'a, String, V>,
}

impl<V> Deref for CacheRef<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.dash_map_ref.value()
    }
}

pub(crate) struct CacheRefMut<'a, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    dash_map_ref_mut: RefMut<'a, String, V>,
    store: Option<Arc<dyn Store<V>>>,
    inner: Arc<DashMap<String, V>>,
}

impl<V> Deref for CacheRefMut<'_, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.dash_map_ref_mut.value()
    }
}

impl<V> DerefMut for CacheRefMut<'_, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dash_map_ref_mut.value_mut()
    }
}

impl<V> Drop for CacheRefMut<'_, V>
where
    V: serde::Serialize + Send + Sync + 'static,
{
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
    _mark: PhantomData<V>,
    namespace: String,
    module: String,
    load_cache_at_start: bool,
    store: Option<Arc<dyn Store<V>>>,
}

const CONFIG_MODULE: &str = "config";
const NAMING_MODULE: &str = "naming";

impl<V> CacheBuilder<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn config(namespace: String) -> Self {
        Self {
            _mark: Default::default(),
            namespace,
            module: CONFIG_MODULE.to_owned(),
            load_cache_at_start: false,
            store: None,
        }
    }

    pub(crate) fn naming(namespace: String) -> Self {
        Self {
            _mark: Default::default(),
            namespace,
            module: NAMING_MODULE.to_owned(),
            load_cache_at_start: false,
            store: None,
        }
    }

    pub(crate) fn load_cache_at_start(self, load_cache_at_start: bool) -> Self {
        Self {
            load_cache_at_start,
            ..self
        }
    }

    pub(crate) fn disk_store(self, cache_dir: Option<std::path::PathBuf>) -> Self {
        let mut disk_path = cache_dir.unwrap_or_else(|| {
            std::path::PathBuf::from(crate::common::util::HOME_DIR.to_owned()).join("nacos")
        });
        disk_path.push(self.module.clone());
        disk_path.push(self.namespace.clone());

        let disk_store = Arc::new(DiskStore::new(disk_path));

        Self {
            store: Some(disk_store),
            ..self
        }
    }

    pub(crate) async fn build(self) -> Cache<V> {
        Cache::new(self.store, self.load_cache_at_start).await
    }
}

// Store trait with separate constraints for load and save operations
#[async_trait]
trait Store<V>: Send + Sync {
    fn name(&self) -> Cow<'_, str>;

    async fn load(&self) -> HashMap<String, V>
    where
        V: serde::de::DeserializeOwned;

    async fn save(&self, key: &str, value: Vec<u8>);

    async fn remove(&self, key: &str);
}
