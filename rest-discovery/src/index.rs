//! 服务名索引:**仅 `heuristic_http=Enabled` 时维护**,服务于裸 `http(s)://host` → canonical 服务名。
//!
//! 存 `normalized_host -> ServiceNameEntry`(而非 `HashSet<String>`):命中后仍需注册中心原始(canonical)名调 discover/watch。
//! 纪律:
//! - 大小写冲突(`Foo`/`foo` 归一到同 key)→ 该 key **整体剔除** + `error`,不拖垮进程。
//! - 服务从列表消失 → 标 `missing_since`,超过 grace 才移除(容忍 list_services 瞬时不一致)。
//! - 本模块 `refresh` 只在「list_services 成功拿到新列表」时调用;刷新失败保留旧索引由调用方负责(不调 refresh)。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::options::ServiceMatchMode;

/// 索引一条:canonical 服务名 + 缺失起始时刻。
#[derive(Debug, Clone)]
struct ServiceNameEntry {
    /// 注册中心原始服务名(命中后用它调 discover/watch)。
    canonical_name: String,
    /// 本服务从最近一次 list_services 缺席的起始时刻;`None`=当前在册。超 grace 才真正移除。
    missing_since: Option<Instant>,
}

/// `refresh` 的结果:本轮【需回收 watch】的旧 canonical 服务名,供上游(client)`mark_removed` 对应 watch。包含两类:
/// ① 服务从索引缺席且超 `removed_service_grace`;
/// ② 同一 normalized key 的 canonical **单值替换**(如 `rust-simple-mvc` → `Rust-Simple-Mvc`,旧 canonical 已不在列表)。
/// 注意:大小写**冲突**剔除的 key【不计入】——那是歧义(同轮出现多个变体),非「服务消失」,其服务可能仍在册(显式 lb:// 仍有效)。
#[derive(Debug, Default, Clone)]
pub(crate) struct IndexRefreshOutcome {
    pub removed: Vec<String>,
}

/// 服务名索引。读多写少:快照存 `ArcSwap`,刷新整体替换。
pub(crate) struct ServiceNameIndex {
    names: ArcSwap<HashMap<String, ServiceNameEntry>>,
    match_mode: ServiceMatchMode,
    grace: Duration,
}

impl ServiceNameIndex {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub(crate) fn new(match_mode: ServiceMatchMode, grace: Duration) -> Self {
        Self {
            names: ArcSwap::from_pointee(HashMap::new()),
            match_mode,
            grace,
        }
    }

    /// 业务作用：按 match_mode 归一化 host(CaseInsensitive → ASCII lowercase)。
    ///
    /// # 参数
    /// - `host`: 实例注册或请求路由使用的主机名。
    fn normalize(&self, host: &str) -> String {
        match self.match_mode {
            ServiceMatchMode::CaseInsensitive => host.to_ascii_lowercase(),
            ServiceMatchMode::CaseSensitive => host.to_string(),
        }
    }

    /// 业务作用：host 命中 → 返回注册中心 canonical 服务名;未命中 → `None`。
    pub(crate) fn lookup(&self, host: &str) -> Option<String> {
        let key = self.normalize(host);
        self.names
            .load()
            .get(&key)
            .map(|e| e.canonical_name.clone())
    }

    /// 业务作用：用一份新服务名列表刷新索引(仅 list_services 成功时调用)。`now` 由调用方传入(便于确定性验证)。
    /// 返回本轮【需回收 watch】的旧 canonical 名(超 grace 缺席 + canonical 单值替换;冲突不计,见 [`IndexRefreshOutcome`]),
    /// 供调用方 `mark_removed`。
    pub(crate) fn refresh(&self, names: Vec<String>, now: Instant) -> IndexRefreshOutcome {
        let old = self.names.load();

        // 1) 归一化分组:key -> 该 key 下出现过的(去重)raw 名,用于检测大小写冲突。
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
        for raw in names {
            let key = self.normalize(&raw);
            let bucket = grouped.entry(key).or_default();
            if !bucket.contains(&raw) {
                bucket.push(raw);
            }
        }

        // 2) 构建新索引:无冲突 key 进索引;冲突 key 剔除 + error,并记下以免被 grace 复活。
        let mut next: HashMap<String, ServiceNameEntry> = HashMap::new();
        let mut conflicted: HashSet<String> = HashSet::new();
        for (key, canonicals) in grouped {
            if canonicals.len() > 1 {
                tracing::error!(
                    key = %key,
                    conflicting = ?canonicals,
                    "rest-discovery: 服务名大小写冲突,该 host 整体剔除出索引(裸 http 将按外部处理)"
                );
                conflicted.insert(key);
                continue;
            }
            next.insert(
                key,
                ServiceNameEntry {
                    canonical_name: canonicals.into_iter().next().unwrap(),
                    missing_since: None,
                },
            );
        }

        // 3) 旧索引中本轮缺席的项:grace 内保留(带 missing_since),超 grace 丢弃;冲突 key 不复活。
        //    另:key 仍在但 canonical 单值替换(CaseInsensitive 下大小写变体,如 rust-simple-mvc → Rust-Simple-Mvc)→
        //    旧 canonical 已不在服务列表,需要回收其 watch,否则会留下旧订阅。
        let mut removed: Vec<String> = Vec::new();
        for (key, old_entry) in old.iter() {
            if conflicted.contains(key) {
                continue; // 冲突表示歧义，不发送 removed；旧 watch 只能由调用方显式解除。
            }
            if let Some(new_entry) = next.get(key) {
                if new_entry.canonical_name != old_entry.canonical_name {
                    removed.push(old_entry.canonical_name.clone());
                }
                continue; // key 仍在册(canonical 未变 → 无操作;变了 → 已记 removed,新 canonical 留在 next)
            }
            let missing_since = old_entry.missing_since.unwrap_or(now);
            if now.duration_since(missing_since) <= self.grace {
                next.insert(
                    key.clone(),
                    ServiceNameEntry {
                        canonical_name: old_entry.canonical_name.clone(),
                        missing_since: Some(missing_since),
                    },
                );
            } else {
                tracing::debug!(key = %key, "rest-discovery: 服务超 grace 仍缺席,移出索引");
                removed.push(old_entry.canonical_name.clone());
            }
        }

        self.names.store(Arc::new(next));
        IndexRefreshOutcome { removed }
    }
}
