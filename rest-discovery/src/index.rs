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
    max_services: usize,
}

impl ServiceNameIndex {
    /// 业务作用：构造尚无服务名快照的启发式路由索引。
    ///
    /// 参数说明：
    /// - `match_mode`：主机名查找时的大小写匹配策略。
    /// - `grace`：成功枚举中名称缺席后的保留期限。
    /// - `max_services`：输入及保留索引的容量上限。
    ///
    /// 返回：返回空索引并保存策略，不校验预算或执行服务枚举。
    pub(crate) fn new(match_mode: ServiceMatchMode, grace: Duration, max_services: usize) -> Self {
        Self {
            names: ArcSwap::from_pointee(HashMap::new()),
            match_mode,
            grace,
            max_services,
        }
    }

    /// 业务作用：按配置统一查找键与服务名的 ASCII 大小写。
    ///
    /// 参数说明：
    /// - `host`：原始请求主机或服务名。
    ///
    /// 返回：大小写不敏感模式返回 ASCII 小写副本，否则原样克隆；不执行 Unicode 归一化。
    fn normalize(&self, host: &str) -> String {
        match self.match_mode {
            ServiceMatchMode::CaseInsensitive => host.to_ascii_lowercase(),
            ServiceMatchMode::CaseSensitive => host.to_string(),
        }
    }

    /// 业务作用：从当前索引快照查询规范服务名。
    ///
    /// 参数说明：
    /// - `host`：按匹配策略归一化后查找的请求主机名。
    ///
    /// 返回：命中返回规范服务名克隆，否则 None；不在此主动检查缺席期限或请求后端。
    pub(crate) fn lookup(&self, host: &str) -> Option<String> {
        let key = self.normalize(host);
        self.names
            .load()
            .get(&key)
            .map(|e| e.canonical_name.clone())
    }

    /// 业务作用：由一次成功枚举构建并发布新的服务名索引及待回收名称。
    ///
    /// 参数说明：
    /// - `names`：后端本次完整成功枚举的服务名。
    /// - `now`：用于计算连续缺席期限的单调观察时刻。
    ///
    /// 返回：输入规模及名称长度有效时原子发布索引，并返回宽限耗尽或规范名替换所需回收的旧名称；非法输入保留旧索引。归一化名称冲突整体不进入索引且不列为 removed，剩余容量不足也可提前回收缺席旧项。
    pub(crate) fn refresh(
        &self,
        names: Vec<String>,
        now: Instant,
    ) -> crate::error::Result<IndexRefreshOutcome> {
        // 超量候选不能替换有效目录，也不能借 grace 保留把历次列表叠加成无界表。
        if names.len() > self.max_services
            || names.iter().any(|name| name.is_empty() || name.len() > 256)
        {
            return Err(crate::error::RestDiscoveryError::DiscoveryStateLimit);
        }
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
            if now.duration_since(missing_since) <= self.grace && next.len() < self.max_services {
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

        // 新快照全部构建完成后再一次发布，读者不会看到归组和旧名称保留过程中的半成品。
        self.names.store(Arc::new(next));
        Ok(IndexRefreshOutcome {
            removed,
        })
    }
}
