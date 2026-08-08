//! 负载均衡策略。
//!
//! 过滤「可承载流量」实例由 client 层统一用 `nadisc::is_traffic_instance` 完成
//! 本模块只在**已过滤后**的实例集上选址。`pick` 收实例切片(而非仅数量)以便加权策略读 `weight`。

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use dashmap::DashMap;
use nadisc::Instance;

/// 负载均衡策略:给定服务名 + 已过滤实例,返回选中的下标(空→`None`)。
pub trait LoadBalancer: Send + Sync {
    /// 业务作用：从实例列表中选择目标下标；用于把一次请求路由到具体实例。
    ///
    /// # 参数
    ///
    /// - `service`: 当前请求的服务名，用于维护按服务隔离的负载均衡状态。
    /// - `instances`: 已过滤为可承载流量的实例列表。
    fn pick(&self, service: &str, instances: &[&Instance]) -> Option<usize>;
}

/// 默认 round-robin:每个 service 独立 `AtomicU64` 计数器,**创建时随机起种**
/// 避免冷启动时所有进程从同一实例开始,导致瞬时流量集中。
#[derive(Default)]
pub struct RoundRobinLoadBalancer {
    counters: DashMap<String, AtomicU64>,
}

impl RoundRobinLoadBalancer {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// 业务作用：递增并返回轮询下标；用于在同一服务的实例间均匀分配请求。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    fn next_index(&self, service: &str, count: u64) -> u64 {
        let entry = self
            .counters
            .entry(service.to_string())
            .or_insert_with(|| AtomicU64::new(random_seed(service)));
        entry.fetch_add(1, Ordering::Relaxed) % count
    }
}

impl LoadBalancer for RoundRobinLoadBalancer {
    /// 业务作用：从实例列表中选择目标下标；用于把一次请求路由到具体实例。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `instances`: 负载均衡候选实例列表。
    fn pick(&self, service: &str, instances: &[&Instance]) -> Option<usize> {
        if instances.is_empty() {
            return None;
        }
        Some(self.next_index(service, instances.len() as u64) as usize)
    }
}

/// 平滑加权 round-robin(nginx smooth WRR)。每 service 维护各实例 `current_weight`:
/// 每次 `current += weight`、选 `current` 最大者、再 `current -= total_weight`——分布平滑且贴合权重比例。
///
/// 实例集每次按当前传入集合**重建**(携带已存在实例的 `current_weight`),自动剪除已下线实例的陈旧状态。
#[derive(Default)]
pub struct WeightedRoundRobinLoadBalancer {
    // 每个 service 单独保存 endpoint 到 current_weight 的映射，避免不同服务互相污染轮询权重。
    state: DashMap<String, Mutex<HashMap<String, f64>>>,
}

impl WeightedRoundRobinLoadBalancer {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub fn new() -> Self {
        Self {
            state: DashMap::new(),
        }
    }
}

/// 业务作用：生成实例稳定标识；用于按地址维持负载均衡状态。
///
/// # 参数
/// - `inst`: 服务发现返回的实例信息。
fn instance_key(inst: &Instance) -> String {
    format!("{}:{}", inst.ip, inst.port)
}

impl LoadBalancer for WeightedRoundRobinLoadBalancer {
    /// 业务作用：从实例列表中选择目标下标；用于把一次请求路由到具体实例。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `instances`: 负载均衡候选实例列表。
    fn pick(&self, service: &str, instances: &[&Instance]) -> Option<usize> {
        if instances.is_empty() {
            return None;
        }
        let cell = self.state.entry(service.to_string()).or_default();
        let mut old = cell.lock().expect("rest-discovery: WRR 状态锁被毒化");

        let mut total = 0.0f64;
        let mut best: Option<usize> = None;
        let mut best_cw = f64::NEG_INFINITY;
        let mut next: HashMap<String, f64> = HashMap::with_capacity(instances.len());

        for (i, inst) in instances.iter().enumerate() {
            let key = instance_key(inst);
            let w = inst.weight.max(0.0);
            let cw = old.get(&key).copied().unwrap_or(0.0) + w;
            total += w;
            if cw > best_cw {
                best_cw = cw;
                best = Some(i);
            }
            next.insert(key, cw);
        }

        if let Some(bi) = best {
            let bkey = instance_key(instances[bi]);
            if let Some(cw) = next.get_mut(&bkey) {
                *cw -= total;
            }
        }
        *old = next; // 重建即剪枝陈旧 key
        best
    }
}

/// 业务作用：每服务计数器的随机起种:进程内随机 keys 的 `RandomState` 对 service 名哈希一次。
///
/// # 参数
/// - `service`: 服务名,用于服务发现或注册中心查询。
fn random_seed(service: &str) -> u64 {
    RandomState::new().hash_one(service)
}
