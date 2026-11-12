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
    /// 业务作用：为一次逻辑请求从候选列表中选择目标位置。
    ///
    /// 参数说明：
    /// - `service`：用于隔离选址状态的逻辑服务名。
    /// - `instances`：调用方已筛为流量候选且保持本次顺序的实例引用列表。
    ///
    /// 返回：有可用选择时返回本次列表内的下标，否则 None；实现负责维持自身算法状态，列表内容及生命周期由调用方管理。
    fn pick(&self, service: &str, instances: &[&Instance]) -> Option<usize>;
}

/// 默认 round-robin:每个 service 独立 `AtomicU64` 计数器,**创建时随机起种**
/// 避免冷启动时所有进程从同一实例开始,导致瞬时流量集中。
#[derive(Default)]
pub struct RoundRobinLoadBalancer {
    counters: DashMap<String, AtomicU64>,
}

impl RoundRobinLoadBalancer {
    /// 业务作用：构造按服务独立计数的轮询选址器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回无已登记计数器的选址器，首次服务请求时才分配状态。
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// 业务作用：推进服务轮询计数并映射到当前候选数量。
    ///
    /// 参数说明：
    /// - `service`：提供独立轮询序列的服务名。
    /// - `count`：调用方必须保证非零的候选数量。
    ///
    /// 返回：返回递增前计数对 count 的余数，首个计数使用随机哈希种子；零 count 不符合前置条件，会 panic。
    fn next_index(&self, service: &str, count: u64) -> u64 {
        let entry = self
            .counters
            .entry(service.to_string())
            .or_insert_with(|| AtomicU64::new(random_seed(service)));
        entry.fetch_add(1, Ordering::Relaxed) % count
    }
}

impl LoadBalancer for RoundRobinLoadBalancer {
    /// 业务作用：从当前非空候选列表按服务轮询选择实例。
    ///
    /// 参数说明：
    /// - `service`：用于隔离选址状态的逻辑服务名。
    /// - `instances`：调用方已筛为流量候选且保持本次顺序的实例引用列表。
    ///
    /// 返回：空列表返回 None，否则返回列表内的轮询下标；并发调用以原子计数推进。
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
    /// 业务作用：构造按服务保存平滑权重状态的选址器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回尚无服务权重状态的选址器，不预分配实例表。
    pub fn new() -> Self {
        Self {
            state: DashMap::new(),
        }
    }
}

/// 业务作用：按实例地址与端口生成权重状态索引键。
///
/// 参数说明：
/// - `inst`：需要关联历史权重的候选实例。
///
/// 返回：返回地址、冒号及端口的文本拼接；不包含集群或 metadata，同地址端口共享该身份。
fn instance_key(inst: &Instance) -> String {
    format!("{}:{}", inst.ip, inst.port)
}

impl LoadBalancer for WeightedRoundRobinLoadBalancer {
    /// 业务作用：按服务维护平滑加权轮询累积值并选择本次最大者。
    ///
    /// 参数说明：
    /// - `service`：用于隔离选址状态的逻辑服务名。
    /// - `instances`：调用方已筛为流量候选且保持本次顺序的实例引用列表。
    ///
    /// 返回：空列表返回 None；非空时按权重更新状态、从选中项扣除总权重并移除旧地址状态，返回所选下标。调用方应保证权重有限且为正，函数只对负权重取零，不独立验证全部实例。
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

/// 业务作用：为服务轮询序列生成进程随机哈希起点。
///
/// 参数说明：
/// - `service`：需要打散首次选址位置的服务名。
///
/// 返回：返回本次 RandomState 对服务名计算的 u64，不保证跨调用或重启稳定，也不作为持久身份。
fn random_seed(service: &str) -> u64 {
    RandomState::new().hash_one(service)
}
