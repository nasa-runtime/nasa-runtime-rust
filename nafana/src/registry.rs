//! 全局命令注册表:按 (group, command) 去重 + 天然有序。
//!
//! 为什么必须去重(合同):Prometheus 文本里同一组 label 出现两条样本是**格式错误**,
//! 抓取端会整页拒收，因此同名 Command 配置冲突必须显式记录并保持首次注册值。
//! 用 BTreeMap 作存储,遍历天然按 (group, command) 排序,`/metrics` 输出稳定可 diff(合同)。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::command::Command;

/// 注册表键。group 在前,保证遍历顺序 = 渲染排序要求。
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CommandKey {
    /// 分组名(Dashboard 分组/threadPool 语义)。
    pub(crate) group: String,
    /// 命令名(圈名/接口名)。
    pub(crate) command: String,
}

/// 同 key 重复注册时用于一致性比对的参数指纹(0 值归一化之后的口径)。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandParams {
    /// 并发上限;None = 不限。
    pub(crate) max_concurrent: Option<usize>,
    /// 单请求超时;None = 不超时。
    pub(crate) timeout: Option<Duration>,
    /// TPS 权重;None = 不计 TPS。
    pub(crate) tps_weight: Option<u64>,
}

/// 同 key 但参数不一致的注册冲突([`crate::Command::try_monitor`] 的错误产物)。
#[derive(Debug)]
pub struct MonitorConflict {
    /// 冲突的命令名。
    pub command: String,
    /// 冲突的分组名。
    pub group: String,
}

impl std::fmt::Display for MonitorConflict {
    /// 业务作用：人类可读的冲突描述,方便直接进日志。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "命令 (group={}, command={}) 已按不同参数注册,保留既有实例",
            self.group, self.command
        )
    }
}

/// 表项:参数指纹 + 命令实例。
type Entry = (CommandParams, Arc<Command>);

// 全局注册表,进程内唯一;/metrics 渲染与 current_tps 都遍历它。
static REGISTRY: OnceLock<Mutex<BTreeMap<CommandKey, Entry>>> = OnceLock::new();

/// 业务作用：返回全局注册表(懒初始化)。
fn registry() -> &'static Mutex<BTreeMap<CommandKey, Entry>> {
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// 业务作用：取得注册表锁；一次持锁 panic 不应永久破坏指标与业务请求。
fn lock_registry() -> std::sync::MutexGuard<'static, BTreeMap<CommandKey, Entry>> {
    registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// 业务作用：取或建(合同 去重语义):
/// - 首次见到 key → 调 `build` 创建并注册;
/// - key 已存在且参数一致 → 返回既有实例;
/// - key 已存在但参数不同 → 保留既有实例并返回 `Err`(调用方决定 warn 复用还是上抛)。
///
/// `build` 在注册表锁内执行,保证同 key 并发首建只创建一次(Command 构造是纯内存 + 一次
/// tokio::spawn,锁内代价可忽略)。
///
/// # 参数
/// - `key`: 注册表键(group + command)。
/// - `params`: 归一化后的参数指纹,用于一致性比对。
/// - `build`: 首建闭包,只在 key 不存在时被调用一次。
pub(crate) fn get_or_register(
    key: CommandKey,
    params: CommandParams,
    build: impl FnOnce() -> Arc<Command>,
) -> Result<Arc<Command>, (MonitorConflict, Arc<Command>)> {
    let mut map = lock_registry();
    if let Some((existing_params, existing)) = map.get(&key) {
        if *existing_params == params {
            if key.group == "macro" {
                tracing::warn!(
                    group = %key.group,
                    command = %key.command,
                    "duplicate grafana command name; reusing the first registration, later path and fallback settings are ignored"
                );
            }
            return Ok(existing.clone());
        }
        let conflict = MonitorConflict {
            command: key.command,
            group: key.group,
        };
        return Err((conflict, existing.clone()));
    }
    let cmd = build();
    map.insert(key, (params, cmd.clone()));
    Ok(cmd)
}

/// 业务作用：快照全部命令(已按 (group, command) 排序);clone Arc 列表后立即放锁,
/// 调用方再逐个 snapshot,避免攥着注册表锁去锁各命令的滚动窗口。
pub(crate) fn all() -> Vec<Arc<Command>> {
    lock_registry()
        .values()
        .map(|(_, cmd)| cmd.clone())
        .collect()
}
