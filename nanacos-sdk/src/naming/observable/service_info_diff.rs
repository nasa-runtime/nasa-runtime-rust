use tracing::info;

use crate::{api::naming::ServiceInstance, naming::dto::ServiceInfo};

/// 两份服务实例快照之间的地址及属性变化，借用原始实例数据。
#[derive(Debug, Clone, Default)]
pub struct ServiceInfoDiff<'a> {
    /// 只在新快照中存在的实例。
    pub new_instances: Vec<&'a ServiceInstance>,
    /// 只在旧快照中存在的实例。
    pub removed_instances: Vec<&'a ServiceInstance>,
    /// 地址仍相同但属性变化的新快照实例。
    pub modified_instances: Vec<&'a ServiceInstance>,
    /// 三类差异中至少有一项时为真。
    pub changed: bool,
}

impl<'a> ServiceInfoDiff<'a> {
    /// 业务作用：按 IP 和端口对齐实例快照，找出新增、移除及属性变化的实例。
    ///
    /// 参数说明：
    /// - `old_hosts`：旧快照，用于定位移除项和比较既有实例。
    /// - `new_hosts`：新快照，用于定位新增项及变化后的实例。
    ///
    /// 返回：借用两份快照的差异，类别内顺序未固定；相同地址的重复项以各自最后一项为准，变更按完整实例属性比较，不判断服务时间先后。
    pub fn calculate(old_hosts: &'a [ServiceInstance], new_hosts: &'a [ServiceInstance]) -> Self {
        let old_hosts_map: std::collections::HashMap<String, &ServiceInstance> = old_hosts
            .iter()
            .map(|host| (host.ip_and_port(), host))
            .collect();
        let new_hosts_map: std::collections::HashMap<String, &ServiceInstance> = new_hosts
            .iter()
            .map(|host| (host.ip_and_port(), host))
            .collect();

        let mut diff = Self::default();

        // 地址作为匹配身份，属性变化保存新实例视图，供调用方发布更新后的目录。
        for (key, new_host) in new_hosts_map.iter() {
            match old_hosts_map.get(key) {
                None => diff.new_instances.push(*new_host),
                Some(old_host) => {
                    if !old_host.is_same_instance(new_host) {
                        diff.modified_instances.push(*new_host);
                    }
                }
            }
        }

        // 移除项必须从旧快照取值，新快照已不再携带它们的元数据。
        for (key, old_host) in old_hosts_map.iter() {
            if !new_hosts_map.contains_key(key) {
                diff.removed_instances.push(*old_host);
            }
        }

        diff.changed = !diff.new_instances.is_empty()
            || !diff.removed_instances.is_empty()
            || !diff.modified_instances.is_empty();

        diff
    }

    /// 业务作用：分别记录三类实例变化，便于观察服务目录的变化来源。
    ///
    /// 参数说明：
    /// - `key`：用于关联目录的服务键，原样写入日志。
    ///
    /// 返回：非空类别以 JSON 记录实例，包含实例元数据且不执行脱敏；序列化失败用空数组文本，不改变差异结果。
    pub fn log_changes(&self, key: &str) {
        if !self.new_instances.is_empty() {
            let json = serde_json::to_string(&self.new_instances).unwrap_or("[]".to_owned());
            info!(
                "new ips({}) service: {} -> {}",
                self.new_instances.len(),
                key,
                json
            );
        }

        if !self.removed_instances.is_empty() {
            let json = serde_json::to_string(&self.removed_instances).unwrap_or("[]".to_owned());
            info!(
                "removed ips({}) service: {} -> {}",
                self.removed_instances.len(),
                key,
                json
            );
        }

        if !self.modified_instances.is_empty() {
            let json = serde_json::to_string(&self.modified_instances).unwrap_or("[]".to_owned());
            info!(
                "modified ips({}) service: {} -> {}",
                self.modified_instances.len(),
                key,
                json
            );
        }
    }
}

/// 业务作用：比较服务端快照刷新时间，阻止明确早于本地已知快照的数据进入更新流程。
///
/// 参数说明：
/// - `old_service`：缓存中的服务快照。
/// - `new_service`：准备接收的新快照。
///
/// 返回：旧快照 last_ref_time 严格大于新快照时为真；时间相等不判过期，不使用本地时钟或校验其它身份。
pub fn is_outdated_data(old_service: &ServiceInfo, new_service: &ServiceInfo) -> bool {
    old_service.last_ref_time > new_service.last_ref_time
}
