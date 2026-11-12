use serde::{Deserialize, Serialize};
use tracing::error;

use crate::api::naming::ServiceInstance;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInfo {
    /// 服务端返回的完整服务名称。
    pub name: String,

    /// 服务所属分组。
    pub group_name: String,

    /// 本次查询或订阅的集群选择值。
    pub clusters: String,

    /// 服务端建议的缓存有效毫秒数。
    pub cache_millis: i64,

    /// 服务端最近刷新时刻的 Unix 毫秒时间戳。
    pub last_ref_time: i64,

    /// 服务端提供的实例集合摘要。
    pub checksum: String,

    #[serde(rename = "allIPs")]
    /// 兼容早期服务端字段的全量地址标志。
    pub all_ips: Option<bool>,

    #[serde(rename = "allIps")]
    /// 服务端 allIps 字段对应的全量地址标志。
    pub all_ips_3x: Option<bool>,

    /// 服务端是否已经触发保护阈值。
    pub reach_protection_threshold: bool,

    /// 当前实例快照；缺失不代表可使用旧实例。
    pub hosts: Option<Vec<ServiceInstance>>,
}

const SERVICE_INFO_SEPARATOR: &str = "@@";
impl ServiceInfo {
    /// 业务作用：读取当前实例快照的地址数量，不触发服务发现。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：hosts 缺失时为零，否则为快照中实例总数，包含不健康实例。
    pub fn ip_count(&self) -> i32 {
        match self.hosts.as_ref() {
            Some(h) => h.len() as i32,
            None => 0,
        }
    }

    /// 业务作用：按服务端字段兼容顺序读取全量地址语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：优先采用 allIPs；其缺失时采用 allIps，两者均缺失为 false。
    fn is_all_ips(&self) -> bool {
        if let Some(item) = self.all_ips {
            item
        } else {
            self.all_ips_3x.unwrap_or(false)
        }
    }

    /// 业务作用：判断当前快照能否作为发现缓存使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全量地址标志为 true 或至少一个实例健康且权重大于零时通过，其余情况拒绝。
    pub fn validate(&self) -> bool {
        if self.is_all_ips() {
            return true;
        }

        let Some(hosts) = self.hosts.as_ref() else {
            return false;
        };

        for host in hosts {
            if !host.healthy {
                continue;
            }

            if host.weight > 0 as f64 {
                return true;
            }
        }

        false
    }

    /// 业务作用：按 Nacos 服务名称规则补齐分组，保持远端协议名称不变。
    ///
    /// 参数说明：
    /// - `service_name`：原服务名称，可以已经包含分组分隔符。
    /// - `group_name`：待补齐的分组名称，允许为空。
    ///
    /// 返回：名称已有 @@ 或分组为空时原样返回，否则以 @@ 连接分组和服务名。
    pub fn get_grouped_service_name(service_name: &str, group_name: &str) -> String {
        if !group_name.is_empty() && !service_name.contains(SERVICE_INFO_SEPARATOR) {
            let service_name = format!("{}{}{}", group_name, SERVICE_INFO_SEPARATOR, service_name);
            return service_name;
        }
        service_name.to_string()
    }

    /// 业务作用：把当前实例快照编码为服务通知正文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时返回 JSON 数组文本；hosts 缺失或序列化失败时返回空串并记录失败。
    pub fn hosts_to_json(&self) -> String {
        let Some(hosts) = self.hosts.as_ref() else {
            return "".to_owned();
        };
        serde_json::to_string(hosts).unwrap_or_else(|e| {
            error!("hosts to json failed. {e:?}");
            "".to_owned()
        })
    }

    /// 业务作用：为发现缓存生成无歧义的服务名与集群组合身份。
    ///
    /// 参数说明：
    /// - `name`：服务端协议使用的完整服务名称。
    /// - `clusters`：当前订阅的集群选择值，允许为空。
    ///
    /// 返回：固定 JSON 二元组；分隔符不会合并不同缓存键，不改变远端服务名称。
    pub fn get_key(name: &str, clusters: &str) -> String {
        serde_json::to_string(&[name, clusters]).expect("string tuple serialization")
    }

    /// 业务作用：为不带集群选择的服务监听匹配 Nacos 分组服务名称。
    ///
    /// 参数说明：
    /// - `service_name`：事件或订阅的原服务名称。
    /// - `group_name`：事件或订阅所属分组。
    ///
    /// 返回：与远端分组名称一致的监听键；不受推送是否携带集群字段影响。
    pub fn get_key_without_clusters(service_name: &str, group_name: &str) -> String {
        Self::get_grouped_service_name(service_name, group_name)
    }
}

impl Default for ServiceInfo {
    /// 业务作用：建立尚无可用实例的发现快照，等待服务端填充内容。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：缓存建议间隔为 1000 毫秒，其余身份和实例为空，全量地址与保护标志关闭。
    fn default() -> Self {
        Self {
            name: Default::default(),
            group_name: Default::default(),
            clusters: Default::default(),
            cache_millis: 1000,
            last_ref_time: 0,
            checksum: Default::default(),
            all_ips: Some(false),
            all_ips_3x: Some(false),
            reach_protection_threshold: false,
            hosts: Default::default(),
        }
    }
}
