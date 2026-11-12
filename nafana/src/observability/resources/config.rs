use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

/// 通用资源采样只读取当前进程、可见 cgroup 和明确的路径、设备及网卡白名单。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorsConfig {
    /// 是否建立周期资源任务，与外部出口开关独立。
    pub enabled: bool,
    /// 进程、cgroup 与网卡采样周期，单位毫秒。
    pub interval_ms: u64,
    /// 文件系统容量采样周期，不短于普通采样周期。
    pub filesystem_interval_ms: u64,
    /// 单轮结果的可接受耗时；超时后仍保留原工作的回收责任。
    pub timeout_ms: u64,
    /// 普通样本的新鲜度上限；文件系统至少保留三个采样周期。
    pub stale_after_ms: u64,
    /// 是否采集当前进程资源。
    pub process: bool,
    /// 是否采集当前进程所属可见 cgroup v2。
    pub cgroup: bool,
    /// 固定别名到绝对路径，按实际文件系统设备去重。
    pub filesystems: BTreeMap<String, String>,
    /// 固定别名到当前网络命名空间的接口名。
    pub network_interfaces: BTreeMap<String, String>,
    /// 固定别名到 major:minor，仅导出指定设备的 cgroup I/O。
    pub io_devices: BTreeMap<String, String>,
    /// 每个内核统计文件允许的完整文本字节数。
    pub max_file_bytes: usize,
    /// 进程描述符枚举的最大条目数，超出后不发布截断数量。
    pub max_entries: usize,
}

impl Default for CollectorsConfig {
    /// 业务作用：保留应用原有开销，资源采样仅在显式启用后创建周期工作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：五秒采样、一秒期限和十五秒新鲜度；外部路径与设备默认不选择。
    fn default() -> Self {
        Self {
            enabled: false,
            interval_ms: 5000,
            filesystem_interval_ms: 30000,
            timeout_ms: 1000,
            stale_after_ms: 15000,
            process: true,
            cgroup: true,
            filesystems: BTreeMap::new(),
            network_interfaces: BTreeMap::new(),
            io_devices: BTreeMap::new(),
            max_file_bytes: 65536,
            max_entries: 65536,
        }
    }
}

impl CollectorsConfig {
    /// 业务作用：启动前限制采样周期、文件读取和标签目录，禁止指标输入制造无界工作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部配置有界且无模糊路径；无效配置不创建采样资源。
    pub fn validate(&self) -> Result<(), String> {
        if !(1000..=300000).contains(&self.interval_ms)
            || !(100..=30000).contains(&self.timeout_ms)
            || self.filesystem_interval_ms < self.interval_ms
            || self.filesystem_interval_ms > 300000
            || self.timeout_ms > self.interval_ms
            || self.stale_after_ms < self.interval_ms.saturating_mul(2)
            || self.stale_after_ms > 900000
            || !(4096..=1048576).contains(&self.max_file_bytes)
            || !(256..=1000000).contains(&self.max_entries)
        {
            return Err("collectors: invalid sampling limits".into());
        }
        for values in [
            &self.filesystems,
            &self.network_interfaces,
            &self.io_devices,
        ] {
            if values.len() > 16 {
                return Err("collectors: alias capacity exceeded".into());
            }
            for alias in values.keys() {
                if alias.is_empty()
                    || alias.len() > 48
                    || !alias
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
                {
                    return Err("collectors: invalid fixed alias".into());
                }
            }
            let unique: std::collections::BTreeSet<_> = values.values().collect();
            if unique.len() != values.len() {
                return Err("collectors: duplicate source aliases".into());
            }
        }
        for value in self.filesystems.values() {
            let path = Path::new(value);
            if !path.is_absolute()
                || value.len() > 4096
                || value.chars().any(char::is_control)
                || path
                    .components()
                    .any(|part| matches!(part, Component::ParentDir))
            {
                return Err("collectors.filesystems: absolute normalized path required".into());
            }
        }
        for value in self.network_interfaces.values() {
            if value.is_empty()
                || value.len() > 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
            {
                return Err("collectors.network_interfaces: invalid interface".into());
            }
        }
        for value in self.io_devices.values() {
            let Some((major, minor)) = value.split_once(':') else {
                return Err("collectors.io_devices: major:minor required".into());
            };
            if major.parse::<u32>().is_err() || minor.parse::<u32>().is_err() {
                return Err("collectors.io_devices: invalid device".into());
            }
        }
        Ok(())
    }
}
