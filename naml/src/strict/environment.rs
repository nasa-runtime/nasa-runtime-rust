use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use super::{ConfigError, ErrorKind, Result};

/// 固定一次环境观察，所有后续查找均不回退真实进程环境。
#[derive(Clone, Default)]
pub struct EnvironmentSnapshot {
    pub(crate) values: BTreeMap<String, String>,
    invalid: BTreeSet<String>,
    overlay_allowed: Option<BTreeSet<String>>,
    fallback_allowed: Option<BTreeSet<String>>,
}

impl EnvironmentSnapshot {
    /// 业务作用：为兼容入口固定一次进程环境，不以严格快照总量限制改变既有启动条件。
    /// 参数说明：无。
    /// 返回：可读取的 UTF-8 环境映射；不可解码项沿用兼容查找的未命中语义。
    pub(crate) fn capture_compatible() -> Self {
        Self::from_compatible_pairs(
            std::env::vars_os().filter_map(|(key, value)| {
                Some((key.into_string().ok()?, value.into_string().ok()?))
            }),
        )
    }

    /// 业务作用：让兼容加载器注入的环境和进程快照采用相同边界。
    /// 参数说明：`values` 为兼容调用方已持有的名称与原值。
    /// 返回：固定映射；严格公开工厂仍独立执行其数量与字节限制。
    pub(crate) fn from_compatible_pairs(
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self {
            values: values.into_iter().collect(),
            ..Self::default()
        }
    }

    /// 业务作用：在应用启动时捕获有限的环境映射。
    /// 参数说明：无。
    /// 返回：完整快照；环境总量超限时拒绝，非法值仅在实际访问时报告。
    pub fn capture() -> Result<Self> {
        let mut snapshot = Self::default();
        let mut bytes = 0usize;
        for (key, value) in std::env::vars_os() {
            bytes = bytes
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
            if bytes > 2 * 1024 * 1024 || snapshot.values.len() + snapshot.invalid.len() >= 4096 {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
            if let Ok(key) = key.into_string() {
                match value.into_string() {
                    Ok(value) => {
                        snapshot.values.insert(key, value);
                    }
                    Err(_) => {
                        snapshot.invalid.insert(key);
                    }
                }
            }
        }
        Ok(snapshot)
    }

    /// 业务作用：从调用方映射创建与真实环境隔离的输入。
    /// 参数说明：`values` 保留原样名称和包括空白在内的原值。
    /// 返回：有限映射快照；名称重复或总量超限时失败。
    pub fn from_pairs(values: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        let mut result = Self::default();
        let mut bytes = 0usize;
        for (key, value) in values {
            bytes = bytes
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
            if bytes > 2 * 1024 * 1024 || result.values.len() >= 4096 {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
            if result.values.insert(key, value).is_some() {
                return Err(ConfigError::new(ErrorKind::Duplicate));
            }
        }
        Ok(result)
    }

    /// 业务作用：限制可参与结构覆盖的原样环境名称。
    /// 参数说明：`names` 为显式允许集合，空集合表示全部禁止。
    /// 返回：覆盖访问受限的新快照，不改变占位符回退范围。
    pub fn allow_overlay(mut self, names: BTreeSet<String>) -> Self {
        self.overlay_allowed = Some(names);
        self
    }

    /// 业务作用：限制占位符可查询的原样或规范化环境候选。
    /// 参数说明：`names` 为允许查询的确切名称。
    /// 返回：回退访问受限的新快照。
    pub fn allow_fallback(mut self, names: BTreeSet<String>) -> Self {
        self.fallback_allowed = Some(names);
        self
    }

    /// 业务作用：按精确名称读取启动 profile 等显式引导字段。
    /// 参数说明：`name` 为受信环境名称。
    /// 返回：原值或缺失；命中非法编码时失败。
    pub fn get(&self, name: &str) -> Result<Option<&str>> {
        if self.invalid.contains(name) {
            return Err(ConfigError::new(ErrorKind::Encoding));
        }
        Ok(self.values.get(name).map(String::as_str))
    }

    /// 业务作用：在不改变原样优先级的前提下查询环境回退。
    /// 参数说明：`name` 是占位符的当前候选名称。
    /// 返回：允许且存在的原值，禁止访问时拒绝而非伪装缺失。
    pub(crate) fn fallback(&self, name: &str) -> Result<Option<&str>> {
        if self
            .fallback_allowed
            .as_ref()
            .is_some_and(|set| !set.contains(name))
        {
            return Err(ConfigError::new(ErrorKind::PolicyChanged));
        }
        self.get(name)
    }

    /// 业务作用：提取可以参与前缀覆盖的环境项并校验编码。
    /// 参数说明：`prefix` 是包含分隔符的完整前缀。
    /// 返回：原样名称与原值，不修改大小写和空白。
    pub(crate) fn overlay(&self, prefix: &str) -> Result<Vec<(&str, &str)>> {
        let prefix = prefix.to_ascii_lowercase();
        for name in &self.invalid {
            if name.to_ascii_lowercase().starts_with(&prefix)
                && self
                    .overlay_allowed
                    .as_ref()
                    .is_none_or(|set| set.contains(name))
            {
                return Err(ConfigError::new(ErrorKind::Encoding));
            }
        }
        Ok(self
            .values
            .iter()
            .filter(|(name, _)| {
                name.to_ascii_lowercase().starts_with(&prefix)
                    && self
                        .overlay_allowed
                        .as_ref()
                        .is_none_or(|set| set.contains(*name))
            })
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect())
    }
}

impl fmt::Debug for EnvironmentSnapshot {
    /// 业务作用：只显示环境快照规模，不泄露名称或值。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：不含环境材料的调试摘要。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentSnapshot")
            .field("entries", &self.values.len())
            .finish_non_exhaustive()
    }
}
