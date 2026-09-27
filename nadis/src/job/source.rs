//! Job 多数据源映射：启动前冻结 canonical qualifier 到已托管 RedisClient 的一一关系。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::client::RedisClient;
use crate::error::Result;
use crate::job::names::require_name;

/// 语言无关的 Redis Job source id；排序顺序用于确定准备、启动与反向停机次序。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobSourceId(String);

impl JobSourceId {
    /// 业务作用：规范化并校验 Job source id；空值表示默认 source，持久协议固定写 `primary`。
    ///
    /// 参数说明：
    /// - `value`: 外部 source 名称。
    ///
    /// 返回：合法 canonical id；非法字符、冒号或超长返回配置错误。
    pub fn new(value: &str) -> Result<Self> {
        let value = if value.trim().is_empty() {
            "primary"
        } else {
            value.trim()
        };
        let value = require_name(value, "qualifier")?;
        if value.contains(':') {
            return Err(
                crate::job::JobError::Config("job qualifier 不得包含 ':'".to_owned()).into(),
            );
        }
        Ok(Self(value))
    }

    /// 业务作用：返回持久协议使用的 canonical source 文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：source id 字符串。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 启动前冻结的多 Redis source 表；重复 qualifier 或 client 身份不一致时拒绝登记。
#[derive(Default)]
pub struct RedisJobSources {
    clients: BTreeMap<JobSourceId, Arc<RedisClient>>,
}

impl RedisJobSources {
    /// 业务作用：创建链式 source builder；builder 与最终冻结映射为同一拥有式类型，不暴露可变共享表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空 source builder。
    pub fn builder() -> Self {
        Self::new()
    }

    /// 业务作用：创建空 source 表，供业务显式登记计划可用的 RedisClient。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空映射。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：创建只含默认 `primary` source 的便捷映射。
    ///
    /// 参数说明：
    /// - `client`: qualifier 必须为 `primary` 的 RedisClient。
    ///
    /// 返回：身份一致时返回单成员映射；否则返回配置错误。
    pub fn primary(client: Arc<RedisClient>) -> Result<Self> {
        Self::new().insert("primary", client)
    }

    /// 业务作用：登记一个已托管 RedisClient，并复验映射 key 与客户端冻结 qualifier 一致。
    ///
    /// 参数说明：
    /// - `qualifier`: 资源边界给出的 source id；本地别名 `default` 只在此映射为 `primary`。
    /// - `client`: 已连接的 RedisClient。
    ///
    /// 返回：登记成功返回更新后的映射；重复或身份不一致返回错误且不覆盖既有成员。
    pub fn insert(mut self, qualifier: &str, client: Arc<RedisClient>) -> Result<Self> {
        let boundary = if qualifier == "default" {
            "primary"
        } else {
            qualifier
        };
        let id = JobSourceId::new(boundary)?;
        if client.qualifier() != id.as_str() {
            return Err(crate::job::JobError::SourceMismatch(format!(
                "mapping={} client={}",
                id.as_str(),
                client.qualifier()
            ))
            .into());
        }
        if self.clients.contains_key(&id) {
            return Err(crate::job::JobError::Config(format!(
                "Job source {} 重复登记",
                id.as_str()
            ))
            .into());
        }
        self.clients.insert(id, client);
        Ok(self)
    }

    /// 业务作用：以文档约定的 builder 名称登记一个 Redis source，复用 `insert` 的身份与重复门禁。
    ///
    /// 参数说明：
    /// - `qualifier`: source id；边界别名 `default` 归一为 `primary`。
    /// - `client`: qualifier 已冻结的 RedisClient。
    ///
    /// 返回：登记后的 builder；身份不一致或重复时返回错误。
    pub fn register(self, qualifier: &str, client: Arc<RedisClient>) -> Result<Self> {
        self.insert(qualifier, client)
    }

    /// 业务作用：冻结 source builder，并拒绝没有任何客户端的运行时资源表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：非空映射原样返回；空映射返回配置错误。
    pub fn build(self) -> Result<Self> {
        if self.clients.is_empty() {
            return Err(
                crate::job::JobError::Config("RedisJob sources 不能为空".to_owned()).into(),
            );
        }
        Ok(self)
    }

    /// 业务作用：按 canonical source id 读取已登记客户端，不做唯一成员或默认成员回退。
    ///
    /// 参数说明：
    /// - `id`: 已规范化 source id。
    ///
    /// 返回：存在时返回共享客户端；未知 source 返回 `None`。
    pub(crate) fn get(&self, id: &JobSourceId) -> Option<Arc<RedisClient>> {
        self.clients.get(id).cloned()
    }
}
