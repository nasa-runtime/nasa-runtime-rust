//! Topic 管理、元数据和离线 offset 查询的公共骨架。

use std::collections::BTreeMap;

use crate::error::{NafkaError, Result};
use crate::rd::admin::AdminHandle;
use crate::types::{KafkaPartitionLag, Tp};
use crate::KafkaProxy;

/// 创建 topic 时使用的稳定自有契约。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicSpec {
    /// topic 名。
    pub name: String,
    /// 初始分区数，必须大于零。
    pub partitions: i32,
    /// 副本数，必须大于零且不超过 broker 数量。
    pub replication_factor: i32,
    /// topic 级原生配置。
    pub configs: BTreeMap<String, String>,
}

/// topic 的 broker 侧只读描述。
///
/// 该类型只暴露稳定自有字段，供上层启动契约核验；不会泄漏底层客户端类型。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicDescription {
    /// topic 名。
    pub name: String,
    /// 排序后的实际分区号。
    pub partitions: Vec<i32>,
    /// 所有分区副本数一致时的副本因子；不一致时为 `None`。
    pub replication_factor: Option<i32>,
    /// topic 配置快照；value 为 `None` 表示 broker 未公开值或该项敏感。
    pub configs: BTreeMap<String, Option<String>>,
}

impl TopicSpec {
    /// 业务作用：构造最小 topic 契约。
    ///
    /// # 参数
    ///
    /// - `name`: topic 名。
    /// - `partitions`: 初始分区数。
    /// - `replication_factor`: 副本数。
    pub fn new(name: impl Into<String>, partitions: i32, replication_factor: i32) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor,
            configs: BTreeMap::new(),
        }
    }

    /// 业务作用：追加一个 topic 级原生配置。
    ///
    /// # 参数
    ///
    /// - `key`: broker topic 配置键。
    /// - `value`: 配置值。
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(key.into(), value.into());
        self
    }
}

/// 延迟构造的 Kafka 管理端句柄。
#[derive(Clone)]
pub struct KafkaAdmin {
    /// 所属运行时。
    proxy: KafkaProxy,
}

impl KafkaAdmin {
    /// 业务作用：构造管理端轻量句柄。
    ///
    /// # 参数
    ///
    /// - `proxy`: 所属运行时。
    pub(crate) fn new(proxy: KafkaProxy) -> Self {
        Self { proxy }
    }

    /// 业务作用：取得或并发安全地初始化共享管理客户端。
    ///
    /// 客户端延迟构造并可被 shutdown 丢弃，
    /// 丢弃后再次调用会重建。注意 步骤 1 只对 publish/register/control 用"拒绝"措辞，
    /// 对管理端用的是"关闭"，因此这里不设生命周期门禁——停机后继续用管理端做清理是被允许的用法。
    ///
    /// # 错误
    ///
    /// 原生配置或管理客户端构造失败时返回错误。
    fn client(&self) -> Result<std::sync::Arc<AdminHandle>> {
        if let Some(client) = self
            .proxy
            .inner
            .admin
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            return Ok(std::sync::Arc::clone(client));
        }
        let mut slot = self
            .proxy
            .inner
            .admin
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 写锁内复检：并发调用只构造一个实例，避免多余的 broker 连接。
        if let Some(client) = slot.as_ref() {
            return Ok(std::sync::Arc::clone(client));
        }
        let candidate = AdminHandle::create(&self.proxy.inner.config)?;
        *slot = Some(std::sync::Arc::clone(&candidate));
        Ok(candidate)
    }

    /// 业务作用：查询集群可见的 topic 名列表。
    ///
    /// # 错误
    ///
    /// 元数据请求超时、无权限或底层传输失败时返回错误。
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        Ok(self
            .client()?
            .topic_metadata()
            .await?
            .into_iter()
            .filter(|topic| topic.error.is_none())
            .map(|topic| topic.name)
            .collect())
    }

    /// 业务作用：判断 topic 是否存在。
    ///
    /// # 参数
    ///
    /// - `topic`: 待查询 topic 名。
    ///
    /// # 错误
    ///
    /// 元数据请求失败时返回错误。
    pub async fn topic_exists(&self, topic: &str) -> Result<bool> {
        validate_topic(topic)?;
        let metadata = self.client()?.topic_metadata().await?;
        match metadata.iter().find(|item| item.name == topic) {
            // 该条目本轮观测不可信（controller 切换、分区迁移、leader 暂不可用等）：
            // 必须报错让调用方走"短时观测失败"的容错预算，绝不能返回 false——
            // 上层会把 false 当成确定的契约漂移并永久停机。
            Some(item) => match &item.error {
                Some(error) => Err(NafkaError::Broker(format!(
                    "topic `{topic}` 元数据本轮不可观测: {error}"
                ))),
                None => Ok(true),
            },
            None => Ok(false),
        }
    }

    /// 业务作用：返回 topic 当前分区号列表。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    ///
    /// # 错误
    ///
    /// topic 不存在、无权限或请求失败时返回错误。
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<i32>> {
        validate_topic(topic)?;
        self.client()?
            .topic_metadata()
            .await?
            .into_iter()
            .find(|item| item.name == topic)
            .ok_or_else(|| NafkaError::Broker(format!("topic 不存在: {topic}")))
            .and_then(|item| match item.error {
                Some(error) => Err(NafkaError::Broker(format!(
                    "topic `{topic}` 元数据本轮不可观测: {error}"
                ))),
                None => Ok(item.partitions),
            })
    }

    /// 业务作用：读取 topic 的分区、副本和可选配置快照。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `include_configs`: 是否额外请求 DescribeConfigs 权限并返回配置。
    ///
    /// # 错误
    ///
    /// topic 不存在、无 Describe 权限或 broker 请求失败时返回错误。
    pub async fn describe_topic(
        &self,
        topic: &str,
        include_configs: bool,
    ) -> Result<TopicDescription> {
        validate_topic(topic)?;
        self.client()?
            .describe_topic(topic.to_owned(), include_configs)
            .await
    }

    /// 业务作用：创建 topic，并逐项检查 broker 返回结果。
    ///
    /// # 参数
    ///
    /// - `spec`: topic 契约。
    ///
    /// # 错误
    ///
    /// 参数非法、topic 已存在、无权限或 broker 拒绝时返回错误。
    pub async fn create_topic(&self, spec: TopicSpec) -> Result<()> {
        validate_spec(&spec)?;
        self.client()?.create_topic(&spec).await
    }

    /// 业务作用：topic 不存在时创建，已存在时返回成功。
    ///
    /// # 参数
    ///
    /// - `spec`: topic 契约。
    ///
    /// # 错误
    ///
    /// 查询或创建失败时返回错误。
    pub async fn create_if_absent(&self, spec: TopicSpec) -> Result<()> {
        validate_spec(&spec)?;
        self.client()?.create_topic_if_absent(&spec).await
    }

    /// 业务作用：删除 topic。
    ///
    /// # 参数
    ///
    /// - `topic`: 待删除 topic 名。
    ///
    /// # 错误
    ///
    /// topic 不存在、无权限或 broker 拒绝时返回错误。
    pub async fn delete_topic(&self, topic: &str) -> Result<()> {
        validate_topic(topic)?;
        self.client()?.delete_topic(topic).await
    }

    /// 业务作用：修改已存在 topic 的一项动态配置。
    ///
    /// 用于运维侧调整 `max.message.bytes`、`retention.ms` 等 broker 动态项；
    /// 不做本地合法性推断，键值合法性由 broker 裁决。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `key`: 动态配置键。
    /// - `value`: 新值。
    ///
    /// # 错误
    ///
    /// topic 名非法、运行时不可用或 broker 拒绝时返回错误。
    pub async fn alter_topic_config(&self, topic: &str, key: &str, value: &str) -> Result<()> {
        validate_topic(topic)?;
        self.client()?.alter_topic_config(topic, key, value).await
    }

    /// 业务作用：只增不减 topic 分区数。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `total_partitions`: 目标总分区数。
    ///
    /// # 错误
    ///
    /// 目标不大于当前分区数或 broker 拒绝时返回错误。
    pub async fn increase_partitions(&self, topic: &str, total_partitions: i32) -> Result<()> {
        validate_topic(topic)?;
        if total_partitions <= 0 {
            return Err(NafkaError::Config("total_partitions 必须大于零".into()));
        }
        let current = self.partitions_for(topic).await?.len();
        let total = usize::try_from(total_partitions)
            .map_err(|_| NafkaError::Config("total_partitions 超出 usize".into()))?;
        if total <= current {
            return Err(NafkaError::Config(format!(
                "total_partitions({total}) 必须大于当前分区数({current})"
            )));
        }
        self.client()?.increase_partitions(topic, total).await
    }

    /// 业务作用：查询分区最早和末尾 offset。
    ///
    /// # 参数
    ///
    /// - `tps`: topic-partition 列表。
    ///
    /// # 错误
    ///
    /// 任一 watermark 查询失败时返回错误。
    pub async fn watermarks(&self, tps: Vec<Tp>) -> Result<Vec<(Tp, i64, i64)>> {
        validate_tps(&tps)?;
        self.client()?.watermarks(tps).await
    }

    /// 业务作用：使用不加入 group 的短命客户端查询已提交 offset。
    ///
    /// # 参数
    ///
    /// - `group`: group id。
    /// - `tps`: 待查询分区。
    ///
    /// # 错误
    ///
    /// group、权限或 broker 查询失败时返回错误。
    pub async fn committed_offsets(
        &self,
        group: &str,
        tps: Vec<Tp>,
    ) -> Result<Vec<(Tp, Option<i64>)>> {
        if group.trim().is_empty() {
            return Err(NafkaError::Config("group 不能为空".into()));
        }
        validate_tps(&tps)?;
        self.client()?
            .committed_offsets(group.to_owned(), tps)
            .await
    }

    /// 业务作用：合并 committed offset 与 end watermark 计算离线滞后。
    ///
    /// # 参数
    ///
    /// - `group`: group id。
    /// - `tps`: 待计算分区。
    ///
    /// # 错误
    ///
    /// 任一底层查询失败时返回错误。
    pub async fn committed_lag(&self, group: &str, tps: Vec<Tp>) -> Result<Vec<KafkaPartitionLag>> {
        let committed = self.committed_offsets(group, tps.clone()).await?;
        let ends: BTreeMap<Tp, i64> = self
            .watermarks(tps)
            .await?
            .into_iter()
            .map(|(tp, _low, high)| (tp, high))
            .collect();
        committed
            .into_iter()
            .map(|(tp, committed)| {
                let end = *ends.get(&tp).ok_or_else(|| {
                    NafkaError::Broker(format!("watermark 结果缺少 {}:{}", tp.topic, tp.partition))
                })?;
                Ok(KafkaPartitionLag {
                    topic: tp.topic,
                    partition: tp.partition,
                    committed,
                    end,
                    lag: committed.map(|offset| end.saturating_sub(offset).max(0)),
                })
            })
            .collect()
    }
}

/// 业务作用：校验 topic 名不为空。
///
/// # 参数
///
/// - `topic`: 待校验 topic。
///
/// # 错误
///
/// topic 为空时返回配置错误。
fn validate_topic(topic: &str) -> Result<()> {
    if topic.trim().is_empty() {
        Err(NafkaError::Config("topic 不能为空".into()))
    } else {
        Ok(())
    }
}

/// 业务作用：校验创建 topic 的公共契约。
///
/// # 参数
///
/// - `spec`: 待校验契约。
///
/// # 错误
///
/// 名称为空、分区数或副本数非正时返回配置错误。
fn validate_spec(spec: &TopicSpec) -> Result<()> {
    validate_topic(&spec.name)?;
    if spec.partitions <= 0 {
        return Err(NafkaError::Config("topic partitions 必须大于零".into()));
    }
    if spec.replication_factor <= 0 {
        return Err(NafkaError::Config(
            "topic replication_factor 必须大于零".into(),
        ));
    }
    if spec.configs.keys().any(|key| key.trim().is_empty()) {
        return Err(NafkaError::Config("topic config key 不能为空".into()));
    }
    Ok(())
}

/// 业务作用：校验 topic-partition 列表并拒绝重复项。
///
/// # 参数
///
/// - `tps`: 待校验分区列表。
///
/// # 错误
///
/// 列表为空、topic 为空、分区为负或存在重复项时返回配置错误。
fn validate_tps(tps: &[Tp]) -> Result<()> {
    if tps.is_empty() {
        return Err(NafkaError::Config("topic-partition 列表不能为空".into()));
    }
    let mut unique = std::collections::BTreeSet::new();
    for tp in tps {
        validate_topic(&tp.topic)?;
        if tp.partition < 0 {
            return Err(NafkaError::Config(format!(
                "partition 不能为负数: {}:{}",
                tp.topic, tp.partition
            )));
        }
        if !unique.insert(tp.clone()) {
            return Err(NafkaError::Config(format!(
                "topic-partition 重复: {}:{}",
                tp.topic, tp.partition
            )));
        }
    }
    Ok(())
}
