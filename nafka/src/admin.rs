//! Topic 管理、元数据和离线 offset 查询的公共骨架。

use std::collections::BTreeMap;

use crate::error::{NafkaError, Result};
use crate::rd::admin::AdminHandle;
use crate::types::{KafkaPartitionLag, Tp};
use crate::KafkaProxy;

/// 业务作用：复用源级Admin连接，避免内部交付任务持有业务句柄造成生命周期环。
/// 参数说明：inner 为真实所属源。返回：共享原生责任；最终关闭门内拒绝重建。
pub(crate) fn shared_client(inner: &crate::KafkaProxyInner) -> Result<std::sync::Arc<AdminHandle>> {
    let ensure = || {
        if inner
            .managed_admin_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            Err(NafkaError::Lifecycle("受管 Kafka Admin 已关闭".into()))
        } else {
            Ok(())
        }
    };
    let current = inner
        .admin
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ensure()?;
    if let Some(client) = current.as_ref() {
        return Ok(std::sync::Arc::clone(client));
    }
    drop(current);
    let mut slot = inner
        .admin
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // 最终关闭持有同一写锁；复验准入后才能发布新连接，防止关闭后复活管理资源。
    ensure()?;
    if let Some(client) = slot.as_ref() {
        return Ok(std::sync::Arc::clone(client));
    }
    let candidate = AdminHandle::create(&inner.config)?;
    *slot = Some(std::sync::Arc::clone(&candidate));
    Ok(candidate)
}

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

/// 幂等创建的明确资源结果；已存在不等于既有规格与声明一致。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicCreation {
    /// 本次请求得到了目标 topic 创建成功确认。
    Created,
    /// broker 明确确认同名 topic 已存在，未验证其规格。
    AlreadyExists,
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
    /// 导出句柄额外继承的宿主准入边界，不影响内部收尾使用共享 Admin。
    scopes: Vec<crate::KafkaAccessScope>,
    /// 视图权限只允许收窄；克隆和附加宿主域不能恢复管理写入。
    read_only: bool,
}

impl KafkaAdmin {
    /// 业务作用：构造管理端轻量句柄。
    ///
    /// # 参数
    ///
    /// - `proxy`: 所属运行时。
    ///
    /// 返回：与该源最终关闭边界绑定的轻量句柄，尚未创建原生 Admin。
    pub(crate) fn new(proxy: KafkaProxy) -> Self {
        Self {
            proxy,
            scopes: Vec::new(),
            read_only: false,
        }
    }

    /// 业务作用：把管理视图永久收窄为只读诊断能力，不改变资源所有者的其它视图。
    /// 参数说明：无。
    /// 返回：克隆继承只读边界；写操作在建连前返回 Authorization，没有恢复写权限的入口。
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// 业务作用：说明当前视图是否禁止管理写入，供装配与权限诊断使用。
    /// 参数说明：无。
    /// 返回：只读限制是否已附加；false 不表示 broker 已授予 ACL 权限。
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// 业务作用：在提交外部管理副作用前核验当前视图的显式写入权。
    /// 参数说明：无。
    /// 返回：开放且未收窄的视图通过；终态为 Lifecycle，只读视图为 Authorization，均不提交请求。
    fn ensure_writable(&self) -> Result<()> {
        self.ensure_open()?;
        // 本地能力边界先于原生客户端准入，不能借 broker 的宽泛 ACL 提升业务视图权限。
        if self.read_only {
            return Err(NafkaError::Authorization);
        }
        Ok(())
    }

    /// 业务作用：为当前管理视图附加不可撤销的宿主准入边界。
    /// 参数说明：scope 为宿主持有并在最终清理时关闭的域。
    /// 返回：继承已有全部边界的句柄；克隆不能绕过关闭，已准入查询继续承担实际资源责任。
    pub fn with_access_scope(mut self, scope: &crate::KafkaAccessScope) -> Self {
        scope.attach(&mut self.scopes);
        self
    }

    /// 业务作用：明确保留管理句柄及克隆与所属源最终关闭边界的绑定。
    /// 参数说明：无。
    /// 返回：保留 graceful shutdown 阶段的显式管理能力，最终 Admin 关闭后永久拒绝新操作；克隆继承该边界。
    pub fn until_shutdown(self) -> Self {
        self
    }

    /// 业务作用：在复用或构造资源前拒绝已退出宿主的管理操作。
    /// 参数说明：无。
    /// 返回：源与附加宿主域均尚未关闭管理准入时通过，否则为 Lifecycle。
    fn ensure_open(&self) -> Result<()> {
        for scope in &self.scopes {
            scope.ensure_open()?;
        }
        if self
            .proxy
            .inner
            .managed_admin_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(NafkaError::Lifecycle("受管 Kafka Admin 已关闭".into()));
        }
        Ok(())
    }

    /// 业务作用：返回创建本管理句柄的稳定 Kafka client 身份，供多 client 路由与审计归因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用冻结配置中的 `client_name`；调用不会创建管理连接或访问 broker。
    pub fn client_name(&self) -> &str {
        &self.proxy.inner.config.client_name
    }

    /// 业务作用：读取来源物理集群身份，供回放合同和跨源操作拒绝错连。
    /// 参数说明：无，使用冻结的 Admin 总请求预算。
    /// 返回：broker 提供的非空 cluster ID；缺少证据、超时或客户端失败均拒绝，不以 bootstrap 地址代替身份。
    pub async fn cluster_id(&self) -> Result<String> {
        self.client()?.cluster_id().await
    }

    /// 业务作用：以明确隔离级别观测位置，保留逐分区、逐字段缺失原因。
    /// 参数说明：group 为可选组，tps 为至多10000个唯一分区，isolation 为读取隔离，timestamp_millis 为可选非负时间，timeout 为全部步骤共享的正预算。
    /// 返回：封存的部分证据报告，不订阅、提交或修改资源；非法声明与关闭源拒绝，取消等待保留实际查询责任。
    pub async fn observe_offsets(
        &self,
        group: Option<&str>,
        tps: Vec<Tp>,
        isolation: crate::OffsetIsolation,
        timestamp_millis: Option<i64>,
        timeout: std::time::Duration,
    ) -> Result<crate::OffsetObservation> {
        validate_tps(&tps)?;
        if tps.len() > 10000
            || timestamp_millis.is_some_and(|time| time < 0)
            || timeout.is_zero()
            || timeout > std::time::Duration::from_millis(i32::MAX as u64)
        {
            return Err(NafkaError::Config("位置观测声明无效".into()));
        }
        if let Some(group) = group {
            crate::wire::validate_group(group)?;
        }
        let deadline = std::time::Instant::now() + timeout;
        let report = crate::rd::offset_observation::observe(
            self.client()?,
            self.proxy.inner.config.clone(),
            group.map(str::to_owned),
            tps,
            isolation,
            timestamp_millis,
            deadline,
        )
        .await?;
        self.proxy.sample_offset_metrics(&report);
        Ok(report)
    }

    /// 业务作用：为有限历史回放读取完整 topic 保留起点、LSO 和可选时间定位，不加入消费组。
    /// 参数说明：topics 为 topic 白名单，timestamp_millis 为可选非负时间，timeout 为全部查询共用正预算且不超过五分钟。
    /// 返回：真实物理身份和完整范围；查询失败或范围不完整拒绝，不提交 offset。
    pub async fn replay_observation(
        &self,
        topics: std::collections::BTreeSet<String>,
        timestamp_millis: Option<i64>,
        timeout: std::time::Duration,
    ) -> crate::ReplayResult<crate::ReplayObservation> {
        self.ensure_open().map_err(|_| crate::ReplayError::Closed)?;
        crate::replay::observe(&self.proxy.inner, topics, timestamp_millis, timeout).await
    }

    /// 业务作用：取得或并发安全地初始化共享管理客户端。
    ///
    /// 客户端延迟构造；仅停止业务消费者后仍可执行明确的管理操作，最终 shutdown 关闭准入后不再重建连接。
    ///
    /// # 错误
    ///
    /// 原生配置或管理客户端构造失败时返回错误。
    /// 参数说明：无。
    /// 返回：通过源与宿主域门禁后的共享客户端；已关闭视图拒绝，不重建连接。
    fn client(&self) -> Result<std::sync::Arc<AdminHandle>> {
        // 对外视图先复验宿主域；内部 DLT 收尾仍独立使用源级共享连接。
        self.ensure_open()?;
        shared_client(&self.proxy.inner)
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
    /// 返回：明确存在为 true，明确不存在为 false；无权限与不完整响应不能用 false 代替。
    pub async fn topic_exists(&self, topic: &str) -> Result<bool> {
        validate_topic(topic)?;
        self.client()?.topic_exists(topic.to_owned()).await
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
    /// 返回：目标资源的实际分区号；只请求 metadata，不要求 DescribeConfigs 权限。
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<i32>> {
        validate_topic(topic)?;
        Ok(self
            .client()?
            .describe_topic(topic.to_owned(), false)
            .await?
            .partitions)
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

    /// 业务作用：只读核对目标 topic 的分区、副本及要求的配置子集。
    /// 参数说明：spec 为冻结的预期合同，timeout 为元数据与配置共用正预算。
    /// 返回：匹配时返回实际描述；漂移为 TopicContract，权限和观测失败保留原分类，不创建或修改资源。
    pub async fn verify_topic(
        &self,
        spec: TopicSpec,
        timeout: std::time::Duration,
    ) -> Result<TopicDescription> {
        validate_spec(&spec)?;
        if timeout.is_zero() || timeout > std::time::Duration::from_millis(i32::MAX as u64) {
            return Err(NafkaError::Config("topic 核验预算无效".into()));
        }
        let deadline = std::time::Instant::now() + timeout;
        let actual = self
            .client()?
            .describe_topic_until(spec.name.clone(), !spec.configs.is_empty(), deadline)
            .await?;
        if actual.partitions.len() != spec.partitions as usize
            || actual
                .partitions
                .iter()
                .enumerate()
                .any(|(index, &partition)| partition != index as i32)
            || actual.replication_factor != Some(spec.replication_factor)
        {
            return Err(NafkaError::TopicContract("分区或副本不满足声明".into()));
        }
        if spec.configs.iter().any(|(key, expected)| {
            actual.configs.get(key).and_then(Option::as_ref) != Some(expected)
        }) {
            return Err(NafkaError::TopicContract("配置未取得匹配证据".into()));
        }
        Ok(actual)
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
        self.ensure_writable()?;
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
    /// 返回：明确创建或已存在均为成功；已提交请求的超时为 AdminOutcomeUnknown，取消等待不撤销远端修改。
    pub async fn create_if_absent(&self, spec: TopicSpec) -> Result<()> {
        self.create_if_absent_result(spec).await.map(|_| ())
    }

    /// 业务作用：幂等创建 topic，并区分本次创建与已经存在，供并发初始化协调。
    /// 参数说明：spec 为资源名、分区、副本及初始配置契约。
    /// 返回：仅明确资源确认返回 Created 或 AlreadyExists；权限拒绝单独报告，等待取消不撤销已提交请求，超时为 AdminOutcomeUnknown。
    pub async fn create_if_absent_result(&self, spec: TopicSpec) -> Result<TopicCreation> {
        self.ensure_writable()?;
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
        self.ensure_writable()?;
        validate_topic(topic)?;
        self.client()?.delete_topic(topic).await
    }

    /// 业务作用：增量设置已有 topic 的单项动态配置，保留请求未声明的配置项。
    /// 参数说明：topic 为目标资源，key 为动态配置键，value 为新值；配置内容是否合法由 broker 裁决。
    /// 返回：目标资源明确确认才成功；非法目标、权限或 broker 拒绝返回错误，已提交请求的超时或响应缺失为 AdminOutcomeUnknown。
    /// broker 不支持增量修改时返回 UnsupportedCapability，不退回整组替换；同一配置键的并发写仍由 broker 决定最终值。
    pub async fn alter_topic_config(&self, topic: &str, key: &str, value: &str) -> Result<()> {
        self.alter_topic_configs(
            topic,
            BTreeMap::from([(key.to_owned(), Some(value.to_owned()))]),
        )
        .await
    }

    /// 业务作用：一次增量设置或恢复目标 topic 的多个配置，保留所有未声明项。
    /// 参数说明：topic 为目标；changes 的 Some 值表示 SET，None 表示 DELETE 并恢复 broker 默认配置。
    /// 返回：唯一目标资源明确确认才成功；空变更、空键或 NUL 在原生提交前拒绝，超时为 AdminOutcomeUnknown，不退回整组替换。
    pub async fn alter_topic_configs(
        &self,
        topic: &str,
        changes: BTreeMap<String, Option<String>>,
    ) -> Result<()> {
        self.ensure_writable()?;
        validate_topic(topic)?;
        if changes.is_empty()
            || changes.iter().any(|(key, value)| {
                key.trim().is_empty()
                    || key.contains('\0')
                    || value.as_ref().is_some_and(|value| value.contains('\0'))
            })
        {
            return Err(NafkaError::Config(
                "topic 配置变更不能为空且键值不能含 NUL".into(),
            ));
        }
        self.client()?.alter_topic_configs(topic, changes).await
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
    /// 返回：目标资源确认才成功；未知结果要求回读实际分区数，不能直接再次扩容。
    pub async fn increase_partitions(&self, topic: &str, total_partitions: i32) -> Result<()> {
        self.ensure_writable()?;
        validate_topic(topic)?;
        if total_partitions <= 0 {
            return Err(NafkaError::Config("total_partitions 必须大于零".into()));
        }
        let total = usize::try_from(total_partitions)
            .map_err(|_| NafkaError::Config("total_partitions 超出 usize".into()))?;
        // 是否超过现有分区数由同一次 broker 操作裁决，避免前置快照竞态及重复消耗完整查询预算。
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

    /// 业务作用：使用不加入消费组的短命客户端查询 broker 已提交位置。
    /// 参数说明：group 为待查的原文组名；tps 为目标分区。
    /// 返回：逐分区提交位置，None 仅表示明确无历史；非法组为 Config，权限拒绝、响应缺项与 broker 失败保留错误。
    pub async fn committed_offsets(
        &self,
        group: &str,
        tps: Vec<Tp>,
    ) -> Result<Vec<(Tp, Option<i64>)>> {
        // 管理查询必须与消费准入使用相同组名，不能拒绝已接纳身份或查询其替代名称。
        crate::wire::validate_group(group)?;
        validate_tps(&tps)?;
        self.client()?
            .committed_offsets(group.to_owned(), tps)
            .await
    }

    /// 业务作用：组合已提交位置与普通日志末尾，计算分区的 offset 距离。
    /// 参数说明：group 为原文组名；tps 为待计算分区。
    /// 返回：逐分区滞后报告；非法组为 Config，其它查询失败保留错误。两次查询共享总预算，不构成原子快照，距离不等于业务消息数。
    pub async fn committed_lag(&self, group: &str, tps: Vec<Tp>) -> Result<Vec<KafkaPartitionLag>> {
        // 管理查询必须与消费准入使用相同组名，不能拒绝已接纳身份或查询其替代名称。
        crate::wire::validate_group(group)?;
        validate_tps(&tps)?;
        let config = &self.proxy.inner.config;
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(
                config
                    .admin
                    .default_api_timeout_ms
                    .unwrap_or(config.behavior.publish_timeout_ms),
            );
        let client = self.client()?;
        let committed = client
            .committed_offsets_until(group.to_owned(), tps.clone(), deadline)
            .await?;
        let ends: BTreeMap<Tp, i64> = client
            .watermarks_until(tps, deadline)
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
    if topic.trim().is_empty() || topic.contains('\0') {
        Err(NafkaError::Config("topic 不能为空或含 NUL".into()))
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
/// 名称非法、分区非正、副本数不在正 i16 范围或配置键值非法时返回配置错误。
fn validate_spec(spec: &TopicSpec) -> Result<()> {
    validate_topic(&spec.name)?;
    if spec.partitions <= 0 {
        return Err(NafkaError::Config("topic partitions 必须大于零".into()));
    }
    if spec.replication_factor <= 0 || spec.replication_factor > i32::from(i16::MAX) {
        return Err(NafkaError::Config(
            "topic replication_factor 必须在 1 到 i16::MAX 之间".into(),
        ));
    }
    if spec
        .configs
        .iter()
        .any(|(key, value)| key.trim().is_empty() || key.contains('\0') || value.contains('\0'))
    {
        return Err(NafkaError::Config("topic 配置键值非法".into()));
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
