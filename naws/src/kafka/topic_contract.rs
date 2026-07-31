//! socket 集群 Kafka topic 的冻结启动契约。

use std::collections::BTreeSet;

use nafka::{KafkaAdmin, NafkaError, TopicDescription, TopicSpec};

/// runtime 对 topic 的管理权限边界。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicManagement {
    /// 生产默认：只检查预建 topic，绝不修改 broker。
    ValidateOnly,
    /// 仅开发环境：缺失时创建，已存在但漂移仍失败。
    CreateIfAbsent,
}

/// 单个 socket Kafka topic 的预期属性。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsKafkaTopicSpec {
    /// topic 名。
    pub name: String,
    /// 精确分区数。
    pub partitions: i32,
    /// 精确副本因子。
    pub replication_factor: i16,
    /// 精确最小同步副本数。
    pub min_insync_replicas: i32,
    /// 精确清理策略；v1 只允许 `delete`。
    pub cleanup_policy: String,
    /// 精确保留时长，单位毫秒。
    pub retention_ms: i64,
    /// topic 接受的最大单消息字节数。
    pub max_message_bytes: usize,
}

impl WsKafkaTopicSpec {
    /// 转为通用管理端创建契约。
    fn create_spec(&self) -> TopicSpec {
        TopicSpec::new(
            self.name.clone(),
            self.partitions,
            i32::from(self.replication_factor),
        )
        .config("cleanup.policy", self.cleanup_policy.clone())
        .config("retention.ms", self.retention_ms.to_string())
        .config("min.insync.replicas", self.min_insync_replicas.to_string())
        .config("max.message.bytes", self.max_message_bytes.to_string())
    }
}

/// control、data 与 DLT topic 的整体冻结契约。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsKafkaTopicContract {
    /// topic 管理策略。
    pub management: TopicManagement,
    /// control plane topic。
    pub control: WsKafkaTopicSpec,
    /// data plane topic。
    pub data: WsKafkaTopicSpec,
    /// 一个或多个死信 topic。
    pub dead_letters: Vec<WsKafkaTopicSpec>,
    /// 是否必须取得并逐项核验 DescribeConfigs。
    pub strict_describe_configs: bool,
}

/// 由 runtime 注入的 topic 容量和 presence 约束。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicContractLimits {
    /// 完整 presence 快照间隔，单位毫秒。
    pub full_presence_interval_ms: u64,
    /// nafka 接受的最大 record 字节数。
    pub max_record_bytes: usize,
    /// 路由 header 最大字节数。
    pub max_route_header_bytes: usize,
    /// 固定协议、key 与 DLT header 的保守字节预算。
    pub protocol_header_budget_bytes: usize,
    /// 单 group 最大 assignment 分区数。
    pub max_assignment_partitions: usize,
}

impl WsKafkaTopicContract {
    /// 完成静态校验，并按策略创建或核验全部 topic。
    ///
    /// 本方法从不删除、改配置或增加已有 topic 的分区。
    ///
    /// # 参数
    ///
    /// - `admin`: 已冻结 KafkaProxy 的管理端。
    /// - `limits`: runtime 的容量与 presence 约束。
    ///
    /// # 错误
    ///
    /// 配置非法、权限不足、topic 缺失或任一实际属性漂移时返回 TopicContract 错误。
    pub async fn enforce(
        &self,
        admin: &KafkaAdmin,
        limits: TopicContractLimits,
    ) -> nafka::Result<()> {
        self.validate(limits)?;
        for spec in self.specs() {
            let exists = admin
                .topic_exists(&spec.name)
                .await
                .map_err(|error| contract_error(&spec.name, error))?;
            if !exists {
                match self.management {
                    TopicManagement::ValidateOnly => {
                        return Err(topic_error(format!("topic `{}` 不存在", spec.name)))
                    }
                    TopicManagement::CreateIfAbsent => admin
                        .create_if_absent(spec.create_spec())
                        .await
                        .map_err(|error| contract_error(&spec.name, error))?,
                }
            }
        }
        for spec in self.specs() {
            let actual = self.describe_with_visibility_retry(admin, spec).await?;
            validate_actual(spec, &actual, self.strict_describe_configs)?;
        }
        Ok(())
    }

    /// 只读复核全部 topic，供运行期漂移监控使用。
    ///
    /// 即使 management 为 CreateIfAbsent，本方法也绝不创建、修改或修复 topic；运行期删除和漂移必须
    /// 暴露为错误并由 runtime 关闭 ingress。
    ///
    /// # 参数
    ///
    /// - `admin`: 已冻结 KafkaProxy 的管理端。
    /// - `limits`: runtime 的容量与 presence 约束。
    ///
    /// # 错误
    ///
    /// 已成功观测后的 topic 缺失或属性漂移返回 TopicContract；broker、传输或权限导致的观测失败
    /// 保留底层错误 variant，由 runtime 的连续失败预算决定是否停止 ingress。
    pub async fn audit(
        &self,
        admin: &KafkaAdmin,
        limits: TopicContractLimits,
    ) -> nafka::Result<()> {
        self.validate(limits)?;
        for spec in self.specs() {
            // 保留 broker/传输错误的原始 variant，让 runtime 能把短时观测失败与
            // 已成功读取后的确定契约漂移分开处理。
            let exists = admin.topic_exists(&spec.name).await?;
            if !exists {
                return Err(topic_error(format!("topic `{}` 不存在", spec.name)));
            }
            let actual = admin
                .describe_topic(&spec.name, self.strict_describe_configs)
                .await?;
            validate_actual(spec, &actual, self.strict_describe_configs)?;
        }
        Ok(())
    }

    /// 开发创建模式下吸收 broker metadata 的短暂传播窗口。
    async fn describe_with_visibility_retry(
        &self,
        admin: &KafkaAdmin,
        spec: &WsKafkaTopicSpec,
    ) -> nafka::Result<TopicDescription> {
        let attempts = if matches!(self.management, TopicManagement::CreateIfAbsent) {
            100
        } else {
            1
        };
        let mut last_error = None;
        for attempt in 0..attempts {
            match admin
                .describe_topic(&spec.name, self.strict_describe_configs)
                .await
            {
                Ok(actual) => return Ok(actual),
                Err(error) => last_error = Some(error),
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        Err(contract_error(
            &spec.name,
            last_error.unwrap_or_else(|| NafkaError::Broker("topic 描述未返回结果".into())),
        ))
    }

    /// 只校验本地可判定的不变量。
    ///
    /// # 参数
    ///
    /// - `limits`: runtime 的容量与 presence 约束。
    ///
    /// # 错误
    ///
    /// topic、容量、生产 durability 或 assignment 上限非法时返回 TopicContract 错误。
    pub fn validate(&self, limits: TopicContractLimits) -> nafka::Result<()> {
        if matches!(self.management, TopicManagement::ValidateOnly) && !self.strict_describe_configs
        {
            return Err(topic_error("ValidateOnly 必须启用 strict_describe_configs"));
        }
        if limits.full_presence_interval_ms == 0
            || limits.max_record_bytes == 0
            || limits.max_route_header_bytes == 0
            || limits.protocol_header_budget_bytes == 0
            || limits.max_assignment_partitions == 0
        {
            return Err(topic_error("runtime topic 容量约束必须全部大于零"));
        }
        let required_message_bytes = limits
            .max_record_bytes
            .checked_add(limits.max_route_header_bytes)
            .and_then(|value| value.checked_add(limits.protocol_header_budget_bytes))
            .ok_or_else(|| topic_error("record 与 header 字节预算溢出"))?;
        let min_control_retention = limits
            .full_presence_interval_ms
            .checked_mul(3)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| topic_error("presence retention 预算溢出"))?;
        let mut names = BTreeSet::new();
        for spec in self.specs() {
            validate_spec(
                spec,
                required_message_bytes,
                limits.max_assignment_partitions,
            )?;
            if !names.insert(spec.name.as_str()) {
                return Err(topic_error(format!("topic 名重复: `{}`", spec.name)));
            }
        }
        if self.control.retention_ms < min_control_retention {
            return Err(topic_error(format!(
                "control retention_ms({}) 小于三倍 presence 间隔({min_control_retention})",
                self.control.retention_ms
            )));
        }
        if matches!(self.management, TopicManagement::ValidateOnly) {
            for spec in self.specs() {
                if spec.replication_factor < 3 || spec.min_insync_replicas < 2 {
                    return Err(topic_error(format!(
                        "生产 topic `{}` 要求 replication_factor>=3 且 min.insync.replicas>=2",
                        spec.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// 以稳定顺序遍历 control、data 和 DLT 契约。
    fn specs(&self) -> impl Iterator<Item = &WsKafkaTopicSpec> {
        std::iter::once(&self.control)
            .chain(std::iter::once(&self.data))
            .chain(self.dead_letters.iter())
    }
}

/// 校验一个 topic 的本地预期值。
fn validate_spec(
    spec: &WsKafkaTopicSpec,
    required_message_bytes: usize,
    max_assignment_partitions: usize,
) -> nafka::Result<()> {
    if spec.name.trim().is_empty()
        || spec.partitions <= 0
        || spec.replication_factor <= 0
        || spec.min_insync_replicas <= 0
        || spec.min_insync_replicas > i32::from(spec.replication_factor)
        || spec.retention_ms <= 0
        || spec.max_message_bytes == 0
    {
        return Err(topic_error(format!(
            "topic `{}` 的名称、分区、副本、ISR、retention 或消息上限非法",
            spec.name
        )));
    }
    if spec.cleanup_policy != "delete" {
        return Err(topic_error(format!(
            "topic `{}` v1 cleanup.policy 必须为 delete",
            spec.name
        )));
    }
    let partitions = usize::try_from(spec.partitions)
        .map_err(|_| topic_error(format!("topic `{}` 分区数无法转换", spec.name)))?;
    if partitions > max_assignment_partitions {
        return Err(topic_error(format!(
            "topic `{}` 分区数({partitions}) 超过 assignment 上限({max_assignment_partitions})",
            spec.name
        )));
    }
    if spec.max_message_bytes < required_message_bytes {
        return Err(topic_error(format!(
            "topic `{}` max.message.bytes({}) 小于 record+header 预算({required_message_bytes})",
            spec.name, spec.max_message_bytes
        )));
    }
    Ok(())
}

/// 把 broker 实际值与冻结契约逐项核验。
fn validate_actual(
    expected: &WsKafkaTopicSpec,
    actual: &TopicDescription,
    strict_configs: bool,
) -> nafka::Result<()> {
    let expected_partitions =
        usize::try_from(expected.partitions).map_err(|_| topic_error("预期分区数无法转换"))?;
    if actual.partitions.len() != expected_partitions {
        return Err(topic_error(format!(
            "topic `{}` 分区漂移 expected={} actual={}",
            expected.name,
            expected.partitions,
            actual.partitions.len()
        )));
    }
    if actual.replication_factor != Some(i32::from(expected.replication_factor)) {
        return Err(topic_error(format!(
            "topic `{}` 副本漂移 expected={} actual={:?}",
            expected.name, expected.replication_factor, actual.replication_factor
        )));
    }
    if !strict_configs {
        return Ok(());
    }
    validate_config(
        expected,
        actual,
        "cleanup.policy",
        &expected.cleanup_policy,
        false,
    )?;
    validate_config(
        expected,
        actual,
        "retention.ms",
        &expected.retention_ms.to_string(),
        false,
    )?;
    validate_config(
        expected,
        actual,
        "min.insync.replicas",
        &expected.min_insync_replicas.to_string(),
        false,
    )?;
    validate_config(
        expected,
        actual,
        "max.message.bytes",
        &expected.max_message_bytes.to_string(),
        true,
    )
}

/// 校验一个配置项；消息上限允许 broker 实际值更大。
fn validate_config(
    expected: &WsKafkaTopicSpec,
    actual: &TopicDescription,
    key: &str,
    expected_value: &str,
    allow_greater: bool,
) -> nafka::Result<()> {
    let value = actual
        .configs
        .get(key)
        .and_then(Option::as_deref)
        .ok_or_else(|| topic_error(format!("topic `{}` 缺少配置 `{key}`", expected.name)))?;
    let matches = if allow_greater {
        value
            .parse::<usize>()
            .ok()
            .zip(expected_value.parse::<usize>().ok())
            .is_some_and(|(actual, expected)| actual >= expected)
    } else {
        value == expected_value
    };
    if matches {
        Ok(())
    } else {
        Err(topic_error(format!(
            "topic `{}` 配置 `{key}` 漂移 expected={expected_value} actual={value}",
            expected.name
        )))
    }
}

/// 构造稳定 TopicContract 错误。
fn topic_error(message: impl Into<String>) -> NafkaError {
    NafkaError::TopicContract(message.into())
}

/// 给底层错误补充 topic 归因并收敛为 TopicContract。
fn contract_error(topic: &str, error: NafkaError) -> NafkaError {
    topic_error(format!("topic `{topic}` 查询或创建失败: {error}"))
}
