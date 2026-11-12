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
    /// 业务作用：转为通用管理端创建契约。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含名称、分区、副本及四项 topic 配置的创建请求；不连接 broker 或验证预期值。
    /// 调用方须先完成本地契约校验，再根据管理策略决定是否允许创建。
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
    /// 承载集群状态控制事件的 topic 属性契约。
    pub control: WsKafkaTopicSpec,
    /// 承载业务载荷及路由头的数据 topic 属性契约。
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
    /// 业务作用：完成静态校验，并按策略创建或核验全部 topic。
    ///
    /// 本方法从不删除、改配置或增加已有 topic 的分区。
    ///
    /// 参数说明：
    ///
    /// - `admin`: 已冻结 KafkaProxy 的管理端。
    /// - `limits`: runtime 的容量与 presence 约束。
    ///
    /// # 错误
    ///
    /// 配置非法、权限不足、topic 缺失或任一实际属性漂移时返回 TopicContract 错误。
    ///
    /// 返回：全部 topic 创建策略及实际属性检查完成时成功；失败或取消时不回滚已经创建的 topic。
    /// 成功只说明本次启动检查成立，持续运行期间的漂移仍须另行审计。
    pub async fn enforce(
        &self,
        admin: &KafkaAdmin,
        limits: TopicContractLimits,
    ) -> nafka::Result<()> {
        // 先完整校验本地契约，避免配置已知无效时仍对 broker 产生部分创建副作用。
        self.validate(limits)?;
        for spec in self.specs() {
            let exists = admin
                .topic_exists(&spec.name)
                .await
                .map_err(|error| contract_error(&spec.name, error))?;
            if !exists {
                match self.management {
                    TopicManagement::ValidateOnly => {
                        // 只读部署不能通过自动创建掩盖缺失配置，必须保持启动失败。
                        return Err(topic_error(format!("topic `{}` 不存在", spec.name)));
                    }
                    // 只有显式创建策略允许此副作用；创建后仍需复核实际属性，不能直接放行业务。
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

    /// 业务作用：只读复核全部 topic，供运行期漂移监控使用。
    ///
    /// 即使 management 为 CreateIfAbsent，本方法也绝不创建或修改 topic；运行期删除和漂移必须
    /// 暴露为错误并由 runtime 关闭 ingress。
    ///
    /// 参数说明：
    ///
    /// - `admin`: 已冻结 KafkaProxy 的管理端。
    /// - `limits`: runtime 的容量与 presence 约束。
    ///
    /// # 错误
    ///
    /// 已成功观测后的 topic 缺失或属性漂移返回 TopicContract；broker、传输或权限导致的观测失败
    /// 保留底层错误 variant，由 runtime 的连续失败预算决定是否停止 ingress。
    ///
    /// 返回：本轮读取的全部 topic 符合契约时成功；首个失败立即返回，不修改 broker。
    /// 该函数只报告观测结果，不直接关闭 ingress，也不把跨 topic 的逐次读取变成原子快照。
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

    /// 业务作用：开发创建模式下吸收 broker metadata 的短暂传播窗口。
    ///
    /// 参数说明：
    /// - `admin`: 查询 topic 元数据及配置的管理端。
    /// - `spec`: 需要等待可见的 topic 预期定义，此处只使用其名称。
    ///
    /// 返回：首次成功取得的描述；只读策略尝试一次，创建策略最多尝试一百次，失败间隔五十毫秒。
    /// 重试不区分底层错误类型，全部失败时包装最后一个错误；不校验实际属性或限制每次查询的耗时。
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
                // 创建后 metadata 传播可能滞后；仅在尚有尝试次数时等待，避免立即判定不可见。
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        Err(contract_error(
            &spec.name,
            last_error.unwrap_or_else(|| NafkaError::Broker("topic 描述未返回结果".into())),
        ))
    }

    /// 业务作用：只校验本地可判定的不变量。
    ///
    /// 参数说明：
    ///
    /// - `limits`: runtime 的容量与 presence 约束。
    ///
    /// # 错误
    ///
    /// topic、容量、生产 durability 或 assignment 上限非法时返回 TopicContract 错误。
    ///
    /// 返回：本地预期满足容量、保留时间、命名唯一性及管理策略约束时成功；否则返回首个契约错误。
    /// 不修改预期值、不访问 broker，也不证明实际 topic 或生产者持久性配置已满足要求。
    pub fn validate(&self, limits: TopicContractLimits) -> nafka::Result<()> {
        // 只读生产策略必须取得真实配置证据，否则无法确认保留时间与持久性要求。
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
        // 控制消息至少覆盖多个完整 presence 周期，避免单次快照丢失后目录缺少恢复依据。
        if self.control.retention_ms < min_control_retention {
            return Err(topic_error(format!(
                "control retention_ms({}) 小于三倍 presence 间隔({min_control_retention})",
                self.control.retention_ms
            )));
        }
        // 只读生产策略还要求最低副本与同步副本冗余，不能沿用开发环境的低副本配置。
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

    /// 业务作用：以稳定顺序遍历 control、data 和 DLT 契约。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：依次借用控制、数据和所列死信 topic 的迭代器；不去重、不查询 broker，允许死信列表为空。
    fn specs(&self) -> impl Iterator<Item = &WsKafkaTopicSpec> {
        std::iter::once(&self.control)
            .chain(std::iter::once(&self.data))
            .chain(self.dead_letters.iter())
    }
}

/// 业务作用：校验一个 topic 的本地预期值。
///
/// 参数说明：
/// - `spec`: 待检查的名称、容量、副本和保留策略。
/// - `required_message_bytes`: 完整记录连同路由和协议头必须能容纳的最小字节数。
/// - `max_assignment_partitions`: 单个消费组允许管理的最大分区数。
///
/// 返回：基本值、delete 策略、分区与消息容量均满足约束时成功，否则返回契约错误。
/// 不修改配置，不验证 broker 可用性或名称的全部 broker 语法约束。
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
    // 分区数量不能超过运行时 assignment 容量，否则消费者无法完整承担该 topic。
    if partitions > max_assignment_partitions {
        return Err(topic_error(format!(
            "topic `{}` 分区数({partitions}) 超过 assignment 上限({max_assignment_partitions})",
            spec.name
        )));
    }
    // 只容纳 payload 不足以交付完整记录，topic 限制必须覆盖 header 和协议预算。
    if spec.max_message_bytes < required_message_bytes {
        return Err(topic_error(format!(
            "topic `{}` max.message.bytes({}) 小于 record+header 预算({required_message_bytes})",
            spec.name, spec.max_message_bytes
        )));
    }
    Ok(())
}

/// 业务作用：把 broker 实际值与冻结契约逐项核验。
///
/// 参数说明：
/// - `expected`: 已通过本地校验的 topic 预期定义。
/// - `actual`: 管理端为同一 topic 获取的当前描述。
/// - `strict_configs`: 是否同时要求清理策略、保留时间、最小同步副本和消息容量符合要求。
///
/// 返回：分区数、副本因子以及启用时的配置检查全部满足后成功；缺失或漂移返回契约错误。
/// 消息容量允许实际值更大，其余受检项必须精确匹配；不检查实时 ISR 数或分区领导者健康。
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
    // 未启用配置读取时仅能确认拓扑属性，不能把此成功结果当作全部持久性配置已核实。
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

/// 业务作用：校验一个配置项；消息上限允许 broker 实际值更大。
///
/// 参数说明：
/// - `expected`: 预期 topic 定义，用于错误中标明配置归属。
/// - `actual`: 包含 broker 实际配置值的描述。
/// - `key`: 要核对的固定配置键。
/// - `expected_value`: 冻结契约要求的字符串或非负整数文本。
/// - `allow_greater`: 为 true 时按 usize 比较下限，否则执行字符串精确匹配。
///
/// 返回：实际值存在且满足比较规则时成功；缺失、null、数值解析失败或不匹配返回契约错误。
/// 错误包含配置键和比较值，不适用于承载秘密的任意配置项。
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

/// 业务作用：构造稳定 TopicContract 错误。
///
/// 参数说明：
/// - `message`: 当前契约不成立的具体原因。
///
/// 返回：保留原因文本的 `NafkaError::TopicContract`，供运行时与短暂 broker 观测失败区分。
fn topic_error(message: impl Into<String>) -> NafkaError {
    NafkaError::TopicContract(message.into())
}

/// 业务作用：给底层错误补充 topic 归因并收敛为 TopicContract。
///
/// 参数说明：
/// - `topic`: 启动创建或描述阶段失败的 topic 名称。
/// - `error`: 管理端返回的底层错误。
///
/// 返回：包含 topic 和底层错误显示文本的契约错误，原错误分类不再保留。
/// 运行期审计需区分短暂观测失败时，不应通过此方法改写错误分类。
fn contract_error(topic: &str, error: NafkaError) -> NafkaError {
    topic_error(format!("topic `{topic}` 查询或创建失败: {error}"))
}
