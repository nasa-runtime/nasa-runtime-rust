//! 三端原生配置合并、安全字段注入与框架不变量覆写。

use std::collections::BTreeMap;

use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;

use crate::config::{KafkaConfig, ProducerLaneOverride};
use crate::error::{NafkaError, Result};

/// 单个物理 producer lane 合并后的完整配置。
#[derive(Clone, Debug)]
pub(crate) struct EffectiveProducerLane {
    /// ack 策略。
    pub(crate) acks: String,
    /// 底层发送重试次数。
    pub(crate) retries: u32,
    /// 重试退避毫秒。
    pub(crate) retry_backoff_ms: u64,
    /// 聚批等待毫秒。
    pub(crate) linger_ms: u64,
    /// 单批字节数。
    pub(crate) batch_size: usize,
    /// 单请求超时毫秒。
    pub(crate) request_timeout_ms: u64,
    /// 投递总超时毫秒。
    pub(crate) delivery_timeout_ms: u64,
    /// 非 fatal 无成功 delivery 达到该时长后允许切换 generation。
    pub(crate) stalled_rebuild_after_ms: u64,
    /// 非 fatal generation 切换前的连续失败阈值。
    pub(crate) stalled_rebuild_min_failures: u32,
    /// 非 fatal generation 切换冷却时长。
    pub(crate) stalled_rebuild_cooldown_ms: u64,
    /// 分区器名称。
    pub(crate) partitioner: String,
    /// 压缩算法。
    pub(crate) compression: String,
    /// 是否启用幂等。
    pub(crate) enable_idempotence: bool,
    /// 单连接最大并行在途请求数。
    pub(crate) max_in_flight_requests_per_connection: u32,
    /// fire 投递观察容量。
    pub(crate) fire_observer_capacity: usize,
    /// 底层队列消息数上限。
    pub(crate) queue_buffering_max_messages: usize,
    /// 底层队列 KiB 上限。
    pub(crate) queue_buffering_max_kbytes: usize,
    /// lane 原生透传属性。
    pub(crate) properties: BTreeMap<String, String>,
}

/// 业务作用：合并源默认producer与一个lane的显式覆盖，先验证可靠发送及有界容量合同。
/// 参数说明：config为冻结的源配置；lane为default或已声明名称，命名lane只继承源默认值。
/// 返回：合法的独立有效配置；未知lane、非法容量/时限及幂等顺序冲突返回错误，不创建原生资源。
pub(crate) fn effective_producer_lane(
    config: &KafkaConfig,
    lane: &str,
) -> Result<EffectiveProducerLane> {
    let base = &config.producer;
    let override_ = if lane == "default" {
        None
    } else {
        Some(
            base.lanes
                .get(lane)
                .ok_or_else(|| NafkaError::NoSuchProducerLane(lane.to_owned()))?,
        )
    };
    let pick = |field: fn(&ProducerLaneOverride) -> Option<u64>, default| {
        override_.and_then(field).unwrap_or(default)
    };
    let effective = EffectiveProducerLane {
        acks: override_
            .and_then(|value| value.acks.clone())
            .unwrap_or_else(|| base.acks.clone()),
        retries: override_
            .and_then(|value| value.retries)
            .unwrap_or(base.retries),
        retry_backoff_ms: pick(|value| value.retry_backoff_ms, base.retry_backoff_ms),
        linger_ms: pick(|value| value.linger_ms, base.linger_ms),
        batch_size: override_
            .and_then(|value| value.batch_size)
            .unwrap_or(base.batch_size),
        request_timeout_ms: pick(|value| value.request_timeout_ms, base.request_timeout_ms),
        delivery_timeout_ms: pick(|value| value.delivery_timeout_ms, base.delivery_timeout_ms),
        stalled_rebuild_after_ms: pick(
            |value| value.stalled_rebuild_after_ms,
            base.stalled_rebuild_after_ms,
        ),
        stalled_rebuild_min_failures: override_
            .and_then(|value| value.stalled_rebuild_min_failures)
            .unwrap_or(base.stalled_rebuild_min_failures),
        stalled_rebuild_cooldown_ms: pick(
            |value| value.stalled_rebuild_cooldown_ms,
            base.stalled_rebuild_cooldown_ms,
        ),
        partitioner: base.partitioner.clone(),
        compression: override_
            .and_then(|value| value.compression.clone())
            .unwrap_or_else(|| base.compression.clone()),
        enable_idempotence: override_
            .and_then(|value| value.enable_idempotence)
            .unwrap_or(base.enable_idempotence),
        max_in_flight_requests_per_connection: override_
            .and_then(|value| value.max_in_flight_requests_per_connection)
            .unwrap_or(base.max_in_flight_requests_per_connection),
        fire_observer_capacity: override_
            .and_then(|value| value.fire_observer_capacity)
            .unwrap_or(base.fire_observer_capacity),
        queue_buffering_max_messages: override_
            .and_then(|value| value.queue_buffering_max_messages)
            .unwrap_or(base.queue_buffering_max_messages),
        queue_buffering_max_kbytes: override_
            .and_then(|value| value.queue_buffering_max_kbytes)
            .unwrap_or(base.queue_buffering_max_kbytes),
        properties: override_
            .map(|value| value.properties.clone())
            .unwrap_or_default(),
    };
    // 这些字段将进入原生有符号整数配置；提前拒绝无界、无确认超时与无效容量，不能等待首次发送再失败。
    if !matches!(effective.acks.as_str(), "all" | "-1" | "0" | "1")
        || effective.request_timeout_ms == 0
        || effective.delivery_timeout_ms == 0
        || effective.batch_size == 0
        || effective.batch_size > i32::MAX as usize
        || effective.retries > i32::MAX as u32
        || effective.max_in_flight_requests_per_connection == 0
        || effective.queue_buffering_max_messages == 0
        || effective.queue_buffering_max_messages > i32::MAX as usize
        || effective.queue_buffering_max_kbytes == 0
        || effective.queue_buffering_max_kbytes > i32::MAX as usize
        || effective.fire_observer_capacity == 0
    {
        return Err(NafkaError::Config(
            "producer确认策略、请求时限或队列容量非法".into(),
        ));
    }
    if effective.delivery_timeout_ms
        < effective
            .request_timeout_ms
            .saturating_add(effective.linger_ms)
    {
        return Err(NafkaError::Config(format!(
            "producer lane `{lane}` delivery_timeout_ms 必须覆盖 request_timeout_ms + linger_ms"
        )));
    }
    if effective.stalled_rebuild_after_ms == 0
        || effective.stalled_rebuild_min_failures == 0
        || effective.stalled_rebuild_min_failures > i32::MAX as u32
        || effective.stalled_rebuild_cooldown_ms == 0
    {
        return Err(NafkaError::Config(format!(
            "producer lane `{lane}` stalled rebuild 的时长、失败阈值与冷却均必须 > 0"
        )));
    }
    if effective.enable_idempotence && (effective.acks != "all" && effective.acks != "-1") {
        return Err(NafkaError::Config(format!(
            "producer lane `{lane}` 启用幂等时 acks 必须为 all"
        )));
    }
    if effective.enable_idempotence
        && (effective.retries == 0 || effective.max_in_flight_requests_per_connection > 5)
    {
        return Err(NafkaError::Config(format!(
            "producer lane `{lane}` 启用幂等时 retries 必须 > 0 且 max_in_flight_requests_per_connection <= 5"
        )));
    }
    if !effective.enable_idempotence
        && effective.retries > 0
        && effective.max_in_flight_requests_per_connection > 1
    {
        return Err(NafkaError::Config(format!(
            "producer lane `{lane}` 启用重试但未启用幂等时 max_in_flight_requests_per_connection 必须为 1，以保持同分区顺序"
        )));
    }
    Ok(effective)
}

/// 业务作用：构建并原生校验指定 producer lane 配置。
///
/// # 参数
///
/// - `config`: 冻结后的顶层配置。
/// - `lane`: 物理 lane 名称。
///
/// # 错误
///
/// 配置合并失败或底层拒绝任一键值时返回错误。
pub(crate) fn producer_config(config: &KafkaConfig, lane: &str) -> Result<ClientConfig> {
    let effective = effective_producer_lane(config, lane)?;
    let role = format!("producer-{lane}");
    let mut native = common_config(config, &role);
    apply_properties(&mut native, &config.producer.properties);
    apply_properties(&mut native, &effective.properties);
    apply_identity_and_security(&mut native, config, &role);
    native
        .set("acks", &effective.acks)
        .set("retries", effective.retries.to_string())
        .set("retry.backoff.ms", effective.retry_backoff_ms.to_string())
        .set("linger.ms", effective.linger_ms.to_string())
        .set("batch.size", effective.batch_size.to_string())
        .set(
            "request.timeout.ms",
            effective.request_timeout_ms.to_string(),
        )
        .set(
            "delivery.timeout.ms",
            effective.delivery_timeout_ms.to_string(),
        )
        .set("partitioner", &effective.partitioner)
        .set("compression.type", &effective.compression)
        .set(
            "enable.idempotence",
            effective.enable_idempotence.to_string(),
        )
        .set(
            "max.in.flight.requests.per.connection",
            effective.max_in_flight_requests_per_connection.to_string(),
        )
        .set(
            "queue.buffering.max.messages",
            effective.queue_buffering_max_messages.to_string(),
        )
        .set(
            "queue.buffering.max.kbytes",
            effective.queue_buffering_max_kbytes.to_string(),
        );
    validate_native(&native, &format!("producer lane `{lane}`"))?;
    Ok(native)
}

/// 业务作用：构建订阅消费者配置，并在原生客户端创建前复验实际组身份。
/// 参数说明：config 为冻结源配置；group 为最终原文 group.id。
/// 返回：满足组身份边界并经原生配置解析的配置；非法组或原生拒绝键值时为 Config，不建立消费会话。
pub(crate) fn consumer_config(config: &KafkaConfig, group: &str) -> Result<ClientConfig> {
    // subscribe、assign 和临时位点查询使用同一 native 配置入口，不能绕过组身份边界。
    crate::wire::validate_group(group)?;
    let consumer = &config.consumer;
    let role = consumer_role(group);
    let mut native = common_config(config, &role);
    apply_properties(&mut native, &consumer.properties);
    apply_identity_and_security(&mut native, config, &role);
    native
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set(
            "session.timeout.ms",
            consumer.session_timeout_ms.to_string(),
        )
        .set(
            "heartbeat.interval.ms",
            consumer.heartbeat_interval_ms.to_string(),
        )
        .set(
            "max.poll.interval.ms",
            consumer.max_poll_interval_ms.to_string(),
        )
        .set("auto.offset.reset", &consumer.auto_offset_reset)
        .set("fetch.min.bytes", consumer.fetch_min_bytes.to_string())
        .set("fetch.wait.max.ms", consumer.fetch_wait_max_ms.to_string())
        .set(
            "max.partition.fetch.bytes",
            consumer.max_partition_fetch_bytes.to_string(),
        );
    validate_native(&native, &format!("consumer group `{group}`"))?;
    Ok(native)
}

/// 业务作用：构建并原生校验管理端配置。
///
/// # 参数
///
/// - `config`: 冻结后的顶层配置。
///
/// # 错误
///
/// 底层拒绝任一键值时返回错误。
pub(crate) fn admin_config(config: &KafkaConfig) -> Result<ClientConfig> {
    let mut native = common_config(config, "admin");
    apply_properties(&mut native, &config.admin.properties);
    apply_identity_and_security(&mut native, config, "admin");
    if let Some(timeout) = config.admin.request_timeout_ms {
        native.set("request.timeout.ms", timeout.to_string());
    }
    validate_native(&native, "admin")?;
    Ok(native)
}

/// 业务作用：由 resolved group 派生 consumer 的 client.id 角色后缀。
///
/// 为什么要带上 group：同一进程可以同时跑多个 group（socket-center 的 control/data 就是如此），
/// 若所有 consumer 共用一个 client.id，broker 指标与 `kafka-consumer-groups --members`
/// 都无法区分成员来源。producer 侧已经用 lane 名做了同样的事。
///
/// # 参数
///
/// - `group`: 已解析的最终 group.id。
///
/// # 返回
///
/// 清洗并截断到固定上限的角色后缀，保证 client.id 长度有界且字符集稳定。
fn consumer_role(group: &str) -> String {
    /// 诊断用 client.id 后缀最多保留64个组名字符；实际 group.id 原文不受此显示长度影响。
    const MAX_GROUP_CHARS: usize = 64;
    let sanitized: String = group
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '-'
            }
        })
        .take(MAX_GROUP_CHARS)
        .collect();
    format!("consumer-{sanitized}")
}

/// 业务作用：构建共同连接、安全和 client.id 配置。
///
/// # 参数
///
/// - `config`: 冻结后的顶层配置。
/// - `role`: client.id 的稳定角色后缀。
fn common_config(config: &KafkaConfig, _role: &str) -> ClientConfig {
    let mut native = ClientConfig::new();
    apply_properties(&mut native, &config.properties);
    native
}

/// 业务作用：写入身份与安全字段；必须在**全部** raw 透传层之后调用。
///
/// 配置优先级是 `common raw → scoped raw → 强类型 → 框架不变量`。
/// 若身份/安全在 scoped raw 之前写，`producer.properties` 里的
/// `security.protocol=PLAINTEXT` 就能把强类型声明的 `SASL_SSL` 静默降级（凭据以明文协议发出），
/// 而启动校验只看强类型值，完全发现不了；`bootstrap.servers` 与派生 `client.id` 同理。
///
/// # 参数
///
/// - `native`: 已应用完全部 raw 层的原生配置。
/// - `config`: 冻结后的顶层配置。
/// - `role`: client.id 的稳定角色后缀。
fn apply_identity_and_security(native: &mut ClientConfig, config: &KafkaConfig, role: &str) {
    native.set("bootstrap.servers", &config.bootstrap_servers);
    let prefix = config.client_id_prefix.as_deref().unwrap_or("nafka");
    native.set(
        "client.id",
        format!("{prefix}-{}-{role}", config.client_name),
    );
    let security = &config.security;
    set_optional(native, "security.protocol", &security.protocol);
    set_optional(native, "sasl.mechanism", &security.sasl_mechanism);
    set_optional(native, "sasl.username", &security.sasl_username);
    set_optional(native, "sasl.password", &security.sasl_password);
    set_optional(native, "ssl.ca.location", &security.ssl_ca_location);
    set_optional(
        native,
        "ssl.certificate.location",
        &security.ssl_certificate_location,
    );
    set_optional(native, "ssl.key.location", &security.ssl_key_location);
    set_optional(native, "ssl.key.password", &security.ssl_key_password);
}

/// 业务作用：按稳定顺序应用一组原生透传属性。
///
/// # 参数
///
/// - `native`: 待写入的原生配置。
/// - `properties`: 已经启动校验的键值表。
fn apply_properties(native: &mut ClientConfig, properties: &BTreeMap<String, String>) {
    for (key, value) in properties {
        native.set(key, value);
    }
}

/// 业务作用：可选字符串存在时写入原生配置。
///
/// # 参数
///
/// - `native`: 待写入的原生配置。
/// - `key`: 原生键。
/// - `value`: 可选配置值。
fn set_optional(native: &mut ClientConfig, key: &str, value: &Option<String>) {
    if let Some(value) = value {
        native.set(key, value);
    }
}

/// 业务作用：让底层配置解析器在启动期检查键名和值。
///
/// # 参数
///
/// - `native`: 已合并的原生配置。
/// - `scope`: 脱敏后的配置归属。
///
/// # 错误
///
/// 任一未知键或非法值返回配置错误。
fn validate_native(native: &ClientConfig, scope: &str) -> Result<()> {
    native
        .create_native_config()
        .map(|_| ())
        .map_err(|error| NafkaError::Config(format!("{scope} 原生配置非法: {}", redact(&error))))
}

/// 业务作用：把底层配置错误压成"只含错误类别与属性名"的稳定文本。
///
/// 必须按 [`KafkaError`] 的结构化 variant 判定，**不能对错误文本做字符串截断**：
/// librdkafka 的 desc 形如 `Invalid value "<VALUE>" for configuration property "<KEY>"`，
/// value 内嵌在 desc 开头，任何"砍尾巴"式脱敏都会把凭据明文原样留下。
///
/// 参数说明：error 为底层返回的错误；其 Display 可能同时在 desc 与尾部回显被拒的值。
/// 返回：配置错误仅含属性名及固定类别，构造错误不保留原文，避免公开错误链输出认证材料。
pub(crate) fn redact(error: &KafkaError) -> String {
    match error {
        // ClientConfig(res, desc, key, value)：只保留错误码与属性名，desc/value 一律丢弃。
        KafkaError::ClientConfig(res, _, key, _) => {
            format!("属性 `{key}` 被底层拒绝({res:?})")
        }
        KafkaError::ClientCreation(_) => "底层客户端构造失败(细节已脱敏)".into(),
        // 其余 variant 不携带配置值，可保留分类文本。
        other => other.to_string(),
    }
}
