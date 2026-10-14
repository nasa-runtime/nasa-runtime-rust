//! 构建支持、冻结启用项与实际组就绪分别呈现；算法可用不证明凭据和资源权限有效。

use crate::{KafkaConfig, KafkaProxy, NafkaError, ReadyRequirement, ReadyStatus, Result};
use std::collections::{BTreeMap, BTreeSet};

/// 不包含凭据的源能力快照；没有普通消费组时，readiness为空，不能解释为Ready。
#[derive(Clone, Debug)]
pub struct KafkaCapabilities {
    /// 实际源的稳定逻辑身份。
    pub source_id: String,
    /// 实际链接的底层客户端版本。
    pub client_version: String,
    /// 当前组件feature和原生实现共同支持的规范能力名。
    pub supported: BTreeSet<String>,
    /// 源配置启用的能力；不表示远端握手或资源权限已验证。
    pub configured: BTreeSet<String>,
    /// default 与命名 lane 的有效压缩算法，仅描述冻结配置，不证明交付可用。
    pub lane_compression: BTreeMap<String, String>,
    /// 当次普通组对Joined要求的观测；事务状态仍由各自句柄报告。
    pub readiness: BTreeMap<String, ReadyStatus>,
}

impl KafkaCapabilities {
    /// 业务作用：独立查询本地可用实现，不创建Kafka连接。
    /// 参数说明：无。返回：配置启用之前的构建证据；没有证据时明确报UnsupportedCapability。
    pub fn supported_capabilities() -> Result<BTreeSet<String>> {
        crate::rd::capabilities::supported()
    }

    /// 业务作用：汇总所属源的配置与真实健康，不把构建能力误报为Ready。
    /// 参数说明：source为真实所属源。返回：不建连、不含认证值或路径的独立快照。
    pub(crate) fn snapshot(source: &KafkaProxy) -> Result<Self> {
        let groups = source
            .inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let readiness = groups
            .iter()
            .map(|(name, group)| {
                (
                    name.clone(),
                    ReadyStatus::evaluate(
                        &source.inner.config.client_name,
                        ReadyRequirement::Joined,
                        group.health(),
                    ),
                )
            })
            .collect();
        Ok(Self {
            source_id: source.inner.config.client_name.clone(),
            client_version: crate::rd::capabilities::version(),
            supported: Self::supported_capabilities()?,
            configured: configured(&source.inner.config)?,
            lane_compression: compressions(&source.inner.config),
            readiness,
        })
    }
}

/// 业务作用：在原生资源创建前拒绝未实现能力和角色间认证漂移。
/// 参数说明：config为完整源配置。返回：全部启用项有实际支持才通过；不输出未知配置值或凭据。
pub(crate) fn validate(config: &KafkaConfig) -> Result<()> {
    for properties in [
        &config.producer.properties,
        &config.consumer.properties,
        &config.admin.properties,
    ]
    .into_iter()
    .chain(config.producer.lanes.values().map(|lane| &lane.properties))
    {
        if properties.keys().any(|key| {
            key.starts_with("sasl.")
                || key.starts_with("ssl.")
                || key == "security.protocol"
                || key == "enable.ssl.certificate.verification"
        }) {
            return Err(NafkaError::Config("客户端专属配置不能改写源认证".into()));
        }
    }
    let supported = KafkaCapabilities::supported_capabilities()?;
    for capability in configured(config)? {
        if !supported.contains(&capability) {
            let capability = match capability.as_str() {
                "tls" => "tls",
                "mtls" => "mtls",
                "sasl-plain" => "sasl-plain",
                "sasl-scram-sha-256" => "sasl-scram-sha-256",
                "sasl-scram-sha-512" => "sasl-scram-sha-512",
                "sasl-gssapi" => "sasl-gssapi",
                "compression-gzip" => "compression-gzip",
                "compression-snappy" => "compression-snappy",
                "compression-lz4" => "compression-lz4",
                "compression-zstd" => "compression-zstd",
                _ => "native-capabilities",
            };
            return Err(NafkaError::UnsupportedCapability(capability));
        }
    }
    Ok(())
}

/// 业务作用：按源级raw和强类型优先级识别认证及通道算法，避免raw绕过构建能力门禁。
/// 参数说明：config为合并源配置。返回：规范能力名，未知声明拒绝而不猜测其它机制。
fn configured(config: &KafkaConfig) -> Result<BTreeSet<String>> {
    let security = &config.security;
    let effective = |typed: &Option<String>, key: &str| {
        typed
            .clone()
            .or_else(|| config.properties.get(key).cloned())
    };
    let protocol =
        effective(&security.protocol, "security.protocol").unwrap_or_else(|| "PLAINTEXT".into());
    if !matches!(
        protocol.as_str(),
        "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
    ) {
        return Err(NafkaError::Config("security.protocol 非法".into()));
    }
    let ssl_material = security.ssl_ca_location.is_some()
        || security.ssl_certificate_location.is_some()
        || security.ssl_key_location.is_some()
        || security.ssl_key_password.is_some()
        || config
            .properties
            .keys()
            .any(|key| key.starts_with("ssl.") || key == "enable.ssl.certificate.verification");
    if ssl_material && !protocol.ends_with("SSL") {
        return Err(NafkaError::Config("SSL 字段要求源协议启用 SSL".into()));
    }
    let sasl_material = security.sasl_mechanism.is_some()
        || security.sasl_username.is_some()
        || security.sasl_password.is_some()
        || config.properties.keys().any(|key| key.starts_with("sasl."));
    if sasl_material && !protocol.starts_with("SASL_") {
        return Err(NafkaError::Config("SASL 字段要求源协议启用 SASL".into()));
    }
    if security.sasl_mechanism.is_none()
        && config.properties.contains_key("sasl.mechanism")
        && config.properties.contains_key("sasl.mechanisms")
        && config.properties["sasl.mechanism"] != config.properties["sasl.mechanisms"]
    {
        return Err(NafkaError::Config("SASL 机制别名不能冲突".into()));
    }
    let mut values = BTreeSet::new();
    if protocol.ends_with("SSL") {
        values.insert("tls".into());
        if effective(
            &security.ssl_certificate_location,
            "ssl.certificate.location",
        )
        .is_some()
            || config.properties.contains_key("ssl.keystore.location")
        {
            values.insert("mtls".into());
        }
    }
    if protocol.starts_with("SASL_") {
        let mechanism = effective(&security.sasl_mechanism, "sasl.mechanism")
            .or_else(|| config.properties.get("sasl.mechanisms").cloned())
            .unwrap_or_else(|| "GSSAPI".into());
        values.insert(
            match mechanism.as_str() {
                "PLAIN" => "sasl-plain",
                "SCRAM-SHA-256" => "sasl-scram-sha-256",
                "SCRAM-SHA-512" => "sasl-scram-sha-512",
                "GSSAPI" => "sasl-gssapi",
                _ => return Err(NafkaError::UnsupportedCapability("sasl-mechanism")),
            }
            .into(),
        );
    }
    for compression in compressions(config).values() {
        if !matches!(
            compression.as_str(),
            "none" | "gzip" | "snappy" | "lz4" | "zstd"
        ) {
            return Err(NafkaError::UnsupportedCapability("compression"));
        }
        values.insert(format!("compression-{compression}"));
    }
    Ok(values)
}

/// 业务作用：在源校验阶段覆盖全部 lane，防止缺失的压缩实现延迟到原生构造或发送时才暴露。
/// 参数说明：config为冻结源配置。返回：default及命名lane的规范压缩算法。
fn compressions(config: &KafkaConfig) -> BTreeMap<String, String> {
    let base = config.producer.compression.clone();
    let mut values = BTreeMap::from([("default".into(), base.clone())]);
    values.extend(config.producer.lanes.iter().map(|(name, lane)| {
        (
            name.clone(),
            lane.compression
                .as_ref()
                .cloned()
                .unwrap_or_else(|| base.clone()),
        )
    }));
    values
}
