//! 原生构建能力查询；只创建配置对象，不创建客户端、连接或后台线程。

use crate::{NafkaError, Result};
use std::{collections::BTreeSet, sync::OnceLock};

/// 业务作用：读取实际链接的librdkafka能力，并限制为组件显式开放的feature。
/// 参数说明：无。返回：固定规范能力名；查询失败明确拒绝，不能只看配置文字宣称支持。
pub(crate) fn supported() -> Result<BTreeSet<String>> {
    static VALUE: OnceLock<Result<BTreeSet<String>>> = OnceLock::new();
    VALUE
        .get_or_init(|| {
            let native = rdkafka::config::ClientConfig::new()
                .create_native_config()
                .map_err(|_| NafkaError::UnsupportedCapability("native-capabilities"))?;
            let features = native
                .get("builtin.features")
                .map_err(|_| NafkaError::UnsupportedCapability("native-capabilities"))?;
            let features = features.split(',').map(str::trim).collect::<BTreeSet<_>>();
            let mut supported = BTreeSet::from(["compression-none".to_owned()]);
            for codec in ["gzip", "snappy", "lz4", "zstd"] {
                if features.contains(codec) && (codec != "zstd" || cfg!(feature = "zstd")) {
                    supported.insert(format!("compression-{codec}"));
                }
            }
            if features.contains("ssl") && cfg!(feature = "tls") {
                supported.extend(["tls".into(), "mtls".into()]);
            }
            if features.contains("sasl_plain") {
                supported.insert("sasl-plain".into());
            }
            if features.contains("sasl_scram") && cfg!(feature = "tls") {
                supported.extend(["sasl-scram-sha-256".into(), "sasl-scram-sha-512".into()]);
            }
            if features.contains("sasl_gssapi") && cfg!(feature = "gssapi") {
                supported.insert("sasl-gssapi".into());
            }
            Ok(supported)
        })
        .clone()
}

/// 业务作用：报告实际链接的底层客户端版本，供部署判断支持范围。
/// 参数说明：无。返回：librdkafka版本文本，不读取配置或连接信息。
pub(crate) fn version() -> String {
    rdkafka::util::get_rdkafka_version().1
}
