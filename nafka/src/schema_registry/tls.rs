//! Registry 独立的信任锚与客户端身份，不继承 Kafka 的认证域。

use super::SchemaRegistryError;

const MAX_MATERIAL_BYTES: usize = 1024 * 1024;

/// 有界 PEM 材料；未声明信任锚时沿用 HTTP 客户端默认根，显式材料替代默认根。
pub struct SchemaRegistryTls {
    /// 可选 PEM 信任锚集合，最多一 MiB、十六个证书。
    pub ca_pem: Option<Vec<u8>>,
    /// 可选 PEM 客户端证书链与一个未加密 PKCS#8 私钥，最多一 MiB。
    pub identity_pem: Option<nasecret::SecretBytes>,
}

impl std::fmt::Debug for SchemaRegistryTls {
    /// 业务作用：阻止 TLS 身份进入配置或错误诊断。
    /// 参数说明：formatter 为诊断输出。
    /// 返回：固定脱敏字段，不输出证书或私钥内容。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchemaRegistryTls")
            .field("ca_pem", &"<redacted>")
            .field("identity_pem", &"<redacted>")
            .finish()
    }
}

impl SchemaRegistryTls {
    /// 业务作用：在建立连接前绑定独立信任域及 mTLS 身份。
    /// 参数说明：builder 为已设置请求预算且禁用重定向的客户端构造器。
    /// 返回：保持证书及主机名验证的构造器；格式、数量或材料大小非法时返回固定配置错误。
    pub(super) fn apply(
        self,
        mut builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, SchemaRegistryError> {
        if let Some(pem) = self.ca_pem {
            let certificates = certificates(&pem)?;
            // 私有信任锚替代默认根，避免其它公开 CA 意外进入此 Registry 的认证范围。
            builder = builder.tls_built_in_root_certs(false);
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(pem) = self.identity_pem {
            let bytes = pem.expose();
            certificates(bytes)?;
            let begin = b"-----BEGIN PRIVATE KEY-----";
            let end = b"-----END PRIVATE KEY-----";
            if occurrences(bytes, begin) != 1
                || occurrences(bytes, end) != 1
                || occurrences(bytes, b"-----BEGIN RSA PRIVATE KEY-----") != 0
                || occurrences(bytes, b"-----BEGIN EC PRIVATE KEY-----") != 0
                || occurrences(bytes, b"-----BEGIN ENCRYPTED PRIVATE KEY-----") != 0
            {
                return Err(SchemaRegistryError::InvalidConfiguration);
            }
            let identity = reqwest::Identity::from_pem(bytes)
                .map_err(|_| SchemaRegistryError::InvalidConfiguration)?;
            builder = builder.identity(identity);
        }
        Ok(builder)
    }
}

/// 业务作用：约束证书解析工作量，并拒绝没有信任或身份意义的空材料。
/// 参数说明：pem 为用户明确提供的有界 PEM 集合。
/// 返回：一至十六个证书；材料过大、缺失或解析失败返回固定配置分类。
fn certificates(pem: &[u8]) -> Result<Vec<reqwest::Certificate>, SchemaRegistryError> {
    if pem.is_empty() || pem.len() > MAX_MATERIAL_BYTES {
        return Err(SchemaRegistryError::InvalidConfiguration);
    }
    let certificates = reqwest::Certificate::from_pem_bundle(pem)
        .map_err(|_| SchemaRegistryError::InvalidConfiguration)?;
    if certificates.is_empty()
        || certificates.len() > 16
        || occurrences(pem, b"-----BEGIN CERTIFICATE-----") != certificates.len()
        || occurrences(pem, b"-----END CERTIFICATE-----") != certificates.len()
    {
        return Err(SchemaRegistryError::InvalidConfiguration);
    }
    Ok(certificates)
}

/// 业务作用：校验身份材料只声明一个受支持的私钥，避免解析器选取歧义身份。
/// 参数说明：value 为有界材料，marker 为固定 PEM 边界。
/// 返回：精确边界出现次数，不构造私钥文本副本。
fn occurrences(value: &[u8], marker: &[u8]) -> usize {
    value
        .windows(marker.len())
        .filter(|part| *part == marker)
        .count()
}
