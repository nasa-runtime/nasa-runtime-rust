//! 显式认证来源。配置中的 exec 和 auth-provider 不会被隐式执行。

use base64::{engine::general_purpose::STANDARD, Engine};
use nadisc::{
    http::{base_url, error, Url},
    managed::{ErrorCode, Result},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Authentication {
    InCluster,
    Kubeconfig { path: PathBuf, context: String },
}

pub(crate) enum Credential {
    Token(String),
    TokenFile(PathBuf),
    Certificate,
}
pub(crate) struct Material {
    pub server: Url,
    pub ca: Vec<u8>,
    pub identity: Option<Vec<u8>>,
    pub credential: Credential,
}

/// 业务作用：读取有限普通文件，拒绝设备、管道及超过上限的认证材料。
/// 参数说明：`path` 是明确的认证来源，内容只用于连接，不能写入诊断。
/// 返回：不超过一 MiB 的完整字节或脱敏错误。
pub(crate) async fn read_file(path: &Path) -> Result<Vec<u8>> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    // 文件轮换可能在路径检查后换成管道；非阻塞打开并检查实际句柄，避免留下无法取消的等待。
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK);
    let file = options.open(path).await.map_err(|_| {
        error(
            ErrorCode::Unauthenticated,
            "Kubernetes credential file cannot be opened",
        )
    })?;
    let metadata = file.metadata().await.map_err(|_| {
        error(
            ErrorCode::Unauthenticated,
            "Kubernetes credential metadata is unavailable",
        )
    })?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(error(
            ErrorCode::InvalidConfig,
            "Kubernetes credential source is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| {
            error(
                ErrorCode::Unauthenticated,
                "Kubernetes credential file cannot be read",
            )
        })?;
    if bytes.len() > 1024 * 1024 {
        return Err(error(
            ErrorCode::CapacityExceeded,
            "Kubernetes credential exceeds byte limit",
        ));
    }
    Ok(bytes)
}

/// 业务作用：只从用户指定的认证模式取得集群地址及证书，不搜索其他 kubeconfig。
/// 参数说明：`auth` 是冻结的来源，集群内模式使用 Kubernetes 标准挂载。
/// 返回：尚未创建网络连接的认证材料。
pub(crate) async fn load(auth: &Authentication) -> Result<Material> {
    match auth {
        Authentication::InCluster => {
            let host = std::env::var("KUBERNETES_SERVICE_HOST")
                .map_err(|_| error(ErrorCode::InvalidConfig, "in-cluster API host is absent"))?;
            let port =
                std::env::var("KUBERNETES_SERVICE_PORT_HTTPS").unwrap_or_else(|_| "443".into());
            let host = if host.contains(':') {
                format!("[{host}]")
            } else {
                host
            };
            let server = base_url(&format!("https://{host}:{port}"))?;
            let root = Path::new("/var/run/secrets/kubernetes.io/serviceaccount");
            Ok(Material {
                server,
                ca: read_file(&root.join("ca.crt")).await?,
                identity: None,
                credential: Credential::TokenFile(root.join("token")),
            })
        }
        Authentication::Kubeconfig { path, context } => {
            let bytes = read_file(path).await?;
            let value: Value = serde_yaml::from_slice(&bytes)
                .map_err(|_| error(ErrorCode::InvalidConfig, "invalid kubeconfig"))?;
            let context = named(&value, "contexts", context)?;
            let context = &context["context"];
            let cluster = named(&value, "clusters", field(context, "cluster")?)?;
            let user = named(&value, "users", field(context, "user")?)?;
            let cluster = &cluster["cluster"];
            let user = &user["user"];
            if cluster["insecure-skip-tls-verify"].as_bool() == Some(true)
                || cluster.get("proxy-url").is_some()
                || cluster.get("tls-server-name").is_some()
                || user.get("exec").is_some()
                || user.get("auth-provider").is_some()
                || user.get("username").is_some()
                || user.get("password").is_some()
                || user.get("as").is_some()
                || user.get("as-uid").is_some()
                || user.get("as-groups").is_some()
                || user.get("as-user-extra").is_some()
            {
                return Err(error(
                    ErrorCode::Unsupported,
                    "kubeconfig authentication or TLS override is unsupported",
                ));
            }
            let server = base_url(field(cluster, "server")?)?;
            if server.scheme() != "https" {
                return Err(error(
                    ErrorCode::InvalidConfig,
                    "Kubernetes API requires HTTPS",
                ));
            }
            let root = path.parent().unwrap_or_else(|| Path::new("."));
            let ca = material(cluster, "certificate-authority", root).await?;
            let token = user.get("token").and_then(Value::as_str);
            let token_file = user.get("tokenFile").and_then(Value::as_str);
            let certificate = user.get("client-certificate").is_some()
                || user.get("client-certificate-data").is_some();
            if usize::from(token.is_some())
                + usize::from(token_file.is_some())
                + usize::from(certificate)
                != 1
            {
                return Err(error(
                    ErrorCode::InvalidConfig,
                    "kubeconfig requires one explicit authentication source",
                ));
            }
            let (credential, identity) = if let Some(token) = token {
                (Credential::Token(token.into()), None)
            } else if let Some(file) = token_file {
                (Credential::TokenFile(root.join(file)), None)
            } else {
                let mut pem = material(user, "client-certificate", root).await?;
                pem.push(b'\n');
                pem.extend(material(user, "client-key", root).await?);
                (Credential::Certificate, Some(pem))
            };
            Ok(Material {
                server,
                ca,
                identity,
                credential,
            })
        }
    }
}

/// 业务作用：拒绝 kubeconfig 同名条目，避免选择错误的集群或身份。
/// 参数说明：`value` 是完整配置，`list` 是条目集合，`name` 为显式引用。
/// 返回：唯一匹配条目；缺失或重复拒绝。
fn named<'a>(value: &'a Value, list: &str, name: &str) -> Result<&'a Value> {
    let items = value[list]
        .as_array()
        .ok_or_else(|| error(ErrorCode::InvalidConfig, "kubeconfig named list is absent"))?;
    let mut matches = items
        .iter()
        .filter(|item| item["name"].as_str() == Some(name));
    let item = matches.next().ok_or_else(|| {
        error(
            ErrorCode::InvalidConfig,
            "kubeconfig named reference is absent",
        )
    })?;
    if matches.next().is_some() {
        return Err(error(
            ErrorCode::Conflict,
            "duplicate kubeconfig named reference",
        ));
    }
    Ok(item)
}

/// 业务作用：读取必须明确配置的认证字段，错误不回显内容。
/// 参数说明：`value` 是选中条目，`name` 为公开配置键。
/// 返回：非空字符串或配置错误。
fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            error(
                ErrorCode::InvalidConfig,
                "required kubeconfig field is absent",
            )
        })
}

/// 业务作用：按 kubeconfig 所在目录解析文件或内联证书，拒绝含混来源。
/// 参数说明：`value` 是集群/身份条目，`name` 是文件键，`root` 是配置父目录。
/// 返回：有界证书字节，不执行外部命令。
async fn material(value: &Value, name: &str, root: &Path) -> Result<Vec<u8>> {
    let data_key = format!("{name}-data");
    match (value[name].as_str(), value[&data_key].as_str()) {
        (Some(file), None) => read_file(&root.join(file)).await,
        (None, Some(data)) if data.len() <= 1024 * 1024 => STANDARD.decode(data).map_err(|_| {
            error(
                ErrorCode::InvalidConfig,
                "invalid kubeconfig certificate encoding",
            )
        }),
        _ => Err(error(
            ErrorCode::InvalidConfig,
            "kubeconfig certificate source is absent or ambiguous",
        )),
    }
}
