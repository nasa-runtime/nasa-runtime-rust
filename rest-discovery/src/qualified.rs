//! 以 qualifier 选择具名 REST 客户端；集合只共享调用能力，不创建连接或接管关闭流程。

use crate::{RestDiscoveryClient, RestDiscoveryError, Result};
use std::{collections::BTreeMap, sync::Arc};

/// 以 qualifier 索引的共享调用能力；默认引用只服务于未显式选择连接的请求。
#[derive(Clone)]
pub struct RestClientSet {
    clients: BTreeMap<String, Arc<RestDiscoveryClient>>,
    default: Option<Arc<RestDiscoveryClient>>,
}

impl RestClientSet {
    /// 业务作用：冻结具名客户端，以 primary 作为未声明 qualifier 的调用目标。
    /// 参数说明：`clients` 由宿主按连接名装配并共享生命周期；`legacy_default` 仅供兼容既有宿主的显式默认引用。
    /// 返回：有效且有界的集合；名称非法、数量超过 256 或默认引用不存在时失败，不发请求。
    pub fn new(
        clients: BTreeMap<String, Arc<RestDiscoveryClient>>,
        legacy_default: Option<&str>,
    ) -> Result<Self> {
        if clients.len() > 256
            || clients.keys().any(|name| {
                let bytes = name.as_bytes();
                bytes.is_empty()
                    || bytes.len() > 64
                    || !bytes[0].is_ascii_alphanumeric()
                    || !bytes
                        .iter()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            })
        {
            return Err(RestDiscoveryError::InvalidOptions {
                reason: "invalid REST qualifier set".into(),
            });
        }
        // 只认 primary 或兼容宿主明确给出的引用；单个具名环境也不能自动成为默认环境。
        let default = legacy_default
            .map(|qualifier| {
                clients.get(qualifier).cloned().ok_or_else(|| {
                    RestDiscoveryError::UnknownQualifier {
                        qualifier: qualifier.into(),
                    }
                })
            })
            .transpose()?
            .or_else(|| clients.get("primary").cloned());
        Ok(Self { clients, default })
    }

    /// 业务作用：保留单客户端注入的默认调用能力，不为其推断任何具名注册中心身份。
    /// 参数说明：`client` 是已有的共享 REST 客户端。
    /// 返回：只允许未声明 qualifier 的调用使用该客户端的集合，不创建新资源。
    pub fn from_client(client: Arc<RestDiscoveryClient>) -> Self {
        Self {
            clients: BTreeMap::new(),
            default: Some(client),
        }
    }

    /// 业务作用：精确选择调用所绑定的注册中心，避免配置错误或故障跨环境路由。
    /// 参数说明：`qualifier` 指定连接名称；None 选择 primary 或宿主明确提供的兼容默认项。
    /// 返回：共享客户端；未知 qualifier 或缺默认项明确失败，不访问全局门面或其他连接。
    pub fn client(&self, qualifier: Option<&str>) -> Result<Arc<RestDiscoveryClient>> {
        // 显式名称失败不能使用默认项，否则同名服务会被发送到另一个业务环境。
        match qualifier {
            Some(qualifier) => self.clients.get(qualifier).cloned().ok_or_else(|| {
                RestDiscoveryError::UnknownQualifier {
                    qualifier: qualifier.into(),
                }
            }),
            None => self
                .default
                .clone()
                .ok_or(RestDiscoveryError::MissingDefaultQualifier),
        }
    }
}
