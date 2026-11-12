use std::collections::HashMap;

use nacos_macro::request;
use serde::{Deserialize, Serialize};

/// 双向连接建立时发送的客户端身份、作用域及能力声明。
#[request(identity = "ConnectionSetupRequest", module = "internal")]
pub(crate) struct ConnectionSetupRequest {
    /// SDK 实现版本标识，供协议兼容处理使用。
    pub(crate) client_version: String,

    /// 当前客户端声明可处理的协议能力集合。
    pub(crate) abilities: NacosClientAbilities,

    /// 连接所属的命名空间，不替代逐请求认证。
    pub(crate) tenant: String,

    /// 连接来源、模块与应用名称等标签。
    pub(crate) labels: HashMap<String, String>,
}

/// 分模块组织的握手能力声明，字段值不构成实现可用性的证明。
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct NacosClientAbilities {
    /// 通用远端连接协议能力。
    #[serde(rename = "remoteAbility")]
    remote_ability: NacosClientRemoteAbility,

    /// 配置模块的远端指标协议能力。
    #[serde(rename = "configAbility")]
    config_ability: NacosClientConfigAbility,

    /// 服务发现的增量推送及指标能力。
    #[serde(rename = "namingAbility")]
    naming_ability: NacosClientNamingAbility,
}

impl NacosClientAbilities {
    /// 业务作用：建立尚未声明可选协议能力的握手能力集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：远端连接、配置指标、发现增量推送及发现指标均关闭的声明对象，不探测客户端实现。
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        NacosClientAbilities {
            remote_ability: NacosClientRemoteAbility::new(),
            config_ability: NacosClientConfigAbility::new(),
            naming_ability: NacosClientNamingAbility::new(),
        }
    }

    /// 业务作用：设置握手中向服务端声明的远端连接支持能力。
    ///
    /// 参数说明：
    /// - `enable`：是否声明支持对应连接协议；调用方应与实际实现一致。
    ///
    /// 返回：更新能力字段，不建立或关闭现有网络连接。
    pub(crate) fn support_remote_connection(&mut self, enable: bool) {
        self.remote_ability.support_remote_connection(enable);
    }

    /// 业务作用：设置配置模块在握手中的远端指标支持声明。
    ///
    /// 参数说明：
    /// - `enable`：向服务端报告的指标支持开关。
    ///
    /// 返回：更新配置能力记录，不创建指标端点或采集任务。
    pub(crate) fn support_config_remote_metrics(&mut self, enable: bool) {
        self.config_ability.support_remote_metrics(enable);
    }

    /// 业务作用：声明客户端是否能处理服务发现的增量推送。
    ///
    /// 参数说明：
    /// - `enable`：是否向服务端声明增量协议支持，须与实际消费能力一致。
    ///
    /// 返回：更新发现能力字段，不自动安装增量合并逻辑或更改现有缓存。
    pub(crate) fn support_naming_delta_push(&mut self, enable: bool) {
        self.naming_ability.support_delta_push(enable);
    }

    /// 业务作用：设置服务发现模块在握手中的远端指标支持声明。
    ///
    /// 参数说明：
    /// - `enable`：是否声明发现指标能力，不在此处验证实现是否存在。
    ///
    /// 返回：更新声明对象，不注册采集源或发起远端调用。
    pub(crate) fn support_naming_remote_metric(&mut self, enable: bool) {
        self.naming_ability.support_remote_metric(enable);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct NacosClientRemoteAbility {
    /// 是否声明支持远端连接协议。
    #[serde(rename = "supportRemoteConnection")]
    support_remote_connection: bool,
}

impl NacosClientRemoteAbility {
    /// 业务作用：建立默认不声明远端连接支持的能力记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：连接支持字段为假的本地记录，不分配连接资源。
    fn new() -> Self {
        NacosClientRemoteAbility {
            support_remote_connection: false,
        }
    }

    /// 业务作用：更新将被序列化到握手的连接协议能力标记。
    ///
    /// 参数说明：
    /// - `enable`：调用方确定的连接支持状态。
    ///
    /// 返回：原地替换标记，不执行连接健康检查。
    fn support_remote_connection(&mut self, enable: bool) {
        self.support_remote_connection = enable;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct NacosClientConfigAbility {
    /// 是否声明支持配置远端指标。
    #[serde(rename = "supportRemoteMetrics")]
    support_remote_metrics: bool,
}

impl NacosClientConfigAbility {
    /// 业务作用：建立默认关闭远端指标声明的配置模块能力记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置指标支持字段为假的记录，不改变配置订阅行为。
    fn new() -> Self {
        NacosClientConfigAbility {
            support_remote_metrics: false,
        }
    }

    /// 业务作用：修改配置模块向服务端声明的指标协议支持标记。
    ///
    /// 参数说明：
    /// - `enable`：此次握手要报告的支持状态。
    ///
    /// 返回：保存声明值，不控制本地日志或其它监控系统。
    fn support_remote_metrics(&mut self, enable: bool) {
        self.support_remote_metrics = enable;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct NacosClientNamingAbility {
    /// 是否声明支持发现增量推送。
    #[serde(rename = "supportDeltaPush")]
    support_delta_push: bool,

    /// 是否声明支持发现远端指标。
    #[serde(rename = "supportRemoteMetric")]
    support_remote_metric: bool,
}

impl NacosClientNamingAbility {
    /// 业务作用：建立默认关闭增量推送及远端指标声明的发现能力记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两个可选发现能力均为假的对象，不读取当前服务实例状态。
    fn new() -> Self {
        NacosClientNamingAbility {
            support_delta_push: false,
            support_remote_metric: false,
        }
    }

    /// 业务作用：更新服务发现握手中的增量推送支持标记。
    ///
    /// 参数说明：
    /// - `enable`：调用方基于客户端实现选择的增量支持状态。
    ///
    /// 返回：原地保存状态，不改写已订阅实例集合。
    fn support_delta_push(&mut self, enable: bool) {
        self.support_delta_push = enable;
    }

    /// 业务作用：更新服务发现握手中的远端指标支持标记。
    ///
    /// 参数说明：
    /// - `enable`：需要向服务端报告的指标能力状态。
    ///
    /// 返回：保存字段供握手序列化使用，不执行指标采集或发送。
    fn support_remote_metric(&mut self, enable: bool) {
        self.support_remote_metric = enable;
    }
}
