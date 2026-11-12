use nacos_macro::request;

use crate::{common::remote::generate_request_id, naming::ServiceInstance};

#[request(identity = "InstanceRequest", module = "naming")]
pub(crate) struct InstanceRequest {
    /// 协议 type 字段，区分实例注册与注销。
    #[serde(rename = "type")]
    pub(crate) r_type: String,
    /// 操作目标实例，原样随请求传递。
    pub(crate) instance: ServiceInstance,
}

impl InstanceRequest {
    /// 业务作用：构造实例注册请求，使实例信息与服务作用域一起提交。
    ///
    /// 参数说明：
    /// - `instance`：待注册实例，包含地址、权重和元数据等信息。
    /// - `service_name`：可选服务名称，原样写入协议字段。
    /// - `namespace`：可选命名空间。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：已分配请求 ID 的注册消息，不发送请求或登记重放任务。
    pub(crate) fn register(
        instance: ServiceInstance,
        service_name: Option<String>,
        namespace: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        InstanceRequest::new(
            RequestType::Register,
            instance,
            service_name,
            namespace,
            group_name,
        )
    }

    /// 业务作用：构造实例注销请求，明确需要撤销的实例及服务作用域。
    ///
    /// 参数说明：
    /// - `instance`：待注销的实例信息，交给服务端匹配。
    /// - `service_name`：可选服务名称。
    /// - `namespace`：可选命名空间。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：已分配请求 ID 的注销消息，不移除本地注册意图或等待远端完成。
    pub(crate) fn deregister(
        instance: ServiceInstance,
        service_name: Option<String>,
        namespace: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        InstanceRequest::new(
            RequestType::Deregister,
            instance,
            service_name,
            namespace,
            group_name,
        )
    }

    /// 业务作用：统一实例注册和注销的协议装配，保证操作码与请求标识同时存在。
    ///
    /// 参数说明：
    /// - `request_type`：注册或注销操作选择。
    /// - `instance`：本次操作的实例信息。
    /// - `service_name`：可选服务名称，不在此归一缺省值。
    /// - `namespace`：可选命名空间。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：带对应协议操作码的新请求，不验证实例是否临时、健康或已经注册。
    fn new(
        request_type: RequestType,
        instance: ServiceInstance,
        service_name: Option<String>,
        namespace: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        let request_id = Some(generate_request_id());
        Self {
            r_type: request_type.request_code(),
            instance,
            request_id,
            namespace,
            service_name,
            group_name,
            ..Default::default()
        }
    }
}

pub(crate) enum RequestType {
    /// 请求服务端接纳实例注册。
    Register,
    /// 请求服务端撤销实例注册。
    Deregister,
}

impl RequestType {
    /// 业务作用：把实例操作选择转换为 Nacos 协议接受的操作码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：注册或注销的固定协议文本，不执行实例状态变化。
    pub(crate) fn request_code(&self) -> String {
        match self {
            RequestType::Register => "registerInstance".to_string(),
            RequestType::Deregister => "deregisterInstance".to_string(),
        }
    }
}

impl From<&str> for RequestType {
    /// 业务作用：从操作码恢复实例操作选择，保留未知文本按注册处理的缺省规则。
    ///
    /// 参数说明：
    /// - `code`：待匹配的协议操作码，按原文精确比较。
    ///
    /// 返回：已知注销码对应注销，其它值对应注册；此转换不提供非法输入错误，不能当作严格操作校验。
    fn from(code: &str) -> Self {
        match code {
            "registerInstance" => RequestType::Register,
            "deregisterInstance" => RequestType::Deregister,
            _ => RequestType::Register,
        }
    }
}
