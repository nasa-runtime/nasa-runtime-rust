use nacos_macro::request;

use crate::{api::naming::ServiceInstance, common::remote::generate_request_id};

#[request(identity = "PersistentInstanceRequest", module = "naming")]
pub(crate) struct PersistentInstanceRequest {
    /// 持久实例请求中的注册或注销操作码。
    #[serde(rename = "type")]
    pub(crate) r_type: String,
    /// 操作目标实例，不在协议对象内修改临时性标记。
    pub(crate) instance: ServiceInstance,
}

impl PersistentInstanceRequest {
    /// 业务作用：为持久实例构造注册协议请求，供上层选择持久注册通道后发送。
    ///
    /// 参数说明：
    /// - `instance`：待注册实例，临时性标记由调用方确保与通道一致。
    /// - `service_name`：可选服务名称。
    /// - `namespace`：可选命名空间。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：带独立请求 ID 的持久注册消息，不校验或改写实例标记，也不访问服务端。
    pub(crate) fn register(
        instance: ServiceInstance,
        service_name: Option<String>,
        namespace: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        PersistentInstanceRequest::new(
            RequestType::Register,
            instance,
            service_name,
            namespace,
            group_name,
        )
    }

    /// 业务作用：构造持久实例的注销请求，供服务端按实例与作用域执行撤销。
    ///
    /// 参数说明：
    /// - `instance`：待注销实例信息。
    /// - `service_name`：可选服务名称。
    /// - `namespace`：可选命名空间。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：持久实例注销消息，不改变本地缓存或重放任务，也不代表远端已撤销。
    pub(crate) fn deregister(
        instance: ServiceInstance,
        service_name: Option<String>,
        namespace: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        PersistentInstanceRequest::new(
            RequestType::Deregister,
            instance,
            service_name,
            namespace,
            group_name,
        )
    }

    /// 业务作用：统一持久实例操作的协议封装，使请求身份与操作选择一致。
    ///
    /// 参数说明：
    /// - `request_type`：持久实例注册或注销操作。
    /// - `instance`：待提交的目标实例信息。
    /// - `service_name`：可选服务名称，原样保留。
    /// - `namespace`：可选命名空间，原样保留。
    /// - `group_name`：可选服务分组，原样保留。
    ///
    /// 返回：已分配请求 ID 的协议对象，不验证服务是否存在或实例是否持久化。
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
    /// 注册持久实例。
    Register,
    /// 注销持久实例。
    Deregister,
}

impl RequestType {
    /// 业务作用：为持久实例操作选择对应的 Nacos 协议操作码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定注册或注销文本，由外层消息身份区分持久实例通道。
    pub(crate) fn request_code(&self) -> String {
        match self {
            RequestType::Register => "registerInstance".to_string(),
            RequestType::Deregister => "deregisterInstance".to_string(),
        }
    }
}

impl From<&str> for RequestType {
    /// 业务作用：按协议文本恢复持久实例操作，保留未知值默认注册的行为。
    ///
    /// 参数说明：
    /// - `code`：待匹配的操作码文本，不执行大小写归一。
    ///
    /// 返回：已知注销码为注销，其它值为注册；不会拒绝未知输入，调用方不能把该转换当作安全校验。
    fn from(code: &str) -> Self {
        match code {
            "registerInstance" => RequestType::Register,
            "deregisterInstance" => RequestType::Deregister,
            _ => RequestType::Register,
        }
    }
}
