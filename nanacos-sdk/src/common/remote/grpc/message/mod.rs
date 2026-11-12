use prost_types::Any;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::fmt::Debug;
use tracing::{error, warn};

use crate::api::error::Error::{ErrResponse, ErrResult, Serialization};
use crate::api::error::Result;
use crate::api::plugin::RequestResource;
use crate::common::remote::grpc::message::response::ErrorResponse;
use crate::nacos_proto::v2::{Metadata, Payload};

/// 将正文转换失败统一交给调用方返回，成功时保留解码值。
macro_rules! try_convert_payload {
    ($result:expr, $body_any:expr, $target_type:expr) => {
        match $result {
            Ok(value) => value,
            Err(error) => {
                // 配置及认证响应可能包含秘密，解析失败只能记录预期类型和字节数。
                error!(
                    expected_type = %$target_type,
                    payload_bytes = $body_any.value.len(),
                    "Nacos gRPC 正文解码失败"
                );
                return Err(error);
            }
        }
    };
}

pub(crate) mod request;
pub(crate) mod response;

/// 同时持有 Nacos 协议元数据与已解码正文的消息。
#[derive(Debug)]
pub(crate) struct GrpcMessage<T>
where
    T: GrpcMessageData,
{
    /// 协议及认证头；读取或转发时保持原始键值。
    headers: HashMap<String, String>,
    /// 已按目标类型解码的业务正文。
    body: T,
    /// 消息声明的客户端地址，不作为认证身份。
    client_ip: String,
}

impl<T> GrpcMessage<T>
where
    T: GrpcMessageData,
{
    /// 业务作用：借用已解码的业务正文，供调用方在保留消息元数据时读取内容。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与当前消息同生命周期的正文引用，不复制或重新解析数据。
    #[allow(dead_code)]
    pub(crate) fn body(&self) -> &T {
        &self.body
    }

    /// 业务作用：在协议外层处理完成后移交业务正文，释放不再需要的消息元数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：正文所有权，头部及客户端地址随消息外层释放，不进行序列化。
    pub(crate) fn into_body(self) -> T {
        self.body
    }

    /// 业务作用：把已构造的消息编码为 Nacos gRPC Payload，附加类型身份、地址及请求头。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含完整 metadata 和 JSON 正文字节的 Payload；正文序列化失败时返回错误，不发送消息。
    pub(crate) fn into_payload(self) -> Result<Payload> {
        let meta_data = Metadata {
            r#type: T::identity().to_string(),
            client_ip: self.client_ip.to_string(),
            headers: self.headers,
        };

        let body = match self.body.to_proto_any() {
            Ok(proto_any) => proto_any,
            Err(error) => {
                // 自定义序列化错误也可能引用原始内容，诊断不展开错误对象。
                error!("Nacos gRPC 正文编码失败");
                return Err(error);
            }
        };

        let payload = Payload {
            metadata: Some(meta_data),
            body: Some(body),
        };
        Ok(payload)
    }

    /// 业务作用：按 Nacos 元数据类型解码响应，兼容未声明类型的响应并识别远端业务拒绝。
    ///
    /// 参数说明：
    /// - `payload`：收到的协议消息，必须包含正文；元数据缺失时按空类型处理。
    ///
    /// 返回：类型匹配且正文可解析时取得消息；类型不匹配时尝试 ErrorResponse。空类型先按目标解析，失败后再尝试错误响应；均失败时保留首次解析错误。
    pub(crate) fn from_payload(payload: Payload) -> Result<Self> {
        let body_any = match payload.body {
            Some(body) => body,
            None => return Err(ErrResult("grpc payload body is empty".to_string())),
        };

        let meta_data = payload.metadata.unwrap_or_default();
        let r_type = meta_data.r#type;
        let client_ip = meta_data.client_ip;
        let headers = meta_data.headers;

        // 已声明类型必须与目标一致；不匹配时只尝试标准错误响应，不将其它业务类型当作目标接受。
        if !r_type.is_empty() {
            if T::identity().eq(&r_type) {
                let ret: Result<T> = T::from_proto_any(&body_any);
                let de_body = try_convert_payload!(ret, body_any, T::identity());
                return Ok(GrpcMessage {
                    headers,
                    body: de_body,
                    client_ip,
                });
            }

            // 服务端业务拒绝使用独立类型，提取其错误代码与请求标识交给上层裁决。
            // 远端类型标识同样是不可信输入，不将其原文写入日志。
            warn!("Nacos gRPC 响应类型不匹配，尝试标准错误响应");
            let ret: Result<ErrorResponse> = ErrorResponse::from_proto_any(&body_any);
            let error_response = try_convert_payload!(ret, body_any, "ErrorResponse");
            return Err(ErrResponse(
                error_response.request_id,
                error_response.result_code,
                error_response.error_code,
                error_response.message,
            ));
        }

        // 兼容缺少类型标识的响应，先按调用方预期的正文合同解码。
        warn!("payload type is empty!");
        let ret: Result<T> = T::from_proto_any(&body_any);
        if let Ok(de_body) = ret {
            return Ok(GrpcMessage {
                headers,
                body: de_body,
                client_ip,
            });
        }

        // 目标合同无法解析时保留原错误，只有标准错误响应明确可解析才改用远端拒绝结果。
        let error = ret.unwrap_err();
        warn!(
            payload_bytes = body_any.value.len(),
            "Nacos gRPC 无类型正文不能按目标合同解码"
        );

        let ret: Result<ErrorResponse> = ErrorResponse::from_proto_any(&body_any);
        if let Ok(error_response) = ret {
            return Err(ErrResponse(
                error_response.request_id,
                error_response.result_code,
                error_response.error_code,
                error_response.message,
            ));
        }

        // 两种合同都无法解析时不能确认远端业务结果，返回首次目标解析错误。
        error!("Nacos gRPC 无类型正文也不符合标准错误响应");
        Err(error)
    }
}

/// Nacos JSON 正文与 protobuf Any 容器之间的编码合同。
pub(crate) trait GrpcMessageData:
    Debug + Clone + Serialize + DeserializeOwned + Send
{
    /// 业务作用：声明消息在 Nacos 协议中的类型身份，供封装与接收端分派使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：协议类型名称，须与对端合同一致；不能以业务正文替代该标识。
    fn identity<'a>() -> std::borrow::Cow<'a, str>;

    /// 业务作用：将消息正文编码为 Nacos 在 protobuf Any 内承载的 JSON 字节。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：type_url 为消息身份且 value 为 JSON 的 Any；序列化失败返回 Serialization，不创建部分有效的正文。
    fn to_proto_any(&self) -> Result<Any> {
        let byte_data = match serde_json::to_vec(self) {
            Ok(data) => data,
            Err(error) => {
                return Err(Serialization(error));
            }
        };
        let any = Any {
            type_url: Self::identity().to_string(),
            value: byte_data,
        };
        Ok(any)
    }

    /// 业务作用：从 Any 正文字节恢复调用方要求的 Nacos 消息类型。
    ///
    /// 参数说明：
    /// - `any`：承载 JSON 字节的正文容器；此方法不检查 type_url，外层负责元数据类型分派。
    ///
    /// 返回：JSON 符合目标类型时取得正文；解析失败返回 Serialization，不能仅凭容器类型认定正文有效。
    fn from_proto_any<T: GrpcMessageData>(any: &Any) -> Result<T> {
        let body = match serde_json::from_slice(&any.value) {
            Ok(data) => data,
            Err(error) => {
                return Err(Serialization(error));
            }
        };
        Ok(body)
    }
}

/// 请求正文提供的元数据及认证资源访问合同。
#[allow(dead_code)]
pub(crate) trait GrpcRequestMessage: GrpcMessageData {
    /// 业务作用：读取请求上的一个协议头，供认证及传输适配使用。
    ///
    /// 参数说明：
    /// - `key`：请求头集合中的确切键名，不执行大小写归一化。
    ///
    /// 返回：存在时借用原值，否则为空，不生成缺省认证材料。
    fn header(&self, key: &str) -> Option<&String>;

    /// 业务作用：借用请求头快照，供消息封装前查看协议及认证字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前头部集合的只读引用，读取不改变字段或移交所有权。
    fn headers(&self) -> &HashMap<String, String>;

    /// 业务作用：将请求头所有权移交给外层协议封装，避免重新复制整个头部集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原头部集合，生成实现同时清空请求中的头部；后续读取不再取得已移交字段。
    fn take_headers(&mut self) -> HashMap<String, String>;

    /// 业务作用：在发送前合并协议及认证头，使身份插件可以补齐请求材料。
    ///
    /// 参数说明：
    /// - `map`：要移入的头部集合，生成实现以传入值覆盖同名键。
    ///
    /// 返回：原地更新请求头，不触发网络发送或验证远端权限。
    fn add_headers(&mut self, map: HashMap<String, String>);

    /// 业务作用：读取请求关联标识，供响应匹配及控制消息关联使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已经分配的请求标识引用；未分配时为空，不在读取时分配新序号。
    fn request_id(&self) -> Option<&String>;

    /// 业务作用：声明请求所属的 SDK 协议模块，供连接及请求分派识别。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：请求类型固定的模块名称，不根据调用方正文动态扩展。
    fn module(&self) -> &str;

    /// 业务作用：提取认证插件需要的资源作用域，防止签名脱离当前请求目标。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：声明了资源合同的请求返回类型、命名空间、分组及资源信息；无需资源签名的内部请求可为空。
    fn request_resource(&self) -> Option<RequestResource>;
}

/// 响应正文提供的请求关联与业务状态读取合同。
#[allow(dead_code)]
pub(crate) trait GrpcResponseMessage: GrpcMessageData {
    /// 业务作用：读取响应关联的请求标识，供调用方核对请求与回执。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：响应携带的标识引用，缺失时为空；不以缺失标识推断业务成功。
    fn request_id(&self) -> Option<&String>;

    /// 业务作用：读取 Nacos 业务响应结果码，区分传输完成与操作结果。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：响应中的原始结果码，不执行重试或状态转换。
    fn result_code(&self) -> i32;

    /// 业务作用：读取远端对业务失败的细分类别，供调用方解释拒绝原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：响应中的原始错误码，必须结合结果码及当前操作判断含义。
    fn error_code(&self) -> i32;

    /// 业务作用：借用远端响应附带的业务说明，供错误转换保留上下文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可选的原始消息引用；内容由远端提供，不作为可信授权证据或已脱敏日志。
    fn message(&self) -> Option<&String>;

    /// 业务作用：按具体响应合同判断当前业务操作是否被接收端标记为成功。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：合同定义的成功状态为真；该判断不证明后续连接仍然健康或其它操作也已获权。
    fn is_success(&self) -> bool;
}

/// 在序列化前集中装配 Nacos 消息头与正文的构造器。
pub(crate) struct GrpcMessageBuilder<T>
where
    T: GrpcMessageData,
{
    /// 待移入最终消息的协议及认证头。
    headers: HashMap<String, String>,
    /// 调用方移交的业务正文，不在装配阶段重复编码。
    body: T,
    /// 构造器创建时读取的本地地址标签。
    client_ip: String,
}

impl<T> GrpcMessageBuilder<T>
where
    T: GrpcMessageData,
{
    /// 业务作用：为待发送的正文建立消息构造器，附加客户端本地地址来源。
    ///
    /// 参数说明：
    /// - `body`：已构造的业务正文，所有权移入构造器。
    ///
    /// 返回：头部为空的构造器；本地地址首次读取可能触发探测，但不发送协议请求。
    pub(crate) fn new(body: T) -> Self {
        GrpcMessageBuilder {
            headers: HashMap::<String, String>::new(),
            body,
            client_ip: crate::common::util::LOCAL_IP.to_owned(),
        }
    }

    /// 业务作用：在消息封装前设置单个协议或认证头。
    ///
    /// 参数说明：
    /// - `key`：协议头键名，按原文存储。
    /// - `value`：要发送的头值，覆盖同名键；此入口不校验凭据有效性。
    ///
    /// 返回：包含该字段的构造器，不进行序列化或网络发送。
    pub(crate) fn header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// 业务作用：合并调用方已准备的请求头，供最终 gRPC metadata 封装。
    ///
    /// 参数说明：
    /// - `headers`：将所有权移入的头部集合，同名键以传入值为准。
    ///
    /// 返回：合并后的构造器；不保证头值已被服务端认可。
    pub(crate) fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }

    /// 业务作用：结束消息装配，将正文、头部与地址交给可编码的消息对象。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：拥有全部字段的 GrpcMessage；不检查网络状态，不在此处分配请求标识或发送数据。
    pub(crate) fn build(self) -> GrpcMessage<T> {
        GrpcMessage {
            headers: self.headers,
            body: self.body,
            client_ip: self.client_ip,
        }
    }
}
