//! 密码流水线内部错误分类与公开错误收敛。

use axum::http::StatusCode;

/// 密码流水线内部可观测的低基数错误类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoErrorKind {
    /// 路由策略、注册表或不可变快照不一致。
    Policy,
    /// 外层信封、编码或 JSON 结构不符合静态协议。
    Envelope,
    /// 外层或解密后数据超过公开大小合同。
    TooLarge,
    /// kid 不属于当前租户和路由密钥域，或密钥不可使用。
    Key,
    /// 密文认证、旧协议解密或算法检查失败。
    Authentication,
    /// 请求时间戳超出允许窗口。
    Expired,
    /// rid 已经被原子占位。
    Replayed,
    /// 重放存储不可用或超时。
    ReplayBackend,
    /// CPU 队列、内存预算或其它进程资源饱和。
    Overloaded,
    /// 单次密码操作超过绝对 deadline。
    Timeout,
    /// handler 响应不满足静态响应合同。
    ResponseContract,
    /// 未分类的内部故障。
    Internal,
}

/// 不携带密钥、明文和完整密文的密码流水线错误。
#[derive(Debug, Clone)]
pub struct CryptoError {
    /// 供指标与统一 HTTP 映射使用的稳定类别。
    pub kind: CryptoErrorKind,
    /// 由实现写死的低基数内部原因，不得拼接外部输入或底层敏感错误。
    pub internal_reason: &'static str,
}

impl CryptoError {
    /// 业务作用：建立可在安全边界内传播的内部错误。
    ///
    /// # 参数
    ///
    /// - `kind`: 统一错误映射使用的稳定类别。
    /// - `internal_reason`: 受控内部原因，禁止包含 key、token、明文和完整密文。
    ///
    /// # 返回
    ///
    /// 返回不泄露底层实现细节的错误值。
    pub const fn new(kind: CryptoErrorKind, internal_reason: &'static str) -> Self {
        Self {
            kind,
            internal_reason,
        }
    }

    /// 业务作用：返回公开协议错误码。
    ///
    /// # 返回
    ///
    /// 在可信密码上下文建立前收敛解析、key 与 tag 差异；大小、过期、重放和服务故障保留稳定类别。
    pub const fn public_code(&self) -> &'static str {
        match self.kind {
            CryptoErrorKind::Envelope | CryptoErrorKind::Key | CryptoErrorKind::Authentication => {
                "invalid_secure_request"
            }
            CryptoErrorKind::TooLarge => "secure_request_too_large",
            CryptoErrorKind::Expired => "secure_request_expired",
            CryptoErrorKind::Replayed => "secure_request_replayed",
            CryptoErrorKind::Policy
            | CryptoErrorKind::ReplayBackend
            | CryptoErrorKind::Overloaded
            | CryptoErrorKind::Timeout
            | CryptoErrorKind::ResponseContract
            | CryptoErrorKind::Internal => "secure_service_unavailable",
        }
    }

    /// 业务作用：返回公开 HTTP 状态码。
    ///
    /// # 返回
    ///
    /// 大小错误为 413，过期和重放为 409，资源与依赖故障为 503，其余未认证协议错误为 400。
    pub const fn status(&self) -> StatusCode {
        match self.kind {
            CryptoErrorKind::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            CryptoErrorKind::Expired | CryptoErrorKind::Replayed => StatusCode::CONFLICT,
            CryptoErrorKind::Policy
            | CryptoErrorKind::ReplayBackend
            | CryptoErrorKind::Overloaded
            | CryptoErrorKind::Timeout
            | CryptoErrorKind::ResponseContract
            | CryptoErrorKind::Internal => StatusCode::SERVICE_UNAVAILABLE,
            CryptoErrorKind::Envelope | CryptoErrorKind::Key | CryptoErrorKind::Authentication => {
                StatusCode::BAD_REQUEST
            }
        }
    }
}

impl std::fmt::Display for CryptoError {
    /// 业务作用：输出不含底层密码详情的错误类别。
    ///
    /// # 参数
    ///
    /// - `formatter`: 错误链提供的输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "端点安全处理失败：{:?}", self.kind)
    }
}

impl std::error::Error for CryptoError {}
