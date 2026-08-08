//! RFC 9457 `application/problem+json` 统一 Web 错误契约。
//!
//! [`ApiProblem`] 是现代 profile 的统一错误表示(取代 RFC 7807)。核心成员 `type`/`title`/`status`/
//! `detail`/`instance` 之外,用扩展成员 `code`(稳定错误码)与 `violations`(字段级校验违规)。
//!
//! **脱敏纪律**:`detail` 只放**客户端可修复**信息;底层错误链、SQL、Redis key、Token、密钥、
//! 内部路径**不得**出现在任何成员里。Problem Details 不是内部调试信息的泄露通道。

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::{Map, Value};

/// 一条字段级校验违规(RFC 9457 扩展成员 `violations` 的元素)。
#[derive(Debug, Clone, Serialize)]
pub struct FieldViolation {
    /// 出错字段路径(如 `email`、`items[0].qty`)。
    pub field: String,
    /// 稳定违规码(如 `required`、`out_of_range`),用于客户端分支,非高基数。
    pub code: &'static str,
    /// 面向客户端的可修复描述,不含内部细节。
    pub message: String,
}

/// RFC 9457 问题详情。序列化为 `application/problem+json`。
///
/// 字段全部只承载**对外安全**信息;构造方负责保证 `detail`/`violations` 不含秘密或内部实现细节。
#[derive(Debug, Clone)]
pub struct ApiProblem {
    /// 问题类型 URI;无专属文档时用 `"about:blank"`。
    pub type_uri: &'static str,
    /// 简短、可读、稳定的问题标题(不随实例变化)。
    pub title: &'static str,
    /// HTTP 状态码。
    pub status: StatusCode,
    /// 稳定业务/错误码(扩展成员),供客户端稳定分支。
    pub code: &'static str,
    /// 面向客户端的可修复描述;`None` 时省略。**不得**含底层错误链/SQL/key/token/路径。
    pub detail: Option<String>,
    /// 本次发生的标识(通常是 request id);`None` 时省略。
    pub instance: Option<String>,
    /// 字段级校验违规;为空时省略该成员。
    pub violations: Vec<FieldViolation>,
}

impl ApiProblem {
    /// 业务作用：用类型、标题、状态与错误码创建一个最小 problem(无 detail/instance/violations)。
    pub fn new(
        type_uri: &'static str,
        title: &'static str,
        status: StatusCode,
        code: &'static str,
    ) -> Self {
        Self {
            type_uri,
            title,
            status,
            code,
            detail: None,
            instance: None,
            violations: Vec::new(),
        }
    }

    /// 业务作用：设置面向客户端的 `detail`(构造方保证不含秘密)。
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// 业务作用：设置 `instance`(通常是 request id)。
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    /// 业务作用：追加字段级校验违规。
    pub fn with_violations(mut self, violations: Vec<FieldViolation>) -> Self {
        self.violations = violations;
        self
    }

    /// 业务作用：渲染为 RFC 9457 JSON 对象(`status` 为数字;空的 detail/instance/violations 省略)。
    pub fn to_problem_json(&self) -> Value {
        let mut object = Map::new();
        object.insert("type".to_owned(), Value::from(self.type_uri));
        object.insert("title".to_owned(), Value::from(self.title));
        object.insert("status".to_owned(), Value::from(self.status.as_u16()));
        object.insert("code".to_owned(), Value::from(self.code));
        if let Some(detail) = &self.detail {
            object.insert("detail".to_owned(), Value::from(detail.as_str()));
        }
        if let Some(instance) = &self.instance {
            object.insert("instance".to_owned(), Value::from(instance.as_str()));
        }
        if !self.violations.is_empty() {
            object.insert(
                "violations".to_owned(),
                serde_json::to_value(&self.violations).unwrap_or(Value::Null),
            );
        }
        Value::Object(object)
    }
}

impl IntoResponse for ApiProblem {
    /// 业务作用：以 `application/problem+json` 与对应状态码序列化响应。
    fn into_response(self) -> Response {
        let body = self.to_problem_json().to_string();
        (
            self.status,
            [(
                header::CONTENT_TYPE,
                "application/problem+json; charset=utf-8",
            )],
            body,
        )
            .into_response()
    }
}
