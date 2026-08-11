//! 输入校验提取器与统一错误。
//!
//! provider-neutral 的 [`ValidateRequest`] trait 由载荷类型实现(可适配 `validator`/`garde`/业务自写);
//! [`ValidatedJson`]/[`ValidatedQuery`]/[`ValidatedPath`] 先按对应 axum 提取器解析,再调 `validate()`,
//! 任一失败都映射为 RFC 9457 [`ApiProblem`](解析失败 400 `invalid_input`;校验失败 422 `validation_failed`),
//! **不**把底层 serde/解析错误原文暴露给客户端。
//!
//! 提取器 Rejection 用 `Response`(axum 惯例):它比一般错误大,但对提取器是标准做法,故本模块允许
//! `result_large_err`。

#![allow(clippy::result_large_err)]

use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::de::DeserializeOwned;

use crate::problem::{ApiProblem, FieldViolation};

/// provider-neutral 请求校验:返回字段级违规列表,空表示通过。
pub trait ValidateRequest {
    /// 业务作用：校验自身,返回**对外安全**的字段违规(不含内部细节)。
    fn validate(&self) -> Vec<FieldViolation>;
}

/// 业务作用：解析失败(400):不回显底层 serde 错误,只给稳定 code + 客户端纠正提示。
fn parse_problem(kind: &'static str) -> Response {
    ApiProblem::new(
        "about:blank",
        "Bad Request",
        StatusCode::BAD_REQUEST,
        "invalid_input",
    )
    .with_detail(kind)
    .into_response()
}

/// 业务作用：校验失败(422):带字段级违规。
fn validation_problem(violations: Vec<FieldViolation>) -> Response {
    ApiProblem::new(
        "about:blank",
        "Validation Failed",
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_failed",
    )
    .with_detail("one or more fields are invalid")
    .with_violations(violations)
    .into_response()
}

/// 业务作用：执行业务 DTO 校验；无违规时保留原值，有违规则统一转换为 422。
fn finish<T: ValidateRequest>(value: T) -> Result<T, Response> {
    let violations = value.validate();
    if violations.is_empty() {
        Ok(value)
    } else {
        Err(validation_problem(violations))
    }
}

/// 校验后的 JSON 请求体提取器。
pub struct ValidatedJson<T>(pub T);

impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned + ValidateRequest,
    S: Send + Sync,
{
    type Rejection = Response;

    /// 业务作用：先按 JSON DTO 提取，再执行业务字段校验并统一映射解析/校验错误。
    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(request, state)
            .await
            .map_err(|_| parse_problem("request body is not valid JSON for the expected schema"))?;
        finish(value).map(ValidatedJson)
    }
}

/// 校验后的查询参数提取器。
pub struct ValidatedQuery<T>(pub T);

impl<T, S> FromRequestParts<S> for ValidatedQuery<T>
where
    T: DeserializeOwned + ValidateRequest,
    S: Send + Sync,
{
    type Rejection = Response;

    /// 业务作用：从 query string 反序列化 DTO 后执行业务字段校验。
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(|_| parse_problem("query parameters do not match the expected schema"))?;
        finish(value).map(ValidatedQuery)
    }
}

/// 校验后的路径参数提取器。
pub struct ValidatedPath<T>(pub T);

impl<T, S> FromRequestParts<S> for ValidatedPath<T>
where
    T: DeserializeOwned + Send + ValidateRequest,
    S: Send + Sync,
{
    type Rejection = Response;

    /// 业务作用：从路由 path 参数反序列化 DTO 后执行业务字段校验。
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(value) = Path::<T>::from_request_parts(parts, state)
            .await
            .map_err(|_| parse_problem("path parameters do not match the expected schema"))?;
        finish(value).map(ValidatedPath)
    }
}
