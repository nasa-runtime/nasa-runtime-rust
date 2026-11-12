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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空列表表示校验通过；非空列表将直接进入公开响应，实现方必须保证字段名与说明不泄露内部或敏感数据。
    fn validate(&self) -> Vec<FieldViolation>;
}

/// 业务作用：解析失败(400):不回显底层 serde 错误,只给稳定 code + 客户端纠正提示。
///
/// 参数说明：
/// - `kind`：由提取器选择的固定公开提示，不接受底层错误文本。
///
/// 返回：状态为 400、code 为 invalid_input 的问题响应，detail 使用指定提示。
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
///
/// 参数说明：
/// - `violations`：业务校验器提供的可公开字段违规列表，未经此处再脱敏。
///
/// 返回：状态为 422、code 为 validation_failed 的问题响应，保留传入的违规项。
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
///
/// 参数说明：
/// - `value`：已经完成解析、等待字段规则检查的载荷。
///
/// 返回：校验列表为空时交还载荷；有违规时丢弃载荷并返回 422 响应，阻止后续处理器取得未通过校验的数据。
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
    ///
    /// 参数说明：
    /// - `request`：将被 JSON 提取器消费的请求及正文。
    /// - `state`：传给底层提取器的共享路由状态。
    ///
    /// 返回：解析与校验均通过时提供载荷；所有 JSON 提取拒绝统一为 400，字段违规为 422，不回显底层解析原因。
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
    ///
    /// 参数说明：
    /// - `parts`：包含查询串的请求元数据，不消费正文。
    /// - `state`：传给底层查询提取器的共享路由状态。
    ///
    /// 返回：解析与校验通过时提供查询载荷；查询解析拒绝为 400，字段违规为 422，不公开底层错误。
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
    ///
    /// 参数说明：
    /// - `parts`：含路由匹配参数的请求元数据，不消费正文。
    /// - `state`：传给底层路径提取器的共享路由状态。
    ///
    /// 返回：解析与校验通过时提供路径载荷；路径提取拒绝统一为 400，字段违规为 422，不公开底层错误。
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(value) = Path::<T>::from_request_parts(parts, state)
            .await
            .map_err(|_| parse_problem("path parameters do not match the expected schema"))?;
        finish(value).map(ValidatedPath)
    }
}
