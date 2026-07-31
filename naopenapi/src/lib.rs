//! 确定性 OpenAPI 3.1 生成器。输入只能来自已经通过启动审计的静态路由事实。

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

const MAX_SCHEMA_BYTES: usize = 1024 * 1024;

/// 一份由业务 DTO 显式提供的 JSON Schema。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaContract {
    /// `components.schemas` 下的稳定名称。
    pub name: String,
    /// 完整 JSON Schema 文本。
    pub json_schema: String,
}

/// OpenAPI 参数位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParameterLocation {
    /// URL query 参数。
    Query,
    /// HTTP header 参数。
    Header,
}

impl ParameterLocation {
    /// OpenAPI `in` 字段的稳定值。
    const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Header => "header",
        }
    }
}

/// 一条显式 query/header 参数合同。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterContract {
    /// 参数名称。
    pub name: String,
    /// 参数位置。
    pub location: ParameterLocation,
    /// 是否必填。
    pub required: bool,
    /// 参数值 schema。
    pub schema: SchemaContract,
}

/// 一条成功响应之外的显式响应合同。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseContract {
    /// HTTP 状态码。
    pub status: u16,
    /// 非空响应描述。
    pub description: String,
    /// 响应媒体类型；无响应体时为 `None`。
    pub media_type: Option<String>,
    /// 响应 schema；无响应体时为 `None`。
    pub schema: Option<SchemaContract>,
}

/// 一条已经冻结的 HTTP 路由合同。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteContract {
    /// 大写 HTTP 方法。
    pub method: String,
    /// OpenAPI 路径模板。
    pub path: String,
    /// 稳定 operationId 来源。
    pub operation_id: String,
    /// 可选请求媒体类型。
    pub consumes: Option<String>,
    /// 可选响应媒体类型。
    pub produces: Option<String>,
    /// 可选请求 DTO schema。
    pub request_schema: Option<SchemaContract>,
    /// 可选响应 DTO schema。
    pub response_schema: Option<SchemaContract>,
    /// query/header 参数合同。
    pub parameters: Vec<ParameterContract>,
    /// 主要成功响应状态码。
    pub success_status: u16,
    /// 成功响应之外的额外响应。
    pub additional_responses: Vec<ResponseContract>,
    /// 是否为流式响应；生成 `x-streaming=true`，且必须显式声明响应媒体类型。
    pub streaming: bool,
    /// 是否要求 Bearer 身份。
    pub auth_required: bool,
}

/// 合同构建错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiError {
    /// 标题或版本为空。
    InvalidInfo,
    /// 方法不在 OpenAPI HTTP operation 集。
    UnsupportedMethod(String),
    /// 路径不合法。
    InvalidPath(String),
    /// method/path 重复。
    DuplicateOperation(String),
    /// operationId 在整个文档中重复。
    DuplicateOperationId(String),
    /// 媒体类型为空或包含非法分隔符。
    InvalidMediaType(String),
    /// schema 名称不符合 OpenAPI component key 约束。
    InvalidSchemaName(String),
    /// schema 文本不是有效 JSON Schema。
    InvalidSchema(String),
    /// schema 文本超过生成器硬上限。
    SchemaTooLarge(String),
    /// 同名 schema 的结构不一致。
    ConflictingSchema(String),
    /// 请求 schema 没有对应的请求媒体类型。
    RequestSchemaWithoutMediaType(String),
    /// 参数名为空、超长、含非法字符或同位置重名。
    InvalidParameter(String),
    /// 响应状态码非法或重复。
    InvalidResponse(String),
    /// 响应 schema 没有对应媒体类型。
    ResponseSchemaWithoutMediaType(String),
    /// streaming 路由没有显式媒体类型。
    StreamingWithoutMediaType(String),
}

impl std::fmt::Display for OpenApiError {
    /// 输出稳定合同错误分类，不包含完整 schema 文本。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "OpenAPI contract error: {self:?}")
    }
}

impl std::error::Error for OpenApiError {}

/// 生成排序稳定的 OpenAPI 3.1 JSON 文档。
pub fn generate(
    title: &str,
    version: &str,
    routes: impl IntoIterator<Item = RouteContract>,
) -> Result<serde_json::Value, OpenApiError> {
    if title.trim().is_empty() || version.trim().is_empty() {
        return Err(OpenApiError::InvalidInfo);
    }
    let mut paths: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut operation_ids = BTreeSet::new();
    let mut schemas = BTreeMap::from([("Problem".to_owned(), problem_schema())]);
    let mut any_auth = false;
    for route in routes {
        let method = route.method.to_ascii_lowercase();
        if !matches!(
            method.as_str(),
            "get" | "put" | "post" | "delete" | "patch" | "head" | "options" | "trace"
        ) {
            return Err(OpenApiError::UnsupportedMethod(route.method));
        }
        if !route.path.starts_with('/') || route.path.contains('?') || route.path.contains('#') {
            return Err(OpenApiError::InvalidPath(route.path));
        }
        let mut parameters = path_parameters(&route.path)?;
        let mut parameter_keys = BTreeSet::new();
        for parameter in &route.parameters {
            validate_parameter_name(parameter)?;
            let normalized_name = if parameter.location == ParameterLocation::Header {
                parameter.name.to_ascii_lowercase()
            } else {
                parameter.name.clone()
            };
            if !parameter_keys.insert((parameter.location, normalized_name)) {
                return Err(OpenApiError::InvalidParameter(format!(
                    "{} {}",
                    parameter.location.as_str(),
                    parameter.name
                )));
            }
            let schema = register_schema(&mut schemas, &parameter.schema)?;
            parameters.push(serde_json::json!({
                "name": parameter.name,
                "in": parameter.location.as_str(),
                "required": parameter.required,
                "schema": schema
            }));
        }
        parameters.sort_by(|left, right| {
            let left_in = left["in"].as_str().unwrap_or_default();
            let right_in = right["in"].as_str().unwrap_or_default();
            left_in.cmp(right_in).then_with(|| {
                left["name"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["name"].as_str().unwrap_or_default())
            })
        });
        let key = format!("{} {}", method, route.path);
        if !seen.insert(key.clone()) {
            return Err(OpenApiError::DuplicateOperation(key));
        }
        let operation_id = stable_operation_id(&route.operation_id);
        if operation_id.is_empty() || !operation_ids.insert(operation_id.clone()) {
            return Err(OpenApiError::DuplicateOperationId(operation_id));
        }
        any_auth |= route.auth_required;
        if !(200..=399).contains(&route.success_status) {
            return Err(OpenApiError::InvalidResponse(format!(
                "{} success status {}",
                route.operation_id, route.success_status
            )));
        }
        if route.streaming && route.produces.is_none() {
            return Err(OpenApiError::StreamingWithoutMediaType(route.operation_id));
        }
        let response_media = media_type(route.produces.as_deref().unwrap_or("application/json"))?;
        let response_schema = match route.response_schema.as_ref() {
            Some(schema) => register_schema(&mut schemas, schema)?,
            None => serde_json::json!({}),
        };
        if matches!(route.success_status, 204 | 205 | 304) && route.response_schema.is_some() {
            return Err(OpenApiError::InvalidResponse(format!(
                "{} status {} cannot carry a response schema",
                route.operation_id, route.success_status
            )));
        }
        let mut responses = BTreeMap::new();
        let success_response = if matches!(route.success_status, 204 | 205 | 304) {
            serde_json::json!({ "description": "Successful response" })
        } else {
            serde_json::json!({
                "description": "Successful response",
                "content": { (response_media): { "schema": response_schema } }
            })
        };
        responses.insert(route.success_status.to_string(), success_response);
        for response in &route.additional_responses {
            if !(100..=599).contains(&response.status) || response.description.trim().is_empty() {
                return Err(OpenApiError::InvalidResponse(format!(
                    "{} status {}",
                    route.operation_id, response.status
                )));
            }
            if matches!(response.status, 204 | 205 | 304)
                && (response.media_type.is_some() || response.schema.is_some())
            {
                return Err(OpenApiError::InvalidResponse(format!(
                    "{} status {} cannot carry response content",
                    route.operation_id, response.status
                )));
            }
            if response.schema.is_some() && response.media_type.is_none() {
                return Err(OpenApiError::ResponseSchemaWithoutMediaType(format!(
                    "{} status {}",
                    route.operation_id, response.status
                )));
            }
            let key = response.status.to_string();
            if responses.contains_key(&key) {
                return Err(OpenApiError::InvalidResponse(format!(
                    "{} duplicate status {}",
                    route.operation_id, response.status
                )));
            }
            let value = match response.media_type.as_deref() {
                Some(value) => {
                    let media = media_type(value)?;
                    let schema = match response.schema.as_ref() {
                        Some(schema) => register_schema(&mut schemas, schema)?,
                        None => serde_json::json!({}),
                    };
                    serde_json::json!({
                        "description": response.description,
                        "content": { (media): { "schema": schema } }
                    })
                }
                None => serde_json::json!({ "description": response.description }),
            };
            responses.insert(key, value);
        }
        responses
            .entry("400".to_owned())
            .or_insert_with(|| serde_json::json!({ "$ref": "#/components/responses/BadRequest" }));
        responses.entry("500".to_owned()).or_insert_with(
            || serde_json::json!({ "$ref": "#/components/responses/InternalError" }),
        );
        let mut operation = serde_json::json!({
            "operationId": operation_id,
            "responses": responses
        });
        if route.streaming {
            operation["x-streaming"] = serde_json::Value::Bool(true);
        }
        if !parameters.is_empty() {
            operation["parameters"] = serde_json::Value::Array(parameters);
        }
        if route.request_schema.is_some() && route.consumes.is_none() {
            return Err(OpenApiError::RequestSchemaWithoutMediaType(
                route.operation_id,
            ));
        }
        if let Some(consumes) = route.consumes {
            let media = media_type(&consumes)?;
            let request_schema = match route.request_schema.as_ref() {
                Some(schema) => register_schema(&mut schemas, schema)?,
                None => serde_json::json!({}),
            };
            operation["requestBody"] = serde_json::json!({
                "required": true,
                "content": { (media): { "schema": request_schema } }
            });
        }
        if route.auth_required {
            operation["security"] = serde_json::json!([{ "bearerAuth": [] }]);
            operation["responses"]["401"] =
                serde_json::json!({ "$ref": "#/components/responses/Unauthorized" });
            operation["responses"]["403"] =
                serde_json::json!({ "$ref": "#/components/responses/Forbidden" });
        }
        paths
            .entry(route.path)
            .or_default()
            .insert(method, operation);
    }
    let mut document = serde_json::json!({
        "openapi": "3.1.0",
        "info": { "title": title, "version": version },
        "paths": paths,
        "components": {
            "schemas": schemas,
            "responses": {
                "BadRequest": problem_response("Bad request"),
                "Unauthorized": problem_response("Unauthorized"),
                "Forbidden": problem_response("Forbidden"),
                "InternalError": problem_response("Internal server error")
            }
        }
    });
    if any_auth {
        document["components"]["securitySchemes"] = serde_json::json!({
            "bearerAuth": { "type": "http", "scheme": "bearer", "bearerFormat": "JWT" }
        });
    }
    Ok(document)
}

/// 校验 query/header 参数名；header 使用 RFC token，query 在此基础上允许方括号。
fn validate_parameter_name(parameter: &ParameterContract) -> Result<(), OpenApiError> {
    if parameter.name.is_empty() || parameter.name.len() > 128 {
        return Err(OpenApiError::InvalidParameter(parameter.name.clone()));
    }
    let valid = parameter.name.bytes().all(|byte| {
        let token = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );
        token || (parameter.location == ParameterLocation::Query && matches!(byte, b'[' | b']'))
    });
    if !valid {
        return Err(OpenApiError::InvalidParameter(parameter.name.clone()));
    }
    Ok(())
}

/// 构造 RFC 9457 Problem Details 的共享 OpenAPI schema。
fn problem_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["type", "title", "status", "code"],
        "properties": {
            "type": { "type": "string", "format": "uri-reference" },
            "title": { "type": "string" },
            "status": { "type": "integer", "minimum": 100, "maximum": 599 },
            "code": { "type": "string" },
            "detail": { "type": "string" },
            "request_id": { "type": "string" }
        }
    })
}

/// 校验并登记命名 schema；同名同结构复用，不同结构明确报冲突。
fn register_schema(
    schemas: &mut BTreeMap<String, serde_json::Value>,
    contract: &SchemaContract,
) -> Result<serde_json::Value, OpenApiError> {
    if contract.name.is_empty()
        || contract.name.len() > 128
        || !contract
            .name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(OpenApiError::InvalidSchemaName(contract.name.clone()));
    }
    if contract.json_schema.len() > MAX_SCHEMA_BYTES {
        return Err(OpenApiError::SchemaTooLarge(contract.name.clone()));
    }
    let schema: serde_json::Value = serde_json::from_str(&contract.json_schema)
        .map_err(|error| OpenApiError::InvalidSchema(format!("{}: {error}", contract.name)))?;
    if !schema.is_object() && !schema.is_boolean() {
        return Err(OpenApiError::InvalidSchema(format!(
            "{}: JSON Schema must be an object or boolean",
            contract.name
        )));
    }
    match schemas.get(&contract.name) {
        Some(existing) if existing != &schema => {
            return Err(OpenApiError::ConflictingSchema(contract.name.clone()));
        }
        Some(_) => {}
        None => {
            schemas.insert(contract.name.clone(), schema);
        }
    }
    Ok(serde_json::json!({
        "$ref": format!("#/components/schemas/{}", contract.name)
    }))
}

/// 构造引用共享 Problem schema 的标准错误响应组件。
fn problem_response(description: &str) -> serde_json::Value {
    serde_json::json!({
        "description": description,
        "content": {
            "application/problem+json": {
                "schema": { "$ref": "#/components/schemas/Problem" }
            }
        }
    })
}

/// 将 handler 标识中的非字母数字字符稳定归一为下划线。
fn stable_operation_id(handler: &str) -> String {
    handler
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect()
}

/// 提取并验证不带参数的 HTTP media type，拒绝超长或非法 token。
fn media_type(value: &str) -> Result<&str, OpenApiError> {
    if value.len() > 255 {
        return Err(OpenApiError::InvalidMediaType(value.to_owned()));
    }
    let media = value.split(';').next().unwrap_or(value).trim();
    let valid_token = |token: &str| {
        !token.is_empty()
            && token.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                    )
            })
    };
    let valid = media
        .split_once('/')
        .is_some_and(|(kind, subtype)| valid_token(kind) && valid_token(subtype));
    if !valid {
        return Err(OpenApiError::InvalidMediaType(value.to_owned()));
    }
    Ok(media)
}

/// 从路由模板提取唯一 `{name}` 段并生成必填 path parameter 合同。
fn path_parameters(path: &str) -> Result<Vec<serde_json::Value>, OpenApiError> {
    let mut names = BTreeSet::new();
    let mut parameters = Vec::new();
    for segment in path.split('/') {
        let opens = segment.starts_with('{');
        let closes = segment.ends_with('}');
        if opens != closes || (segment.contains('{') || segment.contains('}')) && !(opens && closes)
        {
            return Err(OpenApiError::InvalidPath(path.to_owned()));
        }
        if opens {
            let name = &segment[1..segment.len().saturating_sub(1)];
            if name.is_empty()
                || name.contains('{')
                || name.contains('}')
                || !names.insert(name.to_owned())
            {
                return Err(OpenApiError::InvalidPath(path.to_owned()));
            }
            parameters.push(serde_json::json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": { "type": "string" }
            }));
        }
    }
    Ok(parameters)
}
