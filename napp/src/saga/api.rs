//! HTTP 与 gRPC 共用的业务授权、查询游标和 Orchestrator 操作边界。

use super::*;

/// 业务作用：保存协议认证完成后的逻辑身份与固定权限，业务层不读取 HTTP header 或 TLS extension。
#[derive(Clone)]
pub(super) struct SagaApiActor {
    pub(super) identity: nasaga_runtime::ServiceIdentity,
    pub(super) tenants: BTreeSet<String>,
    pub(super) workflows: BTreeSet<String>,
    pub(super) permissions: BTreeSet<String>,
}

/// 业务作用：保存协议解码后的创建意图，原始正文与旧 JSON 入口共同交给唯一领域门禁。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaApiStart {
    pub(super) tenant_id: String,
    pub(super) saga_id: String,
    pub(super) workflow: String,
    pub(super) definition_version: u32,
    pub(super) expected_definition_digest: Option<String>,
    pub(super) business_key: String,
    pub(super) trigger_id: String,
    pub(super) deadline_at_ms: Option<i64>,
    pub(super) input: Option<serde_json::Value>,
    pub(super) payload: Option<nasaga_runtime::SagaPayload>,
}

/// 业务作用：封闭创建入口的业务失败分类，协议适配器只映射状态码，不重新推断错误原因。
pub(super) enum SagaApiStartFailure {
    Invalid,
    PermissionDenied,
    DefinitionInactive,
    DefinitionMismatch,
    RequestConflict,
    Quota,
    Unavailable,
}

impl SagaApiStartFailure {
    /// 业务作用：将唯一创建裁决投影到 HTTP 状态，不公开数据库错误。
    /// 参数说明：无。
    /// 返回：调用方可用于参数更正、幂等冲突或暂时重试的标准 HTTP 状态。
    #[cfg(feature = "web")]
    pub(super) fn http_status(self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::PermissionDenied => StatusCode::FORBIDDEN,
            Self::DefinitionInactive => StatusCode::UNPROCESSABLE_ENTITY,
            Self::DefinitionMismatch | Self::RequestConflict => StatusCode::CONFLICT,
            Self::Quota => StatusCode::TOO_MANY_REQUESTS,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// 业务作用：将唯一创建裁决投影为 gRPC status，并为请求摘要冲突保留稳定机器原因。
    /// 参数说明：无。
    /// 返回：对应 HTTP 业务语义的标准 status；原始存储错误保持隐藏。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub(super) fn grpc_status(self) -> nagrpc::Status {
        match self {
            Self::Invalid => nagrpc::Status::invalid_argument("Saga start request is invalid"),
            Self::PermissionDenied => {
                nagrpc::Status::permission_denied("Saga start permission is required")
            }
            Self::DefinitionInactive => {
                nagrpc::Status::failed_precondition("Saga definition is not active")
            }
            Self::DefinitionMismatch => {
                nagrpc::Status::failed_precondition("Saga definition digest does not match")
            }
            Self::RequestConflict => managed_grpc_error_info_status(
                nagrpc::Code::AlreadyExists,
                "Saga identity conflicts with another request",
                "SAGA_REQUEST_DIGEST_CONFLICT",
            ),
            Self::Quota => nagrpc::Status::resource_exhausted("Saga start quota is exhausted"),
            Self::Unavailable => {
                nagrpc::Status::unavailable("Saga start transaction is unavailable")
            }
        }
    }
}

/// 业务作用：表示已经脱离协议编码的实例过滤条件，两个入口共享默认值、边界与分页规则。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaApiQuery {
    pub(super) tenant_id: String,
    pub(super) workflow: Option<String>,
    #[serde(default)]
    pub(super) statuses: Vec<String>,
    pub(super) created_from_ms: Option<i64>,
    pub(super) created_to_ms: Option<i64>,
    pub(super) after_saga_id: Option<String>,
    pub(super) page_token: Option<String>,
    pub(super) page_size: Option<u32>,
}

/// 业务作用：保存统一 keyset 查询结果，协议适配器只负责各自的快照编码。
pub(super) struct SagaApiPage {
    pub(super) rows: Vec<nasaga_runtime::SagaInstanceSummary>,
    pub(super) next_page_token: Option<String>,
}

/// 业务作用：固定管理请求的业务身份、审计原因和 CAS 前置条件，不接受协议自报的动作名。
pub(super) struct SagaApiAdmin {
    pub(super) tenant_id: String,
    pub(super) saga_id: String,
    pub(super) operation_id: String,
    pub(super) reason: String,
    pub(super) expected_state_version: Option<u64>,
    pub(super) expected_control_version: Option<u64>,
}

/// 业务作用：表示协议无关的审计页请求，原始数据库游标只能在受签名 token 内流转。
pub(super) struct SagaApiAudit {
    pub(super) tenant_id: String,
    pub(super) saga_id: String,
    pub(super) page_size: Option<u32>,
    pub(super) page_token: Option<String>,
}

/// 业务作用：保存统一审计事实及下一页 token，协议只转换公开字段。
pub(super) struct SagaApiAuditPage {
    pub(super) records: Vec<nasaga_runtime::SagaAuditRecord>,
    pub(super) next_page_token: Option<String>,
}

impl ManagedApiFailure {
    /// 业务作用：统一映射只读和管理操作的 HTTP 失败语义，隐藏驱动与数据库细节。
    /// 参数说明：无。
    /// 返回：与业务拒绝类型对应的标准 HTTP 状态。
    #[cfg(feature = "web")]
    pub(super) fn http_status(self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::PermissionDenied => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::Concurrent | Self::Conflict => StatusCode::CONFLICT,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// 业务作用：统一映射只读和管理操作的 gRPC 失败语义，CAS 竞争与确定性状态拒绝保持可区分。
    /// 参数说明：无。
    /// 返回：参数、权限、存在性、容量、并发、状态或暂时故障对应的标准 status。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub(super) fn grpc_status(self) -> nagrpc::Status {
        match self {
            Self::Invalid => nagrpc::Status::invalid_argument("Saga request is invalid"),
            Self::PermissionDenied => {
                nagrpc::Status::permission_denied("Saga permission is required")
            }
            Self::NotFound => nagrpc::Status::not_found("Saga instance was not found"),
            Self::RateLimited => nagrpc::Status::resource_exhausted("Saga quota is exhausted"),
            Self::Concurrent => nagrpc::Status::aborted("Saga version no longer matches"),
            Self::Conflict => nagrpc::Status::failed_precondition("Saga action is not allowed"),
            Self::Unavailable => nagrpc::Status::unavailable("Saga operation is unavailable"),
        }
    }
}

impl SagaOrchestratorApi {
    /// 业务作用：统一读取 Registry 定义，权限检查先于键存在性和数据库查询。
    /// 参数说明：`actor` 为认证主体，`tenant`、`workflow` 与 `version` 是不可变定义键。
    /// 返回：合法授权命中返回保留的定义事实；非法键、越权、不存在或存储异常返回封闭分类。
    pub(super) async fn get_definition_record(
        &self,
        actor: &SagaApiActor,
        tenant: &str,
        workflow: &str,
        version: u32,
    ) -> Result<nasaga_runtime::DefinitionRecord, ManagedApiFailure> {
        Self::authorize_registry(actor, tenant, workflow)?;
        nasaga_runtime::__private::core::TenantId::new(tenant)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        nasaga_runtime::__private::core::WorkflowName::new(workflow)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        nasaga_runtime::__private::core::DefinitionVersion::new(version)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        load_managed_definition(self, tenant, workflow, version)
            .await
            .map_err(|_| ManagedApiFailure::Unavailable)?
            .ok_or(ManagedApiFailure::NotFound)
    }

    /// 业务作用：以同一权限和业务身份规则读取实例，跨租户请求不获得存在性信息。
    /// 参数说明：`actor` 为认证主体，`tenant` 与 `saga` 组成目标实例键。
    /// 返回：合法授权命中返回完整行；非法身份、越权、不存在或存储失败使用封闭分类。
    pub(super) async fn get_instance(
        &self,
        actor: &SagaApiActor,
        tenant: &str,
        saga: &str,
    ) -> Result<nasaga_runtime::SagaInstanceRow, ManagedApiFailure> {
        Self::authorize(actor, tenant, "read")?;
        let tenant = nasaga_runtime::__private::core::TenantId::new(tenant)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        let saga = nasaga_runtime::__private::core::SagaId::new(saga)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        self.load_instance(&tenant, &saga)
            .await
            .map_err(|error| classify_managed_api_failure(&error))?
            .ok_or(ManagedApiFailure::NotFound)
    }

    /// 业务作用：统一绑定管理权限、动作、幂等身份和 CAS，审计与状态变更只由同一核心事务执行。
    /// 参数说明：`actor` 为认证主体，`action` 来自固定协议路由，`request` 提供实例、原因和并发前置条件。
    /// 返回：事务成功后回读实例；不满足权限、状态或版本时不产生部分管理事实。
    pub(super) async fn administer_instance(
        &self,
        actor: &SagaApiActor,
        action: ManagedAdminAction,
        request: SagaApiAdmin,
    ) -> Result<nasaga_runtime::SagaInstanceRow, ManagedApiFailure> {
        Self::authorize(actor, &request.tenant_id, "admin")?;
        let tenant = nasaga_runtime::__private::core::TenantId::new(&request.tenant_id)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        let saga = nasaga_runtime::__private::core::SagaId::new(&request.saga_id)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        use nasaga_runtime::SagaManagementPermission as Permission;
        let permission = match action {
            ManagedAdminAction::Pause => Permission::Pause,
            ManagedAdminAction::Resume => Permission::Resume,
            ManagedAdminAction::RetryCompensation => Permission::RetryCompensation,
            ManagedAdminAction::RetryResolution => Permission::RetryResolution,
            ManagedAdminAction::ManualClose => Permission::ManualClose,
        };
        let management = nasaga_runtime::SagaManagementContext::new(
            actor.identity.as_str(),
            request.reason,
            [permission],
        )
        .map_err(|_| ManagedApiFailure::Invalid)?;
        let expectation = nasaga_runtime::SagaManagementExpectation::new(
            request.expected_state_version,
            request.expected_control_version,
        );
        self.administer(ManagedAdminRequest {
            action,
            management: &management,
            tenant: &tenant,
            saga_id: &saga,
            operation_id: &request.operation_id,
            now_ms: epoch_millis().map_err(|_| ManagedApiFailure::Unavailable)?,
            expectation,
        })
        .await
        .map_err(|error| classify_managed_api_failure(&error))?;
        self.load_instance(&tenant, &saga)
            .await
            .map_err(|error| classify_managed_api_failure(&error))?
            .ok_or(ManagedApiFailure::NotFound)
    }

    /// 业务作用：统一校验审计授权和页游标，保证 HTTP 与 gRPC 按相同事实顺序继续读取。
    /// 参数说明：`actor` 是已认证主体，`request` 定位审计范围和页面，`key` 为共享 token 权威。
    /// 返回：合法查询返回有界事实与下一页 token；越权或伪造游标在数据库读取前拒绝。
    pub(super) async fn query_audit(
        &self,
        actor: &SagaApiActor,
        request: SagaApiAudit,
        key: &[u8; 32],
    ) -> Result<SagaApiAuditPage, ManagedApiFailure> {
        Self::authorize(actor, &request.tenant_id, "audit")?;
        let tenant = nasaga_runtime::__private::core::TenantId::new(&request.tenant_id)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        let saga = nasaga_runtime::__private::core::SagaId::new(&request.saga_id)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        let page_size = request.page_size.unwrap_or(100);
        if page_size == 0 || page_size > 1000 {
            return Err(ManagedApiFailure::Invalid);
        }
        let scope = format!("{}\0{}\0audit", tenant.as_str(), saga.as_str());
        let cursor = Self::parse_page_token(
            key,
            &scope,
            request.page_token.as_deref().unwrap_or_default(),
        )
        .map_err(|_| ManagedApiFailure::Invalid)?
        .map(|value| serde_json::from_str::<nasaga_runtime::SagaAuditPageCursor>(&value))
        .transpose()
        .map_err(|_| ManagedApiFailure::Invalid)?;
        let management = nasaga_runtime::SagaManagementContext::new(
            actor.identity.as_str(),
            "read Saga audit trail",
            [nasaga_runtime::SagaManagementPermission::ReadAudit],
        )
        .map_err(|_| ManagedApiFailure::Invalid)?;
        let page = self
            .audit_page(&management, &tenant, &saga, cursor.as_ref(), page_size)
            .await
            .map_err(|error| classify_managed_api_failure(&error))?;
        let next_page_token = page
            .next_cursor
            .map(|cursor| {
                serde_json::to_string(&cursor).map(|value| Self::page_token(key, &scope, &value))
            })
            .transpose()
            .map_err(|_| ManagedApiFailure::Unavailable)?;
        Ok(SagaApiAuditPage {
            records: page.records,
            next_page_token,
        })
    }

    /// 业务作用：统一执行创建授权、身份与定义校验，并在数据库事务内复验同一 Catalog 资格。
    /// 参数说明：`actor` 是已认证主体，`request` 是完整业务意图，`trace` 为受信链路上下文，`state` 提供生命周期和执行权威。
    /// 返回：首次提交或相同摘要重放返回相同实例；参数、权限、定义或摘要不匹配时拒绝，失败不产生半完成实例。
    pub(super) async fn start_instance(
        &self,
        actor: &SagaApiActor,
        request: SagaApiStart,
        trace: Option<&nasaga_runtime::TraceContext>,
        state: &SagaRuntimeState,
    ) -> Result<nasaga_runtime::StartOutcome, SagaApiStartFailure> {
        use nasaga_runtime::__private::core::{
            BusinessKey, DefinitionVersion, SagaId, TenantId, TriggerKind, WorkflowName,
        };
        use SagaApiStartFailure as Failure;
        Self::authorize(actor, &request.tenant_id, "start")
            .map_err(|_| Failure::PermissionDenied)?;
        state.ensure_ready().map_err(|_| Failure::Unavailable)?;
        let saga_id = SagaId::new(&request.saga_id).map_err(|_| Failure::Invalid)?;
        let tenant = TenantId::new(&request.tenant_id).map_err(|_| Failure::Invalid)?;
        let workflow = WorkflowName::new(&request.workflow).map_err(|_| Failure::Invalid)?;
        let version =
            DefinitionVersion::new(request.definition_version).map_err(|_| Failure::Invalid)?;
        let business_key = BusinessKey::new(&request.business_key).map_err(|_| Failure::Invalid)?;
        if !managed_start_trigger_is_valid(&request.trigger_id)
            || (request.input.is_some() && request.payload.is_some())
        {
            return Err(Failure::Invalid);
        }
        let active_digest = self
            .definition_digest(&tenant, &workflow, version)
            .ok_or(Failure::DefinitionInactive)?;
        if request
            .expected_definition_digest
            .as_deref()
            .is_some_and(|expected| expected != active_digest)
        {
            return Err(Failure::DefinitionMismatch);
        }
        let now_ms = epoch_millis().map_err(|_| Failure::Unavailable)?;
        self.start_saga(
            &nasaga_runtime::StartSagaRequest {
                saga_id: &saga_id,
                tenant: &tenant,
                workflow: &workflow,
                version,
                business_key: &business_key,
                deadline_at_ms: request.deadline_at_ms,
                trigger_kind: TriggerKind::Event,
                trigger_id: &request.trigger_id,
                first_command_payload: request.input,
                first_command_raw_payload: request.payload,
                now_ms,
            },
            trace,
            state,
        )
        .await
        .map_err(
            |error| match nasaga_runtime::StartSagaError::from_error(&error) {
                Some(nasaga_runtime::StartSagaError::InvalidPayload) => Failure::Invalid,
                Some(nasaga_runtime::StartSagaError::RequestConflict) => Failure::RequestConflict,
                Some(nasaga_runtime::StartSagaError::TenantQuotaExceeded) => Failure::Quota,
                Some(
                    nasaga_runtime::StartSagaError::DefinitionInactive
                    | nasaga_runtime::StartSagaError::DefinitionEmpty,
                ) => Failure::DefinitionInactive,
                _ => Failure::Unavailable,
            },
        )
    }

    /// 业务作用：统一执行授权、过滤校验、keyset 解析和有界检索，保持 HTTP 与 gRPC 的分页语义相同。
    /// 参数说明：`actor` 是已认证主体，`request` 是协议无关过滤条件，`key` 为跨副本分页签名权威。
    /// 返回：返回一页摘要和可跨协议继续的 token；越权、非法过滤或游标在读取前拒绝，存储异常返回暂不可用。
    pub(super) async fn query_instances(
        &self,
        actor: &SagaApiActor,
        mut request: SagaApiQuery,
        key: &[u8; 32],
    ) -> Result<SagaApiPage, ManagedApiFailure> {
        use nasaga_runtime::__private::core::{SagaId, SagaStatus, TenantId, WorkflowName};
        Self::authorize(actor, &request.tenant_id, "read")?;
        let tenant = TenantId::new(&request.tenant_id).map_err(|_| ManagedApiFailure::Invalid)?;
        let workflow = request
            .workflow
            .as_deref()
            .map(WorkflowName::new)
            .transpose()
            .map_err(|_| ManagedApiFailure::Invalid)?;
        let page_size = request.page_size.unwrap_or(100);
        if page_size == 0 || page_size > 1000 {
            return Err(ManagedApiFailure::Invalid);
        }
        request.statuses.sort();
        request.statuses.dedup();
        let statuses = request
            .statuses
            .iter()
            .map(|status| SagaStatus::parse(status).ok_or(ManagedApiFailure::Invalid))
            .collect::<Result<Vec<_>, _>>()?;
        let time_ordered = request.created_from_ms.is_some() || request.created_to_ms.is_some();
        if request.after_saga_id.is_some() && (request.page_token.is_some() || time_ordered) {
            return Err(ManagedApiFailure::Invalid);
        }
        let scope = format!(
            "{}\0{}\0{:?}\0{:?}\0{:?}",
            request.tenant_id,
            request.workflow.as_deref().unwrap_or_default(),
            request.statuses,
            request.created_from_ms,
            request.created_to_ms
        );
        let token_cursor = Self::parse_page_token(
            key,
            &scope,
            request.page_token.as_deref().unwrap_or_default(),
        )
        .map_err(|_| ManagedApiFailure::Invalid)?;
        let (after_created_at_us, after) = match token_cursor.or(request.after_saga_id) {
            Some(cursor) if time_ordered => {
                let (time, id) = cursor.split_once('\0').ok_or(ManagedApiFailure::Invalid)?;
                (
                    Some(
                        time.parse::<i64>()
                            .map_err(|_| ManagedApiFailure::Invalid)?,
                    ),
                    Some(SagaId::new(id).map_err(|_| ManagedApiFailure::Invalid)?),
                )
            }
            Some(cursor) => (
                None,
                Some(SagaId::new(cursor).map_err(|_| ManagedApiFailure::Invalid)?),
            ),
            None => (None, None),
        };
        let management = nasaga_runtime::SagaManagementContext::new(
            actor.identity.as_str(),
            "list Saga instances",
            [nasaga_runtime::SagaManagementPermission::ListInstances],
        )
        .map_err(|_| ManagedApiFailure::Invalid)?;
        let query = nasaga_runtime::SagaInstanceQuery {
            tenant: &tenant,
            workflow: workflow.as_ref(),
            statuses: (!statuses.is_empty()).then_some(statuses.as_slice()),
            created_from_ms: request.created_from_ms,
            created_to_ms: request.created_to_ms,
            after_created_at_us,
            after: after.as_ref(),
            limit: page_size + 1,
        };
        nasaga_runtime::validate_saga_instance_query(&query)
            .map_err(|_| ManagedApiFailure::Invalid)?;
        let mut rows = self
            .list_instances(&management, &query)
            .await
            .map_err(|error| classify_managed_api_failure(&error))?;
        let has_more = rows.len() > page_size as usize;
        rows.truncate(page_size as usize);
        let next_page_token = has_more.then(|| {
            let row = rows.last().expect("nonempty page precedes another page");
            let cursor = if time_ordered {
                format!("{}\0{}", row.created_at_cursor_us, row.saga_id.as_str())
            } else {
                row.saga_id.as_str().to_owned()
            };
            Self::page_token(key, &scope, &cursor)
        });
        Ok(SagaApiPage {
            rows,
            next_page_token,
        })
    }

    /// 业务作用：在读取实例是否存在之前完成租户和操作授权，所有协议使用相同缺省拒绝政策。
    /// 参数说明：`actor` 是已认证主体，`tenant` 为目标租户，`permission` 为固定业务权限。
    /// 返回：租户范围和权限同时命中时成功，否则不允许访问业务存储。
    pub(super) fn authorize(
        actor: &SagaApiActor,
        tenant: &str,
        permission: &str,
    ) -> Result<(), ManagedApiFailure> {
        if (actor.tenants.contains("*") || actor.tenants.contains(tenant))
            && actor.permissions.contains(permission)
        {
            Ok(())
        } else {
            Err(ManagedApiFailure::PermissionDenied)
        }
    }

    /// 业务作用：将 definition 与 capability 管理权同时约束到租户和 workflow，不能通过协议切换扩大范围。
    /// 参数说明：`actor` 是已认证主体，`tenant` 与 `workflow` 共同定位完整业务授权目标。
    /// 返回：registry 权限与 workflow 范围全部命中时成功，否则拒绝。
    pub(super) fn authorize_registry(
        actor: &SagaApiActor,
        tenant: &str,
        workflow: &str,
    ) -> Result<(), ManagedApiFailure> {
        Self::authorize(actor, tenant, "registry")?;
        if actor.workflows.contains("*") || actor.workflows.contains(workflow) {
            Ok(())
        } else {
            Err(ManagedApiFailure::PermissionDenied)
        }
    }

    /// 业务作用：为实例和审计查询签发绑定完整过滤范围的同一格式游标，允许经授权的协议适配器共用分页权威。
    /// 参数说明：`key` 是跨副本一致的秘密材料，`scope` 是规范化过滤条件，`cursor` 是内部 keyset。
    /// 返回：正文和认证摘要均为规范小写十六进制的不透明 token。
    pub(super) fn page_token(key: &[u8; 32], scope: &str, cursor: &str) -> String {
        let canonical = format!("nasaga-page\0{}\0{}{}", scope.len(), scope, cursor);
        let signature = ncrypto::hmac_sha256(&canonical, &hex::encode(key));
        format!("{}.{}", hex::encode(cursor.as_bytes()), signature)
    }

    /// 业务作用：在数据库访问前复验 token 的范围、大小与认证摘要，禁止跨查询复用或伪造游标。
    /// 参数说明：`key` 与 `scope` 是当前查询权威，`token` 为空表示第一页。
    /// 返回：规范且认证成功时返回可选内部游标；格式、范围或材料不匹配时拒绝。
    pub(super) fn parse_page_token(
        key: &[u8; 32],
        scope: &str,
        token: &str,
    ) -> Result<Option<String>, ()> {
        if token.is_empty() {
            return Ok(None);
        }
        if token.len() > 4_161 {
            return Err(());
        }
        let (cursor_hex, signature) = token.split_once('.').ok_or(())?;
        if cursor_hex.len() % 2 != 0 || cursor_hex.len() > 4_096 || signature.len() != 64 {
            return Err(());
        }
        let cursor = String::from_utf8(hex::decode(cursor_hex).map_err(|_| ())?).map_err(|_| ())?;
        let expected = Self::page_token(key, scope, &cursor);
        // 比较完整规范表示，既限制大小写与编码歧义，也避免按摘要前缀提前返回。
        let difference = expected
            .as_bytes()
            .iter()
            .zip(token.as_bytes())
            .fold(0u8, |difference, (left, right)| difference | (left ^ right));
        if expected.len() != token.len() || difference != 0 {
            return Err(());
        }
        Ok(Some(cursor))
    }
}

impl SagaOrchestratorApi {
    /// 业务作用：以统一 owner、地址政策和数据库时钟登记逐实例能力。
    /// 参数说明：`actor` 是主体，`activation` 是路由政策，`descriptor` 是能力合同。
    /// 返回：成功返回持久租约；越权、非法地址或存储异常拒绝登记。
    pub(super) async fn register_capability_record(
        &self,
        actor: &SagaApiActor,
        activation: &ManagedDefinitionActivationContract,
        descriptor: &nasaga_runtime::CapabilityDescriptor,
    ) -> anyhow::Result<nasaga_runtime::CapabilityReceipt> {
        use nasaga_runtime::DefinitionCatalogError as Error;
        Self::authorize_registry(actor, &descriptor.tenant, &descriptor.workflow)
            .map_err(|_| Error::PermissionDenied)?;
        // owner 与可投递地址同时受限，未通过的能力不能成为激活或续租证据。
        if descriptor.owner != actor.identity.as_str()
            || descriptor.transport != activation.transport.as_str()
            || activation.address_policy.as_ref().is_none_or(|policy| {
                validate_capability_address(policy, descriptor, ApplicationPhase::Running).is_err()
            })
        {
            return Err(Error::PermissionDenied.into());
        }
        match self.driver() {
            natx_core::DatabaseDriver::MySql => {
                #[cfg(feature = "saga")]
                return nasaga_runtime::register_capability_for(
                    self.datasource_ref().as_str(),
                    &actor.identity,
                    descriptor,
                )
                .await;
                #[cfg(not(feature = "saga"))]
                anyhow::bail!("MySQL Saga runtime is unavailable");
            }
            natx_core::DatabaseDriver::PostgreSql => {
                #[cfg(feature = "saga-pgsql")]
                return nasaga_runtime_pgsql::register_capability_for(
                    self.datasource_ref().as_str(),
                    &actor.identity,
                    descriptor,
                )
                .await;
                #[cfg(not(feature = "saga-pgsql"))]
                anyhow::bail!("PostgreSQL Saga runtime is unavailable");
            }
        }
    }

    /// 业务作用：以统一授权和不可变摘要约束发布 definition。
    /// 参数说明：`actor` 是主体，`artifact` 是已验证签名的流程定义。
    /// 返回：返回首次或幂等发布记录；同键异摘要与越权拒绝写入。
    pub(super) async fn publish_definition_record(
        &self,
        actor: &SagaApiActor,
        artifact: &nasaga_runtime::DefinitionArtifact,
    ) -> anyhow::Result<(
        nasaga_runtime::DefinitionPublishDisposition,
        nasaga_runtime::DefinitionRecord,
    )> {
        Self::authorize_registry(actor, &artifact.tenant, &artifact.workflow)
            .map_err(|_| nasaga_runtime::DefinitionCatalogError::PermissionDenied)?;
        match self.driver() {
            natx_core::DatabaseDriver::MySql => {
                #[cfg(feature = "saga")]
                return nasaga_runtime::publish_definition_for(
                    self.datasource_ref().as_str(),
                    &actor.identity,
                    artifact,
                )
                .await;
                #[cfg(not(feature = "saga"))]
                anyhow::bail!("MySQL Saga runtime is unavailable");
            }
            natx_core::DatabaseDriver::PostgreSql => {
                #[cfg(feature = "saga-pgsql")]
                return nasaga_runtime_pgsql::publish_definition_for(
                    self.datasource_ref().as_str(),
                    &actor.identity,
                    artifact,
                )
                .await;
                #[cfg(not(feature = "saga-pgsql"))]
                anyhow::bail!("PostgreSQL Saga runtime is unavailable");
            }
        }
    }

    /// 业务作用：统一复验 definition 键、workflow owner 和激活证据，再由唯一事务执行生命周期动作。
    /// 参数说明：`actor` 是认证主体，`key` 定位定义，`target` 与 `operation` 固定动作、CAS 和审计，`identity/activation` 为协调者权威。
    /// 返回：成功返回持久记录；越权、无记录、证据不足或并发拒绝不改变生命周期。
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn change_definition_record(
        &self,
        actor: &SagaApiActor,
        key: (&str, &str, u32),
        target: nasaga_runtime::DefinitionLifecycle,
        operation: &nasaga_runtime::DefinitionLifecycleOperation,
        identity: &nasaga_runtime::ServiceIdentity,
        activation: &ManagedDefinitionActivationContract,
    ) -> anyhow::Result<nasaga_runtime::DefinitionRecord> {
        use nasaga_runtime::DefinitionCatalogError as Error;
        Self::authorize_registry(actor, key.0, key.1).map_err(|_| Error::PermissionDenied)?;
        nasaga_runtime::__private::core::TenantId::new(key.0)
            .map_err(|_| Error::InvalidArgument)?;
        nasaga_runtime::__private::core::WorkflowName::new(key.1)
            .map_err(|_| Error::InvalidArgument)?;
        nasaga_runtime::__private::core::DefinitionVersion::new(key.2)
            .map_err(|_| Error::InvalidArgument)?;
        let current = load_managed_definition(self, key.0, key.1, key.2)
            .await?
            .ok_or(Error::NotFound)?;
        // 激活探测会访问外部端点，必须先确认调用者持有目标 workflow。
        if current.artifact.workflow_owner != actor.identity.as_str() {
            return Err(Error::PermissionDenied.into());
        }
        let gate = if target == nasaga_runtime::DefinitionLifecycle::Active {
            Some(
                load_managed_definition_activation_gate(
                    self.driver(),
                    self.datasource_ref().as_str(),
                    identity,
                    activation,
                    key,
                    ApplicationPhase::Running,
                )
                .await
                .map_err(|_| Error::FailedPrecondition)?,
            )
        } else {
            None
        };
        apply_managed_definition_lifecycle(
            self.driver(),
            self.datasource_ref().as_str(),
            &actor.identity,
            key,
            target,
            operation,
            gate.as_ref(),
        )
        .await
    }

    /// 业务作用：在任何协议落库前用独立 Ed25519 seal 复验原始 definition 文档和 workflow 授权。
    /// 参数说明：`actor` 是认证主体，`keys` 是受信签名公钥，`signed` 保存未经改写的文档及签名。
    /// 返回：成功返回完整领域 artifact；格式、摘要、签名、身份或 seal 不成立时返回封闭 Catalog 错误。
    pub(super) fn validate_signed_definition(
        actor: &SagaApiActor,
        keys: &BTreeMap<String, String>,
        signed: &ManagedSignedDefinitionArtifact,
    ) -> anyhow::Result<nasaga_runtime::DefinitionArtifact> {
        use nasaga_runtime::DefinitionCatalogError as Error;
        if signed.format != "nasaga-definition-json"
            || managed_document_sha256(signed.canonical_document.as_bytes()) != signed.sha256
        {
            return Err(Error::InvalidArgument.into());
        }
        let artifact: nasaga_runtime::DefinitionArtifact =
            serde_json::from_str(&signed.canonical_document).map_err(|_| Error::InvalidArgument)?;
        Self::authorize_registry(actor, &artifact.tenant, &artifact.workflow)
            .map_err(|_| Error::PermissionDenied)?;
        let public_key = keys
            .get(&signed.signing_key_id)
            .ok_or(Error::PermissionDenied)?;
        // 传输认证只证明在线主体；不可变流程的长期来源必须由独立签名和 owner 一起确认。
        if artifact.workflow_owner != actor.identity.as_str()
            || !ncrypto::verify_ed25519(&signed.canonical_document, &signed.signature, public_key)
        {
            return Err(Error::PermissionDenied.into());
        }
        artifact
            .to_definition()
            .map_err(|_| Error::InvalidArgument)?;
        Ok(artifact)
    }
}
