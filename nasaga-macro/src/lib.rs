//! `#[saga]` 参与方步骤属性宏：编译期合同检查、descriptor 收集与 adapter 生成。
//!
//! 宏**不拥有全局正确性**（§11.5.1）：它只做本地参与节点接入——校验属性与签名形态、
//! 生成静态 descriptor（linkme 收集，启动预检与 definition 对齐）和调用
//! `nasaga-runtime` 完整事务 wrapper 的 adapter 方法。状态推进、补偿决策、超时与
//! 崩溃恢复全部属于 Orchestrator/Runtime/Store，不在宏内。
//!
//! 当前接受纯本地 `local-fenceable`、实现了 `SagaResolveStep` 的 `resolve-only` Poll，
//! 以及同时实现 `SagaCancelStep + SagaResolveStep` 的 `externally-cancellable`。外部取消的
//! 真实裁决与 gate/result Outbox 由 runtime 同事务持有，绝不伪造取消成功。

#![deny(missing_docs)]

use std::collections::BTreeSet;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned as _;
use syn::{parse_macro_input, Expr, ItemImpl, Lit, LitStr, Meta, Token, Type};

/// 解析后的 `#[saga(...)]` 属性参数。
struct SagaArgs {
    /// workflow 名称字面量。
    workflow: LitStr,
    /// definition 版本。
    version: u32,
    /// 步骤名称字面量。
    step: LitStr,
    /// 是否可补偿；默认 true。
    compensable: bool,
    /// 是否允许 Unknown；只对带类型化 Poll adapter 的外部步骤开放。
    allow_unknown: bool,
    /// 取消形态；当前开放 local-fenceable、resolve-only 与 externally-cancellable。
    cancel_mode: String,
}

/// 业务作用：`#[saga]` 入口——校验合同并生成 descriptor 与 adapter。
///
/// 标注对象必须是 `impl SagaStep for ServiceType` 块；宏保留原实现不变，追加：
/// 1. `COLLECTED_SAGA_STEPS` 中的静态 descriptor（启动预检与 definition 对齐）；
/// 2. `ServiceType::saga_handle_command`——按 envelope phase 分发到 runtime 的
///    execute/cancel/compensate/resolve 完整事务 wrapper。
#[proc_macro_attribute]
pub fn saga(attr: TokenStream, item: TokenStream) -> TokenStream {
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let item_impl = parse_macro_input!(item as ItemImpl);
    match expand(metas, item_impl) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// 业务作用：完成属性解析、合同校验与代码生成的主流程。
///
/// 参数说明：
/// - `metas`: 属性参数列表。
/// - `item_impl`: 被标注的 impl 块。
///
/// 返回：校验全部通过时返回生成代码；任一合同违规返回定位到源码的编译错误。
fn expand(metas: Punctuated<Meta, Token![,]>, item_impl: ItemImpl) -> syn::Result<TokenStream2> {
    let args = parse_args(&metas)?;
    verify_impl_shape(&item_impl)?;
    let service_type = (*item_impl.self_ty).clone();
    let service_type_name = type_name_of(&service_type)?;

    let root = nasa_macro_support::runtime_root("saga", "nasaga-runtime")
        .map_err(|message| syn::Error::new(proc_macro2::Span::call_site(), message))?;
    let workflow = &args.workflow;
    let version = args.version;
    let step = &args.step;
    let allow_unknown = args.allow_unknown;
    let compensation = if args.compensable {
        quote!(#root::__private::core::Compensation::Compensable)
    } else {
        quote!(#root::__private::core::Compensation::NonCompensable)
    };
    let compensate_dispatch = if args.compensable {
        quote!(
            runtime
                .handle_authenticated_compensate(self, envelope, producer)
                .await
        )
    } else {
        // pivot 仍实现 SagaStep 是当前 trait 兼容面的限制，但 adapter 必须在业务调用前
        // fail-closed；否则伪造或人工误发 compensate 命令可撤销不可补偿效果。
        quote!(Err(#root::SagaCommandProcessingError::ContractInvalid.into()))
    };
    let (cancel_mode, resolution_mode, cancel_dispatch, resolve_dispatch) =
        match args.cancel_mode.as_str() {
            "local-fenceable" => (
                quote!(#root::__private::core::CancelMode::LocalFenceable),
                quote!(None),
                quote!(
                    runtime
                        .handle_authenticated_cancel_local(envelope, producer)
                        .await
                ),
                quote!(Err(#root::SagaCommandProcessingError::ContractInvalid.into())),
            ),
            "resolve-only" => (
                quote!(#root::__private::core::CancelMode::ResolveOnly),
                quote!(Some(#root::__private::core::ResolutionMode::Poll)),
                quote!(Err(#root::SagaCommandProcessingError::ContractInvalid.into())),
                // 该分支让 Rust 类型系统强制 Service 同时实现 SagaResolveStep；缺失实现
                // 会在应用编译期失败，不能带着 allow_unknown 的空能力进入 Ready。
                quote!(
                    runtime
                        .handle_authenticated_resolve(self, envelope, producer)
                        .await
                ),
            ),
            "externally-cancellable" => (
                quote!(#root::__private::core::CancelMode::ExternallyCancellable),
                quote!(Some(#root::__private::core::ResolutionMode::Poll)),
                // 该调用让 Rust 类型系统强制 Service 实现 SagaCancelStep；外部取消缺失
                // 时应用无法编译，不能在运行期退化成本地伪屏障。
                quote!(
                    runtime
                        .handle_authenticated_cancel_external(self, envelope, producer)
                        .await
                ),
                // ResolutionPending/execute Unknown 必须有同一 Service 的 Poll 收敛能力。
                quote!(
                    runtime
                        .handle_authenticated_resolve(self, envelope, producer)
                        .await
                ),
            ),
            _ => unreachable!("取消形态已在参数解析阶段封闭校验"),
        };

    Ok(quote! {
        #item_impl

        const _: () = {
            // descriptor 进入 linkme 收集点:启动预检据此做重复检查与 definition 对齐,
            // 不一致在 Ready 前失败,不等运行期毒消息暴露。
            #[#root::__private::linkme::distributed_slice(#root::COLLECTED_SAGA_STEPS)]
            #[linkme(crate = #root::__private::linkme)]
            static __NASAGA_STEP_DESCRIPTOR: #root::SagaStepDescriptor =
                #root::SagaStepDescriptor {
                    workflow: #workflow,
                    definition_version: #version,
                    step: #step,
                    service_type: #service_type_name,
                    compensation: #compensation,
                    cancel_mode: #cancel_mode,
                    allow_unknown: #allow_unknown,
                    resolution_mode: #resolution_mode,
                    source: concat!(file!(), ":", line!()),
                };
        };

        impl #service_type {
            /// 业务作用：按 envelope 阶段把 Saga 命令分发到 runtime 的完整事务 wrapper。
            ///
            /// 事务序（Inbox claim → step gate → 业务 → 结果 Outbox → COMMIT）由
            /// runtime 统一持有；业务实现不得绕开本入口自行 publish 结果事件。
            ///
            /// 参数说明：
            /// - `runtime`: 参与方运行时。
            /// - `envelope`: 已通过 transport 认证的命令 envelope。
            /// - `producer`: transport 从可信凭据映射出的 Orchestrator 逻辑身份。
            ///
            /// 返回：可 ACK 的处理结论；`Retryable` 或基础设施失败返回错误
            /// （事务已回滚，不得 ACK）。
            pub async fn saga_handle_command(
                &self,
                runtime: &#root::ParticipantRuntime,
                envelope: &#root::SagaCommandEnvelope,
                producer: &#root::__private::core::ServiceIdentity,
            ) -> #root::__private::anyhow::Result<#root::ParticipantHandled> {
                // 即使调用方绕过 hosted transport 直接调用 adapter，也必须在 Inbox claim
                // 前绑定宏声明的精确步骤，避免可信 Orchestrator 的错 topic 路由执行错误业务。
                if envelope.workflow != #workflow
                    || envelope.definition_version != #version
                    || envelope.step != #step
                {
                    return Err(#root::SagaCommandProcessingError::RouteUnauthorized.into());
                }
                match envelope.phase.as_str() {
                    "execute" => runtime.handle_authenticated_execute(self, envelope, producer, #allow_unknown).await,
                    "cancel" => #cancel_dispatch,
                    "compensate" => #compensate_dispatch,
                    "resolve" => #resolve_dispatch,
                    _ => Err(#root::SagaCommandProcessingError::ContractInvalid.into()),
                }
            }
        }

        impl #root::SagaCommandService for #service_type {
            async fn handle_saga_command<'a>(
                &'a self,
                runtime: &'a #root::ParticipantRuntime,
                envelope: &'a #root::SagaCommandEnvelope,
                producer: &'a #root::__private::core::ServiceIdentity,
            ) -> #root::__private::anyhow::Result<#root::ParticipantHandled> {
                self.saga_handle_command(runtime, envelope, producer).await
            }
        }
    })
}

/// 业务作用：解析并校验 `#[saga(...)]` 属性参数。
///
/// 参数说明：
/// - `metas`: 属性参数列表。
///
/// 返回：全部合法返回参数集；缺失必填项、名称越界、定义版本为零，或取消形态与
/// `allow_unknown` 不自洽时返回编译错误。
fn parse_args(metas: &Punctuated<Meta, Token![,]>) -> syn::Result<SagaArgs> {
    let mut workflow: Option<LitStr> = None;
    let mut version: Option<(u32, proc_macro2::Span)> = None;
    let mut step: Option<LitStr> = None;
    let mut compensable = true;
    let mut cancel_mode = "local-fenceable".to_string();
    let mut cancel_mode_span = proc_macro2::Span::call_site();
    let mut allow_unknown = false;
    let mut allow_unknown_span = proc_macro2::Span::call_site();
    let mut seen = BTreeSet::new();

    for meta in metas {
        let Meta::NameValue(name_value) = meta else {
            return Err(syn::Error::new(
                meta.span(),
                "#[saga] 只接受 `name = value` 形式的参数",
            ));
        };
        let name = name_value
            .path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_default();
        // 重复属性不能采用“最后一个覆盖前一个”：代码审查者与编译器可能看到不同合同，
        // 尤其会把 compensable=false 的 pivot 静默改成可补偿步骤。
        if !seen.insert(name.clone()) {
            return Err(syn::Error::new(
                name_value.span(),
                format!("#[saga] 参数 `{name}` 重复"),
            ));
        }
        match name.as_str() {
            "workflow" => workflow = Some(expect_str(&name_value.value, "workflow")?),
            "step" => step = Some(expect_str(&name_value.value, "step")?),
            "version" => {
                version = Some((expect_u32(&name_value.value, "version")?, name_value.span()))
            }
            "compensable" => compensable = expect_bool(&name_value.value, "compensable")?,
            "cancel_mode" => {
                let lit = expect_str(&name_value.value, "cancel_mode")?;
                cancel_mode = lit.value();
                cancel_mode_span = lit.span();
            }
            "allow_unknown" => {
                allow_unknown = expect_bool(&name_value.value, "allow_unknown")?;
                allow_unknown_span = name_value.span();
            }
            other => {
                return Err(syn::Error::new(
                    name_value.span(),
                    format!("#[saga] 不认识参数 `{other}`"),
                ));
            }
        }
    }

    let workflow = workflow.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[saga] 缺少必填参数 `workflow`",
        )
    })?;
    let step = step.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[saga] 缺少必填参数 `step`",
        )
    })?;
    let (version, version_span) = version.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[saga] 缺少必填参数 `version`",
        )
    })?;

    validate_identifier(&workflow, "workflow")?;
    validate_identifier(&step, "step")?;
    // 定义版本从 1 开始:0 会让“未设置”与首个版本混淆,definition 侧同样拒绝。
    if version == 0 {
        return Err(syn::Error::new(version_span, "version 必须从 1 开始"));
    }
    match cancel_mode.as_str() {
        "local-fenceable" if allow_unknown => {
            // 纯本地步骤禁 Unknown(与 definition 预检同源)：本地数据库事务结果可知，
            // 允许 Unknown 只会制造不必要的人工对账。
            return Err(syn::Error::new(
                allow_unknown_span,
                "local-fenceable 步骤不允许 allow_unknown = true：本地事务没有\"结果未知\"",
            ));
        }
        "local-fenceable" => {}
        "resolve-only" if !allow_unknown => {
            return Err(syn::Error::new(
                allow_unknown_span,
                "resolve-only 步骤必须声明 allow_unknown = true 并实现 SagaResolveStep",
            ));
        }
        "resolve-only" => {}
        "externally-cancellable" if !allow_unknown => {
            return Err(syn::Error::new(
                allow_unknown_span,
                "externally-cancellable 步骤必须声明 allow_unknown = true 并实现 SagaCancelStep + SagaResolveStep",
            ));
        }
        "externally-cancellable" => {}
        _ => {
            return Err(syn::Error::new(
                cancel_mode_span,
                "cancel_mode 只接受 local-fenceable、resolve-only 或 externally-cancellable",
            ));
        }
    }

    Ok(SagaArgs {
        workflow,
        version,
        step,
        compensable,
        allow_unknown,
        cancel_mode,
    })
}

/// 业务作用：校验被标注对象是 `impl SagaStep for Type`，并拒绝宏可见的合同冲突。
///
/// 参数说明：
/// - `item_impl`: 被标注的 impl 块。
///
/// 返回：形态合法返回 `Ok`；非 trait impl、目标 trait 不是 `SagaStep`、带泛型参数
/// 或方法上叠加 `#[transactional]` 时返回编译错误。
fn verify_impl_shape(item_impl: &ItemImpl) -> syn::Result<()> {
    let Some((_, trait_path, _)) = item_impl.trait_.as_ref() else {
        return Err(syn::Error::new(
            item_impl.span(),
            "#[saga] 只能标注 `impl SagaStep for ServiceType` 块",
        ));
    };
    let is_saga_step = trait_path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "SagaStep");
    if !is_saga_step {
        return Err(syn::Error::new(
            trait_path.span(),
            "#[saga] 只能标注 SagaStep 的实现",
        ));
    }
    // adapter 需要具体 Service 类型:泛型 impl 无法生成稳定 descriptor 与解析入口。
    if !item_impl.generics.params.is_empty() {
        return Err(syn::Error::new(
            item_impl.generics.span(),
            "#[saga] 不支持泛型 impl：descriptor 与 adapter 需要具体 Service 类型",
        ));
    }
    for item in &item_impl.items {
        if let syn::ImplItem::Fn(method) = item {
            for attr in &method.attrs {
                // 事务边界由 #[saga] wrapper 独占:叠加 #[transactional] 会产生
                // 第二套互相嵌套的事务语义,直接拒绝。
                if attr
                    .path()
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "transactional")
                {
                    return Err(syn::Error::new(
                        attr.span(),
                        "#[saga] 与 #[transactional] 不允许叠加：本地事务由 saga wrapper 独占",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// 业务作用：取被标注 Service 类型的展示名，写入 descriptor 供诊断与容器解析。
///
/// 参数说明：
/// - `service_type`: impl 目标类型。
///
/// 返回：路径类型返回末段名称；不支持的类型形态返回编译错误。
fn type_name_of(service_type: &Type) -> syn::Result<String> {
    match service_type {
        Type::Path(path) => Ok(path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default()),
        other => Err(syn::Error::new(
            other.span(),
            "#[saga] 需要具体的 Service 类型路径",
        )),
    }
}

/// 业务作用：校验名称字面量满足严格标识符规则（与 nasaga-core 同一字符集）。
///
/// 名称会进入指标标签、topic 与身份派生，放开字符集会同时带来基数污染与身份歧义。
///
/// 参数说明：
/// - `lit`: 名称字面量。
/// - `kind`: 错误信息中的参数名。
///
/// 返回：合法返回 `Ok`；空白、带首尾空格、超长或含非法字符返回编译错误。
fn validate_identifier(lit: &LitStr, kind: &str) -> syn::Result<()> {
    let value = lit.value();
    if value.is_empty() || value.trim() != value {
        return Err(syn::Error::new(
            lit.span(),
            format!("{kind} 不能为空或带首尾空格"),
        ));
    }
    if value.len() > 128 {
        return Err(syn::Error::new(lit.span(), format!("{kind} 超过 128 字节")));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(syn::Error::new(
            lit.span(),
            format!("{kind} 只允许 ASCII 字母数字与 `_`、`-`、`.`"),
        ));
    }
    Ok(())
}

/// 业务作用：从属性值提取字符串字面量。
///
/// 参数说明：
/// - `expr`: 属性值表达式。
/// - `name`: 错误信息中的参数名。
///
/// 返回：字符串字面量；其它表达式返回编译错误。
fn expect_str(expr: &Expr, name: &str) -> syn::Result<LitStr> {
    if let Expr::Lit(lit) = expr {
        if let Lit::Str(value) = &lit.lit {
            return Ok(value.clone());
        }
    }
    Err(syn::Error::new(
        expr.span(),
        format!("`{name}` 需要字符串字面量"),
    ))
}

/// 业务作用：从属性值提取 u32 字面量。
///
/// 参数说明：
/// - `expr`: 属性值表达式。
/// - `name`: 错误信息中的参数名。
///
/// 返回：u32 值；其它表达式或越界返回编译错误。
fn expect_u32(expr: &Expr, name: &str) -> syn::Result<u32> {
    if let Expr::Lit(lit) = expr {
        if let Lit::Int(value) = &lit.lit {
            return value.base10_parse::<u32>().map_err(|_| {
                syn::Error::new(expr.span(), format!("`{name}` 需要 u32 范围内的整数"))
            });
        }
    }
    Err(syn::Error::new(
        expr.span(),
        format!("`{name}` 需要整数字面量"),
    ))
}

/// 业务作用：从属性值提取 bool 字面量。
///
/// 参数说明：
/// - `expr`: 属性值表达式。
/// - `name`: 错误信息中的参数名。
///
/// 返回：bool 值；其它表达式返回编译错误。
fn expect_bool(expr: &Expr, name: &str) -> syn::Result<bool> {
    if let Expr::Lit(lit) = expr {
        if let Lit::Bool(value) = &lit.lit {
            return Ok(value.value);
        }
    }
    Err(syn::Error::new(
        expr.span(),
        format!("`{name}` 需要布尔字面量"),
    ))
}
