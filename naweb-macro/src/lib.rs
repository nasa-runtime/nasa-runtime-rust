//! 带静态安全合同的 MVC 风格 Axum 路由与 interceptor 属性宏。
//!
//! 通过 linkme 在编译期收集 handler、auth、crypto 和 interceptor 元数据，并由 `mvc_router!`
//! 在业务 crate 根生成与 Application State 类型单态化的路由装配模块。普通路由仍可走兼容装配；
//! 任何安全元数据都必须经过 `try_register_all` 的 effective-plan 与 runtime 审计。
// ============================================================================
// naweb-macro —— #[*_mapping]、#[interceptor] 与 mvc_router! 的编译期前端。
//
// 原理：路由属性不改写 handler 业务函数，只用 linkme #[distributed_slice] 把方法、路径、媒体类型、
//   RoutePolicy、endpoint interceptor 和单态化注册闭包收进 crate::__mvc::ROUTES。mvc_router! 再生成：
//   - try_register_all：合并 global/scope/endpoint interceptor，审计 auth/crypto/runtime 后装配；
//   - register_all：仅供没有任何安全策略和 interceptor 的兼容普通路由使用。
//   produces/consumes 由每路由中间件执行；auth 永远位于 decrypt 前，不能由 order 反转。
//
// 为什么要 mvc_router! 这个宏？—— linkme 的收集数组是【单态】的(static 不能泛型)，而 axum 的
//   Router<S> 的状态类型 S 因项目而异。所以由你用 mvc_router!(你的State) 现场生成一份按 S 单态化的收集器，
//   注解再统一往 crate::__mvc::ROUTES 里塞。这样既支持任意 State，又不需要请求期服务定位器。
// ============================================================================

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, FnArg, GenericArgument, ItemFn, LitBool, LitInt, LitStr, Path,
    PathArguments, Type,
};

/// 业务作用：声明一个可被 mapping 编排器类型化引用的 Axum 风格拦截器。
///
/// # 属性参数
///
/// - `id = "..."`：必填。长度为 1..=64 字节的稳定 ID；同一路由的 effective plan 内必须唯一，
///   推荐在整个应用内保持唯一以便诊断和依赖引用；
/// - `kind = "..."`：可选。固定阶段，只允许 `edge`、`auth`、`plaintext`，默认 `edge`；
/// - `order = ...`：可选。同阶段、同作用域内数值越小越先执行，默认 `0`；
/// - `before = "a,b"` / `after = "c,d"`：可选。逗号分隔的 ID，只能依赖同阶段、
///   同作用域内的 interceptor；
/// - `auth_runtime = true|false`：可选。是否调用共享 `AuthRuntime`，只能用于
///   `kind = "auth"`，默认 `false`；
/// - `global = true|false`：可选。是否由框架自动覆盖当前 crate 中符合阶段合同的全部
///   `*_mapping` 端点，默认 `false`。
///
/// # 装配方式
///
/// `global` 缺省或为 `false` 时，宏只声明 interceptor 并生成类型化 binding helper，不会自动执行。
/// 业务必须选择一种手动装配方式：在某个路由的 `interceptors(...)` 中精确绑定、通过
/// `MappingPlan::scope` 绑定路径层级，或通过 `MappingPlan::global` 手动覆盖全部 mapping 端点。
///
/// `global = true` 时，宏还会把 binding 写入 `mvc_router!` 生成的链接期收集表。
/// `try_register_all` 在监听端口前自动合并、稳定排序并审计这些 binding，业务不需要再在
/// `main`/`configure_mapping` 中登记它。若又手动装配相同 ID，重叠路由会因重复 ID 拒绝启动，
/// 不会静默去重，也不会执行两次。
///
/// 自动 global 只覆盖注解路由，不覆盖手写 Axum Router 路由和框架探针。它要求当前 crate 已调用
/// `mvc_router!`，并且函数无 State 或使用与 Router 根 State 相同的 `State<T>`；需要业务构造窄
/// State、scope、`when_route` 或动态配置开关时，应保持 `global = false` 并手动 `binding_with`。
/// `kind = "auth"` 的自动项仍服从路由身份合同：只参与 `auth = "required|optional"` 的路由，
/// public 或未声明 auth 的路由会在 effective-plan 阶段排除它。
///
/// # 函数签名
///
/// 被标注项必须是非泛型 `async fn`。最后两个参数必须依次为 `Request, Next`；它们之前可以使用
/// `State<T>`、`Extension<T>`、`InterceptorContext` 等 `FromRequestParts` extractor，但不能使用
/// `Json`、`Form`、另一份 `Request` 等会消费 Body 的 extractor。
///
/// # 手动装配示例（默认行为）
///
/// ```ignore
/// #[interceptor(id = "request-audit", kind = "edge", order = 10)]
/// async fn request_audit(request: Request, next: Next) -> Response {
///     audit(request, next).await
/// }
///
/// // napp Application 启动 Hook：只在这里显式装配后才会执行。
/// app.configure_mapping(|plan| Ok(plan.global(request_audit::binding())))?;
/// ```
///
/// # 自动装配示例
///
/// ```ignore
/// #[interceptor(id = "automatic-audit", kind = "edge", order = 10, global = true)]
/// async fn automatic_audit(request: Request, next: Next) -> Response {
///     let mut response = next.run(request).await;
///     response.headers_mut().insert("x-audited", "1".parse().unwrap());
///     response
/// }
///
/// // 不要再调用 plan.global(automatic_audit::binding())；框架会自动装配。
/// ```
#[proc_macro_attribute]
pub fn interceptor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let Some(mapping_p) = web_runtime_path() else {
        return syn::Error::new(
            Span::call_site(),
            "#[interceptor] 必须通过 naweb 或 nasa(feature = \"web\") 运行时门面启用",
        )
        .to_compile_error()
        .into();
    };
    let (axum_p, linkme_p, _) = third_party_paths();
    let mut id: Option<LitStr> = None;
    let mut kind = LitStr::new("edge", Span::call_site());
    let mut order = 0_i32;
    let mut before = Vec::<LitStr>::new();
    let mut after = Vec::<LitStr>::new();
    let mut auth_runtime = false;
    let mut global = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("id") {
            id = Some(meta.value()?.parse::<LitStr>()?);
        } else if meta.path.is_ident("kind") {
            kind = meta.value()?.parse::<LitStr>()?;
        } else if meta.path.is_ident("order") {
            let value = meta.value()?.parse::<LitInt>()?;
            order = value.base10_parse::<i32>()?;
        } else if meta.path.is_ident("before") {
            let value = meta.value()?.parse::<LitStr>()?;
            before = parse_dependency_ids(&value)?;
        } else if meta.path.is_ident("after") {
            let value = meta.value()?.parse::<LitStr>()?;
            after = parse_dependency_ids(&value)?;
        } else if meta.path.is_ident("auth_runtime") {
            auth_runtime = meta.value()?.parse::<LitBool>()?.value;
        } else if meta.path.is_ident("global") {
            global = meta.value()?.parse::<LitBool>()?.value;
        } else {
            return Err(
                meta.error("#[interceptor] 只支持 id/kind/order/before/after/auth_runtime/global")
            );
        }
        Ok(())
    });
    parse_macro_input!(attr with parser);
    let function = parse_macro_input!(item as ItemFn);
    if let Err(error) = validate_interceptor_function(&function) {
        return error.to_compile_error().into();
    }
    let Some(id) = id else {
        return syn::Error::new_spanned(&function.sig.ident, "#[interceptor] 必须声明稳定 id")
            .to_compile_error()
            .into();
    };
    if !valid_policy_id(&id.value()) {
        return syn::Error::new_spanned(id, "interceptor id 必须是 1..=64 字节安全 ASCII 标识")
            .to_compile_error()
            .into();
    }
    let kind_value = kind.value();
    let stage = match kind_value.as_str() {
        "edge" => quote!(#mapping_p::InterceptorStage::Edge),
        "auth" => quote!(#mapping_p::InterceptorStage::Auth),
        "plaintext" => quote!(#mapping_p::InterceptorStage::Plaintext),
        _ => {
            return syn::Error::new_spanned(kind, "interceptor kind 只允许 edge/auth/plaintext")
                .to_compile_error()
                .into()
        }
    };
    if auth_runtime && kind_value != "auth" {
        return syn::Error::new_spanned(
            kind,
            "auth_runtime = true 只能用于 kind = \"auth\" 拦截器",
        )
        .to_compile_error()
        .into();
    }
    let ident = function.sig.ident.clone();
    let visibility = function.vis.clone();
    let state_ty = interceptor_state_type(&function);
    let global_state_ty = state_ty.clone();
    let binding_helper = if let Some(state_ty) = state_ty {
        quote! {
            impl #ident {
                /// 业务作用：建立“拦截器 State 与 Router 根 State 相同”的常用 binding。
                ///
                /// napp 应用通常使用 `State<Application>`，非 napp 应用则使用自己的 AppState；
                /// mapping 只 clone 启动时传入的同一根状态，不建立全局容器。
                #visibility fn binding() -> #mapping_p::InterceptorBinding<#state_ty> {
                    #mapping_p::InterceptorBinding::new(
                        <Self as #mapping_p::InterceptorDefinition>::DESCRIPTOR,
                        |__route: #axum_p::routing::MethodRouter<#state_ty>, __state: &#state_ty| {
                            __route.layer(#axum_p::middleware::from_fn_with_state(
                                __state.clone(),
                                #ident,
                            ))
                        },
                    )
                }

                /// 业务作用：把启动期已经构造好的窄 State 绑定到另一种 Router 根 State。
                ///
                /// 例如 Router 使用 napp `Application`，高频 Token interceptor 只持有 Redis
                /// handle 与配置快照。窄 State 只在装配期 clone，受管资源的关闭权仍归根容器。
                #visibility fn binding_with<__RootState>(
                    __interceptor_state: #state_ty,
                ) -> #mapping_p::InterceptorBinding<__RootState>
                where
                    __RootState: ::core::clone::Clone + ::core::marker::Send + ::core::marker::Sync + 'static,
                    #state_ty: ::core::clone::Clone + ::core::marker::Send + ::core::marker::Sync + 'static,
                {
                    #mapping_p::InterceptorBinding::new(
                        <Self as #mapping_p::InterceptorDefinition>::DESCRIPTOR,
                        move |__route: #axum_p::routing::MethodRouter<__RootState>, _root: &__RootState| {
                            __route.layer(#axum_p::middleware::from_fn_with_state(
                                __interceptor_state.clone(),
                                #ident,
                            ))
                        },
                    )
                }
            }
        }
    } else {
        quote! {
            impl #ident {
                /// 业务作用：建立不依赖 `State<_>` extractor 的可复用 binding。
                ///
                /// Router 根 State 由调用位置推断；Header、Extension、InterceptorContext 等
                /// `FromRequestParts` extractor 仍可照常使用。
                #visibility fn binding<__RootState>() -> #mapping_p::InterceptorBinding<__RootState>
                where
                    __RootState: ::core::clone::Clone + ::core::marker::Send + ::core::marker::Sync + 'static,
                {
                    #mapping_p::InterceptorBinding::new(
                        <Self as #mapping_p::InterceptorDefinition>::DESCRIPTOR,
                        |__route: #axum_p::routing::MethodRouter<__RootState>, _root: &__RootState| {
                            __route.layer(#axum_p::middleware::from_fn(#ident))
                        },
                    )
                }
            }
        }
    };
    let global_registration = if global {
        let registration = format_ident!("__mapping_global_interceptor_{}", ident);
        let binding = if global_state_ty.is_some() {
            quote! {
                #ident::binding()
            }
        } else {
            quote! {
                #ident::binding::<crate::__mvc::RouterState>()
            }
        };
        quote! {
            #[#linkme_p::distributed_slice(crate::__mvc::GLOBAL_INTERCEPTORS)]
            #[linkme(crate = #linkme_p)]
            #[allow(non_upper_case_globals)]
            static #registration: crate::__mvc::GlobalInterceptorEntry =
                crate::__mvc::GlobalInterceptorEntry {
                    descriptor: <#ident as #mapping_p::InterceptorDefinition>::DESCRIPTOR,
                    binding: || #binding,
                };
        }
    } else {
        quote! {}
    };
    quote! {
        #function

        #[allow(non_camel_case_types)]
        #visibility struct #ident {
            __mapping_private: (),
        }

        impl #mapping_p::InterceptorDefinition for #ident {
            const DESCRIPTOR: #mapping_p::InterceptorDescriptor = #mapping_p::InterceptorDescriptor {
                id: #id,
                stage: #stage,
                order: #order,
                before: &[#(#before),*],
                after: &[#(#after),*],
                handler: concat!(module_path!(), "::", stringify!(#ident)),
                source_file: file!(),
                source_line: line!(),
                auth_runtime: #auth_runtime,
            };
        }

        #binding_helper

        #global_registration
    }
    .into()
}

/// 业务作用：解析逗号分隔的 before/after ID，并对每项执行安全标识校验。
fn parse_dependency_ids(value: &LitStr) -> syn::Result<Vec<LitStr>> {
    let mut result = Vec::new();
    for item in value.value().split(',') {
        let item = item.trim();
        if !valid_policy_id(item) {
            return Err(syn::Error::new_spanned(
                value,
                "before/after 必须是逗号分隔的安全 interceptor ID",
            ));
        }
        result.push(LitStr::new(item, value.span()));
    }
    Ok(result)
}

/// 业务作用：校验 interceptor 必须是非泛型 async 自由函数，且 Request/Next 位于末尾。
fn validate_interceptor_function(function: &ItemFn) -> syn::Result<()> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "#[interceptor] 只能标注 async fn",
        ));
    }
    if !function.sig.generics.params.is_empty() || function.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &function.sig.generics,
            "#[interceptor] 不支持泛型函数",
        ));
    }
    let inputs = function.sig.inputs.iter().collect::<Vec<_>>();
    if inputs
        .iter()
        .any(|input| matches!(input, FnArg::Receiver(_)))
    {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "#[interceptor] 不支持 self receiver",
        ));
    }
    if inputs.len() < 2
        || fn_arg_last_type(inputs[inputs.len() - 2]).as_deref() != Some("Request")
        || fn_arg_last_type(inputs[inputs.len() - 1]).as_deref() != Some("Next")
    {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "interceptor 最后两个参数必须依次是 Request、Next",
        ));
    }
    for input in &inputs[..inputs.len() - 2] {
        if matches!(
            fn_arg_last_type(input).as_deref(),
            Some("Json" | "Form" | "Request")
        ) {
            return Err(syn::Error::new_spanned(
                input,
                "Request 之前只能使用 FromRequestParts extractor，禁止消费 Body",
            ));
        }
    }
    Ok(())
}

/// 业务作用：返回一个 typed 参数路径的最后类型名，供 extractor 形状静态审计。
fn fn_arg_last_type(input: &FnArg) -> Option<String> {
    let FnArg::Typed(input) = input else {
        return None;
    };
    let Type::Path(path) = input.ty.as_ref() else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

/// 业务作用：从 interceptor 参数中提取 `State<T>` 的 T，供生成单态化 binding helper。
fn interceptor_state_type(function: &ItemFn) -> Option<Type> {
    for input in &function.sig.inputs {
        let FnArg::Typed(input) = input else {
            continue;
        };
        let Type::Path(path) = input.ty.as_ref() else {
            continue;
        };
        let segment = path.path.segments.last()?;
        if segment.ident != "State" {
            continue;
        }
        let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
            continue;
        };
        let Some(GenericArgument::Type(state)) = arguments.args.first() else {
            continue;
        };
        return Some(state.clone());
    }
    None
}

// ════════════════════════════════════════════════════════════════════════════
// 一、收集器生成宏：mvc_router!(StateType)
//   在 crate 根调用一次，生成 `pub mod __mvc` 的收集表、审计入口与装配入口。
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：# `mvc_router!(StateType)` —— 生成路由收集模块 `crate::__mvc`
///
/// 在 crate 根（`main.rs` / `lib.rs`）调用一次，按 Axum 根 State 类型生成 `RouteEntry`、
/// `ROUTES`、`GLOBAL_INTERCEPTORS`、`try_register_all` 与兼容 `register_all`。
/// `#[*_mapping]` 和 `#[interceptor(global = true)]` 会把静态注册项写入这些链接期集合。
///
/// 只要任一路由声明 auth、crypto 或 interceptor，就必须使用 `try_register_all` 并传入同一份
/// `MappingRuntime`、`MappingPlan` 和根 State。它会先冻结 effective plan、检查阶段顺序与所有
/// provider/condition/key/replay 依赖，再装配 Router；失败发生在监听端口之前。
///
/// ## 参数
/// | 参数 | 类型 | 必填 | 含义 |
/// |---|---|---|---|
/// | (位置参) | 类型 | ✅ | axum 的 State 类型,如 `crate::state::AppState`;无状态写 `()` |
///
/// ## 用法示例
/// ```ignore
/// naweb_macro::mvc_router!(crate::state::AppState);   // 有状态
/// naweb_macro::mvc_router!(());                       // 无状态
/// // 只有普通路由的兼容装配：
/// let app = Router::new().merge(crate::__mvc::register_all(Router::new())).with_state(state);
/// // auth/crypto/interceptor 路由：
/// let app = crate::__mvc::try_register_all(
///     Router::new(),
///     runtime,
///     MappingPlan::new(),
///     state,
/// )?;
/// ```
///
/// # 参数
///
/// - `input`: 宏调用括号内的 axum State 类型 token stream，例如 `crate::state::AppState` 或 `()`。
#[proc_macro]
pub fn mvc_router(input: TokenStream) -> TokenStream {
    let (axum_p, linkme_p, tracing_p) = third_party_paths();
    // 解析括号里的状态类型(如 crate::state::AppState 或 ())。
    let state_ty = parse_macro_input!(input as Type);
    let Some(mapping_p) = web_runtime_path() else {
        let expanded = quote! {
            /// 自动生成的兼容路由收集模块。
            pub mod __mvc {
                use #axum_p::Router;

                /// `mvc_router!` 单态化后的应用根状态类型。
                pub type RouterState = #state_ty;

                /// 一条不含安全策略的兼容路由注册项。
                pub struct RouteEntry {
                    /// 大写 HTTP 方法。
                    pub method: &'static str,
                    /// 已规范化的路由路径。
                    pub path: &'static str,
                    /// 业务处理函数的稳定模块路径。
                    pub handler: &'static str,
                    /// 可选响应 Content-Type。
                    pub produces: ::core::option::Option<&'static str>,
                    /// 可选请求 Content-Type 约束。
                    pub consumes: ::core::option::Option<&'static str>,
                    /// 可选请求 DTO schema 工厂；兼容模式不支持该元数据。
                    pub request_schema: ::core::option::Option<fn() -> (&'static str, &'static str)>,
                    /// 可选响应 DTO schema 工厂；兼容模式不支持该元数据。
                    pub response_schema: ::core::option::Option<fn() -> (&'static str, &'static str)>,
                    /// 把当前路由追加到指定状态 Router 的单态化函数。
                    pub register: fn(Router<#state_ty>) -> Router<#state_ty>,
                }

                /// 链接期收集的全部兼容路由。
                #[#linkme_p::distributed_slice]
                #[linkme(crate = #linkme_p)]
                pub static ROUTES: [RouteEntry];

                /// 业务作用：装配不使用 mapping 运行时的兼容路由。
                ///
                /// # 参数
                ///
                /// - `router`: 待追加注解路由的 Axum Router。
                ///
                /// # 返回
                ///
                /// 返回完成装配的 Router。
                pub fn register_all(mut router: Router<#state_ty>) -> Router<#state_ty> {
                    for entry in ROUTES.iter() {
                        #tracing_p::info!("[mvc] 收集到路由 {:>4} {}", entry.method, entry.path);
                        router = (entry.register)(router);
                    }
                    router
                }
            }
        };
        return expanded.into();
    };

    let expanded = quote! {
        /// 自动生成的路由收集模块（由 naweb_macro::mvc_router! 展开）。
        pub mod __mvc {
            use ::std::sync::Arc;
            use #axum_p::Router;

            /// `mvc_router!` 单态化后的应用根状态类型。
            pub type RouterState = #state_ty;

            /// 一条被 #[*_mapping] 注解的路由的注册项（字段全 'static，可放进 static 被 linkme 收集）。
            pub struct RouteEntry {
                /// HTTP 方法（例如 `GET`/`POST`），用于装配日志与冲突诊断。
                pub method: &'static str,
                /// 已规范化的 axum 路由路径。
                pub path: &'static str,
                /// 业务处理函数的稳定模块路径，用于定位重复注册。
                pub handler: &'static str,
                /// 响应 Content-Type；实际响应头由生成的路由中间件设置。
                pub produces: ::core::option::Option<&'static str>,
                /// 请求 Content-Type 约束；实际校验由生成的路由中间件执行。
                pub consumes: ::core::option::Option<&'static str>,
                /// 显式请求 DTO schema 工厂。
                pub request_schema: ::core::option::Option<#mapping_p::ApiSchemaFactory>,
                /// 显式响应 DTO schema 工厂。
                pub response_schema: ::core::option::Option<#mapping_p::ApiSchemaFactory>,
                /// 显式 query 参数集工厂。
                pub query_parameters: ::core::option::Option<#mapping_p::ApiParametersFactory>,
                /// 显式 header 参数集工厂。
                pub header_parameters: ::core::option::Option<#mapping_p::ApiParametersFactory>,
                /// 主要成功 HTTP 状态码。
                pub success_status: u16,
                /// 成功响应之外的额外响应集工厂。
                pub additional_responses: ::core::option::Option<#mapping_p::ApiResponsesFactory>,
                /// 是否为流式响应。
                pub streaming: bool,
                /// 当前真实路由的完整静态安全合同。
                pub policy: #mapping_p::RoutePolicy,
                /// 端点属性直接引用的拦截器静态描述符。
                pub interceptors: &'static [#mapping_p::InterceptorDescriptor],
                /// 兼容普通路由的无状态注册函数；安全策略或拦截器存在时不得调用。
                pub register_plain: fn(Router<#state_ty>) -> Router<#state_ty>,
                /// 把当前路由和共享安全运行时追加到指定状态类型 Router 的单态化函数。
                pub register: fn(
                    Router<#state_ty>,
                    Arc<#mapping_p::MappingRuntime>,
                    &#mapping_p::MappingPlan<#state_ty>,
                    &#state_ty,
                ) -> ::core::result::Result<Router<#state_ty>, #mapping_p::MappingBuildError>,
            }

            /// `#[interceptor(global = true)]` 生成的应用级自动装配项。
            ///
            /// 工厂返回类型直接固定为 `InterceptorBinding<RouterState>`，因此无 State interceptor 和
            /// `State<RouterState>` interceptor 都能复用普通 binding 合同；窄 State 会在编译期类型不匹配。
            pub struct GlobalInterceptorEntry {
                /// 用于稳定排序、重复诊断和启动审计的不可变描述符。
                pub descriptor: #mapping_p::InterceptorDescriptor,
                /// 把当前声明单态化成 Router 根 State binding 的无捕获工厂。
                pub binding: fn() -> #mapping_p::InterceptorBinding<RouterState>,
            }

            /// 全局分布式数组：所有注解路由的注册项汇集于此（链接器编译期收集，零运行时扫描）。
            // `#[linkme(crate = ...)]`:让 linkme 自身展开也走解析出的路径(其生成代码
            // 默认硬编码 ::linkme;经门面时业务并不直接依赖 linkme)。
            #[#linkme_p::distributed_slice]
            #[linkme(crate = #linkme_p)]
            pub static ROUTES: [RouteEntry];

            /// 当前 crate 中显式声明 `global = true` 的 interceptor 收集表。
            ///
            /// 链接顺序不属于稳定合同；`try_register_all` 会先按 stage/order/id/handler 排序，再写入计划。
            #[#linkme_p::distributed_slice]
            #[linkme(crate = #linkme_p)]
            pub static GLOBAL_INTERCEPTORS: [GlobalInterceptorEntry];

            /// 业务作用：审计完整路由集合后逐条装配，任何安全依赖缺失都在监听端口前返回错误。
            ///
            /// # 参数
            ///
            /// - `router`: 待追加注解路由的 axum Router。
            /// - `runtime`: 已完成配置合并和组件构建的共享安全运行时。
            ///
            /// # 返回
            ///
            /// 审计成功返回完成装配的 Router，失败返回不含 secret 的启动错误。
            pub fn try_register_all(
                mut router: Router<#state_ty>,
                runtime: Arc<#mapping_p::MappingRuntime>,
                mut plan: #mapping_p::MappingPlan<#state_ty>,
                state: #state_ty,
            ) -> ::core::result::Result<Router<#state_ty>, #mapping_p::MappingBuildError> {
                // linkme 不承诺链接顺序；先稳定排序再追加，保证同 order 自动全局 interceptor 的
                // effective plan 不随编译器、平台或链接参数漂移。手动 plan 已先构造，二者若 ID
                // 在任一路由重叠，后续 per-route 审计会明确报重复而不是静默去重或执行两次。
                let mut global_interceptors = GLOBAL_INTERCEPTORS
                    .iter()
                    .collect::<::std::vec::Vec<_>>();
                global_interceptors.sort_by_key(|entry| {
                    let stage = match entry.descriptor.stage {
                        #mapping_p::InterceptorStage::Edge => 0_u8,
                        #mapping_p::InterceptorStage::Auth => 1_u8,
                        #mapping_p::InterceptorStage::Plaintext => 2_u8,
                    };
                    (
                        stage,
                        entry.descriptor.order,
                        entry.descriptor.id,
                        entry.descriptor.handler,
                    )
                });
                for entry in global_interceptors.iter().copied() {
                    let binding = (entry.binding)();
                    if binding.descriptor() != entry.descriptor {
                        return Err(#mapping_p::MappingBuildError::new(::std::format!(
                            "自动全局 interceptor {} 的 binding 描述符不一致",
                            entry.descriptor.id,
                        )));
                    }
                    plan = plan.global(binding);
                }
                // MappingPlan 可由 napp 持有 runtime；必须先确认审计、上下文与 endpoint 使用同一实例。
                plan.validate_runtime(&runtime)?;
                let mut entries = ROUTES.iter().collect::<::std::vec::Vec<_>>();
                entries.sort_by_key(|entry| (entry.method, entry.path, entry.handler));
                let route_plans = entries
                    .iter()
                    .map(|entry| {
                        let interceptors = plan.audit_route(entry.policy, entry.interceptors)?;
                        Ok(#mapping_p::RouteSecurityPlan {
                            policy: entry.policy,
                            auth_interceptor: interceptors.has_auth(),
                            auth_runtime: interceptors.requires_auth_runtime(),
                        })
                    })
                    .collect::<::core::result::Result<::std::vec::Vec<_>, #mapping_p::MappingBuildError>>()?;
                let audit = runtime.audit_route_plans(&route_plans)?;
                for entry in entries {
                    let __extra = match (entry.produces, entry.consumes) {
                        (Some(p), Some(c)) => ::std::format!(" produces={} consumes={}", p, c),
                        (Some(p), None)    => ::std::format!(" produces={}", p),
                        (None, Some(c))    => ::std::format!(" consumes={}", c),
                        (None, None)       => ::std::string::String::new(),
                    };
                    #tracing_p::info!("[mvc] 收集到路由 {:>4} {}{}", entry.method, entry.path, __extra);
                    router = (entry.register)(router, runtime.clone(), &plan, &state)?;
                }
                #tracing_p::info!(
                    "[mvc] 共自动装配 {} 条 MVC 路由，generation={} fingerprint={}",
                    audit.route_count,
                    audit.generation,
                    audit.fingerprint,
                );
                Ok(router)
            }

            /// 业务作用：仅供没有任何身份和密码策略的旧应用装配普通路由。
            ///
            /// # 参数
            ///
            /// - `router`: 待追加普通注解路由的 Axum Router。
            ///
            /// # 返回
            ///
            /// 返回完成装配的 Router；发现任何安全元数据时立即拒绝，避免静默忽略策略。
            #[deprecated(note = "安全路由必须使用 try_register_all 并注入 MappingRuntime")]
            pub fn register_all(router: Router<#state_ty>) -> Router<#state_ty> {
                if !GLOBAL_INTERCEPTORS.is_empty()
                    || ROUTES
                    .iter()
                    .any(|entry| entry.policy.has_security() || !entry.interceptors.is_empty())
                {
                    panic!("register_all 不能装配含 auth/crypto/interceptor 策略或自动全局 interceptor 的路由");
                }
                let mut router = router;
                let mut entries = ROUTES.iter().collect::<::std::vec::Vec<_>>();
                entries.sort_by_key(|entry| (entry.method, entry.path, entry.handler));
                for entry in entries {
                    router = (entry.register_plain)(router);
                }
                router
            }
        }
    };
    expanded.into()
}

// ════════════════════════════════════════════════════════════════════════════
// 二、路由注解：#[get_mapping(...)] / #[post_mapping(...)] / #[put_mapping(...)] / #[delete_mapping(...)] / #[patch_mapping(...)]
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：# `#[get_mapping]` —— 注册一条 GET 服务端路由
///
/// 贴在 axum handler 上（不改写函数体），把路由、身份、密码与 interceptor 元数据收进
/// `crate::__mvc::ROUTES`。应用必须先在 crate 根调用 `mvc_router!(State)`，再用
/// `try_register_all` 注入 `MappingRuntime` 和 `MappingPlan`；
/// 声明了安全参数的路由不会退回旧的无审计 `register_all`。
///
/// ## 完整属性
///
/// 五个 `*_mapping` 宏共享下列属性；HTTP 方法的差异见各自文档。
///
/// | 参数 | 类型 | 必填 | 含义 |
/// |---|---|---|---|
/// | `path` / `value` | 字符串 | 是 | 相对 context path 的路由路径；单字符串写法是 `path` 的简写 |
/// | `produces` | 字符串 | 否 | 强制响应 `Content-Type` |
/// | `consumes` | 字符串 | 否 | 要求请求 `Content-Type` 以该值开头，否则返回 415 |
/// | `request_schema` | Rust 类型路径 | 否 | 显式请求 DTO JSON Schema；类型须实现 `ApiSchema` |
/// | `response_schema` | Rust 类型路径 | 否 | 显式响应 DTO JSON Schema；类型须实现 `ApiSchema` |
/// | `query_parameters` | Rust 类型路径 | 否 | 显式 query 参数集；类型须实现 `ApiParameters` |
/// | `header_parameters` | Rust 类型路径 | 否 | 显式 header 参数集；类型须实现 `ApiParameters` |
/// | `success_status` | `u16` | 否 | 主要成功状态码，默认 200，只允许 200..=399 |
/// | `responses` | Rust 类型路径 | 否 | 额外响应集；类型须实现 `ApiResponses` |
/// | `streaming` | 布尔值 | 否 | 标记流式响应；必须显式声明 `produces` |
/// | `auth` | `public / optional / required` | 否 | 声明静态身份策略 |
/// | `auth_provider` | 静态 ID | 否 | 选择 `AuthRuntime` provider；省略时使用默认 provider |
/// | `auth_condition` | 静态 ID | 否 | 选择已注册的请求级身份条件 |
/// | `interceptors(...)` | Rust 路径列表 | 否 | 挂载由 [`interceptor`] 生成的 endpoint interceptor |
/// | `decrypt` / `encrypt` | 布尔值 | 否 | 开启请求解密或响应加密，默认均为 `false` |
/// | `crypto_protocol` | `modern-v2 / legacy-v1` | 启用密码方向时是 | 选择线协议 |
/// | `crypto_provider` | 静态 ID | 启用密码方向时是 | 选择密码实现 |
/// | `crypto_key_scope` | 静态 ID | 启用密码方向时是 | 选择受审计密钥域 |
/// | `crypto_condition` | 静态 ID | 否 | 选择已注册的受控密码条件 |
/// | `replay` | `required / disabled` | 否 | modern-v2 写请求默认 `required`，其它情况默认 `disabled` |
/// | `audience` | 字符串 | 否 | 显式固定 AAD 业务受众；省略时由包名、方法和路径生成 |
/// | `error_profile` | `http-standard / fore-rest-legacy` | 否 | 选择非 public 身份失败响应合同 |
/// | `response_contract` | `"base-response-v1"` | legacy-v1 响应加密时是 | 声明可被 legacy 加密的响应结构 |
///
/// 静态 ID 必须是 1..=64 字节安全 ASCII 标识，只能包含字母、数字、点、横线和下划线。
/// Header、Token、密钥或动态路径值不得写进这些 ID。
///
/// ## 固定安全顺序
///
/// effective plan 的入站顺序固定为
/// `edge -> auth -> AuthContext gate -> decrypt/replay -> plaintext -> handler`，出站反向执行。
/// 因此 **auth 永远早于请求解密**，`order`、scope 和 endpoint binding 都不能反转阶段。
/// `auth = "public"` 会在请求级判断前移除全部 auth interceptor，保证不访问身份后端；
/// `auth = "required"` 则必须在 handler 前建立 `AuthContext`。
///
/// 普通 auth interceptor 不能与同一路由的 `auth_provider/auth_condition` 混用。需要由一个全局业务宏
/// 复用可热更新 AuthRuntime 时，应在该 `#[interceptor]` 上声明 `auth_runtime = true`；端点仍只声明
/// `auth`，必要时再声明 `auth_condition`。
///
/// ## 密码约束
///
/// - GET 不允许 `decrypt = true`，但可认证，也可执行 legacy-v1 响应加密。
/// - modern-v2 响应复用请求的 rid 与 key 快照，因此 `encrypt = true` 必须同时启用 `decrypt = true`。
/// - modern-v2 自动使用 `application/vnd.nasa.crypto+json;v=2`，显式媒体类型必须与它完全一致。
/// - legacy-v1 没有 rid，不能启用 required replay；响应加密必须声明
///   `response_contract = "base-response-v1"`。
/// - 加密路由拒绝 multipart、事件流、WebSocket、文件和流式媒体类型。
///
/// ## 用法示例
/// ```ignore
/// #[get_mapping("/spot/kline")]
/// pub async fn public_kline() -> Json<Value> { /* ... */ }
///
/// #[get_mapping(
///     path = "/account/profile",
///     auth = "required",
///     auth_condition = "fore-whitelist",
///     interceptors(audit_interceptor)
/// )]
/// pub async fn handler(/* ... */) -> Json</* ... */> { /* ... */ }
/// ```
///
/// # 参数
///
/// - `attr`: `#[get_mapping(...)]` 括号内的完整路由、安全与 interceptor 配置 token stream。
/// - `item`: 被注解的 handler 函数 token stream。
#[proc_macro_attribute]
pub fn get_mapping(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, "GET", "get")
}

/// 业务作用：# `#[post_mapping]` —— 注册一条 POST 服务端路由
///
/// 支持 [`get_mapping`] 文档列出的全部路由、安全与 `interceptors(...)` 属性。
/// ★ **唯一区别**:不写 `consumes` 时【默认 `"application/json"`】(POST 通常吃 JSON body);
///   要收表单就显式写 `consumes = "application/x-www-form-urlencoded"`。
///
/// ⚠ **注意:这是本框架的自定义约定,不是同类注解的通用语义**。常见服务端 POST 注解本身
///   通常【不】默认要求 JSON;此默认仅为对齐既有项目而保留(无 body / 表单 / multipart 会被 415 拦下)。
///   故 `#[put_mapping]`/`#[patch_mapping]` 【刻意不继承】该默认,需要时各自显式写 `consumes`。
///
/// ## 用法示例
/// ```ignore
/// #[post_mapping("/order")] // 默认 consumes = application/json
/// #[post_mapping(path = "/form", consumes = "application/x-www-form-urlencoded")]
/// #[post_mapping(
///     path = "/secure/order",
///     auth = "required",
///     decrypt = true,
///     encrypt = true,
///     crypto_protocol = "modern-v2",
///     crypto_provider = "modern-aead",
///     crypto_key_scope = "order"
/// )]
/// pub async fn handler(/* ... */) -> Json</* ... */> { /* ... */ }
/// ```
///
/// # 参数
///
/// - `attr`: `#[post_mapping(...)]` 括号内的完整路由、安全与 interceptor 配置 token stream。
/// - `item`: 被注解的 handler 函数 token stream。
#[proc_macro_attribute]
pub fn post_mapping(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, "POST", "post")
}

/// 业务作用：# `#[put_mapping]` —— 注册一条 PUT 服务端路由
///
/// 支持 [`get_mapping`] 文档列出的全部路由、安全与 `interceptors(...)` 属性。
/// ★ 与 `#[post_mapping]` 不同:**不写 `consumes` 时不默认任何值**(只有 POST 保留历史默认
///   `application/json`);要强制请求体类型就显式写 `consumes = "application/json"`。
///
/// # 参数
///
/// - `attr`: `#[put_mapping(...)]` 括号内的完整路由、安全与 interceptor 配置 token stream。
/// - `item`: 被注解的 handler 函数 token stream。
#[proc_macro_attribute]
pub fn put_mapping(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, "PUT", "put")
}

/// 业务作用：# `#[delete_mapping]` —— 注册一条 DELETE 服务端路由
///
/// 支持 [`get_mapping`] 文档列出的全部路由、安全与 `interceptors(...)` 属性。
/// 不默认 `consumes`(DELETE 通常无 body)。
///
/// # 参数
///
/// - `attr`: `#[delete_mapping(...)]` 括号内的完整路由、安全与 interceptor 配置 token stream。
/// - `item`: 被注解的 handler 函数 token stream。
#[proc_macro_attribute]
pub fn delete_mapping(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, "DELETE", "delete")
}

/// 业务作用：# `#[patch_mapping]` —— 注册一条 PATCH 服务端路由
///
/// 支持 [`get_mapping`] 文档列出的全部路由、安全与 `interceptors(...)` 属性。
/// 不默认 `consumes`;要强制请求体类型就显式写 `consumes = "application/json"`。
///
/// # 参数
///
/// - `attr`: `#[patch_mapping(...)]` 括号内的完整路由、安全与 interceptor 配置 token stream。
/// - `item`: 被注解的 handler 函数 token stream。
#[proc_macro_attribute]
pub fn patch_mapping(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, "PATCH", "patch")
}

/// 业务作用：get_mapping / post_mapping / put_mapping / delete_mapping / patch_mapping 的公共实现。
///   attr   = 注解括号内容：单串(= path) 或 key=value 列表
///   method = "GET"/"POST"/"PUT"/"DELETE"/"PATCH"（存注册项 + 日志）；
///   verb   = "get"/"post"/"put"/"delete"/"patch"（生成 ::axum::routing::<verb>）
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
/// - `item`: 被宏处理的 Rust item token stream。
/// - `method`: trait 方法 AST 或 HTTP 方法。
/// - `verb`: HTTP 方法字符串。
fn expand(attr: TokenStream, item: TokenStream, method: &str, verb: &str) -> TokenStream {
    let (axum_p, linkme_p, _tracing_p) = third_party_paths();
    let runtime_p = web_runtime_path();
    // method 与 verb 分开传入：
    // - method 是路由清单和日志里的 HTTP 动词字符串；
    // - verb 是 axum::routing 下的函数名，必须是小写标识符。
    // ── ① 解析完整注解参数（path 必填；媒体类型、auth、crypto、interceptor 可选）──
    let mut path: Option<String> = None;
    let mut produces: Option<String> = None;
    let mut consumes: Option<String> = None;
    let mut request_schema: Option<Path> = None;
    let mut response_schema: Option<Path> = None;
    let mut query_parameters: Option<Path> = None;
    let mut header_parameters: Option<Path> = None;
    let mut additional_responses: Option<Path> = None;
    let mut success_status: u16 = 200;
    let mut streaming = false;
    let mut auth: Option<String> = None;
    let mut auth_provider: Option<String> = None;
    let mut auth_condition: Option<String> = None;
    let mut decrypt = false;
    let mut encrypt = false;
    let mut crypto_protocol: Option<String> = None;
    let mut crypto_provider: Option<String> = None;
    let mut crypto_key_scope: Option<String> = None;
    let mut crypto_condition: Option<String> = None;
    let mut replay: Option<String> = None;
    let mut error_profile: Option<String> = None;
    let mut audience: Option<String> = None;
    let mut response_contract: Option<String> = None;
    let mut interceptors = Vec::<Path>::new();

    // 先试【单串简写】：整段就是一个字符串字面量(如 "/x") → 当 path。
    // 解析失败再走 key=value 语法，兼容 `path = "/x"` 与 `value = "/x"` 两种写法。
    if let Ok(lit) = syn::parse::<LitStr>(attr.clone()) {
        path = Some(lit.value());
    } else {
        // 否则按 key=value 列表解析。
        let parser = syn::meta::parser(|meta| {
            if meta.path.is_ident("path") || meta.path.is_ident("value") {
                path = Some(meta.value()?.parse::<LitStr>()?.value()); // path 与 value 同义(对齐 原框架)
            } else if meta.path.is_ident("produces") {
                produces = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("consumes") {
                consumes = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("request_schema") {
                request_schema = Some(meta.value()?.parse::<Path>()?);
            } else if meta.path.is_ident("response_schema") {
                response_schema = Some(meta.value()?.parse::<Path>()?);
            } else if meta.path.is_ident("query_parameters") {
                query_parameters = Some(meta.value()?.parse::<Path>()?);
            } else if meta.path.is_ident("header_parameters") {
                header_parameters = Some(meta.value()?.parse::<Path>()?);
            } else if meta.path.is_ident("success_status") {
                success_status = meta.value()?.parse::<LitInt>()?.base10_parse::<u16>()?;
            } else if meta.path.is_ident("responses") {
                additional_responses = Some(meta.value()?.parse::<Path>()?);
            } else if meta.path.is_ident("streaming") {
                streaming = meta.value()?.parse::<LitBool>()?.value;
            } else if meta.path.is_ident("auth") {
                auth = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("auth_provider") {
                auth_provider = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("auth_condition") {
                auth_condition = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("decrypt") {
                decrypt = meta.value()?.parse::<LitBool>()?.value;
            } else if meta.path.is_ident("encrypt") {
                encrypt = meta.value()?.parse::<LitBool>()?.value;
            } else if meta.path.is_ident("crypto_protocol") {
                crypto_protocol = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("crypto_provider") {
                crypto_provider = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("crypto_key_scope") {
                crypto_key_scope = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("crypto_condition") {
                crypto_condition = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("replay") {
                replay = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("error_profile") {
                error_profile = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("audience") {
                audience = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("response_contract") {
                response_contract = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("interceptors") {
                meta.parse_nested_meta(|nested| {
                    interceptors.push(nested.path.clone());
                    Ok(())
                })?;
            } else {
                return Err(meta.error(format!("#[{verb}_mapping]: 存在不支持的路由参数")));
            }
            Ok(())
        });
        parse_macro_input!(attr with parser);
    }

    let path = match path {
        Some(p) => p,
        None => {
            return syn::Error::new(
                Span::call_site(),
                format!(
                    "#[{verb}_mapping] 必须提供 path，如 #[{verb}_mapping(\"/x\")] 或 #[{verb}_mapping(path = \"/x\")]"
                ),
            )
            .to_compile_error()
            .into();
        }
    };

    // ── path 必须以 '/' 开头(对齐 axum/原框架 路由约定:相对 context-path 的绝对路径)──
    //   否则 axum `.route()` 运行期会 panic("paths must start with a slash"),不如编译期就报错提醒。
    if !path.starts_with('/') {
        return syn::Error::new(
            Span::call_site(),
            format!(
                "#[{verb}_mapping] 的 path 必须以 '/' 开头(当前: {path:?}),如 #[{verb}_mapping(\"/{path}\")]"
            ),
        )
        .to_compile_error()
        .into();
    }

    let consumes_explicit = consumes.is_some();
    let security_declared = auth.is_some()
        || auth_provider.is_some()
        || auth_condition.is_some()
        || decrypt
        || encrypt
        || crypto_protocol.is_some()
        || crypto_provider.is_some()
        || crypto_key_scope.is_some()
        || crypto_condition.is_some()
        || replay.is_some()
        || error_profile.is_some()
        || audience.is_some()
        || response_contract.is_some()
        || !interceptors.is_empty();
    if security_declared && runtime_p.is_none() {
        return attribute_error(
            verb,
            "安全路由参数需要通过 naweb 或 nasa(feature = \"web\") 运行时门面启用",
        );
    }

    if let Some(value) = auth.as_deref() {
        if !matches!(value, "required" | "optional" | "public") {
            return attribute_error(verb, "auth 只允许 required / optional / public");
        }
    }
    for (name, value) in [
        ("auth_provider", &auth_provider),
        ("auth_condition", &auth_condition),
        ("crypto_protocol", &crypto_protocol),
        ("crypto_provider", &crypto_provider),
        ("crypto_key_scope", &crypto_key_scope),
        ("crypto_condition", &crypto_condition),
        ("error_profile", &error_profile),
        ("response_contract", &response_contract),
    ] {
        if value.as_deref().is_some_and(|text| !valid_policy_id(text)) {
            return attribute_error(verb, &format!("{name} 必须是 1..=64 字节安全 ASCII 标识"));
        }
    }
    if audience.as_deref().is_some_and(|value| {
        value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
    }) {
        return attribute_error(verb, "audience 必须是 1..=256 字节且不含控制字符");
    }
    if auth.is_none() && (auth_provider.is_some() || auth_condition.is_some()) {
        return attribute_error(verb, "auth_provider/auth_condition 必须与 auth 同时声明");
    }
    if let Some(profile) = error_profile.as_deref() {
        if auth.is_none() {
            return attribute_error(verb, "error_profile 必须与 auth 同时声明");
        }
        if auth.as_deref() == Some("public") {
            return attribute_error(verb, "public 路由不会认证，禁止声明 error_profile");
        }
        if !matches!(profile, "http-standard" | "fore-rest-legacy") {
            return attribute_error(
                verb,
                "error_profile 只允许 http-standard / fore-rest-legacy",
            );
        }
    }
    if auth.as_deref() == Some("public") && (auth_provider.is_some() || auth_condition.is_some()) {
        return attribute_error(
            verb,
            "public 路由禁止声明 auth_provider/auth_condition，确保请求不会访问身份后端",
        );
    }

    let crypto_enabled = decrypt || encrypt;
    if crypto_enabled {
        if decrypt && !matches!(method, "POST" | "PUT" | "PATCH" | "DELETE") {
            return attribute_error(verb, "请求解密只能用于允许 body 的 HTTP 方法");
        }
        let Some(protocol) = crypto_protocol.as_deref() else {
            return attribute_error(verb, "启用 encrypt/decrypt 时必须声明 crypto_protocol");
        };
        if !matches!(protocol, "legacy-v1" | "modern-v2") {
            return attribute_error(verb, "crypto_protocol 只允许 legacy-v1 / modern-v2");
        }
        if crypto_provider.is_none() || crypto_key_scope.is_none() {
            return attribute_error(
                verb,
                "启用 encrypt/decrypt 时必须声明 crypto_provider 和 crypto_key_scope",
            );
        }
        if protocol == "modern-v2" {
            // modern-v2 响应复用请求建立的 rid 与 key 快照，没有独立响应上下文。
            // 只声明响应加密而不声明请求解密的 modern-v2 路由会在运行期每个请求上 fail closed，
            // 必须在编译期就挡住（对照 endpoint composer 的 modern-response-without-request-context）。
            if encrypt && !decrypt {
                return attribute_error(
                    verb,
                    "modern-v2 响应加密必须同时启用请求解密：响应复用请求的 rid 与 key 快照",
                );
            }
            if response_contract.is_some() {
                return attribute_error(verb, "modern-v2 加密完整响应，禁止声明 response_contract");
            }
            if decrypt {
                if !consumes_explicit {
                    consumes = Some("application/vnd.nasa.crypto+json;v=2".to_string());
                } else if consumes.as_deref() != Some("application/vnd.nasa.crypto+json;v=2") {
                    return attribute_error(verb, "modern-v2 请求必须使用固定 vendor Content-Type");
                }
            }
            if encrypt {
                if produces.is_none() {
                    produces = Some("application/vnd.nasa.crypto+json;v=2".to_string());
                } else if produces.as_deref() != Some("application/vnd.nasa.crypto+json;v=2") {
                    return attribute_error(verb, "modern-v2 响应必须使用固定 vendor Content-Type");
                }
            }
        } else {
            if decrypt && !consumes_explicit {
                consumes = Some("application/json".to_string());
            }
            if !encrypt && response_contract.is_some() {
                return attribute_error(verb, "response_contract 只能用于启用了响应加密的路由");
            }
            if encrypt && response_contract.as_deref() != Some("base-response-v1") {
                return attribute_error(
                    verb,
                    "legacy-v1 响应加密必须声明 response_contract = \"base-response-v1\"",
                );
            }
        }
    } else if crypto_protocol.is_some()
        || crypto_provider.is_some()
        || crypto_key_scope.is_some()
        || crypto_condition.is_some()
        || replay.is_some()
        || audience.is_some()
        || response_contract.is_some()
    {
        return attribute_error(verb, "未启用 encrypt/decrypt 时禁止设置 crypto 路由参数");
    }

    let replay_value = match replay.as_deref() {
        Some("required") => "required",
        Some("disabled") => "disabled",
        Some(_) => return attribute_error(verb, "replay 只允许 required / disabled"),
        None if crypto_protocol.as_deref() == Some("modern-v2")
            && matches!(method, "POST" | "PUT" | "PATCH" | "DELETE") =>
        {
            "required"
        }
        None => "disabled",
    };
    if crypto_protocol.as_deref() == Some("legacy-v1") && replay_value == "required" {
        return attribute_error(verb, "legacy-v1 信封没有 rid，不能启用 required replay");
    }

    // POST 普通路由保留历史 JSON 默认；安全协议路由已在上面按静态协议选择精确媒体类型。
    if method == "POST" && consumes.is_none() {
        consumes = Some("application/json".to_string());
    }
    if request_schema.is_some() && consumes.is_none() {
        return attribute_error(
            verb,
            "request_schema 必须同时声明 consumes（POST 可使用默认 application/json）",
        );
    }
    if !(200..=399).contains(&success_status) {
        return attribute_error(verb, "success_status 只允许 200..=399");
    }
    if streaming && produces.is_none() {
        return attribute_error(verb, "streaming = true 必须显式声明 produces");
    }
    if streaming && crypto_enabled {
        return attribute_error(verb, "加密路由不能声明 streaming = true");
    }

    // produces/consumes 必须非空且不含控制字符，避免生成不可用的 OpenAPI 合同。
    //   produces 走 `HeaderValue::from_static`,非法值会在装配期 panic;consumes 非法则匹配恒不中。
    //   值来自宏字面量(非运行期输入),故在【编译期】就把空串/控制字符挡掉,报错指向注解处,优于启动期 panic。
    for (field, val) in [("produces", &produces), ("consumes", &consumes)] {
        if let Some(v) = val {
            if let Err(e) = validate_media_type(verb, field, v) {
                return e.into();
            }
            if crypto_enabled && forbidden_crypto_media_type(v) {
                return attribute_error(
                    verb,
                    "加密路由不支持 multipart、事件流、WebSocket、文件或流式媒体类型",
                );
            }
        }
    }

    // ── ② 解析被注解函数（只取函数名，本体原样保留，不改写）──
    let func = parse_macro_input!(item as ItemFn);
    // 顺序守卫:执行监控属性必须写在 #[*_mapping] 上方,否则路由属性会被本宏先消费掉,
    // 监控宏读不到真实路由,只能静默退化成函数名(卡片标题和 path 全丢),是纯粹的静默错误。
    // 这里按属性路径末段识别本仓两个监控宏;两者一视同仁,不能只拦一个。
    if let Some(monitor_attr) = func.attrs.iter().find(|attribute| {
        attribute
            .path()
            .segments
            .last()
            .map(|segment| segment.ident == "grafana" || segment.ident == "hystrix")
            .unwrap_or(false)
    }) {
        let name = monitor_attr
            .path()
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default();
        return syn::Error::new_spanned(
            monitor_attr,
            format!("#[{name}] 必须放在 #[*_mapping] 上方，才能读取真实路由"),
        )
        .to_compile_error()
        .into();
    }
    let fn_ident = func.sig.ident.clone();
    let reg_ident = format_ident!("__MVC_ROUTE_{}", fn_ident); // 唯一 static 名（带函数名防撞）
    let mw_ident = format_ident!("__mvc_mw_{}", fn_ident); // 唯一中间件函数名
    let verb_ident = format_ident!("{}", verb);

    // ── ③ produces/consumes → 生成"每路由专属中间件函数" + register 时 .layer(...) ──
    //   字面量直接内联进中间件函数体(不捕获)→ 是普通 async fn，from_fn 直接收。
    let need_mw = produces.is_some() || consumes.is_some();

    // consumes：请求 Content-Type 必须以指定值开头，否则 415（在 handler 之前拦掉）。
    // 用 starts_with 是为了允许 `application/json; charset=utf-8` 这类带参数的媒体类型。
    let internal_consumes = if decrypt {
        Some("application/json".to_string())
    } else {
        consumes.clone()
    };
    let consumes_check = match &internal_consumes {
        Some(c) => quote! {
            let __ok = req
                .headers()
                .get(#axum_p::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.starts_with(#c))
                .unwrap_or(false);
            if !__ok {
                return #axum_p::response::IntoResponse::into_response((
                    #axum_p::http::StatusCode::UNSUPPORTED_MEDIA_TYPE, // 415
                    ::std::format!("[{}] requires Content-Type: {}", #path, #c),
                ));
            }
        },
        None => quote! {},
    };
    // produces：handler 返回后强制写响应头 Content-Type。
    // 这里使用 from_static，所以前面 validate_media_type 会先挡住明显非法的字面量。
    let produces_set = match &produces {
        Some(p) => quote! {
            __resp.headers_mut().insert(
                #axum_p::http::header::CONTENT_TYPE,
                #axum_p::http::HeaderValue::from_static(#p),
            );
        },
        None => quote! {},
    };
    // 中间件函数按 handler 名生成唯一标识符，避免同一模块内多个路由互相重名。
    // 只有确实声明了 produces 或 consumes 时才生成，普通路由不额外包一层。
    let mw_fn_def = if need_mw {
        quote! {
            /// 业务作用：在单一路由边界校验请求媒体类型，并按声明覆盖响应媒体类型。
            ///
            /// # 参数
            /// - `req`: HTTP 请求对象。
            /// - `next`: 下一个中间件或后续处理器。
            async fn #mw_ident(
                req: #axum_p::extract::Request,
                next: #axum_p::middleware::Next,
            ) -> #axum_p::response::Response {
                #consumes_check
                let mut __resp = next.run(req).await;
                #produces_set
                __resp
            }
        }
    } else {
        quote! {}
    };
    // .layer(...) 挂在 MethodRouter 上，只影响当前这条路由，不影响其它自动收集的 handler。
    let layer_apply = if need_mw {
        quote! { .layer(#axum_p::middleware::from_fn(#mw_ident)) }
    } else {
        quote! {}
    };

    let produces_opt = opt_str_tokens(&produces);
    let consumes_opt = opt_str_tokens(&consumes);

    let Some(mapping_p) = runtime_p else {
        if request_schema.is_some()
            || response_schema.is_some()
            || query_parameters.is_some()
            || header_parameters.is_some()
            || additional_responses.is_some()
            || success_status != 200
            || streaming
        {
            return attribute_error(
                verb,
                "OpenAPI 路由元数据需要通过 naweb 或 nasa 门面使用 mapping 宏",
            );
        }
        let expanded = quote! {
            #func
            #mw_fn_def

            #[#linkme_p::distributed_slice(crate::__mvc::ROUTES)]
            #[linkme(crate = #linkme_p)]
            #[allow(non_upper_case_globals)]
            static #reg_ident: crate::__mvc::RouteEntry = crate::__mvc::RouteEntry {
                method: #method,
                path: #path,
                handler: concat!(module_path!(), "::", stringify!(#fn_ident)),
                produces: #produces_opt,
                consumes: #consumes_opt,
                request_schema: None,
                response_schema: None,
                register: |__r| __r.route(
                    #path,
                    #axum_p::routing:: #verb_ident (#fn_ident) #layer_apply,
                ),
            };
        };
        return expanded.into();
    };
    let request_schema_opt = match request_schema {
        Some(schema) => quote! { Some(#mapping_p::api_schema::<#schema>) },
        None => quote! { None },
    };
    let response_schema_opt = match response_schema {
        Some(schema) => quote! { Some(#mapping_p::api_schema::<#schema>) },
        None => quote! { None },
    };
    let query_parameters_opt = match query_parameters {
        Some(parameters) => quote! { Some(#mapping_p::api_parameters::<#parameters>) },
        None => quote! { None },
    };
    let header_parameters_opt = match header_parameters {
        Some(parameters) => quote! { Some(#mapping_p::api_parameters::<#parameters>) },
        None => quote! { None },
    };
    let additional_responses_opt = match additional_responses {
        Some(responses) => quote! { Some(#mapping_p::api_responses::<#responses>) },
        None => quote! { None },
    };

    let auth_tokens = match auth.as_deref() {
        Some("required") => quote! { #mapping_p::AuthRequirement::Required },
        Some("optional") => quote! { #mapping_p::AuthRequirement::Optional },
        Some("public") => quote! { #mapping_p::AuthRequirement::Public },
        _ => quote! { #mapping_p::AuthRequirement::Unspecified },
    };
    let decrypt_tokens = if decrypt {
        quote! { #mapping_p::CryptoRequirement::Required }
    } else {
        quote! { #mapping_p::CryptoRequirement::Disabled }
    };
    let encrypt_tokens = if encrypt {
        quote! { #mapping_p::CryptoRequirement::Required }
    } else {
        quote! { #mapping_p::CryptoRequirement::Disabled }
    };
    let replay_tokens = if replay_value == "required" {
        quote! { #mapping_p::ReplayRequirement::Required }
    } else {
        quote! { #mapping_p::ReplayRequirement::Disabled }
    };
    let auth_provider_opt = opt_str_tokens(&auth_provider);
    let auth_condition_opt = opt_str_tokens(&auth_condition);
    let crypto_protocol_opt = opt_str_tokens(&crypto_protocol);
    let crypto_provider_opt = opt_str_tokens(&crypto_provider);
    let crypto_key_scope_opt = opt_str_tokens(&crypto_key_scope);
    let crypto_condition_opt = opt_str_tokens(&crypto_condition);
    let response_contract_opt = opt_str_tokens(&response_contract);
    let interceptor_descriptors = interceptors.iter().map(|path| {
        quote! { <#path as #mapping_p::InterceptorDefinition>::DESCRIPTOR }
    });
    let endpoint_bindings = interceptors.iter().map(|path| {
        quote! {
            #mapping_p::InterceptorBinding::new(
                <#path as #mapping_p::InterceptorDefinition>::DESCRIPTOR,
                |__method: #axum_p::routing::MethodRouter<crate::__mvc::RouterState>,
                 __state: &crate::__mvc::RouterState| {
                    __method.layer(#axum_p::middleware::from_fn_with_state(
                        __state.clone(),
                        #path,
                    ))
                },
            )
        }
    });
    let error_profile = error_profile.unwrap_or_else(|| "http-standard".to_string());
    let audience_tokens = match audience {
        Some(value) => quote! { #value },
        None => quote! { concat!(env!("CARGO_PKG_NAME"), ":", #method, " ", #path) },
    };
    let policy_tokens = quote! {
        #mapping_p::RoutePolicy {
            route_id: concat!(#method, " ", #path, " ", module_path!(), "::", stringify!(#fn_ident)),
            method: #method,
            path_template: #path,
            handler: concat!(module_path!(), "::", stringify!(#fn_ident)),
            auth: #auth_tokens,
            auth_provider: #auth_provider_opt,
            auth_condition: #auth_condition_opt,
            crypto_request: #decrypt_tokens,
            crypto_response: #encrypt_tokens,
            crypto_protocol: #crypto_protocol_opt,
            crypto_provider: #crypto_provider_opt,
            crypto_key_scope: #crypto_key_scope_opt,
            crypto_condition: #crypto_condition_opt,
            replay: #replay_tokens,
            error_profile: #error_profile,
            audience: #audience_tokens,
            response_contract: #response_contract_opt,
        }
    };
    let endpoint_layer = if auth.is_some() || crypto_enabled {
        quote! {
            __method = __method.layer(#axum_p::middleware::from_fn_with_state(
                #mapping_p::EndpointLayerState::new(
                    __runtime.clone(),
                    __policy,
                    __has_auth_interceptor,
                ),
                #mapping_p::endpoint_middleware,
            ));
        }
    } else {
        quote! {}
    };

    // ── ④ 输出：原 handler 原样 + (可选)中间件函数 + linkme 注册项 ──
    // register 是无捕获闭包，可自动转成 fn 指针放入 static；真正的 route 装配发生在 register_all。
    let expanded = quote! {
        #func
        #mw_fn_def

        #[#linkme_p::distributed_slice(crate::__mvc::ROUTES)] // ← 收进 mvc_router! 生成的全局表(crate::__mvc 恒不变)
        #[linkme(crate = #linkme_p)]
        #[allow(non_upper_case_globals)]
        static #reg_ident: crate::__mvc::RouteEntry = crate::__mvc::RouteEntry {
            method: #method,
            path: #path,
            handler: concat!(module_path!(), "::", stringify!(#fn_ident)),
            produces: #produces_opt,
            consumes: #consumes_opt,
            request_schema: #request_schema_opt,
            response_schema: #response_schema_opt,
            query_parameters: #query_parameters_opt,
            header_parameters: #header_parameters_opt,
            success_status: #success_status,
            additional_responses: #additional_responses_opt,
            streaming: #streaming,
            policy: #policy_tokens,
            interceptors: &[#(#interceptor_descriptors),*],
            register_plain: |__r| {
                __r.route(
                    #path,
                    #axum_p::routing:: #verb_ident (#fn_ident) #layer_apply,
                )
            },
            register: |__r, __runtime, __plan, __state| {
                let __policy = #policy_tokens;
                let __endpoint_bindings = ::std::vec![#(#endpoint_bindings),*];
                let __effective = __plan.effective(__policy, __endpoint_bindings)?;
                let __has_auth_interceptor = __effective.has_auth();
                let mut __method = #axum_p::routing:: #verb_ident (#fn_ident) #layer_apply;
                __method = __effective.apply_plaintext(
                    __method,
                    __state,
                    __policy,
                    __runtime.clone(),
                )?;
                #endpoint_layer
                __method = __effective.apply_auth(
                    __method,
                    __state,
                    __policy,
                    __runtime.clone(),
                )?;
                __method = __effective.apply_edge(
                    __method,
                    __state,
                    __policy,
                    __runtime.clone(),
                )?;
                let _ = __has_auth_interceptor;
                Ok(__r.route(#path, __method))
            },
        };
    };
    expanded.into()
}

/// 业务作用：produces/consumes 的最小媒体类型校验:非空 + 不含控制字符。
///   不做完整 media-type 解析(不引依赖),只挡住空串与控制字符这类明显写错;
///   出错返回指向注解处的 `compile_error!`,由调用方 `return ...into()`。
///
/// # 参数
/// - `verb`: HTTP 方法字符串。
/// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
/// - `value`: `produces` 或 `consumes` 属性中的媒体类型文本。
fn validate_media_type(
    verb: &str,
    field: &str,
    value: &str,
) -> Result<(), proc_macro2::TokenStream> {
    if value.is_empty() {
        return Err(syn::Error::new(
            Span::call_site(),
            format!("#[{verb}_mapping]: {field} 不能为空字符串"),
        )
        .to_compile_error());
    }
    if value.chars().any(char::is_control) {
        return Err(syn::Error::new(
            Span::call_site(),
            format!("#[{verb}_mapping]: {field} 不能包含控制字符(当前: {value:?})"),
        )
        .to_compile_error());
    }
    Ok(())
}

/// 业务作用：Option<String> → `Some("x")` / `None` 的 token。
///
/// # 参数
/// - `o`: 映射宏正在处理的输出字段描述。
fn opt_str_tokens(o: &Option<String>) -> proc_macro2::TokenStream {
    match o {
        Some(s) => quote! { ::core::option::Option::Some(#s) },
        None => quote! { ::core::option::Option::None },
    }
}

/// 业务作用：判断媒体类型是否属于当前明确不支持的流式或文件场景。
///
/// # 参数
///
/// - `value`: 宏字面量提供且已经通过控制字符检查的媒体类型。
///
/// # 返回
///
/// multipart、事件流、八位流或 WebSocket 相关类型返回 `true`。
fn forbidden_crypto_media_type(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.starts_with("multipart/")
        || lower.starts_with("text/event-stream")
        || lower.starts_with("application/octet-stream")
        || lower.contains("websocket")
}

/// 业务作用：校验路由策略中用作注册表索引的静态 ID。
///
/// # 参数
///
/// - `value`: 属性宏字符串字面量，最大允许 64 个 ASCII 字节。
///
/// # 返回
///
/// 非空且只含字母、数字、点、横线与下划线时返回 `true`。
fn valid_policy_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 业务作用：建立指向当前路由属性的编译期错误。
///
/// # 参数
///
/// - `verb`: 当前属性宏的小写 HTTP 动词。
/// - `message`: 不含业务输入的静态或本地校验说明。
///
/// # 返回
///
/// 返回可直接结束过程宏展开的 `compile_error!` token stream。
fn attribute_error(verb: &str, message: &str) -> TokenStream {
    syn::Error::new(Span::call_site(), format!("#[{verb}_mapping]: {message}"))
        .to_compile_error()
        .into()
}

/// 业务作用：解析业务 crate 实际依赖的 naweb 运行时门面路径。
///
/// # 返回
///
/// 通过 `naweb` 或 `nasa::web` 使用宏时返回可生成安全类型的路径；仅直接依赖
/// `naweb-macro` 时返回 `None`，只保留不含安全参数的装配能力。
fn web_runtime_path() -> Option<proc_macro2::TokenStream> {
    use nasa_macro_support::WebRoot;
    match nasa_macro_support::web_root() {
        WebRoot::Runtime(runtime) => Some(quote! { #runtime }),
        WebRoot::DirectMacro => None,
    }
}

/// 业务作用：三方依赖路径解析
/// - 业务依赖 `nasa`(含重命名)→ `::nasa::web::__private::<crate>`;
/// - 业务直接依赖 `naweb` 运行时 → `::naweb::__private::<crate>`;
/// - 旧布局(直接依赖 naweb-macro + axum/linkme/tracing)→ 裸 `::axum` 等。
///
/// `crate::__mvc` 恒不变:收集模块必须生成在业务 crate 内(State 单态化 + linkme 链接收集)。
fn third_party_paths() -> (
    proc_macro2::TokenStream, // axum
    proc_macro2::TokenStream, // linkme
    proc_macro2::TokenStream, // tracing
) {
    use nasa_macro_support::WebRoot;
    match nasa_macro_support::web_root() {
        WebRoot::Runtime(rt) => (
            quote! { #rt::__private::axum },
            quote! { #rt::__private::linkme },
            quote! { #rt::__private::tracing },
        ),
        WebRoot::DirectMacro => (quote! { ::axum }, quote! { ::linkme }, quote! { ::tracing }),
    }
}
