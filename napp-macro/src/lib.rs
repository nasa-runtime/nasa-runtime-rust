//! 应用入口属性宏。
//!
//! 宏在业务二进制内生成静态组件描述、路由收集工厂和同步进程入口。

use std::collections::HashSet;

use nasa_macro_support::runtime_root;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, punctuated::Punctuated, FnArg, GenericArgument, ItemFn, LitStr,
    PathArguments, ReturnType, Token, Type,
};

/// 业务作用：把业务异步 `main` 转换为统一生命周期进程入口。
///
/// # 支持的组件字符串
///
/// `attr` 可以为空；非空时只接受下面 12 个区分大小写的精确字符串，不支持别名：
///
/// - `"log"`：启用两阶段日志。Bootstrap 先建立早期控制台日志，最终配置就绪后再安装文件日志，
///   并支持运行期日志级别热更新；需要 `nasa` 的 `log` feature。
/// - `"nacos-config"`：启用 Nacos 配置中心。启动时拉取远端配置 overlay，运行期监听配置变化并按
///   last-known-good 规则热刷新；需要 `nacos-config` feature，真实连接 Nacos 还需要 `nacos-sdk`。
/// - `"db"`：启用 MySQL 数据源。启动时校验并探测地址、鉴权和数据库，创建连接池、注册应用资源，
///   同时注入 `#[transactional]` 和 Mapper 使用的事务运行时；需要 `tx` feature。
/// - `"redis"`：启用 Redis 客户端。启动时校验配置、探测 standalone/cluster 拓扑并建立受管客户端，
///   停机时由容器显式关闭；需要 `redis` feature。
/// - `"telemetry"`：启用有界 OpenTelemetry span 管道与受管停机 flush；需要 `telemetry` feature。
/// - `"cache"`：启用由容器拥有的两级缓存运行时与可选跨节点失效广播；需要 `cache` feature。
/// - `"kafka"`：启用受管 Kafka producer/consumer。负责 broker 探测、consumer 收集与启动、动态
///   readiness、运行期健康监控、停止消费和 producer flush；需要 `kafka` feature。
/// - `"auth"`：启用 OAuth Resource Server/JWKS warmup、刷新和 readiness；必须同时声明 `"web"`，
///   需要 Web/OAuth 能力。
/// - `"web"`：启用 HTTP MVC 服务。自动收集 mapping 端点，安装 `/healthz`、`/readyz` 和请求观测，
///   绑定监听器并在停机时停止接流、排空在途请求；需要 `web` feature。
/// - `"ws"`：启用 TCP/WebSocket 长连接服务。业务在启动 Hook 中配置鉴权和 endpoint，容器负责监听、
///   会话服务、集群数据面接入与优雅排空；需要 `ws` feature，Redis/Kafka 集群子能力另开对应 feature。
/// - `"nacos-discovery"`：启用服务发现和注册。创建带负载均衡的出站 REST runtime，在服务 Ready 后
///   注册本实例，停机时先从注册中心摘流再关闭客户端；需要 `nacos-discovery` feature，真实 Nacos
///   provider 还需要 `nacos-sdk`。
/// - `"scheduling"`：启用定时任务。收集 `#[scheduled]` 任务，在 Application Ready 后统一启动并在
///   停机时停止；需要 `scheduling` feature，Redis 选主的集群调度使用 `scheduling-cluster`。
///
/// `"hystrix"`、`"grafana"`、`"mapper"` 等是门面 feature 或函数级能力，**不是**组件字符串。
///
/// # 声明顺序：与业务书写顺序无关
///
/// **业务侧不需要按启动顺序书写组件字符串**：宏接受任意顺序,内部按唯一的规范启动顺序
/// （`CANONICAL_COMPONENT_ORDER`：log → nacos-config → telemetry → db → redis → cache →
/// kafka → auth → web → ws → nacos-discovery → scheduling）自动规范化后再生成组件列表。因此
/// `#[application("web", "log", "kafka")]` 与 `#[application("log", "kafka", "web")]` 完全等价,
/// 都按 log → kafka → web 启动、严格反序停机。宏仍会拒绝未知组件名和重复声明。
///
/// 示例（任意顺序均可，等价于规范顺序）：
///
/// ```ignore
/// #[nasa::application(
///     "log",
///     "nacos-config",
///     "telemetry",
///     "db",
///     "redis",
///     "cache",
///     "kafka",
///     "auth",
///     "web",
///     "ws",
///     "nacos-discovery",
///     "scheduling"
/// )]
/// async fn main(app: nasa::Application) -> anyhow::Result<()> {
///     // 在这里登记业务资源以及 Web、WS、Kafka 等定制。
///     Ok(())
/// }
/// ```
///
/// # 参数
///
/// - `attr`：按上述启动顺序声明的零个或多个受支持组件字符串。
/// - `item`：零参数或接收一个 `Application` 的异步主函数。
#[proc_macro_attribute]
pub fn application(attr: TokenStream, item: TokenStream) -> TokenStream {
    let components =
        parse_macro_input!(attr with Punctuated::<LitStr, Token![,]>::parse_terminated);
    let function = parse_macro_input!(item as ItemFn);
    match expand_application(components.into_iter().collect(), function) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// 业务作用：校验入口契约并生成静态描述、业务 Hook 包装和同步主函数。
///
/// # 参数
///
/// - `components`：属性中按源码顺序出现的组件字面量。
/// - `function`：已经解析的业务异步主函数。
fn expand_application(
    components: Vec<LitStr>,
    mut function: ItemFn,
) -> syn::Result<proc_macro2::TokenStream> {
    validate_function(&function)?;
    let component_names = validate_components(&components)?;
    let runtime = runtime_root("application", "napp")
        .map_err(|message| syn::Error::new_spanned(&function.sig.ident, message))?;

    let has_web = component_names.iter().any(|name| name == "web");
    let component_variants = component_names
        .iter()
        .map(|name| component_variant(name))
        .collect::<syn::Result<Vec<_>>>()?;
    let feature_modules = component_names
        .iter()
        .map(|name| component_feature_module(name))
        .collect::<syn::Result<Vec<_>>>()?;
    let accepts_application = function.sig.inputs.len() == 1;
    function.sig.ident = format_ident!("__nasa_user_main");

    let hook = if accepts_application {
        quote!(|application| __nasa_user_main(application))
    } else {
        quote!(|_application| __nasa_user_main())
    };
    let web_items = if has_web {
        quote! {
            #runtime::__private::naweb::mvc_router!(#runtime::Application);

            /// 业务作用：把业务 crate 内收集的 nominal 路由项投影成稳定诊断元数据。
            ///
            /// # 参数
            ///
            /// 本函数无参数；返回值仅包含静态方法、路径和处理函数身份。
            fn __nasa_route_meta() -> ::std::vec::Vec<#runtime::RouteMeta> {
                crate::__mvc::ROUTES
                    .iter()
                    .map(|entry| #runtime::RouteMeta {
                        method: entry.method,
                        path: entry.path,
                        handler: entry.handler,
                        produces: entry.produces,
                        consumes: entry.consumes,
                        request_schema: entry.request_schema,
                        response_schema: entry.response_schema,
                        query_parameters: entry.query_parameters,
                        header_parameters: entry.header_parameters,
                        success_status: entry.success_status,
                        additional_responses: entry.additional_responses,
                        streaming: entry.streaming,
                        auth_required: ::core::matches!(
                            entry.policy.auth,
                            #runtime::__private::naweb::AuthRequirement::Required
                        ),
                    })
                    .collect()
            }

            /// 业务作用：构造只含自动收集端点、尚未补齐状态的业务路由。
            ///
            /// 状态刻意不在这里补：`configure_router` 的定制与框架探针都必须先作用在
            /// `Router<Application>` 上，`with_state` 由运行时在装配顺序末尾统一执行。
            ///
            /// # 参数
            ///
            /// `context` 由 napp Ready 构造，保证 interceptor 与 handler 使用同一个 Application clone。
            fn __nasa_build_router(
                context: #runtime::WebBuildContext,
            ) -> #runtime::ApplicationResult<
                #runtime::__private::axum::Router<#runtime::Application>,
            > {
                context.build(|router, mapping_runtime, mapping_plan, application| {
                    crate::__mvc::try_register_all(
                        router,
                        mapping_runtime,
                        mapping_plan,
                        application,
                    )
                })
            }
        }
    } else {
        quote! {}
    };
    let spec_web = if has_web {
        quote!(
            .with_web_route_meta(__nasa_route_meta)
            .with_web_factory(__nasa_build_router)
        )
    } else {
        quote! {}
    };

    Ok(quote! {
        #[doc(hidden)]
        pub mod __nasa_application_must_be_at_crate_root {}
        use crate::__nasa_application_must_be_at_crate_root as _;
        #(
            const _: () = #runtime::components::#feature_modules::FEATURE_CHECK;
        )*

        #web_items
        #function

        /// 业务作用：在生成入口附近约束业务 Hook 的可移动性、生命周期和错误类型。
        ///
        /// 该薄包装不执行 Hook；它把类型错误定位到业务入口，而不是延后到任务监督器内部。
        ///
        /// # 参数
        ///
        /// - `hook`：拥有业务主函数调用的闭包，接收统一 Application 并返回受监督 future。
        fn __nasa_require_user_hook<F, Fut, E>(hook: F) -> F
        where
            F: ::std::ops::FnOnce(#runtime::Application) -> Fut + ::std::marker::Send + 'static,
            Fut: ::std::future::Future<Output = ::std::result::Result<(), E>>
                + ::std::marker::Send
                + 'static,
            E: ::std::convert::Into<#runtime::__private::anyhow::Error> + 'static,
        {
            hook
        }

        /// 业务作用：创建运行时静态描述并把业务 Hook 交给统一同步入口。
        ///
        /// # 参数
        ///
        /// 本函数无参数；配置路径和进程信号均由运行时按固定契约接管。
        fn main() -> ::std::process::ExitCode {
            #runtime::run(
                #runtime::ApplicationSpec::new(&[
                    #(#runtime::ComponentId::#component_variants),*
                ])
                .with_default_name(env!("CARGO_PKG_NAME"))
                #spec_web,
                __nasa_require_user_hook(#hook),
            )
        }
    })
}

/// 业务作用：校验业务主函数的名称、异步形态、参数和返回结果形状。
///
/// # 参数
///
/// - `function`：属性直接标注的业务函数语法树。
fn validate_function(function: &ItemFn) -> syn::Result<()> {
    if function.sig.ident != "main" {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "application attribute must be attached to the crate main function",
        ));
    }
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "application main must be async and must not use another runtime entry attribute",
        ));
    }
    if !function.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.generics,
            "application main cannot declare generics",
        ));
    }
    if function.sig.inputs.len() > 1 {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "application main accepts at most one Application parameter",
        ));
    }
    if let Some(argument) = function.sig.inputs.first() {
        validate_application_parameter(argument)?;
    }
    validate_return_type(&function.sig.output)?;
    for attribute in &function.attrs {
        let segments = attribute
            .path()
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        if segments.first().is_some_and(|name| name == "tokio")
            && segments.last().is_some_and(|name| name == "main")
        {
            return Err(syn::Error::new_spanned(
                attribute,
                "remove the other runtime entry attribute because application owns the runtime",
            ));
        }
        if segments
            .last()
            .is_some_and(|name| matches!(name.as_str(), "EnableScheduling" | "EnableAsync"))
        {
            return Err(syn::Error::new_spanned(
                attribute,
                "declare the scheduling component in application instead of using an entry attribute",
            ));
        }
    }
    Ok(())
}

/// 业务作用：校验唯一可选参数的类型为统一 Application。
///
/// # 参数
///
/// - `argument`：业务主函数声明的唯一函数参数。
fn validate_application_parameter(argument: &FnArg) -> syn::Result<()> {
    let FnArg::Typed(argument) = argument else {
        return Err(syn::Error::new_spanned(
            argument,
            "application main cannot use a receiver parameter",
        ));
    };
    let Type::Path(path) = argument.ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            &argument.ty,
            "application main parameter must be Application",
        ));
    };
    if path
        .path
        .segments
        .last()
        .is_none_or(|segment| segment.ident != "Application")
    {
        return Err(syn::Error::new_spanned(
            &argument.ty,
            "application main parameter must be Application",
        ));
    }
    Ok(())
}

/// 业务作用：校验业务主函数返回单元成功值的 `Result`。
///
/// # 参数
///
/// - `output`：业务主函数声明的返回类型。
fn validate_return_type(output: &ReturnType) -> syn::Result<()> {
    let ReturnType::Type(_, output_type) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "application main must return anyhow::Result<()>",
        ));
    };
    let Type::Path(path) = output_type.as_ref() else {
        return Err(syn::Error::new_spanned(
            output_type,
            "application main must return anyhow::Result<()>",
        ));
    };
    let Some(result) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            output_type,
            "application main must return anyhow::Result<()>",
        ));
    };
    let PathArguments::AngleBracketed(arguments) = &result.arguments else {
        return Err(syn::Error::new_spanned(
            output_type,
            "application main must return anyhow::Result<()>",
        ));
    };
    let unit_success = matches!(
        arguments.args.first(),
        Some(GenericArgument::Type(Type::Tuple(tuple))) if tuple.elems.is_empty()
    );
    if result.ident != "Result" || arguments.args.len() != 1 || !unit_success {
        return Err(syn::Error::new_spanned(
            output_type,
            "application main must return anyhow::Result<()>",
        ));
    }
    Ok(())
}

/// 规范启动顺序:业务侧可以按任意顺序书写组件字符串,napp 由此秩统一规范化。
///
/// 该数组既是合法组件白名单,也是唯一的规范启动顺序(其偏序满足历史声明规则:log 最先;
/// nacos-config 先于其余;db/redis 先于 kafka;kafka 先于 web/ws/discovery/scheduling;
/// web 先于 nacos-discovery)。新增组件时在此按正确位置插入。
const CANONICAL_COMPONENT_ORDER: [&str; 12] = [
    "log",
    "nacos-config",
    "telemetry",
    "db",
    "redis",
    "cache",
    "kafka",
    "auth",
    "web",
    "ws",
    "nacos-discovery",
    "scheduling",
];

/// 业务作用：校验组件名称与重复项,并按规范启动顺序排序返回(业务书写顺序不影响启动顺序)。
///
/// 业务侧无需按启动顺序书写 `#[application(...)]`:本函数接受任意顺序,拒绝未知名称和重复项,
/// 然后按 [`CANONICAL_COMPONENT_ORDER`] 排序。运行时因此始终收到规范顺序的组件列表。
///
/// # 参数
///
/// - `components`：属性中以任意顺序提供的字符串字面量。
fn validate_components(components: &[LitStr]) -> syn::Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut names = Vec::with_capacity(components.len());
    for component in components.iter() {
        let name = component.value();
        if !CANONICAL_COMPONENT_ORDER.contains(&name.as_str()) {
            return Err(syn::Error::new_spanned(
                component,
                format!("unknown application component `{name}`"),
            ));
        }
        if !seen.insert(name.clone()) {
            return Err(syn::Error::new_spanned(
                component,
                format!("application component `{name}` is declared more than once"),
            ));
        }
        names.push(name);
    }
    // 顺序无关:按规范秩排序,业务书写顺序不再影响启动/停机顺序。
    names.sort_by_key(|name| {
        CANONICAL_COMPONENT_ORDER
            .iter()
            .position(|canonical| canonical == name)
            .expect("name validated against CANONICAL_COMPONENT_ORDER above")
    });
    Ok(names)
}

/// 业务作用：把规范化组件名称转换为运行时枚举变体。
///
/// # 参数
///
/// - `name`：已经通过白名单校验的组件名称。
fn component_variant(name: &str) -> syn::Result<syn::Ident> {
    let variant = match name {
        "log" => "Log",
        "nacos-config" => "NacosConfig",
        "db" => "Db",
        "redis" => "Redis",
        "telemetry" => "Telemetry",
        "cache" => "Cache",
        "kafka" => "Kafka",
        "auth" => "Auth",
        "web" => "Web",
        "ws" => "Ws",
        "nacos-discovery" => "NacosDiscovery",
        "scheduling" => "Scheduling",
        _ => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "component name was not validated",
            ));
        }
    };
    Ok(format_ident!("{variant}"))
}

/// 业务作用：把已校验组件名称转换为编译期能力探测模块。
///
/// # 参数
///
/// - `name`：已经通过组件白名单和顺序校验的规范名称。
fn component_feature_module(name: &str) -> syn::Result<syn::Ident> {
    match name {
        "log" | "db" | "redis" | "telemetry" | "cache" | "kafka" | "auth" | "web" | "ws"
        | "scheduling" => Ok(format_ident!("{name}")),
        "nacos-config" => Ok(format_ident!("nacos_config")),
        "nacos-discovery" => Ok(format_ident!("nacos_discovery")),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "component name was not validated",
        )),
    }
}
