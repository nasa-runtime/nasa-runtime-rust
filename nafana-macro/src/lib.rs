//! `#[grafana]` 属性宏:编译期把 async handler 包装进 `nafana` 运行时命令,保留原函数签名。
//!
//! 参数全部可选，宏负责参数校验、路由名提取、运行时命令初始化和重复标注检测。

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, FnArg, ItemFn, LitInt, LitStr};

/// 业务作用：把一个同步终态响应函数收集为 nafana 进程级全局降级入口。
///
/// 属性不接受参数；函数必须同步、恰好接收一个 `FallbackContext`，并返回任意
/// `IntoResponse`。具体参数和返回类型由生成代码交给 Rust 类型系统校验。
///
/// # 参数说明
///
/// - `attr`: 属性参数，必须为空。
/// - `item`: 被收集的同步终态响应函数。
///
/// # 返回
///
/// 返回原函数与 linkme 描述项；签名不满足终态处理约束时返回定位明确的编译错误。
#[proc_macro_attribute]
pub fn global_fallback(attr: TokenStream, item: TokenStream) -> TokenStream {
    let root = match nasa_macro_support::runtime_root("grafana", "nafana") {
        Ok(root) => root,
        Err(message) => return quote! { ::core::compile_error!(#message); }.into(),
    };
    expand_global_fallback(attr, item, root, "nafana")
}

/// 业务作用：校验全局终态处理函数形态并生成组件级链接切片注册项。
///
/// # 参数说明
///
/// - `attr`: 属性参数，必须为空。
/// - `item`: 被注解函数的源码 token。
/// - `root`: 当前依赖形态下的 nafana 运行时根路径。
/// - `component`: 编译错误中使用的组件名。
///
/// # 返回
///
/// 返回可直接交给编译器的展开结果；失败时返回 `compile_error!`。
fn expand_global_fallback(
    attr: TokenStream,
    item: TokenStream,
    root: proc_macro2::TokenStream,
    component: &str,
) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("#[{component}::global_fallback] 不接受参数"),
        )
        .to_compile_error()
        .into();
    }
    let function = parse_macro_input!(item as ItemFn);
    if function.sig.asyncness.is_some() {
        return syn::Error::new_spanned(
            function.sig.asyncness,
            format!("#[{component}::global_fallback] 必须用于同步 fn"),
        )
        .to_compile_error()
        .into();
    }
    if function.sig.constness.is_some()
        || function.sig.unsafety.is_some()
        || function.sig.abi.is_some()
        || function.sig.variadic.is_some()
        || !function.sig.generics.params.is_empty()
        || function.sig.generics.where_clause.is_some()
    {
        return syn::Error::new_spanned(
            &function.sig,
            format!(
                "#[{component}::global_fallback] 不支持 const、unsafe、extern、可变参数或泛型函数"
            ),
        )
        .to_compile_error()
        .into();
    }
    if function.sig.inputs.len() != 1
        || !matches!(function.sig.inputs.first(), Some(FnArg::Typed(_)))
    {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            format!("#[{component}::global_fallback] 函数必须恰好接收一个 FallbackContext 参数"),
        )
        .to_compile_error()
        .into();
    }

    let ident = &function.sig.ident;
    quote! {
        #function

        const _: () = {
            #[#root::__private::linkme::distributed_slice(
                #root::__private::NAFANA_COLLECTED_GLOBAL_FALLBACKS
            )]
            #[linkme(crate = #root::__private::linkme)]
            static __NAFANA_GLOBAL_FALLBACK: #root::__private::CollectedGlobalFallback =
                #root::__private::CollectedGlobalFallback {
                    source: ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#ident)),
                    handle: |context| #root::FallbackDecision::Respond(
                        #root::__private::axum::response::IntoResponse::into_response(
                            #ident(context)
                        )
                    ),
                };
        };
    }
    .into()
}

/// 业务作用：# `#[grafana]` —— 给 axum handler 加 bulkhead 隔离 + 超时 + Prometheus 指标
///
/// **所有参数全可省**;空参 = 只采集监控指标(不拦截、不限时)。
///
/// | 参数 | 类型 | 说明 |
/// |---|---|---|
/// | `name` | 字符串 | 面板 tile 名/日志名。不写时:若本 fn 下方还贴着 `#[get_mapping]` 等(要求 `#[grafana]` 在它【上面】)→ 自动取其 path;否则取函数名 |
/// | `max_concurrent` | 整数 | 并发上限;不写或 0 = 不限(0 值规范,合同) |
/// | `timeout_ms` | 整数 | 单请求超时毫秒;不写或 0 = 不超时 |
/// | `tps` | 整数 | TPS 权重;不写 = 不计 TPS,显式 0 = 标记但权重 0 |
/// | `reject_response` | JSON 串 | bulkhead 满时以 HTTP 200 返回的 body;与 `reject_fn` 互斥 |
/// | `timeout_response` | JSON 串 | 超时时以 HTTP 200 返回的 body;与 `timeout_fn` 互斥 |
/// | `reject_fn` | 路径 | bulkhead 满时调用的降级 fn;优先级最高 |
/// | `timeout_fn` | 路径 | 超时时调用的降级 fn;优先级最高 |
///
/// ```ignore
/// #[grafana]                                              // 空参 = 只监控
/// #[grafana(max_concurrent = 20, timeout_ms = 800)]       // bulkhead 20 并发 + 超时 800ms
/// #[grafana(name = "下载", max_concurrent = 5, timeout_ms = 3000, tps = 1)]
/// ```
/// # 参数
/// - `attr`: `#[grafana(...)]` 括号内的隔离、超时、降级和监控配置 token stream。
/// - `item`: 被注解的 async fn 源码 token。
#[proc_macro_attribute]
pub fn grafana(attr: TokenStream, item: TokenStream) -> TokenStream {
    // ⓪ 运行时根路径发现:nasa → ::nasa::grafana;旧直接依赖 → ::nafana。
    let root = match nasa_macro_support::runtime_root("grafana", "nafana") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    // axum 经运行时 __private 桥引用:业务只依赖 nasa 时无需直接声明 axum。
    let axum = quote! { #root::__private::axum };

    // ── 1. 解析注解参数;tps 用 Option 以区分"没写"和"显式 0" ──
    let mut name: Option<String> = None;
    let mut max_concurrent: Option<usize> = None;
    let mut timeout_ms: Option<u64> = None;
    let mut tps: Option<u64> = None;
    let mut reject_response: Option<String> = None;
    let mut timeout_response: Option<String> = None;
    let mut reject_fn: Option<syn::Path> = None;
    let mut timeout_fn: Option<syn::Path> = None;

    let arg_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("max_concurrent") {
            max_concurrent = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("timeout_ms") {
            timeout_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("tps") {
            tps = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("reject_response") {
            reject_response = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("timeout_response") {
            timeout_response = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("reject_fn") {
            reject_fn = Some(meta.value()?.parse::<syn::Path>()?);
        } else if meta.path.is_ident("timeout_fn") {
            timeout_fn = Some(meta.value()?.parse::<syn::Path>()?);
        } else {
            return Err(meta.error(
                "#[grafana]: 只支持 name / max_concurrent / timeout_ms / tps / reject_response / timeout_response / reject_fn / timeout_fn",
            ));
        }
        Ok(())
    });
    parse_macro_input!(attr with arg_parser);

    // 1b. 编译期校验自定义返回是合法 JSON,早报错。
    for (label, val) in [
        ("reject_response", &reject_response),
        ("timeout_response", &timeout_response),
    ] {
        if let Some(js) = val {
            if serde_json::from_str::<serde_json::Value>(js).is_err() {
                return syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!("#[grafana]: {label} 不是合法 JSON: {js}"),
                )
                .to_compile_error()
                .into();
            }
        }
    }

    // 1c. 逐路径互斥:降级 fn 与静态返回只能给一个。
    for (cond, msg) in [
        (
            reject_fn.is_some() && reject_response.is_some(),
            "#[grafana]: reject_fn 与 reject_response 互斥(降级 fn 与静态返回只能给一个)",
        ),
        (
            timeout_fn.is_some() && timeout_response.is_some(),
            "#[grafana]: timeout_fn 与 timeout_response 互斥(降级 fn 与静态返回只能给一个)",
        ),
    ] {
        if cond {
            return syn::Error::new(proc_macro2::Span::call_site(), msg)
                .to_compile_error()
                .into();
        }
    }

    // ── 2. 解析被注解函数 + 形态校验 ──
    let func = parse_macro_input!(item as ItemFn);
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&func.sig.ident, "#[grafana] 只能用于 async fn")
            .to_compile_error()
            .into();
    }

    // ── 3. 重复标注守卫 ──
    // 外层展开产物含 __NAFANA_CMD；再次展开会双重计数，因此在生成代码前拒绝。
    let mut saw_nafana_cmd = false;
    scan_idents(func.block.to_token_stream(), &mut saw_nafana_cmd);
    if saw_nafana_cmd {
        return syn::Error::new_spanned(
            &func.sig.ident,
            "#[grafana] 重复贴在同一个端点上(会双重包裹、双重计数),只保留一个",
        )
        .to_compile_error()
        .into();
    }

    // ── 4. 分别解析展示名与真实路由 ──
    // 展示名允许由 `name` 覆盖，但指标的 `path` 必须始终保留路由注解中的地址，
    // 否则面板标题一旦改名，排查时便无法还原实际请求入口。
    let mapping_route = func.attrs.iter().find_map(|attr| {
        let is_mapping = attr_name_is(attr, "get_mapping")
            || attr_name_is(attr, "post_mapping")
            || attr_name_is(attr, "put_mapping")
            || attr_name_is(attr, "delete_mapping")
            || attr_name_is(attr, "patch_mapping");
        is_mapping.then(|| mapping_path(attr)).flatten()
    });
    let name = name
        .or_else(|| mapping_route.clone())
        .unwrap_or_else(|| func.sig.ident.to_string());
    let route = mapping_route.unwrap_or_else(|| name.clone());

    // ── 5. 拆函数部件并生成包装函数 ──
    let attrs = &func.attrs;
    let vis = &func.vis;
    let block = &func.block;
    let mut sig = func.sig.clone();
    // 原返回类型必须留在"返回位置",否则 `-> Result<T, E>` / `-> Result<impl IntoResponse, E>`
    // 会因为类型信息丢失而不可推断(E0282/E0283)。优先把函数体搬进内层 async fn;
    // 签名形态不支持拆参数时退回"中间变量标注",`impl Trait` 再退回直接转换。
    let inner_ident = syn::Ident::new("__nafana_handler_impl", proc_macro2::Span::call_site());
    let wrapped = nasa_macro_support::wrap_handler(&func.sig, block, &inner_ident, "__nafana_arg");
    let convert_output = match &wrapped {
        Some(wrapper) => {
            sig.inputs = wrapper.outer_inputs.clone();
            let inner_fn = &wrapper.inner_fn;
            let call_inner = &wrapper.call_inner;
            quote! {
                #inner_fn
                #axum::response::IntoResponse::into_response(#call_inner)
            }
        }
        None => {
            let original_output: syn::Type = match &func.sig.output {
                syn::ReturnType::Default => syn::parse_quote!(()),
                syn::ReturnType::Type(_, ty) => (**ty).clone(),
            };
            if type_contains_impl_trait(&original_output) {
                quote! {
                    #axum::response::IntoResponse::into_response((async move #block).await)
                }
            } else {
                quote! {
                    let __out: #original_output = (async move #block).await;
                    #axum::response::IntoResponse::into_response(__out)
                }
            }
        }
    };
    sig.output = syn::parse_quote!(-> #axum::response::Response);

    // Option 参数拼成源码级 Option 字面量;0 值归一化统一交给运行时 Command::monitor。
    let mc_opt = option_tokens(max_concurrent.map(|v| quote! { #v }));
    let to_opt =
        option_tokens(timeout_ms.map(|v| quote! { ::std::time::Duration::from_millis(#v) }));
    let tps_opt = option_tokens(tps.map(|v| quote! { #v }));

    // 降级槽:fn 优先 → 静态串 → 空(运行时回退默认壳)。闭包必须显式标注返回
    // `Pin<Box<dyn Future + Send>>`,让 Box::pin 在 return 处强制成 dyn。
    let fb_closure = |p: &syn::Path| {
        quote! {
            ::std::boxed::Box::new(
                || -> ::std::pin::Pin<::std::boxed::Box<
                    dyn ::std::future::Future<Output = #axum::response::Response> + ::core::marker::Send
                >> {
                    ::std::boxed::Box::pin(async {
                        #axum::response::IntoResponse::into_response(#p().await)
                    })
                }
            )
        }
    };
    let set_reject = match (&reject_fn, &reject_response) {
        (Some(p), _) => {
            let c = fb_closure(p);
            quote! { __c.set_reject_fn(#c); }
        }
        (None, Some(s)) => quote! { __c.set_reject_response_str(#s); },
        (None, None) => quote! {},
    };
    let set_timeout = match (&timeout_fn, &timeout_response) {
        (Some(p), _) => {
            let c = fb_closure(p);
            quote! { __c.set_timeout_fn(#c); }
        }
        (None, Some(s)) => quote! { __c.set_timeout_response_str(#s); },
        (None, None) => quote! {},
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            // 每个被注解的 handler 各自持有一个进程内静态、只构造一次的 Command;
            // 首个请求触发构造并进全局注册表(按 key 去重)→ /metrics 可见。
            static __NAFANA_CMD: ::std::sync::OnceLock<::std::sync::Arc<#root::Command>> =
                ::std::sync::OnceLock::new();
            let __cmd = __NAFANA_CMD.get_or_init(|| {
                let __c = #root::Command::monitor(#name, "macro", #mc_opt, #to_opt, #tps_opt);
                __c.set_path(#route);
                #set_reject
                #set_timeout
                __c
            }).clone();

            // 原函数体包成闭包交给 run_fn:抢到许可才执行,超时/拒绝走降级,结局进双轨指标。
            __cmd.run_fn(move || async move {
                #convert_output
            }).await
        }
    };
    expanded.into()
}

/// 业务作用：把 `Option<T>` 的解析结果拼成源码级 `Some(...)` / `None` token。
///
/// # 参数
/// - `value`: Some(值 token) 或 None。
fn option_tokens(value: Option<proc_macro2::TokenStream>) -> proc_macro2::TokenStream {
    match value {
        Some(v) => quote! { ::core::option::Option::Some(#v) },
        None => quote! { ::core::option::Option::None },
    }
}

/// 业务作用：递归扫描 token 流，检测是否已有宏生成的命令槽。
///
/// # 参数
/// - `stream`: 待扫描的 token 流(函数体)。
/// - `saw_nafana_cmd`: 命中 `__NAFANA_CMD` 时置位。
fn scan_idents(stream: proc_macro2::TokenStream, saw_nafana_cmd: &mut bool) {
    for tree in stream {
        match tree {
            proc_macro2::TokenTree::Ident(ident) if ident == "__NAFANA_CMD" => {
                *saw_nafana_cmd = true;
            }
            proc_macro2::TokenTree::Group(group) => {
                scan_idents(group.stream(), saw_nafana_cmd);
            }
            _ => {}
        }
    }
}

/// 业务作用：返回类型中包含返回位置 `impl Trait` 时，不能把它直接写进局部变量类型标注。
fn type_contains_impl_trait(ty: &syn::Type) -> bool {
    /// 业务作用：递归检查返回类型 token 树中是否出现 `impl` 关键字。
    fn contains(stream: proc_macro2::TokenStream) -> bool {
        stream.into_iter().any(|tree| match tree {
            proc_macro2::TokenTree::Ident(ident) => ident == "impl",
            proc_macro2::TokenTree::Group(group) => contains(group.stream()),
            _ => false,
        })
    }

    contains(ty.to_token_stream())
}

/// 业务作用：属性名按最后一个 path segment 匹配，支持短路径、门面完整路径和 Cargo 重命名后的多段路径。
///
/// # 参数
/// - `attr`: 待判定的属性。
/// - `expected`: 期望的末段名。
fn attr_name_is(attr: &syn::Attribute, expected: &str) -> bool {
    attr.path()
        .segments
        .last()
        .map(|segment| segment.ident == expected)
        .unwrap_or(false)
}

/// 业务作用：从一条 #[*_mapping(...)] 属性里抽出 path 字符串;单串简写与 key=value 具名写法都支持,
/// 取不到返回 None。
///
/// # 参数
/// - `attr`: mapping 属性。
fn mapping_path(attr: &syn::Attribute) -> Option<String> {
    if let Ok(lit) = attr.parse_args::<LitStr>() {
        return Some(lit.value());
    }
    let mut found = None;
    let _ = attr.parse_nested_meta(|meta| {
        let is_path = meta.path.is_ident("path") || meta.path.is_ident("value");
        if let Ok(val) = meta.value() {
            if let Ok(s) = val.parse::<LitStr>() {
                if is_path {
                    found = Some(s.value());
                }
            }
        }
        Ok(())
    });
    found
}
