//! 过程宏运行时 crate 路径发现工具。
//!
//! 供各属性宏在“业务直接依赖实现 crate”和“业务只依赖门面 crate”两种接入形态下，
//! 生成可解析的运行时代码路径。
// 运行时优先从直接依赖解析，再回退 nasa 门面，并保留 Cargo 重命名后的实际名称。
// 依赖发现不检查门面 feature；直接运行时优先可以避免其他用途的 nasa 依赖遮蔽可用组件。
// 同一组件的门面与直接依赖必须解析到同一源码身份，否则可能发生包冲突或运行时全局状态分裂。
// 第三方依赖通过运行时 __private 桥访问，业务无需为宏展开细节额外声明这些依赖。

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// 门面 crate 的包名(业务可重命名,proc-macro-crate 能识别)。
pub const FACADE: &str = "nasa";

/// handler 包装产物:把原函数体搬进内层 `async fn`,使**原返回类型仍出现在返回位置**。
///
/// 监控类属性宏(`#[hystrix]` / `#[grafana]`)要把 handler 的返回值转成 `Response`,
/// 因此外层签名的返回类型被改写。若把原函数体直接塞进无标注的 `async move { ... }`,
/// 返回 `Result<T, E>` 的 handler 会丢失类型信息(`E` 不可推断 → E0282/E0283);
/// 而 `let __out: 原类型 = ...` 这种标注又对 `impl Trait` 非法。
/// 内层 `async fn` 同时满足两者:原类型写在 `->` 后面,`impl Trait` 合法,`?` 也能推断。
pub struct HandlerWrapper {
    /// 外层包装函数的参数列表:每个参数改写成 `<前缀><序号>: 原类型`,便于原样透传。
    pub outer_inputs: syn::punctuated::Punctuated<syn::FnArg, syn::Token![,]>,
    /// 内层 `async fn` 定义(保留原参数模式、原返回类型与原函数体)。
    pub inner_fn: TokenStream,
    /// 调用内层 `async fn` 并 `await` 的表达式。
    pub call_inner: TokenStream,
}

/// 业务作用：保留原处理函数的参数模式与返回类型，生成可在外层统一转换响应的异步内层包装。
///
/// 参数说明：
/// - `sig`：调用方改写返回类型之前的原始签名，用于保留输入类型、解构模式和结果类型。
/// - `block`：原业务函数体，不在宏展开阶段执行。
/// - `inner_ident`：内层异步函数标识，应使用宏专属前缀避免与业务声明冲突。
/// - `arg_prefix`：外层透传参数的合法标识前缀，后接参数序号生成局部变量名。
///
/// 返回：可拆分签名的外层输入、内层定义及等待表达式；接收者、泛型、where 子句、可变参数或参数属性使其返回 None。
/// 调用方负责选择其余签名的展开方式；无效标识前缀可能在构造标识时 panic，不会隐式改写业务函数体。
pub fn wrap_handler(
    sig: &syn::Signature,
    block: &syn::Block,
    inner_ident: &syn::Ident,
    arg_prefix: &str,
) -> Option<HandlerWrapper> {
    // 泛型、接收者和参数属性不能安全地机械搬入独立内层函数，保留由上层宏选择展开策略的机会。
    if !sig.generics.params.is_empty()
        || sig.generics.where_clause.is_some()
        || sig.variadic.is_some()
    {
        return None;
    }
    let mut outer_inputs = syn::punctuated::Punctuated::new();
    let mut call_args = Vec::with_capacity(sig.inputs.len());
    for (index, input) in sig.inputs.iter().enumerate() {
        let syn::FnArg::Typed(typed) = input else {
            return None;
        };
        if !typed.attrs.is_empty() {
            return None;
        }
        let ident = format_ident!("{}{}", arg_prefix, index);
        let ty = &typed.ty;
        outer_inputs.push(syn::parse_quote!(#ident: #ty));
        call_args.push(quote! { #ident });
    }
    let inner_inputs = &sig.inputs;
    let inner_output = &sig.output;
    Some(HandlerWrapper {
        outer_inputs,
        inner_fn: quote! {
            /// 业务作用：在监控包装内保留原处理函数的参数模式与返回类型，使业务错误推断保持一致。
            /// 参数说明：
            /// 业务输入及逐参数语义沿用外层处理函数声明，包装层不新增业务参数。
            /// 返回：原处理函数的执行结果，不改变其错误类型。
            async fn #inner_ident(#inner_inputs) #inner_output #block
        },
        call_inner: quote! { #inner_ident(#(#call_args),*).await },
    })
}

/// 业务作用：兼容性包装：查询调用方是否依赖指定运行时包。
///
/// 参数说明：
/// - `package`: Cargo 包名，例如 `hystrix`、`cacheable`、`natx` 或 `naweb`。
///
/// 返回：Cargo 识别的自身包或重命名依赖身份；包缺失或依赖解析失败均为 None，由上层决定回退或报错。
fn crate_name_compat(package: &str) -> Option<proc_macro_crate::FoundCrate> {
    proc_macro_crate::crate_name(package).ok()
}

/// 业务作用：为单层门面模块解析宏展开所用的运行时根，优先匹配直接运行时依赖并支持 Cargo 重命名。
///
/// 参数说明：
/// - `module`: 门面 crate 下的模块名，例如 `hystrix`、`cache`、`tx` 或 `scheduling`。
/// - `legacy`: 直接依赖的包名，用于兼容未走门面导出的调用方。
///
/// 返回：直接运行时根或 nasa 门面模块路径；均无法解析时返回提示，由宏转换为编译错误，不检查门面 feature 是否启用。
pub fn runtime_root(module: &str, legacy: &str) -> Result<TokenStream, String> {
    runtime_root_nested(&[module], module, legacy)
}

/// 业务作用：解析支持多级门面模块的运行时根，同时保持直接运行时优先和 Cargo 重命名语义。
///
/// 参数说明：
/// - `modules`：门面 crate 下按顺序追加的模块段，例如 `tx`、`pgsql`。
/// - `feature_hint`：依赖缺失时向使用者展示的门面 feature 名称。
/// - `legacy`：直接依赖的 Cargo 包名。
///
/// 返回：直接运行时存在时返回其 crate 根；否则返回 nasa 门面下的嵌套路径；两者都缺失时返回提示。
pub fn runtime_root_nested(
    modules: &[&str],
    feature_hint: &str,
    legacy: &str,
) -> Result<TokenStream, String> {
    use proc_macro_crate::{crate_name, FoundCrate};

    if let Some(root) = direct_runtime_root(legacy) {
        return Ok(root);
    }
    let modules = modules
        .iter()
        .map(|module| format_ident!("{module}"))
        .collect::<Vec<_>>();
    match crate_name(FACADE) {
        Ok(FoundCrate::Itself) => Ok(quote!(crate #(::#modules)*)),
        Ok(FoundCrate::Name(name)) => {
            let name = format_ident!("{name}");
            Ok(quote!(::#name #(::#modules)*))
        }
        Err(_) => Err(format!(
            "找不到运行时依赖:请在 Cargo.toml 依赖 `nasa`(features 含 \"{feature_hint}\")\
             或直接依赖 `{legacy}`(若短名被 crates.io 占用,使用本仓对应发布包名)"
        )),
    }
}

/// 业务作用：把直接运行时包解析为宏展开可用的 crate 根，并正确处理同包非 lib target。
///
/// 参数说明：
/// - `legacy`：直接运行时的 Cargo 包名，用于解析重命名依赖及自身包内不同 target 的访问路径。
///
/// 返回：依赖存在时返回实际重命名路径；不存在时返回 `None` 交由门面解析。
fn direct_runtime_root(legacy: &str) -> Option<TokenStream> {
    use proc_macro_crate::FoundCrate;

    match crate_name_compat(legacy)? {
        FoundCrate::Itself => {
            let lib = legacy.replace('-', "_");
            let compiling = std::env::var("CARGO_CRATE_NAME").unwrap_or_default();
            let is_bin = std::env::var_os("CARGO_BIN_NAME").is_some();
            let not_lib = is_bin || (!compiling.is_empty() && compiling != lib);
            if not_lib {
                let name = format_ident!("{lib}");
                Some(quote!(::#name))
            } else {
                Some(quote!(crate))
            }
        }
        FoundCrate::Name(name) => {
            let name = format_ident!("{name}");
            Some(quote!(::#name))
        }
    }
}

/// Web mapping 宏的解析结果：直接依赖 naweb-macro 时使用第三方路径；
/// 其余场景优先使用直接 naweb，再使用 nasa::web 门面，均未发现时回退第三方路径。
pub enum WebRoot {
    /// 经门面/运行时桥:第三方走 `<root>::__private::<crate>`。
    Runtime(TokenStream),
    /// 仅直接依赖 naweb-macro:第三方走裸 `::axum`/`::linkme`/`::tracing`。
    DirectMacro,
}

/// 业务作用：解析 Web mapping 宏的运行时根：仅直接依赖 naweb-macro → 直接 naweb → nasa::web。
///
/// 参数说明: 无。
///
/// 返回：可经运行时桥访问的路径，或仅宏依赖场景使用的直接第三方路径标记。
pub fn web_root() -> WebRoot {
    use proc_macro_crate::{crate_name, FoundCrate};
    // ① 调用方仅直接依赖 naweb-macro(宏经传递 re-export 到达时不会命中)。
    if matches!(crate_name_compat("naweb-macro"), Some(FoundCrate::Name(_))) {
        return WebRoot::DirectMacro;
    }
    // ② 直接依赖 naweb 运行时。
    match crate_name_compat("naweb") {
        Some(FoundCrate::Itself) => return WebRoot::Runtime(quote!(crate)),
        Some(FoundCrate::Name(n)) => {
            let n = format_ident!("{n}");
            return WebRoot::Runtime(quote!(::#n));
        }
        None => {}
    }
    // ③ 门面(含重命名)。
    match crate_name(FACADE) {
        Ok(FoundCrate::Itself) => WebRoot::Runtime(quote!(crate::web)),
        Ok(FoundCrate::Name(n)) => {
            let n = format_ident!("{n}");
            WebRoot::Runtime(quote!(::#n::web))
        }
        // ④ 兜底 = 仅宏依赖所需的裸路径。
        Err(_) => WebRoot::DirectMacro,
    }
}
