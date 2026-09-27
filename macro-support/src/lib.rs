//! 过程宏运行时 crate 路径发现工具。
//!
//! 供各属性宏在“业务直接依赖实现 crate”和“业务只依赖门面 crate”两种接入形态下，
//! 生成可解析的运行时代码路径。
// ============================================================================
// macro-support —— 过程宏的运行时路径发现。
//
// 背景:宏展开代码原先硬编码 `::hystrix`、`::cacheable` 等绝对路径;业务改为只依赖
// `nasa` 门面后,这些传递依赖**不在业务 crate 的 extern prelude 里**,路径无法解析。
// 也不能改死成 `::nasa::hystrix`——用户可能 Cargo 重命名:
//   company-nasa = { package = "nasa", ... }   → 实际 crate 名是 company_nasa。
//
// ★ 解析顺序 = **旧直接依赖优先,门面回退**(proc-macro-crate 只能发现
// crate 是否存在,看不见 feature——若门面优先,"项目只为 WebSocket 引入
// nasa = { features = [\"ws\"] },HTTP 部分仍直接使用 hystrix/naweb-macro"的**渐进迁移**
// 会被无关的 nasa 依赖破坏:旧宏全部展开成 ::nasa::hystrix 而该 feature 未启用):
//   1) 调用方直接依赖旧运行时 crate(如 `hystrix`/`cacheable`,含重命名)→ `::<实际名>`;
//   2) 否则回退门面 `nasa`(含重命名)→ `::<实际名>::<module>`
//      (纯门面消费者没有旧直接依赖,恒走这支);
//   3) 都没有 → Err(统一文案),宏侧转成 compile_error。
// ★ 同模块并存的边界:legacy-first 只为支撑【不同业务模块的渐进
// 迁移】(如 nasa 只开 ws + 旧 hystrix)。**同一模块**新旧入口并存(nasa features 含
// hystrix + 直接依赖 hystrix)仅当两条边解析到**同一 source**(同一 path/同一 registry
// 版本)时被 Cargo 合并为单实例;跨 source(registry 版 nasa + path 版旧依赖)会:
// ① 同版本 → lockfile package collision 拒绝构建;② 不同版本 → 双实例,REGISTRY/缓存
// 注册表/连接池/任务表等 crate 级全局状态分裂。**禁止同模块混用**,流水线应做
// "feature × 直接依赖"配对检查,并结合 `cargo tree -d` 拦截重复实例。
//
// 第三方 crate(axum/tokio/linkme/tracing)不在此解析:宏生成
// `<运行时根>::__private::<第三方>`,由各运行时 crate 的 #[doc(hidden)] __private 桥接
// (业务无需为宏展开细节添加 linkme 等依赖)。
// ============================================================================

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

/// 业务作用：构造 [`HandlerWrapper`];签名不适合内层包装时返回 `None`,由调用方退回旧展开形态。
///
/// 返回 `None` 的情形:带 `self` 接收者、带泛型或 where 子句、可变参数、参数上有属性
/// (这些形态无法把参数列表机械拆成"外层透传 + 内层原模式")。
///
/// # 参数
/// - `sig`: 被注解函数的原签名(调用方改写返回类型**之前**的那一份)。
/// - `block`: 被注解函数的原函数体。
/// - `inner_ident`: 内层 `async fn` 的名字,应带宏前缀避免与业务标识符相撞。
/// - `arg_prefix`: 外层参数名前缀,同样应带宏前缀。
///
/// 返回：签名可安全拆分时返回内外层包装产物；不支持的签名形态返回 `None`。
pub fn wrap_handler(
    sig: &syn::Signature,
    block: &syn::Block,
    inner_ident: &syn::Ident,
    arg_prefix: &str,
) -> Option<HandlerWrapper> {
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
            /// 参数说明：参数沿用被包装处理函数的业务输入声明。
            /// 返回：原处理函数的执行结果，不改变其错误类型。
            async fn #inner_ident(#inner_inputs) #inner_output #block
        },
        call_inner: quote! { #inner_ident(#(#call_args),*).await },
    })
}

/// 业务作用：兼容性包装：查询调用方是否依赖指定运行时包。
///
/// # 参数
/// - `package`: Cargo 包名，例如 `hystrix`、`cacheable`、`natx` 或 `naweb`。
///
/// 返回：能够解析时返回 Cargo 识别的 crate 身份，否则返回 `None`。
fn crate_name_compat(package: &str) -> Option<proc_macro_crate::FoundCrate> {
    proc_macro_crate::crate_name(package).ok()
}

/// 业务作用：解析某个宏的**运行时根路径**(旧直接依赖优先,门面回退;见文件头)。
///
/// - `module`:门面下的模块名(`hystrix`/`cache`/`tx`/`scheduling`);
/// - `legacy`:直接依赖的包名(`hystrix`/`cacheable`/`natx`/`nasched`);
/// - 返回 `Ok(路径 tokens)`,如 `::hystrix`、`::nasa::hystrix`、`::company_nasa::hystrix`、
///   `crate`(在运行时 crate 自身内展开时);
/// - `Err(提示文案)`:两类依赖都找不到,调用方应整体替换为 `compile_error!`。
///
/// # 参数
///
/// - `module`: 门面 crate 下的模块名，例如 `hystrix`、`cache`、`tx` 或 `scheduling`。
/// - `legacy`: 直接依赖的包名，用于兼容未走门面导出的调用方。
///
/// 返回：直接运行时或门面模块的可解析路径；两类依赖均不存在时返回编译提示。
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
/// 参数说明：`legacy` 是直接运行时的 Cargo 包名。
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

/// Web mapping 宏的三态解析：优先使用直接 naweb 依赖，其次使用 nasa::web 门面；
/// 仅直接依赖 naweb-macro 时走裸 axum/linkme/tracing 路径。
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
