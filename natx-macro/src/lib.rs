//! ambient 事务属性宏。
//!
//! `#[transactional]` 在编译期包装 async 函数体，导入路径固定选择 MySQL、PostgreSQL 或 SQLite。
//! 宏将业务 future 交给对应 typed runtime；datasource 配置必须匹配该 driver，不按 URL 改写后端。
//! SQLite 支持 Deferred/Immediate，嵌套调用继承外层模式或拒绝显式冲突；不提供跨库原子事务。
//! napp 的 `db` 组件拥有受管来源，宏不另建连接池，也不把 after-commit 当作持久消息投递。

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{parse_macro_input, ItemFn, LitStr};

/// 业务作用：为 MySQL async 业务函数生成同源环境事务编排。
/// 参数说明：
/// - `attr`：空参数、datasource 名称或 never 传播声明。
/// - `item`：返回值兼容 anyhow::Result 的 async 函数。
/// 返回：保留签名并包装函数体；非法参数或函数形式产生编译错误。
///
/// 根调用由运行时开始并收口事务，嵌套调用加入当前同源事务；业务错误交由运行时裁决回滚。
/// Mapper 或低层查询必须通过同一 typed runtime 的连接入口，另取裸池连接的写入不属于该事务。
/// 同一事务只借用一个连接槽，连接或结果流未释放时再次借用会被拒绝；应先结束当前读取再执行后续 SQL。
/// task-local 不跨 spawn 传播，after_commit 只有在明确提交后执行，不能代替持久消息投递。
#[proc_macro_attribute]
pub fn transactional(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_transactional(
        attr,
        item,
        nasa_macro_support::runtime_root("tx", "natx"),
        None,
    )
}

/// 业务作用：为 PostgreSQL async 业务函数生成固定指向 `natx-pgsql` 或 `nasa::tx::pgsql` 的事务编排。
///
/// 参数说明：
/// - `attr`：空参数或命名 datasource 参数。
/// - `item`：被包装的 async 函数。
///
/// 返回：保持原签名、仅替换函数体的过程宏展开结果；依赖或参数非法时生成编译错误。
#[proc_macro_attribute]
pub fn transactional_pgsql(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_transactional(
        attr,
        item,
        nasa_macro_support::runtime_root_nested(&["tx", "pgsql"], "tx-pgsql", "natx-pgsql"),
        None,
    )
}

/// 业务作用：让 SQLite 业务函数进入同源本地事务，并显式选择外层锁策略。
/// 参数说明：
/// - `attr`：datasource、mode 或 never 传播声明。
/// - `item`：被包装的 async 函数。
/// 返回：保持签名的事务展开；非法模式、重复参数和互斥声明产生编译错误。
#[proc_macro_attribute]
pub fn transactional_sqlite(attr: TokenStream, item: TokenStream) -> TokenStream {
    let root =
        nasa_macro_support::runtime_root_nested(&["tx", "sqlite"], "tx-sqlite", "natx-sqlite");
    if attr.is_empty() || syn::parse::<LitStr>(attr.clone()).is_ok() {
        return expand_transactional(attr, item, root, None);
    }
    let parser = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated;
    let attributes = match parser.parse(attr) {
        Ok(attributes) => attributes,
        Err(error) => return error.to_compile_error().into(),
    };
    let mut mode = None;
    let mut remaining = Vec::new();
    for attribute in attributes {
        if attribute.path().is_ident("mode") {
            let parsed = match &attribute {
                syn::Meta::NameValue(value) => match &value.value {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(value),
                        ..
                    }) => Some(value.value()),
                    _ => None,
                },
                _ => None,
            };
            if mode.is_some() || !matches!(parsed.as_deref(), Some("deferred" | "immediate")) {
                return syn::Error::new_spanned(
                    attribute,
                    "SQLite mode 必须唯一且为 deferred 或 immediate",
                )
                .to_compile_error()
                .into();
            }
            mode = parsed;
        } else {
            remaining.push(attribute);
        }
    }
    expand_transactional(quote!(#(#remaining),*).into(), item, root, mode.as_deref())
}

/// 业务作用：共享数据库事务宏的语法校验与函数体包装，按后端选择运行时路径和锁策略。
///
/// 参数说明：
/// - `attr`：属性参数 token。
/// - `item`：被包装函数 token。
/// - `runtime_root`：对应 typed runtime 的已解析路径或依赖提示。
/// - `sqlite_mode`：SQLite 显式锁策略，其它 driver 不传入。
///
/// 返回：可由编译器继续处理的宏展开 token。
fn expand_transactional(
    attr: TokenStream,
    item: TokenStream,
    runtime_root: Result<proc_macro2::TokenStream, String>,
    sqlite_mode: Option<&str>,
) -> TokenStream {
    let root = match runtime_root {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };

    // ① 解析被注解的函数(源码语法树)
    let func = parse_macro_input!(item as ItemFn);
    let datasource = match parse_transactional_attr(attr) {
        Ok(datasource) => datasource,
        Err(err) => {
            return quote! {
                #err
                #func
            }
            .into()
        }
    };

    // 形态校验:必须 async fn —— 生成体里有 `.await`,非 async fn 会给出晦涩的底层错误,这里前置拦截给清晰提示。
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&func.sig.ident, "#[transactional] 只能用于 async fn")
            .to_compile_error()
            .into();
    }

    // ② 取出要原样搬过去的部件
    let attrs = &func.attrs; // 函数头上的其它属性(#[hystrix]、文档注释等)——保留
    let vis = &func.vis; // 可见性
    let sig = &func.sig; // 签名(含 async / 函数名 / 参数 / 返回类型)——【完全不动】
    let block = &func.block; // 原函数体 { ... }

    // ③ 生成新函数:签名照旧,函数体改成"把原体丢进 tx::run 跑"。
    //    `async move #block`:把原函数体当成一个 async 块,按 move 捕获所有参数 →
    //       它就是 run 需要的"事务内要执行的业务 future",输出 = 原返回类型 anyhow::Result<T>。
    //    #root = 解析出的运行时根(::nasa::tx / ::tx / crate),run 见 tx crate 的 src/lib.rs。
    let mode = sqlite_mode.map(|mode| match mode {
        "immediate" => quote!(#root::TransactionMode::Immediate),
        _ => quote!(#root::TransactionMode::Deferred),
    });
    if mode.is_some() && matches!(datasource, TransactionalMode::Never) {
        return syn::Error::new_spanned(&func.sig.ident, "SQLite never 与 mode 互斥")
            .to_compile_error()
            .into();
    }
    let run_call = match (datasource, mode) {
        // never:进入前拒绝 ambient 事务且不开启新事务——供"绝不能被外层事务包住"的路径
        // (如自治审计写、必须立即可见的对外副作用)把违规调用变成显式错误而不是静默入伙。
        (TransactionalMode::Never, _) => quote! { #root::run_never(async move #block).await },
        (TransactionalMode::Named(datasource), Some(mode)) => {
            quote! { #root::run_for_mode(#datasource, Some(#mode), async move #block).await }
        }
        (TransactionalMode::Default, Some(mode)) => {
            quote! { #root::run_for_mode("default", Some(#mode), async move #block).await }
        }
        (TransactionalMode::Named(datasource), None) => {
            quote! { #root::run_for(#datasource, async move #block).await }
        }
        (TransactionalMode::Default, None) => quote! { #root::run(async move #block).await },
    };
    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #run_call
        }
    };
    expanded.into()
}

/// `#[transactional]` 的编排模式；由属性参数在编译期唯一确定。
enum TransactionalMode {
    /// 默认 datasource 的 ambient 事务。
    Default,
    /// 显式命名 datasource 的 ambient 事务。
    Named(LitStr),
    /// 拒绝 ambient 事务且不开启事务。
    Never,
}

/// 业务作用：解析 `#[transactional]` 属性参数。
///
/// 支持四种写法：空参数表示默认 datasource，字符串字面量表示兼容短写，
/// `datasource = "..."` 表示显式命名 datasource，`never` 表示拒绝 ambient 事务且不开启事务。
/// 其它参数一律编译期报错；`never` 与 datasource 互斥(既拒绝事务又指定事务源自相矛盾)。
///
/// # 参数
/// - `attr`: 属性括号内的 token stream。
///
/// 返回：解析出的编排模式；非法语法返回编译错误。
fn parse_transactional_attr(
    attr: TokenStream,
) -> Result<TransactionalMode, proc_macro2::TokenStream> {
    if attr.is_empty() {
        return Ok(TransactionalMode::Default);
    }

    if let Ok(lit) = syn::parse::<LitStr>(attr.clone()) {
        return validate_datasource_lit(lit).map(TransactionalMode::Named);
    }

    // 使用 syn 的 meta parser 保留准确 span，业务写错参数时能指到属性本身。
    let mut datasource = None;
    let mut never = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("datasource") {
            if datasource.is_some() {
                return Err(meta.error("transactional datasource 不能重复"));
            }
            let lit = meta.value()?.parse::<LitStr>()?;
            datasource = Some(lit);
            Ok(())
        } else if meta.path.is_ident("never") {
            if never {
                return Err(meta.error("transactional never 不能重复"));
            }
            never = true;
            Ok(())
        } else {
            Err(meta.error("transactional 只支持 datasource = \"...\" 或 never 参数"))
        }
    });
    if let Err(err) = Parser::parse(parser, attr) {
        return Err(err.to_compile_error());
    }
    match (never, datasource) {
        (true, Some(_)) => Err(quote! {
            ::core::compile_error!("transactional never 与 datasource 互斥;never 表示拒绝进入任何事务");
        }),
        (true, None) => Ok(TransactionalMode::Never),
        (false, Some(datasource)) => {
            validate_datasource_lit(datasource).map(TransactionalMode::Named)
        }
        (false, None) => Err(quote! {
            ::core::compile_error!("transactional 参数不能为空;请使用 #[transactional]、#[transactional(never)] 或 #[transactional(datasource = \"...\")]");
        }),
    }
}

/// 业务作用：校验 datasource 字面量是否能安全传给运行时。
///
/// # 参数
/// - `lit`: 属性中解析出的 datasource 字符串字面量。
///
/// 返回：名称非空且无首尾空白时返回原字面量，否则返回编译错误。
fn validate_datasource_lit(lit: LitStr) -> Result<LitStr, proc_macro2::TokenStream> {
    let value = lit.value();
    if value.trim().is_empty() || value.trim() != value {
        Err(quote! {
            ::core::compile_error!("transactional datasource 不能为空,且首尾不能包含空白");
        })
    } else {
        Ok(lit)
    }
}
