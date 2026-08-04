//! ambient 事务属性宏。
//!
//! `#[transactional]` 在编译期包装 async 函数体，让业务方法自动进入 `natx` 事务运行时。
// ============================================================================
// tx-macro —— 属性宏 #[transactional](ambient 事务编排)
//
// 作用:把被注解的 async fn 的【函数体】包进 `crate::tx::run(async move { ... }).await`:
//   · 进入时 begin 一个事务(若外层已在事务中则【加入】,不另开 —— 嵌套复用外层事务)
//   · 函数体正常返回 Ok → commit
//   · 函数体返回 Err(或 ? 上抛) → rollback
//
// 它只负责【编排】(begin/commit/rollback 样板);函数体里嵌套调用的 repo 怎么自动用上同一个
// 事务连接,是 src/tx.rs 用 task_local ambient 事务做的(repo 走 tx::conn() 取"当前连接")。
//
// 约束:被注解函数的返回类型必须是 `anyhow::Result<T>`(run 的返回类型与之对齐)。
// 三类过程宏里这是【attribute 属性宏】。
// ============================================================================

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{parse_macro_input, ItemFn, LitStr};

/// 业务作用：`#[transactional]` —— 给 async fn 套上 ambient 事务编排。
///
/// 进入时 begin、正常返回 Ok → commit、返回 Err(或 `?` 上抛)→ rollback。
/// 函数体里嵌套调用的 repo【自动用同一个事务连接】(靠 `src/tx.rs` 的 task_local ambient 事务,
/// repo 走 `tx::conn()` 取"当前连接"——有事务用事务、没有用池)。约束:返回类型须为 `anyhow::Result<T>`。
///
/// # 嵌套调用(a 调 b,a、b 都 #[transactional])—— 默认就对
/// 嵌套复用:a 未提交时调 b,b 检测到"已在事务中"→【不另开】,直接加入 a 的事务,最后 a 统一 commit;
/// b 出错 → 整体回滚。**这是想要的行为**。注意:不要"先让 a commit 再调 b"——那就拆成两个独立事务了。
///
/// # ⚠️ repo 必须用 `tx::conn()` 取连接,否则不进事务
/// 被本注解的流程里,repo 的连接【必须】走 `crate::tx::conn().await`(它会自动用上当前事务连接)。
/// 若 repo 用的是 `&self.pool`(struct 字段那种,如原版 `KLineRepository`),它会【另取一条池连接、
/// 绕过事务】——写入不在事务内、回滚时不会撤销(静默脏写)。详见 `tx::conn()` 的文档表格。
///
/// # ⚠️ 三条使用纪律(ambient 实现的代价,务必看)
/// 1. **同一事务内绝不并发查询**(别 `tokio::join!` 两个都查库的调用)——事务=单连接,物理上也并发不了。
/// 2. **绝不在持有一个 `conn()` 句柄时,再去调用也要 `conn()` 的代码**——`tx::conn()` 用的是非重入
///    `tokio::Mutex`,嵌套获取会【死锁】。正确:一条 query 取-用-丢(让 `Conn` 立刻离开作用域),再下一条。
/// 3. 事务函数保持短小、命名点明,弥补"签名看不出在事务里"的可读性损失。
///
/// # 第 2 条什么时候真会咬人?
/// - **常规分层(service→repo 的 CRUD)不会咬**:`#[transactional]` 的 service 只编排、【不直接写 `conn()`】;
///   `conn()` 只在最底层 repo 的一条自包含查询里"取-用-丢"。所以 a 调 b 时谁都没攥着 conn → 安全。
///   死锁 ≠ "a 没 commit",而是 = "调别人时手里还攥着一个活的 conn 句柄"。常规写法不会出现。
/// - **唯一现实陷阱:repo 里【流式游标 fetch 边读边写】**——`fetch()` 返回的流会长时间攥着连接,
///   这期间又对每行调另一个 `conn()` 的写操作 → 死锁。示意:
///   ```ignore
///   let mut c = tx::conn().await?;
///   let mut rows = sqlx::query("SELECT id FROM t").fetch(c.as_mut()); // c 被流借住到读完
///   while let Some(row) = rows.next().await {
///       other_repo::update(id).await?;  // ❌ c 还攥着 → update 内部 conn() 死锁
///   }
///   ```
///
/// # 想彻底免疫?用【显式 `&mut tx`】(生产推荐)
/// 不用本宏,改成把事务当参数显式传:
/// ```ignore
/// let mut tx = pool.begin().await?;
/// repo::insert_e(&mut *tx, ..).await?;   // 显式传;a 调 b 就 b(&mut *tx)
/// tx.commit().await?;                    // 出错 ? 上抛 → tx drop 自动 ROLLBACK
/// ```
/// 上面那个"流式边读边写"的死锁,显式版会变成【编译错误】(不能同时两次 `&mut *tx`)——
/// 同一个"单连接不能同时干两件事"的限制,借用检查器在编译期就拦住了。详见
/// 展开代码遵守 ambient transaction 传播边界，避免生成调用绕过当前事务上下文。
///
/// # 参数
///
/// - `attr`: `#[transactional(...)]` 括号内的 token stream；为空表示默认 datasource,
///   也可写 `datasource = "reporting"`。
/// - `item`: 被注解的 async 函数 token stream。
#[proc_macro_attribute]
pub fn transactional(attr: TokenStream, item: TokenStream) -> TokenStream {
    // ⓪ 运行时根路径发现:业务依赖 `nasa` → `::nasa::tx`(含重命名);
    //    旧直接依赖 `tx` → `::tx`;都没有 → 编译错误。
    let root = match nasa_macro_support::runtime_root("tx", "natx") {
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
    let run_call = if let Some(datasource) = datasource {
        quote! { #root::run_for(#datasource, async move #block).await }
    } else {
        quote! { #root::run(async move #block).await }
    };
    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #run_call
        }
    };
    expanded.into()
}

/// 业务作用：解析 `#[transactional]` 属性参数。
///
/// 支持三种写法：空参数表示默认 datasource，字符串字面量表示兼容短写，
/// `datasource = "..."` 表示显式命名 datasource。其它参数一律编译期报错。
///
/// # 参数
/// - `attr`: 属性括号内的 token stream。
fn parse_transactional_attr(attr: TokenStream) -> Result<Option<LitStr>, proc_macro2::TokenStream> {
    if attr.is_empty() {
        return Ok(None);
    }

    if let Ok(lit) = syn::parse::<LitStr>(attr.clone()) {
        return validate_datasource_lit(lit).map(Some);
    }

    // 使用 syn 的 meta parser 保留准确 span，业务写错参数时能指到属性本身。
    let mut datasource = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("datasource") {
            if datasource.is_some() {
                return Err(meta.error("transactional datasource 不能重复"));
            }
            let lit = meta.value()?.parse::<LitStr>()?;
            datasource = Some(lit);
            Ok(())
        } else {
            Err(meta.error("transactional 只支持 datasource = \"...\" 参数"))
        }
    });
    if let Err(err) = Parser::parse(parser, attr) {
        return Err(err.to_compile_error());
    }
    let Some(datasource) = datasource else {
        return Err(quote! {
            ::core::compile_error!("transactional 参数不能为空;请使用 #[transactional] 或 #[transactional(datasource = \"...\")]");
        });
    };
    validate_datasource_lit(datasource).map(Some)
}

/// 业务作用：校验 datasource 字面量是否能安全传给运行时。
///
/// # 参数
/// - `lit`: 属性中解析出的 datasource 字符串字面量。
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
