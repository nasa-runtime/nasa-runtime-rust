//! 缓存读写属性宏。
//!
//! `#[cached]` 生成读路径缓存编排，`#[cache_invalidate]` 生成写后失效逻辑；
//! 真正的缓存存储和一致性控制由 `cacheable` 运行时执行。
// ============================================================================
// cache-macro —— Cacheable-lite 的两个属性宏
//   #[cached(scene, key, refresh_ms, expire_ms)]      读:L1(moka)→L2(Redis三防)→原体(DB)
//   #[cache_invalidate(scene, key, value)]            写:执行原体后,失效 L1+L2
//
// 设计见 rust-cache/00-Cacheable-lite设计.md。宏只负责"拼 key + 调运行期 helper",
// 真正的两级缓存逻辑在门面 crate ::cacheable(运行期模块)。
//
// 三类过程宏里这俩都是【attribute 属性宏】,编译期把被注解的 fn 改写。
// ============================================================================

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitInt, LitStr};

// ════════════════════════════════════════════════════════════════════════════
// #[cached] —— 读路径:两级缓存
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：`#[cached]` 为异步读取函数生成 L1/L2 两级缓存编排。
///
/// 贴在【async 方法/函数】上,改写成"先查 L1(moka)→ L2(Redis 三防)→ 未命中才跑原体(DB)"。
/// 原体返回 `anyhow::Result<T>`,本注解透传同一个 `T`(签名/返回类型不变)。
///
/// ## 参数
/// | 参数 | 类型 | 默认 | 含义 |
/// |---|---|---|---|
/// | `scene` | 字符串 | ✅必填 | L1 缓存池名(按 scene 分池)。★**同一 scene 的值类型 V 必须始终一致**(混用 Vec/Option 会 panic) |
/// | `key` | 字符串 | ✅必填 | key 模板,`{xxx}` 内联捕获**同名函数参数**(仅参数名,不支持 `{a.b}` 字段表达式) |
/// | `refresh_ms` | 整数 | 1000 | L1 软刷新阈值(到点返旧值 + 后台刷) |
/// | `expire_ms` | 整数 | 3000 | L1 硬过期(到点淘汰) |
///
/// ## 用法示例
/// ```ignore
/// #[cached(scene = "kline", key = "kline:{symbol}:{period}", refresh_ms = 1000, expire_ms = 3000)]
/// pub async fn find_range(self: Arc<Self>, symbol: String, period: u8) -> anyhow::Result<Vec<KLine>> { /* ... */ }
/// ```
/// ⚠️ 用 `self` 必须取 `self: Arc<Self>`(不能 `&self`):原体会作 loader 丢进 `tokio::spawn` 后台刷新,
///    要求 future 是 `Send + 'static`,借用 `&self` 不满足。取 `Arc<Self>` + 参数取自有值,`async move` 全按值捕获。
///
/// # 参数
///
/// - `attr`: `#[cached(...)]` 括号内的 scene/key/TTL 配置 token stream。
/// - `item`: 被注解的 async 函数 token stream。
#[proc_macro_attribute]
pub fn cached(attr: TokenStream, item: TokenStream) -> TokenStream {
    // ⓪ 运行时根路径发现:nasa → ::nasa::cache;旧依赖 → ::cacheable。
    let root = match nasa_macro_support::runtime_root("cache", "cacheable") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    // ── 1. 解析注解参数(同 hystrix-macro 的 syn::meta::parser 套路) ──
    // 1a. 准备接收变量 + 默认值
    let mut scene: Option<String> = None; // 必填:L1 池名
    let mut key: Option<String> = None; // 必填:key 模板
    let mut refresh_ms: u64 = 1000; // L1 软刷新
    let mut expire_ms: u64 = 3000; // L1 硬过期
                                   // 1b. 定义每个 k=v 怎么取(闭包按引用捕获上面变量),此刻还没解析
    let arg_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("scene") {
            scene = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("key") {
            key = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("refresh_ms") {
            refresh_ms = meta.value()?.parse::<LitInt>()?.base10_parse()?;
        } else if meta.path.is_ident("expire_ms") {
            expire_ms = meta.value()?.parse::<LitInt>()?.base10_parse()?;
        } else {
            return Err(meta.error("#[cached]: 只支持 scene / key / refresh_ms / expire_ms"));
        }
        Ok(())
    });
    // 1c. 真正在 attr 上跑解析,把值填进变量(出错→编译错误)
    parse_macro_input!(attr with arg_parser);

    // scene/key 必填:缺了直接产出一个编译错误返回
    let scene = match scene {
        Some(s) => s,
        None => return compile_err("#[cached]: 缺少必填参数 scene"),
    };
    let key = match key {
        Some(k) => k,
        None => return compile_err("#[cached]: 缺少必填参数 key"),
    };

    // ── 2. 解析被注解的函数,取部件 ──
    let func = parse_macro_input!(item as ItemFn);
    // 形态校验(对齐 #[Async] 的前置守卫,把底层报错前移成清晰提示):
    //   ① 必须 async fn:展开体尾是 `get_or_load_2level(...).await`,贴到同步 fn 上只会报"await 不在 async 上下文"。
    //   ② 拒绝 &self / &mut self:原体被当 loader 丢进 L1 后台刷新 spawn(get_or_load_2level 要求 loader Send+'static),
    //      借用接收者满足不了(本注解与 #[cache_invalidate] 不同——后者内联 await,&self 合法)。
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&func.sig.ident, "#[cached] 只能用于 async fn")
            .to_compile_error()
            .into();
    }
    if let Some(syn::FnArg::Receiver(r)) = func.sig.inputs.first() {
        if r.reference.is_some() {
            return syn::Error::new_spanned(
                r,
                "#[cached] 不能用于 &self / &mut self(原体会进 L1 后台刷新 spawn,要 Send + 'static);请改用 self: Arc<Self> 或自由函数",
            )
            .to_compile_error()
            .into();
        }
    }
    let attrs = &func.attrs; // 函数头其它属性,原样保留
    let vis = &func.vis; // 可见性
    let sig = &func.sig; // 签名【完全不变】(返回类型也不变,#[cached] 只在原体外包一层)
    let block = &func.block; // 原函数体

    // ── 2b. CacheSceneUsage 收集元信息:从返回类型 `-> ...Result<T>` 提取缓存值类型 T,
    //        额外生成一条静态 descriptor(不改写业务函数),由 linkme 编译期收集供启动断言。
    let value_ty = extract_cached_value_type(&func.sig.output);
    let handler_name = func.sig.ident.to_string();
    let usage_ident = quote::format_ident!("__CACHE_SCENE_USAGE_{}", func.sig.ident);

    // ── 3. 生成新函数 + 收集 descriptor ──
    // descriptor 必须位于函数体内：属性宏无法从输入 token 判断自己处于 module 还是 impl，
    // 若在外层展开 static，方法场景会把它变成 Rust 禁止的 associated static。局部 static 仍是
    // linkme 可保留的静态项，同时对自由函数和 impl 方法使用同一展开形态。
    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #[#root::__private::linkme::distributed_slice(#root::CACHE_SCENE_USAGES)]
            #[linkme(crate = #root::__private::linkme)]
            #[doc(hidden)]
            #[allow(non_upper_case_globals)]
            static #usage_ident: #root::CacheSceneUsage = #root::CacheSceneUsage {
                scene: #scene,
                value_type_name: ::core::stringify!(#value_ty),
                value_type_id: || ::core::any::TypeId::of::<#value_ty>(),
                refresh_ms: #refresh_ms,
                expire_ms: #expire_ms,
                handler: #handler_name,
            };

            // ① 拼 key:把模板字符串交给 format!,靠 Rust 内联格式参数捕获同名【函数参数】。
            //    例如 key="kline:{symbol}:{period}" → format!("kline:{symbol}:{period}") 抓局部 symbol/period。
            let __cache_key = ::std::format!(#key);
            // ② 调运行期 helper:L1(local_cache)→L2(CacheLayer 三防)→原体(DB)。
            //    `move || async move #block`:外层闭包+内层 async 块都按值捕获 self/参数 →
            //    loader 的 future 是 Send+'static,满足 L1 后台刷新对 loader 的要求(见上方约束)。
            //    原体返回 anyhow::Result<T>,helper 透传同一个 T(故本函数返回类型保持不变)。
            #root::get_or_load_2level(
                #scene,
                __cache_key,
                #refresh_ms,
                #expire_ms,
                move || async move #block,
            ).await
        }
    };
    expanded.into()
}

// ════════════════════════════════════════════════════════════════════════════
// #[cache_invalidate] —— 写路径:执行原体后失效 L1+L2
// ════════════════════════════════════════════════════════════════════════════
/// 业务作用：`#[cache_invalidate]` 在异步写操作完成后触发 L1/L2 缓存失效。
///
/// 贴在【写操作 async 方法/函数】上:先跑原体(写 DB),再失效对应 key 的 L1 + L2。
/// 失效是尽力而为(失败仅告警,不影响写结果);返回类型不变。
///
/// ## 参数
/// | 参数 | 类型 | 必填 | 含义 |
/// |---|---|---|---|
/// | `scene` | 字符串 | ✅必填 | 同 `#[cached]`,要和被失效的那个 `#[cached]` 用**同一 scene** |
/// | `key` | 字符串 | ✅必填 | 同 `#[cached]` 的 key 模板,`{xxx}` 捕获同名函数参数 |
/// | `value` | 字符串 | ✅必填 | 该 scene 的**值类型**(如 `"Vec<KLine>"`)。L1 按 `<K,V>` 强类型分池,失效要用相同 V 才能定位到池 |
///
/// ## 用法示例
/// ```ignore
/// #[cache_invalidate(scene = "kline", key = "kline:{symbol}:{period}", value = "Vec<KLine>")]
/// pub async fn save(&self, symbol: String, period: u8, rows: Vec<KLine>) -> anyhow::Result<()> { /* 写 DB */ }
/// ```
///
/// # 参数
///
/// - `attr`: `#[cache_invalidate(...)]` 括号内的 scene/key/value 配置 token stream。
/// - `item`: 被注解的 async 写函数 token stream。
#[proc_macro_attribute]
pub fn cache_invalidate(attr: TokenStream, item: TokenStream) -> TokenStream {
    // ⓪ 运行时根路径发现(同 #[cached])。
    let root = match nasa_macro_support::runtime_root("cache", "cacheable") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    // ── 1. 解析参数 ──
    let mut scene: Option<String> = None;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None; // 值类型的字符串,如 "Vec<KLine>"
    let arg_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("scene") {
            scene = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("key") {
            key = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("value") {
            value = Some(meta.value()?.parse::<LitStr>()?.value());
        } else {
            return Err(meta.error("#[cache_invalidate]: 只支持 scene / key / value"));
        }
        Ok(())
    });
    parse_macro_input!(attr with arg_parser);

    let scene = match scene {
        Some(s) => s,
        None => return compile_err("#[cache_invalidate]: 缺少必填参数 scene"),
    };
    let key = match key {
        Some(k) => k,
        None => return compile_err("#[cache_invalidate]: 缺少必填参数 key"),
    };
    let value = match value {
        Some(v) => v,
        None => {
            return compile_err(
                "#[cache_invalidate]: 缺少必填参数 value(该 scene 的值类型,如 \"Vec<KLine>\")",
            )
        }
    };
    // 把 value 字符串解析成"类型"语法节点,后面 invalidate::<#valty> 用
    let valty: syn::Type = match syn::parse_str(&value) {
        Ok(t) => t,
        Err(_) => return compile_err("#[cache_invalidate]: value 不是合法类型"),
    };

    // ── 2. 解析函数 ──
    let func = parse_macro_input!(item as ItemFn);
    // 形态校验:必须 async fn(展开体含 `(async move {..}).await` 与 `invalidate(..).await`,同步 fn 会报 await 不在 async 上下文)。
    // 本注解内联等待原函数体，不会把接收者移入后台任务，因此允许 `&self` / `&mut self`。
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&func.sig.ident, "#[cache_invalidate] 只能用于 async fn")
            .to_compile_error()
            .into();
    }
    let attrs = &func.attrs;
    let vis = &func.vis;
    let sig = &func.sig; // 返回类型不变
    let block = &func.block;

    // ── 3. 生成:先拼 key(趁参数还在)→ 跑原体 → 失效 → 返回原体结果 ──
    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            // ① 先拼 key:此时参数还没被原体 move 走,format! 借用它们拼好 key
            let __cache_key = ::std::format!(#key);
            // ② 跑原体(写 DB)。用 async 块隔离原体的 return:无论原体怎么早返回,
            //    结果都落到 __r,保证下面的失效一定会执行(再把 __r 作为最终返回值)。
            let __r = (async move #block).await;
            // ③ 失效 L1+L2(尽力而为:失效失败不影响写操作的结果,仅告警)。
            //    value 类型用于 L1 按 <String, #valty> 池失效。
            if let Err(e) = #root::invalidate::<#valty>(#scene, __cache_key).await {
                // tracing 经运行时 __private 桥引用:业务无需直接依赖 tracing。
                #root::__private::tracing::warn!("cache_invalidate 失效失败(scene={}): {}", #scene, e);
            }
            // ④ 返回原体结果
            __r
        }
    };
    expanded.into()
}

// 工具:把一句话变成"编译错误" token(用于参数缺失等),返回给编译器即在注解处报错。
/// 业务作用：工具:把一句话变成"编译错误" token(用于参数缺失等),返回给编译器即在注解处报错。
/// # 参数
/// - `msg`: 业务消息体或事件载荷。
fn compile_err(msg: &str) -> TokenStream {
    quote! { ::core::compile_error!(#msg); }.into()
}

/// 业务作用：从 `#[cached]` 的返回类型 `-> ...Result<T>` 提取缓存值类型 `T`(供 CacheSceneUsage)。
///
/// 取返回类型最外层路径(如 `anyhow::Result` / `Result`)的第一个泛型实参作为 `T`;非该形态时回退用
/// 整个返回类型(仍能编译并收集,只是类型不够精确);`-> ()` 回退为 unit。
fn extract_cached_value_type(output: &syn::ReturnType) -> syn::Type {
    if let syn::ReturnType::Type(_, ty) = output {
        if let syn::Type::Path(type_path) = &**ty {
            if let Some(last) = type_path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return inner.clone();
                    }
                }
            }
        }
        return (**ty).clone();
    }
    syn::parse_quote!(())
}
