//! 缓存读写属性宏。
//!
//! `#[cached]` 生成读路径缓存编排，`#[cache_invalidate]` 生成写后失效逻辑；
//! 真正的缓存存储和一致性控制由 `cacheable` 运行时执行。

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitInt, LitStr};

/// 业务作用：`#[cached]` 为异步读取函数生成 L1/L2 两级缓存编排。
///
/// 运行时先读取 L1，配置共享缓存时再读取 L2，未命中才执行原函数体。
/// 原体须返回 `anyhow::Result<T>`，缓存值与加载闭包须满足运行时的类型及生命周期约束。
///
/// 属性配置：
/// | 参数 | 类型 | 默认 | 含义 |
/// |---|---|---|---|
/// | `scene` | 字符串 | 必填 | L1 场景名，同场景必须使用相同值类型与 TTL 配置 |
/// | `key` | 字符串 | 必填 | `format!` 模板，以 `{name}` 捕获可见参数名，不支持 `{a.b}` 字段表达式 |
/// | `refresh_ms` | 整数 | 1000 | L1 软刷新阈值(到点返旧值 + 后台刷) |
/// | `expire_ms` | 整数 | 3000 | L1 硬过期(到点淘汰) |
///
/// 使用示例：
/// ```ignore
/// #[cached(scene = "resource", key = "resource:{id}")]
/// async fn load(id: u64) -> anyhow::Result<String> {
///     fetch_resource(id).await
/// }
/// ```
/// 加载闭包可能参与后台刷新，不能借用短于任务生命周期的 `self` 或参数。
/// 宏拒绝引用接收者；按值接收者仍须满足缓存运行时要求。
///
/// 参数说明：
/// - `attr`：属性内的场景名、键模板及 TTL 配置令牌。
/// - `item`：被包装的异步函数或方法声明。
///
/// 返回：保留签名并注册场景类型描述的缓存包装函数；未知属性、缺少必填项、
/// 非异步函数或引用接收者生成编译错误，值类型约束由生成代码与运行时共同检查。
#[proc_macro_attribute]
pub fn cached(attr: TokenStream, item: TokenStream) -> TokenStream {
    // 按调用方的实际依赖身份选择运行时路径，兼容直接依赖、门面依赖及依赖重命名。
    let root = match nasa_macro_support::runtime_root("cache", "cacheable") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    let mut scene: Option<String> = None; // 必填:L1 池名
    let mut key: Option<String> = None; // 必填:key 模板
    let mut refresh_ms: u64 = 1000; // L1 软刷新
    let mut expire_ms: u64 = 3000; // L1 硬过期
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

    let func = parse_macro_input!(item as ItemFn);
    // 在生成等待与后台加载代码前验证函数形态，避免不满足生命周期要求的接收者进入包装：
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
    let attrs = &func.attrs;
    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;

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

/// 业务作用：`#[cache_invalidate]` 在异步写操作完成后触发 L1/L2 缓存失效。
///
/// 原函数体返回后立即尝试失效，包括原体返回错误的情况；不等待外层事务提交。
/// 失效错误仅记录告警并保留原结果，任务取消或原体 panic 不保证执行失效。
/// 需要提交后持久投递失效意图的场景应使用运行时的事务写入接口。
///
/// 属性配置：
/// | 参数 | 类型 | 必填 | 含义 |
/// |---|---|---|---|
/// | `scene` | 字符串 | ✅必填 | 同 `#[cached]`,要和被失效的那个 `#[cached]` 用**同一 scene** |
/// | `key` | 字符串 | ✅必填 | 同 `#[cached]` 的 key 模板,`{xxx}` 捕获同名函数参数 |
/// | `value` | 字符串 | 必填 | 场景值类型，传给运行时泛型入口；实际删除按场景名与键执行 |
///
/// 使用示例：
/// ```ignore
/// #[cache_invalidate(scene = "resource", key = "resource:{id}", value = "String")]
/// async fn save(id: u64, value: String) -> anyhow::Result<()> {
///     store_resource(id, value).await
/// }
/// ```
///
/// 参数说明：
/// - `attr`：属性内的场景名、键模板与值类型配置令牌。
/// - `item`：被包装的异步写函数或方法声明，允许引用接收者。
///
/// 返回：先构造缓存键、再执行原体、最后尝试失效的函数；保留原签名与返回值。
/// 属性非法、缺少必填项、值类型无法解析或函数非异步时生成编译错误。
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
            // 将原体的 return 限于内层异步块，正常返回错误值时也继续失效；取消或 panic 不在此保证内。
            let __r = (async move #block).await;
            // ③ 失效 L1+L2(尽力而为:失效失败不影响写操作的结果,仅告警)。
            //    泛型值类型保留入口约束，运行时按场景与键执行删除。
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

/// 业务作用：将无效缓存属性转换为编译期拒绝，阻止生成配置不完整的包装函数。
///
/// 参数说明：
/// - `msg`：向编译器输出的配置错误说明，不是业务消息载荷。
///
/// 返回：调用 `compile_error!` 的令牌流，由编译器终止该声明的编译。
fn compile_err(msg: &str) -> TokenStream {
    quote! { ::core::compile_error!(#msg); }.into()
}

/// 业务作用：从 `#[cached]` 的返回类型 `-> ...Result<T>` 提取缓存值类型 `T`(供 CacheSceneUsage)。
///
/// 参数说明：
/// - `output`：原函数返回类型声明，预期为 `anyhow::Result<T>`。
///
/// 返回：末段路径的首个类型实参；无此实参时保留整个返回类型，未声明返回类型时为 unit。
/// 此处仅按语法提取，不验证类型是否为 `Result`；不合运行时约束的类型仍可能无法编译。
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
