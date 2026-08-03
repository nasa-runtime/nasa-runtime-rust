//! 路由级隔离、超时和监控属性宏。
//!
//! 宏在编译期把 async handler 包装进 `hystrix` 运行时命令，保留原函数签名，
//! 并把并发拒绝、超时降级和指标采集接入统一 Dashboard。
// ============================================================================
// hystrix-macro —— 自定义属性宏 #[hystrix(name=.., max_concurrent=.., timeout_ms=.., tps=.., reject_response=.., timeout_response=..)]
//
// 作用:贴在一个 axum async handler 上,【编译期】把它【改写】成
//   「先抢信号量(满→429)、再限时执行原逻辑(超时→504)」的版本。
//   = 注解驱动的 bulkhead + 超时,对照 src/hystrix.rs 的"显式中间件 Command::run"写法。
//
// ✅ 它【接入真正的 hystrix Command】:宏把"原函数体"包成闭包,交给 ::hystrix::Command::run_fn
//    执行。Command 在首次调用时构造并自动注册进全局 REGISTRY,所以注解版的 bulkhead/超时/成功率/延迟
//    会和 /heavy/slow、/spot/kline 一样【出现在 /hystrix.stream Dashboard 上】(一个名为注解里 name 的圈)。
//    —— 之前做成自带 Semaphore 不上 Dashboard,等于"隔离了但监控看不到",注解价值大打折扣;现已修正。
//    为此在 hystrix.rs【新增】了一个 run_fn 入口(run 重构成它的薄封装,原行为不变),没有破坏对照路由。
//
// 三类过程宏里这是【attribute 属性宏】(另两类:derive 派生宏、function-like 函数宏)。
// ============================================================================

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, ItemFn, LitInt, LitStr};

/// 业务作用：# `#[hystrix]` —— 给 axum handler 加 bulkhead 隔离 + 超时 + 监控(对标 Hystrix Command)
///
/// 贴在 async handler 上,编译期改写成"先抢信号量(满→429)、再限时执行(超时→504)"的版本,
/// 并把 QPS/延迟/并发/成功率上报 Dashboard(`/hystrix.stream`)。**所有参数全可省**。
///
/// ## 参数(全部可选)
/// | 参数 | 类型 | 默认 | 含义 |
/// |---|---|---|---|
/// | `name` | 字符串 | 见下 | Dashboard 圈名/日志名。不写时:若本 fn 下方还贴着 `#[get_mapping]`/`#[post_mapping]`(要求 `#[hystrix]` 在它【上面】)→ 自动取其 path;否则取函数名 |
/// | `max_concurrent` | 整数 | 不限并发 | 并发上限(信号量许可数)。不写 = 无 bulkhead、永不 429 |
/// | `timeout_ms` | 整数 | 不超时 | 单请求超时(毫秒)。不写 = 永不 504 |
/// | `tps` | 整数 | 0 | TPS 权重。0 = 不计入顶栏 TPS;>0 = 计入(weight=tps) |
/// | `reject_response` | JSON 字符串 | 默认 429 壳 | **自定义限流返回**:bulkhead 满时返回这段 JSON,以 **HTTP 200** 发出 |
/// | `timeout_response` | JSON 字符串 | 默认 504 壳 | **自定义超时返回**:超时时返回这段 JSON,以 **HTTP 200** 发出 |
/// | `reject_fn` | 路径 | — | **函数式限流降级**:bulkhead 满时调该【零参 async fn】(返回任意 `IntoResponse`,可自定义 status/header/body)。与 `reject_response` 互斥 |
/// | `timeout_fn` | 路径 | — | **函数式超时降级**:超时时调该【零参 async fn】。与 `timeout_response` 互斥 |
///
/// `reject_response`/`timeout_response` 的内容在【编译期】就会校验是否合法 JSON(写错直接编译报错)。
/// 不写则沿用默认壳(限流 429 / 超时 504,body 形如 `{"code":..,"message":..,"data":null}`)。
/// `reject_fn`/`timeout_fn` 用【裸路径】写(可跨模块/跨 crate,如 `crate::fallback::busy`),目标须是【零参 async fn → impl IntoResponse】;
/// 降级 fn 优先于静态串。互斥:`reject_fn` 与 `reject_response` 二选一、`timeout_fn` 与 `timeout_response` 二选一。
///
/// ## 用法示例
/// ```ignore
/// #[hystrix]                                              // 空参 = 只采集监控指标(不拦截、不限时)
/// #[hystrix(max_concurrent = 20, timeout_ms = 800)]       // bulkhead 20 并发 + 超时 800ms
/// #[hystrix(name = "下载", max_concurrent = 5, timeout_ms = 3000, tps = 1)]
///
/// // 自定义限流/超时返回(HTTP 200 + 你给的 JSON body):
/// #[hystrix(
///     max_concurrent = 20,
///     timeout_ms = 800,
///     reject_response = r#"{"code":429,"message":"系统繁忙,请稍后","data":null}"#,
///     timeout_response = r#"{"code":504,"message":"请求超时,请重试","data":null}"#
/// )]
///
/// // 函数式降级(零参 async fn,可自定义 status/header/body;可跨模块路径):
/// #[hystrix(name = "order", max_concurrent = 50, timeout_ms = 800,
///           reject_fn = crate::fb::busy, timeout_fn = crate::fb::late)]
/// async fn get_order(/* ... */) -> Json</* ... */> { /* ... */ }
///
/// #[hystrix(timeout_ms = 500)]   // ← 与 Mapping 同贴时 hystrix 必须在【上面】,会自动取下方 path 当 name
/// #[get_mapping("/spot/kline")]
/// pub async fn kline(/* ... */) -> Json</* ... */> { /* ... */ }
/// ```
///
/// # 参数
///
/// - `attr`: `#[hystrix(...)]` 括号内的隔离、超时、降级和监控配置 token stream。
/// - `item`: 被注解的 axum handler 函数 token stream。
#[proc_macro_attribute]
pub fn hystrix(attr: TokenStream, item: TokenStream) -> TokenStream {
    // ⓪ 运行时根路径发现:nasa → ::nasa::hystrix;旧依赖 → ::hystrix。
    let root = match nasa_macro_support::runtime_root("hystrix", "hystrix") {
        Ok(r) => r,
        Err(msg) => return quote! { ::core::compile_error!(#msg); }.into(),
    };
    // axum 经运行时 __private 桥引用:业务无需直接依赖 axum。
    let axum = quote! { #root::__private::axum };
    // ── 1. 解析注解里的参数：#[hystrix(name = "X", max_concurrent = N, timeout_ms = M)] ──
    //   形参 `attr` = 注解【括号里那串 token】，即 `name = "X", max_concurrent = N, timeout_ms = M`
    //   （`#[hystrix( )]` 外壳已被编译器剥掉）。下面分 1a/1b/1c 三步把它解析进变量。

    // 1a. 先准备"接收结果的变量"。max_concurrent / timeout_ms 用 Option：
    //   不写 → None → 不限并发 / 不超时（`#[hystrix()]` 空参 = 全 None = 只采集监控指标，不拦截不限时）。
    let mut name: Option<String> = None; // 缺省稍后取函数名 / 取 Mapping 的 path
    let mut max_concurrent: Option<usize> = None; // 不写 = 不限并发
    let mut timeout_ms: Option<u64> = None; // 不写 = 不超时
    let mut tps: u64 = 0; // TPS 权重：0=不计入顶栏 TPS；>0=计入(weight=tps)
    let mut reject_response: Option<String> = None; // 自定义限流返回(JSON 字符串)；不写=默认 429 壳
    let mut timeout_response: Option<String> = None; // 自定义超时返回(JSON 字符串)；不写=默认 504 壳
    let mut reject_fn: Option<syn::Path> = None; // 自定义限流降级 fn(路径)；与 reject_response 互斥
    let mut timeout_fn: Option<syn::Path> = None; // 自定义超时降级 fn(路径)；与 timeout_response 互斥

    // 1b. 定义"每遇到一个 k = v 该怎么处理"。
    //   ★ 注意：这一步【只是写好处理逻辑(一个闭包)】，被 syn::meta::parser 包装成一个"参数解析器(Parser)"——
    //      此刻【还没真正解析】，arg_parser 只是"规则"，不是"结果"。闭包会在 1c 执行时被【每个 k=v 调用一次】：
    //        meta.path                 = 等号左边的键；is_ident("name") 判断是不是 name 这个键
    //        meta.value()?             = 跳过 `=`，给出"等号右边值"的解析入口（`?`：语法不对就把错误上抛）
    //        .parse::<LitStr>()? / ::<LitInt>()? = 把右值按【字符串字面量 / 整数字面量】解析
    //        .value() / .base10_parse()?         = 从字面量里取出真正的 Rust String / 数字
    //      取到的值【写回 1a 那几个可变变量】（闭包按可变引用捕获了它们）。
    let arg_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("max_concurrent") {
            max_concurrent = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("timeout_ms") {
            timeout_ms = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
        } else if meta.path.is_ident("tps") {
            tps = meta.value()?.parse::<LitInt>()?.base10_parse()?;
        } else if meta.path.is_ident("reject_response") {
            reject_response = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("timeout_response") {
            timeout_response = Some(meta.value()?.parse::<LitStr>()?.value());
        } else if meta.path.is_ident("reject_fn") {
            // 裸路径(可跨模块/跨 crate):reject_fn = crate::fallback::busy
            reject_fn = Some(meta.value()?.parse::<syn::Path>()?);
        } else if meta.path.is_ident("timeout_fn") {
            timeout_fn = Some(meta.value()?.parse::<syn::Path>()?);
        } else {
            // 遇到不认识的键：返回 Err → 1c 会把它变成编译错误(指向注解处)
            return Err(meta.error(
                "#[hystrix]: 只支持 name / max_concurrent / timeout_ms / tps / reject_response / timeout_response / reject_fn / timeout_fn",
            ));
        }
        Ok(()) // 这一个 k=v 处理成功
    });

    // 1c. ★ 真正执行解析：把 arg_parser 这套规则【跑在 attr 这串 token 上】。
    //   它遍历 attr 里每个逗号分隔的 k=v，逐个调用 1b 的闭包 → 把值填进 name/max_concurrent/timeout_ms。
    //   若语法错误(或闭包返回 Err)，parse_macro_input! 会自动生成 compile_error! 并【提前 return 本宏】。
    //   所以——结果不是"存进 arg_parser"，而是这行执行后【那几个变量】里就是解析好的最终值。
    parse_macro_input!(attr with arg_parser);

    // 1d. 【编译期】校验自定义返回是合法 JSON —— 早报错，免得运行期首个请求才发现 JSON 写错。
    //     用 serde_json 试解析；失败就产出一个指向注解处的编译错误并提前返回。
    for (label, val) in [
        ("reject_response", &reject_response),
        ("timeout_response", &timeout_response),
    ] {
        if let Some(js) = val {
            if serde_json::from_str::<serde_json::Value>(js).is_err() {
                return syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!("#[hystrix]: {label} 不是合法 JSON: {js}"),
                )
                .to_compile_error()
                .into();
            }
        }
    }

    // 1e. 逐路径互斥:fn 与静态串只能给一个(职责重叠,避免歧义)。
    for (cond, msg) in [
        (
            reject_fn.is_some() && reject_response.is_some(),
            "#[hystrix]: reject_fn 与 reject_response 互斥(降级 fn 与静态返回只能给一个)",
        ),
        (
            timeout_fn.is_some() && timeout_response.is_some(),
            "#[hystrix]: timeout_fn 与 timeout_response 互斥(降级 fn 与静态返回只能给一个)",
        ),
    ] {
        if cond {
            return syn::Error::new(proc_macro2::Span::call_site(), msg)
                .to_compile_error()
                .into();
        }
    }

    // ── 2. 解析"被注解的那个函数",拆出它的各个部件(都是【源码语法树】，不是运行期对象) ──

    // 2a. 形参 `item` = 被 #[hystrix] 标注的整个函数的【源码 token】。
    //     parse_macro_input!(item as ItemFn) 把这串 token 解析成 syn 的"函数语法树"节点 ItemFn。
    //     之后 func.xxx 拿到的都是"源码的形状"(名字/可见性/参数/函数体)，能读、能重拼，但【不能在这里执行】。
    let func = parse_macro_input!(item as ItemFn);

    // 2a-2. 形态校验:必须 async fn。本宏生成的体里有 `.await`(run_fn(...).await),贴在同步 fn 上会让编译器报
    //   普通 `.await` 错误而非清晰的宏错误;故前置拦截,给明确提示。
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&func.sig.ident, "#[hystrix] 只能用于 async fn")
            .to_compile_error()
            .into();
    }

    // 2b. ★ 注解没显式给 name 时，先看本函数上是否【还】贴着 #[get_mapping("/x")] / #[post_mapping("/x")]
    //     / #[put_mapping] / #[delete_mapping] / #[patch_mapping]，
    //     有就把它的 path 当作 name（让 Dashboard 圈名 = 路由路径，省得再写一遍）。
    //
    //   为什么能看到 get_mapping？—— 属性宏【从上到下】展开：堆叠时【最上面的先展开】，它收到的 item
    //   里【还带着下面尚未展开的属性】。所以只要 #[hystrix] 写在 #[get_mapping] 【上面】，此刻 func.attrs
    //   里就还留着那条 #[get_mapping(...)]，能读到它的 path。（反过来 get_mapping 在上则它先展开、先被消耗，
    //   等 hystrix 展开时就看不到了——所以用法上要求 hystrix 在上。）
    //
    //   attr.path().is_ident("get_mapping")：判断这条属性的名字是不是 get/post/put/delete/patch mapping。
    //   attr.parse_args::<LitStr>()：把括号里的参数按"字符串字面量"解析（即 "/x"）；.value() 取出 String。
    if name.is_none() {
        for attr in &func.attrs {
            // 末段匹配:同时支持 #[get_mapping]、#[naweb::get_mapping]、
            // #[nasa::web::get_mapping] 及 Cargo 重命名后的多段路径——is_ident 只认单段,
            // 会让 Hystrix 取不到下方路由 path、退回函数名。
            if attr_name_is(attr, "get_mapping")
                || attr_name_is(attr, "post_mapping")
                || attr_name_is(attr, "put_mapping")
                || attr_name_is(attr, "delete_mapping")
                || attr_name_is(attr, "patch_mapping")
            {
                if let Some(p) = mapping_path(attr) {
                    name = Some(p);
                    break;
                }
            }
        }
    }

    // 2b-2. 仍没有 name（既没显式给、也没贴 Mapping 注解）→ 回退用"函数自己的名字"。
    //     func.sig.ident = 函数名这个标识符(token)；.to_string() 转成普通字符串放进日志/错误信息。
    let name = name.unwrap_or_else(|| func.sig.ident.to_string());

    // 2c. 取出生成新函数时要原样搬过去的几个部件(都是对 func 内部语法子树的【借用】)：
    let attrs = &func.attrs; // 函数头上的【其它】属性(如 #[allow(...)]、文档注释)——原样保留，别弄丢
    let vis = &func.vis; // 可见性(pub / pub(crate) / 私有)——保持和原函数一致
    let block = &func.block; // 原【函数体】{ ... }(一棵语法子树)——等下整段塞进新函数里执行

    // 原返回类型必须留在"返回位置":外层签名的返回类型被改写成 Response 后,若把原函数体塞进
    // 无标注的 async block,`-> Result<T, E>`(以及 `-> Result<impl IntoResponse, E>`)会因为类型
    // 信息丢失而不可推断(E0282/E0283)。优先把原函数体搬进内层 async fn(原类型写在 `->` 后,
    // `impl Trait` 也合法);签名形态无法拆参数时退回"中间变量标注",再退回直接转换。
    let inner_ident = syn::Ident::new("__hystrix_handler_impl", proc_macro2::Span::call_site());
    let wrapped = nasa_macro_support::wrap_handler(&func.sig, block, &inner_ident, "__hystrix_arg");
    let convert_output = match &wrapped {
        Some(wrapper) => {
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

    // 2d. 复制一份"函数签名"，并【只改返回类型】为 axum 的 Response。
    //     ★★ 这里就是【参数透传】的源头：func.sig 里含【函数名 + 参数列表(sig.inputs) + 返回类型】，
    //        clone 后我们【只改 output(返回类型)，参数列表一个不动】。所以下面用 #sig 生成的新函数，
    //        参数和原函数【完全一样】(同名、同类型) → 原函数体 #block 里对参数的引用自然就接得上。
    //        换句话说:不是手动"转发参数"，而是"复用同一份参数声明 + 复用同一段原体"，参数就透传过去了。
    //     为什么改返回类型：隔离层要统一产出 Response —— 正常结果、429、504 都得是同一种类型 Response，
    //       所以把原来的 `-> Json<...>` 改成 `-> Response`(原结果稍后用 into_response 转过去，见 ③)。
    //     为什么 clone：func.sig 是借用，不能直接改；克隆一份 sig 才能改它的 output 字段。
    //     parse_quote! :把 `-> ::axum::response::Response` 这段源码直接解析成"返回类型"语法节点，赋给 sig.output。
    //     走内层 async fn 形态时,外层参数名改写成 `__hystrix_argN`(类型不变,axum 提取行为不变),
    //       原参数模式(如 `State(s): State<T>`)保留在内层 fn 上,由 convert_output 负责透传。
    let mut sig = func.sig.clone();
    if let Some(wrapper) = &wrapped {
        sig.inputs = wrapper.outer_inputs.clone();
    }
    sig.output = syn::parse_quote!(-> #axum::response::Response);

    // ── 2.5 把注解参数【在编译期】翻译成 Command::monitor 的三个 Option 实参 ──
    //   注解里的值都是字面量，宏展开时已知，所以这里用 Rust 的 match 直接选好 Some/None 的 token。
    //     max_concurrent: 写了 → Some(n)，不写 → None（不限并发）
    //     timeout_ms    : 写了 → Some(Duration::from_millis(m))，不写 → None（不超时）
    //     tps           : >0 → Some(tps)（计入顶栏 TPS），=0 → None（不计）
    //   三者都 None（即 `#[hystrix()]` 空参）= 只采集监控指标、不拦截不限时。
    let mc_opt = match max_concurrent {
        Some(n) => quote! { ::core::option::Option::Some(#n) },
        None => quote! { ::core::option::Option::None },
    };
    let to_opt = match timeout_ms {
        Some(m) => quote! { ::core::option::Option::Some(::std::time::Duration::from_millis(#m)) },
        None => quote! { ::core::option::Option::None },
    };
    let tps_opt = if tps > 0 {
        quote! { ::core::option::Option::Some(#tps) }
    } else {
        quote! { ::core::option::Option::None }
    };
    // 统一走 Command::monitor(name, group, 并发?, 超时?, tps?)——一个构造器覆盖"限流/限时/只监控"全部组合。
    let ctor = quote! {
        #root::Command::monitor(#name, "macro", #mc_opt, #to_opt, #tps_opt)
    };

    // 自定义降级:三选一(fn 优先 → 静态串 → 空用默认壳)。fn 与静态串已在 1e 校验互斥;JSON 已在 1d 校验合法。
    //   fn 分支:把降级 fn 路径包成无捕获闭包存到 Command。闭包必须【显式标注返回类型】为
    //   `Pin<Box<dyn Future<..>+Send>>`,这样 `Box::pin(async{})` 在 return 处强制成 dyn,
    //   才能匹配 set_*_fn 形参的 `Box<dyn Fn() -> Pin<Box<dyn Future>>>`(关联类型不参与隐式 coercion)。
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

    // ── 3. 用 quote!{} 拼出"新函数"的源码 token：让原 handler 走真正的 hystrix Command ──
    //   quote!{} 里 `#变量` = 把该变量的 token 原样【插入】到这个位置(模板替换)：
    //     #(#attrs)* = 把 attrs 这个列表里的每个属性逐个展开(`#( ... )*` 是"对列表重复展开"的语法)
    //     #vis #sig  = 拼出函数头(可见性 + 改过返回类型的签名，含 async/函数名/参数)
    //     #block / #name / #max_concurrent / #timeout_ms = 把对应变量的内容插进去
    //   生成代码里：① 外部 crate(::std / ::axum)走【绝对路径】防同名遮蔽(宏卫生)；
    //              ② 引用主 crate 自己的东西用 `::hystrix::Command`——这里的 `crate` 在【展开后】指
    //                 "用这个宏的那个 crate"(即主 crate rust-simple-mvc)，所以能找到它的 hystrix 模块。
    let expanded = quote! {
        // 把原函数头上的其它属性、可见性、(改过返回类型的)签名拼回去 → 一个"返回 Response 的同名函数"。
        // ★ #sig 里就带着【原函数的参数列表】——这就是参数透传的落点:新函数声明的参数和原来一模一样，
        //   由 axum 照常提取参数填进来，下面 #block 直接用这些参数(见 ② 的 `async move #block` 会捕获它们)。
        #(#attrs)*
        #vis #sig {
            // ① 每个被注解的 handler 各自持有一个【进程内静态、只构造一次】的 Command。
            //    用 OnceLock 懒初始化：第一次请求进来时构造 Command 并自动注册进 hystrix 全局 REGISTRY
            //    → 之后 /hystrix.stream 就会上报这个圈、Dashboard 看得到。
            //    类型写全 ::std::sync::OnceLock / ::std::sync::Arc(绝对路径，宏卫生)；
            //    ::hystrix::Command 指主 crate 的命令类型(展开后 crate=主 crate)。
            static __HYSTRIX_CMD: ::std::sync::OnceLock<::std::sync::Arc<#root::Command>> =
                ::std::sync::OnceLock::new();
            // get_or_init：已初始化则返回现有 Arc，否则跑一次闭包构造。.clone() 拿一份 Arc(run_fn 要 self: Arc<Self>)。
            let __cmd = __HYSTRIX_CMD.get_or_init(|| {
                // #ctor = 上面按 tps 选好的构造调用(tps>0→with_tps / tps=0→new)，group 统一为 "macro"。
                let __c = #ctor;
                __c.set_path(#name); // CostTime 定时日志显示用(没有真实路由就用 name)
                #set_reject  // 自定义限流返回(写了 reject_response 才生成)
                #set_timeout // 自定义超时返回(写了 timeout_response 才生成)
                __c
            }).clone();

            // ② 把【原函数体 #block】包成"无参闭包 → 产出 Response 的 future"，交给 Command::run_fn 执行。
            //    run_fn 内部:先 try_acquire(满→429)、再 timeout 限时跑这个 future(超时→504)、并把
            //    成功/失败/超时/拒绝记入滚动窗口(→ Dashboard)。和 /heavy/slow 的 Command::run 完全同一套。
            //    `move ||`           把上面 #sig 声明的参数 move 进闭包；
            //    `async move #block` 再把它们 move 进 future —— 参数透传收尾(axum 提取→#sig 声明→此处捕获→#block 使用)。
            //    into_response 把原返回类型(如 Json<...>)统一转成 Response(run_fn 要求执行体产出 Response)。
            __cmd.run_fn(move || async move {
                #convert_output
            }).await
        }
    };
    // 把生成好的 token 还给编译器，替换掉原来的函数 —— 之后编译器对这串新代码做正常的类型检查 + 编译。
    expanded.into()
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

/// 业务作用：属性名按【最后一个 path segment】匹配
/// `#[get_mapping]` / `#[naweb::get_mapping]` / `#[nasa::web::get_mapping]` /
/// `#[company_nasa::web::get_mapping]` 都能识别。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
/// - `expected`: 协议或状态机期望值。
fn attr_name_is(attr: &syn::Attribute, expected: &str) -> bool {
    attr.path()
        .segments
        .last()
        .map(|segment| segment.ident == expected)
        .unwrap_or(false)
}

/// 业务作用：从一条 #[*_mapping(...)] 属性里抽出 path 字符串。
/// 两种写法都支持：
///   - 单串简写：#[get_mapping("/x")]                     → parse_args::<LitStr>
///   - 具名写法：#[get_mapping(path = "/x", produces=..)] → parse_nested_meta 找 path/value
///
/// 取不到返回 None。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn mapping_path(attr: &syn::Attribute) -> Option<String> {
    // ① 单串简写
    if let Ok(lit) = attr.parse_args::<LitStr>() {
        return Some(lit.value());
    }
    // ② key=value 列表：遍历每个 k=v，把 path / value 的值取出；其余键(produces/consumes)也 consume 掉以推进解析
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
