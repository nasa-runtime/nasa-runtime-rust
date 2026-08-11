//! 应用入口与业务 initializer 属性宏。
//!
//! `#[application]` 在业务二进制内生成静态组件描述、路由收集工厂和同步进程入口；
//! `#[initializer]` 把完整 `Initialization` trait impl 登记到同一二进制的静态集合。运行时会把
//! 静态项与 Service 启动 Hook 动态登记项冻结为统一依赖计划，在 `Prepare` 后、`Seal` 前严格执行
//! 全部 `before -> initialize -> after` 三轮，全部成功前不开放入站能力。

use std::collections::HashSet;

use nasa_macro_support::runtime_root;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, punctuated::Punctuated, Expr, FnArg, GenericArgument, ItemFn, ItemImpl, Lit,
    LitStr, Meta, Path, PathArguments, ReturnType, Token, Type,
};

/// 业务作用：把业务异步 `main` 转换为统一生命周期进程入口。
///
/// # 支持的组件字符串
///
/// `attr` 可以为空；非空时只接受下面 14 个区分大小写的精确字符串，不支持别名：
///
/// - `"log"`：启用两阶段日志。Bootstrap 先建立早期控制台日志，最终配置就绪后再安装文件日志，
///   并支持运行期日志级别热更新；需要 `nasa` 的 `log` feature。
/// - `"nacos-config"`：启用 Nacos 配置中心。启动时拉取远端配置 overlay，运行期监听配置变化并按
///   last-known-good 规则热刷新；需要 `nacos-config` feature，真实连接 Nacos 还需要 `nacos-sdk`。
/// - `"telemetry"`：启用有界 OpenTelemetry span 管道与受管停机 flush；需要 `telemetry` feature。
/// - `"db"`：启用 MySQL 数据源。启动时校验并探测地址、鉴权和数据库，创建连接池、注册应用资源，
///   同时注入 `#[transactional]` 和 Mapper 使用的事务运行时；需要 `tx` feature。
/// - `"redis"`：启用 Redis 客户端。启动时校验配置、探测 standalone/cluster 拓扑并建立受管客户端，
///   停机时由容器显式关闭；需要 `redis` feature。
/// - `"cache"`：启用由容器拥有的两级缓存运行时与可选跨节点失效广播；需要 `cache` feature。
/// - `"saga"`：启用 Saga Ready 门禁、只读能力发布与 durable timer 监督，并隐式加入 DB 与
///   Outbox。业务在 UserHook 通过 `configure_saga` 提交 Orchestrator 或参与方计划；需要
///   `saga-runtime` feature。
/// - `"kafka"`：启用受管 Kafka producer/consumer。负责 broker 探测、consumer 收集与启动、动态
///   readiness、运行期健康监控、停止消费和 producer flush；需要 `kafka` feature。
/// - `"outbox"`：启用事务型 Outbox dispatcher。业务在 UserHook 提交发布计划，组件负责持续投递、
///   readiness、退避与停机；需要 `outbox` feature。该组件隐式加入 DB；声明 `"saga"` 时也会自动
///   纳入 Outbox，无需重复书写。
///
/// 隐式加入只负责补齐缺失依赖，不构成互斥约束。`("saga")`、`("saga", "db")` 与
/// `("saga", "db", "outbox")` 会生成相同的组件图；只有同一个字符串在属性中重复出现才会拒绝。
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
/// saga → kafka → outbox → auth → web → ws → nacos-discovery → scheduling）自动规范化后再生成组件列表。因此
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
///     "redis",
///     "cache",
///     "saga",
///     "kafka",
///     "auth",
///     "web",
///     "ws",
///     "nacos-discovery",
///     "scheduling"
/// )]
/// async fn main(app: nasa::Application) -> anyhow::Result<()> {
///     // 声明 Saga 后 DB 与 Outbox 已纳入生命周期，这里只提交业务计划和其它组件定制。
///     Ok(())
/// }
/// ```
///
/// 参数说明：
/// - `attr`：按任意书写顺序声明的零个或多个受支持组件字符串。
/// - `item`：零参数或接收一个 `Application` 的异步主函数。
///
/// 返回：入口合法时生成同步进程入口、规范组件描述和业务启动 Hook；合同非法时生成定位到调用处的
/// 编译错误。
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

/// 业务作用：把完整 `Initialization` trait impl 登记为业务二进制内的静态 initializer，
/// 并与 Service 启动 Hook 动态登记的 initializer 合并成同一份冻结计划。
///
/// 本属性只能标注安全、正向、无 impl 泛型参数的 `Initialization` trait impl，不能标注单个方法、
/// 固有 impl、unsafe impl 或其它 trait。被登记的实例会严格参与三轮全局屏障：全部 `before` 完成后
/// 才进入全部 `initialize`，全部 `initialize` 完成后才进入全部 `after`。
///
/// # 执行阶段
///
/// Service 模式的时序为：业务 `UserHook` 完成登记并冻结 initializer 计划，组件完成 `Prepare`
/// （包括 migration 和出站依赖门禁），然后调用条件工厂并执行三轮 initializer 屏障。只有全部成功后，
/// Runner 才进入 `Seal`、组件 `Ready`、staged task 激活和 `mark_ready()`；因此 initializer 执行期间
/// Web/WS listener、consumer 和服务发现尚未对外接流。
///
/// Batch 模式只收集静态 `one-shot` initializer，在组件 `Prepare` 之后、业务工作负载被 poll 之前
/// 执行条件工厂和三轮屏障；`hosted` initializer 会被拒绝，Batch 工作负载中也不能动态补登记
/// initializer。`hosted` initializer 暂存的长期任务只在 Service 的组件 `Ready` 全部成功后交给
/// Supervisor，任务主体还会继续等待 `mark_ready()`，不会与初始化阶段并发执行。
///
/// # 属性
///
/// - `name = "..."`：可选。应用内唯一的 canonical 身份，只允许 ASCII 小写字母、数字、`_`、`-`、`.`，
///   长度为 1..=128 字节。省略时从 impl 的实现类型名派生 kebab-case，例如
///   `OrderCacheInitialization` 派生为 `order-cache-initialization`；无法稳定派生时必须显式声明。
///   派生结果仍是依赖名、日志字段和指标 label 使用的稳定业务身份，重命名实现类型会同步改变该身份；
///   需要跨发布保持依赖引用和观测连续性时必须显式填写 `name`。
/// - `order = ...`：可选 `i32` 整数字面量，默认 `100000`。依赖条件相同且当前都可执行时，数值越小
///   越先执行；数值相同按 `name` 升序消除平局。`requires` 依赖边始终优先于 `order`，低 `order`
///   不能越过尚未完成的依赖。
/// - `requires = ["..."]`：可选，默认空。声明本项执行前必须成功启用并完成同阶段调用的 initializer，
///   最多 32 项；缺失、重复、自依赖或依赖环都会拒绝启动。
/// - `kind = "one-shot" | "hosted"`：可选，默认 `"one-shot"`。`hosted` 只允许 Service 模式，
///   并可在 `after` 暂存 Ready 后启动的长期任务或 readiness；`one-shot` 只执行有界初始化。
/// - `factory = path`：可选异步条件工厂。签名必须为
///   `async fn(Application) -> ApplicationResult<Option<T>>`；`Some(T)` 启用本项，`None` 表示条件未命中，
///   `Err` 阻止应用接流。省略时实现类型必须实现 `Default`。
///
/// # 示例
///
/// ```ignore
/// #[derive(Default)]
/// struct OrderCacheInitialization;
///
/// #[nasa::initializer(order = 200, requires = ["schema"])]
/// impl nasa::application::Initialization for OrderCacheInitialization {
///     // 实现 before / initialize / after 中实际需要的阶段。
/// }
/// ```
///
/// 参数说明：
/// - `attr`：`name/order/requires/kind/factory` 元数据。
/// - `item`：无 impl 泛型参数的安全、正向 `Initialization` trait impl。
///
/// 返回：原 trait impl、类型擦除工厂和 linkme 静态描述；合同非法时返回定位到属性或 impl 的编译错误。
#[proc_macro_attribute]
pub fn initializer(attr: TokenStream, item: TokenStream) -> TokenStream {
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let item_impl = parse_macro_input!(item as ItemImpl);
    match expand_initializer(metas, item_impl) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// initializer 属性完成字面量校验后的内部参数。
struct InitializerArgs {
    name: LitStr,
    order: Option<i32>,
    requires: Vec<LitStr>,
    kind: InitializerKindArg,
    factory: Option<Path>,
}

/// initializer 生存类型的封闭宏层表示。
enum InitializerKindArg {
    OneShot,
    Hosted,
}

/// 业务作用：校验 initializer impl 形状并生成独立的静态收集项。
///
/// 参数说明：
/// - `metas`：属性内的名值参数。
/// - `item_impl`：被标注的 trait impl 语法树。
///
/// 返回：元数据、工厂和 trait 合同均合法时返回展开代码。
fn expand_initializer(
    metas: Punctuated<Meta, Token![,]>,
    item_impl: ItemImpl,
) -> syn::Result<TokenStream2> {
    verify_initializer_impl(&item_impl)?;
    let args = parse_initializer_args(&metas, &item_impl.self_ty)?;
    let runtime = runtime_root("application", "napp")
        .map_err(|message| syn::Error::new_spanned(&item_impl.self_ty, message))?;
    let initializer_type = (*item_impl.self_ty).clone();
    let name = args.name;
    let order = args
        .order
        .map(|value| quote!(#value))
        .unwrap_or_else(|| quote!(#runtime::DEFAULT_INITIALIZER_ORDER));
    let requires = args.requires;
    let kind = match args.kind {
        InitializerKindArg::OneShot => quote!(#runtime::InitializerKind::OneShot),
        InitializerKindArg::Hosted => quote!(#runtime::InitializerKind::Hosted),
    };
    let construct = match args.factory {
        Some(factory) => quote! {
            let result: #runtime::ApplicationResult<::std::option::Option<#initializer_type>> =
                #factory(application).await;
            let initializer = result?;
            ::std::result::Result::Ok(initializer.map(|value| {
                ::std::boxed::Box::new(value)
                    as ::std::boxed::Box<dyn #runtime::Initialization>
            }))
        },
        None => quote! {
            let _ = application;
            let value: #initializer_type =
                <#initializer_type as ::std::default::Default>::default();
            ::std::result::Result::Ok(::std::option::Option::Some(
                ::std::boxed::Box::new(value)
                    as ::std::boxed::Box<dyn #runtime::Initialization>
            ))
        },
    };

    Ok(quote! {
        #item_impl

        const _: () = {
            /// 业务作用：把强类型条件工厂适配为 Application Runner 可收集的对象安全工厂。
            ///
            /// 参数说明：
            /// - `application`：Prepare 成功后的容器所有权副本。
            ///
            /// 返回：`Some` 表示当前配置启用，`None` 表示条件未命中，失败则拒绝接流。
            fn __nasa_initializer_factory(
                application: #runtime::Application,
            ) -> #runtime::ApplicationFuture<
                'static,
                ::std::option::Option<
                    ::std::boxed::Box<dyn #runtime::Initialization>
                >,
            > {
                ::std::boxed::Box::pin(async move { #construct })
            }

            #[#runtime::__private::linkme::distributed_slice(#runtime::COLLECTED_INITIALIZERS)]
            #[linkme(crate = #runtime::__private::linkme)]
            static __NASA_INITIALIZER_DESCRIPTOR: #runtime::InitializerDescriptor =
                #runtime::InitializerDescriptor::__new(
                    #name,
                    #order,
                    &[#(#requires),*],
                    #kind,
                    __nasa_initializer_factory,
                    concat!(module_path!(), ":", file!(), ":", line!()),
                );
        };
    })
}

/// 业务作用：把 initializer 属性参数解析为封闭内部元数据。
///
/// 参数说明：
/// - `metas`：属性中按源码顺序出现的参数。
/// - `self_type`：未声明 `name` 时用于派生默认 canonical 名称的实现类型。
///
/// 返回：已确定名称与可选顺序/依赖/类型/工厂；重复键、未知键或非法字面量返回编译错误。
fn parse_initializer_args(
    metas: &Punctuated<Meta, Token![,]>,
    self_type: &Type,
) -> syn::Result<InitializerArgs> {
    let mut name = None;
    let mut order = None;
    let mut requires = None;
    let mut kind = None;
    let mut factory = None;
    for meta in metas {
        let Meta::NameValue(value) = meta else {
            return Err(syn::Error::new_spanned(
                meta,
                "initializer attributes must use `key = value` syntax",
            ));
        };
        let key = value
            .path
            .get_ident()
            .map(ToString::to_string)
            .ok_or_else(|| {
                syn::Error::new_spanned(
                    &value.path,
                    "initializer attribute key must be an identifier",
                )
            })?;
        match key.as_str() {
            "name" => set_once(&mut name, parse_string_expr(&value.value, "name")?, meta)?,
            "order" => {
                let parsed = parse_initializer_order(&value.value)?;
                set_once(&mut order, parsed, meta)?;
            }
            "requires" => {
                let Expr::Array(array) = &value.value else {
                    return Err(syn::Error::new_spanned(
                        &value.value,
                        "initializer requires must be an array of string literals",
                    ));
                };
                let mut parsed = Vec::with_capacity(array.elems.len());
                for element in &array.elems {
                    parsed.push(parse_string_expr(element, "requires entry")?);
                }
                set_once(&mut requires, parsed, meta)?;
            }
            "kind" => {
                let literal = parse_string_expr(&value.value, "kind")?;
                let parsed = match literal.value().as_str() {
                    "one-shot" => InitializerKindArg::OneShot,
                    "hosted" => InitializerKindArg::Hosted,
                    _ => {
                        return Err(syn::Error::new_spanned(
                            literal,
                            "initializer kind must be `one-shot` or `hosted`",
                        ));
                    }
                };
                set_once(&mut kind, parsed, meta)?;
            }
            "factory" => {
                let Expr::Path(path) = &value.value else {
                    return Err(syn::Error::new_spanned(
                        &value.value,
                        "initializer factory must be a function path",
                    ));
                };
                set_once(&mut factory, path.path.clone(), meta)?;
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &value.path,
                    "unknown initializer attribute key",
                ));
            }
        }
    }

    let name = match name {
        Some(name) => name,
        None => default_initializer_name(self_type)?,
    };
    validate_initializer_name(&name, "initializer name")?;
    let requires = requires.unwrap_or_default();
    if requires.len() > 32 {
        return Err(syn::Error::new_spanned(
            &name,
            "initializer requires cannot contain more than 32 entries",
        ));
    }
    let mut seen = HashSet::new();
    for required in &requires {
        validate_initializer_name(required, "initializer dependency")?;
        if required.value() == name.value() {
            return Err(syn::Error::new_spanned(
                required,
                "initializer cannot require itself",
            ));
        }
        if !seen.insert(required.value()) {
            return Err(syn::Error::new_spanned(
                required,
                "initializer dependency is repeated",
            ));
        }
    }
    Ok(InitializerArgs {
        name,
        order,
        requires,
        kind: kind.unwrap_or(InitializerKindArg::OneShot),
        factory,
    })
}

/// 业务作用：从被标注的具体实现类型派生可用于依赖、日志和指标的默认 initializer 身份。
///
/// 参数说明：
/// - `self_type`：`impl Initialization for Type` 中的 `Type`。
///
/// 返回：路径末段类型名转为 canonical kebab-case；无法稳定派生时要求调用方显式声明 `name`。
fn default_initializer_name(self_type: &Type) -> syn::Result<LitStr> {
    let Type::Path(path) = self_type else {
        return Err(syn::Error::new_spanned(
            self_type,
            "initializer name cannot be derived from this type; declare `name` explicitly",
        ));
    };
    if path.qself.is_some() {
        return Err(syn::Error::new_spanned(
            self_type,
            "initializer name cannot be derived from a qualified self type; declare `name` explicitly",
        ));
    }
    let segment = path.path.segments.last().ok_or_else(|| {
        syn::Error::new_spanned(
            self_type,
            "initializer name cannot be derived from this type; declare `name` explicitly",
        )
    })?;
    let identifier = segment.ident.to_string();
    let name = canonicalize_type_name(&identifier).ok_or_else(|| {
        syn::Error::new_spanned(
            &segment.ident,
            "initializer type name cannot form a canonical name; declare `name` explicitly",
        )
    })?;
    let name = LitStr::new(&name, segment.ident.span());
    validate_initializer_name(&name, "derived initializer name")?;
    Ok(name)
}

/// 业务作用：把 Rust 类型标识符确定性转换成 initializer canonical kebab-case。
///
/// 参数说明：
/// - `identifier`：实现类型的最后一个 Rust 路径标识符。
///
/// 返回：ASCII 字母、数字和下划线可转换时返回小写名称；其它字符或空结果返回 `None`。
fn canonicalize_type_name(identifier: &str) -> Option<String> {
    let identifier = identifier.strip_prefix("r#").unwrap_or(identifier);
    let bytes = identifier.as_bytes();
    let mut output = String::with_capacity(bytes.len());
    let mut pending_separator = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte == b'_' {
            pending_separator = !output.is_empty();
            continue;
        }
        if !byte.is_ascii_alphanumeric() {
            return None;
        }
        let previous = index
            .checked_sub(1)
            .and_then(|value| bytes.get(value))
            .copied();
        let next = bytes.get(index + 1).copied();
        let word_boundary = byte.is_ascii_uppercase()
            && (previous.is_some_and(|value| value.is_ascii_lowercase() || value.is_ascii_digit())
                || (previous.is_some_and(|value| value.is_ascii_uppercase())
                    && next.is_some_and(|value| value.is_ascii_lowercase())));
        if (pending_separator || word_boundary) && !output.is_empty() && !output.ends_with('-') {
            output.push('-');
        }
        output.push(byte.to_ascii_lowercase() as char);
        pending_separator = false;
    }
    while output.ends_with('-') {
        output.pop();
    }
    (!output.is_empty()).then_some(output)
}

/// 业务作用：解析 initializer 的有符号稳定优先级，保证属性入口与运行时 `i32` 合同一致。
///
/// 参数说明：
/// - `expression`：属性 `order` 等号右侧的表达式。
///
/// 返回：正负整数字面量在 `i32` 范围内时返回其值；其它表达式或越界值返回编译错误。
fn parse_initializer_order(expression: &Expr) -> syn::Result<i32> {
    let invalid = || {
        syn::Error::new_spanned(
            expression,
            "initializer order must be an i32 integer literal",
        )
    };
    let signed = match expression {
        Expr::Lit(expr) => match &expr.lit {
            Lit::Int(value) => value.base10_parse::<i64>().map_err(|_| invalid())?,
            _ => return Err(invalid()),
        },
        Expr::Unary(expr) if matches!(expr.op, syn::UnOp::Neg(_)) => match expr.expr.as_ref() {
            Expr::Lit(expr) => match &expr.lit {
                Lit::Int(value) => value
                    .base10_parse::<i64>()
                    .ok()
                    .and_then(i64::checked_neg)
                    .ok_or_else(invalid)?,
                _ => return Err(invalid()),
            },
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
    i32::try_from(signed).map_err(|_| invalid())
}

/// 业务作用：校验属性只标注可静态收集的安全正向 `Initialization` impl。
///
/// 参数说明：
/// - `item_impl`：待校验的 impl 块。
///
/// 返回：形状可用时成功；固有、unsafe、负向、泛型或错误 trait 时返回编译错误。
fn verify_initializer_impl(item_impl: &ItemImpl) -> syn::Result<()> {
    if item_impl.unsafety.is_some() {
        return Err(syn::Error::new_spanned(
            item_impl.unsafety,
            "initializer cannot annotate an unsafe impl",
        ));
    }
    if !item_impl.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item_impl.generics,
            "initializer impl cannot declare generic parameters",
        ));
    }
    let Some((polarity, trait_path, _)) = &item_impl.trait_ else {
        return Err(syn::Error::new_spanned(
            &item_impl.self_ty,
            "initializer must annotate an Initialization trait impl",
        ));
    };
    if polarity.is_some() {
        return Err(syn::Error::new_spanned(
            polarity,
            "initializer cannot annotate a negative impl",
        ));
    }
    if trait_path
        .segments
        .last()
        .is_none_or(|segment| segment.ident != "Initialization")
    {
        return Err(syn::Error::new_spanned(
            trait_path,
            "initializer trait path must end with Initialization",
        ));
    }
    Ok(())
}

/// 业务作用：解析 initializer 属性中必须是字符串的表达式。
///
/// 参数说明：
/// - `expression`：待解析的属性值。
/// - `field`：出错时的稳定字段名。
///
/// 返回：字符串字面量；其它表达式返回编译错误。
fn parse_string_expr(expression: &Expr, field: &str) -> syn::Result<LitStr> {
    match expression {
        Expr::Lit(expr) => match &expr.lit {
            Lit::Str(value) => Ok(value.clone()),
            _ => Err(syn::Error::new_spanned(
                expression,
                format!("initializer {field} must be a string literal"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            expression,
            format!("initializer {field} must be a string literal"),
        )),
    }
}

/// 业务作用：保证同一 initializer 属性键只设置一次。
///
/// 参数说明：
/// - `slot`：当前字段已解析的可选值。
/// - `value`：本次准备写入的值。
/// - `meta`：重复时用于定位的属性项。
///
/// 返回：首次写入成功；重复键返回编译错误。
fn set_once<T>(slot: &mut Option<T>, value: T, meta: &Meta) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new_spanned(
            meta,
            "initializer attribute key is repeated",
        ));
    }
    *slot = Some(value);
    Ok(())
}

/// 业务作用：在宏展开前校验 initializer 与依赖的 canonical 名称合同。
///
/// 参数说明：
/// - `name`：待校验的字符串字面量。
/// - `field`：出错时的稳定字段分类。
///
/// 返回：1..=128 字节且仅含小写 ASCII、数字、`_`/`-`/`.` 时成功。
fn validate_initializer_name(name: &LitStr, field: &str) -> syn::Result<()> {
    let value = name.value();
    if value.is_empty() || value.len() > 128 {
        return Err(syn::Error::new_spanned(
            name,
            format!("{field} must contain between 1 and 128 bytes"),
        ));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
    }) {
        return Err(syn::Error::new_spanned(
            name,
            format!("{field} must contain only lowercase ASCII letters, digits, `_`, `-`, or `.`"),
        ));
    }
    Ok(())
}

/// 业务作用：校验入口契约并生成静态描述、业务 Hook 包装和同步主函数。
///
/// 参数说明：
/// - `components`：属性中按源码顺序出现的组件字面量。
/// - `function`：已经解析的业务异步主函数。
///
/// 返回：入口合同与组件声明合法时返回完整展开；路径、签名或组件非法时返回定位明确的宏错误。
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
            /// 参数说明: 无。
            ///
            /// 返回：只包含静态方法、路径和处理函数身份的路由元数据。
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
            /// 参数说明：
            /// - `context`：由 napp Ready 构造，保证 interceptor 与 handler 使用同一个 Application clone。
            ///
            /// 返回：路由和安全流水线装配成功时返回统一状态 Router；冲突或合同错误时拒绝监听。
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
        /// 参数说明：
        /// - `hook`：拥有业务主函数调用的闭包，接收统一 Application 并返回受监督 future。
        ///
        /// 返回：类型合同成立时原样返回 Hook，供同步入口唯一消费。
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
        /// 参数说明: 无。
        ///
        /// 返回：业务正常完成或优雅停机时返回成功退出码；启动、运行或停机失败返回对应非零退出码。
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
/// 参数说明：
/// - `function`：属性直接标注的业务函数语法树。
///
/// 返回：名称、异步形态、参数、返回类型和 runtime 所有权均合法时成功，否则返回编译错误。
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
/// 参数说明：
/// - `argument`：业务主函数声明的唯一函数参数。
///
/// 返回：参数为统一 `Application` 类型时成功；receiver 或其它类型返回编译错误。
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
/// 参数说明：
/// - `output`：业务主函数声明的返回类型。
///
/// 返回：返回类型为单元成功值的 `Result` 时成功，其它形态返回编译错误。
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

/// 规范启动顺序：业务侧可以按任意顺序书写组件字符串，napp 由此秩统一规范化。
///
/// 该数组既是合法组件白名单，也是唯一的规范启动顺序：配置先于资源，DB 先于 Saga/Outbox，
/// transport 先于业务入口。新增组件时必须按依赖与反向停机关系插入。
const CANONICAL_COMPONENT_ORDER: [&str; 14] = [
    "log",
    "nacos-config",
    "telemetry",
    "db",
    "redis",
    "cache",
    "saga",
    "kafka",
    "outbox",
    "auth",
    "web",
    "ws",
    "nacos-discovery",
    "scheduling",
];

/// 业务作用：校验组件名称与重复项，并按规范启动顺序排序返回，确保书写顺序不改变生命周期。
///
/// 业务侧无需按启动顺序书写 `#[application(...)]`:本函数接受任意顺序,拒绝未知名称和重复项,
/// 然后按 [`CANONICAL_COMPONENT_ORDER`] 排序。运行时因此始终收到规范顺序的组件列表。
///
/// 参数说明：
/// - `components`：属性中以任意顺序提供的字符串字面量。
///
/// 返回：名称全部合法且唯一时返回规范顺序；未知名称或重复声明返回定位到属性项的错误。
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
    // Saga 的当前持久层固定使用 MySQL，并且消息闭环必然包含 Inbox 与 Outbox；Inbox 没有独立
    // 生命周期，DB 与 Outbox 只在缺失时补入。业务显式写出依赖仍收敛为同一组件图，不应误判重复。
    // Kafka/Redis 等 transport 不在这里推断。
    if seen.contains("saga") && seen.insert("outbox".to_string()) {
        names.push("outbox".to_string());
    }
    if seen.contains("outbox") && seen.insert("db".to_string()) {
        names.push("db".to_string());
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
/// 参数说明：
/// - `name`：已经通过白名单校验的组件名称。
///
/// 返回：名称可映射时返回对应枚举标识；内部传入未校验名称时返回宏展开错误。
fn component_variant(name: &str) -> syn::Result<syn::Ident> {
    let variant = match name {
        "log" => "Log",
        "nacos-config" => "NacosConfig",
        "db" => "Db",
        "redis" => "Redis",
        "telemetry" => "Telemetry",
        "cache" => "Cache",
        "saga" => "Saga",
        "kafka" => "Kafka",
        "outbox" => "Outbox",
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
/// 参数说明：
/// - `name`：已经通过组件白名单和顺序校验的规范名称。
///
/// 返回：返回供展开代码引用的能力模块标识；内部传入未校验名称时返回宏展开错误。
fn component_feature_module(name: &str) -> syn::Result<syn::Ident> {
    match name {
        "log" | "db" | "redis" | "telemetry" | "cache" | "saga" | "kafka" | "outbox" | "auth"
        | "web" | "ws" | "scheduling" => Ok(format_ident!("{name}")),
        "nacos-config" => Ok(format_ident!("nacos_config")),
        "nacos-discovery" => Ok(format_ident!("nacos_discovery")),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "component name was not validated",
        )),
    }
}
