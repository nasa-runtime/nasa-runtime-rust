//! 声明式 HTTP 客户端过程宏。
//!
//! `#[rest_client]` 基于 trait 生成可注入 `RestDiscovery` 的客户端实现，
//! 方法级 mapping 注解决定 HTTP 方法、路径和参数绑定方式。
// ============================================================================
// rest-client-macro —— 声明式 HTTP 客户端宏。
//
//   #[rest_client] 读完整 trait → 生成 {Trait}Client + new(rest) + #[async_trait] impl;
//   方法属性 #[GetMapping]/#[PostMapping]/#[PutMapping]/#[PatchMapping]/#[DeleteMapping] 提供 HTTP 元数据,被 #[rest_client] 消费删除;
//   参数 inert attr(#[PathVariable]/#[RequestParam]/#[RequestHeader]/#[RequestHeaders]/#[QueryMap]/#[RequestBody]/#[FormBody])
//   只在 #[rest_client] 内有效、由它解析并从输出删除(不导出成独立 proc macro, 第8条)。
//
//   service 模式 → self.rest.service_request(service, Method::X, path)(档1 直连,**不生成字符串 URL**);
//   url 模式     → self.rest.request(Method::X, url)(普通外部)。复杂逻辑收敛到 rest-discovery 的 __private。
// ============================================================================

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{
    Attribute, Expr, ExprArray, ExprLit, ExprTuple, FnArg, Ident, ItemTrait, Lit, LitStr, Meta,
    Pat, ReturnType, TraitItem, TraitItemFn, Type,
};

// ── 入口宏 ──

/// trait 级生成器:读 trait → 生成 client struct + `new(rest)` + `#[async_trait] impl`。
///
/// # 参数
///
/// - `attr`: `#[rest_client(...)]` 括号内的配置 token stream；当前保留给扩展。
/// - `item`: 被注解的 trait 定义 token stream。
#[proc_macro_attribute]
pub fn rest_client(attr: TokenStream, item: TokenStream) -> TokenStream {
    match rest_client_impl(attr.into(), item.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// 以下方法映射宏只作 `use` 锚点 + 脱离 `#[rest_client]` 误用时报错;
/// 正常路径下它们贴在 `#[rest_client]` trait 方法上,由外层宏先读取并删除,不会独立展开。
macro_rules! mapping_stub {
    ($name:ident) => {
        ///
        /// # 参数
        /// - `attr`: 属性宏括号内的 token stream。
        /// - `item`: 被属性宏或派生宏处理的 Rust item token stream。
        #[proc_macro_attribute]
        #[allow(non_snake_case)]
        pub fn $name(_attr: TokenStream, item: TokenStream) -> TokenStream {
            let item2: TokenStream2 = item.into();
            quote! {
                ::core::compile_error!(concat!(
                    "#[", stringify!($name),
                    "] 只能放在 #[rest_client] trait 的方法上,由 #[rest_client] 统一消费"
                ));
                #item2
            }
            .into()
        }
    };
}
mapping_stub!(GetMapping);
mapping_stub!(PostMapping);
mapping_stub!(PutMapping);
mapping_stub!(PatchMapping);
mapping_stub!(DeleteMapping);

// ── 运行时根路径解析(直接依赖 rest-discovery → ::rest_discovery;门面 nasa → ::nasa::discovery::rest) ──

/// 解析 REST 门面的根路径；用于生成宏展开时引用的公共类型。
fn rest_root() -> TokenStream2 {
    use proc_macro_crate::{crate_name, FoundCrate};
    /// 尝试查找调用方依赖图中的 `rest-discovery` crate。
    ///
    /// 支持 Cargo 重命名；查不到时返回 `None`,外层会继续尝试 `nasa` 门面或裸路径兜底。
    fn crate_name_compat() -> Option<FoundCrate> {
        crate_name("rest-discovery").ok()
    }

    // 展开代码必须引用调用方 crate graph 里的运行时 crate。先查直接依赖,再查门面依赖,
    // 最后给出裸路径兜底,让真正缺依赖时由编译器报清楚的 unresolved crate。
    // ① 直接依赖 rest-discovery(含 Cargo 重命名)。
    if let Some(found) = crate_name_compat() {
        match found {
            FoundCrate::Itself => return quote!(crate),
            FoundCrate::Name(n) => {
                let n = format_ident!("{n}");
                return quote!(::#n);
            }
        }
    }
    // ② 门面 nasa(含重命名)。
    match crate_name("nasa") {
        Ok(FoundCrate::Itself) => quote!(crate::discovery::rest),
        Ok(FoundCrate::Name(n)) => {
            let n = format_ident!("{n}");
            quote!(::#n::discovery::rest)
        }
        // ③ 兜底:裸 ::rest_discovery(给出可编译路径,真缺依赖时由后续解析报错)。
        Err(_) => quote!(::rest_discovery),
    }
}

// ── trait 级参数 ──

/// 保存客户端 trait 注解参数；用于生成远程调用实现。
#[derive(Default)]
struct TraitArgs {
    service: Option<String>,
    context_path: Option<String>,
    scheme: Option<String>,
    client: Option<Ident>,
}

/// 解析 parse trait args 输入；用于把文本或语法节点转换为内部结构。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn parse_trait_args(attr: TokenStream2) -> syn::Result<TraitArgs> {
    let mut a = TraitArgs::default();
    if attr.is_empty() {
        return Ok(a);
    }
    let parser = syn::meta::parser(|meta| {
        let key = meta
            .path
            .get_ident()
            .map(|i| i.to_string())
            .unwrap_or_default();
        match key.as_str() {
            "service" => a.service = Some(meta.value()?.parse::<LitStr>()?.value()),
            "context_path" => a.context_path = Some(meta.value()?.parse::<LitStr>()?.value()),
            "scheme" => a.scheme = Some(meta.value()?.parse::<LitStr>()?.value()),
            "client" => {
                let s = meta.value()?.parse::<LitStr>()?;
                a.client = Some(format_ident!("{}", s.value()));
            }
            // rest_field 第一版不支持(client 的 RestDiscovery 字段固定为 `rest`):显式拒绝,避免 silent no-op 误导。
            "rest_field" => {
                return Err(
                    meta.error("第一版不支持 rest_field,client 的 RestDiscovery 字段固定为 `rest`")
                );
            }
            other => return Err(meta.error(format!("rest_client 不支持的参数 `{other}`"))),
        }
        Ok(())
    });
    syn::parse::Parser::parse2(parser, attr)?;
    Ok(a)
}

// ── 方法映射属性 ──

/// 保存单个接口方法的映射参数；用于生成请求路径、方法和媒体类型。
#[derive(Default)]
struct MappingArgs {
    path: Option<String>,
    service: Option<String>,
    context_path: Option<String>,
    url: Option<String>,
    scheme: Option<String>,
    produces: Option<String>,
    consumes: Option<String>,
    response: Option<String>,
    /// `unwrap = "data"`:把 2xx JSON 响应的【顶层字段】解包再反序列化成 `T`(仅 response=json、非 `()` 返回)。
    unwrap_field: Option<String>,
    headers: Vec<(String, String)>,
}

/// 是否为客户端方法映射之一,返回对应 HTTP method ident(GET/POST/...)。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn http_method_of(attr: &Attribute) -> Option<Ident> {
    let id = attr.path().get_ident()?;
    let m = match id.to_string().as_str() {
        "GetMapping" => "GET",
        "PostMapping" => "POST",
        "PutMapping" => "PUT",
        "PatchMapping" => "PATCH",
        "DeleteMapping" => "DELETE",
        _ => return None,
    };
    Some(format_ident!("{m}"))
}

/// 解析 parse mapping attr 输入；用于把文本或语法节点转换为内部结构。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn parse_mapping_attr(attr: &Attribute) -> syn::Result<MappingArgs> {
    let mut a = MappingArgs::default();
    match &attr.meta {
        // #[GetMapping] 裸属性:无参数(path 须来自 trait 级或后续校验报错)。
        Meta::Path(_) => Ok(a),
        Meta::List(list) => {
            // 单字符串简写:#[GetMapping("/path")]。
            if let Ok(s) = syn::parse2::<LitStr>(list.tokens.clone()) {
                a.path = Some(s.value());
                return Ok(a);
            }
            // key = value 形式只做词法收集;跨字段互斥、路径规则、header 规则在 `gen_method`
            // 一次性校验,这样错误能统一挂到方法签名或对应参数上。
            let parser = syn::meta::parser(|meta| {
                let key = meta
                    .path
                    .get_ident()
                    .map(|i| i.to_string())
                    .unwrap_or_default();
                match key.as_str() {
                    "path" | "value" | "remote" => {
                        a.path = Some(meta.value()?.parse::<LitStr>()?.value())
                    }
                    "service" => a.service = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "context_path" => {
                        a.context_path = Some(meta.value()?.parse::<LitStr>()?.value())
                    }
                    "url" => a.url = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "scheme" => a.scheme = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "produces" => a.produces = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "response" => a.response = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "unwrap" => a.unwrap_field = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "consumes" => a.consumes = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "headers" => {
                        // headers = [("K", "V"), ...] 只接受字符串字面量,保证后续能在宏展开期
                        // 校验连接层 header、非法 name/value,避免把坏配置留到运行期。
                        let arr: ExprArray = meta.value()?.parse()?;
                        for elem in arr.elems {
                            a.headers.push(parse_header_tuple(&elem)?);
                        }
                    }
                    other => return Err(meta.error(format!("mapping 不支持的参数 `{other}`"))),
                }
                Ok(())
            });
            syn::parse::Parser::parse2(parser, list.tokens.clone())?;
            Ok(a)
        }
        Meta::NameValue(nv) => Err(syn::Error::new(
            nv.span(),
            "mapping 不支持 name=value 顶层写法,请用 #[GetMapping(\"/path\")] 或 #[GetMapping(path=\"...\")]",
        )),
    }
}

/// 解析 parse header tuple 输入；用于把文本或语法节点转换为内部结构。
///
/// # 参数
/// - `elem`: 当前正在解析的语法元素。
fn parse_header_tuple(elem: &Expr) -> syn::Result<(String, String)> {
    if let Expr::Tuple(ExprTuple { elems, .. }) = elem {
        if elems.len() == 2 {
            let k = lit_str_of(&elems[0])?;
            let v = lit_str_of(&elems[1])?;
            return Ok((k, v));
        }
    }
    Err(syn::Error::new(
        elem.span(),
        "headers 元素必须是 (\"Key\", \"Value\") 字符串二元组",
    ))
}

/// 读取表达式里的字符串字面量；用于校验注解参数值。
///
/// # 参数
/// - `e`: 错误对象或外部错误值。
fn lit_str_of(e: &Expr) -> syn::Result<String> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Str(s), ..
    }) = e
    {
        Ok(s.value())
    } else {
        Err(syn::Error::new(e.span(), "期望字符串字面量"))
    }
}

// ── 参数绑定 ──

enum ParamBind {
    Path {
        ident: Ident,
        name: String,
    },
    Query {
        ident: Ident,
        name: String,
        optional: bool,
        /// `Vec<T>` / `Option<Vec<T>>` → 多值,生成重复 key。
        multi: bool,
    },
    Header {
        ident: Ident,
        name: String,
        optional: bool,
    },
    Headers {
        ident: Ident,
    },
    /// `#[QueryMap] filter: T`(`T: Serialize`)→ 整个结构体按字段名展开成 query,按声明顺序 append(不覆盖)。
    QueryMap {
        ident: Ident,
    },
    Body {
        ident: Ident,
    },
    /// `#[RequestBody(raw)]`:原样字节 body(不做 JSON/urlencoded 编码),Content-Type 由 consumes/header 决定。
    RawBody {
        ident: Ident,
    },
    FormBody {
        ident: Ident,
    },
}

/// 解析单个参数的 helper attr 并从签名删除;返回绑定信息(`None` 表示无显式注解,由调用方按 method 默认决定)。
///
/// # 参数
/// - `pt`: 过程宏解析出的 Rust 类型节点。
/// - `http`: 当前 trait 方法映射出的 HTTP verb。
fn classify_param(pt: &mut syn::PatType, http: &str) -> syn::Result<ParamBind> {
    let span = pt.span();
    let ident = match pt.pat.as_ref() {
        Pat::Ident(pi) => pi.ident.clone(),
        _ => {
            return Err(syn::Error::new(
                span,
                "rest_client 方法参数必须是简单标识符",
            ))
        }
    };
    let optional = is_option(&pt.ty);

    // 找(并记下)参数上的 helper attr。
    let mut found: Option<(String, Option<String>)> = None; // (kind, explicit_name)
    let mut body_raw = false; // #[RequestBody(raw)] → true;#[RequestBody] → false
    let mut kept = Vec::new();
    for attr in pt.attrs.drain(..) {
        let kind = attr
            .path()
            .get_ident()
            .map(|i| i.to_string())
            .unwrap_or_default();
        match kind.as_str() {
            // RequestBody 形态单独解析:裸 → json body;#[RequestBody(raw)] → 原样字节 body(均无名字)。
            "RequestBody" => {
                if found.is_some() {
                    return Err(syn::Error::new(attr.span(), "一个参数最多一个 helper 注解"));
                }
                body_raw = parse_request_body_raw(&attr)?;
                found = Some((kind, None));
            }
            "PathVariable" | "RequestParam" | "RequestHeader" | "RequestHeaders" | "QueryMap"
            | "FormBody" => {
                if found.is_some() {
                    return Err(syn::Error::new(attr.span(), "一个参数最多一个 helper 注解"));
                }
                let name = explicit_name(&attr)?;
                found = Some((kind, name));
            }
            _ => kept.push(attr),
        }
    }
    pt.attrs = kept;

    // 默认名 = 参数标识符去 raw 前缀(eager String:避免闭包借用 ident 与后续 move ident 冲突)。
    let default_name = ident.unraw().to_string();
    // query 是否多值(Vec<T> / Option<Vec<T>>)→ 重复 key。
    let multi = is_multi_query(&pt.ty);

    match found {
        Some((k, name)) => match k.as_str() {
            "PathVariable" => {
                if optional {
                    return Err(syn::Error::new(span, "PathVariable 不能是 Option<T>"));
                }
                Ok(ParamBind::Path {
                    ident,
                    name: name.unwrap_or(default_name),
                })
            }
            "RequestParam" => Ok(ParamBind::Query {
                ident,
                name: name.unwrap_or(default_name),
                optional,
                multi,
            }),
            "RequestHeader" => Ok(ParamBind::Header {
                ident,
                name: name.unwrap_or(default_name),
                optional,
            }),
            "RequestHeaders" => Ok(ParamBind::Headers { ident }),
            "QueryMap" => {
                // QueryMap 整体按字段名展开,名字无意义;v1 也不支持 Option<T>(serde_urlencoded 对
                // Option<Struct> 行为不稳定 —— 先拒绝,后续确认稳定再放开。
                if name.is_some() {
                    return Err(syn::Error::new(
                        span,
                        "#[QueryMap] 不接受名字参数(整个结构体按字段名展开成 query)",
                    ));
                }
                if optional {
                    return Err(syn::Error::new(
                        span,
                        "#[QueryMap] 暂不支持 Option<T>(serde_urlencoded 对 Option<Struct> 行为不稳定;请传非 Option 结构体)",
                    ));
                }
                Ok(ParamBind::QueryMap { ident })
            }
            "RequestBody" => {
                if body_raw {
                    Ok(ParamBind::RawBody { ident })
                } else {
                    Ok(ParamBind::Body { ident })
                }
            }
            "FormBody" => Ok(ParamBind::FormBody { ident }),
            _ => unreachable!(),
        },
        None => {
            // 无显式注解:GET/DELETE 默认 query;POST/PUT 要求显式标注。
            match http {
                "GET" | "DELETE" => Ok(ParamBind::Query {
                    ident,
                    name: default_name,
                    optional,
                    multi,
                }),
                _ => Err(syn::Error::new(
                    span,
                    format!(
                        "{http} 方法的参数 `{default_name}` 必须显式标注 RequestParam/PathVariable/RequestHeader/RequestHeaders/QueryMap/RequestBody/FormBody"
                    ),
                )),
            }
        }
    }
}

/// 取 helper attr 的显式名字串(`#[RequestParam("x")]` → Some("x");`#[RequestParam]` → None)。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn explicit_name(attr: &Attribute) -> syn::Result<Option<String>> {
    match &attr.meta {
        Meta::Path(_) => Ok(None),
        Meta::List(list) => {
            let s = syn::parse2::<LitStr>(list.tokens.clone())
                .map_err(|_| syn::Error::new(list.span(), "helper 注解参数必须是单个字符串名字"))?;
            Ok(Some(s.value()))
        }
        Meta::NameValue(nv) => Err(syn::Error::new(nv.span(), "helper 注解不支持 name=value")),
    }
}

/// 解析 `#[RequestBody]` 形态:裸 `#[RequestBody]` → json body(`false`);`#[RequestBody(raw)]` → 原样字节(`true`)。
/// 拒绝 `#[RequestBody("x")]`(body 不带名字)、`#[RequestBody(其它标识)]`、`#[RequestBody(raw, ..)]` 等任何其它形态。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn parse_request_body_raw(attr: &Attribute) -> syn::Result<bool> {
    match &attr.meta {
        Meta::Path(_) => Ok(false),
        Meta::List(list) => {
            // 只按单个 ident 解析:字符串、多个参数、name=value 都会失败并给出固定诊断。
            // 这样 `raw` 是唯一扩展点,普通 body 与原样 body 不会被误判。
            let ident: Ident = syn::parse2(list.tokens.clone()).map_err(|_| {
                syn::Error::new(
                    list.span(),
                    "#[RequestBody] 只支持 #[RequestBody] 或 #[RequestBody(raw)]",
                )
            })?;
            if ident == "raw" {
                Ok(true)
            } else {
                Err(syn::Error::new(
                    ident.span(),
                    format!("#[RequestBody] 不支持参数 `{ident}`,只支持 #[RequestBody(raw)]"),
                ))
            }
        }
        Meta::NameValue(nv) => Err(syn::Error::new(
            nv.span(),
            "#[RequestBody] 不支持 name=value",
        )),
    }
}

/// 返回当前对象的 option 状态。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_option(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "Option";
        }
    }
    false
}

/// 合法 HTTP header name(RFC 7230 token:非空,字符 ∈ ALPHA/DIGIT/`!#$%&'*+-.^_`` `|~`)。
///
/// # 参数
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// 合法 HTTP header value(visible ASCII + SP + HTAB;挡控制字符/CR/LF,对齐 `HeaderValue::from_str`)。
///
/// # 参数
/// - `val`: 要写入 Redis 或发送到下游的值。
fn is_valid_header_value(val: &str) -> bool {
    val.bytes()
        .all(|b| b == b'\t' || (0x20..=0x7e).contains(&b))
}

/// query 参数是否「多值」(`Vec<T>` 或 `Option<Vec<T>>`)→ 生成重复 key `name=v1&name=v2`。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_multi_query(ty: &Type) -> bool {
    /// 读取泛型包装类型的参数；用于识别请求体、查询和响应包装。
    ///
    /// # 参数
    /// - `ty`: Rust 类型 AST,用于宏期类型判定。
    /// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
    fn seg_args<'a>(ty: &'a Type, name: &str) -> Option<&'a syn::PathArguments> {
        if let Type::Path(tp) = ty {
            if let Some(seg) = tp.path.segments.last() {
                if seg.ident == name {
                    return Some(&seg.arguments);
                }
            }
        }
        None
    }
    if seg_args(ty, "Vec").is_some() {
        return true;
    }
    // Option<Vec<T>>:内层是 Vec。
    if let Some(syn::PathArguments::AngleBracketed(ab)) = seg_args(ty, "Option") {
        if let Some(syn::GenericArgument::Type(inner)) = ab.args.first() {
            return seg_args(inner, "Vec").is_some();
        }
    }
    false
}

/// 从 `-> anyhow::Result<T>` / `Result<T>` 抽取 `T`;非该形状报错。
///
/// # 参数
/// - `ret`: 接口方法声明的返回类型信息。
fn extract_result_inner(ret: &ReturnType) -> syn::Result<Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new(
                ret.span(),
                "rest_client 方法返回类型必须是 anyhow::Result<T>",
            ))
        }
    };
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            if seg.ident == "Result" {
                if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                    // 只接受【单类型参数】的 Result(即 anyhow::Result<T> / 别名 Result<T> 形态)。
                    // 生成代码用 `?` 把 RestDiscoveryError 经 anyhow 转换,故 `Result<T, E>`(双参)的自定义 E
                    // 不保证能从 RestDiscoveryError 转换 → 显式拒绝,而非延后到难懂的类型不匹配。
                    let type_args: Vec<&Type> = ab
                        .args
                        .iter()
                        .filter_map(|a| match a {
                            syn::GenericArgument::Type(t) => Some(t),
                            _ => None,
                        })
                        .collect();
                    return match type_args.as_slice() {
                        [inner] => Ok((*inner).clone()),
                        _ => Err(syn::Error::new(
                            ty.span(),
                            "rest_client 方法返回类型必须是 anyhow::Result<T>(单类型参数);\
                             不支持 Result<T, E>——自定义错误类型不保证能从 RestDiscoveryError 转换",
                        )),
                    };
                }
            }
        }
    }
    Err(syn::Error::new(
        ty.span(),
        "rest_client 方法返回类型必须是 anyhow::Result<T>",
    ))
}

/// 返回当前对象的 unit 状态。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

/// 提取模板里的 `{name}` 占位名。
///
/// # 参数
/// - `template`: 路径、URL 或缓存 key 模板。
fn placeholders(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        if let Some(close) = after.find('}') {
            out.push(after[..close].to_string());
            rest = &after[close + 1..];
        } else {
            break;
        }
    }
    out
}

const FORBIDDEN_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

// ── 主流程 ──

/// 生成 REST 客户端实现；用于把 trait 方法转换为请求构造代码。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
/// - `item`: 被宏处理的 Rust item token stream。
fn rest_client_impl(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let trait_args = parse_trait_args(attr)?;
    let mut item_trait: ItemTrait = syn::parse2(item)
        .map_err(|e| syn::Error::new(e.span(), format!("#[rest_client] 只能用于 trait:{e}")))?;

    let root = rest_root();
    let trait_ident = item_trait.ident.clone();
    let vis = item_trait.vis.clone();
    let client_ident = trait_args
        .client
        .clone()
        .unwrap_or_else(|| format_ident!("{}Client", trait_ident));

    // 外层宏必须先消费方法映射与参数 helper 属性,再把它们从 trait 输出中删除。
    // 这些 helper 不是独立属性宏;如果留在输出里,编译器会把它们当未知属性处理。
    let mut impl_methods = Vec::new();
    for it in item_trait.items.iter_mut() {
        if let TraitItem::Fn(m) = it {
            impl_methods.push(gen_method(m, &trait_args, &root)?);
        }
    }

    let async_trait_attr = quote!(#[#root::__private::async_trait::async_trait]);
    let rest_ty = quote!(#root::RestDiscoveryClient);

    Ok(quote! {
        #async_trait_attr
        #item_trait

        #vis struct #client_ident {
            rest: ::std::sync::Arc<#rest_ty>,
        }

        impl #client_ident {
            /// 构造入口:调用方传入共享 `RestDiscoveryClient`,便于在应用启动时统一装配。
            #vis fn new(rest: ::std::sync::Arc<#rest_ty>) -> Self {
                Self { rest }
            }
        }

        #async_trait_attr
        impl #trait_ident for #client_ident {
            #(#impl_methods)*
        }
    })
}

/// 生成单个远程方法实现；用于处理路径、参数、请求体和响应解析。
///
/// # 参数
/// - `m`: trait 中待展开的远程调用方法声明。
/// - `trait_args`: rest client 或 mapper trait 级配置参数。
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
fn gen_method(
    m: &mut TraitItemFn,
    trait_args: &TraitArgs,
    root: &TokenStream2,
) -> syn::Result<TokenStream2> {
    // trait 方法会展开成 async_trait impl;非 async 方法没有统一的发送点,直接拒绝。
    if m.sig.asyncness.is_none() {
        return Err(syn::Error::new(
            m.sig.span(),
            "rest_client trait 方法必须是 async fn",
        ));
    }

    // 找方法映射属性(恰一个),并挡住服务端路由宏误贴到客户端 trait。
    // 映射属性在这里被消费,最终 trait 里只保留普通 Rust 属性。
    let mut http: Option<Ident> = None;
    let mut mapping: Option<MappingArgs> = None;
    let mut kept_attrs = Vec::new();
    for attr in m.attrs.drain(..) {
        if let Some(method) = http_method_of(&attr) {
            if http.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "一个方法只能有一个 HTTP 映射属性",
                ));
            }
            mapping = Some(parse_mapping_attr(&attr)?);
            http = Some(method);
        } else if matches!(
            attr.path().get_ident().map(|i| i.to_string()).as_deref(),
            Some("get_mapping")
                | Some("post_mapping")
                | Some("put_mapping")
                | Some("patch_mapping")
                | Some("delete_mapping")
        ) {
            return Err(syn::Error::new(
                attr.span(),
                "rest_client 客户端方法请用 PascalCase #[GetMapping]/#[PostMapping]/...,snake_case 仅供服务端 axum",
            ));
        } else {
            kept_attrs.push(attr);
        }
    }
    m.attrs = kept_attrs;

    let http = http.ok_or_else(|| {
        syn::Error::new(
            m.sig.span(),
            "rest_client 方法必须有 #[GetMapping]/#[PostMapping]/#[PutMapping]/#[PatchMapping]/#[DeleteMapping]",
        )
    })?;
    let mapping = mapping.unwrap();
    let http_str = http.to_string();

    // 返回类型 T。
    let ret_ty = extract_result_inner(&m.sig.output)?;
    let ret_unit = is_unit(&ret_ty);

    // url 与 service 二选一:
    // - url 模式是外部直连,不允许再带 service/context_path/path/scheme。
    // - service 模式必须生成 service_request,不能拼成 http://service/... 让运行时再猜。
    let url_mode = mapping.url.is_some();
    if url_mode
        && (mapping.service.is_some()
            || mapping.context_path.is_some()
            || mapping.path.is_some()
            || mapping.scheme.is_some())
    {
        return Err(syn::Error::new(
            m.sig.span(),
            "url 模式不能同时配置 service/context_path/path/scheme",
        ));
    }

    // 路径模板(service 模式 = path;url 模式 = url)。
    let template = if url_mode {
        mapping.url.clone().unwrap()
    } else {
        mapping.path.clone().ok_or_else(|| {
            syn::Error::new(m.sig.span(), "service 模式必须配置 path/value/remote")
        })?
    };

    // 参数解析会删除 helper attr,同时记录每个参数如何进入 path/query/header/body。
    let mut params = Vec::new();
    for input in m.sig.inputs.iter_mut() {
        if let FnArg::Typed(pt) = input {
            params.push(classify_param(pt, &http_str)?);
        }
    }

    // 先做统计校验,让 body/header 的互斥关系在宏展开期报错。这里比生成代码后再依赖
    // builder 覆盖更严格,调用者能在接口声明处看到真实原因。
    let body_count = params
        .iter()
        .filter(|p| matches!(p, ParamBind::Body { .. }))
        .count();
    let raw_body_count = params
        .iter()
        .filter(|p| matches!(p, ParamBind::RawBody { .. }))
        .count();
    let headers_count = params
        .iter()
        .filter(|p| matches!(p, ParamBind::Headers { .. }))
        .count();
    if body_count > 1 {
        return Err(syn::Error::new(
            m.sig.span(),
            "一个方法最多一个 #[RequestBody]",
        ));
    }
    if raw_body_count > 1 {
        return Err(syn::Error::new(
            m.sig.span(),
            "一个方法最多一个 #[RequestBody(raw)]",
        ));
    }
    if body_count > 0 && raw_body_count > 0 {
        return Err(syn::Error::new(
            m.sig.span(),
            "#[RequestBody] 与 #[RequestBody(raw)] 不能同时出现(json / raw body 互斥)",
        ));
    }
    if headers_count > 1 {
        return Err(syn::Error::new(
            m.sig.span(),
            "一个方法最多一个 #[RequestHeaders]",
        ));
    }
    if body_count > 0 && matches!(http_str.as_str(), "GET" | "DELETE") {
        return Err(syn::Error::new(
            m.sig.span(),
            "GET/DELETE 第一版不支持 #[RequestBody]",
        ));
    }
    // raw body 与 form 一样仅 POST/PUT/PATCH(GET/DELETE 不带 body)。
    if raw_body_count > 0 && !matches!(http_str.as_str(), "POST" | "PUT" | "PATCH") {
        return Err(syn::Error::new(
            m.sig.span(),
            "#[RequestBody(raw)] 仅 POST/PUT/PATCH 支持",
        ));
    }

    // #[FormBody](application/x-www-form-urlencoded):≤1 个、不与 #[RequestBody] 并存、仅 POST/PUT/PATCH。
    let form_body_count = params
        .iter()
        .filter(|p| matches!(p, ParamBind::FormBody { .. }))
        .count();
    if form_body_count > 1 {
        return Err(syn::Error::new(
            m.sig.span(),
            "一个方法最多一个 #[FormBody]",
        ));
    }
    if body_count > 0 && form_body_count > 0 {
        return Err(syn::Error::new(
            m.sig.span(),
            "#[RequestBody] 与 #[FormBody] 不能同时出现(json / form body 互斥)",
        ));
    }
    if raw_body_count > 0 && form_body_count > 0 {
        return Err(syn::Error::new(
            m.sig.span(),
            "#[RequestBody(raw)] 与 #[FormBody] 不能同时出现(raw / form body 互斥)",
        ));
    }
    if form_body_count > 0 && !matches!(http_str.as_str(), "POST" | "PUT" | "PATCH") {
        return Err(syn::Error::new(
            m.sig.span(),
            "#[FormBody] 仅 POST/PUT/PATCH 支持",
        ));
    }

    // consumes 必须和 body 绑定方式一致;否则 Content-Type 与实际编码会不一致。
    // raw body 是例外:调用方已经提供字节,这里只禁止 form 专属媒体类型。
    if let Some(c) = &mapping.consumes {
        if raw_body_count > 0 {
            // raw body:Content-Type 完全由 consumes 决定,允许任意媒体类型(含 application/json:可发预序列化字节);
            // 但 x-www-form-urlencoded 是 #[FormBody] 的专属编码,配 raw 语义打架 → 拒绝。
            if c == "application/x-www-form-urlencoded" {
                return Err(syn::Error::new(
                    m.sig.span(),
                    "consumes=\"application/x-www-form-urlencoded\" 应配 #[FormBody],不要配 #[RequestBody(raw)]",
                ));
            }
        } else {
            match c.as_str() {
                "application/json" => {
                    if form_body_count > 0 {
                        return Err(syn::Error::new(
                            m.sig.span(),
                            "consumes=\"application/json\" 与 #[FormBody] 冲突(form body 用 application/x-www-form-urlencoded)",
                        ));
                    }
                }
                "application/x-www-form-urlencoded" => {
                    if form_body_count == 0 {
                        return Err(syn::Error::new(
                            m.sig.span(),
                            "consumes=\"application/x-www-form-urlencoded\" 需要一个 #[FormBody] 参数",
                        ));
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        m.sig.span(),
                        format!(
                            "consumes 只支持 \"application/json\" / \"application/x-www-form-urlencoded\"(无 #[RequestBody(raw)] 时;当前 {other:?})"
                        ),
                    ));
                }
            }
        }
    }

    // unwrap = "data":只对 response=json(默认)生效、不能配 `-> Result<()>`、字段名只能是单层字段(无 `.`/`/`/空白/`{}`)。
    if let Some(field) = &mapping.unwrap_field {
        let resp = mapping.response.as_deref().unwrap_or("json");
        if resp != "json" {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("unwrap 仅 response=\"json\" 时可用(当前 response={resp:?})"),
            ));
        }
        if ret_unit {
            return Err(syn::Error::new(
                m.sig.span(),
                "unwrap 不能用于 `-> anyhow::Result<()>`(只校验状态时无字段可解包)",
            ));
        }
        if field.is_empty()
            || field.contains(['.', '/', '{', '}'])
            || field.chars().any(|c| c.is_whitespace())
        {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("unwrap 字段名 {field:?} 非法:仅支持单层顶层字段名(不含 `.` `/` 空白 `{{` `}}`)"),
            ));
        }
    }

    // path 变量 ↔ 占位 双向匹配。这里故意严格:缺失、重复、拼写不一致都在编译期暴露。
    let ph = placeholders(&template);
    // 占位名合法性:非空、不含 `/`/`{`/`}`(挡 `{}` / `{a/b}` / 嵌套等坏模板)。
    for name in &ph {
        if name.is_empty() {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("路径模板 {template:?} 含空占位 `{{}}`,占位名不能为空"),
            ));
        }
        if name.contains(['/', '{', '}']) {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("路径占位 `{{{name}}}` 名非法:不能包含 `/` `{{` `}}`"),
            ));
        }
    }
    // PathVariable 绑定名去重:同名多个参数 → compile_error(避免重复绑定同一占位语义不清)。
    {
        let mut seen = std::collections::HashSet::new();
        for p in &params {
            if let ParamBind::Path { name, ident } = p {
                if !seen.insert(name.clone()) {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("PathVariable `{name}` 重复绑定(一个占位只能对应一个参数)"),
                    ));
                }
            }
        }
    }
    for p in &params {
        if let ParamBind::Path { name, ident } = p {
            if !ph.iter().any(|x| x == name) {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("PathVariable `{name}` 在路径模板里找不到占位 {{{name}}}"),
                ));
            }
        }
    }
    for name in &ph {
        let matched = params
            .iter()
            .any(|p| matches!(p, ParamBind::Path { name: n, .. } if n == name));
        if !matched {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("路径占位 {{{name}}} 缺少对应 #[PathVariable] 参数"),
            ));
        }
    }

    // service 模式下 path/context_path 都是逻辑路径,必须以 `/` 开头;完整 URL 只能走 url 模式。
    if !url_mode {
        if !template.starts_with('/') {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("path 必须以 `/` 开头,当前 {template:?}"),
            ));
        }
        let ctx = mapping
            .context_path
            .clone()
            .or_else(|| trait_args.context_path.clone())
            .unwrap_or_default();
        if !ctx.is_empty() && !ctx.starts_with('/') {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("context_path 必须为空或以 `/` 开头,当前 {ctx:?}"),
            ));
        }
    }

    // 静态 header 合法性:挡 Host/hop-by-hop + 校验 name/value 合法。
    // Host 和连接层 header 由 HTTP client 或负载均衡层控制,不能由声明式接口注入。
    for (k, v) in &mapping.headers {
        if FORBIDDEN_HEADERS.iter().any(|f| k.eq_ignore_ascii_case(f)) {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("静态 header 不能配置连接层/Host header:{k}"),
            ));
        }
        if !is_valid_header_name(k) {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("静态 header 名非法(非合法 HTTP header name):{k:?}"),
            ));
        }
        if !is_valid_header_value(v) {
            return Err(syn::Error::new(
                m.sig.span(),
                format!("静态 header 值非法(含控制字符 / CR / LF):{v:?}"),
            ));
        }
    }

    // ── 代码生成 ──
    let body = gen_body(
        &mapping, trait_args, &http, url_mode, &template, &params, &ret_ty, ret_unit, root,
    )?;
    let sig = &m.sig;
    // 把方法上【保留的】普通属性(cfg/doc/allow 等,映射属性已在上面消费掉)同步到生成的 impl 方法。
    // 否则 #[cfg(feature=...)] 只 gate 了 trait 里的方法【声明】(随 #item_trait 输出),而 impl 方法
    // 恒被生成 → 当该 cfg 关闭时,impl 里会多出一个 trait 并未声明的方法 → 编译报错
    // "method ... is not a member of trait";doc/allow 同理不该在 impl 侧丢失。m.attrs 此刻即 kept_attrs。
    let attrs = &m.attrs;
    Ok(quote! { #(#attrs)* #sig { #body } })
}

/// 生成请求体构造代码；用于按注解和参数类型选择序列化方式。
///
/// # 参数
/// - `mapping`: HTTP 映射属性解析结果。
/// - `trait_args`: rest client 或 mapper trait 级配置参数。
/// - `http`: 当前方法对应的 HTTP verb。
/// - `url_mode`: URL 构造模式,决定直连 URL、服务发现 URL 或 trait base_url 的拼接方式。
/// - `template`: 路径、URL 或缓存 key 模板。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `ret_ty`: 生成客户端调用代码时使用的返回类型。
/// - `ret_unit`: 返回类型是否为单元类型。
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
#[allow(clippy::too_many_arguments)]
fn gen_body(
    mapping: &MappingArgs,
    trait_args: &TraitArgs,
    http: &Ident,
    url_mode: bool,
    template: &str,
    params: &[ParamBind],
    ret_ty: &Type,
    ret_unit: bool,
    root: &TokenStream2,
) -> syn::Result<TokenStream2> {
    let method_tok = quote!(#root::reqwest::Method::#http);

    // path 变量列表会传给运行时 helper 做 percent-encode,避免宏展开里复制复杂转义逻辑。
    let pv: Vec<TokenStream2> = params
        .iter()
        .filter_map(|p| match p {
            ParamBind::Path { ident, name } => {
                Some(quote!((#name, ::std::string::ToString::to_string(&#ident))))
            }
            _ => None,
        })
        .collect();

    // 前置设置只改变 builder 元数据,不触发发送;后续 body/query 绑定仍可继续链式追加。
    let mut pre = Vec::new();
    if !url_mode {
        if let Some(s) = mapping.scheme.clone().or_else(|| trait_args.scheme.clone()) {
            let variant = match s.as_str() {
                "http" => format_ident!("Http"),
                "https" => format_ident!("Https"),
                other => {
                    return Err(syn::Error::new(
                        http.span(),
                        format!("scheme 只能是 http/https,当前 {other:?}"),
                    ))
                }
            };
            pre.push(quote! { __req = __req.scheme(#root::InstanceScheme::#variant); });
        }
    }
    if let Some(p) = &mapping.produces {
        pre.push(quote! { __req = __req.header("Accept", #p); });
    }
    // consumes 写入 Content-Type:仅在【body 不自带 Content-Type】时显式写 —— #[RequestBody](.json)/
    // #[FormBody](.form) 的 builder 已设同值 Content-Type(避免重复 header);而 #[RequestBody(raw)] 的 raw_body()
    // 不自设 Content-Type,故 raw body + consumes 走这里显式写(也覆盖「无 body 但要求 Content-Type」)。
    // 后面的静态/动态 header 会继续 append,运行时 HeaderMap 使用 last-wins,可显式覆盖默认值。
    if let Some(c) = &mapping.consumes {
        let self_typed_body = params
            .iter()
            .any(|p| matches!(p, ParamBind::Body { .. } | ParamBind::FormBody { .. }));
        if !self_typed_body {
            pre.push(quote! { __req = __req.header("Content-Type", #c); });
        }
    }
    for (k, v) in &mapping.headers {
        pre.push(quote! { __req = __req.header(#k, #v); });
    }
    for p in params {
        if let ParamBind::Headers { ident } = p {
            // 统一借用:`&#ident` 对 by-value `HeaderMap` 取引用;对 `&HeaderMap` 入参得 `&&HeaderMap`,
            // 由 deref coercion 收敛回 `&HeaderMap`。两种入参(HeaderMap / &HeaderMap)都可用。
            pre.push(quote! { __req = __req.headers_from_map(&#ident); });
        }
    }

    // query + 单 header + body 参数。
    let mut binds = Vec::new();
    for p in params {
        match p {
            ParamBind::Query {
                ident,
                name,
                optional,
                multi,
            } => {
                // multi(Vec<T>)→ query_pairs 生成重复 key;单值 → query_pair。optional 各自再裹一层 Some 判断。
                binds.push(match (*optional, *multi) {
                    (false, false) => quote! { __req = __req.query_pair(#name, &#ident); },
                    (true, false) => quote! {
                        if let ::core::option::Option::Some(__v) = &#ident {
                            __req = __req.query_pair(#name, __v);
                        }
                    },
                    (false, true) => quote! { __req = __req.query_pairs(#name, &#ident); },
                    (true, true) => quote! {
                        if let ::core::option::Option::Some(__v) = &#ident {
                            __req = __req.query_pairs(#name, __v);
                        }
                    },
                });
            }
            ParamBind::Header {
                ident,
                name,
                optional,
            } => {
                if *optional {
                    binds.push(quote! {
                        if let ::core::option::Option::Some(__v) = &#ident {
                            __req = __req.header(#name, ::std::string::ToString::to_string(__v));
                        }
                    });
                } else {
                    binds.push(quote! {
                        __req = __req.header(#name, ::std::string::ToString::to_string(&#ident));
                    });
                }
            }
            ParamBind::QueryMap { ident } => {
                // 整个结构体序列化成 query 片段,按声明顺序 append(与 RequestParam 共存时不覆盖同名 key)。
                binds.push(quote! { __req = __req.query(&#ident); });
            }
            ParamBind::Body { ident } => {
                binds.push(quote! { __req = __req.json(&#ident); });
            }
            ParamBind::RawBody { ident } => {
                // 原样字节;不做 json/urlencoded 编码,Content-Type 已由上面的 consumes/header 处理。
                // 这里统一传引用,运行时立刻复制到 Bytes,生成代码不需要关心入参是 String/Vec/Bytes/&str。
                binds.push(quote! { __req = __req.raw_body(&#ident); });
            }
            ParamBind::FormBody { ident } => {
                binds.push(quote! { __req = __req.form(&#ident); });
            }
            ParamBind::Path { .. } | ParamBind::Headers { .. } => {}
        }
    }

    // 仅当有后续修改语句时才 `mut`,避免 unused_mut 警告。
    let mut_tok = if pre.is_empty() && binds.is_empty() {
        quote!()
    } else {
        quote!(mut)
    };

    // builder 起点:
    // - url 模式:模板替换后直接 request。
    // - service 模式:context_path + path 归一化后走 service_request,保持显式内部调用语义。
    let start = if url_mode {
        quote! {
            let __url = #root::__private::replace_path_variables(#template, &[ #(#pv),* ])?;
            let #mut_tok __req = self.rest.request(#method_tok, __url);
        }
    } else {
        let service = mapping
            .service
            .clone()
            .or_else(|| trait_args.service.clone())
            .ok_or_else(|| syn::Error::new(http.span(), "service 模式必须配置 service"))?;
        let ctx = mapping
            .context_path
            .clone()
            .or_else(|| trait_args.context_path.clone())
            .unwrap_or_default();
        quote! {
            let __pv = #root::__private::replace_path_variables(#template, &[ #(#pv),* ])?;
            let __path = #root::__private::join_path(#ctx, &__pv);
            let #mut_tok __req = self.rest.service_request(#service, #method_tok, __path);
        }
    };

    // 发送派发由 response 参数决定;unit 返回值只检查状态,其它类型按指定形态解码。
    let response = mapping.response.as_deref().unwrap_or("json");
    let dispatch = match response {
        "json" => {
            if ret_unit {
                quote! { __req.send_ok().await?; ::core::result::Result::Ok(()) }
            } else if let Some(field) = &mapping.unwrap_field {
                // 解包顶层字段再反序列化(gen_method 已校验 field 合法 + response=json + 非 unit)。
                quote! { ::core::result::Result::Ok(__req.send_json_unwrap::<#ret_ty>(#field).await?) }
            } else {
                quote! { ::core::result::Result::Ok(__req.send_json::<#ret_ty>().await?) }
            }
        }
        "text" => quote! { ::core::result::Result::Ok(__req.send_text().await?) },
        "bytes" => quote! { ::core::result::Result::Ok(__req.send_bytes().await?) },
        "raw" => quote! { ::core::result::Result::Ok(__req.send().await?) },
        other => {
            return Err(syn::Error::new(
                http.span(),
                format!("response 只能是 json/text/bytes/raw,当前 {other:?}"),
            ))
        }
    };

    Ok(quote! {
        #start
        #(#pre)*
        #(#binds)*
        #dispatch
    })
}
