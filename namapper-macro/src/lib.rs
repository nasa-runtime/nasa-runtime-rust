//! SQL mapper 声明式过程宏。
//!
//! `#[Mapper]` 读取 trait 方法上的 SQL 注解，在编译期生成基于 `sqlx`、事务运行时和可选 L2 缓存的实现。
use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{
    AngleBracketedGenericArguments, Attribute, Data, DeriveInput, Expr, ExprArray, ExprLit, Fields,
    FnArg, GenericArgument, Ident, ItemTrait, Lit, LitBool, LitInt, LitStr, Local, Meta, Pat,
    PatIdent, Path, PathArguments, ReturnType, Stmt, TraitItem, TraitItemFn, Type, TypePath,
};
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
/// - `item`: 被属性宏或派生宏处理的 Rust item token stream。
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Mapper(attr: TokenStream, item: TokenStream) -> TokenStream {
    match mapper_impl(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
///
/// # 参数
/// - `item`: 被属性宏或派生宏处理的 Rust item token stream。
#[proc_macro_derive(MapperOrderField, attributes(mapper_order_field))]
#[allow(non_snake_case)]
pub fn derive_mapper_order_field(item: TokenStream) -> TokenStream {
    match mapper_order_field_impl(item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
///
/// # 参数
/// - `item`: 被属性宏或派生宏处理的 Rust item token stream。
#[proc_macro_derive(MapperEnum)]
#[allow(non_snake_case)]
pub fn derive_mapper_enum(item: TokenStream) -> TokenStream {
    match mapper_enum_impl(item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

macro_rules! sql_attr_stub {
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
                    "] 只能放在 #[Mapper] trait 方法上,由 #[Mapper] 统一消费"
                ));
                #item2
            }
            .into()
        }
    };
}

sql_attr_stub!(Query);
sql_attr_stub!(StreamQuery);
sql_attr_stub!(Insert);
sql_attr_stub!(Update);
sql_attr_stub!(Delete);
sql_attr_stub!(Execute);
///
/// # 参数
/// - `item`: 被宏处理的 Rust item token stream。
fn mapper_enum_impl(item: TokenStream2) -> syn::Result<TokenStream2> {
    let root = match nasa_macro_support::runtime_root("mapper", "namapper") {
        Ok(root) => root,
        Err(msg) => return Ok(quote! { ::core::compile_error!(#msg); }),
    };

    let input = syn::parse2::<DeriveInput>(item)?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(MapperEnum)] 首版不支持泛型 enum",
        ));
    }

    let enum_data = match &input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "#[derive(MapperEnum)] 只能用于 enum",
            ));
        }
    };

    let ident = &input.ident;
    let mut ordinal_arms = Vec::new();
    let mut from_arms = Vec::new();
    for (idx, variant) in enum_data.variants.iter().enumerate() {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                "#[derive(MapperEnum)] 只支持无字段 enum 变体",
            ));
        }
        let variant_ident = &variant.ident;
        let ordinal = idx as i32;
        ordinal_arms.push(quote! { Self::#variant_ident => #ordinal });
        from_arms.push(quote! { #ordinal => ::core::option::Option::Some(Self::#variant_ident) });
    }

    Ok(quote! {
        impl #root::MapperEnum for #ident {
            /// 返回 enum 变体在数据库中保存的稳定序号。
            ///
            /// 序号按源码声明顺序生成,用于 MyBatis 风格 enum 与整数字段之间的轻量映射。
            fn ordinal(self) -> i32 {
                match self {
                    #(#ordinal_arms,)*
                }
            }
            ///
            /// # 参数
            /// - `value`: 从数据库整数字段读取到的 enum 序号。
            fn from_ordinal(value: i32) -> ::core::option::Option<Self> {
                match value {
                    #(#from_arms,)*
                    _ => ::core::option::Option::None,
                }
            }
        }
    })
}
///
/// # 参数
/// - `item`: 被宏处理的 Rust item token stream。
fn mapper_order_field_impl(item: TokenStream2) -> syn::Result<TokenStream2> {
    let root = match nasa_macro_support::runtime_root("mapper", "namapper") {
        Ok(root) => root,
        Err(msg) => return Ok(quote! { ::core::compile_error!(#msg); }),
    };

    let input = syn::parse2::<DeriveInput>(item)?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(MapperOrderField)] 首版不支持泛型 enum",
        ));
    }

    let enum_data = match &input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "#[derive(MapperOrderField)] 只能用于 enum",
            ));
        }
    };

    let ident = &input.ident;
    let mut arms = Vec::new();
    for variant in &enum_data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                "#[derive(MapperOrderField)] 只支持无字段 enum 变体",
            ));
        }
        let column = mapper_order_field_column(variant)?;
        validate_order_column_literal(&column, variant.ident.span())?;
        let variant_ident = &variant.ident;
        let column_lit = LitStr::new(&column, variant.ident.span());
        arms.push(quote! { Self::#variant_ident => #column_lit });
    }

    Ok(quote! {
        impl #root::MapperOrderField for #ident {
            /// 返回该排序枚举变体绑定的真实数据库列名。
            ///
            /// mapper 的 `order_by` 动态片段只允许从这里取列名,避免业务侧拼接任意 SQL。
            fn mapper_order_field(self) -> &'static str {
                match self {
                    #(#arms,)*
                }
            }
        }
    })
}
///
/// # 参数
/// - `variant`: 枚举派生处理中正在生成映射的变体。
fn mapper_order_field_column(variant: &syn::Variant) -> syn::Result<String> {
    let mut column = None;
    for attr in &variant.attrs {
        if !attr.path().is_ident("mapper_order_field") {
            continue;
        }
        if column.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "mapper_order_field 属性不能重复",
            ));
        }
        match &attr.meta {
            Meta::List(list) => {
                let lit = syn::parse2::<LitStr>(list.tokens.clone())?;
                column = Some(lit.value());
            }
            Meta::NameValue(nv) => {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(lit), ..
                }) = &nv.value
                else {
                    return Err(syn::Error::new_spanned(
                        &nv.value,
                        "mapper_order_field 属性值必须是字符串字面量",
                    ));
                };
                column = Some(lit.value());
            }
            Meta::Path(_) => {
                return Err(syn::Error::new_spanned(
                    attr,
                    "mapper_order_field 属性必须写成 #[mapper_order_field(\"column\")]",
                ));
            }
        }
    }
    Ok(column.unwrap_or_else(|| camel_to_snake(&variant.ident.to_string())))
}
///
/// # 参数
/// - `value`: Rust 标识符或 enum 变体名。
fn camel_to_snake(value: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// mapper 方法声明的 SQL 操作类型。
///
/// 宏根据属性类型决定生成查询、流式查询或写操作代码,并据此推导缓存清理和事务默认行为。
#[derive(Clone, Copy, PartialEq, Eq)]
enum SqlKind {
    /// 返回集合、单行、可选行或标量的普通查询。
    Query,
    /// 返回 `MapperStream` 的流式查询。
    StreamQuery,
    /// 插入语句,默认按写操作处理缓存清理。
    Insert,
    /// 更新语句,默认按写操作处理缓存清理。
    Update,
    /// 删除语句,默认按写操作处理缓存清理。
    Delete,
    /// 通用执行语句,默认按写操作处理缓存清理。
    Execute,
}

impl SqlKind {
    ///
    /// # 参数
    /// - `attr`: 属性宏括号内的 token stream。
    fn of(attr: &Attribute) -> Option<Self> {
        let ident = attr.path().segments.last()?.ident.to_string();
        match ident.as_str() {
            "Query" => Some(Self::Query),
            "StreamQuery" => Some(Self::StreamQuery),
            "Insert" => Some(Self::Insert),
            "Update" => Some(Self::Update),
            "Delete" => Some(Self::Delete),
            "Execute" => Some(Self::Execute),
            _ => None,
        }
    }

    /// 判断当前 mapper 方法是否是普通查询。
    fn is_query(self) -> bool {
        matches!(self, Self::Query)
    }

    /// 判断当前 mapper 方法是否是读请求。
    ///
    /// 普通查询和流式查询都属于读请求,默认不会触发缓存清理。
    fn is_read(self) -> bool {
        matches!(self, Self::Query | Self::StreamQuery)
    }

    /// 判断当前 mapper 方法是否是流式查询。
    fn is_stream(self) -> bool {
        matches!(self, Self::StreamQuery)
    }

    /// 返回该 SQL 类型默认是否需要清理缓存。
    ///
    /// 写操作默认清理 L2,读操作默认不清理；方法级 `flush_cache` 可覆盖该行为。
    fn default_flush_cache(self) -> bool {
        !self.is_read()
    }
}

/// L2 缓存异常的处理策略。
///
/// 查询缓存失败时可选择旁路继续查库,也可选择严格暴露错误给业务侧。
#[derive(Clone, Copy)]
enum CacheErrors {
    /// 缓存异常时跳过缓存并继续走数据库。
    Bypass,
    /// 缓存异常直接返回错误。
    Strict,
}

impl CacheErrors {
    /// 解析 `cache_errors` 属性值。
    ///
    /// # 参数
    /// - `value`: mapper 属性中的缓存错误策略字符串。
    /// - `span`: 源码位置,用于生成精确的编译期错误。
    fn parse(value: &str, span: proc_macro2::Span) -> syn::Result<Self> {
        match value {
            "bypass" => Ok(Self::Bypass),
            "strict" => Ok(Self::Strict),
            other => Err(syn::Error::new(
                span,
                format!("cache_errors 不支持 `{other}`,只能是 \"bypass\" 或 \"strict\""),
            )),
        }
    }

    /// 判断缓存错误策略是否是严格模式。
    ///
    /// 严格模式会把缓存读写错误返回给业务,旁路模式则降级为只走数据库。
    fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

/// mapper 方法的事务要求。
///
/// 生成代码用该值决定是否自动复用当前事务、强制要求事务或走普通连接。
#[derive(Clone, Copy)]
enum TxMode {
    /// 自动选择事务上下文;存在事务则复用,否则走数据源连接池。
    Auto,
    /// 方法必须在事务中调用,否则返回错误。
    Mandatory,
}

impl TxMode {
    /// 解析 `tx` 属性值。
    ///
    /// # 参数
    /// - `value`: mapper 属性中的事务策略字符串。
    /// - `span`: 源码位置,用于生成精确的编译期错误。
    fn parse(value: &str, span: proc_macro2::Span) -> syn::Result<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "mandatory" => Ok(Self::Mandatory),
            "never" => Err(syn::Error::new(
                span,
                "tx = \"never\" 首版不支持,请使用 tx = \"auto\" 或 tx = \"mandatory\"",
            )),
            other => Err(syn::Error::new(
                span,
                format!("tx 不支持 `{other}`,只能是 \"auto\" / \"mandatory\""),
            )),
        }
    }
}

/// mapper trait 级别的宏属性配置。
///
/// 这些配置会作为所有方法的默认值,方法级属性可以覆盖 datasource、缓存、事务和校验等策略。
struct TraitArgs {
    key: Option<String>,
    datasource: Option<String>,
    cache: bool,
    cache_in_tx: bool,
    cache_ttl_ms: Option<u64>,
    cache_errors: CacheErrors,
    cache_codec: Option<Path>,
    strict_params: bool,
    clear_also: Vec<String>,
    clear_when: Vec<String>,
    client: Option<Ident>,
}

impl Default for TraitArgs {
    /// 构造 mapper trait 级别的默认属性。
    ///
    /// 默认启用缓存但不在事务内写共享 L2,缓存异常走旁路,保证未配置业务以数据库结果为准。
    fn default() -> Self {
        Self {
            key: None,
            datasource: None,
            cache: true,
            cache_in_tx: false,
            cache_ttl_ms: None,
            cache_errors: CacheErrors::Bypass,
            cache_codec: None,
            strict_params: false,
            clear_also: Vec::new(),
            clear_when: Vec::new(),
            client: None,
        }
    }
}

/// mapper 方法级别的宏属性配置。
///
/// 用于保存 `#[Query]`/`#[Update]` 等属性上的 SQL、fetch、事务、缓存 codec 和清理策略。
#[derive(Default)]
struct MethodArgs {
    sql: Option<String>,
    datasource: Option<String>,
    checked: Option<bool>,
    fetch: Option<String>,
    tx: Option<TxMode>,
    cache: Option<bool>,
    cache_in_tx: Option<bool>,
    hash_key_suffix: Option<String>,
    cache_ttl_ms: Option<u64>,
    cache_errors: Option<CacheErrors>,
    cache_codec: Option<Path>,
    typed_cache_codec: Option<Path>,
    strict_params: Option<bool>,
    flush_cache: Option<bool>,
    flush_refs: Option<bool>,
}

/// mapper 方法形参信息。
///
/// 宏会把业务方法参数拆成名称和类型,用于 SQL bind 校验、动态 SQL 表达式求值和生成调用代码。
#[derive(Clone)]
struct ParamInfo {
    ident: Ident,
    ty: Type,
}

/// SQL bind 参数的形态。
///
/// 标量直接生成单个占位符,列表只能在 `in_list` 或 `foreach` 场景展开。
#[derive(Clone, Copy, PartialEq, Eq)]
enum BindKind {
    /// 普通单值 bind 参数。
    Scalar,
    /// 集合 bind 参数,需要在 SQL 生成阶段展开多个占位符。
    List,
}

/// SQL bind 占位符解析结果。
///
/// 记录根参数、属性路径、最终 bind 名称和标量/列表类型,供静态 SQL 和动态 SQL 生成绑定代码。
#[derive(Clone)]
struct BindInfo {
    root: Ident,
    path: Vec<Ident>,
    name: String,
    kind: BindKind,
}

/// 动态 SQL 解析后的 AST 节点。
///
/// 生成阶段按节点树重建 SQL 字符串和 bind 列表,同时保留条件分支、循环和安全校验信息。
#[derive(Clone)]
enum SqlNode {
    /// 静态 SQL 文本片段。
    Text(String),
    /// `#{}` 或动态节点内解析出的 bind 参数。
    Bind(BindInfo),
    /// `<if>` 条件节点。
    If {
        /// 控制该节点是否输出的布尔表达式。
        test: Expr,
        /// 条件成立时输出的子节点。
        body: Vec<SqlNode>,
    },
    /// `<choose>` 条件分支节点。
    Choose {
        /// 按声明顺序尝试匹配的 `<when>` 分支。
        whens: Vec<WhenNode>,
        /// 无 `<when>` 命中时输出的 `<otherwise>` 分支。
        otherwise: Vec<SqlNode>,
    },
    /// `<foreach>` 集合展开节点。
    Foreach {
        /// 被遍历的 mapper 方法参数名。
        collection: Ident,
        /// 循环体内代表当前元素的变量名。
        item: Ident,
        /// 集合展开前写入的 SQL 文本。
        open: String,
        /// 相邻元素之间写入的分隔符。
        separator: String,
        /// 集合展开后写入的 SQL 文本。
        close: String,
        /// 每个集合元素对应输出的子节点。
        body: Vec<SqlNode>,
    },
    /// `<trim>`/`<where>`/`<set>` 规范化 SQL 前后缀的节点。
    Trim {
        /// 非空 body 前追加的 SQL 前缀。
        prefix: String,
        /// 非空 body 后追加的 SQL 后缀。
        suffix: String,
        /// body 开头需要剔除的 SQL token。
        prefix_overrides: Vec<String>,
        /// body 结尾需要剔除的 SQL token。
        suffix_overrides: Vec<String>,
        /// 需要参与 trim 规范化的子节点。
        body: Vec<SqlNode>,
    },
    /// `<order_by>` 类型安全排序节点。
    OrderBy {
        /// 引用的 `OrderBy<T>` 方法参数名。
        value: Ident,
    },
}

/// `<when>` 分支节点。
///
/// 保存 choose 分支的判断表达式和命中后要输出的 SQL 子节点。
#[derive(Clone)]
struct WhenNode {
    test: Expr,
    body: Vec<SqlNode>,
}

/// 动态 SQL 计划。
///
/// 由 XML 风格 mapper 片段解析得到,在代码生成阶段翻译为运行时 SQL builder 逻辑。
#[derive(Clone)]
struct DynamicSqlPlan {
    nodes: Vec<SqlNode>,
}

/// 单个 mapper 方法的完整生成计划。
///
/// 汇总 SQL 类型、规范化 SQL、绑定参数、动态 SQL 节点、事务模式、缓存策略和返回值提取方式。
struct MethodPlan {
    method: TraitItemFn,
    kind: SqlKind,
    normalized_sql: String,
    sql_fragments: Vec<String>,
    binds: Vec<BindInfo>,
    dynamic: Option<DynamicSqlPlan>,
    params: Vec<ParamInfo>,
    fetch: Option<String>,
    tx: TxMode,
    datasource: Option<String>,
    cache: bool,
    cache_in_tx: bool,
    hash_key_suffix: Option<String>,
    cache_ttl_ms: Option<u64>,
    cache_errors: CacheErrors,
    cache_codec: Option<Path>,
    typed_cache_codec: Option<Path>,
    flush_cache: bool,
    flush_refs: bool,
    checked: bool,
}

type ParsedSqlTemplate = (String, Vec<String>, Vec<BindInfo>, Option<DynamicSqlPlan>);
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
/// - `item`: 被宏处理的 Rust item token stream。
fn mapper_impl(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let root = match nasa_macro_support::runtime_root("mapper", "namapper") {
        Ok(root) => root,
        Err(msg) => return Ok(quote! { ::core::compile_error!(#msg); }),
    };

    let trait_args = parse_trait_args(attr)?;
    let mut input = syn::parse2::<ItemTrait>(item)?;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[Mapper] 首版不支持 trait 泛型",
        ));
    }
    if has_async_trait_attr(&input.attrs) {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[Mapper] 会自动插入 async_trait,请不要手写 #[async_trait]",
        ));
    }

    let trait_ident = input.ident.clone();
    let client_ident = trait_args
        .client
        .clone()
        .unwrap_or_else(|| format_ident!("{}Client", trait_ident));
    let key_expr = key_expr(trait_args.key.as_deref(), &trait_ident);
    let meta_ident = format_ident!(
        "__{}_MAPPER_CACHE_META",
        trait_ident.to_string().to_ascii_uppercase()
    );

    let mut plans = Vec::new();
    let mut cleaned_items = Vec::new();
    let items = std::mem::take(&mut input.items);
    for item in items {
        match item {
            TraitItem::Fn(method) => {
                let plan = plan_method(method, &trait_args)?;
                let mut cleaned = plan.method.clone();
                cleaned.attrs.retain(|attr| SqlKind::of(attr).is_none());
                cleaned_items.push(TraitItem::Fn(cleaned));
                plans.push(plan);
            }
            other => cleaned_items.push(other),
        }
    }
    input.items = cleaned_items;

    if plans.is_empty() {
        return Err(syn::Error::new_spanned(
            &trait_ident,
            "#[Mapper] trait 至少需要一个带 SQL 注解的方法",
        ));
    }

    let has_cached_query = plans
        .iter()
        .any(|plan| plan.kind.is_query() && plan.cache && !plan.flush_cache);
    let clear_also = str_array_tokens(&trait_args.clear_also);
    let clear_when = str_array_tokens(&trait_args.clear_when);
    let trait_cache_codec = cache_codec_factory_tokens(trait_args.cache_codec.as_ref());

    let impl_methods = plans
        .iter()
        .map(|plan| gen_method(&root, &key_expr, plan))
        .collect::<syn::Result<Vec<_>>>()?;

    let expanded = quote! {
        #[#root::__private::async_trait::async_trait]
        #input

        /// 当前 mapper trait 的默认客户端实现。
        ///
        /// 生成的 client 持有可选 L2 缓存和 codec,业务通常通过 `new()` 创建后直接调用 mapper 方法。
        pub struct #client_ident {
            l2_cache: ::core::option::Option<::std::sync::Arc<dyn #root::MapperL2Cache>>,
            cache_codec: ::core::option::Option<::std::sync::Arc<dyn #root::MapperCacheCodec>>,
        }

        impl #client_ident {
            /// 创建 mapper client。
            ///
            /// 默认接入全局 L2 缓存和 trait 级 codec；特殊场景可用 builder 方法覆盖。
            pub fn new() -> Self {
                Self {
                    l2_cache: #root::default_l2_cache(),
                    cache_codec: #trait_cache_codec,
                }
            }
            ///
            /// # 参数
            /// - `cache`: mapper 方法上的缓存配置。
            pub fn with_l2_cache(
                mut self,
                cache: ::std::sync::Arc<dyn #root::MapperL2Cache>,
            ) -> Self {
                self.l2_cache = ::core::option::Option::Some(cache);
                self
            }
            ///
            /// # 参数
            /// - `codec`: mapper 缓存值使用的序列化 codec 类型。
            pub fn with_cache_codec(
                mut self,
                codec: ::std::sync::Arc<dyn #root::MapperCacheCodec>,
            ) -> Self {
                self.cache_codec = ::core::option::Option::Some(codec);
                self
            }
        }

        #[#root::__private::linkme::distributed_slice(#root::MAPPER_CACHE_META)]
        #[linkme(crate = #root::__private::linkme)]
        static #meta_ident: #root::MapperCacheMeta = #root::MapperCacheMeta {
            key: #key_expr,
            has_cached_query: #has_cached_query,
            clear_also: #clear_also,
            clear_when: #clear_when,
        };

        #[#root::__private::async_trait::async_trait]
        impl #trait_ident for #client_ident {
            #(#impl_methods)*
        }
    };
    Ok(expanded)
}
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn parse_trait_args(attr: TokenStream2) -> syn::Result<TraitArgs> {
    let mut args = TraitArgs::default();
    if attr.is_empty() {
        return Ok(args);
    }
    let parser = syn::meta::parser(|meta| {
        let key = meta
            .path
            .get_ident()
            .map(|ident| ident.to_string())
            .unwrap_or_default();
        match key.as_str() {
            "key" => args.key = Some(meta.value()?.parse::<LitStr>()?.value()),
            "datasource" => {
                let lit = meta.value()?.parse::<LitStr>()?;
                validate_datasource_literal(&lit.value(), lit.span())?;
                args.datasource = Some(lit.value());
            }
            "cache" => args.cache = meta.value()?.parse::<LitBool>()?.value,
            "cache_in_tx" => args.cache_in_tx = meta.value()?.parse::<LitBool>()?.value,
            "cache_ttl_ms" => {
                args.cache_ttl_ms = Some(parse_lit_int(meta.value()?.parse::<LitInt>()?)?)
            }
            "cache_errors" => {
                let lit = meta.value()?.parse::<LitStr>()?;
                args.cache_errors = CacheErrors::parse(&lit.value(), lit.span())?;
            }
            "cache_codec" => args.cache_codec = Some(meta.value()?.parse::<Path>()?),
            "strict_params" => args.strict_params = meta.value()?.parse::<LitBool>()?.value,
            "clear_also" => args.clear_also = parse_string_array(meta.value()?.parse()?)?,
            "clear_when" => args.clear_when = parse_string_array(meta.value()?.parse()?)?,
            "client" => {
                let lit = meta.value()?.parse::<LitStr>()?;
                args.client = Some(format_ident!("{}", lit.value()));
            }
            other => return Err(meta.error(format!("Mapper 不支持的参数 `{other}`"))),
        }
        Ok(())
    });
    Parser::parse2(parser, attr)?;
    Ok(args)
}
///
/// # 参数
/// - `method`: trait 方法 AST 或 HTTP 方法。
/// - `trait_args`: rest client 或 mapper trait 级配置参数。
fn plan_method(method: TraitItemFn, trait_args: &TraitArgs) -> syn::Result<MethodPlan> {
    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "#[Mapper] trait 方法必须是 async fn",
        ));
    }
    if method.default.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "#[Mapper] 首版不支持带默认方法体的 trait 方法",
        ));
    }
    if !method.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "#[Mapper] 首版不支持方法泛型",
        ));
    }

    let mut sql_attrs = method
        .attrs
        .iter()
        .filter_map(|attr| SqlKind::of(attr).map(|kind| (kind, attr)))
        .collect::<Vec<_>>();
    if sql_attrs.is_empty() {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "Mapper 方法必须有且只有一个 SQL 注解",
        ));
    }
    if sql_attrs.len() > 1 {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "Mapper 方法不能同时存在多个 SQL 注解",
        ));
    }
    let (kind, attr) = sql_attrs.remove(0);
    let method_args = parse_method_attr(attr)?;
    if !kind.is_read() && method_args.checked.is_some() {
        return Err(syn::Error::new_spanned(attr, "checked 只能用于 #[Query]"));
    }
    if kind.is_stream() && method_args.checked.unwrap_or(false) {
        return Err(syn::Error::new_spanned(
            attr,
            "StreamQuery 首版不支持 checked = true",
        ));
    }
    if !kind.is_query() && method_args.cache.is_some() {
        return Err(syn::Error::new_spanned(attr, "cache 只能用于 #[Query]"));
    }
    if !kind.is_query() && method_args.cache_in_tx.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "cache_in_tx 只能用于 #[Query]",
        ));
    }
    if !kind.is_query() && method_args.hash_key_suffix.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "hash_key_suffix 只能用于 #[Query]",
        ));
    }
    if !kind.is_query() && method_args.cache_codec.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "cache_codec 只能用于 #[Query]",
        ));
    }
    if !kind.is_query() && method_args.typed_cache_codec.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "typed_cache_codec 只能用于 #[Query]",
        ));
    }
    if method_args.cache_codec.is_some() && method_args.typed_cache_codec.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "cache_codec 与 typed_cache_codec 不能同时使用",
        ));
    }

    let sql = method_args
        .sql
        .clone()
        .ok_or_else(|| syn::Error::new_spanned(attr, "SQL 注解缺少 SQL 字符串"))?;
    let params = parse_params(&method)?;
    let (normalized_sql, sql_fragments, binds, dynamic) =
        parse_sql_template(&sql, &params, attr.span())?;
    let checked = method_args.checked.unwrap_or(false);
    if checked {
        validate_checked_query(&binds, dynamic.as_ref(), attr.span())?;
    }
    if kind.is_stream() {
        validate_stream_query(&binds, dynamic.as_ref(), attr.span())?;
        if method_args.tx.is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "StreamQuery 首版不支持 tx 参数",
            ));
        }
    }
    if let Some(suffix) = &method_args.hash_key_suffix {
        validate_hash_key_suffix(suffix, &params, &binds, attr.span())?;
    }
    let strict_params = method_args
        .strict_params
        .unwrap_or(trait_args.strict_params);
    if strict_params {
        validate_strict_params(
            &params,
            &binds,
            dynamic.as_ref(),
            method_args.hash_key_suffix.as_deref(),
            attr.span(),
        )?;
    }

    let cache = if kind.is_query() {
        method_args.cache.unwrap_or(trait_args.cache)
    } else {
        false
    };
    let cache_in_tx = if kind.is_query() {
        method_args.cache_in_tx.unwrap_or(trait_args.cache_in_tx)
    } else {
        false
    };
    let flush_cache = method_args
        .flush_cache
        .unwrap_or_else(|| kind.default_flush_cache());
    if kind.is_query() && cache && flush_cache {
        return Err(syn::Error::new_spanned(
            attr,
            "Query(cache = true, flush_cache = true) 不支持;实时查询请显式 cache = false, flush_cache = true",
        ));
    }
    if kind.is_query() && !cache && method_args.cache_codec.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "cache_codec 只对 cache = true 的 #[Query] 生效",
        ));
    }
    if kind.is_query() && !cache && method_args.typed_cache_codec.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "typed_cache_codec 只对 cache = true 的 #[Query] 生效",
        ));
    }
    if kind.is_query() && !cache && method_args.cache_in_tx.is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "cache_in_tx 只对 cache = true 的 #[Query] 生效",
        ));
    }

    Ok(MethodPlan {
        method,
        kind,
        normalized_sql,
        sql_fragments,
        binds,
        dynamic,
        params,
        fetch: method_args.fetch,
        tx: method_args.tx.unwrap_or(TxMode::Auto),
        datasource: method_args
            .datasource
            .or_else(|| trait_args.datasource.clone()),
        cache,
        cache_in_tx,
        hash_key_suffix: method_args.hash_key_suffix,
        cache_ttl_ms: method_args.cache_ttl_ms.or(trait_args.cache_ttl_ms),
        cache_errors: method_args.cache_errors.unwrap_or(trait_args.cache_errors),
        cache_codec: method_args.cache_codec,
        typed_cache_codec: method_args.typed_cache_codec,
        flush_cache,
        flush_refs: method_args.flush_refs.unwrap_or(true),
        checked,
    })
}
///
/// # 参数
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `dynamic`: 动态 SQL 节点或动态绑定上下文。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_checked_query(
    binds: &[BindInfo],
    dynamic: Option<&DynamicSqlPlan>,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    if dynamic.is_some() {
        return Err(syn::Error::new(
            span,
            "checked = true 只支持静态 SQL,不支持动态 SQL 标签",
        ));
    }
    if binds.iter().any(|bind| bind.kind == BindKind::List) {
        return Err(syn::Error::new(
            span,
            "checked = true 不支持 IN (#{list}) 列表展开",
        ));
    }
    Ok(())
}
///
/// # 参数
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `dynamic`: 动态 SQL 节点或动态绑定上下文。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_stream_query(
    binds: &[BindInfo],
    dynamic: Option<&DynamicSqlPlan>,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    if dynamic.is_some() {
        return Err(syn::Error::new(
            span,
            "StreamQuery 首版只支持静态 SQL,不支持动态 SQL 标签",
        ));
    }
    if binds.iter().any(|bind| bind.kind == BindKind::List) {
        return Err(syn::Error::new(
            span,
            "StreamQuery 首版不支持 IN (#{list}) 列表展开",
        ));
    }
    Ok(())
}
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
fn parse_method_attr(attr: &Attribute) -> syn::Result<MethodArgs> {
    let mut args = MethodArgs::default();
    match &attr.meta {
        Meta::List(list) => {
            if let Ok(sql) = syn::parse2::<LitStr>(list.tokens.clone()) {
                args.sql = Some(sql.value());
                return Ok(args);
            }
            let parser = syn::meta::parser(|meta| {
                let key = meta
                    .path
                    .get_ident()
                    .map(|ident| ident.to_string())
                    .unwrap_or_default();
                match key.as_str() {
                    "sql" | "value" => args.sql = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "datasource" => {
                        let lit = meta.value()?.parse::<LitStr>()?;
                        validate_datasource_literal(&lit.value(), lit.span())?;
                        args.datasource = Some(lit.value());
                    }
                    "checked" => args.checked = Some(meta.value()?.parse::<LitBool>()?.value),
                    "fetch" => args.fetch = Some(meta.value()?.parse::<LitStr>()?.value()),
                    "tx" => {
                        let lit = meta.value()?.parse::<LitStr>()?;
                        args.tx = Some(TxMode::parse(&lit.value(), lit.span())?);
                    }
                    "cache" => args.cache = Some(meta.value()?.parse::<LitBool>()?.value),
                    "cache_in_tx" => {
                        args.cache_in_tx = Some(meta.value()?.parse::<LitBool>()?.value)
                    }
                    "hash_key_suffix" => {
                        args.hash_key_suffix = Some(meta.value()?.parse::<LitStr>()?.value())
                    }
                    "cache_ttl_ms" => {
                        args.cache_ttl_ms = Some(parse_lit_int(meta.value()?.parse::<LitInt>()?)?)
                    }
                    "cache_errors" => {
                        let lit = meta.value()?.parse::<LitStr>()?;
                        args.cache_errors = Some(CacheErrors::parse(&lit.value(), lit.span())?);
                    }
                    "cache_codec" => args.cache_codec = Some(meta.value()?.parse::<Path>()?),
                    "typed_cache_codec" => {
                        args.typed_cache_codec = Some(meta.value()?.parse::<Path>()?)
                    }
                    "strict_params" => {
                        args.strict_params = Some(meta.value()?.parse::<LitBool>()?.value)
                    }
                    "flush_cache" => {
                        args.flush_cache = Some(meta.value()?.parse::<LitBool>()?.value)
                    }
                    "flush_refs" => args.flush_refs = Some(meta.value()?.parse::<LitBool>()?.value),
                    "result" | "returning" => {
                        let _ = meta.value()?.parse::<LitStr>()?;
                    }
                    other => return Err(meta.error(format!("SQL 注解不支持的参数 `{other}`"))),
                }
                Ok(())
            });
            Parser::parse2(parser, list.tokens.clone())?;
            Ok(args)
        }
        Meta::Path(_) => Err(syn::Error::new_spanned(attr, "SQL 注解必须带 SQL 字符串")),
        Meta::NameValue(nv) => Err(syn::Error::new(
            nv.span(),
            "SQL 注解不支持 name=value 顶层写法,请用 #[Query(\"...\")] 或 #[Query(sql=\"...\")]",
        )),
    }
}
///
/// # 参数
/// - `method`: trait 方法 AST 或 HTTP 方法。
fn parse_params(method: &TraitItemFn) -> syn::Result<Vec<ParamInfo>> {
    let mut inputs = method.sig.inputs.iter();
    match inputs.next() {
        Some(FnArg::Receiver(_)) => {}
        _ => {
            return Err(syn::Error::new_spanned(
                &method.sig.ident,
                "Mapper 方法必须以 &self 开头",
            ))
        }
    }
    let mut params = Vec::new();
    for arg in inputs {
        let FnArg::Typed(pat_ty) = arg else {
            return Err(syn::Error::new_spanned(arg, "Mapper 方法参数形态不支持"));
        };
        let Pat::Ident(pat_ident) = pat_ty.pat.as_ref() else {
            return Err(syn::Error::new_spanned(
                &pat_ty.pat,
                "Mapper 方法参数必须是简单 ident,不支持 mut/_/解构 pattern",
            ));
        };
        if pat_ident.mutability.is_some() || pat_ident.by_ref.is_some() {
            return Err(syn::Error::new_spanned(
                pat_ident,
                "Mapper 方法参数必须是简单 ident,不支持 mut/ref",
            ));
        }
        params.push(ParamInfo {
            ident: pat_ident.ident.clone(),
            ty: (*pat_ty.ty).clone(),
        });
    }
    Ok(params)
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_sql_template(
    sql: &str,
    params: &[ParamInfo],
    span: proc_macro2::Span,
) -> syn::Result<ParsedSqlTemplate> {
    validate_sql_template_safety(sql, span)?;

    if has_dynamic_sql_tag(sql, span)? {
        let dynamic = parse_dynamic_sql_template(sql, params, span)?;
        let mut binds = Vec::new();
        collect_node_binds(&dynamic.nodes, &mut binds);
        let normalized_sql = normalize_sql_whitespace(&render_nodes_for_signature(&dynamic.nodes));
        return Ok((normalized_sql, Vec::new(), binds, Some(dynamic)));
    }

    let (normalized_sql, fragments, binds) = parse_plain_sql_template(sql, params, span)?;
    Ok((normalized_sql, fragments, binds, None))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_sql_template_safety(sql: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if sql.contains("${") {
        return Err(syn::Error::new(
            span,
            "Mapper 禁止 `${...}` 字符串拼接; 请使用 `#{name}` prepared bind 或白名单 SQL 片段",
        ));
    }

    let bytes = sql.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => idx = skip_single_quote(sql, idx, span)?,
            b'"' => idx = skip_double_quote(sql, idx, span)?,
            b'`' => idx = skip_backtick(sql, idx, span)?,
            b'-' if idx + 1 < bytes.len() && bytes[idx + 1] == b'-' => {
                return Err(syn::Error::new(
                    span,
                    "Mapper SQL 模板禁止 `--` 行注释; 行注释会破坏 SQL 归一化和 bind 对齐,请改用 `/* ... */` 块注释或移除注释",
                ));
            }
            b'#' if idx + 1 < bytes.len() && bytes[idx + 1] != b'{' => {
                return Err(syn::Error::new(
                    span,
                    "Mapper SQL 模板禁止 `#` 行注释; 行注释会破坏 SQL 归一化和 bind 对齐,请改用 `/* ... */` 块注释或移除注释",
                ));
            }
            b'/' if idx + 1 < bytes.len() && bytes[idx + 1] == b'*' => {
                idx = skip_block_comment(bytes, idx + 2, span)?;
            }
            b'?' => {
                return Err(syn::Error::new(
                    span,
                    "Mapper 禁止裸 `?` 占位符; 请使用 `#{name}` 让宏生成 prepared bind",
                ));
            }
            b'<' if starts_dangerous_xml_tag_at(sql, idx) => {
                return Err(syn::Error::new(
                    span,
                    "Mapper 不支持会引入字符串拼接/SQL 复用的 XML 标签; 请使用受限动态标签和白名单 SQL 片段",
                ));
            }
            _ => idx += 1,
        }
    }
    Ok(())
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn skip_single_quote(sql: &str, start: usize, span: proc_macro2::Span) -> syn::Result<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start + 1;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    idx += 2;
                } else {
                    return Ok(idx + 1);
                }
            }
            b'\\' => idx += 2,
            _ => idx += 1,
        }
    }
    Err(syn::Error::new(span, "SQL 字符串字面量缺少闭合单引号"))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn skip_double_quote(sql: &str, start: usize, span: proc_macro2::Span) -> syn::Result<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start + 1;
    while idx < bytes.len() {
        match bytes[idx] {
            b'"' => {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                    idx += 2;
                } else {
                    return Ok(idx + 1);
                }
            }
            b'\\' => idx += 2,
            _ => idx += 1,
        }
    }
    Err(syn::Error::new(span, "SQL 字符串字面量缺少闭合双引号"))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn skip_backtick(sql: &str, start: usize, span: proc_macro2::Span) -> syn::Result<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start + 1;
    while idx < bytes.len() {
        if bytes[idx] == b'`' {
            return Ok(idx + 1);
        }
        idx += 1;
    }
    Err(syn::Error::new(span, "SQL 标识符缺少闭合反引号"))
}
///
/// # 参数
/// - `bytes`: 原始字节切片。
/// - `idx`: 扫描下标或集合位置。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn skip_block_comment(bytes: &[u8], mut idx: usize, span: proc_macro2::Span) -> syn::Result<usize> {
    while idx + 1 < bytes.len() {
        if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
            return Ok(idx + 2);
        }
        idx += 1;
    }
    Err(syn::Error::new(span, "SQL 块注释缺少闭合 */"))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
fn starts_dangerous_xml_tag_at(sql: &str, pos: usize) -> bool {
    ["bind", "include", "sql", "selectKey"]
        .into_iter()
        .any(|tag| {
            starts_open_tag_at(sql, pos, tag) || starts_close_tag_at(sql, pos, tag).is_some()
        })
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_plain_sql_template(
    sql: &str,
    params: &[ParamInfo],
    span: proc_macro2::Span,
) -> syn::Result<(String, Vec<String>, Vec<BindInfo>)> {
    let mut prepared = String::new();
    let mut fragments = Vec::new();
    let mut binds = Vec::new();
    let mut rest = sql;
    let mut offset = 0;
    while let Some(start) = rest.find("#{") {
        let abs_start = offset + start;
        fragments.push(rest[..start].to_string());
        prepared.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        let end = after_open
            .find('}')
            .ok_or_else(|| syn::Error::new(span, "SQL 模板存在未闭合的 `#{`"))?;
        let abs_end = abs_start + 2 + end + 1;
        let name = &after_open[..end];
        let placeholder_path = parse_placeholder_path(name, span)?;
        let root_ident = placeholder_path
            .first()
            .expect("placeholder path must have at least root segment");
        let path_tail = placeholder_path[1..].to_vec();
        let ident = params
            .iter()
            .find(|param| param.ident == *root_ident)
            .map(|param| {
                let is_list_context = is_in_list_placeholder(sql, abs_start, abs_end);
                let is_collection = is_collection_type(&param.ty);
                if is_list_context && !path_tail.is_empty() {
                    return Err(syn::Error::new(
                        span,
                        format!(
                            "IN 列表占位符 `#{{{name}}}` 首版只支持顶层集合参数,不支持字段路径"
                        ),
                    ));
                }
                if is_list_context && !is_collection {
                    return Err(syn::Error::new(
                        span,
                        format!(
                            "IN 列表占位符 `#{{{name}}}` 必须使用 Vec<T>、&[T] 或数组等集合参数"
                        ),
                    ));
                }
                if is_collection && !is_list_context {
                    return Err(syn::Error::new(
                        span,
                        format!("集合参数 `{name}` 只能用于 IN (#{{{name}}}) 列表占位符"),
                    ));
                }
                Ok(BindInfo {
                    root: param.ident.clone(),
                    path: path_tail,
                    name: name.to_string(),
                    kind: if is_list_context {
                        BindKind::List
                    } else {
                        BindKind::Scalar
                    },
                })
            })
            .ok_or_else(|| {
                syn::Error::new(span, format!("SQL 引用了不存在的方法参数 `#{{{name}}}`"))
            })??;
        binds.push(ident);
        prepared.push('?');
        rest = &after_open[end + 1..];
        offset = abs_end;
    }
    if rest.contains('}') {
        // 普通 SQL 可能有 } 字符,这里不做额外限制。
    }
    fragments.push(rest.to_string());
    prepared.push_str(rest);
    Ok((normalize_sql_whitespace(&prepared), fragments, binds))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn has_dynamic_sql_tag(sql: &str, span: proc_macro2::Span) -> syn::Result<bool> {
    let bytes = sql.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => idx = skip_single_quote(sql, idx, span)?,
            b'"' => idx = skip_double_quote(sql, idx, span)?,
            b'`' => idx = skip_backtick(sql, idx, span)?,
            b'/' if idx + 1 < bytes.len() && bytes[idx + 1] == b'*' => {
                idx = skip_block_comment(bytes, idx + 2, span)?;
            }
            b'<' if known_open_tag_at(sql, idx).is_some()
                || known_close_tag_at(sql, idx).is_some() =>
            {
                return Ok(true);
            }
            _ => idx += 1,
        }
    }
    Ok(false)
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_dynamic_sql_template(
    sql: &str,
    params: &[ParamInfo],
    span: proc_macro2::Span,
) -> syn::Result<DynamicSqlPlan> {
    let (nodes, pos, close_tag) = parse_dynamic_nodes(sql, 0, &[], params, &[], span)?;
    if let Some(tag) = close_tag {
        return Err(syn::Error::new(
            span,
            format!("SQL 模板存在未匹配的 </{tag}>"),
        ));
    }
    if pos != sql.len() {
        return Err(syn::Error::new(span, "动态 SQL 模板解析未完整消费"));
    }
    Ok(DynamicSqlPlan { nodes })
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `stop_tags`: 动态 SQL 解析时需要停止扫描的标签集合。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_dynamic_nodes(
    sql: &str,
    mut pos: usize,
    stop_tags: &[&str],
    params: &[ParamInfo],
    local_items: &[Ident],
    span: proc_macro2::Span,
) -> syn::Result<(Vec<SqlNode>, usize, Option<String>)> {
    let mut nodes = Vec::new();
    while pos < sql.len() {
        for tag in stop_tags {
            if let Some(close_end) = starts_close_tag_at(sql, pos, tag) {
                return Ok((nodes, close_end, Some((*tag).to_string())));
            }
        }
        if let Some(tag) = known_close_tag_at(sql, pos) {
            return Err(syn::Error::new(
                span,
                format!("SQL 模板存在未匹配的 </{tag}>"),
            ));
        }
        if starts_open_tag_at(sql, pos, "when") {
            return Err(syn::Error::new(span, "<when> 只能写在 <choose> 内"));
        }
        if starts_open_tag_at(sql, pos, "otherwise") {
            return Err(syn::Error::new(span, "<otherwise> 只能写在 <choose> 内"));
        }

        if sql[pos..].starts_with("#{") {
            let (bind, next) = parse_dynamic_bind(sql, pos, params, local_items, span)?;
            nodes.push(SqlNode::Bind(bind));
            pos = next;
            continue;
        }

        if starts_open_tag_at(sql, pos, "if") {
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "if", span)?;
            let test = required_attr(&attrs, "if", "test", span)?;
            let test = parse_test_expr(&test, span)?;
            let (body, next, close_tag) =
                parse_dynamic_nodes(sql, body_start, &["if"], params, local_items, span)?;
            require_close_tag(close_tag, "if", span)?;
            nodes.push(SqlNode::If { test, body });
            pos = next;
            continue;
        }

        if starts_open_tag_at(sql, pos, "choose") {
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "choose", span)?;
            reject_attrs(&attrs, "choose", span)?;
            let (node, next) = parse_choose_node(sql, body_start, params, local_items, span)?;
            nodes.push(node);
            pos = next;
            continue;
        }

        if starts_open_tag_at(sql, pos, "foreach") {
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "foreach", span)?;
            let node = parse_dynamic_foreach(sql, body_start, attrs, params, local_items, span)?;
            pos = node.1;
            nodes.push(node.0);
            continue;
        }

        if starts_open_tag_at(sql, pos, "order_by") {
            let (attrs, next) = parse_xml_self_closing_attrs(sql, pos, "order_by", span)?;
            reject_unknown_attrs(&attrs, "order_by", &["value"], span)?;
            let value = required_attr(&attrs, "order_by", "value", span)?;
            if !is_rust_ident(&value) {
                return Err(syn::Error::new(
                    span,
                    "<order_by> value 必须是方法参数 ident",
                ));
            }
            let value = Ident::new(&value, span);
            if !params.iter().any(|param| param.ident == value) {
                return Err(syn::Error::new(
                    span,
                    format!("order_by value `{value}` 不是方法参数"),
                ));
            }
            nodes.push(SqlNode::OrderBy { value });
            pos = next;
            continue;
        }

        if starts_open_tag_at(sql, pos, "where") {
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "where", span)?;
            reject_attrs(&attrs, "where", span)?;
            let (body, next, close_tag) =
                parse_dynamic_nodes(sql, body_start, &["where"], params, local_items, span)?;
            require_close_tag(close_tag, "where", span)?;
            nodes.push(SqlNode::Trim {
                prefix: "WHERE".to_string(),
                suffix: String::new(),
                prefix_overrides: vec!["AND".to_string(), "OR".to_string()],
                suffix_overrides: Vec::new(),
                body,
            });
            pos = next;
            continue;
        }

        if starts_open_tag_at(sql, pos, "set") {
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "set", span)?;
            reject_attrs(&attrs, "set", span)?;
            let (body, next, close_tag) =
                parse_dynamic_nodes(sql, body_start, &["set"], params, local_items, span)?;
            require_close_tag(close_tag, "set", span)?;
            nodes.push(SqlNode::Trim {
                prefix: "SET".to_string(),
                suffix: String::new(),
                prefix_overrides: vec![",".to_string()],
                suffix_overrides: vec![",".to_string()],
                body,
            });
            pos = next;
            continue;
        }

        if starts_open_tag_at(sql, pos, "trim") {
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "trim", span)?;
            let prefix = optional_attr(&attrs, "prefix").unwrap_or_default();
            let suffix = optional_attr(&attrs, "suffix").unwrap_or_default();
            validate_raw_sql_attr("trim", "prefix", &prefix, span)?;
            validate_raw_sql_attr("trim", "suffix", &suffix, span)?;
            let prefix_overrides = optional_attr(&attrs, "prefixOverrides")
                .map(|value| parse_overrides(&value))
                .unwrap_or_default();
            let suffix_overrides = optional_attr(&attrs, "suffixOverrides")
                .map(|value| parse_overrides(&value))
                .unwrap_or_default();
            reject_unknown_attrs(
                &attrs,
                "trim",
                &["prefix", "suffix", "prefixOverrides", "suffixOverrides"],
                span,
            )?;
            let (body, next, close_tag) =
                parse_dynamic_nodes(sql, body_start, &["trim"], params, local_items, span)?;
            require_close_tag(close_tag, "trim", span)?;
            nodes.push(SqlNode::Trim {
                prefix,
                suffix,
                prefix_overrides,
                suffix_overrides,
                body,
            });
            pos = next;
            continue;
        }

        let next = next_dynamic_special(sql, pos, span)?;
        nodes.push(SqlNode::Text(sql[pos..next].to_string()));
        pos = next;
    }
    Ok((nodes, pos, None))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_choose_node(
    sql: &str,
    mut pos: usize,
    params: &[ParamInfo],
    local_items: &[Ident],
    span: proc_macro2::Span,
) -> syn::Result<(SqlNode, usize)> {
    let mut whens = Vec::new();
    let mut otherwise = Vec::new();
    let mut seen_otherwise = false;
    loop {
        pos = skip_ascii_whitespace(sql, pos);
        if let Some(close_end) = starts_close_tag_at(sql, pos, "choose") {
            if whens.is_empty() {
                return Err(syn::Error::new(span, "<choose> 至少需要一个 <when>"));
            }
            return Ok((SqlNode::Choose { whens, otherwise }, close_end));
        }
        if pos >= sql.len() {
            return Err(syn::Error::new(span, "<choose> 缺少 </choose>"));
        }
        if starts_open_tag_at(sql, pos, "when") {
            if seen_otherwise {
                return Err(syn::Error::new(span, "<otherwise> 之后不能再写 <when>"));
            }
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "when", span)?;
            let test = required_attr(&attrs, "when", "test", span)?;
            let test = parse_test_expr(&test, span)?;
            let (body, next, close_tag) =
                parse_dynamic_nodes(sql, body_start, &["when"], params, local_items, span)?;
            require_close_tag(close_tag, "when", span)?;
            whens.push(WhenNode { test, body });
            pos = next;
            continue;
        }
        if starts_open_tag_at(sql, pos, "otherwise") {
            if seen_otherwise {
                return Err(syn::Error::new(span, "<choose> 只能有一个 <otherwise>"));
            }
            let (attrs, body_start) = parse_xml_open_attrs(sql, pos, "otherwise", span)?;
            reject_attrs(&attrs, "otherwise", span)?;
            let (body, next, close_tag) =
                parse_dynamic_nodes(sql, body_start, &["otherwise"], params, local_items, span)?;
            require_close_tag(close_tag, "otherwise", span)?;
            otherwise = body;
            seen_otherwise = true;
            pos = next;
            continue;
        }
        return Err(syn::Error::new(
            span,
            "<choose> 内只支持 <when> 和 <otherwise>",
        ));
    }
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `body_start`: 动态 SQL 标签体在源码字符串中的起始位置。
/// - `attrs`: 属性列表,用于解析宏配置或 XML 属性。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_dynamic_foreach(
    sql: &str,
    body_start: usize,
    attrs: Vec<(String, String)>,
    params: &[ParamInfo],
    local_items: &[Ident],
    span: proc_macro2::Span,
) -> syn::Result<(SqlNode, usize)> {
    reject_unknown_attrs(
        &attrs,
        "foreach",
        &["collection", "item", "open", "separator", "close"],
        span,
    )?;
    let collection = required_attr(&attrs, "foreach", "collection", span)?;
    let item = required_attr(&attrs, "foreach", "item", span)?;
    let open = optional_attr(&attrs, "open").unwrap_or_default();
    let separator = optional_attr(&attrs, "separator").unwrap_or_default();
    let close = optional_attr(&attrs, "close").unwrap_or_default();
    validate_raw_sql_attr("foreach", "open", &open, span)?;
    validate_raw_sql_attr("foreach", "separator", &separator, span)?;
    validate_raw_sql_attr("foreach", "close", &close, span)?;
    if !is_rust_ident(&collection) {
        return Err(syn::Error::new(
            span,
            "<foreach> collection 必须是方法参数 ident",
        ));
    }
    if !is_rust_ident(&item) {
        return Err(syn::Error::new(span, "<foreach> item 必须是合法 ident"));
    }
    let collection = Ident::new(&collection, span);
    let item = Ident::new(&item, span);
    if params.iter().any(|param| param.ident == item) {
        return Err(syn::Error::new(
            span,
            format!("foreach item `{item}` 不能和方法参数同名"),
        ));
    }
    if local_items.contains(&item) {
        return Err(syn::Error::new(
            span,
            format!("foreach item `{item}` 不能和外层 item 同名"),
        ));
    }
    let collection_param = params
        .iter()
        .find(|param| param.ident == collection)
        .ok_or_else(|| {
            syn::Error::new(
                span,
                format!("foreach collection `{collection}` 不是方法参数"),
            )
        })?;
    if !is_collection_type(&collection_param.ty) {
        return Err(syn::Error::new(
            span,
            format!("foreach collection `{collection}` 必须是 Vec<T>、&[T] 或数组等集合参数"),
        ));
    }
    let mut next_items = local_items.to_vec();
    next_items.push(item.clone());
    let (body, next, close_tag) =
        parse_dynamic_nodes(sql, body_start, &["foreach"], params, &next_items, span)?;
    require_close_tag(close_tag, "foreach", span)?;
    if !nodes_contain_bind_root(&body, &item) {
        return Err(syn::Error::new(
            span,
            "<foreach> body 必须至少引用一次 item",
        ));
    }
    Ok((
        SqlNode::Foreach {
            collection,
            item,
            open,
            separator,
            close,
            body,
        },
        next,
    ))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_dynamic_bind(
    sql: &str,
    start: usize,
    params: &[ParamInfo],
    local_items: &[Ident],
    span: proc_macro2::Span,
) -> syn::Result<(BindInfo, usize)> {
    let after_open = &sql[start + 2..];
    let end = after_open
        .find('}')
        .ok_or_else(|| syn::Error::new(span, "SQL 模板存在未闭合的 `#{`"))?;
    let abs_end = start + 2 + end + 1;
    let name = &after_open[..end];
    let placeholder_path = parse_placeholder_path(name, span)?;
    let root_ident = placeholder_path
        .first()
        .expect("placeholder path must have at least root segment");
    let path_tail = placeholder_path[1..].to_vec();

    if local_items.iter().any(|item| item == root_ident) {
        return Ok((
            BindInfo {
                root: root_ident.clone(),
                path: path_tail,
                name: name.to_string(),
                kind: BindKind::Scalar,
            },
            abs_end,
        ));
    }

    let param = params
        .iter()
        .find(|param| param.ident == *root_ident)
        .ok_or_else(|| {
            syn::Error::new(span, format!("SQL 引用了不存在的方法参数 `#{{{name}}}`"))
        })?;
    let is_list_context = is_in_list_placeholder(sql, start, abs_end);
    let is_collection = is_collection_type(&param.ty);
    if is_list_context && !path_tail.is_empty() {
        return Err(syn::Error::new(
            span,
            format!("IN 列表占位符 `#{{{name}}}` 首版只支持顶层集合参数,不支持字段路径"),
        ));
    }
    if is_list_context && !is_collection {
        return Err(syn::Error::new(
            span,
            format!("IN 列表占位符 `#{{{name}}}` 必须使用 Vec<T>、&[T] 或数组等集合参数"),
        ));
    }
    if is_collection && !is_list_context {
        return Err(syn::Error::new(
            span,
            format!("集合参数 `{name}` 只能用于 IN (#{{{name}}}) 或 <foreach>"),
        ));
    }
    Ok((
        BindInfo {
            root: param.ident.clone(),
            path: path_tail,
            name: name.to_string(),
            kind: if is_list_context {
                BindKind::List
            } else {
                BindKind::Scalar
            },
        },
        abs_end,
    ))
}
///
/// # 参数
/// - `value`: `<if test>` 或 `<when test>` 中声明的条件表达式文本。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_test_expr(value: &str, span: proc_macro2::Span) -> syn::Result<Expr> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(syn::Error::new(span, "动态 SQL test 不能为空"));
    }
    if let Some(path) = normalized.strip_suffix("!= null") {
        let path = path.trim();
        validate_test_path(path, span)?;
        return syn::parse_str::<Expr>(&format!("{path}.is_some()"))
            .map_err(|err| syn::Error::new(span, format!("test 表达式不合法: {err}")));
    }
    if let Some(path) = normalized.strip_suffix("== null") {
        let path = path.trim();
        validate_test_path(path, span)?;
        return syn::parse_str::<Expr>(&format!("{path}.is_none()"))
            .map_err(|err| syn::Error::new(span, format!("test 表达式不合法: {err}")));
    }
    if let Some(path) = normalized.strip_prefix("null !=") {
        let path = path.trim();
        validate_test_path(path, span)?;
        return syn::parse_str::<Expr>(&format!("{path}.is_some()"))
            .map_err(|err| syn::Error::new(span, format!("test 表达式不合法: {err}")));
    }
    if let Some(path) = normalized.strip_prefix("null ==") {
        let path = path.trim();
        validate_test_path(path, span)?;
        return syn::parse_str::<Expr>(&format!("{path}.is_none()"))
            .map_err(|err| syn::Error::new(span, format!("test 表达式不合法: {err}")));
    }
    let rust_expr = rewrite_test_logical_ops(normalized);
    syn::parse_str::<Expr>(&rust_expr)
        .map_err(|err| syn::Error::new(span, format!("test 表达式不合法: {err}")))
}
///
/// # 参数
/// - `value`: `<if test>` 中的条件表达式文本。
fn rewrite_test_logical_ops(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut idx = 0;

    while idx < bytes.len() {
        if let Some(end) = rust_raw_string_end(value, idx) {
            out.push_str(&value[idx..end]);
            idx = end;
            continue;
        }

        match bytes[idx] {
            b'"' => {
                let end = skip_rust_quoted_literal(value, idx, b'"');
                out.push_str(&value[idx..end]);
                idx = end;
            }
            b'\'' => {
                let end = skip_rust_quoted_literal(value, idx, b'\'');
                out.push_str(&value[idx..end]);
                idx = end;
            }
            _ if is_test_logical_word_at(value, idx, "and") => {
                out.push_str("&&");
                idx += "and".len();
            }
            _ if is_test_logical_word_at(value, idx, "or") => {
                out.push_str("||");
                idx += "or".len();
            }
            _ => {
                let ch = value[idx..]
                    .chars()
                    .next()
                    .expect("idx must be inside value");
                out.push(ch);
                idx += ch.len_utf8();
            }
        }
    }

    out
}
///
/// # 参数
/// - `value`: 需要检查当前位置是否为 Rust raw string 的表达式文本。
/// - `start`: 起始位置或范围下界。
fn rust_raw_string_end(value: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    if start >= bytes.len() {
        return None;
    }

    let mut idx = start;
    if bytes[idx] == b'b' {
        idx += 1;
    }
    if idx >= bytes.len() || bytes[idx] != b'r' {
        return None;
    }
    idx += 1;

    let hashes_start = idx;
    while idx < bytes.len() && bytes[idx] == b'#' {
        idx += 1;
    }
    if idx >= bytes.len() || bytes[idx] != b'"' {
        return None;
    }

    let hashes = &value[hashes_start..idx];
    idx += 1;
    while idx < bytes.len() {
        if bytes[idx] == b'"' {
            let close_end = idx + 1 + hashes.len();
            if close_end <= bytes.len() && &value[idx + 1..close_end] == hashes {
                return Some(close_end);
            }
        }
        idx += 1;
    }
    Some(bytes.len())
}
///
/// # 参数
/// - `value`: 需要跳过字符串字面量的表达式文本。
/// - `start`: 起始位置或范围下界。
/// - `quote`: 当前字符串字面量使用的引号字符。
fn skip_rust_quoted_literal(value: &str, start: usize, quote: u8) -> usize {
    let bytes = value.as_bytes();
    let mut idx = start + 1;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\\' => idx = (idx + 2).min(bytes.len()),
            ch if ch == quote => return idx + 1,
            _ => idx += 1,
        }
    }
    bytes.len()
}
///
/// # 参数
/// - `value`: `<if test>` 中待扫描的条件表达式文本。
/// - `start`: 起始位置或范围下界。
/// - `word`: 动态 SQL 解析过程中读取到的标识符。
fn is_test_logical_word_at(value: &str, start: usize, word: &str) -> bool {
    let end = start + word.len();
    if end > value.len() || &value[start..end] != word {
        return false;
    }

    let bytes = value.as_bytes();
    let prev_ok = start == 0 || matches!(bytes[start - 1], b' ' | b'\t' | b'\n' | b'\r' | b'(');
    let next_ok =
        end == bytes.len() || matches!(bytes[end], b' ' | b'\t' | b'\n' | b'\r' | b'(' | b')');
    prev_ok && next_ok
}
///
/// # 参数
/// - `path`: `<if test="...">` 中声明的字段访问路径。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_test_path(path: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if path.split('.').all(is_rust_ident) {
        Ok(())
    } else {
        Err(syn::Error::new(
            span,
            format!("test null 判断只支持字段路径: `{path}`"),
        ))
    }
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_xml_open_attrs(
    sql: &str,
    pos: usize,
    tag: &str,
    span: proc_macro2::Span,
) -> syn::Result<(Vec<(String, String)>, usize)> {
    let tag_end = find_xml_tag_end(sql, pos, tag, span)?;
    let attrs = &sql[pos + tag.len() + 1..tag_end];
    Ok((parse_xml_attrs(attrs, tag, span)?, tag_end + 1))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_xml_self_closing_attrs(
    sql: &str,
    pos: usize,
    tag: &str,
    span: proc_macro2::Span,
) -> syn::Result<(Vec<(String, String)>, usize)> {
    let tag_end = find_xml_tag_end(sql, pos, tag, span)?;
    let attrs = sql[pos + tag.len() + 1..tag_end].trim_end();
    let attrs = attrs.strip_suffix('/').ok_or_else(|| {
        syn::Error::new(span, format!("<{tag}> 必须使用自闭合写法: <{tag} .../>"))
    })?;
    Ok((parse_xml_attrs(attrs, tag, span)?, tag_end + 1))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn find_xml_tag_end(
    sql: &str,
    start: usize,
    tag: &str,
    span: proc_macro2::Span,
) -> syn::Result<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => idx = skip_single_quote(sql, idx, span)?,
            b'"' => idx = skip_double_quote(sql, idx, span)?,
            b'`' => idx = skip_backtick(sql, idx, span)?,
            b'>' => return Ok(idx),
            _ => idx += 1,
        }
    }
    Err(syn::Error::new(span, format!("<{tag}> 缺少闭合的 `>`")))
}
///
/// # 参数
/// - `input`: 宏或解析器收到的原始输入。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_xml_attrs(
    input: &str,
    tag: &str,
    span: proc_macro2::Span,
) -> syn::Result<Vec<(String, String)>> {
    let mut attrs = Vec::new();
    let bytes = input.as_bytes();
    let mut idx = 0;

    while idx < bytes.len() {
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() {
            break;
        }
        let key_start = idx;
        while idx < bytes.len()
            && (bytes[idx].is_ascii_alphanumeric() || bytes[idx] == b'_' || bytes[idx] == b'-')
        {
            idx += 1;
        }
        if key_start == idx {
            return Err(syn::Error::new(span, format!("<{tag}> 属性名不合法")));
        }
        let key = input[key_start..idx].to_string();
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() || bytes[idx] != b'=' {
            return Err(syn::Error::new(
                span,
                format!("<{tag}> 属性必须写成 key=\"value\""),
            ));
        }
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() || (bytes[idx] != b'"' && bytes[idx] != b'\'') {
            return Err(syn::Error::new(span, format!("<{tag}> 属性值必须使用引号")));
        }
        let quote = bytes[idx];
        idx += 1;
        let value_start = idx;
        while idx < bytes.len() && bytes[idx] != quote {
            idx += 1;
        }
        if idx >= bytes.len() {
            return Err(syn::Error::new(span, format!("<{tag}> 属性值缺少闭合引号")));
        }
        attrs.push((key, input[value_start..idx].to_string()));
        idx += 1;
    }
    Ok(attrs)
}
///
/// # 参数
/// - `attrs`: 属性列表,用于解析宏配置或 XML 属性。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn required_attr(
    attrs: &[(String, String)],
    tag: &str,
    name: &str,
    span: proc_macro2::Span,
) -> syn::Result<String> {
    optional_attr(attrs, name)
        .ok_or_else(|| syn::Error::new(span, format!("<{tag}> 缺少 {name} 属性")))
}
///
/// # 参数
/// - `attrs`: 属性列表,用于解析宏配置或 XML 属性。
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn optional_attr(attrs: &[(String, String)], name: &str) -> Option<String> {
    attrs
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}
///
/// # 参数
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `attr`: 属性宏括号内的 token stream。
/// - `value`: `<trim>`、`<foreach>` 或 `<order_by>` 属性中的静态 SQL 片段。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_raw_sql_attr(
    tag: &str,
    attr: &str,
    value: &str,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    if value.contains("${") || value.contains("#{") {
        return Err(syn::Error::new(
            span,
            format!(
                "<{tag}> {attr} 属性是编译期 SQL 片段,禁止写占位符; 值参数必须放在标签 body 的 `#{{...}}` 中"
            ),
        ));
    }
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'?' => {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "<{tag}> {attr} 属性禁止裸 `?`; 值参数必须使用标签 body 中的 `#{{...}}`"
                    ),
                ));
            }
            b'#' => {
                return Err(syn::Error::new(
                    span,
                    format!("<{tag}> {attr} 属性禁止 `#` 行注释或占位符"),
                ));
            }
            b'-' if idx + 1 < bytes.len() && bytes[idx + 1] == b'-' => {
                return Err(syn::Error::new(
                    span,
                    format!("<{tag}> {attr} 属性禁止 `--` 行注释"),
                ));
            }
            b'/' if idx + 1 < bytes.len() && bytes[idx + 1] == b'*' => {
                return Err(syn::Error::new(
                    span,
                    format!("<{tag}> {attr} 属性禁止 SQL 块注释"),
                ));
            }
            b'<' => {
                return Err(syn::Error::new(
                    span,
                    format!("<{tag}> {attr} 属性禁止 XML/SQL 标签片段"),
                ));
            }
            _ => idx += 1,
        }
    }
    Ok(())
}
///
/// # 参数
/// - `attrs`: 属性列表,用于解析宏配置或 XML 属性。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn reject_attrs(attrs: &[(String, String)], tag: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if let Some((key, _)) = attrs.first() {
        return Err(syn::Error::new(span, format!("<{tag}> 不支持属性 `{key}`")));
    }
    Ok(())
}
///
/// # 参数
/// - `attrs`: 属性列表,用于解析宏配置或 XML 属性。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
/// - `allowed`: 允许出现在当前语法位置的标签或属性集合。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn reject_unknown_attrs(
    attrs: &[(String, String)],
    tag: &str,
    allowed: &[&str],
    span: proc_macro2::Span,
) -> syn::Result<()> {
    for (key, _) in attrs {
        if !allowed.iter().any(|allowed| allowed == key) {
            return Err(syn::Error::new(span, format!("<{tag}> 不支持属性 `{key}`")));
        }
    }
    Ok(())
}
///
/// # 参数
/// - `value`: `prefix_overrides` 或 `suffix_overrides` 属性中的管道分隔 token 列表。
fn parse_overrides(value: &str) -> Vec<String> {
    value
        .split('|')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
///
/// # 参数
/// - `close_tag`: 当前动态 SQL 节点期望匹配的闭合标签。
/// - `expected`: 协议或状态机期望值。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn require_close_tag(
    close_tag: Option<String>,
    expected: &str,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    match close_tag.as_deref() {
        Some(tag) if tag == expected => Ok(()),
        _ => Err(syn::Error::new(
            span,
            format!("<{expected}> 缺少 </{expected}>"),
        )),
    }
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn next_dynamic_special(sql: &str, pos: usize, span: proc_macro2::Span) -> syn::Result<usize> {
    let bytes = sql.as_bytes();
    let mut idx = pos;
    while idx < sql.len() {
        match bytes[idx] {
            b'\'' => idx = skip_single_quote(sql, idx, span)?,
            b'"' => idx = skip_double_quote(sql, idx, span)?,
            b'`' => idx = skip_backtick(sql, idx, span)?,
            b'/' if idx + 1 < bytes.len() && bytes[idx + 1] == b'*' => {
                idx = skip_block_comment(bytes, idx + 2, span)?;
            }
            b'#' if idx + 1 < bytes.len() && bytes[idx + 1] == b'{' => {
                return Ok(idx);
            }
            b'<' if known_open_tag_at(sql, idx).is_some()
                || known_close_tag_at(sql, idx).is_some() =>
            {
                return Ok(idx);
            }
            _ => idx += 1,
        }
    }
    Ok(sql.len())
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
fn known_open_tag_at(sql: &str, pos: usize) -> Option<&'static str> {
    [
        "if",
        "choose",
        "when",
        "otherwise",
        "foreach",
        "where",
        "set",
        "trim",
        "order_by",
    ]
    .into_iter()
    .find(|tag| starts_open_tag_at(sql, pos, tag))
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
fn known_close_tag_at(sql: &str, pos: usize) -> Option<&'static str> {
    [
        "if",
        "choose",
        "when",
        "otherwise",
        "foreach",
        "where",
        "set",
        "trim",
        "order_by",
    ]
    .into_iter()
    .find(|tag| starts_close_tag_at(sql, pos, tag).is_some())
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
fn starts_open_tag_at(sql: &str, pos: usize, tag: &str) -> bool {
    if !sql.is_char_boundary(pos) {
        return false;
    }
    let rest = &sql[pos..];
    let prefix = format!("<{tag}");
    if !rest.starts_with(&prefix) {
        return false;
    }
    rest[prefix.len()..]
        .chars()
        .next()
        .is_some_and(|ch| ch == '>' || ch == '/' || ch.is_ascii_whitespace())
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
/// - `tag`: 协议字段 tag 或 Redis Search 标签名。
fn starts_close_tag_at(sql: &str, pos: usize, tag: &str) -> Option<usize> {
    if !sql.is_char_boundary(pos) {
        return None;
    }
    let close = format!("</{tag}>");
    sql[pos..].starts_with(&close).then_some(pos + close.len())
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `pos`: 字符串扫描位置。
fn skip_ascii_whitespace(sql: &str, mut pos: usize) -> usize {
    while pos < sql.len() && sql.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}
///
/// # 参数
/// - `nodes`: 动态 SQL 节点列表。
/// - `out`: 输出缓冲区,用于收集解析结果。
fn collect_node_binds(nodes: &[SqlNode], out: &mut Vec<BindInfo>) {
    for node in nodes {
        match node {
            SqlNode::Text(_) => {}
            SqlNode::Bind(bind) => out.push(bind.clone()),
            SqlNode::If { body, .. } => collect_node_binds(body, out),
            SqlNode::Choose { whens, otherwise } => {
                for when in whens {
                    collect_node_binds(&when.body, out);
                }
                collect_node_binds(otherwise, out);
            }
            SqlNode::Foreach { body, .. } | SqlNode::Trim { body, .. } => {
                collect_node_binds(body, out);
            }
            SqlNode::OrderBy { .. } => {}
        }
    }
}
///
/// # 参数
/// - `nodes`: 动态 SQL 节点列表。
/// - `ident`: Rust 标识符。
fn nodes_contain_bind_root(nodes: &[SqlNode], ident: &Ident) -> bool {
    nodes.iter().any(|node| match node {
        SqlNode::Text(_) => false,
        SqlNode::Bind(bind) => bind.root == *ident,
        SqlNode::If { body, .. } => nodes_contain_bind_root(body, ident),
        SqlNode::Choose { whens, otherwise } => {
            whens
                .iter()
                .any(|when| nodes_contain_bind_root(&when.body, ident))
                || nodes_contain_bind_root(otherwise, ident)
        }
        SqlNode::Foreach { body, .. } | SqlNode::Trim { body, .. } => {
            nodes_contain_bind_root(body, ident)
        }
        SqlNode::OrderBy { .. } => false,
    })
}
///
/// # 参数
/// - `nodes`: 动态 SQL 节点列表。
fn render_nodes_for_signature(nodes: &[SqlNode]) -> String {
    let mut sql = String::new();
    for node in nodes {
        match node {
            SqlNode::Text(text) => sql.push_str(text),
            SqlNode::Bind(bind) => match bind.kind {
                BindKind::Scalar => sql.push('?'),
                BindKind::List => sql.push('?'),
            },
            SqlNode::If { body, .. } => sql.push_str(&render_nodes_for_signature(body)),
            SqlNode::Choose { whens, otherwise } => {
                for when in whens {
                    sql.push_str(&render_nodes_for_signature(&when.body));
                }
                sql.push_str(&render_nodes_for_signature(otherwise));
            }
            SqlNode::Foreach {
                open, close, body, ..
            } => {
                sql.push_str(open);
                sql.push_str(&render_nodes_for_signature(body));
                sql.push_str(close);
            }
            SqlNode::Trim {
                prefix,
                suffix,
                body,
                ..
            } => {
                sql.push_str(prefix);
                sql.push_str(&render_nodes_for_signature(body));
                sql.push_str(suffix);
            }
            SqlNode::OrderBy { .. } => sql.push_str(" ORDER BY <order_by>"),
        }
    }
    sql
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
fn normalize_sql_whitespace(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut pending_space = false;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_block_comment {
            out.push(ch);
            if ch == '*' && chars.peek() == Some(&'/') {
                out.push(chars.next().expect("peeked char must exist"));
                in_block_comment = false;
            }
            continue;
        }

        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    out.push(chars.next().expect("peeked char must exist"));
                } else {
                    in_single_quote = false;
                }
            } else if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            continue;
        }

        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    out.push(chars.next().expect("peeked char must exist"));
                } else {
                    in_double_quote = false;
                }
            } else if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            continue;
        }

        if in_backtick {
            out.push(ch);
            if ch == '`' {
                in_backtick = false;
            }
            continue;
        }

        // 块注释 `/* ... */` 整体原样保留：注释里的撇号/引号/反引号不得翻转字符串状态机，
        // 否则会破坏注释之后真实字符串字面量内部的空白/逗号规范化（改变最终 SQL 语义）。
        if ch == '/' && chars.peek() == Some(&'*') {
            if pending_space && !out.is_empty() && !out.ends_with(',') && !out.ends_with('(') {
                out.push(' ');
            }
            pending_space = false;
            out.push('/');
            out.push(chars.next().expect("peeked char must exist"));
            in_block_comment = true;
            continue;
        }

        if ch.is_ascii_whitespace() {
            pending_space = true;
            continue;
        }

        if ch == ',' {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(',');
            pending_space = false;
            continue;
        }

        if pending_space
            && !out.is_empty()
            && !out.ends_with(',')
            && !out.ends_with('(')
            && ch != ')'
        {
            out.push(' ');
        }
        pending_space = false;

        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
            }
            '`' => {
                in_backtick = true;
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }

    out
}
///
/// # 参数
/// - `suffix`: key 后缀或文件后缀。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_hash_key_suffix_placeholders(
    suffix: &str,
    span: proc_macro2::Span,
) -> syn::Result<Vec<(String, Vec<Ident>)>> {
    let mut placeholders = Vec::new();
    let mut rest = suffix;
    while let Some(start) = rest.find('{') {
        let after_open = &rest[start + 1..];
        let end = after_open
            .find('}')
            .ok_or_else(|| syn::Error::new(span, "hash_key_suffix 模板存在未闭合的 `{`"))?;
        let name = &after_open[..end];
        let path = parse_placeholder_path(name, span).map_err(|_| {
            syn::Error::new(
                span,
                format!("hash_key_suffix 占位符 `{{{name}}}` 不是合法字段路径"),
            )
        })?;
        placeholders.push((name.to_string(), path));
        rest = &after_open[end + 1..];
    }
    if rest.contains('}') {
        return Err(syn::Error::new(
            span,
            "hash_key_suffix 模板存在未匹配的 `}`",
        ));
    }
    Ok(placeholders)
}
///
/// # 参数
/// - `suffix`: key 后缀或文件后缀。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_hash_key_suffix(
    suffix: &str,
    params: &[ParamInfo],
    binds: &[BindInfo],
    span: proc_macro2::Span,
) -> syn::Result<()> {
    for (name, path) in parse_hash_key_suffix_placeholders(suffix, span)? {
        let root = path
            .first()
            .expect("hash_key_suffix path must have root segment");
        let Some(param) = params.iter().find(|param| param.ident == *root) else {
            return Err(syn::Error::new(
                span,
                format!("hash_key_suffix 引用了不存在的方法参数 `{{{root}}}`"),
            ));
        };
        if is_collection_type(&param.ty) {
            return Err(syn::Error::new(
                span,
                format!("hash_key_suffix 不支持集合参数 `{{{name}}}`"),
            ));
        }
        match binds.iter().find(|bind| bind.name == name) {
            Some(bind) if bind.kind == BindKind::Scalar => {}
            Some(_) => {
                return Err(syn::Error::new(
                    span,
                    format!("hash_key_suffix 不支持集合参数 `{{{name}}}`"),
                ));
            }
            None => {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "hash_key_suffix 占位符 `{{{name}}}` 必须同时出现在 SQL 的 `#{{{name}}}` 标量绑定中"
                    ),
                ));
            }
        }
    }
    Ok(())
}
///
/// # 参数
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `dynamic`: 动态 SQL 节点或动态绑定上下文。
/// - `hash_key_suffix`: 业务 key 或 Redis key,用于定位数据。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_strict_params(
    params: &[ParamInfo],
    binds: &[BindInfo],
    dynamic: Option<&DynamicSqlPlan>,
    hash_key_suffix: Option<&str>,
    span: proc_macro2::Span,
) -> syn::Result<()> {
    let mut used = ::std::collections::HashSet::<String>::new();
    for bind in binds {
        mark_used_param(&bind.root, params, &mut used);
    }
    if let Some(dynamic) = dynamic {
        collect_dynamic_used_params(&dynamic.nodes, params, &mut used);
    }
    if let Some(suffix) = hash_key_suffix {
        for (_, path) in parse_hash_key_suffix_placeholders(suffix, span)? {
            let root = path
                .first()
                .expect("hash_key_suffix path must have root segment");
            mark_used_param(root, params, &mut used);
        }
    }

    let unused = params
        .iter()
        .map(|param| param.ident.to_string())
        .filter(|name| !used.contains(name))
        .collect::<Vec<_>>();
    if unused.is_empty() {
        Ok(())
    } else {
        Err(syn::Error::new(
            span,
            format!(
                "strict_params = true 发现未使用方法参数: {}",
                unused.join(", ")
            ),
        ))
    }
}
///
/// # 参数
/// - `ident`: Rust 标识符。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
fn mark_used_param(
    ident: &Ident,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
) {
    if params.iter().any(|param| param.ident == *ident) {
        used.insert(ident.to_string());
    }
}
///
/// # 参数
/// - `nodes`: 动态 SQL 节点列表。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
fn collect_dynamic_used_params(
    nodes: &[SqlNode],
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
) {
    for node in nodes {
        match node {
            SqlNode::Text(_) => {}
            SqlNode::Bind(bind) => mark_used_param(&bind.root, params, used),
            SqlNode::If { test, body } => {
                collect_expr_used_params(test, params, used);
                collect_dynamic_used_params(body, params, used);
            }
            SqlNode::Choose { whens, otherwise } => {
                for when in whens {
                    collect_expr_used_params(&when.test, params, used);
                    collect_dynamic_used_params(&when.body, params, used);
                }
                collect_dynamic_used_params(otherwise, params, used);
            }
            SqlNode::Foreach {
                collection, body, ..
            } => {
                mark_used_param(collection, params, used);
                collect_dynamic_used_params(body, params, used);
            }
            SqlNode::Trim { body, .. } => collect_dynamic_used_params(body, params, used),
            SqlNode::OrderBy { value } => mark_used_param(value, params, used),
        }
    }
}
///
/// # 参数
/// - `expr`: Rust 表达式 AST,用于宏期分析。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
fn collect_expr_used_params(
    expr: &Expr,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
) {
    let mut shadowed = ::std::collections::HashSet::new();
    collect_expr_used_params_inner(expr, params, used, &mut shadowed);
}
///
/// # 参数
/// - `expr`: Rust 表达式 AST,用于宏期分析。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
/// - `shadowed`: 当前作用域内被局部变量遮蔽的参数名集合。
fn collect_expr_used_params_inner(
    expr: &Expr,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
    shadowed: &mut ::std::collections::HashSet<String>,
) {
    match expr {
        Expr::Path(expr_path) if expr_path.qself.is_none() => {
            if let Some(segment) = expr_path.path.segments.first() {
                mark_used_param_unshadowed(&segment.ident, params, used, shadowed);
            }
        }
        Expr::Path(_) => {}
        Expr::Field(expr_field) => {
            collect_expr_used_params_inner(&expr_field.base, params, used, shadowed);
        }
        Expr::MethodCall(expr_method) => {
            collect_expr_used_params_inner(&expr_method.receiver, params, used, shadowed);
            for arg in &expr_method.args {
                collect_expr_used_params_inner(arg, params, used, shadowed);
            }
        }
        Expr::Binary(expr_binary) => {
            collect_expr_used_params_inner(&expr_binary.left, params, used, shadowed);
            collect_expr_used_params_inner(&expr_binary.right, params, used, shadowed);
        }
        Expr::Assign(expr_assign) => {
            collect_expr_used_params_inner(&expr_assign.left, params, used, shadowed);
            collect_expr_used_params_inner(&expr_assign.right, params, used, shadowed);
        }
        Expr::Range(expr_range) => {
            if let Some(start) = &expr_range.start {
                collect_expr_used_params_inner(start, params, used, shadowed);
            }
            if let Some(end) = &expr_range.end {
                collect_expr_used_params_inner(end, params, used, shadowed);
            }
        }
        Expr::Unary(expr_unary) => {
            collect_expr_used_params_inner(&expr_unary.expr, params, used, shadowed);
        }
        Expr::Paren(expr_paren) => {
            collect_expr_used_params_inner(&expr_paren.expr, params, used, shadowed);
        }
        Expr::Group(expr_group) => {
            collect_expr_used_params_inner(&expr_group.expr, params, used, shadowed);
        }
        Expr::Reference(expr_ref) => {
            collect_expr_used_params_inner(&expr_ref.expr, params, used, shadowed);
        }
        Expr::Call(expr_call) => {
            collect_expr_used_params_inner(&expr_call.func, params, used, shadowed);
            for arg in &expr_call.args {
                collect_expr_used_params_inner(arg, params, used, shadowed);
            }
        }
        Expr::Index(expr_index) => {
            collect_expr_used_params_inner(&expr_index.expr, params, used, shadowed);
            collect_expr_used_params_inner(&expr_index.index, params, used, shadowed);
        }
        Expr::Array(expr_array) => {
            for elem in &expr_array.elems {
                collect_expr_used_params_inner(elem, params, used, shadowed);
            }
        }
        Expr::Tuple(expr_tuple) => {
            for elem in &expr_tuple.elems {
                collect_expr_used_params_inner(elem, params, used, shadowed);
            }
        }
        Expr::If(expr_if) => {
            collect_expr_used_params_inner(&expr_if.cond, params, used, shadowed);
            collect_block_used_params(&expr_if.then_branch, params, used, shadowed);
            if let Some((_, else_branch)) = &expr_if.else_branch {
                collect_expr_used_params_inner(else_branch, params, used, shadowed);
            }
        }
        Expr::Match(expr_match) => {
            collect_expr_used_params_inner(&expr_match.expr, params, used, shadowed);
            for arm in &expr_match.arms {
                let mut arm_shadowed = shadowed.clone();
                collect_pat_shadowed_params(&arm.pat, &mut arm_shadowed);
                if let Some((_, guard)) = &arm.guard {
                    collect_expr_used_params_inner(guard, params, used, &mut arm_shadowed);
                }
                collect_expr_used_params_inner(&arm.body, params, used, &mut arm_shadowed);
            }
        }
        Expr::Closure(expr_closure) => {
            let mut closure_shadowed = shadowed.clone();
            for input in &expr_closure.inputs {
                collect_pat_shadowed_params(input, &mut closure_shadowed);
            }
            collect_expr_used_params_inner(&expr_closure.body, params, used, &mut closure_shadowed);
        }
        Expr::Let(expr_let) => {
            collect_expr_used_params_inner(&expr_let.expr, params, used, shadowed);
        }
        Expr::Block(expr_block) => {
            collect_block_used_params(&expr_block.block, params, used, shadowed);
        }
        Expr::Async(expr_async) => {
            collect_block_used_params(&expr_async.block, params, used, shadowed);
        }
        Expr::Const(expr_const) => {
            collect_block_used_params(&expr_const.block, params, used, shadowed);
        }
        Expr::Unsafe(expr_unsafe) => {
            collect_block_used_params(&expr_unsafe.block, params, used, shadowed);
        }
        Expr::Loop(expr_loop) => {
            collect_block_used_params(&expr_loop.body, params, used, shadowed);
        }
        Expr::While(expr_while) => {
            collect_expr_used_params_inner(&expr_while.cond, params, used, shadowed);
            collect_block_used_params(&expr_while.body, params, used, shadowed);
        }
        Expr::ForLoop(expr_for_loop) => {
            collect_expr_used_params_inner(&expr_for_loop.expr, params, used, shadowed);
            let mut loop_shadowed = shadowed.clone();
            collect_pat_shadowed_params(&expr_for_loop.pat, &mut loop_shadowed);
            collect_block_used_params(&expr_for_loop.body, params, used, &mut loop_shadowed);
        }
        Expr::Repeat(expr_repeat) => {
            collect_expr_used_params_inner(&expr_repeat.expr, params, used, shadowed);
            collect_expr_used_params_inner(&expr_repeat.len, params, used, shadowed);
        }
        Expr::Struct(expr_struct) => {
            for field in &expr_struct.fields {
                collect_expr_used_params_inner(&field.expr, params, used, shadowed);
            }
            if let Some(rest) = &expr_struct.rest {
                collect_expr_used_params_inner(rest, params, used, shadowed);
            }
        }
        Expr::Cast(expr_cast) => {
            collect_expr_used_params_inner(&expr_cast.expr, params, used, shadowed);
        }
        Expr::Await(expr_await) => {
            collect_expr_used_params_inner(&expr_await.base, params, used, shadowed);
        }
        Expr::Try(expr_try) => {
            collect_expr_used_params_inner(&expr_try.expr, params, used, shadowed);
        }
        Expr::TryBlock(expr_try_block) => {
            collect_block_used_params(&expr_try_block.block, params, used, shadowed);
        }
        Expr::Return(expr_return) => {
            if let Some(expr) = &expr_return.expr {
                collect_expr_used_params_inner(expr, params, used, shadowed);
            }
        }
        Expr::Break(expr_break) => {
            if let Some(expr) = &expr_break.expr {
                collect_expr_used_params_inner(expr, params, used, shadowed);
            }
        }
        Expr::Yield(expr_yield) => {
            if let Some(expr) = &expr_yield.expr {
                collect_expr_used_params_inner(expr, params, used, shadowed);
            }
        }
        Expr::Macro(expr_macro) => {
            collect_tokens_used_params(&expr_macro.mac.tokens, params, used, shadowed);
        }
        _ => {}
    }
}
///
/// # 参数
/// - `ident`: Rust 标识符。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
/// - `shadowed`: 当前作用域内被局部变量遮蔽的参数名集合。
fn mark_used_param_unshadowed(
    ident: &Ident,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
    shadowed: &::std::collections::HashSet<String>,
) {
    if !shadowed.contains(&ident.to_string()) {
        mark_used_param(ident, params, used);
    }
}
///
/// # 参数
/// - `block`: mapper 方法体中的 Rust block AST。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
/// - `shadowed`: 当前作用域内被局部变量遮蔽的参数名集合。
fn collect_block_used_params(
    block: &syn::Block,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
    shadowed: &mut ::std::collections::HashSet<String>,
) {
    let mut block_shadowed = shadowed.clone();
    for stmt in &block.stmts {
        match stmt {
            Stmt::Local(local) => {
                collect_local_used_params(local, params, used, &mut block_shadowed)
            }
            Stmt::Item(_) => {}
            Stmt::Expr(expr, _) => {
                collect_expr_used_params_inner(expr, params, used, &mut block_shadowed);
            }
            Stmt::Macro(stmt_macro) => {
                collect_tokens_used_params(&stmt_macro.mac.tokens, params, used, &block_shadowed);
            }
        }
    }
}
///
/// # 参数
/// - `local`: 本地变量或模式绑定是否来自当前作用域。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
/// - `shadowed`: 当前作用域内被局部变量遮蔽的参数名集合。
fn collect_local_used_params(
    local: &Local,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
    shadowed: &mut ::std::collections::HashSet<String>,
) {
    if let Some(init) = &local.init {
        collect_expr_used_params_inner(&init.expr, params, used, shadowed);
        if let Some((_, diverge)) = &init.diverge {
            collect_expr_used_params_inner(diverge, params, used, shadowed);
        }
    }
    collect_pat_shadowed_params(&local.pat, shadowed);
}
///
/// # 参数
/// - `pat`: 过程宏正在分析的模式节点。
/// - `shadowed`: 当前作用域内被局部变量遮蔽的参数名集合。
fn collect_pat_shadowed_params(pat: &Pat, shadowed: &mut ::std::collections::HashSet<String>) {
    match pat {
        Pat::Ident(PatIdent { ident, subpat, .. }) => {
            shadowed.insert(ident.to_string());
            if let Some((_, subpat)) = subpat {
                collect_pat_shadowed_params(subpat, shadowed);
            }
        }
        Pat::Or(pat_or) => {
            for case in &pat_or.cases {
                collect_pat_shadowed_params(case, shadowed);
            }
        }
        Pat::Paren(pat_paren) => collect_pat_shadowed_params(&pat_paren.pat, shadowed),
        Pat::Reference(pat_ref) => collect_pat_shadowed_params(&pat_ref.pat, shadowed),
        Pat::Slice(pat_slice) => {
            for elem in &pat_slice.elems {
                collect_pat_shadowed_params(elem, shadowed);
            }
        }
        Pat::Struct(pat_struct) => {
            for field in &pat_struct.fields {
                collect_pat_shadowed_params(&field.pat, shadowed);
            }
        }
        Pat::Tuple(pat_tuple) => {
            for elem in &pat_tuple.elems {
                collect_pat_shadowed_params(elem, shadowed);
            }
        }
        Pat::TupleStruct(pat_tuple_struct) => {
            for elem in &pat_tuple_struct.elems {
                collect_pat_shadowed_params(elem, shadowed);
            }
        }
        Pat::Type(pat_type) => collect_pat_shadowed_params(&pat_type.pat, shadowed),
        _ => {}
    }
}
///
/// # 参数
/// - `tokens`: 过程宏生成或解析的 token 流。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `used`: 已被 SQL、表达式或宏展开消费的参数集合。
/// - `shadowed`: 当前作用域内被局部变量遮蔽的参数名集合。
fn collect_tokens_used_params(
    tokens: &TokenStream2,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
    shadowed: &::std::collections::HashSet<String>,
) {
    for token in tokens.clone() {
        match token {
            TokenTree::Ident(ident) => {
                mark_used_param_unshadowed(&ident, params, used, shadowed);
            }
            TokenTree::Group(group) => {
                collect_tokens_used_params(&group.stream(), params, used, shadowed);
            }
            TokenTree::Punct(_) | TokenTree::Literal(_) => {}
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `key_expr`: 业务 key 或 Redis key,用于定位数据。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
fn gen_method(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    plan: &MethodPlan,
) -> syn::Result<TokenStream2> {
    if plan.kind.is_stream() {
        gen_stream_method(root, plan)
    } else if plan.kind.is_query() {
        gen_query_method(root, key_expr, plan)
    } else {
        gen_write_method(root, key_expr, plan)
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `key_expr`: 业务 key 或 Redis key,用于定位数据。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
fn gen_query_method(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    plan: &MethodPlan,
) -> syn::Result<TokenStream2> {
    let sig = &plan.method.sig;
    let result_ty = result_inner(&sig.output)?;
    let fetch = fetch_mode(plan, result_ty)?;
    let sql_init = sql_init_tokens(root, plan, sig.ident.span());
    let query = query_tokens(root, plan, fetch)?;
    let conn = conn_tokens(root, plan.tx, plan.datasource.as_deref());
    let clear = clear_tokens(root, key_expr, plan.flush_cache, plan.flush_refs);

    if !plan.cache {
        return Ok(quote! {
            #sig {
                #sql_init
                let __mapper_value = {
                    let mut __mapper_conn = #conn;
                    #query.await?
                };
                #clear
                ::core::result::Result::Ok(__mapper_value)
            }
        });
    }

    let cache_ttl = option_u64_tokens(plan.cache_ttl_ms);
    let cache_in_tx = LitBool::new(plan.cache_in_tx, proc_macro2::Span::call_site());
    let cache_ty = result_ty;
    let build_hash = build_hash_key_tokens(root, plan);
    let cache_get_or_load = cache_get_or_load_tokens(
        root,
        cache_ty,
        plan.cache_errors,
        &conn,
        &query,
        plan.typed_cache_codec.as_ref(),
    );
    let cache_codec_init = if let Some(path) = &plan.typed_cache_codec {
        quote! {
            let __mapper_typed_cache_codec = #path();
        }
    } else {
        let cache_codec = match &plan.cache_codec {
            Some(path) => quote! { ::core::option::Option::Some(#path()) },
            None => quote! { self.cache_codec.clone() },
        };
        quote! {
            let __mapper_cache_codec: ::core::option::Option<::std::sync::Arc<dyn #root::MapperCacheCodec>> =
                #cache_codec;
        }
    };

    Ok(quote! {
        #sig {
            let __mapper_l2_key: &str = #key_expr;
            #cache_codec_init
            #sql_init
            let __mapper_cache_ttl_ms: ::core::option::Option<u64> = #cache_ttl;
            // `cache = true` 只表示普通场景启用 L2。ambient 事务内是否也读写 L2
            // 必须由业务显式声明 `cache_in_tx = true`，避免默认把未提交视图写入共享缓存。
            let __mapper_has_l2_cache = self.l2_cache.is_some();
            let __mapper_in_transaction = #root::in_transaction();
            let __mapper_cache_enabled = __mapper_has_l2_cache
                && (!__mapper_in_transaction || #cache_in_tx);

            let __mapper_hash_key = if __mapper_cache_enabled {
                #build_hash
            } else {
                let __mapper_cache_bypass_detail = if !__mapper_has_l2_cache {
                    "no_l2_cache"
                } else {
                    "in_transaction"
                };
                #root::record_mapper_metric(#root::MapperMetric {
                    kind: #root::MapperMetricKind::CacheBypass,
                    mapper_key: __mapper_l2_key,
                    hash_key: ::core::option::Option::None,
                    sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                    detail: ::core::option::Option::Some(__mapper_cache_bypass_detail),
                });
                #root::__private::tracing::debug!(
                    component = "mapper",
                    event = "cache_bypass",
                    mapper_key = %__mapper_l2_key,
                    sql = %__mapper_normalized_sql.as_ref(),
                    reason = %__mapper_cache_bypass_detail,
                    "mapper cache bypass"
                );
                ::core::option::Option::None
            };

            #cache_get_or_load

            let __mapper_value = {
                let mut __mapper_conn = #conn;
                #query.await?
            };

            #clear
            ::core::result::Result::Ok(__mapper_value)
        }
    })
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
fn gen_stream_method(root: &TokenStream2, plan: &MethodPlan) -> syn::Result<TokenStream2> {
    let sig = &plan.method.sig;
    let result_ty = result_inner(&sig.output)?;
    let Some(row_ty) = first_generic_inner(result_ty, "MapperStream") else {
        return Err(syn::Error::new_spanned(
            result_ty,
            "StreamQuery 方法必须返回 anyhow::Result<MapperStream<T>>",
        ));
    };
    let sql_init = sql_init_tokens(root, plan, sig.ident.span());
    let query = stream_query_builder_tokens(root, plan, row_ty)?;
    let pool = pool_tokens(root, plan.datasource.as_deref());

    Ok(quote! {
        #sig {
            if #root::in_transaction() {
                return ::core::result::Result::Err(
                    #root::__private::anyhow::anyhow!(
                        "StreamQuery 首版不支持在 #[transactional] ambient 事务中返回流"
                    )
                );
            }
            #sql_init
            let __mapper_pool = #pool;
            let __mapper_query = #query;
            let __mapper_stream = #root::__private::async_stream::try_stream! {
                let mut __mapper_rows = __mapper_query.fetch(&__mapper_pool);
                while let ::core::option::Option::Some(__mapper_row) =
                    #root::__private::futures_util::TryStreamExt::try_next(&mut __mapper_rows).await?
                {
                    yield __mapper_row;
                }
            };
            ::core::result::Result::Ok(#root::MapperStream::new(__mapper_stream))
        }
    })
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `key_expr`: 业务 key 或 Redis key,用于定位数据。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
fn gen_write_method(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    plan: &MethodPlan,
) -> syn::Result<TokenStream2> {
    let sig = &plan.method.sig;
    let result_ty = result_inner(&sig.output)?;
    let sql_init = sql_init_tokens(root, plan, sig.ident.span());
    let query = write_query_tokens(root, plan)?;
    let conn = conn_tokens(root, plan.tx, plan.datasource.as_deref());
    let clear = clear_tokens(root, key_expr, plan.flush_cache, plan.flush_refs);

    if is_unit(result_ty) {
        Ok(quote! {
            #sig {
                #sql_init
                {
                    let mut __mapper_conn = #conn;
                    #query.execute(__mapper_conn.as_mut()).await?;
                }
                #clear
                ::core::result::Result::Ok(())
            }
        })
    } else if is_type_ident(result_ty, "u64") {
        Ok(quote! {
            #sig {
                #sql_init
                let __mapper_rows_affected = {
                    let mut __mapper_conn = #conn;
                    #query.execute(__mapper_conn.as_mut()).await?.rows_affected()
                };
                #clear
                ::core::result::Result::Ok(__mapper_rows_affected)
            }
        })
    } else {
        Ok(quote! {
            #sig {
                #sql_init
                let __mapper_result = {
                    let mut __mapper_conn = #conn;
                    #query.execute(__mapper_conn.as_mut()).await?
                };
                #clear
                ::core::result::Result::Ok(__mapper_result)
            }
        })
    }
}

/// 查询结果提取模式。
///
/// 宏根据 `fetch` 属性和返回类型选择 `fetch_all`、`fetch_optional`、`fetch_one` 或标量查询路径。
#[derive(Clone, Copy)]
enum FetchMode<'a> {
    /// 读取多行并映射为集合类型。
    All(&'a Type),
    /// 读取可选单行并映射为 `Option<T>`。
    Optional(&'a Type),
    /// 读取必需单行并映射为业务类型。
    One(&'a Type),
    /// 读取单列标量值。
    Scalar(&'a Type),
}
///
/// # 参数
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `result_ty`: mapper 方法最终返回的结果类型。
fn fetch_mode<'a>(plan: &MethodPlan, result_ty: &'a Type) -> syn::Result<FetchMode<'a>> {
    if let Some(fetch) = &plan.fetch {
        return match fetch.as_str() {
            "all" => {
                let Some(inner) = first_generic_inner(result_ty, "Vec") else {
                    return Err(syn::Error::new_spanned(
                        result_ty,
                        "fetch = \"all\" 需要返回 Vec<T>",
                    ));
                };
                Ok(FetchMode::All(inner))
            }
            "optional" => {
                let Some(inner) = first_generic_inner(result_ty, "Option") else {
                    return Err(syn::Error::new_spanned(
                        result_ty,
                        "fetch = \"optional\" 需要返回 Option<T>",
                    ));
                };
                Ok(FetchMode::Optional(inner))
            }
            "one" => {
                reject_container_fetch_ty(result_ty, "fetch = \"one\" 需要返回单行 T")?;
                if is_scalar_type(result_ty) {
                    return Err(syn::Error::new_spanned(
                        result_ty,
                        "标量查询需要显式 fetch = \"scalar\"",
                    ));
                }
                Ok(FetchMode::One(result_ty))
            }
            "scalar" => {
                reject_container_fetch_ty(result_ty, "fetch = \"scalar\" 需要返回标量 T")?;
                Ok(FetchMode::Scalar(result_ty))
            }
            other => Err(syn::Error::new_spanned(
                result_ty,
                format!("fetch 不支持 `{other}`"),
            )),
        };
    }
    if let Some(inner) = first_generic_inner(result_ty, "Vec") {
        Ok(FetchMode::All(inner))
    } else if let Some(inner) = first_generic_inner(result_ty, "Option") {
        Ok(FetchMode::Optional(inner))
    } else if is_scalar_type(result_ty) {
        Err(syn::Error::new_spanned(
            result_ty,
            "标量查询需要显式 fetch = \"scalar\"",
        ))
    } else {
        Ok(FetchMode::One(result_ty))
    }
}
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
/// - `msg`: 业务消息体或事件载荷。
fn reject_container_fetch_ty(ty: &Type, msg: &str) -> syn::Result<()> {
    if first_generic_inner(ty, "Vec").is_some() || first_generic_inner(ty, "Option").is_some() {
        return Err(syn::Error::new_spanned(ty, msg));
    }
    Ok(())
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `fetch`: 本次发现或 HTTP 取数操作的结果。
fn query_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    fetch: FetchMode<'_>,
) -> syn::Result<TokenStream2> {
    let query = if plan.checked {
        checked_query_builder_tokens(root, plan, fetch)?
    } else {
        query_builder_tokens(root, plan, fetch)?
    };
    Ok(match fetch {
        FetchMode::All(_) => quote! { #query.fetch_all(__mapper_conn.as_mut()) },
        FetchMode::Optional(_) => quote! { #query.fetch_optional(__mapper_conn.as_mut()) },
        FetchMode::One(_) | FetchMode::Scalar(_) => {
            quote! { #query.fetch_one(__mapper_conn.as_mut()) }
        }
    })
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `fetch`: 本次发现或 HTTP 取数操作的结果。
fn query_builder_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    fetch: FetchMode<'_>,
) -> syn::Result<TokenStream2> {
    let mut query = match fetch {
        FetchMode::Scalar(row_ty) => {
            quote! {
                #root::__private::sqlx::query_scalar::<_, #row_ty>(
                    #root::__private::sqlx::AssertSqlSafe(__mapper_normalized_sql.clone())
                )
            }
        }
        FetchMode::All(row_ty) | FetchMode::Optional(row_ty) | FetchMode::One(row_ty) => {
            quote! {
                #root::__private::sqlx::query_as::<_, #row_ty>(
                    #root::__private::sqlx::AssertSqlSafe(__mapper_normalized_sql.clone())
                )
            }
        }
    };
    query = if let Some(dynamic) = &plan.dynamic {
        apply_dynamic_binds_tokens(query, dynamic, &plan.params)?
    } else {
        apply_binds_tokens(query, &plan.binds, &plan.params)?
    };
    Ok(query)
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `fetch`: 本次发现或 HTTP 取数操作的结果。
fn checked_query_builder_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    fetch: FetchMode<'_>,
) -> syn::Result<TokenStream2> {
    let sql = LitStr::new(&plan.normalized_sql, plan.method.sig.ident.span());
    let args = plan
        .binds
        .iter()
        .map(|bind| bind_expr(bind, &plan.params))
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(match fetch {
        FetchMode::Scalar(_) => {
            quote! {
                #root::__private::sqlx::query_scalar!(#sql, #(#args),*)
            }
        }
        FetchMode::All(row_ty) | FetchMode::Optional(row_ty) | FetchMode::One(row_ty) => {
            quote! {
                #root::__private::sqlx::query_as!(#row_ty, #sql, #(#args),*)
            }
        }
    })
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `row_ty`: 数据库行转换时使用的业务结构类型。
fn stream_query_builder_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    row_ty: &Type,
) -> syn::Result<TokenStream2> {
    let query = quote! {
        #root::__private::sqlx::query_as::<_, #row_ty>(
            #root::__private::sqlx::AssertSqlSafe(__mapper_normalized_sql.clone())
        )
    };
    apply_stream_binds_tokens(query, &plan.binds, &plan.params)
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
fn write_query_tokens(root: &TokenStream2, plan: &MethodPlan) -> syn::Result<TokenStream2> {
    let query = quote! {
        #root::__private::sqlx::query(
            #root::__private::sqlx::AssertSqlSafe(__mapper_normalized_sql.clone())
        )
    };
    if let Some(dynamic) = &plan.dynamic {
        apply_dynamic_binds_tokens(query, dynamic, &plan.params)
    } else {
        apply_binds_tokens(query, &plan.binds, &plan.params)
    }
}
///
/// # 参数
/// - `query`: 查询对象或 query 参数集合。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `params`: 已解析的函数参数或宏参数列表。
fn apply_binds_tokens(
    mut query: TokenStream2,
    binds: &[BindInfo],
    params: &[ParamInfo],
) -> syn::Result<TokenStream2> {
    for bind in binds {
        match bind.kind {
            BindKind::Scalar => {
                let expr = bind_expr(bind, params)?;
                query = quote! { #query.bind(#expr) };
            }
            BindKind::List => {
                let ident = &bind.root;
                query = quote! {
                    {
                        let mut __mapper_query = #query;
                        for __mapper_item in #ident.iter() {
                            __mapper_query = __mapper_query.bind(__mapper_item);
                        }
                        __mapper_query
                    }
                };
            }
        }
    }
    Ok(query)
}
///
/// # 参数
/// - `query`: 查询对象或 query 参数集合。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `params`: 已解析的函数参数或宏参数列表。
fn apply_stream_binds_tokens(
    mut query: TokenStream2,
    binds: &[BindInfo],
    params: &[ParamInfo],
) -> syn::Result<TokenStream2> {
    for bind in binds {
        let expr = stream_bind_expr(bind, params)?;
        query = quote! { #query.bind(#expr) };
    }
    Ok(query)
}
///
/// # 参数
/// - `query`: 查询对象或 query 参数集合。
/// - `dynamic`: 动态 SQL 节点或动态绑定上下文。
/// - `params`: 已解析的函数参数或宏参数列表。
fn apply_dynamic_binds_tokens(
    query: TokenStream2,
    dynamic: &DynamicSqlPlan,
    params: &[ParamInfo],
) -> syn::Result<TokenStream2> {
    let body = dynamic_bind_steps_tokens(&dynamic.nodes, params, &[])?;
    Ok(quote! {
        {
            let mut __mapper_query = #query;
            #body
            __mapper_query
        }
    })
}
///
/// # 参数
/// - `nodes`: 动态 SQL 节点列表。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
fn dynamic_bind_steps_tokens(
    nodes: &[SqlNode],
    params: &[ParamInfo],
    local_items: &[Ident],
) -> syn::Result<TokenStream2> {
    let mut steps = Vec::new();
    for node in nodes {
        match node {
            SqlNode::Text(_) => {}
            SqlNode::Bind(bind) => {
                steps.push(dynamic_bind_one_token(bind, params, local_items)?);
            }
            SqlNode::If { test, body } => {
                let body = dynamic_bind_steps_tokens(body, params, local_items)?;
                steps.push(quote! {
                    if #test {
                        #body
                    }
                });
            }
            SqlNode::Choose { whens, otherwise } => {
                let otherwise = dynamic_bind_steps_tokens(otherwise, params, local_items)?;
                let mut chain = quote! { #otherwise };
                for when in whens.iter().rev() {
                    let test = &when.test;
                    let body = dynamic_bind_steps_tokens(&when.body, params, local_items)?;
                    chain = quote! {
                        if #test {
                            #body
                        } else {
                            #chain
                        }
                    };
                }
                steps.push(chain);
            }
            SqlNode::Foreach {
                collection,
                item,
                body,
                ..
            } => {
                let mut next_items = local_items.to_vec();
                next_items.push(item.clone());
                let body = dynamic_bind_steps_tokens(body, params, &next_items)?;
                steps.push(quote! {
                    for #item in #collection.iter() {
                        #body
                    }
                });
            }
            SqlNode::Trim { body, .. } => {
                steps.push(dynamic_bind_steps_tokens(body, params, local_items)?);
            }
            SqlNode::OrderBy { .. } => {}
        }
    }
    Ok(quote! { #(#steps)* })
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
fn dynamic_bind_one_token(
    bind: &BindInfo,
    params: &[ParamInfo],
    local_items: &[Ident],
) -> syn::Result<TokenStream2> {
    match bind.kind {
        BindKind::Scalar => {
            let expr = if local_items.contains(&bind.root) {
                foreach_item_bind_expr(bind)
            } else {
                bind_expr(bind, params)?
            };
            Ok(quote! {
                __mapper_query = __mapper_query.bind(#expr);
            })
        }
        BindKind::List => {
            let ident = &bind.root;
            Ok(quote! {
                for __mapper_item in #ident.iter() {
                    __mapper_query = __mapper_query.bind(__mapper_item);
                }
            })
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn sql_init_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    span: proc_macro2::Span,
) -> TokenStream2 {
    if let Some(dynamic) = &plan.dynamic {
        return dynamic_sql_init_tokens(root, dynamic, span);
    }

    if !plan.binds.iter().any(|bind| bind.kind == BindKind::List) {
        let sql_lit = LitStr::new(&plan.normalized_sql, span);
        return quote! {
            let __mapper_normalized_sql: ::std::borrow::Cow<'static, str> =
                ::std::borrow::Cow::Borrowed(#sql_lit);
        };
    }

    let mut parts = Vec::new();
    for (idx, bind) in plan.binds.iter().enumerate() {
        let fragment = LitStr::new(&plan.sql_fragments[idx], span);
        parts.push(quote! {
            __mapper_sql.push_str(#fragment);
        });
        match bind.kind {
            BindKind::Scalar => {
                parts.push(quote! {
                    __mapper_sql.push('?');
                });
            }
            BindKind::List => {
                let ident = &bind.root;
                parts.push(quote! {
                    __mapper_sql.push_str(
                        #root::__private::sql_in_placeholders(#ident.len())?.as_str(),
                    );
                });
            }
        }
    }
    let last_fragment = plan.sql_fragments.last().cloned().unwrap_or_default();
    let last_fragment = LitStr::new(&last_fragment, span);
    quote! {
        let __mapper_normalized_sql: ::std::borrow::Cow<'static, str> = {
            let mut __mapper_sql = ::std::string::String::new();
            #(#parts)*
            __mapper_sql.push_str(#last_fragment);
            ::std::borrow::Cow::Owned(#root::__private::normalize_sql_whitespace(&__mapper_sql))
        };
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `dynamic`: 动态 SQL 节点或动态绑定上下文。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn dynamic_sql_init_tokens(
    root: &TokenStream2,
    dynamic: &DynamicSqlPlan,
    span: proc_macro2::Span,
) -> TokenStream2 {
    let target = format_ident!("__mapper_sql");
    let body = push_dynamic_sql_nodes_tokens(root, &dynamic.nodes, span, &target);
    quote! {
        let __mapper_normalized_sql: ::std::borrow::Cow<'static, str> = {
            let mut #target = ::std::string::String::new();
            #body
            ::std::borrow::Cow::Owned(#root::__private::normalize_sql_whitespace(&#target))
        };
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `nodes`: 动态 SQL 节点列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
/// - `target`: 生成代码中承载动态 SQL 字符串的目标 buffer 变量。
fn push_dynamic_sql_nodes_tokens(
    root: &TokenStream2,
    nodes: &[SqlNode],
    span: proc_macro2::Span,
    target: &Ident,
) -> TokenStream2 {
    let mut parts = Vec::new();
    for node in nodes {
        match node {
            SqlNode::Text(text) => {
                let lit = LitStr::new(text, span);
                parts.push(quote! {
                    #target.push_str(#lit);
                });
            }
            SqlNode::Bind(bind) => match bind.kind {
                BindKind::Scalar => {
                    parts.push(quote! {
                        #target.push('?');
                    });
                }
                BindKind::List => {
                    let ident = &bind.root;
                    parts.push(quote! {
                        #target.push_str(
                            #root::__private::sql_in_placeholders(#ident.len())?.as_str(),
                        );
                    });
                }
            },
            SqlNode::If { test, body } => {
                let body = push_dynamic_sql_nodes_tokens(root, body, span, target);
                parts.push(quote! {
                    if #test {
                        #body
                    }
                });
            }
            SqlNode::Choose { whens, otherwise } => {
                let otherwise = push_dynamic_sql_nodes_tokens(root, otherwise, span, target);
                let mut chain = quote! { #otherwise };
                for when in whens.iter().rev() {
                    let test = &when.test;
                    let body = push_dynamic_sql_nodes_tokens(root, &when.body, span, target);
                    chain = quote! {
                        if #test {
                            #body
                        } else {
                            #chain
                        }
                    };
                }
                parts.push(chain);
            }
            SqlNode::Foreach {
                collection,
                open,
                separator,
                close,
                body,
                ..
            } => {
                let open = LitStr::new(open, span);
                let separator = LitStr::new(separator, span);
                let close = LitStr::new(close, span);
                let body = push_dynamic_sql_nodes_tokens(root, body, span, target);
                parts.push(quote! {
                    if #collection.is_empty() {
                        return ::core::result::Result::Err(
                            #root::__private::anyhow::anyhow!("Mapper foreach collection `{}` 不能为空", stringify!(#collection))
                        );
                    }
                    #target.push_str(#open);
                    let mut __mapper_first_foreach_item = true;
                    for _ in #collection.iter() {
                        if __mapper_first_foreach_item {
                            __mapper_first_foreach_item = false;
                        } else {
                            #target.push_str(#separator);
                        }
                        #body
                    }
                    #target.push_str(#close);
                });
            }
            SqlNode::Trim {
                prefix,
                suffix,
                prefix_overrides,
                suffix_overrides,
                body,
            } => {
                let trim_target = format_ident!("__mapper_trim_sql");
                let prefix = LitStr::new(prefix, span);
                let suffix = LitStr::new(suffix, span);
                let prefix_overrides = str_array_tokens(prefix_overrides);
                let suffix_overrides = str_array_tokens(suffix_overrides);
                let body = push_dynamic_sql_nodes_tokens(root, body, span, &trim_target);
                parts.push(quote! {
                    {
                        let mut #trim_target = ::std::string::String::new();
                        #body
                        if let ::core::option::Option::Some(__mapper_trimmed_sql) =
                            #root::__private::apply_sql_trim(
                                &#trim_target,
                                #prefix,
                                #suffix,
                                #prefix_overrides,
                                #suffix_overrides,
                            )
                        {
                            #target.push_str(&__mapper_trimmed_sql);
                        }
                    }
                });
            }
            SqlNode::OrderBy { value } => {
                parts.push(quote! {
                    #root::__private::write_mapper_order_by_clause(&#value, &mut #target)?;
                });
            }
        }
    }
    quote! { #(#parts)* }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
fn build_hash_key_tokens(root: &TokenStream2, plan: &MethodPlan) -> TokenStream2 {
    let args = if let Some(dynamic) = &plan.dynamic {
        dynamic_cache_arg_steps_tokens(root, &dynamic.nodes, &[])
    } else {
        cache_arg_steps_tokens(root, &plan.binds, None)
    };
    let hash_call = if let Some(suffix) = &plan.hash_key_suffix {
        let suffix_lit = LitStr::new(suffix, proc_macro2::Span::call_site());
        quote! { #root::cache_hash_key_with_suffix(__mapper_normalized_sql.as_ref(), #suffix_lit, &__mapper_cache_args) }
    } else {
        quote! { #root::cache_hash_key(__mapper_normalized_sql.as_ref(), &__mapper_cache_args) }
    };
    let strict = plan.cache_errors.is_strict();
    let err_arm = if strict {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheHashKeyError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::None,
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_hash_key_error",
                mapper_key = %__mapper_l2_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache hash_key build failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheHashKeyError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::None,
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_hash_key_error",
                mapper_key = %__mapper_l2_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache hash_key build failed, bypass"
            );
            ::core::option::Option::None
        }
    };
    quote! {
        let __mapper_hash_key_result = (|| {
            let mut __mapper_cache_args: ::std::vec::Vec<#root::CacheArg> =
                ::std::vec::Vec::new();
            #args
            #hash_call
        })();
        match __mapper_hash_key_result {
            ::core::result::Result::Ok(hash_key) => ::core::option::Option::Some(hash_key),
            ::core::result::Result::Err(e) => {
                #err_arm
            }
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `nodes`: 动态 SQL 节点列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
fn dynamic_cache_arg_steps_tokens(
    root: &TokenStream2,
    nodes: &[SqlNode],
    local_items: &[Ident],
) -> TokenStream2 {
    let mut steps = Vec::new();
    for node in nodes {
        match node {
            SqlNode::Text(_) => {}
            SqlNode::Bind(bind) => {
                steps.push(dynamic_cache_arg_one_token(root, bind, local_items));
            }
            SqlNode::If { test, body } => {
                let body = dynamic_cache_arg_steps_tokens(root, body, local_items);
                steps.push(quote! {
                    if #test {
                        #body
                    }
                });
            }
            SqlNode::Choose { whens, otherwise } => {
                let otherwise = dynamic_cache_arg_steps_tokens(root, otherwise, local_items);
                let mut chain = quote! { #otherwise };
                for when in whens.iter().rev() {
                    let test = &when.test;
                    let body = dynamic_cache_arg_steps_tokens(root, &when.body, local_items);
                    chain = quote! {
                        if #test {
                            #body
                        } else {
                            #chain
                        }
                    };
                }
                steps.push(chain);
            }
            SqlNode::Foreach {
                collection,
                item,
                body,
                ..
            } => {
                let mut next_items = local_items.to_vec();
                next_items.push(item.clone());
                let body = dynamic_cache_arg_steps_tokens(root, body, &next_items);
                steps.push(quote! {
                    for #item in #collection.iter() {
                        #body
                    }
                });
            }
            SqlNode::Trim { body, .. } => {
                steps.push(dynamic_cache_arg_steps_tokens(root, body, local_items));
            }
            SqlNode::OrderBy { .. } => {}
        }
    }
    quote! { #(#steps)* }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `bind`: SQL 占位符绑定信息。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
fn dynamic_cache_arg_one_token(
    root: &TokenStream2,
    bind: &BindInfo,
    local_items: &[Ident],
) -> TokenStream2 {
    let name = bind.name.as_str();
    match bind.kind {
        BindKind::Scalar => {
            let value = if local_items.contains(&bind.root) {
                foreach_item_bind_value_expr(bind)
            } else {
                bind_value_expr(bind)
            };
            quote! {
                __mapper_cache_args.push(#root::CacheArg::try_new(#name, #value)?);
            }
        }
        BindKind::List => {
            let ident = &bind.root;
            quote! {
                for __mapper_item in #ident.iter() {
                    __mapper_cache_args.push(#root::CacheArg::try_new(#name, __mapper_item)?);
                }
            }
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `cache_ty`: mapper 缓存层注入的缓存实现类型。
/// - `cache_errors`: 缓存读写错误处理策略。
/// - `conn`: 生成代码里传给 mapper 缓存层的数据库连接表达式。
/// - `query`: 查询对象或 query 参数集合。
/// - `typed_cache_codec`: 强类型缓存使用的 codec 表达式。
fn cache_get_or_load_tokens(
    root: &TokenStream2,
    cache_ty: &Type,
    cache_errors: CacheErrors,
    conn: &TokenStream2,
    query: &TokenStream2,
    typed_cache_codec: Option<&Path>,
) -> TokenStream2 {
    let load_err = if cache_errors.is_strict() {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheLoadError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_load_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache get_or_load failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheLoadError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_load_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache get_or_load failed, bypass"
            );
        }
    };
    let load_err_loaded_value_fallback = if cache_errors.is_strict() {
        quote! {}
    } else {
        quote! {
            if let ::core::option::Option::Some(value) =
                __mapper_cache_loaded_value.take()
            {
                return ::core::result::Result::Ok(value);
            }
        }
    };
    let decode_err = if cache_errors.is_strict() {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheDecodeError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_decode_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache decode failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheDecodeError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_decode_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache decode failed, bypass"
            );
        }
    };
    let encode_cache_value = if typed_cache_codec.is_some() {
        quote! {
            #root::encode_typed_cache_value(
                &__mapper_value,
                &__mapper_typed_cache_codec,
            )
        }
    } else {
        quote! {
            #root::encode_cache_value_with_codec(
                &__mapper_value,
                __mapper_cache_codec.as_deref(),
            )
        }
    };
    let decode_cache_value = if typed_cache_codec.is_some() {
        quote! {
            #root::decode_typed_cache_value::<#cache_ty, _>(
                &__mapper_load.bytes,
                &__mapper_typed_cache_codec,
            )
        }
    } else {
        quote! {
            #root::decode_cache_value_with_codec::<#cache_ty>(
                &__mapper_load.bytes,
                __mapper_cache_codec.as_deref(),
            )
        }
    };
    let rewrite_cached_value = if typed_cache_codec.is_some() {
        quote! {}
    } else {
        quote! {
            if !::core::matches!(
                __mapper_load.state,
                #root::MapperCacheLoadState::Loaded
            ) && #root::cache_value_needs_rewrite(
                &__mapper_load.bytes,
                __mapper_cache_codec.as_deref(),
            ) {
                match #root::encode_cache_value_with_codec(
                    &value,
                    __mapper_cache_codec.as_deref(),
                ) {
                    ::core::result::Result::Ok(__mapper_rewrite_bytes) => {
                        match cache
                            .put(
                                __mapper_l2_key,
                                hash_key,
                                &__mapper_rewrite_bytes,
                                __mapper_cache_ttl_ms,
                            )
                            .await
                        {
                            ::core::result::Result::Ok(()) => {
                                #root::record_mapper_metric(#root::MapperMetric {
                                    kind: #root::MapperMetricKind::CachePut,
                                    mapper_key: __mapper_l2_key,
                                    hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                    sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                    detail: ::core::option::Option::Some("rewrite"),
                                });
                                #root::__private::tracing::debug!(
                                    component = "mapper",
                                    event = "cache_rewrite",
                                    mapper_key = %__mapper_l2_key,
                                    hash_key = %hash_key,
                                    sql = %__mapper_normalized_sql.as_ref(),
                                    ttl_ms = ?__mapper_cache_ttl_ms,
                                    "mapper cache value rewritten with current codec"
                                );
                            }
                            ::core::result::Result::Err(e) => {
                                #root::record_mapper_metric(#root::MapperMetric {
                                    kind: #root::MapperMetricKind::CachePutError,
                                    mapper_key: __mapper_l2_key,
                                    hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                    sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                    detail: ::core::option::Option::Some("rewrite"),
                                });
                                #root::__private::tracing::warn!(
                                    component = "mapper",
                                    event = "cache_rewrite_put_error",
                                    mapper_key = %__mapper_l2_key,
                                    hash_key = %hash_key,
                                    sql = %__mapper_normalized_sql.as_ref(),
                                    error = %e,
                                    "mapper cache rewrite put failed"
                                );
                            }
                        }
                    }
                    ::core::result::Result::Err(e) => {
                        #root::record_mapper_metric(#root::MapperMetric {
                            kind: #root::MapperMetricKind::CacheEncodeError,
                            mapper_key: __mapper_l2_key,
                            hash_key: ::core::option::Option::Some(hash_key.as_str()),
                            sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                            detail: ::core::option::Option::Some("rewrite"),
                        });
                        #root::__private::tracing::warn!(
                            component = "mapper",
                            event = "cache_rewrite_encode_error",
                            mapper_key = %__mapper_l2_key,
                            hash_key = %hash_key,
                            sql = %__mapper_normalized_sql.as_ref(),
                            error = %e,
                            "mapper cache rewrite encode failed"
                        );
                    }
                }
            }
        }
    };
    quote! {
        if let (::core::option::Option::Some(cache), ::core::option::Option::Some(hash_key)) =
            (&self.l2_cache, __mapper_hash_key.as_ref())
        {
            let mut __mapper_cache_loaded_value: ::core::option::Option<#cache_ty> =
                ::core::option::Option::None;
            let __mapper_cache_loader: #root::MapperCacheLoader<'_> =
                ::std::boxed::Box::new(|| {
                    ::std::boxed::Box::pin(async {
                        let __mapper_value = {
                            let mut __mapper_conn = #conn;
                            #query.await?
                        };
                        let __mapper_bytes = match #encode_cache_value {
                            ::core::result::Result::Ok(bytes) => bytes,
                            ::core::result::Result::Err(e) => {
                                __mapper_cache_loaded_value =
                                    ::core::option::Option::Some(__mapper_value);
                                return ::core::result::Result::Err(e);
                            }
                        };
                        __mapper_cache_loaded_value =
                            ::core::option::Option::Some(__mapper_value);
                        ::core::result::Result::Ok(__mapper_bytes)
                    })
                });
            match cache
                .get_or_load(
                    __mapper_l2_key,
                    hash_key,
                    __mapper_cache_ttl_ms,
                    __mapper_cache_loader,
                )
                .await
            {
                ::core::result::Result::Ok(__mapper_load) => {
                    if ::core::matches!(
                        __mapper_load.state,
                        #root::MapperCacheLoadState::Loaded
                    ) {
                        if let ::core::option::Option::Some(value) =
                            __mapper_cache_loaded_value.take()
                        {
                            #root::record_mapper_metric(#root::MapperMetric {
                                kind: #root::MapperMetricKind::CacheMiss,
                                mapper_key: __mapper_l2_key,
                                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                detail: ::core::option::Option::None,
                            });
                            #root::record_mapper_metric(#root::MapperMetric {
                                kind: #root::MapperMetricKind::CachePut,
                                mapper_key: __mapper_l2_key,
                                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                detail: ::core::option::Option::None,
                            });
                            #root::record_mapper_metric(#root::MapperMetric {
                                kind: #root::MapperMetricKind::CacheLoad,
                                mapper_key: __mapper_l2_key,
                                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                detail: ::core::option::Option::None,
                            });
                            #root::__private::tracing::debug!(
                                component = "mapper",
                                event = "cache_load",
                                mapper_key = %__mapper_l2_key,
                                hash_key = %hash_key,
                                sql = %__mapper_normalized_sql.as_ref(),
                                state = ?__mapper_load.state,
                                "mapper cache get_or_load"
                            );
                            return ::core::result::Result::Ok(value);
                        }
                    }
                    match #decode_cache_value {
                        ::core::result::Result::Ok(value) => {
                            if ::core::matches!(
                                __mapper_load.state,
                                #root::MapperCacheLoadState::Loaded
                            ) {
                                #root::record_mapper_metric(#root::MapperMetric {
                                    kind: #root::MapperMetricKind::CacheMiss,
                                    mapper_key: __mapper_l2_key,
                                    hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                    sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                    detail: ::core::option::Option::None,
                                });
                                #root::record_mapper_metric(#root::MapperMetric {
                                    kind: #root::MapperMetricKind::CachePut,
                                    mapper_key: __mapper_l2_key,
                                    hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                    sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                    detail: ::core::option::Option::None,
                                });
                            }
                            #rewrite_cached_value
                            let __mapper_metric_kind = match __mapper_load.state {
                                #root::MapperCacheLoadState::Hit => #root::MapperMetricKind::CacheHit,
                                #root::MapperCacheLoadState::HitAfterWait => {
                                    #root::MapperMetricKind::CacheHitAfterWait
                                }
                                #root::MapperCacheLoadState::Loaded => #root::MapperMetricKind::CacheLoad,
                            };
                            #root::record_mapper_metric(#root::MapperMetric {
                                kind: __mapper_metric_kind,
                                mapper_key: __mapper_l2_key,
                                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                detail: ::core::option::Option::None,
                            });
                            let __mapper_cache_event = match __mapper_load.state {
                                #root::MapperCacheLoadState::Hit => "cache_hit",
                                #root::MapperCacheLoadState::HitAfterWait => "cache_hit_after_wait",
                                #root::MapperCacheLoadState::Loaded => "cache_load",
                            };
                            #root::__private::tracing::debug!(
                                component = "mapper",
                                event = __mapper_cache_event,
                                mapper_key = %__mapper_l2_key,
                                hash_key = %hash_key,
                                sql = %__mapper_normalized_sql.as_ref(),
                                state = ?__mapper_load.state,
                                "mapper cache get_or_load"
                            );
                            return ::core::result::Result::Ok(value);
                        }
                        ::core::result::Result::Err(e) => {
                            #decode_err
                        }
                    }
                }
                ::core::result::Result::Err(e) => {
                    #load_err
                    #load_err_loaded_value_fallback
                }
            }
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `cache_ty`: mapper 缓存层注入的缓存实现类型。
/// - `cache_errors`: 缓存读写错误处理策略。
#[allow(dead_code)]
fn cache_get_tokens(
    root: &TokenStream2,
    cache_ty: &Type,
    cache_errors: CacheErrors,
) -> TokenStream2 {
    let get_err = if cache_errors.is_strict() {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheGetError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_get_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache get failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheGetError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_get_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache get failed, bypass"
            );
        }
    };
    let decode_err = if cache_errors.is_strict() {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheDecodeError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_decode_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache decode failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheDecodeError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_decode_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache decode failed, bypass"
            );
        }
    };
    quote! {
        if let (::core::option::Option::Some(cache), ::core::option::Option::Some(hash_key)) =
            (&self.l2_cache, __mapper_hash_key.as_ref())
        {
            match cache.get(__mapper_l2_key, hash_key).await {
                ::core::result::Result::Ok(::core::option::Option::Some(bytes)) => {
                    match #root::decode_cache_value_with_codec::<#cache_ty>(
                        &bytes,
                        __mapper_cache_codec.as_deref(),
                    ) {
                        ::core::result::Result::Ok(value) => {
                            #root::record_mapper_metric(#root::MapperMetric {
                                kind: #root::MapperMetricKind::CacheHit,
                                mapper_key: __mapper_l2_key,
                                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                detail: ::core::option::Option::None,
                            });
                            #root::__private::tracing::debug!(
                                component = "mapper",
                                event = "cache_hit",
                                mapper_key = %__mapper_l2_key,
                                hash_key = %hash_key,
                                sql = %__mapper_normalized_sql.as_ref(),
                                "mapper cache hit"
                            );
                            return ::core::result::Result::Ok(value);
                        }
                        ::core::result::Result::Err(e) => {
                            #decode_err
                        }
                    }
                }
                ::core::result::Result::Ok(::core::option::Option::None) => {
                    #root::record_mapper_metric(#root::MapperMetric {
                        kind: #root::MapperMetricKind::CacheMiss,
                        mapper_key: __mapper_l2_key,
                        hash_key: ::core::option::Option::Some(hash_key.as_str()),
                        sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                        detail: ::core::option::Option::None,
                    });
                    #root::__private::tracing::debug!(
                        component = "mapper",
                        event = "cache_miss",
                        mapper_key = %__mapper_l2_key,
                        hash_key = %hash_key,
                        sql = %__mapper_normalized_sql.as_ref(),
                        "mapper cache miss"
                    );
                }
                ::core::result::Result::Err(e) => {
                    #get_err
                }
            }
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `cache_errors`: 缓存读写错误处理策略。
#[allow(dead_code)]
fn cache_put_tokens(root: &TokenStream2, cache_errors: CacheErrors) -> TokenStream2 {
    let encode_err = if cache_errors.is_strict() {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheEncodeError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_encode_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache encode failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CacheEncodeError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_encode_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache encode failed, return source value"
            );
        }
    };
    let put_err = if cache_errors.is_strict() {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CachePutError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::error!(
                component = "mapper",
                event = "cache_put_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache put failed"
            );
            return ::core::result::Result::Err(e.into());
        }
    } else {
        quote! {
            #root::record_mapper_metric(#root::MapperMetric {
                kind: #root::MapperMetricKind::CachePutError,
                mapper_key: __mapper_l2_key,
                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                detail: ::core::option::Option::None,
            });
            #root::__private::tracing::warn!(
                component = "mapper",
                event = "cache_put_error",
                mapper_key = %__mapper_l2_key,
                hash_key = %hash_key,
                sql = %__mapper_normalized_sql.as_ref(),
                error = %e,
                "mapper cache put failed, return source value"
            );
        }
    };
    quote! {
        if let (::core::option::Option::Some(cache), ::core::option::Option::Some(hash_key)) =
            (&self.l2_cache, __mapper_hash_key.as_ref())
        {
            match #root::encode_cache_value_with_codec(
                &__mapper_value,
                __mapper_cache_codec.as_deref(),
            ) {
                ::core::result::Result::Ok(bytes) => {
                    match cache.put(__mapper_l2_key, hash_key, &bytes, __mapper_cache_ttl_ms).await {
                        ::core::result::Result::Ok(()) => {
                            #root::record_mapper_metric(#root::MapperMetric {
                                kind: #root::MapperMetricKind::CachePut,
                                mapper_key: __mapper_l2_key,
                                hash_key: ::core::option::Option::Some(hash_key.as_str()),
                                sql: ::core::option::Option::Some(__mapper_normalized_sql.as_ref()),
                                detail: ::core::option::Option::None,
                            });
                            #root::__private::tracing::debug!(
                                component = "mapper",
                                event = "cache_put",
                                mapper_key = %__mapper_l2_key,
                                hash_key = %hash_key,
                                sql = %__mapper_normalized_sql.as_ref(),
                                ttl_ms = ?__mapper_cache_ttl_ms,
                                "mapper cache put"
                            );
                        }
                        ::core::result::Result::Err(e) => {
                            #put_err
                        }
                    }
                }
                ::core::result::Result::Err(e) => {
                    #encode_err
                }
            }
        }
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `key_expr`: 业务 key 或 Redis key,用于定位数据。
/// - `flush_cache`: 写方法完成后需要刷新的缓存声明。
/// - `flush_refs`: 写方法完成后需要刷新的关联缓存引用。
fn clear_tokens(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    flush_cache: bool,
    flush_refs: bool,
) -> TokenStream2 {
    if flush_cache {
        quote! {
            let __mapper_clear_keys = #root::cache_clear_targets(#key_expr, #flush_refs);
            #root::clear_after_commit_or_now(self.l2_cache.clone(), __mapper_clear_keys).await?;
        }
    } else {
        quote! {}
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `tx`: 后台任务发送消息的通道或事务句柄。
/// - `datasource`: mapper 方法绑定的数据源名称。
fn conn_tokens(root: &TokenStream2, tx: TxMode, datasource: Option<&str>) -> TokenStream2 {
    match (tx, datasource) {
        (TxMode::Auto, Some(datasource)) => {
            let datasource = LitStr::new(datasource, proc_macro2::Span::call_site());
            quote! { #root::conn_for(#datasource).await? }
        }
        (TxMode::Auto, None) => quote! { #root::conn().await? },
        (TxMode::Mandatory, Some(datasource)) => {
            let datasource = LitStr::new(datasource, proc_macro2::Span::call_site());
            quote! { #root::mandatory_conn_for(#datasource).await? }
        }
        (TxMode::Mandatory, None) => quote! { #root::mandatory_conn().await? },
    }
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `datasource`: mapper 方法绑定的数据源名称。
fn pool_tokens(root: &TokenStream2, datasource: Option<&str>) -> TokenStream2 {
    match datasource {
        Some(datasource) => {
            let datasource = LitStr::new(datasource, proc_macro2::Span::call_site());
            quote! { #root::pool_for(#datasource)? }
        }
        None => quote! { #root::pool_for("default")? },
    }
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
/// - `params`: 已解析的函数参数或宏参数列表。
fn bind_expr(bind: &BindInfo, params: &[ParamInfo]) -> syn::Result<TokenStream2> {
    let ident = &bind.root;
    let param = params
        .iter()
        .find(|param| param.ident == *ident)
        .ok_or_else(|| syn::Error::new_spanned(ident, "bind 参数不存在"))?;
    if !bind.path.is_empty() {
        let value = bind_access_tokens(bind);
        return Ok(quote! { &#value });
    }
    if matches!(param.ty, Type::Reference(_)) {
        Ok(quote! { #ident })
    } else {
        Ok(quote! { &#ident })
    }
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
/// - `params`: 已解析的函数参数或宏参数列表。
fn stream_bind_expr(bind: &BindInfo, params: &[ParamInfo]) -> syn::Result<TokenStream2> {
    let ident = &bind.root;
    let param = params
        .iter()
        .find(|param| param.ident == *ident)
        .ok_or_else(|| syn::Error::new_spanned(ident, "bind 参数不存在"))?;
    if !bind.path.is_empty() {
        let value = bind_access_tokens(bind);
        return Ok(quote! { #value.clone() });
    }
    if matches!(param.ty, Type::Reference(_)) {
        Ok(quote! { #ident.to_owned() })
    } else {
        Ok(quote! { #ident })
    }
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
fn bind_value_expr(bind: &BindInfo) -> TokenStream2 {
    let value = bind_access_tokens(bind);
    quote! { &#value }
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
fn foreach_item_bind_expr(bind: &BindInfo) -> TokenStream2 {
    let value = bind_access_tokens(bind);
    if bind.path.is_empty() {
        quote! { #value }
    } else {
        quote! { &#value }
    }
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
fn foreach_item_bind_value_expr(bind: &BindInfo) -> TokenStream2 {
    foreach_item_bind_expr(bind)
}
///
/// # 参数
/// - `bind`: SQL 占位符绑定信息。
fn bind_access_tokens(bind: &BindInfo) -> TokenStream2 {
    let root = &bind.root;
    let mut value = quote! { #root };
    for segment in &bind.path {
        value = quote! { #value.#segment };
    }
    value
}
///
/// # 参数
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `foreach_item`: foreach 动态 SQL 当前循环项变量名。
fn cache_arg_steps_tokens(
    root: &TokenStream2,
    binds: &[BindInfo],
    foreach_item: Option<&Ident>,
) -> TokenStream2 {
    let steps = binds.iter().map(|bind| {
        let name = bind.name.as_str();
        match bind.kind {
            BindKind::Scalar => {
                let value = if foreach_item.is_some_and(|item| bind.root == *item) {
                    foreach_item_bind_value_expr(bind)
                } else {
                    bind_value_expr(bind)
                };
                quote! {
                    __mapper_cache_args.push(#root::CacheArg::try_new(#name, #value)?);
                }
            }
            BindKind::List => {
                let ident = &bind.root;
                quote! {
                    for __mapper_item in #ident.iter() {
                        __mapper_cache_args.push(#root::CacheArg::try_new(#name, __mapper_item)?);
                    }
                }
            }
        }
    });
    quote! { #(#steps)* }
}
///
/// # 参数
/// - `output`: 过程宏拼装后的代码输出 token 流。
fn result_inner(output: &ReturnType) -> syn::Result<&Type> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "Mapper 方法必须返回 anyhow::Result<T>",
        ));
    };
    first_generic_inner(ty, "Result")
        .ok_or_else(|| syn::Error::new_spanned(ty, "Mapper 方法必须返回 anyhow::Result<T>"))
}
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn first_generic_inner<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    let Type::Path(TypePath { path, .. }) = ty else {
        return None;
    };
    let segment = path.segments.last()?;
    if segment.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &segment.arguments
    else {
        return None;
    };
    match args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn is_type_ident(ty: &Type, name: &str) -> bool {
    let Type::Path(TypePath { path, .. }) = ty else {
        return false;
    };
    path.segments
        .last()
        .is_some_and(|segment| segment.ident == name)
}
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_scalar_type(ty: &Type) -> bool {
    let Type::Path(TypePath { path, .. }) = ty else {
        return false;
    };
    let Some(segment) = path.segments.last() else {
        return false;
    };
    matches!(
        segment.ident.to_string().as_str(),
        "bool"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "f32"
            | "f64"
            | "String"
    )
}
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_collection_type(ty: &Type) -> bool {
    match ty {
        Type::Reference(reference) => is_collection_type(&reference.elem),
        Type::Slice(_) | Type::Array(_) => true,
        Type::Path(TypePath { path, .. }) => path.segments.last().is_some_and(|segment| {
            matches!(segment.ident.to_string().as_str(), "Vec" | "VecDeque")
        }),
        _ => false,
    }
}
///
/// # 参数
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `end`: 结束位置或范围上界。
fn is_in_list_placeholder(sql: &str, start: usize, end: usize) -> bool {
    let before = &sql[..start];
    let before_trimmed = before.trim_end();
    if !before_trimmed.ends_with('(') {
        return false;
    }

    let before_paren = before_trimmed[..before_trimmed.len() - 1].trim_end();
    if !ends_with_keyword_ignore_ascii_case(before_paren, "IN") {
        return false;
    }

    sql[end..].trim_start().starts_with(')')
}
///
/// # 参数
/// - `value`: 已裁剪空白的 SQL 前缀文本。
/// - `keyword`: 业务 key 或 Redis key,用于定位数据。
fn ends_with_keyword_ignore_ascii_case(value: &str, keyword: &str) -> bool {
    let trimmed = value.trim_end();
    if trimmed.len() < keyword.len() {
        return false;
    }
    let start = trimmed.len() - keyword.len();
    if !trimmed[start..].eq_ignore_ascii_case(keyword) {
        return false;
    }
    trimmed[..start]
        .chars()
        .next_back()
        .is_none_or(|ch| !is_ident_continue(ch))
}
///
/// # 参数
/// - `ch`: 当前扫描到的字符。
fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}
///
/// # 参数
/// - `key`: `#[mapper(key = "...")]` 显式指定的 mapper 名称。
/// - `trait_ident`: 生成 mapper 实现时使用的 trait 标识符。
fn key_expr(key: Option<&str>, trait_ident: &Ident) -> TokenStream2 {
    if let Some(key) = key {
        let lit = LitStr::new(key, trait_ident.span());
        quote! { #lit }
    } else {
        quote! { concat!(module_path!(), "::", stringify!(#trait_ident)) }
    }
}
///
/// # 参数
/// - `values`: 待校验、写入或比较的值列表。
fn str_array_tokens(values: &[String]) -> TokenStream2 {
    let values = values
        .iter()
        .map(|value| LitStr::new(value, proc_macro2::Span::call_site()));
    quote! { &[#(#values),*] }
}
///
/// # 参数
/// - `value`: mapper 缓存 TTL 的毫秒数;`None` 表示使用缓存实现默认值。
fn option_u64_tokens(value: Option<u64>) -> TokenStream2 {
    match value {
        Some(value) => quote! { ::core::option::Option::Some(#value) },
        None => quote! { ::core::option::Option::None },
    }
}
///
/// # 参数
/// - `path`: `cache_codec` 或 `typed_cache_codec` 指向的工厂函数路径。
fn cache_codec_factory_tokens(path: Option<&Path>) -> TokenStream2 {
    match path {
        Some(path) => quote! { ::core::option::Option::Some(#path()) },
        None => quote! { ::core::option::Option::None },
    }
}
///
/// # 参数
/// - `expr`: Rust 表达式 AST,用于宏期分析。
fn parse_string_array(expr: Expr) -> syn::Result<Vec<String>> {
    let Expr::Array(ExprArray { elems, .. }) = expr else {
        return Err(syn::Error::new_spanned(expr, "期望字符串数组"));
    };
    elems
        .iter()
        .map(|elem| match elem {
            Expr::Lit(ExprLit {
                lit: Lit::Str(lit), ..
            }) => Ok(lit.value()),
            _ => Err(syn::Error::new_spanned(elem, "数组元素必须是字符串字面量")),
        })
        .collect()
}
///
/// # 参数
/// - `lit`: 过程宏正在读取的字符串字面量。
fn parse_lit_int(lit: LitInt) -> syn::Result<u64> {
    lit.base10_parse::<u64>()
}
///
/// # 参数
/// - `value`: `datasource` 属性中的数据源名称字面量。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_datasource_literal(value: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if value.trim().is_empty() || value.trim() != value {
        Err(syn::Error::new(
            span,
            "datasource 不能为空,且首尾不能包含空白",
        ))
    } else {
        Ok(())
    }
}
///
/// # 参数
/// - `value`: SQL 占位符中的参数路径,例如 `id` 或 `page.offset`。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn parse_placeholder_path(value: &str, span: proc_macro2::Span) -> syn::Result<Vec<Ident>> {
    let mut path = Vec::new();
    for segment in value.split('.') {
        if !is_rust_ident(segment) {
            return Err(syn::Error::new(
                span,
                format!("SQL 占位符 `#{{{value}}}` 不是合法字段路径"),
            ));
        }
        path.push(Ident::new(segment, span));
    }
    if path.is_empty() {
        return Err(syn::Error::new(span, "SQL 占位符不能为空"));
    }
    Ok(path)
}
///
/// # 参数
/// - `value`: `MapperOrderField` 属性中的列名或 `alias.column` 字面量。
/// - `span`: 源码位置,用于生成精确的编译期错误。
fn validate_order_column_literal(value: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if is_valid_order_column_literal(value) {
        Ok(())
    } else {
        Err(syn::Error::new(
            span,
            format!("MapperOrderField 字段 `{value}` 不合法,只允许 `column` 或 `alias.column`"),
        ))
    }
}
///
/// # 参数
/// - `value`: 需要校验的排序列名字面量。
fn is_valid_order_column_literal(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    if !is_rust_ident(first) {
        return false;
    }
    let mut count = 1;
    for part in parts {
        count += 1;
        if count > 2 || !is_rust_ident(part) {
            return false;
        }
    }
    true
}
///
/// # 参数
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn is_rust_ident(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
///
/// # 参数
/// - `attrs`: 属性列表,用于解析宏配置或 XML 属性。
fn has_async_trait_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "async_trait")
    })
}
