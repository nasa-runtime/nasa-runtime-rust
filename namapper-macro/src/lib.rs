//! SQL mapper 声明式过程宏。
//!
//! `#[Mapper]` 读取 trait 方法上的 SQL 注解，在编译期生成基于 `sqlx`、事务运行时和可选 L2 缓存的实现。
//! MySQL、PostgreSQL、SQLite 使用同名 SQL 方法属性，通过导入路径选择 typed adapter；
//! SQLite 的生成代码沿用受控 executor 和实际文件 owner，napp 按 datasource driver 管理生命周期。
//! 配置不转换 SQL 方言或 Rust 行类型，宏不创建数据库连接。
//! 同时生成静态方法身份与生命周期守卫，逻辑调用、真实数据库执行和 Stream 终态分别计量。
//! YAML 策略由宿主冻结，宏不创建指标出口或通知任务，也不要求参数实现额外的日志 trait。
mod sqlite;

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
/// 业务作用：展开 MySQL Mapper trait，绑定 SQL、事务与缓存执行合同。
///
/// 参数说明：
/// - `attr`：Mapper 属性中的数据源、缓存和生成选项 token。
/// - `item`：待展开的 Rust trait token。
///
/// 返回：成功返回实现 token；输入非法时返回编译错误 token，不在宏展开时连接数据库。
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Mapper(attr: TokenStream, item: TokenStream) -> TokenStream {
    match mapper_impl(attr.into(), item.into(), MapperBackend::MySql) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// 业务作用：解析并展开 PostgreSQL `#[Mapper]` trait，生成 `$n` 占位符、Pg 执行器和独立缓存身份。
///
/// 参数说明：
/// - `attr`: 属性宏括号内的 token stream。
/// - `item`: 被处理的 Mapper trait。
///
/// 返回：成功时生成指向 `namapper-pgsql` 或 `nasa::mapper::pgsql` 的实现；非法输入生成编译错误。
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn PgMapper(attr: TokenStream, item: TokenStream) -> TokenStream {
    match mapper_impl(attr.into(), item.into(), MapperBackend::PostgreSql) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
/// 业务作用：为排序字段枚举生成 MySQL 运行时的白名单列映射。
///
/// 参数说明：
/// - `item`：待派生的无字段排序枚举 token。
///
/// 返回：成功返回字段映射实现；枚举形态、列名或依赖不合法时生成编译错误。
#[proc_macro_derive(MapperOrderField, attributes(mapper_order_field))]
#[allow(non_snake_case)]
pub fn derive_mapper_order_field(item: TokenStream) -> TokenStream {
    match mapper_order_field_impl(item.into(), MapperBackend::MySql) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// 业务作用：为 PostgreSQL Mapper 生成白名单排序字段映射。
///
/// 参数说明：
/// - `item`: 被派生的字段 enum。
///
/// 返回：成功时实现 PostgreSQL runtime 的 `MapperOrderField`；非法 enum 生成编译错误。
#[proc_macro_derive(PgMapperOrderField, attributes(mapper_order_field))]
pub fn derive_pg_mapper_order_field(item: TokenStream) -> TokenStream {
    match mapper_order_field_impl(item.into(), MapperBackend::PostgreSql) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
/// 业务作用：为业务枚举生成按声明次序的数据库整数映射。
///
/// 参数说明：
/// - `item`：待派生的无字段枚举 token。
///
/// 返回：成功返回 ordinal 与反向映射实现；不支持的枚举或依赖生成编译错误，显式判别值不决定生成的序号。
#[proc_macro_derive(MapperEnum)]
#[allow(non_snake_case)]
pub fn derive_mapper_enum(item: TokenStream) -> TokenStream {
    match mapper_enum_impl(item.into(), MapperBackend::MySql) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// 业务作用：为 PostgreSQL Mapper 生成稳定 enum ordinal 转换。
///
/// 参数说明：
/// - `item`: 被派生的无字段 enum。
///
/// 返回：成功时实现 PostgreSQL runtime 的 `MapperEnum`；非法 enum 生成编译错误。
#[proc_macro_derive(PgMapperEnum)]
pub fn derive_pg_mapper_enum(item: TokenStream) -> TokenStream {
    match mapper_enum_impl(item.into(), MapperBackend::PostgreSql) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// 业务作用：为 SQLite Mapper 生成同名 SQL 注解的受控执行实现。
///
/// 参数说明：
/// - `attr`：数据源与缓存声明。
/// - `item`：业务 Mapper trait。
///
/// 返回：固定绑定 SQLite 的客户端实现或编译期错误。
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn SqliteMapper(attr: TokenStream, item: TokenStream) -> TokenStream {
    match mapper_impl(attr.into(), item.into(), MapperBackend::Sqlite) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// 业务作用：生成 SQLite 业务枚举的稳定序号映射。
///
/// 参数说明：
/// - `item`：无字段业务枚举。
///
/// 返回：SQLite MapperEnum 实现或编译期错误。
#[proc_macro_derive(SqliteMapperEnum)]
pub fn derive_sqlite_mapper_enum(item: TokenStream) -> TokenStream {
    match mapper_enum_impl(item.into(), MapperBackend::Sqlite) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// 业务作用：生成 SQLite 排序字段白名单，避免业务值成为 SQL 标识符。
///
/// 参数说明：
/// - `item`：排序字段枚举。
///
/// 返回：受控字段映射或编译期错误。
#[proc_macro_derive(SqliteMapperOrderField, attributes(mapper_order_field))]
pub fn derive_sqlite_mapper_order_field(item: TokenStream) -> TokenStream {
    match mapper_order_field_impl(item.into(), MapperBackend::Sqlite) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Mapper 宏生成代码所使用的数据库后端。
#[derive(Clone, Copy, PartialEq, Eq)]
enum MapperBackend {
    /// 保持既有 `?` 占位符和 `namapper` runtime。
    MySql,
    /// 使用 `$n` 占位符和 `namapper-pgsql` runtime。
    PostgreSql,
    /// SQLite 文件数据源与原生权限门禁。
    Sqlite,
}

impl MapperBackend {
    /// 业务作用：解析当前后端对应的运行时根路径，支持门面、Cargo 重命名和直接依赖。
    ///
    /// 参数说明：无。
    ///
    /// 返回：可写入宏展开代码的 runtime 路径；依赖缺失时返回清晰提示。
    fn runtime_root(self) -> Result<TokenStream2, String> {
        match self {
            Self::Sqlite => nasa_macro_support::runtime_root_nested(
                &["mapper", "sqlite"],
                "mapper-sqlite",
                "namapper-sqlite",
            ),
            Self::MySql => nasa_macro_support::runtime_root("mapper", "namapper"),
            Self::PostgreSql => nasa_macro_support::runtime_root_nested(
                &["mapper", "pgsql"],
                "mapper-pgsql",
                "namapper-pgsql",
            ),
        }
    }
}

macro_rules! sql_attr_stub {
    ($name:ident) => {
        /// 业务作用：拒绝脱离 `#[Mapper]` trait 单独使用的 SQL 方法注解，避免生成语义不完整的实现。
        ///
        /// 参数说明：
        /// - `_attr`：此独立入口不解析的 SQL 方法属性 token。
        /// - `item`：随编译错误一并保留的原 Rust item。
        ///
        /// 返回：输出 compile_error 与原 item，要求 SQL 方法属性由 Mapper trait 统一消费。
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
/// 业务作用：完成 Mapper 宏 `mapper_enum_impl` 的编译期转换步骤，并把失败定位到调用方源码。
///
/// 参数说明：
/// - `item`: 被宏处理的 Rust item token stream。
/// - `backend`: 派生实现所绑定的 Mapper 数据库后端。
///
/// 返回：后端对应的 ordinal 转换实现；输入不满足合同则返回编译期错误。
fn mapper_enum_impl(item: TokenStream2, backend: MapperBackend) -> syn::Result<TokenStream2> {
    let root = match backend.runtime_root() {
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
            /// 业务作用：返回 enum 变体在数据库中保存的稳定序号。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：从零开始的声明位置序号，不采用显式判别值；重排变体会改变持久映射。
            fn ordinal(self) -> i32 {
                match self {
                    #(#ordinal_arms,)*
                }
            }
            /// 业务作用：把持久整数序号映射回当前枚举变体。
            ///
            /// 参数说明：
            /// - `value`：按枚举声明位置保存的整数序号。
            ///
            /// 返回：命中生成的序号时返回对应变体，否则返回 None，不读取数据库。
            fn from_ordinal(value: i32) -> ::core::option::Option<Self> {
                match value {
                    #(#from_arms,)*
                    _ => ::core::option::Option::None,
                }
            }
        }
    })
}
/// 业务作用：完成 Mapper 宏 `mapper_order_field_impl` 的编译期转换步骤，并把失败定位到调用方源码。
///
/// 参数说明：
/// - `item`: 被宏处理的 Rust item token stream。
/// - `backend`: 派生实现所绑定的 Mapper 数据库后端。
///
/// 返回：后端对应的排序字段白名单实现；输入不满足合同则返回编译期错误。
fn mapper_order_field_impl(
    item: TokenStream2,
    backend: MapperBackend,
) -> syn::Result<TokenStream2> {
    let root = match backend.runtime_root() {
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
            /// 业务作用：把排序枚举变体映射为声明中的白名单列名。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：静态列名或别名限定列名；字符形态已在派生时检查，不证明目标数据库实际存在该列。
            fn mapper_order_field(self) -> &'static str {
                match self {
                    #(#arms,)*
                }
            }
        }
    })
}
/// 业务作用：从枚举变体属性读取排序列名，缺省时由变体名派生。
///
/// 参数说明：
/// - `variant`：需要读取 mapper_order_field 属性的枚举变体。
///
/// 返回：返回显式字符串或蛇形名称；重复属性或属性形态非法时返回错误。本函数不验证列名白名单字符，由调用方继续校验。
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
                    lit: Lit::Str(lit),
                    ..
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
/// 业务作用：将变体名称转换为默认 SQL 列名。
///
/// 参数说明：
/// - `value`：待转换的枚举变体名称。
///
/// 返回：每个 ASCII 大写字母转小写，除首字符外在前面加下划线；其它字符原样保留，连续大写字母分别分隔。
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
    /// 业务作用：按属性路径末段识别支持的 SQL 方法注解。
    ///
    /// 参数说明：
    /// - `attr`：待识别的 Rust 方法属性。
    ///
    /// 返回：末段为 Query、StreamQuery、Insert、Update、Delete 或 Execute 时返回对应类型，否则返回 None；不验证路径来源。
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

    /// 业务作用：区分普通查询与其它 SQL 方法种类。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅 Query 返回 true，StreamQuery 及写操作返回 false。
    fn is_query(self) -> bool {
        matches!(self, Self::Query)
    }

    /// 业务作用：标记无需默认刷新缓存的读取方法。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Query 和 StreamQuery 返回 true，其它方法返回 false。
    fn is_read(self) -> bool {
        matches!(self, Self::Query | Self::StreamQuery)
    }

    /// 业务作用：识别按行流式读取的 SQL 方法。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅 StreamQuery 返回 true。
    fn is_stream(self) -> bool {
        matches!(self, Self::StreamQuery)
    }

    /// 业务作用：确定 SQL 方法未显式指定时的缓存失效默认值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：读取方法返回 false，Insert、Update、Delete 和 Execute 返回 true，不执行实际失效。
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
    /// 业务作用：解析缓存失败时绕过或严格返回的策略。
    ///
    /// 参数说明：
    /// - `value`：cache_errors 声明的原始字符串。
    /// - `span`：产生编译错误时使用的调用方源码位置。
    ///
    /// 返回：精确字符串 bypass 或 strict 返回对应策略；其它值返回位于 span 的编译错误，不去空白或转换大小写。
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

    /// 业务作用：判断缓存失败是否应影响 Mapper 调用结果。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Strict 返回 true，Bypass 返回 false。
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
    /// 方法拒绝在 ambient 事务中调用——语句的副作用不允许随外层事务回滚。
    Never,
}

impl TxMode {
    /// 业务作用：解析 `tx` 属性值。
    ///
    /// 参数说明：
    /// - `value`: mapper 属性中的事务策略字符串。
    /// - `span`: 源码位置,用于生成精确的编译期错误。
    ///
    /// 返回：词表命中时返回对应事务模式；未知值返回带源码位置的编译错误。
    fn parse(value: &str, span: proc_macro2::Span) -> syn::Result<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "mandatory" => Ok(Self::Mandatory),
            "never" => Ok(Self::Never),
            other => Err(syn::Error::new(
                span,
                format!("tx 不支持 `{other}`,只能是 \"auto\" / \"mandatory\" / \"never\""),
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
    /// 业务作用：建立 Mapper trait 的属性缺省值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回启用普通缓存、禁用事务内缓存和严格参数检查、错误绕过且没有数据源、TTL、codec 或失效依赖的选项。
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
    blob_scalar: bool,
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
    backend: MapperBackend,
}

type ParsedSqlTemplate = (String, Vec<String>, Vec<BindInfo>, Option<DynamicSqlPlan>);

/// 业务作用：为静态元数据编码无歧义的 Rust 名称片段，保留大小写、下划线和 Unicode 的区别。
///
/// 参数说明：
/// - `ident`：是 trait 或方法的原始标识符。
///
/// 返回：逐 UTF-8 字节的定宽大写十六进制字符串，组合时不会因分隔符或大小写折叠产生碰撞。
fn metadata_ident_component(ident: &Ident) -> String {
    use std::fmt::Write;
    let spelling = ident.to_string();
    let mut encoded = String::with_capacity(spelling.len() * 2);
    for byte in spelling.bytes() {
        let _ = write!(encoded, "{byte:02X}");
    }
    encoded
}

/// 业务作用：完成 Mapper 宏 `mapper_impl` 的编译期转换步骤，并把失败定位到调用方源码。
///
/// 参数说明：
/// - `attr`: 属性宏括号内的 token stream。
/// - `item`: 被宏处理的 Rust item token stream。
/// - `backend`: 当前 Mapper 固定使用的数据库后端。
///
/// 返回：完整 Mapper trait、client、缓存元数据与方法实现；非法声明返回编译期错误。
fn mapper_impl(
    attr: TokenStream2,
    item: TokenStream2,
    backend: MapperBackend,
) -> syn::Result<TokenStream2> {
    let root = match backend.runtime_root() {
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
    let key_expr = key_expr(
        trait_args.key.as_deref(),
        &trait_ident,
        backend,
        trait_args.datasource.as_deref(),
    );
    let meta_ident = format_ident!(
        "__{}_MAPPER_CACHE_META",
        metadata_ident_component(&trait_ident)
    );

    let mut plans = Vec::new();
    let mut cleaned_items = Vec::new();
    let items = std::mem::take(&mut input.items);
    for item in items {
        match item {
            TraitItem::Fn(method) => {
                let plan = plan_method(method, &trait_args, backend)?;
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
    let clear_also = cache_reference_array_tokens(
        &trait_args.clear_also,
        backend,
        trait_args.datasource.as_deref(),
    );
    let clear_when = cache_reference_array_tokens(
        &trait_args.clear_when,
        backend,
        trait_args.datasource.as_deref(),
    );
    let trait_cache_codec = cache_codec_factory_tokens(trait_args.cache_codec.as_ref());

    let mut method_metadata = Vec::new();
    let mut impl_methods = Vec::new();
    for plan in &plans {
        let method = &plan.method.sig.ident;
        let method_meta = format_ident!(
            "__{}_{}_OBSERVATION",
            metadata_ident_component(&trait_ident),
            metadata_ident_component(method)
        );
        let registration = format_ident!("{}_REGISTRATION", method_meta);
        let datasource = plan.datasource.as_deref().unwrap_or("default");
        let driver = match plan.backend {
            MapperBackend::MySql => "mysql",
            MapperBackend::PostgreSql => "postgresql",
            MapperBackend::Sqlite => "sqlite",
        };
        let operation = match plan.kind {
            SqlKind::Query => "query",
            SqlKind::StreamQuery => "stream",
            SqlKind::Insert => "insert",
            SqlKind::Update => "update",
            SqlKind::Delete => "delete",
            SqlKind::Execute => "execute",
        };
        let tx_mode = match plan.tx {
            TxMode::Auto => "auto",
            TxMode::Mandatory => "mandatory",
            TxMode::Never => "never",
        };
        let sql = &plan.normalized_sql;
        method_metadata.push(quote! {
            static #method_meta: #root::observability::MapperMethodMeta = #root::observability::MapperMethodMeta::new(
                concat!(module_path!(), "::", stringify!(#trait_ident), "::", stringify!(#method)),
                concat!(module_path!(), "::", stringify!(#trait_ident)), #driver, #datasource, #operation, #tx_mode, #sql);
            #[#root::__private::linkme::distributed_slice(#root::observability::MAPPER_METHOD_META)]
            #[linkme(crate = #root::__private::linkme)]
            static #registration: &'static #root::observability::MapperMethodMeta = &#method_meta;
        });
        impl_methods.push(gen_method(&root, &key_expr, plan, &method_meta)?);
    }

    let (source_field, source_init, source_builder) = if backend == MapperBackend::Sqlite {
        let datasources = plans
            .iter()
            .map(|plan| plan.datasource.as_deref().unwrap_or("default"));
        (
            quote!(datasource: #root::SourceBinding,),
            quote!(datasource: #root::SourceBinding::default(),),
            quote! {
                /// 业务作用：将 SQLite client 固定到受管数据源身份，避免同名来源替换后静默改绑。
                ///
                /// 参数说明：
                /// - `source`：应用或独立调用方提供的受管 SQLite 来源。
                ///
                /// 返回：来源名在声明集合内且绑定槽接受该身份时返回 client；未声明或身份冲突时失败。
                pub fn with_datasource(self, source: #root::SqliteDataSource) -> #root::__private::anyhow::Result<Self> {
                    if ![#(#datasources),*].contains(&source.name()) {
                        return Err(#root::__private::anyhow::anyhow!("SQLite source name is not declared by this Mapper"));
                    }
                    self.datasource.bind(source)?;
                    Ok(self)
                }
            },
        )
    } else {
        (
            TokenStream2::new(),
            TokenStream2::new(),
            TokenStream2::new(),
        )
    };

    let expanded = quote! {
        #(#method_metadata)*
        #[#root::__private::async_trait::async_trait]
        #input

        /// 当前 mapper trait 的默认客户端实现。
        ///
        /// 生成的 client 持有可选 L2 缓存和 codec,业务通常通过 `new()` 创建后直接调用 mapper 方法。
        pub struct #client_ident {
            #source_field
            l2_cache: ::core::option::Option<::std::sync::Arc<dyn #root::MapperL2Cache>>,
            cache_codec: ::core::option::Option<::std::sync::Arc<dyn #root::MapperCacheCodec>>,
        }

        impl #client_ident {
            #source_builder
            /// 业务作用：构造 Mapper client，采用当前全局 L2 缓存及 trait 声明的 codec。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：带共享缓存与可选 codec 的 client；配置 codec 工厂时立即调用，SQLite 数据源仍需显式绑定。
            pub fn new() -> Self {
                Self {
                    #source_init
                    l2_cache: #root::default_l2_cache(),
                    cache_codec: #trait_cache_codec,
                }
            }
            /// 业务作用：为当前 client 替换实际执行查询缓存读写的 L2 适配器。
            ///
            /// 参数说明：
            /// - `cache`：共享的 L2 缓存实现。
            ///
            /// 返回：保存新适配器后的 client；不改变全局默认值，不执行缓存读写。
            pub fn with_l2_cache(
                mut self,
                cache: ::std::sync::Arc<dyn #root::MapperL2Cache>,
            ) -> Self {
                self.l2_cache = ::core::option::Option::Some(cache);
                self
            }
            /// 业务作用：为当前 client 设置普通缓存值编解码器。
            ///
            /// 参数说明：
            /// - `codec`：共享的普通缓存值编码与解码实现。
            ///
            /// 返回：保存新 codec 后的 client；方法显式指定的 codec 仍具有更高优先级。
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
/// 业务作用：读取 Mapper trait 层级的数据源、缓存和客户端生成选项。
///
/// 参数说明：
/// - `attr`：Mapper 属性括号内的 token。
///
/// 返回：空输入采用默认值；支持项按解析顺序覆盖，未知项、字面量类型或数据源标识非法时返回错误。客户端名由 Ident 构造，其非法名称可能展开。
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
/// 业务作用：完成 Mapper 宏 `plan_method` 的编译期转换步骤，并把失败定位到调用方源码。
///
/// 参数说明：
/// - `method`: trait 方法 AST 或 HTTP 方法。
/// - `trait_args`: rest client 或 mapper trait 级配置参数。
/// - `backend`: 当前 Mapper 固定使用的数据库后端。
///
/// 返回：已校验且绑定固定后端的方法生成计划；声明冲突时返回编译期错误。
fn plan_method(
    method: TraitItemFn,
    trait_args: &TraitArgs,
    backend: MapperBackend,
) -> syn::Result<MethodPlan> {
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
        parse_sql_template(&sql, &params, attr.span(), backend)?;
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
    if backend != MapperBackend::MySql
        && (cache || flush_cache)
        && method_args.datasource.is_some()
        && method_args.datasource != trait_args.datasource
    {
        return Err(syn::Error::new_spanned(
            attr,
            "该 Mapper 使用 L2 cache 时 datasource 必须声明在 trait 级，确保 driver/datasource 进入统一缓存身份",
        ));
    }
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
        backend,
    })
}
/// 业务作用：限制编译期 checked 查询只使用静态 SQL 和标量绑定。
///
/// 参数说明：
/// - `binds`：按 SQL 出现顺序排列的绑定描述。
/// - `dynamic`：存在动态 SQL 时携带的已解析节点计划。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：不存在动态计划且没有列表绑定时成功，否则返回编译错误；不验证 SQL 方言或返回行类型。
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
/// 业务作用：限制流式查询只使用静态 SQL 和标量绑定。
///
/// 参数说明：
/// - `binds`：按 SQL 出现顺序排列的绑定描述。
/// - `dynamic`：存在动态 SQL 时携带的已解析节点计划。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：不存在动态计划且没有列表绑定时成功，否则返回编译错误；不验证 SQL 方言或返回行类型。
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
/// 业务作用：读取单个 SQL 方法注解中的 SQL 与执行策略。
///
/// 参数说明：
/// - `attr`：Query、StreamQuery 或写操作的 Rust 属性。
///
/// 返回：支持单字符串、位置 SQL 加选项或命名选项；重复 SQL、未知选项、非法字面量或数据源返回错误。缺 SQL 的命名选项结果可返回，由上层最终检查。
fn parse_method_attr(attr: &Attribute) -> syn::Result<MethodArgs> {
    let mut args = MethodArgs::default();
    match &attr.meta {
        Meta::List(list) => {
            if let Ok(sql) = syn::parse2::<LitStr>(list.tokens.clone()) {
                args.sql = Some(sql.value());
                return Ok(args);
            }
            let normalize = |input: syn::parse::ParseStream<'_>| -> syn::Result<TokenStream2> {
                if input.peek(LitStr) {
                    let sql: LitStr = input.parse()?;
                    if input.is_empty() {
                        return Ok(quote!(sql = #sql));
                    }
                    input.parse::<syn::Token![,]>()?;
                    let options: TokenStream2 = input.parse()?;
                    Ok(quote!(sql = #sql, #options))
                } else {
                    input.parse()
                }
            };
            let tokens = Parser::parse2(normalize, list.tokens.clone())?;
            let parser = syn::meta::parser(|meta| {
                let key = meta
                    .path
                    .get_ident()
                    .map(|ident| ident.to_string())
                    .unwrap_or_default();
                match key.as_str() {
                    "sql" | "value" => {
                        if args.sql.is_some() {
                            return Err(meta.error("SQL 只能声明一次"));
                        }
                        args.sql = Some(meta.value()?.parse::<LitStr>()?.value());
                    }
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
            Parser::parse2(parser, tokens)?;
            Ok(args)
        }
        Meta::Path(_) => Err(syn::Error::new_spanned(attr, "SQL 注解必须带 SQL 字符串")),
        Meta::NameValue(nv) => Err(syn::Error::new(
            nv.span(),
            "SQL 注解不支持 name=value 顶层写法,请用 #[Query(\"...\")] 或 #[Query(sql=\"...\")]",
        )),
    }
}
/// 业务作用：提取 Mapper 方法可用于 SQL 绑定的简单参数。
///
/// 参数说明：
/// - `method`：待检查或生成实现的 Mapper trait 方法。
///
/// 返回：首项为接收者且其余为非 mut、非 ref 的标识模式时返回参数信息，否则报错；此处不复验接收者必须恰为 &self，也不做 Rust 类型检查。
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
            blob_scalar: false,
        });
    }
    Ok(params)
}
/// 业务作用：解析 Mapper DSL 的 `parse_sql_template` 阶段输入，并将非法语法定位为编译期错误。
///
/// 参数说明：
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
/// - `backend`: 决定裸 `?` 校验与 prepared bind 占位符语法的数据库后端。
///
/// 返回：规范 SQL、文本片段、bind 与可选动态节点；模板非法时返回编译期错误。
fn parse_sql_template(
    sql: &str,
    params: &[ParamInfo],
    span: proc_macro2::Span,
    backend: MapperBackend,
) -> syn::Result<ParsedSqlTemplate> {
    if backend == MapperBackend::Sqlite {
        return sqlite::parse_template(sql, params, span);
    }
    validate_sql_template_safety(sql, span, backend)?;

    if has_dynamic_sql_tag(sql, span)? {
        let dynamic = parse_dynamic_sql_template(sql, params, span)?;
        let mut binds = Vec::new();
        collect_node_binds(&dynamic.nodes, &mut binds);
        let normalized_sql = normalize_sql_whitespace(&render_nodes_for_signature(&dynamic.nodes));
        return Ok((normalized_sql, Vec::new(), binds, Some(dynamic)));
    }

    let (mut normalized_sql, fragments, binds) = parse_plain_sql_template(sql, params, span)?;
    if backend == MapperBackend::PostgreSql && !binds.iter().any(|bind| bind.kind == BindKind::List)
    {
        normalized_sql = render_postgres_static_sql(&fragments, &binds);
    }
    Ok((normalized_sql, fragments, binds, None))
}
/// 业务作用：校验 Mapper DSL 的 `validate_sql_template_safety` 约束，阻止不安全或歧义输入进入代码生成。
///
/// 参数说明：
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `span`: 源码位置,用于生成精确的编译期错误。
/// - `backend`: 当前 SQL 模板所属的数据库后端。
///
/// 返回：模板满足当前后端安全边界时成功，否则返回对应源码位置的错误。
fn validate_sql_template_safety(
    sql: &str,
    span: proc_macro2::Span,
    backend: MapperBackend,
) -> syn::Result<()> {
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
            b'?' if backend == MapperBackend::MySql => {
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

/// 业务作用：从普通 SQL 的 fragment/bind 结构生成 PostgreSQL 静态 `$n` SQL。
///
/// 参数说明：
/// - `fragments`: 每个业务 bind 前后的原始 SQL 文本。
/// - `binds`: 按源码顺序解析的标量 bind；调用方已排除列表 bind。
///
/// 返回：仅在 bind 节点写入 `$n` 的规范化 SQL，文本中的 `?` 原样保留。
fn render_postgres_static_sql(fragments: &[String], binds: &[BindInfo]) -> String {
    let mut sql = String::new();
    for (index, bind) in binds.iter().enumerate() {
        sql.push_str(&fragments[index]);
        debug_assert!(bind.kind == BindKind::Scalar);
        sql.push('$');
        sql.push_str(&(index + 1).to_string());
    }
    if let Some(fragment) = fragments.last() {
        sql.push_str(fragment);
    }
    normalize_sql_whitespace(&sql)
}
/// 业务作用：跳过 SQL 中的单引号片段，避免将其内容识别为标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回闭合引号之后的字节偏移，支持双写同引号和反斜线跳过下一字节；未闭合返回编译错误。调用方须保证 start 指向起始引号。
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
/// 业务作用：跳过 SQL 中的双引号片段，避免将其内容识别为标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回闭合引号之后的字节偏移，支持双写同引号和反斜线跳过下一字节；未闭合返回编译错误。调用方须保证 start 指向起始引号。
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
/// 业务作用：跳过 SQL 的反引号标识符片段。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回遇到的首个后续反引号之后的位置；未闭合返回错误，不处理双写或转义反引号。
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
/// 业务作用：跳过 SQL 块注释主体。
///
/// 参数说明：
/// - `bytes`：SQL 模板的原始字节。
/// - `idx`：已越过 /* 后的注释主体起始偏移。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：找到首个 */ 时返回其后字节位置，否则返回编译错误；不处理嵌套注释。
fn skip_block_comment(bytes: &[u8], mut idx: usize, span: proc_macro2::Span) -> syn::Result<usize> {
    while idx + 1 < bytes.len() {
        if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
            return Ok(idx + 2);
        }
        idx += 1;
    }
    Err(syn::Error::new(span, "SQL 块注释缺少闭合 */"))
}
/// 业务作用：识别动态模板不允许的绑定、引用和选择键标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
///
/// 返回：当前位置命中 bind、include、sql 或 selectKey 的开闭标签时返回 true，否则返回 false；调用方负责在 SQL 词法环境中调用。
fn starts_dangerous_xml_tag_at(sql: &str, pos: usize) -> bool {
    ["bind", "include", "sql", "selectKey"]
        .into_iter()
        .any(|tag| {
            starts_open_tag_at(sql, pos, tag) || starts_close_tag_at(sql, pos, tag).is_some()
        })
}
/// 业务作用：将普通 SQL 的命名占位符映射为有序绑定与问号模板。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回规范空白后的 SQL、原文本片段及绑定描述；未闭合、未知参数、非法路径或集合上下文不匹配时报错。直接查找所有 #{，自身不跳过字符串或注释。
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
                if is_collection && !is_list_context && !param.blob_scalar {
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
/// 业务作用：在 SQL 词法片段之外探测受支持的动态标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：找到已知开闭标签返回 true，没有则 false；未闭合引号或块注释返回错误。此处不跳过行注释，也不解析节点层次。
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
/// 业务作用：完整解析动态 SQL 为节点计划。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：全部输入消费且没有遗留关闭标签时返回计划；标签不匹配、节点或绑定非法时返回编译错误，不验证数据库 SQL 的可执行性。
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
    Ok(DynamicSqlPlan {
        nodes,
    })
}
/// 业务作用：按嵌套层次读取动态 SQL 文本、绑定、条件、裁剪与循环节点。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `stop_tags`：允许结束当前递归层的关闭标签名称。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `local_items`：当前可引用的外层 foreach 局部元素标识。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：遇允许的关闭标签时消费标签并返回节点、下一偏移和标签名；输入结束时标签名为 None。未知层次、绑定或属性非法时报错，不设置递归深度上限。
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
            nodes.push(SqlNode::If {
                test,
                body,
            });
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
            nodes.push(SqlNode::OrderBy {
                value,
            });
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
/// 业务作用：解析按声明顺序选择的 when 分支与可选 otherwise。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `local_items`：当前可引用的外层 foreach 局部元素标识。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：至少一个 when 且闭合时返回 Choose 节点与后续位置；otherwise 重复或其后仍有 when、其它子内容或缺关闭标签时失败。
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
            return Ok((
                SqlNode::Choose {
                    whens,
                    otherwise,
                },
                close_end,
            ));
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
            whens.push(WhenNode {
                test,
                body,
            });
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
/// 业务作用：验证循环集合与局部元素名称并解析动态 SQL 循环主体。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `body_start`：开始解析标签主体的有效字节偏移。
/// - `attrs`：已经读取的 XML 风格属性名和值序列。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `local_items`：当前可引用的外层 foreach 局部元素标识。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：集合为顶层集合参数、元素名不遮蔽现有参数或外层元素、分隔片段合法且主体引用元素时返回节点；缺失属性、名称或主体非法时失败。
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
/// 业务作用：为动态 SQL 确定单值或显式列表绑定，沿用适配层确认的 BLOB 单值语义。
///
/// 参数说明：
/// - `sql`: SQL 模板文本,用于解析占位符或动态节点。
/// - `start`: 起始位置或范围下界。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
/// - `span`: 源码位置,用于生成精确的编译期错误。
///
/// 返回：绑定计划与占位符终点；非字节集合误作单值或非法字段路径在编译期拒绝。
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
    // 静态与动态路径共用适配层的字节标记；显式 IN 仍按列表展开，普通集合不获得单值绑定能力。
    if is_collection && !is_list_context && !param.blob_scalar {
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
/// 业务作用：把动态 SQL 条件转为 Rust 表达式，支持 Option 空值比较。
///
/// 参数说明：
/// - `value`：test 属性中的条件原文。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：非空合法条件返回 Expr；四种边缘 null 比较转为 is_some/is_none，其余在重写 and/or 后交给 syn。语法或路径非法时报错，不证明表达式无副作用。
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
/// 业务作用：在保留字符串和字符片段时改写模板逻辑连接词。
///
/// 参数说明：
/// - `value`：动态条件表达式文本。
///
/// 返回：返回将指定空白或括号边界的 and/or 转为 &&/|| 的文本；其它内容保留，不验证表达式，未闭合字面量留给后续 syn 报错。
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
/// 业务作用：识别原始字符串片段的结束位置，避免改写其内部连接词。
///
/// 参数说明：
/// - `value`：完整条件表达式文本。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
///
/// 返回：当前位置不是可识别的 r 或 br 原始字符串起始时返回 None；识别后返回闭合位置，未闭合时返回输入长度，不验证前方标识符边界。
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
/// 业务作用：跳过条件表达式中的带引号片段。
///
/// 参数说明：
/// - `value`：条件表达式原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `quote`：起始和结束使用的单字节引号。
///
/// 返回：返回匹配引号后的字节位置，反斜线跳过后一字节；未闭合返回输入末尾，不判断 Rust 生命周期或完整字面量是否合法。
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
/// 业务作用：按模板连接词边界判断当前位置是否可改写。
///
/// 参数说明：
/// - `value`：条件表达式原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `word`：待匹配的 ASCII 逻辑词 and 或 or。
///
/// 返回：精确匹配 word 且前方为开始、指定空白或左括号，后方为末尾、指定空白或括号时返回 true；调用方须保证相关切片位置有效。
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
/// 业务作用：限制 null 比较左值为点分隔的标识路径。
///
/// 参数说明：
/// - `path`：准备转换成 Option 判断的字段路径。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：所有分段满足本层标识字符规则时成功，否则报错；不验证字段是否存在或完整 Rust 关键字语义。
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
/// 业务作用：读取已识别开标签的属性与主体起始位置。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回属性序列及闭合 > 后的位置；引号、属性或标签结尾非法时返回错误。调用方负责先确认标签名与起始位置。
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
/// 业务作用：读取必须自闭合的动态 SQL 标签属性。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：属性片段以 / 结束时返回属性和 > 后的位置；非自闭合、属性或结尾非法时失败。
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
/// 业务作用：寻找引号片段之外的 XML 风格标签结束符。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回首个未被引号包裹的 > 偏移；未闭合引号或找不到结尾返回错误，不验证开标签名称。
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
/// 业务作用：解析动态标签中带引号的键值属性。
///
/// 参数说明：
/// - `input`：去除标签名与结束符后的属性文本。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回按出现顺序保留的属性对，包括重复名称；非法名称、等号或引号返回错误，不处理 XML 实体与反斜线转义。
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
/// 业务作用：读取动态标签必须声明的属性。
///
/// 参数说明：
/// - `attrs`：已经读取的 XML 风格属性名和值序列。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `name`：需要查找的属性名称。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回首个同名属性的原值，包括空字符串；缺失时返回定位到标签的编译错误。
fn required_attr(
    attrs: &[(String, String)],
    tag: &str,
    name: &str,
    span: proc_macro2::Span,
) -> syn::Result<String> {
    optional_attr(attrs, name)
        .ok_or_else(|| syn::Error::new(span, format!("<{tag}> 缺少 {name} 属性")))
}
/// 业务作用：读取动态标签可省略的属性。
///
/// 参数说明：
/// - `attrs`：已经读取的 XML 风格属性名和值序列。
/// - `name`：需要查找的属性名称。
///
/// 返回：返回首个同名属性的值副本，缺失时返回 None，不检查重复或空值。
fn optional_attr(attrs: &[(String, String)], name: &str) -> Option<String> {
    attrs
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}
/// 业务作用：拒绝动态标签的原始 SQL 片段夹带值占位符、注释或标签。
///
/// 参数说明：
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `attr`：用于错误定位的属性名。
/// - `value`：编译期固定 SQL 前后缀或分隔符文本。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：包含 ${、#{、?、#、--、/* 或 < 时失败，其余成功；这是受控模板片段限制，不是通用 SQL 安全解析。
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
/// 业务作用：为不接受属性的动态标签拒绝额外声明。
///
/// 参数说明：
/// - `attrs`：已经读取的 XML 风格属性名和值序列。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：属性列表为空时成功，否则按首个属性返回错误。
fn reject_attrs(attrs: &[(String, String)], tag: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if let Some((key, _)) = attrs.first() {
        return Err(syn::Error::new(span, format!("<{tag}> 不支持属性 `{key}`")));
    }
    Ok(())
}
/// 业务作用：限制动态标签仅使用明确允许的属性。
///
/// 参数说明：
/// - `attrs`：已经读取的 XML 风格属性名和值序列。
/// - `tag`：用于识别标签或定位错误的标签名称。
/// - `allowed`：当前标签明确支持的属性名列表。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：每项名称均在 allowed 中时成功，遇首个未知项报错；不检查重复属性或属性值语义。
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
/// 业务作用：读取 trim 节点的前后缀移除候选。
///
/// 参数说明：
/// - `value`：prefixOverrides 或 suffixOverrides 的属性原文。
///
/// 返回：按竖线切分、去除各项首尾空白并丢弃空项，保留剩余项原次序，不校验其 SQL 语义。
fn parse_overrides(value: &str) -> Vec<String> {
    value
        .split('|')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
/// 业务作用：确认递归节点返回了当前开标签所要求的关闭标签。
///
/// 参数说明：
/// - `close_tag`：递归解析返回的可选关闭标签名。
/// - `expected`：当前层期望的关闭标签名。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：名称精确一致时成功，否则返回缺少匹配关闭标签的编译错误。
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
/// 业务作用：定位 SQL 文本之后的下一个绑定或动态标签起点。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：跳过引号片段和块注释，返回 #{ 或已知开闭标签的位置，未找到返回输入长度；词法片段未闭合返回错误，不跳过行注释。
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
/// 业务作用：从支持列表识别当前动态 SQL 开标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
///
/// 返回：当前起点命中已知标签及名称边界时返回静态名称，否则返回 None，不解析属性和主体。
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
/// 业务作用：从支持列表识别当前动态 SQL 关闭标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
///
/// 返回：当前位置精确命中已知 </tag> 时返回静态名称，否则返回 None，不容许关闭标签内增加空白。
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
/// 业务作用：检查开标签名称与其后分隔边界。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `tag`：用于识别标签或定位错误的标签名称。
///
/// 返回：位置为有效字符边界、命中 <tag 且后续为 >、/ 或 ASCII 空白时返回 true，否则 false；不确认整个标签闭合。
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
/// 业务作用：识别一个精确的动态 SQL 关闭标签。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
/// - `tag`：用于识别标签或定位错误的标签名称。
///
/// 返回：有效字符边界上精确匹配 </tag> 时返回其后位置，否则返回 None。
fn starts_close_tag_at(sql: &str, pos: usize, tag: &str) -> Option<usize> {
    if !sql.is_char_boundary(pos) {
        return None;
    }
    let close = format!("</{tag}>");
    sql[pos..].starts_with(&close).then_some(pos + close.len())
}
/// 业务作用：跳过动态标签之间不参与节点选择的 ASCII 空白。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `pos`：当前 SQL 模板中的有效 UTF-8 字节边界位置。
///
/// 返回：返回首个非 ASCII 空白的字节偏移或输入末尾，不跳过其它 Unicode 空白。
fn skip_ascii_whitespace(sql: &str, mut pos: usize) -> usize {
    while pos < sql.len() && sql.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}
/// 业务作用：按节点结构收集全部可能的 SQL 绑定，用于编译期合同检查。
///
/// 参数说明：
/// - `nodes`：需要递归检查或生成签名的动态 SQL 节点序列。
/// - `out`：接收绑定描述的可追加输出序列。
///
/// 返回：将各分支及循环主体的绑定副本依次追加到 out；不求值条件、不去重，排序节点不产生绑定。
fn collect_node_binds(nodes: &[SqlNode], out: &mut Vec<BindInfo>) {
    for node in nodes {
        match node {
            SqlNode::Text(_) => {}
            SqlNode::Bind(bind) => out.push(bind.clone()),
            SqlNode::If {
                body,
                ..
            } => collect_node_binds(body, out),
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
                for when in whens {
                    collect_node_binds(&when.body, out);
                }
                collect_node_binds(otherwise, out);
            }
            SqlNode::Foreach {
                body,
                ..
            }
            | SqlNode::Trim {
                body,
                ..
            } => {
                collect_node_binds(body, out);
            }
            SqlNode::OrderBy {
                ..
            } => {}
        }
    }
}
/// 业务作用：检查循环主体是否实际包含以指定元素为根的绑定。
///
/// 参数说明：
/// - `nodes`：需要递归检查或生成签名的动态 SQL 节点序列。
/// - `ident`：需要与方法参数或局部绑定比较的 Rust 标识。
///
/// 返回：任意嵌套绑定的根标识匹配时返回 true；仅在条件或排序节点中出现不算绑定。
fn nodes_contain_bind_root(nodes: &[SqlNode], ident: &Ident) -> bool {
    nodes.iter().any(|node| match node {
        SqlNode::Text(_) => false,
        SqlNode::Bind(bind) => bind.root == *ident,
        SqlNode::If {
            body,
            ..
        } => nodes_contain_bind_root(body, ident),
        SqlNode::Choose {
            whens,
            otherwise,
        } => {
            whens
                .iter()
                .any(|when| nodes_contain_bind_root(&when.body, ident))
                || nodes_contain_bind_root(otherwise, ident)
        }
        SqlNode::Foreach {
            body,
            ..
        }
        | SqlNode::Trim {
            body,
            ..
        } => nodes_contain_bind_root(body, ident),
        SqlNode::OrderBy {
            ..
        } => false,
    })
}
/// 业务作用：生成用于 Mapper 方法静态身份的 SQL 结构文本。
///
/// 参数说明：
/// - `nodes`：需要递归检查或生成签名的动态 SQL 节点序列。
///
/// 返回：返回不求值条件的结构串，合并全部选择分支、每个绑定写问号、foreach 保留一次主体，不是直接执行的最终 SQL。
fn render_nodes_for_signature(nodes: &[SqlNode]) -> String {
    let mut sql = String::new();
    for node in nodes {
        match node {
            SqlNode::Text(text) => sql.push_str(text),
            SqlNode::Bind(bind) => match bind.kind {
                BindKind::Scalar => sql.push('?'),
                BindKind::List => sql.push('?'),
            },
            SqlNode::If {
                body,
                ..
            } => sql.push_str(&render_nodes_for_signature(body)),
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
                for when in whens {
                    sql.push_str(&render_nodes_for_signature(&when.body));
                }
                sql.push_str(&render_nodes_for_signature(otherwise));
            }
            SqlNode::Foreach {
                open,
                close,
                body,
                ..
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
            SqlNode::OrderBy {
                ..
            } => sql.push_str(" ORDER BY <order_by>"),
        }
    }
    sql
}
/// 业务作用：规范 SQL 结构空白，保留引号与块注释内部内容。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
///
/// 返回：返回合并 ASCII 空白并移除逗号及括号附近部分空格的文本；不验证片段闭合，不识别行注释或 PostgreSQL 美元引号。
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
/// 业务作用：读取显式缓存键后缀中的字段路径占位符。
///
/// 参数说明：
/// - `suffix`：包含 {字段路径} 的缓存键后缀模板。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：返回按出现顺序排列的原名称与 Ident 路径；缺少 }、路径非法或最后剩余文本含 } 时报错，不对其它位置作通用括号配平。
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
/// 业务作用：限制显式缓存后缀只引用 SQL 已绑定的标量路径。
///
/// 参数说明：
/// - `suffix`：待验证的显式缓存键后缀模板。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `binds`：按 SQL 出现顺序排列的绑定描述。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：每个占位符根参数存在、不是集合且同名 SQL 绑定为标量时成功；否则返回编译错误，不读取参数实际值。
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
/// 业务作用：拒绝声明后未在 SQL、动态条件或缓存后缀中使用的方法参数。
///
/// 参数说明：
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `binds`：按 SQL 出现顺序排列的绑定描述。
/// - `dynamic`：存在动态 SQL 时携带的已解析节点计划。
/// - `hash_key_suffix`：可选的显式缓存键后缀模板。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：收集到全部显式参数时成功，否则返回包含未用参数名的编译错误；表达式遍历是本层支持语法的静态分析，不执行条件。
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
/// 业务作用：把已识别的引用根登记为方法参数使用事实。
///
/// 参数说明：
/// - `ident`：需要与方法参数或局部绑定比较的 Rust 标识。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
///
/// 返回：标识与某个方法参数相同时加入 used，否则不改变集合。
fn mark_used_param(
    ident: &Ident,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
) {
    if params.iter().any(|param| param.ident == *ident) {
        used.insert(ident.to_string());
    }
}
/// 业务作用：遍历动态 SQL 的绑定、条件、循环集合与排序引用。
///
/// 参数说明：
/// - `nodes`：需要递归检查或生成签名的动态 SQL 节点序列。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
///
/// 返回：将可识别的显式参数引用原位合并进 used，不求值条件、不生成 SQL。
fn collect_dynamic_used_params(
    nodes: &[SqlNode],
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
) {
    for node in nodes {
        match node {
            SqlNode::Text(_) => {}
            SqlNode::Bind(bind) => mark_used_param(&bind.root, params, used),
            SqlNode::If {
                test,
                body,
            } => {
                collect_expr_used_params(test, params, used);
                collect_dynamic_used_params(body, params, used);
            }
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
                for when in whens {
                    collect_expr_used_params(&when.test, params, used);
                    collect_dynamic_used_params(&when.body, params, used);
                }
                collect_dynamic_used_params(otherwise, params, used);
            }
            SqlNode::Foreach {
                collection,
                body,
                ..
            } => {
                mark_used_param(collection, params, used);
                collect_dynamic_used_params(body, params, used);
            }
            SqlNode::Trim {
                body,
                ..
            } => collect_dynamic_used_params(body, params, used),
            SqlNode::OrderBy {
                value,
            } => mark_used_param(value, params, used),
        }
    }
}
/// 业务作用：从动态条件的最外层启动参数引用收集。
///
/// 参数说明：
/// - `expr`：需要遍历的动态条件 Rust 表达式。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
///
/// 返回：使用空局部遮蔽集遍历表达式，将识别到的参数名加入 used；不执行表达式。
fn collect_expr_used_params(
    expr: &Expr,
    params: &[ParamInfo],
    used: &mut ::std::collections::HashSet<String>,
) {
    let mut shadowed = ::std::collections::HashSet::new();
    collect_expr_used_params_inner(expr, params, used, &mut shadowed);
}
/// 业务作用：遍历受支持的 Rust 表达式并区分方法参数与局部同名绑定。
///
/// 参数说明：
/// - `expr`：需要遍历的动态条件 Rust 表达式。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
/// - `shadowed`：当前作用域内已遮蔽方法参数同名引用的局部名称集合。
///
/// 返回：将未遮蔽引用加入 used，闭包、匹配分支和循环使用局部遮蔽副本；未匹配的表达式类型跳过。宏仅扫描 token 标识，不能完整推断宏展开与所有 Rust 作用域语义。
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
/// 业务作用：避免把已经由局部绑定接管的名字算作方法参数使用。
///
/// 参数说明：
/// - `ident`：需要与方法参数或局部绑定比较的 Rust 标识。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
/// - `shadowed`：当前作用域内已遮蔽方法参数同名引用的局部名称集合。
///
/// 返回：名称未在 shadowed 中时委托登记，已遮蔽时不改变 used。
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
/// 业务作用：按语句顺序收集代码块引用并限制局部名称的遮蔽范围。
///
/// 参数说明：
/// - `block`：待遍历的条件表达式代码块。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
/// - `shadowed`：当前作用域内已遮蔽方法参数同名引用的局部名称集合。
///
/// 返回：克隆进入块时的遮蔽集，顺序处理局部声明、表达式和宏；忽略 item，块内新增遮蔽不写回外层，但参数使用事实合并到 used。
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
/// 业务作用：先检查局部声明的初始化引用，再登记其绑定名称。
///
/// 参数说明：
/// - `local`：待分析的 let 局部声明。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
/// - `shadowed`：当前作用域内已遮蔽方法参数同名引用的局部名称集合。
///
/// 返回：收集初始化及 let-else 分歧表达式的参数引用，然后更新 shadowed；不求值声明。
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
/// 业务作用：收集模式绑定的局部名称，供后续参数引用排除。
///
/// 参数说明：
/// - `pat`：闭包、分支、循环或局部声明中的 Rust 模式。
/// - `shadowed`：当前作用域内已遮蔽方法参数同名引用的局部名称集合。
///
/// 返回：递归将支持模式中的 Ident 名称加入 shadowed，忽略通配及其它未处理模式，不检查这些名字是否属于方法参数。
fn collect_pat_shadowed_params(pat: &Pat, shadowed: &mut ::std::collections::HashSet<String>) {
    match pat {
        Pat::Ident(PatIdent {
            ident,
            subpat,
            ..
        }) => {
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
/// 业务作用：保守检查宏 token 中可见的未遮蔽参数名。
///
/// 参数说明：
/// - `tokens`：动态条件内宏调用的参数 token。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `used`：原位累积已引用方法参数名的集合。
/// - `shadowed`：当前作用域内已遮蔽方法参数同名引用的局部名称集合。
///
/// 返回：递归扫描 token 组中的 Ident 并合并使用事实，忽略字面量与标点；不展开宏，不区分宏内新绑定和引用。
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
/// 业务作用：在原方法体外建立唯一逻辑调用边界，收口缓存提前返回、执行前拒绝及取消。
///
/// 参数说明：
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `key_expr`: 业务 key 或 Redis key,用于定位数据。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `metadata`: 当前方法的静态观测单元。
///
/// 返回：保留原签名与返回值的实现；无法生成合法 Rust 方法时给出宏期错误。
fn gen_method(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    plan: &MethodPlan,
    metadata: &Ident,
) -> syn::Result<TokenStream2> {
    let generated = if plan.kind.is_stream() {
        gen_stream_method(root, plan)
    } else if plan.kind.is_query() {
        gen_query_method(root, key_expr, plan)
    } else {
        gen_write_method(root, key_expr, plan)
    }?;
    let mut method: syn::ImplItemFn = syn::parse2(generated)?;
    let body = &method.block;
    let entry = if plan.backend == MapperBackend::Sqlite {
        let array_bindings = sqlite::array_bindings(&plan.params);
        let name = plan.datasource.as_deref().unwrap_or("default");
        let mode = match plan.tx {
            TxMode::Auto => "auto",
            TxMode::Mandatory => "mandatory",
            TxMode::Never => "never",
        };
        quote! {
            #array_bindings
            let __mapper_source = self.datasource.resolve(#name)?;
            #root::check_mapper_source(&__mapper_source, #mode)?;
            let __mapper_l2_cache = #root::scoped_cache(self.l2_cache.clone(), &__mapper_source);
        }
    } else {
        quote!(let __mapper_l2_cache = self.l2_cache.clone();)
    };
    method.block = syn::parse_quote!({
        let __mapper_meta = &#metadata;
        let mut __mapper_guard = #root::observability::MapperCallGuard::start(__mapper_meta);
        let __mapper_call = &__mapper_guard;
        // 内层提前返回只结束本次逻辑调用；外层仍统一结算缓存、连接失败和数据库结果。
        let __mapper_outcome = async { #entry #body }.await;
        __mapper_guard.finish(#root::classify_method_result(&__mapper_outcome));
        __mapper_outcome
    });
    Ok(quote! { #method })
}
/// 业务作用：生成查询方法的数据库、缓存加载及失效编排。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `key_expr`：生成代码中 Mapper 缓存命名空间的表达式 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
///
/// 返回：成功返回方法实现；返回类型或绑定不符合生成条件时报错。缓存成功会提前返回，普通数据库尾路径才执行其后的失效片段；宏展开阶段不执行查询。
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
    let conn = conn_tokens(root, plan.tx, plan.datasource.as_deref(), plan.backend);
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
            let __mapper_has_l2_cache = __mapper_l2_cache.is_some();
            let __mapper_in_transaction = #root::in_transaction();
            let __mapper_cache_enabled = __mapper_has_l2_cache
                && (!__mapper_in_transaction || #cache_in_tx);
            if __mapper_cache_enabled {
                __mapper_call.path(#root::observability::CallPath::Cache);
            }

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
/// 业务作用：生成持有受控连接的 Mapper 行流，并在 SQLite 流交付前登记来源消费预算。
///
/// 参数说明：
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
///
/// 返回：成功时返回持有连接到流结束的方法实现 token；签名不符合流式合同时返回编译错误。
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
    let conn = conn_tokens(root, TxMode::Auto, plan.datasource.as_deref(), plan.backend);

    let executor = sqlite::executor_tokens(plan, false);
    let (stream_budget, stream_result) = if matches!(plan.backend, MapperBackend::Sqlite) {
        (
            quote! { let __mapper_stream_budget = __mapper_conn.stream_budget(); },
            quote! { #root::MapperStream::observed_with_budget(__mapper_stream, __mapper_stream_guard, __mapper_stream_budget) },
        )
    } else {
        (
            quote! {},
            quote! { #root::MapperStream::observed(__mapper_stream, __mapper_stream_guard) },
        )
    };
    Ok(quote! {
        #sig {
            #sql_init
            // 流统一持有普通 SQL 入口返回的拥有型连接：事务内是连接槽锁，事务外是池连接。
            // 两条路径因此都执行 datasource 与数据库 driver 门禁，不会在另一种 driver 的
            // ambient transaction 中绕过检查而静默退化为池读取。连接持有到流消费完或丢弃；
            // 事务内流存活期间同一事务不得发出其它语句，未释放就返回事务体会在提交门禁显式失败。
            let mut __mapper_conn = #conn;
            #stream_budget
            let __mapper_query = #query;
            let __mapper_stream_guard = #root::observability::MapperStreamGuard::new(__mapper_meta, __mapper_normalized_sql.as_ref());
            let __mapper_stream = #root::__private::async_stream::try_stream! {
                let mut __mapper_rows = __mapper_query.fetch(#executor);
                while let ::core::option::Option::Some(__mapper_row) =
                    #root::__private::futures_util::TryStreamExt::try_next(&mut __mapper_rows).await?
                {
                    yield __mapper_row;
                }
            };
            ::core::result::Result::Ok(#stream_result)
        }
    })
}
/// 业务作用：生成写方法的受控连接、执行计量和成功后缓存失效流程。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `key_expr`：生成代码中 Mapper 缓存命名空间的表达式 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
///
/// 返回：返回写方法 token；按成功返回类型选择空值、影响行数或 sqlx 执行结果，绑定不合法时报错，其余类型兼容性留给 Rust 编译检查。
fn gen_write_method(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    plan: &MethodPlan,
) -> syn::Result<TokenStream2> {
    let sig = &plan.method.sig;
    let result_ty = result_inner(&sig.output)?;
    let sql_init = sql_init_tokens(root, plan, sig.ident.span());
    let query = write_query_tokens(root, plan)?;
    let executor = sqlite::executor_tokens(plan, true);
    let execute = quote! {
        {
            __mapper_call.path(#root::observability::CallPath::Database);
            let __mapper_prepared_query = #query;
            let mut __mapper_db_guard = #root::observability::MapperDbGuard::start(__mapper_meta);
            let __mapper_db_result = __mapper_prepared_query.execute(#executor).await;
            let __mapper_rows = __mapper_db_result.as_ref().map(|value| value.rows_affected()).unwrap_or(0);
            let __mapper_code = __mapper_meta.database_code_enabled().then(|| __mapper_db_result.as_ref().err().and_then(|error| error.as_database_error()).and_then(|error| error.code())).flatten();
            __mapper_db_guard.finish(#root::classify_sqlx_result(&__mapper_db_result), __mapper_rows, __mapper_normalized_sql.as_ref(), __mapper_code.as_deref());
            __mapper_db_result?
        }
    };
    let conn = conn_tokens(root, plan.tx, plan.datasource.as_deref(), plan.backend);
    let clear = clear_tokens(root, key_expr, plan.flush_cache, plan.flush_refs);

    if is_unit(result_ty) {
        Ok(quote! {
            #sig {
                #sql_init
                {
                    let mut __mapper_conn = #conn;
                    #execute;
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
                    #execute.rows_affected()
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
                    #execute
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
/// 业务作用：根据显式 fetch 声明或返回类型推导查询取数模式。
///
/// 参数说明：
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
/// - `result_ty`：Result 成功分支的返回类型。
///
/// 返回：返回借用目标行类型的模式；Vec、Option、单行及标量声明不匹配时失败。按语法末段识别容器和常见标量，不解析类型别名。
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
/// 业务作用：限制单行或标量查询不得使用集合或可选容器返回形态。
///
/// 参数说明：
/// - `ty`：需要按语法形态识别的 Rust 类型。
/// - `msg`：不符合单行模式时展示的编译错误说明。
///
/// 返回：末段是带类型参数的 Vec 或 Option 时返回指定错误，其它形态成功，不做完整类型解析。
fn reject_container_fetch_ty(ty: &Type, msg: &str) -> syn::Result<()> {
    if first_generic_inner(ty, "Vec").is_some() || first_generic_inner(ty, "Option").is_some() {
        return Err(syn::Error::new_spanned(ty, msg));
    }
    Ok(())
}
/// 业务作用：只围绕真实 SQLx future 记录数据库时长、原始错误类别与准确返回行数。
///
/// 参数说明：
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `fetch`: all、optional、one 或 scalar 的取数形态。
///
/// 返回：尚未 poll 的执行表达式；参数或返回类型不满足 Mapper 合同时产生宏期错误。
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
    let executor = sqlite::executor_tokens(plan, plan.flush_cache);
    let failure = if plan.backend == MapperBackend::Sqlite && plan.flush_cache {
        quote!(if __mapper_db_result.is_err() {
            __mapper_conn.mark_write_failed();
        })
    } else {
        TokenStream2::new()
    };
    let execution = match fetch {
        FetchMode::All(_) => quote! { __mapper_prepared_query.fetch_all(#executor) },
        FetchMode::Optional(_) => {
            quote! { __mapper_prepared_query.fetch_optional(#executor) }
        }
        FetchMode::One(_) | FetchMode::Scalar(_) => {
            quote! { __mapper_prepared_query.fetch_one(#executor) }
        }
    };
    let rows = match fetch {
        FetchMode::All(_) => quote! { __mapper_value.len() as u64 },
        FetchMode::Optional(_) => quote! { u64::from(__mapper_value.is_some()) },
        FetchMode::One(_) | FetchMode::Scalar(_) => quote! { 1u64 },
    };
    Ok(quote! { async {
        __mapper_call.path(#root::observability::CallPath::Database);
        let __mapper_prepared_query = #query;
        let mut __mapper_db_guard = #root::observability::MapperDbGuard::start(__mapper_meta);
        let __mapper_db_result = #execution.await;
        #failure
        let __mapper_rows = __mapper_db_result.as_ref().map(|__mapper_value| #rows).unwrap_or(0);
        let __mapper_code = __mapper_meta.database_code_enabled().then(|| __mapper_db_result.as_ref().err().and_then(|error| error.as_database_error()).and_then(|error| error.code())).flatten();
        __mapper_db_guard.finish(#root::classify_sqlx_result(&__mapper_db_result), __mapper_rows, __mapper_normalized_sql.as_ref(), __mapper_code.as_deref());
        __mapper_db_result
    } })
}
/// 业务作用：生成运行时类型化查询并按计划添加绑定。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
/// - `fetch`：已选定的行集合、可选单行、单行或标量模式。
///
/// 返回：标量模式使用 query_scalar，其它使用 query_as；动态或静态绑定生成失败时返回错误，成功结果仍需后续执行。
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
    Ok(parameter_query_tokens(root, query))
}
/// 业务作用：生成由 sqlx 宏检查静态 SQL 的查询表达式。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
/// - `fetch`：已选定的行集合、可选单行、单行或标量模式。
///
/// 返回：返回 query_scalar! 或 query_as! 及参数捕获包装 token；绑定缺失时报错，SQL 检查在生成代码编译阶段完成，不在本函数访问数据库。
fn checked_query_builder_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    fetch: FetchMode<'_>,
) -> syn::Result<TokenStream2> {
    let sql = LitStr::new(&plan.normalized_sql, plan.method.sig.ident.span());
    let args = plan
        .binds
        .iter()
        .map(|bind| {
            let expr = bind_expr(bind, &plan.params)?;
            let name = &bind.name;
            Ok(quote! {{ let __mapper_bind_value = #expr;
                (&__MapperCapture(&__mapper_bind_value)).capture(&mut __mapper_parameters, #name);
                __mapper_bind_value
            }})
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let query = match fetch {
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
    };
    Ok(parameter_query_tokens(root, query))
}
/// 业务作用：生成流式查询使用的拥有值绑定表达式。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
/// - `row_ty`：流中每行应解码为的 Rust 类型。
///
/// 返回：返回 query_as 与流式绑定包装 token；绑定缺失时报错，不在此处取数。
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
    Ok(parameter_query_tokens(
        root,
        apply_stream_binds_tokens(query, &plan.binds, &plan.params)?,
    ))
}
/// 业务作用：生成写 SQL 的 sqlx 查询及绑定表达式。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
///
/// 返回：返回静态或动态绑定完成的查询 token，绑定生成失败时报错；执行和事务提交由调用方法及运行时负责。
fn write_query_tokens(root: &TokenStream2, plan: &MethodPlan) -> syn::Result<TokenStream2> {
    let query = quote! {
        #root::__private::sqlx::query(
            #root::__private::sqlx::AssertSqlSafe(__mapper_normalized_sql.clone())
        )
    };
    let query = if let Some(dynamic) = &plan.dynamic {
        apply_dynamic_binds_tokens(query, dynamic, &plan.params)
    } else {
        apply_binds_tokens(query, &plan.binds, &plan.params)
    }?;
    Ok(parameter_query_tokens(root, query))
}

/// 业务作用：为实际构造的 prepared query 提供独立参数预算，不在缓存命中路径格式化参数。
///
/// 参数说明：
/// - `root`：为运行时路径
/// - `query`：为原有 SQLx 查询构造表达式。
///
/// 返回：保持查询类型和参数所有权不变的表达式，绑定完成后发送开发参数事件。
fn parameter_query_tokens(root: &TokenStream2, query: TokenStream2) -> TokenStream2 {
    quote! {{
        #[allow(unused_imports)]
        use #root::observability::console::{CaptureParameter as _, Capture as __MapperCapture};
        let mut __mapper_parameters = #root::observability::console::ParameterBuffer::new(__mapper_meta);
        let __mapper_built_query = #query;
        __mapper_parameters.emit();
        __mapper_built_query
    }}
}
/// 业务作用：按 prepared 占位符顺序绑定标量或列表，并在不重复求值的前提下采集开发参数。
///
/// 参数说明：
/// - `query`: 查询对象或 query 参数集合。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `params`: 已解析的函数参数或宏参数列表。
///
/// 返回：查询构造表达式；非法字段引用在编译期拒绝，不为原参数新增显示 trait 约束。
fn apply_binds_tokens(
    mut query: TokenStream2,
    binds: &[BindInfo],
    params: &[ParamInfo],
) -> syn::Result<TokenStream2> {
    for bind in binds {
        let name = &bind.name;
        match bind.kind {
            BindKind::Scalar => {
                let expr = bind_expr(bind, params)?;
                query = quote! {{
                    let __mapper_query = #query;
                    let __mapper_bind_value = #expr;
                    (&__MapperCapture(&__mapper_bind_value)).capture(&mut __mapper_parameters, #name);
                    __mapper_query.bind(__mapper_bind_value)
                }};
            }
            BindKind::List => {
                let ident = &bind.root;
                query = quote! {
                    {
                        let mut __mapper_query = #query;
                        for __mapper_item in #ident.iter() {
                            (&__MapperCapture(&__mapper_item)).capture(&mut __mapper_parameters, #name);
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
/// 业务作用：让结果流拥有其 bind 材料，并保持开发显示不改变传入 SQLx 的值。
///
/// 参数说明：
/// - `query`: 查询对象或 query 参数集合。
/// - `binds`: SQL 动态片段收集出的绑定列表。
/// - `params`: 已解析的函数参数或宏参数列表。
///
/// 返回：拥有型查询构造表达式；无法安全形成流式 bind 时给出编译期错误。
fn apply_stream_binds_tokens(
    mut query: TokenStream2,
    binds: &[BindInfo],
    params: &[ParamInfo],
) -> syn::Result<TokenStream2> {
    for bind in binds {
        let expr = stream_bind_expr(bind, params)?;
        let name = &bind.name;
        query = quote! {{
            let __mapper_query = #query;
            let __mapper_bind_value = #expr;
            (&__MapperCapture(&__mapper_bind_value)).capture(&mut __mapper_parameters, #name);
            __mapper_query.bind(__mapper_bind_value)
        }};
    }
    Ok(query)
}
/// 业务作用：将动态 SQL 节点对应的运行时绑定步骤包入查询表达式。
///
/// 参数说明：
/// - `query`：待追加绑定的 sqlx 查询表达式 token。
/// - `dynamic`：存在动态 SQL 时携带的已解析节点计划。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
///
/// 返回：返回依次更新并交回查询对象的 token；子节点绑定生成失败时返回错误，不在宏展开时求值条件。
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
/// 业务作用：按动态节点控制流生成匹配 SQL 结构的绑定步骤。
///
/// 参数说明：
/// - `nodes`：需要递归检查或生成签名的动态 SQL 节点序列。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
/// - `local_items`：当前可引用的外层 foreach 局部元素标识。
///
/// 返回：返回条件、选择和循环对应的绑定 token；非法绑定传播错误。条件在生成代码执行时求值，排序与固定文本不增加值绑定。
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
            SqlNode::If {
                test,
                body,
            } => {
                let body = dynamic_bind_steps_tokens(body, params, local_items)?;
                steps.push(quote! {
                    if #test {
                        #body
                    }
                });
            }
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
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
            SqlNode::Trim {
                body,
                ..
            } => {
                steps.push(dynamic_bind_steps_tokens(body, params, local_items)?);
            }
            SqlNode::OrderBy {
                ..
            } => {}
        }
    }
    Ok(quote! { #(#steps)* })
}
/// 业务作用：对已选中的动态 SQL 节点绑定真实参数，仅将实际进入查询的值记入开发事件。
///
/// 参数说明：
/// - `bind`: SQL 占位符绑定信息。
/// - `params`: 已解析的函数参数或宏参数列表。
/// - `local_items`: 当前语法块内收集到的局部变量或局部条目。
///
/// 返回：不重复求值的绑定语句；字段引用不合法时在宏展开期拒绝。
fn dynamic_bind_one_token(
    bind: &BindInfo,
    params: &[ParamInfo],
    local_items: &[Ident],
) -> syn::Result<TokenStream2> {
    let name = &bind.name;
    match bind.kind {
        BindKind::Scalar => {
            let expr = if local_items.contains(&bind.root) {
                foreach_item_bind_expr(bind)
            } else {
                bind_expr(bind, params)?
            };
            Ok(quote! {
                let __mapper_bind_value = #expr;
                (&__MapperCapture(&__mapper_bind_value)).capture(&mut __mapper_parameters, #name);
                __mapper_query = __mapper_query.bind(__mapper_bind_value);
            })
        }
        BindKind::List => {
            let ident = &bind.root;
            Ok(quote! {
                for __mapper_item in #ident.iter() {
                    (&__MapperCapture(&__mapper_item)).capture(&mut __mapper_parameters, #name);
                    __mapper_query = __mapper_query.bind(__mapper_item);
                }
            })
        }
    }
}
/// 业务作用：生成 Mapper 宏 `sql_init_tokens` 阶段所需的 Rust token，保持参数绑定和运行时调用语义一致。
///
/// 参数说明：
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `plan`: 宏期方法计划,包含 SQL、参数、缓存和事务信息。
/// - `span`: 源码位置,用于生成精确的编译期错误。
///
/// 返回：运行时构造当前后端最终 prepared SQL 的代码。
fn raw_sql_init_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    span: proc_macro2::Span,
) -> TokenStream2 {
    if let Some(dynamic) = &plan.dynamic {
        return dynamic_sql_init_tokens(root, dynamic, span, plan.backend);
    }

    if plan.backend != MapperBackend::PostgreSql
        && !plan.binds.iter().any(|bind| bind.kind == BindKind::List)
    {
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
                if plan.backend == MapperBackend::PostgreSql {
                    parts.push(quote! {
                        __mapper_bind_index.push_bind(&mut __mapper_sql);
                    });
                } else {
                    parts.push(quote! {
                        __mapper_sql.push('?');
                    });
                }
            }
            BindKind::List => {
                let ident = &bind.root;
                if plan.backend == MapperBackend::PostgreSql {
                    parts.push(quote! {
                        __mapper_bind_index.push_bind_list(&mut __mapper_sql, #ident.len())?;
                    });
                } else {
                    parts.push(quote! {
                        __mapper_sql.push_str(
                            #root::__private::sql_in_placeholders(#ident.len())?.as_str(),
                        );
                    });
                }
            }
        }
    }
    let last_fragment = plan.sql_fragments.last().cloned().unwrap_or_default();
    let last_fragment = LitStr::new(&last_fragment, span);
    let bind_index = if plan.backend == MapperBackend::PostgreSql {
        quote! { let mut __mapper_bind_index = #root::__private::PostgresBindIndex::new(); }
    } else {
        TokenStream2::new()
    };
    quote! {
        let __mapper_normalized_sql: ::std::borrow::Cow<'static, str> = {
            let mut __mapper_sql = ::std::string::String::new();
            #bind_index
            #(#parts)*
            __mapper_sql.push_str(#last_fragment);
            ::std::borrow::Cow::Owned(#root::__private::normalize_sql_whitespace(&__mapper_sql))
        };
    }
}
/// 业务作用：生成最终 SQL，并在 SQLite 缓存命中前执行真实只读权限验证。
///
/// 参数说明：
/// - `root`：当前后端 runtime。
/// - `plan`：方法 SQL 和缓存合同。
/// - `span`：调用方源码位置。
///
/// 返回：SQL 构建与必要的执行前门禁代码。
fn sql_init_tokens(
    root: &TokenStream2,
    plan: &MethodPlan,
    span: proc_macro2::Span,
) -> TokenStream2 {
    let sql = raw_sql_init_tokens(root, plan, span);
    if plan.backend == MapperBackend::Sqlite && plan.cache {
        quote! { #sql #root::validate_cached_read(&__mapper_source, __mapper_normalized_sql.as_ref()).await?; }
    } else {
        sql
    }
}

/// 业务作用：生成 Mapper 宏 `dynamic_sql_init_tokens` 阶段所需的 Rust token，保持参数绑定和运行时调用语义一致。
///
/// 参数说明：
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `dynamic`: 动态 SQL 节点或动态绑定上下文。
/// - `span`: 源码位置,用于生成精确的编译期错误。
/// - `backend`: 决定动态 bind 使用 `?` 或连续 `$n` 的数据库后端。
///
/// 返回：按实际分支构造规范 SQL 的运行时代码。
fn dynamic_sql_init_tokens(
    root: &TokenStream2,
    dynamic: &DynamicSqlPlan,
    span: proc_macro2::Span,
    backend: MapperBackend,
) -> TokenStream2 {
    let target = format_ident!("__mapper_sql");
    let body = push_dynamic_sql_nodes_tokens(root, &dynamic.nodes, span, &target, backend);
    let bind_index = if backend == MapperBackend::PostgreSql {
        quote! { let mut __mapper_bind_index = #root::__private::PostgresBindIndex::new(); }
    } else {
        TokenStream2::new()
    };
    quote! {
        let __mapper_normalized_sql: ::std::borrow::Cow<'static, str> = {
            let mut #target = ::std::string::String::new();
            #bind_index
            #body
            ::std::borrow::Cow::Owned(#root::__private::normalize_sql_whitespace(&#target))
        };
    }
}
/// 业务作用：生成 Mapper 宏 `push_dynamic_sql_nodes_tokens` 阶段所需的 Rust token，保持参数绑定和运行时调用语义一致。
///
/// 参数说明：
/// - `root`: 运行时 crate 根路径 token,用于生成可编译代码。
/// - `nodes`: 动态 SQL 节点列表。
/// - `span`: 源码位置,用于生成精确的编译期错误。
/// - `target`: 生成代码中承载动态 SQL 字符串的目标 buffer 变量。
/// - `backend`: 当前节点固定使用的数据库后端。
///
/// 返回：按节点顺序追加 SQL 并同步推进 bind 编号的运行时代码。
fn push_dynamic_sql_nodes_tokens(
    root: &TokenStream2,
    nodes: &[SqlNode],
    span: proc_macro2::Span,
    target: &Ident,
    backend: MapperBackend,
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
                    if backend == MapperBackend::PostgreSql {
                        parts.push(quote! {
                            __mapper_bind_index.push_bind(&mut #target);
                        });
                    } else {
                        parts.push(quote! {
                            #target.push('?');
                        });
                    }
                }
                BindKind::List => {
                    let ident = &bind.root;
                    if backend == MapperBackend::PostgreSql {
                        parts.push(quote! {
                            __mapper_bind_index.push_bind_list(&mut #target, #ident.len())?;
                        });
                    } else {
                        parts.push(quote! {
                            #target.push_str(
                                #root::__private::sql_in_placeholders(#ident.len())?.as_str(),
                            );
                        });
                    }
                }
            },
            SqlNode::If {
                test,
                body,
            } => {
                let body = push_dynamic_sql_nodes_tokens(root, body, span, target, backend);
                parts.push(quote! {
                    if #test {
                        #body
                    }
                });
            }
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
                let otherwise =
                    push_dynamic_sql_nodes_tokens(root, otherwise, span, target, backend);
                let mut chain = quote! { #otherwise };
                for when in whens.iter().rev() {
                    let test = &when.test;
                    let body =
                        push_dynamic_sql_nodes_tokens(root, &when.body, span, target, backend);
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
                let body = push_dynamic_sql_nodes_tokens(root, body, span, target, backend);
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
                let body = push_dynamic_sql_nodes_tokens(root, body, span, &trim_target, backend);
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
            SqlNode::OrderBy {
                value,
            } => {
                parts.push(quote! {
                    #root::__private::write_mapper_order_by_clause(&#value, &mut #target)?;
                });
            }
        }
    }
    quote! { #(#parts)* }
}
/// 业务作用：生成基于规范 SQL 和当前绑定值的缓存键构造表达式。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `plan`：已经解析并通过上层合同检查的 SQL 方法执行计划。
///
/// 返回：返回结果为 Option 的运行时代码；构键失败记录分类指标，Strict 提前返回调用错误，Bypass 返回 None。动态条件会在构键时再次求值，表达式应由调用方保持无副作用与稳定。
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
/// 业务作用：按动态节点控制流生成缓存参数收集代码。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `nodes`：需要递归检查或生成签名的动态 SQL 节点序列。
/// - `local_items`：当前可引用的外层 foreach 局部元素标识。
///
/// 返回：返回仅在运行时选中分支和循环迭代中追加参数的 token；不在宏展开时序列化，不保证与其它阶段反复求值的条件天然一致。
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
            SqlNode::If {
                test,
                body,
            } => {
                let body = dynamic_cache_arg_steps_tokens(root, body, local_items);
                steps.push(quote! {
                    if #test {
                        #body
                    }
                });
            }
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
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
            SqlNode::Trim {
                body,
                ..
            } => {
                steps.push(dynamic_cache_arg_steps_tokens(root, body, local_items));
            }
            SqlNode::OrderBy {
                ..
            } => {}
        }
    }
    quote! { #(#steps)* }
}
/// 业务作用：为单个动态绑定生成缓存键参数收集代码。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
/// - `local_items`：当前可引用的外层 foreach 局部元素标识。
///
/// 返回：标量添加一个 CacheArg，列表逐项添加；本地 foreach 元素按其借用形态访问，运行时参数编码失败通过 ? 传播。
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
/// 业务作用：生成 L2 缓存读取、数据库加载和编码回写编排。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `cache_ty`：缓存解码后需要恢复的查询成功返回类型。
/// - `cache_errors`：缓存失败时采用严格返回还是绕过策略。
/// - `conn`：数据库加载时获取受控连接的表达式。
/// - `query`：待追加绑定的 sqlx 查询表达式 token。
/// - `typed_cache_codec`：可选的强类型 codec 工厂路径，存在时选择强类型编解码分支。
///
/// 返回：返回运行时代码：有缓存与键时委托 get_or_load，成功提前返回值；Strict 对加载或解码失败报错，Bypass 可复用已取数据库值或落回后续查询。旧格式回写失败只告警，不覆盖已解码结果。
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
                            "mapper cache rewrite encode failed"
                        );
                    }
                }
            }
        }
    };
    quote! {
        if let (::core::option::Option::Some(cache), ::core::option::Option::Some(hash_key)) =
            (&__mapper_l2_cache, __mapper_hash_key.as_ref())
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
/// 业务作用：生成单独的 L2 缓存读取和解码短路代码。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `cache_ty`：缓存解码后需要恢复的查询成功返回类型。
/// - `cache_errors`：缓存失败时采用严格返回还是绕过策略。
///
/// 返回：返回运行时代码：命中并解码成功时提前返回；未命中继续后续路径，读取或解码失败按 Strict 返回错误或按 Bypass 继续。
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
                "mapper cache decode failed, bypass"
            );
        }
    };
    quote! {
        if let (::core::option::Option::Some(cache), ::core::option::Option::Some(hash_key)) =
            (&__mapper_l2_cache, __mapper_hash_key.as_ref())
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
/// 业务作用：生成查询成功值的缓存编码与写回代码。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `cache_errors`：缓存失败时采用严格返回还是绕过策略。
///
/// 返回：返回运行时代码：缓存与键存在时编码并写回；失败按 Strict 提前报错或按 Bypass 保留来源值，宏展开阶段不写缓存。
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
                "mapper cache put failed, return source value"
            );
        }
    };
    quote! {
        if let (::core::option::Option::Some(cache), ::core::option::Option::Some(hash_key)) =
            (&__mapper_l2_cache, __mapper_hash_key.as_ref())
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
/// 业务作用：生成写入成功后的命名空间及引用缓存失效步骤。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `key_expr`：生成代码中 Mapper 缓存命名空间的表达式 token。
/// - `flush_cache`：是否生成当前缓存命名空间的失效步骤。
/// - `flush_refs`：是否把引用当前命名空间的关联缓存纳入失效目标。
///
/// 返回：flush_cache 为 false 时返回空 token，否则生成目标解析及交由运行时延后到提交或立即失效的异步调用；失败以 ? 传播，不等于数据库操作回滚。
fn clear_tokens(
    root: &TokenStream2,
    key_expr: &TokenStream2,
    flush_cache: bool,
    flush_refs: bool,
) -> TokenStream2 {
    if flush_cache {
        quote! {
            let __mapper_clear_keys = #root::cache_clear_targets(#key_expr, #flush_refs);
            #root::clear_after_commit_or_now(__mapper_l2_cache.clone(), __mapper_clear_keys).await?;
        }
    } else {
        quote! {}
    }
}
/// 业务作用：按数据库后端和事务要求生成连接获取表达式。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `tx`：自动参与、必须存在或禁止存在事务的连接模式。
/// - `datasource`：已校验的可选具名数据源名称。
/// - `backend`：生成代码绑定的数据库后端。
///
/// 返回：SQLite 从受管 source 获取连接；其它后端使用具名数据源，缺省为 default，并按 Auto、Mandatory 或 Never 选择入口。获取失败由生成代码传播，不在宏展开时建连。
fn conn_tokens(
    root: &TokenStream2,
    tx: TxMode,
    datasource: Option<&str>,
    backend: MapperBackend,
) -> TokenStream2 {
    if backend == MapperBackend::Sqlite {
        let mode = match tx {
            TxMode::Auto => "auto",
            TxMode::Mandatory => "mandatory",
            TxMode::Never => "never",
        };
        return quote!(#root::mapper_conn_from(&__mapper_source, #mode).await?);
    }
    let datasource = datasource.unwrap_or("default");
    let acquire = match tx {
        TxMode::Auto => quote! { #root::mapper_conn_for(#datasource).await? },
        TxMode::Mandatory => quote! { #root::mapper_mandatory_conn_for(#datasource).await? },
        TxMode::Never => quote! { #root::mapper_never_conn_for(#datasource).await? },
    };
    quote! {{
        __mapper_call.path(#root::observability::CallPath::PreExecution);
        let __mapper_connection = #acquire;
        __mapper_call.path(#root::observability::CallPath::Database);
        __mapper_connection
    }}
}
/// 业务作用：为普通查询生成不转移参数所有权的绑定表达式。
///
/// 参数说明：
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
///
/// 返回：顶层引用参数直接使用，其它顶层参数及字段路径加借用；根参数不存在时返回编译错误，不检查字段是否存在。
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
/// 业务作用：为流式查询生成可由查询持有的绑定值。
///
/// 参数说明：
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
/// - `params`：Mapper 方法的已解析显式参数及其类型信息。
///
/// 返回：字段路径生成 clone，顶层引用生成 to_owned，拥有参数直接使用；未知根参数报错，Clone 与编码能力由后续编译验证。
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
/// 业务作用：生成用于缓存参数编码的字段值借用。
///
/// 参数说明：
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
///
/// 返回：返回完整字段访问表达式的借用 token，不验证字段或类型。
fn bind_value_expr(bind: &BindInfo) -> TokenStream2 {
    let value = bind_access_tokens(bind);
    quote! { &#value }
}
/// 业务作用：为迭代元素生成适合 SQL 绑定的借用形态。
///
/// 参数说明：
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
///
/// 返回：无字段路径时直接使用元素标识，存在字段路径时借用该字段；不额外查验作用域。
fn foreach_item_bind_expr(bind: &BindInfo) -> TokenStream2 {
    let value = bind_access_tokens(bind);
    if bind.path.is_empty() {
        quote! { #value }
    } else {
        quote! { &#value }
    }
}
/// 业务作用：为迭代元素生成与 SQL 绑定一致的缓存参数表达式。
///
/// 参数说明：
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
///
/// 返回：原样返回 foreach_item_bind_expr 的 token，不求值或序列化。
fn foreach_item_bind_value_expr(bind: &BindInfo) -> TokenStream2 {
    foreach_item_bind_expr(bind)
}
/// 业务作用：把绑定根和字段路径组合为 Rust 访问表达式。
///
/// 参数说明：
/// - `bind`：根参数、字段路径、原名称与标量或列表种类组成的绑定描述。
///
/// 返回：返回逐段字段访问 token，不验证根作用域、字段存在性或访问权限。
fn bind_access_tokens(bind: &BindInfo) -> TokenStream2 {
    let root = &bind.root;
    let mut value = quote! { #root };
    for segment in &bind.path {
        value = quote! { #value.#segment };
    }
    value
}
/// 业务作用：为静态绑定顺序生成缓存键参数编码步骤。
///
/// 参数说明：
/// - `root`：宏展开代码应引用的目标 Mapper 运行时路径 token。
/// - `binds`：按 SQL 出现顺序排列的绑定描述。
/// - `foreach_item`：当前可选的 foreach 元素根标识。
///
/// 返回：标量追加一次，列表按迭代顺序逐项追加；匹配 foreach_item 时保留元素借用形态，生成代码中的编码失败通过 ? 传播。
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
/// 业务作用：提取 Mapper 方法 Result 成功类型，供后续取数模式选择。
///
/// 参数说明：
/// - `output`：Mapper 方法签名中的返回类型声明。
///
/// 返回：返回末段名为 Result 的路径类型首个类型实参；省略返回值或不匹配时报错。不要求路径确为 anyhow，也不校验额外泛型参数。
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
/// 业务作用：按语法末段名称读取容器的首个类型参数。
///
/// 参数说明：
/// - `ty`：需要按语法形态识别的 Rust 类型。
/// - `name`：希望匹配的容器末段名称。
///
/// 返回：路径末段名匹配且首个角括号实参为类型时返回借用，否则返回 None，不解析类型别名或完整路径身份。
fn first_generic_inner<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    let Type::Path(TypePath {
        path,
        ..
    }) = ty
    else {
        return None;
    };
    let segment = path.segments.last()?;
    if segment.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
        args,
        ..
    }) = &segment.arguments
    else {
        return None;
    };
    match args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}
/// 业务作用：识别无需向调用方返回执行结果的空元组类型。
///
/// 参数说明：
/// - `ty`：需要按语法形态识别的 Rust 类型。
///
/// 返回：仅空 Type::Tuple 返回 true，其它形态返回 false，不展开类型别名。
fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}
/// 业务作用：按路径末段识别生成代码关心的返回类型名称。
///
/// 参数说明：
/// - `ty`：需要按语法形态识别的 Rust 类型。
/// - `name`：期望匹配的类型末段名称。
///
/// 返回：路径类型末段标识匹配时返回 true，不核实路径来源、泛型或类型别名。
fn is_type_ident(ty: &Type, name: &str) -> bool {
    let Type::Path(TypePath {
        path,
        ..
    }) = ty
    else {
        return false;
    };
    path.segments
        .last()
        .is_some_and(|segment| segment.ident == name)
}
/// 业务作用：识别需要显式声明 scalar 取数的常见标量名称。
///
/// 参数说明：
/// - `ty`：需要按语法形态识别的 Rust 类型。
///
/// 返回：路径末段为内置数值、bool 或 String 白名单时返回 true；引用、别名及其它类型返回 false，不做类型解析。
fn is_scalar_type(ty: &Type) -> bool {
    let Type::Path(TypePath {
        path,
        ..
    }) = ty
    else {
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
/// 业务作用：按声明语法识别用于列表绑定的集合形态。
///
/// 参数说明：
/// - `ty`：需要按语法形态识别的 Rust 类型。
///
/// 返回：引用递归解包，切片、数组及路径末段 Vec 或 VecDeque 返回 true；其它返回 false，不验证泛型和别名。
fn is_collection_type(ty: &Type) -> bool {
    match ty {
        Type::Reference(reference) => is_collection_type(&reference.elem),
        Type::Slice(_) | Type::Array(_) => true,
        Type::Path(TypePath {
            path,
            ..
        }) => path.segments.last().is_some_and(|segment| {
            matches!(segment.ident.to_string().as_str(), "Vec" | "VecDeque")
        }),
        _ => false,
    }
}
/// 业务作用：识别模板占位符是否独占 IN 后的一对括号内容。
///
/// 参数说明：
/// - `sql`：方法声明中的 SQL 模板原文。
/// - `start`：待识别片段起始的有效 UTF-8 字节边界位置。
/// - `end`：占位符结束之后的有效字节偏移。
///
/// 返回：前方忽略空白后为 IN 关键字加左括号，后方忽略空白后为右括号时返回 true；否则 false。start/end 必须是有效占位符字节范围，本检查不是完整 SQL 解析。
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
/// 业务作用：判断文本尾部是否是有标识边界的 SQL 关键字。
///
/// 参数说明：
/// - `value`：待检查关键字后缀的 SQL 前缀。
/// - `keyword`：要匹配的 ASCII SQL 关键字。
///
/// 返回：去除尾空白后 ASCII 忽略大小写匹配，且前一字符不是 ASCII 标识继续字符时返回 true。按字节计算后缀位置，调用方须保证该位置是字符边界，否则切片会展开。
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
/// 业务作用：为 SQL 关键字边界识别提供 ASCII 标识继续字符规则。
///
/// 参数说明：
/// - `ch`：关键字之前待检查的字符。
///
/// 返回：下划线或 ASCII 字母数字返回 true，其它字符返回 false，不覆盖全部 SQL 方言标识字符。
fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}
/// 业务作用：完成 Mapper 宏 `key_expr` 的编译期转换步骤，并把失败定位到调用方源码。
///
/// 参数说明：
/// - `key`: `#[mapper(key = "...")]` 显式指定的 mapper 名称。
/// - `trait_ident`: 生成 mapper 实现时使用的 trait 标识符。
/// - `backend`: 当前 Mapper 固定使用的数据库后端。
/// - `datasource`: trait 级命名数据源；省略时采用 `default`。
///
/// 返回：MySQL 兼容 key 或包含 PostgreSQL driver/datasource 身份的静态表达式。
fn key_expr(
    key: Option<&str>,
    trait_ident: &Ident,
    backend: MapperBackend,
    datasource: Option<&str>,
) -> TokenStream2 {
    match (backend, key) {
        (MapperBackend::MySql, Some(key)) => {
            let lit = LitStr::new(key, trait_ident.span());
            quote! { #lit }
        }
        (MapperBackend::MySql, None) => {
            quote! { concat!(module_path!(), "::", stringify!(#trait_ident)) }
        }
        (MapperBackend::Sqlite, Some(key)) => {
            let datasource = datasource.unwrap_or("default");
            quote!(concat!("sqlite:", #datasource, ":", #key))
        }
        (MapperBackend::Sqlite, None) => {
            let datasource = datasource.unwrap_or("default");
            quote!(
                concat!("sqlite:", #datasource, ":", module_path!(), "::", stringify!(#trait_ident))
            )
        }
        (MapperBackend::PostgreSql, Some(key)) => {
            let key = LitStr::new(key, trait_ident.span());
            let datasource = LitStr::new(datasource.unwrap_or("default"), trait_ident.span());
            quote! { concat!("postgresql:", #datasource, ":", #key) }
        }
        (MapperBackend::PostgreSql, None) => {
            let datasource = LitStr::new(datasource.unwrap_or("default"), trait_ident.span());
            quote! {
                concat!("postgresql:", #datasource, ":", module_path!(), "::", stringify!(#trait_ident))
            }
        }
    }
}
/// 业务作用：生成缓存命名空间依赖等静态字符串列表表达式。
///
/// 参数说明：
/// - `values`：需要嵌入生成代码的字符串序列。
///
/// 返回：返回字符串字面量引用数组的 token，保持顺序并按 Rust 字面量规则转义。
fn str_array_tokens(values: &[String]) -> TokenStream2 {
    let values = values
        .iter()
        .map(|value| LitStr::new(value, proc_macro2::Span::call_site()));
    quote! { &[#(#values),*] }
}
/// 业务作用：为缓存关联键补齐 PostgreSQL driver 与 datasource 身份，防止跨后端或跨数据源误清理。
///
/// 参数说明：
/// - `values`: `clear_also` 或 `clear_when` 声明的 Mapper key。
/// - `backend`: 当前 Mapper 的数据库后端。
/// - `datasource`: trait 级命名数据源；省略时采用 `default`。
///
/// 返回：MySQL 保持既有 key；PostgreSQL 生成与 Mapper 主 key 相同命名规则的静态数组。
fn cache_reference_array_tokens(
    values: &[String],
    backend: MapperBackend,
    datasource: Option<&str>,
) -> TokenStream2 {
    if backend == MapperBackend::MySql {
        return str_array_tokens(values);
    }
    let datasource = datasource.unwrap_or("default");
    let driver = if backend == MapperBackend::Sqlite {
        "sqlite"
    } else {
        "postgresql"
    };
    let values = values.iter().map(|value| {
        LitStr::new(
            &format!("{driver}:{datasource}:{value}"),
            proc_macro2::Span::call_site(),
        )
    });
    quote! { &[#(#values),*] }
}
/// 业务作用：把可选 TTL 等无符号数值嵌入生成代码。
///
/// 参数说明：
/// - `value`：准备嵌入的可选 u64 数值。
///
/// 返回：返回完全限定的 Some 数值或 None 表达式，不验证数值业务范围。
fn option_u64_tokens(value: Option<u64>) -> TokenStream2 {
    match value {
        Some(value) => quote! { ::core::option::Option::Some(#value) },
        None => quote! { ::core::option::Option::None },
    }
}
/// 业务作用：生成可选缓存 codec 工厂的调用表达式。
///
/// 参数说明：
/// - `path`：可选的 codec 工厂 Rust 路径。
///
/// 返回：路径存在时生成 Some(path())，否则生成 None；不在宏展开阶段调用工厂。
fn cache_codec_factory_tokens(path: Option<&Path>) -> TokenStream2 {
    match path {
        Some(path) => quote! { ::core::option::Option::Some(#path()) },
        None => quote! { ::core::option::Option::None },
    }
}
/// 业务作用：读取 Mapper 属性中声明的字符串列表。
///
/// 参数说明：
/// - `expr`：准备解析为字符串数组的 Rust 表达式。
///
/// 返回：数组且每项均为字符串字面量时返回原值列表，否则返回位于非法表达式的编译错误；不检查空值或重复。
fn parse_string_array(expr: Expr) -> syn::Result<Vec<String>> {
    let Expr::Array(ExprArray {
        elems,
        ..
    }) = expr
    else {
        return Err(syn::Error::new_spanned(expr, "期望字符串数组"));
    };
    elems
        .iter()
        .map(|elem| match elem {
            Expr::Lit(ExprLit {
                lit: Lit::Str(lit),
                ..
            }) => Ok(lit.value()),
            _ => Err(syn::Error::new_spanned(elem, "数组元素必须是字符串字面量")),
        })
        .collect()
}
/// 业务作用：把属性整数字面量转换为 u64 配置值。
///
/// 参数说明：
/// - `lit`：已由 syn 解析的整数字面量。
///
/// 返回：成功返回数值，解析或范围超限返回 syn 错误，不检查 TTL 等业务上下界。
fn parse_lit_int(lit: LitInt) -> syn::Result<u64> {
    lit.base10_parse::<u64>()
}
/// 业务作用：在宏展开前拒绝空白数据源名称或首尾空白。
///
/// 参数说明：
/// - `value`：datasource 属性提供的具名数据源原文。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：非空且无首尾空白时成功，否则报错；不验证该数据源是否已配置。
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
/// 业务作用：把 SQL 或缓存占位符的点分隔字段路径转换为标识序列。
///
/// 参数说明：
/// - `value`：不含占位符定界符的字段路径。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：每段满足 ASCII 标识字符规则时返回 Ident 列表，否则报错；不解析字段类型，不证明标识在 Rust 语义上可用。
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
/// 业务作用：在生成排序白名单前验证静态列名形态。
///
/// 参数说明：
/// - `value`：声明的 column 或 alias.column 静态列名。
/// - `span`：产生编译错误时使用的调用方源码位置。
///
/// 返回：一段或两段名称均满足当前 ASCII 标识规则时成功，否则返回编译错误，不检查数据库真实列。
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
/// 业务作用：限制原样写入 ORDER BY 的列名只含一到两个标识段。
///
/// 参数说明：
/// - `value`：待检查的静态排序列名。
///
/// 返回：恰为一个或两个合法 ASCII 段时返回 true；空段、额外点或其它字符返回 false，不识别 SQL 关键字。
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
/// 业务作用：检查模板路径使用的非空 ASCII 标识字符形态。
///
/// 参数说明：
/// - `name`：待识别的标识字符串。
///
/// 返回：首字符为下划线或 ASCII 字母且后续只含下划线或 ASCII 字母数字时返回 true；不排除关键字，也不识别原始或 Unicode 标识符。
fn is_rust_ident(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
/// 业务作用：识别 trait 是否已声明异步方法转换属性。
///
/// 参数说明：
/// - `attrs`：trait 上已经解析的 Rust 属性列表。
///
/// 返回：任一属性路径末段为 async_trait 时返回 true，否则 false，不验证属性路径的实际来源。
fn has_async_trait_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "async_trait")
    })
}
