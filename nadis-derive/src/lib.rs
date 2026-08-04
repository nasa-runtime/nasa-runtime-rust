//! Redis 文档映射派生宏。
//!
//! 根据结构体上的 `#[rs(...)]` 元数据生成索引描述、键片段、字段持久化和反序列化辅助代码。
// ============================================================================
// nadis-derive：实现 #[derive(RedisDocument)]。
//
// 对照 原实现 注解全集(annotation/ 包)与 MetaResolver 启动期解析:
//   @RsDocument(index, prefix, type, bucketCount) → 结构体级 #[rs(...)]
//   @RsId            → #[rs(id)](恰好一个;数值类型自动判定 id_numeric)
//   @TagField        → #[rs(tag)]      可叠 sortable / alias / json_path
//   @TextField       → #[rs(text)]     可叠 sortable / weight / alias / json_path
//   @NumericField    → #[rs(numeric)]  可叠 sortable / alias / json_path
//   @JsonArrayKey    → #[rs(array_key)] 可叠 order = N(多字段升序拼接,
//                       同 order 重复 = 编译错,对照 原实现 duplicate-order 启动错)
//
// 用法:
//   #[derive(RedisDocument, Serialize, Deserialize)]
//   #[rs(index = "idx:order", prefix = "order:", data_type = "hash")]
//   struct Order {
//       #[rs(id)]               id: i64,
//       #[rs(tag, sortable)]    user_id: String,
//       #[rs(text, weight = 2.0)] title: String,
//       #[rs(numeric, sortable, alias = "px")] price: f64,
//       remark: String,          // 未标注:不进索引,但仍随 to_fields/from_fields 持久化
//   }
//
// data_type:"hash" | "json" | "json_array" | "json_array_bucket"(默认 "hash");
// bucket_count = N 仅 json_array_bucket(>1,**持久化协议**不可热修改)。
//
// 生成物(全部只调 nadis 公开 API,语义与手写 impl 零差异):
//   meta()          —— OnceLock<DocMeta> 单例(索引字段 = 标注 tag/text/numeric 的);
//   id()            —— @RsId 字段 to_string;
//   to_fields()     —— **全部字段**(含未标注/占位符字段)→ (name, value) 字符串对
//                      ——占位符字段值只存在 key 里且不可逆推,必须随 HASH 落盘
//                      (对照 原实现 storedFields 修复);
//   from_fields()   —— 逐字段 FromStr 解析,缺字段取 Default(字段类型须
//                      Display + FromStr + Default,String/数值天然满足);
//   placeholder_parts() —— prefix 的 {name} 在**展开期**与结构体字段名匹配
//                      (原实现 是运行时反查;Rust 提前到编译错),按出现顺序取值;
//   array_key_parts()   —— #[rs(array_key)] 字段按 order 升序取值。
// ============================================================================

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, LitChar, LitFloat, LitInt, LitStr, Type};

/// 字段上标注的索引类型。
enum FieldKind {
    Tag,
    Text,
    Numeric,
    Geo,
}

/// 解析后的字段信息(一个 #[rs(...)] 字段)。
struct FieldInfo {
    name: String,            // Rust 字段名(= 存储 field 名)
    ty: Type,                // 字段类型(from_fields 解析与 id_numeric 判定用)
    kind: Option<FieldKind>, // None = 未标注(不进索引,仍持久化)
    is_id: bool,
    sortable: bool,
    weight: f64,
    // TAG 字段属性。
    separator: Option<char>,
    case_sensitive: bool,
    // TEXT 字段属性。
    no_stem: bool,
    phonetic: Option<String>,
    no_index: bool,
    // TAG/TEXT 的 WITHSUFFIXTRIE 后缀与中缀查询能力。
    with_suffix_trie: bool,
    alias: String,
    json_path: Option<String>,
    array_key: bool,
    array_order: i64,
    decl_idx: usize, // 声明顺序(array_key 同 order 时报错的诊断 & 稳定排序)
}

/// 业务作用：判断字段是否为 f64/f32，以便持久化时使用兼容的浮点格式。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_float_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "f64" || seg.ident == "f32";
        }
    }
    false
}

/// 业务作用：展开 Redis 文档派生宏；用于生成键、元数据和序列化辅助实现。
///
/// # 参数
///
/// - `input`: 被 `#[derive(RedisDocument)]` 标注的结构体定义 token stream。
#[proc_macro_derive(RedisDocument, attributes(rs))]
pub fn derive_redis_document(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// 业务作用：解析派生输入并生成实现代码；用于把结构体声明转换为文档契约。
///
/// # 参数
/// - `input`: 宏或解析器收到的原始输入。
fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = &input.ident;

    //**显式拒绝泛型/生命周期参数**(`Order<'a>`/`Wrapper<T>`)——本宏生成的
    // `impl RedisDocument for #struct_name` 不带 `#generics`/`where`,对泛型类型会展开成畸形 impl
    // (引用未声明的参数)。给清晰编译错引导,而非生成不可编译/语义错的代码。
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "RedisDocument 不支持泛型/生命周期参数:DocMeta 是 'static 静态元数据,\
             请用具体类型(或为每个具体化手写 impl)",
        ));
    }

    // ── 运行时根路径发现(nasa-macro-support;消费侧冒烟实测抓出的真 bug):
    //    直接依赖 nadis → `::nadis`(含重命名);纯门面消费者 →
    //    `::nasa::redis`;都没有 → 编译错并给修复指引。硬编码 ::nadis 在
    //    门面工程解析不到(传递依赖不在 extern prelude)。
    let root = match nasa_macro_support::runtime_root("redis", "nadis") {
        Ok(r) => r,
        Err(msg) => return Err(syn::Error::new_spanned(input, msg)),
    };

    // ── 结构体级 #[rs(index/prefix/data_type/bucket_count)] ──
    let mut index: Option<String> = None;
    let mut prefix: Option<String> = None;
    let mut data_type = "hash".to_string();
    let mut bucket_count: u32 = 0;
    for attr in &input.attrs {
        if !attr.path().is_ident("rs") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("index") {
                index = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("prefix") {
                prefix = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("data_type") {
                data_type = meta.value()?.parse::<LitStr>()?.value();
            } else if meta.path.is_ident("bucket_count") {
                bucket_count = meta.value()?.parse::<LitInt>()?.base10_parse()?;
            } else {
                return Err(
                    meta.error("未知的 rs 结构体属性(可用:index/prefix/data_type/bucket_count)")
                );
            }
            Ok(())
        })?;
    }
    let index =
        index.ok_or_else(|| syn::Error::new_spanned(input, "#[rs(index = \"...\")] 必填"))?;
    let prefix =
        prefix.ok_or_else(|| syn::Error::new_spanned(input, "#[rs(prefix = \"...\")] 必填"))?;
    let data_type_variant = match data_type.as_str() {
        "hash" => quote!(#root::DataType::Hash),
        "json" => quote!(#root::DataType::Json),
        "json_array" => quote!(#root::DataType::JsonArray),
        "json_array_bucket" => quote!(#root::DataType::JsonArrayBucket),
        other => {
            return Err(syn::Error::new_spanned(
                input,
                format!("data_type \"{other}\" 非法(hash/json/json_array/json_array_bucket)"),
            ))
        }
    };

    // ── 字段解析 ──
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(RedisDocument)] 仅支持 struct",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(RedisDocument)] 仅支持具名字段 struct",
        ));
    };

    let mut fields: Vec<FieldInfo> = Vec::new();
    for (decl_idx, f) in named.named.iter().enumerate() {
        let name = f.ident.as_ref().unwrap().to_string();
        let mut info = FieldInfo {
            name,
            ty: f.ty.clone(),
            kind: None,
            is_id: false,
            sortable: false,
            weight: 1.0,
            separator: None,
            case_sensitive: false,
            no_stem: false,
            phonetic: None,
            no_index: false,
            with_suffix_trie: false,
            alias: String::new(),
            json_path: None,
            array_key: false,
            array_order: 0,
            decl_idx,
        };
        for attr in &f.attrs {
            if !attr.path().is_ident("rs") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    info.is_id = true;
                } else if meta.path.is_ident("tag") {
                    info.kind = Some(FieldKind::Tag);
                } else if meta.path.is_ident("text") {
                    info.kind = Some(FieldKind::Text);
                } else if meta.path.is_ident("numeric") {
                    info.kind = Some(FieldKind::Numeric);
                } else if meta.path.is_ident("geo") {
                    info.kind = Some(FieldKind::Geo);
                } else if meta.path.is_ident("sortable") {
                    info.sortable = true;
                } else if meta.path.is_ident("weight") {
                    info.weight = meta.value()?.parse::<LitFloat>()?.base10_parse()?;
                } else if meta.path.is_ident("separator") {
                    // separator 只接受单字符，用作 TAG 多值分隔符。
                    info.separator = Some(meta.value()?.parse::<LitChar>()?.value());
                } else if meta.path.is_ident("casesensitive") {
                    info.case_sensitive = true;
                } else if meta.path.is_ident("nostem") {
                    info.no_stem = true;
                } else if meta.path.is_ident("phonetic") {
                    // phonetic 配置 PHONETIC 匹配器，例如 `dm:en`。
                    info.phonetic = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("noindex") {
                    info.no_index = true;
                } else if meta.path.is_ident("suffix") {
                    // suffix 为字段启用 WITHSUFFIXTRIE 后缀与中缀查询。
                    info.with_suffix_trie = true;
                } else if meta.path.is_ident("alias") {
                    info.alias = meta.value()?.parse::<LitStr>()?.value();
                } else if meta.path.is_ident("json_path") {
                    info.json_path = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("array_key") {
                    info.array_key = true;
                } else if meta.path.is_ident("order") {
                    info.array_order = meta.value()?.parse::<LitInt>()?.base10_parse()?;
                } else {
                    return Err(meta.error(
                        "未知的 rs 字段属性(可用:id/tag/text/numeric/geo/sortable/weight/separator/casesensitive/nostem/phonetic/noindex/suffix/alias/json_path/array_key/order)",
                    ));
                }
                Ok(())
            })?;
        }
        fields.push(info);
    }

    // ── 校验:恰好一个 id;array_key 同 order 重复报错(对照 原实现 MetaResolver)──
    let ids: Vec<&FieldInfo> = fields.iter().filter(|f| f.is_id).collect();
    if ids.len() != 1 {
        return Err(syn::Error::new_spanned(
            input,
            format!("#[rs(id)] 必须恰好标注一个字段(当前 {} 个)", ids.len()),
        ));
    }
    let id_field = ids[0];
    let id_name = id_field.name.clone();
    let id_numeric = is_numeric_type(&id_field.ty);
    let id_ident = syn::Ident::new(&id_name, proc_macro2::Span::call_site());

    let mut array_fields: Vec<&FieldInfo> = fields.iter().filter(|f| f.array_key).collect();
    array_fields.sort_by_key(|f| (f.array_order, f.decl_idx));
    for w in array_fields.windows(2) {
        if w[0].array_order == w[1].array_order {
            return Err(syn::Error::new_spanned(
                input,
                format!(
                    "#[rs(array_key)] 字段 \"{}\" 与 \"{}\" 的 order 重复({})——多字段必须显式区分",
                    w[0].name, w[1].name, w[0].array_order
                ),
            ));
        }
    }

    // ── meta():FieldMeta 列表(仅标注 tag/text/numeric 的进索引)──
    let field_metas: Vec<TokenStream2> = fields
        .iter()
        .filter_map(|f| {
            let kind = f.kind.as_ref()?;
            let name = &f.name;
            let alias = &f.alias;
            let json_path = match &f.json_path {
                Some(p) => quote!(Some(#p.to_string())),
                None => quote!(None),
            };
            let sortable = f.sortable;
            let weight = f.weight;
            let case_sensitive = f.case_sensitive;
            let no_stem = f.no_stem;
            let no_index = f.no_index;
            let with_suffix_trie = f.with_suffix_trie;
            let separator = match f.separator {
                Some(c) => quote!(Some(#c)),
                None => quote!(None),
            };
            let phonetic = match &f.phonetic {
                Some(m) => quote!(Some(#m.to_string())),
                None => quote!(None),
            };
            let ftype = match kind {
                FieldKind::Tag => quote!(#root::FieldType::Tag {
                    sortable: #sortable,
                    separator: #separator,
                    case_sensitive: #case_sensitive,
                    with_suffix_trie: #with_suffix_trie,
                }),
                FieldKind::Text => quote!(#root::FieldType::Text {
                    sortable: #sortable,
                    weight: #weight,
                    no_stem: #no_stem,
                    phonetic: #phonetic,
                    no_index: #no_index,
                    with_suffix_trie: #with_suffix_trie,
                }),
                FieldKind::Numeric => {
                    quote!(#root::FieldType::Numeric { sortable: #sortable, no_index: #no_index })
                }
                FieldKind::Geo => quote!(#root::FieldType::Geo),
            };
            Some(quote! {
                #root::FieldMeta {
                    name: #name.to_string(),
                    alias: #alias.to_string(),
                    json_path: #json_path,
                    ftype: #ftype,
                }
            })
        })
        .collect();

    let array_key_names: Vec<&String> = array_fields.iter().map(|f| &f.name).collect();
    let array_key_idents: Vec<syn::Ident> = array_fields
        .iter()
        .map(|f| syn::Ident::new(&f.name, proc_macro2::Span::call_site()))
        .collect();

    // ── placeholder_parts():展开期解析 prefix 的 {name} 并匹配结构体字段——
    //    名字不存在 = 编译错(原实现 运行时反查,Rust 提前到编译期)──
    let placeholder_names =
        parse_placeholders(&prefix).map_err(|msg| syn::Error::new_spanned(input, msg))?;
    let mut placeholder_idents = Vec::new();
    for pn in &placeholder_names {
        if !fields.iter().any(|f| &f.name == pn) {
            return Err(syn::Error::new_spanned(
                input,
                format!("prefix 占位符 {{{pn}}} 不是结构体字段"),
            ));
        }
        placeholder_idents.push(syn::Ident::new(pn, proc_macro2::Span::call_site()));
    }

    // ── to_fields() / from_fields():全部字段(含未标注)──
    // f64/f32 字段使用兼容格式化函数对齐既有系统的 Double.toString（整数补 `.0`、科学计数法 E），
    // 否则 HASH 存储跨语言字节分叉。其余类型仍 `to_string`。
    let to_pairs: Vec<TokenStream2> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let ident = syn::Ident::new(name, proc_macro2::Span::call_site());
            let value_expr = if is_float_type(&f.ty) {
                quote!(#root::compat_double_to_string(self.#ident as f64))
            } else {
                quote!(self.#ident.to_string())
            };
            quote!((#name.to_string(), #value_expr))
        })
        .collect();
    let from_inits: Vec<TokenStream2> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let ident = syn::Ident::new(name, proc_macro2::Span::call_site());
            let ty = &f.ty;
            quote! {
                #ident: match fields.get(#name) {
                    Some(v) => v.parse::<#ty>().map_err(|e| {
                        #root::NasaRedisError::Codec(
                            format!("字段 {} 解析失败: {e:?}", #name),
                        )
                    })?,
                    None => ::core::default::Default::default(),
                }
            }
        })
        .collect();

    Ok(quote! {
        impl #root::RedisDocument for #struct_name {
            /// 业务作用：返回派生生成的文档元数据；用于运行时读取索引和字段定义。
            fn meta() -> &'static #root::DocMeta {
                static META: ::std::sync::OnceLock<#root::DocMeta> =
                    ::std::sync::OnceLock::new();
                META.get_or_init(|| #root::DocMeta {
                    index: #index.to_string(),
                    prefix: #prefix.to_string(),
                    data_type: #data_type_variant,
                    fields: vec![#(#field_metas),*],
                    id_name: #id_name.to_string(),
                    id_numeric: #id_numeric,
                    array_keys: vec![#(#array_key_names.to_string()),*],
                    bucket_count: #bucket_count,
                })
            }

            /// 业务作用：生成文档实例的主键字符串；用于定位 Redis 中的具体对象。
            fn id(&self) -> ::std::string::String {
                self.#id_ident.to_string()
            }

            /// 业务作用：转换为 fields 表示；用于对接下游接口。
            fn to_fields(&self) -> ::std::vec::Vec<(::std::string::String, ::std::string::String)> {
                vec![#(#to_pairs),*]
            }

            /// 业务作用：从 fields 构造结果；用于统一输入适配。
            ///
            /// # 参数
            /// - `fields`: Hash 字段名列表,用于批量读取或删除。
            fn from_fields(
                fields: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> #root::error::Result<Self> {
                Ok(Self {
                    #(#from_inits),*
                })
            }

            /// 业务作用：收集占位符字段值；用于按模板拼接文档键。
            fn placeholder_parts(&self) -> ::std::vec::Vec<::std::string::String> {
                vec![#(self.#placeholder_idents.to_string()),*]
            }

            /// 业务作用：收集数组键字段值；用于生成数组成员的定位键。
            fn array_key_parts(&self) -> ::std::vec::Vec<::std::string::String> {
                vec![#(self.#array_key_idents.to_string()),*]
            }
        }
    })
}

/// 业务作用：id_numeric 判定:字段类型路径末段是数值类型 → JSON 里渲染为数字
/// (决定 idFilter 字面量形态,见 DocMeta.id_numeric 注释)。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn is_numeric_type(ty: &Type) -> bool {
    let Type::Path(p) = ty else { return false };
    let Some(last) = p.path.segments.last() else {
        return false;
    };
    matches!(
        last.ident.to_string().as_str(),
        "i8" | "i16"
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
    )
}

/// 业务作用：展开期解析 prefix 占位符(与 nadis::DocMeta::segments 同语法:
/// `{name}` 不嵌套、括号配对、name 非空)。返回占位符名按出现顺序。
///
/// # 参数
/// - `prefix`: Redis key、配置 key 或占位符路径前缀。
fn parse_placeholders(prefix: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut rest = prefix;
    while let Some(open) = rest.find('{') {
        //`{` 之前含孤立 '}' = 未配对(同 DocMeta::segments;让 derive 用户编译期报错)。
        if rest[..open].contains('}') {
            return Err(format!("prefix \"{prefix}\" 含未配对的 '}}'"));
        }
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .ok_or_else(|| format!("prefix \"{prefix}\" 占位符未闭合"))?;
        let name = &after[..close];
        if name.is_empty() || name.contains('{') {
            return Err(format!("prefix \"{prefix}\" 占位符名非法(空或嵌套)"));
        }
        out.push(name.to_string());
        rest = &after[close + 1..];
    }
    if rest.contains('}') {
        return Err(format!("prefix \"{prefix}\" 含未配对的 '}}'"));
    }
    Ok(out)
}
