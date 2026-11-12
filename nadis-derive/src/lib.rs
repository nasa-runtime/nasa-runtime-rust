//! Redis 文档映射派生宏。
//!
//! 根据结构体上的 `#[rs(...)]` 元数据生成索引描述、键片段、字段持久化和反序列化辅助代码。
// ============================================================================
// nadis-derive：实现 #[derive(RedisDocument)]。
//
// 派生属性在编译期决定文档结构与持久化布局:
//   结构体级 #[rs(index, prefix, data_type, bucket_count)]
//   #[rs(id)](恰好一个;数值类型自动判定 id_numeric)
//   #[rs(tag)]      可叠 sortable / alias / json_path
//   #[rs(text)]     可叠 sortable / weight / alias / json_path
//   #[rs(numeric)]  可叠 sortable / alias / json_path
//   #[rs(array_key)] 可叠 order = N(多字段升序拼接,
//                       重复 order 在编译期拒绝，避免键拼接顺序不确定)
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
//   id()            —— #[rs(id)] 字段 to_string;
//   to_fields()     —— **全部字段**(含未标注/占位符字段)→ (name, value) 字符串对
//                      ——占位符字段值只存在 key 里且不可逆推,必须随 HASH 落盘
//                      ;
//   from_fields()   —— 逐字段 FromStr 解析,缺字段取 Default(字段类型须
//                      Display + FromStr + Default,String/数值天然满足);
//   placeholder_parts() —— prefix 的 {name} 在**展开期**与结构体字段名匹配
//                      并在编译期拒绝未知字段，按出现顺序取值;
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

/// 业务作用：识别字段类型语法中的 f32/f64，使宏类型参数和括号包装沿用相同的浮点持久化格式。
///
/// 参数说明：
/// - `ty`：派生宏读取到的字段类型语法树，可能包含透明分组或括号。
///
/// 返回：类型去除 Group/Paren 后是路径且末段为 f32 或 f64 时返回 true，其余返回 false；只按语法识别，不解析类型别名或用户自定义同名类型。
fn is_float_type(ty: &Type) -> bool {
    // 宏传入的类型和括号包装不改变 Rust 字段语义，不能据此把数字当成字符串或改用其它编码格式。
    let mut inner = ty;
    loop {
        inner = match inner {
            Type::Group(group) => &group.elem,
            Type::Paren(paren) => &paren.elem,
            _ => break,
        };
    }
    if let Type::Path(tp) = inner {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "f64" || seg.ident == "f32";
        }
    }
    false
}

/// 业务作用：接收 RedisDocument 派生声明，将语法和文档映射错误定位为调用处的编译诊断。
///
/// 参数说明：
/// - `input`：包含结构体、字段类型和 rs 属性的原始派生输入。
///
/// 返回：成功生成 RedisDocument 实现；输入解析或映射校验失败时生成 compile_error，不连接 Redis 或执行运行期元数据校验。
#[proc_macro_derive(RedisDocument, attributes(rs))]
pub fn derive_redis_document(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// 业务作用：把具名结构体映射为静态文档元数据、业务键片段和全部字段的 HASH 转换实现。
///
/// 参数说明：
/// - `input`：待展开的结构体语法，字段类型用于数值识别并决定生成代码的转换要求。
///
/// 返回：依赖路径、结构体形态、必填且不重复的属性、索引类型与选项适配、唯一 ID、分组顺序及占位符通过校验时生成实现；配置不满足这些条件时返回带源码位置的错误。字段按 Rust 名称持久化，不解释 serde 重命名；完整 DocMeta 校验由运行期绑定执行。
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

    // ── 运行时根路径发现(nasa-macro-support)：
    //    直接依赖 nadis → `::nadis`(含重命名);纯门面消费者 →
    //    `::nasa::redis`;都没有 → 编译错并给出依赖指引。硬编码 ::nadis 在
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
    let mut struct_attributes = std::collections::HashSet::new();
    for attr in &input.attrs {
        if !attr.path().is_ident("rs") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            // 同一布局属性只能有一个来源，不能让声明顺序悄然改变索引或持久化键。
            if let Some(name) = meta.path.get_ident() {
                if !struct_attributes.insert(name.to_string()) {
                    return Err(meta.error(format!("重复的 rs 结构体属性: {name}")));
                }
            }
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
        // r# 只用于 Rust 关键字转义，持久化字段和占位符使用实际名称；生成访问代码保留原始 Ident。
        let name = f.ident.as_ref().unwrap().to_string();
        let name = name.strip_prefix("r#").unwrap_or(&name).to_owned();
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
        let mut field_attributes = std::collections::HashSet::new();
        for attr in &f.attrs {
            if !attr.path().is_ident("rs") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                // 字段选项跨 rs 属性块共同生效，重复项必须拒绝，避免覆盖先前的业务约束。
                if let Some(name) = meta.path.get_ident() {
                    if !field_attributes.insert(name.to_string()) {
                        return Err(meta.error(format!("重复的 rs 字段属性: {name}")));
                    }
                }
                // 一个字段只能映射一种索引语义，不能因后声明的类型而丢弃已有查询约定。
                if info.kind.is_some()
                    && (meta.path.is_ident("tag")
                        || meta.path.is_ident("text")
                        || meta.path.is_ident("numeric")
                        || meta.path.is_ident("geo"))
                {
                    return Err(meta.error("每个字段只能声明一种索引类型(tag/text/numeric/geo)"));
                }
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
        // 选项必须能进入对应的字段元数据；静默忽略会让声明的查询或索引约束失效。
        for (option, allowed) in [
            (
                "sortable",
                matches!(
                    &info.kind,
                    Some(FieldKind::Tag | FieldKind::Text | FieldKind::Numeric)
                ),
            ),
            ("weight", matches!(&info.kind, Some(FieldKind::Text))),
            ("separator", matches!(&info.kind, Some(FieldKind::Tag))),
            ("casesensitive", matches!(&info.kind, Some(FieldKind::Tag))),
            ("nostem", matches!(&info.kind, Some(FieldKind::Text))),
            ("phonetic", matches!(&info.kind, Some(FieldKind::Text))),
            (
                "noindex",
                matches!(&info.kind, Some(FieldKind::Text | FieldKind::Numeric)),
            ),
            (
                "suffix",
                matches!(&info.kind, Some(FieldKind::Tag | FieldKind::Text)),
            ),
            ("alias", info.kind.is_some()),
            ("json_path", info.kind.is_some()),
        ] {
            if field_attributes.contains(option) && !allowed {
                return Err(syn::Error::new_spanned(
                    f,
                    format!("rs 字段属性 {option} 不适用于当前字段索引类型"),
                ));
            }
        }
        // order 只决定数组分组顺序，不能接受一个实际上不会参与键布局的顺序声明。
        if field_attributes.contains("order") && !info.array_key {
            return Err(syn::Error::new_spanned(
                f,
                "order 必须与 array_key 同时声明",
            ));
        }
        fields.push(info);
    }

    // 单一 ID 决定文档身份；分组顺序必须明确，避免同一声明形成不确定的持久化键。
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
    let id_ident = named.named[id_field.decl_idx].ident.as_ref().unwrap();

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
        .map(|f| named.named[f.decl_idx].ident.clone().unwrap())
        .collect();

    // ── placeholder_parts():展开期解析 prefix 的 {name} 并匹配结构体字段——
    //    字段名不存在时在编译期拒绝，避免运行时构造错误的持久化键──
    let placeholder_names =
        parse_placeholders(&prefix).map_err(|msg| syn::Error::new_spanned(input, msg))?;
    let mut placeholder_idents = Vec::new();
    for pn in &placeholder_names {
        let field = fields
            .iter()
            .find(|field| &field.name == pn)
            .ok_or_else(|| {
                // 未找到来源字段时不能生成其它动态片段，否则会改变持久化键的业务归属。
                syn::Error::new_spanned(input, format!("prefix 占位符 {{{pn}}} 不是结构体字段"))
            })?;
        placeholder_idents.push(named.named[field.decl_idx].ident.clone().unwrap());
    }

    // ── to_fields() / from_fields():全部字段(含未标注)──
    // f64/f32 字段使用兼容格式化函数对齐既有系统的 Double.toString（整数补 `.0`、科学计数法 E），
    // 否则 HASH 存储跨语言字节分叉。其余类型仍 `to_string`。
    let to_pairs: Vec<TokenStream2> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let ident = named.named[f.decl_idx].ident.as_ref().unwrap();
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
            let ident = named.named[f.decl_idx].ident.as_ref().unwrap();
            let ty = &f.ty;
            quote! {
                #ident: match fields.get(#name) {
                    Some(v) => v.parse::<#ty>().map_err(|_| {
                        // 转换错误由业务类型定义，格式化可能携带字段原值或执行额外代码；这里只保留静态字段名。
                        #root::NasaRedisError::Codec(
                            format!("字段 {} 解析失败", #name),
                        )
                    })?,
                    None => ::core::default::Default::default(),
                }
            }
        })
        .collect();

    Ok(quote! {
        impl #root::RedisDocument for #struct_name {
            /// 业务作用：提供此文档类型的静态键布局与索引字段定义，供执行器绑定和路由使用。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：首次调用构造并缓存 DocMeta，后续返回同一引用；此入口不连接 Redis，也不代替执行器的元数据校验。
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

            /// 业务作用：将已标注的业务 ID 转为键路由和子文档匹配使用的字符串。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：返回 ID 字段的 Display 表示，不分配新 ID、不验证数值有限性或序列化字段的一致性。
            fn id(&self) -> ::std::string::String {
                self.#id_ident.to_string()
            }

            /// 业务作用：按声明顺序编码全部 HASH 字段，保留未建立索引和参与键派生的业务数据。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：返回以 Rust 字段名为键的字符串对；f32/f64 按兼容 Double 格式编码，其余使用 Display。不应用 serde 字段属性。
            fn to_fields(&self) -> ::std::vec::Vec<(::std::string::String, ::std::string::String)> {
                vec![#(#to_pairs),*]
            }

            /// 业务作用：从 HASH 字段恢复业务文档，使用各字段类型的 FromStr 和缺省值约定。
            ///
            /// 参数说明：
            /// - `fields`：按 Rust 字段名组织的 Redis HASH 字符串值，额外字段被忽略。
            ///
            /// 返回：已存在字段全部解析成功时构造文档，缺失字段使用 Default；解析失败返回仅包含字段名的 Codec，不格式化原始值或自定义转换错误；自定义解析、缺省值和析构的展开仍向上传播。
            fn from_fields(
                fields: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> #root::error::Result<Self> {
                Ok(Self {
                    #(#from_inits),*
                })
            }

            /// 业务作用：按 prefix 中占位符的出现顺序提供文档分组值，供键布局统一派生。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：返回对应字段的 Display 表示，重复占位符重复取值；此入口不校验动态值中的保留字符。
            fn placeholder_parts(&self) -> ::std::vec::Vec<::std::string::String> {
                vec![#(self.#placeholder_idents.to_string()),*]
            }

            /// 业务作用：按 array_key 的 order 顺序提供数组业务分组，供普通数组和桶键派生。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：返回已标注分组字段的 Display 表示，没有标注时返回空列表；值的合法性由键派生入口校验。
            fn array_key_parts(&self) -> ::std::vec::Vec<::std::string::String> {
                vec![#(self.#array_key_idents.to_string()),*]
            }
        }
    })
}

/// 业务作用：识别 ID 字段的数值类型语法，保持宏生成结构体与直接声明的比较值类型一致。
///
/// 参数说明：
/// - `ty`：派生宏读取到的字段类型语法树，可能包含透明分组或括号。
///
/// 返回：类型去除 Group/Paren 后的路径末段是受支持的整数或浮点名称时返回 true，其余返回 false；不解析类型别名，也不校验实际 ID 值的有限性或精度。
fn is_numeric_type(ty: &Type) -> bool {
    // 宏传入的类型和括号包装不改变 Rust 字段语义，不能据此把数字当成字符串或改用其它编码格式。
    let mut inner = ty;
    loop {
        inner = match inner {
            Type::Group(group) => &group.elem,
            Type::Paren(paren) => &paren.elem,
            _ => break,
        };
    }
    let Type::Path(p) = inner else {
        return false;
    };
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

/// 业务作用：在派生展开期提取持久化键模板中的动态字段，保持与 DocMeta 的括号语法一致。
///
/// 参数说明：
/// - `prefix`：可包含非嵌套、非空名称占位符的 Redis 文档键前缀。
///
/// 返回：成功按出现顺序返回名称，重复名称保留；孤立右括号、未闭合、空名称或嵌套左括号返回错误。此函数不验证名称是否对应结构体字段，也不判断生成键是否具有唯一边界。
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
