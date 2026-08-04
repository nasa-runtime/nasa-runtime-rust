//! 长连接协议字节派生宏。
//!
//! 根据字段 tag 和类型生成 VARINT_TLV、BITPACK_TLV 编解码实现，供消息信封和控制帧复用。
// ============================================================================
// proto-derive —— #[derive(ProtocolBytes)] 派生宏。
//
// 从 proto 手写 codec 提炼:读字段上的 #[proto(tag = N)],按 Rust 字段类型分类
// (Option<String>/Option<Vec<u8>>/Option<Vec<Option<String>>> 为可空引用字段;
//  i64/i32/i8/bool 为 primitive,恒编码),生成 VARINT_TLV + BITPACK_TLV 的
// encode/decode 与 `WireCodec` impl,与手写逐字节一致(golden 对拍验证)。
//
// 生成代码用 `crate::__rt::*` 路径(仅服务于 proto 内部 schema;wire 规范见 架构说明 附录 A)。
// ============================================================================

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

/// 字段的 wire 类别。
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,      // Option<String>      → LD,可空
    Bytes,    // Option<Vec<u8>>     → LD,可空
    StrArray, // Option<Vec<Option<String>>> → LD,可空
    I64,      // i64                 → VARINT zigzag,恒编码
    I32,      // i32                 → VARINT zigzag,恒编码
    I8,       // i8                  → VARINT zigzag,恒编码
    Bool,     // bool                → VARINT 裸,恒编码
}

impl Kind {
    /// 业务作用: 判断字段是否可为空；用于派生编码时选择空值位图策略。
    fn nullable(self) -> bool {
        matches!(self, Kind::Str | Kind::Bytes | Kind::StrArray)
    }

    /// 业务作用: 判断字段是否使用 length-delimited wire type，供 key 编码和 wire type 校验复用。
    fn is_len(self) -> bool {
        self.nullable()
    }
}

/// 保存字段派生信息；用于生成编码、解码和空值处理代码。
struct FieldInfo {
    ident: syn::Ident,
    tag: u64,
    kind: Kind,
}

/// 业务作用: 展开协议字节派生宏；用于为结构体生成二进制编解码实现。
///
/// # 参数
///
/// - `input`: 被 `#[derive(ProtocolBytes)]` 标注的结构体定义 token stream。
#[proc_macro_derive(ProtocolBytes, attributes(proto))]
pub fn derive_protocol_bytes(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = ast.ident.clone();

    let fields = match collect_fields(&ast) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    let n = fields.len();
    let bm_bytes: usize = if n <= 8 {
        1
    } else if n <= 16 {
        2
    } else if n <= 32 {
        4
    } else {
        8
    };

    // ── 各模式分片 ──
    let mut bitpack_presence = Vec::new();
    let mut bitpack_write = Vec::new();
    let mut bitpack_read = Vec::new();
    let mut varint_write = Vec::new();
    let mut varint_decode_arms = Vec::new();

    for (i, f) in fields.iter().enumerate() {
        let id = &f.ident;
        let tag = f.tag;
        let bit = i as u64;

        // BITPACK 置位
        if f.kind.nullable() {
            bitpack_presence.push(quote! { if self.#id.is_some() { bm |= 1u64 << #bit; } });
        } else {
            bitpack_presence.push(quote! { bm |= 1u64 << #bit; }); // primitive 恒编码
        }

        // 写值(BITPACK / VARINT 共用 value 部分)
        let write_val = match f.kind {
            Kind::Str => quote! { w.val_str(x) },
            Kind::Bytes => quote! { w.val_bytes(x) },
            Kind::StrArray => quote! { w.val_str_array(x) },
            Kind::I64 => quote! { w.val_i64(self.#id) },
            Kind::I32 => quote! { w.val_i64(self.#id as i64) },
            Kind::I8 => quote! { w.val_i64(self.#id as i64) },
            Kind::Bool => quote! { w.val_bool(self.#id) },
        };
        if f.kind.nullable() {
            bitpack_write.push(quote! { if let Some(x) = &self.#id { #write_val; } });
        } else {
            bitpack_write.push(quote! { #write_val; });
        }

        // VARINT:写 key + value(可空字段缺则跳;primitive 恒写)
        let wt = if f.kind.is_len() {
            quote! { crate::__rt::WT_LEN }
        } else {
            quote! { crate::__rt::WT_VARINT }
        };
        if f.kind.nullable() {
            varint_write
                .push(quote! { if let Some(x) = &self.#id { w.key(#tag, #wt); #write_val; } });
        } else {
            varint_write.push(quote! { w.key(#tag, #wt); #write_val; });
        }

        // 读值
        let read_val = match f.kind {
            Kind::Str => quote! { ::core::option::Option::Some(r.read_str()?) },
            Kind::Bytes => quote! { ::core::option::Option::Some(r.read_bytes()?) },
            Kind::StrArray => quote! { ::core::option::Option::Some(r.read_str_array()?) },
            Kind::I64 => quote! { r.read_i64()? },
            Kind::I32 => quote! { r.read_i64()? as i32 },
            Kind::I8 => quote! { r.read_i64()? as i8 },
            Kind::Bool => quote! { r.read_bool()? },
        };

        // BITPACK 读(按位)
        bitpack_read.push(quote! { if bm & (1u64 << #bit) != 0 { out.#id = #read_val; } });

        // VARINT decode 分支(check wiretype)
        let exp_wt = wt.clone();
        varint_decode_arms.push(quote! {
            #tag => { crate::__rt::check_wt(#tag, #exp_wt, wt)?; out.#id = #read_val; }
        });
    }

    let expanded = quote! {
        impl #name {
            /// 业务作用: 生成位图编码方法；用于紧凑表示布尔字段集合。
            fn __encode_bitpack(&self) -> ::std::vec::Vec<u8> {
                let mut bm: u64 = 0;
                #(#bitpack_presence)*
                let mut w = crate::__rt::Writer::new();
                w.bitmap(bm, #bm_bytes);
                #(#bitpack_write)*
                w.buf
            }

            /// 业务作用: 生成位图解码方法；用于从紧凑字节恢复布尔字段集合。
            ///
            /// # 参数
            /// - `data`: BITPACK_TLV 模式下的协议字节。
            fn __decode_bitpack(data: &[u8]) -> crate::Result<Self> {
                let mut r = crate::__rt::Reader::new(data);
                let bm = r.read_bitmap(#bm_bytes)?;
                let valid: u64 = if #n >= 64 { u64::MAX } else { (1u64 << #n) - 1 };
                if bm & !valid != 0 {
                    return ::core::result::Result::Err(crate::CodecError::BadBitmap);
                }
                let mut out = Self::default();
                #(#bitpack_read)*
                r.expect_end()?;
                ::core::result::Result::Ok(out)
            }

            /// 业务作用: 生成变长整数编码方法；用于压缩数值字段的线路体积。
            fn __encode_varint(&self) -> ::std::vec::Vec<u8> {
                let mut w = crate::__rt::Writer::new();
                #(#varint_write)*
                w.buf
            }

            /// 业务作用: 生成变长整数解码方法；用于从线路字节恢复数值字段。
            ///
            /// # 参数
            /// - `data`: VARINT_TLV 模式下的协议字节。
            fn __decode_varint(data: &[u8]) -> crate::Result<Self> {
                let mut r = crate::__rt::Reader::new(data);
                let mut out = Self::default();
                while r.remaining() > 0 {
                    let key = r.read_varint()?;
                    let tag = key >> 3;
                    let wt = key & 7;
                    match tag {
                        #(#varint_decode_arms)*
                        _ => r.skip_value(wt)?,
                    }
                }
                ::core::result::Result::Ok(out)
            }
        }

        impl crate::WireCodec for #name {
            /// 业务作用: 按协议模式把 schema 编码为线路字节，统一拒绝尚未实现的固定布局模式。
            ///
            /// # 参数
            /// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
            fn encode(&self, mode: crate::Mode) -> crate::Result<::std::vec::Vec<u8>> {
                match mode {
                    crate::Mode::BitpackTlv => ::core::result::Result::Ok(self.__encode_bitpack()),
                    crate::Mode::VarintTlv => ::core::result::Result::Ok(self.__encode_varint()),
                    crate::Mode::JsonBytes => crate::__rt::json_to_vec(self),
                    crate::Mode::FastFixed => ::core::result::Result::Err(crate::CodecError::Unsupported("FAST_FIXED")),
                }
            }

            /// 业务作用: 按协议模式把线路字节恢复为 schema，统一拒绝尚未实现的固定布局模式。
            ///
            /// # 参数
            /// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
            /// - `data`: 指定编码模式下的入站协议字节。
            fn decode(mode: crate::Mode, data: &[u8]) -> crate::Result<Self> {
                match mode {
                    crate::Mode::BitpackTlv => Self::__decode_bitpack(data),
                    crate::Mode::VarintTlv => Self::__decode_varint(data),
                    crate::Mode::JsonBytes => crate::__rt::json_from_slice(data),
                    crate::Mode::FastFixed => ::core::result::Result::Err(crate::CodecError::Unsupported("FAST_FIXED")),
                }
            }
        }
    };
    expanded.into()
}

/// 业务作用: 收集字段(按 tag 升序),解析 #[proto(tag = N)] 与类型分类。
///
/// # 参数
/// - `ast`: 派生宏解析出的结构体语法树。
fn collect_fields(ast: &DeriveInput) -> syn::Result<Vec<FieldInfo>> {
    let data = match &ast.data {
        Data::Struct(s) => s,
        _ => return Err(syn::Error::new_spanned(ast, "ProtocolBytes 只支持 struct")),
    };
    let named = match &data.fields {
        Fields::Named(n) => &n.named,
        _ => return Err(syn::Error::new_spanned(ast, "ProtocolBytes 需要具名字段")),
    };

    let mut out = Vec::new();
    for field in named {
        let ident = field.ident.clone().unwrap();
        let tag = parse_tag(field)?;
        let kind = classify(&field.ty)
            .ok_or_else(|| syn::Error::new_spanned(&field.ty, "ProtocolBytes 不支持的字段类型"))?;
        if tag == 0 {
            return Err(syn::Error::new_spanned(field, "#[proto(tag)] 必须 > 0"));
        }
        // key 的编码是 `varint(tag << 3 | wire_type)`：tag 超过 2^61 时 `tag << 3` 静默截断，
        // 该字段会以另一个 tag 上线、解码端匹配不到而被 skip_value 吃掉——
        // 往返成功但值丢失（实测 tag=2^61 时字段解回默认值）。这里在编译期拦下。
        const MAX_TAG: u64 = u64::MAX >> 3;
        if tag > MAX_TAG {
            return Err(syn::Error::new_spanned(
                field,
                format!(
                    "#[proto(tag)] 必须 <= {MAX_TAG}：key 编码为 tag<<3|wire_type，再大会静默截断"
                ),
            ));
        }
        out.push(FieldInfo { ident, tag, kind });
    }
    out.sort_by_key(|f| f.tag);
    if out.len() > 64 {
        return Err(syn::Error::new_spanned(ast, "ProtocolBytes 最多 64 字段"));
    }
    // tag 唯一
    for w in out.windows(2) {
        if w[0].tag == w[1].tag {
            return Err(syn::Error::new_spanned(
                ast,
                format!("tag 冲突: {}", w[0].tag),
            ));
        }
    }
    Ok(out)
}

/// 业务作用: 从字段的 `#[proto(tag = N)]` 属性读取 wire tag，缺失时给出编译期错误。
///
/// # 参数
/// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
fn parse_tag(field: &syn::Field) -> syn::Result<u64> {
    for attr in &field.attrs {
        if !attr.path().is_ident("proto") {
            continue;
        }
        let mut tag = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("tag") {
                let v: syn::LitInt = meta.value()?.parse()?;
                tag = Some(v.base10_parse()?);
            }
            Ok(())
        })?;
        if let Some(t) = tag {
            return Ok(t);
        }
    }
    Err(syn::Error::new_spanned(field, "字段缺 #[proto(tag = N)]"))
}

/// 业务作用: Rust 类型 → Kind。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn classify(ty: &Type) -> Option<Kind> {
    if let Some(inner) = option_inner(ty) {
        // 可空引用字段
        if is_path(inner, "String") {
            return Some(Kind::Str);
        }
        if let Some(elem) = vec_inner(inner) {
            if is_path(elem, "u8") {
                return Some(Kind::Bytes);
            }
            if option_inner(elem)
                .map(|e| is_path(e, "String"))
                .unwrap_or(false)
            {
                return Some(Kind::StrArray);
            }
        }
        return None;
    }
    // primitive(恒编码)
    if is_path(ty, "i64") {
        return Some(Kind::I64);
    }
    if is_path(ty, "i32") {
        return Some(Kind::I32);
    }
    if is_path(ty, "i8") {
        return Some(Kind::I8);
    }
    if is_path(ty, "bool") {
        return Some(Kind::Bool);
    }
    None
}

/// 业务作用: `Option<T>` → Some(T)。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn option_inner(ty: &Type) -> Option<&Type> {
    generic_inner(ty, "Option")
}

/// 业务作用: `Vec<T>` → Some(T)。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
fn vec_inner(ty: &Type) -> Option<&Type> {
    generic_inner(ty, "Vec")
}

/// 业务作用: 读取包装类型的内部泛型；用于识别 Option 和 Vec 的字段元素类型。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
/// - `wrapper`: 待识别的 Option 或 Vec 类型包装器。
fn generic_inner<'a>(ty: &'a Type, wrapper: &str) -> Option<&'a Type> {
    let Type::Path(p) = ty else { return None };
    let seg = p.path.segments.last()?;
    if seg.ident != wrapper {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// 业务作用: 判断 Rust 类型路径的末段是否匹配目标名称，供字段类型白名单识别复用。
///
/// # 参数
/// - `ty`: Rust 类型 AST,用于宏期类型判定。
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn is_path(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(p) if p.path.segments.last().map(|s| s.ident == name).unwrap_or(false))
}
