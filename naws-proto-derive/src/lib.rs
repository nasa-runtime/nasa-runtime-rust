//! 根据 schema 的 tag 与字段类型派生 ProtocolBytes 编解码，支持依赖别名和门面入口。

use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

#[derive(Clone, Copy)]
enum Kind {
    Str,
    Bytes,
    StrArray,
    I8,
    I16,
    I32,
    I64,
    U16,
    Bool,
    F32,
    F64,
}

impl Kind {
    /// 业务作用：区分可空引用与固定基础类型，确定存在位及固定布局资格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：字符串、字节和字符串数组需要存在位，其它字段始终编码。
    fn nullable(self) -> bool {
        matches!(self, Self::Str | Self::Bytes | Self::StrArray)
    }

    /// 业务作用：确定 Java ProtocolBytes 对应的 wire type。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：浮点为定宽、引用为长度分隔，其余为变长整数。
    fn wire_type(self) -> u64 {
        match self {
            Self::F32 => 5,
            Self::F64 => 1,
            Self::Str | Self::Bytes | Self::StrArray => 2,
            _ => 0,
        }
    }

    /// 业务作用：编译固定布局的总长度并拒绝可空或变长字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：基础类型的固定字节数；引用类型没有合法固定长度。
    fn fixed_size(self) -> Option<usize> {
        match self {
            Self::Bool | Self::I8 => Some(1),
            Self::I16 | Self::U16 => Some(2),
            Self::I32 | Self::F32 => Some(4),
            Self::I64 | Self::F64 => Some(8),
            _ => None,
        }
    }
}

struct FieldInfo {
    ident: syn::Ident,
    tag: u64,
    kind: Kind,
}

/// 业务作用：为具名业务 schema 生成按 tag 排序的 ProtocolBytes 实现。
///
/// 参数说明：
///
/// - input：带字段协议属性的结构体定义。
///
/// 返回：四种模式的类型化入口；类型、tag 或依赖路径非法时产生编译错误。
#[proc_macro_derive(ProtocolBytes, attributes(proto))]
pub fn derive_protocol_bytes(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    match expand(&ast) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// 业务作用：冻结字段布局并生成有界读写分支，避免运行期反射。
///
/// 参数说明：
///
/// - ast：已经解析的业务结构体。
///
/// 返回：适用于已声明类型的编解码实现；不为非法固定布局生成可执行旁路。
fn expand(ast: &DeriveInput) -> syn::Result<Tokens> {
    let runtime = runtime_path(ast)?;
    let name = &ast.ident;
    if !ast.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &ast.generics,
            "ProtocolBytes requires a concrete schema",
        ));
    }
    let fields = collect_fields(ast)?;
    let n = fields.len();
    let valid_bitmap = if n == 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    };
    let bm_bytes: usize = if n <= 8 {
        1
    } else if n <= 16 {
        2
    } else if n <= 32 {
        4
    } else {
        8
    };
    let mut presence = Vec::new();
    let mut bitpack_write = Vec::new();
    let mut bitpack_read = Vec::new();
    let mut varint_write = Vec::new();
    let mut varint_read = Vec::new();
    let mut fixed_write = Vec::new();
    let mut fixed_read = Vec::new();
    let fixed_length: Option<usize> = fields.iter().map(|field| field.kind.fixed_size()).sum();

    for (index, field) in fields.iter().enumerate() {
        let id = &field.ident;
        let tag = field.tag;
        let bit = index as u64;
        let wt = field.kind.wire_type();
        let write = match field.kind {
            Kind::Str => quote! { w.val_str(x) },
            Kind::Bytes => quote! { w.val_bytes(x) },
            Kind::StrArray => quote! { w.val_str_array(x) },
            Kind::I8 | Kind::I16 | Kind::I32 | Kind::I64 => quote! { w.val_i64(self.#id as i64) },
            Kind::U16 => quote! { w.val_unsigned(self.#id as u64) },
            Kind::Bool => quote! { w.val_bool(self.#id) },
            // Java TLV 浮点写入采用规范 NaN 位模式，固定布局则保留基础值的原始位。
            Kind::F32 => {
                quote! {
                    w.buf.extend_from_slice(&(if self.#id.is_nan() {
                        0x7fc00000u32
                    } else {
                        self.#id.to_bits()
                    }).to_le_bytes())
                }
            }
            Kind::F64 => {
                quote! {
                    w.buf.extend_from_slice(&(if self.#id.is_nan() {
                        0x7ff8000000000000u64
                    } else {
                        self.#id.to_bits()
                    }).to_le_bytes())
                }
            }
        };
        let read = match field.kind {
            Kind::Str => quote! { Some(r.read_str()?) },
            Kind::Bytes => quote! { Some(r.read_bytes()?) },
            Kind::StrArray => quote! { Some(r.read_str_array()?) },
            Kind::I8 => {
                quote! { <::core::primitive::i8 as ::core::convert::TryFrom<i64>>::try_from(r.read_i64()?).map_err(|_| #runtime::CodecError::VarintOverflow)? }
            }
            Kind::I16 => {
                quote! { <::core::primitive::i16 as ::core::convert::TryFrom<i64>>::try_from(r.read_i64()?).map_err(|_| #runtime::CodecError::VarintOverflow)? }
            }
            Kind::I32 => {
                quote! { <::core::primitive::i32 as ::core::convert::TryFrom<i64>>::try_from(r.read_i64()?).map_err(|_| #runtime::CodecError::VarintOverflow)? }
            }
            Kind::I64 => quote! { r.read_i64()? },
            Kind::U16 => {
                quote! { <::core::primitive::u16 as ::core::convert::TryFrom<u64>>::try_from(r.read_varint()?).map_err(|_| #runtime::CodecError::VarintOverflow)? }
            }
            Kind::Bool => quote! { r.read_bool()? },
            Kind::F32 => quote! { f32::from_le_bytes(r.read_fixed::<4>()?) },
            Kind::F64 => quote! { f64::from_le_bytes(r.read_fixed::<8>()?) },
        };
        if field.kind.nullable() {
            presence.push(quote! {
                if self.#id.is_some() {
                    bm |= 1u64 << #bit;
                }
            });
            bitpack_write.push(quote! {
                if let Some(x) = &self.#id {
                    #write;
                }
            });
            varint_write.push(quote! {
                if let Some(x) = &self.#id {
                    w.key(#tag, #wt);
                    #write;
                }
            });
        } else {
            presence.push(quote! {
                bm |= 1u64 << #bit;
            });
            bitpack_write.push(quote! {
                #write;
            });
            varint_write.push(quote! {
                w.key(#tag, #wt);
                #write;
            });
        }
        bitpack_read.push(quote! {
            if bm & (1u64 << #bit) != 0 {
                out.#id = #read;
            }
        });
        varint_read.push(quote! {
            #tag => {
                #runtime::__rt::check_wt(#tag, #wt, wt)?;
                out.#id = #read;
            }
        });
        if let Some(size) = field.kind.fixed_size() {
            if matches!(field.kind, Kind::Bool) {
                fixed_write.push(quote! {
                    bytes.push(u8::from(self.#id));
                });
                fixed_read.push(quote! {
                    out.#id = r.read_fixed::<1>()?[0] != 0;
                });
            } else {
                let ty = match field.kind {
                    Kind::I8 => quote!(i8),
                    Kind::I16 => quote!(i16),
                    Kind::I32 => quote!(i32),
                    Kind::I64 => quote!(i64),
                    Kind::U16 => quote!(u16),
                    Kind::F32 => quote!(f32),
                    Kind::F64 => quote!(f64),
                    _ => unreachable!(),
                };
                fixed_write.push(quote! {
                    bytes.extend_from_slice(&self.#id.to_le_bytes());
                });
                fixed_read.push(quote! {
                    out.#id = #ty::from_le_bytes(r.read_fixed::<#size>()?);
                });
            }
        }
    }
    let (encode_fixed, decode_fixed) = match fixed_length {
        Some(size) => (
            quote! {
                let mut bytes = ::std::vec::Vec::with_capacity(#size);
                #(#fixed_write)*
                Ok(bytes)
            },
            quote! {
                if data.len() != #size {
                    return Err(#runtime::CodecError::LengthExceeds);
                }
                let mut r = #runtime::__rt::Reader::new(data);
                let mut out = Self::default();
                #(#fixed_read)*
                Ok(out)
            },
        ),
        None => (
            quote! { Err(#runtime::CodecError::Unsupported("FAST_FIXED requires non-null primitive fields")) },
            quote! { Err(#runtime::CodecError::Unsupported("FAST_FIXED requires non-null primitive fields")) },
        ),
    };
    Ok(quote! {
        impl #name {
            /// 业务作用：按 schema 顺序生成存在位图和字段值。
            /// 参数说明: 无。
            /// 返回：完整 BITPACK_TLV 字节，缺省引用不占字段值区域。
            fn __encode_bitpack(&self) -> ::std::vec::Vec<u8> {
                let mut bm = 0u64;
                #(#presence)*
                let mut w = #runtime::__rt::Writer::new();
                w.bitmap(bm, #bm_bytes);
                #(#bitpack_write)*
                w.buf
            }

            /// 业务作用：依据固定 schema 位图恢复字段，并拒绝布局外的位和剩余字节。
            /// 参数说明：
            ///
            /// - data：完整 BITPACK_TLV 字节。
            ///
            /// 返回：成功恢复的业务值；非法长度、类型或布局失败。
            fn __decode_bitpack(data: &[u8]) -> #runtime::Result<Self> {
                let mut r = #runtime::__rt::Reader::new(data);
                let bm = r.read_bitmap(#bm_bytes)?;
                let valid: u64 = #valid_bitmap;
                if bm & !valid != 0 {
                    return Err(#runtime::CodecError::BadBitmap);
                }
                let mut out = Self::default();
                #(#bitpack_read)*
                r.expect_end()?;
                Ok(out)
            }

            /// 业务作用：按 tag 顺序编码带 wire type 的字段。
            /// 参数说明: 无。
            /// 返回：完整 VARINT_TLV 字节。
            fn __encode_varint(&self) -> ::std::vec::Vec<u8> {
                let mut w = #runtime::__rt::Writer::new();
                #(#varint_write)*
                w.buf
            }

            /// 业务作用：校验已知字段类型并在有界输入中跳过未知字段。
            /// 参数说明：
            ///
            /// - data：完整 VARINT_TLV 字节。
            ///
            /// 返回：已恢复的业务值；窄化溢出、非法 wire type 或截断失败。
            fn __decode_varint(data: &[u8]) -> #runtime::Result<Self> {
                let mut r = #runtime::__rt::Reader::new(data);
                let mut out = Self::default();
                while r.remaining() > 0 {
                    let key = r.read_varint()?;
                    let tag = key >> 3;
                    let wt = key & 7;
                    match tag {
                        #(#varint_read)*
                        _ => r.skip_value(wt)?
                    }
                }
                Ok(out)
            }
        }

        impl #runtime::WireCodec for #name {
            /// 业务作用：按调用方指定的模式编码 schema。
            /// 参数说明：
            ///
            /// - mode：本次编解码使用的 ProtocolBytes 模式。
            ///
            /// 返回：完整业务字节；不适用的固定布局明确拒绝。
            fn encode(&self, mode: #runtime::Mode) -> #runtime::Result<::std::vec::Vec<u8>> {
                match mode {
                    #runtime::Mode::JsonBytes => #runtime::__rt::json_to_vec(self),
                    #runtime::Mode::VarintTlv => Ok(self.__encode_varint()),
                    #runtime::Mode::BitpackTlv => Ok(self.__encode_bitpack()),
                    #runtime::Mode::FastFixed => {
                        #encode_fixed
                    }
                }
            }

            /// 业务作用：按指定模式解释 payload，不通过备用解码器猜测格式。
            /// 参数说明：
            ///
            /// - mode：本次编解码使用的 ProtocolBytes 模式。
            /// - data：与完整 schema 对应的业务字节。
            ///
            /// 返回：合法业务值；布局、类型或长度不符时失败。
            fn decode(mode: #runtime::Mode, data: &[u8]) -> #runtime::Result<Self> {
                match mode {
                    #runtime::Mode::JsonBytes => #runtime::__rt::json_from_slice(data),
                    #runtime::Mode::VarintTlv => Self::__decode_varint(data),
                    #runtime::Mode::BitpackTlv => Self::__decode_bitpack(data),
                    #runtime::Mode::FastFixed => {
                        #decode_fixed
                    }
                }
            }
        }
    })
}

/// 业务作用：优先使用消费工程明确指定的协议入口，否则解析已知依赖及其别名。
///
/// 参数说明：
/// - `ast`：业务类型及可选 crate_path 容器属性。
///
/// 返回：唯一协议路径；重复路径、非法属性或没有可用协议入口时产生编译错误。
fn runtime_path(ast: &DeriveInput) -> syn::Result<Tokens> {
    use proc_macro_crate::{crate_name, FoundCrate};
    let mut explicit = None;
    for attribute in ast
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("proto"))
    {
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("crate_path") {
                return Err(meta.error("unsupported ProtocolBytes container attribute"));
            }
            if explicit.is_some() {
                return Err(meta.error("duplicate ProtocolBytes crate_path"));
            }
            let value: syn::LitStr = meta.value()?.parse()?;
            explicit = Some(value.parse::<syn::Path>()?);
            Ok(())
        })?;
    }
    // 显式路径由消费工程选择，通用派生器不依赖任何下游产品的包名或结构。
    if let Some(path) = explicit {
        return Ok(quote!(#path));
    }
    for package in ["naws-proto", "naws", "nasa"] {
        if let Ok(found) = crate_name(package) {
            let base = match found {
                FoundCrate::Itself => quote!(crate),
                FoundCrate::Name(name) => {
                    let id = syn::Ident::new(&name, proc_macro2::Span::call_site());
                    quote!(::#id)
                }
            };
            return Ok(match package {
                "naws" => quote!(#base::proto),
                "nasa" => quote!(#base::ws::proto),
                _ => base,
            });
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "ProtocolBytes requires a protocol dependency or #[proto(crate_path = \"path::to::protocol\")]",
    ))
}

/// 业务作用：编译 schema 的唯一 tag 序列并验证位图容量。
///
/// 参数说明：
///
/// - ast：被派生的具名结构体。
///
/// 返回：排序后的字段；重复、越界 tag 或不支持的类型拒绝。
fn collect_fields(ast: &DeriveInput) -> syn::Result<Vec<FieldInfo>> {
    let Data::Struct(data) = &ast.data else {
        return Err(syn::Error::new_spanned(
            ast,
            "ProtocolBytes requires a struct",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            ast,
            "ProtocolBytes requires named fields",
        ));
    };
    let mut fields = Vec::new();
    for field in &named.named {
        let tag = parse_tag(field)?;
        if tag == 0 || tag > (u64::MAX >> 3) {
            return Err(syn::Error::new_spanned(
                field,
                "ProtocolBytes tag is outside wire range",
            ));
        }
        let kind = classify(&field.ty).ok_or_else(|| {
            syn::Error::new_spanned(&field.ty, "unsupported ProtocolBytes field type")
        })?;
        fields.push(FieldInfo {
            ident: field.ident.clone().expect("named field"),
            tag,
            kind,
        });
    }
    fields.sort_by_key(|field| field.tag);
    if fields.len() > 64 {
        return Err(syn::Error::new_spanned(
            ast,
            "ProtocolBytes supports at most 64 fields",
        ));
    }
    if fields.windows(2).any(|pair| pair[0].tag == pair[1].tag) {
        return Err(syn::Error::new_spanned(ast, "duplicate ProtocolBytes tag"));
    }
    Ok(fields)
}

/// 业务作用：读取唯一字段 tag，防止重复属性或未知属性改变布局解释。
///
/// 参数说明：
///
/// - field：待解析的字段定义。
///
/// 返回：显式 tag；缺失、重复或未知属性拒绝。
fn parse_tag(field: &syn::Field) -> syn::Result<u64> {
    let mut tag = None;
    for attr in &field.attrs {
        if attr.path().is_ident("proto") {
            attr.parse_nested_meta(|meta| {
                if !meta.path.is_ident("tag") || tag.is_some() {
                    return Err(meta.error("expected one tag attribute"));
                }
                let value: syn::LitInt = meta.value()?.parse()?;
                tag = Some(value.base10_parse()?);
                Ok(())
            })?;
        }
    }
    tag.ok_or_else(|| syn::Error::new_spanned(field, "missing #[proto(tag = N)]"))
}

/// 业务作用：将 Rust 字段精确映射为已支持的协议类型。
///
/// 参数说明：
///
/// - ty：字段类型语法树。
///
/// 返回：可编译的 wire 类型；未知包装或元素类型不猜测转换。
fn classify(ty: &Type) -> Option<Kind> {
    if let Some(inner) = generic_inner(ty, "Option") {
        if is_path(inner, "String") {
            return Some(Kind::Str);
        }
        if let Some(element) = generic_inner(inner, "Vec") {
            if is_path(element, "u8") {
                return Some(Kind::Bytes);
            }
            if generic_inner(element, "Option").is_some_and(|t| is_path(t, "String")) {
                return Some(Kind::StrArray);
            }
        }
        return None;
    }
    for (name, kind) in [
        ("i8", Kind::I8),
        ("i16", Kind::I16),
        ("i32", Kind::I32),
        ("i64", Kind::I64),
        ("u16", Kind::U16),
        ("bool", Kind::Bool),
        ("f32", Kind::F32),
        ("f64", Kind::F64),
    ] {
        if is_path(ty, name) {
            return Some(kind);
        }
    }
    None
}

/// 业务作用：验证容器形状并读取唯一类型参数。
///
/// 参数说明：
///
/// - ty：字段类型语法树。
/// - wrapper：允许的容器名称。
///
/// 返回：唯一类型参数；不是对应容器或泛型参数数目不符时为空。
fn generic_inner<'a>(ty: &'a Type, wrapper: &str) -> Option<&'a Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != wrapper {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match args.args.first()? {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    }
}

/// 业务作用：识别协议允许的基础类型名称。
///
/// 参数说明：
///
/// - ty：字段类型语法树。
/// - name：目标基础类型名称。
///
/// 返回：路径末段匹配时为真，其余类型不按基础类型处理。
fn is_path(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == name))
}
