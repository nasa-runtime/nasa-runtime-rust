//! `#[kafka_consumer]` 无状态异步消费者收集宏。

#![deny(missing_docs)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, Expr, ExprArray, FnArg, GenericArgument, ItemFn, Lit, LitStr, Meta,
    PathArguments, ReturnType, Token, Type,
};

/// 解析后的属性参数。
struct ConsumerArgs {
    /// 订阅 topic 字面量。
    topics: Vec<LitStr>,
    /// 路由事件名字面量。
    event: LitStr,
    /// 显式普通 group。
    group: Option<LitStr>,
    /// 广播 scope；`Some(None)` 表示使用默认 scope。
    broadcast: Option<Option<LitStr>>,
    /// route 确认模式。
    ack: AckArg,
    /// 静态收集目标 client。
    client: LitStr,
}

/// 宏属性中的确认模式。
#[derive(Clone, Copy, PartialEq, Eq)]
enum AckArg {
    /// 回调成功时自动确认。
    Auto,
    /// 业务回调显式确认且最终成功才生效。
    Manual,
}

/// 业务函数参数的四种规范形态。
enum HandlerShape {
    /// 单条 payload-only。
    SinglePayload(Type),
    /// 单条完整记录。
    SingleRecord(Type),
    /// 批量 payload-only。
    BatchPayload(Type),
    /// 批量完整记录。
    BatchRecord(Type),
}

impl HandlerShape {
    /// 返回关联业务消息类型。
    fn message_type(&self) -> &Type {
        match self {
            Self::SinglePayload(ty)
            | Self::SingleRecord(ty)
            | Self::BatchPayload(ty)
            | Self::BatchRecord(ty) => ty,
        }
    }

    /// 判断是否使用批消费者契约。
    fn is_batch(&self) -> bool {
        matches!(self, Self::BatchPayload(_) | Self::BatchRecord(_))
    }

    /// 判断业务参数是否拿得到可信记录确认能力。
    fn receives_record(&self) -> bool {
        matches!(self, Self::SingleRecord(_) | Self::BatchRecord(_))
    }
}

/// 收集无状态异步消费函数。
///
/// 支持 `T`、`KafkaRecord<T>`、`Vec<T>`、`Vec<KafkaRecord<T>>` 四种参数形态。
#[proc_macro_attribute]
pub fn kafka_consumer(attr: TokenStream, item: TokenStream) -> TokenStream {
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let function = parse_macro_input!(item as ItemFn);
    match expand(metas, function) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// 校验属性和函数签名并生成 trait 实现与静态收集项。
///
/// # 参数
///
/// - `metas`: 属性参数列表。
/// - `function`: 被标注的异步 free function。
///
/// # 错误
///
/// 属性、函数限定符或参数形态不符合契约时返回编译错误。
fn expand(metas: Punctuated<Meta, Token![,]>, function: ItemFn) -> syn::Result<TokenStream2> {
    let args = parse_args(metas)?;
    validate_function(&function)?;
    let input = match function
        .sig
        .inputs
        .first()
        .ok_or_else(|| syn::Error::new_spanned(&function.sig, "消费者函数缺少参数"))?
    {
        FnArg::Typed(input) => input,
        FnArg::Receiver(receiver) => {
            return Err(syn::Error::new_spanned(
                receiver,
                "只支持无 receiver 的 free function",
            ));
        }
    };
    let shape = parse_shape(&input.ty)?;
    if args.ack == AckArg::Manual && !shape.receives_record() {
        return Err(syn::Error::new_spanned(
            &input.ty,
            "ack = \"manual\" 必须使用 KafkaRecord<T> 或 Vec<KafkaRecord<T>> 参数",
        ));
    }

    let root = nasa_macro_support::runtime_root("kafka", "nafka")
        .map_err(|message| syn::Error::new_spanned(&function.sig.ident, message))?;
    let function_name = &function.sig.ident;
    let message = shape.message_type();
    let topics = &args.topics;
    let event = &args.event;
    let client = &args.client;
    let ack_mode = match args.ack {
        AckArg::Auto => quote!(#root::AckMode::Auto),
        AckArg::Manual => quote!(#root::AckMode::Manual),
    };
    let group = group_tokens(&args, function_name, &root);
    let invocation = invocation_tokens(&shape, function_name);
    let trait_impl = if shape.is_batch() {
        quote! {
            impl #root::BatchConsumer for __NafkaConsumer {
                type Message = #message;

                /// 返回属性中声明的 topic 列表。
                fn topics(&self) -> ::std::vec::Vec<::std::string::String> {
                    ::std::vec![#(::std::string::ToString::to_string(#topics)),*]
                }

                /// 返回属性中声明的事件名。
                fn event(&self) -> ::std::string::String {
                    ::std::string::ToString::to_string(#event)
                }

                /// 返回属性中声明的 group 解析规则。
                fn group(&self) -> #root::GroupSpec {
                    #group
                }

                /// 返回模块路径与函数名组成的稳定 id。
                fn id(&self) -> &'static str {
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#function_name))
                }

                /// 返回属性中声明的确认模式。
                fn ack_mode(&self) -> #root::AckMode {
                    #ack_mode
                }

                /// 将框架批记录转换为业务函数声明的参数形态并调用。
                async fn consume_batch(
                    &self,
                    records: ::std::vec::Vec<#root::KafkaRecord<Self::Message>>,
                ) -> #root::Result<()> {
                    #invocation
                }
            }
        }
    } else {
        quote! {
            impl #root::SingleConsumer for __NafkaConsumer {
                type Message = #message;

                /// 返回属性中声明的 topic 列表。
                fn topics(&self) -> ::std::vec::Vec<::std::string::String> {
                    ::std::vec![#(::std::string::ToString::to_string(#topics)),*]
                }

                /// 返回属性中声明的事件名。
                fn event(&self) -> ::std::string::String {
                    ::std::string::ToString::to_string(#event)
                }

                /// 返回属性中声明的 group 解析规则。
                fn group(&self) -> #root::GroupSpec {
                    #group
                }

                /// 返回模块路径与函数名组成的稳定 id。
                fn id(&self) -> &'static str {
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#function_name))
                }

                /// 返回属性中声明的确认模式。
                fn ack_mode(&self) -> #root::AckMode {
                    #ack_mode
                }

                /// 将框架单记录转换为业务函数声明的参数形态并调用。
                async fn consume(
                    &self,
                    record: #root::KafkaRecord<Self::Message>,
                ) -> #root::Result<()> {
                    #invocation
                }
            }
        }
    };
    let erase = if shape.is_batch() {
        quote!(#root::__private::erase_batch(__NafkaConsumer))
    } else {
        quote!(#root::__private::erase_single(__NafkaConsumer))
    };

    Ok(quote! {
        #function

        const _: () = {
            /// 属性函数对应的无状态消费者单元类型。
            struct __NafkaConsumer;

            #trait_impl

            /// 构造由运行时统一实现的类型擦除 adapter。
            fn __build() -> #root::Result<
                ::std::sync::Arc<dyn #root::__private::ErasedConsumer>
            > {
                #erase
            }

            #[#root::__private::linkme::distributed_slice(#root::COLLECTED_CONSUMERS)]
            #[linkme(crate = #root::__private::linkme)]
            /// 当前属性函数的静态收集项。
            static __COLLECTED: #root::__private::CollectedConsumer =
                #root::__private::CollectedConsumer {
                    id: ::core::concat!(
                        ::core::module_path!(),
                        "::",
                        ::core::stringify!(#function_name)
                    ),
                    source: ::core::concat!(
                        ::core::file!(),
                        ":",
                        ::core::line!(),
                        ":",
                        ::core::column!()
                    ),
                    client: #client,
                    build: __build,
                };
        };
    })
}

/// 解析宏属性参数。
///
/// # 参数
///
/// - `metas`: 逗号分隔属性项。
///
/// # 错误
///
/// 必填项缺失、重复项、未知项或字面量类型错误时返回编译错误。
fn parse_args(metas: Punctuated<Meta, Token![,]>) -> syn::Result<ConsumerArgs> {
    let mut topics = None;
    let mut event = None;
    let mut group = None;
    let mut broadcast = None;
    let mut ack = None;
    let mut client = None;

    for meta in metas {
        let path = meta.path();
        if path.is_ident("topics") {
            ensure_missing(&topics, &meta, "topics")?;
            topics = Some(parse_topics(&meta)?);
        } else if path.is_ident("event") {
            ensure_missing(&event, &meta, "event")?;
            event = Some(parse_string_value(&meta, "event")?);
        } else if path.is_ident("group") {
            ensure_missing(&group, &meta, "group")?;
            group = Some(parse_string_value(&meta, "group")?);
        } else if path.is_ident("broadcast") {
            ensure_missing(&broadcast, &meta, "broadcast")?;
            broadcast = Some(match &meta {
                Meta::Path(_) => None,
                Meta::NameValue(_) => Some(parse_string_value(&meta, "broadcast")?),
                Meta::List(_) => {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "broadcast 只支持裸标记或字符串赋值",
                    ));
                }
            });
        } else if path.is_ident("ack") {
            ensure_missing(&ack, &meta, "ack")?;
            let value = parse_string_value(&meta, "ack")?;
            ack = Some(match value.value().as_str() {
                "auto" => AckArg::Auto,
                "manual" => AckArg::Manual,
                _ => {
                    return Err(syn::Error::new_spanned(
                        value,
                        "ack 只允许 \"auto\" 或 \"manual\"",
                    ));
                }
            });
        } else if path.is_ident("client") {
            ensure_missing(&client, &meta, "client")?;
            client = Some(parse_string_value(&meta, "client")?);
        } else {
            return Err(syn::Error::new_spanned(meta, "未知 kafka_consumer 属性项"));
        }
    }

    let topics =
        topics.ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "缺少 topics"))?;
    let event =
        event.ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "缺少 event"))?;
    if let (Some(group_value), Some(_)) = (&group, &broadcast) {
        return Err(syn::Error::new_spanned(
            group_value,
            "group 与 broadcast 互斥",
        ));
    }
    if event.value().trim().is_empty() {
        return Err(syn::Error::new_spanned(&event, "event 不能为空"));
    }
    deny_padded(&event, "event")?;
    if let Some(value) = group
        .as_ref()
        .filter(|value| value.value().trim().is_empty())
    {
        return Err(syn::Error::new_spanned(value, "group 不能为空"));
    }
    if let Some(value) = group.as_ref() {
        deny_padded(value, "group")?;
    }
    if let Some(value) = broadcast
        .as_ref()
        .and_then(Option::as_ref)
        .filter(|value| value.value().trim().is_empty())
    {
        return Err(syn::Error::new_spanned(value, "broadcast scope 不能为空"));
    }
    // broadcast 是仅存的另一个 group.id 来源，与 `group` 同等对待：registry 会把 scope
    // 净化成 [A-Za-z0-9._-] 再拼进 group.id，但**哈希用的是未净化的原值**——
    // `"alpha "` 与 `"alpha"` 因此生成两个不同的 group.id。一个尾随空格编译期无声，
    // 上线即换组：旧 offset 变孤儿，新组按 auto.offset.reset 重置。
    if let Some(value) = broadcast.as_ref().and_then(Option::as_ref) {
        deny_padded(value, "broadcast scope")?;
    }
    let client = client.unwrap_or_else(|| LitStr::new("default", proc_macro2::Span::call_site()));
    if client.value().trim().is_empty() {
        return Err(syn::Error::new_spanned(&client, "client 不能为空"));
    }
    deny_padded(&client, "client")?;
    Ok(ConsumerArgs {
        topics,
        event,
        group,
        broadcast,
        ack: ack.unwrap_or(AckArg::Auto),
        client,
    })
}

/// 拒绝同名属性重复出现。
///
/// # 参数
///
/// - `current`: 已解析的可选值。
/// - `meta`: 当前属性项。
/// - `name`: 诊断显示名。
///
/// # 错误
///
/// `current` 已有值时返回重复属性错误。
fn ensure_missing<T>(current: &Option<T>, meta: &Meta, name: &str) -> syn::Result<()> {
    if current.is_some() {
        Err(syn::Error::new_spanned(meta, format!("属性 `{name}` 重复")))
    } else {
        Ok(())
    }
}

/// 校验一个字面量值不含首尾空白。
///
/// 这些值会原样进入 route 元数据：`client` 与 registry 精确比较、`event` 与 header 精确比较、
/// `topics` 直接作为 Kafka topic 名。带一个空格就会变成「静默永不注册 / 永不匹配」，
/// 且没有任何运行期报错，所以必须在编译期拦下（而不是悄悄 trim，以免与用户意图不符）。
///
/// # 参数
///
/// - `value`: 待校验的字符串字面量。
/// - `field`: 出错时提示的字段名。
///
/// # 错误
///
/// 值带首尾空白时返回编译错误。
fn deny_padded(value: &LitStr, field: &str) -> syn::Result<()> {
    let raw = value.value();
    if raw != raw.trim() {
        return Err(syn::Error::new_spanned(
            value,
            format!("{field} 不能带首尾空白：该值会被原样用于匹配，带空白会导致静默不生效"),
        ));
    }
    Ok(())
}

/// 解析 `topics = ["a", "b"]`。
///
/// # 参数
///
/// - `meta`: topics 属性项。
///
/// # 错误
///
/// 不是非空字符串数组、含空 topic 或含重复 topic 时返回编译错误。
fn parse_topics(meta: &Meta) -> syn::Result<Vec<LitStr>> {
    let Meta::NameValue(value) = meta else {
        return Err(syn::Error::new_spanned(
            meta,
            "topics 必须使用字符串数组赋值",
        ));
    };
    let Expr::Array(ExprArray { elems, .. }) = &value.value else {
        return Err(syn::Error::new_spanned(
            &value.value,
            "topics 必须是字符串数组",
        ));
    };
    if elems.is_empty() {
        return Err(syn::Error::new_spanned(elems, "topics 至少包含一个 topic"));
    }
    let mut output = Vec::with_capacity(elems.len());
    let mut names = std::collections::BTreeSet::new();
    for expression in elems {
        let Expr::Lit(expression) = expression else {
            return Err(syn::Error::new_spanned(
                expression,
                "topic 必须是字符串字面量",
            ));
        };
        let Lit::Str(value) = &expression.lit else {
            return Err(syn::Error::new_spanned(
                expression,
                "topic 必须是字符串字面量",
            ));
        };
        if value.value().trim().is_empty() {
            return Err(syn::Error::new_spanned(value, "topic 不能为空"));
        }
        deny_padded(value, "topic")?;
        if !names.insert(value.value()) {
            return Err(syn::Error::new_spanned(value, "topics 不允许重复"));
        }
        output.push(value.clone());
    }
    Ok(output)
}

/// 解析字符串赋值属性。
///
/// # 参数
///
/// - `meta`: 属性项。
/// - `name`: 诊断显示名。
///
/// # 错误
///
/// 不是字符串字面量赋值时返回编译错误。
fn parse_string_value(meta: &Meta, name: &str) -> syn::Result<LitStr> {
    let Meta::NameValue(value) = meta else {
        return Err(syn::Error::new_spanned(
            meta,
            format!("{name} 必须使用字符串赋值"),
        ));
    };
    let Expr::Lit(expression) = &value.value else {
        return Err(syn::Error::new_spanned(
            &value.value,
            format!("{name} 必须是字符串字面量"),
        ));
    };
    let Lit::Str(value) = &expression.lit else {
        return Err(syn::Error::new_spanned(
            expression,
            format!("{name} 必须是字符串字面量"),
        ));
    };
    Ok(value.clone())
}

/// 校验被标注函数的限定符和输入输出数量。
///
/// # 参数
///
/// - `function`: 被标注函数。
///
/// # 错误
///
/// 非异步、非普通 free function、带泛型或参数数量不为一时返回编译错误。
fn validate_function(function: &ItemFn) -> syn::Result<()> {
    let signature = &function.sig;
    if signature.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            signature,
            "kafka_consumer 只支持 async fn",
        ));
    }
    if signature.constness.is_some()
        || signature.unsafety.is_some()
        || signature.abi.is_some()
        || signature.variadic.is_some()
    {
        return Err(syn::Error::new_spanned(
            signature,
            "只支持普通安全 Rust free function",
        ));
    }
    if !signature.generics.params.is_empty() || signature.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(signature, "消费者函数不能声明泛型"));
    }
    if signature.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            signature,
            "消费者函数必须且只能有一个参数",
        ));
    }
    if matches!(signature.output, ReturnType::Default) {
        return Err(syn::Error::new_spanned(
            signature,
            "消费者函数必须返回 Result<()>",
        ));
    }
    Ok(())
}

/// 识别四种规范参数形态并提取业务消息类型。
///
/// # 参数
///
/// - `ty`: 函数唯一参数类型。
///
/// # 错误
///
/// 类型不是可识别的路径，或容器泛型参数不规范时返回编译错误。
fn parse_shape(ty: &Type) -> syn::Result<HandlerShape> {
    if let Some(inner) = one_type_argument(ty, "KafkaRecord")? {
        return Ok(HandlerShape::SingleRecord(inner.clone()));
    }
    if let Some(inner) = one_type_argument(ty, "Vec")? {
        if let Some(message) = one_type_argument(inner, "KafkaRecord")? {
            return Ok(HandlerShape::BatchRecord(message.clone()));
        }
        return Ok(HandlerShape::BatchPayload(inner.clone()));
    }
    if matches!(ty, Type::Path(path) if path.qself.is_none()) {
        Ok(HandlerShape::SinglePayload(ty.clone()))
    } else {
        Err(syn::Error::new_spanned(
            ty,
            "参数必须是 T、KafkaRecord<T>、Vec<T> 或 Vec<KafkaRecord<T>>",
        ))
    }
}

/// 若类型末段是指定容器，返回其唯一类型参数。
///
/// # 参数
///
/// - `ty`: 待检查类型。
/// - `container`: 末段标识符。
///
/// # 错误
///
/// 命中容器但泛型参数不是唯一类型时返回编译错误。
fn one_type_argument<'a>(ty: &'a Type, container: &str) -> syn::Result<Option<&'a Type>> {
    let Type::Path(path) = ty else {
        return Ok(None);
    };
    let Some(segment) = path.path.segments.last() else {
        return Ok(None);
    };
    if segment.ident != container {
        return Ok(None);
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            segment,
            format!("{container} 必须带一个类型参数"),
        ));
    };
    if arguments.args.len() != 1 {
        return Err(syn::Error::new_spanned(
            arguments,
            format!("{container} 必须只有一个类型参数"),
        ));
    }
    match arguments.args.first() {
        Some(GenericArgument::Type(inner)) => Ok(Some(inner)),
        _ => Err(syn::Error::new_spanned(
            arguments,
            format!("{container} 参数必须是类型"),
        )),
    }
}

/// 生成 group 元数据表达式。
///
/// # 参数
///
/// - `args`: 已解析宏参数。
/// - `function_name`: 业务函数名。
/// - `root`: 运行时根路径。
fn group_tokens(
    args: &ConsumerArgs,
    function_name: &syn::Ident,
    root: &TokenStream2,
) -> TokenStream2 {
    if let Some(group) = &args.group {
        quote!(#root::GroupSpec::Named(::std::string::ToString::to_string(#group)))
    } else if let Some(scope) = &args.broadcast {
        match scope {
            Some(scope) => {
                quote!(#root::GroupSpec::Broadcast(::std::string::ToString::to_string(#scope)))
            }
            None => quote!(#root::GroupSpec::Broadcast(
                ::std::string::ToString::to_string(::core::concat!(
                    ::core::module_path!(),
                    "::",
                    ::core::stringify!(#function_name)
                ))
            )),
        }
    } else {
        quote!(#root::GroupSpec::Default)
    }
}

/// 生成四种函数参数形态对应的调用表达式。
///
/// # 参数
///
/// - `shape`: 已识别参数形态。
/// - `function_name`: 业务函数名。
fn invocation_tokens(shape: &HandlerShape, function_name: &syn::Ident) -> TokenStream2 {
    match shape {
        HandlerShape::SinglePayload(_) => quote!(#function_name(record.value).await),
        HandlerShape::SingleRecord(_) => quote!(#function_name(record).await),
        HandlerShape::BatchPayload(_) => quote!(
            #function_name(
                records
                    .into_iter()
                    .map(|record| record.value)
                    .collect::<::std::vec::Vec<_>>()
            )
            .await
        ),
        HandlerShape::BatchRecord(_) => quote!(#function_name(records).await),
    }
}
