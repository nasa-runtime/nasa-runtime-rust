//! `#[kafka_consumer]` 无状态异步消费者收集宏。
//!
//! # 声明与运行架构
//!
//! 业务通过 `nasa::kafka::kafka_consumer` 声明 topic、精确 event、client、group、确认与执行模式。
//! 宏在编译期校验函数签名及共同身份输入域，生成类型适配与静态 descriptor；
//! nafka / Application 冻结 registry 后负责连接、消费、当前代次确认、重试与两段停机。
//! Java nasa-kafka 的声明式入口采用对应的线路与责任合同，合法组名不裁剪或归一化。
//!
//! # 能力边界
//!
//! 宏不读取 YAML、不创建客户端，也不提供独立 Ready 或指标。运行时仍校验来源、默认组与路由组合。
//! 编译通过不证明 broker 可达或组已获得分区；Manual 必须使用携带确认权威的 record 形态，
//! 按键模式仅适用于单条消费者，事务执行使用独立同步上下文而不通过此宏声明。

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
    /// 共同身份编码的明确广播作用域，与旧 broadcast 声明互斥。
    broadcast_scope: Option<LitStr>,
    /// 共同广播的稳定实例身份，缺省沿用源配置或进程身份。
    instance_id: Option<LitStr>,
    /// route 确认模式。
    ack: AckArg,
    /// 同组共享的分区或按键顺序域。
    execution: ExecutionArg,
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

/// 宏属性中的业务执行顺序域。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExecutionArg {
    /// 分区内顺序派发，允许连续批次。
    PartitionOrdered,
    /// 同分区不同 key 可并发，仅允许单条。
    KeyOrdered,
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
    /// 业务作用：返回关联业务消息类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用四种消费者形态中已经提取的业务消息类型，不包括 Vec 或 KafkaRecord 外壳。
    fn message_type(&self) -> &Type {
        match self {
            Self::SinglePayload(ty)
            | Self::SingleRecord(ty)
            | Self::BatchPayload(ty)
            | Self::BatchRecord(ty) => ty,
        }
    }

    /// 业务作用：判断是否使用批消费者契约。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：BatchPayload 或 BatchRecord 返回 true，其它形态 false，不检查运行时批量大小。
    fn is_batch(&self) -> bool {
        matches!(self, Self::BatchPayload(_) | Self::BatchRecord(_))
    }

    /// 业务作用：判断业务参数是否拿得到可信记录确认能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：SingleRecord 或 BatchRecord 返回 true，表示业务保留 KafkaRecord 外壳；是否允许确认仍由路由确认模式与运行时能力决定。
    fn receives_record(&self) -> bool {
        matches!(self, Self::SingleRecord(_) | Self::BatchRecord(_))
    }
}

/// 业务作用：收集无状态异步消费函数。 支持 `T`、`KafkaRecord<T>`、`Vec<T>`、`Vec<KafkaRecord<T>>` 四种参数形态。
///
/// 参数说明：
/// - `attr`：topics、事件、客户端、组身份、执行与确认策略的属性 token。
/// - `item`：需要作为无状态消费者登记的异步自由函数 token。
///
/// 返回：属性、签名和形态合法时保留业务函数并生成适配与链接期登记，非法时生成编译错误，不创建运行时客户端。
#[proc_macro_attribute]
pub fn kafka_consumer(attr: TokenStream, item: TokenStream) -> TokenStream {
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let function = parse_macro_input!(item as ItemFn);
    match expand(metas, function) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// 业务作用：校验路由属性和函数签名，生成消费 trait 实现与静态收集项。
///
/// 参数说明：
/// - `metas`：已经解析的路由与消费合同属性。
/// - `function`：待保留并生成适配的业务异步函数。
///
/// 返回：成功返回单条或批消费者实现和静态登记；属性、签名、形态、运行时路径不合法，或按键批处理、无记录的手动确认时返回错误。生成代码的实际类型兼容性仍由 Rust 编译检查。
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
    // 批次不能隐藏各个 key 的独立失败责任，在生成任何可激活路由前拒绝。
    if args.execution == ExecutionArg::KeyOrdered && shape.is_batch() {
        return Err(syn::Error::new_spanned(
            &input.ty,
            "execution = \"key_ordered\" 不支持批消费者",
        ));
    }
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
    let execution_mode = match args.execution {
        ExecutionArg::PartitionOrdered => quote!(#root::ExecutionMode::PartitionOrdered),
        ExecutionArg::KeyOrdered => quote!(#root::ExecutionMode::KeyOrdered),
    };
    let invocation = invocation_tokens(&shape, function_name);
    let trait_impl = if shape.is_batch() {
        quote! {
            impl #root::BatchConsumer for __NafkaConsumer {
                type Message = #message;

                /// 业务作用：提供宏声明的主题列表。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回按声明顺序复制的主题名称，不访问 broker。
                fn topics(&self) -> ::std::vec::Vec<::std::string::String> {
                    ::std::vec![#(::std::string::ToString::to_string(#topics)),*]
                }

                /// 业务作用：提供宏声明的精确事件路由名。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回原事件字符串副本，不去空白或改名。
                fn event(&self) -> ::std::string::String {
                    ::std::string::ToString::to_string(#event)
                }

                /// 业务作用：提供宏声明的消费位点身份规则。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回 Named、广播或默认组元数据；最终组名由运行时解析。
                fn group(&self) -> #root::GroupSpec {
                    #group
                }

                /// 业务作用：提供模块与函数路径构成的消费者身份。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回静态模块路径与函数名，不包含运行期请求数据。
                fn id(&self) -> &'static str {
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#function_name))
                }

                /// 业务作用：提供业务声明的消费确认模式。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回冻结的 Auto 或 Manual 模式，不执行确认。
                fn ack_mode(&self) -> #root::AckMode {
                    #ack_mode
                }

                /// 业务作用：提供路由的有序执行范围。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回声明的分区或按键顺序模式，组合是否合法由启动装配继续检查。
                fn execution_mode(&self) -> #root::ExecutionMode {
                    #execution_mode
                }

                /// 业务作用：把批记录按声明形态交给业务函数。
                ///
                /// 参数说明：
                /// - `records`：本批已解码并携带确认能力的记录，顺序由上层运行时固定。
                ///
                /// 返回：等待业务函数并原样返回其消费结果，不在本适配方法中提交位点。
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

                /// 业务作用：提供宏声明的主题列表。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回按声明顺序复制的主题名称，不访问 broker。
                fn topics(&self) -> ::std::vec::Vec<::std::string::String> {
                    ::std::vec![#(::std::string::ToString::to_string(#topics)),*]
                }

                /// 业务作用：提供宏声明的精确事件路由名。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回原事件字符串副本，不去空白或改名。
                fn event(&self) -> ::std::string::String {
                    ::std::string::ToString::to_string(#event)
                }

                /// 业务作用：提供宏声明的消费位点身份规则。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回 Named、广播或默认组元数据；最终组名由运行时解析。
                fn group(&self) -> #root::GroupSpec {
                    #group
                }

                /// 业务作用：提供模块与函数路径构成的消费者身份。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回静态模块路径与函数名，不包含运行期请求数据。
                fn id(&self) -> &'static str {
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#function_name))
                }

                /// 业务作用：提供业务声明的消费确认模式。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回冻结的 Auto 或 Manual 模式，不执行确认。
                fn ack_mode(&self) -> #root::AckMode {
                    #ack_mode
                }

                /// 业务作用：提供路由的有序执行范围。
                ///
                /// 参数说明: 无。
                ///
                /// 返回：返回声明的分区或按键顺序模式，组合是否合法由启动装配继续检查。
                fn execution_mode(&self) -> #root::ExecutionMode {
                    #execution_mode
                }

                /// 业务作用：把单条记录按声明形态交给业务函数。
                ///
                /// 参数说明：
                /// - `record`：已经解码并携带当前投递确认能力的记录。
                ///
                /// 返回：等待业务函数并原样返回其消费结果，不在本适配方法中提交位点。
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

            /// 业务作用：构造当前静态消费者的类型擦除适配。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：返回共享的擦除消费者或底层适配构造错误，不创建 broker 客户端。
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

/// 业务作用：解析路由属性，拒绝不完整、冲突或无法形成确定身份的声明。
///
/// 参数说明：
/// - `metas`：按声明顺序提供的逗号分隔属性项。
///
/// 返回：必需 topics、event 存在且属性类型、重复、互斥及身份边界合法时返回选项；否则返回编译错误。缺省 client 为 default，ack 为 auto，execution 为 partition_ordered，不连接 broker。
fn parse_args(metas: Punctuated<Meta, Token![,]>) -> syn::Result<ConsumerArgs> {
    let mut topics = None;
    let mut event = None;
    let mut group = None;
    let mut broadcast = None;
    let mut broadcast_scope = None;
    let mut instance_id = None;
    let mut ack = None;
    let mut execution = None;
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
        } else if path.is_ident("broadcast_scope") {
            ensure_missing(&broadcast_scope, &meta, "broadcast_scope")?;
            broadcast_scope = Some(parse_string_value(&meta, "broadcast_scope")?);
        } else if path.is_ident("instance_id") {
            ensure_missing(&instance_id, &meta, "instance_id")?;
            instance_id = Some(parse_string_value(&meta, "instance_id")?);
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
        } else if path.is_ident("execution") {
            ensure_missing(&execution, &meta, "execution")?;
            let value = parse_string_value(&meta, "execution")?;
            execution = Some(match value.value().as_str() {
                "partition_ordered" => ExecutionArg::PartitionOrdered,
                "key_ordered" => ExecutionArg::KeyOrdered,
                _ => {
                    return Err(syn::Error::new_spanned(
                        value,
                        "execution 只允许 \"partition_ordered\" 或 \"key_ordered\"",
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
    // 位点归属只能有一个来源；拒绝冲突声明，不能让宏与运行时各选择不同优先级。
    if let Some(scope) = &broadcast_scope {
        if group.is_some() || broadcast.is_some() {
            return Err(syn::Error::new_spanned(
                scope,
                "broadcast_scope 与 group/broadcast 互斥",
            ));
        }
        validate_broadcast_identity(scope)?;
    }
    if let Some(instance) = &instance_id {
        if broadcast_scope.is_none() {
            return Err(syn::Error::new_spanned(
                instance,
                "instance_id 要求 broadcast_scope",
            ));
        }
        validate_broadcast_identity(instance)?;
    }
    if let (Some(group_value), Some(_)) = (&group, &broadcast) {
        return Err(syn::Error::new_spanned(
            group_value,
            "group 与 broadcast 互斥",
        ));
    }
    if is_blank_event(&event.value()) {
        return Err(syn::Error::new_spanned(&event, "event 不能为空"));
    }
    // 组与事件各自保留合法原文；组名按共同 native 边界校验，不使用 Unicode trim 排除 NBSP。
    if let Some(value) = group.as_ref() {
        validate_group(value)?;
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
        broadcast_scope,
        instance_id,
        ack: ack.unwrap_or(AckArg::Auto),
        execution: execution.unwrap_or(ExecutionArg::PartitionOrdered),
        client,
    })
}

/// 业务作用：在编译期拒绝共同广播身份中不可安全编码的字面量。
///
/// 参数说明：
/// - `value`：broadcast_scope 或 instance_id 的字面量身份。
///
/// 返回：非空、不超 4096 UTF-8 字节、无控制字符及首尾 ASCII 空格时成功，否则报错；原值保留，不拒绝所有 Unicode 空白形态。
fn validate_broadcast_identity(value: &LitStr) -> syn::Result<()> {
    let identity = value.value();
    if identity.is_empty()
        || identity.len() > 4096
        || identity.starts_with(' ')
        || identity.ends_with(' ')
        || identity.chars().any(char::is_control)
    {
        return Err(syn::Error::new_spanned(
            value,
            "广播身份必须非空、无控制字符及首尾 ASCII 空格，且最多 4096 UTF-8 字节",
        ));
    }
    Ok(())
}

/// 业务作用：拒绝同名属性重复出现。
///
/// 参数说明：
/// - `current`：已解析的可选值。
/// - `meta`：当前属性项。
/// - `name`：诊断显示名。
///
/// 返回：current 为 None 时成功，否则返回以 meta 定位的重复属性错误，不覆盖旧值。
fn ensure_missing<T>(current: &Option<T>, meta: &Meta, name: &str) -> syn::Result<()> {
    if current.is_some() {
        Err(syn::Error::new_spanned(meta, format!("属性 `{name}` 重复")))
    } else {
        Ok(())
    }
}

/// 业务作用：校验 client、broadcast scope 与 topic 字面量的首尾空白边界。 这些字段参与资源选择或消费组派生，声明中的首尾空白直接拒绝，不能静默裁剪后改用另一个身份。 event 与普通 group 使用各自的精确文本合同及空白边界，不经过本门禁。
///
/// 参数说明：
/// - `value`：参与 client、广播或 topic 选择的原始字面量。
/// - `field`：用于定位属性错误的字段显示名。
///
/// 返回：原文与 trim 后相等时成功，否则返回编译错误，不静默更改消费资源或派生身份。
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

/// 业务作用：在生成静态路由前执行普通组的共同输入边界。
///
/// 参数说明：
/// - `group`：声明中用于消费位点归属的精确组名字面量。
///
/// 返回：非纯协议空白、无 NUL、最多 1024 字节且首尾字节不在 U+0000 至 U+0020 时成功，否则报错；不去空白或改名。
fn validate_group(group: &LitStr) -> syn::Result<()> {
    let value = group.value();
    if is_blank_event(&value)
        || value.contains('\0')
        || value.len() > 1024
        || value.as_bytes().first().is_some_and(|c| *c <= 0x20)
        || value.as_bytes().last().is_some_and(|c| *c <= 0x20)
    {
        return Err(syn::Error::new_spanned(
            group,
            "group 必须非纯空白、不含 NUL、首尾不含 U+0000–0020 且为1至1024个 UTF-8 字节",
        ));
    }
    Ok(())
}

/// 业务作用：在展开前拒绝运行时不能注册的空白事件，保持事件字面量与线路合同一致。
///
/// 参数说明：
/// - `event`：需要验证非空白的事件或普通组名原文。
///
/// 返回：空串或所有字符均在本层事件空白字符集合时返回 true，否则 false；不采用通用 Unicode trim，也不改写事件。
fn is_blank_event(event: &str) -> bool {
    event.chars().all(|value| {
        matches!(value, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}'
            | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}'
            | '\u{205f}' | '\u{3000}')
    })
}

/// 业务作用：解析 `topics = ["a", "b"]`。
///
/// 参数说明：
/// - `meta`：topics 属性项。
///
/// 返回：非空字符串数组且各项非空白、无首尾 Unicode 空白和精确重复时，按原序返回字面量；否则报错。不在此处检查 broker 是否存在该 topic 或完整 native 命名限制。
fn parse_topics(meta: &Meta) -> syn::Result<Vec<LitStr>> {
    let Meta::NameValue(value) = meta else {
        return Err(syn::Error::new_spanned(
            meta,
            "topics 必须使用字符串数组赋值",
        ));
    };
    let Expr::Array(ExprArray {
        elems,
        ..
    }) = &value.value
    else {
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

/// 业务作用：解析字符串赋值属性。
///
/// 参数说明：
/// - `meta`：属性项。
/// - `name`：诊断显示名。
///
/// 返回：字符串字面量赋值时返回其副本，非 NameValue、非字面量或非字符串时返回编译错误；不校验空值或语义。
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

/// 业务作用：校验被标注函数的限定符和输入输出数量。
///
/// 参数说明：
/// - `function`：被标注函数。
///
/// 返回：安全、非 const、无 ABI、非可变参数、无泛型与 where 且单参数 async 函数通过时成功，否则报错。这里只要求显式返回类型，不证明恰为 Result<()>；接收者与参数形态由后续检查。
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

/// 业务作用：识别四种规范参数形态并提取业务消息类型。
///
/// 参数说明：
/// - `ty`：函数唯一参数类型。
///
/// 返回：按末段识别 KafkaRecord、Vec 及嵌套组合并提取业务类型；其它无 qself 路径作为单消息，无法识别形态或容器参数不合法时返回错误，不解析类型别名或路径来源。
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

/// 业务作用：若类型末段是指定容器，返回其唯一类型参数。
///
/// 参数说明：
/// - `ty`：待检查类型。
/// - `container`：末段标识符。
///
/// 返回：类型末段不匹配时返回 None，匹配且恰有一个类型实参时返回借用，容器实参不符时报错；不解析完整类型身份。
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

/// 业务作用：生成 group 元数据表达式。
///
/// 参数说明：
/// - `args`：已解析宏参数。
/// - `function_name`：业务函数名。
/// - `root`：运行时根路径。
///
/// 返回：返回按 StableBroadcast、Named、Broadcast 或 Default 选择的元数据表达式；裸 broadcast 用模块与函数名派生作用域，不在宏展开时计算最终消费组或取得位点。
fn group_tokens(
    args: &ConsumerArgs,
    function_name: &syn::Ident,
    root: &TokenStream2,
) -> TokenStream2 {
    if let Some(scope) = &args.broadcast_scope {
        let instance = match &args.instance_id {
            Some(value) => {
                quote!(::core::option::Option::Some(::std::string::ToString::to_string(#value)))
            }
            None => quote!(::core::option::Option::None),
        };
        quote!(#root::GroupSpec::StableBroadcast { scope: ::std::string::ToString::to_string(#scope), instance_id: #instance })
    } else if let Some(group) = &args.group {
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

/// 业务作用：生成四种函数参数形态对应的调用表达式。
///
/// 参数说明：
/// - `shape`：已识别参数形态。
/// - `function_name`：业务函数名。
///
/// 返回：返回等待业务函数的调用表达式；payload 形态只移动 value，record 形态保留记录外壳，批 payload 按原序提取，不在宏展开时执行函数。
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
