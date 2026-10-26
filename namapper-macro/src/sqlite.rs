use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::Type;

use super::{MapperBackend, MethodPlan, ParamInfo, ParsedSqlTemplate, SqlNode};

/// 业务作用：用 SQLite 引号语义隔离 SQL 文本，再复用共享动态节点和参数规划。
/// 参数说明：
/// - `sql`：业务 SQL 模板。
/// - `params`：原始方法参数。
/// - `span`：诊断位置。
/// 返回：注释和字面量不产生绑定，BLOB 参数保持单值绑定的计划。
pub(super) fn parse_template(
    sql: &str,
    params: &[ParamInfo],
    span: Span,
) -> syn::Result<ParsedSqlTemplate> {
    let (masked, literals) = mask(sql, span)?;
    let mut adjusted = params.to_vec();
    for parameter in &mut adjusted {
        if is_blob(&parameter.ty) {
            parameter.blob_scalar = true;
        }
    }
    let (mut normalized, mut fragments, binds, mut dynamic) =
        super::parse_sql_template(&masked, &adjusted, span, MapperBackend::MySql)?;
    restore(&mut normalized, &literals);
    for fragment in &mut fragments {
        restore(fragment, &literals);
    }
    if let Some(dynamic) = &mut dynamic {
        restore_nodes(&mut dynamic.nodes, &literals);
    }
    Ok((normalized, fragments, binds, dynamic))
}

/// 业务作用：区分二进制值与 IN 列表，防止 Vec<u8> 被当成多个 SQL 参数。
/// 参数说明：
/// - `ty`：参数声明类型。
/// 返回：字节数组、slice 和 Vec 时为真。
fn is_blob(ty: &Type) -> bool {
    match ty {
        Type::Reference(reference) => is_blob(&reference.elem),
        Type::Slice(slice) => super::is_type_ident(&slice.elem, "u8"),
        Type::Array(array) => super::is_type_ident(&array.elem, "u8"),
        _ => super::first_generic_inner(ty, "Vec")
            .is_some_and(|inner| super::is_type_ident(inner, "u8")),
    }
}

/// 业务作用：把定长字节数组转成 SQLx SQLite 支持的 BLOB 类型，保留重复绑定与流的所有权。
/// 参数说明：
/// - `params`：业务声明的原始参数类型。
/// 返回：只作用于 SQLite 字节数组参数的局部绑定，其它参数保持原类型。
pub(super) fn array_bindings(params: &[ParamInfo]) -> TokenStream {
    let bindings = params.iter().filter_map(|param| {
        let ident = &param.ident;
        match &param.ty {
            Type::Array(array) if super::is_type_ident(&array.elem, "u8") => {
                Some(quote! { let #ident = #ident.to_vec(); })
            }
            Type::Reference(reference) => match reference.elem.as_ref() {
                Type::Array(array) if super::is_type_ident(&array.elem, "u8") => {
                    Some(quote! { let #ident: &[u8] = &#ident[..]; })
                }
                _ => None,
            },
            _ => None,
        }
    });
    quote! { #(#bindings)* }
}

/// 业务作用：保留 SQLite 文本字节，按原生词法跳过引号和注释并拒绝手写绑定占位符。
/// 参数说明：
/// - `sql`：未解析的模板。
/// - `span`：错误定位。
/// 返回：没有 SQL 引号干扰的模板和可无损还原的字面量表。
fn mask(sql: &str, span: Span) -> syn::Result<(String, Vec<(String, String)>)> {
    let mut prefix = "__nasa_sqlite_literal_".to_owned();
    while sql.contains(&prefix) {
        prefix.push('_');
    }
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut masked = String::new();
    let mut literals = Vec::new();
    while index < bytes.len() {
        let start = index;
        match bytes[index] {
            b'\'' | b'"' | b'`' | b'[' => {
                let end_quote = if bytes[index] == b'[' {
                    b']'
                } else {
                    bytes[index]
                };
                index += 1;
                loop {
                    if index >= bytes.len() {
                        return Err(syn::Error::new(span, "SQLite SQL 引号未闭合"));
                    }
                    if bytes[index] == end_quote {
                        index += 1;
                        if end_quote != b']' && bytes.get(index) == Some(&end_quote) {
                            index += 1;
                        } else {
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
                let marker = format!("{prefix}{}_end", literals.len());
                masked.push_str(&marker);
                literals.push((marker, sql[start..index].to_owned()));
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                return Err(syn::Error::new(span, "Mapper SQL 不允许行注释"));
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let end = sql[index + 2..]
                    .find("*/")
                    .ok_or_else(|| syn::Error::new(span, "SQLite SQL 注释未闭合"))?;
                index += end + 4;
                let marker = format!("{prefix}{}_end", literals.len());
                masked.push_str(&marker);
                literals.push((marker, sql[start..index].to_owned()));
            }
            b'<' if [
                "if",
                "choose",
                "when",
                "otherwise",
                "foreach",
                "trim",
                "where",
                "set",
                "order_by",
            ]
            .iter()
            .any(|tag| super::starts_open_tag_at(sql, index, tag)) =>
            {
                // 标签属性属于受限 DSL，不按 SQLite 字面量替换其条件表达式。
                let mut quote = None;
                index += 1;
                while index < bytes.len() {
                    let byte = bytes[index];
                    index += 1;
                    if quote == Some(byte) {
                        quote = None;
                    } else if quote.is_none() && matches!(byte, b'\'' | b'"') {
                        quote = Some(byte);
                    } else if quote.is_none() && byte == b'>' {
                        break;
                    }
                }
                masked.push_str(&sql[start..index]);
            }
            b'?' | b':' | b'@' | b'$' => {
                return Err(syn::Error::new(
                    span,
                    "SQLite Mapper 绑定只能使用 #{name}，不接受裸占位符或字符串插值",
                ))
            }
            _ => {
                let character = sql[index..].chars().next().expect("valid UTF-8 boundary");
                masked.push(character);
                index += character.len_utf8();
            }
        }
    }
    Ok((masked, literals))
}

/// 业务作用：在 SQL 结构解析完成后还原 SQLite 字面量，避免反斜线和内部空格被改写。
/// 参数说明：
/// - `text`：已完成绑定解析的文本。
/// - `literals`：本次模板的唯一标记表。
/// 返回：文本原地恢复，不再次扫描绑定标记。
fn restore(text: &mut String, literals: &[(String, String)]) {
    for (marker, original) in literals {
        *text = text.replace(marker, original);
    }
}

/// 业务作用：递归恢复动态节点中的 SQL 字面量，保持条件分支内的同一词法合同。
/// 参数说明：
/// - `nodes`：动态 SQL AST。
/// - `literals`：模板字面量表。
/// 返回：所有静态 SQL 片段原地恢复。
fn restore_nodes(nodes: &mut [SqlNode], literals: &[(String, String)]) {
    for node in nodes {
        match node {
            SqlNode::Text(text) => restore(text, literals),
            SqlNode::If {
                body,
                ..
            }
            | SqlNode::Foreach {
                body,
                ..
            }
            | SqlNode::Trim {
                body,
                ..
            } => restore_nodes(body, literals),
            SqlNode::Choose {
                whens,
                otherwise,
            } => {
                for when in whens {
                    restore_nodes(&mut when.body, literals);
                }
                restore_nodes(otherwise, literals);
            }
            _ => {}
        }
    }
}

/// 业务作用：按后端生成 SQLx 执行入口，SQLite 不向宏暴露底层可变连接。
/// 参数说明：
/// - `plan`：方法固定的后端。
/// - `write`：本方法是否声明写入或 RETURNING 合同。
/// 返回：具有对应权限的 executor 表达式。
pub(super) fn executor_tokens(plan: &MethodPlan, write: bool) -> TokenStream {
    if plan.backend == MapperBackend::Sqlite {
        quote!(__mapper_conn.executor(#write))
    } else {
        quote!(__mapper_conn.as_mut())
    }
}
