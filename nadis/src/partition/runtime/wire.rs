//! 严格解析 Stream 与 PEL 响应；空 data 与记录被删除是不同事实。

use super::GroupRuntime;
use crate::error::{NasaRedisError, Result};

/// 业务作用：把 RESP 文本值转成协议字节，拒绝把异常值调试文本当消息正文。
/// 参数说明：`value` 为 RESP 字符串。
/// 返回：字节文本；其它形态返回 Parsing。
pub(super) fn bytes(value: redis::Value) -> Result<Vec<u8>> {
    match value {
        redis::Value::BulkString(v) => Ok(v),
        redis::Value::SimpleString(v) => Ok(v.into_bytes()),
        _ => Err(NasaRedisError::Parsing(
            "partition RESP text expected".into(),
        )),
    }
}

/// 业务作用：提取 RESP 数组，不把 Nil、错误或嵌套形态异常当空扫描。
/// 参数说明：`value` 为数组响应。
/// 返回：严格数组内容；其它形态拒绝。
pub(super) fn array(value: redis::Value) -> Result<Vec<redis::Value>> {
    match value {
        redis::Value::Array(values) => Ok(values),
        _ => Err(NasaRedisError::Parsing(
            "partition RESP array expected".into(),
        )),
    }
}

/// 业务作用：校验 Redis entry ID，防止不完整坐标进入提交索引。
/// 参数说明：`value` 为 Redis 返回的 ID。
/// 返回：合法毫秒与序号文本；非法或非 UTF-8 值拒绝。
pub(super) fn id(value: redis::Value) -> Result<String> {
    let id = String::from_utf8(bytes(value)?)
        .map_err(|_| NasaRedisError::Parsing("non UTF-8 stream id".into()))?;
    if !id
        .split_once('-')
        .is_some_and(|(a, b)| a.parse::<u64>().is_ok() && b.parse::<u64>().is_ok())
    {
        return Err(NasaRedisError::Parsing("invalid stream id".into()));
    }
    Ok(id)
}

/// 业务作用：解析确实存在的 Stream 条目，缺失 data 作为坏信封而不是墓碑。
/// 参数说明：`value` 为 XRANGE 或读取命令中的 entry 数组。
/// 返回：精确 ID 与正文；Nil 字段只在 Redis 返回已删除引用时标为墓碑。
pub(super) fn entries(value: redis::Value) -> Result<Vec<(String, Option<Vec<u8>>)>> {
    let mut result = Vec::new();
    for entry in array(value)? {
        let mut pair = array(entry)?;
        if pair.len() != 2 {
            return Err(NasaRedisError::Parsing("invalid stream entry".into()));
        }
        let fields = pair.pop().expect("fields");
        let id = id(pair.pop().expect("id"))?;
        if matches!(fields, redis::Value::Nil) {
            result.push((id, None));
            continue;
        }
        let pairs = match fields {
            redis::Value::Map(pairs) => pairs,
            other => {
                let fields = array(other)?;
                if fields.len() % 2 != 0 {
                    return Err(NasaRedisError::Parsing("odd stream fields".into()));
                }
                let mut it = fields.into_iter();
                let mut pairs = Vec::new();
                while let (Some(a), Some(b)) = (it.next(), it.next()) {
                    pairs.push((a, b));
                }
                pairs
            }
        };
        let mut data = None;
        for (key, value) in pairs {
            if bytes(key)? == crate::partition::DATA_FIELD.as_bytes() {
                data = Some(bytes(value)?);
            }
        }
        result.push((id, Some(data.unwrap_or_default())));
    }
    Ok(result)
}

/// 业务作用：解析单来源 XREADGROUP，严格复验返回的 Stream 坐标。
/// 参数说明：`value` 为响应；`stream` 为本次请求的唯一 Stream。
/// 返回：空响应返回空集合；额外 Stream 或异常形态返回协议错误。
pub(super) fn read(value: redis::Value, stream: &str) -> Result<Vec<(String, Option<Vec<u8>>)>> {
    if matches!(value, redis::Value::Nil) {
        return Ok(Vec::new());
    }
    let pairs = match value {
        redis::Value::Map(pairs) => pairs,
        other => {
            let mut pairs = Vec::new();
            for item in array(other)? {
                let mut pair = array(item)?;
                if pair.len() != 2 {
                    return Err(NasaRedisError::Parsing("invalid stream response".into()));
                }
                let values = pair.pop().expect("values");
                pairs.push((pair.pop().expect("key"), values));
            }
            pairs
        }
    };
    if pairs.len() > 1 {
        return Err(NasaRedisError::Parsing("unexpected stream count".into()));
    }
    let mut result = Vec::new();
    for (key, value) in pairs {
        if bytes(key)? != stream.as_bytes() {
            return Err(NasaRedisError::Parsing(
                "unexpected stream coordinate".into(),
            ));
        }
        result.extend(entries(value)?);
    }
    Ok(result)
}

pub(super) struct Pending {
    pub id: String,
    pub consumer: String,
    pub deliveries: u64,
}

/// 业务作用：解析精确 PEL owner 和 delivery count，保留空结果与协议异常的区别。
/// 参数说明：`value` 为 XPENDING 扩展形式响应。
/// 返回：明确 owner 记录；异常形态不转换为空。
pub(super) fn pending(value: redis::Value) -> Result<Vec<Pending>> {
    let mut result = Vec::new();
    for item in array(value)? {
        let fields = array(item)?;
        if fields.len() != 4 {
            return Err(NasaRedisError::Parsing("invalid XPENDING row".into()));
        }
        let mut it = fields.into_iter();
        let id = id(it.next().expect("id"))?;
        let consumer = String::from_utf8(bytes(it.next().expect("consumer"))?)
            .map_err(|_| NasaRedisError::Parsing("invalid consumer".into()))?;
        let _idle = it.next();
        let deliveries = match it.next() {
            Some(redis::Value::Int(n)) if n >= 0 => n as u64,
            _ => return Err(NasaRedisError::Parsing("invalid delivery count".into())),
        };
        result.push(Pending {
            id,
            consumer,
            deliveries,
        });
    }
    Ok(result)
}

/// 业务作用：精确查询一条记录当前 PEL 归属，防止不同 ID 的结论相互覆盖。
/// 参数说明：`rt` 为组；`partition` 为物理分区；`entry` 为唯一记录 ID。
/// 返回：存在时返回 owner 与次数；明确为空才返回 None。
pub(super) async fn exact_pending(
    rt: &GroupRuntime,
    partition: u32,
    entry: &str,
) -> Result<Option<Pending>> {
    let value = redis::cmd("XPENDING")
        .arg(rt.layout.stream(partition))
        .arg(rt.layout.group())
        .arg(entry)
        .arg(entry)
        .arg(1)
        .query_async(&mut rt.client.conn())
        .await?;
    let mut rows = pending(value)?;
    if rows.len() > 1 || rows.first().is_some_and(|row| row.id != entry) {
        return Err(NasaRedisError::Parsing(
            "XPENDING coordinate mismatch".into(),
        ));
    }
    Ok(rows.pop())
}

/// 业务作用：读取精确正文，只有明确不存在的 Stream entry 才作为墓碑。
/// 参数说明：`rt` 为组；`partition` 为物理分区；`entry` 为记录 ID。
/// 返回：存在 entry 时返回正文（可为空）；entry 不存在时返回 None。
pub(super) async fn exact_body(
    rt: &GroupRuntime,
    partition: u32,
    entry: &str,
) -> Result<Option<Vec<u8>>> {
    let value = redis::cmd("XRANGE")
        .arg(rt.layout.stream(partition))
        .arg(entry)
        .arg(entry)
        .arg("COUNT")
        .arg(1)
        .query_async(&mut rt.client.conn())
        .await?;
    let mut rows = entries(value)?;
    if rows.len() > 1 || rows.first().is_some_and(|(id, _)| id != entry) {
        return Err(NasaRedisError::Parsing("XRANGE coordinate mismatch".into()));
    }
    Ok(rows.pop().and_then(|(_, body)| body))
}
