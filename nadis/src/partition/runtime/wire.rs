//! 严格解析 Stream 与 PEL 响应；空 data 与记录被删除是不同事实。

use super::GroupRuntime;
use crate::error::{NasaRedisError, Result};

/// 业务作用：把 RESP 文本值转成协议字节，拒绝把异常值调试文本当消息正文。
///
/// 参数说明：
/// - `value`: 线路中应为文本的 RESP 值，允许二进制字符串和简单字符串。
///
/// 返回：保留原始字节内容；其它类型返回 Parsing，不进行显示文本或数值转换。
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
///
/// 参数说明：
/// - `value`: 线路中应为数组的 RESP 值，元素结构由调用方继续核对。
///
/// 返回：仅对 Array 返回原元素集合，明确空数组保持为空；其它外层类型返回 Parsing。
pub(super) fn array(value: redis::Value) -> Result<Vec<redis::Value>> {
    match value {
        redis::Value::Array(values) => Ok(values),
        _ => Err(NasaRedisError::Parsing(
            "partition RESP array expected".into(),
        )),
    }
}

/// 业务作用：校验 Redis entry ID，防止不完整坐标进入提交索引。
///
/// 参数说明：
/// - `value`: Redis 返回的文本 ID，预期由连字符分隔毫秒与序号两个分量。
///
/// 返回：两个分量都是非空 ASCII 数字且可表示为 u64 时保留原文本；符号、空白、非文本、非 UTF-8 或越界均返回 Parsing。
pub(super) fn id(value: redis::Value) -> Result<String> {
    let id = String::from_utf8(bytes(value)?)
        .map_err(|_| NasaRedisError::Parsing("non UTF-8 stream id".into()))?;
    // 整数解析器允许前导加号，线路坐标却必须是数字分量，不能让宽松文本进入 ACK 与重投索引。
    if !id.split_once('-').is_some_and(|(a, b)| {
        !a.is_empty()
            && !b.is_empty()
            && a.bytes().all(|byte| byte.is_ascii_digit())
            && b.bytes().all(|byte| byte.is_ascii_digit())
            && a.parse::<u64>().is_ok()
            && b.parse::<u64>().is_ok()
    }) {
        return Err(NasaRedisError::Parsing("invalid stream id".into()));
    }
    Ok(id)
}

/// 业务作用：解析确实存在的 Stream 条目，缺失 data 作为坏信封而不是墓碑。
///
/// 参数说明：
/// - `value`: 由记录 ID 和字段集合组成的条目数组；字段集合可为 RESP Map 或交替键值数组。
///
/// 返回：成功时保留每个 ID 及其 data 正文；Nil 字段集合表示墓碑，缺 data 的现存条目保留空正文。
/// 外层或条目形态异常、字段数为奇数、ID、字段名或 data 类型非法均返回 Parsing，不返回部分解析结果。
pub(super) fn entries(value: redis::Value) -> Result<Vec<(String, Option<Vec<u8>>)>> {
    let mut result = Vec::new();
    for entry in array(value)? {
        let mut pair = array(entry)?;
        if pair.len() != 2 {
            return Err(NasaRedisError::Parsing("invalid stream entry".into()));
        }
        let fields = pair.pop().expect("fields");
        let id = id(pair.pop().expect("id"))?;
        // 只有明确 Nil 字段集合表示已删除引用；空字段或缺 data 仍是现存的不可解码信封。
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
///
/// 参数说明：
/// - `value`: XREADGROUP 的 RESP 响应，支持 Map 和 Stream/条目成对数组。
/// - `stream`: 本次读取唯一允许出现的完整物理 Stream 键，按字节精确比较。
///
/// 返回：Nil 或空容器返回空集合，单一匹配 Stream 返回解析后的记录；多 Stream、坐标不符或结构异常返回 Parsing。
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
    // 本次只请求一个物理来源，额外来源即使键名重复也不能合并进同一任期账本。
    if pairs.len() > 1 {
        return Err(NasaRedisError::Parsing("unexpected stream count".into()));
    }
    let mut result = Vec::new();
    for (key, value) in pairs {
        // 不能依赖服务端按请求回显；错误 Stream 的记录不得取得当前分区的提交权限。
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
///
/// 参数说明：
/// - `value`: XPENDING 扩展响应，每项含 ID、consumer、空闲时长和投递次数四个字段。
///
/// 返回：完整校验合法 ID、UTF-8 consumer、非负整数空闲时长及投递次数后，返回归属记录；空数组表示没有待确认项。
/// 项目数、任一字段类型或值不合法时返回 Parsing，不把无法判定的响应转换为空页。
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
        // 即使不使用 idle 作本地决策，也须验证整项证据；异常字段不能被跳过后用于授权恢复或提交。
        if !matches!(it.next(), Some(redis::Value::Int(idle)) if idle >= 0) {
            return Err(NasaRedisError::Parsing("invalid pending idle time".into()));
        }
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
///
/// 参数说明：
/// - `rt`: 提供 Redis 连接、物理 Stream 和消费组名称的运行时。
/// - `partition`: 要查询的物理分区编号，须属于当前来源。
/// - `entry`: 已校验的唯一记录 ID，同时作为 XPENDING 范围上下界。
///
/// 返回：精确匹配时返回 consumer 与投递次数，明确空数组才返回 None。
/// 多项、返回 ID 不匹配、解析异常或命令失败均返回错误，不产生归属结论。
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
    // COUNT 及范围约束仍需回包复验，另一条记录的 PEL 事实不能授权当前记录的 ACK 或重投。
    if rows.len() > 1 || rows.first().is_some_and(|row| row.id != entry) {
        return Err(NasaRedisError::Parsing(
            "XPENDING coordinate mismatch".into(),
        ));
    }
    Ok(rows.pop())
}

/// 业务作用：读取精确正文，只有明确不存在的 Stream entry 才作为墓碑。
///
/// 参数说明：
/// - `rt`: 提供 Redis 连接与物理 Stream 布局的运行时。
/// - `partition`: 当前来源负责的物理分区编号。
/// - `entry`: 已校验的唯一记录 ID，同时作为 XRANGE 的上下界。
///
/// 返回：精确匹配的现存条目返回正文，缺少 data 时正文为空；只有明确空结果返回 None。
/// 多项、ID 不匹配、条目字段为 Nil、其它形态异常或命令失败均返回错误；本函数不证明 PEL 归属，调用方须另行复验。
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
    // 重试只能重建原坐标正文，不能把相邻条目或超量响应替换为当前任务的业务输入。
    if rows.len() > 1 || rows.first().is_some_and(|(id, _)| id != entry) {
        return Err(NasaRedisError::Parsing("XRANGE coordinate mismatch".into()));
    }
    match rows.pop() {
        None => Ok(None),
        Some((_, Some(body))) => Ok(Some(body)),
        // PEL 历史读取允许 Nil 墓碑，XRANGE 只返回现存条目；不能把异常字段集合当作正文不存在的确认依据。
        Some((_, None)) => Err(NasaRedisError::Parsing(
            "XRANGE entry has no field collection".into(),
        )),
    }
}
