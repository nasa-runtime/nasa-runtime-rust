//! Job 状态脚本执行：Job 控制面用独立连接以二进制 KEYS/ARGV 执行 Lua，优先 EVALSHA、NOSCRIPT 回退 EVAL。
//!
//! 所有状态迁移都在一段 Lua 内原子提交；发命令前复验全部 KEYS 同 slot，避免跨 slot 脚本在 Cluster 上错乱
//! 分片状态。Job 控制提交走该独立 lane，不进入普通命令或 pipeline 路径。

use redis::{ErrorKind, ServerErrorKind, Value};

use crate::client::{Conn, RedisClient};
use crate::error::{NasaRedisError, Result};
use crate::keytag::redis_slot;

/// 一段随 crate 归档的 Job 脚本及其编译期名称；SHA1 在首次执行时惰性计算。
pub(crate) struct JobScript {
    /// 脚本文本，编译期经 `include_str!` 确认存在。
    pub(crate) text: &'static str,
    sha: std::sync::OnceLock<String>,
}

impl JobScript {
    /// 业务作用：绑定一段嵌入脚本文本，延迟计算其 SHA1。
    ///
    /// 参数说明：
    /// - `text`: 嵌入的 Lua 文本。
    ///
    /// 返回：尚未计算 SHA1 的脚本描述。
    pub(crate) const fn new(text: &'static str) -> Self {
        Self {
            text,
            sha: std::sync::OnceLock::new(),
        }
    }

    /// 业务作用：返回脚本 SHA1，供 EVALSHA 首选路径使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：小写 hex SHA1。
    fn sha(&self) -> &str {
        self.sha
            .get_or_init(|| redis::Script::new(self.text).get_hash().to_owned())
    }
}

/// 业务作用：在 Job 控制连接上执行一段状态脚本；复验 KEYS 同 slot，EVALSHA 未命中时回退 EVAL。
///
/// 参数说明：
/// - `client`: 承载控制连接的 RedisClient。
/// - `script`: 目标嵌入脚本。
/// - `keys`: 已序列化的二进制键，必须全部同 slot。
/// - `argv`: 已序列化的二进制脚本参数。
///
/// 返回：脚本返回的原始 `Value` 数组；KEYS 跨 slot 时在发命令前返回协议错误；写出后的传输或解析失败
/// 返回 `ExecutionUnknown`，调用方不得自动重放有副作用动作。
pub(crate) async fn eval(
    client: &RedisClient,
    script: &JobScript,
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
) -> Result<Vec<Value>> {
    ensure_same_slot(keys)?;
    let mut conn = client.job_control_conn().await?;
    match run(&mut conn, "EVALSHA", script.sha().as_bytes(), keys, argv).await {
        Ok(values) => Ok(values),
        Err(error) if is_noscript(&error) => {
            // 目标节点尚未缓存脚本：对同一连接回退整文本 EVAL，不依赖一次 SCRIPT LOAD 广播到全部节点。
            let values = run(&mut conn, "EVAL", script.text.as_bytes(), keys, argv)
                .await
                .map_err(classify_execution_error)?;
            client.record_job_script_reload();
            Ok(values)
        }
        Err(error) => Err(classify_execution_error(error)),
    }
}

/// 业务作用：区分脚本的确定性服务端拒绝与写出后结果不确定，阻止上层把可能已提交的状态动作透明重放。
///
/// 参数说明：
/// - `error`: EVAL/EVALSHA 返回的底层错误。
///
/// 返回：服务端明确拒绝保留为 Redis 错误；连接、路由、协议和响应解析失败归为 `ExecutionUnknown`。
fn classify_execution_error(error: redis::RedisError) -> NasaRedisError {
    if matches!(error.kind(), ErrorKind::Server(_)) {
        NasaRedisError::Redis(error)
    } else {
        crate::job::JobError::ExecutionUnknown(
            "状态脚本可能已执行，必须读取权威状态后再决定后续动作".to_owned(),
        )
        .into()
    }
}

/// 业务作用：复验一组 KEYS 落在同一 slot，Cluster 下跨 slot 脚本会破坏分片原子性，必须发命令前拒绝。
///
/// 参数说明：
/// - `keys`: 待执行脚本的全部键。
///
/// 返回：全部同 slot（或空）时成功；存在跨 slot 键时返回协议错误。
fn ensure_same_slot(keys: &[Vec<u8>]) -> Result<()> {
    let mut slot: Option<u16> = None;
    for key in keys {
        let current = redis_slot(key);
        match slot {
            None => slot = Some(current),
            Some(expected) if expected != current => {
                return Err(crate::job::JobError::Protocol(
                    "脚本的 KEYS 不在同一 Redis slot".to_owned(),
                )
                .into());
            }
            _ => {}
        }
    }
    Ok(())
}

/// 业务作用：组装并执行一次 EVAL/EVALSHA，保持二进制键与参数不经 UTF-8 转换。
///
/// 参数说明：
/// - `conn`/`verb`/`body`/`keys`/`argv`: 连接、命令动词、脚本文本或 SHA、二进制键与参数。
///
/// 返回：类型化为 `Vec<Value>` 的脚本返回；底层错误透传由调用方分类。
async fn run(
    conn: &mut Conn,
    verb: &str,
    body: &[u8],
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
) -> std::result::Result<Vec<Value>, redis::RedisError> {
    let mut cmd = redis::cmd(verb);
    cmd.arg(body).arg(keys.len());
    for key in keys {
        cmd.arg(key.as_slice());
    }
    for arg in argv {
        cmd.arg(arg.as_slice());
    }
    cmd.query_async(conn).await
}

/// 业务作用：判断错误是否为服务端 NOSCRIPT，用于精确触发脚本加载回退。
///
/// 参数说明：
/// - `error`: EVALSHA 返回的错误。
///
/// 返回：错误类别为 NOSCRIPT 时为 `true`。
fn is_noscript(error: &redis::RedisError) -> bool {
    error.kind() == ErrorKind::Server(ServerErrorKind::NoScript)
}

/// 业务作用：把脚本返回的单个 `Value` 归一化为字符串，兼容整数、bulk、simple 与 nil。
///
/// 参数说明：
/// - `value`: 脚本返回数组的一个元素。
///
/// 返回：整数与字符串取其文本，nil 与其它类型取空串。
pub(crate) fn value_to_string(value: &Value) -> String {
    match value {
        Value::Nil => String::new(),
        Value::Int(n) => n.to_string(),
        Value::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::SimpleString(text) => text.clone(),
        _ => String::new(),
    }
}
