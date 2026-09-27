// Redis 值编码以 bytes/string 为基础，JSON 由 Json<T> 显式选择。
// 计数器、ZSet member 和 Hash field 不自动包装为 JSON 文档。
//
// Json<T> 使用标准 serde_json，不写入或自动展开类名包装。共享 key 的生产者必须使用相同结构；
// 解码按目标 T 的 Deserialize 合同执行，不根据输入中的类型元数据选择业务类型。

use redis::{FromRedisValue, ParsingError, RedisWrite, ToRedisArgs, ToSingleRedisArg, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// JSON 值包装:写 = serde_json 序列化为 bytes;读 = 从 bytes 反序列化。
/// 用法:`client.set("k", Json(&user)).await?` / `let u: Json<User> = client.get(..)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Json<T>(pub T);

impl<T> Json<T> {
    /// 业务作用：取出包装值并把所有权交还调用方。
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Serialize> Json<T> {
    /// 业务作用：在 Redis 参数编码前显式序列化，使调用方能够处理序列化错误。
    /// 参数说明：无。
    /// 返回：JSON 字节；不支持的 map key 或自定义 Serialize 失败时返回错误。
    /// 成功后可把字节交给 `client.set(k, Json(&value).to_bytes()?)`。
    ///
    /// serde_json 将浮点值 NaN 和无穷编码为 null，并不因此返回错误；要求有限数值的业务
    /// 必须在调用前校验。整数 map key 可转成字符串键，数组等复合 key 会被拒绝。
    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.0)
    }
}

impl<T: Serialize> ToRedisArgs for Json<T> {
    /// 业务作用：把包装值编码为单个 Redis JSON 参数。
    /// 参数说明：`out` 为 Redis 命令参数写入器。
    /// 返回：成功时追加 JSON 字节；trait 没有错误通道，序列化失败时 panic。
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        // 参数编码不能返回错误，也不能丢弃数据后继续提交命令；需处理失败的调用方应先用 to_bytes。
        let bytes = serde_json::to_vec(&self.0)
            .expect("Json<T> 序列化失败；需处理错误时先调用 Json::to_bytes()");
        out.write_arg(&bytes);
    }
}

impl<T: DeserializeOwned> FromRedisValue for Json<T> {
    /// 业务作用：将 Redis 返回的字节按目标业务类型解码为 JSON 值。
    /// 参数说明：`v` 为 Redis 命令返回值。
    /// 返回：可按 T 解码时成功；无法取出字节、JSON 无效或类型不匹配时返回 ParsingError。
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        // 第一步:把 Redis 返回值按原始 bytes 取出(GET/HGET 等返回 BulkString)
        let bytes: Vec<u8> = Vec::<u8>::from_redis_value(v)?;
        // 第二步:serde_json 反序列化为业务类型;失败转 ParsingError(带原因文本)
        let t = serde_json::from_slice(&bytes)
            .map_err(|e| ParsingError::from(format!("Json<T> 反序列化失败: {e}")))?;
        Ok(Json(t))
    }
}

// JSON 只占一个 value 参数槽，不能展开成多个 Redis 参数。
impl<T: Serialize> ToSingleRedisArg for Json<T> {}
