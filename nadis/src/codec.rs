// ============================================================================
// src/codec.rs —— 值编码(文档)。
//
// 红线:bytes/string 原语为基础,**JSON 是显式选择**——`Json<T>` 包装类型,
// 不让所有 Redis 值默认走 serde(计数器/ZSet member/Hash field 与 JSON 文档语义不同)。
//
// **产品决定(2026-06-14):不支持 Jackson `default-typing=true`(NON_FINAL)**。`Json<T>` 用
// 标准 serde_json(写出裸 `{"id":7,...}`,无 `@class`/类名包装)。
// **⚠ 重要:原实现 框架(`NasaLettuceConfig`/`RedisProxy`)
// default-typing 默认是 `true`**(写出 `["类名",{...}]` 的**多态包装数组**)——要与本框架标准 JSON
// 互通,**部署方必须主动在 原实现 侧关闭 default-typing**(不是既成事实)。本框架对此**无运行期强制**。
// 行为澄清:`Json<T>`(期望对象)读到 原实现 default-typing 的**数组** `["类名",{...}]` → serde **会
// 报错** `invalid type: sequence`(非"静默忽略"——`deny_unknown_fields=false` 只对"对象多字段"
// 生效,对"数组 vs 对象"形态不匹配不生效)。即真出错时会报错(好),但**不要**指望它静默兼容。
// `codec-jackson-compat` feature 已撤销不交付。跨语言共享 key 前必须 golden-bytes 验证 原实现 实际
// 写的是标准 JSON(关闭 default-typing)。
// ============================================================================

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
    /// 业务作用：**可失败序列化**:`ToRedisArgs::write_redis_args` 由 trait 约束**无错误
    /// 通道**,序列化失败只能 panic 或静默丢数据;两者都不可接受。处理**可能含 `NaN`/`Inf`/非字符串
    /// Map key** 等不满足 JSON 模型的不可信数据时,调用方应先用本方法拿到 `Result`,再把 `Vec<u8>`
    /// 喂命令(`client.set(k, Json::to_bytes(&v)?)`),从而把"序列化失败"变成可处理的 `Err` 而非 panic。
    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.0)
    }
}

impl<T: Serialize> ToRedisArgs for Json<T> {
    // Serializes the wrapped value into Redis command arguments.
    ///
    /// # 参数
    /// 业务作用：- `out`: 输出缓冲区,用于收集解析结果。
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        // ⚠ 本路径**无错误通道**(trait 约束):序列化失败只能 panic 或静默丢数据。常规 DTO 永不失败;
        // 仅 `NaN`/`Inf`/非字符串 Map key 等非 JSON 模型值会触发。**处理不可信/可能含此类值的数据时,
        // 改用 `Json::to_bytes()`(返回 `Result`)+ 原始命令**,不要走本默认路径( 文档)。
        let bytes = serde_json::to_vec(&self.0).expect("Json<T> 序列化失败:类型不满足 JSON 模型(NaN/Inf/非字符串 key?改用 Json::to_bytes() 处理)");
        out.write_arg(&bytes);
    }
}

// redis 1.2 起 FromRedisValue 改为【按值】接收 Value,解析失败返回 ParsingError
// (与 RedisError 区分:解析错误只有 message,无服务器/网络语义,见 redis-rs 1.0 迁移说明)。
impl<T: DeserializeOwned> FromRedisValue for Json<T> {
    // Decodes a Redis value into the wrapper type.
    ///
    /// # 参数
    /// 业务作用：- `v`: 待转换的值。
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        // 第一步:把 Redis 返回值按原始 bytes 取出(GET/HGET 等返回 BulkString)
        let bytes: Vec<u8> = Vec::<u8>::from_redis_value(v)?;
        // 第二步:serde_json 反序列化为业务类型;失败转 ParsingError(带原因文本)
        let t = serde_json::from_slice(&bytes)
            .map_err(|e| ParsingError::from(format!("Json<T> 反序列化失败: {e}")))?;
        Ok(Json(t))
    }
}

// 标记:Json<T> 是"单参数"——SET value / HSET value 等单值槽位要求 ToSingleRedisArg
// (redis 1.x 新增的类型级防误用:防把多 arg 类型塞进单值位置)。
impl<T: Serialize> ToSingleRedisArg for Json<T> {}
