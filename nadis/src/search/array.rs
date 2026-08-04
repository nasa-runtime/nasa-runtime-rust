// ============================================================================
// src/search/array.rs —— JSON_ARRAY / JSON_ARRAY_BUCKET 操作。
// 保留四种 DataType 与五段 Lua，并对齐既有 JsonArraySupport、
// JsonArrayLuaScripts 与 JsonArrayOperations。
//
// 模型:
//   · JsonArray:1 个 ARRAY key(prefix + @JsonArrayKey 值拼接)装多个子文档,
//     子文档以 @RsId 在数组内唯一;查询/删除走 RedisJSON JSONPath filter;
//   · JsonArrayBucket:同一组 array key 散到 N 个桶(子文档按
//     floorMod(原实现_string_hash(subId), N) 定桶——**持久化协议**,见 DocMeta);
//     所有桶 key 共享 `{body}` hash tag 必落同 slot,跨桶操作(find/remove/count
//     in array)用 Lua 把 N 次 RTT 收敛为 1 次且不触发 CROSSSLOT。
//
// Lua 脚本:5 个逐字节照搬 原实现 JsonArrayLuaScripts(语义注释见各常量;
// SHA1 一致 ⇒ 与 原实现 进程共享 server 端脚本缓存)。EVALSHA + NOSCRIPT fallback
// 由 redis-rs 的 Script 内建(对照 原实现 evalScript 的手工 fallback)。
//
// 不支持(构建期拒绝,对照 原实现):ARRAY 模式查询无 sort/limit(JSONPath 无对应);
// ⚠ @JsonArrayKey 不可变约束:saveOrReplace 只在**当前**算出的目标 key 上
// DEL+APPEND——业务改了 array key 字段值再保存,旧 key 里的旧子文档不会被清
// (跨 key/slot,CROSSSLOT 阻拦),必须先显式 remove_sub_doc(旧 parts) 再 save。
// ============================================================================

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::query::Query;
use super::{DataType, DocMeta, RedisDocument};
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

// ───────────────────────── 5 个 Lua(逐字节照搬 原实现)─────────────────────────

/// "key 不存在则 JSON.SET 初始化 array,否则 JSON.ARRAPPEND 追加"。单 key,多 value。
/// JSON.ARRAPPEND 不接受新 key(报 "could not perform this operation on a key that
/// doesn't exist"),必须先 JSON.SET 创建。Lua 包成 1 RTT + TOCTTOU 安全。
pub const INIT_OR_APPEND: &str = r#"if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('JSON.SET', KEYS[1], '$', '[' .. table.concat(ARGV, ',') .. ']')
else
  for i = 1, #ARGV do
    redis.call('JSON.ARRAPPEND', KEYS[1], '$', ARGV[i])
  end
end
return #ARGV
"#;

/// 替换-或-追加:先 JSON.DEL 旧子文档(若 key 存在),再判断 key 是否还在
/// (DEL 可能把数组删空导致 key 整体消失),不在则 JSON.SET 初始化,在则 ARRAPPEND。
/// 二次 EXISTS 是为了规避"DEL 把唯一子文档删除后 key 被回收"的边界。
/// `ARGV[1]` = idFilter，`ARGV[2]` = 新子文档 JSON。
pub const REPLACE_OR_APPEND: &str = r#"if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('JSON.DEL', KEYS[1], ARGV[1])
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('JSON.SET', KEYS[1], '$', '[' .. ARGV[2] .. ']')
else
  redis.call('JSON.ARRAPPEND', KEYS[1], '$', ARGV[2])
end
return 1
"#;

/// 跨桶 JSON.GET filter：KEYS = N 桶，`ARGV[1]` = JSONPath filter，
/// 返回每桶非空 JSON 字符串的 list(空桶/无命中跳过,客户端不用判空)。
pub const MULTI_GET: &str = r#"local out = {}
for i = 1, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    local s = redis.call('JSON.GET', KEYS[i], ARGV[1])
    if s and s ~= '[]' and s ~= 'null' then
      out[#out+1] = s
    end
  end
end
return out
"#;

/// 跨桶 JSON.DEL filter:返回总删除子文档数(sum)。
/// 单桶不存在直接跳过(JSON.DEL 在 missing key 上某些版本报错)。
pub const MULTI_DEL: &str = r#"local total = 0
for i = 1, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    local n = redis.call('JSON.DEL', KEYS[i], ARGV[1])
    if n then total = total + n end
  end
end
return total
"#;

/// 跨桶 count:cjson.decode 解每桶 GET 结果 + 数子文档数,返回 sum。
pub const MULTI_COUNT: &str = r#"local total = 0
for i = 1, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    local s = redis.call('JSON.GET', KEYS[i], ARGV[1])
    if s and s ~= '[]' and s ~= 'null' then
      local arr = cjson.decode(s)
      total = total + #arr
    end
  end
end
return total
"#;

// ───────────────────────── JsonArrayOps ─────────────────────────

/// JSON 数组操作执行器(对照 原实现 JsonArraySupport;仅 ARRAY 系 DataType 可绑定)。
pub struct JsonArrayOps<T: RedisDocument> {
    client: Arc<RedisClient>,
    meta: &'static DocMeta,
    // Script 持有源文本 + SHA1,invoke_async 自动 EVALSHA→NOSCRIPT fallback
    init_or_append: redis::Script,
    replace_or_append: redis::Script,
    multi_get: redis::Script,
    multi_del: redis::Script,
    multi_count: redis::Script,
    _t: PhantomData<T>,
}

impl<T: RedisDocument + Serialize + DeserializeOwned> JsonArrayOps<T> {
    /// 绑定 ARRAY 执行器并在启动期完成元数据和 RedisJSON 能力校验。
    ///
    /// # 参数
    /// - `client`: 已初始化的 Redis 客户端,后续所有 ARRAY Lua 和 JSON 命令都从它获取连接。
    pub async fn bind(client: Arc<RedisClient>) -> Result<Self> {
        let meta = T::meta();
        meta.validate()?;
        if !meta.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "{} 不是 ARRAY 模式(得到 {:?})—— JsonArrayOps 仅适用 JsonArray/JsonArrayBucket",
                meta.index, meta.data_type
            )));
        }
        // capability:JSON.TYPE 在缺 ReJSON 模块时报 unknown command(对照 actuator 探测)
        let js: std::result::Result<redis::Value, _> = redis::cmd("JSON.TYPE")
            .arg("__nasa_capability_check__")
            .query_async(&mut client.conn())
            .await;
        if let Err(e) = js {
            if e.to_string().contains("unknown command") {
                return Err(NasaRedisError::Config(format!(
                    "RedisJSON 模块不可用(ARRAY 模式需要):{e}"
                )));
            }
            //**非 unknown command 的错误(ACL 禁 JSON.TYPE/超时/READONLY/代理异常)不静默忽略**
            // ——与 `SearchActuator::bind`(actuator.rs)对齐;此前 fall through 到 Ok 会误判模块可用,bind
            // fail-fast 失效,首次 save/find 才暴露。JSON.TYPE 对缺失 key 返回 nil=Ok,出别的错 = 真异常,上抛。
            return Err(e.into());
        }
        Ok(Self {
            client,
            meta,
            init_or_append: redis::Script::new(INIT_OR_APPEND),
            replace_or_append: redis::Script::new(REPLACE_OR_APPEND),
            multi_get: redis::Script::new(MULTI_GET),
            multi_del: redis::Script::new(MULTI_DEL),
            multi_count: redis::Script::new(MULTI_COUNT),
            _t: PhantomData,
        })
    }

    /// Handles the meta operation.
    pub fn meta(&self) -> &'static DocMeta {
        self.meta
    }

    // ───────── save 族 ─────────

    /// 追加保存(不查重):INIT_OR_APPEND 到实体算出的目标 key
    /// (ARRAY = 单 key;BUCKET = 按 subId 定桶)。同 subId 重复 save 会产生
    /// 重复子文档——需要"同 id 覆盖"语义用 save_or_replace。
    ///
    /// # 参数
    /// - `doc`: 要追加写入数组的业务子文档,其 `array_key_parts()` 决定数组组,`id()` 决定子文档 ID。
    pub async fn save(&self, doc: &T) -> Result<()> {
        let parts = doc.array_key_parts();
        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        let key = self.meta.array_key_for(&refs, &doc.id())?;
        let body = super::to_json_omit_null(doc)?;
        let _: i64 = self
            .init_or_append
            .key(&key)
            .arg(&body)
            .invoke_async(&mut self.client.conn())
            .await?;
        Ok(())
    }

    /// 批量保存:按最终 key 分组(BUCKET 下不同 subId 自然分到不同桶),每组一条 INIT_OR_APPEND。
    ///
    /// # 参数
    /// - `docs`: 待追加写入的业务子文档列表,每个文档独立计算数组组和 bucket。
    ///
    ///分片进**真正的 `PipelineSession`**(单次 flush + session 上限约束),
    /// 不再 `join_all` 一次性创建全部 future;逐 ticket surface 服务端错误 / ExecutionUnknown。
    pub async fn save_all(&self, docs: &[T]) -> Result<usize> {
        // 分组:key → 该 key 下待追加的子文档 JSON 列表
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
        for doc in docs {
            let parts = doc.array_key_parts();
            let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            let key = self.meta.array_key_for(&refs, &doc.id())?;
            let body = super::to_json_omit_null(doc)?;
            grouped.entry(key).or_default().push(body);
        }
        let total = docs.len();
        let groups: Vec<(String, Vec<String>)> = grouped.into_iter().collect();
        if groups.is_empty() {
            return Ok(0);
        }
        let chunk_sz = self.client.config().pipeline.session_max_commands.max(1);
        for chunk in groups.chunks(chunk_sz) {
            let mut sess = self.client.pipeline();
            let mut tickets = Vec::with_capacity(chunk.len());
            for (key, bodies) in chunk {
                // 手工构造 EVAL(脚本体 = pub const INIT_OR_APPEND;KEYS[1]=key,ARGV=子文档列表)
                let mut cmd = redis::cmd("EVAL");
                cmd.arg(INIT_OR_APPEND).arg(1).arg(key.as_str());
                for b in bodies {
                    cmd.arg(b.as_str());
                }
                tickets.push(sess.enqueue::<redis::Value>(cmd)?);
            }
            sess.execute().await?;
            for t in tickets {
                t.await_result()?;
            }
        }
        Ok(total)
    }

    /// 替换-或-追加:同 subId 旧子文档先删再追加(REPLACE_OR_APPEND 单脚本原子)。
    /// ⚠ @JsonArrayKey/bucket_count 不可变约束见文件头——array key 字段变更后调用
    /// 本方法不会清旧 key 的旧子文档,必须业务侧先显式 remove_sub_doc(旧 parts)。
    ///
    /// # 参数
    /// - `doc`: 要写入的业务子文档,其 `id()` 用作数组内唯一子文档 ID。
    pub async fn save_or_replace(&self, doc: &T) -> Result<()> {
        let parts = doc.array_key_parts();
        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        let key = self.meta.array_key_for(&refs, &doc.id())?;
        let filter = self.id_filter(&doc.id())?;
        let body = super::to_json_omit_null(doc)?;
        let _: i64 = self
            .replace_or_append
            .key(&key)
            .arg(&filter)
            .arg(&body)
            .invoke_async(&mut self.client.conn())
            .await?;
        Ok(())
    }

    // ───────── 单子文档族(BUCKET 模式直接定位单桶,1 key 1 RTT)─────────

    /// 按 subId 查单个子文档。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的数组组业务 key 段。
    /// - `sub_id`: 数组内子文档 ID,BUCKET 模式下同时用于定位单桶。
    pub async fn find_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<Option<T>> {
        let key = self.key_for_sub_doc(parts, sub_id)?;
        let filter = self.id_filter(sub_id)?;
        let mut rows: Vec<T> = self.json_get_entities(&key, &filter).await?;
        Ok(if rows.is_empty() {
            None
        } else {
            Some(rows.swap_remove(0))
        })
    }

    /// 子文档存在性：只判断 JSON.GET filter 是否命中，不做反序列化。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的数组组业务 key 段。
    /// - `sub_id`: 要判断是否存在的数组内子文档 ID。
    pub async fn exists_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<bool> {
        let key = self.key_for_sub_doc(parts, sub_id)?;
        let filter = self.id_filter(sub_id)?;
        Ok(chunk_non_empty(
            self.json_get_chunk(&key, &filter).await?.as_deref(),
        ))
    }

    /// 按 subId 删单个子文档,返回删除数(0 = 不存在)。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的数组组业务 key 段。
    /// - `sub_id`: 要删除的数组内子文档 ID,BUCKET 模式下用于定位单桶。
    pub async fn remove_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<u64> {
        let key = self.key_for_sub_doc(parts, sub_id)?;
        let filter = self.id_filter(sub_id)?;
        self.json_del(&key, &filter).await
    }

    // ───────── 全 array 族(BUCKET 模式跨桶 Lua,1 RTT 扫全桶)─────────

    /// 条件查询全 array(query 经 render_json_path_filter,构建期拒 sort/limit)。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的数组组业务 key 段;BUCKET 模式下用于生成全部桶 key。
    /// - `q`: ARRAY 可翻译的类型化查询,会被渲染成 RedisJSON JSONPath filter。
    pub async fn find_in_array(&self, parts: &[&str], q: &Query) -> Result<Vec<T>> {
        let filter = q.render_json_path_filter(self.meta)?;
        if self.meta.data_type == DataType::JsonArrayBucket {
            let keys = self.meta.all_bucket_keys(parts)?;
            let chunks = self.eval_multi_get(&keys, &filter).await?;
            let mut out = Vec::new();
            for chunk in &chunks {
                if chunk_non_empty(Some(chunk)) {
                    let mut rows: Vec<T> = serde_json::from_str(chunk)
                        .map_err(|e| NasaRedisError::Codec(format!("JSON 解码失败: {e}")))?;
                    out.append(&mut rows);
                }
            }
            return Ok(out);
        }
        let key = self.meta.array_key_of(parts)?;
        self.json_get_entities(&key, &filter).await
    }

    /// 条件删除全 array,返回总删除数。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的数组组业务 key 段;BUCKET 模式下用于生成全部桶 key。
    /// - `q`: ARRAY 可翻译的类型化查询,决定要删除哪些子文档。
    pub async fn remove_in_array(&self, parts: &[&str], q: &Query) -> Result<u64> {
        let filter = q.render_json_path_filter(self.meta)?;
        if self.meta.data_type == DataType::JsonArrayBucket {
            let keys = self.meta.all_bucket_keys(parts)?;
            let keyrefs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let n: i64 = self
                .invoke_multi(&self.multi_del, &keyrefs, &filter)
                .await?;
            return Ok(n.max(0) as u64);
        }
        self.json_del(&self.meta.array_key_of(parts)?, &filter)
            .await
    }

    /// 条件计数遍历完整 array，但只累计数量，不构造实体。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的数组组业务 key 段;BUCKET 模式下用于生成全部桶 key。
    /// - `q`: ARRAY 可翻译的类型化查询,决定计数范围。
    pub async fn count_in_array(&self, parts: &[&str], q: &Query) -> Result<u64> {
        let filter = q.render_json_path_filter(self.meta)?;
        if self.meta.data_type == DataType::JsonArrayBucket {
            let keys = self.meta.all_bucket_keys(parts)?;
            let keyrefs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let n: i64 = self
                .invoke_multi(&self.multi_count, &keyrefs, &filter)
                .await?;
            return Ok(n.max(0) as u64);
        }
        let chunk = self
            .json_get_chunk(&self.meta.array_key_of(parts)?, &filter)
            .await?;
        match chunk.as_deref() {
            Some(s) if chunk_non_empty(Some(s)) => {
                // 解成 serde_json::Value 数组数个数(不经 T,省实体构造)
                let arr: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| NasaRedisError::Codec(format!("JSON 解码失败: {e}")))?;
                Ok(arr.as_array().map(|a| a.len()).unwrap_or(0) as u64)
            }
            _ => Ok(0),
        }
    }

    // ───────── 内部工具 ─────────

    /// 路由:ARRAY 模式单 key;BUCKET 模式按 subId 定桶(对照 原实现 keyForSubDoc)。
    ///
    /// # 参数
    /// - `parts`: 路径、占位符或 SQL 片段拆分后的部分。
    /// - `sub_id`: 业务标识,用于定位具体对象或记录。
    fn key_for_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<String> {
        match self.meta.data_type {
            DataType::JsonArrayBucket => self.meta.bucket_key(parts, sub_id),
            _ => self.meta.array_key_of(parts),
        }
    }

    /// 构造 `$[?(@.{id_name}=={literal})]` filter(按 subId 精确查/删)。
    /// 字面量形态按 meta.id_numeric:数字直出(校验可解析,防注入/形态错),
    /// 字符串走 JSON 转义加引号——形态错了 `==` 永远不命中(对照 原实现 jsonLiteral
    /// 靠 Object 运行时类型分流,Rust 靠 meta 标记)。
    ///
    /// # 参数
    /// - `sub_id`: 业务标识,用于定位具体对象或记录。
    fn id_filter(&self, sub_id: &str) -> Result<String> {
        let lit = if self.meta.id_numeric {
            //**用解析后规范化的数字串**,不回写原串——原串可能是 RedisJSON JSONPath
            // 数字文法**拒绝**的形态(科学计数 `1e3`/前导零 `01`/前导 `+`),`f64::parse` 接受但 JSONPath
            // 报 parse error。先试 i64(保大整数精度),再退 f64,与 query.rs 数字渲染口径统一。
            // RedisJSON JSONPath 是数值比较,`1000`/`1000.0` 等价,故规范化串能匹配存储的 JSON number。
            if let Ok(n) = sub_id.parse::<i64>() {
                n.to_string()
            } else {
                let v: f64 = sub_id.parse().map_err(|_| {
                    NasaRedisError::Config(format!(
                        "id_numeric=true 但 subId \"{sub_id}\" 不是数字"
                    ))
                })?;
                if !v.is_finite() {
                    return Err(NasaRedisError::Config(
                        "JSONPath 数值必须有限(NaN/Infinity 非法)".into(),
                    ));
                }
                format!("{v}") // Rust `{}`:1000.0→"1000"、1.5→"1.5"(整数值无 .0,JSONPath 数值比较容忍)
            }
        } else {
            super::query::json_path_literal(sub_id)?
        };
        Ok(format!("$[?(@.{}=={})]", self.meta.id_name, lit))
    }

    /// JSON.GET key filter 取原始 chunk(JSONPath 命中结果,通常是 JSON 数组)。
    /// 无命中/无 key 返 None。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `filter`: 待应用的查询、日志或字段过滤条件。
    async fn json_get_chunk(&self, key: &str, filter: &str) -> Result<Option<String>> {
        let raw: Option<String> = redis::cmd("JSON.GET")
            .arg(key)
            .arg(filter)
            .query_async(&mut self.client.conn())
            .await?;
        Ok(raw)
    }

    /// chunk → Vec<T>(经 serde 一次解析直达实体)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `filter`: 待应用的查询、日志或字段过滤条件。
    async fn json_get_entities(&self, key: &str, filter: &str) -> Result<Vec<T>> {
        let chunk = self.json_get_chunk(key, filter).await?;
        match chunk.as_deref() {
            Some(s) if chunk_non_empty(Some(s)) => serde_json::from_str(s)
                .map_err(|e| NasaRedisError::Codec(format!("JSON 解码失败: {e}"))),
            _ => Ok(Vec::new()),
        }
    }

    // Deletes JSON values matching the array filter.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `filter`: 待应用的查询、日志或字段过滤条件。
    async fn json_del(&self, key: &str, filter: &str) -> Result<u64> {
        // `JSON.DEL` 对 missing key 返回 0 而不报错，因此此前前置
        // `EXISTS` 既无必要、又引入两步非原子的 TOCTTOU 窗口(EXISTS 后、DEL 前被并发删)。直接 `JSON.DEL`
        // (本就幂等)单条 RTT。
        let n: i64 = redis::cmd("JSON.DEL")
            .arg(key)
            .arg(filter)
            .query_async(&mut self.client.conn())
            .await?;
        Ok(n.max(0) as u64)
    }

    /// MULTI_GET:返回每桶非空 JSON chunk 列表。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `filter`: 待应用的查询、日志或字段过滤条件。
    async fn eval_multi_get(&self, keys: &[String], filter: &str) -> Result<Vec<String>> {
        let mut inv = self.multi_get.prepare_invoke();
        for k in keys {
            inv.key(k.as_str());
        }
        inv.arg(filter);
        let chunks: Vec<String> = inv.invoke_async(&mut self.client.conn()).await?;
        Ok(chunks)
    }

    /// MULTI_DEL / MULTI_COUNT 共用:KEYS=全桶,ARGV[1]=filter,返回整数。
    ///
    /// # 参数
    /// - `script`: Lua 脚本文本。
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `filter`: 待应用的查询、日志或字段过滤条件。
    async fn invoke_multi(
        &self,
        script: &redis::Script,
        keys: &[&str],
        filter: &str,
    ) -> Result<i64> {
        let mut inv = script.prepare_invoke();
        for k in keys {
            inv.key(*k);
        }
        inv.arg(filter);
        let n: i64 = inv.invoke_async(&mut self.client.conn()).await?;
        Ok(n)
    }
}

/// chunk 是否含命中:非 None/空串/空数组/null(对照 原实现 chunkNonEmpty)。
///
/// # 参数
/// - `chunk`: 分片上传中的单个文件块。
fn chunk_non_empty(chunk: Option<&str>) -> bool {
    matches!(chunk, Some(s) if !s.is_empty() && s != "[]" && s != "null")
}
