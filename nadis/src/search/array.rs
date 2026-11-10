// RedisJSON 数组与分桶数组操作。
//
// JsonArray 在一个键中存储多个以业务 ID 区分的子文档，查询和删除使用 JSONPath。
// JsonArrayBucket 按 UTF-16 String.hashCode 与 floorMod 将子文档分配到固定桶；
// 同组桶共享 hash tag，跨桶 Lua 在一个 slot 内执行，避免 CROSSSLOT。
//
// ARRAY 查询不支持排序和显式分页。数组键字段属于持久化布局，saveOrReplace 只更新
// 当前派生的键；改变字段值前必须显式删除旧 parts 下的子文档，避免遗留旧记录。
// Script 使用 EVALSHA，NOSCRIPT 时回退加载脚本。

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::query::Query;
use super::{DataType, DocMeta, RedisDocument};
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

// 数组的原子更新脚本。

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

/// JSON 数组操作执行器。
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
    /// 业务作用：绑定数组执行器，在创建脚本句柄前检查存储形态与 RedisJSON 命令是否可调用。
    ///
    /// 参数说明：
    /// - `client`：后续数组命令、Lua 和批量写入共用的 Redis 客户端。
    ///
    /// 返回：元数据为数组模式且 JSON.TYPE 的回复形状、类型名称符合连接协议时返回执行器；配置、命令或回复异常时拒绝，不创建索引或修改数据。该只读探测不证明全部 JSON/Lua 命令权限、其它节点能力或后续可用性。
    pub async fn bind(client: Arc<RedisClient>) -> Result<Self> {
        let meta = T::meta();
        meta.validate()?;
        // 数组键布局和普通索引文档不同，绑定到错误执行器会改变数据存取边界。
        if !meta.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "{} 不是 ARRAY 模式(得到 {:?})—— JsonArrayOps 仅适用 JsonArray/JsonArrayBucket",
                meta.index, meta.data_type
            )));
        }
        // 绑定阶段先探测所需命令，避免接纳执行器后才发现 RedisJSON 不可调用。
        let js: std::result::Result<redis::Value, _> = redis::cmd("JSON.TYPE")
            .arg("__nasa_capability_check__")
            .query_async(&mut client.conn())
            .await;
        let reply = match js {
            Ok(reply) => reply,
            Err(e) => {
                if e.to_string().contains("unknown command") {
                    return Err(NasaRedisError::Config(format!(
                        "RedisJSON 模块不可用(ARRAY 模式需要):{e}"
                    )));
                }
                // ACL、超时或代理错误不能证明探测成功，也不能因此开放数组执行器。
                return Err(e.into());
            }
        };
        let info = redis::IntoConnectionInfo::into_connection_info(client.config().url.as_str())?;
        // 无路径探测只读取根值；RESP3 增加一层单元素数组，不能把任意容器或多值回复转成成功。
        let reply = match (info.redis_settings().protocol(), reply) {
            (redis::ProtocolVersion::RESP2, reply) => reply,
            (redis::ProtocolVersion::RESP3, redis::Value::Array(mut values))
                if values.len() == 1 =>
            {
                values.pop().expect("length checked")
            }
            _ => {
                return Err(NasaRedisError::Codec(
                    "ARRAY JSON.TYPE 回复形状不符合连接协议".into(),
                ));
            }
        };
        // 缺失键与全部合法根类型都能证明命令可读；未知文本不得回显，也不得冒充模块能力。
        let valid = match reply {
            redis::Value::Nil => true,
            redis::Value::BulkString(value) => matches!(
                value.as_slice(),
                b"null" | b"boolean" | b"integer" | b"number" | b"string" | b"object" | b"array"
            ),
            _ => false,
        };
        if !valid {
            return Err(NasaRedisError::Codec(
                "ARRAY JSON.TYPE 未返回已知根类型".into(),
            ));
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

    /// 业务作用：返回数组文档使用的静态元数据。
    pub fn meta(&self) -> &'static DocMeta {
        self.meta
    }

    // ───────── save 族 ─────────

    /// 业务作用：按文档业务分组和子文档标识选择目标键，并追加一个 JSON 子文档。
    ///
    /// 参数说明：
    /// - `doc`：提供数组分组、子文档 ID 和序列化内容的业务文档。
    ///
    /// 返回：数值 ID 通过比较范围检查、编码身份与同次 ID 一致、键派生成功且追加脚本调用成功时返回成功；失败返回错误，远端是否已追加需按错误类别判断。此入口不查重，同 ID 可以重复；覆盖应使用 save_or_replace。
    pub async fn save(&self, doc: &T) -> Result<()> {
        let id = doc.id();
        // 非有限数值会失去身份字段，越界浮点条件可能合并整数身份，必须在写入前拒绝。
        if self.meta.id_numeric {
            self.id_filter(&id)?;
        }
        let parts = doc.array_key_parts();
        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        let key = self.meta.array_key_for(&refs, &id)?;
        let body = encode_array_document(doc, self.meta, &id)?;
        let _: i64 = self
            .init_or_append
            .key(&key)
            .arg(&body)
            .invoke_async(&mut self.client.conn())
            .await?;
        Ok(())
    }

    /// 业务作用：将批量子文档按最终数组键分组，再按会话命令上限逐批提交追加脚本。
    ///
    /// 参数说明：
    /// - `docs`：需要追加的文档列表，每个文档独立派生业务分组和桶。
    ///
    /// 返回：空列表返回零；全部会话及票据成功时返回输入文档数。任一数值 ID 不满足比较范围、编码身份与同次 ID 不一致、派生或编码失败时整批不发送；提交失败返回错误，已经完成的批次不会回滚，未知执行结果不能自动当作未写入。
    pub async fn save_all(&self, docs: &[T]) -> Result<usize> {
        // 所有正文与身份先在本地核对，避免后续文档拒绝时已有前序分组发出。
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
        for doc in docs {
            let id = doc.id();
            // 非有限数值会失去身份字段，越界浮点条件可能合并整数身份，必须在写入前拒绝。
            if self.meta.id_numeric {
                self.id_filter(&id)?;
            }
            let parts = doc.array_key_parts();
            let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            let key = self.meta.array_key_for(&refs, &id)?;
            let body = encode_array_document(doc, self.meta, &id)?;
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

    /// 业务作用：在当前派生的数组键中按 ID 删除已有子文档，再追加新内容。
    ///
    /// 参数说明：
    /// - `doc`：提供目标分组、用于匹配的子文档 ID 和新内容的业务文档。
    ///
    /// 返回：使用同一次 ID 读取派生键与删除条件，派生、过滤器及编码身份校验成功且脚本返回可解码整数时返回成功；编码身份不一致在删除旧文档前拒绝，其它失败返回错误，Lua 内已有副作用不回滚；更改分组或桶配置不会清理旧键，迁移前应按旧 parts 显式删除。
    pub async fn save_or_replace(&self, doc: &T) -> Result<()> {
        let parts = doc.array_key_parts();
        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        // 键与删除条件必须共享同一身份观察，重复调用自定义访问器可能把一个桶与另一个 ID 拼成写入目标。
        let id = doc.id();
        let key = self.meta.array_key_for(&refs, &id)?;
        let filter = self.id_filter(&id)?;
        let body = encode_array_document(doc, self.meta, &id)?;
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

    /// 业务作用：按 subId 查单个子文档。
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

    /// 业务作用：子文档存在性：只判断 JSON.GET filter 是否命中，不做反序列化。
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

    /// 业务作用：按 subId 删单个子文档,返回删除数(0 = 不存在)。
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

    /// 业务作用：条件查询全 array(query 经 render_json_path_filter,构建期拒 sort/limit)。
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

    /// 业务作用：条件删除全 array,返回总删除数。
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

    /// 业务作用：条件计数遍历完整 array，但只累计数量，不构造实体。
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

    /// 业务作用：路由:ARRAY 模式单 key;BUCKET 模式按 subId 定桶。
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

    /// 业务作用：把子文档标识转换为与 id_numeric 类型约定对应的 JSONPath 比较条件。
    ///
    /// 参数说明：
    /// - `sub_id`：需要按文档标识字段匹配的原始字符串。
    ///
    /// 返回：i64 整数文本保持精确值，其余形式必须能转换为绝对值小于 2^53 的有限 f64，且非零系数不能下溢为零；不满足条件时返回配置错误。字符串使用 ARRAY 字面量规则；此处不校验序列化正文的 ID 表示或桶路由是否一致。
    fn id_filter(&self, sub_id: &str) -> Result<String> {
        let lit = if self.meta.id_numeric {
            // 完整 i64 范围保留整数比较；不能先转成浮点数，否则相邻的大整数可能共享比较值。
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
                // 非零系数下溢后会选择零身份；真正的正负零和带指数的零仍保持原有比较语义。
                if v == 0.0
                    && sub_id
                        .split(['e', 'E'])
                        .next()
                        .unwrap_or_default()
                        .bytes()
                        .any(|byte| matches!(byte, b'1'..=b'9'))
                {
                    return Err(NasaRedisError::Config(
                        "ARRAY 非零数值 ID 不能下溢为零".into(),
                    ));
                }
                // 浮点比较在此边界可能合并相邻整数；越界整数也不能借浮点回退进入覆盖或删除。
                if v.abs() >= 9_007_199_254_740_992.0 {
                    return Err(NasaRedisError::Config(
                        "ARRAY 数值 ID 超出浮点比较范围；大整数必须使用 i64 整数文本或字符串身份"
                            .into(),
                    ));
                }
                // JSONPath 不接受全部浮点文本写法；渲染为普通十进制，避免把指数作为路径语法。
                format!("{v}")
            }
        } else {
            super::query::json_path_literal(sub_id)?
        };
        Ok(format!("$[?(@.{}=={})]", self.meta.id_name, lit))
    }

    /// 业务作用：JSON.GET key filter 取原始 chunk(JSONPath 命中结果,通常是 JSON 数组)。
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

    /// 业务作用：chunk → Vec<T>(经 serde 一次解析直达实体)。
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

    /// 业务作用：删除数组过滤条件选中的 JSON 值，缺失 key 按幂等删除处理。
    /// 参数说明：`key` 为文档 key；`filter` 为已构造的 JSONPath 过滤条件。
    /// 返回：成功时返回删除数量；连接或 RedisJSON 命令失败时上抛错误。
    async fn json_del(&self, key: &str, filter: &str) -> Result<u64> {
        // JSON.DEL 对缺失 key 返回 0，直接执行即可维持幂等语义并避免检查与删除之间的竞态。
        let n: i64 = redis::cmd("JSON.DEL")
            .arg(key)
            .arg(filter)
            .query_async(&mut self.client.conn())
            .await?;
        Ok(n.max(0) as u64)
    }

    /// 业务作用：MULTI_GET:返回每桶非空 JSON chunk 列表。
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

    /// 业务作用：MULTI_DEL / MULTI_COUNT 共用:KEYS=全桶,ARGV[1]=filter,返回整数。
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

/// 业务作用：chunk 是否含命中:非 None/空串/空数组/null。
///
/// # 参数
/// - `chunk`: 分片上传中的单个文件块。
fn chunk_non_empty(chunk: Option<&str>) -> bool {
    matches!(chunk, Some(s) if !s.is_empty() && s != "[]" && s != "null")
}

/// 业务作用：在发送数组写入前核对编码后的身份，避免正文与选桶、覆盖条件选择不同的业务文档。
///
/// 参数说明：
/// - `doc`：需要追加或覆盖的业务文档，仅调用一次 Serialize。
/// - `meta`：绑定时冻结的身份属性名及数值类型约定。
/// - `id`：本次派生键和过滤条件使用的同一次身份观察，数值形式应已通过 id_filter。
///
/// 返回：根对象的身份属性与 id 类型、值一致时返回 JSON 文本；缺失、类型或数值不一致返回固定 Config，序列化失败返回 Codec。整型双方按 i64 精确比较，涉及浮点时只允许绝对值小于 2^53 的相同值；不改变字段编码、ID 文本或桶布局，不清理已有数据。
fn encode_array_document<T: Serialize>(doc: &T, meta: &DocMeta, id: &str) -> Result<String> {
    let value = super::to_json_value_omit_null(doc)?;
    let stored = value.get(&meta.id_name);
    let matches = if meta.id_numeric {
        let number = stored.and_then(serde_json::Value::as_number);
        match (
            id.parse::<i64>(),
            number.and_then(serde_json::Number::as_i64),
        ) {
            (Ok(expected), Some(actual)) => expected == actual,
            _ => {
                // 整数与浮点混用时越过精确区间会合并相邻身份；不能仅比较各自转成 f64 的结果。
                match (
                    id.parse::<f64>(),
                    number.and_then(serde_json::Number::as_f64),
                ) {
                    (Ok(expected), Some(actual)) => {
                        expected.abs() < 9_007_199_254_740_992.0
                            && actual.abs() < 9_007_199_254_740_992.0
                            && expected == actual
                    }
                    _ => false,
                }
            }
        }
    } else {
        stored.and_then(serde_json::Value::as_str) == Some(id)
    };
    // 先拒绝不一致正文，覆盖不能先删除旧记录；批量入口同样必须在提交任何一组前完成校验。
    if !matches {
        return Err(NasaRedisError::Config(
            "ARRAY 文档编码的身份与 ID 不一致".into(),
        ));
    }
    serde_json::to_string(&value).map_err(|error| {
        NasaRedisError::Codec(format!(
            "JSON 文档编码失败: {:?} at {}:{}",
            error.classify(),
            error.line(),
            error.column()
        ))
    })
}
