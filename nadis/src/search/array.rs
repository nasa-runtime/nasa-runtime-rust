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

// 数组更新脚本提供执行期间的串行隔离；脚本运行错误不会撤销已经完成的 Redis 命令。

/// "key 不存在则 JSON.SET 初始化 array,否则 JSON.ARRAPPEND 追加"。单 key,多 value。
/// JSON.ARRAPPEND 要求目标 key 已存在，因此先在同一脚本内判定并用 JSON.SET 初始化，
/// 避免独立检查与追加之间的键存在性竞争。
pub const INIT_OR_APPEND: &str = r#"-- 非数组根的追加可能返回空结果，覆盖还可能删除对象成员，必须在任何写入前拒绝。
local root_type = redis.call('JSON.TYPE', KEYS[1])
if root_type and root_type ~= 'array' then
  return redis.error_reply('ARRAY root must be an array')
end
if redis.call('EXISTS', KEYS[1]) == 0 then
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
/// `ARGV[1]` = idFilter，`ARGV[2]` = 新子文档 JSON，`ARGV[3]` = 大整数命中项的 ID 属性路径；
/// 不需要数值类型保护时仍传空字符串。大整数命中浮点身份时，在删除前拒绝整个覆盖。
pub const REPLACE_OR_APPEND: &str = r#"-- 非数组根的追加可能返回空结果，覆盖还可能删除对象成员，必须在任何写入前拒绝。
local root_type = redis.call('JSON.TYPE', KEYS[1])
if root_type and root_type ~= 'array' then
  return redis.error_reply('ARRAY root must be an array')
end
if type(ARGV[3]) ~= 'string' then
  return redis.error_reply('ARRAY identity guard is required')
end
-- 大整数与已有浮点身份比较可能合并相邻值；同一脚本先检查全部命中类型，避免误删后才发现歧义。
if root_type and ARGV[3] ~= '' then
  local id_types = redis.call('JSON.TYPE', KEYS[1], ARGV[3])
  if type(id_types) ~= 'table' then
    return redis.error_reply('ARRAY identity types are unavailable')
  end
  for i = 1, #id_types do
    if id_types[i] ~= 'integer' then
      return redis.error_reply('ARRAY large integer identity matches a non-integer value')
    end
  end
end
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('JSON.DEL', KEYS[1], ARGV[1])
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('JSON.SET', KEYS[1], '$', '[' .. ARGV[2] .. ']')
else
  redis.call('JSON.ARRAPPEND', KEYS[1], '$', ARGV[2])
end
return 1
"#;

// 大整数按 ID 删除先在同一脚本中复验根类型与全部命中身份，避免浮点舍入误选其它文档。
// ARGV[1] 为文档条件，ARGV[2] 为命中项的 ID 属性路径；缺失键直接返回零。
const REMOVE_INTEGER_ID: &str = r#"local root_type = redis.call('JSON.TYPE', KEYS[1])
if not root_type then
  return 0
end
if root_type ~= 'array' then
  return redis.error_reply('ARRAY root must be an array')
end
if type(ARGV[2]) ~= 'string' or ARGV[2] == '' then
  return redis.error_reply('ARRAY integer identity path is required')
end
-- 类型复验与删除不能分成两个请求，否则检查之后出现的浮点身份仍可能被删除。
local id_types = redis.call('JSON.TYPE', KEYS[1], ARGV[2])
if type(id_types) ~= 'table' then
  return redis.error_reply('ARRAY identity types are unavailable')
end
for i = 1, #id_types do
  if id_types[i] ~= 'integer' then
    return redis.error_reply('ARRAY large integer identity matches a non-integer value')
  end
end
return redis.call('JSON.DEL', KEYS[1], ARGV[1])
"#;

// 大整数按 ID 读取在同一次脚本中检查身份类型并取回原始 JSON，避免在客户端用浮点数重新解析身份。
const READ_INTEGER_ID: &str = r#"local root_type = redis.call('JSON.TYPE', KEYS[1])
if not root_type then
  return false
end
if root_type ~= 'array' then
  return redis.error_reply('ARRAY root must be an array')
end
if type(ARGV[2]) ~= 'string' or ARGV[2] == '' then
  return redis.error_reply('ARRAY integer identity path is required')
end
-- 类型复验与读取不能分成两个请求，否则检查之后出现的浮点身份仍可能被当作目标文档。
local id_types = redis.call('JSON.TYPE', KEYS[1], ARGV[2])
if type(id_types) ~= 'table' then
  return redis.error_reply('ARRAY identity types are unavailable')
end
for i = 1, #id_types do
  if id_types[i] ~= 'integer' then
    return redis.error_reply('ARRAY large integer identity matches a non-integer value')
  end
end
return redis.call('JSON.GET', KEYS[1], ARGV[1])
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
    // 脚本句柄保留源文本与 SHA1；服务器未缓存时按 NOSCRIPT 流程加载后再执行。
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

    /// 业务作用：提供当前执行器冻结使用的文档元数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回绑定时取得的静态 DocMeta 引用，不重新探测 Redis 或修改布局。
    pub fn meta(&self) -> &'static DocMeta {
        self.meta
    }

    // ───────── save 族 ─────────

    /// 业务作用：按文档业务分组和子文档标识选择目标键，并追加一个 JSON 子文档。
    ///
    /// 参数说明：
    /// - `doc`：提供数组分组、子文档 ID 和序列化内容的业务文档。
    ///
    /// 返回：数值 ID 通过比较范围检查、编码身份与同次 ID 一致、键派生成功且脚本精确回复整数 1 时返回成功；已有键的根值不是数组时，脚本在写入前拒绝；其它失败返回错误，远端是否已追加需按错误类别判断。此入口不查重，同 ID 可以重复；覆盖应使用 save_or_replace。
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
        // 编码完成后才发送追加脚本；命令失败可能处于远端结果未知，调用方不能无条件重试追加。
        let reply: redis::Value = self
            .init_or_append
            .key(&key)
            .arg(&body)
            .invoke_async(&mut self.client.conn())
            .await?;
        check_array_count_reply(reply, Some(1))?;
        Ok(())
    }

    /// 业务作用：将批量子文档按最终数组键分组，再按会话命令上限逐批提交追加脚本。
    ///
    /// 参数说明：
    /// - `docs`：需要追加的文档列表，每个文档独立派生业务分组和桶。
    ///
    /// 返回：空列表返回零；全部会话成功且每张票据精确确认对应分组的文档数量时返回输入文档数。任一数值 ID 不满足比较范围、编码身份与同次 ID 不一致、派生或编码失败时整批不发送；每个键的脚本在写入前拒绝已有的非数组根值；提交失败返回错误，已经完成的批次不会回滚，未知执行结果不能自动当作未写入。
    pub async fn save_all(&self, docs: &[T]) -> Result<usize> {
        // 所有键与正文先在本地准备，避免后续文档编码失败时已有前序批次发出。
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
                tickets.push((sess.enqueue::<redis::Value>(cmd)?, bodies.len()));
            }
            // 会话逐批发送；前序批次成功不会被后续错误撤销，不能把整批视为可回滚事务。
            sess.execute().await?;
            for (ticket, expected) in tickets {
                check_array_count_reply(ticket.await_result()?, Some(expected))?;
            }
        }
        Ok(total)
    }

    /// 业务作用：在当前派生的数组键中按 ID 删除已有子文档，再追加新内容。
    ///
    /// 参数说明：
    /// - `doc`：提供目标分组、用于匹配的子文档 ID 和新内容的业务文档。
    ///
    /// 返回：使用同一次 ID 读取派生键与删除条件，派生、过滤器及编码身份校验成功且脚本精确回复整数 1 时返回成功；编码身份不一致、已有根值不是数组，或大整数条件命中非整数身份时，在删除旧文档前拒绝；其它失败返回错误，Lua 内已有副作用不回滚；更改分组或桶配置不会清理旧键，迁移前应按旧 parts 显式删除。
    pub async fn save_or_replace(&self, doc: &T) -> Result<()> {
        let parts = doc.array_key_parts();
        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        // 键与删除条件必须共享同一身份观察，重复调用自定义访问器可能把一个桶与另一个 ID 拼成写入目标。
        let id = doc.id();
        let key = self.meta.array_key_for(&refs, &id)?;
        let filter = self.id_filter(&id)?;
        let guard_path = self.large_integer_guard_path(&id, &filter)?;
        let body = encode_array_document(doc, self.meta, &id)?;
        // 命中类型检查与删除必须留在同一次 Lua 中，客户端先读再写会让并发插入的浮点身份绕过检查。
        // 所有本地校验先完成；脚本后续运行错误仍不回滚已发生的删除。
        let reply: redis::Value = self
            .replace_or_append
            .key(&key)
            .arg(&filter)
            .arg(&body)
            .arg(&guard_path)
            .invoke_async(&mut self.client.conn())
            .await?;
        check_array_count_reply(reply, Some(1))?;
        Ok(())
    }

    // ───────── 单子文档族(BUCKET 模式直接定位单桶,1 key 1 RTT)─────────

    /// 业务作用：按子文档 ID 定位普通数组或单个桶，并解码首个命中文档。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值。
    /// - `sub_id`：用于生成 ID 条件的标识，分桶模式还用它定位单桶。
    ///
    /// 返回：无命中返回 None，有命中返回首个文档；重复整数或字符串 ID 不在此拒绝。大整数条件命中非整数身份时整次读取拒绝，类型检查与取正文在同一脚本内完成。键或过滤器无效、命令或解码失败返回错误。
    pub async fn find_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<Option<T>> {
        let key = self.key_for_sub_doc(parts, sub_id)?;
        let filter = self.id_filter(sub_id)?;
        let chunk = self.json_get_identity_chunk(&key, sub_id, &filter).await?;
        let mut rows: Vec<T> = decode_array_chunk(chunk.as_deref())?;
        Ok(if rows.is_empty() {
            None
        } else {
            Some(rows.swap_remove(0))
        })
    }

    /// 业务作用：按 ID 读取匹配数组，根据数组是否非空判断子文档存在性。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值。
    /// - `sub_id`：待检查的子文档标识，分桶模式据此定位单桶。
    ///
    /// 返回：缺失回复或合法空 JSON 数组返回 false，非空数组返回 true；其它形态或解码失败返回错误。大整数条件命中非整数身份时拒绝，不把浮点舍入得到的匹配当作存在证据。不构造业务实体，参数和命令错误向上传播。
    pub async fn exists_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<bool> {
        let key = self.key_for_sub_doc(parts, sub_id)?;
        let filter = self.id_filter(sub_id)?;
        let chunk = self.json_get_identity_chunk(&key, sub_id, &filter).await?;
        Ok(!decode_array_chunk::<serde_json::Value>(chunk.as_deref())?.is_empty())
    }

    /// 业务作用：在 ID 对应的普通数组或单桶内删除全部匹配子文档。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值。
    /// - `sub_id`：待删除的子文档标识，同时参与桶路由。
    ///
    /// 返回：成功返回非负删除数，重复 ID 可能删除多条；缺失键返回零。大整数条件在同一脚本中先检查数组根和命中项 ID 类型，命中非整数时在删除前拒绝。键派生、过滤器或命令失败返回错误，大整数脚本的异常回复也返回错误；未知执行结果不能视为未删除。
    pub async fn remove_sub_doc(&self, parts: &[&str], sub_id: &str) -> Result<u64> {
        let key = self.key_for_sub_doc(parts, sub_id)?;
        let filter = self.id_filter(sub_id)?;
        let guard_path = self.large_integer_guard_path(sub_id, &filter)?;
        if guard_path.is_empty() {
            return self.json_del(&key, &filter).await;
        }
        // 浮点记录可能与相邻的大整数同时命中；删除前由同一个脚本检查，拒绝时保留全部原记录。
        let reply: redis::Value = redis::Script::new(REMOVE_INTEGER_ID)
            .key(&key)
            .arg(&filter)
            .arg(&guard_path)
            .invoke_async(&mut self.client.conn())
            .await?;
        Ok(check_array_count_reply(reply, None)? as u64)
    }

    // ───────── 全 array 族(BUCKET 模式跨桶 Lua,1 RTT 扫全桶)─────────

    /// 业务作用：把类型化条件绑定到存储字段，读取业务分组内普通数组或全部桶的命中文档。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值，桶模式据此生成同组全部桶键。
    /// - `q`：仅包含 ARRAY 支持的条件且未显式排序或分页的查询。
    ///
    /// 返回：成功返回解码后的全部命中，桶模式按脚本返回的桶顺序拼接；空集返回空向量。路径、键、命令或任一结果解码失败返回错误，不提供子文档排序或分页。
    pub async fn find_in_array(&self, parts: &[&str], q: &Query) -> Result<Vec<T>> {
        // 先核对字段来源及 ARRAY 能力，不能忽略不支持的条件后扩大读取范围。
        let filter = q.render_json_path_filter(self.meta)?;
        if self.meta.data_type == DataType::JsonArrayBucket {
            let keys = self.meta.all_bucket_keys(parts)?;
            let chunks = self.eval_multi_get(&keys, &filter).await?;
            let mut out = Vec::new();
            for chunk in &chunks {
                let mut rows: Vec<T> = decode_array_chunk(Some(chunk))?;
                out.append(&mut rows);
            }
            return Ok(out);
        }
        let key = self.meta.array_key_of(parts)?;
        self.json_get_entities(&key, &filter).await
    }

    /// 业务作用：按已校验的条件删除一个业务分组中普通数组或全部桶的命中文档。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值。
    /// - `q`：决定删除范围的 ARRAY 类型化条件，空候选集合不选择文档。
    ///
    /// 返回：成功返回经过非负 RESP 整数校验的删除总数；条件、键、命令或回复证据不满足约束时返回错误。跨桶脚本串行执行，发生错误时已经删除的前序桶数据不回滚。
    pub async fn remove_in_array(&self, parts: &[&str], q: &Query) -> Result<u64> {
        // 删除前必须完成条件渲染；不支持的路径或选项不得退化为全组删除。
        let filter = q.render_json_path_filter(self.meta)?;
        if self.meta.data_type == DataType::JsonArrayBucket {
            let keys = self.meta.all_bucket_keys(parts)?;
            let keyrefs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            // 同组桶在脚本内串行处理；命令失败不撤销前序桶已经发生的删除。
            let n: i64 = self
                .invoke_multi(&self.multi_del, &keyrefs, &filter)
                .await?;
            return Ok(n as u64);
        }
        self.json_del(&self.meta.array_key_of(parts)?, &filter)
            .await
    }

    /// 业务作用：统计业务分组中满足 ARRAY 条件的子文档数量，不构造业务实体。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值。
    /// - `q`：决定计数范围的 ARRAY 类型化条件。
    ///
    /// 返回：桶模式返回通过非负 RESP 整数校验的脚本计数；普通数组要求合法 JSON 数组并按长度计数，缺失键返回零。条件、键、命令、JSON 语法或形态不符合约束时返回错误；不校验每条业务实体结构。
    pub async fn count_in_array(&self, parts: &[&str], q: &Query) -> Result<u64> {
        let filter = q.render_json_path_filter(self.meta)?;
        if self.meta.data_type == DataType::JsonArrayBucket {
            let keys = self.meta.all_bucket_keys(parts)?;
            let keyrefs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let n: i64 = self
                .invoke_multi(&self.multi_count, &keyrefs, &filter)
                .await?;
            return Ok(n as u64);
        }
        let chunk = self
            .json_get_chunk(&self.meta.array_key_of(parts)?, &filter)
            .await?;
        Ok(decode_array_chunk::<serde_json::Value>(chunk.as_deref())?.len() as u64)
    }

    // ───────── 内部工具 ─────────

    /// 业务作用：按执行器存储形态为一个子文档选择普通数组键或固定桶键。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 顺序一致的业务分组值。
    /// - `sub_id`：桶模式用于稳定计算桶号的原始标识。
    ///
    /// 返回：返回目标键；业务分组、模式或桶配置不满足键派生约束时返回错误，不访问 Redis。
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
        // 标识字段按完整存储属性名定位，不能让点号或其它路径字符选择另一个业务身份。
        let property = super::query::json_path_property(&self.meta.id_name)?;
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
        Ok(format!("$[?(@{}=={})]", property, lit))
    }

    /// 业务作用：为存在浮点舍入歧义的大整数按 ID 操作生成远端 ID 类型检查路径。
    ///
    /// 参数说明：
    /// - `sub_id`：已经通过 ID 过滤器校验、与目标键共用的业务身份文本。
    /// - `filter`：该身份对应的完整 JSONPath 文档选择条件。
    ///
    /// 返回：数值身份为绝对值至少 2^53 的 i64 时返回命中项的 ID 属性路径，供脚本在执行目标操作前检查；其它身份返回空字符串。属性名无法安全表达时返回配置错误，此处不读取或迁移已有数据。
    fn large_integer_guard_path(&self, sub_id: &str, filter: &str) -> Result<String> {
        // 整数间的比较保留 i64 精度；混入浮点记录时则必须由服务端在同次按 ID 操作中排除歧义。
        if self.meta.id_numeric
            && sub_id
                .parse::<i64>()
                .is_ok_and(|value| value.unsigned_abs() >= 9_007_199_254_740_992)
        {
            Ok(format!(
                "{}{}",
                filter,
                super::query::json_path_property(&self.meta.id_name)?
            ))
        } else {
            Ok(String::new())
        }
    }

    /// 业务作用：读取同一身份条件的数组片段，并在大整数与历史浮点记录混合时拒绝歧义命中。
    ///
    /// 参数说明：
    /// - `key`：由同一次身份观察派生的普通数组或单桶键。
    /// - `sub_id`：已经通过 ID 过滤器校验的业务身份文本。
    /// - `filter`：与该身份一致的完整文档选择条件。
    ///
    /// 返回：缺失键返回 None，有结果返回原始 JSON 文本；大整数分支在同一脚本内核对数组根和全部命中 ID 类型，非整数命中或异常回复返回错误。此处不构造实体，也不迁移历史数据。
    async fn json_get_identity_chunk(
        &self,
        key: &str,
        sub_id: &str,
        filter: &str,
    ) -> Result<Option<String>> {
        let guard_path = self.large_integer_guard_path(sub_id, filter)?;
        if guard_path.is_empty() {
            return self.json_get_chunk(key, filter).await;
        }
        // 身份类型与正文必须来自同一次读取，不能先独立探测后再读取已经可能被替换的数据。
        let raw: redis::Value = redis::Script::new(READ_INTEGER_ID)
            .key(key)
            .arg(filter)
            .arg(&guard_path)
            .invoke_async(&mut self.client.conn())
            .await?;
        // 仅允许缺失或原始 JSON 字节，不能把状态词或计数转换成业务文档。
        match raw {
            redis::Value::Nil => Ok(None),
            redis::Value::BulkString(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|_| NasaRedisError::Codec("ARRAY JSON 回复不是有效 UTF-8".into())),
            _ => Err(NasaRedisError::Codec(
                "ARRAY 身份读取回复类型不符合约束".into(),
            )),
        }
    }

    /// 业务作用：读取给定数组键和过滤路径的原始 RedisJSON 结果。
    ///
    /// 参数说明：
    /// - `key`：目标普通数组或桶的完整 Redis key。
    /// - `filter`：已经构造的 JSONPath 选择路径。
    ///
    /// 返回：Nil 返回 None，UTF-8 BulkString 返回正文；其它类型或非法 UTF-8 返回固定 Codec，命令错误向上传播。此入口不解析 JSON 结构。
    async fn json_get_chunk(&self, key: &str, filter: &str) -> Result<Option<String>> {
        let raw: redis::Value = redis::cmd("JSON.GET")
            .arg(key)
            .arg(filter)
            .query_async(&mut self.client.conn())
            .await?;
        // JSON.GET 返回 JSON 字节或缺失值，不能把状态词、数字或集合转换成业务正文。
        match raw {
            redis::Value::Nil => Ok(None),
            redis::Value::BulkString(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|_| NasaRedisError::Codec("ARRAY JSON 回复不是有效 UTF-8".into())),
            _ => Err(NasaRedisError::Codec(
                "ARRAY JSON.GET 回复类型不符合约束".into(),
            )),
        }
    }

    /// 业务作用：把数组过滤结果一次解码为业务文档列表。
    ///
    /// 参数说明：
    /// - `key`：目标普通数组或桶的完整 Redis key。
    /// - `filter`：决定命中子文档的 JSONPath 路径。
    ///
    /// 返回：缺失键和合法空数组返回空列表，其余数组经 serde 解码；其它 JSON 形态、命令或实体解码失败返回错误。Codec 只含类别与位置，自定义解码展开仍向上传播。
    async fn json_get_entities(&self, key: &str, filter: &str) -> Result<Vec<T>> {
        let chunk = self.json_get_chunk(key, filter).await?;
        decode_array_chunk(chunk.as_deref())
    }

    /// 业务作用：删除路径选中的子文档，并保持缺失键的幂等删除语义。
    ///
    /// 参数说明：
    /// - `key`：目标普通数组或桶的完整 Redis key。
    /// - `filter`：限定删除范围的 JSONPath 路径。
    ///
    /// 返回：命令成功且回复为非负 RESP 整数时返回删除数；缺失键返回零，命令或计数证据不满足约束时返回错误，异常回复不表示删除尚未执行。
    async fn json_del(&self, key: &str, filter: &str) -> Result<u64> {
        // JSON.DEL 对缺失 key 返回 0，直接执行即可维持幂等语义并避免检查与删除之间的竞态。
        let reply: redis::Value = redis::cmd("JSON.DEL")
            .arg(key)
            .arg(filter)
            .query_async(&mut self.client.conn())
            .await?;
        Ok(check_array_count_reply(reply, None)? as u64)
    }

    /// 业务作用：在同组全部桶上执行读取脚本，收集非空原始结果片段。
    ///
    /// 参数说明：
    /// - `keys`：同一业务分组的完整桶键列表，应共享 Cluster hash tag。
    /// - `filter`：每个桶共用的子文档选择路径。
    ///
    /// 返回：回复必须是数组，片段数不得超过请求桶数，每个片段必须是 UTF-8 BulkString；满足约束时返回有序正文列表，否则返回固定 Codec，脚本错误向上传播。此入口不解析 JSON 或验证实体身份。
    async fn eval_multi_get(&self, keys: &[String], filter: &str) -> Result<Vec<String>> {
        let mut inv = self.multi_get.prepare_invoke();
        for k in keys {
            inv.key(k.as_str());
        }
        inv.arg(filter);
        let reply: redis::Value = inv.invoke_async(&mut self.client.conn()).await?;
        // 每个请求桶最多贡献一个片段，超量或非数组回复不能证明结果来自本次键集合。
        let redis::Value::Array(chunks) = reply else {
            return Err(NasaRedisError::Codec("ARRAY 多桶读取回复不是数组".into()));
        };
        if chunks.len() > keys.len() {
            return Err(NasaRedisError::Codec(
                "ARRAY 多桶读取片段数超过请求桶数".into(),
            ));
        }
        chunks
            .into_iter()
            .map(|chunk| match chunk {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes)
                    .map_err(|_| NasaRedisError::Codec("ARRAY JSON 回复不是有效 UTF-8".into())),
                _ => Err(NasaRedisError::Codec(
                    "ARRAY 多桶读取片段不是 BulkString".into(),
                )),
            })
            .collect()
    }

    /// 业务作用：为跨桶删除或计数脚本绑定同组键列表和共用路径，并取得整数回复。
    ///
    /// 参数说明：
    /// - `script`：当前执行器持有的跨桶删除或计数脚本。
    /// - `keys`：同一业务分组的全部桶键，应共享 Cluster hash tag。
    /// - `filter`：传给脚本的子文档选择路径。
    ///
    /// 返回：命令成功且回复为非负 RESP 整数时返回该计数；脚本或计数证据不满足约束时返回错误，删除脚本已有副作用不会因错误撤销。
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
        // 调用方按同组布局提供共享 hash tag 的桶键；这里的命令成功不表示具备跨错误回滚能力。
        let reply: redis::Value = inv.invoke_async(&mut self.client.conn()).await?;
        check_array_count_reply(reply, None)
    }
}

/// 业务作用：核对 ARRAY 命令的计数证据，避免类型转换或异常计数被误认为操作成功。
///
/// 参数说明：
/// - `reply`：Redis 命令或脚本返回的原始回复，必须是非负 RESP 整数。
/// - `expected`：追加或覆盖应确认的文档数量；删除和查询计数不预设精确值。
///
/// 返回：回复类型、非负性与可选精确数量均满足约束时返回计数，否则返回固定 Codec；不包含回复正文，不撤销远端副作用，也不提供安全重试依据。
fn check_array_count_reply(reply: redis::Value, expected: Option<usize>) -> Result<i64> {
    let count = match reply {
        redis::Value::Int(count) if count >= 0 => count,
        _ => return Err(NasaRedisError::Codec("ARRAY 命令未返回非负整数计数".into())),
    };
    // 写入已经发出，只有精确确认数量才足以报告完成；异常回复不能作为未执行的证据。
    if expected.is_some_and(|expected| usize::try_from(count).ok() != Some(expected)) {
        return Err(NasaRedisError::Codec("ARRAY 写入确认数量不符合请求".into()));
    }
    Ok(count)
}

/// 业务作用：统一把 ARRAY 读取片段解码为匹配列表，区分缺失、空集和异常正文。
///
/// 参数说明：
/// - `chunk`：缺失键对应 None，其余为已通过 UTF-8 校验的 JSON 数组文本。
///
/// 返回：None 返回空列表，合法数组逐项解码后返回；非数组、语法或实体解码错误返回只含类别与位置的 Codec，不包含正文或自定义错误消息。自定义 Deserialize 的展开仍向调用方传播。
fn decode_array_chunk<T: DeserializeOwned>(chunk: Option<&str>) -> Result<Vec<T>> {
    match chunk {
        None => Ok(Vec::new()),
        Some(chunk) => serde_json::from_str(chunk).map_err(|error| {
            // 空字符串、null 和对象不能代表已确认的空匹配，错误也不能携带业务正文。
            NasaRedisError::Codec(format!(
                "ARRAY JSON 解码失败: {:?} at {}:{}",
                error.classify(),
                error.line(),
                error.column()
            ))
        }),
    }
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
