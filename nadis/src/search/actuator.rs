// SearchActuator 提供能力检查、索引管理、文档读写与查询。
// 能力命令失败时返回配置或 Redis 错误。索引以 FT.INFO 可提取结构归一化比较，
// ValidateOnly 发现漂移时返回差异；仅显式选择 RecreateExplicitly 才删除并重建。
// 单条写入可直接执行，批量写入可通过 PipelineSession 合批。

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::query::Query;
use super::{DataType, DocMeta, RedisDocument};
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// HASH save Lua:原子 **HDEL(现有字段 − 新字段)+ HSET(新字段)**。
/// KEYS[1]=hash key;ARGV[1]=新字段数 n;ARGV[2..]=f1,v1,f2,v2,...(共 2n 个)。
/// 删本次缺失的旧字段(实体字段 Some→None 时旧值不残留被索引),再覆盖写新字段。
const HASH_SAVE_LUA: &str = r#"
local n = tonumber(ARGV[1])
local keep = {}
for i = 0, n - 1 do keep[ARGV[2 + i * 2]] = true end
local existing = redis.call('HKEYS', KEYS[1])
for _, k in ipairs(existing) do
    if not keep[k] then redis.call('HDEL', KEYS[1], k) end
end
if n > 0 then
    local args = {}
    for i = 2, #ARGV do args[#args + 1] = ARGV[i] end
    redis.call('HSET', KEYS[1], unpack(args))
end
return 1
"#;

/// 索引存在性/漂移处理策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPolicy {
    /// 只校验:索引必须存在且 schema 与 meta 一致,否则报错带 diff。
    ValidateOnly,
    /// 缺失则创建;存在则校验一致(默认)。
    CreateIfMissing,
    /// 显式重建:FT.DROPINDEX(不带 DD,保留文档)后重建——唯一允许 DROP 的路径。
    RecreateExplicitly,
}

/// 类型化搜索执行器(进程内按 T 构建一次,长存复用)。
pub struct SearchActuator<T: RedisDocument> {
    client: Arc<RedisClient>,
    meta: &'static DocMeta,
    _t: PhantomData<T>,
}

impl<T: RedisDocument> SearchActuator<T> {
    /// 业务作用：校验文档模式并探测搜索命令，绑定供该文档类型复用的执行器。
    ///
    /// 参数说明：
    /// - `client`：执行索引管理和文档命令的已初始化客户端。
    ///
    /// 返回：元数据合法、非 ARRAY 且 FT._LIST 与必要的 JSON.TYPE 命令未报错时返回执行器，不创建索引；配置或命令失败返回错误。探测不校验成功回复形态，也不证明后续命令的全部权限。
    pub async fn bind(client: Arc<RedisClient>) -> Result<Self> {
        let meta = T::meta();
        meta.validate()?;
        // ARRAY 系不经 FT 索引(JSONPath filter 通道),走 JsonArrayOps——绑错执行器
        // 在 bind 时就拦下,不等到发命令
        if meta.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "{} 是 ARRAY 模式({:?}),请用 JsonArrayOps 而非 SearchActuator",
                meta.index, meta.data_type
            )));
        }
        // 绑定前要求搜索命令可调用；权限或连接失败也必须拒绝，不能据此断言模块缺失。
        let ft: std::result::Result<redis::Value, _> =
            redis::cmd("FT._LIST").query_async(&mut client.conn()).await;
        if let Err(e) = ft {
            return Err(NasaRedisError::Config(format!(
                "RediSearch 模块不可用(需 Redis Stack):{e}"
            )));
        }
        if matches!(meta.data_type, DataType::Json) {
            let js: std::result::Result<redis::Value, _> = redis::cmd("JSON.TYPE")
                .arg("__nasa_capability_check__")
                .query_async(&mut client.conn())
                .await;
            if let Err(e) = js {
                if e.to_string().contains("unknown command") {
                    return Err(NasaRedisError::Config(format!(
                        "RedisJSON 模块不可用(JSON DataType 需要):{e}"
                    )));
                }
                //**非 unknown command 的错误(网络/权限/超时)不静默忽略**
                // JSON.TYPE 对缺失 key 返回 nil；其它错误不能证明模块可用，绑定阶段必须拒绝。
                return Err(e.into());
            }
        }
        Ok(Self {
            client,
            meta,
            _t: PhantomData,
        })
    }

    /// 业务作用：提供执行器绑定时冻结的索引和键布局元数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回静态元数据引用，不重新读取 Redis。
    pub fn meta(&self) -> &'static DocMeta {
        self.meta
    }

    // ─────────────────────── 索引管理 ───────────────────────

    /// 业务作用：按调用方策略处理索引缺失及已有索引的元数据漂移。
    ///
    /// 参数说明：
    /// - `policy`：仅校验、缺失创建或显式删除后重建的策略。
    ///
    /// 返回：所选校验或创建完成时成功；缺失、漂移或命令失败返回错误。显式重建删除索引时保留文档，删除后创建失败不会恢复旧索引。
    pub async fn ensure_index(&self, policy: IndexPolicy) -> Result<()> {
        let exists = self.index_exists().await?;
        match (policy, exists) {
            (IndexPolicy::ValidateOnly, false) => Err(NasaRedisError::Config(format!(
                "索引 {} 不存在(ValidateOnly)",
                self.meta.index
            ))),
            (IndexPolicy::ValidateOnly, true) | (IndexPolicy::CreateIfMissing, true) => {
                self.validate_schema().await
            }
            (IndexPolicy::CreateIfMissing, false) => self.create_index().await,
            (IndexPolicy::RecreateExplicitly, existed) => {
                if existed {
                    // 策略必须显式允许重建才删除旧定义；保留文档，但后续创建失败不恢复旧索引。
                    let _: String = redis::cmd("FT.DROPINDEX")
                        .arg(&self.meta.index)
                        .query_async(&mut self.client.conn())
                        .await?;
                    tracing::warn!(index = %self.meta.index, "索引已显式 DROP(文档保留),即将重建");
                }
                self.create_index().await
            }
        }
    }

    /// 业务作用：通过索引信息命令区分已有索引与可识别的未知索引错误。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：命令成功返回 true；错误文本包含受支持的未知索引短语时返回 false，其它错误传播。不校验成功回复结构。
    pub async fn index_exists(&self) -> Result<bool> {
        let r: std::result::Result<redis::Value, redis::RedisError> = redis::cmd("FT.INFO")
            .arg(&self.meta.index)
            .query_async(&mut self.client.conn())
            .await;
        match r {
            Ok(_) => Ok(true),
            // 只把明确的未知索引响应视为不存在，避免将权限、模块或代理错误误判为可创建索引。
            Err(e) => {
                if is_unknown_index_error(&e) {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// 业务作用：按存储类型、固定键前缀和字段定义创建搜索索引。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建命令成功且回复可转换为字符串时成功；命令失败返回错误。此入口不复验新索引的画像，也不撤销已提交的创建。
    async fn create_index(&self) -> Result<()> {
        let mut cmd = redis::cmd("FT.CREATE");
        cmd.arg(&self.meta.index)
            .arg("ON")
            .arg(match self.meta.data_type {
                DataType::Hash => "HASH",
                // bind 已拒 ARRAY 系,这里只剩 Json
                _ => "JSON",
            })
            .arg("PREFIX")
            .arg(1)
            // 占位符模式给 literal head(第一个 { 之前):FT PREFIX 按 startsWith
            // 匹配,literal head 覆盖该 entity 全部 key 变体
            .arg(self.meta.literal_prefix())
            .arg("SCHEMA");
        for a in self.meta.schema_args() {
            cmd.arg(a);
        }
        let _: String = cmd.query_async(&mut self.client.conn()).await?;
        tracing::info!(index = %self.meta.index, "索引已创建");
        Ok(())
    }

    /// 业务作用：比较已有索引中可提取的存储类型、前缀、声明字段与支持的字段修饰。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发现声明字段缺失、类型或已比较的修饰不符时返回漂移错误，不自动删除索引；校验通过返回成功。缺失的存储类型、前缀和权重信息不参与对应比较，额外字段及未支持的修饰不在此拒绝。
    async fn validate_schema(&self) -> Result<()> {
        let info: redis::Value = redis::cmd("FT.INFO")
            .arg(&self.meta.index)
            .query_async(&mut self.client.conn())
            .await?;
        let prof = normalize_index(&info);
        let idx = &self.meta.index;
        let drift = |m: String| {
            NasaRedisError::Config(format!(
                "索引 {idx} schema 漂移:{m}(禁自动 DROP,需 RecreateExplicitly)"
            ))
        };

        // ① ON HASH/JSON(bind 已拒 ARRAY 系,这里只剩 Hash / Json)
        let want_kt = if matches!(self.meta.data_type, DataType::Hash) {
            "HASH"
        } else {
            "JSON"
        };
        if !prof.key_type.is_empty() && !prof.key_type.eq_ignore_ascii_case(want_kt) {
            return Err(drift(format!(
                "ON 类型期望 {want_kt} 实际 {}",
                prof.key_type
            )));
        }

        // ② PREFIX：返回的 prefixes 必须包含声明的 literal head，否则文档无法进入索引。
        let want_prefix = self.meta.literal_prefix();
        if !prof.prefixes.is_empty() && !prof.prefixes.iter().any(|p| p == want_prefix) {
            return Err(drift(format!(
                "PREFIX 期望含 \"{want_prefix}\" 实际 {:?}",
                prof.prefixes
            )));
        }

        // ③ + ④ 逐字段
        for f in &self.meta.fields {
            let alias = f.alias_or_name();
            let want_ty = match f.ftype {
                super::FieldType::Tag {
                    ..
                } => "TAG",
                super::FieldType::Text {
                    ..
                } => "TEXT",
                super::FieldType::Numeric {
                    ..
                } => "NUMERIC",
                super::FieldType::Geo => "GEO",
            };
            let Some(ai) = prof.attrs.get(alias) else {
                return Err(drift(format!(
                    "缺字段 \"{alias}\"(实际有 {:?})",
                    prof.attrs.keys().collect::<Vec<_>>()
                )));
            };
            if !ai.ftype.eq_ignore_ascii_case(want_ty) {
                return Err(drift(format!(
                    "字段 \"{alias}\" 类型期望 {want_ty} 实际 {}",
                    ai.ftype
                )));
            }
            // SORTABLE 双向比对(GEO 无 sortable 概念,跳过)
            let want_sortable = match f.ftype {
                super::FieldType::Tag {
                    sortable,
                    ..
                }
                | super::FieldType::Text {
                    sortable,
                    ..
                }
                | super::FieldType::Numeric {
                    sortable,
                    ..
                } => sortable,
                super::FieldType::Geo => continue,
            };
            if want_sortable != ai.sortable {
                return Err(drift(format!(
                    "字段 \"{alias}\" SORTABLE 期望 {want_sortable} 实际 {}",
                    ai.sortable
                )));
            }
            // TAG:SEPARATOR / CASESENSITIVE
            if let super::FieldType::Tag {
                separator,
                case_sensitive,
                ..
            } = &f.ftype
            {
                if let (Some(want), Some(got)) = (separator, &ai.separator) {
                    if want.to_string() != *got {
                        return Err(drift(format!(
                            "字段 \"{alias}\" SEPARATOR 期望 {want:?} 实际 {got:?}"
                        )));
                    }
                }
                if *case_sensitive != ai.case_sensitive {
                    return Err(drift(format!(
                        "字段 \"{alias}\" CASESENSITIVE 期望 {case_sensitive} 实际 {}",
                        ai.case_sensitive
                    )));
                }
            }
            // TEXT:WEIGHT(浮点容差)/ NOSTEM / NOINDEX
            if let super::FieldType::Text {
                weight,
                no_stem,
                no_index,
                ..
            } = &f.ftype
            {
                if let Some(got) = ai.weight {
                    if (got - *weight).abs() > 1e-6 {
                        return Err(drift(format!(
                            "字段 \"{alias}\" WEIGHT 期望 {weight} 实际 {got}"
                        )));
                    }
                }
                if *no_stem != ai.no_stem {
                    return Err(drift(format!(
                        "字段 \"{alias}\" NOSTEM 期望 {no_stem} 实际 {}",
                        ai.no_stem
                    )));
                }
                if *no_index != ai.no_index {
                    return Err(drift(format!(
                        "字段 \"{alias}\" NOINDEX 期望 {no_index} 实际 {}",
                        ai.no_index
                    )));
                }
            }
            // NUMERIC:NOINDEX
            if let super::FieldType::Numeric {
                no_index,
                ..
            } = &f.ftype
            {
                if *no_index != ai.no_index {
                    return Err(drift(format!(
                        "字段 \"{alias}\" NOINDEX 期望 {no_index} 实际 {}",
                        ai.no_index
                    )));
                }
            }
        }
        Ok(())
    }

    // ─────────────────────── 写入 / id 直达───────────────────────

    /// 业务作用：从实体的占位符值和业务 ID 派生完整文档键。
    ///
    /// 参数说明：
    /// - `doc`：提供 ID 及前缀占位符值的业务实体。
    ///
    /// 返回：键布局及动态值符合元数据约束时返回键；派生失败返回配置错误，不访问 Redis。
    fn key_for(&self, doc: &T) -> Result<String> {
        if self.meta.has_placeholder() {
            let parts = doc.placeholder_parts();
            let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            self.meta.key_of_parts(&refs, &doc.id())
        } else {
            self.meta.key_of(&doc.id())
        }
    }

    /// 业务作用：派生实体键并按绑定存储类型覆盖保存单个文档。
    ///
    /// 参数说明：
    /// - `doc`：提供键身份及写入字段或 JSON 正文的业务实体。
    ///
    /// 返回：派生、编码及写命令成功时成功；失败传播。写入结果未知时调用方需核实远端状态，索引可见性不在此等待。
    pub async fn save(&self, doc: &T) -> Result<()>
    where
        T: Serialize,
    {
        let key = self.key_for(doc)?;
        self.save_at(&key, doc).await
    }

    /// 业务作用：准备 HASH 字段替换脚本或省略对象空值的 JSON 根覆盖命令。
    ///
    /// 参数说明：
    /// - `key`：已派生的完整文档键。
    /// - `doc`：提供完整字段集合或 JSON 正文的实体。
    ///
    /// 返回：返回尚未发送的命令，JSON 编码失败返回错误。HASH 脚本先删除未保留字段再写新字段，脚本错误不会回滚已经发生的修改。
    fn build_save_cmd(&self, key: &str, doc: &T) -> Result<redis::Cmd>
    where
        T: Serialize,
    {
        match self.meta.data_type {
            DataType::Hash => {
                let fields = doc.to_fields();
                let mut cmd = redis::cmd("EVAL");
                cmd.arg(HASH_SAVE_LUA).arg(1).arg(key).arg(fields.len());
                for (f, v) in &fields {
                    cmd.arg(f).arg(v);
                }
                Ok(cmd)
            }
            _ => {
                // bind 已拒绝 ARRAY 系；Json 写入时省略 null 字段以对齐既有 NON_NULL 语义。
                let body = super::to_json_omit_null(doc)?;
                let mut cmd = redis::cmd("JSON.SET");
                cmd.arg(key).arg("$").arg(&body);
                Ok(cmd)
            }
        }
    }

    /// 业务作用：执行已定位文档的完整覆盖命令。
    ///
    /// 参数说明：
    /// - `key`：目标文档完整键。
    /// - `doc`：提供覆盖内容的实体。
    ///
    /// 返回：构造和执行命令成功时成功；编码或命令失败返回错误。当前只要求取得原始成功回复，不验证具体回复值。
    async fn save_at(&self, key: &str, doc: &T) -> Result<()>
    where
        T: Serialize,
    {
        let cmd = self.build_save_cmd(key, doc)?;
        let _: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        Ok(())
    }

    /// 业务作用：按会话命令上限分块提交文档覆盖写入，并逐票据传播错误。
    ///
    /// 参数说明：
    /// - `docs`：各自派生键并编码的实体列表。
    ///
    /// 返回：空输入返回零，全部批次和票据成功返回输入长度；失败返回错误。仅当前块在发送前准备，先前批次可能已写入且不会回滚；票据不校验具体成功回复值。
    pub async fn save_all(&self, docs: &[T]) -> Result<usize>
    where
        T: Serialize,
    {
        if docs.is_empty() {
            return Ok(0);
        }
        let chunk_sz = self.client.config().pipeline.session_max_commands.max(1);
        for chunk in docs.chunks(chunk_sz) {
            let mut sess = self.client.pipeline();
            let mut tickets = Vec::with_capacity(chunk.len());
            for doc in chunk {
                let key = self.key_for(doc)?;
                let cmd = self.build_save_cmd(&key, doc)?;
                tickets.push(sess.enqueue::<redis::Value>(cmd)?);
            }
            sess.execute().await?;
            // 单次 flush 后逐条取结果:Server 错误(WRONGTYPE 等)/ ExecutionUnknown 都会上抛
            for t in tickets {
                t.await_result()?;
            }
        }
        Ok(docs.len())
    }

    /// 业务作用：按无占位符键布局直接读取一个文档。
    ///
    /// 参数说明：
    /// - `id`：用于派生完整键的文档业务 ID。
    ///
    /// 返回：缺失返回 None，存在且解码成功返回文档；占位符布局、键派生或读取解码失败返回错误。
    pub async fn find_by_id(&self, id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let key = self.meta.key_of(id)?;
        self.find_at(&key).await
    }

    /// 业务作用：按有序前缀占位符和业务 ID 定位并读取文档。
    ///
    /// 参数说明：
    /// - `parts`：按前缀占位符出现顺序提供的动态值。
    /// - `id`：追加到布局末尾的文档业务 ID。
    ///
    /// 返回：缺失返回 None，成功返回文档；动态值、键派生、命令或解码失败返回错误。
    pub async fn find_by_parts(&self, parts: &[&str], id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let key = self.meta.key_of_parts(parts, id)?;
        self.find_at(&key).await
    }

    /// 业务作用：从完整键读取 HASH 或 JSON 根，并重建业务实体。
    ///
    /// 参数说明：
    /// - `key`：待直接读取的完整文档键。
    ///
    /// 返回：HASH 字段为空或 JSON 回复缺失时返回 None；HASH 经 from_fields 重建，JSON 按数组解码后取末项。命令或解码失败传播，自定义解码展开仍向上传播。
    async fn find_at(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.meta.data_type {
            DataType::Hash => {
                let fields: HashMap<String, String> = self.client.h_get_all(key).await?;
                if fields.is_empty() {
                    return Ok(None);
                }
                Ok(Some(T::from_fields(&fields)?))
            }
            _ => {
                let raw: Option<String> = redis::cmd("JSON.GET")
                    .arg(key)
                    .arg("$")
                    .query_async(&mut self.client.conn())
                    .await?;
                match raw {
                    None => Ok(None),
                    Some(s) => {
                        // JSON.GET $ 返回数组包裹:[{...}]
                        let mut v: Vec<T> = serde_json::from_str(&s)
                            .map_err(|e| NasaRedisError::Codec(format!("JSON 解码失败: {e}")))?;
                        Ok(v.pop())
                    }
                }
            }
        }
    }

    /// 业务作用：按无占位符布局删除 ID 对应的完整文档。
    ///
    /// 参数说明：
    /// - `id`：待删除文档业务 ID。
    ///
    /// 返回：删除数大于零返回 true，缺失返回 false；键布局或删除命令失败传播。
    pub async fn remove_by_id(&self, id: &str) -> Result<bool> {
        Ok(self.client.del(&[&self.meta.key_of(id)?]).await? > 0)
    }

    /// 业务作用：按占位符分组及 ID 删除完整文档。
    ///
    /// 参数说明：
    /// - `parts`：按前缀占位符顺序提供的分组值。
    /// - `id`：待删除文档业务 ID。
    ///
    /// 返回：删除数大于零返回 true，缺失返回 false；派生或删除命令失败传播。
    pub async fn remove_by_parts(&self, parts: &[&str], id: &str) -> Result<bool> {
        Ok(self
            .client
            .del(&[&self.meta.key_of_parts(parts, id)?])
            .await?
            > 0)
    }

    // 索引管理与直接文档操作。

    /// 业务作用：按显式选择删除索引定义，并可同时删除索引文档。
    ///
    /// 参数说明：
    /// - `delete_documents`：为 true 时携带 DD，允许删除被索引的文档；false 仅删索引。
    ///
    /// 返回：命令成功或错误文本被识别为未知索引时成功，其它错误传播；文档删除为不可自动恢复的远端副作用。
    pub async fn drop_index(&self, delete_documents: bool) -> Result<()> {
        let mut cmd = redis::cmd("FT.DROPINDEX");
        cmd.arg(&self.meta.index);
        if delete_documents {
            cmd.arg("DD");
        }
        // 索引已经不存在时视为删除完成；权限、模块和代理错误必须返回，不能伪报清理成功。
        match cmd.query_async::<()>(&mut self.client.conn()).await {
            Ok(()) => Ok(()),
            Err(e) if is_unknown_index_error(&e) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// 业务作用：直接检查无占位符键布局下的文档键是否存在。
    ///
    /// 参数说明：
    /// - `id`：待检查的业务 ID。
    ///
    /// 返回：返回键存在性，不解码文档或确认其已进入索引；派生及命令错误传播。
    pub async fn exists_by_id(&self, id: &str) -> Result<bool> {
        let key = self.meta.key_of(id)?;
        self.client.exists(&key).await
    }

    /// 业务作用：按占位符与 ID 派生完整键并检查存在性。
    ///
    /// 参数说明：
    /// - `parts`：按前缀占位符顺序提供的动态值。
    /// - `id`：待检查文档业务 ID。
    ///
    /// 返回：返回键存在性，不校验文档类型或索引状态；派生及命令错误传播。
    pub async fn exists_by_parts(&self, parts: &[&str], id: &str) -> Result<bool> {
        let key = self.meta.key_of_parts(parts, id)?;
        self.client.exists(&key).await
    }

    /// 业务作用：定位无占位符 JSON 文档并对指定路径执行整数增量。
    ///
    /// 参数说明：
    /// - `id`：JSON 文档业务 ID。
    /// - `json_path`：由调用方选择的 RedisJSON 更新路径。
    /// - `delta`：以整数参数发送的有符号增量。
    ///
    /// 返回：返回可转换为字符串的 RedisJSON 回复；HASH 模式、派生或命令失败返回错误，不解析结果文本。
    pub async fn json_num_incr_by(&self, id: &str, json_path: &str, delta: i64) -> Result<String> {
        self.json_num_incr_by_at(&self.meta.key_of(id)?, json_path, delta)
            .await
    }

    /// 业务作用：按占位符分组和 ID 定位 JSON 文档并更新数值路径。
    ///
    /// 参数说明：
    /// - `parts`：按前缀占位符顺序提供的动态值。
    /// - `id`：JSON 文档业务 ID。
    /// - `json_path`：由调用方选择的 RedisJSON 更新路径。
    /// - `delta`：以整数参数发送的有符号增量。
    ///
    /// 返回：成功返回 RedisJSON 回复文本；模式、键或命令错误传播，不保证多值路径只更新一个节点。
    pub async fn json_num_incr_by_parts(
        &self,
        parts: &[&str],
        id: &str,
        json_path: &str,
        delta: i64,
    ) -> Result<String> {
        self.json_num_incr_by_at(&self.meta.key_of_parts(parts, id)?, json_path, delta)
            .await
    }

    /// 业务作用：在已定位的 JSON 文档路径上执行整数增量命令。
    ///
    /// 参数说明：
    /// - `key`：目标 JSON 文档完整键。
    /// - `json_path`：决定被更新节点集合的 RedisJSON 路径。
    /// - `delta`：有符号整数增量。
    ///
    /// 返回：HASH 模式在发送前拒绝，其余情况返回命令回复字符串；命令或转换失败传播，已发生的更新不会补偿。
    async fn json_num_incr_by_at(&self, key: &str, json_path: &str, delta: i64) -> Result<String> {
        if self.meta.data_type == DataType::Hash {
            return Err(NasaRedisError::Config(
                "json_num_incr_by 仅 JSON 模式可用(当前 Hash 模式)".into(),
            ));
        }
        let s: String = redis::cmd("JSON.NUMINCRBY")
            .arg(key)
            .arg(json_path)
            .arg(delta)
            .query_async(&mut self.client.conn())
            .await?;
        Ok(s)
    }

    /// 业务作用：保留过滤和排序条件，以首条窗口读取文档。
    ///
    /// 参数说明：
    /// - `q`：其分页被覆盖为 LIMIT 0 1 的查询。
    ///
    /// 返回：无匹配返回 None，否则返回解码后的第一项；渲染、命令或解码错误传播。
    pub async fn find_one(&self, q: &Query) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let one = q.clone().limit(0, 1);
        Ok(self.find(&one).await?.into_iter().next())
    }

    /// 业务作用：通过查询总数判断是否有匹配文档。
    ///
    /// 参数说明：
    /// - `q`：提供过滤条件的查询，原分页不参与计数。
    ///
    /// 返回：总数大于零返回 true，空集返回 false；计数路径错误传播。
    pub async fn exists(&self, q: &Query) -> Result<bool> {
        Ok(self.count(q).await? > 0)
    }

    /// 业务作用：查询匹配键并移除固定前缀，提供文档 ID 列表。
    ///
    /// 参数说明：
    /// - `q`：提供过滤、排序及可选显式分页的查询。
    ///
    /// 返回：成功返回当前窗口的 ID；未显式分页使用最多一百万条，空集返回空列表。占位符布局、查询或命令错误传播；不会从动态前缀反推 ID。
    pub async fn find_keys(&self, q: &Query) -> Result<Vec<String>> {
        let (offset, limit) = if q.is_limit_explicit() {
            (q.effective_offset(), q.effective_limit())
        } else {
            (0, 1_000_000)
        };
        self.search_keys(q, offset, limit).await
    }

    /// 业务作用：执行无正文查询并按固定前缀转换匹配键。
    ///
    /// 参数说明：
    /// - `q`：决定过滤和排序的查询。
    /// - `offset`：覆盖查询分页的起始偏移。
    /// - `limit`：覆盖查询分页的最大返回数量。
    ///
    /// 返回：空条件哨兵返回空列表，否则返回宽松解析后的 ID；占位符布局或命令失败返回错误。缺失前缀的键保留原文，解析不能证明每个结果都属于声明前缀。
    async fn search_keys(&self, q: &Query, offset: usize, limit: usize) -> Result<Vec<String>> {
        if self.meta.has_placeholder() {
            return Err(NasaRedisError::Config(
                "find_keys/remove(query) 不支持占位符 prefix 模式(key 无法反推 id)".into(),
            ));
        }
        let expr = q.render(self.meta)?;
        if expr.is_empty() {
            return Ok(Vec::new()); // MATCH_NONE 哨兵
        }
        // 复用 render_args 保留排序与字段可排序性校验；NOCONTENT 只省略正文，不取消排序。
        // 分页范围由 find_keys 或 remove 的调用策略决定。
        let q_for_keys = q.clone().limit(offset, limit);
        let mut cmd = redis::cmd("FT.SEARCH");
        cmd.arg(&self.meta.index).arg(&expr).arg("NOCONTENT");
        for a in q_for_keys.render_args(self.meta)? {
            cmd.arg(a);
        }
        cmd.arg("DIALECT").arg(2);
        let v: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        let prefix = self.meta.literal_prefix();
        Ok(parse_search_keys(v)
            .into_iter()
            .map(|k| k.strip_prefix(prefix).unwrap_or(&k).to_string())
            .collect())
    }

    /// 业务作用：反复读取首批匹配 ID，分组删除完整文档。
    ///
    /// 参数说明：
    /// - `q`：决定删除范围的过滤及排序条件，不支持占位符前缀。
    ///
    /// 返回：返回累计删除数；无命中或一轮删除零条时结束。查询及删除失败传播，已经完成的删除不回滚；持续并发写入下不保证固定轮数结束。
    pub async fn remove(&self, q: &Query) -> Result<u64> {
        let prefix = self.meta.literal_prefix();
        let mut total = 0u64;
        loop {
            let ids = self.search_keys(q, 0, 1_000_000).await?;
            if ids.is_empty() {
                break;
            }
            let mut deleted = 0u64;
            for chunk in ids.chunks(1000) {
                let keys: Vec<String> = chunk.iter().map(|id| format!("{prefix}{id}")).collect();
                let krefs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                deleted += self.client.del(&krefs).await?;
            }
            total += deleted;
            if deleted == 0 {
                break; // 命中但删 0:索引滞后/并发,防死循环
            }
        }
        Ok(total)
    }

    // ─────────────────────── FT.SEARCH 查询族 ───────────────────────

    /// 业务作用：渲染类型化条件和窗口，执行搜索并解码结果实体。
    ///
    /// 参数说明：
    /// - `q`：提供过滤、排序及分页参数的查询。
    ///
    /// 返回：空条件哨兵返回空列表，否则返回解析所得实体；渲染、命令及实体解码错误传播。结果结构按解析器当前宽松规则处理。
    pub async fn find(&self, q: &Query) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let expr = q.render(self.meta)?;
        if expr.is_empty() {
            return Ok(Vec::new()); // MATCH_NONE 哨兵(纯标点 TEXT 值):匹配空集,不打 FT.SEARCH
        }
        let mut cmd = redis::cmd("FT.SEARCH");
        cmd.arg(&self.meta.index).arg(&expr);
        for a in q.render_args(self.meta)? {
            cmd.arg(a);
        }
        cmd.arg("DIALECT").arg(2); //恒发 DIALECT 2
        let v: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        self.parse_search(v)
    }

    /// 业务作用：忽略正文和分页，以零长度窗口读取搜索匹配总数。
    ///
    /// 参数说明：
    /// - `q`：只使用其过滤表达式的查询。
    ///
    /// 返回：空条件哨兵返回零；从数组、集合首项或 Map 的 total_results 转换计数，无法识别时返回错误。当前数值转换允许浮点和数字文本，不保证原回复是精确整数。
    pub async fn count(&self, q: &Query) -> Result<u64> {
        let expr = q.render(self.meta)?;
        if expr.is_empty() {
            return Ok(0); // MATCH_NONE 哨兵:匹配空集
        }
        let v: redis::Value = redis::cmd("FT.SEARCH")
            .arg(&self.meta.index)
            .arg(&expr)
            .arg("LIMIT")
            .arg(0)
            .arg(0)
            .arg("DIALECT")
            .arg(2) // 使用既有查询语义所需的 RediSearch dialect。
            .query_async(&mut self.client.conn())
            .await?;
        //total 取值兼容 Int/Double/Bulk/Simple——RediSearch 8 RESP3 下 total_results
        // 可能回 Double 或字符串,只匹配 Int 会误判"形态异常"。
        if let redis::Value::Array(parts) | redis::Value::Set(parts) = &v {
            if let Some(n) = parts.first().and_then(value_to_u64) {
                return Ok(n);
            }
        }
        // RESP3 Map 形态:total_results 字段(归一化)。复用 `resp3_map_get`(键兼容 Simple/BulkString)。
        if let redis::Value::Map(pairs) = &v {
            if let Some(n) = resp3_map_get(pairs, "total_results").and_then(value_to_u64) {
                return Ok(n);
            }
        }
        Err(NasaRedisError::Config("FT.SEARCH 计数响应形态异常".into()))
    }

    /// 业务作用：渲染过滤与聚合管道，读取列名到值的聚合行。
    ///
    /// 参数说明：
    /// - `q`：提供聚合前过滤表达式的查询。
    /// - `agg`：决定载入、分组、归约、派生、排序及窗口的聚合管道。
    ///
    /// 返回：空条件哨兵返回空列表；成功返回文本化列值。渲染、命令或顶层形态不符返回错误，内部缺失或非数组行可被跳过，不验证声明总数。
    pub async fn aggregate(
        &self,
        q: &Query,
        agg: &super::query::Aggregate,
    ) -> Result<Vec<HashMap<String, String>>> {
        let expr = q.render(self.meta)?;
        if expr.is_empty() {
            return Ok(Vec::new()); // MATCH_NONE 哨兵:过滤条件匹配空集 → 无聚合行
        }
        let mut cmd = redis::cmd("FT.AGGREGATE");
        cmd.arg(&self.meta.index).arg(&expr);
        for a in agg.render_args(self.meta)? {
            cmd.arg(a);
        }
        cmd.arg("DIALECT").arg(2); // 使用既有查询语义所需的 RediSearch dialect。
        let v: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        // RESP3(协商 HELLO 3):`{total_results, results:[{extra_attributes:{...}},...]}`
        // 逐行提取 extra_attributes，与 count/find 共用一致的 RESP3 归一化语义。
        if let redis::Value::Map(pairs) = &v {
            let mut out = Vec::new();
            if let Some(redis::Value::Array(rows)) = resp3_map_get(pairs, "results") {
                for row in rows {
                    out.push(resp3_row_fields(row));
                }
            }
            return Ok(out);
        }
        // RESP2 响应形态:[total, [k1,v1,k2,v2,...], [..], ...]。
        let redis::Value::Array(parts) = v else {
            return Err(NasaRedisError::Config(
                "FT.AGGREGATE 响应形态异常(非 RESP2 数组/RESP3 Map)".into(),
            ));
        };
        let mut out = Vec::new();
        let mut it = parts.into_iter();
        let _total = it.next(); // [0] = 组数
        for row in it {
            let redis::Value::Array(kv) = row else {
                continue;
            };
            let mut map = HashMap::new();
            let mut kit = kv.into_iter();
            while let (Some(k), Some(val)) = (kit.next(), kit.next()) {
                map.insert(bulk_to_string(k), bulk_to_string(val));
            }
            out.push(map);
        }
        Ok(out)
    }

    /// 业务作用：按存储类型将搜索结果字段表还原为实体。
    ///
    /// 参数说明：
    /// - `map`：单条结果的字段文本映射。
    ///
    /// 返回：HASH 委托 from_fields；JSON 优先解码 $ 字段，缺失时选映射迭代的首个值。无正文或解码失败返回错误，自定义解码展开向上传播；备用值选择不保证固定字段。
    fn map_to_item(&self, map: HashMap<String, String>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        match self.meta.data_type {
            DataType::Hash => T::from_fields(&map),
            // bind 已拒 ARRAY 系,这里只剩 Json:文档在 "$" 字段(整文档 JSON 串)。
            //**缺 `$` 回退取首个字段值**——某些
            // RETURN/投影形态不带 `$` 根字段而是单值返回;回退避免误报 Config 缺字段。
            _ => {
                let raw = map
                    .get("$")
                    .or_else(|| map.values().next())
                    .ok_or_else(|| {
                        NasaRedisError::Config("FT.SEARCH(JSON)无任何字段可解".into())
                    })?;
                serde_json::from_str(raw)
                    .map_err(|e| NasaRedisError::Codec(format!("JSON 解码失败: {e}")))
            }
        }
    }

    /// 业务作用：将 RESP2 文档对或 RESP3 结果行转换为实体列表。
    ///
    /// 参数说明：
    /// - `v`：FT.SEARCH 的原始回复。
    ///
    /// 返回：可识别顶层返回已解码列表，实体解码失败传播；其它顶层返回错误。RESP3 缺 results 返回空列表，RESP2 不完整行终止解析、奇数字段尾项被忽略，不核对总数或文档键。
    fn parse_search(&self, v: redis::Value) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        // RESP3 Map 形态:取 results 数组,逐行 extra_attributes → field 表(与 count 一致归一化)
        if let redis::Value::Map(pairs) = &v {
            let mut out = Vec::new();
            if let Some(redis::Value::Array(rows)) = resp3_map_get(pairs, "results") {
                for row in rows {
                    out.push(self.map_to_item(resp3_row_fields(row))?);
                }
            }
            return Ok(out);
        }
        let redis::Value::Array(parts) = v else {
            return Err(NasaRedisError::Config(
                "FT.SEARCH 响应形态异常(非 RESP2 数组/RESP3 Map)".into(),
            ));
        };
        let mut out = Vec::new();
        let mut it = parts.into_iter();
        let _total = it.next(); // [0] = total
        while let Some(_key) = it.next() {
            let Some(redis::Value::Array(fields)) = it.next() else {
                break;
            };
            // fields = [f1, v1, f2, v2, ...]
            let mut map: HashMap<String, String> = HashMap::new();
            let mut fit = fields.into_iter();
            while let (Some(f), Some(val)) = (fit.next(), fit.next()) {
                let fname = bulk_to_string(f);
                let fval = bulk_to_string(val);
                map.insert(fname, fval);
            }
            out.push(self.map_to_item(map)?);
        }
        Ok(out)
    }
}

/// 业务作用：按错误文本识别调用方支持的未知索引提示。
///
/// 参数说明：
/// - `e`：FT.INFO 或 FT.DROPINDEX 返回的 Redis 错误。
///
/// 返回：转小写后包含 unknown index name 或 no such index 时为 true，否则为 false；不核验错误码。
fn is_unknown_index_error(e: &redis::RedisError) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("unknown index name") || msg.contains("no such index")
}

/// 业务作用：把搜索字段或键的原始值转换为当前文本表示。
///
/// 参数说明：
/// - `v`：待消费的 Redis 值。
///
/// 返回：字符串按 UTF-8 容错转换，数值按十进制文本转换，其它类型使用调试表示；无效 UTF-8 会替换字符，不返回转换错误。
fn bulk_to_string(v: redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        redis::Value::SimpleString(s) => s,
        ref other => value_to_string(other),
    }
}

/// 业务作用：从无正文搜索回复提取文档键。
///
/// 参数说明：
/// - `v`：RESP2 键列表或 RESP3 results 映射。
///
/// 返回：RESP2 跳过首项并转换其余值；RESP3 提取可识别行的 id。缺失、异常结构返回空列表或跳过，不验证总数、重复键及键来源。
fn parse_search_keys(v: redis::Value) -> Vec<String> {
    if let redis::Value::Map(pairs) = &v {
        let mut out = Vec::new();
        if let Some(redis::Value::Array(rows)) = resp3_map_get(pairs, "results") {
            for row in rows {
                if let redis::Value::Map(rp) = row {
                    if let Some(idv) = resp3_map_get(rp, "id") {
                        out.push(value_to_string(idv));
                    }
                }
            }
        }
        return out;
    }
    if let redis::Value::Array(parts) = v {
        let mut it = parts.into_iter();
        let _total = it.next(); // [0] = total
        return it.map(bulk_to_string).collect();
    }
    Vec::new()
}

/// 业务作用：将计数回复的整数、浮点或数字文本转换为无符号计数。
///
/// 参数说明：
/// - `v`：原始计数值。
///
/// 返回：非负整数及浮点、可解析的去空白数字文本返回 Some，其它返回 None；浮点按 Rust 转换截去小数并对超界值饱和，不保证精确计数。
fn value_to_u64(v: &redis::Value) -> Option<u64> {
    match v {
        redis::Value::Int(n) => (*n >= 0).then_some(*n as u64),
        redis::Value::Double(d) => (*d >= 0.0).then_some(*d as u64),
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).trim().parse::<u64>().ok(),
        redis::Value::SimpleString(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
}

/// 业务作用：为搜索结果列生成统一文本表示。
///
/// 参数说明：
/// - `v`：待借用的原始列值。
///
/// 返回：字符串按 UTF-8 容错转换，整数和浮点生成文本，其它类型返回调试表示；不拒绝复杂值或无效 UTF-8。
fn value_to_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(n) => n.to_string(),
        redis::Value::Double(d) => d.to_string(),
        other => format!("{other:?}"),
    }
}

/// 业务作用：从 RESP3 映射按可识别的文本字段名查找值。
///
/// 参数说明：
/// - `pairs`：保留回复顺序的键值对。
/// - `key`：要求精确匹配的字段名。
///
/// 返回：返回首个文本键匹配的值引用，否则为 None；不拒绝重复字段。
fn resp3_map_get<'a>(
    pairs: &'a [(redis::Value, redis::Value)],
    key: &str,
) -> Option<&'a redis::Value> {
    pairs
        .iter()
        .find_map(|(k, v)| (val_str(k).as_deref() == Some(key)).then_some(v))
}

/// 业务作用：提取 RESP3 结果行的字段映射并转换为文本。
///
/// 参数说明：
/// - `row`：单条搜索或聚合结果行。
///
/// 返回：优先使用 extra_attributes，缺失时使用 attributes；可识别 Map 形成字段表，其余返回空表。同名转换后字段由后值覆盖。
fn resp3_row_fields(row: &redis::Value) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let redis::Value::Map(pairs) = row {
        let fields =
            resp3_map_get(pairs, "extra_attributes").or_else(|| resp3_map_get(pairs, "attributes"));
        if let Some(redis::Value::Map(fields)) = fields {
            for (f, v) in fields {
                map.insert(value_to_string(f), value_to_string(v));
            }
        }
    }
    map
}

/// 业务作用：提取索引信息中支持的字符串值。
///
/// 参数说明：
/// - `v`：待检查的 Redis 元数据值。
///
/// 返回：SimpleString 或 BulkString 返回文本，其它为 None；BulkString 无效 UTF-8 使用替换字符。
fn val_str(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// 业务作用：把索引字段权重的数值或文本表示转换为浮点。
///
/// 参数说明：
/// - `v`：字段权重的原始值。
///
/// 返回：浮点、整数及可解析字符串返回 Some，否则为 None；不检查有限性或业务允许范围。
fn val_f64(v: &redis::Value) -> Option<f64> {
    match v {
        redis::Value::Double(d) => Some(*d),
        redis::Value::Int(i) => Some(*i as f64),
        _ => val_str(v).and_then(|s| s.parse().ok()),
    }
}

/// 业务作用：统一遍历 RESP2 平铺元数据和 RESP3 映射。
///
/// 参数说明：
/// - `v`：索引信息或其子结构。
///
/// 返回：Map 保留键值对顺序，数组每两项成对且忽略孤立尾项，其它返回空列表；不校验字段唯一性。
fn info_pairs(v: &redis::Value) -> Vec<(&redis::Value, &redis::Value)> {
    match v {
        redis::Value::Map(m) => m.iter().map(|(k, v)| (k, v)).collect(),
        redis::Value::Array(a) => a
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| (&c[0], &c[1]))
            .collect(),
        _ => Vec::new(),
    }
}

/// 业务作用：提取索引字段的公开名称及字段类型映射。
///
/// 参数说明：
/// - `info`：FT.INFO 的 RESP2 或 RESP3 原始回复。
///
/// 返回：返回成功提取的字段类型表；缺失或不可识别部分被跳过，同名后值覆盖。此转换不构成完整 schema 验证。
pub fn normalize_attributes(info: &redis::Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (k, v) in info_pairs(info) {
        if !matches!(val_str(k).as_deref(), Some("attributes") | Some("fields")) {
            continue;
        }
        let redis::Value::Array(attrs) = v else {
            continue;
        };
        for attr in attrs {
            if let Some((alias, ai)) = parse_attr_value(attr) {
                out.insert(alias, ai.ftype);
            }
        }
    }
    out
}

/// 一个字段的 FT.INFO 返回结构，用于比较 type、修饰 flag、WEIGHT 与 SEPARATOR 漂移。
#[derive(Debug, Default, Clone)]
pub struct AttrInfo {
    /// RediSearch 字段类型。
    pub ftype: String,
    /// 是否支持排序。
    pub sortable: bool,
    /// 是否关闭词干还原。
    pub no_stem: bool,
    /// 是否仅存储而不建立索引。
    pub no_index: bool,
    /// 是否使用大小写敏感匹配。
    pub case_sensitive: bool,
    /// 可选的全文相关性权重。
    pub weight: Option<f64>,
    /// 可选的多值 Tag 分隔符。
    pub separator: Option<String>,
}

/// FT.INFO 中可提取的索引画像，仅包含当前漂移比较支持的属性。
#[derive(Debug, Default)]
pub struct IndexProfile {
    /// index_definition.key_type:"HASH" / "JSON"(各版本可能缺,缺=空串→比对放行)。
    pub key_type: String,
    /// index_definition.prefixes 列表(各版本可能缺)。
    pub prefixes: Vec<String>,
    /// alias → 字段画像。
    pub attrs: HashMap<String, AttrInfo>,
}

/// 业务作用：将支持的索引布尔修饰写入字段画像。
///
/// 参数说明：
/// - `info`：累积字段修饰的可变画像。
/// - `flag`：按 ASCII 大小写不敏感匹配的修饰标记。
///
/// 返回：设置 SORTABLE、NOSTEM、NOINDEX 或 CASESENSITIVE 对应状态，其它标记忽略；不清除既有状态。
fn apply_flag(info: &mut AttrInfo, flag: &str) {
    match flag.to_ascii_uppercase().as_str() {
        "SORTABLE" => info.sortable = true,
        "NOSTEM" => info.no_stem = true,
        "NOINDEX" => info.no_index = true,
        "CASESENSITIVE" => info.case_sensitive = true,
        _ => {}
    }
}

/// 业务作用：从 RESP2 混排字段定义或 RESP3 字段映射提取名称和受支持修饰。
///
/// 参数说明：
/// - `attr`：FT.INFO attributes 中的一条字段定义。
///
/// 返回：存在 alias 或 identifier 时返回名称与画像，名称优先 alias；其它顶层或无名称返回 None。缺失类型保留为空，未支持修饰忽略，不提供严格结构验证。
fn parse_attr_value(attr: &redis::Value) -> Option<(String, AttrInfo)> {
    match attr {
        // RESP3:attribute 是 Map,独立 flag 在 `flags` 子数组里。
        redis::Value::Map(_) => {
            let mut alias: Option<String> = None;
            let mut identifier: Option<String> = None;
            let mut info = AttrInfo::default();
            for (k, v) in info_pairs(attr) {
                match val_str(k)
                    .as_deref()
                    .map(|s| s.to_ascii_uppercase())
                    .as_deref()
                {
                    Some("ATTRIBUTE") | Some("ALIAS") => alias = val_str(v),
                    Some("IDENTIFIER") => identifier = val_str(v),
                    Some("TYPE") => info.ftype = val_str(v).unwrap_or_default(),
                    Some("WEIGHT") => info.weight = val_f64(v),
                    Some("SEPARATOR") => info.separator = val_str(v),
                    Some("FLAGS") => {
                        if let redis::Value::Array(flags) = v {
                            for f in flags {
                                if let Some(s) = val_str(f) {
                                    apply_flag(&mut info, &s);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            let key = alias.or(identifier)?;
            Some((key, info))
        }
        // RESP2:attribute 是平铺数组——必须单 token 推进,不能成对迭代
        // (成对迭代会把 SORTABLE 当 key 吃掉后一个 token,各版本 flag 数量奇偶不定即错位)。
        redis::Value::Array(kv) => {
            let mut alias: Option<String> = None;
            let mut identifier: Option<String> = None;
            let mut info = AttrInfo::default();
            let mut i = 0;
            while i < kv.len() {
                let Some(tok) = val_str(&kv[i]) else {
                    i += 1;
                    continue;
                };
                match tok.to_ascii_uppercase().as_str() {
                    "ATTRIBUTE" | "ALIAS" => {
                        alias = kv.get(i + 1).and_then(val_str);
                        i += 2;
                    }
                    "IDENTIFIER" => {
                        identifier = kv.get(i + 1).and_then(val_str);
                        i += 2;
                    }
                    "TYPE" => {
                        info.ftype = kv.get(i + 1).and_then(val_str).unwrap_or_default();
                        i += 2;
                    }
                    "WEIGHT" => {
                        info.weight = kv.get(i + 1).and_then(val_f64);
                        i += 2;
                    }
                    "SEPARATOR" => {
                        info.separator = kv.get(i + 1).and_then(val_str);
                        i += 2;
                    }
                    "PHONETIC" => i += 2, // 暂不参与漂移比对(matcher 串各版本表述差异大)
                    "SORTABLE" | "NOSTEM" | "NOINDEX" | "CASESENSITIVE" => {
                        apply_flag(&mut info, &tok);
                        i += 1;
                    }
                    _ => i += 1, // UNF/NOFREQS/INDEXMISSING 等:跳过
                }
            }
            let key = alias.or(identifier)?;
            Some((key, info))
        }
        _ => None,
    }
}

/// 业务作用：将索引定义和字段描述转换为漂移比较使用的画像。
///
/// 参数说明：
/// - `info`：FT.INFO 的原始索引回复。
///
/// 返回：返回可提取的存储类型、前缀和字段画像；缺失信息保留默认值，异常子结构跳过，同名字段后值覆盖，不在此返回协议错误。
pub fn normalize_index(info: &redis::Value) -> IndexProfile {
    let mut prof = IndexProfile::default();
    for (k, v) in info_pairs(info) {
        match val_str(k).as_deref() {
            Some("index_definition") => {
                // index_definition 自身是 [k,v,...](RESP2)/ Map(RESP3);取 key_type / prefixes
                for (dk, dv) in info_pairs(v) {
                    match val_str(dk).as_deref() {
                        Some("key_type") => prof.key_type = val_str(dv).unwrap_or_default(),
                        Some("prefixes") => {
                            if let redis::Value::Array(ps) = dv {
                                prof.prefixes = ps.iter().filter_map(val_str).collect();
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("attributes") | Some("fields") => {
                // attrs 外层恒为 Array;元素 RESP2 是平铺 Array、RESP3 是 Map——parse_attr_value 双形态归一。
                if let redis::Value::Array(attrs) = v {
                    for attr in attrs {
                        if let Some((alias, ai)) = parse_attr_value(attr) {
                            prof.attrs.insert(alias, ai);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    prof
}
