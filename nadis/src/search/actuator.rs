// ============================================================================
// src/search/actuator.rs —— SearchActuator:capability / 索引策略 / 读写查询。
// (文档;对照 原实现 RediSearch.Actuator<T> + RsCommandExecutor)
//
// 红线:
//   · capability 探测:缺 FT(或 JSON 模式缺 ReJSON)→ 明确的 NotSupported 错误,
//     不是一串协议错(缺模块给明确错误)
//   · 索引校验以 **FT.INFO 实测**为准(schema hash 只是缓存思想——这里直接归一化比较),
//     IndexPolicy{ValidateOnly|CreateIfMissing|RecreateExplicitly},**禁自动 DROP**:
//     发现漂移 ValidateOnly 报错带 diff,Recreate 必须显式选择;
//   · 双通道(对照 原实现):本 actuator = 查询/管理通道;批量写并入 PipelineSession
//     单条 save 由本模块负责。
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
    /// 业务作用：绑定搜索执行器,在启动期完成文档元数据校验和 RediSearch/RedisJSON 能力探测。
    ///
    /// # 参数
    /// - `client`: 已初始化的 Redis 客户端,后续索引管理、读写和查询命令都通过它执行。
    pub async fn bind(client: Arc<RedisClient>) -> Result<Self> {
        let meta = T::meta();
        meta.validate()?;
        // ARRAY 系不经 FT 索引(JSONPath filter 通道),走 JsonArrayOps——绑错执行器
        // 在 bind 时就拦下,不等到发命令(对照 原实现 JsonArraySupport.checkMeta 反向约束)
        if meta.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "{} 是 ARRAY 模式({:?}),请用 JsonArrayOps 而非 SearchActuator",
                meta.index, meta.data_type
            )));
        }
        // capability:FT._LIST 是 RediSearch 独有的轻量命令——失败即模块缺失
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
                // ——此前 fall through 到 Ok 会误判模块可用;改为上抛(JSON.TYPE 对缺失 key 返回
                // nil=Ok,正常探测不该出别的错;出错 = 真异常)。
                return Err(e.into());
            }
        }
        Ok(Self {
            client,
            meta,
            _t: PhantomData,
        })
    }

    /// 业务作用：返回当前文档执行器使用的静态元数据。
    pub fn meta(&self) -> &'static DocMeta {
        self.meta
    }

    // ─────────────────────── 索引管理 ───────────────────────

    /// 业务作用：按策略确保索引(校验以 FT.INFO 实测为准;漂移 diff 进错误文本)。
    ///
    /// # 参数
    /// - `policy`: 索引存在性和漂移处理策略,决定仅校验、缺失创建还是显式重建。
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
                    // 唯一允许 DROP 的路径(显式选择;不带 DD = 保留文档数据)
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

    /// 业务作用：索引是否存在(FT.INFO;对照 原实现 `indexExists`)。只识别明确的"未知索引"判不存在,其余错误上抛。
    pub async fn index_exists(&self) -> Result<bool> {
        let r: std::result::Result<redis::Value, redis::RedisError> = redis::cmd("FT.INFO")
            .arg(&self.meta.index)
            .query_async(&mut self.client.conn())
            .await;
        match r {
            Ok(_) => Ok(true),
            //**只**识别 Redis 明确的"未知索引"才判不存在(对齐 原实现 的
            // `Unknown Index name`/`no such index`)。旧逻辑"错误串含 index 即不存在"过宽——
            // 权限/模块/参数/代理错误文本都可能含 index,会被误判后错误执行 FT.CREATE。
            // 其余一切错误(含语义含混的)必须上抛,绝不静默触发重建。
            Err(e) => {
                if is_unknown_index_error(&e) {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// 业务作用：根据绑定的 RediSearch 元信息创建索引。
    ///
    /// 仅在 `ensure_index` 判定索引缺失且策略允许自动创建时调用,字段定义来自业务实体上的 search 元数据。
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
            // 匹配,literal head 覆盖该 entity 全部 key 变体(对照 原实现 literalPrefix)
            .arg(self.meta.literal_prefix())
            .arg("SCHEMA");
        for a in self.meta.schema_args() {
            cmd.arg(a);
        }
        let _: String = cmd.query_async(&mut self.client.conn()).await?;
        tracing::info!(index = %self.meta.index, "索引已创建");
        Ok(())
    }

    /// 业务作用：将 FT.INFO 实测结果归一化后与 meta 期望全量比较。
    /// 抓四类漂移(任一不符即 fail-fast,**禁自动 DROP**,引导 RecreateExplicitly):
    ///   ① ON HASH/JSON(key_type 不符 → save 写进去也建不进倒排,查询恒空);
    ///   ② PREFIX(literal head 不在实测 prefixes → 文档不被索引拾取);
    ///   ③ 字段集合 + 类型(原有);
    ///   ④ 字段修饰:SORTABLE(影响 SORTBY 可用性)、WEIGHT(打分)、SEPARATOR(TAG 分词)、
    ///      CASESENSITIVE/NOSTEM/NOINDEX —— 与建索引时不一致会让排序/匹配静默失真。
    /// 各版本 FT.INFO 可能缺 key_type/prefixes/WEIGHT 等键:**缺则该项放行**(只比能比的,
    /// 不因版本差异误报),但只要实测给出了值且与期望不符就一定上抛。
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

        // ② PREFIX:实测 prefixes 必须含我们声明的 literal head
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
                super::FieldType::Tag { .. } => "TAG",
                super::FieldType::Text { .. } => "TEXT",
                super::FieldType::Numeric { .. } => "NUMERIC",
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
                super::FieldType::Tag { sortable, .. }
                | super::FieldType::Text { sortable, .. }
                | super::FieldType::Numeric { sortable, .. } => sortable,
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
            if let super::FieldType::Numeric { no_index, .. } = &f.ftype {
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

    // ─────────────────────── 写入 / id 直达(对照 原实现 save/findById)───────────────────────

    /// 业务作用：实体路径算 key:无占位符 fast path = prefix + id;占位符模式从
    /// placeholder_parts 取动态段(对照 原实现 keyOf(entity))。
    ///
    /// # 参数
    /// - `doc`: 当前处理的配置文档。
    fn key_for(&self, doc: &T) -> Result<String> {
        if self.meta.has_placeholder() {
            let parts = doc.placeholder_parts();
            let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            self.meta.key_of_parts(&refs, &doc.id())
        } else {
            self.meta.key_of(&doc.id())
        }
    }

    /// 业务作用：保存单个实体(HASH:HSET 全字段;JSON:JSON.SET $)。
    ///
    /// # 参数
    /// - `doc`: 要落盘并参与索引的业务实体,其 `id()`/占位符字段共同决定 Redis key。
    pub async fn save(&self, doc: &T) -> Result<()>
    where
        T: Serialize,
    {
        let key = self.key_for(doc)?;
        self.save_at(&key, doc).await
    }

    /// 业务作用：构造单文档落盘命令(save / save_all 共用)。HASH 走 `HASH_SAVE_LUA`(原子 HDEL 缺失字段
    /// + HSET 新字段);JSON 走 `JSON.SET $`。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `doc`: 当前处理的配置文档。
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

    /// 业务作用：按已算好的 key 落盘(save / save_all 共用)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `doc`: 当前处理的配置文档。
    async fn save_at(&self, key: &str, doc: &T) -> Result<()>
    where
        T: Serialize,
    {
        let cmd = self.build_save_cmd(key, doc)?;
        let _: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        Ok(())
    }

    /// 业务作用：批量保存:分片进**真正的 `PipelineSession`**(单次 flush + 受
    /// `session_max_commands`/`session_max_bytes` 约束),不再 `join_all` 一次性创建全部 future
    /// (大批量内存/队列压力);逐 ticket surface 服务端错误 / ExecutionUnknown(写出后断线)。
    ///
    /// # 参数
    /// - `docs`: 待落盘的业务实体列表,每个实体独立计算 key 并排入 pipeline。
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

    /// 业务作用：id 直达读取(不经 FT.SEARCH;对照 原实现 findById)。
    /// 占位符模式 key 不能只由 id 派生 → key_of 内部报错引导走 find_by_parts。
    ///
    /// # 参数
    /// - `id`: 文档业务 ID,仅适用于无占位符 prefix 的文档类型。
    pub async fn find_by_id(&self, id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let key = self.meta.key_of(id)?;
        self.find_at(&key).await
    }

    /// 业务作用：parts 直达读取(占位符模式;对照 原实现 findByParts:placeholder 值按
    /// prefix 占位符顺序 + id,不经 FT.SEARCH 按 key 拼接直读)。
    ///
    /// # 参数
    /// - `parts`: prefix 占位符对应的业务分片值,顺序必须与占位符出现顺序一致。
    /// - `id`: 文档业务 ID,与 `parts` 一起拼成完整 Redis key。
    pub async fn find_by_parts(&self, parts: &[&str], id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let key = self.meta.key_of_parts(parts, id)?;
        self.find_at(&key).await
    }

    // Loads one document from the given Redis key.
    ///
    /// # 参数
    /// 业务作用：- `key`: 当前 Redis 命令操作的 key。
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

    /// 业务作用：id 直达删除(占位符模式报错引导 remove_by_parts)。
    ///
    /// # 参数
    /// - `id`: 文档业务 ID,仅适用于无占位符 prefix 的文档类型。
    pub async fn remove_by_id(&self, id: &str) -> Result<bool> {
        Ok(self.client.del(&[&self.meta.key_of(id)?]).await? > 0)
    }

    /// 业务作用：parts 直达删除(占位符模式)。
    ///
    /// # 参数
    /// - `parts`: prefix 占位符对应的业务分片值,顺序必须与占位符出现顺序一致。
    /// - `id`: 文档业务 ID,与 `parts` 一起定位要删除的 Redis key。
    pub async fn remove_by_parts(&self, parts: &[&str], id: &str) -> Result<bool> {
        Ok(self
            .client
            .del(&[&self.meta.key_of_parts(parts, id)?])
            .await?
            > 0)
    }

    // ───────────── 原实现 RediSearchOperations 对齐补全─────────────

    /// 业务作用：删除索引(对照 原实现 `dropIndex(deleteDocuments)`)。`delete_documents=true` → `FT.DROPINDEX DD`
    /// (连同删除被索引的文档),否则只删索引定义、保留文档。
    ///
    /// # 参数
    /// - `delete_documents`: 是否同时删除被索引文档;`true` 使用 `DD`,`false` 只删除索引定义。
    pub async fn drop_index(&self, delete_documents: bool) -> Result<()> {
        let mut cmd = redis::cmd("FT.DROPINDEX");
        cmd.arg(&self.meta.index);
        if delete_documents {
            cmd.arg("DD");
        }
        //**幂等**——索引本不存在视为清理成功(对偶 ensure_index,对齐 原实现 dropIndex 把
        // `Unknown Index name`/`no such index` 当成功)。其余错误(权限/模块/代理)仍上抛,不误判。
        match cmd.query_async::<()>(&mut self.client.conn()).await {
            Ok(()) => Ok(()),
            Err(e) if is_unknown_index_error(&e) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// 业务作用：id 是否存在(对照 原实现 `existsById`)——直接 `EXISTS key`,不反序列化整文档。占位符模式报错引导 parts 版。
    ///
    /// # 参数
    /// - `id`: 文档业务 ID,仅适用于无占位符 prefix 的文档类型。
    pub async fn exists_by_id(&self, id: &str) -> Result<bool> {
        let key = self.meta.key_of(id)?;
        self.client.exists(&key).await
    }

    /// 业务作用：parts 是否存在(占位符模式;对照 原实现 `existsById` 的 parts 重载)。
    ///
    /// # 参数
    /// - `parts`: prefix 占位符对应的业务分片值,顺序必须与占位符出现顺序一致。
    /// - `id`: 文档业务 ID,与 `parts` 一起定位 Redis key。
    pub async fn exists_by_parts(&self, parts: &[&str], id: &str) -> Result<bool> {
        let key = self.meta.key_of_parts(parts, id)?;
        self.client.exists(&key).await
    }

    /// 业务作用：JSON.NUMINCRBY(**整数** delta;direct executor,对照 原实现 `jsonNumIncrBy(type,id,path,long)`)——
    /// 撮合部分成交/持仓加减热路径,不必为单次自增手工开 pipeline。返回 RedisJSON 回的结果串(如 `[6]`)。
    /// ⚠ 仅 JSON 模式；整数 delta 保持 JSON 整数 shape，与 pipeline `json_num_incr_by` 一致。
    ///
    /// # 参数
    /// - `id`: JSON 文档业务 ID,仅适用于无占位符 prefix 的 JSON 文档。
    /// - `json_path`: 要自增的 RedisJSON 路径,例如 `$.filledQty`。
    /// - `delta`: 自增整数步长,保持 RedisJSON 整数形态。
    pub async fn json_num_incr_by(&self, id: &str, json_path: &str, delta: i64) -> Result<String> {
        self.json_num_incr_by_at(&self.meta.key_of(id)?, json_path, delta)
            .await
    }

    /// 业务作用：JSON.NUMINCRBY 整数 delta(占位符模式;对照 原实现 parts 重载)。
    ///
    /// # 参数
    /// - `parts`: prefix 占位符对应的业务分片值,顺序必须与占位符出现顺序一致。
    /// - `id`: JSON 文档业务 ID,与 `parts` 一起定位 Redis key。
    /// - `json_path`: 要自增的 RedisJSON 路径,例如 `$.filledQty`。
    /// - `delta`: 自增整数步长,保持 RedisJSON 整数形态。
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

    // Increments a numeric JSON value at the given path.
    ///
    /// # 参数
    /// 业务作用：- `key`: 当前 Redis 命令操作的 key。
    /// - `json_path`: RedisJSON 文档内的 JSONPath。
    /// - `delta`: 计数、score 或 hash field 的增量。
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

    /// 业务作用：查首个匹配(对照 原实现 `findOne`)——内部 `LIMIT 0 1`,不受调用方 `q` 的分页影响。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,只使用其过滤和排序语义,分页会被覆盖为首条。
    pub async fn find_one(&self, q: &Query) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let one = q.clone().limit(0, 1);
        Ok(self.find(&one).await?.into_iter().next())
    }

    /// 业务作用：是否有匹配(对照 原实现 `exists(query)`)——`LIMIT 0 0` 只取 total 判 >0。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,用于渲染 FT.SEARCH 过滤表达式。
    pub async fn exists(&self, q: &Query) -> Result<bool> {
        Ok(self.count(q).await? > 0)
    }

    /// 业务作用：查匹配文档的 **id**(NOCONTENT;对照 原实现 `findKeys`)。占位符 prefix 模式拒绝(key→id 无法反推)。
    /// **未显式 limit 时用 `LIMIT 0 1000000`**(避免默认 10 静默少取,对齐 原实现)。key 经 `literal_prefix` 反推 id。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,可包含排序和分页;未显式分页时会使用大上限对齐批量取 key 语义。
    pub async fn find_keys(&self, q: &Query) -> Result<Vec<String>> {
        let (offset, limit) = if q.is_limit_explicit() {
            (q.effective_offset(), q.effective_limit())
        } else {
            (0, 1_000_000)
        };
        self.search_keys(q, offset, limit).await
    }

    /// 业务作用：`find_keys` 的核心(offset/limit 由调用方定;`remove` 用大上限循环拉)。
    ///
    /// # 参数
    /// - `q`: 查询对象或 query 参数集合。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `limit`: 本次查询、删除或扫描允许处理的最大条数。
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
        //**复用 `render_args`**(而非手写 LIMIT)——保留 `SORTBY` + 排序字段存在性/SORTABLE
        // 校验(NOCONTENT 只跳 RETURN,不是跳 SORTBY 的理由,对齐 原实现 findKeys/remove 用 renderArgs)。
        // 此前手写 `LIMIT offset limit` 丢了 SORTBY → 排序+分页时返回错误 top-N,且非法 sort 字段不报错。
        // offset/limit 由调用方覆盖(find_keys 显式分页或默认大上限;remove 大上限)。
        let q_for_keys = q.clone().limit(offset, limit);
        let mut cmd = redis::cmd("FT.SEARCH");
        cmd.arg(&self.meta.index).arg(&expr).arg("NOCONTENT");
        for a in q_for_keys.render_args(self.meta)? {
            cmd.arg(a);
        }
        cmd.arg("DIALECT").arg(2); // 对齐 原实现:NOCONTENT + SORTBY + LIMIT + DIALECT
        let v: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        let prefix = self.meta.literal_prefix();
        Ok(parse_search_keys(v)
            .into_iter()
            .map(|k| k.strip_prefix(prefix).unwrap_or(&k).to_string())
            .collect())
    }

    /// 业务作用：按查询批量删除(对照 原实现 `remove(query)`):循环 `LIMIT 0 1000000` 拉 key、每 1000 个 `DEL` 一批;
    /// **本轮有命中但实际删 0 → 提前退出**(防索引滞后/并发写无限循环)。返回删除总数。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,用于选择要删除的文档;占位符 prefix 模式因 key 无法反推 ID 会拒绝。
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
                break; // 命中但删 0:索引滞后/并发,防死循环(对照 原实现)
            }
        }
        Ok(total)
    }

    // ─────────────────────── FT.SEARCH 查询族 ───────────────────────

    /// 业务作用：查询(AST 渲染 → FT.SEARCH → 逐文档解码)。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,包含过滤、排序和分页设置。
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
        cmd.arg("DIALECT").arg(2); //恒发 DIALECT 2(对齐 原实现,高级查询语义一致)
        let v: redis::Value = cmd.query_async(&mut self.client.conn()).await?;
        self.parse_search(v)
    }

    /// 业务作用：计数(LIMIT 0 0:只回 total)。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,只使用过滤表达式计算匹配总数。
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

    /// 业务作用：FT.AGGREGATE(最小封装):Query 出过滤表达式 + Aggregate 出聚合管道,
    /// 返回结果行(列名→值;列 = GROUPBY 字段 + reducer 输出别名)。
    /// RESP2 响应形态:[total, [k1,v1,k2,v2,...], [..], ...]。
    ///
    /// # 参数
    /// - `q`: 类型化查询条件,负责生成聚合前的过滤表达式。
    /// - `agg`: 聚合管道定义,包含 LOAD/GROUPBY/REDUCE/APPLY/FILTER/SORTBY/LIMIT。
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

    /// 业务作用：把单文档 field→value 表转为 T，供 RESP2/RESP3 共用。
    ///
    /// # 参数
    /// - `map`: 当前函数读取或更新的键值映射。
    fn map_to_item(&self, map: HashMap<String, String>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        match self.meta.data_type {
            DataType::Hash => T::from_fields(&map),
            // bind 已拒 ARRAY 系,这里只剩 Json:文档在 "$" 字段(整文档 JSON 串)。
            //**缺 `$` 回退取首个字段值**(对齐 原实现 parseJsonRow)——某些
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

    /// 业务作用：解析 FT.SEARCH 响应。RESP2:`[total, key1, [f1,v1,...], key2, [..], ...]`;
    /// RESP3(协商 HELLO 3):`{total_results, results:[{id,extra_attributes:{...}},...]}`
    /// 两种响应形态都会归一化，避免 RESP3 与 count 路径产生语义分叉。
    ///
    /// # 参数
    /// - `v`: 待转换的值。
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

/// 业务作用：Redis 错误是否为"未知索引"(对齐 原实现 `Unknown Index name`/`no such index`)。`index_exists`(判不存在)
/// 与 `drop_index`(幂等)共用——只认明确的未知索引,权限/模块/代理错误不误判。
///
/// # 参数
/// - `e`: 错误对象或外部错误值。
fn is_unknown_index_error(e: &redis::RedisError) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("unknown index name") || msg.contains("no such index")
}

// Converts a Redis bulk value into a string.
///
/// # 参数
/// 业务作用：- `v`: 待转换的值。
fn bulk_to_string(v: redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        redis::Value::SimpleString(s) => s,
        ref other => value_to_string(other),
    }
}

/// 业务作用：从 `FT.SEARCH ... NOCONTENT` 响应取 key 列表(`find_keys`/`remove` 用)。
/// **RESP2**:`[total, k1, k2, ...]`(total 之后全是 key,无 fields 数组)。
/// **RESP3 Map**:`results` 数组,每行是 `{id, ...}` Map,取 `id`(复用 `resp3_map_get`,与 parse_search 同源)。
/// RESP2 NOCONTENT 形态已用 Redis Stack/RediSearch live 验证(`docker exec redis-websocket redis-cli`,5020);
/// RESP3 Map 形态已用 live 实跑验证(`redis-cli -3`,行内 `id` 字段)。
///
/// # 参数
/// - `v`: 待转换的值。
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

/// 业务作用：把计数类 Value 归一化为 u64(RediSearch 8 RESP3 的 total_results 可能是
/// Int/Double/字符串)。负数/非数字 → None。
///
/// # 参数
/// - `v`: 待转换的值。
fn value_to_u64(v: &redis::Value) -> Option<u64> {
    match v {
        redis::Value::Int(n) => (*n >= 0).then_some(*n as u64),
        redis::Value::Double(d) => (*d >= 0.0).then_some(*d as u64),
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).trim().parse::<u64>().ok(),
        redis::Value::SimpleString(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
}

/// 业务作用：按引用取文本(RESP3 Map 遍历用,不消费 Value)。Int/Double 也归一化为裸值文本,
/// 避免落进 `{other:?}` 的 `Int(..)`/`Double(..)` Debug 串。
///
/// # 参数
/// - `v`: 待转换的值。
fn value_to_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(n) => n.to_string(),
        redis::Value::Double(d) => d.to_string(),
        other => format!("{other:?}"),
    }
}

/// 业务作用：在 RESP3 Map 的 (key,val) 列表里按字段名取值(key 兼容 Simple/BulkString)。
///
/// # 参数
/// - `pairs`: 搜索索引的字段和值配对。
/// - `key`: 当前 Redis 命令操作的 key。
fn resp3_map_get<'a>(
    pairs: &'a [(redis::Value, redis::Value)],
    key: &str,
) -> Option<&'a redis::Value> {
    pairs
        .iter()
        .find_map(|(k, v)| (val_str(k).as_deref() == Some(key)).then_some(v))
}

/// 业务作用：从 RESP3 单行结果(Map,含 `extra_attributes` 子 Map)提取 field→value 表。
/// FT.SEARCH/FT.AGGREGATE RESP3 行形态:`{id, extra_attributes:{f1:v1,...}, values:[...]}`。
///**带 `attributes` 回退键**——个别 RediSearch 版本/形态把字段放 `attributes`
/// 而非 `extra_attributes`(原实现 侧亦兼容两者),漏回退会让某些版本 find 返空。
///
/// # 参数
/// - `row`: Redis 搜索返回的单行结果。
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

/// 业务作用：从 Value 取文本(★FT.INFO 的键值是 **SimpleString**(`+`),不是 BulkString——
/// Redis 8 实测;两形态都要兼容,只匹配 BulkString 会解析出空表)。
///
/// # 参数
/// - `v`: 待转换的值。
fn val_str(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// 业务作用：从 Value 取浮点(★RESP3 下 WEIGHT 是 `Double`、RESP2 下是 BulkString——两形态都兼容,
/// 否则 RESP3 weight 取不到会比 RESP2 少校验一项,造成两协议下行为不对称)。
///
/// # 参数
/// - `v`: 待转换的值。
fn val_f64(v: &redis::Value) -> Option<f64> {
    match v {
        redis::Value::Double(d) => Some(*d),
        redis::Value::Int(i) => Some(*i as f64),
        _ => val_str(v).and_then(|s| s.parse().ok()),
    }
}

/// 业务作用：把 FT.INFO 的一层归一成 (key, value) 对序列——**RESP2/RESP3 双形态兼容**:
/// RESP2 是平铺 `[k1,v1,k2,v2,...]` 数组(成对取);RESP3(HELLO 3 / `?protocol=resp3`)是
/// `Map([(k,v),...])`。FT.INFO 顶层 / index_definition / 每个 attribute
/// 在 RESP3 下全是 Map,旧代码只认 `Value::Array` → 命中 else 返回空 profile → ensure_index
/// 对任何正常索引误报漂移。此 helper 让三层解析都协议无关。
///
/// # 参数
/// - `v`: 待转换的值。
fn info_pairs(v: &redis::Value) -> Vec<(&redis::Value, &redis::Value)> {
    match v {
        redis::Value::Map(m) => m.iter().map(|(k, v)| (k, v)).collect(),
        redis::Value::Array(a) => a.chunks_exact(2).map(|c| (&c[0], &c[1])).collect(),
        _ => Vec::new(),
    }
}

/// 业务作用：FT.INFO 归一化:提取 attributes → alias→类型 表(RESP2 平铺数组 / RESP3 Map 双形态;
/// 各版本字段名 identifier/attribute 兼容;SimpleString/BulkString 双形态)。
///
/// # 参数
/// - `info`: Redis `FT.INFO` 返回值,可以是 RESP2 平铺数组或 RESP3 Map。
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

/// 一个字段的 FT.INFO 实测画像，用于比较 type、修饰 flag、WEIGHT 与 SEPARATOR 漂移。
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

/// FT.INFO 归一化后的整索引画像，覆盖建索引所需的完整形态。
#[derive(Debug, Default)]
pub struct IndexProfile {
    /// index_definition.key_type:"HASH" / "JSON"(各版本可能缺,缺=空串→比对放行)。
    pub key_type: String,
    /// index_definition.prefixes 列表(各版本可能缺)。
    pub prefixes: Vec<String>,
    /// alias → 字段画像。
    pub attrs: HashMap<String, AttrInfo>,
}

/// 业务作用：应用一个独立 flag token(SORTABLE/NOSTEM/NOINDEX/CASESENSITIVE...)到 AttrInfo。
/// RESP2 下 flag 平铺在 attribute 数组里;RESP3 下 flag 收进 `flags` 子数组——两形态共用此函数。
/// UNF/NOFREQS/INDEXMISSING 等不参与漂移比对者落到 `_` 跳过。
///
/// # 参数
/// - `info`: Redis 搜索索引的元信息。
/// - `flag`: Redis 搜索选项的布尔标记。
fn apply_flag(info: &mut AttrInfo, flag: &str) {
    match flag.to_ascii_uppercase().as_str() {
        "SORTABLE" => info.sortable = true,
        "NOSTEM" => info.no_stem = true,
        "NOINDEX" => info.no_index = true,
        "CASESENSITIVE" => info.case_sensitive = true,
        _ => {}
    }
}

/// 业务作用：解析单个 attribute → AttrInfo。**RESP2 平铺数组 / RESP3 Map 双形态**
/// - RESP2:`[identifier, price, attribute, price, type, NUMERIC, SORTABLE, UNF]`——成对键
///   (identifier/attribute/type/WEIGHT/SEPARATOR/PHONETIC)与独立 flag 平铺混排;
/// - RESP3:`Map([(identifier,price),(type,NUMERIC),(flags,[SORTABLE,UNF])])`——flag 收进
///   `flags` 子数组,WEIGHT 是 Double。两形态都归一到同一 AttrInfo,供漂移比对协议无关。
///
/// # 参数
/// - `attr`: 属性宏括号内的 token stream。
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

/// 业务作用：FT.INFO → IndexProfile(RESP2 平铺数组 / RESP3 Map 双形态)。
///
/// # 参数
/// - `info`: Redis `FT.INFO` 返回值,用于抽取 key type、prefixes 和字段画像。
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
