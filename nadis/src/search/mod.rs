// ============================================================================
// src/search/mod.rs —— RediSearch 封装:元数据 trait / 文档编解码(R5.1+R5.2)。
// (架构文档;对照 原实现 RediSearch + annotation/meta/convert 配套)
//
// ┌─ R5.1 已实现 ─────────────────────────────────────────────────────────┐
// │ · RedisDocument trait(静态元数据 + HASH 编解码;对照 原实现 @RsDocument/    │
// │   @RsId/@TagField/@TextField/@NumericField + MetaResolver/EntityMeta)     │
// │ · DocMeta 构建期校验(字段重名/空 alias/ARRAY 约束——错误在 bind 时暴露,  │
// │   不等到发命令)                                                           │
// │ · 查询 = 类型化 AST + renderer(query.rs;字段/类型构建期校验,tag 值转义) │
// │ · IndexPolicy{ValidateOnly|CreateIfMissing|RecreateExplicitly},FT.INFO    │
// │   实测归一化比较,**禁自动 DROP**(actuator.rs)                            │
// │ · capability 探测(缺 FT/JSON 模块给明确错误,非协议错)                   │
// │ · DataType::Hash / Json 两形态(id 直达 + FT.SEARCH 查询)                 │
// ├─ R5.2 已实现 ─────────────────────────────────────────────────────────┤
// │ · prefix {field} 占位符(对照 原实现 KeyParts/KeySegment:prefix 编译成      │
// │   literal/field 交替段;key_of_parts 按占位符顺序拼 key;动态段禁 :{}      │
// │   防撞 key;literal_prefix 给 FT.CREATE PREFIX)                           │
// │ · DataType::JsonArray / JsonArrayBucket + 5 个 JSON 数组 Lua(array.rs;   │
// │   对照 原实现 JsonArraySupport/JsonArrayLuaScripts;bucket 选桶 =            │
// │   floorMod(原实现_string_hash(渲染后 subId), bucketCount) **持久化协议**)   │
// │ · #[derive(RedisDocument)] 过程宏(nadis-derive crate,纯语法糖——   │
// │   生成的代码只调用本模块的公开 API)                                       │
// │ · FT.AGGREGATE 最小封装(actuator.rs:GROUPBY/REDUCE/SORTBY/LIMIT)        │
// ├─ 留待 ────────────────────────────────────────────────────────────────┤
// │ · GeoField / OR 组合(FT.AGGREGATE 表达式级)/ RESP3 Map 全形态归一化     │
// └──────────────────────────────────────────────────────────────────────┘
// ============================================================================

pub mod actuator;
pub mod array;
pub mod query;

use std::collections::HashMap;

use crate::error::{NasaRedisError, Result};
use crate::partition::compat_string_hash;

/// S8:序列化为 JSON 字符串并**省略 null 字段**,对齐 原实现 `@JsonInclude(NON_NULL)`。
/// Rust serde 默认写 `"f":null`,原实现 默认省略 → 同一子文档跨语言字节分叉,
/// JSON_ARRAY 共享 key 时尤其致命(原实现 写的数组 Rust 读回会多/少字段)。
/// 这里序列化到 `serde_json::Value` 后**递归剔除对象里值为 null 的成员**(数组元素、
/// 嵌套对象一并处理),与字段上是否标 `skip_serializing_if` 无关,保证确定性对齐。
///
/// # 参数
/// - `doc`: 当前处理的配置文档。
pub fn to_json_omit_null<T: serde::Serialize>(doc: &T) -> Result<String> {
    let mut v = serde_json::to_value(doc).map_err(|e| NasaRedisError::Codec(e.to_string()))?;
    strip_nulls(&mut v);
    serde_json::to_string(&v).map_err(|e| NasaRedisError::Codec(e.to_string()))
}

/// 递归剔除 JSON 对象中值为 null 的成员(对象/数组深度遍历)。
///
/// # 参数
/// - `v`: 待转换的值。
fn strip_nulls(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            map.retain(|_, val| !val.is_null());
            for val in map.values_mut() {
                strip_nulls(val);
            }
        }
        serde_json::Value::Array(arr) => {
            for val in arr.iter_mut() {
                strip_nulls(val);
            }
        }
        _ => {}
    }
}

/// 存储形态(对照 原实现 DataType 四态全集)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// 扁平 HASH:field=列名;FT.CREATE ON HASH。
    Hash,
    /// RedisJSON 单文档:FT.CREATE ON JSON,字段经 `$.path AS alias` 映射。
    Json,
    /// JSON 数组:1 个 ARRAY key 装多个子文档(key = prefix + @JsonArrayKey 值拼接)。
    /// 不建 FT 索引,查询走 JSONPath filter(JsonArrayOps,见 array.rs)。
    JsonArray,
    /// JSON 数组分桶:同一组 array key 散到 N 个桶 key,
    /// 桶 key 共享 `{body}` hash tag 锁同 slot,跨桶操作用 Lua 一次 RTT 扫全桶。
    JsonArrayBucket,
}

impl DataType {
    /// 是否 ARRAY 系(JsonArrayOps 专属;SearchActuator 拒绝)。
    pub fn is_array(&self) -> bool {
        matches!(self, DataType::JsonArray | DataType::JsonArrayBucket)
    }
}

/// 字段索引类型(对照 原实现 @TagField/@TextField/@NumericField 的属性子集)。
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// 精确匹配(离散值:ID/状态/枚举);FT 语法 `@f:{v}`。
    /// S3:`separator`(多值分隔符,默认 `,`,None=不输出用 RediSearch 默认)、`case_sensitive`
    /// (CASESENSITIVE,默认大小写不敏感)——必须与 原实现 建索引时一致,否则分词/匹配不一致。
    Tag {
        sortable: bool,
        separator: Option<char>,
        case_sensitive: bool,
        /// S8:WITHSUFFIXTRIE——建后缀树,启用 `*foo`(后缀)/`*foo*`(包含)查询。
        with_suffix_trie: bool,
    },
    /// 全文搜索;FT 语法 `@f:(词)`。
    /// S4:`no_stem`(NOSTEM 关词干还原)、`phonetic`(PHONETIC 匹配器如 `dm:en`)、`no_index`
    /// (NOINDEX 只存不建倒排,仅 SORTABLE/投影用)。
    Text {
        sortable: bool,
        weight: f64,
        no_stem: bool,
        phonetic: Option<String>,
        no_index: bool,
        /// S8:WITHSUFFIXTRIE——建后缀树,启用 `*foo`(后缀)/`*foo*`(包含)查询。
        with_suffix_trie: bool,
    },
    /// 数值范围;FT 语法 `@f:[min max]`。
    ///`no_index`(NOINDEX 只存不建数值索引,仅 SORTABLE/投影用——对齐 原实现
    /// `@NumericField(indexed=false)`,补齐 Tag/Text 已有的 NOINDEX 对称性)。
    Numeric { sortable: bool, no_index: bool },
    /// 地理坐标(S5);存储 `"lon,lat"`,FT 语法 `@f:[lon lat radius unit]`(半径圆内)。
    Geo,
}

/// 一个被索引字段的元数据。
#[derive(Debug, Clone)]
pub struct FieldMeta {
    /// 业务字段名(HASH 的 field 名;JSON 模式下若未给 json_path 则默认 `$.{name}`)。
    pub name: String,
    /// RediSearch 别名(查询里 `@alias:`;空 = 用 name)。
    pub alias: String,
    /// JSON 模式的显式 JSONPath(None = `$.{name}`;HASH 模式忽略)。
    pub json_path: Option<String>,
    pub ftype: FieldType,
}

impl FieldMeta {
    /// Returns the field alias when present, otherwise the field name.
    pub fn alias_or_name(&self) -> &str {
        if self.alias.is_empty() {
            &self.name
        } else {
            &self.alias
        }
    }
}

/// prefix 编译段(对照 原实现 KeySegment):要么字面文本,要么占位符字段引用。
/// 原实现 启动期编译缓存进 EntityMeta;Rust 侧 DocMeta 是手写/derive 的 plain struct,
/// 在 key 构建时按需解析(IO 路径上解析成本可忽略;若热点可后续加 OnceLock 缓存)。
#[derive(Debug, Clone, PartialEq)]
pub enum KeySeg {
    /// 字面段:原样拼接。
    Lit(String),
    /// 占位符段:`{name}` —— name 仅作文档与 derive 编译期匹配用,
    /// 运行时值由调用方按占位符出现顺序提供(key_of_parts / placeholder_parts)。
    Field(String),
}

/// 文档元数据(对照 原实现 EntityMeta;trait 静态返回,bind 期校验)。
#[derive(Debug, Clone)]
pub struct DocMeta {
    /// RediSearch 索引名(如 "idx:order")。ARRAY 系不建索引,仍要求非空(诊断用)。
    pub index: String,
    /// Redis key 前缀。三种形态:
    ///   · 纯字面量:完整 key = prefix + id;
    ///   · 含 `{field}` 占位符(R5.2):如 `"order:SMO:{sym}:{d}:"`,动态段运行时填充;
    ///   · ARRAY 系:完整 key = prefix + @JsonArrayKey 值拼接(占位符在 ARRAY 系禁用)。
    pub prefix: String,
    pub data_type: DataType,
    pub fields: Vec<FieldMeta>,
    /// @RsId 字段的存储名(JSONPath idFilter `@.{id_name}==…` 与 HASH 回填用)。
    pub id_name: String,
    /// id 在 JSON 文档中是否渲染为数字(serde 数值类型)。
    /// 决定 idFilter 字面量形态:数字不加引号、字符串加引号转义——
    /// 形态错了 JSONPath `==` 永远不命中(原实现 靠 Object 运行时类型,Rust 靠此标记)。
    pub id_numeric: bool,
    /// ARRAY 系:@JsonArrayKey 字段名列表,**按 order 升序**(决定 key 拼接顺序,
    /// 是持久化协议——改顺序等于改 key,老数据查不到)。非 ARRAY 系为空。
    pub array_keys: Vec<String>,
    /// 仅 JsonArrayBucket:桶数(>1;**不可热修改**——改桶数=改散列,等同 schema
    /// 变更须停服迁移,对照 原实现 @RsDocument.bucketCount 原实现doc)。其他模式 0。
    pub bucket_count: u32,
}

impl DocMeta {
    /// 构建期校验(字段在 metadata 期校验,不等到发命令才失败)。
    pub fn validate(&self) -> Result<()> {
        if self.index.is_empty() || self.prefix.is_empty() {
            return Err(NasaRedisError::Config(
                "DocMeta: index/prefix 不能为空".into(),
            ));
        }
        if self.id_name.is_empty() {
            return Err(NasaRedisError::Config("DocMeta: id_name 不能为空".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for f in &self.fields {
            if f.name.is_empty() {
                return Err(NasaRedisError::Config("DocMeta: 字段 name 为空".into()));
            }
            if !seen.insert(f.alias_or_name().to_string()) {
                return Err(NasaRedisError::Config(format!(
                    "DocMeta: 别名重复 \"{}\"",
                    f.alias_or_name()
                )));
            }
        }
        // 占位符语法校验(括号配对/非空/不嵌套)——解析即校验
        let segs = self.segments()?;
        // ARRAY 系约束(对照 原实现 MetaResolver/RsDocument 注解约束)
        match self.data_type {
            DataType::JsonArray | DataType::JsonArrayBucket => {
                if self.array_keys.is_empty() {
                    return Err(NasaRedisError::Config(
                        "DocMeta: ARRAY 模式必须声明至少一个 array_keys(@JsonArrayKey)".into(),
                    ));
                }
                if segs.is_some() {
                    // ARRAY key = prefix + parts,动态性由 @JsonArrayKey 承担;
                    // 占位符与之叠加会让 key 形态二义(原实现 同样不支持)
                    return Err(NasaRedisError::Config(
                        "DocMeta: ARRAY 模式 prefix 不支持 {field} 占位符".into(),
                    ));
                }
                if self.data_type == DataType::JsonArrayBucket && self.bucket_count <= 1 {
                    return Err(NasaRedisError::Config(format!(
                        "DocMeta: JsonArrayBucket 要求 bucket_count > 1,得到 {}",
                        self.bucket_count
                    )));
                }
                if self.data_type == DataType::JsonArray && self.bucket_count != 0 {
                    return Err(NasaRedisError::Config(
                        "DocMeta: JsonArray(非 BUCKET)bucket_count 必须为 0".into(),
                    ));
                }
            }
            _ => {
                if !self.array_keys.is_empty() || self.bucket_count != 0 {
                    return Err(NasaRedisError::Config(
                        "DocMeta: array_keys/bucket_count 仅 ARRAY 模式可用".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// 按别名查字段(查询 renderer 用)。
    ///
    /// # 参数
    /// - `alias`: 调用方在查询、排序或聚合中使用的字段别名;空别名字段会回退匹配字段名。
    pub fn field(&self, alias: &str) -> Option<&FieldMeta> {
        self.fields.iter().find(|f| f.alias_or_name() == alias)
    }

    // ───────────────── prefix {field} 占位符(R5.2,对照 原实现 KeySegment)─────────────────

    /// 解析 prefix 为编译段。返回 None = 无占位符(fast path:prefix + id)。
    /// 语法:`{name}`,不嵌套;`{`/`}` 必须配对;name 非空。
    pub fn segments(&self) -> Result<Option<Vec<KeySeg>>> {
        if !self.prefix.contains('{') {
            // 孤立的 '}'(无 '{')也算语法错——它会破坏 Cluster hash tag 解析
            if self.prefix.contains('}') {
                return Err(NasaRedisError::Config(format!(
                    "DocMeta: prefix \"{}\" 含未配对的 '}}'",
                    self.prefix
                )));
            }
            return Ok(None);
        }
        let mut segs = Vec::new();
        let mut lit = String::new();
        let mut rest = self.prefix.as_str();
        while let Some(open) = rest.find('{') {
            //`{` 之前的 literal 片段含孤立 '}' = 未配对(会破坏 Cluster hash tag 解析);
            // 此前只检查"无 { 的整串"和"末尾 rest",漏了首个 `{` 之前的 '}'。
            if rest[..open].contains('}') {
                return Err(NasaRedisError::Config(format!(
                    "DocMeta: prefix \"{}\" 含未配对的 '}}'",
                    self.prefix
                )));
            }
            lit.push_str(&rest[..open]);
            let after = &rest[open + 1..];
            let close = after.find('}').ok_or_else(|| {
                NasaRedisError::Config(format!("DocMeta: prefix \"{}\" 占位符未闭合", self.prefix))
            })?;
            let name = &after[..close];
            if name.is_empty() || name.contains('{') {
                return Err(NasaRedisError::Config(format!(
                    "DocMeta: prefix \"{}\" 占位符名非法(空或嵌套)",
                    self.prefix
                )));
            }
            if !lit.is_empty() {
                segs.push(KeySeg::Lit(std::mem::take(&mut lit)));
            }
            segs.push(KeySeg::Field(name.to_string()));
            rest = &after[close + 1..];
        }
        if rest.contains('}') {
            return Err(NasaRedisError::Config(format!(
                "DocMeta: prefix \"{}\" 含未配对的 '}}'",
                self.prefix
            )));
        }
        if !rest.is_empty() {
            segs.push(KeySeg::Lit(rest.to_string()));
        }
        Ok(Some(segs))
    }

    /// 是否含占位符(业务侧据此决定走单 id API 还是 parts API)。
    pub fn has_placeholder(&self) -> bool {
        self.prefix.contains('{')
    }

    /// 占位符段数(key_of_parts 入参校验用)。
    pub fn placeholder_count(&self) -> usize {
        self.prefix.matches('{').count()
    }

    /// FT.CREATE PREFIX 参数用的 literal head:无占位符 = 整个 prefix;
    /// 占位符模式 = 第一个 `{` 之前的字面头(FT PREFIX 按 startsWith 匹配,
    /// literal head 覆盖该 entity 全部 key 变体——对照 原实现 literalPrefix)。
    pub fn literal_prefix(&self) -> &str {
        match self.prefix.find('{') {
            Some(i) => &self.prefix[..i],
            None => &self.prefix,
        }
    }

    /// 完整 Redis key(仅无占位符模式;占位符模式报错引导走 key_of_parts——
    /// 对照 原实现 EntityMeta.key(String) 同款 fail-fast)。
    ///
    /// # 参数
    /// - `id`: 文档业务 ID,会直接拼到无占位符 prefix 之后形成 Redis key。
    pub fn key_of(&self, id: &str) -> Result<String> {
        if self.has_placeholder() {
            return Err(NasaRedisError::Config(format!(
                "prefix \"{}\" 含占位符,必须用 key_of_parts(parts, id)",
                self.prefix
            )));
        }
        Ok(format!("{}{}", self.prefix, id))
    }

    /// 占位符模式拼 key:parts 按占位符出现顺序,id 拼末尾。
    /// 动态段(占位符值 + id)一律 check_part 禁 `:`/`{`/`}` ——
    /// 不同输入拼出相同 key = 数据互相覆盖,必须写入前拒绝(对照 原实现 KeyParts)。
    /// 无占位符模式 parts 必须为空(退化为 prefix + id,id 不校验,与 原实现 fast path 一致)。
    ///
    /// # 参数
    /// - `parts`: prefix 中 `{field}` 占位符对应的业务分片值,顺序必须与占位符出现顺序一致。
    /// - `id`: 文档业务 ID,作为 key 的最后一个动态段并参与防碰撞校验。
    pub fn key_of_parts(&self, parts: &[&str], id: &str) -> Result<String> {
        let Some(segs) = self.segments()? else {
            if !parts.is_empty() {
                return Err(NasaRedisError::Config(format!(
                    "prefix \"{}\" 无占位符,parts 必须为空(得到 {} 个)",
                    self.prefix,
                    parts.len()
                )));
            }
            return Ok(format!("{}{}", self.prefix, id));
        };
        let expected = self.placeholder_count();
        if parts.len() != expected {
            return Err(NasaRedisError::Config(format!(
                "prefix \"{}\" 有 {expected} 个占位符,得到 {} 个 parts",
                self.prefix,
                parts.len()
            )));
        }
        let mut out = String::with_capacity(self.prefix.len() + id.len() + 16);
        let mut idx = 0usize;
        for seg in &segs {
            match seg {
                KeySeg::Lit(s) => out.push_str(s),
                KeySeg::Field(name) => {
                    let v = parts[idx];
                    idx += 1;
                    check_part(v, &format!("占位符 {{{name}}}"))?;
                    out.push_str(v);
                }
            }
        }
        // 占位模式 id 同样校验分隔符防撞 key(对照 原实现 keyOf 末尾 checkPart)
        check_part(id, "@RsId")?;
        out.push_str(id);
        Ok(out)
    }

    // ───────────────── ARRAY key 派生(R5.2,对照 原实现 EntityMeta.arrayKey 族)─────────────────

    /// ARRAY key 主体:parts 按 array_keys 顺序以 `:` 拼接(逐段 check_part)。
    ///
    /// # 参数
    /// - `parts`: 路径、占位符或 SQL 片段拆分后的部分。
    fn array_body(&self, parts: &[&str]) -> Result<String> {
        if parts.len() != self.array_keys.len() {
            return Err(NasaRedisError::Config(format!(
                "array key 需要 {} 个 parts(@JsonArrayKey 字段数),得到 {}",
                self.array_keys.len(),
                parts.len()
            )));
        }
        let mut body = String::with_capacity(32);
        for (i, p) in parts.iter().enumerate() {
            check_part(p, &format!("@JsonArrayKey \"{}\"", self.array_keys[i]))?;
            if i > 0 {
                body.push(':');
            }
            body.push_str(p);
        }
        Ok(body)
    }

    /// ARRAY 模式完整 key = prefix + body;BUCKET 模式 = 不含桶号的"前缀"
    /// (调用方再经 bucket_key/all_bucket_keys 追加;对照 原实现 arrayKeyOf)。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的业务 key 段,每段都会做分隔符和 hash tag 字符校验。
    pub fn array_key_of(&self, parts: &[&str]) -> Result<String> {
        if !self.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "array_key_of 仅 ARRAY 模式可用(当前 {:?})",
                self.data_type
            )));
        }
        Ok(format!("{}{}", self.prefix, self.array_body(parts)?))
    }

    /// BUCKET 模式:subId 所在桶号 = floorMod(原实现_string_hash(渲染后 subId), bucketCount)。
    /// **持久化协议**:原实现 在 String.hashCode 上取模(MetaResolver.renderValue 先归一化,
    /// Rust 侧 id()/parts 已是渲染后字符串)——必须逐位一致,否则查不到 原实现 写的桶。
    /// floorMod(而非 abs%)规避 hashCode==i32::MIN 时 abs 溢出(对照 原实现 bucketOf 注释)。
    ///
    /// # 参数
    /// - `sub_id`: 数组子文档 ID 的最终字符串形态,用于稳定散列到 bucket。
    pub fn bucket_index_of(&self, sub_id: &str) -> Result<u32> {
        if self.data_type != DataType::JsonArrayBucket {
            return Err(NasaRedisError::Config(
                "bucket_index_of 仅 JsonArrayBucket 模式可用".into(),
            ));
        }
        let h = compat_string_hash(sub_id) as i64;
        let n = self.bucket_count as i64;
        Ok(((h % n + n) % n) as u32)
    }

    /// BUCKET 模式:给定 parts + subId 定位单桶 key:
    /// `prefix + "{" + body + "}" + ":bucket:" + idx`(`{body}` = Cluster hash tag,
    /// 同组所有桶必落同 slot,跨桶 Lua 不触发 CROSSSLOT)。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的业务 key 段,决定同组数组的 hash tag 主体。
    /// - `sub_id`: 数组子文档 ID,用于选择具体 bucket 号。
    pub fn bucket_key(&self, parts: &[&str], sub_id: &str) -> Result<String> {
        let idx = self.bucket_index_of(sub_id)?;
        Ok(format!(
            "{}{{{}}}:bucket:{}",
            self.prefix,
            self.array_body(parts)?,
            idx
        ))
    }

    /// BUCKET 模式:全部 N 个桶 key(桶号 0..N-1 顺序;跨桶 Lua 的 KEYS 入参)。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的业务 key 段,用于生成同组全部 bucket key。
    pub fn all_bucket_keys(&self, parts: &[&str]) -> Result<Vec<String>> {
        if self.data_type != DataType::JsonArrayBucket {
            return Err(NasaRedisError::Config(
                "all_bucket_keys 仅 JsonArrayBucket 模式可用".into(),
            ));
        }
        let tag = format!("{}{{{}}}:bucket:", self.prefix, self.array_body(parts)?);
        Ok((0..self.bucket_count)
            .map(|i| format!("{tag}{i}"))
            .collect())
    }

    /// `array_key_of` / `bucket_key` 的**逆向解析**(对照 原实现 EntityMeta.parseKey):
    /// 从一个完整 key 还原出 array_key parts(scan 后按业务键归组时用)。
    /// 段数必须等于 array_keys 数,否则视为非本 entity 的 key → Err。
    /// 因 `check_part` 禁止 parts 含 `:`/`{`/`}`,对 body 直接 `split(':')` 无损还原:
    ///   · JsonArray:`prefix + "a:b"` → ["a","b"];
    ///   · JsonArrayBucket:`prefix + "{a:b}:bucket:N"` → 取 `{}` 内主体再 split。
    ///
    /// # 参数
    /// - `key`: 扫描或事件中拿到的完整 Redis key,必须属于当前 `DocMeta` 的 ARRAY key 空间。
    pub fn parse_array_key(&self, key: &str) -> Result<Vec<String>> {
        if !self.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "parse_array_key 仅 ARRAY 模式可用(当前 {:?})",
                self.data_type
            )));
        }
        let rest = key.strip_prefix(self.prefix.as_str()).ok_or_else(|| {
            NasaRedisError::Config(format!(
                "key \"{key}\" 不以 prefix \"{}\" 开头",
                self.prefix
            ))
        })?;
        let body = if matches!(self.data_type, DataType::JsonArrayBucket) {
            // 形如 {body}:bucket:N —— 取首个 `{` 与 `}:bucket:` 之间的主体
            rest.strip_prefix('{')
                .and_then(|s| s.split_once("}:bucket:"))
                .map(|(b, _)| b)
                .ok_or_else(|| {
                    NasaRedisError::Config(format!(
                        "bucket key \"{key}\" 形态不符 {{body}}:bucket:N"
                    ))
                })?
        } else {
            rest
        };
        let parts: Vec<String> = body.split(':').map(|s| s.to_string()).collect();
        if parts.len() != self.array_keys.len() {
            return Err(NasaRedisError::Config(format!(
                "key \"{key}\" 解析出 {} 段,期望 {}(@JsonArrayKey 字段数)",
                parts.len(),
                self.array_keys.len()
            )));
        }
        Ok(parts)
    }

    /// 实体路径的 ARRAY key(save 用):ARRAY = 单 key;BUCKET = 按 subId 定位单桶。
    ///
    /// # 参数
    /// - `parts`: `array_keys` 顺序对应的业务 key 段。
    /// - `sub_id`: 数组子文档 ID,BUCKET 模式下用于确定写入桶。
    pub fn array_key_for(&self, parts: &[&str], sub_id: &str) -> Result<String> {
        match self.data_type {
            DataType::JsonArray => self.array_key_of(parts),
            DataType::JsonArrayBucket => self.bucket_key(parts, sub_id),
            other => Err(NasaRedisError::Config(format!(
                "array_key_for 仅 ARRAY 模式可用(当前 {other:?})"
            ))),
        }
    }

    /// 生成 FT.CREATE 的 SCHEMA 参数序列(actuator 建索引 + 归一化比较共用同一来源,
    /// 保证"期望 schema"只有一处定义)。
    pub fn schema_args(&self) -> Vec<String> {
        let mut out = Vec::new();
        for f in &self.fields {
            match self.data_type {
                DataType::Json => {
                    // JSON 模式:identifier = JSONPath,AS alias
                    out.push(
                        f.json_path
                            .clone()
                            .unwrap_or_else(|| format!("$.{}", f.name)),
                    );
                    out.push("AS".into());
                    out.push(f.alias_or_name().to_string());
                }
                // HASH 形态(ARRAY 系不建索引,不会走到这;归 HASH 分支保持穷尽)
                _ => out.push(f.name.clone()),
            }
            // HASH 模式且 alias ≠ name:同样需要 AS
            if matches!(self.data_type, DataType::Hash) && !f.alias.is_empty() && f.alias != f.name
            {
                out.push("AS".into());
                out.push(f.alias.clone());
            }
            match &f.ftype {
                FieldType::Tag {
                    sortable,
                    separator,
                    case_sensitive,
                    with_suffix_trie,
                } => {
                    // FT.CREATE TAG 顺序:[SEPARATOR x] [CASESENSITIVE] [WITHSUFFIXTRIE] [SORTABLE]
                    out.push("TAG".into());
                    if let Some(sep) = separator {
                        out.push("SEPARATOR".into());
                        out.push(sep.to_string());
                    }
                    if *case_sensitive {
                        out.push("CASESENSITIVE".into());
                    }
                    if *with_suffix_trie {
                        out.push("WITHSUFFIXTRIE".into());
                    }
                    if *sortable {
                        out.push("SORTABLE".into());
                    }
                }
                FieldType::Text {
                    sortable,
                    weight,
                    no_stem,
                    phonetic,
                    no_index,
                    with_suffix_trie,
                } => {
                    // FT.CREATE TEXT 顺序:[NOSTEM] [WEIGHT w] [PHONETIC m] [WITHSUFFIXTRIE] [SORTABLE] [NOINDEX]
                    out.push("TEXT".into());
                    if *no_stem {
                        out.push("NOSTEM".into());
                    }
                    if (*weight - 1.0).abs() > f64::EPSILON {
                        out.push("WEIGHT".into());
                        out.push(weight.to_string());
                    }
                    if let Some(matcher) = phonetic {
                        out.push("PHONETIC".into());
                        out.push(matcher.clone());
                    }
                    if *with_suffix_trie {
                        out.push("WITHSUFFIXTRIE".into());
                    }
                    if *sortable {
                        out.push("SORTABLE".into());
                    }
                    if *no_index {
                        out.push("NOINDEX".into());
                    }
                }
                FieldType::Numeric { sortable, no_index } => {
                    // FT.CREATE NUMERIC 顺序:[SORTABLE] [NOINDEX]
                    out.push("NUMERIC".into());
                    if *sortable {
                        out.push("SORTABLE".into());
                    }
                    if *no_index {
                        out.push("NOINDEX".into());
                    }
                }
                FieldType::Geo => out.push("GEO".into()),
            }
        }
        out
    }
}

/// 动态 key 段校验(对照 原实现 KeyParts.checkPart):禁 `:`(key 分隔符)与
/// `{`/`}`(Cluster hash tag 字符)——含它们的不同输入会拼出相同 key(数据互相
/// 覆盖)或破坏 slot 归属,写入前 fail-fast。
///
/// # 参数
/// - `s`: 待写入 Redis key 动态段的业务值。
/// - `desc`: 出错时展示的字段说明,用于定位是哪个占位符、ID 或 array key 字段非法。
pub fn check_part(s: &str, desc: &str) -> Result<()> {
    if s.contains(':') || s.contains('{') || s.contains('}') {
        return Err(NasaRedisError::Config(format!(
            "{desc} 值 \"{s}\" 不得含 ':'/'{{'/'}}'(':' 是 key 分隔符;'{{}}' 破坏 hash tag)"
        )));
    }
    Ok(())
}

/// 文档 trait(;手写 impl 与 #[derive(RedisDocument)] 共用同一形态——
/// 宏只是生成下面这些方法,语义零差异)。
/// HASH 编解码内聚于 trait(对照 原实现 RsConverter:字段值 ↔ 存储字符串);
/// JSON 模式经 serde(actuator 对 T: Serialize+DeserializeOwned 额外约束)。
pub trait RedisDocument: Sized + Send + Sync {
    /// 静态元数据(进程内单例;`OnceLock` 惯用)。
    fn meta() -> &'static DocMeta;

    /// 文档 ID(完整 key = meta().prefix + id;对照 原实现 @RsId)。
    fn id(&self) -> String;

    /// HASH 编码:全部字段 →(field, value)字符串对(含 id 字段本身)。
    /// 占位符模式注意:占位符字段也必须在内——值只存在 key 里且 key 不可逆推,
    /// 不写 HASH 则 find(query) 读回为 null(对照 原实现 storedFields 修复)。
    fn to_fields(&self) -> Vec<(String, String)>;

    /// HASH 解码:从 field→value 表重建(缺字段按业务默认/报错,impl 决定)。
    ///
    /// # 参数
    /// - `fields`: Redis HASH 读取出的字段名到字符串值映射,由具体实体负责转成业务类型。
    fn from_fields(fields: &HashMap<String, String>) -> Result<Self>;

    /// 占位符模式:按 prefix 占位符出现顺序返回渲染后的字段值
    /// (对照 原实现 KeySegment 从 entity 反查;无占位符保持默认空)。
    fn placeholder_parts(&self) -> Vec<String> {
        Vec::new()
    }

    /// ARRAY 模式:按 meta().array_keys 顺序返回渲染后的 @JsonArrayKey 字段值
    /// (对照 原实现 arrayKeyPartsOf;非 ARRAY 模式保持默认空)。
    fn array_key_parts(&self) -> Vec<String> {
        Vec::new()
    }
}
