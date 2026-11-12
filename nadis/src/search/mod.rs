// RediSearch 文档元数据、键派生与类型化查询。
//
// RedisDocument 提供静态元数据和 HASH 编解码；DocMeta 在绑定时校验配置。
// 查询 AST 在 I/O 之前校验字段类型并转义 Tag。索引策略显式区分校验、缺失时创建和重建，
// 不自动删除已有索引；缺少 FT 或 JSON 模块时返回明确错误。
//
// Hash、Json 使用直接键读取与 FT.SEARCH；JsonArray、JsonArrayBucket 使用 JSONPath 与 Lua。
// prefix 占位符按出现顺序替换，动态段禁止 :{} 以防键碰撞。分桶按 UTF-16 String.hashCode
// 与 floorMod 派生，属于持久化协议。过程宏生成的代码使用本模块公开 API。

/// RediSearch 索引生命周期、文档读写、查询和聚合执行器。
pub mod actuator;
/// RedisJSON 数组与分桶数组的原子 Lua 操作。
pub mod array;
/// 类型化 RediSearch 查询 AST 及安全渲染器。
pub mod query;

use std::collections::HashMap;

use crate::error::{NasaRedisError, Result};
use crate::partition::compat_string_hash;

/// 业务作用：按共享文档省略规则序列化 JSON，移除对象中值为 null 的成员。
///
/// 参数说明：
/// - `doc`：需要写入 RedisJSON 的业务文档。
///
/// 返回：成功返回 JSON 文本，对象成员递归处理、数组的 null 元素保持位置；序列化错误仅保留类别与行列，捕获到自定义序列化展开时返回固定 Codec 错误。只调用一次 Serialize，不重试或撤销其自身副作用；不改变宿主 panic hook，也不隔离进程终止或栈耗尽。
pub fn to_json_omit_null<T: serde::Serialize>(doc: &T) -> Result<String> {
    let v = to_json_value_omit_null(doc)?;
    serde_json::to_string(&v).map_err(|error| {
        NasaRedisError::Codec(format!(
            "JSON 文档编码失败: {:?} at {}:{}",
            error.classify(),
            error.line(),
            error.column()
        ))
    })
}

/// 业务作用：在已序列化的 JSON 值中递归应用对象空值省略规则。
///
/// 参数说明：
/// - `v`：需要原位整理的 JSON 值。
///
/// 返回：删除所有对象的 null 成员并递归处理对象和数组，不删除数组 null 元素；无返回值。
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

/// 存储形态。
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
    /// 业务作用：识别应交由数组执行器处理的存储形态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：JsonArray 与 JsonArrayBucket 返回 true，Hash 与 Json 返回 false。
    pub fn is_array(&self) -> bool {
        matches!(self, DataType::JsonArray | DataType::JsonArrayBucket)
    }
}

/// 字段索引类型。
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// 精确匹配(离散值:ID/状态/枚举);FT 语法 `@f:{v}`。
    /// `separator` 是多值分隔符（默认 `,`，None 表示使用 RediSearch 默认值），`case_sensitive`
    /// 控制 CASESENSITIVE（默认不区分大小写）；配置必须与实际索引一致，否则匹配语义不同。
    Tag {
        /// 是否允许 RediSearch 对该字段排序。
        sortable: bool,
        /// 多值 Tag 分隔符；`None` 表示使用 RediSearch 默认值。
        separator: Option<char>,
        /// 是否启用大小写敏感匹配。
        case_sensitive: bool,
        /// WITHSUFFIXTRIE 会建立后缀树，启用 `*foo`（后缀）和 `*foo*`（包含）查询。
        with_suffix_trie: bool,
    },
    /// 全文搜索;FT 语法 `@f:(词)`。
    /// `no_stem`（NOSTEM 关闭词干还原）、`phonetic`（例如 `dm:en`）和 `no_index`
    /// (NOINDEX 只存不建倒排,仅 SORTABLE/投影用)。
    Text {
        /// 是否允许 RediSearch 对该字段排序。
        sortable: bool,
        /// 全文字段在相关性评分中的权重。
        weight: f64,
        /// 是否关闭词干还原。
        no_stem: bool,
        /// 可选的 RediSearch phonetic matcher 名称。
        phonetic: Option<String>,
        /// 是否只存储字段而不建立倒排索引。
        no_index: bool,
        /// WITHSUFFIXTRIE 会建立后缀树，启用 `*foo`（后缀）和 `*foo*`（包含）查询。
        with_suffix_trie: bool,
    },
    /// 数值范围字段，FT 语法为 `@f:[min max]`。
    /// no_index 启用 NOINDEX，只保存值而不建立数值索引，适用于 SORTABLE 或投影。
    Numeric {
        /// 是否允许 RediSearch 对该字段排序。
        sortable: bool,
        /// 是否只存储字段而不建立数值索引。
        no_index: bool,
    },
    /// 地理坐标；存储为 `"lon,lat"`，FT 语法为 `@f:[lon lat radius unit]`。
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
    /// 字段的索引类型与 RediSearch 修饰参数。
    pub ftype: FieldType,
}

impl FieldMeta {
    /// 业务作用：取得查询和索引定义共同使用的字段公开名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置了非空 alias 时返回它，否则返回 name；不验证名称是否适合查询或路径语法。
    pub fn alias_or_name(&self) -> &str {
        if self.alias.is_empty() {
            &self.name
        } else {
            &self.alias
        }
    }
}

/// prefix 的字面段或占位符字段引用。
/// DocMeta 可由手写或 derive 构造，在键派生时按需解析，不持有额外的全局解析缓存。
#[derive(Debug, Clone, PartialEq)]
pub enum KeySeg {
    /// 字面段:原样拼接。
    Lit(String),
    /// 占位符段:`{name}` —— name 仅作文档与 derive 编译期匹配用,
    /// 运行时值由调用方按占位符出现顺序提供(key_of_parts / placeholder_parts)。
    Field(String),
}

/// 文档元数据。
#[derive(Debug, Clone)]
pub struct DocMeta {
    /// RediSearch 索引名(如 "idx:order")。ARRAY 系不建索引,仍要求非空(诊断用)。
    pub index: String,
    /// Redis key 前缀。三种形态:
    ///   · 纯字面量:完整 key = prefix + id;
    ///   · 含 `{field}` 占位符:如 `"order:SMO:{sym}:{d}:"`,动态段运行时填充;
    ///   · ARRAY 系:完整 key = prefix + @JsonArrayKey 值拼接(占位符在 ARRAY 系禁用)。
    pub prefix: String,
    /// 文档在 Redis 中的存储与索引形态。
    pub data_type: DataType,
    /// 参与索引、查询和编解码的字段元数据。
    pub fields: Vec<FieldMeta>,
    /// @RsId 字段的存储名(JSONPath idFilter `@.{id_name}==…` 与 HASH 回填用)。
    pub id_name: String,
    /// id 在 JSON 文档中是否渲染为数字(serde 数值类型)。
    /// 决定 idFilter 字面量形态:数字不加引号、字符串加引号转义——
    /// 标记必须与实际存储类型一致，否则 JSONPath 等值条件无法命中。
    pub id_numeric: bool,
    /// ARRAY 系:@JsonArrayKey 字段名列表,**按 order 升序**(决定 key 拼接顺序,
    /// 是持久化协议——改顺序等于改 key,老数据查不到)。非 ARRAY 系为空。
    pub array_keys: Vec<String>,
    /// 仅 JsonArrayBucket 使用的桶数，必须大于 1；其它模式为 0。
    /// 桶数决定持久化键布局，不能热修改；变更时必须迁移已存储数据。
    pub bucket_count: u32,
}

impl DocMeta {
    /// 业务作用：在执行器绑定前检查文档元数据的基本完整性与存储形态约束。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：index/prefix/id_name、字段名与查询别名、占位符语法及数组配置满足当前检查时成功，否则返回配置错误；不访问 Redis，不证明实际 schema 一致或键布局无碰撞。
    pub fn validate(&self) -> Result<()> {
        // 关键身份不能为空，否则后续绑定无法明确索引、键空间或子文档标识。
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
            // 查询通过公开名称定位字段，同名声明会使类型与来源选择不再唯一。
            if !seen.insert(f.alias_or_name().to_string()) {
                return Err(NasaRedisError::Config(format!(
                    "DocMeta: 别名重复 \"{}\"",
                    f.alias_or_name()
                )));
            }
        }
        // 占位符语法校验(括号配对/非空/不嵌套)——解析即校验
        let segs = self.segments()?;
        // 存储形态决定键与执行器协议，不能让数组布局参数混入普通文档或相反。
        match self.data_type {
            DataType::JsonArray | DataType::JsonArrayBucket => {
                if self.array_keys.is_empty() {
                    return Err(NasaRedisError::Config(
                        "DocMeta: ARRAY 模式必须声明至少一个 array_keys(@JsonArrayKey)".into(),
                    ));
                }
                if segs.is_some() {
                    // ARRAY key = prefix + parts,动态性由 @JsonArrayKey 承担;
                    // 占位符与数组键派生叠加会造成键形态歧义，因此在发送命令前拒绝
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

    /// 业务作用：按查询可见名称定位文档字段元数据。
    ///
    /// 参数说明：
    /// - `alias`：字段公开别名；字段未设置 alias 时使用 name。
    ///
    /// 返回：返回第一个公开名称完全相等的字段，缺失时返回 None；不会在已设置别名时另外匹配原字段名。
    pub fn field(&self, alias: &str) -> Option<&FieldMeta> {
        self.fields.iter().find(|f| f.alias_or_name() == alias)
    }

    // ───────────────── prefix {field} 占位符─────────────────

    /// 业务作用：解析 key 前缀中的字面片段与有序字段占位符。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：没有占位符且无孤立右括号时返回 None；有效占位符返回片段列表，空名、嵌套或不配对括号返回错误。只校验语法，不校验动态段之间的边界是否足以防止键碰撞。
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

    /// 业务作用：快速识别前缀是否含占位符起始字符，供选择键构造入口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：前缀包含左花括号即返回 true，不替代 segments 的完整语法校验。
    pub fn has_placeholder(&self) -> bool {
        self.prefix.contains('{')
    }

    /// 业务作用：统计前缀中用于动态参数对位的占位符起始数量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回左花括号数量；必须先通过 segments 校验，才能将其解释为有效占位符个数。
    pub fn placeholder_count(&self) -> usize {
        self.prefix.matches('{').count()
    }

    /// 业务作用：取得索引 PREFIX 可以覆盖所有动态键变体的固定开头。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回首个左花括号之前的字面部分，没有左花括号时返回完整 prefix；占位符位于开头时可返回空串。
    pub fn literal_prefix(&self) -> &str {
        match self.prefix.find('{') {
            Some(i) => &self.prefix[..i],
            None => &self.prefix,
        }
    }

    /// 业务作用：在无占位符布局中按文档 ID 派生 Redis key。
    ///
    /// 参数说明：
    /// - `id`：原样拼到 prefix 后的文档业务 ID。
    ///
    /// 返回：成功返回 prefix 与 id 的连接结果；含占位符起始符时拒绝并要求使用 key_of_parts，不检查 id 中的分隔字符。
    pub fn key_of(&self, id: &str) -> Result<String> {
        if self.has_placeholder() {
            return Err(NasaRedisError::Config(format!(
                "prefix \"{}\" 含占位符,必须用 key_of_parts(parts, id)",
                self.prefix
            )));
        }
        Ok(format!("{}{}", self.prefix, id))
    }

    /// 业务作用：按占位符出现顺序填入业务分片，再追加文档 ID。
    ///
    /// 参数说明：
    /// - `parts`：与前缀占位符顺序逐项对应的业务值。
    /// - `id`：填充前缀后追加的文档标识。
    ///
    /// 返回：占位符模式校验参数数量和动态值中的冒号/花括号后返回 key；语法或参数不符返回错误。无占位符时要求 parts 为空并直接连接 id；此方法不证明任意前缀布局均可唯一解码。
    pub fn key_of_parts(&self, parts: &[&str], id: &str) -> Result<String> {
        let Some(segs) = self.segments()? else {
            // 没有占位符时不能静默丢弃调用方提供的分片身份。
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
        // 参数数量必须先对齐，避免不同字段的业务值被填入错误位置。
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
        // ID 同样不能引入约定的分隔符或 hash tag 边界；完整布局的唯一性仍需单独保证。
        check_part(id, "@RsId")?;
        out.push_str(id);
        Ok(out)
    }

    // ───────────────── ARRAY key 派生─────────────────

    /// 业务作用：按数组键字段顺序生成以冒号分隔的业务分组主体。
    ///
    /// 参数说明：
    /// - `parts`：与 array_keys 数量和顺序对应的业务分组值。
    ///
    /// 返回：数量相同且各段不含保留分隔符时返回主体，参数不符返回错误；空字符串段保持为空段。
    fn array_body(&self, parts: &[&str]) -> Result<String> {
        // 数组业务分组由固定字段数量和顺序定义，缺段或多段会改变持久化身份。
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

    /// 业务作用：为数组存储模式组合静态前缀与业务分组主体。
    ///
    /// 参数说明：
    /// - `parts`：按 array_keys 顺序提供的业务分组值。
    ///
    /// 返回：ARRAY 模式下返回前缀加主体，参数不符或非 ARRAY 模式返回错误；桶模式还需继续派生桶号，不能将该结果视为最终桶 key。
    pub fn array_key_of(&self, parts: &[&str]) -> Result<String> {
        if !self.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "array_key_of 仅 ARRAY 模式可用(当前 {:?})",
                self.data_type
            )));
        }
        Ok(format!("{}{}", self.prefix, self.array_body(parts)?))
    }

    /// 业务作用：按持久化协议把子文档标识映射到固定桶号。
    ///
    /// 参数说明：
    /// - `sub_id`：用于计算 UTF-16 String.hashCode 的子文档标识原文。
    ///
    /// 返回：非桶模式返回错误；已通过 validate 的桶模式返回 floorMod 后的小于 bucket_count 的编号。本方法依赖合法非零桶数，不重复验证配置。
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

    /// 业务作用：为一个子文档生成带分组 hash tag 的持久化桶 key。
    ///
    /// 参数说明：
    /// - `parts`：决定分组 hash tag 的有序业务值。
    /// - `sub_id`：用于稳定选择桶号的子文档标识原文。
    ///
    /// 返回：元数据已验证且参数有效时返回前缀、花括号主体与桶号组合的 key；模式或分组参数不符返回错误。同组桶共享 Cluster slot，桶数和字段顺序变更需要迁移数据。
    pub fn bucket_key(&self, parts: &[&str], sub_id: &str) -> Result<String> {
        let idx = self.bucket_index_of(sub_id)?;
        Ok(format!(
            "{}{{{}}}:bucket:{}",
            self.prefix,
            self.array_body(parts)?,
            idx
        ))
    }

    /// 业务作用：按桶号顺序展开同一业务分组的全部桶 key，供跨桶 Lua 使用。
    ///
    /// 参数说明：
    /// - `parts`：决定该组共同 hash tag 的有序业务值。
    ///
    /// 返回：桶模式且分组参数有效时分配并返回 bucket_count 个 key，非桶模式或参数不符返回错误；容量随桶数增长，不访问 Redis。
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

    /// 业务作用：从带期望前缀的数组 key 提取业务分组段。
    ///
    /// 参数说明：
    /// - `key`：扫描或事件提供的完整数组或桶 key。
    ///
    /// 返回：模式、前缀、桶标记形态及分组段数符合当前检查时返回各段，否则返回错误；当前不校验桶号后缀或重新校验各段保留字符，返回值不是完整来源权威证明。
    pub fn parse_array_key(&self, key: &str) -> Result<Vec<String>> {
        if !self.data_type.is_array() {
            return Err(NasaRedisError::Config(format!(
                "parse_array_key 仅 ARRAY 模式可用(当前 {:?})",
                self.data_type
            )));
        }
        // 先限制静态前缀，不能把其它键空间的扫描结果解释为本组业务字段。
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
        // 段数不符意味着无法按声明的业务字段还原身份，必须拒绝归组。
        if parts.len() != self.array_keys.len() {
            return Err(NasaRedisError::Config(format!(
                "key \"{key}\" 解析出 {} 段,期望 {}(@JsonArrayKey 字段数)",
                parts.len(),
                self.array_keys.len()
            )));
        }
        Ok(parts)
    }

    /// 业务作用：按数组存储形态为子文档选择普通数组键或对应桶键。
    ///
    /// 参数说明：
    /// - `parts`：按 array_keys 顺序提供的业务分组值。
    /// - `sub_id`：桶模式用于稳定选择桶号的子文档标识，普通数组模式不使用。
    ///
    /// 返回：有效 ARRAY 配置和参数返回目标 key，非 ARRAY 模式或下层参数校验失败返回错误。
    pub fn array_key_for(&self, parts: &[&str], sub_id: &str) -> Result<String> {
        match self.data_type {
            DataType::JsonArray => self.array_key_of(parts),
            DataType::JsonArrayBucket => self.bucket_key(parts, sub_id),
            other => Err(NasaRedisError::Config(format!(
                "array_key_for 仅 ARRAY 模式可用(当前 {other:?})"
            ))),
        }
    }

    /// 业务作用：从字段元数据生成 FT.CREATE 的 SCHEMA 参数列表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按字段顺序返回存储来源、公开别名、索引类型和修饰参数；Json 使用显式 JSONPath 或默认字段路径，Hash 使用字段名及可选 AS。不会发送命令，也不保证任意手写参数均被服务端接受。
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
                FieldType::Numeric {
                    sortable,
                    no_index,
                } => {
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

/// 业务作用：拒绝会与数组分组分隔符或 Cluster hash tag 边界冲突的动态值。
///
/// 参数说明：
/// - `s`：准备拼接到 key 动态段的业务原文。
/// - `desc`：错误中标识该动态段角色的描述。
///
/// 返回：不含冒号和花括号时成功，包括空字符串；出现保留字符时返回配置错误。仅校验单段，不证明上层前缀布局可唯一解码。
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
/// HASH 编解码内聚于 trait;
/// JSON 模式经 serde(actuator 对 T: Serialize+DeserializeOwned 额外约束)。
pub trait RedisDocument: Sized + Send + Sync {
    /// 业务作用：向执行器提供该文档类型共享的静态索引和键布局元数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回进程内持续有效的 DocMeta 引用；实现方应保持其与编码、标识及数组分组规则一致，绑定时再执行元数据校验。
    fn meta() -> &'static DocMeta;

    /// 业务作用：提供文档的业务标识，供键派生及数组子文档定位使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回标识的字符串形式，必须与持久化文档字段及 id_numeric 类型约定一致；不在此分配新标识。
    fn id(&self) -> String;

    /// 业务作用：将业务文档编码为 HASH 字段和值，供完整文档写入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回该实现决定的字段值列表，应包含标识及后续解码需要的占位符字段；字段省略和类型表示由具体实现负责。
    fn to_fields(&self) -> Vec<(String, String)>;

    /// 业务作用：从读取到的 HASH 字段表重建业务文档。
    ///
    /// 参数说明：
    /// - `fields`：以实际存储字段名为键的字符串值映射。
    ///
    /// 返回：字段满足具体实现的解码约定时返回文档，否则返回该实现的错误；缺字段采用默认值还是拒绝由实现确定。
    fn from_fields(fields: &HashMap<String, String>) -> Result<Self>;

    /// 业务作用：提供当前文档对应的前缀占位符值，供有序键派生使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与元数据占位符顺序一致的字符串列表；默认返回空列表，只适合不需要这些动态值的布局。
    fn placeholder_parts(&self) -> Vec<String> {
        Vec::new()
    }

    /// 业务作用：提供当前文档所属数组业务分组的有序字段值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回与元数据 array_keys 顺序一致的字符串列表；默认返回空列表，实际数组文档实现应提供所需分组值。
    fn array_key_parts(&self) -> Vec<String> {
        Vec::new()
    }
}

/// 业务作用：一次序列化文档并应用空成员省略规则，供编码与写入前身份校验共享同一份内容。
///
/// 参数说明：
/// - `doc`：提供 RedisJSON 持久化内容的业务文档。
///
/// 返回：成功返回已递归省略对象 null 成员的 JSON 值，数组 null 位置保留；错误只含类别与位置，普通序列化展开返回固定 Codec。不重试 Serialize，不撤销其自身副作用或改变宿主 panic hook。
fn to_json_value_omit_null<T: serde::Serialize>(doc: &T) -> Result<serde_json::Value> {
    // 自定义 Serialize 可能把正文写进错误或展开载荷；文档编码必须在发送命令前结束，返回摘要不携带这些内容。
    let encoded =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| serde_json::to_value(doc)))
            .map_err(|_| NasaRedisError::Codec("JSON 文档序列化展开".into()))?;
    let mut v = encoded.map_err(|error| {
        NasaRedisError::Codec(format!(
            "JSON 文档序列化失败: {:?} at {}:{}",
            error.classify(),
            error.line(),
            error.column()
        ))
    })?;
    strip_nulls(&mut v);
    Ok(v)
}
