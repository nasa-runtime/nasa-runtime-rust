// ============================================================================
// src/search/query.rs —— 类型化查询 AST + renderer(文档)。
//
// 红线
//   · 查询构造成【类型化 AST】,由 renderer 生成 FT.SEARCH 查询串——字段名/类型在
//     render(meta) 时校验(不存在的别名、Tag 字段配 between 等在发命令前报错);
//   · Tag 值转义 RediSearch 特殊字符(防注入/语法破坏);
//   · 基础查询仅组合 AND；OR 由 FT.AGGREGATE 表达式承担；
//   · 只有显式 RawQuery 允许可信调用方传原始表达式(注入防护边界明示)。
// move 语义:render 取 &self 可重复渲染;Query 本身是普通值(原实现 RsQuery 的
// “池化一次性”在 Rust 中由所有权天然消解。
// ============================================================================

use super::{DocMeta, FieldType};
use crate::error::{NasaRedisError, Result};

/// 数值开区间与比较算子，对齐既有系统的 NumericField EQ/GT/GTE/LT/LTE 语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumOp {
    /// `> v`:FT `[(v +inf]`、JSONPath `@.f>v`。
    Gt,
    /// `>= v`:FT `[v +inf]`、JSONPath `@.f>=v`。
    Gte,
    /// `< v`:FT `[-inf (v]`、JSONPath `@.f<v`。
    Lt,
    /// `<= v`:FT `[-inf v]`、JSONPath `@.f<=v`。
    Lte,
    /// `== v`:FT `[v v]`、JSONPath `@.f==v`。
    Eq,
}

/// 单条查询条件(类型化;与字段类型的匹配在 render 时强校验)。
#[derive(Debug, Clone)]
pub enum Cond {
    /// Tag 精确匹配:`@f:{v}`(值自动转义)。
    TagEq {
        /// 字段别名或字段名。
        field: String,
        /// 需要精确匹配的 Tag 字面值。
        value: String,
    },
    /// Tag 多选一:`@f:{v1|v2}`。
    TagIn {
        /// 字段别名或字段名。
        field: String,
        /// 任一命中即可的 Tag 字面值集合。
        values: Vec<String>,
    },
    /// 数值闭区间:`@f:[min max]`。
    NumBetween {
        /// 字段别名或字段名。
        field: String,
        /// 闭区间下界。
        min: f64,
        /// 闭区间上界。
        max: f64,
    },
    /// 数值开区间与比较；FT 使用 `±inf` sentinel，避免用 `between(X, f64::MAX)` 模拟。
    NumCmp {
        /// 字段别名或字段名。
        field: String,
        /// 数值比较算子。
        op: NumOp,
        /// 比较基准值。
        value: f64,
    },
    /// 全文匹配:`@f:(text)`。
    TextMatch {
        /// 字段别名或字段名。
        field: String,
        /// 需要匹配的全文词项。
        text: String,
    },
    /// 全文**前缀**：`@f:(term*)`，匹配以 term 开头的词。
    TextPrefix {
        /// 字段别名或字段名。
        field: String,
        /// 词项前缀。
        prefix: String,
    },
    /// 全文**后缀**：`@f:(*term)`，字段必须启用 WITHSUFFIXTRIE。
    TextSuffix {
        /// 字段别名或字段名。
        field: String,
        /// 词项后缀。
        suffix: String,
    },
    /// 全文**包含/中缀**：`@f:(*term*)`，字段必须启用 WITHSUFFIXTRIE。
    TextContains {
        /// 字段别名或字段名。
        field: String,
        /// 词项中必须包含的片段。
        infix: String,
    },
    /// 全文**模糊**：使用 Levenshtein 距离语法 `%term%`、`%%term%%` 或 `%%%term%%%`。
    TextFuzzy {
        /// 字段别名或字段名。
        field: String,
        /// 需要模糊匹配的词项。
        term: String,
        /// 允许的 Levenshtein 距离。
        distance: u8,
    },
    /// 全文**短语**：`@f:("a b c")`，按词序精确匹配。
    TextPhrase {
        /// 字段别名或字段名。
        field: String,
        /// 按词序匹配的完整短语。
        phrase: String,
    },
    /// **地理半径圆内**：`@f:[lon lat radius unit]`。
    GeoWithin {
        /// 字段别名或字段名。
        field: String,
        /// 圆心经度。
        lon: f64,
        /// 圆心纬度。
        lat: f64,
        /// 搜索半径。
        radius: f64,
        /// 搜索半径单位。
        unit: GeoUnit,
    },
    /// 原始表达式(★仅可信调用方:绕过校验与转义,注入风险自负)。
    Raw(String),
}

/// 地理半径单位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnit {
    /// 米。
    M,
    /// 千米。
    Km,
    /// 英里。
    Mi,
    /// 英尺。
    Ft,
}

impl GeoUnit {
    /// 业务作用：返回 RediSearch GEO 半径参数使用的单位字符串。
    ///
    /// 该值会直接拼进 `FT.SEARCH`/`FT.AGGREGATE` 的 GEO 过滤表达式。
    fn as_str(&self) -> &'static str {
        match self {
            GeoUnit::M => "m",
            GeoUnit::Km => "km",
            GeoUnit::Mi => "mi",
            GeoUnit::Ft => "ft",
        }
    }
}

/// 查询(AND 组合 + 单字段排序 + 分页;对照 原实现 RsQuery 的能力面)。
#[derive(Debug, Clone, Default)]
pub struct Query {
    conds: Vec<Cond>,
    sort: Option<(String, bool)>, // (alias, desc)
    offset: usize,
    limit: usize,
    /// 调用方是否显式设置过 limit——ARRAY 模式(JSONPath filter)不支持分页,
    /// 必须区分"默认 10"与"业务真要分页"(对照 原实现 limitExplicit 标记)。
    limit_explicit: bool,
}

impl Query {
    /// 业务作用：创建不含过滤条件、使用默认分页上限的查询构造器。
    pub fn new() -> Self {
        Self {
            conds: Vec::new(),
            sort: None,
            offset: 0,
            limit: 10,
            limit_explicit: false,
        }
    }

    /// 业务作用：追加一个 TAG 精确匹配条件,用于状态、枚举、业务 ID 等离散值查询。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的字段别名或字段名,渲染时会校验它必须是 TAG 字段。
    /// - `value`: 要匹配的单个 tag 字面值,渲染时按 RediSearch TAG 语法转义。
    pub fn tag_eq(mut self, field: &str, value: &str) -> Self {
        self.conds.push(Cond::TagEq {
            field: field.into(),
            value: value.into(),
        });
        self
    }

    /// 业务作用：追加一个 TAG 多选一条件,等价于同一字段的离散值 OR。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的字段别名或字段名,渲染时会校验它必须是 TAG 字段。
    /// - `values`: 可接受的 tag 字面值列表,每个值都会按 TAG 语法转义后用 `|` 连接。
    pub fn tag_in(mut self, field: &str, values: &[&str]) -> Self {
        self.conds.push(Cond::TagIn {
            field: field.into(),
            values: values.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// 业务作用：追加一个数值闭区间条件,用于价格、数量、时间戳等范围查询。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的字段别名或字段名,渲染时会校验它必须是 NUMERIC 字段。
    /// - `min`: 区间下界,包含该值,必须是有限数。
    /// - `max`: 区间上界,包含该值,必须是有限数且不小于 `min`。
    pub fn between(mut self, field: &str, min: f64, max: f64) -> Self {
        self.conds.push(Cond::NumBetween {
            field: field.into(),
            min,
            max,
        });
        self
    }

    /// 业务作用：数值开区间比较：`gt/gte/lt/lte/eq`；FT 使用 `±inf` sentinel，
    /// 不再用 `between(X, f64::MAX)` 与 原实现 `[(X +inf]` 跨语言分叉。
    ///
    /// # 参数
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    /// - `op`: 当前要执行的 Redis 或配置操作。
    /// - `value`: 需要与搜索字段比较的数值。
    fn num_cmp(mut self, field: &str, op: NumOp, value: f64) -> Self {
        self.conds.push(Cond::NumCmp {
            field: field.into(),
            op,
            value,
        });
        self
    }

    /// 业务作用：追加一个数值大于条件。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 NUMERIC 字段别名或字段名。
    /// - `value`: 开区间边界,查询只匹配大于该值的文档。
    pub fn gt(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Gt, value)
    }

    /// 业务作用：追加一个数值大于等于条件。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 NUMERIC 字段别名或字段名。
    /// - `value`: 闭区间边界,查询匹配大于等于该值的文档。
    pub fn gte(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Gte, value)
    }

    /// 业务作用：追加一个数值小于条件。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 NUMERIC 字段别名或字段名。
    /// - `value`: 开区间边界,查询只匹配小于该值的文档。
    pub fn lt(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Lt, value)
    }

    /// 业务作用：追加一个数值小于等于条件。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 NUMERIC 字段别名或字段名。
    /// - `value`: 闭区间边界,查询匹配小于等于该值的文档。
    pub fn lte(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Lte, value)
    }

    /// 业务作用：追加一个数值相等条件。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 NUMERIC 字段别名或字段名。
    /// - `value`: 要精确匹配的数值,必须是有限数。
    pub fn num_eq(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Eq, value)
    }

    /// 业务作用：追加一个全文字面匹配条件,用于普通 TEXT 字段的分词检索。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的字段别名或字段名,渲染时会校验它必须是 TEXT 字段。
    /// - `text`: 要匹配的用户输入文本,会按字面量转义,不会被当作原始查询语法。
    pub fn text_match(mut self, field: &str, text: &str) -> Self {
        self.conds.push(Cond::TextMatch {
            field: field.into(),
            text: text.into(),
        });
        self
    }

    /// 业务作用：全文前缀匹配：`@f:(prefix*)`。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 TEXT 字段别名或字段名。
    /// - `prefix`: 词项前缀,会按字面量转义后再追加 `*` 前缀匹配语法。
    pub fn text_prefix(mut self, field: &str, prefix: &str) -> Self {
        self.conds.push(Cond::TextPrefix {
            field: field.into(),
            prefix: prefix.into(),
        });
        self
    }

    /// 业务作用：全文后缀匹配：`@f:(*suffix)`，字段建索引时必须启用 WITHSUFFIXTRIE。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 TEXT/TAG 字段别名或字段名,且必须启用 WITHSUFFIXTRIE。
    /// - `suffix`: 词项后缀,会按字面量转义后拼成后缀查询。
    pub fn text_suffix(mut self, field: &str, suffix: &str) -> Self {
        self.conds.push(Cond::TextSuffix {
            field: field.into(),
            suffix: suffix.into(),
        });
        self
    }

    /// 业务作用：全文包含或中缀匹配：`@f:(*infix*)`，字段建索引时必须启用 WITHSUFFIXTRIE。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 TEXT/TAG 字段别名或字段名,且必须启用 WITHSUFFIXTRIE。
    /// - `infix`: 词项中缀,会按字面量转义后拼成包含查询。
    pub fn text_contains(mut self, field: &str, infix: &str) -> Self {
        self.conds.push(Cond::TextContains {
            field: field.into(),
            infix: infix.into(),
        });
        self
    }

    /// 业务作用：全文模糊匹配，Levenshtein 距离限制为 1..=3，越界时收敛到有效范围。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 TEXT 字段别名或字段名。
    /// - `term`: 模糊匹配的词项,会按字面量转义。
    /// - `distance`: Levenshtein 最大距离,渲染时限制在 1 到 3。
    pub fn text_fuzzy(mut self, field: &str, term: &str, distance: u8) -> Self {
        self.conds.push(Cond::TextFuzzy {
            field: field.into(),
            term: term.into(),
            distance,
        });
        self
    }

    /// 业务作用：全文短语匹配：`@f:("a b c")`，词序必须精确。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 TEXT 字段别名或字段名。
    /// - `phrase`: 要按词序匹配的短语,内部引号和反斜杠会被转义。
    pub fn text_phrase(mut self, field: &str, phrase: &str) -> Self {
        self.conds.push(Cond::TextPhrase {
            field: field.into(),
            phrase: phrase.into(),
        });
        self
    }

    /// 业务作用：查询 GEO 字段在指定圆心和半径内的文档。
    ///
    /// # 参数
    /// - `field`: `DocMeta` 中声明的 GEO 字段别名或字段名。
    /// - `lon`: 圆心经度,必须是有限数。
    /// - `lat`: 圆心纬度,必须是有限数。
    /// - `radius`: 搜索半径,必须是非负有限数。
    /// - `unit`: 半径单位,控制渲染到 RediSearch 的 `m`/`km`/`mi`/`ft`。
    pub fn geo_within(
        mut self,
        field: &str,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> Self {
        self.conds.push(Cond::GeoWithin {
            field: field.into(),
            lon,
            lat,
            radius,
            unit,
        });
        self
    }

    /// 业务作用：原始表达式逃生舱(可信调用方;文档红线:默认路径必须走类型化 AST)。
    ///
    /// # 参数
    /// - `expr`: 可信调用方提供的完整 FT 查询片段,会直接拼入查询表达式。
    pub fn raw(mut self, expr: &str) -> Self {
        self.conds.push(Cond::Raw(expr.into()));
        self
    }

    /// 业务作用：单字段降序排序(FT.SEARCH 原生只支持单字段;多字段 = FT.AGGREGATE)。
    ///
    /// # 参数
    /// - `field`: 用于排序的 `DocMeta` 字段别名或字段名,渲染时必须声明为 SORTABLE。
    pub fn sort_desc(mut self, field: &str) -> Self {
        self.sort = Some((field.into(), true));
        self
    }

    /// 业务作用：单字段升序排序(FT.SEARCH 原生只支持单字段;多字段 = FT.AGGREGATE)。
    ///
    /// # 参数
    /// - `field`: 用于排序的 `DocMeta` 字段别名或字段名,渲染时必须声明为 SORTABLE。
    pub fn sort_asc(mut self, field: &str) -> Self {
        self.sort = Some((field.into(), false));
        self
    }

    /// 业务作用：设置 FT.SEARCH 查询窗口。
    ///
    /// # 参数
    /// - `offset`: 起始偏移量,对应 RediSearch `LIMIT offset limit` 的 offset。
    /// - `limit`: 最大返回条数,同时标记调用方显式设置过分页。
    pub fn limit(mut self, offset: usize, limit: usize) -> Self {
        self.offset = offset;
        self.limit = limit;
        self.limit_explicit = true;
        self
    }

    /// 业务作用：调用方是否显式设置过 limit(对照 原实现 limitExplicit;`find_keys` 据此决定默认 `LIMIT 0 1000000`)。
    pub fn is_limit_explicit(&self) -> bool {
        self.limit_explicit
    }

    /// 业务作用：有效 offset(默认 0)。
    pub fn effective_offset(&self) -> usize {
        self.offset
    }

    /// 业务作用：有效 limit(默认 10;未显式设置时 `find_keys` 会改用大上限)。
    pub fn effective_limit(&self) -> usize {
        self.limit
    }

    /// 业务作用：渲染查询串(字段/类型强校验;meta 是唯一事实源)。
    /// 渲染为 FT 查询串。**返回空串 `""` = MATCH_NONE 哨兵**:某个 TEXT 条件的值
    /// 转义后为纯空白(纯标点/空串,如 `+++`/`...`)——无任何可匹配 token,整条 AND query 匹配空集。
    /// 此时**绝不**发出 `@field:(   )`(FT 语法错误);调用方(find/count/aggregate)见 `""` 直接返回
    /// 空结果/0,不打 FT.SEARCH。真·无条件用 `*`(见下),与 `""` 区分。
    ///
    /// # 参数
    /// - `meta`: 当前文档类型的元数据,用于校验字段存在性、字段类型和查询能力。
    pub fn render(&self, meta: &DocMeta) -> Result<String> {
        if self.conds.is_empty() {
            return Ok("*".into()); // 全量(配 limit 用)
        }
        let mut parts = Vec::with_capacity(self.conds.len());
        for c in &self.conds {
            parts.push(match c {
                Cond::TagEq { field, value } => {
                    let f = require_field(meta, field, "TagEq")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    format!("@{}:{{{}}}", f.alias_or_name(), escape_tag(value))
                }
                Cond::TagIn { field, values } => {
                    let f = require_field(meta, field, "TagIn")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    let alts: Vec<String> = values.iter().map(|v| escape_tag(v)).collect();
                    format!("@{}:{{{}}}", f.alias_or_name(), alts.join("|"))
                }
                Cond::NumBetween { field, min, max } => {
                    let f = require_field(meta, field, "NumBetween")?;
                    require_type(f, matches!(f.ftype, FieldType::Numeric { .. }), "Numeric")?;
                    //FT 路径与 JSONPath 路径统一——拒绝 NaN/Infinity 与 min>max
                    // (NaN 会渲染成 "NaN" 破坏 FT 数值区间语法;min>max 是空区间的静默错配)。
                    if !min.is_finite() || !max.is_finite() {
                        return Err(NasaRedisError::Config(format!(
                            "NumBetween 字段 \"{field}\" 的 min/max 必须有限(min={min}, max={max})"
                        )));
                    }
                    if min > max {
                        return Err(NasaRedisError::Config(format!(
                            "NumBetween 字段 \"{field}\" min({min}) > max({max}),空区间"
                        )));
                    }
                    // 注:FT NUMERIC 解析 "1"/"1.0"/"1.0E10" 等价,**查询渲染格式不影响匹配**(Redis 按
                    // 解析后的数值比较），故此处保留 Rust 格式；跨语言格式对齐只需在**存储**路径完成。
                    format!("@{}:[{} {}]", f.alias_or_name(), min, max)
                }
                Cond::NumCmp { field, op, value } => {
                    let f = require_field(meta, field, "NumCmp")?;
                    require_type(f, matches!(f.ftype, FieldType::Numeric { .. }), "Numeric")?;
                    if !value.is_finite() {
                        return Err(NasaRedisError::Config(format!(
                            "NumCmp 字段 \"{field}\" 的值必须有限(value={value})"
                        )));
                    }
                    // FT NUMERIC:`(` 前缀 = 开区间;`±inf` sentinel(对齐 原实现 `[(X +inf]` 等)
                    let range = match op {
                        NumOp::Gt => format!("[({value} +inf]"),
                        NumOp::Gte => format!("[{value} +inf]"),
                        NumOp::Lt => format!("[-inf ({value}]"),
                        NumOp::Lte => format!("[-inf {value}]"),
                        NumOp::Eq => format!("[{value} {value}]"),
                    };
                    format!("@{}:{}", f.alias_or_name(), range)
                }
                Cond::TextMatch { field, text } => {
                    let f = require_field(meta, field, "TextMatch")?;
                    require_type(f, matches!(f.ftype, FieldType::Text { .. }), "Text")?;
                    //调用者文本**必须转义**——未转义时空格/括号/`|-@*"`等会改变
                    // FT 查询结构(把单字段匹配扩成任意查询/注入)。这里按字面量严格转义;
                    // 需要短语/原始语法的调用方走 Raw(命名明确,仅给可信来源)。
                    let esc = escape_text(text);
                    if esc.trim().is_empty() {
                        return Ok(String::new()); // 见下 MATCH_NONE 哨兵说明
                    }
                    format!("@{}:({})", f.alias_or_name(), esc)
                }
                Cond::TextPrefix { field, prefix } => {
                    let f = require_field(meta, field, "TextPrefix")?;
                    require_type(f, matches!(f.ftype, FieldType::Text { .. }), "Text")?;
                    let esc = escape_text(prefix);
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    // term 转义(限字面),`*` 作前缀语法附在转义后(不被转义)
                    format!("@{}:({}*)", f.alias_or_name(), esc)
                }
                Cond::TextSuffix { field, suffix } => {
                    let f = require_field(meta, field, "TextSuffix")?;
                    require_suffix_trie(f, "TextSuffix")?;
                    let esc = escape_text(suffix);
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    format!("@{}:(*{})", f.alias_or_name(), esc)
                }
                Cond::TextContains { field, infix } => {
                    let f = require_field(meta, field, "TextContains")?;
                    require_suffix_trie(f, "TextContains")?;
                    let esc = escape_text(infix);
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    format!("@{}:(*{}*)", f.alias_or_name(), esc)
                }
                Cond::TextFuzzy {
                    field,
                    term,
                    distance,
                } => {
                    let f = require_field(meta, field, "TextFuzzy")?;
                    require_type(f, matches!(f.ftype, FieldType::Text { .. }), "Text")?;
                    let esc = escape_text(term);
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    // RediSearch fuzzy:每侧 `%` 个数 = LD 距离(1..=3)
                    let pct = "%".repeat((*distance).clamp(1, 3) as usize);
                    format!("@{}:({}{}{})", f.alias_or_name(), pct, esc, pct)
                }
                Cond::TextPhrase { field, phrase } => {
                    let f = require_field(meta, field, "TextPhrase")?;
                    require_type(f, matches!(f.ftype, FieldType::Text { .. }), "Text")?;
                    // 短语:引号包裹,内部仅转义 `"`/`\`(空格作词分隔保留,使引擎按词序匹配)
                    let esc: String = phrase
                        .chars()
                        .flat_map(|c| match c {
                            '"' => vec!['\\', '"'],
                            '\\' => vec!['\\', '\\'],
                            c => vec![c],
                        })
                        .collect();
                    format!("@{}:(\"{}\")", f.alias_or_name(), esc)
                }
                Cond::GeoWithin {
                    field,
                    lon,
                    lat,
                    radius,
                    unit,
                } => {
                    let f = require_field(meta, field, "GeoWithin")?;
                    require_type(f, matches!(f.ftype, FieldType::Geo), "Geo")?;
                    if !lon.is_finite() || !lat.is_finite() || !radius.is_finite() || *radius < 0.0
                    {
                        return Err(NasaRedisError::Config(format!(
                            "GeoWithin 字段 \"{field}\":lon/lat/radius 必须有限且 radius≥0"
                        )));
                    }
                    format!(
                        "@{}:[{} {} {} {}]",
                        f.alias_or_name(),
                        lon,
                        lat,
                        radius,
                        unit.as_str()
                    )
                }
                Cond::Raw(expr) => format!("({expr})"),
            });
        }
        Ok(parts.join(" ")) // FT.SEARCH 空格 = AND
    }

    /// 业务作用：排序/分页参数(actuator 组命令用;sortable 校验在此)。
    ///
    /// # 参数
    /// - `meta`: 当前文档类型的元数据,用于校验排序字段存在且声明为 SORTABLE。
    pub fn render_args(&self, meta: &DocMeta) -> Result<Vec<String>> {
        let mut args = Vec::new();
        if let Some((field, desc)) = &self.sort {
            let f = require_field(meta, field, "sort")?;
            let sortable = matches!(
                f.ftype,
                FieldType::Tag { sortable: true, .. }
                    | FieldType::Text { sortable: true, .. }
                    | FieldType::Numeric { sortable: true, .. }
            );
            if !sortable {
                return Err(NasaRedisError::Config(format!(
                    "字段 \"{field}\" 未声明 SORTABLE,不能排序(metadata 构建期约束)"
                )));
            }
            args.push("SORTBY".into());
            args.push(f.alias_or_name().to_string());
            args.push(if *desc { "DESC".into() } else { "ASC".into() });
        }
        args.push("LIMIT".into());
        args.push(self.offset.to_string());
        args.push(self.limit.to_string());
        Ok(args)
    }
}

// Looks up a field and reports the current query context on failure.
///
/// # 参数
/// 业务作用：- `meta`: 搜索字段的元数据定义。
/// - `alias`: 搜索查询中暴露给业务侧的字段别名。
/// - `ctx`: 本次格式化、日志或运行阶段的上下文。
fn require_field<'a>(meta: &'a DocMeta, alias: &str, ctx: &str) -> Result<&'a super::FieldMeta> {
    meta.field(alias).ok_or_else(|| {
        NasaRedisError::Config(format!(
            "{ctx}: 字段 \"{alias}\" 不在 DocMeta(可用: {:?})",
            meta.fields
                .iter()
                .map(|f| f.alias_or_name())
                .collect::<Vec<_>>()
        ))
    })
}

/// 业务作用：后缀与中缀查询要求字段是 Text/Tag **且** 建索引时启用 WITHSUFFIXTRIE；
/// 否则引擎扫不出结果(静默空),提前 fail-fast 引导改 schema。
///
/// # 参数
/// - `f`: 需要校验后缀/中缀查询能力的搜索字段元数据。
/// - `ctx`: 本次格式化、日志或运行阶段的上下文。
fn require_suffix_trie(f: &super::FieldMeta, ctx: &str) -> Result<()> {
    let ok = matches!(
        f.ftype,
        FieldType::Text {
            with_suffix_trie: true,
            ..
        } | FieldType::Tag {
            with_suffix_trie: true,
            ..
        }
    );
    if !ok {
        return Err(NasaRedisError::Config(format!(
            "{ctx}: 字段 \"{}\" 需 Text/Tag 且建索引时 WITHSUFFIXTRIE(实际 {:?})",
            f.alias_or_name(),
            f.ftype
        )));
    }
    Ok(())
}

// Checks that a field has the required query type.
///
/// # 参数
/// 业务作用：- `f`: 需要校验查询类型兼容性的搜索字段元数据。
/// - `ok`: 实际校验结果。
/// - `want`: 断言期望的结果。
fn require_type(f: &super::FieldMeta, ok: bool, want: &str) -> Result<()> {
    if !ok {
        return Err(NasaRedisError::Config(format!(
            "字段 \"{}\" 类型 {:?} 与条件不符(需要 {want})",
            f.alias_or_name(),
            f.ftype
        )));
    }
    Ok(())
}

impl Query {
    /// 业务作用：渲染为 RedisJSON JSONPath filter(ARRAY 模式查询通道;
    /// 对照 原实现 RsQuery.toJsonPathFilter + Criteria.appendJsonPath)。
    ///
    /// # 参数
    /// - `meta`: 当前文档类型的元数据,用于校验字段类型并决定 JSON 字段别名。
    ///
    /// 约束(对照 原实现 逐条):
    ///   · 不支持 sort(子文档级排序 RedisJSON 不支持;要排序用 DataType::Json 单文档);
    ///   · 不支持显式分页(limit_explicit;默认 limit 视为未分页);
    ///   · TextMatch/Raw 无法翻译成 JSONPath(TEXT 是 FT 引擎语义;Raw 是 FT 语法)→ 拒绝;
    ///   · 无条件 = `$[*]`(全量);
    ///   · TagEq → `@.f=='v'`(单引号字面量);TagIn → `(@.f=='v1' || @.f=='v2')`;NumBetween →
    ///     `(@.f>=min && @.f<=max)`;多条件空格语义换 ` && `(JSONPath AND)。
    /// 字段引用用 alias(`@.alias`)——与 FT.SEARCH/JSON 文档字段名保持一致。
    pub fn render_json_path_filter(&self, meta: &DocMeta) -> Result<String> {
        if self.sort.is_some() {
            return Err(NasaRedisError::Config(
                "ARRAY 模式不支持 sort(子文档级排序 RedisJSON 不支持;需排序用 DataType::Json)"
                    .into(),
            ));
        }
        if self.limit_explicit {
            return Err(NasaRedisError::Config(
                "ARRAY 模式不支持分页(子文档级 limit 不支持;需分页用 DataType::Json 或业务层补救)"
                    .into(),
            ));
        }
        if self.conds.is_empty() {
            return Ok("$[*]".into());
        }
        let mut parts = Vec::with_capacity(self.conds.len());
        for c in &self.conds {
            parts.push(match c {
                Cond::TagEq { field, value } => {
                    let f = require_field(meta, field, "TagEq")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    format!("@.{}=={}", f.alias_or_name(), json_path_literal(value)?)
                }
                Cond::TagIn { field, values } => {
                    let f = require_field(meta, field, "TagIn")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    let alias = f.alias_or_name();
                    let alts: Vec<String> = values
                        .iter()
                        .map(|v| Ok(format!("@.{alias}=={}", json_path_literal(v)?)))
                        .collect::<Result<Vec<_>>>()?;
                    format!("({})", alts.join(" || "))
                }
                Cond::NumBetween { field, min, max } => {
                    let f = require_field(meta, field, "NumBetween")?;
                    require_type(f, matches!(f.ftype, FieldType::Numeric { .. }), "Numeric")?;
                    if !min.is_finite() || !max.is_finite() {
                        // NaN/Infinity 不是合法 JSON 数字，RedisJSON parser 会直接报错。
                        return Err(NasaRedisError::Config(
                            "JSONPath 数值必须有限(NaN/Infinity 非法)".into(),
                        ));
                    }
                    //与 FT 路径**统一**——拒绝 min>max(空区间),两通道语义一致
                    if min > max {
                        return Err(NasaRedisError::Config(format!(
                            "NumBetween 字段 \"{field}\" min({min}) > max({max}),空区间"
                        )));
                    }
                    let alias = f.alias_or_name();
                    format!("(@.{alias}>={min} && @.{alias}<={max})")
                }
                Cond::NumCmp { field, op, value } => {
                    let f = require_field(meta, field, "NumCmp")?;
                    require_type(f, matches!(f.ftype, FieldType::Numeric { .. }), "Numeric")?;
                    if !value.is_finite() {
                        return Err(NasaRedisError::Config(
                            "JSONPath 数值必须有限(NaN/Infinity 非法)".into(),
                        ));
                    }
                    let alias = f.alias_or_name();
                    match op {
                        NumOp::Gt => format!("(@.{alias}>{value})"),
                        NumOp::Gte => format!("(@.{alias}>={value})"),
                        NumOp::Lt => format!("(@.{alias}<{value})"),
                        NumOp::Lte => format!("(@.{alias}<={value})"),
                        NumOp::Eq => format!("(@.{alias}=={value})"),
                    }
                }
                Cond::TextMatch { .. }
                | Cond::TextPrefix { .. }
                | Cond::TextSuffix { .. }
                | Cond::TextContains { .. }
                | Cond::TextFuzzy { .. }
                | Cond::TextPhrase { .. }
                | Cond::GeoWithin { .. } => {
                    return Err(NasaRedisError::Config(
                        "ARRAY 模式不支持 TEXT/GEO 查询(TextMatch/Prefix/Suffix/Contains/Fuzzy/Phrase/GeoWithin 是 FT 引擎语义,JSONPath 无对应)".into(),
                    ))
                }
                Cond::Raw(_) => {
                    return Err(NasaRedisError::Config(
                        "ARRAY 模式不支持 Raw(FT 语法不能注入 JSONPath)".into(),
                    ))
                }
            });
        }
        Ok(format!("$[?({})]", parts.join(" && ")))
    }
}

// ───────────────────────── FT.AGGREGATE(最小封装)─────────────────────────

/// 聚合 reducer(对照 原实现 query/Reducer)。
#[derive(Debug, Clone)]
pub enum Reducer {
    /// REDUCE COUNT 0。
    Count,
    /// REDUCE SUM 1 @field。
    Sum(String),
    /// REDUCE AVG 1 @field。
    Avg(String),
    /// REDUCE MIN 1 @field。
    Min(String),
    /// REDUCE MAX 1 @field。
    Max(String),
    /// REDUCE COUNT_DISTINCT 1 @field(去重计数)。
    CountDistinct(String),
    /// REDUCE STDDEV 1 @field(标准差)。
    StdDev(String),
    /// REDUCE TOLIST 1 @field(聚合成列表)。
    ToList(String),
    /// `REDUCE QUANTILE 2 @field q`（分位数 `q∈[0,1]`）。
    Quantile(String, f64),
    /// REDUCE FIRST_VALUE 1 @field(组内首值)。
    FirstValue(String),
}

/// FT.AGGREGATE 构建器，覆盖 LOAD/GROUPBY/REDUCE/APPLY/FILTER/SORTBY/LIMIT。
/// 与 Query 组合时，Query 负责过滤表达式，
/// Aggregate 出聚合管道——`actuator.aggregate(&q, &agg)`。
#[derive(Debug, Clone, Default)]
pub struct Aggregate {
    load: Vec<String>, // LOAD 字段（从文档加载到管道）
    group_by: Vec<String>,
    reducers: Vec<(Reducer, String)>, // (reducer, 输出别名)
    applies: Vec<(String, String)>,   // APPLY 表达式和输出别名
    filters: Vec<String>,             // FILTER 表达式
    sort_by: Vec<(String, bool)>,     // 多字段 SORTBY:[(别名, desc)] —— 可指向 reducer/APPLY 输出
    limit: Option<(usize, usize)>,
}

impl Aggregate {
    /// 业务作用：创建空的聚合管道构造器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：从文档加载字段进入聚合管道，供 APPLY/FILTER/SORTBY 使用。
    ///
    /// # 参数
    /// - `fields`: 需要加载到聚合管道的 `DocMeta` 字段别名列表。
    pub fn load(mut self, fields: &[&str]) -> Self {
        self.load.extend(fields.iter().map(|s| s.to_string()));
        self
    }

    /// 业务作用：计算 APPLY 表达式并命名为输出列（例如 `apply("@sum / @n", "avg")`）。**表达式由可信调用方
    /// 提供**(RediSearch 表达式语法,绕过转义;同 Raw 的信任边界)。
    ///
    /// # 参数
    /// - `expr`: RediSearch 聚合表达式,由可信调用方负责保证语法和注入边界。
    /// - `as_alias`: 表达式输出列名,后续 FILTER/SORTBY 或结果读取会使用该别名。
    pub fn apply(mut self, expr: &str, as_alias: &str) -> Self {
        self.applies.push((expr.into(), as_alias.into()));
        self
    }

    /// 业务作用：按 FILTER 表达式过滤聚合行（例如 `filter("@count > 10")`）。**表达式由可信调用方提供**。
    ///
    /// # 参数
    /// - `expr`: RediSearch 聚合过滤表达式,直接拼入 FT.AGGREGATE 参数。
    pub fn filter(mut self, expr: &str) -> Self {
        self.filters.push(expr.into());
        self
    }

    /// 业务作用：GROUPBY 字段(meta 别名;render 时校验存在)。可多次调用。
    ///
    /// # 参数
    /// - `field`: 用作分组键的 `DocMeta` 字段别名或字段名。
    pub fn group_by(mut self, field: &str) -> Self {
        self.group_by.push(field.into());
        self
    }

    /// 业务作用：REDUCE + 输出别名(查询结果行里的列名)。
    ///
    /// # 参数
    /// - `r`: 聚合 reducer,定义 COUNT/SUM/AVG 等聚合动作及其输入字段。
    /// - `as_alias`: reducer 输出列名,查询结果和后续 SORTBY 会使用该别名。
    pub fn reduce(mut self, r: Reducer, as_alias: &str) -> Self {
        self.reducers.push((r, as_alias.into()));
        self
    }

    /// 业务作用：聚合结果降序排序(别名 = GROUPBY 字段 / reducer 输出 / APPLY 输出)。**可多次调用 = 多字段 SORTBY**
    /// (按调用顺序 `@f1 DIR1 @f2 DIR2 ...`)。
    ///
    /// # 参数
    /// - `alias`: GROUPBY 字段、reducer 输出或 APPLY 输出的列名。
    pub fn sort_desc(mut self, alias: &str) -> Self {
        self.sort_by.push((alias.into(), true));
        self
    }

    /// 业务作用：聚合结果升序排序(别名 = GROUPBY 字段 / reducer 输出 / APPLY 输出)。
    ///
    /// # 参数
    /// - `alias`: GROUPBY 字段、reducer 输出或 APPLY 输出的列名。
    pub fn sort_asc(mut self, alias: &str) -> Self {
        self.sort_by.push((alias.into(), false));
        self
    }

    /// 业务作用：设置 FT.AGGREGATE 输出窗口。
    ///
    /// # 参数
    /// - `offset`: 聚合结果起始偏移量。
    /// - `limit`: 聚合结果最大返回行数。
    pub fn limit(mut self, offset: usize, limit: usize) -> Self {
        self.limit = Some((offset, limit));
        self
    }

    /// 业务作用：渲染 FT.AGGREGATE 的管道参数(index 与查询表达式由 actuator 拼前缀)。
    /// GROUPBY/REDUCE 引用的字段经 meta 校验(输出别名除外)。
    ///
    /// # 参数
    /// - `meta`: 当前文档类型的元数据,用于校验 LOAD/GROUPBY/REDUCE 引用的字段。
    pub fn render_args(&self, meta: &DocMeta) -> Result<Vec<String>> {
        let mut args = Vec::new();
        // LOAD 必须最先把文档字段载入管道；字段经 meta 校验后渲染为 `@alias`。
        if !self.load.is_empty() {
            args.push("LOAD".into());
            args.push(self.load.len().to_string());
            for f in &self.load {
                let fm = require_field(meta, f, "LOAD")?;
                args.push(format!("@{}", fm.alias_or_name()));
            }
        }
        //有 reducers 但无 GROUPBY 字段时,**必须发 `GROUPBY 0`(全局聚合)**——
        // 否则整块被跳过、reducers 被静默丢弃(查询无 REDUCE 列)。`GROUPBY 0` = 对全集做一次聚合。
        if !self.group_by.is_empty() || !self.reducers.is_empty() {
            args.push("GROUPBY".into());
            args.push(self.group_by.len().to_string()); // 0 = 全局聚合
            for g in &self.group_by {
                let f = require_field(meta, g, "GROUPBY")?;
                args.push(format!("@{}", f.alias_or_name()));
            }
            for (r, alias) in &self.reducers {
                args.push("REDUCE".into());
                match r {
                    Reducer::Count => {
                        args.push("COUNT".into());
                        args.push("0".into());
                    }
                    // QUANTILE:2 参(@field + q)
                    Reducer::Quantile(field, q) => {
                        if !(0.0..=1.0).contains(q) {
                            return Err(NasaRedisError::Config(format!(
                                "QUANTILE q 必须 ∈ [0,1],得到 {q}"
                            )));
                        }
                        args.push("QUANTILE".into());
                        args.push("2".into());
                        let f = require_field(meta, field, "REDUCE")?;
                        args.push(format!("@{}", f.alias_or_name()));
                        args.push(q.to_string());
                    }
                    // 单字段 reducer:KEYWORD 1 @field
                    _ => {
                        let (kw, field) = match r {
                            Reducer::Sum(f) => ("SUM", f),
                            Reducer::Avg(f) => ("AVG", f),
                            Reducer::Min(f) => ("MIN", f),
                            Reducer::Max(f) => ("MAX", f),
                            Reducer::CountDistinct(f) => ("COUNT_DISTINCT", f),
                            Reducer::StdDev(f) => ("STDDEV", f),
                            Reducer::ToList(f) => ("TOLIST", f),
                            Reducer::FirstValue(f) => ("FIRST_VALUE", f),
                            Reducer::Count | Reducer::Quantile(..) => unreachable!(),
                        };
                        args.push(kw.into());
                        args.push("1".into());
                        let fm = require_field(meta, field, "REDUCE")?;
                        args.push(format!("@{}", fm.alias_or_name()));
                    }
                }
                args.push("AS".into());
                args.push(alias.clone());
            }
        }
        // APPLY 位于 GROUPBY/REDUCE 后，可继续计算 reducer 输出；随后用 FILTER 过滤结果行。
        // 表达式由可信调用方提供(RediSearch 表达式语法),原样下发。
        for (expr, alias) in &self.applies {
            //空表达式/别名会拼成 Redis 语法错(对照 原实现 isBlank 抛错)——提前 fail-fast。
            if expr.trim().is_empty() || alias.trim().is_empty() {
                return Err(NasaRedisError::Config(format!(
                    "Aggregate APPLY 表达式/别名不能为空(expr={expr:?}, alias={alias:?})"
                )));
            }
            args.push("APPLY".into());
            args.push(expr.clone());
            args.push("AS".into());
            args.push(alias.clone());
        }
        for expr in &self.filters {
            if expr.trim().is_empty() {
                return Err(NasaRedisError::Config(
                    "Aggregate FILTER 表达式不能为空".into(),
                ));
            }
            args.push("FILTER".into());
            args.push(expr.clone());
        }
        if !self.sort_by.is_empty() {
            //校验每个 sort alias ∈ {GROUPBY 输出列} ∪ {reducer 输出} ∪ {APPLY 输出}——
            // 拼错的 alias 在部分 RediSearch 版本**静默丢序**(违规范3),提前 Err 暴露。
            let mut valid: Vec<String> = self.reducers.iter().map(|(_, a)| a.clone()).collect();
            valid.extend(self.applies.iter().map(|(_, a)| a.clone()));
            for g in &self.group_by {
                valid.push(
                    require_field(meta, g, "SORTBY")?
                        .alias_or_name()
                        .to_string(),
                );
            }
            //LOAD 的字段也进入管道、可被 SORTBY 引用(此前漏算 → `load(["price"])
            // .sort_desc("price")` 被误拒,比 原实现 严)。LOAD 字段名原样可作 SORTBY 别名。
            valid.extend(self.load.iter().cloned());
            // 多字段 SORTBY:`SORTBY {2*n} @f1 DIR1 @f2 DIR2 ...`
            args.push("SORTBY".into());
            args.push((self.sort_by.len() * 2).to_string());
            for (alias, desc) in &self.sort_by {
                if !valid.iter().any(|v| v == alias) {
                    return Err(NasaRedisError::Config(format!(
                        "Aggregate SORTBY 别名 \"{alias}\" 不在 GROUPBY 输出列/reducer/APPLY 别名中(可用: {valid:?})"
                    )));
                }
                args.push(format!("@{alias}"));
                args.push(if *desc { "DESC".into() } else { "ASC".into() });
            }
        }
        if let Some((offset, limit)) = self.limit {
            args.push("LIMIT".into());
            args.push(offset.to_string());
            args.push(limit.to_string());
        }
        Ok(args)
    }
}

/// 业务作用：JSONPath 字符串字面量:**按内容选包裹引号**。
///
/// RedisJSON JSONPath **不处理包裹引号内的 `\` 转义**(`\"` 不认、`\'` 同样不认)——所以不能靠转义引号,
/// 只能选一个**值里不出现的**引号来包裹:
///   · 值不含 `'` → **单引号**包裹(默认);
///   · 含 `'` 不含 `"` → **双引号**包裹(实测 `@.cat=="it's"` 命中);
///   · 两者都含 → 引号字面量无法表达 → **fail-fast**(引导改 FT 索引模式或避免该值);
/// 包裹引号选定后值里必不含它,故**值原样写入、不做任何 `\` 转义**。控制字符(<0x20)会让 parser 报
/// "unknown error" 且无法转义嵌入 → 一并 fail-fast。
/// (此前 单引号 + `\'` 转义只是把唯一不可匹配字符从 `"` 换成 `'`,实测含 `'` 的值仍漏,未根治。)
pub(crate) fn json_path_literal(s: &str) -> Result<String> {
    if let Some(c) = s.chars().find(|c| (*c as u32) < 0x20) {
        return Err(NasaRedisError::Config(format!(
            "ARRAY JSONPath 值含控制字符 U+{:04X},无法在 JSONPath 引号字面量中表达(改用 FT 索引模式)",
            c as u32
        )));
    }
    let quote = match (s.contains('\''), s.contains('"')) {
        (true, true) => {
            return Err(NasaRedisError::Config(
                "ARRAY JSONPath 值同时含 ' 和 \":RedisJSON 不认引号内 `\\` 转义,无法用引号字面量表达——改 FT 索引模式或避免该值".into(),
            ))
        }
        (true, false) => '"', // 含单引号 → 双引号包裹
        _ => '\'',            // 否则单引号
    };
    Ok(format!("{quote}{s}{quote}"))
}

/// 业务作用：Tag 值转义:RediSearch 的 tag 语法字符(,.<>{}[]"':;!@#$%^&*()-+=~| 与空格)
/// 一律反斜杠转义——值来自业务数据,不转义会破坏查询语法/被注入。
///
/// # 参数
/// - `v`: 待转换的值。
fn escape_tag(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 4);
    for ch in v.chars() {
        // 白名单(对齐 原实现 `escapeQueryChars`;A-5 改回白名单 +-P1g 收窄):只保留
        // `[A-Za-z0-9_]` + **CJK 表意 `一-龥`(U+4E00..=U+9FA5)** 原样;其余**一切**(标点、空格、
        // 反斜杠、控制字符 tab/换行、**以及假名/韩文/emoji 等其它非 ASCII**)一律 `\` 前缀。原实现 白名单
        // 正是这个范围——Rust 若多保留假名/emoji 不转义,跨语言同库 tag 查询会因转义形态不同而分叉。
        let keep =
            ch.is_ascii_alphanumeric() || ch == '_' || ('\u{4e00}'..='\u{9fa5}').contains(&ch);
        if !keep {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// 业务作用：TEXT 字面量"转义"。
///
/// **关键:标点/结构符一律转成空格,而非反斜杠转义。** 因为 RediSearch 默认分词把标点当**分隔符剥离**
/// (`(beta)` 索引成 token `beta`、`C++` 索引成 `c`),若把标点反斜杠转义查**字面** token(`\(beta\)`、
/// `C\+\+`)在倒排里根本不存在 → **静默漏匹配**(实测 `@name:(alpha \(beta\))`=0、`@name:(C\+\+)`=0 vs
/// 标点→空格 `(alpha  beta )`/`(C  )`=命中)。转空格既对齐"标点=词边界"的默认分词语义,又同样防注入
/// (空格不构成任何查询算子,无法越出 `@field:(...)` 组)。保留:`[A-Za-z0-9_]` + CJK + 空格原样
/// (`_`/CJK 是 RediSearch 默认词字符,不分隔)。短语精确匹配走 text_phrase;TAG 仍用 escape_tag(空格是值的一部分)。
///
/// # 参数
/// - `v`: 待转换的值。
fn escape_text(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        let keep = ch == ' '
            || ch.is_ascii_alphanumeric()
            || ch == '_'
            || ('\u{4e00}'..='\u{9fa5}').contains(&ch);
        out.push(if keep { ch } else { ' ' });
    }
    out
}
