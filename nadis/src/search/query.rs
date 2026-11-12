// 类型化查询 AST 与渲染器。
// 字段与类型在 render(meta) 时校验，Tag 值统一转义，避免错误命令或查询注入。
// 基础查询按 AND 组合，OR 可使用聚合表达式；RawQuery 只接收可信调用方提供的表达式。
// render 通过共享引用读取 Query，同一查询值可以重复渲染。

use super::{DocMeta, FieldType};
use crate::error::{NasaRedisError, Result};

/// 用于数值开区间、闭区间与无界范围的比较算子。
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
    /// 后缀匹配：TEXT 使用 `@f:(*term)`，TAG 使用 `@f:{*value}`；字段须启用 WITHSUFFIXTRIE。
    TextSuffix {
        /// 字段别名或字段名。
        field: String,
        /// 词项后缀。
        suffix: String,
    },
    /// 包含匹配：TEXT 使用 `@f:(*term*)`，TAG 使用 `@f:{*value*}`；字段须启用 WITHSUFFIXTRIE。
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
    /// 业务作用：将地理半径单位转换为 RediSearch 查询接受的参数词。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回 m、km、mi 或 ft 的静态字符串，不做单位换算。
    fn as_str(&self) -> &'static str {
        match self {
            GeoUnit::M => "m",
            GeoUnit::Km => "km",
            GeoUnit::Mi => "mi",
            GeoUnit::Ft => "ft",
        }
    }
}

/// 支持 AND 组合、单字段排序与分页的查询。
#[derive(Debug, Clone)]
pub struct Query {
    conds: Vec<Cond>,
    sort: Option<(String, bool)>, // 字段别名与是否降序。
    offset: usize,
    limit: usize,
    /// 调用方是否显式设置过 limit——ARRAY 模式(JSONPath filter)不支持分页,
    /// 必须区分"默认 10"与"业务真要分页"。
    limit_explicit: bool,
}

impl Default for Query {
    /// 业务作用：让通用 Default 构造与显式 new 使用同一查询窗口和未分页状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无过滤、无排序、offset=0、limit=10 且没有显式分页标记的查询，不访问 Redis。
    fn default() -> Self {
        Self::new()
    }
}

impl Query {
    /// 业务作用：建立无过滤条件且尚未显式分页的查询。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回 offset=0、limit=10 且无排序的查询；构造不访问 Redis。
    pub fn new() -> Self {
        Self {
            conds: Vec::new(),
            sort: None,
            offset: 0,
            limit: 10,
            limit_explicit: false,
        }
    }

    /// 业务作用：追加离散 TAG 值的精确匹配条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `value`：待匹配的 TAG 字面值。
    ///
    /// 返回：返回保存条件的查询；字段和类型校验以及字面量处理发生在渲染时。
    pub fn tag_eq(mut self, field: &str, value: &str) -> Self {
        self.conds.push(Cond::TagEq {
            field: field.into(),
            value: value.into(),
        });
        self
    }

    /// 业务作用：追加同一 TAG 字段的多选一条件，并与已有条件按 AND 组合。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `values`：任一命中即可的 TAG 字面值集合；此处原样保存，包括空集合。
    ///
    /// 返回：返回追加条件的查询，不访问 Redis；空集合在 FT 渲染时成为空集哨兵，在 ARRAY 渲染时成为零宽切片。
    pub fn tag_in(mut self, field: &str, values: &[&str]) -> Self {
        self.conds.push(Cond::TagIn {
            field: field.into(),
            values: values.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// 业务作用：追加包含两端的数值区间条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `min`：闭区间下界，渲染时必须有限且不大于 max。
    /// - `max`：闭区间上界，渲染时必须有限。
    ///
    /// 返回：返回保存边界的查询；非法数值在渲染时返回错误。
    pub fn between(mut self, field: &str, min: f64, max: f64) -> Self {
        self.conds.push(Cond::NumBetween {
            field: field.into(),
            min,
            max,
        });
        self
    }

    /// 业务作用：保存数值比较条件，供 FT 区间与 JSONPath 比较两种渲染使用。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `op`：大于、大于等于、小于、小于等于或等于的比较关系。
    /// - `value`：有限数值边界，渲染时校验。
    ///
    /// 返回：返回追加条件的查询；FT 的无界一侧使用 inf，不以有限极值模拟。
    fn num_cmp(mut self, field: &str, op: NumOp, value: f64) -> Self {
        self.conds.push(Cond::NumCmp {
            field: field.into(),
            op,
            value,
        });
        self
    }

    /// 业务作用：追加数值严格大于条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `value`：不包含在结果内的下界。
    ///
    /// 返回：返回包含开下界条件的查询，字段和有限性留给渲染校验。
    pub fn gt(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Gt, value)
    }

    /// 业务作用：追加数值大于或等于条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `value`：包含在结果内的下界。
    ///
    /// 返回：返回包含闭下界条件的查询，字段和有限性留给渲染校验。
    pub fn gte(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Gte, value)
    }

    /// 业务作用：追加数值严格小于条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `value`：不包含在结果内的上界。
    ///
    /// 返回：返回包含开上界条件的查询，字段和有限性留给渲染校验。
    pub fn lt(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Lt, value)
    }

    /// 业务作用：追加数值小于或等于条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `value`：包含在结果内的上界。
    ///
    /// 返回：返回包含闭上界条件的查询，字段和有限性留给渲染校验。
    pub fn lte(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Lte, value)
    }

    /// 业务作用：追加数值相等条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `value`：需要精确比较的有限数值。
    ///
    /// 返回：返回包含相等条件的查询，渲染时按数值字段处理。
    pub fn num_eq(self, field: &str, value: f64) -> Self {
        self.num_cmp(field, NumOp::Eq, value)
    }

    /// 业务作用：追加 TEXT 词项条件，隔离 ASCII 查询结构字符并保留 Unicode 原文。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `text`：输入文本；渲染保留非 ASCII 字符以及 ASCII 字母数字、下划线和空格。
    ///
    /// 返回：返回保存文本的查询；其它 ASCII 字符在渲染时转为空格，不把输入当作原始查询语法。
    pub fn text_match(mut self, field: &str, text: &str) -> Self {
        self.conds.push(Cond::TextMatch {
            field: field.into(),
            text: text.into(),
        });
        self
    }

    /// 业务作用：追加全文前缀条件，在归一化文本末尾附加通配符。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `prefix`：待归一化的前缀文本，不直接作为查询表达式。
    ///
    /// 返回：返回保存条件的查询；空词项在 FT 渲染时产生匹配空集哨兵。
    pub fn text_prefix(mut self, field: &str, prefix: &str) -> Self {
        self.conds.push(Cond::TextPrefix {
            field: field.into(),
            prefix: prefix.into(),
        });
        self
    }

    /// 业务作用：追加后缀条件，在渲染阶段要求字段声明后缀索引能力。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `suffix`：TEXT 词项或完整 TAG 值需要匹配的后缀。
    ///
    /// 返回：返回保存条件的查询；FT 渲染要求 WITHSUFFIXTRIE，TEXT 按词项归一化，TAG 按完整字面值转义并使用花括号语法；ARRAY 渲染拒绝此条件。
    pub fn text_suffix(mut self, field: &str, suffix: &str) -> Self {
        self.conds.push(Cond::TextSuffix {
            field: field.into(),
            suffix: suffix.into(),
        });
        self
    }

    /// 业务作用：追加包含条件，在渲染阶段要求字段声明后缀索引能力。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `infix`：TEXT 词项或完整 TAG 值需要包含的片段。
    ///
    /// 返回：返回保存条件的查询；FT 渲染要求 WITHSUFFIXTRIE，TEXT 按词项归一化，TAG 按完整字面值转义并使用花括号语法；ARRAY 渲染拒绝此条件。
    pub fn text_contains(mut self, field: &str, infix: &str) -> Self {
        self.conds.push(Cond::TextContains {
            field: field.into(),
            infix: infix.into(),
        });
        self
    }

    /// 业务作用：追加全文模糊条件，并在渲染时约束 Levenshtein 距离。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `term`：待归一化的词项。
    /// - `distance`：期望编辑距离；渲染时收敛到 1 到 3。
    ///
    /// 返回：返回保存原输入的查询；渲染使用成对百分号表达有效距离。
    pub fn text_fuzzy(mut self, field: &str, term: &str, distance: u8) -> Self {
        self.conds.push(Cond::TextFuzzy {
            field: field.into(),
            term: term.into(),
            distance,
        });
        self
    }

    /// 业务作用：追加保留词序的全文短语条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `phrase`：短语文本；渲染时转义双引号和反斜杠，保留其它字符。
    ///
    /// 返回：返回保存短语的查询；字段必须在 FT 渲染时通过 TEXT 类型核对。
    pub fn text_phrase(mut self, field: &str, phrase: &str) -> Self {
        self.conds.push(Cond::TextPhrase {
            field: field.into(),
            phrase: phrase.into(),
        });
        self
    }

    /// 业务作用：追加圆形地理半径条件。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    /// - `lon`：圆心经度，渲染时检查有限性。
    /// - `lat`：圆心纬度，渲染时检查有限性。
    /// - `radius`：半径，渲染时必须是非负有限数。
    /// - `unit`：半径单位，不自动换算数值。
    ///
    /// 返回：返回保存条件的查询；当前构造不校验坐标地理范围，FT 渲染核对 GEO 类型与数值有限性。
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

    /// 业务作用：接纳可信调用方提供的原始 FT 查询片段。
    ///
    /// 参数说明：
    /// - `expr`：完整查询片段，调用方负责语法和授权边界，不接受不可信原始输入。
    ///
    /// 返回：返回保存原文的查询；FT 渲染仅加外层括号，不校验或转义片段，ARRAY 渲染拒绝。
    pub fn raw(mut self, expr: &str) -> Self {
        self.conds.push(Cond::Raw(expr.into()));
        self
    }

    /// 业务作用：设置 FT.SEARCH 的单字段降序规则。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    ///
    /// 返回：返回更新排序的查询，替换此前排序；render_args 校验字段的 SORTABLE 声明。
    pub fn sort_desc(mut self, field: &str) -> Self {
        self.sort = Some((field.into(), true));
        self
    }

    /// 业务作用：设置 FT.SEARCH 的单字段升序规则。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    ///
    /// 返回：返回更新排序的查询，替换此前排序；render_args 校验字段的 SORTABLE 声明。
    pub fn sort_asc(mut self, field: &str) -> Self {
        self.sort = Some((field.into(), false));
        self
    }

    /// 业务作用：设置 FT.SEARCH 查询窗口，并记录调用方确实要求分页。
    ///
    /// 参数说明：
    /// - `offset`：结果起始偏移。
    /// - `limit`：本次最多返回的条数，零原样保留。
    ///
    /// 返回：返回保存窗口且分页标记为真的查询；ARRAY 渲染会拒绝显式分页。
    pub fn limit(mut self, offset: usize, limit: usize) -> Self {
        self.offset = offset;
        self.limit = limit;
        self.limit_explicit = true;
        self
    }

    /// 业务作用：区分调用方明确设置分页与构造器保留的缺省窗口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回是否调用过 limit，供 ARRAY 拒绝分页及 find_keys 选择窗口策略。
    pub fn is_limit_explicit(&self) -> bool {
        self.limit_explicit
    }

    /// 业务作用：读取查询当前保存的分页偏移。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回构造或最后一次 limit 调用保存的 offset，不读取 Redis。
    pub fn effective_offset(&self) -> usize {
        self.offset
    }

    /// 业务作用：读取查询当前保存的分页上限。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回当前 limit；未显式分页时 find_keys 使用其独立的窗口策略。
    pub fn effective_limit(&self) -> usize {
        self.limit
    }

    /// 业务作用：将过滤条件按 AND 组合为 FT 表达式，在各条件渲染时核对字段和可支持的输入。
    ///
    /// 参数说明：
    /// - `meta`：用于定位字段及核对类型、排序与后缀能力的文档元数据。
    ///
    /// 返回：无条件时返回 *；普通条件返回表达式，已遍历条件不合法时返回错误。空 TAG 候选集合或 TEXT 归一化后无词项会提前返回空串哨兵，执行器应按空集处理而不发 FT 查询。排序和分页由 render_args 单独校验；Raw 原文由调用方负责。
    pub fn render(&self, meta: &DocMeta) -> Result<String> {
        if self.conds.is_empty() {
            return Ok("*".into()); // 全量(配 limit 用)
        }
        let mut parts = Vec::with_capacity(self.conds.len());
        for c in &self.conds {
            parts.push(match c {
                Cond::TagEq {
                    field,
                    value,
                } => {
                    let f = require_field(meta, field, "TagEq")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    format!("@{}:{{{}}}", f.alias_or_name(), escape_tag(value))
                }
                Cond::TagIn {
                    field,
                    values,
                } => {
                    let f = require_field(meta, field, "TagIn")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    // 空候选集合不可能命中；返回空集哨兵，不能把空花括号交给 FT 语法解析。
                    if values.is_empty() {
                        return Ok(String::new());
                    }
                    let alts: Vec<String> = values.iter().map(|v| escape_tag(v)).collect();
                    format!("@{}:{{{}}}", f.alias_or_name(), alts.join("|"))
                }
                Cond::NumBetween {
                    field,
                    min,
                    max,
                } => {
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
                Cond::NumCmp {
                    field,
                    op,
                    value,
                } => {
                    let f = require_field(meta, field, "NumCmp")?;
                    require_type(f, matches!(f.ftype, FieldType::Numeric { .. }), "Numeric")?;
                    // 非有限数不能作为数值边界交给查询语法解释。
                    if !value.is_finite() {
                        return Err(NasaRedisError::Config(format!(
                            "NumCmp 字段 \"{field}\" 的值必须有限(value={value})"
                        )));
                    }
                    // FT NUMERIC 使用 `(` 表示开区间、`±inf` 表示无界，避免有限值截断范围。
                    let range = match op {
                        NumOp::Gt => format!("[({value} +inf]"),
                        NumOp::Gte => format!("[{value} +inf]"),
                        NumOp::Lt => format!("[-inf ({value}]"),
                        NumOp::Lte => format!("[-inf {value}]"),
                        NumOp::Eq => format!("[{value} {value}]"),
                    };
                    format!("@{}:{}", f.alias_or_name(), range)
                }
                Cond::TextMatch {
                    field,
                    text,
                } => {
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
                Cond::TextPrefix {
                    field,
                    prefix,
                } => {
                    let f = require_field(meta, field, "TextPrefix")?;
                    require_type(f, matches!(f.ftype, FieldType::Text { .. }), "Text")?;
                    let esc = escape_text(prefix);
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    // term 转义(限字面),`*` 作前缀语法附在转义后(不被转义)
                    format!("@{}:({}*)", f.alias_or_name(), esc)
                }
                Cond::TextSuffix {
                    field,
                    suffix,
                } => {
                    let f = require_field(meta, field, "TextSuffix")?;
                    require_suffix_trie(f, "TextSuffix")?;
                    let is_tag = matches!(f.ftype, FieldType::Tag { .. });
                    // TAG 使用独立语法并保留完整字面值，不能按全文分词改写标点或空格。
                    let esc = if is_tag {
                        escape_tag(suffix)
                    } else {
                        escape_text(suffix)
                    };
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    if is_tag {
                        format!("@{}:{{*{}}}", f.alias_or_name(), esc)
                    } else {
                        format!("@{}:(*{})", f.alias_or_name(), esc)
                    }
                }
                Cond::TextContains {
                    field,
                    infix,
                } => {
                    let f = require_field(meta, field, "TextContains")?;
                    require_suffix_trie(f, "TextContains")?;
                    let is_tag = matches!(f.ftype, FieldType::Tag { .. });
                    // 通配符只由框架加在字面值两端，输入里的 TAG 语法字符仍须转义。
                    let esc = if is_tag {
                        escape_tag(infix)
                    } else {
                        escape_text(infix)
                    };
                    if esc.trim().is_empty() {
                        return Ok(String::new());
                    }
                    if is_tag {
                        format!("@{}:{{*{}*}}", f.alias_or_name(), esc)
                    } else {
                        format!("@{}:(*{}*)", f.alias_or_name(), esc)
                    }
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
                Cond::TextPhrase {
                    field,
                    phrase,
                } => {
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
                    // 非有限坐标或负半径不能表达有效圆形区域，在生成命令前拒绝。
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
                // 此分支保留可信调用方的查询能力，不把原始片段当作已经转义的业务值。
                Cond::Raw(expr) => format!("({expr})"),
            });
        }
        Ok(parts.join(" ")) // FT.SEARCH 空格 = AND
    }

    /// 业务作用：生成 FT.SEARCH 的单字段排序与分页参数。
    ///
    /// 参数说明：
    /// - `meta`：用于定位字段及核对类型、排序与后缀能力的文档元数据。
    ///
    /// 返回：字段存在且可排序时返回 SORTBY 与 LIMIT 参数，无排序时只返回 LIMIT；排序字段不合法返回错误，不发送命令。
    pub fn render_args(&self, meta: &DocMeta) -> Result<Vec<String>> {
        let mut args = Vec::new();
        if let Some((field, desc)) = &self.sort {
            let f = require_field(meta, field, "sort")?;
            let sortable = matches!(
                f.ftype,
                FieldType::Tag {
                    sortable: true,
                    ..
                } | FieldType::Text {
                    sortable: true,
                    ..
                } | FieldType::Numeric {
                    sortable: true,
                    ..
                }
            );
            // 不能把未声明可排序的字段当作有效排序键，避免查询结果顺序与调用意图不一致。
            if !sortable {
                return Err(NasaRedisError::Config(format!(
                    "字段 \"{field}\" 未声明 SORTABLE,不能排序(metadata 构建期约束)"
                )));
            }
            args.push("SORTBY".into());
            args.push(f.alias_or_name().to_string());
            args.push(if *desc {
                "DESC".into()
            } else {
                "ASC".into()
            });
        }
        args.push("LIMIT".into());
        args.push(self.offset.to_string());
        args.push(self.limit.to_string());
        Ok(args)
    }
}

/// 业务作用：把查询别名绑定到文档声明，拒绝访问未声明字段。
///
/// 参数说明：
/// - `meta`：提供字段定义的文档元数据。
/// - `alias`：查询可见的字段别名，未设置别名时为字段名。
/// - `ctx`：标识本次查询条件或管道阶段的错误上下文。
///
/// 返回：找到字段时返回借用的元数据，否则返回包含上下文及可用别名的配置错误。
fn require_field<'a>(meta: &'a DocMeta, alias: &str, ctx: &str) -> Result<&'a super::FieldMeta> {
    // 未声明的别名不能被当作自由字段表达式拼入查询，必须由当前元数据明确接纳。
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

/// 业务作用：在渲染后缀或中缀条件前核对元数据声明的后缀索引能力。
///
/// 参数说明：
/// - `f`：已定位的字段元数据。
/// - `ctx`：用于说明拒绝来源的条件名称。
///
/// 返回：Text/Tag 且 with_suffix_trie 为真时成功，否则返回配置错误；不读取实际 Redis 索引。
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
    // 元数据未声明后缀能力时拒绝构造依赖该能力的查询，不能静默退化匹配语义。
    if !ok {
        return Err(NasaRedisError::Config(format!(
            "{ctx}: 字段 \"{}\" 需 Text/Tag 且建索引时 WITHSUFFIXTRIE(实际 {:?})",
            f.alias_or_name(),
            f.ftype
        )));
    }
    Ok(())
}

/// 业务作用：根据已计算的类型判定拒绝字段与查询条件不兼容的组合。
///
/// 参数说明：
/// - `f`：已定位的字段元数据，用于错误描述。
/// - `ok`：调用方按条件类型计算的兼容性判定。
/// - `want`：错误中展示的期望字段类型。
///
/// 返回：兼容时成功，否则返回包含字段及预期类型的配置错误。
fn require_type(f: &super::FieldMeta, ok: bool, want: &str) -> Result<()> {
    // 字段与条件的解释规则不兼容时拒绝，避免把数据值按另一种索引类型处理。
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
    /// 业务作用：为 ARRAY 文档生成 TAG 或数值 JSONPath 过滤器，并在本地拒绝不能表达的选项。
    ///
    /// 参数说明：
    /// - `meta`：用于定位字段及核对类型、排序与后缀能力的文档元数据。
    ///
    /// 返回：无条件返回 `$[*]`，空 TAG 候选集合返回不会选择子文档的 `$[0:0]`，其它可表达条件返回过滤路径；排序、显式分页、TEXT/GEO、Raw、非法字段/数值或不支持的字符串字面量返回错误。字段引用按存储 name 或显式 json_path 的单值路径定位，别名只用于查找定义；路径不合法时返回错误，不发送 Redis 命令。
    /// 数值条件的参数为 f64，大整数可能在调用前已经舍入；生成路径沿用 RedisJSON 的数值比较，混合整数与浮点记录可能匹配相邻业务值。该路径也用于条件删除，精确身份定位应使用按 ID 入口或字符串字段。
    pub fn render_json_path_filter(&self, meta: &DocMeta) -> Result<String> {
        // ARRAY 路径不能承诺子文档排序，拒绝选项比忽略排序更能保持调用合同。
        if self.sort.is_some() {
            return Err(NasaRedisError::Config(
                "ARRAY 模式不支持 sort(子文档级排序 RedisJSON 不支持;需排序用 DataType::Json)"
                    .into(),
            ));
        }
        // JSONPath 过滤没有这里的分页能力，显式窗口不能被悄悄丢弃。
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
                    // 查询别名只用于找到定义，实际读取必须绑定存储来源。
                    format!("{}=={}", json_path_field(f)?, json_path_literal(value)?)
                }
                Cond::TagIn { field, values } => {
                    let f = require_field(meta, field, "TagIn")?;
                    require_type(f, matches!(f.ftype, FieldType::Tag { .. }), "Tag")?;
                    // ARRAY 查询和删除都需要有效的空路径；零宽切片不会误选任何子文档。
                    if values.is_empty() {
                        return Ok("$[0:0]".into());
                    }
                    let reference = json_path_field(f)?;
                    let alts: Vec<String> = values
                        .iter()
                        .map(|v| Ok(format!("{reference}=={}", json_path_literal(v)?)))
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
                    let reference = json_path_field(f)?;
                    format!("({reference}>={min} && {reference}<={max})")
                }
                Cond::NumCmp { field, op, value } => {
                    let f = require_field(meta, field, "NumCmp")?;
                    require_type(f, matches!(f.ftype, FieldType::Numeric { .. }), "Numeric")?;
                    if !value.is_finite() {
                        return Err(NasaRedisError::Config(
                            "JSONPath 数值必须有限(NaN/Infinity 非法)".into(),
                        ));
                    }
                    let reference = json_path_field(f)?;
                    match op {
                        NumOp::Gt => format!("({reference}>{value})"),
                        NumOp::Gte => format!("({reference}>={value})"),
                        NumOp::Lt => format!("({reference}<{value})"),
                        NumOp::Lte => format!("({reference}<={value})"),
                        NumOp::Eq => format!("({reference}=={value})"),
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

/// 业务作用：将实际 JSON 属性名编码成单个括号选择器，隔离名称中的路径语法。
///
/// 参数说明：
/// - `name`：需要原样定位的 JSON 对象属性名，不是查询别名或路径表达式。
///
/// 返回：返回带双引号的单属性选择器，反斜杠与双引号按属性名语法转义；含 U+0020 以下控制字符时返回配置错误，不生成可能错配的路径。
pub(super) fn json_path_property(name: &str) -> Result<String> {
    // 控制字符的 JSON 文本转义不等同于路径中的属性名，不能让它静默选择另一个成员。
    if name.chars().any(|ch| ch < '\u{20}') {
        return Err(NasaRedisError::Config(
            "ARRAY JSONPath 属性名不能含控制字符".into(),
        ));
    }
    let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
    Ok(format!("[\"{escaped}\"]"))
}

/// 业务作用：将文档字段的存储来源转换为当前子文档内的单值引用，避免查询别名改变被查询或删除的对象。
///
/// 参数说明：
/// - `field`：已按查询别名定位的字段定义，name 为默认属性名，json_path 为可选显式存储路径。
///
/// 返回：无显式路径时按完整 name 定位属性；显式路径须从 `$` 开始，仅支持点号属性、引号括号属性和有符号 64 位数组下标，返回以 `@` 开始的规范化引用。拒绝多值选择、过滤表达式、控制字符、非法转义及越界下标，不发送 Redis 命令。
fn json_path_field(field: &super::FieldMeta) -> Result<String> {
    let Some(path) = &field.json_path else {
        return Ok(format!("@{}", json_path_property(&field.name)?));
    };
    let invalid = || {
        NasaRedisError::Config(
            "ARRAY 字段 json_path 仅支持从 $ 开始的单值属性或数组下标路径".into(),
        )
    };
    // ARRAY 过滤以子文档为边界，根引用与多值表达式不能直接拼接进删除条件。
    let mut rest = path.strip_prefix('$').ok_or_else(invalid)?;
    if rest.is_empty() || rest.chars().any(|ch| ch < '\u{20}') {
        return Err(invalid());
    }
    let mut reference = String::from("@");
    while !rest.is_empty() {
        if let Some(tail) = rest.strip_prefix('.') {
            rest = tail;
            if rest.starts_with('[') {
                continue;
            }
            let length = rest
                .char_indices()
                .take_while(|(_, ch)| ch.is_alphanumeric() || "-_`~+#%$^/:".contains(*ch))
                .map(|(offset, ch)| offset + ch.len_utf8())
                .last()
                .unwrap_or(0);
            if length == 0 {
                return Err(invalid());
            }
            reference.push_str(&json_path_property(&rest[..length])?);
            rest = &rest[length..];
        } else if let Some(tail) = rest.strip_prefix('[') {
            rest = tail;
            if let Some(quote @ ('\'' | '"')) = rest.chars().next() {
                let mut chars = rest[quote.len_utf8()..].char_indices();
                let mut name = String::new();
                let end = loop {
                    let (offset, ch) = chars.next().ok_or_else(invalid)?;
                    if ch == quote {
                        break quote.len_utf8() + offset + ch.len_utf8();
                    }
                    if ch == '\\' {
                        let (_, escaped) = chars.next().ok_or_else(invalid)?;
                        // 属性名只接受当前包裹引号与反斜杠转义；其它转义不能猜测成不同的属性。
                        if escaped != quote && escaped != '\\' {
                            return Err(invalid());
                        }
                        name.push(escaped);
                    } else {
                        name.push(ch);
                    }
                };
                rest = rest[end..].strip_prefix(']').ok_or_else(invalid)?;
                reference.push_str(&json_path_property(&name)?);
            } else {
                let end = rest.find(']').ok_or_else(invalid)?;
                let raw = &rest[..end];
                let digits = raw.strip_prefix('-').unwrap_or(raw);
                // 只接纳单一下标，不能把逗号、切片或溢出数字变成隐含的多对象选择。
                if digits.is_empty()
                    || !digits.bytes().all(|ch| ch.is_ascii_digit())
                    || (digits.len() > 1 && digits.starts_with('0'))
                {
                    return Err(invalid());
                }
                let index = raw.parse::<i64>().map_err(|_| invalid())?;
                reference.push_str(&format!("[{index}]"));
                rest = &rest[end + 1..];
            }
        } else {
            return Err(invalid());
        }
    }
    Ok(reference)
}

// ───────────────────────── FT.AGGREGATE(最小封装)─────────────────────────

/// 聚合 reducer。
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
    /// 业务作用：建立没有任何聚合管道阶段的构造器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回空 LOAD/GROUPBY/REDUCE/APPLY/FILTER/SORTBY 且未指定 LIMIT 的聚合定义。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：追加从文档加载到聚合管道的字段。
    ///
    /// 参数说明：
    /// - `fields`：DocMeta 公开的字段别名列表，追加顺序保留。
    ///
    /// 返回：返回保存 LOAD 列表的聚合定义，字段存在性在渲染时校验。
    pub fn load(mut self, fields: &[&str]) -> Self {
        self.load.extend(fields.iter().map(|s| s.to_string()));
        self
    }

    /// 业务作用：追加由可信调用方定义的聚合计算列。
    ///
    /// 参数说明：
    /// - `expr`：原样发送的 RediSearch 表达式，调用方负责语法与输入信任边界。
    /// - `as_alias`：计算结果的输出列名。
    ///
    /// 返回：返回保存 APPLY 的定义；渲染拒绝空表达式或空别名，不转义表达式。
    pub fn apply(mut self, expr: &str, as_alias: &str) -> Self {
        self.applies.push((expr.into(), as_alias.into()));
        self
    }

    /// 业务作用：追加由可信调用方定义的聚合行过滤表达式。
    ///
    /// 参数说明：
    /// - `expr`：原样发送的聚合过滤表达式，调用方负责语法与输入信任边界。
    ///
    /// 返回：返回保存 FILTER 的定义；渲染拒绝空表达式，不改写原文。
    pub fn filter(mut self, expr: &str) -> Self {
        self.filters.push(expr.into());
        self
    }

    /// 业务作用：追加聚合分组字段并保留字段调用顺序。
    ///
    /// 参数说明：
    /// - `field`：DocMeta 公开的字段别名；未设置别名时使用字段名。
    ///
    /// 返回：返回保存 GROUPBY 字段的定义，渲染时核对字段存在性。
    pub fn group_by(mut self, field: &str) -> Self {
        self.group_by.push(field.into());
        self
    }

    /// 业务作用：追加聚合计算器及其输出列名。
    ///
    /// 参数说明：
    /// - `r`：聚合动作、输入字段和可选分位数参数。
    /// - `as_alias`：该计算器在结果行中的输出名称。
    ///
    /// 返回：返回保存计算器的定义；无分组字段时渲染 GROUPBY 0，字段引用与分位数范围在渲染时校验。
    pub fn reduce(mut self, r: Reducer, as_alias: &str) -> Self {
        self.reducers.push((r, as_alias.into()));
        self
    }

    /// 业务作用：向聚合结果追加一个降序键，保留多字段优先级。
    ///
    /// 参数说明：
    /// - `alias`：LOAD/GROUPBY、计算器或 APPLY 提供的输出列名。
    ///
    /// 返回：返回追加排序的聚合定义，不替换已有键；渲染时校验列名来源。
    pub fn sort_desc(mut self, alias: &str) -> Self {
        self.sort_by.push((alias.into(), true));
        self
    }

    /// 业务作用：向聚合结果追加一个升序键，保留多字段优先级。
    ///
    /// 参数说明：
    /// - `alias`：LOAD/GROUPBY、计算器或 APPLY 提供的输出列名。
    ///
    /// 返回：返回追加排序的聚合定义，不替换已有键；渲染时校验列名来源。
    pub fn sort_asc(mut self, alias: &str) -> Self {
        self.sort_by.push((alias.into(), false));
        self
    }

    /// 业务作用：设置聚合管道末端的输出窗口。
    ///
    /// 参数说明：
    /// - `offset`：输出结果起始偏移。
    /// - `limit`：最多返回的行数，零原样保留。
    ///
    /// 返回：返回保存 LIMIT 的聚合定义，替换先前窗口，不改变前面阶段的计算范围。
    pub fn limit(mut self, offset: usize, limit: usize) -> Self {
        self.limit = Some((offset, limit));
        self
    }

    /// 业务作用：按固定阶段顺序生成聚合参数，并核对字段引用、分位数和输出列来源。
    ///
    /// 参数说明：
    /// - `meta`：用于核对 LOAD/GROUPBY/REDUCE 字段的元数据。
    ///
    /// 返回：成功返回 LOAD、GROUPBY/REDUCE、APPLY、FILTER、SORTBY、LIMIT 的有序参数；不合法引用、分位数或空表达式返回错误。可信表达式原样保留，不发送命令。
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
                        // 分位值必须处于闭区间，范围判断同时拒绝 NaN，避免下发无定义的计算。
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
            //空表达式/别名会拼成 Redis 语法错——提前 fail-fast。
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
            // 排序只能引用本管道声明的输出列，避免把拼写错误交给 Redis 解释。
            let mut valid: Vec<String> = self.reducers.iter().map(|(_, a)| a.clone()).collect();
            valid.extend(self.applies.iter().map(|(_, a)| a.clone()));
            for g in &self.group_by {
                valid.push(
                    require_field(meta, g, "SORTBY")?
                        .alias_or_name()
                        .to_string(),
                );
            }
            // LOAD 字段进入聚合管道后可按原名被 SORTBY 引用，校验时须保留这些合法别名。
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
                args.push(if *desc {
                    "DESC".into()
                } else {
                    "ASC".into()
                });
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

/// 业务作用：编码 ARRAY 条件和子文档 ID 的字符串比较值，拒绝不能保持字面身份的字符组合。
///
/// 参数说明：
/// - `s`：需要精确匹配的业务字符串或子文档标识，不是属性名。
///
/// 返回：选择原值中不存在的包裹引号并转义反斜杠；同时含两类引号、反斜杠紧接引号或 U+0020 以下控制字符时返回配置错误，不生成可能错配的比较条件。
pub(crate) fn json_path_literal(s: &str) -> Result<String> {
    // 控制字符不能按当前比较值合同安全编码，必须在发出查询或删除前拒绝。
    if s.chars().any(|ch| ch < '\u{20}') {
        return Err(NasaRedisError::Config(
            "ARRAY JSONPath 比较值不能含控制字符".into(),
        ));
    }
    let quote = match (s.contains('\''), s.contains('"')) {
        // 比较值与属性名采用不同的解码路径，不能用属性名转义来表达两类引号。
        (true, true) => {
            return Err(NasaRedisError::Config(
                "ARRAY JSONPath 比较值不支持同时包含单引号和双引号".into(),
            ));
        }
        (true, false) => '"',
        _ => '\'',
    };
    // 反斜杠紧邻引号可能在服务端再次被解码，必须拒绝以免匹配、覆盖或删除到不同身份。
    if s.contains("\\'") || s.contains("\\\"") {
        return Err(NasaRedisError::Config(
            "ARRAY JSONPath 比较值不支持反斜杠紧接引号".into(),
        ));
    }
    let escaped = s.replace('\\', "\\\\");
    Ok(format!("{quote}{escaped}{quote}"))
}

/// 业务作用：把 TAG 数据值与查询结构隔开，在非保留字符前添加反斜杠。
///
/// 参数说明：
/// - `v`：需要按字面值匹配的 TAG 内容。
///
/// 返回：非 ASCII 字符、ASCII 字母数字与下划线原样保留，其余 ASCII 字符加反斜杠；空输入返回空串。此函数不解释索引分隔符或校验服务端支持范围。
fn escape_tag(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 4);
    for ch in v.chars() {
        // TAG 结构符属于 ASCII；非 ASCII 字符原样保留，不能用反斜杠拆开 UTF-8 字面值。
        let keep = !ch.is_ascii() || ch.is_ascii_alphanumeric() || ch == '_';
        if !keep {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// 业务作用：隔离全文输入中的 ASCII 查询算子，同时保留非 ASCII 文字与组合序列。
///
/// 参数说明：
/// - `v`：待归一化的文本。
///
/// 返回：保留非 ASCII 原文以及 ASCII 字母数字、下划线和空格；其余 ASCII 字符转为空格。不做 Unicode 归一化或词干处理，只有空白的结果由调用方按空集处理。
fn escape_text(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        // 查询结构由 ASCII 符号组成；保留其它 Unicode 原文，避免丢失组合符、文字或词内符号。
        let keep = !ch.is_ascii() || ch == ' ' || ch.is_ascii_alphanumeric() || ch == '_';
        out.push(if keep {
            ch
        } else {
            ' '
        });
    }
    out
}
