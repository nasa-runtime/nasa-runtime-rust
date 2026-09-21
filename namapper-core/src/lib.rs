//! 后端中立 Mapper 合同。
//!
//! 本 crate 承载结构化 SQL bind 渲染、分页、排序、缓存、静态方法目录与原子观测合同，
//! 不依赖 SQLx 或具体数据库事务运行时。观测策略支持递归默认、逐叶覆盖和冻结；
//! 数据库完成路径只记录低基数事实，命中通知规则时提交有界队列，不调用通知 provider。
//! 慢指标、慢日志和通知统一比较原始耗时与阈值，相等也命中；连接等待和消费者处理不计入 SQL。
//! 业务主动安装 `Notify` 并启用告警；需要逐条通知时设 `cooldown_ms=0`。缺失实现则忽略，队列
//! 拥塞或下游失败不改变数据库结果；通知协议由业务微服务适配器负责。

#![forbid(unsafe_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

pub use async_trait::async_trait;

mod cache_runtime;
pub use cache_runtime::*;

pub mod observability;

/// 已选择动态分支后的后端中立 SQL 节点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlNode<'a> {
    /// 不含业务 bind 的 SQL 文本，字面量和操作符保持原样。
    Text(&'a str),
    /// 一个 prepared bind。
    Bind,
    /// 连续的列表 bind；零长度会被拒绝。
    BindList(usize),
}

/// PostgreSQL bind 序号渲染器。
///
/// 宏展开代码对动态节点按实际执行顺序共享一个实例，因此 SQL 中 `$n` 与随后产生的 bind 调用具有同一
/// 顺序来源。普通文本从不扫描或转换 `?`，JSON 操作符、字面量、identifier 与注释会保持原样。
#[derive(Debug, Clone, Default)]
pub struct PostgresBindIndex {
    next: usize,
}

impl PostgresBindIndex {
    /// 业务作用: 创建从 `$1` 开始编号的 PostgreSQL bind 渲染器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 尚未消费任何 bind 的渲染状态。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用: 把下一个 PostgreSQL prepared 占位符写入指定 SQL 缓冲区。
    ///
    /// # 参数
    /// - `sql`: 当前结构节点所属的 SQL 输出缓冲区。
    ///
    /// 返回: 新写入占位符的 1-based bind 序号。
    pub fn push_bind(&mut self, sql: &mut String) -> usize {
        self.next += 1;
        sql.push('$');
        sql.push_str(&self.next.to_string());
        self.next
    }

    /// 业务作用: 连续写入 PostgreSQL 列表 bind，占位符序号与同一渲染器的前后节点连续。
    ///
    /// # 参数
    /// - `sql`: 当前 SQL 输出缓冲区。
    /// - `len`: 列表元素数量；零长度会产生非法 `IN ()`，因此明确拒绝。
    ///
    /// 返回: 成功时写入以逗号分隔的 `$n`；空列表返回业务错误。
    pub fn push_bind_list(&mut self, sql: &mut String, len: usize) -> anyhow::Result<()> {
        if len == 0 {
            anyhow::bail!("Mapper IN 列表参数不能为空");
        }
        for index in 0..len {
            if index > 0 {
                sql.push(',');
            }
            self.push_bind(sql);
        }
        Ok(())
    }

    /// 业务作用: 返回当前已经渲染的 prepared bind 数量，用于验证 SQL 与参数列表对齐。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 已分配的最大占位符序号。
    pub fn bind_count(&self) -> usize {
        self.next
    }
}

/// 业务作用: 从已选中的结构化节点渲染 PostgreSQL SQL 与 bind 总数。
///
/// # 参数
/// - `nodes`: 按最终执行顺序排列的文本、标量 bind 与列表 bind 节点。
///
/// 返回: 规范化 SQL 和实际 bind 数量；空列表节点返回错误。
pub fn render_postgres(nodes: &[SqlNode<'_>]) -> anyhow::Result<(String, usize)> {
    let mut sql = String::new();
    let mut binds = PostgresBindIndex::new();
    for node in nodes {
        match node {
            SqlNode::Text(text) => sql.push_str(text),
            SqlNode::Bind => {
                binds.push_bind(&mut sql);
            }
            SqlNode::BindList(len) => binds.push_bind_list(&mut sql, *len)?,
        }
    }
    Ok((normalize_sql_whitespace(&sql), binds.bind_count()))
}

/// Mapper 分页参数，`page_no` 使用 1-based 语义。
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageRequest {
    /// SQL `LIMIT` bind 值。
    pub limit: i64,
    /// SQL `OFFSET` bind 值。
    pub offset: i64,
}

impl PageRequest {
    /// 默认单页最大条数。
    pub const DEFAULT_MAX_PAGE_SIZE: u64 = 1_000;

    /// 业务作用: 创建采用默认最大页大小的分页参数。
    ///
    /// # 参数
    /// - `page_no`: 从 1 开始的业务页码。
    /// - `page_size`: 大于零且不超过 1000 的单页条数。
    ///
    /// 返回: 可直接 bind 的 limit/offset；输入无效或溢出时返回错误。
    pub fn new(page_no: u64, page_size: u64) -> anyhow::Result<Self> {
        Self::with_max_page_size(page_no, page_size, Self::DEFAULT_MAX_PAGE_SIZE)
    }

    /// 业务作用: 使用调用方上限创建 1-based 分页参数。
    ///
    /// # 参数
    /// - `page_no`: 从 1 开始的业务页码。
    /// - `page_size`: 大于零的单页条数。
    /// - `max_page_size`: 调用方允许的最大单页条数。
    ///
    /// 返回: 可直接 bind 的 limit/offset；边界无效或计算溢出时返回错误。
    pub fn with_max_page_size(
        page_no: u64,
        page_size: u64,
        max_page_size: u64,
    ) -> anyhow::Result<Self> {
        if page_no == 0 || page_size == 0 || max_page_size == 0 {
            anyhow::bail!("mapper page number, size, and maximum must be greater than zero");
        }
        if page_size > max_page_size {
            anyhow::bail!("mapper page size exceeds configured maximum");
        }
        let offset = page_no
            .checked_sub(1)
            .and_then(|page| page.checked_mul(page_size))
            .ok_or_else(|| anyhow::anyhow!("mapper page offset overflow"))?;
        Self::from_offset_limit(offset, page_size)
    }

    /// 业务作用: 从已经计算好的 offset/limit 构造分页 bind。
    ///
    /// # 参数
    /// - `offset`: 非负偏移量。
    /// - `limit`: 大于零的读取条数。
    ///
    /// 返回: i64 范围内的分页值；零 limit 或转换溢出时返回错误。
    pub fn from_offset_limit(offset: u64, limit: u64) -> anyhow::Result<Self> {
        if limit == 0 {
            anyhow::bail!("mapper page limit must be greater than zero");
        }
        Ok(Self {
            limit: i64::try_from(limit).map_err(|_| anyhow::anyhow!("mapper limit overflow"))?,
            offset: i64::try_from(offset).map_err(|_| anyhow::anyhow!("mapper offset overflow"))?,
        })
    }
}

/// Mapper 批量操作的借用切片迭代器。
#[derive(Debug)]
pub struct MapperBatchChunks<'a, T> {
    items: &'a [T],
    chunk_size: usize,
    offset: usize,
}

impl<'a, T> Iterator for MapperBatchChunks<'a, T> {
    type Item = &'a [T];

    /// 业务作用: 返回下一段不复制业务对象的批量参数切片。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 尚有元素时返回下一批，耗尽后返回 `None`。
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.items.len() {
            return None;
        }
        let end = self
            .offset
            .saturating_add(self.chunk_size)
            .min(self.items.len());
        let chunk = &self.items[self.offset..end];
        self.offset = end;
        Some(chunk)
    }
}

/// 业务作用: 按固定最大元素数拆分批量参数，不隐式改变事务边界。
///
/// # 参数
/// - `items`: 待处理业务对象切片。
/// - `chunk_size`: 每批最大元素数，必须大于零。
///
/// 返回: 借用原切片的迭代器；零批大小返回错误。
pub fn batch_chunks<T>(items: &[T], chunk_size: usize) -> anyhow::Result<MapperBatchChunks<'_, T>> {
    if chunk_size == 0 {
        anyhow::bail!("mapper batch chunk size must be greater than zero");
    }
    Ok(MapperBatchChunks {
        items,
        chunk_size,
        offset: 0,
    })
}

/// Mapper 动态排序字段白名单。
pub trait MapperOrderField: Copy + Sized + Send + Sync + 'static {
    /// 业务作用: 返回允许写入 `ORDER BY` 的静态字段名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: `column` 或 `alias.column` 形状的业务白名单字段。
    fn mapper_order_field(self) -> &'static str;
}

/// SQL 排序方向。
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderDirection {
    /// 升序。
    Asc,
    /// 降序。
    Desc,
}

impl OrderDirection {
    /// 业务作用: 返回白名单排序方向对应的 SQL 关键字。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: `ASC` 或 `DESC` 静态文本。
    fn sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

/// 单个白名单排序项。
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrderBy<F: MapperOrderField> {
    field: F,
    direction: OrderDirection,
}

impl<F: MapperOrderField> OrderBy<F> {
    /// 业务作用: 创建白名单字段的升序排序项。
    ///
    /// # 参数
    /// - `field`: 业务定义的静态字段枚举。
    ///
    /// 返回: 可由 Mapper 渲染的升序项。
    pub fn asc(field: F) -> Self {
        Self {
            field,
            direction: OrderDirection::Asc,
        }
    }

    /// 业务作用: 创建白名单字段的降序排序项。
    ///
    /// # 参数
    /// - `field`: 业务定义的静态字段枚举。
    ///
    /// 返回: 可由 Mapper 渲染的降序项。
    pub fn desc(field: F) -> Self {
        Self {
            field,
            direction: OrderDirection::Desc,
        }
    }

    /// 业务作用: 返回排序项包含的业务字段枚举。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 创建排序项时传入的字段值。
    pub fn field(self) -> F {
        self.field
    }

    /// 业务作用: 返回排序项选择的升降序方向。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: `Asc` 或 `Desc`。
    pub fn direction(self) -> OrderDirection {
        self.direction
    }
}

/// 可由 Mapper 动态标签渲染的排序集合。
pub trait MapperOrderBy {
    /// 业务作用: 向输出写入不含 `ORDER BY` 前缀的安全排序片段。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: 实际写入内容时为 `true`，空可选值为 `false`。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool>;
}

impl<T: MapperOrderBy + ?Sized> MapperOrderBy for &T {
    /// 业务作用: 允许借用值复用原类型的排序渲染合同。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: 底层实现的写入结果。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        (*self).write_mapper_order_by(out)
    }
}

impl<T: MapperOrderBy> MapperOrderBy for Option<T> {
    /// 业务作用: 把 `None` 解释为不追加排序，把 `Some` 交给白名单实现。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: `Some` 的渲染结果或 `false`。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        match self {
            Some(value) => value.write_mapper_order_by(out),
            None => Ok(false),
        }
    }
}

impl<F: MapperOrderField> MapperOrderBy for OrderBy<F> {
    /// 业务作用: 在运行期复验字段形状后渲染一个排序项。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: 字段合法时写入字段和方向；非法实现返回错误。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        let field = self.field.mapper_order_field();
        validate_order_field(field)?;
        out.push_str(field);
        out.push(' ');
        out.push_str(self.direction.sql());
        Ok(true)
    }
}

impl<F: MapperOrderField> MapperOrderBy for [OrderBy<F>] {
    /// 业务作用: 按业务声明顺序渲染多个白名单排序项。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: 空切片为 `false`；非空时以逗号连接并返回 `true`。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        if self.is_empty() {
            return Ok(false);
        }
        for (index, item) in self.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            item.write_mapper_order_by(out)?;
        }
        Ok(true)
    }
}

impl<F: MapperOrderField> MapperOrderBy for Vec<OrderBy<F>> {
    /// 业务作用: 渲染业务常用的可变长度白名单排序列表。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: 委托切片实现得到的写入结果。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        self.as_slice().write_mapper_order_by(out)
    }
}

impl<F: MapperOrderField, const N: usize> MapperOrderBy for [OrderBy<F>; N] {
    /// 业务作用: 渲染固定长度的白名单排序数组。
    ///
    /// # 参数
    /// - `out`: 排序 SQL 输出缓冲区。
    ///
    /// 返回: 委托切片实现得到的写入结果。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        self.as_slice().write_mapper_order_by(out)
    }
}

/// Mapper enum 的稳定 ordinal 合同。
pub trait MapperEnum: Copy + Sized + Send + Sync + 'static {
    /// 业务作用: 返回数据库与 cache 共同保存的稳定整数序号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 业务定义的 ordinal。
    fn ordinal(self) -> i32;

    /// 业务作用: 从数据库或 cache 的 ordinal 还原业务 enum。
    ///
    /// # 参数
    /// - `value`: 外部持久状态读取到的整数序号。
    ///
    /// 返回: 已知序号对应 enum，未知值返回 `None`。
    fn from_ordinal(value: i32) -> Option<Self>;
}

/// 编译期收集的 Mapper 二级缓存元数据。
pub struct MapperCacheMeta {
    /// 缓存 namespace。
    pub key: &'static str,
    /// 当前 Mapper 是否存在默认启用缓存的查询。
    pub has_cached_query: bool,
    /// 清理当前 key 时需要同步清理的 key。
    pub clear_also: &'static [&'static str],
    /// 所列 key 被清理时也需要清理当前 key。
    pub clear_when: &'static [&'static str],
}

/// 所有 Mapper 宏生成的缓存元数据。
#[linkme::distributed_slice]
pub static MAPPER_CACHE_META: [MapperCacheMeta];

/// 一个参与查询缓存身份计算的 prepared bind。
pub struct CacheArg {
    name: &'static str,
    json: Vec<u8>,
    scalar: Option<String>,
}

impl CacheArg {
    /// 业务作用: 把任意可序列化 bind 转换为稳定缓存身份材料。
    ///
    /// # 参数
    /// - `name`: 宏期确认的方法参数名。
    /// - `value`: prepared bind 对应的业务值。
    ///
    /// 返回: 保存 JSON bytes 与可选标量文本的缓存参数；序列化失败返回错误。
    pub fn try_new<T: serde::Serialize + ?Sized>(
        name: &'static str,
        value: &T,
    ) -> anyhow::Result<Self> {
        let value = serde_json::to_value(value)?;
        let scalar = match &value {
            serde_json::Value::Null => Some("null".to_owned()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        };
        Ok(Self {
            name,
            json: serde_json::to_vec(&value)?,
            scalar,
        })
    }
}

/// 业务作用: 使用规范化 SQL 与有序 bind JSON hash 构造稳定查询缓存字段。
///
/// # 参数
/// - `normalized_sql`: 最终发送给数据库的 prepared SQL。
/// - `args`: 按占位符顺序排列的 bind 参数。
///
/// 返回: 不直接暴露长参数值的稳定缓存字段。
pub fn cache_hash_key(normalized_sql: &str, args: &[CacheArg]) -> anyhow::Result<String> {
    use sha2::{Digest as _, Sha256};
    let mut key = format!("sql:{normalized_sql}");
    for arg in args {
        key.push(':');
        key.push_str(&hex::encode(Sha256::digest(&arg.json)));
    }
    Ok(key)
}

/// 业务作用: 使用显式标量参数模板构造缓存字段，并保留最终 SQL 身份。
///
/// # 参数
/// - `normalized_sql`: 最终发送给数据库的 prepared SQL。
/// - `suffix_template`: 只允许 `{param}` 引用标量参数的后缀模板。
/// - `args`: 宏按 bind 顺序收集的参数。
///
/// 返回: 模板全部可解析时返回缓存字段；未知、非标量或未闭合引用返回错误。
pub fn cache_hash_key_with_suffix(
    normalized_sql: &str,
    suffix_template: &str,
    args: &[CacheArg],
) -> anyhow::Result<String> {
    let mut suffix = String::new();
    let mut rest = suffix_template;
    while let Some(start) = rest.find('{') {
        suffix.push_str(&rest[..start]);
        let after_open = &rest[start + 1..];
        let end = after_open
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("mapper cache suffix has an unclosed placeholder"))?;
        let name = &after_open[..end];
        let arg = args
            .iter()
            .find(|arg| arg.name == name)
            .ok_or_else(|| anyhow::anyhow!("mapper cache suffix references an unknown argument"))?;
        suffix.push_str(
            arg.scalar
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("mapper cache suffix argument is not scalar"))?,
        );
        rest = &after_open[end + 1..];
    }
    if rest.contains('}') {
        anyhow::bail!("mapper cache suffix has an unmatched closing delimiter");
    }
    suffix.push_str(rest);
    Ok(format!("sql:{normalized_sql}:{suffix}"))
}

/// Mapper cache miss loader future。
pub type MapperCacheLoadFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + 'a>>;

/// Mapper cache miss 时加载并编码数据库结果的单次回调。
pub type MapperCacheLoader<'a> = Box<dyn FnOnce() -> MapperCacheLoadFuture<'a> + Send + 'a>;

/// Mapper cache 查询结果来源。
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MapperCacheLoadState {
    /// 首次读取命中。
    Hit,
    /// 等待其它 loader 后命中。
    HitAfterWait,
    /// 当前调用从数据库加载并写入。
    Loaded,
}

/// Mapper cache get-or-load 返回值。
#[derive(Debug)]
pub struct MapperCacheLoad {
    /// 编码后的缓存 value。
    pub bytes: Vec<u8>,
    /// 结果来源。
    pub state: MapperCacheLoadState,
}

impl MapperCacheLoad {
    /// 业务作用: 构造首次读取即命中的缓存结果。
    ///
    /// # 参数
    /// - `bytes`: 从 L2 cache 读取的原始 value。
    ///
    /// 返回: 标记为首次命中的缓存结果。
    pub(crate) fn hit(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            state: MapperCacheLoadState::Hit,
        }
    }

    /// 业务作用: 构造等待同 key loader 完成后命中的缓存结果。
    ///
    /// # 参数
    /// - `bytes`: 等待期间由其它调用写入的缓存 value。
    ///
    /// 返回: 标记为等待后命中的缓存结果。
    pub(crate) fn hit_after_wait(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            state: MapperCacheLoadState::HitAfterWait,
        }
    }

    /// 业务作用: 构造当前调用从数据库加载并写入缓存后的结果。
    ///
    /// # 参数
    /// - `bytes`: loader 编码后的业务结果。
    ///
    /// 返回: 标记为当前调用已加载的缓存结果。
    pub(crate) fn loaded(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            state: MapperCacheLoadState::Loaded,
        }
    }
}

/// Mapper 二级缓存合同，可由 Redis、本地 cache 或业务网关实现。
#[async_trait]
pub trait MapperL2Cache: Send + Sync + 'static {
    /// 业务作用: 读取一个 Mapper 查询缓存字段。
    ///
    /// # 参数
    /// - `key`: 包含 driver/datasource 的缓存 namespace。
    /// - `hash_key`: SQL 与 bind 派生的查询字段。
    ///
    /// 返回: 命中 bytes、未命中 `None` 或缓存错误。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>>;

    /// 业务作用: 写入一个已编码 Mapper 查询结果。
    ///
    /// # 参数
    /// - `key`: 缓存 namespace。
    /// - `hash_key`: 查询字段。
    /// - `value`: codec 编码后的业务结果。
    /// - `ttl_ms`: 可选毫秒级 TTL。
    ///
    /// 返回: 缓存确认写入时成功，否则返回实现错误。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()>;

    /// 业务作用: 删除一个查询缓存字段。
    ///
    /// # 参数
    /// - `key`: 缓存 namespace。
    /// - `hash_key`: 需要删除的查询字段。
    ///
    /// 返回: 缓存确认删除时成功。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()>;

    /// 业务作用: 清理一个 Mapper namespace 下的全部查询结果。
    ///
    /// # 参数
    /// - `key`: 需要整体失效的缓存 namespace。
    ///
    /// 返回: 缓存确认清理时成功。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()>;

    /// 业务作用: 在未命中时执行一次数据库 loader 并写回编码结果。
    ///
    /// # 参数
    /// - `key`: 缓存 namespace。
    /// - `hash_key`: 查询字段。
    /// - `ttl_ms`: 写回 TTL。
    /// - `loader`: 仅在 miss 时消费的数据库加载回调。
    ///
    /// 返回: 命中或加载结果及来源；缓存与 loader 错误原样返回。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        if let Some(bytes) = self.get(key, hash_key).await? {
            return Ok(MapperCacheLoad {
                bytes,
                state: MapperCacheLoadState::Hit,
            });
        }
        let bytes = loader().await?;
        self.put(key, hash_key, &bytes, ttl_ms).await?;
        Ok(MapperCacheLoad {
            bytes,
            state: MapperCacheLoadState::Loaded,
        })
    }

    /// 业务作用: 尽力清理多个缓存 namespace，并在全部尝试后返回首个错误。
    ///
    /// # 参数
    /// - `keys`: 需要失效的缓存 namespace 集合。
    ///
    /// 返回: 全部成功时为 `Ok`；否则返回首个实现错误。
    async fn clear_keys(&self, keys: &[String]) -> anyhow::Result<()> {
        let mut first_error = None;
        for key in keys {
            if let Err(error) = self.clear_key(key).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Mapper cache 指标类型。
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MapperMetricKind {
    /// 绕过 cache。
    CacheBypass,
    /// 首次读取命中。
    CacheHit,
    /// 等待其它 loader 后命中。
    CacheHitAfterWait,
    /// 未命中。
    CacheMiss,
    /// 当前调用完成数据库加载。
    CacheLoad,
    /// get-or-load 失败。
    CacheLoadError,
    /// 写入成功。
    CachePut,
    /// 缓存字段构造失败。
    CacheHashKeyError,
    /// 读取失败。
    CacheGetError,
    /// 解码失败。
    CacheDecodeError,
    /// 编码失败。
    CacheEncodeError,
    /// 写入失败。
    CachePutError,
}

/// 一条 Mapper cache 指标事件。
pub struct MapperMetric<'a> {
    /// 事件类型。
    pub kind: MapperMetricKind,
    /// Mapper cache namespace。
    pub mapper_key: &'a str,
    /// 查询字段。
    pub hash_key: Option<&'a str>,
    /// 最终 prepared SQL。
    pub sql: Option<&'a str>,
    /// 稳定补充分类。
    pub detail: Option<&'a str>,
}

/// Mapper 指标出口。
pub trait MapperMetrics: Send + Sync + 'static {
    /// 业务作用: 记录一条非阻塞 Mapper cache 指标。
    ///
    /// # 参数
    /// - `metric`: 宏执行路径产生的结构化事件。
    ///
    /// 返回: 无；实现不得让观测失败改变数据库结果。
    fn record(&self, metric: MapperMetric<'_>);
}

/// Mapper 查询结果缓存 codec。
pub trait MapperCacheCodec: Send + Sync + 'static {
    /// 业务作用: 把查询结果 JSON value 编码为缓存 bytes。
    ///
    /// # 参数
    /// - `value`: serde 转换后的业务结果。
    ///
    /// 返回: 可写入 cache 的 bytes 或编码错误。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>>;

    /// 业务作用: 把缓存 bytes 解码为查询结果 JSON value。
    ///
    /// # 参数
    /// - `bytes`: cache 返回的原始 bytes。
    ///
    /// 返回: 可反序列化为业务类型的 JSON value 或解码错误。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value>;

    /// 业务作用: 指示成功读取的旧格式 value 是否应回写为当前格式。
    ///
    /// # 参数
    /// - `_bytes`: 已成功解码的缓存 bytes。
    ///
    /// 返回: 默认 `false`；迁移 codec 可按格式返回 `true`。
    fn should_rewrite_value(&self, _bytes: &[u8]) -> bool {
        false
    }
}

/// 默认 JSON cache codec。
#[derive(Debug, Default)]
pub struct JsonMapperCacheCodec;

impl MapperCacheCodec for JsonMapperCacheCodec {
    /// 业务作用: 使用 serde JSON 编码缓存 value。
    ///
    /// # 参数
    /// - `value`: Mapper 查询结果 JSON value。
    ///
    /// 返回: JSON bytes 或 serde 错误。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    /// 业务作用: 使用 serde JSON 解码缓存 value。
    ///
    /// # 参数
    /// - `bytes`: cache 保存的 JSON bytes。
    ///
    /// 返回: JSON value 或 serde 错误。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// 强类型 Mapper cache codec。
pub trait MapperTypedCacheCodec<T>: Send + Sync + 'static {
    /// 业务作用: 直接编码具体查询返回类型。
    ///
    /// # 参数
    /// - `value`: 查询结果。
    ///
    /// 返回: cache bytes 或 codec 错误。
    fn encode_typed(&self, value: &T) -> anyhow::Result<Vec<u8>>;

    /// 业务作用: 直接从 cache bytes 解码具体查询返回类型。
    ///
    /// # 参数
    /// - `bytes`: cache 原始 bytes。
    ///
    /// 返回: 业务查询类型或 codec 错误。
    fn decode_typed(&self, bytes: &[u8]) -> anyhow::Result<T>;
}

static DEFAULT_L2_CACHE: OnceLock<Arc<dyn MapperL2Cache>> = OnceLock::new();
static DEFAULT_MAPPER_METRICS: OnceLock<Arc<dyn MapperMetrics>> = OnceLock::new();
static DEFAULT_MAPPER_CACHE_CODEC: OnceLock<Arc<dyn MapperCacheCodec>> = OnceLock::new();

/// 业务作用: 安装当前进程唯一的默认 Mapper 二级缓存。
///
/// # 参数
/// - `cache`: 业务提供的 cache 实现。
///
/// 返回: 首次安装成功；重复安装返回错误。
pub fn set_default_l2_cache(cache: Arc<dyn MapperL2Cache>) -> anyhow::Result<()> {
    DEFAULT_L2_CACHE
        .set(cache)
        .map_err(|_| anyhow::anyhow!("mapper default L2 cache is already installed"))
}

/// 业务作用: 获取进程默认 Mapper 二级缓存的共享引用。
///
/// 参数说明: 无。
///
/// 返回: 已安装 cache 的 `Arc` clone；未安装返回 `None`。
pub fn default_l2_cache() -> Option<Arc<dyn MapperL2Cache>> {
    DEFAULT_L2_CACHE.get().cloned()
}

/// 业务作用: 安装当前进程唯一的 Mapper 指标出口。
///
/// # 参数
/// - `metrics`: 业务观测实现。
///
/// 返回: 首次安装成功；重复安装返回错误。
pub fn set_default_mapper_metrics(metrics: Arc<dyn MapperMetrics>) -> anyhow::Result<()> {
    DEFAULT_MAPPER_METRICS
        .set(metrics)
        .map_err(|_| anyhow::anyhow!("mapper default metrics is already installed"))
}

/// 业务作用: 获取进程默认 Mapper 指标出口的共享引用。
///
/// 参数说明: 无。
///
/// 返回: 已安装指标出口的 `Arc` clone；未安装返回 `None`。
pub fn default_mapper_metrics() -> Option<Arc<dyn MapperMetrics>> {
    DEFAULT_MAPPER_METRICS.get().cloned()
}

/// 业务作用: 安装当前进程唯一的默认 Mapper cache codec。
///
/// # 参数
/// - `codec`: 业务缓存值编解码器。
///
/// 返回: 首次安装成功；重复安装返回错误。
pub fn set_default_mapper_cache_codec(codec: Arc<dyn MapperCacheCodec>) -> anyhow::Result<()> {
    DEFAULT_MAPPER_CACHE_CODEC
        .set(codec)
        .map_err(|_| anyhow::anyhow!("mapper default cache codec is already installed"))
}

/// 业务作用: 获取默认 Mapper cache codec 的共享引用。
///
/// 参数说明: 无。
///
/// 返回: 已安装 codec 的 `Arc` clone；未安装返回 `None`。
pub fn default_mapper_cache_codec() -> Option<Arc<dyn MapperCacheCodec>> {
    DEFAULT_MAPPER_CACHE_CODEC.get().cloned()
}

/// 业务作用: 使用进程默认或 JSON codec 编码 Mapper 查询结果。
///
/// # 参数
/// - `value`: 可序列化查询结果。
///
/// 返回: cache bytes 或序列化/codec 错误。
#[doc(hidden)]
pub fn encode_cache_value<T: serde::Serialize + ?Sized>(value: &T) -> anyhow::Result<Vec<u8>> {
    encode_cache_value_with_codec(value, None)
}

/// 业务作用: 使用方法级、全局或默认 JSON codec 编码查询结果。
///
/// # 参数
/// - `value`: 可序列化查询结果。
/// - `codec`: 方法级 codec，可为空。
///
/// 返回: cache bytes 或序列化/codec 错误。
pub fn encode_cache_value_with_codec<T: serde::Serialize + ?Sized>(
    value: &T,
    codec: Option<&dyn MapperCacheCodec>,
) -> anyhow::Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    match codec.or_else(|| DEFAULT_MAPPER_CACHE_CODEC.get().map(Arc::as_ref)) {
        Some(codec) => codec.encode_value(&value),
        None => JsonMapperCacheCodec.encode_value(&value),
    }
}

/// 业务作用: 使用方法级、全局或默认 JSON codec 解码查询结果。
///
/// # 参数
/// - `bytes`: cache 原始 bytes。
/// - `codec`: 方法级 codec，可为空。
///
/// 返回: 具体查询结果或解码错误。
pub fn decode_cache_value_with_codec<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    codec: Option<&dyn MapperCacheCodec>,
) -> anyhow::Result<T> {
    let value = match codec.or_else(|| DEFAULT_MAPPER_CACHE_CODEC.get().map(Arc::as_ref)) {
        Some(codec) => codec.decode_value(bytes)?,
        None => JsonMapperCacheCodec.decode_value(bytes)?,
    };
    Ok(serde_json::from_value(value)?)
}

/// 业务作用: 使用进程默认或 JSON codec 解码 Mapper 查询结果。
///
/// # 参数
/// - `bytes`: cache 返回的原始 bytes。
///
/// 返回: 具体查询结果或解码错误。
#[doc(hidden)]
pub fn decode_cache_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    decode_cache_value_with_codec(bytes, None)
}

/// 业务作用: 使用强类型 codec 编码查询结果。
///
/// # 参数
/// - `value`: 具体查询结果。
/// - `codec`: 方法级强类型 codec。
///
/// 返回: cache bytes 或 codec 错误。
pub fn encode_typed_cache_value<T, C>(value: &T, codec: &C) -> anyhow::Result<Vec<u8>>
where
    C: MapperTypedCacheCodec<T> + ?Sized,
{
    codec.encode_typed(value)
}

/// 业务作用: 使用强类型 codec 解码查询结果。
///
/// # 参数
/// - `bytes`: cache 原始 bytes。
/// - `codec`: 方法级强类型 codec。
///
/// 返回: 具体查询结果或 codec 错误。
pub fn decode_typed_cache_value<T, C>(bytes: &[u8], codec: &C) -> anyhow::Result<T>
where
    C: MapperTypedCacheCodec<T> + ?Sized,
{
    codec.decode_typed(bytes)
}

/// 业务作用: 判断命中的缓存 value 是否需要由当前 codec 回写。
///
/// # 参数
/// - `bytes`: 已成功解码的缓存 bytes。
/// - `codec`: 方法级 codec，可为空。
///
/// 返回: 方法级或全局 codec 建议回写时为 `true`。
pub fn cache_value_needs_rewrite(bytes: &[u8], codec: Option<&dyn MapperCacheCodec>) -> bool {
    codec
        .or_else(|| DEFAULT_MAPPER_CACHE_CODEC.get().map(Arc::as_ref))
        .is_some_and(|codec| codec.should_rewrite_value(bytes))
}

/// 业务作用: 把 Mapper cache 指标交给已安装观测出口，未安装时静默跳过。
///
/// # 参数
/// - `metric`: 当前查询或缓存路径产生的结构化指标。
///
/// 返回: 无；观测缺失不影响数据库业务结果。
pub fn record_mapper_metric(metric: MapperMetric<'_>) {
    if let Some(metrics) = DEFAULT_MAPPER_METRICS.get() {
        metrics.record(metric);
    }
}

/// 业务作用: 在应用 Ready 前确认声明默认缓存的 Mapper 已安装 L2 cache。
///
/// 参数说明: 无。
///
/// 返回: 无缓存查询或已安装 cache 时成功；否则阻断启动。
pub fn assert_l2_cache_installed_for_cached_queries() -> anyhow::Result<()> {
    if MAPPER_CACHE_META.iter().any(|meta| meta.has_cached_query)
        && DEFAULT_L2_CACHE.get().is_none()
    {
        anyhow::bail!("mapper has cache-enabled queries but default L2 cache is not installed");
    }
    Ok(())
}

/// 业务作用: 根据 Mapper 元数据计算写操作提交后需要失效的全部缓存 namespace。
///
/// # 参数
/// - `source_key`: 发起清理的 Mapper namespace。
/// - `flush_refs`: 是否展开正向与反向关联。
///
/// 返回: 去重并排序的缓存 namespace。
pub fn cache_clear_targets(source_key: &str, flush_refs: bool) -> Vec<String> {
    let mut targets = std::collections::HashSet::from([source_key.to_owned()]);
    if flush_refs {
        for meta in MAPPER_CACHE_META.iter() {
            if meta.key == source_key {
                targets.extend(meta.clear_also.iter().map(|key| (*key).to_owned()));
            }
            if meta.clear_when.contains(&source_key) {
                targets.insert(meta.key.to_owned());
            }
        }
    }
    let mut targets = targets.into_iter().collect::<Vec<_>>();
    targets.sort();
    targets
}

/// 业务作用: 向最终 SQL 追加经过白名单验证的完整 `ORDER BY` 子句。
///
/// # 参数
/// - `value`: 排序项、列表或可选排序项。
/// - `sql`: 最终 SQL 输出缓冲区。
///
/// 返回: 无排序时保持原 SQL；字段非法时返回错误。
pub fn write_mapper_order_by_clause<T: MapperOrderBy + ?Sized>(
    value: &T,
    sql: &mut String,
) -> anyhow::Result<()> {
    let mut order = String::new();
    if value.write_mapper_order_by(&mut order)? {
        let order = normalize_sql_whitespace(&order);
        if !order.trim().is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(order.trim());
        }
    }
    Ok(())
}

/// 业务作用: 规范化结构化 Mapper SQL 外部空白，同时保持字面量、quoted identifier 与块注释内部内容。
///
/// # 参数
/// - `sql`: 宏按节点顺序构造的 prepared SQL。
///
/// 返回: 可用于执行和缓存身份的稳定 SQL 文本。
pub fn normalize_sql_whitespace(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut single = false;
    let mut double = false;
    let mut block_comment = false;
    let mut pending_space = false;
    while let Some(ch) = chars.next() {
        if block_comment {
            out.push(ch);
            if ch == '*' && chars.peek() == Some(&'/') {
                out.push(chars.next().expect("peeked block comment terminator"));
                block_comment = false;
            }
            continue;
        }
        if single {
            out.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    out.push(chars.next().expect("peeked escaped quote"));
                } else {
                    single = false;
                }
            } else if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            continue;
        }
        if double {
            out.push(ch);
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    out.push(chars.next().expect("peeked escaped identifier quote"));
                } else {
                    double = false;
                }
            } else if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            continue;
        }
        if ch == '/' && chars.peek() == Some(&'*') {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            out.push(ch);
            out.push(chars.next().expect("peeked block comment opener"));
            block_comment = true;
            continue;
        }
        if ch.is_ascii_whitespace() {
            pending_space = true;
            continue;
        }
        if ch == ',' {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(ch);
            pending_space = false;
            continue;
        }
        if pending_space && !out.is_empty() && !out.ends_with(['(', ',']) && ch != ')' {
            out.push(' ');
        }
        pending_space = false;
        if ch == '\'' {
            single = true;
        } else if ch == '"' {
            double = true;
        }
        out.push(ch);
    }
    out
}

/// 业务作用: 应用动态 SQL `trim/where/set` 的前后缀与 token 剥离规则。
///
/// # 参数
/// - `body`: 已展开动态节点的 SQL 片段。
/// - `prefix`: 非空 body 前缀。
/// - `suffix`: 非空 body 后缀。
/// - `prefix_overrides`: 可从片段头部剥离的 token。
/// - `suffix_overrides`: 可从片段尾部剥离的 token。
///
/// 返回: 空片段返回 `None`；其它情况返回规范化后的完整片段。
pub fn apply_sql_trim(
    body: &str,
    prefix: &str,
    suffix: &str,
    prefix_overrides: &[&str],
    suffix_overrides: &[&str],
) -> Option<String> {
    let normalized = normalize_sql_whitespace(body);
    let mut trimmed = normalized.trim().to_owned();
    for token in prefix_overrides.iter().map(|token| token.trim()) {
        if !token.is_empty() && starts_with_sql_token(&trimmed, token) {
            trimmed = trimmed[token.len()..].trim_start().to_owned();
            break;
        }
    }
    for token in suffix_overrides.iter().map(|token| token.trim()) {
        if !token.is_empty() && ends_with_sql_token(&trimmed, token) {
            trimmed.truncate(trimmed.len() - token.len());
            trimmed = trimmed.trim_end().to_owned();
            break;
        }
    }
    if trimmed.is_empty() {
        return None;
    }
    let mut out = String::new();
    if !prefix.trim().is_empty() {
        out.push_str(prefix.trim());
        out.push(' ');
    }
    out.push_str(&trimmed);
    if !suffix.trim().is_empty() {
        out.push(' ');
        out.push_str(suffix.trim());
    }
    Some(out)
}

/// 业务作用: 校验排序字段仅由一到两段普通 identifier 组成。
///
/// # 参数
/// - `field`: `MapperOrderField` 实现返回的静态文本。
///
/// 返回: 安全字段成功；表达式、引号或额外分段返回错误。
fn validate_order_field(field: &str) -> anyhow::Result<()> {
    let segments = field.split('.').collect::<Vec<_>>();
    if (1..=2).contains(&segments.len()) && segments.iter().all(|segment| is_sql_ident(segment)) {
        Ok(())
    } else {
        anyhow::bail!("invalid mapper order field");
    }
}

/// 业务作用: 判断单段文本是否为普通 ASCII SQL identifier。
///
/// # 参数
/// - `value`: 字段名或表别名。
///
/// 返回: 首字符与后续字符均满足安全白名单时为 `true`。
fn is_sql_ident(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first == b'_' || first.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

/// 业务作用: 按 SQL token 边界判断片段是否以指定 override 开头。
///
/// # 参数
/// - `value`: 已规范化 SQL 片段。
/// - `token`: trim 配置提供的头部 token。
///
/// 返回: 大小写无关匹配且后侧是 token 边界时为 `true`。
fn starts_with_sql_token(value: &str, token: &str) -> bool {
    value
        .get(..token.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(token))
        && value[token.len()..]
            .chars()
            .next()
            .is_none_or(is_sql_token_boundary)
}

/// 业务作用: 按 SQL token 边界判断片段是否以指定 override 结尾。
///
/// # 参数
/// - `value`: 已规范化 SQL 片段。
/// - `token`: trim 配置提供的尾部 token。
///
/// 返回: 大小写无关匹配且前侧是 token 边界时为 `true`。
fn ends_with_sql_token(value: &str, token: &str) -> bool {
    if value.len() < token.len() {
        return false;
    }
    let start = value.len() - token.len();
    value[start..].eq_ignore_ascii_case(token)
        && value[..start]
            .chars()
            .next_back()
            .is_none_or(is_sql_token_boundary)
}

/// 业务作用: 判断字符是否能分隔两个 SQL token。
///
/// # 参数
/// - `ch`: token 相邻字符。
///
/// 返回: 非 identifier 字符视为安全边界。
fn is_sql_token_boundary(ch: char) -> bool {
    !(ch == '_' || ch.is_ascii_alphanumeric())
}
