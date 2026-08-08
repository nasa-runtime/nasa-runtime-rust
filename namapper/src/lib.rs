//! SQL mapper 运行时与公共类型。
//!
//! 提供 mapper 宏展开依赖的连接获取、事务桥接、枚举/排序辅助和可选 Redis L2 缓存适配能力。
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
#[cfg(feature = "redis-cache")]
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::task::{Context, Poll};
#[cfg(feature = "redis-cache")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Redis TTL 与 Tokio timer 共用的 Mapper 运行时毫秒上限。
#[cfg(feature = "redis-cache")]
const MAX_MAPPER_RUNTIME_MILLIS: u64 = 365 * 24 * 60 * 60 * 1_000;

/// 业务作用：非 fallible single-flight builder 统一收敛到有效毫秒范围。
#[cfg(feature = "redis-cache")]
fn bounded_mapper_millis(value: u64) -> u64 {
    value.clamp(1, MAX_MAPPER_RUNTIME_MILLIS)
}

/// 业务作用：Redis 写入前校验 TTL，防止 value 已落盘后才因过期参数非法而留下永久字段。
#[cfg(feature = "redis-cache")]
fn validate_mapper_ttl(ttl_ms: u64) -> anyhow::Result<()> {
    if !(1..=MAX_MAPPER_RUNTIME_MILLIS).contains(&ttl_ms) {
        anyhow::bail!("mapper cache TTL must be within 1ms..=365 days");
    }
    Ok(())
}

pub use async_trait::async_trait;
pub use namapper_macro::{
    Delete, Execute, Insert, Mapper, MapperEnum, MapperOrderField, Query, StreamQuery, Update,
};
pub use sqlx::types::Json;

/// Mapper 流式查询返回类型。
///
/// `MapperStream` 拥有底层 stream；首版 `#[StreamQuery]` 只允许无 ambient 事务场景，
/// 由生成代码克隆 datasource pool 并把 pool 生命周期放进 stream 内部。
pub struct MapperStream<T> {
    inner: Pin<Box<dyn futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static>>,
}

impl<T> MapperStream<T> {
    /// 业务作用：包装一个 owned stream。
    ///
    /// # 参数
    /// - `stream`: 由 `sqlx::query_as(...).fetch(...)` 等调用返回的 owned stream；
    ///   生成代码会把 datasource pool 生命周期一起移动进 stream，避免借用外部连接。
    pub fn new<S>(stream: S) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }

    /// 业务作用：读取下一行。
    ///
    /// 对业务侧而言这等价于逐行消费查询结果；返回 `None` 表示数据库结果集已经结束。
    pub async fn next(&mut self) -> Option<Result<T, sqlx::Error>> {
        futures_util::StreamExt::next(&mut self.inner).await
    }
}

impl<T> futures_core::Stream for MapperStream<T> {
    type Item = Result<T, sqlx::Error>;

    /// 业务作用：将外层 `MapperStream` 的轮询转发给内部 boxed stream。
    ///
    /// # 参数
    /// - `self`: 当前被 pin 住的 Mapper stream。
    /// - `cx`: 异步运行时传入的唤醒上下文。
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

/// Mapper 分页参数，供 `LIMIT #{page.limit} OFFSET #{page.offset}` 绑定使用。
///
/// `page_no` 采用业务常见的 1-based 语义；构造后只暴露最终 SQL 需要的
/// `limit/offset`，避免每个 repository 重复计算和校验。
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageRequest {
    /// SQL `LIMIT` 绑定值。
    pub limit: i64,
    /// SQL `OFFSET` 绑定值。
    pub offset: i64,
}

impl PageRequest {
    /// 默认单页最大条数。
    pub const DEFAULT_MAX_PAGE_SIZE: u64 = 1_000;

    /// 业务作用：创建分页参数，页码从 1 开始，单页最大默认 1000。
    ///
    /// # 参数
    /// - `page_no`: 业务请求页码，采用 1-based 语义；传 0 会返回错误。
    /// - `page_size`: 单页条数，必须大于 0 且不超过默认上限。
    pub fn new(page_no: u64, page_size: u64) -> anyhow::Result<Self> {
        Self::with_max_page_size(page_no, page_size, Self::DEFAULT_MAX_PAGE_SIZE)
    }

    /// 业务作用：创建分页参数并指定单页最大条数。
    ///
    /// # 参数
    /// - `page_no`: 业务请求页码，采用 1-based 语义。
    /// - `page_size`: 单页条数，必须大于 0。
    /// - `max_page_size`: 调用方允许的最大单页条数，用于限制导出类或列表类接口。
    pub fn with_max_page_size(
        page_no: u64,
        page_size: u64,
        max_page_size: u64,
    ) -> anyhow::Result<Self> {
        if page_no == 0 {
            return Err(anyhow::anyhow!("page_no must start from 1"));
        }
        if page_size == 0 {
            return Err(anyhow::anyhow!("page_size must be greater than 0"));
        }
        if max_page_size == 0 {
            return Err(anyhow::anyhow!("max_page_size must be greater than 0"));
        }
        if page_size > max_page_size {
            return Err(anyhow::anyhow!(
                "page_size {page_size} exceeds max_page_size {max_page_size}"
            ));
        }
        let offset = page_no
            .checked_sub(1)
            .and_then(|page_index| page_index.checked_mul(page_size))
            .ok_or_else(|| anyhow::anyhow!("page offset overflow"))?;
        Self::from_offset_limit(offset, page_size)
    }

    /// 业务作用：从 offset/limit 创建分页参数。
    ///
    /// # 参数
    /// - `offset`: SQL `OFFSET` 原始值，适合已经由上游计算好偏移量的场景。
    /// - `limit`: SQL `LIMIT` 原始值，必须大于 0。
    pub fn from_offset_limit(offset: u64, limit: u64) -> anyhow::Result<Self> {
        if limit == 0 {
            return Err(anyhow::anyhow!("limit must be greater than 0"));
        }
        Ok(Self {
            limit: i64::try_from(limit).map_err(|_| anyhow::anyhow!("limit overflowed i64"))?,
            offset: i64::try_from(offset).map_err(|_| anyhow::anyhow!("offset overflowed i64"))?,
        })
    }
}

/// Mapper 批量写入/更新的切片迭代器。
///
/// 这是业务侧组织批量 `Insert/Update/Delete` 的轻量工具；宏本身只负责把单次
/// SQL 模板展开成 prepared statement，不替业务隐式拆分事务边界。
#[derive(Debug)]
pub struct MapperBatchChunks<'a, T> {
    /// 待批量写入、更新或删除的业务对象切片。
    items: &'a [T],
    /// 每个批次最多包含的元素数量。
    chunk_size: usize,
    /// 当前迭代到的起始位置。
    offset: usize,
}

impl<'a, T> Iterator for MapperBatchChunks<'a, T> {
    type Item = &'a [T];

    /// 业务作用：返回下一段批量参数切片。
    ///
    /// 该方法不复制业务对象，只返回原始切片的子切片，适合批量 SQL 在外层显式控制事务。
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

/// 业务作用：按固定大小拆分批量参数。
///
/// `chunk_size` 必须大于 0；空集合会返回一个空迭代器。
///
/// # 参数
/// - `items`: 需要按批次提交给 Mapper 写操作的业务对象切片。
/// - `chunk_size`: 每批元素数量；应结合数据库最大参数数和事务大小设置。
pub fn batch_chunks<T>(items: &[T], chunk_size: usize) -> anyhow::Result<MapperBatchChunks<'_, T>> {
    if chunk_size == 0 {
        return Err(anyhow::anyhow!(
            "mapper batch chunk_size must be greater than 0"
        ));
    }
    Ok(MapperBatchChunks {
        items,
        chunk_size,
        offset: 0,
    })
}

/// Mapper 动态排序字段白名单。
///
/// 推荐业务 enum 使用 `#[derive(MapperOrderField)]` 生成实现。返回值只允许
/// `column` 或 `table_alias.column` 这类简单字段名，不能返回表达式、函数调用或
/// 用户输入字符串。
pub trait MapperOrderField: Copy + Sized + Send + Sync + 'static {
    /// 业务作用：返回 SQL `ORDER BY` 中允许出现的字段名。
    ///
    /// 该字段名必须来自业务 enum 白名单，不能从用户输入直接拼接。
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
    /// 业务作用：返回可直接写入 SQL 的排序方向关键字。
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
    /// 业务白名单字段。
    field: F,
    /// 排序方向。
    direction: OrderDirection,
}

impl<F: MapperOrderField> OrderBy<F> {
    /// 业务作用：创建升序排序项。
    ///
    /// # 参数
    /// - `field`: 已通过 `MapperOrderField` 白名单约束的业务排序字段。
    pub fn asc(field: F) -> Self {
        Self {
            field,
            direction: OrderDirection::Asc,
        }
    }

    /// 业务作用：创建降序排序项。
    ///
    /// # 参数
    /// - `field`: 已通过 `MapperOrderField` 白名单约束的业务排序字段。
    pub fn desc(field: F) -> Self {
        Self {
            field,
            direction: OrderDirection::Desc,
        }
    }

    /// 业务作用：返回字段白名单项。
    ///
    /// 该方法主要用于业务日志、调试或自定义排序组合逻辑。
    pub fn field(self) -> F {
        self.field
    }

    /// 业务作用：返回排序方向。
    ///
    /// 该方法主要用于业务日志、调试或自定义排序组合逻辑。
    pub fn direction(self) -> OrderDirection {
        self.direction
    }
}

/// 可被 `<order_by value="..."/>` 标签渲染的类型。
pub trait MapperOrderBy {
    /// 业务作用：向 `out` 写入不带 `ORDER BY` 前缀的排序片段，返回是否实际写入。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区；实现只写字段和方向，不写 `ORDER BY` 前缀。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool>;
}

impl<T: MapperOrderBy + ?Sized> MapperOrderBy for &T {
    /// 业务作用：允许以引用形式复用底层排序渲染实现。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        (*self).write_mapper_order_by(out)
    }
}

impl<T: MapperOrderBy> MapperOrderBy for Option<T> {
    /// 业务作用：渲染可选排序项；`None` 表示业务本次不追加排序。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        match self {
            Some(value) => value.write_mapper_order_by(out),
            None => Ok(false),
        }
    }
}

impl<F: MapperOrderField> MapperOrderBy for OrderBy<F> {
    /// 业务作用：渲染单个排序项，并在写入前再次校验字段名形状。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区。
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
    /// 业务作用：按声明顺序渲染多个排序项。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区；多个排序项之间用逗号连接。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        if self.is_empty() {
            return Ok(false);
        }
        for (idx, item) in self.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            item.write_mapper_order_by(out)?;
        }
        Ok(true)
    }
}

impl<F: MapperOrderField> MapperOrderBy for Vec<OrderBy<F>> {
    /// 业务作用：渲染业务常用的动态排序列表。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        self.as_slice().write_mapper_order_by(out)
    }
}

impl<F: MapperOrderField, const N: usize> MapperOrderBy for [OrderBy<F>; N] {
    /// 业务作用：渲染固定长度排序数组。
    ///
    /// # 参数
    /// - `out`: SQL 片段输出缓冲区。
    fn write_mapper_order_by(&self, out: &mut String) -> anyhow::Result<bool> {
        self.as_slice().write_mapper_order_by(out)
    }
}

/// Mapper 默认枚举转换契约。
///
/// 语义对齐既有 ordinal 枚举处理:写库时使用 ordinal，读库时由
/// ordinal 还原业务 enum。业务 enum 可用 `#[derive(MapperEnum)]` 按声明顺序
/// 生成实现；需要固定业务码时应手写实现，避免变体顺序调整后产生静默兼容风险。
pub trait MapperEnum: Copy + Sized + Send + Sync + 'static {
    /// 业务作用：返回写入数据库和 cache JSON 的 ordinal。
    fn ordinal(self) -> i32;

    /// 业务作用：从数据库 ordinal 还原 enum；未知值应返回 `None`。
    ///
    /// # 参数
    /// - `value`: 数据库列或缓存 value 中读取到的 ordinal。
    fn from_ordinal(value: i32) -> Option<Self>;
}

/// 以 ordinal 方式参与 Mapper SQL bind、FromRow decode 和 L2 cache serde 的 enum 包装。
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnumOrdinal<E: MapperEnum>(
    /// 被包装的业务 enum 值。
    pub E,
);

impl<E: MapperEnum> EnumOrdinal<E> {
    /// 业务作用：创建 ordinal enum 包装值。
    ///
    /// # 参数
    /// - `value`: 需要按 ordinal 语义写库、读库或序列化缓存的业务 enum。
    pub fn new(value: E) -> Self {
        Self(value)
    }

    /// 业务作用：取回内部 enum。
    ///
    /// 该方法用于业务层已经完成数据库/cache 交互后恢复原始 enum 类型。
    pub fn into_inner(self) -> E {
        self.0
    }

    /// 业务作用：返回当前 enum 的 ordinal。
    ///
    /// 该值会作为 MySQL 整数字段和缓存 JSON 整数值。
    pub fn ordinal(self) -> i32 {
        self.0.ordinal()
    }
}

impl<E: MapperEnum> From<E> for EnumOrdinal<E> {
    /// 业务作用：从业务 enum 直接构造 ordinal 包装。
    ///
    /// # 参数
    /// - `value`: 需要进入 Mapper 编解码流程的业务 enum。
    fn from(value: E) -> Self {
        Self(value)
    }
}

impl<E: MapperEnum> Deref for EnumOrdinal<E> {
    type Target = E;

    /// 业务作用：允许业务代码以只读方式访问内部 enum。
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E: MapperEnum> serde::Serialize for EnumOrdinal<E> {
    /// 业务作用：将 enum ordinal 写成 JSON 整数。
    ///
    /// # 参数
    /// - `serializer`: serde 提供的目标序列化器。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.0.ordinal())
    }
}

impl<'de, E: MapperEnum> serde::Deserialize<'de> for EnumOrdinal<E> {
    /// 业务作用：从 JSON 整数还原 enum ordinal 包装。
    ///
    /// # 参数
    /// - `deserializer`: serde 提供的来源反序列化器。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ordinal = <i32 as serde::Deserialize>::deserialize(deserializer)?;
        E::from_ordinal(ordinal).map(Self).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown mapper enum ordinal {ordinal} for {}",
                std::any::type_name::<E>()
            ))
        })
    }
}

impl<E: MapperEnum> sqlx::Type<sqlx::MySql> for EnumOrdinal<E> {
    /// 业务作用：告诉 sqlx 该包装类型在 MySQL 中按 `i32` 类型绑定。
    fn type_info() -> <sqlx::MySql as sqlx::Database>::TypeInfo {
        <i32 as sqlx::Type<sqlx::MySql>>::type_info()
    }

    /// 业务作用：判断数据库列类型是否可以按 `i32` 解码。
    ///
    /// # 参数
    /// - `ty`: sqlx 从 MySQL 元数据读取到的列类型信息。
    fn compatible(ty: &<sqlx::MySql as sqlx::Database>::TypeInfo) -> bool {
        <i32 as sqlx::Type<sqlx::MySql>>::compatible(ty)
    }
}

impl<'q, E: MapperEnum> sqlx::Encode<'q, sqlx::MySql> for EnumOrdinal<E> {
    /// 业务作用：按值把 enum ordinal 编码进 MySQL 参数缓冲区。
    ///
    /// # 参数
    /// - `buf`: sqlx 提供的 MySQL 参数缓冲区。
    fn encode(self, buf: &mut Vec<u8>) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError>
    where
        Self: Sized,
    {
        <i32 as sqlx::Encode<'q, sqlx::MySql>>::encode(self.0.ordinal(), buf)
    }

    /// 业务作用：按引用把 enum ordinal 编码进 MySQL 参数缓冲区。
    ///
    /// # 参数
    /// - `buf`: sqlx 提供的 MySQL 参数缓冲区。
    fn encode_by_ref(
        &self,
        buf: &mut Vec<u8>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let ordinal = self.0.ordinal();
        <i32 as sqlx::Encode<'q, sqlx::MySql>>::encode_by_ref(&ordinal, buf)
    }
}

impl<'r, E: MapperEnum> sqlx::Decode<'r, sqlx::MySql> for EnumOrdinal<E> {
    /// 业务作用：从 MySQL 整数字段解码 enum ordinal。
    ///
    /// # 参数
    /// - `value`: sqlx 传入的 MySQL 原始列值引用。
    fn decode(
        value: <sqlx::MySql as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let ordinal = <i32 as sqlx::Decode<'r, sqlx::MySql>>::decode(value)?;
        E::from_ordinal(ordinal).map(Self).ok_or_else(|| {
            format!(
                "unknown mapper enum ordinal {ordinal} for {}",
                std::any::type_name::<E>()
            )
            .into()
        })
    }
}

/// 编译期收集到的 Mapper 二级缓存元数据。
pub struct MapperCacheMeta {
    /// Redis Hash key / 缓存组名。
    pub key: &'static str,
    /// 当前 Mapper 是否存在默认启用缓存的查询。
    pub has_cached_query: bool,
    /// 清理当前 key 时需要额外清理的 key。
    pub clear_also: &'static [&'static str],
    /// 被列出的 key 清理时，也需要清理当前 key。
    pub clear_when: &'static [&'static str],
}

/// 所有 `#[Mapper]` trait 生成的缓存元数据。
#[linkme::distributed_slice]
pub static MAPPER_CACHE_META: [MapperCacheMeta];

/// 参与缓存 hash_key 生成的绑定参数。
pub struct CacheArg {
    /// Mapper 方法参数名或嵌套参数路径名。
    name: &'static str,
    /// 参数的 JSON bytes，用于默认 hash_key 的稳定 hash。
    json: Vec<u8>,
    /// 参数是标量时保留明文值，供 `hash_key_suffix` 显式引用。
    scalar: Option<String>,
}

impl CacheArg {
    /// 业务作用：将任意可序列化参数转成稳定缓存参数。
    ///
    /// # 参数
    /// - `name`: 方法参数名,仅供自定义 `hash_key_suffix` 引用。
    /// - `value`: 参数值。
    pub fn try_new<T: serde::Serialize + ?Sized>(
        name: &'static str,
        value: &T,
    ) -> anyhow::Result<Self> {
        let value = serde_json::to_value(value)?;
        let scalar = scalar_key_part(&value);
        let json = serde_json::to_vec(&value)?;
        Ok(Self { name, json, scalar })
    }
}

/// 业务作用：将 JSON 标量转换为可读缓存 key 后缀片段。
///
/// # 参数
/// - `value`: 已由 Mapper 方法参数序列化得到的 JSON value。
fn scalar_key_part(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(v) => Some(v.to_string()),
        serde_json::Value::Number(v) => Some(v.to_string()),
        serde_json::Value::String(v) => Some(v.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

/// 业务作用：生成 `IN (#{ids})` 列表参数需要的 prepared 占位符。
///
/// # 参数
/// - `len`: 集合参数元素个数；为 0 时拒绝生成非法 `IN ()` SQL。
#[doc(hidden)]
pub fn sql_in_placeholders(len: usize) -> anyhow::Result<String> {
    if len == 0 {
        return Err(anyhow::anyhow!("Mapper IN 列表参数不能为空"));
    }
    Ok((0..len).map(|_| "?").collect::<Vec<_>>().join(","))
}

/// 业务作用：向 SQL 写入完整 `ORDER BY ...` 子句。
///
/// # 参数
/// - `value`: 业务传入的白名单排序项、排序列表或可选排序项。
/// - `sql`: 已生成的 SQL 主体缓冲区；本函数只在有排序内容时追加 `ORDER BY`。
#[doc(hidden)]
pub fn write_mapper_order_by_clause<T: MapperOrderBy + ?Sized>(
    value: &T,
    sql: &mut String,
) -> anyhow::Result<()> {
    let mut order_sql = String::new();
    if value.write_mapper_order_by(&mut order_sql)? {
        let order_sql = normalize_sql_whitespace(&order_sql);
        if !order_sql.trim().is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(order_sql.trim());
        }
    }
    Ok(())
}

/// 业务作用：校验排序字段是否满足 Mapper 的最小安全形状。
///
/// # 参数
/// - `field`: `MapperOrderField` 返回的字段名。
fn validate_order_field(field: &str) -> anyhow::Result<()> {
    if is_valid_order_field(field) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "invalid mapper order field `{field}`; only `column` or `alias.column` is allowed"
        ))
    }
}

/// 业务作用：判断排序字段是否为 `column` 或 `alias.column`。
///
/// # 参数
/// - `field`: 待检查的字段名。
fn is_valid_order_field(field: &str) -> bool {
    let mut segments = field.split('.');
    let Some(first) = segments.next() else {
        return false;
    };
    if !is_sql_ident(first) {
        return false;
    }
    let mut count = 1;
    for segment in segments {
        count += 1;
        if count > 2 || !is_sql_ident(segment) {
            return false;
        }
    }
    true
}

/// 业务作用：判断字符串是否为安全 SQL 标识符片段。
///
/// # 参数
/// - `value`: 待检查的单段字段名或表别名。
fn is_sql_ident(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// 业务作用：规范化 mapper SQL，确保缓存 hash_key 中的 SQL 与实际执行 SQL 一致。
///
/// # 参数
/// - `sql`: 宏展开或运行期动态标签拼出来的 prepared SQL。
#[doc(hidden)]
pub fn normalize_sql_whitespace(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut pending_space = false;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_block_comment {
            out.push(ch);
            if ch == '*' && chars.peek() == Some(&'/') {
                out.push(chars.next().expect("peeked char must exist"));
                in_block_comment = false;
            }
            continue;
        }

        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    out.push(chars.next().expect("peeked char must exist"));
                } else {
                    in_single_quote = false;
                }
            } else if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            continue;
        }

        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    out.push(chars.next().expect("peeked char must exist"));
                } else {
                    in_double_quote = false;
                }
            } else if ch == '\\' {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            continue;
        }

        if in_backtick {
            out.push(ch);
            if ch == '`' {
                in_backtick = false;
            }
            continue;
        }

        // 块注释 `/* ... */` 整体原样保留：注释里的撇号/引号/反引号不得翻转字符串状态机，
        // 否则会破坏注释之后真实字符串字面量内部的空白/逗号规范化（改变最终 SQL 语义）。
        if ch == '/' && chars.peek() == Some(&'*') {
            if pending_space && !out.is_empty() && !out.ends_with(',') && !out.ends_with('(') {
                out.push(' ');
            }
            pending_space = false;
            out.push('/');
            out.push(chars.next().expect("peeked char must exist"));
            in_block_comment = true;
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
            out.push(',');
            pending_space = false;
            continue;
        }

        if pending_space
            && !out.is_empty()
            && !out.ends_with(',')
            && !out.ends_with('(')
            && ch != ')'
        {
            out.push(' ');
        }
        pending_space = false;

        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
            }
            '`' => {
                in_backtick = true;
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }

    out
}

/// 业务作用：应用声明式 SQL 中 `trim/where/set` 的前后缀规则。
///
/// # 参数
/// - `body`: 动态 SQL 标签体已经展开后的 SQL 片段。
/// - `prefix`: 片段非空时要添加的前缀，例如 `WHERE` 或 `SET`。
/// - `suffix`: 片段非空时要添加的后缀。
/// - `prefix_overrides`: 需要从片段头部剥离的 token，例如 `AND` / `OR`。
/// - `suffix_overrides`: 需要从片段尾部剥离的 token，例如逗号。
#[doc(hidden)]
pub fn apply_sql_trim(
    body: &str,
    prefix: &str,
    suffix: &str,
    prefix_overrides: &[&str],
    suffix_overrides: &[&str],
) -> Option<String> {
    let normalized = normalize_sql_whitespace(body);
    let mut trimmed = normalized.trim().to_string();
    if trimmed.is_empty() {
        return None;
    }

    for override_token in prefix_overrides {
        let token = override_token.trim();
        if token.is_empty() {
            continue;
        }
        if starts_with_sql_token(&trimmed, token) {
            trimmed = trimmed
                .get(token.len()..)
                .expect("starts_with_sql_token guarantees token boundary")
                .trim_start()
                .to_string();
            break;
        }
    }

    for override_token in suffix_overrides {
        let token = override_token.trim();
        if token.is_empty() {
            continue;
        }
        if ends_with_sql_token(&trimmed, token) {
            let end = trimmed.len() - token.len();
            trimmed = trimmed
                .get(..end)
                .expect("ends_with_sql_token guarantees token boundary")
                .trim_end()
                .to_string();
            break;
        }
    }

    if trimmed.is_empty() {
        return None;
    }

    let mut out = String::new();
    let prefix = prefix.trim();
    let suffix = suffix.trim();
    if !prefix.is_empty() {
        out.push_str(prefix);
        out.push(' ');
    }
    out.push_str(&trimmed);
    if !suffix.is_empty() {
        out.push(' ');
        out.push_str(suffix);
    }
    Some(out)
}

/// 业务作用：判断 SQL 片段是否以指定 token 开头，且 token 后是合法边界。
///
/// # 参数
/// - `value`: 已经规范化空白的 SQL 片段。
/// - `token`: `prefix_overrides` 中配置的待剥离 token。
fn starts_with_sql_token(value: &str, token: &str) -> bool {
    let Some(prefix) = value.get(..token.len()) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case(token) {
        return false;
    }
    if is_symbol_sql_token(token) {
        return true;
    }
    value
        .get(token.len()..)
        .and_then(|rest| rest.chars().next())
        .is_none_or(is_sql_token_boundary)
}

/// 业务作用：判断 SQL 片段是否以指定 token 结尾，且 token 前是合法边界。
///
/// # 参数
/// - `value`: 已经规范化空白的 SQL 片段。
/// - `token`: `suffix_overrides` 中配置的待剥离 token。
fn ends_with_sql_token(value: &str, token: &str) -> bool {
    let Some(start) = value.len().checked_sub(token.len()) else {
        return false;
    };
    let Some(suffix) = value.get(start..) else {
        return false;
    };
    if !suffix.eq_ignore_ascii_case(token) {
        return false;
    }
    if is_symbol_sql_token(token) {
        return true;
    }
    value
        .get(..start)
        .and_then(|prefix| prefix.chars().next_back())
        .is_none_or(is_sql_token_boundary)
}

/// 业务作用：判断 override token 是否完全由符号组成。
///
/// # 参数
/// - `token`: trim/where/set 配置中的前后缀覆盖 token。
fn is_symbol_sql_token(token: &str) -> bool {
    token
        .chars()
        .all(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
}

/// 业务作用：判断字符是否可以作为 SQL token 边界。
///
/// # 参数
/// - `ch`: 待判断的相邻字符。
fn is_sql_token_boundary(ch: char) -> bool {
    ch.is_ascii_whitespace() || ch == '(' || ch == ',' || ch == ')'
}

/// 业务作用：生成默认二级缓存 hash_key。
///
/// `normalized_sql` 必须是 `#{name}` 已转换成 `?` 的 SQL；返回值保留 SQL 明文,
/// 参数值用 SHA-256 小写 hex，避免长参数直接进入 Redis Hash field。
///
/// # 参数
/// - `normalized_sql`: 规范化后的 prepared SQL。
/// - `args`: 按 SQL 占位符出现顺序排列的参数。
pub fn cache_hash_key(normalized_sql: &str, args: &[CacheArg]) -> anyhow::Result<String> {
    let mut hash_key = format!("sql:{normalized_sql}");
    for arg in args {
        hash_key.push(':');
        hash_key.push_str(&sha256_hex(&arg.json));
    }
    Ok(hash_key)
}

/// 业务作用：用自定义参数后缀生成 hash_key，仍强制保留 SQL 明文前缀。
///
/// # 参数
/// - `normalized_sql`: 规范化后的 prepared SQL。
/// - `suffix_template`: 只允许 `{param}` 形式引用标量方法参数。
/// - `args`: 可供模板引用的参数。
pub fn cache_hash_key_with_suffix(
    normalized_sql: &str,
    suffix_template: &str,
    args: &[CacheArg],
) -> anyhow::Result<String> {
    let mut out = String::new();
    let mut rest = suffix_template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after_open = &rest[start + 1..];
        let end = after_open
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("hash_key_suffix 模板存在未闭合的 `{{`"))?;
        let name = &after_open[..end];
        if name.is_empty() {
            return Err(anyhow::anyhow!("hash_key_suffix 模板存在空占位符"));
        }
        let arg = args
            .iter()
            .find(|arg| arg.name == name)
            .ok_or_else(|| anyhow::anyhow!("hash_key_suffix 模板引用了未知参数 `{name}`"))?;
        let scalar = arg.scalar.as_ref().ok_or_else(|| {
            anyhow::anyhow!("hash_key_suffix 参数 `{name}` 不是标量,请使用默认参数 hash")
        })?;
        out.push_str(scalar);
        rest = &after_open[end + 1..];
    }
    if rest.contains('}') {
        return Err(anyhow::anyhow!("hash_key_suffix 模板存在未匹配的 `}}`"));
    }
    out.push_str(rest);
    Ok(format!("sql:{normalized_sql}:{out}"))
}

/// 业务作用：计算小写 SHA-256 hex 字符串。
///
/// # 参数
/// - `bytes`: 需要进入 hash_key 的参数 JSON bytes 或锁 key 原始 bytes。
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// Mapper cache loader future。
///
/// 该 future 返回已经编码好的 cache value bytes，便于 `get_or_load` 在写缓存前不再
/// 关心业务返回类型。
pub type MapperCacheLoadFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + 'a>>;

/// Mapper cache miss 时用于加载并编码查询结果的回调。
///
/// loader 必须是 `FnOnce`，因为一次 miss 只能消费一次数据库查询结果。
pub type MapperCacheLoader<'a> = Box<dyn FnOnce() -> MapperCacheLoadFuture<'a> + Send + 'a>;

/// `get_or_load` 的结果来源。
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MapperCacheLoadState {
    /// 首次读取命中缓存。
    Hit,
    /// 等待同 key 加载后再次读取命中缓存。
    HitAfterWait,
    /// 当前调用负责加载源数据并写入缓存。
    Loaded,
}

/// `get_or_load` 返回值。
#[derive(Debug)]
pub struct MapperCacheLoad {
    /// 已编码的缓存 value bytes。
    pub bytes: Vec<u8>,
    /// 结果来源。
    pub state: MapperCacheLoadState,
}

impl MapperCacheLoad {
    /// 业务作用：构造首次读取即命中的返回值。
    ///
    /// # 参数
    /// - `bytes`: 从 L2 cache 读取到的原始 value bytes。
    fn hit(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            state: MapperCacheLoadState::Hit,
        }
    }

    /// 业务作用：构造等待同 key loader 完成后命中的返回值。
    ///
    /// # 参数
    /// - `bytes`: 等待期间由其它调用写入缓存后再次读取到的 value bytes。
    fn hit_after_wait(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            state: MapperCacheLoadState::HitAfterWait,
        }
    }

    /// 业务作用：构造由当前调用加载数据库并写入缓存后的返回值。
    ///
    /// # 参数
    /// - `bytes`: loader 从数据库查询并编码后的 value bytes。
    fn loaded(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            state: MapperCacheLoadState::Loaded,
        }
    }
}

/// Mapper 二级缓存入口。业务侧可以接 Redis Hash、本地缓存或自研缓存。
#[async_trait]
pub trait MapperL2Cache: Send + Sync + 'static {
    /// 业务作用：读取单条查询缓存。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace，通常来自 `#[Mapper(cache_key = "...")]`。
    /// - `hash_key`: 单条查询的 Redis Hash field / 业务缓存字段。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>>;

    /// 业务作用：写入单条查询缓存。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `value`: 已由 Mapper codec 编码的查询结果 bytes。
    /// - `ttl_ms`: 可选毫秒级 TTL；`None` 表示不过期或由实现自行决定。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()>;

    /// 业务作用：删除单条查询缓存。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 需要删除的单条查询缓存字段。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()>;

    /// 业务作用：删除整个缓存组。
    ///
    /// # 参数
    /// - `key`: 需要整体清理的 Mapper cache namespace。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()>;

    /// 业务作用：读取缓存；未命中时加载源数据、写入缓存并返回已编码结果。
    ///
    /// 默认实现保持兼容：`get -> loader -> put`。需要防缓存击穿时可使用
    /// [`SingleFlightMapperL2Cache`] 包装具体缓存，或在业务自定义实现中重写该方法。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `ttl_ms`: 本次写入使用的毫秒级 TTL。
    /// - `loader`: cache miss 时执行的数据库查询和编码回调。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        if let Some(bytes) = self.get(key, hash_key).await? {
            return Ok(MapperCacheLoad::hit(bytes));
        }
        let bytes = loader().await?;
        self.put(key, hash_key, &bytes, ttl_ms).await?;
        Ok(MapperCacheLoad::loaded(bytes))
    }

    /// 业务作用：删除多个缓存组。某个 key 删除失败时仍继续尝试后续 key。
    ///
    /// # 参数
    /// - `keys`: 需要清理的 Mapper cache namespace 列表。
    async fn clear_keys(&self, keys: &[String]) -> anyhow::Result<()> {
        let mut first_error: Option<anyhow::Error> = None;
        for key in keys {
            if let Err(e) = self.clear_key(key).await {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        if let Some(e) = first_error {
            Err(e)
        } else {
            Ok(())
        }
    }
}

/// 给任意二级缓存增加进程内 single-flight 防击穿能力。
///
/// 它只合并同一进程内相同 `(key, hash_key)` 的并发 miss。分布式锁、跨进程互斥仍应
/// 由具体缓存实现或业务缓存网关决定。
pub struct SingleFlightMapperL2Cache {
    /// 被包装的真实二级缓存实现。
    inner: Arc<dyn MapperL2Cache>,
    /// 进程内按 `(key, hash_key)` 维度维护的互斥锁表。
    locks: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// single-flight 锁表清理守卫。
///
/// 加载 future 被取消时也会走 `Drop`，避免某个 cache key 的锁永久滞留在进程内。
struct SingleFlightLockCleanup<'a> {
    /// 所属锁表。
    locks: &'a StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// 当前 `(key, hash_key)` 拼出的锁表 key。
    lock_key: String,
    /// 当前调用持有的互斥锁引用。
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl Drop for SingleFlightLockCleanup<'_> {
    /// 业务作用：在最后一个等待者离开时移除锁表项。
    fn drop(&mut self) {
        let Ok(mut locks) = self.locks.lock() else {
            tracing::error!(
                component = "mapper",
                event = "single_flight_cleanup_error",
                "mapper single-flight lock table poisoned during cleanup"
            );
            return;
        };
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(&self.lock_key);
        }
    }
}

impl SingleFlightMapperL2Cache {
    /// 业务作用：包装已有二级缓存。
    ///
    /// # 参数
    /// - `inner`: 真实执行 `get/put/evict/clear_key` 的 L2 cache 实现。
    pub fn new(inner: Arc<dyn MapperL2Cache>) -> Self {
        Self {
            inner,
            locks: StdMutex::new(HashMap::new()),
        }
    }

    /// 业务作用：返回被包装的缓存。
    ///
    /// 该方法便于业务在启动诊断或组合多层 cache wrapper 时取回底层实现。
    pub fn inner(&self) -> Arc<dyn MapperL2Cache> {
        self.inner.clone()
    }
}

#[async_trait]
impl MapperL2Cache for SingleFlightMapperL2Cache {
    /// 业务作用：透传单条缓存读取。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key, hash_key).await
    }

    /// 业务作用：透传单条缓存写入。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `value`: 已编码缓存 value bytes。
    /// - `ttl_ms`: 本次写入使用的毫秒级 TTL。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        self.inner.put(key, hash_key, value, ttl_ms).await
    }

    /// 业务作用：透传单条缓存删除。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        self.inner.evict(key, hash_key).await
    }

    /// 业务作用：透传整个缓存组清理。
    ///
    /// # 参数
    /// - `key`: 需要清理的 Mapper cache namespace。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        self.inner.clear_key(key).await
    }

    /// 业务作用：在同进程内合并相同 key 的并发 cache miss。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `ttl_ms`: loader 写回缓存时使用的毫秒级 TTL。
    /// - `loader`: 只有抢到本地锁的调用会执行的数据库加载回调。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        // 先读一次缓存：大部分请求命中时不需要进入锁表，避免给热 key 增加互斥开销。
        if let Some(bytes) = self.inner.get(key, hash_key).await? {
            return Ok(MapperCacheLoad::hit(bytes));
        }

        // 同一个 `(key, hash_key)` 共用一把本地异步锁，只合并同进程内的击穿窗口。
        let lock_key = format!("{key}\0{hash_key}");
        let lock = {
            let mut locks = self
                .locks
                .lock()
                .map_err(|_| anyhow::anyhow!("mapper single-flight lock table poisoned"))?;
            locks
                .entry(lock_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };

        let cleanup = SingleFlightLockCleanup {
            locks: &self.locks,
            lock_key,
            lock,
        };
        let guard = cleanup.lock.lock().await;
        // 拿到锁后必须二次读取缓存：等待期间可能已有先到请求完成加载并写入。
        let result = match self.inner.get(key, hash_key).await {
            Ok(Some(bytes)) => Ok(MapperCacheLoad::hit_after_wait(bytes)),
            Ok(None) => match loader().await {
                Ok(bytes) => match self.inner.put(key, hash_key, &bytes, ttl_ms).await {
                    Ok(()) => Ok(MapperCacheLoad::loaded(bytes)),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        drop(guard);
        drop(cleanup);

        result
    }
}

/// Mapper 运行期指标类型。
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MapperMetricKind {
    /// 查询绕过二级缓存。
    CacheBypass,
    /// 二级缓存命中。
    CacheHit,
    /// 二级缓存等待同 key 加载后命中。
    CacheHitAfterWait,
    /// 二级缓存未命中。
    CacheMiss,
    /// 当前调用加载源数据并完成缓存写入。
    CacheLoad,
    /// 二级缓存 get-or-load 失败。
    CacheLoadError,
    /// 二级缓存写入成功。
    CachePut,
    /// 二级缓存 hash_key 构建失败。
    CacheHashKeyError,
    /// 二级缓存读取失败。
    CacheGetError,
    /// 二级缓存反序列化失败。
    CacheDecodeError,
    /// 二级缓存序列化失败。
    CacheEncodeError,
    /// 二级缓存写入失败。
    CachePutError,
}

/// 单条 Mapper 指标事件。
pub struct MapperMetric<'a> {
    /// 事件类型。
    pub kind: MapperMetricKind,
    /// Mapper cache key / namespace。
    pub mapper_key: &'a str,
    /// Redis Hash field 级 hash_key；cache bypass 或构建失败时可能为空。
    pub hash_key: Option<&'a str>,
    /// 规范化后的 prepared SQL；写操作清理类事件可能为空。
    pub sql: Option<&'a str>,
    /// 补充原因或错误摘要。
    pub detail: Option<&'a str>,
}

/// Mapper 指标入口。业务侧可接 Prometheus、日志、内部监控或诊断探针。
pub trait MapperMetrics: Send + Sync + 'static {
    /// 业务作用：记录单条指标事件。实现内部应避免 panic 和长阻塞。
    ///
    /// # 参数
    /// - `metric`: 由宏展开代码在 cache 命中、绕过、错误等路径上产生的指标事件。
    fn record(&self, metric: MapperMetric<'_>);
}

/// Mapper 查询结果缓存值 codec。
///
/// 该 trait 只负责 Redis Hash value 这层 bytes 编码，不参与 hash_key 生成。
/// hash_key 仍固定由 normalized SQL + 参数 JSON hash 派生，避免 codec 切换导致 key
/// 语义漂移。
pub trait MapperCacheCodec: Send + Sync + 'static {
    /// 业务作用：将 serde JSON value 编码为缓存 bytes。
    ///
    /// # 参数
    /// - `value`: Mapper 查询结果先序列化得到的 JSON value。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>>;

    /// 业务作用：将缓存 bytes 解码为 serde JSON value。
    ///
    /// # 参数
    /// - `bytes`: 从 L2 cache 读取到的原始 value bytes。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value>;

    /// 业务作用：已成功解码后是否建议把该缓存 value 回写为当前 codec 格式。
    ///
    /// 默认不回写。版本化 / fallback codec 可用它完成“读旧写新”的迁移闭环。
    ///
    /// # 参数
    /// - `_bytes`: 已经能成功解码的缓存 value bytes。
    fn should_rewrite_value(&self, _bytes: &[u8]) -> bool {
        false
    }
}

/// 默认 JSON codec，保持当前 Redis value 格式兼容。
#[derive(Debug, Default)]
pub struct JsonMapperCacheCodec;

impl MapperCacheCodec for JsonMapperCacheCodec {
    /// 业务作用：用 serde JSON 直接编码缓存 value，保持最朴素的可读格式。
    ///
    /// # 参数
    /// - `value`: Mapper 查询结果 JSON value。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    /// 业务作用：用 serde JSON 直接解码缓存 value。
    ///
    /// # 参数
    /// - `bytes`: Redis Hash value 或其它 L2 cache 中保存的 JSON bytes。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

const MAPPER_CODEC_MAGIC: &[u8] = b"namapper:codec:";
const MAPPER_CODEC_V1_PREFIX: &[u8] = b"namapper:codec:v1:";

/// 带版本前缀的 Mapper cache value codec。
///
/// 写入格式为 `namapper:codec:v1:<codec_name>:<payload>`。读取时：
/// - 匹配当前 `codec_name` 时用 primary codec 解码。
/// - 匹配已注册 fallback codec name 时用对应 fallback 解码。
/// - 无 `namapper:codec:` 前缀时默认按 legacy JSON 解码，兼容已有缓存。
pub struct VersionedMapperCacheCodec {
    /// 当前写入使用的 codec 名称，进入缓存 value 版本头。
    codec_name: String,
    /// 当前主 codec。
    codec: Arc<dyn MapperCacheCodec>,
    /// 可读取的历史 codec 列表。
    fallbacks: Vec<(String, Arc<dyn MapperCacheCodec>)>,
    /// 是否允许读取无版本头的历史 JSON value。
    read_legacy_json: bool,
}

impl VersionedMapperCacheCodec {
    /// 业务作用：创建一个带版本头的 cache value codec。
    ///
    /// # 参数
    /// - `codec_name`: 当前 codec 的稳定名称，只允许 ASCII 字母、数字、`-`、`_`、`.`。
    /// - `codec`: 实际负责编解码 payload 的主 codec。
    pub fn new(
        codec_name: impl Into<String>,
        codec: Arc<dyn MapperCacheCodec>,
    ) -> anyhow::Result<Self> {
        let codec_name = validate_mapper_codec_name(codec_name.into())?;
        Ok(Self {
            codec_name,
            codec,
            fallbacks: Vec::new(),
            read_legacy_json: true,
        })
    }

    /// 业务作用：注册一个具名历史 codec，用于读取旧缓存。
    ///
    /// # 参数
    /// - `codec_name`: 历史缓存 value 版本头中的 codec 名称。
    /// - `codec`: 能够解码该历史格式 payload 的 codec。
    pub fn with_named_fallback(
        mut self,
        codec_name: impl Into<String>,
        codec: Arc<dyn MapperCacheCodec>,
    ) -> anyhow::Result<Self> {
        let codec_name = validate_mapper_codec_name(codec_name.into())?;
        self.fallbacks.push((codec_name, codec));
        Ok(self)
    }

    /// 业务作用：关闭无版本头 JSON value 的兼容读取。
    ///
    /// 业务确认 Redis 中不再存在旧 JSON 缓存后可以调用，避免继续接受非当前格式数据。
    pub fn without_legacy_json_fallback(mut self) -> Self {
        self.read_legacy_json = false;
        self
    }
}

impl MapperCacheCodec for VersionedMapperCacheCodec {
    /// 业务作用：写入 `namapper:codec:v1:<codec_name>:` 版本头，再追加主 codec payload。
    ///
    /// # 参数
    /// - `value`: Mapper 查询结果 JSON value。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        let payload = self.codec.encode_value(value)?;
        let mut bytes = Vec::with_capacity(
            MAPPER_CODEC_V1_PREFIX.len() + self.codec_name.len() + 1 + payload.len(),
        );
        bytes.extend_from_slice(MAPPER_CODEC_V1_PREFIX);
        bytes.extend_from_slice(self.codec_name.as_bytes());
        bytes.push(b':');
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    /// 业务作用：根据缓存 value 版本头选择当前 codec、历史 codec 或 legacy JSON 路径。
    ///
    /// # 参数
    /// - `bytes`: 从 L2 cache 读取到的缓存 value bytes。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        if bytes.starts_with(MAPPER_CODEC_V1_PREFIX) {
            // v1 格式把 codec 名放在 payload 前，迁移时能精确找到对应历史解码器。
            let rest = &bytes[MAPPER_CODEC_V1_PREFIX.len()..];
            let Some(name_end) = rest.iter().position(|byte| *byte == b':') else {
                anyhow::bail!("mapper cache codec v1 payload missing codec name separator");
            };
            let codec_name = std::str::from_utf8(&rest[..name_end])
                .map_err(|err| anyhow::anyhow!("mapper cache codec name is not utf8: {err}"))?;
            let payload = &rest[name_end + 1..];
            if codec_name == self.codec_name {
                return self.codec.decode_value(payload);
            }
            for (name, codec) in &self.fallbacks {
                if codec_name == name {
                    return codec.decode_value(payload);
                }
            }
            anyhow::bail!("unknown mapper cache codec `{codec_name}`");
        }
        // 有 namapper codec magic 但不是已知版本时直接失败，避免误按 JSON 解析新协议。
        if bytes.starts_with(MAPPER_CODEC_MAGIC) {
            anyhow::bail!("unknown mapper cache codec version");
        }
        // 老缓存没有版本头，默认按 JSON 解码，保证升级过程不需要立即清空 Redis。
        if self.read_legacy_json {
            JsonMapperCacheCodec.decode_value(bytes)
        } else {
            self.codec.decode_value(bytes)
        }
    }

    /// 业务作用：判断旧格式 value 是否需要在命中后回写为当前 codec。
    ///
    /// # 参数
    /// - `bytes`: 已命中的缓存 value bytes。
    fn should_rewrite_value(&self, bytes: &[u8]) -> bool {
        if bytes.starts_with(MAPPER_CODEC_V1_PREFIX) {
            let rest = &bytes[MAPPER_CODEC_V1_PREFIX.len()..];
            let Some(name_end) = rest.iter().position(|byte| *byte == b':') else {
                return false;
            };
            return std::str::from_utf8(&rest[..name_end])
                .map(|codec_name| codec_name != self.codec_name)
                .unwrap_or(false);
        }
        self.read_legacy_json && !bytes.starts_with(MAPPER_CODEC_MAGIC)
    }
}

/// Mapper cache value 迁移用 fallback codec。
///
/// 写入永远使用 primary codec；读取时先尝试 primary，失败后按顺序尝试 fallback。
pub struct FallbackMapperCacheCodec {
    /// 写入路径和首选读取路径使用的 codec。
    primary: Arc<dyn MapperCacheCodec>,
    /// 主 codec 读取失败后按顺序尝试的兼容 codec。
    fallbacks: Vec<Arc<dyn MapperCacheCodec>>,
}

impl FallbackMapperCacheCodec {
    /// 业务作用：创建 fallback codec 组合。
    ///
    /// # 参数
    /// - `primary`: 当前写入和优先读取使用的 codec。
    pub fn new(primary: Arc<dyn MapperCacheCodec>) -> Self {
        Self {
            primary,
            fallbacks: Vec::new(),
        }
    }

    /// 业务作用：追加一个 fallback codec。
    ///
    /// # 参数
    /// - `fallback`: 主 codec 解码失败后尝试的历史 codec。
    pub fn with_fallback(mut self, fallback: Arc<dyn MapperCacheCodec>) -> Self {
        self.fallbacks.push(fallback);
        self
    }

    /// 业务作用：批量追加 fallback codec。
    ///
    /// # 参数
    /// - `fallbacks`: 按尝试顺序排列的历史 codec 集合。
    pub fn with_fallbacks<I>(mut self, fallbacks: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn MapperCacheCodec>>,
    {
        self.fallbacks.extend(fallbacks);
        self
    }
}

impl MapperCacheCodec for FallbackMapperCacheCodec {
    /// 业务作用：写入始终使用 primary codec，避免继续产生旧格式缓存。
    ///
    /// # 参数
    /// - `value`: Mapper 查询结果 JSON value。
    fn encode_value(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        self.primary.encode_value(value)
    }

    /// 业务作用：读取时先尝试 primary，再按顺序尝试 fallback。
    ///
    /// # 参数
    /// - `bytes`: 从 L2 cache 读取到的缓存 value bytes。
    fn decode_value(&self, bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
        let mut errors = Vec::new();
        // 主 codec 成功说明 value 已经是当前格式，直接返回。
        match self.primary.decode_value(bytes) {
            Ok(value) => return Ok(value),
            Err(err) => errors.push(format!("primary={err}")),
        }
        // 逐个尝试历史 codec，错误信息保留下来便于排查缓存迁移失败原因。
        for (idx, fallback) in self.fallbacks.iter().enumerate() {
            match fallback.decode_value(bytes) {
                Ok(value) => return Ok(value),
                Err(err) => errors.push(format!("fallback[{idx}]={err}")),
            }
        }
        anyhow::bail!("mapper cache codec decode failed: {}", errors.join("; "))
    }

    /// 业务作用：判断是否命中了 fallback 格式，需要读旧写新。
    ///
    /// # 参数
    /// - `bytes`: 已命中的缓存 value bytes。
    fn should_rewrite_value(&self, bytes: &[u8]) -> bool {
        if self.primary.decode_value(bytes).is_ok() {
            return false;
        }
        self.fallbacks
            .iter()
            .any(|fallback| fallback.decode_value(bytes).is_ok())
    }
}

/// 强类型 Mapper cache codec。
///
/// 这是 method-level 静态 codec 的低层接口，跳过 `serde_json::Value` 中间层。
/// 它不做 dyn object 抹平，由宏在具体 `#[Query]` 返回类型上单态化调用。
pub trait MapperTypedCacheCodec<T>: Send + Sync + 'static {
    /// 业务作用：编码具体业务返回类型。
    ///
    /// # 参数
    /// - `value`: `#[Query]` 方法返回的具体类型值。
    fn encode_typed(&self, value: &T) -> anyhow::Result<Vec<u8>>;

    /// 业务作用：解码具体业务返回类型。
    ///
    /// # 参数
    /// - `bytes`: 从 L2 cache 读取到的原始 value bytes。
    fn decode_typed(&self, bytes: &[u8]) -> anyhow::Result<T>;
}

/// 业务作用：校验 cache codec 名称是否能安全写入版本头。
///
/// # 参数
/// - `codec_name`: 业务传入的 codec 名称。
fn validate_mapper_codec_name(codec_name: String) -> anyhow::Result<String> {
    if codec_name.is_empty() {
        anyhow::bail!("mapper cache codec name must not be empty");
    }
    if codec_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(codec_name)
    } else {
        anyhow::bail!(
            "mapper cache codec name `{codec_name}` may only contain ASCII letters, digits, '-', '_' or '.'"
        );
    }
}

static DEFAULT_L2_CACHE: OnceLock<Arc<dyn MapperL2Cache>> = OnceLock::new();
static DEFAULT_MAPPER_METRICS: OnceLock<Arc<dyn MapperMetrics>> = OnceLock::new();
static DEFAULT_MAPPER_CACHE_CODEC: OnceLock<Arc<dyn MapperCacheCodec>> = OnceLock::new();

/// 业务作用：安装进程级默认 Mapper 二级缓存。
///
/// # 参数
/// - `cache`: 默认缓存实现。
pub fn set_default_l2_cache(cache: Arc<dyn MapperL2Cache>) -> anyhow::Result<()> {
    DEFAULT_L2_CACHE
        .set(cache)
        .map_err(|_| anyhow::anyhow!("mapper default L2 cache has already been installed"))
}

/// 业务作用：获取进程级默认 Mapper 二级缓存。
///
/// 返回 clone 后的 `Arc`，业务代码可以安全持有，不影响全局默认值生命周期。
pub fn default_l2_cache() -> Option<Arc<dyn MapperL2Cache>> {
    DEFAULT_L2_CACHE.get().cloned()
}

/// 业务作用：安装进程级 Mapper 指标入口。
///
/// # 参数
/// - `metrics`: 业务提供的指标实现，通常转接 Prometheus、日志或 APM。
pub fn set_default_mapper_metrics(metrics: Arc<dyn MapperMetrics>) -> anyhow::Result<()> {
    DEFAULT_MAPPER_METRICS
        .set(metrics)
        .map_err(|_| anyhow::anyhow!("mapper default metrics has already been installed"))
}

/// 业务作用：获取进程级 Mapper 指标入口。
///
/// 返回 clone 后的 `Arc`，宏展开代码通过它记录缓存命中、绕过和错误事件。
pub fn default_mapper_metrics() -> Option<Arc<dyn MapperMetrics>> {
    DEFAULT_MAPPER_METRICS.get().cloned()
}

/// 业务作用：安装进程级 Mapper cache value codec。
///
/// 必须在产生 cache value 之前调用。hash_key 生成不受 codec 影响。
///
/// # 参数
/// - `codec`: 业务指定的默认缓存 value 编解码器。
pub fn set_default_mapper_cache_codec(codec: Arc<dyn MapperCacheCodec>) -> anyhow::Result<()> {
    DEFAULT_MAPPER_CACHE_CODEC
        .set(codec)
        .map_err(|_| anyhow::anyhow!("mapper default cache codec has already been installed"))
}

/// 业务作用：获取进程级 Mapper cache value codec。
///
/// 未安装时运行时会回退到 [`JsonMapperCacheCodec`]。
pub fn default_mapper_cache_codec() -> Option<Arc<dyn MapperCacheCodec>> {
    DEFAULT_MAPPER_CACHE_CODEC.get().cloned()
}

/// 业务作用：编码 Mapper 查询结果缓存值。
///
/// # 参数
/// - `value`: `#[Query(cache = true)]` 返回值或返回值集合。
#[doc(hidden)]
pub fn encode_cache_value<T: serde::Serialize + ?Sized>(value: &T) -> anyhow::Result<Vec<u8>> {
    encode_cache_value_with_codec(value, None)
}

/// 业务作用：使用指定 codec 编码 Mapper 查询结果缓存值。
///
/// # 参数
/// - `value`: `#[Query(cache = true)]` 返回值或返回值集合。
/// - `codec`: 方法级 codec；为 `None` 时使用进程默认 codec 或 JSON codec。
#[doc(hidden)]
pub fn encode_cache_value_with_codec<T: serde::Serialize + ?Sized>(
    value: &T,
    codec: Option<&dyn MapperCacheCodec>,
) -> anyhow::Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    // 先选方法级 codec，再选全局默认 codec，最后回退 JSON，保证老项目零配置可用。
    match codec.or_else(|| DEFAULT_MAPPER_CACHE_CODEC.get().map(|codec| codec.as_ref())) {
        Some(codec) => codec.encode_value(&value),
        None => JsonMapperCacheCodec.encode_value(&value),
    }
}

/// 业务作用：解码 Mapper 查询结果缓存值。
///
/// # 参数
/// - `bytes`: 从 L2 cache 读出的原始 value bytes。
#[doc(hidden)]
pub fn decode_cache_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    decode_cache_value_with_codec(bytes, None)
}

/// 业务作用：使用指定 codec 解码 Mapper 查询结果缓存值。
///
/// # 参数
/// - `bytes`: 从 L2 cache 读出的原始 value bytes。
/// - `codec`: 方法级 codec；为 `None` 时使用进程默认 codec 或 JSON codec。
#[doc(hidden)]
pub fn decode_cache_value_with_codec<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    codec: Option<&dyn MapperCacheCodec>,
) -> anyhow::Result<T> {
    let value = match codec.or_else(|| DEFAULT_MAPPER_CACHE_CODEC.get().map(|codec| codec.as_ref()))
    {
        Some(codec) => codec.decode_value(bytes)?,
        None => JsonMapperCacheCodec.decode_value(bytes)?,
    };
    // 宏调用点知道具体返回类型，这里再从通用 JSON value 还原为业务类型。
    Ok(serde_json::from_value(value)?)
}

/// 业务作用：使用强类型 codec 编码 Mapper 查询结果缓存值。
///
/// # 参数
/// - `value`: 具体业务返回类型值。
/// - `codec`: 方法级强类型 codec。
#[doc(hidden)]
pub fn encode_typed_cache_value<T, C>(value: &T, codec: &C) -> anyhow::Result<Vec<u8>>
where
    C: MapperTypedCacheCodec<T> + ?Sized,
{
    codec.encode_typed(value)
}

/// 业务作用：使用强类型 codec 解码 Mapper 查询结果缓存值。
///
/// # 参数
/// - `bytes`: 从 L2 cache 读出的原始 value bytes。
/// - `codec`: 方法级强类型 codec。
#[doc(hidden)]
pub fn decode_typed_cache_value<T, C>(bytes: &[u8], codec: &C) -> anyhow::Result<T>
where
    C: MapperTypedCacheCodec<T> + ?Sized,
{
    codec.decode_typed(bytes)
}

/// 业务作用：判断命中的缓存 value 是否应按当前 codec 回写。
///
/// # 参数
/// - `bytes`: 已命中的缓存 value bytes。
/// - `codec`: 方法级 codec；为 `None` 时使用进程默认 codec。
#[doc(hidden)]
pub fn cache_value_needs_rewrite(bytes: &[u8], codec: Option<&dyn MapperCacheCodec>) -> bool {
    codec
        .or_else(|| DEFAULT_MAPPER_CACHE_CODEC.get().map(|codec| codec.as_ref()))
        .map(|codec| codec.should_rewrite_value(bytes))
        .unwrap_or(false)
}

/// 业务作用：宏展开调用的指标记录入口。
///
/// # 参数
/// - `metric`: 本次 Mapper cache 或 SQL 路径产生的指标事件。
#[doc(hidden)]
pub fn record_mapper_metric(metric: MapperMetric<'_>) {
    if let Some(metrics) = DEFAULT_MAPPER_METRICS.get() {
        metrics.record(metric);
    }
}

/// 业务作用：生产启动检查：存在默认缓存查询时，必须已经安装默认 L2 cache。
///
/// 该函数用于应用启动期 fail-fast，避免 `cache = true` 查询在生产环境静默绕过缓存。
///
pub fn assert_l2_cache_installed_for_cached_queries() -> anyhow::Result<()> {
    let has_cached_query = MAPPER_CACHE_META.iter().any(|meta| meta.has_cached_query);
    if has_cached_query && DEFAULT_L2_CACHE.get().is_none() {
        return Err(anyhow::anyhow!(
            "mapper has cache-enabled queries but default L2 cache is not installed"
        ));
    }
    Ok(())
}

/// 业务作用：当前是否处于 ambient 事务中。
///
/// Mapper 宏用它决定查询是否要走事务连接，以及是否允许业务显式启用事务内 L2 cache。
pub fn in_transaction() -> bool {
    natx::in_transaction()
}

/// 业务作用：当前 ambient 事务所属 datasource；无事务时返回 `None`。
pub fn current_datasource() -> Option<&'static str> {
    natx::current_datasource()
}

/// 业务作用：获取指定 datasource 的连接池 clone。
///
/// 该入口不加入 ambient 事务，主要给 `#[StreamQuery]` 这类需要拥有连接池生命周期的
/// 无事务流式查询使用。
///
/// # 参数
/// - `datasource`: `#[Mapper(datasource = "...")]` 指定的数据源名称。
pub fn pool_for(datasource: &'static str) -> anyhow::Result<sqlx::MySqlPool> {
    natx::pool_for_datasource(datasource)
}

/// 业务作用：获取当前 Mapper SQL 执行连接。
///
/// 无 ambient 事务时会从默认 datasource 连接池取连接；事务内会复用当前事务连接。
pub async fn conn() -> anyhow::Result<natx::Conn> {
    natx::conn().await
}

/// 业务作用：从指定 datasource 获取 Mapper SQL 执行连接。
///
/// # 参数
/// - `datasource`: `#[Mapper(datasource = "...")]` 指定的数据源名称。
pub async fn conn_for(datasource: &'static str) -> anyhow::Result<natx::Conn> {
    natx::conn_for(datasource).await
}

/// 业务作用：获取必须处于事务中的 Mapper SQL 执行连接。
///
/// 该入口用于 `tx = true` 的 Mapper 方法，事务缺失时立即报错。
pub async fn mandatory_conn() -> anyhow::Result<natx::Conn> {
    natx::mandatory_conn().await
}

/// 业务作用：从指定 datasource 获取必须处于事务中的 Mapper SQL 执行连接。
///
/// # 参数
/// - `datasource`: `#[Mapper(datasource = "...")]` 指定的数据源名称。
pub async fn mandatory_conn_for(datasource: &'static str) -> anyhow::Result<natx::Conn> {
    natx::mandatory_conn_for(datasource).await
}

/// 业务作用：根据当前 key 和关联元数据计算需要清理的缓存组。
///
/// # 参数
/// - `source_key`: 当前写操作触发清理的源 key。
/// - `flush_refs`: 是否展开关联 key。
pub fn cache_clear_targets(source_key: &str, flush_refs: bool) -> Vec<String> {
    let mut targets = HashSet::new();
    targets.insert(source_key.to_string());

    if flush_refs {
        // 写操作声明 flush_refs 时，既清理本 key 主动关联的 key，也清理依赖本 key 的反向 key。
        for meta in MAPPER_CACHE_META.iter() {
            if meta.key == source_key {
                targets.extend(meta.clear_also.iter().map(|key| (*key).to_string()));
            }
            if meta.clear_when.contains(&source_key) {
                targets.insert(meta.key.to_string());
            }
        }
    }

    let mut targets: Vec<String> = targets.into_iter().collect();
    // 排序让日志与指标输出稳定，便于排查跨 key 缓存清理问题。
    targets.sort();
    targets
}

/// 业务作用：在无事务时立即清理缓存；在事务内注册 commit 后清理。
///
/// # 参数
/// - `cache`: 当前 client 注入的缓存实现。
/// - `keys`: 需要清理的缓存组。
pub async fn clear_after_commit_or_now(
    cache: Option<Arc<dyn MapperL2Cache>>,
    keys: Vec<String>,
) -> anyhow::Result<()> {
    let Some(cache) = cache else {
        tracing::debug!(
            component = "mapper",
            event = "cache_clear_bypass",
            reason = "no_l2_cache",
            keys = ?keys,
            "mapper cache clear bypass"
        );
        return Ok(());
    };
    if natx::in_transaction() {
        // 事务内不能提前清缓存，否则 rollback 后会造成缓存被误删；因此注册 after_commit。
        tracing::debug!(
            component = "mapper",
            event = "cache_clear_deferred",
            keys = ?keys,
            "mapper cache clear deferred until transaction commit"
        );
        natx::after_commit(move || async move {
            match cache.clear_keys(&keys).await {
                Ok(()) => {
                    tracing::debug!(
                        component = "mapper",
                        event = "cache_clear_after_commit",
                        keys = ?keys,
                        "mapper cache clear after commit"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        component = "mapper",
                        event = "cache_clear_after_commit_error",
                        keys = ?keys,
                        error = %e,
                        "mapper cache clear after commit failed"
                    );
                }
            }
        })?;
        Ok(())
    } else {
        // 无事务时写操作已经完成，可以立即清理，保证后续读请求不会命中过期数据。
        match cache.clear_keys(&keys).await {
            Ok(()) => {
                tracing::debug!(
                    component = "mapper",
                    event = "cache_clear_now",
                    keys = ?keys,
                    "mapper cache clear"
                );
            }
            Err(e) => {
                tracing::error!(
                    component = "mapper",
                    event = "cache_clear_error",
                    keys = ?keys,
                    error = %e,
                    "mapper cache clear failed"
                );
            }
        }
        Ok(())
    }
}

/// Redis Hash 版 Mapper 二级缓存适配器。
///
/// 数据结构按 namespace/cache-key 拆分二级缓存身份：
/// - Redis key = mapper key / namespace；
/// - Redis hash field = `sql:{normalized_sql}:...` 形式的 hash_key；
/// - Redis hash value = mapper 运行时序列化后的查询结果 bytes。
///
/// `cache_ttl_ms` 优先使用 Redis Hash field 级过期；如果 Redis 端不支持
/// `HPEXPIRE`，运行时会自动降级为整个 Redis Hash key 的 `PEXPIRE`。
#[cfg(feature = "redis-cache")]
pub struct RedisMapperL2Cache {
    redis: redis::cluster_async::ClusterConnection,
    ttl_mode: AtomicU8,
}

#[cfg(feature = "redis-cache")]
impl RedisMapperL2Cache {
    /// 业务作用：创建 Redis Hash 版 Mapper 二级缓存。
    ///
    /// # 参数
    /// - `redis`: Redis Cluster 异步连接；内部按请求 clone 连接句柄。
    pub fn new(redis: redis::cluster_async::ClusterConnection) -> Self {
        Self {
            redis,
            ttl_mode: AtomicU8::new(REDIS_TTL_MODE_UNKNOWN),
        }
    }

    /// 业务作用：生产启动期探测 Redis 是否真正支持 Hash field 级 TTL。
    ///
    /// 默认 `put(..., Some(ttl_ms))` 会在 Redis 不支持 `HPEXPIRE` 时降级为 key 级
    /// `PEXPIRE`，保证旧环境可用。若业务要求必须具备 per-field TTL，请在启动阶段
    /// 调用本方法；它会写入一个短生命周期探测 field，验证 `HPEXPIRE` + `HPTTL`
    /// 成功且没有给整个 Redis key 设置 TTL。
    ///
    pub async fn assert_hash_field_ttl_supported(&self) -> anyhow::Result<()> {
        assert_redis_hash_field_ttl_supported(self.redis.clone()).await
    }

    /// 业务作用：为刚写入的 Redis Hash field 设置 TTL，必要时降级为整个 key 的 TTL。
    ///
    /// # 参数
    /// - `conn`: 当前请求使用的 Redis Cluster 连接。
    /// - `key`: Mapper cache namespace 对应的 Redis key。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `ttl_ms`: 业务配置的毫秒级 TTL。
    async fn apply_ttl(
        &self,
        conn: &mut redis::cluster_async::ClusterConnection,
        key: &str,
        hash_key: &str,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        let ttl_mode = self.ttl_mode.load(Ordering::Relaxed);
        if ttl_mode != REDIS_TTL_MODE_KEY {
            // 优先使用 Redis 7.4+ 的 Hash field TTL，避免同一个 mapper key 下其它 field 被一起过期。
            match redis_hpexpire_field(conn, key, hash_key, ttl_ms).await {
                Ok(()) => {
                    self.ttl_mode.store(REDIS_TTL_MODE_FIELD, Ordering::Relaxed);
                    return Ok(());
                }
                Err(err)
                    if ttl_mode == REDIS_TTL_MODE_UNKNOWN
                        && redis_error_is_hpexpire_unsupported(&err) =>
                {
                    self.ttl_mode.store(REDIS_TTL_MODE_KEY, Ordering::Relaxed);
                }
                Err(err) => return Err(err.into()),
            }
        }
        // 旧 Redis 不支持 HPEXPIRE 时退到 PEXPIRE，保证缓存能力可用但 TTL 粒度变粗。
        redis::cmd("PEXPIRE")
            .arg(key)
            .arg(ttl_ms)
            .query_async::<()>(&mut *conn)
            .await?;
        Ok(())
    }
}

/// 业务作用：生产启动期探测 Redis Cluster 连接是否支持 Hash field 级 TTL。
///
/// 该函数只做能力探测，不安装任何 Mapper 默认缓存。探测成功说明当前 Redis 端支持
/// `HPEXPIRE` / `HPTTL`，可安全依赖 `RedisMapperL2Cache` 的 per-field TTL 语义。
///
/// # 参数
/// - `redis`: 用于执行探测命令的 Redis Cluster 异步连接。
#[cfg(feature = "redis-cache")]
pub async fn assert_redis_hash_field_ttl_supported(
    redis: redis::cluster_async::ClusterConnection,
) -> anyhow::Result<()> {
    let mut conn = redis;
    let probe_key = redis_hash_field_ttl_probe_key();
    let probe_field = "probe";
    let ttl_ms = 60_000_u64;

    // 先删除同名探测 key，保证后续 HPTTL/TTL 判断不受历史残留影响。
    redis::cmd("DEL")
        .arg(&probe_key)
        .query_async::<()>(&mut conn)
        .await?;
    redis::cmd("HSET")
        .arg(&probe_key)
        .arg(probe_field)
        .arg("1")
        .query_async::<()>(&mut conn)
        .await?;

    let probe_result = async {
        redis_hpexpire_field(&mut conn, &probe_key, probe_field, ttl_ms).await?;
        // HPTTL 返回正数才说明 field 级 TTL 真的生效。
        let field_ttl = redis::cmd("HPTTL")
            .arg(&probe_key)
            .arg("FIELDS")
            .arg(1)
            .arg(probe_field)
            .query_async::<Vec<i64>>(&mut conn)
            .await?;
        if field_ttl.len() != 1 || field_ttl[0] <= 0 {
            return Err(anyhow::anyhow!(
                "Redis HPTTL did not report positive hash field TTL: {field_ttl:?}"
            ));
        }
        // field TTL 不应给整个 Hash key 设置 TTL，否则会影响同组其它查询缓存。
        let key_ttl = redis::cmd("TTL")
            .arg(&probe_key)
            .query_async::<i64>(&mut conn)
            .await?;
        if key_ttl != -1 {
            return Err(anyhow::anyhow!(
                "Redis Hash field TTL probe unexpectedly set key TTL: {key_ttl}"
            ));
        }
        Ok(())
    }
    .await;

    // 无论探测成败都尽量清理探测 key，避免污染业务 Redis。
    let cleanup_result = redis::cmd("DEL")
        .arg(&probe_key)
        .query_async::<()>(&mut conn)
        .await;
    match (probe_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), _) => Err(anyhow::anyhow!(
            "Redis Hash field TTL is required but not available: {err}; \
                 use a Redis 7.4+ compatible cluster or disable the strict startup check"
        )),
        (Ok(()), Err(err)) => Err(err.into()),
    }
}

#[cfg(feature = "redis-cache")]
const REDIS_TTL_MODE_UNKNOWN: u8 = 0;
#[cfg(feature = "redis-cache")]
const REDIS_TTL_MODE_FIELD: u8 = 1;
#[cfg(feature = "redis-cache")]
const REDIS_TTL_MODE_KEY: u8 = 2;

/// 业务作用：对单个 Redis Hash field 执行毫秒级过期。
///
/// # 参数
/// - `conn`: 当前请求使用的 Redis Cluster 连接。
/// - `key`: Redis Hash key，也就是 Mapper cache namespace。
/// - `hash_key`: Redis Hash field，也就是单条查询缓存字段。
/// - `ttl_ms`: 需要设置的毫秒级 TTL。
#[cfg(feature = "redis-cache")]
async fn redis_hpexpire_field(
    conn: &mut redis::cluster_async::ClusterConnection,
    key: &str,
    hash_key: &str,
    ttl_ms: u64,
) -> redis::RedisResult<()> {
    // Redis HPEXPIRE field 模式返回每个 field 的设置结果；这里只有一个 field。
    let result = redis::cmd("HPEXPIRE")
        .arg(key)
        .arg(ttl_ms)
        .arg("FIELDS")
        .arg(1)
        .arg(hash_key)
        .query_async::<Vec<i64>>(conn)
        .await?;
    match result.as_slice() {
        [1] => Ok(()),
        [_] => Err(redis::RedisError::from((
            redis::ErrorKind::UnexpectedReturnType,
            "HPEXPIRE did not set hash field TTL",
        ))),
        _ => Err(redis::RedisError::from((
            redis::ErrorKind::UnexpectedReturnType,
            "invalid HPEXPIRE response",
        ))),
    }
}

/// 业务作用：判断 Redis 错误是否表示当前服务端不支持 `HPEXPIRE`。
///
/// # 参数
/// - `err`: Redis 命令返回的错误。
#[cfg(feature = "redis-cache")]
fn redis_error_is_hpexpire_unsupported(err: &redis::RedisError) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("unknown command")
        || message.contains("unsupported command")
        || message.contains("unknown redis command")
}

#[cfg(feature = "redis-cache")]
static REDIS_FIELD_TTL_PROBE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// 业务作用：生成 Redis Hash field TTL 探测用 key。
///
/// key 中包含进程号、纳秒时间和进程内递增序号，避免并发启动的多个服务实例互相干扰。
#[cfg(feature = "redis-cache")]
fn redis_hash_field_ttl_probe_key() -> String {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = REDIS_FIELD_TTL_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "namapper:hash-field-ttl-probe:{}:{now_nanos}:{counter}",
        std::process::id(),
    )
}

#[cfg(feature = "redis-cache")]
#[async_trait]
impl MapperL2Cache for RedisMapperL2Cache {
    /// 业务作用：从 Redis Hash 读取单条 Mapper 查询缓存。
    ///
    /// # 参数
    /// - `key`: Redis Hash key，也就是 Mapper cache namespace。
    /// - `hash_key`: Redis Hash field，也就是单条查询缓存字段。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        use redis::AsyncCommands;

        let mut conn = self.redis.clone();
        Ok(conn.hget(key, hash_key).await?)
    }

    /// 业务作用：写入 Redis Hash field，并按需设置 TTL。
    ///
    /// # 参数
    /// - `key`: Redis Hash key，也就是 Mapper cache namespace。
    /// - `hash_key`: Redis Hash field，也就是单条查询缓存字段。
    /// - `value`: 已编码的 Mapper 查询结果 bytes。
    /// - `ttl_ms`: 可选毫秒级 TTL。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        use redis::AsyncCommands;

        if let Some(ttl_ms) = ttl_ms {
            validate_mapper_ttl(ttl_ms)?;
        }
        let mut conn = self.redis.clone();
        conn.hset::<_, _, _, ()>(key, hash_key, value).await?;
        if let Some(ttl_ms) = ttl_ms {
            // TTL 写在 value 之后，确保成功写入的 field 才进入过期策略。
            self.apply_ttl(&mut conn, key, hash_key, ttl_ms).await?;
        }
        Ok(())
    }

    /// 业务作用：删除 Redis Hash 中的单个查询缓存 field。
    ///
    /// # 参数
    /// - `key`: Redis Hash key，也就是 Mapper cache namespace。
    /// - `hash_key`: Redis Hash field，也就是单条查询缓存字段。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        use redis::AsyncCommands;

        let mut conn = self.redis.clone();
        conn.hdel::<_, _, ()>(key, hash_key).await?;
        Ok(())
    }

    /// 业务作用：删除整个 Redis Hash，清理当前 Mapper cache namespace 下所有查询缓存。
    ///
    /// # 参数
    /// - `key`: Redis Hash key，也就是 Mapper cache namespace。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        use redis::AsyncCommands;

        let mut conn = self.redis.clone();
        conn.del::<_, ()>(key).await?;
        Ok(())
    }
}

/// Redis 分布式 single-flight 包装器。
///
/// 它在具体 L2 cache 之外增加跨进程短锁：同一 `(key, hash_key)` 的并发 miss 中，
/// 一个调用方获得 Redis 锁并执行 loader，其它调用方轮询底层 cache，命中后返回
/// `HitAfterWait`。锁只保护击穿窗口，不改变 `get/put/evict/clear_key` 的存储语义。
#[cfg(feature = "redis-cache")]
pub struct RedisDistributedSingleFlightMapperL2Cache {
    /// 被包装的真实 L2 cache，负责缓存存取。
    inner: Arc<dyn MapperL2Cache>,
    /// 用于跨进程短锁的 Redis Cluster 连接。
    redis: redis::cluster_async::ClusterConnection,
    /// 分布式锁自动过期时间，防止持锁进程崩溃后永久阻塞。
    lock_ttl_ms: u64,
    /// 等待者单轮轮询缓存的最长时间。
    wait_timeout_ms: u64,
    /// 等待者轮询底层缓存的间隔。
    poll_interval_ms: u64,
}

#[cfg(feature = "redis-cache")]
impl RedisDistributedSingleFlightMapperL2Cache {
    /// 业务作用：包装已有二级缓存，并使用同一个 Redis 连接做跨进程短锁。
    ///
    /// # 参数
    /// - `inner`: 真实执行缓存读写的 L2 cache 实现。
    /// - `redis`: 用于 `SET NX PX` 和释放锁 Lua 脚本的 Redis Cluster 连接。
    pub fn new(
        inner: Arc<dyn MapperL2Cache>,
        redis: redis::cluster_async::ClusterConnection,
    ) -> Self {
        Self {
            inner,
            redis,
            lock_ttl_ms: 5_000,
            wait_timeout_ms: 5_000,
            poll_interval_ms: 25,
        }
    }

    /// 业务作用：设置 Redis 锁 TTL。过短会让慢 loader 重复执行，过长会放大进程崩溃后的等待。
    ///
    /// # 参数
    /// - `lock_ttl_ms`: 分布式锁过期时间，单位毫秒；小于 1 时按 1 处理。
    pub fn with_lock_ttl_ms(mut self, lock_ttl_ms: u64) -> Self {
        self.lock_ttl_ms = bounded_mapper_millis(lock_ttl_ms);
        self
    }

    /// 业务作用：设置单轮等待时间；超时后会再次尝试抢锁。
    ///
    /// # 参数
    /// - `wait_timeout_ms`: 等待者单轮最大等待时间，单位毫秒；小于 1 时按 1 处理。
    pub fn with_wait_timeout_ms(mut self, wait_timeout_ms: u64) -> Self {
        self.wait_timeout_ms = bounded_mapper_millis(wait_timeout_ms);
        self
    }

    /// 业务作用：设置等待者轮询底层 cache 的间隔。
    ///
    /// # 参数
    /// - `poll_interval_ms`: 等待期间读取底层 cache 的间隔，单位毫秒；小于 1 时按 1 处理。
    pub fn with_poll_interval_ms(mut self, poll_interval_ms: u64) -> Self {
        self.poll_interval_ms = bounded_mapper_millis(poll_interval_ms);
        self
    }

    /// 业务作用：返回被包装的缓存。
    ///
    /// 该方法便于启动诊断或组合 wrapper 时访问底层缓存实现。
    pub fn inner(&self) -> Arc<dyn MapperL2Cache> {
        self.inner.clone()
    }
}

#[cfg(feature = "redis-cache")]
#[async_trait]
impl MapperL2Cache for RedisDistributedSingleFlightMapperL2Cache {
    /// 业务作用：透传单条缓存读取。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    async fn get(&self, key: &str, hash_key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key, hash_key).await
    }

    /// 业务作用：透传单条缓存写入。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `value`: 已编码缓存 value bytes。
    /// - `ttl_ms`: 本次写入使用的毫秒级 TTL。
    async fn put(
        &self,
        key: &str,
        hash_key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        self.inner.put(key, hash_key, value, ttl_ms).await
    }

    /// 业务作用：透传单条缓存删除。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    async fn evict(&self, key: &str, hash_key: &str) -> anyhow::Result<()> {
        self.inner.evict(key, hash_key).await
    }

    /// 业务作用：透传整个缓存组清理。
    ///
    /// # 参数
    /// - `key`: 需要清理的 Mapper cache namespace。
    async fn clear_key(&self, key: &str) -> anyhow::Result<()> {
        self.inner.clear_key(key).await
    }

    /// 业务作用：用 Redis 短锁合并跨进程相同 key 的并发 cache miss。
    ///
    /// # 参数
    /// - `key`: Mapper cache namespace。
    /// - `hash_key`: 单条查询缓存字段。
    /// - `ttl_ms`: loader 写回缓存时使用的毫秒级 TTL。
    /// - `loader`: 只有获得 Redis 锁的调用会执行的数据库加载回调。
    async fn get_or_load(
        &self,
        key: &str,
        hash_key: &str,
        ttl_ms: Option<u64>,
        loader: MapperCacheLoader<'_>,
    ) -> anyhow::Result<MapperCacheLoad> {
        // 第一跳直接读缓存，命中时完全不参与 Redis 锁竞争。
        if let Some(bytes) = self.inner.get(key, hash_key).await? {
            return Ok(MapperCacheLoad::hit(bytes));
        }

        let mut loader = Some(loader);
        let lock_key = distributed_single_flight_lock_key(key, hash_key);
        let poll_interval = Duration::from_millis(self.poll_interval_ms.max(1));
        let wait_timeout = Duration::from_millis(self.wait_timeout_ms.max(1));

        loop {
            let token = distributed_single_flight_token();
            let mut conn = self.redis.clone();
            // 每轮都重新尝试抢锁；等待超时通常意味着持锁方慢、崩溃或锁 TTL 已过。
            if redis_try_acquire_single_flight(
                &mut conn,
                &lock_key,
                &token,
                self.lock_ttl_ms.max(1),
            )
            .await?
            {
                let result = async {
                    // 抢到锁后仍二次读缓存，避免刚好在抢锁前其它进程已写入。
                    if let Some(bytes) = self.inner.get(key, hash_key).await? {
                        return Ok(MapperCacheLoad::hit_after_wait(bytes));
                    }
                    let loader = loader.take().ok_or_else(|| {
                        anyhow::anyhow!(
                            "mapper distributed single-flight loader was already consumed"
                        )
                    })?;
                    let bytes = loader().await?;
                    self.inner.put(key, hash_key, &bytes, ttl_ms).await?;
                    Ok(MapperCacheLoad::loaded(bytes))
                }
                .await;
                // 使用 token 校验释放锁，避免删除已经被其它进程重新获得的锁。
                if let Err(err) = redis_release_single_flight(&mut conn, &lock_key, &token).await {
                    tracing::warn!(
                        component = "mapper",
                        event = "distributed_single_flight_release_error",
                        lock_key = %lock_key,
                        error = %err,
                        "mapper distributed single-flight release failed"
                    );
                }
                return result;
            }

            let wait_started = tokio::time::Instant::now();
            // 未获得锁的调用不打数据库，只轮询底层 cache 等待持锁方写入。
            while wait_started.elapsed() < wait_timeout {
                tokio::time::sleep(poll_interval).await;
                if let Some(bytes) = self.inner.get(key, hash_key).await? {
                    return Ok(MapperCacheLoad::hit_after_wait(bytes));
                }
            }
        }
    }
}

/// 业务作用：生成分布式 single-flight 锁 key。
///
/// # 参数
/// - `key`: Mapper cache namespace。
/// - `hash_key`: 单条查询缓存字段。
#[cfg(feature = "redis-cache")]
fn distributed_single_flight_lock_key(key: &str, hash_key: &str) -> String {
    let mut raw = String::with_capacity(key.len() + hash_key.len() + 1);
    raw.push_str(key);
    raw.push('\0');
    raw.push_str(hash_key);
    format!("namapper:singleflight:{}", sha256_hex(raw.as_bytes()))
}

#[cfg(feature = "redis-cache")]
static REDIS_SINGLE_FLIGHT_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

/// 业务作用：生成分布式锁持有者 token。
///
/// token 由进程号、时间戳和进程内序号组成，用于释放锁时确认“锁仍属于自己”。
#[cfg(feature = "redis-cache")]
fn distributed_single_flight_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = REDIS_SINGLE_FLIGHT_TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}:{nanos}:{counter}", std::process::id())
}

/// 业务作用：尝试获取 Redis 分布式 single-flight 锁。
///
/// # 参数
/// - `conn`: 当前请求使用的 Redis Cluster 连接。
/// - `lock_key`: 由 `(key, hash_key)` 派生出的锁 key。
/// - `token`: 当前调用方的锁持有者 token。
/// - `lock_ttl_ms`: 锁自动过期时间，单位毫秒。
#[cfg(feature = "redis-cache")]
async fn redis_try_acquire_single_flight(
    conn: &mut redis::cluster_async::ClusterConnection,
    lock_key: &str,
    token: &str,
    lock_ttl_ms: u64,
) -> redis::RedisResult<bool> {
    // SET NX PX 原子地完成“仅不存在时加锁”和“锁自动过期”。
    let response = redis::cmd("SET")
        .arg(lock_key)
        .arg(token)
        .arg("NX")
        .arg("PX")
        .arg(lock_ttl_ms)
        .query_async::<Option<String>>(conn)
        .await?;
    Ok(response.is_some())
}

/// 业务作用：释放 Redis 分布式 single-flight 锁。
///
/// # 参数
/// - `conn`: 当前请求使用的 Redis Cluster 连接。
/// - `lock_key`: 由 `(key, hash_key)` 派生出的锁 key。
/// - `token`: 当前调用方加锁时写入的持有者 token。
#[cfg(feature = "redis-cache")]
async fn redis_release_single_flight(
    conn: &mut redis::cluster_async::ClusterConnection,
    lock_key: &str,
    token: &str,
) -> redis::RedisResult<()> {
    // Lua 脚本把“检查 token”和“删除锁”放进同一个 Redis 原子操作。
    let script = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"#;
    redis::cmd("EVAL")
        .arg(script)
        .arg(1)
        .arg(lock_key)
        .arg(token)
        .query_async::<i64>(conn)
        .await?;
    Ok(())
}

/// 宏展开专用第三方依赖桥。
#[doc(hidden)]
pub mod __private {
    pub use super::{
        apply_sql_trim, normalize_sql_whitespace, sql_in_placeholders, write_mapper_order_by_clause,
    };
    pub use anyhow;
    pub use async_stream;
    pub use async_trait;
    pub use futures_util;
    pub use linkme;
    pub use natx;
    pub use serde;
    pub use serde_json;
    pub use sqlx;
    pub use tracing;
}
