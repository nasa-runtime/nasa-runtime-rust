//! SQL mapper 运行时与公共类型。
//!
//! 提供 mapper 宏展开依赖的连接获取、事务桥接、枚举/排序辅助和可选 Redis L2 缓存适配能力。
//! 所有生成方法通过固定原子单元分别记录逻辑调用与数据库结果；受管应用由 YAML 装配日志、
//! 开发参数显示、通知及指标出口，默认不输出 bind 值，通知失败不改变 SQL 或事务结果。
//! 原始 SQL 耗时达到或超过有效阈值时命中慢规则，连接等待与 Stream 消费者处理另计。
//! 业务初始化 `Notify`、启用告警并设 `cooldown_ms=0` 后逐条尝试入队，不依赖慢日志开关；
//! 发送由受管 worker 完成，缺失实现忽略，容量或投递失败不构成业务失败。
#![forbid(unsafe_code)]

use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub use async_trait::async_trait;
pub use namapper_core::observability;
pub use natx::{mapper_conn_for, mapper_mandatory_conn_for, mapper_never_conn_for};

mod observation;
pub use namapper_core::{
    apply_sql_trim, assert_l2_cache_installed_for_cached_queries, batch_chunks,
    cache_clear_targets, cache_hash_key, cache_hash_key_with_suffix, cache_value_needs_rewrite,
    decode_cache_value, decode_cache_value_with_codec, decode_typed_cache_value, default_l2_cache,
    default_mapper_cache_codec, default_mapper_metrics, encode_cache_value,
    encode_cache_value_with_codec, encode_typed_cache_value, normalize_sql_whitespace,
    record_mapper_metric, set_default_l2_cache, set_default_mapper_cache_codec,
    set_default_mapper_metrics, write_mapper_order_by_clause, CacheArg, FallbackMapperCacheCodec,
    JsonMapperCacheCodec, MapperBatchChunks, MapperCacheCodec, MapperCacheLoad,
    MapperCacheLoadFuture, MapperCacheLoadState, MapperCacheLoader, MapperCacheMeta, MapperEnum,
    MapperL2Cache, MapperMetric, MapperMetricKind, MapperMetrics, MapperOrderBy, MapperOrderField,
    MapperTypedCacheCodec, OrderBy, OrderDirection, PageRequest, SingleFlightMapperL2Cache,
    VersionedMapperCacheCodec, MAPPER_CACHE_META,
};
#[cfg(feature = "redis-cache")]
pub use namapper_core::{
    assert_redis_hash_field_ttl_supported, RedisDistributedSingleFlightMapperL2Cache,
    RedisMapperL2Cache,
};
pub use namapper_macro::{
    Delete, Execute, Insert, Mapper, MapperEnum, MapperOrderField, Query, StreamQuery, Update,
};
pub use observation::{classify_method_result, classify_sqlx_result};
pub use sqlx::types::Json;

/// Mapper 流式查询返回类型。
///
/// `MapperStream` 拥有底层 stream。事务外由生成代码持有 datasource 的池连接；
/// `#[transactional]` ambient 事务内则持有事务连接的槽锁直到流被消费完或
/// 丢弃——流存活期间同一事务不得发出其它语句,未释放就返回事务体会在提交门禁处显式失败。
pub struct MapperStream<T> {
    inner: Pin<Box<dyn futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static>>,
    observation: Option<observability::MapperStreamGuard>,
}

impl<T> MapperStream<T> {
    /// 业务作用：包装一个 owned stream。
    ///
    /// # 参数
    /// - `stream`: 由 `sqlx::query_as(...).fetch(...)` 等调用返回的 owned stream；
    ///   生成代码会把 datasource pool 生命周期一起移动进 stream，避免借用外部连接。
    ///
    /// 返回：不附加方法观测的手动结果流；宏使用 observed 绑定静态方法身份。
    pub fn new<S>(stream: S) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
            observation: None,
        }
    }

    /// 业务作用：将宏生成的结果流与独立生命周期观测绑定，保留从未 poll 的丢弃事实。
    /// 参数说明：`stream` 为拥有型 SQLx 行流，`observation` 为构造时创建的守卫。
    /// 返回：连接和守卫共同随 EOF、错误或 Drop 收口的结果流。
    pub fn observed<S>(stream: S, observation: observability::MapperStreamGuard) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
            observation: Some(observation),
        }
    }

    /// 业务作用：经同一轮询边界读取下一行，使显式 next 与 StreamExt 保持一致的观测语义。
    /// 参数说明：无。
    /// 返回：行值或 SQLx 错误；None 表示结果集结束并完成终态计量。
    pub async fn next(&mut self) -> Option<Result<T, sqlx::Error>> {
        futures_util::StreamExt::next(self).await
    }
}

impl<T> futures_core::Stream for MapperStream<T> {
    type Item = Result<T, sqlx::Error>;

    /// 业务作用：围绕底层取行记录活跃等待、首行和唯一终态，不把消费者间隔算入数据库耗时。
    /// 参数说明：`cx` 为异步运行时的唤醒上下文。
    /// 返回：保留内部流的行、错误或等待状态；EOF 和错误只结算一次观测。
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(guard) = &mut this.observation {
            guard.poll_started();
        }
        let polled = this.inner.as_mut().poll_next(cx);
        if let Some(guard) = &mut this.observation {
            match &polled {
                Poll::Ready(Some(Ok(_))) => guard.row(),
                Poll::Ready(Some(Err(error))) => {
                    let code = guard
                        .database_code_enabled()
                        .then(|| error.as_database_error().and_then(|error| error.code()))
                        .flatten();
                    guard.finish(observation::classify_error(error), code.as_deref());
                }
                Poll::Ready(None) => guard.finish(observability::DbOutcome::Ok, None),
                Poll::Pending => {}
            }
        }
        polled
    }
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

/// 业务作用：当前是否处于 ambient 事务中。
///
/// Mapper 宏用它决定查询是否要走事务连接，以及是否允许业务显式启用事务内 L2 cache。
pub fn in_transaction() -> bool {
    natx::in_transaction()
}

/// 业务作用：当前 ambient 事务所属 datasource；无事务时返回 `None`。
///
/// 参数说明: 无。
///
/// 返回：事务内返回拥有型 datasource 引用，使运行期命名源不需要泄漏成静态字符串。
pub fn current_datasource() -> Option<natx::DatasourceRef> {
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

/// 业务作用：获取拒绝 ambient 事务的 Mapper SQL 执行连接(`tx = "never"` 的连接入口)。
///
/// 该入口用于副作用不允许随外层事务回滚的语句;处于事务内时立即报错且不取连接。
///
/// 参数说明：无。
///
/// 返回：无 ambient 事务时返回默认 datasource 的池连接；事务存在时立即拒绝且不取连接。
pub async fn never_conn() -> anyhow::Result<natx::Conn> {
    natx::never_conn().await
}

/// 业务作用：从指定 datasource 获取拒绝 ambient 事务的 Mapper SQL 执行连接。
///
/// 参数说明：
/// - `datasource`: `#[Mapper(datasource = "...")]` 指定的数据源名称。
///
/// 返回：无 ambient 事务时返回指定 datasource 的池连接；事务存在时立即拒绝且不取连接。
pub async fn never_conn_for(datasource: &'static str) -> anyhow::Result<natx::Conn> {
    natx::never_conn_for(datasource).await
}

/// 业务作用：在无事务时立即清理缓存；在事务内注册 commit 后清理。
///
/// # 参数
/// - `cache`: 当前 client 注入的缓存实现。
/// - `keys`: 需要清理的缓存组。
///
/// 返回：事务外返回立即清理结果；事务内成功登记 commit 后动作即返回，未注入缓存时直接成功。
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
