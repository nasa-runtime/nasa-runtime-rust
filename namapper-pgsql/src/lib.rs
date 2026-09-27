//! PostgreSQL Mapper 运行时。
//!
//! 宏展开代码通过本 crate 获取 `natx-pgsql` 连接、PostgreSQL enum 编解码、流式读取和后端中立缓存
//! 合同。普通文本中的 `?` 不参与占位符处理，prepared bind 只由结构节点产生 `$1..$n`。
//! 方法调用、实际数据库执行与流消费分开采集；受管 YAML 控制日志、开发参数与有界通知。
//! 参数默认不输出，通知拥塞或失败不改变 SQL 返回与事务裁决。
//! 慢规则比较原始活跃执行耗时，相等也命中，不计连接等待和消费者处理。
//! 业务初始化 `Notify`、启用告警并将冷却设为 0 后逐条尝试入队；日志开关独立，框架不选择通知协议。

#![forbid(unsafe_code)]

use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub use namapper_core::*;
pub use natx_pgsql::{mapper_conn_for, mapper_mandatory_conn_for, mapper_never_conn_for};
mod observation;
pub use namapper_macro::{
    Delete, Execute, Insert, PgMapper as Mapper, PgMapperEnum as MapperEnum,
    PgMapperOrderField as MapperOrderField, Query, StreamQuery, Update,
};
pub use observation::{classify_method_result, classify_sqlx_result};
pub use sqlx::types::Json;

/// PostgreSQL Mapper 流式查询返回类型。
///
/// 事务外持有 datasource 的池连接；`#[transactional_pgsql]` ambient 事务内则持有事务连接的槽锁
/// 直到流被消费完或丢弃——流存活期间同一事务不得发出其它语句,未释放就返回事务体会在提交
/// 门禁处显式失败。
pub struct MapperStream<T> {
    inner: Pin<Box<dyn futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static>>,
    observation: Option<observability::MapperStreamGuard>,
}

impl<T> MapperStream<T> {
    /// 业务作用: 包装拥有 PgPool 生命周期的 SQLx 行流，允许调用方逐行消费而不借用外部连接。
    ///
    /// # 参数
    /// - `stream`: 宏生成的 PostgreSQL owned stream。
    ///
    /// 返回: 可由业务异步迭代的 Mapper stream。
    pub fn new<S>(stream: S) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
            observation: None,
        }
    }

    /// 业务作用：将 PostgreSQL 行流绑定独立状态机，区分数据库读取和消费者间隔。
    /// 参数说明：`stream` 为拥有型行流，`observation` 为构造时建立的守卫。
    /// 返回：终止时只记一次事实的受观测结果流。
    pub fn observed<S>(stream: S, observation: observability::MapperStreamGuard) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
            observation: Some(observation),
        }
    }

    /// 业务作用: 异步读取 PostgreSQL 结果流的下一行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 行值或 SQLx 错误；结果集结束时返回 `None`。
    pub async fn next(&mut self) -> Option<Result<T, sqlx::Error>> {
        futures_util::StreamExt::next(self).await
    }
}

impl<T> futures_core::Stream for MapperStream<T> {
    type Item = Result<T, sqlx::Error>;

    /// 业务作用：按底层取行边界采集 PostgreSQL 活跃耗时、首行与终态，排除消费者停顿。
    /// 参数说明：`context` 为异步运行时唤醒上下文。
    /// 返回：保留内部流的轮询结果，在错误被擦除前完成分类且不重复计量。
    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(guard) = &mut this.observation {
            guard.poll_started();
        }
        let polled = this.inner.as_mut().poll_next(context);
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

/// 以 ordinal 参与 PostgreSQL bind、行解码和 cache serde 的 enum 包装。
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnumOrdinal<E: MapperEnum>(
    /// 被包装的业务 enum。
    pub E,
);

impl<E: MapperEnum> EnumOrdinal<E> {
    /// 业务作用: 创建按稳定整数序号持久化的 enum 包装。
    ///
    /// # 参数
    /// - `value`: 业务 enum 值。
    ///
    /// 返回: 可参与 PostgreSQL 与 cache 编解码的包装值。
    pub fn new(value: E) -> Self {
        Self(value)
    }

    /// 业务作用: 在数据库或 cache 交互完成后取回原业务 enum。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 包装内部 enum。
    pub fn into_inner(self) -> E {
        self.0
    }

    /// 业务作用: 返回当前 enum 写入 PostgreSQL 整数字段的稳定序号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: `MapperEnum` 定义的 ordinal。
    pub fn ordinal(self) -> i32 {
        self.0.ordinal()
    }
}

impl<E: MapperEnum> From<E> for EnumOrdinal<E> {
    /// 业务作用: 从业务 enum 直接构造 PostgreSQL ordinal 包装。
    ///
    /// # 参数
    /// - `value`: 业务 enum。
    ///
    /// 返回: ordinal 包装值。
    fn from(value: E) -> Self {
        Self(value)
    }
}

impl<E: MapperEnum> Deref for EnumOrdinal<E> {
    type Target = E;

    /// 业务作用: 允许业务代码以只读方式访问包装内部 enum。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 内部 enum 引用。
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E: MapperEnum> serde::Serialize for EnumOrdinal<E> {
    /// 业务作用: 把 enum ordinal 序列化为 cache JSON 整数。
    ///
    /// # 参数
    /// - `serializer`: serde 目标序列化器。
    ///
    /// 返回: 序列化器结果。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.0.ordinal())
    }
}

impl<'de, E: MapperEnum> serde::Deserialize<'de> for EnumOrdinal<E> {
    /// 业务作用: 从 cache JSON 整数还原业务 enum ordinal。
    ///
    /// # 参数
    /// - `deserializer`: serde 来源反序列化器。
    ///
    /// 返回: 已知 ordinal 对应包装值；未知序号返回 serde 错误。
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

impl<E: MapperEnum> sqlx::Type<sqlx::Postgres> for EnumOrdinal<E> {
    /// 业务作用: 声明 PostgreSQL ordinal 包装采用 `i32` 数据库类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: SQLx 的 PostgreSQL `i32` 类型信息。
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        <i32 as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    /// 业务作用: 判断 PostgreSQL 列是否可按 `i32` ordinal 解码。
    ///
    /// # 参数
    /// - `ty`: 服务端返回的列类型信息。
    ///
    /// 返回: SQLx `i32` 解码兼容时为 `true`。
    fn compatible(ty: &<sqlx::Postgres as sqlx::Database>::TypeInfo) -> bool {
        <i32 as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'query, E: MapperEnum> sqlx::Encode<'query, sqlx::Postgres> for EnumOrdinal<E> {
    /// 业务作用: 按值把 enum ordinal 写入 PostgreSQL 参数缓冲区。
    ///
    /// # 参数
    /// - `buffer`: SQLx PostgreSQL 参数缓冲区。
    ///
    /// 返回: SQLx null 状态或编码错误。
    fn encode(
        self,
        buffer: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError>
    where
        Self: Sized,
    {
        <i32 as sqlx::Encode<'query, sqlx::Postgres>>::encode(self.0.ordinal(), buffer)
    }

    /// 业务作用: 按引用把 enum ordinal 写入 PostgreSQL 参数缓冲区。
    ///
    /// # 参数
    /// - `buffer`: SQLx PostgreSQL 参数缓冲区。
    ///
    /// 返回: SQLx null 状态或编码错误。
    fn encode_by_ref(
        &self,
        buffer: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let ordinal = self.0.ordinal();
        <i32 as sqlx::Encode<'query, sqlx::Postgres>>::encode_by_ref(&ordinal, buffer)
    }
}

impl<'row, E: MapperEnum> sqlx::Decode<'row, sqlx::Postgres> for EnumOrdinal<E> {
    /// 业务作用: 从 PostgreSQL 整数字段解码 enum ordinal。
    ///
    /// # 参数
    /// - `value`: SQLx PostgreSQL 原始列值。
    ///
    /// 返回: 已知 ordinal 对应 enum；未知值返回解码错误。
    fn decode(
        value: <sqlx::Postgres as sqlx::Database>::ValueRef<'row>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let ordinal = <i32 as sqlx::Decode<'row, sqlx::Postgres>>::decode(value)?;
        E::from_ordinal(ordinal).map(Self).ok_or_else(|| {
            format!(
                "unknown mapper enum ordinal {ordinal} for {}",
                std::any::type_name::<E>()
            )
            .into()
        })
    }
}

/// 业务作用: 判断当前任务是否已进入 PostgreSQL ambient transaction。
///
/// 参数说明: 无。
///
/// 返回: 当前存在 PostgreSQL 事务上下文时为 `true`。
pub fn in_transaction() -> bool {
    natx_pgsql::in_transaction()
}

/// 业务作用: 返回当前 PostgreSQL ambient transaction 的 datasource 身份。
///
/// 参数说明: 无。
///
/// 返回: 事务内为拥有型 datasource 引用，事务外为 `None`。
pub fn current_datasource() -> Option<natx_pgsql::DatasourceRef> {
    natx_pgsql::current_datasource()
}

/// 业务作用: 获取指定 PostgreSQL datasource 的连接池 clone，供无事务流式读取拥有其生命周期。
///
/// # 参数
/// - `datasource`: Mapper trait 声明的静态 datasource。
///
/// 返回: registry 中对应 PgPool；名称或生命周期不可用时返回错误。
pub fn pool_for(datasource: &'static str) -> anyhow::Result<sqlx::PgPool> {
    natx_pgsql::pool_for_datasource(datasource)
}

/// 业务作用: 获取默认 PostgreSQL Mapper 执行连接，事务内复用当前连接，事务外从池获取。
///
/// 参数说明: 无。
///
/// 返回: 可执行单条 Mapper SQL 的连接句柄或 registry/连接错误。
pub async fn conn() -> anyhow::Result<natx_pgsql::PgConn> {
    natx_pgsql::conn().await
}

/// 业务作用: 获取指定 datasource 的 PostgreSQL Mapper 执行连接，并阻止事务内跨源复用。
///
/// 参数说明：
/// - `datasource`: Mapper 声明的静态 datasource。
///
/// 返回: 同源事务连接或池连接；跨源与连接失败返回错误。
pub async fn conn_for(datasource: &'static str) -> anyhow::Result<natx_pgsql::PgConn> {
    natx_pgsql::conn_for(datasource).await
}

/// 业务作用: 获取必须属于当前 PostgreSQL ambient transaction 的默认 datasource 连接。
///
/// 参数说明: 无。
///
/// 返回: 事务连接；缺失事务上下文时立即拒绝。
pub async fn mandatory_conn() -> anyhow::Result<natx_pgsql::PgConn> {
    natx_pgsql::mandatory_conn().await
}

/// 业务作用: 获取必须属于当前 PostgreSQL ambient transaction 的命名 datasource 连接。
///
/// # 参数
/// - `datasource`: Mapper 声明的静态 datasource。
///
/// 返回: 同源事务连接；缺失事务或 datasource 不一致时拒绝。
pub async fn mandatory_conn_for(datasource: &'static str) -> anyhow::Result<natx_pgsql::PgConn> {
    natx_pgsql::mandatory_conn_for(datasource).await
}

/// 业务作用: 获取拒绝 ambient 事务的 Mapper SQL 执行连接(`tx = "never"` 的连接入口)。
///
/// 参数说明: 无。
///
/// 返回: 无事务时的池连接；处于事务内立即拒绝且不取连接。
pub async fn never_conn() -> anyhow::Result<natx_pgsql::PgConn> {
    natx_pgsql::never_conn().await
}

/// 业务作用: 从指定 datasource 获取拒绝 ambient 事务的 Mapper SQL 执行连接。
///
/// 参数说明：
/// - `datasource`: Mapper 声明的静态 datasource。
///
/// 返回: 无事务时该 datasource 的池连接；处于事务内立即拒绝。
pub async fn never_conn_for(datasource: &'static str) -> anyhow::Result<natx_pgsql::PgConn> {
    natx_pgsql::never_conn_for(datasource).await
}

/// 业务作用: 在无事务时立即清理 Mapper cache，在事务内延迟到 PostgreSQL COMMIT 被明确确认后清理。
///
/// # 参数
/// - `cache`: 当前 Mapper client 的可选 L2 cache。
/// - `keys`: 已包含 driver/datasource 身份的缓存 namespace。
///
/// 返回: 事务外传播清理错误；事务内成功登记 hook 后返回，hook 失败只记录观测事件。
pub async fn clear_after_commit_or_now(
    cache: Option<Arc<dyn MapperL2Cache>>,
    keys: Vec<String>,
) -> anyhow::Result<()> {
    let Some(cache) = cache else {
        return Ok(());
    };
    if natx_pgsql::in_transaction() {
        // cache 失效不能早于数据库提交，否则回滚会让共享 cache 与真实数据产生不必要的错位窗口。
        natx_pgsql::after_commit(move || async move {
            if let Err(error) = cache.clear_keys(&keys).await {
                tracing::error!(
                    component = "mapper",
                    event = "cache_clear_after_commit_error",
                    error = %error,
                    "PostgreSQL mapper cache clear failed after commit"
                );
            }
        })?;
        Ok(())
    } else {
        cache.clear_keys(&keys).await
    }
}

/// 宏展开专用依赖桥。
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use async_stream;
    pub use async_trait;
    pub use futures_util;
    pub use linkme;
    pub use namapper_core::{
        apply_sql_trim, normalize_sql_whitespace, write_mapper_order_by_clause, PostgresBindIndex,
    };
    pub use serde;
    pub use serde_json;
    pub use sqlx;
    pub use tracing;
}
