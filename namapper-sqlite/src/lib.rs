//! SQLite Mapper 运行时。
//!
//! 业务沿用 `#[Mapper]`、`#[Query]`、`#[Insert]` 等属性，通过 `nasa::mapper::sqlite` 使用。
//! napp 的同一个 `db` 组件管理按 driver 选择的数据源、迁移门禁和关闭责任；业务不另建 pool。
//! 编译期生成绑定和结果映射，Client 固定实际 datasource/owner，再通过受控 executor 执行；
//! 配置不会改变宏的 driver 类型，也不转换 SQL 方言或提供跨库事务。
//!
//! 宏展开代码通过本 crate 获取 `natx-sqlite` 连接、SQLite enum 编解码、流式读取和后端中立缓存
//! 合同。prepared bind 只由结构节点产生，业务 SQL 经数据源的原生权限门禁执行。
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
pub use natx_sqlite::{mapper_conn_for, mapper_mandatory_conn_for, mapper_never_conn_for};
mod observation;
pub use namapper_macro::{
    Delete, Execute, Insert, Query, SqliteMapper as Mapper, SqliteMapperEnum as MapperEnum,
    SqliteMapperOrderField as MapperOrderField, StreamQuery, Update,
};
pub use observation::{classify_method_result, classify_sqlx_result};
pub use sqlx::types::Json;

/// SQLite Mapper 流式查询返回类型。
///
/// 事务外持有 datasource 的池连接；`#[transactional_sqlite]` ambient 事务内则持有事务连接的槽锁
/// 直到流消费完、失败、丢弃或预算到期；流持有连接期间同一事务不得发出其它语句，未释放就返回事务体会在提交
/// 门禁处显式失败。
pub struct MapperStream<T> {
    inner:
        Option<Pin<Box<dyn futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static>>>,
    observation: Option<observability::MapperStreamGuard>,
    budget: Option<natx_sqlite::SqliteStreamBudget>,
}

impl<T> MapperStream<T> {
    /// 业务作用: 包装拥有 SqliteDataSource 生命周期的 SQLx 行流，允许调用方逐行消费而不借用外部连接。
    ///
    /// 参数说明：
    /// - `stream`: 宏生成的 SQLite owned stream。
    ///
    /// 返回: 可由业务异步迭代的 Mapper stream。
    pub fn new<S>(stream: S) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Some(Box::pin(stream)),
            observation: None,
            budget: None,
        }
    }

    /// 业务作用：将 SQLite 行流绑定独立状态机，区分数据库读取和消费者间隔。
    /// 参数说明：
    /// - `stream` 为拥有型行流
    /// - `observation` 为构造时建立的守卫。
    /// 返回：终止时只记一次事实的受观测结果流。
    pub fn observed<S>(stream: S, observation: observability::MapperStreamGuard) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Some(Box::pin(stream)),
            observation: Some(observation),
            budget: None,
        }
    }

    /// 业务作用：把生成的 Mapper 流绑定来源消费预算，未消费和小结果缓冲都不能越过空闲期限。
    /// 参数说明：
    /// - `stream`：持有连接的行流。
    /// - `observation`：记录一次最终结果的观测守卫。
    /// - `budget`：在流交付之前由同一连接创建的消费预算。
    /// 返回：到期时交付一次错误并释放内部流，原生责任仍等待 worker 收口。
    pub fn observed_with_budget<S>(
        stream: S,
        observation: observability::MapperStreamGuard,
        budget: natx_sqlite::SqliteStreamBudget,
    ) -> Self
    where
        S: futures_core::Stream<Item = Result<T, sqlx::Error>> + Send + 'static,
    {
        Self {
            inner: Some(Box::pin(stream)),
            observation: Some(observation),
            budget: Some(budget),
        }
    }

    /// 业务作用: 异步读取 SQLite 结果流的下一行。
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

    /// 业务作用：按底层取行边界采集 SQLite 活跃耗时、首行与终态，排除消费者停顿。
    /// 参数说明：
    /// - `context` 为异步运行时唤醒上下文。
    /// 返回：消费、执行期限和来源资格有效时交付结果；失效时只交付一次错误，保留已确认的数据库失败。
    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.is_none() {
            return Poll::Ready(None);
        }
        if let Some(guard) = &mut this.observation {
            guard.poll_started();
        }
        let mut polled = match this
            .budget
            .as_ref()
            .map(|budget| budget.poll_started())
            .transpose()
        {
            Err(error) => Poll::Ready(Some(Err(error))),
            Ok(_) => this
                .inner
                .as_mut()
                .expect("active SQLite Mapper stream")
                .as_mut()
                .poll_next(context),
        };
        if let Some(budget) = &this.budget {
            // 内部映射完成也不代表交付时仍有业务资格，缓冲行必须服从同一来源与单调期限。
            if matches!(polled, Poll::Ready(Some(Ok(_))) | Poll::Ready(None)) {
                if let Err(error) = budget.check_delivery() {
                    polled = Poll::Ready(Some(Err(error)));
                }
            }
            match &polled {
                Poll::Ready(Some(Ok(_))) => budget.row_ready(),
                Poll::Ready(_) => budget.finish(),
                Poll::Pending => {}
            }
        }
        if matches!(polled, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.inner = None;
        }
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

/// 以 ordinal 参与 SQLite bind、行解码和 cache serde 的 enum 包装。
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnumOrdinal<E: MapperEnum>(
    /// 被包装的业务 enum。
    pub E,
);

impl<E: MapperEnum> EnumOrdinal<E> {
    /// 业务作用: 创建按稳定整数序号持久化的 enum 包装。
    ///
    /// 参数说明：
    /// - `value`: 业务 enum 值。
    ///
    /// 返回: 可参与 SQLite 与 cache 编解码的包装值。
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

    /// 业务作用: 返回当前 enum 写入 SQLite 整数字段的稳定序号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: `MapperEnum` 定义的 ordinal。
    pub fn ordinal(self) -> i32 {
        self.0.ordinal()
    }
}

impl<E: MapperEnum> From<E> for EnumOrdinal<E> {
    /// 业务作用: 从业务 enum 直接构造 SQLite ordinal 包装。
    ///
    /// 参数说明：
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
    /// 参数说明：
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
    /// 参数说明：
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

impl<E: MapperEnum> sqlx::Type<sqlx::Sqlite> for EnumOrdinal<E> {
    /// 业务作用: 声明 SQLite ordinal 包装采用 `i32` 数据库类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: SQLx 的 SQLite `i32` 类型信息。
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <i32 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }

    /// 业务作用: 判断 SQLite 列是否可按 `i32` ordinal 解码。
    ///
    /// 参数说明：
    /// - `ty`: 服务端返回的列类型信息。
    ///
    /// 返回: SQLx `i32` 解码兼容时为 `true`。
    fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
        <i32 as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
    }
}

impl<'query, E: MapperEnum> sqlx::Encode<'query, sqlx::Sqlite> for EnumOrdinal<E> {
    /// 业务作用: 按值把 enum ordinal 写入 SQLite 参数缓冲区。
    ///
    /// 参数说明：
    /// - `buffer`: SQLx SQLite 参数缓冲区。
    ///
    /// 返回: SQLx null 状态或编码错误。
    fn encode(
        self,
        buffer: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError>
    where
        Self: Sized,
    {
        <i32 as sqlx::Encode<'query, sqlx::Sqlite>>::encode(self.0.ordinal(), buffer)
    }

    /// 业务作用: 按引用把 enum ordinal 写入 SQLite 参数缓冲区。
    ///
    /// 参数说明：
    /// - `buffer`: SQLx SQLite 参数缓冲区。
    ///
    /// 返回: SQLx null 状态或编码错误。
    fn encode_by_ref(
        &self,
        buffer: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let ordinal = self.0.ordinal();
        <i32 as sqlx::Encode<'query, sqlx::Sqlite>>::encode_by_ref(&ordinal, buffer)
    }
}

impl<'row, E: MapperEnum> sqlx::Decode<'row, sqlx::Sqlite> for EnumOrdinal<E> {
    /// 业务作用: 从 SQLite 整数字段解码 enum ordinal。
    ///
    /// 参数说明：
    /// - `value`: SQLx SQLite 原始列值。
    ///
    /// 返回: 已知 ordinal 对应 enum；未知值返回解码错误。
    fn decode(
        value: <sqlx::Sqlite as sqlx::Database>::ValueRef<'row>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let ordinal = <i32 as sqlx::Decode<'row, sqlx::Sqlite>>::decode(value)?;
        E::from_ordinal(ordinal).map(Self).ok_or_else(|| {
            format!(
                "unknown mapper enum ordinal {ordinal} for {}",
                std::any::type_name::<E>()
            )
            .into()
        })
    }
}

/// 业务作用: 判断当前任务是否已进入 SQLite ambient transaction。
///
/// 参数说明: 无。
///
/// 返回: 当前存在 SQLite 事务上下文时为 `true`。
pub fn in_transaction() -> bool {
    natx_sqlite::in_transaction()
}

/// 业务作用: 返回当前 SQLite ambient transaction 的 datasource 身份。
///
/// 参数说明: 无。
///
/// 返回: 事务内为拥有型 datasource 引用，事务外为 `None`。
pub fn current_datasource() -> Option<natx_sqlite::DatasourceRef> {
    natx_sqlite::current_datasource()
}

pub use natx_sqlite::{conn, conn_for, mandatory_conn_for, never_conn_for, SqliteDataSource};
mod binding;
pub use binding::{
    apply_sql_trim, check_mapper_source, mapper_conn_from, normalize_sql_whitespace, scoped_cache,
    sql_in_placeholders, validate_cached_read, SourceBinding,
};

/// 业务作用: 在无事务时立即清理 Mapper cache，在事务内延迟到 SQLite COMMIT 被明确确认后清理。
///
/// 参数说明：
/// - `cache`: 当前 Mapper client 的可选 L2 cache。
/// - `keys`: 已包含 driver/datasource 身份的缓存 namespace。
///
/// 返回：已确认写入不因缓存清理失败而变为失败；作用域缓存负责隔离来源，事务内无法登记 hook 时仍返回错误以阻止提交。
pub async fn clear_after_commit_or_now(
    cache: Option<Arc<dyn MapperL2Cache>>,
    keys: Vec<String>,
) -> anyhow::Result<()> {
    let Some(cache) = cache else {
        return Ok(());
    };
    if natx_sqlite::in_transaction() {
        // cache 失效不能早于数据库提交，否则回滚会让共享 cache 与真实数据产生不必要的错位窗口。
        natx_sqlite::after_commit(move || async move {
            if let Err(error) = cache.clear_keys(&keys).await {
                tracing::error!(
                    component = "mapper",
                    event = "cache_clear_after_commit_error",
                    error = %error,
                    "SQLite mapper cache clear failed after commit"
                );
            }
        })?;
        Ok(())
    } else {
        // SQL 已独立提交，缓存故障不能诱导业务按写入失败重试；作用域缓存先撤销后续共享命中。
        if let Err(error) = cache.clear_keys(&keys).await {
            tracing::error!(
                component = "mapper",
                event = "cache_clear_after_commit_error",
                error = %error,
                "SQLite mapper cache clear failed after commit"
            );
        }
        Ok(())
    }
}

/// 宏展开专用依赖桥。
#[doc(hidden)]
pub mod __private {
    pub use crate::binding::{apply_sql_trim, normalize_sql_whitespace, sql_in_placeholders};
    pub use anyhow;
    pub use async_stream;
    pub use async_trait;
    pub use futures_util;
    pub use linkme;
    pub use namapper_core::write_mapper_order_by_clause;
    pub use serde;
    pub use serde_json;
    pub use sqlx;
    pub use tracing;
}
