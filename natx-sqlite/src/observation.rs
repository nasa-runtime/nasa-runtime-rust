use std::sync::atomic::Ordering;
use std::sync::Arc;

use natx_core::observability::{PoolMetricsSource, PoolSnapshot, PoolState};

use crate::{DatabaseDriver, SqliteDataSource};

impl PoolSnapshot for SqliteDataSource {
    /// 业务作用：抓取真实物理连接责任和当前可借用数量，不把 async 取消误记为连接退出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：允许并发变化的有界池状态快照。
    fn snapshot(&self) -> PoolState {
        PoolState {
            total: self
                .0
                .file
                .live
                .load(Ordering::Acquire)
                .min(u32::MAX as usize) as u32,
            idle: self
                .0
                .idle
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len()
                .min(u32::MAX as usize) as u32,
            max: self.0.config.max_connections,
        }
    }
}

/// 业务作用：为冻结的 SQLite 来源表提供统一连接池指标，不创建采样任务。
///
/// 参数说明：
/// - `sources`：名称与来源使用句柄。
///
/// 返回：身份合法且不重复时返回固定目录的指标源。
pub fn pool_metrics_source(
    sources: impl IntoIterator<Item = (String, SqliteDataSource)>,
) -> anyhow::Result<PoolMetricsSource> {
    PoolMetricsSource::new(
        DatabaseDriver::Sqlite,
        sources
            .into_iter()
            .map(|(name, source)| (name, Arc::new(source) as Arc<dyn PoolSnapshot>))
            .collect(),
    )
}
