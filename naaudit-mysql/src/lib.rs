//! MySQL Outbox 审计适配器：审计事件与业务写必须共享同一个 ambient 事务。

#![forbid(unsafe_code)]

use naaudit::{AuditEvent, AuditWriteError, TransactionalAuditSink};
use naoutbox_mysql::MySqlOutbox;

/// 业务作用：把审计事件可靠写入句柄绑定 datasource 的 MySQL Outbox。
#[derive(Debug, Clone)]
pub struct MySqlOutboxAuditSink {
    outbox: MySqlOutbox,
}

impl Default for MySqlOutboxAuditSink {
    /// 业务作用：以兼容语义构造绑定默认 datasource 的审计适配器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`MySqlOutboxAuditSink::new`] 相同的句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl MySqlOutboxAuditSink {
    /// 业务作用：创建绑定默认 datasource 的审计适配器；连接与事务由 `natx` ambient context 拥有。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：后续审计事件固定写入默认 datasource Outbox 的轻量句柄。
    pub fn new() -> Self {
        Self {
            outbox: MySqlOutbox::new(),
        }
    }

    /// 业务作用：创建绑定命名 datasource 的审计 outbox 适配器。
    ///
    /// 参数说明：`datasource` 为启动期已注册的数据源名称。
    ///
    /// 返回：审计写入固定进入该 datasource 的 Outbox；名称非法时在 I/O 前失败。
    pub fn with_datasource(
        datasource: impl AsRef<str>,
    ) -> Result<Self, naoutbox_mysql::OutboxStoreError> {
        Ok(Self {
            outbox: MySqlOutbox::with_datasource(datasource)?,
        })
    }

    /// 业务作用：读取审计事件最终写入的 datasource 身份，供启动计划复验同源约束。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层 Outbox 固定使用的不可变 qualifier。
    pub fn datasource_ref(&self) -> &natx::DatasourceRef {
        self.outbox.datasource_ref()
    }
}

#[async_trait::async_trait]
impl TransactionalAuditSink for MySqlOutboxAuditSink {
    /// 业务作用：使用 ambient MySQL 事务写入审计 outbox，避免业务事实与审计事实发生双写分叉。
    async fn record_transactional(&self, event: AuditEvent) -> Result<(), AuditWriteError> {
        self.outbox
            .append_transactional(&event.into_outbox_event())
            .await
            .map_err(|_| AuditWriteError::new("transactional MySQL outbox append failed"))
    }
}
