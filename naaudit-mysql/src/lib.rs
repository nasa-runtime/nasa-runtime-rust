//! MySQL Outbox 审计适配器：审计事件与业务写必须共享同一个 ambient 事务。

#![forbid(unsafe_code)]

use naaudit::{AuditEvent, AuditWriteError, TransactionalAuditSink};
use naoutbox_mysql::MySqlOutbox;

/// 把审计事件可靠写入 MySQL outbox 的无状态适配器。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlOutboxAuditSink {
    outbox: MySqlOutbox,
}

impl MySqlOutboxAuditSink {
    /// 业务作用：创建适配器；连接与事务由 `natx` ambient context 拥有。
    pub fn new() -> Self {
        Self {
            outbox: MySqlOutbox::new(),
        }
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
