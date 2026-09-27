//! PostgreSQL Outbox 审计适配器：审计事实与业务写必须共享同一 ambient transaction。

#![forbid(unsafe_code)]

use naaudit::{AuditEvent, AuditWriteError, TransactionalAuditSink};
use naoutbox_pgsql::PgOutbox;

/// PostgreSQL 事务审计 sink；全部审计写固定进入底层 Outbox 绑定的 datasource。
#[derive(Debug, Clone)]
pub struct PgOutboxAuditSink {
    outbox: PgOutbox,
}

impl Default for PgOutboxAuditSink {
    /// 业务作用：创建绑定默认 PostgreSQL datasource 的审计适配器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`PgOutboxAuditSink::new`] 相同的轻量句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl PgOutboxAuditSink {
    /// 业务作用：创建绑定默认 datasource 的审计适配器，不建立独立连接池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：审计事件固定写入默认 datasource Outbox 的轻量句柄。
    pub fn new() -> Self {
        Self {
            outbox: PgOutbox::new(),
        }
    }

    /// 业务作用：创建绑定命名 PostgreSQL datasource 的审计适配器。
    ///
    /// 参数说明：
    /// - `datasource`：启动期已登记的稳定 datasource 名称。
    ///
    /// 返回：名称合法时返回固定绑定句柄；非法名称在网络 I/O 前失败。
    pub fn with_datasource(
        datasource: impl AsRef<str>,
    ) -> Result<Self, naoutbox_pgsql::OutboxStoreError> {
        Ok(Self {
            outbox: PgOutbox::with_datasource(datasource)?,
        })
    }

    /// 业务作用：读取审计事件最终写入的 PostgreSQL datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层 Outbox 固定使用且不含连接信息的 qualifier。
    pub fn datasource_ref(&self) -> &natx_pgsql::DatasourceRef {
        self.outbox.datasource_ref()
    }
}

#[async_trait::async_trait]
impl TransactionalAuditSink for PgOutboxAuditSink {
    /// 业务作用：在当前同源 PostgreSQL ambient transaction 内写入审计 Outbox 事实。
    ///
    /// 参数说明：
    /// - `event`：已经完成 actor、资源、结局与脱敏 context 归因的业务审计事件。
    ///
    /// 返回：Outbox append 与 after-commit 登记成功时完成；事务、路由或数据库失败返回脱敏错误。
    async fn record_transactional(&self, event: AuditEvent) -> Result<(), AuditWriteError> {
        self.outbox
            .append_transactional(&event.into_outbox_event())
            .await
            .map_err(|_| AuditWriteError::new("transactional PostgreSQL outbox append failed"))
    }
}
