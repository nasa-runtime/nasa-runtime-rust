//! 聚合根之间显式轮转，聚合根内部仍只投递原待确认头事件。

mod selection;

use crate::{
    dispatch_in_order_fenced, empty_report, OutboxPublishError, OutboxPublisher, OutboxStoreError,
    PgOutbox, DEFAULT_CHANNEL, DEFAULT_OWNER_ROLE, OWNER_LEASE_MS,
};
use naoutbox_core::DispatchReport;
use std::time::Duration;

/// 单一派发任务的有界轮转位置；不保存正文，不授予数据库或节点控制权。
#[derive(Default)]
pub struct AggregateCursor {
    last: Option<selection::Key>,
}

impl AggregateCursor {
    /// 业务作用：在更换派发任务或数据源时重新从当前待投递集合起点轮转。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：丢弃本地位置，不改变任何持久事件、租约或投递结果。
    pub fn reset(&mut self) {
        self.last = None;
    }
}

impl PgOutbox {
    /// 业务作用：在同一global租约下轮转独立聚合根，失败操作不会阻挡下一轮的其它聚合根。
    ///
    /// 参数说明：
    /// - `publisher`：至少一次发布端，只有真实接收确认才返回成功。
    /// - `cursor`：此派发任务唯一拥有的本地轮转位置，重启允许从头开始。
    /// - `max_payload_bytes`：单事件正文上限，范围为1至16MiB，超限正文不读取到客户端。
    ///
    /// 返回：一次至多尝试一个头事件；发布、头锁或正文容量失败保留原行并继续下轮轮转。
    /// 未取得owner返回空报告；结构、数据库或30秒整轮期限失败返回错误，未知提交保持可重投。
    /// 此入口需要聚合根待投递索引，与原global入口共用owner，不能与lane模式混用。
    /// 不保证跨聚合根全局顺序；同一聚合根的生产者必须串行提交，不能用id推断并发事务的提交顺序。
    pub async fn dispatch_next_aggregate<P>(
        &self,
        publisher: &P,
        cursor: &mut AggregateCursor,
        max_payload_bytes: usize,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        if !(1..=16 * 1024 * 1024).contains(&max_payload_bytes) {
            return Err(OutboxStoreError::new(
                "invalid aggregate dispatch payload budget",
            ));
        }
        if natx_pgsql::in_transaction() {
            return Err(OutboxStoreError::new(
                "dispatcher cannot run inside an ambient transaction",
            ));
        }
        tokio::time::timeout(
            Duration::from_secs(30),
            self.aggregate_round(publisher, cursor, max_payload_bytes),
        )
        .await
        .map_err(|_| OutboxStoreError::new("aggregate dispatch deadline exceeded"))?
    }

    /// 业务作用：让有界候选、真实发布与精确确认共同受原global owner约束，轮转位置不替代fencing。
    ///
    /// 参数说明：
    /// - `publisher`：当前轮次的真实发布端。
    /// - `cursor`：成功读取候选后才能推进的非权威本地位置。
    /// - `max_payload_bytes`：已校验的正文读取预算。
    ///
    /// 返回：正常轮次先确认明确成功行再释放owner；未知数据库结果保留租约到期门。
    async fn aggregate_round<P>(
        &self,
        publisher: &P,
        cursor: &mut AggregateCursor,
        max_payload_bytes: usize,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        // 与旧global派发共用同一租约，新增读取策略不能绕过原派发器的数据库权威。
        let Some(claim) = self
            .try_claim_owner(DEFAULT_OWNER_ROLE, DEFAULT_CHANNEL, OWNER_LEASE_MS)
            .await?
        else {
            return Ok(empty_report());
        };
        let Some(head) = selection::next(&claim, cursor.last.as_ref()).await? else {
            claim.release().await?;
            return Ok(empty_report());
        };
        let candidate = selection::load(&claim, &head, max_payload_bytes).await?;
        // 候选读取已明确完成或因头锁明确回滚；网络发布前移动位置，该根发布失败仍允许下轮服务另一根。
        cursor.last = Some(head.key);
        let report = match candidate {
            selection::Candidate::Empty => empty_report(),
            selection::Candidate::Deferred(reason) => DispatchReport {
                published: 0,
                total: 1,
                first_error: Some(OutboxPublishError::transient(reason)),
            },
            selection::Candidate::Ready(row) => {
                let report =
                    dispatch_in_order_fenced(&claim, std::slice::from_ref(&row.event), publisher)
                        .await?;
                if report.published == 1 {
                    // 发布成功仍须在原fence下精确确认并释放名额，不能把轮转后继当作成功前缀。
                    self.mark_dispatched_exact(&claim, std::slice::from_ref(&row))
                        .await?;
                }
                report
            }
        };
        claim.release().await?;
        Ok(report)
    }
}
