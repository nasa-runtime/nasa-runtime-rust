//! 只读关闭责任；本地资源退出不能证明未确认业务成功或远端没有写入。

use crate::{KafkaProxy, Lifecycle};
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// 本次调用是否等待关闭；未完成等待不撤回停止意图。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum CloseWaitOutcome {
    /// 仅采样，没有发起等待。
    NotWaited,
    /// 原关闭协议确认全部受管资源退出。
    Completed,
    /// 期限或其它关闭错误仍保留实际责任。
    Incomplete,
}

/// 管理资源的本地寿命，不代表业务请求或 broker 修改结局。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum CloseResourceState {
    /// 从未创建。
    NotCreated,
    /// 仍有共享原生客户端。
    Open,
    /// 已移交关闭线程，借用或销毁尚未确认结束。
    Closing,
    /// 原生客户端与关闭线程均已退出。
    Closed,
}

/// 普通消费关闭计数；释放次数不是跨 epoch 去重后的业务事件数。
#[derive(Clone, Debug, Default, Serialize)]
pub struct CloseConsumers {
    /// 尚未退出的实际 owner 线程数。
    pub active_owners: usize,
    /// 尚未实际返回的业务调用数。
    pub active_handlers: usize,
    /// 仍由输入凭证持有的记录数。
    pub retained_records: usize,
    /// 输入凭证仍计费的字节数。
    pub retained_bytes: usize,
    /// 停止期间释放但没有 broker 提交确认的观察次数。
    pub released_unverified_records: u64,
    /// 有可信来源恢复证据的交回数；普通关闭不探测具体记录存活，故保持零。
    pub returned_for_redelivery: u64,
}
impl CloseConsumers {
    /// 业务作用：合并不同 owner 的关闭责任，长期累计饱和而不回绕。
    /// 参数说明：other 为一个独立槽或已封存消费集合。
    /// 返回：原地累加，不保留 owner、客户端或原文引用。
    pub(crate) fn add(&mut self, other: &Self) {
        self.active_owners = self.active_owners.saturating_add(other.active_owners);
        self.active_handlers = self.active_handlers.saturating_add(other.active_handlers);
        self.retained_records = self.retained_records.saturating_add(other.retained_records);
        self.retained_bytes = self.retained_bytes.saturating_add(other.retained_bytes);
        self.released_unverified_records = self
            .released_unverified_records
            .saturating_add(other.released_unverified_records);
        self.returned_for_redelivery = self
            .returned_for_redelivery
            .saturating_add(other.returned_for_redelivery);
    }
}

/// lane 的当前责任与累计未知交付；关闭不会抹去远端结果不明的终态。
#[derive(Clone, Debug, Serialize)]
pub struct CloseLane {
    /// 冻结的 lane 名。
    pub name: String,
    /// 已准入且尚无本地终态的交付数。
    pub pending_delivery: usize,
    /// 源寿命内本地已终结、远端结果仍不明的交付数。
    pub terminal_delivery_unknown: u64,
    /// 包括旧代次在内的存活原生客户端数。
    pub live_native: usize,
    /// 永久关闭准入且交付和原生资源均已退出。
    pub closed: bool,
}

/// 源关闭的独立采样；资源退出、输入提交和业务成功分别裁决。
#[derive(Clone, Debug, Serialize)]
pub struct CloseReport {
    /// 规范源身份。
    pub source_id: String,
    /// 是否已永久请求停止消费。
    pub stop_requested: bool,
    /// 只有完整源关闭协议进入 Stopped 才为 true。
    pub resources_closed: bool,
    /// 本次 API 等待结局。
    pub wait_outcome: CloseWaitOutcome,
    /// 返回时是否已耗尽本次等待期限；只读采样为 false。
    pub deadline_expired: bool,
    /// 普通消费当前与封存责任。
    pub consumers: CloseConsumers,
    /// 最多64个尚未退出的组名，按名称排序。
    pub pending_groups: Vec<String>,
    /// 完整未退出组数，不受名单截断影响。
    pub pending_group_count: usize,
    /// 配置冻结的全部 lane 责任。
    pub lanes: Vec<CloseLane>,
    /// 尚未退出的回放任务数；业务终态从 ReplayHandle 查询。
    pub replay_tasks: usize,
    /// 目标仍持有的来源转发依赖数，包括未激活票据。
    pub transfer_dependencies: usize,
    /// 尚未归还目录的事务 owner 数；未知事务结局从 TransactionHandle 查询。
    pub transaction_tasks: usize,
    /// 仍执行的 DLT 任务数，包括旧分配任务。
    pub dlt_active_tasks: usize,
    /// 尚未归还的完整 DLT 信封记录数。
    pub dlt_records: usize,
    /// 尚未归还的 DLT 信封字节。
    pub dlt_bytes: usize,
    /// 共享管理资源寿命。
    pub admin: CloseResourceState,
    /// 管理资源额外借用数，覆盖未退出的阻塞请求与短连查询。
    pub auxiliary_resources_active: usize,
    /// 尚未执行的观测事件数。
    pub metrics_queued: usize,
    /// 正在用户后端中执行的回调数。
    pub metrics_active: usize,
    /// 导出线程是否实际退出；未配置后端时为 true。
    pub metrics_closed: bool,
}

impl KafkaProxy {
    /// 业务作用：读取实际关闭责任，不触发 broker 查询、停止或用户回调。
    /// 参数说明：无。
    /// 返回：独立采样；有限任务业务结局仍由原句柄保留，资源全部关闭不抹去未确认输入或未知交付。
    pub fn close_report(&self) -> CloseReport {
        let resources_closed = self.inner.lifecycle() == Lifecycle::Stopped;
        let groups = self
            .inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut consumers = self
            .inner
            .stopped_consumers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut pending_groups = Vec::new();
        let mut pending_group_count = 0;
        for (name, group) in groups.iter() {
            let current = group.close_report();
            if current.active_owners > 0 {
                pending_group_count += 1;
                if pending_groups.len() < 64 {
                    pending_groups.push(name.clone());
                }
            }
            consumers.add(&current);
        }
        drop(groups);
        let (admin, auxiliary_resources_active) = {
            let live = self
                .inner
                .admin
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let closing = self
                .inner
                .admin_close
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(state) = closing.as_ref() {
                (
                    if state.is_closed() {
                        CloseResourceState::Closed
                    } else {
                        CloseResourceState::Closing
                    },
                    state.resource_holders(),
                )
            } else if let Some(client) = live.as_ref() {
                (
                    CloseResourceState::Open,
                    std::sync::Arc::strong_count(client).saturating_sub(1),
                )
            } else {
                (CloseResourceState::NotCreated, 0)
            }
        };
        let (dlt_active_tasks, dlt_records, dlt_bytes) = self
            .inner
            .dlt_dispatcher
            .get()
            .map_or((0, 0, 0), |dispatcher| dispatcher.close_usage());
        let metrics = self.metrics_export_stats();
        CloseReport {
            source_id: self.config().client_name.clone(),
            stop_requested: self.inner.consumers_stop_requested.load(Ordering::Acquire),
            resources_closed,
            wait_outcome: CloseWaitOutcome::NotWaited,
            deadline_expired: false,
            consumers,
            pending_groups,
            pending_group_count,
            lanes: self
                .producer_lanes()
                .values()
                .map(|lane| lane.close_report())
                .collect(),
            replay_tasks: self.inner.replays.pending_names().len(),
            transfer_dependencies: self.inner.transfers.pending_names().len(),
            transaction_tasks: self.inner.transactions.active_count(),
            dlt_active_tasks,
            dlt_records,
            dlt_bytes,
            admin,
            auxiliary_resources_active,
            metrics_queued: metrics.queued,
            metrics_active: metrics.active,
            metrics_closed: metrics.worker_exited,
        }
    }

    /// 业务作用：按既有全源关闭协议推进资源退出，以结构化结果保留未完成责任。
    /// 参数说明：deadline 为消费、交付、管理与观测共用的绝对期限。
    /// 返回：成功为 Completed，关闭错误为 Incomplete；停止意图不可撤销，可再次调用续等。取消本 future 不伪造已退出报告。
    pub async fn shutdown_report_until(&self, deadline: Instant) -> CloseReport {
        let outcome = if self.shutdown_until(deadline).await.is_ok() {
            CloseWaitOutcome::Completed
        } else {
            CloseWaitOutcome::Incomplete
        };
        let mut report = self.close_report();
        report.wait_outcome = outcome;
        report.deadline_expired = Instant::now() >= deadline;
        report
    }
}
