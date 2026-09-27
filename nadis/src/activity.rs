//! 普通消费与微批的本地任务、责任数量和进展证据，不推断远端 PEL 或业务成功。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 当前领域 owner 的本地责任快照；各数量不代表 Redis 的持久消息总数。
#[derive(Debug, Clone, Default)]
pub struct RedisTaskObservation {
    /// 尚未退出的普通消费任务，包括等待激活的任务。
    pub consumers: usize,
    /// 尚未退出的 PEL 回收任务。
    pub reclaimers: usize,
    /// 尚未退出的自动微批任务。
    pub flushers: usize,
    /// 入队但尚未被 flusher 取出的命令参数字节，不含编码、批次和等待提交者。
    pub queued_parameter_bytes: usize,
    /// 已接纳但尚无本地处理结束证据的工作次数；强制退出后仍保留，不能视为可安全重放。
    pub unfinished: usize,
    /// 已完成本地处理的工作次数；handler 失败、服务器错误或结果未知均不等于业务成功。
    pub completed: u64,
    /// 微批派发中没有取得完整传输结果的批次数。
    pub batch_transport_failures: u64,
    /// 最近一次有效读取、处理结束或批次回执距采样时刻的间隔；None 表示尚无进展证据。
    pub last_progress_ago: Option<Duration>,
}

#[derive(Default)]
pub(crate) struct Activity {
    state: Mutex<(RedisTaskObservation, Option<Instant>, bool)>,
}
pub(crate) enum TaskKind {
    Consumer,
    Reclaimer,
    Flusher,
}
pub(crate) struct TaskGuard(Arc<Activity>, TaskKind);

pub(crate) struct CompletionOwner<T> {
    activity: Arc<Activity>,
    sender: tokio::sync::watch::Sender<T>,
}

impl<T> CompletionOwner<T> {
    /// 业务作用：由唯一收尾 owner 发布领域退出证据。
    /// 参数说明：`value` 为已经裁决的本地状态或报告。
    /// 返回：替换保留的最新证据，不开放已关闭准入。
    pub(crate) fn send_replace(&self, value: T) {
        self.sender.send_replace(value);
    }
}
impl<T> Drop for CompletionOwner<T> {
    /// 业务作用：收尾 owner 失去责任前先撤销接流权威，包含执行器销毁和未首次轮询的路径。
    /// 参数说明：无。
    /// 返回：先与最终发布串行收口，再释放退出证据通道，不遗留可接流的旧快照。
    fn drop(&mut self) {
        self.activity.close();
    }
}

impl Activity {
    /// 业务作用：让独立收尾 owner 的退出与最终接流裁决使用同一保护。
    /// 参数说明：`sender` 为本代唯一完成证据发布端。
    /// 返回：持有证据通道的责任守卫，释放时先撤销准入再关闭通道。
    pub(crate) fn completion<T>(
        self: &Arc<Self>,
        sender: tokio::sync::watch::Sender<T>,
    ) -> CompletionOwner<T> {
        CompletionOwner {
            activity: self.clone(),
            sender,
        }
    }

    /// 业务作用：永久撤销本代接流权威，并等待正在提交的本地发布完成。
    /// 参数说明：无。
    /// 返回：后续最终裁决均拒绝执行，不代表后台任务已经退出。
    pub(crate) fn close(&self) {
        self.state.lock().unwrap().2 = true;
    }
    /// 业务作用：在本地任务责任完整的保护范围内执行接流裁决，与任务退出串行化。
    /// 参数说明：三个数量为准备时固定的任务数；`publish` 不得阻塞或重入本 owner 的任务观测。
    /// 返回：数量均匹配时返回裁决结果，否则不执行；不推断远端可用性。
    pub(crate) fn with_tasks<T>(
        &self,
        consumers: usize,
        reclaimers: usize,
        flushers: usize,
        publish: impl FnOnce() -> T,
    ) -> Option<T> {
        let state = self.state.lock().unwrap();
        // 关闭或固定任务缺席都使本代失去完整运行责任，不能以剩余任务仍存活为由开放业务。
        if state.2
            || (state.0.consumers, state.0.reclaimers, state.0.flushers)
                != (consumers, reclaimers, flushers)
        {
            return None;
        }
        Some(publish())
    }
    /// 业务作用：在任务移交执行器之前登记责任，使未被首次轮询的取消也能归还数量。
    /// 参数说明：`kind` 为固定任务类别。
    /// 返回：随任务 future 持有的退出守卫。
    pub(crate) fn task(self: &Arc<Self>, kind: TaskKind) -> TaskGuard {
        let mut state = self.state.lock().unwrap();
        match kind {
            TaskKind::Consumer => state.0.consumers += 1,
            TaskKind::Reclaimer => state.0.reclaimers += 1,
            TaskKind::Flusher => state.0.flushers += 1,
        }
        TaskGuard(self.clone(), kind)
    }
    /// 业务作用：登记已被本地接受的处理责任，准备阶段不虚构业务进展。
    /// 参数说明：`count` 为工作次数；`queued_bytes` 仅为微批入队的参数字节。
    /// 返回：增加未完成数量和排队字节。
    pub(crate) fn accept(&self, count: usize, queued_bytes: usize) {
        let mut state = self.state.lock().unwrap();
        state.0.unfinished += count;
        state.0.queued_parameter_bytes += queued_bytes;
    }
    /// 业务作用：命令移入在途批次时解除其排队字节占用，未完成责任继续保留。
    /// 参数说明：`bytes` 为已取出命令的参数字节。
    /// 返回：减少排队字节，不表示写入已完成。
    pub(crate) fn dequeue(&self, bytes: usize) {
        self.state.lock().unwrap().0.queued_parameter_bytes -= bytes;
    }
    /// 业务作用：记录本轮处理结束或有效空轮询，保留结果与业务成功的区别。
    /// 参数说明：`count` 为本地已结束工作次数；`transport_failed` 表示微批传输证据不完整。
    /// 返回：更新计数和进展时间；取消而未调用本方法的责任继续显示未完成。
    pub(crate) fn finish(&self, count: usize, transport_failed: bool) {
        let mut state = self.state.lock().unwrap();
        state.0.unfinished -= count;
        state.0.completed = state.0.completed.saturating_add(count as u64);
        state.0.batch_transport_failures = state
            .0
            .batch_transport_failures
            .saturating_add(u64::from(transport_failed));
        state.1 = Some(Instant::now());
    }
    /// 业务作用：任务退出后释放本地队列字节，仍保留没有结果证据的命令数量。
    /// 参数说明：无。
    /// 返回：队列内存不再占用；不把未知工作标记为完成。
    pub(crate) fn queue_dropped(&self) {
        self.state.lock().unwrap().0.queued_parameter_bytes = 0;
    }
    /// 业务作用：读取有限维度的本地证据，不查询或改变 Redis 数据。
    /// 参数说明：无。
    /// 返回：同锁采样的数量与进展时间。
    pub(crate) fn snapshot(&self) -> RedisTaskObservation {
        let state = self.state.lock().unwrap();
        let mut value = state.0.clone();
        value.last_progress_ago = state.1.map(|at| at.elapsed());
        value
    }
}
impl Drop for TaskGuard {
    /// 业务作用：任务 future 释放时归还对应存活数量，包含异常和取消路径。
    /// 参数说明：无。
    /// 返回：撤销本代接流权威并减少任务数，未完成工作仍需独立裁决。
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        // 固定任务中任意一个退出都使本代失去完整运行责任，不能继续发布 Ready。
        state.2 = true;
        match self.1 {
            TaskKind::Consumer => state.0.consumers -= 1,
            TaskKind::Reclaimer => state.0.reclaimers -= 1,
            TaskKind::Flusher => state.0.flushers -= 1,
        }
    }
}
