//! consumer owner 有界控制命令的公共 API 骨架。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::consumer::erased::erase_single;
use crate::consumer::supervisor::{
    CommandReply, GroupHandle, GroupPlan, OwnerCommandKind, OwnerMode, StartupGate,
};
use crate::consumer::SingleConsumer;
use crate::error::Result;
use crate::health::GroupHealth;
use crate::types::{AssignmentHandle, StartOffset, Tp};
use crate::KafkaProxy;

impl KafkaProxy {
    /// 以独占 group 固定消费指定分区。
    ///
    /// # 参数
    ///
    /// - `group`: 与 subscribe/其他 assign 共用命名空间的 group id。
    /// - `partitions`: 固定 topic-partition 集合。
    /// - `start`: 起始位点策略。
    /// - `consumer`: 单条类型化 handler。
    ///
    /// # 错误
    ///
    /// group 冲突、分区非法、位点解析或 owner 构造失败时返回错误。
    pub async fn assign<C: SingleConsumer>(
        &self,
        group: &str,
        partitions: impl IntoIterator<Item = Tp>,
        start: StartOffset,
        consumer: C,
    ) -> Result<AssignmentHandle> {
        if group.trim().is_empty() {
            return Err(crate::error::NafkaError::Registry(
                "assign group 不能为空".into(),
            ));
        }
        let mut partitions: Vec<Tp> = partitions.into_iter().collect();
        partitions.sort();
        if partitions.is_empty() {
            return Err(crate::error::NafkaError::Registry(
                "assign partitions 不能为空".into(),
            ));
        }
        if partitions
            .iter()
            .any(|tp| tp.topic.trim().is_empty() || tp.partition < 0)
        {
            return Err(crate::error::NafkaError::Registry(
                "assign topic 不能为空且 partition 不能为负".into(),
            ));
        }
        if partitions.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(crate::error::NafkaError::Registry(
                "assign partitions 存在重复项".into(),
            ));
        }
        let max = self.config().consumer.max_assignment_partitions;
        if partitions.len() > max {
            return Err(crate::error::NafkaError::AssignmentTooLarge {
                group: group.to_owned(),
                actual: partitions.len(),
                max,
            });
        }
        // 与 shutdown 的状态切换/快照串行化，覆盖 owner spawn 到本地构造结果返回的窗口。
        let _consumer_lifecycle = self.inner.consumer_lifecycle_gate.lock().await;
        {
            let groups = self
                .inner
                .groups
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if matches!(
                self.inner.lifecycle(),
                crate::Lifecycle::ShuttingDown | crate::Lifecycle::Stopped
            ) {
                return Err(crate::error::NafkaError::Lifecycle(format!(
                    "运行时已进入 {:?}，不能启动 assign owner",
                    self.inner.lifecycle()
                )));
            }
            if groups.contains_key(group) {
                return Err(crate::error::NafkaError::Registry(format!(
                    "group 已被 subscribe 或 assign 占用: {group}"
                )));
            }
        }
        crate::rd::config::consumer_config(self.config(), group)?;
        let route = erase_single(consumer)?;
        let starts = partitions.iter().cloned().map(|tp| (tp, start)).collect();
        let plan = GroupPlan {
            group: group.to_owned(),
            mode: OwnerMode::Assign {
                partitions: partitions.clone(),
                starts,
            },
            typed: vec![route],
            passthrough: Vec::new(),
        };
        let startup_gate = Arc::new(StartupGate::new());
        let (handle, startup_result) =
            GroupHandle::start(Arc::clone(&self.inner), plan, Arc::clone(&startup_gate))?;
        let startup_result = match startup_result.await {
            Ok(result) => result,
            Err(_) => Err(crate::error::NafkaError::Lifecycle(format!(
                "group `{group}` owner 在本地构造结果返回前退出"
            ))),
        };
        if let Err(error) = startup_result {
            startup_gate.abort();
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(self.config().behavior.shutdown_timeout_ms);
            let _ = handle.stop_until(deadline).await;
            return Err(error);
        }

        let commit_result = {
            let mut groups = self
                .inner
                .groups
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match self.inner.lifecycle() {
                crate::Lifecycle::ShuttingDown | crate::Lifecycle::Stopped => {
                    Err(crate::error::NafkaError::Lifecycle(format!(
                        "运行时已进入 {:?}，不能启动 assign owner",
                        self.inner.lifecycle()
                    )))
                }
                crate::Lifecycle::Created | crate::Lifecycle::Running
                    if groups.contains_key(group) =>
                {
                    Err(crate::error::NafkaError::Registry(format!(
                        "group 并发注册冲突: {group}"
                    )))
                }
                crate::Lifecycle::Created => {
                    match self
                        .inner
                        .transition(crate::Lifecycle::Created, crate::Lifecycle::Running)
                    {
                        Ok(()) => {
                            groups.insert(group.to_owned(), Arc::clone(&handle));
                            startup_gate.open();
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                }
                crate::Lifecycle::Running => {
                    groups.insert(group.to_owned(), Arc::clone(&handle));
                    startup_gate.open();
                    Ok(())
                }
            }
        };
        if let Err(error) = commit_result {
            startup_gate.abort();
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(self.config().behavior.shutdown_timeout_ms);
            let _ = handle.stop_until(deadline).await;
            return Err(error);
        }
        Ok(AssignmentHandle::new(self.clone(), group.to_owned()))
    }

    /// 将一个分区定位到显式 offset，并清理该分区本地 progress/retry 状态。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tp`: 目标分区。
    /// - `offset`: 新消费位置。
    ///
    /// # 错误
    ///
    /// group 不存在、命令未执行、结果未知或底层 seek 失败时返回错误。
    pub async fn seek(&self, group: &str, tp: Tp, offset: i64) -> Result<()> {
        if offset < 0 {
            return Err(crate::error::NafkaError::Config(
                "seek offset 不能为负".into(),
            ));
        }
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Seek {
                    offsets: BTreeMap::from([(tp, offset)]),
                })
                .await?,
        )
    }

    /// 将分区集合定位到最早可用 offset。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tps`: 目标分区集合。
    ///
    /// # 错误
    ///
    /// 命令未执行、结果未知或任一 seek 失败时返回错误。
    pub async fn seek_to_beginning(&self, group: &str, tps: Vec<Tp>) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::SeekBeginning { tps })
                .await?,
        )
    }

    /// 将分区集合定位到当前末尾 offset。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tps`: 目标分区集合。
    ///
    /// # 错误
    ///
    /// 命令未执行、结果未知或任一 seek 失败时返回错误。
    pub async fn seek_to_end(&self, group: &str, tps: Vec<Tp>) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::SeekEnd { tps })
                .await?,
        )
    }

    /// 按分区查询并定位到首条时间戳不小于目标值的记录。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `times`: 每分区目标 Unix epoch 毫秒。
    ///
    /// # 错误
    ///
    /// offset 查询、命令执行或任一 seek 失败时返回错误。
    pub async fn seek_to_timestamp(&self, group: &str, times: BTreeMap<Tp, i64>) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::SeekTimestamp { times })
                .await?,
        )
    }

    /// 将分区集合定位到 group 已提交位点。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tps`: 目标分区集合。
    ///
    /// # 错误
    ///
    /// committed 查询、命令执行或任一 seek 失败时返回错误。
    pub async fn seek_to_committed(&self, group: &str, tps: Vec<Tp>) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::SeekCommitted { tps })
                .await?,
        )
    }

    /// 为指定分区增加业务 `User` 暂停原因。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tps`: 待暂停分区。
    ///
    /// # 错误
    ///
    /// 命令未执行、结果未知或底层暂停失败时返回错误。
    pub async fn pause(&self, group: &str, tps: Vec<Tp>) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Pause { tps })
                .await?,
        )
    }

    /// 只移除指定分区的业务 `User` 暂停原因。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tps`: 待恢复分区。
    ///
    /// # 错误
    ///
    /// 命令未执行、结果未知或底层恢复失败时返回错误。
    pub async fn resume(&self, group: &str, tps: Vec<Tp>) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Resume { tps })
                .await?,
        )
    }

    /// 为当前 assignment 全部分区增加业务暂停原因。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    ///
    /// # 错误
    ///
    /// group 不存在或命令执行失败时返回错误。
    pub async fn pause_all(&self, group: &str) -> Result<()> {
        self.pause(group, Vec::new()).await
    }

    /// 只移除当前 assignment 全部分区的业务暂停原因。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    ///
    /// # 错误
    ///
    /// group 不存在或命令执行失败时返回错误。
    pub async fn resume_all(&self, group: &str) -> Result<()> {
        self.resume(group, Vec::new()).await
    }

    /// 查询 owner 当前消费位置。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    /// - `tps`: 待查询分区。
    ///
    /// # 错误
    ///
    /// group 不存在、命令结果未知或底层查询失败时返回错误。
    pub async fn position(&self, group: &str, tps: Vec<Tp>) -> Result<BTreeMap<Tp, Option<i64>>> {
        match self
            .control_group_handle(group)?
            .command(OwnerCommandKind::Position { tps })
            .await?
        {
            CommandReply::Positions(values) => Ok(values),
            _ => Err(crate::error::NafkaError::Lifecycle(
                "position 命令返回类型不匹配".into(),
            )),
        }
    }

    /// 查询 group 当前 assignment 快照。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 group。
    ///
    /// # 错误
    ///
    /// group 不存在、命令未执行或结果未知时返回错误。
    pub async fn assignment(&self, group: &str) -> Result<Vec<Tp>> {
        match self
            .control_group_handle(group)?
            .command(OwnerCommandKind::Assignment)
            .await?
        {
            CommandReply::Assignment(values) => Ok(values),
            _ => Err(crate::error::NafkaError::Lifecycle(
                "assignment 命令返回类型不匹配".into(),
            )),
        }
    }

    /// 查询 group 当前健康快照。
    ///
    /// # 参数
    ///
    /// - `group`: 已注册或运行 group。
    ///
    /// # 错误
    ///
    /// group 不存在时返回错误。
    pub async fn group_health(&self, group: &str) -> Result<GroupHealth> {
        Ok(self.group_handle(group)?.health())
    }

    /// 重建处于允许恢复状态的 group owner。
    ///
    /// # 参数
    ///
    /// - `group`: `Crashed` 或 `Degraded` group。
    ///
    /// # 错误
    ///
    /// 状态不允许、group 不存在或重建失败时返回错误。
    pub async fn restart_group(&self, group: &str) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Restart)
                .await?,
        )
    }

    /// 动态替换一个已运行 subscribe group 的 topic 集合。
    ///
    /// route 表仍保持启动时冻结；新 topic 上未匹配 event 的记录按 unmatched policy 处理。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 subscribe group。
    /// - `topics`: 新的非空 topic 集合。
    ///
    /// # 错误
    ///
    /// topic 非法、group 不存在或 owner 拒绝订阅时返回错误。
    pub async fn subscribe_group(&self, group: &str, mut topics: Vec<String>) -> Result<()> {
        topics.sort();
        if topics.is_empty()
            || topics.iter().any(|topic| topic.trim().is_empty())
            || topics.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(crate::error::NafkaError::Config(
                "subscribe topics 必须非空、无空值且不重复".into(),
            ));
        }
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Subscribe { topics })
                .await?,
        )
    }

    /// 取消一个 subscribe group 的当前订阅但保留 owner 与控制句柄。
    ///
    /// # 参数
    ///
    /// - `group`: 已运行 subscribe group。
    ///
    /// # 错误
    ///
    /// group 不存在或命令执行失败时返回错误。
    pub async fn unsubscribe_group(&self, group: &str) -> Result<()> {
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Unsubscribe)
                .await?,
        )
    }

    /// 在线性化的生命周期门禁内取得外部控制命令目标。
    ///
    /// shutdown 使用同一张 group 表的写锁切换 `ShuttingDown`；因此本方法要么在切换前
    /// 取得句柄并完成 admission，要么在切换后稳定返回生命周期错误，不会把新命令塞进
    /// 已经开始收尾的 owner 邮箱。
    ///
    /// # 参数
    ///
    /// - `group`: 最终 group.id。
    ///
    /// # 错误
    ///
    /// 运行时不在 Running，或 group 不存在时返回错误。
    fn control_group_handle(&self, group: &str) -> Result<Arc<GroupHandle>> {
        let groups = self
            .inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.inner.lifecycle() {
            crate::Lifecycle::Running => {}
            // Created 时 group 表必然为空(表写入与 Running 转换在同一写锁内绑定)：
            // 调用方的真实问题是"该 group 尚未启动"，报 NoSuchGroup 比"生命周期拒绝
            // Created"更能指向修复动作(先 start()/assign())。
            crate::Lifecycle::Created => {
                return Err(crate::error::NafkaError::NoSuchGroup(group.to_owned()));
            }
            state @ (crate::Lifecycle::ShuttingDown | crate::Lifecycle::Stopped) => {
                return Err(crate::error::NafkaError::Lifecycle(format!(
                    "运行时已进入 {state:?}，拒绝新的 consumer 控制命令"
                )));
            }
        }
        groups
            .get(group)
            .cloned()
            .ok_or_else(|| crate::error::NafkaError::NoSuchGroup(group.to_owned()))
    }

    /// 从共享 owner 表读取 group 句柄，不跨 await 持有锁。
    ///
    /// # 参数
    ///
    /// - `group`: 最终 group.id。
    ///
    /// # 错误
    ///
    /// group 不存在时返回错误。
    pub(crate) fn group_handle(&self, group: &str) -> Result<Arc<GroupHandle>> {
        self.inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(group)
            .cloned()
            .ok_or_else(|| crate::error::NafkaError::NoSuchGroup(group.to_owned()))
    }
}

/// 校验无返回值命令没有发生内部 reply 类型错配。
///
/// # 参数
///
/// - `reply`: owner 命令结果。
///
/// # 错误
///
/// reply 不是 Unit 时返回生命周期错误。
fn expect_unit(reply: CommandReply) -> Result<()> {
    if matches!(reply, CommandReply::Unit) {
        Ok(())
    } else {
        Err(crate::error::NafkaError::Lifecycle(
            "控制命令返回类型不匹配".into(),
        ))
    }
}
