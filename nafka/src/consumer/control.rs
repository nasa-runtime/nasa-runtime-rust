//! consumer owner 有界控制命令的公共 API 骨架。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::consumer::erased::{erase_batch, erase_single, ErasedConsumer};
use crate::consumer::supervisor::{
    CommandReply, GroupHandle, GroupPlan, OwnerCommandKind, OwnerMode, StartupGate,
};
use crate::consumer::{BatchConsumer, SingleConsumer};
use crate::error::Result;
use crate::health::GroupHealth;
use crate::types::{AssignmentHandle, StartOffset, Tp};
use crate::KafkaProxy;

impl KafkaProxy {
    /// 业务作用：执行逐分区控制并保留部分成功证据，不自动重路由或重试未知结果。
    /// 参数说明：group 为最终组名；actions 为非空、显式的目标与操作映射。
    /// 返回：总控制预算内的独立报告；非法声明、组不存在或生命周期拒绝返回错误。
    pub async fn control_partitions(
        &self,
        group: &str,
        actions: BTreeMap<Tp, crate::PartitionControl>,
    ) -> Result<crate::ControlReport> {
        if actions.is_empty() {
            return Err(crate::NafkaError::Config("分区控制目标不能为空".into()));
        }
        for (tp, action) in &actions {
            action.validate(tp)?;
        }
        let outcomes = self
            .control_group_handle(group)?
            .control_partitions(actions)
            .await;
        for outcome in outcomes.values() {
            let name = match outcome {
                crate::ControlOutcome::Applied => "applied",
                crate::ControlOutcome::NotApplied => "not_applied",
                crate::ControlOutcome::Unknown => "unknown",
                crate::ControlOutcome::OwnerChanged => "owner_changed",
            };
            self.inner.counter(
                "control_target_total",
                1,
                &[("group", group), ("outcome", name)],
            );
        }
        Ok(crate::ControlReport {
            source_id: self.config().client_name.clone(),
            group: group.into(),
            outcomes,
        })
    }

    /// 业务作用：以独占 group 固定消费指定分区。
    ///
    /// # 参数说明
    ///
    /// - `group`: 与 subscribe/其他 assign 共用命名空间及共同输入边界的精确 group id。
    /// - `partitions`: 固定 topic-partition 集合。
    /// - `start`: 起始位点策略。
    /// - `consumer`: 单条类型化 handler。
    ///
    /// # 错误
    ///
    /// group 冲突、分区非法或 owner 本地构造失败时返回错误。
    /// 返回：绑定本次 owner 的句柄；broker 分配及起点解析通过 Ready 与 health 继续观测。
    pub async fn assign<C: SingleConsumer>(
        &self,
        group: &str,
        partitions: impl IntoIterator<Item = Tp>,
        start: StartOffset,
        consumer: C,
    ) -> Result<AssignmentHandle> {
        self.assign_route(
            group,
            partitions.into_iter().collect(),
            start,
            erase_single(consumer)?,
        )
        .await
    }

    /// 业务作用：以独占 group 对固定分区执行连续批消费，确认与停止边界与单条消费一致。
    ///
    /// 参数说明：group 为独占组名，partitions 为固定分区，start 为起点，consumer 为批处理器。
    /// 返回：绑定该 owner 的句柄；声明冲突或初始化失败时返回错误且不开放消费。
    pub async fn assign_batch<C: BatchConsumer>(
        &self,
        group: &str,
        partitions: impl IntoIterator<Item = Tp>,
        start: StartOffset,
        consumer: C,
    ) -> Result<AssignmentHandle> {
        self.assign_route(
            group,
            partitions.into_iter().collect(),
            start,
            erase_batch(consumer)?,
        )
        .await
    }

    /// 业务作用：统一验证固定分区声明并在资源登记之后开放 owner。
    ///
    /// 参数说明：group 为独占组名，partitions 为分区，start 为起点，route 为冻结处理器。
    /// 返回：当前 owner 的句柄；非法组以 Config 拒绝且不创建资源，其它失败关闭本次创建的资源。
    async fn assign_route(
        &self,
        group: &str,
        mut partitions: Vec<Tp>,
        start: StartOffset,
        route: Arc<dyn ErasedConsumer>,
    ) -> Result<AssignmentHandle> {
        // 固定分区也会持有组位点，必须先验证完整身份再占有生命周期与 owner 槽。
        crate::wire::validate_group(group)?;
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
            GroupHandle::start(Arc::clone(&self.inner), plan, Arc::clone(&startup_gate)).await?;
        let startup_result = match startup_result.await {
            Ok(result) => result,
            Err(_) => Err(crate::error::NafkaError::Lifecycle(format!(
                "group `{group}` owner 在本地构造结果返回前退出"
            ))),
        };
        if let Err(error) = startup_result {
            // 尚未发布的 owner 必须保持消费门关闭，失败不允许留下后台业务副作用。
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
                            // 先发布实例登记再开放数据面，控制入口不能看不见已经开始消费的 owner。
                            groups.insert(group.to_owned(), Arc::clone(&handle));
                            startup_gate.open();
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                }
                crate::Lifecycle::Running => {
                    // 统一登记后放行的顺序，使停止快照能够覆盖本次新增 owner。
                    groups.insert(group.to_owned(), Arc::clone(&handle));
                    startup_gate.open();
                    Ok(())
                }
            }
        };
        if let Err(error) = commit_result {
            // 登记未成功时不能开放消费；收口失败的资源仍由原句柄承担停止责任。
            startup_gate.abort();
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(self.config().behavior.shutdown_timeout_ms);
            let _ = handle.stop_until(deadline).await;
            return Err(error);
        }
        Ok(AssignmentHandle::new(
            self.clone(),
            group.to_owned(),
            handle,
        ))
    }

    /// 业务作用：停止当前注册的消费组，并且只注销本次捕获的 owner。
    ///
    /// 参数说明：group 为已运行组名。
    /// 返回：不存在或退出完成时成功；期限内未退出时保留所有权并返回错误。
    pub async fn stop_group(&self, group: &str) -> Result<()> {
        let handle = match self.group_handle(group) {
            Ok(handle) => handle,
            Err(crate::error::NafkaError::NoSuchGroup(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        self.stop_group_instance(group, &handle).await
    }

    /// 业务作用：保护停止动作的实例身份，旧句柄不得停止或删除后来重建的同名组。
    ///
    /// 参数说明：group 为组名，handle 为创建时捕获的 owner。
    /// 返回：本实例退出或已被替换时成功；仍未退出时保留登记并返回错误。
    pub(crate) async fn stop_group_instance(
        &self,
        group: &str,
        handle: &Arc<GroupHandle>,
    ) -> Result<()> {
        {
            let groups = self
                .inner
                .groups
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // 同名新实例不继承旧句柄的控制权，停止前先按实例身份复验。
            if !groups
                .get(group)
                .is_some_and(|current| Arc::ptr_eq(current, handle))
            {
                return Ok(());
            }
        }
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(self.config().behavior.shutdown_timeout_ms);
        handle.stop_until(deadline).await?;
        // 等待期间允许其它生命周期操作推进；完成后必须复验实例，防止删掉同名新 owner。
        let mut groups = self
            .inner
            .groups
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if groups
            .get(group)
            .is_some_and(|current| Arc::ptr_eq(current, handle))
        {
            groups.remove(group);
        }
        Ok(())
    }

    /// 业务作用：将一个分区定位到显式 offset，并清理该分区本地 progress/retry 状态。
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
    /// 返回：owner 完成定位时成功；结果未知时应重新查询实际位置。
    pub async fn seek(&self, group: &str, tp: Tp, offset: i64) -> Result<()> {
        self.seek_offsets(group, BTreeMap::from([(tp, offset)]))
            .await
    }

    /// 业务作用：在同一 owner 命令中定位多个分区；不承诺多个 native seek 的原子性。
    /// 参数说明：group 为消费组，offsets 为各分区的非负目标。
    /// 返回：全部定位成功才成功；失败或超时需查询实际位置，不能推定全部未执行。
    pub async fn seek_offsets(&self, group: &str, offsets: BTreeMap<Tp, i64>) -> Result<()> {
        if offsets.is_empty() || offsets.values().any(|offset| *offset < 0) {
            return Err(crate::error::NafkaError::Config(
                "seek offset 不能为负".into(),
            ));
        }
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Seek { offsets })
                .await?,
        )
    }

    /// 业务作用：将分区集合定位到最早可用 offset。
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

    /// 业务作用：将分区集合定位到当前末尾 offset。
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

    /// 业务作用：按分区查询并定位到首条时间戳不小于目标值的记录。
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

    /// 业务作用：将分区集合定位到 group 已提交位点。
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

    /// 业务作用：为指定分区增加业务 `User` 暂停原因。
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

    /// 业务作用：只移除指定分区的业务 `User` 暂停原因。
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

    /// 业务作用：为当前 assignment 全部分区增加业务暂停原因。
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

    /// 业务作用：只移除当前 assignment 全部分区的业务暂停原因。
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

    /// 业务作用：查询 owner 当前消费位置。
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

    /// 业务作用：查询 group 当前 assignment 快照。
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

    /// 业务作用：查询 group 当前健康快照。
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

    /// 业务作用：查询组内全部 owner，区分空闲槽、慢调用与异常成员。
    /// 参数说明：group 为最终消费组名称。
    /// 返回：按固定槽号排列的健康快照；组未注册时返回 NoSuchGroup。
    pub async fn group_slots_health(&self, group: &str) -> Result<Vec<GroupHealth>> {
        Ok(self.group_handle(group)?.slots_health())
    }

    /// 业务作用：重建处于允许恢复状态的 group owner。
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

    /// 业务作用：动态替换一个已运行 subscribe group 的 topic 集合。
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
                .command(OwnerCommandKind::Subscribe {
                    topics,
                    change: super::supervisor::SubscriptionChange::Replace,
                })
                .await?,
        )
    }

    /// 业务作用：明确替换订阅集合；空集合清空订阅并保留 owner。
    /// 参数说明：group 为已运行组，topics 为新集合。
    /// 返回：owner 已应用或返回控制失败，不改变冻结路由表。
    pub async fn replace_subscriptions(&self, group: &str, topics: Vec<String>) -> Result<()> {
        if topics.is_empty() {
            self.unsubscribe_group(group).await
        } else {
            self.subscribe_group(group, topics).await
        }
    }

    /// 业务作用：原子追加订阅，避免多个调用通过读改写互相覆盖。
    /// 参数说明：group 为已运行组，topics 为待增加集合。
    /// 返回：owner 已应用新集合或返回控制错误。
    pub async fn add_subscriptions(&self, group: &str, topics: Vec<String>) -> Result<()> {
        self.change_subscriptions(group, topics, super::supervisor::SubscriptionChange::Add)
            .await
    }

    /// 业务作用：只退订指定 topic，其它订阅继续保留。
    /// 参数说明：group 为已运行组，topics 为待删除集合。
    /// 返回：owner 已应用；删空时保留可再次订阅的 owner。
    pub async fn remove_subscriptions(&self, group: &str, topics: Vec<String>) -> Result<()> {
        self.change_subscriptions(group, topics, super::supervisor::SubscriptionChange::Remove)
            .await
    }

    /// 业务作用：在进入有界控制队列前验证订阅修改的完整声明。
    /// 参数说明：group 为组名，topics 为修改集，change 为追加或删除。
    /// 返回：命令实际终态，非法声明不入队。
    async fn change_subscriptions(
        &self,
        group: &str,
        mut topics: Vec<String>,
        change: super::supervisor::SubscriptionChange,
    ) -> Result<()> {
        topics.sort();
        if topics.is_empty()
            || topics.iter().any(|topic| topic.trim().is_empty())
            || topics.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(crate::error::NafkaError::Config(
                "topics 必须非空且不重复".into(),
            ));
        }
        expect_unit(
            self.control_group_handle(group)?
                .command(OwnerCommandKind::Subscribe { topics, change })
                .await?,
        )
    }

    /// 业务作用：清空订阅且保留 owner，供后续重新订阅。
    /// 参数说明：group 为已运行组。
    /// 返回：owner 已取消订阅或返回控制错误。
    pub async fn clear_subscriptions(&self, group: &str) -> Result<()> {
        self.unsubscribe_group(group).await
    }

    /// 业务作用：取消一个 subscribe group 的当前订阅但保留 owner 与控制句柄。
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

    /// 业务作用：在线性化的生命周期门禁内取得外部控制命令目标。
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
            // Created"更能指向正确动作（先 start()/assign()）。
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

    /// 业务作用：从共享 owner 表读取 group 句柄，不跨 await 持有锁。
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

/// 业务作用：校验无返回值命令没有发生内部 reply 类型错配。
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
