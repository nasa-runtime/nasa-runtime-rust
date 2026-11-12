//! 消费者显式注册、静态收集激活与启动冻结边界。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};

use crate::consumer::erased::{erase_batch, erase_single, ConsumerMeta, ErasedConsumer};
use crate::consumer::supervisor::{GroupHandle, GroupPlan, OwnerMode, StartupGate};
use crate::consumer::{AckMode, BatchConsumer, GroupSpec, PassthroughConsumer, SingleConsumer};
use crate::error::{NafkaError, Result};
use crate::{KafkaProxy, COLLECTED_CONSUMERS};

/// 已激活静态收集项的 client 名，防止同一进程重复启动相同静态 handler。
static ACTIVATED_CLIENTS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

/// 进程内静态收集激活租约。
///
/// 为什么用 RAII 而不是"先检查后插入"：检查与插入之间隔着 owner 线程创建等大段异步流程，
/// 两个并发 `start()` 都能通过检查。这里改为
/// 一次原子 `insert` 直接占位，并让任何提前返回的失败路径经 `Drop` 自动归还。
struct ActivationLease {
    /// 仍被本租约持有的 client 名；`None` 表示所有权已转交运行时。
    client: Option<String>,
}

impl ActivationLease {
    /// 业务作用：放弃释放：owner 线程去向不明时，宁可永久占住该 client 也不放行第二个运行时。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：租约不再通过 Drop 释放，进程激活表保留该 client；已转交或已放弃的租约再次调用无效果。
    fn leak_on_uncertain_stop(&mut self) {
        std::mem::forget(self.client.take());
    }
}

/// 业务作用：回滚已启动的 owner，并在任一 owner 未能在 deadline 内退出时保留激活租约。
///
/// 直接丢弃 `stop_until` 的结果会让租约随 `Drop` 释放，而线程可能仍然存活——
/// 此时第二个同名 client 就能并发启动，违反「同 client 单激活」。
///
/// 参数说明：
///
/// - `started`: 已经 spawn 的 owner 句柄。
/// - `deadline`: 回滚总截止时刻。
/// - `lease`: 本次启动持有的激活租约。
///
/// 返回：等待全部停止尝试结束；任一失败则永久保留静态激活占位并记录错误，全部成功时保留租约供调用方退出时释放。
async fn rollback_started(
    started: &std::collections::BTreeMap<String, Arc<GroupHandle>>,
    deadline: std::time::Instant,
    lease: &mut Option<ActivationLease>,
) {
    let results =
        futures::future::join_all(started.values().map(|handle| handle.stop_until(deadline))).await;
    if results.iter().any(std::result::Result::is_err) {
        // 有 owner 线程没能确认退出：让租约随进程存续，宁可后续启动被拒，
        // 也不能让两个运行时同时持有同一个 collected client。
        if let Some(lease) = lease.as_mut() {
            lease.leak_on_uncertain_stop();
        }
        tracing::error!("consumer 启动回滚时有 owner 未在 deadline 内退出，保留激活租约");
    }
}

impl ActivationLease {
    /// 业务作用：原子占用指定 client 的激活租约。
    ///
    /// 参数说明：
    ///
    /// - `client`: 静态收集项绑定的客户端实例名。
    ///
    /// # 错误
    ///
    /// 该 client 已被另一个存活运行时占用时返回注册错误。
    ///
    /// 返回：首次占位成功时返回可自动归还的进程内租约，重复占位返回 Registry；不向 Kafka 或注册中心申请分布式租约。
    fn acquire(client: &str) -> Result<Self> {
        // 检查与插入使用同一锁，避免并发启动都观察到空闲后分别开放同名静态消费者。
        let inserted = ACTIVATED_CLIENTS
            .get_or_init(|| Mutex::new(BTreeSet::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(client.to_owned());
        if inserted {
            Ok(Self {
                client: Some(client.to_owned()),
            })
        } else {
            Err(NafkaError::Registry(format!(
                "client `{client}` 的静态 consumer 已被另一运行时激活"
            )))
        }
    }

    /// 业务作用：启动成功后把租约交给运行时，由 shutdown 负责释放。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：转交仍被占用的 client 名，使当前守卫析构不再归还；没有可转交的租约时因违反内部生命周期约束而 panic。
    fn into_owner(mut self) -> String {
        self.client.take().expect("租约只能转交一次")
    }
}

impl Drop for ActivationLease {
    /// 业务作用：启动未走到成功分支时归还租约，避免失败的一次 start 永久占住 client 名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仍由本守卫持有的激活占位被释放；已转交运行时或因退出不确定而放弃释放时保持现有占位。
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            release_activation(&client);
        }
    }
}

/// 业务作用：释放一个静态收集激活租约。
///
/// 正常关闭须在全部 owner 退出后调用；启动回滚也可由仍持有租约的守卫调用。
/// 退出结果不确定时不能释放，否则同名消费者可能被重复激活。
///
/// 参数说明：
///
/// - `client`: 启动时占用的客户端实例名。
///
/// 返回：从进程激活集合移除目标，未初始化或目标缺席时无副作用；本函数不验证线程是否退出，调用方必须持有释放权。
pub(crate) fn release_activation(client: &str) {
    if let Some(activated) = ACTIVATED_CLIENTS.get() {
        activated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(client);
    }
}

/// 已注册 passthrough 消费者及其冻结元数据。
#[derive(Clone)]
pub(crate) struct RegisteredPassthrough {
    /// 业务同步借用 handler。
    #[allow(dead_code)]
    pub(crate) handler: Arc<dyn PassthroughConsumer>,
    /// 注册时读取并校验的元数据。
    pub(crate) meta: PassthroughMeta,
}

/// passthrough route 的冻结元数据。
#[derive(Clone)]
pub(crate) struct PassthroughMeta {
    /// 稳定 handler id。
    pub(crate) id: &'static str,
    /// 排序后的 topic 列表。
    #[allow(dead_code)]
    pub(crate) topics: Vec<String>,
    /// 业务事件名。
    #[allow(dead_code)]
    pub(crate) event: String,
    /// 尚未解析的 group 规则。
    #[allow(dead_code)]
    pub(crate) group: GroupSpec,
    /// route 确认模式。
    #[allow(dead_code)]
    pub(crate) ack_mode: AckMode,
}

/// 一次性消费者注册 builder。
///
/// 所有注册方法消费并返回 `Self`，防止同一 builder 被重复启动或重复激活静态收集项。
pub struct ConsumerRegistryBuilder {
    /// 所属运行时。
    proxy: KafkaProxy,
    /// 类型化消费者列表。
    typed: Vec<Arc<dyn ErasedConsumer>>,
    /// 同步借用消费者列表。
    passthrough: Vec<RegisteredPassthrough>,
    /// 已读取的静态收集目标 client；`with_collected()` 是配置 client_name 的简写。
    collected_client: Option<String>,
}

impl ConsumerRegistryBuilder {
    /// 业务作用：创建空注册 builder。
    ///
    /// 参数说明：
    ///
    /// - `proxy`: 所属运行时。
    ///
    /// 返回：绑定现有源的空注册器；不激活静态消费者、不创建 owner，后续 start 至少需要一个已登记消费者。
    pub(crate) fn new(proxy: KafkaProxy) -> Self {
        Self {
            proxy,
            typed: Vec::new(),
            passthrough: Vec::new(),
            collected_client: None,
        }
    }

    /// 业务作用：登记有状态单条消费者并冻结其路由声明。
    ///
    /// 参数说明：
    /// - `consumer`：consumer 为已完成业务依赖注入的实例。
    ///
    /// 返回：持有冻结路由的注册器；组身份不满足共同边界时为 Config，其它元数据非法或 handler id 重复时为 Registry。
    pub fn register<C: SingleConsumer>(mut self, consumer: C) -> Result<Self> {
        let erased = erase_single(consumer)?;
        self.push_typed(erased)?;
        Ok(self)
    }

    /// 业务作用：登记有状态批消费者并冻结其路由声明。
    ///
    /// 参数说明：
    /// - `consumer`：consumer 为已完成业务依赖注入的实例。
    ///
    /// 返回：持有冻结路由的注册器；组身份不满足共同边界时为 Config，其它元数据非法、按键批模式或 handler id 重复时为 Registry。
    pub fn register_batch<C: BatchConsumer>(mut self, consumer: C) -> Result<Self> {
        let erased = erase_batch(consumer)?;
        self.push_typed(erased)?;
        Ok(self)
    }

    /// 业务作用：登记在 poll 栈内同步借用原文的少拷贝消费者。
    ///
    /// 参数说明：
    /// - `consumer`：consumer 为已完成本地路由和 outbox 注入的同步 handler。
    ///
    /// 返回：持有冻结路由的注册器；组身份不满足共同边界时为 Config，其它元数据非法或 handler id 重复时为 Registry。
    pub fn register_passthrough<C: PassthroughConsumer>(mut self, consumer: C) -> Result<Self> {
        let meta = passthrough_meta(&consumer)?;
        self.ensure_unique_id(meta.id)?;
        self.passthrough.push(RegisteredPassthrough {
            handler: Arc::new(consumer),
            meta,
        });
        Ok(self)
    }

    /// 业务作用：收集并构造与当前配置 client_name 匹配的静态属性宏消费者，交由后续启动统一激活。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含匹配消费者的注册器；重复收集或构造、身份冲突返回错误，此时尚未取得进程激活租约或开始消费。
    ///
    /// # 错误
    ///
    /// 重复调用、静态构造失败或 handler id 重复时返回注册错误。
    pub fn with_collected(self) -> Result<Self> {
        let client = self.proxy.config().client_name.clone();
        self.with_collected_for(&client)
    }

    /// 业务作用：按指定客户端名精确收集静态消费者，并冻结为待启动声明。
    ///
    /// 参数说明：
    ///
    /// - `client`: 属性宏声明的客户端实例名。
    ///
    /// # 错误
    ///
    /// 重复调用、client 为空、静态构造失败或 handler id 重复时返回注册错误。
    ///
    /// 返回：构造并登记全部匹配项的注册器；无匹配也保存目标名，实际激活占位在 start 取得，任一构造失败则不启动消费。
    pub fn with_collected_for(mut self, client: &str) -> Result<Self> {
        if self.collected_client.is_some() {
            return Err(NafkaError::Registry("静态 consumer 已经激活".into()));
        }
        if client.trim().is_empty() {
            return Err(NafkaError::Registry("collected client 不能为空".into()));
        }
        let mut items: Vec<_> = COLLECTED_CONSUMERS
            .iter()
            .filter(|item| item.client == client)
            .collect();
        // 一个都没匹配上通常意味着 client 名写错。静默启动 0 个 consumer 是最难排查的
        // 故障形态：进程正常跑着，就是什么都不消费。
        // 不能直接报错——只为取激活租约而调用、本就没有静态 consumer 是合法用法——
        // 但必须留下明确信号，并把实际登记的 client 名列出来便于比对。
        if items.is_empty() && !COLLECTED_CONSUMERS.is_empty() {
            let mut known: Vec<&str> = COLLECTED_CONSUMERS.iter().map(|item| item.client).collect();
            known.sort_unstable();
            known.dedup();
            tracing::warn!(
                client,
                known = ?known,
                "collected client 没有匹配到任何静态 consumer（client 名是否写错？）"
            );
        }
        items.sort_by_key(|item| item.id);
        for item in items {
            let consumer = (item.build)().map_err(|error| {
                NafkaError::Registry(format!(
                    "静态 consumer 构造失败 id={} source={}: {error}",
                    item.id, item.source
                ))
            })?;
            self.push_typed(consumer)?;
        }
        self.collected_client = Some(client.to_owned());
        Ok(self)
    }

    /// 业务作用：冻结 registry 并启动每个 resolved group 的 owner 线程。
    ///
    /// 成功仅表示本地 consumer 已构造并通过 startup gate，不表示 broker join 或 assignment 就绪。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组与激活租约登记完成、消费门开放后的生命周期句柄；失败关闭启动门并等待已创建 owner 退出，退出不确定时保留激活占位。
    ///
    /// # 错误
    ///
    /// route 冲突、group 无法解析、静态 client 已被另一运行时激活或 owner 构造失败时返回错误。
    pub async fn start(self) -> Result<ConsumerRuntime> {
        // 覆盖完整本地构造握手，防止 shutdown 在 owner 已 spawn、尚未写入 groups 的
        // 窗口先完成快照并返回。
        let _consumer_lifecycle = self.proxy.inner.consumer_lifecycle_gate.lock().await;
        let collected_client = self.collected_client.clone();
        // 占位必须发生在任何可能失败的步骤之前，并由 RAII 覆盖全部提前返回路径；
        // 只有走到最后的成功分支才把所有权转交运行时。
        let mut lease = if let Some(client) = collected_client.as_deref() {
            Some(ActivationLease::acquire(client)?)
        } else {
            None
        };
        let mut plans: BTreeMap<String, GroupPlan> = BTreeMap::new();
        let instance = broadcast_instance(self.proxy.config());
        let mut routes = BTreeSet::new();
        for consumer in self.typed {
            if matches!(
                consumer.codec(),
                crate::wire::PayloadCodec::Proto(naws_proto::Mode::FastFixed)
            ) {
                return Err(NafkaError::Registry(format!(
                    "consumer `{}` 使用尚未开放的 FAST_FIXED mode",
                    consumer.meta().id
                )));
            }
            let group = resolve_group(self.proxy.config(), &consumer.meta().group, &instance)?;
            for topic in &consumer.meta().topics {
                ensure_route_unique(
                    &mut routes,
                    &group,
                    topic,
                    &consumer.meta().event,
                    consumer.meta().id,
                )?;
            }
            let plan = plans.entry(group.clone()).or_insert_with(|| GroupPlan {
                group,
                mode: OwnerMode::Subscribe {
                    topics: Vec::new(),
                },
                typed: Vec::new(),
                passthrough: Vec::new(),
            });
            add_topics(&mut plan.mode, &consumer.meta().topics);
            plan.typed.push(consumer);
        }
        for consumer in self.passthrough {
            let group = resolve_group(self.proxy.config(), &consumer.meta.group, &instance)?;
            for topic in &consumer.meta.topics {
                ensure_route_unique(
                    &mut routes,
                    &group,
                    topic,
                    &consumer.meta.event,
                    consumer.meta.id,
                )?;
            }
            let plan = plans.entry(group.clone()).or_insert_with(|| GroupPlan {
                group,
                mode: OwnerMode::Subscribe {
                    topics: Vec::new(),
                },
                typed: Vec::new(),
                passthrough: Vec::new(),
            });
            add_topics(&mut plan.mode, &consumer.meta.topics);
            plan.passthrough.push(consumer);
        }
        if plans.is_empty() {
            return Err(NafkaError::Registry("没有注册任何 consumer".into()));
        }
        for plan in plans.values_mut() {
            ensure_group_route_kind(plan)?;
            if let OwnerMode::Subscribe {
                topics,
            } = &mut plan.mode
            {
                topics.sort();
                topics.dedup();
            }
            crate::rd::config::consumer_config(self.proxy.config(), &plan.group)?;
        }
        {
            let existing = self
                .proxy
                .inner
                .groups
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !existing.is_empty() {
                return Err(NafkaError::Lifecycle(
                    "consumer registry 已经冻结并启动".into(),
                ));
            }
            if self.proxy.inner.lifecycle() != crate::Lifecycle::Created {
                return Err(NafkaError::Lifecycle(format!(
                    "运行时已进入 {:?}，不能启动 consumer registry",
                    self.proxy.inner.lifecycle()
                )));
            }
        }
        let startup_gate = Arc::new(StartupGate::new());
        // 事务与普通 owner 不能竞争同一组的提交；生命周期门覆盖双方完整登记过程。
        if plans
            .keys()
            .any(|group| self.proxy.inner.transactions.contains(group))
        {
            return Err(NafkaError::Lifecycle(
                "consumer group 已由事务 owner 占用".into(),
            ));
        }
        let mut started = BTreeMap::new();
        let mut startup_results = Vec::new();
        for (group, plan) in plans {
            match GroupHandle::start(
                Arc::clone(&self.proxy.inner),
                plan,
                Arc::clone(&startup_gate),
            )
            .await
            {
                Ok((handle, startup_result)) => {
                    startup_results.push((group.clone(), startup_result));
                    started.insert(group, handle);
                }
                Err(error) => {
                    startup_gate.abort();
                    let deadline = std::time::Instant::now()
                        + std::time::Duration::from_millis(
                            self.proxy.config().behavior.shutdown_timeout_ms,
                        );
                    rollback_started(&started, deadline, &mut lease).await;
                    return Err(error);
                }
            }
        }

        let startup_results = futures::future::join_all(startup_results.into_iter().map(
            |(group, result)| async move {
                match result.await {
                    Ok(result) => result,
                    Err(_) => Err(NafkaError::Lifecycle(format!(
                        "group `{group}` owner 在本地构造结果返回前退出"
                    ))),
                }
            },
        ))
        .await;
        // 全部本地构造均成功才允许发布组表，单个失败时先封闭共同启动门再等待资源收口。
        if let Some(error) = startup_results.into_iter().find_map(Result::err) {
            startup_gate.abort();
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(
                    self.proxy.config().behavior.shutdown_timeout_ms,
                );
            rollback_started(&started, deadline, &mut lease).await;
            return Err(error);
        }

        let groups: Vec<String> = started.keys().cloned().collect();
        let commit_result = {
            let mut existing = self
                .proxy
                .inner
                .groups
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !existing.is_empty() {
                Err(NafkaError::Lifecycle(
                    "consumer registry 已经冻结并启动".into(),
                ))
            } else if self.proxy.inner.lifecycle() != crate::Lifecycle::Created {
                Err(NafkaError::Lifecycle(format!(
                    "运行时已进入 {:?}，不能启动 consumer registry",
                    self.proxy.inner.lifecycle()
                )))
            } else {
                match self
                    .proxy
                    .inner
                    .transition(crate::Lifecycle::Created, crate::Lifecycle::Running)
                {
                    Ok(()) => {
                        if let Some(lease) = lease.take() {
                            *self
                                .proxy
                                .inner
                                .collected_lease
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                Some(lease.into_owner());
                        }
                        *existing = std::mem::take(&mut started);
                        // 在 groups 写锁内放行：shutdown 只有拿到同一把写锁后才能切换
                        // ShuttingDown，因此不会漏掉已启动但尚未进入 map 的 owner。
                        startup_gate.open();
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
        };
        // 登记未成功不能保留可消费的后台 owner，回滚也必须保住静态 client 的唯一激活权。
        if let Err(error) = commit_result {
            startup_gate.abort();
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(
                    self.proxy.config().behavior.shutdown_timeout_ms,
                );
            rollback_started(&started, deadline, &mut lease).await;
            return Err(error);
        }
        drop(_consumer_lifecycle);
        // 要的是已注册 route 数——`(group, topic, event)` 三元组，即 `routes` 的基数。
        // 不是 handler 数：一个 consumer 挂 5 个 topic 是 5 条 route，按 handler 数会报成 1。
        // 也不是 group 数：多 route 单 group 是常见形态，两者差很远。
        // 标签必须带 client：同进程两个 KafkaProxy 用空标签会互相覆盖同一条序列。
        publish_registry_gauges(&self.proxy.inner, routes.len(), groups.len());
        Ok(ConsumerRuntime::new(self.proxy, groups))
    }

    /// 业务作用：加入一个擦除类型化消费者并检查 id 唯一性。
    ///
    /// 参数说明：
    ///
    /// - `consumer`: 已冻结元数据的擦除消费者。
    ///
    /// # 错误
    ///
    /// handler id 已注册时返回错误。
    ///
    /// 返回：身份在当前注册器内唯一时追加消费者；冲突返回 Registry 且列表不变，此时不做最终组与路由冲突裁决。
    fn push_typed(&mut self, consumer: Arc<dyn ErasedConsumer>) -> Result<()> {
        self.ensure_unique_id(consumer.meta().id)?;
        self.typed.push(consumer);
        Ok(())
    }

    /// 业务作用：检查当前注册器内 typed 与 passthrough 两类 handler id 的共同唯一性。
    ///
    /// 参数说明：
    ///
    /// - `candidate`: 待加入 id。
    ///
    /// # 错误
    ///
    /// id 已存在时返回注册错误。
    ///
    /// 返回：两类已登记消费者中均无精确同名 id 时成功；重复时返回 Registry，不占用名字或检查其它运行时。
    fn ensure_unique_id(&self, candidate: &str) -> Result<()> {
        let exists = self.typed.iter().any(|item| item.meta().id == candidate)
            || self
                .passthrough
                .iter()
                .any(|item| item.meta.id == candidate);
        if exists {
            Err(NafkaError::Registry(format!(
                "consumer id 重复: `{candidate}`"
            )))
        } else {
            Ok(())
        }
    }
}

/// 业务作用：在开放消费前拒绝同组混合消息所有权模型或执行顺序域。
///
/// 参数说明：
/// - `plan`：plan 为已合并全部路由的组计划。
///
/// 返回：全部路由所有权和顺序域一致时成功，否则返回 Registry。
///
/// typed 路由将记录物化后交给业务执行资源，passthrough 只能在 poll 栈内借用；混用无法维持同组的既定交付责任。
fn ensure_group_route_kind(plan: &GroupPlan) -> Result<()> {
    // 顺序域属于整个分区，不能让不同事件在同一个 key 上绕过彼此的串行约束。
    if let Some(first) = plan.typed.first() {
        if plan
            .typed
            .iter()
            .any(|route| route.meta().execution_mode != first.meta().execution_mode)
        {
            return Err(NafkaError::Registry(format!(
                "group `{}` 不能混合 execution mode",
                plan.group
            )));
        }
    }
    if !plan.typed.is_empty() && !plan.passthrough.is_empty() {
        Err(NafkaError::Registry(format!(
            "group `{}` 不能混合 typed 与 passthrough consumer",
            plan.group
        )))
    } else {
        Ok(())
    }
}

/// 业务作用：将默认、命名或广播声明解析为实际消费与提交使用的组身份。
///
/// 参数说明：
/// - `config`：config 为冻结源配置。
/// - `spec`：spec 为路由的组规则。
/// - `instance`：instance 为进程广播实例身份。
///
/// 返回：通过共同边界校验的原文组名；默认组缺失或最终身份非法时为 Config，旧广播声明非法时为 Registry。
fn resolve_group(
    config: &crate::config::KafkaConfig,
    spec: &GroupSpec,
    instance: &str,
) -> Result<String> {
    let group = match spec {
        GroupSpec::Default => config.group_id.clone().ok_or_else(|| {
            NafkaError::Config("GroupSpec::Default 无法解析: config.group_id 未配置".into())
        })?,
        GroupSpec::Named(group) => group.clone(),
        GroupSpec::StableBroadcast {
            scope,
            instance_id,
        } => crate::broadcast_group(
            scope,
            &instance_id
                .clone()
                .unwrap_or_else(|| broadcast_identity_instance(config)),
        )?,
        GroupSpec::Broadcast(scope) => {
            if scope.trim().is_empty() {
                return Err(NafkaError::Registry("Broadcast scope 不能为空".into()));
            }
            let sanitized: String = scope
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                        ch
                    } else {
                        '-'
                    }
                })
                .take(96)
                .collect();
            format!(
                "nasa-kafka-broadcast-{sanitized}-{:016x}-{instance}",
                stable_hash(scope.as_bytes())
            )
        }
    };
    // Default、Named 与派生组在 owner 规划之前收敛到同一身份边界，不截短或另取名称。
    crate::wire::validate_group(&group)?;
    Ok(group)
}

/// 业务作用：生成或读取本进程广播实例标识。
///
/// 参数说明：
///
/// - `config`: 冻结顶层配置。
///
/// 返回：依配置、环境变量、进程临时身份优先级取得原文后，仅保留字母数字及 ._-，其它字符替换为 - 并截取 96 字符；不保证跨进程唯一。
fn broadcast_instance(config: &crate::config::KafkaConfig) -> String {
    broadcast_identity_instance(config)
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '-'
            }
        })
        .take(96)
        .collect()
}

/// 业务作用：取得未经净化的广播实例声明，供共同编码保留完整身份。
///
/// 参数说明：
/// - `config`：config 为冻结配置。
///
/// 返回：精确身份原文；最终身份是否合法由所选组算法在建组前验证。
fn broadcast_identity_instance(config: &crate::config::KafkaConfig) -> String {
    static PROCESS_INSTANCE: OnceLock<String> = OnceLock::new();
    config
        .behavior
        .broadcast_instance_id
        .clone()
        .or_else(|| std::env::var("NASA_KAFKA_BROADCAST_INSTANCE_ID").ok())
        .unwrap_or_else(|| {
            PROCESS_INSTANCE
                .get_or_init(|| {
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |duration| duration.as_nanos());
                    format!("{}-{nanos:x}", std::process::id())
                })
                .clone()
        })
}

/// 业务作用：固定 FNV-1a 64 位哈希，保证广播 group 派生在协议演进期间可复现。
///
/// 参数说明：
///
/// - `bytes`: 原始 scope 字节。
///
/// 返回：按固定初值及 wrapping 乘法计算的 64 位标识，用于区分净化后的广播作用域；不提供密码学抗碰撞或认证能力。
fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

/// 业务作用：向 subscribe plan 合并 topic。
///
/// 参数说明：
///
/// - `mode`: 仅允许 subscribe 模式。
/// - `topics`: route topic 列表。
///
/// 返回：追加 topic 副本到订阅计划，排序去重由后续启动步骤统一完成；固定分配模式违反内部前置条件并 panic。
fn add_topics(mode: &mut OwnerMode, topics: &[String]) {
    match mode {
        OwnerMode::Subscribe {
            topics: all,
        } => all.extend_from_slice(topics),
        OwnerMode::Assign {
            ..
        } => unreachable!("registry start 只构造 subscribe plan"),
    }
}

/// 业务作用：按统一 client 标签发布注册表规模，供启动登记与停机归零使用。
///
/// 参数说明：
///
/// - `inner`: 指标出口。
/// - `routes`: `(group, topic, event)` 三元组数量。
/// - `groups`: 实际建出的 group 数量。
///
/// 返回：无；向 MetricsSink 写入路由数与 group 数，超出 i64 的数量按上限饱和。
pub(crate) fn publish_registry_gauges(
    inner: &crate::KafkaProxyInner,
    routes: usize,
    groups: usize,
) {
    let client = inner.config.client_id_prefix.as_deref().unwrap_or("nafka");
    let labels = [("client", client)];
    inner.gauge(
        "registry_consumers",
        i64::try_from(routes).unwrap_or(i64::MAX),
        &labels,
    );
    inner.gauge(
        "registry_groups",
        i64::try_from(groups).unwrap_or(i64::MAX),
        &labels,
    );
}

/// 业务作用：将 `(group, topic, event)` 写入全局路由集合，并对重复注册返回可定位的稳定错误。
///
/// 参数说明：
/// - `routes`：本次启动各消费者共用的路由身份集合，不跨运行时共享。
/// - `group`：解析完成的消费组身份。
/// - `topic`：该路由订阅的精确 topic 名。
/// - `event`：用于匹配记录的精确事件名。
/// - `id`：当前 handler 标识，仅用于重复路由诊断。
///
/// 返回：首次出现的三元组加入集合并成功；重复返回 Registry，已有集合内容不变，不开放业务路由。
fn ensure_route_unique(
    routes: &mut BTreeSet<(String, String, String)>,
    group: &str,
    topic: &str,
    event: &str,
    id: &str,
) -> Result<()> {
    let route = (group.to_owned(), topic.to_owned(), event.to_owned());
    if routes.insert(route.clone()) {
        Ok(())
    } else {
        Err(NafkaError::Registry(format!(
            "route 冲突 group={} topic={} event={} handler={id}",
            route.0, route.1, route.2
        )))
    }
}

/// 已启动消费者集合的生命周期句柄。
///
/// 关闭仍由所属 [`KafkaProxy`] 统一协调，避免生产者、DLT 与消费者分别关闭产生竞态。
#[derive(Clone)]
pub struct ConsumerRuntime {
    /// 所属 Kafka 运行时。
    proxy: KafkaProxy,
    /// 冻结后的 resolved group 列表。
    groups: Arc<[String]>,
}

impl std::fmt::Debug for ConsumerRuntime {
    /// 业务作用：输出该句柄冻结的组列表及所属客户端名，不展开连接配置。
    ///
    /// 参数说明：
    /// - `f`：接收运行句柄身份摘要的格式化目标。
    ///
    /// 返回：写入身份摘要或传递格式化错误；不查询实时健康或表示这些组当前仍在线。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerRuntime")
            .field("groups", &self.groups)
            .field("client_name", &self.proxy.config().client_name)
            .finish()
    }
}

impl ConsumerRuntime {
    /// 业务作用：由启动器构造生命周期句柄。
    ///
    /// 参数说明：
    ///
    /// - `proxy`: 所属运行时。
    /// - `groups`: 已启动 resolved group 列表。
    ///
    /// 返回：共享源与冻结组列表的句柄；构造本身不启动或校验 owner，也不对输入组名排序。
    #[allow(dead_code)]
    pub(crate) fn new(proxy: KafkaProxy, groups: Vec<String>) -> Self {
        Self {
            proxy,
            groups: groups.into(),
        }
    }

    /// 业务作用：返回稳定排序的 resolved group 列表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动时冻结的组名切片，不随后续 stop_group 或 assign 更新，实时状态须通过所属源查询。
    pub fn groups(&self) -> &[String] {
        &self.groups
    }

    /// 业务作用：返回所属运行时句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前消费者集合所属 KafkaProxy 的借用，供统一控制与关闭；不创建新源或改变任何组生命周期。
    pub fn kafka(&self) -> &KafkaProxy {
        &self.proxy
    }
}

/// 业务作用：读取并校验 passthrough 消费者元数据。
///
/// 参数说明：
///
/// - `consumer`: 待读取一次元数据的同步借用 handler。
///
/// # 错误
///
/// id、event、topic 为空或 topic 重复时返回注册错误。
///
/// 返回：满足共同事件与命名组合同且 topic 不重复的元数据；组非法为 Config，其它元数据非法为 Registry，拒绝时不登记消费路由。
fn passthrough_meta<C: PassthroughConsumer>(consumer: &C) -> Result<PassthroughMeta> {
    // 借用式入口也先冻结并验证命名组，不能比 typed 注册更晚接纳无效消费责任。
    let group = consumer.group();
    if let GroupSpec::Named(value) = &group {
        crate::wire::validate_group(value)?;
    }
    let id = consumer.id();
    let event = consumer.event();
    let mut topics = consumer.topics();
    if id.trim().is_empty() || crate::wire::is_blank_event(&event) || topics.is_empty() {
        return Err(NafkaError::Registry(format!(
            "passthrough consumer `{id}` 元数据为空"
        )));
    }
    if topics.iter().any(|topic| topic.trim().is_empty()) {
        return Err(NafkaError::Registry(format!(
            "passthrough consumer `{id}` topic 为空"
        )));
    }
    let before = topics.len();
    let unique: BTreeSet<&str> = topics.iter().map(String::as_str).collect();
    if unique.len() != before {
        return Err(NafkaError::Registry(format!(
            "passthrough consumer `{id}` topics 存在重复项"
        )));
    }
    topics.sort();
    Ok(PassthroughMeta {
        id,
        topics,
        event,
        group,
        ack_mode: consumer.ack_mode(),
    })
}

/// 业务作用：返回类型化注册项的稳定 route 描述，供注册冲突校验复用。
///
/// 参数说明：
///
/// - `meta`: 冻结消费者元数据。
///
/// 返回：每个 topic 对应的 `(topic,event)` 列表。
#[allow(dead_code)]
pub(crate) fn typed_routes(meta: &ConsumerMeta) -> Vec<(&str, &str)> {
    meta.topics
        .iter()
        .map(|topic| (topic.as_str(), meta.event.as_str()))
        .collect()
}
