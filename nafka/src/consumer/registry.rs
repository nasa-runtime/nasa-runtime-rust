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
    /// 放弃释放：owner 线程去向不明时，宁可永久占住该 client 也不放行第二个运行时。
    fn leak_on_uncertain_stop(&mut self) {
        std::mem::forget(self.client.take());
    }
}

/// 回滚已启动的 owner，并在任一 owner 未能在 deadline 内退出时保留激活租约。
///
/// 直接丢弃 `stop_until` 的结果会让租约随 `Drop` 释放，而线程可能仍然存活——
/// 此时第二个同名 client 就能并发启动，违反「同 client 单激活」。
///
/// # 参数
///
/// - `started`: 已经 spawn 的 owner 句柄。
/// - `deadline`: 回滚总截止时刻。
/// - `lease`: 本次启动持有的激活租约。
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
    /// 原子占用指定 client 的激活租约。
    ///
    /// # 参数
    ///
    /// - `client`: 静态收集项绑定的客户端实例名。
    ///
    /// # 错误
    ///
    /// 该 client 已被另一个存活运行时占用时返回注册错误。
    fn acquire(client: &str) -> Result<Self> {
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

    /// 启动成功后把租约交给运行时，由 shutdown 负责释放。
    fn into_owner(mut self) -> String {
        self.client.take().expect("租约只能转交一次")
    }
}

impl Drop for ActivationLease {
    /// 启动未走到成功分支时归还租约，避免失败的一次 start 永久占住 client 名。
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            release_activation(&client);
        }
    }
}

/// 释放一个静态收集激活租约。
///
/// 由 `KafkaProxy::shutdown` 在所有 owner 线程退出、运行时进入 Stopped 后调用
///。不释放会让同一进程再也无法用相同 collected client
/// 重建运行时，热重载与 restart 型运维流程都会被永久拒绝。
///
/// # 参数
///
/// - `client`: 启动时占用的客户端实例名。
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
    /// 创建空注册 builder。
    ///
    /// # 参数
    ///
    /// - `proxy`: 所属运行时。
    pub(crate) fn new(proxy: KafkaProxy) -> Self {
        Self {
            proxy,
            typed: Vec::new(),
            passthrough: Vec::new(),
            collected_client: None,
        }
    }

    /// 注册有状态单条消费者实例。
    ///
    /// # 参数
    ///
    /// - `consumer`: 已完成依赖注入的实例。
    ///
    /// # 错误
    ///
    /// 元数据非法或 handler id 重复时返回注册错误。
    pub fn register<C: SingleConsumer>(mut self, consumer: C) -> Result<Self> {
        let erased = erase_single(consumer)?;
        self.push_typed(erased)?;
        Ok(self)
    }

    /// 注册有状态批消费者实例。
    ///
    /// # 参数
    ///
    /// - `consumer`: 已完成依赖注入的实例。
    ///
    /// # 错误
    ///
    /// 元数据非法或 handler id 重复时返回注册错误。
    pub fn register_batch<C: BatchConsumer>(mut self, consumer: C) -> Result<Self> {
        let erased = erase_batch(consumer)?;
        self.push_typed(erased)?;
        Ok(self)
    }

    /// 注册同步借用式少拷贝消费者。
    ///
    /// # 参数
    ///
    /// - `consumer`: 已完成本地路由和 outbox 注入的同步 handler。
    ///
    /// # 错误
    ///
    /// 元数据非法或 handler id 重复时返回注册错误。
    pub fn register_passthrough<C: PassthroughConsumer>(mut self, consumer: C) -> Result<Self> {
        let meta = passthrough_meta(&consumer)?;
        self.ensure_unique_id(meta.id)?;
        self.passthrough.push(RegisteredPassthrough {
            handler: Arc::new(consumer),
            meta,
        });
        Ok(self)
    }

    /// 激活与当前配置 `client_name` 匹配的静态属性宏消费者。
    ///
    /// # 错误
    ///
    /// 重复调用、静态构造失败或 handler id 重复时返回注册错误。
    pub fn with_collected(self) -> Result<Self> {
        let client = self.proxy.config().client_name.clone();
        self.with_collected_for(&client)
    }

    /// 激活与指定客户端名匹配的静态属性宏消费者。
    ///
    /// # 参数
    ///
    /// - `client`: 属性宏声明的客户端实例名。
    ///
    /// # 错误
    ///
    /// 重复调用、client 为空、静态构造失败或 handler id 重复时返回注册错误。
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

    /// 冻结 registry 并启动每个 resolved group 的 owner 线程。
    ///
    /// 成功仅表示本地 consumer 已构造并通过 startup gate，不表示 broker join 或 assignment 就绪。
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
                mode: OwnerMode::Subscribe { topics: Vec::new() },
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
                mode: OwnerMode::Subscribe { topics: Vec::new() },
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
            if let OwnerMode::Subscribe { topics } = &mut plan.mode {
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
        let mut started = BTreeMap::new();
        let mut startup_results = Vec::new();
        for (group, plan) in plans {
            match GroupHandle::start(
                Arc::clone(&self.proxy.inner),
                plan,
                Arc::clone(&startup_gate),
            ) {
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

    /// 加入一个擦除类型化消费者并检查 id 唯一性。
    ///
    /// # 参数
    ///
    /// - `consumer`: 已冻结元数据的擦除消费者。
    ///
    /// # 错误
    ///
    /// handler id 已注册时返回错误。
    fn push_typed(&mut self, consumer: Arc<dyn ErasedConsumer>) -> Result<()> {
        self.ensure_unique_id(consumer.meta().id)?;
        self.typed.push(consumer);
        Ok(())
    }

    /// 检查 typed 与 passthrough 两类 handler id 的全局唯一性。
    ///
    /// # 参数
    ///
    /// - `candidate`: 待加入 id。
    ///
    /// # 错误
    ///
    /// id 已存在时返回注册错误。
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

/// 保证同一个 resolved group 只采用一种消息所有权模型。
///
/// typed route 会把记录物化后进入异步回调，passthrough route 则只能在 poll 栈内借用；二者混合会让
/// owner 无法在不复制或重排的前提下维持 broker 交付顺序，因此启动时必须拒绝。
///
/// # 参数
///
/// - `plan`: 已合并完 route 的 group 计划。
///
/// # 错误
///
/// 同时存在 typed 与 passthrough route 时返回注册错误。
fn ensure_group_route_kind(plan: &GroupPlan) -> Result<()> {
    if !plan.typed.is_empty() && !plan.passthrough.is_empty() {
        Err(NafkaError::Registry(format!(
            "group `{}` 不能混合 typed 与 passthrough consumer",
            plan.group
        )))
    } else {
        Ok(())
    }
}

/// 解析 route 的最终 group.id。
///
/// # 参数
///
/// - `config`: 冻结顶层配置。
/// - `spec`: route 的 group 规则。
/// - `instance`: 本进程广播实例标识。
///
/// # 错误
///
/// Default 无回退 group、Named/Broadcast 为空或最终长度超限时返回注册错误。
fn resolve_group(
    config: &crate::config::KafkaConfig,
    spec: &GroupSpec,
    instance: &str,
) -> Result<String> {
    let group = match spec {
        GroupSpec::Default => config.group_id.clone().ok_or_else(|| {
            NafkaError::Registry("GroupSpec::Default 无法解析: config.group_id 未配置".into())
        })?,
        GroupSpec::Named(group) => group.clone(),
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
    if group.trim().is_empty() {
        return Err(NafkaError::Registry("最终 group.id 不能为空".into()));
    }
    if group.len() > 255 {
        return Err(NafkaError::Registry(format!(
            "最终 group.id 长度超限: {} > 255",
            group.len()
        )));
    }
    Ok(group)
}

/// 生成或读取本进程广播实例标识。
///
/// # 参数
///
/// - `config`: 冻结顶层配置。
fn broadcast_instance(config: &crate::config::KafkaConfig) -> String {
    static PROCESS_INSTANCE: OnceLock<String> = OnceLock::new();
    let raw = config
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
        });
    raw.chars()
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

/// 固定 FNV-1a 64 位哈希，保证广播 group 派生跨版本可复现。
///
/// # 参数
///
/// - `bytes`: 原始 scope 字节。
fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

/// 向 subscribe plan 合并 topic。
///
/// # 参数
///
/// - `mode`: 仅允许 subscribe 模式。
/// - `topics`: route topic 列表。
fn add_topics(mode: &mut OwnerMode, topics: &[String]) {
    match mode {
        OwnerMode::Subscribe { topics: all } => all.extend_from_slice(topics),
        OwnerMode::Assign { .. } => unreachable!("registry start 只构造 subscribe plan"),
    }
}

/// 检查 `(group,topic,event)` 路由唯一性。
///
/// # 参数
///
/// - `routes`: 已占用 route 集合。
/// - `group`: 最终 group.id。
/// - `topic`: topic。
/// - `event`: event。
/// - `id`: 当前 handler id。
///
/// # 错误
///
/// route 已被其他 handler 占用时返回注册错误。
/// 发布注册表规模指标。
///
/// 单独抽出来是为了让"注册成功"与"停机归零"用同一份标签集合与同一份口径，
/// 否则两处各写一遍必然漂移（此前只在 `start()` 成功末尾发一次、且从不归零，
/// consumer 停掉之后仪表盘仍显示旧值）。
///
/// # 参数
///
/// - `inner`: 指标出口。
/// - `routes`: `(group, topic, event)` 三元组数量。
/// - `groups`: 实际建出的 group 数量。
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

/// 将 `(group, topic, event)` 写入全局路由集合，并对重复注册返回可定位的稳定错误。
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
    /// 输出已启动 group 与所属运行时状态；不含配置凭据。
    ///
    /// `start()` 返回 `Result<ConsumerRuntime>`，缺少 `Debug` 会让调用方无法使用
    /// `unwrap_err` / `expect_err` 之类的标准写法。
    ///
    /// - `f`: 调用方提供的格式化缓冲区。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerRuntime")
            .field("groups", &self.groups)
            .field("client_name", &self.proxy.config().client_name)
            .finish()
    }
}

impl ConsumerRuntime {
    /// 由启动器构造生命周期句柄。
    ///
    /// # 参数
    ///
    /// - `proxy`: 所属运行时。
    /// - `groups`: 已启动 resolved group 列表。
    #[allow(dead_code)]
    pub(crate) fn new(proxy: KafkaProxy, groups: Vec<String>) -> Self {
        Self {
            proxy,
            groups: groups.into(),
        }
    }

    /// 返回稳定排序的 resolved group 列表。
    pub fn groups(&self) -> &[String] {
        &self.groups
    }

    /// 返回所属运行时句柄。
    pub fn kafka(&self) -> &KafkaProxy {
        &self.proxy
    }
}

/// 读取并校验 passthrough 消费者元数据。
///
/// # 参数
///
/// - `consumer`: 待读取一次元数据的同步借用 handler。
///
/// # 错误
///
/// id、event、topic 为空或 topic 重复时返回注册错误。
fn passthrough_meta<C: PassthroughConsumer>(consumer: &C) -> Result<PassthroughMeta> {
    let id = consumer.id();
    let event = consumer.event();
    let mut topics = consumer.topics();
    if id.trim().is_empty() || event.trim().is_empty() || topics.is_empty() {
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
        group: consumer.group(),
        ack_mode: consumer.ack_mode(),
    })
}

/// 返回类型化注册项的稳定 route 描述，供 P3 冲突校验复用。
///
/// # 参数
///
/// - `meta`: 冻结消费者元数据。
///
/// # 返回
///
/// 每个 topic 对应的 `(topic,event)` 列表。
#[allow(dead_code)]
pub(crate) fn typed_routes(meta: &ConsumerMeta) -> Vec<(&str, &str)> {
    meta.topics
        .iter()
        .map(|topic| (topic.as_str(), meta.event.as_str()))
        .collect()
}
