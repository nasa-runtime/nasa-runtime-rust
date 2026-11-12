//! 消费计划冻结解码、业务键和调用边界；具体业务键不会进入任务账本。

use super::Envelope;
use futures::{future::BoxFuture, FutureExt};
use serde::de::DeserializeOwned;
use std::{any::Any, collections::HashMap, hash::Hash, sync::Arc};

/// Stream 中一条记录的完整身份；不同 stream 的相同 entry id 不是同一记录。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordIdentity {
    /// Redis 中完整的物理 Stream 键名。
    pub stream: Arc<str>,
    /// 该 Stream 内的 entry ID，须与 stream 一起标识记录。
    pub id: Arc<str>,
}

/// 一次业务调用持有的已解码消息与显式透传上下文。
#[derive(Debug)]
pub struct PartitionRecord<T> {
    /// 消息的物理 Stream 与 entry ID。
    pub identity: RecordIdentity,
    /// 信封中用于匹配消费计划的主题。
    pub topic: Arc<str>,
    /// 信封中用于匹配消费计划的事件名。
    pub event: Arc<str>,
    /// 按消费计划目标类型完成解码的业务正文。
    pub data: T,
    /// 信封显式携带的透传上下文；缺省时为 None。
    pub passthrough: Option<Arc<serde_json::Map<String, serde_json::Value>>>,
}

pub(crate) type Payload = Box<dyn Any + Send>;
pub(crate) type PlanMap = HashMap<(String, String), Arc<ConsumerPlan>>;
pub(crate) type Decode =
    dyn Fn(RecordIdentity, Envelope) -> Result<PreparedRecord, DecodeFailure> + Send + Sync;
pub(crate) type Invoke =
    dyn Fn(Vec<(String, Payload)>) -> BoxFuture<'static, Vec<String>> + Send + Sync;

/// 解码失败与程序路由失败具有不同处置权限，程序路由失败不得自动丢弃。
#[derive(Debug, Clone, Copy)]
pub(crate) enum DecodeFailure {
    /// 信封或业务类型不符合编码合同，处置权限仍由消费模式决定。
    Malformed,
    /// 路由或应用回调无法安全完成，不能按消息内容错误自动丢弃。
    Internal,
}

pub(crate) struct PreparedRecord {
    /// 按计划类型解码后唯一持有的业务对象，调用时须按同一计划还原类型。
    pub payload: Payload,
    /// 业务键的稳定哈希；None 表示无业务键约束。
    pub route: Option<napart::RouteHash>,
    /// 解码对象的估算存活字节，不包含另行计量的原始信封字节。
    pub weight: usize,
}

pub(crate) struct ConsumerPlan {
    /// 当前来源内不复用的计划身份，也是有序与无序任务类型的编号基础。
    pub id: u32,
    /// 是否采用成功解码子集一次批量调用的兼容行为。
    pub legacy: bool,
    /// 产生业务对象、路由与字节估算的冻结回调。
    pub decode: Arc<Decode>,
    /// 消耗已准备正文并仅返回失败 ID 的业务调用入口。
    pub invoke: Arc<Invoke>,
}

impl ConsumerPlan {
    /// 业务作用：为一个消费计划选择永久不复用的有序或无序任务类型。
    ///
    /// 参数说明：
    /// - `ordered`: 当前记录是否具有已冻结的业务键；兼容批量计划始终使用无序任务类型。
    ///
    /// 返回：逐条有序任务使用 strict 偶数类型，其余使用 relaxed 奇数类型；
    /// 计划身份由登记入口预留双倍空间，类型编号不因重试或租约变化而复用。
    pub(crate) fn spec(&self, ordered: bool) -> napart::TaskSpec {
        if ordered && !self.legacy {
            napart::TaskSpec::strict(napart::TaskType(self.id * 2))
        } else {
            napart::TaskSpec::relaxed(napart::TaskType(self.id * 2 + 1))
        }
    }

    /// 业务作用：冻结单记录计划，业务数据只解码一次，键提取与权重回调展开按内部路由失败隔离。
    ///
    /// 参数说明：
    /// - `id`: 登记入口签发的唯一计划身份，已为有序和无序类型编号保留算术空间。
    /// - `key`: 从解码正文提取可选业务键的纯回调，键被哈希为路由值后不保留原值。
    /// - `weight`: 估算业务对象额外堆分配字节数的回调，与 T 的静态大小共同计入正文预算。
    /// - `handler`: 逐条接收完整记录的异步业务回调，同一次调用桶中的记录按给定次序等待完成。
    ///
    /// 返回：可跨多个 topic 共用顺序域的不可变计划；解码错误分类为 Malformed，
    /// 键或权重回调展开分类为 Internal，调用端负责捕获反序列化展开；业务错误、展开或类型不符只汇总失败 ID。
    pub(crate) fn typed<T, K, KF, WF, H, Fut>(id: u32, key: KF, weight: WF, handler: H) -> Arc<Self>
    where
        T: DeserializeOwned + Send + 'static,
        K: Hash,
        KF: Fn(&T) -> Option<K> + Send + Sync + 'static,
        WF: Fn(&T) -> usize + Send + Sync + 'static,
        H: Fn(PartitionRecord<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Arc::new(Self {
            id,
            legacy: false,
            decode: Arc::new(move |identity, env| {
                let data: T =
                    serde_json::from_value(env.data).map_err(|_| DecodeFailure::Malformed)?;
                // 无法稳定计算键或权重时不能把记录当作无序数据放行，也不能按毒消息自动丢弃。
                let (route, weight) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        (
                            key(&data).map(|key| napart::RouteHash::from_key(&key)),
                            weight(&data),
                        )
                    }))
                    .map_err(|_| DecodeFailure::Internal)?;
                Ok(PreparedRecord {
                    payload: Box::new(PartitionRecord {
                        identity,
                        topic: Arc::from(env.topic),
                        event: Arc::from(env.event),
                        data,
                        passthrough: env.passthrough.map(Arc::new),
                    }),
                    route,
                    weight: weight.saturating_add(std::mem::size_of::<T>()),
                })
            }),
            invoke: Arc::new(move |items| {
                let handler = handler.clone();
                Box::pin(async move {
                    let mut failed = Vec::new();
                    for (id, payload) in items {
                        // 同时覆盖 Future 构造与轮询展开，保留原记录 ID 交给账本决定重试，不暴露业务错误正文。
                        let result = std::panic::AssertUnwindSafe(async {
                            match payload.downcast::<PartitionRecord<T>>() {
                                Ok(record) => handler(*record).await,
                                Err(_) => Err("consumer plan payload type mismatch".into()),
                            }
                        })
                        .catch_unwind()
                        .await;
                        if !matches!(result, Ok(Ok(()))) {
                            failed.push(id);
                        }
                    }
                    failed
                })
            }),
        })
    }

    /// 业务作用：把旧批量入口冻结为一个调用桶，维持成功解码子集一次 Vec 调用的合同。
    ///
    /// 参数说明：
    /// - `id`: 登记入口签发且预留双倍类型编号空间的独立计划身份。
    /// - `handler`: 一次接收本调用桶中类型匹配的全部正文，返回整体成功或整体失败的异步回调。
    ///
    /// 返回：共享执行器和提交账本的兼容批量计划；解码错误分类为 Malformed，
    /// 解码展开由调用端隔离；类型不匹配的 ID 单独失败，批量回调失败或展开则把全部已交付 ID 归为失败。
    pub(crate) fn legacy<T, F, Fut>(id: u32, handler: F) -> Arc<Self>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Arc::new(Self {
            id,
            legacy: true,
            decode: Arc::new(|_, env| {
                let data: T =
                    serde_json::from_value(env.data).map_err(|_| DecodeFailure::Malformed)?;
                Ok(PreparedRecord {
                    payload: Box::new(data),
                    route: None,
                    weight: std::mem::size_of::<T>(),
                })
            }),
            invoke: Arc::new(move |items| {
                let handler = handler.clone();
                Box::pin(async move {
                    let mut values = Vec::with_capacity(items.len());
                    let mut failed = Vec::new();
                    let mut good = Vec::new();
                    for (id, payload) in items {
                        match payload.downcast::<T>() {
                            Ok(value) => {
                                good.push(id);
                                values.push(*value);
                            }
                            Err(_) => failed.push(id),
                        }
                    }
                    // 空的成功子集不进入业务回调；非空子集只有整次回调确定成功，才允许账本继续确认这些记录。
                    if !values.is_empty()
                        && !matches!(
                            std::panic::AssertUnwindSafe(async move { handler(values).await })
                                .catch_unwind()
                                .await,
                            Ok(Ok(()))
                        )
                    {
                        failed.extend(good);
                    }
                    failed
                })
            }),
        })
    }
}
