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
    Malformed,
    Internal,
}

pub(crate) struct PreparedRecord {
    pub payload: Payload,
    pub route: Option<napart::RouteHash>,
    pub weight: usize,
}

pub(crate) struct ConsumerPlan {
    pub id: u32,
    pub legacy: bool,
    pub decode: Arc<Decode>,
    pub invoke: Arc<Invoke>,
}

impl ConsumerPlan {
    /// 业务作用：为一个消费计划选择永久不复用的有序或无序任务类型。
    /// 参数说明：`ordered` 表示是否有冻结的业务键。
    /// 返回：逐条有序任务使用 strict；无序逐条和兼容批量任务使用独立 relaxed 类型。
    pub(crate) fn spec(&self, ordered: bool) -> napart::TaskSpec {
        if ordered && !self.legacy {
            napart::TaskSpec::strict(napart::TaskType(self.id * 2))
        } else {
            napart::TaskSpec::relaxed(napart::TaskType(self.id * 2 + 1))
        }
    }

    /// 业务作用：冻结单记录计划，业务数据只解码一次，键提取与权重回调展开按内部路由失败隔离。
    /// 参数说明：`id` 为计划身份；`key` 提取可选业务键；`weight` 估算额外堆分配；`handler` 执行业务。
    /// 返回：可跨多个 topic 共用同一顺序域的不可变计划。
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
    /// 参数说明：`id` 为独立计划身份；`handler` 接收成功解码子集的完整 Vec。
    /// 返回：每个物理来源上的批量计划，与单记录计划共享执行器和提交账本。
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
