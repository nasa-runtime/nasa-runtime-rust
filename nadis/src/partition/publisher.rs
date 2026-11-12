//! 发布登记先于序列化与网络发送，调用方取消等待不会撤销已受理的发送责任。

use super::{PartitionLimits, DATA_FIELD};
use crate::{
    client::RedisClient,
    error::{NasaRedisError, Result},
};
use serde::Serialize;
use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex},
};
use tokio::sync::{oneshot, Notify};

/// 单一发布 owner 的当前责任与累计结果；XADD 成功不代表业务已消费。
#[derive(Debug, Clone, Default)]
pub struct PublisherSnapshot {
    /// 已经登记但尚未终结的发布票据数。
    pub inflight: usize,
    /// 在途发布占用的正文预留字节数，包含序列化前的最坏大小预留。
    pub payload_bytes: usize,
    /// 已取得 XADD 成功响应的累计发布数。
    pub succeeded: u64,
    /// 已登记但在发送前确定失败的累计发布数。
    pub failed_before_send: u64,
    /// 交给发送监督后未取得确定结果的累计发布数，不能据此安全重发。
    pub outcome_unknown: u64,
    /// 是否已关闭新发布准入，既有票据仍需完成排干。
    pub closed: bool,
    /// 仍由发布 owner 持有、尚未完成 join 回收的发送任务数。
    pub unjoined_tasks: usize,
}

#[derive(Default)]
struct State {
    next: u64,
    tickets: HashMap<u64, usize>,
    snapshot: PublisherSnapshot,
    tasks: HashMap<u64, tokio::task::JoinHandle<()>>,
    forced: bool,
}

struct PublishCompletion {
    owner: Arc<PublisherCoordinator>,
    ticket: u64,
    kind: u8,
}

impl Drop for PublishCompletion {
    /// 业务作用：发送任务完成或被中止后终结票据，中止已发命令只归为结果未知。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：归还发布预算并通知排干者，不自动重发 XADD。
    fn drop(&mut self) {
        self.owner.finish(self.ticket, self.kind);
    }
}

pub(super) struct PublisherCoordinator {
    state: Mutex<State>,
    limits: PartitionLimits,
    changed: Notify,
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedWriter {
    /// 业务作用：在扩展 JSON 缓冲前检查序列化上限，防止先分配超大正文再拒绝。
    ///
    /// 参数说明：
    /// - `bytes`: serializer 本次写出的编码片段，与已缓存字节共同计入单条记录上限。
    ///
    /// 返回：容量足够时完整写入；不足时不写该片段并返回错误。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(std::io::Error::other(
                "partition publish exceeds max_record_bytes",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    /// 业务作用：内存 JSON writer 没有外部待刷数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无副作用成功。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl PublisherCoordinator {
    /// 业务作用：建立当前代理独占发布准入与票据注册表。
    ///
    /// 参数说明：
    /// - `limits`: 已验证并冻结的共享发布数量、正文预留与总字节预算。
    ///
    /// 返回：开放但尚无在途 XADD 的协调器。
    pub(super) fn new(limits: PartitionLimits) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                next: 1,
                ..Default::default()
            }),
            limits,
            changed: Notify::new(),
        })
    }

    /// 业务作用：在登记与预留完成后有界序列化，并由独立票据负责 XADD 至本地终态。
    ///
    /// 参数说明：
    /// - `client`: 持有目标 Redis 连接的共享客户端，发送任务保留到取得本地结果。
    /// - `stream`: 已经按目标组与业务键选定的完整物理 Stream 键。
    /// - `topic`: 写入信封的非空主题，供消费计划选择处理器。
    /// - `event`: 主题内的非空事件名。
    /// - `data`: 需要在单条正文上限内编码的业务对象。
    /// - `passthrough`: 显式透传的上下文字段；None 时不写入该字段。
    ///
    /// 返回：取得合法非零 XADD 记录 ID 时成功；路由、运行时、准入或序列化错误确定发生在发送前。
    /// 发送监督接手后的错误返回 PublishOutcomeUnknown；调用方取消等待不会撤销已受理的发送。
    pub(super) async fn publish<T: Serialize>(
        self: &Arc<Self>,
        client: Arc<RedisClient>,
        stream: String,
        topic: &str,
        event: &str,
        data: &T,
        passthrough: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<String> {
        super::activation::validate_route(topic, event)?;
        // 票据登记前必须取得可派发的运行时，避免持有注册表锁时因缺少执行上下文而触发析构重入。
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            NasaRedisError::NotExecuted("partition publish requires a Tokio runtime".into())
        })?;
        self.reap_finished().await;
        let ticket = {
            let mut state = self.state.lock().expect("publish registry");
            // 登记与最坏正文预留共用一个屏障；准入关闭或容量不足时必须在调用用户序列化逻辑前拒绝。
            if state.snapshot.closed
                || state.tickets.len()
                    + state
                        .tasks
                        .keys()
                        .filter(|id| !state.tickets.contains_key(id))
                        .count()
                    >= self.limits.max_inflight_publishes
                || state
                    .snapshot
                    .payload_bytes
                    .checked_add(self.limits.max_record_bytes)
                    .is_none_or(|n| n > self.limits.max_inflight_publish_bytes)
            {
                return Err(NasaRedisError::NotExecuted(
                    "partition publish admission closed or exhausted".into(),
                ));
            }
            let id = state.next;
            state.next = id.checked_add(1).ok_or_else(|| {
                NasaRedisError::NotExecuted("publisher identity exhausted".into())
            })?;
            state.tickets.insert(id, self.limits.max_record_bytes);
            state.snapshot.payload_bytes += self.limits.max_record_bytes;
            state.snapshot.inflight += 1;
            id
        };
        #[derive(Serialize)]
        struct WireEnvelope<'a, T> {
            topic: &'a str,
            event: &'a str,
            data: &'a T,
            #[serde(skip_serializing_if = "Option::is_none")]
            passthrough: Option<serde_json::Map<String, serde_json::Value>>,
        }
        let serialized = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut writer = BoundedWriter {
                bytes: Vec::with_capacity(self.limits.max_record_bytes),
                limit: self.limits.max_record_bytes,
            };
            serde_json::to_writer(
                &mut writer,
                &WireEnvelope {
                    topic,
                    event,
                    data,
                    passthrough,
                },
            )
            .map(|_| writer.bytes)
        }));
        let mut body = match serialized {
            Ok(Ok(body)) => body,
            result => {
                self.finish(ticket, 1);
                return Err(NasaRedisError::Codec(match result {
                    // 自定义 serializer 可在错误中携带正文；只保留固定类别和位置，避免泄露或无界摘要。
                    Ok(Err(error)) => format!(
                        "partition serialization failed: {:?} at line {} column {}",
                        error.classify(),
                        error.line(),
                        error.column()
                    ),
                    _ => "partition serializer panic".into(),
                }));
            }
        };
        body.shrink_to_fit();
        let (sender, receiver) = oneshot::channel();
        let completion = PublishCompletion {
            owner: self.clone(),
            ticket,
            kind: 2,
        };
        let unclaimed = {
            let mut state = self.state.lock().expect("publish registry");
            if state.forced {
                // 强停发生在同步序列化期间时仍可确认未发送；先归还票据，再拒绝派发 XADD。
                drop(state);
                self.finish(ticket, 1);
                return Err(NasaRedisError::NotExecuted(
                    "publisher force stop before send".into(),
                ));
            }
            let previous = state
                .tickets
                .insert(ticket, body.len())
                .expect("registered publish");
            state.snapshot.payload_bytes -= previous - body.len();
            let (dispatch, dispatched) = oneshot::channel::<PublishCompletion>();
            // 已关闭的运行时可同步析构新任务；先创建不持有票据的等待者，避免在注册表锁内重入 finish。
            let handle = runtime.spawn(async move {
                let Ok(mut completion) = dispatched.await else {
                    return;
                };
                let result = redis::cmd("XADD")
                    .arg(stream)
                    .arg("*")
                    .arg(DATA_FIELD)
                    .arg(&body)
                    .query_async::<String>(&mut client.conn())
                    .await;
                drop(body);
                let result = match result {
                    Ok(id) => {
                        // XADD 已经发出，畸形回包不能证明成功，也不能证明未写入；保留未知结果以禁止安全重发的误判。
                        let valid = id.split_once('-').is_some_and(|(millis, sequence)| {
                            if millis.is_empty()
                                || sequence.is_empty()
                                || !millis.bytes().all(|byte| byte.is_ascii_digit())
                                || !sequence.bytes().all(|byte| byte.is_ascii_digit())
                            {
                                return false;
                            }
                            matches!(
                                (millis.parse::<u64>(), sequence.parse::<u64>()),
                                (Ok(millis), Ok(sequence)) if millis != 0 || sequence != 0
                            )
                        });
                        if valid {
                            completion.kind = 0;
                            Ok(id)
                        } else {
                            Err(NasaRedisError::PublishOutcomeUnknown(
                                "XADD returned an invalid stream entry ID".into(),
                            ))
                        }
                    }
                    // Redis 错误详情可能含服务端文本或原始响应；仅保留固定错误类别供诊断。
                    Err(error) => Err(NasaRedisError::PublishOutcomeUnknown(format!(
                        "partition XADD failed: {:?}",
                        error.kind()
                    ))),
                };
                let _ = sender.send(result);
                drop(completion);
            });
            state.tasks.insert(ticket, handle);
            // 监督句柄登记后才移交票据并允许发送；派发与强停共用锁，不能遗漏一个已放行的任务。
            dispatch.send(completion).err()
        };
        // 执行器拒收任务时，未移交的票据必须在解锁后终结，后续 drain 仍会回收已退出的句柄。
        drop(unclaimed);
        receiver.await.unwrap_or_else(|_| {
            Err(NasaRedisError::PublishOutcomeUnknown(
                "publisher supervisor unavailable".into(),
            ))
        })
    }

    /// 业务作用：原子终结一个发布票据并归还字节预算，保留累计结果供停机观测。
    ///
    /// 参数说明：
    /// - `ticket`: 当前协调器签发的唯一发布责任身份。
    /// - `kind`: 本地结果分类，零为成功、一为发送前失败，其余为结果未知。
    ///
    /// 返回：重复终结无副作用，累计计数饱和时仍归还全部票据预算，并唤醒等待排干者。
    fn finish(&self, ticket: u64, kind: u8) {
        let mut state = self.state.lock().expect("publish registry");
        // 仅持有当前票据时才能归还责任；重复完成或中止后的迟到通知不能扣减其它发布预算。
        if let Some(bytes) = state.tickets.remove(&ticket) {
            state.snapshot.payload_bytes -= bytes;
            state.snapshot.inflight -= 1;
            // 观测累计值耗尽不能阻断真实责任回收，否则完成回调会在注册表锁内失败并拖住停机。
            let counter = match kind {
                0 => &mut state.snapshot.succeeded,
                1 => &mut state.snapshot.failed_before_send,
                _ => &mut state.snapshot.outcome_unknown,
            };
            *counter = counter.saturating_add(1);
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：同步关闭发布根准入，已有票据继续负责网络结果。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：后续 publish 在序列化前被拒绝。
    pub(super) fn close(&self) {
        // 关闭只拒绝后续登记，已有票据仍需完成或明确归为未知，不能提前归还其发送预算。
        self.state.lock().expect("publish registry").snapshot.closed = true;
    }

    /// 业务作用：显式强停关闭发布屏障并请求中止尚未结束的发送任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同步发布强停状态，尚未派发的序列化调用不能继续 XADD；已派发任务等待实际退出后归还责任。
    /// 本地中止不能撤销远端可能完成的命令，未取得成功结果的等待者按结果未知处理。
    pub(super) fn force(&self) {
        let mut state = self.state.lock().expect("publish registry");
        // 先禁止所有后继派发再请求中止，保证正在序列化的旧票据也会在发送屏障内看到强停。
        state.snapshot.closed = true;
        state.forced = true;
        for task in state.tasks.values() {
            task.abort();
        }
    }

    /// 业务作用：等待所有登记发布到达成功、发送前失败或未知的本地终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发布票据与字节预算全部归零后返回。
    pub(super) async fn drain(&self) {
        loop {
            self.reap_finished().await;
            let changed = self.changed.notified();
            let snapshot = self.snapshot();
            if snapshot.inflight == 0 && snapshot.unjoined_tasks == 0 {
                return;
            }
            tokio::select! {
                _ = changed => {
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {
                }
            }
        }
    }

    /// 业务作用：回收已终结发布任务的真实 join 证明，保持监督集合与发布预算一起有界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅取走已终结任务并 await，进行中的发送继续由注册表拥有。
    async fn reap_finished(&self) {
        let handles = {
            let mut state = self.state.lock().expect("publish registry");
            let ids: Vec<_> = state
                .tasks
                .iter()
                .filter(|(_, task)| task.is_finished())
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| state.tasks.remove(&id))
                .collect::<Vec<_>>()
        };
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// 业务作用：取得低基数发布容量与结果快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：一次注册表锁内的一致计数。
    pub(super) fn snapshot(&self) -> PublisherSnapshot {
        let state = self.state.lock().expect("publish registry");
        let mut snapshot = state.snapshot.clone();
        snapshot.unjoined_tasks = state.tasks.len();
        snapshot
    }
}
