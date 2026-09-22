//! 发布登记先于序列化与网络发送，调用方取消等待不会撤销已登记 XADD。

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

#[derive(Debug, Clone, Default)]
pub struct PublisherSnapshot {
    pub inflight: usize,
    pub payload_bytes: usize,
    pub succeeded: u64,
    pub failed_before_send: u64,
    pub outcome_unknown: u64,
    pub closed: bool,
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
    /// 参数说明: 无。
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
    /// 参数说明：`bytes` 为 serializer 本轮写入片段。
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
    /// 参数说明: 无。
    /// 返回：无副作用成功。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl PublisherCoordinator {
    /// 业务作用：建立当前代理独占发布准入与票据注册表。
    /// 参数说明：`limits` 为冻结的共享发布预算。
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
    /// 参数说明：`client` 为连接；`stream` 为已路由来源；`topic`、`event`、`data`、`passthrough` 为既有信封字段。
    /// 返回：成功返回 ID；发送前错误可确定未发送；发送后的不确定响应返回 PublishOutcomeUnknown。
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
        self.reap_finished().await;
        let ticket = {
            let mut state = self.state.lock().expect("publish registry");
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
                    Ok(Err(error)) => error.to_string(),
                    _ => "partition serializer panic".into(),
                }));
            }
        };
        body.shrink_to_fit();
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock().expect("publish registry");
            if state.forced {
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
            let mut completion = PublishCompletion {
                owner: self.clone(),
                ticket,
                kind: 2,
            };
            // 此处起发送责任由监督票据拥有；外部 Future 取消只结束等待，不中断潜在已发送命令。
            let handle = tokio::spawn(async move {
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
                        completion.kind = 0;
                        Ok(id)
                    }
                    Err(error) => Err(NasaRedisError::PublishOutcomeUnknown(error.to_string())),
                };
                let _ = sender.send(result);
                drop(completion);
            });
            state.tasks.insert(ticket, handle);
        }
        receiver.await.unwrap_or_else(|_| {
            Err(NasaRedisError::PublishOutcomeUnknown(
                "publisher supervisor unavailable".into(),
            ))
        })
    }

    /// 业务作用：原子终结一个发布票据并归还字节预算，保留累计结果供停机观测。
    /// 参数说明：`ticket` 为唯一身份；`kind` 区分成功、发送前失败与结果未知。
    /// 返回：重复终结无副作用，等待排干者被唤醒。
    fn finish(&self, ticket: u64, kind: u8) {
        let mut state = self.state.lock().expect("publish registry");
        if let Some(bytes) = state.tickets.remove(&ticket) {
            state.snapshot.payload_bytes -= bytes;
            state.snapshot.inflight -= 1;
            match kind {
                0 => state.snapshot.succeeded += 1,
                1 => state.snapshot.failed_before_send += 1,
                _ => state.snapshot.outcome_unknown += 1,
            }
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：同步关闭发布根准入，已有票据继续负责网络结果。
    /// 参数说明: 无。
    /// 返回：后续 publish 在序列化前被拒绝。
    pub(super) fn close(&self) {
        self.state.lock().expect("publish registry").snapshot.closed = true;
    }

    /// 业务作用：显式强停关闭发布屏障并中止在途发送，等待者收到结果未知。
    /// 参数说明: 无。
    /// 返回：已登记发送受同一屏障约束，尚未发送的序列化调用不能越过强停继续 XADD。
    pub(super) fn force(&self) {
        let mut state = self.state.lock().expect("publish registry");
        state.snapshot.closed = true;
        state.forced = true;
        for task in state.tasks.values() {
            task.abort();
        }
    }

    /// 业务作用：等待所有登记发布到达成功、发送前失败或未知的本地终态。
    /// 参数说明: 无。
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
    /// 参数说明: 无。
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
    /// 参数说明: 无。
    /// 返回：一次注册表锁内的一致计数。
    pub(super) fn snapshot(&self) -> PublisherSnapshot {
        let state = self.state.lock().expect("publish registry");
        let mut snapshot = state.snapshot.clone();
        snapshot.unjoined_tasks = state.tasks.len();
        snapshot
    }
}
