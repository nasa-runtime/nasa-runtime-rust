//! RedisJob RESP3 Push lane：按固定 Fanout 桶预订阅通知与回执频道，持久索引仍承担最终恢复。

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::client::{Conn, RedisClient};
use crate::error::{NasaRedisError, Result};
use crate::job::keyspace::JobKeyspace;
use crate::job::model::JobPubSubMode;

/// 一条已经通过 RESP3 lane 接收的 Fanout 信号。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FanoutSignal {
    /// 发给稳定节点的 shard 定位通知。
    Notification(Vec<u8>),
    /// 发给创建执行器的 receipt 唤醒；持久 receipt 索引仍是权威。
    Receipt(Vec<u8>),
}

/// 每个 Job source 独占的 RESP3 Pub/Sub lane；连接内不执行普通控制命令。
pub(crate) struct JobPubSubLane {
    _connection: Conn,
    inbox: JobPushInbox,
    receipt_channels: HashSet<String>,
}

/// RESP3 Push 的有界接收端；容量耗尽意味当前订阅代次已不再完整。
struct JobPushInbox {
    receiver: tokio::sync::mpsc::Receiver<redis::PushInfo>,
    overflowed: Arc<AtomicBool>,
    overflow_notify: Arc<tokio::sync::Notify>,
}

impl JobPushInbox {
    /// 业务作用：建立不阻塞 Redis driver 的有界 Push 通道，并把溢出显式转换为代次失效信号。
    ///
    /// 参数说明：`capacity` 是启动期按固定订阅桶数计算的队列上限。
    ///
    /// 返回：Redis 连接使用的非阻塞 sender 与唯一 receiver；队列满或 receiver 关闭时唤醒监督器重建订阅。
    fn new(capacity: usize) -> (Arc<dyn redis::aio::AsyncPushSender>, Self) {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity.max(1));
        let overflowed = Arc::new(AtomicBool::new(false));
        let overflow_notify = Arc::new(tokio::sync::Notify::new());
        let sender_overflowed = overflowed.clone();
        let sender_notify = overflow_notify.clone();
        let push_sender: Arc<dyn redis::aio::AsyncPushSender> = Arc::new(
            move |push: redis::PushInfo| -> std::result::Result<(), ()> {
                match sender.try_send(push) {
                    Ok(()) => Ok(()),
                    Err(_) => {
                        // Push 已不能完整排队，立即废弃整个订阅代次，避免业务将丢失通知误判为已接收。
                        sender_overflowed.store(true, Ordering::Release);
                        sender_notify.notify_one();
                        Err(())
                    }
                }
            },
        );
        (
            push_sender,
            Self {
                receiver,
                overflowed,
                overflow_notify,
            },
        )
    }

    /// 业务作用：等待下一条完整入队的 Push，并优先暴露已发生的容量溢出。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时返回 Push；队列溢出或 sender 关闭时返回执行状态未知，由上层重建订阅代次。
    async fn recv(&mut self) -> Result<redis::PushInfo> {
        loop {
            if self.overflowed.load(Ordering::Acquire) {
                return Err(crate::job::JobError::ExecutionUnknown(
                    "RedisJob RESP3 Push 有界通道容量耗尽".to_owned(),
                )
                .into());
            }
            tokio::select! {
                push = self.receiver.recv() => {
                    return push.ok_or_else(|| {
                        crate::job::JobError::ExecutionUnknown(
                            "RedisJob RESP3 Push 通道已关闭".to_owned(),
                        )
                        .into()
                    });
                }
                _ = self.overflow_notify.notified() => {}
            }
        }
    }
}

impl JobPubSubLane {
    /// 业务作用：在执行器声明 `fanoutReady` 前订阅全部固定桶频道，并等待每条订阅命令确认。
    ///
    /// 参数说明：`client` 为 source 客户端，`keyspace` 为固定桶布局，`mode` 为 namespace 冻结模式，
    /// `node_identity`/`executor_id` 分别定位通知与回执频道。
    ///
    /// 返回：全部频道确认后返回可拉取信号的 lane；任一 RESP3/ACL/命令失败时拒绝 source 启动。
    pub(crate) async fn start(
        client: Arc<RedisClient>,
        keyspace: &JobKeyspace,
        mode: JobPubSubMode,
        node_identity: &str,
        executor_id: &str,
    ) -> Result<Self> {
        let bucket_count = usize::try_from(keyspace.fanout_bucket_count()).unwrap_or(usize::MAX);
        let push_capacity = bucket_count.saturating_mul(4).saturating_add(32).max(64);
        let (push_sender, mut inbox) = JobPushInbox::new(push_capacity);
        let mut connection = client.job_pubsub_conn(push_sender).await?;
        let ack_timeout_ms = client.config().command.response_timeout_ms;
        let mut receipt_channels = HashSet::new();
        for bucket in 0..keyspace.fanout_bucket_count() {
            let notification = keyspace.fanout_notify_channel_at(bucket, node_identity);
            let receipt = keyspace.fanout_receipt_channel_at(bucket, executor_id);
            subscribe(
                &mut connection,
                &mut inbox,
                mode,
                &notification,
                ack_timeout_ms,
            )
            .await?;
            subscribe(&mut connection, &mut inbox, mode, &receipt, ack_timeout_ms).await?;
            receipt_channels.insert(receipt);
        }
        Ok(Self {
            _connection: connection,
            inbox,
            receipt_channels,
        })
    }

    /// 业务作用：等待下一条业务信号，忽略订阅确认 Push，并在连接断开时立即撤销 source 准入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：通知或回执携带原始信封；断线、通道关闭或畸形 Push 返回错误，不把非权威消息当成功。
    pub(crate) async fn next_signal(&mut self) -> Result<FanoutSignal> {
        loop {
            let push = self.inbox.recv().await?;
            match push.kind {
                redis::PushKind::Message | redis::PushKind::SMessage => {
                    if push.data.len() != 2 {
                        return Err(crate::job::JobError::Protocol(
                            "Fanout Push 字段数不符合频道与信封合同".to_owned(),
                        )
                        .into());
                    }
                    let channel = value_bytes(&push.data[0]);
                    let payload = value_bytes(&push.data[1]);
                    let channel = String::from_utf8(channel).map_err(|_| {
                        NasaRedisError::from(crate::job::JobError::Protocol(
                            "Fanout Push channel 不是 UTF-8".to_owned(),
                        ))
                    })?;
                    return Ok(if self.receipt_channels.contains(&channel) {
                        FanoutSignal::Receipt(payload)
                    } else {
                        FanoutSignal::Notification(payload)
                    });
                }
                redis::PushKind::Disconnection => {
                    return Err(crate::job::JobError::ExecutionUnknown(
                        "RedisJob RESP3 subscription generation 已断开".to_owned(),
                    )
                    .into());
                }
                redis::PushKind::Subscribe
                | redis::PushKind::SSubscribe
                | redis::PushKind::Unsubscribe
                | redis::PushKind::SUnsubscribe => {}
                _ => {}
            }
        }
    }
}

/// 业务作用：按 namespace 冻结模式发出单频道订阅并等待 Redis ACK，Cluster 不跨 slot 合并频道。
///
/// 参数说明：`connection` 为 RESP3 lane，`mode` 选择 SUBSCRIBE 或 SSUBSCRIBE，`channel` 为单个固定桶频道。
///
/// 返回：Redis 接受订阅时成功；命令、ACL 或协议不支持时返回错误。
async fn subscribe(
    connection: &mut Conn,
    inbox: &mut JobPushInbox,
    mode: JobPubSubMode,
    channel: &str,
    timeout_ms: u64,
) -> Result<()> {
    let command = match mode {
        JobPubSubMode::Sharded => "SSUBSCRIBE",
        JobPubSubMode::Broadcast => "SUBSCRIBE",
    };
    let mut cmd = redis::cmd(command);
    cmd.arg(channel);
    cmd.exec_async(connection)
        .await
        .map_err(NasaRedisError::Redis)?;
    let expected = match mode {
        JobPubSubMode::Sharded => redis::PushKind::SSubscribe,
        JobPubSubMode::Broadcast => redis::PushKind::Subscribe,
    };
    let wait_ack = async {
        loop {
            let push = inbox.recv().await?;
            if push.kind == redis::PushKind::Disconnection {
                return Err(crate::job::JobError::ExecutionUnknown(
                    "RedisJob RESP3 订阅 ACK 前连接断开".to_owned(),
                )
                .into());
            }
            if push.kind == expected
                && push
                    .data
                    .first()
                    .is_some_and(|value| value_bytes(value) == channel.as_bytes())
            {
                return Ok(());
            }
            // 启动门禁期出现的业务通知不承担权威；持久 receipt/ready 索引会在 Ready 后重新定位。
        }
    };
    let timeout = if timeout_ms == 0 { 30_000 } else { timeout_ms };
    tokio::time::timeout(std::time::Duration::from_millis(timeout), wait_ack)
        .await
        .map_err(|_| {
            NasaRedisError::from(crate::job::JobError::ExecutionUnknown(
                "RedisJob RESP3 订阅未在命令期限内取得 ACK".to_owned(),
            ))
        })?
}

/// 业务作用：把 RESP3 Push 字段转换为原始字节，不对业务信封执行有损文本转换。
///
/// 参数说明：`value` 为 Push 中的 channel 或 payload 字段。
///
/// 返回：字符串或 bulk 字节原样返回；其它类型返回空字节并由上层合同校验拒绝。
fn value_bytes(value: &redis::Value) -> Vec<u8> {
    match value {
        redis::Value::BulkString(bytes) => bytes.clone(),
        redis::Value::SimpleString(text) => text.as_bytes().to_vec(),
        _ => Vec::new(),
    }
}
