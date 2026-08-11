//! Saga 与 Redis Streams 的生产 transport 适配。
//!
//! 通用 stream 订阅器的 Auto/OnSuccess ACK 不足以承载 Saga：本适配器持有**显式、封闭的
//! 投递裁决**——只有本地数据库 COMMIT 明确成功或 Inbox 明确 Duplicate 才 XACK；commit
//! uncertain、rollback failure、数据库不可达与停机取消一律保留 PEL；确定性协议拒绝先以
//! Lua 原子完成"幂等 DLT 记录 + XACK"。基础设施瞬态错误**永不**因重投次数耗尽被标死
//! 越过——只有不可解析、身份伪造、越权与合同漂移这类确定性错误进入 DLT。
//!
//! 至少一次语义：XADD 成功只表示 Redis 已确认写入；回包丢失的发布重试可产生两条同
//! `event_id` entry，由 Inbox 幂等吸收并计入重复指标。Redis stream entry id 不承担业务
//! 幂等，`event_id` 才是唯一去重身份。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac as _};
use nadis::client::RedisClient;
use naoutbox_core::{OutboxEvent, OutboxPublishError, OutboxPublisher};
use nasaga_core::{DefinitionVersion, ServiceIdentity, StepName, WorkflowName};
use natelemetry::TraceContext;
use sha2::Sha256;

use crate::envelope::canonical_bytes;
use crate::transport_shared::{SagaCommandHandler, SagaResultHandler};
use crate::{SagaCommandEnvelope, SagaResultEnvelope};

/// entry 字段:事件类型。
const FIELD_EVENT: &str = "event";
/// entry 字段:全局唯一事件 id(Inbox 去重身份)。
const FIELD_EVENT_ID: &str = "event_id";
/// entry 字段:envelope JSON 载荷。
const FIELD_PAYLOAD: &str = "payload";
/// entry 字段:W3C traceparent(可选)。
const FIELD_TRACEPARENT: &str = "traceparent";
/// entry 字段:签名 key 的稳定 id。
const FIELD_KEY_ID: &str = "key_id";
/// entry 字段:canonical HMAC-SHA256 十六进制签名。
const FIELD_SIGNATURE: &str = "sig";

/// 业务作用：消息来源认证模式——共享 stream 无法从已落 entry 反推 producer 连接身份，
/// 必须二选一：每条消息携带稳定 key id 的 canonical 签名，或 producer 独占可写 stream
/// 并在 route 配置绑定 owner。
#[derive(Clone)]
pub enum SagaStreamAuth {
    /// 消息级签名：`sig = HMAC-SHA256(key, canonical(stream, event_id, event, payload))`。
    /// 验签 key 的保留期必须覆盖 stream 与 pending 的最大保留期。
    Hmac {
        /// 签名 key 的稳定 id，随消息进入 `key_id` 字段。
        key_id: String,
        /// HMAC 密钥字节。
        key: Vec<u8>,
    },
    /// producer 独占可写 stream：不写消息级签名，来源身份由部署层（ACL 独占写权限）
    /// 保证，route 配置仍显式绑定 owner。
    ExclusiveStream,
}

impl std::fmt::Debug for SagaStreamAuth {
    /// 业务作用：输出不含密钥字节的认证模式摘要。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hmac { key_id, .. } => formatter
                .debug_struct("Hmac")
                .field("key_id", key_id)
                .finish_non_exhaustive(),
            Self::ExclusiveStream => formatter.write_str("ExclusiveStream"),
        }
    }
}

/// 业务作用：构造覆盖 stream、事件身份、类型、完整 payload 与 trace 上下文的
/// canonical HMAC——字段以长度前缀拼接消除边界歧义。签名与验签共用本函数,保证
/// 两侧口径一致。
///
/// `traceparent` 会被消费边界读取并持久化为实例因果上下文,必须纳入签名:否则持有
/// stream 写权限但没有 HMAC key 的主体可以复制合法 entry、替换 trace 字段重放,伪造
/// 因果链与遥测归属。缺失与空值经显式存在标志区分,不产生拼接歧义。
///
/// 参数说明：
/// - `key`: HMAC 密钥。
/// - `stream`: 目标 stream 名（签名绑定通道，防跨 stream 重放）。
/// - `event_id`/`event_type`/`payload`: 消息身份与载荷。
/// - `traceparent`: 随消息传播的因果上下文;`None` 表示未携带。
///
/// 返回：已 update 完毕、可 finalize 或 verify_slice 的 MAC。
fn entry_mac(
    key: &[u8],
    stream: &str,
    event_id: &str,
    event_type: &str,
    payload: &[u8],
    traceparent: Option<&[u8]>,
) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(&canonical_bytes(&[
        stream.as_bytes(),
        event_id.as_bytes(),
        event_type.as_bytes(),
        payload,
        // 存在标志先行:区分"未携带 trace"与"携带空 trace",拼接不产生歧义。
        if traceparent.is_some() { b"1" } else { b"0" },
        traceparent.unwrap_or_default(),
    ]));
    mac
}

/// 业务作用：为发布端生成 entry 的十六进制 canonical 签名。
///
/// 参数说明：同 [`entry_mac`]。
///
/// 返回：十六进制签名文本。
fn sign_entry(
    key: &[u8],
    stream: &str,
    event_id: &str,
    event_type: &str,
    payload: &[u8],
    traceparent: Option<&[u8]>,
) -> String {
    hex::encode(
        entry_mac(key, stream, event_id, event_type, payload, traceparent)
            .finalize()
            .into_bytes(),
    )
}

/// 业务作用：校验 stream/key 名与 cluster hash tag 合同——启用 tag 时 DLT、marker 与
/// 源 stream 必须同槽，Lua 原子脚本才能在 Cluster 下合法执行。
///
/// 参数说明：
/// - `name`: 待校验的 stream/key 名。
/// - `key_tag`: Cluster 同槽 tag；`None` 表示单节点部署不作约束。
///
/// 返回：名称有界且满足同槽合同时返回真。
fn valid_stream_name(name: &str, key_tag: Option<&str>) -> bool {
    if name.is_empty() || name.len() > 190 || name.chars().any(char::is_control) {
        return false;
    }
    match key_tag {
        // 同槽判定必须用 Redis 实际采用的 hash tag,而不是"名字里出现过 {tag}":
        // Redis 只取第一个含非空内容的 `{...}` 参与 slot 计算,`a{x}{tag}` 的槽由 `{x}`
        // 决定——contains 校验会放行这类键,DLT Lua 到运行期才 CROSSSLOT 崩裂。
        Some(tag) => {
            !tag.is_empty()
                && tag.len() <= 64
                && !tag.contains(['{', '}'])
                && !tag.chars().any(char::is_control)
                && first_hash_tag(name) == Some(tag)
        }
        None => true,
    }
}

/// 业务作用：按 Redis Cluster 的 slot 规则解析键名实际采用的 hash tag。
///
/// 规则与服务端一致:取第一个 `{` 与其后第一个 `}` 之间的内容,内容非空才生效;
/// `{}` 或没有闭合的 `{` 都视为无 tag(整个键名参与 slot 计算)。
///
/// 参数说明：
/// - `name`: 键名。
///
/// 返回：实际生效的 hash tag;不存在时返回 `None`。
fn first_hash_tag(name: &str) -> Option<&str> {
    let open = name.find('{')?;
    let close = name[open + 1..].find('}')?;
    if close == 0 {
        // `{}`:Redis 视为无 tag。服务端遇到空 tag 后按整键计算 slot,不再看后续
        // `{...}`,这里保持同一行为。
        return None;
    }
    Some(&name[open + 1..open + 1 + close])
}

// ───────────────────────────── 发布端 ─────────────────────────────

/// 业务作用：把 Outbox 事件按事件类型写入受信 stream 的发布端。
///
/// 与 Outbox dispatcher 组合成"至少一次"链路：XADD 确认才算发布成功，失败/不确定由
/// dispatcher 保留重投；同 `event_id` 重投可能产生第二条 entry，由消费侧 Inbox 吸收。
pub struct SagaRedisStreamPublisher {
    client: Arc<RedisClient>,
    /// event_type → stream 的冻结映射；未映射的事件类型直接失败（fail-closed 保留在
    /// Outbox），不猜测路由。
    streams: BTreeMap<String, String>,
    auth: SagaStreamAuth,
    duplicates_hint: AtomicU64,
}

impl SagaRedisStreamPublisher {
    /// 业务作用：构造已冻结路由与认证模式的 stream 发布端。
    ///
    /// 参数说明：
    /// - `client`: 已建连的 Redis 客户端。
    /// - `streams`: event_type 到受信 stream 的冻结映射。
    /// - `auth`: 消息来源认证模式。
    /// - `key_tag`: Cluster 同槽 tag；启用时全部 stream 名必须含 `{tag}`。
    ///
    /// 返回：路由与名称满足合同时返回发布端；否则拒绝构造。
    pub fn new(
        client: Arc<RedisClient>,
        streams: BTreeMap<String, String>,
        auth: SagaStreamAuth,
        key_tag: Option<&str>,
    ) -> anyhow::Result<Self> {
        if streams.is_empty() {
            anyhow::bail!("saga stream publisher requires at least one event route");
        }
        for (event_type, stream) in &streams {
            if event_type.is_empty() || !valid_stream_name(stream, key_tag) {
                anyhow::bail!("saga stream route violates the naming or hash-tag contract");
            }
        }
        Ok(Self {
            client,
            streams,
            auth,
            duplicates_hint: AtomicU64::new(0),
        })
    }

    /// 业务作用：读取"发布重试可能产生的重复 entry"提示计数，供重复指标导出。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：进程内累计的疑似重复发布次数（XADD 失败后再次进入发布路径的事件数）。
    pub fn duplicate_hints(&self) -> u64 {
        self.duplicates_hint.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl OutboxPublisher for SagaRedisStreamPublisher {
    /// 业务作用：把一条事件以稳定 `event_id` 写入受信 stream。
    ///
    /// 参数说明：
    /// - `event`: 携带稳定事件身份的待发布事件。
    ///
    /// 返回：XADD 确认后返回成功；路由缺失或写入失败返回脱敏错误（dispatcher 保留重投）。
    async fn publish(&self, event: &OutboxEvent) -> Result<(), OutboxPublishError> {
        let Some(stream) = self.streams.get(&event.event_type) else {
            return Err(OutboxPublishError::new("saga stream route missing"));
        };
        let mut fields: Vec<(&str, Vec<u8>)> = vec![
            (FIELD_EVENT, event.event_type.clone().into_bytes()),
            (FIELD_EVENT_ID, event.event_id.clone().into_bytes()),
            (FIELD_PAYLOAD, event.payload.clone()),
        ];
        if let Some(traceparent) = &event.traceparent {
            fields.push((FIELD_TRACEPARENT, traceparent.clone().into_bytes()));
        }
        if let SagaStreamAuth::Hmac { key_id, key } = &self.auth {
            fields.push((FIELD_KEY_ID, key_id.clone().into_bytes()));
            fields.push((
                FIELD_SIGNATURE,
                sign_entry(
                    key,
                    stream,
                    &event.event_id,
                    &event.event_type,
                    &event.payload,
                    event.traceparent.as_deref().map(str::as_bytes),
                )
                .into_bytes(),
            ));
        }
        let mut command = redis::cmd("XADD");
        command.arg(stream).arg("*");
        for (name, value) in &fields {
            command.arg(*name).arg(value.as_slice());
        }
        let mut connection = self.client.conn();
        let appended: Result<String, _> = command.query_async(&mut connection).await;
        match appended {
            Ok(_) => Ok(()),
            Err(_) => {
                // 回包丢失/失败:XADD 可能已在服务端生效;dispatcher 重投会以同 event_id
                // 追加第二条 entry——至少一次合同下由 Inbox 吸收,此处计入重复提示指标。
                self.duplicates_hint.fetch_add(1, Ordering::Relaxed);
                PUBLISHER_DUPLICATE_HINTS.fetch_add(1, Ordering::Relaxed);
                // XADD 往返失败是基础设施瞬态:保留重投,不许进入死信预算。
                Err(OutboxPublishError::transient("saga stream publish failed"))
            }
        }
    }
}

// ───────────────────────────── 消费公共构件 ─────────────────────────────

/// 业务作用：一条已解码 stream entry 的最小视图。
struct StreamEntry {
    entry_id: String,
    fields: BTreeMap<String, Vec<u8>>,
}

/// 业务作用：单轮消费的低基数结果，供托管层指标与告警。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamPollReport {
    /// 本轮 COMMIT/Duplicate 后已 XACK 的消息数。
    pub acked: u64,
    /// 本轮确定性拒绝并完成"DLT+XACK"的消息数。
    pub dead_lettered: u64,
    /// 本轮保留在 PEL 的消息数（瞬态失败、handler 超时或停机让路）。
    pub retained: u64,
    /// 本轮经 XAUTOCLAIM 重领的消息数。
    pub reclaimed: u64,
    /// XAUTOCLAIM 报告的已删除 pending id 数——entry 在确认前被外部删除,必须告警。
    pub deleted_pending: u64,
    /// 签名/来源拒绝数（计入 dead_lettered 的子集）。
    pub auth_rejected: u64,
    /// 本轮完成裁决的消息数(handler 延迟均值的分母)。
    pub handled: u64,
    /// 本轮裁决累计耗时(微秒;含验签与解码,主体为业务 handler)。
    pub handler_micros_sum: u64,
}

/// 进程级"发布重试可能产生的重复 entry"累计——受管观测面按低基数导出;
/// 每个 publisher 实例的精确值仍经 [`SagaRedisStreamPublisher::duplicate_hints`] 读取。
static PUBLISHER_DUPLICATE_HINTS: AtomicU64 = AtomicU64::new(0);

/// 业务作用：读取当前进程全部 stream 发布端累计的疑似重复发布次数。
///
/// 参数说明: 无。
///
/// 返回：进程内累计值。
pub fn publisher_duplicate_hints_total() -> u64 {
    PUBLISHER_DUPLICATE_HINTS.load(Ordering::Relaxed)
}

/// 业务作用：读取单个 (stream, group) 的积压概要——pending 数与最老 pending entry 的
/// 年龄,供受管观测面按冻结标签导出。
///
/// 年龄按最老 pending entry id 的毫秒段与调用方时钟差计算;PEL 为空时无年龄。回包
/// 形态异常按错误上抛,调用方据此把观测轮记为失败而不是导出假数据。
///
/// 参数说明：
/// - `client`: Redis 客户端。
/// - `stream`: 源 stream。
/// - `group`: consumer group。
/// - `now_ms`: 当前 epoch 毫秒。
///
/// 返回：`(pending 数, 最老 pending 年龄毫秒)`;Redis 往返失败或回包异常返回错误。
pub async fn stream_group_backlog(
    client: &RedisClient,
    stream: &str,
    group: &str,
    now_ms: i64,
) -> anyhow::Result<(u64, Option<u64>)> {
    let mut connection = client.conn();
    let pending: redis::Value = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(&mut connection)
        .await
        .map_err(|error| anyhow::anyhow!("XPENDING backlog probe failed: {error}"))?;
    let redis::Value::Array(parts) = pending else {
        anyhow::bail!("XPENDING backlog probe returned an unexpected reply shape");
    };
    if parts.is_empty() {
        anyhow::bail!("XPENDING backlog probe returned an empty reply");
    }
    let count = match &parts[0] {
        redis::Value::Int(count) => (*count).max(0) as u64,
        _ => anyhow::bail!("XPENDING backlog probe returned a non-numeric count"),
    };
    if count == 0 {
        return Ok((0, None));
    }
    let oldest = match parts.get(1) {
        Some(redis::Value::BulkString(bytes)) => {
            parse_entry_id(&String::from_utf8_lossy(bytes)).map(|(ms, _)| ms)
        }
        _ => None,
    };
    let age = oldest.map(|ms| (now_ms.max(0) as u64).saturating_sub(ms));
    Ok((count, age))
}

/// 幂等 DLT + XACK 的原子脚本。
///
/// KEYS[1]=DLT stream、KEYS[2]=去重 marker、KEYS[3]=源 stream;
/// ARGV[1]=group、ARGV[2]=源 entry id、ARGV[3]=event_id、ARGV[4]=reason、ARGV[5]=payload、
/// ARGV[6]=marker TTL 秒。Cluster 下三个 KEY 必须同槽（构造期校验 hash tag）。
///
/// 副作用顺序是安全边界本身:**死信必须先于 marker、更先于 XACK 持久化**。Redis Lua
/// 只保证执行期间不被并发插入,不回滚已完成的写入——若 marker 先写而 `XADD` 运行期
/// 失败,重放会因 marker 在场跳过死信、直接确认源消息,证据从此丢失。因此脚本按
/// EXISTS→XADD→SET(marker)→XACK 排列:`XADD` 失败即中止,marker 与 XACK 均未发生,
/// 消息留在 PEL 安全重试;marker 在死信落地之后才代表"已隔离",重放据其跳过 `XADD`
/// 只补 XACK。最坏部分失败(XADD 成功后 SET 中止)重放会追加第二条同 event_id 的
/// 死信——DLT 是至少一次通道,由 event_id 去重消费;这与"无死信证据即确认"不可同日而语。
const DLT_ACK_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[2]) == 0 then
    redis.call('XADD', KEYS[1], '*', 'event_id', ARGV[3], 'reason', ARGV[4], 'payload', ARGV[5])
    redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[6])
end
redis.call('XACK', KEYS[3], ARGV[1], ARGV[2])
return 1
"#;

/// 业务作用：一次投递的封闭裁决——与 Kafka connector 共享同一行为合同词汇。
enum StreamVerdict {
    /// 本地事务 COMMIT（或 Inbox Duplicate）:XACK 前移。
    Ack,
    /// 确定性协议拒绝:先 Lua 原子 DLT 再 XACK,携带稳定原因。
    DeadLetter(&'static str),
    /// 瞬态/延后:保留 PEL,不消耗任何"死信预算"（瞬态永不自动进 DLT）。
    Retain,
}

/// 业务作用：消费侧公共配置——stream/group/DLT/marker、批与阻塞预算、重领与初始位置。
#[derive(Debug, Clone)]
pub struct SagaStreamConsumerConfig {
    /// 源 stream。
    pub stream: String,
    /// 确定性拒绝的 DLT stream（Cluster 下与源 stream、marker 同槽）。
    pub dlt_stream: String,
    /// DLT 去重 marker 前缀（`{prefix}:<event_id>`）。
    pub marker_prefix: String,
    /// 稳定 consumer group;已存在的 group 不因重启被重置。
    pub group: String,
    /// 逐副本稳定 consumer 名。
    pub consumer: String,
    /// 单轮 XREADGROUP COUNT 上限。
    pub batch: u32,
    /// 单轮 XREADGROUP BLOCK 毫秒(1..=60_000;0 在 Redis 语义里是无限等待,被拒绝)。
    pub block_ms: u64,
    /// 单条消息 handler 超时毫秒(1..=60_000);超时保留 PEL。与 batch 的乘积受
    /// 单轮一小时预算约束。
    pub handler_timeout_ms: u64,
    /// XAUTOCLAIM 的最小空闲毫秒（重领他人失联消息的下限）。
    pub min_idle_ms: u64,
    /// 新建 group 的初始位置:真=从 `0-0` 回放历史,假=从当前末尾开始。
    /// 该值只影响 group 首次创建;既有 group 绝不重置。
    pub replay_from_beginning: bool,
    /// DLT 去重 marker 的保留秒数;必须覆盖 DLT 排查与重放窗口。
    pub marker_ttl_seconds: u64,
    /// 消息来源认证模式（与发布端对应）。
    pub auth: SagaStreamAuth,
    /// stream 唯一可信 producer 逻辑身份（route owner 绑定）。
    pub producer: ServiceIdentity,
    /// Cluster 同槽 tag;启用时 stream/DLT/marker 名必须含 `{tag}`。
    pub key_tag: Option<String>,
}

impl SagaStreamConsumerConfig {
    /// 业务作用：校验消费配置满足命名、同槽与预算合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：合同满足返回 `Ok`；否则返回拒绝启动的错误。
    pub fn validate(&self) -> anyhow::Result<()> {
        let tag = self.key_tag.as_deref();
        if !valid_stream_name(&self.stream, tag)
            || !valid_stream_name(&self.dlt_stream, tag)
            || !valid_stream_name(&self.marker_prefix, tag)
        {
            anyhow::bail!("saga stream consumer violates the naming or hash-tag contract");
        }
        if self.group.is_empty()
            || self.group.len() > 128
            || self.consumer.is_empty()
            || self.consumer.len() > 128
            || self.batch == 0
            || self.batch > 10_000
            // BLOCK 0 是 Redis 的"无限等待"语义,会让单轮永不返回、停机无法收口;
            // 阻塞预算必须是正有界值。
            || self.block_ms == 0
            || self.block_ms > 60_000
            || self.handler_timeout_ms == 0
            || self.handler_timeout_ms > 60_000
            || self.min_idle_ms == 0
            || self.marker_ttl_seconds == 0
        {
            anyhow::bail!("saga stream consumer requires bounded budgets and identities");
        }
        // 单轮最坏时长 ≈ batch × handler_timeout + block:整轮预算必须有界,否则
        // "有限 batch/block/handler timeout"合同在组合上失效,停机预算也无从谈起。
        if u64::from(self.batch) * self.handler_timeout_ms > 3_600_000 {
            anyhow::bail!(
                "saga stream consumer worst-case round (batch x handler_timeout_ms) must stay within one hour"
            );
        }
        Ok(())
    }
}

/// 业务作用：解析 XREADGROUP/XAUTOCLAIM 应答中的 entry 列表。
///
/// 参数说明：
/// - `value`: 单个 stream 的 entry 数组 redis 值。
///
/// 返回：entry 视图集合；结构不符按空处理（上层按无消息退避）。
fn parse_entries(value: &redis::Value) -> Vec<StreamEntry> {
    let mut entries = Vec::new();
    let redis::Value::Array(items) = value else {
        return entries;
    };
    for item in items {
        let redis::Value::Array(pair) = item else {
            continue;
        };
        if pair.len() != 2 {
            continue;
        }
        let entry_id = match &pair[0] {
            redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            redis::Value::SimpleString(text) => text.clone(),
            _ => continue,
        };
        let mut fields = BTreeMap::new();
        if let redis::Value::Array(kv) = &pair[1] {
            let mut iterator = kv.iter();
            while let (Some(name), Some(value)) = (iterator.next(), iterator.next()) {
                let name = match name {
                    redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    redis::Value::SimpleString(text) => text.clone(),
                    _ => continue,
                };
                let value = match value {
                    redis::Value::BulkString(bytes) => bytes.clone(),
                    redis::Value::SimpleString(text) => text.clone().into_bytes(),
                    _ => continue,
                };
                fields.insert(name, value);
            }
        }
        entries.push(StreamEntry { entry_id, fields });
    }
    entries
}

/// 业务作用：消费公共骨架——group 幂等创建、重领与新消息读取、按裁决 ACK/DLT/保留。
///
/// 裁决闭包拿到 owned 的消息视图后返回封闭裁决;骨架不理解业务语义,只执行裁决对应的
/// Redis 副作用,保证"XACK 只发生在 COMMIT/Duplicate 或 DLT 落地之后"。
struct StreamConsumerCore {
    config: Arc<SagaStreamConsumerConfig>,
    /// XAUTOCLAIM 扫描游标:跨轮持有服务端返回的 next-start-id。固定从 0-0 重扫会在
    /// 首页持续 Retain 时把后续 PEL 永久饿死——每轮只重复领取同一页,失联消息永无
    /// 接管机会。游标回到 0-0 表示完成一次全 PEL 扫描,下一轮重新开始。
    reclaim_cursor: std::sync::Mutex<String>,
}

impl StreamConsumerCore {
    /// 业务作用：幂等确保 consumer group 存在;既有 group 绝不重置初始位置。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：group 就绪返回 `Ok`；创建失败（非 BUSYGROUP）返回错误。
    async fn ensure_group(&self, client: &RedisClient) -> anyhow::Result<()> {
        let start = if self.config.replay_from_beginning {
            "0-0"
        } else {
            "$"
        };
        let mut connection = client.conn();
        let created: Result<redis::Value, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .arg(start)
            .arg("MKSTREAM")
            .query_async(&mut connection)
            .await;
        match created {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
            Err(error) => Err(anyhow::anyhow!("saga stream group create failed: {error}")),
        }
    }

    /// 业务作用：执行一轮"先重领、后读新"的消费,按裁决完成 ACK/DLT/保留。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    /// - `adjudicate`: 单条消息的封闭裁决（含 handler 调用与超时约束）。
    ///
    /// 返回：本轮低基数报告；Redis 往返失败返回错误（消息全部留在 PEL/stream）。
    async fn poll_once<F, Fut>(
        &self,
        client: &RedisClient,
        mut adjudicate: F,
    ) -> anyhow::Result<StreamPollReport>
    where
        F: FnMut(SagaStreamEntry) -> Fut,
        Fut: std::future::Future<Output = StreamVerdict>,
    {
        let mut report = StreamPollReport::default();
        let mut connection = client.conn();

        // 先重领失联消息:XAUTOCLAIM 按 min-idle 分页接管旧 owner 的 PEL;返回的
        // "已删除 pending id"单独计数——entry 在确认前被外部删除是保留合同被破坏的
        // 信号,必须暴露而不是伪装成功。往返失败或回包形态异常必须上抛:吞掉它会让
        // 调用方把"Redis 不可用"当成"无消息",readiness 在真实断连时保持假绿。
        // 起点取跨轮游标而不是固定 0-0:分页扫描才能让全 PEL 都有被接管的机会。
        let reclaim_start = self
            .reclaim_cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let reclaim: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .arg(&self.config.consumer)
            .arg(self.config.min_idle_ms)
            .arg(&reclaim_start)
            .arg("COUNT")
            .arg(self.config.batch)
            .query_async(&mut connection)
            .await
            .map_err(|error| anyhow::anyhow!("saga stream XAUTOCLAIM failed: {error}"))?;
        let mut reclaimed_entries: Vec<StreamEntry> = Vec::new();
        match &reclaim {
            redis::Value::Array(parts) if parts.len() >= 2 => {
                // 回包首项是服务端给出的 next-start-id:持久到下一轮,回到 0-0 才算
                // 完成一次全 PEL 扫描。读不出游标按协议异常上抛,不静默重置——重置
                // 等价于回到"永远扫第一页"的饥饿路径。
                let next_cursor = match &parts[0] {
                    redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    redis::Value::SimpleString(text) => text.clone(),
                    _ => {
                        anyhow::bail!("saga stream XAUTOCLAIM returned an unreadable next cursor")
                    }
                };
                *self
                    .reclaim_cursor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = next_cursor;
                reclaimed_entries.extend(parse_entries(&parts[1]));
                if parts.len() >= 3 {
                    if let redis::Value::Array(deleted) = &parts[2] {
                        report.deleted_pending += deleted.len() as u64;
                    }
                }
            }
            _ => anyhow::bail!("saga stream XAUTOCLAIM returned an unexpected reply shape"),
        }

        // 已重领的 entry 先裁决,再进入阻塞读:它们已经等过至少一个 min-idle,不能再
        // 排在"等新消息"的 BLOCK 之后;这也保证没有新消息时旧 PEL 仍持续收敛。
        for entry in reclaimed_entries {
            report.reclaimed += 1;
            self.settle_entry(&mut connection, entry, &mut adjudicate, &mut report)
                .await?;
        }

        // 再读新消息(">"):有限 COUNT/BLOCK,不无界阻塞。超时无消息回 Nil 属正常;
        // 往返失败与异常形态同样上抛。
        let fresh: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.config.group)
            .arg(&self.config.consumer)
            .arg("COUNT")
            .arg(self.config.batch)
            .arg("BLOCK")
            .arg(self.config.block_ms)
            .arg("STREAMS")
            .arg(&self.config.stream)
            .arg(">")
            .query_async(&mut connection)
            .await
            .map_err(|error| anyhow::anyhow!("saga stream XREADGROUP failed: {error}"))?;
        let mut fresh_entries: Vec<StreamEntry> = Vec::new();
        match &fresh {
            redis::Value::Nil => {}
            redis::Value::Array(streams) => {
                for stream in streams {
                    if let redis::Value::Array(pair) = stream {
                        if pair.len() == 2 {
                            fresh_entries.extend(parse_entries(&pair[1]));
                        }
                    }
                }
            }
            _ => anyhow::bail!("saga stream XREADGROUP returned an unexpected reply shape"),
        }
        for entry in fresh_entries {
            self.settle_entry(&mut connection, entry, &mut adjudicate, &mut report)
                .await?;
        }
        Ok(report)
    }

    /// 业务作用：对单条消息执行裁决并落实对应的 Redis 副作用。
    ///
    /// ACK 与"DLT+XACK"脚本的往返失败一律上抛终止本轮:消息与其后未处理的 entry 全部
    /// 留在 PEL/stream(至少一次合同下安全),而调用方据此把 readiness 摘流——ACK 通道
    /// 持续失败时绝不能对外维持健康假象。DLT 脚本重放由 marker 幂等吸收,不会写第二条
    /// 死信。
    ///
    /// 参数说明：
    /// - `connection`: 本轮复用的 Redis 连接。
    /// - `entry`: 待裁决的 entry。
    /// - `adjudicate`: 单条消息的封闭裁决。
    /// - `report`: 本轮累计报告。
    ///
    /// 返回：裁决副作用落实返回 `Ok`;Redis 往返失败返回错误。
    async fn settle_entry<F, Fut>(
        &self,
        connection: &mut (impl redis::aio::ConnectionLike + Send),
        entry: StreamEntry,
        adjudicate: &mut F,
        report: &mut StreamPollReport,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SagaStreamEntry) -> Fut,
        Fut: std::future::Future<Output = StreamVerdict>,
    {
        let view = SagaStreamEntry {
            config: Arc::clone(&self.config),
            fields: entry.fields.clone(),
        };
        // 裁决计时覆盖验签、解码与业务 handler(含超时);它是"handler 延迟"观测的
        // 数据源,只在进程内累计,不携带任何业务身份。
        let started = std::time::Instant::now();
        let verdict = adjudicate(view).await;
        report.handled += 1;
        report.handler_micros_sum += started.elapsed().as_micros() as u64;
        match verdict {
            StreamVerdict::Ack => {
                let _acked: u64 = redis::cmd("XACK")
                    .arg(&self.config.stream)
                    .arg(&self.config.group)
                    .arg(&entry.entry_id)
                    .query_async(&mut *connection)
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("saga stream XACK failed after commit: {error}")
                    })?;
                report.acked += 1;
            }
            StreamVerdict::DeadLetter(reason) => {
                let event_id = entry
                    .fields
                    .get(FIELD_EVENT_ID)
                    .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                    .unwrap_or_else(|| format!("entry:{}", entry.entry_id));
                let marker = format!("{}:{}", self.config.marker_prefix, event_id);
                let payload = entry.fields.get(FIELD_PAYLOAD).cloned().unwrap_or_default();
                let _script: i64 = redis::cmd("EVAL")
                    .arg(DLT_ACK_SCRIPT)
                    .arg(3)
                    .arg(&self.config.dlt_stream)
                    .arg(&marker)
                    .arg(&self.config.stream)
                    .arg(&self.config.group)
                    .arg(&entry.entry_id)
                    .arg(&event_id)
                    .arg(reason)
                    .arg(payload.as_slice())
                    .arg(self.config.marker_ttl_seconds)
                    .query_async(&mut *connection)
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("saga stream DLT+XACK script failed: {error}")
                    })?;
                report.dead_lettered += 1;
                if reason.contains("unauthorized") || reason.contains("signature") {
                    report.auth_rejected += 1;
                }
            }
            StreamVerdict::Retain => {
                report.retained += 1;
            }
        }
        Ok(())
    }
}

/// 业务作用：裁决函数可见的单条消息视图（owned）——字段访问与来源验证收敛在此。
pub struct SagaStreamEntry {
    config: Arc<SagaStreamConsumerConfig>,
    fields: BTreeMap<String, Vec<u8>>,
}

impl SagaStreamEntry {
    /// 业务作用：验证消息来源——Hmac 模式重算 canonical 签名并以常量时间比对,独占
    /// stream 模式由部署 ACL 保证来源、此处只确认必要字段在场。
    ///
    /// 比对走 RustCrypto `Mac::verify_slice`（常量时间），与 HTTP transport 的
    /// [`SagaHttpMessageAuthenticator`](crate::SagaHttpMessageAuthenticator) 同一口径,
    /// 不做十六进制字符串的短路比较,消除签名比对的 timing side channel。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：来源可信返回 `Ok`；缺字段、key id 不符或签名不匹配返回稳定原因。
    fn verify_origin(&self) -> Result<(), &'static str> {
        match &self.config.auth {
            SagaStreamAuth::Hmac { key_id, key } => {
                let presented_key = self
                    .fields
                    .get(FIELD_KEY_ID)
                    .map(|bytes| String::from_utf8_lossy(bytes).to_string());
                if presented_key.as_deref() != Some(key_id.as_str()) {
                    return Err("saga_stream_signature_key_unknown");
                }
                let Some(signature) = self.fields.get(FIELD_SIGNATURE) else {
                    return Err("saga_stream_signature_missing");
                };
                // 签名字段是十六进制文本:先解码回原始字节,再常量时间比对。解码失败
                // 即视为签名非法,不进入比对。
                let Some(decoded) = self
                    .field_text(FIELD_SIGNATURE)
                    .and_then(|hex_text| hex::decode(hex_text).ok())
                else {
                    let _ = signature;
                    return Err("saga_stream_signature_invalid");
                };
                let event_id = self.field_text(FIELD_EVENT_ID).unwrap_or_default();
                let event_type = self.field_text(FIELD_EVENT).unwrap_or_default();
                let payload = self.fields.get(FIELD_PAYLOAD).cloned().unwrap_or_default();
                // trace 字段按"实际在场的字节"参与验签:被篡改、增删的 trace 都会
                // 使签名失配,伪造因果链的 entry 走确定性拒绝进 DLT。
                let traceparent = self.fields.get(FIELD_TRACEPARENT).map(Vec::as_slice);
                entry_mac(
                    key,
                    &self.config.stream,
                    &event_id,
                    &event_type,
                    &payload,
                    traceparent,
                )
                .verify_slice(&decoded)
                .map_err(|_| "saga_stream_signature_invalid")?;
                Ok(())
            }
            SagaStreamAuth::ExclusiveStream => Ok(()),
        }
    }

    /// 业务作用：读取文本字段。
    ///
    /// 参数说明：
    /// - `name`: 字段名。
    ///
    /// 返回：字段存在返回其 UTF-8 文本；缺失返回 `None`。
    fn field_text(&self, name: &str) -> Option<String> {
        self.fields
            .get(name)
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
    }

    /// 业务作用：解析收据中的 W3C trace 上下文，显式交给 traced handler。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：header 合法返回上下文；缺失或非法返回 `None`（trace 不阻塞投递）。
    fn trace_context(&self) -> Option<TraceContext> {
        self.field_text(FIELD_TRACEPARENT)
            .and_then(|raw| TraceContext::parse_traceparent(&raw))
    }
}

// ───────────────────────────── result 消费者 ─────────────────────────────

/// 业务作用：Orchestrator 侧的 result stream 消费者——显式 ACK 裁决 + 收据 trace 续接。
pub struct SagaRedisStreamResultConsumer<H> {
    handler: Arc<H>,
    core: StreamConsumerCore,
}

impl<H: SagaResultHandler> SagaRedisStreamResultConsumer<H> {
    /// 业务作用：构造 result stream 消费者。
    ///
    /// 参数说明：
    /// - `handler`: 结果处理实现（通常为 [`crate::Orchestrator`]）。
    /// - `config`: 已通过 [`SagaStreamConsumerConfig::validate`] 的消费配置。
    ///
    /// 返回：配置合同满足时返回消费者。
    pub fn new(handler: Arc<H>, config: SagaStreamConsumerConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            handler,
            core: StreamConsumerCore {
                config: Arc::new(config),
                reclaim_cursor: std::sync::Mutex::new("0-0".to_string()),
            },
        })
    }

    /// 业务作用：幂等确保 consumer group 存在（Ready 前调用）。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：group 就绪返回 `Ok`。
    pub async fn ensure_group(&self, client: &RedisClient) -> anyhow::Result<()> {
        self.core.ensure_group(client).await
    }

    /// 业务作用：读取已冻结的消费配置——受管生命周期用它做 Ready 探测与身份唯一性校验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置引用。
    pub fn config(&self) -> &SagaStreamConsumerConfig {
        &self.core.config
    }

    /// 业务作用：执行一轮消费——重领 + 新消息,按封闭裁决 XACK/DLT/保留。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：本轮报告；Redis 往返失败返回错误（消息原位保留）。
    pub async fn poll_once(
        &self,
        client: &RedisClient,
        now_ms: i64,
    ) -> anyhow::Result<StreamPollReport> {
        let handler = Arc::clone(&self.handler);
        let producer = self.core.config.producer.clone();
        let timeout = Duration::from_millis(self.core.config.handler_timeout_ms);
        self.core
            .poll_once(client, move |view| {
                let handler = Arc::clone(&handler);
                let producer = producer.clone();
                async move {
                    if let Err(reason) = view.verify_origin() {
                        return StreamVerdict::DeadLetter(reason);
                    }
                    let Some(payload) = view.fields.get(FIELD_PAYLOAD) else {
                        return StreamVerdict::DeadLetter("saga_stream_payload_missing");
                    };
                    let Ok(envelope) = serde_json::from_slice::<SagaResultEnvelope>(payload) else {
                        // 不可解析是确定性拒绝:重投永远失败,必须隔离而不是无限重试。
                        return StreamVerdict::DeadLetter("saga_result_payload_undecodable");
                    };
                    let receipt_trace = view.trace_context();
                    // handler future 上堆:debug 态编排全链 future 以 MB 计,叠在消费
                    // 骨架的栈帧上会击穿默认 worker 栈。
                    let outcome = tokio::time::timeout(
                        timeout,
                        Box::pin(handler.handle_authenticated_result_traced(
                            &envelope,
                            &producer,
                            receipt_trace.as_ref(),
                            now_ms,
                        )),
                    )
                    .await;
                    match outcome {
                        Ok(Ok(_)) => StreamVerdict::Ack,
                        Ok(Err(error)) => classify_result_error(&error),
                        // handler 超时:COMMIT 结果未知,保留 PEL 持续重投直到收敛。
                        Err(_) => StreamVerdict::Retain,
                    }
                }
            })
            .await
    }
}

/// 业务作用：把 result 处理错误映射为封闭裁决——确定性协议错误进 DLT,其余保留。
///
/// 参数说明：
/// - `error`: handler 返回的完整错误链。
///
/// 返回：与 Kafka connector 同一分类边界的裁决。
fn classify_result_error(error: &anyhow::Error) -> StreamVerdict {
    if matches!(
        crate::classify_result_delivery_error(error),
        crate::ResultDeliveryDisposition::DeadLetter
    ) {
        let reason = error
            .chain()
            .find_map(|cause| {
                cause
                    .downcast_ref::<crate::SagaResultProcessingError>()
                    .copied()
            })
            .and_then(crate::SagaResultProcessingError::dead_letter_reason)
            .unwrap_or("saga_result_contract_invalid");
        return StreamVerdict::DeadLetter(reason);
    }
    // PAUSED 等类型化延后与全部瞬态错误一致:保留 PEL,绝不消耗死信预算。
    StreamVerdict::Retain
}

// ───────────────────────────── command 消费者 ─────────────────────────────

/// 业务作用：参与方侧的 command stream 消费者——精确 route 门禁 + 收据 trace 传递。
pub struct SagaRedisStreamCommandConsumer<H> {
    handler: Arc<H>,
    core: StreamConsumerCore,
    /// 本 stream 唯一承载的 workflow/version/digest/step 合同（与 Kafka route 同语义）。
    workflow: WorkflowName,
    version: DefinitionVersion,
    digest: String,
    step: StepName,
}

impl<H: SagaCommandHandler> SagaRedisStreamCommandConsumer<H> {
    /// 业务作用：构造 command stream 消费者并冻结 route 合同。
    ///
    /// 参数说明：
    /// - `handler`: 命令处理实现（宏生成 Service 的 [`crate::ParticipantCommandHandler`]）。
    /// - `config`: 消费配置（producer 字段=该 stream 唯一可信 Orchestrator）。
    /// - `workflow`/`version`/`digest`/`step`: 本 stream 唯一目标步骤合同。
    ///
    /// 返回：配置与摘要合同满足时返回消费者。
    pub fn new(
        handler: Arc<H>,
        config: SagaStreamConsumerConfig,
        workflow: WorkflowName,
        version: DefinitionVersion,
        digest: impl Into<String>,
        step: StepName,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let digest = digest.into();
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            anyhow::bail!("saga stream command route requires a canonical definition digest");
        }
        Ok(Self {
            handler,
            core: StreamConsumerCore {
                config: Arc::new(config),
                reclaim_cursor: std::sync::Mutex::new("0-0".to_string()),
            },
            workflow,
            version,
            digest,
            step,
        })
    }

    /// 业务作用：幂等确保 consumer group 存在（Ready 前调用）。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：group 就绪返回 `Ok`。
    pub async fn ensure_group(&self, client: &RedisClient) -> anyhow::Result<()> {
        self.core.ensure_group(client).await
    }

    /// 业务作用：读取已冻结的消费配置——受管生命周期用它做 Ready 探测与身份唯一性校验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置引用。
    pub fn config(&self) -> &SagaStreamConsumerConfig {
        &self.core.config
    }

    /// 业务作用：执行一轮 command 消费——重领 + 新消息,按封闭裁决 XACK/DLT/保留。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：本轮报告；Redis 往返失败返回错误（消息原位保留）。
    pub async fn poll_once(&self, client: &RedisClient) -> anyhow::Result<StreamPollReport> {
        let handler = Arc::clone(&self.handler);
        let producer = self.core.config.producer.clone();
        let timeout = Duration::from_millis(self.core.config.handler_timeout_ms);
        let workflow = self.workflow.clone();
        let version = self.version;
        let digest = self.digest.clone();
        let step = self.step.clone();
        self.core
            .poll_once(client, move |view| {
                let handler = Arc::clone(&handler);
                let producer = producer.clone();
                let workflow = workflow.clone();
                let digest = digest.clone();
                let step = step.clone();
                async move {
                    if let Err(reason) = view.verify_origin() {
                        return StreamVerdict::DeadLetter(reason);
                    }
                    let Some(payload) = view.fields.get(FIELD_PAYLOAD) else {
                        return StreamVerdict::DeadLetter("saga_stream_payload_missing");
                    };
                    let Ok(envelope) = serde_json::from_slice::<SagaCommandEnvelope>(payload)
                    else {
                        return StreamVerdict::DeadLetter("saga_command_payload_undecodable");
                    };
                    let identity_ok = envelope.verified().is_ok();
                    if !identity_ok {
                        return StreamVerdict::DeadLetter("saga_command_identity_invalid");
                    }
                    // route 精确匹配:可信 producer 不代表可以把任意 workflow/步骤塞给本
                    // 参与方;错配在 Inbox 前隔离。
                    if envelope.workflow != workflow.as_str()
                        || envelope.definition_version != version.get()
                        || envelope.definition_digest != digest
                        || envelope.step != step.as_str()
                    {
                        return StreamVerdict::DeadLetter("saga_command_route_unauthorized");
                    }
                    let receipt_trace = view.trace_context();
                    // 同 result 侧:参与方 handler future 上堆,避免默认 worker 栈溢出。
                    let outcome = tokio::time::timeout(
                        timeout,
                        Box::pin(handler.handle_authenticated_command_traced(
                            &envelope,
                            &producer,
                            receipt_trace.as_ref(),
                        )),
                    )
                    .await;
                    match outcome {
                        Ok(Ok(_)) => StreamVerdict::Ack,
                        Ok(Err(error)) => classify_command_error(&error),
                        Err(_) => StreamVerdict::Retain,
                    }
                }
            })
            .await
    }
}

/// 业务作用：把 command 处理错误映射为封闭裁决——确定性协议错误进 DLT,其余保留。
///
/// 参数说明：
/// - `error`: handler 返回的完整错误链。
///
/// 返回：与 Kafka connector 同一分类边界的裁决。
fn classify_command_error(error: &anyhow::Error) -> StreamVerdict {
    if let Some(reason) = crate::command_dead_letter_reason(error) {
        return StreamVerdict::DeadLetter(reason);
    }
    StreamVerdict::Retain
}

// ───────────────────────────── 保留清剪与受管生命周期 ─────────────────────────────

/// 业务作用：受管生命周期使用的对象安全消费抽象——把 result/command 两种带泛型
/// handler 的消费者收敛成同一个受监督轮询单元。
///
/// Application 组件按本抽象持有一组消费者:Ready 前统一 `ensure_group` 探测,运行期
/// 逐个 `poll_once`,停机时停止调用(在途轮次内已接管消息由 poll_once 自身排空,
/// 未确认消息留在 PEL 交由重启后 XAUTOCLAIM 重领)。
#[async_trait::async_trait]
pub trait SagaStreamPoller: Send + Sync {
    /// 业务作用：读取冻结消费配置,供探测与身份唯一性校验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置引用。
    fn config(&self) -> &SagaStreamConsumerConfig;

    /// 业务作用：幂等确保 consumer group 存在(Ready 探测的一部分)。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：group 就绪返回 `Ok`。
    async fn ensure_group(&self, client: &RedisClient) -> anyhow::Result<()>;

    /// 业务作用：执行一轮消费。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    /// - `now_ms`: 当前 epoch 毫秒(command 消费不依赖该值,统一签名便于对象安全)。
    ///
    /// 返回：本轮报告;Redis 往返失败返回错误(消息原位保留)。
    async fn poll_once(
        &self,
        client: &RedisClient,
        now_ms: i64,
    ) -> anyhow::Result<StreamPollReport>;
}

#[async_trait::async_trait]
impl<H: SagaResultHandler> SagaStreamPoller for SagaRedisStreamResultConsumer<H> {
    /// 业务作用：暴露 result 消费配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置引用。
    fn config(&self) -> &SagaStreamConsumerConfig {
        SagaRedisStreamResultConsumer::config(self)
    }

    /// 业务作用：委托 result 消费者的 group 幂等创建。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：group 就绪返回 `Ok`。
    async fn ensure_group(&self, client: &RedisClient) -> anyhow::Result<()> {
        SagaRedisStreamResultConsumer::ensure_group(self, client).await
    }

    /// 业务作用：委托 result 消费者的单轮裁决。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：本轮报告。
    async fn poll_once(
        &self,
        client: &RedisClient,
        now_ms: i64,
    ) -> anyhow::Result<StreamPollReport> {
        SagaRedisStreamResultConsumer::poll_once(self, client, now_ms).await
    }
}

#[async_trait::async_trait]
impl<H: SagaCommandHandler> SagaStreamPoller for SagaRedisStreamCommandConsumer<H> {
    /// 业务作用：暴露 command 消费配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置引用。
    fn config(&self) -> &SagaStreamConsumerConfig {
        SagaRedisStreamCommandConsumer::config(self)
    }

    /// 业务作用：委托 command 消费者的 group 幂等创建。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    ///
    /// 返回：group 就绪返回 `Ok`。
    async fn ensure_group(&self, client: &RedisClient) -> anyhow::Result<()> {
        SagaRedisStreamCommandConsumer::ensure_group(self, client).await
    }

    /// 业务作用：委托 command 消费者的单轮裁决;命令裁决不依赖调用方时刻。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端。
    /// - `now_ms`: 未使用(统一对象安全签名)。
    ///
    /// 返回：本轮报告。
    async fn poll_once(
        &self,
        client: &RedisClient,
        _now_ms: i64,
    ) -> anyhow::Result<StreamPollReport> {
        SagaRedisStreamCommandConsumer::poll_once(self, client).await
    }
}

/// 业务作用：按"全部 consumer group 的已确认前沿"计算安全 `XTRIM MINID` 并执行清剪。
///
/// 禁止无界 `MAXLEN ~` 充当安全清理——它可能删掉尚未确认的 entry。本方法读取 stream 上
/// **每个** group 的 `last-delivered-id` 与 PEL 最小 id,取全体最小值作为 MINID:该 id 之前
/// 的 entry 已被所有 group 消费确认,删除不影响任何 PEL 与未读消息。不共享保留合同的
/// consumer 应使用独立 stream,而不是放宽本口径。
///
/// **证据采集与删除在一条 Lua 脚本内原子执行**:Redis 脚本执行期间不接受任何并发命令,
/// 因此"快照前沿之后、XTRIM 之前有新 group 以历史回放位创建"的窗口按构造不存在——新
/// group 要么在脚本前建立(参与前沿,其未读历史受保护),要么在脚本后建立(只能看到清剪
/// 后的流,与其创建时刻的可见状态一致)。脚本内任何 group 的证据不完整(名称/前沿/PEL
/// 概要读不出、形态异常)都放弃本轮清剪,零删除;PEL 非空而最小 pending id 不可读同样
/// 零删除——绝不退回 last-delivered 越过未确认消息。
///
/// 参数说明：
/// - `client`: Redis 客户端。
/// - `stream`: 目标 stream。
///
/// 返回：本次删除的 entry 数;stream 无 group、或任一证据不完整时零删除返回 0。
pub async fn safe_trim_by_group_frontier(
    client: &RedisClient,
    stream: &str,
) -> anyhow::Result<u64> {
    // 脚本只触达 KEYS[1] 一个键,Cluster 同槽约束天然满足。前沿比较在 Lua 内按
    // ms-seq 数值语义实现,不做字符串比较。
    const TRIM_SCRIPT: &str = r#"
local function parse_id(raw)
    if type(raw) ~= 'string' then return nil end
    local ms, seq = string.match(raw, '^(%d+)-(%d+)$')
    if ms == nil then return nil end
    return { tonumber(ms), tonumber(seq) }
end
local function less(a, b)
    if a[1] ~= b[1] then return a[1] < b[1] end
    return a[2] < b[2]
end
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if type(groups) ~= 'table' or #groups == 0 then
    return 0
end
local frontier = nil
for _, group in ipairs(groups) do
    if type(group) ~= 'table' then return 0 end
    local name = nil
    local delivered = nil
    for index = 1, #group - 1, 2 do
        local field = group[index]
        if field == 'name' then name = group[index + 1] end
        if field == 'last-delivered-id' then delivered = group[index + 1] end
    end
    if name == nil then return 0 end
    local candidate = parse_id(delivered)
    local pending = redis.call('XPENDING', KEYS[1], name)
    if type(pending) ~= 'table' or #pending < 2 then return 0 end
    local count = pending[1]
    if type(count) ~= 'number' then return 0 end
    if count > 0 then
        local minimum = parse_id(pending[2])
        if minimum == nil then return 0 end
        if candidate == nil or less(minimum, candidate) then candidate = minimum end
    end
    if candidate == nil then return 0 end
    if frontier == nil or less(candidate, frontier) then frontier = candidate end
end
if frontier == nil then return 0 end
return redis.call('XTRIM', KEYS[1], 'MINID', frontier[1] .. '-' .. frontier[2])
"#;
    let mut connection = client.conn();
    let trimmed: u64 = redis::cmd("EVAL")
        .arg(TRIM_SCRIPT)
        .arg(1)
        .arg(stream)
        .query_async(&mut connection)
        .await
        .map_err(|error| anyhow::anyhow!("safe trim script failed: {error}"))?;
    Ok(trimmed)
}

/// 业务作用：把 `ms-seq` 形态的 entry id 解析成可比较的二元组。
///
/// 参数说明：
/// - `raw`: entry id 文本。
///
/// 返回：解析成功返回 `(ms, seq)`；非法返回 `None`。
fn parse_entry_id(raw: &str) -> Option<(u64, u64)> {
    let (ms, seq) = raw.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// 业务作用：Redis Streams transport 的受管就绪探针——Ready 前校验连接、stream/group
/// 拓扑与 DLT 同槽合同,任何一项不满足都拒绝进入 Ready。
///
/// 参数说明：
/// - `client`: Redis 客户端。
/// - `configs`: 本进程全部 stream 消费配置。
///
/// 返回：PING 可达、全部配置合法且 group 幂等就绪时返回 `Ok`；否则返回拒绝 Ready 的错误。
pub async fn verify_stream_transport_ready(
    client: &RedisClient,
    configs: &[&SagaStreamConsumerConfig],
) -> anyhow::Result<()> {
    let mut connection = client.conn();
    let pong: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .map_err(|error| anyhow::anyhow!("redis stream transport unreachable: {error}"))?;
    if pong != "PONG" {
        anyhow::bail!("redis stream transport returned an unexpected ping reply");
    }
    for config in configs {
        config.validate()?;
        // group 幂等创建也是 ACL 探测:无 XGROUP/XADD 权限在 Ready 前暴露,
        // 不等运行期毒消息。
        StreamConsumerCore {
            config: Arc::new((*config).clone()),
            reclaim_cursor: std::sync::Mutex::new("0-0".to_string()),
        }
        .ensure_group(client)
        .await?;
    }
    Ok(())
}
