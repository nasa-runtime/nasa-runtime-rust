// ============================================================================
// src/proxy.rs：对齐既有 RedisProxy.loadStreamSubscribe 的 PROXY 消费路径。
//
// **共享 group 多 consumer 并行消费**(高吞吐、无序),区别于 PARTITION(每分区串行 + owner 锁 +
// fenced ACK):PROXY 是**单共享 stream + 一个 consumer group**,N 个 consumer 并行 `XREADGROUP >`,
// Redis 自动把消息分发给空闲 consumer;**无 owner/fence**(无需互斥,本就并行)。
// at-least-once:handler 失败 → 不 ACK,留 group PEL;`XAUTOCLAIM` 周期回收 idle pending(含崩溃
// consumer 的 + 本节点失败的)重投;超 `max_redeliver` → poison(Drop=XACK 丢弃 / Dlq=转 DLQ stream)。
//
// 设计取向:与 PARTITION 同一 `Envelope`(topic/event/data)+ 同一 handler 注册形态(逐 ID 解码、
// handler 失败/panic 归约为该批 failed),复用心智模型;publish = 无路由 XADD 到共享 stream。
// ============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::config::{MAX_REDIS_NAME_BYTES, MAX_REDIS_RUNTIME_DURATION_MS, MAX_STREAM_BATCH_SIZE};
use crate::error::{NasaRedisError, Result};
use crate::partition::{Envelope, ErasedHandler, HandlerMap, DATA_FIELD};

/// 单实例 PROXY consumer 任务上限。
///
/// 每个 consumer 都是独立 Tokio task，且持有 Redis 轮询状态；更高并行度应通过多实例横向扩展，
/// 不能让一个配置值在启动时无界 spawn。
const MAX_PROXY_CONSUMERS: usize = 256;

/// PROXY 消费配置。
///
/// ⚠ **PROXY handler 应幂等**:本路径无 owner-fence,reclaim(XAUTOCLAIM)会回收
/// idle pending 重投——慢/崩溃 consumer 的在途消息可能被另一 consumer 重跑。当 `handler_timeout_ms>0`
/// 且 `reclaim_min_idle_ms > handler_timeout_ms` 时,handler 被 timeout 中断早于被 reclaim 抢占,从时间上
/// **关闭"抢占仍在跑的 handler"双跑窗口**。**默认 `handler_timeout_ms=10s`(< 默认 reclaim 30s)→ 双跑窗口
/// 默认关闭**；启动校验拒绝无界 handler。
#[derive(Debug, Clone)]
pub struct ProxyCfg {
    /// 每节点并行 consumer 任务数(>=1)。
    pub consumers: usize,
    /// 每次 XREADGROUP COUNT。
    pub batch_size: usize,
    /// 冷流再 poll 间隔毫秒(**NOBLOCK 轮询** + 空轮 sleep——不在共享多路复用连接上用 BLOCK,
    /// 否则 BLOCK 命令会卡住整条 multiplexed 连接的其它命令,对齐 partition 托管模式 NOBLOCK 语义)。
    pub poll_idle_ms: u64,
    /// 重投上限,超过即 poison(必须 >=1;0 会让首投即判毒)。
    pub max_redeliver: u32,
    /// XAUTOCLAIM 回收 idle 阈值毫秒(pending 闲置超此即可被重投)。
    pub reclaim_min_idle_ms: u64,
    /// 毒消息策略。
    pub poison: ProxyPoison,
    /// handler 强制超时毫秒。超时整桶留 PEL 待重投。
    /// ⚠ 启用时**必须** `reclaim_min_idle_ms > handler_timeout_ms`(prepare 校验):这样在途 handler
    /// 要么超时释放、要么 ACK 完成,**早于**被 XAUTOCLAIM 回收的时点——从结构上关闭"reclaim 抢占
    /// 仍在跑的慢 handler 致双跑"窗口。
    pub handler_timeout_ms: u64,
    /// publish 时给共享 stream 的近似上限。
    /// `Some(n)` → `XADD MAXLEN ~ n`,防 ACK 只出 PEL、entry 留存的无界增长 OOM。
    pub max_stream_len: Option<u64>,
    /// 未注册 (topic,event) 的处置
    /// `false`(默认)= 本节点无 handler 即 **XACK 丢弃**(发 warn 日志);
    /// `true` = **留 PEL**(不 ACK),让 XAUTOCLAIM 把它交给注册了 handler 的其它节点,
    ///   始终无人处理则最终 poison。
    ///
    /// ⚠ **异构多节点 = 静默丢消息**:默认 `false` 假设**全节点注册同一
    /// handler 集**(同构)。若节点 B 没注册 event X 而先读到 X 的消息,B 会**直接 XACK 丢弃**,注册了 X 的
    /// 节点 A **永远收不到** → 静默丢失(仅一条 warn)。**多节点共享组务必全节点注册同一 handler 集**;
    /// **资金类/不可丢的共享组应显式置 `true`**(留 PEL 交注册节点,真无人处理才 poison,不静默丢)。
    ///
    /// ⚠ **`true` 是"降低静默丢失风险",不是可靠路由**:无 handler 节点留 PEL 后,
    /// **它自己的 reclaim loop 也会再 claim 到这条**;若 `max_redeliver` 小、`reclaim_min_idle_ms` 激进,
    /// 注册了 handler 的节点未必先抢到 → 最终仍可能 poison。要**严格路由**请按 handler 集**拆 stream/group**
    /// (异构节点各消费各自的 stream),`requeue_unregistered=true` 只是同构假设破裂时的兜底,非保证。
    pub requeue_unregistered: bool,
    /// 停机 drain 预算毫秒。超时未退的 task 强制 abort,
    /// 停机不被卡死 handler 永久阻塞。
    pub drain_deadline_ms: u64,
}

impl Default for ProxyCfg {
    /// 业务作用：构造 stream proxy 默认配置。
    ///
    /// 默认单消费者、100 条批量、Park 前的重投上限为 5,并设置非零 handler 超时避免慢处理器双跑。
    fn default() -> Self {
        Self {
            consumers: 1,
            batch_size: 100,
            poll_idle_ms: 200,
            max_redeliver: 5,
            reclaim_min_idle_ms: 30_000,
            poison: ProxyPoison::Drop,
            //**默认非零**(< 默认 reclaim_min_idle 30s)关掉"慢 handler 双跑"footgun——
            // 默认配置下 handler 10s 必被 timeout 中断,早于 30s 被 reclaim 抢占重投,故无在途双跑。
            // 与 partition 一样，prepare 拒绝 0，防止无限 handler 与 reclaim 双跑。
            handler_timeout_ms: 10_000,
            max_stream_len: Some(1_000_000),
            requeue_unregistered: false,
            drain_deadline_ms: 10_000,
        }
    }
}

/// PROXY 毒消息策略(无 fence/quarantine,比 partition 简化)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyPoison {
    /// 超上限 → XACK 丢弃(记 error 日志)。
    Drop,
    /// 超上限 → 转存到 `{stream}:dlq` + XACK 源。
    Dlq,
}

/// 新建 consumer group 的起始位点(**仅首次建组生效**;BUSYGROUP 已存在则不变)。
/// 传给 [`PreparedProxy::prepare_with_offset`];[`PreparedProxy::prepare`] 默认用 `New`。
///对齐 原实现 `RedisProxy` 的 PROXY 语义——原实现 PROXY 路径用 `ReadOffset.latest()`(`$`)
/// 只消费组建后的新消息(live-subscribe),`from("0-0")` 仅 partition(durable 工作队列)用。Rust proxy
/// 此前误用 `0-0`,会让新 proxy 部署到**有 backlog 的 stream** 时重放全部历史(事件洪泛 footgun)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyStartOffset {
    /// `$`:只消费组建后的新消息([`prepare`](PreparedProxy::prepare) 的默认;对齐 原实现 PROXY=live-subscribe)。
    New,
    /// `0-0`:从头消费(含历史 backlog)。durable-replay 语义,需要重放历史时显式选用。
    History,
}

impl ProxyStartOffset {
    /// 业务作用：XGROUP CREATE 的位点字面量。
    fn redis_id(self) -> &'static str {
        match self {
            ProxyStartOffset::New => "$",
            ProxyStartOffset::History => "0-0",
        }
    }
}

/// 阶段一:已建 group,未启消费(register 只在此阶段)。
pub struct PreparedProxy {
    client: Arc<RedisClient>,
    stream: String,
    group: String,
    handlers: HandlerMap,
    cfg: ProxyCfg,
}

impl PreparedProxy {
    /// 业务作用：prepare:建共享 stream + consumer group(MKSTREAM;BUSYGROUP 幂等)。
    /// **建组位点默认 `$`**(只消费组建后的新消息,对齐 原实现 `RedisProxy` PROXY=live-subscribe);
    /// 需要从头重放历史 backlog 时改用 [`prepare_with_offset`](Self::prepare_with_offset) 传
    /// [`ProxyStartOffset::History`]。
    ///
    /// # 参数
    /// - `client`: Redis 客户端共享句柄。
    /// - `stream`: 共享 stream 名。
    /// - `group`: consumer group 名。
    /// - `cfg`: proxy 消费运行配置。
    pub async fn prepare(
        client: Arc<RedisClient>,
        stream: impl Into<String>,
        group: impl Into<String>,
        cfg: ProxyCfg,
    ) -> Result<Self> {
        // 默认位点 `$`(对齐 原实现 PROXY);。
        Self::prepare_with_offset(client, stream, group, cfg, ProxyStartOffset::New).await
    }

    /// 业务作用：同 [`prepare`](Self::prepare),但**显式指定建组起始位点**(原实现 `RedisProxy.xGroupCreate(stream,
    /// group, ReadOffset)` 重载的对应物):`New`=`$`(只收新消息)/ `History`=`0-0`(从头重放历史)。
    /// 位点**仅首次建组生效**(BUSYGROUP 已存在则不变)。不需要重放历史就用 [`prepare`](Self::prepare) 走默认 `$`。
    ///
    /// # 参数
    /// - `client`: Redis 客户端共享句柄。
    /// - `stream`: 共享 stream 名。
    /// - `group`: consumer group 名。
    /// - `cfg`: proxy 消费运行配置。
    /// - `start_offset`: 首次建组时使用的 stream 起始位点。
    pub async fn prepare_with_offset(
        client: Arc<RedisClient>,
        stream: impl Into<String>,
        group: impl Into<String>,
        cfg: ProxyCfg,
        start_offset: ProxyStartOffset,
    ) -> Result<Self> {
        if cfg.consumers == 0 || cfg.batch_size == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.consumers/batch_size 必须 > 0".into(),
            ));
        }
        if cfg.consumers > MAX_PROXY_CONSUMERS {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg.consumers 过大(上限 {MAX_PROXY_CONSUMERS})"
            )));
        }
        if cfg.batch_size > MAX_STREAM_BATCH_SIZE {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg.batch_size 过大(上限 {MAX_STREAM_BATCH_SIZE})"
            )));
        }
        if cfg.reclaim_min_idle_ms == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.reclaim_min_idle_ms 必须 > 0".into(),
            ));
        }
        if cfg.handler_timeout_ms == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.handler_timeout_ms 必须 > 0".into(),
            ));
        }
        if cfg.drain_deadline_ms == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.drain_deadline_ms 必须 > 0".into(),
            ));
        }
        if cfg.max_stream_len == Some(0) {
            return Err(NasaRedisError::Config(
                "ProxyCfg.max_stream_len 配置后必须 > 0".into(),
            ));
        }
        if cfg.poll_idle_ms == 0
            || cfg.poll_idle_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || cfg.reclaim_min_idle_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || cfg.handler_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || cfg.drain_deadline_ms > MAX_REDIS_RUNTIME_DURATION_MS
        {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg 的 poll/reclaim/handler/drain 时长必须位于 1..={MAX_REDIS_RUNTIME_DURATION_MS}ms"
            )));
        }
        //max_redeliver=0 会让每条消息首投(deliveries>=1)即判毒 → 全量进毒处置。
        if cfg.max_redeliver == 0 {
            return Err(NasaRedisError::Config(
                "ProxyCfg.max_redeliver 必须 >= 1(否则首投即判毒)".into(),
            ));
        }
        //启用 handler 超时时,必须 reclaim_min_idle > handler_timeout,
        // 否则仍在跑的 handler 会被 XAUTOCLAIM 抢占重投(双跑 + 跨消费者 ACK)。
        if cfg.handler_timeout_ms > 0 && cfg.reclaim_min_idle_ms <= cfg.handler_timeout_ms {
            return Err(NasaRedisError::Config(format!(
                "ProxyCfg.reclaim_min_idle_ms({}) 必须 > handler_timeout_ms({})——否则 reclaim 会抢占在途 handler 致双跑",
                cfg.reclaim_min_idle_ms, cfg.handler_timeout_ms
            )));
        }
        let stream = stream.into();
        let group = group.into();
        if stream.trim().is_empty()
            || group.trim().is_empty()
            || stream != stream.trim()
            || group != group.trim()
            || stream.len() > MAX_REDIS_NAME_BYTES
            || group.len() > MAX_REDIS_NAME_BYTES
        {
            return Err(NasaRedisError::Config(format!(
                "PROXY stream / group 必须无首尾空白、非空且不超过 {MAX_REDIS_NAME_BYTES} 字节"
            )));
        }
        //位点按 start_offset 参数(默认 `$`,对齐 原实现 PROXY live-subscribe);
        // 仅首次建组生效(BUSYGROUP 已存在则位点不变)。partition 才用 `0-0`(durable 工作队列)。
        let r: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg(start_offset.redis_id())
            .arg("MKSTREAM")
            .query_async(&mut client.conn())
            .await;
        match r {
            Ok(_) => {}
            Err(e) if e.to_string().contains("BUSYGROUP") => {}
            Err(e) => return Err(e.into()),
        }
        Ok(Self {
            client,
            stream,
            group,
            handlers: HashMap::new(),
            cfg,
        })
    }

    /// 业务作用：注册 handler(逐 ID 解码 T;handler 失败/panic → 该批 failed)。与 partition 同形态。
    ///
    ///
    /// # 参数
    /// - `topic`: stream/partition 使用的业务主题。
    /// - `event`: 该 topic 下的事件类型,用于选择批量处理器。
    /// - `f`: 解码出同批业务消息后执行的异步处理器。
    pub fn register<T, F, Fut>(&mut self, topic: &str, event: &str, f: F) -> &mut Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: ErasedHandler = Arc::new(move |items: Vec<(String, serde_json::Value)>| {
            let f = Arc::clone(&f);
            Box::pin(async move {
                let mut typed: Vec<T> = Vec::with_capacity(items.len());
                let mut good_ids: Vec<String> = Vec::with_capacity(items.len());
                let mut failed: Vec<String> = Vec::new();
                for (id, v) in items {
                    match serde_json::from_value::<T>(v) {
                        Ok(t) => {
                            typed.push(t);
                            good_ids.push(id);
                        }
                        Err(e) => {
                            tracing::error!(id, err = %e, "PROXY 业务类型解码失败,该 ID 进重投/毒");
                            failed.push(id);
                        }
                    }
                }
                if typed.is_empty() {
                    return failed;
                }
                let fut = std::panic::AssertUnwindSafe(f(typed));
                match futures::FutureExt::catch_unwind(fut).await {
                    Ok(Ok(())) => failed,
                    Ok(Err(e)) => {
                        tracing::warn!(err = %e, "PROXY handler 失败,本桶 ID 转重投");
                        failed.extend(good_ids);
                        failed
                    }
                    Err(_) => {
                        tracing::error!("PROXY handler panic,本桶 ID 转重投");
                        failed.extend(good_ids);
                        failed
                    }
                }
            })
        });
        self.handlers
            .insert((topic.to_string(), event.to_string()), h);
        self
    }

    /// 业务作用：start:spawn `consumers` 个消费循环 + 1 个回收循环;返回 RunningProxy。
    pub async fn start(self) -> Result<RunningProxy> {
        let cancel = CancellationToken::new();
        let shared = Arc::new(ProxyShared {
            client: Arc::clone(&self.client),
            stream: self.stream.clone(),
            group: self.group.clone(),
            handlers: self.handlers,
            cfg: self.cfg.clone(),
        });
        let mut handles = Vec::new();
        // 记下所有 consumer 名,停机时 XGROUP DELCONSUMER 清理(名字每次随机 UUID,
        // 不清理则空 consumer 在组里无限堆积,污染 XINFO CONSUMERS 视图)。
        let mut consumer_names = Vec::with_capacity(self.cfg.consumers + 1);
        for i in 0..self.cfg.consumers {
            let consumer = format!("c-{}-{}", uuid::Uuid::new_v4().simple(), i);
            consumer_names.push(consumer.clone());
            handles.push(tokio::spawn(consumer_loop(
                Arc::clone(&shared),
                consumer,
                cancel.clone(),
            )));
        }
        // 回收循环(XAUTOCLAIM idle pending → 重投/poison)——名字在 start 生成以便停机清理。
        let reclaim_consumer = format!("reclaim-{}", uuid::Uuid::new_v4().simple());
        consumer_names.push(reclaim_consumer.clone());
        handles.push(tokio::spawn(reclaim_loop(
            Arc::clone(&shared),
            reclaim_consumer,
            cancel.clone(),
        )));
        Ok(RunningProxy {
            client: self.client,
            stream: self.stream,
            group: self.group,
            consumer_names,
            cancel,
            handles,
            max_stream_len: self.cfg.max_stream_len,
            drain_deadline_ms: self.cfg.drain_deadline_ms,
        })
    }
}

/// 保存内部共享状态；用于在多个调用路径之间复用数据。
struct ProxyShared {
    client: Arc<RedisClient>,
    stream: String,
    group: String,
    handlers: HandlerMap,
    cfg: ProxyCfg,
}

/// 运行态:发布 + 停机。
pub struct RunningProxy {
    client: Arc<RedisClient>,
    stream: String,
    group: String,
    /// 本节点的 consumer 名(含 reclaim),停机时 DELCONSUMER 清理。
    consumer_names: Vec<String>,
    cancel: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    /// publish 近似上限(None=不限);停机 drain 预算。
    max_stream_len: Option<u64>,
    drain_deadline_ms: u64,
}

impl RunningProxy {
    /// 业务作用：发布到共享 stream(无路由;consumers 竞争消费)。返回 entry ID。
    ///
    /// **wire 说明**:写 `XADD * data {Envelope JSON}`——**单 `data` 字段裹标准
    /// `Envelope{topic,event,data,passthrough}`**,**不等于** 原实现 `RedisProxy.publish` 的 `XADD * {event}
    /// {message}`(event 名作 entry field、可多 event/entry)。即 **Rust Proxy 与 原实现 原生 PROXY wire 不互通**:
    /// 原实现 侧要与本 Proxy 共享 stream,必须改写成 `data`-field Envelope(适配器);否则无 `data` 字段的 原实现-shape
    /// entry 会被本 Proxy 判为不可解析 → 转 DLQ(不再静默卡 PEL)。当前不支持双 wire 自动识别。
    ///
    /// # 参数
    /// - `topic`: stream/partition 使用的业务主题。
    /// - `event`: 写入信封的事件类型。
    /// - `data`: 待序列化进信封 `data` 字段的业务对象。
    pub async fn publish<T: Serialize>(
        &self,
        topic: &str,
        event: &str,
        data: &T,
    ) -> Result<String> {
        if topic.is_empty() || event.is_empty() {
            return Err(NasaRedisError::Config(format!(
                "InvalidPublish: topic/event 为空(topic={topic:?}, event={event:?})"
            )));
        }
        let env = Envelope {
            topic: topic.to_string(),
            event: event.to_string(),
            data: serde_json::to_value(data).map_err(|e| NasaRedisError::Codec(e.to_string()))?,
            passthrough: None,
        };
        let body = serde_json::to_vec(&env).map_err(|e| NasaRedisError::Codec(e.to_string()))?;
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.stream);
        //可选近似上限,防共享 stream 无界增长 OOM(ACK 只出 PEL,entry 留存)。
        if let Some(n) = self.max_stream_len {
            cmd.arg("MAXLEN").arg("~").arg(n);
        }
        cmd.arg("*").arg(DATA_FIELD).arg(body);
        let id: String = cmd.query_async(&mut self.client.conn()).await?;
        Ok(id)
    }

    /// 业务作用：停机:cancel 所有循环并等其退出(**带 drain 预算**,卡死 handler 不会让停机永久阻塞)。
    /// 超预算未退的 task 显式 `abort()`(JoinHandle drop 只分离不中止,卡死 handler 会变游离 task)。
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        let deadline = self.drain_deadline_ms;
        // 先留一份 abort 句柄(超时兜底),再整体超时等待全部 join 退出。
        let handles = std::mem::take(&mut self.handles);
        let aborts: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
        let join_all = futures::future::join_all(handles);
        let mut aborted = false;
        if tokio::time::timeout(Duration::from_millis(deadline), join_all)
            .await
            .is_err()
        {
            tracing::warn!(
                deadline_ms = deadline,
                "PROXY 停机 drain 超时,abort 残余 task"
            );
            for a in aborts {
                a.abort();
            }
            aborted = true;
        }
        //**abort(超时)路径整体跳过 DELCONSUMER**——abort 可能落在 reclaim 的
        // XAUTOCLAIM 与 XACK **之间**,此时 consumer 已持 PEL 但下方 XPENDING 快照可能仍显示 0(竞态),
        // 误删 = 丢消息。abort 已是非优雅退出,宁留空 consumer(靠后续 XAUTOCLAIM/运维清理)也不冒删 PEL 风险。
        if aborted {
            tracing::warn!("PROXY 停机走 abort 路径,跳过 DELCONSUMER(避免删到刚被 XAUTOCLAIM 占用、快照未及更新的 consumer 的 PEL)");
            return;
        }
        // 正常 drain(全 task 已 join 退出,无在途 XAUTOCLAIM):**仅删 PEL 为空的 consumer**——
        // `XGROUP DELCONSUMER` 会把该 consumer **未 ACK 的 PEL pending 一并删除丢失**(不是重分配)。PROXY 是
        // at-least-once,handler 失败/在途的消息故意留 PEL 等 XAUTOCLAIM 重投;无条件 DELCONSUMER = 确定性丢消息。
        // 因此检查每个 consumer 的 pending 数，只清理 pending==0 的空 consumer；有 PEL 的交给 reclaim。
        let pending = consumer_pending_counts(&self.client, &self.stream, &self.group)
            .await
            .unwrap_or_default();
        for name in &self.consumer_names {
            let p = pending.get(name).copied().unwrap_or(0);
            if p > 0 {
                tracing::debug!(consumer = %name, pending = p, "PROXY 停机:consumer 仍有 PEL,跳过 DELCONSUMER(留 reclaim,防丢消息)");
                continue;
            }
            let r: std::result::Result<i64, redis::RedisError> = redis::cmd("XGROUP")
                .arg("DELCONSUMER")
                .arg(&self.stream)
                .arg(&self.group)
                .arg(name)
                .query_async(&mut self.client.conn())
                .await;
            if let Err(e) = r {
                tracing::warn!(consumer = %name, err = %e, "PROXY 停机 DELCONSUMER 失败(best-effort)");
            }
        }
    }
}

impl Drop for RunningProxy {
    /// 业务作用：未显式 shutdown 时立即停止 consumer/reclaim，禁止句柄丢失后继续消费。
    ///
    /// Drop 不能安全执行 `XGROUP DELCONSUMER`：任务可能刚把消息移入 PEL。这里只关闭 admission 并
    /// abort，保留 consumer/PEL 给后续 XAUTOCLAIM；正常路径仍应调用 [`shutdown`](Self::shutdown)。
    fn drop(&mut self) {
        self.cancel.cancel();
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

/// 业务作用：取消费组各 consumer 的 PEL pending 数(XPENDING summary 第 4 段 `[[name, count], ...]`)。
/// 用于停机时判断哪些 consumer 可安全 DELCONSUMER(count==0)。解析异常返空表(调用方保守不删)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `stream`: 需要读取 XPENDING 摘要的 Redis Stream key。
/// - `group`: 消费组、服务分组或任务分组名称。
async fn consumer_pending_counts(
    client: &RedisClient,
    stream: &str,
    group: &str,
) -> std::result::Result<HashMap<String, i64>, redis::RedisError> {
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(&mut client.conn())
        .await?;
    let mut out = HashMap::new();
    let (redis::Value::Array(cols) | redis::Value::Set(cols)) = &v else {
        return Ok(out);
    };
    // cols[3] = [[consumer, count_str], ...](空 PEL 时为 Nil)
    if let Some(redis::Value::Array(list) | redis::Value::Set(list)) = cols.get(3) {
        for row in list {
            if let redis::Value::Array(kv) | redis::Value::Set(kv) = row {
                if let (Some(name), Some(cnt)) = (kv.first(), kv.get(1)) {
                    let name = match name {
                        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                        redis::Value::SimpleString(s) => s.clone(),
                        _ => continue,
                    };
                    // count 在 summary 里是字符串
                    let c = match cnt {
                        redis::Value::BulkString(b) => {
                            String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0)
                        }
                        redis::Value::SimpleString(s) => s.parse::<i64>().unwrap_or(0),
                        redis::Value::Int(n) => *n,
                        _ => 0,
                    };
                    out.insert(name, c);
                }
            }
        }
    }
    Ok(out)
}

/// 解析失败的 entry(无 `data` 字段 / `data` 非 Envelope JSON)。**此前 parser
/// 直接“跳过”坏 entry,但 entry 已被 XREADGROUP/XAUTOCLAIM 交付进 PEL,跳过后既不 ACK 也不 poison →
/// **永久 pending 泄漏**(且每轮 reclaim 重复占用前几个名额)。改为保留坏 entry 的 id + reason + 原始字段,
/// 走 [`dispose_bad`] 立即处置(Dlq 转存原始字段 / Drop XACK),与旧文档“缺 data 即 tombstone,避免无限重投”
/// 的意图一致。坏 entry **确定性不可解析**(重读必再失败),故立即处置、不耗 max_redeliver 重投。
struct BadEntry {
    id: String,
    reason: String,
    /// 原始 field/value 字节对(**lossless**:保序、保重复 field、保二进制字节),供 DLQ 完整复盘
    /// (此前 lossy-utf8 + JSON map 会丢二进制 + 覆盖重复 field)。
    raw: Vec<(Vec<u8>, Vec<u8>)>,
}

/// `parse_entries` 结果:成功 `(id, Envelope)` 与坏 entry 分流。
struct ParsedEntries {
    ok: Vec<(String, Envelope)>,
    bad: Vec<BadEntry>,
}

/// 业务作用：解析 XREADGROUP / XAUTOCLAIM 的 entry 列表 → 成功 `(id, Envelope)` + 坏 entry(保留 id,不丢)。
///
/// # 参数
/// - `entries`: 批处理或流水线中的命令条目集合。
fn parse_entries(entries: &redis::Value) -> ParsedEntries {
    let mut ok = Vec::new();
    let mut bad = Vec::new();
    //接受 Array|Set(RESP3 部分形态返 Set);其它非 nil 形态不静默吞,记 warn。
    let (redis::Value::Array(items) | redis::Value::Set(items)) = entries else {
        if !matches!(entries, redis::Value::Nil) {
            tracing::warn!("PROXY entries 形态非 Array/Set(疑似 RESP3 未适配),本批跳过");
        }
        return ParsedEntries { ok, bad };
    };
    for item in items {
        // item = [id, [field, value, ...]]
        let redis::Value::Array(pair) = item else {
            continue;
        };
        let Some(redis::Value::BulkString(id_bytes)) = pair.first() else {
            continue;
        };
        let id = String::from_utf8_lossy(id_bytes).into_owned();
        //Nil/缺字段段 = XAUTOCLAIM 已删 entry(此 id 不在本 consumer PEL),跳过;
        // 但**拿到 id 而字段段形态异常(非 Array 非 Nil)**不能静默丢——entry 可能在 PEL,转 BadEntry。
        let fields = match pair.get(1) {
            Some(redis::Value::Array(f)) => f,
            None | Some(redis::Value::Nil) => continue,
            Some(_) => {
                bad.push(BadEntry {
                    id,
                    reason: "entry 字段段形态异常(非 Array/Nil)".to_string(),
                    raw: Vec::new(),
                });
                continue;
            }
        };
        // 收集 DATA_FIELD 的值 + 全部原始字段(lossless 字节,供坏 entry 转 DLQ 完整复盘)
        let mut data_bytes: Option<&[u8]> = None;
        let mut raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut it = fields.iter();
        while let (Some(f), Some(v)) = (it.next(), it.next()) {
            if let (redis::Value::BulkString(fk), redis::Value::BulkString(vv)) = (f, v) {
                if fk == DATA_FIELD.as_bytes() {
                    data_bytes = Some(vv);
                }
                raw.push((fk.clone(), vv.clone()));
            }
        }
        let Some(bytes) = data_bytes else {
            // 无 `data` 字段(如 原实现 event-field wire,或缺字段的 producer)→ 坏 entry,不丢。
            bad.push(BadEntry {
                id,
                reason: "缺 data 字段(非标准 Envelope wire)".to_string(),
                raw,
            });
            continue;
        };
        match serde_json::from_slice::<Envelope>(bytes) {
            Ok(env) => ok.push((id, env)),
            Err(e) => {
                tracing::error!(id, err = %e, "PROXY entry envelope 解析失败,转 DLQ/Drop(不再静默卡 PEL)");
                bad.push(BadEntry {
                    id,
                    reason: format!("envelope 解析失败: {e}"),
                    raw,
                });
            }
        }
    }
    ParsedEntries { ok, bad }
}

/// 业务作用：把一批 (id, Envelope) 按 (topic,event) 分桶,逐桶 dispatch handler,返回**失败 ID 集合**。
/// 无 handler 的 (topic,event) → 当作成功(ACK,不滞留;对齐 partition 未注册即跳过的语义)。
///
/// # 参数
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `batch`: 一次性提交到 Redis 的批量命令。
async fn dispatch(shared: &ProxyShared, batch: Vec<(String, Envelope)>) -> Vec<String> {
    let mut buckets: HashMap<(String, String), Vec<(String, serde_json::Value)>> = HashMap::new();
    for (id, env) in batch {
        buckets
            .entry((env.topic, env.event))
            .or_default()
            .push((id, env.data));
    }
    let mut failed = Vec::new();
    for (key, items) in buckets {
        match shared.handlers.get(&key) {
            Some(h) => {
                let bucket_ids: Vec<String> = items.iter().map(|(id, _)| id.clone()).collect();
                //handler 超时强制——超时整桶留 PEL(转 failed),不让挂起 handler 永占 task。
                let dur = Duration::from_millis(shared.cfg.handler_timeout_ms);
                match tokio::time::timeout(dur, h(items)).await {
                    Ok(f) => failed.extend(f),
                    Err(_) => {
                        tracing::warn!(topic = %key.0, event = %key.1, timeout_ms = shared.cfg.handler_timeout_ms, "PROXY handler 超时,整桶转重投(留 PEL)");
                        failed.extend(bucket_ids);
                    }
                }
            }
            None => {
                //共享 stream 多节点下,本节点无 handler 即丢弃 → 注册了的节点永收不到
                // = 确定性丢消息。降级为 **warn**(非 debug)使可见;requeue_unregistered=true 时留 PEL
                // 交 XAUTOCLAIM 转给注册节点。
                tracing::warn!(topic = %key.0, event = %key.1, requeue = shared.cfg.requeue_unregistered, "PROXY 本节点无注册 handler");
                if shared.cfg.requeue_unregistered {
                    failed.extend(items.into_iter().map(|(id, _)| id));
                }
            }
        }
    }
    failed
}

// Acknowledges processed proxy stream entries.
///
/// # 参数
/// 业务作用：- `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `ids`: Redis stream entry id 或业务记录 id 列表。
async fn ack(shared: &ProxyShared, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    let mut cmd = redis::cmd("XACK");
    cmd.arg(&shared.stream).arg(&shared.group);
    for id in ids {
        cmd.arg(id);
    }
    let r: std::result::Result<i64, redis::RedisError> =
        cmd.query_async(&mut shared.client.conn()).await;
    if let Err(e) = r {
        tracing::warn!(err = %e, "PROXY XACK 失败(下轮 XAUTOCLAIM 会重投)");
    }
}

// Runs one proxy consumer loop until cancellation.
///
/// # 参数
/// 业务作用：- `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `consumer`: Redis Stream consumer 名称。
/// - `cancel`: 后台任务使用的取消信号。
async fn consumer_loop(shared: Arc<ProxyShared>, consumer: String, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            r = read_new(&shared, &consumer) => {
                match r {
                    Ok(parsed) if !parsed.ok.is_empty() || !parsed.bad.is_empty() => {
                        // 坏 entry 立即处置(转 DLQ/Drop),否则永久卡 PEL。
                        if !parsed.bad.is_empty() {
                            dispose_bad(&shared, &parsed.bad).await;
                        }
                        if !parsed.ok.is_empty() {
                            let ids: Vec<String> = parsed.ok.iter().map(|(id, _)| id.clone()).collect();
                            let failed = dispatch(&shared, parsed.ok).await;
                            // ACK 成功子集(failed 留 PEL,reclaim_loop 重投/poison);failed 进 HashSet 避免 O(n²)。
                            let failed: std::collections::HashSet<String> = failed.into_iter().collect();
                            let ok: Vec<String> = ids.into_iter().filter(|i| !failed.contains(i)).collect();
                            ack(&shared, &ok).await;
                        }
                    }
                    // 空轮(无新消息)→ 冷流 sleep,不忙等(NOBLOCK 轮询语义)
                    Ok(_) => tokio::time::sleep(Duration::from_millis(shared.cfg.poll_idle_ms.max(1))).await,
                    Err(e) => {
                        tracing::warn!(err = %e, "PROXY XREADGROUP 失败,短暂退避");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}

// Reads new proxy entries for one consumer.
///
/// # 参数
/// 业务作用：- `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `consumer`: Redis Stream consumer 名称。
async fn read_new(shared: &ProxyShared, consumer: &str) -> Result<ParsedEntries> {
    // **NOBLOCK**(无 BLOCK 参数):立即返回(空则空数组)——不在共享多路复用连接上阻塞。
    let v: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&shared.group)
        .arg(consumer)
        .arg("COUNT")
        .arg(shared.cfg.batch_size)
        .arg("STREAMS")
        .arg(&shared.stream)
        .arg(">")
        .query_async(&mut shared.client.conn())
        .await?;
    // 形态:RESP2 `[[stream, [entries]]]` / nil(空);RESP3 `{stream: [entries]}`(Map)。
    let empty = || ParsedEntries {
        ok: Vec::new(),
        bad: Vec::new(),
    };
    match &v {
        redis::Value::Nil => Ok(empty()),
        redis::Value::Array(streams) | redis::Value::Set(streams) => {
            if let Some(redis::Value::Array(pair) | redis::Value::Set(pair)) = streams.first() {
                if let Some(entries) = pair.get(1) {
                    return Ok(parse_entries(entries));
                }
            }
            Ok(empty())
        }
        //RESP3 HELLO 3 下 XREADGROUP 返 Map{stream:entries}——取首个 stream 的 entries。
        redis::Value::Map(pairs) => Ok(pairs
            .first()
            .map(|(_, entries)| parse_entries(entries))
            .unwrap_or_else(empty)),
        _ => {
            tracing::warn!("PROXY XREADGROUP 顶层形态异常(疑似 RESP3 未适配),本轮空轮");
            Ok(empty())
        }
    }
}

/// 业务作用：回收循环:周期 XAUTOCLAIM 回收 idle pending(崩溃 consumer 的 + 本节点失败的)→ 查重投次数 →
/// 超 max_redeliver 的 poison(Drop/Dlq),其余重投(再 dispatch);ACK 成功子集。
///
/// # 参数
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `consumer`: Redis Stream consumer 名称。
/// - `cancel`: 后台任务使用的取消信号。
async fn reclaim_loop(shared: Arc<ProxyShared>, consumer: String, cancel: CancellationToken) {
    let period = Duration::from_millis((shared.cfg.reclaim_min_idle_ms / 2).clamp(200, 10_000));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                if let Err(e) = reclaim_once(&shared, &consumer).await {
                    tracing::warn!(err = %e, "PROXY reclaim 失败,下轮重试");
                }
            }
        }
    }
}

// Reclaims stale proxy entries for retry.
///
/// # 参数
/// 业务作用：- `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `consumer`: Redis Stream consumer 名称。
async fn reclaim_once(shared: &ProxyShared, consumer: &str) -> Result<()> {
    // XAUTOCLAIM stream group consumer min-idle 0 COUNT n —— 回收闲置 pending 到本回收 consumer
    let v: redis::Value = redis::cmd("XAUTOCLAIM")
        .arg(&shared.stream)
        .arg(&shared.group)
        .arg(consumer)
        .arg(shared.cfg.reclaim_min_idle_ms)
        .arg("0")
        .arg("COUNT")
        .arg(shared.cfg.batch_size)
        .query_async(&mut shared.client.conn())
        .await?;
    // 形态:[cursor, [entries], [deleted-ids]](RESP2/3 同为序列;接受 Array|Set)
    let (redis::Value::Array(parts) | redis::Value::Set(parts)) = &v else {
        if !matches!(v, redis::Value::Nil) {
            tracing::warn!("PROXY XAUTOCLAIM 形态异常(疑似 RESP3 未适配),本轮跳过");
        }
        return Ok(());
    };
    let Some(entries) = parts.get(1) else {
        return Ok(());
    };
    let parsed = parse_entries(entries);
    // 坏 entry(无 data / 非 Envelope JSON,可能来自崩溃 consumer 的 PEL)立即处置,不卡 PEL。
    if !parsed.bad.is_empty() {
        dispose_bad(shared, &parsed.bad).await;
    }
    let batch = parsed.ok;
    if batch.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = batch.iter().map(|(id, _)| id.clone()).collect();
    // 查每条重投次数(XPENDING),分流 poison vs 重投
    let counts = pending_counts(shared, &ids).await?;
    let mut to_dispatch = Vec::new();
    let mut poison_ids = Vec::new();
    for (id, env) in batch {
        //XPENDING 概要未返回该 id 的 count(协议异常/竞态)→ 缺省 **1** 重投。
        // 注:**故意取安全方向**(重投,非判毒)——判毒=Drop/Dlq 是销毁性的,若误把正常消息判毒会**丢消息**;
        // 缺省 1 最多让真毒多重投几次(无丢失,且重投上限仍兜底)。缺省发生时记 warn 暴露不可见性。
        let n = match counts.get(&id).copied() {
            Some(n) => n,
            None => {
                tracing::warn!(
                    id,
                    "PROXY XPENDING 未返回该 id 的重投计数(协议异常/竞态),保守按 1 重投"
                );
                1
            }
        };
        if n > shared.cfg.max_redeliver {
            poison_ids.push(id);
        } else {
            to_dispatch.push((id, env));
        }
    }
    if !poison_ids.is_empty() {
        handle_poison(shared, &poison_ids).await;
    }
    if !to_dispatch.is_empty() {
        let d_ids: Vec<String> = to_dispatch.iter().map(|(id, _)| id.clone()).collect();
        let failed = dispatch(shared, to_dispatch).await;
        let failed: std::collections::HashSet<String> = failed.into_iter().collect();
        let ok: Vec<String> = d_ids.into_iter().filter(|i| !failed.contains(i)).collect();
        ack(shared, &ok).await;
    }
    Ok(())
}

/// 业务作用：取本批每个 id 的 delivery count(重投次数),用于 poison 判定。
///
///**逐 id 精确查询**,不再用全局近似 `XPENDING stream group IDLE 0 - + count`——
/// 后者按 entry-ID 升序取**全局最早的 N 条**,当 group PEL 里存在比本批更早的积压时(典型:某 consumer
/// 崩溃留大量 pending,reclaim 按 batch_size 分批认领),返回窗口与本批 ∩=∅ → 本批 count 全缺失 →
/// 缺省按 1 重投 → **非最早批次的真毒永远 n≤max_redeliver,Drop/Dlq 永不触发**(毒消息无限重投)。
/// 改为每 id `XPENDING stream group {id} {id} 1`,批量进一条 pipeline 一次 RTT(同 stream 单 key,
/// cluster 下同 slot 不 CROSSSLOT);结果按入队顺序与 ids 一一对应,取第 4 列 deliveries。
///
/// # 参数
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `ids`: Redis stream entry id 或业务记录 id 列表。
async fn pending_counts(shared: &ProxyShared, ids: &[String]) -> Result<HashMap<String, u32>> {
    let mut out = HashMap::new();
    if ids.is_empty() {
        return Ok(out);
    }
    let mut pipe = redis::pipe();
    for id in ids {
        pipe.cmd("XPENDING")
            .arg(&shared.stream)
            .arg(&shared.group)
            .arg(id)
            .arg(id)
            .arg(1);
    }
    let mut conn = shared.client.conn();
    let results: Vec<redis::Value> =
        redis::aio::ConnectionLike::req_packed_commands(&mut conn, &pipe, 0, ids.len()).await?;
    // 每个 id 的结果 = [[id, consumer, idle, deliveries]](0 或 1 行);按位置与 ids 对齐。
    for (id, res) in ids.iter().zip(results) {
        let (redis::Value::Array(rows) | redis::Value::Set(rows)) = res else {
            continue;
        };
        if let Some(redis::Value::Array(cols) | redis::Value::Set(cols)) = rows.into_iter().next() {
            if let Some(redis::Value::Int(n)) = cols.get(3) {
                out.insert(id.clone(), (*n).max(0) as u32);
            }
        }
    }
    Ok(out)
}

/// 业务作用：毒消息处置。
/// ⚠ Dlq 路径的 XRANGE→XADD→XACK 三步**非事务**：XADD 转存成功但 XACK 失败时，
/// 该消息会 **DLQ 重复 + 源仍重投**——这是 PROXY at-least-once 语义的固有取舍(DLQ 消费方需幂等/去重),
/// 可接受;若需精确一次转存可改 Lua 原子化(留 backlog)。
///
/// # 参数
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `ids`: Redis stream entry id 或业务记录 id 列表。
async fn handle_poison(shared: &ProxyShared, ids: &[String]) {
    match shared.cfg.poison {
        ProxyPoison::Drop => {
            tracing::error!(?ids, "PROXY 毒消息超重投上限,按 Drop 策略 XACK 丢弃");
            ack(shared, ids).await;
        }
        ProxyPoison::Dlq => {
            // 转存 payload 到 {stream}:dlq 后 XACK 源(best-effort)。**批量化**——
            // 循环内逐条 XRANGE+XADD 会放大往返并制造半完成窗口，因此先用一条 pipeline 批量读取全部原 entry，
            // ② 一条 pipeline 批量 XADD 写 DLQ(同 stream/同 dlq 单 key,cluster 同 slot)。
            let dlq = format!("{}:dlq", shared.stream);
            let mut conn = shared.client.conn();
            // ① 批量 XRANGE
            let mut rpipe = redis::pipe();
            for id in ids {
                rpipe.cmd("XRANGE").arg(&shared.stream).arg(id).arg(id);
            }
            let ranges: Vec<redis::Value> = match redis::aio::ConnectionLike::req_packed_commands(
                &mut conn,
                &rpipe,
                0,
                ids.len(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    // 整批读失败:**不 ACK**(源留 PEL 下轮重投),不静默丢。
                    tracing::warn!(?ids, err = %e, "PROXY DLQ 批量 XRANGE 失败,本批不 ACK 待重投");
                    return;
                }
            };
            // ② 解析 + 序列化,收集 DLQ body(序列化失败跳过该条,不写空 body)
            let mut bodies: Vec<Vec<u8>> = Vec::new();
            for r in &ranges {
                // 仅转存可解析的 Envelope;坏 entry 在 dispose_bad 已单独处置(此处只处理 max_redeliver 判毒的 id)。
                for (_eid, env) in parse_entries(r).ok {
                    match serde_json::to_vec(&env) {
                        Ok(b) => bodies.push(b),
                        Err(e) => {
                            tracing::warn!(err = %e, "PROXY DLQ 序列化失败,跳过该条(不写空 body)")
                        }
                    }
                }
            }
            //**数量一致性 fail-closed**——若可转存 body 少于 ids(源 entry 被外部 XDEL/XTRIM、
            // 或序列化失败),不能"只写部分 DLQ 又 XACK 全批"(会丢审计 body)。整批不 ACK,留 PEL 待下轮;
            // 被外部删的 entry 由下轮 XAUTOCLAIM **自动 reap**(返回在 deleted-ids,Redis 自清 PEL),不会永久卡。
            if bodies.len() != ids.len() {
                tracing::warn!(
                    ?ids,
                    got = bodies.len(),
                    want = ids.len(),
                    "PROXY DLQ body 数量与 ids 不一致(源 entry 可能被外部删/序列化失败),本批不 ACK"
                );
                return;
            }
            // ③ 批量 XADD 到 DLQ(此时 bodies 非空且数量 == ids)
            let mut apipe = redis::pipe();
            for b in &bodies {
                apipe
                    .cmd("XADD")
                    .arg(&dlq)
                    .arg("*")
                    .arg(DATA_FIELD)
                    .arg(b.as_slice());
            }
            //**逐槽位校验** XADD 结果——server error(WRONGTYPE 等)逐槽位内联在 Ok 里,
            // 不检查就会"DLQ 没写进去却 XACK 源 = 丢消息"。任一失败 → 不 ACK,留 PEL 下轮重投。
            match redis::aio::ConnectionLike::req_packed_commands(
                &mut conn,
                &apipe,
                0,
                bodies.len(),
            )
            .await
            {
                Ok(v) if all_pipeline_replies_ok(&v, bodies.len()) => {}
                Ok(v) => {
                    tracing::warn!(?ids, replies = ?v, "PROXY DLQ XADD 返回 server error/数量异常,本批不 ACK 待重投");
                    return;
                }
                Err(e) => {
                    tracing::warn!(?ids, err = %e, "PROXY DLQ XADD 传输失败,本批不 ACK 待重投");
                    return;
                }
            }
            tracing::error!(?ids, dlq = %dlq, "PROXY 毒消息超上限,已转 DLQ 并 XACK 源");
            ack(shared, ids).await;
        }
    }
}

/// 业务作用：pipeline 批量回复是否**全部成功**。本 redis crate 把单命令 server error
/// **逐槽位内联**成 `Value::ServerError`(不聚合成 outer `Err`,见 `pipeline.rs` 注释),故 DLQ XADD
/// 这类写入**必须逐槽位检查**——否则 `XADD {dlq}` 撞 WRONGTYPE(key 被占成 string 等)会被当成功、随后
/// XACK 源 → **消息既没进 DLQ 又被确认 = 永久丢失**。返回 true 仅当数量相符且无任一 `ServerError`。
///
/// # 参数
/// - `values`: 待校验、写入或比较的值列表。
/// - `expected`: 协议或状态机期望值。
fn all_pipeline_replies_ok(values: &[redis::Value], expected: usize) -> bool {
    values.len() == expected
        && !values
            .iter()
            .any(|v| matches!(v, redis::Value::ServerError(_)))
}

/// 业务作用：标准 base64 编码(RFC 4648,带 `=` padding)。**dep-free**:仅 DLQ raw 字段做 lossless 还原用,
/// 量小,无需引第三方 crate。
///
/// # 参数
/// - `bytes`: 原始字节切片。
fn base64_encode(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(T[b0 >> 2] as char);
        out.push(T[((b0 & 0b11) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 {
            T[((b1 & 0b1111) << 2) | (b2 >> 6)] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[b2 & 0b111111] as char
        } else {
            '='
        });
    }
    out
}

/// 业务作用：处置**不可解析**的坏 entry（无 `data` 字段或 `data` 不是 Envelope JSON）：
/// 此前坏 entry 被 parser 静默跳过 → 永久卡 PEL。坏 entry 确定性不可解析(重读必再失败),故
/// **立即处置不重投**:`Drop` → XACK 丢弃;`Dlq` → 把 `{reason, stream, group, id, raw 字段}` 转存
/// `{stream}:dlq` 后 XACK 源(原始字段 lossless 保留供运维复盘)。Dlq 转存失败 → 不 ACK,留 PEL 待下轮重试(不丢)。
///
/// ⚠ **at-least-once 转存**(同 [`handle_poison`] 的 DLQ,本轮):XADD→XACK 非事务,XADD 成功
/// 但 XACK 失败时该 entry 下轮会**再次写 DLQ**(DLQ 重复)。DLQ body 带 `stream/group/id/reason` 足够做
/// 幂等去重 key。若需精确一次转存须 Lua 化 XADD+XACK(cluster 下还要 source 与 dlq 同 slot,留 backlog)。
///
/// # 参数
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `bad`: 检测到异常的 Redis 连接或节点。
async fn dispose_bad(shared: &ProxyShared, bad: &[BadEntry]) {
    if bad.is_empty() {
        return;
    }
    let ids: Vec<String> = bad.iter().map(|b| b.id.clone()).collect();
    match shared.cfg.poison {
        ProxyPoison::Drop => {
            tracing::error!(
                ?ids,
                "PROXY 不可解析 entry(无 data/非 Envelope),按 Drop 策略 XACK 丢弃"
            );
        }
        ProxyPoison::Dlq => {
            let dlq = format!("{}:dlq", shared.stream);
            let mut conn = shared.client.conn();
            let mut apipe = redis::pipe();
            for b in bad {
                //**lossless** raw——pairs 数组保序/保重复 field;`*_utf8`(lossy)排障友好,
                // `*_b64`(base64)是权威值(还原二进制/非 UTF-8 字节)。
                let raw: Vec<serde_json::Value> = b
                    .raw
                    .iter()
                    .map(|(k, v)| {
                        serde_json::json!({
                            "field_utf8": String::from_utf8_lossy(k),
                            "field_b64": base64_encode(k),
                            "value_utf8": String::from_utf8_lossy(v),
                            "value_b64": base64_encode(v),
                        })
                    })
                    .collect();
                let body = serde_json::to_vec(&serde_json::json!({
                    "_proxy_dlq": "unparseable",
                    "reason": b.reason,
                    "stream": shared.stream,
                    "group": shared.group,
                    "id": b.id,
                    "raw": raw,
                }))
                .unwrap_or_default();
                apipe
                    .cmd("XADD")
                    .arg(&dlq)
                    .arg("*")
                    .arg(DATA_FIELD)
                    .arg(body);
            }
            //**逐槽位校验**——server error(WRONGTYPE 等)逐槽位内联在 Ok 里,outer Err
            // 只覆盖传输失败;不检查就会"DLQ 没写进去却 XACK 源 = 丢消息"。任一失败 → 不 ACK,留 PEL 下轮重试。
            match redis::aio::ConnectionLike::req_packed_commands(&mut conn, &apipe, 0, bad.len())
                .await
            {
                Ok(v) if all_pipeline_replies_ok(&v, bad.len()) => {}
                Ok(v) => {
                    tracing::warn!(?ids, replies = ?v, "PROXY 不可解析 entry 转 DLQ 返回 server error/数量异常,本批不 ACK 待重试");
                    return;
                }
                Err(e) => {
                    tracing::warn!(?ids, err = %e, "PROXY 不可解析 entry 转 DLQ 传输失败,本批不 ACK 待重试");
                    return;
                }
            }
            tracing::error!(?ids, dlq = %dlq, "PROXY 不可解析 entry 已转 DLQ 并 XACK 源");
        }
    }
    ack(shared, &ids).await;
}
