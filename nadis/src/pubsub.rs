// ============================================================================
// Redis Pub/Sub 的发布、订阅与重连恢复。
//
// · publish:经 `Conn`(单节点/cluster 通用);classic Pub/Sub 在 cluster 下跨节点传播。
// · subscribe:派生**专用** Pub/Sub 连接
//   返回 `Subscription`,`next_message()` 拉取消息(显式拉模型,业务自行 loop/spawn)。
// 设计取向:Rust 用**显式订阅 + 拉取**,不做注解魔法/自动分发。
// ============================================================================

use futures::StreamExt;
use redis::{AsyncCommands, FromRedisValue, ToSingleRedisArg};

use crate::client::RedisClient;
use crate::error::Result;

/// 业务作用：核对 PUBLISH 报告的订阅计数，避免通用数值转换把负数或其它回复类型当作广播数量。
///
/// 参数说明：
/// - `reply`：本次 PUBLISH 命令的原始回复。
///
/// 返回：仅非负 Redis Integer 成功，其它类型或负数返回固定解析错误；不包含原始正文，也不证明业务送达或远端尚未广播。
pub(crate) fn check_publish_count_reply(reply: &redis::Value) -> Result<()> {
    if matches!(reply, redis::Value::Int(count) if *count >= 0) {
        Ok(())
    } else {
        Err(crate::error::NasaRedisError::Parsing(
            "PUBLISH 回复不是有效的订阅计数".into(),
        ))
    }
}

/// 收到的一条 Pub/Sub 消息。
#[derive(Debug, Clone)]
pub struct Message {
    /// 收到消息的 Redis channel。
    pub channel: String,
    /// 消息原始 payload 字节。
    pub payload: Vec<u8>,
}

impl Message {
    /// 业务作用：按调用方指定的 Redis 值类型解释当前消息正文，不改变订阅状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：复制原始 payload 后交给 T 的 FromRedisValue 转换，成功返回 T，转换失败向上传播；不解包 Stream 信封或确认业务消费。
    pub fn parse<T: FromRedisValue>(&self) -> Result<T> {
        Ok(T::from_redis_value(redis::Value::BulkString(
            self.payload.clone(),
        ))?)
    }

    /// 业务作用：为展示或宽松文本处理提供消息正文的有损 UTF-8 视图。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：合法 UTF-8 借用原字节，非法编码分配替代字符后的文本；不修改原始 payload，不适合作为精确字节身份校验。
    pub fn as_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.payload)
    }
}

/// 订阅句柄:持有专用 Pub/Sub 连接;`next_message()` 等待下一条消息。
/// Drop 释放本地连接责任，远端断开可迟于本地析构。多频道可通过 subscribe 追加。
///
///classic Pub/Sub **专用单连接无自动重连**——断线后 `next_message` 返 None
/// 且订阅全部丢失。本类型**追踪订阅集**(channels/patterns)并提供 `reconnect()`:`next_message`
/// 返 None 时调用方可 `reconnect()` 重建连接 + 重放订阅,继续拉取。订阅变更等待被取消或报错后，
/// 当前连接的状态不再可信，后续拉取和变更停止，必须成功重连后才能继续。设计取向是**显式拉**
/// (不内置后台重连循环——那会与拉模型冲突,也会吞掉调用方对断线的可观测);积木齐备,策略归调用方。
/// 重连只重放订阅命令，不补回断线窗口中已发布的消息；成功不代表业务送达。
pub struct Subscription {
    ps: redis::aio::PubSub,
    /// 重连用:原始 client(clone 廉价,仅配置)。
    client: redis::Client,
    /// 最后确认的频道与模式，用于重连恢复及 `subscribed_*` 查询，不包含结果未知的变更。
    channels: Vec<String>,
    patterns: Vec<String>,
    /// 变更命令一旦开始等待就撤销连接可信状态，只有确认成功或完整重连才能解除。
    needs_reconnect: bool,
}

impl Subscription {
    /// 业务作用：在当前专用连接追加频道订阅，并在命令完成后记录重连所需的频道。
    ///
    /// 参数说明：
    /// - `channel`：原样发送的 classic Pub/Sub 频道名。
    ///
    /// 返回：底层订阅成功后将频道加入本地去重列表并恢复可信状态；错误或等待取消保留最后确认列表，停止拉取及后续变更，须完整重连后继续。
    /// 已要求重连时直接拒绝发送；即使底层返回明确错误也保守要求重连，不能据此证明远端未动作。
    pub async fn subscribe(&mut self, channel: &str) -> Result<()> {
        // 上次变更结果未知时，旧连接可能已经偏离本地记录，不能叠加新的状态变更。
        if self.needs_reconnect {
            return Err(crate::error::NasaRedisError::Config(
                "Pub/Sub 订阅状态未确认，请先成功 reconnect".into(),
            ));
        }
        // 先撤销可信状态再等待远端结果；Future 被丢弃也必须留下重连门禁。
        self.needs_reconnect = true;
        self.ps.subscribe(channel).await?;
        if !self.channels.iter().any(|c| c == channel) {
            self.channels.push(channel.to_string());
        }
        self.needs_reconnect = false;
        Ok(())
    }

    /// 业务作用：请求当前连接取消一个频道，并在命令完成后撤销该频道的本地重连记录。
    ///
    /// 参数说明：
    /// - `channel`：需要退订的 classic Pub/Sub 频道名。
    ///
    /// 返回：底层退订成功后移除全部同名本地记录并恢复可信状态；错误或等待取消保留最后确认列表并要求重连。
    /// 已要求重连时直接拒绝发送；不保证远端仍保持订阅，成功退订后不再交付排队中的该频道消息。
    pub async fn unsubscribe(&mut self, channel: &str) -> Result<()> {
        // 上次变更结果未知时，旧连接可能已经偏离本地记录，不能叠加新的状态变更。
        if self.needs_reconnect {
            return Err(crate::error::NasaRedisError::Config(
                "Pub/Sub 订阅状态未确认，请先成功 reconnect".into(),
            ));
        }
        // 先撤销可信状态再等待远端结果；Future 被丢弃也必须留下重连门禁。
        self.needs_reconnect = true;
        self.ps.unsubscribe(channel).await?;
        self.channels.retain(|c| c != channel);
        self.needs_reconnect = false;
        Ok(())
    }

    /// 业务作用：在当前连接追加模式订阅，并记录成功完成的模式供重连时重放。
    ///
    /// 参数说明：
    /// - `pattern`：原样发送给 Redis 的 glob 模式，消息频道仍是实际命中的频道。
    ///
    /// 返回：底层订阅成功后把模式加入本地去重列表并恢复可信状态；错误或等待取消保留最后确认列表并要求重连。
    /// 已要求重连时直接拒绝发送；等待取消不撤回底层已排队的命令，不能继续按旧列表交付消息。
    pub async fn psubscribe(&mut self, pattern: &str) -> Result<()> {
        // 上次变更结果未知时，旧连接可能已经偏离本地记录，不能叠加新的状态变更。
        if self.needs_reconnect {
            return Err(crate::error::NasaRedisError::Config(
                "Pub/Sub 订阅状态未确认，请先成功 reconnect".into(),
            ));
        }
        // 先撤销可信状态再等待远端结果；Future 被丢弃也必须留下重连门禁。
        self.needs_reconnect = true;
        self.ps.psubscribe(pattern).await?;
        if !self.patterns.iter().any(|p| p == pattern) {
            self.patterns.push(pattern.to_string());
        }
        self.needs_reconnect = false;
        Ok(())
    }

    /// 业务作用：请求取消一个模式订阅，并在完成后移除其本地重连记录。
    ///
    /// 参数说明：
    /// - `pattern`：需要退订的 Redis glob 模式。
    ///
    /// 返回：底层退订成功后移除全部同名模式记录并恢复可信状态；错误或等待取消保留最后确认列表并要求重连。
    /// 已要求重连时直接拒绝发送；成功退订后不再交付该模式已排队的消息，远端结果未知时停止当前连接的使用。
    pub async fn punsubscribe(&mut self, pattern: &str) -> Result<()> {
        // 上次变更结果未知时，旧连接可能已经偏离本地记录，不能叠加新的状态变更。
        if self.needs_reconnect {
            return Err(crate::error::NasaRedisError::Config(
                "Pub/Sub 订阅状态未确认，请先成功 reconnect".into(),
            ));
        }
        // 先撤销可信状态再等待远端结果；Future 被丢弃也必须留下重连门禁。
        self.needs_reconnect = true;
        self.ps.punsubscribe(pattern).await?;
        self.patterns.retain(|p| p != pattern);
        self.needs_reconnect = false;
        Ok(())
    }

    /// 业务作用：向调用方展示当前句柄保存的频道重连记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地频道列表的借用，初始构造时的重复输入可能保留；不查询远端，不能据此证明断线或命令取消后的实际订阅状态。
    pub fn subscribed_channels(&self) -> &[String] {
        &self.channels
    }

    /// 业务作用：提供当前句柄保存的模式订阅重连记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地模式列表的借用，初始构造时的重复输入可能保留；不查询远端或证明当前连接健康。
    pub fn subscribed_patterns(&self) -> &[String] {
        &self.patterns
    }

    /// 业务作用：在默认五秒总预算内新建专用连接并恢复本地记录的频道和模式订阅。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部重放成功后替换旧连接并解除重连门禁；连接、订阅错误或超时保留旧连接及其可信状态，调用方负责后续退避；不补回断线期间的消息。
    pub async fn reconnect(&mut self) -> Result<()> {
        self.reconnect_timeout(std::time::Duration::from_secs(5))
            .await
    }

    /// 业务作用：以显式总预算完成连接重建和全部订阅重放，避免把半配置的新连接交给消息拉取。
    ///
    /// 参数说明：
    /// - `timeout`：建连与依次重放频道、模式共享的总时间预算，不得超过运行时期限上限。
    ///
    /// 返回：全部准备成功才替换旧连接并解除重连门禁；预算过大、建连或订阅失败、超时返回错误，取消等待也保留旧连接、最后确认列表和原可信状态。
    /// 零预算不提供额外等待时间；恢复订阅不恢复断线窗口中错过的消息。
    pub async fn reconnect_timeout(&mut self, timeout: std::time::Duration) -> Result<()> {
        if timeout > std::time::Duration::from_millis(crate::config::MAX_REDIS_RUNTIME_DURATION_MS)
        {
            return Err(crate::error::NasaRedisError::Config(format!(
                "pubsub reconnect timeout 超过运行时上限 {}ms",
                crate::config::MAX_REDIS_RUNTIME_DURATION_MS
            )));
        }
        // 先在独立新连接上完成重放；任何中途错误或取消都不能覆盖仍由当前句柄持有的旧连接。
        let build = async {
            let mut ps = self.client.get_async_pubsub().await?;
            for ch in &self.channels {
                ps.subscribe(ch).await?;
            }
            for p in &self.patterns {
                ps.psubscribe(p).await?;
            }
            Ok::<_, crate::error::NasaRedisError>(ps)
        };
        match tokio::time::timeout(timeout, build).await {
            Ok(Ok(ps)) => {
                // 全部频道和模式都已完成准备，此时才将新连接发布给后续消息拉取。
                self.ps = ps;
                self.needs_reconnect = false;
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(crate::error::NasaRedisError::Config(format!(
                "pubsub reconnect 超时({}ms)——网络可能仍未恢复,请退避重试",
                timeout.as_millis()
            ))),
        }
    }

    /// 业务作用：从专用 Pub/Sub 流拉取下一条消息，由调用方决定消费循环和断线恢复策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：来源仍在本地已确认订阅集合内、频道为 UTF-8 批量字符串且正文为批量字符串时返回消息；空正文和任意二进制正文均保留。
    /// 订阅变更结果未知、流结束或消息字段无法确认时返回 None；重连门禁开启时不读取旧连接，不自动重连或确认消费。
    /// 模式匹配由 Redis 执行，此处只核对模式身份，不重验底层已丢弃的帧信息。
    pub async fn next_message(&mut self) -> Option<Message> {
        // 未确认的订阅变更可能已经在远端生效，不能按旧记录继续向业务发布消息。
        if self.needs_reconnect {
            return None;
        }
        let msg = self.ps.on_message().next().await?;
        let parsed = (|| {
            let redis::Value::BulkString(channel) = msg.get_channel().ok()? else {
                return None;
            };
            let channel = String::from_utf8(channel).ok()?;
            // 频道和模式是路由身份，不能把编码失败的替代值或不在已确认订阅中的来源交给业务。
            if msg.from_pattern() {
                let redis::Value::BulkString(pattern) = msg.get_pattern().ok()? else {
                    return None;
                };
                let pattern = String::from_utf8(pattern).ok()?;
                if !self.patterns.contains(&pattern) {
                    return None;
                }
            } else if !self.channels.contains(&channel) {
                return None;
            }
            // 正文允许任意字节；只有协议中的批量字符串才具有这份字节身份，不能用空值掩盖错误类型。
            let redis::Value::BulkString(payload) = msg.get_payload().ok()? else {
                return None;
            };
            Some(Message {
                channel,
                payload,
            })
        })();
        if parsed.is_none() {
            tracing::warn!("Pub/Sub 消息字段或订阅来源无法确认，停止当前拉取");
        }
        parsed
    }
}

impl RedisClient {
    /// 业务作用：通过共享命令连接向 classic Pub/Sub 频道发布消息，不持久化正文或等待业务确认。
    ///
    /// 参数说明：
    /// - `channel`：目标 Redis 频道名。
    /// - `msg`：按单个 Redis 参数编码的消息正文。
    ///
    /// 返回：沿用共享命令预算，仅非负 Integer 回复返回被路由节点报告的订阅计数；命令错误向上传播，异常计数或类型返回固定解析错误。
    /// Cluster 下该计数不是全局订阅者数或送达数；错误也不能证明消息没有广播。名称使用 Rust raw 标识符，将 publish 保留给 Stream。
    pub async fn r#pub<V: ToSingleRedisArg + Send + Sync>(
        &self,
        channel: &str,
        msg: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        let value: redis::Value = self.timed(c.publish(channel, msg)).await?;
        check_publish_count_reply(&value)?;
        Ok(u64::from_redis_value(value)?)
    }

    /// 业务作用：新建 classic Pub/Sub 专用连接并依次完成初始频道订阅，返回由调用方拉取消息的句柄。
    ///
    /// 参数说明：
    /// - `channels`：初始频道列表，允许为空；输入顺序和重复项保留为本地重连记录。
    ///
    /// 返回：全部订阅完成后返回句柄，建连或订阅错误向上传播，取消等待释放尚未交付的连接责任。
    /// 不创建后台业务分发循环，也不在此设置整体准备期限；后续追加订阅和重连由调用方显式发起。
    pub async fn sub(&self, channels: &[&str]) -> Result<Subscription> {
        let mut ps = self.raw_client().get_async_pubsub().await?;
        for ch in channels {
            ps.subscribe(*ch).await?;
        }
        Ok(Subscription {
            ps,
            needs_reconnect: false,
            client: self.raw_client().clone(),
            channels: channels.iter().map(|s| s.to_string()).collect(),
            patterns: Vec::new(),
        })
    }

    /// 业务作用：新建专用连接并依次完成初始 glob 模式订阅，供调用方显式拉取匹配频道消息。
    ///
    /// 参数说明：
    /// - `patterns`：初始 Redis glob 模式列表，允许为空；顺序和重复项保留为重连记录。
    ///
    /// 返回：全部订阅完成后返回句柄，建连或订阅错误向上传播，取消等待释放未交付的连接责任。
    /// 不设置整体准备期限，不自动恢复断线或补发断线期间的消息。
    pub async fn psubscribe(&self, patterns: &[&str]) -> Result<Subscription> {
        let mut ps = self.raw_client().get_async_pubsub().await?;
        for p in patterns {
            ps.psubscribe(*p).await?;
        }
        Ok(Subscription {
            ps,
            needs_reconnect: false,
            client: self.raw_client().clone(),
            channels: Vec::new(),
            patterns: patterns.iter().map(|s| s.to_string()).collect(),
        })
    }
}
