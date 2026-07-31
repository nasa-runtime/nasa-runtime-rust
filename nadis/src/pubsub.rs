// ============================================================================
// src/pubsub.rs —— Pub/Sub 消息(F1;对照 原实现 RedisProxy subscribe/pub/publish + Subscriber)。
//
// · publish:经 `Conn`(单节点/cluster 通用);classic Pub/Sub 在 cluster 下跨节点传播。
// · subscribe:派生**专用** Pub/Sub 连接
//   返回 `Subscription`,`next_message()` 拉取消息(显式拉模型,业务自行 loop/spawn)。
// 设计取向(对照 原实现 @Subscribe 注解驱动):Rust 用**显式订阅 + 拉取**,不做注解魔法/自动分发。
// ============================================================================

use futures::StreamExt;
use redis::{AsyncCommands, FromRedisValue, ToSingleRedisArg};

use crate::client::RedisClient;
use crate::error::Result;

/// 收到的一条 Pub/Sub 消息。
#[derive(Debug, Clone)]
pub struct Message {
    /// 收到消息的 Redis channel。
    pub channel: String,
    /// 消息原始 payload 字节。
    pub payload: Vec<u8>,
}

impl Message {
    /// 按类型解码 payload(String / `Json<T>` / 整数等,经 redis `FromRedisValue`)。
    pub fn parse<T: FromRedisValue>(&self) -> Result<T> {
        Ok(T::from_redis_value(redis::Value::BulkString(
            self.payload.clone(),
        ))?)
    }

    /// payload 当 UTF-8 字符串(lossy)。
    pub fn as_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.payload)
    }
}

/// 订阅句柄:持有专用 Pub/Sub 连接;`next_message()` 等待下一条消息。
/// Drop 即退订(连接释放)。多频道:`subscribe` 可多次追加。
///
///classic Pub/Sub **专用单连接无自动重连**——断线后 `next_message` 返 None
/// 且订阅全部丢失。本类型**追踪订阅集**(channels/patterns)并提供 `reconnect()`:`next_message`
/// 返 None(疑似断线)时调用方可 `reconnect()` 重建连接 + 重放订阅,继续拉取。设计取向是**显式拉**
/// (不内置后台重连循环——那会与拉模型冲突,也会吞掉调用方对断线的可观测);积木齐备,策略归调用方。
pub struct Subscription {
    ps: redis::aio::PubSub,
    /// 重连用:原始 client(clone 廉价,仅配置)。
    client: redis::Client,
    /// 已订阅频道 / 模式(reconnect 重放 + `subscribed_*` 自省;对照 原实现 getSubscribedTopics)。
    channels: Vec<String>,
    patterns: Vec<String>,
}

impl Subscription {
    /// 追加订阅一个频道。
    ///
    /// # 参数
    /// - `channel`: 要追加订阅的 classic Pub/Sub 频道名。
    pub async fn subscribe(&mut self, channel: &str) -> Result<()> {
        self.ps.subscribe(channel).await?;
        if !self.channels.iter().any(|c| c == channel) {
            self.channels.push(channel.to_string());
        }
        Ok(())
    }

    /// 退订一个频道。
    ///
    /// # 参数
    /// - `channel`: 要退订的 classic Pub/Sub 频道名。
    pub async fn unsubscribe(&mut self, channel: &str) -> Result<()> {
        self.ps.unsubscribe(channel).await?;
        self.channels.retain(|c| c != channel);
        Ok(())
    }

    /// 追加**模式订阅**(glob:`news.*` / `user.?` 等;消息的 `channel` 是实际命中的频道名)。
    ///
    /// # 参数
    /// - `pattern`: 要追加订阅的 Redis glob 模式。
    pub async fn psubscribe(&mut self, pattern: &str) -> Result<()> {
        self.ps.psubscribe(pattern).await?;
        if !self.patterns.iter().any(|p| p == pattern) {
            self.patterns.push(pattern.to_string());
        }
        Ok(())
    }

    /// 退订模式。
    ///
    /// # 参数
    /// - `pattern`: 要退订的 Redis glob 模式。
    pub async fn punsubscribe(&mut self, pattern: &str) -> Result<()> {
        self.ps.punsubscribe(pattern).await?;
        self.patterns.retain(|p| p != pattern);
        Ok(())
    }

    /// 当前已订阅频道(对照 原实现 getSubscribedTopics)。
    pub fn subscribed_channels(&self) -> &[String] {
        &self.channels
    }

    /// 当前已订阅模式。
    pub fn subscribed_patterns(&self) -> &[String] {
        &self.patterns
    }

    /// **重建专用 Pub/Sub 连接并重放全部订阅**(断线恢复)。默认 5s 超时(见
    /// `reconnect_timeout`)。典型用法:`while let Some(m) = sub.next_message().await {...}` 退出后疑似断线 →
    /// `sub.reconnect().await?` → 继续循环。重连失败/超时上抛 Err(调用方退避重试)。
    pub async fn reconnect(&mut self) -> Result<()> {
        self.reconnect_timeout(std::time::Duration::from_secs(5))
            .await
    }

    /// 同 `reconnect`,但显式超时(专用连接无 ConnectionManager 兜底,网络真断时
    /// `get_async_pubsub`/`subscribe` 会长 pending → 必须套 timeout,否则调用方循环卡死;对齐 lock 路径纪律)。
    ///
    /// # 参数
    /// - `timeout`: 重建 Pub/Sub 专用连接和重放订阅的总超时时长。
    pub async fn reconnect_timeout(&mut self, timeout: std::time::Duration) -> Result<()> {
        if timeout > std::time::Duration::from_millis(crate::config::MAX_REDIS_RUNTIME_DURATION_MS)
        {
            return Err(crate::error::NasaRedisError::Config(format!(
                "pubsub reconnect timeout 超过运行时上限 {}ms",
                crate::config::MAX_REDIS_RUNTIME_DURATION_MS
            )));
        }
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
                self.ps = ps;
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(crate::error::NasaRedisError::Config(format!(
                "pubsub reconnect 超时({}ms)——网络可能仍未恢复,请退避重试",
                timeout.as_millis()
            ))),
        }
    }

    /// 下一条消息(等待;**连接关闭/断线 → None**——调用方据此 `reconnect()` 重订后继续)。
    pub async fn next_message(&mut self) -> Option<Message> {
        let msg = self.ps.on_message().next().await?;
        Some(Message {
            channel: msg.get_channel_name().to_string(),
            payload: msg.get_payload_bytes().to_vec(),
        })
    }
}

impl RedisClient {
    /// PUBLISH:发布到频道,返回 `PUBLISH` 的 integer reply。
    ///
    /// 命名:classic Pub/Sub 发布叫 **`r#pub`**;
    /// `pub` 是 Rust 关键字,用 raw 标识符),把 `publish` 让给 Redis Stream 发布(见 `stream.rs`)。
    ///
    /// ⚠ **cluster 下返回值不可作为全局订阅者数**:classic Pub/Sub 消息能跨节点传播
    /// 到订阅者,但 `PUBLISH` 的 integer reply 在 redis-rs cluster 路由下只反映**被路由到的那个节点视角**
    /// 的订阅者数(实测 cluster 返回 0、单机返回 1)。**不要**用它判断"是否无人在线 / 全局 delivery 数";
    /// 需要全局在线/送达计数请走 socket/registry 侧在线表或业务 ACK。单机下返回值即本机订阅者数,可用。
    ///
    /// # 参数
    /// - `channel`: 要发布到的 classic Pub/Sub 频道名。
    /// - `msg`: 要发送的 Redis 单参数 payload。
    pub async fn r#pub<V: ToSingleRedisArg + Send + Sync>(
        &self,
        channel: &str,
        msg: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.publish(channel, msg)).await
    }

    /// SUBSCRIBE:订阅一组频道,返回 `Subscription`(派生专用连接)。空切片亦合法(随后 subscribe)。
    ///
    /// 命名:classic Pub/Sub 订阅叫 **`sub`**,把 `subscribe` 让给 Redis Stream 订阅(见 `stream.rs`)。
    ///
    /// # 参数
    /// - `channels`: 初始订阅的频道列表;空列表表示先建连接后续再追加。
    pub async fn sub(&self, channels: &[&str]) -> Result<Subscription> {
        let mut ps = self.raw_client().get_async_pubsub().await?;
        for ch in channels {
            ps.subscribe(*ch).await?;
        }
        Ok(Subscription {
            ps,
            client: self.raw_client().clone(),
            channels: channels.iter().map(|s| s.to_string()).collect(),
            patterns: Vec::new(),
        })
    }

    /// PSUBSCRIBE:**模式订阅**一组 glob 模式,返回 `Subscription`。
    ///
    /// # 参数
    /// - `patterns`: 初始订阅的 Redis glob 模式列表。
    pub async fn psubscribe(&self, patterns: &[&str]) -> Result<Subscription> {
        let mut ps = self.raw_client().get_async_pubsub().await?;
        for p in patterns {
            ps.psubscribe(*p).await?;
        }
        Ok(Subscription {
            ps,
            client: self.raw_client().clone(),
            channels: Vec::new(),
            patterns: patterns.iter().map(|s| s.to_string()).collect(),
        })
    }
}
