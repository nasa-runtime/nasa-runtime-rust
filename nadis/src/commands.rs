//! 类型化 Redis 命令层，统一计时、连接与错误语义。
//!
//! 值参数默认按字节或字符串传输，JSON 由 `Json<T>` 显式选择。TTL 使用类型化状态，
//! 多键写入在集群模式下检查同槽约束。结果未知时不自动重试非幂等写入，
//! 读取接口返回的存在性或数量仅代表当次观察，不能替代原子条件写入。

use std::time::Duration;

use redis::{AsyncCommands, AsyncIter, FromRedisValue, ToRedisArgs, ToSingleRedisArg};

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 业务作用：Duration → 毫秒:**非零时下界 1ms**。亚毫秒 Duration 直接 `as_millis()`
/// 会截断成 0,使 PSETEX 报错 / PEXPIRE 0 删 key;此处保证只要 ttl>0 就不塌成非法 0。
/// `Duration::ZERO` 仍返回 0(调用方显式表达"立即过期/删")。Redis 的相对过期参数按
/// signed 64-bit 毫秒解析；超出该范围必须在本地拒绝，不能经 `as` 转换回绕成负数或较小 TTL。
///
/// 参数说明：
/// - `ttl`：相对存活时间，零值由具体命令解释为立即过期或非法过期时间。
///
/// 返回：截断到毫秒且非零时至少为一的时间值；超出 Redis 有符号整数范围返回配置错误。
pub(crate) fn duration_to_millis_floor1(ttl: Duration) -> Result<u64> {
    let raw = ttl.as_millis();
    if raw > i64::MAX as u128 {
        return Err(NasaRedisError::Config(
            "Redis TTL 超过 signed 64-bit 毫秒范围".into(),
        ));
    }
    let ms = raw as u64;
    if ms == 0 && !ttl.is_zero() {
        Ok(1)
    } else {
        Ok(ms)
    }
}

/// 业务作用：将 TTL 转为 Redis 可表示的非负整数秒数，防止整数转换溢出。
///
/// 参数说明：
/// - `ttl`：待转换时长，小于一秒的余量会截断。
///
/// 返回：不超过有符号 64 位上限的秒数，允许零；超出范围返回配置错误，不执行命令。
pub(crate) fn duration_to_redis_seconds(ttl: Duration) -> Result<u64> {
    let seconds = ttl.as_secs();
    if seconds > i64::MAX as u64 {
        return Err(NasaRedisError::Config(
            "Redis TTL 超过 signed 64-bit 秒范围".into(),
        ));
    }
    Ok(seconds)
}

/// PTTL 的类型化结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyTtl {
    /// key 不存在(PTTL = -2)。
    Missing,
    /// key 存在但无过期(PTTL = -1)。
    Persistent,
    /// 剩余存活时间。
    Expires(Duration),
}

impl RedisClient {
    /// 业务作用：按客户端配置限制单次命令往返的本地等待，不自动重试结果不确定的操作。
    ///
    /// 参数说明：
    /// - `fut`：借用有效连接的单次命令 future，不用于包含多个往返的完整游标周期。
    ///
    /// 返回：完成时保留结果或底层错误；本层超时返回 `ExecutionUnknown`，不证明远端未执行。
    /// `command.timeout_ms` 为零时不增加本层超时，底层连接时间限制仍生效。
    pub(crate) async fn timed<T>(
        &self,
        fut: impl std::future::Future<Output = redis::RedisResult<T>>,
    ) -> Result<T> {
        let t = self.config().command.timeout_ms;
        let raw = if t == 0 {
            fut.await
        } else {
            match tokio::time::timeout(Duration::from_millis(t), fut).await {
                Ok(r) => r,
                Err(_) => {
                    return Err(crate::error::NasaRedisError::ExecutionUnknown(format!(
                        "命令超时({t}ms,可能已执行)"
                    )))
                }
            }
        };
        Ok(raw?)
    }

    // ───────────────────────── string / 通用 ─────────────────────────

    /// 业务作用：读取单个字符串键并按调用方指定类型解码。
    ///
    /// 参数说明：
    /// - `key`：待读取的完整 Redis 键。
    ///
    /// 返回：键存在时返回解码值，缺失时为 `None`；类型不符、解码或命令失败返回错误。
    pub async fn get<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.get(key)).await
    }

    /// 业务作用：无条件写入字符串键，覆盖已有值及其过期设置。
    ///
    /// 参数说明：
    /// - `key`：待写入的完整 Redis 键。
    /// - `val`：编码为单个 Redis 参数的值，JSON 须显式选择 `Json<T>`。
    ///
    /// 返回：收到成功确认后返回 `Ok`；命令失败返回错误，超时不能证明写入未生效。
    pub async fn set<V: ToSingleRedisArg + Send + Sync>(&self, key: &str, val: V) -> Result<()> {
        let mut c = self.conn();
        self.timed::<()>(c.set(key, val)).await
    }

    /// 业务作用：执行带过期时间的 Redis SET 命令。
    ///
    /// 参数说明：
    /// - `key`：待覆盖的完整 Redis 键。
    /// - `val`：编码为单个 Redis 参数的新值。
    /// - `ttl`：从服务端写入时起计算的存活时间，非零值至少保留一毫秒，零值由服务器拒绝。
    ///
    /// 返回：写入值与过期时间均获确认时成功；时间越界或命令失败返回错误。
    pub async fn set_ttl<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
        ttl: Duration,
    ) -> Result<()> {
        let mut c = self.conn();
        //亚毫秒 ttl 截断成 0 会让 PSETEX 报 "invalid expire time"——
        // 非零 Duration 截断到毫秒后至少保留 1ms，不把正存活时间转换成非法零值。
        self.timed::<()>(c.pset_ex(key, val, duration_to_millis_floor1(ttl)?))
            .await
    }

    /// 业务作用：在发送要求同槽的多键命令前拒绝跨槽布局，避免部分调用绕过集群键约束。
    ///
    /// 参数说明：
    /// - `keys`：当前单条命令涉及的全部 Redis 键，按实际字节计算哈希槽。
    ///
    /// 返回：单节点模式、少于两个键或全部同槽时成功；集群跨槽时返回配置错误且不发送命令。
    fn cluster_same_slot_guard(&self, keys: &[&str]) -> Result<()> {
        if !self.is_cluster() || keys.len() < 2 {
            return Ok(());
        }
        let slot0 = crate::keytag::redis_slot(keys[0].as_bytes());
        if let Some(k) = keys[1..]
            .iter()
            .find(|k| crate::keytag::redis_slot(k.as_bytes()) != slot0)
        {
            return Err(crate::error::NasaRedisError::Config(format!(
                "cluster 多 key 命令跨 slot:\"{}\" 与 \"{}\" 不同槽——需同 {{hashtag}} 才能单命令跨 key",
                keys[0], k
            )));
        }
        Ok(())
    }

    /// 业务作用：删除指定键集合，集群模式下先保证单条命令的所有键同槽。
    ///
    /// 参数说明：
    /// - `keys`: 待删除的一组 Redis key；空切片会短路返回 `0`，cluster 下必须同槽。
    ///
    /// 返回：实际删除的键数；跨槽或命令失败返回错误，缺失键不计入数量。
    pub async fn del(&self, keys: &[&str]) -> Result<u64> {
        // 空键集合没有删除对象，直接返回零，避免向服务器发送参数数量不合法的命令。
        if keys.is_empty() {
            return Ok(0);
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.del(keys)).await
    }

    /// 业务作用：检查键在本次服务器查询时是否存在，不取得其值或持有权。
    ///
    /// 参数说明：
    /// - `key`: 待检查是否存在的 Redis key。
    ///
    /// 返回：存在为真、缺失为假；命令失败返回错误，结果不能作为后续写入的原子前置条件。
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.exists(key)).await
    }

    /// 业务作用：以毫秒精度更新已有键的相对过期时间。
    ///
    /// 参数说明：
    /// - `key`: 需要设置相对过期时间的 Redis key。
    /// - `ttl`: 从当前时间开始计算的存活时间；非零亚毫秒会按 1ms 下界处理。
    ///
    /// 返回：过期设置获应用时为真，键缺失时为假；时间越界或命令失败返回错误。
    /// 显式零时长会要求服务器立即删除已有键。
    pub async fn expire(&self, key: &str, ttl: Duration) -> Result<bool> {
        let mut c = self.conn();
        //亚毫秒 ttl 截断成 0 会让 PEXPIRE 0 **直接删 key**——
        // 非零 Duration 至少留 1ms(立即过期请显式传 Duration::ZERO)。
        self.timed(c.pexpire(key, duration_to_millis_floor1(ttl)? as i64))
            .await
    }

    /// 业务作用：将键的剩余存活时间与缺失、永久状态区分，避免业务误用负 TTL。
    ///
    /// 参数说明：
    /// - `key`: 待查询剩余毫秒 TTL 的 Redis key。
    ///
    /// 返回：`-2` 映射缺失、`-1` 映射永久，其余值按非负毫秒映射过期时间；命令失败返回错误。
    pub async fn pttl(&self, key: &str) -> Result<KeyTtl> {
        let mut c = self.conn();
        let v: i64 = self.timed(c.pttl(key)).await?;
        Ok(match v {
            -2 => KeyTtl::Missing,
            -1 => KeyTtl::Persistent,
            ms => KeyTtl::Expires(Duration::from_millis(ms.max(0) as u64)),
        })
    }

    /// 业务作用：在 Redis 内原子调整整数计数，避免客户端读后写的竞争。
    ///
    /// 参数说明：
    /// - `key`: 保存整数计数值的 Redis key。
    /// - `delta`: 本次递增的整数步长，可为负值。
    ///
    /// 返回：调整后的整数值；类型、整数范围或命令失败返回错误。
    /// 同一步长重复调用会再次计数，结果未知时须由业务幂等机制裁决。
    pub async fn incr_by(&self, key: &str, delta: i64) -> Result<i64> {
        let mut c = self.conn();
        self.timed(c.incr(key, delta)).await
    }

    /// 业务作用：通过 SCAN 游标收集匹配键，避免使用一次遍历全部键的 KEYS 命令。
    ///
    /// 参数说明：
    /// - `pattern`: Redis `MATCH` 模式，用于限制本次遍历返回的 key 范围。
    ///
    /// 返回：完整游标过程收集的键列表；遍历失败返回错误，可能包含重复项且不是一致性快照。
    /// 结果全部留在内存，本方法不保证遍历集群全部节点；不套单命令预算，整体时间和结果规模由调用方控制。
    pub async fn scan_match(&self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.conn();
        let mut out = Vec::new();
        {
            let mut it: AsyncIter<String> = conn.scan_match(pattern).await?;
            while let Some(item) = it.next_item().await {
                out.push(item?);
            }
        }
        Ok(out)
    }

    // ───────────────────────── hash ─────────────────────────

    /// 业务作用：写入一个 Hash 字段，保留同一键下其它字段。
    ///
    /// 参数说明：
    /// - `key`：Hash 所在的完整 Redis 键。
    /// - `field`：需要新增或覆盖的字段名。
    /// - `val`：编码为单个参数的字段值。
    ///
    /// 返回：新增字段为真、覆盖已有字段为假；两者均可能成功写入，命令失败返回错误。
    pub async fn h_set<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.hset(key, field, val)).await
    }

    /// 业务作用：读取并解码 Hash 中的单个字段，保留字段缺失的独立语义。
    ///
    /// 参数说明：
    /// - `key`：Hash 所在的完整 Redis 键。
    /// - `field`：待读取的字段名。
    ///
    /// 返回：字段存在时返回解码值，键或字段缺失时为 `None`；命令或解码失败返回错误。
    pub async fn h_get<T: FromRedisValue>(&self, key: &str, field: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.hget(key, field)).await
    }

    /// 业务作用：一次读取 Hash 的全部字段和值，适用于调用方已控制字段规模的对象。
    ///
    /// 参数说明：
    /// - `key`：待读取完整内容的 Hash 键。
    ///
    /// 返回：字段名到解码值的映射，缺失键为空映射；任一解码或命令失败返回错误，不提供分页。
    pub async fn h_get_all<T: FromRedisValue>(
        &self,
        key: &str,
    ) -> Result<std::collections::HashMap<String, T>> {
        let mut c = self.conn();
        self.timed(c.hgetall(key)).await
    }

    /// 业务作用：删除 Hash 的指定字段，保留未指定字段。
    ///
    /// 参数说明：
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `fields`: 待删除的 hash field 列表；空切片会短路返回 `0`。
    ///
    /// 返回：实际删除的字段数；缺失字段不计数，命令失败返回错误。
    pub async fn h_del(&self, key: &str, fields: &[&str]) -> Result<u64> {
        //空切片短路返回 0——否则 `HDEL key`(无 field)报参数错误。
        if fields.is_empty() {
            return Ok(0);
        }
        let mut c = self.conn();
        self.timed(c.hdel(key, fields)).await
    }

    /// 业务作用：在服务端原子调整 Hash 的整数字段计数。
    ///
    /// 参数说明：
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递增的 hash field。
    /// - `delta`: 本次递增的整数步长，可为负值。
    ///
    /// 返回：调整后的字段整数值；类型、范围或命令失败返回错误，结果未知时不得无条件重复增量。
    pub async fn h_incr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64> {
        let mut c = self.conn();
        self.timed(c.hincr(key, field, delta)).await
    }

    // ───────────────────────── zset ─────────────────────────

    /// 业务作用：新增有序集合成员或更新已有成员分数。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `score`：成员的新分数，合法性由 Redis 命令检查。
    /// - `member`：编码为单个参数的成员身份。
    ///
    /// 返回：新增成员为真，已有成员为假；返回假仍可能更新分数，命令失败返回错误。
    pub async fn z_add<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        score: f64,
        member: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.zadd(key, member, score)).await
    }

    /// 业务作用：一次移除有序集合的指定成员，支持单成员和批量参数。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `members`：编码后作为成员列表的参数，空列表不发送命令。
    ///
    /// 返回：实际删除的成员数，空列表为零；命令失败返回错误。
    pub async fn z_rem<V: ToRedisArgs + Send + Sync>(&self, key: &str, members: V) -> Result<u64> {
        //空 members → no-op `Ok(0)`,不发空 `ZREM`。
        if members.num_of_args() == 0 {
            return Ok(0);
        }
        let mut c = self.conn();
        self.timed(c.zrem(key, members)).await
    }

    /// 业务作用：读取有序集合的成员数量，不拉取成员正文。
    ///
    /// 参数说明：
    /// - `key`: 有序集合所在的 Redis key。
    ///
    /// 返回：服务器查询时的成员数，缺失键为零；类型不符或命令失败返回错误。
    pub async fn z_card(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.zcard(key)).await
    }

    /// 业务作用：按分数闭区间读取有序集合成员，保持服务器返回的分数顺序。
    ///
    /// 参数说明：
    /// - `key`：待查询的有序集合键。
    /// - `min`：包含在查询范围内的最小分数。
    /// - `max`：包含在查询范围内的最大分数。
    ///
    /// 返回：区间内的解码成员列表，无匹配时为空；命令或解码失败返回错误，不限制结果数量。
    pub async fn z_range_by_score<T: FromRedisValue>(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.zrangebyscore(key, min, max)).await
    }

    /// 业务作用：删除有序集合分数闭区间内的全部成员。
    ///
    /// 参数说明：
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 需要删除的最小 score 边界。
    /// - `max`: 需要删除的最大 score 边界。
    ///
    /// 返回：实际删除的成员数，无匹配时为零；命令失败返回错误，删除成功后不能由本接口恢复。
    pub async fn z_rem_range_by_score(&self, key: &str, min: f64, max: f64) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.zrembyscore(key, min, max)).await
    }

    // ───────────────────────── set ─────────────────────────

    /// 业务作用：将单个成员加入集合，已存在成员不会重复存储。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `member`：编码为单个参数的成员值。
    ///
    /// 返回：新增成员时为一，已存在时为零；命令失败返回错误。
    pub async fn s_add<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        member: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.sadd(key, member)).await
    }

    /// 业务作用：读取集合的全部成员，适用于调用方已控制集合规模的场景。
    ///
    /// 参数说明：
    /// - `key`：待查询的集合键。
    ///
    /// 返回：无排序保证的解码成员列表，键缺失时为空；命令或解码失败返回错误，不提供分页。
    pub async fn s_members<T: FromRedisValue>(&self, key: &str) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.smembers(key)).await
    }

    // ───────────────────────── string / 批量 ─────────────────────────

    /// 业务作用：仅在键不存在时原子写入值，避免先查存在性再写入的竞争。
    ///
    /// 参数说明：
    /// - `key`：需要条件写入的完整 Redis 键。
    /// - `val`：键缺失时写入的单参数值。
    ///
    /// 返回：写入成功为真，键已存在为假；命令失败返回错误。
    /// 本方法不设置 TTL、所有权令牌或续租，不能单独替代完整分布式锁。
    pub async fn set_nx<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.set_nx(key, val)).await
    }

    /// 业务作用：按输入顺序批量读取字符串键，保留每个键的缺失位置。
    ///
    /// 参数说明：
    /// - `keys`：待读取的键序列，空切片不发送命令。
    ///
    /// 返回：与输入位置对应的可选值，缺失或非字符串键为 `None`；命令或解码失败返回错误。
    /// 集群传输可按槽拆分读取，不提供跨槽的一致性快照。
    pub async fn mget<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut c = self.conn();
        self.timed(c.mget(keys)).await
    }

    /// 业务作用：以一条 MSET 原子写入一组键值，集群模式下要求全部键同槽。
    ///
    /// 参数说明：
    /// - `items`：待写入的键值对，值须编码为一个 Redis 参数；空切片不发送命令。
    ///
    /// 返回：全部写入获确认或输入为空时成功；跨槽或命令失败返回错误，已有键的 TTL 不保留。
    pub async fn mset<V: ToRedisArgs + Send + Sync>(&self, items: &[(&str, V)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = items.iter().map(|(k, _)| *k).collect();
        self.cluster_same_slot_guard(&keys)?;
        let mut c = self.conn();
        self.timed::<()>(c.mset(items)).await
    }

    /// 业务作用：原子替换字符串值并取得替换前内容，避免分离读取与写入之间的竞争。
    ///
    /// 参数说明：
    /// - `key`：需要替换值的完整 Redis 键。
    /// - `val`：编码为单参数的新值，写入后不保留原 TTL。
    ///
    /// 返回：解码后的旧值，原键缺失时为 `None`；命令或旧值解码失败返回错误，解码失败不撤销写入。
    pub async fn get_set<T: FromRedisValue, V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
    ) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.getset(key, val)).await
    }

    /// 业务作用：原子取出字符串值并删除该键，供一次性领取场景使用。
    ///
    /// 参数说明：
    /// - `key`：需要取出并删除的字符串键。
    ///
    /// 返回：存在时返回解码值，缺失时为 `None`；命令或解码失败返回错误，响应解码失败不能恢复已删除的键。
    pub async fn get_del<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.get_del(key)).await
    }

    /// 业务作用：EXPIREAT:绝对过期(**秒级** unix 时间戳)。
    /// 参数使用秒级时间戳，传入毫秒值会产生错误的过期点。
    /// 本方法显式命名 `_secs` 杜绝同名不同单位的静默错误。毫秒戳请用 `expire_at_millis`。
    ///
    /// 参数说明：
    /// - `key`: 需要设置绝对过期时间的 Redis key。
    /// - `unix_secs`: 秒级 Unix 时间戳，到达该时间后 key 过期。
    ///
    /// 返回：过期设置生效时为真，键缺失为假；过去的时间使已有键立即删除，命令失败返回错误。
    pub async fn expire_at_secs(&self, key: &str, unix_secs: i64) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.expire_at(key, unix_secs)).await
    }

    /// 业务作用：PEXPIREAT:绝对过期(**毫秒级** unix 时间戳)。
    ///
    /// 参数说明：
    /// - `key`: 需要设置绝对过期时间的 Redis key。
    /// - `unix_millis`: 毫秒级 Unix 时间戳，到达该时间后 key 过期。
    ///
    /// 返回：过期设置生效时为真，键缺失为假；过去的时间使已有键立即删除，命令失败返回错误。
    pub async fn expire_at_millis(&self, key: &str, unix_millis: i64) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.pexpire_at(key, unix_millis)).await
    }

    /// 业务作用：PERSIST:移除过期(转永久)。
    ///
    /// 参数说明：
    /// - `key`: 需要移除 TTL、转为永久保存的 Redis key。
    ///
    /// 返回：移除已有 TTL 为真，键缺失或本已永久为假；命令失败返回错误。
    pub async fn persist(&self, key: &str) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.persist(key)).await
    }

    // ───────────────────────── hash ─────────────────────────

    /// 业务作用：HSETNX:field 不存在才写。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：仅在缺失时创建的字段名。
    /// - `val`：创建字段时写入的单参数值。
    ///
    /// 返回：创建字段为真，已有字段保持不变并返回假；命令失败返回错误，不附加字段过期时间。
    pub async fn h_set_nx<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.hset_nx(key, field, val)).await
    }

    /// 业务作用：按字段顺序批量读取同一个 Hash，保留缺失字段的位置。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `fields`：待查询字段序列，空切片不发送命令。
    ///
    /// 返回：与字段位置对应的可选值列表，缺失项为 `None`；命令或任一值解码失败返回错误。
    pub async fn h_mget<T: FromRedisValue>(
        &self,
        key: &str,
        fields: &[&str],
    ) -> Result<Vec<Option<T>>> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(key);
        for f in fields {
            cmd.arg(*f);
        }
        self.execute_raw(&cmd).await
    }

    /// 业务作用：读取 Hash 的全部字段名，不返回字段值。
    ///
    /// 参数说明：
    /// - `key`: 哈希结构所在的 Redis key。
    ///
    /// 返回：字段名列表，缺失键为空；命令或字符串解码失败返回错误，结果不排序也不分页。
    pub async fn h_keys(&self, key: &str) -> Result<Vec<String>> {
        let mut c = self.conn();
        self.timed(c.hkeys(key)).await
    }

    /// 业务作用：查询 Hash 字段数量，避免为计数读取完整对象。
    ///
    /// 参数说明：
    /// - `key`: 哈希结构所在的 Redis key。
    ///
    /// 返回：查询时的字段数量，缺失键为零；类型不符或命令失败返回错误。
    pub async fn h_len(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.hlen(key)).await
    }

    // ───────────────────────── zset 查询族 ─────────────────────────

    /// 业务作用：ZSCORE:member 分数(不存在 → None)。
    ///
    /// 参数说明：
    /// - `key`: 有序集合所在的 Redis key。
    /// - `member`: 待查询 score 的有序集合成员。
    ///
    /// 返回：成员存在时的分数，键或成员缺失时为 `None`；命令或数值解码失败返回错误。
    pub async fn z_score(&self, key: &str, member: &str) -> Result<Option<f64>> {
        let mut c = self.conn();
        self.timed(c.zscore(key, member)).await
    }

    /// 业务作用：ZRANK:升序排名(不存在 → None)。
    ///
    /// 参数说明：
    /// - `key`: 有序集合所在的 Redis key。
    /// - `member`: 待查询升序排名的有序集合成员。
    ///
    /// 返回：从零开始的升序排名，成员缺失时为 `None`；命令失败返回错误。
    pub async fn z_rank(&self, key: &str, member: &str) -> Result<Option<u64>> {
        let mut c = self.conn();
        self.timed(c.zrank(key, member)).await
    }

    /// 业务作用：ZREVRANK:降序排名。
    ///
    /// 参数说明：
    /// - `key`: 有序集合所在的 Redis key。
    /// - `member`: 待查询降序排名的有序集合成员。
    ///
    /// 返回：从零开始的降序排名，成员缺失时为 `None`；命令失败返回错误。
    pub async fn z_rev_rank(&self, key: &str, member: &str) -> Result<Option<u64>> {
        let mut c = self.conn();
        self.timed(c.zrevrank(key, member)).await
    }

    /// 业务作用：ZINCRBY:member 分数 +delta,返回新分数。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `member`：需要调整分数的单个成员。
    /// - `delta`：分数增量，可为负值，非法浮点运算由 Redis 拒绝。
    ///
    /// 返回：调整后的分数；命令失败返回错误，结果未知时重复调用可能重复增加分数。
    pub async fn z_incr_by<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        member: V,
        delta: f64,
    ) -> Result<f64> {
        let mut c = self.conn();
        self.timed(c.zincr(key, member, delta)).await
    }

    /// 业务作用：ZCOUNT:分数区间 `min..=max` 的成员数。
    ///
    /// 参数说明：
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 统计区间的最小 score。
    /// - `max`: 统计区间的最大 score。
    ///
    /// 返回：闭区间内成员数量，无匹配时为零；参数或命令失败返回错误，不拉取成员正文。
    pub async fn z_count(&self, key: &str, min: f64, max: f64) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.zcount(key, min, max)).await
    }

    /// 业务作用：按升序排名区间读取成员及分数，避免逐成员查询分数。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：包含在结果内的起始排名，零为首项，负数从末尾计算。
    /// - `stop`：包含在结果内的结束排名，负数从末尾计算。
    ///
    /// 返回：按升序排列的成员与分数列表，无匹配时为空；命令或解码失败返回错误。
    pub async fn z_range_withscores<T: FromRedisValue>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<(T, f64)>> {
        let mut c = self.conn();
        self.timed(c.zrange_withscores(key, start, stop)).await
    }

    /// 业务作用：ZREVRANGE:降序按排名区间取成员。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：降序排列中的起始排名，包含该项，负数从末尾计算。
    /// - `stop`：降序排列中的结束排名，包含该项，负数从末尾计算。
    ///
    /// 返回：区间内的降序成员列表，无匹配时为空；命令或解码失败返回错误。
    pub async fn z_rev_range<T: FromRedisValue>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.zrevrange(key, start, stop)).await
    }

    // ───────────────────────── set 判定族 ─────────────────────────

    /// 业务作用：SISMEMBER:member 是否在集合。
    ///
    /// 参数说明：
    /// - `key`：待检查的集合键。
    /// - `member`：按实际编码字节判断是否存在的单个成员。
    ///
    /// 返回：成员存在为真，键或成员缺失为假；命令失败返回错误，不锁定后续成员状态。
    pub async fn s_is_member<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        member: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.sismember(key, member)).await
    }

    /// 业务作用：SMISMEMBER:批量判定成员是否在集合,返回与入参同序的 bool 列表(Redis 6.2+)。
    ///
    /// 参数说明：
    /// - `key`：待检查的集合键。
    /// - `members`：编码后依次查询的成员，空列表不发送命令。
    ///
    /// 返回：与输入顺序对应的存在性列表，空输入为空；服务器不支持命令或执行失败返回错误。
    pub async fn s_mis_member<V: ToRedisArgs + Send + Sync>(
        &self,
        key: &str,
        members: V,
    ) -> Result<Vec<bool>> {
        //空 members → 空结果,不发空 `SMISMEMBER`。
        if members.num_of_args() == 0 {
            return Ok(Vec::new());
        }
        let mut c = self.conn();
        self.timed(c.smismember(key, members)).await
    }

    /// 业务作用：SRANDMEMBER:随机取一个成员(不移除;空集 → None)。
    ///
    /// 参数说明：
    /// - `key`：待抽取成员的集合键。
    ///
    /// 返回：存在时返回一个解码成员，键缺失时为 `None`；命令或解码失败返回错误，集合内容不变。
    pub async fn s_rand_member<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.srandmember(key)).await
    }

    /// 业务作用：SRANDMEMBER key count:随机取 count 个(count<0 允许重复;不移除)。
    ///
    /// 参数说明：
    /// - `key`：待抽取成员的集合键。
    /// - `count`：正值指定最多抽取的不同成员数，负值按绝对值抽取且允许重复，零值不返回成员。
    ///
    /// 返回：随机成员列表，缺失键为空；命令或解码失败返回错误，不修改集合。
    pub async fn s_rand_member_multiple<T: FromRedisValue>(
        &self,
        key: &str,
        count: isize,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.srandmember_multiple(key, count)).await
    }

    /// 业务作用：读取集合的成员总数，不传输成员正文。
    ///
    /// 参数说明：
    /// - `key`: 集合所在的 Redis key。
    ///
    /// 返回：查询时的成员数，缺失键为零；类型不符或命令失败返回错误。
    pub async fn s_card(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.scard(key)).await
    }

    /// 业务作用：一次移除集合中的指定成员，保留未指定的成员。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `members`：编码后需删除的成员列表，空列表不发送命令。
    ///
    /// 返回：实际删除的成员数，空输入或全部缺失时为零；命令失败返回错误。
    pub async fn s_rem<V: ToRedisArgs + Send + Sync>(&self, key: &str, members: V) -> Result<u64> {
        //空 members → no-op `Ok(0)`,不发空 `SREM`。
        if members.num_of_args() == 0 {
            return Ok(0);
        }
        let mut c = self.conn();
        self.timed(c.srem(key, members)).await
    }

    /// 业务作用：SPOP:随机弹出一个成员(空集 → None)。
    ///
    /// 参数说明：
    /// - `key`：需要随机取出并移除成员的集合键。
    ///
    /// 返回：被移除成员的解码值，缺失键为 `None`；命令或解码失败返回错误，解码失败不恢复成员。
    pub async fn s_pop<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.spop(key)).await
    }

    /// 业务作用：使用 SPOP key count 随机弹出至多 count 个成员。
    ///
    /// 参数说明：
    /// - `key`：需要取出并移除成员的集合键。
    /// - `count`：最多移除的成员数，零表示不取出成员。
    ///
    /// 返回：已移除成员的解码列表，缺失键为空；命令或解码失败返回错误，不能据此认为没有移除成员。
    pub async fn s_pop_multiple<T: FromRedisValue>(
        &self,
        key: &str,
        count: usize,
    ) -> Result<Vec<T>> {
        self.execute_raw(redis::cmd("SPOP").arg(key).arg(count))
            .await
    }

    // ───────────────────────── list 全族 ─────────────────────────

    /// 业务作用：将一个或多个元素依次压入列表左端，支持在单次往返中追加一批元素。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `val`：非空元素参数序列；多个元素依次左插后在列表头呈输入的反序。
    ///
    /// 返回：插入后的列表长度；空参数在本地拒绝，命令失败返回错误，结果未知时重试可能重复插入。
    pub async fn l_push<V: ToRedisArgs + Send + Sync>(&self, key: &str, val: V) -> Result<u64> {
        //返回新长度,空值无法 no-op(返 0 会误导调用方);**fail-fast**——
        // 不把 Redis arity error 泄漏给调用方。
        if val.num_of_args() == 0 {
            return Err(crate::error::NasaRedisError::Config(
                "l_push: values 不能为空".into(),
            ));
        }
        let mut c = self.conn();
        self.timed(c.lpush(key, val)).await
    }

    /// 业务作用：按输入顺序将一个或多个元素追加到列表右端。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `val`：按顺序追加的非空元素参数序列。
    ///
    /// 返回：插入后的列表长度；空参数在本地拒绝，命令失败返回错误，结果未知时重试可能重复插入。
    pub async fn r_push<V: ToRedisArgs + Send + Sync>(&self, key: &str, val: V) -> Result<u64> {
        //同 `l_push`,空值 fail-fast(返回新长度无法 no-op)。
        if val.num_of_args() == 0 {
            return Err(crate::error::NasaRedisError::Config(
                "r_push: values 不能为空".into(),
            ));
        }
        let mut c = self.conn();
        self.timed(c.rpush(key, val)).await
    }

    /// 业务作用：LPOP:左弹出一个(空 → None)。
    ///
    /// 参数说明：
    /// - `key`：需要从左端领取元素的列表键。
    ///
    /// 返回：移除后的元素值，无可取元素时为 `None`；命令或解码失败返回错误，解码失败不恢复元素。
    pub async fn l_pop<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.lpop(key, None)).await
    }

    /// 业务作用：RPOP:右弹出一个(空 → None)。
    ///
    /// 参数说明：
    /// - `key`：需要从右端领取元素的列表键。
    ///
    /// 返回：移除后的元素值，无可取元素时为 `None`；命令或解码失败返回错误，解码失败不恢复元素。
    pub async fn r_pop<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.rpop(key, None)).await
    }

    /// 业务作用：查询列表长度，不拉取或移除元素。
    ///
    /// 参数说明：
    /// - `key`: 列表所在的 Redis key。
    ///
    /// 返回：查询时的元素总数，缺失键为零；类型不符或命令失败返回错误。
    pub async fn l_len(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.llen(key)).await
    }

    /// 业务作用：按位置读取列表片段，保留元素顺序且不移除内容。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `start`：包含在结果内的起始下标，零为首项，负数从末尾计算。
    /// - `stop`：包含在结果内的结束下标，`-1` 表示末项。
    ///
    /// 返回：区间内元素的解码列表，无匹配时为空；命令或解码失败返回错误，`0..=-1` 会读取全部元素。
    pub async fn l_range<T: FromRedisValue>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.lrange(key, start, stop)).await
    }

    /// 业务作用：LREM:删除 count 个等于 val 的元素(count>0 从头,<0 从尾,=0 全部),返回删除数。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `count`：正值从左端最多删除该数量，负值从右端最多删除其绝对值，零删除全部匹配项。
    /// - `val`：按编码后的字节匹配的元素值。
    ///
    /// 返回：实际删除的元素数，无匹配时为零；命令失败返回错误，删除不会保留恢复副本。
    pub async fn l_rem<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        count: isize,
        val: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.lrem(key, count, val)).await
    }

    /// 业务作用：LINDEX:取指定下标元素(越界 → None)。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `index`：元素下标，零为首项，负数从列表末尾计算。
    ///
    /// 返回：指定元素的解码值，越界或键缺失时为 `None`；命令或解码失败返回错误。
    pub async fn l_index<T: FromRedisValue>(&self, key: &str, index: isize) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.lindex(key, index)).await
    }

    /// 业务作用：LTRIM:只保留 `start..=stop` 区间(其余删除)。
    ///
    /// 参数说明：
    /// - `key`: 列表所在的 Redis key。
    /// - `start`: 保留区间起始下标，支持 Redis 负索引。
    /// - `stop`: 保留区间结束下标，支持 Redis 负索引。
    ///
    /// 返回：裁剪获确认后成功；空保留区间会清空列表，命令失败返回错误，不保留被裁剪元素。
    pub async fn l_trim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let mut c = self.conn();
        self.timed::<()>(c.ltrim(key, start, stop)).await
    }

    // ════════════════════ 低频命令补全(string/hash/set 运算/eval)════════════════════

    /// 业务作用：GETRANGE:子串 `start..=end`(负索引从尾)。
    ///
    /// 参数说明：
    /// - `key`: 字符串值所在的 Redis key。
    /// - `start`: 字节区间起始下标，支持 Redis 负索引。
    /// - `end`: 字节区间结束下标，支持 Redis 负索引。
    ///
    /// 返回：区间字节解码后的 UTF-8 字符串，无匹配时为空；命令失败或截取后不是合法 UTF-8 时返回错误。
    pub async fn get_range(&self, key: &str, start: isize, end: isize) -> Result<String> {
        let mut c = self.conn();
        self.timed(c.getrange(key, start, end)).await
    }

    /// 业务作用：从指定字节偏移覆盖字符串内容，保留覆盖范围之外的原有字节。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键。
    /// - `offset`：从字符串起始位置计算的字节偏移，须位于 `isize` 非负范围内。
    /// - `val`：覆盖写入的字节内容，不按字符下标计算长度。
    ///
    /// 返回：写入后的字节长度；本地偏移越界在发送前返回配置错误，服务端范围限制或命令失败返回对应错误。
    pub async fn set_range<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        offset: usize,
        val: V,
    ) -> Result<u64> {
        // 先验证无符号偏移的可表示范围，防止强制转换后向服务端发送负偏移。
        let offset = isize::try_from(offset).map_err(|_| {
            NasaRedisError::Config("set_range: offset exceeds the nonnegative isize range".into())
        })?;
        let mut c = self.conn();
        self.timed(c.setrange(key, offset, val)).await
    }

    /// 业务作用：STRLEN:字符串字节长度。
    ///
    /// 参数说明：
    /// - `key`: 字符串值所在的 Redis key。
    ///
    /// 返回：原始值的字节数，缺失键为零；类型不符或命令失败返回错误，不计算 Unicode 字符数。
    pub async fn str_len(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.strlen(key)).await
    }

    /// 业务作用：APPEND:追加,返回新长度。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键，缺失时由服务器创建。
    /// - `val`：追加到已有字节之后的单参数内容，不增加分隔符或 JSON 外壳。
    ///
    /// 返回：追加后的总字节长度；命令失败返回错误，结果未知时重复调用可能重复追加。
    pub async fn append<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.append(key, val)).await
    }

    /// 业务作用：通过 HINCRBY 的反向增量原子减少 Hash 的整数字段。
    ///
    /// 参数说明：
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递减的 hash field。
    /// - `delta`：本次减去的有符号步长，负值表示增加；不接受无法在 `i64` 范围内取负的最小值。
    ///
    /// 返回：调整后的字段值；步长无法取负时在发送命令前返回配置错误，命令失败返回对应错误。
    pub async fn h_decr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64> {
        // 反向增量必须可表示，避免边界值触发异常或环绕成方向错误的计数操作。
        let increment = delta.checked_neg().ok_or_else(|| {
            NasaRedisError::Config("h_decr_by: delta cannot be negated within i64".into())
        })?;
        let mut c = self.conn();
        self.timed(c.hincr(key, field, increment)).await
    }

    // ───────────────────────── set 集合运算 ─────────────────────────

    /// 业务作用：SDIFF:差集(第一个 key 减其余)。空切片短路;cluster 跨 slot fail-closed。
    ///
    /// 参数说明：
    /// - `keys`：首个集合为被减集合，其余集合提供需排除的成员；集群模式下所有键必须同槽。
    ///
    /// 返回：差集成员列表，空输入直接返回空列表；跨槽、命令或解码失败返回错误，不改变源集合。
    pub async fn s_diff<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<T>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.sdiff(keys)).await
    }

    /// 业务作用：SINTER:交集。空切片短路;cluster 跨 slot fail-closed。
    ///
    /// 参数说明：
    /// - `keys`：参与交集计算的集合键，集群模式下必须同槽。
    ///
    /// 返回：所有集合共有成员的列表，空输入直接返回空列表；跨槽、命令或解码失败返回错误，不改变源集合。
    pub async fn s_inter<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<T>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.sinter(keys)).await
    }

    /// 业务作用：SUNION:并集。空切片短路;cluster 跨 slot fail-closed。
    ///
    /// 参数说明：
    /// - `keys`：参与并集计算的集合键，集群模式下必须同槽。
    ///
    /// 返回：去重后的所有成员列表，空输入直接返回空列表；跨槽、命令或解码失败返回错误，不改变源集合。
    pub async fn s_union<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<T>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.sunion(keys)).await
    }

    // ───────────────────────── 脚本(类型化封装)─────────────────────────

    /// 业务作用：在 Redis 内执行受信 Lua 脚本，发送前检查集群多键同槽约束。
    ///
    /// 参数说明：
    /// - `script`：调用方控制的脚本文本，调用方负责权限与副作用范围。
    /// - `keys`：依次传入 Lua `KEYS` 的全部业务键，集群模式下必须同槽。
    /// - `args`：依次传入 Lua `ARGV` 的非键参数。
    ///
    /// 返回：脚本结果的目标类型值；跨槽、脚本执行、传输或解码失败返回错误。
    /// 脚本错误与客户端超时不保证回滚此前已执行的写入。
    pub async fn eval<T: FromRedisValue>(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<T> {
        //Cluster 下脚本 KEYS 必须同槽,**本地 fail-closed**(对齐 del/mset/s_* 多 key 命令;
        // keys.len()<2 放行)——否则只在服务端报 CROSSSLOT,诊断与其它多 key 命令不一致。
        self.cluster_same_slot_guard(keys)?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys {
            cmd.arg(*k);
        }
        for a in args {
            cmd.arg(*a);
        }
        self.execute_raw(&cmd).await
    }

    /// 业务作用：EVALSHA:按 SHA1 执行已缓存脚本(NOSCRIPT 时调用方需先 SCRIPT LOAD / 退回 eval)。
    ///
    /// 参数说明：
    /// - `sha1`：服务器脚本缓存中的受信脚本摘要。
    /// - `keys`：依次传入 Lua `KEYS` 的业务键，集群模式下必须同槽。
    /// - `args`：依次传入 Lua `ARGV` 的非键参数。
    ///
    /// 返回：脚本结果的目标类型值；跨槽、缓存缺失、执行或解码失败返回错误，不自动加载或重试脚本。
    pub async fn eval_sha<T: FromRedisValue>(
        &self,
        sha1: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<T> {
        //同 `eval`,Cluster 跨 slot 本地 fail-closed。
        self.cluster_same_slot_guard(keys)?;
        let mut cmd = redis::cmd("EVALSHA");
        cmd.arg(sha1).arg(keys.len());
        for k in keys {
            cmd.arg(*k);
        }
        for a in args {
            cmd.arg(*a);
        }
        self.execute_raw(&cmd).await
    }

    /// 业务作用：SCRIPT LOAD:缓存脚本,返回 SHA1。
    ///
    /// 参数说明：
    /// - `script`: 需要加载到 Redis 脚本缓存中的 Lua 脚本文本。
    ///
    /// 返回：服务器确认的脚本摘要，加载失败返回错误；不执行脚本，后续仍须处理缓存清理导致的 `NOSCRIPT`。
    pub async fn script_load(&self, script: &str) -> Result<String> {
        self.execute_raw(redis::cmd("SCRIPT").arg("LOAD").arg(script))
            .await
    }

    // ───────────────────────── 直接 Stream 命令─────────────
    // partition 内部用 XREADGROUP/XAUTOCLAIM/XACK,这里把 RedisProxy 暴露的"运维/低频"
    // Stream 命令补成对外类型化方法(execute_raw 已套 command.timeout_ms)。

    /// 业务作用：向 Stream 追加一条文本字段记录，由调用方选择自动生成或显式指定记录 ID。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `id`: entry id；通常传 `"*"` 让 Redis 自动分配。
    /// - `fields`: 要写入 entry 的 field/value 列表；为空会 fail-fast。
    ///
    /// 返回：与请求坐标一致的非零完整记录 ID；空字段、非法身份、命令或回复校验失败返回错误。
    /// 自动 ID 的写入结果未知时再次发送可能追加另一条记录，本入口不提供幂等发布。
    pub async fn x_add(&self, key: &str, id: &str, fields: &[(&str, &str)]) -> Result<String> {
        //空 fields 会拼出非法 `XADD key id`(Redis 报参数错)——提前 fail-fast。
        if fields.is_empty() {
            return Err(crate::error::NasaRedisError::Config(
                "x_add: fields 不能为空(XADD 至少需一个 field-value 对)".into(),
            ));
        }
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key).arg(id);
        for (f, v) in fields {
            cmd.arg(*f).arg(*v);
        }
        let reply: redis::Value = self.execute_raw(&cmd).await?;
        crate::stream::check_stream_id_reply(&reply, id)?;
        Ok(String::from_redis_value(reply)?)
    }

    /// 业务作用：向 Stream 追加原始字节字段，避免为传输二进制内容强制转换为 UTF-8 文本。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `id`: entry id；通常传 `"*"` 让 Redis 自动分配。
    /// - `fields`: 要写入 entry 的 field/bytes 列表；为空会 fail-fast。
    ///
    /// 返回：与请求坐标一致的非零完整记录 ID；空字段、非法身份、命令或回复校验失败返回错误。
    /// 不解析字段正文；结果未知或回复异常不证明追加未发生，也不自动重试。
    pub async fn x_add_bytes(
        &self,
        key: &str,
        id: &str,
        fields: &[(&str, &[u8])],
    ) -> Result<String> {
        if fields.is_empty() {
            return Err(crate::error::NasaRedisError::Config(
                "x_add_bytes: fields 不能为空(XADD 至少需一个 field-value 对)".into(),
            ));
        }
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key).arg(id);
        for (f, v) in fields {
            cmd.arg(*f).arg(*v);
        }
        let reply: redis::Value = self.execute_raw(&cmd).await?;
        crate::stream::check_stream_id_reply(&reply, id)?;
        Ok(String::from_redis_value(reply)?)
    }

    /// 业务作用：读取 Stream 当前保存的记录数，不将其混同于消费组待处理数。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    ///
    /// 返回：已保存记录数，缺失键为零；命令失败返回错误，结果不代表尚未 ACK 的数量。
    pub async fn x_len(&self, key: &str) -> Result<u64> {
        self.execute_raw(redis::cmd("XLEN").arg(key)).await
    }

    /// 业务作用：XDEL:删除指定 id 的 entries,返回实际删除数。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `ids`: 待删除的 entry id 列表；空白 id 会被过滤，过滤后为空则短路返回 `0`。
    ///
    /// 返回：实际删除的记录数；非空 ID 不裁剪空白，非法 ID 或命令失败返回错误。
    /// 删除正文不等同于消费组 ACK，调用方须自行维护确认与恢复责任。
    pub async fn x_del(&self, key: &str, ids: &[&str]) -> Result<u64> {
        //过滤 blank id,过滤后为空 → no-op `Ok(0)`,不发空 `XDEL`
        // 空集合或全部空白的 ID 不发送命令，避免 Redis 参数数量错误。
        let ids: Vec<&str> = ids
            .iter()
            .copied()
            .filter(|s| !s.trim().is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("XDEL");
        cmd.arg(key);
        for id in &ids {
            cmd.arg(*id);
        }
        self.execute_raw(&cmd).await
    }

    /// 业务作用：按近似记录数量裁剪 Stream，允许 Redis 按存储节点边界保留超出目标的记录。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `maxlen`: 近似裁剪后希望保留的最大 entry 数。
    ///
    /// 返回：实际删除的记录数，命令失败返回错误；不提供严格数量上限，也不等待消费组确认。
    pub async fn x_trim_maxlen_approx(&self, key: &str, maxlen: u64) -> Result<u64> {
        self.execute_raw(
            redis::cmd("XTRIM")
                .arg(key)
                .arg("MAXLEN")
                .arg("~")
                .arg(maxlen),
        )
        .await
    }

    /// 业务作用：XTRIM MAXLEN(**精确**裁剪到 maxlen 条),返回删除数。`maxlen=0` 清空 stream。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `maxlen`: 精确裁剪后允许保留的最大 entry 数；`0` 表示清空。
    ///
    /// 返回：命令执行时实际删除的记录数，命令失败返回错误；裁剪不检查消费完成情况，调用方负责保留策略。
    pub async fn x_trim_maxlen_exact(&self, key: &str, maxlen: u64) -> Result<u64> {
        self.execute_raw(redis::cmd("XTRIM").arg(key).arg("MAXLEN").arg(maxlen))
            .await
    }

    /// 业务作用：XTRIM MINID:裁掉 id < min_id 的 entries(精确),返回删除数。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `min_id`: 保留边界 id；小于该 id 的 entry 会被裁剪。
    ///
    /// 返回：实际删除的记录数，边界非法或命令失败返回错误；不以 ACK 状态保护未完成记录。
    pub async fn x_trim_minid(&self, key: &str, min_id: &str) -> Result<u64> {
        self.execute_raw(redis::cmd("XTRIM").arg(key).arg("MINID").arg(min_id))
            .await
    }

    /// 业务作用：按记录 ID 正序读取 Stream 区间，不创建消费组或待确认责任。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `start`：起始记录 ID，`-` 表示最小边界，区间包含性遵循 Redis ID 语法。
    /// - `end`：结束记录 ID，`+` 表示最大边界。
    /// - `count`：可选返回条数限制，缺席时读取整个匹配区间。
    ///
    /// 返回：按目标类型解码的记录集合，边界、命令或解码失败返回错误；不解析字段中的业务正文。
    pub async fn x_range<T: FromRedisValue>(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<T> {
        let mut cmd = redis::cmd("XRANGE");
        cmd.arg(key).arg(start).arg(end);
        if let Some(n) = count {
            cmd.arg("COUNT").arg(n);
        }
        self.execute_raw(&cmd).await
    }

    /// 业务作用：XREVRANGE:逆序读区间(`end`/`start` 顺序与 XRANGE 相反,如 `+`/`-`)。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `end`：逆序读取首先使用的较大 ID 边界，`+` 表示最大边界。
    /// - `start`：读取终止的较小 ID 边界，`-` 表示最小边界。
    /// - `count`：可选返回条数限制，缺席时读取全部匹配记录。
    ///
    /// 返回：逆序记录集合的目标类型值；边界、命令或解码失败返回错误，不改变消费组状态。
    pub async fn x_revrange<T: FromRedisValue>(
        &self,
        key: &str,
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<T> {
        let mut cmd = redis::cmd("XREVRANGE");
        cmd.arg(key).arg(end).arg(start);
        if let Some(n) = count {
            cmd.arg("COUNT").arg(n);
        }
        self.execute_raw(&cmd).await
    }

    /// 业务作用：XGROUP CREATE:建消费组。`id="$"` 只读新消息、`"0"` 从头;`mkstream`=stream 不存在则创建。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 要创建的消费组名称。
    /// - `id`: 消费组起始读取位置；`"$"` 表示只读新消息，`"0"` 表示从头读取。
    /// - `mkstream`: stream 不存在时是否由 Redis 自动创建。
    ///
    /// 返回：新组创建获确认时成功；组已存在也返回服务器错误，需幂等创建时使用 `x_group_create_if_missing`。
    /// 初始位置只作用于新组，不重置已有组的读取位置。
    pub async fn x_group_create(
        &self,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> Result<()> {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("CREATE").arg(key).arg(group).arg(id);
        if mkstream {
            cmd.arg("MKSTREAM");
        }
        let _: String = self.execute_raw(&cmd).await?;
        Ok(())
    }

    /// 业务作用：按需创建消费组，将组已存在与新建成功区分，避免初始化过程重置已有消费进度。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 要确保存在的消费组名称。
    /// - `id`: 消费组起始读取位置；`"$"` 表示只读新消息，`"0"` 表示从头读取。
    /// - `mkstream`: stream 不存在时是否由 Redis 自动创建。
    ///
    /// 返回：新建成功为真，仅服务器错误码为 `BUSYGROUP` 时为假，其余失败传播。
    /// 已有组不重新应用传入的起始位置，错误正文中的相同词语不构成组已存在的证据。
    pub async fn x_group_create_if_missing(
        &self,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> Result<bool> {
        match self.x_group_create(key, group, id, mkstream).await {
            Ok(()) => Ok(true),
            // 只有服务器结构化错误码能够确认组已存在，不能把 ACL 或参数错误中的文字当成成功证据。
            Err(NasaRedisError::Redis(error)) if error.code() == Some("BUSYGROUP") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// 业务作用：XGROUP DESTROY:销毁消费组,返回是否销毁(1/0)。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 要销毁的消费组名称。
    ///
    /// 返回：组存在并销毁时为一，组缺失为零，命令失败返回错误。
    /// 该组的消费者及 PEL 状态随之删除，Stream 正文保留；调用方须先处理未完成责任。
    pub async fn x_group_destroy(&self, key: &str, group: &str) -> Result<u64> {
        self.execute_raw(redis::cmd("XGROUP").arg("DESTROY").arg(key).arg(group))
            .await
    }

    /// 业务作用：XACK:确认组内已处理的 entries,返回实际确认数。
    ///
    /// 参数说明：
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 消费组名称。
    /// - `ids`: 待确认的 entry id 列表；空白 id 会被过滤，过滤后为空则短路返回 `0`。
    ///
    /// 返回：仅接受不超过发送 ID 数量的非负 Redis 整数；未在 PEL 中的 ID 不计数，命令或回复校验失败返回错误。
    /// 回复异常不证明确认未发生，不自动重发。
    /// 不删除 Stream 正文，也不验证业务已完成或调用方仍持有分区权威。
    pub async fn x_ack(&self, key: &str, group: &str, ids: &[&str]) -> Result<u64> {
        //过滤 blank id,过滤后为空 → no-op `Ok(0)`,不发空 `XACK`
        // 空 ID 集合不发送命令，避免 Redis 参数数量错误。
        let ids: Vec<&str> = ids
            .iter()
            .copied()
            .filter(|s| !s.trim().is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("XACK");
        cmd.arg(key).arg(group);
        for id in &ids {
            cmd.arg(*id);
        }
        let reply: redis::Value = self.execute_raw(&cmd).await?;
        crate::stream::check_stream_ack_reply(&reply, ids.len())?;
        Ok(u64::from_redis_value(reply)?)
    }

    /// 业务作用：查询消费组的 PEL 摘要，观察待确认数量及消费者分布，不接管或确认记录。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `group`：需要查询的消费组名称。
    ///
    /// 返回：包含待确认数量、最小和最大 ID、消费者统计的目标类型值；空 PEL 的 ID 边界为空。
    /// 命令或解码失败返回错误，数量不包含该组尚未读取的新记录。
    pub async fn x_pending<T: FromRedisValue>(&self, key: &str, group: &str) -> Result<T> {
        self.execute_raw(redis::cmd("XPENDING").arg(key).arg(group))
            .await
    }

    // ───────────────────────── 阻塞 list(专用连接)─────────────────────────

    /// 业务作用：在专用单节点连接上等待并领取列表左端元素，避免阻塞普通命令连接。
    ///
    /// 参数说明：
    /// - `keys`：按服务器选择优先级排列的非空列表键集合。
    /// - `timeout_secs`：服务端等待秒数，可带小数；零表示无限等待，合法性由服务器检查。
    ///
    /// 返回：命中键和已移除元素，服务端等待超时为 `None`；空键列表、集群模式或命令失败返回错误。
    /// 不套客户端命令预算，调用方须设置整体等待边界；取消或解码错误不保证元素尚未移除。
    pub async fn bl_pop<T: FromRedisValue>(
        &self,
        keys: &[&str],
        timeout_secs: f64,
    ) -> Result<Option<(String, T)>> {
        self.blocking_pop("BLPOP", keys, timeout_secs).await
    }

    /// 业务作用：在专用单节点连接上等待并领取列表右端元素，隔离阻塞读取对普通命令的影响。
    ///
    /// 参数说明：
    /// - `keys`：按服务器选择优先级排列的非空列表键集合。
    /// - `timeout_secs`：服务端等待秒数，可带小数；零表示无限等待。
    ///
    /// 返回：命中键和已移除元素，服务端等待超时为 `None`；空键列表、集群模式或命令失败返回错误。
    /// 不套客户端命令预算，取消或响应解码失败不保证元素尚未移除。
    pub async fn br_pop<T: FromRedisValue>(
        &self,
        keys: &[&str],
        timeout_secs: f64,
    ) -> Result<Option<(String, T)>> {
        self.blocking_pop("BRPOP", keys, timeout_secs).await
    }

    /// 业务作用：为左右弹出共用独立连接与响应解码路径，在无法保证路由时拒绝集群调用。
    ///
    /// 参数说明：
    /// - `op`：由内部入口固定选择的 `BLPOP` 或 `BRPOP` 命令。
    /// - `keys`：参与本次等待的非空列表键集合。
    /// - `timeout_secs`：传入 Redis 的阻塞秒数，零表示无限等待。
    ///
    /// 返回：领取结果或服务器等待超时的空值；建连、命令和解码错误直接传播。
    /// 本层不为专用连接设置整体截止时间，外层取消等待也不能证明远端未移除元素。
    async fn blocking_pop<T: FromRedisValue>(
        &self,
        op: &str,
        keys: &[&str],
        timeout_secs: f64,
    ) -> Result<Option<(String, T)>> {
        if keys.is_empty() {
            return Err(crate::error::NasaRedisError::Config(
                "bl_pop/br_pop: keys 不能为空".into(),
            ));
        }
        // 本路径从种子客户端建立单节点连接，不承担集群重定向责任；必须在发送前拒绝集群模式。
        if self.is_cluster() {
            return Err(crate::error::NasaRedisError::Config(format!(
                "bl_pop/br_pop 不支持 Cluster(单连接 BLOCK 不路由 MOVED;keys={keys:?})——\
                 请改用非阻塞轮询(lpop/rpop + 退避)或对目标 key 所在节点显式建连"
            )));
        }
        // 专用连接:BLOCK 只阻塞本连接,不影响共享连接的其它命令。
        let mut conn = self.raw_client().get_multiplexed_async_connection().await?;
        let mut cmd = redis::cmd(op);
        for k in keys {
            cmd.arg(*k);
        }
        cmd.arg(timeout_secs);
        // 命中 → [key, value];超时 → nil
        let r: Option<(String, T)> = cmd.query_async(&mut conn).await?;
        Ok(r)
    }
}

impl RedisClient {
    /// 业务作用：在调用链绝对预算内执行无写入副作用的 GET。
    ///
    /// 参数说明：
    /// - `key`：待查询的字符串键。
    /// - `budget`：从上游传递的绝对截止时间与取消节点，不为本次读取重新计算总预算。
    ///
    /// 返回：缓存值、不存在或调用错误；已取消时不轮询命令，不自动重试。
    pub async fn get_with_budget<T: FromRedisValue>(
        &self,
        key: &str,
        budget: &nabudget::RequestBudget,
    ) -> Result<Option<T>> {
        budget.run(self.get(key)).await?
    }

    /// 业务作用：在调用链绝对预算内读取 Hash 字段。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：待查询的字段名。
    /// - `budget`：与调用链共享的截止时间和取消信号。
    ///
    /// 返回：字段值、未命中或错误；取消停止本地等待，不改变共享键。
    pub async fn h_get_with_budget<T: FromRedisValue>(
        &self,
        key: &str,
        field: &str,
        budget: &nabudget::RequestBudget,
    ) -> Result<Option<T>> {
        budget.run(self.h_get(key, field)).await?
    }

    /// 业务作用：使批量只读查询共享同一绝对预算。
    ///
    /// 参数说明：
    /// - `keys`：按结果位置排列的字符串键，集群传输可按槽拆分。
    /// - `budget`：覆盖整批查询的绝对截止时间和取消信号。
    ///
    /// 返回：按输入顺序返回值或错误；不为单个 key 重新开始计时。
    pub async fn mget_with_budget<T: FromRedisValue>(
        &self,
        keys: &[&str],
        budget: &nabudget::RequestBudget,
    ) -> Result<Vec<Option<T>>> {
        budget.run(self.mget(keys)).await?
    }

    /// 业务作用：在请求预算内查询键存在性。
    ///
    /// 参数说明：
    /// - `key`：需要检查是否存在的 Redis 键。
    /// - `budget`：共享的绝对截止时间与取消信号。
    ///
    /// 返回：存在性或明确的取消、到期、后端错误。
    pub async fn exists_with_budget(
        &self,
        key: &str,
        budget: &nabudget::RequestBudget,
    ) -> Result<bool> {
        budget.run(self.exists(key)).await?
    }
}
