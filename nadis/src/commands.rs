// ============================================================================
// src/commands.rs —— 类型化命令层(文档;对照 原实现 RedisProxy 的命令 API 子集)。
//
// 第一阶段只覆盖 Lock/Pipeline/常用命令(不 1:1 抄 150 个方法)
// 红线:
//   · pttl 返回 KeyTtl 类型化结果,不把 -1/-2 混进裸整数;
//   · KEYS 不提供(admin/raw 能力走 execute_raw);常规遍历 = scan_match 流式;
//   · 值参数显式选择:bytes/String 原语直传,JSON 走 Json<T> 包装(codec.rs)。
// ============================================================================

use std::time::Duration;

use redis::{AsyncCommands, AsyncIter, FromRedisValue, ToRedisArgs, ToSingleRedisArg};

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// Duration → 毫秒:**非零时下界 1ms**。亚毫秒 Duration 直接 `as_millis()`
/// 会截断成 0,使 PSETEX 报错 / PEXPIRE 0 删 key;此处保证只要 ttl>0 就不塌成非法 0。
/// `Duration::ZERO` 仍返回 0(调用方显式表达"立即过期/删")。Redis 的相对过期参数按
/// signed 64-bit 毫秒解析；超出该范围必须在本地拒绝，不能经 `as` 转换回绕成负数或较小 TTL。
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

/// 把秒精度 Redis TTL 转成服务端可表达的 signed 64-bit 正整数范围。
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
    ///给**单次往返命令** future 套 `command.timeout_ms`(0=不限)。超时 →
    /// `ExecutionUnknown`(命令可能已写出执行,非幂等命令不得透明重试,语义同断线)。
    /// 用法:`let mut c = self.conn(); self.timed(c.get(key)).await`——conn 先绑定到本地(不让
    /// 临时量在 await 前被 drop),future 借用它传入本 helper。多往返游标(scan_match)不套此 timeout。
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

    /// Runs the Redis GET command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn get<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.get(key)).await
    }

    /// Runs the Redis SET command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn set<V: ToSingleRedisArg + Send + Sync>(&self, key: &str, val: V) -> Result<()> {
        let mut c = self.conn();
        self.timed::<()>(c.set(key, val)).await
    }

    /// Runs the Redis SET with expiration command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    /// - `ttl`: 过期时间,用于控制 Redis key 生命周期。
    pub async fn set_ttl<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
        ttl: Duration,
    ) -> Result<()> {
        let mut c = self.conn();
        //亚毫秒 ttl 截断成 0 会让 PSETEX 报 "invalid expire time"——
        // 非零 Duration 至少留 1ms(向上取整,不静默塌成非法值)。
        self.timed::<()>(c.pset_ex(key, val, duration_to_millis_floor1(ttl)?))
            .await
    }

    ///cluster 下多 key 命令(del/mset/s_diff/s_inter/s_union)跨 slot 会撞服务端
    /// `CROSSSLOT`(报错晦涩)。提前 **fail-closed** 回清晰错误(对齐 bl_pop 风格),引导用同 `{hashtag}`。
    /// standalone 无 slot 概念直接放行;`mget` 由 driver 按 slot 自动拆分,豁免不调用本守卫。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
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

    /// Runs the Redis DEL command.
    ///
    /// # 参数
    ///
    /// - `keys`: 待删除的一组 Redis key；空切片会短路返回 `0`，cluster 下必须同槽。
    pub async fn del(&self, keys: &[&str]) -> Result<u64> {
        //空切片短路返回 0(对齐 原实现 `isEmpty` 短路)——否则 `DEL`(无 key)
        // 触发 Redis "wrong number of arguments" 报错。
        if keys.is_empty() {
            return Ok(0);
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.del(keys)).await
    }

    /// Runs the Redis EXISTS command.
    ///
    /// # 参数
    ///
    /// - `key`: 待检查是否存在的 Redis key。
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.exists(key)).await
    }

    /// Runs the Redis EXPIRE command.
    ///
    /// # 参数
    ///
    /// - `key`: 需要设置相对过期时间的 Redis key。
    /// - `ttl`: 从当前时间开始计算的存活时间；非零亚毫秒会按 1ms 下界处理。
    pub async fn expire(&self, key: &str, ttl: Duration) -> Result<bool> {
        let mut c = self.conn();
        //亚毫秒 ttl 截断成 0 会让 PEXPIRE 0 **直接删 key**——
        // 非零 Duration 至少留 1ms(立即过期请显式传 Duration::ZERO)。
        self.timed(c.pexpire(key, duration_to_millis_floor1(ttl)? as i64))
            .await
    }

    /// PTTL → KeyTtl(-2=Missing,-1=Persistent)。
    ///
    /// # 参数
    ///
    /// - `key`: 待查询剩余毫秒 TTL 的 Redis key。
    pub async fn pttl(&self, key: &str) -> Result<KeyTtl> {
        let mut c = self.conn();
        let v: i64 = self.timed(c.pttl(key)).await?;
        Ok(match v {
            -2 => KeyTtl::Missing,
            -1 => KeyTtl::Persistent,
            ms => KeyTtl::Expires(Duration::from_millis(ms.max(0) as u64)),
        })
    }

    /// Runs the Redis INCRBY command.
    ///
    /// # 参数
    ///
    /// - `key`: 保存整数计数值的 Redis key。
    /// - `delta`: 本次递增的整数步长，可为负值。
    pub async fn incr_by(&self, key: &str, delta: i64) -> Result<i64> {
        let mut c = self.conn();
        self.timed(c.incr(key, delta)).await
    }

    /// SCAN 流式遍历(替代 KEYS; 第 4 条)。返回收集后的 Vec——
    /// AsyncIter 借用连接,先在内部消费完(单页 count 由 Redis 决定,遍历期间可被并发修改)。
    /// M2 注:**多往返游标不套 `command.timeout_ms`**(单命令超时语义不适用于整轮 SCAN);
    /// 需整体超时由调用方在外层 `tokio::time::timeout` 包裹。
    ///
    /// # 参数
    ///
    /// - `pattern`: Redis `MATCH` 模式，用于限制本次遍历返回的 key 范围。
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

    /// Runs the Redis HSET command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn h_set<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.hset(key, field, val)).await
    }

    /// Runs the Redis HGET command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    pub async fn h_get<T: FromRedisValue>(&self, key: &str, field: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.hget(key, field)).await
    }

    /// Runs the Redis HGETALL command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn h_get_all<T: FromRedisValue>(
        &self,
        key: &str,
    ) -> Result<std::collections::HashMap<String, T>> {
        let mut c = self.conn();
        self.timed(c.hgetall(key)).await
    }

    /// Runs the Redis HDEL command.
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `fields`: 待删除的 hash field 列表；空切片会短路返回 `0`。
    pub async fn h_del(&self, key: &str, fields: &[&str]) -> Result<u64> {
        //空切片短路返回 0(对齐 原实现)——否则 `HDEL key`(无 field)报参数错误。
        if fields.is_empty() {
            return Ok(0);
        }
        let mut c = self.conn();
        self.timed(c.hdel(key, fields)).await
    }

    /// Runs the Redis HINCRBY command.
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递增的 hash field。
    /// - `delta`: 本次递增的整数步长，可为负值。
    pub async fn h_incr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64> {
        let mut c = self.conn();
        self.timed(c.hincr(key, field, delta)).await
    }

    // ───────────────────────── zset ─────────────────────────

    /// Runs the Redis ZADD command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `score`: 有序集合成员 score。
    /// - `member`: 集合或有序集合成员。
    pub async fn z_add<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        score: f64,
        member: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.zadd(key, member, score)).await
    }

    /// ZREM:移除一个或多个成员(放宽到 `ToRedisArgs`,单值/批量同一方法,
    /// 对齐 原实现 varargs,避免批量删被迫 N 次往返)。单个 `&str` 仍直接可传。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `members`: 集合或有序集合成员列表。
    pub async fn z_rem<V: ToRedisArgs + Send + Sync>(&self, key: &str, members: V) -> Result<u64> {
        //空 members → no-op `Ok(0)`,不发空 `ZREM`(对照 原实现 zRem;否则 Redis arity error)。
        if members.num_of_args() == 0 {
            return Ok(0);
        }
        let mut c = self.conn();
        self.timed(c.zrem(key, members)).await
    }

    /// Runs the Redis ZCARD command.
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    pub async fn z_card(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.zcard(key)).await
    }

    /// Runs the Redis ZRANGEBYSCORE command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `min`: 允许的最小值或区间下界。
    /// - `max`: 允许的最大值或区间上界。
    pub async fn z_range_by_score<T: FromRedisValue>(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.zrangebyscore(key, min, max)).await
    }

    /// Runs the Redis ZREMRANGEBYSCORE command.
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 需要删除的最小 score 边界。
    /// - `max`: 需要删除的最大 score 边界。
    pub async fn z_rem_range_by_score(&self, key: &str, min: f64, max: f64) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.zrembyscore(key, min, max)).await
    }

    // ───────────────────────── set ─────────────────────────

    /// Runs the Redis SADD command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub async fn s_add<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        member: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.sadd(key, member)).await
    }

    /// Runs the Redis SMEMBERS command.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn s_members<T: FromRedisValue>(&self, key: &str) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.smembers(key)).await
    }

    // ════════════════════ ② 命令广度(对照 原实现 RedisProxy)════════════════════

    // ───────────────────────── string / 批量 ─────────────────────────

    /// SETNX(`set_if_absent`):key 不存在才写,返回是否写入(锁/幂等基础)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn set_nx<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.set_nx(key, val)).await
    }

    /// MGET:批量取(缺失键 → None,顺序对应)。空切片短路返回空 Vec(对齐 原实现)。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    pub async fn mget<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut c = self.conn();
        self.timed(c.mget(keys)).await
    }

    /// MSET:批量写。空切片短路(对齐 原实现)。
    ///
    /// # 参数
    /// - `items`: 已编译的日志 pattern 片段列表。
    pub async fn mset<V: ToRedisArgs + Send + Sync>(&self, items: &[(&str, V)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = items.iter().map(|(k, _)| *k).collect();
        self.cluster_same_slot_guard(&keys)?;
        let mut c = self.conn();
        self.timed::<()>(c.mset(items)).await
    }

    /// GETSET:写新值并返回旧值。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn get_set<T: FromRedisValue, V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
    ) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.getset(key, val)).await
    }

    /// GETDEL:取值并删除。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn get_del<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.get_del(key)).await
    }

    /// EXPIREAT:绝对过期(**秒级** unix 时间戳)。
    /// ⚠ **跨语言迁移陷阱**:原实现 `expireAt(key, millis)` 是**毫秒**戳;
    /// 本方法显式命名 `_secs` 杜绝同名不同单位的静默错误。毫秒戳请用 `expire_at_millis`。
    ///
    /// # 参数
    ///
    /// - `key`: 需要设置绝对过期时间的 Redis key。
    /// - `unix_secs`: 秒级 Unix 时间戳，到达该时间后 key 过期。
    pub async fn expire_at_secs(&self, key: &str, unix_secs: i64) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.expire_at(key, unix_secs)).await
    }

    /// PEXPIREAT:绝对过期(**毫秒级** unix 时间戳;对齐 原实现 `expireAt(key, millis)`)。
    ///
    /// # 参数
    ///
    /// - `key`: 需要设置绝对过期时间的 Redis key。
    /// - `unix_millis`: 毫秒级 Unix 时间戳，到达该时间后 key 过期。
    pub async fn expire_at_millis(&self, key: &str, unix_millis: i64) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.pexpire_at(key, unix_millis)).await
    }

    /// PERSIST:移除过期(转永久)。
    ///
    /// # 参数
    ///
    /// - `key`: 需要移除 TTL、转为永久保存的 Redis key。
    pub async fn persist(&self, key: &str) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.persist(key)).await
    }

    // ───────────────────────── hash ─────────────────────────

    /// HSETNX:field 不存在才写。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn h_set_nx<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.hset_nx(key, field, val)).await
    }

    /// HMGET:批量取 field(缺失 → None)。空切片短路。
    /// (redis-rs `hget` 仅单 field,HMGET 手搓 cmd 经 execute_raw——已套 command.timeout_ms。)
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `fields`: Hash 字段名列表,用于批量读取或删除。
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

    /// HKEYS:所有 field 名。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    pub async fn h_keys(&self, key: &str) -> Result<Vec<String>> {
        let mut c = self.conn();
        self.timed(c.hkeys(key)).await
    }

    /// HLEN:field 数。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    pub async fn h_len(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.hlen(key)).await
    }

    // ───────────────────────── zset 查询族 ─────────────────────────

    /// ZSCORE:member 分数(不存在 → None)。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `member`: 待查询 score 的有序集合成员。
    pub async fn z_score(&self, key: &str, member: &str) -> Result<Option<f64>> {
        let mut c = self.conn();
        self.timed(c.zscore(key, member)).await
    }

    /// ZRANK:升序排名(不存在 → None)。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `member`: 待查询升序排名的有序集合成员。
    pub async fn z_rank(&self, key: &str, member: &str) -> Result<Option<u64>> {
        let mut c = self.conn();
        self.timed(c.zrank(key, member)).await
    }

    /// ZREVRANK:降序排名。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `member`: 待查询降序排名的有序集合成员。
    pub async fn z_rev_rank(&self, key: &str, member: &str) -> Result<Option<u64>> {
        let mut c = self.conn();
        self.timed(c.zrevrank(key, member)).await
    }

    /// ZINCRBY:member 分数 +delta,返回新分数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    /// - `delta`: 计数、score 或 hash field 的增量。
    pub async fn z_incr_by<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        member: V,
        delta: f64,
    ) -> Result<f64> {
        let mut c = self.conn();
        self.timed(c.zincr(key, member, delta)).await
    }

    /// ZCOUNT:分数区间 `min..=max` 的成员数。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 统计区间的最小 score。
    /// - `max`: 统计区间的最大 score。
    pub async fn z_count(&self, key: &str, min: f64, max: f64) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.zcount(key, min, max)).await
    }

    /// ZRANGE WITHSCORES:按排名区间取 (member, score)(start/stop 支持负索引)。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
    pub async fn z_range_withscores<T: FromRedisValue>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<(T, f64)>> {
        let mut c = self.conn();
        self.timed(c.zrange_withscores(key, start, stop)).await
    }

    /// ZREVRANGE:降序按排名区间取成员。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
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

    /// SISMEMBER:member 是否在集合。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub async fn s_is_member<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        member: V,
    ) -> Result<bool> {
        let mut c = self.conn();
        self.timed(c.sismember(key, member)).await
    }

    /// SMISMEMBER:批量判定成员是否在集合,返回与入参同序的 bool 列表(Redis 6.2+)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `members`: 集合或有序集合成员列表。
    pub async fn s_mis_member<V: ToRedisArgs + Send + Sync>(
        &self,
        key: &str,
        members: V,
    ) -> Result<Vec<bool>> {
        //空 members → 空结果,不发空 `SMISMEMBER`(对照 原实现 sMisMember 空 vals 返回 emptyMap)。
        if members.num_of_args() == 0 {
            return Ok(Vec::new());
        }
        let mut c = self.conn();
        self.timed(c.smismember(key, members)).await
    }

    /// SRANDMEMBER:随机取一个成员(不移除;空集 → None)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn s_rand_member<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.srandmember(key)).await
    }

    /// SRANDMEMBER key count:随机取 count 个(count<0 允许重复;不移除)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub async fn s_rand_member_multiple<T: FromRedisValue>(
        &self,
        key: &str,
        count: isize,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.srandmember_multiple(key, count)).await
    }

    /// SCARD:集合基数。
    ///
    /// # 参数
    ///
    /// - `key`: 集合所在的 Redis key。
    pub async fn s_card(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.scard(key)).await
    }

    /// SREM:移除一个或多个成员,返回移除数(放宽到 `ToRedisArgs` 支持批量,
    /// 对齐 原实现 varargs)。单个 `&str` 仍直接可传。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `members`: 集合或有序集合成员列表。
    pub async fn s_rem<V: ToRedisArgs + Send + Sync>(&self, key: &str, members: V) -> Result<u64> {
        //空 members → no-op `Ok(0)`,不发空 `SREM`(对照 原实现 sRem)。
        if members.num_of_args() == 0 {
            return Ok(0);
        }
        let mut c = self.conn();
        self.timed(c.srem(key, members)).await
    }

    /// SPOP:随机弹出一个成员(空集 → None)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn s_pop<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.spop(key)).await
    }

    /// SPOP key count:随机弹出至多 count 个成员(对照 原实现 sPop(key,count);第九轮 P2 对称补全)。
    /// redis-rs `spop` 无 count 形态,这里直接拼命令(execute_raw 已套 timeout)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub async fn s_pop_multiple<T: FromRedisValue>(
        &self,
        key: &str,
        count: usize,
    ) -> Result<Vec<T>> {
        self.execute_raw(redis::cmd("SPOP").arg(key).arg(count))
            .await
    }

    // ───────────────────────── list 全族 ─────────────────────────

    /// LPUSH:左压入,返回新长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn l_push<V: ToRedisArgs + Send + Sync>(&self, key: &str, val: V) -> Result<u64> {
        //返回新长度,空值无法 no-op(返 0 会误导调用方);**fail-fast**——
        // 不把 Redis arity error 泄漏给调用方(对照 原实现 lPush 空 vals 直接 return)。
        if val.num_of_args() == 0 {
            return Err(crate::error::NasaRedisError::Config(
                "l_push: values 不能为空".into(),
            ));
        }
        let mut c = self.conn();
        self.timed(c.lpush(key, val)).await
    }

    /// RPUSH:右压入,返回新长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
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

    /// LPOP:左弹出一个(空 → None)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn l_pop<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.lpop(key, None)).await
    }

    /// RPOP:右弹出一个(空 → None)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub async fn r_pop<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.rpop(key, None)).await
    }

    /// LLEN:长度。
    ///
    /// # 参数
    ///
    /// - `key`: 列表所在的 Redis key。
    pub async fn l_len(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.llen(key)).await
    }

    /// LRANGE:区间取(支持负索引;`lrange(key,0,-1)` = 全量)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
    pub async fn l_range<T: FromRedisValue>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<T>> {
        let mut c = self.conn();
        self.timed(c.lrange(key, start, stop)).await
    }

    /// LREM:删除 count 个等于 val 的元素(count>0 从头,<0 从尾,=0 全部),返回删除数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn l_rem<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        count: isize,
        val: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.lrem(key, count, val)).await
    }

    /// LINDEX:取指定下标元素(越界 → None)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `index`: 列表下标、归档序号或字段位置。
    pub async fn l_index<T: FromRedisValue>(&self, key: &str, index: isize) -> Result<Option<T>> {
        let mut c = self.conn();
        self.timed(c.lindex(key, index)).await
    }

    /// LTRIM:只保留 `start..=stop` 区间(其余删除)。
    ///
    /// # 参数
    ///
    /// - `key`: 列表所在的 Redis key。
    /// - `start`: 保留区间起始下标，支持 Redis 负索引。
    /// - `stop`: 保留区间结束下标，支持 Redis 负索引。
    pub async fn l_trim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let mut c = self.conn();
        self.timed::<()>(c.ltrim(key, start, stop)).await
    }

    // ════════════════════ 低频命令补全(string/hash/set 运算/eval)════════════════════

    /// GETRANGE:子串 `start..=end`(负索引从尾)。
    ///
    /// # 参数
    ///
    /// - `key`: 字符串值所在的 Redis key。
    /// - `start`: 字节区间起始下标，支持 Redis 负索引。
    /// - `end`: 字节区间结束下标，支持 Redis 负索引。
    pub async fn get_range(&self, key: &str, start: isize, end: isize) -> Result<String> {
        let mut c = self.conn();
        self.timed(c.getrange(key, start, end)).await
    }

    /// SETRANGE:从 offset 覆盖写,返回新长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn set_range<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        offset: usize,
        val: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.setrange(key, offset as isize, val)).await
    }

    /// STRLEN:字符串字节长度。
    ///
    /// # 参数
    ///
    /// - `key`: 字符串值所在的 Redis key。
    pub async fn str_len(&self, key: &str) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.strlen(key)).await
    }

    /// APPEND:追加,返回新长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub async fn append<V: ToSingleRedisArg + Send + Sync>(
        &self,
        key: &str,
        val: V,
    ) -> Result<u64> {
        let mut c = self.conn();
        self.timed(c.append(key, val)).await
    }

    /// HDECRBY:HINCRBY 负 delta(对照 原实现 hDecrBy)。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递减的 hash field。
    /// - `delta`: 本次递减的正向步长，内部会转为负增量。
    pub async fn h_decr_by(&self, key: &str, field: &str, delta: i64) -> Result<i64> {
        let mut c = self.conn();
        self.timed(c.hincr(key, field, -delta)).await
    }

    // ───────────────────────── set 集合运算 ─────────────────────────

    /// SDIFF:差集(第一个 key 减其余)。空切片短路;cluster 跨 slot fail-closed。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    pub async fn s_diff<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<T>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.sdiff(keys)).await
    }

    /// SINTER:交集。空切片短路;cluster 跨 slot fail-closed。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    pub async fn s_inter<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<T>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.sinter(keys)).await
    }

    /// SUNION:并集。空切片短路;cluster 跨 slot fail-closed。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    pub async fn s_union<T: FromRedisValue>(&self, keys: &[&str]) -> Result<Vec<T>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.cluster_same_slot_guard(keys)?;
        let mut c = self.conn();
        self.timed(c.sunion(keys)).await
    }

    // ───────────────────────── 脚本(类型化封装)─────────────────────────

    /// EVAL:执行 Lua 脚本(类型化返回;`execute_raw` 已套 command.timeout_ms)。
    /// `keys`=KEYS[1..],`args`=ARGV[1..]。Cluster 下脚本 KEYS **必须同槽**(否则 CROSSSLOT)。
    ///
    /// # 参数
    /// - `script`: Lua 脚本文本。
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `args`: 命令或脚本参数列表。
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

    /// EVALSHA:按 SHA1 执行已缓存脚本(NOSCRIPT 时调用方需先 SCRIPT LOAD / 退回 eval)。
    ///
    /// # 参数
    /// - `sha1`: Redis Lua 脚本的 SHA1 摘要。
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `args`: 命令或脚本参数列表。
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

    /// SCRIPT LOAD:缓存脚本,返回 SHA1。
    ///
    /// # 参数
    ///
    /// - `script`: 需要加载到 Redis 脚本缓存中的 Lua 脚本文本。
    pub async fn script_load(&self, script: &str) -> Result<String> {
        self.execute_raw(redis::cmd("SCRIPT").arg("LOAD").arg(script))
            .await
    }

    // ───────────────────────── 直接 Stream 命令(对照 原实现 RedisProxy 裸 Stream API)─────────────
    // partition 内部用 XREADGROUP/XAUTOCLAIM/XACK,这里把 RedisProxy 暴露的"运维/低频"
    // Stream 命令补成对外类型化方法(execute_raw 已套 command.timeout_ms)。

    /// XADD:追加一条 entry(`id="*"` 自动生成),返回服务端分配的 entry id。
    /// `fields` = [(field, value), ...]。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `id`: entry id；通常传 `"*"` 让 Redis 自动分配。
    /// - `fields`: 要写入 entry 的 field/value 列表；为空会 fail-fast。
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
        self.execute_raw(&cmd).await
    }

    /// XADD(**byte-safe** value):field 名为 `&str`、value 为 `&[u8]`(序列化后的 envelope JSON 等)。
    /// 供 `stream` 模块的 event-field 发布用(`x_add` 的 `&str` 版仍保留给纯文本场景)。空 fields fail-fast。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `id`: entry id；通常传 `"*"` 让 Redis 自动分配。
    /// - `fields`: 要写入 entry 的 field/bytes 列表；为空会 fail-fast。
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
        self.execute_raw(&cmd).await
    }

    /// XLEN:stream 长度。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    pub async fn x_len(&self, key: &str) -> Result<u64> {
        self.execute_raw(redis::cmd("XLEN").arg(key)).await
    }

    /// XDEL:删除指定 id 的 entries,返回实际删除数。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `ids`: 待删除的 entry id 列表；空白 id 会被过滤，过滤后为空则短路返回 `0`。
    pub async fn x_del(&self, key: &str, ids: &[&str]) -> Result<u64> {
        //过滤 blank id,过滤后为空 → no-op `Ok(0)`,不发空 `XDEL`
        // (对照 原实现 RedisProxy.xDel:空/全 blank 直接返回 0,否则 Redis arity error)。
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

    /// XTRIM MAXLEN ~:**近似**裁剪(`~` 让引擎按整 macro-node 裁,O(1) 摊销,可能超额保留),返回删除数。
    /// ⚠ 需严格限长用 `x_trim_maxlen_exact`(原实现 默认是精确 MAXLEN)。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `maxlen`: 近似裁剪后希望保留的最大 entry 数。
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

    /// XTRIM MAXLEN(**精确**裁剪到 maxlen 条,对齐 原实现 默认),返回删除数。`maxlen=0` 清空 stream。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `maxlen`: 精确裁剪后允许保留的最大 entry 数；`0` 表示清空。
    pub async fn x_trim_maxlen_exact(&self, key: &str, maxlen: u64) -> Result<u64> {
        self.execute_raw(redis::cmd("XTRIM").arg(key).arg("MAXLEN").arg(maxlen))
            .await
    }

    /// XTRIM MINID:裁掉 id < min_id 的 entries(精确),返回删除数。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `min_id`: 保留边界 id；小于该 id 的 entry 会被裁剪。
    pub async fn x_trim_minid(&self, key: &str, min_id: &str) -> Result<u64> {
        self.execute_raw(redis::cmd("XTRIM").arg(key).arg("MINID").arg(min_id))
            .await
    }

    /// XRANGE:正序读区间(`start`/`end` 用 `-`/`+` 表全开;`count` 限条数)。
    /// 返回 `[(id, [(field,value),...]), ...]`(redis-rs 的 StreamRangeReply 形态由调用方泛型决定)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `end`: Redis 区间、时间窗口或解析范围的终点。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// XREVRANGE:逆序读区间(`end`/`start` 顺序与 XRANGE 相反,如 `+`/`-`)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `end`: Redis 区间、时间窗口或解析范围的终点。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// XGROUP CREATE:建消费组。`id="$"` 只读新消息、`"0"` 从头;`mkstream`=stream 不存在则创建。
    /// 组已存在返回 BUSYGROUP 错误——调用方按需吞掉(对照 原实现 ignoreBusyGroup)。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 要创建的消费组名称。
    /// - `id`: 消费组起始读取位置；`"$"` 表示只读新消息，`"0"` 表示从头读取。
    /// - `mkstream`: stream 不存在时是否由 Redis 自动创建。
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

    /// XGROUP CREATE,**幂等**:组已存在(BUSYGROUP)吞掉返回 false,新建返回 true,其余错误上抛
    /// (对齐 原实现 `ignoreBusyGroup`,免调用方手动判错串)。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 要确保存在的消费组名称。
    /// - `id`: 消费组起始读取位置；`"$"` 表示只读新消息，`"0"` 表示从头读取。
    /// - `mkstream`: stream 不存在时是否由 Redis 自动创建。
    pub async fn x_group_create_if_missing(
        &self,
        key: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> Result<bool> {
        match self.x_group_create(key, group, id, mkstream).await {
            Ok(()) => Ok(true),
            Err(e) if format!("{e}").to_uppercase().contains("BUSYGROUP") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// XGROUP DESTROY:销毁消费组,返回是否销毁(1/0)。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 要销毁的消费组名称。
    pub async fn x_group_destroy(&self, key: &str, group: &str) -> Result<u64> {
        self.execute_raw(redis::cmd("XGROUP").arg("DESTROY").arg(key).arg(group))
            .await
    }

    /// XACK:确认组内已处理的 entries,返回实际确认数。
    ///
    /// # 参数
    ///
    /// - `key`: stream 所在的 Redis key。
    /// - `group`: 消费组名称。
    /// - `ids`: 待确认的 entry id 列表；空白 id 会被过滤，过滤后为空则短路返回 `0`。
    pub async fn x_ack(&self, key: &str, group: &str, ids: &[&str]) -> Result<u64> {
        //过滤 blank id,过滤后为空 → no-op `Ok(0)`,不发空 `XACK`
        // (对照 原实现 RedisProxy.ack:空/全 blank 直接返回,否则 Redis arity error)。
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
        self.execute_raw(&cmd).await
    }

    /// XPENDING(summary 形态):返回 `(待确认数, 最小id, 最大id, [(consumer, 持有数),...])`。
    /// 空 PEL 时 min/max 为 None。运维排障用(看积压在谁手里)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `group`: 消费组、服务分组或任务分组名称。
    pub async fn x_pending<T: FromRedisValue>(&self, key: &str, group: &str) -> Result<T> {
        self.execute_raw(redis::cmd("XPENDING").arg(key).arg(group))
            .await
    }

    // ───────────────────────── 阻塞 list(专用连接)─────────────────────────

    /// BLPOP:阻塞左弹出(超时秒;0=永久阻塞)。返回 `(命中的 key, 值)` 或超时 `None`。
    /// ⚠ **不复用共享多路复用连接**(BLOCK 会卡死整条连接的其它命令)——每次派生专用连接,
    /// 命令本身就是要阻塞等待,故不套 `command.timeout_ms`(用 `timeout_secs` 控制)。
    ///
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `timeout_secs`: 阻塞命令等待的超时时间秒数。
    pub async fn bl_pop<T: FromRedisValue>(
        &self,
        keys: &[&str],
        timeout_secs: f64,
    ) -> Result<Option<(String, T)>> {
        self.blocking_pop("BLPOP", keys, timeout_secs).await
    }

    /// BRPOP:阻塞右弹出(同 `bl_pop`)。
    ///
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `timeout_secs`: 阻塞命令等待的超时时间秒数。
    pub async fn br_pop<T: FromRedisValue>(
        &self,
        keys: &[&str],
        timeout_secs: f64,
    ) -> Result<Option<(String, T)>> {
        self.blocking_pop("BRPOP", keys, timeout_secs).await
    }

    // Runs the shared blocking pop implementation.
    ///
    /// # 参数
    /// - `op`: 当前要执行的 Redis 或配置操作。
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `timeout_secs`: 阻塞命令等待的超时时间秒数。
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
        //Cluster 下阻塞命令走 `raw_client()` 派生的**单 seed 多路复用连接**
        //(redis-rs cluster_async 不支持单连接 BLOCK 路由)→ key 不在 seed 槽会拿 `MOVED` 而非阻塞弹出。
        // 规范3 不静默发往 seed:**fail-closed** 明确拒绝,引导改用非阻塞轮询或按 key 节点显式连接。
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
