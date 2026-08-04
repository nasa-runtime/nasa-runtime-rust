// ============================================================================
// src/lock.rs —— 分布式锁(文档;对照 原实现 LettuceDistributedLock)。
//
// V1 互锁保证:4 个 Lua **逐字节照搬** 原实现(hash 结构 + holder field + 重入计数),
// 原实现/Rust 节点可竞争同一把锁;解锁通知通道 = `{完整锁 key}:pub`(原实现 :444 同款)。
//
// 与既有实现的显式差异如下，均为稳定语义：
//   · 本地重入:首次 acquire 仅一次服务端 LOCK(服务端计数恒 1);`guard.reenter()` 只加
//     本地 permit;最后一个 permit 显式 unlock 才发服务端 UNLOCK;提前 unlock 返回
//     Err(StillReentered)。互斥语义与 原实现 一致,崩溃残留同靠 lease 过期回收。
//   · holder = INSTANCE_ID(进程 uuid):guard_seq——async 无线程亲和,不用 threadId;
//   · Drop 仅 best-effort(取消看门狗 + spawn UNLOCK),正确性入口 = 显式
//     unlock().await / with_lock();
//   · 看门狗:每锁一个 task,interval(lease/3) 调 RENEW;网络异常保留 Unknown 重试,
//     仅服务端明确返回 0 才判 Lost(watch 通知);CancellationToken 取消即终结,
//     Guard 不复用 → 无 原实现 的 generation/released 幽灵续期问题;
//   · 等锁(TOCTOU 全保留):订阅 `{lockkey}:pub` **确认就绪后立即再抢一次**,再
//     select!(消息, sleep(PTTL 兜底), 总超时)。
// ============================================================================

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::StreamExt;
use redis::AsyncCommands;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::config::MAX_REDIS_RUNTIME_DURATION_MS;
use crate::error::{NasaRedisError, Result};

// ── 4 个 Lua:与 原实现 LettuceDistributedLock 逐字节一致(:64/:87/:109/:128)──

const LOCK_LUA: &str = r#"if redis.call('exists', KEYS[1]) == 0 then
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return redis.call('pttl', KEYS[1])
"#;

const UNLOCK_LUA: &str = r#"if redis.call('hexists', KEYS[1], ARGV[2]) == 0 then
    return nil
end
local count = redis.call('hincrby', KEYS[1], ARGV[2], -1)
if count > 0 then
    redis.call('pexpire', KEYS[1], ARGV[1])
    return 0
end
redis.call('del', KEYS[1])
return 1
"#;

const RENEW_LUA: &str = r#"if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    redis.call('pexpire', KEYS[1], ARGV[1])
    return 1
end
return 0
"#;

const HOLDS_LUA: &str = r#"if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    return 1
end
return 0
"#;

/// 进程级实例标识(对照 原实现 INSTANCE_ID;Rust 端每次启动唯一,不落盘)。
fn instance_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::new_v4().simple().to_string())
}

static GUARD_SEQ: AtomicU64 = AtomicU64::new(0);

/// 三态持有判定(对照 原实现 holdsStatus:1 / 0 / 异常→null)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldStatus {
    /// 服务端锁仍由当前 holder 持有。
    Held,
    /// 服务端锁已不存在或 holder 不匹配。
    Lost,
    /// 通讯异常——区分"真丢锁"与"网络抖动",调用方按 Unknown 场景表处置。
    Unknown,
}

/// 分布式锁工厂(对照 原实现 LettuceDistributedLock 实例)。
/// Clone 廉价(Arc + String + u64),供 `lock()` 超时路径 spawn 可取消安全的获取任务(R-P1i)。
#[derive(Clone)]
pub struct DistributedLock {
    client: Arc<RedisClient>,
    prefix: String,
    lease_ms: u64,
}

/// 保存内部共享状态；用于在多个调用路径之间复用数据。
struct GuardInner {
    client: Arc<RedisClient>,
    /// 完整锁 key(含 prefix)。
    lock_key: String,
    holder: String,
    lease_ms: u64,
    /// 本地重入深度(服务端计数恒 1; 本地重入模型)。
    depth: AtomicU32,
    /// 是否已发起服务端释放(防重复 UNLOCK)。
    released: AtomicBool,
    cancel: CancellationToken,
    lost_tx: watch::Sender<bool>,
}

impl GuardInner {
    /// 服务端 UNLOCK + pub 唤醒(unlock 与 Drop 共用)。
    ///
    ///先 UNLOCK 再 **无条件** cancel 看门狗,而非"先 cancel 再
    /// UNLOCK"。理由:
    ///   1. guard 已被 `unlock(self)` 消费,返回后无重试路径、无 `lost` 订阅者——所以无论 UNLOCK
    ///      成功与否都**必须** cancel,否则看门狗成孤儿,对一把已无 guard 的锁续租到进程死。
    ///   2. 先 cancel 会留窗口:看门狗停转但 UNLOCK 未落,期间锁"无人续租";先 UNLOCK 则锁一直被
    ///      正常续租直到真正释放那一刻,语义更干净(要么释放、要么仍被续租)。
    ///   3. UNLOCK 成功后即便有在途 RENEW 完成也无害:`RENEW_LUA` 以 `hexists(holder)==1` 为条件,
    ///      锁已删则返 0,绝不会复活已删锁或窃他人锁(无双持有风险)。
    ///
    ///UNLOCK 命令本身报错(连接抖动)时,**释放结果不确定**——命令可能已落、
    /// 也可能没落。本 guard 已消费、无重试路径,故仍 cancel 看门狗(避免孤儿续租,见 1.),锁由 lease
    /// 兜底过期;并返回 `ExecutionUnknown`(而非裸 Redis 错误)让调用方知"可能未释放、可能已释放"。
    /// (注:原实现 unlock 在 UNLOCK 失败时**不停看门狗**让锁继续被续租;Rust 因 `unlock(self)` 消费
    /// guard、无法保留续租通道,因此选择 cancel + lease 兜底,并把不确定释放状态显式返回给调用方。)
    async fn server_unlock(self: &Arc<Self>) -> Result<()> {
        let r: std::result::Result<Option<i64>, redis::RedisError> = redis::Script::new(UNLOCK_LUA)
            .key(&self.lock_key)
            .arg(self.lease_ms)
            .arg(&self.holder)
            .invoke_async(&mut self.client.conn())
            .await;
        self.cancel.cancel(); // 无条件停看门狗(guard 已消费,绝不留孤儿续租)
        match r {
            Ok(Some(_)) => {
                let channel = format!("{}:pub", self.lock_key);
                //pub 丢失不再静默吞——失败时等锁者只能靠 PTTL 兜底迟醒(放大等锁延迟),
                // 留 warn 便于排障(非正确性问题)。
                if let Err(e) = self.client.conn().publish::<_, _, i64>(&channel, "").await {
                    tracing::warn!(key = %self.lock_key, err = %e, "解锁后 pub 唤醒失败(等锁者将靠 PTTL 兜底迟醒)");
                }
                Ok(())
            }
            Ok(None) => Err(NasaRedisError::LockNotHeld(self.lock_key.clone())),
            //UNLOCK 命令出错——释放结果不确定(可能已删/可能没删)。**看门狗已停**
            // (上方无条件 cancel),锁不再续租,将于 ≤lease 后自然过期;调用方勿误以为仍持有/仍在续租。
            Err(e) => Err(NasaRedisError::ExecutionUnknown(format!(
                "UNLOCK 失败,锁释放结果不确定;看门狗已停、锁将于 ≤lease 后过期(不再续租): {e}"
            ))),
        }
    }
}

/// 锁守卫:`reenter()` 重入(共享同一 inner),最后一个 permit 显式 `unlock()` 释放。
///
/// `inner` 用 `Option` 是为了让 `unlock`/`Drop` 能把 Arc **安全 move 出**(`take()`)后各自恰好
/// 释放一份引用——避免历史上 `Arc::clone(&self.inner) + mem::forget(self)` 的净泄漏(clone +1、
/// forget 跳过原件 -1,每次 unlock 泄漏一整个 `GuardInner`)。全安全实现,保持本 crate 零 unsafe。
pub struct LockGuard {
    inner: Option<Arc<GuardInner>>,
}

impl DistributedLock {
    /// 创建分布式锁组件,读取客户端里的锁前缀和 lease 配置。
    ///
    /// # 参数
    /// - `client`: Redis 客户端共享句柄。
    pub fn new(client: Arc<RedisClient>) -> Self {
        let prefix = client.config().lock.prefix.clone();
        let lease_ms = client.config().lock.lease_ms;
        Self {
            client,
            prefix,
            lease_ms,
        }
    }

    /// 业务 key → 完整锁 key。**幂等**:若 `key` 已以
    /// `self.prefix`(`DISTRIBUTED-LOCK:` 等,distinctive)开头,原样返回不再叠前缀——这样误把
    /// `guard.lock_key()`(已含前缀的完整 key)传给 `holds_status`/`lock`/`try_lock` 不再双前缀
    /// 恒判 Lost / 抢错锁,所有锁 API 对"已含前缀的 key"行为一致。业务 key 不会以锁前缀开头,故无误伤。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    fn full_key(&self, key: &str) -> String {
        if !self.prefix.is_empty() && key.starts_with(&self.prefix) {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }

    /// 生成一次抢锁使用的唯一 holder token。
    ///
    /// holder 由进程实例标识和本进程递增序号组成,用于 Lua 续租/释放时确认“锁仍由自己持有”。
    fn new_holder() -> String {
        format!(
            "{}:{}",
            instance_id(),
            GUARD_SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// 非阻塞抢锁。Ok(Some(guard)) = 成功(看门狗已启动);Ok(None) = 他人持有。
    ///
    /// # 参数
    /// - `key`: 业务锁 key;可传未加前缀的业务 key 或已完整加前缀的锁 key。
    pub async fn try_lock(&self, key: &str) -> Result<Option<LockGuard>> {
        self.try_lock_inner(self.full_key(key), Self::new_holder())
            .await
    }

    // Attempts to acquire the lock with the prepared holder token.
    ///
    /// # 参数
    /// - `lock_key`: 业务 key 或 Redis key,用于定位数据。
    /// - `holder`: 当前分布式锁持有者 token。
    async fn try_lock_inner(&self, lock_key: String, holder: String) -> Result<Option<LockGuard>> {
        let r: Option<i64> = redis::Script::new(LOCK_LUA)
            .key(&lock_key)
            .arg(self.lease_ms)
            .arg(&holder)
            .invoke_async(&mut self.client.conn())
            .await?;
        match r {
            None => Ok(Some(self.spawn_guard(lock_key, holder))),
            Some(_pttl) => Ok(None),
        }
    }

    // Builds a guard and starts lease maintenance.
    ///
    /// # 参数
    /// - `lock_key`: 业务 key 或 Redis key,用于定位数据。
    /// - `holder`: 当前分布式锁持有者 token。
    fn spawn_guard(&self, lock_key: String, holder: String) -> LockGuard {
        let (lost_tx, _) = watch::channel(false);
        let inner = Arc::new(GuardInner {
            client: Arc::clone(&self.client),
            lock_key,
            holder,
            lease_ms: self.lease_ms,
            depth: AtomicU32::new(1),
            released: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            lost_tx,
        });
        let wd = Arc::clone(&inner);
        tokio::spawn(async move {
            //**task 退出(含 panic/挂死被 drop)兜底发 lost**——否则续租链
            // 静默死亡时 `lost()` 永远 false、业务以为仍持有 → 与新 owner 双持有破坏互斥。
            // 干净退出(显式 unlock 触发 cancel)时 disarm 不误发 lost。
            //
            //3(panic 策略):本守卫靠 Drop 兜底,**在 `panic=unwind`(Rust/本仓默认)
            // 下,task panic 会展开栈、Drop 照常执行 → lost 正常发出**(unwind 期正是 Drop 触发时机)。
            // 仅在 `panic=abort` 下 Drop 不跑——但那时**整个进程已 abort**,持锁的业务进程一并消失,
            // 不存在"本地仍以为持锁却与新 owner 双持有"的场景,锁由 lease 过期兜底清理(moot-but-safe,
            // 非"比静默更糟")。若部署改 `panic=abort`,应同时确保锁的 lease 足够短以快速回收;此假设
            // 若部署改 `panic=abort`,需用足够短的 lease 限制异常退出后的锁残留时间。
            /// watchdog 任务退出时的 lost 兜底发送器。
            ///
            /// 第二个字段为是否 armed；干净释放锁时会解除 armed,异常退出或 panic 展开时会通知业务锁已丢失。
            struct LostOnExit(watch::Sender<bool>, bool);
            impl Drop for LostOnExit {
                /// 在 watchdog 异常退出时发布锁丢失信号。
                ///
                /// 如果守卫仍处于 armed 状态,说明不是正常 unlock/cancel 路径,必须让业务侧看到 `lost()`。
                fn drop(&mut self) {
                    if self.1 {
                        let _ = self.0.send(true);
                    }
                }
            }
            let mut lost_guard = LostOnExit(wd.lost_tx.clone(), true);

            let interval = Duration::from_millis((wd.lease_ms / 3).max(1));
            let mut tick = tokio::time::interval(interval);
            // Skip:错过的 tick 不顺延累积(Delay 会让有效续租间隔逼近/超过 lease)
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // interval 首跳立即完成,跳过
            loop {
                tokio::select! {
                    _ = wd.cancel.cancelled() => {
                        lost_guard.1 = false; // 干净退出(显式 unlock):不发 lost
                        break;
                    }
                    _ = tick.tick() => {
                        // RENEW 套**单次 timeout**(略短于 tick),不让续租 await 挂死整轮——抖动/重连期
                        // 长 pending 会让续租迟到;timeout 让看门狗按 tick 节奏继续推进。
                        let mut conn = wd.client.conn();
                        let script = redis::Script::new(RENEW_LUA);
                        let mut invocation = script.prepare_invoke();
                        invocation.key(&wd.lock_key).arg(wd.lease_ms).arg(&wd.holder);
                        let renew = invocation.invoke_async::<i64>(&mut conn);
                        let r = tokio::time::timeout(interval, renew).await;
                        match r {
                            Ok(Ok(1)) => {} // 续期成功
                            Ok(Ok(_)) => {
                                // **服务端明确返 0**:锁已不归本 holder(过期被抢/被删)→ 真 Lost(definitive)。
                                // 这是**唯一**判 lost 的依据(对齐 原实现 + still_held/holds_status 的"Unknown≠真丢锁")。
                                let _ = wd.lost_tx.send(true);
                                lost_guard.1 = false;
                                tracing::warn!(key = %wd.lock_key, "看门狗:服务端确认锁已丢失,停止续期");
                                break;
                            }
                            //(CLIENT PAUSE 实测修正):RENEW 报错/超时 = **Unknown**,
                            // **绝不判 lost、绝不 break**,只 log + 下轮继续续租(对齐 原实现:网络异常只重试)。
                            // 原"连续 2 失败降级 lost + break"会:① 对覆盖 ≤lease 的瞬时抖动**误报 lost**→
                            // coordinator 误下健康分区致 rebalance 抖动;② break 后永不恢复续租,把可恢复抖动
                            // 变成不可逆丢锁(被暂停的 RENEW 在 pause 结束后仍会迟到执行、续上锁,实测锁仍在)。
                            // 真分区下本节点也连不上 Redis、无法造成双写危害(且 RustV2 fencing 兜底陈旧写),
                            // 分区愈合后 RENEW 会返回 1（锁仍在）或 0（已被抢占），返回 0 时再判定 lost。
                            Ok(Err(e)) => tracing::warn!(key = %wd.lock_key, err = %e, "看门狗续期异常(Unknown),继续重试"),
                            Err(_) => tracing::warn!(key = %wd.lock_key, "看门狗续期超时(Unknown),继续重试"),
                        }
                    }
                }
            }
        });
        LockGuard { inner: Some(inner) }
    }

    /// 阻塞式加锁。TOCTOU 防御:订阅就绪后立即再抢,再 select!(通知, PTTL 兜底)。
    ///
    ///`timeout` 是**绝对 deadline**,覆盖整个获取过程——try_lock(SET NX)、
    /// `get_async_pubsub`、`subscribe`、`PTTL`、等待**全部**纳入。此前 deadline 只在等待 select!
    /// 内生效,任一 Redis await 挂死(连接抖动/重连)会让 `lock(key, Some(1s))` 远超 1s。
    ///
    /// # 参数
    /// - `key`: 业务锁 key;可传未加前缀的业务 key 或已完整加前缀的锁 key。
    /// - `timeout`: 获取锁的绝对超时;`None` 表示一直等待直到拿到锁或出错。
    pub async fn lock(&self, key: &str, timeout: Option<Duration>) -> Result<LockGuard> {
        let lock_key = self.full_key(key);
        match timeout {
            Some(t) => {
                if t > Duration::from_millis(MAX_REDIS_RUNTIME_DURATION_MS) {
                    return Err(NasaRedisError::Config(format!(
                        "lock timeout 超过运行时上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms"
                    )));
                }
                let d = tokio::time::Instant::now().checked_add(t).ok_or_else(|| {
                    NasaRedisError::Config("lock timeout 无法表示为 Tokio deadline".into())
                })?;
                //不再 `timeout_at(d, lock_inner)` 直接在 deadline
                // 处 drop 在途 LOCK——那会把"服务端已加锁但本地 future 被取消"留成幽灵锁(无 guard
                // 可释放,只能等 lease 过期)。改为 **spawn** 获取任务(可取消安全)+ 内层自感知 deadline。
                // **返回时机(修正)**:
                //   · 争用未果(常态):内层 lock_inner 在 napping/重试间到点自返 LockTimeout → 在 **~t** 返回;
                //   · 某次 redis await 真挂死(连接抖动):内层卡在 await 无法自返 → 外层 timeout_at 命中后给
                //     一小段 grace 让**那一次在途 LOCK** 收敛:若拿到 guard 则持有它干净 `unlock()`(绝无幽灵)
                //     后返 LockTimeout;grace 内仍未收敛才 abort,残留幽灵由 A-6 全新 holder 兜底 ≤lease 自愈。
                // 即上界为 **deadline + grace**(grace 仅覆盖挂死那一次 LOCK 的 1-RTT 收敛,非每次都吃满)。
                let this = self.clone();
                let lk = lock_key.clone();
                // 把 deadline 传入内层——争用场景内层到点自返 LockTimeout(在 ~t),外层 timeout_at+grace
                // 此后只在"某次 redis await 真挂死"时兜底(grace 让在途 LOCK 1-RTT 收敛,防幽灵)。
                let mut task = tokio::spawn(async move { this.lock_inner(&lk, Some(d)).await });
                match tokio::time::timeout_at(d, &mut task).await {
                    Ok(joined) => match joined {
                        Ok(r) => r, // 任务在 deadline 内完成(成功/错误原样返回)
                        Err(e) => Err(NasaRedisError::LockTimeout(format!(
                            "{lock_key}: 获取任务异常: {e}"
                        ))),
                    },
                    Err(_) => {
                        // deadline 到:给 grace 让在途 LOCK 收敛,避免幽灵锁(lease/10,夹 [50,500]ms)
                        let grace = Duration::from_millis((self.lease_ms / 10).clamp(50, 500));
                        match tokio::time::timeout(grace, &mut task).await {
                            // grace 内拿到锁:持有 guard,干净 unlock 后守 deadline 契约报超时。
                            //unlock 套小 timeout(不让释放再挂死突破 deadline 契约)+
                            // 失败打 warn(不静默吞 ExecutionUnknown——释放不确定也要留痕,锁由 lease 兜底)。
                            Ok(Ok(Ok(g))) => {
                                let lk = lock_key.clone();
                                match tokio::time::timeout(grace, g.unlock()).await {
                                    Ok(Ok(())) => {}
                                    Ok(Err(e)) => {
                                        tracing::warn!(key = %lk, err = %e, "等锁超时后释放刚获取的锁失败(lease 兜底)")
                                    }
                                    Err(_) => {
                                        tracing::warn!(key = %lk, "等锁超时后释放刚获取的锁超时(lease 兜底)")
                                    }
                                }
                                Err(NasaRedisError::LockTimeout(lock_key))
                            }
                            // grace 内完成但未拿到(错误/None 不会到这,lock_inner 只在拿到/出错时返回)
                            Ok(Ok(Err(_))) | Ok(Err(_)) => {
                                Err(NasaRedisError::LockTimeout(lock_key))
                            }
                            // grace 也耗尽(连接挂死):abort,残留幽灵 ≤lease 自愈
                            Err(_) => {
                                task.abort();
                                Err(NasaRedisError::LockTimeout(lock_key))
                            }
                        }
                    }
                }
            }
            None => self.lock_inner(&lock_key, None).await,
        }
    }

    /// 加锁重试循环。`deadline`:`Some` 时争用场景内层**自感知 deadline**(napping/重试**之间**到点即返
    /// `LockTimeout`,使 `lock(k, Some(t))` 在 ~t 返回——不再被外层 grace 走满);`None` 时无限阻塞。
    /// **关键(防幽灵)**:deadline 只在 try_lock await **之间**检查、并把 nap 夹到剩余预算,**绝不取消在途
    /// 的一次 LOCK(SET NX)** → 不会留幽灵锁。真正卡在某次 redis await(连接挂死)时由 `lock` 外层
    /// `timeout_at + grace + abort` 兜底(grace 让那一次在途 LOCK 1-RTT 收敛后干净释放)。
    ///
    /// # 参数
    /// - `lock_key`: 业务 key 或 Redis key,用于定位数据。
    /// - `deadline`: 关闭或等待的绝对截止时间。
    async fn lock_inner(
        &self,
        lock_key: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<LockGuard> {
        loop {
            // 争用未果时内层到点自返——只在 try_lock **之间**检查(不打断在途 LOCK,无幽灵)。
            if deadline.is_some_and(|dl| tokio::time::Instant::now() >= dl) {
                return Err(NasaRedisError::LockTimeout(lock_key.to_string()));
            }
            //**每轮等待用全新 holder**——等锁循环本不该"重入"。若上一轮 try_lock
            // 实际在服务端加了锁但本地状态丢失(timeout 取消/响应丢失,ExecutionUnknown),复用同
            // holder 会让本轮 LOCK_LUA 命中 `hexists==1` 误判为**重入** hincrby 到 2,而 Rust 是全新
            // guard(depth=1)→ 服务端计数 2 / 本地 1 永久错位,锁到 lease 才释放。全新 holder 下:
            // 本轮 SET 失败(锁被旧 holder 持有)→ 正常等待,旧 holder 锁自然过期后再抢,无错位。
            let holder = Self::new_holder();
            if let Some(g) = self
                .try_lock_inner(lock_key.to_string(), holder.clone())
                .await?
            {
                return Ok(g);
            }
            // 独立 Pub/Sub 连接订阅解锁通道(pub")
            let channel = format!("{lock_key}:pub");
            let mut pubsub = self.client.raw_client().get_async_pubsub().await?;
            pubsub.subscribe(&channel).await?;
            // ★ TOCTOU:订阅确认就绪后立即再抢(pub 可能发生在订阅完成前)
            if let Some(g) = self
                .try_lock_inner(lock_key.to_string(), holder.clone())
                .await?
            {
                return Ok(g);
            }
            // PTTL 兜底:锁自然过期也能醒来重试
            let pttl: i64 = self
                .client
                .conn()
                .pttl(lock_key)
                .await
                .unwrap_or(self.lease_ms as i64);
            //区分 PTTL 负值语义,不再把 -1/-2 一律 clamp 成 20ms 高频空转:
            //   -2 = 锁已不存在(刚被释放/过期)→ 短 nap 立即重抢;
            //   -1 = 锁存在但无 TTL(异常态:非本库所设/SET 漏 PX)→ 退避一个 lease 周期,避免空转;
            //   >=0 = 实际剩余 TTL → clamp 到 [20, lease]。
            let nap = Duration::from_millis(match pttl {
                -2 => 20,
                -1 => self.lease_ms,
                ms if ms >= 0 => (ms as u64).clamp(20, self.lease_ms),
                _ => 20,
            });
            // 把 nap 夹到剩余预算——否则可能在一个长 nap 里"睡过" deadline,导致 lock(k,Some(t)) 远超 t。
            // 夹紧后到点醒来 → 下轮 loop 顶部即返 LockTimeout(争用场景在 ~t 返回)。
            let nap = match deadline {
                Some(dl) => nap.min(dl.saturating_duration_since(tokio::time::Instant::now())),
                None => nap,
            };
            let mut msgs = pubsub.on_message();
            tokio::select! {
                _ = msgs.next() => {}
                _ = tokio::time::sleep(nap) => {}
            }
            // 下一轮重试;pubsub 随作用域 drop 自动退订
        }
    }

    /// 模板:加锁 → 执行 → 显式解锁(推荐入口)。
    ///
    /// # 参数
    /// - `key`: 要保护的业务锁 key。
    /// - `timeout`: 获取锁的绝对超时;`None` 表示一直等待。
    /// - `f`: 拿到锁后执行的异步业务逻辑。
    pub async fn with_lock<F, Fut, T>(
        &self,
        key: &str,
        timeout: Option<Duration>,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let guard = self.lock(key, timeout).await?;
        let out = f().await;
        guard.unlock().await?;
        Ok(out)
    }

    /// 三态持有检测(HOLDS_LUA + 异常→Unknown;对照 原实现 holdsStatus)。
    /// `full_key` 保持幂等；即使误传已含前缀的 `guard.lock_key()`，也不会因双前缀而恒判
    /// Lost;持有自己锁的便捷查询仍推荐 `guard.still_held()`(零前缀风险)。
    ///
    /// # 参数
    /// - `key`: 要检查的业务锁 key 或完整锁 key。
    /// - `holder`: 持锁方 token,通常来自 [`LockGuard::holder`]。
    pub async fn holds_status(&self, key: &str, holder: &str) -> HoldStatus {
        let r: std::result::Result<i64, redis::RedisError> = redis::Script::new(HOLDS_LUA)
            .key(self.full_key(key))
            .arg(holder)
            .invoke_async(&mut self.client.conn())
            .await;
        match r {
            Ok(1) => HoldStatus::Held,
            Ok(_) => HoldStatus::Lost,
            Err(_) => HoldStatus::Unknown,
        }
    }
}

impl LockGuard {
    /// 取内部共享态。`inner` 只在 `unlock`/`Drop` 消费 self 时被 `take()`,而那两处消费后不再访问
    /// self,故常规方法调用期间恒为 `Some`;`expect` 是不可达的编程错误兜底。
    #[inline]
    fn arc(&self) -> &Arc<GuardInner> {
        self.inner
            .as_ref()
            .expect("LockGuard inner accessed after unlock/drop")
    }

    /// Returns the lock holder token.
    pub fn holder(&self) -> &str {
        &self.arc().holder
    }

    /// Returns the Redis key protected by this guard.
    pub fn lock_key(&self) -> &str {
        &self.arc().lock_key
    }

    /// 本 guard 当前是否仍由本持有者持有(消除双前缀 footgun)。
    /// **直接用本 guard 的完整 lock_key + holder**,不经 `DistributedLock::holds_status`(后者对
    /// 传入 key 再做一次 `full_key` → 若误传 `guard.lock_key()`(已含前缀)会双前缀恒判 Lost)。
    /// 业务"我还持有这把锁吗"应优先用本方法,而非 `holds_status(guard.lock_key(), ...)`。
    pub async fn still_held(&self) -> HoldStatus {
        let r: std::result::Result<i64, redis::RedisError> = redis::Script::new(HOLDS_LUA)
            .key(&self.arc().lock_key) // 已是完整 key,不再 full_key
            .arg(&self.arc().holder)
            .invoke_async(&mut self.arc().client.conn())
            .await;
        match r {
            Ok(1) => HoldStatus::Held,
            Ok(_) => HoldStatus::Lost,
            Err(_) => HoldStatus::Unknown,
        }
    }

    /// 锁丢失通知(看门狗 RENEW 明确返 0 → true)。
    pub fn lost(&self) -> watch::Receiver<bool> {
        self.arc().lost_tx.subscribe()
    }

    /// 显式重入:共享同一 GuardInner,本地深度 +1,**不发服务端 LOCK**。
    /// ⚠ 契约:不得对**已最终释放**的 guard 重入(否则 fetch_add 产生幽灵 guard,其后续 unlock 返
    /// LockNotHeld)。debug 构建断言捕获此误用(release 构建不付出运行时开销)。
    pub fn reenter(&self) -> LockGuard {
        debug_assert!(
            !self.arc().released.load(Ordering::Acquire),
            "对已释放的 LockGuard reenter():幽灵 guard(契约违反)"
        );
        self.arc().depth.fetch_add(1, Ordering::AcqRel);
        LockGuard {
            inner: Some(Arc::clone(self.arc())),
        }
    }

    /// 显式解锁(正确性入口)。仍有其他重入 permit → Err(StillReentered)(本 permit 已消费,
    /// 服务端锁未动);最后一个 permit → 停看门狗 + 服务端 UNLOCK + pub 唤醒等锁者。
    pub async fn unlock(mut self) -> Result<()> {
        // 把 Arc move 出(self.inner 置 None):本方法负责这一份 permit 的 depth 递减,
        // 随后 self 走 Drop 时因 inner 为 None 不再重复递减。`inner` 在函数结束时正好释放一份引用,
        // 无泄漏(旧实现 clone+forget 会净泄漏一份)。
        let inner = self
            .inner
            .take()
            .expect("LockGuard::unlock called after inner was already taken");
        let prev = inner.depth.fetch_sub(1, Ordering::AcqRel);
        if prev > 1 {
            return Err(NasaRedisError::StillReentered {
                remaining: prev - 1,
            });
        }
        if inner.released.swap(true, Ordering::AcqRel) {
            return Err(NasaRedisError::LockNotHeld(inner.lock_key.clone()));
        }
        inner.server_unlock().await
    }
}

impl Drop for LockGuard {
    /// best-effort:最后一个 permit 被 Drop(未显式 unlock)→ 取消看门狗 + spawn UNLOCK。
    /// 不承诺确定释放(runtime 关闭时 spawn 可能不执行),兜底 = lease 过期。
    fn drop(&mut self) {
        // unlock 已 take 过则 inner 为 None(depth 由 unlock 递减完毕),此处不再重复递减。
        let Some(inner) = self.inner.take() else {
            return;
        };
        if inner.depth.fetch_sub(1, Ordering::AcqRel) == 1
            && !inner.released.swap(true, Ordering::AcqRel)
        {
            inner.cancel.cancel();
            tracing::warn!(key = %inner.lock_key, "LockGuard 被 Drop(未显式 unlock),best-effort 释放");
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = inner.server_unlock().await;
                });
            }
        }
    }
}
