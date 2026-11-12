// 基于 Redis hash、holder 与租约的分布式锁，通知通道为 `{完整锁 key}:pub`。
//
// 首次 acquire 在服务端持锁，本地 reenter 只增加 permit；最后一个 permit 才能释放服务端锁。
// holder 由进程 UUID 与 guard 序号组成，不依赖线程。Drop 仅尽力停止续租并异步解锁，
// 需要等待释放结果时使用 unlock().await 或 with_lock。
//
// 看门狗按 lease/3 续租，网络不确定时保留 Unknown，仅确定失去 holder 时报告 Lost。
// 等待者在订阅确认后再次争锁，避免遗漏通知；通知丢失时以 PTTL 与总等待期限兜底。

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

// 锁脚本共享 hash holder 协议；修改布局会影响节点之间的互斥。

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

/// 业务作用：惰性生成进程级身份，作为 holder 前缀以隔离不同进程的锁所有权。
///
/// 参数说明：无。
///
/// 返回：本进程共享的 UUID 小写紧凑文本，不随线程或任务改变。
fn instance_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::new_v4().simple().to_string())
}

static GUARD_SEQ: AtomicU64 = AtomicU64::new(0);

/// 三态持有判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldStatus {
    /// 服务端锁仍由当前 holder 持有。
    Held,
    /// 服务端锁已不存在或 holder 不匹配。
    Lost,
    /// 通讯异常——区分"真丢锁"与"网络抖动",调用方按 Unknown 场景表处置。
    Unknown,
}

/// 分布式锁工厂，克隆后共享客户端并保留同一前缀和租约配置。
#[derive(Clone)]
pub struct DistributedLock {
    /// 与业务命令共享的 Redis 客户端。
    client: Arc<RedisClient>,
    /// 加在业务锁名前的稳定键前缀，已含此前缀的输入不重复添加。
    prefix: String,
    /// 服务端租约毫秒数，构造客户端时已限制有效范围。
    lease_ms: u64,
}

/// 同一服务端 holder 的本地重入许可和续租责任。
struct GuardInner {
    /// 续租、持有查询和释放共用的客户端。
    client: Arc<RedisClient>,
    /// 完整锁 key(含 prefix)。
    lock_key: String,
    /// 进程身份与递增序号组成的本次抢锁标识。
    holder: String,
    /// 服务端续租窗口，也是看门狗周期的依据。
    lease_ms: u64,
    /// 本地重入深度(服务端计数恒 1; 本地重入模型)。
    depth: AtomicU32,
    /// 是否已发起服务端释放(防重复 UNLOCK)。
    released: AtomicBool,
    /// 停止本 holder 看门狗的不可逆信号。
    cancel: CancellationToken,
    /// 发布确定失权或看门狗异常退出的通知；没有通知不能证明当前持锁。
    lost_tx: watch::Sender<bool>,
}

impl GuardInner {
    /// 业务作用：尝试服务端 UNLOCK，并在请求结束或等待被取消时停止看门狗。
    /// 先尝试解锁使持有期间续租持续有效；guard 已被消费，随后必须取消续租，避免无人持有的
    /// 孤立看门狗长期占锁。RENEW 仅更新仍存在的 holder，不会复活已删除的锁。
    ///
    /// 参数说明：无。
    ///
    /// 返回：确定释放时成功；请求失败时返回 ExecutionUnknown。取消等待不证明远端释放，
    /// 已提交的续租仍可能完成，残留锁在最后一次服务端续租的租期结束后回收。
    async fn server_unlock(self: &Arc<Self>) -> Result<()> {
        // 最后一份许可已消费，必须在首次等待前接管取消责任；Future 被丢弃也不能遗留持续续租的任务。
        let _stop_on_exit = self.cancel.clone().drop_guard();
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
                // 锁释放已确定；通知失败只影响唤醒延迟，等待者仍需按 PTTL 再次争锁。
                if let Err(e) = self.client.conn().publish::<_, _, i64>(&channel, "").await {
                    tracing::warn!(key = %self.lock_key, err = %e, "解锁后 pub 唤醒失败(等锁者将靠 PTTL 兜底迟醒)");
                }
                Ok(())
            }
            Ok(None) => Err(NasaRedisError::LockNotHeld(self.lock_key.clone())),
            // 释放结局未知时仍结束续租责任；不能把通信失败当作持锁证明，也不能撤销已提交的续租。
            Err(e) => Err(NasaRedisError::ExecutionUnknown(format!(
                "UNLOCK 失败,锁释放结果不确定;已请求停止看门狗,残留锁依赖最后一次服务端续租的租期回收: {e}"
            ))),
        }
    }
}

/// 锁守卫:`reenter()` 重入(共享同一 inner),最后一个 permit 显式 `unlock()` 释放。
///
/// inner 使用 Option，使 unlock 与 Drop 可以通过 take 移交 Arc，保证每份引用只释放一次。
/// 解锁不会遗留持有续租状态的额外引用，守卫销毁仍遵循租约与显式解锁的边界。
pub struct LockGuard {
    /// 当前重入许可持有的共享状态；消费时取走，避免 Drop 重复释放许可。
    inner: Option<Arc<GuardInner>>,
}

impl DistributedLock {
    /// 业务作用：创建分布式锁组件,读取客户端里的锁前缀和 lease 配置。
    ///
    /// 参数说明：
    /// - `client`: Redis 客户端共享句柄。
    ///
    /// 返回：绑定该客户端前缀和租约的锁工厂，此时不争锁或启动看门狗。
    pub fn new(client: Arc<RedisClient>) -> Self {
        let prefix = client.config().lock.prefix.clone();
        let lease_ms = client.config().lock.lease_ms;
        Self {
            client,
            prefix,
            lease_ms,
        }
    }

    /// 业务作用：统一业务锁名和完整锁键的寻址规则，避免对已有前缀的键重复添加前缀。
    ///
    /// 参数说明：
    /// - `key`：业务锁名或完整锁键，以配置前缀开头时始终按完整键解释。
    ///
    /// 返回：保留已有前缀，或拼接一次前缀；业务命名必须避免把需要额外前缀的名称与完整键混用。
    fn full_key(&self, key: &str) -> String {
        if !self.prefix.is_empty() && key.starts_with(&self.prefix) {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }

    /// 业务作用：生成一次抢锁使用的唯一 holder token。
    ///
    /// 参数说明：无。
    ///
    /// 返回：进程实例标识与递增序号组成的 holder，用于续租和释放时匹配所有权；序号回绕前同进程不重复。
    fn new_holder() -> String {
        format!(
            "{}:{}",
            instance_id(),
            GUARD_SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// 业务作用：进行一次异步抢锁，争用时直接返回而不进入等待循环。
    ///
    /// 参数说明：
    /// - `key`: 业务锁 key;可传未加前缀的业务 key 或已完整加前缀的锁 key。
    ///
    /// 返回：取得锁时返回已启动看门狗的守卫，争用时返回 None；通信或脚本失败返回错误，未确认结果不能视为持锁。
    pub async fn try_lock(&self, key: &str) -> Result<Option<LockGuard>> {
        self.try_lock_inner(self.full_key(key), Self::new_holder())
            .await
    }

    /// 业务作用：用已选定的完整键和 holder 执行一次服务端抢占，只有明确成功才建立本地许可。
    ///
    /// 参数说明：
    /// - `lock_key`：已包含锁前缀的完整 Redis 键。
    /// - `holder`：本轮唯一的抢锁身份，同一等待轮的确定失败后重试可以复用。
    ///
    /// 返回：脚本确认成功时建立守卫；返回剩余租期时表示争用，通信或脚本错误直接透传。
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

    /// 业务作用：为已确认取得的服务端 holder 建立首份本地许可，并启动续租和确定失权通知。
    ///
    /// 参数说明：
    /// - `lock_key`：已成功持有的完整 Redis 锁键。
    /// - `holder`：服务端已确认的持有者身份，所有续租和释放都必须匹配它。
    ///
    /// 返回：拥有一份许可的守卫；看门狗异步续租，通信未知不等于持锁证明，业务副作用仍需 fencing。
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
            // 续租任务异常退出时必须通知本地业务失权；正常释放解除通知，进程直接退出则由服务端租约回收。
            /// 看门狗任务退出时的本地失权通知责任。
            struct LostOnExit(
                /// 当前 holder 的失权通知通道。
                watch::Sender<bool>,
                /// 是否仍需在退出时通知；显式释放或已有通知时解除。
                bool,
            );
            impl Drop for LostOnExit {
                /// 业务作用：在 watchdog 异常退出时发布锁丢失信号。
                ///
                /// 参数说明：无。
                ///
                /// 返回：仍处于 armed 状态时发布本地失权通知，避免业务继续依赖已结束的续租维护；
                /// 正常解除后不重复通知，不发 Redis 命令，也不证明远端锁已释放。
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
                                // 服务端明确不再匹配 holder，立即通知本地业务失权并停止继续续租。
                                let _ = wd.lost_tx.send(true);
                                lost_guard.1 = false;
                                tracing::warn!(key = %wd.lock_key, "看门狗:服务端确认锁已丢失,停止续期");
                                break;
                            }
                            // RENEW 报错或超时只能证明结果未知，迟到的命令仍可能完成续租。
                            // 保留看门狗并重试，直到服务端明确返回失权；调用方仍须在业务副作用前复验
                            // holder，RustV2 写入还受 fencing 约束，不能把 Unknown 当作持锁证明。
                            Ok(Err(e)) => tracing::warn!(key = %wd.lock_key, err = %e, "看门狗续期异常(Unknown),继续重试"),
                            Err(_) => tracing::warn!(key = %wd.lock_key, "看门狗续期超时(Unknown),继续重试"),
                        }
                    }
                }
            }
        });
        LockGuard {
            inner: Some(inner),
        }
    }

    /// 业务作用：异步等待锁可用，订阅确认后再次争锁，并以剩余租期补偿丢失的解锁通知。
    ///
    /// 参数说明：
    /// - `key`: 业务锁 key;可传未加前缀的业务 key 或已完整加前缀的锁 key。
    /// - `timeout`：从调用开始计算的获取预算；None 表示持续等待直到成功或出错。
    ///
    /// 返回：成功返回守卫；超时或 Redis 操作失败返回错误。到期后可额外等待一次获取收敛窗口，
    /// 若迟到取得锁还会尝试有限时解锁，未确认的远端状态最终依赖租约回收。
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
                // 独立任务保留在途获取的结果接收责任；到期后仍给一次收敛窗口，迟到取得的锁需要尝试释放。
                let this = self.clone();
                let lk = lock_key.clone();
                let mut task = tokio::spawn(async move {
                    // 循环内部也检查同一截止，争用等待不会为每次重试重新分配预算。
                    this.lock_inner(&lk, Some(d)).await
                });
                match tokio::time::timeout_at(d, &mut task).await {
                    Ok(joined) => match joined {
                        Ok(r) => r, // 任务在 deadline 内完成(成功/错误原样返回)
                        Err(e) => Err(NasaRedisError::LockTimeout(format!(
                            "{lock_key}: 获取任务异常: {e}"
                        ))),
                    },
                    Err(_) => {
                        // 到期后为在途获取保留有限收敛时间；仍未知时停止等待，残留状态依赖服务端租约。
                        let grace = Duration::from_millis((self.lease_ms / 10).clamp(50, 500));
                        match tokio::time::timeout(grace, &mut task).await {
                            // 迟到取得的锁不能交给已超时调用方，另给一次有限窗口尝试释放并保留失败日志。
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
                            // 获取仍未返回时请求中止；不能据此宣称远端尚未执行或锁已释放。
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

    /// 业务作用：按抢锁、确认订阅、再次抢锁和等待通知的顺序重试，避免订阅建立期间丢失释放事件。
    ///
    /// 参数说明：
    /// - `lock_key`：已经包含前缀的完整锁键。
    /// - `deadline`：可选的单调截止，循环开始和等待间隔据此限制继续争锁。
    ///
    /// 返回：确认抢锁成功时返回守卫，循环发现到期则返回 LockTimeout，Redis 失败透传；
    /// 本函数不取消在途单次 Redis 请求，外层负责获取任务的期限与清理。
    async fn lock_inner(
        &self,
        lock_key: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<LockGuard> {
        loop {
            // 在发起下一轮抢占前检查截止，不能因重试重新延长调用方预算。
            if deadline.is_some_and(|dl| tokio::time::Instant::now() >= dl) {
                return Err(NasaRedisError::LockTimeout(lock_key.to_string()));
            }
            // 每轮使用新 holder，避免把上轮不确定的远端获取解释为服务端重入而造成许可计数不一致。
            let holder = Self::new_holder();
            if let Some(g) = self
                .try_lock_inner(lock_key.to_string(), holder.clone())
                .await?
            {
                return Ok(g);
            }
            // 独立订阅连接只接收释放唤醒，不阻塞普通命令连接。
            let channel = format!("{lock_key}:pub");
            let mut pubsub = self.client.raw_client().get_async_pubsub().await?;
            pubsub.subscribe(&channel).await?;
            // 释放可能发生在订阅确认前，确认后再次争锁以免丢失唯一唤醒。
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
            // 键已不存在时短暂等待后重试；无 TTL 属于异常锁状态，以租约周期退避避免高频空转。
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

    /// 业务作用：取得锁后执行异步业务，再显式释放最后一份许可，向调用方传播释放失败。
    ///
    /// 参数说明：
    /// - `key`: 要保护的业务锁 key。
    /// - `timeout`：只约束获取阶段的等待预算，None 表示持续等待。
    /// - `f`: 拿到锁后执行的异步业务逻辑。
    ///
    /// 返回：业务完成且解锁成功后返回其输出；获取或解锁失败返回错误。业务 panic 或外部取消依赖守卫析构，
    /// 本入口不因租约失效自动中断业务，外部副作用仍需 fencing。
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

    /// 业务作用：检查 holder 的三态持有结果；确定持有为 Held，确定缺失为 Lost，查询失败为 Unknown。
    /// `full_key` 保持幂等；即使误传已含前缀的 `guard.lock_key()`，也不会因双前缀而恒判
    /// Lost;持有自己锁的便捷查询仍推荐 `guard.still_held()`(零前缀风险)。
    ///
    /// 参数说明：
    /// - `key`: 要检查的业务锁 key 或完整锁 key。
    /// - `holder`: 持锁方 token,通常来自 [`LockGuard::holder`]。
    ///
    /// 返回：查询时 holder 匹配为 Held，不匹配为 Lost，通信或脚本错误为 Unknown；快照不能代替后续写入的 fencing。
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
    /// 业务作用：缺少任务退出证明时只停止续租，禁止析构路径提前主动解锁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：消费守卫但不发送 UNLOCK，服务端租约按自身期限结束。
    pub(crate) fn abandon(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.cancel.cancel();
            inner.released.store(true, Ordering::Release);
        }
    }

    /// 业务作用：借用尚未消费许可的共享 holder 状态，供守卫查询和重入复用同一所有权身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享状态引用；许可已被内部消费却仍访问时触发不变量断言。
    #[inline]
    fn arc(&self) -> &Arc<GuardInner> {
        self.inner
            .as_ref()
            .expect("LockGuard inner accessed after unlock/drop")
    }

    /// 业务作用：返回当前锁持有者 token，供续租和解锁脚本复验所有权。
    ///
    /// 参数说明：无。
    ///
    /// 返回：借用本次抢锁身份；该标识本身不证明租约仍然有效。
    pub fn holder(&self) -> &str {
        &self.arc().holder
    }

    /// 业务作用：返回当前 guard 保护的 Redis key。
    ///
    /// 参数说明：无。
    ///
    /// 返回：借用已包含配置前缀的完整锁键，不查询持有状态。
    pub fn lock_key(&self) -> &str {
        &self.arc().lock_key
    }

    /// 业务作用：用守卫自带的完整键和 holder 查询当前持有状态，避免调用方提供不匹配的身份组合。
    ///
    /// 参数说明：无。
    ///
    /// 返回：查询时匹配为 Held，不匹配为 Lost，查询失败为 Unknown；结果只是观测，不能保证后续副作用仍排他。
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

    /// 业务作用：订阅当前 holder 的本地失权信号，供业务在确定失锁或续租任务异常退出时停止工作。
    ///
    /// 参数说明：无。
    ///
    /// 返回：独立 watch 接收端；true 表示已经通知失权，false 不证明远端仍持锁，正常释放不保证发送 true。
    pub fn lost(&self) -> watch::Receiver<bool> {
        self.arc().lost_tx.subscribe()
    }

    /// 业务作用：为同一 holder 增加本地重入许可，不额外发送服务端抢锁或延长租约。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享内部状态的新守卫，所有许可结束后才允许远端释放；不得对已停止释放责任的守卫重入。
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

    /// 业务作用：消费当前重入许可，只有最后一份许可才提交服务端释放并尝试通知等待者。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仍有其它许可时返回 StillReentered 且本许可已消费；最后一份释放成功时完成。
    /// 远端已不匹配或释放结局未知返回对应错误，不能据此恢复已消费的许可。
    pub async fn unlock(mut self) -> Result<()> {
        // 把 Arc move 出(self.inner 置 None):本方法负责这一份 permit 的 depth 递减,
        // 随后 self 走 Drop 时因 inner 为 None 不再重复递减，`inner` 在函数结束时释放这一份引用。
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
    /// 业务作用：销毁本地许可，最后一份未显式释放的许可停止续租并尽力安排远端解锁。
    ///
    /// 参数说明：无。
    ///
    /// 返回：同步消费许可；可用运行时存在时安排异步解锁，否则依赖租约到期，不保证析构时远端锁已经释放。
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
