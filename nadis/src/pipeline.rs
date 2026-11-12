// 显式 PipelineSession 与多生产者自动微批。
//
// 默认会话在条数或字节阈值达到后封批，后台串行提交各段；execute 等待全部段并发送尾批。
// 同一 slot 内按入队顺序执行，跨 slot 或与 direct 命令混用时不提供全局顺序。
// 每条服务器错误只影响对应 ticket；传输失败表示结果不确定，不能据此安全重发写命令。
// 丢弃会话只将尚未封批的命令标为 NotExecuted，已提交后台的段继续执行。

use std::sync::Arc;
use std::time::Duration;

use redis::FromRedisValue;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::config::PipelineCfg;
use crate::error::{NasaRedisError, Result};

/// Hash 字段自减及非正余值删除脚本，KEYS 为键，ARGV 为字段与已校验可取负的 i64 减量。
/// 步长和余值始终使用十进制文本，避免 Lua 数字舍入改变持久增量或回包；返回删除前的精确余值文本。
const H_DECR_BY_AND_DEL_LUA: &str = r#"
local k = KEYS[1]
local hk = ARGV[1]
local delta = ARGV[2]
local increment
-- 调用入口已校验有符号范围；这里只改变文本符号，不让完整整数经过 Lua 浮点数。
if delta == '0' then
    increment = '0'
elseif string.sub(delta, 1, 1) == '-' then
    increment = string.sub(delta, 2)
else
    increment = '-' .. delta
end
-- 精确回读要求 HGET 权限；先执行只读检查，避免因缺少读取权限而在减量之后留下失败结果。
redis.call('HGET', k, hk)
-- HINCRBY 负责整数合法性和溢出裁决；忽略其 Lua 数字回包，从持久字段读取精确十进制结果。
redis.call('HINCRBY', k, hk, increment)
local value = redis.call('HGET', k, hk)
if value == '0' or string.sub(value, 1, 1) == '-' then
    -- 先保留余值再删除字段，使最后一个字段被删时仍能返回本次更新结果。
    redis.call('HDEL', k, hk)
end
return value
"#;

/// 单条命令的失败形态(经 oneshot 分发;RedisError 不 Clone,传输错误需广播给
/// 整批 ticket,故自定义枚举承载)。
#[derive(Debug, Clone)]
enum TicketErr {
    /// 服务器已答复的确定性失败(WRONGTYPE 等)——仅该命令失败,同批其余不受污染。
    Server(redis::ServerError),
    /// 传输层失败(写出后断线/超时):该命令「可能已执行」。
    Unknown(String),
    /// 命令仍由会话缓冲区持有且尚未进入派发，释放尾批时保留确定未发送证据。
    NotSent,
}

/// 单条命令的类型化回执。execute 后调用 `await_result()` 取值。
pub struct Ticket<T> {
    rx: oneshot::Receiver<std::result::Result<redis::Value, TicketErr>>,
    /// 命令 helper 附加的回复合同，仅约束本张票据，不改变原始 enqueue 的类型转换。
    reply_check: ReplyCheck,
    _t: std::marker::PhantomData<T>,
}

/// 本地命令票据的回复合同；异常回复不提供自动重放或副作用回滚证据。
enum ReplyCheck {
    /// 由调用方指定的 FromRedisValue 类型解释回复。
    General,
    /// PUBLISH 返回的非负订阅计数，不等同于业务消费确认。
    PublishCount,
    /// XADD 自动分配的非零完整身份。
    StreamId,
    /// XACK 确认量不能超过本次发送的 ID 数。
    StreamAck {
        /// 本命令实际提交的 ID 数量。
        requested: usize,
    },
}

impl<T: FromRedisValue> Ticket<T> {
    /// 业务作用：为空集合操作创建已完成的 ticket，不向 Redis 发送非法空命令。
    ///
    /// 参数说明：
    /// - `v`：空输入对应的 Redis 响应值，按调用方期望的类型在读取 ticket 时转换。
    ///
    /// 返回：可立即读取的预置结果，不占用会话条数或字节额度；类型不匹配在读取时返回解析错误。
    fn ready(v: redis::Value) -> Ticket<T> {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(v));
        Ticket {
            rx,
            reply_check: ReplyCheck::General,
            _t: std::marker::PhantomData,
        }
    }

    /// 业务作用：消费本命令的 ticket 并读取已经送达的结果，本方法不会异步等待。
    ///
    /// 参数说明：无。
    ///
    /// 返回：就绪时返回命令结果或对应错误；尚未就绪时返回 SessionLimit 且 ticket 被消费。
    /// 命令 helper 的回复合同先于通用类型转换，校验失败不证明远端未执行，也不自动重放。
    /// 通常应先等待 execute 完成；空输入的预置结果或已完成的后台封批可以更早就绪。
    pub fn await_result(mut self) -> Result<T> {
        match self.rx.try_recv() {
            Ok(Ok(v)) => {
                // 类型可转换不等于命令证据成立；只复验本票据的合同，不能让同批其它命令承担约束。
                match self.reply_check {
                    ReplyCheck::General => {}
                    ReplyCheck::PublishCount => crate::pubsub::check_publish_count_reply(&v)?,
                    ReplyCheck::StreamId => crate::stream::check_stream_id_reply(&v, "*")?,
                    ReplyCheck::StreamAck {
                        requested,
                    } => {
                        crate::stream::check_stream_ack_reply(&v, requested)?;
                    }
                }
                Ok(T::from_redis_value(v)?)
            }
            Ok(Err(TicketErr::Server(e))) => Err(NasaRedisError::Redis(e.into())),
            Ok(Err(TicketErr::Unknown(msg))) => Err(NasaRedisError::ExecutionUnknown(msg)),
            // 只有仍在会话缓冲区、尚未封批的命令有确定未发送证据；后台段不使用此结果。
            Ok(Err(TicketErr::NotSent)) => Err(NasaRedisError::NotExecuted(
                "PipelineSession 尾批尚未派发即被释放，命令确定未发送".into(),
            )),
            // 结果通道消失无法证明远端未执行，必须保留不确定性，避免调用方据此重放写命令。
            Err(oneshot::error::TryRecvError::Closed) => Err(NasaRedisError::ExecutionUnknown(
                "ticket 通道关闭(execute 被取消或未送达,命令可能已发到服务端执行)".into(),
            )),
            Err(oneshot::error::TryRecvError::Empty) => Err(NasaRedisError::SessionLimit(
                "execute() 之前不可取结果".into(),
            )),
        }
    }
}

/// 显式批会话，由 `client.pipeline()` 创建并以 `execute(self)` 等待完成。
///
/// 默认在入队后达到 1000 条或 4MB 时封批，后台按封批顺序逐段发送；同 slot 跨段保序。
/// 封批后仍可继续入队，每段 ticket 在该段完成时就绪，可能早于 execute。
/// execute 等待已封批的段并发送尾批，汇总批级传输错误；单命令错误由对应 ticket 返回。
///
/// helper 返回只表示本地入队或已调度封批，不表示 Redis 已执行。丢弃会话不撤销后台段，
/// 仅最后尚未发送的批次返回 NotExecuted。多生产者共享合批使用 [`AutoPipeline`]。
pub struct PipelineSession {
    client: Arc<RedisClient>,
    cfg: PipelineCfg,
    cmds: Vec<(
        redis::Cmd,
        oneshot::Sender<std::result::Result<redis::Value, TicketErr>>,
    )>,
    bytes: usize,
    /// 已 seal 各段的后台 flush 链尾(每段 await 前一段 → 跨段保序;任务返回值携带**该链第一个批级传输错误**,
    /// 供 `execute()` 聚合上抛——)。
    flush_chain: Option<JoinHandle<Result<()>>>,
    /// `false` 表示 `execute()` 是唯一提交点，达到会话上限时在入队前拒绝，不产生后台写出。
    auto_flush: bool,
    /// 尾批已从会话移交派发；等待封批链期间仍为 false，取消时可确认剩余尾批未发送。
    executed: bool,
}

impl RedisClient {
    /// 业务作用：创建共享当前客户端连接配置的显式 Pipeline 会话，达到阈值后可按段后台提交。
    ///
    /// 参数说明：无。
    ///
    /// 返回：尚无命令的会话；创建不会连接或发送，后续封批使用独立 Pipeline lane，同 slot 各段依次派发。
    pub fn pipeline(self: &Arc<Self>) -> PipelineSession {
        PipelineSession {
            client: Arc::clone(self),
            cfg: self.config().pipeline.clone(),
            cmds: Vec::new(),
            bytes: 0,
            flush_chain: None,
            auto_flush: true,
            executed: false,
        }
    }

    /// 业务作用：创建以 `execute()` 为唯一提交点的有界 Pipeline，供需要“未提交即确定未发送”语义的上层 API 使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：沿用公开会话条数、字节与专用连接配置，但达到上限时拒绝继续入队且不会滚动写出。
    pub(crate) fn deferred_pipeline(self: &Arc<Self>) -> PipelineSession {
        let mut session = self.pipeline();
        session.auto_flush = false;
        session
    }
}

impl PipelineSession {
    /// 业务作用：接纳一条原始 Redis 命令并创建类型化票据，作为各命令 helper 的统一封批入口。
    ///
    /// 参数说明：
    /// - `cmd`：完整命令及参数；调用方负责原始命令的键位置、同槽要求和服务端能力。
    ///
    /// 返回：接纳后返回票据，不表示 Redis 已执行；延迟提交模式超过条数或参数字节额度时拒绝本条命令。
    /// 自动封批模式按入队后的阈值封批，单条可超过字节阈值；没有 Tokio 上下文时暂留缓冲区直到 execute。
    /// Cluster 按首键 slot 分桶，识别 EVAL/EVALSHA/FCALL 的 numkeys 布局；其它原始命令按首个参数作为键，
    /// 不提供任意命令的键解析、跨槽拆分或全局顺序，特殊命令需由调用方选择适合的入口。
    pub fn enqueue<T: FromRedisValue>(&mut self, cmd: redis::Cmd) -> Result<Ticket<T>> {
        // 估算本命令参数字节(协议开销忽略;Arg 是 non_exhaustive,未知形态按 8 字节计)
        let sz: usize = cmd
            .args_iter()
            .map(|a| match a {
                redis::Arg::Simple(b) => b.len(),
                _ => 8,
            })
            .sum();
        if !self.auto_flush
            && (self.cmds.len() >= self.cfg.session_max_commands
                || self
                    .bytes
                    .checked_add(sz)
                    .is_none_or(|total| total > self.cfg.session_max_bytes))
        {
            return Err(NasaRedisError::SessionLimit(
                "延迟提交会话已达到 session_max_commands 或 session_max_bytes，尚未发送任何命令"
                    .to_owned(),
            ));
        }
        // rx 先建,push 后即使 seal_and_flush 把 batch `mem::take` 走,本 ticket 仍正常返回(tx 在 batch 内,
        // 由后台 dispatch 回填)。
        let (tx, rx) = oneshot::channel();
        self.bytes += sz;
        self.cmds.push((cmd, tx));
        // 滚动自动 flush:**push 后**判断——第 1000 条入队后
        // `len==session_max_commands` 立即 seal 后台发出。字节阈值同样 push 后判断
        // (`bytes >= max_bytes`),单条命令本身超限时它自己一段发出,不死循环。
        if self.auto_flush
            && (self.cmds.len() >= self.cfg.session_max_commands
                || self.bytes >= self.cfg.session_max_bytes)
        {
            self.seal_and_flush();
        }
        Ok(Ticket {
            rx,
            reply_check: ReplyCheck::General,
            _t: std::marker::PhantomData,
        })
    }

    /// 业务作用：把当前缓冲批次交给后台提交链，在前段结束后派发本段，保持同 slot 的段间发送顺序。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有执行上下文时移出当前批次、重置参数字节计数并更新链尾；空批不派发。
    /// 没有 Tokio 上下文时保留原缓冲区，等待 execute；后台退出或传输失败由 execute 与对应 ticket 报告。
    fn seal_and_flush(&mut self) {
        // 无执行上下文时不能接手后台责任；保留缓冲，后续 execute 或会话析构仍可处理原票据。
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let batch = std::mem::take(&mut self.cmds);
        self.bytes = 0;
        if batch.is_empty() {
            return;
        }
        let client = Arc::clone(&self.client);
        let prev = self.flush_chain.take();
        self.flush_chain = Some(handle.spawn(async move {
            // 前段结束后才发送本段；失败不能解释为未执行或自动重发，仍保留首个批级错误供调用方裁决。
            let prev_res = match prev {
                Some(p) => p.await.unwrap_or_else(join_err_to_unknown),
                None => Ok(()),
            };
            let own = dispatch_jobs(&client, batch).await;
            prev_res.and(own)
        }));
    }

    // 类型化命令统一通过 enqueue 接纳。

    /// 业务作用：将单键字符串读取加入当前批次，按该命令的回包独立转换业务类型。
    ///
    /// 参数说明：
    /// - `key`：待读取的 Redis 字符串键。
    ///
    /// 返回：接纳后返回票据，完成时键不存在为 None；会话拒绝直接返回错误，服务端及类型转换错误由票据报告。
    pub fn get<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("GET").arg(key).to_owned())
    }

    /// 业务作用：将单键字符串覆盖写入当前批次，写入成功时由 Redis 清除该键原有过期时间。
    ///
    /// 参数说明：
    /// - `key`：待写入的 Redis 键。
    /// - `val`：编码为一个 Redis 参数的完整值。
    ///
    /// 返回：接纳后返回确认写入的票据；本地拒绝立即返回错误，远端错误及结果未知由票据报告，不自动重放。
    pub fn set<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("SET").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将 Hash 单字段写入当前批次，保留其它字段并区分新增与覆盖。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：本次新增或覆盖的字段名。
    /// - `val`：字段的新值，编码为一个 Redis 参数。
    ///
    /// 返回：接纳后返回票据，true 表示新增字段，false 表示覆盖已有字段；本地拒绝直接返回错误，执行错误由票据报告。
    pub fn h_set<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("HSET").arg(key).arg(field).arg(val).to_owned())
    }

    /// 业务作用：将 Hash 单字段读取加入当前批次，不把字段缺失混同为空正文。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：待读取的字段名。
    ///
    /// 返回：接纳后返回票据，键或字段不存在时为 None；本地拒绝直接返回错误，服务端及类型转换错误由票据报告。
    pub fn h_get<T: FromRedisValue>(
        &mut self,
        key: &str,
        field: &str,
    ) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("HGET").arg(key).arg(field).to_owned())
    }

    /// 业务作用：将单键删除加入当前批次，键不存在时保持幂等的零删除结果。
    ///
    /// 参数说明：
    /// - `key`：待删除的 Redis 键，不要求特定数据类型。
    ///
    /// 返回：接纳后返回实际删除数量的票据，单键结果为零或一；本地拒绝立即返回错误，执行失败或未知由票据报告。
    pub fn del(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("DEL").arg(key).to_owned())
    }

    /// 业务作用：将整数键的原子增量操作加入当前批次，避免客户端读取后回写造成并发覆盖。
    ///
    /// 参数说明：
    /// - `key`：保存有符号整数的 Redis 字符串键，缺失时以零为初值。
    /// - `delta`：有符号增量，负数表示递减。
    ///
    /// 返回：接纳后返回更新值的票据；本地拒绝立即返回错误，非整数内容或算术越界等服务端错误由票据报告。
    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("INCRBY").arg(key).arg(delta).to_owned())
    }

    /// 业务作用：将有序集合单成员的新增或分值更新加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `score`：该成员的新排序分值，合法性由 Redis 校验。
    /// - `member`：成员身份，编码为一个 Redis 参数。
    ///
    /// 返回：接纳后返回新增成员数的票据，更新既有成员分值返回零；本地拒绝立即报错，非法分值或执行错误由票据报告。
    pub fn z_add<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        score: f64,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZADD")
                .arg(key)
                .arg(score)
                .arg(member)
                .to_owned(),
        )
    }

    /// 业务作用：将一条字段消息追加请求加入当前批次，由 Redis 生成 Stream 记录 ID。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键，缺失时按 XADD 语义创建。
    /// - `fields`：按给定顺序发送的字段名与二进制正文；有效消息应至少包含一对字段。
    ///
    /// 返回：接纳后返回非零完整 ID 的字符串票据；本地拒绝立即报错，服务端或回复校验错误由票据报告，不自动重发追加请求。
    pub fn x_add(&mut self, stream: &str, fields: &[(&str, &[u8])]) -> Result<Ticket<String>> {
        let mut c = redis::cmd("XADD");
        c.arg(stream).arg("*");
        for (f, v) in fields {
            c.arg(*f).arg(*v);
        }
        let mut ticket = self.enqueue(c)?;
        ticket.reply_check = ReplyCheck::StreamId;
        Ok(ticket)
    }

    // 多键命令仍由调用方保证同槽；本地封批不自动拆分单条命令。

    // 键和字符串命令。
    /// 业务作用：将单键相对过期设置加入当前批次，保留正亚毫秒时长，避免截断为零造成立即删除。
    ///
    /// 参数说明：
    /// - `key`：需要设置过期时间的 Redis 键。
    /// - `ttl`：从服务端执行时刻起计算的存活时长；零表示立即过期，正亚毫秒按一毫秒处理。
    ///
    /// 返回：时长超出有符号 64 位毫秒范围或会话拒绝时不入队并返回错误；接纳后的票据表示是否成功设置，执行失败由票据报告。
    pub fn expire(&mut self, key: &str, ttl: Duration) -> Result<Ticket<bool>> {
        let ms = crate::commands::duration_to_millis_floor1(ttl)?;
        self.enqueue(redis::cmd("PEXPIRE").arg(key).arg(ms).to_owned())
    }

    /// 业务作用：将单键的秒级绝对过期设置加入当前批次，以 Redis 执行时的时钟裁决是否已到期。
    ///
    /// 参数说明：
    /// - `key`：需要设置过期时间的 Redis 键。
    /// - `unix_secs`：Unix 秒级时间戳，过去的时刻使已有键立即过期。
    ///
    /// 返回：接纳后返回是否成功设置的票据；本地拒绝立即报错，服务端时间范围或执行错误由票据报告。
    pub fn expire_at_secs(&mut self, key: &str, unix_secs: i64) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("EXPIREAT").arg(key).arg(unix_secs).to_owned())
    }

    /// 业务作用：将单键的毫秒级绝对过期设置加入当前批次，避免客户端等待改变业务截止时刻。
    ///
    /// 参数说明：
    /// - `key`：需要设置过期时间的 Redis 键。
    /// - `unix_ms`：Unix 毫秒时间戳，过去的时刻使已有键立即过期。
    ///
    /// 返回：接纳后返回是否成功设置的票据；本地拒绝立即报错，服务端执行错误由票据报告。
    pub fn expire_at_millis(&mut self, key: &str, unix_ms: i64) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("PEXPIREAT").arg(key).arg(unix_ms).to_owned())
    }

    /// 业务作用：将单键过期时间移除请求加入当前批次，使已有带期限的键转为无期限保存。
    ///
    /// 参数说明：
    /// - `key`：需要移除 TTL 的 Redis 键。
    ///
    /// 返回：接纳后返回是否实际移除了期限的票据，键不存在或原本无期限为 false；本地及执行错误分别在入队或票据中报告。
    pub fn persist(&mut self, key: &str) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("PERSIST").arg(key).to_owned())
    }

    /// 业务作用：将单键存在性查询加入当前批次，观察服务端执行该命令时的状态。
    ///
    /// 参数说明：
    /// - `key`：待检查的 Redis 键，不要求特定数据类型。
    ///
    /// 返回：接纳后返回存在性票据；本地拒绝立即报错，查询错误由票据报告，存在性结果不构成后续写入的原子前提。
    pub fn exists(&mut self, key: &str) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("EXISTS").arg(key).to_owned())
    }

    /// 业务作用：将单键毫秒剩余期限查询加入当前批次，保留无期限和不存在的不同状态。
    ///
    /// 参数说明：
    /// - `key`：待查询剩余 TTL 的 Redis 键。
    ///
    /// 返回：接纳后返回有符号毫秒票据，-1 表示无 TTL，-2 表示键不存在；本地拒绝立即报错，查询错误由票据报告。
    pub fn pttl(&mut self, key: &str) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("PTTL").arg(key).to_owned())
    }

    /// 业务作用：将仅在键不存在时写入的原子条件请求加入当前批次，避免先查询再写入的竞争。
    ///
    /// 参数说明：
    /// - `key`：待条件写入的 Redis 键。
    /// - `val`：键不存在时写入的单个参数值。
    ///
    /// 返回：接纳后返回是否实际写入的票据；已有键保持原值并返回 false，本地拒绝或执行错误分别直接或通过票据报告。
    pub fn set_nx<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("SETNX").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将字符串覆盖写入与毫秒 TTL 设置作为一条原子 SET 命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：待写入的 Redis 键。
    /// - `val`：覆盖写入的单个参数值。
    /// - `ttl_ms`：执行后存活的毫秒数，必须是 Redis 接受的正期限，零或越界值由服务端拒绝。
    ///
    /// 返回：接纳后返回写入确认票据；本地拒绝立即报错，非法 TTL、执行失败或结果未知由票据报告。
    pub fn set_ttl<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
        ttl_ms: u64,
    ) -> Result<Ticket<()>> {
        self.enqueue(
            redis::cmd("SET")
                .arg(key)
                .arg(val)
                .arg("PX")
                .arg(ttl_ms)
                .to_owned(),
        )
    }

    /// 业务作用：将字符串尾部追加加入当前批次，缺失键按空字符串开始，已有值无需客户端读回。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键。
    /// - `val`：待追加的单个二进制参数值。
    ///
    /// 返回：接纳后返回追加后的字节长度票据；本地拒绝立即报错，执行失败或结果未知由票据报告，不能据未知结果自动重发追加。
    pub fn append<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("APPEND").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将读取旧字符串并覆盖新值的原子交换加入当前批次，成功后清除原键的 TTL。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键。
    /// - `val`：替换旧值的单个参数值。
    ///
    /// 返回：接纳后返回旧值票据，原键不存在时为 None；本地拒绝立即报错，执行或旧值转换错误由票据报告，转换失败不回滚已发生的覆盖。
    pub fn get_set<T: FromRedisValue, V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("GETSET").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将读取字符串并删除对应键的原子请求加入当前批次，用于一次性取走持久值。
    ///
    /// 参数说明：
    /// - `key`：待读取并删除的字符串键。
    ///
    /// 返回：接纳后返回被删除值的票据，键不存在时为 None；本地拒绝立即报错，执行或类型转换错误由票据报告，转换失败不恢复已删除的数据。
    pub fn get_del<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("GETDEL").arg(key).to_owned())
    }

    /// 业务作用：将字符串长度查询加入当前批次，只读取字节数量而不取回正文。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键。
    ///
    /// 返回：接纳后返回字节长度票据，缺失键为零；本地拒绝立即报错，键类型不符或执行错误由票据报告。
    pub fn str_len(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("STRLEN").arg(key).to_owned())
    }

    /// 业务作用：将多个字符串键作为一条 MGET 加入批次，按输入顺序保留每个键的可选结果。
    ///
    /// 参数说明：
    /// - `keys`：有序键列表；Cluster 使用时调用方应提供同槽键，本入口不拆分这条多键命令。
    ///
    /// 返回：空输入立即得到空数组票据且不发送；非空接纳后返回逐键结果，缺失或非字符串键为 None。
    /// 会话拒绝立即报错，服务端及类型转换错误由票据报告，不能据返回数组推断后续命令仍看到同一状态。
    pub fn mget<T: FromRedisValue>(&mut self, keys: &[&str]) -> Result<Ticket<Vec<Option<T>>>> {
        if keys.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("MGET");
        for k in keys {
            c.arg(*k);
        }
        self.enqueue(c)
    }

    // hash
    /// 业务作用：将同一 Hash 的字段删除合并为一条命令，保留未指定的字段。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `fields`：待删除的字段名列表，重复或缺失字段不增加实际删除数。
    ///
    /// 返回：空字段列表立即得到零票据且不发送；非空接纳后返回实际删除数，会话拒绝立即报错，执行错误由票据报告。
    pub fn h_del(&mut self, key: &str, fields: &[&str]) -> Result<Ticket<u64>> {
        if fields.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("HDEL");
        c.arg(key);
        for f in fields {
            c.arg(*f);
        }
        self.enqueue(c)
    }

    /// 业务作用：将 Hash 字段不存在时才写入的原子条件操作加入当前批次，保护已有字段值。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，缺失时可创建。
    /// - `field`：待条件写入的字段名。
    /// - `val`：字段不存在时写入的单个参数值。
    ///
    /// 返回：接纳后返回是否新增字段的票据；字段已有值时为 false，本地拒绝立即报错，执行错误由票据报告。
    pub fn h_set_nx<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("HSETNX").arg(key).arg(field).arg(val).to_owned())
    }

    /// 业务作用：将 Hash 整数字段的原子增量操作加入当前批次，避免并发计数被读改写覆盖。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：保存有符号整数的字段，缺失时以零为初值。
    /// - `delta`：有符号增量，负数表示递减。
    ///
    /// 返回：接纳后返回更新值的票据；会话拒绝立即报错，非整数内容、算术越界或其它执行错误由票据报告。
    pub fn h_incr_by(&mut self, key: &str, field: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(
            redis::cmd("HINCRBY")
                .arg(key)
                .arg(field)
                .arg(delta)
                .to_owned(),
        )
    }

    /// 业务作用：将 Hash 全字段读取加入当前批次，按调用方选择的容器类型转换完整字段集合。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键；该操作读取全部字段，不分页。
    ///
    /// 返回：接纳后返回完整响应的类型化票据，缺失键对应空集合；会话拒绝立即报错，执行或容器转换错误由票据报告。
    pub fn h_get_all<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<T>> {
        self.enqueue(redis::cmd("HGETALL").arg(key).to_owned())
    }

    /// 业务作用：将同一 Hash 的指定字段读取合并为一条命令，按请求顺序保留缺失位置。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `fields`：有序字段名列表，重复字段也保留对应结果位置。
    ///
    /// 返回：空列表立即得到空数组票据且不发送；非空接纳后返回逐字段可选值，缺失位置为 None。
    /// 会话拒绝立即报错，服务端及值类型转换错误由票据报告。
    pub fn h_mget<T: FromRedisValue>(
        &mut self,
        key: &str,
        fields: &[&str],
    ) -> Result<Ticket<Vec<Option<T>>>> {
        if fields.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("HMGET");
        c.arg(key);
        for f in fields {
            c.arg(*f);
        }
        self.enqueue(c)
    }

    /// 业务作用：将 Hash 全字段名查询加入当前批次，不读取字段正文。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键；该操作一次返回所有字段名。
    ///
    /// 返回：接纳后返回字段名字符串列表的票据，缺失键为空列表；会话拒绝立即报错，执行或字段名 UTF-8 转换错误由票据报告。
    pub fn h_keys(&mut self, key: &str) -> Result<Ticket<Vec<String>>> {
        self.enqueue(redis::cmd("HKEYS").arg(key).to_owned())
    }

    /// 业务作用：将 Hash 字段数量查询加入当前批次，不拉取字段名或正文。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    ///
    /// 返回：接纳后返回字段数量票据，缺失键为零；会话拒绝立即报错，类型不符或其它执行错误由票据报告。
    pub fn h_len(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("HLEN").arg(key).to_owned())
    }

    // list
    /// 业务作用：将单元素从列表头部入队的请求加入当前批次，缺失列表按 Redis 语义创建。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `val`：待插入头部的单个元素。
    ///
    /// 返回：接纳后返回插入后的列表长度票据；本地拒绝立即报错，执行失败或结果未知由票据报告，不自动重放入队。
    pub fn l_push<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LPUSH").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将单元素从列表尾部入队的请求加入当前批次，保留已有元素的先后顺序。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，缺失时可创建。
    /// - `val`：待追加到尾部的单个元素。
    ///
    /// 返回：接纳后返回插入后的列表长度票据；本地拒绝立即报错，执行失败或结果未知由票据报告，不自动重放入队。
    pub fn r_push<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("RPUSH").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将从列表头部移除并返回一个元素的请求加入当前批次，不等待新元素到达。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    ///
    /// 返回：接纳后返回移出元素的票据，列表缺失时为 None；本地拒绝立即报错，执行或解码错误由票据报告，解码失败不恢复已移出的元素。
    pub fn l_pop<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("LPOP").arg(key).to_owned())
    }

    /// 业务作用：将从列表尾部移除并返回一个元素的请求加入当前批次，不等待新元素到达。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    ///
    /// 返回：接纳后返回移出元素的票据，列表缺失时为 None；本地拒绝立即报错，执行或解码错误由票据报告，解码失败不恢复已移出的元素。
    pub fn r_pop<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("RPOP").arg(key).to_owned())
    }

    /// 业务作用：将列表闭区间读取加入当前批次，保留所选元素从头到尾的顺序。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `start`：包含在结果内的起始下标，零为头部，负数从尾部计算。
    /// - `stop`：包含在结果内的结束下标，支持负索引，超出列表的边界由 Redis 截取。
    ///
    /// 返回：接纳后返回有序元素列表票据，缺失键或无交集区间为空；本地拒绝立即报错，执行或元素转换错误由票据报告。
    pub fn l_range<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("LRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .to_owned(),
        )
    }

    /// 业务作用：将列表中匹配值的定向删除加入当前批次，按数量参数限制删除范围。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `count`：正数从头部起删除至多该数量，负数从尾部起删除至多其绝对值数量，零表示全部匹配项。
    /// - `val`：按 Redis 参数字节精确匹配的元素值。
    ///
    /// 返回：接纳后返回实际删除数票据，缺失键或无匹配项为零；本地拒绝立即报错，执行失败或结果未知由票据报告。
    pub fn l_rem<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        count: isize,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LREM").arg(key).arg(count).arg(val).to_owned())
    }

    /// 业务作用：将列表长度查询加入当前批次，不取回元素正文。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    ///
    /// 返回：接纳后返回元素数量票据，缺失列表为零；本地拒绝立即报错，键类型不符或其它执行错误由票据报告。
    pub fn l_len(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LLEN").arg(key).to_owned())
    }

    /// 业务作用：将列表裁剪为指定闭区间的请求加入当前批次，删除保留范围之外的元素。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `start`：保留区间的包含式起点，负索引从列表尾部计算。
    /// - `stop`：保留区间的包含式终点，-1 表示最后一个元素；空区间会移除全部元素。
    ///
    /// 返回：接纳后返回裁剪确认票据，列表缺失也可成功；本地拒绝立即报错，执行失败或结果未知由票据报告，不能凭未知结果安全重试相对索引裁剪。
    pub fn l_trim(&mut self, key: &str, start: isize, stop: isize) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("LTRIM").arg(key).arg(start).arg(stop).to_owned())
    }

    // set
    /// 业务作用：将集合单成员加入请求放入当前批次，已有同值成员保持去重。
    ///
    /// 参数说明：
    /// - `key`：目标集合键，缺失时可创建。
    /// - `member`：按参数字节识别的成员值。
    ///
    /// 返回：接纳后返回新增数量票据，已有成员为零；会话拒绝立即报错，类型不符或其它执行错误由票据报告。
    pub fn s_add<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("SADD").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将集合单成员移除加入当前批次，保留其它成员。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `member`：按参数字节匹配的待移除成员。
    ///
    /// 返回：接纳后返回实际移除数量票据，键或成员不存在时为零；会话拒绝立即报错，执行错误由票据报告。
    pub fn s_rem<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("SREM").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将集合成员数量查询加入当前批次，不取回成员正文。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    ///
    /// 返回：接纳后返回成员数量票据，缺失键为零；会话拒绝立即报错，类型不符或其它执行错误由票据报告。
    pub fn s_card(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("SCARD").arg(key).to_owned())
    }

    /// 业务作用：将集合全成员读取加入当前批次，用于一次取得当前成员快照。
    ///
    /// 参数说明：
    /// - `key`：目标集合键；该操作读取全部成员，不分页。
    ///
    /// 返回：接纳后返回成员列表票据，缺失键为空，结果顺序无保证；会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn s_members<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("SMEMBERS").arg(key).to_owned())
    }

    /// 业务作用：将集合单成员归属查询加入当前批次，按服务端执行时的成员状态回答。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `member`：待核对的成员参数值。
    ///
    /// 返回：接纳后返回成员是否存在的票据，缺失键为 false；会话拒绝立即报错，执行错误由票据报告，不为后续操作提供原子保护。
    pub fn s_is_member<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("SISMEMBER").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将随机移出集合一个成员的请求加入当前批次，读取与移除在同一命令内完成。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    ///
    /// 返回：接纳后返回被移出成员的票据，缺失键为 None；会话拒绝立即报错，执行或解码错误由票据报告，解码失败不会恢复成员。
    pub fn s_pop<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("SPOP").arg(key).to_owned())
    }

    // zset
    /// 业务作用：将有序集合的单成员移除加入当前批次，同时移除该成员的排序分值。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `member`：待移除的成员身份，按参数字节匹配。
    ///
    /// 返回：接纳后返回实际移除数量票据，键或成员缺失时为零；会话拒绝立即报错，执行错误由票据报告。
    pub fn z_rem<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("ZREM").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将有序集合成员数量查询加入当前批次，不返回成员或分值正文。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    ///
    /// 返回：接纳后返回成员数量票据，缺失键为零；会话拒绝立即报错，类型不符或其它执行错误由票据报告。
    pub fn z_card(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("ZCARD").arg(key).to_owned())
    }

    /// 业务作用：将有序集合单成员的分值查询加入当前批次，区分成员缺失与零分值。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `member`：待查询分值的成员身份。
    ///
    /// 返回：接纳后返回可选浮点分值票据，键或成员不存在时为 None；会话拒绝立即报错，执行或分值转换错误由票据报告。
    pub fn z_score<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<Option<f64>>> {
        self.enqueue(redis::cmd("ZSCORE").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将有序集合单成员的升序排名查询加入当前批次，以零表示排序中的首位。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `member`：待查询排名的成员身份。
    ///
    /// 返回：接纳后返回从零开始的可选排名票据，键或成员不存在时为 None；会话拒绝立即报错，执行错误由票据报告。
    pub fn z_rank<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<Option<u64>>> {
        self.enqueue(redis::cmd("ZRANK").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将有序集合单成员的降序排名查询加入当前批次，以零表示反向排序中的首位。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `member`：待查询反向排名的成员身份。
    ///
    /// 返回：接纳后返回从零开始的可选降序排名票据，键或成员不存在时为 None；会话拒绝立即报错，执行错误由票据报告。
    pub fn z_rev_rank<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<Option<u64>>> {
        self.enqueue(redis::cmd("ZREVRANK").arg(key).arg(member).to_owned())
    }

    /// 业务作用：将有序集合分值闭区间的成员数量查询加入当前批次，不拉取匹配成员。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `min`：包含在统计范围内的分值下界。
    /// - `max`：包含在统计范围内的分值上界，本入口不表达开区间。
    ///
    /// 返回：接纳后返回匹配成员数量票据，缺失键或空区间为零；会话拒绝立即报错，非法分值或执行错误由票据报告。
    pub fn z_count(&mut self, key: &str, min: f64, max: f64) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("ZCOUNT").arg(key).arg(min).arg(max).to_owned())
    }

    /// 业务作用：将有序集合单成员的原子分值增量加入当前批次，避免客户端读改写覆盖并发变化。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `delta`：分值增量，负数表示递减，合法性由 Redis 校验。
    /// - `member`：待更新成员，缺失时以零分值为初值创建。
    ///
    /// 返回：接纳后返回更新分值票据；会话拒绝立即报错，非法分值、执行失败或结果未知由票据报告，不自动重放增量。
    pub fn z_incr_by<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        delta: f64,
        member: V,
    ) -> Result<Ticket<f64>> {
        self.enqueue(
            redis::cmd("ZINCRBY")
                .arg(key)
                .arg(delta)
                .arg(member)
                .to_owned(),
        )
    }

    /// 业务作用：将有序集合分值闭区间读取加入当前批次，按分值升序返回匹配成员。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `min`：包含在结果内的分值下界。
    /// - `max`：包含在结果内的分值上界；本入口不分页，也不表达开区间。
    ///
    /// 返回：接纳后返回匹配成员列表票据，缺失键或空区间为空；会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn z_range_by_score<T: FromRedisValue>(
        &mut self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZRANGEBYSCORE")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    // Stream 和脚本命令。
    /// 业务作用：将指定 Stream 记录正文的删除加入当前批次，不将删除正文视为消费组确认。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    /// - `ids`：按原文发送的记录 ID 列表，不过滤空白，也不代替 Redis 校验 ID。
    ///
    /// 返回：空列表立即得到零票据且不发送；非空接纳后返回实际删除数，本地拒绝立即报错，执行错误由票据报告。
    /// 本入口不核对分区权威或 PEL，调用方须保证被删正文不再用于业务恢复，并另行维护确认责任。
    pub fn x_del(&mut self, stream: &str, ids: &[&str]) -> Result<Ticket<u64>> {
        if ids.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("XDEL");
        c.arg(stream);
        for id in ids {
            c.arg(*id);
        }
        self.enqueue(c)
    }

    /// 业务作用：将指定消费组的记录确认加入当前批次，从该组 PEL 移除已处理责任而保留 Stream 正文。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    /// - `group`：本次确认归属的消费组。
    /// - `ids`：按原文发送的待确认 ID 列表，不过滤空白。
    ///
    /// 返回：空列表立即得到零票据且不发送；非空接纳后返回实际从 PEL 移除的记录数，未在 PEL 中的 ID 不计数。
    /// 仅接受不超过本次 ID 数量的非负 Redis 整数；本地拒绝立即报错，执行或回复校验错误由票据报告。
    /// 本入口不验证业务完成或处理权威，回复异常不证明远端未确认。
    pub fn x_ack(&mut self, stream: &str, group: &str, ids: &[&str]) -> Result<Ticket<u64>> {
        if ids.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("XACK");
        c.arg(stream).arg(group);
        for id in ids {
            c.arg(*id);
        }
        let mut ticket = self.enqueue(c)?;
        ticket.reply_check = ReplyCheck::StreamAck {
            requested: ids.len(),
        };
        Ok(ticket)
    }

    /// 业务作用：将 Stream 当前正文记录数量查询加入当前批次，避免将保存条数与消费责任数混用。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    ///
    /// 返回：接纳后返回现存记录数量票据，缺失键为零；本地拒绝立即报错，执行错误由票据报告，结果不代表 PEL 数量。
    pub fn x_len(&mut self, stream: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("XLEN").arg(stream).to_owned())
    }

    /// 业务作用：按精确 MAXLEN 裁剪 Stream，不使用近似裁剪标记 `~`。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    /// - `maxlen`：裁剪时允许保留的最大记录数，零表示清空正文。
    ///
    /// 返回：接纳后返回实际删除数量票据；本地拒绝立即报错，执行错误由票据报告。
    /// 不检查消费组确认或分区权威，可能删去仍在 PEL 中的正文；调用方负责恢复所需数据的保留策略。
    pub fn x_trim_maxlen(&mut self, stream: &str, maxlen: u64) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("XTRIM")
                .arg(stream)
                .arg("MAXLEN")
                .arg(maxlen)
                .to_owned(),
        )
    }

    /// 业务作用：将按存储节点边界进行的 Stream 近似数量裁剪加入当前批次，允许少做细粒度删除。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    /// - `maxlen`：期望保留的记录数量阈值，实际数量可能高于它，不是硬容量上限。
    ///
    /// 返回：接纳后返回实际删除数量票据；本地拒绝立即报错，执行错误由票据报告。
    /// 本入口不等待消费组确认，调用方须保护恢复所需的正文；需要精确数量边界时使用 x_trim_maxlen。
    pub fn x_trim_maxlen_approx(&mut self, stream: &str, maxlen: u64) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("XTRIM")
                .arg(stream)
                .arg("MAXLEN")
                .arg("~")
                .arg(maxlen)
                .to_owned(),
        )
    }

    /// 业务作用：将一段 Lua 脚本及其显式键集合加入当前批次，按脚本的首键参与 Cluster 分桶。
    ///
    /// 参数说明：
    /// - `script`：交给 Redis 执行的 Lua 正文，调用方负责其安全性和副作用约束。
    /// - `keys`：脚本访问的键列表，按顺序传入 KEYS；Cluster 下须满足同槽约束。
    /// - `args`：按顺序传入 ARGV 的字符串参数。
    ///
    /// 返回：接纳后返回脚本结果的类型化票据；本地拒绝立即报错，脚本或转换错误由票据报告。
    /// 脚本错误不保证撤销此前写入，结果未知也不能作为安全重放的依据。
    pub fn eval<T: FromRedisValue>(
        &mut self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Ticket<T>> {
        let mut c = redis::cmd("EVAL");
        c.arg(script).arg(keys.len());
        for k in keys {
            c.arg(*k);
        }
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    /// 业务作用：将按摘要执行已缓存 Lua 脚本的请求加入当前批次，不隐式加载或重放脚本。
    ///
    /// 参数说明：
    /// - `sha`：目标脚本的 SHA1 摘要，须存在于实际执行节点的脚本缓存。
    /// - `keys`：按顺序传入 KEYS 的访问键，Cluster 下须满足同槽约束。
    /// - `args`：按顺序传入 ARGV 的字符串参数。
    ///
    /// 返回：接纳后返回脚本结果票据；本地拒绝立即报错，NOSCRIPT、脚本或转换错误由票据报告。
    /// 调用方据明确的 NOSCRIPT 决定加载或改用 eval，不能把其它错误和未知结果当作安全重放证据。
    pub fn eval_sha<T: FromRedisValue>(
        &mut self,
        sha: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Ticket<T>> {
        let mut c = redis::cmd("EVALSHA");
        c.arg(sha).arg(keys.len());
        for k in keys {
            c.arg(*k);
        }
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    // Redis 命令的类型化入队入口。

    // key(其余)
    /// 业务作用：将单键秒级剩余期限查询加入当前批次，保留无期限和键不存在的不同状态。
    ///
    /// 参数说明：
    /// - `key`：待查询 TTL 的 Redis 键。
    ///
    /// 返回：接纳后返回有符号秒数票据，-1 表示无 TTL，-2 表示键不存在；会话拒绝立即报错，查询错误由票据报告。
    pub fn ttl(&mut self, key: &str) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("TTL").arg(key).to_owned())
    }

    /// 业务作用：将单键存储类型查询加入当前批次，观察服务端执行该命令时的实际类型。
    ///
    /// 参数说明：
    /// - `key`：待查询数据类型的 Redis 键。
    ///
    /// 返回：接纳后返回类型名称票据，键不存在时为 none；会话拒绝立即报错，查询或响应转换错误由票据报告。
    pub fn key_type(&mut self, key: &str) -> Result<Ticket<String>> {
        self.enqueue(redis::cmd("TYPE").arg(key).to_owned())
    }

    /// 业务作用：将执行节点上匹配模式的全部键名查询加入当前批次，不提供游标分页或集群全节点遍历。
    ///
    /// 参数说明：
    /// - `pattern`：Redis glob 匹配模式；全量枚举会占用执行节点处理时间。
    ///
    /// 返回：接纳后返回匹配键名票据；会话拒绝立即报错，查询或 UTF-8 转换错误由票据报告，结果不构成集群完整键清单。
    pub fn keys(&mut self, pattern: &str) -> Result<Ticket<Vec<String>>> {
        self.enqueue(redis::cmd("KEYS").arg(pattern).to_owned())
    }

    /// 业务作用：将单键的 Redis 内部格式导出加入当前批次，取得可用于相应恢复命令的值正文。
    ///
    /// 参数说明：
    /// - `key`：待导出值的 Redis 键。
    ///
    /// 返回：接纳后返回可选二进制票据，缺失键为 None；导出不包含键的 TTL，不能单独充当完整备份。
    /// 会话拒绝立即报错，命令执行错误由票据报告。
    pub fn dump(&mut self, key: &str) -> Result<Ticket<Option<Vec<u8>>>> {
        self.enqueue(redis::cmd("DUMP").arg(key).to_owned())
    }

    /// 业务作用：将多个键的删除合并为一条 DEL 加入当前批次，不逐键隐式拆分。
    ///
    /// 参数说明：
    /// - `keys`：待删除键列表，Cluster 使用时调用方应保证同槽；重复键不会重复计入实际删除数。
    ///
    /// 返回：空列表立即得到零票据且不发送；非空接纳后返回实际删除键数，会话拒绝立即报错，执行错误由票据报告。
    pub fn del_multi(&mut self, keys: &[&str]) -> Result<Ticket<u64>> {
        if keys.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("DEL");
        for k in keys {
            c.arg(*k);
        }
        self.enqueue(c)
    }

    /// 业务作用：将多个键的存在性统计合并为一条 EXISTS 加入当前批次，保留重复参数的计数语义。
    ///
    /// 参数说明：
    /// - `keys`：待检查键列表，存在的重复键会按出现次数计数；Cluster 使用时调用方应保证同槽。
    ///
    /// 返回：空列表立即得到零票据且不发送；非空接纳后返回匹配参数次数，会话拒绝立即报错，执行错误由票据报告。
    pub fn exists_multi(&mut self, keys: &[&str]) -> Result<Ticket<u64>> {
        if keys.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("EXISTS");
        for k in keys {
            c.arg(*k);
        }
        self.enqueue(c)
    }
    // (expire_at_secs 见上方 TTL 区块,统一单位语义)

    // string(其余)
    /// 业务作用：将字符串指定字节偏移处的覆盖写入加入当前批次，超出原长度的间隙由 Redis 补零。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键。
    /// - `offset`：从零开始的字节偏移，须满足 Redis 偏移和字符串长度限制，服务端负责校验。
    /// - `val`：从该偏移开始写入的单个二进制参数。
    ///
    /// 返回：接纳后返回写入后的字节长度票据；会话拒绝立即报错，非法偏移、类型不符或其它执行错误由票据报告。
    pub fn set_range<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        offset: u64,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("SETRANGE")
                .arg(key)
                .arg(offset)
                .arg(val)
                .to_owned(),
        )
    }

    /// 业务作用：将字符串字节闭区间读取加入当前批次，并将截取结果转换为 UTF-8 字符串。
    ///
    /// 参数说明：
    /// - `key`：目标字符串键。
    /// - `start`：包含式字节起点，负索引从尾部计算，不按字符边界对齐。
    /// - `end`：包含式字节终点，超出正文的范围由 Redis 截取。
    ///
    /// 返回：接纳后返回字符串票据，缺失键或空区间为空字符串；会话拒绝立即报错，执行错误或截断造成的非法 UTF-8 由票据报告。
    pub fn get_range(&mut self, key: &str, start: isize, end: isize) -> Result<Ticket<String>> {
        self.enqueue(
            redis::cmd("GETRANGE")
                .arg(key)
                .arg(start)
                .arg(end)
                .to_owned(),
        )
    }

    /// 业务作用：将整数字符串的原子减量加入当前批次，把减法范围检查交给 Redis。
    ///
    /// 参数说明：
    /// - `key`：保存有符号整数的字符串键，缺失时以零为初值。
    /// - `delta`：待减去的有符号步长，负数表示增加；本入口原样传给 DECRBY，不在本地取负。
    ///
    /// 返回：接纳后返回更新值票据；会话拒绝立即报错，数值范围、非整数内容或其它执行错误由票据报告，不自动重放减量。
    pub fn decr_by(&mut self, key: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("DECRBY").arg(key).arg(delta).to_owned())
    }

    /// 业务作用：将字符串浮点计数的原子增量加入当前批次，避免客户端读取后覆盖并发更新。
    ///
    /// 参数说明：
    /// - `key`：保存浮点数的字符串键，缺失时以零为初值。
    /// - `delta`：有符号浮点增量，合法性及计算结果范围由 Redis 校验。
    ///
    /// 返回：接纳后返回更新值票据；会话拒绝立即报错，非法数值、执行失败或结果未知由票据报告，不自动重放增量。
    pub fn incr_by_float(&mut self, key: &str, delta: f64) -> Result<Ticket<f64>> {
        self.enqueue(redis::cmd("INCRBYFLOAT").arg(key).arg(delta).to_owned())
    }

    /// 业务作用：将键值对作为一条原子 MSET 入队，空输入直接完成。
    ///
    /// 参数说明：
    /// - `pairs`：同一条 MSET 的键和字节值列表；Cluster 下全部键必须同槽，本入口不拆分。
    ///
    /// 返回：空列表立即得到完成票据且不发送；非空接纳后返回整条命令的确认票据，本地拒绝立即报错，执行错误由票据报告。
    /// 跨槽分别写入可用 multi_set_split，但不再具备单条 MSET 的原子性。
    pub fn mset(&mut self, pairs: &[(&str, &[u8])]) -> Result<Ticket<()>> {
        if pairs.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("MSET");
        for (k, v) in pairs {
            c.arg(*k).arg(*v);
        }
        self.enqueue(c)
    }

    /// 业务作用：为每个键值对分别入队 SET，返回独立 ticket。
    ///
    /// 参数说明：
    /// - `pairs`：按输入顺序分别提交的键和二进制值列表，允许不同键落在不同 slot。
    ///
    /// 返回：全部接纳时返回逐项票据，空列表返回空向量；中途拒绝时返回错误，已接纳前缀不会撤销，其票据不随错误返回。
    /// 前缀可能已封批或仍在会话缓冲区，错误不表示整组未执行；各命令独立执行，不提供跨键原子性。
    pub fn multi_set_split(&mut self, pairs: &[(&str, &[u8])]) -> Result<Vec<Ticket<()>>> {
        let mut tickets = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            tickets.push(self.enqueue(redis::cmd("SET").arg(*k).arg(*v).to_owned())?);
        }
        Ok(tickets)
    }

    // hash(其余)
    /// 业务作用：将同一 Hash 的多个字段更新合并为一条 HSET 加入当前批次，保留未指定字段。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，缺失时可创建。
    /// - `pairs`：字段名与字节值列表，按输入顺序构造同一条命令。
    ///
    /// 返回：空列表立即得到完成票据且不发送；非空接纳后返回执行确认票据，不暴露新增字段数量。
    /// 会话拒绝立即报错，类型不符或其它执行错误由票据报告。
    pub fn h_m_set(&mut self, key: &str, pairs: &[(&str, &[u8])]) -> Result<Ticket<()>> {
        if pairs.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("HSET");
        c.arg(key);
        for (f, v) in pairs {
            c.arg(*f).arg(*v);
        }
        self.enqueue(c)
    }

    /// 业务作用：将 Hash 字段存在性查询加入当前批次，不读取字段值正文。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：待检查的字段名。
    ///
    /// 返回：接纳后返回存在性票据，键或字段缺失时为 false；会话拒绝立即报错，执行错误由票据报告，不为后续写入提供原子条件。
    pub fn h_exists(&mut self, key: &str, field: &str) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("HEXISTS").arg(key).arg(field).to_owned())
    }

    /// 业务作用：将 Hash 的全部字段值读取加入当前批次，不返回字段名。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键；该操作读取全部值，不分页。
    ///
    /// 返回：接纳后返回字段值列表票据，缺失键为空，结果顺序无保证；会话拒绝立即报错，执行或值转换错误由票据报告。
    pub fn h_vals<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("HVALS").arg(key).to_owned())
    }

    /// 业务作用：将 Hash 随机字段名读取加入当前批次，不删除字段也不读取其值。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    ///
    /// 返回：接纳后返回可选字段名票据，缺失键为 None；会话拒绝立即报错，执行或字段名 UTF-8 转换错误由票据报告。
    pub fn h_rand_field(&mut self, key: &str) -> Result<Ticket<Option<String>>> {
        self.enqueue(redis::cmd("HRANDFIELD").arg(key).to_owned())
    }

    /// 业务作用：将 Hash 浮点字段的原子增量加入当前批次，保留同键其它字段。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：保存浮点数的字段，缺失时以零为初值。
    /// - `delta`：有符号浮点增量，合法性及结果范围由 Redis 校验。
    ///
    /// 返回：接纳后返回更新值票据；会话拒绝立即报错，非法数值、执行失败或结果未知由票据报告，不自动重放增量。
    pub fn h_incr_by_float(&mut self, key: &str, field: &str, delta: f64) -> Result<Ticket<f64>> {
        self.enqueue(
            redis::cmd("HINCRBYFLOAT")
                .arg(key)
                .arg(field)
                .arg(delta)
                .to_owned(),
        )
    }

    /// 业务作用：将 Hash 整数字段的减量转为可表示的负增量后入队，避免边界取负改变更新方向。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：保存有符号整数的字段，缺失时以零为初值。
    /// - `delta`：待减去的有符号步长，负数表示增加；i64 最小值无法转为同范围的增量。
    ///
    /// 返回：步长不可取负或会话拒绝时直接报错，不接纳本条命令；接纳后的票据返回更新值，执行错误由票据报告。
    pub fn h_decr_by(&mut self, key: &str, field: &str, delta: i64) -> Result<Ticket<i64>> {
        // 取负必须先于入队完成范围校验，不能让极值在不同编译配置下变成异常或方向相反的持久更新。
        let increment = delta.checked_neg().ok_or_else(|| {
            NasaRedisError::Config("h_decr_by: delta 无法在 i64 范围内取负".into())
        })?;
        self.enqueue(
            redis::cmd("HINCRBY")
                .arg(key)
                .arg(field)
                .arg(increment)
                .to_owned(),
        )
    }

    // list(其余)
    /// 业务作用：将一组值从列表左侧压入的命令加入当前批次，保留 Redis 逐个左插的顺序。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，缺失时由非空写入创建。
    /// - `vals`：依次左插的值，最终从左到右的新增部分与输入顺序相反。
    ///
    /// 返回：空输入直接返回值为零的完成票据，零不代表当前列表长度；非空接纳后返回更新长度票据，会话拒绝立即报错，执行失败或结果未知由票据报告。
    pub fn l_push_multi(&mut self, key: &str, vals: &[&[u8]]) -> Result<Ticket<u64>> {
        if vals.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("LPUSH");
        c.arg(key);
        for v in vals {
            c.arg(*v);
        }
        self.enqueue(c)
    }

    /// 业务作用：将一组值按输入顺序追加到列表右侧的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，缺失时由非空写入创建。
    /// - `vals`：依次追加的值。
    ///
    /// 返回：空输入直接返回值为零的完成票据，零不代表当前列表长度；非空接纳后返回更新长度票据，会话拒绝立即报错，执行失败或结果未知由票据报告。
    pub fn r_push_multi(&mut self, key: &str, vals: &[&[u8]]) -> Result<Ticket<u64>> {
        if vals.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("RPUSH");
        c.arg(key);
        for v in vals {
            c.arg(*v);
        }
        self.enqueue(c)
    }

    /// 业务作用：将仅向已存在列表左侧插入值的 LPUSHX 命令加入当前批次，缺失键不创建。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，存在性在服务端执行时判定。
    /// - `val`：待插入的值，不进行成员去重。
    ///
    /// 返回：接纳后返回更新长度票据，缺失键为零；会话拒绝立即报错，类型或执行错误由票据报告。
    pub fn l_push_if_absent<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LPUSHX").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将仅向已存在列表右侧追加值的 RPUSHX 命令加入当前批次，缺失键不创建。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，存在性在服务端执行时判定。
    /// - `val`：待追加的值，不进行成员去重。
    ///
    /// 返回：接纳后返回更新长度票据，缺失键为零；会话拒绝立即报错，类型或执行错误由票据报告。
    pub fn r_push_if_absent<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("RPUSHX").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将按下标读取单个列表元素的命令加入当前批次，不移除该元素。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `index`：从零开始的下标，负数从列表尾部倒数。
    ///
    /// 返回：接纳后返回可选元素票据，键缺失或下标越界时为 None；会话拒绝立即报错，执行或元素转换错误由票据报告。
    pub fn l_index<T: FromRedisValue>(
        &mut self,
        key: &str,
        index: isize,
    ) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("LINDEX").arg(key).arg(index).to_owned())
    }

    /// 业务作用：将查询列表中首个匹配元素下标的命令加入当前批次，不改变列表。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `val`：按编码后的字节匹配的元素值。
    ///
    /// 返回：接纳后返回从零开始的可选下标票据，键或匹配元素缺失时为 None；会话拒绝立即报错，执行错误由票据报告。
    pub fn l_pos<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<Option<i64>>> {
        self.enqueue(redis::cmd("LPOS").arg(key).arg(val).to_owned())
    }

    /// 业务作用：将替换列表指定位置元素的命令加入当前批次，不扩展列表长度。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `index`：从零开始的位置，负数从尾部倒数。
    /// - `val`：替换后的元素值。
    ///
    /// 返回：接纳后返回写入确认票据；会话拒绝立即报错，键缺失、位置越界或执行失败由票据报告。
    pub fn l_set<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        index: isize,
        val: V,
    ) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("LSET").arg(key).arg(index).arg(val).to_owned())
    }

    /// 业务作用：将向列表首个匹配基准元素之前插入新值的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，缺失时不创建。
    /// - `pivot`：从列表左侧起查找的基准元素。
    /// - `val`：待插入的新值。
    ///
    /// 返回：接纳后返回更新长度票据，键缺失为零、基准元素不存在为负一；会话拒绝立即报错，执行失败或结果未知由票据报告。
    pub fn l_insert_before<P: redis::ToSingleRedisArg, V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        pivot: P,
        val: V,
    ) -> Result<Ticket<i64>> {
        self.enqueue(
            redis::cmd("LINSERT")
                .arg(key)
                .arg("BEFORE")
                .arg(pivot)
                .arg(val)
                .to_owned(),
        )
    }

    /// 业务作用：将向列表首个匹配基准元素之后插入新值的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标列表键，缺失时不创建。
    /// - `pivot`：从列表左侧起查找的基准元素。
    /// - `val`：待插入的新值。
    ///
    /// 返回：接纳后返回更新长度票据，键缺失为零、基准元素不存在为负一；会话拒绝立即报错，执行失败或结果未知由票据报告。
    pub fn l_insert_after<P: redis::ToSingleRedisArg, V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        pivot: P,
        val: V,
    ) -> Result<Ticket<i64>> {
        self.enqueue(
            redis::cmd("LINSERT")
                .arg(key)
                .arg("AFTER")
                .arg(pivot)
                .arg(val)
                .to_owned(),
        )
    }

    // set(其余)
    /// 业务作用：将多个成员加入集合的命令加入当前批次，服务端按成员字节去重。
    ///
    /// 参数说明：
    /// - `key`：目标集合键，缺失时由非空写入创建。
    /// - `members`：待加入的成员；已有成员不计入新增数量。
    ///
    /// 返回：空输入直接返回零值完成票据；非空接纳后返回新增成员数量票据，会话拒绝立即报错，执行错误由票据报告。
    pub fn s_add_multi(&mut self, key: &str, members: &[&[u8]]) -> Result<Ticket<u64>> {
        if members.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("SADD");
        c.arg(key);
        for m in members {
            c.arg(*m);
        }
        self.enqueue(c)
    }

    /// 业务作用：将从集合删除多个指定成员的命令加入当前批次，不存在的成员不影响其它删除。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `members`：待删除的成员，重复项不会重复计数。
    ///
    /// 返回：空输入直接返回零值完成票据；非空接纳后返回实际删除数量票据，键缺失为零，会话拒绝立即报错，执行错误由票据报告。
    pub fn s_rem_multi(&mut self, key: &str, members: &[&[u8]]) -> Result<Ticket<u64>> {
        if members.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("SREM");
        c.arg(key);
        for m in members {
            c.arg(*m);
        }
        self.enqueue(c)
    }

    /// 业务作用：将单个成员从源集合原子移至目标集合的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `src`：源集合键。
    /// - `dst`：目标集合键；Cluster 下调用方须保证与源键同槽。
    /// - `member`：待移动的成员，目标中已存在时仍可从源删除。
    ///
    /// 返回：接纳后返回是否发生移动的票据，源中缺少成员时为 false；会话拒绝立即报错，类型、路由或执行错误由票据报告。
    pub fn s_move<V: redis::ToSingleRedisArg>(
        &mut self,
        src: &str,
        dst: &str,
        member: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("SMOVE").arg(src).arg(dst).arg(member).to_owned())
    }

    /// 业务作用：将多个成员的集合归属查询加入当前批次，保留输入位置与结果位置的对应关系。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `members`：逐项查询的成员，允许重复。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空接纳后返回布尔数组票据，缺失键的各项为 false，会话拒绝立即报错，执行错误由票据报告。
    pub fn s_mis_member(&mut self, key: &str, members: &[&[u8]]) -> Result<Ticket<Vec<bool>>> {
        if members.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("SMISMEMBER");
        c.arg(key);
        for m in members {
            c.arg(*m);
        }
        self.enqueue(c)
    }

    /// 业务作用：将随机读取单个集合成员的命令加入当前批次，读取后保留成员。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    ///
    /// 返回：接纳后返回可选成员票据，键缺失时为 None；会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn s_rand_member<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("SRANDMEMBER").arg(key).to_owned())
    }

    /// 业务作用：将首个集合相对其余集合的差集查询加入当前批次，不写入结果集合。
    ///
    /// 参数说明：
    /// - `keys`：顺序有意义的集合键列表，首项为被减集合；Cluster 下须同槽。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空接纳后返回无顺序保证的成员列表票据，缺失键按空集合处理，会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn s_diff<T: FromRedisValue>(&mut self, keys: &[&str]) -> Result<Ticket<Vec<T>>> {
        if keys.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("SDIFF");
        for k in keys {
            c.arg(*k);
        }
        self.enqueue(c)
    }

    /// 业务作用：将多个集合的交集查询加入当前批次，不改变源集合。
    ///
    /// 参数说明：
    /// - `keys`：待求交集的集合键列表；Cluster 下须同槽。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空接纳后返回无顺序保证的成员列表票据，任一缺失键使交集为空，会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn s_inter<T: FromRedisValue>(&mut self, keys: &[&str]) -> Result<Ticket<Vec<T>>> {
        if keys.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("SINTER");
        for k in keys {
            c.arg(*k);
        }
        self.enqueue(c)
    }

    /// 业务作用：将多个集合的并集查询加入当前批次，不写入结果集合。
    ///
    /// 参数说明：
    /// - `keys`：待求并集的集合键列表；Cluster 下须同槽。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空接纳后返回去重且无顺序保证的成员列表票据，缺失键按空集合处理，会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn s_union<T: FromRedisValue>(&mut self, keys: &[&str]) -> Result<Ticket<Vec<T>>> {
        if keys.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("SUNION");
        for k in keys {
            c.arg(*k);
        }
        self.enqueue(c)
    }

    // zset(其余)
    /// 业务作用：将按升序排名区间删除有序集合成员的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：从零开始的起始排名，包含该位置；负数从尾部倒数。
    /// - `stop`：包含在删除范围内的结束排名，负数从尾部倒数。
    ///
    /// 返回：接纳后返回实际删除数量票据，缺失键或空区间为零；会话拒绝立即报错，执行失败或结果未知由票据报告。
    pub fn z_rem_range(&mut self, key: &str, start: isize, stop: isize) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZREMRANGEBYRANK")
                .arg(key)
                .arg(start)
                .arg(stop)
                .to_owned(),
        )
    }

    /// 业务作用：将按成员字典序区间删除的命令加入当前批次，仅适用于成员分值相同的有序集合。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键，调用方须保证成员具有相同分值。
    /// - `min`：字节字典序下界，以 `[` 包含或 `(` 排除边界，`-` 表示无下界。
    /// - `max`：字节字典序上界，以 `[` 包含或 `(` 排除边界，`+` 表示无上界。
    ///
    /// 返回：接纳后返回实际删除数量票据，缺失键或空区间为零；会话拒绝立即报错，非法边界或执行错误由票据报告。
    pub fn z_rem_range_by_lex(&mut self, key: &str, min: &str, max: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZREMRANGEBYLEX")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// 业务作用：将按闭合分值区间删除有序集合成员的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `min`：包含在删除范围内的最低分值。
    /// - `max`：包含在删除范围内的最高分值。
    ///
    /// 返回：接纳后返回实际删除数量票据，缺失键或空区间为零；会话拒绝立即报错，非法数值或执行错误由票据报告。
    pub fn z_rem_range_by_score(&mut self, key: &str, min: f64, max: f64) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZREMRANGEBYSCORE")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// 业务作用：将多个有序集合成员的分值查询加入当前批次，逐项保留输入顺序。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `members`：待查询的成员，重复项各占一个结果位置。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空接纳后返回可选分值数组票据，缺失成员对应 None，会话拒绝立即报错，执行或转换错误由票据报告。
    pub fn z_m_score(&mut self, key: &str, members: &[&[u8]]) -> Result<Ticket<Vec<Option<f64>>>> {
        if members.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("ZMSCORE");
        c.arg(key);
        for m in members {
            c.arg(*m);
        }
        self.enqueue(c)
    }

    /// 业务作用：将按成员字典序统计区间数量的命令加入当前批次，仅适用于成员分值相同的有序集合。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键，调用方须保证成员具有相同分值。
    /// - `min`：字典序下界，`[` 包含、`(` 排除，`-` 表示无下界。
    /// - `max`：字典序上界，`[` 包含、`(` 排除，`+` 表示无上界。
    ///
    /// 返回：接纳后返回匹配数量票据，缺失键或空区间为零；会话拒绝立即报错，非法边界或执行错误由票据报告。
    pub fn z_lex_count(&mut self, key: &str, min: &str, max: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZLEXCOUNT")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// 业务作用：将按升序排名读取有序集合成员的命令加入当前批次，不返回分值。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：从最低分成员起算的零基起始排名，包含该位置；负数从尾部倒数。
    /// - `stop`：包含在读取范围内的结束排名，负数从尾部倒数。
    ///
    /// 返回：接纳后返回按分值、同分值按成员字节升序排列的票据，缺失键或空区间为空；会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn z_range<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .to_owned(),
        )
    }

    /// 业务作用：将按降序排名读取有序集合成员的命令加入当前批次，不返回分值。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：从最高分成员起算的零基起始排名，包含该位置；负数从该顺序尾部倒数。
    /// - `stop`：包含在读取范围内的结束排名，负数从该顺序尾部倒数。
    ///
    /// 返回：接纳后返回按分值、同分值按成员字节降序排列的票据，缺失键或空区间为空；会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn z_rev_range<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZREVRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .to_owned(),
        )
    }

    /// 业务作用：将按升序排名读取成员及其分值的命令加入当前批次，保持成员与分值配对。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：从最低分成员起算的零基起始排名，包含该位置；负数从尾部倒数。
    /// - `stop`：包含在读取范围内的结束排名，负数从尾部倒数。
    ///
    /// 返回：接纳后返回按分值及同分成员字节升序排列的配对列表票据，缺失键或空区间为空；会话拒绝立即报错，执行或转换错误由票据报告。
    pub fn z_range_with_scores<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(
            redis::cmd("ZRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .arg("WITHSCORES")
                .to_owned(),
        )
    }

    /// 业务作用：将按降序排名读取成员及其分值的命令加入当前批次，保持成员与分值配对。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `start`：从最高分成员起算的零基起始排名，包含该位置；负数从该顺序尾部倒数。
    /// - `stop`：包含在读取范围内的结束排名，负数从该顺序尾部倒数。
    ///
    /// 返回：接纳后返回按分值及同分成员字节降序排列的配对列表票据，缺失键或空区间为空；会话拒绝立即报错，执行或转换错误由票据报告。
    pub fn z_rev_range_with_scores<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(
            redis::cmd("ZREVRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .arg("WITHSCORES")
                .to_owned(),
        )
    }

    /// 业务作用：将按闭合分值区间升序读取成员及分值的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `min`：包含在读取范围内的最低分值。
    /// - `max`：包含在读取范围内的最高分值。
    ///
    /// 返回：接纳后返回按分值及同分成员字节升序排列的配对列表票据，缺失键或空区间为空；会话拒绝立即报错，非法数值、执行或转换错误由票据报告。
    pub fn z_range_by_score_with_scores<T: FromRedisValue>(
        &mut self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(
            redis::cmd("ZRANGEBYSCORE")
                .arg(key)
                .arg(min)
                .arg(max)
                .arg("WITHSCORES")
                .to_owned(),
        )
    }

    /// 业务作用：将按闭合分值区间降序读取成员的命令加入当前批次，命令参数按上界在前排列。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `max`：包含在读取范围内的最高分值。
    /// - `min`：包含在读取范围内的最低分值。
    ///
    /// 返回：接纳后返回按分值及同分成员字节降序排列的成员票据，缺失键或空区间为空；会话拒绝立即报错，非法数值、执行或转换错误由票据报告。
    pub fn z_rev_range_by_score<T: FromRedisValue>(
        &mut self,
        key: &str,
        max: f64,
        min: f64,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg(key)
                .arg(max)
                .arg(min)
                .to_owned(),
        )
    }

    /// 业务作用：将按闭合分值区间降序读取成员及分值的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `max`：包含在读取范围内的最高分值，在线路参数中位于下界之前。
    /// - `min`：包含在读取范围内的最低分值。
    ///
    /// 返回：接纳后返回按分值及同分成员字节降序排列的配对列表票据，缺失键或空区间为空；会话拒绝立即报错，非法数值、执行或转换错误由票据报告。
    pub fn z_rev_range_by_score_with_scores<T: FromRedisValue>(
        &mut self,
        key: &str,
        max: f64,
        min: f64,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg(key)
                .arg(max)
                .arg(min)
                .arg("WITHSCORES")
                .to_owned(),
        )
    }

    /// 业务作用：将按成员字典序区间升序读取的命令加入当前批次，仅适用于成员分值相同的有序集合。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键，调用方须保证成员具有相同分值。
    /// - `min`：字典序下界，`[` 包含、`(` 排除，`-` 表示无下界。
    /// - `max`：字典序上界，`[` 包含、`(` 排除，`+` 表示无上界。
    ///
    /// 返回：接纳后返回按成员字节升序排列的票据，缺失键或空区间为空；会话拒绝立即报错，非法边界、执行或转换错误由票据报告。
    pub fn z_range_by_lex<T: FromRedisValue>(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZRANGEBYLEX")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// 业务作用：将按分值从低到高弹出有序集合成员的命令加入当前批次，读取同时移除成员。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `count`：最多弹出的成员数，服务端校验数量范围。
    ///
    /// 返回：接纳后返回被移除成员与分值的配对列表票据，缺失键为空；会话拒绝立即报错，执行或转换错误由票据报告，转换失败不会回滚弹出。
    pub fn z_pop_min<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(redis::cmd("ZPOPMIN").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将按分值从高到低弹出有序集合成员的命令加入当前批次，读取同时移除成员。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `count`：最多弹出的成员数，服务端校验数量范围。
    ///
    /// 返回：接纳后返回被移除成员与分值的配对列表票据，缺失键为空；会话拒绝立即报错，执行或转换错误由票据报告，转换失败不会回滚弹出。
    pub fn z_pop_max<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(redis::cmd("ZPOPMAX").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将随机读取单个有序集合成员的命令加入当前批次，不删除成员也不返回分值。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    ///
    /// 返回：接纳后返回可选成员票据，缺失键为 None；会话拒绝立即报错，执行或成员转换错误由票据报告。
    pub fn z_rand_member<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("ZRANDMEMBER").arg(key).to_owned())
    }

    // stream / script / pubsub(其余)
    /// 业务作用：将按精确 ID 下界裁剪 Stream 正文的命令加入当前批次，不检查消费组的待确认记录。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    /// - `minid`：保留的最小 ID，严格小于它的条目会被删除；格式由 Redis 校验。
    ///
    /// 返回：接纳后返回实际删除数量票据，缺失键为零；会话拒绝立即报错，执行错误由票据报告。调用方须自行保护尚需重读的正文。
    pub fn x_trim_minid(&mut self, stream: &str, minid: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("XTRIM")
                .arg(stream)
                .arg("MINID")
                .arg(minid)
                .to_owned(),
        )
    }

    /// 业务作用：将按 ID 下界近似裁剪 Stream 的命令加入当前批次，以整块回收减少裁剪开销。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键。
    /// - `minid`：保留的最小 ID，近似裁剪可能继续保留部分更早条目。
    ///
    /// 返回：接纳后返回实际删除数量票据；会话拒绝立即报错，执行错误由票据报告。该命令不检查待确认记录，不能单独保证消费恢复所需正文仍在。
    pub fn x_trim_minid_approx(&mut self, stream: &str, minid: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("XTRIM")
                .arg(stream)
                .arg("MINID")
                .arg("~")
                .arg(minid)
                .to_owned(),
        )
    }

    /// 业务作用：将清空 Lua 脚本缓存的命令加入显式会话；Cluster 下只作用于本批实际选中的节点。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：接纳后返回清空确认票据，会话拒绝立即报错，执行错误由票据报告；成功不保证集群其它节点缓存为空。
    pub fn script_flush(&mut self) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("SCRIPT").arg("FLUSH").to_owned())
    }

    /// 业务作用：将终止当前 Lua 脚本的请求加入显式会话；Cluster 下只尝试本批实际选中的节点。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：接纳后返回终止确认票据；会话拒绝立即报错，无脚本运行、脚本已写入而不可终止或其它执行错误由票据报告。
    pub fn script_kill(&mut self) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("SCRIPT").arg("KILL").to_owned())
    }

    /// 业务作用：将 Lua 脚本载入服务端缓存的命令加入显式会话，不执行脚本。
    ///
    /// 参数说明：
    /// - `script`：待缓存的 Lua 脚本文本。
    ///
    /// 返回：接纳后返回脚本 SHA1 票据；会话拒绝立即报错，加载失败由票据报告。Cluster 下不广播加载，不能据此保证其它节点的 EVALSHA 可命中。
    pub fn script_load(&mut self, script: &str) -> Result<Ticket<String>> {
        self.enqueue(redis::cmd("SCRIPT").arg("LOAD").arg(script).to_owned())
    }

    /// 业务作用：将按 SHA1 检查 Lua 脚本缓存的命令加入显式会话。
    ///
    /// 参数说明：
    /// - `shas`：待查询的脚本摘要，结果位置与输入位置对应。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空接纳后返回缓存存在性票据，会话拒绝立即报错，执行错误由票据报告。Cluster 下只反映本批选中节点的当次状态。
    pub fn script_exists(&mut self, shas: &[&str]) -> Result<Ticket<Vec<bool>>> {
        if shas.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let mut c = redis::cmd("SCRIPT");
        c.arg("EXISTS");
        for s in shas {
            c.arg(*s);
        }
        self.enqueue(c)
    }

    // 支持 count 参数的批量弹出入口。
    /// 业务作用：将从列表左端批量弹出元素的命令加入当前批次，返回顺序与弹出顺序一致。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `count`：最多弹出的元素数，在线路上保留原值交由 Redis 校验。
    ///
    /// 返回：接纳后返回元素列表票据，缺失键解码为空列表；会话拒绝立即报错，执行或转换错误由票据报告，转换失败不会回滚弹出，结果未知时不自动重放。
    pub fn l_pop_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("LPOP").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将从列表右端批量弹出元素的命令加入当前批次，返回顺序从尾部向前排列。
    ///
    /// 参数说明：
    /// - `key`：目标列表键。
    /// - `count`：最多弹出的元素数，在线路上保留原值交由 Redis 校验。
    ///
    /// 返回：接纳后返回元素列表票据，缺失键解码为空列表；会话拒绝立即报错，执行或转换错误由票据报告，转换失败不会回滚弹出，结果未知时不自动重放。
    pub fn r_pop_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("RPOP").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将随机移除集合中至多指定数量成员的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `count`：最多移除的成员数，超过集合大小时仅返回已有成员。
    ///
    /// 返回：接纳后返回不重复且无顺序保证的成员列表票据，缺失键为空；会话拒绝立即报错，执行或转换错误由票据报告，转换失败不会恢复已移除成员。
    pub fn s_pop_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("SPOP").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将按指定数量随机读取集合成员的命令加入当前批次，保留原集合。
    ///
    /// 参数说明：
    /// - `key`：目标集合键。
    /// - `count`：正数表示最多返回该数量的不重复成员，负数表示允许重复并返回其绝对值个，零表示空结果。
    ///
    /// 返回：接纳后返回随机成员列表票据，缺失键为空；会话拒绝立即报错，非法数量、执行或成员转换错误由票据报告。
    pub fn s_rand_member_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("SRANDMEMBER").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将按指定数量随机读取有序集合成员的命令加入当前批次，不返回分值或删除成员。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `count`：正数表示最多返回该数量的不重复成员，负数表示允许重复并返回其绝对值个，零表示空结果。
    ///
    /// 返回：接纳后返回随机成员列表票据，缺失键为空；会话拒绝立即报错，非法数量、执行或成员转换错误由票据报告。
    pub fn z_rand_member_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("ZRANDMEMBER").arg(key).arg(count).to_owned())
    }

    /// 业务作用：将按指定数量随机读取 Hash 字段名的命令加入当前批次，不读取字段值或删除字段。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `count`：正数表示最多返回该数量的不重复字段，负数表示允许重复并返回其绝对值个，零表示空结果。
    ///
    /// 返回：接纳后返回字段名列表票据，缺失键为空；会话拒绝立即报错，非法数量、执行或字段名 UTF-8 转换错误由票据报告。
    pub fn h_rand_field_count(&mut self, key: &str, count: isize) -> Result<Ticket<Vec<String>>> {
        self.enqueue(redis::cmd("HRANDFIELD").arg(key).arg(count).to_owned())
    }

    // ── zset LIMIT 分页 / 区间字符串重载──
    /// 业务作用：将按分值区间升序分页读取成员的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `min`：分值下界，数字默认包含，前缀 `(` 排除边界，支持 `-inf`。
    /// - `max`：分值上界，数字默认包含，前缀 `(` 排除边界，支持 `+inf`。
    /// - `offset`：跳过的匹配成员数量，应为非负；深分页仍须遍历被跳过的成员。
    /// - `count`：最多返回的成员数，负数表示读取偏移后的全部匹配成员。
    ///
    /// 返回：接纳后返回按分值及同分成员字节升序排列的成员票据，缺失键或空页为空；会话拒绝立即报错，非法参数、执行或转换错误由票据报告。
    pub fn z_range_by_score_limit<T: FromRedisValue>(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        offset: isize,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZRANGEBYSCORE")
                .arg(key)
                .arg(min)
                .arg(max)
                .arg("LIMIT")
                .arg(offset)
                .arg(count)
                .to_owned(),
        )
    }

    /// 业务作用：将按分值区间降序分页读取成员的命令加入当前批次，上界参数位于下界之前。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键。
    /// - `max`：分值上界，数字默认包含，前缀 `(` 排除边界，支持 `+inf`。
    /// - `min`：分值下界，数字默认包含，前缀 `(` 排除边界，支持 `-inf`。
    /// - `offset`：在降序匹配结果中跳过的数量，应为非负；深分页仍须遍历被跳过的成员。
    /// - `count`：最多返回的成员数，负数表示读取偏移后的全部匹配成员。
    ///
    /// 返回：接纳后返回按分值及同分成员字节降序排列的成员票据，缺失键或空页为空；会话拒绝立即报错，非法参数、执行或转换错误由票据报告。
    pub fn z_rev_range_by_score_limit<T: FromRedisValue>(
        &mut self,
        key: &str,
        max: &str,
        min: &str,
        offset: isize,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg(key)
                .arg(max)
                .arg(min)
                .arg("LIMIT")
                .arg(offset)
                .arg(count)
                .to_owned(),
        )
    }

    /// 业务作用：将按字典序区间升序分页读取成员的命令加入当前批次，仅适用于成员分值相同的有序集合。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键，调用方须保证成员具有相同分值。
    /// - `min`：字典序下界，`[` 包含、`(` 排除，`-` 表示无下界。
    /// - `max`：字典序上界，`[` 包含、`(` 排除，`+` 表示无上界。
    /// - `offset`：跳过的匹配成员数量，应为非负；深分页仍须遍历被跳过的成员。
    /// - `count`：最多返回的成员数，负数表示读取偏移后的全部匹配成员。
    ///
    /// 返回：接纳后返回按成员字节升序排列的票据，缺失键或空页为空；会话拒绝立即报错，非法参数、执行或转换错误由票据报告。
    pub fn z_range_by_lex_limit<T: FromRedisValue>(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        offset: isize,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(
            redis::cmd("ZRANGEBYLEX")
                .arg(key)
                .arg(min)
                .arg(max)
                .arg("LIMIT")
                .arg(offset)
                .arg(count)
                .to_owned(),
        )
    }

    // ── stream 读重载──
    /// 业务作用：将按 ID 升序读取 Stream 区间的命令加入当前批次，不改变消费组游标或待确认记录。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `start`：读取下界，支持 `-` 表示无下界；具体 ID 默认包含，前缀 `(` 表示排除。
    /// - `end`：读取上界，支持 `+` 表示无上界；具体 ID 默认包含，前缀 `(` 表示排除。
    ///
    /// 返回：接纳后返回由 T 解码的条目及字段集合票据，缺失键产生空响应；会话拒绝立即报错，非法 ID、执行或转换错误由票据报告。
    pub fn x_range<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: &str,
        end: &str,
    ) -> Result<Ticket<T>> {
        self.enqueue(redis::cmd("XRANGE").arg(key).arg(start).arg(end).to_owned())
    }

    /// 业务作用：将带数量上限的 Stream 升序区间读取加入当前批次，不登记消息消费责任。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `start`：ID 下界，`-` 表示无下界，前缀 `(` 排除该 ID。
    /// - `end`：ID 上界，`+` 表示无上界，前缀 `(` 排除该 ID。
    /// - `count`：最多返回的条目数，服务端校验范围。
    ///
    /// 返回：接纳后返回由 T 解码的升序条目票据，缺失键产生空响应；会话拒绝立即报错，参数、执行或转换错误由票据报告。
    pub fn x_range_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: &str,
        end: &str,
        count: usize,
    ) -> Result<Ticket<T>> {
        self.enqueue(
            redis::cmd("XRANGE")
                .arg(key)
                .arg(start)
                .arg(end)
                .arg("COUNT")
                .arg(count)
                .to_owned(),
        )
    }

    /// 业务作用：将按 ID 降序读取 Stream 区间的命令加入当前批次，上界在线路参数中先发送。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `end`：ID 上界，`+` 表示无上界，具体 ID 默认包含，前缀 `(` 表示排除。
    /// - `start`：ID 下界，`-` 表示无下界，具体 ID 默认包含，前缀 `(` 表示排除。
    ///
    /// 返回：接纳后返回由 T 解码的降序条目票据，缺失键产生空响应；会话拒绝立即报错，参数、执行或转换错误由票据报告，不改变消费组状态。
    pub fn x_rev_range<T: FromRedisValue>(
        &mut self,
        key: &str,
        end: &str,
        start: &str,
    ) -> Result<Ticket<T>> {
        self.enqueue(
            redis::cmd("XREVRANGE")
                .arg(key)
                .arg(end)
                .arg(start)
                .to_owned(),
        )
    }

    /// 业务作用：将带数量上限的 Stream 降序区间读取加入当前批次，不改变消费组状态。
    ///
    /// 参数说明：
    /// - `key`：目标 Stream 键。
    /// - `end`：ID 上界，`+` 表示无上界，前缀 `(` 排除该 ID。
    /// - `start`：ID 下界，`-` 表示无下界，前缀 `(` 排除该 ID。
    /// - `count`：最多返回的条目数，服务端校验范围。
    ///
    /// 返回：接纳后返回由 T 解码的降序条目票据，缺失键产生空响应；会话拒绝立即报错，参数、执行或转换错误由票据报告。
    pub fn x_rev_range_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        end: &str,
        start: &str,
        count: usize,
    ) -> Result<Ticket<T>> {
        self.enqueue(
            redis::cmd("XREVRANGE")
                .arg(key)
                .arg(end)
                .arg(start)
                .arg("COUNT")
                .arg(count)
                .to_owned(),
        )
    }

    /// 业务作用：将 Redis Pub/Sub 频道广播加入当前批次，消息不持久化，也不等待订阅者完成业务处理。
    ///
    /// 参数说明：
    /// - `channel`：接收广播的频道名。
    /// - `message`：按单个 Redis 参数编码的消息体。
    ///
    /// 返回：接纳后返回仅接受非负 Integer 的订阅计数票据；会话拒绝立即报错，执行失败、结果未知或回复不符由票据报告。
    /// 数量不等同于业务消费确认；Cluster 下仅统计被路由节点上的订阅者，回复异常不触发自动重放。
    pub fn publish_channel<V: redis::ToSingleRedisArg>(
        &mut self,
        channel: &str,
        message: V,
    ) -> Result<Ticket<u64>> {
        let mut ticket =
            self.enqueue(redis::cmd("PUBLISH").arg(channel).arg(message).to_owned())?;
        ticket.reply_check = ReplyCheck::PublishCount;
        Ok(ticket)
    }

    /// 业务作用：将包含业务信封的 Stream 事件追加加入当前批次，沿用直接发布入口的校验和编码。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream 键，同时写入信封 topic，不能为空。
    /// - `event`：条目的字段名，供消费侧选择处理器，不能为空。
    /// - `message`：序列化为信封 data 的业务值，不允许 JSON null。
    ///
    /// 返回：校验、编码或会话拒绝时立即报错；接纳后返回非零完整 XADD ID 的字符串票据。
    /// 执行或回复校验错误由票据报告，结果未知不自动重放。
    pub fn publish<T: serde::Serialize>(
        &mut self,
        stream: &str,
        event: &str,
        message: &T,
    ) -> Result<Ticket<String>> {
        // 校验 + 信封编码走 stream 模块的**共享 helper**(与 `RedisClient::publish` 同一路径,
        // 逐字节一致);这里只负责把 XADD 排进 pipeline。
        let body = crate::stream::stream_publish_body(stream, event, message)?;
        let mut ticket = self.enqueue(
            redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg(event)
                .arg(body)
                .to_owned(),
        )?;
        ticket.reply_check = ReplyCheck::StreamId;
        Ok(ticket)
    }

    /// 业务作用：使用默认事件字段 STREAM_EVENT 将业务信封追加到 Stream 的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `stream`：非空的目标 Stream 键。
    /// - `message`：序列化为信封 data 的业务值，不允许 JSON null。
    ///
    /// 返回：校验、编码或会话拒绝时立即报错；接纳后返回 XADD ID 字符串票据，执行或解码错误由票据报告。
    pub fn publish_default<T: serde::Serialize>(
        &mut self,
        stream: &str,
        message: &T,
    ) -> Result<Ticket<String>> {
        self.publish(stream, crate::stream::STREAM_EVENT, message)
    }

    /// 业务作用：通过与 publish 相同的校验和编码，将指定事件的业务信封追加加入当前批次。
    ///
    /// 参数说明：
    /// - `stream`：非空的目标 Stream 键。
    /// - `event`：非空的事件字段名。
    /// - `data`：序列化为信封 data 的业务值，不允许 JSON null。
    ///
    /// 返回：校验、编码或会话拒绝时立即报错；接纳后返回 XADD ID 字符串票据，执行或解码错误由票据报告。
    pub fn publish_stream<T: serde::Serialize>(
        &mut self,
        stream: &str,
        event: &str,
        data: &T,
    ) -> Result<Ticket<String>> {
        self.publish(stream, event, data)
    }

    /// 业务作用：使用默认事件字段将业务信封追加加入当前批次，复用 publish_default 的接纳与错误语义。
    ///
    /// 参数说明：
    /// - `stream`：非空的目标 Stream 键。
    /// - `data`：序列化为信封 data 的业务值，不允许 JSON null。
    ///
    /// 返回：校验、编码或会话拒绝时立即报错；接纳后返回 XADD ID 字符串票据，执行或解码错误由票据报告。
    pub fn publish_stream_default<T: serde::Serialize>(
        &mut self,
        stream: &str,
        data: &T,
    ) -> Result<Ticket<String>> {
        self.publish_default(stream, data)
    }

    /// 业务作用：将单键 Lua 自减及非正余值删字段的操作加入当前批次，脚本内的读改写不与其它命令交错。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，也是脚本的唯一 KEYS 路由键。
    /// - `field`：待更新的字段，缺失时以零为初值。
    /// - `delta`：有符号减量，负数表示增加；i64 最小值无法转为 HINCRBY 的增量。
    ///
    /// 返回：不可取负的步长或会话拒绝立即报错；接纳后返回删除前余值的票据，字段被删除不改变结果。
    /// 非法字段值、结果溢出或脚本执行失败由票据报告；Redis 脚本错误不回滚已经完成的命令，结果未知时不得自动重放。
    pub fn h_decr_by_and_del(&mut self, key: &str, field: &str, delta: i64) -> Result<Ticket<i64>> {
        // 反向增量必须能由 HINCRBY 表示；先拒绝不可取负步长，避免已接纳命令和容量裁决受到无效更新影响。
        delta.checked_neg().ok_or_else(|| {
            NasaRedisError::Config("h_decr_by_and_del: delta 无法在 i64 范围内取负".into())
        })?;
        self.enqueue(
            redis::cmd("EVAL")
                .arg(H_DECR_BY_AND_DEL_LUA)
                .arg(1)
                .arg(key)
                .arg(field)
                .arg(delta)
                .to_owned(),
        )
    }

    /// 业务作用：将 Hash 字段减一并在非正余值时删除的单键脚本加入当前批次，复用有符号减量入口。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：待更新字段，缺失时以零为初值。
    ///
    /// 返回：接纳后返回删除前精确余值票据；会话拒绝立即报错，字段值、溢出或脚本错误由票据报告。需要 HGET、HINCRBY 及实际删除时的 HDEL 权限，脚本错误不承诺回滚。
    pub fn h_decr_by_and_del_one(&mut self, key: &str, field: &str) -> Result<Ticket<i64>> {
        self.h_decr_by_and_del(key, field, 1)
    }

    /// 业务作用：将单个 Hash 字段的相对过期设置加入当前批次，要求服务端支持 HEXPIRE。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `field`：待设置有效期的字段。
    /// - `ttl`：从服务端执行时起算的有效期，按整秒截断；不足一秒变为零并立即删除字段。
    ///
    /// 返回：秒数超过 i64 范围或会话拒绝时立即报错；接纳后返回单项状态数组票据，1 表示设置、2 表示删除、-2 表示字段缺失，执行错误由票据报告。需要 Redis 7.4 或更高版本。
    pub fn h_expire(&mut self, key: &str, field: &str, ttl: Duration) -> Result<Ticket<Vec<i64>>> {
        let seconds = crate::commands::duration_to_redis_seconds(ttl)?;
        self.enqueue(
            redis::cmd("HEXPIRE")
                .arg(key)
                .arg(seconds)
                .arg("FIELDS")
                .arg(1)
                .arg(field)
                .to_owned(),
        )
    }

    /// 业务作用：将同一 Hash 的多个字段过期设置合为一条命令加入当前批次。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键。
    /// - `fields`：待设置有效期的字段列表，结果位置与参数位置对应。
    /// - `ttl`：从服务端执行时起算的有效期，按整秒截断；不足一秒时删除已存在的目标字段。
    ///
    /// 返回：空输入直接返回空结果完成票据；非空输入在秒数越界或会话拒绝时立即报错，接纳后返回状态数组票据，1 为设置、2 为删除、-2 为缺失，执行错误由票据报告。需要 Redis 7.4 或更高版本。
    pub fn h_expire_multi(
        &mut self,
        key: &str,
        fields: &[&str],
        ttl: Duration,
    ) -> Result<Ticket<Vec<i64>>> {
        if fields.is_empty() {
            return Ok(Ticket::ready(redis::Value::Array(vec![])));
        }
        let seconds = crate::commands::duration_to_redis_seconds(ttl)?;
        let mut c = redis::cmd("HEXPIRE");
        c.arg(key).arg(seconds).arg("FIELDS").arg(fields.len());
        for f in fields {
            c.arg(*f);
        }
        self.enqueue(c)
    }

    // search feature 提供 RedisJSON 与 RediSearch 命令。
    /// 业务作用：将按路径写入 RedisJSON 文档的命令加入当前批次，JSON 与路径合法性由服务端校验。
    ///
    /// 参数说明：
    /// - `key`：目标 JSON 文档键，创建新文档时路径须指向根。
    /// - `path`：要替换或新增值的 RedisJSON 路径。
    /// - `json`：待写入的 JSON 文本，不在本地重新编码。
    ///
    /// 返回：接纳后返回单位类型票据；会话拒绝立即报错，执行错误由票据报告。单位结果不区分 OK 与未能新增路径的空响应，不能作为路径确已更新的数量证明。
    #[cfg(feature = "search")]
    pub fn json_set(&mut self, key: &str, path: &str, json: &str) -> Result<Ticket<()>> {
        self.enqueue(
            redis::cmd("JSON.SET")
                .arg(key)
                .arg(path)
                .arg(json)
                .to_owned(),
        )
    }

    /// 业务作用：将按路径读取 RedisJSON 文档的命令加入当前批次，不在本地解析返回的 JSON 正文。
    ///
    /// 参数说明：
    /// - `key`：目标 JSON 文档键。
    /// - `path`：RedisJSON 查询路径，匹配形态由路径语法决定。
    ///
    /// 返回：接纳后返回可选字符串票据，服务端空响应为 None；会话拒绝立即报错，路径、执行或字符串转换错误由票据报告。
    #[cfg(feature = "search")]
    pub fn json_get(&mut self, key: &str, path: &str) -> Result<Ticket<Option<String>>> {
        self.enqueue(redis::cmd("JSON.GET").arg(key).arg(path).to_owned())
    }

    /// 业务作用：将删除 RedisJSON 指定路径的命令加入当前批次，根路径删除可移除整个文档键。
    ///
    /// 参数说明：
    /// - `key`：目标 JSON 文档键。
    /// - `path`：待删除的 RedisJSON 路径，允许匹配多个位置。
    ///
    /// 返回：接纳后返回删除数量票据，缺失键或无匹配时为零；会话拒绝立即报错，路径或执行错误由票据报告，结果未知时不自动重放。
    #[cfg(feature = "search")]
    pub fn json_del(&mut self, key: &str, path: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("JSON.DEL").arg(key).arg(path).to_owned())
    }

    /// 业务作用：将整数步长的 JSON 数值增量加入当前批次，参数直接编码为十进制整数，避免本地先转为 f64。
    ///
    /// 参数说明：
    /// - `key`：目标 JSON 文档键。
    /// - `path`：待递增的数值路径，可匹配多个位置。
    /// - `delta`：有符号整数步长；最终数值形态和范围由 RedisJSON 的现值及运算规则决定。
    ///
    /// 返回：接纳后返回按字符串转换的服务端结果票据，不保证任意现值都保持整数类型；会话拒绝立即报错，执行或转换错误由票据报告，结果未知不自动重放。
    #[cfg(feature = "search")]
    pub fn json_num_incr_by(
        &mut self,
        key: &str,
        path: &str,
        delta: i64,
    ) -> Result<Ticket<String>> {
        self.enqueue(
            redis::cmd("JSON.NUMINCRBY")
                .arg(key)
                .arg(path)
                .arg(delta)
                .to_owned(),
        )
    }

    /// 业务作用：将浮点步长的 JSON 数值增量加入当前批次，适用于允许浮点结果的业务字段。
    ///
    /// 参数说明：
    /// - `key`：目标 JSON 文档键。
    /// - `path`：待递增的数值路径，可匹配多个位置。
    /// - `delta`：有符号浮点步长，数值合法性由 RedisJSON 校验。
    ///
    /// 返回：接纳后返回按字符串转换的服务端结果票据；会话拒绝立即报错，执行或转换错误由票据报告。结果可能为浮点表示，调用方不能假定仍可解码为整数业务字段。
    #[cfg(feature = "search")]
    pub fn json_num_incr_by_f64(
        &mut self,
        key: &str,
        path: &str,
        delta: f64,
    ) -> Result<Ticket<String>> {
        self.enqueue(
            redis::cmd("JSON.NUMINCRBY")
                .arg(key)
                .arg(path)
                .arg(delta)
                .to_owned(),
        )
    }

    /// 业务作用：将 RediSearch 查询加入当前批次，保留服务端默认查询选项和原始响应结构。
    ///
    /// 参数说明：
    /// - `index`：目标 RediSearch 索引名称。
    /// - `query`：完整查询表达式，调用方负责转义或使用类型化构建器处理非可信输入。
    ///
    /// 返回：接纳后返回原始 Redis 响应票据；会话拒绝立即报错，索引、查询或执行错误由票据报告，不自动转换为业务文档。
    #[cfg(feature = "search")]
    pub fn ft_search(&mut self, index: &str, query: &str) -> Result<Ticket<redis::Value>> {
        self.enqueue(redis::cmd("FT.SEARCH").arg(index).arg(query).to_owned())
    }

    /// 业务作用：将 RediSearch 聚合查询的基础入口加入当前批次，不附加聚合步骤或分页选项。
    ///
    /// 参数说明：
    /// - `index`：目标 RediSearch 索引名称。
    /// - `query`：完整查询表达式，调用方负责参数语义及非可信输入处理。
    ///
    /// 返回：接纳后返回原始 Redis 响应票据；会话拒绝立即报错，索引、查询或执行错误由票据报告。需要其它聚合参数时使用 ft_aggregate_args。
    #[cfg(feature = "search")]
    pub fn ft_aggregate(&mut self, index: &str, query: &str) -> Result<Ticket<redis::Value>> {
        self.enqueue(redis::cmd("FT.AGGREGATE").arg(index).arg(query).to_owned())
    }

    /// 业务作用：将删除 RediSearch 索引的命令加入当前批次，保留底层文档键。
    ///
    /// 参数说明：
    /// - `index`：待删除的索引名称。
    ///
    /// 返回：接纳后返回删除确认票据；会话拒绝立即报错，索引不存在或执行失败由票据报告，不将索引删除解释为文档已删除。
    #[cfg(feature = "search")]
    pub fn ft_drop_index(&mut self, index: &str) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("FT.DROPINDEX").arg(index).to_owned())
    }

    /// 业务作用：将删除 RediSearch 索引并可选删除已索引文档的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `index`：待删除的索引名称。
    /// - `dd`：为 true 时发送 DD 删除已索引文档，为 false 时保留文档键。
    ///
    /// 返回：接纳后返回删除确认票据；会话拒绝立即报错，执行错误由票据报告。DD 按服务端已索引集合删除，不能保证尚未完成索引的文档也被移除。
    #[cfg(feature = "search")]
    pub fn ft_drop_index_dd(&mut self, index: &str, dd: bool) -> Result<Ticket<()>> {
        let mut c = redis::cmd("FT.DROPINDEX");
        c.arg(index);
        if dd {
            c.arg("DD");
        }
        self.enqueue(c)
    }

    /// 业务作用：将查询 RediSearch 索引状态和统计信息的命令加入当前批次。
    ///
    /// 参数说明：
    /// - `index`：待观察的索引名称。
    ///
    /// 返回：接纳后返回原始元信息票据，内容反映服务端当次状态；会话拒绝立即报错，索引不存在或执行失败由票据报告。
    #[cfg(feature = "search")]
    pub fn ft_info(&mut self, index: &str) -> Result<Ticket<redis::Value>> {
        self.enqueue(redis::cmd("FT.INFO").arg(index).to_owned())
    }

    // ── FT 原样 args 透传──
    /// 业务作用：将 RediSearch 查询及原样选项加入当前批次，不猜测或重排参数边界。
    ///
    /// 参数说明：
    /// - `index`：目标 RediSearch 索引名称。
    /// - `args`：索引名之后的完整参数序列，须包含查询表达式；调用方负责 SORTBY、LIMIT、DIALECT、RETURN、FILTER 等语义及非可信输入处理。
    ///
    /// 返回：接纳后返回原始 Redis 响应票据；会话拒绝立即报错，缺失或非法参数及执行错误由票据报告。
    #[cfg(feature = "search")]
    pub fn ft_search_args(&mut self, index: &str, args: &[&str]) -> Result<Ticket<redis::Value>> {
        let mut c = redis::cmd("FT.SEARCH");
        c.arg(index);
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    /// 业务作用：将 RediSearch 聚合查询的完整参数序列加入当前批次，允许调用方组合分组、规约和分页。
    ///
    /// 参数说明：
    /// - `index`：目标 RediSearch 索引名称。
    /// - `args`：索引名之后的查询及聚合选项，调用方负责参数顺序、数量、转义和资源边界。
    ///
    /// 返回：接纳后返回原始 Redis 响应票据；会话拒绝立即报错，缺失或非法参数及执行错误由票据报告。
    #[cfg(feature = "search")]
    pub fn ft_aggregate_args(
        &mut self,
        index: &str,
        args: &[&str],
    ) -> Result<Ticket<redis::Value>> {
        let mut c = redis::cmd("FT.AGGREGATE");
        c.arg(index);
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    /// 业务作用：将创建 RediSearch 索引的命令加入当前批次，按调用方给定的文档范围和字段定义建立索引。
    ///
    /// 参数说明：
    /// - `index`：新索引名称。
    /// - `args`：索引名之后的完整选项和 SCHEMA 定义，包括 ON、PREFIX 等范围约束，调用方负责语义及非可信输入处理。
    ///
    /// 返回：接纳后返回创建确认票据；会话拒绝立即报错，索引冲突、参数或执行错误由票据报告，确认不表示已有文档已全部完成索引。
    #[cfg(feature = "search")]
    pub fn ft_create_args(&mut self, index: &str, args: &[&str]) -> Result<Ticket<()>> {
        let mut c = redis::cmd("FT.CREATE");
        c.arg(index);
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    /// 业务作用：收尾显式会话，先等待已封批的串行提交链，再发送仍留在会话中的尾批。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：所有批次均完成响应分发时成功；批级传输或任务异常返回首个错误，单命令服务器错误仍须从各票据读取。
    /// 取消等待不撤销已封批任务；仍留在会话缓冲区的尾批返回确定未发送，已进入派发但未获回执的命令保留结果未知。
    pub async fn execute(mut self) -> Result<()> {
        // 等齐已 seal 的后台各段(链式串行,await 链尾即等齐全部);取链上第一个批级错误。
        // 链尾 task panic/cancel → JoinError 映射成 ExecutionUnknown(不假成功)。
        let chain_res = match self.flush_chain.take() {
            Some(chain) => chain.await.unwrap_or_else(join_err_to_unknown),
            None => Ok(()),
        };
        // 等待封批链期间，尾批仍由会话独占且从未派发；只有移交尾批时才改变析构裁决，避免取消等待丢失未发送证据。
        self.executed = true;
        let cmds = std::mem::take(&mut self.cmds);
        let last = dispatch_jobs(&self.client, cmds).await;
        chain_res.and(last) // 前序段错误优先,其次末段
    }
}

impl Drop for PipelineSession {
    /// 业务作用：释放显式会话仍持有的命令和回执，已交给后台的封批继续自行完成。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仍留在会话缓冲区的命令收到确定未发送结果，包括等待已封批提交链时被取消的尾批；已经移交的命令不在本会话撤销。析构不等待 Redis 或撤销后台任务。
    fn drop(&mut self) {
        // 后台封批拥有独立责任，释放等待句柄不会撤销；未移交的尾批仍有确定未发送证据，可由调用方重新提交。
        if !self.executed && !self.cmds.is_empty() {
            let pending = self.cmds.len();
            for (_cmd, tx) in self.cmds.drain(..) {
                let _ = tx.send(Err(TicketErr::NotSent));
            }
            tracing::warn!(
                pending,
                "PipelineSession 释放尚未派发的尾批，命令确定未发送，已封批任务继续完成"
            );
        }
    }
}

// 自动微批使用有界 mpsc 队列接收多生产者命令，按时间窗与批量上限合并发送。
// 队列满时生产者异步等待容量，不在生产者内执行 Redis 命令。服务器错误按命令分发，
// 传输失败或响应数量不符返回 ExecutionUnknown；停机等待已入队命令排干。

/// 自动微批配置。
#[derive(Debug, Clone)]
pub struct MicroBatchCfg {
    /// 合批时间窗:收到首命令后等这么久收集更多。
    pub window: Duration,
    /// 单批最大命令数，到达后立即发送；零按逐条处理，不停用队列。
    pub max_batch: usize,
    /// 入队队列容量；满时生产者异步等待可用容量。
    pub queue_capacity: usize,
    /// **单命令参数字节上限**(0=不限)。超限的命令入队即拒绝(字节级背压)。
    /// 队列参数字节不超过 `queue_capacity × max_command_bytes`；不包含 Cmd 容量、RESP 编码、
    /// 当前批次、响应和等待生产者持有的内存，不能解释为进程内存上限。
    pub max_command_bytes: usize,
    /// **单批参数字节软上限**(0=不限):累计达此立即 flush(即便未满 `max_batch`/窗口),防单条巨 pipeline。
    /// 正常合批与关闭排干都在累计达到 B 后停止收集；若单条最多 M 字节，保守上界为 B＋M。
    /// 合法命令可使本批跨过 B；每批仍受 `max_batch` 限制，不能把 B 当作硬内存限制。
    pub max_batch_bytes: usize,
}

impl Default for MicroBatchCfg {
    /// 业务作用：提供自动微批的默认合批窗口、条数和队列容量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回 1ms 窗口、1000 条批量和 4096 个队列槽的配置，单命令与单批字节限制均为零表示不启用；不创建任务或连接。
    fn default() -> Self {
        Self {
            window: Duration::from_millis(1),
            max_batch: 1000,
            queue_capacity: 4096,
            max_command_bytes: 0, // 默认不限(opt-in,避免改变既有行为)
            max_batch_bytes: 0,
        }
    }
}

/// 业务作用：按实际参数内容估算命令占用的参数字节，供自动微批接纳和批量软边界共用。
///
/// 参数说明：
/// - `cmd`：完整 Redis 命令，普通参数计实际长度，游标等其它形态按八字节计。
///
/// 返回：返回参数字节估计，不包含容器容量、RESP 编码、回执或连接开销，不能解释为进程内存占用。
fn estimate_cmd_bytes(cmd: &redis::Cmd) -> usize {
    cmd.args_iter()
        .map(|a| match a {
            redis::Arg::Simple(b) => b.len(),
            _ => 8, // Cursor 等非数据实参极小,记常量
        })
        .sum()
}

/// 业务作用：识别自动微批不应接纳的阻塞、订阅及全局管理命令，避免共享 flusher 被拖住或连接状态改变。
///
/// 参数说明：
/// - `cmd`：尚未入队的完整原始命令；此处只作共享通道准入判断，不代替 Redis 参数校验。
///
/// 返回：命中拒绝规则时给出固定原因，否则继续其它准入检查；显式 PipelineSession 的原始入口不使用此规则。
fn auto_pipeline_reject(cmd: &redis::Cmd) -> Option<&'static str> {
    let mut it = cmd.args_iter();
    let name = match it.next() {
        Some(redis::Arg::Simple(b)) if !b.is_empty() => b.to_ascii_uppercase(),
        _ => return Some("命令无名字"),
    };
    match name.as_slice() {
        // 这些状态属于物理连接；允许业务批次改写会把后续生产者置于错误数据库、身份、事务或响应模式。
        b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" | b"SELECT" | b"AUTH"
        | b"HELLO" | b"QUIT" | b"CLIENT" | b"READONLY" | b"READWRITE" | b"ASKING" | b"SYNC"
        | b"PSYNC" | b"REPLCONF" => {
            Some("连接状态命令不可进 AutoPipeline，请使用独立连接或专门 API")
        }
        // 阻塞命令:会拖住当前 batch 直到连接级 timeout 才恢复
        b"BLPOP" | b"BRPOP" | b"BLMOVE" | b"BRPOPLPUSH" | b"BLMPOP" | b"BZPOPMIN" | b"BZPOPMAX"
        | b"BZMPOP" | b"WAIT" | b"WAITAOF" => {
            Some("阻塞命令不可进 AutoPipeline(会拖住共享 flusher),请走专门 API")
        }
        // Pub/Sub / MONITOR:改变连接语义,不适合普通 pipeline transport
        b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE" | b"UNSUBSCRIBE" | b"PUNSUBSCRIBE"
        | b"SUNSUBSCRIBE" | b"MONITOR" => {
            Some("Pub/Sub/MONITOR 不可进 AutoPipeline(改变连接语义),请走 pubsub API")
        }
        // 全局/管理:cluster 下只落任意单节点,语义不等价"全节点执行"
        b"SCRIPT" | b"FUNCTION" | b"FLUSHALL" | b"FLUSHDB" | b"SHUTDOWN" | b"FAILOVER"
        | b"SWAPDB" | b"RESET" | b"ACL" | b"CONFIG" | b"MODULE" | b"CLUSTER" | b"REPLICAOF"
        | b"SLAVEOF" | b"DEBUG" | b"SAVE" | b"BGSAVE" | b"BGREWRITEAOF" | b"MIGRATE" => Some(
            "全局/管理命令不可进 AutoPipeline(cluster 下只落任意单节点,语义不等价),请走专门 API",
        ),
        // 只在选项位置识别 BLOCK；组名、消费者名和 STREAMS 后的键属于业务数据，不能当成阻塞选项。
        b"XREAD" | b"XREADGROUP" => {
            let grouped = name == b"XREADGROUP";
            if grouped
                && (!matches!(it.next(), Some(redis::Arg::Simple(s)) if s.eq_ignore_ascii_case(b"GROUP"))
                    || !matches!(it.next(), Some(redis::Arg::Simple(_)))
                    || !matches!(it.next(), Some(redis::Arg::Simple(_))))
            {
                return Some("XREADGROUP 缺少完整 GROUP 参数，不可进 AutoPipeline");
            }
            while let Some(arg) = it.next() {
                let redis::Arg::Simple(option) = arg else {
                    return Some("无法确认 Stream 读取为非阻塞命令，不可进 AutoPipeline");
                };
                if option.eq_ignore_ascii_case(b"BLOCK") {
                    return Some("XREAD/XREADGROUP BLOCK 阻塞,不可进 AutoPipeline");
                }
                if option.eq_ignore_ascii_case(b"STREAMS") {
                    return None;
                }
                if option.eq_ignore_ascii_case(b"COUNT")
                    || option.eq_ignore_ascii_case(b"MAXCOUNT")
                    || option.eq_ignore_ascii_case(b"MAXSIZE")
                {
                    if !matches!(it.next(), Some(redis::Arg::Simple(_))) {
                        return Some("Stream 读取选项缺少参数，不可进 AutoPipeline");
                    }
                } else if !(grouped && option.eq_ignore_ascii_case(b"NOACK")) {
                    // 未知选项无法可靠定位后续 BLOCK，拒绝猜测，避免将阻塞命令放入共享提交链。
                    return Some("无法确认 Stream 读取选项边界，不可进 AutoPipeline");
                }
            }
            Some("Stream 读取缺少 STREAMS 边界，不可进 AutoPipeline")
        }
        _ => None,
    }
}

type BatchJob = (
    redis::Cmd,
    oneshot::Sender<std::result::Result<redis::Value, TicketErr>>,
);

/// 业务作用：将已封批后台任务的异常退出保留为批级结果未知，供显式会话汇总提交链。
///
/// 参数说明：
/// - `e`：等待封批任务得到的取消或展开错误，不能证明命令未发出。
///
/// 返回：始终返回 ExecutionUnknown，不将任务缺失的回执解释为成功或允许自动重放。
fn join_err_to_unknown(e: tokio::task::JoinError) -> Result<()> {
    Err(NasaRedisError::ExecutionUnknown(format!(
        "pipeline auto-flush task 结束异常(panic/cancel): {e}"
    )))
}

/// 自动微批管道:后台任务把多生产者的命令合批成一条 pipeline 发出。`Arc` 共享给多调用方。
pub struct AutoPipeline {
    tx: mpsc::Sender<BatchJob>,
    activity: Arc<crate::activity::Activity>,
    cancel: CancellationToken,
    /// 单命令字节上限(0=不限),入队前检查(从 `MicroBatchCfg` 复制)。
    max_command_bytes: usize,
    /// 接纳与接收端释放共用短锁，队列退出后不能再登记或移交命令。
    admission: Arc<std::sync::Mutex<bool>>,
    abort: tokio::task::AbortHandle,
    /// 完成状态由独立 owner 留存；取消任意等待方不转移 flusher 的退出责任。
    outcome: tokio::sync::watch::Receiver<AutoPipelineState>,
}

/// 持有唯一接收端，在任务未轮询、展开或执行器关闭时同步收回队列责任。
struct AutoPipelineQueue {
    /// 已接纳命令的唯一缓冲区，释放前须阻止发送方继续移交。
    rx: mpsc::Receiver<BatchJob>,
    /// 与所有发送方共用的准入屏障。
    admission: Arc<std::sync::Mutex<bool>>,
    /// 唤醒尚在等待容量的发送方，令其按未接纳返回。
    cancel: CancellationToken,
    /// 释放队列字节时保留没有回执的未完成数量。
    activity: Arc<crate::activity::Activity>,
}

impl Drop for AutoPipelineQueue {
    /// 业务作用：接收责任结束时同步关闭准入并释放排队命令，覆盖尚未首次轮询及宿主运行时销毁。
    ///
    /// 参数说明：无。
    ///
    /// 返回：队列参数字节归零；未取得处理结束证据的责任继续保留，不将丢失回执解释为未发送或成功。
    fn drop(&mut self) {
        let mut open = self.admission.lock().expect("auto pipeline admission");
        // 与发送方登记和移交串行化，避免队列清空后仍有旧许可登记新的责任。
        self.activity.close();
        *open = false;
        self.cancel.cancel();
        self.rx.close();
        // 先释放实际排队命令，再发布字节归零；清理不依赖另一个异步任务获得轮询机会。
        while self.rx.try_recv().is_ok() {}
        self.activity.queue_dropped();
    }
}

/// 自动微批后台任务的本地运行状态，不代表所有远端命令执行成功。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoPipelineState {
    /// 合批或排干仍在进行。
    Running,
    /// 已接纳命令均产生回执，flusher 正常退出；具体执行结果由各回执决定。
    Drained,
    /// 任务异常或被强制终止，未取得回执的命令保留结果未知。
    Failed,
}

impl AutoPipeline {
    /// 业务作用：在 flusher 责任存在且入口仍开放时执行宿主的同步接流发布，与活动责任变更串行化。
    ///
    /// 参数说明：
    /// - `publish`：短小、不阻塞且不重入本微批入口的同步发布动作。
    ///
    /// 返回：满足活动任务和关闭门禁时执行并返回 Some，否则返回 None 且不执行；不据此证明远端命令成功。
    pub fn with_running<T>(&self, publish: impl FnOnce() -> T) -> Option<T> {
        self.activity
            .with_tasks(0, 0, 1, || {
                if self.cancel.is_cancelled() || self.state() != AutoPipelineState::Running {
                    None
                } else {
                    Some(publish())
                }
            })
            .flatten()
    }
    /// 业务作用：创建有界自动微批队列，并将 flusher 的退出责任交给独立等待任务。
    ///
    /// 参数说明：
    /// - `client`：固定的 Redis 来源和连接配置，后台提交期间由任务持有。
    /// - `cfg`：合批窗口、条数及参数字节策略；队列容量限制在一至 Tokio 可表示许可上限之间。
    ///
    /// 返回：返回可共享句柄，最后一个句柄释放时关闭准入并触发排干；需要当前 Tokio 执行上下文，缺少时会 panic。运行时关闭可使任务失败，调用方须通过 state 或 shutdown_result 读取退出证据。
    pub fn start(client: Arc<RedisClient>, cfg: MicroBatchCfg) -> Arc<Self> {
        let queue_capacity = cfg
            .queue_capacity
            .clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let (tx, rx) = mpsc::channel(queue_capacity);
        let cancel = CancellationToken::new();
        let max_command_bytes = cfg.max_command_bytes;
        let activity = Arc::new(crate::activity::Activity::default());
        let admission = Arc::new(std::sync::Mutex::new(true));
        // 在移交执行器之前绑定接收端和同步收口责任，未首次轮询的销毁也必须释放队列证据。
        let queue = AutoPipelineQueue {
            rx,
            admission: admission.clone(),
            cancel: cancel.clone(),
            activity: activity.clone(),
        };
        let active = activity.task(crate::activity::TaskKind::Flusher);
        let progress = activity.clone();
        let worker_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            let mut queue = queue;
            let _active = active;
            flush_loop(client, &mut queue.rx, cfg, worker_cancel, progress).await;
        });
        let abort = handle.abort_handle();
        let (done, outcome) = tokio::sync::watch::channel(AutoPipelineState::Running);
        let done = activity.completion(done);
        let task_cancel = cancel.clone();
        let owner_stop = cancel.clone().drop_guard();
        // owner 从创建时起独占 JoinHandle，关闭等待方被取消也不能分离 flusher。
        tokio::spawn(async move {
            let _stop = owner_stop;
            let state = if handle.await.is_ok() {
                AutoPipelineState::Drained
            } else {
                AutoPipelineState::Failed
            };
            task_cancel.cancel();
            done.send_replace(state);
        });
        Arc::new(Self {
            tx,
            activity,
            cancel,
            max_command_bytes,
            admission,
            abort,
            outcome,
        })
    }

    /// 业务作用：将单条命令交给共享微批并等待其独立回执，队列满时异步等待接纳容量。
    ///
    /// 参数说明：
    /// - `cmd`：完整原始命令，入队前检查共享通道准入及参数字节上限。
    ///
    /// 返回：成功返回转换后的单条结果；本地拒绝表示未接纳，服务器错误与执行未知分别报告。接纳后取消等待不撤销命令，解码失败也不回滚已发生的写入。
    pub async fn execute<T: FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.precheck(&cmd)?;
        let (otx, orx) = oneshot::channel();
        self.enqueue((cmd, otx)).await?;
        match orx.await {
            Ok(Ok(v)) => Ok(T::from_redis_value(v)?),
            Ok(Err(TicketErr::Server(e))) => Err(NasaRedisError::Redis(e.into())),
            Ok(Err(TicketErr::Unknown(m))) => Err(NasaRedisError::ExecutionUnknown(m)),
            Ok(Err(TicketErr::NotSent)) => Err(NasaRedisError::NotExecuted("命令未发送".into())),
            Err(_) => Err(NasaRedisError::ExecutionUnknown(
                "微批回执通道关闭(命令可能已执行)".into(),
            )),
        }
    }

    /// 业务作用：将单条命令交给共享微批后立即返回，不保留该命令的执行回执。
    ///
    /// 参数说明：
    /// - `cmd`：完整原始命令，须通过准入和参数字节检查；队列满时等待容量。
    ///
    /// 返回：成功只表示入队，服务器错误和结果未知不再返回给本调用；失败表示未接纳。后续 barrier 只提供提交顺序，不能替代已放弃的逐命令成功证据。
    pub async fn submit(&self, cmd: redis::Cmd) -> Result<()> {
        self.precheck(&cmd)?;
        let (otx, _orx) = oneshot::channel(); // 丢弃接收端 = 不等结果
        self.enqueue((cmd, otx)).await?;
        Ok(())
    }

    /// 业务作用：在队列容量可用后复验关闭门禁，再同时登记未完成责任并移交命令。
    ///
    /// 参数说明：
    /// - `job`：尚未接纳的 Redis 命令和独立回执发送端。
    ///
    /// 返回：成功表示命令已交给队列；关闭或通道失效返回 NotExecuted。容量等待被取消时不接纳命令，移交后由 flusher 负责回执。
    async fn enqueue(&self, job: BatchJob) -> Result<()> {
        let permit = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(NasaRedisError::NotExecuted("自动微批已关闭".into())),
            permit = self.tx.reserve() => permit.map_err(|_| NasaRedisError::NotExecuted("自动微批队列已关闭".into()))?,
        };
        // 容量等待期间可能关闭；同一短锁内复验并发送，避免检查与入队之间重新开放责任。
        let open = self.admission.lock().expect("auto pipeline admission");
        if !*open || self.cancel.is_cancelled() {
            return Err(NasaRedisError::NotExecuted("自动微批已关闭".into()));
        }
        self.activity.accept(1, estimate_cmd_bytes(&job.0));
        permit.send(job);
        Ok(())
    }

    /// 业务作用：等待此前已入队命令经过 flusher 处理，建立同一管道的提交顺序屏障。
    ///
    /// 参数说明：无。
    ///
    /// 返回：屏障 PING 的结果；不汇总此前命令的服务器错误或未知结果，写入成功仍以各自回执为准。
    pub async fn barrier(&self) -> Result<()> {
        self.execute::<redis::Value>(redis::cmd("PING"))
            .await
            .map(|_| ())
    }

    /// 业务作用：在等待队列容量前拒绝已关闭入口、共享连接不兼容命令及超出单命令字节额度的请求。
    ///
    /// 参数说明：
    /// - `cmd`：尚未入队的完整命令，准入规则不代替服务端参数验证。
    ///
    /// 返回：通过时不产生提交副作用；关闭返回 NotExecuted，命令类别或字节额度不符返回 Config，真正入队前仍须复验关闭状态。
    fn precheck(&self, cmd: &redis::Cmd) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(NasaRedisError::NotExecuted("自动微批已关闭".into()));
        }
        if let Some(reason) = auto_pipeline_reject(cmd) {
            return Err(NasaRedisError::Config(reason.into()));
        }
        if self.max_command_bytes > 0 {
            let bytes = estimate_cmd_bytes(cmd);
            if bytes > self.max_command_bytes {
                return Err(NasaRedisError::Config(format!(
                    "命令参数 {bytes} 字节超 max_command_bytes={}(AutoPipeline 拒绝大 value 入队)",
                    self.max_command_bytes
                )));
            }
        }
        Ok(())
    }

    /// 业务作用：永久停止接纳新命令，通知 flusher 排干已入队责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：准入已关闭；不表示排干完成。
    pub fn begin_shutdown(&self) {
        let mut open = self.admission.lock().expect("auto pipeline admission");
        self.activity.close();
        *open = false;
        self.cancel.cancel();
    }

    /// 业务作用：预算耗尽时终止 flusher；仍需等待 owner 确认退出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：关闭准入并请求终止，未获回执的命令不能自动重放。
    pub fn abort(&self) {
        self.begin_shutdown();
        self.abort.abort();
    }

    /// 业务作用：取得后台任务终态用于监督和退出裁决。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地任务状态；退出证据通道丢失时报告失败，不提升为远端逐命令成功。
    pub fn state(&self) -> AutoPipelineState {
        let disconnected = self.outcome.has_changed().is_err();
        let state = *self.outcome.borrow();
        if disconnected && state == AutoPipelineState::Running {
            AutoPipelineState::Failed
        } else {
            state
        }
    }

    /// 业务作用：观察当前已占用的队列容量，包括正在提交的许可。
    ///
    /// 参数说明：无。
    ///
    /// 返回：队列槽占用数，不含 flusher 已取出的批次。
    pub fn queued(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    /// 业务作用：读取 flusher、参数字节、未完成责任及最近批次进展。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地工作证据，完成数量不等于远端写入成功数量。
    pub fn observation(&self) -> crate::RedisTaskObservation {
        self.activity.snapshot()
    }

    /// 业务作用：关闭准入并等待 flusher 的确定终态，支持并发、重复和取消后再次等待。
    ///
    /// 参数说明：无。
    ///
    /// 返回：正常退出时成功；异常或强制退出时报告结果未知，逐命令仍以回执为准。
    pub async fn shutdown_result(&self) -> Result<()> {
        self.begin_shutdown();
        let mut outcome = self.outcome.clone();
        loop {
            match *outcome.borrow_and_update() {
                AutoPipelineState::Drained => return Ok(()),
                AutoPipelineState::Failed => {
                    return Err(NasaRedisError::ExecutionUnknown(
                        "自动微批任务异常退出".into(),
                    ))
                }
                AutoPipelineState::Running => {}
            }
            if outcome.changed().await.is_err() {
                return Err(NasaRedisError::ExecutionUnknown(
                    "自动微批退出证据不可用".into(),
                ));
            }
        }
    }

    /// 业务作用：兼容独立调用入口，等待关闭并记录异常终态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：等待结束；需要程序化裁决的调用方使用 shutdown_result。
    pub async fn shutdown(&self) {
        if self.shutdown_result().await.is_err() {
            tracing::warn!("自动微批关闭时存在未确认命令");
        }
    }
}

impl Drop for AutoPipeline {
    /// 业务作用：最后一个业务句柄释放时收回准入，独立 owner 继续承担排干责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：触发关闭；不伪报已退出。
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// 业务作用：按窗口、条数和参数字节软边界合批派发，关闭后排干已接纳的队列责任。
///
/// 参数说明：
/// - `client`：固定 Redis 来源，直到后台提交结束仍由本任务持有。
/// - `rx`：借用外层责任守卫持有的唯一接收端；本函数退出或被取消后仍由守卫同步关闭并释放剩余队列。
/// - `cfg`：合批策略，窗口最多按一年计，零条数实际仍逐条处理。
/// - `cancel`：停止继续接纳和等待合批窗口的关闭通知，不撤销已派发批次。
/// - `activity`：记录队列取出、未完成责任及批次结束的本地证据。
///
/// 返回：正常返回表示已取出责任均完成回执分发，不表示每条命令成功；任务被取消或异常结束时由外层独立任务发布失败状态。
async fn flush_loop(
    client: Arc<RedisClient>,
    rx: &mut mpsc::Receiver<BatchJob>,
    cfg: MicroBatchCfg,
    cancel: CancellationToken,
    activity: Arc<crate::activity::Activity>,
) {
    loop {
        // 等首命令(或停机)
        let first = tokio::select! {
            _ = cancel.cancelled() => break,
            j = rx.recv() => match j {
                Some(j) => j,
                None => break
            },
        };
        let mut batch_bytes = estimate_cmd_bytes(&first.0);
        activity.dequeue(batch_bytes);
        let mut batch = vec![first];
        // 时间窗内收集更多,到 max_batch / max_batch_bytes 立即停。窗口收集也监听
        // cancel——否则停机时若正在收集(尤其 window 调大做大批合并),drain 会被推迟整个 window;加 cancel
        // 分支即时收口。`max_batch_bytes>0` 时累计字节达上限即停(防单条巨 pipeline)。
        let bounded_window = cfg.window.min(Duration::from_secs(365 * 24 * 60 * 60));
        let deadline = tokio::time::Instant::now() + bounded_window;
        while batch.len() < cfg.max_batch
            && (cfg.max_batch_bytes == 0 || batch_bytes < cfg.max_batch_bytes)
        {
            tokio::select! {
                _ = cancel.cancelled() => break,
                r = tokio::time::timeout_at(deadline, rx.recv()) => match r {
                    Ok(Some(j)) => {
                        let bytes = estimate_cmd_bytes(&j.0);
                        activity.dequeue(bytes);
                        batch_bytes += bytes;
                        batch.push(j);
                    }
                    _ => break, // 窗口到 / 通道关闭
                }
            }
        }
        flush_batch(&client, batch, &activity).await;
    }
    // 关闭接收端后只排干已接纳集合；正常合批与排干保持相同的条数和参数字节软上限。
    rx.close();
    while let Some(first) = rx.recv().await {
        let mut bytes = estimate_cmd_bytes(&first.0);
        activity.dequeue(bytes);
        let mut batch = vec![first];
        while batch.len() < cfg.max_batch.max(1)
            && (cfg.max_batch_bytes == 0 || bytes < cfg.max_batch_bytes)
        {
            let Some(job) = rx.recv().await else {
                break;
            };
            let size = estimate_cmd_bytes(&job.0);
            activity.dequeue(size);
            bytes = bytes.saturating_add(size);
            batch.push(job);
        }
        flush_batch(&client, batch, &activity).await;
    }
}

/// 业务作用：派发一个自动微批，并在所有物理批次结束后记录本地完成进展。
///
/// 参数说明：
/// - `client`：本批固定使用的 Redis 来源。
/// - `batch`：已从队列取出的命令及各自回执。
/// - `activity`：统计本批完成数及批级传输失败的本地观察对象。
///
/// 返回：正常结束后各命令已分发响应，完成计数包含服务器报错命令；批级失败计入本地失败进展，逐命令结果仍以回执为准。
async fn flush_batch(
    client: &Arc<RedisClient>,
    batch: Vec<BatchJob>,
    activity: &crate::activity::Activity,
) {
    let count = batch.len();
    let failed = dispatch_jobs(client, batch).await.is_err();
    activity.finish(count, failed);
}

/// 业务作用：按客户端拓扑派发一批命令，并由每个物理批次独立回填票据，供显式会话和自动微批共用。
///
/// 参数说明：
/// - `client`：固定 Redis 来源及专用 Pipeline 连接入口。
/// - `jobs`：按本地接纳顺序排列的命令与各自回执通道。
///
/// 返回：所有物理批次正常分发响应时成功，命令级服务器错误仍由各票据报告；任一批次传输或响应数异常时返回错误。
/// Standalone 使用单个物理批次；Cluster 按首键 slot 分桶并发发送，无键命令单独派发，同槽保序但不保证跨槽原子性。
/// Cluster 的无参 PING 是本地派发屏障：先等齐前面的全部槽桶，再发送 PING，之后才处理后续命令。
async fn dispatch_jobs(client: &RedisClient, jobs: Vec<BatchJob>) -> Result<()> {
    if jobs.is_empty() {
        return Ok(());
    }
    if !client.is_cluster() {
        return send_one_pipe(client, jobs).await;
    }
    let mut pending = jobs.into_iter();
    let mut first_err = None;
    loop {
        let mut buckets: std::collections::HashMap<u16, Vec<BatchJob>> =
            std::collections::HashMap::new();
        // 无键命令不能混入真实 slot 0，避免被错误绑定到该槽节点。
        let mut keyless = Vec::new();
        let mut barrier = None;
        for (cmd, tx) in pending.by_ref() {
            let is_barrier = {
                let mut args = cmd.args_iter();
                matches!(args.next(), Some(redis::Arg::Simple(name)) if name.eq_ignore_ascii_case(b"PING"))
                    && args.next().is_none()
            };
            if is_barrier {
                // barrier 使用无参 PING；不能把它与前序槽桶并发发送，否则成功回包时其它槽仍可能尚未执行。
                barrier = Some((cmd, tx));
                break;
            }
            match first_key_slot(&cmd) {
                Some(slot) => buckets.entry(slot).or_default().push((cmd, tx)),
                None => keyless.push((cmd, tx)),
            }
        }
        let mut futs: Vec<_> = buckets
            .into_values()
            .map(|batch| send_one_pipe(client, batch))
            .collect();
        if !keyless.is_empty() {
            futs.push(send_one_pipe(client, keyless));
        }
        for result in futures::future::join_all(futs).await {
            if let Err(error) = result {
                first_err.get_or_insert(error);
            }
        }
        let Some(barrier) = barrier else {
            break;
        };
        // 所有前序回执均已分发后才发送屏障；保留前序批级错误，但不把它冒充 PING 自身的响应。
        if let Err(error) = send_one_pipe(client, vec![barrier]).await {
            first_err.get_or_insert(error);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// 业务作用：为本地 Cluster 分桶估算首键槽位，显式识别脚本命令的 numkeys 布局。
///
/// 参数说明：
/// - `cmd`：完整命令；EVAL、EVALSHA、FCALL 及只读变体从 numkeys 后取首键，其它命令按首个参数估算。
///
/// 返回：取得参数时返回哈希槽，缺少参数或无法解析脚本键数量时为 None；这只是本地分桶依据，不是任意命令的完整键发现或多键同槽校验。无键管理命令的参数也可能被当作估算键。
fn first_key_slot(cmd: &redis::Cmd) -> Option<u16> {
    let mut it = cmd.args_iter();
    let name = match it.next()? {
        redis::Arg::Simple(b) => b,
        _ => return None,
    };
    if is_eval_family(name) {
        // arg[1]=script/sha(跳过),arg[2]=numkeys,arg[3..]=keys
        let _script = it.next()?;
        let numkeys: usize = match it.next()? {
            redis::Arg::Simple(b) => std::str::from_utf8(b).ok()?.parse().ok()?,
            _ => return None,
        };
        if numkeys == 0 {
            return None; // 无键脚本 → keyless 桶
        }
        return match it.next()? {
            redis::Arg::Simple(key) => Some(crate::keytag::redis_slot(key)),
            _ => None,
        };
    }
    // 其余命令:key 在 arg[1]
    match it.next()? {
        redis::Arg::Simple(key) => Some(crate::keytag::redis_slot(key)),
        _ => None,
    }
}

/// 业务作用：识别首键位于脚本体或函数名及 numkeys 之后的命令族，供本地分桶选择参数位置。
///
/// 参数说明：
/// - `name`：不区分 ASCII 大小写的原始命令名。
///
/// 返回：EVAL、EVALSHA、FCALL 及其单个 _RO 后缀形式返回 true，其它名称返回 false；不校验服务端是否支持该命令。
fn is_eval_family(name: &[u8]) -> bool {
    let mut up = name.to_ascii_uppercase();
    // 去掉可选的 `_RO` 只读后缀,统一比对基名
    if up.ends_with(b"_RO") {
        up.truncate(up.len() - 3);
    }
    matches!(up.as_slice(), b"EVAL" | b"EVALSHA" | b"FCALL")
}

/// 业务作用：通过专用 Pipeline lane 发送一个物理批次，并按响应位置分发各命令的回执。
///
/// 参数说明：
/// - `client`：负责获取 Pipeline 连接的 Redis 客户端。
/// - `jobs`：按批内顺序排列的命令和回执发送端。
///
/// 返回：空批或完整响应分发完成时成功，单命令服务器错误仅发给对应票据；连接、传输或响应数量异常时整批返回 ExecutionUnknown 并通知票据，不自动重放。
async fn send_one_pipe(client: &RedisClient, jobs: Vec<BatchJob>) -> Result<()> {
    let n = jobs.len();
    if n == 0 {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    let mut txs = Vec::with_capacity(n);
    for (cmd, tx) in jobs {
        pipe.add_command(cmd);
        txs.push(tx);
    }
    // 专用 pipeline lane(与 direct/control/lock 隔离,防头阻塞;惰性建连)。
    let mut conn = match client.pipe_conn().await {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("pipeline 专用连接获取失败,整批结果不确定: {e}");
            for tx in txs {
                let _ = tx.send(Err(TicketErr::Unknown(msg.clone())));
            }
            return Err(NasaRedisError::ExecutionUnknown(msg));
        }
    };
    match redis::aio::ConnectionLike::req_packed_commands(&mut conn, &pipe, 0, n).await {
        Ok(values) if values.len() == n => {
            let mut it = values.into_iter();
            for tx in txs {
                let _ = match it.next() {
                    Some(redis::Value::ServerError(e)) => tx.send(Err(TicketErr::Server(e))),
                    Some(v) => tx.send(Ok(v)),
                    None => unreachable!("已校验 values.len()==n"),
                };
            }
            Ok(())
        }
        Ok(values) => {
            let msg = format!(
                "pipeline 响应数 {} != 命令数 {n}(协议异常,整批结果不确定)",
                values.len()
            );
            for tx in txs {
                let _ = tx.send(Err(TicketErr::Unknown(msg.clone())));
            }
            Err(NasaRedisError::ExecutionUnknown(msg))
        }
        Err(e) => {
            let msg = format!("pipeline 传输失败,整批结果不确定: {e}");
            for tx in txs {
                let _ = tx.send(Err(TicketErr::Unknown(msg.clone())));
            }
            Err(NasaRedisError::ExecutionUnknown(msg))
        }
    }
}
