// ============================================================================
// src/pipeline.rs —— 显式 PipelineSession(文档,typed ticket 正式 API)。
//
// 模型:
//   · session = **本地 buffer**,无共享生产者——到 max_commands 条数 / max_bytes 单批字节阈值即
//     **滚动 auto-flush**:seal 当前批后台串行发出、会话续接,**永不报错**(对齐 原实现 pipelineAutoFlush,
//     第 1001 条无缝接续;各段经 flush_chain 链式串行保证跨段保序);
//   · move 语义:`execute(self)` 收尾会话(等齐已 flush 各段 + 发最后一段)——原实现 的 execTag 配对/
//     openNested 在 Rust 是编译期事实;Drop 未 execute = 最后一段未发(NotSent)+ warn,已 auto-flush 段已提交;
//   · typed ticket:入队返回 Ticket<T>,execute 后 `ticket.await_result()` 取各自类型化结果;
//   · **保序 + 一批一写 + 逐命令隔离**(此前逐命令
//     tokio::spawn,task 调度顺序 ≠ 入队顺序,实测 2000 轮出现 6 次乱序——撤回):
//     现经 `MultiplexedConnection::send_packed_commands(&Pipeline, 0, n)` 单条消息进
//     driver、单次 flush、严格按入队顺序;返回裸 `Vec<Value>`,server error 以
//     `Value::ServerError` **逐槽位内联**；聚合成整批 Err 只发生在
//     `Pipeline::query_async` 包装层,绕开包装层即得逐命令隔离)。
//   · 失败语义:传输层错误(写出后断线等)= 整 session 未确认结果统一
//     ExecutionUnknown;`Value::ServerError` = 单命令确定性失败,不污染同批其它命令。
//   · 顺序:单 session 内严格有序(同一连接单批);direct 与 session 混用不保证顺序。
// ============================================================================

use std::sync::Arc;
use std::time::Duration;

use redis::FromRedisValue;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::config::PipelineCfg;
use crate::error::{NasaRedisError, Result};

/// `h_decr_by_and_del` 的 Lua —— **逐字复刻 原实现 `/lua/hash_hdecriby_del.lua`**:HINCRBY 负 delta,自减后
/// 余值 `<= 0` 则 HDEL 该 field,返回自减后余值。KEYS=[key] ARGV=[field, delta](Lua 用单引号,合法)。
const H_DECR_BY_AND_DEL_LUA: &str = "local k = KEYS[1];\n\
local hk = ARGV[1];\n\
local delta = tonumber(ARGV[2]) or 1;\n\
local v = redis.call('hincrby', k, hk, -delta);\n\
if v <= 0 then\n\
    redis.call('hdel', k, hk);\n\
end\n\
return v;";

/// 单条命令的失败形态(经 oneshot 分发;RedisError 不 Clone,传输错误需广播给
/// 整批 ticket,故自定义枚举承载)。
#[derive(Debug, Clone)]
enum TicketErr {
    /// 服务器已答复的确定性失败(WRONGTYPE 等)——仅该命令失败,同批其余不受污染。
    Server(redis::ServerError),
    /// 传输层失败(写出后断线/超时):该命令「可能已执行」。
    Unknown(String),
    /// **确定未发送**:PipelineSession 未 execute 即 drop —— 命令从未进 dispatch。
    NotSent,
}

/// 单条命令的类型化回执。execute 后调用 `await_result()` 取值。
pub struct Ticket<T> {
    rx: oneshot::Receiver<std::result::Result<redis::Value, TicketErr>>,
    _t: std::marker::PhantomData<T>,
}

impl<T: FromRedisValue> Ticket<T> {
    /// **预置已完成 ticket**(空集合短路)。空输入(`h_del([])`/`mget([])`…)
    /// 不构造非法命令、不发 Redis——直接给定结果,对照 原实现 Actuator 空集合 early-return。`await_result`
    /// 立即返回该值;不占 session 配额、不入 dispatch。
    ///
    /// # 参数
    /// - `v`: 待转换的值。
    fn ready(v: redis::Value) -> Ticket<T> {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(v));
        Ticket {
            rx,
            _t: std::marker::PhantomData,
        }
    }

    /// 取本命令的结果(必须在 `execute()` 之后调用,否则 Err)。
    pub fn await_result(mut self) -> Result<T> {
        match self.rx.try_recv() {
            // 命令成功:Value 按值移交 FromRedisValue(redis 1.2 形态),类型化失败 = Parsing
            Ok(Ok(v)) => Ok(T::from_redis_value(v)?),
            // 确定性 server error / 传输 Unknown 已在 execute 阶段分类
            Ok(Err(TicketErr::Server(e))) => Err(NasaRedisError::Redis(e.into())),
            Ok(Err(TicketErr::Unknown(msg))) => Err(NasaRedisError::ExecutionUnknown(msg)),
            // 会话未 execute 即 drop —— **确定未发送**(区别于下面"可能已发"的取消场景)。
            Ok(Err(TicketErr::NotSent)) => Err(NasaRedisError::NotExecuted(
                "PipelineSession 未 execute 即丢弃,命令确定未发送".into(),
            )),
            // sender 被 drop(execute future 被取消 / 内部未送达):命令**可能已写到服务端执行**
            // (写命令!)——按 `ExecutionUnknown` 处置(兑现文档 取消语义),
            // 不再误报"内部不变量被破坏"(把 async 取消的真实 bug 误导成内部错误)。
            Err(oneshot::error::TryRecvError::Closed) => Err(NasaRedisError::ExecutionUnknown(
                "ticket 通道关闭(execute 被取消或未送达,命令可能已发到服务端执行)".into(),
            )),
            Err(oneshot::error::TryRecvError::Empty) => Err(NasaRedisError::SessionLimit(
                "execute() 之前不可取结果".into(),
            )),
        }
    }
}

/// 显式批会话。`client.pipeline()` 创建,入队若干命令后 `execute(self)` 收尾。
///
/// **滚动自动 flush(对齐 原实现 `LettucePipeline.pipelineAutoFlush`)**:**第 N 条入队
/// 后**当前批达 `session_max_commands`(默认 1000)条或 `session_max_bytes`(默认 4MB)字节即 seal 后台发出、
/// 会话继续接收——**永不返 `SessionLimit`**。**正好 1000 条也立即提交**(入队后判断,对齐 原实现 line 1540;
/// 第 1001 条进新批),不必等 `execute()`。各段经 `flush_chain` **链式串行**(段 N 落库后才发段 N+1),保证跨段
/// 严格按 seal 顺序(同 slot 跨段保序;cluster 跨 slot 同单批限制)。每段命令各自的 `Ticket` 在该段后台 dispatch
/// 完成时回填——auto-flush 段的 ticket 可能在 `execute()` 前就绪。`execute()` 等齐各段 + 发最后一段未满批,并
/// **聚合各段批级传输错误上抛**。**已 auto-flush 段=已提交**(同 原实现),会话未 `execute` 即 drop 也会后台
/// 完成(仅最后一段未满批按 `NotExecuted`)。
///
/// ⚠ **时间可见性**(对照 原实现 的契约差异):原实现 `pipelineAutoFlush` 在入队线程内同步 `pipelineForce()`;Rust
/// 为保持 helper 同步,seal 后**后台 spawn 发出**——第 1000 个 helper 返回时,命令可能尚未写到 Redis。保证的是
/// "达阈值即 seal 并调度提交 + `execute()` 等齐所有已 seal 段",**不**保证"helper 返回即 Redis 可见"。需 原实现
/// 级同步可见性只能改 helper 为 async(不为此做大改)。
///
/// 需要多生产者并发微批仍用 [`AutoPipeline`];`PipelineSession` 是单调用方顺序攒批 + 滚动 flush。
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
    executed: bool,
}

impl RedisClient {
    /// 创建显式批会话(self: &Arc 形态由调用方 clone 传入)。
    ///
    pub fn pipeline(self: &Arc<Self>) -> PipelineSession {
        PipelineSession {
            client: Arc::clone(self),
            cfg: self.config().pipeline.clone(),
            cmds: Vec::new(),
            bytes: 0,
            flush_chain: None,
            executed: false,
        }
    }
}

impl PipelineSession {
    /// 入队任意命令(类型化 helper 的底座;也是 raw 逃生舱)。
    ///
    /// ⚠ cluster 分桶:本会话在 cluster 下按命令 key 的 slot 分桶并行发。typed helper 与
    /// EVAL/EVALSHA/FCALL 系(key 在 numkeys 之后)均已正确取 key;**其它 key 不在 `arg[1]`、又非 EVAL 系的
    /// 罕见命令**经 raw `enqueue` 入队时会按 `arg[1]` 算 slot,cluster 下可能误桶(返回 MOVED/CROSSSLOT,非静默
    /// 错写)。这类命令请确保同 slot 或走 typed API。standalone 不分桶,不受影响。
    ///
    /// # 参数
    /// - `cmd`: 底层 Redis 命令对象。
    pub fn enqueue<T: FromRedisValue>(&mut self, cmd: redis::Cmd) -> Result<Ticket<T>> {
        // 估算本命令参数字节(协议开销忽略;Arg 是 non_exhaustive,未知形态按 8 字节计)
        let sz: usize = cmd
            .args_iter()
            .map(|a| match a {
                redis::Arg::Simple(b) => b.len(),
                _ => 8,
            })
            .sum();
        // rx 先建,push 后即使 seal_and_flush 把 batch `mem::take` 走,本 ticket 仍正常返回(tx 在 batch 内,
        // 由后台 dispatch 回填)。
        let (tx, rx) = oneshot::channel();
        self.bytes += sz;
        self.cmds.push((cmd, tx));
        // 滚动自动 flush(对齐 原实现 pipelineAutoFlush;**不报错**):**push 后**判断——第 1000 条入队后
        // `len==session_max_commands` 立即 seal 后台发出(exact-1000 即提交,对齐 原实现 line 1540;复审
        // off-by-one 修正:此前 push 前判断,要等第 1001 条才 seal 前 1000)。字节阈值同样 push 后判断
        // (`bytes >= max_bytes`),单条命令本身超限时它自己一段发出,不死循环。
        if self.cmds.len() >= self.cfg.session_max_commands
            || self.bytes >= self.cfg.session_max_bytes
        {
            self.seal_and_flush();
        }
        Ok(Ticket {
            rx,
            _t: std::marker::PhantomData,
        })
    }

    /// seal 当前批 → 后台 dispatch(滚动 auto-flush 的核心)。把当前 `cmds` 取走丢进一个 spawn 任务,**先
    /// await 前一段的 flush 句柄**再发本段,从而跨段严格按 seal 顺序落库(避免 的乱序——这里是段级链式
    /// 串行,不是逐命令 spawn)。本段各 ticket 在该任务内由 `dispatch_jobs` 回填;传输错误广播给本段 ticket。
    /// 空批直接返回。
    fn seal_and_flush(&mut self) {
        // helper 是同步的——若调用方在**无 tokio runtime** 的上下文攒批(罕见),无法后台 flush:不 seal,
        // 保留缓冲并由 `execute().await`（必在 runtime 内）统一发出，既不 panic 也不提前报错。
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
            // 先等前段落库(保序);保留**第一个**批级传输错误(prev 在前)。前段 task panic/cancel →
            // JoinError 映射成 ExecutionUnknown(批级不确定,**不吞成 Ok**)。
            let prev_res = match prev {
                Some(p) => p.await.unwrap_or_else(join_err_to_unknown),
                None => Ok(()),
            };
            let own = dispatch_jobs(&client, batch).await; // 段内 ticket 在此回填(含错误广播)
            prev_res.and(own)
        }));
    }

    // ── 常用类型化 helper(按需渐进扩充, 第 1 条同款纪律)──

    /// Queues the Redis GET command and returns its ticket.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn get<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("GET").arg(key).to_owned())
    }

    /// Queues the Redis SET command and returns its ticket.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn set<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("SET").arg(key).arg(val).to_owned())
    }

    /// Queues the Redis HSET command and returns its ticket.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn h_set<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("HSET").arg(key).arg(field).arg(val).to_owned())
    }

    /// Queues the Redis HGET command and returns its ticket.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    pub fn h_get<T: FromRedisValue>(
        &mut self,
        key: &str,
        field: &str,
    ) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("HGET").arg(key).arg(field).to_owned())
    }

    /// Queues the Redis DEL command and returns its ticket.
    ///
    /// # 参数
    ///
    /// - `key`: 待删除的 Redis key。
    pub fn del(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("DEL").arg(key).to_owned())
    }

    /// Queues the Redis INCRBY command and returns its ticket.
    ///
    /// # 参数
    ///
    /// - `key`: 保存整数计数值的 Redis key。
    /// - `delta`: 本次递增的整数步长，可为负值。
    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("INCRBY").arg(key).arg(delta).to_owned())
    }

    /// Queues the Redis ZADD command and returns its ticket.
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `score`: 有序集合成员 score。
    /// - `member`: 集合或有序集合成员。
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

    /// Queues the Redis XADD command and returns its ticket.
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `fields`: 要写入 entry 的 field/bytes 列表；会使用 `"*"` 自动生成 entry id。
    pub fn x_add(&mut self, stream: &str, fields: &[(&str, &[u8])]) -> Result<Ticket<String>> {
        let mut c = redis::cmd("XADD");
        c.arg(stream).arg("*");
        for (f, v) in fields {
            c.arg(*f).arg(*v);
        }
        self.enqueue(c)
    }

    // ── Phase-1 常用命令 typed helper(API 完整性审;命名对齐 commands.rs,
    //    底座仍是 enqueue;cluster 多 key 命令的跨 slot 由 Redis loud CROSSSLOT 兜底)──

    // key / string
    /// 相对过期 → `PEXPIRE`(毫秒)。对照 原实现 `LettucePipeline.expire(key, millis)`(= `OP_EXPIRE`→pexpire)
    /// **与 direct `commands.rs::expire(Duration)` 同源**(此前误用 `EXPIRE` 秒,
    /// 比 原实现 放大 1000×)。入参取 `Duration` 消除单位歧义;非零下界 1ms(亚毫秒不塌成 `PEXPIRE 0` 删 key)。
    ///
    /// # 参数
    ///
    /// - `key`: 需要设置相对过期时间的 Redis key。
    /// - `ttl`: 从当前时间开始计算的存活时间；非零亚毫秒会按 1ms 下界处理。
    pub fn expire(&mut self, key: &str, ttl: Duration) -> Result<Ticket<bool>> {
        let ms = crate::commands::duration_to_millis_floor1(ttl)?;
        self.enqueue(redis::cmd("PEXPIRE").arg(key).arg(ms).to_owned())
    }

    /// 绝对过期(epoch **秒**)→ `EXPIREAT`。对照 direct `commands.rs::expire_at_secs`。
    ///
    /// # 参数
    ///
    /// - `key`: 需要设置绝对过期时间的 Redis key。
    /// - `unix_secs`: 秒级 Unix 时间戳，到达该时间后 key 过期。
    pub fn expire_at_secs(&mut self, key: &str, unix_secs: i64) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("EXPIREAT").arg(key).arg(unix_secs).to_owned())
    }

    /// 绝对过期(epoch **毫秒**)→ `PEXPIREAT`。对照 原实现 `LettucePipeline.expireAt(key, millis)`
    /// (= `OP_EXPIRE_AT`→pexpireat)与 direct `commands.rs::expire_at_millis`。
    ///
    /// # 参数
    ///
    /// - `key`: 需要设置绝对过期时间的 Redis key。
    /// - `unix_ms`: 毫秒级 Unix 时间戳，到达该时间后 key 过期。
    pub fn expire_at_millis(&mut self, key: &str, unix_ms: i64) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("PEXPIREAT").arg(key).arg(unix_ms).to_owned())
    }

    /// PERSIST(去掉 TTL)。
    ///
    /// # 参数
    ///
    /// - `key`: 需要移除 TTL、转为永久保存的 Redis key。
    pub fn persist(&mut self, key: &str) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("PERSIST").arg(key).to_owned())
    }

    /// EXISTS(单 key)。
    ///
    /// # 参数
    ///
    /// - `key`: 待检查是否存在的 Redis key。
    pub fn exists(&mut self, key: &str) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("EXISTS").arg(key).to_owned())
    }

    /// PTTL(ms;-1 无 TTL,-2 不存在)。
    ///
    /// # 参数
    ///
    /// - `key`: 待查询剩余毫秒 TTL 的 Redis key。
    pub fn pttl(&mut self, key: &str) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("PTTL").arg(key).to_owned())
    }

    /// SETNX(不存在才设)→ 是否设置成功。对照 原实现 `setIfAbsent`。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn set_nx<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("SETNX").arg(key).arg(val).to_owned())
    }

    /// SET key val PX ttl_ms(带过期写)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    /// - `ttl_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
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

    /// APPEND → 追加后长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn append<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("APPEND").arg(key).arg(val).to_owned())
    }

    /// GETSET → 旧值。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn get_set<T: FromRedisValue, V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("GETSET").arg(key).arg(val).to_owned())
    }

    /// GETDEL → 取并删。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn get_del<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("GETDEL").arg(key).to_owned())
    }

    /// STRLEN。
    ///
    /// # 参数
    ///
    /// - `key`: 字符串值所在的 Redis key。
    pub fn str_len(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("STRLEN").arg(key).to_owned())
    }

    /// MGET(⚠ cluster 跨 slot → Redis CROSSSLOT)。空 keys 短路返回空数组。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
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
    /// HDEL(多 field)→ 删除数。空 field 短路返回 0(:不发非法空 HDEL)。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `fields`: 待删除的 hash field 列表；空切片会返回预完成 ticket。
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

    /// HSETNX → 是否新建。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn h_set_nx<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        field: &str,
        val: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("HSETNX").arg(key).arg(field).arg(val).to_owned())
    }

    /// HINCRBY → 增后值。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递增的 hash field。
    /// - `delta`: 本次递增的整数步长，可为负值。
    pub fn h_incr_by(&mut self, key: &str, field: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(
            redis::cmd("HINCRBY")
                .arg(key)
                .arg(field)
                .arg(delta)
                .to_owned(),
        )
    }

    /// HGETALL。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn h_get_all<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<T>> {
        self.enqueue(redis::cmd("HGETALL").arg(key).to_owned())
    }

    /// HMGET(多 field)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `fields`: Hash 字段名列表,用于批量读取或删除。
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

    /// HKEYS。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    pub fn h_keys(&mut self, key: &str) -> Result<Ticket<Vec<String>>> {
        self.enqueue(redis::cmd("HKEYS").arg(key).to_owned())
    }

    /// HLEN。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    pub fn h_len(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("HLEN").arg(key).to_owned())
    }

    // list
    /// LPUSH → push 后长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn l_push<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LPUSH").arg(key).arg(val).to_owned())
    }

    /// RPUSH → push 后长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn r_push<V: redis::ToSingleRedisArg>(&mut self, key: &str, val: V) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("RPUSH").arg(key).arg(val).to_owned())
    }

    /// LPOP。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn l_pop<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("LPOP").arg(key).to_owned())
    }

    /// RPOP。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn r_pop<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("RPOP").arg(key).to_owned())
    }

    /// LRANGE。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
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

    /// LREM(count 0=全删,>0 头向尾,<0 尾向头)→ 删除数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn l_rem<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        count: isize,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LREM").arg(key).arg(count).arg(val).to_owned())
    }

    /// LLEN。
    ///
    /// # 参数
    ///
    /// - `key`: 列表所在的 Redis key。
    pub fn l_len(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LLEN").arg(key).to_owned())
    }

    /// LTRIM。
    ///
    /// # 参数
    ///
    /// - `key`: 列表所在的 Redis key。
    /// - `start`: 保留区间起始下标，支持 Redis 负索引。
    /// - `stop`: 保留区间结束下标，支持 Redis 负索引。
    pub fn l_trim(&mut self, key: &str, start: isize, stop: isize) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("LTRIM").arg(key).arg(start).arg(stop).to_owned())
    }

    // set
    /// SADD(单 member)→ 新增数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn s_add<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("SADD").arg(key).arg(member).to_owned())
    }

    /// SREM(单 member)→ 删除数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn s_rem<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("SREM").arg(key).arg(member).to_owned())
    }

    /// SCARD。
    ///
    /// # 参数
    ///
    /// - `key`: 集合所在的 Redis key。
    pub fn s_card(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("SCARD").arg(key).to_owned())
    }

    /// SMEMBERS。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn s_members<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("SMEMBERS").arg(key).to_owned())
    }

    /// SISMEMBER。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn s_is_member<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("SISMEMBER").arg(key).arg(member).to_owned())
    }

    /// SPOP(单个)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn s_pop<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("SPOP").arg(key).to_owned())
    }

    // zset
    /// ZREM(单 member)→ 删除数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn z_rem<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("ZREM").arg(key).arg(member).to_owned())
    }

    /// ZCARD。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    pub fn z_card(&mut self, key: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("ZCARD").arg(key).to_owned())
    }

    /// ZSCORE。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn z_score<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<Option<f64>>> {
        self.enqueue(redis::cmd("ZSCORE").arg(key).arg(member).to_owned())
    }

    /// ZRANK。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn z_rank<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<Option<u64>>> {
        self.enqueue(redis::cmd("ZRANK").arg(key).arg(member).to_owned())
    }

    /// ZREVRANK。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `member`: 集合或有序集合成员。
    pub fn z_rev_rank<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        member: V,
    ) -> Result<Ticket<Option<u64>>> {
        self.enqueue(redis::cmd("ZREVRANK").arg(key).arg(member).to_owned())
    }

    /// ZCOUNT。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 统计区间的最小 score。
    /// - `max`: 统计区间的最大 score。
    pub fn z_count(&mut self, key: &str, min: f64, max: f64) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("ZCOUNT").arg(key).arg(min).arg(max).to_owned())
    }

    /// ZINCRBY → 增后分数。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `delta`: 计数、score 或 hash field 的增量。
    /// - `member`: 集合或有序集合成员。
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

    /// ZRANGEBYSCORE。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `min`: 允许的最小值或区间下界。
    /// - `max`: 允许的最大值或区间上界。
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

    // stream / script
    /// XDEL → 删除数。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `ids`: 待删除的 entry id 列表；空切片会返回预完成 ticket。
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

    /// XACK → ack 数。空 ids 短路返回 0。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `group`: 消费组名称。
    /// - `ids`: 待确认的 entry id 列表；空切片会返回预完成 ticket。
    pub fn x_ack(&mut self, stream: &str, group: &str, ids: &[&str]) -> Result<Ticket<u64>> {
        if ids.is_empty() {
            return Ok(Ticket::ready(redis::Value::Int(0)));
        }
        let mut c = redis::cmd("XACK");
        c.arg(stream).arg(group);
        for id in ids {
            c.arg(*id);
        }
        self.enqueue(c)
    }

    /// XLEN。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    pub fn x_len(&mut self, stream: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("XLEN").arg(stream).to_owned())
    }

    /// XTRIM MAXLEN(**精确**)→ 删除数。对照 原实现 `xTrimMaxlen`(= `c.xtrim(s, l)`,无 `~`)与 direct
    /// `commands.rs::x_trim_maxlen_exact`(此前误用 `~` 近似,小 stream 完全不裁)。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `maxlen`: 精确裁剪后允许保留的最大 entry 数；`0` 表示清空。
    pub fn x_trim_maxlen(&mut self, stream: &str, maxlen: u64) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("XTRIM")
                .arg(stream)
                .arg("MAXLEN")
                .arg(maxlen)
                .to_owned(),
        )
    }

    /// XTRIM MAXLEN **近似**(`~`,radix-tree 节点边界裁剪,吞吐优先)。需要精确条数用 `x_trim_maxlen`。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `maxlen`: 近似裁剪后希望保留的最大 entry 数。
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

    /// EVAL(KEYS 同槽要求由调用方保证;EVAL 系已按真 key 路由)。
    ///
    /// # 参数
    /// - `script`: Lua 脚本文本。
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `args`: 命令或脚本参数列表。
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

    /// EVALSHA(NOSCRIPT 时调用方需先 script_load / 退回 eval)。
    ///
    /// # 参数
    /// - `sha`: Redis 脚本 SHA1 摘要。
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
    /// - `args`: 命令或脚本参数列表。
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

    // ── 全量对齐 原实现 Actuator(框架须一次铺完整,X/XAsync 折叠为单方法)──

    // key(其余)
    /// TTL(秒;-1 无 TTL,-2 不存在)。
    ///
    /// # 参数
    ///
    /// - `key`: 待查询剩余秒级 TTL 的 Redis key。
    pub fn ttl(&mut self, key: &str) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("TTL").arg(key).to_owned())
    }

    /// TYPE。
    ///
    /// # 参数
    ///
    /// - `key`: 待查询底层类型的 Redis key。
    pub fn key_type(&mut self, key: &str) -> Result<Ticket<String>> {
        self.enqueue(redis::cmd("TYPE").arg(key).to_owned())
    }

    /// KEYS(⚠ 全量扫描 + cluster 仅落单节点,生产慎用)。
    ///
    /// # 参数
    ///
    /// - `pattern`: Redis glob 模式，用于匹配当前节点上的 key。
    pub fn keys(&mut self, pattern: &str) -> Result<Ticket<Vec<String>>> {
        self.enqueue(redis::cmd("KEYS").arg(pattern).to_owned())
    }

    /// DUMP(序列化值)。
    ///
    /// # 参数
    ///
    /// - `key`: 需要导出 Redis 内部序列化值的 key。
    pub fn dump(&mut self, key: &str) -> Result<Ticket<Option<Vec<u8>>>> {
        self.enqueue(redis::cmd("DUMP").arg(key).to_owned())
    }

    /// DEL(多 key)→ 删除数(⚠ cluster 跨 slot → CROSSSLOT)。空 keys 短路返回 0。
    ///
    /// # 参数
    ///
    /// - `keys`: 待删除的一组 Redis key；空切片会返回预完成 ticket。
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

    /// EXISTS(多 key)→ 存在计数(⚠ cluster 跨 slot)。空 keys 短路返回 0。
    ///
    /// # 参数
    ///
    /// - `keys`: 待检查的一组 Redis key；空切片会返回预完成 ticket。
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
    /// SETRANGE → 修改后长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `val`: 要写入 Redis 或发送到下游的值。
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

    /// GETRANGE。
    ///
    /// # 参数
    ///
    /// - `key`: 字符串值所在的 Redis key。
    /// - `start`: 字节区间起始下标，支持 Redis 负索引。
    /// - `end`: 字节区间结束下标，支持 Redis 负索引。
    pub fn get_range(&mut self, key: &str, start: isize, end: isize) -> Result<Ticket<String>> {
        self.enqueue(
            redis::cmd("GETRANGE")
                .arg(key)
                .arg(start)
                .arg(end)
                .to_owned(),
        )
    }

    /// DECRBY → 减后值(对照 原实现 `decrement`)。
    ///
    /// # 参数
    ///
    /// - `key`: 保存整数计数值的 Redis key。
    /// - `delta`: 本次递减的正向步长。
    pub fn decr_by(&mut self, key: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(redis::cmd("DECRBY").arg(key).arg(delta).to_owned())
    }

    /// INCRBYFLOAT → 增后值。
    ///
    /// # 参数
    ///
    /// - `key`: 保存浮点计数值的 Redis key。
    /// - `delta`: 本次递增的浮点步长，可为负值。
    pub fn incr_by_float(&mut self, key: &str, delta: f64) -> Result<Ticket<f64>> {
        self.enqueue(redis::cmd("INCRBYFLOAT").arg(key).arg(delta).to_owned())
    }

    /// **Redis 原生 `MSET`**(单命令原子多写)。⚠ cluster 下所有 key **须同 slot**,否则 Redis loud
    /// CROSSSLOT。空 pairs 短路 no-op。**注意:这不等价 原实现 `multiSet`** ——原实现 的 `multiSet(Map)`
    /// 把每对**拆成独立 `SET`**(`kvMap.forEach((k,v)->set(k,v))`),cluster 下各自按 slot 路由;要那个语义用
    /// [`Self::multi_set_split`]。
    ///
    /// # 参数
    ///
    /// - `pairs`: 要在单条 `MSET` 中写入的 key/value 字节对；空切片会返回预完成 ticket。
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

    /// **对齐 原实现 `multiSet(Map)`**:逐对入队独立 `SET`,返回每对各自的 `Ticket<()>`。cluster 下由
    /// `execute` 的按-slot 分桶把它们路由到各自节点(**不会 CROSSSLOT**)。空 pairs → 空 Vec。
    ///
    /// # 参数
    ///
    /// - `pairs`: 要拆分为多条 `SET` 入队的 key/value 字节对。
    pub fn multi_set_split(&mut self, pairs: &[(&str, &[u8])]) -> Result<Vec<Ticket<()>>> {
        let mut tickets = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            tickets.push(self.enqueue(redis::cmd("SET").arg(*k).arg(*v).to_owned())?);
        }
        Ok(tickets)
    }

    // hash(其余)
    /// HSET(多 field,= HMSET)。对照 原实现 `hMSet`。空 pairs 短路 no-op。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `pairs`: 要写入的 field/value 字节对；空切片会返回预完成 ticket。
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

    /// HEXISTS。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 待检查是否存在的 hash field。
    pub fn h_exists(&mut self, key: &str, field: &str) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("HEXISTS").arg(key).arg(field).to_owned())
    }

    /// HVALS。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn h_vals<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("HVALS").arg(key).to_owned())
    }

    /// HRANDFIELD(单个)。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    pub fn h_rand_field(&mut self, key: &str) -> Result<Ticket<Option<String>>> {
        self.enqueue(redis::cmd("HRANDFIELD").arg(key).to_owned())
    }

    /// HINCRBYFLOAT → 增后值。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递增的 hash field。
    /// - `delta`: 本次递增的浮点步长，可为负值。
    pub fn h_incr_by_float(&mut self, key: &str, field: &str, delta: f64) -> Result<Ticket<f64>> {
        self.enqueue(
            redis::cmd("HINCRBYFLOAT")
                .arg(key)
                .arg(field)
                .arg(delta)
                .to_owned(),
        )
    }

    /// HINCRBY 负增(对照 原实现 `hDecrBy`)→ 减后值。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递减的 hash field。
    /// - `delta`: 本次递减的正向步长，内部会转为负增量。
    pub fn h_decr_by(&mut self, key: &str, field: &str, delta: i64) -> Result<Ticket<i64>> {
        self.enqueue(
            redis::cmd("HINCRBY")
                .arg(key)
                .arg(field)
                .arg(-delta)
                .to_owned(),
        )
    }

    // list(其余)
    /// LPUSH(多值)→ push 后长度。空 vals 短路返回 0(no-op 哨兵,**非列表当前长度**; 避免非法命令)。
    ///
    /// # 参数
    ///
    /// - `key`: 列表所在的 Redis key。
    /// - `vals`: 要从左侧压入的一组字节值；空切片会返回预完成 ticket。
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

    /// RPUSH(多值)→ push 后长度。空 vals 短路返回 0(no-op 哨兵,**非列表当前长度**)。
    ///
    /// # 参数
    ///
    /// - `key`: 列表所在的 Redis key。
    /// - `vals`: 要从右侧压入的一组字节值；空切片会返回预完成 ticket。
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

    /// LPUSHX(仅 key 存在;对照 原实现 `lPushIfAbsent`)→ push 后长度。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn l_push_if_absent<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("LPUSHX").arg(key).arg(val).to_owned())
    }

    /// RPUSHX(对照 原实现 `rPushIfAbsent`)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn r_push_if_absent<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("RPUSHX").arg(key).arg(val).to_owned())
    }

    /// LINDEX。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `index`: 列表下标、归档序号或字段位置。
    pub fn l_index<T: FromRedisValue>(
        &mut self,
        key: &str,
        index: isize,
    ) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("LINDEX").arg(key).arg(index).to_owned())
    }

    /// LPOS(首个匹配下标)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn l_pos<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        val: V,
    ) -> Result<Ticket<Option<i64>>> {
        self.enqueue(redis::cmd("LPOS").arg(key).arg(val).to_owned())
    }

    /// LSET。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `index`: 列表下标、归档序号或字段位置。
    /// - `val`: 要写入 Redis 或发送到下游的值。
    pub fn l_set<V: redis::ToSingleRedisArg>(
        &mut self,
        key: &str,
        index: isize,
        val: V,
    ) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("LSET").arg(key).arg(index).arg(val).to_owned())
    }

    /// LINSERT BEFORE → 插入后长度(-1=pivot 不存在)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `pivot`: 列表插入使用的 pivot 元素。
    /// - `val`: 要写入 Redis 或发送到下游的值。
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

    /// LINSERT AFTER。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `pivot`: 列表插入使用的 pivot 元素。
    /// - `val`: 要写入 Redis 或发送到下游的值。
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
    /// SADD(多 member)→ 新增数。空 members 短路返回 0。
    ///
    /// # 参数
    ///
    /// - `key`: 集合所在的 Redis key。
    /// - `members`: 要加入集合的一组成员字节值；空切片会返回预完成 ticket。
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

    /// SREM(多 member)→ 删除数。空 members 短路返回 0。
    ///
    /// # 参数
    ///
    /// - `key`: 集合所在的 Redis key。
    /// - `members`: 要移出集合的一组成员字节值；空切片会返回预完成 ticket。
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

    /// SMOVE(⚠ src/dst 须同 slot)。
    ///
    /// # 参数
    /// - `src`: 重命名、拷贝或迁移操作的源 key。
    /// - `dst`: 重命名、拷贝或迁移操作的目标 key。
    /// - `member`: 集合或有序集合成员。
    pub fn s_move<V: redis::ToSingleRedisArg>(
        &mut self,
        src: &str,
        dst: &str,
        member: V,
    ) -> Result<Ticket<bool>> {
        self.enqueue(redis::cmd("SMOVE").arg(src).arg(dst).arg(member).to_owned())
    }

    /// SMISMEMBER(多 member)→ 各是否成员。空 members 短路返回空数组。
    ///
    /// # 参数
    ///
    /// - `key`: 集合所在的 Redis key。
    /// - `members`: 待批量判定的一组成员字节值；空切片会返回空结果 ticket。
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

    /// SRANDMEMBER(单个)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn s_rand_member<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("SRANDMEMBER").arg(key).to_owned())
    }

    /// SDIFF(⚠ 多 key 同 slot)。空 keys 短路返回空数组。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
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

    /// SINTER(⚠ 多 key 同 slot)。空 keys 短路返回空数组。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
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

    /// SUNION(⚠ 多 key 同 slot)。空 keys 短路返回空数组。
    ///
    /// # 参数
    /// - `keys`: Redis key 列表,用于批量读取、删除或集合运算。
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
    /// ZREMRANGEBYRANK → 删除数(对照 原实现 `zRemRange`)。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `start`: 要删除的排名起始下标，支持 Redis 负索引。
    /// - `stop`: 要删除的排名结束下标，支持 Redis 负索引。
    pub fn z_rem_range(&mut self, key: &str, start: isize, stop: isize) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZREMRANGEBYRANK")
                .arg(key)
                .arg(start)
                .arg(stop)
                .to_owned(),
        )
    }

    /// ZREMRANGEBYLEX → 删除数。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 字典序区间下界，使用 Redis lex 语法。
    /// - `max`: 字典序区间上界，使用 Redis lex 语法。
    pub fn z_rem_range_by_lex(&mut self, key: &str, min: &str, max: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZREMRANGEBYLEX")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// ZREMRANGEBYSCORE → 删除数。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 要删除的最小 score 边界。
    /// - `max`: 要删除的最大 score 边界。
    pub fn z_rem_range_by_score(&mut self, key: &str, min: f64, max: f64) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZREMRANGEBYSCORE")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// ZMSCORE(多 member)。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `members`: 待批量查询 score 的成员字节值；空切片会返回空结果 ticket。
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

    /// ZLEXCOUNT。
    ///
    /// # 参数
    ///
    /// - `key`: 有序集合所在的 Redis key。
    /// - `min`: 字典序统计区间下界，使用 Redis lex 语法。
    /// - `max`: 字典序统计区间上界，使用 Redis lex 语法。
    pub fn z_lex_count(&mut self, key: &str, min: &str, max: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("ZLEXCOUNT")
                .arg(key)
                .arg(min)
                .arg(max)
                .to_owned(),
        )
    }

    /// ZRANGE(按下标)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
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

    /// ZREVRANGE(按下标)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
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

    /// ZRANGE WITHSCORES → `(member, score)` 列表。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
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

    /// ZREVRANGE WITHSCORES。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `stop`: Redis 区间或列表裁剪的终点。
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

    /// ZRANGEBYSCORE WITHSCORES。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `min`: 允许的最小值或区间下界。
    /// - `max`: 允许的最大值或区间上界。
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

    /// ZREVRANGEBYSCORE(注意 max 在前)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `max`: 允许的最大值或区间上界。
    /// - `min`: 允许的最小值或区间下界。
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

    /// ZREVRANGEBYSCORE WITHSCORES。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `max`: 允许的最大值或区间上界。
    /// - `min`: 允许的最小值或区间下界。
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

    /// ZRANGEBYLEX。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `min`: 允许的最小值或区间下界。
    /// - `max`: 允许的最大值或区间上界。
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

    /// ZPOPMIN(count 个)→ `(member, score)` 列表。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn z_pop_min<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(redis::cmd("ZPOPMIN").arg(key).arg(count).to_owned())
    }

    /// ZPOPMAX(count 个)。
    ///
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn z_pop_max<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<(T, f64)>>> {
        self.enqueue(redis::cmd("ZPOPMAX").arg(key).arg(count).to_owned())
    }

    /// ZRANDMEMBER(单个)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    pub fn z_rand_member<T: FromRedisValue>(&mut self, key: &str) -> Result<Ticket<Option<T>>> {
        self.enqueue(redis::cmd("ZRANDMEMBER").arg(key).to_owned())
    }

    // stream / script / pubsub(其余)
    /// XTRIM MINID(**精确**)→ 删除数。对照 原实现 `xTrimMinId`(= `XTrimArgs.minId(minId)`,无 `~`)。
    ///此前误用 `~` 近似。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `minid`: 保留边界 id；小于该 id 的 entry 会被精确裁剪。
    pub fn x_trim_minid(&mut self, stream: &str, minid: &str) -> Result<Ticket<u64>> {
        self.enqueue(
            redis::cmd("XTRIM")
                .arg(stream)
                .arg("MINID")
                .arg(minid)
                .to_owned(),
        )
    }

    /// XTRIM MINID **近似**(`~`,吞吐优先)。需要精确边界用 `x_trim_minid`。
    ///
    /// # 参数
    ///
    /// - `stream`: stream 所在的 Redis key。
    /// - `minid`: 近似裁剪的保留边界 id。
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

    /// SCRIPT FLUSH(⚠ cluster 下仅落单节点,非全节点;需全节点请 per-node)。
    pub fn script_flush(&mut self) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("SCRIPT").arg("FLUSH").to_owned())
    }

    /// SCRIPT KILL(⚠ cluster 下仅落单节点)。
    pub fn script_kill(&mut self) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("SCRIPT").arg("KILL").to_owned())
    }

    /// SCRIPT LOAD → sha1。对照 原实现 `scriptLoad`。
    ///
    /// # 参数
    ///
    /// - `script`: 需要加载到 Redis 脚本缓存中的 Lua 脚本文本。
    pub fn script_load(&mut self, script: &str) -> Result<Ticket<String>> {
        self.enqueue(redis::cmd("SCRIPT").arg("LOAD").arg(script).to_owned())
    }

    /// SCRIPT EXISTS(多 sha)→ 各是否已缓存。对照 原实现 `scriptExists`。空 sha 短路返回空数组。
    ///
    /// # 参数
    ///
    /// - `shas`: 待检查是否存在于脚本缓存中的 SHA1 列表；空切片会返回空结果 ticket。
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

    // ── count 重载(;对照 原实现 `lPop(key,count)`/`sPop(key,count)` 等)──
    /// LPOP key count → 弹出的多个元素。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn l_pop_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("LPOP").arg(key).arg(count).to_owned())
    }

    /// RPOP key count → 弹出的多个元素。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn r_pop_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("RPOP").arg(key).arg(count).to_owned())
    }

    /// SPOP key count → 弹出的多个 member。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn s_pop_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: usize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("SPOP").arg(key).arg(count).to_owned())
    }

    /// SRANDMEMBER key count(count<0 允许重复)→ 多个 member。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn s_rand_member_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("SRANDMEMBER").arg(key).arg(count).to_owned())
    }

    /// ZRANDMEMBER key count(count<0 允许重复)→ 多个 member。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
    pub fn z_rand_member_count<T: FromRedisValue>(
        &mut self,
        key: &str,
        count: isize,
    ) -> Result<Ticket<Vec<T>>> {
        self.enqueue(redis::cmd("ZRANDMEMBER").arg(key).arg(count).to_owned())
    }

    /// HRANDFIELD key count(count<0 允许重复)→ 多个 field。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `count`: 随机返回的 field 数；负数允许重复。
    pub fn h_rand_field_count(&mut self, key: &str, count: isize) -> Result<Ticket<Vec<String>>> {
        self.enqueue(redis::cmd("HRANDFIELD").arg(key).arg(count).to_owned())
    }

    // ── zset LIMIT 分页 / 区间字符串重载──
    /// ZRANGEBYSCORE key min max LIMIT offset count(min/max 用 `(`/`[`/`-inf`/`+inf` 字符串语法表达开闭/无穷)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `min`: 允许的最小值或区间下界。
    /// - `max`: 允许的最大值或区间上界。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// ZREVRANGEBYSCORE key max min LIMIT offset count。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `max`: 允许的最大值或区间上界。
    /// - `min`: 允许的最小值或区间下界。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// ZRANGEBYLEX key min max LIMIT offset count(min/max 用 `[`/`(`/`-`/`+` lex 语法)。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `min`: 允许的最小值或区间下界。
    /// - `max`: 允许的最大值或区间上界。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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
    /// XRANGE key start end(`-`/`+` 表全范围)→ `Vec<(id, fields)>` 由调用方类型化。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `end`: Redis 区间、时间窗口或解析范围的终点。
    pub fn x_range<T: FromRedisValue>(
        &mut self,
        key: &str,
        start: &str,
        end: &str,
    ) -> Result<Ticket<T>> {
        self.enqueue(redis::cmd("XRANGE").arg(key).arg(start).arg(end).to_owned())
    }

    /// XRANGE key start end COUNT n。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `end`: Redis 区间、时间窗口或解析范围的终点。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// XREVRANGE key end start。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `end`: Redis 区间、时间窗口或解析范围的终点。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
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

    /// XREVRANGE key end start COUNT n。
    ///
    /// # 参数
    /// - `key`: 当前 Redis 命令操作的 key。
    /// - `end`: Redis 区间、时间窗口或解析范围的终点。
    /// - `start`: Redis 区间、时间窗口或解析范围的起点。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// Redis Pub/Sub `PUBLISH` → 收到的订阅者数。classic Pub/Sub 在 pipeline 中也让位,
    /// 频道发布只保留 `publish_channel`;`publish` 方法名留给 stream 事件发布。
    ///
    /// # 参数
    /// - `channel`: Redis 发布订阅使用的频道名称。
    /// - `message`: 业务消息体或事件载荷。
    pub fn publish_channel<V: redis::ToSingleRedisArg>(
        &mut self,
        channel: &str,
        message: V,
    ) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("PUBLISH").arg(channel).arg(message).to_owned())
    }

    /// 发布 stream 事件(event-field wire),写 `XADD <stream> * <event> <StreamEnvelope JSON>`。
    /// 空 stream/event 或 message 序列化为 JSON null → `Config` 错。
    ///
    /// # 参数
    /// - `stream`: XADD 写入的 Redis Stream key。
    /// - `event`: entry field 名,订阅侧按该值分发 handler。
    /// - `message`: 业务消息体或事件载荷。
    pub fn publish<T: serde::Serialize>(
        &mut self,
        stream: &str,
        event: &str,
        message: &T,
    ) -> Result<Ticket<String>> {
        // 校验 + 信封编码走 stream 模块的**共享 helper**(与 `RedisClient::publish` 同一路径,
        // 逐字节一致);这里只负责把 XADD 排进 pipeline。
        let body = crate::stream::stream_publish_body(stream, event, message)?;
        self.enqueue(
            redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg(event)
                .arg(body)
                .to_owned(),
        )
    }

    /// 使用默认 event = `STREAM_EVENT`(`"msg"`)发布一条 stream 消息。
    ///
    /// # 参数
    /// - `stream`: XADD 写入的 Redis Stream key。
    /// - `message`: 业务消息体或事件载荷。
    pub fn publish_default<T: serde::Serialize>(
        &mut self,
        stream: &str,
        message: &T,
    ) -> Result<Ticket<String>> {
        self.publish(stream, crate::stream::STREAM_EVENT, message)
    }

    /// 兼容旧 pipeline 命名,语义同 [`Self::publish`]。
    ///
    /// # 参数
    /// - `stream`: XADD 写入的 Redis Stream key。
    /// - `event`: entry field 名,订阅侧按该值分发 handler。
    /// - `data`: 待序列化进 StreamEnvelope 的业务对象。
    pub fn publish_stream<T: serde::Serialize>(
        &mut self,
        stream: &str,
        event: &str,
        data: &T,
    ) -> Result<Ticket<String>> {
        self.publish(stream, event, data)
    }

    /// 兼容旧 pipeline 命名,语义同 [`Self::publish_default`]。
    ///
    /// # 参数
    /// - `stream`: XADD 写入的 Redis Stream key。
    /// - `data`: 待序列化进 StreamEnvelope 的业务对象。
    pub fn publish_stream_default<T: serde::Serialize>(
        &mut self,
        stream: &str,
        data: &T,
    ) -> Result<Ticket<String>> {
        self.publish_default(stream, data)
    }

    /// hash field 自减 `delta`,**余值 ≤0 则删该 field**,返回自减后余值(对照 原实现 `hDecrByAndDel`,EVAL
    /// Lua 逐字复刻 `/lua/hash_hdecriby_del.lua`)。`KEYS=[key] ARGV=[field, delta]`,cluster 安全(key 经 KEYS 路由)。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key，也是脚本的唯一 KEYS 路由键。
    /// - `field`: 需要递减并按余值决定是否删除的 hash field。
    /// - `delta`: 本次递减的正向步长。
    pub fn h_decr_by_and_del(&mut self, key: &str, field: &str, delta: i64) -> Result<Ticket<i64>> {
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

    /// `h_decr_by_and_del` delta=1(对照 原实现 `hDecrByAndDel(key, hashKey)`)。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要递减 `1` 并按余值决定是否删除的 hash field。
    pub fn h_decr_by_and_del_one(&mut self, key: &str, field: &str) -> Result<Ticket<i64>> {
        self.h_decr_by_and_del(key, field, 1)
    }

    /// HEXPIRE key `<秒>` FIELDS 1 `<field>`(对照 原实现 `expire(key, Duration, hashKey)` = lettuce `hexpire`,
    /// **秒**精度;亚秒 Duration 截断成 0=立即过期,与 原实现 一致)。返回单 field 状态码数组(1=设/2=已过期删/
    /// 0=条件未满足/-2=无此 field)。⚠ 需 Redis 7.4+(跟随 原实现 行为,不论服务端版本)。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `field`: 需要设置 field 级过期时间的 hash field。
    /// - `ttl`: field 级相对过期时间，发送给 Redis 时取秒级。
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

    /// HEXPIRE 多 field(对照 原实现 `OP_HEXPIRE_MULTI` = `c.hexpire(key, Duration, byte[][])`)。空 fields 短路空数组。
    ///
    /// # 参数
    ///
    /// - `key`: 哈希结构所在的 Redis key。
    /// - `fields`: 需要设置 field 级过期时间的 hash field 列表；空切片会返回空结果 ticket。
    /// - `ttl`: field 级相对过期时间，发送给 Redis 时取秒级。
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

    // ── RedisJSON / RediSearch(feature "search";对照 原实现 jsonSet/ftSearch 等)──
    /// JSON.SET(RedisJSON)。对照 原实现 `jsonSet`。
    ///
    /// # 参数
    ///
    /// - `key`: RedisJSON 文档所在的 Redis key。
    /// - `path`: RedisJSON 路径，例如 `$` 或 `$.items[0]`。
    /// - `json`: 要写入该路径的 JSON 文本。
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

    /// JSON.GET。对照 原实现 `jsonGet`。
    ///
    /// # 参数
    ///
    /// - `key`: RedisJSON 文档所在的 Redis key。
    /// - `path`: RedisJSON 路径，例如 `$` 或 `$.items[0]`。
    #[cfg(feature = "search")]
    pub fn json_get(&mut self, key: &str, path: &str) -> Result<Ticket<Option<String>>> {
        self.enqueue(redis::cmd("JSON.GET").arg(key).arg(path).to_owned())
    }

    /// JSON.DEL → 删除的路径数。对照 原实现 `jsonDel`。
    ///
    /// # 参数
    ///
    /// - `key`: RedisJSON 文档所在的 Redis key。
    /// - `path`: 要删除的 RedisJSON 路径。
    #[cfg(feature = "search")]
    pub fn json_del(&mut self, key: &str, path: &str) -> Result<Ticket<u64>> {
        self.enqueue(redis::cmd("JSON.DEL").arg(key).arg(path).to_owned())
    }

    /// JSON.NUMINCRBY(**整数** delta)。对照 原实现 `jsonNumIncrBy(long delta)`(撮合部分成交/持仓加减热路径,
    /// 数量是整数)。此前用 `f64` 会把 `5.0` 原样下发,RedisJSON 把整数字段写成浮点 shape
    /// (`"qty":6.0`),后续 typed `i64/u64` 文档 serde 读回失败(`invalid type: floating point`)。
    /// 浮点自增请显式用 [`Self::json_num_incr_by_f64`]。
    ///
    /// # 参数
    ///
    /// - `key`: RedisJSON 文档所在的 Redis key。
    /// - `path`: 需要递增的 RedisJSON 数值路径。
    /// - `delta`: 本次递增的整数步长，可为负值。
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

    /// JSON.NUMINCRBY(**浮点** delta)。⚠ 会保留/制造 JSON 浮点形态(`"x":6.0`),整数 typed 字段勿用——
    /// 那会破坏 serde 读回(见 [`Self::json_num_incr_by`])。仅当字段本身就是浮点时用。
    ///
    /// # 参数
    ///
    /// - `key`: RedisJSON 文档所在的 Redis key。
    /// - `path`: 需要递增的 RedisJSON 数值路径。
    /// - `delta`: 本次递增的浮点步长，可为负值。
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

    /// FT.SEARCH(原样 query;typed 解析见 search 模块)。对照 原实现 `ftSearch`。
    ///
    /// # 参数
    ///
    /// - `index`: RediSearch 索引名称。
    /// - `query`: 要传给 `FT.SEARCH` 的查询字符串。
    #[cfg(feature = "search")]
    pub fn ft_search(&mut self, index: &str, query: &str) -> Result<Ticket<redis::Value>> {
        self.enqueue(redis::cmd("FT.SEARCH").arg(index).arg(query).to_owned())
    }

    /// FT.AGGREGATE。对照 原实现 `ftAggregate`。
    ///
    /// # 参数
    ///
    /// - `index`: RediSearch 索引名称。
    /// - `query`: 要传给 `FT.AGGREGATE` 的聚合查询字符串。
    #[cfg(feature = "search")]
    pub fn ft_aggregate(&mut self, index: &str, query: &str) -> Result<Ticket<redis::Value>> {
        self.enqueue(redis::cmd("FT.AGGREGATE").arg(index).arg(query).to_owned())
    }

    /// FT.DROPINDEX。对照 原实现 `ftDropIndex`。
    ///
    /// # 参数
    ///
    /// - `index`: 要删除的 RediSearch 索引名称。
    #[cfg(feature = "search")]
    pub fn ft_drop_index(&mut self, index: &str) -> Result<Ticket<()>> {
        self.enqueue(redis::cmd("FT.DROPINDEX").arg(index).to_owned())
    }

    /// `FT.DROPINDEX [DD]`（`dd=true` 时连同删除文档）。对照 原实现
    /// `ftDropIndex(index, dropDocs)`。
    ///
    /// # 参数
    ///
    /// - `index`: 要删除的 RediSearch 索引名称。
    /// - `dd`: 是否同时删除索引关联的文档。
    #[cfg(feature = "search")]
    pub fn ft_drop_index_dd(&mut self, index: &str, dd: bool) -> Result<Ticket<()>> {
        let mut c = redis::cmd("FT.DROPINDEX");
        c.arg(index);
        if dd {
            c.arg("DD");
        }
        self.enqueue(c)
    }

    /// FT.INFO。对照 原实现 `ftInfo`。
    ///
    /// # 参数
    ///
    /// - `index`: 要查询元信息的 RediSearch 索引名称。
    #[cfg(feature = "search")]
    pub fn ft_info(&mut self, index: &str) -> Result<Ticket<redis::Value>> {
        self.enqueue(redis::cmd("FT.INFO").arg(index).to_owned())
    }

    // ── FT 原样 args 透传──
    /// FT.SEARCH index 后接**原样 args**(SORTBY/LIMIT/DIALECT/RETURN/FILTER… 由调用方逐段给)。对照 原实现
    /// `ftSearch(index, String[] args)`。typed 结果解析建议走 search 模块的 query builder,本入口只透传。
    ///
    /// # 参数
    ///
    /// - `index`: RediSearch 索引名称。
    /// - `args`: `FT.SEARCH` 在索引名之后的原样参数片段。
    #[cfg(feature = "search")]
    pub fn ft_search_args(&mut self, index: &str, args: &[&str]) -> Result<Ticket<redis::Value>> {
        let mut c = redis::cmd("FT.SEARCH");
        c.arg(index);
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    /// FT.AGGREGATE index 后接原样 args。对照 原实现 `ftAggregate(index, String[] args)`。
    ///
    /// # 参数
    ///
    /// - `index`: RediSearch 索引名称。
    /// - `args`: `FT.AGGREGATE` 在索引名之后的原样参数片段。
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

    /// FT.CREATE index 后接原样 args(schema/ON HASH|JSON/PREFIX… 由调用方给)。对照 原实现 `ftCreate`。
    ///
    /// # 参数
    ///
    /// - `index`: 要创建的 RediSearch 索引名称。
    /// - `args`: `FT.CREATE` 在索引名之后的 schema、前缀和索引选项参数。
    #[cfg(feature = "search")]
    pub fn ft_create_args(&mut self, index: &str, args: &[&str]) -> Result<Ticket<()>> {
        let mut c = redis::cmd("FT.CREATE");
        c.arg(index);
        for a in args {
            c.arg(*a);
        }
        self.enqueue(c)
    }

    /// 收尾会话(消费 self):**先等齐已 auto-flush 的各段**(它们的 ticket 已在后台按段序回填),**再发出
    /// 最后一段未满批**。逐命令独立结果回填到各 Ticket。
    ///
    /// **standalone**:最后一段组装单条 `redis::Pipeline`,一条 driver 消息、一次 flush、严格入队顺序;
    /// 返回裸 `Vec<Value>`,server error 以 `Value::ServerError` 逐槽位内联(不经 query_async 整批
    /// 聚合),单条 server error 只影响该 Ticket;传输失败 → 该段 ExecutionUnknown。
    /// **cluster**:**按 key slot 分桶**(同 AutoPipeline),每桶同 slot 一条
    /// sub-pipeline 并行发——否则多 key 单批(如 `save_all`)跨 slot 会整批路由首命令节点、其余 MOVED。
    /// ⚠ cluster 下**仅同 slot 内保序**(跨 slot 命令落不同节点,无法单批原子保序);保序需求请同槽。
    ///
    /// `execute` 返回的 `Result` 聚合**所有已 seal 段 + 最后一段**的批级传输状态:任一段传输失败(写出后
    /// 断线 / 响应数不符)返回**第一个**错误(不再吞掉 auto-flush 段错误)。单命令确定性失败
    /// (`Value::ServerError`)不进此返回值,仍经各自 ticket 的 `await_result()` 获取——逐 ticket 检查结果的
    /// 契约不变,`execute().await?` 现额外保证"本 session 已提交各段无批级传输异常"。
    pub async fn execute(mut self) -> Result<()> {
        self.executed = true;
        // 等齐已 seal 的后台各段(链式串行,await 链尾即等齐全部);取链上第一个批级错误。
        // 链尾 task panic/cancel → JoinError 映射成 ExecutionUnknown(不假成功)。
        let chain_res = match self.flush_chain.take() {
            Some(chain) => chain.await.unwrap_or_else(join_err_to_unknown),
            None => Ok(()),
        };
        let cmds = std::mem::take(&mut self.cmds);
        let last = dispatch_jobs(&self.client, cmds).await;
        chain_res.and(last) // 前序段错误优先,其次末段
    }
}

impl Drop for PipelineSession {
    /// 丢弃未显式执行的 pipeline 会话。
    ///
    /// 已 auto-flush 的段保持后台提交；仅最后一段尚未发出的命令被标记为 `NotSent`,让调用方可安全重试。
    fn drop(&mut self) {
        // 已 auto-flush 的各段 = **已提交**(同 原实现 autoFlush 已写出):`flush_chain` 句柄被 drop 后任务
        // 自动 detach,在后台继续完成、回填各自 ticket——不撤销。仅**最后一段未满批**(从未发出)按 NotSent
        // → 调用方 await_result 得 `NotExecuted`(确定未发,可安全重发),区别于"可能已发"的 ExecutionUnknown。
        if !self.executed && !self.cmds.is_empty() {
            let pending = self.cmds.len();
            for (_cmd, tx) in self.cmds.drain(..) {
                let _ = tx.send(Err(TicketErr::NotSent));
            }
            tracing::warn!(pending, "PipelineSession 未 execute 即丢弃,最后一段未发(ticket → NotExecuted;已 auto-flush 段不受影响)");
        }
    }
}

// ═══════════════════ 自动微批（时间轮合批 + caller-runs 背压）═══════════════════
//
// 对照 原实现 LettucePipeline 自动微批:多生产者把命令丢进有界队列,后台任务按**时间窗 + 批量上限**
// 合并成一条 pipeline 一次发出;队列满时 `execute().await` 阻塞 = caller-runs 背压(天然限流)。
// 失败语义同显式 session:Value::ServerError = 单命令确定性失败;传输错误/响应数不符 = 整批
// ExecutionUnknown。停机 drain 已入队命令不丢。

/// 自动微批配置。
#[derive(Debug, Clone)]
pub struct MicroBatchCfg {
    /// 合批时间窗:收到首命令后等这么久收集更多(对照 原实现 1ms 时间轮)。
    pub window: Duration,
    /// 单批最大命令数(到此立即 flush,不等满窗)。
    pub max_batch: usize,
    /// 入队队列容量(满 → caller-runs 背压)。
    pub queue_capacity: usize,
    /// **单命令参数字节上限**(0=不限)。超限的命令入队即拒绝(字节级背压)。
    /// 与 `queue_capacity` 联合给出队列字节上界 ≈ `queue_capacity × max_command_bytes`,防大 value 撑爆内存。
    pub max_command_bytes: usize,
    /// **单批参数字节软上限**(0=不限):累计达此立即 flush(即便未满 `max_batch`/窗口),防单条巨 pipeline。
    /// ⚠ **软上限**:检查在收下一条之后,故实际批字节**最多超出一条命令的字节**
    /// (≤ `max_command_bytes`,若已设)。需要硬上限请配 `max_command_bytes` 收敛单条体量;真·硬上限
    /// 若业务需要严格硬上限,需扩展为"溢出命令转下批首"的分批策略。
    pub max_batch_bytes: usize,
}

impl Default for MicroBatchCfg {
    /// 构造自动微批默认配置。
    ///
    /// 默认 1ms 时间窗、1000 条单批和 4096 入队容量,字节上限保持关闭以兼容既有业务行为。
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

/// 估算命令参数总字节(用于字节级背压;只数 `Simple` 实参,游标极小忽略)。
///
/// # 参数
/// - `cmd`: 底层 Redis 命令对象。
fn estimate_cmd_bytes(cmd: &redis::Cmd) -> usize {
    cmd.args_iter()
        .map(|a| match a {
            redis::Arg::Simple(b) => b.len(),
            _ => 8, // Cursor 等非数据实参极小,记常量
        })
        .sum()
}

/// AutoPipeline 命令准入:阻塞 / Pub-Sub / 全局管理命令
/// **不进自动微批**——它们会拖住共享后台 flusher 或改变连接语义。返回 `Some(原因)` = 拒绝。
/// (`PipelineSession::enqueue` 保留 raw 能力;`AutoPipeline` 是共享设施故收紧。)
///
/// # 参数
/// - `cmd`: 底层 Redis 命令对象。
fn auto_pipeline_reject(cmd: &redis::Cmd) -> Option<&'static str> {
    let mut it = cmd.args_iter();
    let name = match it.next() {
        Some(redis::Arg::Simple(b)) => b.to_ascii_uppercase(),
        _ => return Some("命令无名字"),
    };
    match name.as_slice() {
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
        | b"SWAPDB" | b"RESET" => Some(
            "全局/管理命令不可进 AutoPipeline(cluster 下只落任意单节点,语义不等价),请走专门 API",
        ),
        // XREAD/XREADGROUP 仅带 BLOCK 时阻塞
        b"XREAD" | b"XREADGROUP" => {
            for a in it {
                if let redis::Arg::Simple(s) = a {
                    if s.eq_ignore_ascii_case(b"BLOCK") {
                        return Some("XREAD/XREADGROUP BLOCK 阻塞,不可进 AutoPipeline");
                    }
                }
            }
            None
        }
        _ => None,
    }
}

type BatchJob = (
    redis::Cmd,
    oneshot::Sender<std::result::Result<redis::Value, TicketErr>>,
);

/// 后台 auto-flush task 的 `JoinError`(panic/cancel)→ `ExecutionUnknown`(批级不确定,**不吞成 Ok**;
///pipeline)。仅用于 `flush_chain` 段任务的 `JoinHandle::await` 失败映射。
///
/// # 参数
/// - `e`: 错误对象或外部错误值。
fn join_err_to_unknown(e: tokio::task::JoinError) -> Result<()> {
    Err(NasaRedisError::ExecutionUnknown(format!(
        "pipeline auto-flush task 结束异常(panic/cancel): {e}"
    )))
}

/// 自动微批管道:后台任务把多生产者的命令合批成一条 pipeline 发出。`Arc` 共享给多调用方。
pub struct AutoPipeline {
    tx: mpsc::Sender<BatchJob>,
    cancel: CancellationToken,
    /// 单命令字节上限(0=不限),入队前检查(从 `MicroBatchCfg` 复制)。
    max_command_bytes: usize,
    handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// drain 完成信号(`AutoPipeline` 是 `Arc` 共享,多持有者并发 `shutdown()` 时
    /// 只有抢到 handle 的那个 await drain,其余此前 take 到 None **立即返回**误以为 drain 完成。改:抢到的
    /// drain 完后 `send(true)`,其余 await 此 watch 直到 true——**所有调用者都等到同一 drain 完成**)。
    drained: tokio::sync::watch::Sender<bool>,
}

impl AutoPipeline {
    /// 启动后台合批任务。
    ///
    /// # 参数
    ///
    /// - `client`: 后台 flusher 使用的共享 Redis 客户端。
    /// - `cfg`: 自动微批窗口、批大小、队列容量和字节背压配置。
    pub fn start(client: Arc<RedisClient>, cfg: MicroBatchCfg) -> Arc<Self> {
        let queue_capacity = cfg
            .queue_capacity
            .clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let (tx, rx) = mpsc::channel(queue_capacity);
        let cancel = CancellationToken::new();
        let max_command_bytes = cfg.max_command_bytes;
        let handle = tokio::spawn(flush_loop(client, rx, cfg, cancel.clone()));
        let (drained, _) = tokio::sync::watch::channel(false);
        Arc::new(Self {
            tx,
            cancel,
            max_command_bytes,
            handle: std::sync::Mutex::new(Some(handle)),
            drained,
        })
    }

    /// 提交一条命令,await 自己的类型化结果。**队列满 → 阻塞(真 caller-runs 背压)**。
    /// 注:这是对 原实现 的**有意改良**——原实现 LettucePipeline 用无界 ConcurrentLinkedQueue
    /// (满了不阻塞、内存可能无界增长),Rust 用有界 `mpsc`(`queue_capacity`),满则 `send().await` 阻塞生产者
    /// = 天然限流,绝不无界堆积 OOM。
    ///
    /// # 参数
    /// - `cmd`: 底层 Redis 命令对象。
    pub async fn execute<T: FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.precheck(&cmd)?;
        let (otx, orx) = oneshot::channel();
        self.tx
            .send((cmd, otx))
            .await
            .map_err(|_| NasaRedisError::ExecutionUnknown("自动微批已停机,命令未提交".into()))?;
        match orx.await {
            Ok(Ok(v)) => Ok(T::from_redis_value(v)?),
            Ok(Err(TicketErr::Server(e))) => Err(NasaRedisError::Redis(e.into())),
            Ok(Err(TicketErr::Unknown(m))) => Err(NasaRedisError::ExecutionUnknown(m)),
            // AutoPipeline 不产生 NotSent(命令一旦 send 进队列即会被 flush);列此仅为穷尽匹配。
            Ok(Err(TicketErr::NotSent)) => Err(NasaRedisError::NotExecuted("命令未发送".into())),
            Err(_) => Err(NasaRedisError::ExecutionUnknown(
                "微批回执通道关闭(命令可能已执行)".into(),
            )),
        }
    }

    /// **fire-and-forget 提交**:入队后立即返回(不等结果),只 await send 做背压 + 保证后续 [`barrier`](Self::barrier)
    /// 的顺序。适合"提交一批写、再 barrier、再 direct 读"的热路径。命令仍会被 flush 执行;
    /// 只是放弃类型化结果(server error 不再回传给调用方,但同批其它命令不受影响)。
    ///
    /// # 参数
    ///
    /// - `cmd`: 需要进入自动微批队列的 Redis 命令，会先做准入和字节上限校验。
    pub async fn submit(&self, cmd: redis::Cmd) -> Result<()> {
        self.precheck(&cmd)?;
        let (otx, _orx) = oneshot::channel(); // 丢弃接收端 = 不等结果
        self.tx
            .send((cmd, otx))
            .await
            .map_err(|_| NasaRedisError::ExecutionUnknown("自动微批已停机,命令未提交".into()))?;
        Ok(())
    }

    /// **屏障**:等此前经 [`submit`](Self::submit)/[`execute`](Self::execute) **已入队**的命令
    /// 全部 flush 到 redis 并执行完——返回后 direct 读必能看到这些 batched 写(解决 direct/batched 混用的顺序)。
    ///
    /// 实现:入队一个 `PING` 并 await。同 lane 单 flusher FIFO 保证 PING 在所有 prior 命令**之后**被处理;
    /// PING 在专用 pipeline 连接上排在那些写之后,Redis 单线程顺序执行 → PING 应答时 prior 写已生效。
    pub async fn barrier(&self) -> Result<()> {
        self.execute::<redis::Value>(redis::cmd("PING"))
            .await
            .map(|_| ())
    }

    /// 命令准入 + 字节级背压预检:阻塞/PubSub/全局命令 fail-fast;单命令超
    /// `max_command_bytes` 拒绝入队。`submit`/`execute` 共用。
    ///
    /// # 参数
    /// - `cmd`: 底层 Redis 命令对象。
    fn precheck(&self, cmd: &redis::Cmd) -> Result<()> {
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

    /// 停机:停收新命令 + 后台 flush 完已入队的再退出。**幂等且对并发调用安全**
    /// 抢到 handle 的调用者 await drain 完成后广播信号;其余并发调用者**等同一 drain 完成信号**才返回,
    /// 不会先于后台 flush 退出就误判完成(避免上游过早释放资源)。
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let h = self.handle.lock().expect("autopipeline handle").take();
        match h {
            Some(h) => {
                let _ = h.await;
                let _ = self.drained.send(true); // 广播:drain 已完成
            }
            None => {
                // 别的调用者在 drain;等其完成信号(不抢先返回)
                let mut rx = self.drained.subscribe();
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break; // sender 已 drop(理论不可达:self 持有 sender)
                    }
                }
            }
        }
    }
}

// Runs the background pipeline flush loop.
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `rx`: 后台任务接收消息的通道。
/// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
/// - `cancel`: 后台任务使用的取消信号。
async fn flush_loop(
    client: Arc<RedisClient>,
    mut rx: mpsc::Receiver<BatchJob>,
    cfg: MicroBatchCfg,
    cancel: CancellationToken,
) {
    loop {
        // 等首命令(或停机)
        let first = tokio::select! {
            _ = cancel.cancelled() => break,
            j = rx.recv() => match j { Some(j) => j, None => break },
        };
        let mut batch_bytes = estimate_cmd_bytes(&first.0);
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
                    Ok(Some(j)) => { batch_bytes += estimate_cmd_bytes(&j.0); batch.push(j); }
                    _ => break, // 窗口到 / 通道关闭
                }
            }
        }
        flush_batch(&client, batch).await;
    }
    // 停机 drain:把 cancel **之前已入队**的命令一并发出(正常结果);cancel 之后到达的命令在 execute()
    // 侧会得 ExecutionUnknown(通道关闭后 send 失败/不被消费)——**没丢、没假成功**,但不保证发出。
    let mut rest = Vec::new();
    while let Ok(j) = rx.try_recv() {
        rest.push(j);
    }
    if !rest.is_empty() {
        flush_batch(&client, rest).await;
    }
}

// Sends one flushed batch to Redis and completes tickets.
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `batch`: 一次性提交到 Redis 的批量命令。
async fn flush_batch(client: &Arc<RedisClient>, batch: Vec<BatchJob>) {
    // AutoPipeline fire-and-forget:逐 ticket 结果已由 dispatch_jobs 回执,整体 Result 忽略。
    let _ = dispatch_jobs(client, batch).await;
}

/// **统一的 cluster 感知派发**(显式 `PipelineSession::execute` 与
/// AutoPipeline `flush_batch` **共用同一路径**——此前只有 AutoPipeline 分桶,显式会话 `save_all`
/// 跨 slot 写确定性失败)。standalone 走单条 pipeline(保序);cluster **按 key slot 分桶**:同桶
/// 同 slot 不 CROSSSLOT、一条 sub-pipeline 并行发,各 ticket 独立回执。返回 `Err` = 至少一个(桶的)
/// 传输/响应数异常 → ExecutionUnknown(per-ticket 结果仍精确;cluster 下"传输失败"细化到 slot 桶级)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `jobs`: 待放入 Redis pipeline 的任务集合。
async fn dispatch_jobs(client: &RedisClient, jobs: Vec<BatchJob>) -> Result<()> {
    if jobs.is_empty() {
        return Ok(());
    }
    if !client.is_cluster() {
        return send_one_pipe(client, jobs).await;
    }
    let mut buckets: std::collections::HashMap<u16, Vec<BatchJob>> =
        std::collections::HashMap::new();
    //keyless 命令(PING/SCRIPT LOAD 等)**单独成桶**,不与真正 hash 到 slot 0 的
    // key 命令混批——否则将来加 keyless 写会被错误绑到 slot-0 节点。keyless 桶整体发任意节点(cluster_async
    // 路由),与具体 slot 无关。
    let mut keyless: Vec<BatchJob> = Vec::new();
    for (cmd, tx) in jobs {
        match first_key_slot(&cmd) {
            Some(slot) => buckets.entry(slot).or_default().push((cmd, tx)),
            None => keyless.push((cmd, tx)),
        }
    }
    let mut futs: Vec<_> = buckets
        .into_values()
        .map(|b| send_one_pipe(client, b))
        .collect();
    if !keyless.is_empty() {
        futs.push(send_one_pipe(client, keyless));
    }
    let results = futures::future::join_all(futs).await;
    // 任一桶传输/响应异常 → 整体 Err(各桶 ticket 已各自收到精确结果)
    let mut first_err = None;
    for r in results {
        if let Err(e) = r {
            first_err.get_or_insert(e);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// 取命令第一个 key 的 slot 用于 cluster 分桶。本库所有类型化 helper 的 key 均在 arg[1]。
/// 无 key(keyless)→ None(归 keyless 桶)。
///
///**EVAL 系命令的 key 不在 arg[1]**(arg 布局 `NAME script/sha numkeys [key...]`,
/// arg[1] 是脚本体/sha)——raw `enqueue(EVAL ...)` 在 cluster 下若按 arg[1] 算 slot 会**误桶**(脚本字节
/// 当 key → 路由到错误节点 MOVED/CROSSSLOT)。这里识别 EVAL/EVALSHA/FCALL(及 `_RO` 变体),跳过
/// `numkeys` 取**真 key**;`numkeys==0` 的无键脚本 → None(keyless 桶,任意节点)。MSET 等多 key 命令的
/// arg[1] 本就是真 key(跨 slot 由 Redis 正确判 CROSSSLOT),无需特判。
///
/// # 参数
/// - `cmd`: 底层 Redis 命令对象。
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

/// 命令名是否为 EVAL 系(key 在 `numkeys` 之后,非 arg[1]):EVAL/EVALSHA/FCALL + `_RO` 变体。大小写无关。
///
/// # 参数
/// - `name`: 业务名称、字段名或配置名,用于定位目标对象。
fn is_eval_family(name: &[u8]) -> bool {
    let mut up = name.to_ascii_uppercase();
    // 去掉可选的 `_RO` 只读后缀,统一比对基名
    if up.ends_with(b"_RO") {
        up.truncate(up.len() - 3);
    }
    matches!(up.as_slice(), b"EVAL" | b"EVALSHA" | b"FCALL")
}

/// 把一批 job 组成单条 pipeline 发出,结果逐槽位回填各 ticket。返回 `Err(ExecutionUnknown)` =
/// 传输失败 / 响应数不符(该批 ticket 已统一收到 Unknown);`Ok` = 已逐条分发(含内联 ServerError)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `jobs`: 待放入 Redis pipeline 的任务集合。
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
