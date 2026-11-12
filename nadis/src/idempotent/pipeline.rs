//! nonce 幂等计数的显式 Pipeline：业务只提供命令参数与 nonce，每条入队项仍是一段完整原子 Lua。

use std::sync::Arc;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::idempotent::metrics::IdempotentCounterMetrics;
use crate::idempotent::runtime::{interpret_result, Operation};
use crate::pipeline::{PipelineSession, Ticket};

/// 幂等 Pipeline 的类型化回执；提交后解析三元素脚本协议并返回首次结算值。
pub struct IdempotentTicket<T> {
    /// 底层批量命令回执，读取前须完成会话提交。
    inner: Ticket<Vec<redis::Value>>,
    /// 归属同一账本运行时的指标，按解析出的脚本结局记录。
    metrics: Arc<IdempotentCounterMetrics>,
    /// 限定回执的整数或浮点结果类型，不额外保存业务值。
    _value: std::marker::PhantomData<T>,
}

impl IdempotentTicket<i64> {
    /// 业务作用：读取整数幂等命令的首次结算结果；重复 nonce 与首次执行返回相同值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：精确 int64；会话未提交、传输不确定、结构化拒绝或结果越界时返回错误。
    pub fn await_result(self) -> Result<i64> {
        let raw = self.inner.await_result()?;
        interpret_result(&self.metrics, &raw)?
            .parse::<i64>()
            .map_err(|_| protocol("idempotent pipeline integer result is not int64"))
    }
}

impl IdempotentTicket<f64> {
    /// 业务作用：读取 ZSet 幂等命令的首次结算结果，保留 Redis 的正负无穷语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次 double；会话未提交、传输不确定、结构化拒绝或非数值结果返回错误。
    pub fn await_result(self) -> Result<f64> {
        let raw = self.inner.await_result()?;
        let value = interpret_result(&self.metrics, &raw)?;
        match value.to_ascii_lowercase().as_str() {
            "inf" | "+inf" => Ok(f64::INFINITY),
            "-inf" => Ok(f64::NEG_INFINITY),
            _ => value
                .parse::<f64>()
                .map_err(|_| protocol("idempotent pipeline ZSet result is not numeric")),
        }
    }
}

/// 已完成布局准备的幂等 Pipeline；普通 Pipeline 与本类型显式分离，避免写命令悄然改变语义。
pub struct IdempotentPipelineSession {
    /// 固定连接及已经准备的账本布局，供全部入队项共享。
    client: Arc<RedisClient>,
    /// 仅由显式提交发送的命令队列，不因容量达到上限而自动发送。
    inner: PipelineSession,
}

impl RedisClient {
    /// 业务作用：创建只接受 nonce 幂等计数 helper 的显式 Pipeline，并在入队前固定共享账本布局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：marker 与 all-master 能力门禁通过时返回空会话；布局冲突或探测不满足时返回错误且不接收命令。
    pub async fn idempotent_pipeline(self: &Arc<Self>) -> Result<IdempotentPipelineSession> {
        self.prepare_idempotent_counter().await?;
        Ok(IdempotentPipelineSession {
            client: Arc::clone(self),
            // nonce 会话必须把 `execute()` 保持为唯一提交点；达到普通 Pipeline 上限时整会话仍确定未发送。
            inner: self.deferred_pipeline(),
        })
    }
}

impl IdempotentPipelineSession {
    /// 业务作用：入队字符串计数器的 nonce 幂等自增一。
    ///
    /// 参数说明：
    /// - `key`：目标字符串计数键，参与账本身份和槽位派生。
    /// - `nonce`：稳定业务幂等标识，不能为空或全空白。
    ///
    /// 返回：首次结算值的类型化 ticket；空 nonce 或派生失败时不入队并返回错误。
    pub fn incr_idempotent(&mut self, key: &str, nonce: &str) -> Result<IdempotentTicket<i64>> {
        self.incr_by_idempotent(key, 1, nonce)
    }

    /// 业务作用：入队字符串计数器的 nonce 幂等自增。
    ///
    /// 参数说明：
    /// - `key`：目标字符串计数键，参与账本身份和槽位派生。
    /// - `delta`：首次结算使用的有符号增量，负数表示减少。
    /// - `nonce`：稳定业务幂等标识；重复 nonce 不替换窗口内已结算的增量。
    ///
    /// 返回：成功入队的整数回执，尚未发送；nonce、账本派生或队列容量不满足约束时返回错误。
    pub fn incr_by_idempotent(
        &mut self,
        key: &str,
        delta: i64,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        self.enqueue_integer(Operation::StringIncrement, key, b"", delta, nonce)
    }

    /// 业务作用：入队字符串计数器的 nonce 幂等自减一。
    ///
    /// 参数说明：
    /// - `key`：目标字符串计数键，参与账本身份和槽位派生。
    /// - `nonce`：稳定业务幂等标识，不能为空或全空白。
    ///
    /// 返回：首次结算值的类型化 ticket；空 nonce 或派生失败时不入队并返回错误。
    pub fn decr_idempotent(&mut self, key: &str, nonce: &str) -> Result<IdempotentTicket<i64>> {
        self.decr_by_idempotent(key, 1, nonce)
    }

    /// 业务作用：入队字符串计数器的 nonce 幂等自减。
    ///
    /// 参数说明：
    /// - `key`：目标字符串计数键，参与账本身份和槽位派生。
    /// - `delta`：首次结算需要减去的有符号步长，负数表示增加。
    /// - `nonce`：稳定业务幂等标识；重复 nonce 不替换窗口内已结算的步长。
    ///
    /// 返回：成功入队的整数回执，尚未发送；nonce、账本派生或队列容量不满足约束时返回错误。
    pub fn decr_by_idempotent(
        &mut self,
        key: &str,
        delta: i64,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        self.enqueue_integer(Operation::StringDecrement, key, b"", delta, nonce)
    }

    /// 业务作用：入队 Hash 字段的 nonce 幂等自增一。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，参与账本键和槽位派生。
    /// - `field`：计数字段，其原始字节参与幂等身份。
    /// - `nonce`：稳定业务幂等标识，不能为空或全空白。
    ///
    /// 返回：首次结算值的类型化 ticket；参数不满足账本合同时返回错误。
    pub fn h_incr_idempotent(
        &mut self,
        key: &str,
        field: &str,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        self.h_incr_by_idempotent(key, field, 1, nonce)
    }

    /// 业务作用：入队 Hash 字段的 nonce 幂等自增。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，参与账本键和槽位派生。
    /// - `field`：计数字段，其原始字节参与幂等身份。
    /// - `delta`：首次结算使用的有符号增量，负数表示减少。
    /// - `nonce`：稳定业务幂等标识；重复 nonce 返回窗口内首次结算的结果。
    ///
    /// 返回：首次结算值的类型化 ticket；参数不满足账本合同时返回错误。
    pub fn h_incr_by_idempotent(
        &mut self,
        key: &str,
        field: &str,
        delta: i64,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        self.enqueue_integer(
            Operation::HashIncrement,
            key,
            field.as_bytes(),
            delta,
            nonce,
        )
    }

    /// 业务作用：入队 Hash 字段的 nonce 幂等自减一。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，参与账本键和槽位派生。
    /// - `field`：计数字段，其原始字节参与幂等身份。
    /// - `nonce`：稳定业务幂等标识，不能为空或全空白。
    ///
    /// 返回：首次结算值的类型化 ticket；参数不满足账本合同时返回错误。
    pub fn h_decr_idempotent(
        &mut self,
        key: &str,
        field: &str,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        self.h_decr_by_idempotent(key, field, 1, nonce)
    }

    /// 业务作用：入队 Hash 字段的 nonce 幂等自减。
    ///
    /// 参数说明：
    /// - `key`：目标 Hash 键，参与账本键和槽位派生。
    /// - `field`：计数字段，其原始字节参与幂等身份。
    /// - `delta`：首次结算需要减去的有符号步长，负数表示增加。
    /// - `nonce`：稳定业务幂等标识；重复 nonce 返回窗口内首次结算的结果。
    ///
    /// 返回：首次结算值的类型化 ticket；参数不满足账本合同时返回错误。
    pub fn h_decr_by_idempotent(
        &mut self,
        key: &str,
        field: &str,
        delta: i64,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        self.enqueue_integer(
            Operation::HashDecrement,
            key,
            field.as_bytes(),
            delta,
            nonce,
        )
    }

    /// 业务作用：入队 ZSet member 的 nonce 幂等增分，摘要与命令使用完全相同的单参数字节。
    ///
    /// 参数说明：
    /// - `key`：目标有序集合键，参与账本键和槽位派生。
    /// - `member`：成员的单参数编码，必须与其它共享该账本的调用保持字节一致。
    /// - `delta`：首次结算使用的浮点分数增量。
    /// - `nonce`：稳定业务幂等标识，不能为空或全空白。
    ///
    /// 返回：首次 score 的类型化 ticket；member 非单参数、空 nonce 或派生失败时返回错误。
    pub fn z_incr_by_idempotent<V>(
        &mut self,
        key: &str,
        member: V,
        delta: f64,
        nonce: &str,
    ) -> Result<IdempotentTicket<f64>>
    where
        V: redis::ToSingleRedisArg,
    {
        let member = single_arg(member)?;
        let command = self.client.idempotent_runtime()?.pipeline_command(
            Operation::ZSetIncrement,
            key,
            &member,
            delta.to_string().into_bytes(),
            nonce,
        )?;
        let inner = self.inner.enqueue(command)?;
        Ok(IdempotentTicket {
            inner,
            metrics: self.client.idempotent_runtime()?.metrics().clone(),
            _value: std::marker::PhantomData,
        })
    }

    /// 业务作用：提交全部幂等 Lua 调用并等待每个 Cluster slot 的批级传输结局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部批级传输确认成功时返回；任一分桶结果未知时返回错误，各命令业务结局仍从 ticket 读取。
    pub async fn execute(self) -> Result<()> {
        self.inner.execute().await
    }

    /// 业务作用：构造并入队一个整数幂等 Lua 调用，统一四个字符串/Hash helper 的协议路径。
    ///
    /// 参数说明：
    /// - `op`：整数计数的结构类型与增减方向，方向不进入判重身份。
    /// - `key`：目标计数键，决定计数与账本共同使用的槽位。
    /// - `member`：Hash 字段的原始字节；字符串计数传入空字节。
    /// - `delta`：按操作方向解释的有符号整数步长。
    /// - `nonce`：稳定业务幂等标识，窗口内重复请求复用首次结果。
    ///
    /// 返回：首次 int64 的 ticket；构造或入队失败时返回错误。
    fn enqueue_integer(
        &mut self,
        op: Operation,
        key: &str,
        member: &[u8],
        delta: i64,
        nonce: &str,
    ) -> Result<IdempotentTicket<i64>> {
        let runtime = self.client.idempotent_runtime()?;
        let command =
            runtime.pipeline_command(op, key, member, delta.to_string().into_bytes(), nonce)?;
        let inner = self.inner.enqueue(command)?;
        Ok(IdempotentTicket {
            inner,
            metrics: runtime.metrics().clone(),
            _value: std::marker::PhantomData,
        })
    }
}

/// 业务作用：把 `ToSingleRedisArg` 转成账本与原生命令共用的唯一二进制参数。
///
/// 参数说明：
/// - `value`：待编码的有序集合成员，编码字节同时参与命令与账本身份。
///
/// 返回：恰好一个参数时返回字节；零个或多个参数返回协议错误。
fn single_arg<V: redis::ToSingleRedisArg>(value: V) -> Result<Vec<u8>> {
    let mut args = redis::ToRedisArgs::to_redis_args(&value);
    let arg = args
        .pop()
        .ok_or_else(|| protocol("ZSet member did not produce a Redis argument"))?;
    if !args.is_empty() {
        return Err(protocol("ZSet member produced multiple Redis arguments"));
    }
    Ok(arg)
}

/// 业务作用：构造幂等 Pipeline 协议错误。
///
/// 参数说明：
/// - `message`: 不含业务值的稳定错误摘要。
///
/// 返回：`IdempotentProtocol` 错误。
fn protocol(message: &str) -> NasaRedisError {
    NasaRedisError::IdempotentProtocol(message.to_owned())
}
