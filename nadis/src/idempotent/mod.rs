//! nonce 幂等计数：为 String、Hash、ZSet 计数提供 nonce 窗口幂等，一段 Lua 同时完成判重、计数与凭证登记。
//!
//! 幂等域是"目标 key + 结构类型 + field/member + nonce"；方向与 delta 不进入幂等域。窗口内重复 nonce
//! 只结算一次并返回首次结果，是保护资金类计数在网络重试下不重复扣减的核心不变量。账本身份是稳定
//! 线协议：首次写入与后续重放命中同一条凭证。

pub mod config;
pub mod metrics;
pub mod pipeline;
pub mod result;
pub(crate) mod runtime;
/// 账本身份线协议：摘要、ledger shard、canonical slot token 与 ledger key 派生的低层稳定入口，
/// 供互通身份校验与运维核对使用。
pub mod wire;

pub use config::{IdempotentCounterCfg, IdempotentTtlMode};
pub use metrics::{IdempotentCounterMetrics, IdempotentCounterSnapshot};
pub use pipeline::{IdempotentPipelineSession, IdempotentTicket};
pub use result::{IdempotentCounterError, IdempotentRejection};

use crate::client::RedisClient;
use crate::error::Result;

use runtime::{IdempotentCounterRuntime, Operation};

impl RedisClient {
    /// 业务作用：取得惰性构造的幂等计数运行时；首次访问才构造，不触发任何 Redis 往返。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：运行时引用；账本配置在启动期已校验，构造只计算脚本 SHA1，不做布局解析。
    fn idempotent_runtime(&self) -> Result<&IdempotentCounterRuntime> {
        if let Some(runtime) = self.idempotent.get() {
            return Ok(runtime);
        }
        // 布局解析（读 marker、能力探测）由运行时内部的 OnceCell 在首个 execute 时才执行；
        // 这里只做零 I/O 的构造，保证只用基础命令的应用不承担幂等计数成本。
        let snapshot = self
            .config()
            .idempotent_counter
            .clone()
            .unwrap_or_default()
            .snapshot(self.profile())?;
        let _ = self.idempotent.set(IdempotentCounterRuntime::new(snapshot));
        Ok(self.idempotent.get().expect("idempotent 运行时刚刚初始化"))
    }

    /// 业务作用：按业务 nonce 原子自增字符串计数器（+1）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`: 目标计数键。
    /// - `nonce`: 由稳定业务事件推导的幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值；空 nonce 或原生命令拒绝返回错误。
    pub async fn incr_idempotent(&self, key: &str, nonce: &str) -> Result<i64> {
        self.incr_by_idempotent(key, 1, nonce).await
    }

    /// 业务作用：按业务 nonce 原子自增字符串计数器（+delta）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`: 目标计数键。
    /// - `delta`: 增量。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值；越界或空 nonce 返回错误。
    pub async fn incr_by_idempotent(&self, key: &str, delta: i64, nonce: &str) -> Result<i64> {
        self.idempotent_runtime()?
            .execute_long(self, Operation::StringIncrement, key, b"", delta, nonce)
            .await
    }

    /// 业务作用：按业务 nonce 原子自减字符串计数器（-1）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`: 目标计数键。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值。
    pub async fn decr_idempotent(&self, key: &str, nonce: &str) -> Result<i64> {
        self.decr_by_idempotent(key, 1, nonce).await
    }

    /// 业务作用：按业务 nonce 原子自减字符串计数器（-delta）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`: 目标计数键。
    /// - `delta`: 减量。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值。
    pub async fn decr_by_idempotent(&self, key: &str, delta: i64, nonce: &str) -> Result<i64> {
        self.idempotent_runtime()?
            .execute_long(self, Operation::StringDecrement, key, b"", delta, nonce)
            .await
    }

    /// 业务作用：按业务 nonce 原子自增 HASH 字段（+1）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`/`field`: 目标 HASH 键与字段。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值。
    pub async fn h_incr_idempotent(&self, key: &str, field: &str, nonce: &str) -> Result<i64> {
        self.h_incr_by_idempotent(key, field, 1, nonce).await
    }

    /// 业务作用：按业务 nonce 原子自增 HASH 字段（+delta）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`/`field`/`delta`: 目标 HASH 键、字段与增量。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值。
    pub async fn h_incr_by_idempotent(
        &self,
        key: &str,
        field: &str,
        delta: i64,
        nonce: &str,
    ) -> Result<i64> {
        self.idempotent_runtime()?
            .execute_long(
                self,
                Operation::HashIncrement,
                key,
                field.as_bytes(),
                delta,
                nonce,
            )
            .await
    }

    /// 业务作用：按业务 nonce 原子自减 HASH 字段（-1）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`/`field`: 目标 HASH 键与字段。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值。
    pub async fn h_decr_idempotent(&self, key: &str, field: &str, nonce: &str) -> Result<i64> {
        self.h_decr_by_idempotent(key, field, 1, nonce).await
    }

    /// 业务作用：按业务 nonce 原子自减 HASH 字段（-delta）；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`/`field`/`delta`: 目标 HASH 键、字段与减量。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值；`i64::MIN` 减量越界返回错误。
    pub async fn h_decr_by_idempotent(
        &self,
        key: &str,
        field: &str,
        delta: i64,
        nonce: &str,
    ) -> Result<i64> {
        self.idempotent_runtime()?
            .execute_long(
                self,
                Operation::HashDecrement,
                key,
                field.as_bytes(),
                delta,
                nonce,
            )
            .await
    }

    /// 业务作用：按业务 nonce 原子对 ZSet member 增分；窗口内重放只返回首次结果。
    ///
    /// 参数说明：
    /// - `key`/`member`: 目标 ZSet 键与成员；`member` 的命令字节必须与普通 `z_incr_by` 一致，
    ///   与既有账本共享凭证时业务需保证同一编码字节。
    /// - `delta`: 浮点增量。
    /// - `nonce`: 幂等标识，不能为空。
    ///
    /// 返回：首次请求执行后的精确 double（含 ±inf）；重复 nonce 返回首次值；NaN 由原生命令拒绝。
    pub async fn z_incr_by_idempotent<V>(
        &self,
        key: &str,
        member: V,
        delta: f64,
        nonce: &str,
    ) -> Result<f64>
    where
        V: redis::ToSingleRedisArg + Send + Sync,
    {
        let mut args = redis::ToRedisArgs::to_redis_args(&member);
        let member = args.pop().ok_or_else(|| {
            crate::error::NasaRedisError::IdempotentProtocol(
                "ZSet member 未生成单个 Redis 参数".to_owned(),
            )
        })?;
        if !args.is_empty() {
            return Err(crate::error::NasaRedisError::IdempotentProtocol(
                "ZSet member 生成了多个 Redis 参数".to_owned(),
            ));
        }
        self.idempotent_runtime()?
            .execute_double(self, key, &member, delta, nonce)
            .await
    }

    /// 业务作用：读取幂等计数观测事实，供健康端点与指标桥接；`ttl_missing` 出现后 `degraded` 保持置位。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：布局已构造时返回当前快照；从未使用幂等方法时返回 `None`，表示零成本未初始化。
    pub fn idempotent_counter_snapshot(&self) -> Option<IdempotentCounterSnapshot> {
        self.idempotent
            .get()
            .map(|runtime| runtime.metrics().snapshot())
    }

    /// 业务作用：在接流量前主动固定幂等计数账本布局：读或建共享 marker，并对 `HASH_FIELD` 完成 all-master 能力探测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：布局解析并复验成功返回；marker 与本地配置冲突、共享 `HASH_FIELD` 但所连 master 缺 HPEXPIRE、或探测不完整时
    /// 返回错误并 fail-closed。受管场景应在发布 Ready 前调用；独立使用者希望在接流量前证明布局时也调用。
    pub async fn prepare_idempotent_counter(&self) -> Result<()> {
        self.idempotent_runtime()?.prepare(self).await
    }
}
