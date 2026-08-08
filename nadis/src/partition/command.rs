// ============================================================================
// src/partition/command.rs —— 管理命令 outbox / result key。
// 采用定向 outbox 与 per-operation result key。
//
// 解决的问题:
//   · 管理请求(resume/drop/force)可能落到任意节点,而处置必须由当前 partition owner
//     执行(fenced)——非 owner 节点把命令写进【定向】command stream,owner 消费执行;
//   · Parked 分区禁业务 `>` poll,若管理命令复用业务 poll 则 Resume 永远读不到(自锁,
//)→ control poll 独立于业务 Ready/Parked 状态;
//   · outbox 异步:调用方只能靠【持久化 result】查询结果——deadline/冲突/重复提交都要有
//     稳定可查的终态(park marker 的单字段承载不了多宗先后失败的 operation)。
//
// ┌─ Key 布局 ──────────────────────────────────────────────────────────────┐
// │ command stream : {prefix}:park-cmd:{p}              (定向:每分区一条——     │
// │:共享 stream 会被 consumer group 随机路由到错误   │
// │                  owner 且永久卡死;admin group 固定 "nasa-park-admin")       │
// │ result key     : {prefix}:park-cmd-result:{p}:{operation_id}(JSON string)   │
// └────────────────────────────────────────────────────────────────────────┘
//
// 协议顺序
//   ①producer:SET NX 建 Pending result(含 request digest;同 op_id 不同 digest →
//     OperationIdConflict)→ XADD command(**at-least-once**:Unknown 后可重 XADD,
//     owner 以 result state 去重——稳定终态下的重复命令只 ACK/XDEL 不再执行副作用);
//   ②owner:读命令 → result 置 Executing → 校验 deadline(**Redis TIME 基准**,
//     producer 传 timeout_ms 非绝对时刻,防节点时钟漂移)→ 执行 disposition 动作
//     (resume/drop/force 自身幂等/单向,Executing 中断重入直接重做);
//   ③**先写稳定 result 再 owner-fenced XACK+XDEL**(result 终态是"副作用已完成"的凭据:
//     ACK Unknown 时新 owner 只补清理,不重复执行);
//   ④deadline 过期/非法动作同样先写 Rejected result 再 ACK(不静默丢弃)。
//
// owner 接管旧 PEL 使用 XAUTOCLAIM(Partition 持锁语义下 admin group
// 同样单 owner,无 owner 混淆——XAUTOCLAIM 在 Partition 协议内是允许的);result TTL
// 终态后 24h(应长于客户端查询窗口)。
// ============================================================================

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::KeyLayout;
use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};

/// 管理命令的 admin consumer group(全节点共用;consumer = 当前 owner 的 node_id)。
pub const ADMIN_GROUP: &str = "nasa-park-admin";

/// 业务作用：command stream key(定向:每分区一条)。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
pub fn cmd_stream_key(layout: &KeyLayout, p: u32) -> String {
    format!("{}:park-cmd:{}", layout.prefix, p)
}

/// 业务作用：result key(per-operation 持久结果)。
///
/// # 参数
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `operation_id`: 管理命令的幂等操作 ID。
pub fn result_key(layout: &KeyLayout, p: u32, operation_id: &str) -> String {
    format!("{}:park-cmd-result:{}:{}", layout.prefix, p, operation_id)
}

/// 管理动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmdAction {
    /// 恢复 parked 消息到目标 stream。
    Resume,
    /// 丢弃 parked 消息。
    Drop,
    /// 强制重发或推进处置,由调用方显式承担重复风险。
    Force,
    /// 把 Parked 消息发布到 DLQ stream(planned-ID 协议)。
    Dlq,
}

/// 持久化 result(JSON 存于 result key)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CmdResult {
    /// 当前命令执行状态。
    pub state: CmdState,
    /// 当前命令对应的管理动作。
    pub action: CmdAction,
    /// 请求摘要(同 op_id 重复提交的判重依据;crc16(action|p) 足够——op_id 本身已唯一,
    /// digest 只防"同 op_id 配不同动作"的误用)。
    pub digest: String,
    /// 创建时刻(Redis TIME **毫秒**;deadline 判定基准)。
    pub created_at: u64,
    /// 超时窗口 ms(producer 传相对值,owner 以 Redis TIME 判过期)。
    pub timeout_ms: u64,
    /// 终态信息:Succeeded 的 new_ids / Rejected 的原因。
    #[serde(default)]
    pub detail: String,
    /// owner 执行**前**捕获的 disposition marker version（0 表示无 marker 或未捕获）。
    #[serde(default)]
    pub marker_version_before: u64,
    /// 执行**后**的 disposition marker version。`after>before` 证明本 op 确实
    /// 推进了状态机(claim_transition 成功);`after==before` = 命中幂等/重入未推进 —— 用于
    /// 事后区分"真执行"与"去重命中",定位重复提交/丢响应导致的可疑重放。
    #[serde(default)]
    pub marker_version_after: u64,
}

/// 管理命令持久化结果状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmdState {
    /// 命令已提交但尚未被 owner 执行。
    Pending,
    /// owner 正在执行命令。
    Executing,
    /// 命令已成功完成。
    Succeeded,
    /// 命令被拒绝,通常因为租约、状态或幂等摘要不匹配。
    Rejected,
}

/// 业务作用：取 Redis 服务端时间(**毫秒**)——deadline 的唯一时钟基准。
/// TIME 返回 [秒, 微秒];只取秒位会让同一秒内 elapsed=0,timeout_ms=0/小值的过期判定
/// 永不触发(实测踩中)→ 毫秒 = 秒*1000 + 微秒/1000。
pub(crate) async fn redis_now(client: &Arc<RedisClient>) -> Result<u64> {
    let v: Vec<String> = redis::cmd("TIME").query_async(&mut client.conn()).await?;
    //**fail-closed**——TIME 响应损坏不得静默归零(now=0 会让
    // `now-created_at=0` 永不超过 timeout_ms、Pending deadline 永不触发、心跳异常)。
    // 与本模块其它 fail-closed 纪律一致:损坏即上抛 ProtocolMarker。
    let sec: u64 = v
        .first()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| NasaRedisError::ProtocolMarker("TIME 响应缺/坏秒位".into()))?;
    let usec: u64 = v
        .get(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| NasaRedisError::ProtocolMarker("TIME 响应缺/坏微秒位".into()))?;
    Ok(sec * 1000 + usec / 1000)
}

/// 业务作用：请求摘要(同 op_id 重复提交的判重依据)。**必须纳入 `timeout_ms`**——
/// 否则同 op_id+action 不同 timeout 的重试 digest 相同,会静默返回旧 result(含已死的 Rejected/
/// DeadlineExceeded),新 timeout 被吞、该 op_id 永久拿回死结果。纳入后:同 timeout → 幂等返回现状;
/// 不同 timeout → `OperationIdConflict`(暴露给调用方,强制换 op_id 或识别冲突)。
/// 摘要采用足够宽的碰撞域，并与 operation_id 一起约束重复请求的业务语义。
///
/// # 参数
/// - `action`: 分区控制命令对应的动作。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `timeout_ms`: 超时时间毫秒数。
fn digest_of(action: CmdAction, p: u32, timeout_ms: u64) -> String {
    format!(
        "{:04x}",
        crate::keytag::crc16(format!("{action:?}|{p}|{timeout_ms}").as_bytes())
    )
}

/// 业务作用：提交管理命令(任意节点可调)。幂等:同 op_id 同 digest 重复提交 = 返回现有 result;
/// 同 op_id 不同 digest = OperationIdConflict。
///
/// # 参数
/// - `client`: 写入 command stream 和 result key 的 Redis 客户端。
/// - `layout`: 目标分区组 key 布局。
/// - `p`: 目标分区编号。
/// - `operation_id`: 调用方提供的幂等操作 ID。
/// - `action`: 要提交给 owner 执行的管理动作。
/// - `timeout_ms`: 从 Redis TIME 起算的命令接纳超时窗口。
pub async fn submit(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    operation_id: &str,
    action: CmdAction,
    timeout_ms: u64,
) -> Result<CmdResult> {
    let rkey = result_key(layout, p, operation_id);
    let digest = digest_of(action, p, timeout_ms);
    let now = redis_now(client).await?;
    let fresh = CmdResult {
        state: CmdState::Pending,
        action,
        digest: digest.clone(),
        created_at: now,
        timeout_ms,
        detail: String::new(),
        marker_version_before: 0,
        marker_version_after: 0,
    };
    let payload =
        serde_json::to_string(&fresh).map_err(|e| NasaRedisError::Codec(e.to_string()))?;

    // ① SET NX 建 Pending(先 result 后 XADD:result 是去重与查询的事实源)
    let created: Option<String> = redis::cmd("SET")
        .arg(&rkey)
        .arg(&payload)
        .arg("NX")
        .query_async(&mut client.conn())
        .await?;
    if created.is_none() {
        // 已存在:digest 校验(同 op 重复提交 = 幂等返回现状;不同 = 冲突)
        let existing: CmdResult = query(client, layout, p, operation_id)
            .await?
            .ok_or_else(|| NasaRedisError::Config("result 消失(竞态)".into()))?;
        if existing.digest != digest {
            return Err(NasaRedisError::Config(format!(
                "OperationIdConflict: op={operation_id} 已绑定 {:?},本次 {action:?}",
                existing.action
            )));
        }
        //**终态(Succeeded/Rejected)跳过重 XADD**——result 已是确定答复,producer
        // 重复 submit 不再投 command entry(否则 owner 每次 Finalize 清理是纯资源浪费,autoTrim 未实现
        // 下 stream 累积)。仅**非终态**(Pending/Executing,命令可能没送达)才补投 entry(at-least-once)。
        if !matches!(existing.state, CmdState::Succeeded | CmdState::Rejected) {
            let _: String = redis::cmd("XADD")
                .arg(cmd_stream_key(layout, p))
                .arg("*")
                .arg("op")
                .arg(operation_id)
                .query_async(&mut client.conn())
                .await?;
        }
        return Ok(existing);
    }

    // ② XADD command(entry 只携带 op_id;参数全在 result——entry 丢失可重发)
    let _: String = redis::cmd("XADD")
        .arg(cmd_stream_key(layout, p))
        .arg("*")
        .arg("op")
        .arg(operation_id)
        .query_async(&mut client.conn())
        .await?;
    Ok(fresh)
}

/// 业务作用：查询命令结果(producer 轮询入口)。
///
/// # 参数
/// - `client`: 读取 result key 的 Redis 客户端。
/// - `layout`: 目标分区组 key 布局。
/// - `p`: 目标分区编号。
/// - `operation_id`: 要查询的幂等操作 ID。
pub async fn query(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    operation_id: &str,
) -> Result<Option<CmdResult>> {
    let raw: Option<String> = client.get(&result_key(layout, p, operation_id)).await?;
    match raw {
        None => Ok(None),
        Some(s) => Ok(Some(serde_json::from_str(&s).map_err(|e| {
            NasaRedisError::Codec(format!("result 解析失败: {e}"))
        })?)),
    }
}

/// 业务作用：owner 端写 result(覆盖写;单 owner 无并发,Executing→终态单向由调用方保证)。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `layout`: 分区 key 布局,用于生成 stream、marker 和 lock key。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `operation_id`: 业务标识,用于定位具体对象或记录。
/// - `r`: Redis 返回值或解析结果。
async fn put_result(
    client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    operation_id: &str,
    r: &CmdResult,
) -> Result<()> {
    let rkey = result_key(layout, p, operation_id);
    let payload = serde_json::to_string(r).map_err(|e| NasaRedisError::Codec(e.to_string()))?;
    if matches!(r.state, CmdState::Succeeded | CmdState::Rejected) {
        //终态写 + TTL 必须**原子**(`SET ... EX`)——旧实现 SET 与
        // EXPIRE 分离,崩在中间会永久保留 result。终态保留 24h(长于 producer 查询窗口)。
        let _: redis::Value = redis::cmd("SET")
            .arg(&rkey)
            .arg(payload.as_str())
            .arg("EX")
            .arg(24 * 3600)
            .query_async(&mut client.conn())
            .await?;
    } else {
        // 非终态(Pending/Executing):无 TTL 覆盖写
        client.set(&rkey, payload.as_str()).await?;
    }
    Ok(())
}

/// 命令处置决策(基础设施错误后不得 ACK+XDEL,否则丢失 at-least-once
/// 重试能力,且把基础设施故障与业务拒绝混成同一 Rejected 终态)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmdAck {
    /// 确定终态(Succeeded/Rejected/deadline/重复/producer 违规):调用方 ACK+XDEL command。
    Finalize,
    /// 基础设施错误(Redis/网络):result 不推进终态,**保留 command PEL**,
    /// 下一轮或新 owner 接管重试(动作自身幂等/单向,重做安全)。
    Retry,
}

/// 业务作用：owner 执行一条命令(control loop 调用)。返回 `Finalize` 才允许 XACK+XDEL。
///
/// # 参数
/// - `rt_client`: owner runtime 使用的 Redis 客户端。
/// - `layout`: 当前分区组 key 布局。
/// - `p`: 当前分区编号。
/// - `operation_id`: command entry 指向的幂等操作 ID。
/// - `do_action`: 真正执行 disposition 副作用的 future。
/// - `action_hint`: command stream 中携带或上层判定出的动作类型。
pub async fn execute_one(
    rt_client: &Arc<RedisClient>,
    layout: &KeyLayout,
    p: u32,
    operation_id: &str,
    do_action: impl std::future::Future<Output = Result<Vec<String>>>,
    action_hint: CmdAction,
) -> Result<CmdAck> {
    // ① 读 result:缺失 = producer 违规(协议要求先建 result)→ 清理孤儿 command
    //    (无 result 的命令永远无法执行,留 PEL 无意义)
    let Some(mut r) = query(rt_client, layout, p, operation_id).await? else {
        tracing::warn!(
            p,
            operation_id,
            "命令无 result 记录(producer 违规),清理孤儿 command"
        );
        return Ok(CmdAck::Finalize);
    };
    // ② 去重:已终态 → 只清理,不重复副作用(at-least-once 的 owner 端职责)
    if matches!(r.state, CmdState::Succeeded | CmdState::Rejected) {
        return Ok(CmdAck::Finalize);
    }
    // ③ deadline(Redis TIME 基准):**只对 Pending(尚未开始)生效**。
    //    timeout_ms 是 producer 的"接纳/等待窗口",不是执行中止权:`Executing` 说明动作可能
    //    已产生副作用,过期就把它 Rejected+Finalize 会让 command 被 ACK/XDEL,而 disposition
    //    永久留在 *Publishing 中间态。Executing 重入只能继续对账收敛,不再按 admission deadline 拒绝。
    if matches!(r.state, CmdState::Pending) {
        let now = redis_now(rt_client).await?;
        // created_at/now 均为 Redis TIME 毫秒:elapsed 直接与 timeout_ms 比
        if now.saturating_sub(r.created_at) > r.timeout_ms {
            r.state = CmdState::Rejected;
            r.detail = "DeadlineExceeded".into();
            put_result(rt_client, layout, p, operation_id, &r).await?;
            tracing::warn!(p, operation_id, "管理命令(Pending)超时,已写 Rejected");
            return Ok(CmdAck::Finalize);
        }
    }
    // ④ Executing(中断重入:disposition 动作自身幂等/单向,直接重做)。
    // 执行前捕获 disposition marker version 作为审计基线；无 marker 时记为 0。
    r.marker_version_before = super::disposition::marker_version(rt_client, layout, p)
        .await
        .unwrap_or(0);
    r.state = CmdState::Executing;
    put_result(rt_client, layout, p, operation_id, &r).await?;
    // ⑤ 执行动作 → ⑥ 按错误性质分流:
    //    · 成功 / 确定性业务拒绝 → 写稳定 result → Finalize(ACK)
    //    · **基础设施错误(Redis/网络)→ 不写 Rejected,保留 Executing → Retry(留 PEL)**
    //caller 传的 action_hint 应与持久化 result 的 action 一致;
    // debug 下交叉校验(release 零开销),命中即 producer/owner 路由错配的早期信号。
    debug_assert_eq!(
        action_hint, r.action,
        "action_hint 与 result.action 不一致(路由错配?)"
    );
    match do_action.await {
        Ok(ids) => {
            r.state = CmdState::Succeeded;
            r.detail = ids.join(",");
            // 执行后捕获 marker version:after>before ⇒ 本 op 真正推进了状态机
            r.marker_version_after = super::disposition::marker_version(rt_client, layout, p)
                .await
                .unwrap_or(r.marker_version_before);
            put_result(rt_client, layout, p, operation_id, &r).await?;
            Ok(CmdAck::Finalize)
        }
        Err(NasaRedisError::OperationConflict(msg)) => {
            //**OperationConflict 是 pre-side-effect 拒绝**(assert_op_owns 在动作
            // 副作用前就挡下,异 op B 撞原 op A 在途 `*Publishing`)——与会自愈的网络抖动不同,重试 B
            // **永不成功**(只有原 op A 同 op_id 重提、或 liveness takeover 能续作 A 的转换)。若仍当
            // 普通 infra 错误无限留 PEL:control loop 每 500ms 空转重试 + result 永驻 Executing(deadline
            // 只对 Pending)+ producer 轮询永远 Executing(timeout 失效)。
            // 修法:**按 producer 窗口 bound**——窗口内留 PEL(给原 op A 同 op_id 重提的机会);超窗 →
            // 因无副作用,安全写 `Rejected` 终态 + Finalize,停忙等、给 producer 确定答复(需 takeover)。
            // (分区若因 A 丢失仍冻在 `*Publishing`,属于 liveness-takeover 的独立增强;本逻辑只止住新引入的
            //  忙等/Executing 永驻。)
            let now = redis_now(rt_client).await?;
            if now.saturating_sub(r.created_at) > r.timeout_ms {
                r.state = CmdState::Rejected;
                r.detail = format!("OperationConflict(超 producer 窗口,需异 op takeover): {msg}");
                //**after=before**——本 op 是 pre-side-effect 拒绝、未推进状态机,
                // 不可重读 marker version(否则会捕获并发 op A 的推进使 after>before,污染审计
                // "after>before=本 op 真执行"语义,把"本 op 没做事"误标成"本 op 推进了状态")。
                r.marker_version_after = r.marker_version_before;
                put_result(rt_client, layout, p, operation_id, &r).await?;
                tracing::warn!(
                    p,
                    operation_id,
                    "operation 冲突超 producer 窗口,写 Rejected 终态停忙等"
                );
                Ok(CmdAck::Finalize)
            } else {
                tracing::warn!(
                    p,
                    operation_id,
                    "operation 冲突(窗口内),保留 PEL 候原 op 重提"
                );
                Ok(CmdAck::Retry)
            }
        }
        Err(e) if e.is_infra_retryable() => {
            // 基础设施故障(网络/Redis 瞬时):会自愈,result 留 Executing(已落盘),保留 command PEL 下轮重试
            tracing::warn!(p, operation_id, err = %e, "管理动作基础设施错误,保留 PEL 重试(不终结)");
            Ok(CmdAck::Retry)
        }
        Err(e) => {
            r.state = CmdState::Rejected;
            r.detail = e.to_string();
            r.marker_version_after = super::disposition::marker_version(rt_client, layout, p)
                .await
                .unwrap_or(r.marker_version_before);
            put_result(rt_client, layout, p, operation_id, &r).await?;
            Ok(CmdAck::Finalize)
        }
    }
}
