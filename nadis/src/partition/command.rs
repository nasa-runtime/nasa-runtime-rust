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
// │ 结果键：{prefix}:park-cmd-result:{p}:{operation_id}，内容为 JSON。          │
// └────────────────────────────────────────────────────────────────────────┘
//
// 协议顺序
//   ①producer:SET NX 建 Pending result(含 request digest;同 op_id 不同 digest →
//     OperationIdConflict)→ XADD command(**at-least-once**:Unknown 后可重 XADD,
//     owner 以 result state 去重——稳定终态下的重复命令只 ACK/XDEL 不再执行副作用);
//   ②owner:读命令 → 校验 Pending 的 deadline(**Redis TIME 基准**,
//     producer 传 timeout_ms 非绝对时刻,防节点时钟漂移)→ result 置 Executing → 执行 disposition 动作
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

/// 业务作用：定位分区专属管理命令流，使请求由该分区的持有者执行。
///
/// 参数说明：
/// - `layout`: 分区组 key 布局。
/// - `p`: 调用方已校验的目标分区编号。
///
/// 返回：包含布局前缀和分区编号的命令流键；仅生成名称，不检查分区是否存在。
pub fn cmd_stream_key(layout: &KeyLayout, p: u32) -> String {
    format!("{}:park-cmd:{}", layout.prefix, p)
}

/// 业务作用：为同一分区内的幂等操作定位独立结果记录，供提交方查询终态。
///
/// 参数说明：
/// - `layout`: 分区组 key 布局。
/// - `p`: 分区编号。
/// - `operation_id`: 管理命令的幂等操作 ID。
///
/// 返回：操作结果键；不读取记录，也不验证操作 ID 的业务唯一性。
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
    /// 动作、分区和接纳窗口的 CRC16 摘要，保留持久协议格式；不能单独保证请求语义相等。
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
    /// 执行后的 marker 版本观测；读取失败时保留执行前值。
    /// 两次读取不是同一快照，差值不能单独证明变化仅由本操作产生。
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
/// TIME 返回 [秒, 微秒]，换算时同时使用两部分，保持同秒内毫秒 deadline 的判定精度。
///
/// 参数说明：
/// - `client`：提供权威时钟的 Redis 客户端。
///
/// 返回：合法且能用 u64 毫秒表示的服务端时间；元素数量、分量、溢出或通信错误拒绝继续判定 deadline。
pub(crate) async fn redis_now(client: &Arc<RedisClient>) -> Result<u64> {
    let v: Vec<String> = redis::cmd("TIME").query_async(&mut client.conn()).await?;
    // 权威时间必须完整可信；截断、归零或回绕会改变命令接纳窗口，不能继续保存结果或执行动作。
    if v.len() != 2 {
        return Err(NasaRedisError::ProtocolMarker(
            "TIME 响应必须包含秒与微秒两个分量".into(),
        ));
    }
    let sec: u64 = v
        .first()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| NasaRedisError::ProtocolMarker("TIME 响应缺/坏秒位".into()))?;
    let usec: u64 = v
        .get(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| NasaRedisError::ProtocolMarker("TIME 响应缺/坏微秒位".into()))?;
    if usec >= 1_000_000 {
        return Err(NasaRedisError::ProtocolMarker(
            "TIME 微秒分量超出合法范围".into(),
        ));
    }
    sec.checked_mul(1_000)
        .and_then(|millis| millis.checked_add(usec / 1_000))
        .ok_or_else(|| NasaRedisError::ProtocolMarker("TIME 毫秒值超出整数范围".into()))
}

/// 业务作用：按当前持久协议生成动作、分区和超时的短摘要，供重复提交比对。
///
/// 参数说明：
/// - `action`: 分区控制命令对应的动作。
/// - `p`: 目标分区编号。
/// - `timeout_ms`: 管理命令的接纳窗口，变化后属于不同请求。
///
/// 返回：CRC16 的四位十六进制文本；摘要可能碰撞，不能替代原始字段的语义比对或权限校验。
fn digest_of(action: CmdAction, p: u32, timeout_ms: u64) -> String {
    format!(
        "{:04x}",
        crate::keytag::crc16(format!("{action:?}|{p}|{timeout_ms}").as_bytes())
    )
}

/// 业务作用：先建立可查询的管理结果，再向目标分区投递操作 ID，使提交响应丢失后可以继续追踪。
///
/// 参数说明：
/// - `client`: 写入 command stream 和 result key 的 Redis 客户端。
/// - `layout`: 目标分区组 key 布局。
/// - `p`: 目标分区编号。
/// - `operation_id`: 调用方提供的幂等操作 ID。
/// - `action`: 要提交给 owner 执行的管理动作。
/// - `timeout_ms`: 从 Redis TIME 起算的命令接纳超时窗口。
///
/// 返回：新建或已存在的结果记录；未终结请求允许再次投递，终态直接返回。
/// 动作、接纳窗口或摘要冲突时拒绝复用操作 ID；Redis 失败不撤销已建立的结果或已经投递的命令。
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
        // 短摘要存在碰撞，必须同时比较结果中保存的原始语义，避免不同动作或窗口复用同一操作。
        let existing: CmdResult = query(client, layout, p, operation_id)
            .await?
            .ok_or_else(|| NasaRedisError::Config("result 消失(竞态)".into()))?;
        if existing.digest != digest
            || existing.action != action
            || existing.timeout_ms != timeout_ms
        {
            return Err(NasaRedisError::Config(format!(
                "OperationIdConflict: op={operation_id} 已绑定 {:?},本次 {action:?}",
                existing.action
            )));
        }
        // 终态已经给出确定结果，不再扩大命令流；非终态补投以覆盖此前入流结果未知的情况。
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
/// 参数说明：
/// - `client`: 读取 result key 的 Redis 客户端。
/// - `layout`: 目标分区组 key 布局。
/// - `p`: 目标分区编号。
/// - `operation_id`: 要查询的幂等操作 ID。
///
/// 返回：记录存在时解码其当前状态，不存在时返回 None；通信或 JSON 格式错误直接返回错误。
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

/// 业务作用：覆盖保存管理结果，终态同时附加查询保留期限，非终态继续保留供接管重试。
///
/// 参数说明：
/// - `client`: 保存结果的 Redis 客户端。
/// - `layout`: 管理操作所属分区组的键布局。
/// - `p`: 操作所属分区编号。
/// - `operation_id`: 定位该次幂等操作的标识。
/// - `r`: 调用方已确定的新状态及动作结果。
///
/// 返回：服务端确认写入时成功；编码或 Redis 错误透传，通信失败不能证明未写入。
/// 此函数不执行持有权比较，调用方负责操作权威及状态迁移顺序。
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
        // 终态与 24 小时 TTL 原子写入，避免崩溃留下无法到期的结果，并覆盖 producer 查询窗口。
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
/// 参数说明：
/// - `rt_client`: owner runtime 使用的 Redis 客户端。
/// - `layout`: 当前分区组 key 布局。
/// - `p`: 当前分区编号。
/// - `operation_id`: command entry 指向的幂等操作 ID。
/// - `do_action`: 执行 disposition 的惰性 Future；必须自行复验持有权并支持同操作重入。
/// - `action_hint`: 调用方据持久结果选择的动作，必须与当前记录中的 action 一致。
///
/// 返回：确定终态或无结果记录时返回 Finalize；可重试基础设施错误及窗口内冲突返回 Retry。
/// 动作不符、读取或保存失败直接返回错误，不能据此确认消息；本函数不执行 XACK/XDEL。
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
    // 动作必须在任何状态发布和 Future 轮询前一致；所有构建模式都拒绝将错误动作记为本操作成功。
    if action_hint != r.action {
        return Err(NasaRedisError::ProtocolMarker(
            "管理动作与持久结果不一致，拒绝执行或确认命令".into(),
        ));
    }
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
    match do_action.await {
        Ok(ids) => {
            r.state = CmdState::Succeeded;
            r.detail = ids.join(",");
            // 保存动作返回后的版本观测；读取失败不覆盖已有基线，也不把观测差值当作持有权证明。
            r.marker_version_after = super::disposition::marker_version(rt_client, layout, p)
                .await
                .unwrap_or(r.marker_version_before);
            put_result(rt_client, layout, p, operation_id, &r).await?;
            Ok(CmdAck::Finalize)
        }
        Err(NasaRedisError::OperationConflict(msg)) => {
            // OperationConflict 必须表示副作用前拒绝；窗口内等待原操作收敛，超窗后终结当前请求。
            // 此处不接管其它操作的中间态，也不能将已产生副作用的未知结果归入该分支。
            let now = redis_now(rt_client).await?;
            if now.saturating_sub(r.created_at) > r.timeout_ms {
                r.state = CmdState::Rejected;
                r.detail = format!("OperationConflict(超 producer 窗口,需异 op takeover): {msg}");
                // 当前操作在副作用前被拒绝，保留原版本观测，避免将其它操作的并发推进记入本次结果。
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
