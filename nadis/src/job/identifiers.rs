//! Job 标识线协议：Run、Fanout、shard 与 Worker 的稳定字节标识。
//!
//! 所有标识都以 4 字节网络序长度前缀拼接 UTF-8 字段后取 SHA-256，是持久线协议：多节点据此对同一逻辑
//! Run/Fanout 收敛为同一批次。任何字段顺序或编码漂移都会让重试落到不同标识，破坏"同因果只处理一次"。

use crate::idempotent::wire::framed_digest;

/// 业务作用：生成固定 128 位协议标识（摘要前 16 字节的小写 hex）。
///
/// 参数说明：
/// - `fields`: 按协议顺序排列的字段。
///
/// 返回：32 位小写 hex。
fn digest128(fields: &[&str]) -> String {
    let parts: Vec<&[u8]> = fields.iter().map(|f| f.as_bytes()).collect();
    hex::encode(&framed_digest(&parts)[..16])
}

/// 业务作用：生成完整 256 位协议摘要的小写 hex。
///
/// 参数说明：
/// - `fields`: 按协议顺序排列的字段。
///
/// 返回：64 位小写 hex。
fn digest256(fields: &[&str]) -> String {
    hex::encode(framed_digest(
        &fields.iter().map(|f| f.as_bytes()).collect::<Vec<_>>(),
    ))
}

/// 业务作用：生成自动调度 Run 的稳定幂等标识；同一逻辑触发时刻只对应一个 Run。
///
/// 参数说明：
/// - `qualifier`/`namespace`/`job_name`/`logical_fire_at`/`trigger_type`: source、命名空间、任务名、逻辑触发毫秒时刻与触发类型。
///
/// 返回：32 位小写 hex 标识。
pub fn scheduled_run_id(
    qualifier: &str,
    namespace: &str,
    job_name: &str,
    logical_fire_at: i64,
    trigger_type: &str,
) -> String {
    digest128(&[
        qualifier,
        namespace,
        job_name,
        &logical_fire_at.to_string(),
        trigger_type,
    ])
}

/// 业务作用：生成手工触发 Run 的稳定幂等标识；观测时刻不参与，requestId 相同即同一个 Run。
///
/// 参数说明：
/// - `qualifier`/`namespace`/`job_name`/`request_id`: source、命名空间、任务名与调用方幂等请求标识。
///
/// 返回：32 位小写 hex 标识。
pub fn manual_run_id(qualifier: &str, namespace: &str, job_name: &str, request_id: &str) -> String {
    digest128(&[qualifier, namespace, job_name, "MANUAL", request_id])
}

/// 业务作用：生成与根 attempt 一一对应的 Fanout 标识，使网络重试采用同一批次。
///
/// 参数说明：
/// - `root_run_id`/`root_attempt`: 根 Run 标识与根 attempt。
///
/// 返回：32 位小写 hex 标识。
pub fn fanout_id(root_run_id: &str, root_attempt: i32) -> String {
    digest128(&[root_run_id, &root_attempt.to_string()])
}

/// 业务作用：生成不随目标重分配变化的 Fanout shard 标识。
///
/// 参数说明：
/// - `fanout_id`/`seq`: Fanout 标识与稳定分片序号。
///
/// 返回：32 位小写 hex 标识。
pub fn shard_run_id(fanout_id: &str, seq: i64) -> String {
    digest128(&[fanout_id, &seq.to_string()])
}

/// 业务作用：生成完整 Worker 摘要作为 Stream 路由键，避免截断碰撞共享消费通道。
///
/// 参数说明：
/// - `worker_name`: Worker 能力名。
///
/// 返回：64 位小写 hex 摘要。
pub fn worker_key(worker_name: &str) -> String {
    digest256(&[worker_name])
}

/// 业务作用：生成能力快照的跨语言完整摘要，source 与成员字段分别编码以消除分隔符歧义。
///
/// 参数说明：
/// - `qualifier`/`namespace`/`worker_name`: 快照所属 source、命名空间与 Worker。
/// - `selected_at`: Redis 选定时刻。
/// - `members`: 已按稳定顺序展开的 nodeIdentity、executorId、heartbeatRevision 字段。
///
/// 返回：64 位小写 hex 摘要。
pub fn snapshot_digest(
    qualifier: &str,
    namespace: &str,
    worker_name: &str,
    selected_at: i64,
    members: &[String],
) -> String {
    let selected_at = selected_at.to_string();
    let mut fields = Vec::with_capacity(4 + members.len());
    fields.extend([qualifier, namespace, worker_name, selected_at.as_str()]);
    fields.extend(members.iter().map(String::as_str));
    digest256(&fields)
}
