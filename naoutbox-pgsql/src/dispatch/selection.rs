//! 身份与正文分两次有界读取，头行被锁或正文超限都不能越过同一聚合根的顺序。

use crate::{
    commit_transaction, ensure_owner, map_err, parse_pending, OutboxStoreError, OwnerClaim,
    PendingRow,
};
use sqlx::{Postgres, Row, Transaction};

pub(super) struct Key {
    aggregate_type: String,
    aggregate_id: String,
}

pub(super) struct Head {
    pub(super) key: Key,
    id: i64,
    event_id: String,
}

pub(super) enum Candidate {
    Empty,
    Deferred(&'static str),
    Ready(PendingRow),
}

/// 业务作用：在固定数据库等待预算内复验原owner，避免坏锁或无效索引占住派发任务。
///
/// 参数说明：
/// - `claim`：本轮已取得的global租约与fence。
///
/// 返回：已收紧语句和锁等待并复验权威的事务；失败不读取事件正文。
async fn begin(claim: &OwnerClaim) -> Result<Transaction<'static, Postgres>, OutboxStoreError> {
    let mut transaction = claim.pool.begin().await.map_err(map_err)?;
    sqlx::query("SET LOCAL statement_timeout = '2s'")
        .execute(&mut *transaction)
        .await
        .map_err(map_err)?;
    sqlx::query("SET LOCAL lock_timeout = '250ms'")
        .execute(&mut *transaction)
        .await
        .map_err(map_err)?;
    // 获取候选和读取正文分别复验；第一次成功不能延长第二次事务的控制权威。
    ensure_owner(&mut transaction, claim).await?;
    Ok(transaction)
}

/// 业务作用：按当前待投递集合寻找轮转后继并回绕，不扫描或缓存全部操作身份。
///
/// 参数说明：
/// - `claim`：本轮global派发权威。
/// - `after`：上轮已明确取得的聚合根位置，缺省从当前集合起点读取。
///
/// 返回：明确提交后的单个最早事件身份；空集合为None，索引或事务不满足时拒绝。
pub(super) async fn next(
    claim: &OwnerClaim,
    after: Option<&Key>,
) -> Result<Option<Head>, OutboxStoreError> {
    let mut transaction = begin(claim).await?;
    let indexed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid \
         JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid=to_regclass('outbox_event') \
         AND i.indexrelid=to_regclass('outbox_aggregate_dispatchable_idx') \
         AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique \
         AND i.indnkeyatts=3 AND i.indnatts=3 AND a.amname='btree' \
         AND i.indoption[0]=0 AND i.indoption[1]=0 AND i.indoption[2]=0 \
         AND NOT EXISTS (SELECT 1 FROM generate_series(0,2) AS k(n) \
             LEFT JOIN pg_attribute att ON att.attrelid=i.indrelid AND att.attnum=i.indkey[k.n] \
             LEFT JOIN pg_opclass op ON op.oid=i.indclass[k.n] \
             WHERE att.attnum IS NULL OR op.oid IS NULL OR NOT op.opcdefault \
                OR op.opcintype<>att.atttypid OR op.opcmethod<>c.relam \
                OR i.indcollation[k.n]<>att.attcollation) \
         AND pg_get_indexdef(i.indexrelid,1,true)='aggregate_type' \
         AND pg_get_indexdef(i.indexrelid,2,true)='aggregate_id' \
         AND pg_get_indexdef(i.indexrelid,3,true)='id' \
         AND regexp_replace(pg_get_expr(i.indpred,i.indrelid),'[()[:space:]]','','g')='NOTdispatchedANDNOTdead')",
    )
    .fetch_one(&mut *transaction)
    .await
    .map_err(map_err)?;
    if !indexed {
        // 缺少已声明的访问路径时不能退化为无界队列排序，结构变更必须先由显式迁移完成。
        return Err(OutboxStoreError::new(
            "aggregate dispatch index unavailable",
        ));
    }
    let mut row = if let Some(after) = after {
        sqlx::query(
            "SELECT id,event_id,aggregate_type,aggregate_id FROM outbox_event \
             WHERE NOT dispatched AND NOT dead AND (aggregate_type,aggregate_id)>($1,$2) \
             ORDER BY aggregate_type,aggregate_id,id LIMIT 1",
        )
        .bind(&after.aggregate_type)
        .bind(&after.aggregate_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_err)?
    } else {
        None
    };
    if row.is_none() {
        row = sqlx::query(
            "SELECT id,event_id,aggregate_type,aggregate_id FROM outbox_event \
             WHERE NOT dispatched AND NOT dead ORDER BY aggregate_type,aggregate_id,id LIMIT 1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_err)?;
    }
    let head = row
        .map(|row| -> Result<Head, OutboxStoreError> {
            Ok(Head {
                key: Key {
                    aggregate_type: row.try_get("aggregate_type").map_err(map_err)?,
                    aggregate_id: row.try_get("aggregate_id").map_err(map_err)?,
                },
                id: row.try_get("id").map_err(map_err)?,
                event_id: row.try_get("event_id").map_err(map_err)?,
            })
        })
        .transpose()?;
    commit_transaction(transaction).await?;
    Ok(head)
}

/// 业务作用：只读取已选头事件，锁冲突或超限时保留该根事实并允许下一根获得轮转机会。
///
/// 参数说明：
/// - `claim`：仍需在本事务复验的原租约。
/// - `head`：已明确读取的精确事件身份及聚合根。
/// - `max_payload_bytes`：SQL向客户端交付正文的最大字节数。
///
/// 返回：至多一份移入发布链的正文、明确暂缓或已消失的候选；数据库未知失败不执行发布。
pub(super) async fn load(
    claim: &OwnerClaim,
    head: &Head,
    max_payload_bytes: usize,
) -> Result<Candidate, OutboxStoreError> {
    let mut transaction = begin(claim).await?;
    // NOWAIT拒绝被锁的原头行；SKIP LOCKED在这里会越过同根顺序，不能改为投递后继事件。
    let selected = sqlx::query(
        "SELECT id,event_id,aggregate_type,aggregate_id,event_type,traceparent,tenant, \
         octet_length(payload) AS payload_length, \
         CASE WHEN octet_length(payload)<=$3 THEN payload ELSE NULL END AS payload \
         FROM outbox_event WHERE id=$1 AND event_id=$2 AND NOT dispatched AND NOT dead \
         FOR UPDATE NOWAIT",
    )
    .bind(head.id)
    .bind(&head.event_id)
    .bind(max_payload_bytes as i64)
    .fetch_optional(&mut *transaction)
    .await;
    let row = match selected {
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => {
            // 锁拒绝会中止当前事务，明确回滚后才能正常释放owner；未发布头行仍保持待投递。
            transaction.rollback().await.map_err(map_err)?;
            return Ok(Candidate::Deferred("aggregate head is locked"));
        }
        result => result.map_err(map_err)?,
    };
    let candidate = if let Some(row) = row {
        let aggregate_type: &str = row.try_get("aggregate_type").map_err(map_err)?;
        let aggregate_id: &str = row.try_get("aggregate_id").map_err(map_err)?;
        if aggregate_type != head.key.aggregate_type || aggregate_id != head.key.aggregate_id {
            // 事件被移动到其它聚合根后，旧排序位置不再证明它仍是该根头事件。
            return Err(OutboxStoreError::new("aggregate head identity changed"));
        }
        let length: i32 = row.try_get("payload_length").map_err(map_err)?;
        if length < 0 || length as usize > max_payload_bytes {
            Candidate::Deferred("aggregate payload budget exceeded")
        } else {
            Candidate::Ready(parse_pending(row)?)
        }
    } else {
        Candidate::Empty
    };
    // 身份与正文事务均须明确完成，网络发布不持有数据库连接或行锁。
    commit_transaction(transaction).await?;
    Ok(candidate)
}
