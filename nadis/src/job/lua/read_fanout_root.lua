-- 业务作用：按固定字段顺序读取 Fanout 根的跨监视器权威快照。
-- KEYS[1] Fanout 根 HASH。
-- ARGV：无。
-- 返回：根不存在时返回空数组；存在时按 HMGET 中声明的顺序返回根状态、合同、游标、截止点和对账字段。
-- 原子性：脚本只读取单个 HASH，调用方必须依字段顺序解码，不得把 Pub/Sub 信封当作权威状态。

if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
return redis.call('HMGET', KEYS[1],
        'fanoutId', 'state', 'rootRunId', 'rootAttempt', 'rootJobName', 'rootScheduleShard',
        'workerName', 'contractRevision', 'schemaId', 'wireCodec', 'shardTotal',
        'deliveryCursor', 'fanoutReceiptTimeoutMs', 'fanoutReceiptMaxRetries',
        'failurePolicy', 'expireAt', 'cleanupCursor', 'reconciledAt', 'cancelReason',
        'capabilityCursor', 'cancelCursor', 'createDeadlineAt', 'schedulerQualifier')
