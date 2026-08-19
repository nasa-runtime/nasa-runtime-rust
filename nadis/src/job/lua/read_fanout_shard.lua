-- 业务作用：按固定字段顺序读取 Fanout shard 的合同、assignment、执行权、参数和恢复截止点。
-- KEYS[1] shard HASH。
-- ARGV：无。
-- 返回：shard 不存在时返回空数组；存在时按 HMGET 中声明的顺序返回 28 个字段。
-- 原子性：通知只用于定位 fanoutId/seq，目标身份、assignmentEpoch 和执行状态必须以本读取结果为准。

if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
return redis.call('HMGET', KEYS[1],
        'fanoutId', 'rootRunId', 'snapshotId', 'workerName', 'contractRevision',
        'schemaId', 'wireCodec', 'shardIndex', 'shardTotal', 'seq', 'executionKey',
        'targetNodeIdentity', 'targetStartupId', 'assignmentEpoch', 'assignmentCount',
        'state', 'attempt', 'attemptToken', 'owner', 'leaseUntil', 'parameterPayload',
        'inboxMessageId', 'receiptRetryCount', 'readyWakeupCount', 'originExecutorId',
        'receiptDeadlineAt', 'startVisibleAt', 'targetHeartbeatRevision')
