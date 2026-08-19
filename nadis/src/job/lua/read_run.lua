-- 业务作用：按固定字段顺序读取普通 Run 的身份、状态、执行权、结果、参数和 Fanout intent。
-- KEYS[1] Run HASH。
-- ARGV：无。
-- 返回：Run 不存在时返回空数组；存在时按 HMGET 中声明的顺序返回 21 个字段。
-- 原子性：脚本只读取单个 HASH；返回的 owner/attemptToken 是观测值，副作用提交仍必须通过写脚本重新复验。

if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
return redis.call('HMGET', KEYS[1],
        'runId', 'jobName', 'workerName', 'state', 'logicalFireAt', 'triggeredAt',
        'attempt', 'attemptToken', 'owner', 'leaseUntil', 'resultCode', 'resultSummary',
        'errorType', 'errorSummary', 'parameterPayload', 'schemaId', 'wireCodec',
        'fanoutId', 'snapshotId', 'rootAttempt', 'createDeadlineAt')
