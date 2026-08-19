-- 业务作用：在固定 Fanout 桶中幂等建立根 intent，保存冻结快照和后续恢复所需的全部合同。
-- KEYS[1] Fanout 根 HASH；KEYS[2] Fanout roots/watch ZSET。
-- ARGV[1..5] fanoutId、rootRunId、rootAttempt、snapshotId、snapshotPayload；
-- ARGV[6..10] workerName、contractRevision、schemaId、wireCodec、shardTotal；
-- ARGV[11..16] receiptTimeoutMs、receiptMaxRetries、failurePolicy、createTimeoutMs、rootJobName、rootScheduleShard；
-- ARGV[17] schedulerQualifier：根记录自带的来源声明，监视器凭它发现被写错前缀的外来 Fanout。
-- 返回：ADOPTED、CONFLICT，或 {OK, redisNow}。
-- 安全不变量：同 fanoutId 只能绑定一个 snapshotId；根 HASH 与看门狗截止点同时建立，使创建中断仍有持久恢复入口。

if redis.call('EXISTS', KEYS[1]) == 1 then
    if redis.call('HGET', KEYS[1], 'snapshotId') == ARGV[4] then return {'ADOPTED'} end
    return {'CONFLICT'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local nextWatchAt = now + tonumber(ARGV[14])
redis.call('HSET', KEYS[1],
        'schedulerQualifier', ARGV[17],
        'fanoutId', ARGV[1], 'rootRunId', ARGV[2], 'rootAttempt', ARGV[3],
        'snapshotId', ARGV[4], 'snapshotPayload', ARGV[5], 'workerName', ARGV[6],
        'contractRevision', ARGV[7], 'schemaId', ARGV[8], 'wireCodec', ARGV[9],
        'shardTotal', ARGV[10], 'createdShardCount', 0, 'deliveryCursor', 0,
        'fanoutReceiptTimeoutMs', ARGV[11], 'fanoutReceiptMaxRetries', ARGV[12],
        'failurePolicy', ARGV[13], 'state', 'CREATING', 'createdAt', now,
        'createDeadlineAt', nextWatchAt, 'nextWatchAt', nextWatchAt,
        'successCount', 0, 'failedCount', 0,
        'deadCount', 0, 'skippedCount', 0, 'cancelledCount', 0,
        'rootJobName', ARGV[15], 'rootScheduleShard', ARGV[16])
redis.call('ZADD', KEYS[2], nextWatchAt, ARGV[1])
return {'OK', tostring(now)}
