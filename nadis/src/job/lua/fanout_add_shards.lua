-- 业务作用：在 Fanout 提交前按有界批次幂等建立 shard HASH。
-- KEYS[1] Fanout 根 HASH；KEYS[2..n] 本批 shard HASH。
-- ARGV[1] snapshotId；ARGV[2] 本批数量；ARGV[3] schedulerQualifier；ARGV[4] protocolVersion；
-- 之后每个 shard 使用 15 个字段：
-- shardIndex、fanoutId、rootRunId、workerName、contractRevision、schemaId、wireCodec、shardTotal、seq、executionKey、
-- targetNodeIdentity、targetStartupId、targetHeartbeatRevision、originExecutorId、parameterPayload。
-- 返回：STATE_MISMATCH、CONFLICT，或 {OK, addedCount, createdShardCount}。
-- 安全不变量：只在 CREATING 且 snapshotId 一致时写入；重放已存在 shard 不重复计数，但 shardIndex 不同会拒绝整批。

if redis.call('HGET', KEYS[1], 'state') ~= 'CREATING' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'snapshotId') ~= ARGV[1] then return {'CONFLICT'} end
local count = tonumber(ARGV[2])
local added = 0
-- 头部新增两个字段后, 每批 shard 的字段游标随之后移
local cursor = 5
for index = 1, count do
    local shardKey = KEYS[index + 1]
    local shardIndex = ARGV[cursor]
    if redis.call('EXISTS', shardKey) == 0 then
        redis.call('HSET', shardKey,
                'fanoutId', ARGV[cursor + 1], 'rootRunId', ARGV[cursor + 2],
                -- shard 自带来源与协议代次声明：Fanout 取得执行权时要能脱离键前缀独立复验，
                -- 并像普通 Run 一样拒绝自己不理解的高协议消息。
                'schedulerQualifier', ARGV[3], 'protocolVersion', ARGV[4],
                'snapshotId', ARGV[1], 'workerName', ARGV[cursor + 3],
                'contractRevision', ARGV[cursor + 4], 'schemaId', ARGV[cursor + 5],
                'wireCodec', ARGV[cursor + 6], 'shardIndex', shardIndex,
                'shardTotal', ARGV[cursor + 7], 'seq', ARGV[cursor + 8],
                'executionKey', ARGV[cursor + 9], 'targetNodeIdentity', ARGV[cursor + 10],
                'targetStartupId', ARGV[cursor + 11], 'assignmentEpoch', 0,
                'assignmentCount', 1, 'receiptRetryCount', 0, 'readyWakeupCount', 0,
                'targetHeartbeatRevision', ARGV[cursor + 12],
                'originExecutorId', ARGV[cursor + 13], 'parameterPayload', ARGV[cursor + 14],
                'state', 'AWAITING_RECEIPT', 'attempt', 0)
        added = added + 1
    elseif redis.call('HGET', shardKey, 'shardIndex') ~= shardIndex then
        return {'CONFLICT'}
    end
    cursor = cursor + 15
end
if added > 0 then redis.call('HINCRBY', KEYS[1], 'createdShardCount', added) end
return {'OK', tostring(added), redis.call('HGET', KEYS[1], 'createdShardCount')}
