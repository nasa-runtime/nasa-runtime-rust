-- 业务作用：优雅停机时注销当前执行器，并从全部 Worker 能力反向索引移除其元数据。
-- KEYS[1] 执行器存活 ZSET；KEYS[2] 执行器 HASH；KEYS[3] registry GC ZSET；
-- KEYS[4..n] 按“Worker 能力 ZSET、Worker 成员元数据 HASH”成对排列。
-- ARGV[1] executorId。
-- 返回：NOT_FOUND、STALE_EXECUTOR 或 OK。
-- 原子性：执行器主记录、存活索引与所有能力索引同时撤销，新 Fanout 快照不会观察到部分注销状态。

if redis.call('EXISTS', KEYS[2]) == 0 then
    redis.call('ZREM', KEYS[1], ARGV[1])
    redis.call('ZREM', KEYS[3], ARGV[1])
    return {'NOT_FOUND'}
end
if redis.call('HGET', KEYS[2], 'executorId') ~= ARGV[1] then return {'STALE_EXECUTOR'} end

redis.call('HSET', KEYS[2], 'state', 'DRAINING', 'fanoutReady', 'false')
for index = 4, #KEYS, 2 do
    local capability = KEYS[index]
    local metadata = KEYS[index + 1]
    redis.call('ZREM', capability, ARGV[1])
    redis.call('HDEL', metadata,
        ARGV[1] .. '|nodeIdentity', ARGV[1] .. '|startupId',
        ARGV[1] .. '|applicationName', ARGV[1] .. '|runtime',
        ARGV[1] .. '|contractRevision', ARGV[1] .. '|schemaId',
        ARGV[1] .. '|wireCodecs', ARGV[1] .. '|implementationDigest')
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('DEL', KEYS[2])
return {'OK'}
