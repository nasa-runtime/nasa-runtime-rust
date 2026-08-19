-- 业务作用：在全部 shard 已幂等建立后提交 Fanout 根，开放首次投递与跨 slot 对账。
-- KEYS[1] Fanout 根 HASH；KEYS[2] roots/watch ZSET；KEYS[3] completion Stream。
-- ARGV[1] fanoutId。
-- 返回：ADOPTED、STATE_MISMATCH、INVALID，或 {OK, redisNow}。
-- 安全不变量：createdShardCount 必须等于 shardTotal；COMMITTED、看门狗索引与完成事件在同一 slot 内同时发布。

local state = redis.call('HGET', KEYS[1], 'state')
if state == 'COMMITTED' or state == 'WAITING_CHILDREN' then return {'ADOPTED'} end
if state ~= 'CREATING' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'createdShardCount') ~= redis.call('HGET', KEYS[1], 'shardTotal') then
    return {'INVALID'}
end
-- 先确认完整性再开放 COMMITTED，目标节点不会运行半个 Fanout。
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('HSET', KEYS[1], 'state', 'COMMITTED', 'committedAt', now,
        'deliveryCursor', 0, 'nextWatchAt', now)
redis.call('ZADD', KEYS[2], now, ARGV[1])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMMITTED',
        'fanoutId', ARGV[1], 'rootRunId', redis.call('HGET', KEYS[1], 'rootRunId'))
return {'OK', tostring(now)}
