-- 业务作用：读取 Fanout 根的对账定位字段，并依据非终态、待对账终态或已对账终态安排下次看门时刻。
-- KEYS[1] Fanout 根 HASH；KEYS[2] roots/watch ZSET。
-- ARGV[1] fanoutId；ARGV[2] 看门重试间隔毫秒。
-- 返回：NOT_FOUND，或 {OK, state, rootRunId, rootAttempt, rootJobName, rootScheduleShard, cancelReason, errorType}。
-- 原子性：根不存在时删除孤立索引；未对账终态保持快速重试，已对账终态直接指向 expireAt。

if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('ZREM', KEYS[2], ARGV[1])
    return {'NOT_FOUND'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'SUCCEEDED' and state ~= 'PARTIAL_FAILED' and state ~= 'FAILED' and state ~= 'CANCELLED' then
    local nextWatchAt = now + tonumber(ARGV[2])
    redis.call('HSET', KEYS[1], 'nextWatchAt', nextWatchAt)
    redis.call('ZADD', KEYS[2], nextWatchAt, ARGV[1])
elseif redis.call('HGET', KEYS[1], 'reconciledAt') then
    redis.call('ZADD', KEYS[2], redis.call('HGET', KEYS[1], 'expireAt') or now + tonumber(ARGV[2]), ARGV[1])
else
    redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
end
return {'OK', state, redis.call('HGET', KEYS[1], 'rootRunId') or '',
        redis.call('HGET', KEYS[1], 'rootAttempt') or '0',
        redis.call('HGET', KEYS[1], 'rootJobName') or '',
        redis.call('HGET', KEYS[1], 'rootScheduleShard') or '0',
        redis.call('HGET', KEYS[1], 'cancelReason') or '',
        redis.call('HGET', KEYS[1], 'errorType') or ''}
