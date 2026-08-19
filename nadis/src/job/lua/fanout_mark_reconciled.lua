-- 业务作用：标记 Fanout 终态已成功回填普通根 Run，从此允许按保留期回收桶内证据。
-- KEYS[1] Fanout 根 HASH；KEYS[2] roots/watch ZSET。
-- ARGV[1] fanoutId。
-- 返回：STATE_MISMATCH 或 {OK, reconciledAt}。
-- 安全不变量：只有桶内根已终态才能标记对账；清理脚本会同时复验 reconciledAt 和 expireAt。

local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'SUCCEEDED' and state ~= 'PARTIAL_FAILED' and state ~= 'FAILED' and state ~= 'CANCELLED' then
    return {'STATE_MISMATCH'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('HSET', KEYS[1], 'reconciledAt', now)
redis.call('ZADD', KEYS[2], redis.call('HGET', KEYS[1], 'expireAt') or now, ARGV[1])
return {'OK', tostring(now)}
