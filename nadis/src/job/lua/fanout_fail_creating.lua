-- 业务作用：Fanout 桶内根长时停留 CREATING 时，在服务端截止点后收敛为 FAILED 并进入对账与回收流程。
-- KEYS[1] Fanout 根 HASH；KEYS[2] roots/watch ZSET；KEYS[3] completion Stream；KEYS[4] GC ZSET。
-- ARGV[1] fanoutId；ARGV[2] Fanout 终态保留期毫秒。
-- 返回：STATE_MISMATCH、{NOT_DUE, deadline} 或 {OK, FAILED, expireAt}。
-- 安全不变量：只有 CREATING 且 createDeadlineAt 已到期才收敛；根终态与 completion/GC 索引同时建立。

if redis.call('HGET', KEYS[1], 'state') ~= 'CREATING' then return {'STATE_MISMATCH'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local deadline = tonumber(redis.call('HGET', KEYS[1], 'createDeadlineAt') or '0')
if deadline > now then return {'NOT_DUE', tostring(deadline)} end
local expireAt = now + tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'state', 'FAILED', 'finishedAt', now, 'expireAt', expireAt,
        'errorType', 'FANOUT_CREATE_TIMEOUT', 'resultCode', 'FAILED')
redis.call('ZADD', KEYS[2], now, ARGV[1])
redis.call('ZADD', KEYS[4], expireAt, ARGV[1])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMPLETED',
        'fanoutId', ARGV[1], 'rootRunId', redis.call('HGET', KEYS[1], 'rootRunId'),
        'state', 'FAILED', 'errorType', 'FANOUT_CREATE_TIMEOUT')
return {'OK', 'FAILED', tostring(expireAt)}
