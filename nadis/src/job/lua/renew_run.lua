-- 业务作用：续期单个普通 Run 的执行租约，或在 Fanout 创建阶段延长跨 slot intent 截止点。
-- KEYS[1] Run HASH；KEYS[2] leases ZSET；KEYS[3] waiting ZSET。
-- ARGV[1] runId；ARGV[2] owner；ARGV[3] attemptToken；ARGV[4] 续期时长毫秒。
-- 返回：STATE_MISMATCH、STALE_OWNER，或 {OK, redisNow, deadline, cancelRequestedAt}。
-- 安全不变量：只有当前 owner/token 可续期；RUNNING 与 FANOUT_CREATING 分别写入不同权威索引，取消信号随续期响应返回。

local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'RUNNING' and state ~= 'FANOUT_CREATING' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return {'STALE_OWNER'} end
if redis.call('HGET', KEYS[1], 'attemptToken') ~= ARGV[3] then return {'STALE_OWNER'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local deadline = now + tonumber(ARGV[4])
if state == 'RUNNING' then
    redis.call('HSET', KEYS[1], 'leaseUntil', deadline)
    redis.call('ZADD', KEYS[2], deadline, ARGV[1])
else
    redis.call('HSET', KEYS[1], 'createDeadlineAt', deadline)
    redis.call('ZADD', KEYS[3], deadline, ARGV[1])
end
return {'OK', tostring(now), tostring(deadline), redis.call('HGET', KEYS[1], 'cancelRequestedAt') or ''}
