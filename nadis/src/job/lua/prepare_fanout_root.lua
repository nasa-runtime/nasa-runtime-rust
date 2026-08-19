-- 业务作用：在普通根 Run 上持久跨 slot Fanout intent，把完成权威从普通 Handler 转移给 Fanout 对账流程。
-- KEYS[1] 普通根 Run HASH；KEYS[2] leases ZSET；KEYS[3] waiting ZSET；KEYS[4] 任务定义 HASH。
-- ARGV[1] runId；ARGV[2] owner；ARGV[3] attemptToken；ARGV[4] fanoutId；ARGV[5] snapshotId；ARGV[6] shardTotal；ARGV[7] createTimeoutMs。
-- 返回：{ADOPTED, deadline}、STATE_MISMATCH、STALE_OWNER、JOB_DELETED，或 {PREPARED, redisNow, deadline}。
-- 安全不变量：只有当前 owner/token 可把 RUNNING 转为 FANOUT_CREATING；转移完成权前必须复验删除 fence；
-- 原执行租约与新创建截止索引在同一操作中切换。

local state = redis.call('HGET', KEYS[1], 'state')
if state == 'FANOUT_CREATING' and redis.call('HGET', KEYS[1], 'fanoutId') == ARGV[4] then
    local deletedRevision = tonumber(redis.call('HGET', KEYS[4], 'deletedRevision') or '-1')
    local runRevision = tonumber(redis.call('HGET', KEYS[1], 'definitionRevision') or '0')
    if redis.call('HGET', KEYS[4], 'state') == 'DELETED' or runRevision <= deletedRevision then
        return {'JOB_DELETED'}
    end
    return {'ADOPTED', redis.call('HGET', KEYS[1], 'createDeadlineAt') or '0'}
end
if state ~= 'RUNNING' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2]
        or redis.call('HGET', KEYS[1], 'attemptToken') ~= ARGV[3] then return {'STALE_OWNER'} end
local deletedRevision = tonumber(redis.call('HGET', KEYS[4], 'deletedRevision') or '-1')
local runRevision = tonumber(redis.call('HGET', KEYS[1], 'definitionRevision') or '0')
if redis.call('HGET', KEYS[4], 'state') == 'DELETED' or runRevision <= deletedRevision then
    -- 当前 attempt 仍由普通完成出口收尾；删除后不再创建新 Fanout intent。
    return {'JOB_DELETED'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local deadline = now + tonumber(ARGV[7])
-- 一旦进入 FANOUT_CREATING，普通 Handler 不再以普通完成路径提交，必须由跨 slot 对账收敛。
redis.call('HSET', KEYS[1], 'state', 'FANOUT_CREATING', 'fanoutId', ARGV[4],
        'snapshotId', ARGV[5], 'shardTotal', ARGV[6], 'rootAttempt',
        redis.call('HGET', KEYS[1], 'attempt'), 'createDeadlineAt', deadline)
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], deadline, ARGV[1])
return {'PREPARED', tostring(now), tostring(deadline)}
