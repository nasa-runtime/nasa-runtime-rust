-- 业务作用：在同一 Redis slot 内使用一次服务端时间续期多个普通 Run 或 Fanout shard 执行权。
-- KEYS：每项三个键，依次为 Run/shard HASH、leases ZSET、waiting ZSET。
-- ARGV[1] 项数；之后每项四个参数：索引成员、owner、attemptToken、leaseDurationMs。
-- 返回：{redisNow, 每项的 code, deadline, cancelRequestedAt}；code 为 OK、STALE_OWNER 或 STATE_MISMATCH。
-- 安全不变量：状态、owner 与 attemptToken 全部匹配才续期；FANOUT_CREATING 延长创建截止点，不写执行租约索引。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local result = {tostring(now)}
local count = tonumber(ARGV[1])
for index = 1, count do
    local keyOffset = (index - 1) * 3
    local argOffset = 2 + (index - 1) * 4
    local recordKey = KEYS[keyOffset + 1]
    local leaseKey = KEYS[keyOffset + 2]
    local waitingKey = KEYS[keyOffset + 3]
    local member = ARGV[argOffset]
    local owner = ARGV[argOffset + 1]
    local token = ARGV[argOffset + 2]
    local duration = tonumber(ARGV[argOffset + 3])
    local state = redis.call('HGET', recordKey, 'state')
    local code = 'STATE_MISMATCH'
    local deadline = 0
    local cancellation = ''
    if (state == 'RUNNING' or state == 'FANOUT_CREATING')
            and redis.call('HGET', recordKey, 'owner') == owner
            and redis.call('HGET', recordKey, 'attemptToken') == token then
        -- 只有当前代次可推进截止点，旧线程的批量续期不会延长新 owner 的权威。
        code = 'OK'
        deadline = now + duration
        cancellation = redis.call('HGET', recordKey, 'cancelRequestedAt') or ''
        if state == 'RUNNING' then
            redis.call('HSET', recordKey, 'leaseUntil', deadline)
            redis.call('ZADD', leaseKey, deadline, member)
        else
            redis.call('HSET', recordKey, 'createDeadlineAt', deadline)
            redis.call('ZADD', waitingKey, deadline, member)
        end
    elseif state == 'RUNNING' or state == 'FANOUT_CREATING' then
        code = 'STALE_OWNER'
    end
    result[#result + 1] = code
    result[#result + 1] = tostring(deadline)
    result[#result + 1] = cancellation
end
return result
