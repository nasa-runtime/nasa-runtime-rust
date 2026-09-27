-- 业务作用：以两阶段宽限回收过期执行器主记录、能力索引和成员元数据。
-- KEYS[1] 执行器存活 ZSET；KEYS[2] registry GC ZSET。
-- ARGV[1] 回收宽限毫秒；ARGV[2] 单轮上限；ARGV[3] registry 键前缀。
-- 返回：{deletedCount, newlyExpiredCount, redisNow}。
-- 安全不变量：首次过期只进入 GC 候选；宽限到期时再复验执行器 expireAt，期间心跳恢复的成员不删除。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local grace = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
for _, executorId in ipairs(expired) do
    -- NX 保留第一次观察到过期的宽限截止点，重复扫描不无限延长回收。
    redis.call('ZADD', KEYS[2], 'NX', now + grace, executorId)
end
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, limit)
local deleted = 0
for _, executorId in ipairs(due) do
    local executorKey = ARGV[3] .. 'executor:' .. executorId
    local expireAt = tonumber(redis.call('HGET', executorKey, 'expireAt') or '0')
    if expireAt == 0 or expireAt + grace <= now then
        -- 主记录仍过期才删能力反向索引，避免新启动代次被旧 GC 撤销。
        redis.call('ZREM', KEYS[1], executorId)
        local fields = redis.call('HKEYS', executorKey)
        for _, field in ipairs(fields) do
            if string.sub(field, 1, 11) == 'capability:' then
                local workerName = string.sub(field, 12)
                redis.call('ZREM', ARGV[3] .. 'capability:' .. workerName, executorId)
                local metadata = ARGV[3] .. 'capability-meta:' .. workerName
                redis.call('HDEL', metadata,
                    executorId .. '|nodeIdentity', executorId .. '|startupId',
                    executorId .. '|applicationName', executorId .. '|runtime',
                    executorId .. '|contractRevision', executorId .. '|schemaId',
                    executorId .. '|wireCodecs', executorId .. '|implementationDigest')
            end
        end
        redis.call('DEL', executorKey)
        redis.call('ZREM', KEYS[2], executorId)
        deleted = deleted + 1
    else
        redis.call('ZREM', KEYS[2], executorId)
    end
end
return {tostring(deleted), tostring(#expired), tostring(now)}
