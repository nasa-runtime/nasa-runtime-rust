-- 业务作用：扫描到期 visible 成员，为仍可执行的普通 Run 重建可能丢失的 Dispatch 消息。
-- KEYS[1] visible ZSET；KEYS[2] 命名空间控制 HASH。
-- ARGV[1] 单轮上限；ARGV[2] visibilityTimeoutMs；ARGV[3] 连续消息重建阈值；ARGV[4] 超限或暂停时的再扫描延迟毫秒；
-- ARGV[5] 调度分片键前缀；ARGV[6] 终态保留期毫秒。
-- 返回：{promotedCount, nextScore, redisNow}。
-- 安全不变量：Run 内 nextVisibleAt 必须与 ZSET score 一致；任务暂停时只延后，不创建新消息；
-- 连续重建超过阈值后降低频率并保留诊断，但仍须建立消息，兼容执行器恢复后才能重新取得执行权。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local runIds = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local prefix = ARGV[5]
local namespaceState = redis.call('HGET', KEYS[2], 'namespaceState') or 'ENABLED'
local promoted = 0
for _, runId in ipairs(runIds) do
    local runKey = prefix .. 'run:' .. runId
    local state = redis.call('HGET', runKey, 'state')
    local storedScore = tonumber(redis.call('HGET', runKey, 'nextVisibleAt') or '0')
    local indexScore = tonumber(redis.call('ZSCORE', KEYS[1], runId) or '0')
    -- HASH 与 ZSET 的截止点同时匹配才能提升，迟到扫描不得重放旧截止点。
    if (state == 'QUEUED' or state == 'RETRY_WAIT') and storedScore == indexScore then
        local jobName = redis.call('HGET', runKey, 'jobName')
        local jobKey = prefix .. 'job:' .. jobName
        local jobState = redis.call('HGET', jobKey, 'state')
        local deletedRevision = tonumber(redis.call('HGET', jobKey, 'deletedRevision') or '-1')
        local runRevision = tonumber(redis.call('HGET', runKey, 'definitionRevision') or '0')
        if jobState == 'DELETED' or runRevision <= deletedRevision then
            -- 删除是确定性终局：fence 内的 Run 直接终态化，不再按暂停语义无限延后空转。
            -- PAUSED 承担"暂时停止、以后继续"的可逆语义，删除不承担；FIXED_DELAY 也不重排。
            redis.call('HSET', runKey, 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'JOB_DELETED')
            redis.call('ZREM', KEYS[1], runId)
            redis.call('XADD', prefix .. 'completion', 'MAXLEN', '~', 100000, '*',
                    'runId', runId, 'state', 'CANCELLED', 'resultCode', 'JOB_DELETED')
            redis.call('PEXPIRE', runKey, tonumber(ARGV[6]))
        elseif jobState ~= 'ENABLED' or namespaceState ~= 'ENABLED' then
            -- 单任务与命名空间暂停都在恢复路径复验，尚未取得 attempt 的积压不能绕过控制面。
            local nextVisibleAt = now + tonumber(ARGV[4])
            redis.call('HSET', runKey, 'nextVisibleAt', nextVisibleAt)
            redis.call('ZADD', KEYS[1], nextVisibleAt, runId)
        else
            local maxAttempts = tonumber(ARGV[3])
            local previousAttempts = tonumber(redis.call('HGET', runKey, 'dispatchAttempts') or '0')
            -- 阈值只度量连续未取得执行权的重建。达到长退避档后保持饱和值，避免等待期间计数无界增长。
            local attempts = math.min(previousAttempts + 1, maxAttempts + 1)
            local throttled = attempts > maxAttempts
            local nextVisibleAt = now + (throttled and tonumber(ARGV[4]) or tonumber(ARGV[2]))
            local workerName = redis.call('HGET', runKey, 'workerName')
            local workerKey = redis.call('HGET', runKey, 'workerKey')
            redis.call('HSET', runKey, 'state', 'QUEUED', 'nextVisibleAt', nextVisibleAt,
                    'dispatchAttempts', attempts)
            -- 重建的信封与首次派发同构：来源声明取自 Run 记录，消费者无需读记录即可复核。
            -- 长退避只限制重建频率，不关闭唯一恢复入口；状态 CAS 继续阻止重复消息取得第二份执行权。
            redis.call('XADD', prefix .. 'dispatch:' .. workerKey, 'MAXLEN', '~', 100000, '*',
                    'runId', runId, 'jobName', jobName,
                    'workerName', workerName, 'protocolVersion', redis.call('HGET', runKey, 'protocolVersion'),
                    'definitionRevision', redis.call('HGET', runKey, 'definitionRevision'),
                    'schedulerQualifier', redis.call('HGET', runKey, 'schedulerQualifier') or '')
            if throttled then
                redis.call('HSET', runKey, 'errorType', 'NO_CAPABLE_EXECUTOR')
            end
            redis.call('ZADD', KEYS[1], nextVisibleAt, runId)
            promoted = promoted + 1
        end
    else
        redis.call('ZREM', KEYS[1], runId)
    end
end
local nextEntry = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return {tostring(promoted), nextEntry[2] or '', tostring(now)}
