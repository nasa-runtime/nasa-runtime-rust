-- 业务作用：在一个调度分片中使用同一 Redis TIME 批量创建到期 Run，每项仍独立执行逻辑时刻 CAS。
-- KEYS[1] schedule ZSET；KEYS[2] visible ZSET；KEYS[3] 命名空间控制 HASH；之后每项三个键：任务 HASH、Run HASH、Dispatch Stream。
-- ARGV[1] 项数；之后每项 13 个参数：definitionRevision、expectedFireAt、proposedNextFireAt、runId、scheduleType、
-- visibilityTimeoutMs、protocolVersion、jobName、scheduleShard、wireCodec、SKIP/RUN、triggerType、mustEndInFuture。
-- 返回：{redisNow, code1, messageId1, code2, messageId2, ...}。
-- 安全不变量：所有项共享同一服务端时间，但各自复验定义、修订号和 score；某项被拒绝不影响其它项的独立结果。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local result = {tostring(now)}
local count = tonumber(ARGV[1])
local namespaceState = redis.call('HGET', KEYS[3], 'namespaceState') or 'ENABLED'
for index = 1, count do
    local keyOffset = 3 + (index - 1) * 3
    local argOffset = 2 + (index - 1) * 13
    local jobKey = KEYS[keyOffset + 1]
    local runKey = KEYS[keyOffset + 2]
    local dispatchKey = KEYS[keyOffset + 3]
    local revision = ARGV[argOffset]
    local expected = ARGV[argOffset + 1]
    local proposed = ARGV[argOffset + 2]
    local runId = ARGV[argOffset + 3]
    local scheduleType = ARGV[argOffset + 4]
    local visibility = tonumber(ARGV[argOffset + 5])
    local protocol = ARGV[argOffset + 6]
    local jobName = ARGV[argOffset + 7]
    local shard = ARGV[argOffset + 8]
    local codec = ARGV[argOffset + 9]
    local decision = ARGV[argOffset + 10]
    local triggerType = ARGV[argOffset + 11]
    local mustFuture = ARGV[argOffset + 12]
    local code = 'OK'
    local messageId = ''
    local state = redis.call('HGET', jobKey, 'state')
    local currentScore = redis.call('ZSCORE', KEYS[1], jobName)
    if namespaceState ~= 'ENABLED' or state ~= 'ENABLED' then
        code = 'STATE_MISMATCH'
    elseif redis.call('HGET', jobKey, 'definitionRevision') ~= revision then
        code = 'STALE'
    elseif not currentScore or tonumber(currentScore) ~= tonumber(expected) then
        code = 'STALE'
    elseif tonumber(expected) > now then
        code = 'NOT_DUE'
    elseif scheduleType ~= 'FIXED_DELAY' and mustFuture == '1' and tonumber(proposed) <= now then
        -- 批次在调用侧计算后可能已跨过下一时刻，拒绝提交并让调用方基于新 Redis TIME 重算。
        code = 'NEED_RECOMPUTE'
    elseif decision == 'SKIP' then
        redis.call('HSET', jobKey, 'lastFireAt', expected, 'nextFireAt', proposed)
        redis.call('ZADD', KEYS[1], proposed, jobName)
        code = 'SKIPPED'
    elseif redis.call('EXISTS', runKey) == 1 then
        code = 'ADOPTED'
    else
        local workerName = redis.call('HGET', jobKey, 'workerName')
        local workerKey = redis.call('HGET', jobKey, 'workerKey')
        -- Run 的来源声明只从定义继承, 使消费者可以独立核对消息来源而不依赖键前缀
        local sourceId = redis.call('HGET', jobKey, 'schedulerQualifier') or ''
        redis.call('HSET', runKey,
                'schedulerQualifier', sourceId,
                'runId', runId, 'jobName', jobName, 'workerName', workerName, 'workerKey', workerKey,
                'protocolVersion', protocol, 'scheduleShard', shard,
                'definitionRevision', revision,
                'contractRevision', redis.call('HGET', jobKey, 'contractRevision'),
                'schemaId', redis.call('HGET', jobKey, 'schemaId'),
                'wireCodec', codec, 'triggerType', triggerType,
                'logicalFireAt', expected, 'triggeredAt', now,
                'state', 'QUEUED', 'attempt', 0, 'dispatchAttempts', 0,
                'nextVisibleAt', now + visibility, 'createdAt', now)
        -- 信封同样携带来源声明: 跨语言消费者不必先读 Run 记录就能拒绝串源消息
        messageId = redis.call('XADD', dispatchKey, 'MAXLEN', '~', 100000, '*',
                'runId', runId, 'jobName', jobName, 'workerName', workerName,
                'protocolVersion', protocol, 'definitionRevision', revision,
                'schedulerQualifier', sourceId)
        redis.call('ZADD', KEYS[2], now + visibility, runId)
        if scheduleType == 'FIXED_DELAY' then
            redis.call('ZREM', KEYS[1], jobName)
        else
            redis.call('HSET', jobKey, 'lastFireAt', expected, 'nextFireAt', proposed)
            redis.call('ZADD', KEYS[1], proposed, jobName)
        end
    end
    result[#result + 1] = code
    result[#result + 1] = messageId
end
return result
