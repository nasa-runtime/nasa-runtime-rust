-- 业务作用：提交普通 Run 或 Fanout shard 的当前 attempt 结果，根据重试上限进入下一可执行状态或终态。
-- KEYS（NORMAL）：Run HASH、leases ZSET、visible ZSET、waiting ZSET、running HASH、任务 waitq、completion Stream、Dispatch Stream、任务 HASH、schedule ZSET。
-- KEYS（FANOUT）：shard HASH、leases ZSET、ready ZSET、receipts ZSET、Fanout 根 HASH、roots/watch ZSET、completion Stream、GC ZSET。
-- ARGV[1..10] runId/shardRunId、owner、attemptToken、jobName/workerName、resultCode、resultSummary、maxAttempts、retryDelayMs、retentionMs、NORMAL/FANOUT；
-- FANOUT ARGV[11..13] fanoutId、seq、assignmentEpoch；NORMAL ARGV[14] 分片键前缀，FANOUT ARGV[14] 期望 executionKey。
-- 返回：STATE_MISMATCH、STALE_OWNER、STALE_ASSIGNMENT、IDENTITY_MISMATCH，或 {OK, nextState, redisNow, optionalWakeDelayMs}。
-- 安全不变量：owner 与 attemptToken 必须同时匹配；Fanout 还必须匹配 assignmentEpoch 与 executionKey；释放执行权、写结果与推进队首在同一 slot 内提交。

if ARGV[10] == 'FANOUT' then
    if redis.call('HGET', KEYS[1], 'state') ~= 'RUNNING' then return {'STATE_MISMATCH'} end
    if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2]
            or redis.call('HGET', KEYS[1], 'attemptToken') ~= ARGV[3] then return {'STALE_OWNER'} end
    if redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[13] then return {'STALE_ASSIGNMENT'} end
    -- executionKey 与 workerName 复验：完成结果只能记到调用方实际执行的那个分片身份上，
    -- 记录被错误改写时宁可拒绝完成并交给租约恢复，也不能把结果计入另一个分片的聚合计数。
    if redis.call('HGET', KEYS[1], 'executionKey') ~= ARGV[14]
            or redis.call('HGET', KEYS[1], 'workerName') ~= ARGV[4] then
        return {'IDENTITY_MISMATCH'}
    end
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)
    local member = ARGV[11] .. ':' .. ARGV[12]
    redis.call('ZREM', KEYS[2], member)
    redis.call('ZREM', KEYS[3], member)
    redis.call('ZREM', KEYS[4], member)
    local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
    local submittedResult = ARGV[5]
    -- 根已进入 CANCELLING 时不再接受 shard 的新重试，当前完成统一按取消收敛。
    if redis.call('HGET', KEYS[5], 'state') == 'CANCELLING' then submittedResult = 'CANCELLED' end
    if submittedResult == 'RETRY' and attempt < tonumber(ARGV[7]) then
        local startVisibleAt = now + tonumber(ARGV[8])
        redis.call('HSET', KEYS[1], 'state', 'RECEIVED', 'owner', '', 'leaseUntil', '',
                'resultCode', submittedResult, 'resultSummary', ARGV[6], 'startVisibleAt', startVisibleAt)
        redis.call('ZADD', KEYS[3], startVisibleAt, member)
        return {'OK', 'RECEIVED', tostring(now)}
    end
    local nextState = submittedResult == 'SUCCESS' and 'SUCCEEDED'
            or (submittedResult == 'CANCELLED' and 'CANCELLED'
            or (submittedResult == 'RETRY' and 'DEAD' or 'FAILED'))
    redis.call('HSET', KEYS[1], 'state', nextState, 'owner', '', 'leaseUntil', '',
            'finishedAt', now, 'resultCode', submittedResult, 'resultSummary', ARGV[6])
    -- 成功终态与"状态推进异常"标记不能同时为真：终态记录会在保留期内被审计读取，
    -- 残留的 errorType 会把成功分片当作失败统计。失败与取消终态保留最后一次异常归因。
    if nextState == 'SUCCEEDED' then redis.call('HDEL', KEYS[1], 'errorType') end
    local counter = nextState == 'SUCCEEDED' and 'successCount'
            or (nextState == 'FAILED' and 'failedCount'
            or (nextState == 'DEAD' and 'deadCount' or 'cancelledCount'))
    redis.call('HINCRBY', KEYS[5], counter, 1)
    local terminal = tonumber(redis.call('HGET', KEYS[5], 'successCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'failedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'deadCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'skippedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'cancelledCount') or '0')
    local total = tonumber(redis.call('HGET', KEYS[5], 'shardTotal') or '0')
    if terminal == total then
        -- 最后一个 shard 原子决定根终态，不依赖完成顺序。
        local failed = tonumber(redis.call('HGET', KEYS[5], 'failedCount') or '0')
                + tonumber(redis.call('HGET', KEYS[5], 'deadCount') or '0')
        local skipped = tonumber(redis.call('HGET', KEYS[5], 'skippedCount') or '0')
        local cancelled = tonumber(redis.call('HGET', KEYS[5], 'cancelledCount') or '0')
        local cancelling = redis.call('HGET', KEYS[5], 'state') == 'CANCELLING'
        local rootState = cancelling and 'CANCELLED' or (failed > 0 and 'FAILED'
                or (skipped > 0 and 'PARTIAL_FAILED'
                or (cancelled == total and 'CANCELLED' or 'SUCCEEDED')))
        local expireAt = now + tonumber(ARGV[9])
        redis.call('HSET', KEYS[5], 'state', rootState, 'finishedAt', now, 'expireAt', expireAt)
        redis.call('ZADD', KEYS[8], expireAt, ARGV[11])
        redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMPLETED',
                'fanoutId', ARGV[11], 'rootRunId', redis.call('HGET', KEYS[5], 'rootRunId'), 'state', rootState)
    end
    return {'OK', nextState, tostring(now)}
end

if redis.call('HGET', KEYS[1], 'state') ~= 'RUNNING' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] or redis.call('HGET', KEYS[1], 'attemptToken') ~= ARGV[3] then
    return {'STALE_OWNER'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local wakeDelayMs = -1
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[6], ARGV[1])
local releasedSlot = false
if redis.call('HGET', KEYS[5], ARGV[4]) == ARGV[1] then
    redis.call('HDEL', KEYS[5], ARGV[4])
    releasedSlot = true
end

local result = ARGV[5]
local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
local nextState
if result == 'SUCCESS' then nextState = 'SUCCEEDED'
elseif result == 'CANCELLED' then nextState = 'CANCELLED'
elseif result == 'RETRY' and attempt < tonumber(ARGV[7]) then nextState = 'RETRY_WAIT'
elseif result == 'RETRY' then nextState = 'DEAD'
else nextState = 'FAILED' end

-- 删除 fence：任务已删除（或 Run 落在删除修订 fence 内）时不再建立新 attempt。
-- RETRY 与协作式 CANCELLED 都按删除终态归因，避免删除后才被拒绝的 Fanout 创建丢失删除原因；
-- 已经完成的 SUCCESS、失败与超时仍保留当前 attempt 的真实执行结果。
-- 该约束同时保证删除收敛期间任务索引成员只减不增，后台 reaper 才能安全判定归零。
local jobState = redis.call('HGET', KEYS[9], 'state')
local deletedRevision = tonumber(redis.call('HGET', KEYS[9], 'deletedRevision') or '-1')
local runRevision = tonumber(redis.call('HGET', KEYS[1], 'definitionRevision') or '0')
local deletedByFence = jobState == 'DELETED' or runRevision <= deletedRevision
if deletedByFence and (nextState == 'RETRY_WAIT' or result == 'CANCELLED') then
    nextState = 'CANCELLED'
    result = 'JOB_DELETED'
end
redis.call('HSET', KEYS[1], 'state', nextState, 'resultCode', result,
        'resultSummary', ARGV[6], 'finishedAt', now, 'owner', '', 'leaseUntil', '')
-- 与 FANOUT 分支同一条不变量：成功终态清除状态推进异常标记，失败与取消保留归因。
if nextState == 'SUCCEEDED' then redis.call('HDEL', KEYS[1], 'errorType') end
if nextState == 'RETRY_WAIT' then
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[1])
    wakeDelayMs = tonumber(ARGV[8])
else
    redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1], 'state', nextState, 'resultCode', result)
    redis.call('PEXPIRE', KEYS[1], ARGV[9])
    -- 固定延迟重排前必须复验定义未换代：Run 携带的 definitionRevision 与当前定义相等、
    -- 定义仍为 ENABLED 且仍声明 FIXED_DELAY。缺任一条件时，旧执行实例会取得新定义的调度写权，
    -- 把已改为 FIXED_RATE/CRON 的定义拉回旧固定延迟节奏，覆盖新修订已计算的调度时刻。
    if redis.call('HGET', KEYS[1], 'triggerType') == 'FIXED_DELAY'
            and redis.call('HGET', KEYS[9], 'state') == 'ENABLED'
            and redis.call('HGET', KEYS[9], 'scheduleType') == 'FIXED_DELAY'
            and redis.call('HGET', KEYS[1], 'definitionRevision') == redis.call('HGET', KEYS[9], 'definitionRevision') then
        local nextFireAt = now + tonumber(redis.call('HGET', KEYS[9], 'intervalMs') or '0')
        redis.call('HSET', KEYS[9], 'lastFireAt', redis.call('HGET', KEYS[1], 'logicalFireAt'),
                'nextFireAt', nextFireAt)
        redis.call('ZADD', KEYS[10], nextFireAt, ARGV[4])
    end
end
if releasedSlot then
    -- 只在确认当前 Run 仍占有串行槽时处理下一队首，避免迟到完成重复推进。删除 fence 内的
    -- BLOCKED 成员在这里直接终态化；任务已用更高修订号重建时继续越过旧成员，直到开放首个
    -- fence 外成员，否则空出的串行槽会让新修订积压永久失去唤醒者。
    local prefix = ARGV[14]
    local handoffComplete = false
    for index = 1, 100 do
        local nextEntry = redis.call('ZPOPMIN', KEYS[6], 1)
        if not nextEntry[1] then
            handoffComplete = true
            break
        end
        local nextRunKey = prefix .. 'run:' .. nextEntry[1]
        if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
            local nextRevision = tonumber(redis.call('HGET', nextRunKey, 'definitionRevision') or '0')
            if jobState == 'DELETED' or nextRevision <= deletedRevision then
                redis.call('HSET', nextRunKey, 'state', 'CANCELLED', 'finishedAt', now,
                        'resultCode', 'JOB_DELETED')
                redis.call('ZREM', KEYS[2], nextEntry[1])
                redis.call('ZREM', KEYS[3], nextEntry[1])
                redis.call('ZREM', KEYS[4], nextEntry[1])
                redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*',
                        'runId', nextEntry[1], 'state', 'CANCELLED', 'resultCode', 'JOB_DELETED')
                redis.call('PEXPIRE', nextRunKey, ARGV[9])
            else
                -- 合法队首先回到统一可见索引，任务暂停门禁和正常派发都由 promoter 复验。
                local visibleAt = now
                redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', visibleAt)
                redis.call('ZADD', KEYS[3], visibleAt, nextEntry[1])
                wakeDelayMs = 0
                handoffComplete = true
                break
            end
        end
    end
    -- 单次完成脚本最多处理固定数量的队首；余量登记给有界 reaper，避免积压规模放大原子脚本耗时。
    if not handoffComplete and redis.call('ZCARD', KEYS[6]) > 0 then
        redis.call('SADD', prefix .. 'reaping', ARGV[4])
    end
end
return {'OK', nextState, tostring(now), tostring(wakeDelayMs)}
