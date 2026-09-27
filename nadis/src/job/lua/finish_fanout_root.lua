-- 业务作用：把桶内 Fanout 的 COMMITTED 或终态幂等回填到普通根 Run，并在终态时释放串行槽。
-- KEYS[1] 普通根 Run HASH；KEYS[2] waiting ZSET；KEYS[3] running HASH；KEYS[4] visible ZSET；KEYS[5] 任务 waitq；KEYS[6] completion Stream。
-- ARGV[1] rootRunId；ARGV[2] fanoutId；ARGV[3] rootAttempt；ARGV[4] COMMITTED 或桶内终态；ARGV[5] errorType；
-- ARGV[6] 根最大等待毫秒；ARGV[7] resultSummary；ARGV[8] rootJobName；ARGV[9] Run 保留期毫秒；
-- ARGV[10] 可见性延迟（当前队首直接唤醒分支不使用）；ARGV[11] 分片键前缀。
-- 返回：STALE、{ALREADY_COMPLETED, state}、STATE_MISMATCH，或 {OK, state, redisNowOrDeadline, wakeDelayMs}。
-- 安全不变量：fanoutId 与 rootAttempt 必须匹配；COMMITTED 只转入 WAITING_CHILDREN；根终态持久后才释放槽并唤醒下一队首。

if redis.call('HGET', KEYS[1], 'fanoutId') ~= ARGV[2]
        or redis.call('HGET', KEYS[1], 'rootAttempt') ~= ARGV[3] then return {'STALE'} end
local current = redis.call('HGET', KEYS[1], 'state')
if current == 'SUCCEEDED' or current == 'FAILED' or current == 'CANCELLED' then return {'ALREADY_COMPLETED', current} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local prefix = ARGV[11]
local jobKey = prefix .. 'job:' .. ARGV[8]
local jobState = redis.call('HGET', jobKey, 'state')
local deletedRevision = tonumber(redis.call('HGET', jobKey, 'deletedRevision') or '-1')
local runRevision = tonumber(redis.call('HGET', KEYS[1], 'definitionRevision') or '0')
local deletedByFence = jobState == 'DELETED' or runRevision <= deletedRevision
if ARGV[4] == 'COMMITTED' then
    if current == 'WAITING_CHILDREN' then return {'ADOPTED'} end
    if current ~= 'FANOUT_CREATING' then return {'STATE_MISMATCH'} end
    if deletedByFence then
        -- 删除后不再延长普通根的等待窗口；保留创建截止点与 waiting 成员，
        -- 由删除收敛驱动器在截止点后先关闭桶内新执行权，再收尾普通根。
        return {'JOB_DELETED', current, redis.call('HGET', KEYS[1], 'createDeadlineAt') or '0', '-1'}
    end
    local deadline = now + tonumber(ARGV[6])
    redis.call('HSET', KEYS[1], 'state', 'WAITING_CHILDREN', 'owner', '', 'waitDeadlineAt', deadline)
    redis.call('ZADD', KEYS[2], deadline, ARGV[1])
    return {'OK', 'WAITING_CHILDREN', tostring(deadline), '-1'}
end
if current ~= 'WAITING_CHILDREN' and current ~= 'FANOUT_CREATING' then return {'STATE_MISMATCH'} end
local rootState = ARGV[4] == 'SUCCEEDED' and 'SUCCEEDED'
        or (ARGV[4] == 'CANCELLED' and ARGV[5] ~= 'WAIT_TIMEOUT' and 'CANCELLED' or 'FAILED')
local rootResult = ARGV[5] == 'JOB_DELETED' and 'JOB_DELETED' or ARGV[4]
redis.call('HSET', KEYS[1], 'state', rootState, 'finishedAt', now,
        'resultCode', rootResult, 'errorType', ARGV[5], 'resultSummary', ARGV[7])
redis.call('ZREM', KEYS[2], ARGV[1])
local releasedSlot = false
if redis.call('HGET', KEYS[3], ARGV[8]) == ARGV[1] then
    redis.call('HDEL', KEYS[3], ARGV[8])
    releasedSlot = true
end
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('XADD', KEYS[6], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1], 'state', rootState, 'resultCode', rootResult)
redis.call('PEXPIRE', KEYS[1], ARGV[9])
if releasedSlot then
    -- 串行交接先复验删除 fence；单次最多处理固定数量，余量由 reaper 继续推进。
    local handoffComplete = false
    for index = 1, 100 do
        local nextEntry = redis.call('ZPOPMIN', KEYS[5], 1)
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
                redis.call('ZREM', KEYS[4], nextEntry[1])
                redis.call('XADD', KEYS[6], 'MAXLEN', '~', 100000, '*',
                        'runId', nextEntry[1], 'state', 'CANCELLED', 'resultCode', 'JOB_DELETED')
                redis.call('PEXPIRE', nextRunKey, ARGV[9])
            else
                redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', now)
                redis.call('ZADD', KEYS[4], now, nextEntry[1])
                handoffComplete = true
                return {'OK', rootState, tostring(now), '0'}
            end
        end
    end
    if not handoffComplete and redis.call('ZCARD', KEYS[5]) > 0 then
        redis.call('SADD', prefix .. 'reaping', ARGV[8])
    end
end
return {'OK', rootState, tostring(now), '-1'}
