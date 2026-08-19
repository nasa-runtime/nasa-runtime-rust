-- 业务作用：分批收敛已删除任务的全部存量（waitq、visible、leases、waiting），使删除动作保持 O(1)
-- 而收敛不依赖本地定义仍然存在——删除分片最后一个定义后，常规索引扫描不再覆盖该分片，
-- 这里是这些存量唯一的推进者。
-- KEYS[1] 分片 reaping SET；KEYS[2] visible ZSET；KEYS[3] leases ZSET；KEYS[4] completion Stream。
-- ARGV[1] 单批上限；ARGV[2] 终态保留期毫秒；ARGV[3] 调度分片键前缀。
-- 返回：{jobName 或空串, reapedCount, active, runId, fanoutId, rootAttempt, ...}；active=1 表示该任务仍有待推进存量；
-- 尾部三元组是已到等待截止点、需跨 slot 协作式取消的 Fanout 根。
-- 安全不变量：只终态化删除 fence 内（definitionRevision <= deletedRevision）的成员；未到期租约按
-- "允许当前 attempt 完成"保留。共享索引用 ZSCAN 游标跨轮接力，游标与周期结果持久在任务 HASH 上，
-- 前 N 个外部成员不会永久遮挡目标；完整周期无 pending 才判定该索引 clean。所有状态入口与
-- 串行槽移交都在写回索引前复验删除 fence，因此收敛期间旧任务的索引成员只减不增。reaping 标记只有在 waitq
-- 旧存量清完且三个索引都 clean 后才撤销——它是 tombstone 清理的唯一存量门禁。

local jobName = redis.call('SRANDMEMBER', KEYS[1])
if not jobName then return {'', '0', '0'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local prefix = ARGV[3]
local jobKey = prefix .. 'job:' .. jobName
local waitqKey = prefix .. 'waitq:' .. jobName
local waitingKey = prefix .. 'waiting'
local runningKey = prefix .. 'running'
local jobState = redis.call('HGET', jobKey, 'state')
local deletedRevision = tonumber(redis.call('HGET', jobKey, 'deletedRevision') or '-1')
local batch = tonumber(ARGV[1])
local reaped = 0
local pendingRound = 0
local fanoutRequests = {}

local function usedBudget()
    return reaped + math.floor(#fanoutRequests / 3)
end

local function fenced(runKey)
    return jobState == 'DELETED'
            or tonumber(redis.call('HGET', runKey, 'definitionRevision') or '0') <= deletedRevision
end

local function terminalState(state)
    return state == 'SUCCEEDED' or state == 'FAILED' or state == 'DEAD'
            or state == 'SKIPPED' or state == 'CANCELLED'
end

local handoff

local function terminate(runId, runKey, expiredLease)
    redis.call('HSET', runKey, 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'JOB_DELETED')
    -- 租约到期后被撤权的 attempt 保留 LEASE_EXPIRED 归因：它属于"从未成功完成"的推进异常
    if expiredLease then redis.call('HSET', runKey, 'errorType', 'LEASE_EXPIRED') end
    redis.call('ZREM', KEYS[2], runId)
    redis.call('ZREM', KEYS[3], runId)
    redis.call('ZREM', waitingKey, runId)
    -- 崩溃的 owner 可能仍占串行槽；撤权后必须在同一原子段内把槽位交给
    -- 第一个 fence 外的 BLOCKED 成员，否则新修订号的执行实例会永久等待。
    local releasedSlot = false
    if redis.call('HGET', runningKey, jobName) == runId then
        redis.call('HDEL', runningKey, jobName)
        releasedSlot = true
    end
    redis.call('XADD', KEYS[4], 'MAXLEN', '~', 100000, '*',
            'runId', runId, 'state', 'CANCELLED', 'resultCode', 'JOB_DELETED')
    redis.call('PEXPIRE', runKey, tonumber(ARGV[2]))
    reaped = reaped + 1
    if releasedSlot and handoff then handoff() end
end

-- 槽位移交与本轮收敛共用 batch 预算，且单次最多扫过 100 个队首。旧成员终态化后
-- 继续向后寻找；遇到新修订号成员时，只在槽位仍空闲时转为 QUEUED。
handoff = function()
    local remaining = batch - usedBudget()
    if remaining <= 0 then return false end
    local limit = math.min(100, remaining)
    for index = 1, limit do
        local entry = redis.call('ZPOPMIN', waitqKey, 1)
        if not entry[1] then return true end
        local runId = entry[1]
        local runKey = prefix .. 'run:' .. runId
        if redis.call('HGET', runKey, 'state') == 'BLOCKED' then
            if fenced(runKey) then
                terminate(runId, runKey, false)
            elseif not redis.call('HGET', runningKey, jobName) then
                redis.call('HSET', runKey, 'state', 'QUEUED', 'nextVisibleAt', now)
                redis.call('ZADD', KEYS[2], now, runId)
                return true
            else
                redis.call('ZADD', waitqKey, entry[2], runId)
                return true
            end
        end
        if reaped >= batch then break end
    end
    return redis.call('ZCARD', waitqKey) == 0
end

-- 第一段：waitq。成员按逻辑时刻有序，旧存量必然先出队；fence 外成员说明该任务已被更高修订号
-- 重新登记且旧存量已清完，放回并视为本段收敛完成。
local waitqClean = false
for index = 1, batch do
    if usedBudget() >= batch then break end
    local entry = redis.call('ZPOPMIN', waitqKey, 1)
    if not entry[1] then
        waitqClean = true
        break
    end
    local runKey = prefix .. 'run:' .. entry[1]
    if not fenced(runKey) then
        -- 旧 owner 已被收敛且槽位空闲时，reaper 承担最后一次队首唤醒；
        -- 槽位仍有主时保留原顺序，由真正的 owner 结束后移交。
        if redis.call('HGET', runKey, 'state') == 'BLOCKED'
                and not redis.call('HGET', runningKey, jobName) then
            redis.call('HSET', runKey, 'state', 'QUEUED', 'nextVisibleAt', now)
            redis.call('ZADD', KEYS[2], now, entry[1])
        else
            redis.call('ZADD', waitqKey, entry[2], entry[1])
        end
        waitqClean = true
        break
    end
    local state = redis.call('HGET', runKey, 'state')
    if state == 'BLOCKED' or state == 'QUEUED' or state == 'RETRY_WAIT' then
        terminate(entry[1], runKey, false)
    end
end
if not waitqClean and redis.call('ZCARD', waitqKey) == 0 then waitqClean = true end

-- 共享索引的游标接力扫描：游标与周期 pending 标志持久在任务 HASH，跨轮从上次位置继续；
-- ZSCAN 保证全程存在的成员至少返回一次，配合"成员只减不增"的 fence 不变量，
-- 完整周期（游标回 0）无 pending 即可判定该索引对本任务归零。
local function scanIndex(zsetKey, cursorField, dirtyField, cleanField, mode)
    local cursor = redis.call('HGET', jobKey, cursorField) or '0'
    for iteration = 1, 8 do
        if usedBudget() >= batch then break end
        local scan = redis.call('ZSCAN', zsetKey, cursor, 'COUNT', 500)
        cursor = scan[1]
        local members = scan[2]
        local pageComplete = true
        for index = 1, #members, 2 do
            if usedBudget() >= batch then
                pageComplete = false
                break
            end
            local runId = members[index]
            local score = tonumber(members[index + 1])
            local runKey = prefix .. 'run:' .. runId
            if mode == 'waiting' and redis.call('EXISTS', runKey) == 0 then
                -- 对账索引指向已消失 Run 的残留成员直接清除
                redis.call('ZREM', zsetKey, runId)
            elseif redis.call('HGET', runKey, 'jobName') == jobName and fenced(runKey) then
                local state = redis.call('HGET', runKey, 'state')
                if mode == 'leases' then
                    if score <= now then
                        terminate(runId, runKey, true)
                    else
                        -- 未到期租约按"允许当前 attempt 完成"保留，本周期不得判定归零
                        redis.call('HSET', jobKey, dirtyField, '1')
                        pendingRound = pendingRound + 1
                    end
                elseif state == 'QUEUED' or state == 'RETRY_WAIT' or state == 'BLOCKED' then
                    terminate(runId, runKey, false)
                elseif mode == 'waiting' and (state == 'FANOUT_CREATING' or state == 'WAITING_CHILDREN') then
                    if score <= now then
                        local fanoutId = redis.call('HGET', runKey, 'fanoutId') or ''
                        if fanoutId == '' then
                            -- 缺少跨 slot 定位证据时已无可取消的桶，截止点后直接收敛普通根。
                            terminate(runId, runKey, false)
                        else
                            -- Lua 不能在 Cluster 中跨 slot 改写 Fanout 桶；保留 waiting 与 reaping 门禁，
                            -- 由运行时 control loop 先关闭桶内新 attempt，桶终态后再幂等回填普通根。
                            table.insert(fanoutRequests, runId)
                            table.insert(fanoutRequests, fanoutId)
                            table.insert(fanoutRequests, redis.call('HGET', runKey, 'rootAttempt') or '0')
                            redis.call('HSET', jobKey, dirtyField, '1')
                            pendingRound = pendingRound + 1
                        end
                    else
                        -- 在飞 Fanout 在持久截止点前仍可自然完成，删除不伪造已停止事实。
                        redis.call('HSET', jobKey, dirtyField, '1')
                        pendingRound = pendingRound + 1
                    end
                elseif not state or terminalState(state) then
                    redis.call('ZREM', zsetKey, runId)
                else
                    redis.call('HSET', jobKey, dirtyField, '1')
                    pendingRound = pendingRound + 1
                end
            end
        end
        if not pageComplete then
            -- ZSCAN 的 COUNT 是返回量提示而不是硬上限；页内到达终态化预算后立即停止。
            -- 本页未处理的成员可能已被游标跨过，因此本周期必须记为未完成，等游标回到
            -- 起点后再用新周期覆盖，不得沿用 clean 结论撤销收敛门禁。
            redis.call('HSET', jobKey, dirtyField, '1', cleanField, '0')
            break
        elseif cursor == '0' then
            -- 周期结算：整周期未出现 pending 即 clean；被终态化的成员已经离开索引，不计入
            redis.call('HSET', jobKey, cleanField,
                    (redis.call('HGET', jobKey, dirtyField) or '0') == '0' and '1' or '0')
            redis.call('HDEL', jobKey, dirtyField)
            break
        end
    end
    redis.call('HSET', jobKey, cursorField, cursor)
    return cursor
end

scanIndex(KEYS[2], 'reapCursorVisible', 'reapDirtyVisible', 'reapCleanVisible', 'visible')
scanIndex(KEYS[3], 'reapCursorLeases', 'reapDirtyLeases', 'reapCleanLeases', 'leases')
scanIndex(waitingKey, 'reapCursorWaiting', 'reapDirtyWaiting', 'reapCleanWaiting', 'waiting')

local allClean = waitqClean
        and (redis.call('HGET', jobKey, 'reapCleanVisible') or '0') == '1'
        and (redis.call('HGET', jobKey, 'reapCleanLeases') or '0') == '1'
        and (redis.call('HGET', jobKey, 'reapCleanWaiting') or '0') == '1'
if allClean then
    redis.call('SREM', KEYS[1], jobName)
    redis.call('HDEL', jobKey, 'reapCursorVisible', 'reapCursorLeases', 'reapCursorWaiting',
            'reapCleanVisible', 'reapCleanLeases', 'reapCleanWaiting',
            'reapDirtyVisible', 'reapDirtyLeases', 'reapDirtyWaiting')
end
local active = (not allClean or reaped > 0 or pendingRound > 0) and '1' or '0'
local response = {jobName, tostring(reaped), active}
for _, value in ipairs(fanoutRequests) do table.insert(response, value) end
return response
