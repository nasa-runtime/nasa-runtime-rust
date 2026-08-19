-- 业务作用：撤销旧 Fanout assignment 的 inbox 与期限索引，然后在保持 seq/executionKey 不变的前提下重建目标代次。
-- KEYS[1] shard HASH；KEYS[2] receipts ZSET；KEYS[3] ready ZSET；KEYS[4] leases ZSET；KEYS[5] 旧目标 inbox Stream。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] 旧 targetNodeIdentity；ARGV[4] 预期 assignmentEpoch；
-- ARGV[5..7] 新 targetNodeIdentity、startupId、heartbeatRevision；ARGV[8] inbox 消费组；ARGV[9] assignment 上限；
-- ARGV[10] FAILURE、CAPACITY 或 CAPACITY_TARGET_LOST；ARGV[11] 单次容量等待窗口毫秒数。
-- 返回：BUSY、STALE_ASSIGNMENT、STALE_CAPACITY、CAPACITY_ROUTE_EXHAUSTED、NO_CAPABLE_EXECUTOR，或 {OK, nextEpoch}。
-- 安全不变量：RUNNING shard 不允许直接换目标；旧消息与所有截止索引先在同一原子操作中撤销，才增加 assignmentEpoch。
-- 同一稳定节点出现新存活证据后的恢复只推进 assignmentEpoch；故障换节点与容量路由使用独立配额，
-- 容量路由按“有界突发 + 静默窗口”限频；目标离开存活快照时只允许有容量证据的裁决，
-- 故障计数已满的兼容出口在 shard 生命周期内至多使用一次。

local state = redis.call('HGET', KEYS[1], 'state')
if state == 'RUNNING' then return {'BUSY'} end
if redis.call('HGET', KEYS[1], 'targetNodeIdentity') ~= ARGV[3]
        or redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[4] then return {'STALE_ASSIGNMENT'} end
local assignmentCount = tonumber(redis.call('HGET', KEYS[1], 'assignmentCount') or '1')
local capacityRouteCount = tonumber(redis.call('HGET', KEYS[1], 'capacityRouteCount') or '0')
-- 累计值不参与路由门禁，跨静默窗口保留，供运维区分“当前窗口频率”与“分片生命周期总迁移量”。
local capacityRouteTotal = tonumber(redis.call('HGET', KEYS[1], 'capacityRouteTotal') or '0')
local maxAssignments = tonumber(ARGV[9])
local changesTarget = ARGV[5] ~= '' and ARGV[5] ~= ARGV[3]
local capacityRoute = ARGV[10] == 'CAPACITY'
local capacityTargetLost = ARGV[10] == 'CAPACITY_TARGET_LOST'
local capacityEvidence = redis.call('HGET', KEYS[1], 'capacityDeferredAt')
local capacityTargetLostEscapeUsed = redis.call('HGET', KEYS[1], 'capacityTargetLostEscapeUsed') == '1'
local oldMessageId = redis.call('HGET', KEYS[1], 'inboxMessageId')
-- 容量目标失联是容量等待状态的后继裁决；没有原子证据时拒绝外部伪造该兼容出口。
if capacityTargetLost and not capacityEvidence then return {'STALE_CAPACITY'} end
-- 容量探测允许有界突发；额度耗尽后至少静默 assignment 上限个容量窗口，再开放下一轮探测。
-- 未传容量窗口的旧调用保持生命周期总上限，不会因缺省参数意外放开频率门禁。
if capacityRoute then
    local capacityRouteLimit = maxAssignments - 1
    if ARGV[5] == '' or capacityRouteLimit <= 0 then return {'CAPACITY_ROUTE_EXHAUSTED'} end
    if capacityRouteCount >= capacityRouteLimit then
        local time = redis.call('TIME')
        local now = time[1] * 1000 + math.floor(time[2] / 1000)
        local blockedAt = tonumber(redis.call('HGET', KEYS[1], 'capacityRouteBlockedAt') or '0')
        if blockedAt == 0 then
            redis.call('HSET', KEYS[1], 'capacityRouteBlockedAt', now)
            return {'CAPACITY_ROUTE_EXHAUSTED'}
        end
        local resetAfterMs = (tonumber(ARGV[11]) or 0) * maxAssignments
        if resetAfterMs <= 0 or now - blockedAt < resetAfterMs then
            return {'CAPACITY_ROUTE_EXHAUSTED'}
        end
        capacityRouteCount = 0
        redis.call('HDEL', KEYS[1], 'capacityRouteCount', 'capacityRouteBlockedAt')
    end
end
-- 新 assignment 必须重新累计自己的容量窗口，不能继承旧目标的背压时长。
redis.call('HDEL', KEYS[1], 'capacityDeferredAt')
-- 旧 inbox 必须与 assignment 切换同时撤销，否则旧节点可能在新代次已发布后再次接收。
if oldMessageId and oldMessageId ~= '' then
    redis.pcall('XACK', KEYS[5], ARGV[8], oldMessageId)
    redis.call('XDEL', KEYS[5], oldMessageId)
end
-- 故障计数已满时只给带容量证据的存量形态一次退出机会；使用标记持久保留，后续抖动仍受上限约束。
local capacityTargetLostEscape = capacityTargetLost and changesTarget
        and assignmentCount >= maxAssignments and not capacityTargetLostEscapeUsed
if (changesTarget and not capacityRoute and assignmentCount >= maxAssignments
        and not capacityTargetLostEscape) or ARGV[5] == '' then
    redis.call('HDEL', KEYS[1], 'capacityRouteCount', 'capacityRouteBlockedAt')
    redis.call('HSET', KEYS[1], 'state', 'AWAITING_CAPABILITY', 'errorType', 'NO_CAPABLE_EXECUTOR',
            'owner', '', 'attemptToken', '', 'leaseUntil', '', 'inboxMessageId', '',
            'receivedByExecutorId', '', 'receivedAt', '', 'receiptDeadlineAt', '', 'startVisibleAt', '')
    redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
    redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
    redis.call('ZREM', KEYS[4], ARGV[1] .. ':' .. ARGV[2])
    return {'NO_CAPABLE_EXECUTOR'}
end
local nextEpoch = tonumber(ARGV[4]) + 1
local nextAssignmentCount = assignmentCount
if changesTarget and not capacityRoute and assignmentCount < maxAssignments then
    nextAssignmentCount = assignmentCount + 1
end
if capacityTargetLostEscape then
    redis.call('HSET', KEYS[1], 'capacityTargetLostEscapeUsed', 1)
end
if capacityRoute then
    capacityRouteCount = capacityRouteCount + 1
    capacityRouteTotal = capacityRouteTotal + 1
    redis.call('HSET', KEYS[1], 'capacityRouteCount', capacityRouteCount)
    redis.call('HSET', KEYS[1], 'capacityRouteTotal', capacityRouteTotal)
    if capacityRouteCount >= maxAssignments - 1 then
        local time = redis.call('TIME')
        local now = time[1] * 1000 + math.floor(time[2] / 1000)
        redis.call('HSET', KEYS[1], 'capacityRouteBlockedAt', now)
    else
        redis.call('HDEL', KEYS[1], 'capacityRouteBlockedAt')
    end
else
    -- 故障恢复建立新的目标语义，后续容量探测从独立预算起点重新累计。
    redis.call('HDEL', KEYS[1], 'capacityRouteCount', 'capacityRouteBlockedAt')
end
redis.call('HSET', KEYS[1], 'state', 'AWAITING_RECEIPT',
        'targetNodeIdentity', ARGV[5], 'targetStartupId', ARGV[6],
        'targetHeartbeatRevision', ARGV[7],
        'assignmentEpoch', nextEpoch, 'assignmentCount', nextAssignmentCount,
        'receiptRetryCount', 0, 'readyWakeupCount', 0, 'owner', '',
        'attemptToken', '', 'leaseUntil', '', 'inboxMessageId', '',
        'receivedByExecutorId', '', 'receivedAt', '', 'receiptDeadlineAt', '', 'startVisibleAt', '')
redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZREM', KEYS[4], ARGV[1] .. ':' .. ARGV[2])
return {'OK', tostring(nextEpoch)}
