-- 业务作用：续期执行器及其全部 Worker 能力，并发布当前容量和 Fanout 就绪状态。
-- KEYS[1] 执行器存活 ZSET；KEYS[2] 执行器 HASH；KEYS[3] 节点失联证据 HASH；KEYS[4..n] Worker 能力 ZSET。
-- ARGV[1] executorId；ARGV[2] 请求状态；ARGV[3] 失效时长毫秒；ARGV[4] inflight；ARGV[5] 请求的 fanoutReady；
-- ARGV[6] 可选逻辑请求 ID；ARGV[7] 可选的已确认 heartbeatRevision。
-- 返回：NOT_FOUND、{STALE_AUTHORITY, currentRevision}，或 {OK, redisNow, expireAt, heartbeatRevision}。
-- 安全不变量：已过期但尚未 GC 的记录不能重新续租；同一逻辑请求只推进一次 revision；
-- FANOUT_UNREADY 在冷却截止前不能被普通心跳覆盖，避免已被多个根证实失联的节点立即重新进入快照。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if redis.call('EXISTS', KEYS[2]) == 0 then return {'NOT_FOUND'} end
local currentExpireAt = tonumber(redis.call('HGET', KEYS[2], 'expireAt') or '0')
if currentExpireAt <= now then return {'NOT_FOUND'} end
local currentRevision = tonumber(redis.call('HGET', KEYS[2], 'heartbeatRevision') or '0')
local requestId = ARGV[6] or ''
if requestId ~= '' and redis.call('HGET', KEYS[2], 'heartbeatRequestId') == requestId then
    return {'OK', tostring(now), tostring(currentExpireAt), tostring(currentRevision)}
end
if ARGV[7] and ARGV[7] ~= '' and currentRevision ~= tonumber(ARGV[7]) then
    return {'STALE_AUTHORITY', tostring(currentRevision)}
end
local expireAt = now + tonumber(ARGV[3])
local revision = redis.call('HINCRBY', KEYS[2], 'heartbeatRevision', 1)
local requestedState = ARGV[2]
local requestedReady = ARGV[5]
if redis.call('HGET', KEYS[2], 'state') == 'FANOUT_UNREADY' then
    -- 冷却期内保持保护态；只有服务端时间越过截止点才清除证据。
    local unreadyUntil = tonumber(redis.call('HGET', KEYS[2], 'fanoutUnreadyUntil') or '0')
    if unreadyUntil > now then
        requestedState = 'FANOUT_UNREADY'
        requestedReady = 'false'
    else
        redis.call('DEL', KEYS[3])
        redis.call('HDEL', KEYS[2], 'fanoutUnreadyUntil')
    end
end
redis.call('HSET', KEYS[2], 'heartbeatAt', now, 'expireAt', expireAt,
        'state', requestedState, 'inflight', ARGV[4], 'fanoutReady', requestedReady)
if requestId ~= '' then redis.call('HSET', KEYS[2], 'heartbeatRequestId', requestId) end
redis.call('ZADD', KEYS[1], expireAt, ARGV[1])
for index = 4, #KEYS do redis.call('ZADD', KEYS[index], expireAt, ARGV[1]) end
return {'OK', tostring(now), tostring(expireAt), tostring(revision)}
