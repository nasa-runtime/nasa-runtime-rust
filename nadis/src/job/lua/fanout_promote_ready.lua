-- 业务作用：重新唤醒已持久接收但尚未取得执行权的 Fanout shard。
-- KEYS[1] shard HASH；KEYS[2] ready ZSET；KEYS[3] 目标通知 channel。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] assignmentEpoch；ARGV[4] 普通唤醒上限；ARGV[5] 基础延迟毫秒；
-- ARGV[6] PUBLISH 或 SPUBLISH；ARGV[7] failurePolicy；ARGV[8] STRICT_SNAPSHOT 最大退避毫秒。
-- 返回：STALE、STALE_ASSIGNMENT、{NOT_DUE, visibleAt}、{CAPACITY_EXHAUSTED/WAKEUP_EXHAUSTED, count}
-- 或 {OK, count, nextVisibleAt}。
-- 安全不变量：只唤醒当前 assignment；容量窗口与无响应唤醒使用独立出口；STRICT_SNAPSHOT 不换目标，
-- 超过普通上限后使用有界指数退避。

if redis.call('HGET', KEYS[1], 'state') ~= 'RECEIVED' then
    redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
    return {'STALE'}
end
if redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[3] then return {'STALE_ASSIGNMENT'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local visibleAt = tonumber(redis.call('HGET', KEYS[1], 'startVisibleAt') or '0')
if visibleAt > now then return {'NOT_DUE', tostring(visibleAt)} end
local wakeups = tonumber(redis.call('HGET', KEYS[1], 'readyWakeupCount') or '0')
local strict = ARGV[7] == 'STRICT_SNAPSHOT'
if wakeups >= tonumber(ARGV[4]) and not strict then
    -- 本地槽位不足是节点在线的确定证据，不能借无响应出口累计跨根失联证据。
    if tonumber(redis.call('HGET', KEYS[1], 'capacityDeferredAt') or '0') > 0 then
        return {'CAPACITY_EXHAUSTED', tostring(wakeups)}
    end
    return {'WAKEUP_EXHAUSTED', tostring(wakeups)}
end
wakeups = redis.call('HINCRBY', KEYS[1], 'readyWakeupCount', 1)
local delay = tonumber(ARGV[5])
if strict and wakeups > tonumber(ARGV[4]) then
    -- 严格快照必须保留原目标，通过有界退避避免持续空转。
    local exponent = math.min(wakeups - tonumber(ARGV[4]), 16)
    delay = math.min(delay * (2 ^ exponent), tonumber(ARGV[8]))
end
visibleAt = now + delay
redis.call('HSET', KEYS[1], 'startVisibleAt', visibleAt)
redis.call('ZADD', KEYS[2], visibleAt, ARGV[1] .. ':' .. ARGV[2])
redis.call(ARGV[6], KEYS[3], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3]
        .. '|' .. (redis.call('HGET', KEYS[1], 'inboxMessageId') or ''))
return {'OK', tostring(wakeups), tostring(visibleAt)}
