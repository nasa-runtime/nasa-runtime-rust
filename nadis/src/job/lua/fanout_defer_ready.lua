-- 业务作用：目标节点明确在线但本地执行槽暂满时，保留当前 Fanout assignment 并延后启动可见时刻。
-- KEYS[1] shard HASH；KEYS[2] ready ZSET；KEYS[3] Fanout 根 HASH。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] targetNodeIdentity；ARGV[4] assignmentEpoch；ARGV[5] 延迟毫秒；
-- ARGV[6] 连续容量等待上限毫秒；ARGV[7] 普通唤醒上限；ARGV[8] CONTINUE 或 REOPEN。
-- 返回：STALE、STALE_ASSIGNMENT、{CAPACITY_EXHAUSTED, firstDeferredAt, elapsedMs}，或 {DEFERRED, nextVisibleAt}。
-- 安全不变量：只有当前目标、当前代次的 RECEIVED 分片可延后；短时容量背压不累计失联唤醒证据，
-- 非严格策略连续超窗后只开放容量路由裁决；严格快照保持固定目标与既有指数退避。

if redis.call('HGET', KEYS[1], 'state') ~= 'RECEIVED' then return {'STALE'} end
if redis.call('HGET', KEYS[1], 'targetNodeIdentity') ~= ARGV[3]
        or redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[4] then
    return {'STALE_ASSIGNMENT'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local firstDeferredAt = tonumber(redis.call('HGET', KEYS[1], 'capacityDeferredAt') or '0')
-- 没有其它兼容目标时由监视器开启下一段等待窗口，保持当前 assignment 而不进入能力缺失态。
if ARGV[8] == 'REOPEN' or firstDeferredAt == 0 then
    firstDeferredAt = now
    redis.call('HSET', KEYS[1], 'capacityDeferredAt', firstDeferredAt)
end
local elapsed = now - firstDeferredAt
if elapsed >= tonumber(ARGV[6]) then
    if redis.call('HGET', KEYS[3], 'failurePolicy') == 'STRICT_SNAPSHOT' then
        -- 严格快照不允许容量压力改变目标；保留 promote 已写入的指数退避时刻，避免满载期间空转。
        return {'CAPACITY_EXHAUSTED', tostring(firstDeferredAt), tostring(elapsed)}
    end
    -- 非严格策略跨过容量窗口后由 ready 扫描执行容量专用路由；这里不写失联证据或能力缺失状态。
    redis.call('HSET', KEYS[1], 'startVisibleAt', now, 'readyWakeupCount', tonumber(ARGV[7]))
    redis.call('ZADD', KEYS[2], now, ARGV[1] .. ':' .. ARGV[2])
    return {'CAPACITY_EXHAUSTED', tostring(firstDeferredAt), tostring(elapsed)}
end
local nextVisibleAt = now + tonumber(ARGV[5])
redis.call('HSET', KEYS[1], 'startVisibleAt', nextVisibleAt, 'readyWakeupCount', 0)
redis.call('ZADD', KEYS[2], nextVisibleAt, ARGV[1] .. ':' .. ARGV[2])
return {'DEFERRED', tostring(nextVisibleAt)}
