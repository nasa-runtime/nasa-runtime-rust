-- 业务作用：累积不同 Fanout 根对同一节点启动代次的失联证据，达到门槛后暂停其 Fanout 能力。
-- KEYS[1] 执行器存活 ZSET；KEYS[2] 稳定节点的失联证据 HASH。
-- ARGV[1] nodeIdentity；ARGV[2] 快照中的 startupId；ARGV[3] 快照中的 heartbeatRevision；ARGV[4] fanoutId；
-- ARGV[5] 证据门槛；ARGV[6] 节点不就绪时长毫秒；ARGV[7] 证据保留期毫秒；ARGV[8] registry 键前缀。
-- 返回：STALE_STARTUP、STALE_HEARTBEAT、NOT_ACTIVE、{RECORDED, count} 或 {MARKED_UNREADY, count, until}。
-- 安全不变量：启动代次或心跳修订号已变化时拒绝旧证据；同一 fanoutId 只计一次。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], now, '+inf')
local matchedKey = nil
for _, executorId in ipairs(candidates) do
    local executorKey = ARGV[8] .. 'executor:' .. executorId
    if redis.call('HGET', executorKey, 'nodeIdentity') == ARGV[1] then
        if redis.call('HGET', executorKey, 'startupId') ~= ARGV[2] then return {'STALE_STARTUP'} end
        if tonumber(redis.call('HGET', executorKey, 'heartbeatRevision') or '0') > tonumber(ARGV[3]) then
            return {'STALE_HEARTBEAT'}
        end
        matchedKey = executorKey
        break
    end
end
if not matchedKey then return {'NOT_ACTIVE'} end
-- HSETNX 使同一根的重试不会放大失联证据。
redis.call('HSETNX', KEYS[2], ARGV[4], now)
redis.call('PEXPIRE', KEYS[2], ARGV[7])
local evidenceCount = redis.call('HLEN', KEYS[2])
if evidenceCount < tonumber(ARGV[5]) then return {'RECORDED', tostring(evidenceCount)} end
local unreadyUntil = now + tonumber(ARGV[6])
redis.call('HSET', matchedKey, 'state', 'FANOUT_UNREADY', 'fanoutReady', 'false',
        'fanoutUnreadyUntil', unreadyUntil)
return {'MARKED_UNREADY', tostring(evidenceCount), tostring(unreadyUntil)}
