-- 业务作用：复验一个自动任务的权威逻辑时刻，原子创建唯一 Run、Dispatch 消息与可见性恢复索引。
-- KEYS[1] schedule ZSET；KEYS[2] 任务定义 HASH；KEYS[3] Run HASH；KEYS[4] visible ZSET；KEYS[5] Dispatch Stream；KEYS[6] 命名空间控制 HASH。
-- ARGV[1] definitionRevision；ARGV[2] expected logicalFireAt；ARGV[3] proposedNextFireAt；ARGV[4] runId；ARGV[5] 保留的 scheduleType；
-- ARGV[6] visibilityTimeoutMs；ARGV[7] protocolVersion；ARGV[8] jobName；ARGV[9] scheduleShard；ARGV[10] wireCodec；
-- ARGV[11] scheduleType；ARGV[12] SKIP/RUN；ARGV[13] triggerType。
-- 返回：STATE_MISMATCH、STALE、{NOT_DUE, redisNow}、{NEED_RECOMPUTE, redisNow}、{SKIPPED, redisNow, nextFireAt}、{ADOPTED, runId} 或 {OK, redisNow, messageId}。
-- 安全不变量：命名空间、定义状态、修订号和 schedule score 全部匹配才能触发；Run 标识使重放收敛为 ADOPTED。

local state = redis.call('HGET', KEYS[2], 'state')
if state ~= 'ENABLED' then return {'STATE_MISMATCH'} end
if (redis.call('HGET', KEYS[6], 'namespaceState') or 'ENABLED') ~= 'ENABLED' then
    return {'STATE_MISMATCH'}
end
if redis.call('HGET', KEYS[2], 'definitionRevision') ~= ARGV[1] then return {'STALE'} end
local currentScore = redis.call('ZSCORE', KEYS[1], ARGV[8])
-- score 是调度时刻的 CAS 权威，迟到扫描不能覆盖已推进的新时刻。
if not currentScore or tonumber(currentScore) ~= tonumber(ARGV[2]) then return {'STALE'} end

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if tonumber(ARGV[2]) > now then return {'NOT_DUE', tostring(now)} end
if ARGV[11] ~= 'FIXED_DELAY' and tonumber(ARGV[3]) <= now then
    return {'NEED_RECOMPUTE', tostring(now)}
end
if ARGV[12] == 'SKIP' then
    redis.call('HSET', KEYS[2], 'lastFireAt', ARGV[2], 'nextFireAt', ARGV[3])
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[8])
    return {'SKIPPED', tostring(now), ARGV[3]}
end
if redis.call('EXISTS', KEYS[3]) == 1 then return {'ADOPTED', ARGV[4]} end

local workerName = redis.call('HGET', KEYS[2], 'workerName')
local workerKey = redis.call('HGET', KEYS[2], 'workerKey')
-- Run 的来源声明只从定义继承, 使消费者可以独立核对消息来源而不依赖键前缀
local sourceId = redis.call('HGET', KEYS[2], 'schedulerQualifier') or ''
redis.call('HSET', KEYS[3],
        'schedulerQualifier', sourceId,
        'runId', ARGV[4], 'jobName', ARGV[8], 'workerName', workerName, 'workerKey', workerKey,
        'protocolVersion', ARGV[7], 'scheduleShard', ARGV[9],
        'definitionRevision', ARGV[1],
        'contractRevision', redis.call('HGET', KEYS[2], 'contractRevision'),
        'schemaId', redis.call('HGET', KEYS[2], 'schemaId'),
        'wireCodec', ARGV[10], 'triggerType', ARGV[13],
        'logicalFireAt', ARGV[2], 'triggeredAt', now,
        'state', 'QUEUED', 'attempt', 0, 'dispatchAttempts', 0,
        'nextVisibleAt', now + tonumber(ARGV[6]), 'createdAt', now)
-- 信封同样携带来源声明: 跨语言消费者不必先读 Run 记录就能拒绝串源消息
local messageId = redis.call('XADD', KEYS[5], 'MAXLEN', '~', 100000, '*',
        'runId', ARGV[4], 'jobName', ARGV[8], 'workerName', workerName,
        'protocolVersion', ARGV[7], 'definitionRevision', ARGV[1],
        'schedulerQualifier', sourceId)
redis.call('ZADD', KEYS[4], now + tonumber(ARGV[6]), ARGV[4])
if ARGV[11] == 'FIXED_DELAY' then
    redis.call('ZREM', KEYS[1], ARGV[8])
else
    redis.call('HSET', KEYS[2], 'lastFireAt', ARGV[2], 'nextFireAt', ARGV[3])
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[8])
end
return {'OK', tostring(now), messageId}
