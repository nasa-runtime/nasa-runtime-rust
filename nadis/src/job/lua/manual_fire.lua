-- 业务作用：以调用方 requestId 派生的稳定 runId 幂等创建手工 Run、参数、Dispatch 消息和可见性索引。
-- KEYS[1] 任务定义 HASH；KEYS[2] Run HASH；KEYS[3] visible ZSET；KEYS[4] Dispatch Stream；KEYS[5] completion Stream；KEYS[6] 命名空间控制 HASH。
-- ARGV[1] runId；ARGV[2] requestId；ARGV[3] Base64 参数；ARGV[4] schemaId；ARGV[5] wireCodec；
-- ARGV[6] visibilityTimeoutMs；ARGV[7] protocolVersion；ARGV[8] scheduleShard；ARGV[9] jobName。
-- 返回：NOT_FOUND、STATE_MISMATCH、{ADOPTED, runId} 或 {OK, runId, redisNow, messageId}。
-- 安全不变量：任务和命名空间必须均为 ENABLED，FANOUT_ONLY 不能独立触发；已存在 runId 直接 ADOPTED。

if redis.call('EXISTS', KEYS[1]) == 0 then return {'NOT_FOUND'} end
local definitionState = redis.call('HGET', KEYS[1], 'state')
if definitionState ~= 'ENABLED' then return {'STATE_MISMATCH'} end
if (redis.call('HGET', KEYS[6], 'namespaceState') or 'ENABLED') ~= 'ENABLED' then
    return {'STATE_MISMATCH'}
end
if redis.call('HGET', KEYS[1], 'trigger') == 'FANOUT_ONLY' then return {'STATE_MISMATCH'} end
if redis.call('EXISTS', KEYS[2]) == 1 then return {'ADOPTED', ARGV[1]} end

-- Run、持久消息与可见性索引在同一 slot 中同时建立，响应丢失后可依 runId 重放。
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local workerName = redis.call('HGET', KEYS[1], 'workerName')
local workerKey = redis.call('HGET', KEYS[1], 'workerKey')
-- Run 的来源声明只从定义继承, 不由调用方传入: 定义是该数据源的唯一权威
local sourceId = redis.call('HGET', KEYS[1], 'schedulerQualifier') or ''
redis.call('HSET', KEYS[2],
        'schedulerQualifier', sourceId,
        'runId', ARGV[1], 'requestId', ARGV[2], 'jobName', ARGV[9],
        'workerName', workerName, 'workerKey', workerKey,
        'protocolVersion', ARGV[7], 'scheduleShard', ARGV[8],
        'definitionRevision', redis.call('HGET', KEYS[1], 'definitionRevision'),
        'contractRevision', redis.call('HGET', KEYS[1], 'contractRevision'),
        'schemaId', ARGV[4], 'wireCodec', ARGV[5], 'parameterPayload', ARGV[3],
        'triggerType', 'MANUAL', 'logicalFireAt', now, 'triggeredAt', now,
        'state', 'QUEUED', 'attempt', 0, 'dispatchAttempts', 0,
        'nextVisibleAt', now + tonumber(ARGV[6]), 'createdAt', now)
-- 信封同样携带来源声明: 跨语言消费者不必先读 Run 记录就能拒绝串源消息
local messageId = redis.call('XADD', KEYS[4], 'MAXLEN', '~', 100000, '*',
        'runId', ARGV[1], 'jobName', ARGV[9], 'workerName', workerName,
        'protocolVersion', ARGV[7],
        'definitionRevision', redis.call('HGET', KEYS[1], 'definitionRevision'),
        'schedulerQualifier', sourceId)
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[6]), ARGV[1])
return {'OK', ARGV[1], tostring(now), messageId}
