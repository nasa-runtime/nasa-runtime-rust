-- 业务作用：当 Fanout shard 未在 receipt deadline 前持久确认时，重发当前 assignment 的通知并推进下一截止点。
-- KEYS[1] shard HASH；KEYS[2] receipts ZSET；KEYS[3] 目标通知 channel。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] assignmentEpoch；ARGV[4] 普通重发上限；ARGV[5] 基础 receiptTimeoutMs；
-- ARGV[6] PUBLISH 或 SPUBLISH；ARGV[7] failurePolicy；ARGV[8] STRICT_SNAPSHOT 最大退避毫秒。
-- 返回：STALE、STALE_ASSIGNMENT、{NOT_DUE, deadline}、{RETRY_EXHAUSTED, count} 或 {OK, count, nextDeadline}。
-- 安全不变量：Redis 中的 AWAITING_RECEIPT 和 deadline 是权威证据；STRICT_SNAPSHOT 不换目标，超额后有界退避。

if redis.call('HGET', KEYS[1], 'state') ~= 'AWAITING_RECEIPT' then
    redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
    return {'STALE'}
end
if redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[3] then return {'STALE_ASSIGNMENT'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local deadline = tonumber(redis.call('HGET', KEYS[1], 'receiptDeadlineAt') or '0')
if deadline > now then return {'NOT_DUE', tostring(deadline)} end
local retryCount = tonumber(redis.call('HGET', KEYS[1], 'receiptRetryCount') or '0')
local strict = ARGV[7] == 'STRICT_SNAPSHOT'
if retryCount >= tonumber(ARGV[4]) and not strict then return {'RETRY_EXHAUSTED', tostring(retryCount)} end
retryCount = redis.call('HINCRBY', KEYS[1], 'receiptRetryCount', 1)
local delay = tonumber(ARGV[5])
if strict and retryCount > tonumber(ARGV[4]) then
    -- 严格快照允许持续恢复原目标，但不允许无界高频重发。
    local exponent = math.min(retryCount - tonumber(ARGV[4]), 16)
    delay = math.min(delay * (2 ^ exponent), tonumber(ARGV[8]))
end
deadline = now + delay
redis.call('HSET', KEYS[1], 'receiptDeadlineAt', deadline)
redis.call('ZADD', KEYS[2], deadline, ARGV[1] .. ':' .. ARGV[2])
redis.call(ARGV[6], KEYS[3], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3]
        .. '|' .. (redis.call('HGET', KEYS[1], 'inboxMessageId') or ''))
return {'OK', tostring(retryCount), tostring(deadline)}
