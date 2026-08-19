-- 业务作用：在 Fanout 终态已回填普通根且保留期到期后，按有界批次删除 shard、inbox 和根记录。
-- KEYS[1] 根 HASH；KEYS[2] GC ZSET；KEYS[3] roots/watch ZSET；KEYS[4] receipts ZSET；KEYS[5] ready ZSET；KEYS[6] leases ZSET；
-- KEYS[7..n] 按“shard HASH、目标 inbox Stream”成对排列。
-- ARGV[1] fanoutId；ARGV[2] 本批数量；ARGV[3] inbox 消费组。
-- 返回：STATE_MISMATCH、{NOT_DUE, expireAt}、NOT_RECONCILED、{OK, deletedCount, nextCursor} 或 {COMPLETED, deletedCount, cursor}。
-- 安全不变量：未对账或未到保留期的根绝不删除；先删 shard 及 inbox 消息，最后删根与索引。

local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'SUCCEEDED' and state ~= 'PARTIAL_FAILED' and state ~= 'FAILED' and state ~= 'CANCELLED' then
    return {'STATE_MISMATCH'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local expireAt = tonumber(redis.call('HGET', KEYS[1], 'expireAt') or '0')
if expireAt > now then return {'NOT_DUE', tostring(expireAt)} end
if not redis.call('HGET', KEYS[1], 'reconciledAt') then return {'NOT_RECONCILED'} end
local cursor = tonumber(redis.call('HGET', KEYS[1], 'cleanupCursor') or '0')
local count = tonumber(ARGV[2])
for index = 1, count do
    local seq = cursor + index - 1
    local member = ARGV[1] .. ':' .. seq
    local shardKey = KEYS[6 + (index - 1) * 2 + 1]
    local inboxKey = KEYS[6 + (index - 1) * 2 + 2]
    local messageId = redis.call('HGET', shardKey, 'inboxMessageId')
    if messageId and messageId ~= '' then
        redis.pcall('XACK', inboxKey, ARGV[3], messageId)
        redis.call('XDEL', inboxKey, messageId)
    end
    redis.call('ZREM', KEYS[4], member)
    redis.call('ZREM', KEYS[5], member)
    redis.call('ZREM', KEYS[6], member)
    redis.call('DEL', shardKey)
    if redis.call('XLEN', inboxKey) == 0 then
        local pending = redis.pcall('XPENDING', inboxKey, ARGV[3])
        if type(pending) == 'table' and tonumber(pending[1] or '0') == 0 then
            redis.call('DEL', inboxKey)
        end
    end
end
local nextCursor = cursor + count
local total = tonumber(redis.call('HGET', KEYS[1], 'shardTotal') or '0')
if nextCursor < total then
    redis.call('HSET', KEYS[1], 'cleanupCursor', nextCursor)
    return {'OK', tostring(count), tostring(nextCursor)}
end
-- 普通根已持久收敛且所有 shard 证据已删除，此时才可删 Fanout 根。
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return {'COMPLETED', tostring(count), tostring(nextCursor)}
