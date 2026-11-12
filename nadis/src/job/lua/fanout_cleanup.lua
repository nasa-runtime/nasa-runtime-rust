-- 业务作用：在 Fanout 终态已回填普通根且保留期到期后，按有界批次删除 shard、inbox 和根记录。
-- KEYS[1] 根 HASH；KEYS[2] GC ZSET；KEYS[3] roots/watch ZSET；KEYS[4] receipts ZSET；KEYS[5] ready ZSET；KEYS[6] leases ZSET；
-- KEYS[7..n] 按“shard HASH、目标 inbox Stream”成对排列。
-- ARGV[1] fanoutId；ARGV[2] 本批数量；ARGV[3] inbox 消费组；ARGV[4] 构造 KEYS 时观察的清理游标。
-- 返回：STATE_MISMATCH、{NOT_DUE, expireAt}、NOT_RECONCILED、{OK, deletedCount, nextCursor} 或 {COMPLETED, deletedCount, cursor}。
-- 安全不变量：未对账或未到保留期的根绝不删除；旧游标零写入返回，先删 shard 及 inbox 消息，最后删根与索引。

local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'SUCCEEDED' and state ~= 'PARTIAL_FAILED' and state ~= 'FAILED' and state ~= 'CANCELLED' then
    return {'STATE_MISMATCH'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local expireAt = tonumber(redis.call('HGET', KEYS[1], 'expireAt') or '0')
if expireAt > now then
    return {'NOT_DUE', tostring(expireAt)}
end
if not redis.call('HGET', KEYS[1], 'reconciledAt') then
    return {'NOT_RECONCILED'}
end
local cursor = tonumber(redis.call('HGET', KEYS[1], 'cleanupCursor') or '0')
local observedCursor = tonumber(ARGV[4])
if not observedCursor or observedCursor < 0 or observedCursor ~= math.floor(observedCursor) then
    return redis.error_reply('ERR invalid observed cleanup cursor')
end
-- 请求中的分片键对应观察游标；其它监视器已推进时不得继续删除旧键或跳过新分片。
if cursor ~= observedCursor then
    return {'OK', '0', tostring(cursor)}
end
local count = tonumber(ARGV[2])
local total = tonumber(redis.call('HGET', KEYS[1], 'shardTotal') or '0')
-- 在任何删除前确认键对数量和区间完整，避免部分清理后才发现输入不匹配。
if not count or count < 0 or count ~= math.floor(count)
    or not total or total < cursor or count > total - cursor
    or #KEYS ~= 6 + count * 2 or (count == 0 and cursor < total) then
    return redis.error_reply('ERR invalid cleanup range')
end
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
if nextCursor < total then
    redis.call('HSET', KEYS[1], 'cleanupCursor', nextCursor)
    return {'OK', tostring(count), tostring(nextCursor)}
end
-- 普通根已持久收敛且所有 shard 证据已删除，此时才可删 Fanout 根。
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return {'COMPLETED', tostring(count), tostring(nextCursor)}
