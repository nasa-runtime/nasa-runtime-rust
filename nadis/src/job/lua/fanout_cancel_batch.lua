-- 业务作用：先封闭 Fanout 根的新执行权入口，再按有界批次取消未运行 shard 并向运行中 shard 传递取消信号。
-- KEYS[1] 根 HASH；KEYS[2] roots/watch ZSET；KEYS[3] completion Stream；KEYS[4] GC ZSET；
-- KEYS[5] receipts ZSET；KEYS[6] ready ZSET；KEYS[7] leases ZSET；KEYS[8..n] 按“shard HASH、目标 inbox Stream”成对排列。
-- ARGV[1] fanoutId；ARGV[2] 取消原因；ARGV[3] 终态保留期毫秒；ARGV[4] 预期 cancelCursor；
-- ARGV[5] 本批数量；ARGV[6] inbox 消费组；ARGV[7] 未收敛 shard 的再扫描延迟毫秒。
-- 返回：{ALREADY_COMPLETED, state}、{STALE, cursor}、{OK, CANCELLING, nextCursor} 或 {OK, CANCELLED, expireAt}。
-- 安全不变量：根必须先进入 CANCELLING；RUNNING shard 仅记录协作式取消，不直接伪造完成；只有全部 shard 终态后才关闭根。

local state = redis.call('HGET', KEYS[1], 'state')
-- 缺失根没有取消权威；不能用迟到请求重新创建已回收的批次。
if not state then
    return redis.error_reply('ERR Fanout cancellation root is absent')
end
if state == 'SUCCEEDED' or state == 'PARTIAL_FAILED' or state == 'FAILED' or state == 'CANCELLED' then
    return {'ALREADY_COMPLETED', state}
end
if state ~= 'CREATING' and state ~= 'COMMITTED' and state ~= 'WAITING_CHILDREN' and state ~= 'CANCELLING' then
    return redis.error_reply('ERR Fanout cancellation state is invalid')
end
local total = tonumber(redis.call('HGET', KEYS[1], 'shardTotal'))
local expected = tonumber(ARGV[4])
local count = tonumber(ARGV[5])
local retention = tonumber(ARGV[3])
local rescan = tonumber(ARGV[7])
local cursor = 0
if state == 'CANCELLING' then
    cursor = tonumber(redis.call('HGET', KEYS[1], 'cancelCursor') or '0')
end
-- Redis 脚本失败不会撤销先前写入，完整参数和键对必须在发布 CANCELLING 之前核验。
if #ARGV ~= 7 or redis.call('HGET', KEYS[1], 'fanoutId') ~= ARGV[1]
        or not total or total < 0 or total > 9007199254740991 or total ~= math.floor(total)
        or not cursor or cursor < 0 or cursor > total or cursor ~= math.floor(cursor)
        or not expected or expected < 0 or expected > total or expected ~= math.floor(expected)
        or not count or count < 0 or count > total - expected or count ~= math.floor(count)
        or #KEYS ~= 7 + count * 2 or (count == 0 and expected < total)
        or not retention or retention <= 0 or retention > 9007199254740991 or retention ~= math.floor(retention)
        or not rescan or rescan <= 0 or rescan > 9007199254740991 or rescan ~= math.floor(rescan) then
    return redis.error_reply('ERR Fanout cancellation arguments are invalid')
end
if cursor ~= expected then
    return {'STALE', tostring(cursor)}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if state ~= 'CANCELLING' then
    -- start_run.lua 会拒绝 CANCELLING 根，因此必须先发布保护态再撤销各 shard。
    redis.call('HSET', KEYS[1], 'state', 'CANCELLING', 'cancelReason', ARGV[2], 'cancelCursor', 0)
end
for index = 1, count do
    local seq = cursor + index - 1
    local member = ARGV[1] .. ':' .. seq
    local shardKey = KEYS[7 + (index - 1) * 2 + 1]
    local inboxKey = KEYS[7 + (index - 1) * 2 + 2]
    local shardState = redis.call('HGET', shardKey, 'state')
    if shardState == 'RUNNING' then
        redis.call('HSET', shardKey, 'cancelRequestedAt', now)
    elseif shardState ~= 'SUCCEEDED' and shardState ~= 'FAILED' and shardState ~= 'DEAD'
            and shardState ~= 'SKIPPED' and shardState ~= 'CANCELLED' then
        local messageId = redis.call('HGET', shardKey, 'inboxMessageId')
        if messageId and messageId ~= '' then
            redis.pcall('XACK', inboxKey, ARGV[6], messageId)
            redis.call('XDEL', inboxKey, messageId)
        end
        redis.call('ZREM', KEYS[5], member)
        redis.call('ZREM', KEYS[6], member)
        redis.call('ZREM', KEYS[7], member)
        redis.call('HSET', shardKey, 'state', 'CANCELLED', 'finishedAt', now,
                'resultCode', 'CANCELLED', 'errorType', ARGV[2], 'owner', '', 'leaseUntil', '')
        redis.call('HINCRBY', KEYS[1], 'cancelledCount', 1)
    end
end
local nextCursor = cursor + count
if nextCursor < total then
    redis.call('HSET', KEYS[1], 'cancelCursor', nextCursor, 'nextWatchAt', now)
    redis.call('ZADD', KEYS[2], now, ARGV[1])
    return {'OK', 'CANCELLING', tostring(nextCursor)}
end
local terminal = tonumber(redis.call('HGET', KEYS[1], 'successCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'failedCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'deadCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'skippedCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'cancelledCount') or '0')
if terminal < total then
    local nextWatchAt = now + tonumber(ARGV[7])
    redis.call('HSET', KEYS[1], 'cancelCursor', 0, 'nextWatchAt', nextWatchAt)
    redis.call('ZADD', KEYS[2], nextWatchAt, ARGV[1])
    return {'OK', 'CANCELLING', '0'}
end
local expireAt = now + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'finishedAt', now, 'expireAt', expireAt)
redis.call('ZADD', KEYS[4], expireAt, ARGV[1])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMPLETED',
        'fanoutId', ARGV[1], 'rootRunId', redis.call('HGET', KEYS[1], 'rootRunId'),
        'state', 'CANCELLED', 'cancelReason', redis.call('HGET', KEYS[1], 'cancelReason') or '')
return {'OK', 'CANCELLED', tostring(expireAt)}
