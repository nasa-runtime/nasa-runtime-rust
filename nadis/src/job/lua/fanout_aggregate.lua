-- 业务作用：把一个尚未终态的 Fanout shard 收敛为指定结果，并在最后一个 shard 完成时决定根终态。
-- KEYS[1] Fanout 根 HASH；KEYS[2] shard HASH；KEYS[3] receipts ZSET；KEYS[4] ready ZSET；KEYS[5] leases ZSET；
-- KEYS[6] completion Stream；KEYS[7] GC ZSET。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] shard 终态；ARGV[4] resultCode；ARGV[5] resultSummary；ARGV[6] Fanout 保留期毫秒。
-- 返回：ALREADY_COMPLETED，或 {OK, rootStateOrWaiting, expireAtOrTerminalCount}。
-- 安全不变量：同一 shard 只能计入一次终态计数；只有全部 shard 终态时才发布根完成事件和 GC 截止点。

local shardState = redis.call('HGET', KEYS[2], 'state')
if shardState == 'SUCCEEDED' or shardState == 'FAILED' or shardState == 'DEAD'
        or shardState == 'SKIPPED' or shardState == 'CANCELLED' then return {'ALREADY_COMPLETED'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('HSET', KEYS[2], 'state', ARGV[3], 'finishedAt', now,
        'resultCode', ARGV[4], 'resultSummary', ARGV[5], 'owner', '', 'leaseUntil', '')
redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZREM', KEYS[4], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZREM', KEYS[5], ARGV[1] .. ':' .. ARGV[2])
local counter = ARGV[3] == 'SUCCEEDED' and 'successCount'
        or (ARGV[3] == 'FAILED' and 'failedCount'
        or (ARGV[3] == 'DEAD' and 'deadCount'
        or (ARGV[3] == 'SKIPPED' and 'skippedCount' or 'cancelledCount')))
redis.call('HINCRBY', KEYS[1], counter, 1)
local terminal = tonumber(redis.call('HGET', KEYS[1], 'successCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'failedCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'deadCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'skippedCount') or '0')
        + tonumber(redis.call('HGET', KEYS[1], 'cancelledCount') or '0')
local total = tonumber(redis.call('HGET', KEYS[1], 'shardTotal') or '0')
if terminal == total then
    -- 根结果的优先级为失败、部分完成、全量取消、全量成功，不依赖 shard 完成顺序。
    local failed = tonumber(redis.call('HGET', KEYS[1], 'failedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[1], 'deadCount') or '0')
    local skipped = tonumber(redis.call('HGET', KEYS[1], 'skippedCount') or '0')
    local cancelled = tonumber(redis.call('HGET', KEYS[1], 'cancelledCount') or '0')
    local rootState = failed > 0 and 'FAILED' or ((skipped > 0) and 'PARTIAL_FAILED'
            or ((cancelled == total) and 'CANCELLED' or 'SUCCEEDED'))
    local expireAt = now + tonumber(ARGV[6])
    redis.call('HSET', KEYS[1], 'state', rootState, 'finishedAt', now, 'expireAt', expireAt)
    redis.call('ZADD', KEYS[7], expireAt, ARGV[1])
    redis.call('XADD', KEYS[6], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMPLETED',
            'fanoutId', ARGV[1], 'rootRunId', redis.call('HGET', KEYS[1], 'rootRunId'), 'state', rootState)
    return {'OK', rootState, tostring(expireAt)}
end
return {'OK', 'WAITING_CHILDREN', tostring(terminal)}
