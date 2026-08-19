-- 业务作用：为一批 Fanout shard 建立持久 inbox 消息和 receipt deadline，再发布低延迟通知。
-- KEYS[1] Fanout 根 HASH；KEYS[2] receipts ZSET；之后每个 shard 使用三个键：shard HASH、目标 inbox Stream、通知 channel。
-- ARGV[1] 本批数量；ARGV[2] fanoutId；ARGV[3] PUBLISH 或 SPUBLISH；ARGV[4] 是否推进首次投递游标；
-- 之后每个 shard 使用 seq、assignmentEpoch、shardRunId、receiptTimeoutMs 四个参数。
-- 返回：NOT_COMMITTED，或 {OK, deliveredCount, deliveryCursor}。
-- 安全不变量：只投递当前 assignment 且尚无 inboxMessageId 的 shard；先写 Stream 与截止索引，后发 Pub/Sub。

local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'COMMITTED' and state ~= 'WAITING_CHILDREN' then return {'NOT_COMMITTED'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local count = tonumber(ARGV[1])
local delivered = 0
local cursor = 5
for index = 1, count do
    local shardKey = KEYS[3 + (index - 1) * 3]
    local inboxKey = KEYS[4 + (index - 1) * 3]
    local notifyChannel = KEYS[5 + (index - 1) * 3]
    local shardState = redis.call('HGET', shardKey, 'state')
    local epoch = redis.call('HGET', shardKey, 'assignmentEpoch')
    local inboxMessageId = redis.call('HGET', shardKey, 'inboxMessageId')
    if shardState == 'AWAITING_RECEIPT' and epoch == ARGV[cursor + 1]
            and (not inboxMessageId or inboxMessageId == '') then
        -- inbox 信封携带 shard 的来源声明, 与 Dispatch 信封同构; 业务参数仍以持久 shard 为准
        local messageId = redis.call('XADD', inboxKey, '*', 'fanoutId', ARGV[2],
                'seq', ARGV[cursor], 'assignmentEpoch', epoch, 'shardRunId', ARGV[cursor + 2],
                'schedulerQualifier', redis.call('HGET', shardKey, 'schedulerQualifier') or '')
        local deadline = now + tonumber(ARGV[cursor + 3])
        redis.call('HSET', shardKey, 'inboxMessageId', messageId, 'receiptDeadlineAt', deadline)
        redis.call('ZADD', KEYS[2], deadline, ARGV[2] .. ':' .. ARGV[cursor])
        -- 通知丢失时，inbox 和 deadline 仍能驱动重发，Pub/Sub 不承担正确性。
        redis.call(ARGV[3], notifyChannel, ARGV[2] .. '|' .. ARGV[cursor]
                .. '|' .. epoch .. '|' .. messageId)
        delivered = delivered + 1
    end
    cursor = cursor + 4
end
if ARGV[4] == '1' then redis.call('HINCRBY', KEYS[1], 'deliveryCursor', count) end
return {'OK', tostring(delivered), redis.call('HGET', KEYS[1], 'deliveryCursor')}
