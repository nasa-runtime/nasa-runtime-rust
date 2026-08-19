-- 业务作用：以单调定义修订号删除任务，立即使未取得执行权的存量 Run 失效，并保留 tombstone 阻止旧进程重新登记。
-- KEYS[1] 任务名 SET；KEYS[2] schedule ZSET；KEYS[3] 任务定义 HASH；KEYS[4] 分片 reaping SET。
-- ARGV[1] jobName；ARGV[2] 删除修订号；ARGV[3] tombstone 保留期毫秒。
-- 返回：NOT_FOUND、{STALE, currentRevision} 或 {OK, redisNow}。
-- 安全不变量：不允许较低修订号覆盖已知定义；先写 DELETED、删除修订 fence 与保留截止点，再从 schedule 移除；
-- 本脚本保持 O(1)，存量 Run 的终态化由 start/promote 的 fence 与后台有界 reap 过程分批完成。

local currentRevision = redis.call('HGET', KEYS[3], 'definitionRevision')
if not currentRevision then return {'NOT_FOUND'} end
if tonumber(currentRevision) > tonumber(ARGV[2]) then return {'STALE', currentRevision} end
local currentState = redis.call('HGET', KEYS[3], 'state')
local previousDeletedRevision = tonumber(redis.call('HGET', KEYS[3], 'deletedRevision') or '-1')
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
-- deletedRevision 是永久 fence：修订号不高于它的 Run 永远不得再取得执行权，
-- 即使之后以更高修订号重新登记同名任务；重新登记的完整 HSET 不触碰该字段。
redis.call('HSET', KEYS[3], 'state', 'DELETED', 'definitionRevision', ARGV[2],
        'deletedRevision', ARGV[2], 'deletedAt', now, 'tombstoneUntil', now + tonumber(ARGV[3]))
-- 新删除周期不得继承上一次游标的 clean 结论：同名任务以更高修订号重建期间可以产生新 Run，
-- 沿用旧结论会在其它索引恰好吃满本批上限时提前撤销 reaping，留下永远不再扫描的旧存量。
-- 同一删除修订号的幂等重放保留当前进度，避免多个节点重复请求让长索引不断从头开始。
if currentState ~= 'DELETED' or tonumber(ARGV[2]) > previousDeletedRevision then
    redis.call('HDEL', KEYS[3],
            'reapCursorVisible', 'reapCursorLeases', 'reapCursorWaiting',
            'reapCleanVisible', 'reapCleanLeases', 'reapCleanWaiting',
            'reapDirtyVisible', 'reapDirtyLeases', 'reapDirtyWaiting')
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
-- O(1) 登记待收敛标记：waitq 成员不会自然进入 visible，由后台按固定批次终态化
redis.call('SADD', KEYS[4], ARGV[1])
return {'OK', tostring(now)}
