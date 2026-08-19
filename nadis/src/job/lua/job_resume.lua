-- 业务作用：从调用方计算的新逻辑时刻恢复任务触发。
-- KEYS[1] schedule ZSET；KEYS[2] 任务定义 HASH。
-- ARGV[1] jobName；ARGV[2] nextFireAt，零表示没有自动时刻。
-- 返回：NOT_FOUND、STATE_MISMATCH 或 OK。
-- 原子性：DELETED 和 CONFLICT 不能通过普通 resume 绕过保护门禁；定义状态与 schedule score 同时恢复。

if redis.call('EXISTS', KEYS[2]) == 0 then
    return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'DELETED' or state == 'CONFLICT' then
    return {'STATE_MISMATCH'}
end
redis.call('HSET', KEYS[2], 'state', 'ENABLED', 'nextFireAt', ARGV[2])
if tonumber(ARGV[2]) > 0 then
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
end
return {'OK'}
