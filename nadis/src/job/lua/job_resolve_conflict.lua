-- 业务作用：经显式管理动作选择持久定义摘要，解除 CONFLICT 并按新基准恢复调度。
-- KEYS[1] schedule ZSET；KEYS[2] 任务定义 HASH。
-- ARGV[1] jobName；ARGV[2] definitionRevision；ARGV[3] definitionDigest；ARGV[4] 目标状态；ARGV[5] nextFireAt。
-- 返回：NOT_FOUND、STATE_MISMATCH、STALE 或 OK。
-- 安全不变量：只能解除当前 CONFLICT，且管理者选择的修订号与摘要必须仍与 Redis 权威记录一致。

if redis.call('EXISTS', KEYS[2]) == 0 then return {'NOT_FOUND'} end
if redis.call('HGET', KEYS[2], 'state') ~= 'CONFLICT' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[2], 'definitionRevision') ~= ARGV[2]
        or redis.call('HGET', KEYS[2], 'definitionDigest') ~= ARGV[3] then
    return {'STALE'}
end
redis.call('HSET', KEYS[2], 'state', ARGV[4])
if ARGV[4] == 'ENABLED' and tonumber(ARGV[5]) > 0 then
    redis.call('HSET', KEYS[2], 'nextFireAt', ARGV[5])
    redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
else
    redis.call('ZREM', KEYS[1], ARGV[1])
end
return {'OK'}
