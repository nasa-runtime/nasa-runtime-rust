-- 业务作用：暂停一个任务的新自动触发，保留定义和已取得的执行权。
-- KEYS[1] schedule ZSET；KEYS[2] 任务定义 HASH。
-- ARGV[1] jobName。
-- 返回：NOT_FOUND 或 OK。
-- 原子性：定义进入 PAUSED 与从 schedule 移除同时生效；脚本不撤销已开始 attempt。

if redis.call('EXISTS', KEYS[2]) == 0 then
    return {'NOT_FOUND'}
end
redis.call('HSET', KEYS[2], 'state', 'PAUSED')
redis.call('ZREM', KEYS[1], ARGV[1])
return {'OK'}
