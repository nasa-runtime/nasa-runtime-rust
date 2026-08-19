-- 业务作用：有界回收保留期已到的任务 tombstone、调度残留和 fencing 字段。
-- KEYS[1] 任务名 SET；KEYS[2] schedule ZSET；KEYS[3] fences HASH；KEYS[4] 分片 reaping SET。
-- ARGV[1] 单轮候选上限；ARGV[2] 调度分片键前缀。
-- 返回：{deletedCount, redisNow}。
-- 安全不变量：只删除 state=DELETED 且 tombstoneUntil 已到期的定义；保留期内继续拒绝旧修订号回注册。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local names = redis.call('SRANDMEMBER', KEYS[1], tonumber(ARGV[1]))
local deleted = 0
for _, jobName in ipairs(names) do
    local jobKey = ARGV[2] .. 'job:' .. jobName
    if redis.call('HGET', jobKey, 'state') == 'DELETED'
            and tonumber(redis.call('HGET', jobKey, 'tombstoneUntil') or '0') <= now
            -- 存量 Run 收敛未完成前不得删除定义：定义一旦消失，fence 随之丢失，
            -- 残余 Run 会退回"定义缺失即无限延后"的空转
            and redis.call('SISMEMBER', KEYS[4], jobName) == 0 then
        -- 定义防回滚窗口已结束，才能同时清除定义枚举、调度项和 fencing 计数。
        redis.call('DEL', jobKey)
        redis.call('SREM', KEYS[1], jobName)
        redis.call('ZREM', KEYS[2], jobName)
        redis.call('HDEL', KEYS[3], jobName)
        deleted = deleted + 1
    end
end
return {tostring(deleted), tostring(now)}
