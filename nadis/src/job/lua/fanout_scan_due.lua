-- 业务作用：在一次 Redis TIME 观测下批量读取同 Fanout 桶的 receipt、ready、lease 和 GC 到期成员。
-- KEYS[1] roots/watch ZSET；KEYS[2..n] 待扫描的期限 ZSET。
-- ARGV[1] 每个期限索引的最大候选数。
-- 返回：{redisNow, rootCount, 每个索引的 nextScore, dueCount, member1, score1, ...}。
-- 原子性：脚本只读取不迁移状态；四类截止索引共享同一服务端时间，避免单轮扫描产生时间偏差。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local limit = tonumber(ARGV[1])
local result = {tostring(now), tostring(redis.call('ZCARD', KEYS[1]))}
for keyIndex = 2, #KEYS do
    local key = KEYS[keyIndex]
    local due = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit)
    local nextEntry = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    result[#result + 1] = nextEntry[2] or ''
    result[#result + 1] = tostring(#due / 2)
    for index = 1, #due do
        result[#result + 1] = due[index]
    end
end
return result
