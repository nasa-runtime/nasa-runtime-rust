-- 业务作用：读取一个 ZSET 中基于 Redis TIME 已到期的有界成员，并返回下一最小 score 供自适应调度。
-- KEYS[1] 待扫描 ZSET；SCHEDULE 模式下 KEYS[2] 为命名空间控制 HASH。
-- ARGV[1] 候选上限；ARGV[2] 可选 SCHEDULE 模式；ARGV[3] SCHEDULE 模式下的调度分片键前缀。
-- 返回：通用模式为 {redisNow, nextScore, member1, score1, ...}；SCHEDULE 模式为
-- {redisNow, nextScore, namespaceState, member1, score1, definitionRevision1, ...}。
-- 原子性：脚本只读取不删除成员；真正状态迁移必须由后续写脚本重新复验 score、状态和修订号。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[1]))
local nextEntry = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local result = {tostring(now), nextEntry[2] or ''}
if ARGV[2] == 'SCHEDULE' then
    result[#result + 1] = redis.call('HGET', KEYS[2], 'namespaceState') or 'ENABLED'
    for index = 1, #due, 2 do
        result[#result + 1] = due[index]
        result[#result + 1] = due[index + 1]
        result[#result + 1] = redis.call('HGET', ARGV[3] .. 'job:' .. due[index], 'definitionRevision') or ''
    end
else
    for index = 1, #due do
        result[#result + 1] = due[index]
    end
end
return result
