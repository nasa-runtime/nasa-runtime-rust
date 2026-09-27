-- 业务作用：当本地暂无执行容量或缺少兼容 Handler 时，延后普通 Run；删除 fence 内的 Run 直接终态化。
-- KEYS[1] Run HASH；KEYS[2] visible ZSET；KEYS[3] Dispatch Stream；KEYS[4] 任务定义 HASH；KEYS[5] completion Stream。
-- ARGV[1] runId；ARGV[2] Stream messageId；ARGV[3] 重新可见延迟毫秒；ARGV[4] 消费组；ARGV[5] Run 终态保留期毫秒。
-- 返回：STALE、{JOB_DELETED, -1}，或 {DEFERRED, delayMs}。
-- 安全不变量：只有 QUEUED Run 可延后；终态或迟到消息只 ACK 不改写 Run；删除判定、可见索引
-- 或终态、当前消息 ACK/删除在同一 slot 内提交，
-- 已删除任务的在途消息不得把 fence 内 Run 重新写回可见索引。

if redis.call('HGET', KEYS[1], 'state') ~= 'QUEUED' then
    -- Run 可能在消息拉取后被 reaper 终态化；此时只确认当前消费者的 PEL 项，
    -- 保留 Stream 记录的 STALE 语义，同时避免迟到消息被不断接管。
    redis.call('XACK', KEYS[3], ARGV[4], ARGV[2])
    return {'STALE'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local jobState = redis.call('HGET', KEYS[4], 'state')
local deletedRevision = tonumber(redis.call('HGET', KEYS[4], 'deletedRevision') or '-1')
local runRevision = tonumber(redis.call('HGET', KEYS[1], 'definitionRevision') or '0')
if jobState == 'DELETED' or runRevision <= deletedRevision then
    redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'JOB_DELETED')
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('XADD', KEYS[5], 'MAXLEN', '~', 100000, '*',
            'runId', ARGV[1], 'state', 'CANCELLED', 'resultCode', 'JOB_DELETED')
    redis.call('PEXPIRE', KEYS[1], ARGV[5])
    redis.call('XACK', KEYS[3], ARGV[4], ARGV[2])
    redis.call('XDEL', KEYS[3], ARGV[2])
    return {'JOB_DELETED', '-1'}
end
local nextVisibleAt = now + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
redis.call('ZADD', KEYS[2], nextVisibleAt, ARGV[1])
redis.call('XACK', KEYS[3], ARGV[4], ARGV[2])
redis.call('XDEL', KEYS[3], ARGV[2])
return {'DEFERRED', tostring(tonumber(ARGV[3]))}
