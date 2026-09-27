-- 业务作用：登记一个执行器的 Worker 能力、跨语言合同与存活截止点。
-- KEYS[1] 执行器存活 ZSET；KEYS[2] 执行器 HASH；KEYS[3] Worker 能力 ZSET；KEYS[4] Worker 成员元数据 HASH；
-- KEYS[5] Worker 合同 HASH；KEYS[6] workerKey 与 workerName 的绑定键。
-- ARGV[1..6] executorId、nodeIdentity、startupId、applicationName、runtime、state；
-- ARGV[7..11] workerName、contractRevision、schemaId、wireCodecs、implementationDigest；
-- ARGV[12..15] capacity、失效时长毫秒、capabilityDigest、canonicalDigest；
-- ARGV[16] fanoutCapable：'1' 表示定义真正具备 Fanout Handler（FANOUT_ONLY），才写能力索引与合同。
-- 返回：WORKER_KEY_CONFLICT、CONTRACT_MISMATCH，或 {OK, redisNow, expireAt, heartbeatRevision}。
-- 安全不变量：先固定 Worker 名与合同，再把执行器加入存活及能力索引，避免半登记成员进入 Fanout 快照。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local expireAt = now + tonumber(ARGV[13])

local boundWorker = redis.call('GET', KEYS[6])
-- workerKey 碰撞时必须拒绝，否则普通 Dispatch Stream 会把不同 Worker 混到同一能力池。
if boundWorker and boundWorker ~= ARGV[7] then return {'WORKER_KEY_CONFLICT'} end
if not boundWorker then redis.call('SET', KEYS[6], ARGV[7]) end

-- Fanout 能力、合同与成员元数据只由真正具备 Fanout Handler 的定义（FANOUT_ONLY）持有：
-- 普通定义即使复用 workerName，也不提供该能力的 Handler，写入能力索引会让快照冻结一个
-- 无法执行的目标，删除 Fanout Worker 时还会因为普通定义仍在而无法撤权。
local fanoutCapable = ARGV[16] == '1'
if fanoutCapable then
    local existingSchema = redis.call('HGET', KEYS[5], 'schemaId')
    -- 同一 Worker 合同不能按节点启动顺序覆盖，不一致时封闭新快照。
    if existingSchema and (existingSchema ~= ARGV[9] or redis.call('HGET', KEYS[5], 'wireCodecs') ~= ARGV[10]) then
        redis.call('HSET', KEYS[5], 'conflictState', 'CONFLICT')
        return {'CONTRACT_MISMATCH'}
    end
    if not existingSchema then
        redis.call('HSET', KEYS[5], 'workerName', ARGV[7], 'contractRevision', ARGV[8],
                'schemaId', ARGV[9], 'wireCodecs', ARGV[10], 'canonicalDigest', ARGV[15],
                'firstDeclaredBy', ARGV[1], 'firstDeclaredAt', now, 'conflictState', '')
    end
end

local revision = redis.call('HINCRBY', KEYS[2], 'heartbeatRevision', 1)
redis.call('HSET', KEYS[2],
        'executorId', ARGV[1], 'nodeIdentity', ARGV[2], 'startupId', ARGV[3],
        'applicationName', ARGV[4], 'runtime', ARGV[5], 'state', ARGV[6],
        'capacity', ARGV[12], 'inflight', 0, 'fanoutReady', ARGV[6] == 'ACTIVE' and 'true' or 'false',
        'heartbeatAt', now, 'expireAt', expireAt, 'capabilityDigest', ARGV[14])
redis.call('ZADD', KEYS[1], expireAt, ARGV[1])
if fanoutCapable then
    redis.call('HSET', KEYS[2], 'capability:' .. ARGV[7], '1')
    redis.call('ZADD', KEYS[3], expireAt, ARGV[1])
    redis.call('HSET', KEYS[4],
            ARGV[1] .. '|nodeIdentity', ARGV[2],
            ARGV[1] .. '|startupId', ARGV[3],
            ARGV[1] .. '|applicationName', ARGV[4],
            ARGV[1] .. '|runtime', ARGV[5],
            ARGV[1] .. '|contractRevision', ARGV[8],
            ARGV[1] .. '|schemaId', ARGV[9],
            ARGV[1] .. '|wireCodecs', ARGV[10],
            ARGV[1] .. '|implementationDigest', ARGV[11])
end
return {'OK', tostring(now), tostring(expireAt), tostring(revision)}
