-- 业务作用：目标节点持久确认已接收 Fanout shard，建立 start 截止索引后向根执行器发回执信号。
-- KEYS[1] Fanout 根 HASH；KEYS[2] shard HASH；KEYS[3] receipt deadline ZSET；KEYS[4] ready ZSET；KEYS[5] 根执行器回执 channel。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] targetNodeIdentity；ARGV[4] executorId；ARGV[5] assignmentEpoch；
-- ARGV[6] start 重新可见延迟毫秒；ARGV[7] PUBLISH 或 SPUBLISH；
-- ARGV[8..12] 本执行器期望的 schedulerQualifier、protocolVersion、contractRevision、schemaId、wireCodecs。
-- 返回：NOT_COMMITTED、ADOPTED、STATE_MISMATCH、STALE_ASSIGNMENT、SOURCE_MISMATCH、PROTOCOL_UNSUPPORTED、CONTRACT_MISMATCH，
-- 或 {OK, redisNow, startVisibleAt}。
-- 安全不变量：目标身份与 assignmentEpoch 必须同时匹配；来源、协议与任务契约在持久确认前复验；
-- 三道门禁拒绝时在 shard 写 rejectedCode/rejectedBy/rejectedExpectation/rejectedAt 供跨节点排查。
-- 证据组独立于 errorType：errorType 由重分配、租约恢复和取消等状态推进路径改写，
-- 两组语义互不覆盖，历史拒绝证据不会被后续状态换成从未发生过的归因。
-- 不改 state、不回执；先持久 RECEIVED 并删除 receipt deadline，再发 Pub/Sub 信号。

local rootState = redis.call('HGET', KEYS[1], 'state')
if rootState ~= 'COMMITTED' and rootState ~= 'WAITING_CHILDREN' then return {'NOT_COMMITTED'} end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'RECEIVED' and redis.call('HGET', KEYS[2], 'assignmentEpoch') == ARGV[5] then return {'ADOPTED'} end
if state ~= 'AWAITING_RECEIPT' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[2], 'targetNodeIdentity') ~= ARGV[3]
        or redis.call('HGET', KEYS[2], 'assignmentEpoch') ~= ARGV[5] then return {'STALE_ASSIGNMENT'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
-- 拒绝必须在 shard 上留下证据：根节点的 receipt 监控看不到进程本地指标，
-- 没有这些字段就无法区分"目标宕机/丢包"与"目标明确拒绝执行"，排查只能逐节点猜。
-- 证据写独立的 rejectedCode 而不是 errorType：errorType 属于状态推进路径（重分配、租约恢复、取消），
-- 复用会让后续状态把拒绝归因改写成从未发生的事件。只写观测字段、不改 state，
-- 收敛仍靠"不回执、接收重试按失败策略换兼容节点"。
local function reject(code, expectation)
    redis.call('HSET', KEYS[2], 'rejectedCode', code, 'rejectedBy', ARGV[4],
            'rejectedExpectation', expectation, 'rejectedAt', now)
    return {code}
end
-- 来源复验必须发生在持久确认之前：确认即向根执行器承诺"该节点会执行这个分片"。
-- 本节点根本不该执行其它数据源的 shard 时，不能先回执再在 start 阶段拒绝，
-- 那会让接收重试与重分配机制把一个确定不可执行的目标当作健康目标。
if (redis.call('HGET', KEYS[2], 'schedulerQualifier') or '') ~= ARGV[8] then
    return reject('SOURCE_MISMATCH', 'source=' .. ARGV[8])
end
-- 协议代次门禁：不理解的高版本 shard 不得确认接收，由接收超时把它交给兼容节点。
if tonumber(redis.call('HGET', KEYS[2], 'protocolVersion') or '0') > tonumber(ARGV[9]) then
    return reject('PROTOCOL_UNSUPPORTED', 'protocol<=' .. ARGV[9])
end
-- 任务契约复验：本地 Worker 合同与 shard 冻结的合同不兼容时拒绝确认，
-- 让 receipt 重试按"寻找其它兼容节点"的既有路径重分配，而不是确认后卡在 start 门禁前。
local shardCodec = redis.call('HGET', KEYS[2], 'wireCodec') or ''
if redis.call('HGET', KEYS[2], 'contractRevision') ~= ARGV[10]
        or redis.call('HGET', KEYS[2], 'schemaId') ~= ARGV[11]
        or not string.find(',' .. ARGV[12] .. ',', ',' .. shardCodec .. ',', 1, true) then
    return reject('CONTRACT_MISMATCH', 'contract=' .. ARGV[10] .. '|' .. ARGV[11] .. '|' .. ARGV[12])
end
-- 门禁全部通过说明记录与本节点兼容，此前其它节点留下的拒绝证据已过时，随本次确认一并清除。
redis.call('HDEL', KEYS[2], 'rejectedCode', 'rejectedBy', 'rejectedExpectation', 'rejectedAt',
        'capacityDeferredAt')
local startVisibleAt = now + tonumber(ARGV[6])
redis.call('HSET', KEYS[2], 'state', 'RECEIVED', 'receivedByExecutorId', ARGV[4],
        'receivedAt', now, 'startVisibleAt', startVisibleAt)
redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZADD', KEYS[4], startVisibleAt, ARGV[1] .. ':' .. ARGV[2])
-- Pub/Sub 只加速根节点观察，即使信号丢失，前面的持久状态仍是权威确认。
redis.call(ARGV[7], KEYS[5], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[5] .. '|RECEIVED')
return {'OK', tostring(now), tostring(startVisibleAt)}
