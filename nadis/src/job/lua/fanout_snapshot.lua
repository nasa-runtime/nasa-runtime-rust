-- 业务作用：在 registry slot 中选择当前存活、就绪且合同完全兼容的 Worker 执行器快照。
-- KEYS[1] 执行器存活 ZSET；KEYS[2] Worker 能力 ZSET；KEYS[3] Worker 成员元数据 HASH；KEYS[4] Worker 合同 HASH。
-- ARGV[1] workerName；ARGV[2] contractRevision；ARGV[3] schemaId；ARGV[4] wireCodec；ARGV[5] 候选上限；ARGV[6] registry 键前缀。
-- 返回：CONTRACT_MISMATCH，或 {OK, selectedAt, 每成员的 nodeIdentity、executorId、startupId、applicationName、runtime、implementationDigest、heartbeatRevision}。
-- 安全不变量：仅选 ACTIVE、fanoutReady 且未过期成员；合同必须精确匹配；同一稳定节点只保留最新心跳代次。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if redis.call('HGET', KEYS[4], 'conflictState') == 'CONFLICT' then return {'CONTRACT_MISMATCH'} end
if redis.call('HGET', KEYS[4], 'schemaId') ~= ARGV[3] then return {'CONTRACT_MISMATCH'} end
local supported = redis.call('HGET', KEYS[4], 'wireCodecs') or ''
if not string.find(',' .. supported .. ',', ',' .. ARGV[4] .. ',', 1, true) then return {'CONTRACT_MISMATCH'} end

local candidates = redis.call('ZRANGEBYSCORE', KEYS[2], now, '+inf', 'LIMIT', 0, tonumber(ARGV[5]))
local members = {}
for _, executorId in ipairs(candidates) do
    local executorKey = ARGV[6] .. 'executor:' .. executorId
    local state = redis.call('HGET', executorKey, 'state')
    local ready = redis.call('HGET', executorKey, 'fanoutReady')
    local revision = redis.call('HGET', KEYS[3], executorId .. '|contractRevision')
    local schema = redis.call('HGET', KEYS[3], executorId .. '|schemaId')
    local codecs = redis.call('HGET', KEYS[3], executorId .. '|wireCodecs') or ''
    if state == 'ACTIVE' and ready == 'true' and revision == ARGV[2] and schema == ARGV[3]
            and string.find(',' .. codecs .. ',', ',' .. ARGV[4] .. ',', 1, true) then
        members[#members + 1] = {
            redis.call('HGET', KEYS[3], executorId .. '|nodeIdentity') or '', executorId,
            redis.call('HGET', KEYS[3], executorId .. '|startupId') or '',
            redis.call('HGET', KEYS[3], executorId .. '|applicationName') or '',
            redis.call('HGET', KEYS[3], executorId .. '|runtime') or '',
            redis.call('HGET', KEYS[3], executorId .. '|implementationDigest') or '',
            redis.call('HGET', executorKey, 'heartbeatRevision') or '0'}
    end
end
table.sort(members, function(left, right)
    -- 先按稳定节点排序，同节点再把更新的心跳代次放在前面，使去重结果确定。
    if left[1] == right[1] then return tonumber(left[7]) > tonumber(right[7]) end
    return left[1] < right[1]
end)

local result = {'OK', tostring(now)}
local seenNodes = {}
for _, member in ipairs(members) do
    if not seenNodes[member[1]] then
        seenNodes[member[1]] = true
        for index = 1, #member do result[#result + 1] = member[index] end
    end
end
return result
