-- 业务作用：以单调修订号和规范摘要登记任务定义，并原子建立任务枚举与首个调度时刻。
-- KEYS[1] 任务名 SET；KEYS[2] schedule ZSET；KEYS[3] 任务定义 HASH。
-- ARGV[1..8] jobName、definitionRevision、definitionDigest、state、scheduleShard、workerName、workerKey、trigger；
-- ARGV[9..17] scheduleType、cron、zone、intervalMs、concurrency、misfire、timeoutMs、maxAttempts、retryDelayMs；
-- ARGV[18..24] contractRevision、schemaId、wireCodecs、fanoutReceiptTimeoutMs、fanoutReceiptMaxRetries、fanoutFailurePolicy、nextFireAt；
-- ARGV[25] schedulerQualifier：本定义所属数据源的语言无关 source id，作为消息内部可独立核对的来源声明。
-- 返回：{SOURCE_MISMATCH, 已有来源}、{DELETED, revision}、{STALE, revision}、{CONFLICT, revision}、
-- {ADOPTED, revision} 或 {OK, revision}。
-- 安全不变量：来源门禁先于一切状态、修订号与摘要裁决，已有非空来源与本地不同时不做任何写入；
-- 低修订号不能回滚定义；同修订号但摘要不同时进入 CONFLICT 并停止触发，不按节点启动顺序裁决。

-- 来源门禁先于一切状态、修订号与摘要裁决，且独立于 definitionRevision 是否完整：
-- 来源身份决定当前调用方是否有权解释该记录，不能用另一个字段是否存在来决定门禁是否启用。
-- 修订号缺失的记录可能来自旧格式数据、不完整的迁移恢复或其它语言实现，
-- 若把来源比对放进修订号分支，这类残缺记录会被当作全新定义静默覆盖、串源证据随之消失。
local currentSource = redis.call('HGET', KEYS[3], 'schedulerQualifier')
if currentSource and currentSource ~= '' and currentSource ~= ARGV[25] then
    return {'SOURCE_MISMATCH', currentSource}
end

local currentRevision = redis.call('HGET', KEYS[3], 'definitionRevision')
if currentRevision then
    local requestedRevision = tonumber(ARGV[2])
    if redis.call('HGET', KEYS[3], 'state') == 'DELETED' and requestedRevision <= tonumber(currentRevision) then
        return {'DELETED', currentRevision}
    end
    if tonumber(currentRevision) > requestedRevision then
        return {'STALE', currentRevision}
    end
    if tonumber(currentRevision) == requestedRevision then
        local currentDigest = redis.call('HGET', KEYS[3], 'definitionDigest')
        if currentDigest ~= ARGV[3] then
            -- 相同修订号出现两份语义不同的定义时封闭调度，必须经显式管理动作选择。
            redis.call('HSET', KEYS[3], 'state', 'CONFLICT')
            redis.call('ZREM', KEYS[2], ARGV[1])
            return {'CONFLICT', currentRevision}
        end
        -- 收养已有定义时补齐历史空来源；来源不同的情形已在上方门禁拒绝。
        if not currentSource or currentSource == '' then
            redis.call('HSET', KEYS[3], 'schedulerQualifier', ARGV[25])
        end
        return {'ADOPTED', currentRevision}
    end
end

redis.call('SADD', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[3],
        'schedulerQualifier', ARGV[25],
        'jobName', ARGV[1],
        'definitionRevision', ARGV[2],
        'definitionDigest', ARGV[3],
        'state', ARGV[4],
        'scheduleShard', ARGV[5],
        'workerName', ARGV[6],
        'workerKey', ARGV[7],
        'trigger', ARGV[8],
        'scheduleType', ARGV[9],
        'cron', ARGV[10],
        'zone', ARGV[11],
        'intervalMs', ARGV[12],
        'concurrency', ARGV[13],
        'misfire', ARGV[14],
        'timeoutMs', ARGV[15],
        'maxAttempts', ARGV[16],
        'retryDelayMs', ARGV[17],
        'contractRevision', ARGV[18],
        'schemaId', ARGV[19],
        'wireCodecs', ARGV[20],
        'fanoutReceiptTimeoutMs', ARGV[21],
        'fanoutReceiptMaxRetries', ARGV[22],
        'fanoutFailurePolicy', ARGV[23],
        'nextFireAt', ARGV[24])
redis.call('HDEL', KEYS[3], 'deletedAt', 'tombstoneUntil')

if ARGV[4] == 'ENABLED' and tonumber(ARGV[24]) > 0 then
    redis.call('ZADD', KEYS[2], ARGV[24], ARGV[1])
else
    redis.call('ZREM', KEYS[2], ARGV[1])
end
return {'OK', ARGV[2]}
