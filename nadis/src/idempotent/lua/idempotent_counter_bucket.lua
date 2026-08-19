-- 业务计数幂等内核：窗口内重复 nonce 返回首次结果，首次命令成功后才登记凭证。
--
-- KEYS[1]    目标 String、Hash 或 ZSet 键
-- KEYS[2..n] 当前逻辑计数器所属 ledger shard 的全部环形桶，必须与目标键同 slot
-- ARGV[1]    recordField，原始 SHA-256 字节，不包含方向和增量
-- ARGV[2]    STR_INCR | STR_DECR | HASH_INCR | HASH_DECR | ZSET_INCR
-- ARGV[3]    Hash field 或 ZSet member；String 操作为空字节
-- ARGV[4]    原始十进制增量
-- ARGV[5]    nonce TTL 毫秒数
-- ARGV[6]    单个环形桶跨度毫秒数
--
-- 返回 {code, value, detail}。顶层数组保证直发和批次路径使用相同 MULTI 返回形态。

local targetKey = KEYS[1]
local recordField = ARGV[1]
local opType = ARGV[2]
local member = ARGV[3]
local delta = ARGV[4]
local nonceTtlMs = tonumber(ARGV[5])
local bucketSpanMs = tonumber(ARGV[6])
local bucketCount = #KEYS - 1

if nonceTtlMs == nil or nonceTtlMs <= 0
        or bucketSpanMs == nil or bucketSpanMs <= 0 or bucketCount <= 0 then
    return { 'REJECTED_OPERATION', '', 'invalid idempotent bucket configuration' }
end

local time = redis.call('TIME')
local nowMs = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local currentEpoch = math.floor(nowMs / bucketSpanMs)

-- 判重必须早于方向、增量和目标类型校验，重试参数漂移不能改变已经完成的业务事实。
for index = 2, #KEYS do
    local ledgerType = redis.call('TYPE', KEYS[index])['ok']
    if ledgerType ~= 'none' and ledgerType ~= 'hash' then
        return { 'REJECTED_LEDGER_TYPE', '', '' }
    end
    if ledgerType == 'hash' then
        local storedEpoch = tonumber(redis.call('HGET', KEYS[index], '__epoch'))
        if storedEpoch ~= nil then
            local age = currentEpoch - storedEpoch
            -- 时钟回拨会让已写桶短暂位于“未来”；这类凭证仍在保证窗口内，必须继续参与判重。
            -- 环形桶真正的陈旧边界只由 age >= bucketCount 判定，不能因负 age 放开同 nonce 重放。
            if age < bucketCount then
                local previous = redis.call('HGET', KEYS[index], recordField)
                if previous then
                    return { 'DUPLICATE', previous, '' }
                end
            end
        end
    end
end

-- Hash 没有 HDECRBY，只在首次请求路径改变符号；-9223372036854775808 取负会 int64 越界，单独拒绝。
if opType == 'HASH_DECR' then
    if delta == '-9223372036854775808' then
        return { 'REJECTED_COMMAND', '', 'hash decrement delta is outside HINCRBY range' }
    end
    if string.sub(delta, 1, 1) == '-' then
        delta = string.sub(delta, 2)
    else
        delta = '-' .. delta
    end
end

-- 原生命令负责类型、数值、NaN 与 int64 越界校验；拒绝时不会写目标键。
local result
if opType == 'STR_INCR' then
    result = redis.pcall('INCRBY', targetKey, delta)
elseif opType == 'STR_DECR' then
    result = redis.pcall('DECRBY', targetKey, delta)
elseif opType == 'HASH_INCR' or opType == 'HASH_DECR' then
    result = redis.pcall('HINCRBY', targetKey, member, delta)
elseif opType == 'ZSET_INCR' then
    result = redis.pcall('ZINCRBY', targetKey, delta, member)
else
    return { 'REJECTED_OPERATION', '', '' }
end
if type(result) == 'table' and result.err then
    return { 'REJECTED_COMMAND', '', result.err }
end

-- int64 结果重新读取十进制字节，避免 Lua number 经过 double 后丢失精度。
local value
if opType == 'ZSET_INCR' then
    value = result
elseif opType == 'HASH_INCR' or opType == 'HASH_DECR' then
    value = redis.call('HGET', targetKey, member)
else
    value = redis.call('GET', targetKey)
end

-- ring index 随时间前进再次利用时先整桶换代，旧窗口凭证不能进入新一轮判重。
-- 时钟回拨命中未来代次时保留整桶，并把新凭证写入该较新代次；删除它会放开仍在保证窗口内的重放。
local currentIndex = currentEpoch % bucketCount
local currentLedgerKey = KEYS[currentIndex + 2]
local storedEpoch = tonumber(redis.call('HGET', currentLedgerKey, '__epoch'))
if storedEpoch == nil or storedEpoch < currentEpoch then
    redis.call('DEL', currentLedgerKey)
    redis.call('HSET', currentLedgerKey, '__epoch', string.format('%d', currentEpoch))
end

redis.call('HSET', currentLedgerKey, recordField, value)
local ttlResult = redis.pcall('PEXPIRE', currentLedgerKey, nonceTtlMs)
if type(ttlResult) == 'table' and ttlResult.err then
    -- 目标已经变化后不能删除凭证放开重放；保留凭证并把回收风险交给框架告警。
    return { 'APPLIED_TTL_MISSING', value, ttlResult.err }
end
if ttlResult ~= 1 then
    return { 'APPLIED_TTL_MISSING', value, 'PEXPIRE did not attach bucket TTL' }
end
return { 'APPLIED', value, '' }
