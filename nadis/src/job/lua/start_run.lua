-- 业务作用：为普通 Run 或 Fanout shard 原子分配 attempt、单调 fencing token 和租约，同时确认当前持久消息。
-- KEYS（NORMAL）：Run HASH、leases ZSET、visible ZSET、running HASH、任务 waitq、fences HASH、Dispatch Stream、completion Stream、任务定义 HASH、schedule ZSET、命名空间控制 HASH。
-- KEYS（FANOUT）：shard HASH、leases ZSET、ready ZSET、Fanout 根 HASH、receipts ZSET、fence 所在根 HASH、inbox Stream、completion Stream、Fanout 根 HASH。
-- ARGV[1..11] jobName/workerName、runId/shardRunId、executorId、messageId、leaseMs、concurrency、maxSerialBacklog、deferDelayMs、consumerGroup、retentionMs、NORMAL/FANOUT；
-- NORMAL ARGV[12..18] serialOverflowPolicy、protocolVersion、definitionRevision、contractRevision、schemaId、wireCodecs、分片键前缀；
-- NORMAL ARGV[19] 本执行器期望的 schedulerQualifier，用于复验记录自身声明的来源数据源；ARGV[20] 暂停期间的再扫描延迟毫秒；
-- FANOUT ARGV[12..15] targetNodeIdentity、assignmentEpoch、fanoutId、seq；ARGV[16] 期望 executionKey；ARGV[17..18] 预留；
-- FANOUT ARGV[19..23] 本执行器期望的 schedulerQualifier、protocolVersion、contractRevision、schemaId、wireCodecs。
-- 返回：NOT_COMMITTED、STALE_ASSIGNMENT、IDENTITY_MISMATCH、STATE_MISMATCH、NOT_FOUND、DEFERRED、LEASE_EXPIRED、JOB_DELETED、SOURCE_MISMATCH、PROTOCOL_UNSUPPORTED、CONTRACT_MISMATCH、CONFLICT、STALE、CANCELLED、SKIPPED、BLOCKED、FENCING_REGRESSION，
-- 或 {ADOPTED/STARTED, attempt, attemptToken, redisNow, leaseUntil}。
-- 安全不变量：状态门禁先于一切会写记录或索引的业务门禁，终态不被迟到消息改写；同 owner 重复认领只在租约窗口内续租；
-- 执行前重新复验根状态、目标 assignment、任务门禁、协议与合同；新 token 必须严格大于记录中历史 token；状态、租约和消息 ACK 一次提交。
-- FANOUT 门禁拒绝时在 shard 写 rejectedCode/rejectedBy/rejectedExpectation/rejectedAt 供跨节点排查，不改 state；门禁通过则清除过时拒绝证据。
-- 证据组独立于 errorType：errorType 由重分配、租约恢复和取消等状态推进路径改写，两组语义互不覆盖。

local state = redis.call('HGET', KEYS[1], 'state')
if ARGV[11] == 'FANOUT' then
    local rootState = redis.call('HGET', KEYS[4], 'state')
    -- 只有已提交且未进入取消/终态的根可以开放新 shard 执行权。
    if rootState ~= 'COMMITTED' and rootState ~= 'WAITING_CHILDREN' then return {'NOT_COMMITTED'} end
    if redis.call('HGET', KEYS[1], 'targetNodeIdentity') ~= ARGV[12]
            or redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[13] then return {'STALE_ASSIGNMENT'} end
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)
    -- 状态门禁先于一切会写 shard 的业务门禁（与 NORMAL 分支同一条不变量）：
    -- 根仍在等待其它 shard 时，个别 shard 早已终态是正常窗口；迟到的重放消息既不得给终态追加
    -- 当前拒绝证据，也不得先清除终态已有证据再返回，否则审计会看到 SUCCEEDED 与拒绝证据并存的矛盾。
    if state == 'RUNNING' and redis.call('HGET', KEYS[1], 'owner') == ARGV[3] then
        -- 只有仍在认领窗口内的同 owner 重复消息才能续租返回原 attempt/token。
        -- 租约已过 Redis 当前时间说明该执行器已自我失权，复活旧 token 会延后恢复裁决，
        -- 并在旧 Handler 未停净时打开同 token 的并行副作用窗口；过期一律交给恢复状态机。
        if tonumber(redis.call('HGET', KEYS[1], 'leaseUntil') or '0') <= now then
            redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
            return {'LEASE_EXPIRED'}
        end
        local leaseUntil = now + tonumber(ARGV[5])
        redis.call('HSET', KEYS[1], 'leaseUntil', leaseUntil)
        redis.call('ZADD', KEYS[2], leaseUntil, ARGV[14] .. ':' .. ARGV[15])
        redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
        redis.call('XDEL', KEYS[7], ARGV[4])
        return {'ADOPTED', redis.call('HGET', KEYS[1], 'attempt'),
                redis.call('HGET', KEYS[1], 'attemptToken'), tostring(now), tostring(leaseUntil)}
    end
    if state ~= 'RECEIVED' then return {'STATE_MISMATCH'} end
    -- 调用方已经取得本地执行槽，容量压力不再是本次未启动的有效归因；后续门禁拒绝须走各自出口。
    redis.call('HDEL', KEYS[1], 'capacityDeferredAt', 'capacityRouteCount', 'capacityRouteBlockedAt')
    -- 拒绝在 shard 上留下证据（与 accept 相同的字段），使跨节点排查不依赖进程本地指标；
    -- 证据写独立的 rejectedCode 而不是 errorType，避免重分配、租约恢复或取消改写 errorType 时
    -- 留下"新归因 + 旧拒绝方"的自相矛盾记录。只写观测字段、不改 state，
    -- 收敛仍由 ready 唤醒上限按失败策略升级；经上方状态门禁，以下只作用于仍可认领的 RECEIVED shard。
    local function reject(code, expectation)
        redis.call('HSET', KEYS[1], 'rejectedCode', code, 'rejectedBy', ARGV[3],
                'rejectedExpectation', expectation, 'rejectedAt', now)
        return {code}
    end
    -- 身份一致性复验：shard 记录自身保存的 fanoutId、seq、workerName 与 executionKey 必须与
    -- 调用方按同一分片推导出的值一致。executionKey 是跨重发、重分配和重试保持稳定的业务幂等键，
    -- 记录内容被错误改写时不能继续把它交给 Worker 当作去重依据。
    if redis.call('HGET', KEYS[1], 'fanoutId') ~= ARGV[14]
            or redis.call('HGET', KEYS[1], 'seq') ~= ARGV[15]
            or redis.call('HGET', KEYS[1], 'workerName') ~= ARGV[1]
            or redis.call('HGET', KEYS[1], 'executionKey') ~= ARGV[16] then
        return reject('IDENTITY_MISMATCH', 'executionKey=' .. ARGV[16])
    end
    -- 来源复验：与普通 Run 同一条不变量。键前缀只说明 shard 放在哪里，这里核对它自己声明的来源；
    -- 缺少这道门禁时，写进本前缀的外来 shard 会被本节点按本地 qualifier 构造上下文执行。
    if (redis.call('HGET', KEYS[1], 'schedulerQualifier') or '') ~= ARGV[19] then
        return reject('SOURCE_MISMATCH', 'source=' .. ARGV[19])
    end
    -- 协议代次门禁：跨语言节点必须能拒绝自己不理解的高版本 shard，而不是按旧语义解码。
    if tonumber(redis.call('HGET', KEYS[1], 'protocolVersion') or '0') > tonumber(ARGV[20]) then
        return reject('PROTOCOL_UNSUPPORTED', 'protocol<=' .. ARGV[20])
    end
    -- Worker 合同复验：快照冻结之后可能发生滚动发布或节点重启，"快照创建时兼容"不等于"取得执行权时仍兼容"。
    -- 少了这一步，旧 shard 会被新合同的同名 Handler 用错误 Schema 解码执行。
    local shardCodec = redis.call('HGET', KEYS[1], 'wireCodec') or ''
    if redis.call('HGET', KEYS[1], 'contractRevision') ~= ARGV[21]
            or redis.call('HGET', KEYS[1], 'schemaId') ~= ARGV[22]
            or not string.find(',' .. ARGV[23] .. ',', ',' .. shardCodec .. ',', 1, true) then
        return reject('CONTRACT_MISMATCH', 'contract=' .. ARGV[21] .. '|' .. ARGV[22] .. '|' .. ARGV[23])
    end
    -- 门禁全部通过说明记录与本节点兼容，其它节点留下的拒绝证据已过时，认领动作前一并清除。
    redis.call('HDEL', KEYS[1], 'rejectedCode', 'rejectedBy', 'rejectedExpectation', 'rejectedAt')
    local previousToken = tonumber(redis.call('HGET', KEYS[1], 'attemptToken')) or 0
    local token = redis.call('HINCRBY', KEYS[6], 'fence:' .. ARGV[15], 1)
    -- 持久 fencing 计数必须超过 shard 记录中的历史 token，否则封闭执行入口。
    if token <= previousToken then return {'FENCING_REGRESSION'} end
    local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
    local leaseUntil = now + tonumber(ARGV[5])
    -- 成功取得执行权说明此前的推进异常（NO_CAPABLE_EXECUTOR、LEASE_EXPIRED 等）已经解除，
    -- errorType 只保留未解除的异常，避免健康运行的分片带着看似当前故障的标记。
    redis.call('HDEL', KEYS[1], 'errorType')
    redis.call('HSET', KEYS[1], 'state', 'RUNNING', 'owner', ARGV[3],
            'attemptToken', token, 'startedAt', now, 'leaseUntil', leaseUntil)
    redis.call('ZADD', KEYS[2], leaseUntil, ARGV[14] .. ':' .. ARGV[15])
    redis.call('ZREM', KEYS[3], ARGV[14] .. ':' .. ARGV[15])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'STARTED', tostring(attempt), tostring(token), tostring(now), tostring(leaseUntil)}
end
if not state then
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'NOT_FOUND'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)

-- 确定性终态与 finish_run.lua 同一组收尾：索引清理、串行槽释放与队首唤醒、FIXED_DELAY 重排、
-- Completion 与保留期一次提交。终态出口若各自维护不完整的收尾，FIXED_DELAY 任务会在确定性异常后
-- 从 Schedule ZSET 永久消失（同修订号重启只返回 ADOPTED、不重写调度时刻），串行 BLOCKED 成员也可能
-- 失去唯一唤醒者。
local function finalize(terminalState, resultCode)
    redis.call('ZREM', KEYS[2], ARGV[2])
    redis.call('ZREM', KEYS[3], ARGV[2])
    redis.call('ZREM', KEYS[5], ARGV[2])
    -- 单活槽以 jobName 为 field；QUEUED Run 常态不持槽，这里是异常路径的防御性复验后释放
    if redis.call('HGET', KEYS[4], ARGV[1]) == ARGV[2] then
        redis.call('HDEL', KEYS[4], ARGV[1])
        -- 释放串行槽后只能唤醒删除 fence 外的新执行实例；旧成员在同一原子步骤中
        -- 进入终态，避免重新登记后旧任务借槽位重新可见。单次最多处理 100 个队首，
        -- 超出部分交给分批收敛入口，避免积压量决定 Lua 原子段时长。
        local handoffComplete = false
        local handoffJobState = redis.call('HGET', KEYS[9], 'state')
        local handoffDeletedRevision = tonumber(redis.call('HGET', KEYS[9], 'deletedRevision') or '-1')
        for index = 1, 100 do
            local nextEntry = redis.call('ZPOPMIN', KEYS[5], 1)
            if not nextEntry[1] then
                handoffComplete = true
                break
            end
            local nextRunKey = ARGV[18] .. 'run:' .. nextEntry[1]
            if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
                local nextRevision = tonumber(redis.call('HGET', nextRunKey, 'definitionRevision') or '0')
                if handoffJobState == 'DELETED' or nextRevision <= handoffDeletedRevision then
                    redis.call('HSET', nextRunKey, 'state', 'CANCELLED', 'finishedAt', now,
                            'resultCode', 'JOB_DELETED')
                    redis.call('ZREM', KEYS[2], nextEntry[1])
                    redis.call('ZREM', KEYS[3], nextEntry[1])
                    redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*',
                            'runId', nextEntry[1], 'state', 'CANCELLED', 'resultCode', 'JOB_DELETED')
                    redis.call('PEXPIRE', nextRunKey, tonumber(ARGV[10]))
                else
                    redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', now)
                    redis.call('ZADD', KEYS[3], now, nextEntry[1])
                    handoffComplete = true
                    break
                end
            end
        end
        if not handoffComplete and redis.call('ZCARD', KEYS[5]) > 0 then
            redis.call('SADD', ARGV[18] .. 'reaping', ARGV[1])
        end
    end
    -- 与 finish_run 相同的固定延迟重排：任何终态都要产生下一逻辑时刻，任务不得脱离调度
    -- 固定延迟重排前必须复验定义未换代：Run 携带的 definitionRevision 与当前定义相等、
    -- 定义仍为 ENABLED 且仍声明 FIXED_DELAY。缺任一条件时，旧执行实例会取得新定义的调度写权，
    -- 把已改为 FIXED_RATE/CRON 的定义拉回旧固定延迟节奏，覆盖新修订已计算的调度时刻。
    if redis.call('HGET', KEYS[1], 'triggerType') == 'FIXED_DELAY'
            and redis.call('HGET', KEYS[9], 'state') == 'ENABLED'
            and redis.call('HGET', KEYS[9], 'scheduleType') == 'FIXED_DELAY'
            and redis.call('HGET', KEYS[1], 'definitionRevision') == redis.call('HGET', KEYS[9], 'definitionRevision') then
        local nextFireAt = now + tonumber(redis.call('HGET', KEYS[9], 'intervalMs') or '0')
        redis.call('HSET', KEYS[9], 'lastFireAt', redis.call('HGET', KEYS[1], 'logicalFireAt'),
                'nextFireAt', nextFireAt)
        redis.call('ZADD', KEYS[10], nextFireAt, ARGV[1])
    end
    redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*',
            'runId', ARGV[2], 'state', terminalState, 'resultCode', resultCode)
    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[10]))
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
end

-- 状态门禁先于一切会写记录或索引的业务门禁：终态是最终事实，迟到的重复消息不得改写它，
-- 也不得让暂停、来源、协议或合同门禁给终态 Run 写 nextVisibleAt、错误归因或第二条 Completion。
-- 需求规定 STALE 只确认当前消费者看到的消息，不 XDEL、不改变 Run 及可见性索引。
if state == 'SUCCEEDED' or state == 'FAILED' or state == 'DEAD' or state == 'SKIPPED' or state == 'CANCELLED' then
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    return {'STALE'}
end
if state == 'RUNNING' then
    if redis.call('HGET', KEYS[1], 'owner') == ARGV[3] then
        -- 只有仍在认领窗口内的同 owner 重复消息才能续租返回原 attempt/token。
        -- 租约已过 Redis 当前时间说明该执行器已自我失权，复活旧 token 会延后恢复裁决，
        -- 并在旧 Handler 未停净时打开同 token 的并行副作用窗口；过期一律交给恢复状态机。
        if tonumber(redis.call('HGET', KEYS[1], 'leaseUntil') or '0') <= now then
            redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
            return {'LEASE_EXPIRED'}
        end
        local leaseUntil = now + tonumber(ARGV[5])
        redis.call('HSET', KEYS[1], 'leaseUntil', leaseUntil)
        redis.call('ZADD', KEYS[2], leaseUntil, ARGV[2])
        redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
        redis.call('XDEL', KEYS[7], ARGV[4])
        return {'ADOPTED', redis.call('HGET', KEYS[1], 'attempt'), redis.call('HGET', KEYS[1], 'attemptToken'), tostring(now), tostring(leaseUntil)}
    end
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    return {'STALE'}
end
if state ~= 'QUEUED' then
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    return {'STALE'}
end

-- 来源复验先于暂停与合同校验：来源不一致是确定性隔离结论，不随任务暂停或恢复而改变，
-- 暂停期间也不应让串源记录按 DEFERRED 反复重投。必须同时核对定义与 Run 两端：
-- 只比 Run 时，错误生产者把 Run 字段填成本地值就能在定义仍声明别处时通过。
local runSource = redis.call('HGET', KEYS[1], 'schedulerQualifier') or ''
local definitionSource = redis.call('HGET', KEYS[9], 'schedulerQualifier') or ''
if runSource ~= ARGV[19] or definitionSource ~= ARGV[19] then
    -- 只有 QUEUED Run 才会走到这里被隔离终态；证据字段留给运维排查，Completion 进入统一告警通道
    redis.call('HSET', KEYS[1], 'state', 'FAILED', 'errorType', 'SOURCE_MISMATCH',
            'resultCode', 'SOURCE_MISMATCH', 'finishedAt', now,
            'observedSource', runSource, 'observedDefinitionSource', definitionSource)
    finalize('FAILED', 'SOURCE_MISMATCH')
    return {'SOURCE_MISMATCH'}
end

-- 删除 fence 先于暂停门禁：删除是确定性终局而暂停是可逆延后。DELETED 定义、或修订号不高于
-- deletedRevision 的 Run（删除后又以更高修订号重新登记的场景）都立即终态化，不再无限 DEFERRED 空转；
-- finalize 的固定延迟重排门禁要求定义 ENABLED 且修订号相等，两种情形都不满足，因删除终态化的
-- FIXED_DELAY Run 不会重新排程。
local jobState = redis.call('HGET', KEYS[9], 'state')
local deletedRevision = tonumber(redis.call('HGET', KEYS[9], 'deletedRevision') or '-1')
local runRevision = tonumber(redis.call('HGET', KEYS[1], 'definitionRevision') or '0')
if jobState == 'DELETED' or runRevision <= deletedRevision then
    redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'JOB_DELETED')
    finalize('CANCELLED', 'JOB_DELETED')
    return {'JOB_DELETED'}
end

local namespaceState = redis.call('HGET', KEYS[11], 'namespaceState') or 'ENABLED'
if jobState ~= 'ENABLED' or namespaceState ~= 'ENABLED' then
    -- 单任务或命名空间暂停只延后未开始 Run，当前消息确认后仍由 visible 索引保证恢复。
    -- RUNNING 的同 owner 重认领已在上方完成，暂停不会撤销已经取得的 attempt。
    local nextVisibleAt = now + tonumber(ARGV[20])
    redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'DEFERRED'}
end

if tonumber(redis.call('HGET', KEYS[1], 'protocolVersion') or '0') > tonumber(ARGV[13]) then
    -- 当前执行器不理解更高协议时不得启动 Handler，保留 Run 等待兼容节点。
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'errorType', 'PROTOCOL_UNSUPPORTED', 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    -- 当前消息已经被本执行器确定性拒绝，后续只由 visible 重建；保留已确认条目不会再被任何消费者处理。
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'PROTOCOL_UNSUPPORTED'}
end
local storedCodec = redis.call('HGET', KEYS[1], 'wireCodec') or ''
if redis.call('HGET', KEYS[1], 'jobName') ~= ARGV[1]
        or redis.call('HGET', KEYS[1], 'definitionRevision') ~= ARGV[14]
        or redis.call('HGET', KEYS[1], 'contractRevision') ~= ARGV[15]
        or redis.call('HGET', KEYS[1], 'schemaId') ~= ARGV[16]
        or not string.find(',' .. ARGV[17] .. ',', ',' .. storedCodec .. ',', 1, true) then
    -- 定义、合同、Schema 或 codec 任一不匹配都拒绝解码和执行，避免滚动发布期间使用错误语义。
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'errorType', 'CONTRACT_MISMATCH', 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'CONFLICT'}
end

if redis.call('HGET', KEYS[1], 'cancelRequestedAt') then
    redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'CANCELLED')
    finalize('CANCELLED', 'CANCELLED')
    return {'CANCELLED'}
end

if ARGV[6] ~= 'PARALLEL' then
    -- 串行槽的占用、溢出处置和 waitq 入队必须与当前 Run 状态一次提交。
    local occupied = redis.call('HGET', KEYS[4], ARGV[1])
    if occupied and occupied ~= ARGV[2] then
        if ARGV[6] == 'DISCARD_IF_RUNNING' then
            redis.call('HSET', KEYS[1], 'state', 'SKIPPED', 'finishedAt', now, 'resultCode', 'RUNNING_EXISTS')
            finalize('SKIPPED', 'RUNNING_EXISTS')
            return {'SKIPPED'}
        end
        if ARGV[6] == 'SERIAL_QUEUE' and redis.call('ZCARD', KEYS[5]) >= tonumber(ARGV[7]) then
            if ARGV[12] == 'SKIP_NEWEST' then
                redis.call('HSET', KEYS[1], 'state', 'SKIPPED', 'finishedAt', now,
                        'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                finalize('SKIPPED', 'SERIAL_BACKLOG_OVERFLOW')
                return {'SKIPPED'}
            end
            local oldest = redis.call('ZPOPMIN', KEYS[5], 1)
            if oldest[1] then
                local oldestKey = ARGV[18] .. 'run:' .. oldest[1]
                if redis.call('HGET', oldestKey, 'state') == 'BLOCKED' then
                    redis.call('HSET', oldestKey, 'state', 'SKIPPED', 'finishedAt', now,
                            'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                    redis.call('ZREM', KEYS[3], oldest[1])
                    redis.call('ZREM', KEYS[2], oldest[1])
                    -- 被淘汰成员必须按它自己的快照收尾，调度责任不能转嫁给触发类型未知的占槽 Run：
                    -- 占槽与新入队的可能都是 MANUAL，它们的 finish 不会执行固定延迟重排，
                    -- 而被淘汰的 FIXED_DELAY Run 已从 Schedule 取出且就此终态，缺这一步任务永久脱离调度。
                    -- 门禁与其它终态出口同一条：定义未换代（修订号相等）、仍 ENABLED、仍声明 FIXED_DELAY。
                    if redis.call('HGET', oldestKey, 'triggerType') == 'FIXED_DELAY'
                            and redis.call('HGET', KEYS[9], 'state') == 'ENABLED'
                            and redis.call('HGET', KEYS[9], 'scheduleType') == 'FIXED_DELAY'
                            and redis.call('HGET', oldestKey, 'definitionRevision') == redis.call('HGET', KEYS[9], 'definitionRevision') then
                        local oldestNextFireAt = now + tonumber(redis.call('HGET', KEYS[9], 'intervalMs') or '0')
                        redis.call('HSET', KEYS[9], 'lastFireAt', redis.call('HGET', oldestKey, 'logicalFireAt'),
                                'nextFireAt', oldestNextFireAt)
                        redis.call('ZADD', KEYS[10], oldestNextFireAt, ARGV[1])
                    end
                    redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*', 'runId', oldest[1],
                            'state', 'SKIPPED', 'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                    redis.call('PEXPIRE', oldestKey, ARGV[10])
                end
            end
        end
        redis.call('HSET', KEYS[1], 'state', 'BLOCKED')
        redis.call('ZREM', KEYS[3], ARGV[2])
        redis.call('ZADD', KEYS[5], redis.call('HGET', KEYS[1], 'logicalFireAt'), ARGV[2])
        redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
        redis.call('XDEL', KEYS[7], ARGV[4])
        return {'BLOCKED'}
    end
end

local previousToken = tonumber(redis.call('HGET', KEYS[1], 'attemptToken')) or 0
local attemptToken = redis.call('HINCRBY', KEYS[6], ARGV[1], 1)
-- 新 token 不大于 Run 历史值意味着 fencing 权威发生回退，必须拒绝执行而不是继续写外部资源。
if attemptToken <= previousToken then
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'errorType', 'FENCING_REGRESSION', 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'FENCING_REGRESSION'}
end
local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
local leaseUntil = now + tonumber(ARGV[5])
-- 与 FANOUT 同一条不变量：取得执行权即清除已解除的推进异常标记（协议、合同、无兼容执行器等退避原因）。
-- dispatchAttempts 只度量连续未取得执行权的消息重建；新的 attempt 已经成立，后续 RETRY/DEFER 从零计量。
redis.call('HDEL', KEYS[1], 'errorType')
redis.call('HSET', KEYS[1], 'state', 'RUNNING', 'owner', ARGV[3],
        'attemptToken', attemptToken, 'startedAt', now, 'leaseUntil', leaseUntil, 'dispatchAttempts', 0)
if ARGV[6] ~= 'PARALLEL' then redis.call('HSET', KEYS[4], ARGV[1], ARGV[2]) end
redis.call('ZADD', KEYS[2], leaseUntil, ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('ZREM', KEYS[5], ARGV[2])
redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
redis.call('XDEL', KEYS[7], ARGV[4])
return {'STARTED', tostring(attempt), tostring(attemptToken), tostring(now), tostring(leaseUntil)}
