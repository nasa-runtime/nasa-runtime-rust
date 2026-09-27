-- 业务作用：为一个 (qualifier, namespace) 原子创建或比对不可变布局指纹。
-- 同一 source 的所有语言、所有节点必须使用相同分片数、Fanout 桶数、协议代次和消费组，
-- 否则同一个任务会形成两份定义与两套调度时刻，相同 fanoutId 也会落到不同 slot。
-- 首个节点写入指纹，后续节点只允许完全一致；不一致时返回已有指纹交由调用方拒绝启动。
--
-- KEYS[1] layout marker 键（位于 registry slot，与可变分片布局无关）
-- ARGV[1] 本节点的布局指纹
-- 返回 { 'OK' } 或 { 'MISMATCH', 已有指纹 }

local existing = redis.call('GET', KEYS[1])
if not existing then
    -- SETNX 而不是 SET：并发启动时只有一个节点写入，其余节点走比对分支得到同一结论
    if redis.call('SETNX', KEYS[1], ARGV[1]) == 1 then
        return { 'OK' }
    end
    existing = redis.call('GET', KEYS[1])
end

if existing == ARGV[1] then
    return { 'OK' }
end

return { 'MISMATCH', existing }
