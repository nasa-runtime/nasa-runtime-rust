-- 业务作用：以 CAS 推进 Fanout 根的能力恢复游标，使等待兼容节点的 shard 被公平轮询。
-- KEYS[1] Fanout 根 HASH。
-- ARGV[1] 调用方观察的当前游标；ARGV[2] 下一游标。
-- 返回：NOT_FOUND、{STALE, currentCursor} 或 {OK, nextCursor}。
-- 原子性：只有仍持有当前游标观测值的扫描者可推进，避免并发扫描覆盖更新的进度。

if redis.call('EXISTS', KEYS[1]) == 0 then return {'NOT_FOUND'} end
local current = tonumber(redis.call('HGET', KEYS[1], 'capabilityCursor') or '0')
if current ~= tonumber(ARGV[1]) then return {'STALE', tostring(current)} end
redis.call('HSET', KEYS[1], 'capabilityCursor', ARGV[2])
return {'OK', ARGV[2]}
