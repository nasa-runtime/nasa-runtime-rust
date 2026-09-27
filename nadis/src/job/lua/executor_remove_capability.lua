-- 业务作用：从单个 Worker 能力索引撤销当前执行器，使删除 Worker 后创建的新 Fanout 快照不再选中本节点。
-- KEYS[1] Worker 能力 ZSET；KEYS[2] Worker 成员元数据 HASH；KEYS[3] 执行器主记录 HASH。
-- ARGV[1] executorId；ARGV[2] workerName。
-- 返回：{OK}。
-- 安全不变量：能力成员、成员元数据与执行器反向字段在 registry slot 内一次撤销，不触碰其它能力和其它执行器；
-- 与优雅停机的全量注销使用同一组字段，快照和后续 GC 不会观察到半撤销成员。

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2],
    ARGV[1] .. '|nodeIdentity', ARGV[1] .. '|startupId',
    ARGV[1] .. '|applicationName', ARGV[1] .. '|runtime',
    ARGV[1] .. '|contractRevision', ARGV[1] .. '|schemaId',
    ARGV[1] .. '|wireCodecs', ARGV[1] .. '|implementationDigest')
redis.call('HDEL', KEYS[3], 'capability:' .. ARGV[2])
return {'OK'}
