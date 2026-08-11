-- 业务作用：死信生命周期证据。dead_at 记录进入死信的时刻,死信保留期以它为唯一时间
-- 依据(created_at 会把"长期待投递后刚标死"的行立即变成清理候选);历史死信行 dead_at
-- 为 NULL 时永不进入清理候选,须由运维核对后显式回填。outbox_dead_disposal 与删除同
-- 事务持久化批准标识与收据身份,人工处置可事后复验。

ALTER TABLE outbox_event
    ADD COLUMN dead_at TIMESTAMP(6) NULL AFTER dead,
    -- 保留清理的时间过滤与最老候选聚合按生命周期时刻走索引;缺失时聚合退化为全表扫描。
    ADD INDEX idx_retention_dispatched (dispatched, dead, dispatched_at),
    ADD INDEX idx_retention_dead (dead, dead_at);

CREATE TABLE outbox_dead_disposal (
    event_id CHAR(36) NOT NULL,
    approval VARCHAR(128) NOT NULL,
    receipt_event_id CHAR(36) NOT NULL,
    disposed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
