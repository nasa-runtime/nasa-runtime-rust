-- 业务作用：在旧 binary 已恢复且数据库负责人批准后还原旧投递索引；回退会恢复随表规模增长的扫描成本。
ALTER TABLE outbox_event
    ADD INDEX idx_pending (dispatched, id),
    DROP INDEX idx_dispatchable,
    DROP INDEX idx_dead,
    ALGORITHM=INPLACE,
    LOCK=NONE;
