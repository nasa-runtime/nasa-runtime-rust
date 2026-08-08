-- 业务作用：先建立计数与候选定位索引，再删除查询前缀已被包含的旧索引，避免扫描死信热区和重复写放大。
ALTER TABLE outbox_event
    ADD INDEX idx_dispatchable (dispatched, dead, id),
    ADD INDEX idx_dead (dead, id),
    DROP INDEX idx_pending,
    ALGORITHM=INPLACE,
    LOCK=NONE;
