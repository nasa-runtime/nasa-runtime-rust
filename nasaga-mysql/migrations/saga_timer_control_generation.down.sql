-- 业务作用：仅在旧 binary 已恢复、且暂停退避数据不再被新协议依赖时回退上一版结构。

ALTER TABLE saga_instance
    DROP COLUMN control_version,
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE saga_timer
    DROP INDEX idx_due,
    ADD INDEX idx_due (state, due_at),
    DROP COLUMN available_at,
    ALGORITHM=INPLACE,
    LOCK=NONE;
