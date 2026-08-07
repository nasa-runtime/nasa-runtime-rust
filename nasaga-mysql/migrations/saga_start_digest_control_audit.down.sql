-- 业务作用：仅在旧 binary 已恢复且控制操作审计已导出后回退本批结构。

DROP TABLE saga_control_transition;

ALTER TABLE saga_instance
    DROP COLUMN start_request_digest,
    ALGORITHM=INPLACE,
    LOCK=NONE;
