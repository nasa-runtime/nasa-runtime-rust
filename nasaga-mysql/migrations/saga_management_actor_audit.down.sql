-- 业务作用：仅供已批准的结构回退；执行会永久删除人工操作主体与原因审计。

DROP TABLE saga_management_audit;

ALTER TABLE saga_control_transition
    DROP COLUMN reason,
    DROP COLUMN actor,
    ALGORITHM=INPLACE,
    LOCK=NONE;
