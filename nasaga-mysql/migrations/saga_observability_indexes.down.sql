-- 只在指标端点已停用且确认不会重新引入全表扫描后回退。
ALTER TABLE saga_step_attempt
    DROP INDEX idx_attempt_status_finished,
    DROP INDEX idx_attempt_retry,
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE saga_transition
    DROP INDEX idx_transition_state_time,
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE saga_instance
    DROP INDEX idx_status_lifecycle,
    ALGORITHM=INPLACE,
    LOCK=NONE;
