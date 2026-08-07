-- Saga 指标端点只做低基数聚合；覆盖状态、终态耗时和 retry 扫描，
-- 避免生产规模下每次 Prometheus scrape 退化为全表扫描。
ALTER TABLE saga_instance
    ADD INDEX idx_status_lifecycle (status, created_at, updated_at),
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE saga_transition
    ADD INDEX idx_transition_state_time (to_state, occurred_at),
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE saga_step_attempt
    ADD INDEX idx_attempt_status_finished (status, finished_at),
    ADD INDEX idx_attempt_retry (attempt_no),
    ALGORITHM=INPLACE,
    LOCK=NONE;
