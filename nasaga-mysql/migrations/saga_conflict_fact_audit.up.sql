-- 业务作用：持久化同一 attempt 的互斥终态，保证升级人工介入时双方证据同事务留存。

CREATE TABLE saga_conflict_fact (
    saga_id VARCHAR(256) NOT NULL,
    incoming_event_id VARCHAR(190) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    phase VARCHAR(16) NOT NULL,
    attempt_no INT UNSIGNED NOT NULL,
    existing_status VARCHAR(32) NOT NULL,
    incoming_status VARCHAR(32) NOT NULL,
    conflict_kind VARCHAR(64) NOT NULL,
    occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (saga_id, incoming_event_id),
    KEY idx_conflict_saga_time (saga_id, occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
