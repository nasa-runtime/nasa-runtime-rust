-- 业务作用：拒绝同 business key 的启动输入漂移，并持久化 pause/resume operation 幂等证据。
-- 历史实例无法重建首步 payload，start_request_digest 必须保留 NULL 交由 runtime fail-closed。

ALTER TABLE saga_instance
    ADD COLUMN start_request_digest CHAR(64) NULL AFTER definition_digest,
    ALGORITHM=INPLACE,
    LOCK=NONE;

CREATE TABLE saga_control_transition (
    saga_id VARCHAR(256) NOT NULL,
    control_seq BIGINT UNSIGNED NOT NULL,
    from_state VARCHAR(16) NOT NULL,
    to_state VARCHAR(16) NOT NULL,
    operation_id VARCHAR(190) NOT NULL,
    occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (saga_id, control_seq),
    UNIQUE KEY uk_control_operation (saga_id, operation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
