-- 业务作用：为 pause/resume 与人工恢复动作补齐 actor、reason 和稳定 operation 审计。
-- 历史控制行无法恢复当时主体，只能显式标为 legacy；禁止伪造新主体归因。

ALTER TABLE saga_control_transition
    ADD COLUMN actor VARCHAR(128) NOT NULL DEFAULT 'legacy-unknown' AFTER operation_id,
    ADD COLUMN reason VARCHAR(512) NOT NULL DEFAULT 'legacy operation predates actor audit' AFTER actor,
    ALGORITHM=INPLACE,
    LOCK=NONE;

CREATE TABLE saga_management_audit (
    saga_id VARCHAR(256) NOT NULL,
    operation_id VARCHAR(190) NOT NULL,
    action VARCHAR(64) NOT NULL,
    actor VARCHAR(128) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (saga_id, operation_id),
    KEY idx_management_actor_time (actor, occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
