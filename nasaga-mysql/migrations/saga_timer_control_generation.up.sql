-- 业务作用：把 timer 轮询退避与业务期限拆开，并给管理控制态建立独立 CAS generation。
-- 前置结构：saga_timer 尚无 available_at，saga_instance 尚无 control_version。

ALTER TABLE saga_timer
    ADD COLUMN available_at BIGINT NULL AFTER due_at,
    ALGORITHM=INPLACE,
    LOCK=NONE;

UPDATE saga_timer SET available_at = due_at WHERE available_at IS NULL;

ALTER TABLE saga_timer
    MODIFY COLUMN available_at BIGINT NOT NULL,
    DROP INDEX idx_due,
    ADD INDEX idx_due (state, available_at, due_at),
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE saga_instance
    ADD COLUMN control_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER control_state,
    ALGORITHM=INPLACE,
    LOCK=NONE;
