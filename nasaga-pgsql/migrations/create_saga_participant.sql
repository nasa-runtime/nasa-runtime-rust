-- 业务作用：创建 PostgreSQL Saga 参与方的本地准入门禁持久结构。
CREATE TABLE IF NOT EXISTS saga_participant_step (
    saga_id VARCHAR(256) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    tenant_id VARCHAR(256) NOT NULL,
    workflow_name VARCHAR(128) NOT NULL,
    definition_version BIGINT NOT NULL CHECK (definition_version > 0),
    definition_digest CHAR(64) NOT NULL,
    forward_status VARCHAR(32) NOT NULL,
    cancel_status VARCHAR(32) NOT NULL,
    compensation_status VARCHAR(32) NOT NULL,
    resolution_status VARCHAR(32) NOT NULL,
    execute_effect_id CHAR(36) NOT NULL,
    cancel_effect_id CHAR(36),
    compensate_effect_id CHAR(36),
    resolve_effect_id CHAR(36),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_saga_participant_definition_digest
        CHECK (definition_digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (saga_id, step_name),
    CONSTRAINT saga_participant_execute_effect UNIQUE (execute_effect_id)
);
